from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.strategy.close_policy import decide_pair_close_reason
from src.strategy.entry_policy import evaluate_entry_policy
from src.strategy.funding_events import (
    estimate_pair_funding_events,
    infer_funding_interval_hours,
    normalize_funding_interval_hours,
)
from src.strategy.maker_entry_engine import MakerEntryEngine, MakerOrderState
from src.strategy.models import OpenPairState, StrategyConfig


def config(**overrides) -> StrategyConfig:
    values = {
        "exchange_names": ["binance", "okx"],
        "symbol_input": "BTC",
        "allowed_pairs": [("binance", "okx")],
    }
    values.update(overrides)
    return StrategyConfig(**values)


def pair() -> OpenPairState:
    return OpenPairState(
        pair_id="BTC:binance:okx:1",
        symbol="BTC",
        long_exchange="binance",
        short_exchange="okx",
        qty=1.0,
        requested_qty=1.0,
        long_entry_price=100.0,
        short_entry_price=101.0,
        entry_time_ms=1,
        entry_net_edge_usd=1.0,
        funding_long_rate=0.001,
        funding_short_rate=0.002,
    )


class FundingEventPolicyTest(unittest.TestCase):
    def estimate(self, *, entry_ms: int, end_ms: int):
        return estimate_pair_funding_events(
            long_rate=0.001,
            short_rate=0.002,
            long_notional_usd=1000.0,
            short_notional_usd=1000.0,
            long_next_funding_ms=10_000,
            short_next_funding_ms=10_000,
            long_interval_hours=8,
            short_interval_hours=4,
            long_interval_source="test",
            short_interval_source="test",
            entry_complete_ms=entry_ms,
            forecast_end_ms=end_ms,
        )

    def test_no_settlement_crossed_means_zero_cashflow(self):
        estimate = self.estimate(entry_ms=1_000, end_ms=9_999)
        self.assertEqual(estimate.event_count, 0)
        self.assertEqual(estimate.pnl_usd, 0.0)

    def test_full_event_cashflow_is_applied_once(self):
        estimate = self.estimate(entry_ms=1_000, end_ms=10_000)
        self.assertEqual(estimate.event_count, 2)
        self.assertAlmostEqual(estimate.long.pnl_usd, -1.0)
        self.assertAlmostEqual(estimate.short.pnl_usd, 2.0)
        self.assertAlmostEqual(estimate.pnl_usd, 1.0)

    def test_entry_completed_at_settlement_does_not_claim_funding(self):
        estimate = self.estimate(entry_ms=10_000, end_ms=20_000)
        self.assertEqual(estimate.event_count, 0)
        self.assertEqual(estimate.pnl_usd, 0.0)

    def test_common_and_generic_intervals_are_supported(self):
        for hours in (1, 2, 4, 6, 8, 12, 24):
            self.assertEqual(normalize_funding_interval_hours(hours), hours)
        self.assertEqual(infer_funding_interval_hours(1_000, 1_000 + 6 * 3_600_000), 6)

    def test_incomplete_pair_schedule_is_not_counted_as_expected_cashflow(self):
        estimate = estimate_pair_funding_events(
            long_rate=0.001,
            short_rate=0.002,
            long_notional_usd=1000.0,
            short_notional_usd=1000.0,
            long_next_funding_ms=10_000,
            short_next_funding_ms=0,
            long_interval_hours=8,
            short_interval_hours=None,
            long_interval_source="test",
            short_interval_source="unknown",
            entry_complete_ms=1_000,
            forecast_end_ms=10_000,
        )
        self.assertFalse(estimate.schedule_complete)
        self.assertEqual(estimate.pnl_usd, 0.0)
        self.assertEqual(estimate.reason, "funding_schedule_incomplete")

    def test_payment_direction_is_checked_even_without_event_cashflow(self):
        decision = evaluate_entry_policy(
            config=config(require_funding_payment=True),
            raw_funding_pnl_usd=0.0,
            funding_direction_pnl_usd=-1.0,
            effective_funding_pnl_usd=0.0,
            expected_net_edge_usd=1.0,
            expected_net_edge_bps=10.0,
        )
        self.assertTrue(decision.can_enter)

    def test_positive_funding_wait_never_overrides_max_hold(self):
        cfg = config(
            close_positive_funding_wait_enabled=True,
            close_positive_funding_wait_max_sec=1800.0,
            max_hold_sec=100.0,
        )
        decision = decide_pair_close_reason(
            pair(),
            cfg,
            hold_pnl_usd=1.0,
            entry_age_sec=100.0,
            close_pnl_usd=-1.0,
            close_edge_usd=-0.1,
            next_funding_pnl_usd=1.0,
            funding_wait_sec=10.0,
        )
        self.assertEqual(decision, "max_hold")

    def test_positive_funding_wait_is_bounded_and_net_positive_only(self):
        cfg = config(
            close_positive_funding_wait_enabled=True,
            close_positive_funding_wait_max_sec=1800.0,
        )
        waiting = decide_pair_close_reason(
            pair(),
            cfg,
            hold_pnl_usd=1.0,
            entry_age_sec=10.0,
            close_pnl_usd=-1.0,
            close_edge_usd=-0.1,
            next_funding_pnl_usd=1.0,
            funding_wait_sec=60.0,
        )
        paying = decide_pair_close_reason(
            pair(),
            cfg,
            hold_pnl_usd=-1.0,
            entry_age_sec=10.0,
            close_pnl_usd=-1.0,
            close_edge_usd=-0.1,
            next_funding_pnl_usd=-1.0,
            funding_wait_sec=60.0,
        )
        self.assertEqual(waiting, "hold_for_positive_funding")
        self.assertEqual(paying, "spread_nonpositive")

    def test_maker_full_admission_disables_soft_preposition(self):
        engine = MakerEntryEngine(SimpleNamespace())  # type: ignore[arg-type]
        candidate = SimpleNamespace(
            can_enter=False,
            executable_qty=1.0,
            target_qty=1.0,
            long_ask=100.0,
            short_bid=101.0,
            mid_reference=100.5,
            block_reasons=["net_edge_below_bps_threshold"],
        )
        self.assertFalse(
            engine._candidate_allowed_for_preposition(  # noqa: SLF001
                candidate,
                config(maker_entry_engine_require_full_admission=True),
            )
        )
        self.assertTrue(
            engine._candidate_allowed_for_preposition(  # noqa: SLF001
                candidate,
                config(maker_entry_engine_require_full_admission=False),
            )
        )

    def test_maker_own_order_exception_does_not_hide_foreign_order(self):
        hooks = SimpleNamespace(
            extract_order_uuid=lambda row: str(row.get("order_id") or ""),
        )
        engine = MakerEntryEngine(hooks)  # type: ignore[arg-type]
        candidate = SimpleNamespace(symbol="BTC")
        state = MakerOrderState(
            candidate=candidate,  # type: ignore[arg-type]
            first_exchange_name="binance",
            first_side="buy",
            hedge_exchange_name="okx",
            hedge_side="sell",
            order_result={"order_id": "own"},
            order_id="own",
            target_qty=1.0,
            price=100.0,
            pre_qty_first=0.0,
            pre_qty_hedge=0.0,
            submitted_ms=1,
        )
        exchange = SimpleNamespace(
            waiting_list=[
                {"symbol": "BTCUSDT", "order_id": "own"},
                {"symbol": "BTCUSDT", "order_id": "foreign"},
            ],
            open_orders_snapshot={},
        )
        self.assertTrue(engine._has_foreign_first_leg_order(state, {"binance": exchange}))  # noqa: SLF001


if __name__ == "__main__":
    unittest.main()
