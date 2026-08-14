from __future__ import annotations

import unittest
from types import SimpleNamespace

import strategy
from src.strategy.models import FlattenLegResult, StrategyConfig


class PairCloseFailureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_flatten = strategy.emergency_flatten_symbol_state_detailed
        self.original_restore = strategy.restore_pair_neutrality_after_close_failure
        self.pair = SimpleNamespace(
            pair_id="pair-1",
            symbol="ETH",
            long_exchange="binance",
            short_exchange="okx",
            qty=1.0,
        )
        self.exchanges = {"binance": object(), "okx": object()}
        self.config = StrategyConfig(["binance", "okx"], "ETH", [("binance", "okx")])

    def tearDown(self) -> None:
        strategy.emergency_flatten_symbol_state_detailed = self.original_flatten
        strategy.restore_pair_neutrality_after_close_failure = self.original_restore

    async def test_first_leg_failure_does_not_close_other_leg(self) -> None:
        calls: list[str] = []

        async def fake_flatten(_exchange, symbol, *, aggressive_bps, role, config):
            calls.append(role)
            return FlattenLegResult(
                exchange_name="binance",
                symbol=symbol,
                role=role,
                ok=False,
                failure_reason="exchange_unavailable",
            )

        strategy.emergency_flatten_symbol_state_detailed = fake_flatten
        result = await strategy.emergency_flatten_pair_state(
            self.pair,
            self.exchanges,
            config=self.config,
            close_first_leg_role="long",
        )
        self.assertFalse(result.ok)
        self.assertEqual(calls, ["long"])
        self.assertEqual(result.other_leg_action, "skipped")

    async def test_second_leg_failure_runs_neutrality_restore(self) -> None:
        calls: list[str] = []

        async def fake_flatten(_exchange, symbol, *, aggressive_bps, role, config):
            calls.append(role)
            return FlattenLegResult(
                exchange_name="binance" if role == "long" else "okx",
                symbol=symbol,
                role=role,
                ok=role == "long",
                closed_qty=1.0 if role == "long" else 0.0,
                failure_reason="exchange_unavailable" if role == "short" else "",
            )

        restore_calls = 0

        async def fake_restore(*_args, **_kwargs):
            nonlocal restore_calls
            restore_calls += 1
            return {"neutralized": True, "remaining_net_qty": 0.0}

        strategy.emergency_flatten_symbol_state_detailed = fake_flatten
        strategy.restore_pair_neutrality_after_close_failure = fake_restore
        result = await strategy.emergency_flatten_pair_state(
            self.pair,
            self.exchanges,
            config=self.config,
            close_first_leg_role="long",
        )
        self.assertFalse(result.ok)
        self.assertEqual(calls, ["long", "short"])
        self.assertEqual(restore_calls, 1)
        self.assertTrue(result.neutralized)
        self.assertEqual(result.other_leg_action, "rehedged")


if __name__ == "__main__":
    unittest.main()
