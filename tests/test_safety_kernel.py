from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.core.action_gate import ActionGate
from src.core.order_intent import DuplicateOrderIntentError, OrderIntentJournal
from src.ops.persistence.funding_cashflow import FundingPairAttributionRegistry
from src.strategy.hedge import execute_residual_hedge
from src.strategy.maker_entry_engine import MakerEntryEngine
from src.strategy.models import StrategyConfig
from src.strategy.order_intent_supervisor import reconcile_unresolved_order_intents
from src.binance_f_module import api as binance_api
from src.binance_f_module import exchange as binance_exchange
from src.okx_f_module import api as okx_api


class ActionGateTests(unittest.TestCase):
    def test_same_symbol_has_exactly_one_winner(self) -> None:
        gate = ActionGate(stale_timeout_sec=60)
        first = gate.try_acquire_symbol(owner_id="maker-1", symbol="ETH", domain="maker")
        second = gate.try_acquire_symbol(owner_id="manual-1", symbol="ETH", domain="manual")
        other_symbol = gate.try_acquire_symbol(owner_id="manual-2", symbol="BTC", domain="manual")
        self.assertTrue(first[0])
        self.assertFalse(second[0])
        self.assertTrue(other_symbol[0])

    def test_shutdown_takeover_is_explicit(self) -> None:
        gate = ActionGate(stale_timeout_sec=60)
        gate.try_acquire_symbol(owner_id="maker-1", symbol="ETH", domain="maker")
        previous = gate.takeover_symbol(owner_id="shutdown-1", symbol="ETH", domain="shutdown")
        self.assertEqual(previous["owner_id"], "maker-1")
        self.assertEqual(gate.symbol_lease("ETH")["owner_id"], "shutdown-1")

    def test_four_distinct_pair_symbols_can_hold_leases_concurrently(self) -> None:
        gate = ActionGate(stale_timeout_sec=60)
        results = [
            gate.try_acquire_symbol(owner_id=f"pair-{symbol}", symbol=symbol, domain="pair")
            for symbol in ("ETH", "BTC", "SOL", "XRP")
        ]
        self.assertTrue(all(ok for ok, _reason in results))
        self.assertEqual(len(gate.snapshot()["symbol_leases"]), 4)


class OrderIntentJournalTests(unittest.TestCase):
    def test_duplicate_client_order_id_submit_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = OrderIntentJournal(Path(tmp) / "intents.jsonl", fsync=False)
            payload = {"client_order_id": "same-id", "side": "buy", "qty": 1}
            journal.record(exchange="binance", symbol="ETH", event_type="order_submit", payload=payload)
            with self.assertRaises(DuplicateOrderIntentError):
                journal.record(exchange="binance", symbol="ETH", event_type="order_submit", payload=payload)

    def test_ack_after_fill_does_not_regress_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = OrderIntentJournal(Path(tmp) / "intents.jsonl", fsync=False)
            payload = {"client_order_id": "cid-1", "side": "buy", "qty": 1}
            journal.record(exchange="binance", symbol="ETH", event_type="order_submit", payload=payload)
            journal.record(
                exchange="binance",
                symbol="ETH",
                event_type="fill",
                payload={**payload, "executed_qty": 1, "status": "FILLED"},
            )
            journal.record(exchange="binance", symbol="ETH", event_type="order_ack", payload=payload)
            self.assertEqual(journal.snapshot()["unresolved_count"], 0)

    def test_partial_intent_survives_process_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "intents.jsonl"
            journal = OrderIntentJournal(path, fsync=False)
            payload = {"client_order_id": "cid-partial", "side": "buy", "qty": 2}
            journal.record(exchange="binance", symbol="ETH", event_type="order_submit", payload=payload)
            journal.record(
                exchange="binance",
                symbol="ETH",
                event_type="partial_fill",
                payload={**payload, "executed_qty": 1},
            )
            restarted = OrderIntentJournal(path, fsync=False)
            self.assertEqual(restarted.snapshot()["unresolved_count"], 1)
            self.assertEqual(restarted.unresolved()[0]["state"], "PARTIAL")


class OrderIntentSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_order_resolves_only_after_flat_grace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = OrderIntentJournal(Path(tmp) / "intents.jsonl", fsync=False)
            journal.record(
                exchange="binance",
                symbol="ETH",
                event_type="order_submit",
                payload={"client_order_id": "missing-1", "timestamp": 1},
            )

            class Exchange:
                async def get_order_status(self, _request):
                    return {"state": "not_exist", "client_order_id": "missing-1"}

                def get_symbol_snapshot(self, _symbol):
                    return {"position": {"qty": 0}, "open_orders": []}

            events: list[str] = []
            remaining = await reconcile_unresolved_order_intents(
                journal,
                {"binance": Exchange()},
                position_qty=lambda snapshot: float((snapshot.get("position") or {}).get("qty") or 0),
                record_event=lambda event_type, **_payload: events.append(event_type),
                unknown_grace_sec=1,
                record_order_state=lambda *_args, **_kwargs: None,
            )
            self.assertEqual(remaining, [])
            self.assertIn("order_intent_startup_reconciled", events)

    def test_cancel_then_late_fill_is_recorded_as_fill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = OrderIntentJournal(Path(tmp) / "intents.jsonl", fsync=False)
            payload = {"client_order_id": "cid-2", "side": "sell", "qty": 2}
            journal.record(exchange="okx", symbol="BTC", event_type="order_submit", payload=payload)
            journal.record(
                exchange="okx",
                symbol="BTC",
                event_type="order_canceled",
                payload={**payload, "status": "CANCELED"},
            )
            late = journal.record(
                exchange="okx",
                symbol="BTC",
                event_type="fill",
                payload={**payload, "executed_qty": 2, "status": "FILLED"},
            )
            self.assertTrue(late["late_fill"])
            self.assertEqual(late["state"], "FILLED")
            self.assertEqual(journal.snapshot()["unresolved_count"], 0)


class MakerEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_pair_capacity_is_separate_from_one_active_maker(self) -> None:
        hooks = SimpleNamespace(record_event=lambda *_args, **_kwargs: None)
        engine = MakerEntryEngine(hooks)  # type: ignore[arg-type]
        candidate = SimpleNamespace(symbol="XRP", id="xrp", expected_net_edge_bps=20.0, entry_open_edge_bps=25.0)
        config = StrategyConfig(["binance", "okx"], "", [("binance", "okx")])
        config.maker_entry_engine_enabled = True
        config.maker_entry_engine_max_open_orders = 1
        config.max_open_pairs = 4
        submitted: list[str] = []

        engine._candidate_queue = lambda *_args, **_kwargs: [candidate]  # type: ignore[method-assign]

        async def submit(row, *_args, **_kwargs):
            submitted.append(row.symbol)

        engine._submit_maker_order = submit  # type: ignore[method-assign]
        three_pairs = {
            str(index): SimpleNamespace(status="open", symbol=f"S{index}")
            for index in range(3)
        }
        await engine.step(
            candidates=[candidate],
            open_pairs=three_pairs,
            exchanges={},
            config=config,
            symbols=["XRP"],
            assumed_flat_until_ms={},
        )
        self.assertEqual(submitted, ["XRP"])

        submitted.clear()
        four_pairs = {
            str(index): SimpleNamespace(status="open", symbol=f"S{index}")
            for index in range(4)
        }
        await engine.step(
            candidates=[candidate],
            open_pairs=four_pairs,
            exchanges={},
            config=config,
            symbols=["XRP"],
            assumed_flat_until_ms={},
        )
        self.assertEqual(submitted, [])

    async def test_unknown_cancel_response_is_not_terminal(self) -> None:
        engine = MakerEntryEngine(SimpleNamespace())  # type: ignore[arg-type]
        self.assertFalse(engine._cancel_result_is_terminal({"ok": True}))
        self.assertTrue(engine._cancel_result_is_terminal({"status": "CANCELED"}))


class HedgeFillTruthTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_ack_does_not_count_as_fill(self) -> None:
        class Exchange:
            def get_symbol_snapshot(self, _symbol):
                return {"position": {"qty": 1.0}}

            async def market_order(self, *_args, **_kwargs):
                return {"order_id": "order-1", "client_order_id": "cid-1", "status": "NEW"}

            async def order(self, *_args, **_kwargs):
                return {"error": "unexpected_limit_fallback"}

        resolve_calls = 0

        async def resolve_effective_order_qty(**_kwargs):
            nonlocal resolve_calls
            resolve_calls += 1
            return 0.0

        async def position_waiter(*_args, **_kwargs):
            return 1.0

        config = SimpleNamespace(
            hedge_market_immediate=True,
            hedge_fast_ack=True,
            maker_entry_fast_hedge=True,
            action_settle_wait_sec=0.0,
            entry_order_settle_sec=0.01,
            hedge_requote_timeout_sec=0.01,
            hedge_requote_count=0,
            close_aggressive_bps=1.0,
            assumed_flat_grace_sec=0.01,
        )
        result = await execute_residual_hedge(
            stage_label="test",
            exchange_name="binance",
            exchange=Exchange(),
            symbol="ETH",
            side="sell",
            pre_qty=0.0,
            target_qty=1.0,
            config=config,
            is_close=False,
            eps=1e-9,
            extract_position_qty=lambda _snapshot: 0.0,
            marketable_limit_price_with_bps=lambda *_args: 100.0,
            print_verbose=lambda *_args: None,
            use_reduce_only_market_close=lambda *_args: False,
            resolve_effective_order_qty=resolve_effective_order_qty,
            extract_order_avg_price=lambda _row, default: default,
            wait_for_position_fill=position_waiter,
            wait_for_position_close=position_waiter,
            cancel_if_open=lambda *_args, **_kwargs: asyncio.sleep(0),
            settle_grace_sec=lambda _config: 0.01,
        )
        self.assertEqual(result.filled_qty, 1.0)
        self.assertIsNotNone(result.acknowledged_ms)
        self.assertIsNotNone(result.full_fill_ms)
        self.assertGreaterEqual(resolve_calls, 1)


class FundingCashflowParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_binance_funding_payment_keeps_signed_income(self) -> None:
        original = binance_api._request

        async def fake_request(*_args, **_kwargs):
            return (
                200,
                [{
                    "symbol": "ETHUSDT",
                    "incomeType": "FUNDING_FEE",
                    "income": "-1.25",
                    "asset": "USDT",
                    "time": 1234,
                    "tranId": 99,
                }],
                {},
            )

        binance_api._request = fake_request
        try:
            rows = await binance_api.get_funding_payments()
        finally:
            binance_api._request = original
        self.assertEqual(rows[0]["symbol"], "ETH")
        self.assertEqual(rows[0]["amount"], -1.25)

    async def test_okx_funding_payment_uses_funding_subtypes_only(self) -> None:
        original = okx_api._request

        async def fake_request(*_args, **_kwargs):
            return (
                200,
                {
                    "code": "0",
                    "data": [
                        {"billId": "1", "instId": "BTC-USDT-SWAP", "subType": "174", "pnl": "0.7", "ccy": "USDT", "ts": "1234"},
                        {"billId": "2", "instId": "BTC-USDT-SWAP", "subType": "1", "pnl": "9", "ccy": "USDT", "ts": "1235"},
                    ],
                },
                {},
            )

        okx_api._request = fake_request
        try:
            rows = await okx_api.get_funding_payments()
        finally:
            okx_api._request = original
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "BTC")
        self.assertEqual(rows[0]["amount"], 0.7)


class FundingPairAttributionTests(unittest.TestCase):
    def test_closed_pair_is_attributed_by_payment_time(self) -> None:
        registry = FundingPairAttributionRegistry()
        registry.observe_event(
            "pair_opened",
            {"pair": {"pair_id": "p1", "symbol": "ETH", "entry_time_ms": 1_000}},
            1_000,
        )
        registry.observe_event(
            "pair_closed",
            {"pair": {"pair_id": "p1", "symbol": "ETH", "entry_time_ms": 1_000, "status": "closed"}},
            5_000,
        )
        self.assertEqual(registry.pair_id_at("ETH", 3_000), "p1")
        self.assertIsNone(registry.pair_id_at("ETH", 6_000))


class OpenOrderBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_binance_symbol_poll_uses_active_set_between_global_reconciles(self) -> None:
        exchange = binance_exchange.Exchange(["ETH", "BTC", "SOL"])
        exchange.open_orders_last_full_reconcile_ms = 10**15
        exchange.waiting_list = [{"ticker": "ETH", "uuid": "maker-1"}]
        calls: list[dict] = []
        original = binance_exchange.get_open_orders

        async def fake_get_open_orders(option=None):
            calls.append(dict(option or {}))
            return []

        binance_exchange.get_open_orders = fake_get_open_orders
        try:
            rows = await exchange._fetch_open_orders_snapshot()
        finally:
            binance_exchange.get_open_orders = original
        self.assertEqual(rows, [])
        self.assertEqual(calls, [{"symbol": "ETH"}])


if __name__ == "__main__":
    unittest.main()
