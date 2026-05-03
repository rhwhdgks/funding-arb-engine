import asyncio
import os
import time
from collections import defaultdict
from copy import deepcopy
from typing import Any, Optional
from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.event_hooks import (
    build_cancel_all_request_payload,
    build_cancel_request_payload,
    build_order_request_payload,
    schedule_cancel_all_result,
    schedule_cancel_terminal_event,
    schedule_cancel_all_submit,
    schedule_cancel_result,
    schedule_cancel_submit,
    schedule_absent_open_orders_cleanup,
    schedule_matched_trade_history_upsert,
    schedule_order_progress_from_trades,
    schedule_order_result,
    schedule_order_state_event,
    schedule_order_submit,
    schedule_polled_order_status,
    schedule_trade_history_upsert,
)

from .api import (
    cancel_all_orders,
    delete_order,
    get_accounts,
    get_funding_rate,
    get_funding_rate_history,
    get_margin_mode as api_get_margin_mode,
    get_my_trades,
    get_ohlcv_1m,
    get_open_orders,
    get_order,
    get_ticker_list,
    post_limit_order,
    post_market_order,
    set_leverage,
    sync_server_time,
)
from .websocket_manager import WebSocketManager


def simple_stdout_enabled() -> bool:
    return os.getenv("STRATEGY_STDOUT_LOG_STYLE", "verbose").strip().lower() == "simple"


class Exchange:
    name = "okxFuture"
    persist_name = "okx"

    def __init__(
        self,
        ticker_list: Optional[list[str]] = None,
        *,
        account_interval: float = 2.0,
        order_interval: float = 1.0,
        open_orders_interval: float = 30.0,
        funding_interval: float = 10.0,
        health_interval: float = 0.5,
        order_cooldown_sec: float = 1.0,
        cancel_cooldown_sec: float = 0.5,
        stale_threshold_ms: int = 1500,
        max_server_pc_drift_ms: int = 500,
    ):
        self.ticker_list = [t.upper() for t in (ticker_list or [])]
        self.ticker_info: dict[str, dict] = {}

        self.account_dict: dict[str, dict] = {}
        self.asset_dict: dict[str, dict] = {}
        self.total_balance: float = 0.0
        self.leverage: float = 0.0
        self.delta_leverage: float = 0.0
        self.position_snapshot: dict[str, dict] = {}
        self.net_exposure: dict[str, float] = {}
        self.funding_now: dict[str, dict] = {}
        self.funding_history: dict[str, list[dict]] = {}

        self.waiting_list: list[dict] = []
        self.order_status_cache: dict[str, dict] = {}
        self.last_order_time: dict[str, float] = {}
        self.last_delete_time: dict[str, float] = {}
        self.ob: dict[str, dict] = {}
        self.tick: dict[str, dict] = {}
        self.market_state: dict[str, dict] = {}
        self.open_orders_snapshot: dict[str, list[dict]] = defaultdict(list)

        self.open_orders_snapshot_last_sync_ms = 0
        self.account_snapshot_last_sync_ms = 0
        self.funding_snapshot_last_sync_ms = 0
        self.funding_snapshot_last_attempt_ms = 0

        self.ws_drift_last_ms: dict[str, float] = {}
        self.ws_drift_avg_ms: dict[str, float] = {}
        self.ws_last_update_ms: dict[str, int] = {}
        self.ws_source_last: dict[str, str] = {}
        self.ws_server_time_last_ms: dict[str, int] = {}
        self.ws_event_time_last_ms: dict[str, int] = {}
        self.ws_desktop_time_last_ms: dict[str, int] = {}
        self.max_server_pc_drift_ms = max_server_pc_drift_ms
        self.stale_threshold_ms = stale_threshold_ms

        self.hedge_state: dict[str, dict] = {}
        self.rate_limit_state: dict[str, Any] = {}

        # summary 자체를 여러 루프가 직접 수정하지 않도록 meta와 summary를 분리한다.
        self._summary_meta: dict[str, Any] = {
            "last_reason": "",
            "last_refresh_source": "",
            "last_manual_reconcile_reason": "",
            "last_attempt_ms": 0,
            "last_auto_request_ms": 0,
            "last_auto_reason": "",
            "auto_trigger_count": 0,
            "guard_last_check_ms": 0,
            "guard_last_issue_ms": 0,
            "guard_last_reason": "",
            "guard_issue_count": 0,
            "last_periodic_request_ms": 0,
            "last_error": "",
        }
        self.reconcile_status: dict[str, Any] = {}

        self.account_interval = float(os.getenv("OKX_ACCOUNT_INTERVAL_SEC", str(account_interval)))
        self.order_interval = order_interval
        self.open_orders_interval = open_orders_interval
        self.funding_interval = funding_interval
        self.health_interval = health_interval
        self.order_cooldown_sec = order_cooldown_sec
        self.cancel_cooldown_sec = cancel_cooldown_sec
        self.market_order_followup_delay_sec = float(os.getenv("MARKET_ORDER_FOLLOWUP_DELAY_SEC", "0.6"))
        self.market_order_followup_trade_limit = int(os.getenv("MARKET_ORDER_FOLLOWUP_TRADE_LIMIT", "20"))

        self._event_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.websocket = None
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._reconcile_task: asyncio.Task | None = None

        self.auto_reconcile_enabled = os.getenv("AUTO_RECONCILE_ON_WS_EVENTS", "1").strip().lower() in {
            "1", "true", "yes", "y", "on"
        }
        self.auto_reconcile_cooldown_ms = int(float(os.getenv("AUTO_RECONCILE_COOLDOWN_SEC", "5")) * 1000)
        self.reconcile_guard_interval_ms = int(float(os.getenv("RECONCILE_GUARD_INTERVAL_SEC", "2")) * 1000)
        self.periodic_reconcile_interval_ms = int(float(os.getenv("PERIODIC_RECONCILE_SEC", "60")) * 1000)
        self.account_stale_guard_ms = int(float(os.getenv("ACCOUNT_STALE_GUARD_SEC", "10")) * 1000)
        self.open_orders_stale_guard_ms = int(float(os.getenv("OPEN_ORDERS_STALE_GUARD_SEC", "75")) * 1000)
        self.waiting_order_snapshot_grace_ms = int(float(os.getenv("WAITING_ORDER_SNAPSHOT_GRACE_SEC", "10")) * 1000)

        self.reconcile_status = self._build_runtime_summary()

    def _validate_funding_snapshot(
        self,
        ticker: str,
        funding: dict[str, Any] | None,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(funding, dict):
            return None

        snapshot = deepcopy(funding)
        now_ms = now_ms or int(time.time() * 1000)
        ticker = ticker.upper()
        errors: list[str] = []

        rate = snapshot.get("funding_rate")
        next_funding_time = snapshot.get("next_funding_time")

        if rate is None:
            errors.append("missing_funding_rate")
        if not next_funding_time or int(next_funding_time) <= 0:
            errors.append("missing_next_funding_time")

        observed = [rate, next_funding_time]
        status = "valid" if not errors else (
            "partial" if any(value not in (None, "", 0, 0.0) for value in observed) else "invalid"
        )

        snapshot["exchange"] = snapshot.get("exchange") or self.name
        snapshot["symbol"] = ticker
        snapshot["is_valid"] = status == "valid"
        snapshot["validation_status"] = status
        snapshot["validation_errors"] = errors
        snapshot["validated_at_ms"] = now_ms
        return snapshot

    def _apply_funding_snapshot(
        self,
        ticker: str,
        funding: dict[str, Any] | None,
        *,
        source: str,
        now_ms: int | None = None,
    ) -> bool:
        now_ms = now_ms or int(time.time() * 1000)
        self.funding_snapshot_last_attempt_ms = now_ms
        snapshot = self._validate_funding_snapshot(ticker, funding, now_ms=now_ms)
        if snapshot is None:
            return False
        snapshot["sync_source"] = source
        self.funding_now[ticker.upper()] = snapshot
        if snapshot.get("is_valid"):
            self.funding_snapshot_last_sync_ms = now_ms
            return True
        return False

    def _build_funding_sync_summary(self) -> dict[str, Any]:
        valid_symbols: list[str] = []
        partial_symbols: list[str] = []
        invalid_symbols: list[str] = []

        for ticker in self.ticker_list:
            snapshot = self.funding_now.get(ticker)
            if not isinstance(snapshot, dict):
                invalid_symbols.append(ticker)
                continue

            status = str(snapshot.get("validation_status") or "").lower()
            if snapshot.get("is_valid"):
                valid_symbols.append(ticker)
            elif status == "partial":
                partial_symbols.append(ticker)
            else:
                invalid_symbols.append(ticker)

        return {
            "funding_synced": bool(valid_symbols),
            "funding_count": len(valid_symbols),
            "funding_valid_count": len(valid_symbols),
            "funding_partial_count": len(partial_symbols),
            "funding_invalid_count": len(invalid_symbols),
            "funding_problem_symbols": partial_symbols + invalid_symbols,
            "funding_last_attempt_ms": int(self.funding_snapshot_last_attempt_ms or 0),
            "funding_last_success_ms": int(self.funding_snapshot_last_sync_ms or 0),
        }
    def _build_runtime_summary(self) -> dict[str, Any]:
        funding_summary = self._build_funding_sync_summary()
        merged_open_orders = self._get_merged_open_order_map()

        account_synced = bool(self.account_snapshot_last_sync_ms)
        open_orders_synced = bool(self.open_orders_snapshot_last_sync_ms)

        last_success_ms = max(
            int(self.account_snapshot_last_sync_ms or 0),
            int(self.open_orders_snapshot_last_sync_ms or 0),
            int(funding_summary.get("funding_last_success_ms") or 0),
        )

        return {
            "last_reason": self._summary_meta.get("last_reason", ""),
            "last_refresh_source": self._summary_meta.get("last_refresh_source", ""),
            "last_manual_reconcile_reason": self._summary_meta.get("last_manual_reconcile_reason", ""),
            "last_attempt_ms": int(self._summary_meta.get("last_attempt_ms", 0) or 0),
            "last_success_ms": last_success_ms,
            "account_synced": account_synced,
            "open_orders_synced": open_orders_synced,
            "funding_synced": funding_summary["funding_synced"],
            "funding_count": funding_summary["funding_count"],
            "funding_valid_count": funding_summary["funding_valid_count"],
            "funding_partial_count": funding_summary["funding_partial_count"],
            "funding_invalid_count": funding_summary["funding_invalid_count"],
            "funding_problem_symbols": funding_summary["funding_problem_symbols"],
            "funding_last_attempt_ms": funding_summary["funding_last_attempt_ms"],
            "funding_last_success_ms": funding_summary["funding_last_success_ms"],
            "open_order_count": sum(len(rows) for rows in merged_open_orders.values()),
            "error": self._summary_meta.get("last_error", ""),
            "last_auto_request_ms": int(self._summary_meta.get("last_auto_request_ms", 0) or 0),
            "last_auto_reason": self._summary_meta.get("last_auto_reason", ""),
            "auto_trigger_count": int(self._summary_meta.get("auto_trigger_count", 0) or 0),
            "guard_last_check_ms": int(self._summary_meta.get("guard_last_check_ms", 0) or 0),
            "guard_last_issue_ms": int(self._summary_meta.get("guard_last_issue_ms", 0) or 0),
            "guard_last_reason": self._summary_meta.get("guard_last_reason", ""),
            "guard_issue_count": int(self._summary_meta.get("guard_issue_count", 0) or 0),
            "last_periodic_request_ms": int(self._summary_meta.get("last_periodic_request_ms", 0) or 0),
        }


    def _refresh_runtime_summary(
        self,
        *,
        refresh_source: str | None = None,
        error: str | None = None,
        touch_attempt: bool = True,
    ) -> dict[str, Any]:
        if refresh_source is not None:
            self._summary_meta["last_refresh_source"] = refresh_source
            self._summary_meta["last_reason"] = refresh_source  # report 호환용
        if touch_attempt:
            self._summary_meta["last_attempt_ms"] = int(time.time() * 1000)
        if error is not None:
            self._summary_meta["last_error"] = error

        self.reconcile_status = self._build_runtime_summary()
        return deepcopy(self.reconcile_status)


    def get_symbol_funding_state(self, ticker: str) -> dict[str, Any]:
        ticker = ticker.upper()
        snapshot = deepcopy(self.funding_now.get(ticker, {}) or {})
        if snapshot:
            return snapshot
        return {
            "exchange": self.name,
            "symbol": ticker,
            "is_valid": False,
            "validation_status": "missing",
            "validation_errors": ["funding_missing"],
            "validated_at_ms": 0,
        }


    def _build_symbol_health_row(self, ticker: str, now_ms: int | None = None) -> dict[str, Any]:
        ticker = ticker.upper()
        now_ms = now_ms or int(time.time() * 1000)

        raw_health = deepcopy(self.hedge_state.get(ticker, {}) or {})
        market_ready = bool(raw_health.get("market_ready", raw_health.get("can_trade", False)))
        market_block_reasons = list(raw_health.get("market_block_reasons", raw_health.get("block_reasons", []) or []))

        account_last_sync_ms = int(self.account_snapshot_last_sync_ms or 0)
        account_ready = bool(account_last_sync_ms) and (now_ms - account_last_sync_ms) <= self.account_stale_guard_ms
        if not account_last_sync_ms:
            account_reason = "account_snapshot_never_synced"
        elif account_ready:
            account_reason = "ok"
        else:
            account_reason = "account_snapshot_stale"

        funding_state = self.get_symbol_funding_state(ticker)
        funding_ready = bool(funding_state.get("is_valid"))
        funding_status = str(funding_state.get("validation_status") or "missing").lower()
        funding_errors = list(funding_state.get("validation_errors", []) or [])
        funding_reason = ",".join(funding_errors) if funding_errors else ("ok" if funding_ready else funding_status)

        ws_server_time_last_ms = raw_health.get("ws_server_time_last_ms")
        ws_event_time_last_ms = raw_health.get("ws_event_time_last_ms")
        ws_desktop_time_last_ms = raw_health.get("ws_desktop_time_last_ms")
        event_lag_ms = None
        transport_lag_ms = None
        if ws_desktop_time_last_ms and ws_event_time_last_ms:
            event_lag_ms = int(ws_desktop_time_last_ms) - int(ws_event_time_last_ms)
        if ws_desktop_time_last_ms and ws_server_time_last_ms:
            transport_lag_ms = int(ws_desktop_time_last_ms) - int(ws_server_time_last_ms)

        return {
            "market_ready": market_ready,
            "market_block_reasons": market_block_reasons,
            "can_trade": market_ready,
            "block_reasons": market_block_reasons,
            "last_health_ts": raw_health.get("last_health_ts"),
            "ws_drift_last_ms": raw_health.get("ws_drift_last_ms"),
            "ws_drift_avg_ms": raw_health.get("ws_drift_avg_ms"),
            "ws_last_update_ms": raw_health.get("ws_last_update_ms"),
            "stale_age_ms": raw_health.get("stale_age_ms"),
            "ws_source_last": raw_health.get("ws_source_last"),
            "market_source_last": raw_health.get("market_source_last"),
            "ws_server_time_last_ms": ws_server_time_last_ms,
            "ws_event_time_last_ms": ws_event_time_last_ms,
            "ws_desktop_time_last_ms": ws_desktop_time_last_ms,
            "event_lag_ms": event_lag_ms,
            "transport_lag_ms": transport_lag_ms,
            "account_ready": account_ready,
            "account_sync_ok": account_ready,
            "account_reason": account_reason,
            "account_sync_reason": account_reason,
            "funding_ready": funding_ready,
            "funding_sync_ok": funding_ready,
            "funding_status": funding_status,
            "funding_errors": funding_errors,
            "funding_reason": funding_reason,
            "funding_sync_reason": funding_reason,
        }

    
    async def run(self):
        if self._running:
            return

        if not simple_stdout_enabled():
            print(f"[{self.name}] run start")

        await self.before_run()
        await self.bootstrap()
        await self.reconcile_state(reason="startup", include_funding=True)

        self._running = True
        self._tasks = [
            asyncio.create_task(self.run_account(), name=f"{self.name}-run_account"),
            asyncio.create_task(self.run_order(), name=f"{self.name}-run_order"),
            asyncio.create_task(self.run_public(), name=f"{self.name}-run_public"),
            asyncio.create_task(self.run_check_open_orders(), name=f"{self.name}-run_check_open_orders"),
            asyncio.create_task(self.run_funding(), name=f"{self.name}-run_funding"),
            asyncio.create_task(self.run_health(), name=f"{self.name}-run_health"),
            asyncio.create_task(self.run_reconcile_guard(), name=f"{self.name}-run_reconcile_guard"),
        ]

        if not simple_stdout_enabled():
            print(f"[{self.name}] tasks created")

    async def stop(self):
        self._running = False

        if self.websocket is not None and hasattr(self.websocket, "close"):
            try:
                await self.websocket.close()
            except Exception:
                pass

        for task in self._tasks:
            task.cancel()

        if self._reconcile_task is not None:
            self._reconcile_task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._reconcile_task is not None:
            await asyncio.gather(self._reconcile_task, return_exceptions=True)

        self._tasks.clear()
        self._reconcile_task = None

    async def before_run(self):
        await sync_server_time()

    async def bootstrap(self):
        result = await get_ticker_list()
        if result is None:
            ticker_list, ticker_info = [], {}
        else:
            ticker_list, ticker_info = result

        self.ticker_info = ticker_info or {}

        if not self.ticker_list:
            self.ticker_list = [t.upper() for t in (ticker_list or [])]

        for ticker in self.ticker_list:
            self.hedge_state.setdefault(
                ticker,
                {
                    "market_ready": False,
                    "market_block_reasons": ["ws_not_ready"],
                    "can_trade": False,
                    "block_reasons": ["ws_not_ready"],
                    "last_health_ts": 0,
                },
            )

        self.reconcile_status = self._build_runtime_summary()

    async def bootstrap_symbol(self, symbol: str, leverage: int = 3, margin_mode: str = "CROSS"):
        symbol = symbol.upper()
        normalized_mode = (margin_mode or "CROSS").upper()
        if normalized_mode == "CROSSED":
            normalized_mode = "CROSS"
        return await set_leverage(symbol, leverage, normalized_mode.lower())

    async def get_margin_mode(self, symbol: str, expected_mode: str | None = None) -> dict[str, Any]:
        return await api_get_margin_mode(symbol, expected_mode or "cross")

    async def reconcile_state(
        self,
        *,
        include_account: bool = True,
        include_open_orders: bool = True,
        include_funding: bool = False,
        reason: str = "manual",
    ) -> dict[str, Any]:
        attempt_ms = int(time.time() * 1000)
        account_refreshed = False
        open_orders_refreshed = False
        funding_refreshed = False
        first_error = ""

        self._summary_meta["last_manual_reconcile_reason"] = reason
        self._summary_meta["last_reason"] = reason
        self._summary_meta["last_attempt_ms"] = attempt_ms

        if include_account:
            try:
                account_data, asset_data = await get_accounts({"info": self.ticker_info})
                if account_data is not None and asset_data is not None:
                    self.update_account_state(account_data, asset_data)
                    self.account_snapshot_last_sync_ms = attempt_ms
                    account_refreshed = True
            except Exception as exc:
                first_error = first_error or str(exc)
                print(f"[{self.name}][reconcile][account] {exc}")

        if include_open_orders:
            try:
                order_list = await get_open_orders({"info": self.ticker_info})
                if order_list is not None:
                    self._apply_open_orders_reconcile(order_list)
                    self.open_orders_snapshot_last_sync_ms = attempt_ms
                    open_orders_refreshed = True
            except Exception as exc:
                first_error = first_error or str(exc)
                print(f"[{self.name}][reconcile][open_orders] {exc}")

        if include_funding:
            any_funding_seen = False
            for ticker in self.ticker_list:
                try:
                    funding = await get_funding_rate(ticker)
                    applied = self._apply_funding_snapshot(
                        ticker,
                        funding,
                        source=f"reconcile:{reason}",
                        now_ms=attempt_ms,
                    )
                    any_funding_seen = any_funding_seen or bool(applied or funding)
                except Exception as exc:
                    first_error = first_error or str(exc)
                    print(f"[{self.name}][reconcile][funding][{ticker}] {exc}")
            funding_refreshed = any_funding_seen

        self._refresh_runtime_summary(
        refresh_source=f"reconcile:{reason}",
        error=first_error,
        touch_attempt=False,
    )

        return {
            "reason": reason,
            "attempt_ms": attempt_ms,
            "account_refreshed": account_refreshed,
            "open_orders_refreshed": open_orders_refreshed,
            "funding_refreshed": funding_refreshed,
            "error": first_error,
            "runtime_summary": deepcopy(self.reconcile_status),
        }

    def _schedule_auto_reconcile(self, reason: str) -> bool:
        if not self.auto_reconcile_enabled:
            return False
        if self._reconcile_task is not None and not self._reconcile_task.done():
            return False

        current_summary = self._build_runtime_summary()
        now_ms = int(time.time() * 1000)

        last_success_ms = int(current_summary.get("last_success_ms", 0) or 0)
        if last_success_ms and (now_ms - last_success_ms) < self.auto_reconcile_cooldown_ms:
            return False

        last_auto_request_ms = int(self._summary_meta.get("last_auto_request_ms", 0) or 0)
        if last_auto_request_ms and (now_ms - last_auto_request_ms) < self.auto_reconcile_cooldown_ms:
            return False

        self._summary_meta["last_auto_request_ms"] = now_ms
        self._summary_meta["last_auto_reason"] = reason
        self._summary_meta["auto_trigger_count"] = int(self._summary_meta.get("auto_trigger_count", 0) or 0) + 1
        self._refresh_runtime_summary(refresh_source=reason, error=self._summary_meta.get("last_error", ""), touch_attempt=False)

        async def _runner():
            try:
                await self.reconcile_state(
                    include_account=True,
                    include_open_orders=True,
                    include_funding=False,
                    reason=reason,
                )
            except Exception as exc:
                self._refresh_runtime_summary(refresh_source=reason, error=str(exc))
                print(f"[{self.name}][auto_reconcile][error] {exc}")

        self._reconcile_task = asyncio.create_task(_runner(), name=f"{self.name}-auto-reconcile")
        return True

    def register_listener(self, code: str) -> asyncio.Queue:
        return self._event_queues[code.upper()]

    async def emit(self, code: str, data: dict):
        await self._event_queues[code.upper()].put(data)

    async def broadcast_event(self, code: str, data: dict):
        await self.emit(code, data)

    async def websocket_callback(self, event: dict):
        now_ms = int(time.time() * 1000)
        channel = event.get("_channel", "unknown")

        for code, data in event.items():
            if code.startswith("_"):
                continue
            if not isinstance(data, dict):
                continue

            code = str(code).upper()

            server_time = data.get("server_time", now_ms)
            desktop_time = data.get("desktop_time", now_ms)
            event_time = data.get("event_time", server_time)
            source = data.get("source", channel)
            is_prefill = str(source).startswith("rest_prefill")

            if not is_prefill:
                drift_ms = float(desktop_time - server_time)
                prev_avg = self.ws_drift_avg_ms.get(code)
                drift_avg = drift_ms if prev_avg is None else prev_avg * 0.9 + drift_ms * 0.1

                self.ws_last_update_ms[code] = now_ms
                self.ws_drift_last_ms[code] = drift_ms
                self.ws_drift_avg_ms[code] = drift_avg
                self.ws_source_last[code] = source
                self.ws_server_time_last_ms[code] = int(server_time or 0)
                self.ws_event_time_last_ms[code] = int(event_time or server_time or 0)
                self.ws_desktop_time_last_ms[code] = int(desktop_time or now_ms)

            current = self.market_state.get(code, {})
            merged = deepcopy(current)
            merged.update(data)
            merged["ticker"] = code
            self.market_state[code] = merged

            if source in {"tickers", "book_ticker", "tick"} or channel in {"book_ticker", "tick"}:
                self.tick[code] = deepcopy(merged)
            else:
                self.ob[code] = deepcopy(merged)

            await self.broadcast_event(code, deepcopy(merged))

    async def run_public(self):
        if self.websocket is None:
            self.websocket = WebSocketManager(self.ticker_info)
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    async def run_account(self):
        while self._running:
            try:
                account_data, asset_data = await get_accounts({"info": self.ticker_info})
                if account_data is not None and asset_data is not None:
                    self.update_account_state(account_data, asset_data)
                    self.account_snapshot_last_sync_ms = int(time.time() * 1000)
                    self._refresh_runtime_summary(refresh_source="run_account", error="", touch_attempt=False)
            except Exception as e:
                print(f"[{self.name}][run_account][error] {e}")
                self._refresh_runtime_summary(refresh_source="run_account", error=str(e), touch_attempt=False)

            await asyncio.sleep(self.account_interval)

    async def run_order(self):
        while self._running:
            try:
                copy_list = deepcopy(self.waiting_list)

                for order in copy_list:
                    status = await self.get_order_status(order)
                    self.check_order_status(order, status)

            except Exception as e:
                print(f"[{self.name}][run_order][error] {e}")

            await asyncio.sleep(self.order_interval)

    async def run_check_open_orders(self):
        while self._running:
            try:
                order_list = await get_open_orders({"info": self.ticker_info})
                if order_list is None:
                    await asyncio.sleep(self.open_orders_interval)
                    continue

                disappeared_orders = self._apply_open_orders_reconcile(order_list)
                if disappeared_orders:
                    await self._finalize_disappeared_orders(disappeared_orders)
                self.open_orders_snapshot_last_sync_ms = int(time.time() * 1000)
                self._refresh_runtime_summary(refresh_source="run_check_open_orders", error="", touch_attempt=False)

            except Exception as e:
                print(f"[{self.name}][run_check_open_orders][error] {e}")
                self._refresh_runtime_summary(refresh_source="run_check_open_orders", error=str(e), touch_attempt=False)

            await asyncio.sleep(self.open_orders_interval)

    async def run_funding(self):
        while self._running:
            try:
                now_ms = int(time.time() * 1000)
                for ticker in self.ticker_list:
                    funding = await get_funding_rate(ticker)
                    self._apply_funding_snapshot(ticker, funding, source="run_funding", now_ms=now_ms)
                self._refresh_runtime_summary(refresh_source="run_funding", error="", touch_attempt=False)
            except Exception as e:
                print(f"[{self.name}][run_funding][error] {e}")
                self._refresh_runtime_summary(refresh_source="run_funding", error=str(e), touch_attempt=False)

            await asyncio.sleep(self.funding_interval)

    async def run_health(self):
        while self._running:
            try:
                now_ms = int(time.time() * 1000)
                should_auto_reconcile_stale = False
                should_auto_reconcile_recovered = False

                for ticker in self.ticker_list:
                    prev_state = deepcopy(self.hedge_state.get(ticker, {}))
                    prev_market_ready = bool(prev_state.get("market_ready", prev_state.get("can_trade", False)))
                    prev_reasons = set(prev_state.get("market_block_reasons", prev_state.get("block_reasons", []) or []))

                    last_update = self.ws_last_update_ms.get(ticker, 0)
                    stale_ms = now_ms - last_update if last_update else None
                    drift_ms = self.ws_drift_last_ms.get(ticker)
                    market_source = self.market_state.get(ticker, {}).get("source")
                    ws_server_time_last_ms = self.ws_server_time_last_ms.get(ticker)
                    ws_event_time_last_ms = self.ws_event_time_last_ms.get(ticker)
                    ws_desktop_time_last_ms = self.ws_desktop_time_last_ms.get(ticker)
                    event_lag_ms = None
                    transport_lag_ms = None
                    if ws_desktop_time_last_ms and ws_event_time_last_ms:
                        event_lag_ms = int(ws_desktop_time_last_ms) - int(ws_event_time_last_ms)
                    if ws_desktop_time_last_ms and ws_server_time_last_ms:
                        transport_lag_ms = int(ws_desktop_time_last_ms) - int(ws_server_time_last_ms)

                    market_ready = True
                    reasons: list[str] = []

                    if not last_update:
                        market_ready = False
                        if isinstance(market_source, str) and market_source.startswith("rest_prefill"):
                            reasons.append("prefill_only")
                        reasons.append("ws_not_ready")
                    else:
                        if drift_ms is not None and abs(drift_ms) > self.max_server_pc_drift_ms:
                            market_ready = False
                            reasons.append(f"drift>{self.max_server_pc_drift_ms}ms")

                        if stale_ms is not None and stale_ms > self.stale_threshold_ms:
                            market_ready = False
                            reasons.append("stale_orderbook")

                    if self.rate_limit_state.get("blocked", False):
                        market_ready = False
                        reasons.append(self.rate_limit_state.get("reason", "rate_limit_block"))

                    self.hedge_state[ticker] = {
                        "market_ready": market_ready,
                        "market_block_reasons": reasons,
                        # strategy 호환용 alias
                        "can_trade": market_ready,
                        "block_reasons": reasons,
                        "last_health_ts": now_ms,
                        "ws_drift_last_ms": drift_ms,
                        "ws_drift_avg_ms": self.ws_drift_avg_ms.get(ticker),
                        "ws_last_update_ms": last_update,
                        "ws_source_last": self.ws_source_last.get(ticker),
                        "market_source_last": market_source,
                        "stale_age_ms": stale_ms,
                        "ws_server_time_last_ms": ws_server_time_last_ms,
                        "ws_event_time_last_ms": ws_event_time_last_ms,
                        "ws_desktop_time_last_ms": ws_desktop_time_last_ms,
                        "event_lag_ms": event_lag_ms,
                        "transport_lag_ms": transport_lag_ms,
                    }

                    has_ws_issue = any(reason in {"ws_not_ready", "stale_orderbook"} for reason in reasons)
                    prev_has_ws_issue = any(reason in {"ws_not_ready", "stale_orderbook"} for reason in prev_reasons)

                    if has_ws_issue:
                        should_auto_reconcile_stale = True
                    if (not prev_market_ready) and market_ready and prev_has_ws_issue:
                        should_auto_reconcile_recovered = True

                    if (
                        not simple_stdout_enabled()
                        and (prev_market_ready != market_ready or prev_reasons != set(reasons))
                        and (reasons or prev_reasons)
                    ):
                        print(
                            f"[{self.name}][health][{ticker}] "
                            f"market_ready={market_ready} reasons={reasons or ['-']} "
                            f"market_source={market_source or '-'} ws_source={self.ws_source_last.get(ticker) or '-'} "
                            f"drift_ms={drift_ms} stale_age_ms={stale_ms} "
                            f"event_lag_ms={event_lag_ms} transport_lag_ms={transport_lag_ms} "
                            f"event_time_ms={self.ws_event_time_last_ms.get(ticker)} "
                            f"server_time_ms={self.ws_server_time_last_ms.get(ticker)} "
                            f"desktop_time_ms={self.ws_desktop_time_last_ms.get(ticker)} "
                            f"local_update_ms={last_update}"
                        )

                if should_auto_reconcile_recovered:
                    self._schedule_auto_reconcile("ws_recovered")
                elif should_auto_reconcile_stale:
                    self._schedule_auto_reconcile("ws_stale")

            except Exception as e:
                print(f"[{self.name}][run_health][error] {e}")
                self._refresh_runtime_summary(refresh_source="run_health", error=str(e), touch_attempt=False)

            await asyncio.sleep(self.health_interval)

    async def run_reconcile_guard(self):
        sleep_sec = max(self.reconcile_guard_interval_ms / 1000.0, 0.5)
        while self._running:
            try:
                self._run_reconcile_guard_step()
            except Exception as exc:
                self._refresh_runtime_summary(refresh_source="run_reconcile_guard", error=str(exc), touch_attempt=False)
                print(f"[{self.name}][run_reconcile_guard][error] {exc}")

            await asyncio.sleep(sleep_sec)
    def can_submit_order(self, ticker: str, *, require_funding: bool = False) -> tuple[bool, list[str]]:
        ticker = ticker.upper()
        snapshot = self.get_symbol_snapshot(ticker)
        health = snapshot.get("health", {}) or {}

        reasons: list[str] = []

        if not snapshot.get("market_ready", False):
            market_reasons = list(health.get("block_reasons", []) or [])
            if market_reasons:
                reasons.extend([f"market:{reason}" for reason in market_reasons])
            else:
                reasons.append("market:not_ready")

        if not snapshot.get("account_ready", False):
            reasons.append(f"account:{health.get('account_sync_reason') or 'not_ready'}")

        if require_funding and not snapshot.get("funding_ready", False):
            reasons.append(f"funding:{health.get('funding_sync_reason') or 'not_ready'}")

        deduped_reasons: list[str] = []
        seen: set[str] = set()
        for reason in reasons:
            if reason not in seen:
                seen.add(reason)
                deduped_reasons.append(reason)

        return (len(deduped_reasons) == 0), deduped_reasons
    def can_trade_symbol(self, ticker: str) -> bool:
        can_submit, _ = self.can_submit_order(ticker, require_funding=False)
        return can_submit

    async def order(self, ticker: str, side: str, amount: float, price: float, *, is_hedge: bool = False):
        ticker = ticker.upper()
        side = side.lower()
        request_payload = build_order_request_payload(ticker, side, amount, price=price, order_type="limit")
        request_payload.setdefault("client_order_id", f"okxl{ticker}{int(time.time() * 1000)}")
        request_payload.setdefault("uuid", request_payload["client_order_id"])
        if is_hedge:
            request_payload["is_hedge"] = True
            snapshot = self.get_symbol_snapshot(ticker)
            if not snapshot.get("account_ready", False):
                result = {
                    "error": "cannot_submit_order",
                    "ticker": ticker,
                    "gate_reasons": [f"account:{snapshot.get('health', {}).get('account_sync_reason') or 'not_ready'}"],
                }
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

        if not is_hedge:
            can_submit, gate_reasons = self.can_submit_order(ticker, require_funding=False)
            if not can_submit:
                result = {
                    "error": "cannot_submit_order",
                    "ticker": ticker,
                    "gate_reasons": gate_reasons,
                }
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

            key = f"{ticker}_{side}"
            if time.time() - self.last_order_time.get(key, 0) < self.order_cooldown_sec:
                result = {"error": "order_cooldown", "ticker": ticker}
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

            self.last_order_time[key] = time.time()
        schedule_order_submit(self.persist_name, request_payload)

        order_data = await post_limit_order(
            ticker,
            side,
            amount,
            price,
            {"info": self.ticker_info, "client_order_id": request_payload["client_order_id"]},
        )

        if order_data and "error" not in order_data:
            self._replace_waiting_order(order_data)
            self._remember_order_status(order_data)

        schedule_order_result(self.persist_name, ticker, order_data, request_payload)
        schedule_order_state_event(self.persist_name, ticker, order_data, prefer_live=True, event_source="rest_result")
        self._update_rate_limit_state(order_data)
        return order_data

    async def market_order(self, ticker: str, side: str, amount: float, is_close: bool = False, *, is_hedge: bool = False):
        ticker = ticker.upper()
        side = side.lower()
        request_payload = build_order_request_payload(ticker, side, amount, order_type="market", is_close=is_close)
        request_payload.setdefault("client_order_id", f"okxm{ticker}{int(time.time() * 1000)}")
        request_payload.setdefault("uuid", request_payload["client_order_id"])
        if is_hedge:
            request_payload["is_hedge"] = True
            snapshot = self.get_symbol_snapshot(ticker)
            if not snapshot.get("account_ready", False):
                result = {
                    "error": "cannot_submit_order",
                    "ticker": ticker,
                    "gate_reasons": [f"account:{snapshot.get('health', {}).get('account_sync_reason') or 'not_ready'}"],
                }
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

        if not is_close and not is_hedge:
            can_submit, gate_reasons = self.can_submit_order(ticker, require_funding=False)
            if not can_submit:
                result = {
                    "error": "cannot_submit_order",
                    "ticker": ticker,
                    "gate_reasons": gate_reasons,
                }
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

            key = f"{ticker}_{side}"
            if time.time() - self.last_order_time.get(key, 0) < self.order_cooldown_sec:
                result = {"error": "order_cooldown", "ticker": ticker}
                schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
                return result

            self.last_order_time[key] = time.time()

        schedule_order_submit(self.persist_name, request_payload)
        order_data = await post_market_order(
            ticker,
            side,
            amount,
            {
                "info": self.ticker_info,
                "client_order_id": request_payload["client_order_id"],
                "reduce_only": bool(is_close),
            },
        )

        schedule_order_result(self.persist_name, ticker, order_data, request_payload)
        schedule_order_state_event(self.persist_name, ticker, order_data, prefer_live=False, event_source="rest_result")
        await self._finalize_market_order_lifecycle(ticker, order_data)
        self._update_rate_limit_state(order_data)
        return order_data

    async def cancel_order(self, ticker: str, uuid: str):
        ticker = ticker.upper()
        request_payload = build_cancel_request_payload(ticker, uuid)

        if time.time() - self.last_delete_time.get(ticker, 0) < self.cancel_cooldown_sec:
            result = {"error": "cancel_cooldown", "ticker": ticker, "uuid": uuid}
            schedule_cancel_result(self.persist_name, ticker, result, request_payload, event_source="local")
            return result

        self.last_delete_time[ticker] = time.time()
        schedule_cancel_submit(self.persist_name, request_payload)

        result = await delete_order(
            uuid,
            {
                "ticker": ticker,
                "symbol": ticker,
                "client_order_id": uuid,
                "lookup_by": "client_order_id",
            },
        )
        schedule_cancel_result(self.persist_name, ticker, result, request_payload)
        cached_order = self._get_cached_order_status({"uuid": uuid, "client_order_id": uuid, "order_id": uuid, "ticker": ticker, "symbol": ticker})
        schedule_cancel_terminal_event(
            self.persist_name,
            ticker,
            result,
            request_payload,
            order_payload=cached_order,
            event_source="rest_cancel",
        )
        self.handle_cancel_result(result, uuid)
        self._update_rate_limit_state(result)
        return result

    async def cancel_all_orders_for_symbol(self, ticker: Optional[str] = None):
        normalized_ticker = ticker.upper() if ticker is not None else None
        request_payload = build_cancel_all_request_payload(normalized_ticker)

        option = {}
        if normalized_ticker is not None:
            option["ticker"] = normalized_ticker
            option["symbol"] = normalized_ticker

        schedule_cancel_all_submit(self.persist_name, request_payload)
        result = await cancel_all_orders(option)

        if result and result.get("ok"):
            if normalized_ticker:
                removed_orders = [o for o in self.waiting_list if (o.get("ticker") or o.get("symbol")) == normalized_ticker]
                self.waiting_list = [o for o in self.waiting_list if (o.get("ticker") or o.get("symbol")) != normalized_ticker]
                self.open_orders_snapshot.pop(normalized_ticker, None)
                for row in removed_orders:
                    canceled_payload = deepcopy(row)
                    canceled_payload["status"] = "CANCELED"
                    canceled_payload["state"] = "not_exist"
                    canceled_payload["timestamp"] = int(time.time() * 1000)
                    canceled_payload["cancel_all"] = True
                    schedule_order_state_event(self.persist_name, normalized_ticker, canceled_payload, event_source="rest_cancel_all")
                    self._forget_order_status(row)
            else:
                removed_orders = list(self.waiting_list)
                self.waiting_list = []
                self.open_orders_snapshot = defaultdict(list)
                for row in removed_orders:
                    row_ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
                    canceled_payload = deepcopy(row)
                    canceled_payload["status"] = "CANCELED"
                    canceled_payload["state"] = "not_exist"
                    canceled_payload["timestamp"] = int(time.time() * 1000)
                    canceled_payload["cancel_all"] = True
                    schedule_order_state_event(self.persist_name, row_ticker, canceled_payload, event_source="rest_cancel_all")
                self.order_status_cache = {}
            schedule_absent_open_orders_cleanup(self.persist_name, normalized_ticker or "ALL", int(time.time() * 1000))

        schedule_cancel_all_result(self.persist_name, normalized_ticker, result, request_payload)
        return result

    async def preload_funding_history(self, ticker: str, limit: int = 30):
        ticker = ticker.upper()
        history = await get_funding_rate_history(ticker, limit=limit)
        if history is not None:
            self.funding_history[ticker] = history
        return history

    async def get_ohlcv_snapshot(self, ticker: str, limit: int = 120):
        return await get_ohlcv_1m(ticker.upper(), limit=limit)

    async def get_trades(self, ticker: str | None = None, limit: int = 100):
        return await get_my_trades(ticker.upper() if ticker else None, limit=limit)

    async def get_trade_history(self, ticker: str | None = None, limit: int = 100):
        return await self.get_trades(ticker=ticker, limit=limit)

    async def get_order_status(self, order: dict):
        ticker = (order.get("ticker") or order.get("symbol") or "").upper()
        uuid = order.get("uuid") or order.get("client_order_id") or order.get("order_id")

        if not uuid:
            return None

        return await get_order(
            uuid,
            {
                "ticker": ticker,
                "symbol": ticker,
                "client_order_id": uuid,
                "lookup_by": "client_order_id",
                "info": self.ticker_info,
            },
        )

    def default_dict(self, timestamp: int) -> dict:
        return {
            "balance": 0.0,
            "locked": 0.0,
            "avgPrice": 0.0,
            "avg_price": 0.0,
            "nowPrice": 0.0,
            "now_price": 0.0,
            "unrealizedProfit": 0.0,
            "unrealized_profit": 0.0,
            "markPrice": 0.0,
            "positionSide": "NET",
            "timestamp": timestamp,
        }

    def update_account_state(self, account_data: dict, asset_data: dict):
        timestamp = int(time.time() * 1000)
        long_usd = 0.0
        short_usd = 0.0

        account_dict = {ticker: self.default_dict(timestamp) for ticker in self.ticker_list}

        for ticker, data in (account_data or {}).items():
            ticker = str(ticker).upper()
            if ticker not in self.ticker_list:
                continue

            entry = account_dict[ticker]
            entry.update(data)

            entry["nowPrice"] = self.ob.get(ticker, {}).get("bid", 0.0)

        for ticker, entry in account_dict.items():
            price = self.ob.get(ticker, {}).get("ask", 0.0)
            qty = float(entry.get("balance", 0.0) or 0.0)

            if qty >= 0:
                long_usd += qty * price
            else:
                short_usd += qty * price

            self.position_snapshot[ticker] = {
                "qty": qty,
                "entry_price": entry.get("avg_price", entry.get("avgPrice", 0.0)),
                "mark_price": entry.get("markPrice", 0.0),
                "unrealized_profit": entry.get("unrealized_profit", entry.get("unrealizedProfit", 0.0)),
                "position_side": entry.get("positionSide", "NET"),
                "timestamp": timestamp,
            }
            self.net_exposure[ticker] = qty

        parsed_asset_dict = {
            str(ticker).upper(): {
                "balance": data.get("balance", 0.0),
                "availableBalance": data.get("availableBalance", data.get("available", 0.0)),
                "marginBalance": data.get("marginBalance", data.get("balance", 0.0)),
                "locked": data.get("locked", 0.0),
                "avgPrice": data.get("avgPrice", data.get("avg_price", 0.0)),
                "nowPrice": self.ob.get(str(ticker).upper(), {}).get("bid", 0.0),
                "unrealizedProfit": data.get("unrealizedProfit", data.get("unrealized_profit", 0.0)),
                "timestamp": timestamp,
            }
            for ticker, data in (asset_data or {}).items()
        }

        usdt = parsed_asset_dict.get("USDT", {"balance": 0.0, "unrealizedProfit": 0.0})
        parsed_asset_dict["USD"] = {
            "balance": usdt["balance"],
            "locked": 0.0,
            "avgPrice": 1.0,
            "nowPrice": 1.0,
            "unrealizedProfit": usdt.get("unrealizedProfit", 0.0),
            "timestamp": timestamp,
        }

        total_balance = parsed_asset_dict["USD"]["balance"] + parsed_asset_dict["USD"]["unrealizedProfit"]
        total_position = long_usd + abs(short_usd)
        delta_position = long_usd + short_usd

        self.account_dict = account_dict
        self.asset_dict = parsed_asset_dict
        self.total_balance = total_balance
        self.leverage = total_position / total_balance if total_balance else 0.0
        self.delta_leverage = delta_position / total_balance if total_balance else 0.0

    def check_order_status(self, order: dict, status: Optional[dict]):
        if not status:
            return

        current_status = self._merge_polled_status(order, status)
        previous_status = self._get_cached_order_status(order)
        ticker = (current_status.get("ticker") or current_status.get("symbol") or "").upper()
        polled_event = schedule_polled_order_status(
            self.persist_name,
            ticker,
            previous_status,
            current_status,
            event_source="rest_poll",
        )
        if polled_event in {"partial_fill", "fill"}:
            self._schedule_trade_history_sync(ticker, current_status)

        state = current_status.get("state")
        error = current_status.get("error")

        if state == "not_exist" or error == "not_exist":
            self._remove_waiting_order(order)
            self._forget_order_status(order)
            return

        latest_status = str(current_status.get("status", "")).upper()
        if latest_status in {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED"}:
            self._remove_waiting_order(order)
            self._forget_order_status(order)
            return

        self._replace_waiting_order(current_status)
        self._remember_order_status(current_status)

    def _schedule_trade_history_sync(self, ticker: str, order_payload: dict | None):
        if not ticker or not isinstance(order_payload, dict) or not order_payload:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._sync_trade_history_for_order(ticker, order_payload))
        except RuntimeError:
            return

    async def _sync_trade_history_for_order(self, ticker: str, order_payload: dict | None):
        if not ticker or not isinstance(order_payload, dict) or not order_payload:
            return
        try:
            # OKX fills can lag the order status endpoint by several seconds,
            # especially on testnet. Keep this async so strategy execution is not blocked.
            retry_count = max(int(os.getenv("TRADE_HISTORY_SYNC_RETRY_COUNT", "12") or 12), 1)
            retry_delay_sec = max(float(os.getenv("TRADE_HISTORY_SYNC_RETRY_DELAY_SEC", "1.0") or 1.0), 0.0)
            last_trades: list[dict] = []
            for attempt in range(retry_count):
                trades = await self.get_trades(ticker, limit=self.market_order_followup_trade_limit)
                last_trades = [trade for trade in (trades or []) if isinstance(trade, dict)]
                matched_trades = self._matched_trades_for_order_payload(order_payload, last_trades)
                matched_count = schedule_matched_trade_history_upsert(
                    self.persist_name,
                    ticker,
                    order_payload,
                    last_trades,
                )
                if matched_count > 0:
                    order_executed_qty = self._order_status_executed_qty(order_payload)
                    matched_qty = self._matched_trade_qty(matched_trades)
                    if order_executed_qty <= 1e-12 or matched_qty + 1e-9 >= order_executed_qty:
                        return
                    if attempt + 1 < retry_count:
                        if retry_delay_sec > 0:
                            await asyncio.sleep(retry_delay_sec)
                        continue
                    print(
                        f"[{self.name}][trade_history_sync][warn] fills partial gap unresolved "
                        f"ticker={ticker} matched_qty={matched_qty} executed_qty={order_executed_qty} "
                        f"attempts={retry_count}"
                    )
                    return
                if attempt + 1 < retry_count and retry_delay_sec > 0:
                    await asyncio.sleep(retry_delay_sec)
            order_refs = self._order_identifiers(order_payload)
            trade_refs = []
            for trade in last_trades[:5]:
                refs = self._order_identifiers(trade)
                if refs:
                    trade_refs.append("/".join(refs))
            print(
                f"[{self.name}][trade_history_sync][warn] no matched trades "
                f"ticker={ticker} attempts={retry_count} delay_sec={retry_delay_sec:g} "
                f"order_refs={order_refs or '-'} recent_trade_refs={trade_refs or '-'}"
            )
        except Exception as exc:
            print(f"[{self.name}][trade_history_sync][error] {exc}")

    def handle_cancel_result(self, result: Optional[dict], uuid: str):
        if not result:
            return

        if result.get("error") == "not_exist":
            self._remove_waiting_order(uuid)
            self._forget_order_status(uuid)
            return

        self._remove_waiting_order(uuid)
        self._forget_order_status(uuid)

    def _apply_open_orders_reconcile(self, order_list: Optional[list[dict]]) -> list[dict]:
        previous_waiting_by_key: dict[str, dict] = {}
        for row in self.waiting_list:
            order_key = self._order_key(row)
            if not order_key or order_key in previous_waiting_by_key:
                continue
            previous_waiting_by_key[order_key] = deepcopy(row)

        self._update_open_orders_snapshot(order_list)
        reconciled_waiting_list: list[dict] = []
        seen_keys: set[str] = set()
        for row in order_list or []:
            order_key = self._order_key(row)
            if order_key in seen_keys:
                continue
            seen_keys.add(order_key)
            reconciled_waiting_list.append(row)
        disappeared_orders = [
            row
            for order_key, row in previous_waiting_by_key.items()
            if order_key not in seen_keys
        ]
        self.waiting_list = reconciled_waiting_list
        self.order_status_cache = {}
        for row in reconciled_waiting_list:
            self._remember_order_status(row)
        return disappeared_orders

    async def _finalize_disappeared_orders(self, orders: list[dict]):
        for order in orders:
            try:
                await self._finalize_disappeared_order(order)
            except Exception as exc:
                ticker = str(order.get("ticker") or order.get("symbol") or "").upper()
                order_key = self._order_key(order)
                print(
                    f"[{self.name}][open_orders_reconcile][error] "
                    f"ticker={ticker or '-'} order_key={order_key or '-'} error={exc}"
                )

    async def _finalize_disappeared_order(self, order: dict):
        if not isinstance(order, dict) or not order:
            return None

        ticker = str(order.get("ticker") or order.get("symbol") or "").upper()
        if not ticker:
            return None

        order_key = self._order_key(order)
        previous_status = self._get_cached_order_status(order)
        current_status: dict | None = None

        try:
            polled = await self.get_order_status(order)
            if polled:
                current_status = self._merge_polled_status(order, polled)
                latest_status = str(current_status.get("status", "")).upper()
                latest_state = str(current_status.get("state", "")).lower()

                is_open_like = latest_status in {"NEW", "OPEN", "LIVE", "PARTIALLY_FILLED"}
                if latest_state == "exist" or (is_open_like and latest_state != "not_exist"):
                    self._replace_waiting_order(current_status)
                    self._remember_order_status(current_status)
                    print(
                        f"[{self.name}][open_orders_reconcile][restore_live] "
                        f"ticker={ticker} order_key={order_key or '-'} "
                        f"status={latest_status or '-'} state={latest_state or '-'}"
                    )
                    return None

                polled_event = schedule_polled_order_status(
                    self.persist_name,
                    ticker,
                    previous_status,
                    current_status,
                    event_source="open_orders_reconcile",
                )
                if polled_event in {"partial_fill", "fill"}:
                    self._schedule_trade_history_sync(ticker, current_status)
                if polled_event in {"fill", "order_canceled", "order_rejected", "order_expired", "order_missing"}:
                    self._forget_order_status(order)
                    print(
                        f"[{self.name}][open_orders_reconcile][finalized] "
                        f"ticker={ticker} order_key={order_key or '-'} event={polled_event}"
                    )
                    return polled_event
        except Exception as exc:
            print(
                f"[{self.name}][open_orders_reconcile][status_error] "
                f"ticker={ticker} order_key={order_key or '-'} error={exc}"
            )

        try:
            trades = await self.get_trades(ticker, limit=self.market_order_followup_trade_limit)
            progress_event, progress_payload = schedule_order_progress_from_trades(
                self.persist_name,
                ticker,
                order,
                trades or [],
                event_source="open_orders_reconcile",
            )
            if progress_event == "fill":
                self._forget_order_status(order)
                print(
                    f"[{self.name}][open_orders_reconcile][finalized] "
                    f"ticker={ticker} order_key={order_key or '-'} event=fill"
                )
                return progress_event
            if progress_event == "partial_fill" and isinstance(progress_payload, dict):
                terminal_payload = deepcopy(progress_payload)
                terminal_payload["status"] = "CANCELED"
                terminal_payload["state"] = "not_exist"
                terminal_payload["event_reason"] = "open_orders_reconcile_partial_close"
                terminal_event = schedule_order_state_event(
                    self.persist_name,
                    ticker,
                    terminal_payload,
                    event_source="open_orders_reconcile",
                )
                self._forget_order_status(order)
                print(
                    f"[{self.name}][open_orders_reconcile][finalized] "
                    f"ticker={ticker} order_key={order_key or '-'} event={terminal_event or 'order_canceled'}"
                )
                return terminal_event or "order_canceled"
        except Exception as exc:
            print(
                f"[{self.name}][open_orders_reconcile][trade_error] "
                f"ticker={ticker} order_key={order_key or '-'} error={exc}"
            )

        missing_payload = deepcopy(current_status or previous_status or order)
        missing_payload["state"] = "not_exist"
        missing_payload.setdefault("timestamp", int(time.time() * 1000))
        missing_event = schedule_order_state_event(
            self.persist_name,
            ticker,
            missing_payload,
            event_source="open_orders_reconcile",
        )
        self._forget_order_status(order)
        print(
            f"[{self.name}][open_orders_reconcile][finalized] "
            f"ticker={ticker} order_key={order_key or '-'} event={missing_event or 'order_missing'}"
        )
        return missing_event or "order_missing"

    def _update_rate_limit_state(self, response: Optional[dict]):
        if not response:
            return

        rate_limits = response.get("rate_limits")
        if not rate_limits:
            return

        self.rate_limit_state["last"] = rate_limits
        self.rate_limit_state["blocked"] = False
        self.rate_limit_state["reason"] = ""

        for item in rate_limits:
            count = item.get("count")
            limit = item.get("limit")
            interval = item.get("interval")
            interval_num = item.get("intervalNum")

            if count is None or limit in (None, 0):
                continue

            usage = count / limit
            if usage >= 0.9:
                self.rate_limit_state["blocked"] = True
                self.rate_limit_state["reason"] = f"rate_limit_{interval}_{interval_num}"
                break

    def _order_key(self, order: dict) -> str:
        return str(
            order.get("order_id")
            or order.get("uuid")
            or order.get("client_order_id")
            or order.get("identifier")
        )

    def _order_identifiers(self, order: dict | None) -> list[str]:
        identifiers: list[str] = []
        if not isinstance(order, dict):
            return identifiers
        for key in ("order_id", "uuid", "client_order_id", "order_uid", "identifier"):
            value = order.get(key)
            if value in (None, ""):
                continue
            normalized = str(value)
            if normalized not in identifiers:
                identifiers.append(normalized)
        return identifiers

    def _matched_trades_for_order_payload(
        self,
        order_payload: dict | None,
        trades: list[dict] | None,
    ) -> list[dict]:
        order_refs = set(self._order_identifiers(order_payload))
        if not order_refs:
            return []
        matched: list[dict] = []
        for trade in trades or []:
            if not isinstance(trade, dict):
                continue
            trade_refs = set(self._order_identifiers(trade))
            if order_refs & trade_refs:
                matched.append(trade)
        return matched

    def _matched_trade_qty(self, trades: list[dict] | None) -> float:
        total = 0.0
        seen: set[str] = set()
        for trade in trades or []:
            if not isinstance(trade, dict):
                continue
            trade_key = str(
                trade.get("trade_id")
                or trade.get("uuid")
                or trade.get("identifier")
                or f"{trade.get('order_id')}:{trade.get('timestamp')}:{trade.get('qty')}"
            )
            if trade_key in seen:
                continue
            seen.add(trade_key)
            total += max(self._first_float(trade.get("qty")), 0.0)
        return total

    def _order_status_executed_qty(self, order_payload: dict | None) -> float:
        if not isinstance(order_payload, dict):
            return 0.0
        executed_qty = self._first_float(
            order_payload.get("executed_qty"),
            order_payload.get("filled_qty"),
            order_payload.get("filled"),
        )
        order_qty = self._first_float(order_payload.get("qty"), order_payload.get("amount"))
        left_qty = self._first_float(order_payload.get("left_qty"), order_payload.get("remaining"), default=-1.0)
        if executed_qty <= 1e-12 and order_qty > 1e-12 and 0 <= left_qty < order_qty:
            executed_qty = max(order_qty - left_qty, 0.0)
        return max(executed_qty, 0.0)

    def _first_float(self, *values: Any, default: float = 0.0) -> float:
        for value in values:
            try:
                if value in (None, ""):
                    continue
                return float(value)
            except Exception:
                continue
        return default

    def _first_int(self, *values: Any, default: int = 0) -> int:
        for value in values:
            try:
                if value in (None, ""):
                    continue
                return int(float(value))
            except Exception:
                continue
        return default

    def _get_cached_order_status(self, order: dict) -> dict:
        for identifier in self._order_identifiers(order):
            cached = self.order_status_cache.get(identifier)
            if isinstance(cached, dict):
                return deepcopy(cached)
        return deepcopy(order or {})

    def _collect_reconcile_guard_issues(self, now_ms: int | None = None) -> list[str]:
        now_ms = now_ms or int(time.time() * 1000)
        issues: list[str] = []

        account_last_sync_ms = int(self.account_snapshot_last_sync_ms or 0)
        if account_last_sync_ms and (now_ms - account_last_sync_ms) > self.account_stale_guard_ms:
            issues.append("account_snapshot_stale")

        open_orders_last_sync_ms = int(self.open_orders_snapshot_last_sync_ms or 0)
        if self.waiting_list:
            if not open_orders_last_sync_ms:
                issues.append("open_orders_snapshot_never_synced")
            elif (now_ms - open_orders_last_sync_ms) > self.open_orders_stale_guard_ms:
                issues.append("open_orders_snapshot_stale")

        waiting_keys = [self._order_key(row) for row in self.waiting_list if self._order_key(row)]
        if len(waiting_keys) != len(set(waiting_keys)):
            issues.append("duplicate_waiting_keys")

        snapshot_keys_by_symbol: dict[str, set[str]] = defaultdict(set)
        for ticker, rows in (self.open_orders_snapshot or {}).items():
            normalized_ticker = str(ticker).upper()
            for row in rows or []:
                order_key = self._order_key(row)
                if order_key:
                    snapshot_keys_by_symbol[normalized_ticker].add(order_key)

        for row in self.waiting_list:
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            order_key = self._order_key(row)
            if not ticker or not order_key:
                continue
            order_ts = row.get("timestamp") or row.get("create_time") or row.get("time")
            try:
                order_age_ms = now_ms - int(float(order_ts))
            except Exception:
                order_age_ms = self.waiting_order_snapshot_grace_ms
            if order_age_ms < self.waiting_order_snapshot_grace_ms:
                continue
            if order_key not in snapshot_keys_by_symbol.get(ticker, set()):
                issues.append(f"waiting_missing_from_snapshot:{ticker}:{order_key}")
                break

        return issues

    def _run_reconcile_guard_step(self, now_ms: int | None = None) -> list[str]:
        now_ms = now_ms or int(time.time() * 1000)
        self._summary_meta["guard_last_check_ms"] = now_ms

        issues = self._collect_reconcile_guard_issues(now_ms)
        if issues:
            self._summary_meta["guard_last_issue_ms"] = now_ms
            self._summary_meta["guard_last_reason"] = issues[0]
            self._summary_meta["guard_issue_count"] = int(self._summary_meta.get("guard_issue_count", 0) or 0) + len(issues)
            self._refresh_runtime_summary(refresh_source=f"guard:{issues[0]}", error=self._summary_meta.get("last_error", ""), touch_attempt=False)
            self._schedule_auto_reconcile(f"guard:{issues[0]}")
            return issues

        if self.periodic_reconcile_interval_ms > 0:
            last_periodic_request_ms = int(self._summary_meta.get("last_periodic_request_ms", 0) or 0)
            if not last_periodic_request_ms or (now_ms - last_periodic_request_ms) >= self.periodic_reconcile_interval_ms:
                self._summary_meta["last_periodic_request_ms"] = now_ms
                self._refresh_runtime_summary(refresh_source="periodic_guard", error=self._summary_meta.get("last_error", ""), touch_attempt=False)
                self._schedule_auto_reconcile("periodic_guard")

        return issues

    async def _finalize_market_order_lifecycle(self, ticker: str, order_data: dict | None):
        if not isinstance(order_data, dict) or not order_data or order_data.get("error"):
            return None

        if self.market_order_followup_delay_sec > 0:
            await asyncio.sleep(self.market_order_followup_delay_sec)

        merged_status: dict | None = None
        try:
            current_status = await self.get_order_status(order_data)
            if current_status:
                merged_status = self._merge_polled_status(order_data, current_status)
                latest_status = str(merged_status.get("status", "")).upper()
                latest_state = str(merged_status.get("state", "")).lower()
                if latest_state == "exist" or latest_status in {"NEW", "OPEN", "LIVE", "PARTIALLY_FILLED"}:
                    emitted = schedule_polled_order_status(
                        self.persist_name,
                        ticker,
                        order_data,
                        merged_status,
                        event_source="rest_poll_market",
                    )
                    self._replace_waiting_order(merged_status)
                    self._remember_order_status(merged_status)
                    return emitted
                if latest_status in {"FILLED", "FINISHED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                    emitted = schedule_polled_order_status(
                        self.persist_name,
                        ticker,
                        order_data,
                        merged_status,
                        event_source="rest_poll_market",
                    )

                    if latest_status in {"FILLED", "FINISHED"}:
                        self._schedule_trade_history_sync(ticker, merged_status)

                    return emitted
        except Exception as exc:
            print(f"[{self.name}][market_followup][status][error] {exc}")

        try:
            trades = await self.get_trades(ticker, limit=self.market_order_followup_trade_limit)
            progress_event, progress_payload = schedule_order_progress_from_trades(
                self.persist_name,
                ticker,
                order_data,
                trades or [],
                event_source="trade_poll_market",
            )
            if progress_event == "partial_fill" and isinstance(progress_payload, dict):
                self._replace_waiting_order(progress_payload)
                self._remember_order_status(progress_payload)
                return progress_event
            if progress_event is not None:
                return progress_event
        except Exception as exc:
            print(f"[{self.name}][market_followup][trades][error] {exc}")

        if merged_status is not None:
            return schedule_polled_order_status(
                self.persist_name,
                ticker,
                order_data,
                merged_status,
                event_source="rest_poll_market",
            )
        return None

    def _remember_order_status(self, order: dict | None):
        if not isinstance(order, dict) or not order:
            return
        snapshot = deepcopy(order)
        for identifier in self._order_identifiers(snapshot):
            self.order_status_cache[identifier] = snapshot

    def _forget_order_status(self, order_or_id: dict | str | None):
        identifiers: set[str] = set()
        if isinstance(order_or_id, dict):
            identifiers.update(self._order_identifiers(order_or_id))
            for identifier in list(identifiers):
                cached = self.order_status_cache.get(identifier)
                if isinstance(cached, dict):
                    identifiers.update(self._order_identifiers(cached))
        elif order_or_id not in (None, ""):
            normalized = str(order_or_id)
            identifiers.add(normalized)
            cached = self.order_status_cache.get(normalized)
            if isinstance(cached, dict):
                identifiers.update(self._order_identifiers(cached))

        for identifier in identifiers:
            self.order_status_cache.pop(identifier, None)

    def _replace_waiting_order(self, order: dict):
        order_key = self._order_key(order)
        self.waiting_list = [row for row in self.waiting_list if self._order_key(row) != order_key]
        self.waiting_list.append(order)

    def _remove_waiting_order(self, order_or_id: dict | str | None):
        identifiers: set[str] = set()
        if isinstance(order_or_id, dict):
            identifiers.update(self._order_identifiers(order_or_id))
        elif order_or_id not in (None, ""):
            identifiers.add(str(order_or_id))
            cached = self.order_status_cache.get(str(order_or_id))
            if isinstance(cached, dict):
                identifiers.update(self._order_identifiers(cached))

        self.waiting_list = [
            row
            for row in self.waiting_list
            if identifiers.isdisjoint(self._order_identifiers(row))
        ]
        if identifiers:
            pruned_snapshot: dict[str, list[dict]] = defaultdict(list)
            for ticker, rows in (self.open_orders_snapshot or {}).items():
                kept_rows = [
                    row
                    for row in (rows or [])
                    if identifiers.isdisjoint(self._order_identifiers(row))
                ]
                if kept_rows:
                    pruned_snapshot[str(ticker).upper()] = kept_rows
            self.open_orders_snapshot = pruned_snapshot

    def _merge_polled_status(self, order: dict, status: dict) -> dict:
        merged = deepcopy(order or {})
        merged.update(status or {})
        ticker = str(merged.get("ticker") or merged.get("symbol") or "").upper()
        if ticker:
            merged["ticker"] = ticker
            merged["symbol"] = ticker
        merged.setdefault("timestamp", int(time.time() * 1000))
        return merged

    def _update_open_orders_snapshot(self, order_list: Optional[list[dict]]):
        grouped = defaultdict(list)
        for row in order_list or []:
            ticker = row.get("ticker") or row.get("symbol")
            if ticker:
                grouped[str(ticker).upper()].append(row)
        self.open_orders_snapshot = grouped

    def _get_merged_open_order_map(self) -> dict[str, list[dict]]:
        merged: dict[str, dict[str, dict]] = defaultdict(dict)

        for ticker, rows in (self.open_orders_snapshot or {}).items():
            normalized_ticker = str(ticker).upper()
            for row in rows or []:
                merged[normalized_ticker][self._order_key(row)] = row

        for row in self.waiting_list:
            ticker = row.get("ticker") or row.get("symbol")
            if not ticker:
                continue
            normalized_ticker = str(ticker).upper()
            merged[normalized_ticker][self._order_key(row)] = row

        return {
            ticker: list(order_map.values())
            for ticker, order_map in merged.items()
        }

    def get_open_order_map(self) -> dict[str, list[dict]]:
        return self._get_merged_open_order_map()

    def get_account_snapshot(self) -> dict:
        self.reconcile_status = self._build_runtime_summary()
        return {
            "exchange": self.name,
            "total_balance": self.total_balance,
            "leverage": self.leverage,
            "delta_leverage": self.delta_leverage,
            "asset_dict": self.asset_dict,
            "reconcile_status": deepcopy(self.reconcile_status),
        }

    def get_reconcile_snapshot(self) -> dict:
        return self._build_runtime_summary()

    def get_symbol_snapshot(self, ticker: str) -> dict:
        ticker = ticker.upper()
        open_order_map = self._get_merged_open_order_map()
        funding_state = self.get_symbol_funding_state(ticker)
        health_row = self._build_symbol_health_row(ticker)

        return {
            "ticker": ticker,
            "orderbook": self.ob.get(ticker, {}),
            "tick": self.tick.get(ticker, {}),
            "funding": funding_state,
            "position": self.position_snapshot.get(ticker, {}),
            "account_last_sync_ms": int(self.account_snapshot_last_sync_ms or 0),
            "health": health_row,
            "open_orders": open_order_map.get(ticker, []),
            # 상위 레이어가 내부 구현을 추측하지 않도록 파생 상태를 같이 준다.
            "market_ready": health_row["market_ready"],
            "account_ready": health_row["account_ready"],
            "funding_ready": health_row["funding_ready"],
            "funding_status": health_row["funding_status"],
            "funding_errors": health_row["funding_errors"],
        }
    
    def get_balance_report(self) -> dict[str, Any]:
        account_snapshot = self.get_account_snapshot()
        symbol_snapshots = {
            ticker: self.get_symbol_snapshot(ticker)
            for ticker in self.ticker_list
        }
        return build_balance_report(self.persist_name, account_snapshot, symbol_snapshots)


    def print_balance_report(self):
        report = self.get_balance_report()

        print(f"[BALANCE][{report['exchange']}]")
        print(f"  exchange_wallet_balance_usd : {report['exchange_wallet_balance_usd']:.8f}")
        print(f"  available_balance_usd       : {report['available_balance_usd']:.8f}")
        print(f"  margin_balance_usd          : {report['margin_balance_usd']:.8f}")
        print(f"  unrealized_profit_usd       : {report['unrealized_profit_usd']:.8f}")
        print(f"  actual_equity_usd           : {report['actual_equity_usd']:.8f}")
        print(f"  total_balance_usd           : {report['total_balance_usd']:.8f}")
        print(f"  long_notional_usd           : {report['long_notional_usd']:.8f}")
        print(f"  short_notional_usd          : {report['short_notional_usd']:.8f}")
        print(f"  gross_notional_usd          : {report['gross_notional_usd']:.8f}")
        print(f"  net_notional_usd            : {report['net_notional_usd']:.8f}")
        print(f"  leverage                    : {report['leverage']:.8f}")
        print(f"  delta_leverage              : {report['delta_leverage']:.8f}")

        if not report["positions"]:
            print("  positions                   : []")
            return

        print("  positions:")
        for row in report["positions"]:
            print(
                f"    - {row['ticker']}: qty={row['qty']}, "
                f"entry={row['entry_price']}, mark={row['mark_price']}, "
                f"notional_mark_usd={row['notional_mark_usd']:.8f}, "
                f"upnl={row['unrealized_profit_usd']:.8f}, "
                f"side={row['position_side']}"
            )
    def get_health_snapshot(self) -> dict:
        now_ms = int(time.time() * 1000)
        return {
            "exchange": self.name,
            "rate_limit_state": self.rate_limit_state,
            "symbols": {
                ticker: self._build_symbol_health_row(ticker, now_ms=now_ms)
                for ticker in self.ticker_list
            },
        }

    def print_health_snapshot(self):
        snap = self.get_health_snapshot()
        print(f"[HEALTH][{snap['exchange']}]")
        print(f"  rate_limit_state: {snap['rate_limit_state']}")
        for ticker, row in snap["symbols"].items():
            print(
                f"  - {ticker}: can_trade={row['can_trade']}, "
                f"block_reasons={row['block_reasons']}, "
                f"drift_last_ms={row['ws_drift_last_ms']}, "
                f"drift_avg_ms={row['ws_drift_avg_ms']}, "
                f"ws_last_update_ms={row['ws_last_update_ms']}, "
                f"stale_age_ms={row.get('stale_age_ms')}, "
                f"ws_source={row.get('ws_source_last')}, "
                f"market_source={row.get('market_source_last')}, "
                f"event_time_ms={row.get('ws_event_time_last_ms')}, "
                f"server_time_ms={row.get('ws_server_time_last_ms')}"
            )
