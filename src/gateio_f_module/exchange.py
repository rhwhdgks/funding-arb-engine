import asyncio
import os
import time
from collections import defaultdict
from copy import deepcopy
from typing import Any, Optional

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
)

from .api import (
    sync_server_time,
    get_accounts,
    get_order,
    get_ticker_list,
    get_open_orders,
    post_limit_order,
    post_market_order,
    delete_order,
    cancel_all_orders,
    get_funding_rate,
    get_funding_rate_history,
    get_ohlcv_1m,
    set_leverage,
    set_margin_mode,
    get_my_trades,
)
from .websocket_manager import WebSocketManager


class Exchange:
    name = "gateioFuture"
    persist_name = "gateio"

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
        self.reconcile_status: dict[str, Any] = {
            "last_reason": "",
            "last_attempt_ms": 0,
            "last_success_ms": 0,
            "account_synced": False,
            "open_orders_synced": False,
            "funding_synced": False,
            "open_order_count": 0,
            "error": "",
            "last_auto_request_ms": 0,
            "last_auto_reason": "",
            "auto_trigger_count": 0,
            "guard_last_check_ms": 0,
            "guard_last_issue_ms": 0,
            "guard_last_reason": "",
            "guard_issue_count": 0,
            "last_periodic_request_ms": 0,
        }

        self.account_interval = float(os.getenv("GATEIO_ACCOUNT_INTERVAL_SEC", str(account_interval)))
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

        self.order_block_until_ms = 0
        self.order_block_reason = ""
        self.max_stale_orderbook_ms = int(os.getenv("MAX_STALE_ORDERBOOK_MS", str(self.stale_threshold_ms)))
        self.auto_reconcile_enabled = os.getenv("AUTO_RECONCILE_ON_WS_EVENTS", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        self.auto_reconcile_cooldown_ms = int(float(os.getenv("AUTO_RECONCILE_COOLDOWN_SEC", "5")) * 1000)
        self.reconcile_guard_interval_ms = int(float(os.getenv("RECONCILE_GUARD_INTERVAL_SEC", "2")) * 1000)
        self.periodic_reconcile_interval_ms = int(float(os.getenv("PERIODIC_RECONCILE_SEC", "60")) * 1000)
        self.account_stale_guard_ms = int(float(os.getenv("ACCOUNT_STALE_GUARD_SEC", "10")) * 1000)
        self.open_orders_stale_guard_ms = int(float(os.getenv("OPEN_ORDERS_STALE_GUARD_SEC", "75")) * 1000)
        self.waiting_order_snapshot_grace_ms = int(float(os.getenv("WAITING_ORDER_SNAPSHOT_GRACE_SEC", "10")) * 1000)

    async def run(self):
        if self._running:
            return

        print(f"[{self.name}] run start")

        await self.before_run()
        await self.bootstrap()
        await self.reconcile_state(reason="startup")

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

        print(f"[{self.name}] tasks created")

    async def stop(self):
        self._running = False

        if self.websocket is not None:
            if hasattr(self.websocket, "close"):
                try:
                    await self.websocket.close()
                except Exception:
                    pass
            elif hasattr(self.websocket, "stop"):
                try:
                    self.websocket.stop()
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
        try:
            offset = await sync_server_time()
            print(f"[{self.name}] time offset: {offset}")
        except Exception as e:
            print(f"[{self.name}][before_run][warn] sync_server_time failed: {e}")

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
                    "can_trade": False,
                    "block_reasons": ["ws_not_ready"],
                    "last_health_ts": 0,
                },
            )

    async def bootstrap_symbol(
        self,
        symbol: str,
        leverage: int | float | None = None,
        margin_mode: str | None = None,
    ):
        symbol = symbol.upper()
        result = {}

        normalized_mode = margin_mode.upper() if margin_mode else None

        # Gate에서는 CROSS = leverage 0 의미라서
        # positive leverage와 동시에 받으면 충돌로 본다.
        if normalized_mode in {"CROSS", "CROSSED"}:
            if leverage not in (None, 0):
                return {
                    "ok": False,
                    "symbol": symbol,
                    "error": "gate_cross_conflicts_with_positive_leverage",
                    "message": "Use leverage=0 for cross, or use positive leverage with margin_mode=None for isolated.",
                }
            result["margin_mode"] = await set_margin_mode(symbol, "CROSS")
            return result

        # isolated 쪽은 positive leverage만으로 표현
        if leverage is not None:
            result["leverage"] = await set_leverage(symbol, leverage)
            return result

        return {
            "ok": True,
            "symbol": symbol,
            "message": "No bootstrap action requested.",
        }

    async def reconcile_state(
        self,
        *,
        include_account: bool = True,
        include_open_orders: bool = True,
        include_funding: bool = False,
        reason: str = "manual",
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        summary: dict[str, Any] = {
            "last_reason": reason,
            "last_attempt_ms": now_ms,
            "last_success_ms": self.reconcile_status.get("last_success_ms", 0),
            "account_synced": False,
            "open_orders_synced": False,
            "funding_synced": False,
            "open_order_count": len(self.waiting_list),
            "error": "",
            "last_auto_request_ms": self.reconcile_status.get("last_auto_request_ms", 0),
            "last_auto_reason": self.reconcile_status.get("last_auto_reason", ""),
            "auto_trigger_count": self.reconcile_status.get("auto_trigger_count", 0),
            "guard_last_check_ms": self.reconcile_status.get("guard_last_check_ms", 0),
            "guard_last_issue_ms": self.reconcile_status.get("guard_last_issue_ms", 0),
            "guard_last_reason": self.reconcile_status.get("guard_last_reason", ""),
            "guard_issue_count": self.reconcile_status.get("guard_issue_count", 0),
            "last_periodic_request_ms": self.reconcile_status.get("last_periodic_request_ms", 0),
        }

        if include_account:
            try:
                account_data, asset_data = await get_accounts({"info": self.ticker_info})
                if account_data is not None and asset_data is not None:
                    self.update_account_state(account_data, asset_data)
                    self.account_snapshot_last_sync_ms = now_ms
                    summary["account_synced"] = True
            except Exception as exc:
                summary["error"] = str(exc)
                print(f"[{self.name}][reconcile][account] {exc}")

        if include_open_orders:
            try:
                order_list = await get_open_orders(option={"info": self.ticker_info})
                if order_list is not None:
                    self._apply_open_orders_reconcile(order_list)
                    self.open_orders_snapshot_last_sync_ms = now_ms
                    summary["open_orders_synced"] = True
                    summary["open_order_count"] = len(order_list)
            except Exception as exc:
                summary["error"] = summary["error"] or str(exc)
                print(f"[{self.name}][reconcile][open_orders] {exc}")

        if include_funding:
            funding_count = 0
            for ticker in self.ticker_list:
                try:
                    funding = await get_funding_rate(ticker.upper())
                    if funding is not None:
                        self.funding_now[ticker] = funding
                        self.funding_snapshot_last_sync_ms = now_ms
                        funding_count += 1
                except Exception as exc:
                    summary["error"] = summary["error"] or str(exc)
                    print(f"[{self.name}][reconcile][funding][{ticker}] {exc}")
            summary["funding_synced"] = funding_count > 0
            summary["funding_count"] = funding_count

        if summary["account_synced"] or summary["open_orders_synced"] or summary["funding_synced"]:
            summary["last_success_ms"] = now_ms

        self.reconcile_status = summary
        return deepcopy(summary)

    def _schedule_auto_reconcile(self, reason: str) -> bool:
        if not self.auto_reconcile_enabled:
            return False
        if self._reconcile_task is not None and not self._reconcile_task.done():
            return False

        now_ms = int(time.time() * 1000)
        last_success_ms = int(self.reconcile_status.get("last_success_ms", 0) or 0)
        if last_success_ms and (now_ms - last_success_ms) < self.auto_reconcile_cooldown_ms:
            return False

        last_auto_request_ms = int(self.reconcile_status.get("last_auto_request_ms", 0) or 0)
        if last_auto_request_ms and (now_ms - last_auto_request_ms) < self.auto_reconcile_cooldown_ms:
            return False

        self.reconcile_status["last_auto_request_ms"] = now_ms
        self.reconcile_status["last_auto_reason"] = reason
        self.reconcile_status["auto_trigger_count"] = int(self.reconcile_status.get("auto_trigger_count", 0) or 0) + 1

        async def _runner():
            try:
                await self.reconcile_state(
                    include_account=True,
                    include_open_orders=True,
                    include_funding=False,
                    reason=reason,
                )
            except Exception as exc:
                self.reconcile_status["error"] = str(exc)
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
        if "ticker" in event:
            ticker = event.get("ticker")
            if not ticker:
                return
            ticker = str(ticker).upper()

            now_ms = int(time.time() * 1000)
            desktop_time = event.get("desktop_time", now_ms)
            server_time = event.get("server_time", now_ms)
            event_time = event.get("event_time", server_time)
            source = event.get("source", "unknown")

            drift_ms = float(desktop_time - server_time)
            prev_avg = self.ws_drift_avg_ms.get(ticker)
            drift_avg = drift_ms if prev_avg is None else prev_avg * 0.9 + drift_ms * 0.1

            self.ws_last_update_ms[ticker] = now_ms
            self.ws_drift_last_ms[ticker] = drift_ms
            self.ws_drift_avg_ms[ticker] = drift_avg
            self.ws_source_last[ticker] = source
            self.ws_server_time_last_ms[ticker] = int(server_time or 0)
            self.ws_event_time_last_ms[ticker] = int(event_time or server_time or 0)
            self.ws_desktop_time_last_ms[ticker] = int(desktop_time or now_ms)

            current = self.market_state.get(ticker, {})
            merged = deepcopy(current)
            merged.update(event)
            merged["ticker"] = ticker
            self.market_state[ticker] = merged

            if source in {"bookTicker", "book_ticker", "tick", "futures.book_ticker"}:
                self.tick[ticker] = deepcopy(merged)
            else:
                self.ob[ticker] = deepcopy(merged)

            await self.broadcast_event(ticker, deepcopy(merged))
            return

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

            if source in {"bookTicker", "book_ticker", "tick", "futures.book_ticker"} or channel in {"book_ticker", "tick"}:
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
            except Exception as e:
                print(f"[{self.name}][run_account][error] {e}")

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
                order_list = await get_open_orders(option={"info": self.ticker_info})
                if order_list is None:
                    await asyncio.sleep(self.open_orders_interval)
                    continue

                self._apply_open_orders_reconcile(order_list)
                self.open_orders_snapshot_last_sync_ms = int(time.time() * 1000)
                self.reconcile_status["open_orders_synced"] = True
                self.reconcile_status["open_order_count"] = len(order_list)
                self.reconcile_status["last_success_ms"] = int(time.time() * 1000)
                self.reconcile_status["error"] = ""

            except Exception as e:
                print(f"[{self.name}][run_check_open_orders][error] {e}")
                self.reconcile_status["error"] = str(e)

            await asyncio.sleep(self.open_orders_interval)

    async def run_funding(self):
        while self._running:
            try:
                for ticker in self.ticker_list:
                    funding = await get_funding_rate(ticker.upper())
                    if funding is not None:
                        self.funding_now[ticker] = funding
                        self.funding_snapshot_last_sync_ms = int(time.time() * 1000)
            except Exception as e:
                print(f"[{self.name}][run_funding][error] {e}")

            await asyncio.sleep(self.funding_interval)

    async def run_health(self):
        while self._running:
            try:
                now_ms = int(time.time() * 1000)
                should_auto_reconcile_stale = False
                should_auto_reconcile_recovered = False

                for ticker in self.ticker_list:
                    prev_state = deepcopy(self.hedge_state.get(ticker, {}))
                    prev_can_trade = prev_state.get("can_trade")
                    prev_reasons = set(prev_state.get("block_reasons", []) or [])
                    last_update = self.ws_last_update_ms.get(ticker, 0)
                    stale_ms = now_ms - last_update if last_update else None
                    drift_ms = self.ws_drift_last_ms.get(ticker)
                    ws_server_time_last_ms = self.ws_server_time_last_ms.get(ticker)
                    ws_event_time_last_ms = self.ws_event_time_last_ms.get(ticker)
                    ws_desktop_time_last_ms = self.ws_desktop_time_last_ms.get(ticker)
                    event_lag_ms = None
                    transport_lag_ms = None
                    if ws_desktop_time_last_ms and ws_event_time_last_ms:
                        event_lag_ms = int(ws_desktop_time_last_ms) - int(ws_event_time_last_ms)
                    if ws_desktop_time_last_ms and ws_server_time_last_ms:
                        transport_lag_ms = int(ws_desktop_time_last_ms) - int(ws_server_time_last_ms)

                    can_trade = True
                    reasons = []

                    if not last_update:
                        can_trade = False
                        reasons.append("ws_not_ready")
                    else:
                        if drift_ms is not None and abs(drift_ms) > self.max_server_pc_drift_ms:
                            can_trade = False
                            reasons.append(f"drift>{self.max_server_pc_drift_ms}ms")

                        stale_limit_ms = getattr(self, "max_stale_orderbook_ms", self.stale_threshold_ms)
                        if stale_ms is not None and stale_ms > stale_limit_ms:
                            can_trade = False
                            reasons.append("stale_orderbook")

                    if now_ms < getattr(self, "order_block_until_ms", 0):
                        can_trade = False
                        reasons.append(getattr(self, "order_block_reason", "manual_block"))

                    if self.rate_limit_state.get("blocked", False):
                        can_trade = False
                        reasons.append(self.rate_limit_state.get("reason", "rate_limit_block"))

                    self.hedge_state.setdefault(ticker, {})
                    self.hedge_state[ticker]["can_trade"] = can_trade
                    self.hedge_state[ticker]["block_reasons"] = reasons
                    self.hedge_state[ticker]["last_health_ts"] = now_ms
                    self.hedge_state[ticker]["ws_drift_last_ms"] = drift_ms
                    self.hedge_state[ticker]["ws_drift_avg_ms"] = self.ws_drift_avg_ms.get(ticker)
                    self.hedge_state[ticker]["ws_last_update_ms"] = last_update
                    self.hedge_state[ticker]["ws_source_last"] = self.ws_source_last.get(ticker)
                    self.hedge_state[ticker]["market_source_last"] = self.market_state.get(ticker, {}).get("source")
                    self.hedge_state[ticker]["stale_age_ms"] = stale_ms
                    self.hedge_state[ticker]["ws_server_time_last_ms"] = ws_server_time_last_ms
                    self.hedge_state[ticker]["ws_event_time_last_ms"] = ws_event_time_last_ms
                    self.hedge_state[ticker]["ws_desktop_time_last_ms"] = ws_desktop_time_last_ms
                    self.hedge_state[ticker]["event_lag_ms"] = event_lag_ms
                    self.hedge_state[ticker]["transport_lag_ms"] = transport_lag_ms

                    has_ws_issue = any(reason in {"ws_not_ready", "stale_orderbook"} for reason in reasons)
                    prev_has_ws_issue = any(reason in {"ws_not_ready", "stale_orderbook"} for reason in prev_reasons)
                    if has_ws_issue:
                        should_auto_reconcile_stale = True
                    if prev_can_trade is False and can_trade and prev_has_ws_issue:
                        should_auto_reconcile_recovered = True

                    if (
                        not simple_stdout_enabled()
                        and (prev_can_trade != can_trade or prev_reasons != set(reasons))
                        and (reasons or prev_reasons)
                    ):
                        print(
                            f"[{self.name}][health][{ticker}] "
                            f"market_ready={can_trade} reasons={reasons or ['-']} "
                            f"market_source={self.market_state.get(ticker, {}).get('source') or '-'} "
                            f"ws_source={self.ws_source_last.get(ticker) or '-'} "
                            f"drift_ms={drift_ms} stale_age_ms={stale_ms} "
                            f"event_lag_ms={event_lag_ms} transport_lag_ms={transport_lag_ms} "
                            f"event_time_ms={ws_event_time_last_ms} "
                            f"server_time_ms={ws_server_time_last_ms} "
                            f"desktop_time_ms={ws_desktop_time_last_ms} "
                            f"local_update_ms={last_update}"
                        )

                if should_auto_reconcile_recovered:
                    self._schedule_auto_reconcile("ws_recovered")
                elif should_auto_reconcile_stale:
                    self._schedule_auto_reconcile("ws_stale")

            except Exception as e:
                print(f"[{self.name}][run_health][error] {e}")

            await asyncio.sleep(self.health_interval)

    async def run_reconcile_guard(self):
        sleep_sec = max(self.reconcile_guard_interval_ms / 1000.0, 0.5)
        while self._running:
            try:
                self._run_reconcile_guard_step()
            except Exception as exc:
                self.reconcile_status["error"] = str(exc)
                print(f"[{self.name}][run_reconcile_guard][error] {exc}")

            await asyncio.sleep(sleep_sec)

    def can_trade_symbol(self, ticker: str) -> bool:
        ticker = ticker.upper()
        state = self.hedge_state.get(ticker, {})
        return state.get("can_trade", True)

    async def order(self, ticker: str, side: str, amount: float, price: float, *, is_hedge: bool = False):
        ticker = ticker.upper()
        side = side.lower()
        request_payload = build_order_request_payload(ticker, side, amount, price=price, order_type="limit")
        request_payload.setdefault("client_order_id", f"gtl{ticker}{int(time.time() * 1000)}")
        request_payload.setdefault("uuid", request_payload["client_order_id"])
        if is_hedge:
            request_payload["is_hedge"] = True

        if not is_hedge:
            if not self.can_trade_symbol(ticker):
                result = {"error": "cannot_trade_symbol", "ticker": ticker}
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
        request_payload.setdefault("client_order_id", f"gtm{ticker}{int(time.time() * 1000)}")
        request_payload.setdefault("uuid", request_payload["client_order_id"])
        if is_hedge:
            request_payload["is_hedge"] = True

        if not self.can_trade_symbol(ticker) and not is_close and not is_hedge:
            result = {"error": "cannot_trade_symbol", "ticker": ticker}
            schedule_order_result(self.persist_name, ticker, result, request_payload, event_source="local")
            return result

        schedule_order_submit(self.persist_name, request_payload)
        order_data = await post_market_order(
            ticker,
            side,
            amount,
            {"info": self.ticker_info, "client_order_id": request_payload["client_order_id"]},
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

        result = await delete_order(uuid, {"info": self.ticker_info})
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
        schedule_cancel_all_submit(self.persist_name, request_payload)
        result = await cancel_all_orders(normalized_ticker)

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
        self._update_rate_limit_state(result)
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
        return await get_my_trades(ticker.upper() if ticker else None, limit=limit, option={"info": self.ticker_info})

    async def get_trade_history(self, ticker: str | None = None, limit: int = 100):
        return await self.get_trades(ticker=ticker, limit=limit)

    async def get_order_status(self, order: dict):
        uuid = order.get("uuid") or order.get("order_id")
        if not uuid:
            return None
        return await get_order(uuid, option={"info": self.ticker_info})

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

            ob_row = self.ob.get(ticker, {})
            entry["now_price"] = ob_row.get("bid", entry.get("markPrice", 0.0))
            entry["nowPrice"] = ob_row.get("bid", entry.get("markPrice", 0.0))

        for ticker, entry in account_dict.items():
            ob_row = self.ob.get(ticker, {})
            price = ob_row.get("ask", entry.get("markPrice", 0.0))
            qty = float(entry.get("balance", 0.0) or 0.0)

            if qty >= 0:
                long_usd += qty * price
            else:
                short_usd += qty * price

            self.position_snapshot[ticker] = {
                "qty": qty,
                "entry_price": float(entry.get("avg_price", entry.get("avgPrice", 0.0))),
                "mark_price": float(entry.get("markPrice", 0.0)),
                "unrealized_profit": float(entry.get("unrealized_profit", entry.get("unrealizedProfit", 0.0))),
                "position_side": entry.get("positionSide", entry.get("position_side", "BOTH")),
                "timestamp": timestamp,
            }
            self.net_exposure[ticker] = qty

        normalized_asset = {}
        for asset, data in (asset_data or {}).items():
            asset = str(asset).upper()
            normalized_asset[asset] = {
                "balance": data.get("balance", 0.0),
                "availableBalance": data.get("available", data.get("availableBalance", data.get("balance", 0.0))),
                "marginBalance": data.get(
                    "marginBalance",
                    data.get("balance", 0.0) + data.get("unrealized_profit", 0.0),
                ),
                "locked": data.get("locked", 0.0),
                "avgPrice": data.get("avgPrice", 1.0),
                "nowPrice": data.get("nowPrice", 1.0),
                "unrealizedProfit": data.get("unrealized_profit", data.get("unrealizedProfit", 0.0)),
                "timestamp": timestamp,
                "raw": data.get("raw"),
            }

        usdt = normalized_asset.get("USDT", {"balance": 0.0, "unrealizedProfit": 0.0})
        normalized_asset["USD"] = {
            "balance": usdt["balance"],
            "availableBalance": usdt.get("availableBalance", usdt["balance"]),
            "marginBalance": usdt.get("marginBalance", usdt["balance"]),
            "locked": usdt.get("locked", 0.0),
            "avgPrice": 1.0,
            "nowPrice": 1.0,
            "unrealizedProfit": usdt.get("unrealizedProfit", 0.0),
            "timestamp": timestamp,
        }

        total_balance = normalized_asset["USD"]["balance"] + normalized_asset["USD"]["unrealizedProfit"]
        total_position = long_usd + abs(short_usd)
        delta_position = long_usd + short_usd

        self.account_dict = account_dict
        self.asset_dict = normalized_asset
        self.total_balance = total_balance
        self.leverage = total_position / total_balance if total_balance else 0.0
        self.delta_leverage = delta_position / total_balance if total_balance else 0.0

    def default_dict(self, timestamp: int):
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
            "positionSide": "BOTH",
            "position_side": "BOTH",
            "timestamp": timestamp,
        }

    def check_order_status(self, order: dict, status: dict | None):
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

        if current_status.get("state") == "not_exist" or current_status.get("error") == "not_exist":
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
            retry_count = max(int(os.getenv("TRADE_HISTORY_SYNC_RETRY_COUNT", "4") or 4), 1)
            retry_delay_sec = max(float(os.getenv("TRADE_HISTORY_SYNC_RETRY_DELAY_SEC", "0.5") or 0.5), 0.0)
            for attempt in range(retry_count):
                trades = await self.get_trades(ticker, limit=self.market_order_followup_trade_limit)
                matched_count = schedule_matched_trade_history_upsert(
                    self.persist_name,
                    ticker,
                    order_payload,
                    trades or [],
                )
                if matched_count > 0:
                    return
                if attempt + 1 < retry_count and retry_delay_sec > 0:
                    await asyncio.sleep(retry_delay_sec)
            print(f"[{self.name}][trade_history_sync][warn] no matched trades ticker={ticker}")
        except Exception as exc:
            print(f"[{self.name}][trade_history_sync][error] {exc}")

    def handle_cancel_result(self, result: dict | None, uuid: str):
        if not result:
            return

        if result.get("state") == "not_exist" or result.get("error") == "not_exist":
            self._remove_waiting_order(uuid)
            self._forget_order_status(uuid)
            return

        self._remove_waiting_order(uuid)
        self._forget_order_status(uuid)

    def _apply_open_orders_reconcile(self, order_list: Optional[list[dict]]):
        self._update_open_orders_snapshot(order_list)
        reconciled_waiting_list: list[dict] = []
        seen_keys: set[str] = set()
        for row in order_list or []:
            order_key = self._order_key(row)
            if order_key in seen_keys:
                continue
            seen_keys.add(order_key)
            reconciled_waiting_list.append(row)
        self.waiting_list = reconciled_waiting_list
        self.order_status_cache = {}
        for row in reconciled_waiting_list:
            self._remember_order_status(row)

    def get_open_order_map(self) -> dict[str, list[dict]]:
        return self._get_merged_open_order_map()

    def get_account_snapshot(self) -> dict:
        return {
            "exchange": self.name,
            "total_balance": self.total_balance,
            "leverage": self.leverage,
            "delta_leverage": self.delta_leverage,
            "asset_dict": self.asset_dict,
            "account_dict": self.account_dict,
            "reconcile_status": deepcopy(self.reconcile_status),
        }

    def get_reconcile_snapshot(self) -> dict:
        return deepcopy(self.reconcile_status)

    def get_symbol_snapshot(self, ticker: str) -> dict:
        ticker = ticker.upper()
        open_order_map = self._get_merged_open_order_map()
        return {
            "ticker": ticker,
            "orderbook": self.ob.get(ticker, {}),
            "tick": self.tick.get(ticker, {}),
            "funding": self.funding_now.get(ticker, {}),
            "position": self.position_snapshot.get(ticker, {}),
            "account_last_sync_ms": int(self.account_snapshot_last_sync_ms or 0),
            "health": self.hedge_state.get(ticker, {}),
            "open_orders": open_order_map.get(ticker, []),
        }

    def block_orders(self, reason: str, duration_ms: int):
        now_ms = int(time.time() * 1000)
        self.order_block_until_ms = now_ms + duration_ms
        self.order_block_reason = reason

    def clear_order_block(self):
        self.order_block_until_ms = 0
        self.order_block_reason = ""

    def _update_rate_limit_state(self, response: Optional[dict]):
        if not response:
            return

        rate_limits = response.get("rate_limits")
        if not rate_limits:
            return

        self.rate_limit_state["last"] = rate_limits
        self.rate_limit_state["blocked"] = False
        self.rate_limit_state["reason"] = ""

        if isinstance(rate_limits, list):
            for item in rate_limits:
                if not isinstance(item, dict):
                    continue
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

    def get_rate_limit_state(self) -> dict[str, Any]:
        return deepcopy(self.rate_limit_state)

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
        for key in ("order_id", "uuid", "client_order_id", "identifier"):
            value = order.get(key)
            if value in (None, ""):
                continue
            normalized = str(value)
            if normalized not in identifiers:
                identifiers.append(normalized)
        return identifiers

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
        self.reconcile_status["guard_last_check_ms"] = now_ms

        issues = self._collect_reconcile_guard_issues(now_ms)
        if issues:
            self.reconcile_status["guard_last_issue_ms"] = now_ms
            self.reconcile_status["guard_last_reason"] = issues[0]
            self.reconcile_status["guard_issue_count"] = int(self.reconcile_status.get("guard_issue_count", 0) or 0) + len(issues)
            self._schedule_auto_reconcile(f"guard:{issues[0]}")
            return issues

        if self.periodic_reconcile_interval_ms > 0:
            last_periodic_request_ms = int(self.reconcile_status.get("last_periodic_request_ms", 0) or 0)
            if not last_periodic_request_ms or (now_ms - last_periodic_request_ms) >= self.periodic_reconcile_interval_ms:
                self.reconcile_status["last_periodic_request_ms"] = now_ms
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
                    return schedule_polled_order_status(
                        self.persist_name,
                        ticker,
                        order_data,
                        merged_status,
                        event_source="rest_poll_market",
                    )
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

    def get_health_snapshot(self) -> dict:
        return {
            "exchange": self.name,
            "rate_limit_state": self.rate_limit_state,
            "symbols": {
                ticker: {
                    "can_trade": self.hedge_state.get(ticker, {}).get("can_trade"),
                    "block_reasons": self.hedge_state.get(ticker, {}).get("block_reasons", []),
                    "last_health_ts": self.hedge_state.get(ticker, {}).get("last_health_ts"),
                    "ws_drift_last_ms": self.hedge_state.get(ticker, {}).get("ws_drift_last_ms"),
                    "ws_drift_avg_ms": self.hedge_state.get(ticker, {}).get("ws_drift_avg_ms"),
                    "ws_last_update_ms": self.hedge_state.get(ticker, {}).get("ws_last_update_ms"),
                    "ws_source_last": self.hedge_state.get(ticker, {}).get("ws_source_last"),
                    "market_source_last": self.hedge_state.get(ticker, {}).get("market_source_last"),
                    "stale_age_ms": self.hedge_state.get(ticker, {}).get("stale_age_ms"),
                    "ws_server_time_last_ms": self.hedge_state.get(ticker, {}).get("ws_server_time_last_ms"),
                    "ws_event_time_last_ms": self.hedge_state.get(ticker, {}).get("ws_event_time_last_ms"),
                    "ws_desktop_time_last_ms": self.hedge_state.get(ticker, {}).get("ws_desktop_time_last_ms"),
                    "event_lag_ms": self.hedge_state.get(ticker, {}).get("event_lag_ms"),
                    "transport_lag_ms": self.hedge_state.get(ticker, {}).get("transport_lag_ms"),
                }
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
                f"event_lag_ms={row.get('event_lag_ms')}, "
                f"transport_lag_ms={row.get('transport_lag_ms')}, "
                f"event_time_ms={row.get('ws_event_time_last_ms')}, "
                f"server_time_ms={row.get('ws_server_time_last_ms')}, "
                f"desktop_time_ms={row.get('ws_desktop_time_last_ms')}"
            )
