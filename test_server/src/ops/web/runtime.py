import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.live_report import build_exchange_report_from_state
from src.ops.persistence.sync import SUPPORTED_EXCHANGES, build_default_symbols, build_exchange, parse_csv_list


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def runtime_default_exchanges() -> list[str]:
    raw = os.getenv("TARGET_EXCHANGES") or os.getenv("TARGET_EXCHANGE", "okx")
    exchanges = [name.strip().lower() for name in raw.split(",") if name.strip()]
    return [name for name in exchanges if name in SUPPORTED_EXCHANGES]


def runtime_default_symbols() -> list[str]:
    raw = os.getenv("WEB_RUNTIME_SYMBOLS")
    if raw:
        symbols = parse_csv_list(raw)
        if symbols:
            return symbols
    return build_default_symbols()


def runtime_startup_wait_sec() -> float:
    return float(os.getenv("WEB_RUNTIME_STARTUP_WAIT_SEC", "1.5"))


def runtime_action_timeout_sec() -> float:
    return float(os.getenv("WEB_ACTION_TIMEOUT_SEC", "10.0"))


class ExchangeRuntimeSession:
    def __init__(
        self,
        exchange_name: str,
        symbols: list[str],
        *,
        startup_wait_sec: float,
        action_timeout_sec: float,
    ) -> None:
        self.exchange_name = exchange_name.strip().lower()
        self.symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        self.symbol_set = set(self.symbols)
        self.exchange = build_exchange(self.exchange_name, self.symbols)
        self.startup_wait_sec = float(startup_wait_sec)
        self.action_timeout_sec = float(action_timeout_sec)
        self.started = False
        self.started_at: str | None = None
        self.last_error: str | None = None
        self.last_action_at: str | None = None
        self.start_lock = asyncio.Lock()
        self.action_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self.start_lock:
            if self.started:
                return
            try:
                await self.exchange.run()
                if self.startup_wait_sec > 0:
                    await asyncio.sleep(self.startup_wait_sec)
                self.started = True
                self.started_at = _utc_now_iso()
                self.last_error = None
            except Exception as exc:
                self.started = False
                self.last_error = str(exc)
                raise

    async def ensure_started(self) -> None:
        if self.started:
            return
        await self.start()

    async def stop(self) -> None:
        async with self.start_lock:
            if not self.started:
                return
            try:
                await self.exchange.stop()
            finally:
                self.started = False

    def ensure_symbols(self, symbols: list[str]) -> list[str]:
        normalized = [symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()]
        missing = [symbol for symbol in normalized if symbol not in self.symbol_set]
        if missing:
            raise ValueError(
                f"runtime session for {self.exchange_name} was started with {self.symbols}, missing symbols={missing}"
            )
        return normalized

    def runtime_status(self) -> dict[str, Any]:
        return {
            "mode": "resident",
            "exchange": self.exchange_name,
            "configured_symbols": self.symbols,
            "started": self.started,
            "started_at": self.started_at,
            "last_error": self.last_error,
            "last_action_at": self.last_action_at,
        }

    def build_live_report(
        self,
        symbols: list[str],
        *,
        recent_trades: dict[str, list[dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        normalized_symbols = self.ensure_symbols(symbols)
        report = build_exchange_report_from_state(
            self.exchange_name,
            self.exchange,
            normalized_symbols,
            recent_trades=recent_trades,
        )
        report["runtime"] = self.runtime_status()
        return report

    async def execute_action(
        self,
        symbol: str,
        action_name: str,
        action: Callable[[Any], Awaitable[Any]],
        *,
        settle_wait_sec: float = 0.0,
    ) -> dict[str, Any]:
        normalized_symbol = self.ensure_symbols([symbol])[0]

        async with self.action_lock:
            before_account = self.exchange.get_account_snapshot()
            before_symbol = self.exchange.get_symbol_snapshot(normalized_symbol)
            before_balance_report = build_balance_report(
                self.exchange_name,
                before_account,
                {normalized_symbol: before_symbol},
            )
            before_open_orders = self.exchange.get_open_order_map().get(normalized_symbol, [])

            action_result = await asyncio.wait_for(action(self.exchange), timeout=self.action_timeout_sec)

            if settle_wait_sec > 0:
                await asyncio.sleep(settle_wait_sec)

            after_account = self.exchange.get_account_snapshot()
            after_symbol = self.exchange.get_symbol_snapshot(normalized_symbol)
            after_balance_report = build_balance_report(
                self.exchange_name,
                after_account,
                {normalized_symbol: after_symbol},
            )
            after_open_orders = self.exchange.get_open_order_map().get(normalized_symbol, [])
            self.last_action_at = _utc_now_iso()

            return {
                "ok": True,
                "action": action_name,
                "exchange": self.exchange_name,
                "symbol": normalized_symbol,
                "runtime": self.runtime_status(),
                "action_result": action_result,
                "before": {
                    "account": before_account,
                    "balance_report": before_balance_report,
                    "symbol": before_symbol,
                    "open_orders": before_open_orders,
                },
                "after": {
                    "account": after_account,
                    "balance_report": after_balance_report,
                    "symbol": after_symbol,
                    "open_orders": after_open_orders,
                },
            }


class OpsRuntimeManager:
    def __init__(
        self,
        exchange_names: list[str] | None = None,
        symbols: list[str] | None = None,
        *,
        startup_wait_sec: float | None = None,
        action_timeout_sec: float | None = None,
    ) -> None:
        self.exchange_names = exchange_names or runtime_default_exchanges()
        self.symbols = symbols or runtime_default_symbols()
        self.startup_wait_sec = runtime_startup_wait_sec() if startup_wait_sec is None else float(startup_wait_sec)
        self.action_timeout_sec = (
            runtime_action_timeout_sec() if action_timeout_sec is None else float(action_timeout_sec)
        )
        self.sessions = {
            exchange_name: ExchangeRuntimeSession(
                exchange_name,
                self.symbols,
                startup_wait_sec=self.startup_wait_sec,
                action_timeout_sec=self.action_timeout_sec,
            )
            for exchange_name in self.exchange_names
        }

    async def start(self) -> None:
        for session in self.sessions.values():
            try:
                await session.start()
            except Exception:
                continue

    async def stop(self) -> None:
        for session in self.sessions.values():
            try:
                await session.stop()
            except Exception:
                continue

    def get_session(self, exchange_name: str) -> ExchangeRuntimeSession:
        normalized = exchange_name.strip().lower()
        session = self.sessions.get(normalized)
        if session is None:
            raise ValueError(f"exchange {normalized} is not configured in runtime manager")
        return session

    async def build_live_reports(
        self,
        exchange_names: list[str],
        symbols: list[str],
        *,
        recent_trades_by_exchange: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    ) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []

        for exchange_name in exchange_names:
            session = self.get_session(exchange_name)
            try:
                await session.ensure_started()
                reports.append(
                    session.build_live_report(
                        symbols,
                        recent_trades=(recent_trades_by_exchange or {}).get(exchange_name),
                    )
                )
            except Exception as exc:
                reports.append(
                    {
                        "exchange": exchange_name,
                        "symbols": [symbol.upper() for symbol in symbols],
                        "generated_at": _utc_now_iso(),
                        "error": str(exc),
                        "runtime": session.runtime_status(),
                    }
                )

        return reports

    async def execute_action(
        self,
        exchange_name: str,
        symbol: str,
        action_name: str,
        action: Callable[[Any], Awaitable[Any]],
        *,
        settle_wait_sec: float = 0.0,
    ) -> dict[str, Any]:
        session = self.get_session(exchange_name)
        await session.ensure_started()
        return await session.execute_action(symbol, action_name, action, settle_wait_sec=settle_wait_sec)

    def config_snapshot(self) -> dict[str, Any]:
        return {
            "mode": "resident",
            "exchange_names": self.exchange_names,
            "symbols": self.symbols,
            "startup_wait_sec": self.startup_wait_sec,
            "action_timeout_sec": self.action_timeout_sec,
            "sessions": {name: session.runtime_status() for name, session in self.sessions.items()},
        }
