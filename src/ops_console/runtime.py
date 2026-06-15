from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from src.ops.persistence.sync import SUPPORTED_EXCHANGES, build_default_symbols, build_exchange, parse_csv_list
from src.ops_console.state import build_exchange_state, normalize_symbol, now_ms, safe_float


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_positive_float(value: Any, default: float = 1.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def exchange_action_error(result: Any) -> str:
    if not isinstance(result, dict):
        return ""
    error = result.get("error")
    if error:
        return json.dumps(error, ensure_ascii=False, default=str)[:800]
    raw = result.get("raw")
    if isinstance(raw, dict):
        code = str(raw.get("code") or "").strip()
        if code and code not in {"0", "00000"}:
            return json.dumps(raw, ensure_ascii=False, default=str)[:800]
    status = str(result.get("status") or result.get("state") or "").strip().upper()
    if status in {"REJECTED", "EXPIRED", "FAILED", "ERROR"}:
        return json.dumps(result, ensure_ascii=False, default=str)[:800]
    return ""


def console_default_exchanges() -> list[str]:
    raw = os.getenv("OPS_CONSOLE_EXCHANGES") or os.getenv("TARGET_EXCHANGES") or "binance,okx"
    names = [name.strip().lower() for name in raw.split(",") if name.strip()]
    return [name for name in names if name in SUPPORTED_EXCHANGES]


def console_default_symbols() -> list[str]:
    # Default to the strategy universe so the console follows strategy restarts.
    # OPS_CONSOLE_SYMBOLS is kept only as an explicit diagnostic override.
    raw = os.getenv("OPS_CONSOLE_SYMBOLS") or os.getenv("STRATEGY_SYMBOLS") or os.getenv("WEB_RUNTIME_SYMBOLS")
    symbols = parse_csv_list(raw, fallback=build_default_symbols())
    return [normalize_symbol(symbol) for symbol in symbols]


def console_symbols_source() -> str:
    if os.getenv("OPS_CONSOLE_SYMBOLS"):
        return "OPS_CONSOLE_SYMBOLS"
    if os.getenv("STRATEGY_SYMBOLS"):
        return "STRATEGY_SYMBOLS"
    if os.getenv("WEB_RUNTIME_SYMBOLS"):
        return "WEB_RUNTIME_SYMBOLS"
    return "default"


def entry_control_path() -> Path:
    raw = os.getenv("STRATEGY_ENTRY_CONTROL_FILE", "runtime/entry_control.json")
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def read_entry_control() -> dict[str, Any]:
    path = entry_control_path()
    if not path.exists():
        return {"entry_paused": False, "reason": "", "source_path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "entry_paused": True,
            "reason": "entry_control_read_error",
            "error": str(exc),
            "source_path": str(path),
        }
    if not isinstance(payload, dict):
        return {"entry_paused": True, "reason": "entry_control_invalid_payload", "source_path": str(path)}
    return {
        "entry_paused": bool(payload.get("entry_paused", payload.get("paused", False))),
        "reason": str(payload.get("reason") or ""),
        "updated_at_ms": payload.get("updated_at_ms"),
        "source_path": str(path),
    }


def write_entry_control(paused: bool, reason: str) -> dict[str, Any]:
    path = entry_control_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "entry_paused": bool(paused),
        "reason": str(reason or ("manual_ops_console" if paused else "manual_ops_console_resume")),
        "updated_at_ms": now_ms(),
        "source": "ops_console",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {**payload, "source_path": str(path)}


class ConsoleExchangeSession:
    def __init__(self, exchange_name: str, symbols: list[str]) -> None:
        self.exchange_name = exchange_name.strip().lower()
        self.symbols = [normalize_symbol(symbol) for symbol in symbols if symbol]
        self.exchange = build_exchange(
            self.exchange_name,
            self.symbols,
            account_interval=float(os.getenv("OPS_CONSOLE_ACCOUNT_INTERVAL_SEC", "1.2")),
            order_interval=float(os.getenv("OPS_CONSOLE_ORDER_INTERVAL_SEC", "1.0")),
            open_orders_interval=float(os.getenv("OPS_CONSOLE_OPEN_ORDERS_INTERVAL_SEC", "3.0")),
            funding_interval=float(os.getenv("OPS_CONSOLE_FUNDING_INTERVAL_SEC", "30.0")),
            health_interval=float(os.getenv("OPS_CONSOLE_HEALTH_INTERVAL_SEC", "0.5")),
            stale_threshold_ms=int(os.getenv("OPS_CONSOLE_EXCHANGE_STALE_MS", os.getenv("OPS_CONSOLE_STALE_MS", "1500"))),
        )
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
                wait_sec = float(os.getenv("OPS_CONSOLE_STARTUP_WAIT_SEC", "1.5"))
                if wait_sec > 0:
                    await asyncio.sleep(wait_sec)
                self.started = True
                self.started_at = utc_now_iso()
                self.last_error = None
            except Exception as exc:
                self.started = False
                self.last_error = str(exc)
                raise

    async def ensure_started(self) -> None:
        if not self.started:
            await self.start()

    async def stop(self) -> None:
        async with self.start_lock:
            if not self.started:
                return
            try:
                await self.exchange.stop()
            finally:
                self.started = False

    def assert_symbol(self, symbol: str) -> str:
        normalized = normalize_symbol(symbol)
        if normalized not in self.symbols:
            raise ValueError(f"{self.exchange_name} runtime missing symbol={normalized}; configured={self.symbols}")
        return normalized

    def status(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange_name,
            "started": self.started,
            "started_at": self.started_at,
            "last_error": self.last_error,
            "last_action_at": self.last_action_at,
            "symbols": self.symbols,
        }

    def snapshot(self, *, stale_ms: int) -> dict[str, Any]:
        state = build_exchange_state(self.exchange_name, self.exchange, self.symbols, stale_ms=stale_ms)
        state["runtime"] = self.status()
        return state

    async def execute_action(
        self,
        symbol: str,
        action_name: str,
        action: Callable[[Any], Awaitable[Any]],
        *,
        wait_sec: float = 0.0,
    ) -> dict[str, Any]:
        normalized_symbol = self.assert_symbol(symbol)
        timeout_sec = float(os.getenv("OPS_CONSOLE_ACTION_TIMEOUT_SEC", "10.0"))
        async with self.action_lock:
            before = self.snapshot(stale_ms=int(os.getenv("OPS_CONSOLE_STALE_MS", "1500")))
            result = await asyncio.wait_for(action(self.exchange), timeout=timeout_sec)
            error = exchange_action_error(result)
            if error:
                raise ValueError(f"{action_name} failed on {self.exchange_name}:{normalized_symbol}: {error}")
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
            after = self.snapshot(stale_ms=int(os.getenv("OPS_CONSOLE_STALE_MS", "1500")))
            self.last_action_at = utc_now_iso()
            return {
                "ok": True,
                "action": action_name,
                "exchange": self.exchange_name,
                "symbol": normalized_symbol,
                "action_result": result,
                "before": before,
                "after": after,
            }


class ConsoleRuntime:
    def __init__(self, exchange_names: list[str] | None = None, symbols: list[str] | None = None) -> None:
        self.exchange_names = exchange_names or console_default_exchanges()
        self.symbols = symbols or console_default_symbols()
        self.stale_ms = int(os.getenv("OPS_CONSOLE_STALE_MS", "1500"))
        self.sessions = {
            exchange_name: ConsoleExchangeSession(exchange_name, self.symbols)
            for exchange_name in self.exchange_names
        }

    async def start(self) -> None:
        results = await asyncio.gather(*(session.start() for session in self.sessions.values()), return_exceptions=True)
        for exchange_name, result in zip(self.exchange_names, results):
            if isinstance(result, Exception):
                print(f"[ops-console][runtime][start_error] exchange={exchange_name} error={result!r}")

    async def stop(self) -> None:
        await asyncio.gather(*(session.stop() for session in self.sessions.values()), return_exceptions=True)

    def session(self, exchange_name: str) -> ConsoleExchangeSession:
        normalized = exchange_name.strip().lower()
        session = self.sessions.get(normalized)
        if session is None:
            raise ValueError(f"exchange not configured: {exchange_name}")
        return session

    async def snapshot(self) -> dict[str, Any]:
        exchange_states: dict[str, Any] = {}
        for exchange_name, session in self.sessions.items():
            try:
                await session.ensure_started()
                exchange_states[exchange_name] = session.snapshot(stale_ms=self.stale_ms)
            except Exception as exc:
                exchange_states[exchange_name] = {
                    "exchange": exchange_name,
                    "error": str(exc),
                    "runtime": session.status(),
                    "symbols": {},
                }
        return {
            "timestamp_ms": now_ms(),
            "timestamp_utc": utc_now_iso(),
            "mode": os.getenv("STRATEGY_MODE", "LIVE"),
            "symbols": self.symbols,
            "exchanges": exchange_states,
            "runtime": {
                "exchange_names": self.exchange_names,
                "symbols": self.symbols,
                "symbols_source": console_symbols_source(),
                "stale_ms": self.stale_ms,
                "strategy_leverage": parse_positive_float(os.getenv("STRATEGY_LEVERAGE"), 1.0),
                "entry_control": read_entry_control(),
                "web_trading_enabled": parse_bool(os.getenv("ENABLE_WEB_TRADING"), False),
                "token_required": action_token_required(),
            },
        }

    async def execute_action(
        self,
        exchange_name: str,
        symbol: str,
        action_name: str,
        action: Callable[[Any], Awaitable[Any]],
        *,
        wait_sec: float = 0.0,
    ) -> dict[str, Any]:
        session = self.session(exchange_name)
        await session.ensure_started()
        return await session.execute_action(symbol, action_name, action, wait_sec=wait_sec)


def action_token() -> str:
    return (os.getenv("OPS_CONSOLE_ACTION_TOKEN") or os.getenv("WEB_ACTION_TOKEN") or "").strip()


def action_token_required() -> bool:
    raw = os.getenv("OPS_CONSOLE_ACTION_TOKEN_REQUIRED")
    if raw is not None:
        return parse_bool(raw, False)
    return bool(action_token())


def position_qty_for_symbol(snapshot: dict[str, Any], exchange_name: str, symbol: str) -> float:
    exchange_state = (snapshot.get("exchanges") or {}).get(exchange_name) or {}
    symbol_state = (exchange_state.get("symbols") or {}).get(normalize_symbol(symbol)) or {}
    position = symbol_state.get("position") or {}
    return safe_float(position.get("qty"), 0.0)
