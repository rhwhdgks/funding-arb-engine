from __future__ import annotations

import json
import os
from collections import deque
from pathlib import Path
from typing import Any

from src.ops_console.runtime import PROJECT_ROOT, action_token, action_token_required, parse_bool
from src.ops_console.state import normalize_symbol


TERMINAL_PAIR_EVENTS = {
    "pair_closed",
    "pair_closed_detected",
    "pair_closed_after_recovery",
    "pair_closed_after_micro_residual_cleanup",
    "pair_closed_after_shutdown",
}
_ACTIVE_STRATEGY_SYMBOLS_PROVIDER = None


def set_active_strategy_symbols_provider(provider) -> None:
    global _ACTIVE_STRATEGY_SYMBOLS_PROVIDER
    _ACTIVE_STRATEGY_SYMBOLS_PROVIDER = provider


def web_trading_enabled() -> bool:
    return parse_bool(os.getenv("ENABLE_WEB_TRADING"), False)


def strategy_event_log_path() -> Path:
    raw = os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH", str(PROJECT_ROOT / "logs" / "strategy_live.jsonl"))
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def active_strategy_symbols(max_lines: int = 20000) -> dict[str, str]:
    if not parse_bool(os.getenv("OPS_CONSOLE_STRATEGY_CONFLICT_CHECK", "1"), True):
        return {}
    if callable(_ACTIVE_STRATEGY_SYMBOLS_PROVIDER):
        try:
            provided = _ACTIVE_STRATEGY_SYMBOLS_PROVIDER()
        except Exception:
            provided = None
        if isinstance(provided, dict):
            return {
                str(pair_id): normalize_symbol(str(symbol))
                for pair_id, symbol in provided.items()
                if str(symbol).strip()
            }

    path = strategy_event_log_path()
    if not path.exists():
        return {}

    active: dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            rows = deque(handle, maxlen=max_lines)
    except Exception:
        return {}

    for line in rows:
        try:
            row = json.loads(line)
        except Exception:
            continue
        event_type = str(row.get("event_type") or "")
        pair = row.get("pair") if isinstance(row.get("pair"), dict) else {}
        pair_id = str(pair.get("pair_id") or row.get("pair_id") or "")
        symbol = normalize_symbol(str(pair.get("symbol") or row.get("symbol") or ""))
        if not pair_id:
            continue
        if event_type == "pair_opened" and symbol:
            active[pair_id] = symbol
        elif event_type in TERMINAL_PAIR_EVENTS:
            active.pop(pair_id, None)
    return active


def assert_action_token(provided_token: str | None) -> None:
    if not action_token_required():
        return
    expected = action_token()
    if not expected:
        raise PermissionError("action token is required but not configured")
    if (provided_token or "").strip() != expected:
        raise PermissionError("invalid action token")


def _symbol_state(snapshot: dict[str, Any], exchange: str, symbol: str) -> dict[str, Any]:
    normalized_exchange = exchange.strip().lower()
    normalized_symbol = normalize_symbol(symbol)
    exchange_state = (snapshot.get("exchanges") or {}).get(normalized_exchange) or {}
    return (exchange_state.get("symbols") or {}).get(normalized_symbol) or {}


def assert_symbol_not_stale(snapshot: dict[str, Any], exchange: str, symbol: str) -> None:
    state = _symbol_state(snapshot, exchange, symbol)
    if not state:
        raise ValueError(f"missing runtime state for {exchange}:{symbol}")
    if state.get("stale"):
        orderbook = state.get("orderbook") or {}
        raise ValueError(
            f"stale market data for {exchange}:{normalize_symbol(symbol)} age_ms={orderbook.get('age_ms')}"
        )
    block_reasons = [str(reason) for reason in state.get("block_reasons") or []]
    if any("rate_limit" in reason.lower() for reason in block_reasons):
        raise ValueError(f"rate-limit pressure for {exchange}:{normalize_symbol(symbol)} reasons={block_reasons}")


def assert_no_strategy_conflict(symbol: str) -> None:
    normalized_symbol = normalize_symbol(symbol)
    active = active_strategy_symbols()
    conflicting = [pair_id for pair_id, active_symbol in active.items() if active_symbol == normalized_symbol]
    if conflicting:
        raise ValueError(f"strategy has active pair for {normalized_symbol}: {conflicting}")


def assert_live_action_allowed(
    snapshot: dict[str, Any],
    *,
    exchange: str,
    symbol: str,
    ops_token: str | None,
    check_strategy_conflict: bool = True,
) -> None:
    if not web_trading_enabled():
        raise PermissionError("web trading disabled; set ENABLE_WEB_TRADING=1")
    assert_action_token(ops_token)
    assert_symbol_not_stale(snapshot, exchange, symbol)
    if check_strategy_conflict:
        assert_no_strategy_conflict(symbol)
