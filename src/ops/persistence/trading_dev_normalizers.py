from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from .account_context import current_account_context
from .db import current_utc_ms, json_dumps


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _fee_cost(value: Any) -> float | None:
    fee = _safe_float(value)
    return abs(fee) if fee is not None else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_symbol(value: Any) -> str | None:
    text = _string_or_none(value)
    return text.upper() if text else None


def _bucket_start_ms(timestamp_ms: int | None, env_key: str, default_seconds: int) -> int:
    ts_ms = int(timestamp_ms or current_utc_ms())
    interval_sec = max(int(os.getenv(env_key, str(default_seconds)) or default_seconds), 1)
    interval_ms = interval_sec * 1000
    return (ts_ms // interval_ms) * interval_ms


def _payload_identifiers(payload: dict[str, Any] | None) -> list[str]:
    payload = payload or {}
    identifiers: list[str] = []
    for key in ("order_uid", "uuid", "client_order_id", "order_id", "identifier"):
        value = _string_or_none(payload.get(key))
        if value and value not in identifiers:
            identifiers.append(value)
    raw = payload.get("raw")
    if isinstance(raw, dict):
        for key in ("order_uid", "uuid", "client_order_id", "order_id", "identifier"):
            value = _string_or_none(raw.get(key))
            if value and value not in identifiers:
                identifiers.append(value)
    return identifiers


def _resolve_order_uuid(payload: dict[str, Any] | None) -> str | None:
    identifiers = _payload_identifiers(payload)
    return identifiers[0] if identifiers else None


def _resolve_order_type(payload: dict[str, Any] | None, event_type: str | None = None) -> str:
    payload = payload or {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for key in ("order_type", "type", "orderType", "ordType"):
        value = _string_or_none(payload.get(key))
        if value:
            return value.lower()
        if isinstance(raw, dict):
            value = _string_or_none(raw.get(key))
            if value:
                return value.lower()
    if event_type and event_type.startswith("cancel"):
        return "cancel"
    return "unknown"


def _resolve_order_status(payload: dict[str, Any] | None, event_type: str | None = None) -> str:
    payload = payload or {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}

    event_mapping = {
        "order_submit": "SUBMITTED",
        "order_ack": "ACKNOWLEDGED",
        "order_rejected": "REJECTED",
        "order_live": "LIVE",
        "order_status_update": "UPDATED",
        "partial_fill": "PARTIALLY_FILLED",
        "fill": "FILLED",
        "cancel_ack": "CANCELED",
        "order_canceled": "CANCELED",
        "order_missing": "MISSING",
        "order_expired": "EXPIRED",
        "cancel_rejected": "CANCEL_REJECTED",
    }
    event_status = event_mapping.get(str(event_type or "").lower())

    status = _string_or_none(payload.get("status"))
    raw_status = _string_or_none(raw.get("status")) if isinstance(raw, dict) else None
    state = _string_or_none(payload.get("state"))
    raw_state = _string_or_none(raw.get("state")) if isinstance(raw, dict) else None
    error = _string_or_none(payload.get("error"))
    raw_error = _string_or_none(raw.get("error") or raw.get("msg")) if isinstance(raw, dict) else None

    terminal_statuses = {"FILLED", "FINISHED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED"}
    for value in (status, raw_status):
        normalized = value.upper() if value else None
        if normalized in terminal_statuses:
            return normalized

    # Terminal lifecycle events must win over stale exchange payloads such as
    # status=NEW + state=not_exist. Otherwise missing/canceled orders remain
    # open-like in order_history.
    if event_status in {"FILLED", "CANCELED", "MISSING", "EXPIRED", "REJECTED", "FAILED"}:
        return event_status

    not_exist_markers = {
        str(value or "").strip().lower()
        for value in (state, raw_state, error, raw_error)
        if value not in (None, "")
    }
    if "not_exist" in not_exist_markers or "order does not exist." in not_exist_markers:
        return "MISSING"

    for key in ("status", "state"):
        value = _string_or_none(payload.get(key))
        if value:
            return value.upper()
    if isinstance(raw, dict):
        for key in ("status", "state"):
            value = _string_or_none(raw.get(key))
            if value:
                return value.upper()
    return event_status or "UNKNOWN"


def _resolve_side(value: Any) -> str:
    side = _string_or_none(value)
    return side.lower() if side else "unknown"


def _resolve_position_side(value: Any) -> str | None:
    side = _string_or_none(value)
    return side.upper() if side else None


def _trade_side(trade: dict[str, Any]) -> str:
    side = _string_or_none(trade.get("side"))
    if side:
        return side.lower()
    qty = _safe_float(trade.get("qty"))
    if qty is not None:
        if qty > 0:
            return "buy"
        if qty < 0:
            return "sell"
    return "unknown"


def _trade_role(trade: dict[str, Any]) -> str | None:
    role = _string_or_none(trade.get("role"))
    if role:
        return role.lower()
    maker = trade.get("maker")
    if maker is True:
        return "maker"
    if maker is False:
        return "taker"
    execution_type = _string_or_none(trade.get("execution_type"))
    if execution_type == "M":
        return "maker"
    if execution_type == "T":
        return "taker"
    return None


def _synth_trade_uuid(exchange_name: str, symbol: str, trade: dict[str, Any], server_time_ms: int, captured_at_ms: int) -> str:
    payload = {
        "exchange_name": exchange_name,
        "symbol": symbol,
        "order_id": _string_or_none(trade.get("order_id")),
        "client_order_id": _string_or_none(trade.get("client_order_id")),
        "side": _trade_side(trade),
        "price": _safe_float(trade.get("price")),
        "qty": _safe_float(trade.get("qty")),
        "quote_qty": _safe_float(trade.get("quote_qty")),
        "server_time_ms": server_time_ms,
        "captured_at_ms": captured_at_ms,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]
    return f"synthetic:{digest}"


def build_trade_history_rows(
    exchange_name: str,
    symbol: str,
    trades: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    normalized_symbol = _normalize_symbol(symbol)
    if normalized_symbol is None:
        return []

    account_context = current_account_context()
    rows: list[dict[str, Any]] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        captured_at_ms = current_utc_ms()
        server_time_ms = _safe_int(trade.get("create_time") or trade.get("timestamp") or trade.get("time"))
        if server_time_ms is None:
            server_time_ms = captured_at_ms

        trade_uuid = (
            _string_or_none(trade.get("uuid"))
            or _string_or_none(trade.get("trade_uuid"))
            or _string_or_none(trade.get("trade_id"))
            or _synth_trade_uuid(exchange_name, normalized_symbol, trade, server_time_ms, captured_at_ms)
        )

        rows.append(
            {
                **account_context,
                "uuid": trade_uuid,
                "exchange_name": exchange_name,
                "symbol": normalized_symbol,
                "order_uuid": _resolve_order_uuid(trade),
                "order_id": _string_or_none(trade.get("order_id")),
                "client_order_id": _string_or_none(trade.get("client_order_id")),
                "trade_id": _string_or_none(trade.get("trade_id")),
                "side": _trade_side(trade),
                "role": _trade_role(trade),
                "position_side": _resolve_position_side(trade.get("position_side") or trade.get("pos_side")),
                "price": _safe_float(trade.get("price")) or 0.0,
                "qty": abs(_safe_float(trade.get("qty")) or 0.0),
                "quote_qty": _safe_float(trade.get("quote_qty")),
                "fee": _fee_cost(trade.get("fee") if trade.get("fee") is not None else trade.get("commission")),
                "fee_ccy": _string_or_none(trade.get("fee_ccy") or trade.get("commission_asset")),
                "realized_pnl_usdt": _safe_float(trade.get("realized_pnl")),
                "desktop_time_ms": captured_at_ms,
                "server_time_ms": server_time_ms,
                "raw_json": json_dumps(trade.get("raw", trade)),
            }
        )
    return rows


def build_order_history_row(
    exchange_name: str,
    symbol: str,
    payload: dict[str, Any] | None,
    *,
    event_type: str | None = None,
) -> dict[str, Any] | None:
    payload = dict(payload or {})
    normalized_symbol = _normalize_symbol(symbol or payload.get("symbol") or payload.get("ticker"))
    if normalized_symbol in (None, "ALL"):
        return None

    order_uuid = _resolve_order_uuid(payload)
    if order_uuid is None:
        return None

    captured_at_ms = current_utc_ms()
    server_time_ms = _safe_int(
        payload.get("timestamp")
        or payload.get("update_time")
        or payload.get("create_time")
    )

    return {
        **current_account_context(),
        "uuid": order_uuid,
        "exchange_name": exchange_name,
        "symbol": normalized_symbol,
        "order_id": _string_or_none(payload.get("order_id")),
        "client_order_id": _string_or_none(payload.get("client_order_id")),
        "side": _resolve_side(payload.get("side")),
        "position_side": _resolve_position_side(payload.get("position_side")),
        "reduce_only": 1 if bool(payload.get("reduce_only", False)) else 0,
        "order_type": _resolve_order_type(payload, event_type=event_type),
        "price": _safe_float(payload.get("price")),
        "qty": abs(_safe_float(payload.get("qty")) or 0.0),
        "executed_qty": abs(_safe_float(payload.get("executed_qty")) or 0.0),
        "avg_price": _safe_float(payload.get("avg_price")),
        "status": _resolve_order_status(payload, event_type=event_type),
        "order_start": int(server_time_ms or captured_at_ms),
        "order_end": int(server_time_ms or captured_at_ms),
        "desktop_time_ms": captured_at_ms,
        "server_time_ms": server_time_ms,
        "raw_json": json_dumps(payload),
    }


def build_balance_snapshot_row(
    exchange_name: str,
    tracked_symbols: list[str],
    balance_report: dict[str, Any],
    *,
    snapshot_time_ms: int | None = None,
) -> dict[str, Any]:
    captured_at_ms = int(snapshot_time_ms or current_utc_ms())
    bucket_start_ms = _bucket_start_ms(captured_at_ms, "SNAPSHOT_INTERVAL_SEC", 120)
    normalized_symbols = [symbol for symbol in (_normalize_symbol(symbol) for symbol in tracked_symbols) if symbol]

    positions_by_symbol = {
        _normalize_symbol(position.get("ticker") or position.get("symbol")): position
        for position in (balance_report.get("positions") or [])
        if _normalize_symbol(position.get("ticker") or position.get("symbol"))
    }
    symbol_snapshot: dict[str, Any] = {}
    for symbol in normalized_symbols:
        position = positions_by_symbol.get(symbol) or {}
        symbol_snapshot[symbol] = {
            "qty": _safe_float(position.get("qty")) or 0.0,
            "entry_price": _safe_float(position.get("entry_price")),
            "mark_price": _safe_float(position.get("mark_price")),
            "position_side": _string_or_none(position.get("position_side")),
            "exchange_unrealized_profit_usd": _safe_float(position.get("exchange_unrealized_profit_usd")),
            "theoretical_unrealized_profit_usd": _safe_float(position.get("theoretical_unrealized_profit_usd")),
        }

    return {
        **current_account_context(),
        "exchange_name": exchange_name,
        "bucket_start_ms": bucket_start_ms,
        "desktop_time_ms": captured_at_ms,
        "server_time_ms": None,
        "tracked_symbols_json": json_dumps(normalized_symbols),
        "symbol_balances_json": json_dumps(symbol_snapshot),
        "cash_usdt": _safe_float(balance_report.get("wallet_balance_usd")) or 0.0,
        "available_cash_usdt": _safe_float(balance_report.get("available_balance_usd")) or 0.0,
        "locked_cash_usdt": _safe_float(balance_report.get("locked_balance_usd")) or 0.0,
        "total_equity_usdt": _safe_float(balance_report.get("actual_equity_usd")) or 0.0,
        "unrealized_pnl_usdt": _safe_float(balance_report.get("exchange_unrealized_profit_usd")) or 0.0,
        "raw_json": json_dumps(balance_report),
    }


def build_funding_rate_rows(
    exchange_name: str,
    symbol_snapshots: dict[str, dict[str, Any]],
    *,
    snapshot_time_ms: int | None = None,
) -> list[dict[str, Any]]:
    captured_at_ms = int(snapshot_time_ms or current_utc_ms())
    bucket_start_ms = _bucket_start_ms(captured_at_ms, "SNAPSHOT_INTERVAL_SEC", 120)
    rows: list[dict[str, Any]] = []

    for symbol, snapshot in sorted((symbol_snapshots or {}).items()):
        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol is None:
            continue
        funding = dict((snapshot or {}).get("funding") or {})
        if not funding:
            continue

        funding_rate = _safe_float(funding.get("funding_rate"))
        next_funding_time_ms = _safe_int(funding.get("next_funding_time"))
        if funding_rate is None and next_funding_time_ms is None:
            continue

        rows.append(
            {
                "exchange_name": exchange_name,
                "symbol": normalized_symbol,
                "bucket_start_ms": bucket_start_ms,
                "desktop_time_ms": captured_at_ms,
                "server_time_ms": _safe_int(funding.get("timestamp")),
                "funding_rate": funding_rate if funding_rate is not None else 0.0,
                "next_funding_time_ms": next_funding_time_ms,
                "funding_interval_hours": _safe_int(funding.get("funding_interval_hours")),
                "funding_interval_source": funding.get("funding_interval_source"),
                "raw_json": json_dumps(funding),
            }
        )

    return rows


def build_symbol_price_rows(
    exchange_name: str,
    api_snapshots: dict[str, dict[str, Any]] | None,
    impact_snapshots: dict[str, dict[str, Any]] | None = None,
    funding_snapshots: dict[str, dict[str, Any]] | None = None,
    *,
    snapshot_time_ms: int | None = None,
) -> list[dict[str, Any]]:
    captured_at_ms = int(snapshot_time_ms or current_utc_ms())
    bucket_start_ms = _bucket_start_ms(captured_at_ms, "SYMBOL_PRICE_BUCKET_SEC", 60)
    rows: list[dict[str, Any]] = []
    impact_snapshots = impact_snapshots or {}
    funding_snapshots = funding_snapshots or {}

    for symbol, api_row in sorted((api_snapshots or {}).items()):
        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol is None:
            continue

        best_bid = _safe_float((api_row or {}).get("best_bid"))
        best_ask = _safe_float((api_row or {}).get("best_ask"))
        if best_bid is None or best_ask is None:
            continue

        impact_row = dict(impact_snapshots.get(normalized_symbol) or {})
        api_server_time_ms = _safe_int((api_row or {}).get("server_time"))
        impact_server_time_ms = _safe_int(impact_row.get("server_time"))
        server_time_ms = max(
            [value for value in (api_server_time_ms, impact_server_time_ms) if value is not None],
            default=None,
        )
        funding_row = dict(funding_snapshots.get(normalized_symbol) or {})
        funding_rate = _safe_float(funding_row.get("funding_rate"))
        next_funding_time_ms = _safe_int(funding_row.get("next_funding_time_ms"))
        funding_interval_hours = _safe_int(funding_row.get("funding_interval_hours"))
        funding_interval_source = funding_row.get("funding_interval_source")
        funding_updated_at_ms = _safe_int(funding_row.get("funding_updated_at_ms"))

        rows.append(
            {
                "exchange_name": exchange_name,
                "symbol": normalized_symbol,
                "bucket_start_ms": bucket_start_ms,
                "desktop_time_ms": captured_at_ms,
                "server_time_ms": server_time_ms,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid": _safe_float(impact_row.get("bid")),
                "ask": _safe_float(impact_row.get("ask")),
                "funding_rate": funding_rate,
                "next_funding_time_ms": next_funding_time_ms,
                "funding_interval_hours": funding_interval_hours,
                "funding_interval_source": funding_interval_source,
                "funding_updated_at_ms": funding_updated_at_ms,
            }
        )

    return rows
