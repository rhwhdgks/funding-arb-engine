import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .db import get_db_connection
from .trading_dev_normalizers import build_order_history_row, build_trade_history_rows
from .trading_dev_store import upsert_order_history_rows, upsert_trade_history_rows


def _env_flag(key: str, default: str = "1") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def _event_enabled() -> bool:
    return _env_flag("ENABLE_ORDER_EVENTS", "1")

_BG_MAX_WORKERS = max(int(os.getenv("EVENT_HOOKS_MAX_WORKERS", "4") or 4), 1)
_BG_EXECUTOR = ThreadPoolExecutor(max_workers=_BG_MAX_WORKERS, thread_name_prefix="event-hooks")


def _normalize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    return dict(payload or {})


def _identifier_map(payload: dict[str, Any] | None) -> dict[str, str]:
    payload = payload or {}
    identifiers: dict[str, str] = {}
    for key in ("order_uid", "order_id", "client_order_id", "uuid"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        identifiers[key] = str(value)
    return identifiers


def _with_event_context(payload: dict[str, Any], *, event_type: str, event_source: str) -> dict[str, Any]:
    enriched = _normalize_payload(payload)
    enriched.setdefault("timestamp", int(time.time() * 1000))
    enriched["order_refs"] = _identifier_map(enriched)
    enriched["event_context"] = {
        "event_type": event_type,
        "event_source": event_source,
        "captured_at_ms": enriched.get("timestamp"),
        "has_request": bool(enriched.get("request")),
        "has_previous": bool(enriched.get("previous")),
        "has_raw": "raw" in enriched,
    }
    return enriched


def _build_transition(previous_payload: dict[str, Any] | None, current_payload: dict[str, Any] | None) -> dict[str, Any]:
    previous_payload = previous_payload or {}
    current_payload = current_payload or {}
    previous_exec = _executed_qty(previous_payload)
    current_exec = _executed_qty(current_payload)
    return {
        "previous_status": previous_payload.get("status"),
        "current_status": current_payload.get("status"),
        "previous_state": previous_payload.get("state"),
        "current_state": current_payload.get("state"),
        "previous_executed_qty": previous_exec,
        "current_executed_qty": current_exec,
        "delta_executed_qty": max(current_exec - previous_exec, 0.0),
        "previous_left_qty": previous_payload.get("left_qty"),
        "current_left_qty": current_payload.get("left_qty"),
        "previous_avg_price": previous_payload.get("avg_price"),
        "current_avg_price": current_payload.get("avg_price"),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _looks_success(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict) or not result:
        return False
    if result.get("error"):
        return False
    if result.get("ok") is False:
        return False
    status = str(result.get("status") or "").lower()
    state = str(result.get("state") or "").lower()
    if status in {"error", "rejected", "reject", "failed"}:
        return False
    if state in {"error", "rejected", "reject", "failed"}:
        return False
    return True


def _run_background(func, *args, **kwargs) -> None:
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(_BG_EXECUTOR, lambda: func(*args, **kwargs))
    except RuntimeError:
        func(*args, **kwargs)


def _store_trade_history_sync(exchange_name: str, symbol: str, trades: list[dict[str, Any]]) -> None:
    if not trades:
        return

    conn = None
    try:
        conn = get_db_connection()
        rows = build_trade_history_rows(exchange_name, symbol, trades)
        upsert_trade_history_rows(conn, rows)
    except Exception as exc:
        print(f"[trade-history] skip exchange={exchange_name} symbol={symbol}: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _store_order_history_sync(exchange_name: str, symbol: str, event_type: str, payload: dict[str, Any]) -> None:
    if not _event_enabled():
        return

    conn = None
    try:
        conn = get_db_connection()
        row = build_order_history_row(exchange_name, symbol, payload, event_type=event_type)
        if row is None:
            return
        upsert_order_history_rows(conn, [row])
    except Exception as exc:
        print(f"[order-history] skip exchange={exchange_name} symbol={symbol} type={event_type}: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def schedule_trade_history_upsert(exchange_name: str, symbol: str, trades: list[dict[str, Any]] | None) -> int:
    normalized_symbol = str(symbol or "").upper()
    normalized_trades = [dict(trade) for trade in (trades or []) if isinstance(trade, dict)]
    if not normalized_symbol or not normalized_trades:
        return 0

    _run_background(_store_trade_history_sync, exchange_name, normalized_symbol, normalized_trades)
    return len(normalized_trades)


def schedule_matched_trade_history_upsert(
    exchange_name: str,
    symbol: str,
    order_payload: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None,
) -> int:
    matched = _matched_trades_for_order(order_payload, trades)
    return schedule_trade_history_upsert(exchange_name, symbol, matched)


def schedule_order_event(exchange_name: str, symbol: str, event_type: str, event_source: str, payload: dict[str, Any]) -> None:
    normalized_symbol = str(symbol or "").upper()
    normalized_payload = _with_event_context(payload, event_type=event_type, event_source=event_source)
    normalized_payload.setdefault("symbol", normalized_symbol)
    normalized_payload.setdefault("ticker", normalized_symbol)
    _run_background(_store_order_history_sync, exchange_name, normalized_symbol, event_type, normalized_payload)


def build_order_request_payload(
    symbol: str,
    side: str,
    amount: float,
    *,
    price: float | None = None,
    order_type: str = "limit",
    is_close: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "symbol": symbol.upper(),
        "ticker": symbol.upper(),
        "side": side.lower(),
        "qty": amount,
        "order_type": order_type,
        "is_close": bool(is_close),
        "timestamp": int(time.time() * 1000),
        "event_nonce": int(time.time() * 1000),
    }
    if price is not None:
        payload["price"] = price
    return payload


def build_cancel_request_payload(symbol: str, uuid: str) -> dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "ticker": symbol.upper(),
        "order_uid": uuid,
        "uuid": uuid,
        "client_order_id": uuid,
        "timestamp": int(time.time() * 1000),
        "event_nonce": int(time.time() * 1000),
    }


def build_cancel_all_request_payload(symbol: str | None) -> dict[str, Any]:
    normalized_symbol = symbol.upper() if symbol else "ALL"
    return {
        "symbol": normalized_symbol,
        "ticker": normalized_symbol,
        "target": "all",
        "timestamp": int(time.time() * 1000),
        "event_nonce": int(time.time() * 1000),
    }


def _merge_request_context(result: dict[str, Any] | None, request_payload: dict[str, Any]) -> dict[str, Any]:
    payload = _normalize_payload(result)
    payload.setdefault("symbol", request_payload.get("symbol"))
    payload.setdefault("ticker", request_payload.get("ticker"))
    payload.setdefault(
        "order_uid",
        request_payload.get("order_uid") or request_payload.get("client_order_id") or request_payload.get("uuid"),
    )
    payload.setdefault("side", request_payload.get("side"))
    payload.setdefault("qty", request_payload.get("qty"))
    payload.setdefault("price", request_payload.get("price"))
    payload.setdefault("client_order_id", request_payload.get("client_order_id"))
    payload.setdefault("uuid", request_payload.get("uuid"))
    payload["request"] = request_payload
    payload.setdefault("timestamp", int(time.time() * 1000))
    return payload


def schedule_order_submit(exchange_name: str, request_payload: dict[str, Any]) -> None:
    payload = _normalize_payload(request_payload)
    payload["event_reason"] = "order_submit"
    schedule_order_event(exchange_name, str(payload.get("symbol") or ""), "order_submit", "local", payload)


def schedule_order_result(
    exchange_name: str,
    symbol: str,
    result: dict[str, Any] | None,
    request_payload: dict[str, Any],
    *,
    event_source: str | None = None,
) -> None:
    payload = _merge_request_context(result, request_payload)
    event_type = "order_ack" if _looks_success(result) else "order_rejected"
    payload["event_reason"] = "order_result"
    schedule_order_event(exchange_name, symbol, event_type, event_source or "rest", payload)


def schedule_cancel_submit(exchange_name: str, request_payload: dict[str, Any]) -> None:
    payload = _normalize_payload(request_payload)
    payload["event_reason"] = "cancel_submit"
    schedule_order_event(exchange_name, str(payload.get("symbol") or ""), "cancel_submit", "local", payload)


def schedule_cancel_result(
    exchange_name: str,
    symbol: str,
    result: dict[str, Any] | None,
    request_payload: dict[str, Any],
    *,
    event_source: str | None = None,
) -> None:
    payload = _merge_request_context(result, request_payload)
    event_type = "cancel_ack" if _looks_success(result) else "cancel_rejected"
    payload["event_reason"] = "cancel_result"
    schedule_order_event(exchange_name, symbol, event_type, event_source or "rest", payload)


def schedule_cancel_all_submit(exchange_name: str, request_payload: dict[str, Any]) -> None:
    payload = _normalize_payload(request_payload)
    payload["event_reason"] = "cancel_all_submit"
    schedule_order_event(exchange_name, str(payload.get("symbol") or "ALL"), "cancel_all_submit", "local", payload)


def schedule_cancel_all_result(exchange_name: str, symbol: str | None, result: dict[str, Any] | None, request_payload: dict[str, Any]) -> None:
    payload = _merge_request_context(result, request_payload)
    event_type = "cancel_all_ack" if _looks_success(result) else "cancel_all_rejected"
    payload["event_reason"] = "cancel_all_result"
    schedule_order_event(exchange_name, symbol or "ALL", event_type, "rest", payload)


def derive_order_state_event_type(payload: dict[str, Any] | None, *, prefer_live: bool = False) -> str | None:
    if not payload:
        return None

    status = _normalized_status_text(payload)
    state = _normalized_state_text(payload)
    error = str(payload.get("error") or "").lower()
    executed_qty = _executed_qty(payload)
    qty = _safe_float(payload.get("qty"), 0.0)
    left_qty_raw = payload.get("left_qty")
    left_qty = _safe_float(left_qty_raw, 0.0)

    if status in {"FILLED", "FINISHED"}:
        return "fill"
    if status in {"CANCELED", "CANCELLED"}:
        return "order_canceled"
    if status == "REJECTED":
        return "order_rejected"
    if status == "EXPIRED":
        return "order_expired"
    if state == "not_exist" or error == "not_exist":
        return "order_missing"
    if executed_qty > 1e-12:
        if qty > 1e-12 and left_qty_raw is not None and left_qty <= 1e-12:
            return "fill"
        return "partial_fill"
    if prefer_live and (state == "exist" or status in {"NEW", "OPEN", "LIVE", "PARTIALLY_FILLED"}):
        return "order_live"
    return None


def schedule_order_state_event(
    exchange_name: str,
    symbol: str,
    payload: dict[str, Any] | None,
    *,
    prefer_live: bool = False,
    event_source: str = "rest_state",
) -> str | None:
    event_type = derive_order_state_event_type(payload, prefer_live=prefer_live)
    if event_type is None or not payload:
        return None
    enriched = _normalize_payload(payload)
    enriched["event_reason"] = "order_state_event"
    enriched["transition"] = _build_transition(None, enriched)
    schedule_order_event(exchange_name, symbol, event_type, event_source, enriched)
    return event_type


def schedule_cancel_terminal_event(
    exchange_name: str,
    symbol: str,
    result: dict[str, Any] | None,
    request_payload: dict[str, Any] | None = None,
    *,
    order_payload: dict[str, Any] | None = None,
    event_source: str = "rest_cancel",
) -> str | None:
    payload = _normalize_payload(order_payload)
    payload.update(_normalize_payload(result))

    request_payload = request_payload or {}
    payload.setdefault("symbol", payload.get("ticker") or request_payload.get("symbol") or symbol.upper())
    payload.setdefault("ticker", payload.get("symbol") or request_payload.get("ticker") or symbol.upper())
    payload.setdefault(
        "order_uid",
        request_payload.get("order_uid")
        or request_payload.get("client_order_id")
        or request_payload.get("uuid")
        or payload.get("client_order_id")
        or payload.get("uuid"),
    )
    payload.setdefault("uuid", request_payload.get("uuid"))
    payload.setdefault("client_order_id", request_payload.get("client_order_id") or request_payload.get("uuid"))
    payload.setdefault("timestamp", int(time.time() * 1000))
    payload["event_reason"] = "cancel_terminal"
    payload["cancel_request"] = _normalize_payload(request_payload)
    payload["transition"] = _build_transition(order_payload, payload)

    if _looks_success(result):
        payload["status"] = "CANCELED"
        payload["state"] = "not_exist"
        return schedule_order_state_event(exchange_name, symbol, payload, event_source=event_source)

    if str((result or {}).get("error") or "").lower() == "not_exist" or str((result or {}).get("state") or "").lower() == "not_exist":
        payload["state"] = "not_exist"
        return schedule_order_event(exchange_name, symbol, "order_missing", event_source, payload) or "order_missing"

    return None


def _payload_identifiers(payload: dict[str, Any] | None) -> set[str]:
    payload = payload or {}
    identifiers: set[str] = set()
    for key in ("order_id", "uuid", "client_order_id", "order_uid"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        identifiers.add(str(value))
    return identifiers


def _trade_matches_order(order_payload: dict[str, Any] | None, trade: dict[str, Any] | None) -> bool:
    if not order_payload or not trade:
        return False
    order_ids = _payload_identifiers(order_payload)
    if not order_ids:
        return False
    trade_ids = _payload_identifiers(trade)
    if order_ids & trade_ids:
        return True
    order_symbol = str(order_payload.get("symbol") or order_payload.get("ticker") or "").upper()
    trade_symbol = str(trade.get("symbol") or trade.get("ticker") or "").upper()
    if order_symbol and trade_symbol and order_symbol != trade_symbol:
        return False
    return False


def _matched_trades_for_order(
    order_payload: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not order_payload or not trades:
        return []
    return [trade for trade in trades if _trade_matches_order(order_payload, trade)]


def _build_order_progress_from_matched_trades(
    order_payload: dict[str, Any] | None,
    matched: list[dict[str, Any]] | None,
) -> tuple[str, dict[str, Any]] | None:
    if not order_payload or not matched:
        return None

    total_qty = sum(max(_safe_float(trade.get("qty"), 0.0), 0.0) for trade in matched)
    total_qty_contract = sum(max(_safe_float(trade.get("qty_contract"), 0.0), 0.0) for trade in matched)
    if total_qty <= 1e-12 and total_qty_contract <= 1e-12:
        return None

    weighted_notional = 0.0
    last_timestamp = 0
    trade_ids: list[str] = []
    for trade in matched:
        qty = max(_safe_float(trade.get("qty"), 0.0), 0.0)
        price = _safe_float(trade.get("price"), 0.0)
        weighted_notional += price * qty
        last_timestamp = max(
            last_timestamp,
            int(trade.get("create_time") or trade.get("timestamp") or trade.get("time") or 0),
        )
        trade_id = trade.get("trade_id")
        if trade_id not in (None, ""):
            trade_ids.append(str(trade_id))

    order_qty = max(_safe_float(order_payload.get("qty"), 0.0), 0.0)
    executed_qty = total_qty if total_qty > 0 else order_qty
    left_qty = max(order_qty - executed_qty, 0.0) if order_qty > 0 else 0.0
    avg_price = (weighted_notional / total_qty) if total_qty > 1e-12 else _safe_float(order_payload.get("avg_price"), 0.0)
    is_full_fill = order_qty <= 1e-12 or executed_qty + 1e-9 >= order_qty

    payload = _normalize_payload(order_payload)
    payload["status"] = "FILLED" if is_full_fill else "PARTIALLY_FILLED"
    payload["state"] = "not_exist" if is_full_fill else "exist"
    payload["executed_qty"] = executed_qty
    payload["left_qty"] = left_qty
    payload["avg_price"] = avg_price
    if total_qty_contract > 0:
        payload["executed_qty_contract"] = total_qty_contract
    payload["timestamp"] = last_timestamp or payload.get("timestamp") or int(time.time() * 1000)
    payload["trade_count"] = len(matched)
    payload["trade_ids"] = trade_ids
    payload["fill_ratio"] = (executed_qty / order_qty) if order_qty > 1e-12 else 1.0
    payload["matched_order_refs"] = sorted(_identifier_map(order_payload).values())
    payload["event_reason"] = "trade_progress"
    payload["transition"] = _build_transition(order_payload, payload)
    payload["raw"] = {
        "order": order_payload.get("raw", order_payload),
        "trades": [trade.get("raw", trade) for trade in matched],
    }
    return ("fill" if is_full_fill else "partial_fill", payload)


def build_order_progress_from_trades(
    order_payload: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None,
) -> tuple[str, dict[str, Any]] | None:
    matched = _matched_trades_for_order(order_payload, trades)
    return _build_order_progress_from_matched_trades(order_payload, matched)


def build_fill_payload_from_trades(order_payload: dict[str, Any] | None, trades: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    built = build_order_progress_from_trades(order_payload, trades)
    if built is None:
        return None
    event_type, payload = built
    if event_type != "fill":
        return None
    return payload


def schedule_order_progress_from_trades(
    exchange_name: str,
    symbol: str,
    order_payload: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None,
    *,
    event_source: str = "trade_poll",
) -> tuple[str | None, dict[str, Any] | None]:
    matched = _matched_trades_for_order(order_payload, trades)
    built = _build_order_progress_from_matched_trades(order_payload, matched)
    if built is None:
        return (None, None)
    event_type, payload = built
    schedule_trade_history_upsert(exchange_name, symbol, matched)
    schedule_order_event(exchange_name, symbol, event_type, event_source, payload)
    return (event_type, payload)


def schedule_order_fill_from_trades(
    exchange_name: str,
    symbol: str,
    order_payload: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None,
    *,
    event_source: str = "trade_poll",
) -> str | None:
    event_type, _ = schedule_order_progress_from_trades(
        exchange_name,
        symbol,
        order_payload,
        trades,
        event_source=event_source,
    )
    if event_type != "fill":
        return None
    return event_type


def _normalized_status_text(payload: dict[str, Any] | None) -> str:
    return str((payload or {}).get("status") or "").upper()


def _normalized_state_text(payload: dict[str, Any] | None) -> str:
    return str((payload or {}).get("state") or "").lower()


def _executed_qty(payload: dict[str, Any] | None) -> float:
    payload = payload or {}
    value = payload.get("executed_qty")
    if value is not None:
        return _safe_float(value, 0.0)
    qty = _safe_float(payload.get("qty"), 0.0)
    left_qty = payload.get("left_qty")
    if left_qty is None:
        return 0.0
    return max(qty - _safe_float(left_qty, 0.0), 0.0)


def _payload_changed(previous_payload: dict[str, Any] | None, current_payload: dict[str, Any] | None) -> bool:
    previous_payload = previous_payload or {}
    current_payload = current_payload or {}
    keys = (
        "status",
        "state",
        "price",
        "qty",
        "qty_contract",
        "executed_qty",
        "left_qty",
        "avg_price",
        "reduce_only",
    )
    for key in keys:
        if previous_payload.get(key) != current_payload.get(key):
            return True
    return False


def derive_polled_order_event_type(
    previous_payload: dict[str, Any] | None,
    current_payload: dict[str, Any] | None,
) -> str | None:
    if not current_payload:
        return None

    current_status = _normalized_status_text(current_payload)
    current_state = _normalized_state_text(current_payload)
    current_error = str(current_payload.get("error") or "").lower()
    previous_status = _normalized_status_text(previous_payload)
    previous_exec = _executed_qty(previous_payload)
    current_exec = _executed_qty(current_payload)
    exec_increased = current_exec > (previous_exec + 1e-12)

    if current_status in {"FILLED", "FINISHED"}:
        return "fill"
    if current_status in {"CANCELED", "CANCELLED"}:
        return "order_canceled"
    if current_status == "REJECTED":
        return "order_rejected"
    if current_status == "EXPIRED":
        return "order_expired"
    if current_state == "not_exist" or current_error == "not_exist":
        return "order_missing"
    if exec_increased:
        return "partial_fill"
    if not previous_payload:
        if current_state == "exist" or current_status in {"NEW", "OPEN", "LIVE", "PARTIALLY_FILLED"}:
            return "order_live"
        return "order_status_update"
    if previous_status != current_status or _payload_changed(previous_payload, current_payload):
        return "order_status_update"
    return None


def schedule_polled_order_status(
    exchange_name: str,
    symbol: str,
    previous_payload: dict[str, Any] | None,
    current_payload: dict[str, Any] | None,
    *,
    event_source: str = "rest_poll",
) -> str | None:
    event_type = derive_polled_order_event_type(previous_payload, current_payload)
    if event_type is None or not current_payload:
        return None

    payload = _normalize_payload(current_payload)
    if previous_payload:
        payload["previous"] = {
            "status": previous_payload.get("status"),
            "state": previous_payload.get("state"),
            "executed_qty": previous_payload.get("executed_qty"),
            "left_qty": previous_payload.get("left_qty"),
            "avg_price": previous_payload.get("avg_price"),
            "timestamp": previous_payload.get("timestamp"),
        }
        payload["fill_delta_qty"] = max(_executed_qty(current_payload) - _executed_qty(previous_payload), 0.0)
    payload["event_reason"] = "polled_order_status"
    payload["transition"] = _build_transition(previous_payload, current_payload)

    schedule_order_event(exchange_name, symbol, event_type, event_source, payload)
    return event_type
