from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
from typing import Any

from .db import db_persistence_enabled, get_db_connection, json_dumps
from .trading_dev_store import upsert_strategy_entry_quality_row


_BG_MAX_WORKERS = max(int(os.getenv("ENTRY_QUALITY_MAX_WORKERS", "2") or 2), 1)
_BG_EXECUTOR = ThreadPoolExecutor(max_workers=_BG_MAX_WORKERS, thread_name_prefix="entry-quality")
_REF_KEYS = {
    "client_order_id",
    "first_order_id",
    "hedge_order_id",
    "order_id",
    "order_uuid",
    "trade_id",
    "uuid",
}


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _decimal_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    return format(decimal_value, "f")


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _run_id_from_event(event: dict[str, Any]) -> str | None:
    return _string_or_none(
        event.get("run_id")
        or os.getenv("STRATEGY_RUN_ID")
        or os.getenv("STRATEGY_LIVE_RUN_ID")
    )


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _collect_ref_values(value: Any, *, depth: int = 0) -> Any:
    if depth > 3:
        return None
    if isinstance(value, dict):
        refs: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            key_lower = key_text.lower()
            if key_lower in _REF_KEYS and child not in (None, "", [], {}):
                refs[key_text] = child
                continue
            nested = _collect_ref_values(child, depth=depth + 1)
            if nested not in (None, {}, []):
                refs[key_text] = nested
        return refs or None
    if isinstance(value, list):
        refs_list = [
            item_refs
            for item in value
            if (item_refs := _collect_ref_values(item, depth=depth + 1)) not in (None, {}, [])
        ]
        return refs_list or None
    return None


def build_strategy_entry_quality_row(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("event_type") != "entry_quality_analysis":
        return None

    pair_id = _string_or_none(event.get("pair_id"))
    symbol = _string_or_none(event.get("symbol"))
    entry_time_ms = (
        _safe_int(event.get("entry_time_ms"))
        or _safe_int(event.get("ts_ms"))
        or _safe_int(event.get("hedge_done_ms"))
    )
    if not pair_id or not symbol or entry_time_ms is None:
        return None

    trade_refs = _collect_ref_values(event)
    return {
        "run_id": _run_id_from_event(event),
        "pair_id": pair_id,
        "symbol": symbol.upper(),
        "entry_time_ms": entry_time_ms,
        "expected_net_edge_bps": _decimal_string(event.get("expected_net_edge_bps")),
        "actual_open_edge_bps": _decimal_string(event.get("actual_open_edge_bps")),
        "entry_slippage_bps": _decimal_string(
            _first_present(event.get("entry_slippage_bps"), event.get("entry_edge_slippage_bps"))
        ),
        "hedge_latency_ms": (
            _safe_int(_first_present(
                event.get("hedge_latency_ms"),
                event.get("first_fill_to_hedge_fill_ms"),
                event.get("hedge_elapsed_ms"),
            ))
        ),
        "trade_refs_json": json_dumps(trade_refs) if trade_refs else None,
        "raw_json": json_dumps(event),
    }


def _store_entry_quality_sync(event: dict[str, Any]) -> None:
    if not db_persistence_enabled():
        return

    row = build_strategy_entry_quality_row(event)
    if row is None:
        return

    conn = None
    try:
        conn = get_db_connection()
        upsert_strategy_entry_quality_row(conn, row)
    except Exception as exc:
        pair_id = event.get("pair_id")
        print(f"[entry-quality] skip pair_id={pair_id}: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def schedule_entry_quality_persist(event: dict[str, Any]) -> None:
    if event.get("event_type") != "entry_quality_analysis" or not db_persistence_enabled():
        return

    event_copy = dict(event)
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(_BG_EXECUTOR, lambda: _store_entry_quality_sync(event_copy))
    except RuntimeError:
        _store_entry_quality_sync(event_copy)
