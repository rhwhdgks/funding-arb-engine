import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .db import fetch_trade_history_rows, upsert_pair_ledgers

TERMINAL_EVENT_TYPES = {
    "pair_closed",
    "pair_closed_after_emergency",
    "pair_closed_after_recovery",
    "pair_closed_detected",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def load_live_event_rows(path: str | Path, *, started_at_ms: int | None = None) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        ts_ms = _safe_int(row.get("ts_ms"))
        if started_at_ms is not None and ts_ms < started_at_ms:
            continue
        rows.append(row)
    return rows


def _base_record_from_pair(row: dict[str, Any], pair: dict[str, Any]) -> dict[str, Any]:
    ts_ms = _safe_int(row.get("ts_ms"))
    entry_time_ms = _safe_int(pair.get("entry_time_ms"), ts_ms)
    candidate = row.get("candidate") or {}
    return {
        "pair_id": str(pair.get("pair_id") or ""),
        "symbol": str(pair.get("symbol") or "").upper(),
        "long_exchange": str(pair.get("long_exchange") or "").lower(),
        "short_exchange": str(pair.get("short_exchange") or "").lower(),
        "opened_time_ms": entry_time_ms or ts_ms,
        "closed_time_ms": None,
        "last_event_time_ms": ts_ms,
        "duration_sec": 0.0,
        "status": str(pair.get("status") or "unknown"),
        "final_event_type": str(row.get("event_type") or "unknown"),
        "close_reason": str(pair.get("last_close_reason") or row.get("reason") or "").strip() or None,
        "requested_qty": _safe_float(pair.get("requested_qty")),
        "pair_qty": _safe_float(pair.get("qty")),
        "long_entry_price": _safe_float(pair.get("long_entry_price")),
        "short_entry_price": _safe_float(pair.get("short_entry_price")),
        "entry_net_edge_usd": _safe_float(pair.get("entry_net_edge_usd")),
        "expected_funding_pnl_usd": candidate.get("effective_funding_pnl_usd"),
        "funding_long_rate": _safe_float(pair.get("funding_long_rate")),
        "funding_short_rate": _safe_float(pair.get("funding_short_rate")),
        "last_hold_decision": str(pair.get("last_hold_decision") or "").strip() or None,
        "last_hold_pnl_usd": _safe_float(pair.get("last_hold_pnl_usd")),
        "last_close_pnl_usd": _safe_float(pair.get("last_close_pnl_usd")),
        "last_close_edge_usd": _safe_float(pair.get("last_close_edge_usd")),
        "auto_close_trigger_count": 0,
        "auto_close_not_completed_count": 0,
        "close_pending_count": 0,
        "partial_close_count": 0,
        "recovery_event_count": 0,
        "emergency_close_count": 0,
        "detected_close_count": 0,
        "close_noop_count": 0,
        "trade_count": 0,
        "long_trade_count": 0,
        "short_trade_count": 0,
        "total_fee_usd": 0.0,
        "long_fee_usd": 0.0,
        "short_fee_usd": 0.0,
        "total_realized_pnl_usd": 0.0,
        "long_realized_pnl_usd": 0.0,
        "short_realized_pnl_usd": 0.0,
        "event_counts": Counter(),
        "event_types": [],
        "trigger_reasons": [],
        "forced_flat_exchanges": [],
    }


def build_pair_ledger_records(event_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}

    for row in event_rows:
        pair = row.get("pair")
        if not isinstance(pair, dict):
            continue
        pair_id = str(pair.get("pair_id") or "").strip()
        if not pair_id:
            continue

        record = records.setdefault(pair_id, _base_record_from_pair(row, pair))
        event_type = str(row.get("event_type") or "unknown")
        ts_ms = _safe_int(row.get("ts_ms"))

        record["last_event_time_ms"] = max(record["last_event_time_ms"], ts_ms)
        record["final_event_type"] = event_type
        record["symbol"] = str(pair.get("symbol") or record["symbol"]).upper()
        record["long_exchange"] = str(pair.get("long_exchange") or record["long_exchange"]).lower()
        record["short_exchange"] = str(pair.get("short_exchange") or record["short_exchange"]).lower()
        record["requested_qty"] = _safe_float(pair.get("requested_qty"), record["requested_qty"])
        record["pair_qty"] = _safe_float(pair.get("qty"), record["pair_qty"])
        record["long_entry_price"] = _safe_float(pair.get("long_entry_price"), record["long_entry_price"])
        record["short_entry_price"] = _safe_float(pair.get("short_entry_price"), record["short_entry_price"])
        record["entry_net_edge_usd"] = _safe_float(pair.get("entry_net_edge_usd"), record["entry_net_edge_usd"])
        record["funding_long_rate"] = _safe_float(pair.get("funding_long_rate"), record["funding_long_rate"])
        record["funding_short_rate"] = _safe_float(pair.get("funding_short_rate"), record["funding_short_rate"])
        record["last_hold_decision"] = str(pair.get("last_hold_decision") or record["last_hold_decision"] or "").strip() or None
        record["last_hold_pnl_usd"] = _safe_float(pair.get("last_hold_pnl_usd"), record["last_hold_pnl_usd"])
        record["last_close_pnl_usd"] = _safe_float(pair.get("last_close_pnl_usd"), record["last_close_pnl_usd"])
        record["last_close_edge_usd"] = _safe_float(pair.get("last_close_edge_usd"), record["last_close_edge_usd"])
        record["status"] = str(pair.get("status") or record["status"] or "unknown")
        if row.get("reason"):
            record["close_reason"] = str(row.get("reason"))
        elif pair.get("last_close_reason"):
            record["close_reason"] = str(pair.get("last_close_reason"))

        if event_type == "pair_opened":
            record["opened_time_ms"] = _safe_int(pair.get("entry_time_ms"), ts_ms) or ts_ms
            candidate = row.get("candidate") or {}
            if "effective_funding_pnl_usd" in candidate:
                record["expected_funding_pnl_usd"] = _safe_float(candidate.get("effective_funding_pnl_usd"))
        elif event_type in TERMINAL_EVENT_TYPES:
            record["closed_time_ms"] = ts_ms
            record["status"] = "closed"
        elif event_type == "pair_auto_close_trigger":
            record["auto_close_trigger_count"] += 1
            if row.get("reason"):
                record["trigger_reasons"].append(str(row.get("reason")))
        elif event_type == "pair_auto_close_not_completed":
            record["auto_close_not_completed_count"] += 1
        elif event_type == "pair_close_pending":
            record["close_pending_count"] += 1
        elif event_type in {"pair_partial_close_done", "pair_partial_close_done_after_emergency"}:
            record["partial_close_count"] += 1
        elif event_type == "pair_closed_after_emergency":
            record["emergency_close_count"] += 1
        elif event_type == "pair_closed_after_recovery":
            record["recovery_event_count"] += 1
            record["forced_flat_exchanges"] = list(row.get("forced_flat_exchanges") or [])
        elif event_type == "pair_recovered_balanced_open":
            record["recovery_event_count"] += 1
        elif event_type == "pair_closed_detected":
            record["detected_close_count"] += 1
        elif event_type == "pair_close_noop":
            record["close_noop_count"] += 1

        record["event_counts"][event_type] += 1
        record["event_types"].append(event_type)

    finalized: list[dict[str, Any]] = []
    for record in records.values():
        end_ms = record["closed_time_ms"] or record["last_event_time_ms"] or record["opened_time_ms"]
        record["duration_sec"] = max((end_ms - record["opened_time_ms"]) / 1000.0, 0.0)
        if record["closed_time_ms"] is None and record["final_event_type"] in TERMINAL_EVENT_TYPES:
            record["closed_time_ms"] = record["last_event_time_ms"]
        if record["closed_time_ms"] is not None:
            record["status"] = "closed"
        raw = {
            "event_counts": dict(record.pop("event_counts")),
            "event_types": record.pop("event_types"),
            "trigger_reasons": record.pop("trigger_reasons"),
            "forced_flat_exchanges": record.pop("forced_flat_exchanges"),
        }
        record["raw"] = raw
        finalized.append(record)

    finalized.sort(key=lambda row: (row["opened_time_ms"], row["pair_id"]))
    return finalized


def enrich_pair_ledgers_with_trade_history(
    conn,
    ledgers: list[dict[str, Any]],
    *,
    entry_buffer_ms: int = 10_000,
    exit_buffer_ms: int = 10_000,
) -> list[dict[str, Any]]:
    grouped_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ledgers:
        grouped_by_symbol[row["symbol"]].append(row)

    for symbol_rows in grouped_by_symbol.values():
        symbol_rows.sort(key=lambda row: (row["opened_time_ms"], row["pair_id"]))

    for symbol, symbol_rows in grouped_by_symbol.items():
        for index, row in enumerate(symbol_rows):
            start_ms = max(int(row["opened_time_ms"]) - entry_buffer_ms, 0)
            end_ms = int((row.get("closed_time_ms") or row.get("last_event_time_ms") or row["opened_time_ms"])) + exit_buffer_ms
            if index > 0:
                previous_row = symbol_rows[index - 1]
                previous_end_ms = int(previous_row.get("closed_time_ms") or previous_row.get("last_event_time_ms") or previous_row["opened_time_ms"])
                start_ms = max(start_ms, previous_end_ms + 1)
            if index + 1 < len(symbol_rows):
                next_row = symbol_rows[index + 1]
                next_start_ms = int(next_row["opened_time_ms"])
                end_ms = min(end_ms, next_start_ms - 1)

            trades = fetch_trade_history_rows(
                conn,
                symbol=symbol,
                exchange_names=[row["long_exchange"], row["short_exchange"]],
                start_time_ms=start_ms,
                end_time_ms=end_ms,
            )
            total_fee_usd = 0.0
            total_realized_pnl_usd = 0.0
            pnl_source = "trade_history"
            long_fee_usd = 0.0
            short_fee_usd = 0.0
            long_realized_pnl_usd = 0.0
            short_realized_pnl_usd = 0.0
            long_trade_count = 0
            short_trade_count = 0
            has_realized_pnl_value = False
            for trade in trades:
                exchange_name = str(trade.get("exchange_name") or "").lower()
                fee_value = abs(_safe_float(trade.get("fee")))
                realized_raw = trade.get("realized_pnl")
                if realized_raw not in (None, ""):
                    has_realized_pnl_value = True
                realized_value = _safe_float(realized_raw)
                total_fee_usd += fee_value
                total_realized_pnl_usd += realized_value
                if exchange_name == row["long_exchange"]:
                    long_trade_count += 1
                    long_fee_usd += fee_value
                    long_realized_pnl_usd += realized_value
                elif exchange_name == row["short_exchange"]:
                    short_trade_count += 1
                    short_fee_usd += fee_value
                    short_realized_pnl_usd += realized_value

            if row.get("status") == "closed" and (not trades or not has_realized_pnl_value):
                total_realized_pnl_usd = _safe_float(row.get("last_close_pnl_usd"))
                long_realized_pnl_usd = 0.0
                short_realized_pnl_usd = 0.0
                pnl_source = "strategy_last_close_pnl_fallback"

            row["trade_count"] = len(trades)
            row["long_trade_count"] = long_trade_count
            row["short_trade_count"] = short_trade_count
            row["total_fee_usd"] = total_fee_usd
            row["long_fee_usd"] = long_fee_usd
            row["short_fee_usd"] = short_fee_usd
            row["total_realized_pnl_usd"] = total_realized_pnl_usd
            row["long_realized_pnl_usd"] = long_realized_pnl_usd
            row["short_realized_pnl_usd"] = short_realized_pnl_usd
            row["raw"]["trade_window"] = {"start_time_ms": start_ms, "end_time_ms": end_ms}
            row["raw"]["trade_ids"] = [str(trade.get("trade_id") or "") for trade in trades]
            row["raw"]["pnl_source"] = pnl_source
            row["raw"]["fee_source"] = "trade_history" if trades else "unavailable"

    return ledgers


def sync_pair_ledgers_from_events(
    conn,
    event_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ledgers = build_pair_ledger_records(event_rows)
    enrich_pair_ledgers_with_trade_history(conn, ledgers)
    upsert_pair_ledgers(conn, ledgers)
    return ledgers


def summarize_pair_ledgers(ledgers: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "pair_rows": len(ledgers),
        "closed_pairs": 0,
        "open_pairs": 0,
        "total_fee_usd": 0.0,
        "total_realized_pnl_usd": 0.0,
        "by_symbol": {},
    }
    symbol_counters: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "pairs": 0,
            "closed_pairs": 0,
            "open_pairs": 0,
            "total_fee_usd": 0.0,
            "total_realized_pnl_usd": 0.0,
        }
    )
    for row in ledgers:
        status = str(row.get("status") or "")
        if status == "closed":
            summary["closed_pairs"] += 1
        else:
            summary["open_pairs"] += 1
        summary["total_fee_usd"] += _safe_float(row.get("total_fee_usd"))
        summary["total_realized_pnl_usd"] += _safe_float(row.get("total_realized_pnl_usd"))

        symbol_summary = symbol_counters[str(row.get("symbol") or "").upper()]
        symbol_summary["pairs"] += 1
        if status == "closed":
            symbol_summary["closed_pairs"] += 1
        else:
            symbol_summary["open_pairs"] += 1
        symbol_summary["total_fee_usd"] += _safe_float(row.get("total_fee_usd"))
        symbol_summary["total_realized_pnl_usd"] += _safe_float(row.get("total_realized_pnl_usd"))

    summary["by_symbol"] = dict(sorted(symbol_counters.items()))
    return summary
