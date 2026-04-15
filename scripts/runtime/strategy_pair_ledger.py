#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
load_dotenv()

from src.ops.persistence.db import ensure_schema, get_db_connection
from src.ops.persistence.pair_ledger import (
    load_live_event_rows,
    summarize_pair_ledgers,
    sync_pair_ledgers_from_events,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and sync pair-level live strategy ledger records.")
    parser.add_argument("--jsonl-path", default=str(PROJECT_ROOT / "logs" / "strategy_live.jsonl"))
    parser.add_argument("--run-meta", default=str(PROJECT_ROOT / "logs" / "strategy_live.run.json"))
    parser.add_argument("--started-after-ms", type=int, default=None)
    parser.add_argument("--ensure-schema", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--json", action="store_true", help="Print the full summary JSON.")
    return parser


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_run_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _recent_rows_payload(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    recent_rows = sorted(rows, key=lambda row: (row.get("opened_time_ms") or 0, row.get("pair_id") or ""))[-limit:]
    payload: list[dict[str, Any]] = []
    for row in recent_rows:
        payload.append(
            {
                "pair_id": row.get("pair_id"),
                "symbol": row.get("symbol"),
                "status": row.get("status"),
                "final_event_type": row.get("final_event_type"),
                "close_reason": row.get("close_reason"),
                "duration_sec": round(_safe_float(row.get("duration_sec")), 3),
                "entry_net_edge_usd": round(_safe_float(row.get("entry_net_edge_usd")), 6),
                "total_fee_usd": round(_safe_float(row.get("total_fee_usd")), 6),
                "total_realized_pnl_usd": round(_safe_float(row.get("total_realized_pnl_usd")), 6),
                "pnl_source": ((row.get("raw") or {}).get("pnl_source")),
                "auto_close_trigger_count": int(row.get("auto_close_trigger_count") or 0),
                "auto_close_not_completed_count": int(row.get("auto_close_not_completed_count") or 0),
                "recovery_event_count": int(row.get("recovery_event_count") or 0),
                "emergency_close_count": int(row.get("emergency_close_count") or 0),
            }
        )
    return payload


def main() -> int:
    args = build_parser().parse_args()

    run_meta = _load_run_meta(Path(args.run_meta))
    started_after_ms = args.started_after_ms
    if started_after_ms is None:
        started_after_ms = int(run_meta.get("started_at_ms") or 0) or None

    event_rows = load_live_event_rows(args.jsonl_path, started_at_ms=started_after_ms)

    db_sync_error: str | None = None
    ledger_rows: list[dict[str, Any]] = []
    conn = None
    try:
        conn = get_db_connection()
        if args.ensure_schema:
            ensure_schema(conn)
        ledger_rows = sync_pair_ledgers_from_events(conn, event_rows)
    except Exception as exc:
        db_sync_error = repr(exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    summary = summarize_pair_ledgers(ledger_rows)
    output = {
        "run_started_at_iso": run_meta.get("started_at_iso"),
        "started_after_ms": started_after_ms,
        "event_rows": len(event_rows),
        "pair_rows": summary["pair_rows"],
        "closed_pairs": summary["closed_pairs"],
        "open_pairs": summary["open_pairs"],
        "total_fee_usd": summary["total_fee_usd"],
        "total_realized_pnl_usd": summary["total_realized_pnl_usd"],
        "by_symbol": summary["by_symbol"],
        "recent_pairs": _recent_rows_payload(ledger_rows, args.limit),
        "db_sync_error": db_sync_error,
    }

    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    print("run_started_at_iso =", output["run_started_at_iso"])
    print("event_rows =", output["event_rows"])
    print("pair_rows =", output["pair_rows"])
    print("closed_pairs =", output["closed_pairs"])
    print("open_pairs =", output["open_pairs"])
    print("total_fee_usd =", round(_safe_float(output["total_fee_usd"]), 6))
    print("total_realized_pnl_usd =", round(_safe_float(output["total_realized_pnl_usd"]), 6))
    if db_sync_error:
        print("db_sync_error =", db_sync_error)
    print("by_symbol =")
    for symbol, symbol_summary in output["by_symbol"].items():
        print(f"{symbol} = {json.dumps(symbol_summary, ensure_ascii=False)}")
    print("recent_pairs =")
    for row in output["recent_pairs"]:
        print(json.dumps(row, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
