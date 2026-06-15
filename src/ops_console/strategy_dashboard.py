from __future__ import annotations

import json
import os
import time
from collections import Counter, deque
from pathlib import Path
from typing import Any

from src.ops_console.runtime import PROJECT_ROOT, read_entry_control


KEY_EVENT_TYPES = (
    "runtime_started",
    "runtime_stopped",
    "candidate_scan_summary",
    "maker_entry_preposition_candidate_selected",
    "maker_entry_candidate_queue_selected",
    "maker_entry_order_submitted",
    "maker_entry_fill_detected",
    "maker_entry_order_cancelled",
    "maker_entry_symbol_paused",
    "pair_opened",
    "pair_closed",
    "pair_close_pending",
    "pair_close_leg_failed",
    "pair_entry_overfill_detected",
    "pair_entry_overfill_flatten_failed",
    "pair_closed_after_recovery",
    "pair_neutralized_after_close_failure",
    "entry_quality_analysis",
    "api_pressure_summary",
    "shutdown_cleanup_started",
    "shutdown_cleanup_done",
)

DANGER_EVENT_TYPES = {
    "pair_close_pending",
    "pair_close_leg_failed",
    "pair_entry_overfill_detected",
    "pair_entry_overfill_flatten_failed",
    "entry_bad_edge_rollback_failed",
}


def now_ms() -> int:
    return int(time.time() * 1000)


def resolve_path(raw: str | None, default: Path) -> Path:
    path = Path(raw or default).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_read_error": str(exc)}
    return payload if isinstance(payload, dict) else {"_read_error": "json_payload_not_object"}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "pid": None, "running": False}
    pid = safe_int(path.read_text(encoding="utf-8", errors="replace").strip(), 0)
    return {"path": str(path), "pid": pid or None, "running": is_pid_running(pid)}


def tail_jsonl(path: Path, *, started_at_ms: int, max_rows: int = 6000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: deque[dict[str, Any]] = deque(maxlen=max_rows)
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if safe_int(row.get("ts_ms"), 0) >= started_at_ms:
                rows.append(row)
    return list(rows)


def compact_candidate(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    return {
        "candidate_id": raw.get("candidate_id"),
        "symbol": raw.get("symbol"),
        "pair": raw.get("pair"),
        "long_exchange": raw.get("long_exchange"),
        "short_exchange": raw.get("short_exchange"),
        "qty": safe_float(raw.get("qty")),
        "notional_usd": safe_float(raw.get("notional_usd")),
        "expected_net_edge_usd": safe_float(raw.get("expected_net_edge_usd")),
        "expected_net_edge_bps": safe_float(raw.get("expected_net_edge_bps")),
        "effective_funding_pnl_usd": safe_float(raw.get("effective_funding_pnl_usd")),
        "can_enter": bool(raw.get("can_enter")),
        "block_reasons": list(raw.get("block_reasons") or []),
    }


def compact_event(row: dict[str, Any]) -> dict[str, Any]:
    event_type = str(row.get("event_type") or "unknown")
    compact: dict[str, Any] = {
        "ts_utc": row.get("ts_utc"),
        "event_type": event_type,
    }
    if event_type == "candidate_scan_summary":
        compact.update(
            {
                "total_candidates": row.get("total_candidates"),
                "enterable_candidates": row.get("enterable_candidates"),
                "top_candidate": compact_candidate(row.get("top_candidate")),
                "top_block_reason": row.get("top_block_reason"),
            }
        )
    elif event_type == "api_pressure_summary":
        compact["exchanges"] = compact_api_pressure(row)
    else:
        for key in (
            "symbol",
            "reason",
            "pair_id",
            "failed_exchange",
            "failed_side",
            "error_code",
            "error_message",
        ):
            if key in row:
                compact[key] = row.get(key)
        pair = row.get("pair")
        if isinstance(pair, dict):
            compact["pair"] = {
                "pair_id": pair.get("pair_id"),
                "symbol": pair.get("symbol"),
                "qty": pair.get("qty"),
                "last_close_reason": pair.get("last_close_reason"),
            }
    return compact


def compact_api_pressure(row: dict[str, Any] | None) -> dict[str, Any]:
    exchanges: dict[str, Any] = {}
    if not isinstance(row, dict):
        return exchanges
    for exchange_name, raw in (row.get("exchanges") or {}).items():
        exchanges[str(exchange_name)] = {
            "remote_calls": safe_int(raw.get("remote_calls")),
            "local_skips": safe_int(raw.get("local_skips")),
            "retry_exhausted": safe_int(raw.get("retry_exhausted")),
            "rate_limit_violations": safe_int(raw.get("rate_limit_violations")),
            "ip_ban_events": safe_int(raw.get("ip_ban_events")),
            "latency_ms_max": safe_float(raw.get("latency_ms_max")),
            "max_request_weight_usage_1m": safe_float(raw.get("max_request_weight_usage_1m")),
            "max_order_count_usage_10s": safe_float(raw.get("max_order_count_usage_10s")),
            "max_order_count_usage_1m": safe_float(raw.get("max_order_count_usage_1m")),
            "transient_counts": raw.get("transient_counts") or {},
            "by_category": raw.get("by_category") or {},
        }
    return exchanges


def utc_day_start_ms() -> int:
    now = time.gmtime()
    return int(
        time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0))
        - time.timezone
    ) * 1000


def realized_pnl_summary(account_label: str | None, started_at_ms: int) -> dict[str, Any]:
    """Best-effort realized PnL from trade_history. Never raises — the cockpit
    must keep rendering even if the DB is down."""
    summary = {
        "ok": False,
        "account_label": account_label,
        "today_realized_usd": 0.0,
        "today_fee_usd": 0.0,
        "today_trades": 0,
        "run_realized_usd": 0.0,
        "run_fee_usd": 0.0,
        "run_trades": 0,
        "by_symbol": [],
    }
    if not account_label:
        return summary
    try:
        from src.ops.persistence.db import db_persistence_enabled, get_db_connection

        if not db_persistence_enabled():
            return summary
        day_start = utc_day_start_ms()
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COALESCE(SUM(realized_pnl_usdt),0), COALESCE(SUM(fee),0), COUNT(*) "
                "FROM trade_history WHERE account_label=%s AND desktop_time_ms>=%s",
                (account_label, day_start),
            )
            row = cur.fetchone() or (0, 0, 0)
            summary["today_realized_usd"] = float(row[0] or 0.0)
            summary["today_fee_usd"] = float(row[1] or 0.0)
            summary["today_trades"] = int(row[2] or 0)

            cur.execute(
                "SELECT COALESCE(SUM(realized_pnl_usdt),0), COALESCE(SUM(fee),0), COUNT(*) "
                "FROM trade_history WHERE account_label=%s AND desktop_time_ms>=%s",
                (account_label, max(started_at_ms, 0)),
            )
            row = cur.fetchone() or (0, 0, 0)
            summary["run_realized_usd"] = float(row[0] or 0.0)
            summary["run_fee_usd"] = float(row[1] or 0.0)
            summary["run_trades"] = int(row[2] or 0)

            cur.execute(
                "SELECT symbol, COALESCE(SUM(realized_pnl_usdt),0), COALESCE(SUM(fee),0), COUNT(*) "
                "FROM trade_history WHERE account_label=%s AND desktop_time_ms>=%s "
                "GROUP BY symbol ORDER BY SUM(realized_pnl_usdt) ASC",
                (account_label, max(started_at_ms, 0)),
            )
            summary["by_symbol"] = [
                {
                    "symbol": r[0],
                    "realized_usd": float(r[1] or 0.0),
                    "fee_usd": float(r[2] or 0.0),
                    "trades": int(r[3] or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            summary["ok"] = True
        finally:
            conn.close()
    except Exception as exc:
        summary["error"] = str(exc)[:200]
    return summary


def latest_artifacts(log_dir: Path) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    patterns = {
        "final_report": "final_run_report*.md",
        "afterrun_summary": "strategy_afterrun_summary*.md",
        "order_trade_reconcile": "order_trade_reconcile*.md",
    }
    for key, pattern in patterns.items():
        matches = sorted(log_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
        if matches:
            path = matches[0]
            artifacts[key] = {
                "path": str(path),
                "mtime_ms": int(path.stat().st_mtime * 1000),
                "size_bytes": path.stat().st_size,
            }
    return artifacts


def build_strategy_dashboard_state() -> dict[str, Any]:
    log_dir = resolve_path(os.getenv("STRATEGY_LIVE_LOG_DIR"), PROJECT_ROOT / "logs")
    pid_path = resolve_path(os.getenv("STRATEGY_LIVE_PID_FILE"), log_dir / "strategy_live.pid")
    meta_path = resolve_path(os.getenv("STRATEGY_LIVE_RUN_META_FILE"), log_dir / "strategy_live.run.json")
    meta = read_json(meta_path)
    jsonl_path = resolve_path(os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH"), Path(str(meta.get("jsonl_log") or log_dir / "strategy_live.jsonl")))
    stdout_path = resolve_path(os.getenv("STRATEGY_LIVE_STDOUT_LOG"), Path(str(meta.get("stdout_log") or log_dir / "strategy_live.out")))

    started_at_ms = safe_int(meta.get("started_at_ms"), 0)
    rows = tail_jsonl(jsonl_path, started_at_ms=started_at_ms) if started_at_ms else []
    event_counts = Counter(str(row.get("event_type") or "unknown") for row in rows)
    latest_by_type: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_type = str(row.get("event_type") or "unknown")
        latest_by_type[event_type] = row

    candidate = latest_by_type.get("candidate_scan_summary") or {}
    api_pressure = latest_by_type.get("api_pressure_summary") or {}
    runtime_stopped = latest_by_type.get("runtime_stopped")
    shutdown_done = latest_by_type.get("shutdown_cleanup_done")
    danger_counts = {event: event_counts.get(event, 0) for event in sorted(DANGER_EVENT_TYPES)}

    return {
        "timestamp_ms": now_ms(),
        "paths": {
            "log_dir": str(log_dir),
            "pid_file": str(pid_path),
            "meta_file": str(meta_path),
            "jsonl_log": str(jsonl_path),
            "stdout_log": str(stdout_path),
        },
        "runtime": {
            **read_pid_file(pid_path),
            "started_at_ms": started_at_ms,
            "started_at_iso": meta.get("started_at_iso"),
            "duration": meta.get("duration"),
            "execution_profile": meta.get("execution_profile") or "legacy_unknown",
            "env_file": meta.get("env_file"),
            "symbols": meta.get("symbols"),
            "exchanges": meta.get("exchanges"),
            "account_label": meta.get("account_label"),
            "logic": meta.get("logic"),
            "runtime_stopped": bool(runtime_stopped),
            "entry_control": read_entry_control(),
        },
        "health": {
            "danger_total": sum(danger_counts.values()),
            "danger_counts": danger_counts,
            "shutdown_remaining_positions": (shutdown_done or {}).get("remaining_positions"),
            "shutdown_pending_pair_ids": (shutdown_done or {}).get("pending_pair_ids"),
        },
        "events": {
            "rows_loaded": len(rows),
            "counts": {event: event_counts.get(event, 0) for event in KEY_EVENT_TYPES},
            "top_counts": dict(event_counts.most_common(20)),
            "recent": [compact_event(row) for row in rows[-25:]],
        },
        "candidate": {
            "latest_ts_utc": candidate.get("ts_utc"),
            "total_candidates": candidate.get("total_candidates"),
            "enterable_candidates": candidate.get("enterable_candidates"),
            "blocked_candidates": candidate.get("blocked_candidates"),
            "top_candidate": compact_candidate(candidate.get("top_candidate")),
            "top_enterable_candidate": compact_candidate(candidate.get("top_enterable_candidate")),
            "top_blocked_candidate": compact_candidate(candidate.get("top_blocked_candidate")),
            "top_block_reason": candidate.get("top_block_reason"),
            "reason_counts": candidate.get("reason_counts") or {},
        },
        "api_pressure": {
            "latest_ts_utc": api_pressure.get("ts_utc"),
            "window_sec": api_pressure.get("window_sec"),
            "exchanges": compact_api_pressure(api_pressure),
        },
        "pnl": realized_pnl_summary(meta.get("account_label"), started_at_ms),
        "artifacts": latest_artifacts(log_dir),
        "meta": meta,
    }
