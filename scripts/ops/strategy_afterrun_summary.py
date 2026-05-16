#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path


OPEN_ORDER_STATUSES = (
    "ACKNOWLEDGED",
    "LIVE",
    "NEW",
    "OPEN",
    "PARTIALLY_FILLED",
    "SUBMITTED",
    "UPDATED",
)

FAIL_EVENT_TYPES = (
    "pair_entry_overfill_detected",
    "pair_entry_overfill_flatten_failed",
    "pair_close_leg_failed",
    "pair_close_pending",
)

WARN_EVENT_TYPES = (
    "pair_closed_after_recovery",
    "pair_neutralized_after_close_failure",
    "pair_micro_residual_cleanup_failed",
)

INFO_EVENT_TYPES = (
    "pair_entry_micro_partial_detected",
    "pair_entry_micro_residual_ignored",
    "pair_direction_micro_residual_detected",
    "pair_closed_after_micro_residual_cleanup",
    "entry_order_disappeared_settle_started",
    "entry_order_disappeared_resolved",
    "entry_limit_order_submit_preflight",
    "entry_limit_order_submit_result",
)

SUBMIT_MARKET_AGE_WARN_MS = 1500


def parse_duration_ms(value: str) -> int:
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", text)
    if not match:
        return 0
    amount = float(match.group(1))
    unit = match.group(2) or "s"
    factor = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return int(amount * factor)


def is_forever_duration(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"0", "forever", "infinite", "infinity", "none", "unlimited"}


def run_cmd(root: Path, cmd: str, timeout: int = 60) -> str:
    try:
        completed = subprocess.run(
            cmd,
            shell=True,
            cwd=root,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        return (completed.stdout + completed.stderr).strip()
    except Exception as exc:
        return f"command_error: {exc}"


def load_run_rows(jsonl_path: Path, started_at_ms: int) -> list[dict]:
    rows: list[dict] = []
    if not jsonl_path.exists():
        return rows
    for line in jsonl_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if int(row.get("ts_ms") or 0) >= started_at_ms:
            rows.append(row)
    return rows


def format_duration_ms(value: int | float) -> str:
    total_seconds = max(int(value / 1000), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def add_check(checks: list[dict], status: str, name: str, detail: str) -> None:
    checks.append({"status": status, "name": name, "detail": detail})


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * pct))
    return float(ordered[max(0, min(index, len(ordered) - 1))])


def numeric_values(rows: list[dict], event_type: str, key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        if row.get("event_type") != event_type:
            continue
        try:
            value = float(row.get(key))
        except Exception:
            continue
        values.append(value)
    return values


def summarize_entry_submit_telemetry(run_rows: list[dict]) -> dict:
    preflight_rows = [row for row in run_rows if row.get("event_type") == "entry_limit_order_submit_preflight"]
    result_rows = [row for row in run_rows if row.get("event_type") == "entry_limit_order_submit_result"]
    market_age_values = numeric_values(run_rows, "entry_limit_order_submit_preflight", "market_age_ms")
    stale_age_values = numeric_values(run_rows, "entry_limit_order_submit_preflight", "stale_age_ms")
    submit_latency_values = numeric_values(run_rows, "entry_limit_order_submit_result", "submit_latency_ms")
    stale_preflight_rows = [
        row for row in preflight_rows
        if float(row.get("market_age_ms") or 0) > SUBMIT_MARKET_AGE_WARN_MS
        or row.get("can_trade") is False
        or bool(row.get("block_reasons") or [])
    ]
    return {
        "preflight_count": len(preflight_rows),
        "result_count": len(result_rows),
        "market_age_ms_avg": (sum(market_age_values) / len(market_age_values)) if market_age_values else 0.0,
        "market_age_ms_p95": percentile(market_age_values, 0.95),
        "market_age_ms_max": max(market_age_values) if market_age_values else 0.0,
        "stale_age_ms_max": max(stale_age_values) if stale_age_values else 0.0,
        "submit_latency_ms_avg": (sum(submit_latency_values) / len(submit_latency_values)) if submit_latency_values else 0.0,
        "submit_latency_ms_p95": percentile(submit_latency_values, 0.95),
        "submit_latency_ms_max": max(submit_latency_values) if submit_latency_values else 0.0,
        "stale_preflight_count": len(stale_preflight_rows),
        "warn_threshold_ms": SUBMIT_MARKET_AGE_WARN_MS,
    }


def evaluate_soak(
    *,
    meta: dict,
    status_text: str,
    event_counts: Counter,
    run_rows: list[dict],
    submit_telemetry: dict,
    latest_shutdown: dict | None,
    open_order_rows: list[tuple],
    db_error: str,
    critical_lines: list[str],
) -> dict:
    checks: list[dict] = []

    runtime_started = event_counts.get("runtime_started", 0)
    runtime_stopped = event_counts.get("runtime_stopped", 0)
    add_check(
        checks,
        "PASS" if runtime_started >= 1 else "FAIL",
        "runtime_started_event",
        f"count={runtime_started}",
    )

    for event_type in FAIL_EVENT_TYPES:
        count = event_counts.get(event_type, 0)
        add_check(
            checks,
            "PASS" if count == 0 else "FAIL",
            f"{event_type}_zero",
            f"count={count}",
        )

    for event_type in WARN_EVENT_TYPES:
        count = event_counts.get(event_type, 0)
        add_check(
            checks,
            "PASS" if count == 0 else "WARN",
            f"{event_type}_zero",
            f"count={count}",
        )

    for event_type in INFO_EVENT_TYPES:
        count = event_counts.get(event_type, 0)
        add_check(
            checks,
            "INFO",
            f"{event_type}_count",
            f"count={count}",
        )

    preflight_count = int(submit_telemetry.get("preflight_count") or 0)
    stale_preflight_count = int(submit_telemetry.get("stale_preflight_count") or 0)
    if preflight_count <= 0:
        add_check(
            checks,
            "INFO",
            "entry_submit_price_age_telemetry_present",
            "count=0; available after next strategy restart",
        )
    else:
        add_check(
            checks,
            "PASS" if stale_preflight_count == 0 else "WARN",
            "entry_submit_price_age_within_threshold",
            (
                f"preflight_count={preflight_count} stale_preflight_count={stale_preflight_count} "
                f"max_market_age_ms={submit_telemetry.get('market_age_ms_max', 0):.1f} "
                f"threshold_ms={submit_telemetry.get('warn_threshold_ms')}"
            ),
        )

    if db_error:
        add_check(checks, "FAIL", "db_open_like_orders_verified", f"db_error={db_error}")
    else:
        add_check(
            checks,
            "PASS" if not open_order_rows else "FAIL",
            "db_open_like_orders_zero",
            f"rows={sum(int(row[3] or 0) for row in open_order_rows) if open_order_rows else 0}",
        )

    if latest_shutdown:
        remaining_positions = latest_shutdown.get("remaining_positions") or []
        pending_pair_ids = latest_shutdown.get("pending_pair_ids") or []
        failed_cancel_before = ((latest_shutdown.get("cancel_before") or {}).get("failed") or [])
        failed_cancel_after = ((latest_shutdown.get("cancel_after") or {}).get("failed") or [])
        flatten_failed = ((latest_shutdown.get("flatten") or {}).get("failed") or [])
        add_check(
            checks,
            "PASS" if not remaining_positions else "FAIL",
            "shutdown_remaining_positions_empty",
            f"remaining_positions={remaining_positions}",
        )
        add_check(
            checks,
            "PASS" if not pending_pair_ids else "FAIL",
            "shutdown_pending_pair_ids_empty",
            f"pending_pair_ids={pending_pair_ids}",
        )
        add_check(
            checks,
            "PASS" if not failed_cancel_before and not failed_cancel_after else "FAIL",
            "shutdown_cancel_failures_zero",
            f"before={failed_cancel_before} after={failed_cancel_after}",
        )
        add_check(
            checks,
            "PASS" if not flatten_failed else "FAIL",
            "shutdown_flatten_failures_zero",
            f"failed={flatten_failed}",
        )
    elif "not running" in status_text or runtime_stopped:
        add_check(checks, "FAIL", "shutdown_cleanup_done_present", "runtime stopped but shutdown event not found")
    else:
        add_check(checks, "WARN", "shutdown_cleanup_done_present", "runtime may still be running")

    danger_count = sum(1 for line in critical_lines if "[strategy][danger]" in line)
    add_check(
        checks,
        "PASS" if danger_count == 0 else "WARN",
        "danger_log_lines_zero",
        f"count={danger_count}",
    )

    started_at_ms = int(meta.get("started_at_ms") or 0)
    requested_duration_ms = parse_duration_ms(meta.get("duration") or "")
    stopped_rows = [row for row in run_rows if row.get("event_type") == "runtime_stopped"]
    if started_at_ms and stopped_rows:
        stopped_at_ms = int(stopped_rows[-1].get("ts_ms") or started_at_ms)
        observed_duration_ms = max(stopped_at_ms - started_at_ms, 0)
        if requested_duration_ms and observed_duration_ms < requested_duration_ms * 0.9:
            add_check(
                checks,
                "WARN",
                "run_reached_requested_duration",
                f"observed={format_duration_ms(observed_duration_ms)} requested={meta.get('duration')}",
            )
        else:
            add_check(
                checks,
                "PASS",
                "run_reached_requested_duration",
                f"observed={format_duration_ms(observed_duration_ms)} requested={meta.get('duration')}",
            )

    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    verdict = "FAIL" if fail_count else ("WARN" if warn_count else "PASS")
    return {
        "verdict": verdict,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
    }


def collect_open_order_rows(root: Path, env_file: Path, started_at_ms: int) -> tuple[list[tuple], str]:
    try:
        from dotenv import load_dotenv

        sys.path.insert(0, str(root))
        load_dotenv(env_file, override=True)
        from src.ops.persistence.db import get_db_connection
        from src.ops.persistence.account_context import resolve_account_label, resolve_logic_name

        account_label = resolve_account_label()
        logic = resolve_logic_name()
        conn = get_db_connection()
        cur = conn.cursor()
        placeholders = ", ".join("?" for _ in OPEN_ORDER_STATUSES)
        cur.execute(
            f"""
            SELECT exchange_name, symbol, status, COUNT(*)
            FROM order_history
            WHERE account_label = ?
              AND logic = ?
              AND order_start >= ?
              AND UPPER(status) IN ({placeholders})
            GROUP BY exchange_name, symbol, status
            ORDER BY exchange_name, symbol, status
            """,
            (account_label, logic, started_at_ms, *OPEN_ORDER_STATUSES),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows, ""
    except Exception as exc:
        return [], str(exc)


def build_summary_payload(root: Path, meta_path: Path, wait: bool) -> dict:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    started_at_ms = int(meta.get("started_at_ms") or 0)
    forever_run = is_forever_duration(meta.get("duration"))
    duration_ms = parse_duration_ms(meta.get("duration")) or (0 if forever_run else 3 * 3_600_000)
    end_ms = started_at_ms + duration_ms
    env_file = root / str(meta.get("env_file") or "test_server/logs/strategy_live.3h.env")

    if wait and forever_run:
        print("duration is forever; --wait is ignored until the run is stopped", flush=True)
        status_cmd = f"STRATEGY_ENV_FILE={shlex.quote(str(env_file))} scripts/runtime/run_strategy_live.sh status"
        status = run_cmd(root, status_cmd, timeout=20)
    elif wait:
        sleep_sec = max((end_ms - int(time.time() * 1000)) / 1000.0 + 30.0, 0.0)
        if sleep_sec > 0:
            print(f"sleeping {sleep_sec:.1f}s until after strategy end", flush=True)
            time.sleep(sleep_sec)

        status_cmd = f"STRATEGY_ENV_FILE={shlex.quote(str(env_file))} scripts/runtime/run_strategy_live.sh status"
        status = ""
        for _ in range(80):
            status = run_cmd(root, status_cmd, timeout=20)
            if "not running" in status:
                break
            time.sleep(15)
    else:
        status_cmd = f"STRATEGY_ENV_FILE={shlex.quote(str(env_file))} scripts/runtime/run_strategy_live.sh status"
        status = run_cmd(root, status_cmd, timeout=20)

    summary_output = run_cmd(
        root,
        f"STRATEGY_ENV_FILE={shlex.quote(str(env_file))} scripts/runtime/run_strategy_live.sh summary",
        timeout=60,
    )

    stdout_path = root / str(meta.get("stdout_log") or "test_server/logs/strategy_live.out")
    jsonl_path = root / str(meta.get("jsonl_log") or "test_server/logs/strategy_live.jsonl")
    stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.exists() else ""
    marker = f"strategy live runtime start duration={meta.get('duration')}"
    marker_index = stdout_text.rfind(marker)
    if marker_index < 0:
        marker_index = stdout_text.rfind("strategy live runtime start duration=")
    run_stdout = stdout_text[marker_index:] if marker_index >= 0 else stdout_text
    run_lines = [line for line in run_stdout.splitlines() if line.strip()]
    critical_patterns = (
        "[strategy][danger]",
        "[strategy][close][leg_failed]",
        "[strategy][close][neutralized]",
        "pair_neutralized_after_close_failure",
        "pair_close_leg_failed",
        "pair_close_pending",
        "overfill",
        "micro_partial",
        "shutdown]",
        "marked_absent_open_orders",
    )
    critical_lines = [line for line in run_lines if any(pattern in line for pattern in critical_patterns)]

    run_rows = load_run_rows(jsonl_path, started_at_ms)
    event_counts = Counter(str(row.get("event_type") or "unknown") for row in run_rows)
    submit_telemetry = summarize_entry_submit_telemetry(run_rows)
    latest_shutdown = next((row for row in reversed(run_rows) if row.get("event_type") == "shutdown_cleanup_done"), None)
    open_order_rows, db_error = collect_open_order_rows(root, env_file, started_at_ms)
    verdict = evaluate_soak(
        meta=meta,
        status_text=status,
        event_counts=event_counts,
        run_rows=run_rows,
        submit_telemetry=submit_telemetry,
        latest_shutdown=latest_shutdown,
        open_order_rows=open_order_rows,
        db_error=db_error,
        critical_lines=critical_lines,
    )

    stopped_rows = [row for row in run_rows if row.get("event_type") == "runtime_stopped"]
    observed_duration_ms = 0
    if started_at_ms and stopped_rows:
        observed_duration_ms = max(int(stopped_rows[-1].get("ts_ms") or started_at_ms) - started_at_ms, 0)

    return {
        "meta": meta,
        "meta_path": str(meta_path),
        "generated_at_ms": int(time.time() * 1000),
        "status_text": status,
        "summary_output": summary_output,
        "stdout_log": str(stdout_path),
        "jsonl_log": str(jsonl_path),
        "run_rows_count": len(run_rows),
        "event_counts": dict(event_counts),
        "submit_telemetry": submit_telemetry,
        "latest_shutdown": latest_shutdown,
        "open_order_rows": [
            {
                "exchange_name": row[0],
                "symbol": row[1],
                "status": row[2],
                "row_count": int(row[3] or 0),
            }
            for row in open_order_rows
        ],
        "db_error": db_error,
        "critical_lines": critical_lines,
        "observed_duration_ms": observed_duration_ms,
        "verdict": verdict,
    }


def render_summary_markdown(payload: dict) -> str:
    meta = payload["meta"]
    verdict = payload["verdict"]
    event_counts = Counter(payload.get("event_counts") or {})
    submit_telemetry = payload.get("submit_telemetry") or {}
    latest_shutdown = payload.get("latest_shutdown")
    open_order_rows = payload.get("open_order_rows") or []
    db_error = payload.get("db_error") or ""
    critical_lines = payload.get("critical_lines") or []

    lines: list[str] = []
    lines.append(f"# Strategy {meta.get('duration') or ''} Soak Summary")
    lines.append("")
    lines.append("## Verdict")
    lines.append(f"- result: **{verdict['verdict']}**")
    lines.append(f"- failed_checks: {verdict['fail_count']}")
    lines.append(f"- warning_checks: {verdict['warn_count']}")
    lines.append("")
    lines.append("## Checks")
    for check in verdict["checks"]:
        lines.append(f"- {check['status']} {check['name']}: {check['detail']}")
    lines.append("")
    lines.append("## Run Metadata")
    lines.append(f"- generated_at_utc: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime((payload.get('generated_at_ms') or int(time.time() * 1000)) / 1000))}")
    lines.append(f"- run_started_at_utc: {meta.get('started_at_iso')}")
    lines.append(f"- run_duration: {meta.get('duration')}")
    if payload.get("observed_duration_ms"):
        lines.append(f"- observed_duration: {format_duration_ms(int(payload.get('observed_duration_ms') or 0))}")
    lines.append(f"- env_file: {meta.get('env_file')}")
    lines.append(f"- status_after_wait: `{payload.get('status_text')}`")
    lines.append(f"- stdout_log: {payload.get('stdout_log')}")
    lines.append(f"- jsonl_log: {payload.get('jsonl_log')}")
    lines.append("")
    lines.append("## Event Counts")
    if event_counts:
        for key, value in event_counts.most_common():
            lines.append(f"- {key}: {value}")
    else:
        lines.append("- no jsonl events found after run start")
    lines.append("")
    lines.append("## Entry Submit Price Age")
    if int(submit_telemetry.get("preflight_count") or 0) <= 0:
        lines.append("- no entry submit telemetry yet; available after next strategy restart")
    else:
        lines.append(f"- preflight_count: {submit_telemetry.get('preflight_count')}")
        lines.append(f"- stale_preflight_count: {submit_telemetry.get('stale_preflight_count')}")
        lines.append(f"- market_age_ms_avg: {submit_telemetry.get('market_age_ms_avg', 0):.1f}")
        lines.append(f"- market_age_ms_p95: {submit_telemetry.get('market_age_ms_p95', 0):.1f}")
        lines.append(f"- market_age_ms_max: {submit_telemetry.get('market_age_ms_max', 0):.1f}")
        lines.append(f"- submit_latency_ms_avg: {submit_telemetry.get('submit_latency_ms_avg', 0):.1f}")
        lines.append(f"- submit_latency_ms_p95: {submit_telemetry.get('submit_latency_ms_p95', 0):.1f}")
        lines.append(f"- submit_latency_ms_max: {submit_telemetry.get('submit_latency_ms_max', 0):.1f}")
    lines.append("")
    lines.append("## Shutdown")
    if latest_shutdown:
        lines.append(f"- remaining_positions: {latest_shutdown.get('remaining_positions')}")
        lines.append(f"- pending_pair_ids: {latest_shutdown.get('pending_pair_ids')}")
        lines.append(f"- closed_pair_ids: {latest_shutdown.get('closed_pair_ids')}")
    else:
        lines.append("- shutdown_cleanup_done event not found yet")
    lines.append("")
    lines.append("## DB Open-Like Order Rows")
    if db_error:
        lines.append(f"- db_error: {db_error}")
    elif open_order_rows:
        for row in open_order_rows:
            lines.append(f"- {row.get('exchange_name')} {row.get('symbol')} {row.get('status')}: {row.get('row_count')}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Critical Log Lines")
    if critical_lines:
        for line in critical_lines[-120:]:
            lines.append(f"- {line}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Script Summary Output")
    lines.append("```text")
    lines.append(payload.get("summary_output") or "")
    lines.append("```")
    return "\n".join(lines) + "\n"


def build_summary(root: Path, meta_path: Path, output_path: Path, wait: bool) -> dict:
    payload = build_summary_payload(root, meta_path, wait=wait)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_summary_markdown(payload), encoding="utf-8")
    print(f"wrote {output_path}", flush=True)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--meta", default="test_server/logs/strategy_live.run.json")
    parser.add_argument("--output", default="test_server/logs/strategy_live_3h_summary.md")
    parser.add_argument("--wait", action="store_true")
    args = parser.parse_args()

    root = Path.cwd()
    build_summary(root, root / args.meta, root / args.output, wait=args.wait)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
