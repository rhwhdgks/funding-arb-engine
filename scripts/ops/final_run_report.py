#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ops.order_trade_reconcile_report import (  # noqa: E402
    build_report as build_order_trade_report,
)
from scripts.ops.order_trade_reconcile_report import (  # noqa: E402
    render_markdown as render_order_trade_markdown,
)
from scripts.ops.order_trade_reconcile_report import write_json as write_report_json  # noqa: E402
from scripts.ops.strategy_afterrun_summary import (  # noqa: E402
    build_summary_payload,
    format_duration_ms,
    render_summary_markdown,
)


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int | None) -> str:
    if not ms:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def resolve_path(value: str | Path | None) -> Path | None:
    if value in (None, ""):
        return None
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def parse_csv(raw: Any, fallback: list[str] | None = None, *, lower: bool = False) -> list[str]:
    if not raw:
        return list(fallback or [])
    values = [item.strip() for item in str(raw).split(",") if item.strip()]
    if lower:
        return [item.lower() for item in values]
    return [item.upper() for item in values]


def safe_label(label: str | None) -> str:
    text = (label or "latest").strip()
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    return text or "latest"


def default_artifact_path(meta_path: Path, prefix: str, suffix: str, label: str | None) -> Path:
    return meta_path.parent / f"{prefix}_{safe_label(label)}{suffix}"


def check(status: str, name: str, detail: str) -> dict[str, str]:
    return {"status": status, "name": name, "detail": detail}


def verdict_to_check(name: str, report: dict[str, Any]) -> dict[str, str]:
    verdict = str(report.get("verdict") or "FAIL").upper()
    status = verdict if verdict in {"PASS", "WARN", "FAIL"} else "FAIL"
    return check(
        status,
        name,
        f"result={verdict} failed={report.get('fail_count', 0)} warnings={report.get('warn_count', 0)}",
    )


def count_open_order_map(open_orders: dict[str, list[dict[str, Any]]] | None) -> int:
    if not open_orders:
        return 0
    return sum(len(rows or []) for rows in open_orders.values())


def nonzero_balance_positions(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = ((report.get("balance_report") or {}).get("positions") or [])
    result: list[dict[str, Any]] = []
    for row in rows:
        try:
            qty = float(row.get("qty") or 0.0)
        except Exception:
            qty = 0.0
        if abs(qty) > 0.0:
            result.append(row)
    return result


async def collect_live_flat_report(
    *,
    env_file: Path,
    exchanges: list[str],
    symbols: list[str],
    wait_ws_sec: float,
    trade_limit: int,
    runtime_status_text: str,
) -> dict[str, Any]:
    from src.ops.persistence.live_report import collect_reports

    reports = await collect_reports(exchanges, symbols, wait_ws_sec, trade_limit)
    checks: list[dict[str, str]] = []

    runtime_stopped = "not running" in (runtime_status_text or "")
    checks.append(
        check(
            "PASS" if runtime_stopped else "FAIL",
            "runtime_not_running_before_live_check",
            runtime_status_text or "-",
        )
    )

    exchange_summaries: list[dict[str, Any]] = []
    for report in reports:
        exchange_name = str(report.get("exchange") or "").lower()
        error = str(report.get("error") or "")
        positions = report.get("current_positions") or {}
        balance_positions = nonzero_balance_positions(report)
        open_orders = report.get("open_orders") or {}
        open_order_count = count_open_order_map(open_orders)
        position_count = max(len(positions), len(balance_positions))

        if error:
            checks.append(check("FAIL", f"{exchange_name}_live_report_ok", error))
        else:
            checks.append(check("PASS", f"{exchange_name}_live_report_ok", "ok"))

        checks.append(
            check(
                "PASS" if position_count == 0 else "FAIL",
                f"{exchange_name}_live_positions_flat",
                f"positions={position_count}",
            )
        )
        checks.append(
            check(
                "PASS" if open_order_count == 0 else "FAIL",
                f"{exchange_name}_live_open_orders_zero",
                f"open_orders={open_order_count}",
            )
        )
        exchange_summaries.append(
            {
                "exchange": exchange_name,
                "error": error,
                "position_count": position_count,
                "open_order_count": open_order_count,
                "positions": positions,
                "balance_positions": balance_positions,
                "open_orders": open_orders,
            }
        )

    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    return {
        "generated_at_ms": now_ms(),
        "env_file": str(env_file),
        "exchanges": exchanges,
        "symbols": symbols,
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "exchange_summaries": exchange_summaries,
    }


def skipped_live_report(reason: str) -> dict[str, Any]:
    return {
        "generated_at_ms": now_ms(),
        "verdict": "WARN",
        "fail_count": 0,
        "warn_count": 1,
        "checks": [check("WARN", "live_exchange_flat_check_skipped", reason)],
        "exchange_summaries": [],
    }


def symbol_price_error_report(exc: Exception) -> dict[str, Any]:
    return {
        "generated_at_ms": now_ms(),
        "verdict": "WARN",
        "fail_count": 0,
        "warn_count": 1,
        "checks": [check("WARN", "symbol_price_query_ok", str(exc))],
        "exchange_summaries": [],
    }


def collect_symbol_price_report(
    *,
    exchanges: list[str],
    symbols: list[str],
    interval_sec: float,
) -> dict[str, Any]:
    from src.ops.persistence.db import get_db_connection

    checks: list[dict[str, str]] = []
    exchange_summaries: list[dict[str, Any]] = []
    current_ms = now_ms()
    freshness_threshold_ms = int(max(interval_sec * 3.0, 180.0) * 1000)
    symbols = [symbol.upper() for symbol in symbols]
    expected_symbol_count = len(symbols)
    placeholders = ", ".join("?" for _ in symbols)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        for exchange_name in exchanges:
            cur.execute(
                f"""
                SELECT MAX(bucket_start_ms)
                FROM symbol_price
                WHERE exchange_name = ?
                  AND symbol IN ({placeholders})
                """,
                (exchange_name, *symbols),
            )
            latest_bucket_ms = int((cur.fetchone() or [0])[0] or 0)
            latest_age_ms = current_ms - latest_bucket_ms if latest_bucket_ms else None
            latest_symbol_count = 0
            if latest_bucket_ms:
                cur.execute(
                    f"""
                    SELECT COUNT(DISTINCT symbol)
                    FROM symbol_price
                    WHERE exchange_name = ?
                      AND bucket_start_ms = ?
                      AND symbol IN ({placeholders})
                    """,
                    (exchange_name, latest_bucket_ms, *symbols),
                )
                latest_symbol_count = int((cur.fetchone() or [0])[0] or 0)

            checks.append(
                check(
                    "PASS" if latest_bucket_ms > 0 else "WARN",
                    f"{exchange_name}_symbol_price_rows_present",
                    f"latest_bucket_ms={latest_bucket_ms}",
                )
            )
            checks.append(
                check(
                    "PASS" if latest_age_ms is not None and latest_age_ms <= freshness_threshold_ms else "WARN",
                    f"{exchange_name}_symbol_price_fresh",
                    f"latest_age_ms={latest_age_ms} threshold_ms={freshness_threshold_ms}",
                )
            )
            checks.append(
                check(
                    "PASS" if latest_symbol_count == expected_symbol_count else "WARN",
                    f"{exchange_name}_symbol_price_symbol_count",
                    f"latest_symbols={latest_symbol_count} expected={expected_symbol_count}",
                )
            )
            exchange_summaries.append(
                {
                    "exchange": exchange_name,
                    "latest_bucket_ms": latest_bucket_ms,
                    "latest_bucket_utc": iso_utc(latest_bucket_ms),
                    "latest_age_ms": latest_age_ms,
                    "latest_symbol_count": latest_symbol_count,
                    "expected_symbol_count": expected_symbol_count,
                    "freshness_threshold_ms": freshness_threshold_ms,
                }
            )
        cur.close()
    finally:
        conn.close()

    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    return {
        "generated_at_ms": current_ms,
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "exchange_summaries": exchange_summaries,
    }


def db_error_report(exc: Exception, *, since_ms: int, run_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at_ms": now_ms(),
        "since_ms": since_ms,
        "since_utc": iso_utc(since_ms),
        "run_meta": run_meta,
        "verdict": "FAIL",
        "fail_count": 1,
        "warn_count": 0,
        "checks": [check("FAIL", "order_trade_reconcile_query_ok", str(exc))],
        "summary": {},
    }


def render_db_error_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Order/Trade DB Reconcile Report",
        "",
        "## Verdict",
        f"- result: **{report['verdict']}**",
        f"- failed_checks: {report['fail_count']}",
        f"- warning_checks: {report['warn_count']}",
        f"- generated_at_utc: {iso_utc(report['generated_at_ms'])}",
        f"- since_utc: {report.get('since_utc')}",
        "",
        "## Checks",
        *render_check_lines(report.get("checks") or []),
        "",
    ]
    return "\n".join(lines)


def build_final_verdict(
    component_checks: list[dict[str, str]],
    component_reports: list[dict[str, Any]],
    *,
    allow_warnings: bool,
) -> dict[str, Any]:
    fail_count = sum(int(report.get("fail_count") or 0) for report in component_reports)
    warn_count = sum(int(report.get("warn_count") or 0) for report in component_reports)
    fail_count = max(fail_count, sum(1 for item in component_checks if item["status"] == "FAIL"))
    warn_count = max(warn_count, sum(1 for item in component_checks if item["status"] == "WARN"))
    blocking_count = fail_count + (0 if allow_warnings else warn_count)
    return {
        "verdict": "FAIL" if blocking_count else "PASS",
        "fail_count": fail_count,
        "warn_count": warn_count,
        "blocking_count": blocking_count,
        "allow_warnings": allow_warnings,
    }


def render_check_lines(checks: list[dict[str, Any]]) -> list[str]:
    if not checks:
        return ["- none"]
    return [f"- {item.get('status')} {item.get('name')}: {item.get('detail')}" for item in checks]


def render_final_markdown(payload: dict[str, Any]) -> str:
    final = payload["final"]
    run_meta = payload.get("run_meta") or {}
    afterrun = payload.get("afterrun") or {}
    db_report = payload.get("order_trade_reconcile") or {}
    live_report = payload.get("live_flat") or {}
    symbol_price_report = payload.get("symbol_price") or {}
    artifacts = payload.get("artifacts") or {}
    event_counts = afterrun.get("event_counts") or {}
    submit_telemetry = afterrun.get("submit_telemetry") or {}

    lines: list[str] = []
    lines.append("# Final Run PASS/FAIL Report")
    lines.append("")
    lines.append("## Verdict")
    lines.append(f"- result: **{final['verdict']}**")
    lines.append(f"- failed_checks: {final['fail_count']}")
    lines.append(f"- warning_checks: {final['warn_count']}")
    lines.append(f"- blocking_checks: {final['blocking_count']}")
    lines.append(f"- allow_warnings: {final['allow_warnings']}")
    lines.append(f"- generated_at_utc: {iso_utc(payload.get('generated_at_ms'))}")
    lines.append("")
    lines.append("## Component Checks")
    lines.extend(render_check_lines(payload.get("component_checks") or []))
    lines.append("")
    lines.append("## Run Info")
    lines.append(f"- run_started_at_utc: {run_meta.get('started_at_iso')}")
    lines.append(f"- requested_duration: {run_meta.get('duration')}")
    lines.append(f"- observed_duration: {format_duration_ms(int(afterrun.get('observed_duration_ms') or 0))}")
    lines.append(f"- env_file: {run_meta.get('env_file')}")
    lines.append(f"- stdout_log: {run_meta.get('stdout_log')}")
    lines.append(f"- jsonl_log: {run_meta.get('jsonl_log')}")
    lines.append("")
    lines.append("## Event Verdict")
    lines.extend(render_check_lines((afterrun.get("verdict") or {}).get("checks") or []))
    lines.append("")
    lines.append("## Info Counters")
    for key in (
        "pair_opened",
        "pair_closed",
        "pair_entry_micro_partial_detected",
        "pair_entry_micro_residual_ignored",
        "pair_direction_micro_residual_detected",
        "entry_order_disappeared_settle_started",
        "entry_order_disappeared_resolved",
        "entry_limit_order_submit_preflight",
        "entry_limit_order_submit_result",
        "pair_closed_after_recovery",
        "pair_neutralized_after_close_failure",
    ):
        lines.append(f"- {key}: {event_counts.get(key, 0)}")
    lines.append("")
    lines.append("## Entry Submit Price Age")
    if int(submit_telemetry.get("preflight_count") or 0) <= 0:
        lines.append("- no entry submit telemetry yet; available after next strategy restart")
    else:
        lines.append(f"- preflight_count: {submit_telemetry.get('preflight_count')}")
        lines.append(f"- stale_preflight_count: {submit_telemetry.get('stale_preflight_count')}")
        lines.append(f"- market_age_ms_avg: {float(submit_telemetry.get('market_age_ms_avg') or 0):.1f}")
        lines.append(f"- market_age_ms_p95: {float(submit_telemetry.get('market_age_ms_p95') or 0):.1f}")
        lines.append(f"- market_age_ms_max: {float(submit_telemetry.get('market_age_ms_max') or 0):.1f}")
        lines.append(f"- submit_latency_ms_avg: {float(submit_telemetry.get('submit_latency_ms_avg') or 0):.1f}")
        lines.append(f"- submit_latency_ms_p95: {float(submit_telemetry.get('submit_latency_ms_p95') or 0):.1f}")
        lines.append(f"- submit_latency_ms_max: {float(submit_telemetry.get('submit_latency_ms_max') or 0):.1f}")
    lines.append("")
    lines.append("## DB Verdict")
    lines.extend(render_check_lines(db_report.get("checks") or []))
    lines.append("")
    lines.append("## Live Exchange Flat")
    lines.extend(render_check_lines(live_report.get("checks") or []))
    for row in live_report.get("exchange_summaries") or []:
        lines.append(
            f"- {row.get('exchange')}: positions={row.get('position_count')} "
            f"open_orders={row.get('open_order_count')} error={row.get('error') or '-'}"
        )
    lines.append("")
    lines.append("## Symbol Price Collector")
    lines.extend(render_check_lines(symbol_price_report.get("checks") or []))
    for row in symbol_price_report.get("exchange_summaries") or []:
        lines.append(
            f"- {row.get('exchange')}: latest_bucket={row.get('latest_bucket_utc') or '-'} "
            f"age_ms={row.get('latest_age_ms')} "
            f"symbols={row.get('latest_symbol_count')}/{row.get('expected_symbol_count')}"
        )
    lines.append("")
    lines.append("## Artifacts")
    for key, value in artifacts.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate final PASS/FAIL status for a completed strategy run.")
    parser.add_argument("--env-file", default=os.getenv("STRATEGY_ENV_FILE") or "")
    parser.add_argument("--meta", default="test_server/logs/strategy_live.run.json")
    parser.add_argument("--output")
    parser.add_argument("--json-output")
    parser.add_argument("--afterrun-output")
    parser.add_argument("--reconcile-output")
    parser.add_argument("--reconcile-json-output")
    parser.add_argument("--label")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--allow-warnings", action="store_true")
    parser.add_argument("--skip-live-check", action="store_true")
    parser.add_argument("--wait-ws-sec", type=float, default=2.0)
    parser.add_argument("--trade-limit", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    meta_path = resolve_path(args.meta)
    if not meta_path or not meta_path.exists():
        print(f"final_verdict=FAIL reason=missing_meta meta={meta_path}", file=sys.stderr)
        return 1

    run_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    env_path = resolve_path(args.env_file or run_meta.get("env_file") or ".env")
    if env_path:
        load_dotenv(env_path, override=True)

    label = safe_label(args.label)
    final_output = resolve_path(args.output) or default_artifact_path(meta_path, "final_run_report", ".md", label)
    final_json_output = resolve_path(args.json_output) or default_artifact_path(meta_path, "final_run_report", ".json", label)
    afterrun_output = resolve_path(args.afterrun_output) or default_artifact_path(meta_path, "strategy_afterrun_summary", ".md", label)
    reconcile_output = resolve_path(args.reconcile_output) or default_artifact_path(meta_path, "order_trade_reconcile", ".md", label)
    reconcile_json_output = resolve_path(args.reconcile_json_output) or default_artifact_path(
        meta_path, "order_trade_reconcile", ".json", label
    )

    afterrun_payload = build_summary_payload(PROJECT_ROOT, meta_path, wait=args.wait)
    afterrun_output.parent.mkdir(parents=True, exist_ok=True)
    afterrun_output.write_text(render_summary_markdown(afterrun_payload), encoding="utf-8")

    since_ms = int(run_meta.get("started_at_ms") or 0)
    db_markdown = ""
    try:
        from src.ops.persistence.db import get_db_connection

        conn = get_db_connection()
        try:
            db_report = build_order_trade_report(
                conn,
                since_ms=since_ms,
                run_meta=run_meta,
            )
        finally:
            conn.close()
        db_markdown = render_order_trade_markdown(db_report)
    except Exception as exc:
        db_report = db_error_report(exc, since_ms=since_ms, run_meta=run_meta)
        db_markdown = render_db_error_markdown(db_report)
    reconcile_output.parent.mkdir(parents=True, exist_ok=True)
    reconcile_output.write_text(db_markdown, encoding="utf-8")
    write_report_json(reconcile_json_output, db_report)

    exchanges = parse_csv(run_meta.get("exchanges") or os.getenv("STRATEGY_EXCHANGES"), ["binance", "okx"], lower=True)
    symbols = parse_csv(run_meta.get("symbols") or os.getenv("STRATEGY_SYMBOLS"), ["ETH", "SOL", "XRP", "DOGE"])
    if args.skip_live_check:
        live_flat = skipped_live_report("--skip-live-check")
    else:
        live_flat = asyncio.run(
            collect_live_flat_report(
                env_file=env_path or PROJECT_ROOT / ".env",
                exchanges=exchanges,
                symbols=symbols,
                wait_ws_sec=args.wait_ws_sec,
                trade_limit=args.trade_limit,
                runtime_status_text=afterrun_payload.get("status_text") or "",
            )
        )
    try:
        symbol_price = collect_symbol_price_report(
            exchanges=exchanges,
            symbols=symbols,
            interval_sec=float(os.getenv("SYMBOL_PRICE_INTERVAL_SEC", "60") or 60),
        )
    except Exception as exc:
        symbol_price = symbol_price_error_report(exc)

    component_checks = [
        verdict_to_check("afterrun_summary_passed", afterrun_payload.get("verdict") or {}),
        verdict_to_check("order_trade_reconcile_passed", db_report),
        verdict_to_check("live_exchange_flat_passed", live_flat),
        verdict_to_check("symbol_price_collector_passed", symbol_price),
    ]
    final = build_final_verdict(
        component_checks,
        [afterrun_payload.get("verdict") or {}, db_report, live_flat, symbol_price],
        allow_warnings=args.allow_warnings,
    )
    payload = {
        "generated_at_ms": now_ms(),
        "run_meta": run_meta,
        "final": final,
        "component_checks": component_checks,
        "afterrun": afterrun_payload,
        "order_trade_reconcile": db_report,
        "live_flat": live_flat,
        "symbol_price": symbol_price,
        "artifacts": {
            "afterrun_summary_md": str(afterrun_output),
            "order_trade_reconcile_md": str(reconcile_output),
            "order_trade_reconcile_json": str(reconcile_json_output),
            "final_report_md": str(final_output),
            "final_report_json": str(final_json_output),
        },
    }

    final_output.parent.mkdir(parents=True, exist_ok=True)
    final_output.write_text(render_final_markdown(payload), encoding="utf-8")
    write_report_json(final_json_output, payload)
    print(
        f"final_verdict={final['verdict']} warnings={final['warn_count']} "
        f"failures={final['fail_count']} blocking={final['blocking_count']} output={final_output}",
        flush=True,
    )
    return 1 if final["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
