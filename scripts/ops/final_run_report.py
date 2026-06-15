#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections import Counter
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
    percentile,
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


def boolish(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def strategy_execution_profile(run_meta: dict[str, Any]) -> str:
    raw = str(run_meta.get("execution_profile") or "").strip().lower().replace("-", "_")
    if raw in {"auto_trade", "monitor", "dry_run"}:
        return raw
    has_profile_fields = any(
        key in run_meta
        for key in (
            "enable_live",
            "auto_enter_enabled",
            "maker_entry_engine_enabled",
        )
    )
    if not has_profile_fields:
        return "legacy_unknown"
    if not boolish(run_meta.get("enable_live")):
        return "dry_run"
    if boolish(run_meta.get("auto_enter_enabled")) or boolish(run_meta.get("maker_entry_engine_enabled")):
        return "auto_trade"
    return "monitor"


def is_no_entry_profile(run_meta: dict[str, Any]) -> bool:
    profile = strategy_execution_profile(run_meta)
    has_entry_flags = "auto_enter_enabled" in run_meta or "maker_entry_engine_enabled" in run_meta
    return profile in {"monitor", "dry_run"} or (
        has_entry_flags
        and not boolish(run_meta.get("auto_enter_enabled"))
        and not boolish(run_meta.get("maker_entry_engine_enabled"))
    )


def safe_label(label: str | None) -> str:
    text = (label or "latest").strip()
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    return text or "latest"


def default_artifact_path(meta_path: Path, prefix: str, suffix: str, label: str | None) -> Path:
    return meta_path.parent / f"{prefix}_{safe_label(label)}{suffix}"


def check(status: str, name: str, detail: str) -> dict[str, str]:
    return {"status": status, "name": name, "detail": detail}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def load_run_rows_from_meta(run_meta: dict[str, Any]) -> list[dict[str, Any]]:
    started_at_ms = int(run_meta.get("started_at_ms") or 0)
    jsonl_path = resolve_path(run_meta.get("jsonl_log"))
    if not jsonl_path or not jsonl_path.exists():
        return []

    rows: list[dict[str, Any]] = []
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


def build_strategy_performance_report(run_meta: dict[str, Any]) -> dict[str, Any]:
    run_rows = load_run_rows_from_meta(run_meta)
    event_counts = Counter(str(row.get("event_type") or "unknown") for row in run_rows)
    opened_by_pair_id: dict[str, dict[str, Any]] = {}
    close_trigger_by_pair_id: dict[str, dict[str, Any]] = {}
    pair_rows: list[dict[str, Any]] = []
    block_reason_counts: Counter[str] = Counter()
    top_symbol_counts: Counter[str] = Counter()
    top_enterable_symbol_counts: Counter[str] = Counter()
    enterable_scan_count = 0
    max_enterable_candidates = 0

    for row in run_rows:
        event_type = str(row.get("event_type") or "")
        if event_type == "candidate_scan_summary":
            enterable_count = int(row.get("enterable_candidates") or 0)
            enterable_scan_count += 1 if enterable_count > 0 else 0
            max_enterable_candidates = max(max_enterable_candidates, enterable_count)
            block_reason_counts.update(row.get("reason_counts") or {})

            top_candidate = row.get("top_candidate") or {}
            top_symbol = top_candidate.get("symbol")
            if top_symbol:
                top_symbol_counts[str(top_symbol).upper()] += 1

            top_enterable = row.get("top_enterable_candidate") or {}
            top_enterable_symbol = top_enterable.get("symbol")
            if top_enterable_symbol:
                top_enterable_symbol_counts[str(top_enterable_symbol).upper()] += 1
            continue

        pair = row.get("pair") or {}
        pair_id = pair.get("pair_id")
        if not pair_id:
            continue

        if event_type == "pair_opened":
            opened_by_pair_id[str(pair_id)] = row
        elif event_type == "pair_auto_close_trigger":
            close_trigger_by_pair_id[str(pair_id)] = row
        elif event_type == "pair_closed":
            opened_row = opened_by_pair_id.get(str(pair_id)) or {}
            opened_pair = opened_row.get("pair") or {}
            candidate = opened_row.get("candidate") or {}
            trigger = close_trigger_by_pair_id.get(str(pair_id)) or {}
            trigger_pair = trigger.get("pair") or {}
            entry_time_ms = int(pair.get("entry_time_ms") or opened_pair.get("entry_time_ms") or 0)
            close_time_ms = int(row.get("ts_ms") or 0)
            hold_ms = max(close_time_ms - entry_time_ms, 0) if entry_time_ms and close_time_ms else 0
            pair_rows.append(
                {
                    "pair_id": pair_id,
                    "symbol": str(pair.get("symbol") or opened_pair.get("symbol") or "").upper(),
                    "long_exchange": pair.get("long_exchange") or opened_pair.get("long_exchange"),
                    "short_exchange": pair.get("short_exchange") or opened_pair.get("short_exchange"),
                    "requested_qty": safe_float(pair.get("requested_qty") or opened_pair.get("requested_qty")),
                    "opened_qty": safe_float(opened_pair.get("qty") or pair.get("requested_qty")),
                    "entry_time_ms": entry_time_ms,
                    "entry_time_utc": iso_utc(entry_time_ms),
                    "close_time_ms": close_time_ms,
                    "close_time_utc": iso_utc(close_time_ms),
                    "hold_ms": hold_ms,
                    "hold_duration": format_duration_ms(hold_ms),
                    "entry_expected_net_edge_usd": safe_float(candidate.get("expected_net_edge_usd")),
                    "entry_expected_net_edge_bps": safe_float(candidate.get("expected_net_edge_bps")),
                    "entry_open_edge_usd": safe_float(candidate.get("entry_open_edge_usd")),
                    "entry_open_edge_bps": safe_float(candidate.get("entry_open_edge_bps")),
                    "close_reason": pair.get("last_close_reason") or trigger.get("reason") or trigger_pair.get("last_close_reason") or "",
                    "close_pnl_usd": safe_float(pair.get("last_close_pnl_usd")),
                    "close_edge_usd": safe_float(pair.get("last_close_edge_usd")),
                    "hold_pnl_usd": safe_float(pair.get("last_hold_pnl_usd")),
                    "auto_close_reason": trigger.get("reason") or "",
                }
            )

    closed_count = len(pair_rows)
    total_close_pnl = sum(row["close_pnl_usd"] for row in pair_rows)
    total_entry_expected_edge = sum(row["entry_expected_net_edge_usd"] for row in pair_rows)
    total_hold_ms = sum(int(row["hold_ms"] or 0) for row in pair_rows)
    avg_hold_ms = int(total_hold_ms / closed_count) if closed_count else 0
    close_reason_counts = Counter(str(row.get("close_reason") or "unknown") for row in pair_rows)

    return {
        "execution_profile": strategy_execution_profile(run_meta),
        "new_entries_disabled_expected": is_no_entry_profile(run_meta),
        "run_rows": len(run_rows),
        "pair_opened": int(event_counts.get("pair_opened", 0)),
        "pair_closed": closed_count,
        "pair_close_reasons": dict(close_reason_counts.most_common()),
        "total_close_pnl_usd": total_close_pnl,
        "total_entry_expected_edge_usd": total_entry_expected_edge,
        "realized_minus_expected_usd": total_close_pnl - total_entry_expected_edge,
        "avg_close_pnl_usd": total_close_pnl / closed_count if closed_count else 0.0,
        "avg_hold_ms": avg_hold_ms,
        "avg_hold_duration": format_duration_ms(avg_hold_ms),
        "entry_submit_attempts": int(event_counts.get("entry_limit_order_submit_preflight", 0)),
        "candidate_scan_count": int(event_counts.get("candidate_scan_summary", 0)),
        "enterable_scan_count": enterable_scan_count,
        "max_enterable_candidates": max_enterable_candidates,
        "top_block_reasons": dict(block_reason_counts.most_common(10)),
        "top_candidate_symbols": dict(top_symbol_counts.most_common(10)),
        "top_enterable_symbols": dict(top_enterable_symbol_counts.most_common(10)),
        "entry_quality": build_entry_quality_report(run_rows),
        "pairs": pair_rows,
    }


def _sum_counter(target: Counter[str], raw: dict[str, Any]) -> None:
    for key, value in (raw or {}).items():
        try:
            target[str(key)] += int(value or 0)
        except Exception:
            continue


def build_api_pressure_report(run_meta: dict[str, Any]) -> dict[str, Any]:
    run_rows = load_run_rows_from_meta(run_meta)
    summaries = [row for row in run_rows if row.get("event_type") == "api_pressure_summary"]
    checks: list[dict[str, str]] = []

    if not summaries:
        checks.append(check("WARN", "api_pressure_data_present", "no_api_pressure_summary_events"))
        return {
            "generated_at_ms": now_ms(),
            "verdict": "WARN",
            "fail_count": 0,
            "warn_count": 1,
            "checks": checks,
            "console_monitor_readiness": "WARN",
            "exchanges": {},
        }

    warn_usage_ratio = safe_float(os.getenv("API_PRESSURE_BINANCE_WARN_USAGE_RATIO"), 0.75)
    fail_usage_ratio = safe_float(os.getenv("API_PRESSURE_BINANCE_FAIL_USAGE_RATIO"), 0.95)
    exchange_rows: dict[str, dict[str, Any]] = {}

    for row in summaries:
        for exchange_name, raw in (row.get("exchanges") or {}).items():
            exchange_name = str(exchange_name).lower()
            bucket = exchange_rows.setdefault(
                exchange_name,
                {
                    "remote_calls": 0,
                    "local_skips": 0,
                    "by_category": Counter(),
                    "by_path": Counter(),
                    "status_counts": Counter(),
                    "error_code_counts": Counter(),
                    "transient_counts": Counter(),
                    "latency_ms_max": 0.0,
                    "retry_attempts": 0,
                    "retry_exhausted": 0,
                    "private_read_pause_started": 0,
                    "private_read_pause_skips": 0,
                    "rate_limit_violations": 0,
                    "ip_ban_events": 0,
                    "max_request_weight_usage_1m": 0.0,
                    "max_order_count_usage_10s": 0.0,
                    "max_order_count_usage_1m": 0.0,
                },
            )
            bucket["remote_calls"] += int(raw.get("remote_calls") or 0)
            bucket["local_skips"] += int(raw.get("local_skips") or 0)
            _sum_counter(bucket["by_category"], raw.get("by_category") or {})
            _sum_counter(bucket["by_path"], raw.get("by_path") or {})
            _sum_counter(bucket["status_counts"], raw.get("status_counts") or {})
            _sum_counter(bucket["error_code_counts"], raw.get("error_code_counts") or {})
            _sum_counter(bucket["transient_counts"], raw.get("transient_counts") or {})
            bucket["latency_ms_max"] = max(safe_float(bucket.get("latency_ms_max")), safe_float(raw.get("latency_ms_max")))
            bucket["retry_attempts"] += int(raw.get("retry_attempts") or 0)
            bucket["retry_exhausted"] += int(raw.get("retry_exhausted") or 0)
            bucket["private_read_pause_started"] += int(raw.get("private_read_pause_started") or 0)
            bucket["private_read_pause_skips"] += int(raw.get("private_read_pause_skips") or 0)
            bucket["rate_limit_violations"] += int(raw.get("rate_limit_violations") or 0)
            bucket["ip_ban_events"] += int(raw.get("ip_ban_events") or 0)
            bucket["max_request_weight_usage_1m"] = max(
                safe_float(bucket.get("max_request_weight_usage_1m")),
                safe_float(raw.get("max_request_weight_usage_1m")),
            )
            bucket["max_order_count_usage_10s"] = max(
                safe_float(bucket.get("max_order_count_usage_10s")),
                safe_float(raw.get("max_order_count_usage_10s")),
            )
            bucket["max_order_count_usage_1m"] = max(
                safe_float(bucket.get("max_order_count_usage_1m")),
                safe_float(raw.get("max_order_count_usage_1m")),
            )

    top_limit = max(1, int(safe_float(os.getenv("API_PRESSURE_TOP_ENDPOINT_LIMIT"), 10)))
    serializable_exchanges: dict[str, dict[str, Any]] = {}
    for exchange_name, bucket in sorted(exchange_rows.items()):
        max_binance_usage = max(
            safe_float(bucket.get("max_request_weight_usage_1m")),
            safe_float(bucket.get("max_order_count_usage_10s")),
            safe_float(bucket.get("max_order_count_usage_1m")),
        )
        serializable_exchanges[exchange_name] = {
            **{
                key: value
                for key, value in bucket.items()
                if not isinstance(value, Counter)
            },
            "by_category": dict(bucket["by_category"].most_common()),
            "by_path": dict(bucket["by_path"].most_common(top_limit)),
            "status_counts": dict(bucket["status_counts"].most_common()),
            "error_code_counts": dict(bucket["error_code_counts"].most_common()),
            "transient_counts": dict(bucket["transient_counts"].most_common()),
            "max_binance_usage_ratio": max_binance_usage,
        }

        if exchange_name == "binance":
            if int(bucket.get("ip_ban_events") or 0) > 0:
                checks.append(check("FAIL", "binance_ip_ban_events_zero", f"ip_ban_events={bucket['ip_ban_events']}"))
            else:
                checks.append(check("PASS", "binance_ip_ban_events_zero", "ip_ban_events=0"))
            if int(bucket.get("rate_limit_violations") or 0) > 0:
                checks.append(
                    check("FAIL", "binance_rate_limit_violations_zero", f"violations={bucket['rate_limit_violations']}")
                )
            else:
                checks.append(check("PASS", "binance_rate_limit_violations_zero", "violations=0"))
            if max_binance_usage >= fail_usage_ratio:
                checks.append(
                    check("FAIL", "binance_usage_below_fail_threshold", f"max_usage={max_binance_usage:.3f}")
                )
            elif max_binance_usage >= warn_usage_ratio:
                checks.append(check("WARN", "binance_usage_below_warn_threshold", f"max_usage={max_binance_usage:.3f}"))
            else:
                checks.append(check("PASS", "binance_usage_below_warn_threshold", f"max_usage={max_binance_usage:.3f}"))
            if int(bucket.get("local_skips") or 0) > 0:
                checks.append(check("WARN", "binance_local_pause_skips_zero", f"local_skips={bucket['local_skips']}"))

        if exchange_name == "okx":
            if int(bucket.get("rate_limit_violations") or 0) > 0:
                checks.append(check("FAIL", "okx_rate_limit_violations_zero", f"violations={bucket['rate_limit_violations']}"))
            else:
                checks.append(check("PASS", "okx_rate_limit_violations_zero", "violations=0"))
            transient_count = sum(int(value or 0) for value in bucket["transient_counts"].values())
            pause_count = int(bucket.get("private_read_pause_started") or 0) + int(bucket.get("private_read_pause_skips") or 0)
            retry_exhausted = int(bucket.get("retry_exhausted") or 0)
            if transient_count or pause_count or retry_exhausted:
                checks.append(
                    check(
                        "WARN",
                        "okx_transient_pressure_zero",
                        f"transient={transient_count} private_pause={pause_count} retry_exhausted={retry_exhausted}",
                    )
                )
            else:
                checks.append(check("PASS", "okx_transient_pressure_zero", "transient=0 private_pause=0 retry_exhausted=0"))

    checks.append(check("PASS", "api_pressure_data_present", f"summaries={len(summaries)}"))
    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    return {
        "generated_at_ms": now_ms(),
        "summary_count": len(summaries),
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "console_monitor_readiness": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "exchanges": serializable_exchanges,
    }


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


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def build_entry_quality_report(run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    quality_rows = [row for row in run_rows if row.get("event_type") == "entry_quality_analysis"]
    expected_bps_values = [safe_float(row.get("expected_net_edge_bps")) for row in quality_rows]
    expected_open_bps_values = [safe_float(row.get("expected_open_edge_bps")) for row in quality_rows]
    actual_bps_values = [safe_float(row.get("actual_open_edge_bps")) for row in quality_rows]
    slippage_bps_values = [safe_float(row.get("entry_edge_slippage_bps")) for row in quality_rows]
    first_fill_to_hedge_values = [safe_float(row.get("first_fill_to_hedge_fill_ms")) for row in quality_rows]
    total_entry_elapsed_values = [safe_float(row.get("total_entry_elapsed_ms")) for row in quality_rows]
    bad_expected_rows = [
        row
        for row in quality_rows
        if safe_float(row.get("expected_net_edge_bps")) >= 10.0
        and safe_float(row.get("actual_open_edge_bps")) <= 0.0
    ]

    by_symbol: dict[str, dict[str, Any]] = {}
    for row in quality_rows:
        symbol = str(row.get("symbol") or "").upper() or "-"
        bucket = by_symbol.setdefault(
            symbol,
            {
                "count": 0,
                "expected_net_edge_bps_values": [],
                "expected_open_edge_bps_values": [],
                "actual_open_edge_bps_values": [],
                "slippage_bps_values": [],
                "first_fill_to_hedge_fill_ms_values": [],
            },
        )
        bucket["count"] += 1
        bucket["expected_net_edge_bps_values"].append(safe_float(row.get("expected_net_edge_bps")))
        bucket["expected_open_edge_bps_values"].append(safe_float(row.get("expected_open_edge_bps")))
        bucket["actual_open_edge_bps_values"].append(safe_float(row.get("actual_open_edge_bps")))
        bucket["slippage_bps_values"].append(safe_float(row.get("entry_edge_slippage_bps")))
        bucket["first_fill_to_hedge_fill_ms_values"].append(safe_float(row.get("first_fill_to_hedge_fill_ms")))

    symbol_rows: list[dict[str, Any]] = []
    for symbol, bucket in by_symbol.items():
        symbol_rows.append(
            {
                "symbol": symbol,
                "count": bucket["count"],
                "expected_net_edge_bps_avg": avg(bucket["expected_net_edge_bps_values"]),
                "expected_open_edge_bps_avg": avg(bucket["expected_open_edge_bps_values"]),
                "actual_open_edge_bps_avg": avg(bucket["actual_open_edge_bps_values"]),
                "slippage_bps_avg": avg(bucket["slippage_bps_values"]),
                "first_fill_to_hedge_fill_ms_avg": avg(bucket["first_fill_to_hedge_fill_ms_values"]),
            }
        )
    symbol_rows.sort(key=lambda row: (-int(row["count"]), str(row["symbol"])))

    worst_pairs: list[dict[str, Any]] = []
    for row in sorted(
        quality_rows,
        key=lambda item: abs(safe_float(item.get("entry_edge_slippage_bps"))),
        reverse=True,
    )[:5]:
        worst_pairs.append(
            {
                "pair_id": row.get("pair_id"),
                "symbol": str(row.get("symbol") or "").upper(),
                "pair": f"{row.get('long_exchange') or '-'}L/{row.get('short_exchange') or '-'}S",
                "expected_net_edge_bps": safe_float(row.get("expected_net_edge_bps")),
                "expected_open_edge_bps": safe_float(row.get("expected_open_edge_bps")),
                "actual_open_edge_bps": safe_float(row.get("actual_open_edge_bps")),
                "slippage_bps": safe_float(row.get("entry_edge_slippage_bps")),
                "first_fill_to_hedge_fill_ms": safe_float(row.get("first_fill_to_hedge_fill_ms")),
                "total_entry_elapsed_ms": safe_float(row.get("total_entry_elapsed_ms")),
            }
        )

    return {
        "entry_quality_count": len(quality_rows),
        "expected_ge_10_actual_nonpositive_count": len(bad_expected_rows),
        "expected_net_edge_bps_avg": avg(expected_bps_values),
        "expected_open_edge_bps_avg": avg(expected_open_bps_values),
        "actual_open_edge_bps_avg": avg(actual_bps_values),
        "slippage_bps_avg": avg(slippage_bps_values),
        "first_fill_to_hedge_fill_ms_p50": percentile(first_fill_to_hedge_values, 0.50),
        "first_fill_to_hedge_fill_ms_p90": percentile(first_fill_to_hedge_values, 0.90),
        "total_entry_elapsed_ms_p50": percentile(total_entry_elapsed_values, 0.50),
        "total_entry_elapsed_ms_p90": percentile(total_entry_elapsed_values, 0.90),
        "by_symbol": symbol_rows,
        "worst_pairs": worst_pairs,
    }


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


def db_skipped_report(reason: str, *, since_ms: int, run_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at_ms": now_ms(),
        "since_ms": since_ms,
        "since_utc": iso_utc(since_ms),
        "run_meta": run_meta,
        "verdict": "WARN",
        "fail_count": 0,
        "warn_count": 1,
        "checks": [check("WARN", "order_trade_reconcile_query_ok", reason)],
        "summary": {"skipped": reason},
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
    api_pressure = payload.get("api_pressure") or {}
    performance = payload.get("strategy_performance") or {}
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
    lines.append(f"- execution_profile: {strategy_execution_profile(run_meta)}")
    lines.append(f"- enable_live: {run_meta.get('enable_live')}")
    lines.append(f"- auto_enter_enabled: {run_meta.get('auto_enter_enabled')}")
    lines.append(f"- maker_entry_engine_enabled: {run_meta.get('maker_entry_engine_enabled')}")
    lines.append(f"- auto_close_enabled: {run_meta.get('auto_close_enabled')}")
    lines.append(f"- max_open_pairs: {run_meta.get('max_open_pairs')}")
    lines.append(f"- env_file: {run_meta.get('env_file')}")
    lines.append(f"- stdout_log: {run_meta.get('stdout_log')}")
    lines.append(f"- jsonl_log: {run_meta.get('jsonl_log')}")
    lines.append("")
    lines.append("## Event Verdict")
    lines.extend(render_check_lines((afterrun.get("verdict") or {}).get("checks") or []))
    lines.append("")
    lines.append("## Info Counters")
    for key in (
        "maker_entry_preposition_candidate_selected",
        "maker_entry_order_submitted",
        "maker_entry_fill_detected",
        "maker_entry_order_cancelled",
        "maker_entry_symbol_paused",
        "pair_opened",
        "pair_closed",
        "pair_entry_micro_partial_detected",
        "pair_entry_micro_residual_ignored",
        "pair_direction_micro_residual_detected",
        "entry_order_disappeared_settle_started",
        "entry_order_disappeared_resolved",
        "entry_limit_order_submit_preflight",
        "entry_limit_order_submit_result",
        "entry_post_first_fill_edge_check",
        "entry_aborted_after_first_fill_bad_edge",
        "entry_quality_analysis",
        "pair_closed_after_recovery",
        "pair_neutralized_after_close_failure",
    ):
        lines.append(f"- {key}: {event_counts.get(key, 0)}")
    lines.append("")
    lines.append("## Strategy Performance")
    lines.append(f"- execution_profile: {performance.get('execution_profile') or strategy_execution_profile(run_meta)}")
    lines.append(f"- new_entries_disabled_expected: {performance.get('new_entries_disabled_expected')}")
    lines.append(f"- pair_opened: {performance.get('pair_opened', 0)}")
    lines.append(f"- pair_closed: {performance.get('pair_closed', 0)}")
    lines.append(f"- total_close_pnl_usd: {float(performance.get('total_close_pnl_usd') or 0):.6f}")
    lines.append(f"- total_entry_expected_edge_usd: {float(performance.get('total_entry_expected_edge_usd') or 0):.6f}")
    lines.append(f"- realized_minus_expected_usd: {float(performance.get('realized_minus_expected_usd') or 0):.6f}")
    lines.append(f"- avg_close_pnl_usd: {float(performance.get('avg_close_pnl_usd') or 0):.6f}")
    lines.append(f"- avg_hold_duration: {performance.get('avg_hold_duration') or '-'}")
    lines.append(f"- entry_submit_attempts: {performance.get('entry_submit_attempts', 0)}")
    lines.append(f"- candidate_scan_count: {performance.get('candidate_scan_count', 0)}")
    lines.append(f"- enterable_scan_count: {performance.get('enterable_scan_count', 0)}")
    lines.append(f"- max_enterable_candidates: {performance.get('max_enterable_candidates', 0)}")
    close_reasons = performance.get("pair_close_reasons") or {}
    if close_reasons:
        lines.append("- close_reasons: " + ", ".join(f"{key}={value}" for key, value in close_reasons.items()))
    top_block_reasons = performance.get("top_block_reasons") or {}
    if top_block_reasons:
        lines.append("- top_block_reasons: " + ", ".join(f"{key}={value}" for key, value in top_block_reasons.items()))
    top_symbols = performance.get("top_candidate_symbols") or {}
    if top_symbols:
        lines.append("- top_candidate_symbols: " + ", ".join(f"{key}={value}" for key, value in top_symbols.items()))
    pairs = performance.get("pairs") or []
    if pairs:
        lines.append("")
        lines.append("| symbol | pair | qty | entry_edge_bps | close_pnl_usd | hold | close_reason |")
        lines.append("| --- | --- | ---: | ---: | ---: | --- | --- |")
        for row in pairs:
            pair_name = f"{row.get('long_exchange') or '-'}L/{row.get('short_exchange') or '-'}S"
            lines.append(
                "| "
                f"{row.get('symbol') or '-'} | "
                f"{pair_name} | "
                f"{float(row.get('opened_qty') or 0):.8g} | "
                f"{float(row.get('entry_expected_net_edge_bps') or 0):.2f} | "
                f"{float(row.get('close_pnl_usd') or 0):.6f} | "
                f"{row.get('hold_duration') or '-'} | "
                f"{row.get('close_reason') or '-'} |"
            )
    else:
        if performance.get("new_entries_disabled_expected"):
            lines.append("- no closed pairs in this run; expected for monitor/no-entry profile")
        else:
            lines.append("- no closed pairs in this run; treated as neutral, not a failure")
    lines.append("")
    entry_quality = performance.get("entry_quality") or {}
    lines.append("## Entry Quality")
    lines.append(f"- entry_quality_count: {entry_quality.get('entry_quality_count', 0)}")
    lines.append(
        "- expected_ge_10_actual_nonpositive_count: "
        f"{entry_quality.get('expected_ge_10_actual_nonpositive_count', 0)}"
    )
    lines.append(f"- expected_net_edge_bps_avg: {float(entry_quality.get('expected_net_edge_bps_avg') or 0):.2f}")
    lines.append(f"- expected_open_edge_bps_avg: {float(entry_quality.get('expected_open_edge_bps_avg') or 0):.2f}")
    lines.append(f"- actual_open_edge_bps_avg: {float(entry_quality.get('actual_open_edge_bps_avg') or 0):.2f}")
    lines.append(f"- slippage_bps_avg: {float(entry_quality.get('slippage_bps_avg') or 0):.2f}")
    lines.append(
        f"- first_fill_to_hedge_fill_ms_p50/p90: "
        f"{float(entry_quality.get('first_fill_to_hedge_fill_ms_p50') or 0):.1f}/"
        f"{float(entry_quality.get('first_fill_to_hedge_fill_ms_p90') or 0):.1f}"
    )
    symbol_quality = entry_quality.get("by_symbol") or []
    if symbol_quality:
        lines.append("")
        lines.append("| symbol | count | expected_net_bps | expected_open_bps | actual_open_bps | slippage_bps | hedge_lag_ms_avg |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in symbol_quality[:10]:
            lines.append(
                "| "
                f"{row.get('symbol') or '-'} | "
                f"{int(row.get('count') or 0)} | "
                f"{float(row.get('expected_net_edge_bps_avg') or 0):.2f} | "
                f"{float(row.get('expected_open_edge_bps_avg') or 0):.2f} | "
                f"{float(row.get('actual_open_edge_bps_avg') or 0):.2f} | "
                f"{float(row.get('slippage_bps_avg') or 0):.2f} | "
                f"{float(row.get('first_fill_to_hedge_fill_ms_avg') or 0):.1f} |"
            )
    worst_pairs = entry_quality.get("worst_pairs") or []
    if worst_pairs:
        lines.append("")
        lines.append("| symbol | pair | expected_net_bps | expected_open_bps | actual_open_bps | slippage_bps | hedge_lag_ms |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
        for row in worst_pairs:
            lines.append(
                "| "
                f"{row.get('symbol') or '-'} | "
                f"{row.get('pair') or '-'} | "
                f"{float(row.get('expected_net_edge_bps') or 0):.2f} | "
                f"{float(row.get('expected_open_edge_bps') or 0):.2f} | "
                f"{float(row.get('actual_open_edge_bps') or 0):.2f} | "
                f"{float(row.get('slippage_bps') or 0):.2f} | "
                f"{float(row.get('first_fill_to_hedge_fill_ms') or 0):.1f} |"
            )
    lines.append("")
    lines.append("## Entry Submit Price Age")
    if int(submit_telemetry.get("preflight_count") or 0) <= 0:
        lines.append("- no entry submit telemetry yet; available after next strategy restart")
    else:
        lines.append(f"- preflight_count: {submit_telemetry.get('preflight_count')}")
        lines.append(f"- stale_preflight_count: {submit_telemetry.get('stale_preflight_count')}")
        lines.append(f"- blocked_preflight_count: {submit_telemetry.get('blocked_preflight_count', 0)}")
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
    lines.append("## API Pressure")
    lines.extend(render_check_lines(api_pressure.get("checks") or []))
    lines.append(f"- console_monitor_readiness: {api_pressure.get('console_monitor_readiness') or '-'}")
    lines.append(f"- summary_count: {api_pressure.get('summary_count', 0)}")
    for exchange_name, row in sorted((api_pressure.get("exchanges") or {}).items()):
        lines.append(
            f"- {exchange_name}: remote_calls={row.get('remote_calls', 0)} "
            f"local_skips={row.get('local_skips', 0)} "
            f"rate_limit_violations={row.get('rate_limit_violations', 0)} "
            f"retry_exhausted={row.get('retry_exhausted', 0)} "
            f"max_binance_usage={float(row.get('max_binance_usage_ratio') or 0):.3f}"
        )
        by_category = row.get("by_category") or {}
        if by_category:
            lines.append("- " + exchange_name + "_by_category: " + ", ".join(f"{k}={v}" for k, v in by_category.items()))
        by_path = row.get("by_path") or {}
        if by_path:
            lines.append("- " + exchange_name + "_top_paths: " + ", ".join(f"{k}={v}" for k, v in by_path.items()))
        errors = row.get("error_code_counts") or {}
        if errors:
            lines.append("- " + exchange_name + "_error_codes: " + ", ".join(f"{k}={v}" for k, v in errors.items()))
        transients = row.get("transient_counts") or {}
        if transients:
            lines.append("- " + exchange_name + "_transients: " + ", ".join(f"{k}={v}" for k, v in transients.items()))
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
    parser.add_argument(
        "--allow-warnings",
        dest="allow_warnings",
        action="store_true",
        default=True,
        help="Allow WARN component checks in the final verdict. This is the default.",
    )
    parser.add_argument(
        "--strict-warnings",
        dest="allow_warnings",
        action="store_false",
        help="Treat WARN component checks as final verdict blockers.",
    )
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
        from src.ops.persistence.db import db_persistence_enabled, get_db_connection

        if db_persistence_enabled():
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
        else:
            db_report = db_skipped_report("db_persistence_disabled", since_ms=since_ms, run_meta=run_meta)
            db_markdown = render_db_error_markdown(db_report)
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
        from src.ops.persistence.db import db_persistence_enabled

        if db_persistence_enabled():
            symbol_price = collect_symbol_price_report(
                exchanges=exchanges,
                symbols=symbols,
                interval_sec=float(os.getenv("SYMBOL_PRICE_INTERVAL_SEC", "60") or 60),
            )
        else:
            symbol_price = symbol_price_error_report(RuntimeError("db_persistence_disabled"))
    except Exception as exc:
        symbol_price = symbol_price_error_report(exc)
    strategy_performance = build_strategy_performance_report(run_meta)
    api_pressure = build_api_pressure_report(run_meta)

    component_checks = [
        verdict_to_check("afterrun_summary_passed", afterrun_payload.get("verdict") or {}),
        verdict_to_check("order_trade_reconcile_passed", db_report),
        verdict_to_check("live_exchange_flat_passed", live_flat),
        verdict_to_check("symbol_price_collector_passed", symbol_price),
        verdict_to_check("api_pressure_passed", api_pressure),
    ]
    final = build_final_verdict(
        component_checks,
        [afterrun_payload.get("verdict") or {}, db_report, live_flat, symbol_price, api_pressure],
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
        "api_pressure": api_pressure,
        "strategy_performance": strategy_performance,
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
