#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(path: str | Path | None = None, *, override: bool = False, **_: object) -> bool:  # type: ignore[no-redef]
        if path in (None, ""):
            return False
        env_path = Path(path)
        if not env_path.exists():
            return False
        for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and (override or key not in os.environ):
                os.environ[key] = value
        return True


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ops.strategy_afterrun_summary import FAIL_EVENT_TYPES, WARN_EVENT_TYPES  # noqa: E402


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int | None) -> str:
    if not ms:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def resolve_path(raw: str | Path | None) -> Path | None:
    if raw in (None, ""):
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def boolish(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def csv_values(raw: Any, *, upper: bool = False, lower: bool = False) -> list[str]:
    values = [item.strip() for item in str(raw or "").split(",") if item.strip()]
    if upper:
        return [item.upper() for item in values]
    if lower:
        return [item.lower() for item in values]
    return values


def check(status: str, name: str, detail: str) -> dict[str, str]:
    return {"status": status, "name": name, "detail": detail}


def is_ip_ban_line(line: str) -> bool:
    return "IP banned" in line or bool(re.search(r"(?<![\d.])418(?!\d)", line))


def is_exchange_unavailable_line(line: str) -> bool:
    return (
        "http_503" in line
        or "Service temporarily unavailable" in line
        or bool(re.search(r"(?<!\d)50001(?!\d)", line))
    )


def pressure_line_counts(lines: list[str]) -> dict[str, int]:
    return {
        "total_lines": len(lines),
        "rate_limit_lines": sum(
            1
            for line in lines
            if "-1003" in line
            or "Way too many requests" in line
            or "http_429" in line
            or "status=429" in line
            or "rate_limit_pressure" in line
        ),
        "ip_ban_lines": sum(1 for line in lines if is_ip_ban_line(line)),
        "http_429_lines": sum(1 for line in lines if "http_429" in line or "status=429" in line or " 429" in line),
        "exchange_unavailable_lines": sum(1 for line in lines if is_exchange_unavailable_line(line)),
    }


def active_orphan_marker_state() -> tuple[list[dict[str, Any]], str]:
    try:
        from src.ops.orphan_guard import active_orphans

        return active_orphans(), ""
    except Exception as exc:
        return [], str(exc)


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_jsonl(path: Path | None, *, since_ms: int = 0) -> list[dict[str, Any]]:
    if not path or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if int(row.get("ts_ms") or 0) >= since_ms:
            rows.append(row)
    return rows


def tail_lines(path: Path | None, line_count: int) -> list[str]:
    if not path or not path.exists() or line_count <= 0:
        return []
    try:
        proc = subprocess.run(
            ["tail", "-n", str(line_count), str(path)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return proc.stdout.splitlines()
    except Exception:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[-line_count:]


def read_lines(path: Path | None) -> list[str]:
    if not path or not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def current_run_stdout_lines(
    path: Path | None,
    *,
    tail_count: int,
    started_at_iso: str,
) -> tuple[list[str], dict[str, Any]]:
    all_lines = read_lines(path)
    if not all_lines:
        return [], {
            "scope": "missing",
            "stdout_log": str(path) if path else "",
            "total_lines": 0,
            "returned_lines": 0,
            "historical_ignored_lines": 0,
            "trimmed_current_run_lines": 0,
            "started_at_iso": started_at_iso,
        }

    marker_idx = -1
    marker_reason = ""
    marker_text = ""
    if started_at_iso:
        exact_markers = (
            f"[{started_at_iso}] strategy live runtime start",
            f"[{started_at_iso}] [런타임][시작]",
        )
        for idx, line in enumerate(all_lines):
            matched_marker = next((marker for marker in exact_markers if marker in line), "")
            if matched_marker:
                marker_idx = idx
                marker_reason = "started_at_iso"
                marker_text = matched_marker
                break

    if marker_idx < 0:
        for idx in range(len(all_lines) - 1, -1, -1):
            if "strategy live runtime start" in all_lines[idx] or "[런타임][시작]" in all_lines[idx]:
                marker_idx = idx
                marker_reason = "latest_runtime_marker"
                marker_text = all_lines[idx]
                break

    if marker_idx >= 0:
        scoped_lines = all_lines[marker_idx:]
        scope = "current_run" if marker_reason == "started_at_iso" else "latest_runtime_marker"
        historical_ignored = marker_idx
    else:
        scoped_lines = all_lines
        scope = "tail_fallback"
        historical_ignored = 0

    current_run_line_count = len(scoped_lines)
    if tail_count > 0 and len(scoped_lines) > tail_count:
        returned = scoped_lines[-tail_count:]
    else:
        returned = scoped_lines

    return returned, {
        "scope": scope,
        "stdout_log": str(path) if path else "",
        "total_lines": len(all_lines),
        "current_run_lines": current_run_line_count,
        "returned_lines": len(returned),
        "historical_ignored_lines": historical_ignored,
        "trimmed_current_run_lines": max(current_run_line_count - len(returned), 0),
        "started_at_iso": started_at_iso,
        "marker_reason": marker_reason,
        "marker_text": marker_text,
        "marker_line": marker_idx + 1 if marker_idx >= 0 else None,
    }


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def pgrep_count(pattern: str) -> tuple[int, list[int]]:
    if not pattern:
        return 0, []
    proc = subprocess.run(
        ["pgrep", "-f", pattern],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    pids: list[int] = []
    for line in proc.stdout.splitlines():
        try:
            pid = int(line.strip())
        except Exception:
            continue
        if pid != os.getpid():
            pids.append(pid)
    return len(pids), pids


def service_status(name: str, *, pid_file: Path | None, script_path: Path | str | None, required: bool) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    pid_file_pid = 0
    pid_file_alive = False
    if pid_file and pid_file.exists():
        try:
            pid_file_pid = int(pid_file.read_text(encoding="utf-8").strip() or "0")
            pid_file_alive = pid_alive(pid_file_pid)
        except Exception:
            pid_file_pid = 0

    pattern = str(script_path) if script_path else ""
    pgrep_n, pids = pgrep_count(pattern)
    running = pid_file_alive or pgrep_n > 0
    status = "PASS" if running else ("FAIL" if required else "WARN")
    checks.append(
        check(
            status,
            f"{name}_process_running",
            f"running={running} pid_file_pid={pid_file_pid or None} pgrep_count={pgrep_n} pids={pids[:5]}",
        )
    )
    return {
        "name": name,
        "running": running,
        "pid_file": str(pid_file) if pid_file else "",
        "pid_file_pid": pid_file_pid or None,
        "pgrep_count": pgrep_n,
        "pids": pids,
        "checks": checks,
    }


def ops_console_health_url() -> str:
    raw = (os.getenv("OPS_CONSOLE_HEALTH_URL") or os.getenv("OPS_CONSOLE_URL") or "").strip()
    if raw:
        return raw if raw.rstrip("/").endswith("/healthz") else raw.rstrip("/") + "/healthz"

    host = (os.getenv("OPS_CONSOLE_HOST") or "0.0.0.0").strip()
    if host in {"0.0.0.0", "::", ""}:
        host = "127.0.0.1"
    port = (os.getenv("OPS_CONSOLE_PORT") or "8010").strip()
    return f"http://{host}:{port}/healthz"


def collect_ops_console_http_report(*, running: bool, required: bool) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    url = ops_console_health_url()
    payload: dict[str, Any] = {}
    ok = False
    status_code: int | None = None
    error = ""

    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=2.0) as response:
            status_code = int(response.status)
            body = response.read(4096).decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                payload = parsed if isinstance(parsed, dict) else {"raw": parsed}
            except Exception:
                payload = {"raw": body[:300]}
            source_status = str(payload.get("source_status") or "")
            ok = (
                200 <= status_code < 300
                and bool(payload.get("ok", True))
                and bool(payload.get("embedded"))
                and source_status == "strategy_attached"
            )
    except (OSError, URLError, TimeoutError) as exc:
        error = str(exc)
    except Exception as exc:
        error = repr(exc)

    status = "PASS" if ok else ("FAIL" if required or running else "WARN")
    checks.append(
        check(
            status,
            "ops_console_healthz_ok",
            f"url={url} status_code={status_code} running={running} "
            f"source_status={payload.get('source_status') or ''} error={error or ''}",
        )
    )
    return {
        "url": url,
        "ok": ok,
        "status_code": status_code,
        "payload": payload,
        "checks": checks,
    }


def db_enabled_from_env() -> bool:
    required = ("MARIADB_HOST", "MARIADB_USER", "MARIADB_DATABASE")
    return boolish(os.getenv("ENABLE_DB_PERSISTENCE")) and all((os.getenv(key) or "").strip() for key in required)


def collect_db_report(*, exchanges: list[str], interval_sec: float) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    symbol_price: list[dict[str, Any]] = []
    generated_ms = now_ms()

    if not db_enabled_from_env():
        checks.append(check("WARN", "db_persistence_enabled", "enabled=false or required env missing"))
        return {"enabled": False, "verdict": "WARN", "checks": checks, "symbol_price": symbol_price}

    try:
        from src.ops.persistence.db import get_db_connection

        conn = get_db_connection()
    except Exception as exc:
        checks.append(check("FAIL", "db_connection_ok", str(exc)))
        return {"enabled": True, "verdict": "FAIL", "checks": checks, "symbol_price": symbol_price}

    freshness_threshold_ms = int(max(interval_sec * 3.0, 180.0) * 1000)
    try:
        cur = conn.cursor()
        checks.append(check("PASS", "db_connection_ok", "connected"))

        for exchange_name in exchanges:
            cur.execute(
                "SELECT MAX(bucket_start_ms) FROM symbol_price WHERE exchange_name = ?",
                (exchange_name,),
            )
            latest_bucket_ms = int((cur.fetchone() or [0])[0] or 0)
            latest_age_ms = generated_ms - latest_bucket_ms if latest_bucket_ms else None
            rows_n = 0
            symbols_n = 0
            missing_impact_rows = 0
            missing_symbols: list[str] = []
            if latest_bucket_ms:
                cur.execute(
                    """
                    SELECT
                        COUNT(*),
                        COUNT(DISTINCT symbol),
                        SUM(CASE WHEN bid IS NULL OR ask IS NULL THEN 1 ELSE 0 END)
                    FROM symbol_price
                    WHERE exchange_name = ?
                      AND bucket_start_ms = ?
                    """,
                    (exchange_name, latest_bucket_ms),
                )
                fetched = cur.fetchone() or [0, 0, 0]
                rows_n = int(fetched[0] or 0)
                symbols_n = int(fetched[1] or 0)
                missing_impact_rows = int(fetched[2] or 0)
                cur.execute(
                    """
                    SELECT symbol
                    FROM symbol_price
                    WHERE exchange_name = ?
                      AND bucket_start_ms = ?
                      AND (bid IS NULL OR ask IS NULL)
                    ORDER BY symbol
                    """,
                    (exchange_name, latest_bucket_ms),
                )
                missing_symbols = [str(row[0]) for row in cur.fetchall()]

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
                    "PASS" if missing_impact_rows == 0 else "WARN",
                    f"{exchange_name}_symbol_price_impact_complete",
                    f"missing_impact_rows={missing_impact_rows} missing_symbols={missing_symbols}",
                )
            )
            symbol_price.append(
                {
                    "exchange": exchange_name,
                    "latest_bucket_ms": latest_bucket_ms,
                    "latest_bucket_utc": iso_utc(latest_bucket_ms),
                    "latest_age_ms": latest_age_ms,
                    "rows": rows_n,
                    "symbols": symbols_n,
                    "missing_impact_rows": missing_impact_rows,
                    "missing_symbols": missing_symbols,
                }
            )

        try:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM order_history
                WHERE UPPER(COALESCE(status, '')) IN ('NEW', 'LIVE', 'OPEN', 'PARTIALLY_FILLED', 'SUBMITTED', 'UPDATED')
                """
            )
            open_like_count = int((cur.fetchone() or [0])[0] or 0)
            checks.append(check("INFO", "db_open_like_orders_current", f"rows={open_like_count}"))
        except Exception as exc:
            checks.append(check("WARN", "db_open_like_orders_query_ok", str(exc)))

        cur.close()
    except Exception as exc:
        checks.append(check("FAIL", "db_query_ok", str(exc)))
    finally:
        conn.close()

    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    return {
        "enabled": True,
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "checks": checks,
        "symbol_price": symbol_price,
    }


def collect_strategy_log_report(
    run_meta_path: Path,
    *,
    stdout_log: Path,
    tail_count: int,
    strategy_running: bool,
) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    meta = read_json(run_meta_path)
    jsonl_path = resolve_path(meta.get("jsonl_log")) if meta else None
    started_at_ms = int(meta.get("started_at_ms") or 0) if meta else 0
    started_at_iso = str(meta.get("started_at_iso") or iso_utc(started_at_ms))
    rows = load_jsonl(jsonl_path, since_ms=started_at_ms)
    event_counts = Counter(str(row.get("event_type") or "unknown") for row in rows)

    if not meta:
        checks.append(check("WARN", "strategy_run_meta_present", f"missing={run_meta_path}"))
    else:
        checks.append(check("PASS", "strategy_run_meta_present", str(run_meta_path)))
    if meta and not rows:
        checks.append(check("WARN", "strategy_jsonl_rows_present", f"jsonl={jsonl_path} rows=0"))
    elif rows:
        checks.append(check("PASS", "strategy_jsonl_rows_present", f"rows={len(rows)}"))

    for event_type in FAIL_EVENT_TYPES:
        count = event_counts.get(event_type, 0)
        checks.append(check("PASS" if count == 0 else "FAIL", f"{event_type}_zero", f"count={count}"))
    for event_type in WARN_EVENT_TYPES:
        count = event_counts.get(event_type, 0)
        checks.append(check("PASS" if count == 0 else "WARN", f"{event_type}_zero", f"count={count}"))

    orphan_records, orphan_error = active_orphan_marker_state()
    if orphan_error:
        checks.append(check("WARN", "active_orphan_markers_readable", f"error={orphan_error}"))
    else:
        checks.append(
            check(
                "PASS" if not orphan_records else "FAIL",
                "active_orphan_markers_empty",
                f"orphans={orphan_records}",
            )
        )

    lines, stdout_scope = current_run_stdout_lines(
        stdout_log,
        tail_count=tail_count,
        started_at_iso=started_at_iso,
    )
    all_stdout_lines = read_lines(stdout_log)
    marker_line = int(stdout_scope.get("marker_line") or 0)
    historical_lines = all_stdout_lines[: marker_line - 1] if marker_line > 1 else []
    scope = str(stdout_scope.get("scope") or "unknown")
    scope_status = "PASS" if scope == "current_run" else "WARN"
    checks.append(
        check(
            scope_status,
            "strategy_stdout_current_run_scope",
            " ".join(
                [
                    f"scope={scope}",
                    f"started_at_iso={started_at_iso or None}",
                    f"marker_line={stdout_scope.get('marker_line')}",
                    f"current_run_lines={stdout_scope.get('current_run_lines')}",
                    f"returned_lines={stdout_scope.get('returned_lines')}",
                    f"historical_ignored_lines={stdout_scope.get('historical_ignored_lines')}",
                    f"trimmed_current_run_lines={stdout_scope.get('trimmed_current_run_lines')}",
                ]
            ),
        )
    )
    current_pressure = pressure_line_counts(lines)
    historical_pressure = pressure_line_counts(historical_lines)
    danger_count = sum(1 for line in lines if "[strategy][danger]" in line)
    ip_ban_count = current_pressure["ip_ban_lines"]
    rate_limit_count = current_pressure["rate_limit_lines"]
    exchange_unavailable_count = current_pressure["exchange_unavailable_lines"]
    stopped_strategy_issue_status = "FAIL" if strategy_running else "WARN"
    stdout_detail = (
        f"scope={scope} returned_lines={len(lines)} "
        f"historical_ignored_lines={stdout_scope.get('historical_ignored_lines')} "
        f"strategy_running={strategy_running}"
    )
    checks.append(
        check(
            "PASS" if danger_count == 0 else stopped_strategy_issue_status,
            "strategy_danger_log_lines_zero",
            f"{stdout_detail} count={danger_count}",
        )
    )
    checks.append(
        check(
            "PASS" if ip_ban_count == 0 else stopped_strategy_issue_status,
            "exchange_ip_ban_lines_zero",
            f"{stdout_detail} count={ip_ban_count}",
        )
    )
    checks.append(check("PASS" if rate_limit_count == 0 else "WARN", "rate_limit_pressure_lines_zero", f"{stdout_detail} count={rate_limit_count}"))
    checks.append(check("PASS" if exchange_unavailable_count == 0 else "WARN", "exchange_unavailable_lines_zero", f"{stdout_detail} count={exchange_unavailable_count}"))
    checks.append(
        check(
            "INFO",
            "strategy_historical_rate_limit_lines",
            " ".join(
                [
                    f"historical_lines={len(historical_lines)}",
                    f"rate_limit={historical_pressure['rate_limit_lines']}",
                    f"ip_ban={historical_pressure['ip_ban_lines']}",
                    f"exchange_unavailable={historical_pressure['exchange_unavailable_lines']}",
                ]
            ),
        )
    )

    return {
        "run_meta": meta,
        "jsonl_path": str(jsonl_path) if jsonl_path else "",
        "run_rows": len(rows),
        "event_counts": dict(event_counts.most_common(20)),
        "stdout_scope": stdout_scope,
        "active_orphans": orphan_records,
        "active_orphan_error": orphan_error,
        "rate_limit_scope": {
            "current_run": current_pressure,
            "historical": historical_pressure,
        },
        "checks": checks,
    }


def collect_collector_log_report(stdout_log: Path, *, tail_count: int) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    lines = tail_lines(stdout_log, tail_count)
    collector_pressure = pressure_line_counts(lines)
    okx_reconnect_count = sum(1 for line in lines if "[OKXFuture WS] reconnect" in line)
    binance_reconnect_count = sum(1 for line in lines if "[BinanceFuture WS] reconnect" in line)
    traceback_count = sum(1 for line in lines if "Traceback" in line)
    rate_limit_count = collector_pressure["rate_limit_lines"]
    exchange_unavailable_count = collector_pressure["exchange_unavailable_lines"]
    symbol_rows = [line for line in lines if line.startswith("[symbol-price]")]
    latest_symbol_rows = symbol_rows[-2:]

    checks.append(check("PASS" if traceback_count == 0 else "FAIL", "collector_traceback_lines_zero", f"tail_lines={tail_count} count={traceback_count}"))
    checks.append(check("PASS" if rate_limit_count == 0 else "WARN", "collector_rate_limit_lines_zero", f"tail_lines={tail_count} count={rate_limit_count}"))
    checks.append(check("PASS" if exchange_unavailable_count == 0 else "WARN", "collector_exchange_unavailable_lines_zero", f"tail_lines={tail_count} count={exchange_unavailable_count}"))
    checks.append(check("PASS" if okx_reconnect_count <= 5 else "WARN", "collector_okx_ws_reconnect_reasonable", f"tail_lines={tail_count} count={okx_reconnect_count}"))
    checks.append(check("PASS" if binance_reconnect_count <= 2 else "WARN", "collector_binance_ws_reconnect_reasonable", f"tail_lines={tail_count} count={binance_reconnect_count}"))

    return {
        "stdout_log": str(stdout_log),
        "latest_symbol_rows": latest_symbol_rows,
        "rate_limit_scope": {
            "collector_tail": collector_pressure,
        },
        "checks": checks,
    }


def combine_verdict(sections: list[dict[str, Any]]) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    for section in sections:
        checks.extend(section.get("checks") or [])
    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    info_count = sum(1 for item in checks if item["status"] == "INFO")
    return {
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "fail_count": fail_count,
        "warn_count": warn_count,
        "info_count": info_count,
        "checks": checks,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    verdict = payload["verdict"]
    lines.append("# Ops Health Report")
    lines.append("")
    lines.append(f"- verdict: `{verdict['verdict']}`")
    lines.append(f"- generated_at_utc: `{payload['generated_at_utc']}`")
    lines.append(f"- failures: `{verdict['fail_count']}`")
    lines.append(f"- warnings: `{verdict['warn_count']}`")
    lines.append("")
    lines.append("## Services")
    for service in payload.get("services", []):
        lines.append(f"- {service['name']}: running={service['running']} pids={service.get('pids') or []}")
    lines.append("")
    stdout_scope = (payload.get("strategy_logs") or {}).get("stdout_scope") or {}
    if stdout_scope:
        lines.append("## Strategy Log Scope")
        lines.append(
            "- "
            f"scope={stdout_scope.get('scope')} "
            f"started_at_iso={stdout_scope.get('started_at_iso') or ''} "
            f"marker_line={stdout_scope.get('marker_line')} "
            f"current_run_lines={stdout_scope.get('current_run_lines')} "
            f"returned_lines={stdout_scope.get('returned_lines')} "
            f"historical_ignored_lines={stdout_scope.get('historical_ignored_lines')}"
        )
        lines.append("")
    active_orphans = (payload.get("strategy_logs") or {}).get("active_orphans") or []
    active_orphan_error = (payload.get("strategy_logs") or {}).get("active_orphan_error") or ""
    lines.append("## Active Orphan Markers")
    if active_orphan_error:
        lines.append(f"- error: {active_orphan_error}")
    elif active_orphans:
        for row in active_orphans:
            lines.append(f"- {row}")
    else:
        lines.append("- none")
    lines.append("")
    strategy_rate_scope = (payload.get("strategy_logs") or {}).get("rate_limit_scope") or {}
    collector_rate_scope = (payload.get("collector_logs") or {}).get("rate_limit_scope") or {}
    if strategy_rate_scope or collector_rate_scope:
        lines.append("## Rate Limit Scope")
        for label, row in (
            ("strategy_current_run", strategy_rate_scope.get("current_run") or {}),
            ("strategy_historical", strategy_rate_scope.get("historical") or {}),
            ("collector_tail", collector_rate_scope.get("collector_tail") or {}),
        ):
            if not row:
                continue
            lines.append(
                "- "
                f"{label}: total_lines={row.get('total_lines', 0)} "
                f"rate_limit={row.get('rate_limit_lines', 0)} "
                f"ip_ban={row.get('ip_ban_lines', 0)} "
                f"http_429={row.get('http_429_lines', 0)} "
                f"exchange_unavailable={row.get('exchange_unavailable_lines', 0)}"
            )
        lines.append("")
    lines.append("## Symbol Price")
    for row in (payload.get("db") or {}).get("symbol_price") or []:
        lines.append(
            "- "
            f"{row['exchange']}: latest={row['latest_bucket_utc']} "
            f"age_ms={row['latest_age_ms']} rows={row['rows']} symbols={row['symbols']} "
            f"missing_impact={row['missing_impact_rows']} missing_symbols={row['missing_symbols']}"
        )
    lines.append("")
    lines.append("## Checks")
    for item in verdict["checks"]:
        lines.append(f"- `{item['status']}` `{item['name']}`: {item['detail']}")
    return "\n".join(lines) + "\n"


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    env_path = resolve_path(args.env_file)
    if env_path and env_path.exists():
        load_dotenv(env_path, override=True)
    if env_path:
        os.environ["STRATEGY_ENV_FILE"] = str(env_path)

    generated_ms = now_ms()
    strategy_log_dir = resolve_path(os.getenv("STRATEGY_LIVE_LOG_DIR") or "logs") or (PROJECT_ROOT / "logs")
    symbol_log_dir = resolve_path(os.getenv("SYMBOL_PRICE_LOG_DIR") or os.getenv("STRATEGY_LIVE_LOG_DIR") or "logs") or (PROJECT_ROOT / "logs")
    ops_console_log_dir = resolve_path(os.getenv("OPS_CONSOLE_LOG_DIR") or os.getenv("STRATEGY_LIVE_LOG_DIR") or "logs") or (PROJECT_ROOT / "logs")

    strategy_script = resolve_path(os.getenv("STRATEGY_SCRIPT_PATH") or "main.py")
    symbol_script = resolve_path(os.getenv("SYMBOL_PRICE_SCRIPT_PATH") or "scripts/runtime/symbol_price_collector.py")
    strategy_pid_file = resolve_path(os.getenv("STRATEGY_LIVE_PID_FILE") or str(strategy_log_dir / "strategy_live.pid"))
    symbol_pid_file = resolve_path(os.getenv("SYMBOL_PRICE_PID_FILE") or str(symbol_log_dir / "symbol_price_collector.pid"))
    ops_console_pid_file = resolve_path(os.getenv("OPS_CONSOLE_PID_FILE") or str(ops_console_log_dir / "ops_console.pid"))
    strategy_stdout = resolve_path(os.getenv("STRATEGY_LIVE_STDOUT_LOG") or str(strategy_log_dir / "strategy_live.out")) or (strategy_log_dir / "strategy_live.out")
    symbol_stdout = resolve_path(os.getenv("SYMBOL_PRICE_STDOUT_LOG") or str(symbol_log_dir / "symbol_price_collector.out")) or (symbol_log_dir / "symbol_price_collector.out")
    run_meta = resolve_path(args.meta or os.getenv("STRATEGY_LIVE_RUN_META_FILE") or str(strategy_log_dir / "strategy_live.run.json")) or (strategy_log_dir / "strategy_live.run.json")

    exchanges = csv_values(os.getenv("SYMBOL_PRICE_EXCHANGES") or os.getenv("STRATEGY_EXCHANGES") or "binance,okx", lower=True)
    interval_sec = float(os.getenv("SYMBOL_PRICE_INTERVAL_SEC") or "60")

    services = [
        service_status("strategy", pid_file=strategy_pid_file, script_path=strategy_script, required=False),
        service_status("symbol_price_collector", pid_file=symbol_pid_file, script_path=symbol_script, required=args.require_collector),
        service_status(
            "ops_console",
            pid_file=ops_console_pid_file,
            script_path="src.ops_console.app:app",
            required=args.require_ops_console,
        ),
    ]
    strategy_running = bool(services[0].get("running"))
    strategy_logs = collect_strategy_log_report(
        run_meta,
        stdout_log=strategy_stdout,
        tail_count=args.tail_lines,
        strategy_running=strategy_running,
    )
    collector_logs = collect_collector_log_report(symbol_stdout, tail_count=args.tail_lines)
    legacy_console_detected = bool(services[2].get("running"))
    ops_console = collect_ops_console_http_report(running=legacy_console_detected, required=args.require_ops_console)
    if ops_console.get("ok"):
        services[2]["running"] = True
        services[2]["checks"] = [
            check(
                "PASS",
                "ops_console_process_running",
                "running=embedded_http_only pid_file_pid=None pgrep_count=0 pids=[]",
            )
        ]
    elif legacy_console_detected:
        services[2]["running"] = False
        services[2]["checks"] = [
            check(
                "FAIL" if args.require_ops_console else "WARN",
                "ops_console_process_running",
                "legacy_standalone_detected=true standalone_disabled=true",
            )
        ]
    db = collect_db_report(exchanges=exchanges, interval_sec=interval_sec)
    combined = combine_verdict([*services, strategy_logs, collector_logs, ops_console, db])
    return {
        "generated_at_ms": generated_ms,
        "generated_at_utc": iso_utc(generated_ms),
        "env_file": str(env_path) if env_path else "",
        "verdict": combined,
        "services": services,
        "strategy_logs": strategy_logs,
        "collector_logs": collector_logs,
        "ops_console": ops_console,
        "db": db,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Report live ops health for strategy, collector, DB, and recent logs.")
    parser.add_argument("--env-file", default=os.getenv("STRATEGY_ENV_FILE") or ".env")
    parser.add_argument("--meta", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--json-output", default="")
    parser.add_argument("--tail-lines", type=int, default=5000)
    parser.add_argument("--require-collector", action="store_true", default=True)
    parser.add_argument("--collector-optional", action="store_false", dest="require_collector")
    parser.add_argument("--require-ops-console", action="store_true")
    args = parser.parse_args()

    payload = build_payload(args)
    markdown = render_markdown(payload)
    if args.output:
        out = resolve_path(args.output)
        assert out is not None
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")

    if args.json_output:
        out_json = resolve_path(args.json_output)
        assert out_json is not None
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    verdict = payload["verdict"]
    print(
        f"ops_health_verdict={verdict['verdict']} warnings={verdict['warn_count']} "
        f"failures={verdict['fail_count']}"
    )
    return 1 if verdict["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
