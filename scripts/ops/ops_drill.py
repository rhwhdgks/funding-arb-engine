#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def resolve_path(raw: str | Path | None) -> Path | None:
    if raw in (None, ""):
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def check(status: str, name: str, detail: str) -> dict[str, str]:
    return {"status": status, "name": name, "detail": detail}


def run_cmd(args: list[str], *, timeout: int = 30, env: dict[str, str] | None = None) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env=env or os.environ.copy(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    return {
        "args": args,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def load_entry_control(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {"entry_paused": False, "source_path": str(path) if path else ""}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"entry_paused": True, "reason": "entry_control_read_error", "error": str(exc), "source_path": str(path)}
    if not isinstance(payload, dict):
        return {"entry_paused": True, "reason": "entry_control_invalid_payload", "source_path": str(path)}
    payload["source_path"] = str(path)
    return payload


def restore_entry_control(path: Path | None, *, existed: bool, payload: dict[str, Any]) -> dict[str, Any]:
    if path is None:
        return {"restored": False, "reason": "missing_path"}
    if not existed:
        try:
            path.unlink(missing_ok=True)
            return {"restored": True, "removed": True, "source_path": str(path)}
        except Exception as exc:
            return {"restored": False, "error": str(exc), "source_path": str(path)}

    restored_payload = {key: value for key, value in payload.items() if key != "source_path"}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.ops_drill_restore.tmp")
        tmp_path.write_text(json.dumps(restored_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(path)
        restored_payload["source_path"] = str(path)
        return {"restored": True, "payload": restored_payload}
    except Exception as exc:
        return {"restored": False, "error": str(exc), "source_path": str(path)}


def service_status(command: list[str], *, expected_running: bool | None = None) -> tuple[dict[str, Any], dict[str, str]]:
    result = run_cmd(command, timeout=15)
    output = "\n".join(part for part in (result["stdout"], result["stderr"]) if part)
    running = "running" in output and "not running" not in output
    if expected_running is None:
        status = "INFO"
    else:
        status = "PASS" if running == expected_running else "FAIL"
    return result, check(status, "_".join(command[-2:]) + "_status", f"running={running} output={output[:240]}")


def parse_verdict_line(output: str, prefix: str) -> tuple[str, int, int]:
    verdict = "UNKNOWN"
    warnings = 0
    failures = 0
    for line in output.splitlines():
        if not line.startswith(prefix):
            continue
        parts = dict(part.split("=", 1) for part in line.split() if "=" in part)
        verdict = parts.get(prefix.rstrip("_verdict"), parts.get("ops_health_verdict", parts.get("db_persistence_smoke_verdict", "UNKNOWN")))
        warnings = int(parts.get("warnings", "0") or 0)
        failures = int(parts.get("failures", "0") or 0)
    return verdict, warnings, failures


def first_strategy_symbol() -> str:
    raw = os.getenv("STRATEGY_SYMBOLS") or os.getenv("WEB_RUNTIME_SYMBOLS") or "BNB"
    for item in raw.split(","):
        symbol = item.strip().upper()
        if symbol:
            return symbol
    return "BNB"


def run_console_active_conflict_drill(env: dict[str, str], *, generated_ms: int) -> tuple[dict[str, Any], list[dict[str, str]]]:
    log_dir = resolve_path(os.getenv("STRATEGY_LIVE_LOG_DIR") or "logs") or (PROJECT_ROOT / "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    symbol = first_strategy_symbol()
    pair_id = f"ops-drill:{symbol}:{generated_ms}"
    synthetic_log = log_dir / f"ops_console_active_conflict_drill_{generated_ms}.jsonl"
    rows = [
        {
            "ts_ms": generated_ms,
            "event_type": "pair_opened",
            "pair": {"pair_id": pair_id, "symbol": symbol},
        },
    ]
    synthetic_log.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")

    drill_env = env.copy()
    drill_env["STRATEGY_LIVE_EVENT_LOG_PATH"] = str(synthetic_log)
    block_code = f"""
from src.ops_console.action_gate import active_strategy_symbols, assert_no_strategy_conflict
active = active_strategy_symbols(max_lines=50)
assert active.get({pair_id!r}) == {symbol!r}, active
try:
    assert_no_strategy_conflict({symbol!r})
except ValueError as exc:
    print(str(exc))
else:
    raise SystemExit('expected conflict block')
"""
    block_result = run_cmd([sys.executable, "-c", block_code], env=drill_env, timeout=15)

    with synthetic_log.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "ts_ms": generated_ms + 1,
                    "event_type": "pair_closed",
                    "pair": {"pair_id": pair_id, "symbol": symbol},
                },
                ensure_ascii=False,
            )
            + "\n"
        )

    release_code = f"""
from src.ops_console.action_gate import active_strategy_symbols, assert_no_strategy_conflict
active = active_strategy_symbols(max_lines=50)
assert {pair_id!r} not in active, active
assert_no_strategy_conflict({symbol!r})
print('released')
"""
    release_result = run_cmd([sys.executable, "-c", release_code], env=drill_env, timeout=15)

    checks = [
        check(
            "PASS" if block_result["returncode"] == 0 else "FAIL",
            "ops_console_active_pair_blocks_live_entry",
            f"symbol={symbol} returncode={block_result['returncode']} output={(block_result['stdout'] or block_result['stderr'])[:240]}",
        ),
        check(
            "PASS" if release_result["returncode"] == 0 else "FAIL",
            "ops_console_closed_pair_releases_live_entry",
            f"symbol={symbol} returncode={release_result['returncode']} output={(release_result['stdout'] or release_result['stderr'])[:240]}",
        ),
    ]
    return {
        "symbol": symbol,
        "pair_id": pair_id,
        "synthetic_log": str(synthetic_log),
        "block_result": block_result,
        "release_result": release_result,
    }, checks


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    env_path = resolve_path(args.env_file)
    if env_path and env_path.exists():
        load_dotenv(env_path, override=True)
    env = os.environ.copy()
    if env_path:
        env["STRATEGY_ENV_FILE"] = str(env_path)

    generated_ms = now_ms()
    checks: list[dict[str, str]] = []
    artifacts: dict[str, Any] = {}

    strategy_status, strategy_check = service_status(
        ["scripts/runtime/run_strategy_live.sh", "status"],
        expected_running=args.expect_strategy_running,
    )
    collector_status, collector_check = service_status(
        ["scripts/runtime/run_symbol_price_collector.sh", "status"],
        expected_running=args.expect_collector_running,
    )
    expect_ops_console_running = args.expect_ops_console_running
    if expect_ops_console_running is None and args.expect_strategy_running is False:
        expect_ops_console_running = False
    elif expect_ops_console_running is None and args.expect_strategy_running is True:
        expect_ops_console_running = True

    ops_console_status, ops_console_check = service_status(
        ["scripts/runtime/run_ops_console.sh", "status"],
        expected_running=expect_ops_console_running,
    )
    artifacts["strategy_status"] = strategy_status
    artifacts["collector_status"] = collector_status
    artifacts["ops_console_status"] = ops_console_status
    checks.extend([strategy_check, collector_check, ops_console_check])

    entry_control_path = resolve_path(os.getenv("STRATEGY_ENTRY_CONTROL_FILE") or "runtime/entry_control.json")
    entry_control_existed = bool(entry_control_path and entry_control_path.exists())
    before_control = load_entry_control(entry_control_path)
    checks.append(
        check(
            "PASS" if "error" not in before_control else "FAIL",
            "entry_control_readable",
            f"path={entry_control_path} paused={before_control.get('entry_paused')} reason={before_control.get('reason')}",
        )
    )
    artifacts["entry_control_before"] = before_control

    if args.apply_pause_drill:
        pause_on = run_cmd(
            ["scripts/runtime/entry_pause.sh", "on", "ops_drill_pause"],
            env=env,
            timeout=15,
        )
        paused_state = load_entry_control(entry_control_path)
        pause_off = run_cmd(
            ["scripts/runtime/entry_pause.sh", "off", "ops_drill_resume"],
            env=env,
            timeout=15,
        )
        resumed_state = load_entry_control(entry_control_path)
        restore_result = restore_entry_control(
            entry_control_path,
            existed=entry_control_existed,
            payload=before_control,
        )
        restored_state = load_entry_control(entry_control_path)
        artifacts["pause_on"] = pause_on
        artifacts["pause_off"] = pause_off
        artifacts["entry_control_restore"] = restore_result
        artifacts["entry_control_paused_state"] = paused_state
        artifacts["entry_control_resumed_state"] = resumed_state
        artifacts["entry_control_restored_state"] = restored_state
        checks.append(
            check(
                "PASS" if paused_state.get("entry_paused") is True else "FAIL",
                "entry_pause_on_applied",
                f"paused={paused_state.get('entry_paused')} reason={paused_state.get('reason')}",
            )
        )
        checks.append(
            check(
                "PASS" if resumed_state.get("entry_paused") is False else "FAIL",
                "entry_pause_off_applied",
                f"paused={resumed_state.get('entry_paused')} reason={resumed_state.get('reason')}",
            )
        )
        checks.append(
            check(
                "PASS" if restore_result.get("restored") else "FAIL",
                "entry_control_restored",
                f"path={entry_control_path} restored={restore_result.get('restored')} final_reason={restored_state.get('reason')}",
            )
        )
    else:
        checks.append(check("INFO", "entry_pause_drill_skipped", "pass --apply-pause-drill to mutate entry_control"))

    if args.skip_console_conflict_drill:
        checks.append(check("INFO", "ops_console_active_conflict_drill_skipped", "skipped by option"))
    else:
        conflict_artifact, conflict_checks = run_console_active_conflict_drill(env, generated_ms=generated_ms)
        artifacts["ops_console_active_conflict_drill"] = conflict_artifact
        checks.extend(conflict_checks)

    health_cmd = [
        sys.executable,
        "scripts/ops/ops_health_report.py",
        "--env-file",
        str(env_path or ".env"),
        "--tail-lines",
        str(args.tail_lines),
    ]
    health = run_cmd(health_cmd, env=env, timeout=45)
    artifacts["health"] = health
    health_verdict, health_warnings, health_failures = parse_verdict_line(health["stdout"], "ops_health_verdict")
    checks.append(
        check(
            "PASS" if health["returncode"] == 0 and health_failures == 0 else "FAIL",
            "ops_health_failures_zero",
            f"verdict={health_verdict} warnings={health_warnings} failures={health_failures}",
        )
    )

    db_smoke = run_cmd(
        [sys.executable, "scripts/ops/db_persistence_smoke.py"],
        env=env,
        timeout=30,
    )
    artifacts["db_smoke"] = db_smoke
    db_verdict, _db_warnings, db_failures = parse_verdict_line(db_smoke["stdout"], "db_persistence_smoke_verdict")
    checks.append(
        check(
            "PASS" if db_smoke["returncode"] == 0 and db_failures == 0 else "FAIL",
            "db_persistence_smoke_passed",
            f"verdict={db_verdict} failures={db_failures}",
        )
    )

    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    return {
        "generated_at_ms": generated_ms,
        "generated_at_utc": iso_utc(generated_ms),
        "env_file": str(env_path) if env_path else "",
        "verdict": "FAIL" if fail_count else ("WARN" if warn_count else "PASS"),
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "artifacts": artifacts,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Ops Drill Report",
        "",
        f"- verdict: `{payload['verdict']}`",
        f"- generated_at_utc: `{payload['generated_at_utc']}`",
        f"- failures: `{payload['fail_count']}`",
        f"- warnings: `{payload['warn_count']}`",
        "",
        "## Checks",
    ]
    for item in payload["checks"]:
        lines.append(f"- `{item['status']}` `{item['name']}`: {item['detail']}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run non-destructive ops drill checks.")
    parser.add_argument("--env-file", default=os.getenv("STRATEGY_ENV_FILE") or ".env")
    parser.add_argument("--output", default="")
    parser.add_argument("--json-output", default="")
    parser.add_argument("--tail-lines", type=int, default=2000)
    parser.add_argument("--apply-pause-drill", action="store_true")
    parser.add_argument("--expect-strategy-running", action="store_const", const=True, default=None, dest="expect_strategy_running")
    parser.add_argument("--expect-strategy-stopped", action="store_const", const=False, dest="expect_strategy_running")
    parser.add_argument("--expect-collector-running", action="store_true", default=True)
    parser.add_argument("--expect-ops-console-running", action="store_const", const=True, default=None, dest="expect_ops_console_running")
    parser.add_argument("--expect-ops-console-stopped", action="store_const", const=False, dest="expect_ops_console_running")
    parser.add_argument("--skip-console-conflict-drill", action="store_true")
    args = parser.parse_args()

    payload = build_payload(args)
    markdown = render_markdown(payload)
    if args.output:
        output = resolve_path(args.output)
        assert output is not None
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")
    if args.json_output:
        output = resolve_path(args.json_output)
        assert output is not None
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    print(f"ops_drill_verdict={payload['verdict']} warnings={payload['warn_count']} failures={payload['fail_count']}")
    return 1 if payload["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
