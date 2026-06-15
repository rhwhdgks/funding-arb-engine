#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]


SMOKE_SNIPPET = r"""
import asyncio
import os
import time

from src.ops.persistence.db import db_persistence_enabled
from src.ops.persistence.entry_quality import schedule_entry_quality_persist
from src.ops.persistence.event_hooks import (
    schedule_absent_open_orders_cleanup,
    schedule_order_event,
    schedule_trade_history_upsert,
)
from src.ops.persistence.live_snapshot import persist_exchange_snapshots
from src.ops.persistence.runtime import build_runtime_persistence_from_env


class FakeExchange:
    def get_account_snapshot(self):
        return {
            "asset_dict": {"USDT": {"balance": 1000, "available": 1000}},
            "total_balance": 1000,
        }

    def get_symbol_snapshot(self, symbol):
        return {
            "position": {"qty": 0, "entry_price": 0, "mark_price": 100},
            "funding": {"rate": 0.0, "next_funding_time_ms": int(time.time() * 1000) + 3600000},
            "tick": {"best_bid": 99, "best_ask": 101},
        }


async def run():
    enabled = db_persistence_enabled()
    runtime_persistence = build_runtime_persistence_from_env(log=lambda *_args, **_kwargs: None)

    schedule_order_event(
        "binance",
        "BTC",
        "smoke_order",
        "db_persistence_smoke",
        {
            "symbol": "BTC",
            "side": "buy",
            "qty": 0.001,
            "price": 100,
            "status": "NEW",
            "timestamp": int(time.time() * 1000),
        },
    )
    schedule_trade_history_upsert(
        "binance",
        "BTC",
        [
            {
                "trade_id": "smoke-trade",
                "order_id": "smoke-order",
                "side": "buy",
                "qty": 0.001,
                "price": 100,
                "fee": 0.01,
                "timestamp": int(time.time() * 1000),
            }
        ],
    )
    schedule_absent_open_orders_cleanup("binance", "BTC", int(time.time() * 1000))
    schedule_entry_quality_persist(
        {
            "event_type": "entry_quality_analysis",
            "pair_id": "smoke-pair",
            "symbol": "BTC",
            "ts_ms": int(time.time() * 1000),
            "expected_net_edge_bps": 10,
            "actual_open_edge_bps": 9,
            "entry_slippage_bps": 1,
        }
    )

    snapshot_result = None
    snapshot_error = None
    try:
        snapshot_result = await persist_exchange_snapshots(
            "binance",
            FakeExchange(),
            ["BTC"],
            persist_funding=True,
            persist_balance=True,
            persist_positions=True,
        )
    except Exception as exc:
        snapshot_error = str(exc)

    print(
        {
            "db_persistence_enabled": enabled,
            "runtime_persistence_enabled": runtime_persistence.enabled,
            "snapshot_result": snapshot_result,
            "snapshot_error": snapshot_error,
        }
    )


asyncio.run(run())
"""


def run_case(
    name: str,
    env_updates: dict[str, str],
    *,
    expect_success: bool,
    expect_db_enabled: bool | None,
    expect_snapshot_error: bool | None = None,
) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(env_updates)
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", SMOKE_SNIPPET],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=False,
    )
    passed = proc.returncode == 0 if expect_success else proc.returncode != 0
    enabled_match = True
    if expect_db_enabled is not None and proc.returncode == 0:
        enabled_match = f"'db_persistence_enabled': {expect_db_enabled}" in proc.stdout
    snapshot_error_match = True
    if expect_snapshot_error is not None and proc.returncode == 0:
        has_snapshot_error = "'snapshot_error': None" not in proc.stdout
        snapshot_error_match = has_snapshot_error == expect_snapshot_error
    return {
        "name": name,
        "passed": bool(passed and enabled_match and snapshot_error_match),
        "returncode": proc.returncode,
        "expect_success": expect_success,
        "expect_db_enabled": expect_db_enabled,
        "expect_snapshot_error": expect_snapshot_error,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip().splitlines()[-10:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test that persistence hooks do not break DB-off operation.")
    parser.add_argument("--json-output", default="")
    args = parser.parse_args()

    disabled_env = {
        "STRATEGY_ENV_FILE": "",
        "ENABLE_DB_PERSISTENCE": "0",
        "MARIADB_HOST": "",
        "MARIADB_USER": "",
        "MARIADB_PASSWORD": "",
        "MARIADB_DATABASE": "",
    }
    missing_required_env = {
        "STRATEGY_ENV_FILE": "",
        "ENABLE_DB_PERSISTENCE": "1",
        "MARIADB_HOST": "",
        "MARIADB_USER": "",
        "MARIADB_PASSWORD": "",
        "MARIADB_DATABASE": "",
    }
    bad_configured_db_env = {
        "STRATEGY_ENV_FILE": "",
        "ENABLE_DB_PERSISTENCE": "1",
        "MARIADB_HOST": "127.0.0.1",
        "MARIADB_PORT": "1",
        "MARIADB_USER": "trader",
        "MARIADB_PASSWORD": "bad",
        "MARIADB_DATABASE": "trading_dev",
    }

    cases = [
        run_case(
            "db_disabled_no_env",
            disabled_env,
            expect_success=True,
            expect_db_enabled=False,
        ),
        run_case(
            "db_enabled_but_required_env_missing",
            missing_required_env,
            expect_success=True,
            expect_db_enabled=False,
            expect_snapshot_error=False,
        ),
        run_case(
            "db_configured_but_unreachable",
            bad_configured_db_env,
            expect_success=True,
            expect_db_enabled=True,
            expect_snapshot_error=True,
        ),
    ]
    failures = [case for case in cases if not case["passed"]]
    payload = {
        "verdict": "FAIL" if failures else "PASS",
        "cases": cases,
    }
    if args.json_output:
        output = Path(args.json_output)
        if not output.is_absolute():
            output = PROJECT_ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    for case in cases:
        status = "PASS" if case["passed"] else "FAIL"
        print(f"{status} {case['name']} returncode={case['returncode']} stdout={case['stdout']}")
        for line in case["stderr"]:
            print(textwrap.shorten(f"  stderr: {line}", width=240))
    print(f"db_persistence_smoke_verdict={payload['verdict']} failures={len(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
