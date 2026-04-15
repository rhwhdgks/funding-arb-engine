#!/usr/bin/env python3
import argparse
import asyncio
import contextlib
import io
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
load_dotenv()

from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.sync import build_exchange


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect lightweight strategy balance snapshots.")
    parser.add_argument("--exchanges", required=True, help="Comma-separated exchange names, e.g. binance,okx")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols used to warm the exchange runtime")
    parser.add_argument("--wait-ws-sec", type=float, default=1.5)
    return parser.parse_args()


def parse_csv_symbols(value: str) -> list[str]:
    return [part.strip().upper() for part in value.split(",") if part.strip()]


async def collect_one(exchange_name: str, symbols: list[str], wait_ws_sec: float) -> dict[str, Any]:
    exchange = build_exchange(exchange_name, symbols)
    try:
        await exchange.run()
        await asyncio.sleep(wait_ws_sec)
        account_snapshot = exchange.get_account_snapshot()
        symbol_snapshots = {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
        balance_report = build_balance_report(exchange_name, account_snapshot, symbol_snapshots)
        return {
            "exchange": exchange_name,
            "symbols": symbols,
            "total_balance": account_snapshot.get("total_balance"),
            "actual_equity_usd": balance_report.get("actual_equity_usd"),
            "theoretical_equity_usd": balance_report.get("theoretical_equity_usd"),
            "gross_notional_usd": balance_report.get("gross_notional_usd"),
        }
    except Exception as exc:
        return {
            "exchange": exchange_name,
            "symbols": symbols,
            "error": repr(exc),
        }
    finally:
        try:
            await exchange.stop()
        except Exception:
            pass


async def main() -> int:
    args = parse_args()
    exchanges = [part.strip() for part in args.exchanges.split(",") if part.strip()]
    symbols = parse_csv_symbols(args.symbols)
    captured_stdout = io.StringIO()
    captured_stderr = io.StringIO()
    with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
        rows = await asyncio.gather(
            *[collect_one(exchange_name, symbols, args.wait_ws_sec) for exchange_name in exchanges]
        )
    print(json.dumps({row["exchange"]: row for row in rows}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
