from __future__ import annotations

import asyncio
from typing import Any

from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.db import current_utc_ms, db_persistence_enabled, get_db_connection
from src.ops.persistence.trading_dev_normalizers import (
    build_balance_snapshot_row,
    build_funding_rate_rows,
)
from src.ops.persistence.trading_dev_store import (
    upsert_balance_snapshot_rows,
    upsert_funding_rate_rows,
)


async def persist_exchange_snapshots(
    exchange_name: str,
    exchange: Any,
    symbols: list[str],
    *,
    funding_limit: int = 30,
    persist_funding: bool = True,
    persist_balance: bool = True,
    persist_positions: bool = True,
) -> dict[str, Any]:
    """
    Persist funding history / balance snapshots / position snapshots
    using an already-running exchange instance.

    This avoids creating a second exchange instance and avoids extra WS/runtime duplication.
    """
    normalized_exchange = str(exchange_name).strip().lower()
    normalized_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]

    if not db_persistence_enabled():
        return {
            "exchange": normalized_exchange,
            "funding": {},
            "balance": {},
            "positions": {},
            "skipped": "db_disabled",
        }

    if not normalized_symbols:
        return {
            "exchange": normalized_exchange,
            "funding": {},
            "balance": {},
            "positions": {},
            "skipped": "no_symbols",
        }

    conn = get_db_connection()
    try:
        summary: dict[str, Any] = {
            "exchange": normalized_exchange,
            "funding": {},
            "balance": {},
            "positions": {},
        }

        if persist_funding:
            summary["funding"] = {}

        if persist_balance or persist_positions:
            snapshot_time_ms = current_utc_ms()
            account_snapshot = exchange.get_account_snapshot()
            symbol_snapshots = {
                symbol: exchange.get_symbol_snapshot(symbol)
                for symbol in normalized_symbols
            }
            balance_report = build_balance_report(
                normalized_exchange,
                account_snapshot,
                symbol_snapshots,
            )

            if persist_funding:
                funding_rows = build_funding_rate_rows(
                    normalized_exchange,
                    symbol_snapshots,
                    snapshot_time_ms=snapshot_time_ms,
                )
                inserted = await asyncio.to_thread(upsert_funding_rate_rows, conn, funding_rows)
                summary["funding"] = {
                    "inserted": inserted,
                    "snapshot_time_ms": snapshot_time_ms,
                    "symbol_count": len(funding_rows),
                }

            if persist_balance:
                balance_row = build_balance_snapshot_row(
                    normalized_exchange,
                    normalized_symbols,
                    balance_report,
                    snapshot_time_ms=snapshot_time_ms,
                )
                inserted = await asyncio.to_thread(upsert_balance_snapshot_rows, conn, [balance_row])
                summary["balance"] = {
                    "inserted": inserted,
                    "snapshot_time_ms": snapshot_time_ms,
                    "actual_equity_usd": balance_report.get("actual_equity_usd"),
                    "theoretical_equity_usd": balance_report.get("theoretical_equity_usd"),
                    "gross_notional_usd": balance_report.get("gross_notional_usd"),
                    "position_count": len(balance_report.get("positions") or []),
                }

            if persist_positions:
                summary["positions"] = {
                    "inserted": len(balance_report.get("positions") or []),
                    "snapshot_time_ms": snapshot_time_ms,
                    "position_count": len(balance_report.get("positions") or []),
                    "symbols_with_positions": [
                        row.get("ticker") or row.get("symbol")
                        for row in (balance_report.get("positions") or [])
                    ],
                    "embedded_in_balance_snapshot": bool(persist_balance),
                }

        return summary
    finally:
        conn.close()
