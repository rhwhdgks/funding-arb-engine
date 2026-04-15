from __future__ import annotations

from typing import Any

from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.db import (
    current_utc_ms,
    get_db_connection,
    insert_balance_snapshot,
    insert_position_snapshots,
    upsert_funding_history,
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
            funding_result: dict[str, int] = {}
            for symbol in normalized_symbols:
                try:
                    history = await exchange.preload_funding_history(symbol, limit=funding_limit)
                    inserted = upsert_funding_history(conn, normalized_exchange, symbol, history or [])
                    funding_result[symbol] = inserted
                except Exception as exc:
                    funding_result[symbol] = -1
                    funding_result[f"{symbol}__error"] = str(exc)
            summary["funding"] = funding_result

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

            if persist_balance:
                inserted = insert_balance_snapshot(
                    conn,
                    normalized_exchange,
                    normalized_symbols,
                    balance_report,
                    snapshot_time_ms=snapshot_time_ms,
                )
                summary["balance"] = {
                    "inserted": inserted,
                    "snapshot_time_ms": snapshot_time_ms,
                    "actual_equity_usd": balance_report.get("actual_equity_usd"),
                    "theoretical_equity_usd": balance_report.get("theoretical_equity_usd"),
                    "gross_notional_usd": balance_report.get("gross_notional_usd"),
                    "position_count": len(balance_report.get("positions") or []),
                }

            if persist_positions:
                inserted = insert_position_snapshots(
                    conn,
                    normalized_exchange,
                    normalized_symbols,
                    balance_report,
                    snapshot_time_ms=snapshot_time_ms,
                )
                summary["positions"] = {
                    "inserted": inserted,
                    "snapshot_time_ms": snapshot_time_ms,
                    "position_count": len(balance_report.get("positions") or []),
                    "symbols_with_positions": [
                        row.get("ticker") or row.get("symbol")
                        for row in (balance_report.get("positions") or [])
                    ],
                }

        return summary
    finally:
        conn.close()