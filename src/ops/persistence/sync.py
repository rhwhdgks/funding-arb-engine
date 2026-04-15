import asyncio
import os
from typing import Any

from src.core.exchange_registry import SUPPORTED_EXCHANGES, build_exchange, load_exchange_class

from .balance_report import build_balance_report
from .db import (
    insert_balance_snapshot,
    insert_position_snapshots,
    current_utc_ms,
    ensure_schema,
    fetch_open_order_history_rows,
    get_db_connection,
    insert_order_events,
    order_uid,
    upsert_funding_history,
    upsert_order_history_snapshot,
    build_order_event_payload_from_history_row,
)


def parse_csv_list(raw: str | None, fallback: list[str] | None = None) -> list[str]:
    if not raw:
        return list(fallback or [])
    return [item.strip().upper() for item in raw.split(",") if item.strip()]

def build_default_symbols() -> list[str]:
    symbols = parse_csv_list(os.getenv("SYMBOLS"))
    if symbols:
        return symbols
    symbol = os.getenv("SYMBOL", "BTC").strip().upper()
    return [symbol]


async def sync_trades_to_db(conn, exchange_name: str, symbols: list[str], limit: int = 100) -> dict[str, int]:
    Exchange = load_exchange_class(exchange_name)
    exchange = Exchange(ticker_list=symbols)
    result: dict[str, int] = {}

    for symbol in symbols:
        trades = await exchange.get_trades(symbol, limit=limit)
        inserted = upsert_trade_history(conn, exchange_name, symbol, trades or [])
        result[symbol] = inserted

    return result


async def sync_funding_to_db(conn, exchange_name: str, symbols: list[str], limit: int = 30) -> dict[str, int]:
    Exchange = load_exchange_class(exchange_name)
    exchange = Exchange(ticker_list=symbols)
    result: dict[str, int] = {}

    for symbol in symbols:
        history = await exchange.preload_funding_history(symbol, limit=limit)
        inserted = upsert_funding_history(conn, exchange_name, symbol, history or [])
        result[symbol] = inserted

    return result


async def sync_order_history_to_db(
    conn,
    exchange_name: str,
    symbols: list[str],
    *,
    wait_ws_sec: float = 6.0,
) -> dict[str, dict[str, int]]:
    summary = await sync_live_state_to_db(
        conn,
        exchange_name,
        symbols,
        wait_ws_sec=wait_ws_sec,
        sync_orders=True,
        sync_balance=False,
        sync_positions=False,
    )
    return summary["sync_orders"]


async def sync_live_state_to_db(
    conn,
    exchange_name: str,
    symbols: list[str],
    *,
    wait_ws_sec: float = 6.0,
    sync_orders: bool = True,
    sync_balance: bool = True,
    sync_positions: bool = True,
) -> dict[str, Any]:
    exchange = build_exchange(exchange_name, symbols)

    try:
        await exchange.run()
        await asyncio.sleep(wait_ws_sec)

        result: dict[str, Any] = {
            "sync_orders": {},
            "sync_balance": {},
            "sync_positions": {},
        }
        snapshot_time_ms: int | None = None
        balance_report: dict[str, Any] | None = None

        if sync_orders:
            order_map = exchange.get_open_order_map()
            order_result: dict[str, dict[str, int]] = {}
            seen_ms = current_utc_ms()
            for symbol in symbols:
                normalized_symbol = symbol.upper()
                symbol_orders = order_map.get(normalized_symbol, [])
                previous_open_rows = fetch_open_order_history_rows(conn, exchange_name, normalized_symbol)
                previous_open_uids = {row["order_uid"]: row for row in previous_open_rows}
                current_uids = {
                    uid: order
                    for uid, order in ((order_uid(order), order) for order in symbol_orders)
                    if uid is not None
                }

                order_events: list[dict[str, Any]] = []
                for order in symbol_orders:
                    payload = dict(order)
                    payload["event_reason"] = "reconcile_seen"
                    payload["reconcile"] = {
                        "source": "sync",
                        "seen_ms": seen_ms,
                        "symbol_open_count": len(symbol_orders),
                    }
                    order_events.append(
                        {
                            "event_type": "reconcile_seen",
                            "event_source": "sync",
                            "event_time_ms": seen_ms,
                            "payload": payload,
                        }
                    )

                for missing_uid, previous_row in previous_open_uids.items():
                    if missing_uid in current_uids:
                        continue
                    payload = build_order_event_payload_from_history_row(previous_row)
                    payload["event_reason"] = "reconcile_missing"
                    payload["reconcile"] = {
                        "source": "sync",
                        "seen_ms": seen_ms,
                        "missing_order_uid": missing_uid,
                        "symbol_open_count": len(symbol_orders),
                    }
                    order_events.append(
                        {
                            "event_type": "reconcile_missing",
                            "event_source": "sync",
                            "event_time_ms": seen_ms,
                            "payload": payload,
                        }
                    )

                events_appended = insert_order_events(conn, exchange_name, normalized_symbol, order_events)
                snapshot_summary = upsert_order_history_snapshot(
                    conn,
                    exchange_name=exchange_name,
                    symbol=symbol,
                    orders=symbol_orders,
                    seen_ms=seen_ms,
                )
                order_result[normalized_symbol] = {
                    **snapshot_summary,
                    "events_appended": events_appended,
                    "seen_events": len(symbol_orders),
                    "missing_events": max(len(order_events) - len(symbol_orders), 0),
                }
            result["sync_orders"] = order_result

        if sync_balance or sync_positions:
            snapshot_time_ms = current_utc_ms()
            account_snapshot = exchange.get_account_snapshot()
            symbol_snapshots = {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
            balance_report = build_balance_report(exchange_name, account_snapshot, symbol_snapshots)

        if sync_balance:
            inserted = insert_balance_snapshot(
                conn,
                exchange_name,
                symbols,
                balance_report,
                snapshot_time_ms=snapshot_time_ms,
            )
            result["sync_balance"] = {
                "inserted": inserted,
                "snapshot_time_ms": snapshot_time_ms,
                "tracked_symbols": symbols,
                "position_count": len(balance_report.get("positions") or []),
                "theoretical_equity_usd": balance_report.get("theoretical_equity_usd"),
                "actual_equity_usd": balance_report.get("actual_equity_usd"),
                "gross_notional_usd": balance_report.get("gross_notional_usd"),
            }

        if sync_positions:
            inserted = insert_position_snapshots(
                conn,
                exchange_name,
                symbols,
                balance_report or {},
                snapshot_time_ms=snapshot_time_ms,
            )
            result["sync_positions"] = {
                "inserted": inserted,
                "snapshot_time_ms": snapshot_time_ms or current_utc_ms(),
                "tracked_symbols": symbols,
                "position_count": len((balance_report or {}).get("positions") or []),
                "symbols_with_positions": [row.get("ticker") for row in ((balance_report or {}).get("positions") or [])],
            }

        return result
    finally:
        await exchange.stop()


async def sync_exchange_to_db(
    exchange_name: str,
    symbols: list[str],
    *,
    sync_trades: bool = False,
    sync_funding: bool = True,
    sync_orders: bool = True,
    sync_balance: bool = True,
    sync_positions: bool = True,
    trade_limit: int = 100,
    funding_limit: int = 30,
    wait_ws_sec: float = 6.0,
    ensure_schema_first: bool = True,
) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        if ensure_schema_first:
            ensure_schema(conn)

        summary: dict[str, Any] = {
            "exchange": exchange_name,
            "symbols": symbols,
            "sync_trades": {},
            "sync_funding": {},
            "sync_orders": {},
            "sync_balance": {},
            "sync_positions": {},
        }

        if sync_trades:
            raise RuntimeError(
                "sync_trades is disabled in sync_exchange_to_db. Use event_hooks-based trade_history only."
            )
        if sync_funding:
            summary["sync_funding"] = await sync_funding_to_db(conn, exchange_name, symbols, limit=funding_limit)
        if sync_orders or sync_balance or sync_positions:
            live_summary = await sync_live_state_to_db(
                conn,
                exchange_name,
                symbols,
                wait_ws_sec=wait_ws_sec,
                sync_orders=sync_orders,
                sync_balance=sync_balance,
                sync_positions=sync_positions,
            )
            summary["sync_orders"] = live_summary.get("sync_orders", {})
            summary["sync_balance"] = live_summary.get("sync_balance", {})
            summary["sync_positions"] = live_summary.get("sync_positions", {})

        return summary
    finally:
        conn.close()
