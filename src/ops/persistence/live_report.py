import asyncio
from datetime import datetime, timezone
from typing import Any

from .balance_report import build_balance_report
from .sync import build_exchange


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


async def fetch_recent_trades(exchange: Any, symbols: list[str], trade_limit: int) -> dict[str, list[dict[str, Any]]]:
    return {
        symbol: (await exchange.get_trades(symbol, limit=trade_limit) or [])[:trade_limit]
        for symbol in symbols
    }


def build_exchange_report_from_state(
    exchange_name: str,
    exchange: Any,
    symbols: list[str],
    *,
    recent_trades: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    account_snapshot = exchange.get_account_snapshot()
    symbol_snapshots = {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
    balance_report = build_balance_report(exchange_name, account_snapshot, symbol_snapshots)

    positions = {
        symbol: snapshot.get("position", {})
        for symbol, snapshot in symbol_snapshots.items()
        if abs(float(snapshot.get("position", {}).get("qty", 0.0) or 0.0)) > 0
    }

    funding_snapshot = {
        symbol: snapshot.get("funding", {})
        for symbol, snapshot in symbol_snapshots.items()
    }
    if hasattr(exchange, "get_health_snapshot"):
        health_overview_raw = exchange.get_health_snapshot()
    else:
        health_overview_raw = {
            "exchange": exchange_name,
            "rate_limit_state": {},
            "symbols": {
                symbol: snapshot.get("health", {})
                for symbol, snapshot in symbol_snapshots.items()
            },
        }
    health_snapshot = {
        symbol: (health_overview_raw.get("symbols", {}) or {}).get(symbol, symbol_snapshots[symbol].get("health", {}))
        for symbol in symbols
    }
    health_overview = {
        **health_overview_raw,
        "symbols": health_snapshot,
    }
    if hasattr(exchange, "get_reconcile_snapshot"):
        reconcile_status = exchange.get_reconcile_snapshot()
    else:
        reconcile_status = account_snapshot.get("reconcile_status", {})

    open_orders_all = exchange.get_open_order_map()
    open_orders = {
        symbol: open_orders_all.get(symbol, [])
        for symbol in symbols
        if open_orders_all.get(symbol)
    }

    return {
        "exchange": exchange_name,
        "symbols": symbols,
        "generated_at": utc_now_iso(),
        "current_balance": account_snapshot,
        "balance_report": balance_report,
        "current_positions": positions,
        "open_orders": open_orders,
        "recent_trades": recent_trades or {symbol: [] for symbol in symbols},
        "funding_snapshot": funding_snapshot,
        "health_overview": health_overview,
        "health_snapshot": health_snapshot,
        "reconcile_status": reconcile_status,
    }


async def collect_exchange_report(exchange_name: str, symbols: list[str], wait_ws_sec: float, trade_limit: int) -> dict[str, Any]:
    exchange = build_exchange(exchange_name, symbols)

    try:
        await exchange.run()
        await asyncio.sleep(wait_ws_sec)
        recent_trades = await fetch_recent_trades(exchange, symbols, trade_limit)
        return build_exchange_report_from_state(
            exchange_name,
            exchange,
            symbols,
            recent_trades=recent_trades,
        )
    finally:
        await exchange.stop()


async def collect_exchange_report_safe(exchange_name: str, symbols: list[str], wait_ws_sec: float, trade_limit: int) -> dict[str, Any]:
    try:
        return await collect_exchange_report(exchange_name, symbols, wait_ws_sec, trade_limit)
    except Exception as exc:
        return {
            "exchange": exchange_name,
            "symbols": symbols,
            "generated_at": utc_now_iso(),
            "error": str(exc),
        }


async def collect_reports(exchange_names: list[str], symbols: list[str], wait_ws_sec: float, trade_limit: int) -> list[dict[str, Any]]:
    tasks = [
        collect_exchange_report_safe(exchange_name, symbols, wait_ws_sec, trade_limit)
        for exchange_name in exchange_names
    ]
    return await asyncio.gather(*tasks)
