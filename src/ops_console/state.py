from __future__ import annotations

import time
from typing import Any

from src.ops.persistence.balance_report import build_balance_report


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper().replace("USDT", "")


def normalize_order_side(value: str) -> str:
    side = str(value or "").strip().lower()
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported side: {value}")
    return side


def normalize_levels(raw_levels: Any, fallback_price: Any, fallback_qty: Any) -> list[list[float]]:
    levels: list[list[float]] = []
    if isinstance(raw_levels, list):
        for row in raw_levels[:5]:
            if isinstance(row, dict):
                price = safe_float(row.get("price"), 0.0)
                qty = safe_float(row.get("qty", row.get("amount")), 0.0)
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                price = safe_float(row[0], 0.0)
                qty = safe_float(row[1], 0.0)
            else:
                continue
            if price > 0:
                levels.append([price, max(qty, 0.0)])

    if not levels:
        price = safe_float(fallback_price, 0.0)
        qty = safe_float(fallback_qty, 0.0)
        if price > 0:
            levels.append([price, max(qty, 0.0)])
    return levels[:5]


def build_orderbook_state(snapshot: dict[str, Any], *, stale_ms: int) -> dict[str, Any]:
    orderbook = snapshot.get("orderbook") or {}
    tick = snapshot.get("tick") or {}
    source = orderbook.get("source") or tick.get("source") or "-"

    bids = normalize_levels(
        orderbook.get("bids"),
        orderbook.get("best_bid", orderbook.get("bid")),
        orderbook.get("bid_qty_top"),
    )
    asks = normalize_levels(
        orderbook.get("asks"),
        orderbook.get("best_ask", orderbook.get("ask")),
        orderbook.get("ask_qty_top"),
    )
    best_bid = bids[0][0] if bids else safe_float(orderbook.get("best_bid", orderbook.get("bid")), 0.0)
    best_ask = asks[0][0] if asks else safe_float(orderbook.get("best_ask", orderbook.get("ask")), 0.0)

    update_ms = int(safe_float(orderbook.get("desktop_time") or tick.get("desktop_time"), 0.0))
    server_time_ms = int(safe_float(orderbook.get("server_time") or tick.get("server_time"), 0.0))
    age_ms = max(now_ms() - update_ms, 0) if update_ms > 0 else None
    spread = max(best_ask - best_bid, 0.0) if best_bid > 0 and best_ask > 0 else 0.0
    spread_bps = (spread / best_ask * 10000.0) if best_ask > 0 else 0.0
    stale = bool(age_ms is None or age_ms > stale_ms or best_bid <= 0 or best_ask <= 0)

    return {
        "source": source,
        "bids": bids,
        "asks": asks,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0,
        "spread": spread,
        "spread_bps": spread_bps,
        "updated_ms": update_ms,
        "server_time_ms": server_time_ms,
        "age_ms": age_ms,
        "stale": stale,
    }


def build_symbol_state(exchange_name: str, symbol: str, snapshot: dict[str, Any], *, stale_ms: int) -> dict[str, Any]:
    orderbook = build_orderbook_state(snapshot, stale_ms=stale_ms)
    position = snapshot.get("position") or {}
    health = snapshot.get("health") or {}
    funding = snapshot.get("funding") or {}
    open_orders = snapshot.get("open_orders") or []
    account_last_sync_ms = int(safe_float(snapshot.get("account_last_sync_ms"), 0.0))

    market_ready = bool(snapshot.get("market_ready", health.get("market_ready", False)))
    account_ready = bool(snapshot.get("account_ready", health.get("account_ready", False)))
    block_reasons = list(health.get("block_reasons") or health.get("market_block_reasons") or [])
    stale = bool(orderbook["stale"] or not market_ready)

    return {
        "exchange": exchange_name,
        "symbol": normalize_symbol(symbol),
        "connected": market_ready or bool(orderbook["updated_ms"]),
        "market_ready": market_ready,
        "account_ready": account_ready,
        "stale": stale,
        "block_reasons": block_reasons,
        "account_last_sync_ms": account_last_sync_ms,
        "orderbook": orderbook,
        "position": {
            "qty": safe_float(position.get("qty", position.get("balance")), 0.0),
            "entry_price": safe_float(position.get("entry_price", position.get("avg_price", position.get("avgPrice"))), 0.0),
            "unrealized_profit": safe_float(position.get("unrealized_profit", position.get("unrealizedProfit")), 0.0),
            "raw": position,
        },
        "funding": {
            "rate": safe_float(funding.get("funding_rate"), 0.0),
            "next_funding_time_ms": int(safe_float(funding.get("next_funding_time"), 0.0)),
            "mark_price": safe_float(funding.get("mark_price"), 0.0),
        },
        "open_orders": open_orders if isinstance(open_orders, list) else [],
        "health": health,
    }


def build_exchange_state(exchange_name: str, exchange: Any, symbols: list[str], *, stale_ms: int) -> dict[str, Any]:
    account_snapshot = exchange.get_account_snapshot()
    symbol_snapshots = {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
    symbol_states = {
        symbol: build_symbol_state(exchange_name, symbol, snapshot, stale_ms=stale_ms)
        for symbol, snapshot in symbol_snapshots.items()
    }
    balance_report = build_balance_report(exchange_name, account_snapshot, symbol_snapshots)
    return {
        "exchange": exchange_name,
        "account": account_snapshot,
        "balance_report": balance_report,
        "symbols": symbol_states,
    }
