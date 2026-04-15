from typing import Any


PRIMARY_BALANCE_ASSET_CODES = ("USD", "USDT", "USDC", "BUSD")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _pick_balance_asset(account_snapshot: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    asset_dict = account_snapshot.get("asset_dict") or {}
    if not isinstance(asset_dict, dict):
        return "USD", {}

    for asset_code in PRIMARY_BALANCE_ASSET_CODES:
        asset = asset_dict.get(asset_code)
        if isinstance(asset, dict):
            return asset_code, asset

    best_code = "USD"
    best_asset: dict[str, Any] = {}
    best_balance = -1.0
    for asset_code, asset in asset_dict.items():
        if not isinstance(asset, dict):
            continue
        balance_abs = abs(_safe_float(asset.get("balance")))
        if balance_abs > best_balance:
            best_balance = balance_abs
            best_code = str(asset_code).upper()
            best_asset = asset
    return best_code, best_asset


def _resolve_mark_price(snapshot: dict[str, Any], qty: float, entry_price: float) -> tuple[float, str]:
    position = snapshot.get("position") or {}
    funding = snapshot.get("funding") or {}
    orderbook = snapshot.get("orderbook") or {}
    tick = snapshot.get("tick") or {}

    direct_mark = _safe_float(position.get("mark_price"))
    if direct_mark > 0:
        return direct_mark, "position.mark_price"

    direct_mark = _safe_float(position.get("markPrice"))
    if direct_mark > 0:
        return direct_mark, "position.markPrice"

    funding_mark = _safe_float(funding.get("mark_price"))
    if funding_mark > 0:
        return funding_mark, "funding.mark_price"

    funding_index = _safe_float(funding.get("index_price"))
    if funding_index > 0:
        return funding_index, "funding.index_price"

    bid = _safe_float(orderbook.get("bid"))
    ask = _safe_float(orderbook.get("ask"))
    if qty > 0 and bid > 0:
        return bid, "orderbook.bid"
    if qty < 0 and ask > 0:
        return ask, "orderbook.ask"
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0, "orderbook.mid"

    for key in ("last_price", "lastPrice", "last", "price", "close"):
        tick_price = _safe_float(tick.get(key))
        if tick_price > 0:
            return tick_price, f"tick.{key}"

    if entry_price > 0:
        return entry_price, "entry_price"

    return 0.0, "-"


def build_balance_report(
    exchange_name: str,
    account_snapshot: dict[str, Any],
    symbol_snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    balance_asset_code, balance_asset = _pick_balance_asset(account_snapshot)
    wallet_balance = _safe_float(
        balance_asset.get("balance"),
        _safe_float(account_snapshot.get("total_balance")),
    )
    available_balance = _safe_float(
        balance_asset.get("availableBalance", balance_asset.get("available")),
        wallet_balance,
    )
    locked_balance = _safe_float(
        balance_asset.get("locked"),
        max(wallet_balance - available_balance, 0.0),
    )
    margin_balance = _safe_float(
        balance_asset.get("marginBalance"),
        wallet_balance,
    )
    exchange_unrealized_profit = _safe_float(
        balance_asset.get("unrealizedProfit", balance_asset.get("unrealized_profit")),
    )
    actual_equity = wallet_balance + exchange_unrealized_profit
    total_balance = _safe_float(account_snapshot.get("total_balance"), actual_equity)

    long_notional = 0.0
    short_notional = 0.0
    theoretical_unrealized_profit = 0.0
    positions: list[dict[str, Any]] = []

    for symbol in sorted(symbol_snapshots):
        snapshot = symbol_snapshots[symbol] or {}
        position = snapshot.get("position") or {}
        qty = _safe_float(position.get("qty", position.get("balance")))
        if abs(qty) <= 0:
            continue

        entry_price = _safe_float(position.get("entry_price", position.get("avg_price", position.get("avgPrice"))))
        exchange_position_upl = _safe_float(
            position.get("unrealized_profit", position.get("unrealizedProfit")),
        )
        mark_price, mark_source = _resolve_mark_price(snapshot, qty, entry_price)
        notional_mark = abs(qty) * mark_price

        if qty > 0:
            long_notional += notional_mark
        else:
            short_notional += notional_mark

        if entry_price > 0 and mark_price > 0:
            theoretical_position_upl = qty * (mark_price - entry_price)
            theoretical_upl_source = "recomputed"
        else:
            theoretical_position_upl = exchange_position_upl
            theoretical_upl_source = "exchange_fallback"

        theoretical_unrealized_profit += theoretical_position_upl
        positions.append(
            {
                "ticker": symbol,
                "qty": qty,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "mark_source": mark_source,
                "position_side": position.get("position_side", "-"),
                "notional_mark_usd": notional_mark,
                "exchange_unrealized_profit_usd": exchange_position_upl,
                "theoretical_unrealized_profit_usd": theoretical_position_upl,
                "unrealized_profit_gap_usd": theoretical_position_upl - exchange_position_upl,
                "theoretical_upl_source": theoretical_upl_source,
                "timestamp": position.get("timestamp"),
            }
        )

    gross_notional = long_notional + short_notional
    net_notional = long_notional - short_notional
    leverage = _safe_float(account_snapshot.get("leverage"))
    delta_leverage = _safe_float(account_snapshot.get("delta_leverage"))
    theoretical_equity = wallet_balance + theoretical_unrealized_profit

    return {
        "exchange": exchange_name,
        "balance_asset_code": balance_asset_code,
        "wallet_balance_usd": wallet_balance,
        "available_balance_usd": available_balance,
        "locked_balance_usd": locked_balance,
        "margin_balance_usd": margin_balance,
        "exchange_unrealized_profit_usd": exchange_unrealized_profit,
        "theoretical_unrealized_profit_usd": theoretical_unrealized_profit,
        "actual_equity_usd": actual_equity,
        "theoretical_equity_usd": theoretical_equity,
        "theoretical_equity_gap_usd": theoretical_equity - actual_equity,
        "total_balance_usd": total_balance,
        "long_notional_usd": long_notional,
        "short_notional_usd": short_notional,
        "gross_notional_usd": gross_notional,
        "net_notional_usd": net_notional,
        "leverage": leverage,
        "delta_leverage": delta_leverage,
        "positions": positions,
        "asset_dict": account_snapshot.get("asset_dict", {}),
    }
