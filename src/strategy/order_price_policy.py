from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

from src.strategy.candidates import extract_best_ask, extract_best_bid
from src.strategy.models import PairCandidate, StrategyConfig
from src.strategy.rules import required_entry_open_edge_usd


EPS = 1e-9


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default

def _floor_by_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))


def _ceil_by_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_UP) * Decimal(str(step)))


def normalize_limit_price_for_side(exchange: Any, symbol: str, side: str, raw_price: float) -> float:
    if raw_price <= 0:
        return 0.0
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    tick_size = _safe_float(meta.get("tick_size"), 0.0)
    if tick_size <= 0:
        return raw_price
    if side.lower() == "buy":
        return _floor_by_step(raw_price, tick_size)
    return _ceil_by_step(raw_price, tick_size)


def marketable_limit_price(exchange: Any, symbol: str, side: str) -> float:
    snapshot = exchange.get_symbol_snapshot(symbol)
    if side.lower() == "buy":
        return extract_best_ask(snapshot)
    return extract_best_bid(snapshot)


def marketable_limit_price_with_bps(exchange: Any, symbol: str, side: str, bps: float) -> float:
    base_price = marketable_limit_price(exchange, symbol, side)
    if base_price <= 0:
        return 0.0
    if side.lower() == "buy":
        return base_price * (1.0 + (bps / 10000.0))
    return base_price * (1.0 - (bps / 10000.0))


def maker_close_limit_price(exchange: Any, symbol: str, side: str, bps: float = 0.0) -> float:
    snapshot = exchange.get_symbol_snapshot(symbol)
    bps = max(float(bps or 0.0), 0.0)
    if side.lower() == "buy":
        best_bid = extract_best_bid(snapshot)
        if best_bid <= 0:
            return 0.0
        return normalize_limit_price_for_side(exchange, symbol, side, best_bid * (1.0 - bps / 10000.0))

    best_ask = extract_best_ask(snapshot)
    if best_ask <= 0:
        return 0.0
    return normalize_limit_price_for_side(exchange, symbol, side, best_ask * (1.0 + bps / 10000.0))


def marketable_entry_target_price(exchange: Any, symbol: str, side: str, config: StrategyConfig) -> float:
    base_price = marketable_limit_price(exchange, symbol, side)
    if base_price <= 0:
        return 0.0
    bps = max(config.entry_first_leg_marketable_bps, 0.0)
    if side.lower() == "buy":
        raw_price = base_price * (1.0 + (bps / 10000.0))
    else:
        raw_price = base_price * (1.0 - (bps / 10000.0))
    return normalize_limit_price_for_side(exchange, symbol, side, raw_price)


def spread_buffer_entry_target_price(
    candidate: PairCandidate,
    first_exchange_name: str,
    first_side: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    symbol = candidate.symbol
    first_exchange = exchanges[first_exchange_name]
    first_snapshot = first_exchange.get_symbol_snapshot(symbol)
    # Question4-style maker quote: opportunity threshold is checked elsewhere;
    # this price only decides where the maker order rests.
    target_bps = max(config.entry_spread_bps, 0.0) + (max(config.entry_cancel_buffer_bps, 0.0) / 2.0)

    if first_exchange_name == candidate.long_exchange and first_side.lower() == "buy":
        reference_snapshot = exchanges[candidate.short_exchange].get_symbol_snapshot(symbol)
        reference_price = extract_best_bid(reference_snapshot)
        maker_best_bid = extract_best_bid(first_snapshot)
        if reference_price <= 0:
            return 0.0
        raw_target = reference_price * (1.0 - (target_bps / 10000.0))
        target_price = min(raw_target, maker_best_bid) if maker_best_bid > 0 else raw_target
        return normalize_limit_price_for_side(first_exchange, symbol, first_side, target_price)

    if first_exchange_name == candidate.short_exchange and first_side.lower() == "sell":
        reference_snapshot = exchanges[candidate.long_exchange].get_symbol_snapshot(symbol)
        reference_price = extract_best_ask(reference_snapshot)
        maker_best_ask = extract_best_ask(first_snapshot)
        if reference_price <= 0:
            return 0.0
        raw_target = reference_price * (1.0 + (target_bps / 10000.0))
        target_price = max(raw_target, maker_best_ask) if maker_best_ask > 0 else raw_target
        return normalize_limit_price_for_side(first_exchange, symbol, first_side, target_price)

    return marketable_entry_target_price(first_exchange, symbol, first_side, config)


def passive_entry_target_price(
    candidate: PairCandidate,
    first_exchange_name: str,
    first_side: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    symbol = candidate.symbol
    first_exchange = exchanges[first_exchange_name]
    if config.entry_first_leg_price_mode == "marketable":
        return marketable_entry_target_price(first_exchange, symbol, first_side, config)
    if config.entry_first_leg_price_mode == "spread_buffer":
        return spread_buffer_entry_target_price(candidate, first_exchange_name, first_side, exchanges, config)

    first_snapshot = first_exchange.get_symbol_snapshot(symbol)
    quote_qty = candidate.executable_qty if candidate.executable_qty > EPS else candidate.target_qty
    if quote_qty <= EPS:
        return 0.0

    required_unit_edge = required_entry_open_edge_usd(candidate, config) / quote_qty

    if first_exchange_name == candidate.long_exchange and first_side.lower() == "buy":
        reference_snapshot = exchanges[candidate.short_exchange].get_symbol_snapshot(symbol)
        reference_price = extract_best_bid(reference_snapshot)
        maker_best_bid = extract_best_bid(first_snapshot)
        if reference_price <= 0:
            return 0.0
        raw_target = reference_price - required_unit_edge
        passive_cap = maker_best_bid if maker_best_bid > 0 else raw_target
        target_price = min(raw_target, passive_cap)
        return normalize_limit_price_for_side(first_exchange, symbol, first_side, target_price)

    if first_exchange_name == candidate.short_exchange and first_side.lower() == "sell":
        reference_snapshot = exchanges[candidate.long_exchange].get_symbol_snapshot(symbol)
        reference_price = extract_best_ask(reference_snapshot)
        maker_best_ask = extract_best_ask(first_snapshot)
        if reference_price <= 0:
            return 0.0
        raw_target = reference_price + required_unit_edge
        passive_floor = maker_best_ask if maker_best_ask > 0 else raw_target
        target_price = max(raw_target, passive_floor)
        return normalize_limit_price_for_side(first_exchange, symbol, first_side, target_price)

    return normalize_limit_price_for_side(
        first_exchange,
        symbol,
        first_side,
        marketable_limit_price(first_exchange, symbol, first_side),
    )
