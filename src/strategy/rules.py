from src.strategy.models import OpenPairState, PairCandidate, StrategyConfig
from src.strategy.close_policy import (
    close_thresholds_usd,
    decide_pair_close_reason as _decide_pair_close_reason,
    pair_entry_reference_notional_usd,
)


def first_leg_plan(candidate: PairCandidate, config: StrategyConfig) -> tuple[str, str]:
    role = config.first_leg_role
    if role == "short":
        return candidate.short_exchange, "sell"
    return candidate.long_exchange, "buy"


def hedge_leg_plan(candidate: PairCandidate, config: StrategyConfig) -> tuple[str, str]:
    first_exchange, _ = first_leg_plan(candidate, config)
    if first_exchange == candidate.long_exchange:
        return candidate.short_exchange, "sell"
    return candidate.long_exchange, "buy"


def close_first_leg_plan(pair: OpenPairState, config: StrategyConfig) -> tuple[str, str]:
    if config.close_first_leg_role == "long":
        return pair.long_exchange, "sell"
    return pair.short_exchange, "buy"


def close_hedge_leg_plan(pair: OpenPairState, config: StrategyConfig) -> tuple[str, str]:
    first_exchange, _ = close_first_leg_plan(pair, config)
    if first_exchange == pair.long_exchange:
        return pair.short_exchange, "buy"
    return pair.long_exchange, "sell"


def required_entry_open_edge_usd(candidate: PairCandidate, config: StrategyConfig) -> float:
    required_net_edge_usd = max(
        config.min_expected_net_edge_usd,
        candidate.notional_usd * config.min_expected_net_edge_bps / 10000.0,
    )
    cost_stack_usd = (
        candidate.open_fee_cost_usd
        + candidate.expected_close_cost_usd
        + candidate.risk_buffer_usd
        + candidate.stale_data_penalty_usd
    )
    return max(required_net_edge_usd - candidate.effective_funding_pnl_usd + cost_stack_usd, 0.0)


def decide_pair_close_reason(
    pair: OpenPairState,
    config: StrategyConfig,
    hold_pnl: float,
    entry_age_sec: float,
    reference_notional_usd: float | None = None,
) -> str:
    """Backward-compatible wrapper around the close policy module."""
    return _decide_pair_close_reason(
        pair,
        config,
        hold_pnl_usd=hold_pnl,
        entry_age_sec=entry_age_sec,
        close_pnl_usd=pair.last_close_pnl_usd,
        close_edge_usd=pair.last_close_edge_usd,
        reference_notional_usd=reference_notional_usd,
    )
