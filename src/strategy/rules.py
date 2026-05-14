from src.strategy.models import OpenPairState, PairCandidate, StrategyConfig


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


def pair_entry_reference_notional_usd(pair: OpenPairState) -> float:
    reference_price = (float(pair.long_entry_price or 0.0) + float(pair.short_entry_price or 0.0)) / 2.0
    return max(float(pair.qty or 0.0) * reference_price, 0.0)


def close_thresholds_usd(
    pair: OpenPairState,
    config: StrategyConfig,
    reference_notional_usd: float | None = None,
) -> dict[str, float]:
    reference_notional = float(reference_notional_usd or 0.0)
    if reference_notional <= 0:
        reference_notional = pair_entry_reference_notional_usd(pair)

    take_profit_usd = float(config.take_profit_usd)
    if config.take_profit_bps is not None and reference_notional > 0:
        take_profit_usd = reference_notional * float(config.take_profit_bps) / 10000.0

    stop_loss_usd = float(config.stop_loss_usd)
    if config.stop_loss_bps is not None and reference_notional > 0:
        stop_loss_usd = reference_notional * float(config.stop_loss_bps) / 10000.0

    hold_buffer_usd = float(config.hold_buffer_usd)
    if config.hold_buffer_bps is not None and reference_notional > 0:
        hold_buffer_usd = reference_notional * float(config.hold_buffer_bps) / 10000.0

    return {
        "reference_notional_usd": reference_notional,
        "take_profit_usd": take_profit_usd,
        "stop_loss_usd": stop_loss_usd,
        "hold_buffer_usd": hold_buffer_usd,
    }


def decide_pair_close_reason(
    pair: OpenPairState,
    config: StrategyConfig,
    hold_pnl: float,
    entry_age_sec: float,
    reference_notional_usd: float | None = None,
) -> str:
    thresholds = close_thresholds_usd(pair, config, reference_notional_usd)
    if entry_age_sec < config.min_hold_sec:
        return "hold_grace"
    if pair.last_close_pnl_usd <= thresholds["stop_loss_usd"]:
        return "stop_loss"
    if pair.last_close_pnl_usd >= thresholds["take_profit_usd"]:
        return "take_profit"
    if entry_age_sec >= config.max_hold_sec:
        return "max_hold"
    if hold_pnl > thresholds["hold_buffer_usd"]:
        return "hold"
    if pair.last_close_edge_usd > 0:
        return "hold_spread_support"
    if entry_age_sec < config.close_candidate_min_age_sec:
        return "hold_close_age_gate"
    return "close_candidate"
