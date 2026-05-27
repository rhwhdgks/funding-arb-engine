from __future__ import annotations

from dataclasses import dataclass

from src.strategy.models import OpenPairState, StrategyConfig


AUTO_CLOSE_DECISIONS = frozenset({"stop_loss", "take_profit", "max_hold", "close_candidate"})


@dataclass(frozen=True)
class ClosePolicyEvaluation:
    decision: str
    hold_pnl_usd: float
    entry_age_sec: float
    reference_notional_usd: float
    close_pnl_usd: float
    close_edge_usd: float
    thresholds: dict[str, float]

    @property
    def should_auto_close(self) -> bool:
        return self.decision in AUTO_CLOSE_DECISIONS


def pair_entry_reference_notional_usd(pair: OpenPairState) -> float:
    reference_price = (float(pair.long_entry_price or 0.0) + float(pair.short_entry_price or 0.0)) / 2.0
    return max(float(pair.qty or 0.0) * reference_price, 0.0)


def close_fee_cost_usd(
    *,
    long_exchange: str,
    short_exchange: str,
    long_notional_usd: float,
    short_notional_usd: float,
    config: StrategyConfig,
) -> float:
    """Return expected close fees using the configured close execution mode."""
    long_notional = max(float(long_notional_usd or 0.0), 0.0)
    short_notional = max(float(short_notional_usd or 0.0), 0.0)

    if config.close_first_leg_role == "long":
        first_exchange, first_notional = long_exchange, long_notional
        hedge_exchange, hedge_notional = short_exchange, short_notional
    else:
        first_exchange, first_notional = short_exchange, short_notional
        hedge_exchange, hedge_notional = long_exchange, long_notional

    if config.close_execution_mode == "maker_then_taker":
        first_fee_bps = config.close_fee_bps.get(first_exchange, 0.0)
        hedge_fee_bps = config.taker_fee_bps.get(hedge_exchange, 0.0)
    else:
        first_fee_bps = config.taker_fee_bps.get(first_exchange, 0.0)
        hedge_fee_bps = config.taker_fee_bps.get(hedge_exchange, 0.0)

    return (
        first_notional * float(first_fee_bps or 0.0) / 10000.0
        + hedge_notional * float(hedge_fee_bps or 0.0) / 10000.0
    )


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
    *,
    hold_pnl_usd: float,
    entry_age_sec: float,
    close_pnl_usd: float,
    close_edge_usd: float,
    reference_notional_usd: float | None = None,
    action_inputs_fresh: bool = True,
) -> str:
    if not action_inputs_fresh:
        return "await_account_sync"

    thresholds = close_thresholds_usd(pair, config, reference_notional_usd)
    if entry_age_sec < config.min_hold_sec:
        return "hold_grace"
    if close_pnl_usd <= thresholds["stop_loss_usd"]:
        return "stop_loss"
    if close_pnl_usd >= thresholds["take_profit_usd"]:
        return "take_profit"
    if entry_age_sec >= config.max_hold_sec:
        return "max_hold"
    if hold_pnl_usd > thresholds["hold_buffer_usd"]:
        return "hold"
    if close_edge_usd > 0:
        return "hold_spread_support"
    if entry_age_sec < config.close_candidate_min_age_sec:
        return "hold_close_age_gate"
    return "close_candidate"


def evaluate_close_policy(
    pair: OpenPairState,
    config: StrategyConfig,
    *,
    hold_pnl_usd: float,
    entry_age_sec: float,
    close_pnl_usd: float,
    close_edge_usd: float,
    reference_notional_usd: float | None = None,
    action_inputs_fresh: bool = True,
) -> ClosePolicyEvaluation:
    thresholds = close_thresholds_usd(pair, config, reference_notional_usd)
    decision = decide_pair_close_reason(
        pair,
        config,
        hold_pnl_usd=hold_pnl_usd,
        entry_age_sec=entry_age_sec,
        close_pnl_usd=close_pnl_usd,
        close_edge_usd=close_edge_usd,
        reference_notional_usd=thresholds["reference_notional_usd"],
        action_inputs_fresh=action_inputs_fresh,
    )
    return ClosePolicyEvaluation(
        decision=decision,
        hold_pnl_usd=hold_pnl_usd,
        entry_age_sec=entry_age_sec,
        reference_notional_usd=thresholds["reference_notional_usd"],
        close_pnl_usd=close_pnl_usd,
        close_edge_usd=close_edge_usd,
        thresholds=thresholds,
    )
