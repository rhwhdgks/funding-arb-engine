from __future__ import annotations

from dataclasses import dataclass

from src.strategy.models import OpenPairState, StrategyConfig


AUTO_CLOSE_DECISIONS = frozenset(
    {"spread_nonpositive", "funding_cost_over_gap", "stop_loss", "take_profit", "max_hold", "close_candidate"}
)


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


@dataclass(frozen=True)
class ClosePnlBreakdown:
    gross_close_pnl_usd: float
    close_fee_cost_usd: float
    close_slippage_cost_usd: float
    net_close_pnl_usd: float


@dataclass(frozen=True)
class NormalCloseFeeGateEvaluation:
    defer: bool
    reason: str
    metrics: dict[str, float | str | int | bool]


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


def close_pnl_breakdown_usd(
    pair: OpenPairState,
    config: StrategyConfig,
    *,
    long_bid: float,
    short_ask: float,
) -> ClosePnlBreakdown:
    """Break close PnL into gross PnL, close fees/slippage, and net PnL."""
    if long_bid <= 0 or short_ask <= 0:
        return ClosePnlBreakdown(
            gross_close_pnl_usd=-1e18,
            close_fee_cost_usd=0.0,
            close_slippage_cost_usd=0.0,
            net_close_pnl_usd=-1e18,
        )

    gross_pnl = pair.qty * (
        (long_bid - short_ask) - (pair.long_entry_price - pair.short_entry_price)
    )
    long_notional = pair.qty * long_bid
    short_notional = pair.qty * short_ask
    close_fee_cost = close_fee_cost_usd(
        long_exchange=pair.long_exchange,
        short_exchange=pair.short_exchange,
        long_notional_usd=long_notional,
        short_notional_usd=short_notional,
        config=config,
    )
    close_slippage = ((long_notional + short_notional) / 2.0) * (config.close_slippage_bps * 2.0) / 10000.0
    return ClosePnlBreakdown(
        gross_close_pnl_usd=gross_pnl,
        close_fee_cost_usd=close_fee_cost,
        close_slippage_cost_usd=close_slippage,
        net_close_pnl_usd=gross_pnl - close_fee_cost - close_slippage,
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

    if close_edge_usd <= 0:
        return "spread_nonpositive"
    if hold_pnl_usd <= 0:
        return "funding_cost_over_gap"
    if entry_age_sec >= config.max_hold_sec:
        return "max_hold"
    return "hold_spread_support"


def evaluate_normal_close_fee_gate(
    *,
    config: StrategyConfig,
    decision: str,
    hold_pnl_usd: float,
    pnl: ClosePnlBreakdown,
    defer_age_sec: float,
    defer_count: int,
) -> NormalCloseFeeGateEvaluation:
    """Decide whether to defer only normal, non-risk close attempts."""
    metrics: dict[str, float | str | int | bool] = {
        "original_close_reason": decision,
        "gross_close_pnl_usd": pnl.gross_close_pnl_usd,
        "close_fee_cost_usd": pnl.close_fee_cost_usd,
        "close_slippage_cost_usd": pnl.close_slippage_cost_usd,
        "net_close_pnl_usd": pnl.net_close_pnl_usd,
        "hold_pnl_usd": hold_pnl_usd,
        "defer_count": int(defer_count),
        "defer_age_sec": max(float(defer_age_sec or 0.0), 0.0),
        "max_defer_sec": max(float(config.normal_close_fee_gate_max_defer_sec or 0.0), 0.0),
        "min_net_usd": float(config.normal_close_fee_gate_min_net_usd or 0.0),
        "enabled": bool(config.normal_close_fee_gate_enabled),
    }
    if not config.normal_close_fee_gate_enabled:
        return NormalCloseFeeGateEvaluation(False, "fee_gate_disabled", metrics)
    if decision != "spread_nonpositive":
        return NormalCloseFeeGateEvaluation(False, "reason_not_fee_gate_eligible", metrics)
    if hold_pnl_usd <= 0:
        return NormalCloseFeeGateEvaluation(False, "hold_pnl_not_positive", metrics)
    if pnl.net_close_pnl_usd >= float(config.normal_close_fee_gate_min_net_usd or 0.0):
        return NormalCloseFeeGateEvaluation(False, "close_net_above_threshold", metrics)
    max_defer_sec = max(float(config.normal_close_fee_gate_max_defer_sec or 0.0), 0.0)
    if max_defer_sec <= 0 or defer_age_sec >= max_defer_sec:
        return NormalCloseFeeGateEvaluation(False, "max_defer_elapsed", metrics)
    return NormalCloseFeeGateEvaluation(True, "normal_close_deferred_by_fee", metrics)


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
