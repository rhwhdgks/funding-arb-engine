from __future__ import annotations

from dataclasses import dataclass, field

from src.strategy.models import PairCandidate, StrategyConfig


EPS = 1e-9


@dataclass(frozen=True)
class EntryPolicyDecision:
    can_enter: bool
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PostFirstFillEdgeDecision:
    can_hedge: bool
    reason: str
    qty: float
    long_price: float
    short_price: float
    hedge_price: float
    actual_open_edge_usd: float
    actual_open_edge_bps: float
    min_open_edge_bps: float
    actual_estimated_net_edge_usd: float
    actual_estimated_net_edge_bps: float
    min_net_edge_bps: float
    reference_notional_usd: float


def evaluate_entry_policy(
    *,
    config: StrategyConfig,
    base_reasons: list[str] | None = None,
    raw_funding_pnl_usd: float,
    effective_funding_pnl_usd: float,
    expected_net_edge_usd: float,
    expected_net_edge_bps: float,
) -> EntryPolicyDecision:
    """Apply strategy-specific entry conditions.

    Safety/risk gates live outside this module. This policy only answers:
    "does this candidate satisfy the current strategy thesis?"
    """
    reasons = list(base_reasons or [])

    if (
        config.min_effective_funding_usd is not None
        and effective_funding_pnl_usd < config.min_effective_funding_usd
    ):
        reasons.append("effective_funding_below_usd_threshold")
    if config.require_funding_payment and raw_funding_pnl_usd >= -EPS:
        reasons.append("funding_payment_required")
    if expected_net_edge_usd < config.min_expected_net_edge_usd:
        reasons.append("net_edge_below_usd_threshold")
    if expected_net_edge_bps < config.min_expected_net_edge_bps:
        reasons.append("net_edge_below_bps_threshold")

    return EntryPolicyDecision(can_enter=len(reasons) == 0, reasons=reasons)


def evaluate_post_first_fill_edge(
    *,
    candidate: PairCandidate,
    config: StrategyConfig,
    first_exchange_name: str,
    first_fill_price: float,
    hedge_price: float,
    qty: float,
) -> PostFirstFillEdgeDecision:
    """Check whether the already-filled first leg is still worth hedging.

    This uses only current in-memory market data. It intentionally does not
    perform REST reads because this decision sits on the hot hedge path.
    """
    min_open_edge_bps = max(float(config.entry_post_fill_min_open_edge_bps or 0.0), 0.0)
    min_net_edge_bps = max(float(config.entry_post_fill_min_net_edge_bps or 0.0), 0.0)
    qty = max(float(qty or 0.0), 0.0)
    first_fill_price = float(first_fill_price or 0.0)
    hedge_price = float(hedge_price or 0.0)

    if qty <= EPS:
        return PostFirstFillEdgeDecision(
            can_hedge=False,
            reason="post_first_fill_qty_missing",
            qty=qty,
            long_price=0.0,
            short_price=0.0,
            hedge_price=hedge_price,
            actual_open_edge_usd=0.0,
            actual_open_edge_bps=0.0,
            min_open_edge_bps=min_open_edge_bps,
            actual_estimated_net_edge_usd=0.0,
            actual_estimated_net_edge_bps=0.0,
            min_net_edge_bps=min_net_edge_bps,
            reference_notional_usd=0.0,
        )
    if not config.entry_post_fill_edge_check_enabled:
        if first_exchange_name == candidate.long_exchange:
            long_price = first_fill_price
            short_price = hedge_price
        else:
            long_price = hedge_price
            short_price = first_fill_price
        reference_notional_usd = qty * ((long_price + short_price) / 2.0) if hedge_price > 0 else 0.0
        actual_open_edge_usd = qty * (short_price - long_price) if hedge_price > 0 else 0.0
        actual_open_edge_bps = (
            actual_open_edge_usd / reference_notional_usd * 10000.0
            if reference_notional_usd > EPS
            else 0.0
        )
        return PostFirstFillEdgeDecision(
            can_hedge=True,
            reason="post_first_fill_edge_check_disabled",
            qty=qty,
            long_price=long_price,
            short_price=short_price,
            hedge_price=hedge_price,
            actual_open_edge_usd=actual_open_edge_usd,
            actual_open_edge_bps=actual_open_edge_bps,
            min_open_edge_bps=min_open_edge_bps,
            actual_estimated_net_edge_usd=0.0,
            actual_estimated_net_edge_bps=0.0,
            min_net_edge_bps=min_net_edge_bps,
            reference_notional_usd=reference_notional_usd,
        )
    if first_fill_price <= 0 or hedge_price <= 0:
        return PostFirstFillEdgeDecision(
            can_hedge=False,
            reason="post_first_fill_price_missing",
            qty=qty,
            long_price=0.0,
            short_price=0.0,
            hedge_price=hedge_price,
            actual_open_edge_usd=0.0,
            actual_open_edge_bps=0.0,
            min_open_edge_bps=min_open_edge_bps,
            actual_estimated_net_edge_usd=0.0,
            actual_estimated_net_edge_bps=0.0,
            min_net_edge_bps=min_net_edge_bps,
            reference_notional_usd=0.0,
        )

    if first_exchange_name == candidate.long_exchange:
        long_price = first_fill_price
        short_price = hedge_price
    else:
        long_price = hedge_price
        short_price = first_fill_price

    reference_notional_usd = qty * ((long_price + short_price) / 2.0)
    actual_open_edge_usd = qty * (short_price - long_price)
    actual_open_edge_bps = (
        actual_open_edge_usd / reference_notional_usd * 10000.0
        if reference_notional_usd > EPS
        else 0.0
    )
    qty_scale = qty / candidate.executable_qty if candidate.executable_qty > EPS else 1.0
    actual_estimated_net_edge_usd = (
        actual_open_edge_usd
        + candidate.effective_funding_pnl_usd * qty_scale
        - candidate.open_fee_cost_usd * qty_scale
        - candidate.expected_close_cost_usd * qty_scale
        - candidate.risk_buffer_usd * qty_scale
        - candidate.stale_data_penalty_usd * qty_scale
    )
    actual_estimated_net_edge_bps = (
        actual_estimated_net_edge_usd / reference_notional_usd * 10000.0
        if reference_notional_usd > EPS
        else 0.0
    )
    if actual_open_edge_bps <= min_open_edge_bps:
        can_hedge = False
        reason = "post_first_fill_edge_below_threshold"
    elif actual_estimated_net_edge_bps < min_net_edge_bps:
        can_hedge = False
        reason = "post_first_fill_net_edge_below_threshold"
    else:
        can_hedge = True
        reason = "ok"
    return PostFirstFillEdgeDecision(
        can_hedge=can_hedge,
        reason=reason,
        qty=qty,
        long_price=long_price,
        short_price=short_price,
        hedge_price=hedge_price,
        actual_open_edge_usd=actual_open_edge_usd,
        actual_open_edge_bps=actual_open_edge_bps,
        min_open_edge_bps=min_open_edge_bps,
        actual_estimated_net_edge_usd=actual_estimated_net_edge_usd,
        actual_estimated_net_edge_bps=actual_estimated_net_edge_bps,
        min_net_edge_bps=min_net_edge_bps,
        reference_notional_usd=reference_notional_usd,
    )
