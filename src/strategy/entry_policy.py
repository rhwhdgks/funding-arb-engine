from __future__ import annotations

from dataclasses import dataclass, field

from src.strategy.models import StrategyConfig


EPS = 1e-9


@dataclass(frozen=True)
class EntryPolicyDecision:
    can_enter: bool
    reasons: list[str] = field(default_factory=list)


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
