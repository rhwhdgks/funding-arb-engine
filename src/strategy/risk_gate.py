from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


RISK_SYMBOL_PAIR_EXISTS = "risk:symbol_pair_exists"
RISK_SYMBOL_SIDE_ORDER_EXISTS = "risk:symbol_side_order_exists"
RISK_ENTRY_PAUSED = "risk:entry_paused"
RISK_INSUFFICIENT_MARGIN = "risk:insufficient_margin"
RISK_MARGIN_MODE_NOT_CROSS = "risk:margin_mode_not_cross"
RISK_MARGIN_MODE_UNKNOWN = "risk:margin_mode_unknown"
RISK_MARGIN_MODE_CHECK_FAILED = "risk:margin_mode_check_failed"
RISK_EXPOSURE_CAP_EXCEEDED = "risk:exposure_cap_exceeded"
RISK_EXPOSURE_EQUITY_UNAVAILABLE = "risk:exposure_equity_unavailable"
RISK_RATE_LIMIT_PRESSURE = "risk:rate_limit_pressure"


@dataclass(frozen=True)
class OrderSlot:
    exchange_name: str
    symbol: str
    side: str = ""

    def normalized(self) -> "OrderSlot":
        return OrderSlot(
            exchange_name=str(self.exchange_name or "").strip().lower(),
            symbol=str(self.symbol or "").strip().upper(),
            side=str(self.side or "").strip().lower(),
        )


@dataclass(frozen=True)
class RiskGateContext:
    active_symbols: frozenset[str]
    open_order_slots: frozenset[OrderSlot]
    entry_control_reasons: tuple[str, ...] = ()
    startup_block_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class RiskGateDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)


def _dedupe_reasons(reasons: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        normalized = str(reason or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return tuple(result)


def _order_slots_match(planned: OrderSlot, existing: OrderSlot) -> bool:
    planned = planned.normalized()
    existing = existing.normalized()
    if not planned.exchange_name or not planned.symbol:
        return False
    if planned.exchange_name != existing.exchange_name or planned.symbol != existing.symbol:
        return False
    return not existing.side or not planned.side or planned.side == existing.side


def evaluate_entry_risk(
    *,
    symbol: str,
    planned_order_slots: Iterable[OrderSlot],
    margin_reasons: Iterable[str] = (),
    exposure_reasons: Iterable[str] = (),
    rate_limit_reasons: Iterable[str] = (),
    details: dict[str, Any] | None = None,
    context: RiskGateContext,
) -> RiskGateDecision:
    reasons: list[str] = []
    normalized_symbol = str(symbol or "").strip().upper()

    if normalized_symbol and normalized_symbol in context.active_symbols:
        reasons.append(RISK_SYMBOL_PAIR_EXISTS)

    for planned_slot in planned_order_slots:
        if any(_order_slots_match(planned_slot, open_slot) for open_slot in context.open_order_slots):
            reasons.append(RISK_SYMBOL_SIDE_ORDER_EXISTS)
            break

    if context.entry_control_reasons:
        reasons.append(RISK_ENTRY_PAUSED)

    reasons.extend(context.startup_block_reasons)

    if any(str(reason or "").strip() for reason in margin_reasons):
        reasons.append(RISK_INSUFFICIENT_MARGIN)

    reasons.extend(exposure_reasons)
    reasons.extend(rate_limit_reasons)

    deduped = _dedupe_reasons(reasons)
    return RiskGateDecision(allowed=not deduped, reasons=deduped, details=dict(details or {}))
