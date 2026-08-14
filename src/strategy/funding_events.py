from __future__ import annotations

from dataclasses import dataclass
from typing import Any


COMMON_FUNDING_INTERVAL_HOURS = (1, 2, 4, 6, 8)
EPS = 1e-12


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_funding_interval_hours(value: Any) -> int | None:
    parsed = safe_int(value)
    if 0 < parsed <= 24:
        return parsed
    return None


def infer_funding_interval_hours(
    current_funding_time_ms: Any,
    next_funding_time_ms: Any,
) -> int | None:
    current_ms = safe_int(current_funding_time_ms)
    next_ms = safe_int(next_funding_time_ms)
    if current_ms <= 0 or next_ms <= current_ms:
        return None
    interval_hours = (next_ms - current_ms) / 3_600_000.0
    for allowed in COMMON_FUNDING_INTERVAL_HOURS:
        if abs(interval_hours - allowed) <= 0.05:
            return allowed
    return None


@dataclass(frozen=True)
class FundingLegEventEstimate:
    side: str
    rate: float
    notional_usd: float
    next_funding_time_ms: int
    interval_hours: int | None
    interval_source: str
    entry_complete_ms: int
    forecast_end_ms: int
    event_count: int
    pnl_usd: float
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "rate": self.rate,
            "notional_usd": self.notional_usd,
            "next_funding_time_ms": self.next_funding_time_ms,
            "interval_hours": self.interval_hours,
            "interval_source": self.interval_source,
            "entry_complete_ms": self.entry_complete_ms,
            "forecast_end_ms": self.forecast_end_ms,
            "event_count": self.event_count,
            "pnl_usd": self.pnl_usd,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PairFundingEventEstimate:
    long: FundingLegEventEstimate
    short: FundingLegEventEstimate
    pnl_usd: float
    event_count: int
    next_event_time_ms: int
    schedule_complete: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "pnl_usd": self.pnl_usd,
            "event_count": self.event_count,
            "next_event_time_ms": self.next_event_time_ms,
            "schedule_complete": self.schedule_complete,
            "reason": self.reason,
            "long": self.long.as_dict(),
            "short": self.short.as_dict(),
        }


def estimate_leg_funding_event(
    *,
    side: str,
    rate: float,
    notional_usd: float,
    next_funding_time_ms: int,
    interval_hours: int | None,
    interval_source: str,
    entry_complete_ms: int,
    forecast_end_ms: int,
) -> FundingLegEventEstimate:
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"long", "short"}:
        raise ValueError(f"unsupported funding side: {side}")

    rate = float(rate or 0.0)
    notional_usd = max(float(notional_usd or 0.0), 0.0)
    next_funding_time_ms = safe_int(next_funding_time_ms)
    entry_complete_ms = safe_int(entry_complete_ms)
    forecast_end_ms = safe_int(forecast_end_ms)
    interval_hours = normalize_funding_interval_hours(interval_hours)

    if notional_usd <= EPS:
        event_count = 0
        pnl_usd = 0.0
        reason = "notional_missing"
    elif next_funding_time_ms <= 0:
        event_count = 0
        pnl_usd = 0.0
        reason = "next_funding_time_missing"
    elif forecast_end_ms <= entry_complete_ms:
        event_count = 0
        pnl_usd = 0.0
        reason = "forecast_window_empty"
    elif next_funding_time_ms <= entry_complete_ms:
        event_count = 0
        pnl_usd = 0.0
        reason = "settlement_before_entry_complete"
    elif next_funding_time_ms > forecast_end_ms:
        event_count = 0
        pnl_usd = 0.0
        reason = "settlement_outside_forecast"
    else:
        event_count = 1
        signed_multiplier = -1.0 if normalized_side == "long" else 1.0
        pnl_usd = signed_multiplier * rate * notional_usd
        reason = "next_settlement_in_forecast"

    return FundingLegEventEstimate(
        side=normalized_side,
        rate=rate,
        notional_usd=notional_usd,
        next_funding_time_ms=next_funding_time_ms,
        interval_hours=interval_hours,
        interval_source=str(interval_source or "unknown"),
        entry_complete_ms=entry_complete_ms,
        forecast_end_ms=forecast_end_ms,
        event_count=event_count,
        pnl_usd=pnl_usd,
        reason=reason,
    )


def estimate_pair_funding_events(
    *,
    long_rate: float,
    short_rate: float,
    long_notional_usd: float,
    short_notional_usd: float,
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    long_interval_hours: int | None,
    short_interval_hours: int | None,
    long_interval_source: str,
    short_interval_source: str,
    entry_complete_ms: int,
    forecast_end_ms: int,
) -> PairFundingEventEstimate:
    """Forecast only each leg's next explicitly known settlement event.

    Interval metadata is retained for observability and schedule validation. We
    deliberately do not extrapolate later events with the current funding rate.
    """
    long = estimate_leg_funding_event(
        side="long",
        rate=long_rate,
        notional_usd=long_notional_usd,
        next_funding_time_ms=long_next_funding_ms,
        interval_hours=long_interval_hours,
        interval_source=long_interval_source,
        entry_complete_ms=entry_complete_ms,
        forecast_end_ms=forecast_end_ms,
    )
    short = estimate_leg_funding_event(
        side="short",
        rate=short_rate,
        notional_usd=short_notional_usd,
        next_funding_time_ms=short_next_funding_ms,
        interval_hours=short_interval_hours,
        interval_source=short_interval_source,
        entry_complete_ms=entry_complete_ms,
        forecast_end_ms=forecast_end_ms,
    )
    event_times = [
        leg.next_funding_time_ms
        for leg in (long, short)
        if leg.event_count > 0 and leg.next_funding_time_ms > 0
    ]
    schedule_complete = long.next_funding_time_ms > 0 and short.next_funding_time_ms > 0
    event_count = long.event_count + short.event_count
    pair_pnl_usd = long.pnl_usd + short.pnl_usd if schedule_complete else 0.0
    if not schedule_complete:
        reason = "funding_schedule_incomplete"
    elif event_count > 0:
        reason = "settlement_event_in_forecast"
    else:
        reason = "no_settlement_in_forecast"
    return PairFundingEventEstimate(
        long=long,
        short=short,
        pnl_usd=pair_pnl_usd,
        event_count=event_count,
        next_event_time_ms=min(event_times) if event_times else 0,
        schedule_complete=schedule_complete,
        reason=reason,
    )


def per_settlement_direction_pnl_usd(
    *,
    long_rate: float,
    short_rate: float,
    long_notional_usd: float,
    short_notional_usd: float,
) -> float:
    return (
        -float(long_rate or 0.0) * max(float(long_notional_usd or 0.0), 0.0)
        + float(short_rate or 0.0) * max(float(short_notional_usd or 0.0), 0.0)
    )


def legacy_prorated_funding_pnl_usd(
    *,
    direction_pnl_usd: float,
    forecast_hold_sec: float,
    settlement_sec: float = 28_800.0,
) -> float:
    if settlement_sec <= 0:
        return 0.0
    return float(direction_pnl_usd or 0.0) * max(float(forecast_hold_sec or 0.0), 0.0) / settlement_sec
