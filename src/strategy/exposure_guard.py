from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.strategy.risk_gate import (
    RISK_EXPOSURE_CAP_EXCEEDED,
    RISK_EXPOSURE_EQUITY_UNAVAILABLE,
)


@dataclass(frozen=True)
class ExposureCapConfig:
    max_gross_leverage: float = 0.0
    max_long_leverage: float = 0.0
    max_short_leverage: float = 0.0

    @property
    def enabled(self) -> bool:
        return (
            self.max_gross_leverage > 0
            or self.max_long_leverage > 0
            or self.max_short_leverage > 0
        )


@dataclass(frozen=True)
class ExposureGuardResult:
    allowed: bool
    reasons: tuple[str, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _positive_report_value(report: dict[str, Any], keys: tuple[str, ...]) -> float:
    for key in keys:
        value = _safe_float(report.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def evaluate_exposure_caps(
    *,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None,
    planned_long_notional_usd: float,
    planned_short_notional_usd: float,
    config: ExposureCapConfig,
) -> ExposureGuardResult:
    reports = balance_reports_by_exchange or {}
    total_equity = 0.0
    current_long = 0.0
    current_short = 0.0

    for report in reports.values():
        if not isinstance(report, dict):
            continue
        total_equity += _positive_report_value(
            report,
            ("actual_equity_usd", "total_balance_usd", "margin_balance_usd", "wallet_balance_usd"),
        )
        current_long += max(_safe_float(report.get("long_notional_usd"), 0.0), 0.0)
        current_short += max(_safe_float(report.get("short_notional_usd"), 0.0), 0.0)

    planned_long = max(float(planned_long_notional_usd or 0.0), 0.0)
    planned_short = max(float(planned_short_notional_usd or 0.0), 0.0)
    projected_long = current_long + planned_long
    projected_short = current_short + planned_short
    current_gross = current_long + current_short
    planned_gross = planned_long + planned_short
    projected_gross = projected_long + projected_short

    metrics: dict[str, Any] = {
        "total_equity_usd": total_equity,
        "current_long_notional_usd": current_long,
        "current_short_notional_usd": current_short,
        "current_gross_notional_usd": current_gross,
        "planned_long_notional_usd": planned_long,
        "planned_short_notional_usd": planned_short,
        "planned_gross_notional_usd": planned_gross,
        "projected_long_notional_usd": projected_long,
        "projected_short_notional_usd": projected_short,
        "projected_gross_notional_usd": projected_gross,
        "max_gross_leverage": config.max_gross_leverage,
        "max_long_leverage": config.max_long_leverage,
        "max_short_leverage": config.max_short_leverage,
        "violated_caps": [],
    }

    if not config.enabled:
        metrics.update(
            {
                "projected_gross_leverage": 0.0,
                "projected_long_leverage": 0.0,
                "projected_short_leverage": 0.0,
            }
        )
        return ExposureGuardResult(allowed=True, metrics=metrics)

    if total_equity <= 0:
        return ExposureGuardResult(
            allowed=False,
            reasons=(RISK_EXPOSURE_EQUITY_UNAVAILABLE,),
            metrics=metrics,
        )

    projected_gross_leverage = projected_gross / total_equity
    projected_long_leverage = projected_long / total_equity
    projected_short_leverage = projected_short / total_equity
    metrics.update(
        {
            "projected_gross_leverage": projected_gross_leverage,
            "projected_long_leverage": projected_long_leverage,
            "projected_short_leverage": projected_short_leverage,
        }
    )

    violated_caps: list[str] = []
    if config.max_gross_leverage > 0 and projected_gross_leverage > config.max_gross_leverage:
        violated_caps.append("gross")
    if config.max_long_leverage > 0 and projected_long_leverage > config.max_long_leverage:
        violated_caps.append("long")
    if config.max_short_leverage > 0 and projected_short_leverage > config.max_short_leverage:
        violated_caps.append("short")

    metrics["violated_caps"] = violated_caps
    if violated_caps:
        return ExposureGuardResult(
            allowed=False,
            reasons=(RISK_EXPOSURE_CAP_EXCEEDED,),
            metrics=metrics,
        )
    return ExposureGuardResult(allowed=True, metrics=metrics)
