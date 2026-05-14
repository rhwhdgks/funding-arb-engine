from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from src.strategy.risk_gate import RISK_RATE_LIMIT_PRESSURE


@dataclass(frozen=True)
class RateLimitGuardConfig:
    enabled: bool = True
    usage_threshold: float = 0.85


@dataclass(frozen=True)
class RateLimitGuardResult:
    allowed: bool
    reasons: tuple[str, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except Exception:
        return default


def _exchange_rate_limit_state(exchange: Any) -> dict[str, Any]:
    getter = getattr(exchange, "get_rate_limit_state", None)
    if callable(getter):
        try:
            state = getter()
            return dict(state or {}) if isinstance(state, dict) else {}
        except Exception as exc:
            return {"blocked": True, "reason": f"rate_limit_state_error:{exc}"}
    state = getattr(exchange, "rate_limit_state", {}) or {}
    return dict(state) if isinstance(state, dict) else {}


def _iter_limit_items(state: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for item in state.get("last") or state.get("rate_limits") or ():
        if isinstance(item, dict):
            yield item


def _item_usage(item: dict[str, Any]) -> float:
    count = _safe_float(item.get("count"), 0.0)
    limit = _safe_float(item.get("limit"), 0.0)
    if limit <= 0:
        return 0.0
    return max(count / limit, 0.0)


def evaluate_rate_limit_guard(
    *,
    exchanges: dict[str, Any],
    exchange_names: Iterable[str],
    config: RateLimitGuardConfig,
    now_ms: int | None = None,
) -> RateLimitGuardResult:
    if not config.enabled:
        return RateLimitGuardResult(allowed=True)

    now_ms = now_ms or _now_ms()
    threshold = max(min(float(config.usage_threshold or 0.0), 1.0), 0.0)
    exchange_metrics: dict[str, Any] = {}
    blocked = False

    for exchange_name in dict.fromkeys(str(name).strip().lower() for name in exchange_names if str(name).strip()):
        exchange = exchanges.get(exchange_name)
        if exchange is None:
            continue

        state = _exchange_rate_limit_state(exchange)
        limit_items = list(_iter_limit_items(state))
        max_usage = max((_item_usage(item) for item in limit_items), default=0.0)
        cooldown_until_ms = _safe_int(
            state.get("cooldown_until_ms")
            or state.get("pause_until_ms")
            or state.get("blocked_until_ms"),
            0,
        )
        state_blocked = bool(state.get("blocked")) or (cooldown_until_ms > now_ms)
        pressure = max_usage >= threshold if threshold > 0 else False

        reasons: list[str] = []
        if state_blocked:
            reasons.append(str(state.get("reason") or "rate_limit_block"))
        if pressure:
            reasons.append("usage_threshold")

        if state_blocked or pressure:
            blocked = True

        exchange_metrics[exchange_name] = {
            "blocked": state_blocked,
            "pressure": pressure,
            "reason": state.get("reason", ""),
            "max_usage": max_usage,
            "threshold": threshold,
            "cooldown_until_ms": cooldown_until_ms,
            "rate_limits": limit_items,
            "block_reasons": reasons,
        }

    metrics = {
        "enabled": config.enabled,
        "usage_threshold": threshold,
        "exchanges": exchange_metrics,
    }
    if blocked:
        return RateLimitGuardResult(
            allowed=False,
            reasons=(RISK_RATE_LIMIT_PRESSURE,),
            metrics=metrics,
        )
    return RateLimitGuardResult(allowed=True, metrics=metrics)
