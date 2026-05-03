from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from src.strategy.risk_gate import (
    RISK_MARGIN_MODE_CHECK_FAILED,
    RISK_MARGIN_MODE_NOT_CROSS,
    RISK_MARGIN_MODE_UNKNOWN,
)


def normalize_margin_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if normalized in {"cross", "crossed", "multi", "portfolio", "pm"}:
        return "cross"
    if normalized in {"isolated", "isolate", "single"}:
        return "isolated"
    return "unknown"


def _dedupe_reasons(reasons: list[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        normalized = str(reason or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return tuple(result)


async def _read_margin_mode(
    exchange: Any,
    symbol: str,
    expected_mode: str,
    *,
    retries: int = 1,
) -> dict[str, Any]:
    get_margin_mode = getattr(exchange, "get_margin_mode", None)
    if not callable(get_margin_mode):
        return {
            "raw_mode": None,
            "normalized_mode": "unknown",
            "reason": RISK_MARGIN_MODE_UNKNOWN,
            "error": "exchange_margin_mode_getter_missing",
        }

    result: dict[str, Any] = {}
    raw_mode: Any = None
    normalized_mode = "unknown"

    for attempt in range(retries + 1):
        try:
            maybe_result = await get_margin_mode(symbol, expected_mode=expected_mode)
            result = maybe_result if isinstance(maybe_result, dict) else {
                "ok": False,
                "margin_mode": None,
                "error": "invalid_margin_mode_response",
            }
        except Exception as exc:
            result = {"ok": False, "margin_mode": None, "error": str(exc)}

        raw_mode = result.get("margin_mode")
        normalized_mode = normalize_margin_mode(raw_mode)
        if normalized_mode != "unknown" or attempt >= retries:
            break
        await asyncio.sleep(0.25)

    if result.get("error") and normalized_mode == "unknown":
        return {
            "raw_mode": raw_mode,
            "normalized_mode": normalized_mode,
            "reason": RISK_MARGIN_MODE_CHECK_FAILED,
            "error": str(result.get("error")),
        }
    if normalized_mode == "unknown":
        return {
            "raw_mode": raw_mode,
            "normalized_mode": normalized_mode,
            "reason": RISK_MARGIN_MODE_UNKNOWN,
            "error": "",
        }
    if normalized_mode != "cross":
        return {
            "raw_mode": raw_mode,
            "normalized_mode": normalized_mode,
            "reason": RISK_MARGIN_MODE_NOT_CROSS,
            "error": "",
        }
    return {
        "raw_mode": raw_mode,
        "normalized_mode": normalized_mode,
        "reason": "",
        "error": "",
    }


async def check_startup_margin_mode_guard(
    *,
    exchanges: dict[str, Any],
    symbols: list[str],
    margin_mode: str,
    enabled: bool,
    record_event: Callable[..., None] | None = None,
) -> tuple[str, ...]:
    if not enabled:
        return ()

    expected_mode = normalize_margin_mode(margin_mode)
    checks: list[dict[str, Any]] = []
    block_reasons: list[str] = []

    if expected_mode != "cross":
        block_reasons.append(RISK_MARGIN_MODE_NOT_CROSS)
        print(
            f"[strategy][startup][margin-mode][block] configured_margin_mode={margin_mode} "
            f"normalized={expected_mode} reason={RISK_MARGIN_MODE_NOT_CROSS}"
        )

    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            result = await _read_margin_mode(exchange, symbol, expected_mode)
            reason = str(result.get("reason") or "")
            normalized_mode = str(result.get("normalized_mode") or "unknown")
            raw_mode = result.get("raw_mode")
            error = str(result.get("error") or "")
            ok = reason == "" and normalized_mode == "cross"

            if reason:
                block_reasons.append(reason)

            checks.append(
                {
                    "exchange": exchange_name,
                    "symbol": symbol,
                    "raw_mode": raw_mode,
                    "normalized_mode": normalized_mode,
                    "ok": ok,
                    "reason": reason,
                    "error": error,
                }
            )
            print(
                f"[strategy][startup][margin-mode] exchange={exchange_name} symbol={symbol} "
                f"raw={raw_mode or 'None'} normalized={normalized_mode} ok={'Y' if ok else 'N'}"
                + (f" reason={reason}" if reason else "")
            )

    deduped = _dedupe_reasons(block_reasons)
    if record_event is not None:
        record_event(
            "startup_margin_mode_checked",
            expected_mode=expected_mode,
            ok=not deduped,
            block_reasons=list(deduped),
            checks=checks,
        )
        if deduped:
            record_event(
                "startup_entry_blocked",
                source="startup_margin_mode_guard",
                reasons=list(deduped),
            )

    if deduped:
        print(f"[strategy][startup][entry-block] reasons={list(deduped)}")
    return deduped
