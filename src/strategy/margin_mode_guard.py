from __future__ import annotations

import asyncio
import inspect
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


def _classify_margin_mode(raw_mode: Any, *, source: str, error: str = "") -> dict[str, Any]:
    normalized_mode = normalize_margin_mode(raw_mode)
    if error and normalized_mode == "unknown":
        reason = RISK_MARGIN_MODE_CHECK_FAILED
    elif normalized_mode == "unknown":
        reason = RISK_MARGIN_MODE_UNKNOWN
    elif normalized_mode != "cross":
        reason = RISK_MARGIN_MODE_NOT_CROSS
    else:
        reason = ""

    return {
        "raw_mode": raw_mode,
        "normalized_mode": normalized_mode,
        "reason": reason,
        "error": error if reason == RISK_MARGIN_MODE_CHECK_FAILED else "",
        "source": source,
    }


async def _read_margin_mode_from_account_snapshot(exchange: Any, symbol: str) -> dict[str, Any] | None:
    getter = getattr(exchange, "get_margin_mode_from_account_snapshot", None)
    if not callable(getter):
        return None

    try:
        maybe_result = getter(symbol)
        result = await maybe_result if inspect.isawaitable(maybe_result) else maybe_result
    except Exception as exc:
        return _classify_margin_mode(
            None,
            source="account_snapshot",
            error=f"account_snapshot_margin_mode_read_failed:{exc}",
        )

    if not isinstance(result, dict):
        return None

    raw_mode = result.get("margin_mode")
    if raw_mode not in (None, ""):
        return _classify_margin_mode(
            raw_mode,
            source=str(result.get("source") or "account_snapshot"),
            error=str(result.get("error") or ""),
        )

    if result.get("fallback_allowed") is False:
        if result.get("non_blocking"):
            return {
                "raw_mode": raw_mode,
                "normalized_mode": "unknown",
                "reason": "",
                "error": str(result.get("error") or ""),
                "source": str(result.get("source") or "account_snapshot"),
            }
        return _classify_margin_mode(
            raw_mode,
            source=str(result.get("source") or "account_snapshot"),
            error=str(result.get("error") or "margin_mode_unavailable"),
        )

    return None


async def _read_margin_mode(
    exchange: Any,
    symbol: str,
    expected_mode: str,
    *,
    retries: int = 1,
) -> dict[str, Any]:
    snapshot_result = await _read_margin_mode_from_account_snapshot(exchange, symbol)
    if snapshot_result is not None:
        return snapshot_result

    get_margin_mode = getattr(exchange, "get_margin_mode", None)
    if not callable(get_margin_mode):
        return {
            "raw_mode": None,
            "normalized_mode": "unknown",
            "reason": RISK_MARGIN_MODE_UNKNOWN,
            "error": "exchange_margin_mode_getter_missing",
            "source": "api",
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

    return _classify_margin_mode(
        raw_mode,
        source="api",
        error=str(result.get("error") or ""),
    )


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
            source = str(result.get("source") or "api")
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
                    "source": source,
                }
            )
            print(
                f"[strategy][startup][margin-mode] exchange={exchange_name} symbol={symbol} "
                f"raw={raw_mode or 'None'} normalized={normalized_mode} source={source} ok={'Y' if ok else 'N'}"
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
