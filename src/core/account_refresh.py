from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable


RefreshFunc = Callable[[str], Awaitable[bool | None]]
EventLogger = Callable[[str, dict[str, Any]], None]


class AccountRefreshDeferred(Exception):
    """Signal that refresh should be retried later without counting as failure."""

    def __init__(self, reason: str, retry_after_sec: float = 0.0) -> None:
        super().__init__(reason)
        self.reason = str(reason or "deferred")
        self.retry_after_sec = max(float(retry_after_sec or 0.0), 0.0)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off", ""}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _now_ms() -> int:
    return int(time.time() * 1000)


class AccountRefreshScheduler:
    """Merge account refresh requests and execute them under cooldowns.

    The scheduler deliberately knows nothing about strategy decisions. The
    caller injects the actual exchange account refresh function.
    """

    def __init__(
        self,
        *,
        exchange: str,
        refresh_func: RefreshFunc,
        enabled: bool | None = None,
        min_interval_sec: float | None = None,
        urgent_interval_sec: float | None = None,
        max_wait_sec: float | None = None,
        failure_cooldown_sec: float | None = None,
        event_logger: EventLogger | None = None,
    ) -> None:
        self.exchange = exchange
        self.refresh_func = refresh_func
        self.enabled = _env_bool("ACCOUNT_REFRESH_SCHEDULER_ENABLED", True) if enabled is None else bool(enabled)
        self.min_interval_sec = max(
            _env_float("ACCOUNT_REFRESH_MIN_INTERVAL_SEC", 2.0)
            if min_interval_sec is None
            else float(min_interval_sec),
            0.0,
        )
        self.urgent_interval_sec = max(
            _env_float("ACCOUNT_REFRESH_URGENT_INTERVAL_SEC", 0.5)
            if urgent_interval_sec is None
            else float(urgent_interval_sec),
            0.0,
        )
        self.max_wait_sec = max(
            _env_float("ACCOUNT_REFRESH_MAX_WAIT_SEC", 1.0)
            if max_wait_sec is None
            else float(max_wait_sec),
            0.0,
        )
        self.failure_cooldown_sec = max(
            _env_float("ACCOUNT_REFRESH_FAILURE_COOLDOWN_SEC", 5.0)
            if failure_cooldown_sec is None
            else float(failure_cooldown_sec),
            0.0,
        )
        self.event_logger = event_logger

        self._pending: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._event = asyncio.Event()
        self._running = False
        self._refreshing = False

        self.last_refresh_attempt_ms = 0
        self.last_refresh_success_ms = 0
        self.last_refresh_failure_ms = 0
        self.failure_cooldown_until_ms = 0
        self.refresh_count = 0
        self.failure_count = 0
        self.deferred_count = 0
        self.skipped_count = 0
        self.coalesced_count = 0
        self.request_count = 0
        self.last_reason = ""
        self.last_priority = ""
        self.last_error = ""

    def request(self, reason: str, priority: str = "normal", symbol: str | None = None) -> bool:
        if not self.enabled:
            return False
        normalized_priority = "urgent" if str(priority).strip().lower() == "urgent" else "normal"
        normalized_reason = str(reason or "unspecified").strip() or "unspecified"
        normalized_symbol = str(symbol or "").strip().upper()
        key = (normalized_reason, normalized_priority, normalized_symbol)
        now_ms = _now_ms()
        existed = key in self._pending
        item = self._pending.setdefault(
            key,
            {
                "reason": normalized_reason,
                "priority": normalized_priority,
                "symbol": normalized_symbol,
                "first_requested_ms": now_ms,
                "count": 0,
            },
        )
        item["last_requested_ms"] = now_ms
        item["count"] = int(item.get("count", 0) or 0) + 1
        self.request_count += 1
        if existed:
            self.coalesced_count += 1
            self._event.set()
            return True
        self._log(
            "account_refresh_requested",
            reason=normalized_reason,
            priority=normalized_priority,
            symbol=normalized_symbol or None,
            success=None,
            error="",
        )
        self._event.set()
        return True

    async def run(self) -> None:
        if not self.enabled:
            return
        self._running = True
        while self._running:
            try:
                if not self._pending:
                    await self._event.wait()
                    self._event.clear()
                    continue

                wait_sec = self._seconds_until_refresh_allowed()
                if wait_sec > 0:
                    self.skipped_count += 1
                    self._log(
                        "account_refresh_skipped_cooldown",
                        reason=self._merged_reason(),
                        priority=self._highest_priority(),
                        symbol=self._merged_symbol(),
                        success=False,
                        error="cooldown",
                    )
                    try:
                        await asyncio.wait_for(self._event.wait(), timeout=min(wait_sec, max(self.max_wait_sec, 0.05)))
                        self._event.clear()
                    except asyncio.TimeoutError:
                        pass
                    continue

                await self.refresh_now(self._merged_reason(), self._highest_priority())
            except asyncio.CancelledError:
                self._running = False
                raise

    async def refresh_now(self, reason: str, priority: str = "normal") -> bool:
        if not self.enabled:
            return False
        if self._refreshing:
            self.skipped_count += 1
            self._log(
                "account_refresh_skipped_cooldown",
                reason=reason,
                priority=priority,
                symbol=self._merged_symbol(),
                success=False,
                error="already_refreshing",
            )
            return False

        wait_sec = self._seconds_until_refresh_allowed(priority=priority)
        if wait_sec > 0:
            self.skipped_count += 1
            self._log(
                "account_refresh_skipped_cooldown",
                reason=reason,
                priority=priority,
                symbol=self._merged_symbol(),
                success=False,
                error="cooldown",
            )
            return False

        pending_snapshot = list(self._pending.values())
        self._pending.clear()
        merged_reason = self._format_reason(reason, pending_snapshot)
        merged_priority = self._highest_priority(pending_snapshot, fallback=priority)
        elapsed_before_attempt_ms = self._elapsed_since_last_refresh_ms()
        self._refreshing = True
        self.last_refresh_attempt_ms = _now_ms()
        self.last_reason = merged_reason
        self.last_priority = merged_priority

        try:
            result = await self.refresh_func(merged_reason)
            success = result is not False
            if not success:
                raise RuntimeError("refresh_func_returned_false")
            self.refresh_count += 1
            self.last_refresh_success_ms = _now_ms()
            self.last_error = ""
            self._log(
                "account_refresh_executed",
                reason=merged_reason,
                priority=merged_priority,
                symbol=self._symbols_from_pending(pending_snapshot),
                success=True,
                error="",
                pending_snapshot=pending_snapshot,
                elapsed_since_last_refresh_ms=elapsed_before_attempt_ms,
            )
            return True
        except AccountRefreshDeferred as exc:
            self.deferred_count += 1
            self.skipped_count += 1
            now_ms = _now_ms()
            retry_after_ms = int(exc.retry_after_sec * 1000)
            if retry_after_ms > 0:
                self.failure_cooldown_until_ms = max(self.failure_cooldown_until_ms, now_ms + retry_after_ms)
            self.last_error = exc.reason
            self._log(
                "account_refresh_skipped_cooldown",
                reason=merged_reason,
                priority=merged_priority,
                symbol=self._symbols_from_pending(pending_snapshot),
                success=False,
                error=exc.reason,
                pending_snapshot=pending_snapshot,
                elapsed_since_last_refresh_ms=elapsed_before_attempt_ms,
            )
            return False
        except Exception as exc:
            self.failure_count += 1
            self.last_refresh_failure_ms = _now_ms()
            self.failure_cooldown_until_ms = self.last_refresh_failure_ms + int(self.failure_cooldown_sec * 1000)
            self.last_error = str(exc)
            self._log(
                "account_refresh_failed",
                reason=merged_reason,
                priority=merged_priority,
                symbol=self._symbols_from_pending(pending_snapshot),
                success=False,
                error=str(exc),
                pending_snapshot=pending_snapshot,
                elapsed_since_last_refresh_ms=elapsed_before_attempt_ms,
            )
            return False
        finally:
            self._refreshing = False

    def snapshot_status(self) -> dict[str, Any]:
        pending = list(self._pending.values())
        return {
            "enabled": self.enabled,
            "exchange": self.exchange,
            "pending_count": len(pending),
            "pending_reason_count": len({item.get("reason") for item in pending}),
            "pending_reasons": sorted({str(item.get("reason") or "") for item in pending if item.get("reason")}),
            "pending_symbols": sorted({str(item.get("symbol") or "") for item in pending if item.get("symbol")}),
            "last_refresh_attempt_ms": self.last_refresh_attempt_ms,
            "last_refresh_success_ms": self.last_refresh_success_ms,
            "last_refresh_failure_ms": self.last_refresh_failure_ms,
            "failure_cooldown_until_ms": self.failure_cooldown_until_ms,
            "refresh_count": self.refresh_count,
            "failure_count": self.failure_count,
            "deferred_count": self.deferred_count,
            "skipped_count": self.skipped_count,
            "coalesced_count": self.coalesced_count,
            "request_count": self.request_count,
            "last_reason": self.last_reason,
            "last_priority": self.last_priority,
            "last_error": self.last_error,
            "refreshing": self._refreshing,
        }

    def _seconds_until_refresh_allowed(self, priority: str | None = None) -> float:
        now_ms = _now_ms()
        if self.failure_cooldown_until_ms > now_ms:
            return (self.failure_cooldown_until_ms - now_ms) / 1000.0
        last_attempt_ms = int(self.last_refresh_attempt_ms or 0)
        if last_attempt_ms <= 0:
            return 0.0
        interval_sec = self.urgent_interval_sec if (priority or self._highest_priority()) == "urgent" else self.min_interval_sec
        next_allowed_ms = last_attempt_ms + int(interval_sec * 1000)
        if next_allowed_ms <= now_ms:
            return 0.0
        return (next_allowed_ms - now_ms) / 1000.0

    def _highest_priority(
        self,
        pending: list[dict[str, Any]] | None = None,
        *,
        fallback: str = "normal",
    ) -> str:
        items = pending if pending is not None else list(self._pending.values())
        if any(str(item.get("priority")) == "urgent" for item in items):
            return "urgent"
        return "urgent" if str(fallback).strip().lower() == "urgent" else "normal"

    def _merged_reason(self) -> str:
        return self._format_reason("pending", list(self._pending.values()))

    def _merged_symbol(self) -> str | None:
        return self._symbols_from_pending(list(self._pending.values()))

    def _format_reason(self, fallback: str, pending: list[dict[str, Any]]) -> str:
        counter: dict[str, int] = defaultdict(int)
        for item in pending:
            reason = str(item.get("reason") or "").strip()
            if reason:
                counter[reason] += int(item.get("count", 1) or 1)
        if not counter:
            return str(fallback or "manual")
        parts = [f"{reason}x{count}" if count > 1 else reason for reason, count in sorted(counter.items())]
        return ",".join(parts)

    def _symbols_from_pending(self, pending: list[dict[str, Any]]) -> str | None:
        symbols = sorted({str(item.get("symbol") or "").strip().upper() for item in pending if item.get("symbol")})
        if not symbols:
            return None
        if len(symbols) == 1:
            return symbols[0]
        return ",".join(symbols)

    def _pending_reason_count(self, pending: list[dict[str, Any]] | None = None) -> int:
        items = pending if pending is not None else list(self._pending.values())
        return len({item.get("reason") for item in items if item.get("reason")})

    def _elapsed_since_last_refresh_ms(self) -> int | None:
        last_ms = int(self.last_refresh_attempt_ms or self.last_refresh_success_ms or 0)
        if last_ms <= 0:
            return None
        return max(_now_ms() - last_ms, 0)

    def _log(
        self,
        event_type: str,
        *,
        reason: str,
        priority: str,
        symbol: str | None,
        success: bool | None,
        error: str,
        pending_snapshot: list[dict[str, Any]] | None = None,
        elapsed_since_last_refresh_ms: int | None = None,
    ) -> None:
        payload = {
            "exchange": self.exchange,
            "reason": reason,
            "priority": "urgent" if str(priority).strip().lower() == "urgent" else "normal",
            "symbol": symbol,
            "pending_reason_count": self._pending_reason_count(pending_snapshot),
            "elapsed_since_last_refresh_ms": (
                self._elapsed_since_last_refresh_ms()
                if elapsed_since_last_refresh_ms is None
                else elapsed_since_last_refresh_ms
            ),
            "success": success,
            "error": error,
        }
        if self.event_logger is not None:
            try:
                self.event_logger(event_type, payload)
            except Exception as exc:
                print(f"[account_refresh][log_error] exchange={self.exchange} event={event_type} error={exc!r}")
            return
        print(
            f"[account_refresh] event={event_type} exchange={self.exchange} "
            f"reason={reason} priority={payload['priority']} symbol={symbol or ''} "
            f"pending_reason_count={payload['pending_reason_count']} "
            f"elapsed_since_last_refresh_ms={payload['elapsed_since_last_refresh_ms']} "
            f"success={success} error={error}"
        )
