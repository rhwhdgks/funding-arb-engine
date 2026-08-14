from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .account_context import current_account_context
from .db import db_persistence_enabled, get_db_connection, insert_cashflow_row


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class PairFundingInterval:
    pair_id: str
    symbol: str
    entry_time_ms: int
    close_time_ms: int | None = None


class FundingPairAttributionRegistry:
    """Keep recent pair lifetimes so delayed funding rows remain attributable."""

    def __init__(self, *, max_intervals: int = 2_000) -> None:
        self.max_intervals = max(int(max_intervals), 100)
        self._intervals: dict[str, PairFundingInterval] = {}
        self._lock = threading.Lock()

    def observe_event(self, event_type: str, payload: dict[str, Any], event_time_ms: int) -> None:
        pair = payload.get("pair") if isinstance(payload.get("pair"), dict) else {}
        pair_id = str(pair.get("pair_id") or payload.get("pair_id") or "").strip()
        symbol = str(pair.get("symbol") or payload.get("symbol") or "").strip().upper()
        if not pair_id or not symbol:
            return
        entry_time_ms = int(pair.get("entry_time_ms") or payload.get("entry_time_ms") or event_time_ms)
        normalized_event = str(event_type or "").lower()
        is_closed = "closed" in normalized_event or str(pair.get("status") or "").lower() in {"closed", "failed"}
        with self._lock:
            interval = self._intervals.get(pair_id)
            if interval is None:
                interval = PairFundingInterval(pair_id, symbol, entry_time_ms)
                self._intervals[pair_id] = interval
            else:
                interval.entry_time_ms = min(interval.entry_time_ms, entry_time_ms)
            if is_closed:
                interval.close_time_ms = max(int(event_time_ms), interval.entry_time_ms)
            overflow = len(self._intervals) - self.max_intervals
            if overflow > 0:
                oldest = sorted(
                    self._intervals.values(),
                    key=lambda row: row.close_time_ms or row.entry_time_ms,
                )[:overflow]
                for row in oldest:
                    self._intervals.pop(row.pair_id, None)

    def pair_id_at(self, symbol: str, event_time_ms: int) -> str | None:
        normalized_symbol = symbol.upper()
        with self._lock:
            matches = [
                row
                for row in self._intervals.values()
                if row.symbol == normalized_symbol
                and row.entry_time_ms <= event_time_ms
                and (row.close_time_ms is None or event_time_ms <= row.close_time_ms)
            ]
        if not matches:
            return None
        return max(matches, key=lambda row: row.entry_time_ms).pair_id


_PAIR_ATTRIBUTION_REGISTRY = FundingPairAttributionRegistry()


def observe_pair_lifecycle_event(event_type: str, payload: dict[str, Any], event_time_ms: int) -> None:
    _PAIR_ATTRIBUTION_REGISTRY.observe_event(event_type, payload, event_time_ms)


def _movement_key(exchange_name: str, payment: dict[str, Any]) -> str:
    movement_id = str(payment.get("movement_id") or "").strip()
    if not movement_id:
        identity = json.dumps(
            {
                "exchange": exchange_name,
                "symbol": payment.get("symbol"),
                "amount": payment.get("amount"),
                "event_time_ms": payment.get("event_time_ms"),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        movement_id = hashlib.sha256(identity.encode()).hexdigest()[:24]
    return f"funding:{exchange_name}:{movement_id}"


def _active_pair_id(open_pairs: dict[str, Any], symbol: str, event_time_ms: int) -> str | None:
    for pair_id, pair in open_pairs.items():
        if str(getattr(pair, "symbol", "")).upper() != symbol.upper():
            continue
        entry_time_ms = int(getattr(pair, "entry_time_ms", 0) or 0)
        if entry_time_ms <= event_time_ms and str(getattr(pair, "status", "")) not in {"closed", "failed"}:
            return str(pair_id)
    return _PAIR_ATTRIBUTION_REGISTRY.pair_id_at(symbol, event_time_ms)


def _store_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn = get_db_connection()
    try:
        for row in rows:
            insert_cashflow_row(conn, row)
    finally:
        conn.close()
    return len(rows)


async def sync_funding_cashflows_once(
    *,
    exchanges: dict[str, Any],
    open_pairs: dict[str, Any],
    start_time_ms: int,
    end_time_ms: int,
    record_event: Callable[..., None] | None = None,
) -> tuple[int, int]:
    if not db_persistence_enabled():
        return (0, end_time_ms)
    context = current_account_context()
    rows: list[dict[str, Any]] = []
    newest_ms = start_time_ms
    for exchange_name, exchange in exchanges.items():
        fetch = getattr(exchange, "get_funding_payments", None)
        if not callable(fetch):
            continue
        try:
            payments = await fetch(
                start_time_ms=max(start_time_ms - 60_000, 0),
                end_time_ms=end_time_ms,
                limit=100,
            )
        except Exception as exc:
            if record_event is not None:
                record_event(
                    "funding_cashflow_sync_failed",
                    exchange=exchange_name,
                    error=repr(exc),
                )
            continue
        for payment in payments or []:
            symbol = str(payment.get("symbol") or "").upper()
            event_time_ms = int(payment.get("event_time_ms") or 0)
            if not symbol or event_time_ms <= 0:
                continue
            newest_ms = max(newest_ms, event_time_ms + 1)
            pair_id = _active_pair_id(open_pairs, symbol, event_time_ms)
            raw = {
                **context,
                "pair_id": pair_id,
                "symbol": symbol,
                "payment": payment.get("raw") or payment,
            }
            rows.append(
                {
                    "movement_key": _movement_key(exchange_name, payment),
                    "flow_type": "funding_payment",
                    "status": "completed",
                    "reconcile_status": "matched" if pair_id else "unattributed",
                    "asset_code": payment.get("asset_code") or "USDT",
                    "amount": float(payment.get("amount") or 0.0),
                    "fee_amount": 0.0,
                    "exchange_name": exchange_name,
                    "channel": "futures",
                    "event_time_ms": event_time_ms,
                    "memo": f"symbol={symbol} pair_id={pair_id or '-'}",
                    "raw_json": raw,
                }
            )
    stored = await asyncio.to_thread(_store_rows, rows) if rows else 0
    if record_event is not None:
        record_event(
            "funding_cashflow_synced",
            row_count=stored,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            newest_event_time_ms=newest_ms,
        )
    return (stored, max(newest_ms, end_time_ms - 60_000))


async def run_funding_cashflow_loop(
    *,
    exchanges: dict[str, Any],
    open_pairs: dict[str, Any],
    record_event: Callable[..., None] | None = None,
) -> None:
    interval_sec = max(float(os.getenv("STRATEGY_FUNDING_CASHFLOW_INTERVAL_SEC", "300") or 300), 60.0)
    cursor_ms = _now_ms() - int(max(float(os.getenv("STRATEGY_FUNDING_CASHFLOW_BACKFILL_SEC", "86400") or 86400), 0.0) * 1000)
    while True:
        end_time_ms = _now_ms()
        try:
            _, cursor_ms = await sync_funding_cashflows_once(
                exchanges=exchanges,
                open_pairs=open_pairs,
                start_time_ms=cursor_ms,
                end_time_ms=end_time_ms,
                record_event=record_event,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if record_event is not None:
                record_event("funding_cashflow_sync_failed", exchange="all", error=repr(exc))
        await asyncio.sleep(interval_sec)
