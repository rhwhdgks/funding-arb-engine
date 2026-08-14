from __future__ import annotations

import os
import time
from typing import Any, Callable

from src.core.order_intent import OrderIntentJournal
from src.ops.persistence.event_hooks import schedule_order_state_event


EventSink = Callable[..., None]
PositionQtyReader = Callable[[dict[str, Any]], float]
OrderStateSink = Callable[..., Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


async def reconcile_unresolved_order_intents(
    journal: OrderIntentJournal,
    exchanges: dict[str, Any],
    *,
    position_qty: PositionQtyReader,
    record_event: EventSink,
    unknown_grace_sec: float | None = None,
    record_order_state: OrderStateSink = schedule_order_state_event,
) -> list[dict[str, Any]]:
    """Resolve durable intents without depending on strategy orchestration."""
    grace_sec = max(
        float(
            unknown_grace_sec
            if unknown_grace_sec is not None
            else os.getenv("STRATEGY_ORDER_INTENT_UNKNOWN_GRACE_SEC", "30") or 30
        ),
        1.0,
    )
    before = journal.unresolved()
    for intent in before:
        exchange_name = str(intent.get("exchange") or "").lower()
        symbol = str(intent.get("symbol") or "").upper()
        exchange = exchanges.get(exchange_name)
        get_status = getattr(exchange, "get_order_status", None) if exchange is not None else None
        if not exchange or not symbol or not callable(get_status):
            continue
        order_ref = intent.get("client_order_id") or intent.get("order_id")
        if not order_ref:
            continue
        try:
            status = await get_status(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "uuid": order_ref,
                    "client_order_id": intent.get("client_order_id"),
                    "order_id": intent.get("order_id"),
                }
            )
            if not isinstance(status, dict):
                continue
            record_order_state(
                exchange_name,
                symbol,
                status,
                prefer_live=True,
                event_source="startup_intent_reconcile",
            )
            normalized_state = str(status.get("state") or "").strip().lower()
            normalized_status = str(status.get("status") or "").strip().upper()
            if normalized_state != "not_exist" and normalized_status not in {"CANCELED", "CANCELLED", "EXPIRED"}:
                continue

            latest = next(
                (row for row in journal.unresolved() if row.get("intent_id") == intent.get("intent_id")),
                intent,
            )
            first_unknown_ms = int(latest.get("first_unknown_ms") or latest.get("created_ms") or _now_ms())
            snapshot = exchange.get_symbol_snapshot(symbol) or {}
            snapshot_orders = snapshot.get("open_orders") or []
            target_refs = {
                str(value)
                for value in (intent.get("client_order_id"), intent.get("order_id"))
                if value not in (None, "")
            }
            matching_open = any(
                target_refs
                & {
                    str(value)
                    for value in (row.get("client_order_id"), row.get("order_id"), row.get("uuid"))
                    if value not in (None, "")
                }
                for row in snapshot_orders
                if isinstance(row, dict)
            )
            if (
                not matching_open
                and abs(position_qty(snapshot)) <= 1e-9
                and _now_ms() - first_unknown_ms >= int(grace_sec * 1000)
            ):
                journal.resolve(
                    str(intent["intent_id"]),
                    state="CANCELLED",
                    note="reconcile_missing_after_grace_and_account_flat",
                )
        except Exception as exc:
            record_event(
                "order_intent_startup_reconcile_error",
                intent_id=intent.get("intent_id"),
                exchange=exchange_name,
                symbol=symbol,
                error=repr(exc),
            )

    remaining = journal.unresolved()
    record_event(
        "order_intent_startup_reconciled",
        before_count=len(before),
        remaining_count=len(remaining),
        remaining=[
            {
                "intent_id": row.get("intent_id"),
                "exchange": row.get("exchange"),
                "symbol": row.get("symbol"),
                "state": row.get("state"),
            }
            for row in remaining[:20]
        ],
    )
    return remaining
