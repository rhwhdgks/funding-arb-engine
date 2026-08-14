from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any


TERMINAL_STATES = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "COMPENSATED"}
STATE_RANK = {
    "CREATED": 0,
    "SUBMITTING": 1,
    "ACKED": 2,
    "CANCEL_REQUESTED": 3,
    "UNKNOWN": 3,
    "RECONCILING": 4,
    "PARTIAL": 5,
    "CANCELLED": 6,
    "REJECTED": 6,
    "EXPIRED": 6,
    "FILLED": 7,
    "COMPENSATED": 8,
}


class DuplicateOrderIntentError(RuntimeError):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _intent_ref(payload: dict[str, Any]) -> str:
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    for source in (payload, request):
        for key in ("client_order_id", "uuid", "order_id", "order_uid"):
            value = source.get(key)
            if value not in (None, ""):
                return str(value)
    return ""


def _state_for_event(event_type: str, payload: dict[str, Any]) -> str | None:
    normalized = event_type.strip().lower()
    status = _text(payload.get("status")).upper()
    state = _text(payload.get("state")).upper()
    if normalized == "order_submit":
        return "SUBMITTING"
    if normalized == "order_ack":
        return "ACKED"
    if normalized == "order_rejected" or status == "REJECTED":
        return "REJECTED"
    if normalized == "partial_fill":
        return "PARTIAL"
    if normalized == "fill" or status in {"FILLED", "FINISHED"}:
        return "FILLED"
    if normalized == "cancel_submit":
        return "CANCEL_REQUESTED"
    if normalized == "cancel_ack":
        return "CANCELLED" if status in {"CANCELED", "CANCELLED"} or state == "NOT_EXIST" else "CANCEL_REQUESTED"
    if normalized in {"order_canceled", "order_cancelled"} or status in {"CANCELED", "CANCELLED"}:
        return "CANCELLED"
    if normalized == "order_expired" or status == "EXPIRED":
        return "EXPIRED"
    if normalized == "order_missing" or state == "NOT_EXIST":
        return "UNKNOWN"
    if normalized == "order_live":
        return "ACKED"
    return None


class OrderIntentJournal:
    """Append-only order lifecycle journal used even when MariaDB is disabled."""

    def __init__(self, path: str | Path | None = None, *, fsync: bool | None = None) -> None:
        configured = path or os.getenv("STRATEGY_ORDER_INTENT_JOURNAL", "runtime/order_intents.jsonl")
        self.path = Path(configured).expanduser()
        if not self.path.is_absolute():
            self.path = Path(__file__).resolve().parents[2] / self.path
        self.fsync = (
            os.getenv("STRATEGY_ORDER_INTENT_FSYNC", "1").strip().lower() in {"1", "true", "yes", "on"}
            if fsync is None
            else bool(fsync)
        )
        self._lock = threading.Lock()
        self._latest: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (TypeError, ValueError):
                        continue
                    intent_id = _text(row.get("intent_id"))
                    if intent_id:
                        self._latest[intent_id] = row
        except OSError:
            return

    def record(
        self,
        *,
        exchange: str,
        symbol: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        order_ref = _intent_ref(payload)
        state = _state_for_event(event_type, payload)
        if not order_ref or state is None:
            return None
        intent_id = f"{exchange.lower()}:{order_ref}"
        with self._lock:
            previous = self._latest.get(intent_id, {})
            if event_type.strip().lower() == "order_submit" and previous:
                raise DuplicateOrderIntentError(
                    f"duplicate order intent refused intent_id={intent_id} state={previous.get('state')}"
                )
            previous_state = _text(previous.get("state")).upper()
            late_fill = state == "FILLED" and previous_state in {"CANCELLED", "REJECTED", "EXPIRED", "COMPENSATED"}
            if (
                previous_state
                and state != "FILLED"
                and STATE_RANK.get(state, -1) < STATE_RANK.get(previous_state, -1)
            ):
                state = previous_state
            row = {
                "ts_ms": int(payload.get("timestamp") or _now_ms()),
                "created_ms": int(previous.get("created_ms") or payload.get("timestamp") or _now_ms()),
                "intent_id": intent_id,
                "exchange": exchange.lower(),
                "symbol": symbol.upper(),
                "event_type": event_type,
                "state": state,
                "previous_state": previous_state or None,
                "late_fill": late_fill,
                "first_unknown_ms": (
                    int(previous.get("first_unknown_ms") or payload.get("timestamp") or _now_ms())
                    if state == "UNKNOWN"
                    else previous.get("first_unknown_ms")
                ),
                "order_id": payload.get("order_id"),
                "client_order_id": payload.get("client_order_id") or payload.get("uuid"),
                "side": payload.get("side"),
                "qty": payload.get("qty"),
                "executed_qty": payload.get("executed_qty"),
                "reduce_only": bool(payload.get("reduce_only") or payload.get("is_close")),
                "domain": (
                    payload.get("strategy_order_role")
                    or payload.get("owner")
                    or ("hedge" if payload.get("is_hedge") else "close" if payload.get("is_close") else "entry")
                ),
                "error": payload.get("error"),
            }
            encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
                if self.fsync:
                    os.fsync(handle.fileno())
            self._latest[intent_id] = row
        return row

    def unresolved(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(row)
                for row in self._latest.values()
                if _text(row.get("state")).upper() not in TERMINAL_STATES
            ]

    def resolve(self, intent_id: str, *, state: str, note: str) -> dict[str, Any]:
        normalized_state = state.strip().upper()
        if normalized_state not in TERMINAL_STATES:
            raise ValueError(f"non-terminal resolution state: {state}")
        previous = self._latest.get(intent_id, {})
        row = {
            **previous,
            "ts_ms": _now_ms(),
            "intent_id": intent_id,
            "event_type": "intent_resolved",
            "state": normalized_state,
            "note": note,
        }
        encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
                if self.fsync:
                    os.fsync(handle.fileno())
            self._latest[intent_id] = row
        return dict(row)

    def resolve_all_unresolved(self, *, state: str, note: str) -> list[dict[str, Any]]:
        return [
            self.resolve(str(row["intent_id"]), state=state, note=note)
            for row in self.unresolved()
        ]

    def snapshot(self) -> dict[str, Any]:
        unresolved = self.unresolved()
        return {
            "path": str(self.path),
            "intent_count": len(self._latest),
            "unresolved_count": len(unresolved),
            "unresolved": unresolved,
        }


_JOURNAL: OrderIntentJournal | None = None
_JOURNAL_LOCK = threading.Lock()


def active_order_intent_journal() -> OrderIntentJournal:
    global _JOURNAL
    if _JOURNAL is not None:
        return _JOURNAL
    with _JOURNAL_LOCK:
        if _JOURNAL is None:
            _JOURNAL = OrderIntentJournal()
    return _JOURNAL


def record_order_intent_event(
    exchange: str,
    symbol: str,
    event_type: str,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    return active_order_intent_journal().record(
        exchange=exchange,
        symbol=symbol,
        event_type=event_type,
        payload=payload,
    )
