from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class IntentLock:
    intent_id: str
    exchange_name: str
    symbol: str
    side: str
    reduce_only: bool
    created_ms: int = field(default_factory=_now_ms)
    last_update_ms: int = field(default_factory=_now_ms)
    state: str = "inflight"   # inflight | cancel_pending | terminal
    note: str = ""


class ActionGate:
    """
    Lightweight in-memory gate.
    목적:
    1) 같은 intent 중복 발사 방지
    2) cancel 이후 terminal 확인 전 재주문 방지
    """

    def __init__(self, *, stale_timeout_sec: float = 30.0):
        self._locks_by_intent: dict[str, IntentLock] = {}
        self._locks_by_slot: dict[str, str] = {}
        self._stale_timeout_ms = int(stale_timeout_sec * 1000)

    @staticmethod
    def make_slot(exchange_name: str, symbol: str, side: str, reduce_only: bool) -> str:
        return f"{exchange_name.lower()}:{symbol.upper()}:{side.lower()}:{int(bool(reduce_only))}"

    def _gc(self) -> None:
        now_ms = _now_ms()
        delete_intents: list[str] = []

        for intent_id, lock in self._locks_by_intent.items():
            if lock.state == "terminal":
                delete_intents.append(intent_id)
                continue
            if (now_ms - lock.last_update_ms) > self._stale_timeout_ms:
                delete_intents.append(intent_id)

        for intent_id in delete_intents:
            lock = self._locks_by_intent.pop(intent_id, None)
            if lock is None:
                continue
            slot = self.make_slot(lock.exchange_name, lock.symbol, lock.side, lock.reduce_only)
            current_intent = self._locks_by_slot.get(slot)
            if current_intent == intent_id:
                self._locks_by_slot.pop(slot, None)

    def try_acquire(
        self,
        *,
        intent_id: str,
        exchange_name: str,
        symbol: str,
        side: str,
        reduce_only: bool,
        note: str = "",
    ) -> tuple[bool, str]:
        self._gc()
        slot = self.make_slot(exchange_name, symbol, side, reduce_only)

        existing_intent = self._locks_by_slot.get(slot)
        if existing_intent is not None and existing_intent != intent_id:
            existing = self._locks_by_intent.get(existing_intent)
            if existing is not None and existing.state in {"inflight", "cancel_pending"}:
                return (False, f"slot_locked:{slot}:{existing.state}")

        existing = self._locks_by_intent.get(intent_id)
        if existing is not None and existing.state in {"inflight", "cancel_pending"}:
            return (False, f"intent_locked:{intent_id}:{existing.state}")

        lock = IntentLock(
            intent_id=intent_id,
            exchange_name=exchange_name,
            symbol=symbol,
            side=side,
            reduce_only=reduce_only,
            note=note,
        )
        self._locks_by_intent[intent_id] = lock
        self._locks_by_slot[slot] = intent_id
        return (True, "ok")

    def mark_inflight(self, intent_id: str, note: str = "") -> None:
        lock = self._locks_by_intent.get(intent_id)
        if lock is None:
            return
        lock.state = "inflight"
        lock.note = note or lock.note
        lock.last_update_ms = _now_ms()

    def mark_cancel_pending(self, intent_id: str, note: str = "") -> None:
        lock = self._locks_by_intent.get(intent_id)
        if lock is None:
            return
        lock.state = "cancel_pending"
        lock.note = note or lock.note
        lock.last_update_ms = _now_ms()

    def release(self, intent_id: str, note: str = "") -> None:
        lock = self._locks_by_intent.get(intent_id)
        if lock is None:
            return
        lock.state = "terminal"
        lock.note = note or lock.note
        lock.last_update_ms = _now_ms()
        slot = self.make_slot(lock.exchange_name, lock.symbol, lock.side, lock.reduce_only)
        current_intent = self._locks_by_slot.get(slot)
        if current_intent == intent_id:
            self._locks_by_slot.pop(slot, None)

    def touch(self, intent_id: str, note: str = "") -> None:
        lock = self._locks_by_intent.get(intent_id)
        if lock is None:
            return
        if note:
            lock.note = note
        lock.last_update_ms = _now_ms()

    def get_state(self, intent_id: str) -> str | None:
        lock = self._locks_by_intent.get(intent_id)
        return None if lock is None else lock.state

    def is_cancel_pending(self, *, exchange_name: str, symbol: str, side: str, reduce_only: bool) -> bool:
        self._gc()
        slot = self.make_slot(exchange_name, symbol, side, reduce_only)
        intent_id = self._locks_by_slot.get(slot)
        if intent_id is None:
            return False
        lock = self._locks_by_intent.get(intent_id)
        return lock is not None and lock.state == "cancel_pending"

    def snapshot(self) -> dict[str, Any]:
        self._gc()
        return {
            intent_id: {
                "exchange_name": lock.exchange_name,
                "symbol": lock.symbol,
                "side": lock.side,
                "reduce_only": lock.reduce_only,
                "state": lock.state,
                "created_ms": lock.created_ms,
                "last_update_ms": lock.last_update_ms,
                "note": lock.note,
            }
            for intent_id, lock in self._locks_by_intent.items()
        }