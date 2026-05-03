from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class EntryControlState:
    entry_paused: bool
    reason: str
    updated_at_ms: int | None = None
    updated_by: str = ""
    source_path: str = ""
    error: str = ""


@dataclass(frozen=True)
class EntryControlDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()
    state: EntryControlState | None = None


def normalize_pause_reason(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_.:-]+", "_", text)
    return text.strip("_") or "manual_pause"


class EntryControlStore:
    def __init__(self, path: str | Path, *, fail_closed_on_error: bool = True) -> None:
        self.path = Path(path).expanduser()
        self.fail_closed_on_error = fail_closed_on_error

    def read_state(self) -> EntryControlState:
        source_path = str(self.path)
        if not self.path.exists():
            return EntryControlState(entry_paused=False, reason="", source_path=source_path)

        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            return EntryControlState(
                entry_paused=bool(self.fail_closed_on_error),
                reason="entry_control_read_error",
                source_path=source_path,
                error=str(exc),
            )

        if not isinstance(payload, dict):
            return EntryControlState(
                entry_paused=bool(self.fail_closed_on_error),
                reason="entry_control_invalid_payload",
                source_path=source_path,
                error="payload is not an object",
            )

        paused = bool(payload.get("entry_paused", payload.get("paused", False)))
        reason = normalize_pause_reason(payload.get("reason") if paused else "")
        return EntryControlState(
            entry_paused=paused,
            reason=reason if paused else "",
            updated_at_ms=_safe_int(payload.get("updated_at_ms")),
            updated_by=str(payload.get("updated_by") or ""),
            source_path=source_path,
        )

    def evaluate(self) -> EntryControlDecision:
        state = self.read_state()
        if not state.entry_paused:
            return EntryControlDecision(allowed=True, state=state)

        reason = state.reason or "manual_pause"
        if state.error:
            reason = f"{reason}:{normalize_pause_reason(state.error)[:80]}"
        return EntryControlDecision(
            allowed=False,
            reasons=(f"entry_paused:{reason}",),
            state=state,
        )


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def apply_entry_control_blocks(candidates: Iterable[Any], decision: EntryControlDecision) -> list[Any]:
    result = list(candidates)
    if decision.allowed:
        return result

    for candidate in result:
        block_reasons = getattr(candidate, "block_reasons", None)
        if block_reasons is None:
            continue
        for reason in decision.reasons:
            if reason not in block_reasons:
                block_reasons.append(reason)
        setattr(candidate, "can_enter", False)
    return result
