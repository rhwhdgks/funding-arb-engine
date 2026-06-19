"""Orphan (unhedged) position guard.

When a hedge leg fails AND the rollback also fails, one leg of an arb pair is
left open without its counterpart — an orphaned, directional exposure. This
module records such symbols to a lock-protected JSON file so that:

  * the strategy refuses to re-enter the same symbol while it is orphaned
  * the ops console can surface a red warning for the operator
  * shutdown-flatten timeouts can mark whatever they could not close

Records are keyed by ``symbol + exchange``. A failed hedge can leave both
venues in different states, and collapsing those into one symbol-level row can
hide the actual leg that still needs cleanup.

It deliberately mirrors the lock/atomic-write pattern of
``src/ops/position_ownership.py`` (a proven file-based shared-state mechanism)
rather than introducing a new persistence strategy.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import time
from pathlib import Path
from typing import Any, Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("USDT", "")


def normalize_exchange(exchange: str) -> str:
    return str(exchange or "").strip().lower()


def _resolve_path(raw: str | Path | None) -> Path | None:
    if raw in (None, ""):
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def orphan_file_path(raw: str | Path | None = None) -> Path:
    explicit = _resolve_path(raw or os.getenv("ORPHAN_POSITION_FILE"))
    if explicit is not None:
        return explicit
    # Co-locate with entry_control.json so all runtime control files live together.
    entry_control = _resolve_path(os.getenv("STRATEGY_ENTRY_CONTROL_FILE"))
    if entry_control is not None:
        return entry_control.parent / "orphan_positions.json"
    return PROJECT_ROOT / "runtime" / "orphan_positions.json"


@contextlib.contextmanager
def _orphan_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _empty_state() -> dict[str, Any]:
    return {"version": 1, "updated_at_ms": now_ms(), "orphans": []}


def load_state(path: str | Path | None = None) -> dict[str, Any]:
    orphan_path = orphan_file_path(path)
    if not orphan_path.exists():
        return _empty_state()
    try:
        payload = json.loads(orphan_path.read_text(encoding="utf-8"))
    except Exception:
        return _empty_state()
    if not isinstance(payload, dict):
        return _empty_state()
    if not isinstance(payload.get("orphans"), list):
        payload["orphans"] = []
    return payload


def _write_state(payload: dict[str, Any], path: Path) -> None:
    payload = dict(payload)
    payload["version"] = 1
    payload["updated_at_ms"] = now_ms()
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def mark_orphan(
    *,
    symbol: str,
    exchange: str,
    qty: float,
    reason: str,
    path: str | Path | None = None,
) -> dict[str, Any]:
    """Record (or refresh) an orphaned, unhedged position leg."""
    orphan_path = orphan_file_path(path)
    symbol = normalize_symbol(symbol)
    exchange = normalize_exchange(exchange)
    current_ms = now_ms()
    with _orphan_lock(orphan_path):
        state = load_state(orphan_path)
        records = [r for r in state.get("orphans") or [] if isinstance(r, dict)]
        existing = next(
            (
                r
                for r in records
                if normalize_symbol(r.get("symbol", "")) == symbol
                and normalize_exchange(r.get("exchange", "")) == exchange
            ),
            None,
        )
        if existing is None:
            existing = {"symbol": symbol, "exchange": exchange, "created_at_ms": current_ms}
            records.append(existing)
        existing.update(
            {
                "symbol": symbol,
                "exchange": exchange,
                "qty": float(qty),
                "reason": str(reason),
                "updated_at_ms": current_ms,
            }
        )
        state["orphans"] = records
        _write_state(state, orphan_path)
        return dict(existing)


def clear_orphan(symbol: str, *, exchange: str | None = None, path: str | Path | None = None) -> bool:
    """Remove orphan records after they are reconciled/flattened.

    If ``exchange`` is omitted, every orphan leg for the symbol is removed.
    """
    orphan_path = orphan_file_path(path)
    symbol = normalize_symbol(symbol)
    exchange_key = normalize_exchange(exchange or "")
    with _orphan_lock(orphan_path):
        state = load_state(orphan_path)
        records = [r for r in state.get("orphans") or [] if isinstance(r, dict)]
        remaining = []
        for record in records:
            same_symbol = normalize_symbol(record.get("symbol", "")) == symbol
            same_exchange = not exchange_key or normalize_exchange(record.get("exchange", "")) == exchange_key
            if same_symbol and same_exchange:
                continue
            remaining.append(record)
        if len(remaining) == len(records):
            return False
        state["orphans"] = remaining
        _write_state(state, orphan_path)
        return True


def active_orphan_symbols(path: str | Path | None = None) -> set[str]:
    return {
        normalize_symbol(r.get("symbol", ""))
        for r in load_state(path).get("orphans") or []
        if isinstance(r, dict) and r.get("symbol")
    }


def active_orphans(path: str | Path | None = None) -> list[dict[str, Any]]:
    return [dict(r) for r in load_state(path).get("orphans") or [] if isinstance(r, dict) and r.get("symbol")]
