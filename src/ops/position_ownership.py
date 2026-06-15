from __future__ import annotations

import contextlib
import fcntl
import json
import os
import time
from pathlib import Path
from typing import Any, Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ACTIVE_STATUSES = {"open", "partial"}
MANUAL_OWNERSHIPS = {"manual_only"}


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("USDT", "")


def normalize_exchange(exchange: str) -> str:
    return str(exchange or "").strip().lower()


def resolve_path(raw: str | Path | None) -> Path | None:
    if raw in (None, ""):
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def ownership_file_path(raw: str | Path | None = None) -> Path:
    explicit = resolve_path(raw or os.getenv("POSITION_OWNERSHIP_FILE"))
    if explicit is not None:
        return explicit

    entry_control = resolve_path(os.getenv("STRATEGY_ENTRY_CONTROL_FILE"))
    if entry_control is not None:
        return entry_control.parent / "position_ownership.json"
    return PROJECT_ROOT / "runtime" / "position_ownership.json"


@contextlib.contextmanager
def ownership_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def empty_state() -> dict[str, Any]:
    return {"version": 1, "updated_at_ms": now_ms(), "positions": []}


def load_state(path: str | Path | None = None) -> dict[str, Any]:
    ownership_path = ownership_file_path(path)
    if not ownership_path.exists():
        return empty_state()
    try:
        payload = json.loads(ownership_path.read_text(encoding="utf-8"))
    except Exception:
        return empty_state()
    if not isinstance(payload, dict):
        return empty_state()
    positions = payload.get("positions")
    if not isinstance(positions, list):
        payload["positions"] = []
    payload.setdefault("version", 1)
    payload.setdefault("updated_at_ms", now_ms())
    return payload


def write_state(payload: dict[str, Any], path: str | Path | None = None) -> None:
    ownership_path = ownership_file_path(path)
    ownership_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["version"] = 1
    payload["updated_at_ms"] = now_ms()
    tmp_path = ownership_path.with_name(f".{ownership_path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(ownership_path)


def manual_position_key(symbol: str) -> str:
    return f"manual:{normalize_symbol(symbol)}"


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def active_records(payload: dict[str, Any] | None = None, *, path: str | Path | None = None) -> list[dict[str, Any]]:
    state = payload if payload is not None else load_state(path)
    current_ms = now_ms()
    records: list[dict[str, Any]] = []
    for record in state.get("positions") or []:
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "").strip().lower()
        ownership = str(record.get("ownership") or "").strip().lower()
        expires_at_ms = int(_safe_float(record.get("expires_at_ms")))
        if status not in ACTIVE_STATUSES or ownership not in MANUAL_OWNERSHIPS:
            continue
        if expires_at_ms > 0 and expires_at_ms <= current_ms:
            continue
        records.append(record)
    return records


def active_manual_symbols(path: str | Path | None = None) -> set[str]:
    return {normalize_symbol(record.get("symbol", "")) for record in active_records(path=path) if record.get("symbol")}


def active_manual_exchange_symbols(path: str | Path | None = None) -> set[tuple[str, str]]:
    locked: set[tuple[str, str]] = set()
    for record in active_records(path=path):
        symbol = normalize_symbol(record.get("symbol", ""))
        for exchange in (record.get("positions_by_exchange") or {}).keys():
            locked.add((normalize_exchange(exchange), symbol))
    return locked


def _normalize_positions(positions_by_exchange: dict[str, Any]) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for exchange, qty in (positions_by_exchange or {}).items():
        name = normalize_exchange(exchange)
        if not name:
            continue
        normalized[name] = _safe_float(qty)
    return normalized


def upsert_manual_position(
    *,
    symbol: str,
    positions_by_exchange: dict[str, Any],
    source: str = "ops_console",
    note: str = "",
    force_open: bool = False,
    path: str | Path | None = None,
) -> dict[str, Any]:
    ownership_path = ownership_file_path(path)
    symbol = normalize_symbol(symbol)
    positions = _normalize_positions(positions_by_exchange)
    status = "open" if force_open or any(abs(qty) > 0 for qty in positions.values()) else "closed"
    current_ms = now_ms()
    key = manual_position_key(symbol)

    with ownership_lock(ownership_path):
        state = load_state(ownership_path)
        records = [record for record in state.get("positions") or [] if isinstance(record, dict)]
        existing = next((record for record in records if record.get("position_key") == key), None)
        if existing is None:
            existing = {
                "position_key": key,
                "created_at_ms": current_ms,
            }
            records.append(existing)

        existing.update(
            {
                "position_key": key,
                "source": source,
                "ownership": "manual_only",
                "symbol": symbol,
                "status": status,
                "positions_by_exchange": positions,
                "pending_manual_order": bool(force_open and not any(abs(qty) > 0 for qty in positions.values())),
                "updated_at_ms": current_ms,
            }
        )
        if note:
            existing["note"] = note
        if status == "closed":
            existing["closed_at_ms"] = current_ms
        else:
            existing.pop("closed_at_ms", None)

        state["positions"] = records
        write_state(state, ownership_path)
        return dict(existing)


def close_manual_position(
    *,
    symbol: str,
    source: str = "ops_console",
    note: str = "",
    path: str | Path | None = None,
) -> dict[str, Any]:
    return upsert_manual_position(
        symbol=symbol,
        positions_by_exchange={},
        source=source,
        note=note,
        path=path,
    )
