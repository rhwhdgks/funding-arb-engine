from __future__ import annotations

import os
from typing import Any

from src.core.exchange_registry import SUPPORTED_EXCHANGES, build_exchange as _build_exchange


DEFAULT_SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ARB", "LINK"]


def parse_csv_list(raw: str | None, fallback: list[str] | None = None) -> list[str]:
    if raw is None:
        return list(fallback or [])

    parts = [part.strip().upper() for part in str(raw).split(",") if part.strip()]
    if not parts:
        return list(fallback or [])
    if len(parts) == 1 and parts[0] == "ALL":
        return list(fallback or DEFAULT_SYMBOLS)

    ordered: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        ordered.append(part)
    return ordered


def build_default_symbols() -> list[str]:
    raw = (
        os.getenv("WEB_RUNTIME_SYMBOLS")
        or os.getenv("STRATEGY_SYMBOLS")
        or os.getenv("TARGET_SYMBOLS")
        or ",".join(DEFAULT_SYMBOLS)
    )
    return parse_csv_list(raw, fallback=DEFAULT_SYMBOLS)


def build_exchange(exchange_name: str, symbols: list[str], **overrides: Any):
    return _build_exchange(exchange_name, symbols, **overrides)
