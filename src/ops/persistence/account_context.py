from __future__ import annotations

import os


DEFAULT_LOGIC_NAME = "funding fee"


def resolve_account_label() -> str:
    label = (os.getenv("STRATEGY_ACCOUNT_LABEL") or os.getenv("ACCOUNT_LABEL") or "").strip().lower()
    if label:
        return label
    return "test" if os.getenv("STRATEGY_TEST_ENV", "").strip().lower() in {"1", "true", "yes", "y", "on"} else "real"


def resolve_logic_name() -> str:
    logic = (os.getenv("STRATEGY_LOGIC") or os.getenv("LOGIC") or "").strip()
    return logic or DEFAULT_LOGIC_NAME


def current_account_context() -> dict[str, str]:
    return {
        "account_label": resolve_account_label(),
        "logic": resolve_logic_name(),
    }
