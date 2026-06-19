from typing import Any


SUPPORTED_EXCHANGES = {"binance", "gateio", "okx"}

EXPECTED_PUBLIC_METHODS = (
    "run",
    "stop",
    "order",
    "market_order",
    "cancel_order",
    "cancel_all_orders_for_symbol",
    "get_account_snapshot",
    "get_symbol_snapshot",
    "get_open_order_map",
    "get_trades",
    "get_trade_history",
    "get_ohlcv_snapshot",
    "preload_funding_history",
    "bootstrap_symbol",
)


def load_exchange_class(exchange_name: str):
    normalized = exchange_name.strip().lower()
    if normalized == "binance":
        from src.binance_f_module.exchange import Exchange

        return Exchange
    if normalized == "gateio":
        from src.gateio_f_module.exchange import Exchange

        return Exchange
    if normalized == "okx":
        from src.okx_f_module.exchange import Exchange

        return Exchange
    raise ValueError(f"unsupported exchange: {exchange_name}")


def build_exchange(exchange_name: str, symbols: list[str], **overrides: Any):
    Exchange = load_exchange_class(exchange_name)
    kwargs = {
        "ticker_list": symbols,
        "account_interval": 0.5,
        "order_interval": 1.0,
        "open_orders_interval": 3.0,
        "funding_interval": 10.0,
        "health_interval": 0.5,
    }
    kwargs.update(overrides)
    return Exchange(**kwargs)


def validate_exchange_interface(exchange_or_class: Any) -> dict[str, Any]:
    missing = [method for method in EXPECTED_PUBLIC_METHODS if not hasattr(exchange_or_class, method)]
    return {
        "ok": len(missing) == 0,
        "missing_methods": missing,
        "expected_methods": list(EXPECTED_PUBLIC_METHODS),
    }


# Top-level keys the strategy dereferences on get_symbol_snapshot().
REQUIRED_SYMBOL_SNAPSHOT_KEYS = (
    "orderbook",
    "position",
    "funding",
    "health",
    "account_last_sync_ms",
    "open_orders",
)
# Nested keys that must be dicts (strategy reads sub-fields off them).
REQUIRED_DICT_SNAPSHOT_KEYS = ("orderbook", "position", "funding", "health")


def validate_exchange_snapshot_shape(exchange: Any, sample_symbol: str) -> dict[str, Any]:
    """Verify a live exchange instance returns the snapshot SHAPE the strategy
    relies on. Method-presence (validate_exchange_interface) only proves the
    methods exist; this proves they return usable structures, so a new exchange
    fails loudly at bootstrap rather than at runtime mid-trade."""
    issues: list[str] = []

    try:
        snapshot = exchange.get_symbol_snapshot(sample_symbol)
    except Exception as exc:
        return {"ok": False, "issues": [f"get_symbol_snapshot raised: {exc!r}"]}

    if not isinstance(snapshot, dict):
        return {"ok": False, "issues": ["get_symbol_snapshot did not return a dict"]}

    for key in REQUIRED_SYMBOL_SNAPSHOT_KEYS:
        if key not in snapshot:
            issues.append(f"symbol_snapshot missing key: {key}")
    for key in REQUIRED_DICT_SNAPSHOT_KEYS:
        if key in snapshot and not isinstance(snapshot[key], dict):
            issues.append(f"symbol_snapshot[{key}] is not a dict")

    health = snapshot.get("health")
    if isinstance(health, dict):
        for key in ("market_ready", "account_ready"):
            if key not in health and key not in snapshot:
                issues.append(f"health missing key: {key}")

    try:
        account = exchange.get_account_snapshot()
        if not isinstance(account, dict):
            issues.append("get_account_snapshot did not return a dict")
    except Exception as exc:
        issues.append(f"get_account_snapshot raised: {exc!r}")

    return {"ok": not issues, "issues": issues, "sample_symbol": sample_symbol}
