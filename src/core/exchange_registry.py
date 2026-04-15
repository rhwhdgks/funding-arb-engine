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
