import os
from typing import Literal


def _is_blank(value: str | None) -> bool:
    return value is None or str(value).strip() == ""


def validate_common_env() -> list[str]:
    errors: list[str] = []
    warnings: list[str] = []

    symbol = os.getenv("SYMBOL", "").strip().upper()
    if not symbol:
        errors.append("SYMBOL is missing.")
    elif symbol.endswith("USDT") or "_" in symbol:
        warnings.append("SYMBOL should usually be base code like BTC, not BTCUSDT/BTC_USDT.")

    try:
        leverage = int(os.getenv("LEVERAGE", "3"))
        if leverage < 0:
            errors.append("LEVERAGE must be >= 0.")
    except Exception:
        errors.append("LEVERAGE must be an integer.")

    margin_mode = os.getenv("MARGIN_MODE", "").strip().upper()
    if margin_mode and margin_mode not in {"CROSS", "CROSSED", "ISOLATED", "ISOLATE"}:
        warnings.append(f"MARGIN_MODE={margin_mode} is unusual.")

    try:
        drift = int(os.getenv("MAX_SERVER_PC_DRIFT_MS", "500"))
        if drift <= 0:
            warnings.append("MAX_SERVER_PC_DRIFT_MS should usually be positive.")
    except Exception:
        errors.append("MAX_SERVER_PC_DRIFT_MS must be an integer.")

    return errors + [f"[WARN] {w}" for w in warnings]


def validate_binance_env() -> list[str]:
    errors: list[str] = []
    warnings: list[str] = []

    if _is_blank(os.getenv("BINANCE_ACCESS_KEY")):
        errors.append("BINANCE_ACCESS_KEY is missing.")
    if _is_blank(os.getenv("BINANCE_SECRET_KEY")):
        errors.append("BINANCE_SECRET_KEY is missing.")

    book_ticker = os.getenv("BINANCE_BOOK_TICKER_ENABLED", "0")
    if book_ticker not in {"0", "1"}:
        warnings.append("BINANCE_BOOK_TICKER_ENABLED should be 0 or 1.")

    for key in [
        "BINANCE_REQUEST_WEIGHT_LIMIT_1M",
        "BINANCE_ORDER_LIMIT_10S",
        "BINANCE_ORDER_LIMIT_1M",
    ]:
        value = os.getenv(key)
        if value:
            try:
                if int(value) <= 0:
                    warnings.append(f"{key} should be > 0.")
            except Exception:
                warnings.append(f"{key} should be an integer.")

    return errors + [f"[WARN] {w}" for w in warnings]


def validate_gate_env() -> list[str]:
    errors: list[str] = []
    warnings: list[str] = []

    if _is_blank(os.getenv("GATEIO_ACCESS_KEY")):
        errors.append("GATEIO_ACCESS_KEY is missing.")
    if _is_blank(os.getenv("GATEIO_SECRET_KEY")):
        errors.append("GATEIO_SECRET_KEY is missing.")

    settle = os.getenv("GATEIO_SETTLE", "usdt").lower()
    if settle not in {"usdt", "btc"}:
        warnings.append(f"GATEIO_SETTLE={settle} is unusual.")

    ws_url = os.getenv("GATE_FUTURES_WS_URL", "").strip()
    if ws_url and not ws_url.startswith("wss://"):
        warnings.append("GATE_FUTURES_WS_URL should usually start with wss://")

    for key in [
        "GATE_BOOK_TICKER_ENABLED",
        "GATE_WS_PING_INTERVAL_SEC",
        "GATE_WS_RECONNECT_DELAY_SEC",
        "GATE_ORDERBOOK_DEPTH_LEVEL",
        "GATE_ORDERBOOK_UPDATE_FREQ",
    ]:
        value = os.getenv(key)
        if key == "GATE_BOOK_TICKER_ENABLED" and value and value not in {"0", "1"}:
            warnings.append("GATE_BOOK_TICKER_ENABLED should be 0 or 1.")

    return errors + [f"[WARN] {w}" for w in warnings]


def validate_exchange_env(exchange: Literal["binance", "gate"]) -> None:
    msgs = validate_common_env()

    if exchange == "binance":
        msgs += validate_binance_env()
    elif exchange == "gate":
        msgs += validate_gate_env()
    else:
        raise ValueError(f"unknown exchange: {exchange}")

    hard_errors = [m for m in msgs if not m.startswith("[WARN]")]
    warns = [m for m in msgs if m.startswith("[WARN]")]

    for w in warns:
        print(w)

    if hard_errors:
        raise RuntimeError("Environment validation failed:\n- " + "\n- ".join(hard_errors))