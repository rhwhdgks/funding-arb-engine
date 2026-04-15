import asyncio
import os
from typing import Any, Awaitable, Callable

from src.ops.persistence.balance_report import build_balance_report
from src.ops.persistence.sync import SUPPORTED_EXCHANGES, build_exchange


def web_trading_enabled() -> bool:
    return os.getenv("ENABLE_WEB_TRADING", "0").strip().lower() in {"1", "true", "yes", "y", "on"}


def default_action_wait_ws_sec() -> float:
    return float(os.getenv("WEB_ACTION_WAIT_WS_SEC", os.getenv("WEB_ACTION_POST_WAIT_SEC", "0.0")))


def default_action_post_wait_sec() -> float:
    return float(os.getenv("WEB_ACTION_POST_WAIT_SEC", "1.0"))


async def execute_exchange_action(
    exchange_name: str,
    symbol: str,
    action_name: str,
    action: Callable[[Any], Awaitable[Any]],
    *,
    wait_ws_sec: float | None = None,
    post_wait_sec: float | None = None,
    runtime_manager: Any | None = None,
) -> dict[str, Any]:
    normalized_exchange = exchange_name.strip().lower()
    if normalized_exchange not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {exchange_name}")

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")

    wait_ws_sec = default_action_wait_ws_sec() if wait_ws_sec is None else float(wait_ws_sec)
    post_wait_sec = default_action_post_wait_sec() if post_wait_sec is None else float(post_wait_sec)

    settle_wait_sec = max(wait_ws_sec, post_wait_sec)

    if runtime_manager is not None:
        return await runtime_manager.execute_action(
            normalized_exchange,
            normalized_symbol,
            action_name,
            action,
            settle_wait_sec=settle_wait_sec,
        )

    exchange = build_exchange(normalized_exchange, [normalized_symbol])
    try:
        await exchange.run()
        await asyncio.sleep(wait_ws_sec)

        before_account = exchange.get_account_snapshot()
        before_symbol = exchange.get_symbol_snapshot(normalized_symbol)
        before_balance_report = build_balance_report(
            normalized_exchange,
            before_account,
            {normalized_symbol: before_symbol},
        )
        before_open_orders = exchange.get_open_order_map().get(normalized_symbol, [])

        action_result = await action(exchange)

        if post_wait_sec > 0:
            await asyncio.sleep(post_wait_sec)

        after_account = exchange.get_account_snapshot()
        after_symbol = exchange.get_symbol_snapshot(normalized_symbol)
        after_balance_report = build_balance_report(
            normalized_exchange,
            after_account,
            {normalized_symbol: after_symbol},
        )
        after_open_orders = exchange.get_open_order_map().get(normalized_symbol, [])

        return {
            "ok": True,
            "action": action_name,
            "exchange": normalized_exchange,
            "symbol": normalized_symbol,
            "action_result": action_result,
            "before": {
                "account": before_account,
                "balance_report": before_balance_report,
                "symbol": before_symbol,
                "open_orders": before_open_orders,
            },
            "after": {
                "account": after_account,
                "balance_report": after_balance_report,
                "symbol": after_symbol,
                "open_orders": after_open_orders,
            },
        }
    finally:
        await exchange.stop()
