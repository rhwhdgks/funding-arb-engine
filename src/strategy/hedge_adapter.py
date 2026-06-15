from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

from src.strategy import order_price_policy
from src.strategy.config import load_strategy_config
from src.strategy.execution import (
    cancel_if_open as execution_cancel_if_open,
    extract_order_avg_price,
    extract_order_uuid,
    resolve_effective_order_qty as execution_resolve_effective_order_qty,
    settle_grace_sec,
    wait_for_position_close as execution_wait_for_position_close,
    wait_for_position_fill as execution_wait_for_position_fill,
)
from src.strategy.hedge import HedgeExecutionResult, execute_residual_hedge


EPS = 1e-9


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def extract_position_qty(snapshot: dict[str, Any]) -> float:
    position = snapshot.get("position", {}) or {}
    for key in ("qty", "position_amt", "size"):
        if key in position:
            return _safe_float(position.get(key), 0.0)
    return 0.0


def position_fill_delta(pre_qty: float, post_qty: float, side: str) -> float:
    side = side.lower()
    if side == "buy":
        return max(post_qty - pre_qty, 0.0)
    if side == "sell":
        return max(pre_qty - post_qty, 0.0)
    return 0.0


def position_close_delta(pre_qty: float, post_qty: float, side: str) -> float:
    side = side.lower()
    if side == "sell":
        return max(pre_qty - post_qty, 0.0)
    if side == "buy":
        return max(post_qty - pre_qty, 0.0)
    return 0.0


def print_verbose(config: Any, *args: Any, **kwargs: Any) -> None:
    if str(getattr(config, "stdout_log_style", "verbose")).strip().lower() != "simple":
        print(*args, **kwargs)


async def refresh_symbol_state(exchange: Any, symbol: str, reason: str) -> None:
    scheduler = getattr(exchange, "account_refresh_scheduler", None)
    refresh_now = getattr(scheduler, "refresh_now", None)
    if callable(refresh_now):
        try:
            refreshed = await refresh_now(reason, priority="urgent")
            if refreshed:
                return
        except Exception as exc:
            print(f"[strategy][hedge_adapter][refresh][warn] symbol={symbol} reason={reason} error={exc!r}")

    requester = getattr(exchange, "request_account_refresh", None)
    if callable(requester):
        try:
            requester(reason, priority="urgent", symbol=symbol)
        except Exception as exc:
            print(f"[strategy][hedge_adapter][refresh_request][warn] symbol={symbol} reason={reason} error={exc!r}")
    await asyncio.sleep(0.1)


async def wait_for_position_fill(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
) -> float:
    return await execution_wait_for_position_fill(
        exchange,
        symbol,
        side,
        pre_qty,
        target_qty,
        timeout_sec,
        extract_position_qty=extract_position_qty,
        position_fill_delta=position_fill_delta,
        eps=EPS,
    )


async def wait_for_position_close(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
) -> float:
    return await execution_wait_for_position_close(
        exchange,
        symbol,
        side,
        pre_qty,
        target_qty,
        timeout_sec,
        extract_position_qty=extract_position_qty,
        position_close_delta=position_close_delta,
        eps=EPS,
    )


async def resolve_effective_order_qty(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    order_result: dict[str, Any] | None,
    is_close: bool,
    reason: str,
) -> float:
    return await execution_resolve_effective_order_qty(
        exchange=exchange,
        symbol=symbol,
        side=side,
        pre_qty=pre_qty,
        target_qty=target_qty,
        order_result=order_result,
        is_close=is_close,
        reason=reason,
        refresh_symbol_state=refresh_symbol_state,
        extract_position_qty=extract_position_qty,
        position_close_delta=position_close_delta,
        position_fill_delta=position_fill_delta,
        eps=EPS,
    )


async def cancel_if_open(exchange: Any, symbol: str, order_result: dict[str, Any] | None) -> Any:
    return await execution_cancel_if_open(
        exchange,
        symbol,
        order_result,
        extract_order_uuid=extract_order_uuid,
    )


def use_reduce_only_market_close(exchange: Any, current_qty: float, side: str) -> bool:
    return True


def _console_hedge_config(wait_sec: float | None = None) -> Any:
    config = load_strategy_config()
    if wait_sec is None or wait_sec <= 0:
        return config
    return replace(
        config,
        action_settle_wait_sec=max(config.action_settle_wait_sec, min(float(wait_sec), 3.0)),
    )


async def execute_strategy_style_hedge(
    *,
    exchange_name: str,
    exchange: Any,
    symbol: str,
    side: str,
    target_qty: float,
    is_close: bool,
    stage_label: str = "ops_console",
    wait_sec: float | None = None,
) -> HedgeExecutionResult:
    config = _console_hedge_config(wait_sec)
    pre_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return await execute_residual_hedge(
        stage_label=stage_label,
        exchange_name=exchange_name,
        exchange=exchange,
        symbol=symbol,
        side=side,
        pre_qty=pre_qty,
        target_qty=abs(float(target_qty or 0.0)),
        config=config,
        is_close=is_close,
        eps=EPS,
        extract_position_qty=extract_position_qty,
        marketable_limit_price_with_bps=order_price_policy.marketable_limit_price_with_bps,
        print_verbose=print_verbose,
        use_reduce_only_market_close=use_reduce_only_market_close,
        resolve_effective_order_qty=resolve_effective_order_qty,
        extract_order_avg_price=extract_order_avg_price,
        wait_for_position_fill=wait_for_position_fill,
        wait_for_position_close=wait_for_position_close,
        cancel_if_open=cancel_if_open,
        settle_grace_sec=settle_grace_sec,
        record_order_event=None,
    )
