from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Awaitable

from src.strategy.logging import log_hedge_done, log_hedge_partial_fill, log_hedge_start


@dataclass
class HedgeExecutionResult:
    requested_qty: float
    filled_qty: float
    remaining_qty: float
    avg_price: float = 0.0


def emit_hedge_event(
    recorder: Callable[..., None] | None,
    event_type: str,
    **payload: Any,
) -> None:
    if recorder is None:
        return
    try:
        recorder(event_type, **payload)
    except Exception as exc:
        print(f"[strategy][hedge_event][warn] event_type={event_type} error={exc!r}")


async def execute_residual_hedge(
    *,
    stage_label: str,
    exchange_name: str,
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    config: Any,
    is_close: bool,
    eps: float,
    extract_position_qty: Callable[[Any], float],
    marketable_limit_price_with_bps: Callable[[Any, str, str, float], float],
    print_verbose: Callable[[Any, Any], None],
    use_reduce_only_market_close: Callable[[Any, float, str], bool],
    resolve_effective_order_qty: Callable[..., Awaitable[float]],
    extract_order_avg_price: Callable[[dict[str, Any] | None, float], float],
    wait_for_position_fill: Callable[..., Awaitable[float]],
    wait_for_position_close: Callable[..., Awaitable[float]],
    cancel_if_open: Callable[[Any, str, dict[str, Any] | None], Awaitable[None]],
    settle_grace_sec: Callable[[Any], float],
    record_order_event: Callable[..., None] | None = None,
) -> HedgeExecutionResult:
    hedged_qty = 0.0
    hedge_price_used = 0.0
    last_logged_hedged_qty = 0.0

    if target_qty <= eps:
        return HedgeExecutionResult(
            requested_qty=target_qty,
            filled_qty=0.0,
            remaining_qty=0.0,
            avg_price=0.0,
        )

    market_log_prefix = "close-hedge" if is_close else "hedge"
    market_log_text = "청산 헤지 시장가" if is_close else "헤지 시장가"
    requote_log_text = "청산 헤지 재호가" if is_close else "헤지 재호가"
    probe_reason_prefix = f"strategy_{stage_label}_hedge"
    position_waiter = wait_for_position_close if is_close else wait_for_position_fill
    fast_ack_enabled = bool(getattr(config, "hedge_fast_ack", False)) and stage_label == "entry" and not is_close

    def outstanding_qty() -> float:
        return max(target_qty - hedged_qty, 0.0)

    def log_progress(reason: str) -> None:
        nonlocal last_logged_hedged_qty
        if hedged_qty <= last_logged_hedged_qty + eps:
            return
        filled_delta = hedged_qty - last_logged_hedged_qty
        remaining_qty = outstanding_qty()
        post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        if remaining_qty > eps:
            log_hedge_partial_fill(
                symbol=symbol,
                exchange=exchange_name,
                side=side,
                filled_delta=filled_delta,
                filled_total=hedged_qty,
                target_qty=target_qty,
                remaining_qty=remaining_qty,
                stage_label=stage_label,
                is_close=is_close,
                reason=reason,
            )
        if not is_close and stage_label == "entry":
            emit_hedge_event(
                record_order_event,
                "entry_hedge_fill_progress",
                exchange=exchange_name,
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                filled_delta=filled_delta,
                filled_total=hedged_qty,
                remaining_qty=remaining_qty,
                pre_qty=pre_qty,
                post_qty=post_qty,
                position_delta=hedged_qty,
                reason=reason,
                fast_ack=(reason == "market_fast_ack"),
            )
        last_logged_hedged_qty = hedged_qty

    log_hedge_start(
        symbol=symbol,
        exchange=exchange_name,
        side=side,
        target_qty=target_qty,
        stage_label=stage_label,
        is_close=is_close,
    )

    if config.hedge_market_immediate:
        remaining_qty = outstanding_qty()
        if remaining_qty > eps:
            print_verbose(
                config,
                f"[strategy][{market_log_prefix}][market] {market_log_text} exchange={exchange_name} side={side} "
                f"qty={remaining_qty:.6f}",
            )
            market_kwargs: dict[str, Any] = {"is_hedge": True}
            if fast_ack_enabled:
                market_kwargs["fast_ack"] = True
            if is_close:
                current_hedge_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
                market_kwargs["is_close"] = use_reduce_only_market_close(exchange, current_hedge_qty, side)
            market_result = await exchange.market_order(symbol, side, remaining_qty, **market_kwargs)
            if market_result and market_result.get("error"):
                print(f"[strategy][{market_log_prefix}][market][error] {market_log_text} 오류 result={market_result}")
            if fast_ack_enabled and market_result and not market_result.get("error"):
                hedged_qty = max(hedged_qty, remaining_qty)
                emit_hedge_event(
                    record_order_event,
                    "entry_hedge_fast_ack_assumed",
                    exchange=exchange_name,
                    symbol=symbol,
                    side=side,
                    target_qty=target_qty,
                    assumed_filled_qty=hedged_qty,
                    remaining_qty=outstanding_qty(),
                    order_id=market_result.get("order_id"),
                    client_order_id=market_result.get("client_order_id") or market_result.get("uuid"),
                    reason="market_order_accepted",
                )
                log_progress("market_fast_ack")
            else:
                await asyncio.sleep(config.action_settle_wait_sec)
                hedged_qty = max(
                    hedged_qty,
                    await resolve_effective_order_qty(
                        exchange=exchange,
                        symbol=symbol,
                        side=side,
                        pre_qty=pre_qty,
                        target_qty=target_qty,
                        order_result=market_result,
                        is_close=is_close,
                        reason=f"{probe_reason_prefix}_market",
                    ),
                )
                log_progress("market")
            hedge_price_used = extract_order_avg_price(market_result, hedge_price_used)
        if outstanding_qty() <= eps:
            log_hedge_done(
                symbol=symbol,
                exchange=exchange_name,
                filled_qty=hedged_qty,
                remaining_qty=outstanding_qty(),
                stage_label=stage_label,
                is_close=is_close,
            )
        return HedgeExecutionResult(
            requested_qty=target_qty,
            filled_qty=hedged_qty,
            remaining_qty=outstanding_qty(),
            avg_price=hedge_price_used,
        )

    for attempt in range(config.hedge_requote_count + 1):
        remaining_qty = outstanding_qty()
        if remaining_qty <= eps:
            break
        hedge_price = marketable_limit_price_with_bps(exchange, symbol, side, config.close_aggressive_bps)
        if hedge_price <= 0:
            break
        hedge_price_used = hedge_price
        print_verbose(
            config,
            f"[strategy][{market_log_prefix}] {requote_log_text} attempt={attempt + 1} exchange={exchange_name} side={side} "
            f"qty={remaining_qty:.6f} price={hedge_price}",
        )
        hedge_result = await exchange.order(symbol, side, remaining_qty, hedge_price, is_hedge=True)
        if hedge_result and not hedge_result.get("error"):
            await position_waiter(
                exchange,
                symbol,
                side,
                pre_qty,
                target_qty,
                config.hedge_requote_timeout_sec,
            )
            hedged_qty = max(
                hedged_qty,
                await resolve_effective_order_qty(
                    exchange=exchange,
                    symbol=symbol,
                    side=side,
                    pre_qty=pre_qty,
                    target_qty=target_qty,
                    order_result=hedge_result,
                    is_close=is_close,
                    reason=f"{probe_reason_prefix}_probe_before_cancel",
                ),
            )
            log_progress("probe_before_cancel")
            if outstanding_qty() > eps:
                await cancel_if_open(exchange, symbol, hedge_result)
                await asyncio.sleep(config.action_settle_wait_sec)
                hedged_qty = max(
                    hedged_qty,
                    await resolve_effective_order_qty(
                        exchange=exchange,
                        symbol=symbol,
                        side=side,
                        pre_qty=pre_qty,
                        target_qty=target_qty,
                        order_result=hedge_result,
                        is_close=is_close,
                        reason=f"{probe_reason_prefix}_after_cancel",
                    ),
                )
                log_progress("after_cancel_probe")
                if outstanding_qty() > eps:
                    hedged_qty = max(
                        hedged_qty,
                        await position_waiter(
                            exchange,
                            symbol,
                            side,
                            pre_qty,
                            target_qty,
                            settle_grace_sec(config),
                        ),
                    )
                    log_progress("final_settle")

    if outstanding_qty() <= eps:
        log_hedge_done(
            symbol=symbol,
            exchange=exchange_name,
            filled_qty=hedged_qty,
            remaining_qty=outstanding_qty(),
            stage_label=stage_label,
            is_close=is_close,
        )
    return HedgeExecutionResult(
        requested_qty=target_qty,
        filled_qty=hedged_qty,
        remaining_qty=outstanding_qty(),
        avg_price=hedge_price_used,
    )
