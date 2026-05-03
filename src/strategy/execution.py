from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


async def wait_for_position_fill(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
    *,
    extract_position_qty: Callable[[dict[str, Any]], float],
    position_fill_delta: Callable[[float, float, str], float],
    eps: float,
) -> float:
    deadline = time.monotonic() + max(timeout_sec, 0.0)
    filled_qty = 0.0
    while time.monotonic() < deadline:
        post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        filled_qty = position_fill_delta(pre_qty, post_qty, side)
        if filled_qty + eps >= target_qty:
            return filled_qty
        await asyncio.sleep(0.1)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return position_fill_delta(pre_qty, post_qty, side)


async def wait_for_position_close(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
    *,
    extract_position_qty: Callable[[dict[str, Any]], float],
    position_close_delta: Callable[[float, float, str], float],
    eps: float,
) -> float:
    deadline = time.monotonic() + max(timeout_sec, 0.0)
    closed_qty = 0.0
    while time.monotonic() < deadline:
        post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        closed_qty = position_close_delta(pre_qty, post_qty, side)
        if closed_qty + eps >= target_qty:
            return closed_qty
        await asyncio.sleep(0.1)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return position_close_delta(pre_qty, post_qty, side)


def extract_order_uuid(order_result: dict[str, Any] | None) -> str | None:
    if not isinstance(order_result, dict):
        return None
    for key in ("uuid", "client_order_id", "order_id"):
        value = order_result.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def extract_order_avg_price(order_result: dict[str, Any] | None, fallback_price: float) -> float:
    if not isinstance(order_result, dict):
        return fallback_price
    for key in ("avg_price", "price", "fill_price"):
        value = _safe_float(order_result.get(key), 0.0)
        if value > 0:
            return value
    return fallback_price


def exchange_runtime_name(exchange: Any) -> str:
    return str(getattr(exchange, "persist_name", None) or getattr(exchange, "name", "") or "unknown")


def entry_order_key(exchange: Any, symbol: str, side: str) -> tuple[str, str, str]:
    return (exchange_runtime_name(exchange), str(symbol).upper(), str(side).lower())


def wall_time_ms() -> int:
    return int(time.time() * 1000)


def emit_order_event(
    recorder: Callable[..., None] | None,
    event_type: str,
    **payload: Any,
) -> None:
    if recorder is None:
        return
    try:
        recorder(event_type, **payload)
    except Exception as exc:
        print(f"[strategy][order_event][warn] event_type={event_type} error={exc!r}")


def entry_order_settle_sec(config: Any) -> float:
    configured = _safe_float(getattr(config, "entry_order_settle_sec", 0.0), 0.0)
    action_settle = _safe_float(getattr(config, "action_settle_wait_sec", 0.0), 0.0)
    return max(configured, action_settle * 2.0, 1.2)


def entry_order_cooldown_sec(config: Any) -> float:
    return max(_safe_float(getattr(config, "entry_order_cooldown_sec", 0.0), 0.0), 0.0)


async def cancel_if_open(
    exchange: Any,
    symbol: str,
    order_result: dict[str, Any] | None,
    *,
    extract_order_uuid: Callable[[dict[str, Any] | None], str | None],
) -> None:
    order_uuid = extract_order_uuid(order_result)
    if not order_uuid:
        return
    try:
        await exchange.cancel_order(symbol, order_uuid)
    except Exception as exc:
        print(f"[strategy][cancel_if_open][warn] symbol={symbol} uuid={order_uuid} error={exc}")


def adaptive_limit_action_interval_sec(exchange: Any, config: Any) -> float:
    return max(
        _safe_float(getattr(exchange, "order_cooldown_sec", 0.0), 0.0),
        _safe_float(getattr(exchange, "cancel_cooldown_sec", 0.0), 0.0),
        config.min_recompute_interval_sec,
        0.25,
    )


def limit_price_changed(exchange: Any, symbol: str, previous_price: float, target_price: float, *, eps: float) -> bool:
    if previous_price <= 0 or target_price <= 0:
        return True
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    tick_size = _safe_float(meta.get("tick_size"), 0.0)
    if tick_size > 0:
        return abs(previous_price - target_price) + eps >= tick_size
    return abs(previous_price - target_price) > max(eps, abs(previous_price) * 1e-9)


def is_waiting_order_active(
    exchange: Any,
    order_result: dict[str, Any] | None,
    *,
    extract_order_uuid: Callable[[dict[str, Any] | None], str | None],
) -> bool:
    order_uuid = extract_order_uuid(order_result)
    if not order_uuid:
        return False
    for row in getattr(exchange, "waiting_list", []) or []:
        if extract_order_uuid(row) == order_uuid:
            return True
    return False


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
    refresh_symbol_state: Callable[[Any, str, str], Awaitable[None]],
    extract_position_qty: Callable[[dict[str, Any]], float],
    position_close_delta: Callable[[float, float, str], float],
    position_fill_delta: Callable[[float, float, str], float],
    eps: float,
) -> float:
    await refresh_symbol_state(exchange, symbol, reason)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    # Match the reference/question4 philosophy: trading decisions are based on
    # actual account position movement. Order status and trade history may lag
    # or arrive out of order, so they should not advance strategy state.
    return (
        position_close_delta(pre_qty, post_qty, side)
        if is_close
        else position_fill_delta(pre_qty, post_qty, side)
    )


def settle_grace_sec(config: Any) -> float:
    return max(1.5, config.action_settle_wait_sec * 4.0)


async def resolve_after_cancel_from_account(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    order_result: dict[str, Any] | None,
    is_close: bool,
    resolved_qty: float,
    settle_wait_sec: float,
    deadline: float,
    config: Any,
    log_prefix: str,
    reason_prefix: str,
    resolve_effective_order_qty: Callable[..., Awaitable[float]],
    record_order_event: Callable[..., None] | None = None,
    extract_position_qty: Callable[[dict[str, Any]], float] | None = None,
    eps: float,
) -> float:
    """After cancel, trust account movement first and only then continue requoting."""
    previous_qty = resolved_qty
    poll_interval_sec = min(max(_safe_float(getattr(config, "min_recompute_interval_sec", 0.2), 0.2), 0.15), 0.4)
    max_wait_sec = min(max(settle_wait_sec, poll_interval_sec), max(deadline - time.monotonic(), 0.0))
    confirm_until = time.monotonic() + max_wait_sec
    attempt = 0

    while True:
        attempt += 1
        resolved_qty = max(
            resolved_qty,
            await resolve_effective_order_qty(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=order_result,
                is_close=is_close,
                reason=f"{reason_prefix}_{attempt}",
            ),
        )
        if resolved_qty > previous_qty + eps:
            post_qty = (
                extract_position_qty(exchange.get_symbol_snapshot(symbol))
                if extract_position_qty is not None
                else None
            )
            print(
                f"[strategy][{log_prefix}][late_fill_detected] account position changed after cancel "
                f"symbol={symbol} side={side} filled_delta={resolved_qty - previous_qty:.6f} "
                f"resolved={resolved_qty:.6f} target={target_qty:.6f}"
            )
            emit_order_event(
                record_order_event,
                "entry_order_disappeared_resolved",
                exchange=exchange_runtime_name(exchange),
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                resolved_qty=resolved_qty,
                remaining_qty=max(target_qty - resolved_qty, 0.0),
                order_id=extract_order_uuid(order_result),
                client_order_id=extract_order_uuid(order_result),
                pre_qty=pre_qty,
                post_qty=post_qty,
                position_delta=resolved_qty,
                reason="after_cancel",
            )
            previous_qty = resolved_qty
        if resolved_qty + eps >= target_qty or time.monotonic() >= confirm_until:
            return resolved_qty
        await asyncio.sleep(min(poll_interval_sec, max(confirm_until - time.monotonic(), 0.0)))


async def resolve_after_order_disappeared_from_account(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    order_result: dict[str, Any] | None,
    is_close: bool,
    resolved_qty: float,
    settle_wait_sec: float,
    deadline: float,
    config: Any,
    log_prefix: str,
    reason_prefix: str,
    reason: str,
    resolve_effective_order_qty: Callable[..., Awaitable[float]],
    record_order_event: Callable[..., None] | None,
    extract_position_qty: Callable[[dict[str, Any]], float] | None,
    eps: float,
) -> float:
    """Treat a disappeared entry order as in-flight until account movement settles."""
    poll_interval_sec = min(max(_safe_float(getattr(config, "min_recompute_interval_sec", 0.2), 0.2), 0.15), 0.25)
    max_wait_sec = min(max(settle_wait_sec, poll_interval_sec), max(deadline - time.monotonic(), 0.0))
    confirm_until = time.monotonic() + max_wait_sec
    order_uuid = extract_order_uuid(order_result)
    post_qty = (
        extract_position_qty(exchange.get_symbol_snapshot(symbol))
        if extract_position_qty is not None
        else None
    )
    emit_order_event(
        record_order_event,
        "entry_order_disappeared_settle_started",
        exchange=exchange_runtime_name(exchange),
        symbol=symbol,
        side=side,
        target_qty=target_qty,
        resolved_qty=resolved_qty,
        remaining_qty=max(target_qty - resolved_qty, 0.0),
        order_id=order_uuid,
        client_order_id=order_uuid,
        pre_qty=pre_qty,
        post_qty=post_qty,
        position_delta=resolved_qty,
        reason=reason,
    )
    previous_qty = resolved_qty
    attempt = 0

    while True:
        attempt += 1
        resolved_qty = max(
            resolved_qty,
            await resolve_effective_order_qty(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=order_result,
                is_close=is_close,
                reason=f"{reason_prefix}_{attempt}",
            ),
        )
        if resolved_qty > previous_qty + eps:
            post_qty = (
                extract_position_qty(exchange.get_symbol_snapshot(symbol))
                if extract_position_qty is not None
                else None
            )
            print(
                f"[strategy][{log_prefix}][inflight_resolved] disappeared order moved account "
                f"symbol={symbol} side={side} filled_delta={resolved_qty - previous_qty:.6f} "
                f"resolved={resolved_qty:.6f} target={target_qty:.6f}"
            )
            emit_order_event(
                record_order_event,
                "entry_order_disappeared_resolved",
                exchange=exchange_runtime_name(exchange),
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                resolved_qty=resolved_qty,
                remaining_qty=max(target_qty - resolved_qty, 0.0),
                order_id=order_uuid,
                client_order_id=order_uuid,
                pre_qty=pre_qty,
                post_qty=post_qty,
                position_delta=resolved_qty,
                reason=reason,
            )
            previous_qty = resolved_qty
        if resolved_qty + eps >= target_qty or time.monotonic() >= confirm_until:
            return resolved_qty
        await asyncio.sleep(min(poll_interval_sec, max(confirm_until - time.monotonic(), 0.0)))


async def execute_requoting_limit_leg(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    target_qty: float,
    pre_qty: float,
    timeout_sec: float,
    config: Any,
    is_close: bool,
    price_resolver: Callable[[], float],
    log_prefix: str,
    probe_reason_prefix: str,
    resolve_effective_order_qty: Callable[..., Awaitable[float]],
    is_waiting_order_active: Callable[[Any, dict[str, Any] | None], bool],
    limit_price_changed: Callable[[Any, str, float, float], bool],
    cancel_if_open: Callable[[Any, str, dict[str, Any] | None], Awaitable[None]],
    normalize_exchange_qty_for_order: Callable[[Any, str, float, float], float],
    print_verbose: Callable[..., None],
    settle_grace_sec: Callable[[Any], float],
    wait_for_position_fill: Callable[..., Awaitable[float]],
    wait_for_position_close: Callable[..., Awaitable[float]],
    record_order_event: Callable[..., None] | None = None,
    last_order_submit_ms_by_key: dict[tuple[str, str, str], int] | None = None,
    extract_position_qty: Callable[[dict[str, Any]], float] | None = None,
    eps: float,
) -> tuple[float, dict[str, Any] | None, float]:
    deadline = time.monotonic() + max(timeout_sec, 0.0)
    action_interval_sec = adaptive_limit_action_interval_sec(exchange, config)
    settle_wait_sec = max(config.action_settle_wait_sec, action_interval_sec)
    if not is_close and log_prefix == "enter":
        settle_wait_sec = max(settle_wait_sec, entry_order_settle_sec(config))

    active_order: dict[str, Any] | None = None
    active_price = 0.0
    last_order_price_used = 0.0
    resolved_qty = 0.0
    entry_event_recorder = record_order_event if (not is_close and log_prefix == "enter") else None

    while True:
        remaining_time_sec = deadline - time.monotonic()
        resolved_qty = max(
            resolved_qty,
            await resolve_effective_order_qty(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=active_order,
                is_close=is_close,
                reason=f"{probe_reason_prefix}_loop",
            ),
        )
        remaining_qty = max(target_qty - resolved_qty, 0.0)
        if remaining_qty <= eps or remaining_time_sec <= 0:
            break

        if active_order and not is_waiting_order_active(exchange, active_order):
            disappeared_order = active_order
            resolved_qty = await resolve_after_order_disappeared_from_account(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=disappeared_order,
                is_close=is_close,
                resolved_qty=resolved_qty,
                settle_wait_sec=settle_wait_sec,
                deadline=deadline,
                config=config,
                log_prefix=log_prefix,
                reason_prefix=f"{probe_reason_prefix}_order_disappeared",
                reason="order_disappeared",
                resolve_effective_order_qty=resolve_effective_order_qty,
                record_order_event=entry_event_recorder,
                extract_position_qty=extract_position_qty,
                eps=eps,
            )
            active_order = None
            active_price = 0.0
            remaining_qty = max(target_qty - resolved_qty, 0.0)
            if remaining_qty <= eps:
                break
            continue

        target_price = price_resolver()
        if target_price <= 0:
            await asyncio.sleep(min(action_interval_sec, max(remaining_time_sec, 0.05)))
            continue

        if active_order and limit_price_changed(exchange, symbol, active_price, target_price):
            print(
                f"[strategy][{log_prefix}][requote] 재호가 symbol={symbol} side={side} "
                f"old_price={active_price} new_price={target_price}"
            )
            await cancel_if_open(exchange, symbol, active_order)
            canceled_order = active_order
            active_order = None
            active_price = 0.0
            resolved_qty = await resolve_after_cancel_from_account(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=canceled_order,
                is_close=is_close,
                resolved_qty=resolved_qty,
                settle_wait_sec=settle_wait_sec,
                deadline=deadline,
                config=config,
                log_prefix=log_prefix,
                reason_prefix=f"{probe_reason_prefix}_after_requote_cancel",
                resolve_effective_order_qty=resolve_effective_order_qty,
                record_order_event=entry_event_recorder,
                extract_position_qty=extract_position_qty,
                eps=eps,
            )
            remaining_qty = max(target_qty - resolved_qty, 0.0)
            if remaining_qty <= eps:
                break

        if active_order is None:
            if (
                not is_close
                and log_prefix == "enter"
                and last_order_submit_ms_by_key is not None
                and entry_order_cooldown_sec(config) > 0
            ):
                key = entry_order_key(exchange, symbol, side)
                cooldown_ms = int(entry_order_cooldown_sec(config) * 1000.0)
                last_submit_ms = last_order_submit_ms_by_key.get(key, 0)
                cur_ms = wall_time_ms()
                remaining_cooldown_ms = cooldown_ms - (cur_ms - last_submit_ms)
                if last_submit_ms > 0 and remaining_cooldown_ms > 0:
                    print(
                        f"[strategy][{log_prefix}][cooldown] 재주문 대기 "
                        f"exchange={key[0]} symbol={symbol} side={side} "
                        f"remaining_ms={remaining_cooldown_ms} resolved={resolved_qty:.6f} "
                        f"target={target_qty:.6f}"
                    )
                    emit_order_event(
                        entry_event_recorder,
                        "entry_order_reorder_blocked_by_cooldown",
                        exchange=key[0],
                        symbol=symbol,
                        side=side,
                        target_qty=target_qty,
                        resolved_qty=resolved_qty,
                        remaining_qty=remaining_qty,
                        order_id=None,
                        client_order_id=None,
                        pre_qty=pre_qty,
                        post_qty=(
                            extract_position_qty(exchange.get_symbol_snapshot(symbol))
                            if extract_position_qty is not None
                            else None
                        ),
                        position_delta=resolved_qty,
                        reason="cooldown",
                    )
                    resolved_qty = await resolve_after_order_disappeared_from_account(
                        exchange=exchange,
                        symbol=symbol,
                        side=side,
                        pre_qty=pre_qty,
                        target_qty=target_qty,
                        order_result=None,
                        is_close=is_close,
                        resolved_qty=resolved_qty,
                        settle_wait_sec=min(settle_wait_sec, max(remaining_cooldown_ms / 1000.0, action_interval_sec)),
                        deadline=deadline,
                        config=config,
                        log_prefix=log_prefix,
                        reason_prefix=f"{probe_reason_prefix}_cooldown_settle",
                        reason="cooldown",
                        resolve_effective_order_qty=resolve_effective_order_qty,
                        record_order_event=entry_event_recorder,
                        extract_position_qty=extract_position_qty,
                        eps=eps,
                    )
                    await asyncio.sleep(min(remaining_cooldown_ms / 1000.0, action_interval_sec))
                    continue
            submit_qty = normalize_exchange_qty_for_order(exchange, symbol, remaining_qty, target_price)
            if submit_qty <= eps:
                break
            print_verbose(
                config,
                f"[strategy][{log_prefix}] exchange={getattr(exchange, 'persist_name', getattr(exchange, 'name', 'unknown'))} "
                f"side={side} qty={submit_qty:.6f} requested={target_qty:.6f} "
                f"remaining={remaining_qty:.6f} price={target_price}",
            )
            order_result = await exchange.order(symbol, side, submit_qty, target_price)
            if order_result and not order_result.get("error"):
                active_order = order_result
                active_price = target_price
                last_order_price_used = target_price
                if (
                    not is_close
                    and log_prefix == "enter"
                    and last_order_submit_ms_by_key is not None
                ):
                    last_order_submit_ms_by_key[entry_order_key(exchange, symbol, side)] = wall_time_ms()
            else:
                print(f"[strategy][{log_prefix}][warn] 주문 제출 실패 result={order_result}")
                await asyncio.sleep(min(action_interval_sec, max(deadline - time.monotonic(), 0.05)))
                continue

        await asyncio.sleep(min(action_interval_sec, max(deadline - time.monotonic(), 0.05)))

    resolved_qty = max(
        resolved_qty,
        await resolve_effective_order_qty(
            exchange=exchange,
            symbol=symbol,
            side=side,
            pre_qty=pre_qty,
            target_qty=target_qty,
            order_result=active_order,
            is_close=is_close,
            reason=f"{probe_reason_prefix}_before_final_cancel",
        ),
    )
    if active_order:
        if is_waiting_order_active(exchange, active_order):
            canceled_order = active_order
            await cancel_if_open(exchange, symbol, active_order)
            resolved_qty = await resolve_after_cancel_from_account(
                exchange=exchange,
                symbol=symbol,
                side=side,
                pre_qty=pre_qty,
                target_qty=target_qty,
                order_result=canceled_order,
                is_close=is_close,
                resolved_qty=resolved_qty,
                settle_wait_sec=settle_wait_sec,
                deadline=time.monotonic() + settle_grace_sec(config),
                config=config,
                log_prefix=log_prefix,
                reason_prefix=f"{probe_reason_prefix}_after_final_cancel",
                resolve_effective_order_qty=resolve_effective_order_qty,
                record_order_event=entry_event_recorder,
                extract_position_qty=extract_position_qty,
                eps=eps,
            )
        active_order = None

    if resolved_qty + eps < target_qty:
        waiter = wait_for_position_close if is_close else wait_for_position_fill
        resolved_qty = max(
            resolved_qty,
            await waiter(
                exchange,
                symbol,
                side,
                pre_qty,
                target_qty,
                settle_grace_sec(config),
            ),
        )

    return resolved_qty, active_order, active_price if active_price > 0 else last_order_price_used
