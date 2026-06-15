from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

from src.strategy.logging import log_entry_partial_fill


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


def schedule_trade_history_sync_if_available(exchange: Any, symbol: str, order_result: dict[str, Any] | None) -> None:
    if not isinstance(order_result, dict) or not order_result:
        return
    sync = getattr(exchange, "_schedule_trade_history_sync", None)
    if not callable(sync):
        return
    try:
        sync(symbol, order_result)
    except Exception as exc:
        print(
            f"[strategy][trade_history_sync][warn] "
            f"exchange={exchange_runtime_name(exchange)} symbol={symbol} error={exc!r}"
        )


def request_account_refresh_if_available(
    exchange: Any,
    reason: str,
    *,
    priority: str = "normal",
    symbol: str | None = None,
) -> None:
    requester = getattr(exchange, "request_account_refresh", None)
    if not callable(requester):
        return
    try:
        requester(reason, priority=priority, symbol=symbol)
    except Exception as exc:
        print(
            f"[strategy][account_refresh][warn] "
            f"exchange={exchange_runtime_name(exchange)} reason={reason} symbol={symbol or ''} error={exc!r}"
        )


def market_freshness_payload(exchange: Any, symbol: str, submit_start_ms: int) -> dict[str, Any]:
    try:
        snapshot = exchange.get_symbol_snapshot(symbol)
    except Exception:
        snapshot = {}
    if not isinstance(snapshot, dict):
        snapshot = {}

    health = snapshot.get("health", {}) or {}
    if not isinstance(health, dict):
        health = {}

    market_state = snapshot.get("market_state", {}) or {}
    orderbook = snapshot.get("orderbook", {}) or {}
    tick = snapshot.get("tick", {}) or {}

    ws_desktop_time_last_ms = int(health.get("ws_desktop_time_last_ms") or 0)
    ws_event_time_last_ms = int(health.get("ws_event_time_last_ms") or 0)
    ws_server_time_last_ms = int(health.get("ws_server_time_last_ms") or 0)
    stale_age_ms = int(health.get("stale_age_ms") or 0)
    submit_market_age_ms = submit_start_ms - ws_desktop_time_last_ms if ws_desktop_time_last_ms > 0 else stale_age_ms

    return {
        "submit_start_ms": submit_start_ms,
        "market_age_ms": submit_market_age_ms,
        "stale_age_ms": stale_age_ms,
        "ws_last_update_ms": int(health.get("ws_last_update_ms") or 0),
        "ws_desktop_time_last_ms": ws_desktop_time_last_ms,
        "ws_event_time_last_ms": ws_event_time_last_ms,
        "ws_server_time_last_ms": ws_server_time_last_ms,
        "ws_source_last": health.get("ws_source_last"),
        "market_source_last": health.get("market_source_last") or market_state.get("source"),
        "event_lag_ms": health.get("event_lag_ms"),
        "transport_lag_ms": health.get("transport_lag_ms"),
        "can_trade": bool(health.get("can_trade", True)),
        "block_reasons": list(health.get("block_reasons", []) or []),
        "best_bid": _safe_float(orderbook.get("best_bid") or tick.get("best_bid") or tick.get("bid"), 0.0),
        "best_ask": _safe_float(orderbook.get("best_ask") or tick.get("best_ask") or tick.get("ask"), 0.0),
    }


def log_entry_first_leg_progress(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    target_qty: float,
    previous_qty: float,
    resolved_qty: float,
    order_result: dict[str, Any] | None,
    pre_qty: float,
    post_qty: float | None,
    reason: str,
    record_order_event: Callable[..., None] | None,
    is_close: bool,
    log_prefix: str,
    eps: float,
) -> None:
    if is_close or log_prefix != "enter" or resolved_qty <= previous_qty + eps:
        return
    filled_delta = resolved_qty - previous_qty
    remaining_qty = max(target_qty - resolved_qty, 0.0)
    exchange_name = exchange_runtime_name(exchange)
    order_uuid = extract_order_uuid(order_result)
    log_entry_partial_fill(
        symbol=symbol,
        exchange=exchange_name,
        side=side,
        filled_delta=filled_delta,
        filled_total=resolved_qty,
        target_qty=target_qty,
        remaining_qty=remaining_qty,
        reason=reason,
    )
    emit_order_event(
        record_order_event,
        "entry_first_leg_fill_progress",
        exchange=exchange_name,
        symbol=symbol,
        side=side,
        target_qty=target_qty,
        filled_delta=filled_delta,
        filled_total=resolved_qty,
        resolved_qty=resolved_qty,
        remaining_qty=remaining_qty,
        order_id=order_uuid,
        client_order_id=order_uuid,
        pre_qty=pre_qty,
        post_qty=post_qty,
        position_delta=resolved_qty,
        reason=reason,
    )
    schedule_trade_history_sync_if_available(exchange, symbol, order_result)
    request_account_refresh_if_available(
        exchange,
        f"{log_prefix}_first_leg_fill:{reason}",
        priority="urgent",
        symbol=symbol,
    )


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
) -> Any:
    order_uuid = extract_order_uuid(order_result)
    if not order_uuid:
        return None
    try:
        return await exchange.cancel_order(symbol, order_uuid)
    except Exception as exc:
        print(f"[strategy][cancel_if_open][warn] symbol={symbol} uuid={order_uuid} error={exc}")
        return {"error": repr(exc), "ticker": symbol, "uuid": order_uuid}


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
            log_entry_first_leg_progress(
                exchange=exchange,
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                previous_qty=previous_qty,
                resolved_qty=resolved_qty,
                order_result=order_result,
                pre_qty=pre_qty,
                post_qty=post_qty,
                reason="after_cancel",
                record_order_event=record_order_event,
                is_close=is_close,
                log_prefix=log_prefix,
                eps=eps,
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
            log_entry_first_leg_progress(
                exchange=exchange,
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                previous_qty=previous_qty,
                resolved_qty=resolved_qty,
                order_result=order_result,
                pre_qty=pre_qty,
                post_qty=post_qty,
                reason=reason,
                record_order_event=record_order_event,
                is_close=is_close,
                log_prefix=log_prefix,
                eps=eps,
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
    telemetry: dict[str, Any] | None = None,
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
    entry_telemetry = telemetry if (not is_close and log_prefix == "enter") else None
    if entry_telemetry is not None:
        entry_telemetry.setdefault("exchange", exchange_runtime_name(exchange))
        entry_telemetry.setdefault("symbol", str(symbol).upper())
        entry_telemetry.setdefault("side", str(side).lower())
        entry_telemetry.setdefault("target_qty", target_qty)

    while True:
        remaining_time_sec = deadline - time.monotonic()
        previous_resolved_qty = resolved_qty
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
        if resolved_qty > previous_resolved_qty + eps:
            if entry_telemetry is not None:
                fill_detected_ms = wall_time_ms()
                entry_telemetry.setdefault("first_fill_detected_ms", fill_detected_ms)
                entry_telemetry["last_fill_detected_ms"] = fill_detected_ms
                entry_telemetry["resolved_qty"] = resolved_qty
            log_entry_first_leg_progress(
                exchange=exchange,
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                previous_qty=previous_resolved_qty,
                resolved_qty=resolved_qty,
                order_result=active_order,
                pre_qty=pre_qty,
                post_qty=(
                    extract_position_qty(exchange.get_symbol_snapshot(symbol))
                    if extract_position_qty is not None
                    else None
                ),
                reason="loop",
                record_order_event=entry_event_recorder,
                is_close=is_close,
                log_prefix=log_prefix,
                eps=eps,
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
            print_verbose(
                config,
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
                    print_verbose(
                        config,
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
            submit_start_ms = wall_time_ms()
            freshness = market_freshness_payload(exchange, symbol, submit_start_ms)
            if entry_telemetry is not None:
                entry_telemetry.setdefault("first_submit_start_ms", submit_start_ms)
                entry_telemetry.setdefault("first_market_age_ms", freshness.get("market_age_ms"))
                entry_telemetry.setdefault("first_stale_age_ms", freshness.get("stale_age_ms"))
                entry_telemetry["last_submit_start_ms"] = submit_start_ms
                entry_telemetry["last_market_age_ms"] = freshness.get("market_age_ms")
                entry_telemetry["last_stale_age_ms"] = freshness.get("stale_age_ms")
                entry_telemetry["submit_count"] = int(entry_telemetry.get("submit_count") or 0) + 1
            if entry_event_recorder is not None:
                emit_order_event(
                    entry_event_recorder,
                    "entry_limit_order_submit_preflight",
                    exchange=exchange_runtime_name(exchange),
                    symbol=symbol,
                    side=side,
                    target_qty=target_qty,
                    submit_qty=submit_qty,
                    remaining_qty=remaining_qty,
                    target_price=target_price,
                    active_price=active_price,
                    resolved_qty=resolved_qty,
                    **freshness,
                )
            order_result = await exchange.order(symbol, side, submit_qty, target_price)
            submit_end_ms = wall_time_ms()
            if entry_telemetry is not None:
                submit_latency_ms = submit_end_ms - submit_start_ms
                entry_telemetry.setdefault("first_submit_end_ms", submit_end_ms)
                entry_telemetry.setdefault("first_submit_latency_ms", submit_latency_ms)
                entry_telemetry["last_submit_end_ms"] = submit_end_ms
                entry_telemetry["last_submit_latency_ms"] = submit_latency_ms
                entry_telemetry["last_submit_target_price"] = target_price
            if entry_event_recorder is not None:
                emit_order_event(
                    entry_event_recorder,
                    "entry_limit_order_submit_result",
                    exchange=exchange_runtime_name(exchange),
                    symbol=symbol,
                    side=side,
                    target_qty=target_qty,
                    submit_qty=submit_qty,
                    remaining_qty=remaining_qty,
                    target_price=target_price,
                    submit_start_ms=submit_start_ms,
                    submit_end_ms=submit_end_ms,
                    submit_latency_ms=submit_end_ms - submit_start_ms,
                    order_id=extract_order_uuid(order_result),
                    client_order_id=extract_order_uuid(order_result),
                    ok=bool(order_result and not order_result.get("error")),
                    error=order_result.get("error") if isinstance(order_result, dict) else None,
                    error_code=order_result.get("code") if isinstance(order_result, dict) else None,
                    error_message=order_result.get("message") if isinstance(order_result, dict) else None,
                )
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

    previous_resolved_qty = resolved_qty
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
    if resolved_qty > previous_resolved_qty + eps:
        if entry_telemetry is not None:
            fill_detected_ms = wall_time_ms()
            entry_telemetry.setdefault("first_fill_detected_ms", fill_detected_ms)
            entry_telemetry["last_fill_detected_ms"] = fill_detected_ms
            entry_telemetry["resolved_qty"] = resolved_qty
        log_entry_first_leg_progress(
            exchange=exchange,
            symbol=symbol,
            side=side,
            target_qty=target_qty,
            previous_qty=previous_resolved_qty,
            resolved_qty=resolved_qty,
            order_result=active_order,
            pre_qty=pre_qty,
            post_qty=(
                extract_position_qty(exchange.get_symbol_snapshot(symbol))
                if extract_position_qty is not None
                else None
            ),
            reason="before_final_cancel",
            record_order_event=entry_event_recorder,
            is_close=is_close,
            log_prefix=log_prefix,
            eps=eps,
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
        previous_resolved_qty = resolved_qty
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
        if resolved_qty > previous_resolved_qty + eps:
            if entry_telemetry is not None:
                fill_detected_ms = wall_time_ms()
                entry_telemetry.setdefault("first_fill_detected_ms", fill_detected_ms)
                entry_telemetry["last_fill_detected_ms"] = fill_detected_ms
                entry_telemetry["resolved_qty"] = resolved_qty
            log_entry_first_leg_progress(
                exchange=exchange,
                symbol=symbol,
                side=side,
                target_qty=target_qty,
                previous_qty=previous_resolved_qty,
                resolved_qty=resolved_qty,
                order_result=active_order,
                pre_qty=pre_qty,
                post_qty=(
                    extract_position_qty(exchange.get_symbol_snapshot(symbol))
                    if extract_position_qty is not None
                    else None
                ),
                reason="final_settle",
                record_order_event=entry_event_recorder,
                is_close=is_close,
                log_prefix=log_prefix,
                eps=eps,
            )

    final_price_used = active_price if active_price > 0 else last_order_price_used
    if entry_telemetry is not None:
        entry_telemetry["completed_ms"] = wall_time_ms()
        entry_telemetry["resolved_qty"] = resolved_qty
        entry_telemetry["final_price_used"] = final_price_used
    return resolved_qty, active_order, final_price_used
