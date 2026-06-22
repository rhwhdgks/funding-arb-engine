from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from src.ops.position_ownership import close_manual_position, upsert_manual_position
from src.ops_console.action_gate import assert_live_action_allowed, assert_symbol_not_stale
from src.ops_console.runtime import ConsoleRuntime, position_qty_for_symbol
from src.ops_console.schemas import (
    CancelSymbolRequest,
    FlattenSymbolRequest,
    LiveLimitOrderRequest,
    LiveMarketOrderRequest,
    PaperOrderRequest,
    RebalanceHedgeRequest,
)
from src.ops_console.state import normalize_order_side, normalize_symbol
from src.strategy.hedge_adapter import execute_strategy_style_hedge

EPS = 1e-9


def resolve_hedge_exchange(runtime: ConsoleRuntime, primary_exchange: str, requested_exchange: str | None) -> str:
    primary = primary_exchange.strip().lower()
    if requested_exchange:
        hedge_exchange = requested_exchange.strip().lower()
        if hedge_exchange == primary:
            raise ValueError("hedge_exchange must differ from primary exchange")
        runtime.session(hedge_exchange)
        return hedge_exchange
    candidates = [exchange_name for exchange_name in runtime.exchange_names if exchange_name != primary]
    if len(candidates) != 1:
        raise ValueError(f"hedge_exchange is required; configured exchanges={runtime.exchange_names}")
    return candidates[0]


def position_qty_from_exchange_state(exchange_state: dict[str, Any], symbol: str) -> float:
    symbol_state = (exchange_state.get("symbols") or {}).get(normalize_symbol(symbol)) or {}
    position = symbol_state.get("position") or {}
    try:
        return float(position.get("qty") or 0.0)
    except Exception:
        return 0.0


def primary_fill_delta(primary_result: dict[str, Any], symbol: str) -> float:
    before_qty = position_qty_from_exchange_state(primary_result.get("before") or {}, symbol)
    after_qty = position_qty_from_exchange_state(primary_result.get("after") or {}, symbol)
    return after_qty - before_qty


def open_order_conflicts(snapshot: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    normalized_symbol = normalize_symbol(symbol)
    conflicts: list[dict[str, Any]] = []
    for exchange_name, exchange_state in (snapshot.get("exchanges") or {}).items():
        symbol_state = ((exchange_state or {}).get("symbols") or {}).get(normalized_symbol) or {}
        for row in symbol_state.get("open_orders") or []:
            if not isinstance(row, dict):
                continue
            conflicts.append(
                {
                    "exchange": exchange_name,
                    "symbol": normalized_symbol,
                    "order_id": row.get("orderId") or row.get("ordId") or row.get("id"),
                    "client_order_id": row.get("clientOrderId") or row.get("clOrdId") or row.get("client_id"),
                    "side": row.get("side"),
                    "status": row.get("status") or row.get("state"),
                    "price": row.get("price") or row.get("px"),
                    "qty": row.get("origQty") or row.get("sz") or row.get("amount") or row.get("qty"),
                }
            )
    return conflicts


def assert_no_open_order_conflicts(snapshot: dict[str, Any], symbol: str) -> None:
    conflicts = open_order_conflicts(snapshot, symbol)
    if conflicts:
        raise ValueError(
            "console live order blocked because open orders already exist "
            f"for {normalize_symbol(symbol)}; cancel-symbol first. conflicts={conflicts[:5]}"
        )


def _env_float(name: str, default: float) -> float:
    try:
        parsed = float(os.getenv(name, str(default)))
    except Exception:
        return default
    return parsed if parsed > 0 else default


def _auto_hedge_wait_sec(request_wait_sec: float) -> float:
    configured = _env_float("OPS_CONSOLE_AUTO_HEDGE_FILL_WAIT_SEC", 2.5)
    return min(max(configured, float(request_wait_sec or 0.0)), 10.0)


async def refresh_console_symbol_state(runtime: ConsoleRuntime, exchange_name: str, symbol: str, reason: str) -> None:
    """Ask the strategy-owned exchange object to refresh account state now.

    In embedded mode this does not create another exchange session; it only wakes
    the existing exchange/account refresh path so console auto-hedge can observe
    post-order position delta before deciding whether to hedge or cancel.
    """

    try:
        session = runtime.session(exchange_name)
        exchange = session.exchange
    except Exception:
        return

    scheduler = getattr(exchange, "account_refresh_scheduler", None)
    refresh_now = getattr(scheduler, "refresh_now", None)
    if callable(refresh_now):
        try:
            refreshed = await refresh_now(reason, priority="urgent")
            if refreshed:
                return
        except Exception as exc:
            print(
                f"[ops_console][auto_hedge][refresh_warn] "
                f"exchange={exchange_name} symbol={symbol} reason={reason} error={exc!r}"
            )

    requester = getattr(exchange, "request_account_refresh", None)
    if callable(requester):
        try:
            requester(reason, priority="urgent", symbol=symbol)
        except Exception as exc:
            print(
                f"[ops_console][auto_hedge][refresh_request_warn] "
                f"exchange={exchange_name} symbol={symbol} reason={reason} error={exc!r}"
            )


async def wait_for_primary_fill_delta(
    runtime: ConsoleRuntime,
    *,
    exchange_name: str,
    symbol: str,
    before_qty: float,
    wait_sec: float,
    poll_sec: float,
    reason: str,
    return_on_first_delta: bool = True,
) -> tuple[float, dict[str, Any]]:
    deadline = time.monotonic() + max(wait_sec, 0.0)
    attempts = 0
    last_qty = before_qty
    last_snapshot_ts_ms = None
    last_delta = 0.0
    while time.monotonic() <= deadline:
        attempts += 1
        await refresh_console_symbol_state(runtime, exchange_name, symbol, reason)
        snapshot = await runtime.snapshot()
        last_snapshot_ts_ms = snapshot.get("timestamp_ms")
        last_qty = position_qty_for_symbol(snapshot, exchange_name, symbol)
        delta = last_qty - before_qty
        if abs(delta) > EPS:
            last_delta = delta
        if abs(delta) > EPS and return_on_first_delta:
            return delta, {
                "attempts": attempts,
                "before_qty": before_qty,
                "observed_qty": last_qty,
                "position_delta": delta,
                "snapshot_ts_ms": last_snapshot_ts_ms,
                "wait_sec": wait_sec,
                "reason": reason,
            }
        await asyncio.sleep(max(poll_sec, 0.05))
    return last_delta, {
        "attempts": attempts,
        "before_qty": before_qty,
        "observed_qty": last_qty,
        "position_delta": last_delta,
        "snapshot_ts_ms": last_snapshot_ts_ms,
        "wait_sec": wait_sec,
        "reason": reason,
    }


def hedge_execution_payload(result: Any) -> dict[str, Any]:
    return {
        "requested_qty": float(getattr(result, "requested_qty", 0.0) or 0.0),
        "filled_qty": float(getattr(result, "filled_qty", 0.0) or 0.0),
        "remaining_qty": float(getattr(result, "remaining_qty", 0.0) or 0.0),
        "avg_price": float(getattr(result, "avg_price", 0.0) or 0.0),
        "order_id": getattr(result, "order_id", None),
        "client_order_id": getattr(result, "client_order_id", None),
    }


async def record_manual_ownership_from_runtime(
    runtime: ConsoleRuntime,
    *,
    symbol: str,
    note: str,
    force_open: bool = False,
) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    positions_by_exchange = {
        exchange_name: position_qty_for_symbol(snapshot, exchange_name, symbol)
        for exchange_name in runtime.exchange_names
    }
    if force_open or any(abs(qty) > 0 for qty in positions_by_exchange.values()):
        return upsert_manual_position(
            symbol=symbol,
            positions_by_exchange=positions_by_exchange,
            source="ops_console",
            note=note,
            force_open=force_open,
        )
    return close_manual_position(symbol=symbol, source="ops_console", note=note)


async def flatten_primary_after_hedge_failure(
    runtime: ConsoleRuntime,
    *,
    exchange_name: str,
    symbol: str,
    wait_sec: float,
) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    qty = position_qty_for_symbol(snapshot, exchange_name, symbol)
    if abs(qty) <= 0:
        return {"ok": True, "exchange": exchange_name, "symbol": symbol, "action": "rollback_flatten", "noop": True}
    side = "sell" if qty > 0 else "buy"
    session = runtime.session(exchange_name)
    await session.ensure_started()
    result = await execute_strategy_style_hedge(
        exchange_name=exchange_name,
        exchange=session.exchange,
        symbol=symbol,
        side=side,
        target_qty=abs(qty),
        is_close=True,
        stage_label="ops_console_rollback",
        wait_sec=wait_sec,
    )
    payload = hedge_execution_payload(result)
    return {
        "ok": payload["remaining_qty"] <= 1e-9,
        "exchange": exchange_name,
        "symbol": symbol,
        "action": "rollback_flatten_after_hedge_failure",
        "side": side,
        **payload,
    }


async def maybe_auto_hedge(
    runtime: ConsoleRuntime,
    request: LiveLimitOrderRequest | LiveMarketOrderRequest,
    *,
    primary_result: dict[str, Any],
    ops_token: str | None,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    if request.hedge_mode != "auto":
        return {
            "ok": True,
            "hedge_mode": request.hedge_mode,
            "primary": primary_result,
            "hedge": None,
            "rollback": None,
        }

    symbol = normalize_symbol(request.symbol)
    fill_delta = primary_fill_delta(primary_result, symbol)
    settle_payload: dict[str, Any] | None = None
    cancel_payload: dict[str, Any] | None = None

    if abs(fill_delta) <= EPS:
        before_qty = position_qty_from_exchange_state(primary_result.get("before") or {}, symbol)
        fill_delta, settle_payload = await wait_for_primary_fill_delta(
            runtime,
            exchange_name=request.exchange,
            symbol=symbol,
            before_qty=before_qty,
            wait_sec=_auto_hedge_wait_sec(request.wait_sec),
            poll_sec=_env_float("OPS_CONSOLE_AUTO_HEDGE_POLL_SEC", 0.2),
            reason="ops_console_auto_hedge_primary_settle",
        )

    if isinstance(request, LiveLimitOrderRequest):
        cancel_reason = "cancel_residual_limit_before_hedge" if abs(fill_delta) > EPS else "cancel_unfilled_limit"
        print(
            f"[ops_console][auto_hedge][{cancel_reason}] "
            f"exchange={request.exchange} symbol={symbol} side={request.side} qty={request.quantity} "
            f"price={request.price} fill_delta={fill_delta} settle={settle_payload}"
        )
        try:
            cancel_payload = await runtime.execute_action(
                request.exchange,
                symbol,
                "live_limit_auto_hedge_cancel_primary_residual",
                lambda exchange: exchange.cancel_all_orders_for_symbol(symbol),
                wait_sec=min(max(float(request.wait_sec or 0.0), 0.3), 2.0),
            )
        except Exception as exc:
            cancel_payload = {"ok": False, "error": repr(exc)}
        before_qty = position_qty_from_exchange_state(primary_result.get("before") or {}, symbol)
        fill_delta, after_cancel_settle = await wait_for_primary_fill_delta(
            runtime,
            exchange_name=request.exchange,
            symbol=symbol,
            before_qty=before_qty,
            wait_sec=min(_auto_hedge_wait_sec(request.wait_sec), 2.0),
            poll_sec=_env_float("OPS_CONSOLE_AUTO_HEDGE_POLL_SEC", 0.2),
            reason="ops_console_auto_hedge_after_cancel_settle",
            return_on_first_delta=False,
        )
        settle_payload = {"before_cancel": settle_payload, "after_cancel": after_cancel_settle}

        if cancel_payload and not cancel_payload.get("ok"):
            raise ValueError(
                "auto hedge limit residual cancel failed; refusing to hedge while "
                f"primary order may still be live exchange={request.exchange} symbol={symbol} "
                f"fill_delta={fill_delta} cancel={cancel_payload}"
            )

    if abs(fill_delta) <= EPS:
        return {
            "ok": True,
            "hedge_mode": request.hedge_mode,
            "primary": primary_result,
            "hedge": {
                "ok": True,
                "action": "live_auto_hedge_market_order",
                "skipped": True,
                "reason": (
                    "primary_limit_not_filled_cancelled"
                    if isinstance(request, LiveLimitOrderRequest)
                    else "primary_fill_delta_zero_after_settle"
                ),
                "settle": settle_payload,
                "cancel": cancel_payload,
            },
            "rollback": None,
        }

    primary_exchange = request.exchange
    hedge_exchange = resolve_hedge_exchange(runtime, primary_exchange, request.hedge_exchange)
    assert_live_action_allowed(snapshot, exchange=hedge_exchange, symbol=symbol, ops_token=ops_token)

    hedge_side = "sell" if fill_delta > 0 else "buy"
    hedge_qty = abs(fill_delta)
    try:
        hedge_session = runtime.session(hedge_exchange)
        await hedge_session.ensure_started()
        hedge_execution = await execute_strategy_style_hedge(
            exchange_name=hedge_exchange,
            exchange=hedge_session.exchange,
            symbol=symbol,
            side=hedge_side,
            target_qty=hedge_qty,
            is_close=False,
            stage_label="ops_console_auto",
            wait_sec=request.wait_sec,
        )
        hedge_payload = hedge_execution_payload(hedge_execution)
        if hedge_payload["remaining_qty"] > 1e-9:
            raise RuntimeError(
                "auto hedge did not change hedge position enough "
                f"hedge_side={hedge_side} target_qty={hedge_qty} "
                f"filled_qty={hedge_payload['filled_qty']} remaining_qty={hedge_payload['remaining_qty']} "
                f"result={hedge_payload}"
            )
    except Exception as exc:
        rollback_result: dict[str, Any]
        try:
            rollback_result = await flatten_primary_after_hedge_failure(
                runtime,
                exchange_name=primary_exchange,
                symbol=symbol,
                wait_sec=request.wait_sec,
            )
        except Exception as rollback_exc:
            rollback_result = {"ok": False, "error": repr(rollback_exc)}
        try:
            hedge_rollback_result = await flatten_primary_after_hedge_failure(
                runtime,
                exchange_name=hedge_exchange,
                symbol=symbol,
                wait_sec=request.wait_sec,
            )
        except Exception as hedge_rollback_exc:
            hedge_rollback_result = {"ok": False, "error": repr(hedge_rollback_exc)}
        try:
            ownership_after_failure = await record_manual_ownership_from_runtime(
                runtime,
                symbol=symbol,
                note="auto_hedge_failed_after_rollback",
            )
        except Exception as ownership_exc:
            ownership_after_failure = {"ok": False, "error": repr(ownership_exc)}
        raise ValueError(
            "auto hedge failed; attempted primary rollback "
            f"primary_exchange={primary_exchange} hedge_exchange={hedge_exchange} "
            f"symbol={symbol} fill_delta={fill_delta} hedge_error={exc!r} "
            f"rollback={rollback_result} hedge_rollback={hedge_rollback_result} "
            f"ownership={ownership_after_failure}"
        ) from exc

    return {
        "ok": True,
        "hedge_mode": request.hedge_mode,
        "primary_fill_delta": fill_delta,
        "hedge_exchange": hedge_exchange,
        "hedge_side": hedge_side,
        "hedge_quantity": hedge_qty,
        "hedge_filled_quantity": hedge_payload["filled_qty"],
        "primary": primary_result,
        "hedge": {
            "ok": True,
            "action": "live_auto_hedge_strategy_style",
            "exchange": hedge_exchange,
            "symbol": symbol,
            "side": hedge_side,
            **hedge_payload,
        },
        "rollback": None,
    }


async def paper_order(runtime: ConsoleRuntime, request: PaperOrderRequest) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    assert_symbol_not_stale(snapshot, request.exchange, request.symbol)
    return {
        "ok": True,
        "mode": "paper",
        "intent": {
            "exchange": request.exchange,
            "symbol": normalize_symbol(request.symbol),
            "side": normalize_order_side(request.side),
            "order_type": request.order_type,
            "quantity": request.quantity,
            "price": request.price,
            "reduce_only": request.reduce_only,
            "hedge_mode": request.hedge_mode,
        },
        "snapshot_ts_ms": snapshot.get("timestamp_ms"),
    }


async def live_limit_order(runtime: ConsoleRuntime, request: LiveLimitOrderRequest, *, ops_token: str | None) -> dict[str, Any]:
    if request.reduce_only:
        raise ValueError("reduce_only limit order is not supported by the console MVP; use flatten-symbol")
    snapshot = await runtime.snapshot()
    assert_live_action_allowed(snapshot, exchange=request.exchange, symbol=request.symbol, ops_token=ops_token)
    assert_no_open_order_conflicts(snapshot, request.symbol)
    side = normalize_order_side(request.side)
    symbol = normalize_symbol(request.symbol)
    ownership = None
    if request.ownership_mode == "manual_only":
        ownership = await record_manual_ownership_from_runtime(
            runtime,
            symbol=symbol,
            note=f"live_limit_order_pre_submit hedge_mode={request.hedge_mode}",
            force_open=True,
        )
    try:
        primary_result = await runtime.execute_action(
            request.exchange,
            symbol,
            "live_limit_order",
            lambda exchange: exchange.order(symbol, side, request.quantity, request.price),
            wait_sec=request.wait_sec,
        )
    except Exception:
        if request.ownership_mode == "manual_only":
            await record_manual_ownership_from_runtime(
                runtime,
                symbol=symbol,
                note="live_limit_order_submit_failed",
            )
        raise
    hedged = await maybe_auto_hedge(
        runtime,
        request,
        primary_result=primary_result,
        ops_token=ops_token,
        snapshot=await runtime.snapshot(),
    )
    if request.ownership_mode == "manual_only":
        hedge = hedged.get("hedge") if isinstance(hedged, dict) else {}
        no_fill_cancelled = bool(
            isinstance(hedge, dict)
            and hedge.get("skipped")
            and hedge.get("reason") == "primary_limit_not_filled_cancelled"
        )
        ownership = await record_manual_ownership_from_runtime(
            runtime,
            symbol=symbol,
            note=(
                "live_limit_order_not_filled_cancelled"
                if no_fill_cancelled
                else f"live_limit_order hedge_mode={request.hedge_mode}"
            ),
            force_open=not no_fill_cancelled,
        )
    return {"ok": True, "action": "live_limit_order", "ownership": ownership, **hedged}


async def live_market_order(runtime: ConsoleRuntime, request: LiveMarketOrderRequest, *, ops_token: str | None) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    assert_live_action_allowed(snapshot, exchange=request.exchange, symbol=request.symbol, ops_token=ops_token)
    assert_no_open_order_conflicts(snapshot, request.symbol)
    side = normalize_order_side(request.side)
    symbol = normalize_symbol(request.symbol)
    ownership = None
    if request.ownership_mode == "manual_only":
        ownership = await record_manual_ownership_from_runtime(
            runtime,
            symbol=symbol,
            note=f"live_market_order_pre_submit hedge_mode={request.hedge_mode}",
            force_open=True,
        )
    try:
        primary_result = await runtime.execute_action(
            request.exchange,
            symbol,
            "live_market_order",
            lambda exchange: exchange.market_order(symbol, side, request.quantity, is_close=False),
            wait_sec=request.wait_sec,
        )
    except Exception:
        if request.ownership_mode == "manual_only":
            await record_manual_ownership_from_runtime(
                runtime,
                symbol=symbol,
                note="live_market_order_submit_failed",
            )
        raise
    hedged = await maybe_auto_hedge(
        runtime,
        request,
        primary_result=primary_result,
        ops_token=ops_token,
        snapshot=await runtime.snapshot(),
    )
    if request.ownership_mode == "manual_only":
        ownership = await record_manual_ownership_from_runtime(
            runtime,
            symbol=symbol,
            note=f"live_market_order hedge_mode={request.hedge_mode}",
        )
    return {"ok": True, "action": "live_market_order", "ownership": ownership, **hedged}


async def rebalance_hedge(runtime: ConsoleRuntime, request: RebalanceHedgeRequest, *, ops_token: str | None) -> dict[str, Any]:
    """Read current per-exchange position for the symbol, compute net exposure
    across exchanges, and market-trade the imbalance on the smaller leg so the
    two legs become equal and opposite (net exposure → 0)."""
    if not request.confirm:
        raise ValueError("rebalance-hedge requires confirm=true")

    snapshot = await runtime.snapshot()
    symbol = normalize_symbol(request.symbol)
    qty_by_exchange = {
        exchange_name: position_qty_for_symbol(snapshot, exchange_name, symbol)
        for exchange_name in runtime.exchange_names
    }
    net = sum(qty_by_exchange.values())
    if abs(net) <= 1e-9:
        return {
            "ok": True,
            "action": "rebalance_hedge",
            "symbol": symbol,
            "noop": True,
            "reason": "already_hedged",
            "net_exposure": net,
            "qty_by_exchange": qty_by_exchange,
        }

    # Trade the imbalance on the leg with the smaller |qty| so it grows to match
    # the larger leg. side reduces net toward zero.
    target_exchange = min(qty_by_exchange, key=lambda ex: abs(qty_by_exchange[ex]))
    side = "sell" if net > 0 else "buy"
    hedge_qty = abs(net)

    assert_live_action_allowed(
        snapshot,
        exchange=target_exchange,
        symbol=symbol,
        ops_token=ops_token,
        check_strategy_conflict=False,
    )

    session = runtime.session(target_exchange)
    await session.ensure_started()
    hedge_result = await execute_strategy_style_hedge(
        exchange_name=target_exchange,
        exchange=session.exchange,
        symbol=symbol,
        side=side,
        target_qty=hedge_qty,
        is_close=False,
        stage_label="ops_console_rebalance",
        wait_sec=request.wait_sec,
    )
    payload = hedge_execution_payload(hedge_result)
    ownership = await record_manual_ownership_from_runtime(
        runtime,
        symbol=symbol,
        note=f"rebalance_hedge net_before={net:.8f}",
    )
    return {
        "ok": payload["remaining_qty"] <= 1e-9,
        "action": "rebalance_hedge",
        "symbol": symbol,
        "net_exposure_before": net,
        "qty_by_exchange_before": qty_by_exchange,
        "hedge_exchange": target_exchange,
        "hedge_side": side,
        "hedge_quantity": hedge_qty,
        "ownership": ownership,
        **payload,
    }


async def cancel_symbol(runtime: ConsoleRuntime, request: CancelSymbolRequest, *, ops_token: str | None) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    assert_live_action_allowed(
        snapshot,
        exchange=request.exchange,
        symbol=request.symbol,
        ops_token=ops_token,
        check_strategy_conflict=False,
    )
    symbol = normalize_symbol(request.symbol)
    result = await runtime.execute_action(
        request.exchange,
        symbol,
        "cancel_symbol_orders",
        lambda exchange: exchange.cancel_all_orders_for_symbol(symbol),
        wait_sec=request.wait_sec,
    )
    ownership = await record_manual_ownership_from_runtime(
        runtime,
        symbol=symbol,
        note="cancel_symbol_orders",
    )
    return {"ok": True, "action": "cancel_symbol_orders", "result": result, "ownership": ownership}


async def flatten_symbol(runtime: ConsoleRuntime, request: FlattenSymbolRequest, *, ops_token: str | None) -> dict[str, Any]:
    if not request.confirm:
        raise ValueError("flatten-symbol requires confirm=true")

    snapshot = await runtime.snapshot()
    symbol = normalize_symbol(request.symbol)
    exchange_names = [request.exchange] if request.exchange else runtime.exchange_names
    results: list[dict[str, Any]] = []

    for exchange_name in exchange_names:
        assert_live_action_allowed(
            snapshot,
            exchange=exchange_name,
            symbol=symbol,
            ops_token=ops_token,
            check_strategy_conflict=False,
        )
        qty = position_qty_for_symbol(snapshot, exchange_name, symbol)
        if abs(qty) <= 0:
            results.append({"ok": True, "exchange": exchange_name, "symbol": symbol, "action": "flatten_symbol", "noop": True})
            continue
        side = "sell" if qty > 0 else "buy"
        amount = abs(qty)
        session = runtime.session(exchange_name)
        await session.ensure_started()
        hedge_result = await execute_strategy_style_hedge(
            exchange_name=exchange_name,
            exchange=session.exchange,
            symbol=symbol,
            side=side,
            target_qty=amount,
            is_close=True,
            stage_label="ops_console_flatten",
            wait_sec=request.wait_sec,
        )
        payload = hedge_execution_payload(hedge_result)
        if payload["remaining_qty"] > 1e-9:
            raise ValueError(
                "flatten_symbol incomplete "
                f"exchange={exchange_name} symbol={symbol} side={side} "
                f"target_qty={amount} filled_qty={payload['filled_qty']} remaining_qty={payload['remaining_qty']}"
            )
        results.append(
            {
                "ok": True,
                "exchange": exchange_name,
                "symbol": symbol,
                "action": "flatten_symbol",
                "side": side,
                **payload,
            }
        )

    ownership = await record_manual_ownership_from_runtime(
        runtime,
        symbol=symbol,
        note="flatten_symbol",
    )
    return {"ok": True, "symbol": symbol, "results": results, "ownership": ownership}
