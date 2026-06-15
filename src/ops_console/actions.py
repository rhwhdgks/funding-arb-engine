from __future__ import annotations

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
    if abs(fill_delta) <= 0:
        return {
            "ok": True,
            "hedge_mode": request.hedge_mode,
            "primary": primary_result,
            "hedge": {
                "ok": True,
                "action": "live_auto_hedge_market_order",
                "skipped": True,
                "reason": "primary_fill_delta_zero",
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
        ownership = await record_manual_ownership_from_runtime(
            runtime,
            symbol=symbol,
            note=f"live_limit_order hedge_mode={request.hedge_mode}",
            force_open=True,
        )
    return {"ok": True, "action": "live_limit_order", "ownership": ownership, **hedged}


async def live_market_order(runtime: ConsoleRuntime, request: LiveMarketOrderRequest, *, ops_token: str | None) -> dict[str, Any]:
    snapshot = await runtime.snapshot()
    assert_live_action_allowed(snapshot, exchange=request.exchange, symbol=request.symbol, ops_token=ops_token)
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
