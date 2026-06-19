from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from src.ops_console.action_gate import assert_action_token
from src.ops_console.actions import cancel_symbol, flatten_symbol, live_limit_order, live_market_order, paper_order, rebalance_hedge
from src.ops_console.embedded import active_service
from src.ops_console.runtime import ConsoleRuntime, PROJECT_ROOT, read_entry_control, write_entry_control
from src.ops_console.schemas import (
    CancelSymbolRequest,
    EntryPauseRequest,
    EntryResumeRequest,
    FlattenSymbolRequest,
    LiveLimitOrderRequest,
    LiveMarketOrderRequest,
    PaperOrderRequest,
    RebalanceHedgeRequest,
)
from src.ops_console.strategy_dashboard import build_strategy_dashboard_state


def _load_env() -> Path:
    raw = os.getenv("STRATEGY_ENV_FILE", str(PROJECT_ROOT / ".env"))
    env_path = Path(raw)
    if not env_path.is_absolute():
        env_path = PROJECT_ROOT / env_path
    load_dotenv(env_path, override=True)
    return env_path


ENV_PATH = _load_env()
_LEGACY_RUNTIME: ConsoleRuntime | None = None
STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Ops Console", version="0.1.0")


def _legacy_runtime() -> ConsoleRuntime:
    global _LEGACY_RUNTIME
    if _LEGACY_RUNTIME is None:
        _LEGACY_RUNTIME = ConsoleRuntime()
    return _LEGACY_RUNTIME


@app.on_event("startup")
async def startup_runtime() -> None:
    if active_service() is None:
        await _legacy_runtime().start()


@app.on_event("shutdown")
async def shutdown_runtime() -> None:
    if _LEGACY_RUNTIME is not None:
        await _LEGACY_RUNTIME.stop()


def _ops_token(header_token: str | None) -> str | None:
    return header_token


def _no_cache_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }


def _raise_http_error(exc: Exception) -> None:
    if isinstance(exc, PermissionError):
        raise HTTPException(status_code=403, detail=str(exc))
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=500, detail=repr(exc))


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(
        (STATIC_DIR / "index.html").read_text(encoding="utf-8"),
        headers=_no_cache_headers(),
    )


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    service = active_service()
    payload = {
        "ok": True,
        "env_file": str(ENV_PATH),
        "embedded": service is not None,
        "source_status": "strategy_attached" if service is not None else "standalone_legacy",
    }
    return JSONResponse(payload, headers=_no_cache_headers())


@app.get("/api/state")
async def api_state() -> dict[str, Any]:
    service = active_service()
    if service is not None:
        return JSONResponse(await service.build_console_state(), headers=_no_cache_headers())
    return JSONResponse(await _legacy_runtime().snapshot(), headers=_no_cache_headers())


@app.get("/api/strategy/state")
async def api_strategy_state() -> dict[str, Any]:
    service = active_service()
    if service is not None:
        return JSONResponse(service.build_strategy_state(), headers=_no_cache_headers())
    return JSONResponse(build_strategy_dashboard_state(), headers=_no_cache_headers())


@app.post("/api/order/paper")
async def api_paper_order(request: PaperOrderRequest) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("paper-order", request, ops_token=None)
        return await paper_order(_legacy_runtime(), request)
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/order/live-limit")
async def api_live_limit_order(
    request: LiveLimitOrderRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("live-limit", request, ops_token=_ops_token(x_ops_token))
        return await live_limit_order(_legacy_runtime(), request, ops_token=_ops_token(x_ops_token))
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/order/live-market")
async def api_live_market_order(
    request: LiveMarketOrderRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("live-market", request, ops_token=_ops_token(x_ops_token))
        return await live_market_order(_legacy_runtime(), request, ops_token=_ops_token(x_ops_token))
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/orders/cancel-symbol")
async def api_cancel_symbol(
    request: CancelSymbolRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("cancel-symbol", request, ops_token=_ops_token(x_ops_token))
        return await cancel_symbol(_legacy_runtime(), request, ops_token=_ops_token(x_ops_token))
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/positions/flatten-symbol")
async def api_flatten_symbol(
    request: FlattenSymbolRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("flatten-symbol", request, ops_token=_ops_token(x_ops_token))
        return await flatten_symbol(_legacy_runtime(), request, ops_token=_ops_token(x_ops_token))
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/positions/rebalance-hedge")
async def api_rebalance_hedge(
    request: RebalanceHedgeRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("rebalance-hedge", request, ops_token=_ops_token(x_ops_token))
        return await rebalance_hedge(_legacy_runtime(), request, ops_token=_ops_token(x_ops_token))
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/runtime/pause-entry")
async def api_pause_entry(
    request: EntryPauseRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("pause-entry", request, ops_token=_ops_token(x_ops_token))
        assert_action_token(_ops_token(x_ops_token))
        return {"ok": True, "entry_control": write_entry_control(True, request.reason)}
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.post("/api/runtime/resume-entry")
async def api_resume_entry(
    request: EntryResumeRequest,
    x_ops_token: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        service = active_service()
        if service is not None:
            return await service.submit_command("resume-entry", request, ops_token=_ops_token(x_ops_token))
        assert_action_token(_ops_token(x_ops_token))
        return {"ok": True, "entry_control": write_entry_control(False, request.reason)}
    except Exception as exc:
        _raise_http_error(exc)
        raise


@app.get("/api/runtime/entry-control")
async def api_entry_control() -> dict[str, Any]:
    service = active_service()
    if service is not None:
        return JSONResponse({"ok": True, "entry_control": service.entry_control_dict()}, headers=_no_cache_headers())
    return JSONResponse({"ok": True, "entry_control": read_entry_control()}, headers=_no_cache_headers())


@app.websocket("/ws/state")
async def ws_state(websocket: WebSocket) -> None:
    await websocket.accept()
    interval_sec = float(os.getenv("OPS_CONSOLE_WS_INTERVAL_SEC", "1.0"))
    try:
        while True:
            service = active_service()
            if service is not None:
                await websocket.send_json(await service.build_console_state())
            else:
                await websocket.send_json(await _legacy_runtime().snapshot())
            await asyncio.sleep(max(interval_sec, 0.2))
    except WebSocketDisconnect:
        return
