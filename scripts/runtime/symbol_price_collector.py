from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import websockets

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ops.persistence.db import ensure_schema, get_db_connection, current_utc_ms
from src.ops.persistence.trading_dev_normalizers import build_symbol_price_rows
from src.ops.persistence.trading_dev_store import upsert_symbol_price_rows


def _parse_csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _normalize_symbol(value: str) -> str:
    return value.strip().upper()


def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on", "y"}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _has_full_price_fields(row: dict[str, Any]) -> bool:
    return all(
        _safe_float(row.get(field)) not in (None, 0.0)
        for field in ("best_bid", "best_ask", "bid", "ask")
    )


def _requested_symbols() -> list[str] | None:
    raw = os.getenv("SYMBOL_PRICE_SYMBOLS", os.getenv("STRATEGY_SYMBOLS", "ALL")).strip()
    if not raw or raw.upper() == "ALL":
        return None
    return [_normalize_symbol(symbol) for symbol in raw.split(",") if symbol.strip()]


def _iter_impact_rows(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if not isinstance(payload, dict):
        return []
    ticker = payload.get("ticker") or payload.get("symbol")
    if ticker:
        return [(_normalize_symbol(str(ticker)), dict(payload))]

    rows: list[tuple[str, dict[str, Any]]] = []
    for key, value in payload.items():
        if str(key).startswith("_"):
            continue
        if isinstance(value, dict):
            rows.append((_normalize_symbol(str(key)), dict(value)))
    return rows


def _sleep_until_next_bucket(interval_sec: int) -> float:
    now_ms = current_utc_ms()
    interval_ms = max(int(interval_sec), 1) * 1000
    next_bucket_ms = ((now_ms // interval_ms) + 1) * interval_ms
    return max((next_bucket_ms - now_ms) / 1000.0, 0.05)


_OKX_BOOKS5_INVALID_RE = re.compile(r"instId:([A-Z0-9-]+)")


async def _validate_okx_books5_symbols(symbols: list[str]) -> set[str]:
    from src.okx_f_module.websocket_manager import (
        MAX_TICKERS_PER_CONNECTION,
        OKX_FORCE_IPV4,
        OKX_WS_PUBLIC_URL,
    )

    invalid_symbols: set[str] = set()
    if not symbols:
        return invalid_symbols

    async def validate_chunk(chunk: list[str]) -> None:
        args = [{"channel": "books5", "instId": f"{symbol.upper()}-USDT-SWAP"} for symbol in chunk]
        try:
            async with websockets.connect(
                OKX_WS_PUBLIC_URL,
                ping_interval=None,
                max_queue=4096,
                close_timeout=5,
                family=socket.AF_INET if OKX_FORCE_IPV4 else 0,
            ) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                deadline = asyncio.get_running_loop().time() + 1.5
                while True:
                    timeout = deadline - asyncio.get_running_loop().time()
                    if timeout <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        break
                    try:
                        message = json.loads(raw)
                    except Exception:
                        continue
                    if message.get("event") != "error":
                        continue
                    matched = _OKX_BOOKS5_INVALID_RE.search(str(message.get("msg") or ""))
                    if not matched:
                        continue
                    inst_id = matched.group(1).upper()
                    invalid_symbols.add(inst_id.replace("-USDT-SWAP", ""))
        except Exception as exc:
            print(f"[symbol-price][okx][warn] books5 지원 심볼 사전 검증 실패 error={exc}")

    chunk_size = max(int(MAX_TICKERS_PER_CONNECTION), 1)
    for i in range(0, len(symbols), chunk_size):
        await validate_chunk(symbols[i:i + chunk_size])
    return invalid_symbols


@dataclass
class ExchangeCollector:
    exchange_name: str
    symbols: list[str]
    fetch_api: Callable[[], Any]
    ws_manager: Any
    impact_state: dict[str, dict[str, Any]]
    ws_task: asyncio.Task | None = None

    def handle_ws_payload(self, payload: dict[str, Any]) -> None:
        for symbol, row in _iter_impact_rows(payload):
            self.impact_state[symbol] = row

    def filtered_impact_state(self, now_ms: int, stale_ms: int) -> tuple[dict[str, dict[str, Any]], int]:
        if stale_ms <= 0:
            return dict(self.impact_state), 0

        filtered: dict[str, dict[str, Any]] = {}
        stale_count = 0
        for symbol, row in self.impact_state.items():
            row_time_ms = int(row.get("desktop_time") or 0)
            if row_time_ms <= 0 or now_ms - row_time_ms > stale_ms:
                stale_count += 1
                continue
            filtered[symbol] = row
        return filtered, stale_count

    def ws_prefill_rows(self, filtered_impact: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        target_symbols = set(self.symbols)
        rows: dict[str, dict[str, Any]] = {}
        for symbol, row in filtered_impact.items():
            normalized_symbol = _normalize_symbol(symbol)
            if normalized_symbol not in target_symbols:
                continue
            if not _has_full_price_fields(row):
                continue
            rows[normalized_symbol] = dict(row)
        return rows

    def ws_prefill_is_complete(self, ws_rows: dict[str, dict[str, Any]]) -> bool:
        target_symbols = set(self.symbols)
        return bool(target_symbols) and target_symbols.issubset(set(ws_rows))


async def _build_binance_collector(symbols: list[str] | None, impact_notional_usd: float) -> ExchangeCollector:
    from src.binance_f_module.api import get_market_prefill, get_ticker_list
    from src.binance_f_module.websocket_manager import WebSocketManager

    result = await get_ticker_list()
    ticker_list, _ = result
    available = [_normalize_symbol(symbol) for symbol in ticker_list]
    targets = available if symbols is None else [symbol for symbol in symbols if symbol in set(available)]
    impact_state: dict[str, dict[str, Any]] = {}
    ws_manager = WebSocketManager(target_value=impact_notional_usd)
    collector = ExchangeCollector(
        exchange_name="binance",
        symbols=targets,
        fetch_api=lambda: get_market_prefill(targets),
        ws_manager=ws_manager,
        impact_state=impact_state,
    )
    collector.ws_task = asyncio.create_task(
        ws_manager.subscribe(targets, collector.handle_ws_payload, include_book_ticker=False)
    )
    return collector


async def _build_okx_collector(symbols: list[str] | None, impact_notional_usd: float) -> ExchangeCollector:
    from src.okx_f_module.api import get_market_prefill, get_ticker_list
    from src.okx_f_module.websocket_manager import WebSocketManager

    result = await get_ticker_list()
    if not result:
        raise RuntimeError("failed to load okx ticker list")
    ticker_list, ticker_info = result
    ticker_set = {_normalize_symbol(symbol) for symbol in ticker_list}
    prefill_rows = await get_market_prefill(symbols)
    active_symbols = {_normalize_symbol(symbol) for symbol in (prefill_rows or {}).keys()}
    pre_candidates = [_normalize_symbol(symbol) for symbol in ticker_list if _normalize_symbol(symbol) in active_symbols]
    invalid_books5_symbols = await _validate_okx_books5_symbols(pre_candidates)
    available = [
        _normalize_symbol(symbol)
        for symbol in ticker_list
        if _normalize_symbol(symbol) in active_symbols
        and _normalize_symbol(symbol) not in invalid_books5_symbols
    ]
    requested_symbols = available if symbols is None else [symbol for symbol in symbols if symbol in set(available)]
    skipped_symbols = sorted((ticker_set - active_symbols) | invalid_books5_symbols)
    if symbols is None:
        targets = available
    else:
        targets = requested_symbols
    if skipped_symbols:
        preview = ",".join(skipped_symbols[:10])
        suffix = "..." if len(skipped_symbols) > 10 else ""
        print(
            f"[symbol-price][okx][info] books5 구독 제외 심볼 {len(skipped_symbols)}개 "
            f"preview={preview}{suffix}"
        )
    impact_state: dict[str, dict[str, Any]] = {}
    ws_manager = WebSocketManager(ticker_info=ticker_info, target_value=impact_notional_usd)
    collector = ExchangeCollector(
        exchange_name="okx",
        symbols=targets,
        fetch_api=lambda: get_market_prefill(targets),
        ws_manager=ws_manager,
        impact_state=impact_state,
    )
    collector.ws_task = asyncio.create_task(
        ws_manager.subscribe(targets, collector.handle_ws_payload, include_book_ticker=False)
    )
    return collector


async def build_collectors(exchange_names: list[str], symbols: list[str] | None, impact_notional_usd: float) -> list[ExchangeCollector]:
    collectors: list[ExchangeCollector] = []
    for name in exchange_names:
        if name == "binance":
            collectors.append(await _build_binance_collector(symbols, impact_notional_usd))
        elif name == "okx":
            collectors.append(await _build_okx_collector(symbols, impact_notional_usd))
        else:
            raise ValueError(f"unsupported symbol_price exchange: {name}")
    return collectors


async def persist_symbol_price_once(
    collectors: list[ExchangeCollector],
    ws_stale_sec: float,
    *,
    ws_first: bool,
) -> dict[str, dict[str, int | str]]:
    now_ms = current_utc_ms()
    stale_ms = max(int(ws_stale_sec * 1000), 0)

    conn = await asyncio.to_thread(get_db_connection)
    try:
        summary: dict[str, dict[str, int | str]] = {}
        for collector in collectors:
            filtered_impact, stale_count = collector.filtered_impact_state(now_ms, stale_ms)
            ws_rows = collector.ws_prefill_rows(filtered_impact)
            if ws_first and collector.ws_prefill_is_complete(ws_rows):
                prefill_rows = ws_rows
                prefill_source = "ws"
            else:
                prefill_rows = await collector.fetch_api() or {}
                prefill_source = "rest"
            rows = build_symbol_price_rows(
                collector.exchange_name,
                prefill_rows,
                filtered_impact,
                snapshot_time_ms=now_ms,
            )
            inserted = await asyncio.to_thread(upsert_symbol_price_rows, conn, rows)
            missing_impact = max(len(rows) - len(filtered_impact), 0)
            summary[collector.exchange_name] = {
                "symbols": len(collector.symbols),
                "api_rows": 0 if prefill_source == "ws" else len(prefill_rows),
                "ws_prefill_rows": len(ws_rows),
                "impact_rows": len(filtered_impact),
                "stale_impact_rows": stale_count,
                "missing_impact_rows": missing_impact,
                "inserted": inserted,
                "prefill_source": prefill_source,
            }
        return summary
    finally:
        conn.close()


async def run_collector(*, once: bool = False) -> None:
    exchange_names = [name.strip().lower() for name in _parse_csv_env("SYMBOL_PRICE_EXCHANGES", "binance,okx")]
    symbols = _requested_symbols()
    impact_notional_usd = float(os.getenv("SYMBOL_PRICE_IMPACT_NOTIONAL_USD", "20000") or 20000)
    interval_sec = max(int(float(os.getenv("SYMBOL_PRICE_INTERVAL_SEC", "60") or 60)), 1)
    ws_stale_sec = float(os.getenv("SYMBOL_PRICE_WS_STALE_SEC", "15") or 15)
    ws_first = _env_bool("SYMBOL_PRICE_WS_FIRST", "1")

    schema_conn = await asyncio.to_thread(get_db_connection)
    try:
        await asyncio.to_thread(ensure_schema, schema_conn)
    finally:
        schema_conn.close()

    collectors = await build_collectors(exchange_names, symbols, impact_notional_usd)

    try:
        await asyncio.sleep(3.0)
        while True:
            summary = await persist_symbol_price_once(collectors, ws_stale_sec=ws_stale_sec, ws_first=ws_first)
            now_ms = current_utc_ms()
            for exchange_name, item in summary.items():
                print(
                    "[symbol-price]"
                    f" exchange={exchange_name}"
                    f" inserted={item['inserted']}"
                    f" prefill_source={item['prefill_source']}"
                    f" api_rows={item['api_rows']}"
                    f" ws_prefill_rows={item['ws_prefill_rows']}"
                    f" impact_rows={item['impact_rows']}"
                    f" stale_impact_rows={item['stale_impact_rows']}"
                    f" missing_impact_rows={item['missing_impact_rows']}"
                    f" symbols={item['symbols']}"
                    f" impact_notional_usd={impact_notional_usd:.0f}"
                    f" bucket_ms={now_ms}"
                )
            if once:
                return
            await asyncio.sleep(_sleep_until_next_bucket(interval_sec))
    finally:
        for collector in collectors:
            with contextlib.suppress(Exception):
                collector.ws_manager.stop()
            if collector.ws_task is not None:
                collector.ws_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    await asyncio.wait_for(collector.ws_task, timeout=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist symbol_price snapshots into trading_dev.")
    parser.add_argument("--once", action="store_true", help="Collect and persist one snapshot bucket, then exit.")
    args = parser.parse_args()
    asyncio.run(run_collector(once=args.once))


if __name__ == "__main__":
    main()
