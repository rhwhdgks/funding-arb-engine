import asyncio
import inspect
import json
import os
import socket
import time
from collections import defaultdict
from typing import Callable, Iterable

import websockets
from dotenv import load_dotenv

load_dotenv()

OKX_WS_PUBLIC_URL = os.getenv("OKX_WS_PUBLIC_URL", "wss://ws.okx.com:8443/ws/v5/public")
ORDERBOOK_TARGET_VALUE = float(os.getenv("ORDERBOOK_TARGET_VALUE", os.getenv("TARGET_VALUE", "10000")))
WS_RECONNECT_DELAY_SEC = float(os.getenv("OKX_WS_RECONNECT_DELAY_SEC", "2"))
WS_PING_INTERVAL_SEC = int(os.getenv("OKX_WS_PING_INTERVAL_SEC", "20"))
BOOK_TICKER_ENABLED = os.getenv("OKX_BOOK_TICKER_ENABLED", "1") == "1"
OKX_FORCE_IPV4 = os.getenv("OKX_FORCE_IPV4", "1") == "1"
MAX_TICKERS_PER_CONNECTION = int(os.getenv("OKX_MAX_TICKERS_PER_CONNECTION", "80"))


class WebSocketManager:
    def __init__(self, ticker_info: dict[str, dict], target_value: float | None = None):
        self.ticker_info = ticker_info or {}
        self.target_value = float(target_value or ORDERBOOK_TARGET_VALUE)

        self.orderbook: dict[str, dict] = {}
        self.book_ticker: dict[str, dict] = {}
        self.last_message_ts_ms = 0
        self.last_server_ts_ms = 0

        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._callbacks: defaultdict[str, list[Callable[[dict], None]]] = defaultdict(list)

    def register_callback(self, channel: str, callback: Callable[[dict], None]) -> None:
        self._callbacks[channel].append(callback)

    def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()

    async def close(self):
        self.stop()

    async def subscribe(self, tickers: list[str], callback: Callable[[dict], None], include_book_ticker: bool = BOOK_TICKER_ENABLED):
        if not tickers:
            return

        self.register_callback("orderbook", callback)
        if include_book_ticker:
            self.register_callback("book_ticker", callback)

        self._running = True
        tasks: list[asyncio.Task] = []
        for chunk in self._chunk_tickers(tickers, max_size=MAX_TICKERS_PER_CONNECTION):
            tasks.append(asyncio.create_task(self._run_shared_connection(chunk, include_book_ticker=include_book_ticker)))
        self._tasks.extend(tasks)
        await asyncio.gather(*tasks)

    async def subscribe_book_ticker(self, tickers: Iterable[str], callback: Callable[[dict], None]):
        tickers = list(tickers)
        self.register_callback("book_ticker", callback)
        self._running = True
        tasks: list[asyncio.Task] = []
        for chunk in self._chunk_tickers(tickers, max_size=MAX_TICKERS_PER_CONNECTION):
            tasks.append(asyncio.create_task(self._run_shared_connection(chunk, include_orderbook=False, include_book_ticker=True)))
        self._tasks.extend(tasks)
        await asyncio.gather(*tasks)

    async def _run_shared_connection(
        self,
        tickers: list[str],
        include_orderbook: bool = True,
        include_book_ticker: bool = False,
    ):
        args = []
        for ticker in tickers:
            inst_id = self._symbol_to_inst_id(ticker)
            if include_orderbook:
                args.append({"channel": "books5", "instId": inst_id})
            if include_book_ticker:
                args.append({"channel": "tickers", "instId": inst_id})

        while self._running:
            try:
                async with websockets.connect(
                    OKX_WS_PUBLIC_URL,
                    ping_interval=None,
                    max_queue=4096,
                    close_timeout=5,
                    family=socket.AF_INET if OKX_FORCE_IPV4 else 0,
                ) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    ping_task = asyncio.create_task(self._ping_keepalive(ws))

                    async for raw in ws:
                        desktop_time = int(time.time() * 1000)
                        self.last_message_ts_ms = desktop_time

                        try:
                            message = json.loads(raw)
                        except Exception:
                            continue

                        if self._is_system_message(message):
                            continue

                        processed = self.process_message(message, desktop_time=desktop_time)
                        if not processed:
                            continue

                        channel = processed.pop("_channel", "orderbook")
                        for cb in self._callbacks.get(channel, []):
                            try:
                                cb_result = cb(processed)
                                if inspect.isawaitable(cb_result):
                                    asyncio.create_task(cb_result)
                            except Exception as cb_err:
                                print(f"[OKXFuture WS][callback][{channel}] {cb_err}")

                    ping_task.cancel()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[OKXFuture WS] reconnect after error: {e}")
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def _ping_keepalive(self, ws):
        while True:
            try:
                await ws.send("ping")
            except Exception:
                break
            await asyncio.sleep(WS_PING_INTERVAL_SEC)

    def _chunk_tickers(self, tickers: list[str], max_size: int) -> Iterable[list[str]]:
        if max_size <= 0:
            yield tickers
            return
        for i in range(0, len(tickers), max_size):
            yield tickers[i:i + max_size]

    async def _dispatch(self, channel: str, payload: dict) -> None:
        for cb in self._callbacks.get(channel, []):
            try:
                cb_result = cb(payload)
                if inspect.isawaitable(cb_result):
                    await cb_result
            except Exception as cb_err:
                print(f"[OKXFuture WS][callback][{channel}] {cb_err}")

    def _is_system_message(self, message: dict) -> bool:
        event = message.get("event")
        if event in {"subscribe", "unsubscribe", "login"}:
            return True
        if message.get("op") == "pong":
            return True
        if message.get("data") == "pong":
            return True
        if message.get("code") and message.get("code") != "0":
            print(f"[OKXFuture WS][error] {message}")
            return True
        return False

    def process_message(self, message: dict, desktop_time: int | None = None) -> dict | None:
        desktop_time = desktop_time or int(time.time() * 1000)
        arg = message.get("arg", {})
        channel = arg.get("channel")

        if channel == "books5":
            return self._process_orderbook_message(message, desktop_time)
        if channel == "tickers":
            return self._process_book_ticker_message(message, desktop_time)
        return None

    def _process_orderbook_message(self, message: dict, desktop_time: int) -> dict | None:
        rows = message.get("data") or []
        if not rows:
            return None

        payload = rows[0]
        symbol = self._inst_id_to_symbol(payload.get("instId") or message.get("arg", {}).get("instId"))
        if not symbol:
            return None

        ct_val = float(self.ticker_info.get(symbol, {}).get("ct_val", 1.0))
        bids = [(float(price), float(size) * ct_val) for price, size, *_ in payload.get("bids", [])]
        asks = [(float(price), float(size) * ct_val) for price, size, *_ in payload.get("asks", [])]
        if not bids or not asks:
            return None

        server_time = self._safe_int(payload.get("ts"), desktop_time)
        self.last_server_ts_ms = server_time

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid = self.estimate_price(bids)
        ask = self.estimate_price(asks)

        data = {
            symbol: {
                "desktop_time": desktop_time,
                "server_time": server_time,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid": bid,
                "ask": ask,
                "mid": (best_bid + best_ask) / 2.0,
                "spread": best_ask - best_bid,
                "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
                "bid_qty_top": float(bids[0][1]),
                "ask_qty_top": float(asks[0][1]),
                "depth_levels": min(len(bids), len(asks)),
                "source": "books5",
            },
            "_channel": "orderbook",
        }
        self.orderbook[symbol] = data[symbol]
        return data

    def _process_book_ticker_message(self, message: dict, desktop_time: int) -> dict | None:
        rows = message.get("data") or []
        if not rows:
            return None

        out = {}
        for payload in rows:
            symbol = self._inst_id_to_symbol(payload.get("instId") or message.get("arg", {}).get("instId"))
            if not symbol:
                continue

            best_bid = self._safe_float(payload.get("bidPx"))
            best_ask = self._safe_float(payload.get("askPx"))
            if best_bid is None or best_ask is None:
                continue

            ct_val = float(self.ticker_info.get(symbol, {}).get("ct_val", 1.0))
            bid_size = self._safe_float(payload.get("bidSz"))
            ask_size = self._safe_float(payload.get("askSz"))
            server_time = self._safe_int(payload.get("ts"), desktop_time)
            self.last_server_ts_ms = server_time

            out[symbol] = {
                "desktop_time": desktop_time,
                "server_time": server_time,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid": best_bid,
                "ask": best_ask,
                "mid": (best_bid + best_ask) / 2.0,
                "spread": best_ask - best_bid,
                "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
                "bid_qty_top": (bid_size * ct_val) if bid_size is not None else None,
                "ask_qty_top": (ask_size * ct_val) if ask_size is not None else None,
                "source": "tickers",
            }
            self.book_ticker[symbol] = out[symbol]

        if not out:
            return None
        out["_channel"] = "book_ticker"
        return out

    def estimate_price(self, orders: list[tuple[float, float]]) -> float:
        value = float(self.target_value)
        temp_sum = 0.0
        qty_sum = 0.0
        px = 0.0

        for price, qty in orders:
            price = float(price)
            qty = float(qty)
            if price <= 0 or qty <= 0:
                continue

            temp_sum += price * qty
            qty_sum += qty

            if px == 0.0 and temp_sum >= value:
                qty_sum -= (temp_sum - value) / price
                px = value / qty_sum if qty_sum > 0 else price
                break

        if px == 0.0 and orders:
            px = float(orders[-1][0])

        return round(px, 8)

    def _symbol_to_inst_id(self, symbol: str) -> str:
        return f"{symbol.upper()}-USDT-SWAP"

    def _inst_id_to_symbol(self, inst_id: str | None) -> str | None:
        if not inst_id:
            return None
        return str(inst_id).upper().replace("-USDT-SWAP", "")

    @staticmethod
    def _safe_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value, default: int = 0):
        try:
            return int(float(value))
        except Exception:
            return default
