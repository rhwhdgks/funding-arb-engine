
import asyncio
import json
import os
import time
import inspect
from collections import defaultdict
from typing import Callable, Iterable

import websockets
from dotenv import load_dotenv


load_dotenv()

BINANCE_FUTURES_WS_PUBLIC_URL = os.getenv(
    "BINANCE_FUTURES_WS_PUBLIC_URL",
    "wss://fstream.binance.com/public/stream",
)
ORDERBOOK_TARGET_VALUE = float(os.getenv("ORDERBOOK_TARGET_VALUE", os.getenv("TARGET_VALUE", "10000")))
DEPTH_LEVEL = int(os.getenv("BINANCE_DEPTH_LEVEL", "20"))
DEPTH_SPEED = os.getenv("BINANCE_DEPTH_SPEED", "100ms")
WS_RECONNECT_DELAY_SEC = float(os.getenv("BINANCE_WS_RECONNECT_DELAY_SEC", "2"))
BOOK_TICKER_ENABLED = os.getenv("BINANCE_BOOK_TICKER_ENABLED", "0") == "1"
BOOK_TICKER_MODE = os.getenv("BINANCE_BOOK_TICKER_MODE", "auto").lower()  # auto|all|individual|off
MAX_STREAMS_PER_CONNECTION = int(os.getenv("BINANCE_MAX_STREAMS_PER_CONNECTION", "900"))


class WebSocketManager:
    """
    Binance USD-M futures market-data websocket manager.

    Emits a single-event payload shape:
    {
        "ticker": "BTC",
        "desktop_time": ...,
        "server_time": ...,
        "best_bid": ...,
        ...
        "source": "depth20@100ms" | "bookTicker",
        "_channel": "orderbook" | "book_ticker",
    }
    """

    def __init__(self, target_value: float | None = None):
        self.target_value = float(target_value or ORDERBOOK_TARGET_VALUE)

        self.orderbook: dict[str, dict] = {}
        self.book_ticker: dict[str, dict] = {}
        self.last_message_ts_ms = 0
        self.last_server_ts_ms = 0

        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._callbacks: defaultdict[str, list[Callable[[dict], None]]] = defaultdict(list)

    def register_callback(self, channel: str, callback: Callable[[dict], None]) -> None:
        self._callbacks[channel].append(callback)

    async def close(self):
        self.stop()

    def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()

    async def subscribe(
        self,
        tickers: list[str],
        callback: Callable[[dict], None],
        include_book_ticker: bool = BOOK_TICKER_ENABLED,
    ):
        if not tickers:
            return

        self.register_callback("orderbook", callback)
        if include_book_ticker:
            self.register_callback("book_ticker", callback)

        self._running = True
        tasks: list[asyncio.Task] = []

        orderbook_streams = [self._depth_stream_name(t) for t in tickers]
        for chunk in self._chunk_streams(orderbook_streams, max_streams=MAX_STREAMS_PER_CONNECTION):
            tasks.append(asyncio.create_task(self._run_combined_connection(chunk, channel_hint="orderbook")))

        if include_book_ticker:
            for streams in self._resolve_book_ticker_streams(tickers):
                tasks.append(asyncio.create_task(self._run_combined_connection(streams, channel_hint="book_ticker")))

        self._tasks.extend(tasks)
        await asyncio.gather(*tasks)

    async def subscribe_book_ticker(self, tickers: Iterable[str], callback: Callable[[dict], None]):
        tickers = list(tickers)
        self.register_callback("book_ticker", callback)
        self._running = True

        tasks: list[asyncio.Task] = []
        for streams in self._resolve_book_ticker_streams(tickers):
            tasks.append(asyncio.create_task(self._run_combined_connection(streams, channel_hint="book_ticker")))

        self._tasks.extend(tasks)
        await asyncio.gather(*tasks)

    async def _run_combined_connection(self, streams: list[str], channel_hint: str):
        if not streams:
            return

        url = self._build_combined_url(streams)

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=170,
                    ping_timeout=30,
                    max_queue=4096,
                    close_timeout=5,
                ) as ws:
                    async for raw in ws:
                        desktop_time = int(time.time() * 1000)
                        self.last_message_ts_ms = desktop_time

                        try:
                            message = json.loads(raw)
                        except Exception:
                            continue

                        processed = self.process_message(message, desktop_time=desktop_time, channel_hint=channel_hint)
                        if not processed:
                            continue

                        channel = processed.pop("_channel", channel_hint)
                        for cb in self._callbacks.get(channel, []):
                            try:
                                result = cb(processed)
                                if inspect.isawaitable(result):
                                    asyncio.create_task(result)
                            except Exception as cb_err:
                                print(f"[BinanceFuture WS][callback][{channel}] {cb_err}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BinanceFuture WS] reconnect after error: {e}")
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    def process_message(self, message: dict, desktop_time: int | None = None, channel_hint: str | None = None) -> dict | None:
        desktop_time = desktop_time or int(time.time() * 1000)

        payload = message.get("data", message)
        event_type = payload.get("e")
        stream_name = str(message.get("stream", "")).lower()
        channel_hint = channel_hint or self._infer_channel_hint(stream_name, event_type)

        if event_type == "depthUpdate" or "@depth" in stream_name:
            return self._process_orderbook_message(payload, desktop_time)
        if event_type == "bookTicker" or "bookticker" in stream_name:
            return self._process_book_ticker_message(payload, desktop_time)
        return None

    def _process_orderbook_message(self, payload: dict, desktop_time: int) -> dict | None:
        symbol = self._normalize_symbol(payload.get("s"))
        if not symbol:
            return None

        bids_raw = payload.get("b", [])
        asks_raw = payload.get("a", [])
        if not bids_raw or not asks_raw:
            return None

        bids = [(float(price), float(qty)) for price, qty in bids_raw]
        asks = [(float(price), float(qty)) for price, qty in asks_raw]
        if not bids or not asks:
            return None

        event_time = self._safe_int(payload.get("E"))
        transaction_time = self._safe_int(payload.get("T"))
        server_time = event_time or transaction_time or desktop_time
        self.last_server_ts_ms = server_time

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid = self.estimate_price(bids)
        ask = self.estimate_price(asks)

        item = {
            "ticker": symbol,
            "desktop_time": desktop_time,
            "server_time": server_time,
            "transaction_time": transaction_time,
            "event_time": event_time,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid": bid,
            "ask": ask,
            "mid": (best_bid + best_ask) / 2.0,
            "spread": best_ask - best_bid,
            "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
            "bid_qty_top": float(bids[0][1]),
            "ask_qty_top": float(asks[0][1]),
            "bids": [[float(price), float(qty)] for price, qty in bids[:5]],
            "asks": [[float(price), float(qty)] for price, qty in asks[:5]],
            "depth_levels": min(len(bids), len(asks)),
            "update_id_first": self._safe_int(payload.get("U")),
            "update_id_final": self._safe_int(payload.get("u")),
            "update_id_prev": self._safe_int(payload.get("pu")),
            "source": f"depth{DEPTH_LEVEL}@{DEPTH_SPEED}",
            "_channel": "orderbook",
        }
        self.orderbook[symbol] = {k: v for k, v in item.items() if k != "_channel"}
        return item

    def _process_book_ticker_message(self, payload: dict, desktop_time: int) -> dict | None:
        symbol = self._normalize_symbol(payload.get("s"))
        if not symbol:
            return None

        best_bid = self._safe_float(payload.get("b"))
        best_ask = self._safe_float(payload.get("a"))
        if best_bid is None or best_ask is None:
            return None

        bid_qty = self._safe_float(payload.get("B"))
        ask_qty = self._safe_float(payload.get("A"))
        server_time = self._safe_int(payload.get("E")) or desktop_time
        self.last_server_ts_ms = server_time

        item = {
            "ticker": symbol,
            "desktop_time": desktop_time,
            "server_time": server_time,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid": best_bid,
            "ask": best_ask,
            "mid": (best_bid + best_ask) / 2.0,
            "spread": best_ask - best_bid,
            "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
            "bid_qty_top": bid_qty,
            "ask_qty_top": ask_qty,
            "update_id_final": self._safe_int(payload.get("u")),
            "source": "bookTicker",
            "_channel": "book_ticker",
        }
        self.book_ticker[symbol] = {k: v for k, v in item.items() if k != "_channel"}
        return item

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

            px = price
            if qty * price >= value:
                temp_sum += value
                qty_sum += value / price
                return temp_sum / qty_sum
            else:
                temp_sum += qty * price
                qty_sum += qty
                value -= qty * price

        return temp_sum / qty_sum if qty_sum > 0 else float(px)

    def _normalize_symbol(self, symbol: str | None) -> str | None:
        if not symbol:
            return None
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            symbol = symbol[:-4]
        return symbol.replace("/", "").replace("-", "").replace("_", "")

    def _depth_stream_name(self, ticker: str) -> str:
        return f"{self._normalize_symbol(ticker).lower()}usdt@depth{DEPTH_LEVEL}@{DEPTH_SPEED}"

    def _book_ticker_stream_name(self, ticker: str) -> str:
        return f"{self._normalize_symbol(ticker).lower()}usdt@bookTicker"

    def _resolve_book_ticker_streams(self, tickers: list[str]) -> list[list[str]]:
        mode = BOOK_TICKER_MODE
        if mode == "off":
            return []

        if mode == "all":
            return [["!bookTicker"]]

        if mode == "individual":
            streams = [self._book_ticker_stream_name(t) for t in tickers]
            return list(self._chunk_streams(streams, max_streams=MAX_STREAMS_PER_CONNECTION))

        # auto
        if len(tickers) >= 200:
            return [["!bookTicker"]]
        streams = [self._book_ticker_stream_name(t) for t in tickers]
        return list(self._chunk_streams(streams, max_streams=MAX_STREAMS_PER_CONNECTION))

    def _build_combined_url(self, streams: list[str]) -> str:
        joined = "/".join(streams)
        return f"{BINANCE_FUTURES_WS_PUBLIC_URL}?streams={joined}"

    def _infer_channel_hint(self, stream_name: str, event_type: str | None) -> str:
        if "bookticker" in stream_name or event_type == "bookTicker":
            return "book_ticker"
        return "orderbook"

    def _chunk_streams(self, streams: list[str], max_streams: int) -> Iterable[list[str]]:
        for i in range(0, len(streams), max_streams):
            yield streams[i:i + max_streams]

    async def _dispatch(self, channel: str, payload: dict) -> None:
        for cb in self._callbacks.get(channel, []):
            try:
                result = cb(payload)
                if inspect.isawaitable(result):
                    await result
            except Exception as cb_err:
                print(f"[BinanceFuture WS][callback][{channel}] {cb_err}")

    @staticmethod
    def _safe_int(v) -> int | None:
        try:
            return int(v)
        except Exception:
            return None

    @staticmethod
    def _safe_float(v) -> float | None:
        try:
            return float(v)
        except Exception:
            return None
