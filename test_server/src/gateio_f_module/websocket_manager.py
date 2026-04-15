import asyncio
import json
import os
import time
import inspect
from collections import defaultdict
from typing import Callable, Iterable

import websockets
from dotenv import load_dotenv

from .api import get_market_prefill

load_dotenv()

GATE_FUTURES_WS_URL = os.getenv("GATE_FUTURES_WS_URL", "wss://fx-ws.gateio.ws/v4/ws/usdt")
ORDERBOOK_TARGET_VALUE = float(os.getenv("ORDERBOOK_TARGET_VALUE", os.getenv("TARGET_VALUE", "10000")))
WS_PING_INTERVAL_SEC = int(os.getenv("GATE_WS_PING_INTERVAL_SEC", "25"))
WS_RECONNECT_DELAY_SEC = float(os.getenv("GATE_WS_RECONNECT_DELAY_SEC", "2"))
ORDERBOOK_DEPTH_LEVEL = str(os.getenv("GATE_ORDERBOOK_DEPTH_LEVEL", "20"))
ORDERBOOK_UPDATE_FREQ = str(os.getenv("GATE_ORDERBOOK_UPDATE_FREQ", "100ms"))
BOOK_TICKER_ENABLED = os.getenv("GATE_BOOK_TICKER_ENABLED", "0") == "1"
MAX_TICKERS_PER_CONNECTION = int(os.getenv("GATE_MAX_TICKERS_PER_CONNECTION", "120"))
REST_PREFILL_ENABLED = os.getenv("GATE_WS_REST_PREFILL_ENABLED", "1") == "1"


class WebSocketManager:
    """
    Gate.io futures market-data websocket manager.

    Goals
    - one shared connection for many symbols
    - orderbook callback payload aligned with Binance manager shape
    - optional best bid/ask ticker stream
    - simple health metrics for exchange.py
    """

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

    async def subscribe(self, tickers: list[str], callback: Callable[[dict], None], include_book_ticker: bool = BOOK_TICKER_ENABLED):
        """
        Subscribe orderbook for all requested tickers using a shared websocket.
        Emits callback({symbol: {...}}) just like the Binance version.
        """
        if not tickers:
            return

        self.register_callback("orderbook", callback)
        if include_book_ticker:
            self.register_callback("book_ticker", callback)

        self._running = True
        if REST_PREFILL_ENABLED:
            await self._emit_rest_prefill(tickers, include_orderbook=True, include_book_ticker=include_book_ticker)
        tasks: list[asyncio.Task] = []
        for chunk in self._chunk_tickers(tickers, max_size=MAX_TICKERS_PER_CONNECTION):
            tasks.append(asyncio.create_task(self._run_shared_connection(chunk, include_book_ticker=include_book_ticker)))
        self._tasks.extend(tasks)
        await asyncio.gather(*tasks)

    async def subscribe_book_ticker(self, tickers: Iterable[str], callback: Callable[[dict], None]):
        tickers = list(tickers)
        self.register_callback("book_ticker", callback)
        self._running = True
        if REST_PREFILL_ENABLED:
            await self._emit_rest_prefill(tickers, include_orderbook=False, include_book_ticker=True)
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
        contracts = [f"{ticker.upper()}_USDT" for ticker in tickers]

        while self._running:
            try:
                async with websockets.connect(
                    GATE_FUTURES_WS_URL,
                    ping_interval=None,
                    max_queue=4096,
                    close_timeout=5,
                ) as ws:
                    ping_task = asyncio.create_task(self._ping_keepalive(ws))

                    if include_orderbook:
                        await self._send_subscribe_orderbook(ws, contracts)
                    if include_book_ticker:
                        await self._send_subscribe_book_ticker(ws, contracts)

                    async for raw in ws:
                        desktop_time = int(time.time() * 1000)
                        try:
                            message = json.loads(raw)
                        except Exception:
                            continue

                        if self._is_system_message(message):
                            continue

                        self.last_message_ts_ms = desktop_time
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
                                print(f"[GateioFuture WS][callback][{channel}] {cb_err}")

                    ping_task.cancel()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[GateioFuture WS] reconnect after error: {e}")
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def close(self) :
        self.stop()

    def _chunk_tickers(self, tickers: list[str], max_size: int) -> Iterable[list[str]]:
        if max_size <= 0:
            yield tickers
            return
        for i in range(0, len(tickers), max_size):
            yield tickers[i:i + max_size]

    async def _emit_rest_prefill(
        self,
        tickers: list[str],
        *,
        include_orderbook: bool,
        include_book_ticker: bool,
    ) -> None:
        try:
            rows = await get_market_prefill(tickers)
        except Exception as exc:
            print(f"[GateioFuture WS][prefill] {exc}")
            return

        for symbol, base in rows.items():
            cont_size = float(self.ticker_info.get(symbol, {}).get("cont_size", 1.0))
            raw = base.get("raw", {})
            bid_size_contract = self._safe_float(raw.get("highest_size"))
            ask_size_contract = self._safe_float(raw.get("lowest_size"))

            if include_orderbook:
                orderbook_item = dict(base)
                orderbook_item["source"] = "futures.order_book"
                orderbook_item["bid_qty_top"] = (bid_size_contract * cont_size) if bid_size_contract is not None else orderbook_item.get("bid_qty_top")
                orderbook_item["ask_qty_top"] = (ask_size_contract * cont_size) if ask_size_contract is not None else orderbook_item.get("ask_qty_top")
                self.orderbook[symbol] = dict(orderbook_item)
                await self._dispatch("orderbook", orderbook_item)

            if include_book_ticker:
                tick_item = dict(base)
                tick_item["source"] = "futures.book_ticker"
                tick_item["bid_qty_top"] = (bid_size_contract * cont_size) if bid_size_contract is not None else tick_item.get("bid_qty_top")
                tick_item["ask_qty_top"] = (ask_size_contract * cont_size) if ask_size_contract is not None else tick_item.get("ask_qty_top")
                self.book_ticker[symbol] = dict(tick_item)
                await self._dispatch("book_ticker", tick_item)

    async def _dispatch(self, channel: str, payload: dict) -> None:
        for cb in self._callbacks.get(channel, []):
            try:
                cb_result = cb(payload)
                if inspect.isawaitable(cb_result):
                    await cb_result
            except Exception as cb_err:
                print(f"[GateioFuture WS][callback][{channel}] {cb_err}")

    async def _send_subscribe_orderbook(self, ws, contracts: list[str]):
        # legacy order_book channel: no REST snapshot sync required, easier for first production version
        for contract in contracts:
            msg = {
                "time": int(time.time()),
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": [contract, ORDERBOOK_DEPTH_LEVEL, "0"],
            }
            await ws.send(json.dumps(msg))
            await asyncio.sleep(0.01)

    async def _send_subscribe_book_ticker(self, ws, contracts: list[str]):
        # docs allow multiple subscriptions and keep prior subscriptions active
        batch_size = 50
        for i in range(0, len(contracts), batch_size):
            payload = contracts[i:i + batch_size]
            msg = {
                "time": int(time.time()),
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": payload,
            }
            await ws.send(json.dumps(msg))
            await asyncio.sleep(0.01)

    async def _ping_keepalive(self, ws):
        while True:
            try:
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping", "event": "ping"}))
            except Exception:
                break
            await asyncio.sleep(WS_PING_INTERVAL_SEC)

    def _is_system_message(self, message: dict) -> bool:
        event = message.get("event")
        channel = message.get("channel")
        if event in {"subscribe", "unsubscribe", "pong"}:
            return True
        if message.get("error"):
            print(f"[GateioFuture WS][error] {message['error']}")
            return True
        if channel in {"futures.ping"}:
            return True
        return False

    def process_message(self, message: dict, desktop_time: int | None = None) -> dict | None:
        desktop_time = desktop_time or int(time.time() * 1000)
        channel = message.get("channel")

        if channel == "futures.order_book":
            return self._process_orderbook_message(message, desktop_time)
        if channel == "futures.book_ticker":
            return self._process_book_ticker_message(message, desktop_time)
        return None

    def _process_orderbook_message(self, message: dict, desktop_time: int) -> dict | None:
        payload = message.get("result")
        if not payload or "bids" not in payload or "asks" not in payload:
            return None

        contract = payload.get("contract", "")
        symbol = self._contract_to_symbol(contract)
        if not symbol:
            return None

        cont_size = float(self.ticker_info.get(symbol, {}).get("cont_size", 1.0))
        bids = [(float(entry["p"]), float(entry["s"]) * cont_size) for entry in payload.get("bids", [])]
        asks = [(float(entry["p"]), float(entry["s"]) * cont_size) for entry in payload.get("asks", [])]
        if not bids or not asks:
            return None

        server_time = self._extract_server_time_ms(payload, desktop_time)
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
                "source": "futures.order_book",
            },
            "_channel": "orderbook",
        }
        self.orderbook[symbol] = data[symbol]
        return data

    def _process_book_ticker_message(self, message: dict, desktop_time: int) -> dict | None:
        payload = message.get("result")
        if not payload:
            return None

        # Gate returns either a single object or a list depending on channel/version.
        if isinstance(payload, list):
            out = {}
            for item in payload:
                parsed = self._parse_book_ticker_item(item, desktop_time)
                if parsed:
                    out.update(parsed)
            if not out:
                return None
            out["_channel"] = "book_ticker"
            return out

        parsed = self._parse_book_ticker_item(payload, desktop_time)
        if not parsed:
            return None
        parsed["_channel"] = "book_ticker"
        return parsed

    def _parse_book_ticker_item(self, payload: dict, desktop_time: int) -> dict | None:
        contract = payload.get("contract") or payload.get("s") or ""
        symbol = self._contract_to_symbol(contract)
        if not symbol:
            return None

        best_bid = self._safe_float(payload.get("b") or payload.get("bid_price"))
        best_ask = self._safe_float(payload.get("a") or payload.get("ask_price"))
        if best_bid is None or best_ask is None:
            return None

        bid_size = self._safe_float(payload.get("B") or payload.get("bid_size"))
        ask_size = self._safe_float(payload.get("A") or payload.get("ask_size"))
        cont_size = float(self.ticker_info.get(symbol, {}).get("cont_size", 1.0))

        server_time = self._extract_server_time_ms(payload, desktop_time)
        self.last_server_ts_ms = server_time

        item = {
            symbol: {
                "desktop_time": desktop_time,
                "server_time": server_time,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid": best_bid,
                "ask": best_ask,
                "mid": (best_bid + best_ask) / 2.0,
                "spread": best_ask - best_bid,
                "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
                "bid_qty_top": (bid_size * cont_size) if bid_size is not None else None,
                "ask_qty_top": (ask_size * cont_size) if ask_size is not None else None,
                "source": "futures.book_ticker",
            }
        }
        self.book_ticker[symbol] = item[symbol]
        return item

    def estimate_price(self, orders: list[tuple[float, float]]) -> float:
        """
        Average execution price when sweeping self.target_value notional.
        orders: [(price, token_qty), ...]
        """
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

    def _contract_to_symbol(self, contract: str) -> str | None:
        if not contract or not isinstance(contract, str):
            return None
        if contract.endswith("_USDT"):
            return contract[:-5]
        if contract.endswith("USDT"):
            return contract[:-4]
        return contract.upper()

    def _extract_server_time_ms(self, payload: dict, fallback: int) -> int:
        for key in ("t", "time_ms", "create_time_ms"):
            value = payload.get(key)
            if value is None:
                continue
            try:
                value = int(float(value))
                if value < 10_000_000_000:
                    return value * 1000
                return value
            except Exception:
                continue

        value = payload.get("time") or payload.get("create_time")
        if value is not None:
            try:
                return int(float(value) * 1000)
            except Exception:
                pass

        return fallback

    @staticmethod
    def _safe_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None
