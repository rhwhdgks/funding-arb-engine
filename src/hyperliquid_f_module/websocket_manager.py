import asyncio
import json
import os
import time
import websockets
from dotenv import load_dotenv

load_dotenv()

TARGET_VALUE = int(os.getenv("ORDERBOOK_TARGET_VALUE", "10000"))
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")


class WebSocketManager:
    def __init__(self):
        self.target_value = TARGET_VALUE
        self._book_state = {}  # coin -> latest l2Book raw snapshot

    def estimate_price(self, levels):
        """
        levels: [[price, size], ...] 또는 [{"px":..., "sz":...}, ...] 둘 다 허용
        TARGET_VALUE 만큼 시장가 체결한다고 가정했을 때 평균 체결가 계산
        """
        target_value = self.target_value
        notional_sum = 0.0
        qty_sum = 0.0
        px = 0.0

        for level in levels:
            if isinstance(level, dict):
                price = float(level.get("px", 0))
                qty = float(level.get("sz", 0))
            else:
                price = float(level[0])
                qty = float(level[1])

            if price <= 0 or qty <= 0:
                continue

            notional_sum += price * qty
            qty_sum += qty

            if px == 0.0 and notional_sum >= target_value:
                qty_sum -= (notional_sum - target_value) / price
                px = target_value / qty_sum if qty_sum > 0 else price
                break

        if px == 0.0 and levels:
            last = levels[-1]
            px = float(last.get("px", 0) if isinstance(last, dict) else last[0])

        return round(px, 8) if px > 0 else 0.0

    def _extract_book_levels(self, book_msg: dict):
        """
        Hyperliquid l2Book 메시지를 내부 공통 형식으로 변환.
        문서상 l2Book은 레벨2 호가 snapshot/updates를 제공한다. 

        반환:
          bids: [[px, sz], ...]
          asks: [[px, sz], ...]
          server_time: int | None
        """
        data = book_msg.get("data", {}) if "data" in book_msg else book_msg

        # time 필드가 있으면 사용, 없으면 None
        server_time = data.get("time")
        if server_time is not None:
            server_time = int(server_time)

        levels = data.get("levels", [])

        bids = []
        asks = []

        # Hyperliquid l2Book의 일반적인 구조는 [bids, asks] 형태로 알려져 있음
        if isinstance(levels, list) and len(levels) >= 2:
            raw_bids = levels[0] or []
            raw_asks = levels[1] or []

            for row in raw_bids:
                if isinstance(row, dict):
                    px = row.get("px")
                    sz = row.get("sz")
                else:
                    px, sz = row[0], row[1]
                bids.append([float(px), float(sz)])

            for row in raw_asks:
                if isinstance(row, dict):
                    px = row.get("px")
                    sz = row.get("sz")
                else:
                    px, sz = row[0], row[1]
                asks.append([float(px), float(sz)])

        # 혹시 다른 구조가 오더라도 대충 안전하게 대응
        elif "bids" in data and "asks" in data:
            bids = [[float(x["px"]), float(x["sz"])] for x in data.get("bids", [])]
            asks = [[float(x["px"]), float(x["sz"])] for x in data.get("asks", [])]

        return bids, asks, server_time

    def _build_orderbook_event(self, coin: str, book_msg: dict):
        """
        Hyperliquid l2Book raw -> 바이낸스 쪽과 유사한 callback 포맷으로 변환
        """
        desktop_time = int(time.time() * 1000)
        bids, asks, server_time = self._extract_book_levels(book_msg)

        if not bids or not asks:
            return None

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])

        bid = self.estimate_price(bids)
        ask = self.estimate_price(asks)

        return {
            coin: {
                "desktop_time": desktop_time,
                "server_time": server_time,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid": bid,
                "ask": ask,
            }
        }

    def _build_trade_event(self, coin: str, trade_msg: dict):
        """
        trades raw -> tick 비슷한 포맷으로 변환
        현재 exchange.py에서 바로 안 쓰더라도 나중을 위해 제공
        """
        desktop_time = int(time.time() * 1000)
        data = trade_msg.get("data", trade_msg)

        # trades는 배열일 가능성이 큼
        trades = data if isinstance(data, list) else data.get("trades", [])
        if not trades:
            return None

        last_trade = trades[-1]

        price = float(last_trade.get("px", 0))
        qty = float(last_trade.get("sz", 0))
        server_time = last_trade.get("time")
        if server_time is not None:
            server_time = int(server_time)

        return {
            coin: {
                "desktop_time": desktop_time,
                "server_time": server_time,
                "price": price,
                "qty": qty,
            }
        }
    async def close(self) :
        self.stop()
    async def subscribe(self, tickers: list[str], callback, trade_callback=None):
        """
        orderbook(l2Book) 기본 구독.
        trade_callback이 있으면 trades도 같이 구독.
        callback 포맷은 Binance websocket_manager와 최대한 비슷하게 유지.
        """
        tickers = [t.strip().upper() for t in tickers if t.strip()]
        if not tickers:
            raise ValueError("tickers is empty")

        while True:
            try:
                async with websockets.connect(
                    HYPERLIQUID_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**22,
                ) as ws:
                    # orderbook 구독
                    for coin in tickers:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {
                                "type": "l2Book",
                                "coin": coin,
                            }
                        }))

                        if trade_callback is not None:
                            await ws.send(json.dumps({
                                "method": "subscribe",
                                "subscription": {
                                    "type": "trades",
                                    "coin": coin,
                                }
                            }))

                    while True:
                        raw = await ws.recv()
                        desktop_time = int(time.time() * 1000)

                        msg = json.loads(raw)
                        channel = msg.get("channel")

                        # 일부 서버 메시지는 subscriptionResponse/pong 등일 수 있으니 무시
                        if not channel:
                            continue

                        # l2Book 처리
                        if channel == "l2Book":
                            data = msg.get("data", {})
                            coin = data.get("coin") or msg.get("coin")
                            if not coin:
                                continue

                            event = self._build_orderbook_event(coin.upper(), {
                                **msg,
                                "_desktop_time_recv": desktop_time,
                            })
                            if event:
                                callback(event)

                        # trades 처리
                        elif channel == "trades" and trade_callback is not None:
                            data = msg.get("data", [])
                            coin = None

                            if isinstance(data, list) and data:
                                coin = data[-1].get("coin")
                            elif isinstance(data, dict):
                                coin = data.get("coin")

                            if not coin:
                                continue

                            trade_event = self._build_trade_event(coin.upper(), {
                                **msg,
                                "_desktop_time_recv": desktop_time,
                            })
                            if trade_event:
                                trade_callback(trade_event)

            except Exception as e:
                print(f"[Hyperliquid WebSocket] reconnect after error: {e}")
                await asyncio.sleep(2)