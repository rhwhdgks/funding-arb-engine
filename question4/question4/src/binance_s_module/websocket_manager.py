import asyncio
import websockets
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()
TARGET_VALUE = int(os.getenv("TARGET_VALUE", 10000))



class WebSocketManager:
    def __init__(self):
        self.ws_tasks = []
        self.ob = {}

    async def subscribe(self, tickers, callback):
        stream_names = [f"{ticker.lower()}usdt@depth20@100ms" for ticker in tickers]
        url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(stream_names)}"

        async def listen():
            while True:
                try:
                    async with websockets.connect(url, ping_interval=None) as ws:
                        await self._ping_keepalive(ws)
                        async for message in ws:
                            result = self._process_message(message)
                            if result:
                                callback(result)
                except Exception as e:
                    print(f"[Reconnect] WebSocket reconnecting due to: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(listen())
        self.ws_tasks.append(task)

    async def _ping_keepalive(self, ws):
        async def ping():
            while True:
                try:
                    await ws.ping()
                    await asyncio.sleep(30)
                except:
                    break
        asyncio.create_task(ping())

    def _process_message(self, message):
        try:
            data = json.loads(message)
            if 'data' not in data:
                return None

            ob = data['data']
            desktop_time = int(time.time() * 1000)
            code = data['stream'].split("usdt@")[0].upper()

            bid_price = self._estimate_price(ob['bids'])
            ask_price = self._estimate_price(ob['asks'])
            result = {
                code: {
                    "desktop_time": desktop_time,
                    "best_ask": float(ob['asks'][0][0]),
                    "best_bid": float(ob['bids'][0][0]),
                    "ask": ask_price,
                    "bid": bid_price,
                }
            }
            return result
        except Exception as e:
            print("Error in _process_message:", e)
            return None

    def _estimate_price(self, orders):
        """오더북에서 평균 체결가 추정"""
        value = TARGET_VALUE  # 기준 금액
        temp_sum = 0
        qty_sum = 0
        price_result = 0

        for price, qty in orders:
            p = float(price)
            q = float(qty)
            temp_sum += p * q
            qty_sum += q

            if price_result == 0 and temp_sum >= value:
                qty_sum -= (temp_sum - value) / p
                price_result = value / qty_sum
                break

        if price_result == 0:
            price_result = float(orders[-1][0])
        return round(price_result, 8)
