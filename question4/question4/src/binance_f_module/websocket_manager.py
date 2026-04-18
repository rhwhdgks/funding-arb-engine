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
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"
        
        async def listen():
            while True:
                try:
                    async with websockets.connect(url, ping_interval=None) as ws:
                        await self.ping_keepalive(ws)
                        async for message in ws:
                            result = self.process_message(message)
                            if result:
                                callback(result)
                except Exception as e:
                    print(f"[Reconnect] WebSocket reconnecting due to: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(listen())
        self.ws_tasks.append(task)

    async def ping_keepalive(self, ws):
        async def ping():
            while True:
                try:
                    await ws.ping()
                    await asyncio.sleep(30)
                except:
                    break
        asyncio.create_task(ping())

    def process_message(self, message):
        try:
            data = json.loads(message)
            if 'data' not in data:
                return None
            data = data['data']
            desktop_time = int(time.time() * 1000)
            code = data['s'].replace("USDT", "")
            bid_price = self.estimate_price(data['b'])
            ask_price = self.estimate_price(data['a'])
            result = {
                code: {
                    "desktop_time": desktop_time,
                    "server_time": int(data['E']),
                    "best_ask": float(data['a'][0][0]),
                    "best_bid": float(data['b'][0][0]),
                    "ask": ask_price,
                    "bid": bid_price,
                }
            }
            return result
        except:
            return None

    def estimate_price(self, orders):
        value = TARGET_VALUE
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
