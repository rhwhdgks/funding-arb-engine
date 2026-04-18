import asyncio
import websockets
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()
TARGET_VALUE = int(os.getenv("TARGET_VALUE", 10000))


class WebSocketManager:
    def __init__(self, ticker_info):
        self.ws_tasks = []
        self.ob = {}
        self.ticker_info = ticker_info

    async def subscribe(self, tickers, callback):
        async def listen(ticker):
            while True:
                try:
                    async with websockets.connect("wss://fx-ws.gateio.ws/v4/ws/usdt", ping_interval=None) as ws:
                        # Subscribe to order book channel
                        msg = json.dumps({
                            "time": int(time.time()),
                            "channel": "futures.order_book",
                            "event": "subscribe",
                            "payload": [f"{ticker.upper()}_USDT", "20", "0"]
                        })

                        await ws.send(msg)

                        await self.ping_keepalive(ws)
                        async for message in ws:
                            result = self.process_message(message)
                            if result:
                                callback(result)
                except Exception as e:
                    print(f"[Reconnect] {ticker} reconnecting due to: {e}")
                    await asyncio.sleep(5)

        for ticker in tickers:
            task = asyncio.create_task(listen(ticker))
            self.ws_tasks.append(task)

    async def ping_keepalive(self, ws):
        async def ping():
            while True:
                try:
                    await ws.send(json.dumps({"time": int(time.time()), "event": "ping"}))
                    await asyncio.sleep(30)
                except:
                    break
        asyncio.create_task(ping())


    def process_message(self, message):
        try:
            data = json.loads(message)
            if data.get("channel") != "futures.order_book":
                return None

            payload = data.get("result")
            if not payload or "bids" not in payload or "asks" not in payload:
                return None

            contract = payload.get("contract", "")
            if not contract.endswith("_USDT"):
                return None
            code = contract.replace("_USDT", "")

            # 가격 및 수량 추출 (p: price, s: size)
            bids = [(entry['p'], entry['s'] * self.ticker_info[code]['cont_size']) for entry in payload["bids"]]
            asks = [(entry['p'], entry['s'] * self.ticker_info[code]['cont_size']) for entry in payload["asks"]]

            desktop_time = int(time.time() * 1000)
            bid_price = self.estimate_price(bids)
            ask_price = self.estimate_price(asks)

            return {
                code: {
                    "desktop_time": desktop_time,
                    "server_time": int(payload.get("t", desktop_time)),
                    "best_ask": float(asks[0][0]),
                    "best_bid": float(bids[0][0]),
                    "ask": ask_price,
                    "bid": bid_price,
                }
            }

        except Exception as e:
            print(f"[Error] process_message: {e}")
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
        if price_result == 0 and orders:
            price_result = float(orders[-1][0])
        return round(price_result, 8)
