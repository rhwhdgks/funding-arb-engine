import asyncio
import json
import time
import websockets
import os
from dotenv import load_dotenv

load_dotenv()
TARGET_VALUE = int(os.getenv("TARGET_VALUE", 10000))



class WebSocketManager:
    WS_URL = "wss://api.gateio.ws/ws/v4/"  # Spot public endpoint

    def __init__(self):
        self.ws_tasks: list[asyncio.Task] = []

    async def subscribe(self, tickers: list[str], callback):
        for tk in tickers:
            task = asyncio.create_task(self._listen(tk.upper(), callback))
            self.ws_tasks.append(task)

    async def _listen(self, symbol: str, cb):
        pair = f"{symbol}_USDT"
        sub  = {
            "time": int(time.time()),
            "channel": "spot.order_book",
            "event": "subscribe",
            "payload": [pair, "20", "100ms"],  # 20‑depth, no interval diff
        }
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=None) as ws:
                    await ws.send(json.dumps(sub))
                    # start ping keep‑alive
                    asyncio.create_task(self._ping(ws))

                    async for msg in ws:
                        res = self._process_message(msg)
                        if res:
                            cb(res)
            except Exception as e:
                print(f"[WS‑reconnect] {symbol}: {e}")
                await asyncio.sleep(5)

    async def _ping(self, ws):
        while True:
            try:
                await ws.send(json.dumps({"time": int(time.time()), "event": "ping"}))
                await asyncio.sleep(30)
            except:
                break
            
    def _process_message(self, message: str):
        try:
            data = json.loads(message)

            if data.get("channel") != "spot.order_book":
                return None
            if data.get("event") not in {"update", "all"}:          # 첫 스냅샷 = all, 이후 = update
                return None

            payload = data.get("result") or data.get("data") or data.get("payload")
            if not payload or "bids" not in payload or "asks" not in payload:
                return None

            contract = payload.get("s") or payload.get("currency_pair") or payload.get("contract")
            if not contract or not contract.endswith("_USDT"):
                return None
            code = contract.replace("_USDT", "")

            bids = [(row[0], row[1]) for row in payload["bids"]]
            asks = [(row[0], row[1]) for row in payload["asks"]]

            now_ms = int(time.time() * 1000)
            return {
                code: {
                    "desktop_time": now_ms,
                    "server_time": int(payload.get("t", now_ms)),
                    "best_ask": float(asks[0][0]),
                    "best_bid": float(bids[0][0]),
                    "ask": self._estimate_price(asks),
                    "bid": self._estimate_price(bids),
                }
            }

        except Exception as e:
            print("[WS parse error]", e)
            return None


    def _estimate_price(self, orders):
        value = TARGET_VALUE
        temp_sum = qty_sum = 0
        avg_price = 0
        for price, qty in orders:
            p = float(price)
            q = float(qty)
            temp_sum += p * q
            qty_sum += q

            if avg_price == 0 and temp_sum >= value:
                qty_sum -= (temp_sum - value) / p
                avg_price = value / qty_sum
                break

        if avg_price == 0 and orders:
            avg_price = float(orders[-1][0])
        return round(avg_price, 8)