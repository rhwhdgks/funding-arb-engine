import asyncio
import time
import traceback
from copy import deepcopy
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN

from .api import (
    get_ticker_list,
    post_limit_order,
    get_accounts,
    get_order,
    delete_order,
    avg_cost_from_qty
)
from .websocket_manager import WebSocketManager


def floor_by_tick(value: float, tick_size: float) -> float:
    val = Decimal(str(value))
    tick = Decimal(str(tick_size))
    floored = (val / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return float(floored)


class Exchange:
    name = "GateioSpot"

    def __init__(self, ticker_list: list[str]):
        self.ticker_list = ticker_list
        self.ticker_info: dict = {}
        
        self.account_dict: dict = {}
        self.asset_dict: dict = {}
        self.waiting_list: list = []
        self.last_order_time: dict = {}
        self.last_delete_time: dict = {}

        self.ob: dict = {}
        self.websocket: WebSocketManager | None = None
        self._event_queues = defaultdict(asyncio.Queue)

        self.avg_cost_from_qty = avg_cost_from_qty

    async def run(self):
        _, self.ticker_info = await get_ticker_list()
        asyncio.create_task(self.run_account())
        asyncio.create_task(self.run_order())
        asyncio.create_task(self.run_public())

    async def run_public(self):
        self.websocket = WebSocketManager()
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    def websocket_callback(self, message: dict):
        if message:
            self.ob.update(message)
            for code, data in message.items():
                self.emit(code, data)

    def emit(self, code: str, data: dict):
        asyncio.create_task(self._event_queues[code].put(data))

    def register_listener(self, code: str):
        return self._event_queues[code]

    async def run_account(self):
        while True:
            try:
                accounts = await get_accounts()
                if accounts is not None:
                    self.account_dict, self.asset_dict = accounts
            except Exception as e:
                print("[run_account]", e)
                traceback.print_exc()
            await asyncio.sleep(0.5)

    async def run_order(self):
        while True:
            snapshot = deepcopy(self.waiting_list)
            for order in snapshot:
                order_id = order["uuid"]
                symbol = order["ticker"]
                status = await get_order(order_id, symbol)
                if status and status.get("state") != "exist":
                    self.waiting_list = [o for o in self.waiting_list if o["uuid"] != order_id]
            await asyncio.sleep(1)

    async def order(self, ticker: str, side: str, amount: float, price: float):
        key = f"{ticker}_{side}"
        if time.time() - self.last_order_time.get(key, 0) < 1:
            return None

        self.last_order_time[key] = time.time()
        parsed_amount = floor_by_tick(amount, self.ticker_info[ticker]["min_q"])
        order = await post_limit_order(ticker, side, parsed_amount, price)

        if order is not None:
            self.waiting_list.append(order)

    async def cancel_order(self, ticker: str, order_id: str):
        if time.time() - self.last_delete_time.get(ticker, 0) < 0.5:
            return None

        self.last_delete_time[ticker] = time.time()
        return await delete_order(order_id, ticker)