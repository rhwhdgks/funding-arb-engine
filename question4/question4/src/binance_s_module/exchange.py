# binance_s_module/exchange.py
import asyncio
import time
from copy import deepcopy
from .api import get_account, get_order, post_limit_order, delete_order, get_ticker_list, post_market_order, avg_cost_from_qty
from .websocket_manager import WebSocketManager
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN, ROUND_UP

# 매수 시
def floor_by_tick(value, tick_size):
    val = Decimal(str(value))
    tick = Decimal(str(tick_size))
    floored = (val / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return float(floored)


class Exchange:
    name = "binanceSpot"
    
    def __init__(self, ticker_list):
        self.account_dict = {}
        self.asset_dict = {}
        self.total_balance = 0
        self.waiting_list = []
        self.last_order_time = {}
        self.last_delete_time = {}
        self.ticker_list = ticker_list
        self.ob = {}
        self.websocket = WebSocketManager()
        self._event_queues = defaultdict(asyncio.Queue)
        self.get_account = get_account
        self.get_order = get_order
        self.post_limit_order = post_limit_order
        self.post_market_order = post_market_order
        self.delete_order = delete_order
        self.get_ticker_list = get_ticker_list
        self.avg_cost_from_qty = avg_cost_from_qty

    async def run_public(self):
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    async def run(self):
        _, self.ticker_info = await get_ticker_list()
        asyncio.create_task(self.run_account())
        asyncio.create_task(self.run_order())
        asyncio.create_task(self.run_public())

    def register_listener(self, code: str):
        return self._event_queues[code]

    def emit(self, code: str, data: dict):
        asyncio.create_task(self._event_queues[code].put(data))

    def websocket_callback(self, message: dict):
        if message:
            self.ob.update(message)
            for code, data in message.items():
                self.emit(code, data)

    async def run_account(self):
        while True:
            account, asset = await self.get_account()
            self.account_callback([account, asset])
            await asyncio.sleep(0.5)

    async def run_order(self):
        while True:
            copy_list = deepcopy(self.waiting_list)
            for order in copy_list:
                uuid = order['uuid']
                ticker = order['ticker']
                status = await self.get_order(ticker, uuid)
                self.check_callback(status, [uuid])
            await asyncio.sleep(1)

    def account_callback(self, messagelist):
        if not messagelist:
            return
        account_data, asset_data = messagelist
        self.account_dict = account_data
        self.asset_dict = asset_data

    async def order(self, ticker, side, amount, price):
        key = f"{ticker}_{side}"
        if self.last_order_time.get(key, 0) + 1 <= time.time():
            self.last_order_time[key] = time.time()
            parsed_amount = floor_by_tick(amount, self.ticker_info[ticker]['min_q'])
            order = await self.post_limit_order(ticker, side, parsed_amount, price)
            self.order_callback(order)

    def order_callback(self, data):
        if data and 'error' not in data:
            self.waiting_list.append(data)

    def check_callback(self, data, request_context):
        if data:
            uuid = request_context[0]
            if 'error' in data and data['error'] == 'not_exist':
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]
            elif data.get('state') == 'not_exist':
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]

    async def cancel_order(self, ticker, uuid):
        now = time.time()
        if self.last_delete_time.get(ticker, 0) + 0.5 <= now:
            self.last_delete_time[ticker] = now
            result = await self.delete_order(ticker, uuid)
            self.cancel_callback(result, [ticker, uuid])

    def cancel_callback(self, data, request_context):
        uuid = request_context[1]
        if data:
            if 'error' in data and data['error'] == 'not_exist':
                pass
            else:
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]
