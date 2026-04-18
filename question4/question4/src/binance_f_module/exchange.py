import asyncio
import time
from copy import deepcopy
from collections import defaultdict

from .api import (
    get_accounts,
    get_order,
    post_limit_order,
    delete_order,
    get_ticker_list,
    get_open_orders
)
from .websocket_manager import WebSocketManager


class Exchange:
    name = "binanceFuture"

    def __init__(self, ticker_list: list[str]):
        self.ticker_list = ticker_list
        self.account_dict = {}
        self.asset_dict = {}
        self.total_balance = 0
        self.leverage = 0
        self.delta_leverage = 0

        self.waiting_list = []
        self.last_order_time = {}
        self.last_delete_time = {}
        self.ob = {}

        self.websocket = WebSocketManager()
        self._event_queues = defaultdict(asyncio.Queue)

    async def run(self):
        _, self.ticker_info = await get_ticker_list()
        asyncio.create_task(self.run_account())
        asyncio.create_task(self.run_order())
        asyncio.create_task(self.run_public())
        asyncio.create_task(self.run_check_open_orders())

    async def run_public(self):
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    def websocket_callback(self, message: dict):
        if message:
            self.ob.update(message)
            for code, data in message.items():
                asyncio.create_task(self._event_queues[code].put(data))

    def register_listener(self, code: str):
        return self._event_queues[code]

    async def run_account(self):
        while True:
            account, asset = await get_accounts()
            if account is not None and asset is not None:
                self.update_account_state(account, asset)
            await asyncio.sleep(0.2)

    async def run_order(self):
        while True:
            for order in deepcopy(self.waiting_list):
                status = await get_order(order['ticker'], order['uuid'])
                self.check_order_status(order['uuid'], status)
            await asyncio.sleep(1)

    async def run_check_open_orders(self):
        while True:
            order_list = await get_open_orders()
            if order_list is None:
                await asyncio.sleep(30)
                continue

            open_uuids = {o['uuid'] for o in order_list}
            local_uuids = {o['uuid'] for o in self.waiting_list}
            # self.waiting_list에는 있으나, 실제로는 없는 경우 → 삭제
            self.waiting_list = [o for o in self.waiting_list if o['uuid'] in open_uuids]

            # order_list에는 있으나, self.waiting_list에는 없는 경우 → 추가
            new_orders = [o for o in order_list if o['uuid'] not in local_uuids]
            self.waiting_list.extend(new_orders)
            await asyncio.sleep(30)

    def update_account_state(self, account_data: dict, asset_data: dict):
        timestamp = int(time.time() * 1000)
        long_usd, short_usd = 0, 0
        account_dict = {t: self.default_dict(timestamp) for t in self.ticker_list}

        for ticker, data in account_data.items():
            if ticker in self.ticker_list:
                entry = account_dict[ticker]
                entry.update(data)
                entry['nowPrice'] = self.ob.get(ticker, {}).get('bid', 0)
                price = self.ob.get(ticker, {}).get('ask', 0)
                if entry['balance'] >= 0:
                    long_usd += entry['balance'] * price
                else:
                    short_usd += entry['balance'] * price

        asset_dict = {
            ticker: {
                'balance': data['balance'],
                'locked': data.get('locked', 0),
                'avgPrice': data.get('avgPrice', 0),
                'nowPrice': self.ob.get(ticker, {}).get('bid', 0),
                'unrealizedProfit': data.get('unrealizedProfit', 0),
                'timestamp': timestamp
            } for ticker, data in asset_data.items()
        }

        usdt = asset_dict.get('USDT', {'balance': 0, 'unrealizedProfit': 0})
        asset_dict['USD'] = {
            'balance': usdt['balance'],
            'locked': 0,
            'avgPrice': 1,
            'nowPrice': 1,
            'unrealizedProfit': usdt.get('unrealizedProfit', 0),
            'timestamp': timestamp
        }

        total_balance = asset_dict['USD']['balance'] + asset_dict['USD']['unrealizedProfit']
        total_position = long_usd + abs(short_usd)
        delta_position = long_usd + short_usd

        self.account_dict = account_dict
        self.asset_dict = asset_dict
        self.total_balance = total_balance
        self.leverage = total_position / total_balance if total_balance else 0
        self.delta_leverage = delta_position / total_balance if total_balance else 0

    def default_dict(self, timestamp):
        return {
            'balance': 0,
            'locked': 0,
            'avgPrice': 0,
            'nowPrice': 0,
            'unrealizedProfit': 0,
            'timestamp': timestamp
        }

    async def order(self, ticker: str, side: str, amount: float, price: float):
        key = f"{ticker}_{side}"
        if time.time() - self.last_order_time.get(key, 0) >= 1:
            self.last_order_time[key] = time.time()
            order_data = await post_limit_order(ticker, side, amount, price)
            if order_data and 'error' not in order_data:
                self.waiting_list.append(order_data)

    async def cancel_order(self, ticker: str, uuid: str):
        if time.time() - self.last_delete_time.get(ticker, 0) >= 0.5:
            self.last_delete_time[ticker] = time.time()
            result = await delete_order(ticker, uuid)
            self.handle_cancel_result(result, uuid)

    def check_order_status(self, uuid: str, status: dict):
        if status:
            if status.get('state') == 'not_exist' or status.get('error') == 'not_exist':
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]

    def handle_cancel_result(self, result: dict, uuid: str):
        if result:
            if result.get('error') != 'not_exist':
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]
