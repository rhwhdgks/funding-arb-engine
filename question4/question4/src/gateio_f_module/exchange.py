import asyncio
import time
from copy import deepcopy
from collections import defaultdict
from .api import (
    get_account, get_order, post_limit_order, delete_order,
    get_ticker_list, post_market_order, get_balance, get_open_orders
)
from .websocket_manager import WebSocketManager
import traceback


class Exchange:
    name = "GateioFuture"
    
    def __init__(self, ticker_list):
        self.account_dict = {}
        self.asset_dict = {}
        self.total_balance = 0
        self.waiting_list = []
        self.last_order_time = {}
        self.last_delete_time = {}
        self.ticker_list = ticker_list
        self.ob = {}
        self._event_queues = defaultdict(asyncio.Queue)
        self.get_account = get_account
        self.get_order = get_order
        self.post_limit_order = post_limit_order
        self.post_market_order = post_market_order
        self.delete_order = delete_order
        self.get_ticker_list = get_ticker_list

    async def run_public(self):
        self.websocket = WebSocketManager(self.ticker_info)
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    async def run(self):
        _, self.ticker_info = await get_ticker_list()
        asyncio.create_task(self.run_account())
        asyncio.create_task(self.run_balance())
        asyncio.create_task(self.run_order())
        asyncio.create_task(self.run_public())
        asyncio.create_task(self.run_check_open_orders())

    def register_listener(self, code: str):
        return self._event_queues[code]

    def emit(self, code: str, data: dict):
        asyncio.create_task(self._event_queues[code].put(data))

    def websocket_callback(self, message: dict):
        if message:
            self.ob.update(message)
            for code, data in message.items():
                self.emit(code, data)

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

    async def run_account(self):
        while True:
            try:
                account = await get_account({"info": self.ticker_info})  
                if account:
                    self.account_callback(account)
            except Exception as e:
                # 로그만 남기고 루프 계속
                print(f"[run_account] error: {e}")
                traceback.print_exc()

            await asyncio.sleep(0.2)      

    async def run_balance(self):
        while True:
                try:
                    bal = await get_balance()
                    if bal is not None:
                        self.asset = {
                            "balance": bal,
                            "locked": 0,
                            "avgPrice": 1,
                            "nowPrice": 1,
                            "unrealizedProfit": 0,
                            "timestamp": int(time.time() * 1000),
                        }
                except Exception as e:
                    print(f"[run_balance] error: {e}")
                    traceback.print_exc()

                await asyncio.sleep(1)      

    async def run_order(self):
        while True:
            copy_list = deepcopy(self.waiting_list)
            for order in copy_list:
                uuid = order['uuid']
                status = await get_order(uuid, {"info": self.ticker_info})
                self.check_callback(status, [uuid])
            await asyncio.sleep(1)

    def account_callback(self, account_data):
        if not account_data:
            return
        self.account_dict = account_data

    def default_dict(self):
        return {
            'balance': 0, 'locked': 0, 'avgPrice': 0,
            'nowPrice': 0, 'unrealizedProfit': 0, 'timestamp': int(time.time() * 1000)
        }

    async def order(self, ticker, side, amount, price):
        key = f"{ticker}_{side}"
        if self.last_order_time.get(key, 0) + 1 <= time.time():
            self.last_order_time[key] = time.time()
            order = await post_limit_order(ticker, side, amount, price, {"info": self.ticker_info})
            self.order_callback(order)

    def order_callback(self, data):
        if data and 'error' not in data:
            self.waiting_list.append(data)

    def check_callback(self, data, request_context):
        if data:
            uuid = request_context[0]
            if 'error' in data:
                if data['error'] == 'not_exist':
                    self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]
            elif data.get('state') == 'not_exist':
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]

    async def cancel_order(self, ticker, uuid):
        now = time.time()
        if self.last_delete_time.get(ticker, 0) + 0.5 <= now:
            self.last_delete_time[ticker] = now
            result = await delete_order(uuid, {"info": self.ticker_info})
            self.cancel_callback(result, [ticker, uuid])

    def cancel_callback(self, data, request_context):
        uuid = request_context[1]
        if data:
            if 'error' in data and data['error'] == 'not_exist':
                pass
            else:
                self.waiting_list = [o for o in self.waiting_list if o['uuid'] != uuid]
