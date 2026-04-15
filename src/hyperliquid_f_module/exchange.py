import asyncio
import time
from copy import deepcopy
from collections import defaultdict

from .api import (
    get_accounts,
    get_order,
    post_limit_order,
    delete_order,
    get_open_orders,
    get_funding_rate,
    get_funding_rate_history,
    get_ohlcv_1m,
    get_meta,
)
from .websocket_manager import WebSocketManager


class Exchange:
    name = "hyperliquid"

    def __init__(self, ticker_list: list[str]):
        self.ticker_list = [x.strip().upper() for x in ticker_list]
        self.ticker_info = {}

        # 공통 상태
        self.account_dict = {}
        self.asset_dict = {}
        self.total_balance = 0.0
        self.leverage = 0.0
        self.delta_leverage = 0.0

        self.waiting_list = []
        self.last_order_time = {}
        self.last_delete_time = {}
        self.ob = {}

        # 펀딩/포지션 상태
        self.funding_now = {}
        self.funding_history = {}
        self.position_snapshot = {}
        self.net_exposure = {}
        self.hedge_state = {}

        # websocket health
        self.ws_drift_last_ms = {}
        self.ws_drift_avg_ms = {}
        self.ws_last_update_ms = {}
        self.max_server_pc_drift_ms = 500

        self.websocket = WebSocketManager()
        self._event_queues = defaultdict(asyncio.Queue)

    async def run(self):
        """
        전체 루프 시작
        """
        meta = await get_meta()
        if meta is None:
            raise RuntimeError("Failed to load Hyperliquid meta")

        asyncio.create_task(self.run_account())
        asyncio.create_task(self.run_order())
        asyncio.create_task(self.run_public())
        asyncio.create_task(self.run_check_open_orders())
        asyncio.create_task(self.run_funding())
        asyncio.create_task(self.run_health())

    async def bootstrap_symbol(self, symbol: str, leverage: int = 3, margin_mode: str = "CROSSED"):
        """
        Hyperliquid는 현재 바이낸스처럼 set_leverage / set_margin_mode를
        그대로 쓰지 않으므로, main.py 호환용 no-op 성공 응답 형태로 둔다.
        """
        symbol = symbol.upper()
        return {
            "leverage": {
                "ok": True,
                "symbol": symbol,
                "leverage": leverage,
                "raw": {"note": "not_applied_yet_on_hyperliquid"},
            },
            "margin_mode": {
                "ok": True,
                "symbol": symbol,
                "margin_mode": margin_mode.upper(),
                "raw": {"note": "not_applied_yet_on_hyperliquid"},
            },
        }

    async def run_public(self):
        """
        공개 websocket(orderbook) 시작
        """
        await self.websocket.subscribe(self.ticker_list, self.websocket_callback)

    def websocket_callback(self, message: dict):
        """
        websocket_manager가 가공한 오더북 이벤트 반영
        """
        if not message:
            return

        self.ob.update(message)
        now_ms = int(time.time() * 1000)

        for code, data in message.items():
            server_time = data.get("server_time")

            if server_time is not None:
                drift_ms = now_ms - server_time
                self.ws_drift_last_ms[code] = drift_ms

                prev_avg = self.ws_drift_avg_ms.get(code, drift_ms)
                self.ws_drift_avg_ms[code] = prev_avg * 0.9 + drift_ms * 0.1

            self.ws_last_update_ms[code] = now_ms
            asyncio.create_task(self._event_queues[code].put(data))

    def register_listener(self, code: str):
        """
        심볼별 이벤트 큐 반환
        """
        return self._event_queues[code]

    async def run_account(self):
        """
        계정/포지션 상태 갱신
        """
        while True:
            try:
                account, asset = await get_accounts()
                if account is not None and asset is not None:
                    self.update_account_state(account, asset)
            except Exception as e:
                print(f"[{self.name}][run_account][error] {e}")

            await asyncio.sleep(0.2)

    async def run_order(self):
        """
        waiting_list의 주문 개별 조회
        """
        while True:
            try:
                for order in deepcopy(self.waiting_list):
                    status = await get_order(order["ticker"], order["uuid"])
                    self.check_order_status(order["uuid"], status)
            except Exception as e:
                print(f"[{self.name}][run_order][error] {e}")

            await asyncio.sleep(1)

    async def run_check_open_orders(self):
        """
        거래소 전체 open orders와 로컬 waiting_list 정합성 맞추기
        """
        while True:
            try:
                order_list = await get_open_orders()
                if order_list is None:
                    await asyncio.sleep(30)
                    continue

                open_uuids = {o["uuid"] for o in order_list}
                local_uuids = {o["uuid"] for o in self.waiting_list}

                self.waiting_list = [o for o in self.waiting_list if o["uuid"] in open_uuids]
                new_orders = [o for o in order_list if o["uuid"] not in local_uuids]
                self.waiting_list.extend(new_orders)

            except Exception as e:
                print(f"[{self.name}][run_check_open_orders][error] {e}")

            await asyncio.sleep(30)

    async def run_funding(self):
        """
        현재 펀딩비 주기적 갱신
        """
        while True:
            try:
                for ticker in self.ticker_list:
                    funding = await get_funding_rate(ticker)
                    if funding is not None:
                        self.funding_now[ticker] = funding
            except Exception as e:
                print(f"[{self.name}][run_funding][error] {e}")

            await asyncio.sleep(10)

    async def preload_funding_history(self, ticker: str, limit: int = 30):
        """
        과거 펀딩비 preload
        """
        history = await get_funding_rate_history(ticker, limit=limit)
        if history is not None:
            self.funding_history[ticker] = history
        return history

    async def get_ohlcv_snapshot(self, ticker: str, limit: int = 120):
        """
        1분봉 조회 helper
        """
        return await get_ohlcv_1m(ticker, limit=limit)

    async def run_health(self):
        """
        websocket stale / drift 체크
        """
        while True:
            try:
                now_ms = int(time.time() * 1000)

                for ticker in self.ticker_list:
                    last_update = self.ws_last_update_ms.get(ticker, 0)
                    stale_ms = now_ms - last_update if last_update else None
                    drift_ms = self.ws_drift_last_ms.get(ticker)

                    can_trade = True
                    reasons = []

                    if drift_ms is not None and drift_ms > self.max_server_pc_drift_ms:
                        can_trade = False
                        reasons.append(f"drift>{self.max_server_pc_drift_ms}ms")

                    if stale_ms is not None and stale_ms > 1500:
                        can_trade = False
                        reasons.append("stale_orderbook")

                    self.hedge_state[ticker] = {
                        "can_trade": can_trade,
                        "block_reasons": reasons,
                        "last_health_ts": now_ms,
                    }

            except Exception as e:
                print(f"[{self.name}][run_health][error] {e}")

            await asyncio.sleep(0.5)
    def update_account_state(self, account_data: dict, asset_data: dict):
        timestamp = int(time.time() * 1000)
        long_usd = 0.0
        short_usd = 0.0

        # 1) 모든 ticker 기본 account 상태
        account_dict = {
            ticker: self.default_dict(timestamp)
            for ticker in self.ticker_list
        }

        # 2) 모든 ticker 기본 position snapshot 상태
        position_snapshot = {
            ticker: {
                "qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "unrealized_profit": 0.0,
                "position_side": "BOTH",
                "timestamp": timestamp,
            }
            for ticker in self.ticker_list
        }

        # 3) 모든 ticker 기본 net exposure
        net_exposure = {
            ticker: 0.0
            for ticker in self.ticker_list
        }

        # 4) 실제 응답 덮어쓰기
        for ticker, data in account_data.items():
            if ticker not in self.ticker_list:
                continue

            entry = account_dict[ticker]
            entry.update(data)

            entry["nowPrice"] = self.ob.get(ticker, {}).get("bid", 0.0)
            px_for_notional = self.ob.get(ticker, {}).get("ask", 0.0)

            qty = float(entry.get("balance", 0.0))

            if qty >= 0:
                long_usd += qty * px_for_notional
            else:
                short_usd += qty * px_for_notional

            position_snapshot[ticker] = {
                "qty": qty,
                "entry_price": float(entry.get("avg_price", 0.0)),
                "mark_price": float(entry.get("markPrice", 0.0)),
                "unrealized_profit": float(entry.get("unrealized_profit", 0.0)),
                "position_side": entry.get("positionSide", "BOTH"),
                "timestamp": timestamp,
            }

            net_exposure[ticker] = qty

        # 5) 자산 상태
        asset_dict = {
            ticker: {
                "balance": float(data.get("balance", 0.0)),
                "availableBalance": float(data.get("availableBalance", 0.0)),
                "marginBalance": float(data.get("marginBalance", 0.0)),
                "locked": float(data.get("locked", 0.0)),
                "avgPrice": float(data.get("avgPrice", 0.0)),
                "nowPrice": 1.0,
                "unrealizedProfit": float(data.get("unrealizedProfit", 0.0)),
                "timestamp": timestamp,
            }
            for ticker, data in asset_data.items()
        }

        usdc = asset_dict.get("USDC", {"balance": 0.0, "unrealizedProfit": 0.0})
        asset_dict["USD"] = {
            "balance": float(usdc["balance"]),
            "locked": 0.0,
            "avgPrice": 1.0,
            "nowPrice": 1.0,
            "unrealizedProfit": float(usdc.get("unrealizedProfit", 0.0)),
            "timestamp": timestamp,
        }

        total_balance = asset_dict["USD"]["balance"] + asset_dict["USD"]["unrealizedProfit"]
        total_position = long_usd + abs(short_usd)
        delta_position = long_usd + short_usd

        # 6) 최종 반영
        self.account_dict = account_dict
        self.position_snapshot = position_snapshot
        self.net_exposure = net_exposure
        self.asset_dict = asset_dict
        self.total_balance = total_balance
        self.leverage = total_position / total_balance if total_balance else 0.0
        self.delta_leverage = delta_position / total_balance if total_balance else 0.0

    def default_dict(self, timestamp: int):
        """
        바이낸스 쪽과 최대한 유사한 기본 상태
        """
        return {
            "balance": 0.0,
            "locked": 0.0,
            "avgPrice": 0.0,
            "avg_price": 0.0,
            "nowPrice": 0.0,
            "unrealizedProfit": 0.0,
            "unrealized_profit": 0.0,
            "markPrice": 0.0,
            "positionSide": "BOTH",
            "timestamp": timestamp,
        }

    def can_trade_symbol(self, ticker: str) -> bool:
        state = self.hedge_state.get(ticker, {})
        return state.get("can_trade", True)

    async def order(self, ticker: str, side: str, amount: float, price: float):
        """
        지정가 주문 진입
        """
        if not self.can_trade_symbol(ticker):
            return None

        key = f"{ticker}_{side}"

        if time.time() - self.last_order_time.get(key, 0) >= 1:
            self.last_order_time[key] = time.time()

            order_data = await post_limit_order(
                ticker,
                side,
                amount,
                price,
                {}
            )

            if order_data and "error" not in order_data:
                self.waiting_list.append(order_data)
                return order_data

        return None

    async def cancel_order(self, ticker: str, uuid: str):
        """
        단일 주문 취소
        """
        if time.time() - self.last_delete_time.get(ticker, 0) >= 0.5:
            self.last_delete_time[ticker] = time.time()
            result = await delete_order(ticker, uuid)
            self.handle_cancel_result(result, uuid)
            return result

        return None

    def check_order_status(self, uuid: str, status: dict):
        """
        단일 주문 상태 반영
        """
        if status:
            if status.get("state") == "not_exist" or status.get("error") == "not_exist":
                self.waiting_list = [o for o in self.waiting_list if o["uuid"] != uuid]

    def handle_cancel_result(self, result: dict, uuid: str):
        """
        주문 취소 결과 반영
        """
        if result:
            if result.get("error") != "not_exist":
                self.waiting_list = [o for o in self.waiting_list if o["uuid"] != uuid]
