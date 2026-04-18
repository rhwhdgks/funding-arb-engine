# strategy.py
import time, asyncio
from .util import ceil_by_tick, floor_by_tick


class Strategy:
    def __init__(self, ticker, max_qty, trade_qty, spread, cancel_buffer):
        self.ticker = ticker
        self.max_qty = max_qty
        self.trade_qty = trade_qty
        self.spread = spread
        self.cancel_buffer = cancel_buffer if cancel_buffer is not None else 0
        self.maker_last_account = None
        self.taker_last_account = None
        self.start_time = int(time.time())

    async def __call__(self, maker, taker):
        tkr = self.ticker
        try:
            # 오더북 준비됐는지 확인
            if tkr not in taker.ob or tkr not in maker.ob or int(time.time()) - self.start_time < 3:
                return

            ask_price = taker.ob[tkr]['ask']       # 기준가 = taker ask (시장가 예상 평단가)
            tick   = maker.ticker_info[tkr]['tick_size']

            # 헷지는 항상 백그라운드 태스크
            asyncio.create_task(self._hedge_if_needed(maker, taker, ask_price))

            # ────── cancel-buffer 로 주문가·허용 범위 계산 ──────
            low_ratio   = self.spread                               # 예: 0.0001
            high_ratio  = self.spread + self.cancel_buffer          # 예: 0.00015
            target_ratio= self.spread + self.cancel_buffer / 2      # 예: 0.000125

            target_price = ceil_by_tick(ask_price * (1 + target_ratio), tick)
            target_price = max(target_price, maker.ob[tkr]['best_ask'])   # 호가보다 낮게는 X, 메이커 체결 보장

            # 신규 주문(없으면) 생성
            if not maker.waiting_list and maker.account_dict[tkr]['balance'] > -self.max_qty:
                asyncio.create_task(
                    maker.order(tkr, 'sell', self.trade_qty, target_price)
                )
            # 기존 주문 검증: 스프레드가 허용 범위 밖이면 취소
            for o in maker.waiting_list:
                prev_price = o['price']
                prev_ratio = prev_price / ask_price - 1

                if self.cancel_buffer == 0:
                    if target_price != prev_price:
                        asyncio.create_task(maker.cancel_order(o['ticker'], o['uuid']))
                        continue
                else:
                    if not (low_ratio <= prev_ratio <= high_ratio) and target_price != maker.ob[tkr]['best_ask']:
                        asyncio.create_task(maker.cancel_order(o['ticker'], o['uuid']))
                        continue

                # 수량이 (거의) 다 채워지면 주문 취소
                if self.max_qty * (0.999) < abs(maker.account_dict[tkr]['balance']) < self.max_qty * (1.0001):
                    asyncio.create_task(maker.cancel_order(o['ticker'], o['uuid']))

            # 5) 체결 출력
            asyncio.create_task(self.trade_print(maker, taker))
        except Exception as e:
            print("[전략 오류]", e)


    async def _hedge_if_needed(self, maker, taker, taker_price: float | None):
        tkr = self.ticker
        if taker_price is None or tkr not in maker.account_dict or tkr not in taker.account_dict:
            return
        now = time.time() * 1000
        if (now - maker.account_dict[tkr]['timestamp'] < 1000 and
            now - taker.account_dict[tkr]['timestamp'] < 1000):
            sum_qty = -(maker.account_dict[tkr]['balance'] + taker.account_dict[tkr]['balance'])

            if sum_qty > 0:
                if sum_qty > taker.ticker_info[tkr]['min_q']:
                    hedge_price = ceil_by_tick(
                        taker_price * 1.005, # 시장가처럼 거래가 되기 위해
                        taker.ticker_info[tkr]['tick_size']
                    )
                    await taker.order(tkr, 'buy', sum_qty, hedge_price)
            else:
                abs_sum_qty = abs(sum_qty)
                if abs_sum_qty > taker.ticker_info[tkr]['min_q']:
                    hedge_price = floor_by_tick(
                        taker_price * 0.995, # 시장가처럼 거래가 되기 위해
                        taker.ticker_info[tkr]['tick_size']
                    )
                    await taker.order(tkr, 'sell', abs_sum_qty, hedge_price)


    async def trade_print(self, maker, taker):
        last_maker_balance = 0
        last_taker_balance = 0
        cur_maker_balance = 0
        cur_taker_balance = 0

        if self.maker_last_account is None:
            self.maker_last_account = maker.account_dict
        
        if self.taker_last_account is None:
            self.taker_last_account = taker.account_dict

        if self.ticker in maker.account_dict:
            cur_maker_balance = maker.account_dict[self.ticker]['balance']

        if self.ticker in taker.account_dict:
            cur_taker_balance = taker.account_dict[self.ticker]['balance']

        if self.ticker in self.maker_last_account:
            last_maker_balance = self.maker_last_account[self.ticker]['balance']

        if self.ticker in self.taker_last_account:
            last_taker_balance = self.taker_last_account[self.ticker]['balance']

        self.maker_last_account = maker.account_dict
        self.taker_last_account = taker.account_dict

        if last_maker_balance - cur_maker_balance != 0 or last_taker_balance - cur_taker_balance != 0:
            taker_avg_price = await taker.avg_cost_from_qty(self.ticker, cur_taker_balance)
            maker_avg_price = maker.account_dict[self.ticker]['avg_price']

            if last_maker_balance - cur_maker_balance != 0:
                print("[메이커 체결]")
            else:
                print("[테이커 체결]")
            print("maker", maker.name, f"수량: {cur_maker_balance:.6f}", f"평단: {maker_avg_price:.2f}")
            print("taker", taker.name, f"수량: {cur_taker_balance:.6f}", f"평단: {taker_avg_price:.2f}\n")
