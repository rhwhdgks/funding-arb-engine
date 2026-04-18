import os, time, hmac, hashlib, aiohttp, decimal, asyncio
from typing import List
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
BASE_URL = 'https://api.binance.com'
start_time = int(time.time()) * 1000

def create_signature(query_string, secret_key=BINANCE_SECRET_KEY):
    return hmac.new(
        secret_key.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

async def get_account(option: dict = {}):
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/api/v3/account?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return process_account_data(data)
            else:
                print("Error in get_account:", data)
                return None, None

async def post_limit_order(symbol, side, amount, price, option: dict = {}):
    symbol = symbol.upper() + "USDT"
    side = side.upper()
    timestamp = int(time.time() * 1000)
    query_string = (
        f"symbol={symbol}&side={side}&type=LIMIT&timeInForce=GTC"
        f"&quantity={amount:.8f}&price={price:.8f}&timestamp={timestamp}"
    )
    signature = create_signature(query_string)
    url = f"{BASE_URL}/api/v3/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return {
                    'ticker': data['symbol'].replace("USDT", ""),
                    'uuid': data['orderId'],
                    'price': float(data['price']),
                    'qty': float(data['origQty']),
                    'timestamp': int(time.time() * 1000),
                    'identifier': f"{data['symbol']}_{data['origQty']}_{data['price']}"
                }
            else:
                return {"error": data.get("msg", "Unknown error")}


async def post_market_order(symbol, side, amount, option: dict = {}):
    symbol = symbol.upper() + "USDT"
    side = side.upper()
    timestamp = int(time.time() * 1000)
    query_string = (
        f"symbol={symbol}&side={side}&type=MARKET"
        f"&quantity={amount:.8f}&timestamp={timestamp}"
    )
    signature = create_signature(query_string)
    url = f"{BASE_URL}/api/v3/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return {
                    'ticker': data['symbol'].replace("USDT", ""),
                    'uuid': data['clientOrderId'],
                    'price': float(data['avgFillPrice']) if 'avgFillPrice' in data else None,
                    'qty': float(data['executedQty']),
                    'timestamp': int(time.time() * 1000),
                    'identifier': f"{data['symbol']}_{data['executedQty']}_{data.get('avgFillPrice', 'market')}"
                }
            else:
                return {"error": data.get("msg", "Unknown error")}

async def get_order(symbol, order_id):
    symbol = symbol.upper() + "USDT"
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&orderId={order_id}&timestamp={timestamp}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/api/v3/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                status = data.get("status")
                if status in ["FILLED", "CANCELED", "EXPIRED", "REJECTED"]:
                    return {"state": "not_exist"}
                elif status == "NEW":
                    return {"state": "exist"}
            elif response.status == 400 and data.get("code") == -2013:
                return {"state": "not_exist"}
            else:
                return {"error": data.get("msg", "Unknown error")}

async def delete_order(symbol, order_id):
    symbol = symbol.upper() + "USDT"
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&orderId={order_id}&timestamp={timestamp}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/api/v3/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.delete(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return data
            elif response.status == 400 and data.get("code") in [-2011, -2013]:
                return {"error": "not_exist"}
            else:
                return {"error": data.get("msg", "Unknown error")}


async def get_ticker_list():
    url = f"{BASE_URL}/api/v3/exchangeInfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            ticker_list = []
            info = {}
            for symbol in data['symbols']:
                if symbol['quoteAsset'] == 'USDT' and symbol['status'] == 'TRADING':
                    ticker = symbol['symbol']
                    ticker_list.append(ticker.replace("USDT", ""))
                    filters = {f['filterType']: f for f in symbol['filters']}
                    info[ticker.replace("USDT", "")] = {
                        'min_q': float(filters['LOT_SIZE']['minQty']),
                        'max_q': float(filters['LOT_SIZE']['maxQty']),
                        'tick_size': float(filters['PRICE_FILTER']['tickSize'])
                    }
            return ticker_list, info


def process_account_data(data):
    account_dict = {}
    asset_dict = {}
    timestamp = int(time.time() * 1000)

    for i in range(len(data['balances'])):
        asset_info = data['balances'][i]
        asset = asset_info['asset']

        if asset == "USDT":
            asset_dict[asset] = {
                'balance': float(asset_info['free']) + float(asset_info['locked']),
                'now_price': 1,
                'locked': float(asset_info['locked']),
                'avg_price': 1,
                'timestamp': timestamp
            }
        else:
            account_dict[asset] = {
                'balance': float(asset_info['free']) + float(asset_info['locked']),
                'now_price': None,
                'locked': float(asset_info['locked']),
                'avg_price': None,
                'timestamp': timestamp
            }

    return account_dict, asset_dict

async def get_my_trades(symbol: str,
                        limit: int = 1000) -> List[dict]:
    ts = int(time.time() * 1000)
    qs = [f"symbol={symbol}USDT", f"limit={limit}", f"timestamp={ts}", f"startTime={start_time}"]

    query = "&".join(qs)
    sig   = create_signature(query)
    url   = f"{BASE_URL}/api/v3/myTrades?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as s:
        async with s.get(url, headers=headers) as r:
            data = await r.json()
            return data if r.status == 200 and isinstance(data, list) else []

def fifo_cost_basis(trades: List[dict], current_qty: float) -> float:
    """
    trades      : buy/sell 포함, time 오래된 순 정렬 필요
    current_qty : 외부에서 주는 현 잔고(수량)
    """
    if current_qty <= 0:
        return 0.0

    # 오래된 체결 → 최신 체결 순서
    trades = sorted(trades, key=lambda t: int(t["time"]))
    lots: list[tuple[decimal.Decimal, decimal.Decimal]] = []  # [(qty, price)]

    for t in trades:
        qty   = decimal.Decimal(t["qty"])
        price = decimal.Decimal(t["price"])
        side  = "buy" if t["isBuyer"] else "sell"

        if side == "buy":
            lots.append([qty, price])
        else:  # sell
            sell_qty = qty
            while sell_qty > 0 and lots:
                lot_qty, lot_price = lots[0]
                if lot_qty <= sell_qty:        # lot 소진
                    sell_qty -= lot_qty
                    lots.pop(0)
                else:  # 일부 소진
                    lots[0][0] = lot_qty - sell_qty
                    sell_qty = decimal.Decimal("0")

    remain_qty  = sum(q for q, _ in lots)
    if remain_qty == 0:
        return 0.0
    remain_cost = sum(q * p for q, p in lots)
    return float(remain_cost / remain_qty)

async def avg_cost_from_qty(symbol: str,
                            cur_qty: float) -> float:
    if cur_qty <= 0:
        return 0.0

    trades   = await get_my_trades(symbol)
    return fifo_cost_basis(trades, cur_qty)



# async def monitor_avg_cost(symbol="BTC",
#                            interval=5):
#     while True:
#         try:
#             accounts = await get_account()
#             bal_dict = accounts[0]
#             cur_qty  = bal_dict.get(symbol, {}).get("balance", 0.0)
#             cost = await avg_cost_from_qty(symbol, cur_qty)
#             print(f"[{time.strftime('%H:%M:%S')}] {symbol} 잔고 {cur_qty}개 | 평단 {cost:,.2f} USD")
#         except Exception as e:
#             print("⛔ monitor error:", e)
#         await asyncio.sleep(interval)

# if __name__ == "__main__":
#     # 예) 다른 스레드 또는 WS에서 실시간 업데이트하는 변수라고 가정
#     asyncio.run(monitor_avg_cost("BTC"))

# if __name__ == "__main__":
#     import asyncio

#     async def main():
# #         # print("✅ 계정 자산 조회 중...")
# #         # balances = await get_account()

# #         # print("✅ 티커 목록 조회 중...")
# #         # tickers, info = await get_ticker_list()
# #         # print(f"Tickers 개수: {info}")

#         # ⚠️ 실제 주문은 아래 코드 활성화 전 반드시 확인
#         print("✅ 시장가 주문 테스트 (주의)")
#         order = await post_limit_order("BTC", "BUY", 0.0001, 103000)
#         print("Market Order:", order)

#         print("✅ 주문 조회 테스트")
#         order_status = await get_order("BTC", order["uuid"])
#         print("Order Status:", order_status)

#         print("✅ 주문 취소 테스트")
#         result = await delete_order("BTC", order["uuid"])
#         print("Cancel Result:", result)

#     asyncio.run(main())