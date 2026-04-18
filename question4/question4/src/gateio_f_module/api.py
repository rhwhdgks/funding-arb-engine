import os
import time
import hmac
import hashlib
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()

GATE_KEY = os.getenv("GATEIO_API_KEY")
GATE_SECRET = os.getenv("GATEIO_API_SECRET")
BASE_URL = "https://api.gateio.ws"
SETTLE = "usdt"
COMMON_HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json'}


def gen_sign(method: str, url: str, query_string: str = "", payload_string: str = "") -> dict:
    t = str(time.time())
    m = hashlib.sha512()
    m.update(payload_string.encode('utf-8'))
    hashed_payload = m.hexdigest()
    s = f"{method}\n{url}\n{query_string}\n{hashed_payload}\n{t}"
    sign = hmac.new(GATE_SECRET.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'KEY': GATE_KEY, 'Timestamp': t, 'SIGN': sign}


def process_account_data(data, info):
    account_dict = {}
    timestamp = int(time.time() * 1000)
    for d in data:
        symbol = d['contract'].replace("_USDT", "")
        if symbol in info:
            account_dict[symbol] = {
                'balance': float(d['size']) * info[symbol]['cont_size'],
                'now_price': None,
                'locked': 0,
                'unrealized_profit': float(d['unrealised_pnl']),
                'avg_price': float(d['entry_price']),
                'timestamp': timestamp
            }
    return account_dict


async def get_account(option: dict = {}):
    info = option['info']
    path = f"/api/v4/futures/{SETTLE}/positions"
    headers = gen_sign("GET", path)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + path, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return process_account_data(data, info)
            else:
                print("[get_account] 에러:", data)
                return None


async def get_balance(option: dict = {}):
    path = f"/api/v4/futures/{SETTLE}/accounts"
    headers = gen_sign("GET", path)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + path, headers=headers) as response:
            data = await response.json()
            if "message" in data:
                print("[get_balance] 에러:", data)
                return None
            return float(data['available'])


async def get_ticker_list():
    path = f"/api/v4/futures/{SETTLE}/contracts"
    async with aiohttp.ClientSession() as s:
        r = await s.get(BASE_URL + path)
        data = await r.json()

        if "message" in data:
            print("[get_ticker_list] 에러:", data)
            return None

        ticker_list = []
        ticker_info = {}

        for t in data:
            ticker = t['name'].replace("_USDT", "")
            ticker_list.append(ticker)
            ticker_info[ticker] = {
                "min_q": float(t['order_size_min']),
                "tick_size": float(t['order_price_round']),
                "cont_size": float(t['quanto_multiplier'])
            }

        return ticker_list, ticker_info


async def post_limit_order(contract: str, side: str, amount: float, price: float, option: dict = {}):
    parsed_amount = int(amount / option["info"][contract]['cont_size'])
    if side == "sell":
        parsed_amount *= -1

    path = f"/api/v4/futures/{SETTLE}/orders"
    payload = json.dumps({
        "contract": contract + "_USDT",
        "size": parsed_amount,
        "price": price,
        "side": side.lower(),
        "tif": "gtc",
    })
    headers = gen_sign("POST", path, payload_string=payload)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.post(BASE_URL + path, headers=headers, data=payload) as response:
            data = await response.json()
            if "message" in data:
                print("[post_limit_order] 에러:", data)
                return None
            return {
                'ticker': data['contract'].replace("_USDT", ""),
                'uuid': data['id'],
                'price': float(data['price']),
                'qty': parsed_amount,
                'timestamp': int(time.time() * 1000),
                'identifier': f"{data['contract'].replace('_USDT', '')}_{data['size']}_{data['price']}"
            }


async def post_market_order(contract: str, side: str, amount: float, option: dict = {}):
    parsed_amount = int(amount / option["info"][contract]['cont_size'])
    if side == "sell":
        parsed_amount *= -1

    path = f"/api/v4/futures/{SETTLE}/orders"
    payload = json.dumps({
        "contract": contract + "_USDT",
        "size": parsed_amount,
        "price": 0,
        "tif": "fok"
    })
    headers = gen_sign("POST", path, payload_string=payload)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.post(BASE_URL + path, headers=headers, data=payload) as response:
            data = await response.json()
            if "message" in data:
                print("[post_market_order] 에러:", data)
                return None
            return {
                'ticker': data['contract'].replace("_USDT", ""),
                'uuid': data['id'],
                'price': float(data['price']),
                'qty': parsed_amount,
                'timestamp': int(time.time() * 1000),
                'identifier': f"{data['contract'].replace('_USDT', '')}_{data['size']}_{data['price']}"
            }


async def get_order(order_id: str, option: dict = {}):
    path = f"/api/v4/futures/{SETTLE}/orders/{order_id}"
    headers = gen_sign("GET", path)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + path, headers=headers) as response:
            data = await response.json()
            if "message" in data:
                print("[get_order] 에러:", data)
                return None
            return {"state": "exist"} if data['status'] == "open" else {"state": "not_exist"}


async def get_open_orders(option: dict = {}):
    path = f"/api/v4/futures/{SETTLE}/orders"
    query_param = 'status=open'
    headers = gen_sign("GET", path, query_param)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + path + "?" + query_param, headers=headers) as response:
            data = await response.json()
            if "message" in data:
                print("[get_order] 에러:", data)
                return None
            else:
                order_list = []
                for order in data:
                    order_list.append({
                        'ticker': order['contract'].replace("_USDT", ""),
                        'uuid': order['id'],
                        'price': float(order['price']),
                        'qty': order['size'],
                        'timestamp': int(time.time() * 1000),
                        'identifier': f"{order['contract'].replace('_USDT', '')}_{order['size']}_{order['price']}"
                    })
                return order_list


async def delete_order(order_id: str, option: dict = {}):
    path = f"/api/v4/futures/{SETTLE}/orders/{order_id}"
    headers = gen_sign("DELETE", path)
    headers.update(COMMON_HEADERS)

    async with aiohttp.ClientSession() as session:
        async with session.delete(BASE_URL + path, headers=headers) as response:
            data = await response.json()
            if "message" in data:
                print("[delete_order] 에러:", data)
                return None

            if 'status' in data:
                return {"state": "exist"} if data['status'] == "open" else {"state": "not_exist"}
            if data.get('label') == 'ORDER_NOT_FOUND':
                return {"state": "not_exist"}
            return None
        