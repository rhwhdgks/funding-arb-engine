import os
import time
import hmac
import hashlib
import aiohttp
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
BASE_URL = "https://fapi.binance.com"


def create_signature(query_string: str, secret_key: str = BINANCE_SECRET_KEY) -> str:
    return hmac.new(
        secret_key.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def process_account_data(data: dict) -> tuple[dict, dict]:
    account_dict = {}
    asset_dict = {}
    timestamp = int(time.time() * 1000)

    for asset in data.get("assets", []):
        asset_dict[asset["asset"]] = {
            "balance": float(asset["walletBalance"]),
            "unrealizedProfit": float(asset["unrealizedProfit"]),
            "now_price": None,
            "locked": 0,
            "avg_price": 0,
            "timestamp": timestamp
        }

    for position in data.get("positions", []):
        symbol = position["symbol"]
        if symbol.endswith("USDT") and "_" not in symbol:
            code = symbol.replace("USDT", "")
            account_dict[code] = {
                "balance": float(position["positionAmt"]),
                "now_price": None,
                "locked": 0,
                "unrealized_profit": float(position["unrealizedProfit"]),
                "avg_price": float(position["entryPrice"]),
                "timestamp": timestamp
            }

    return account_dict, asset_dict


async def get_accounts(option: dict = {}) -> tuple[dict, dict] | tuple[None, None]:
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v2/account?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return process_account_data(data)
            else:
                print("Error in get_accounts:", data)
                return None, None


async def post_limit_order(symbol: str, side: str, amount: float, price: float, option: dict = {}) -> dict:
    symbol = symbol.upper().rstrip("USDT") + "USDT"
    side = side.upper()
    timestamp = int(time.time() * 1000)

    query_string = (
        f"timestamp={timestamp}&symbol={symbol}&side={side}&type=LIMIT"
        f"&quantity={amount:.4f}&price={price}&timeInForce=GTC"
    )
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v1/order?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return {
                    "ticker": data["symbol"].replace("USDT", ""),
                    "uuid": data["clientOrderId"],
                    "price": float(data["price"]),
                    "qty": float(data["origQty"]),
                    "timestamp": int(time.time() * 1000),
                    "identifier": f"{data['symbol']}_{data['origQty']}_{data['price']}"
                }
            return {"error": data.get("msg", "Unknown error")}


async def post_market_order(symbol: str, side: str, amount: float, option: dict = {}) -> dict:
    symbol = symbol.upper().rstrip("USDT") + "USDT"
    side = side.upper()
    timestamp = int(time.time() * 1000)

    query_string = (
        f"timestamp={timestamp}&symbol={symbol}&side={side}&type=MARKET"
        f"&quantity={amount:.4f}"
    )
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v1/order?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return {
                    "ticker": data["symbol"].replace("USDT", ""),
                    "uuid": data["clientOrderId"],
                    "price": float(data.get("avgFillPrice", 0)),
                    "qty": float(data["executedQty"]),
                    "timestamp": int(time.time() * 1000),
                    "identifier": f"{data['symbol']}_{data['executedQty']}_{data.get('avgFillPrice', 'market')}"
                }
            return {"error": data.get("msg", "Unknown error")}


async def get_order(symbol: str, client_order_id: str) -> dict:
    symbol = symbol.upper().rstrip("USDT") + "USDT"
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}&symbol={symbol}&origClientOrderId={client_order_id}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v1/order?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/json"
    }

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
            return {"error": data.get("msg", "Unknown error")}


async def delete_order(symbol: str, client_order_id: str) -> dict:
    symbol = symbol.upper().rstrip("USDT") + "USDT"
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}&symbol={symbol}&origClientOrderId={client_order_id}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v1/order?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.delete(url, headers=headers) as response:
            data = await response.json()
            if response.status == 200:
                return data
            elif response.status == 400 and data.get("code") in [-2011, -2013]:
                return {"error": "not_exist"}
            return {"error": data.get("msg", "Unknown error")}


async def get_ticker_list() -> tuple[list[str], dict]:
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    headers = {"Content-Type": "application/json"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch exchangeInfo: {response.status}")

            data = await response.json()
            ticker_list = []
            info = {}

            for symbol in data.get("symbols", []):
                if (
                    symbol.get("contractType") == "PERPETUAL"
                    and symbol.get("symbol", "").endswith("USDT")
                    and symbol.get("status") == "TRADING"
                ):
                    coin = symbol["symbol"].replace("USDT", "")
                    ticker_list.append(coin)

                    filters = symbol.get("filters", [])
                    info[coin] = {
                        "min_q": float(filters[2]["minQty"]),
                        "max_q": float(filters[2]["maxQty"]),
                        "tick_size": float(filters[0]["tickSize"]),
                    }

            return ticker_list, info


async def get_open_orders():
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = create_signature(query_string)
    url = f"{BASE_URL}/fapi/v1/openOrders?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()

            if response.status == 200:
                order_list = []
                for order in data:
                    parse_order = {
                        "ticker": order["symbol"].replace("USDT", ""),
                        "uuid": order["clientOrderId"],
                        "price": float(order["price"]),
                        "qty": float(order["origQty"]),
                        "timestamp": int(time.time() * 1000),
                        "identifier": f"{order['symbol']}_{order['origQty']}_{order['price']}"
                    }
                    order_list.append(parse_order)
                return order_list
            else:
                return None