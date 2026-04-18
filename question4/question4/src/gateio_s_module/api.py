import os, time, hmac, hashlib, aiohttp, json, decimal
from dotenv import load_dotenv
from typing import Optional, Tuple

load_dotenv()

GATE_KEY    = os.getenv("GATEIO_API_KEY")
GATE_SECRET = os.getenv("GATEIO_API_SECRET")
BASE_URL    = "https://api.gateio.ws"
start_time = int(time.time())


def gen_sign(method: str, url: str, query_string: str = "", payload_string: str = "") -> dict:
    """Return auth headers (KEY, Timestamp, SIGN)"""
    t  = str(time.time())
    m  = hashlib.sha512()
    m.update((payload_string or "").encode("utf-8"))
    hashed_body = m.hexdigest()
    prehash     = f"{method}\n{url}\n{query_string}\n{hashed_body}\n{t}"
    sign        = hmac.new(GATE_SECRET.encode(), prehash.encode(), hashlib.sha512).hexdigest()
    return {"KEY": GATE_KEY, "Timestamp": t, "SIGN": sign}

COMMON_HDR = {"Accept": "application/json", "Content-Type": "application/json"}


async def get_ticker_list() -> Tuple[list, dict]:
    path = "/api/v4/spot/currency_pairs"
    async with aiohttp.ClientSession() as s:
        r     = await s.get(BASE_URL + path)
        pairs = await r.json()
    tl, info = [], {}
    for p in pairs:
        if p.get("quote") == "USDT":
            symbol = p["id"]        
            base   = symbol.replace("_USDT", "")
            tl.append(base)
            info[base] = {
                "min_q": float(p["min_base_amount"]),
                "min_price":  float(p["min_quote_amount"]),
                "tick_size":  10 ** (float(p["precision"]) * -1)
            }
    return tl, info


def _order_payload(pair: str, side: str, amount, price=None, tif="gtc", otype="limit"):
    body = {
        "currency_pair": pair,
        "side": side.lower(),            # buy / sell
        "type": otype                    # limit / market
    }
    if otype == "limit":
        body.update({"amount": str(amount), "price": str(price), "time_in_force": tif})
    else:  # market
        body.update({"amount": str(amount)})
    return json.dumps(body)


async def post_limit_order(symbol: str, side: str, amount: float, price: float):
    pair    = f"{symbol.upper()}_USDT"
    path    = "/api/v4/spot/orders"
    payload = _order_payload(pair, side, amount, price, tif="gtc", otype="limit")
    hdr     = gen_sign("POST", path, payload_string=payload); hdr.update(COMMON_HDR)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(BASE_URL + path, headers=hdr, data=payload) as r:
                res = await r.json()
                if isinstance(res, dict):
                    return {
                        'ticker': symbol,
                        'uuid': res['id'],
                        'price': float(res['price']) if 'price' in res else None,
                        'qty': float(amount),
                        'timestamp': int(time.time() * 1000),
                        'identifier': f"{symbol}_{amount}_{float(res['price']) if 'price' in res else "market"}"
                    }
                else: 
                    return None
        except:
            return None


async def post_market_order(symbol: str, side: str, amount: float):
    pair = f"{symbol.upper()}_USDT"
    path = "/api/v4/spot/orders"
    payload = _order_payload(pair, side, amount, otype="market")
    hdr = gen_sign("POST", path, payload_string=payload); hdr.update(COMMON_HDR)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(BASE_URL + path, headers=hdr, data=payload) as r:
                res = await r.json()
                return res if isinstance(res, dict) else None
        except:
            return None

async def get_order(order_id: str, symbol: str):
    pair = f"{symbol.upper()}_USDT"
    path = f"/api/v4/spot/orders/{order_id}"
    q    = f"currency_pair={pair}"
    hdr  = gen_sign("GET", path, query_string=q); hdr.update(COMMON_HDR)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(BASE_URL + path + "?" + q, headers=hdr) as r:
                data = await r.json()
                if "message" in data:
                    return data
                else:
                    if (data['status'] == "open"):
                        return {"state": "exist"}
                    else:
                        return {"state": "not_exist"}
        except:
            return None


async def delete_order(order_id: str, symbol: str):
    pair = f"{symbol.upper()}_USDT"
    path = f"/api/v4/spot/orders/{order_id}"
    q    = f"currency_pair={pair}"
    hdr  = gen_sign("DELETE", path, query_string=q); hdr.update(COMMON_HDR)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.delete(BASE_URL + path + "?" + q, headers=hdr) as r:
                data = await r.json()
                if "message" in data:
                    return data
                else:
                    if (data['status'] == "open"):
                        return {"state": "exist"}
                    else:
                        return {"state": "not_exist"}
        except:
            return None


async def get_my_trades(symbol: str,
                        limit: int = 1000) -> list[dict]:
    pair  = f"{symbol}_USDT"
    q     = [f"currency_pair={pair}", f"limit={limit}", f"from={start_time}"]
    q     = "&".join(q)

    path  = "/api/v4/spot/my_trades"
    hdr   = gen_sign("GET", path, query_string=q); hdr.update(COMMON_HDR)

    async with aiohttp.ClientSession() as s:
        async with s.get(f"{BASE_URL}{path}?{q}", headers=hdr) as r:
            data = await r.json()
            return data if isinstance(data, list) else []


# FIFO 평단 계산 (기존과 동일)
def fifo_cost_basis(trades: list[dict], current_qty: float) -> float:
    if current_qty <= 0:
        return 0.0

    trades = sorted(trades, key=lambda t: int(float(t["create_time_ms"])))
    lots: list[tuple[decimal.Decimal, decimal.Decimal]] = []

    for t in trades:
        qty   = decimal.Decimal(t["amount"])
        price = decimal.Decimal(t["price"])
        if t["side"] == "buy":
            lots.append([qty, price])
        else:                               
            sell_qty = qty
            while sell_qty > 0 and lots:
                lot_qty, lot_price = lots[0]
                if lot_qty <= sell_qty:
                    sell_qty -= lot_qty
                    lots.pop(0)
                else:           
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
    trades   = await get_my_trades(symbol, limit=1000)
    return fifo_cost_basis(trades, cur_qty)


async def get_accounts() -> Optional[list]:
    path   = "/api/v4/spot/accounts"
    header = gen_sign("GET", path)
    header.update(COMMON_HDR)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(BASE_URL + path, headers=header) as r:
                data = await r.json()
                return process_account_data(data)
        except:
            return None


def process_account_data(data):
    account_dict = {}
    asset_dict = {}
    timestamp = int(time.time() * 1000)
    
    for ticker_info in data:
        ticker = ticker_info['currency']
        locked = float(ticker_info['locked'])
        balance = float(ticker_info['available']) + locked

        if ticker_info['currency'] == "USDT":
            asset_dict[ticker] = {
                "balance": balance,
                "locked": locked,
                "timestamp": timestamp
            }
        else:
            account_dict[ticker] = {
                "balance": balance,
                "locked": locked,
                "timestamp": timestamp
            }
    return [account_dict, asset_dict]