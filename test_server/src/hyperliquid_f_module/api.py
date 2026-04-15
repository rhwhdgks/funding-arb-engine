import os
import time
import asyncio
import aiohttp
from dotenv import load_dotenv

from eth_account import Account
from hyperliquid.exchange import Exchange as HlExchange
from hyperliquid.info import Info as HlInfo
from hyperliquid.utils import constants

load_dotenv()

# =========================================================
# env / constants
# =========================================================

HYPERLIQUID_ACCESS_KEY = os.getenv("HYPERLIQUID_ACCESS_KEY")   # 메인 계정 주소
HYPERLIQUID_SECRET_KEY = os.getenv("HYPERLIQUID_SECRET_KEY")   # API wallet or private key
HYPERLIQUID_USE_TESTNET = os.getenv("HYPERLIQUID_USE_TESTNET", "false").lower() == "true"

INFO_URL = "https://api.hyperliquid.xyz/info"
if HYPERLIQUID_USE_TESTNET:
    API_URL = constants.TESTNET_API_URL
else:
    API_URL = constants.MAINNET_API_URL

# 전역 캐시
_HL_INFO = None
_HL_EXCHANGE = None
_META_CACHE = None

_ASSET_MAP = {}      # {"BTC": 0, "ETH": 1, ...}
_TICKER_LIST = []    # ["BTC", "ETH", ...]
_SYMBOL_META = {}    # {"BTC": {"asset": 0, "sz_decimals": 5, "raw": {...}}, ...}


# =========================================================
# client bootstrap
# =========================================================

def _build_wallet():
    """
    SDK용 signer wallet 생성
    """
    if not HYPERLIQUID_SECRET_KEY:
        raise ValueError("HYPERLIQUID_SECRET_KEY is missing")
    return Account.from_key(HYPERLIQUID_SECRET_KEY)


def get_info_client() -> HlInfo:
    """
    Hyperliquid Info SDK client
    """
    global _HL_INFO
    if _HL_INFO is None:
        _HL_INFO = HlInfo(API_URL, skip_ws=True)
    return _HL_INFO


def get_exchange_client() -> HlExchange:
    """
    Hyperliquid Exchange SDK client
    """
    global _HL_EXCHANGE
    if _HL_EXCHANGE is None:
        wallet = _build_wallet()
        if not HYPERLIQUID_ACCESS_KEY:
            raise ValueError("HYPERLIQUID_ACCESS_KEY (account address) is missing")
        _HL_EXCHANGE = HlExchange(wallet, API_URL, account_address=HYPERLIQUID_ACCESS_KEY)
    return _HL_EXCHANGE


# =========================================================
# symbol / asset helpers
# =========================================================

def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.endswith("USDT"):
        s = s[:-4]
    return s


async def get_meta(force_refresh: bool = False) -> dict | None:
    """
    Hyperliquid perp meta 조회 + 전역 캐시 구성

    구성되는 캐시:
      - _META_CACHE
      - _ASSET_MAP
      - _TICKER_LIST
      - _SYMBOL_META
    """
    global _META_CACHE, _ASSET_MAP, _TICKER_LIST, _SYMBOL_META

    if _META_CACHE is not None and not force_refresh:
        return _META_CACHE

    payload = {"type": "meta"}

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                print("[get_meta] error:", data)
                return None

    if not isinstance(data, dict):
        print("[get_meta] invalid response:", data)
        return None

    _META_CACHE = data
    _ASSET_MAP = {}
    _TICKER_LIST = []
    _SYMBOL_META = {}

    universe = data.get("universe", [])
    for idx, entry in enumerate(universe):
        coin = entry.get("name")
        if not coin:
            continue

        symbol = normalize_symbol(coin)

        _TICKER_LIST.append(symbol)
        _ASSET_MAP[symbol] = idx
        _SYMBOL_META[symbol] = {
            "asset": idx,
            "sz_decimals": int(entry.get("szDecimals", 0) or 0),
            "raw": entry,
        }

    return _META_CACHE


def get_ticker_list() -> list[str]:
    """
    캐시된 전종목 심볼 리스트 반환
    get_meta() 선호출 필요
    """
    return _TICKER_LIST


def get_symbol_meta(symbol: str) -> dict:
    """
    심볼별 메타 반환
    """
    coin = normalize_symbol(symbol)
    if coin not in _SYMBOL_META:
        raise KeyError(f"Meta not loaded for symbol={coin}. Run get_meta() first.")
    return _SYMBOL_META[coin]


def get_asset_index(symbol: str) -> int:
    """
    BTC -> asset index
    meta 선조회 필요
    """
    return get_symbol_meta(symbol)["asset"]


# =========================================================
# info endpoint wrappers
# =========================================================

async def get_accounts(option: dict = {}) -> tuple[dict, dict] | tuple[None, None]:
    """
    clearinghouseState 기반 계정/포지션 조회
    반환 형식은 Binance 쪽과 최대한 비슷하게 맞춤:
      - account_dict: 종목별 포지션
      - asset_dict: 자산별 잔고
    """
    if not HYPERLIQUID_ACCESS_KEY:
        return None, None

    payload = {
        "type": "clearinghouseState",
        "user": HYPERLIQUID_ACCESS_KEY,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                print("Error in get_accounts:", data)
                return None, None

    timestamp = int(time.time() * 1000)
    account_dict = {}
    asset_dict = {}

    cross = data.get("crossMarginSummary", {}) or {}
    asset_dict["USDC"] = {
        "balance": float(cross.get("accountValue", 0) or 0),
        "availableBalance": float(cross.get("accountValue", 0) or 0),
        "marginBalance": float(cross.get("accountValue", 0) or 0),
        "unrealizedProfit": 0.0,
        "now_price": 1.0,
        "locked": 0.0,
        "avg_price": 1.0,
        "timestamp": timestamp,
    }

    for item in data.get("assetPositions", []):
        pos = item.get("position", {})
        coin = normalize_symbol(pos.get("coin", ""))
        if not coin:
            continue

        szi = float(pos.get("szi", 0) or 0)
        entry_px = float(pos.get("entryPx", 0) or 0)
        unreal = float(pos.get("unrealizedPnl", 0) or 0)

        account_dict[coin] = {
            "balance": szi,
            "now_price": None,
            "locked": 0.0,
            "unrealized_profit": unreal,
            "avg_price": entry_px,
            "markPrice": float(pos.get("markPx", 0) or 0),
            "positionSide": "LONG" if szi > 0 else ("SHORT" if szi < 0 else "BOTH"),
            "timestamp": timestamp,
        }

    return account_dict, asset_dict


async def get_open_orders(symbol: str | None = None):
    """
    open orders 조회
    SDK info.open_orders 사용
    """
    info = get_info_client()
    if not HYPERLIQUID_ACCESS_KEY:
        return None

    coin_filter = normalize_symbol(symbol) if symbol else None

    def _run():
        return info.open_orders(HYPERLIQUID_ACCESS_KEY)

    data = await asyncio.to_thread(_run)

    order_list = []
    for order in data:
        coin = normalize_symbol(order.get("coin", ""))
        if coin_filter and coin != coin_filter:
            continue

        order_list.append({
            "ticker": coin,
            "uuid": str(order.get("oid")),
            "order_id": str(order.get("oid")),
            "client_order_id": order.get("cloid"),
            "side": "buy" if order.get("side") in ("B", "buy", "BUY") else "sell",
            "position_side": "BOTH",
            "price": float(order.get("limitPx", 0) or 0),
            "qty": abs(float(order.get("sz", 0) or 0)),
            "executed_qty": 0.0,
            "avg_price": 0.0,
            "status": "OPEN",
            "reduce_only": bool(order.get("reduceOnly", False)),
            "timestamp": int(time.time() * 1000),
            "identifier": f"{coin}_{order.get('sz')}_{order.get('limitPx')}",
            "raw": order,
        })

    return order_list


async def get_funding_rate(symbol: str) -> dict | None:
    """
    현재 funding rate / mark price 조회
    """
    coin = normalize_symbol(symbol)
    rows = await get_all_funding_rates()
    if rows is None:
        return None

    for row in rows:
        if row["symbol"] == coin:
            return row
    return None


async def get_all_funding_rates() -> list[dict] | None:
    """
    전종목 funding / mark price 조회
    """
    payload = {"type": "metaAndAssetCtxs"}

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                print("[get_all_funding_rates] error:", data)
                return None

    if not isinstance(data, list) or len(data) != 2:
        return None

    meta, ctxs = data
    universe = meta.get("universe", [])

    out = []
    for i, entry in enumerate(universe):
        if i >= len(ctxs):
            continue

        coin = normalize_symbol(entry.get("name", ""))
        ctx = ctxs[i] or {}

        out.append({
            "exchange": "hyperliquid",
            "symbol": coin,
            "asset": i,
            "funding_rate": float(ctx.get("funding", 0) or 0),
            "next_funding_time": None,
            "mark_price": float(ctx.get("markPx", 0) or 0) if ctx.get("markPx") is not None else None,
            "index_price": float(ctx.get("oraclePx", 0) or 0) if ctx.get("oraclePx") is not None else None,
            "sz_decimals": int(entry.get("szDecimals", 0) or 0),
            "timestamp": int(time.time() * 1000),
            "raw": {"meta_entry": entry, "ctx": ctx},
        })

    return out


async def get_funding_rate_history(
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100
) -> list[dict] | None:
    """
    과거 funding history 조회
    """
    coin = normalize_symbol(symbol)

    if end_time is None:
        end_time = int(time.time() * 1000)
    if start_time is None:
        start_time = end_time - 7 * 24 * 60 * 60 * 1000

    payload = {
        "type": "fundingHistory",
        "coin": coin,
        "startTime": start_time,
        "endTime": end_time,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                return None

    trimmed = data[-limit:] if limit and len(data) > limit else data

    out = []
    for row in trimmed:
        out.append({
            "exchange": "hyperliquid",
            "symbol": coin,
            "funding_time": int(row.get("time", 0)),
            "funding_rate": float(row.get("fundingRate", 0) or 0),
            "mark_price": float(row.get("px", 0) or 0) if row.get("px") is not None else None,
            "raw": row,
        })
    return out


async def get_ohlcv_1m(symbol: str, limit: int = 500):
    """
    1분봉 조회
    """
    coin = normalize_symbol(symbol)
    end_time = int(time.time() * 1000)
    start_time = end_time - limit * 60 * 1000

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": "1m",
            "startTime": start_time,
            "endTime": end_time,
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                return None

    out = []
    for row in data:
        out.append({
            "exchange": "hyperliquid",
            "symbol": coin,
            "open_time": int(row.get("t", 0)),
            "open": float(row.get("o", 0) or 0),
            "high": float(row.get("h", 0) or 0),
            "low": float(row.get("l", 0) or 0),
            "close": float(row.get("c", 0) or 0),
            "volume": float(row.get("v", 0) or 0),
            "close_time": int(row.get("T", row.get("t", 0))),
            "raw": row,
        })
    return out


# =========================================================
# exchange endpoint wrappers (via official SDK)
# =========================================================

async def post_limit_order(symbol: str, side: str, amount: float, price: float, option: dict = {}) -> dict:
    """
    지정가 주문
    option 예시:
      {
        "reduce_only": False,
        "client_order_id": None,
        "time_in_force": "Gtc",  # "Alo", "Ioc", "Gtc"
      }
    """
    coin = normalize_symbol(symbol)
    is_buy = side.strip().lower() == "buy"
    reduce_only = bool(option.get("reduce_only", False))
    client_order_id = option.get("client_order_id")
    tif = option.get("time_in_force", "Gtc")

    exchange = get_exchange_client()
    order_type = {"limit": {"tif": tif}}

    def _run():
        return exchange.order(
            coin,
            is_buy,
            float(amount),
            float(price),
            order_type,
            reduce_only=reduce_only,
            cloid=client_order_id,
        )

    data = await asyncio.to_thread(_run)

    if data.get("status") == "ok":
        statuses = data.get("response", {}).get("data", {}).get("statuses", [])
        status0 = statuses[0] if statuses else {}

        oid = None
        hl_status = "UNKNOWN"

        if "resting" in status0:
            oid = status0["resting"].get("oid")
            hl_status = "OPEN"
        elif "filled" in status0:
            oid = status0["filled"].get("oid")
            hl_status = "FILLED"

        return {
            "ticker": coin,
            "uuid": str(oid) if oid is not None else str(client_order_id or f"{coin}-{time.time()}"),
            "order_id": str(oid) if oid is not None else None,
            "client_order_id": client_order_id,
            "side": "buy" if is_buy else "sell",
            "position_side": "BOTH",
            "price": float(price),
            "qty": float(amount),
            "executed_qty": float(amount) if hl_status == "FILLED" else 0.0,
            "avg_price": float(price) if hl_status == "FILLED" else 0.0,
            "status": hl_status,
            "reduce_only": reduce_only,
            "timestamp": int(time.time() * 1000),
            "identifier": f"{coin}_{amount}_{price}",
            "raw": data,
        }

    return {
        "error": "order_failed",
        "raw": data,
    }


async def delete_order(symbol: str, client_order_id: str) -> dict:
    """
    단일 주문 취소
    여기서는 기존 구조 호환 위해 uuid에 oid를 넣었다고 가정
    """
    coin = normalize_symbol(symbol)
    exchange = get_exchange_client()
    oid = int(client_order_id)

    def _run():
        return exchange.cancel(coin, oid)

    data = await asyncio.to_thread(_run)

    if data.get("status") == "ok":
        return {
            "ok": True,
            "ticker": coin,
            "uuid": str(oid),
            "order_id": str(oid),
            "client_order_id": None,
            "status": "CANCELED",
            "timestamp": int(time.time() * 1000),
            "raw": data,
        }

    return {
        "error": "not_exist" if "error" in str(data).lower() else "cancel_failed",
        "raw": data,
    }


async def get_order(symbol: str, client_order_id: str) -> dict:
    """
    단일 주문 조회
    Hyperliquid는 open_orders 전체 조회로 대체 구현
    """
    coin = normalize_symbol(symbol)
    open_orders = await get_open_orders(coin)
    if open_orders is None:
        return {"error": "open_orders_failed"}

    for order in open_orders:
        if order["uuid"] == str(client_order_id) or order.get("client_order_id") == client_order_id:
            return {
                "state": "exist",
                **order,
            }

    return {
        "state": "not_exist",
        "error": "not_exist",
    }


# =========================================================
# optional helpers
# =========================================================

async def get_user_fills() -> list[dict] | None:
    """
    최근 유저 fill 조회
    """
    if not HYPERLIQUID_ACCESS_KEY:
        return None

    payload = {
        "type": "userFills",
        "user": HYPERLIQUID_ACCESS_KEY,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(INFO_URL, json=payload) as response:
            data = await response.json()
            if response.status != 200:
                return None
            return data