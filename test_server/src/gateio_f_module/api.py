import os
import time
import json
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

import aiohttp
from dotenv import load_dotenv

load_dotenv()

GATE_KEY = os.getenv("GATEIO_ACCESS_KEY")
GATE_SECRET = os.getenv("GATEIO_SECRET_KEY")

BASE_URL = "https://fx-api.gateio.ws"
API_PREFIX = "/api/v4"
SETTLE = os.getenv("GATEIO_SETTLE", "usdt").lower()
COMMON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
_TICKER_LIST_CACHE: tuple[int, tuple[list[str], dict[str, dict[str, Any]]]] | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ticker_list_cache_ttl_sec() -> int:
    return int(float(os.getenv("GATEIO_TICKER_LIST_CACHE_SEC", "21600")))


def _clone_ticker_list_cache(payload: tuple[list[str], dict[str, dict[str, Any]]]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    ticker_list, info = payload
    return list(ticker_list), {symbol: dict(row) for symbol, row in info.items()}


def _hash_payload(payload_string: str = "") -> str:
    m = hashlib.sha512()
    m.update(payload_string.encode("utf-8"))
    return m.hexdigest()


def gen_sign(method: str, url: str, query_string: str = "", payload_string: str = "") -> dict[str, str]:
    """
    Gate API v4 signature
    sign_string = METHOD + "\n" + URL + "\n" + QUERY + "\n" + SHA512(BODY) + "\n" + TIMESTAMP
    - url 은 /api/v4/... 전체 path 여야 함
    - timestamp 는 unix seconds
    """
    timestamp = str(int(time.time()))
    sign_string = f"{method.upper()}\n{url}\n{query_string}\n{_hash_payload(payload_string)}\n{timestamp}"
    sign = hmac.new(
        GATE_SECRET.encode("utf-8"),
        sign_string.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return {"KEY": GATE_KEY, "Timestamp": timestamp, "SIGN": sign}


async def _request(
    method: str,
    path: str,
    *,
    query_string: str = "",
    payload: dict[str, Any] | list[Any] | None = None,
    auth: bool = False,
    timeout: int = 10,
) -> tuple[int, Any]:
    url = f"{BASE_URL}{API_PREFIX}{path}"
    if query_string:
        url = f"{url}?{query_string}"

    payload_string = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
    headers = dict(COMMON_HEADERS)

    if auth:
        signed_path = f"{API_PREFIX}{path}"
        headers.update(gen_sign(method, signed_path, query_string, payload_string))

    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        async with session.request(method.upper(), url, headers=headers, data=payload_string or None) as response:
            try:
                data = await response.json(content_type=None)
            except Exception:
                data = await response.text()
            return response.status, data


def floor_by_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    val = Decimal(str(value))
    step_dec = Decimal(str(step))
    return float((val / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec)


def ceil_by_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    val = Decimal(str(value))
    step_dec = Decimal(str(step))
    return float((val / step_dec).to_integral_value(rounding=ROUND_UP) * step_dec)


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "_").replace("-", "_")
    if symbol.endswith("_USDT"):
        return symbol
    if symbol.endswith("USDT"):
        base = symbol[:-4].rstrip("_")
        return f"{base}_USDT"
    return f"{symbol}_USDT"


def symbol_to_code(contract: str) -> str:
    return contract.replace("_USDT", "")


def contract_size_to_token_qty(size: int | float, cont_size: float) -> float:
    return float(size) * float(cont_size)


def token_qty_to_contract_size(token_qty: float, cont_size: float, side: str, min_size: float = 1) -> int:
    """
    Gate 선물 주문 size 는 계약 개수다.
    token_qty 를 contract size 로 변환한 뒤 정수 계약수로 내림한다.
    sell 이면 음수 size 사용.
    """
    if cont_size <= 0:
        return 0
    contracts = int(abs(token_qty) / cont_size)
    if contracts < int(min_size):
        return 0
    return -contracts if side.lower() == "sell" else contracts


def normalize_order_price(price: float, tick_size: float, side: str) -> float:
    return floor_by_step(price, tick_size) if side.upper() == "BUY" else ceil_by_step(price, tick_size)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _parse_contract_info(row: dict[str, Any]) -> dict[str, Any]:
    cont_size = _safe_float(row.get("quanto_multiplier"))
    min_q = _safe_float(row.get("order_size_min"), 1.0)
    max_q = _safe_float(row.get("order_size_max"))
    return {
        "symbol": symbol_to_code(row["name"]),
        "contract": row["name"],
        "tick_size": _safe_float(row.get("order_price_round")),
        "mark_price_round": _safe_float(row.get("mark_price_round")),
        "cont_size": cont_size,
        "min_q": min_q,
        "max_q": max_q,
        "step_size": cont_size,
        "min_qty": min_q * cont_size,
        "max_qty": max_q * cont_size if max_q > 0 else 0.0,
        "leverage_min": _safe_float(row.get("leverage_min")),
        "leverage_max": _safe_float(row.get("leverage_max")),
        "mark_price": _safe_float(row.get("mark_price")),
        "index_price": _safe_float(row.get("index_price")),
        "last_price": _safe_float(row.get("last_price")),
        "funding_rate": _safe_float(row.get("funding_rate")),
        "funding_rate_indicative": _safe_float(row.get("funding_rate_indicative")),
        "funding_next_apply": int(row.get("funding_next_apply", 0) or 0) * 1000,
        "raw": row,
    }


def process_account_data(positions: list[dict[str, Any]], info: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    account_dict: dict[str, dict[str, Any]] = {}
    timestamp = _now_ms()

    for pos in positions:
        contract = pos.get("contract")
        if not contract:
            continue

        symbol = symbol_to_code(contract)
        if symbol not in info:
            continue

        cont_size = float(info[symbol]["cont_size"])
        qty_token = contract_size_to_token_qty(pos.get("size", 0), cont_size)

        account_dict[symbol] = {
            "balance": qty_token,
            "nowPrice": _safe_float(pos.get("mark_price")),
            "markPrice": _safe_float(pos.get("mark_price")),
            "locked": 0.0,
            "unrealized_profit": _safe_float(pos.get("unrealised_pnl")),
            "avg_price": _safe_float(pos.get("entry_price")),
            "liquidation_price": _safe_float(pos.get("liq_price")),
            "leverage": _safe_float(pos.get("leverage")),
            "margin": _safe_float(pos.get("margin")),
            "value": _safe_float(pos.get("value")),
            "timestamp": timestamp,
            "raw": pos,
        }
    return account_dict


def _parse_asset_data(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    timestamp = _now_ms()
    available = _safe_float(data.get("available"))
    total = _safe_float(data.get("total"), available)
    unreal = _safe_float(data.get("unrealised_pnl"))
    order_margin = _safe_float(data.get("order_margin"))
    position_margin = _safe_float(data.get("position_margin"))

    return {
        SETTLE.upper(): {
            "balance": total,
            "available": available,
            "locked": max(total - available, 0.0),
            "order_margin": order_margin,
            "position_margin": position_margin,
            "unrealized_profit": unreal,
            "currency": data.get("currency", SETTLE.upper()),
            "timestamp": timestamp,
            "raw": data,
        }
    }


def _normalize_order(order: dict[str, Any], info: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    contract = order.get("contract", "")
    symbol = symbol_to_code(contract) if contract else order.get("symbol", "")
    cont_size = 1.0
    if info and symbol in info:
        cont_size = float(info[symbol].get("cont_size", 1.0))

    size_contract = int(order.get("size", 0) or 0)
    qty_token = contract_size_to_token_qty(size_contract, cont_size)
    fill_price = _safe_float(order.get("fill_price") or order.get("price"))

    return {
        "exchange": "gateioFuture",
        "ticker": symbol,
        "symbol": symbol,
        "contract": contract,
        "uuid": str(order.get("id")),
        "order_id": str(order.get("id")),
        "client_order_id": order.get("text"),
        "side": "buy" if size_contract > 0 else "sell",
        "position_side": "BOTH",
        "price": _safe_float(order.get("price")),
        "qty": qty_token,
        "qty_contract": size_contract,
        "executed_qty": contract_size_to_token_qty(abs(int(order.get("left", 0) or 0) - size_contract), cont_size)
        if order.get("left") is not None else None,
        "left_qty": contract_size_to_token_qty(order.get("left", 0), cont_size) if order.get("left") is not None else None,
        "avg_price": fill_price,
        "status": str(order.get("status", "UNKNOWN")).upper(),
        "reduce_only": bool(order.get("is_reduce_only", False)),
        "timestamp": int(float(order.get("finish_time") or order.get("create_time") or time.time()) * 1000),
        "identifier": f"{symbol}_{size_contract}_{order.get('price')}",
        "raw": order,
    }


async def get_server_time() -> int:
    # Gate futures REST 문서에 전용 time endpoint가 명확히 드러나지 않아,
    # 현재 구현은 로컬 시간을 반환하는 no-op placeholder 로 둔다.
    # drift 검사는 websocket server_time(t) 기준으로 하는 편이 더 정확하다.
    return _now_ms()


async def sync_server_time() -> int:
    return 0


async def get_ticker_list() -> tuple[list[str], dict[str, dict[str, Any]]] | None:
    global _TICKER_LIST_CACHE

    ttl_sec = _ticker_list_cache_ttl_sec()
    now_ms = _now_ms()
    if _TICKER_LIST_CACHE is not None:
        cached_at_ms, cached_payload = _TICKER_LIST_CACHE
        if ttl_sec > 0 and (now_ms - cached_at_ms) < ttl_sec * 1000:
            return _clone_ticker_list_cache(cached_payload)

    path = f"/futures/{SETTLE}/contracts"
    status, data = await _request("GET", path)
    if status != 200 or not isinstance(data, list):
        if _TICKER_LIST_CACHE is not None:
            print("[gateio][get_ticker_list][warn] using stale cache after fetch failure:", data)
            return _clone_ticker_list_cache(_TICKER_LIST_CACHE[1])
        print("[get_ticker_list] error:", data)
        return None

    ticker_list: list[str] = []
    ticker_info: dict[str, dict[str, Any]] = {}
    for row in data:
        info = _parse_contract_info(row)
        symbol = info["symbol"]
        ticker_list.append(symbol)
        ticker_info[symbol] = info
    payload = (ticker_list, ticker_info)
    _TICKER_LIST_CACHE = (now_ms, payload)
    return _clone_ticker_list_cache(payload)


async def get_market_prefill(symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    path = f"/futures/{SETTLE}/tickers"
    status, data = await _request("GET", path)
    if status != 200 or not isinstance(data, list):
        return {}

    allowed_contracts = {normalize_symbol(symbol) for symbol in (symbols or [])}
    desktop_time = _now_ms()
    result: dict[str, dict[str, Any]] = {}

    for row in data:
        contract = str(row.get("contract", "")).upper()
        if not contract.endswith("_USDT"):
            continue
        if allowed_contracts and contract not in allowed_contracts:
            continue

        symbol = symbol_to_code(contract)
        best_bid = _safe_float(row.get("highest_bid"), None)
        best_ask = _safe_float(row.get("lowest_ask"), None)
        if best_bid in (None, 0.0) or best_ask in (None, 0.0):
            continue

        cont_size = _safe_float(row.get("quanto_multiplier"), 1.0)
        bid_size_contract = _safe_float(row.get("highest_size"), None)
        ask_size_contract = _safe_float(row.get("lowest_size"), None)

        result[symbol] = {
            "ticker": symbol,
            "desktop_time": desktop_time,
            "server_time": desktop_time,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid": best_bid,
            "ask": best_ask,
            "mid": (best_bid + best_ask) / 2.0,
            "spread": best_ask - best_bid,
            "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
            "bid_qty_top": (bid_size_contract * cont_size) if bid_size_contract is not None else None,
            "ask_qty_top": (ask_size_contract * cont_size) if ask_size_contract is not None else None,
            "depth_levels": 1,
            "prefill_source": "rest",
            "raw": row,
        }

    return result


async def get_contract_detail(symbol: str) -> dict[str, Any] | None:
    contract = normalize_symbol(symbol)
    path = f"/futures/{SETTLE}/contracts/{contract}"
    status, data = await _request("GET", path)
    if status != 200 or not isinstance(data, dict):
        print("[get_contract_detail] error:", data)
        return None
    return _parse_contract_info(data)


async def get_account(option: dict = {}) -> dict[str, dict[str, Any]] | None:
    info = option.get("info", {})
    path = f"/futures/{SETTLE}/positions"
    status, data = await _request("GET", path, auth=True)
    if status != 200 or not isinstance(data, list):
        print("[get_account] error:", data)
        return None
    return process_account_data(data, info)


async def get_balance(option: dict = {}) -> float | None:
    path = f"/futures/{SETTLE}/accounts"
    status, data = await _request("GET", path, auth=True)
    if status != 200 or not isinstance(data, dict):
        print("[get_balance] error:", data)
        return None
    return _safe_float(data.get("available"))


async def get_accounts(option: dict = {}) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    info = option.get("info", {})
    positions = await get_account({"info": info})

    path = f"/futures/{SETTLE}/accounts"
    status, data = await _request("GET", path, auth=True)
    if status != 200 or not isinstance(data, dict):
        print("[get_accounts] asset error:", data)
        return positions, None

    return positions, _parse_asset_data(data)


async def post_limit_order(contract: str, side: str, amount: float, price: float, option: dict = {}) -> dict[str, Any] | None:
    info = option.get("info", {})
    symbol = contract.upper()
    if symbol not in info:
        detail = await get_contract_detail(symbol)
        if detail is None:
            return None
        info = {**info, symbol: detail}

    meta = info[symbol]
    parsed_price = normalize_order_price(float(price), float(meta["tick_size"]), side)
    parsed_amount = token_qty_to_contract_size(
        token_qty=float(amount),
        cont_size=float(meta["cont_size"]),
        side=side,
        min_size=float(meta.get("min_q", 1)),
    )

    if parsed_amount == 0:
        print("[post_limit_order] amount too small after contract conversion")
        return None

    path = f"/futures/{SETTLE}/orders"
    payload = {
        "contract": normalize_symbol(symbol),
        "size": parsed_amount,
        "price": str(parsed_price),
        "tif": "gtc",
        "text": option.get("client_order_id") or f"t-limit-{symbol}-{_now_ms()}",
    }
    status, data = await _request("POST", path, payload=payload, auth=True)
    if status not in (200, 201) or not isinstance(data, dict) or data.get("message"):
        print("[post_limit_order] error:", data)
        return None
    return _normalize_order(data, info)


async def post_market_order(contract: str, side: str, amount: float, option: dict = {}) -> dict[str, Any] | None:
    info = option.get("info", {})
    symbol = contract.upper()
    if symbol not in info:
        detail = await get_contract_detail(symbol)
        if detail is None:
            return None
        info = {**info, symbol: detail}

    meta = info[symbol]
    parsed_amount = token_qty_to_contract_size(
        token_qty=float(amount),
        cont_size=float(meta["cont_size"]),
        side=side,
        min_size=float(meta.get("min_q", 1)),
    )

    if parsed_amount == 0:
        print("[post_market_order] amount too small after contract conversion")
        return None

    path = f"/futures/{SETTLE}/orders"
    payload = {
        "contract": normalize_symbol(symbol),
        "size": parsed_amount,
        "price": "0",
        "tif": "ioc",
        "text": option.get("client_order_id") or f"t-market-{symbol}-{_now_ms()}",
    }
    status, data = await _request("POST", path, payload=payload, auth=True)
    if status not in (200, 201) or not isinstance(data, dict) or data.get("message"):
        print("[post_market_order] error:", data)
        return None
    return _normalize_order(data, info)


async def get_order(*args, option: dict = {}) -> dict[str, Any] | None:
    """
    호환성용:
    - get_order(order_id)
    - get_order(symbol, order_id)
    """
    if len(args) == 1:
        order_id = str(args[0])
    elif len(args) >= 2:
        order_id = str(args[1])
    else:
        raise TypeError("get_order requires order_id or (symbol, order_id)")

    info = option.get("info", {})
    path = f"/futures/{SETTLE}/orders/{order_id}"
    status, data = await _request("GET", path, auth=True)

    if status == 404 or (isinstance(data, dict) and data.get("label") == "ORDER_NOT_FOUND"):
        return {"state": "not_exist"}

    if status != 200 or not isinstance(data, dict):
        print("[get_order] error:", data)
        return None

    normalized = _normalize_order(data, info)
    if normalized["status"] == "OPEN":
        return {"state": "exist", **normalized}
    return {"state": "not_exist", **normalized}


async def get_open_orders(symbol: str | None = None, option: dict = {}) -> list[dict[str, Any]] | None:
    info = option.get("info", {})
    path = f"/futures/{SETTLE}/orders"

    params = ["status=open"]
    if symbol:
        params.insert(0, f"contract={normalize_symbol(symbol)}")
    query_string = "&".join(params)

    status, data = await _request("GET", path, query_string=query_string, auth=True)
    if status != 200 or not isinstance(data, list):
        print("[get_open_orders] error:", data)
        return None
    return [_normalize_order(order, info) for order in data]


async def delete_order(order_id: str, option: dict = {}) -> dict[str, Any] | None:
    info = option.get("info", {})
    path = f"/futures/{SETTLE}/orders/{order_id}"
    status, data = await _request("DELETE", path, auth=True)

    if status == 404 or (isinstance(data, dict) and data.get("label") == "ORDER_NOT_FOUND"):
        return {"state": "not_exist"}

    if status != 200 or not isinstance(data, dict):
        print("[delete_order] error:", data)
        return None

    normalized = _normalize_order(data, info)
    if normalized["status"] == "OPEN":
        return {"state": "exist", **normalized}
    return {"state": "not_exist", **normalized}


async def cancel_all_orders(symbol: str | None = None) -> dict[str, Any]:
    path = f"/futures/{SETTLE}/orders"
    query = f"contract={normalize_symbol(symbol)}" if symbol else ""
    status, data = await _request("DELETE", path, query_string=query, auth=True)

    if status == 200:
        return {"ok": True, "symbol": symbol.upper() if symbol else None, "raw": data}
    return {"ok": False, "symbol": symbol.upper() if symbol else None, "raw": data}


async def get_funding_rate(symbol: str) -> dict[str, Any] | None:
    detail = await get_contract_detail(symbol)
    if detail is None:
        return None

    # Gate 문서상 contracts/{contract} 응답에 funding_rate, funding_next_apply,
    # 그리고 tickers 응답에 funding_rate_indicative 가 포함된다.
    return {
        "exchange": "gateioFuture",
        "symbol": detail["symbol"],
        "funding_rate": detail["funding_rate"],
        "funding_rate_indicative": detail["funding_rate_indicative"],
        "next_funding_time": detail["funding_next_apply"],
        "mark_price": detail["mark_price"],
        "index_price": detail["index_price"],
        "timestamp": _now_ms(),
        "raw": detail["raw"],
    }


async def get_funding_rate_history(
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]] | None:
    path = f"/futures/{SETTLE}/funding_rate"
    params = [f"contract={normalize_symbol(symbol)}", f"limit={limit}"]

    # Gate docs는 초 단위 timestamp 를 쓰는 경우가 많다. ms 가 오면 sec 로 변환.
    if start_time is not None:
        params.append(f"from={int(start_time / 1000) if start_time > 10**10 else int(start_time)}")
    if end_time is not None:
        params.append(f"to={int(end_time / 1000) if end_time > 10**10 else int(end_time)}")

    status, data = await _request("GET", path, query_string="&".join(params))
    if status != 200 or not isinstance(data, list):
        print("[get_funding_rate_history] error:", data)
        return None

    rows = []
    for row in data:
        rows.append(
            {
                "exchange": "gateioFuture",
                "symbol": symbol.upper(),
                "funding_time": int(float(row.get("t") or row.get("time") or 0)) * 1000,
                "funding_rate": _safe_float(row.get("r") if row.get("r") is not None else row.get("funding_rate")),
                "raw": row,
            }
        )
    return rows


async def get_ohlcv_1m(symbol: str, limit: int = 500) -> list[dict[str, Any]] | None:
    path = f"/futures/{SETTLE}/candlesticks"
    query = f"contract={normalize_symbol(symbol)}&interval=1m&limit={limit}"
    status, data = await _request("GET", path, query_string=query)
    if status != 200 or not isinstance(data, list):
        print("[get_ohlcv_1m] error:", data)
        return None

    rows = []
    for row in data:
        ts_sec = int(float(row.get("t") or row.get("timestamp") or 0))
        rows.append(
            {
                "exchange": "gateioFuture",
                "symbol": symbol.upper(),
                "open_time": ts_sec * 1000,
                "open": _safe_float(row.get("o") if row.get("o") is not None else row.get("open")),
                "high": _safe_float(row.get("h") if row.get("h") is not None else row.get("high")),
                "low": _safe_float(row.get("l") if row.get("l") is not None else row.get("low")),
                "close": _safe_float(row.get("c") if row.get("c") is not None else row.get("close")),
                "volume": _safe_float(row.get("v") if row.get("v") is not None else row.get("volume")),
                "close_time": (ts_sec + 60) * 1000 - 1,
                "raw": row,
            }
        )
    return rows


async def set_leverage(symbol: str, leverage: int | float) -> dict[str, Any]:
    contract = normalize_symbol(symbol)
    path = f"/futures/{SETTLE}/positions/{contract}/leverage"
    query = f"leverage={leverage}"
    status, data = await _request("POST", path, query_string=query, auth=True)

    if status == 200 and isinstance(data, dict):
        return {
            "ok": True,
            "symbol": symbol.upper(),
            "leverage": _safe_float(data.get("leverage"), float(leverage)),
            "raw": data,
        }
    return {"ok": False, "symbol": symbol.upper(), "leverage": leverage, "raw": data}


async def set_margin_mode(symbol: str, mode: str) -> dict[str, Any]:
    """
    Gate futures 문서에는 Binance 처럼 별도 marginType endpoint 가 보이지 않는다.
    Position schema 상 leverage=0 이 cross margin 을 의미한다.
    그래서 CROSS 는 leverage=0 으로 시도하고,
    ISOLATED 는 별도 leverage 값 지정이 필요하므로 no-op 실패로 돌려준다.
    """
    normalized_mode = mode.upper()
    if normalized_mode in {"CROSS", "CROSSED"}:
        result = await set_leverage(symbol, 0)
        return {"ok": result.get("ok", False), "symbol": symbol.upper(), "margin_mode": "CROSS", "raw": result.get("raw")}

    return {
        "ok": False,
        "symbol": symbol.upper(),
        "margin_mode": normalized_mode,
        "error": "Gate futures has no separate margin mode endpoint in the referenced docs; isolated mode should be handled via positive leverage on an existing position.",
    }


async def get_my_trades(symbol: str | None = None, limit: int = 100, option: dict[str, Any] | None = None) -> list[dict[str, Any]] | None:
    option = option or {}
    info = dict(option.get("info") or {})
    path = f"/futures/{SETTLE}/my_trades"
    params = [f"limit={limit}"]
    if symbol:
        params.insert(0, f"contract={normalize_symbol(symbol)}" )
    status, data = await _request("GET", path, query_string="&".join(params), auth=True)

    if status != 200 or not isinstance(data, list):
        print("[get_my_trades] error:", data)
        return None

    rows = []
    missing_symbols: set[str] = set()
    for row in data:
        contract = row.get("contract", "")
        symbol_code = symbol_to_code(contract) if contract else (symbol.upper() if symbol else None)
        if symbol_code and symbol_code not in info:
            missing_symbols.add(symbol_code)

    for missing_symbol in sorted(missing_symbols):
        detail = await get_contract_detail(missing_symbol)
        if detail is not None:
            info[missing_symbol] = detail

    for row in data:
        contract = row.get("contract", "")
        symbol_code = symbol_to_code(contract) if contract else (symbol.upper() if symbol else None)
        meta = info.get(symbol_code or "", {})
        cont_size = float(meta.get("cont_size", 1.0))
        signed_size_contract = int(row.get("size", 0) or 0)
        qty_contract = abs(signed_size_contract)
        qty = contract_size_to_token_qty(qty_contract, cont_size)
        price = _safe_float(row.get("price"))
        timestamp = int(float(row.get("create_time") or row.get("time") or 0) * 1000)
        rows.append(
            {
                "exchange": "gateioFuture",
                "symbol": symbol_code,
                "ticker": symbol_code,
                "trade_id": row.get("id") or row.get("trade_id"),
                "order_id": row.get("order_id"),
                "side": "buy" if signed_size_contract > 0 else "sell" if signed_size_contract < 0 else None,
                "price": price,
                "qty": qty,
                "qty_contract": qty_contract,
                "quote_qty": price * qty,
                "fee": _safe_float(row.get("fee")),
                "fee_ccy": str(row.get("fee_currency") or SETTLE).upper(),
                "role": row.get("role"),
                "timestamp": timestamp,
                "create_time": timestamp,
                "raw": row,
            }
        )
    return rows
