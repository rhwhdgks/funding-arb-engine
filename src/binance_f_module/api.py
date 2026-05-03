import os
import time
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any
from urllib.parse import urlencode

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")

_TIME_OFFSET_MS = 0
_TICKER_LIST_CACHE: tuple[int, tuple[list[str], dict[str, dict[str, Any]]]] | None = None


def _ticker_list_cache_ttl_sec() -> int:
    return int(float(os.getenv("BINANCE_TICKER_LIST_CACHE_SEC", "21600")))


def _clone_ticker_list_cache(payload: tuple[list[str], dict[str, dict[str, Any]]]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    ticker_list, info = payload
    return list(ticker_list), {symbol: dict(row) for symbol, row in info.items()}


# ---------- numeric helpers ----------
def _header_int(headers: dict[str, str], key: str) -> int | None:
    value = headers.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _build_rate_limits(headers: dict[str, str]) -> list[dict[str, Any]]:
    """
    exchange.py의 _update_rate_limit_state()가 기대하는 형식으로 변환.
    limit 값은 운영 기본값이며, 필요하면 env로 덮어써.
    """
    request_weight_limit_1m = int(os.getenv("BINANCE_REQUEST_WEIGHT_LIMIT_1M", "2400"))
    order_limit_10s = int(os.getenv("BINANCE_ORDER_LIMIT_10S", "300"))
    order_limit_1m = int(os.getenv("BINANCE_ORDER_LIMIT_1M", "1200"))

    result: list[dict[str, Any]] = []

    used_weight_1m = _header_int(headers, "x-mbx-used-weight-1m")
    if used_weight_1m is not None and used_weight_1m >= 0:
        result.append(
            {
                "type": "REQUEST_WEIGHT",
                "count": used_weight_1m,
                "limit": request_weight_limit_1m,
                "interval": "MINUTE",
                "intervalNum": 1,
            }
        )

    order_count_10s = _header_int(headers, "x-mbx-order-count-10s")
    if order_count_10s is not None:
        result.append(
            {
                "type": "ORDERS",
                "count": order_count_10s,
                "limit": order_limit_10s,
                "interval": "SECOND",
                "intervalNum": 10,
            }
        )

    order_count_1m = _header_int(headers, "x-mbx-order-count-1m")
    if order_count_1m is not None:
        result.append(
            {
                "type": "ORDERS",
                "count": order_count_1m,
                "limit": order_limit_1m,
                "interval": "MINUTE",
                "intervalNum": 1,
            }
        )

    return result
def _now_ms() -> int:
    return int(time.time() * 1000)


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


def normalize_order_price(price: float, tick_size: float, side: str) -> float:
    return floor_by_step(price, tick_size) if side.upper() == "BUY" else ceil_by_step(price, tick_size)


def normalize_order_qty(qty: float, step_size: float, min_qty: float, max_qty: float | None = None) -> float:
    normalized = floor_by_step(qty, step_size)
    if normalized < min_qty:
        return 0.0
    if max_qty is not None:
        normalized = min(normalized, max_qty)
    return normalized
def _safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


# ---------- signing / request helpers ----------
def create_signature(query_string: str, secret_key: str | None = None) -> str:
    secret = secret_key or BINANCE_SECRET_KEY or ""
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()


def _ts() -> int:
    return _now_ms() + _TIME_OFFSET_MS


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "").replace("-", "")
    if symbol.endswith("USDT"):
        return symbol
    return symbol + "USDT"


def symbol_to_code(symbol: str) -> str:
    return symbol.replace("USDT", "")


async def _request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    signed: bool = False,
    timeout: int = 10,
) -> tuple[int, Any, dict[str, str]]:
    params = dict(params or {})

    headers = {"Content-Type": "application/json"}
    if BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY

    if signed:
        params["timestamp"] = params.get("timestamp", _ts())
        query_string = urlencode(params, doseq=True)
        signature = create_signature(query_string)
        url = f"{BASE_URL}{path}?{query_string}&signature={signature}"
    else:
        query_string = urlencode(params, doseq=True)
        url = f"{BASE_URL}{path}"
        if query_string:
            url = f"{url}?{query_string}"

    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        async with session.request(method.upper(), url, headers=headers) as response:
            try:
                data = await response.json(content_type=None)
            except Exception:
                data = await response.text()
            return response.status, data, dict(response.headers)


# ---------- time sync ----------
async def get_server_time() -> int | None:
    status, data, _ = await _request("GET", "/fapi/v1/time")
    if status == 200:
        return int(data["serverTime"])
    return None


async def sync_server_time() -> int | None:
    global _TIME_OFFSET_MS
    local_before = _now_ms()
    server_time = await get_server_time()
    local_after = _now_ms()
    if server_time is None:
        return None
    local_mid = (local_before + local_after) // 2
    _TIME_OFFSET_MS = server_time - local_mid
    return _TIME_OFFSET_MS


# ---------- parsing helpers ----------
def process_account_data(data: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    account_dict: dict[str, dict[str, Any]] = {}
    asset_dict: dict[str, dict[str, Any]] = {}
    timestamp = _now_ms()

    for asset in data.get("assets", []):
        asset_code = asset["asset"]
        asset_dict[asset_code] = {
            "balance": _safe_float(asset.get("walletBalance")),
            "available": _safe_float(asset.get("availableBalance")),
            "availableBalance": _safe_float(asset.get("availableBalance")),
            "marginBalance": _safe_float(asset.get("marginBalance")),
            "locked": max(_safe_float(asset.get("walletBalance")) - _safe_float(asset.get("availableBalance")), 0.0),
            "unrealized_profit": _safe_float(asset.get("unrealizedProfit")),
            "currency": asset_code,
            "timestamp": timestamp,
            "raw": asset,
        }

    for position in data.get("positions", []):
        symbol = position.get("symbol", "")
        if not symbol.endswith("USDT") or "_" in symbol:
            continue
        code = symbol_to_code(symbol)
        account_dict[code] = {
            "balance": _safe_float(position.get("positionAmt")),
            "nowPrice": _safe_float(position.get("markPrice")),
            "markPrice": _safe_float(position.get("markPrice")),
            "locked": 0.0,
            "unrealized_profit": _safe_float(position.get("unrealizedProfit")),
            "avg_price": _safe_float(position.get("entryPrice")),
            "liquidation_price": _safe_float(position.get("liquidationPrice")),
            "leverage": _safe_float(position.get("leverage")),
            "margin_type": position.get("marginType"),
            "positionSide": position.get("positionSide", "BOTH"),
            "timestamp": timestamp,
            "raw": position,
        }

    return account_dict, asset_dict


def _parse_symbol_info(symbol_row: dict[str, Any]) -> dict[str, Any]:
    price_filter = None
    lot_size_filter = None
    min_notional_filter = None

    for f in symbol_row.get("filters", []):
        if f.get("filterType") == "PRICE_FILTER":
            price_filter = f
        elif f.get("filterType") == "LOT_SIZE":
            lot_size_filter = f
        elif f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL"):
            min_notional_filter = f

    if price_filter is None or lot_size_filter is None:
        raise ValueError(f"missing filter for symbol={symbol_row.get('symbol')}")

    min_qty = _safe_float(lot_size_filter.get("minQty"))
    max_qty = _safe_float(lot_size_filter.get("maxQty"))

    return {
        "symbol": symbol_to_code(symbol_row["symbol"]),
        "contract": symbol_row["symbol"],
        "min_q": min_qty,
        "max_q": max_qty,
        "min_qty": min_qty,
        "max_qty": max_qty,
        "step_size": _safe_float(lot_size_filter.get("stepSize")),
        "tick_size": _safe_float(price_filter.get("tickSize")),
        "price_precision": int(symbol_row.get("pricePrecision", 8) or 8),
        "quantity_precision": int(symbol_row.get("quantityPrecision", 8) or 8),
        "min_notional": _safe_float((min_notional_filter or {}).get("notional", 0.0)),
        "cont_size": 1.0,
        "raw": symbol_row,
    }


def _normalize_order(order: dict[str, Any]) -> dict[str, Any]:
    symbol = order.get("symbol", "")
    status = str(order.get("status", "UNKNOWN")).upper()
    state = "exist" if status in {"NEW", "PARTIALLY_FILLED"} else "not_exist"
    qty = _safe_float(order.get("origQty"))
    executed_qty = _safe_float(order.get("executedQty"))

    return {
        "exchange": "binanceFuture",
        "ticker": symbol_to_code(symbol),
        "symbol": symbol_to_code(symbol),
        "contract": symbol,
        "uuid": order.get("clientOrderId") or str(order.get("orderId")),
        "order_id": str(order.get("orderId")) if order.get("orderId") is not None else None,
        "client_order_id": order.get("clientOrderId"),
        "side": str(order.get("side", "")).lower(),
        "position_side": order.get("positionSide", "BOTH"),
        "price": _safe_float(order.get("price")),
        "qty": qty,
        "qty_contract": qty,
        "executed_qty": executed_qty,
        "left_qty": max(qty - executed_qty, 0.0),
        "avg_price": _safe_float(order.get("avgPrice")),
        "status": status,
        "state": state,
        "reduce_only": bool(order.get("reduceOnly", False)),
        "timestamp": int(order.get("updateTime") or order.get("time") or _now_ms()),
        "identifier": f"{symbol}_{order.get('origQty', 0)}_{order.get('price', 0)}",
        "raw": order,
    }


def _extract_symbol_from_option(option: dict[str, Any]) -> str | None:
    symbol = option.get("symbol") or option.get("ticker") or option.get("contract")
    if symbol:
        return normalize_symbol(str(symbol))
    return None


# ---------- public endpoints ----------
async def get_ticker_list() -> tuple[list[str], dict[str, dict[str, Any]]]:
    global _TICKER_LIST_CACHE

    ttl_sec = _ticker_list_cache_ttl_sec()
    now_ms = int(time.time() * 1000)
    if _TICKER_LIST_CACHE is not None:
        cached_at_ms, cached_payload = _TICKER_LIST_CACHE
        if ttl_sec > 0 and (now_ms - cached_at_ms) < ttl_sec * 1000:
            return _clone_ticker_list_cache(cached_payload)

    status, data, _ = await _request("GET", "/fapi/v1/exchangeInfo")
    if status != 200:
        if _TICKER_LIST_CACHE is not None:
            print(f"[binance][get_ticker_list][warn] using stale cache after fetch failure: status={status}, data={data}")
            return _clone_ticker_list_cache(_TICKER_LIST_CACHE[1])
        raise RuntimeError(f"Failed to fetch exchangeInfo: status={status}, data={data}")

    ticker_list: list[str] = []
    info: dict[str, dict[str, Any]] = {}

    for row in data.get("symbols", []):
        if not (
            row.get("contractType") == "PERPETUAL"
            and row.get("status") == "TRADING"
            and str(row.get("symbol", "")).endswith("USDT")
        ):
            continue
        code = symbol_to_code(row["symbol"])
        ticker_list.append(code)
        info[code] = _parse_symbol_info(row)

    payload = (ticker_list, info)
    _TICKER_LIST_CACHE = (now_ms, payload)
    return _clone_ticker_list_cache(payload)


async def get_market_prefill(symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    status, data, _ = await _request("GET", "/fapi/v1/ticker/bookTicker")
    if status != 200 or not isinstance(data, list):
        return {}

    allowed_contracts = {normalize_symbol(symbol) for symbol in (symbols or [])}
    desktop_time = _now_ms()
    result: dict[str, dict[str, Any]] = {}

    for row in data:
        contract = str(row.get("symbol", "")).upper()
        if not contract.endswith("USDT"):
            continue
        if allowed_contracts and contract not in allowed_contracts:
            continue

        symbol = symbol_to_code(contract)
        best_bid = _safe_float(row.get("bidPrice"), None)
        best_ask = _safe_float(row.get("askPrice"), None)
        if best_bid in (None, 0.0) or best_ask in (None, 0.0):
            continue

        result[symbol] = {
            "ticker": symbol,
            "desktop_time": desktop_time,
            "server_time": int(row.get("time") or desktop_time),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid": best_bid,
            "ask": best_ask,
            "mid": (best_bid + best_ask) / 2.0,
            "spread": best_ask - best_bid,
            "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
            "bid_qty_top": _safe_float(row.get("bidQty"), None),
            "ask_qty_top": _safe_float(row.get("askQty"), None),
            "depth_levels": 1,
            "update_id_final": int(row.get("lastUpdateId") or 0),
            "prefill_source": "rest",
            "raw": row,
        }

    return result


async def get_funding_rate(symbol: str) -> dict[str, Any] | None:
    normalized = normalize_symbol(symbol)
    status, data, _ = await _request("GET", "/fapi/v1/premiumIndex", params={"symbol": normalized})
    if status != 200:
        return None
    return {
        "exchange": "binanceFuture",
        "symbol": symbol_to_code(normalized),
        "funding_rate": _safe_float(data.get("lastFundingRate"), None),
        "next_funding_time": _safe_int(data.get("nextFundingTime"), None),
        "mark_price": _safe_float(data.get("markPrice"), None),
        "index_price": _safe_float(data.get("indexPrice"), None),
        "timestamp": _now_ms(),
        "raw": data,
    }


async def get_funding_rate_history(
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]] | None:
    normalized = normalize_symbol(symbol)
    params: dict[str, Any] = {"symbol": normalized, "limit": limit}
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    status, data, _ = await _request("GET", "/fapi/v1/fundingRate", params=params)
    if status != 200:
        return None

    return [
        {
            "exchange": "binanceFuture",
            "symbol": symbol_to_code(normalized),
            "funding_time": int(row.get("fundingTime") or 0),
            "funding_rate": _safe_float(row.get("fundingRate")),
            "mark_price": _safe_float(row.get("markPrice"), None),
            "raw": row,
        }
        for row in data
    ]


async def get_ohlcv_1m(symbol: str, limit: int = 500) -> list[dict[str, Any]] | None:
    normalized = normalize_symbol(symbol)
    status, data, _ = await _request(
        "GET",
        "/fapi/v1/klines",
        params={"symbol": normalized, "interval": "1m", "limit": limit},
    )
    if status != 200:
        return None

    return [
        {
            "exchange": "binanceFuture",
            "symbol": symbol_to_code(normalized),
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
            "raw": row,
        }
        for row in data
    ]


# ---------- signed account / order endpoints ----------
async def get_accounts(option: dict[str, Any] = {}) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    status, data, _ = await _request("GET", "/fapi/v2/account", signed=True)
    if status == 200:
        return process_account_data(data)
    return None, None


async def get_margin_mode(symbol: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    status, data, _ = await _request(
        "GET",
        "/fapi/v2/positionRisk",
        params={"symbol": normalized},
        signed=True,
    )
    if status != 200:
        return {
            "ok": False,
            "symbol": symbol_to_code(normalized),
            "margin_mode": None,
            "error": data,
        }

    rows = data if isinstance(data, list) else [data]
    row = next(
        (item for item in rows if isinstance(item, dict) and item.get("positionSide") in (None, "", "BOTH")),
        rows[0] if rows else {},
    )
    raw_mode = row.get("marginType") if isinstance(row, dict) else None
    return {
        "ok": bool(raw_mode),
        "symbol": symbol_to_code(normalized),
        "margin_mode": raw_mode,
        "error": "" if raw_mode else "margin_mode_missing",
    }


async def post_limit_order(symbol: str, side: str, amount: float, price: float, option: dict[str, Any] = {}) -> dict[str, Any]:
    raw_symbol = symbol_to_code(normalize_symbol(symbol))
    normalized_symbol = normalize_symbol(symbol)
    side_upper = side.upper()

    info = option.get("info", {})
    symbol_info = info.get(raw_symbol, {})

    tick_size = _safe_float(symbol_info.get("tick_size"))
    step_size = _safe_float(symbol_info.get("step_size"))
    min_q = _safe_float(symbol_info.get("min_q"))
    max_q = _safe_float(symbol_info.get("max_q"), None)
    min_notional = _safe_float(symbol_info.get("min_notional"))

    normalized_price = normalize_order_price(float(price), tick_size, side_upper) if tick_size > 0 else float(price)
    normalized_qty = normalize_order_qty(float(amount), step_size, min_q, max_q) if step_size > 0 and min_q > 0 else float(amount)

    if normalized_qty <= 0:
        return {"error": "normalized_qty_is_zero", "raw": {"symbol": normalized_symbol, "qty": amount}}

    if min_notional > 0 and normalized_price * normalized_qty < min_notional:
        return {
            "error": "below_min_notional",
            "raw": {
                "symbol": normalized_symbol,
                "normalized_qty": normalized_qty,
                "normalized_price": normalized_price,
                "min_notional": min_notional,
            },
        }

    params: dict[str, Any] = {
        "symbol": normalized_symbol,
        "side": side_upper,
        "type": "LIMIT",
        "quantity": normalized_qty,
        "price": normalized_price,
        "timeInForce": option.get("time_in_force", "GTC"),
    }

    if option.get("reduce_only", False):
        params["reduceOnly"] = "true"
    if option.get("client_order_id"):
        params["newClientOrderId"] = option["client_order_id"]
    if option.get("position_side"):
        params["positionSide"] = option["position_side"]
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, headers = await _request("POST", "/fapi/v1/order", params=params, signed=True)
    if status == 200:
        parsed = _normalize_order(data)
        parsed["rate_limits"] = _build_rate_limits(headers)
        return parsed

    return {"error": data.get("msg", "Unknown error") if isinstance(data, dict) else str(data), "code": data.get("code") if isinstance(data, dict) else None, "raw": data}


async def post_market_order(symbol: str, side: str, amount: float, option: dict[str, Any] = {}) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    side_upper = side.upper()

    info = option.get("info", {})
    raw_symbol = symbol_to_code(normalized_symbol)
    symbol_info = info.get(raw_symbol, {})
    step_size = _safe_float(symbol_info.get("step_size"))
    min_q = _safe_float(symbol_info.get("min_q"))
    max_q = _safe_float(symbol_info.get("max_q"), None)

    normalized_qty = normalize_order_qty(float(amount), step_size, min_q, max_q) if step_size > 0 and min_q > 0 else float(amount)
    if normalized_qty <= 0:
        return {"error": "normalized_qty_is_zero", "raw": {"symbol": normalized_symbol, "qty": amount}}

    params: dict[str, Any] = {
        "symbol": normalized_symbol,
        "side": side_upper,
        "type": "MARKET",
        "quantity": normalized_qty,
    }
    if option.get("reduce_only", False):
        params["reduceOnly"] = "true"
    if option.get("client_order_id"):
        params["newClientOrderId"] = option["client_order_id"]
    if option.get("position_side"):
        params["positionSide"] = option["position_side"]
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, headers = await _request("POST", "/fapi/v1/order", params=params, signed=True)
    if status == 200:
        parsed = _normalize_order(data)
        parsed["rate_limits"] = _build_rate_limits(headers)
        return parsed

    return {"error": data.get("msg", "Unknown error") if isinstance(data, dict) else str(data), "code": data.get("code") if isinstance(data, dict) else None, "raw": data}


async def get_order(order_id: str, option: dict[str, Any] = {}) -> dict[str, Any] | None:
    symbol = _extract_symbol_from_option(option)
    if not symbol:
        return {"error": "symbol_required", "raw": {"order_id": order_id, "hint": "Binance requires symbol for get_order"}}

    params: dict[str, Any] = {"symbol": symbol}
    lookup_by = option.get("lookup_by")

    if lookup_by == "client_order_id" or option.get("client_order_id"):
        params["origClientOrderId"] = option.get("client_order_id") or order_id
    elif str(order_id).isdigit():
        params["orderId"] = int(order_id)
    else:
        params["origClientOrderId"] = order_id

    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, _ = await _request("GET", "/fapi/v1/order", params=params, signed=True)
    if status == 200:
        return _normalize_order(data)
    if status == 400 and isinstance(data, dict) and data.get("code") == -2013:
        return {"state": "not_exist", "error": "not_exist", "code": data.get("code"), "raw": data}
    return {"error": data.get("msg", "Unknown error") if isinstance(data, dict) else str(data), "code": data.get("code") if isinstance(data, dict) else None, "raw": data}


async def get_open_orders(option: dict[str, Any] = {}) -> list[dict[str, Any]] | None:
    params: dict[str, Any] = {}
    symbol = _extract_symbol_from_option(option)
    if symbol:
        params["symbol"] = symbol
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, _ = await _request("GET", "/fapi/v1/openOrders", params=params, signed=True)
    if status != 200:
        return None
    return [_normalize_order(row) for row in data]


async def delete_order(order_id: str, option: dict[str, Any] = {}) -> dict[str, Any] | None:
    symbol = _extract_symbol_from_option(option)
    if not symbol:
        return {"error": "symbol_required", "raw": {"order_id": order_id, "hint": "Binance requires symbol for delete_order"}}

    params: dict[str, Any] = {"symbol": symbol}
    lookup_by = option.get("lookup_by")
    if lookup_by == "client_order_id" or option.get("client_order_id"):
        params["origClientOrderId"] = option.get("client_order_id") or order_id
    elif str(order_id).isdigit():
        params["orderId"] = int(order_id)
    else:
        params["origClientOrderId"] = order_id
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, _ = await _request("DELETE", "/fapi/v1/order", params=params, signed=True)
    if status == 200:
        parsed = _normalize_order(data)
        parsed["ok"] = True
        return parsed
    if status == 400 and isinstance(data, dict) and data.get("code") in (-2011, -2013):
        return {"error": "not_exist", "code": data.get("code"), "raw": data}
    return {"error": data.get("msg", "Unknown error") if isinstance(data, dict) else str(data), "code": data.get("code") if isinstance(data, dict) else None, "raw": data}


async def cancel_all_orders(option: dict[str, Any] = {}) -> dict[str, Any]:
    params: dict[str, Any] = {}
    symbol = _extract_symbol_from_option(option)
    if symbol:
        params["symbol"] = symbol
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, _ = await _request("DELETE", "/fapi/v1/allOpenOrders", params=params, signed=True)
    if status == 200:
        return {"ok": True, "symbol": symbol_to_code(symbol) if symbol else None, "raw": data}
    return {"ok": False, "error": data.get("msg", "Unknown error") if isinstance(data, dict) else str(data), "code": data.get("code") if isinstance(data, dict) else None, "raw": data}


async def set_leverage(symbol: str, leverage: int) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    status, data, _ = await _request(
        "POST",
        "/fapi/v1/leverage",
        params={"symbol": normalized, "leverage": leverage},
        signed=True,
    )
    if status == 200:
        return {"ok": True, "symbol": symbol_to_code(normalized), "leverage": int(data.get("leverage", leverage)), "raw": data}
    return {"ok": False, "error": data, "raw": data}


async def set_margin_mode(symbol: str, mode: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)

    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode in {"cross", "crossed"}:
        api_mode = "CROSSED"
    elif normalized_mode in {"isolated", "isolate"}:
        api_mode = "ISOLATED"
    else:
        return {
            "ok": False,
            "error": f"unsupported margin mode: {mode}",
            "raw": {"symbol": normalized, "mode": mode},
        }

    status, data, _ = await _request(
        "POST",
        "/fapi/v1/marginType",
        params={"symbol": normalized, "marginType": api_mode},
        signed=True,
    )

    if status == 200 or (isinstance(data, dict) and data.get("msg") == "No need to change margin type."):
        return {
            "ok": True,
            "symbol": symbol_to_code(normalized),
            "margin_mode": api_mode,
            "raw": data,
        }

    return {
        "ok": False,
        "error": data,
        "raw": data,
    }


async def get_my_trades(
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
    option: dict[str, Any] = {},
) -> list[dict[str, Any]] | None:
    normalized = normalize_symbol(symbol)
    params: dict[str, Any] = {"symbol": normalized, "limit": limit}
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time
    if option.get("from_id") is not None:
        params["fromId"] = int(option["from_id"])
    if option.get("recv_window"):
        params["recvWindow"] = int(option["recv_window"])

    status, data, _ = await _request("GET", "/fapi/v1/userTrades", params=params, signed=True)
    if status != 200:
        return None

    out: list[dict[str, Any]] = []
    for row in data:
        timestamp = int(row.get("time") or _now_ms())
        price = _safe_float(row.get("price"))
        qty = _safe_float(row.get("qty"))
        out.append(
            {
                "exchange": "binanceFuture",
                "symbol": symbol_to_code(normalized),
                "ticker": symbol_to_code(normalized),
                "trade_id": str(row.get("id")),
                "order_id": str(row.get("orderId")) if row.get("orderId") is not None else None,
                "side": "buy" if bool(row.get("buyer", False)) else "sell",
                "price": price,
                "qty": qty,
                "qty_contract": qty,
                "quote_qty": _safe_float(row.get("quoteQty"), price * qty),
                "realized_pnl": _safe_float(row.get("realizedPnl")),
                "commission": _safe_float(row.get("commission")),
                "commission_asset": row.get("commissionAsset"),
                "fee": _safe_float(row.get("commission")),
                "fee_ccy": row.get("commissionAsset"),
                "maker": bool(row.get("maker", False)),
                "buyer": bool(row.get("buyer", False)),
                "role": "maker" if bool(row.get("maker", False)) else "taker",
                "position_side": row.get("positionSide", "BOTH"),
                "timestamp": timestamp,
                "create_time": timestamp,
                "raw": row,
            }
        )
    return out
