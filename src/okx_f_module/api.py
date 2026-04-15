import base64
import hashlib
import hmac
import json
import os
import socket
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any
from urllib.parse import urlencode

import aiohttp
from dotenv import load_dotenv

load_dotenv()

OKX_API_KEY = os.getenv("OKX_ACCESS_KEY")
OKX_SECRET_KEY = os.getenv("OKX_SECRET_KEY")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE")
BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com")
SIMULATED_TRADING = os.getenv("OKX_SIMULATED_TRADING", "0")
OKX_FORCE_IPV4 = os.getenv("OKX_FORCE_IPV4", "1") == "1"

_TIME_OFFSET_MS = 0
_TICKER_LIST_CACHE: tuple[int, tuple[list[str], dict[str, dict[str, Any]]]] | None = None
_LOG_THROTTLE_MS: dict[str, int] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ticker_list_cache_ttl_sec() -> int:
    return int(float(os.getenv("OKX_TICKER_LIST_CACHE_SEC", "21600")))


def _log_throttled(key: str, message: str, cooldown_sec: float = 10.0) -> None:
    now = _now_ms()
    last = _LOG_THROTTLE_MS.get(key, 0)
    if now - last >= int(cooldown_sec * 1000):
        print(message)
        _LOG_THROTTLE_MS[key] = now


def _clone_ticker_list_cache(payload: tuple[list[str], dict[str, dict[str, Any]]]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    ticker_list, info = payload
    return list(ticker_list), {symbol: dict(row) for symbol, row in info.items()}


def _ts() -> int:
    return _now_ms() + _TIME_OFFSET_MS


def _okx_timestamp() -> str:
    dt = datetime.fromtimestamp(_ts() / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


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


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "-").replace("_", "-")
    if symbol.endswith("-USDT-SWAP"):
        return symbol
    if symbol.endswith("USDT-SWAP"):
        base = symbol[:-9].rstrip("-")
        return f"{base}-USDT-SWAP"
    if symbol.endswith("-USDT"):
        return f"{symbol}-SWAP"
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT-SWAP"
    return f"{symbol}-USDT-SWAP"


def symbol_to_code(inst_id: str) -> str:
    normalized = str(inst_id or "").upper()
    return normalized.replace("-USDT-SWAP", "").replace("-USDT", "")


def contract_size_to_token_qty(size: int | float | str, ct_val: float) -> float:
    return float(size) * float(ct_val)


def token_qty_to_contract_size(
    token_qty: float,
    ct_val: float,
    side: str,
    min_size: float = 1.0,
    step_size: float | None = None,
) -> float:
    if ct_val <= 0:
        return 0.0
    raw_contracts = abs(float(token_qty)) / float(ct_val)
    normalized_step = float(step_size if step_size is not None else min_size)
    contracts = floor_by_step(raw_contracts, normalized_step) if normalized_step > 0 else raw_contracts
    if contracts < float(min_size):
        return 0.0
    return contracts


def normalize_order_price(price: float, tick_size: float, side: str) -> float:
    return floor_by_step(price, tick_size) if side.upper() == "BUY" else ceil_by_step(price, tick_size)


def _build_rate_limits(headers: dict[str, str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    remaining = headers.get("x-ratelimit-remaining")
    limit = headers.get("x-ratelimit-limit")
    reset = headers.get("x-ratelimit-reset")

    try:
        if remaining is not None and limit is not None:
            remaining_i = int(remaining)
            limit_i = int(limit)
            result.append(
                {
                    "type": "REQUESTS",
                    "count": max(limit_i - remaining_i, 0),
                    "limit": limit_i,
                    "interval": "WINDOW",
                    "intervalNum": _safe_int(reset, 0),
                }
            )
    except Exception:
        pass

    return result


def create_signature(
    timestamp: str,
    method: str,
    request_path: str,
    body: str = "",
    secret_key: str | None = None,
) -> str:
    secret = secret_key or OKX_SECRET_KEY or ""
    prehash = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


async def _request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | list[Any] | None = None,
    auth: bool = False,
    timeout: int = 10,
) -> tuple[int, Any, dict[str, str]]:
    params = dict(params or {})
    query = urlencode(params, doseq=True)
    request_path = path if not query else f"{path}?{query}"
    url = f"{BASE_URL}{request_path}"

    body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""

    headers = {"Content-Type": "application/json"}
    if SIMULATED_TRADING in {"1", "true", "TRUE"}:
        headers["x-simulated-trading"] = "1"

    if auth:
        timestamp = _okx_timestamp()
        headers.update(
            {
                "OK-ACCESS-KEY": OKX_API_KEY or "",
                "OK-ACCESS-SIGN": create_signature(timestamp, method, request_path, body),
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE or "",
            }
        )

    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(family=socket.AF_INET) if OKX_FORCE_IPV4 else None
    async with aiohttp.ClientSession(timeout=timeout_obj, connector=connector) as session:
        async with session.request(method.upper(), url, headers=headers, data=body or None) as response:
            try:
                data = await response.json(content_type=None)
            except Exception:
                data = await response.text()
            return response.status, data, dict(response.headers)


async def get_server_time() -> int | None:
    status, data, _ = await _request("GET", "/api/v5/public/time")
    if status != 200 or not isinstance(data, dict):
        return None
    rows = data.get("data") or []
    if not rows:
        return None
    return _safe_int(rows[0].get("ts"), 0) or None


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


def _parse_contract_info(row: dict[str, Any]) -> dict[str, Any]:
    ct_val = _safe_float(row.get("ctVal"), 1.0)
    min_sz_contract = _safe_float(row.get("minSz"), 1.0)
    lot_sz_contract = _safe_float(row.get("lotSz"), min_sz_contract)
    max_lmt_sz = _safe_float(row.get("maxLmtSz"), 0.0)

    return {
        "symbol": symbol_to_code(row.get("instId")),
        "contract": row.get("instId"),
        "inst_id": row.get("instId"),
        "inst_family": row.get("instFamily"),
        "tick_size": _safe_float(row.get("tickSz")),
        "lot_q": lot_sz_contract,
        "step_size": ct_val * lot_sz_contract,
        "cont_size": ct_val,
        "ct_val": ct_val,
        "min_q": min_sz_contract,
        "max_q": max_lmt_sz,
        "min_qty": min_sz_contract * ct_val,
        "max_qty": max_lmt_sz * ct_val if max_lmt_sz > 0 else 0.0,
        "lever": _safe_float(row.get("lever"), 0.0),
        "ct_type": row.get("ctType"),
        "settle_ccy": row.get("settleCcy"),
        "state": row.get("state"),
        "raw": row,
    }


def _normalize_order(order: dict[str, Any], info: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    inst_id = order.get("instId", "")
    symbol = symbol_to_code(inst_id) if inst_id else order.get("symbol", "")
    meta = (info or {}).get(symbol, {})
    ct_val = float(meta.get("ct_val", 1.0))

    qty_contract = _safe_float(order.get("sz"))
    fill_sz_contract = _safe_float(order.get("accFillSz"))
    qty = contract_size_to_token_qty(qty_contract, ct_val)
    executed_qty = contract_size_to_token_qty(fill_sz_contract, ct_val)

    state = str(order.get("state", order.get("status", "UNKNOWN"))).lower()
    normalized_state = "exist" if state in {"live", "partially_filled"} else "not_exist"

    side = str(order.get("side", "")).lower()
    pos_side = order.get("posSide", "net")

    return {
        "exchange": "okxFuture",
        "ticker": symbol,
        "symbol": symbol,
        "contract": inst_id,
        "uuid": order.get("clOrdId") or str(order.get("ordId")),
        "order_id": str(order.get("ordId")) if order.get("ordId") is not None else None,
        "client_order_id": order.get("clOrdId"),
        "side": side,
        "position_side": pos_side.upper() if pos_side else "NET",
        "price": _safe_float(order.get("px")),
        "qty": qty,
        "qty_contract": qty_contract,
        "executed_qty": executed_qty,
        "left_qty": max(qty - executed_qty, 0.0),
        "avg_price": _safe_float(order.get("avgPx")),
        "status": str(order.get("state", order.get("status", "UNKNOWN"))).upper(),
        "state": normalized_state,
        "reduce_only": str(order.get("reduceOnly", "false")).lower() == "true",
        "timestamp": _safe_int(order.get("uTime") or order.get("cTime") or _ts()),
        "identifier": f"{inst_id}_{qty_contract}_{order.get('px', 0)}",
        "raw": order,
    }


def _normalize_trade(row: dict[str, Any]) -> dict[str, Any]:
    inst_id = row.get("instId", "")
    symbol = symbol_to_code(inst_id) if inst_id else None
    side = str(row.get("side", "")).lower()
    qty_contract = _safe_float(row.get("fillSz"))
    ct_val = _safe_float(row.get("ctVal"), 0.0)
    qty = contract_size_to_token_qty(qty_contract, ct_val) if ct_val > 0 else qty_contract
    exec_type = row.get("execType")
    role = "maker" if exec_type == "M" else "taker" if exec_type == "T" else None
    timestamp = _safe_int(row.get("fillTime") or row.get("ts"))

    price = _safe_float(row.get("fillPx"))
    quote_qty = (price * qty) if (price is not None and qty is not None) else None

    return {
        "exchange": "okxFuture",
        "symbol": symbol,
        "ticker": symbol,
        "trade_id": str(row.get("tradeId")) if row.get("tradeId") is not None else None,
        "order_id": str(row.get("ordId")) if row.get("ordId") is not None else None,
        "client_order_id": row.get("clOrdId"),
        "side": side,
        "price": price,
        "qty": qty,
        "qty_contract": qty_contract,
        "quote_qty": quote_qty,
        "realized_pnl": _safe_float(row.get("fillPnl")),
        "fee": _safe_float(row.get("fee")),
        "fee_ccy": row.get("feeCcy"),
        "execution_type": exec_type,
        "role": role,
        "pos_side": row.get("posSide"),
        "timestamp": timestamp,
        "create_time": timestamp,
        "raw": row,
    }


def _normalize_order_lookup(order_id: str, option: dict[str, Any]) -> tuple[str | None, dict[str, Any], str]:
    symbol = option.get("ticker") or option.get("symbol") or option.get("contract")
    inst_id = normalize_symbol(str(symbol)) if symbol else None
    params: dict[str, Any] = {}
    lookup_by = option.get("lookup_by")

    if lookup_by == "client_order_id" or option.get("client_order_id"):
        params["clOrdId"] = option.get("client_order_id") or order_id
    elif str(order_id).isdigit():
        params["ordId"] = str(order_id)
    else:
        params["clOrdId"] = str(order_id)

    return inst_id, params, params.get("clOrdId") or params.get("ordId") or str(order_id)


def _build_client_order_id(prefix: str, symbol: str) -> str:
    raw = f"{prefix}{symbol.upper()}{_now_ms()}"
    safe = "".join(ch for ch in raw if ch.isalnum())
    return safe[:32]


def process_account_data(
    positions: list[dict[str, Any]],
    balance_payload: dict[str, Any],
    info: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    account_dict: dict[str, dict[str, Any]] = {}
    asset_dict: dict[str, dict[str, Any]] = {}
    timestamp = _now_ms()

    details = ((balance_payload.get("details") or [{}])[0:]) if isinstance(balance_payload, dict) else [{}]
    usdt_row = None
    for row in details:
        if str(row.get("ccy", "")).upper() == "USDT":
            usdt_row = row
            break
    if usdt_row is None and details:
        usdt_row = details[0]

    if isinstance(usdt_row, dict):
        equity = _safe_float(usdt_row.get("eq"))
        available = _safe_float(usdt_row.get("availEq"), equity)
        unreal = _safe_float(usdt_row.get("upl"))
        frozen = _safe_float(usdt_row.get("frozenBal"))
        asset_dict["USDT"] = {
            "balance": equity,
            "available": available,
            "availableBalance": available,
            "marginBalance": _safe_float(usdt_row.get("eqUsd"), equity),
            "locked": max(frozen, equity - available, 0.0),
            "unrealized_profit": unreal,
            "currency": usdt_row.get("ccy", "USDT"),
            "timestamp": timestamp,
            "raw": usdt_row,
        }

    for pos in positions:
        inst_id = pos.get("instId")
        if not inst_id:
            continue

        symbol = symbol_to_code(inst_id)
        if symbol not in info:
            continue

        ct_val = float(info[symbol].get("ct_val", 1.0))
        qty = contract_size_to_token_qty(pos.get("pos", 0), ct_val)

        account_dict[symbol] = {
            "balance": qty,
            "nowPrice": _safe_float(pos.get("markPx")),
            "markPrice": _safe_float(pos.get("markPx")),
            "locked": 0.0,
            "unrealized_profit": _safe_float(pos.get("upl")),
            "avg_price": _safe_float(pos.get("avgPx")),
            "liquidation_price": _safe_float(pos.get("liqPx")),
            "leverage": _safe_float(pos.get("lever")),
            "margin_type": pos.get("mgnMode"),
            "positionSide": str(pos.get("posSide", "net")).upper(),
            "timestamp": timestamp,
            "raw": pos,
        }

    return account_dict, asset_dict


async def get_ticker_list() -> tuple[list[str], dict[str, dict[str, Any]]] | None:
    global _TICKER_LIST_CACHE

    ttl_sec = _ticker_list_cache_ttl_sec()
    now_ms = _now_ms()
    if _TICKER_LIST_CACHE is not None:
        cached_at_ms, cached_payload = _TICKER_LIST_CACHE
        if ttl_sec > 0 and (now_ms - cached_at_ms) < ttl_sec * 1000:
            return _clone_ticker_list_cache(cached_payload)

    status, data, _ = await _request("GET", "/api/v5/public/instruments", params={"instType": "SWAP"})
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        if _TICKER_LIST_CACHE is not None:
            print(f"[okx][get_ticker_list][warn] using stale cache after fetch failure: {data}")
            return _clone_ticker_list_cache(_TICKER_LIST_CACHE[1])
        print("[okx][get_ticker_list] error:", data)
        return None

    ticker_list: list[str] = []
    ticker_info: dict[str, dict[str, Any]] = {}
    for row in data.get("data", []):
        if row.get("state") != "live":
            continue
        inst_id = str(row.get("instId", ""))
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        info = _parse_contract_info(row)
        ticker_list.append(info["symbol"])
        ticker_info[info["symbol"]] = info
    payload = (ticker_list, ticker_info)
    _TICKER_LIST_CACHE = (now_ms, payload)
    return _clone_ticker_list_cache(payload)


async def get_market_prefill(symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    status, data, _ = await _request("GET", "/api/v5/market/tickers", params={"instType": "SWAP"})
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return {}

    allowed_inst_ids = {normalize_symbol(symbol) for symbol in (symbols or [])}
    result: dict[str, dict[str, Any]] = {}
    desktop_time = _now_ms()

    for row in data.get("data", []):
        inst_id = str(row.get("instId", "")).upper()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        if allowed_inst_ids and inst_id not in allowed_inst_ids:
            continue

        best_bid = _safe_float(row.get("bidPx"), None)
        best_ask = _safe_float(row.get("askPx"), None)
        if best_bid in (None, 0.0) or best_ask in (None, 0.0):
            continue

        ct_val = _safe_float(row.get("ctVal"), 1.0)
        bid_size = _safe_float(row.get("bidSz"), None)
        ask_size = _safe_float(row.get("askSz"), None)
        symbol = symbol_to_code(inst_id)

        result[symbol] = {
            "ticker": symbol,
            "desktop_time": desktop_time,
            "server_time": _safe_int(row.get("ts"), desktop_time),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid": best_bid,
            "ask": best_ask,
            "mid": (best_bid + best_ask) / 2.0,
            "spread": best_ask - best_bid,
            "spread_bps": ((best_ask - best_bid) / best_ask * 10000.0) if best_ask > 0 else 0.0,
            "bid_qty_top": (bid_size * ct_val) if bid_size is not None else None,
            "ask_qty_top": (ask_size * ct_val) if ask_size is not None else None,
            "depth_levels": 1,
            "prefill_source": "rest",
            "raw": row,
        }

    return result


async def get_contract_detail(symbol: str) -> dict[str, Any] | None:
    inst_id = normalize_symbol(symbol)
    status, data, _ = await _request(
        "GET",
        "/api/v5/public/instruments",
        params={"instType": "SWAP", "instId": inst_id},
    )
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        print("[okx][get_contract_detail] error:", data)
        return None
    rows = data.get("data") or []
    if not rows:
        return None
    return _parse_contract_info(rows[0])


async def get_funding_rate(symbol: str) -> dict[str, Any] | None:
    inst_id = normalize_symbol(symbol)
    status, data, _ = await _request("GET", "/api/v5/public/funding-rate", params={"instId": inst_id})

    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return None

    rows = data.get("data") or []
    if not rows:
        return None

    row = rows[0]

    return {
        "exchange": "okxFuture",
        "symbol": symbol_to_code(inst_id),
        "funding_rate": _safe_float(row.get("fundingRate"), None),
        "next_funding_time": _safe_int(row.get("nextFundingTime"), None),
        "timestamp": _now_ms(),
        "raw": row,
    }

async def get_funding_rate_history(
    symbol: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]] | None:
    inst_id = normalize_symbol(symbol)
    params: dict[str, Any] = {"instId": inst_id, "limit": limit}
    if start_time is not None:
        params["after"] = int(start_time)
    if end_time is not None:
        params["before"] = int(end_time)

    status, data, _ = await _request("GET", "/api/v5/public/funding-rate-history", params=params)
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return None

    return [
        {
            "exchange": "okxFuture",
            "symbol": symbol_to_code(inst_id),
            "funding_time": _safe_int(row.get("fundingTime")),
            "funding_rate": _safe_float(row.get("fundingRate")),
            "realized_rate": _safe_float(row.get("realizedRate")),
            "raw": row,
        }
        for row in data.get("data", [])
    ]


async def get_ohlcv_1m(symbol: str, limit: int = 500) -> list[dict[str, Any]] | None:
    inst_id = normalize_symbol(symbol)
    status, data, _ = await _request(
        "GET",
        "/api/v5/market/history-candles",
        params={"instId": inst_id, "bar": "1m", "limit": limit},
    )
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return None

    rows: list[dict[str, Any]] = []
    for row in data.get("data", []):
        rows.append(
            {
                "exchange": "okxFuture",
                "symbol": symbol_to_code(inst_id),
                "open_time": _safe_int(row[0]),
                "open": _safe_float(row[1]),
                "high": _safe_float(row[2]),
                "low": _safe_float(row[3]),
                "close": _safe_float(row[4]),
                "volume": _safe_float(row[5]),
                "close_time": _safe_int(row[0]) + 60_000 - 1,
                "raw": row,
            }
        )
    rows.reverse()
    return rows


async def get_accounts(option: dict[str, Any] = {}) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    info = option.get("info", {})

    balance_status, balance_data, _ = await _request("GET", "/api/v5/account/balance", auth=True)
    if balance_status != 200 or not isinstance(balance_data, dict) or balance_data.get("code") != "0":
        _log_throttled("get_accounts_balance_error", f"[okx][get_accounts][balance] error: {balance_data}")
        return None, None

    position_status, position_data, _ = await _request(
        "GET",
        "/api/v5/account/positions",
        params={"instType": "SWAP"},
        auth=True,
    )
    if position_status != 200 or not isinstance(position_data, dict) or position_data.get("code") != "0":
        _log_throttled("get_accounts_positions_error", f"[okx][get_accounts][positions] error: {position_data}")
        return None, None

    balance_rows = balance_data.get("data") or []
    balance_payload = balance_rows[0] if balance_rows else {}
    return process_account_data(position_data.get("data", []), balance_payload, info)


async def post_limit_order(symbol: str, side: str, amount: float, price: float, option: dict[str, Any] = {}) -> dict[str, Any]:
    info = option.get("info", {})
    ticker = symbol.upper()
    if ticker not in info:
        detail = await get_contract_detail(ticker)
        if detail is None:
            return {"error": "contract_detail_missing", "symbol": ticker}
        info = {**info, ticker: detail}

    meta = info[ticker]
    parsed_price = normalize_order_price(float(price), float(meta.get("tick_size", 0.0)), side)
    parsed_amount = token_qty_to_contract_size(
        token_qty=float(amount),
        ct_val=float(meta.get("ct_val", 1.0)),
        side=side,
        min_size=float(meta.get("min_q", 1.0)),
        step_size=float(meta.get("lot_q", meta.get("min_q", 1.0))),
    )

    if parsed_amount == 0:
        return {"error": "normalized_qty_is_zero", "raw": {"symbol": ticker, "qty": amount}}

    cl_ord_id = option.get("client_order_id") or _build_client_order_id("okxl", ticker)
    td_mode = str(option.get("td_mode", option.get("margin_mode", "cross"))).lower()

    payload = {
        "instId": normalize_symbol(ticker),
        "tdMode": td_mode,
        "clOrdId": cl_ord_id[:32],
        "side": side.lower(),
        "ordType": "limit",
        "px": str(parsed_price),
        "sz": str(parsed_amount),
    }
    if option.get("reduce_only", False):
        payload["reduceOnly"] = "true"
    if option.get("position_side"):
        payload["posSide"] = str(option["position_side"]).lower()

    status, data, headers = await _request("POST", "/api/v5/trade/order", payload=payload, auth=True)
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return {"error": data, "raw": data}

    rows = data.get("data") or []
    if not rows:
        return {"error": "empty_order_response", "raw": data}
    row = rows[0]
    if row.get("sCode") not in (None, "", "0"):
        return {"error": row.get("sMsg") or "order_rejected", "code": row.get("sCode"), "raw": data}

    normalized = _normalize_order(
        {
            "instId": payload["instId"],
            "clOrdId": payload["clOrdId"],
            "ordId": row.get("ordId"),
            "side": payload["side"],
            "posSide": payload.get("posSide", "net"),
            "px": payload["px"],
            "sz": payload["sz"],
            "accFillSz": "0",
            "state": "live",
            "reduceOnly": payload.get("reduceOnly", "false"),
            "cTime": _ts(),
        },
        info,
    )
    normalized["rate_limits"] = _build_rate_limits(headers)
    return normalized


async def post_market_order(symbol: str, side: str, amount: float, option: dict[str, Any] = {}) -> dict[str, Any]:
    info = option.get("info", {})
    ticker = symbol.upper()
    if ticker not in info:
        detail = await get_contract_detail(ticker)
        if detail is None:
            return {"error": "contract_detail_missing", "symbol": ticker}
        info = {**info, ticker: detail}

    meta = info[ticker]
    parsed_amount = token_qty_to_contract_size(
        token_qty=float(amount),
        ct_val=float(meta.get("ct_val", 1.0)),
        side=side,
        min_size=float(meta.get("min_q", 1.0)),
        step_size=float(meta.get("lot_q", meta.get("min_q", 1.0))),
    )

    if parsed_amount == 0:
        return {"error": "normalized_qty_is_zero", "raw": {"symbol": ticker, "qty": amount}}

    cl_ord_id = option.get("client_order_id") or _build_client_order_id("okxm", ticker)
    td_mode = str(option.get("td_mode", option.get("margin_mode", "cross"))).lower()

    payload = {
        "instId": normalize_symbol(ticker),
        "tdMode": td_mode,
        "clOrdId": cl_ord_id[:32],
        "side": side.lower(),
        "ordType": "market",
        "sz": str(parsed_amount),
    }
    if option.get("reduce_only", False):
        payload["reduceOnly"] = "true"
    if option.get("position_side"):
        payload["posSide"] = str(option["position_side"]).lower()

    status, data, headers = await _request("POST", "/api/v5/trade/order", payload=payload, auth=True)
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return {"error": data, "raw": data}

    rows = data.get("data") or []
    if not rows:
        return {"error": "empty_order_response", "raw": data}
    row = rows[0]
    if row.get("sCode") not in (None, "", "0"):
        return {"error": row.get("sMsg") or "order_rejected", "code": row.get("sCode"), "raw": data}

    normalized = _normalize_order(
        {
            "instId": payload["instId"],
            "clOrdId": payload["clOrdId"],
            "ordId": row.get("ordId"),
            "side": payload["side"],
            "posSide": payload.get("posSide", "net"),
            "px": "0",
            "sz": payload["sz"],
            "accFillSz": "0",
            "state": "live",
            "reduceOnly": payload.get("reduceOnly", "false"),
            "cTime": _ts(),
        },
        info,
    )
    normalized["rate_limits"] = _build_rate_limits(headers)
    return normalized


async def get_order(order_id: str, option: dict[str, Any] = {}) -> dict[str, Any] | None:
    inst_id, params, _ = _normalize_order_lookup(order_id, option)
    if not inst_id:
        return {"error": "symbol_required", "raw": {"order_id": order_id}}

    params["instId"] = inst_id
    status, data, _ = await _request("GET", "/api/v5/trade/order", params=params, auth=True)
    if status != 200 or not isinstance(data, dict):
        return None
    if data.get("code") != "0":
        if data.get("code") in {"51603", "51604"}:
            return {"state": "not_exist", "error": "not_exist", "raw": data}
        return {"error": data, "raw": data}
    rows = data.get("data") or []
    if not rows:
        return {"state": "not_exist", "error": "not_exist", "raw": data}
    return _normalize_order(rows[0], option.get("info"))


async def get_open_orders(option: dict[str, Any] = {}) -> list[dict[str, Any]] | None:
    params: dict[str, Any] = {"instType": "SWAP"}
    symbol = option.get("symbol") or option.get("ticker") or option.get("contract")
    if symbol:
        params["instId"] = normalize_symbol(str(symbol))

    status, data, _ = await _request("GET", "/api/v5/trade/orders-pending", params=params, auth=True)
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        return None
    info = option.get("info")
    return [_normalize_order(row, info) for row in data.get("data", [])]


async def delete_order(order_id: str, option: dict[str, Any] = {}) -> dict[str, Any] | None:
    inst_id, _, lookup_value = _normalize_order_lookup(order_id, option)
    if not inst_id:
        return {"error": "symbol_required", "raw": {"order_id": order_id}}

    payload: dict[str, Any] = {"instId": inst_id}
    if str(lookup_value).isdigit() and not option.get("client_order_id"):
        payload["ordId"] = lookup_value
    else:
        payload["clOrdId"] = lookup_value

    status, data, _ = await _request("POST", "/api/v5/trade/cancel-order", payload=payload, auth=True)
    if status != 200 or not isinstance(data, dict):
        return None
    if data.get("code") != "0":
        return {"error": data, "raw": data}

    rows = data.get("data") or []
    if not rows:
        return {"state": "not_exist", "raw": data}
    row = rows[0]
    if row.get("sCode") not in (None, "", "0"):
        return {"error": "not_exist", "code": row.get("sCode"), "raw": data}
    return {"ok": True, "state": "not_exist", "uuid": payload.get("clOrdId") or payload.get("ordId"), "raw": data}


async def cancel_all_orders(option: dict[str, Any] = {}) -> dict[str, Any]:
    open_orders = await get_open_orders(option)
    if open_orders is None:
        return {"ok": False, "symbol": option.get("ticker") or option.get("symbol"), "raw": None}

    cancelled: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for order in open_orders:
        result = await delete_order(
            order.get("order_id") or order.get("uuid"),
            {
                "ticker": order.get("ticker"),
                "symbol": order.get("symbol"),
                "client_order_id": order.get("client_order_id"),
            },
        )
        if result and not result.get("error"):
            cancelled.append(result)
        else:
            failed.append(result or {"order": order})

    return {
        "ok": len(failed) == 0,
        "symbol": option.get("ticker") or option.get("symbol"),
        "cancelled": cancelled,
        "failed": failed,
        "raw": {"requested": len(open_orders)},
    }


async def set_leverage(symbol: str, leverage: int | float, margin_mode: str = "cross") -> dict[str, Any]:
    inst_id = normalize_symbol(symbol)
    normalized_mode = str(margin_mode or "cross").lower()
    if normalized_mode == "crossed":
        normalized_mode = "cross"
    if normalized_mode == "isolated":
        normalized_mode = "isolated"

    payload = {
        "instId": inst_id,
        "lever": str(leverage),
        "mgnMode": normalized_mode,
    }

    status, data, _ = await _request("POST", "/api/v5/account/set-leverage", payload=payload, auth=True)
    if status == 200 and isinstance(data, dict) and data.get("code") == "0":
        return {
            "ok": True,
            "symbol": symbol_to_code(inst_id),
            "leverage": float(leverage),
            "margin_mode": normalized_mode.upper(),
            "raw": data,
        }
    return {
        "ok": False,
        "symbol": symbol_to_code(inst_id),
        "leverage": float(leverage),
        "margin_mode": normalized_mode.upper(),
        "raw": data,
    }


async def set_margin_mode(symbol: str, mode: str) -> dict[str, Any]:
    normalized_mode = str(mode or "").upper()
    return {
        "ok": False,
        "symbol": str(symbol).upper(),
        "margin_mode": normalized_mode,
        "error": "okx_margin_mode_requires_leverage",
        "message": "OKX margin mode is configured together with leverage via set_leverage/bootstrap_symbol.",
    }


async def get_my_trades(
    symbol: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 100,
    option: dict[str, Any] = {},
) -> list[dict[str, Any]] | None:
    params: dict[str, Any] = {"instType": "SWAP", "limit": limit}
    if symbol:
        params["instId"] = normalize_symbol(symbol)
    if start_time is not None:
        params["after"] = int(start_time)
    if end_time is not None:
        params["before"] = int(end_time)

    status, data, _ = await _request("GET", "/api/v5/trade/fills", params=params, auth=True)
    if status != 200 or not isinstance(data, dict) or data.get("code") != "0":
        print("[okx][get_my_trades] error:", data)
        return None

    ct_val_cache: dict[str, float] = {}
    if symbol:
        detail = await get_contract_detail(symbol)
        if detail is not None:
            ct_val_cache[normalize_symbol(symbol)] = float(detail.get("ct_val", 0.0))

    normalized_rows: list[dict[str, Any]] = []
    for row in data.get("data", []):
        inst_id = row.get("instId")
        if inst_id and inst_id not in ct_val_cache:
            detail = await get_contract_detail(symbol_to_code(inst_id))
            ct_val_cache[inst_id] = float(detail.get("ct_val", 0.0)) if detail is not None else 0.0

        enriched = dict(row)
        if inst_id:
            enriched["ctVal"] = ct_val_cache.get(inst_id, 0.0)
        normalized_rows.append(_normalize_trade(enriched))

    return normalized_rows
