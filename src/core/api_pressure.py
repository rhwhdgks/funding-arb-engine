from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LOCK = threading.Lock()
_WINDOW_START_MS = 0
_LAST_FLUSH_MS = 0
_BUCKET: dict[str, Any] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _enabled() -> bool:
    return _env_bool("API_PRESSURE_ENABLED", True)


def _flush_interval_ms() -> int:
    try:
        return max(1, int(float(os.getenv("API_PRESSURE_FLUSH_INTERVAL_SEC", "60")) * 1000))
    except Exception:
        return 60_000


def _jsonl_path() -> Path:
    raw = os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH", str(PROJECT_ROOT / "logs" / "strategy_live.jsonl"))
    path = Path(raw).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _error_code(data: Any) -> str:
    if isinstance(data, dict):
        value = data.get("code") or data.get("errorCode") or data.get("sCode")
        return str(value) if value not in (None, "") else ""
    return ""


def classify_category(exchange: str, method: str, path: str) -> str:
    exchange = exchange.lower()
    method = method.upper()
    path = path.split("?", 1)[0]
    lowered = path.lower()

    if exchange == "binance":
        if "usertrades" in lowered or "allorders" in lowered:
            return "trade_history"
        if method in {"POST", "DELETE", "PUT"} and "/order" in lowered:
            return "trade_write"
        if "/openorders" in lowered or lowered.endswith("/order"):
            return "private_order_read"
        if "/account" in lowered or "/balance" in lowered or "/positionrisk" in lowered:
            return "private_account"
        if "/time" in lowered or "/exchangeinfo" in lowered or "/ticker" in lowered or "/premiumindex" in lowered:
            return "public_market"
        return "unknown"

    if exchange == "okx":
        if "/trade/fills" in lowered or "/trade/orders-history" in lowered:
            return "trade_history"
        if method in {"POST", "DELETE"} and "/trade/" in lowered:
            return "trade_write"
        if "/trade/orders-pending" in lowered or "/trade/order" in lowered:
            return "private_order_read"
        if "/account/" in lowered:
            return "private_account"
        if "/public/" in lowered or "/market/" in lowered:
            return "public_market"
        return "unknown"

    return "unknown"


def _new_exchange_bucket() -> dict[str, Any]:
    return {
        "remote_calls": 0,
        "local_skips": 0,
        "by_category": Counter(),
        "by_path": Counter(),
        "status_counts": Counter(),
        "error_code_counts": Counter(),
        "transient_counts": Counter(),
        "latency_ms_total": 0.0,
        "latency_ms_max": 0.0,
        "retry_attempts": 0,
        "retry_exhausted": 0,
        "private_read_pause_started": 0,
        "private_read_pause_skips": 0,
        "rate_limit_violations": 0,
        "ip_ban_events": 0,
        "max_request_weight_usage_1m": 0.0,
        "max_order_count_usage_10s": 0.0,
        "max_order_count_usage_1m": 0.0,
    }


def _ensure_bucket(now_ms: int) -> None:
    global _WINDOW_START_MS, _LAST_FLUSH_MS, _BUCKET
    if _WINDOW_START_MS <= 0:
        _WINDOW_START_MS = now_ms
        _LAST_FLUSH_MS = now_ms
        _BUCKET = {"exchanges": defaultdict(_new_exchange_bucket)}


def _usage_key(item: dict[str, Any]) -> str | None:
    kind = str(item.get("type") or "").upper()
    interval = str(item.get("interval") or "").upper()
    interval_num = int(_safe_float(item.get("intervalNum"), 0.0))
    if kind == "REQUEST_WEIGHT" and interval == "MINUTE" and interval_num == 1:
        return "max_request_weight_usage_1m"
    if kind == "ORDERS" and interval == "SECOND" and interval_num == 10:
        return "max_order_count_usage_10s"
    if kind == "ORDERS" and interval == "MINUTE" and interval_num == 1:
        return "max_order_count_usage_1m"
    return None


def _usage_ratio(item: dict[str, Any]) -> float:
    limit = _safe_float(item.get("limit"), 0.0)
    if limit <= 0:
        return 0.0
    return max(_safe_float(item.get("count"), 0.0) / limit, 0.0)


def _serializable_exchange_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    top_limit = max(1, int(float(os.getenv("API_PRESSURE_TOP_ENDPOINT_LIMIT", "10") or 10)))
    remote_calls = int(bucket.get("remote_calls") or 0)
    latency_avg = (float(bucket.get("latency_ms_total") or 0.0) / remote_calls) if remote_calls else 0.0
    return {
        "remote_calls": remote_calls,
        "local_skips": int(bucket.get("local_skips") or 0),
        "by_category": dict(bucket.get("by_category") or {}),
        "by_path": dict((bucket.get("by_path") or Counter()).most_common(top_limit)),
        "status_counts": dict(bucket.get("status_counts") or {}),
        "error_code_counts": dict(bucket.get("error_code_counts") or {}),
        "transient_counts": dict(bucket.get("transient_counts") or {}),
        "latency_ms_avg": latency_avg,
        "latency_ms_max": float(bucket.get("latency_ms_max") or 0.0),
        "retry_attempts": int(bucket.get("retry_attempts") or 0),
        "retry_exhausted": int(bucket.get("retry_exhausted") or 0),
        "private_read_pause_started": int(bucket.get("private_read_pause_started") or 0),
        "private_read_pause_skips": int(bucket.get("private_read_pause_skips") or 0),
        "rate_limit_violations": int(bucket.get("rate_limit_violations") or 0),
        "ip_ban_events": int(bucket.get("ip_ban_events") or 0),
        "max_request_weight_usage_1m": float(bucket.get("max_request_weight_usage_1m") or 0.0),
        "max_order_count_usage_10s": float(bucket.get("max_order_count_usage_10s") or 0.0),
        "max_order_count_usage_1m": float(bucket.get("max_order_count_usage_1m") or 0.0),
    }


def _build_summary(now_ms: int) -> dict[str, Any]:
    exchanges = _BUCKET.get("exchanges") or {}
    return {
        "ts_ms": now_ms,
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ms / 1000)),
        "event_type": "api_pressure_summary",
        "window_start_ms": _WINDOW_START_MS,
        "window_end_ms": now_ms,
        "window_sec": max((now_ms - _WINDOW_START_MS) / 1000.0, 0.0),
        "exchanges": {
            str(exchange): _serializable_exchange_bucket(bucket)
            for exchange, bucket in exchanges.items()
        },
    }


def _append_summary(row: dict[str, Any]) -> None:
    path = _jsonl_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        # API pressure reporting must never block trading paths.
        return


def flush_api_pressure(*, force: bool = False) -> dict[str, Any] | None:
    global _WINDOW_START_MS, _LAST_FLUSH_MS, _BUCKET
    if not _enabled():
        return None

    now_ms = _now_ms()
    with _LOCK:
        _ensure_bucket(now_ms)
        if not force and now_ms - _LAST_FLUSH_MS < _flush_interval_ms():
            return None
        summary = _build_summary(now_ms)
        _WINDOW_START_MS = now_ms
        _LAST_FLUSH_MS = now_ms
        _BUCKET = {"exchanges": defaultdict(_new_exchange_bucket)}

    _append_summary(summary)
    return summary


def record_api_call(
    *,
    exchange: str,
    method: str,
    path: str,
    status: int | None = None,
    data: Any = None,
    headers: dict[str, str] | None = None,
    rate_limits: list[dict[str, Any]] | None = None,
    latency_ms: float | None = None,
    local_pause: bool = False,
    transient_reason: str | None = None,
    retry_attempt: bool = False,
    retry_exhausted: bool = False,
    private_read_pause_started: bool = False,
    private_read_pause_skip: bool = False,
) -> None:
    if not _enabled():
        return

    now_ms = _now_ms()
    exchange_name = str(exchange or "unknown").lower()
    method_name = str(method or "GET").upper()
    clean_path = str(path or "unknown").split("?", 1)[0]
    category = classify_category(exchange_name, method_name, clean_path)
    code = _error_code(data)
    status_text = str(status) if status is not None else "0"

    with _LOCK:
        _ensure_bucket(now_ms)
        bucket = _BUCKET["exchanges"][exchange_name]
        if local_pause:
            bucket["local_skips"] += 1
        else:
            bucket["remote_calls"] += 1
            bucket["latency_ms_total"] += max(float(latency_ms or 0.0), 0.0)
            bucket["latency_ms_max"] = max(bucket["latency_ms_max"], max(float(latency_ms or 0.0), 0.0))

        bucket["by_category"][category] += 1
        bucket["by_path"][f"{method_name} {clean_path}"] += 1
        bucket["status_counts"][status_text] += 1
        if code:
            bucket["error_code_counts"][code] += 1
        if transient_reason:
            bucket["transient_counts"][str(transient_reason)] += 1
        if retry_attempt:
            bucket["retry_attempts"] += 1
        if retry_exhausted:
            bucket["retry_exhausted"] += 1
        if private_read_pause_started:
            bucket["private_read_pause_started"] += 1
        if private_read_pause_skip:
            bucket["private_read_pause_skips"] += 1

        if not local_pause:
            if exchange_name == "binance" and (status in {429, 418} or code == "-1003"):
                bucket["rate_limit_violations"] += 1
            if exchange_name == "binance" and status == 418:
                bucket["ip_ban_events"] += 1
            if exchange_name == "okx" and code == "50011":
                bucket["rate_limit_violations"] += 1

        for item in rate_limits or []:
            key = _usage_key(item)
            if key:
                bucket[key] = max(float(bucket.get(key) or 0.0), _usage_ratio(item))

        should_flush = now_ms - _LAST_FLUSH_MS >= _flush_interval_ms()

    if should_flush:
        flush_api_pressure(force=True)

