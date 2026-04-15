from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src.ops.web.actions import execute_exchange_action, web_trading_enabled
from src.ops.persistence.db import get_db_connection
from src.ops.persistence.live_report import utc_now_iso
from src.ops.persistence.sync import SUPPORTED_EXCHANGES, build_default_symbols, parse_csv_list
from src.ops.web.runtime import OpsRuntimeManager

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_EXCHANGES = os.getenv("TARGET_EXCHANGES") or os.getenv("TARGET_EXCHANGE", "okx")
DEFAULT_SYMBOLS = ",".join(build_default_symbols())
DEFAULT_WAIT_WS_SEC = float(os.getenv("REPORT_WAIT_WS_SEC", "6"))
DEFAULT_TRADE_LIMIT = int(os.getenv("REPORT_TRADE_LIMIT", "5"))
DEFAULT_DB_TRADE_LIMIT = int(os.getenv("WEB_DB_TRADE_LIMIT", "20"))
DEFAULT_DB_FUNDING_LIMIT = int(os.getenv("WEB_DB_FUNDING_LIMIT", "20"))
DEFAULT_DB_EVENT_LIMIT = int(os.getenv("WEB_DB_EVENT_LIMIT", "50"))
DEFAULT_DB_ORDER_LIMIT = int(os.getenv("WEB_DB_ORDER_LIMIT", "30"))
DEFAULT_DB_BALANCE_LIMIT = int(os.getenv("WEB_DB_BALANCE_LIMIT", "30"))
DEFAULT_DB_POSITION_LIMIT = int(os.getenv("WEB_DB_POSITION_LIMIT", "60"))
DEFAULT_DB_TRACE_LIMIT = int(os.getenv("WEB_DB_TRACE_LIMIT", "120"))
DEFAULT_DB_TRACE_WINDOW_SEC = int(os.getenv("WEB_DB_TRACE_WINDOW_SEC", "300"))
DEFAULT_DB_TRACE_SNAPSHOT_LIMIT = int(os.getenv("WEB_DB_TRACE_SNAPSHOT_LIMIT", "60"))
DEFAULT_SOAK_JSONL_PATH = os.getenv("SOAK_JSONL_PATH", str(PROJECT_ROOT / "logs" / "runtime_soak.jsonl"))
DEFAULT_SOAK_RECENT_LIMIT = int(os.getenv("WEB_SOAK_RECENT_LIMIT", "80"))
DEFAULT_ACTION_WAIT_WS_SEC = float(os.getenv("WEB_ACTION_SETTLE_WAIT_SEC", os.getenv("WEB_ACTION_POST_WAIT_SEC", "0.0")))
WEB_ACTION_TOKEN = os.getenv("WEB_ACTION_TOKEN", "").strip()
WEB_ACTION_TOKEN_REQUIRED = os.getenv("WEB_ACTION_TOKEN_REQUIRED", "1" if WEB_ACTION_TOKEN else "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
RUNTIME_MANAGER = OpsRuntimeManager()
DEFAULT_EXCHANGES = ",".join(RUNTIME_MANAGER.exchange_names) or DEFAULT_EXCHANGES
DEFAULT_SYMBOLS = ",".join(RUNTIME_MANAGER.symbols) or DEFAULT_SYMBOLS

app = FastAPI(title="펀딩 운영 관제", version="0.1.0")


@app.on_event("startup")
async def startup_runtime_manager():
    await _get_runtime_manager().start()


@app.on_event("shutdown")
async def shutdown_runtime_manager():
    await _get_runtime_manager().stop()


class LimitOrderRequest(BaseModel):
    exchange: str
    symbol: str
    side: str
    amount: float = Field(gt=0)
    price: float = Field(gt=0)
    wait_ws_sec: float | None = Field(default=None, ge=0.5, le=30.0)


class CancelOrderRequest(BaseModel):
    exchange: str
    symbol: str
    uuid: str
    wait_ws_sec: float | None = Field(default=None, ge=0.5, le=30.0)


class CancelAllRequest(BaseModel):
    exchange: str
    symbol: str
    wait_ws_sec: float | None = Field(default=None, ge=0.5, le=30.0)


def _validate_exchanges(raw: str) -> list[str]:
    exchange_names = [name.strip().lower() for name in raw.split(",") if name.strip()]
    if not exchange_names:
        raise HTTPException(status_code=400, detail="조회할 거래소가 없습니다.")
    for exchange_name in exchange_names:
        if exchange_name not in SUPPORTED_EXCHANGES:
            raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")
    return exchange_names


def _validate_symbols(raw: str | None) -> list[str]:
    symbols = parse_csv_list(raw, fallback=build_default_symbols())
    if not symbols:
        raise HTTPException(status_code=400, detail="조회할 심볼이 없습니다.")
    return symbols


def _require_web_trading_enabled() -> None:
    if not web_trading_enabled():
        raise HTTPException(
            status_code=403,
            detail="웹 주문 기능이 비활성화되어 있습니다. ENABLE_WEB_TRADING=1 설정 후 웹 서비스를 재시작하세요.",
        )


def _get_runtime_manager() -> OpsRuntimeManager:
    return RUNTIME_MANAGER


def _ensure_runtime_request(exchange_names: list[str], symbols: list[str]) -> None:
    runtime_manager = _get_runtime_manager()
    configured_exchanges = set(runtime_manager.exchange_names)
    configured_symbols = set(runtime_manager.symbols)

    missing_exchanges = [exchange_name for exchange_name in exchange_names if exchange_name not in configured_exchanges]
    if missing_exchanges:
        raise HTTPException(
            status_code=400,
            detail=f"resident runtime에 올라오지 않은 거래소입니다: {missing_exchanges}. configured={runtime_manager.exchange_names}",
        )

    missing_symbols = [symbol for symbol in symbols if symbol not in configured_symbols]
    if missing_symbols:
        raise HTTPException(
            status_code=400,
            detail=f"resident runtime에 올라오지 않은 심볼입니다: {missing_symbols}. configured={runtime_manager.symbols}",
        )


def _require_web_action_authorized(ops_token: str | None) -> None:
    _require_web_trading_enabled()
    if not WEB_ACTION_TOKEN_REQUIRED:
        return
    if not WEB_ACTION_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="웹 주문 토큰이 필수인데 WEB_ACTION_TOKEN이 비어 있습니다. 서버 설정을 확인하세요.",
        )
    if (ops_token or "").strip() != WEB_ACTION_TOKEN:
        raise HTTPException(status_code=401, detail="유효한 웹 주문 토큰이 필요합니다.")


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _fetch_rows(query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows: list[dict[str, Any]] = []
        for raw_row in cur.fetchall():
            rows.append({column: _serialize_value(value) for column, value in zip(columns, raw_row)})
        return rows
    finally:
        cur.close()
        conn.close()


def fetch_trade_history_rows(exchange_name: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, symbol, trade_id, order_id, client_order_id,
        side, role, position_side, price, qty, qty_contract, quote_qty,
        fee, fee_ccy, realized_pnl, create_time_ms, created_at_utc
    FROM trade_history
    WHERE exchange_name = ?
      AND symbol = ?
    ORDER BY create_time_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, symbol.upper(), limit))


def fetch_recent_trade_rows_map(
    exchange_names: list[str],
    symbols: list[str],
    limit: int,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    try:
        return {
            exchange_name: {
                symbol: fetch_trade_history_rows(exchange_name, symbol, limit)
                for symbol in symbols
            }
            for exchange_name in exchange_names
        }
    except Exception:
        return {
            exchange_name: {
                symbol: []
                for symbol in symbols
            }
            for exchange_name in exchange_names
        }


def fetch_funding_history_rows(exchange_name: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, symbol, funding_time_ms, funding_time_utc,
        funding_rate, realized_rate, mark_price, index_price, next_funding_time_ms
    FROM funding_history
    WHERE exchange_name = ?
      AND symbol = ?
    ORDER BY funding_time_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, symbol.upper(), limit))


def fetch_order_event_rows(exchange_name: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, symbol, order_uid, order_id, client_order_id,
        event_type, event_source, side, position_side, price, qty, qty_contract,
        executed_qty, left_qty, avg_price, status, state, error_code, error_message,
        event_time_ms, event_time_utc
    FROM order_events
    WHERE exchange_name = ?
      AND symbol = ?
    ORDER BY event_time_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, symbol.upper(), limit))


def fetch_order_history_rows(exchange_name: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
        side, position_side, price, qty, qty_contract, executed_qty, left_qty, avg_price,
        status, state, reduce_only, source, first_seen_ms, first_seen_at_utc,
        last_seen_ms, last_seen_at_utc, is_open
    FROM order_history
    WHERE exchange_name = ?
      AND symbol = ?
    ORDER BY last_seen_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, symbol.upper(), limit))


def fetch_balance_snapshot_rows(exchange_name: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, tracked_symbols_csv, snapshot_time_ms, snapshot_time_utc,
        balance_asset_code, wallet_balance_usd, available_balance_usd, locked_balance_usd,
        margin_balance_usd, exchange_unrealized_profit_usd, theoretical_unrealized_profit_usd,
        actual_equity_usd, theoretical_equity_usd, theoretical_equity_gap_usd,
        total_balance_usd, long_notional_usd, short_notional_usd, gross_notional_usd, net_notional_usd,
        leverage, delta_leverage, position_count
    FROM balance_snapshots
    WHERE exchange_name = ?
    ORDER BY snapshot_time_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, limit))


def fetch_position_snapshot_rows(exchange_name: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    query = """
    SELECT
        exchange_name, tracked_symbols_csv, symbol, snapshot_time_ms, snapshot_time_utc,
        position_side, qty, entry_price, mark_price, mark_source, notional_mark_usd,
        exchange_unrealized_profit_usd, theoretical_unrealized_profit_usd, unrealized_profit_gap_usd,
        theoretical_upl_source
    FROM position_snapshots
    WHERE exchange_name = ?
      AND symbol = ?
    ORDER BY snapshot_time_ms DESC, id DESC
    LIMIT ?
    """
    return _fetch_rows(query, (exchange_name, symbol.upper(), limit))


def _read_jsonl_tail(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    if limit > 0:
        return rows[-limit:]
    return rows


def fetch_soak_status(path_value: str | None = None, limit: int = DEFAULT_SOAK_RECENT_LIMIT) -> dict[str, Any]:
    path = Path(path_value or DEFAULT_SOAK_JSONL_PATH)
    rows = _read_jsonl_tail(path, limit)
    latest_summary = None
    recent_cycles: list[dict[str, Any]] = []
    recent_issues: list[dict[str, Any]] = []
    exchange_last: dict[str, dict[str, Any]] = {}

    for row in rows:
        if "soak_summary" in row:
            latest_summary = row.get("soak_summary")
            continue

        exchange_name = row.get("exchange")
        if exchange_name:
            exchange_last[exchange_name] = {
                "phase": row.get("phase"),
                "cycle": row.get("cycle"),
                "errors": row.get("errors") or [],
                "waiting_count": row.get("waiting_count"),
                "merged_open_count": row.get("merged_open_count"),
                "ws_ready_map": row.get("ws_ready_map") or {},
                "reconcile_status": row.get("reconcile_status") or row.get("reconcile_summary") or {},
            }

        if row.get("phase"):
            recent_cycles.append(
                {
                    "exchange": exchange_name,
                    "phase": row.get("phase"),
                    "cycle": row.get("cycle"),
                    "errors": row.get("errors") or [],
                    "generated_at": row.get("generated_at"),
                    "reconcile_summary": row.get("reconcile_summary") or {},
                }
            )
        if row.get("errors"):
            recent_issues.append(row)

    if latest_summary is None and exchange_last:
        latest_summary = [
            {
                "exchange": exchange_name,
                "symbols": [],
                "cycles_ran": exchange_row.get("cycle") or 0,
                "restart_count": 1 if exchange_row.get("phase") == "post_restart" else 0,
                "failure_count": 1 if exchange_row.get("errors") else 0,
                "failures": exchange_row.get("errors") or [],
                "inferred": True,
            }
            for exchange_name, exchange_row in sorted(exchange_last.items())
        ]

    updated_at = None
    if path.exists():
        updated_at = datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()

    return {
        "available": path.exists(),
        "path": str(path),
        "updated_at": updated_at,
        "latest_summary": latest_summary,
        "exchange_last": exchange_last,
        "recent_cycles": recent_cycles[-20:],
        "recent_issues": recent_issues[-10:],
        "row_count": len(rows),
    }


def _clean_order_ref(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _unique_order_refs(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = _clean_order_ref(value)
        if text is None or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _collect_order_refs(*row_groups: list[dict[str, Any]]) -> list[str]:
    collected: list[Any] = []
    for rows in row_groups:
        for row in rows:
            collected.extend(
                [
                    row.get("order_uid"),
                    row.get("order_id"),
                    row.get("client_order_id"),
                    row.get("uuid"),
                ]
            )
    return _unique_order_refs(collected)


def _build_identifier_clause(columns: tuple[str, ...], values: list[str]) -> tuple[str, tuple[Any, ...]]:
    if not values:
        return "1=0", ()

    placeholders = ", ".join("?" for _ in values)
    clauses = [f"{column} IN ({placeholders})" for column in columns]
    params: list[Any] = []
    for _ in columns:
        params.extend(values)
    return "(" + " OR ".join(clauses) + ")", tuple(params)


def _trace_utc_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0).astimezone().isoformat()


def _trace_window_ms(
    order_events: list[dict[str, Any]],
    order_history: list[dict[str, Any]],
    trade_history: list[dict[str, Any]],
    window_sec: int,
) -> tuple[int | None, int | None]:
    points: list[int] = []

    for row in order_events:
        value = row.get("event_time_ms")
        if isinstance(value, (int, float)):
            points.append(int(value))
    for row in order_history:
        for key in ("first_seen_ms", "last_seen_ms"):
            value = row.get(key)
            if isinstance(value, (int, float)):
                points.append(int(value))
    for row in trade_history:
        value = row.get("create_time_ms")
        if isinstance(value, (int, float)):
            points.append(int(value))

    if not points:
        return None, None

    pad_ms = max(1, int(window_sec)) * 1000
    return min(points) - pad_ms, max(points) + pad_ms


def fetch_order_trace_rows(
    exchange_name: str,
    symbol: str,
    order_ref: str,
    *,
    row_limit: int,
    snapshot_limit: int,
    window_sec: int,
) -> dict[str, Any]:
    symbol_name = symbol.upper()
    base_refs = _unique_order_refs([order_ref])
    if not base_refs:
        raise HTTPException(status_code=400, detail="추적할 주문 식별자가 필요합니다.")

    order_clause, order_params = _build_identifier_clause(("order_uid", "order_id", "client_order_id", "uuid"), base_refs)
    trade_clause, trade_params = _build_identifier_clause(("order_id", "client_order_id"), base_refs)

    order_event_rows = _fetch_rows(
        f"""
        SELECT
            exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
            event_type, event_source, side, position_side, price, qty, qty_contract,
            executed_qty, left_qty, avg_price, status, state, error_code, error_message,
            event_time_ms, event_time_utc
        FROM order_events
        WHERE exchange_name = ?
          AND symbol = ?
          AND {order_clause}
        ORDER BY event_time_ms DESC, id DESC
        LIMIT ?
        """,
        (exchange_name, symbol_name, *order_params, row_limit),
    )
    order_history_rows = _fetch_rows(
        f"""
        SELECT
            exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
            side, position_side, price, qty, qty_contract, executed_qty, left_qty, avg_price,
            status, state, reduce_only, source, first_seen_ms, first_seen_at_utc,
            last_seen_ms, last_seen_at_utc, is_open
        FROM order_history
        WHERE exchange_name = ?
          AND symbol = ?
          AND {order_clause}
        ORDER BY last_seen_ms DESC, id DESC
        LIMIT ?
        """,
        (exchange_name, symbol_name, *order_params, row_limit),
    )
    trade_history_rows = _fetch_rows(
        f"""
        SELECT
            exchange_name, symbol, trade_id, order_id, client_order_id,
            side, role, position_side, price, qty, qty_contract, quote_qty,
            fee, fee_ccy, realized_pnl, create_time_ms, created_at_utc
        FROM trade_history
        WHERE exchange_name = ?
          AND symbol = ?
          AND {trade_clause}
        ORDER BY create_time_ms DESC, id DESC
        LIMIT ?
        """,
        (exchange_name, symbol_name, *trade_params, row_limit),
    )

    related_refs = _collect_order_refs(order_event_rows, order_history_rows, trade_history_rows)
    if related_refs and related_refs != base_refs:
        order_clause, order_params = _build_identifier_clause(("order_uid", "order_id", "client_order_id", "uuid"), related_refs)
        trade_clause, trade_params = _build_identifier_clause(("order_id", "client_order_id"), related_refs)
        order_event_rows = _fetch_rows(
            f"""
            SELECT
                exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
                event_type, event_source, side, position_side, price, qty, qty_contract,
                executed_qty, left_qty, avg_price, status, state, error_code, error_message,
                event_time_ms, event_time_utc
            FROM order_events
            WHERE exchange_name = ?
              AND symbol = ?
              AND {order_clause}
            ORDER BY event_time_ms DESC, id DESC
            LIMIT ?
            """,
            (exchange_name, symbol_name, *order_params, row_limit),
        )
        order_history_rows = _fetch_rows(
            f"""
            SELECT
                exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
                side, position_side, price, qty, qty_contract, executed_qty, left_qty, avg_price,
                status, state, reduce_only, source, first_seen_ms, first_seen_at_utc,
                last_seen_ms, last_seen_at_utc, is_open
            FROM order_history
            WHERE exchange_name = ?
              AND symbol = ?
              AND {order_clause}
            ORDER BY last_seen_ms DESC, id DESC
            LIMIT ?
            """,
            (exchange_name, symbol_name, *order_params, row_limit),
        )
        trade_history_rows = _fetch_rows(
            f"""
            SELECT
                exchange_name, symbol, trade_id, order_id, client_order_id,
                side, role, position_side, price, qty, qty_contract, quote_qty,
                fee, fee_ccy, realized_pnl, create_time_ms, created_at_utc
            FROM trade_history
            WHERE exchange_name = ?
              AND symbol = ?
              AND {trade_clause}
            ORDER BY create_time_ms DESC, id DESC
            LIMIT ?
            """,
            (exchange_name, symbol_name, *trade_params, row_limit),
        )
    else:
        related_refs = base_refs

    window_start_ms, window_end_ms = _trace_window_ms(order_event_rows, order_history_rows, trade_history_rows, window_sec)
    balance_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    if window_start_ms is not None and window_end_ms is not None:
        balance_rows = _fetch_rows(
            """
            SELECT
                exchange_name, tracked_symbols_csv, snapshot_time_ms, snapshot_time_utc,
                balance_asset_code, actual_equity_usd, theoretical_equity_usd,
                theoretical_equity_gap_usd, gross_notional_usd, net_notional_usd, position_count
            FROM balance_snapshots
            WHERE exchange_name = ?
              AND snapshot_time_ms BETWEEN ? AND ?
            ORDER BY snapshot_time_ms DESC, id DESC
            LIMIT ?
            """,
            (exchange_name, window_start_ms, window_end_ms, snapshot_limit),
        )
        position_rows = _fetch_rows(
            """
            SELECT
                exchange_name, tracked_symbols_csv, symbol, snapshot_time_ms, snapshot_time_utc,
                position_side, qty, entry_price, mark_price, mark_source, notional_mark_usd,
                exchange_unrealized_profit_usd, theoretical_unrealized_profit_usd, unrealized_profit_gap_usd
            FROM position_snapshots
            WHERE exchange_name = ?
              AND symbol = ?
              AND snapshot_time_ms BETWEEN ? AND ?
            ORDER BY snapshot_time_ms DESC, id DESC
            LIMIT ?
            """,
            (exchange_name, symbol_name, window_start_ms, window_end_ms, snapshot_limit),
        )

    latest_history = order_history_rows[0] if order_history_rows else {}
    latest_event = order_event_rows[0] if order_event_rows else {}
    summary = {
        "lookup_ref": order_ref,
        "resolved_order_uid": latest_history.get("order_uid") or latest_event.get("order_uid"),
        "latest_status": latest_history.get("status") or latest_event.get("status"),
        "latest_state": latest_history.get("state") or latest_event.get("state"),
        "latest_event_type": latest_event.get("event_type"),
        "latest_side": latest_history.get("side") or latest_event.get("side"),
        "latest_qty": latest_history.get("qty") or latest_event.get("qty"),
        "latest_executed_qty": latest_history.get("executed_qty") or latest_event.get("executed_qty"),
        "is_open": latest_history.get("is_open"),
        "event_count": len(order_event_rows),
        "order_snapshot_count": len(order_history_rows),
        "trade_count": len(trade_history_rows),
        "position_snapshot_count": len(position_rows),
        "balance_snapshot_count": len(balance_rows),
        "first_time_ms": min(
            [
                int(value)
                for value in [
                    *[row.get("event_time_ms") for row in order_event_rows],
                    *[row.get("first_seen_ms") for row in order_history_rows],
                    *[row.get("create_time_ms") for row in trade_history_rows],
                ]
                if isinstance(value, (int, float))
            ],
            default=None,
        ),
        "last_time_ms": max(
            [
                int(value)
                for value in [
                    *[row.get("event_time_ms") for row in order_event_rows],
                    *[row.get("last_seen_ms") for row in order_history_rows],
                    *[row.get("create_time_ms") for row in trade_history_rows],
                ]
                if isinstance(value, (int, float))
            ],
            default=None,
        ),
    }
    summary["first_time_utc"] = _trace_utc_iso(summary["first_time_ms"])
    summary["last_time_utc"] = _trace_utc_iso(summary["last_time_ms"])

    return {
        "generated_at": utc_now_iso(),
        "exchange": exchange_name,
        "symbol": symbol_name,
        "lookup_ref": order_ref,
        "related_identifiers": related_refs,
        "summary": summary,
        "time_window": {
            "window_sec": window_sec,
            "start_ms": window_start_ms,
            "start_utc": _trace_utc_iso(window_start_ms),
            "end_ms": window_end_ms,
            "end_utc": _trace_utc_iso(window_end_ms),
        },
        "order_events": order_event_rows,
        "order_history": order_history_rows,
        "trade_history": trade_history_rows,
        "position_snapshots": position_rows,
        "balance_snapshots": balance_rows,
    }


@app.get("/healthz")
async def healthz():
    runtime_manager = _get_runtime_manager()
    return {
        "ok": True,
        "ts": utc_now_iso(),
        "runtime_mode": "resident",
        "runtime_exchanges": runtime_manager.exchange_names,
        "runtime_symbols": runtime_manager.symbols,
    }


@app.get("/api/config")
async def api_config():
    runtime_manager = _get_runtime_manager()
    return {
        "default_exchanges": ",".join(runtime_manager.exchange_names),
        "default_symbols": ",".join(runtime_manager.symbols),
        "default_wait_ws_sec": DEFAULT_WAIT_WS_SEC,
        "default_trade_limit": DEFAULT_TRADE_LIMIT,
        "default_db_trade_limit": DEFAULT_DB_TRADE_LIMIT,
        "default_db_funding_limit": DEFAULT_DB_FUNDING_LIMIT,
        "default_db_event_limit": DEFAULT_DB_EVENT_LIMIT,
        "default_db_order_limit": DEFAULT_DB_ORDER_LIMIT,
        "default_db_balance_limit": DEFAULT_DB_BALANCE_LIMIT,
        "default_db_position_limit": DEFAULT_DB_POSITION_LIMIT,
        "default_db_trace_limit": DEFAULT_DB_TRACE_LIMIT,
        "default_db_trace_window_sec": DEFAULT_DB_TRACE_WINDOW_SEC,
        "default_db_trace_snapshot_limit": DEFAULT_DB_TRACE_SNAPSHOT_LIMIT,
        "default_soak_log_path": DEFAULT_SOAK_JSONL_PATH,
        "default_soak_recent_limit": DEFAULT_SOAK_RECENT_LIMIT,
        "default_action_wait_ws_sec": DEFAULT_ACTION_WAIT_WS_SEC,
        "web_trading_enabled": web_trading_enabled(),
        "web_runtime_mode": "resident",
        "runtime_exchanges": runtime_manager.exchange_names,
        "runtime_symbols": runtime_manager.symbols,
        "runtime_startup_wait_sec": runtime_manager.startup_wait_sec,
        "web_action_token_required": WEB_ACTION_TOKEN_REQUIRED,
        "web_action_token_configured": bool(WEB_ACTION_TOKEN),
        "supported_exchanges": sorted(SUPPORTED_EXCHANGES),
    }


@app.get("/api/live")
async def api_live(
    exchanges: str = Query(DEFAULT_EXCHANGES),
    symbols: str | None = Query(DEFAULT_SYMBOLS),
    wait_ws_sec: float = Query(DEFAULT_WAIT_WS_SEC, ge=0.5, le=30.0),
    trade_limit: int = Query(DEFAULT_TRADE_LIMIT, ge=1, le=100),
):
    exchange_names = _validate_exchanges(exchanges)
    symbol_list = _validate_symbols(symbols)
    _ensure_runtime_request(exchange_names, symbol_list)
    runtime_manager = _get_runtime_manager()
    recent_trades = fetch_recent_trade_rows_map(exchange_names, symbol_list, trade_limit)
    reports = await runtime_manager.build_live_reports(
        exchange_names,
        symbol_list,
        recent_trades_by_exchange=recent_trades,
    )
    return {
        "generated_at": utc_now_iso(),
        "runtime_mode": "resident",
        "requested_wait_ws_sec": wait_ws_sec,
        "reports": reports,
    }


@app.get("/api/history")
async def api_history(
    exchange: str = Query(DEFAULT_EXCHANGES.split(",")[0].strip().lower()),
    symbol: str = Query(build_default_symbols()[0]),
    trade_limit: int = Query(DEFAULT_DB_TRADE_LIMIT, ge=1, le=200),
    funding_limit: int = Query(DEFAULT_DB_FUNDING_LIMIT, ge=1, le=200),
    event_limit: int = Query(DEFAULT_DB_EVENT_LIMIT, ge=1, le=300),
    order_limit: int = Query(DEFAULT_DB_ORDER_LIMIT, ge=1, le=200),
    balance_limit: int = Query(DEFAULT_DB_BALANCE_LIMIT, ge=1, le=300),
    position_limit: int = Query(DEFAULT_DB_POSITION_LIMIT, ge=1, le=500),
):
    exchange_name = exchange.strip().lower()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")

    symbol_name = symbol.strip().upper()
    if not symbol_name:
        raise HTTPException(status_code=400, detail="심볼이 필요합니다.")

    return {
        "generated_at": utc_now_iso(),
        "exchange": exchange_name,
        "symbol": symbol_name,
        "trade_history": fetch_trade_history_rows(exchange_name, symbol_name, trade_limit),
        "funding_history": fetch_funding_history_rows(exchange_name, symbol_name, funding_limit),
        "order_events": fetch_order_event_rows(exchange_name, symbol_name, event_limit),
        "order_history": fetch_order_history_rows(exchange_name, symbol_name, order_limit),
        "balance_snapshots": fetch_balance_snapshot_rows(exchange_name, balance_limit),
        "position_snapshots": fetch_position_snapshot_rows(exchange_name, symbol_name, position_limit),
    }


@app.get("/api/order_trace")
async def api_order_trace(
    exchange: str = Query(DEFAULT_EXCHANGES.split(",")[0].strip().lower()),
    symbol: str = Query(build_default_symbols()[0]),
    order_ref: str = Query(..., min_length=1),
    row_limit: int = Query(DEFAULT_DB_TRACE_LIMIT, ge=1, le=300),
    snapshot_limit: int = Query(DEFAULT_DB_TRACE_SNAPSHOT_LIMIT, ge=1, le=300),
    window_sec: int = Query(DEFAULT_DB_TRACE_WINDOW_SEC, ge=30, le=3600),
):
    exchange_name = exchange.strip().lower()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")

    symbol_name = symbol.strip().upper()
    if not symbol_name:
        raise HTTPException(status_code=400, detail="심볼이 필요합니다.")

    trace_ref = order_ref.strip()
    if not trace_ref:
        raise HTTPException(status_code=400, detail="추적할 주문 식별자가 필요합니다.")

    return fetch_order_trace_rows(
        exchange_name,
        symbol_name,
        trace_ref,
        row_limit=row_limit,
        snapshot_limit=snapshot_limit,
        window_sec=window_sec,
    )


@app.get("/api/soak_status")
async def api_soak_status(
    limit: int = Query(DEFAULT_SOAK_RECENT_LIMIT, ge=10, le=1000),
):
    return {
        "generated_at": utc_now_iso(),
        "soak_status": fetch_soak_status(limit=limit),
    }


@app.post("/api/action/order")
async def api_action_order(
    payload: LimitOrderRequest,
    ops_token: str | None = Header(default=None, alias="X-OPS-TOKEN"),
):
    _require_web_action_authorized(ops_token)
    exchange_name = payload.exchange.strip().lower()
    symbol_name = payload.symbol.strip().upper()
    side = payload.side.strip().lower()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")
    if side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail="side는 buy 또는 sell 이어야 합니다.")
    _ensure_runtime_request([exchange_name], [symbol_name])

    return await execute_exchange_action(
        exchange_name,
        symbol_name,
        "limit_order",
        lambda exchange: exchange.order(symbol_name, side, payload.amount, payload.price),
        wait_ws_sec=payload.wait_ws_sec,
        runtime_manager=_get_runtime_manager(),
    )


@app.post("/api/action/cancel")
async def api_action_cancel(
    payload: CancelOrderRequest,
    ops_token: str | None = Header(default=None, alias="X-OPS-TOKEN"),
):
    _require_web_action_authorized(ops_token)
    exchange_name = payload.exchange.strip().lower()
    symbol_name = payload.symbol.strip().upper()
    uuid = payload.uuid.strip()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")
    if not uuid:
        raise HTTPException(status_code=400, detail="취소할 주문 식별자(uuid)가 필요합니다.")
    _ensure_runtime_request([exchange_name], [symbol_name])

    return await execute_exchange_action(
        exchange_name,
        symbol_name,
        "cancel_order",
        lambda exchange: exchange.cancel_order(symbol_name, uuid),
        wait_ws_sec=payload.wait_ws_sec,
        runtime_manager=_get_runtime_manager(),
    )


@app.post("/api/action/cancel_all")
async def api_action_cancel_all(
    payload: CancelAllRequest,
    ops_token: str | None = Header(default=None, alias="X-OPS-TOKEN"),
):
    _require_web_action_authorized(ops_token)
    exchange_name = payload.exchange.strip().lower()
    symbol_name = payload.symbol.strip().upper()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 거래소입니다: {exchange_name}")
    _ensure_runtime_request([exchange_name], [symbol_name])

    return await execute_exchange_action(
        exchange_name,
        symbol_name,
        "cancel_all",
        lambda exchange: exchange.cancel_all_orders_for_symbol(symbol_name),
        wait_ws_sec=payload.wait_ws_sec,
        runtime_manager=_get_runtime_manager(),
    )


INDEX_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>펀딩 운영 관제</title>
  <style>
    :root {
      --bg: #f3efe5;
      --panel: rgba(255, 252, 246, 0.92);
      --ink: #1d2b2a;
      --muted: #5f6e6c;
      --line: rgba(29, 43, 42, 0.14);
      --accent: #0f766e;
      --accent-soft: rgba(15, 118, 110, 0.12);
      --warn: #b45309;
      --good: #166534;
      --bad: #b91c1c;
      --shadow: 0 14px 42px rgba(29, 43, 42, 0.10);
      --radius: 20px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      color: var(--ink);
      font-family: "Pretendard", "Noto Sans KR", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.20), transparent 28rem),
        radial-gradient(circle at top right, rgba(180, 83, 9, 0.16), transparent 22rem),
        linear-gradient(180deg, #fcfaf6 0%, var(--bg) 100%);
    }

    .shell {
      width: min(1380px, calc(100vw - 32px));
      margin: 24px auto 48px;
    }

    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }

    .hero {
      padding: 28px;
      display: grid;
      gap: 18px;
    }

    .hero-top {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
    }

    .eyebrow {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 7px 12px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    h1 {
      margin: 10px 0 6px;
      font-size: clamp(30px, 4vw, 52px);
      line-height: 0.98;
      letter-spacing: -0.05em;
    }

    .subcopy {
      margin: 0;
      color: var(--muted);
      max-width: 56rem;
      font-size: 15px;
      line-height: 1.6;
    }

    .controls {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }

    .overview-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .overview-card, .trend-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: rgba(255, 255, 255, 0.68);
      display: grid;
      gap: 8px;
    }

    .overview-card .label, .trend-card .label {
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
    }

    .overview-card .value, .trend-card .value {
      font-size: 24px;
      font-weight: 800;
      letter-spacing: -0.05em;
    }

    .overview-card .meta, .trend-card .meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }

    .field {
      display: grid;
      gap: 6px;
    }

    .field label {
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }

    input, select, button {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      background: #fffdfa;
      color: var(--ink);
      font: inherit;
    }

    button {
      cursor: pointer;
      font-weight: 700;
      background: linear-gradient(135deg, #134e4a 0%, #0f766e 100%);
      color: white;
      border: none;
      transition: transform 120ms ease, opacity 120ms ease;
    }

    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.5; cursor: wait; transform: none; }

    .hero-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }

    .status {
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(29, 43, 42, 0.06);
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
    }

    .status.loading { background: rgba(180, 83, 9, 0.12); color: var(--warn); }
    .status.good { background: rgba(22, 101, 52, 0.12); color: var(--good); }
    .status.bad { background: rgba(185, 28, 28, 0.12); color: var(--bad); }

    .layout {
      margin-top: 18px;
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) minmax(320px, 0.95fr);
      gap: 18px;
      align-items: start;
    }

    .stack {
      display: grid;
      gap: 18px;
    }

    .panel {
      padding: 18px;
    }

    .panel h2, .panel h3 {
      margin: 0 0 12px;
      font-size: 18px;
      letter-spacing: -0.03em;
    }

    .section-title {
      margin: 2px 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .report-card {
      display: grid;
      gap: 14px;
      border-top: 1px solid var(--line);
      padding-top: 16px;
      margin-top: 16px;
    }

    .report-card:first-child {
      border-top: none;
      padding-top: 0;
      margin-top: 0;
    }

    .report-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      flex-wrap: wrap;
    }

    .report-title {
      margin: 0;
      font-size: 24px;
      letter-spacing: -0.04em;
    }

    .report-meta {
      color: var(--muted);
      font-size: 13px;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
    }

    .trend-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }

    .metric {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.66);
    }

    .metric .label {
      display: block;
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }

    .metric .value {
      font-size: 20px;
      font-weight: 700;
      letter-spacing: -0.04em;
    }

    .sparkline {
      width: 100%;
      height: 52px;
      display: block;
    }

    .chart-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }

    .pill-list {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }

    .mini-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(29, 43, 42, 0.06);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }

    .mini-pill.good {
      background: rgba(22, 101, 52, 0.12);
      color: var(--good);
    }

    .mini-pill.bad {
      background: rgba(185, 28, 28, 0.12);
      color: var(--bad);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.62);
    }

    th, td {
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }

    th {
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
      position: sticky;
      top: 0;
      background: rgba(255, 253, 250, 0.96);
      backdrop-filter: blur(6px);
      z-index: 1;
    }

    tbody tr:nth-child(odd) {
      background: rgba(255, 255, 255, 0.38);
    }

    tbody tr:hover {
      background: rgba(15, 118, 110, 0.08);
    }

    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 16px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.55);
    }

    .chip-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }

    .chip {
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.72);
      color: var(--ink);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 12px;
      cursor: pointer;
    }

    .chip.active {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }

    .mono {
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
    }

    .history-meta {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 14px;
    }

    .action-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }

    .action-buttons {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }

    .btn-secondary {
      background: rgba(15, 118, 110, 0.12);
      color: var(--accent);
      border: 1px solid rgba(15, 118, 110, 0.22);
    }

    .btn-danger {
      background: linear-gradient(135deg, #991b1b 0%, #b91c1c 100%);
      color: white;
    }

    .inline-button {
      width: auto;
      min-width: 88px;
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 12px;
    }

    .inline-actions {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }

    .tiny-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.6;
      margin-top: 12px;
    }

    .result-box {
      margin-top: 14px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.7);
      max-height: 240px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
    }

    @media (max-width: 1120px) {
      .layout { grid-template-columns: 1fr; }
      .controls { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .overview-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .action-grid { grid-template-columns: 1fr 1fr; }
    }

    @media (max-width: 720px) {
      .shell { width: min(100vw - 20px, 1380px); margin-top: 14px; }
      .hero, .panel { border-radius: 18px; }
      .controls { grid-template-columns: 1fr; }
      .overview-grid { grid-template-columns: 1fr; }
      .action-grid { grid-template-columns: 1fr; }
      .hero-top { align-items: stretch; }
      .hero-actions { width: 100%; }
      .table-wrap table { min-width: 640px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="hero-top">
        <div>
          <div class="eyebrow">실시간 OMS + DB 뷰어</div>
          <h1>펀딩 운영 관제</h1>
          <p class="subcopy">왼쪽에서는 현재 잔고, 포지션, 미체결, 최근 체결, 펀딩, 헬스 상태를 실시간으로 확인하고, 오른쪽에서는 DB에 저장된 잔고/포지션 스냅샷, 체결 이력, 펀딩 이력, 주문 스냅샷, 주문 이벤트를 함께 볼 수 있습니다.</p>
        </div>
        <div class="hero-actions">
          <button id="refreshBtn" type="button">지금 새로고침</button>
          <div class="status" id="statusPill">대기 중</div>
        </div>
      </div>

      <div class="controls">
        <div class="field">
          <label for="exchangesInput">거래소</label>
          <input id="exchangesInput" placeholder="okx,binance,gateio" />
        </div>
        <div class="field">
          <label for="symbolsInput">심볼</label>
          <input id="symbolsInput" placeholder="BTC,ETH" />
        </div>
        <div class="field">
          <label for="waitInput">WS 대기(초)</label>
          <input id="waitInput" type="number" min="1" max="30" step="1" />
        </div>
        <div class="field">
          <label for="tradeLimitInput">최근 체결 개수</label>
          <input id="tradeLimitInput" type="number" min="1" max="100" step="1" />
        </div>
        <div class="field">
          <label for="autoRefreshSelect">자동 새로고침</label>
          <select id="autoRefreshSelect">
            <option value="0">끔</option>
            <option value="15">15초</option>
            <option value="30" selected>30초</option>
            <option value="60">60초</option>
          </select>
        </div>
      </div>

      <div id="overviewRoot" class="overview-grid"></div>
    </section>

    <section class="layout">
      <div class="stack">
        <section class="panel">
          <h2>실시간 상태</h2>
          <div id="liveRoot" class="empty">아직 불러온 데이터가 없습니다.</div>
        </section>
      </div>

      <div class="stack">
        <section class="panel">
          <h2>조회 대상</h2>
          <div class="chip-row" id="exchangeChips"></div>
          <div class="chip-row" id="symbolChips"></div>
          <div class="history-meta" id="historyMeta">거래소와 심볼을 선택하면 DB 이력을 불러옵니다.</div>
        </section>

        <section class="panel">
          <h3>장시간 안정화 상태</h3>
          <div id="soakRoot" class="empty">아직 soak 로그가 없습니다.</div>
        </section>

        <section class="panel">
          <h2>주문 / 취소</h2>
          <div class="history-meta" id="actionMeta">위에서 거래소와 심볼을 먼저 선택하세요.</div>
          <div class="action-grid">
            <div class="field">
              <label for="orderSideSelect">방향</label>
              <select id="orderSideSelect">
                <option value="buy">매수</option>
                <option value="sell">매도</option>
              </select>
            </div>
            <div class="field">
              <label for="actionWaitInput">정합 대기(초)</label>
              <input id="actionWaitInput" type="number" min="0" max="30" step="0.1" />
            </div>
            <div class="field">
              <label for="actionTokenInput">액션 토큰</label>
              <input id="actionTokenInput" type="password" placeholder="선택: X-OPS-TOKEN" autocomplete="off" />
            </div>
            <div class="field">
              <label for="orderAmountInput">수량</label>
              <input id="orderAmountInput" type="number" min="0.000001" step="0.000001" placeholder="0.001" />
            </div>
            <div class="field">
              <label for="orderPriceInput">지정가</label>
              <input id="orderPriceInput" type="number" min="0.0001" step="0.0001" placeholder="1700" />
            </div>
          </div>
          <div class="action-buttons">
            <button id="placeOrderBtn" type="button">지정가 주문</button>
            <button id="cancelAllBtn" type="button" class="btn-danger">이 심볼 전체 취소</button>
            <div class="status" id="actionPill">웹 주문 비활성화</div>
          </div>
          <div class="tiny-note" id="actionHint">ENABLE_WEB_TRADING=1 설정 후 웹 서비스를 재시작하면 브라우저에서 주문/취소가 가능합니다.</div>
          <div id="actionResultRoot" class="result-box">아직 실행한 액션이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>주문 추적</h3>
          <div class="history-meta" id="traceMeta">주문 스냅샷, 주문 이벤트, 최근 체결 표에서 '추적'을 누르거나 주문 ID를 직접 넣으면 관련 흐름을 묶어서 보여줍니다.</div>
          <div class="action-grid">
            <div class="field">
              <label for="traceOrderInput">주문 식별자</label>
              <input id="traceOrderInput" placeholder="order_uid / order_id / client_order_id / uuid" />
            </div>
            <div class="field">
              <label for="traceWindowInput">윈도우(초)</label>
              <input id="traceWindowInput" type="number" min="30" max="3600" step="30" />
            </div>
          </div>
          <div class="action-buttons">
            <button id="traceLoadBtn" type="button" class="btn-secondary">주문 추적 불러오기</button>
            <button id="traceClearBtn" type="button" class="btn-secondary">선택 해제</button>
            <div class="status" id="tracePill">주문 선택 필요</div>
          </div>
          <div class="tiny-note">조회 대상은 현재 선택된 거래소/심볼 기준입니다. 주문 UID, 거래소 order_id, client_order_id, uuid 중 하나로 찾습니다.</div>
          <div id="traceResultRoot" class="empty">아직 선택한 주문이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>잔고 스냅샷</h3>
          <div id="balanceHistoryRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>포지션 스냅샷</h3>
          <div id="positionHistoryRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>체결 이력</h3>
          <div id="tradeHistoryRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>펀딩 이력</h3>
          <div id="fundingHistoryRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>주문 스냅샷</h3>
          <div id="orderHistoryRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>

        <section class="panel">
          <h3>주문 이벤트</h3>
          <div id="orderEventsRoot" class="empty">아직 DB 이력이 없습니다.</div>
        </section>
      </div>
    </section>
  </main>

  <script>
    const ACTION_TOKEN_STORAGE_KEY = "opsActionToken";
    const state = {
      loading: false,
      timer: null,
      selectedExchange: "",
      selectedSymbol: "",
      traceOrderRef: "",
      reports: [],
      soakStatus: null,
      config: null,
    };

    const el = {
      refreshBtn: document.getElementById("refreshBtn"),
      statusPill: document.getElementById("statusPill"),
      exchangesInput: document.getElementById("exchangesInput"),
      symbolsInput: document.getElementById("symbolsInput"),
      waitInput: document.getElementById("waitInput"),
      tradeLimitInput: document.getElementById("tradeLimitInput"),
      autoRefreshSelect: document.getElementById("autoRefreshSelect"),
      overviewRoot: document.getElementById("overviewRoot"),
      liveRoot: document.getElementById("liveRoot"),
      exchangeChips: document.getElementById("exchangeChips"),
      symbolChips: document.getElementById("symbolChips"),
      historyMeta: document.getElementById("historyMeta"),
      soakRoot: document.getElementById("soakRoot"),
      balanceHistoryRoot: document.getElementById("balanceHistoryRoot"),
      positionHistoryRoot: document.getElementById("positionHistoryRoot"),
      tradeHistoryRoot: document.getElementById("tradeHistoryRoot"),
      fundingHistoryRoot: document.getElementById("fundingHistoryRoot"),
      orderHistoryRoot: document.getElementById("orderHistoryRoot"),
      orderEventsRoot: document.getElementById("orderEventsRoot"),
      actionMeta: document.getElementById("actionMeta"),
      orderSideSelect: document.getElementById("orderSideSelect"),
      actionWaitInput: document.getElementById("actionWaitInput"),
      actionTokenInput: document.getElementById("actionTokenInput"),
      orderAmountInput: document.getElementById("orderAmountInput"),
      orderPriceInput: document.getElementById("orderPriceInput"),
      placeOrderBtn: document.getElementById("placeOrderBtn"),
      cancelAllBtn: document.getElementById("cancelAllBtn"),
      actionPill: document.getElementById("actionPill"),
      actionHint: document.getElementById("actionHint"),
      actionResultRoot: document.getElementById("actionResultRoot"),
      traceMeta: document.getElementById("traceMeta"),
      traceOrderInput: document.getElementById("traceOrderInput"),
      traceWindowInput: document.getElementById("traceWindowInput"),
      traceLoadBtn: document.getElementById("traceLoadBtn"),
      traceClearBtn: document.getElementById("traceClearBtn"),
      tracePill: document.getElementById("tracePill"),
      traceResultRoot: document.getElementById("traceResultRoot"),
    };

    function fmt(value, decimals = 4) {
      if (value === null || value === undefined || value === "") return "-";
      const num = Number(value);
      if (Number.isFinite(num)) {
        return num.toLocaleString("ko-KR", { maximumFractionDigits: decimals, minimumFractionDigits: decimals });
      }
      return String(value);
    }

    function fmtTs(value) {
      if (!value) return "-";
      let dateValue = null;
      if (typeof value === "string") {
        const parsed = new Date(value);
        if (!Number.isNaN(parsed.getTime())) {
          dateValue = parsed;
        }
      }
      if (!dateValue) {
        const num = Number(value);
        if (Number.isFinite(num) && num > 0) {
          dateValue = new Date(num);
        }
      }
      if (!dateValue || Number.isNaN(dateValue.getTime())) {
        return String(value);
      }
      return dateValue.toLocaleString("ko-KR", {
        timeZone: "Asia/Seoul",
        hour12: false,
      }) + " KST";
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function setStatus(text, mode = "") {
      el.statusPill.textContent = text;
      el.statusPill.className = "status" + (mode ? ` ${mode}` : "");
    }

    function setActionStatus(text, mode = "") {
      el.actionPill.textContent = text;
      el.actionPill.className = "status" + (mode ? ` ${mode}` : "");
    }

    function tradingEnabled() {
      return Boolean(state.config && state.config.web_trading_enabled);
    }

    function selectedTargetReady() {
      return Boolean(state.selectedExchange && state.selectedSymbol);
    }

    function stringifyPretty(value) {
      return JSON.stringify(value, null, 2);
    }

    function sectionBlock(title, innerHtml) {
      return `<div><div class="section-title">${escapeHtml(title)}</div>${innerHtml}</div>`;
    }

    function numeric(value, fallback = 0) {
      const num = Number(value);
      return Number.isFinite(num) ? num : fallback;
    }

    function sumBy(rows, getter) {
      return (rows || []).reduce((acc, row) => acc + numeric(getter(row), 0), 0);
    }

    function sparklineSvg(points, color = "#0f766e") {
      const values = (points || []).map((value) => numeric(value, NaN)).filter((value) => Number.isFinite(value));
      if (values.length < 2) {
        return `<svg class="sparkline" viewBox="0 0 160 52" preserveAspectRatio="none"><line x1="0" y1="40" x2="160" y2="40" stroke="${color}" stroke-opacity="0.18" stroke-width="2" stroke-dasharray="4 4" /></svg>`;
      }
      const width = 160;
      const height = 52;
      const min = Math.min(...values);
      const max = Math.max(...values);
      const spread = Math.max(max - min, 1e-9);
      const coords = values.map((value, index) => {
        const x = (index / Math.max(values.length - 1, 1)) * width;
        const y = height - ((value - min) / spread) * (height - 8) - 4;
        return [x, y];
      });
      const line = coords.map(([x, y]) => `${x.toFixed(2)},${y.toFixed(2)}`).join(" ");
      const area = [`0,${height}`, ...coords.map(([x, y]) => `${x.toFixed(2)},${y.toFixed(2)}`), `${width},${height}`].join(" ");
      return `
        <svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
          <polyline points="${area}" fill="${color}" fill-opacity="0.10" stroke="none"></polyline>
          <polyline points="${line}" fill="none" stroke="${color}" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"></polyline>
        </svg>
      `;
    }

    function trendCardHtml(title, value, meta, points, color = "#0f766e") {
      return `
        <div class="trend-card">
          <div class="label">${escapeHtml(title)}</div>
          <div class="value">${escapeHtml(value)}</div>
          ${sparklineSvg(points, color)}
          <div class="meta">${escapeHtml(meta)}</div>
        </div>
      `;
    }

    function summarizeReports() {
      const reports = state.reports || [];
      const totalEquity = sumBy(reports, (row) => row.balance_report?.actual_equity_usd ?? row.current_balance?.total_balance);
      const totalOpenOrders = sumBy(reports, (row) => Object.values(row.open_orders || {}).reduce((acc, items) => acc + (items || []).length, 0));
      const healthRows = reports.flatMap((row) => Object.entries(row.health_snapshot || {}).map(([symbol, health]) => ({ symbol, ...health })));
      const canTradeCount = healthRows.filter((row) => row.can_trade).length;
      const blockedCount = healthRows.filter((row) => row.can_trade === false).length;
      const symbolCount = reports.reduce((acc, row) => acc + ((row.symbols || []).length), 0);
      return {
        reportCount: reports.length,
        symbolCount,
        totalEquity,
        totalOpenOrders,
        canTradeCount,
        blockedCount,
      };
    }

    function renderOverview() {
      const summary = summarizeReports();
      const soak = state.soakStatus?.latest_summary || [];
      const soakFailureCount = soak.reduce((acc, row) => acc + numeric(row.failure_count, 0), 0);
      const soakExchangeCount = soak.length;
      const updatedAt = state.soakStatus?.updated_at ? fmtTs(state.soakStatus.updated_at) : "없음";
      el.overviewRoot.innerHTML = [
        `<div class="overview-card"><div class="label">실시간 거래소</div><div class="value">${summary.reportCount}</div><div class="meta">현재 live report 기준</div></div>`,
        `<div class="overview-card"><div class="label">관리 심볼</div><div class="value">${summary.symbolCount}</div><div class="meta">선택된 거래소 범위 합계</div></div>`,
        `<div class="overview-card"><div class="label">합산 실제 에쿼티</div><div class="value">${fmt(summary.totalEquity, 4)}</div><div class="meta">현재 live balance_report 기준</div></div>`,
        `<div class="overview-card"><div class="label">주문 가능 / 차단</div><div class="value">${summary.canTradeCount} / ${summary.blockedCount}</div><div class="meta">헬스 게이트 기준</div></div>`,
        `<div class="overview-card"><div class="label">현재 미체결</div><div class="value">${summary.totalOpenOrders}</div><div class="meta">전 거래소 합산</div></div>`,
        `<div class="overview-card"><div class="label">soak 검증 거래소</div><div class="value">${soakExchangeCount}</div><div class="meta">최근 soak summary 기준</div></div>`,
        `<div class="overview-card"><div class="label">soak 실패 수</div><div class="value">${soakFailureCount}</div><div class="meta">0이면 최근 루프 정상</div></div>`,
        `<div class="overview-card"><div class="label">soak 로그 갱신</div><div class="value">${escapeHtml(updatedAt)}</div><div class="meta">${escapeHtml(state.soakStatus?.path || "로그 경로 없음")}</div></div>`,
      ].join("");
    }

    function renderSoakStatus() {
      const soak = state.soakStatus;
      if (!soak || !soak.available) {
        el.soakRoot.innerHTML = '<div class="empty">아직 soak 로그가 없거나 경로가 설정되지 않았습니다.</div>';
        return;
      }

      const latestSummary = soak.latest_summary || [];
      const trendCards = latestSummary.length ? latestSummary.map((row) => {
        const label = String(row.exchange || "").toUpperCase();
        const value = `fail=${numeric(row.failure_count, 0)}`;
        const meta = `cycles=${numeric(row.cycles_ran, 0)} restart=${numeric(row.restart_count, 0)}`;
        const color = numeric(row.failure_count, 0) > 0 ? "#b91c1c" : "#166534";
        return trendCardHtml(label, value, meta, [0, numeric(row.cycles_ran, 0), numeric(row.restart_count, 0), numeric(row.failure_count, 0)], color);
      }).join("") : "";

      const recentCycles = soak.recent_cycles || [];
      const recentIssues = soak.recent_issues || [];
      const pills = latestSummary.map((row) => {
        const ok = numeric(row.failure_count, 0) === 0;
        return `<span class="mini-pill ${ok ? "good" : "bad"}">${escapeHtml(String(row.exchange || "").toUpperCase())} ${ok ? "정상" : "이슈"}</span>`;
      }).join("");

      el.soakRoot.innerHTML = `
        <div class="pill-list">${pills || '<span class="mini-pill">요약 없음</span>'}</div>
        ${trendCards ? `<div class="trend-grid" style="margin-top:12px;">${trendCards}</div>` : ""}
        <div class="chart-note">로그: ${escapeHtml(soak.path || "-")}<br />마지막 갱신: ${escapeHtml(fmtTs(soak.updated_at))} / 읽은 row: ${escapeHtml(String(soak.row_count || 0))}</div>
        ${sectionBlock("최근 soak 사이클", tableOrEmpty([
          { label: "거래소", render: (row) => String(row.exchange || "").toUpperCase() },
          { label: "phase", render: (row) => row.phase || "-" },
          { label: "cycle", render: (row) => row.cycle ?? "-" },
          { label: "에러", render: (row) => (row.errors || []).join(", ") || "-" },
        ], recentCycles))}
        ${sectionBlock("최근 이슈", tableOrEmpty([
          { label: "거래소", render: (row) => String(row.exchange || "").toUpperCase() },
          { label: "phase", render: (row) => row.phase || "-" },
          { label: "cycle", render: (row) => row.cycle ?? "-" },
          { label: "에러", render: (row) => (row.errors || []).join(", ") || "-" },
        ], recentIssues))}
      `;
    }

    function currentParams() {
      return {
        exchanges: el.exchangesInput.value.trim(),
        symbols: el.symbolsInput.value.trim(),
        wait_ws_sec: el.waitInput.value.trim(),
        trade_limit: el.tradeLimitInput.value.trim(),
      };
    }

    function tableOrEmpty(columns, rows) {
      if (!rows || !rows.length) {
        return '<div class="empty">표시할 항목이 없습니다.</div>';
      }
      const header = "<tr>" + columns.map((col) => `<th>${escapeHtml(col.label)}</th>`).join("") + "</tr>";
      const body = rows.map((row) => {
        const cells = columns.map((col) => {
          const value = col.render(row);
          const rendered = col.html ? String(value) : escapeHtml(value);
          return `<td class="${col.className || ""}">${rendered}</td>`;
        }).join("");
        return `<tr>${cells}</tr>`;
      }).join("");
      return `<div class="table-wrap"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>`;
    }

    function renderMetrics(report) {
      const balance = report.balance_report || {};
      const account = report.current_balance || {};
      const assetDict = account.asset_dict || {};
      const usdAsset = assetDict.USD || {};
      const usdtAsset = assetDict.USDT || {};
      const assetCode = balance.balance_asset_code || "USD";
      const wallet = balance.wallet_balance_usd ?? usdAsset.balance ?? usdtAsset.balance ?? account.total_balance;
      const available = balance.available_balance_usd ?? usdAsset.availableBalance ?? usdtAsset.availableBalance ?? wallet;
      const locked = balance.locked_balance_usd ?? usdAsset.locked ?? usdtAsset.locked ?? 0;
      const margin = balance.margin_balance_usd ?? usdAsset.marginBalance ?? usdtAsset.marginBalance ?? wallet;
      const exchangeUpl = balance.exchange_unrealized_profit_usd ?? usdAsset.unrealizedProfit ?? usdtAsset.unrealizedProfit ?? 0;
      const theoreticalUpl = balance.theoretical_unrealized_profit_usd ?? exchangeUpl;
      const actualEquity = balance.actual_equity_usd ?? account.total_balance ?? wallet + exchangeUpl;
      const theoreticalEquity = balance.theoretical_equity_usd ?? actualEquity;
      return `
        <div class="metrics">
          <div class="metric"><span class="label">총 잔고</span><span class="value">${fmt(balance.total_balance_usd ?? account.total_balance, 6)}</span></div>
          <div class="metric"><span class="label">지갑 잔고 (${escapeHtml(assetCode)})</span><span class="value">${fmt(wallet, 6)}</span></div>
          <div class="metric"><span class="label">사용 가능</span><span class="value">${fmt(available, 6)}</span></div>
          <div class="metric"><span class="label">잠김 금액</span><span class="value">${fmt(locked, 6)}</span></div>
          <div class="metric"><span class="label">증거금</span><span class="value">${fmt(margin, 6)}</span></div>
          <div class="metric"><span class="label">거래소 미실현손익</span><span class="value">${fmt(exchangeUpl, 6)}</span></div>
          <div class="metric"><span class="label">이론 미실현손익</span><span class="value">${fmt(theoreticalUpl, 6)}</span></div>
          <div class="metric"><span class="label">이론 에쿼티</span><span class="value">${fmt(theoreticalEquity, 6)}</span></div>
          <div class="metric"><span class="label">실제 에쿼티</span><span class="value">${fmt(actualEquity, 6)}</span></div>
          <div class="metric"><span class="label">에쿼티 차이</span><span class="value">${fmt((balance.theoretical_equity_gap_usd ?? (theoreticalEquity - actualEquity)), 6)}</span></div>
          <div class="metric"><span class="label">총 노셔널</span><span class="value">${fmt(balance.gross_notional_usd, 6)}</span></div>
          <div class="metric"><span class="label">순 노셔널</span><span class="value">${fmt(balance.net_notional_usd, 6)}</span></div>
        </div>
      `;
    }

    function orderUid(row) {
      return row.uuid || row.order_id || row.client_order_id || "-";
    }

    function traceOrderRef(row) {
      return row.order_uid || row.order_id || row.client_order_id || row.uuid || "-";
    }

    function setTraceStatus(text, mode = "") {
      el.tracePill.textContent = text;
      el.tracePill.className = "status" + (mode ? ` ${mode}` : "");
    }

    function selectedTraceRef() {
      return String(el.traceOrderInput.value || state.traceOrderRef || "").trim();
    }

    function traceButtonHtml(exchange, symbol, row) {
      const orderRef = traceOrderRef(row);
      if (!orderRef || orderRef === "-") {
        return '<button type="button" class="inline-button btn-secondary" disabled>추적 불가</button>';
      }
      const payload = [
        JSON.stringify(exchange),
        JSON.stringify(symbol || row.symbol || state.selectedSymbol || ""),
        JSON.stringify(orderRef),
      ].join(",");
      return `<button type="button" class="inline-button btn-secondary" onclick='openOrderTrace(${payload})'>추적</button>`;
    }

    function liveActionButtonsHtml(exchange, row) {
      return `<div class="inline-actions">${traceButtonHtml(exchange, row.symbol, row)}${cancelButtonHtml(exchange, row)}</div>`;
    }

    function cancelButtonHtml(exchange, row) {
      if (!tradingEnabled()) {
        return '<button type="button" class="inline-button btn-secondary" disabled>비활성화</button>';
      }
      const uid = orderUid(row);
      if (!uid || uid === "-") {
        return '<button type="button" class="inline-button btn-secondary" disabled>ID 없음</button>';
      }
      const payload = [
        JSON.stringify(exchange),
        JSON.stringify(row.symbol),
        JSON.stringify(uid),
      ].join(",");
      return `<button type="button" class="inline-button btn-danger" onclick='cancelLiveOrder(${payload})'>취소</button>`;
    }

    function renderActionPanel() {
      const targetText = selectedTargetReady()
        ? `선택 대상: ${state.selectedExchange.toUpperCase()} / ${state.selectedSymbol}`
        : "위 칩에서 거래소와 심볼을 선택하세요.";
      el.actionMeta.textContent = targetText;

      if (state.config) {
        el.actionWaitInput.value = state.config.default_action_wait_ws_sec;
      }

      const enabled = tradingEnabled() && selectedTargetReady();
      el.placeOrderBtn.disabled = !enabled;
      el.cancelAllBtn.disabled = !enabled;

      if (tradingEnabled()) {
        setActionStatus(selectedTargetReady() ? "준비 완료" : "대상 선택 필요", selectedTargetReady() ? "good" : "");
        if (state.config?.web_action_token_required) {
          el.actionHint.textContent = "resident runtime으로 바로 주문을 보냅니다. 액션 토큰이 필요하면 입력하고, 반드시 작은 테스트 주문으로 시작하세요.";
        } else {
          el.actionHint.textContent = "resident runtime으로 바로 주문을 보냅니다. 반드시 작은 테스트 주문으로 시작하고, 아래 주문 이벤트에서 결과를 확인하세요.";
        }
      } else {
        setActionStatus("웹 주문 비활성화");
        el.actionHint.textContent = "ENABLE_WEB_TRADING=1 설정 후 웹 서비스를 재시작하면 브라우저에서 주문/취소가 가능합니다.";
      }
    }

    function renderLiveReport(report) {
      if (report.error) {
        return `
          <article class="report-card">
            <div class="report-head">
              <div>
                <h3 class="report-title">${escapeHtml(String(report.exchange || "").toUpperCase())}</h3>
                <div class="report-meta">${escapeHtml((report.symbols || []).join(","))}</div>
              </div>
            </div>
            <div class="empty">실시간 조회 실패: ${escapeHtml(report.error)}</div>
          </article>
        `;
      }

      const balanceReport = report.balance_report || {};
      const healthOverview = report.health_overview || {};
      const rateLimitState = healthOverview.rate_limit_state || {};
      const rateLimitRows = (rateLimitState.last || []).map((row) => ({
        interval: row.interval,
        intervalNum: row.intervalNum,
        count: row.count,
        limit: row.limit,
      }));
      const balancePositions = (balanceReport.positions || []).map((row) => ({
        symbol: row.ticker || row.symbol,
        ...row,
      }));
      const reconcileStatus = report.reconcile_status || {};
      const legacyPositions = Object.entries(report.current_positions || {}).map(([symbol, row]) => ({ symbol, ...row }));
      const positionRows = balancePositions.length ? balancePositions : legacyPositions;
      const openOrders = Object.entries(report.open_orders || {}).flatMap(([symbol, rows]) => (rows || []).map((row) => ({ symbol, ...row })));
      const recentTrades = Object.entries(report.recent_trades || {}).flatMap(([symbol, rows]) => (rows || []).map((row) => ({ symbol, ...row })));
      const fundingRows = Object.entries(report.funding_snapshot || {}).map(([symbol, row]) => ({ symbol, ...row }));
      const healthRows = Object.entries(report.health_snapshot || {}).map(([symbol, row]) => ({ symbol, ...row }));

      return `
        <article class="report-card">
          <div class="report-head">
            <div>
              <h3 class="report-title">${escapeHtml(String(report.exchange || "").toUpperCase())}</h3>
              <div class="report-meta">심볼=${escapeHtml((report.symbols || []).join(","))} 갱신=${escapeHtml(fmtTs(report.generated_at || "-"))}</div>
            </div>
          </div>

          ${renderMetrics(report)}

          ${sectionBlock("포지션", tableOrEmpty([
            { label: "심볼", render: (row) => row.symbol },
            { label: "수량", render: (row) => fmt(row.qty, 6) },
            { label: "진입가", render: (row) => fmt(row.entry_price, 4) },
            { label: "평가가", render: (row) => fmt(row.mark_price, 4) },
            { label: "노셔널", render: (row) => fmt(row.notional_mark_usd, 6) },
            { label: "거래소 손익", render: (row) => fmt(row.exchange_unrealized_profit_usd ?? row.unrealized_profit, 6) },
            { label: "이론 손익", render: (row) => fmt(row.theoretical_unrealized_profit_usd ?? row.unrealized_profit, 6) },
            { label: "차이", render: (row) => fmt(row.unrealized_profit_gap_usd, 6) },
            { label: "평가 기준", render: (row) => row.mark_source || "-" },
            { label: "포지션 방향", render: (row) => row.position_side || "-" },
          ], positionRows))}

          ${sectionBlock("익스포저 분해", tableOrEmpty([
            { label: "롱 노셔널", render: (row) => fmt(row.long_notional_usd, 6) },
            { label: "숏 노셔널", render: (row) => fmt(row.short_notional_usd, 6) },
            { label: "총 노셔널", render: (row) => fmt(row.gross_notional_usd, 6) },
            { label: "순 노셔널", render: (row) => fmt(row.net_notional_usd, 6) },
            { label: "레버리지", render: (row) => fmt(row.leverage, 3) },
            { label: "델타 레버리지", render: (row) => fmt(row.delta_leverage, 3) },
          ], balanceReport && Object.keys(balanceReport).length ? [balanceReport] : []))}

          ${sectionBlock("호출량 / 차단 상태", tableOrEmpty([
            { label: "차단 여부", render: (row) => row.blocked ? "차단" : "정상" },
            { label: "사유", render: (row) => row.reason || "-" },
            { label: "구간", render: (row) => row.interval || "-" },
            { label: "배수", render: (row) => row.intervalNum ?? "-" },
            { label: "사용량", render: (row) => row.count ?? "-" },
            { label: "한도", render: (row) => row.limit ?? "-" },
          ], rateLimitRows.length ? rateLimitRows.map((row) => ({
            ...row,
            blocked: Boolean(rateLimitState.blocked),
            reason: rateLimitState.reason,
          })) : [{
            blocked: Boolean(rateLimitState.blocked),
            reason: rateLimitState.reason || "-",
            interval: "-",
            intervalNum: "-",
            count: "-",
            limit: "-",
          }]))}

          ${sectionBlock("정합성 가드", tableOrEmpty([
            { label: "최근 reconcile 사유", render: (row) => row.last_reason || "-" },
            { label: "자동 reconcile 사유", render: (row) => row.last_auto_reason || "-" },
            { label: "최근 guard 사유", render: (row) => row.guard_last_reason || "-" },
            { label: "최근 guard 점검", render: (row) => fmtTs(row.guard_last_check_ms) },
            { label: "최근 guard 이슈", render: (row) => fmtTs(row.guard_last_issue_ms) },
            { label: "guard 누적", render: (row) => row.guard_issue_count ?? 0 },
            { label: "주기 reconcile 요청", render: (row) => fmtTs(row.last_periodic_request_ms) },
          ], Object.keys(reconcileStatus).length ? [reconcileStatus] : []))}

          ${sectionBlock("미체결 주문", tableOrEmpty([
            { label: "심볼", render: (row) => row.symbol },
            { label: "주문 ID", render: (row) => orderUid(row) },
            { label: "방향", render: (row) => row.side || "-" },
            { label: "수량", render: (row) => fmt(row.qty, 6) },
            { label: "가격", render: (row) => fmt(row.price, 4) },
            { label: "상태", render: (row) => row.status || "-" },
            { label: "동작", render: (row) => liveActionButtonsHtml(report.exchange, row), html: true },
          ], openOrders))}

          ${sectionBlock("최근 체결", tableOrEmpty([
            { label: "심볼", render: (row) => row.symbol },
            { label: "시간", render: (row) => fmtTs(row.create_time || row.timestamp) },
            { label: "방향", render: (row) => row.side || "-" },
            { label: "수량", render: (row) => row.qty ?? row.qty_contract ?? "-" },
            { label: "가격", render: (row) => fmt(row.price, 4) },
            { label: "체결 타입", render: (row) => row.role || "-" },
            { label: "추적", render: (row) => traceButtonHtml(report.exchange, row.symbol, row), html: true },
          ], recentTrades))}

          ${sectionBlock("펀딩 스냅샷", tableOrEmpty([
            { label: "심볼", render: (row) => row.symbol },
            { label: "펀딩비율", render: (row) => fmt(row.funding_rate, 8) },
            { label: "다음 펀딩", render: (row) => fmtTs(row.next_funding_time) },
            { label: "마크가", render: (row) => fmt(row.mark_price, 4) },
            { label: "인덱스가", render: (row) => fmt(row.index_price, 4) },
          ], fundingRows))}

          ${sectionBlock("헬스 상태", tableOrEmpty([
            { label: "심볼", render: (row) => row.symbol },
            { label: "주문 가능", render: (row) => row.can_trade ? "가능" : "차단" },
            { label: "차단 사유", render: (row) => (row.block_reasons || []).join(", ") || "-" },
            { label: "WS 지연(ms)", render: (row) => fmt(row.ws_drift_last_ms, 1) },
            { label: "마지막 WS 시각", render: (row) => fmtTs(row.ws_last_update_ms) },
          ], healthRows))}
        </article>
      `;
    }

    function renderBalanceHistorySection(rows) {
      if (!rows || !rows.length) {
        return '<div class="empty">아직 DB 이력이 없습니다.</div>';
      }
      const ordered = [...rows].reverse();
      const actualEquityPoints = ordered.map((row) => numeric(row.actual_equity_usd, 0));
      const grossPoints = ordered.map((row) => numeric(row.gross_notional_usd, 0));
      const latest = rows[0];
      const oldest = rows[rows.length - 1];
      const equityDelta = numeric(latest.actual_equity_usd, 0) - numeric(oldest.actual_equity_usd, 0);
      return `
        <div class="trend-grid">
          ${trendCardHtml("실제 에쿼티 추세", fmt(latest.actual_equity_usd, 6), `시작 대비 ${fmt(equityDelta, 6)}`, actualEquityPoints, "#0f766e")}
          ${trendCardHtml("총 노셔널 추세", fmt(latest.gross_notional_usd, 6), `최근 포지션 수 ${fmt(latest.position_count, 0)}`, grossPoints, "#b45309")}
        </div>
        ${tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.snapshot_time_utc || row.snapshot_time_ms) },
          { label: "대상 심볼", render: (row) => row.tracked_symbols_csv || "-" },
          { label: "자산", render: (row) => row.balance_asset_code || "-" },
          { label: "실제 에쿼티", render: (row) => fmt(row.actual_equity_usd, 6) },
          { label: "이론 에쿼티", render: (row) => fmt(row.theoretical_equity_usd, 6) },
          { label: "차이", render: (row) => fmt(row.theoretical_equity_gap_usd, 6) },
          { label: "총 노셔널", render: (row) => fmt(row.gross_notional_usd, 6) },
          { label: "순 노셔널", render: (row) => fmt(row.net_notional_usd, 6) },
          { label: "포지션 수", render: (row) => fmt(row.position_count, 0) },
        ], rows)}
      `;
    }

    function renderPositionHistorySection(rows) {
      if (!rows || !rows.length) {
        return '<div class="empty">아직 DB 이력이 없습니다.</div>';
      }
      const ordered = [...rows].reverse();
      const qtyPoints = ordered.map((row) => numeric(row.qty, 0));
      const uplPoints = ordered.map((row) => numeric(row.exchange_unrealized_profit_usd, 0));
      const latest = rows[0];
      return `
        <div class="trend-grid">
          ${trendCardHtml("포지션 수량 추세", fmt(latest.qty, 6), `방향 ${latest.position_side || "-"}`, qtyPoints, "#0f766e")}
          ${trendCardHtml("거래소 UPL 추세", fmt(latest.exchange_unrealized_profit_usd, 6), `이론 UPL ${fmt(latest.theoretical_unrealized_profit_usd, 6)}`, uplPoints, "#1d4ed8")}
        </div>
        ${tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.snapshot_time_utc || row.snapshot_time_ms) },
          { label: "대상 심볼", render: (row) => row.symbol || "-" },
          { label: "방향", render: (row) => row.position_side || "-" },
          { label: "수량", render: (row) => fmt(row.qty, 6) },
          { label: "진입가", render: (row) => fmt(row.entry_price, 6) },
          { label: "마크가", render: (row) => fmt(row.mark_price, 6) },
          { label: "마크 소스", render: (row) => row.mark_source || "-" },
          { label: "노셔널", render: (row) => fmt(row.notional_mark_usd, 6) },
          { label: "거래소 UPL", render: (row) => fmt(row.exchange_unrealized_profit_usd, 6) },
          { label: "이론 UPL", render: (row) => fmt(row.theoretical_unrealized_profit_usd, 6) },
          { label: "차이", render: (row) => fmt(row.unrealized_profit_gap_usd, 6) },
        ], rows)}
      `;
    }

    function renderTradeHistorySection(rows) {
      if (!rows || !rows.length) {
        return '<div class="empty">아직 DB 이력이 없습니다.</div>';
      }
      const totalQty = sumBy(rows, (row) => Math.abs(numeric(row.qty, 0)));
      const totalFee = sumBy(rows, (row) => numeric(row.fee, 0));
      const sideMix = rows.reduce((acc, row) => {
        const side = String(row.side || "-").toUpperCase();
        acc[side] = (acc[side] || 0) + 1;
        return acc;
      }, {});
      return `
        <div class="trend-grid">
          ${trendCardHtml("최근 체결 수", fmt(rows.length, 0), `BUY ${sideMix.BUY || 0} / SELL ${sideMix.SELL || 0}`, rows.map((_, index) => index + 1), "#0f766e")}
          ${trendCardHtml("누적 체결 수량", fmt(totalQty, 6), `누적 수수료 ${fmt(totalFee, 8)}`, rows.map((row) => Math.abs(numeric(row.qty, 0))), "#b45309")}
        </div>
        ${tableOrEmpty([
          { label: "시간", render: (row) => fmtTs(row.created_at_utc || row.create_time_ms) },
          { label: "체결 ID", render: (row) => row.trade_id },
          { label: "방향", render: (row) => row.side || "-" },
          { label: "수량", render: (row) => fmt(row.qty, 6) },
          { label: "가격", render: (row) => fmt(row.price, 4) },
          { label: "수수료", render: (row) => fmt(row.fee, 8) },
          { label: "추적", render: (row) => traceButtonHtml(state.selectedExchange, state.selectedSymbol, row), html: true },
        ], rows)}
      `;
    }

    function renderOrderEventSection(rows) {
      if (!rows || !rows.length) {
        return '<div class="empty">아직 DB 이력이 없습니다.</div>';
      }
      const eventMix = rows.reduce((acc, row) => {
        const eventType = String(row.event_type || "-");
        acc[eventType] = (acc[eventType] || 0) + 1;
        return acc;
      }, {});
      const eventCountPoints = Object.values(eventMix).map((value) => numeric(value, 0));
      const topTypes = Object.entries(eventMix)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([name, count]) => `${name}:${count}`)
        .join(" / ");
      return `
        <div class="trend-grid">
          ${trendCardHtml("주문 이벤트 수", fmt(rows.length, 0), topTypes || "-", eventCountPoints, "#7c3aed")}
        </div>
        ${tableOrEmpty([
          { label: "이벤트 시각", render: (row) => fmtTs(row.event_time_utc || row.event_time_ms) },
          { label: "타입", render: (row) => row.event_type },
          { label: "출처", render: (row) => row.event_source },
          { label: "주문 UID", render: (row) => row.order_uid },
          { label: "상태", render: (row) => row.status || "-" },
          { label: "에러", render: (row) => row.error_message || "-" },
          { label: "추적", render: (row) => traceButtonHtml(state.selectedExchange, row.symbol, row), html: true },
        ], rows)}
      `;
    }

    function renderChipRow(root, values, selected, onClickName) {
      if (!values.length) {
        root.innerHTML = '<div class="empty">선택 가능한 항목이 없습니다.</div>';
        return;
      }
      root.innerHTML = values.map((value) => {
        const active = value === selected ? " active" : "";
        return `<button type="button" class="chip${active}" onclick="${onClickName}('${escapeHtml(value)}')">${escapeHtml(value)}</button>`;
      }).join("");
    }

    window.pickExchange = function (exchange) {
      state.selectedExchange = exchange;
      const symbols = symbolsForExchange(exchange);
      if (!symbols.includes(state.selectedSymbol)) {
        state.selectedSymbol = symbols[0] || "";
      }
      renderHistoryTarget();
      loadHistory();
    };

    window.pickSymbol = function (symbol) {
      state.selectedSymbol = symbol;
      renderHistoryTarget();
      loadHistory();
    };

    function symbolsForExchange(exchange) {
      const report = state.reports.find((item) => item.exchange === exchange);
      return report ? (report.symbols || []) : [];
    }

    function renderHistoryTarget() {
      const exchanges = state.reports.map((item) => item.exchange);
      renderChipRow(el.exchangeChips, exchanges, state.selectedExchange, "pickExchange");
      renderChipRow(el.symbolChips, symbolsForExchange(state.selectedExchange), state.selectedSymbol, "pickSymbol");
      if (state.selectedExchange && state.selectedSymbol) {
        el.historyMeta.textContent = `DB 조회 대상: ${state.selectedExchange.toUpperCase()} / ${state.selectedSymbol}`;
      } else {
        el.historyMeta.textContent = "거래소와 심볼을 선택하면 DB 이력을 불러옵니다.";
      }
      const traceRef = selectedTraceRef();
      if (state.selectedExchange && state.selectedSymbol && traceRef) {
        el.traceMeta.textContent = `주문 추적 대상: ${state.selectedExchange.toUpperCase()} / ${state.selectedSymbol} / ${traceRef}`;
      } else if (state.selectedExchange && state.selectedSymbol) {
        el.traceMeta.textContent = `현재 대상(${state.selectedExchange.toUpperCase()} / ${state.selectedSymbol}) 기준으로 주문 UID, order_id, client_order_id, uuid 중 하나를 넣거나 표에서 '추적'을 누르세요.`;
      } else {
        el.traceMeta.textContent = "주문 스냅샷, 주문 이벤트, 최근 체결 표에서 '추적'을 누르거나 주문 ID를 직접 넣으면 관련 흐름을 묶어서 보여줍니다.";
      }
      renderActionPanel();
    }

    function renderOrderTrace(trace) {
      const summary = trace.summary || {};
      const relatedIds = trace.related_identifiers || [];
      const window = trace.time_window || {};
      return `
        ${sectionBlock("주문 요약", `
          <div class="metrics">
            <div class="metric"><span class="label">조회 식별자</span><span class="value mono">${escapeHtml(trace.lookup_ref || "-")}</span></div>
            <div class="metric"><span class="label">해석된 주문 UID</span><span class="value mono">${escapeHtml(summary.resolved_order_uid || "-")}</span></div>
            <div class="metric"><span class="label">최신 상태</span><span class="value">${escapeHtml(summary.latest_status || "-")}</span></div>
            <div class="metric"><span class="label">최신 이벤트</span><span class="value">${escapeHtml(summary.latest_event_type || "-")}</span></div>
            <div class="metric"><span class="label">체결 수</span><span class="value">${fmt(summary.trade_count, 0)}</span></div>
            <div class="metric"><span class="label">오픈 여부</span><span class="value">${summary.is_open === null || summary.is_open === undefined ? "-" : (summary.is_open ? "열림" : "닫힘")}</span></div>
            <div class="metric"><span class="label">첫 시각</span><span class="value">${escapeHtml(fmtTs(summary.first_time_utc || summary.first_time_ms))}</span></div>
            <div class="metric"><span class="label">마지막 시각</span><span class="value">${escapeHtml(fmtTs(summary.last_time_utc || summary.last_time_ms))}</span></div>
          </div>
          <div class="tiny-note">연결된 식별자: ${escapeHtml(relatedIds.join(", ") || "-")}<br />윈도우: ${escapeHtml(fmtTs(window.start_utc || window.start_ms))} ~ ${escapeHtml(fmtTs(window.end_utc || window.end_ms))}</div>
        `)}
        ${sectionBlock("관련 주문 이벤트", tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.event_time_utc || row.event_time_ms) },
          { label: "타입", render: (row) => row.event_type || "-" },
          { label: "출처", render: (row) => row.event_source || "-" },
          { label: "상태", render: (row) => row.status || "-" },
          { label: "체결 수량", render: (row) => fmt(row.executed_qty, 6) },
          { label: "잔량", render: (row) => fmt(row.left_qty, 6) },
          { label: "평균가", render: (row) => fmt(row.avg_price, 6) },
          { label: "에러", render: (row) => row.error_message || "-" },
        ], trace.order_events || []))}
        ${sectionBlock("관련 주문 스냅샷", tableOrEmpty([
          { label: "마지막 확인", render: (row) => fmtTs(row.last_seen_at_utc || row.last_seen_ms) },
          { label: "주문 UID", render: (row) => row.order_uid || "-" },
          { label: "상태", render: (row) => row.status || "-" },
          { label: "열림 여부", render: (row) => row.is_open ? "열림" : "닫힘" },
          { label: "수량", render: (row) => fmt(row.qty, 6) },
          { label: "체결 수량", render: (row) => fmt(row.executed_qty, 6) },
          { label: "잔량", render: (row) => fmt(row.left_qty, 6) },
          { label: "평균가", render: (row) => fmt(row.avg_price, 6) },
        ], trace.order_history || []))}
        ${sectionBlock("관련 체결", tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.created_at_utc || row.create_time_ms) },
          { label: "체결 ID", render: (row) => row.trade_id || "-" },
          { label: "방향", render: (row) => row.side || "-" },
          { label: "역할", render: (row) => row.role || "-" },
          { label: "수량", render: (row) => fmt(row.qty, 6) },
          { label: "가격", render: (row) => fmt(row.price, 6) },
          { label: "수수료", render: (row) => fmt(row.fee, 8) },
        ], trace.trade_history || []))}
        ${sectionBlock("주변 포지션 변화", tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.snapshot_time_utc || row.snapshot_time_ms) },
          { label: "방향", render: (row) => row.position_side || "-" },
          { label: "수량", render: (row) => fmt(row.qty, 6) },
          { label: "진입가", render: (row) => fmt(row.entry_price, 6) },
          { label: "마크가", render: (row) => fmt(row.mark_price, 6) },
          { label: "노셔널", render: (row) => fmt(row.notional_mark_usd, 6) },
          { label: "거래소 UPL", render: (row) => fmt(row.exchange_unrealized_profit_usd, 6) },
          { label: "이론 UPL", render: (row) => fmt(row.theoretical_unrealized_profit_usd, 6) },
        ], trace.position_snapshots || []))}
        ${sectionBlock("주변 잔고 변화", tableOrEmpty([
          { label: "시각", render: (row) => fmtTs(row.snapshot_time_utc || row.snapshot_time_ms) },
          { label: "실제 에쿼티", render: (row) => fmt(row.actual_equity_usd, 6) },
          { label: "이론 에쿼티", render: (row) => fmt(row.theoretical_equity_usd, 6) },
          { label: "차이", render: (row) => fmt(row.theoretical_equity_gap_usd, 6) },
          { label: "총 노셔널", render: (row) => fmt(row.gross_notional_usd, 6) },
          { label: "순 노셔널", render: (row) => fmt(row.net_notional_usd, 6) },
          { label: "포지션 수", render: (row) => fmt(row.position_count, 0) },
        ], trace.balance_snapshots || []))}
      `;
    }

    async function loadOrderTrace() {
      const orderRef = selectedTraceRef();
      if (!state.selectedExchange || !state.selectedSymbol) {
        el.traceResultRoot.innerHTML = '<div class="empty">거래소와 심볼을 먼저 선택하세요.</div>';
        setTraceStatus("대상 선택 필요");
        return;
      }
      if (!orderRef) {
        el.traceResultRoot.innerHTML = '<div class="empty">아직 선택한 주문이 없습니다.</div>';
        setTraceStatus("주문 선택 필요");
        return;
      }

      setTraceStatus("주문 추적 불러오는 중", "loading");
      const params = new URLSearchParams({
        exchange: state.selectedExchange,
        symbol: state.selectedSymbol,
        order_ref: orderRef,
        window_sec: String(Number(el.traceWindowInput.value || 0) || (state.config?.default_db_trace_window_sec || 300)),
      });
      const response = await fetch(`/api/order_trace?${params.toString()}`);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `주문 추적 조회 실패: ${response.status}`);
      }
      const data = await response.json();
      state.traceOrderRef = orderRef;
      el.traceOrderInput.value = orderRef;
      el.traceResultRoot.innerHTML = renderOrderTrace(data);
      const summary = data.summary || {};
      const hasRelatedRows = (summary.event_count || 0) + (summary.order_snapshot_count || 0) + (summary.trade_count || 0) > 0;
      setTraceStatus(hasRelatedRows ? "주문 추적 갱신 완료" : "관련 데이터 없음", hasRelatedRows ? "good" : "");
    }

    function clearOrderTrace() {
      state.traceOrderRef = "";
      el.traceOrderInput.value = "";
      el.traceResultRoot.innerHTML = '<div class="empty">아직 선택한 주문이 없습니다.</div>';
      setTraceStatus("주문 선택 필요");
      renderHistoryTarget();
    }

    async function loadConfig() {
      const response = await fetch("/api/config");
      const data = await response.json();
      state.config = data;
      el.exchangesInput.value = data.default_exchanges;
      el.symbolsInput.value = data.default_symbols;
      el.waitInput.value = data.default_wait_ws_sec;
      el.tradeLimitInput.value = data.default_trade_limit;
      el.actionWaitInput.value = data.default_action_wait_ws_sec;
      el.actionTokenInput.value = window.localStorage.getItem(ACTION_TOKEN_STORAGE_KEY) || "";
      el.traceWindowInput.value = data.default_db_trace_window_sec;
      renderActionPanel();
      setTraceStatus("주문 선택 필요");
      renderOverview();
      renderSoakStatus();
    }

    async function loadSoakStatus() {
      const recentLimit = Number(state.config?.default_soak_recent_limit || 80);
      const response = await fetch(`/api/soak_status?limit=${recentLimit}`);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `soak 상태 조회 실패: ${response.status}`);
      }
      const data = await response.json();
      state.soakStatus = data.soak_status || null;
      renderSoakStatus();
      renderOverview();
      return data;
    }

    async function loadLive() {
      const params = new URLSearchParams(currentParams());
      const response = await fetch(`/api/live?${params.toString()}`);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `실시간 조회 실패: ${response.status}`);
      }
      const data = await response.json();
      state.reports = data.reports || [];
      if (!state.selectedExchange && state.reports.length) {
        state.selectedExchange = state.reports[0].exchange;
      }
      const allowedSymbols = symbolsForExchange(state.selectedExchange);
      if (!allowedSymbols.includes(state.selectedSymbol)) {
        state.selectedSymbol = allowedSymbols[0] || "";
      }
      renderHistoryTarget();
      el.liveRoot.innerHTML = state.reports.length
        ? state.reports.map(renderLiveReport).join("")
        : '<div class="empty">실시간 응답 데이터가 없습니다.</div>';
      renderOverview();
      return data;
    }

    async function loadHistory() {
      if (!state.selectedExchange || !state.selectedSymbol) {
        return;
      }
      const params = new URLSearchParams({
        exchange: state.selectedExchange,
        symbol: state.selectedSymbol,
      });
      const response = await fetch(`/api/history?${params.toString()}`);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `DB 이력 조회 실패: ${response.status}`);
      }
      const data = await response.json();

      el.balanceHistoryRoot.innerHTML = renderBalanceHistorySection(data.balance_snapshots || []);

      el.positionHistoryRoot.innerHTML = renderPositionHistorySection(data.position_snapshots || []);

      el.tradeHistoryRoot.innerHTML = renderTradeHistorySection(data.trade_history || []);

      el.fundingHistoryRoot.innerHTML = tableOrEmpty([
        { label: "펀딩 시각", render: (row) => fmtTs(row.funding_time_utc || row.funding_time_ms) },
        { label: "비율", render: (row) => fmt(row.funding_rate, 8) },
        { label: "실현 비율", render: (row) => fmt(row.realized_rate, 8) },
        { label: "마크가", render: (row) => fmt(row.mark_price, 4) },
        { label: "인덱스가", render: (row) => fmt(row.index_price, 4) },
      ], data.funding_history || []);

      el.orderHistoryRoot.innerHTML = tableOrEmpty([
        { label: "마지막 확인", render: (row) => fmtTs(row.last_seen_at_utc || row.last_seen_ms) },
        { label: "주문 UID", render: (row) => row.order_uid },
        { label: "방향", render: (row) => row.side || "-" },
        { label: "수량", render: (row) => fmt(row.qty, 6) },
        { label: "상태", render: (row) => row.status || "-" },
        { label: "열림 여부", render: (row) => row.is_open ? "열림" : "닫힘" },
        { label: "추적", render: (row) => traceButtonHtml(state.selectedExchange, row.symbol, row), html: true },
      ], data.order_history || []);

      el.orderEventsRoot.innerHTML = renderOrderEventSection(data.order_events || []);

      if (selectedTraceRef()) {
        await loadOrderTrace();
      } else {
        el.traceResultRoot.innerHTML = '<div class="empty">아직 선택한 주문이 없습니다.</div>';
        setTraceStatus("주문 선택 필요");
      }
    }

    async function postJson(url, payload) {
      const headers = { "Content-Type": "application/json" };
      const actionToken = String(el.actionTokenInput.value || "").trim();
      if (actionToken) {
        headers["X-OPS-TOKEN"] = actionToken;
      }
      const response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });
      const text = await response.text();
      let data = {};
      if (text) {
        try {
          data = JSON.parse(text);
        } catch (error) {
          throw new Error(text);
        }
      }
      if (!response.ok) {
        throw new Error(data.detail || text || `요청 실패: ${response.status}`);
      }
      return data;
    }

    function actionLabel(actionName) {
      const labels = {
        place_limit_order: "지정가 주문",
        cancel_all: "전체 취소",
        cancel_order: "주문 취소",
      };
      return labels[actionName] || actionName;
    }

    async function runAction(actionName, payload, url) {
      if (!tradingEnabled()) {
        throw new Error("웹 주문 기능이 비활성화되어 있습니다.");
      }
      if (!selectedTargetReady()) {
        throw new Error("먼저 거래소와 심볼을 선택하세요.");
      }

      const label = actionLabel(actionName);
      setActionStatus(`${label} 진행 중`, "loading");
      el.actionResultRoot.textContent = `${label} 요청\n` + stringifyPretty(payload);
      const data = await postJson(url, payload);
      el.actionResultRoot.textContent = stringifyPretty(data);
      setActionStatus(`${label} 완료`, "good");
      await refreshAll();
      return data;
    }

    async function submitLimitOrder() {
      const amount = Number(el.orderAmountInput.value);
      const price = Number(el.orderPriceInput.value);
      if (!Number.isFinite(amount) || amount <= 0) {
        throw new Error("수량은 0보다 커야 합니다.");
      }
      if (!Number.isFinite(price) || price <= 0) {
        throw new Error("가격은 0보다 커야 합니다.");
      }
      const payload = {
        exchange: state.selectedExchange,
        symbol: state.selectedSymbol,
        side: el.orderSideSelect.value,
        amount,
        price,
        wait_ws_sec: Number(el.actionWaitInput.value || 0) || undefined,
      };
      return runAction("place_limit_order", payload, "/api/action/order");
    }

    async function cancelAllOrders() {
      const payload = {
        exchange: state.selectedExchange,
        symbol: state.selectedSymbol,
        wait_ws_sec: Number(el.actionWaitInput.value || 0) || undefined,
      };
      return runAction("cancel_all", payload, "/api/action/cancel_all");
    }

    window.openOrderTrace = async function (exchange, symbol, orderRef) {
      try {
        state.selectedExchange = exchange;
        state.selectedSymbol = symbol;
        state.traceOrderRef = String(orderRef || "").trim();
        el.traceOrderInput.value = state.traceOrderRef;
        renderHistoryTarget();
        await loadHistory();
      } catch (error) {
        console.error(error);
        setTraceStatus(`오류: ${error.message}`, "bad");
        el.traceResultRoot.innerHTML = `<div class="empty">${escapeHtml(String(error.message || error))}</div>`;
      }
    };

    window.cancelLiveOrder = async function (exchange, symbol, uuid) {
      try {
        const payload = {
          exchange,
          symbol,
          uuid,
          wait_ws_sec: Number(el.actionWaitInput.value || 0) || undefined,
        };
        await runAction("cancel_order", payload, "/api/action/cancel");
      } catch (error) {
        console.error(error);
        setActionStatus(`오류: ${error.message}`, "bad");
        el.actionResultRoot.textContent = String(error.message || error);
      }
    };

    async function refreshAll() {
      if (state.loading) return;
      state.loading = true;
      el.refreshBtn.disabled = true;
      setStatus("실시간 / DB 불러오는 중...", "loading");
      try {
        await loadLive();
        await loadSoakStatus();
        await loadHistory();
        setStatus("갱신 완료", "good");
      } catch (error) {
        console.error(error);
        setStatus(`오류: ${error.message}`, "bad");
      } finally {
        state.loading = false;
        el.refreshBtn.disabled = false;
      }
    }

    function scheduleAutoRefresh() {
      if (state.timer) {
        clearInterval(state.timer);
        state.timer = null;
      }
      const seconds = Number(el.autoRefreshSelect.value);
      if (Number.isFinite(seconds) && seconds > 0) {
        state.timer = setInterval(refreshAll, seconds * 1000);
      }
    }

    el.refreshBtn.addEventListener("click", refreshAll);
    el.autoRefreshSelect.addEventListener("change", scheduleAutoRefresh);
    el.exchangesInput.addEventListener("change", refreshAll);
    el.symbolsInput.addEventListener("change", refreshAll);
    el.waitInput.addEventListener("change", refreshAll);
    el.tradeLimitInput.addEventListener("change", refreshAll);
    el.actionTokenInput.addEventListener("input", () => {
      window.localStorage.setItem(ACTION_TOKEN_STORAGE_KEY, el.actionTokenInput.value || "");
    });
    el.traceLoadBtn.addEventListener("click", async () => {
      try {
        state.traceOrderRef = selectedTraceRef();
        await loadOrderTrace();
      } catch (error) {
        console.error(error);
        setTraceStatus(`오류: ${error.message}`, "bad");
        el.traceResultRoot.innerHTML = `<div class="empty">${escapeHtml(String(error.message || error))}</div>`;
      }
    });
    el.traceClearBtn.addEventListener("click", clearOrderTrace);
    el.traceOrderInput.addEventListener("keydown", async (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      try {
        state.traceOrderRef = selectedTraceRef();
        await loadOrderTrace();
      } catch (error) {
        console.error(error);
        setTraceStatus(`오류: ${error.message}`, "bad");
        el.traceResultRoot.innerHTML = `<div class="empty">${escapeHtml(String(error.message || error))}</div>`;
      }
    });
    el.placeOrderBtn.addEventListener("click", async () => {
      try {
        await submitLimitOrder();
      } catch (error) {
        console.error(error);
        setActionStatus(`오류: ${error.message}`, "bad");
        el.actionResultRoot.textContent = String(error.message || error);
      }
    });
    el.cancelAllBtn.addEventListener("click", async () => {
      try {
        await cancelAllOrders();
      } catch (error) {
        console.error(error);
        setActionStatus(`오류: ${error.message}`, "bad");
        el.actionResultRoot.textContent = String(error.message || error);
      }
    });

    (async function bootstrap() {
      await loadConfig();
      scheduleAutoRefresh();
      await refreshAll();
    })();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(INDEX_HTML)
