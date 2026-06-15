import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import mariadb
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LEGACY_SCHEMA_PATH = PROJECT_ROOT / "sql" / "mariadb_schema.sql"
TRADING_DEV_SCHEMA_PATH = PROJECT_ROOT / "sql" / "trading_dev_schema.sql"
DEFAULT_DB_NAME = "trading_dev"
DB_PERSISTENCE_TRUE_VALUES = {"1", "true", "on"}
DB_ENV_KEYS = (
    "MARIADB_HOST",
    "MARIADB_PORT",
    "MARIADB_USER",
    "MARIADB_PASSWORD",
    "MARIADB_DATABASE",
)
DB_REQUIRED_ENV_KEYS = ("MARIADB_HOST", "MARIADB_USER", "MARIADB_DATABASE")


def _resolve_env_file_path(raw_path: str) -> Path:
    env_path = Path(raw_path).expanduser()
    if not env_path.is_absolute():
        env_path = PROJECT_ROOT / env_path
    return env_path


def _load_persistence_env() -> None:
    env_file = os.getenv("STRATEGY_ENV_FILE")
    if env_file:
        env_path = _resolve_env_file_path(env_file)
        if env_path.exists():
            load_dotenv(env_path, override=True)
        # Keep DB writes explicit to the selected strategy env.
        # This prevents a later bare load_dotenv() from enabling DB via root .env.
        os.environ.setdefault("ENABLE_DB_PERSISTENCE", "0")
        for key in DB_ENV_KEYS:
            os.environ.setdefault(key, "3306" if key == "MARIADB_PORT" else "")
        return

    load_dotenv(PROJECT_ROOT / ".env", override=False)


_load_persistence_env()


def resolve_default_schema_path() -> Path:
    database_name = (os.getenv("MARIADB_DATABASE") or DEFAULT_DB_NAME).strip().lower()
    if database_name == "trading_dev" and TRADING_DEV_SCHEMA_PATH.exists():
        return TRADING_DEV_SCHEMA_PATH
    return LEGACY_SCHEMA_PATH


DEFAULT_SCHEMA_PATH = resolve_default_schema_path()


def _env_flag(key: str, default: str = "0") -> bool:
    return os.getenv(key, default).strip().lower() in DB_PERSISTENCE_TRUE_VALUES


def db_env_configured() -> bool:
    return all((os.getenv(key) or "").strip() for key in DB_REQUIRED_ENV_KEYS)


def db_persistence_enabled() -> bool:
    return _env_flag("ENABLE_DB_PERSISTENCE", "0") and db_env_configured()


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        port_text = (os.getenv("MARIADB_PORT") or "3306").strip()
        return cls(
            host=(os.getenv("MARIADB_HOST") or "127.0.0.1").strip(),
            port=int(port_text or "3306"),
            user=(os.getenv("MARIADB_USER") or "trader").strip(),
            password=os.getenv("MARIADB_PASSWORD", ""),
            database=(os.getenv("MARIADB_DATABASE") or DEFAULT_DB_NAME).strip(),
        )


def get_db_connection(config: DbConfig | None = None):
    config = config or DbConfig.from_env()
    return mariadb.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
    )


def to_utc_datetime(ms: int | float | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def json_loads_or_none(raw: Any) -> Any:
    if raw in (None, ""):
        return None
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return None


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).strip().rstrip(";"))
            current = []

    if current:
        statements.append("\n".join(current).strip().rstrip(";"))

    return [stmt for stmt in statements if stmt]


def ensure_schema(conn, schema_path: str | Path = DEFAULT_SCHEMA_PATH) -> int:
    schema_text = Path(schema_path).read_text(encoding="utf-8")
    statements = split_sql_statements(schema_text)
    cur = conn.cursor()
    try:
        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
    finally:
        cur.close()
    return len(statements)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def current_utc_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _tracked_symbols_csv(symbols: list[str]) -> str:
    return ",".join(sorted({str(symbol).upper() for symbol in symbols if str(symbol).strip()}))


def _trade_side(trade: dict[str, Any]) -> str | None:
    side = trade.get("side")
    if side:
        return str(side).upper()

    for key in ("qty_contract", "qty"):
        value = trade.get(key)
        if value is None:
            continue
        try:
            value_float = float(value)
        except Exception:
            continue
        if value_float > 0:
            return "BUY"
        if value_float < 0:
            return "SELL"
    return None


def _trade_role(trade: dict[str, Any]) -> str | None:
    if trade.get("role"):
        return str(trade["role"]).lower()
    if trade.get("maker") is True:
        return "maker"
    if trade.get("maker") is False:
        return "taker"
    execution_type = trade.get("execution_type")
    if execution_type == "M":
        return "maker"
    if execution_type == "T":
        return "taker"
    return None

def _synth_trade_id(exchange_name: str, symbol: str, trade: dict[str, Any], create_time_ms: int | None) -> str | None:
    if create_time_ms is None:
        return None
    payload = {
        "exchange_name": exchange_name,
        "symbol": symbol.upper(),
        "order_id": _string_or_none(trade.get("order_id")),
        "client_order_id": _string_or_none(trade.get("client_order_id")),
        "side": _trade_side(trade),
        "price": _safe_float(trade.get("price")),
        "qty": _safe_float(trade.get("qty")),
        "qty_contract": _safe_float(trade.get("qty_contract")),
        "quote_qty": _safe_float(trade.get("quote_qty")),
        "create_time_ms": int(create_time_ms),
    }
    digest = hashlib.sha256(json_dumps(payload).encode("utf-8")).hexdigest()[:24]
    return f"synthetic:{digest}"


def upsert_trade_history(conn, exchange_name: str, symbol: str, trades: list[dict[str, Any]]) -> int:
    if not trades:
        return 0

    sql = """
    INSERT INTO trade_history (
        exchange_name, symbol, trade_id, order_id, client_order_id,
        side, role, position_side, price, qty, qty_contract, quote_qty,
        fee, fee_ccy, realized_pnl, create_time_ms, created_at_utc, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        order_id = VALUES(order_id),
        client_order_id = VALUES(client_order_id),
        side = VALUES(side),
        role = VALUES(role),
        position_side = VALUES(position_side),
        price = VALUES(price),
        qty = VALUES(qty),
        qty_contract = VALUES(qty_contract),
        quote_qty = VALUES(quote_qty),
        fee = VALUES(fee),
        fee_ccy = VALUES(fee_ccy),
        realized_pnl = VALUES(realized_pnl),
        create_time_ms = VALUES(create_time_ms),
        created_at_utc = VALUES(created_at_utc),
        raw_json = VALUES(raw_json)
    """

    rows = []
    for trade in trades:
        create_time_ms = _safe_int(
            trade.get("create_time")
            or trade.get("timestamp")
            or trade.get("time")
        )
        if create_time_ms is None:
            continue

        trade_id = _string_or_none(trade.get("trade_id")) or _synth_trade_id(
            exchange_name,
            symbol,
            trade,
            create_time_ms,
        )
        if trade_id is None:
            continue

        rows.append(
            (
                exchange_name,
                symbol.upper(),
                trade_id,
                _string_or_none(trade.get("order_id")),
                _string_or_none(trade.get("client_order_id")),
                _trade_side(trade),
                _trade_role(trade),
                _string_or_none(trade.get("position_side") or trade.get("pos_side")),
                float(trade.get("price", 0.0) or 0.0),
                float(trade.get("qty", 0.0) or 0.0),
                _safe_float(trade.get("qty_contract")),
                _safe_float(trade.get("quote_qty")),
                _safe_float(trade.get("fee")),
                _string_or_none(trade.get("fee_ccy") or trade.get("commission_asset")),
                _safe_float(trade.get("realized_pnl")),
                create_time_ms,
                to_utc_datetime(create_time_ms),
                json_dumps(trade.get("raw", trade)),
            )
        )

    if not rows:
        return 0

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()
    return len(rows)


def upsert_funding_history(conn, exchange_name: str, symbol: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO funding_history (
        exchange_name, symbol, funding_time_ms, funding_time_utc,
        funding_rate, realized_rate, mark_price, index_price, next_funding_time_ms, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        funding_rate = VALUES(funding_rate),
        realized_rate = VALUES(realized_rate),
        mark_price = VALUES(mark_price),
        index_price = VALUES(index_price),
        next_funding_time_ms = VALUES(next_funding_time_ms),
        raw_json = VALUES(raw_json)
    """

    values = []
    for row in rows:
        funding_time_ms = _safe_int(row.get("funding_time"))
        if funding_time_ms is None:
            continue

        values.append(
            (
                exchange_name,
                symbol.upper(),
                funding_time_ms,
                to_utc_datetime(funding_time_ms),
                float(row.get("funding_rate", 0.0) or 0.0),
                _safe_float(row.get("realized_rate")),
                _safe_float(row.get("mark_price")),
                _safe_float(row.get("index_price")),
                _safe_int(row.get("next_funding_time")),
                json_dumps(row.get("raw", row)),
            )
        )

    if not values:
        return 0

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)


def insert_balance_snapshot(
    conn,
    exchange_name: str,
    tracked_symbols: list[str],
    balance_report: dict[str, Any],
    *,
    snapshot_time_ms: int | None = None,
) -> int:
    snapshot_time_ms = snapshot_time_ms or current_utc_ms()
    tracked_symbols_csv = _tracked_symbols_csv(tracked_symbols)
    if not tracked_symbols_csv:
        tracked_symbols_csv = "ALL"

    sql = """
    INSERT INTO balance_snapshots (
        exchange_name, tracked_symbols_csv, snapshot_time_ms, snapshot_time_utc,
        balance_asset_code, wallet_balance_usd, available_balance_usd, locked_balance_usd, margin_balance_usd,
        exchange_unrealized_profit_usd, theoretical_unrealized_profit_usd,
        actual_equity_usd, theoretical_equity_usd, theoretical_equity_gap_usd,
        total_balance_usd, long_notional_usd, short_notional_usd, gross_notional_usd, net_notional_usd,
        leverage, delta_leverage, position_count, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        balance_asset_code = VALUES(balance_asset_code),
        wallet_balance_usd = VALUES(wallet_balance_usd),
        available_balance_usd = VALUES(available_balance_usd),
        locked_balance_usd = VALUES(locked_balance_usd),
        margin_balance_usd = VALUES(margin_balance_usd),
        exchange_unrealized_profit_usd = VALUES(exchange_unrealized_profit_usd),
        theoretical_unrealized_profit_usd = VALUES(theoretical_unrealized_profit_usd),
        actual_equity_usd = VALUES(actual_equity_usd),
        theoretical_equity_usd = VALUES(theoretical_equity_usd),
        theoretical_equity_gap_usd = VALUES(theoretical_equity_gap_usd),
        total_balance_usd = VALUES(total_balance_usd),
        long_notional_usd = VALUES(long_notional_usd),
        short_notional_usd = VALUES(short_notional_usd),
        gross_notional_usd = VALUES(gross_notional_usd),
        net_notional_usd = VALUES(net_notional_usd),
        leverage = VALUES(leverage),
        delta_leverage = VALUES(delta_leverage),
        position_count = VALUES(position_count),
        raw_json = VALUES(raw_json)
    """

    snapshot_payload = {
        "tracked_symbols": tracked_symbols_csv.split(",") if tracked_symbols_csv != "ALL" else [],
        **balance_report,
    }
    values = (
        exchange_name,
        tracked_symbols_csv,
        snapshot_time_ms,
        to_utc_datetime(snapshot_time_ms),
        _string_or_none(balance_report.get("balance_asset_code")) or "USD",
        _safe_float(balance_report.get("wallet_balance_usd")) or 0.0,
        _safe_float(balance_report.get("available_balance_usd")) or 0.0,
        _safe_float(balance_report.get("locked_balance_usd")) or 0.0,
        _safe_float(balance_report.get("margin_balance_usd")) or 0.0,
        _safe_float(balance_report.get("exchange_unrealized_profit_usd")) or 0.0,
        _safe_float(balance_report.get("theoretical_unrealized_profit_usd")) or 0.0,
        _safe_float(balance_report.get("actual_equity_usd")) or 0.0,
        _safe_float(balance_report.get("theoretical_equity_usd")) or 0.0,
        _safe_float(balance_report.get("theoretical_equity_gap_usd")) or 0.0,
        _safe_float(balance_report.get("total_balance_usd")) or 0.0,
        _safe_float(balance_report.get("long_notional_usd")) or 0.0,
        _safe_float(balance_report.get("short_notional_usd")) or 0.0,
        _safe_float(balance_report.get("gross_notional_usd")) or 0.0,
        _safe_float(balance_report.get("net_notional_usd")) or 0.0,
        _safe_float(balance_report.get("leverage")) or 0.0,
        _safe_float(balance_report.get("delta_leverage")) or 0.0,
        len(balance_report.get("positions") or []),
        json_dumps(snapshot_payload),
    )

    cur = conn.cursor()
    try:
        cur.execute(sql, values)
        conn.commit()
        return 1
    finally:
        cur.close()


def insert_position_snapshots(
    conn,
    exchange_name: str,
    tracked_symbols: list[str],
    balance_report: dict[str, Any],
    *,
    snapshot_time_ms: int | None = None,
) -> int:
    snapshot_time_ms = snapshot_time_ms or current_utc_ms()
    tracked_symbols_csv = _tracked_symbols_csv(tracked_symbols)
    if not tracked_symbols_csv:
        tracked_symbols_csv = "ALL"

    positions = balance_report.get("positions") or []
    if not positions:
        return 0

    sql = """
    INSERT INTO position_snapshots (
        exchange_name, tracked_symbols_csv, symbol, snapshot_time_ms, snapshot_time_utc,
        position_side, qty, entry_price, mark_price, mark_source, notional_mark_usd,
        exchange_unrealized_profit_usd, theoretical_unrealized_profit_usd, unrealized_profit_gap_usd,
        theoretical_upl_source, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        position_side = VALUES(position_side),
        qty = VALUES(qty),
        entry_price = VALUES(entry_price),
        mark_price = VALUES(mark_price),
        mark_source = VALUES(mark_source),
        notional_mark_usd = VALUES(notional_mark_usd),
        exchange_unrealized_profit_usd = VALUES(exchange_unrealized_profit_usd),
        theoretical_unrealized_profit_usd = VALUES(theoretical_unrealized_profit_usd),
        unrealized_profit_gap_usd = VALUES(unrealized_profit_gap_usd),
        theoretical_upl_source = VALUES(theoretical_upl_source),
        raw_json = VALUES(raw_json)
    """

    rows = []
    for position in positions:
        symbol = _string_or_none(position.get("ticker") or position.get("symbol"))
        if symbol is None:
            continue
        payload = {
            "tracked_symbols": tracked_symbols_csv.split(",") if tracked_symbols_csv != "ALL" else [],
            **position,
        }
        rows.append(
            (
                exchange_name,
                tracked_symbols_csv,
                symbol.upper(),
                snapshot_time_ms,
                to_utc_datetime(snapshot_time_ms),
                _string_or_none(position.get("position_side")),
                _safe_float(position.get("qty")) or 0.0,
                _safe_float(position.get("entry_price")) or 0.0,
                _safe_float(position.get("mark_price")) or 0.0,
                _string_or_none(position.get("mark_source")),
                _safe_float(position.get("notional_mark_usd")) or 0.0,
                _safe_float(position.get("exchange_unrealized_profit_usd")) or 0.0,
                _safe_float(position.get("theoretical_unrealized_profit_usd")) or 0.0,
                _safe_float(position.get("unrealized_profit_gap_usd")) or 0.0,
                _string_or_none(position.get("theoretical_upl_source")),
                json_dumps(payload),
            )
        )

    if not rows:
        return 0

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        cur.close()


def order_uid(order: dict[str, Any]) -> str | None:
    for key in ("order_id", "uuid", "client_order_id", "identifier"):
        value = _string_or_none(order.get(key))
        if value is not None:
            return value
    return None


def fetch_open_order_history_rows(conn, exchange_name: str, symbol: str) -> list[dict[str, Any]]:
    sql = """
    SELECT
        exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
        side, position_side, price, qty, qty_contract, executed_qty, left_qty, avg_price,
        status, state, reduce_only, source, first_seen_ms, last_seen_ms, raw_json
    FROM order_history
    WHERE exchange_name = ?
      AND symbol = ?
      AND is_open = 1
    ORDER BY last_seen_ms ASC, id ASC
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (exchange_name, symbol.upper()))
        columns = [desc[0] for desc in cur.description]
        rows = []
        for raw_row in cur.fetchall():
            row = dict(zip(columns, raw_row))
            row["raw"] = json_loads_or_none(row.get("raw_json"))
            rows.append(row)
        return rows
    finally:
        cur.close()


def build_order_event_payload_from_history_row(row: dict[str, Any]) -> dict[str, Any]:
    raw_payload = row.get("raw")
    return {
        "ticker": row.get("symbol"),
        "symbol": row.get("symbol"),
        "order_id": row.get("order_id"),
        "client_order_id": row.get("client_order_id"),
        "uuid": row.get("uuid"),
        "side": row.get("side"),
        "position_side": row.get("position_side"),
        "price": row.get("price"),
        "qty": row.get("qty"),
        "qty_contract": row.get("qty_contract"),
        "executed_qty": row.get("executed_qty"),
        "left_qty": row.get("left_qty"),
        "avg_price": row.get("avg_price"),
        "status": row.get("status"),
        "state": row.get("state"),
        "reduce_only": bool(row.get("reduce_only")),
        "timestamp": row.get("last_seen_ms") or row.get("first_seen_ms"),
        "raw": raw_payload if raw_payload is not None else row,
    }


def resolve_order_event_uid(
    payload: dict[str, Any],
    *,
    symbol: str,
    event_type: str,
    event_time_ms: int,
) -> str:
    explicit_uid = _string_or_none(payload.get("order_uid"))
    if explicit_uid is not None:
        return explicit_uid

    uid = order_uid(payload)
    if uid is not None:
        return uid

    raw_payload = payload.get("raw")
    if isinstance(raw_payload, dict):
        raw_explicit_uid = _string_or_none(raw_payload.get("order_uid"))
        if raw_explicit_uid is not None:
            return raw_explicit_uid
        raw_uid = order_uid(raw_payload)
        if raw_uid is not None:
            return raw_uid

    fallback_symbol = _string_or_none(payload.get("ticker") or payload.get("symbol")) or symbol.upper()
    return f"synthetic:{fallback_symbol}:{event_type}:{event_time_ms}"


def _event_error_code(payload: dict[str, Any]) -> str | None:
    for key in ("error_code", "code", "sCode", "label"):
        value = _string_or_none(payload.get(key))
        if value is not None:
            return value
    raw_payload = payload.get("raw")
    if isinstance(raw_payload, dict):
        for key in ("error_code", "code", "sCode", "label"):
            value = _string_or_none(raw_payload.get(key))
            if value is not None:
                return value
    return None


def _event_error_message(payload: dict[str, Any]) -> str | None:
    for key in ("error_message", "error", "msg", "message", "sMsg"):
        value = _string_or_none(payload.get(key))
        if value is not None:
            return value[:255]
    raw_payload = payload.get("raw")
    if isinstance(raw_payload, dict):
        for key in ("error_message", "error", "msg", "message", "sMsg"):
            value = _string_or_none(raw_payload.get(key))
            if value is not None:
                return value[:255]
    return None


def build_order_event_key(
    *,
    exchange_name: str,
    symbol: str,
    order_uid_value: str,
    event_type: str,
    event_source: str,
    payload: dict[str, Any],
) -> str:
    digest_source = {
        "exchange_name": exchange_name,
        "symbol": symbol.upper(),
        "order_uid": order_uid_value,
        "event_type": event_type,
        "event_source": event_source,
        "order_id": _string_or_none(payload.get("order_id")),
        "client_order_id": _string_or_none(payload.get("client_order_id")),
        "uuid": _string_or_none(payload.get("uuid")),
        "side": _string_or_none(payload.get("side")),
        "position_side": _string_or_none(payload.get("position_side")),
        "price": _safe_float(payload.get("price")),
        "qty": _safe_float(payload.get("qty")),
        "qty_contract": _safe_float(payload.get("qty_contract")),
        "executed_qty": _safe_float(payload.get("executed_qty")),
        "left_qty": _safe_float(payload.get("left_qty")),
        "avg_price": _safe_float(payload.get("avg_price")),
        "status": _string_or_none(payload.get("status")),
        "state": _string_or_none(payload.get("state")),
        "reduce_only": bool(payload.get("reduce_only", False)),
        "error_code": _event_error_code(payload),
        "error_message": _event_error_message(payload),
        "event_nonce": _safe_int(payload.get("event_nonce")),
    }
    raw = json_dumps(digest_source)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_order_event_raw_json(
    *,
    payload: dict[str, Any],
    event_type: str,
    event_source: str,
    event_time_ms: int,
    order_uid_value: str,
) -> dict[str, Any]:
    normalized_payload = dict(payload or {})
    exchange_raw = normalized_payload.pop("raw", None)
    return {
        "storage_version": 2,
        "event_type": event_type,
        "event_source": event_source,
        "event_time_ms": event_time_ms,
        "order_uid": order_uid_value,
        "normalized": normalized_payload,
        "exchange_raw": exchange_raw,
    }


def insert_order_events(
    conn,
    exchange_name: str,
    symbol: str,
    events: list[dict[str, Any]],
) -> int:
    if not events:
        return 0

    sql = """
    INSERT IGNORE INTO order_events (
        event_key, exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
        event_type, event_source, side, position_side, price, qty, qty_contract,
        executed_qty, left_qty, avg_price, status, state, reduce_only,
        error_code, error_message, event_time_ms, event_time_utc, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    symbol = symbol.upper()
    rows = []
    for event in events:
        payload = dict(event.get("payload") or {})
        event_type = _string_or_none(event.get("event_type")) or "snapshot"
        event_source = _string_or_none(event.get("event_source")) or "sync"
        event_time_ms = _safe_int(
            event.get("event_time_ms")
            or payload.get("timestamp")
            or payload.get("create_time")
            or payload.get("time")
        ) or current_utc_ms()
        event_uid = resolve_order_event_uid(payload, symbol=symbol, event_type=event_type, event_time_ms=event_time_ms)
        event_key = _string_or_none(event.get("event_key")) or build_order_event_key(
            exchange_name=exchange_name,
            symbol=symbol,
            order_uid_value=event_uid,
            event_type=event_type,
            event_source=event_source,
            payload=payload,
        )

        raw_payload = build_order_event_raw_json(
            payload=payload,
            event_type=event_type,
            event_source=event_source,
            event_time_ms=event_time_ms,
            order_uid_value=event_uid,
        )

        rows.append(
            (
                event_key,
                exchange_name,
                symbol,
                event_uid,
                _string_or_none(payload.get("order_id")),
                _string_or_none(payload.get("client_order_id")),
                _string_or_none(payload.get("uuid")),
                event_type,
                event_source,
                _string_or_none(payload.get("side")),
                _string_or_none(payload.get("position_side")),
                _safe_float(payload.get("price")),
                _safe_float(payload.get("qty")),
                _safe_float(payload.get("qty_contract")),
                _safe_float(payload.get("executed_qty")),
                _safe_float(payload.get("left_qty")),
                _safe_float(payload.get("avg_price")),
                _string_or_none(payload.get("status")),
                _string_or_none(payload.get("state")),
                1 if bool(payload.get("reduce_only", False)) else 0,
                _event_error_code(payload),
                _event_error_message(payload),
                event_time_ms,
                to_utc_datetime(event_time_ms),
                json_dumps(raw_payload),
            )
        )

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        inserted_count = cur.rowcount
        conn.commit()
        return inserted_count
    finally:
        cur.close()


def append_order_event(
    conn,
    *,
    exchange_name: str,
    symbol: str,
    event_type: str,
    event_source: str,
    payload: dict[str, Any],
    event_time_ms: int | None = None,
) -> int:
    return insert_order_events(
        conn,
        exchange_name=exchange_name,
        symbol=symbol,
        events=[
            {
                "event_type": event_type,
                "event_source": event_source,
                "event_time_ms": event_time_ms,
                "payload": payload,
            }
        ],
    )


def upsert_order_history_snapshot(
    conn,
    exchange_name: str,
    symbol: str,
    orders: list[dict[str, Any]],
    *,
    source: str = "open_order_sync",
    seen_ms: int | None = None,
) -> dict[str, int]:
    symbol = symbol.upper()
    seen_ms = seen_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    seen_at_utc = to_utc_datetime(seen_ms)

    close_cur = conn.cursor()
    try:
        current_uids = [uid for uid in (order_uid(order) for order in orders) if uid is not None]
        if current_uids:
            placeholders = ", ".join("?" for _ in current_uids)
            close_sql = f"""
            UPDATE order_history
            SET is_open = 0,
                last_seen_ms = ?,
                last_seen_at_utc = ?,
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE exchange_name = ?
              AND symbol = ?
              AND is_open = 1
              AND order_uid NOT IN ({placeholders})
            """
            close_params = [seen_ms, seen_at_utc, exchange_name, symbol, *current_uids]
        else:
            close_sql = """
            UPDATE order_history
            SET is_open = 0,
                last_seen_ms = ?,
                last_seen_at_utc = ?,
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE exchange_name = ?
              AND symbol = ?
              AND is_open = 1
            """
            close_params = [seen_ms, seen_at_utc, exchange_name, symbol]

        close_cur.execute(close_sql, close_params)
        closed_count = close_cur.rowcount
        conn.commit()
    finally:
        close_cur.close()

    if not orders:
        return {"open_synced": 0, "closed_marked": closed_count}

    sql = """
    INSERT INTO order_history (
        exchange_name, symbol, order_uid, order_id, client_order_id, uuid,
        side, position_side, price, qty, qty_contract, executed_qty, left_qty, avg_price,
        status, state, reduce_only, source, first_seen_ms, first_seen_at_utc,
        last_seen_ms, last_seen_at_utc, is_open, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        order_id = VALUES(order_id),
        client_order_id = VALUES(client_order_id),
        uuid = VALUES(uuid),
        side = VALUES(side),
        position_side = VALUES(position_side),
        price = VALUES(price),
        qty = VALUES(qty),
        qty_contract = VALUES(qty_contract),
        executed_qty = VALUES(executed_qty),
        left_qty = VALUES(left_qty),
        avg_price = VALUES(avg_price),
        status = VALUES(status),
        state = VALUES(state),
        reduce_only = VALUES(reduce_only),
        source = VALUES(source),
        first_seen_ms = IFNULL(first_seen_ms, VALUES(first_seen_ms)),
        first_seen_at_utc = IFNULL(first_seen_at_utc, VALUES(first_seen_at_utc)),
        last_seen_ms = VALUES(last_seen_ms),
        last_seen_at_utc = VALUES(last_seen_at_utc),
        is_open = VALUES(is_open),
        raw_json = VALUES(raw_json)
    """

    rows = []
    for order in orders:
        uid = order_uid(order)
        if uid is None:
            continue

        first_seen_ms = _safe_int(order.get("timestamp")) or seen_ms
        first_seen_at_utc = to_utc_datetime(first_seen_ms)

        rows.append(
            (
                exchange_name,
                symbol,
                uid,
                _string_or_none(order.get("order_id")),
                _string_or_none(order.get("client_order_id")),
                _string_or_none(order.get("uuid")),
                _string_or_none(order.get("side")),
                _string_or_none(order.get("position_side")),
                _safe_float(order.get("price")),
                _safe_float(order.get("qty")),
                _safe_float(order.get("qty_contract")),
                _safe_float(order.get("executed_qty")),
                _safe_float(order.get("left_qty")),
                _safe_float(order.get("avg_price")),
                _string_or_none(order.get("status")),
                _string_or_none(order.get("state")),
                1 if bool(order.get("reduce_only", False)) else 0,
                source,
                first_seen_ms,
                first_seen_at_utc,
                seen_ms,
                seen_at_utc,
                1,
                json_dumps(order.get("raw", order)),
            )
        )

    if not rows:
        return {"open_synced": 0, "closed_marked": closed_count}

    upsert_cur = conn.cursor()
    try:
        upsert_cur.executemany(sql, rows)
        conn.commit()
    finally:
        upsert_cur.close()

    return {"open_synced": len(rows), "closed_marked": closed_count}


def fetch_trade_history_rows(
    conn,
    *,
    symbol: str,
    exchange_names: list[str],
    start_time_ms: int,
    end_time_ms: int,
) -> list[dict[str, Any]]:
    normalized_symbol = symbol.upper()
    normalized_exchanges = [str(name).strip().lower() for name in exchange_names if str(name).strip()]
    if not normalized_exchanges:
        return []

    placeholders = ", ".join("?" for _ in normalized_exchanges)
    sql = f"""
    SELECT
        exchange_name, symbol, trade_id, order_id, client_order_id,
        side, role, position_side, price, qty, qty_contract, quote_qty,
        fee, fee_ccy, realized_pnl, create_time_ms, created_at_utc, raw_json
    FROM trade_history
    WHERE symbol = ?
      AND exchange_name IN ({placeholders})
      AND create_time_ms BETWEEN ? AND ?
    ORDER BY create_time_ms ASC, id ASC
    """

    params: list[Any] = [normalized_symbol, *normalized_exchanges, int(start_time_ms), int(end_time_ms)]
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        rows: list[dict[str, Any]] = []
        for raw_row in cur.fetchall():
            row = dict(zip(columns, raw_row))
            row["raw"] = json_loads_or_none(row.get("raw_json"))
            rows.append(row)
        return rows
    finally:
        cur.close()


def upsert_pair_ledgers(conn, ledgers: list[dict[str, Any]]) -> int:
    if not ledgers:
        return 0

    sql = """
    INSERT INTO pair_ledger (
        pair_id, symbol, long_exchange, short_exchange,
        opened_time_ms, opened_time_utc, closed_time_ms, closed_time_utc,
        last_event_time_ms, last_event_time_utc, duration_sec,
        status, final_event_type, close_reason,
        requested_qty, pair_qty, long_entry_price, short_entry_price,
        entry_net_edge_usd, expected_funding_pnl_usd, funding_long_rate, funding_short_rate,
        last_hold_decision, last_hold_pnl_usd, last_close_pnl_usd, last_close_edge_usd,
        auto_close_trigger_count, auto_close_not_completed_count, close_pending_count, partial_close_count,
        recovery_event_count, emergency_close_count, detected_close_count, close_noop_count,
        trade_count, long_trade_count, short_trade_count,
        total_fee_usd, long_fee_usd, short_fee_usd,
        total_realized_pnl_usd, long_realized_pnl_usd, short_realized_pnl_usd,
        raw_json
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?
    )
    ON DUPLICATE KEY UPDATE
        symbol = VALUES(symbol),
        long_exchange = VALUES(long_exchange),
        short_exchange = VALUES(short_exchange),
        opened_time_ms = VALUES(opened_time_ms),
        opened_time_utc = VALUES(opened_time_utc),
        closed_time_ms = VALUES(closed_time_ms),
        closed_time_utc = VALUES(closed_time_utc),
        last_event_time_ms = VALUES(last_event_time_ms),
        last_event_time_utc = VALUES(last_event_time_utc),
        duration_sec = VALUES(duration_sec),
        status = VALUES(status),
        final_event_type = VALUES(final_event_type),
        close_reason = VALUES(close_reason),
        requested_qty = VALUES(requested_qty),
        pair_qty = VALUES(pair_qty),
        long_entry_price = VALUES(long_entry_price),
        short_entry_price = VALUES(short_entry_price),
        entry_net_edge_usd = VALUES(entry_net_edge_usd),
        expected_funding_pnl_usd = VALUES(expected_funding_pnl_usd),
        funding_long_rate = VALUES(funding_long_rate),
        funding_short_rate = VALUES(funding_short_rate),
        last_hold_decision = VALUES(last_hold_decision),
        last_hold_pnl_usd = VALUES(last_hold_pnl_usd),
        last_close_pnl_usd = VALUES(last_close_pnl_usd),
        last_close_edge_usd = VALUES(last_close_edge_usd),
        auto_close_trigger_count = VALUES(auto_close_trigger_count),
        auto_close_not_completed_count = VALUES(auto_close_not_completed_count),
        close_pending_count = VALUES(close_pending_count),
        partial_close_count = VALUES(partial_close_count),
        recovery_event_count = VALUES(recovery_event_count),
        emergency_close_count = VALUES(emergency_close_count),
        detected_close_count = VALUES(detected_close_count),
        close_noop_count = VALUES(close_noop_count),
        trade_count = VALUES(trade_count),
        long_trade_count = VALUES(long_trade_count),
        short_trade_count = VALUES(short_trade_count),
        total_fee_usd = VALUES(total_fee_usd),
        long_fee_usd = VALUES(long_fee_usd),
        short_fee_usd = VALUES(short_fee_usd),
        total_realized_pnl_usd = VALUES(total_realized_pnl_usd),
        long_realized_pnl_usd = VALUES(long_realized_pnl_usd),
        short_realized_pnl_usd = VALUES(short_realized_pnl_usd),
        raw_json = VALUES(raw_json)
    """

    rows: list[tuple[Any, ...]] = []
    for ledger in ledgers:
        opened_time_ms = _safe_int(ledger.get("opened_time_ms")) or 0
        closed_time_ms = _safe_int(ledger.get("closed_time_ms"))
        last_event_time_ms = _safe_int(ledger.get("last_event_time_ms")) or opened_time_ms
        rows.append(
            (
                str(ledger.get("pair_id") or ""),
                str(ledger.get("symbol") or "").upper(),
                str(ledger.get("long_exchange") or "").lower(),
                str(ledger.get("short_exchange") or "").lower(),
                opened_time_ms,
                to_utc_datetime(opened_time_ms),
                closed_time_ms,
                to_utc_datetime(closed_time_ms),
                last_event_time_ms,
                to_utc_datetime(last_event_time_ms),
                _safe_float(ledger.get("duration_sec")),
                _string_or_none(ledger.get("status")) or "unknown",
                _string_or_none(ledger.get("final_event_type")) or "unknown",
                _string_or_none(ledger.get("close_reason")),
                _safe_float(ledger.get("requested_qty")) or 0.0,
                _safe_float(ledger.get("pair_qty")) or 0.0,
                _safe_float(ledger.get("long_entry_price")),
                _safe_float(ledger.get("short_entry_price")),
                _safe_float(ledger.get("entry_net_edge_usd")),
                _safe_float(ledger.get("expected_funding_pnl_usd")),
                _safe_float(ledger.get("funding_long_rate")),
                _safe_float(ledger.get("funding_short_rate")),
                _string_or_none(ledger.get("last_hold_decision")),
                _safe_float(ledger.get("last_hold_pnl_usd")),
                _safe_float(ledger.get("last_close_pnl_usd")),
                _safe_float(ledger.get("last_close_edge_usd")),
                _safe_int(ledger.get("auto_close_trigger_count")) or 0,
                _safe_int(ledger.get("auto_close_not_completed_count")) or 0,
                _safe_int(ledger.get("close_pending_count")) or 0,
                _safe_int(ledger.get("partial_close_count")) or 0,
                _safe_int(ledger.get("recovery_event_count")) or 0,
                _safe_int(ledger.get("emergency_close_count")) or 0,
                _safe_int(ledger.get("detected_close_count")) or 0,
                _safe_int(ledger.get("close_noop_count")) or 0,
                _safe_int(ledger.get("trade_count")) or 0,
                _safe_int(ledger.get("long_trade_count")) or 0,
                _safe_int(ledger.get("short_trade_count")) or 0,
                _safe_float(ledger.get("total_fee_usd")) or 0.0,
                _safe_float(ledger.get("long_fee_usd")) or 0.0,
                _safe_float(ledger.get("short_fee_usd")) or 0.0,
                _safe_float(ledger.get("total_realized_pnl_usd")) or 0.0,
                _safe_float(ledger.get("long_realized_pnl_usd")) or 0.0,
                _safe_float(ledger.get("short_realized_pnl_usd")) or 0.0,
                json_dumps(ledger.get("raw", ledger)),
            )
        )

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        cur.close()


def fetch_pair_ledger_rows(
    conn,
    *,
    limit: int = 50,
    opened_after_ms: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        pair_id, symbol, long_exchange, short_exchange,
        opened_time_ms, closed_time_ms, last_event_time_ms,
        duration_sec, status, final_event_type, close_reason,
        requested_qty, pair_qty, long_entry_price, short_entry_price,
        entry_net_edge_usd, expected_funding_pnl_usd, funding_long_rate, funding_short_rate,
        last_hold_decision, last_hold_pnl_usd, last_close_pnl_usd, last_close_edge_usd,
        auto_close_trigger_count, auto_close_not_completed_count, close_pending_count, partial_close_count,
        recovery_event_count, emergency_close_count, detected_close_count, close_noop_count,
        trade_count, long_trade_count, short_trade_count,
        total_fee_usd, long_fee_usd, short_fee_usd,
        total_realized_pnl_usd, long_realized_pnl_usd, short_realized_pnl_usd,
        raw_json
    FROM pair_ledger
    WHERE (? IS NULL OR opened_time_ms >= ?)
    ORDER BY opened_time_ms DESC, pair_id DESC
    LIMIT ?
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (opened_after_ms, opened_after_ms, int(limit)))
        columns = [desc[0] for desc in cur.description]
        rows: list[dict[str, Any]] = []
        for raw_row in cur.fetchall():
            row = dict(zip(columns, raw_row))
            row["raw"] = json_loads_or_none(row.get("raw_json"))
            rows.append(row)
        return rows
    finally:
        cur.close()


def insert_cashflow_row(conn, row: dict[str, Any]) -> int:
    sql = """
    INSERT INTO cashflow_ledger (
        movement_key, flow_type, status, reconcile_status,
        asset_code, amount, fee_amount,
        exchange_name, from_exchange_name, to_exchange_name,
        channel, network, txid,
        event_time_ms, memo, raw_json,
        created_at_ms, updated_at_ms
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    now_ms = current_utc_ms()
    values = (
        _string_or_none(row.get("movement_key")),
        _string_or_none(row.get("flow_type")),
        _string_or_none(row.get("status")),
        _string_or_none(row.get("reconcile_status")) or "pending",
        (_string_or_none(row.get("asset_code")) or "").upper(),
        abs(_safe_float(row.get("amount")) or 0.0),
        _safe_float(row.get("fee_amount")),
        _string_or_none(row.get("exchange_name")),
        _string_or_none(row.get("from_exchange_name")),
        _string_or_none(row.get("to_exchange_name")),
        _string_or_none(row.get("channel")),
        _string_or_none(row.get("network")),
        _string_or_none(row.get("txid")),
        _safe_int(row.get("event_time_ms")) or now_ms,
        _string_or_none(row.get("memo")),
        json_dumps(row.get("raw_json") if row.get("raw_json") is not None else row),
        _safe_int(row.get("created_at_ms")) or now_ms,
        _safe_int(row.get("updated_at_ms")) or now_ms,
    )

    cur = conn.cursor()
    try:
        cur.execute(sql, values)
        conn.commit()
        return 1
    finally:
        cur.close()


def _cashflow_row_from_cursor(cur, raw_row: tuple[Any, ...]) -> dict[str, Any]:
    columns = [desc[0] for desc in cur.description]
    row = dict(zip(columns, raw_row))
    row["raw"] = json_loads_or_none(row.get("raw_json"))
    return row


def fetch_cashflow_row_by_movement_key(conn, movement_key: str) -> dict[str, Any] | None:
    sql = """
    SELECT
        id, movement_key, flow_type, status, reconcile_status,
        asset_code, amount, fee_amount,
        exchange_name, from_exchange_name, to_exchange_name,
        channel, network, txid,
        event_time_ms, memo, raw_json,
        created_at_ms, updated_at_ms
    FROM cashflow_ledger
    WHERE movement_key = ?
    LIMIT 1
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (movement_key,))
        raw_row = cur.fetchone()
        if raw_row is None:
            return None
        return _cashflow_row_from_cursor(cur, raw_row)
    finally:
        cur.close()


def fetch_cashflow_row_by_id(conn, row_id: int) -> dict[str, Any] | None:
    sql = """
    SELECT
        id, movement_key, flow_type, status, reconcile_status,
        asset_code, amount, fee_amount,
        exchange_name, from_exchange_name, to_exchange_name,
        channel, network, txid,
        event_time_ms, memo, raw_json,
        created_at_ms, updated_at_ms
    FROM cashflow_ledger
    WHERE id = ?
    LIMIT 1
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (int(row_id),))
        raw_row = cur.fetchone()
        if raw_row is None:
            return None
        return _cashflow_row_from_cursor(cur, raw_row)
    finally:
        cur.close()


def fetch_cashflow_rows(
    conn,
    *,
    limit: int = 100,
    flow_type: str | None = None,
    status: str | None = None,
    reconcile_status: str | None = None,
    channel: str | None = None,
    asset_code: str | None = None,
    exchange_name: str | None = None,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        id, movement_key, flow_type, status, reconcile_status,
        asset_code, amount, fee_amount,
        exchange_name, from_exchange_name, to_exchange_name,
        channel, network, txid,
        event_time_ms, memo, raw_json,
        created_at_ms, updated_at_ms
    FROM cashflow_ledger
    WHERE (? IS NULL OR flow_type = ?)
      AND (? IS NULL OR status = ?)
      AND (? IS NULL OR reconcile_status = ?)
      AND (? IS NULL OR channel = ?)
      AND (? IS NULL OR asset_code = ?)
      AND (? IS NULL OR exchange_name = ? OR from_exchange_name = ? OR to_exchange_name = ?)
      AND (? IS NULL OR event_time_ms >= ?)
      AND (? IS NULL OR event_time_ms <= ?)
    ORDER BY event_time_ms DESC, id DESC
    LIMIT ?
    """

    cur = conn.cursor()
    try:
        normalized_asset = (_string_or_none(asset_code) or "").upper() or None
        normalized_exchange = _string_or_none(exchange_name)
        cur.execute(
            sql,
            (
                _string_or_none(flow_type),
                _string_or_none(flow_type),
                _string_or_none(status),
                _string_or_none(status),
                _string_or_none(reconcile_status),
                _string_or_none(reconcile_status),
                _string_or_none(channel),
                _string_or_none(channel),
                normalized_asset,
                normalized_asset,
                normalized_exchange,
                normalized_exchange,
                normalized_exchange,
                normalized_exchange,
                _safe_int(start_time_ms),
                _safe_int(start_time_ms),
                _safe_int(end_time_ms),
                _safe_int(end_time_ms),
                int(limit),
            ),
        )
        return [_cashflow_row_from_cursor(cur, raw_row) for raw_row in cur.fetchall()]
    finally:
        cur.close()


def update_cashflow_status(
    conn,
    *,
    movement_key: str | None = None,
    row_id: int | None = None,
    status: str,
) -> int:
    if movement_key is None and row_id is None:
        raise ValueError("movement_key or row_id is required")

    sql = """
    UPDATE cashflow_ledger
    SET status = ?,
        updated_at_ms = ?
    WHERE (? IS NULL OR movement_key = ?)
      AND (? IS NULL OR id = ?)
    """

    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (
                status,
                current_utc_ms(),
                _string_or_none(movement_key),
                _string_or_none(movement_key),
                _safe_int(row_id),
                _safe_int(row_id),
            ),
        )
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()


def mark_cashflow_reconcile_result(
    conn,
    *,
    movement_key: str | None = None,
    row_id: int | None = None,
    reconcile_status: str,
) -> int:
    if movement_key is None and row_id is None:
        raise ValueError("movement_key or row_id is required")

    sql = """
    UPDATE cashflow_ledger
    SET reconcile_status = ?,
        updated_at_ms = ?
    WHERE (? IS NULL OR movement_key = ?)
      AND (? IS NULL OR id = ?)
    """

    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (
                reconcile_status,
                current_utc_ms(),
                _string_or_none(movement_key),
                _string_or_none(movement_key),
                _safe_int(row_id),
                _safe_int(row_id),
            ),
        )
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()


def fetch_balance_snapshot_before_after(
    conn,
    *,
    exchange_name: str,
    event_time_ms: int,
) -> dict[str, dict[str, Any] | None]:
    from src.ops.persistence.account_context import resolve_account_label, resolve_logic_name

    account_label = resolve_account_label()
    logic = resolve_logic_name()
    base_sql = """
    SELECT
        account_label, logic, exchange_name, bucket_start_ms, desktop_time_ms, server_time_ms,
        cash_usdt, available_cash_usdt, locked_cash_usdt, total_equity_usdt, unrealized_pnl_usdt,
        raw_json
    FROM balance_snapshot
    WHERE account_label = ?
      AND logic = ?
      AND exchange_name = ?
      AND desktop_time_ms {operator} ?
    ORDER BY desktop_time_ms {direction}, bucket_start_ms {direction}
    LIMIT 1
    """

    def _fetch(operator: str, direction: str) -> dict[str, Any] | None:
        cur = conn.cursor()
        try:
            cur.execute(
                base_sql.format(operator=operator, direction=direction),
                (account_label, logic, exchange_name, int(event_time_ms)),
            )
            raw_row = cur.fetchone()
            if raw_row is None:
                return None
            columns = [desc[0] for desc in cur.description]
            row = dict(zip(columns, raw_row))
            row["raw"] = json_loads_or_none(row.get("raw_json"))
            return row
        finally:
            cur.close()

    return {
        "before": _fetch("<=", "DESC"),
        "after": _fetch(">=", "ASC"),
    }
