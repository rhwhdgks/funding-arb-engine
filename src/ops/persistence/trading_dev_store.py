from __future__ import annotations

from typing import Any

from .account_context import resolve_account_label, resolve_logic_name


OPEN_ORDER_STATUSES = {
    "ACKNOWLEDGED",
    "LIVE",
    "NEW",
    "OPEN",
    "PARTIALLY_FILLED",
    "SUBMITTED",
    "UPDATED",
}
TERMINAL_ORDER_STATUSES = {
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "FAILED",
    "FILLED",
    "MISSING",
    "NOT_EXIST",
    "REJECTED",
}


def _row_account_label(row: dict[str, Any] | None = None) -> str:
    value = (row or {}).get("account_label")
    return str(value).strip().lower() if value else resolve_account_label()


def _row_logic(row: dict[str, Any] | None = None) -> str:
    value = (row or {}).get("logic")
    return str(value).strip() if value else resolve_logic_name()


def upsert_trade_history_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO trade_history (
        account_label, logic,
        uuid, exchange_name, symbol,
        order_uuid, order_id, client_order_id, trade_id,
        side, role, position_side,
        price, qty, quote_qty,
        fee, fee_ccy, realized_pnl_usdt,
        desktop_time_ms, server_time_ms, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        account_label = VALUES(account_label),
        logic = VALUES(logic),
        order_uuid = VALUES(order_uuid),
        order_id = VALUES(order_id),
        client_order_id = VALUES(client_order_id),
        trade_id = VALUES(trade_id),
        side = VALUES(side),
        role = VALUES(role),
        position_side = VALUES(position_side),
        price = VALUES(price),
        qty = VALUES(qty),
        quote_qty = VALUES(quote_qty),
        fee = VALUES(fee),
        fee_ccy = VALUES(fee_ccy),
        realized_pnl_usdt = VALUES(realized_pnl_usdt),
        desktop_time_ms = VALUES(desktop_time_ms),
        server_time_ms = VALUES(server_time_ms),
        raw_json = VALUES(raw_json)
    """

    values = [
        (
            _row_account_label(row),
            _row_logic(row),
            row["uuid"],
            row["exchange_name"],
            row["symbol"],
            row.get("order_uuid"),
            row.get("order_id"),
            row.get("client_order_id"),
            row.get("trade_id"),
            row["side"],
            row.get("role"),
            row.get("position_side"),
            row["price"],
            row["qty"],
            row.get("quote_qty"),
            row.get("fee"),
            row.get("fee_ccy"),
            row.get("realized_pnl_usdt"),
            row["desktop_time_ms"],
            row.get("server_time_ms"),
            row.get("raw_json"),
        )
        for row in rows
    ]

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)


def upsert_order_history_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO order_history (
        account_label, logic,
        uuid, exchange_name, symbol,
        order_id, client_order_id,
        side, position_side, reduce_only, order_type,
        price, qty, executed_qty, avg_price,
        status, order_start, order_end, desktop_time_ms, server_time_ms, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        account_label = VALUES(account_label),
        logic = VALUES(logic),
        order_id = COALESCE(VALUES(order_id), order_id),
        client_order_id = COALESCE(VALUES(client_order_id), client_order_id),
        side = IF(VALUES(side) <> 'unknown', VALUES(side), side),
        position_side = COALESCE(VALUES(position_side), position_side),
        reduce_only = VALUES(reduce_only),
        order_type = IF(VALUES(order_type) <> 'unknown', VALUES(order_type), order_type),
        price = COALESCE(VALUES(price), price),
        qty = IF(VALUES(qty) > 0, VALUES(qty), qty),
        executed_qty = GREATEST(executed_qty, VALUES(executed_qty)),
        avg_price = COALESCE(VALUES(avg_price), avg_price),
        status = IF(
            UPPER(COALESCE(status, '')) IN ('CANCELED', 'CANCELLED', 'EXPIRED', 'FAILED', 'FILLED', 'MISSING', 'NOT_EXIST', 'REJECTED')
            AND UPPER(COALESCE(VALUES(status), '')) IN ('ACKNOWLEDGED', 'LIVE', 'NEW', 'OPEN', 'PARTIALLY_FILLED', 'SUBMITTED', 'UPDATED'),
            status,
            VALUES(status)
        ),
        order_start = LEAST(order_start, VALUES(order_start)),
        order_end = GREATEST(order_end, VALUES(order_end)),
        desktop_time_ms = VALUES(desktop_time_ms),
        server_time_ms = COALESCE(VALUES(server_time_ms), server_time_ms),
        raw_json = VALUES(raw_json)
    """

    values = [
        (
            _row_account_label(row),
            _row_logic(row),
            row["uuid"],
            row["exchange_name"],
            row["symbol"],
            row.get("order_id"),
            row.get("client_order_id"),
            row["side"],
            row.get("position_side"),
            row["reduce_only"],
            row["order_type"],
            row.get("price"),
            row["qty"],
            row["executed_qty"],
            row.get("avg_price"),
            row["status"],
            row["order_start"],
            row["order_end"],
            row["desktop_time_ms"],
            row.get("server_time_ms"),
            row.get("raw_json"),
        )
        for row in rows
    ]

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)


def mark_absent_open_order_rows(
    conn,
    *,
    exchange_name: str,
    symbol: str | None = None,
    event_time_ms: int | None = None,
    terminal_status: str = "NOT_EXIST",
) -> int:
    timestamp_ms = int(event_time_ms or 0)
    if timestamp_ms <= 0:
        import time

        timestamp_ms = int(time.time() * 1000)

    normalized_symbol = str(symbol or "").upper()
    terminal_status = str(terminal_status or "NOT_EXIST").upper()
    account_label = resolve_account_label()
    logic = resolve_logic_name()
    open_statuses = tuple(sorted(OPEN_ORDER_STATUSES))
    placeholders = ", ".join("?" for _ in open_statuses)
    params: list[Any] = [terminal_status, timestamp_ms, timestamp_ms, account_label, logic, exchange_name, *open_statuses]
    symbol_filter = ""
    if normalized_symbol and normalized_symbol != "ALL":
        symbol_filter = " AND symbol = ?"
        params.append(normalized_symbol)

    sql = f"""
    UPDATE order_history
    SET
        status = ?,
        order_end = GREATEST(order_end, ?),
        desktop_time_ms = ?,
        raw_json = COALESCE(raw_json, '{{}}')
    WHERE account_label = ?
      AND logic = ?
      AND exchange_name = ?
      AND UPPER(status) IN ({placeholders})
      {symbol_filter}
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        affected = cur.rowcount
        conn.commit()
    finally:
        cur.close()
    return int(affected or 0)


def upsert_balance_snapshot_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO balance_snapshot (
        account_label, logic,
        exchange_name, bucket_start_ms, desktop_time_ms, server_time_ms,
        tracked_symbols_json, symbol_balances_json,
        cash_usdt, available_cash_usdt, locked_cash_usdt, total_equity_usdt, unrealized_pnl_usdt,
        raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        account_label = VALUES(account_label),
        logic = VALUES(logic),
        desktop_time_ms = VALUES(desktop_time_ms),
        server_time_ms = VALUES(server_time_ms),
        tracked_symbols_json = VALUES(tracked_symbols_json),
        symbol_balances_json = VALUES(symbol_balances_json),
        cash_usdt = VALUES(cash_usdt),
        available_cash_usdt = VALUES(available_cash_usdt),
        locked_cash_usdt = VALUES(locked_cash_usdt),
        total_equity_usdt = VALUES(total_equity_usdt),
        unrealized_pnl_usdt = VALUES(unrealized_pnl_usdt),
        raw_json = VALUES(raw_json)
    """

    values = [
        (
            _row_account_label(row),
            _row_logic(row),
            row["exchange_name"],
            row["bucket_start_ms"],
            row["desktop_time_ms"],
            row.get("server_time_ms"),
            row["tracked_symbols_json"],
            row["symbol_balances_json"],
            row["cash_usdt"],
            row["available_cash_usdt"],
            row["locked_cash_usdt"],
            row["total_equity_usdt"],
            row["unrealized_pnl_usdt"],
            row.get("raw_json"),
        )
        for row in rows
    ]

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)


def upsert_funding_rate_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO funding_rate (
        exchange_name, symbol, bucket_start_ms, desktop_time_ms, server_time_ms,
        funding_rate, next_funding_time_ms, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        desktop_time_ms = VALUES(desktop_time_ms),
        server_time_ms = VALUES(server_time_ms),
        funding_rate = VALUES(funding_rate),
        next_funding_time_ms = VALUES(next_funding_time_ms),
        raw_json = VALUES(raw_json)
    """

    values = [
        (
            row["exchange_name"],
            row["symbol"],
            row["bucket_start_ms"],
            row["desktop_time_ms"],
            row.get("server_time_ms"),
            row["funding_rate"],
            row.get("next_funding_time_ms"),
            row.get("raw_json"),
        )
        for row in rows
    ]

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)


def upsert_symbol_price_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO symbol_price (
        exchange_name, symbol, bucket_start_ms, desktop_time_ms, server_time_ms,
        best_bid, best_ask, bid, ask
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
        desktop_time_ms = VALUES(desktop_time_ms),
        server_time_ms = VALUES(server_time_ms),
        best_bid = VALUES(best_bid),
        best_ask = VALUES(best_ask),
        bid = VALUES(bid),
        ask = VALUES(ask)
    """

    values = [
        (
            row["exchange_name"],
            row["symbol"],
            row["bucket_start_ms"],
            row["desktop_time_ms"],
            row.get("server_time_ms"),
            row["best_bid"],
            row["best_ask"],
            row.get("bid"),
            row.get("ask"),
        )
        for row in rows
    ]

    cur = conn.cursor()
    try:
        cur.executemany(sql, values)
        conn.commit()
    finally:
        cur.close()
    return len(values)
