#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


OPEN_ORDER_STATUSES = (
    "ACKNOWLEDGED",
    "LIVE",
    "NEW",
    "OPEN",
    "PARTIALLY_FILLED",
    "SUBMITTED",
    "UPDATED",
)


def now_utc_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int | None) -> str:
    if not ms:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def load_env(env_file: Path | None) -> None:
    if env_file:
        load_dotenv(env_file, override=True)
    else:
        fallback = os.getenv("STRATEGY_ENV_FILE")
        if fallback:
            load_dotenv(fallback, override=True)
        else:
            load_dotenv(PROJECT_ROOT / ".env")


def load_run_meta(meta_path: Path | None) -> dict[str, Any]:
    if not meta_path or not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_since_ms(args: argparse.Namespace, run_meta: dict[str, Any]) -> int:
    if args.since_ms is not None:
        return int(args.since_ms)
    if run_meta.get("started_at_ms"):
        return int(run_meta["started_at_ms"])
    hours = float(args.hours or 24.0)
    return now_utc_ms() - int(hours * 3_600_000)


def fetch_all_dict(cur) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def query_dicts(conn, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return fetch_all_dict(cur)
    finally:
        cur.close()


def query_scalar(conn, sql: str, params: tuple[Any, ...] = ()) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        cur.close()


def placeholders(values: tuple[str, ...]) -> str:
    return ", ".join("?" for _ in values)


def check(status: str, name: str, detail: str) -> dict[str, str]:
    return {"status": status, "name": name, "detail": detail}


def build_report(conn, *, since_ms: int, run_meta: dict[str, Any]) -> dict[str, Any]:
    from src.ops.persistence.account_context import resolve_account_label, resolve_logic_name

    account_label = resolve_account_label()
    logic = resolve_logic_name()
    open_placeholders = placeholders(OPEN_ORDER_STATUSES)
    open_params = tuple(OPEN_ORDER_STATUSES)

    run_open_like = query_dicts(
        conn,
        f"""
        SELECT exchange_name, symbol, status, COUNT(*) AS row_count
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
          AND UPPER(status) IN ({open_placeholders})
        GROUP BY exchange_name, symbol, status
        ORDER BY exchange_name, symbol, status
        """,
        (account_label, logic, since_ms, *open_params),
    )
    all_open_like = query_dicts(
        conn,
        f"""
        SELECT exchange_name, symbol, status, COUNT(*) AS row_count
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND UPPER(status) IN ({open_placeholders})
        GROUP BY exchange_name, symbol, status
        ORDER BY exchange_name, symbol, status
        """,
        (account_label, logic, *open_params),
    )

    order_status_counts = query_dicts(
        conn,
        """
        SELECT exchange_name, status, COUNT(*) AS row_count
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
        GROUP BY exchange_name, status
        ORDER BY exchange_name, status
        """,
        (account_label, logic, since_ms),
    )
    trade_counts = query_dicts(
        conn,
        """
        SELECT
            exchange_name,
            symbol,
            COUNT(*) AS trade_count,
            ROUND(SUM(ABS(COALESCE(quote_qty, price * qty))), 8) AS abs_quote_qty_sum,
            MIN(desktop_time_ms) AS first_trade_ms,
            MAX(desktop_time_ms) AS last_trade_ms
        FROM trade_history
        WHERE account_label = ?
          AND logic = ?
          AND desktop_time_ms >= ?
        GROUP BY exchange_name, symbol
        ORDER BY symbol, exchange_name
        """,
        (account_label, logic, since_ms),
    )

    order_rows = query_scalar(conn, "SELECT COUNT(*) FROM order_history WHERE account_label = ? AND logic = ? AND order_start >= ?", (account_label, logic, since_ms))
    trade_rows = query_scalar(conn, "SELECT COUNT(*) FROM trade_history WHERE account_label = ? AND logic = ? AND desktop_time_ms >= ?", (account_label, logic, since_ms))
    run_open_like_count = sum(int(row["row_count"] or 0) for row in run_open_like)
    all_open_like_count = sum(int(row["row_count"] or 0) for row in all_open_like)

    orders_missing_external_id = query_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
          AND COALESCE(order_id, '') = ''
          AND COALESCE(client_order_id, '') = ''
        """,
        (account_label, logic, since_ms),
    )
    duplicate_order_ids = query_dicts(
        conn,
        """
        SELECT exchange_name, order_id, COUNT(*) AS row_count
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
          AND COALESCE(order_id, '') <> ''
        GROUP BY exchange_name, order_id
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC, exchange_name, order_id
        LIMIT 20
        """,
        (account_label, logic, since_ms),
    )
    duplicate_client_order_ids = query_dicts(
        conn,
        """
        SELECT exchange_name, client_order_id, COUNT(*) AS row_count
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
          AND COALESCE(client_order_id, '') <> ''
        GROUP BY exchange_name, client_order_id
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC, exchange_name, client_order_id
        LIMIT 20
        """,
        (account_label, logic, since_ms),
    )
    executed_qty_over_qty = query_dicts(
        conn,
        """
        SELECT uuid, exchange_name, symbol, side, qty, executed_qty, status
        FROM order_history
        WHERE account_label = ?
          AND logic = ?
          AND order_start >= ?
          AND executed_qty > qty + 0.000000000001
        ORDER BY order_end DESC
        LIMIT 20
        """,
        (account_label, logic, since_ms),
    )
    filled_orders_missing_trade_rows = query_dicts(
        conn,
        """
        SELECT
            o.exchange_name,
            o.symbol,
            COUNT(*) AS row_count,
            ROUND(SUM(ABS(COALESCE(o.avg_price, o.price, 0) * o.executed_qty)), 8) AS exec_notional_sum,
            ROUND(SUM(ABS(o.executed_qty)), 8) AS executed_qty_sum
        FROM order_history o
        WHERE o.account_label = ?
          AND o.logic = ?
          AND o.order_start >= ?
          AND o.executed_qty > 0
          AND NOT EXISTS (
              SELECT 1
              FROM trade_history t
              WHERE t.account_label = o.account_label
                AND t.logic = o.logic
                AND t.exchange_name = o.exchange_name
                AND t.symbol = o.symbol
                AND t.desktop_time_ms >= ?
                AND (
                    (t.order_uuid IS NOT NULL AND t.order_uuid <> '' AND t.order_uuid = o.uuid)
                    OR (t.order_id IS NOT NULL AND t.order_id <> '' AND o.order_id IS NOT NULL AND o.order_id <> '' AND t.order_id = o.order_id)
                    OR (t.client_order_id IS NOT NULL AND t.client_order_id <> '' AND o.client_order_id IS NOT NULL AND o.client_order_id <> '' AND t.client_order_id = o.client_order_id)
                )
          )
        GROUP BY o.exchange_name, o.symbol
        ORDER BY exec_notional_sum DESC, o.exchange_name, o.symbol
        LIMIT 40
        """,
        (account_label, logic, since_ms, since_ms),
    )
    filled_orders_missing_trade_count = sum(int(row["row_count"] or 0) for row in filled_orders_missing_trade_rows)

    trades_missing_order_ref = query_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM trade_history
        WHERE account_label = ?
          AND logic = ?
          AND desktop_time_ms >= ?
          AND COALESCE(order_uuid, '') = ''
          AND COALESCE(order_id, '') = ''
          AND COALESCE(client_order_id, '') = ''
        """,
        (account_label, logic, since_ms),
    )
    trades_missing_trade_id = query_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM trade_history
        WHERE account_label = ?
          AND logic = ?
          AND desktop_time_ms >= ?
          AND COALESCE(trade_id, '') = ''
        """,
        (account_label, logic, since_ms),
    )
    duplicate_trade_ids = query_dicts(
        conn,
        """
        SELECT exchange_name, symbol, trade_id, COUNT(*) AS row_count
        FROM trade_history
        WHERE account_label = ?
          AND logic = ?
          AND desktop_time_ms >= ?
          AND COALESCE(trade_id, '') <> ''
        GROUP BY exchange_name, symbol, trade_id
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC, exchange_name, symbol, trade_id
        LIMIT 20
        """,
        (account_label, logic, since_ms),
    )
    trades_unlinked_to_order = query_scalar(
        conn,
        """
        SELECT COUNT(DISTINCT t.uuid)
        FROM trade_history t
        LEFT JOIN order_history ou
          ON t.order_uuid IS NOT NULL
         AND t.order_uuid <> ''
         AND t.account_label = ou.account_label
         AND t.logic = ou.logic
         AND t.order_uuid = ou.uuid
        LEFT JOIN order_history oo
          ON t.order_id IS NOT NULL
         AND t.order_id <> ''
         AND t.account_label = oo.account_label
         AND t.logic = oo.logic
         AND t.exchange_name = oo.exchange_name
         AND t.order_id = oo.order_id
        LEFT JOIN order_history oc
          ON t.client_order_id IS NOT NULL
         AND t.client_order_id <> ''
         AND t.account_label = oc.account_label
         AND t.logic = oc.logic
         AND t.exchange_name = oc.exchange_name
         AND t.client_order_id = oc.client_order_id
        WHERE t.account_label = ?
          AND t.logic = ?
          AND t.desktop_time_ms >= ?
          AND ou.uuid IS NULL
          AND oo.uuid IS NULL
          AND oc.uuid IS NULL
        """,
        (account_label, logic, since_ms),
    )

    symbol_exchange_notional: dict[str, dict[str, float]] = defaultdict(dict)
    for row in trade_counts:
        symbol_exchange_notional[str(row["symbol"])][str(row["exchange_name"])] = float(row["abs_quote_qty_sum"] or 0.0)
    notional_balance_warnings = []
    for symbol, by_exchange in sorted(symbol_exchange_notional.items()):
        if len(by_exchange) < 2:
            continue
        values = list(by_exchange.values())
        max_value = max(values)
        min_value = min(values)
        if max_value >= 100.0 and (max_value - min_value) / max_value > 0.20:
            notional_balance_warnings.append(
                {
                    "symbol": symbol,
                    "exchange_notional": by_exchange,
                    "diff_ratio": round((max_value - min_value) / max_value, 6),
                }
            )

    checks = [
        check("PASS" if order_rows > 0 else "WARN", "order_rows_present", f"rows={order_rows}"),
        check("PASS" if trade_rows > 0 else "WARN", "trade_rows_present", f"rows={trade_rows}"),
        check("PASS" if run_open_like_count == 0 else "FAIL", "run_open_like_orders_zero", f"rows={run_open_like_count}"),
        check("PASS" if all_open_like_count == 0 else "WARN", "all_open_like_orders_zero", f"rows={all_open_like_count}"),
        check(
            "PASS" if orders_missing_external_id == 0 else "WARN",
            "orders_have_external_id_or_client_id",
            f"rows={orders_missing_external_id}",
        ),
        check(
            "PASS" if not duplicate_order_ids else "WARN",
            "duplicate_order_ids_zero",
            f"groups={len(duplicate_order_ids)}",
        ),
        check(
            "PASS" if not duplicate_client_order_ids else "WARN",
            "duplicate_client_order_ids_zero",
            f"groups={len(duplicate_client_order_ids)}",
        ),
        check(
            "PASS" if not executed_qty_over_qty else "FAIL",
            "executed_qty_not_greater_than_qty",
            f"rows={len(executed_qty_over_qty)}",
        ),
        check(
            "PASS" if filled_orders_missing_trade_count == 0 else "WARN",
            "filled_orders_have_trade_rows",
            f"rows={filled_orders_missing_trade_count}",
        ),
        check(
            "PASS" if trades_missing_order_ref == 0 else "WARN",
            "trades_have_order_reference",
            f"rows={trades_missing_order_ref}",
        ),
        check(
            "PASS" if trades_missing_trade_id == 0 else "WARN",
            "trades_have_trade_id",
            f"rows={trades_missing_trade_id}",
        ),
        check(
            "PASS" if not duplicate_trade_ids else "WARN",
            "duplicate_trade_ids_zero",
            f"groups={len(duplicate_trade_ids)}",
        ),
        check(
            "PASS" if trades_unlinked_to_order == 0 else "WARN",
            "trades_link_to_order_history",
            f"rows={trades_unlinked_to_order}",
        ),
        check(
            "PASS" if not notional_balance_warnings else "WARN",
            "exchange_symbol_trade_notional_roughly_balanced",
            f"symbols={len(notional_balance_warnings)}",
        ),
    ]
    fail_count = sum(1 for item in checks if item["status"] == "FAIL")
    warn_count = sum(1 for item in checks if item["status"] == "WARN")
    verdict = "FAIL" if fail_count else ("WARN" if warn_count else "PASS")

    return {
        "generated_at_ms": now_utc_ms(),
        "since_ms": since_ms,
        "since_utc": iso_utc(since_ms),
        "run_meta": run_meta,
        "account_label": account_label,
        "logic": logic,
        "verdict": verdict,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "summary": {
            "order_rows": order_rows,
            "trade_rows": trade_rows,
            "run_open_like_order_rows": run_open_like_count,
            "all_open_like_order_rows": all_open_like_count,
            "orders_missing_external_id": orders_missing_external_id,
            "filled_orders_missing_trade_rows": filled_orders_missing_trade_count,
            "trades_missing_order_ref": trades_missing_order_ref,
            "trades_unlinked_to_order": trades_unlinked_to_order,
        },
        "order_status_counts": order_status_counts,
        "trade_counts": trade_counts,
        "run_open_like_orders": run_open_like,
        "all_open_like_orders": all_open_like,
        "duplicate_order_ids": duplicate_order_ids,
        "duplicate_client_order_ids": duplicate_client_order_ids,
        "executed_qty_over_qty": executed_qty_over_qty,
        "filled_orders_missing_trade_rows": filled_orders_missing_trade_rows,
        "duplicate_trade_ids": duplicate_trade_ids,
        "notional_balance_warnings": notional_balance_warnings,
    }


def markdown_table(rows: list[dict[str, Any]], columns: list[str], *, limit: int = 40) -> list[str]:
    if not rows:
        return ["- none"]
    selected = rows[:limit]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in selected:
        values = [str(row.get(col, "")) for col in columns]
        lines.append("| " + " | ".join(value.replace("\n", " ") for value in values) + " |")
    if len(rows) > limit:
        lines.append(f"- truncated: showing {limit} of {len(rows)} rows")
    return lines


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Order/Trade DB Reconcile Report")
    lines.append("")
    lines.append("## Verdict")
    lines.append(f"- result: **{report['verdict']}**")
    lines.append(f"- failed_checks: {report['fail_count']}")
    lines.append(f"- warning_checks: {report['warn_count']}")
    lines.append(f"- generated_at_utc: {iso_utc(report['generated_at_ms'])}")
    lines.append(f"- since_utc: {report['since_utc']}")
    if report.get("run_meta", {}).get("started_at_iso"):
        lines.append(f"- run_started_at_utc: {report['run_meta'].get('started_at_iso')}")
    lines.append("")
    lines.append("## Checks")
    for item in report["checks"]:
        lines.append(f"- {item['status']} {item['name']}: {item['detail']}")
    lines.append("")
    lines.append("## Summary")
    for key, value in report["summary"].items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Order Status Counts")
    lines.extend(markdown_table(report["order_status_counts"], ["exchange_name", "status", "row_count"]))
    lines.append("")
    lines.append("## Trade Counts")
    lines.extend(markdown_table(report["trade_counts"], ["exchange_name", "symbol", "trade_count", "abs_quote_qty_sum"]))
    lines.append("")
    lines.append("## Run Open-Like Orders")
    lines.extend(markdown_table(report["run_open_like_orders"], ["exchange_name", "symbol", "status", "row_count"]))
    lines.append("")
    lines.append("## All Open-Like Orders")
    lines.extend(markdown_table(report["all_open_like_orders"], ["exchange_name", "symbol", "status", "row_count"]))
    lines.append("")
    lines.append("## Duplicate Order IDs")
    lines.extend(markdown_table(report["duplicate_order_ids"], ["exchange_name", "order_id", "row_count"]))
    lines.append("")
    lines.append("## Duplicate Client Order IDs")
    lines.extend(markdown_table(report["duplicate_client_order_ids"], ["exchange_name", "client_order_id", "row_count"]))
    lines.append("")
    lines.append("## Executed Qty Greater Than Qty")
    lines.extend(markdown_table(report["executed_qty_over_qty"], ["uuid", "exchange_name", "symbol", "side", "qty", "executed_qty", "status"]))
    lines.append("")
    lines.append("## Filled Orders Missing Trade Rows")
    lines.extend(markdown_table(report["filled_orders_missing_trade_rows"], ["exchange_name", "symbol", "row_count", "exec_notional_sum", "executed_qty_sum"]))
    lines.append("")
    lines.append("## Duplicate Trade IDs")
    lines.extend(markdown_table(report["duplicate_trade_ids"], ["exchange_name", "symbol", "trade_id", "row_count"]))
    lines.append("")
    lines.append("## Notional Balance Warnings")
    lines.extend(markdown_table(report["notional_balance_warnings"], ["symbol", "exchange_notional", "diff_ratio"]))
    lines.append("")
    return "\n".join(lines)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check trading_dev order/trade DB consistency for the latest run.")
    parser.add_argument("--env-file", default=os.getenv("STRATEGY_ENV_FILE") or ".env")
    parser.add_argument("--meta", default="test_server/logs/strategy_live.run.json")
    parser.add_argument("--since-ms", type=int)
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--output", default="test_server/logs/order_trade_reconcile_latest.md")
    parser.add_argument("--json-output", default="test_server/logs/order_trade_reconcile_latest.json")
    args = parser.parse_args()

    env_path = PROJECT_ROOT / args.env_file if args.env_file else None
    load_env(env_path)
    meta_path = PROJECT_ROOT / args.meta if args.meta else None
    run_meta = load_run_meta(meta_path)
    since_ms = resolve_since_ms(args, run_meta)

    from src.ops.persistence.db import get_db_connection

    conn = get_db_connection()
    try:
        report = build_report(conn, since_ms=since_ms, run_meta=run_meta)
    finally:
        conn.close()

    output_path = PROJECT_ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(report), encoding="utf-8")
    write_json(PROJECT_ROOT / args.json_output, report)
    print(f"verdict={report['verdict']} failed={report['fail_count']} warnings={report['warn_count']}")
    print(f"wrote {output_path}")
    return 1 if report["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
