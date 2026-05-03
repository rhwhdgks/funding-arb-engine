#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
env_file = os.getenv("STRATEGY_ENV_FILE")
if env_file:
    load_dotenv(env_file, override=True)
else:
    load_dotenv()

from src.core.exchange_registry import SUPPORTED_EXCHANGES
from src.ops.persistence.live_report import collect_exchange_report


def parse_csv_list(raw: str | None, fallback: list[str] | None = None) -> list[str]:
    if not raw:
        return list(fallback or [])
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def build_default_symbols() -> list[str]:
    symbols = parse_csv_list(os.getenv("SYMBOLS"))
    if symbols:
        return symbols
    symbol = os.getenv("SYMBOL", "BTC").strip().upper()
    return [symbol]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal live OMS/MD CLI report.")
    parser.add_argument("--exchanges", default=os.getenv("TARGET_EXCHANGES") or os.getenv("TARGET_EXCHANGE", "okx"))
    parser.add_argument("--symbols", default=os.getenv("SYMBOLS"))
    parser.add_argument("--wait-ws-sec", type=float, default=float(os.getenv("REPORT_WAIT_WS_SEC", "6")))
    parser.add_argument("--trade-limit", type=int, default=int(os.getenv("REPORT_TRADE_LIMIT", "5")))
    parser.add_argument("--json", action="store_true", help="Print the raw report dict instead of the formatted view.")
    return parser

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _fmt_num(value: Any, decimals: int = 4) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return str(value)


def _fmt_bool(value: Any) -> str:
    return "Y" if bool(value) else "N"


def _fmt_ts_ms(value: Any) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        dt = datetime.fromtimestamp(int(float(value)) / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(value)


def _trade_side(trade: dict[str, Any]) -> str:
    side = str(trade.get("side") or "").strip()
    if side:
        return side

    signed_qty = trade.get("qty")
    if signed_qty is not None:
        signed_qty_float = _safe_float(signed_qty)
        if signed_qty_float > 0:
            return "buy"
        if signed_qty_float < 0:
            return "sell"

    signed_contract = trade.get("qty_contract")
    if signed_contract is not None:
        signed_contract_float = _safe_float(signed_contract)
        if signed_contract_float > 0:
            return "buy"
        if signed_contract_float < 0:
            return "sell"

    raw_size = ((trade.get("raw") or {}).get("size"))
    if raw_size is not None:
        raw_size_float = _safe_float(raw_size)
        if raw_size_float > 0:
            return "buy"
        if raw_size_float < 0:
            return "sell"

    return "-"


def _trade_qty_text(trade: dict[str, Any]) -> str:
    qty = trade.get("qty")
    if qty is not None:
        qty_float = _safe_float(qty)
        if abs(qty_float) > 0:
            return f"qty={_fmt_num(abs(qty_float), 6)}"

    qty_contract = trade.get("qty_contract")
    if qty_contract is not None:
        qty_contract_float = _safe_float(qty_contract)
        if abs(qty_contract_float) > 0:
            return f"contracts={_fmt_num(abs(qty_contract_float), 6)}"

    raw_size = ((trade.get("raw") or {}).get("size"))
    if raw_size is not None:
        raw_size_float = _safe_float(raw_size)
        if abs(raw_size_float) > 0:
            return f"contracts={_fmt_num(abs(raw_size_float), 6)}"

    return "qty=-"


def _account_balance_line(account_snapshot: dict[str, Any]) -> str:
    asset_dict = account_snapshot.get("asset_dict", {})
    usd_asset = asset_dict.get("USD", {})
    usdt_asset = asset_dict.get("USDT", {})
    balance = usd_asset.get("balance", usdt_asset.get("balance", account_snapshot.get("total_balance", 0.0)))
    available = usd_asset.get("availableBalance", usdt_asset.get("availableBalance", balance))
    margin = usd_asset.get("marginBalance", usdt_asset.get("marginBalance", balance))
    total_balance = account_snapshot.get("total_balance", balance)
    leverage = account_snapshot.get("leverage", 0.0)
    delta_leverage = account_snapshot.get("delta_leverage", 0.0)
    return (
        f"total={_fmt_num(total_balance, 6)} "
        f"wallet={_fmt_num(balance, 6)} "
        f"available={_fmt_num(available, 6)} "
        f"margin={_fmt_num(margin, 6)} "
        f"lev={_fmt_num(leverage, 3)} "
        f"delta_lev={_fmt_num(delta_leverage, 3)}"
    )


def _balance_breakdown_lines(balance_report: dict[str, Any], account_snapshot: dict[str, Any]) -> list[str]:
    if not balance_report:
        return [f"- {_account_balance_line(account_snapshot)}"]

    return [
        (
            f"- asset={balance_report.get('balance_asset_code', 'USD')} "
            f"total={_fmt_num(balance_report.get('total_balance_usd'), 6)} "
            f"wallet={_fmt_num(balance_report.get('wallet_balance_usd'), 6)} "
            f"available={_fmt_num(balance_report.get('available_balance_usd'), 6)} "
            f"locked={_fmt_num(balance_report.get('locked_balance_usd'), 6)} "
            f"margin={_fmt_num(balance_report.get('margin_balance_usd'), 6)}"
        ),
        (
            f"- exch_upl={_fmt_num(balance_report.get('exchange_unrealized_profit_usd'), 6)} "
            f"theo_upl={_fmt_num(balance_report.get('theoretical_unrealized_profit_usd'), 6)} "
            f"actual_eq={_fmt_num(balance_report.get('actual_equity_usd'), 6)} "
            f"theo_eq={_fmt_num(balance_report.get('theoretical_equity_usd'), 6)} "
            f"eq_gap={_fmt_num(balance_report.get('theoretical_equity_gap_usd'), 6)}"
        ),
        (
            f"- long={_fmt_num(balance_report.get('long_notional_usd'), 6)} "
            f"short={_fmt_num(balance_report.get('short_notional_usd'), 6)} "
            f"gross={_fmt_num(balance_report.get('gross_notional_usd'), 6)} "
            f"net={_fmt_num(balance_report.get('net_notional_usd'), 6)} "
            f"lev={_fmt_num(balance_report.get('leverage'), 3)} "
            f"delta_lev={_fmt_num(balance_report.get('delta_leverage'), 3)}"
        ),
    ]


def _position_lines(positions: dict[str, dict[str, Any]], balance_report: dict[str, Any] | None = None) -> list[str]:
    balance_positions = (balance_report or {}).get("positions") or []
    if balance_positions:
        lines: list[str] = []
        for pos in balance_positions:
            lines.append(
                f"- {pos.get('ticker', '-')}: "
                f"qty={_fmt_num(pos.get('qty'), 6)} "
                f"entry={_fmt_num(pos.get('entry_price'), 4)} "
                f"mark={_fmt_num(pos.get('mark_price'), 4)} "
                f"notional={_fmt_num(pos.get('notional_mark_usd'), 6)} "
                f"exch_upl={_fmt_num(pos.get('exchange_unrealized_profit_usd'), 6)} "
                f"theo_upl={_fmt_num(pos.get('theoretical_unrealized_profit_usd'), 6)} "
                f"upl_gap={_fmt_num(pos.get('unrealized_profit_gap_usd'), 6)} "
                f"mark_src={pos.get('mark_source', '-')} "
                f"side={pos.get('position_side', '-')}"
            )
        return lines

    if not positions:
        return ["- none"]

    lines: list[str] = []
    for symbol in sorted(positions):
        pos = positions[symbol]
        lines.append(
            f"- {symbol}: "
            f"qty={_fmt_num(pos.get('qty'), 6)} "
            f"entry={_fmt_num(pos.get('entry_price'), 4)} "
            f"mark={_fmt_num(pos.get('mark_price'), 4)} "
            f"upl={_fmt_num(pos.get('unrealized_profit'), 6)} "
            f"side={pos.get('position_side', '-')}"
        )
    return lines


def _open_order_lines(order_map: dict[str, list[dict[str, Any]]]) -> list[str]:
    if not order_map:
        return ["- none"]

    lines: list[str] = []
    for symbol in sorted(order_map):
        for order in order_map[symbol]:
            lines.append(
                f"- {symbol}: "
                f"uid={order.get('uuid') or order.get('order_id') or order.get('client_order_id')} "
                f"side={order.get('side', '-')} "
                f"qty={_fmt_num(order.get('qty'), 6)} "
                f"price={_fmt_num(order.get('price'), 4)} "
                f"status={order.get('status', '-')} "
                f"left={_fmt_num(order.get('left_qty'), 6)}"
            )
    return lines


def _recent_trade_lines(recent_trades: dict[str, list[dict[str, Any]]]) -> list[str]:
    rows: list[str] = []
    for symbol in sorted(recent_trades):
        trades = recent_trades[symbol]
        if not trades:
            rows.append(f"- {symbol}: none")
            continue
        for trade in trades[:3]:
            rows.append(
                f"- {symbol}: "
                f"{_fmt_ts_ms(trade.get('create_time') or trade.get('timestamp'))} "
                f"side={_trade_side(trade)} "
                f"{_trade_qty_text(trade)} "
                f"price={_fmt_num(trade.get('price'), 4)} "
                f"fee={_fmt_num(trade.get('fee'), 8)} "
                f"role={trade.get('role', '-')}"
            )
    return rows or ["- none"]


def _funding_lines(funding_snapshot: dict[str, dict[str, Any]]) -> list[str]:
    if not funding_snapshot:
        return ["- none"]

    lines: list[str] = []
    for symbol in sorted(funding_snapshot):
        funding = funding_snapshot[symbol]
        status = funding.get("validation_status") or ("valid" if funding.get("is_valid") else "missing")
        reasons = ",".join(funding.get("validation_errors", []) or []) or "-"
        lines.append(
            f"- {symbol}: "
            f"status={status} "
            f"rate={_fmt_num(funding.get('funding_rate'), 8)} "
            f"next={_fmt_ts_ms(funding.get('next_funding_time'))} "
            f"mark={_fmt_num(funding.get('mark_price'), 4)} "
            f"index={_fmt_num(funding.get('index_price'), 4)} "
            f"validated={_fmt_ts_ms(funding.get('validated_at_ms'))} "
            f"reasons={reasons}"
        )
    return lines


def _health_lines(health_snapshot: dict[str, dict[str, Any]]) -> list[str]:
    if not health_snapshot:
        return ["- none"]

    lines: list[str] = []
    for symbol in sorted(health_snapshot):
        health = health_snapshot[symbol]
        reasons = ",".join(health.get("block_reasons", []) or []) or "-"
        lines.append(
            f"- {symbol}: "
            f"can_trade={_fmt_bool(health.get('can_trade'))} "
            f"gate_reasons={reasons} "
            f"account={health.get('account_sync_reason') or '-'} "
            f"funding={health.get('funding_sync_reason') or '-'} "
            f"source={health.get('ws_source_last') or health.get('market_source_last') or '-'} "
            f"drift_last_ms={_fmt_num(health.get('ws_drift_last_ms'), 1)} "
            f"stale_age_ms={_fmt_num(health.get('stale_age_ms'), 1)} "
            f"event_lag_ms={_fmt_num(health.get('event_lag_ms'), 1)} "
            f"transport_lag_ms={_fmt_num(health.get('transport_lag_ms'), 1)} "
            f"ws_last={_fmt_ts_ms(health.get('ws_last_update_ms'))} "
            f"event={_fmt_ts_ms(health.get('ws_event_time_last_ms'))} "
            f"server={_fmt_ts_ms(health.get('ws_server_time_last_ms'))} "
            f"desktop={_fmt_ts_ms(health.get('ws_desktop_time_last_ms'))}"
        )
    return lines


def _rate_limit_lines(health_overview: dict[str, Any]) -> list[str]:
    rate_limit_state = (health_overview or {}).get("rate_limit_state") or {}
    if not rate_limit_state:
        return ["- none"]

    last_rows = rate_limit_state.get("last") or []
    summary = [
        f"- blocked={_fmt_bool(rate_limit_state.get('blocked'))} "
        f"reason={rate_limit_state.get('reason') or '-'} "
        f"rows={len(last_rows)}"
    ]
    for row in last_rows[:5]:
        summary.append(
            f"- interval={row.get('interval', '-')} "
            f"interval_num={row.get('intervalNum', '-')} "
            f"count={row.get('count', '-')} "
            f"limit={row.get('limit', '-')}"
        )
    return summary


def _reconcile_lines(reconcile_status: dict[str, Any]) -> list[str]:
    if not reconcile_status:
        return ["- none"]

    last_reason = reconcile_status.get("last_reason") or "-"
    last_refresh_source = reconcile_status.get("last_refresh_source") or "-"
    last_manual_reconcile_reason = reconcile_status.get("last_manual_reconcile_reason") or "-"

    return [
        (
            f"- reason={last_reason} "
            f"refresh_source={last_refresh_source} "
            f"manual_reconcile_reason={last_manual_reconcile_reason}"
        ),
        (
            f"- attempt={_fmt_ts_ms(reconcile_status.get('last_attempt_ms'))} "
            f"success={_fmt_ts_ms(reconcile_status.get('last_success_ms'))}"
        ),
        (
            f"- account_synced={_fmt_bool(reconcile_status.get('account_synced'))} "
            f"open_orders_synced={_fmt_bool(reconcile_status.get('open_orders_synced'))} "
            f"funding_synced={_fmt_bool(reconcile_status.get('funding_synced'))} "
            f"open_order_count={reconcile_status.get('open_order_count', '-')}"
        ),
        (
            f"- funding_valid={reconcile_status.get('funding_valid_count', reconcile_status.get('funding_count', 0))} "
            f"funding_partial={reconcile_status.get('funding_partial_count', 0)} "
            f"funding_invalid={reconcile_status.get('funding_invalid_count', 0)} "
            f"funding_last_attempt={_fmt_ts_ms(reconcile_status.get('funding_last_attempt_ms'))} "
            f"funding_last_success={_fmt_ts_ms(reconcile_status.get('funding_last_success_ms'))}"
        ),
        (
            f"- funding_problem_symbols={','.join(reconcile_status.get('funding_problem_symbols', []) or []) or '-'}"
        ),
        (
            f"- auto_reason={reconcile_status.get('last_auto_reason') or '-'} "
            f"auto_request={_fmt_ts_ms(reconcile_status.get('last_auto_request_ms'))} "
            f"auto_count={reconcile_status.get('auto_trigger_count', 0)}"
        ),
        (
            f"- guard_reason={reconcile_status.get('guard_last_reason') or '-'} "
            f"guard_check={_fmt_ts_ms(reconcile_status.get('guard_last_check_ms'))} "
            f"guard_issue={_fmt_ts_ms(reconcile_status.get('guard_last_issue_ms'))} "
            f"guard_count={reconcile_status.get('guard_issue_count', 0)}"
        ),
        (
            f"- periodic_request={_fmt_ts_ms(reconcile_status.get('last_periodic_request_ms'))}"
        ),
        f"- error={reconcile_status.get('error') or '-'}",
    ]


def format_report(report: dict[str, Any]) -> str:
    exchange_name = str(report.get("exchange", "")).upper()
    symbols = ",".join(report.get("symbols", []))
    balance_report = report.get("balance_report", {})
    lines = [
        f"===== REPORT {exchange_name} =====",
        f"Symbols: {symbols}",
        "",
        "Balance Breakdown:",
        *_balance_breakdown_lines(balance_report, report.get("current_balance", {})),
        "",
        "Positions:",
        *_position_lines(report.get("current_positions", {}), balance_report),
        "",
        "Open Orders:",
        *_open_order_lines(report.get("open_orders", {})),
        "",
        "Recent Trades:",
        *_recent_trade_lines(report.get("recent_trades", {})),
        "",
        "Funding Snapshot:",
        *_funding_lines(report.get("funding_snapshot", {})),
        "",
        "Rate Limit / Block State:",
        *_rate_limit_lines(report.get("health_overview", {})),
        "",
        "Reconcile Status:",
        *_reconcile_lines(report.get("reconcile_status", {})),
        "",
        "Health Snapshot:",
        *_health_lines(report.get("health_snapshot", {})),
    ]
    return "\n".join(lines)


async def async_main():
    args = build_parser().parse_args()

    exchanges = [name.strip().lower() for name in args.exchanges.split(",") if name.strip()]
    for exchange_name in exchanges:
        if exchange_name not in SUPPORTED_EXCHANGES:
            raise ValueError(f"unsupported exchange in --exchanges: {exchange_name}")

    symbols = parse_csv_list(args.symbols, fallback=build_default_symbols())
    if not symbols:
        raise ValueError("No symbols resolved. Use --symbols or SYMBOL/SYMBOLS env.")

    for exchange_name in exchanges:
        report = await collect_exchange_report(exchange_name, symbols, args.wait_ws_sec, args.trade_limit)
        if args.json:
            print(report)
        else:
            print()
            print(format_report(report))


if __name__ == "__main__":
    asyncio.run(async_main())
