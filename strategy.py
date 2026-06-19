import asyncio
import contextlib
import json
import os
import signal
import sys
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any
from src.core.action_gate import ActionGate
from src.core.api_pressure import flush_api_pressure
from dotenv import load_dotenv
from src.ops.position_ownership import active_manual_symbols
from src.ops.orphan_guard import active_orphan_symbols, active_orphans, mark_orphan, clear_orphan
from src.ops.persistence.entry_quality import schedule_entry_quality_persist
from src.ops.persistence.runtime import build_runtime_persistence_from_env
from src.ops_console.action_gate import assert_action_token
from src.ops_console.actions import (
    cancel_symbol as console_cancel_symbol,
    flatten_symbol as console_flatten_symbol,
    live_limit_order as console_live_limit_order,
    live_market_order as console_live_market_order,
    paper_order as console_paper_order,
    rebalance_hedge as console_rebalance_hedge,
)
from src.ops_console.embedded import (
    EmbeddedControlServer,
    StrategyControlCommand,
    StrategyControlService,
    record_control_plane_event,
)
from src.ops_console.runtime import write_entry_control
from src.core.exchange_registry import build_exchange, validate_exchange_interface, validate_exchange_snapshot_shape
from src.strategy.config import load_strategy_config, normalize_exchange_name, parse_csv
from src.strategy.candidates import (
    CandidateBuildHooks,
    CandidateCollectionHooks,
    build_candidate_scan_summary,
    collect_pair_candidates,
    compute_effective_funding_components,
    compute_stale_data_penalty_usd,
    extract_best_ask,
    extract_best_bid,
    extract_funding_rate,
    extract_mid_price,
    extract_next_funding_ms,
    rank_candidates,
    seconds_until,
)
from src.strategy.control import EntryControlStore
from src.strategy.hedge import HedgeExecutionResult, execute_residual_hedge
from src.strategy.logging import (
    log_danger,
    log_entry_attempt,
    log_entry_blocked,
    log_hedge_incomplete,
    korean_side,
    log_micro_partial,
    log_overfill,
    log_pair_close_attempt,
    log_pair_close_trigger,
    log_pair_closed,
    log_pair_closed_detected,
    log_pair_opened,
    log_residual_ignored,
    log_startup_bootstrap_summary,
    log_strategy_config_summary,
)
from src.strategy.maker_entry_engine import MakerEntryEngine, MakerEntryHooks
from src.strategy.risk_gate import (
    OrderSlot,
    RiskGateContext,
    RiskGateDecision,
    evaluate_entry_risk,
)
from src.strategy.close_policy import (
    close_pnl_breakdown_usd,
    evaluate_close_policy,
    evaluate_normal_close_fee_gate,
    pair_entry_reference_notional_usd,
)
from src.strategy.entry_policy import evaluate_post_first_fill_edge
from src.strategy.rules import (
    close_first_leg_plan,
    close_hedge_leg_plan,
    first_leg_plan,
    hedge_leg_plan,
)
from src.strategy import order_price_policy
from src.strategy.models import (
    PROJECT_ROOT,
    FlattenLegResult,
    OpenPairState,
    PairCandidate,
    PairFlattenResult,
    StrategyConfig,
)
from src.strategy.margin_mode_guard import check_startup_margin_mode_guard
from src.strategy.exposure_guard import ExposureCapConfig, evaluate_exposure_caps
from src.strategy.rate_limit_guard import RateLimitGuardConfig, evaluate_rate_limit_guard
from src.strategy.runtime_log_filter import install_runtime_log_filter
from src.strategy.execution import (
    wait_for_position_fill as _execution_wait_for_position_fill,
    wait_for_position_close as _execution_wait_for_position_close,
    extract_order_uuid as _execution_extract_order_uuid,
    extract_order_avg_price as _execution_extract_order_avg_price,
    cancel_if_open as _execution_cancel_if_open,
    execute_requoting_limit_leg as _execution_execute_requoting_limit_leg,
    resolve_effective_order_qty as _execution_resolve_effective_order_qty,
    settle_grace_sec as _execution_settle_grace_sec,
)

load_dotenv()

SUPPORTED_SYMBOLS_CACHE_DIR = PROJECT_ROOT / "logs" / "supported_symbols_cache"
EPS = 1e-9
_BOOTSTRAP_SUCCESS_CACHE: set[tuple[str, str, int, str]] = set()


def flatten_leg_result_payload(result: FlattenLegResult | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "exchange": result.exchange_name,
        "symbol": result.symbol,
        "role": result.role,
        "side": result.side,
        "requested_qty": result.requested_qty,
        "start_qty": result.start_qty,
        "end_qty": result.end_qty,
        "closed_qty": result.closed_qty,
        "ok": result.ok,
        "assumed_flat": result.assumed_flat,
        "attempted_order": result.attempted_order,
        "failure_reason": result.failure_reason,
        "error_code": result.error_code,
        "error_message": result.error_message,
        "temporarily_unavailable": result.temporarily_unavailable,
    }


def normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def now_ms() -> int:
    return int(time.time() * 1000)


def entry_locked_symbols(config: "StrategyConfig") -> set[str]:
    """Symbols the strategy must not enter: manually-locked positions plus
    orphaned (unhedged, failed-rollback) positions awaiting operator cleanup."""
    return active_manual_symbols(config.position_ownership_file) | active_orphan_symbols()


def reconcile_orphan_markers(exchanges: dict[str, Any], config: "StrategyConfig") -> None:
    """Auto-clear an orphan marker once the symbol is flat on every exchange
    (e.g. after the operator manually flattened it via the console).

    The clear decision must use account snapshots newer than the orphan marker.
    Otherwise a stale flat snapshot can erase the only operator warning for a
    real unhedged leg.
    """
    records_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for record in active_orphans():
        symbol = normalize_symbol(record.get("symbol") or "")
        if symbol:
            records_by_symbol.setdefault(symbol, []).append(record)

    for symbol, orphan_records in records_by_symbol.items():
        marker_ms = max(
            int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
            for record in orphan_records
        )
        still_open = False
        snapshots_fresh = True
        for exchange_name, exchange in exchanges.items():
            try:
                snapshot = exchange.get_symbol_snapshot(symbol)
                account_last_sync_ms = int((snapshot or {}).get("account_last_sync_ms") or 0)
                qty = effective_position_qty(snapshot, exchange, symbol, config)
            except Exception:
                still_open = True
                snapshots_fresh = False
                break
            if account_last_sync_ms <= marker_ms:
                snapshots_fresh = False
            if abs(qty) > EPS:
                still_open = True
                break
        if not still_open and snapshots_fresh:
            clear_orphan(symbol)


def mark_actual_orphan_legs(
    *,
    symbol: str,
    exchanges: dict[str, Any],
    config: "StrategyConfig",
    reason: str,
    fallback_exchange: str | None = None,
    fallback_qty: float | None = None,
) -> list[dict[str, Any]]:
    """Persist the actual non-flat orphan legs visible in account snapshots.

    A hedge failure can leave exposure on either venue. Recording only the
    first leg's requested fill can be wrong after a partial rollback, so this
    helper uses the same effective-position filter as recovery/shutdown.
    """
    records: list[dict[str, Any]] = []
    for exchange_name, exchange in exchanges.items():
        try:
            qty = effective_position_qty(exchange.get_symbol_snapshot(symbol), exchange, symbol, config)
        except Exception:
            continue
        if abs(qty) > EPS:
            records.append(
                mark_orphan(
                    symbol=symbol,
                    exchange=exchange_name,
                    qty=qty,
                    reason=reason,
                )
            )

    if not records and fallback_exchange and fallback_qty is not None and abs(fallback_qty) > EPS:
        records.append(
            mark_orphan(
                symbol=symbol,
                exchange=fallback_exchange,
                qty=fallback_qty,
                reason=f"{reason}:snapshot_fallback",
            )
        )
    return records


def iso_utc_from_ms(timestamp_ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp_ms / 1000.0))


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def pair_event_payload(pair: OpenPairState) -> dict[str, Any]:
    return {
        "pair_id": pair.pair_id,
        "symbol": pair.symbol,
        "long_exchange": pair.long_exchange,
        "short_exchange": pair.short_exchange,
        "qty": pair.qty,
        "requested_qty": pair.requested_qty,
        "long_entry_price": pair.long_entry_price,
        "short_entry_price": pair.short_entry_price,
        "entry_time_ms": pair.entry_time_ms,
        "entry_time_utc": iso_utc_from_ms(pair.entry_time_ms),
        "entry_net_edge_usd": pair.entry_net_edge_usd,
        "funding_long_rate": pair.funding_long_rate,
        "funding_short_rate": pair.funding_short_rate,
        "status": pair.status,
        "last_hold_decision": pair.last_hold_decision,
        "last_hold_pnl_usd": pair.last_hold_pnl_usd,
        "last_close_pnl_usd": pair.last_close_pnl_usd,
        "last_close_edge_usd": pair.last_close_edge_usd,
        "last_close_reason": pair.last_close_reason,
    }


def record_live_event(config: StrategyConfig, event_type: str, **payload: Any) -> None:
    timestamp_ms = now_ms()
    row = {
        "ts_ms": timestamp_ms,
        "ts_utc": iso_utc_from_ms(timestamp_ms),
        "event_type": event_type,
        **payload,
    }
    try:
        append_jsonl(config.live_event_log_path, row)
    except Exception as exc:
        print(f"[strategy][warn] failed_to_write_live_event type={event_type} error={exc!r}")
    try:
        record_control_plane_event(row)
    except Exception as exc:
        print(f"[strategy][warn] failed_to_publish_control_plane_event type={event_type} error={exc!r}")
    try:
        schedule_entry_quality_persist(row)
    except Exception as exc:
        print(f"[strategy][warn] failed_to_schedule_entry_quality_persist type={event_type} error={exc!r}")


def simple_stdout_enabled(config: StrategyConfig | None = None) -> bool:
    style = config.stdout_log_style if config is not None else os.getenv("STRATEGY_STDOUT_LOG_STYLE", "verbose")
    return str(style).strip().lower() == "simple"


def print_verbose(config: StrategyConfig | None, *args: Any, **kwargs: Any) -> None:
    if not simple_stdout_enabled(config):
        print(*args, **kwargs)


async def load_supported_symbols(exchange_name: str) -> set[str]:
    exchange_name = normalize_exchange_name(exchange_name)
    cache_ttl_sec = int(float(os.getenv("STRATEGY_SUPPORTED_SYMBOLS_CACHE_SEC", "21600") or 21600))

    def cache_path() -> Path:
        return SUPPORTED_SYMBOLS_CACHE_DIR / f"{exchange_name}.json"

    def load_cached_symbols(allow_stale: bool) -> set[str] | None:
        path = cache_path()
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            fetched_at_ms = int(payload.get("fetched_at_ms") or 0)
            symbols = payload.get("symbols") or []
            if not allow_stale and fetched_at_ms > 0:
                age_sec = max((int(time.time() * 1000) - fetched_at_ms) / 1000.0, 0.0)
                if age_sec > cache_ttl_sec:
                    return None
            return {normalize_symbol(symbol) for symbol in symbols if str(symbol).strip()}
        except Exception:
            return None

    def save_cached_symbols(symbols: set[str]) -> None:
        path = cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "exchange": exchange_name,
            "fetched_at_ms": int(time.time() * 1000),
            "symbols": sorted(symbols),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    cached_symbols = load_cached_symbols(allow_stale=False)
    if cached_symbols:
        return cached_symbols

    if exchange_name == "binance":
        from src.binance_f_module.api import get_ticker_list
    elif exchange_name == "gateio":
        from src.gateio_f_module.api import get_ticker_list
    elif exchange_name == "okx":
        from src.okx_f_module.api import get_ticker_list
    else:
        raise ValueError(f"unsupported exchange: {exchange_name}")

    try:
        result = await get_ticker_list()
        if result is None:
            raise RuntimeError(f"failed to fetch ticker list for exchange={exchange_name}")
        ticker_list, _ = result
        supported = {normalize_symbol(symbol) for symbol in ticker_list}
        save_cached_symbols(supported)
        return supported
    except Exception as exc:
        stale_symbols = load_cached_symbols(allow_stale=True)
        if stale_symbols:
            print(
                f"[strategy][warn] using stale supported-symbols cache for exchange={exchange_name} "
                f"reason={exc!r}"
            )
            return stale_symbols
        raise


async def resolve_strategy_symbols(config: StrategyConfig) -> list[str]:
    if config.symbol_input.strip().upper() == "ALL":
        symbol_sets = []
        for exchange_name in config.exchange_names:
            symbol_sets.append(await load_supported_symbols(exchange_name))
        common = set.intersection(*symbol_sets) if symbol_sets else set()
        symbols = sorted(common)
    else:
        requested = [normalize_symbol(symbol) for symbol in parse_csv(config.symbol_input)]
        if not requested:
            raise ValueError("no symbols resolved from STRATEGY_SYMBOLS")
        common: set[str] | None = None
        for exchange_name in config.exchange_names:
            try:
                supported = await load_supported_symbols(exchange_name)
            except Exception as exc:
                print(
                    f"[strategy][warn] support fetch failed for exchange={exchange_name}; "
                    f"falling back to requested symbols reason={exc!r}"
                )
                supported = set(requested)
            common = supported if common is None else (common & supported)
        common = common or set()
        symbols = [symbol for symbol in requested if symbol in common]
        missing = [symbol for symbol in requested if symbol not in common]
        if missing:
            print(f"[strategy][warn] unsupported symbols filtered out: {missing}")

    if not symbols:
        raise ValueError("resolved symbol universe is empty")
    return symbols


def build_strategy_exchanges(config: StrategyConfig, symbols: list[str]) -> dict[str, Any]:
    exchanges: dict[str, Any] = {}
    for exchange_name in config.exchange_names:
        exchanges[exchange_name] = build_exchange(
            exchange_name,
            symbols,
            account_interval=0.5,
            order_interval=0.3,
            open_orders_interval=1.5,
            funding_interval=5.0,
            health_interval=0.3,
        )
    return exchanges


async def start_strategy_exchanges(exchanges: dict[str, Any]) -> None:
    for exchange_name, exchange in exchanges.items():
        await exchange.run()
        print_verbose(None, f"[strategy][run] exchange={exchange_name}")


async def stop_strategy_exchanges(exchanges: dict[str, Any]) -> None:
    for exchange_name, exchange in exchanges.items():
        try:
            await exchange.stop()
        except Exception as exc:
            print(f"[strategy][stop][error] exchange={exchange_name} error={exc}")


async def attach_strategy_listeners(
    exchanges: dict[str, Any],
    symbols: list[str],
    wake_queue: asyncio.Queue[tuple[str, str, int]],
) -> list[asyncio.Task[Any]]:
    tasks: list[asyncio.Task[Any]] = []

    async def _worker(exchange_name: str, symbol: str, exchange: Any) -> None:
        queue = exchange.register_listener(symbol)
        while True:
            event = await queue.get()
            timestamp = int(time.time() * 1000)
            if isinstance(event, dict):
                timestamp = int(float(event.get("desktop_time") or timestamp))
            await wake_queue.put((exchange_name, symbol, timestamp))

    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            tasks.append(asyncio.create_task(_worker(exchange_name, symbol, exchange), name=f"strategy-listener-{exchange_name}-{symbol}"))
    return tasks


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def floor_by_step(value: float, step: float) -> float:
    if step <= 0:
        return max(value, 0.0)
    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    return float((value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec)


def ceil_by_step(value: float, step: float) -> float:
    if step <= 0:
        return max(value, 0.0)
    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    units = (value_dec / step_dec).to_integral_value(rounding=ROUND_UP)
    return float(units * step_dec)


def extract_position_qty(snapshot: dict[str, Any]) -> float:
    position = snapshot.get("position", {}) or {}
    for key in ("qty", "position_amt", "size"):
        if key in position:
            return _safe_float(position.get(key), 0.0)
    return 0.0


def extract_position_entry_price(snapshot: dict[str, Any]) -> float:
    position = snapshot.get("position", {}) or {}
    for key in ("entry_price", "avg_price", "avgPrice", "entryPrice"):
        value = _safe_float(position.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def assumed_flat_key(exchange_name: str, symbol: str) -> tuple[str, str]:
    return (str(exchange_name).lower(), str(symbol).upper())


def prune_assumed_flat(assumed_flat_until_ms: dict[tuple[str, str], int]) -> None:
    now_ms = int(time.time() * 1000)
    expired = [key for key, until_ms in assumed_flat_until_ms.items() if until_ms <= now_ms]
    for key in expired:
        assumed_flat_until_ms.pop(key, None)


def is_assumed_flat(
    assumed_flat_until_ms: dict[tuple[str, str], int] | None,
    exchange_name: str,
    symbol: str,
) -> bool:
    if not assumed_flat_until_ms:
        return False
    until_ms = assumed_flat_until_ms.get(assumed_flat_key(exchange_name, symbol), 0)
    return until_ms > int(time.time() * 1000)


def effective_position_qty(
    snapshot: dict[str, Any],
    exchange: Any,
    symbol: str,
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> float:
    exchange_name = getattr(exchange, "persist_name", None) or getattr(exchange, "name", "") or ""
    if is_assumed_flat(assumed_flat_until_ms, exchange_name, symbol):
        return 0.0
    qty = extract_position_qty(snapshot)
    if abs(qty) <= EPS:
        return 0.0

    side = "sell" if qty > 0 else "buy"
    preview = exchange_order_execution_preview(
        exchange,
        symbol,
        side,
        abs(qty),
        config.close_aggressive_bps,
        snapshot=snapshot,
    )
    close_price = _safe_float(preview.get("price"), 0.0)
    min_executable_qty = _safe_float(preview.get("min_executable_qty"), 0.0)
    # Treat any residual below the actual executable threshold as flat. For
    # low-notional leftovers this threshold can be higher than raw min_qty.
    if _safe_float(preview.get("executable_qty"), 0.0) <= EPS:
        _log_throttled_dust_ignored(
            exchange_name,
            symbol,
            qty,
            min_executable_qty,
            reason="below_min_executable_qty",
            price=close_price,
        )
        return 0.0

    account_last_sync_ms = int(
        snapshot.get("account_last_sync_ms")
        or snapshot.get("position", {}).get("timestamp")
        or 0
    )
    if account_last_sync_ms > 0:
        age_sec = max((now_ms() - account_last_sync_ms) / 1000.0, 0.0)
        if age_sec >= config.position_stale_ignore_sec and not has_open_orders(snapshot):
            return 0.0
    return qty


_DUST_LOG_LAST_MS: dict[tuple[str, str], int] = {}
_DUST_LOG_COOLDOWN_MS = 60_000


def _log_throttled_dust_ignored(
    exchange_name: str,
    symbol: str,
    qty: float,
    min_executable_qty: float,
    *,
    reason: str,
    price: float = 0.0,
) -> None:
    key = (exchange_name, symbol)
    cur = int(time.time() * 1000)
    if cur - _DUST_LOG_LAST_MS.get(key, 0) < _DUST_LOG_COOLDOWN_MS:
        return
    _DUST_LOG_LAST_MS[key] = cur
    print(
        f"[strategy][dust][ignored] exchange={exchange_name} symbol={symbol} "
        f"qty={qty:.12f} min_executable_qty={min_executable_qty:.12f} "
        f"price={price:.8f} reason={reason} (treated as flat)"
    )


def register_pair_assumed_flat(
    pair: OpenPairState,
    assumed_flat_until_ms: dict[tuple[str, str], int],
    config: StrategyConfig,
) -> None:
    exchange_names = list(pair.assumed_flat_exchanges)
    if not exchange_names and pair.status == "closed":
        exchange_names = [pair.long_exchange, pair.short_exchange]
    expiry_ms = int(time.time() * 1000 + (config.assumed_flat_grace_sec * 1000.0))
    for exchange_name in exchange_names:
        assumed_flat_until_ms[assumed_flat_key(exchange_name, pair.symbol)] = expiry_ms


def clear_pair_assumed_flat(
    pair: OpenPairState,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None,
) -> None:
    if not assumed_flat_until_ms:
        return
    for exchange_name in (pair.long_exchange, pair.short_exchange):
        assumed_flat_until_ms.pop(assumed_flat_key(exchange_name, pair.symbol), None)


def extract_health_can_trade(snapshot: dict[str, Any]) -> bool:
    health = snapshot.get("health", {}) or {}
    if "can_trade" in health:
        return bool(health.get("can_trade"))
    return False


def extract_health_block_reasons(snapshot: dict[str, Any]) -> list[str]:
    health = snapshot.get("health", {}) or {}
    reasons = health.get("block_reasons", [])
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons]
    if reasons:
        return [str(reasons)]
    return []


def has_open_orders(snapshot: dict[str, Any]) -> bool:
    orders = snapshot.get("open_orders", []) or []
    return len(orders) > 0


def snapshot_account_age_sec(snapshot: dict[str, Any]) -> float | None:
    account_last_sync_ms = int(
        snapshot.get("account_last_sync_ms")
        or snapshot.get("position", {}).get("timestamp")
        or 0
    )
    if account_last_sync_ms <= 0:
        return None
    return max((now_ms() - account_last_sync_ms) / 1000.0, 0.0)


def snapshot_account_is_fresh(snapshot: dict[str, Any], max_age_sec: float) -> bool:
    age_sec = snapshot_account_age_sec(snapshot)
    if age_sec is None:
        return False
    return age_sec <= max_age_sec


def pair_action_inputs_are_fresh(
    long_snapshot: dict[str, Any],
    short_snapshot: dict[str, Any],
    config: StrategyConfig,
) -> bool:
    return (
        snapshot_account_is_fresh(long_snapshot, config.action_account_fresh_sec)
        and snapshot_account_is_fresh(short_snapshot, config.action_account_fresh_sec)
    )


def get_exchange_balance_report(exchange: Any) -> dict[str, Any]:
    getter = getattr(exchange, "get_balance_report", None)
    if callable(getter):
        try:
            report = getter()
            if isinstance(report, dict):
                return report
        except Exception:
            pass

    snapshot_getter = getattr(exchange, "get_account_snapshot", None)
    if callable(snapshot_getter):
        try:
            snapshot = snapshot_getter()
            if isinstance(snapshot, dict):
                asset_dict = snapshot.get("asset_dict") or {}
                usd_asset = asset_dict.get("USD") or asset_dict.get("USDT") or {}
                wallet_balance = _safe_float(usd_asset.get("balance"), _safe_float(snapshot.get("total_balance"), 0.0))
                available_balance = _safe_float(
                    usd_asset.get("availableBalance", usd_asset.get("available")),
                    wallet_balance,
                )
                margin_balance = _safe_float(usd_asset.get("marginBalance"), wallet_balance)
                return {
                    "wallet_balance_usd": wallet_balance,
                    "available_balance_usd": available_balance,
                    "margin_balance_usd": margin_balance,
                }
        except Exception:
            pass
    return {}


def collect_balance_reports_by_exchange(exchanges: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        exchange_name: get_exchange_balance_report(exchange)
        for exchange_name, exchange in exchanges.items()
    }


def exchange_available_balance_usd(exchange: Any, balance_report: dict[str, Any] | None = None) -> float:
    report = balance_report or get_exchange_balance_report(exchange)
    available_balance = _safe_float(report.get("available_balance_usd"), 0.0)
    if available_balance > 0:
        return available_balance
    margin_balance = _safe_float(report.get("margin_balance_usd"), 0.0)
    if margin_balance > 0:
        return margin_balance
    return _safe_float(report.get("wallet_balance_usd"), 0.0)


def estimate_entry_margin_required_usd(exchange_name: str, leg_notional_usd: float, config: StrategyConfig) -> float:
    if leg_notional_usd <= 0:
        return 0.0
    leverage = max(config.leverage, 1)
    base_margin = leg_notional_usd / leverage
    entry_fee = leg_notional_usd * config.entry_fee_bps.get(exchange_name, 0.0) / 10000.0
    return (base_margin * max(config.margin_guard_safety_factor, 1.0)) + entry_fee + max(config.margin_guard_extra_usd, 0.0)


def margin_guard_block_reason(exchange_name: str, available_balance_usd: float, required_margin_usd: float) -> str:
    return f"{exchange_name}:insufficient_available_margin({available_balance_usd:.2f}<{required_margin_usd:.2f})"


def pair_entry_margin_guard_reasons(
    *,
    long_exchange: str,
    short_exchange: str,
    long_exchange_obj: Any,
    short_exchange_obj: Any,
    long_notional_usd: float,
    short_notional_usd: float,
    config: StrategyConfig,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
) -> list[str]:
    if not config.margin_guard_enabled:
        return []

    reasons: list[str] = []
    for exchange_name, exchange_obj, leg_notional_usd in (
        (long_exchange, long_exchange_obj, long_notional_usd),
        (short_exchange, short_exchange_obj, short_notional_usd),
    ):
        if leg_notional_usd <= 0:
            continue
        balance_report = (balance_reports_by_exchange or {}).get(exchange_name)
        available_balance_usd = exchange_available_balance_usd(exchange_obj, balance_report)
        required_margin_usd = estimate_entry_margin_required_usd(exchange_name, leg_notional_usd, config)
        if available_balance_usd + EPS < required_margin_usd:
            reasons.append(margin_guard_block_reason(exchange_name, available_balance_usd, required_margin_usd))
    return reasons


def render_candidate_table(
    candidates: list[PairCandidate],
    max_rows: int,
    show_command_hint: bool = True,
) -> str:
    lines = []
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"\n[strategy][scan] {ts}")
    lines.append("idx symbol pair               qty        net_usd  net_bps  fund_usd  edge_usd  status")
    lines.append("--- ------ ------------------ ---------- -------- ------- --------- -------- ----------------")
    for idx, candidate in enumerate(candidates[:max_rows], start=1):
        pair = f"{candidate.long_exchange[:3]}L/{candidate.short_exchange[:3]}S"
        status = "OK" if candidate.can_enter else ";".join(candidate.block_reasons[:2])
        lines.append(
            f"{idx:>3} {candidate.symbol:<6} {pair:<18} "
                f"{candidate.executable_qty:>10.6f} "
                f"{candidate.expected_net_edge_usd:>8.4f} "
                f"{candidate.expected_net_edge_bps:>7.2f} "
                f"{candidate.effective_funding_pnl_usd:>9.4f} "
                f"{candidate.entry_open_edge_usd:>8.4f} "
                f"{status}"
            )
    if not candidates:
        lines.append("(no candidates)")
    if show_command_hint:
        lines.append("command: list | show <idx> | enter <idx> | pairs | close <pair> | quit")
    return "\n".join(lines)


def build_candidate_signature(candidates: list[PairCandidate], top_n: int = 5) -> tuple[Any, ...]:
    signature: list[Any] = []
    for candidate in candidates[:top_n]:
        signature.append(
            (
                candidate.id,
                round(candidate.expected_net_edge_usd, 6),
                round(candidate.effective_funding_pnl_usd, 6),
                candidate.can_enter,
            )
        )
    return tuple(signature)


async def console_command_reader(command_queue: asyncio.Queue[str]) -> None:
    if not sys.stdin.isatty():
        return
    while True:
        try:
            line = await asyncio.to_thread(input, "strategy> ")
        except EOFError:
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[strategy][console][error] {exc}")
            await asyncio.sleep(0.2)
            continue
        await command_queue.put(line.strip())


def apply_open_pair_capacity_blocks(
    candidates: list[PairCandidate],
    open_pairs: dict[str, OpenPairState],
    config: StrategyConfig,
) -> list[PairCandidate]:
    if config.max_open_pairs <= 0:
        return rank_candidates(candidates)
    active_count = sum(1 for pair in open_pairs.values() if pair.status not in {"closed", "failed"})
    for candidate in candidates:
        if active_count >= config.max_open_pairs and "max_open_pairs_reached" not in candidate.block_reasons:
            candidate.block_reasons.append("max_open_pairs_reached")
            candidate.can_enter = False
    return rank_candidates(candidates)


def _order_symbol(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    return str(row.get("ticker") or row.get("symbol") or "").upper()


def _order_is_terminal(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return True
    status = str(row.get("status") or "").upper()
    state = str(row.get("state") or "").lower()
    error = str(row.get("error") or "").lower()
    if state == "not_exist" or error == "not_exist":
        return True
    return status in {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED"}


def exchange_has_symbol_waiting_order(exchange: Any, symbol: str) -> bool:
    normalized_symbol = str(symbol or "").upper()
    if not normalized_symbol:
        return False

    for row in getattr(exchange, "waiting_list", []) or []:
        if _order_symbol(row) == normalized_symbol and not _order_is_terminal(row):
            return True

    snapshot = getattr(exchange, "open_orders_snapshot", {}) or {}
    if isinstance(snapshot, dict):
        for key, rows in snapshot.items():
            if str(key).upper() != normalized_symbol:
                continue
            for row in rows or []:
                if not _order_is_terminal(row):
                    return True
    return False


def _order_side(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    return str(row.get("side") or row.get("order_side") or "").strip().lower()


def iter_exchange_open_order_slots(exchange_name: str, exchange: Any) -> set[OrderSlot]:
    slots: set[OrderSlot] = set()

    for row in getattr(exchange, "waiting_list", []) or []:
        if _order_is_terminal(row):
            continue
        symbol = _order_symbol(row)
        if symbol:
            slots.add(OrderSlot(exchange_name, symbol, _order_side(row)).normalized())

    snapshot = getattr(exchange, "open_orders_snapshot", {}) or {}
    if isinstance(snapshot, dict):
        for snapshot_symbol, rows in snapshot.items():
            for row in rows or []:
                if _order_is_terminal(row):
                    continue
                symbol = _order_symbol(row) or str(snapshot_symbol or "").upper()
                if symbol:
                    slots.add(OrderSlot(exchange_name, symbol, _order_side(row)).normalized())
    return slots


def planned_entry_order_slots(candidate: PairCandidate, config: StrategyConfig) -> list[OrderSlot]:
    first_exchange, first_side = first_leg_plan(candidate, config)
    hedge_exchange, hedge_side = hedge_leg_plan(candidate, config)
    return [
        OrderSlot(first_exchange, candidate.symbol, first_side).normalized(),
        OrderSlot(hedge_exchange, candidate.symbol, hedge_side).normalized(),
    ]


def candidate_margin_guard_reasons(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
) -> list[str]:
    return pair_entry_margin_guard_reasons(
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        long_exchange_obj=exchanges[candidate.long_exchange],
        short_exchange_obj=exchanges[candidate.short_exchange],
        long_notional_usd=candidate.executable_qty * candidate.long_ask,
        short_notional_usd=candidate.executable_qty * candidate.short_bid,
        config=config,
        balance_reports_by_exchange=balance_reports_by_exchange,
    )


def candidate_exposure_guard_result(
    candidate: PairCandidate,
    config: StrategyConfig,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
):
    return evaluate_exposure_caps(
        balance_reports_by_exchange=balance_reports_by_exchange,
        planned_long_notional_usd=candidate.executable_qty * candidate.long_ask,
        planned_short_notional_usd=candidate.executable_qty * candidate.short_bid,
        config=ExposureCapConfig(
            max_gross_leverage=config.max_gross_leverage,
            max_long_leverage=config.max_long_leverage,
            max_short_leverage=config.max_short_leverage,
        ),
    )


def candidate_rate_limit_guard_result(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
):
    return evaluate_rate_limit_guard(
        exchanges=exchanges,
        exchange_names=(candidate.long_exchange, candidate.short_exchange),
        config=RateLimitGuardConfig(
            enabled=config.rate_limit_guard_enabled,
            usage_threshold=config.rate_limit_guard_usage_threshold,
        ),
    )


def build_risk_gate_context(
    open_pairs: dict[str, OpenPairState] | None,
    exchanges: dict[str, Any],
    entry_control_store: EntryControlStore | None,
    startup_block_reasons: tuple[str, ...] = (),
    manual_locked_symbols: set[str] | None = None,
) -> RiskGateContext:
    active_symbols = frozenset(
        pair.symbol.upper()
        for pair in (open_pairs or {}).values()
        if pair.status not in {"closed", "failed"}
    )
    open_order_slots: set[OrderSlot] = set()
    for exchange_name, exchange in exchanges.items():
        open_order_slots.update(iter_exchange_open_order_slots(exchange_name, exchange))

    entry_control_reasons: tuple[str, ...] = ()
    if entry_control_store is not None:
        entry_decision = entry_control_store.evaluate()
        if not entry_decision.allowed:
            entry_control_reasons = tuple(entry_decision.reasons)

    return RiskGateContext(
        active_symbols=active_symbols,
        open_order_slots=frozenset(open_order_slots),
        manual_locked_symbols=frozenset(symbol.upper() for symbol in (manual_locked_symbols or set())),
        entry_control_reasons=entry_control_reasons,
        startup_block_reasons=tuple(startup_block_reasons or ()),
    )


def evaluate_candidate_risk_now(
    candidate: PairCandidate,
    open_pairs: dict[str, OpenPairState] | None,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    entry_control_store: EntryControlStore | None,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
    startup_block_reasons: tuple[str, ...] = (),
    manual_locked_symbols: set[str] | None = None,
) -> RiskGateDecision:
    context = build_risk_gate_context(open_pairs, exchanges, entry_control_store, startup_block_reasons, manual_locked_symbols)
    exposure_result = candidate_exposure_guard_result(candidate, config, balance_reports_by_exchange)
    rate_limit_result = candidate_rate_limit_guard_result(candidate, exchanges, config)
    details: dict[str, Any] = {}
    if exposure_result.metrics:
        details["exposure"] = exposure_result.metrics
    if rate_limit_result.metrics:
        details["rate_limit"] = rate_limit_result.metrics
    return evaluate_entry_risk(
        symbol=candidate.symbol,
        planned_order_slots=planned_entry_order_slots(candidate, config),
        margin_reasons=candidate_margin_guard_reasons(
            candidate,
            exchanges,
            config,
            balance_reports_by_exchange,
        ),
        exposure_reasons=exposure_result.reasons,
        rate_limit_reasons=rate_limit_result.reasons,
        details=details,
        context=context,
    )


def apply_risk_gate_blocks(
    candidates: list[PairCandidate],
    open_pairs: dict[str, OpenPairState] | None,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    entry_control_store: EntryControlStore | None,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
    startup_block_reasons: tuple[str, ...] = (),
    manual_locked_symbols: set[str] | None = None,
) -> list[PairCandidate]:
    context = build_risk_gate_context(open_pairs, exchanges, entry_control_store, startup_block_reasons, manual_locked_symbols)
    for candidate in candidates:
        exposure_result = candidate_exposure_guard_result(candidate, config, balance_reports_by_exchange)
        rate_limit_result = candidate_rate_limit_guard_result(candidate, exchanges, config)
        details: dict[str, Any] = {}
        if exposure_result.metrics:
            details["exposure"] = exposure_result.metrics
        if rate_limit_result.metrics:
            details["rate_limit"] = rate_limit_result.metrics
        decision = evaluate_entry_risk(
            symbol=candidate.symbol,
            planned_order_slots=planned_entry_order_slots(candidate, config),
            margin_reasons=candidate_margin_guard_reasons(
                candidate,
                exchanges,
                config,
                balance_reports_by_exchange,
            ),
            exposure_reasons=exposure_result.reasons,
            rate_limit_reasons=rate_limit_result.reasons,
            details=details,
            context=context,
        )
        if decision.allowed:
            continue
        apply_risk_decision_to_candidate(candidate, decision)
    return rank_candidates(candidates)


def apply_risk_decision_to_candidate(candidate: PairCandidate, decision: RiskGateDecision) -> None:
    if decision.allowed:
        return
    candidate.can_enter = False
    for reason in decision.reasons:
        if reason not in candidate.block_reasons:
            candidate.block_reasons.append(reason)


def record_entry_risk_blocked(
    config: StrategyConfig,
    candidate: PairCandidate,
    decision: RiskGateDecision,
) -> None:
    payload: dict[str, Any] = {
        "candidate_id": candidate.id,
        "symbol": candidate.symbol,
        "long_exchange": candidate.long_exchange,
        "short_exchange": candidate.short_exchange,
        "reasons": list(decision.reasons),
    }
    if decision.details:
        payload["details"] = decision.details
        if decision.details.get("exposure"):
            payload["exposure"] = decision.details["exposure"]
        if decision.details.get("rate_limit"):
            payload["rate_limit"] = decision.details["rate_limit"]
    record_live_event(
        config,
        "entry_risk_blocked",
        **payload,
    )


def mark_symbol_recently_closed(
    symbol_recent_close_ms: dict[str, int] | None,
    symbol: str,
) -> None:
    if symbol_recent_close_ms is None:
        return
    symbol_recent_close_ms[str(symbol).upper()] = now_ms()


def request_account_refresh(
    exchange: Any,
    reason: str,
    priority: str = "normal",
    symbol: str | None = None,
) -> bool:
    requester = getattr(exchange, "request_account_refresh", None)
    if not callable(requester):
        return False
    try:
        return bool(requester(reason, priority=priority, symbol=symbol))
    except Exception as exc:
        print(
            f"[strategy][account_refresh][warn] "
            f"exchange={getattr(exchange, 'persist_name', getattr(exchange, 'name', 'unknown'))} "
            f"reason={reason} symbol={symbol or ''} error={exc!r}"
        )
        return False


def candidate_symbol_is_strong_flat(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None,
) -> bool:
    for exchange_name in {candidate.long_exchange, candidate.short_exchange}:
        exchange = exchanges.get(exchange_name)
        if exchange is None:
            return False
        snapshot = exchange.get_symbol_snapshot(candidate.symbol)
        if not snapshot_account_is_fresh(snapshot, config.action_account_fresh_sec):
            return False
        # Do not use assumed-flat grace here; this is the early-release check
        # for the recent-close cooldown, so it must be based on actual fresh
        # account state.
        if abs(effective_position_qty(snapshot, exchange, candidate.symbol, config, None)) > EPS:
            return False
        if has_open_orders(snapshot) or exchange_has_symbol_waiting_order(exchange, candidate.symbol):
            return False
    return True


def apply_recent_symbol_close_blocks(
    candidates: list[PairCandidate],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    symbol_recent_close_ms: dict[str, int] | None,
    recent_close_block_logged_ms: dict[str, int] | None = None,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> list[PairCandidate]:
    cooldown_ms = int(max(config.symbol_recent_close_cooldown_sec, 0.0) * 1000.0)
    if cooldown_ms <= 0 or not symbol_recent_close_ms:
        return rank_candidates(candidates)

    cur_ms = now_ms()
    for candidate in candidates:
        symbol = candidate.symbol.upper()
        last_close_ms = symbol_recent_close_ms.get(symbol, 0)
        if last_close_ms <= 0:
            continue
        remaining_ms = cooldown_ms - (cur_ms - last_close_ms)
        if remaining_ms <= 0:
            symbol_recent_close_ms.pop(symbol, None)
            continue
        if candidate_symbol_is_strong_flat(candidate, exchanges, config, assumed_flat_until_ms):
            symbol_recent_close_ms.pop(symbol, None)
            continue

        reason = "symbol_recently_closed_settling"
        if reason not in candidate.block_reasons:
            candidate.block_reasons.append(reason)
        candidate.can_enter = False

        last_log_ms = (recent_close_block_logged_ms or {}).get(symbol, 0)
        if recent_close_block_logged_ms is not None and cur_ms - last_log_ms >= 1000:
            recent_close_block_logged_ms[symbol] = cur_ms
            record_live_event(
                config,
                "entry_blocked_recent_symbol_close",
                symbol=symbol,
                remaining_ms=remaining_ms,
                reason="recent_close",
            )
    return rank_candidates(candidates)


def find_open_pair(
    open_pairs: dict[str, OpenPairState],
    token: str,
) -> OpenPairState | None:
    normalized = token.strip()
    if not normalized:
        return None

    ordered_pairs = list(open_pairs.values())
    if normalized.isdigit():
        index = int(normalized) - 1
        if 0 <= index < len(ordered_pairs):
            return ordered_pairs[index]

    for pair in ordered_pairs:
        if pair.pair_id == normalized or pair.pair_id.startswith(normalized):
            return pair
    return None


def render_candidate_detail(candidate: PairCandidate) -> str:
    lines = []
    lines.append(f"[strategy][candidate] id={candidate.id}")
    lines.append(f"symbol={candidate.symbol}")
    lines.append(f"long_exchange={candidate.long_exchange} long_ask={candidate.long_ask}")
    lines.append(f"short_exchange={candidate.short_exchange} short_bid={candidate.short_bid}")
    lines.append(
        f"target_qty={candidate.target_qty:.6f} executable_qty={candidate.executable_qty:.6f} "
        f"notional_usd={candidate.notional_usd:.4f}"
    )
    lines.append(
        f"min_order_notional long={candidate.long_min_order_notional_usd:.4f} "
        f"short={candidate.short_min_order_notional_usd:.4f}"
    )
    lines.append(f"entry_open_edge_usd={candidate.entry_open_edge_usd:.6f} entry_open_edge_bps={candidate.entry_open_edge_bps:.4f}")
    lines.append(f"open_fee_cost_usd={candidate.open_fee_cost_usd:.6f}")
    lines.append(
        f"funding raw_usd={candidate.raw_funding_pnl_usd:.6f} "
        f"effective_usd={candidate.effective_funding_pnl_usd:.6f}"
    )
    lines.append(
        f"funding factors confidence={candidate.funding_confidence:.4f} "
        f"alignment={candidate.funding_alignment_factor:.4f} "
        f"time_decay={candidate.funding_time_decay_factor:.4f}"
    )
    lines.append(f"expected_close_cost_usd={candidate.expected_close_cost_usd:.6f}")
    lines.append(f"risk_buffer_usd={candidate.risk_buffer_usd:.6f}")
    lines.append(f"stale_data_penalty_usd={candidate.stale_data_penalty_usd:.6f}")
    lines.append(f"expected_net_edge_usd={candidate.expected_net_edge_usd:.6f} expected_net_edge_bps={candidate.expected_net_edge_bps:.4f}")
    lines.append(
        f"next_funding long={candidate.long_next_funding_ms} short={candidate.short_next_funding_ms}"
    )
    lines.append(f"can_enter={candidate.can_enter} block_reasons={candidate.block_reasons}")
    return "\n".join(lines)


def render_open_pairs(open_pairs: dict[str, OpenPairState]) -> str:
    if not open_pairs:
        return "[strategy][pairs] no open pairs"
    lines = []
    for index, pair in enumerate(open_pairs.values(), start=1):
        lines.append(
            f"[strategy][pairs] idx={index} pair_id={pair.pair_id} symbol={pair.symbol} "
            f"qty={pair.qty:.6f} long={pair.long_exchange} short={pair.short_exchange} "
            f"status={pair.status} hold={pair.last_hold_decision} hold_pnl={pair.last_hold_pnl_usd:.4f} "
            f"close_pnl={pair.last_close_pnl_usd:.4f}"
        )
    return "\n".join(lines)


def marketable_limit_price(exchange: Any, symbol: str, side: str) -> float:
    return order_price_policy.marketable_limit_price(exchange, symbol, side)


def marketable_limit_price_with_bps(exchange: Any, symbol: str, side: str, bps: float) -> float:
    return order_price_policy.marketable_limit_price_with_bps(exchange, symbol, side, bps)


def maker_close_limit_price(exchange: Any, symbol: str, side: str, bps: float = 0.0) -> float:
    return order_price_policy.maker_close_limit_price(exchange, symbol, side, bps)


def marketable_entry_target_price(exchange: Any, symbol: str, side: str, config: StrategyConfig) -> float:
    return order_price_policy.marketable_entry_target_price(exchange, symbol, side, config)


def spread_buffer_entry_target_price(
    candidate: PairCandidate,
    first_exchange_name: str,
    first_side: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    return order_price_policy.spread_buffer_entry_target_price(
        candidate,
        first_exchange_name,
        first_side,
        exchanges,
        config,
    )


def normalize_limit_price_for_side(exchange: Any, symbol: str, side: str, raw_price: float) -> float:
    return order_price_policy.normalize_limit_price_for_side(exchange, symbol, side, raw_price)


def passive_entry_target_price(
    candidate: PairCandidate,
    first_exchange_name: str,
    first_side: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    return order_price_policy.passive_entry_target_price(
        candidate,
        first_exchange_name,
        first_side,
        exchanges,
        config,
    )


def required_edge_entry_target_price(
    candidate: PairCandidate,
    first_exchange_name: str,
    first_side: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    return order_price_policy.required_edge_entry_target_price(
        candidate,
        first_exchange_name,
        first_side,
        exchanges,
        config,
    )


def normalize_exchange_qty(exchange: Any, symbol: str, qty: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    min_qty = exchange_min_qty(exchange, symbol)
    max_qty = exchange_max_qty(exchange, symbol)

    normalized = floor_by_step(abs(qty), step_size) if step_size > 0 else abs(qty)
    if max_qty > 0:
        normalized = min(normalized, max_qty)
    if min_qty > 0 and normalized + EPS < min_qty:
        return 0.0
    return normalized


def exchange_min_order_notional_usd(exchange: Any, symbol: str, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    explicit_min_notional = _safe_float(meta.get("min_notional"), 0.0)
    min_qty = exchange_min_qty(exchange, symbol)
    implied_min_notional = (min_qty * price) if min_qty > 0 and price > 0 else 0.0
    return max(explicit_min_notional, implied_min_notional)


def exchange_min_executable_qty(exchange: Any, symbol: str, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    min_qty = exchange_min_qty(exchange, symbol)
    min_notional = exchange_min_order_notional_usd(exchange, symbol, price)
    min_qty_from_notional = (min_notional / price) if min_notional > 0 and price > 0 else 0.0
    required_qty = max(min_qty, min_qty_from_notional)
    if required_qty <= 0:
        return 0.0
    if step_size > 0:
        return ceil_by_step(required_qty, step_size)
    return required_qty


def order_execution_price_from_snapshot(
    snapshot: dict[str, Any],
    side: str,
    aggressive_bps: float,
) -> float:
    normalized_side = side.lower()
    if normalized_side == "buy":
        best_ask = extract_best_ask(snapshot)
        if best_ask > 0:
            return best_ask * (1.0 + (aggressive_bps / 10000.0))
    else:
        best_bid = extract_best_bid(snapshot)
        if best_bid > 0:
            return best_bid * (1.0 - (aggressive_bps / 10000.0))
    return extract_mid_price(snapshot)


def exchange_order_execution_preview(
    exchange: Any,
    symbol: str,
    side: str,
    target_qty: float,
    aggressive_bps: float,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = snapshot or exchange.get_symbol_snapshot(symbol)
    price = order_execution_price_from_snapshot(snapshot, side, aggressive_bps)
    if price > 0:
        executable_qty = normalize_exchange_qty_for_order(exchange, symbol, target_qty, price)
        min_executable_qty = exchange_min_executable_qty(exchange, symbol, price)
    else:
        executable_qty = normalize_exchange_qty(exchange, symbol, target_qty)
        min_executable_qty = exchange_min_qty(exchange, symbol)
    return {
        "side": side,
        "price": price,
        "target_qty": abs(target_qty),
        "executable_qty": executable_qty,
        "min_executable_qty": min_executable_qty,
    }


def normalize_exchange_qty_for_order(exchange: Any, symbol: str, qty: float, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    max_qty = exchange_max_qty(exchange, symbol)

    normalized = floor_by_step(abs(qty), step_size) if step_size > 0 else abs(qty)
    if max_qty > 0:
        normalized = min(normalized, max_qty)

    required_qty = exchange_min_executable_qty(exchange, symbol, price)
    if required_qty > 0 and normalized + EPS < required_qty:
        return 0.0
    return normalized


def exchange_min_qty(exchange: Any, symbol: str) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    return _safe_float(meta.get("min_qty"), _safe_float(meta.get("min_q"), 0.0))


def exchange_max_qty(exchange: Any, symbol: str) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    return _safe_float(meta.get("max_qty"), _safe_float(meta.get("max_q"), 0.0))


def exchange_position_close_preview(
    exchange: Any,
    symbol: str,
    current_qty: float,
    aggressive_bps: float = 0.0,
) -> dict[str, Any]:
    side = "sell" if current_qty > 0 else "buy"
    raw_qty = abs(current_qty)
    price = marketable_limit_price_with_bps(exchange, symbol, side, aggressive_bps)
    price_source = "marketable_bps"
    if price <= 0:
        price = extract_mid_price(exchange.get_symbol_snapshot(symbol))
        price_source = "mid"

    if price > 0:
        close_qty = normalize_exchange_qty_for_order(exchange, symbol, raw_qty, price)
        min_executable_qty = exchange_min_executable_qty(exchange, symbol, price)
        min_notional_usd = exchange_min_order_notional_usd(exchange, symbol, price)
    else:
        close_qty = normalize_exchange_qty(exchange, symbol, raw_qty)
        min_executable_qty = exchange_min_qty(exchange, symbol)
        min_notional_usd = 0.0
        price_source = "missing"

    return {
        "side": side,
        "raw_qty": raw_qty,
        "close_qty": close_qty,
        "price": price,
        "price_source": price_source,
        "notional_usd": raw_qty * price if price > 0 else 0.0,
        "min_executable_qty": min_executable_qty,
        "min_notional_usd": min_notional_usd,
    }


def classify_micro_residual(
    exchange: Any,
    symbol: str,
    side: str,
    residual_qty: float,
    reference_qty: float,
    config: StrategyConfig,
    *,
    aggressive_bps: float | None = None,
    snapshot: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    qty = abs(residual_qty)
    bps = config.close_aggressive_bps if aggressive_bps is None else aggressive_bps
    preview = exchange_order_execution_preview(exchange, symbol, side, qty, bps, snapshot=snapshot)
    price = _safe_float(preview.get("price"), 0.0)
    notional_usd = qty * price if price > 0 else 0.0
    ref_qty = abs(reference_qty)
    ratio = (qty / ref_qty) if ref_qty > EPS else 0.0
    by_ratio = ref_qty > EPS and ratio <= max(config.micro_residual_info_ratio, 0.0)
    by_notional = (
        config.micro_residual_info_notional_usd > 0
        and notional_usd > 0
        and notional_usd <= config.micro_residual_info_notional_usd
    )
    is_micro = qty > EPS and by_ratio and (by_notional or price <= 0)
    return (
        is_micro,
        {
            "qty": qty,
            "reference_qty": ref_qty,
            "ratio": ratio,
            "notional_usd": notional_usd,
            "price": price,
            "executable_qty": _safe_float(preview.get("executable_qty"), 0.0),
            "min_executable_qty": _safe_float(preview.get("min_executable_qty"), 0.0),
            "micro_notional_threshold_usd": config.micro_residual_info_notional_usd,
            "micro_ratio_threshold": config.micro_residual_info_ratio,
        },
    )


def print_emergency_flatten_dust(
    exchange: Any,
    symbol: str,
    preview: dict[str, Any],
    *,
    reason: str,
    closed_qty: float | None = None,
) -> None:
    exchange_name = getattr(exchange, "persist_name", getattr(exchange, "name", "unknown"))
    suffix = "" if closed_qty is None else f" closed_qty={closed_qty:.12f}"
    print(
        f"[페어종료][소량잔량] 정리불가소량으로 flat 간주 "
        f"사유={reason} 거래소={exchange_name} 심볼={symbol} 방향={korean_side(preview.get('side'))} "
        f"수량={_safe_float(preview.get('raw_qty')):.12f} "
        f"정리수량={_safe_float(preview.get('close_qty')):.12f} "
        f"최소수량={_safe_float(preview.get('min_executable_qty')):.12f} "
        f"가격={_safe_float(preview.get('price')):.8f} "
        f"명목USD={_safe_float(preview.get('notional_usd')):.8f} "
        f"최소명목USD={_safe_float(preview.get('min_notional_usd')):.8f} "
        f"가격출처={preview.get('price_source')}{suffix}"
    )


def compute_common_executable_qty(
    left_exchange: Any,
    right_exchange: Any,
    symbol: str,
    qty: float,
    left_price: float = 0.0,
    right_price: float = 0.0,
) -> float:
    executable = abs(qty)
    if left_price <= 0:
        left_price = extract_mid_price(left_exchange.get_symbol_snapshot(symbol))
    if right_price <= 0:
        right_price = extract_mid_price(right_exchange.get_symbol_snapshot(symbol))
    for _ in range(4):
        left_qty = normalize_exchange_qty_for_order(left_exchange, symbol, executable, left_price)
        right_qty = normalize_exchange_qty_for_order(right_exchange, symbol, executable, right_price)
        next_qty = min(left_qty, right_qty)
        if abs(next_qty - executable) <= EPS:
            return next_qty
        executable = next_qty
        if executable <= EPS:
            return 0.0
    return executable


def position_fill_delta(pre_qty: float, post_qty: float, side: str) -> float:
    multiplier = 1.0 if side.lower() == "buy" else -1.0
    return max((post_qty - pre_qty) * multiplier, 0.0)


def entry_delta_qty(pre_qty: float, post_qty: float, role: str) -> float:
    if role == "long":
        return position_fill_delta(pre_qty, post_qty, "buy")
    if role == "short":
        return position_fill_delta(pre_qty, post_qty, "sell")
    return 0.0


def position_close_delta(pre_qty: float, post_qty: float, side: str) -> float:
    if side.lower() == "sell":
        return max(pre_qty - post_qty, 0.0)
    return max(post_qty - pre_qty, 0.0)


async def wait_for_position_fill(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
) -> float:
    return await _execution_wait_for_position_fill(
        exchange,
        symbol,
        side,
        pre_qty,
        target_qty,
        timeout_sec,
        extract_position_qty=extract_position_qty,
        position_fill_delta=position_fill_delta,
        eps=EPS,
    )


async def wait_for_position_close(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
) -> float:
    return await _execution_wait_for_position_close(
        exchange,
        symbol,
        side,
        pre_qty,
        target_qty,
        timeout_sec,
        extract_position_qty=extract_position_qty,
        position_close_delta=position_close_delta,
        eps=EPS,
    )


def extract_order_uuid(order_result: dict[str, Any] | None) -> str | None:
    return _execution_extract_order_uuid(order_result)


def extract_order_avg_price(order_result: dict[str, Any] | None, fallback_price: float) -> float:
    return _execution_extract_order_avg_price(order_result, fallback_price)


def pair_entry_spread_usd(pair: OpenPairState) -> float:
    return pair.qty * (pair.short_entry_price - pair.long_entry_price)


def current_pair_close_edge(pair: OpenPairState, exchanges: dict[str, Any]) -> tuple[float, float, float]:
    long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
    short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
    long_bid = extract_best_bid(long_snapshot)
    short_ask = extract_best_ask(short_snapshot)
    close_edge_usd = pair.qty * (long_bid - short_ask) if long_bid > 0 and short_ask > 0 else 0.0
    return long_bid, short_ask, close_edge_usd


def current_pair_open_edge(pair: OpenPairState, exchanges: dict[str, Any]) -> tuple[float, float, float]:
    long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
    short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
    long_ask = extract_best_ask(long_snapshot)
    short_bid = extract_best_bid(short_snapshot)
    open_edge_usd = pair.qty * (short_bid - long_ask) if long_ask > 0 and short_bid > 0 else 0.0
    return long_ask, short_bid, open_edge_usd


def bootstrap_cache_key(exchange_name: str, symbol: str, config: StrategyConfig) -> tuple[str, str, int, str]:
    return (
        normalize_exchange_name(exchange_name),
        normalize_symbol(symbol),
        int(config.leverage),
        str(config.margin_mode or "").strip().lower(),
    )


async def bootstrap_exchange_symbol_once(
    exchange_name: str,
    symbol: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> list[dict[str, Any]]:
    if not config.bootstrap_on_entry:
        return []

    exchange_name = normalize_exchange_name(exchange_name)
    symbol = normalize_symbol(symbol)
    key = bootstrap_cache_key(exchange_name, symbol, config)
    if key in _BOOTSTRAP_SUCCESS_CACHE:
        return []

    exchange = exchanges.get(exchange_name)
    if exchange is None:
        return [
            {
                "exchange": exchange_name,
                "symbol": symbol,
                "field": "bootstrap",
                "error": "exchange_not_configured",
            }
        ]

    result = await exchange.bootstrap_symbol(
        symbol=symbol,
        leverage=config.leverage,
        margin_mode=config.margin_mode,
    )
    failures = bootstrap_result_failures(
        exchange_name=exchange_name,
        symbol=symbol,
        result=result,
    )
    if failures:
        return failures

    _BOOTSTRAP_SUCCESS_CACHE.add(key)
    return []


async def bootstrap_strategy_symbols_once(
    exchanges: dict[str, Any],
    symbols: list[str],
    config: StrategyConfig,
) -> list[dict[str, Any]]:
    if not config.bootstrap_on_entry:
        return []

    failures: list[dict[str, Any]] = []
    attempted_keys: set[tuple[str, str]] = set()
    for exchange_name in sorted(exchanges.keys()):
        for symbol in symbols:
            attempted_keys.add((normalize_exchange_name(exchange_name), normalize_symbol(symbol)))
            failures.extend(
                await bootstrap_exchange_symbol_once(
                    exchange_name,
                    symbol,
                    exchanges,
                    config,
                )
            )

    failed_keys = {
        (normalize_exchange_name(str(row.get("exchange", ""))), normalize_symbol(str(row.get("symbol", ""))))
        for row in failures
        if row.get("exchange") and row.get("symbol")
    }
    record_live_event(
        config,
        "startup_bootstrap_symbols_done",
        exchange_count=len(exchanges),
        symbol_count=len(symbols),
        attempted_count=len(attempted_keys),
        success_count=max(0, len(attempted_keys) - len(failed_keys)),
        failure_count=len(failures),
        failures=failures[:20],
    )

    # Validate each exchange exposes the method + snapshot SHAPE the strategy
    # relies on. Catches a new/partial exchange adapter at startup instead of
    # crashing mid-trade on a missing snapshot field.
    sample_symbol = symbols[0] if symbols else None
    for exchange_name, exchange in exchanges.items():
        iface = validate_exchange_interface(exchange)
        if not iface["ok"]:
            print(f"[strategy][interface][경고] 거래소={exchange_name} 누락메서드={iface['missing_methods']}")
            record_live_event(config, "exchange_interface_invalid", exchange=exchange_name, **iface)
        if sample_symbol is not None:
            shape = validate_exchange_snapshot_shape(exchange, sample_symbol)
            if not shape["ok"]:
                print(f"[strategy][snapshot][경고] 거래소={exchange_name} 스냅샷이슈={shape['issues']}")
                record_live_event(config, "exchange_snapshot_shape_invalid", exchange=exchange_name, **shape)

    return failures


async def bootstrap_candidate_exchanges(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> list[dict[str, Any]]:
    if not config.bootstrap_on_entry:
        return []
    failures: list[dict[str, Any]] = []
    for exchange_name in {candidate.long_exchange, candidate.short_exchange}:
        failures.extend(
            await bootstrap_exchange_symbol_once(
                exchange_name,
                candidate.symbol,
                exchanges,
                config,
            )
        )
    return failures


def bootstrap_result_failures(exchange_name: str, symbol: str, result: Any) -> list[dict[str, Any]]:
    if not isinstance(result, dict):
        return [
            {
                "exchange": exchange_name,
                "symbol": symbol,
                "field": "bootstrap",
                "error": "invalid_bootstrap_response",
                "raw": repr(result),
            }
        ]

    rows: list[tuple[str, Any]]
    if "ok" in result:
        rows = [("bootstrap", result)]
    else:
        rows = [(str(key), value) for key, value in result.items()]

    failures: list[dict[str, Any]] = []
    for field, value in rows:
        if not isinstance(value, dict):
            continue
        if value.get("ok") is not False:
            continue
        failures.append(
            {
                "exchange": exchange_name,
                "symbol": symbol,
                "field": field,
                "error": value.get("error") or value.get("msg") or value.get("message") or "bootstrap_failed",
                "raw": value.get("raw", value),
            }
        )
    return failures


async def cancel_if_open(exchange: Any, symbol: str, order_result: dict[str, Any] | None) -> Any:
    return await _execution_cancel_if_open(
        exchange,
        symbol,
        order_result,
        extract_order_uuid=extract_order_uuid,
    )


def limit_price_changed(exchange: Any, symbol: str, previous_price: float, target_price: float) -> bool:
    if previous_price <= 0 or target_price <= 0:
        return True
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    tick_size = _safe_float(meta.get("tick_size"), 0.0)
    if tick_size > 0:
        return abs(previous_price - target_price) + EPS >= tick_size
    return abs(previous_price - target_price) > max(EPS, abs(previous_price) * 1e-9)


def entry_limit_price_changed(
    exchange: Any,
    symbol: str,
    previous_price: float,
    target_price: float,
    config: StrategyConfig,
) -> bool:
    if config.entry_first_leg_price_mode != "spread_buffer":
        return limit_price_changed(exchange, symbol, previous_price, target_price)
    if previous_price <= 0 or target_price <= 0:
        return True
    buffer_bps = max(config.entry_cancel_buffer_bps, 0.0)
    if buffer_bps <= 0:
        return limit_price_changed(exchange, symbol, previous_price, target_price)
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    tick_size = _safe_float(meta.get("tick_size"), 0.0)
    price_gap = abs(previous_price - target_price)
    buffer_gap = abs(target_price) * (buffer_bps / 2.0) / 10000.0
    min_gap = max(buffer_gap, tick_size if tick_size > 0 else EPS)
    return price_gap > min_gap + EPS


def is_waiting_order_active(exchange: Any, order_result: dict[str, Any] | None) -> bool:
    order_uuid = extract_order_uuid(order_result)
    if not order_uuid:
        return False
    for row in getattr(exchange, "waiting_list", []) or []:
        if extract_order_uuid(row) == order_uuid:
            return True
    return False


async def execute_requoting_limit_leg(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    target_qty: float,
    pre_qty: float,
    timeout_sec: float,
    config: StrategyConfig,
    is_close: bool,
    price_resolver,
    log_prefix: str,
    probe_reason_prefix: str,
    last_order_submit_ms_by_key: dict[tuple[str, str, str], int] | None = None,
    telemetry: dict[str, Any] | None = None,
) -> tuple[float, dict[str, Any] | None, float]:
    return await _execution_execute_requoting_limit_leg(
        exchange=exchange,
        symbol=symbol,
        side=side,
        target_qty=target_qty,
        pre_qty=pre_qty,
        timeout_sec=timeout_sec,
        config=config,
        is_close=is_close,
        price_resolver=price_resolver,
        log_prefix=log_prefix,
        probe_reason_prefix=probe_reason_prefix,
        resolve_effective_order_qty=resolve_effective_order_qty,
        is_waiting_order_active=is_waiting_order_active,
        limit_price_changed=(
            (lambda exch, sym, prev, target: entry_limit_price_changed(exch, sym, prev, target, config))
            if not is_close
            else limit_price_changed
        ),
        cancel_if_open=cancel_if_open,
        normalize_exchange_qty_for_order=normalize_exchange_qty_for_order,
        print_verbose=print_verbose,
        settle_grace_sec=settle_grace_sec,
        wait_for_position_fill=wait_for_position_fill,
        wait_for_position_close=wait_for_position_close,
        record_order_event=lambda event_type, **payload: record_live_event(config, event_type, **payload),
        last_order_submit_ms_by_key=last_order_submit_ms_by_key,
        extract_position_qty=extract_position_qty,
        telemetry=telemetry,
        eps=EPS,
    )


async def resolve_effective_order_qty(
    *,
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    order_result: dict[str, Any] | None,
    is_close: bool,
    reason: str,
) -> float:
    return await _execution_resolve_effective_order_qty(
        exchange=exchange,
        symbol=symbol,
        side=side,
        pre_qty=pre_qty,
        target_qty=target_qty,
        order_result=order_result,
        is_close=is_close,
        reason=reason,
        refresh_symbol_state=refresh_symbol_state,
        extract_position_qty=extract_position_qty,
        position_close_delta=position_close_delta,
        position_fill_delta=position_fill_delta,
        eps=EPS,
    )


def settle_grace_sec(config: StrategyConfig) -> float:
    return _execution_settle_grace_sec(config)


def use_reduce_only_market_close(exchange: Any, current_qty: float, side: str) -> bool:
    return True


def qty_match_tolerance(reference_qty: float) -> float:
    return max(1e-6, abs(reference_qty) * 1e-6)


def classify_pair_position_state(
    long_qty: float,
    short_qty: float,
    reference_qty: float = 0.0,
) -> tuple[str, float, float]:
    tolerance = qty_match_tolerance(max(abs(long_qty), abs(short_qty), abs(reference_qty)))
    long_flat = abs(long_qty) <= tolerance
    short_flat = abs(short_qty) <= tolerance
    if long_flat and short_flat:
        return ("closed", 0.0, 0.0)
    if long_qty > tolerance and short_qty < -tolerance:
        balanced_qty = min(abs(long_qty), abs(short_qty))
        residual_gap = abs(abs(long_qty) - abs(short_qty))
        if residual_gap <= tolerance:
            return ("balanced_open", balanced_qty, residual_gap)
        return ("imbalanced_open", balanced_qty, residual_gap)
    return ("direction_mismatch", 0.0, max(abs(long_qty), abs(short_qty)))


def error_means_no_position_to_close(order_result: Any) -> bool:
    text = str(order_result).lower()
    return (
        "51169" in text
        or "don't have any positions in this direction" in text
        or "no positions in this direction" in text
    )


def flatten_error_details(order_result: Any, exc: Exception | None = None) -> tuple[str, str]:
    if exc is not None:
        return (exc.__class__.__name__, str(exc))
    if not isinstance(order_result, dict):
        text = str(order_result or "")
        return ("", text)

    raw = order_result.get("raw")
    code = (
        order_result.get("code")
        or order_result.get("error_code")
        or order_result.get("status_code")
        or order_result.get("http_status")
    )
    message = order_result.get("message") or order_result.get("msg") or order_result.get("error")
    if isinstance(raw, dict):
        code = code or raw.get("code") or raw.get("sCode") or raw.get("status_code")
        message = message or raw.get("msg") or raw.get("message") or raw.get("sMsg")
    elif raw not in (None, ""):
        message = message or str(raw)
    return (str(code or ""), str(message or order_result))


def classify_flatten_failure(order_result: Any, exc: Exception | None = None) -> tuple[str, str, str, bool]:
    code, message = flatten_error_details(order_result, exc)
    text = f"{code} {message} {order_result}".lower()
    temporarily_unavailable = (
        "50001" in text
        or "503" in text
        or "service temporarily unavailable" in text
        or "temporarily unavailable" in text
        or "service unavailable" in text
        or "retry exhausted" in text
        or "retry_exhausted" in text
    )
    if temporarily_unavailable:
        return ("exchange_temporarily_unavailable", code, message, True)
    if exc is not None:
        return ("exception", code, message, False)
    if error_means_no_position_to_close(order_result):
        return ("dust_assumed_flat", code, message, False)
    if isinstance(order_result, dict) and order_result.get("error"):
        return ("order_rejected", code, message, False)
    return ("remaining_actionable_after_retries", code, message, False)


async def refresh_symbol_state(
    exchange: Any,
    symbol: str,
    reason: str,
    *,
    include_account: bool = False,
    include_open_orders: bool = True,
    include_funding: bool = False,
) -> None:
    reconcile = getattr(exchange, "reconcile_state", None)
    if not callable(reconcile):
        return
    try:
        await reconcile(
            include_account=include_account,
            include_open_orders=include_open_orders,
            include_funding=include_funding,
            reason=reason,
        )
    except TypeError:
        try:
            await reconcile(reason=reason)
        except Exception as exc:
            print(f"[strategy][refresh_state][warn] symbol={symbol} reason={reason} error={exc}")
    except Exception as exc:
        print(f"[strategy][refresh_state][warn] symbol={symbol} reason={reason} error={exc}")


async def settle_pair_close_state(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    reference_qty: float,
    reason_prefix: str,
    attempts: int = 4,
    forced_flat_exchanges: set[str] | None = None,
) -> tuple[str, float, float, float, float]:
    forced_flat_exchanges = forced_flat_exchanges or set()
    last_state = "direction_mismatch"
    last_balanced_qty = 0.0
    last_gap = 0.0
    long_qty = 0.0
    short_qty = 0.0
    for attempt in range(attempts):
        await refresh_symbol_state(exchanges[pair.long_exchange], pair.symbol, f"{reason_prefix}_long_{attempt + 1}")
        await refresh_symbol_state(exchanges[pair.short_exchange], pair.symbol, f"{reason_prefix}_short_{attempt + 1}")
        long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
        short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
        long_qty = effective_position_qty(long_snapshot, exchanges[pair.long_exchange], pair.symbol, config)
        short_qty = effective_position_qty(short_snapshot, exchanges[pair.short_exchange], pair.symbol, config)
        if pair.long_exchange in forced_flat_exchanges:
            long_qty = 0.0
        if pair.short_exchange in forced_flat_exchanges:
            short_qty = 0.0
        state, balanced_qty, gap = classify_pair_position_state(long_qty, short_qty, reference_qty=reference_qty)
        last_state = state
        last_balanced_qty = balanced_qty
        last_gap = gap
        if state in {"closed", "balanced_open"}:
            return (state, long_qty, short_qty, balanced_qty, gap)
        if attempt + 1 < attempts:
            await asyncio.sleep(config.action_settle_wait_sec)
    return (last_state, long_qty, short_qty, last_balanced_qty, last_gap)


async def emergency_flatten_symbol_state_detailed(
    exchange: Any,
    symbol: str,
    aggressive_bps: float = 20.0,
    *,
    role: str = "",
) -> FlattenLegResult:
    exchange_name = getattr(exchange, "persist_name", getattr(exchange, "name", "unknown"))
    result = FlattenLegResult(exchange_name=exchange_name, symbol=symbol, role=role)
    for attempt in range(3):
        await refresh_symbol_state(exchange, symbol, f"strategy_emergency_flatten_{attempt + 1}")
        current_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        if attempt == 0:
            result.start_qty = current_qty
        if abs(current_qty) <= EPS:
            result.end_qty = current_qty
            result.ok = True
            return result

        close_preview = exchange_position_close_preview(
            exchange,
            symbol,
            current_qty,
            aggressive_bps=aggressive_bps,
        )
        side = str(close_preview["side"])
        close_qty = _safe_float(close_preview.get("close_qty"), 0.0)
        result.side = side
        result.requested_qty = close_qty
        if close_qty <= EPS:
            print_emergency_flatten_dust(
                exchange,
                symbol,
                close_preview,
                reason="below_min_executable_qty_before_order",
            )
            result.end_qty = current_qty
            result.ok = True
            result.assumed_flat = True
            result.failure_reason = "dust_assumed_flat"
            return result

        try:
            log_pair_close_attempt(
                attempt=attempt + 1,
                exchange=getattr(exchange, "persist_name", getattr(exchange, "name", "unknown")),
                symbol=symbol,
                side=side,
                qty=close_qty,
                price_preview=_safe_float(close_preview.get("price")),
                min_executable_qty=_safe_float(close_preview.get("min_executable_qty")),
            )
            result.attempted_order = True
            market_result = await exchange.market_order(
                symbol,
                side,
                close_qty,
                is_close=use_reduce_only_market_close(exchange, current_qty, side),
            )
            if not (market_result and market_result.get("error")):
                request_account_refresh(
                    exchange,
                    "emergency_flatten_market_order",
                    priority="urgent",
                    symbol=symbol,
                )
                resolved_close_qty = await resolve_effective_order_qty(
                    exchange=exchange,
                    symbol=symbol,
                    side=side,
                    pre_qty=current_qty,
                    target_qty=close_qty,
                    order_result=market_result,
                    is_close=True,
                    reason=f"strategy_emergency_flatten_market_{attempt + 1}",
                )
                result.closed_qty += max(resolved_close_qty, 0.0)
                await asyncio.sleep(1.2)
                await refresh_symbol_state(exchange, symbol, f"strategy_emergency_flatten_check_{attempt + 1}")
                current_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
                if abs(current_qty) <= EPS:
                    result.end_qty = current_qty
                    result.ok = True
                    return result
                remaining_preview = exchange_position_close_preview(
                    exchange,
                    symbol,
                    current_qty,
                    aggressive_bps=aggressive_bps,
                )
                remaining_close_qty = _safe_float(remaining_preview.get("close_qty"), 0.0)
                if remaining_close_qty <= EPS:
                    print_emergency_flatten_dust(
                        exchange,
                        symbol,
                        remaining_preview,
                        reason="below_min_executable_qty_after_order",
                        closed_qty=resolved_close_qty,
                    )
                    result.end_qty = current_qty
                    result.ok = True
                    result.assumed_flat = True
                    result.failure_reason = "dust_assumed_flat"
                    return result
            else:
                if error_means_no_position_to_close(market_result):
                    result.end_qty = current_qty
                    result.ok = True
                    result.assumed_flat = True
                    result.failure_reason = "dust_assumed_flat"
                    return result
                failure_reason, error_code, error_message, temporarily_unavailable = classify_flatten_failure(market_result)
                result.failure_reason = failure_reason
                result.error_code = error_code
                result.error_message = error_message
                result.temporarily_unavailable = temporarily_unavailable
                print(
                    f"[페어종료][경고] 시장가 종료 주문 실패 "
                    f"거래소={getattr(exchange, 'persist_name', getattr(exchange, 'name', 'unknown'))} "
                    f"심볼={symbol} 사유={failure_reason} 결과={market_result}"
                )
                await asyncio.sleep(0.8)
        except Exception as exc:
            failure_reason, error_code, error_message, temporarily_unavailable = classify_flatten_failure(None, exc)
            result.failure_reason = failure_reason
            result.error_code = error_code
            result.error_message = error_message
            result.temporarily_unavailable = temporarily_unavailable
            print(
                f"[페어종료][경고] 시장가 종료 주문 오류 "
                f"심볼={symbol} 사유={failure_reason} 오류={exc}"
            )
            await asyncio.sleep(0.8)

    remaining_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    result.end_qty = remaining_qty
    if abs(remaining_qty) <= EPS:
        result.ok = True
        return result
    remaining_preview = exchange_position_close_preview(
        exchange,
        symbol,
        remaining_qty,
        aggressive_bps=aggressive_bps,
    )
    if _safe_float(remaining_preview.get("close_qty"), 0.0) <= EPS:
        print_emergency_flatten_dust(
            exchange,
            symbol,
            remaining_preview,
            reason="below_min_executable_qty_after_retries",
        )
        result.ok = True
        result.assumed_flat = True
        result.failure_reason = "dust_assumed_flat"
        return result
    if not result.failure_reason:
        failure_reason, error_code, error_message, temporarily_unavailable = classify_flatten_failure(None)
        result.failure_reason = failure_reason
        result.error_code = error_code
        result.error_message = error_message
        result.temporarily_unavailable = temporarily_unavailable
    return result


async def emergency_flatten_symbol_state(
    exchange: Any,
    symbol: str,
    aggressive_bps: float = 20.0,
) -> tuple[bool, bool]:
    result = await emergency_flatten_symbol_state_detailed(exchange, symbol, aggressive_bps=aggressive_bps)
    return (result.ok, result.assumed_flat)


async def emergency_flatten_symbol(exchange: Any, symbol: str, aggressive_bps: float = 20.0) -> bool:
    ok, _ = await emergency_flatten_symbol_state(exchange, symbol, aggressive_bps=aggressive_bps)
    return ok


async def close_leg_market_qty(
    *,
    exchange: Any,
    symbol: str,
    role: str,
    side: str,
    target_qty: float,
    config: StrategyConfig,
    is_close: bool,
) -> FlattenLegResult:
    exchange_name = getattr(exchange, "persist_name", getattr(exchange, "name", "unknown"))
    result = FlattenLegResult(exchange_name=exchange_name, symbol=symbol, role=role, side=side)
    await refresh_symbol_state(exchange, symbol, f"strategy_close_exact_pre_{role}")
    pre_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    result.start_qty = pre_qty

    preview = exchange_order_execution_preview(
        exchange,
        symbol,
        side,
        target_qty,
        config.close_aggressive_bps,
    )
    close_qty = _safe_float(preview.get("executable_qty"), 0.0)
    result.requested_qty = close_qty
    if close_qty <= EPS:
        result.end_qty = pre_qty
        result.ok = True
        result.assumed_flat = True
        result.failure_reason = "dust_assumed_flat"
        return result

    try:
        log_pair_close_attempt(
            attempt=1,
            exchange=exchange_name,
            symbol=symbol,
            side=side,
            qty=close_qty,
            price_preview=_safe_float(preview.get("price")),
            min_executable_qty=_safe_float(preview.get("min_executable_qty")),
        )
        result.attempted_order = True
        market_kwargs: dict[str, Any] = {"is_hedge": True}
        if is_close:
            market_kwargs["is_close"] = use_reduce_only_market_close(exchange, pre_qty, side)
        market_result = await exchange.market_order(symbol, side, close_qty, **market_kwargs)
        if market_result and market_result.get("error"):
            if is_close and error_means_no_position_to_close(market_result):
                result.ok = True
                result.assumed_flat = True
                result.failure_reason = "dust_assumed_flat"
                return result
            failure_reason, error_code, error_message, temporarily_unavailable = classify_flatten_failure(market_result)
            result.failure_reason = failure_reason
            result.error_code = error_code
            result.error_message = error_message
            result.temporarily_unavailable = temporarily_unavailable
            return result

        request_account_refresh(
            exchange,
            "close_leg_market_order",
            priority="urgent",
            symbol=symbol,
        )
        await asyncio.sleep(config.action_settle_wait_sec)
        resolved_qty = await resolve_effective_order_qty(
            exchange=exchange,
            symbol=symbol,
            side=side,
            pre_qty=pre_qty,
            target_qty=close_qty,
            order_result=market_result,
            is_close=is_close,
            reason=f"strategy_close_exact_market_{role}",
        )
        result.closed_qty = resolved_qty
        await refresh_symbol_state(exchange, symbol, f"strategy_close_exact_post_{role}")
        result.end_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        if resolved_qty + qty_match_tolerance(close_qty) >= close_qty:
            result.ok = True
            return result
        result.failure_reason = "partial_fill"
        return result
    except Exception as exc:
        failure_reason, error_code, error_message, temporarily_unavailable = classify_flatten_failure(None, exc)
        result.failure_reason = failure_reason
        result.error_code = error_code
        result.error_message = error_message
        result.temporarily_unavailable = temporarily_unavailable
        result.end_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        return result


def pair_close_leg_specs(pair_like: Any, close_first_leg_role: str | None = None) -> list[dict[str, Any]]:
    specs = [
        {
            "role": "long",
            "exchange_name": pair_like.long_exchange,
            "side": "sell",
        },
        {
            "role": "short",
            "exchange_name": pair_like.short_exchange,
            "side": "buy",
        },
    ]
    if close_first_leg_role == "long":
        return specs
    if close_first_leg_role == "short":
        return [specs[1], specs[0]]
    return specs


async def restore_pair_neutrality_after_close_failure(
    pair_like: Any,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    *,
    failed_leg: FlattenLegResult,
    closed_leg: FlattenLegResult,
) -> dict[str, Any]:
    symbol = pair_like.symbol
    await refresh_symbol_state(exchanges[pair_like.long_exchange], symbol, "strategy_close_restore_long_pre")
    await refresh_symbol_state(exchanges[pair_like.short_exchange], symbol, "strategy_close_restore_short_pre")
    long_snapshot = exchanges[pair_like.long_exchange].get_symbol_snapshot(symbol)
    short_snapshot = exchanges[pair_like.short_exchange].get_symbol_snapshot(symbol)
    long_qty = effective_position_qty(long_snapshot, exchanges[pair_like.long_exchange], symbol, config)
    short_qty = effective_position_qty(short_snapshot, exchanges[pair_like.short_exchange], symbol, config)

    payload: dict[str, Any] = {
        "pair_id": getattr(pair_like, "pair_id", ""),
        "symbol": symbol,
        "failed_leg": flatten_leg_result_payload(failed_leg),
        "closed_leg": flatten_leg_result_payload(closed_leg),
        "long_qty_before_restore": long_qty,
        "short_qty_before_restore": short_qty,
        "fallback_result": "not_attempted",
    }

    if failed_leg.role == "long":
        residual_exchange = pair_like.long_exchange
        residual_qty = max(long_qty, 0.0)
        fallback_exchange_name = pair_like.short_exchange
        fallback_side = "sell"
    elif failed_leg.role == "short":
        residual_exchange = pair_like.short_exchange
        residual_qty = abs(min(short_qty, 0.0))
        fallback_exchange_name = pair_like.long_exchange
        fallback_side = "buy"
    else:
        residual_exchange = failed_leg.exchange_name
        residual_qty = max(abs(failed_leg.end_qty), abs(failed_leg.start_qty))
        fallback_exchange_name = closed_leg.exchange_name
        fallback_side = "buy" if closed_leg.role == "long" else "sell"

    payload.update(
        {
            "residual_exchange": residual_exchange,
            "residual_qty": residual_qty,
            "fallback_exchange": fallback_exchange_name,
            "fallback_side": fallback_side,
            "fallback_qty": 0.0,
        }
    )

    fallback_exchange = exchanges[fallback_exchange_name]
    fallback_price = marketable_limit_price_with_bps(
        fallback_exchange,
        symbol,
        fallback_side,
        config.close_aggressive_bps,
    )
    fallback_qty = normalize_exchange_qty_for_order(fallback_exchange, symbol, residual_qty, fallback_price)
    payload["fallback_qty"] = fallback_qty
    if fallback_qty <= EPS:
        payload["fallback_result"] = "not_executable"
        payload["remaining_net_qty"] = long_qty + short_qty
        return payload

    pre_qty = extract_position_qty(fallback_exchange.get_symbol_snapshot(symbol))
    try:
        hedge_execution = await execute_residual_hedge(
            stage_label="close_neutrality_restore",
            exchange_name=fallback_exchange_name,
            exchange=fallback_exchange,
            symbol=symbol,
            side=fallback_side,
            pre_qty=pre_qty,
            target_qty=fallback_qty,
            config=config,
            is_close=False,
            eps=EPS,
            extract_position_qty=extract_position_qty,
            marketable_limit_price_with_bps=marketable_limit_price_with_bps,
            print_verbose=print_verbose,
            use_reduce_only_market_close=use_reduce_only_market_close,
            resolve_effective_order_qty=resolve_effective_order_qty,
            extract_order_avg_price=extract_order_avg_price,
            wait_for_position_fill=wait_for_position_fill,
            wait_for_position_close=wait_for_position_close,
            cancel_if_open=cancel_if_open,
            settle_grace_sec=settle_grace_sec,
        )
        payload["fallback_filled_qty"] = hedge_execution.filled_qty
        payload["fallback_remaining_qty"] = hedge_execution.remaining_qty
        payload["fallback_result"] = "filled" if hedge_execution.remaining_qty <= EPS else "partial"
    except Exception as exc:
        payload["fallback_result"] = "error"
        payload["error_message"] = str(exc)
        return payload

    await refresh_symbol_state(exchanges[pair_like.long_exchange], symbol, "strategy_close_restore_long_post")
    await refresh_symbol_state(exchanges[pair_like.short_exchange], symbol, "strategy_close_restore_short_post")
    long_qty_after = effective_position_qty(
        exchanges[pair_like.long_exchange].get_symbol_snapshot(symbol),
        exchanges[pair_like.long_exchange],
        symbol,
        config,
    )
    short_qty_after = effective_position_qty(
        exchanges[pair_like.short_exchange].get_symbol_snapshot(symbol),
        exchanges[pair_like.short_exchange],
        symbol,
        config,
    )
    state, balanced_qty, gap = classify_pair_position_state(
        long_qty_after,
        short_qty_after,
        reference_qty=max(getattr(pair_like, "qty", 0.0), residual_qty),
    )
    payload.update(
        {
            "long_qty_after": long_qty_after,
            "short_qty_after": short_qty_after,
            "remaining_net_qty": long_qty_after + short_qty_after,
            "close_state_after_restore": state,
            "balanced_qty_after_restore": balanced_qty,
            "residual_gap_after_restore": gap,
        }
    )
    payload["neutralized"] = state in {"balanced_open", "closed"} or abs(long_qty_after + short_qty_after) <= qty_match_tolerance(residual_qty)
    if payload["neutralized"]:
        record_live_event(config, "pair_neutralized_after_close_failure", **payload)
        print(
            f"[복구][중립화완료] pair_id={getattr(pair_like, 'pair_id', '')} 심볼={symbol} "
            f"잔여={residual_exchange}:{residual_qty:.6f} "
            f"대체헷지={fallback_exchange_name}:{fallback_side}:{fallback_qty:.6f} "
            f"남은순수량={payload.get('remaining_net_qty', 0.0):.6f}"
        )
    return payload


async def emergency_flatten_pair_state(
    pair_like: Any,
    exchanges: dict[str, Any],
    aggressive_bps: float = 20.0,
    *,
    config: StrategyConfig | None = None,
    close_first_leg_role: str | None = None,
    restore_neutrality: bool = True,
) -> PairFlattenResult:
    symbol = pair_like.symbol
    leg_results: list[FlattenLegResult] = []
    forced_flat_exchanges: set[str] = set()

    for spec in pair_close_leg_specs(pair_like, close_first_leg_role):
        exchange_name = spec["exchange_name"]
        leg_result = await emergency_flatten_symbol_state_detailed(
            exchanges[exchange_name],
            symbol,
            aggressive_bps=aggressive_bps,
            role=spec["role"],
        )
        leg_results.append(leg_result)
        if leg_result.assumed_flat:
            forced_flat_exchanges.add(exchange_name)
        if not leg_result.ok:
            other_leg_action = "skipped" if len(leg_results) == 1 else "closed"
            pair_result = PairFlattenResult(
                ok=False,
                forced_flat_exchanges=forced_flat_exchanges,
                leg_results=leg_results,
                failed_leg=leg_result,
                other_leg_action=other_leg_action,
            )
            if (
                restore_neutrality
                and config is not None
                and len(leg_results) >= 2
                and leg_results[0].ok
            ):
                neutralize_payload = await restore_pair_neutrality_after_close_failure(
                    pair_like,
                    exchanges,
                    config,
                    failed_leg=leg_result,
                    closed_leg=leg_results[0],
                )
                pair_result.neutralize_payload = neutralize_payload
                pair_result.neutralized = bool(neutralize_payload.get("neutralized"))
                pair_result.other_leg_action = "rehedged" if pair_result.neutralized else other_leg_action
            return pair_result

    return PairFlattenResult(
        ok=True,
        forced_flat_exchanges=forced_flat_exchanges,
        leg_results=leg_results,
        other_leg_action="closed",
    )


async def emergency_flatten_pair(pair_like: Any, exchanges: dict[str, Any], aggressive_bps: float = 20.0) -> bool:
    result = await emergency_flatten_pair_state(pair_like, exchanges, aggressive_bps=aggressive_bps)
    return result.ok


def install_shutdown_signal_handlers(
    shutdown_requested: asyncio.Event,
    shutdown_reason: dict[str, str],
    wake_queue: asyncio.Queue[tuple[str, str, int]],
) -> list[signal.Signals]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request_shutdown(signame: str) -> None:
        if not shutdown_requested.is_set():
            print(f"[종료][요청] stop 신호 수신 signal={signame}")
            record_reason = signame.lower()
            shutdown_reason["reason"] = record_reason
            shutdown_requested.set()
            try:
                wake_queue.put_nowait(("shutdown", record_reason, now_ms()))
            except Exception:
                pass

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_shutdown, sig.name)
            installed.append(sig)
        except (NotImplementedError, RuntimeError):
            def _handler(signum: int, _frame: Any, *, fallback_sig: signal.Signals = sig) -> None:
                loop.call_soon_threadsafe(request_shutdown, fallback_sig.name)

            signal.signal(sig, _handler)

    return installed


def remove_shutdown_signal_handlers(installed: list[signal.Signals]) -> None:
    if not installed:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    for sig in installed:
        try:
            loop.remove_signal_handler(sig)
        except Exception:
            pass


async def cancel_shutdown_open_orders(
    *,
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    skip_symbols: set[str] | None = None,
) -> dict[str, Any]:
    skip_symbols = {symbol.upper() for symbol in (skip_symbols or set())}
    summary: dict[str, Any] = {"attempted": 0, "failed": [], "skipped_manual_locked": []}
    if not config.enable_live or not config.graceful_shutdown_cancel_orders:
        return summary

    for exchange_name, exchange in exchanges.items():
        cancel_all = getattr(exchange, "cancel_all_orders_for_symbol", None)
        if not callable(cancel_all):
            continue
        for symbol in symbols:
            if symbol.upper() in skip_symbols:
                summary["skipped_manual_locked"].append({"exchange": exchange_name, "symbol": symbol})
                continue
            try:
                summary["attempted"] += 1
                result = await cancel_all(symbol)
                if result and result.get("error"):
                    summary["failed"].append(
                        {"exchange": exchange_name, "symbol": symbol, "result": str(result)[:300]}
                    )
                else:
                    await refresh_symbol_state(exchange, symbol, "strategy_shutdown_cancel_reconcile")
            except Exception as exc:
                summary["failed"].append(
                    {"exchange": exchange_name, "symbol": symbol, "error": str(exc)[:300]}
                )
                print(
                    f"[종료][주문취소][경고] 거래소={exchange_name} "
                    f"심볼={symbol} 오류={exc}"
                )

    return summary


async def flatten_shutdown_symbols(
    *,
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    skip_symbols: set[str] | None = None,
) -> dict[str, Any]:
    skip_symbols = {symbol.upper() for symbol in (skip_symbols or set())}
    summary: dict[str, Any] = {"checked": 0, "failed": [], "assumed_flat": [], "skipped_manual_locked": []}
    if not config.enable_live or not config.graceful_shutdown_flatten_positions:
        return summary

    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            if symbol.upper() in skip_symbols:
                summary["skipped_manual_locked"].append({"exchange": exchange_name, "symbol": symbol})
                continue
            summary["checked"] += 1
            try:
                ok, assumed_flat = await asyncio.wait_for(
                    emergency_flatten_symbol_state(
                        exchange,
                        symbol,
                        aggressive_bps=config.emergency_flatten_bps,
                    ),
                    timeout=config.shutdown_per_symbol_timeout_sec,
                )
                if assumed_flat:
                    summary["assumed_flat"].append({"exchange": exchange_name, "symbol": symbol})
                if not ok:
                    summary["failed"].append({"exchange": exchange_name, "symbol": symbol})
            except asyncio.TimeoutError:
                # A hung exchange must not block the whole shutdown. Record the
                # position we could not close as an orphan and move on.
                summary["failed"].append(
                    {"exchange": exchange_name, "symbol": symbol, "error": "shutdown_flatten_timeout"}
                )
                with contextlib.suppress(Exception):
                    qty = effective_position_qty(exchange.get_symbol_snapshot(symbol), exchange, symbol, config)
                    if abs(qty) > EPS:
                        mark_orphan(
                            symbol=symbol,
                            exchange=exchange_name,
                            qty=qty,
                            reason="shutdown_flatten_timeout",
                        )
                print(
                    f"[종료][포지션정리][타임아웃] 거래소={exchange_name} 심볼={symbol} "
                    f"timeout={config.shutdown_per_symbol_timeout_sec}s → orphan 기록 후 다음 진행"
                )
            except Exception as exc:
                summary["failed"].append(
                    {"exchange": exchange_name, "symbol": symbol, "error": str(exc)[:300]}
                )
                print(
                    f"[종료][포지션정리][경고] 거래소={exchange_name} "
                    f"심볼={symbol} 오류={exc}"
                )
    return summary


async def collect_remaining_positions(
    *,
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    skip_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    skip_symbols = {symbol.upper() for symbol in (skip_symbols or set())}
    remaining: list[dict[str, Any]] = []
    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            if symbol.upper() in skip_symbols:
                continue
            try:
                await refresh_symbol_state(
                    exchange,
                    symbol,
                    "strategy_shutdown_final_check",
                    include_account=True,
                )
                qty = effective_position_qty(
                    exchange.get_symbol_snapshot(symbol),
                    exchange,
                    symbol,
                    config,
                )
                if abs(qty) > EPS:
                    remaining.append({"exchange": exchange_name, "symbol": symbol, "qty": qty})
            except Exception as exc:
                remaining.append({"exchange": exchange_name, "symbol": symbol, "error": str(exc)[:300]})
    return remaining


async def collect_remaining_open_orders(
    *,
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    skip_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    skip_symbols = {symbol.upper() for symbol in (skip_symbols or set())}
    remaining: list[dict[str, Any]] = []
    if not config.enable_live:
        return remaining

    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            if symbol.upper() in skip_symbols:
                continue
            try:
                await refresh_symbol_state(
                    exchange,
                    symbol,
                    "strategy_shutdown_final_open_orders_check",
                    include_account=False,
                    include_open_orders=True,
                )
                snapshot = exchange.get_symbol_snapshot(symbol) or {}
                open_orders = snapshot.get("open_orders") or []
                if open_orders:
                    remaining.append(
                        {
                            "exchange": exchange_name,
                            "symbol": symbol,
                            "order_count": len(open_orders),
                            "order_ids": [extract_order_uuid(row) for row in open_orders[:10]],
                        }
                    )
            except Exception as exc:
                remaining.append({"exchange": exchange_name, "symbol": symbol, "error": str(exc)[:300]})
    return remaining


async def graceful_shutdown_flatten(
    *,
    reason: str,
    symbols: list[str],
    exchanges: dict[str, Any],
    open_pairs: dict[str, OpenPairState],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int],
    manual_locked_symbols: set[str] | None = None,
) -> None:
    if not config.enable_live or not config.graceful_shutdown_enabled:
        return

    manual_locked_symbols = {
        symbol.upper()
        for symbol in (
            manual_locked_symbols
            if manual_locked_symbols is not None
            else active_manual_symbols(config.position_ownership_file)
        )
    }
    pair_ids = sorted(open_pairs.keys())
    print(
        f"[종료][정리시작] 사유={reason} "
        f"오픈페어={len(pair_ids)} 심볼수={len(symbols)} 수동관리={sorted(manual_locked_symbols)}"
    )
    record_live_event(
        config,
        "shutdown_cleanup_started",
        reason=reason,
        open_pair_ids=pair_ids,
        manual_locked_symbols=sorted(manual_locked_symbols),
    )
    for exchange in exchanges.values():
        request_account_refresh(exchange, "shutdown_cleanup_before", priority="urgent")

    cancel_before = await cancel_shutdown_open_orders(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )

    closed_pairs: list[str] = []
    pending_pairs: list[str] = []
    for pair_id, pair in list(open_pairs.items()):
        try:
            ok = await close_pair_limit_then_hedge(pair, exchanges, config)
            if ok and pair.status == "closed":
                register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
                open_pairs.pop(pair_id, None)
                closed_pairs.append(pair_id)
            else:
                pending_pairs.append(pair_id)
        except Exception as exc:
            pending_pairs.append(pair_id)
            print(f"[종료][페어종료][경고] pair_id={pair_id} 오류={exc}")

    cancel_after = await cancel_shutdown_open_orders(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )
    flatten_summary = await flatten_shutdown_symbols(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )
    cancel_final = await cancel_shutdown_open_orders(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )
    for exchange in exchanges.values():
        request_account_refresh(exchange, "shutdown_cleanup_after", priority="urgent")
    remaining_open_orders = await collect_remaining_open_orders(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )
    remaining_positions = await collect_remaining_positions(
        symbols=symbols,
        exchanges=exchanges,
        config=config,
        skip_symbols=manual_locked_symbols,
    )

    if remaining_open_orders:
        print(f"[종료][정리경고] 잔여주문={remaining_open_orders}")
    if remaining_positions:
        print(f"[종료][정리경고] 잔여포지션={remaining_positions}")
    elif not remaining_open_orders:
        print("[종료][정리완료] 잔여포지션=없음")

    record_live_event(
        config,
        "shutdown_cleanup_done",
        reason=reason,
        closed_pair_ids=closed_pairs,
        pending_pair_ids=pending_pairs,
        cancel_before=cancel_before,
        cancel_after=cancel_after,
        cancel_final=cancel_final,
        flatten=flatten_summary,
        remaining_open_orders=remaining_open_orders,
        remaining_positions=remaining_positions,
        manual_locked_symbols=sorted(manual_locked_symbols),
    )


def active_open_pair_symbols(open_pairs: dict[str, OpenPairState]) -> set[str]:
    return {
        pair.symbol
        for pair in open_pairs.values()
        if pair.status not in {"closed", "failed"}
    }


def account_imbalance_hedge_leg_candidates(
    left_exchange: str,
    right_exchange: str,
    left_qty: float,
    right_qty: float,
    *,
    symbol: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> list[tuple[str, str, float]]:
    net_qty = left_qty + right_qty
    if abs(net_qty) <= EPS:
        return []

    side = "sell" if net_qty > 0 else "buy"
    target_qty = abs(net_qty)
    candidates: list[tuple[tuple[int, float, float, float], tuple[str, str, float]]] = []

    for exchange_name, current_qty, other_qty in (
        (left_exchange, left_qty, right_qty),
        (right_exchange, right_qty, left_qty),
    ):
        reduction_capacity = 0.0
        if side == "sell" and current_qty > EPS:
            reduction_capacity = current_qty
        elif side == "buy" and current_qty < -EPS:
            reduction_capacity = abs(current_qty)
        requested_qty = min(target_qty, reduction_capacity) if reduction_capacity > EPS else target_qty
        preview = exchange_order_execution_preview(
            exchanges[exchange_name],
            symbol,
            side,
            requested_qty,
            config.close_aggressive_bps,
        )
        executable_qty = _safe_float(preview.get("executable_qty"), 0.0)
        if executable_qty <= EPS:
            continue

        post_qty = current_qty - executable_qty if side == "sell" else current_qty + executable_qty
        net_after = post_qty + other_qty
        position_reducing = reduction_capacity > EPS
        score = (
            0 if position_reducing else 1,
            abs(net_after),
            abs(post_qty),
            -executable_qty,
        )
        candidates.append((score, (exchange_name, side, requested_qty)))

    if not candidates:
        return []

    candidates.sort(key=lambda item: item[0])
    return [candidate for _, candidate in candidates]


def select_account_imbalance_hedge_leg(
    left_exchange: str,
    right_exchange: str,
    left_qty: float,
    right_qty: float,
    *,
    symbol: str,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> tuple[str, str, float] | None:
    candidates = account_imbalance_hedge_leg_candidates(
        left_exchange,
        right_exchange,
        left_qty,
        right_qty,
        symbol=symbol,
        exchanges=exchanges,
        config=config,
    )
    return candidates[0] if candidates else None


def build_recovered_pair_from_positions(
    *,
    symbol: str,
    left_exchange: str,
    right_exchange: str,
    left_qty: float,
    right_qty: float,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> OpenPairState | None:
    if left_qty > EPS and right_qty < -EPS:
        long_exchange = left_exchange
        short_exchange = right_exchange
        long_qty = abs(left_qty)
        short_qty = abs(right_qty)
    elif right_qty > EPS and left_qty < -EPS:
        long_exchange = right_exchange
        short_exchange = left_exchange
        long_qty = abs(right_qty)
        short_qty = abs(left_qty)
    else:
        return None

    long_snapshot = exchanges[long_exchange].get_symbol_snapshot(symbol)
    short_snapshot = exchanges[short_exchange].get_symbol_snapshot(symbol)
    pair_qty = min(long_qty, short_qty)
    if pair_qty <= EPS:
        return None

    long_entry_price = extract_position_entry_price(long_snapshot) or extract_mid_price(long_snapshot)
    short_entry_price = extract_position_entry_price(short_snapshot) or extract_mid_price(short_snapshot)
    if long_entry_price <= 0 or short_entry_price <= 0:
        return None

    timestamp_ms = now_ms()
    return OpenPairState(
        pair_id=f"{symbol}:{long_exchange}:{short_exchange}:recovered:{timestamp_ms}",
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        qty=pair_qty,
        requested_qty=pair_qty,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        entry_time_ms=timestamp_ms,
        entry_net_edge_usd=0.0,
        funding_long_rate=extract_funding_rate(long_snapshot),
        funding_short_rate=extract_funding_rate(short_snapshot),
        status="open",
    )


async def recover_account_imbalances(
    *,
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    open_pairs: dict[str, OpenPairState],
    assumed_flat_until_ms: dict[tuple[str, str], int],
    next_retry_ms_by_key: dict[tuple[str, str, str, float], int],
    skip_symbols: set[str] | None = None,
) -> None:
    if not config.enable_live or not config.account_imbalance_hedge_enabled:
        return

    active_symbols = active_open_pair_symbols(open_pairs)
    skip_symbols = {symbol.upper() for symbol in (skip_symbols or set())}
    current_ms = now_ms()

    for symbol in symbols:
        if symbol.upper() in skip_symbols:
            continue
        symbol_has_open_pair = symbol in active_symbols

        for left_exchange, right_exchange in config.allowed_pairs:
            if left_exchange not in exchanges or right_exchange not in exchanges:
                continue

            left = exchanges[left_exchange]
            right = exchanges[right_exchange]
            left_snapshot = left.get_symbol_snapshot(symbol)
            right_snapshot = right.get_symbol_snapshot(symbol)

            if not pair_action_inputs_are_fresh(left_snapshot, right_snapshot, config):
                continue

            left_qty = effective_position_qty(
                left_snapshot,
                left,
                symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            right_qty = effective_position_qty(
                right_snapshot,
                right,
                symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            if abs(left_qty) <= EPS and abs(right_qty) <= EPS:
                continue

            qty_fingerprint = round(abs(left_qty) + abs(right_qty), 8)
            retry_key = (symbol, left_exchange, right_exchange, qty_fingerprint)
            if current_ms < next_retry_ms_by_key.get(retry_key, 0):
                continue

            candidate_legs = account_imbalance_hedge_leg_candidates(
                left_exchange,
                right_exchange,
                left_qty,
                right_qty,
                symbol=symbol,
                exchanges=exchanges,
                config=config,
            )
            if not candidate_legs:
                if symbol_has_open_pair:
                    continue
                recovered_pair = build_recovered_pair_from_positions(
                    symbol=symbol,
                    left_exchange=left_exchange,
                    right_exchange=right_exchange,
                    left_qty=left_qty,
                    right_qty=right_qty,
                    exchanges=exchanges,
                    config=config,
                )
                if recovered_pair is not None:
                    open_pairs[recovered_pair.pair_id] = recovered_pair
                    active_symbols.add(symbol)
                    record_live_event(
                        config,
                        "pair_adopted_from_account_balance",
                        pair=pair_event_payload(recovered_pair),
                        left_qty=left_qty,
                        right_qty=right_qty,
                    )
                    print(
                        f"[복구][페어편입] pair_id={recovered_pair.pair_id} "
                        f"심볼={symbol} 수량={recovered_pair.qty:.6f}"
                    )
                continue

            hedge_completed = False
            for hedge_exchange_name, hedge_side, raw_target_qty in candidate_legs:
                hedge_exchange = exchanges[hedge_exchange_name]
                if exchange_has_symbol_waiting_order(hedge_exchange, symbol):
                    print(
                        f"[복구][대기] 심볼={symbol} 사유=대기주문존재 "
                        f"헷지={hedge_exchange_name}:{hedge_side}"
                    )
                    continue

                hedge_price = marketable_limit_price_with_bps(
                    hedge_exchange,
                    symbol,
                    hedge_side,
                    config.close_aggressive_bps,
                )
                hedge_qty = normalize_exchange_qty_for_order(hedge_exchange, symbol, raw_target_qty, hedge_price)
                if hedge_qty <= EPS:
                    print(
                        f"[복구][대기] 심볼={symbol} 사유=최소주문수량미만 "
                        f"좌측={left_exchange}:{left_qty:.12f} 우측={right_exchange}:{right_qty:.12f} "
                        f"헷지={hedge_exchange_name}:{hedge_side} 원수량={raw_target_qty:.12f}"
                    )
                    continue

                print(
                    f"[복구][불균형감지] 심볼={symbol} "
                    f"좌측={left_exchange}:{left_qty:.6f} 우측={right_exchange}:{right_qty:.6f} "
                    f"헷지={hedge_exchange_name}:{hedge_side} 수량={hedge_qty:.6f}"
                )
                pre_qty = extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol))
                try:
                    hedge_execution = await execute_residual_hedge(
                        stage_label="account_imbalance",
                        exchange_name=hedge_exchange_name,
                        exchange=hedge_exchange,
                        symbol=symbol,
                        side=hedge_side,
                        pre_qty=pre_qty,
                        target_qty=hedge_qty,
                        config=config,
                        is_close=False,
                        eps=EPS,
                        extract_position_qty=extract_position_qty,
                        marketable_limit_price_with_bps=marketable_limit_price_with_bps,
                        print_verbose=print_verbose,
                        use_reduce_only_market_close=use_reduce_only_market_close,
                        resolve_effective_order_qty=resolve_effective_order_qty,
                        extract_order_avg_price=extract_order_avg_price,
                        wait_for_position_fill=wait_for_position_fill,
                        wait_for_position_close=wait_for_position_close,
                        cancel_if_open=cancel_if_open,
                        settle_grace_sec=settle_grace_sec,
                    )
                except Exception as exc:
                    print(
                        f"[복구][경고] 심볼={symbol} 사유=헷지주문오류 "
                        f"헷지={hedge_exchange_name}:{hedge_side} 수량={hedge_qty:.6f} 오류={exc}"
                    )
                    continue

                if hedge_execution.remaining_qty > EPS:
                    print(
                        f"[복구][경고] 심볼={symbol} 사유=헷지미완료 "
                        f"헷지={hedge_exchange_name}:{hedge_side} 요청={hedge_qty:.6f} "
                        f"체결={hedge_execution.filled_qty:.6f} 잔량={hedge_execution.remaining_qty:.6f}"
                    )
                    if hedge_execution.filled_qty > EPS:
                        hedge_completed = True
                        next_retry_ms_by_key[retry_key] = current_ms + int(config.account_imbalance_cooldown_sec * 1000)
                        break
                    continue

                await refresh_symbol_state(left, symbol, "strategy_account_hedge_post_left")
                await refresh_symbol_state(right, symbol, "strategy_account_hedge_post_right")
                left_qty_after = effective_position_qty(
                    left.get_symbol_snapshot(symbol),
                    left,
                    symbol,
                    config,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                )
                right_qty_after = effective_position_qty(
                    right.get_symbol_snapshot(symbol),
                    right,
                    symbol,
                    config,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                )
                hedge_completed = True
                next_retry_ms_by_key[retry_key] = current_ms + int(config.account_imbalance_cooldown_sec * 1000)
                if symbol_has_open_pair:
                    remaining_net_qty = left_qty_after + right_qty_after
                    if abs(remaining_net_qty) <= EPS:
                        print(
                            f"[복구][잔량정리완료] 심볼={symbol} "
                            f"좌측수량={left_qty_after:.6f} 우측수량={right_qty_after:.6f}"
                        )
                    else:
                        print(
                            f"[복구][잔량유지] 심볼={symbol} "
                            f"좌측수량={left_qty_after:.6f} 우측수량={right_qty_after:.6f}"
                        )
                    break
                recovered_pair = build_recovered_pair_from_positions(
                    symbol=symbol,
                    left_exchange=left_exchange,
                    right_exchange=right_exchange,
                    left_qty=left_qty_after,
                    right_qty=right_qty_after,
                    exchanges=exchanges,
                    config=config,
                )
                if recovered_pair is None:
                    print(
                        f"[복구][경고] 심볼={symbol} 사유=페어편입불가 "
                        f"좌측수량={left_qty_after:.6f} 우측수량={right_qty_after:.6f}"
                    )
                    break

                open_pairs[recovered_pair.pair_id] = recovered_pair
                active_symbols.add(symbol)
                record_live_event(
                    config,
                    "pair_recovered_from_account_imbalance",
                    pair=pair_event_payload(recovered_pair),
                    left_qty_before=left_qty,
                    right_qty_before=right_qty,
                    left_qty_after=left_qty_after,
                    right_qty_after=right_qty_after,
                    hedge_exchange=hedge_exchange_name,
                    hedge_side=hedge_side,
                    hedge_qty=hedge_qty,
                )
                print(
                    f"[복구][완료] pair_id={recovered_pair.pair_id} "
                    f"심볼={symbol} 수량={recovered_pair.qty:.6f}"
                )
                break

            if hedge_completed:
                break
            next_retry_ms_by_key[retry_key] = current_ms + int(config.account_imbalance_cooldown_sec * 1000)


async def enter_pair_limit_then_hedge(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
    last_entry_order_submit_ms_by_key: dict[tuple[str, str, str], int] | None = None,
) -> OpenPairState | None:
    entry_started_ms = now_ms()
    entry_telemetry: dict[str, Any] = {"entry_started_ms": entry_started_ms}
    symbol = candidate.symbol
    first_exchange_name, first_side = first_leg_plan(candidate, config)
    hedge_exchange_name, hedge_side = hedge_leg_plan(candidate, config)
    first_exchange = exchanges[first_exchange_name]
    hedge_exchange = exchanges[hedge_exchange_name]

    bootstrap_failures = await bootstrap_candidate_exchanges(candidate, exchanges, config)
    if bootstrap_failures:
        record_live_event(
            config,
            "entry_bootstrap_failed",
            candidate_id=candidate.id,
            symbol=candidate.symbol,
            long_exchange=candidate.long_exchange,
            short_exchange=candidate.short_exchange,
            failures=bootstrap_failures,
        )
        print(
            f"[진입][중단] 심볼={symbol} 사유=거래소준비실패 "
            f"상세={bootstrap_failures}"
        )
        return None

    first_price = passive_entry_target_price(
        candidate,
        first_exchange_name,
        first_side,
        exchanges,
        config,
    )
    hedge_price_preview = marketable_limit_price_with_bps(hedge_exchange, symbol, hedge_side, config.close_aggressive_bps)
    executable_qty = compute_common_executable_qty(
        first_exchange,
        hedge_exchange,
        symbol,
        candidate.executable_qty if candidate.executable_qty > EPS else candidate.target_qty,
        left_price=first_price,
        right_price=hedge_price_preview,
    )
    if executable_qty <= EPS:
        print(
            f"[진입][중단] 심볼={symbol} 사유=공통주문수량없음 "
            f"요청={candidate.target_qty:.6f} 후보수량={candidate.executable_qty:.6f}"
        )
        return None

    long_leg_price = first_price if first_exchange_name == candidate.long_exchange else hedge_price_preview
    short_leg_price = first_price if first_exchange_name == candidate.short_exchange else hedge_price_preview
    margin_guard_reasons = pair_entry_margin_guard_reasons(
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        long_exchange_obj=exchanges[candidate.long_exchange],
        short_exchange_obj=exchanges[candidate.short_exchange],
        long_notional_usd=executable_qty * long_leg_price,
        short_notional_usd=executable_qty * short_leg_price,
        config=config,
    )
    if margin_guard_reasons:
        print(
            f"[진입][차단] 심볼={symbol} 사유=증거금부족 "
            f"상세={margin_guard_reasons}"
        )
        return None

    pre_qty_first = extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
    pre_qty_hedge = extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol))

    if first_price <= 0:
        print(f"[진입][중단] 심볼={symbol} 거래소={first_exchange_name} 사유=첫주문가격없음")
        return None

    print_verbose(
        config,
        f"[strategy][enter] first_leg exchange={first_exchange_name} side={first_side} "
        f"qty={executable_qty:.6f} requested={candidate.target_qty:.6f} price={first_price}"
    )
    first_filled_qty, first_order_result, first_price_used = await execute_requoting_limit_leg(
        exchange=first_exchange,
        symbol=symbol,
        side=first_side,
        target_qty=executable_qty,
        pre_qty=pre_qty_first,
        timeout_sec=config.first_leg_timeout_sec,
        config=config,
        is_close=False,
        price_resolver=lambda: passive_entry_target_price(
            candidate,
            first_exchange_name,
            first_side,
            exchanges,
            config,
        ),
        log_prefix="enter",
        probe_reason_prefix="strategy_entry_first",
        last_order_submit_ms_by_key=last_entry_order_submit_ms_by_key,
        telemetry=entry_telemetry,
    )
    first_leg_done_ms = now_ms()

    if first_filled_qty <= EPS:
        return None

    hedge_preview = exchange_order_execution_preview(
        hedge_exchange,
        symbol,
        hedge_side,
        first_filled_qty,
        config.close_aggressive_bps,
    )
    hedge_min_qty = _safe_float(hedge_preview.get("min_executable_qty"), 0.0)
    hedgeable_first_fill_qty = _safe_float(hedge_preview.get("executable_qty"), 0.0)
    first_fill_residual_qty = max(first_filled_qty - hedgeable_first_fill_qty, 0.0)
    if hedgeable_first_fill_qty <= EPS:
        first_flatten_preview = exchange_order_execution_preview(
            first_exchange,
            symbol,
            "sell" if first_side.lower() == "buy" else "buy",
            first_filled_qty,
            config.emergency_flatten_bps,
        )
        first_flatten_min_qty = _safe_float(first_flatten_preview.get("min_executable_qty"), 0.0)
        first_flattenable_qty = _safe_float(first_flatten_preview.get("executable_qty"), 0.0)
        if first_flattenable_qty <= EPS:
            exchange_name = getattr(first_exchange, "persist_name", None) or getattr(first_exchange, "name", "") or ""
            _log_throttled_dust_ignored(
                exchange_name,
                symbol,
                first_filled_qty if first_side.lower() == "buy" else -first_filled_qty,
                first_flatten_min_qty,
                reason="late_fill_below_min_executable_qty",
                price=_safe_float(first_flatten_preview.get("price"), 0.0),
            )
            print(
                f"[진입][소량체결무시] 심볼={symbol} "
                f"첫체결={first_filled_qty:.6f} 헷지가능={hedgeable_first_fill_qty:.6f} "
                f"정리가능={first_flattenable_qty:.6f} 정리최소={first_flatten_min_qty:.6f} "
                f"헷지최소={hedge_min_qty:.6f}"
            )
            return None
        print(
            f"[진입][중단] 심볼={symbol} 사유=첫체결헷지불가 "
            f"첫체결={first_filled_qty:.6f} 헷지가능={hedgeable_first_fill_qty:.6f} "
            f"헷지최소={hedge_min_qty:.6f}"
        )
        flattened = await emergency_flatten_symbol(
            first_exchange,
            symbol,
            aggressive_bps=config.emergency_flatten_bps,
        )
        if not flattened:
            log_danger(title="헤지불가첫체결", symbol=symbol, result="페어종료실패")
        return None
    if first_fill_residual_qty > EPS:
        first_residual_preview = exchange_order_execution_preview(
            first_exchange,
            symbol,
            "sell" if first_side.lower() == "buy" else "buy",
            first_fill_residual_qty,
            config.emergency_flatten_bps,
        )
        if _safe_float(first_residual_preview.get("executable_qty"), 0.0) > EPS:
            print(
                f"[진입][잔량정리대상] 심볼={symbol} "
                f"첫체결={first_filled_qty:.6f} 헷지가능={hedgeable_first_fill_qty:.6f} "
                f"잔량={first_fill_residual_qty:.6f}"
            )
        else:
            print(
                f"[진입][잔량무시] 심볼={symbol} "
                f"첫체결={first_filled_qty:.6f} 헷지가능={hedgeable_first_fill_qty:.6f} "
                f"잔량={first_fill_residual_qty:.6f} 사유=최소주문수량미만"
            )

    post_fill_edge_decision = evaluate_post_first_fill_edge(
        candidate=candidate,
        config=config,
        first_exchange_name=first_exchange_name,
        first_fill_price=first_price_used,
        hedge_price=_safe_float(hedge_preview.get("price"), 0.0),
        qty=hedgeable_first_fill_qty,
    )
    record_live_event(
        config,
        "entry_post_first_fill_edge_check",
        candidate_id=candidate.id,
        symbol=symbol,
        first_exchange=first_exchange_name,
        first_side=first_side,
        hedge_exchange=hedge_exchange_name,
        hedge_side=hedge_side,
        first_filled_qty=first_filled_qty,
        hedgeable_qty=hedgeable_first_fill_qty,
        first_fill_price=first_price_used,
        hedge_price=post_fill_edge_decision.hedge_price,
        long_price=post_fill_edge_decision.long_price,
        short_price=post_fill_edge_decision.short_price,
        actual_open_edge_usd=post_fill_edge_decision.actual_open_edge_usd,
        actual_open_edge_bps=post_fill_edge_decision.actual_open_edge_bps,
        min_open_edge_bps=post_fill_edge_decision.min_open_edge_bps,
        actual_estimated_net_edge_usd=post_fill_edge_decision.actual_estimated_net_edge_usd,
        actual_estimated_net_edge_bps=post_fill_edge_decision.actual_estimated_net_edge_bps,
        min_net_edge_bps=post_fill_edge_decision.min_net_edge_bps,
        reference_notional_usd=post_fill_edge_decision.reference_notional_usd,
        can_hedge=post_fill_edge_decision.can_hedge,
        reason=post_fill_edge_decision.reason,
        expected_net_edge_bps=candidate.expected_net_edge_bps,
        expected_open_edge_bps=candidate.entry_open_edge_bps,
    )
    if not post_fill_edge_decision.can_hedge:
        print(
            f"[진입][중단] 심볼={symbol} 사유=체결후갭악화 "
            f"실제오픈bps={post_fill_edge_decision.actual_open_edge_bps:.4f} "
            f"오픈기준bps={post_fill_edge_decision.min_open_edge_bps:.4f} "
            f"실제예상순bps={post_fill_edge_decision.actual_estimated_net_edge_bps:.4f} "
            f"순기준bps={post_fill_edge_decision.min_net_edge_bps:.4f} "
            f"상세={post_fill_edge_decision.reason}"
        )
        flattened = await emergency_flatten_symbol(
            first_exchange,
            symbol,
            aggressive_bps=config.emergency_flatten_bps,
        )
        record_live_event(
            config,
            "entry_aborted_after_first_fill_bad_edge",
            candidate_id=candidate.id,
            symbol=symbol,
            first_exchange=first_exchange_name,
            first_side=first_side,
            first_filled_qty=first_filled_qty,
            hedgeable_qty=hedgeable_first_fill_qty,
            first_fill_price=first_price_used,
            hedge_price=post_fill_edge_decision.hedge_price,
            actual_open_edge_usd=post_fill_edge_decision.actual_open_edge_usd,
            actual_open_edge_bps=post_fill_edge_decision.actual_open_edge_bps,
            min_open_edge_bps=post_fill_edge_decision.min_open_edge_bps,
            actual_estimated_net_edge_usd=post_fill_edge_decision.actual_estimated_net_edge_usd,
            actual_estimated_net_edge_bps=post_fill_edge_decision.actual_estimated_net_edge_bps,
            min_net_edge_bps=post_fill_edge_decision.min_net_edge_bps,
            reason=post_fill_edge_decision.reason,
            rollback_ok=flattened,
        )
        if not flattened:
            record_live_event(
                config,
                "entry_bad_edge_rollback_failed",
                candidate_id=candidate.id,
                symbol=symbol,
                first_exchange=first_exchange_name,
                first_side=first_side,
                first_filled_qty=first_filled_qty,
                reason=post_fill_edge_decision.reason,
            )
            log_danger(
                title="진입후나쁜갭정리실패",
                symbol=symbol,
                first_exchange=first_exchange_name,
                first_qty=f"{first_filled_qty:.6f}",
                actual_open_edge_bps=f"{post_fill_edge_decision.actual_open_edge_bps:.4f}",
                actual_net_edge_bps=f"{post_fill_edge_decision.actual_estimated_net_edge_bps:.4f}",
            )
        return None

    hedge_start_ms = now_ms()
    hedge_execution = await execute_residual_hedge(
        stage_label="entry",
        exchange_name=hedge_exchange_name,
        exchange=hedge_exchange,
        symbol=symbol,
        side=hedge_side,
        pre_qty=pre_qty_hedge,
        target_qty=hedgeable_first_fill_qty,
        config=config,
        is_close=False,
        eps=EPS,
        extract_position_qty=extract_position_qty,
        marketable_limit_price_with_bps=marketable_limit_price_with_bps,
        print_verbose=print_verbose,
        use_reduce_only_market_close=use_reduce_only_market_close,
        resolve_effective_order_qty=resolve_effective_order_qty,
        extract_order_avg_price=extract_order_avg_price,
        wait_for_position_fill=wait_for_position_fill,
        wait_for_position_close=wait_for_position_close,
        cancel_if_open=cancel_if_open,
        settle_grace_sec=settle_grace_sec,
        record_order_event=lambda event_type, **payload: record_live_event(config, event_type, **payload),
    )
    hedge_done_ms = now_ms()
    hedged_qty = hedge_execution.filled_qty
    hedge_price_used = hedge_execution.avg_price
    remaining_qty = hedge_execution.remaining_qty

    if remaining_qty > EPS:
        hedge_remaining_preview = exchange_order_execution_preview(
            hedge_exchange,
            symbol,
            hedge_side,
            remaining_qty,
            config.close_aggressive_bps,
        )
        if _safe_float(hedge_remaining_preview.get("executable_qty"), 0.0) > EPS:
            is_micro_residual, micro_payload = classify_micro_residual(
                hedge_exchange,
                symbol,
                hedge_side,
                remaining_qty,
                executable_qty,
                config,
                aggressive_bps=config.close_aggressive_bps,
            )
            if is_micro_residual:
                print(
                    f"[헷지][소량잔량] 심볼={symbol} "
                    f"첫체결={first_filled_qty:.6f} 헷지완료={hedged_qty:.6f} "
                    f"잔량={remaining_qty:.6f} "
                    f"잔량명목USD={micro_payload['notional_usd']:.4f} "
                    f"잔량비율={micro_payload['ratio']:.6f} 사유=정보성"
                )
                record_live_event(
                    config,
                    "pair_entry_micro_residual_ignored",
                    symbol=symbol,
                    first_filled_qty=first_filled_qty,
                    hedged_qty=hedged_qty,
                    remaining_qty=remaining_qty,
                    hedge_exchange=hedge_exchange_name,
                    hedge_side=hedge_side,
                    **micro_payload,
                )
            else:
                log_hedge_incomplete(
                    symbol=symbol,
                    first_filled_qty=first_filled_qty,
                    hedged_qty=hedged_qty,
                    remaining_qty=remaining_qty,
                )
                rollback_ok = await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
                if not rollback_ok:
                    # Hedge incomplete AND rollback failed: persist the actual
                    # remaining account legs, not the original first-fill
                    # assumption, so operator cleanup sees the right venue/qty.
                    orphan_records = mark_actual_orphan_legs(
                        symbol=symbol,
                        exchanges=exchanges,
                        config=config,
                        reason="hedge_incomplete_rollback_failed",
                        fallback_exchange=first_exchange_name,
                        fallback_qty=remaining_qty,
                    )
                    print(
                        f"[orphan][marked] symbol={symbol} reason=hedge_incomplete_rollback_failed "
                        f"records={orphan_records}"
                    )
                    record_live_event(
                        config,
                        "pair_orphan_marked",
                        symbol=symbol,
                        exchange=first_exchange_name,
                        first_filled_qty=first_filled_qty,
                        hedged_qty=hedged_qty,
                        remaining_qty=remaining_qty,
                        reason="hedge_incomplete_rollback_failed",
                        orphan_records=orphan_records,
                    )
                return None
        else:
            log_residual_ignored(
                symbol=symbol,
                first_filled_qty=first_filled_qty,
                hedged_qty=hedged_qty,
                residual_qty=remaining_qty,
                reason="min_executable_qty",
            )

    await refresh_symbol_state(exchanges[candidate.long_exchange], symbol, "strategy_entry_post_open_long")
    await refresh_symbol_state(exchanges[candidate.short_exchange], symbol, "strategy_entry_post_open_short")
    long_snapshot_after = exchanges[candidate.long_exchange].get_symbol_snapshot(symbol)
    short_snapshot_after = exchanges[candidate.short_exchange].get_symbol_snapshot(symbol)
    long_qty_after = extract_position_qty(long_snapshot_after)
    short_qty_after = extract_position_qty(short_snapshot_after)
    pre_qty_by_exchange = {
        first_exchange_name: pre_qty_first,
        hedge_exchange_name: pre_qty_hedge,
    }
    pre_long_qty = pre_qty_by_exchange.get(candidate.long_exchange, 0.0)
    pre_short_qty = pre_qty_by_exchange.get(candidate.short_exchange, 0.0)
    long_entry_delta = entry_delta_qty(pre_long_qty, long_qty_after, "long")
    short_entry_delta = entry_delta_qty(pre_short_qty, short_qty_after, "short")
    pair_qty = min(long_entry_delta, short_entry_delta)
    if pair_qty <= EPS and min(first_filled_qty, hedged_qty) > EPS:
        await asyncio.sleep(config.action_settle_wait_sec)
        await refresh_symbol_state(exchanges[candidate.long_exchange], symbol, "strategy_entry_post_delta_retry_long")
        await refresh_symbol_state(exchanges[candidate.short_exchange], symbol, "strategy_entry_post_delta_retry_short")
        long_snapshot_after = exchanges[candidate.long_exchange].get_symbol_snapshot(symbol)
        short_snapshot_after = exchanges[candidate.short_exchange].get_symbol_snapshot(symbol)
        long_qty_after = extract_position_qty(long_snapshot_after)
        short_qty_after = extract_position_qty(short_snapshot_after)
        long_entry_delta = entry_delta_qty(pre_long_qty, long_qty_after, "long")
        short_entry_delta = entry_delta_qty(pre_short_qty, short_qty_after, "short")
        pair_qty = min(long_entry_delta, short_entry_delta)

    if long_qty_after <= EPS or short_qty_after >= -EPS:
        log_danger(
            title="진입후방향불일치",
            symbol=symbol,
            long_exchange=candidate.long_exchange,
            long_qty=f"{long_qty_after:.6f}",
            short_exchange=candidate.short_exchange,
            short_qty=f"{short_qty_after:.6f}",
        )
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        return None
    if pair_qty <= EPS:
        log_danger(
            title="진입후수량없음",
            symbol=symbol,
            long_before=f"{pre_long_qty:.6f}",
            long_after=f"{long_qty_after:.6f}",
            long_delta=f"{long_entry_delta:.6f}",
            short_before=f"{pre_short_qty:.6f}",
            short_after=f"{short_qty_after:.6f}",
            short_delta=f"{short_entry_delta:.6f}",
        )
        record_live_event(
            config,
            "pair_entry_delta_missing",
            symbol=symbol,
            requested_qty=executable_qty,
            first_filled_qty=first_filled_qty,
            hedged_qty=hedged_qty,
            long_exchange=candidate.long_exchange,
            short_exchange=candidate.short_exchange,
            long_pre_qty=pre_long_qty,
            long_qty_after=long_qty_after,
            long_entry_delta=long_entry_delta,
            short_pre_qty=pre_short_qty,
            short_qty_after=short_qty_after,
            short_entry_delta=short_entry_delta,
        )
        flatten_result = await emergency_flatten_pair_state(
            candidate,
            exchanges,
            aggressive_bps=config.emergency_flatten_bps,
            config=config,
            close_first_leg_role=config.close_first_leg_role,
            restore_neutrality=True,
        )
        record_live_event(
            config,
            "pair_entry_delta_missing_flattened" if flatten_result.ok else "pair_entry_delta_missing_flatten_failed",
            symbol=symbol,
            flatten_ok=flatten_result.ok,
            failed_leg=flatten_leg_result_payload(flatten_result.failed_leg),
            neutralized=flatten_result.neutralized,
        )
        return None

    long_order_price = first_price_used if first_exchange_name == candidate.long_exchange else hedge_price_used
    short_order_price = hedge_price_used if hedge_exchange_name == candidate.short_exchange else first_price_used
    long_entry_price = (
        extract_position_entry_price(long_snapshot_after)
        or long_order_price
        or extract_mid_price(long_snapshot_after)
    )
    short_entry_price = (
        extract_position_entry_price(short_snapshot_after)
        or short_order_price
        or extract_mid_price(short_snapshot_after)
    )
    if long_entry_price <= 0 or short_entry_price <= 0:
        log_danger(
            title="진입평단누락",
            symbol=symbol,
            long_exchange=candidate.long_exchange,
            long_entry=f"{long_entry_price:.8f}",
            short_exchange=candidate.short_exchange,
            short_entry=f"{short_entry_price:.8f}",
        )
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        return None

    account_pair_qty = min(abs(long_qty_after), abs(short_qty_after))
    overfill_qty = max(pair_qty - executable_qty, 0.0)
    if overfill_qty > qty_match_tolerance(executable_qty):
        long_overfill_preview = exchange_order_execution_preview(
            exchanges[candidate.long_exchange],
            symbol,
            "sell",
            overfill_qty,
            config.emergency_flatten_bps,
        )
        short_overfill_preview = exchange_order_execution_preview(
            exchanges[candidate.short_exchange],
            symbol,
            "buy",
            overfill_qty,
            config.emergency_flatten_bps,
        )
        long_overfill_executable = _safe_float(long_overfill_preview.get("executable_qty"), 0.0)
        short_overfill_executable = _safe_float(short_overfill_preview.get("executable_qty"), 0.0)
        overfill_actionable = long_overfill_executable > EPS and short_overfill_executable > EPS
        event_name = "pair_entry_overfill_detected" if overfill_actionable else "pair_entry_overfill_dust_ignored"
        log_overfill(
            symbol=symbol,
            requested_qty=executable_qty,
            pair_qty=pair_qty,
            overfill_qty=overfill_qty,
            long_executable_qty=long_overfill_executable,
            short_executable_qty=short_overfill_executable,
            actionable=overfill_actionable,
        )
        record_live_event(
            config,
            event_name,
            symbol=symbol,
            requested_qty=executable_qty,
            pair_qty=pair_qty,
            overfill_qty=overfill_qty,
            actionable=overfill_actionable,
            account_pair_qty=account_pair_qty,
            long_exchange=candidate.long_exchange,
            short_exchange=candidate.short_exchange,
            long_pre_qty=pre_long_qty,
            long_qty_after=long_qty_after,
            long_entry_delta=long_entry_delta,
            short_pre_qty=pre_short_qty,
            short_qty_after=short_qty_after,
            short_entry_delta=short_entry_delta,
            long_executable_qty=long_overfill_executable,
            short_executable_qty=short_overfill_executable,
        )
        if overfill_actionable:
            flatten_result = await emergency_flatten_pair_state(
                candidate,
                exchanges,
                aggressive_bps=config.emergency_flatten_bps,
                config=config,
                close_first_leg_role=config.close_first_leg_role,
                restore_neutrality=True,
            )
            print(
                f"[위험][초과체결정리] 심볼={symbol} "
                f"정리성공={flatten_result.ok} 중립복구={flatten_result.neutralized}"
            )
            record_live_event(
                config,
                "pair_entry_overfill_flattened" if flatten_result.ok else "pair_entry_overfill_flatten_failed",
                symbol=symbol,
                requested_qty=executable_qty,
                pair_qty=pair_qty,
                overfill_qty=overfill_qty,
                account_pair_qty=account_pair_qty,
                flatten_ok=flatten_result.ok,
                failed_leg=flatten_leg_result_payload(flatten_result.failed_leg),
                neutralized=flatten_result.neutralized,
                neutralize=flatten_result.neutralize_payload,
            )
            return None
        pair_qty = min(pair_qty, executable_qty)
    if executable_qty > EPS and pair_qty > EPS and pair_qty / executable_qty < 0.10:
        log_micro_partial(symbol=symbol, requested_qty=executable_qty, pair_qty=pair_qty, ratio=pair_qty / executable_qty)
        record_live_event(
            config,
            "pair_entry_micro_partial_detected",
            symbol=symbol,
            requested_qty=executable_qty,
            pair_qty=pair_qty,
            fill_ratio=pair_qty / executable_qty,
            account_pair_qty=account_pair_qty,
            long_exchange=candidate.long_exchange,
            short_exchange=candidate.short_exchange,
            long_pre_qty=pre_long_qty,
            long_qty_after=long_qty_after,
            long_entry_delta=long_entry_delta,
            short_pre_qty=pre_short_qty,
            short_qty_after=short_qty_after,
            short_entry_delta=short_entry_delta,
        )
    pair_id = f"{symbol}:{candidate.long_exchange}:{candidate.short_exchange}:{int(time.time() * 1000)}"

    actual_reference_notional_usd = max(pair_qty * ((long_entry_price + short_entry_price) / 2.0), 0.0)
    actual_entry_open_edge_usd = pair_qty * (short_entry_price - long_entry_price)
    actual_entry_open_edge_bps = (
        actual_entry_open_edge_usd / actual_reference_notional_usd * 10000.0
        if actual_reference_notional_usd > EPS
        else 0.0
    )
    expected_open_edge_usd_for_actual_qty = pair_qty * (candidate.short_bid - candidate.long_ask)
    entry_edge_slippage_usd = actual_entry_open_edge_usd - expected_open_edge_usd_for_actual_qty
    entry_edge_slippage_bps = actual_entry_open_edge_bps - candidate.entry_open_edge_bps
    qty_scale = pair_qty / candidate.executable_qty if candidate.executable_qty > EPS else 1.0
    actual_estimated_net_edge_usd = (
        actual_entry_open_edge_usd
        + candidate.effective_funding_pnl_usd * qty_scale
        - candidate.open_fee_cost_usd * qty_scale
        - candidate.expected_close_cost_usd * qty_scale
        - candidate.risk_buffer_usd * qty_scale
        - candidate.stale_data_penalty_usd * qty_scale
    )
    actual_estimated_net_edge_bps = (
        actual_estimated_net_edge_usd / actual_reference_notional_usd * 10000.0
        if actual_reference_notional_usd > EPS
        else 0.0
    )
    first_fill_detected_ms = int(entry_telemetry.get("first_fill_detected_ms") or first_leg_done_ms)
    record_live_event(
        config,
        "entry_quality_analysis",
        pair_id=pair_id,
        symbol=symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        qty=pair_qty,
        requested_qty=executable_qty,
        expected_open_edge_usd=candidate.entry_open_edge_usd,
        expected_open_edge_bps=candidate.entry_open_edge_bps,
        expected_open_edge_usd_for_actual_qty=expected_open_edge_usd_for_actual_qty,
        expected_net_edge_usd=candidate.expected_net_edge_usd,
        expected_net_edge_bps=candidate.expected_net_edge_bps,
        actual_open_edge_usd=actual_entry_open_edge_usd,
        actual_open_edge_bps=actual_entry_open_edge_bps,
        actual_estimated_net_edge_usd=actual_estimated_net_edge_usd,
        actual_estimated_net_edge_bps=actual_estimated_net_edge_bps,
        entry_edge_slippage_usd=entry_edge_slippage_usd,
        entry_edge_slippage_bps=entry_edge_slippage_bps,
        first_exchange=first_exchange_name,
        first_side=first_side,
        hedge_exchange=hedge_exchange_name,
        hedge_side=hedge_side,
        first_leg_fill_price=first_price_used,
        hedge_fill_price=hedge_price_used,
        first_order_id=extract_order_uuid(first_order_result),
        first_client_order_id=extract_order_uuid(first_order_result),
        hedge_order_id=hedge_execution.order_id,
        hedge_client_order_id=hedge_execution.client_order_id,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        candidate_long_ask=candidate.long_ask,
        candidate_short_bid=candidate.short_bid,
        candidate_market_age_ms=entry_telemetry.get("first_market_age_ms"),
        candidate_stale_age_ms=entry_telemetry.get("first_stale_age_ms"),
        first_submit_latency_ms=entry_telemetry.get("first_submit_latency_ms"),
        first_submit_start_ms=entry_telemetry.get("first_submit_start_ms"),
        first_submit_end_ms=entry_telemetry.get("first_submit_end_ms"),
        first_fill_detected_ms=first_fill_detected_ms,
        first_leg_done_ms=first_leg_done_ms,
        hedge_start_ms=hedge_start_ms,
        hedge_done_ms=hedge_done_ms,
        first_leg_elapsed_ms=max(first_leg_done_ms - entry_started_ms, 0),
        first_fill_to_hedge_fill_ms=max(hedge_done_ms - first_fill_detected_ms, 0),
        hedge_elapsed_ms=max(hedge_done_ms - hedge_start_ms, 0),
        total_entry_elapsed_ms=max(hedge_done_ms - entry_started_ms, 0),
        first_leg_submit_count=int(entry_telemetry.get("submit_count") or 0),
    )

    log_pair_opened(
        pair_id=pair_id,
        symbol=symbol,
        qty=pair_qty,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
    )
    pair = OpenPairState(
        pair_id=pair_id,
        symbol=symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        qty=pair_qty,
        requested_qty=executable_qty,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        entry_time_ms=int(time.time() * 1000),
        entry_net_edge_usd=candidate.expected_net_edge_usd,
        funding_long_rate=candidate.funding_long_rate,
        funding_short_rate=candidate.funding_short_rate,
        status="open",
    )
    clear_pair_assumed_flat(pair, assumed_flat_until_ms)
    record_live_event(
        config,
        "pair_opened",
        pair=pair_event_payload(pair),
        candidate={
            "id": candidate.id,
            "executable_qty": candidate.executable_qty,
            "notional_usd": candidate.notional_usd,
            "entry_open_edge_usd": candidate.entry_open_edge_usd,
            "entry_open_edge_bps": candidate.entry_open_edge_bps,
            "effective_funding_pnl_usd": candidate.effective_funding_pnl_usd,
            "expected_net_edge_usd": candidate.expected_net_edge_usd,
            "expected_net_edge_bps": candidate.expected_net_edge_bps,
            "actual_entry_open_edge_usd": actual_entry_open_edge_usd,
            "actual_entry_open_edge_bps": actual_entry_open_edge_bps,
            "actual_estimated_net_edge_usd": actual_estimated_net_edge_usd,
            "actual_estimated_net_edge_bps": actual_estimated_net_edge_bps,
            "entry_edge_slippage_usd": entry_edge_slippage_usd,
            "entry_edge_slippage_bps": entry_edge_slippage_bps,
        },
        entry_position_delta={
            "account_pair_qty": account_pair_qty,
            "long_pre_qty": pre_long_qty,
            "long_qty_after": long_qty_after,
            "long_entry_delta": long_entry_delta,
            "short_pre_qty": pre_short_qty,
            "short_qty_after": short_qty_after,
            "short_entry_delta": short_entry_delta,
            "first_exchange": first_exchange_name,
            "first_side": first_side,
            "first_filled_qty": first_filled_qty,
            "hedge_exchange": hedge_exchange_name,
            "hedge_side": hedge_side,
            "hedged_qty": hedged_qty,
        },
    )
    return pair


async def complete_entry_after_maker_first_fill(
    *,
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    first_exchange_name: str,
    first_side: str,
    hedge_exchange_name: str,
    hedge_side: str,
    pre_qty_first: float,
    pre_qty_hedge: float,
    first_filled_qty: float,
    first_price_used: float,
    executable_qty: float,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None,
    entry_started_ms: int,
    source: str = "maker_entry_engine",
    first_order_id: str | None = None,
    first_client_order_id: str | None = None,
) -> OpenPairState | None:
    symbol = candidate.symbol
    first_exchange = exchanges[first_exchange_name]
    hedge_exchange = exchanges[hedge_exchange_name]
    first_leg_done_ms = now_ms()

    hedge_preview = exchange_order_execution_preview(
        hedge_exchange,
        symbol,
        hedge_side,
        first_filled_qty,
        config.close_aggressive_bps,
    )
    hedgeable_first_fill_qty = _safe_float(hedge_preview.get("executable_qty"), 0.0)
    if config.maker_entry_q4_fast_hedge:
        hedge_price = _safe_float(hedge_preview.get("price"), 0.0)
        hedgeable_first_fill_qty = normalize_exchange_qty_for_order(
            hedge_exchange,
            symbol,
            first_filled_qty,
            hedge_price,
        )
    if hedgeable_first_fill_qty <= EPS:
        flattened = await emergency_flatten_symbol(
            first_exchange,
            symbol,
            aggressive_bps=config.emergency_flatten_bps,
        )
        record_live_event(
            config,
            "maker_entry_fill_not_hedgeable",
            candidate_id=candidate.id,
            symbol=symbol,
            first_exchange=first_exchange_name,
            first_side=first_side,
            first_filled_qty=first_filled_qty,
            hedge_min_qty=_safe_float(hedge_preview.get("min_executable_qty"), 0.0),
            rollback_ok=flattened,
        )
        if not flattened:
            log_danger(title="메이커체결헤지불가정리실패", symbol=symbol, first_qty=f"{first_filled_qty:.6f}")
        return None

    if config.maker_entry_q4_fast_hedge:
        record_live_event(
            config,
            "maker_entry_q4_fast_hedge",
            source=source,
            candidate_id=candidate.id,
            symbol=symbol,
            first_exchange=first_exchange_name,
            first_side=first_side,
            hedge_exchange=hedge_exchange_name,
            hedge_side=hedge_side,
            first_filled_qty=first_filled_qty,
            hedgeable_qty=hedgeable_first_fill_qty,
            first_fill_price=first_price_used,
            hedge_price=_safe_float(hedge_preview.get("price"), 0.0),
            reason="question4_style_skip_post_fill_edge_check",
        )
    else:
        post_fill_edge_decision = evaluate_post_first_fill_edge(
            candidate=candidate,
            config=config,
            first_exchange_name=first_exchange_name,
            first_fill_price=first_price_used,
            hedge_price=_safe_float(hedge_preview.get("price"), 0.0),
            qty=hedgeable_first_fill_qty,
        )
        record_live_event(
            config,
            "entry_post_first_fill_edge_check",
            source=source,
            candidate_id=candidate.id,
            symbol=symbol,
            first_exchange=first_exchange_name,
            first_side=first_side,
            hedge_exchange=hedge_exchange_name,
            hedge_side=hedge_side,
            first_filled_qty=first_filled_qty,
            hedgeable_qty=hedgeable_first_fill_qty,
            first_fill_price=first_price_used,
            hedge_price=post_fill_edge_decision.hedge_price,
            long_price=post_fill_edge_decision.long_price,
            short_price=post_fill_edge_decision.short_price,
            actual_open_edge_usd=post_fill_edge_decision.actual_open_edge_usd,
            actual_open_edge_bps=post_fill_edge_decision.actual_open_edge_bps,
            min_open_edge_bps=post_fill_edge_decision.min_open_edge_bps,
            actual_estimated_net_edge_usd=post_fill_edge_decision.actual_estimated_net_edge_usd,
            actual_estimated_net_edge_bps=post_fill_edge_decision.actual_estimated_net_edge_bps,
            min_net_edge_bps=post_fill_edge_decision.min_net_edge_bps,
            reference_notional_usd=post_fill_edge_decision.reference_notional_usd,
            can_hedge=post_fill_edge_decision.can_hedge,
            reason=post_fill_edge_decision.reason,
            expected_net_edge_bps=candidate.expected_net_edge_bps,
            expected_open_edge_bps=candidate.entry_open_edge_bps,
        )
        if not post_fill_edge_decision.can_hedge:
            flattened = await emergency_flatten_symbol(
                first_exchange,
                symbol,
                aggressive_bps=config.emergency_flatten_bps,
            )
            record_live_event(
                config,
                "entry_aborted_after_first_fill_bad_edge",
                source=source,
                candidate_id=candidate.id,
                symbol=symbol,
                first_exchange=first_exchange_name,
                first_side=first_side,
                first_filled_qty=first_filled_qty,
                hedgeable_qty=hedgeable_first_fill_qty,
                first_fill_price=first_price_used,
                hedge_price=post_fill_edge_decision.hedge_price,
                actual_open_edge_usd=post_fill_edge_decision.actual_open_edge_usd,
                actual_open_edge_bps=post_fill_edge_decision.actual_open_edge_bps,
                min_open_edge_bps=post_fill_edge_decision.min_open_edge_bps,
                actual_estimated_net_edge_usd=post_fill_edge_decision.actual_estimated_net_edge_usd,
                actual_estimated_net_edge_bps=post_fill_edge_decision.actual_estimated_net_edge_bps,
                min_net_edge_bps=post_fill_edge_decision.min_net_edge_bps,
                reason=post_fill_edge_decision.reason,
                rollback_ok=flattened,
            )
            if not flattened:
                record_live_event(
                    config,
                    "entry_bad_edge_rollback_failed",
                    source=source,
                    candidate_id=candidate.id,
                    symbol=symbol,
                    first_exchange=first_exchange_name,
                    first_side=first_side,
                    first_filled_qty=first_filled_qty,
                    reason=post_fill_edge_decision.reason,
                )
                log_danger(
                    title="메이커체결나쁜갭정리실패",
                    symbol=symbol,
                    first_exchange=first_exchange_name,
                    first_qty=f"{first_filled_qty:.6f}",
                    actual_open_edge_bps=f"{post_fill_edge_decision.actual_open_edge_bps:.4f}",
                    actual_net_edge_bps=f"{post_fill_edge_decision.actual_estimated_net_edge_bps:.4f}",
                )
            return None

    hedge_start_ms = now_ms()
    hedge_execution = await execute_residual_hedge(
        stage_label="entry",
        exchange_name=hedge_exchange_name,
        exchange=hedge_exchange,
        symbol=symbol,
        side=hedge_side,
        pre_qty=pre_qty_hedge,
        target_qty=hedgeable_first_fill_qty,
        config=config,
        is_close=False,
        eps=EPS,
        extract_position_qty=extract_position_qty,
        marketable_limit_price_with_bps=marketable_limit_price_with_bps,
        print_verbose=print_verbose,
        use_reduce_only_market_close=use_reduce_only_market_close,
        resolve_effective_order_qty=resolve_effective_order_qty,
        extract_order_avg_price=extract_order_avg_price,
        wait_for_position_fill=wait_for_position_fill,
        wait_for_position_close=wait_for_position_close,
        cancel_if_open=cancel_if_open,
        settle_grace_sec=settle_grace_sec,
        record_order_event=lambda event_type, **payload: record_live_event(config, event_type, **payload),
    )
    hedge_done_ms = now_ms()
    hedged_qty = hedge_execution.filled_qty
    hedge_price_used = hedge_execution.avg_price
    if hedge_execution.remaining_qty > EPS:
        log_hedge_incomplete(
            symbol=symbol,
            first_filled_qty=first_filled_qty,
            hedged_qty=hedged_qty,
            remaining_qty=hedge_execution.remaining_qty,
        )
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        return None

    pre_qty_by_exchange = {
        first_exchange_name: pre_qty_first,
        hedge_exchange_name: pre_qty_hedge,
    }
    pre_long_qty = pre_qty_by_exchange.get(candidate.long_exchange, 0.0)
    pre_short_qty = pre_qty_by_exchange.get(candidate.short_exchange, 0.0)

    async def refresh_entry_deltas(reason_prefix: str) -> tuple[dict[str, Any], dict[str, Any], float, float, float, float, float]:
        await refresh_symbol_state(
            exchanges[candidate.long_exchange],
            symbol,
            f"{reason_prefix}_long",
            include_account=False,
            include_open_orders=False,
            include_funding=False,
        )
        await refresh_symbol_state(
            exchanges[candidate.short_exchange],
            symbol,
            f"{reason_prefix}_short",
            include_account=False,
            include_open_orders=False,
            include_funding=False,
        )
        long_snapshot = exchanges[candidate.long_exchange].get_symbol_snapshot(symbol)
        short_snapshot = exchanges[candidate.short_exchange].get_symbol_snapshot(symbol)
        long_qty = extract_position_qty(long_snapshot)
        short_qty = extract_position_qty(short_snapshot)
        long_delta = entry_delta_qty(pre_long_qty, long_qty, "long")
        short_delta = entry_delta_qty(pre_short_qty, short_qty, "short")
        return long_snapshot, short_snapshot, long_qty, short_qty, long_delta, short_delta, min(long_delta, short_delta)

    (
        long_snapshot_after,
        short_snapshot_after,
        long_qty_after,
        short_qty_after,
        long_entry_delta,
        short_entry_delta,
        pair_qty,
    ) = await refresh_entry_deltas("maker_entry_post_open")

    delta_attempts = 1
    max_delta_attempts = 4
    while pair_qty <= EPS and min(first_filled_qty, hedged_qty) > EPS and delta_attempts < max_delta_attempts:
        delta_attempts += 1
        record_live_event(
            config,
            "maker_entry_delta_waiting_for_account_snapshot",
            candidate_id=candidate.id,
            symbol=symbol,
            attempt=delta_attempts,
            max_attempts=max_delta_attempts,
            first_filled_qty=first_filled_qty,
            hedged_qty=hedged_qty,
            long_qty_after=long_qty_after,
            short_qty_after=short_qty_after,
            long_entry_delta=long_entry_delta,
            short_entry_delta=short_entry_delta,
        )
        await asyncio.sleep(config.action_settle_wait_sec)
        (
            long_snapshot_after,
            short_snapshot_after,
            long_qty_after,
            short_qty_after,
            long_entry_delta,
            short_entry_delta,
            pair_qty,
        ) = await refresh_entry_deltas(f"maker_entry_post_delta_retry_{delta_attempts}")

    if pair_qty <= EPS:
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        record_live_event(
            config,
            "maker_entry_delta_missing",
            candidate_id=candidate.id,
            symbol=symbol,
            first_filled_qty=first_filled_qty,
            hedged_qty=hedged_qty,
            long_qty_after=long_qty_after,
            short_qty_after=short_qty_after,
            long_entry_delta=long_entry_delta,
            short_entry_delta=short_entry_delta,
            attempts=delta_attempts,
        )
        return None

    long_order_price = first_price_used if first_exchange_name == candidate.long_exchange else hedge_price_used
    short_order_price = hedge_price_used if hedge_exchange_name == candidate.short_exchange else first_price_used
    long_entry_price = (
        extract_position_entry_price(long_snapshot_after)
        or long_order_price
        or extract_mid_price(long_snapshot_after)
    )
    short_entry_price = (
        extract_position_entry_price(short_snapshot_after)
        or short_order_price
        or extract_mid_price(short_snapshot_after)
    )
    if long_entry_price <= 0 or short_entry_price <= 0:
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        log_danger(
            title="메이커진입평단누락",
            symbol=symbol,
            long_entry=f"{long_entry_price:.8f}",
            short_entry=f"{short_entry_price:.8f}",
        )
        return None

    pair_id = f"{symbol}:{candidate.long_exchange}:{candidate.short_exchange}:{int(time.time() * 1000)}"
    reference_notional_usd = max(pair_qty * ((long_entry_price + short_entry_price) / 2.0), 0.0)
    actual_entry_open_edge_usd = pair_qty * (short_entry_price - long_entry_price)
    actual_entry_open_edge_bps = (
        actual_entry_open_edge_usd / reference_notional_usd * 10000.0
        if reference_notional_usd > EPS
        else 0.0
    )
    expected_open_edge_usd_for_actual_qty = pair_qty * (candidate.short_bid - candidate.long_ask)
    entry_edge_slippage_usd = actual_entry_open_edge_usd - expected_open_edge_usd_for_actual_qty
    entry_edge_slippage_bps = actual_entry_open_edge_bps - candidate.entry_open_edge_bps
    qty_scale = pair_qty / candidate.executable_qty if candidate.executable_qty > EPS else 1.0
    actual_estimated_net_edge_usd = (
        actual_entry_open_edge_usd
        + candidate.effective_funding_pnl_usd * qty_scale
        - candidate.open_fee_cost_usd * qty_scale
        - candidate.expected_close_cost_usd * qty_scale
        - candidate.risk_buffer_usd * qty_scale
        - candidate.stale_data_penalty_usd * qty_scale
    )
    actual_estimated_net_edge_bps = (
        actual_estimated_net_edge_usd / reference_notional_usd * 10000.0
        if reference_notional_usd > EPS
        else 0.0
    )
    record_live_event(
        config,
        "entry_quality_analysis",
        source=source,
        pair_id=pair_id,
        symbol=symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        qty=pair_qty,
        requested_qty=executable_qty,
        expected_open_edge_usd=candidate.entry_open_edge_usd,
        expected_open_edge_bps=candidate.entry_open_edge_bps,
        expected_open_edge_usd_for_actual_qty=expected_open_edge_usd_for_actual_qty,
        expected_net_edge_usd=candidate.expected_net_edge_usd,
        expected_net_edge_bps=candidate.expected_net_edge_bps,
        actual_open_edge_usd=actual_entry_open_edge_usd,
        actual_open_edge_bps=actual_entry_open_edge_bps,
        actual_estimated_net_edge_usd=actual_estimated_net_edge_usd,
        actual_estimated_net_edge_bps=actual_estimated_net_edge_bps,
        entry_edge_slippage_usd=entry_edge_slippage_usd,
        entry_edge_slippage_bps=entry_edge_slippage_bps,
        first_exchange=first_exchange_name,
        first_side=first_side,
        hedge_exchange=hedge_exchange_name,
        hedge_side=hedge_side,
        first_leg_fill_price=first_price_used,
        hedge_fill_price=hedge_price_used,
        first_order_id=first_order_id,
        first_client_order_id=first_client_order_id,
        hedge_order_id=hedge_execution.order_id,
        hedge_client_order_id=hedge_execution.client_order_id,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        first_leg_done_ms=first_leg_done_ms,
        hedge_start_ms=hedge_start_ms,
        hedge_done_ms=hedge_done_ms,
        total_entry_elapsed_ms=max(hedge_done_ms - entry_started_ms, 0),
        first_fill_to_hedge_fill_ms=max(hedge_done_ms - first_leg_done_ms, 0),
    )

    log_pair_opened(
        pair_id=pair_id,
        symbol=symbol,
        qty=pair_qty,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
    )
    pair = OpenPairState(
        pair_id=pair_id,
        symbol=symbol,
        long_exchange=candidate.long_exchange,
        short_exchange=candidate.short_exchange,
        qty=pair_qty,
        requested_qty=executable_qty,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        entry_time_ms=int(time.time() * 1000),
        entry_net_edge_usd=candidate.expected_net_edge_usd,
        funding_long_rate=candidate.funding_long_rate,
        funding_short_rate=candidate.funding_short_rate,
        status="open",
    )
    clear_pair_assumed_flat(pair, assumed_flat_until_ms)
    record_live_event(
        config,
        "pair_opened",
        source=source,
        pair=pair_event_payload(pair),
        candidate={
            "id": candidate.id,
            "executable_qty": candidate.executable_qty,
            "notional_usd": candidate.notional_usd,
            "entry_open_edge_usd": candidate.entry_open_edge_usd,
            "entry_open_edge_bps": candidate.entry_open_edge_bps,
            "effective_funding_pnl_usd": candidate.effective_funding_pnl_usd,
            "expected_net_edge_usd": candidate.expected_net_edge_usd,
            "expected_net_edge_bps": candidate.expected_net_edge_bps,
            "actual_entry_open_edge_usd": actual_entry_open_edge_usd,
            "actual_entry_open_edge_bps": actual_entry_open_edge_bps,
            "actual_estimated_net_edge_usd": actual_estimated_net_edge_usd,
            "actual_estimated_net_edge_bps": actual_estimated_net_edge_bps,
            "entry_edge_slippage_usd": entry_edge_slippage_usd,
            "entry_edge_slippage_bps": entry_edge_slippage_bps,
        },
        entry_position_delta={
            "long_pre_qty": pre_long_qty,
            "long_qty_after": long_qty_after,
            "long_entry_delta": long_entry_delta,
            "short_pre_qty": pre_short_qty,
            "short_qty_after": short_qty_after,
            "short_entry_delta": short_entry_delta,
            "first_exchange": first_exchange_name,
            "first_side": first_side,
            "first_filled_qty": first_filled_qty,
            "hedge_exchange": hedge_exchange_name,
            "hedge_side": hedge_side,
            "hedged_qty": hedged_qty,
        },
    )
    return pair


def estimate_hold_pnl_next_funding(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
    short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
    long_ref = extract_mid_price(long_snapshot)
    short_ref = extract_mid_price(short_snapshot)
    if long_ref <= 0 or short_ref <= 0:
        return -1e18

    long_notional = pair.qty * long_ref
    short_notional = pair.qty * short_ref
    long_next_funding_ms = extract_next_funding_ms(long_snapshot)
    short_next_funding_ms = extract_next_funding_ms(short_snapshot)
    now_ms = int(time.time() * 1000)
    # This estimator answers "what's the PnL if we hold until the next funding
    # settlement?" — the relevant horizon is whichever side settles first.
    long_remaining_sec = seconds_until(long_next_funding_ms, now_ms=now_ms) if long_next_funding_ms > 0 else 0.0
    short_remaining_sec = seconds_until(short_next_funding_ms, now_ms=now_ms) if short_next_funding_ms > 0 else 0.0
    candidates_sec = [s for s in (long_remaining_sec, short_remaining_sec) if s > 0]
    forecast_hold_sec = min(candidates_sec) if candidates_sec else float(config.min_hold_sec)
    (
        _raw_funding_pnl_usd,
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        effective_funding_pnl_usd,
    ) = compute_effective_funding_components(
        long_rate=extract_funding_rate(long_snapshot),
        short_rate=extract_funding_rate(short_snapshot),
        long_notional=long_notional,
        short_notional=short_notional,
        long_next_funding_ms=long_next_funding_ms,
        short_next_funding_ms=short_next_funding_ms,
        config=config,
        forecast_hold_sec=forecast_hold_sec,
        now_ms=now_ms,
    )
    stale_data_penalty_usd = compute_stale_data_penalty_usd(
        max((long_notional + short_notional) / 2.0, 0.0),
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        config,
    )
    _long_ask, _short_bid, open_edge_usd = current_pair_open_edge(pair, exchanges)
    return open_edge_usd + effective_funding_pnl_usd - stale_data_penalty_usd


def current_pair_reference_notional_usd(
    pair: OpenPairState,
    exchanges: dict[str, Any],
) -> float:
    long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
    short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
    long_ref = extract_mid_price(long_snapshot)
    short_ref = extract_mid_price(short_snapshot)
    if long_ref > 0 and short_ref > 0:
        return max(pair.qty * ((long_ref + short_ref) / 2.0), 0.0)
    return pair_entry_reference_notional_usd(pair)


def spread_close_signal_confirmed(
    pair: OpenPairState,
    decision: str,
    config: StrategyConfig,
    current_ms: int,
) -> bool:
    if decision != "spread_nonpositive":
        pair.close_signal_first_seen_ms = 0
        pair.close_signal_confirm_count = 0
        return True

    confirm_count = max(int(config.close_spread_confirm_count or 1), 1)
    confirm_ms = int(max(float(config.close_spread_confirm_sec or 0.0), 0.0) * 1000)
    if confirm_count <= 1 and confirm_ms <= 0:
        return True

    if pair.last_close_reason != decision or pair.close_signal_first_seen_ms <= 0:
        pair.close_signal_first_seen_ms = current_ms
        pair.close_signal_confirm_count = 1
    else:
        pair.close_signal_confirm_count += 1

    count_ready = pair.close_signal_confirm_count >= confirm_count
    time_ready = current_ms - pair.close_signal_first_seen_ms >= confirm_ms
    return count_ready and time_ready


def estimate_close_pnl_now(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    long_bid, short_ask, _ = current_pair_close_edge(pair, exchanges)
    return close_pnl_breakdown_usd(
        pair,
        config,
        long_bid=long_bid,
        short_ask=short_ask,
    ).net_close_pnl_usd


def maker_then_taker_close_enabled(pair: OpenPairState, config: StrategyConfig) -> bool:
    if config.close_execution_mode != "maker_then_taker":
        return False
    if config.close_maker_timeout_sec <= 0:
        return False
    allowed_reasons = {str(reason).strip().lower() for reason in config.close_maker_reasons if str(reason).strip()}
    close_reason = str(pair.last_close_reason or "").strip().lower()
    return close_reason in allowed_reasons


async def close_pair_maker_then_taker_once(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> PairFlattenResult | None:
    if not maker_then_taker_close_enabled(pair, config):
        return None

    specs = pair_close_leg_specs(pair, config.close_first_leg_role)
    maker_spec = specs[0]
    taker_spec = specs[1]
    symbol = pair.symbol

    await refresh_symbol_state(exchanges[pair.long_exchange], symbol, "strategy_close_maker_pre_long")
    await refresh_symbol_state(exchanges[pair.short_exchange], symbol, "strategy_close_maker_pre_short")
    long_qty = effective_position_qty(
        exchanges[pair.long_exchange].get_symbol_snapshot(symbol),
        exchanges[pair.long_exchange],
        symbol,
        config,
    )
    short_qty = effective_position_qty(
        exchanges[pair.short_exchange].get_symbol_snapshot(symbol),
        exchanges[pair.short_exchange],
        symbol,
        config,
    )
    state, balanced_qty, _gap = classify_pair_position_state(long_qty, short_qty, reference_qty=pair.qty)
    if state != "balanced_open" or balanced_qty <= EPS:
        record_live_event(
            config,
            "pair_close_maker_skipped",
            pair=pair_event_payload(pair),
            reason="not_balanced_open",
            close_state=state,
            long_qty=long_qty,
            short_qty=short_qty,
        )
        return None

    maker_exchange_name = maker_spec["exchange_name"]
    maker_exchange = exchanges[maker_exchange_name]
    maker_side = maker_spec["side"]
    maker_role = maker_spec["role"]
    maker_pre_qty = long_qty if maker_role == "long" else short_qty
    maker_target_qty = min(pair.qty, balanced_qty, abs(maker_pre_qty))
    maker_price = maker_close_limit_price(
        maker_exchange,
        symbol,
        maker_side,
        config.close_maker_price_bps,
    )
    maker_target_qty = normalize_exchange_qty_for_order(maker_exchange, symbol, maker_target_qty, maker_price)
    if maker_price <= 0 or maker_target_qty <= EPS:
        record_live_event(
            config,
            "pair_close_maker_skipped",
            pair=pair_event_payload(pair),
            reason="maker_not_executable",
            maker_exchange=maker_exchange_name,
            maker_side=maker_side,
            maker_price=maker_price,
            maker_target_qty=maker_target_qty,
        )
        return None

    record_live_event(
        config,
        "pair_close_maker_then_taker_started",
        pair=pair_event_payload(pair),
        maker_exchange=maker_exchange_name,
        maker_role=maker_role,
        maker_side=maker_side,
        maker_target_qty=maker_target_qty,
        maker_price=maker_price,
        timeout_sec=config.close_maker_timeout_sec,
    )

    maker_closed_qty, maker_order, maker_price_used = await execute_requoting_limit_leg(
        exchange=maker_exchange,
        symbol=symbol,
        side=maker_side,
        target_qty=maker_target_qty,
        pre_qty=maker_pre_qty,
        timeout_sec=config.close_maker_timeout_sec,
        config=config,
        is_close=True,
        price_resolver=lambda: maker_close_limit_price(
            maker_exchange,
            symbol,
            maker_side,
            config.close_maker_price_bps,
        ),
        log_prefix="close-maker",
        probe_reason_prefix="strategy_close_maker",
    )
    maker_closed_qty = min(maker_closed_qty, maker_target_qty)
    if maker_closed_qty <= EPS:
        record_live_event(
            config,
            "pair_close_maker_no_fill",
            pair=pair_event_payload(pair),
            maker_exchange=maker_exchange_name,
            maker_role=maker_role,
            maker_side=maker_side,
            maker_target_qty=maker_target_qty,
            maker_price=maker_price_used or maker_price,
            order_id=extract_order_uuid(maker_order),
        )
        return None

    await refresh_symbol_state(maker_exchange, symbol, "strategy_close_maker_filled")
    maker_leg = FlattenLegResult(
        exchange_name=maker_exchange_name,
        symbol=symbol,
        role=maker_role,
        side=maker_side,
        requested_qty=maker_target_qty,
        start_qty=maker_pre_qty,
        end_qty=extract_position_qty(maker_exchange.get_symbol_snapshot(symbol)),
        closed_qty=maker_closed_qty,
        ok=True,
        attempted_order=True,
    )

    taker_exchange_name = taker_spec["exchange_name"]
    taker_exchange = exchanges[taker_exchange_name]
    taker_side = taker_spec["side"]
    taker_role = taker_spec["role"]
    taker_result = await close_leg_market_qty(
        exchange=taker_exchange,
        symbol=symbol,
        role=taker_role,
        side=taker_side,
        target_qty=maker_closed_qty,
        config=config,
        is_close=True,
    )

    if not taker_result.ok:
        restore_side = "buy" if maker_side == "sell" else "sell"
        restore_result = await close_leg_market_qty(
            exchange=maker_exchange,
            symbol=symbol,
            role=f"{maker_role}_restore",
            side=restore_side,
            target_qty=maker_closed_qty,
            config=config,
            is_close=False,
        )
        pair_result = PairFlattenResult(
            ok=False,
            leg_results=[maker_leg, taker_result, restore_result],
            failed_leg=taker_result,
            other_leg_action="rehedged" if restore_result.ok else "maker_close_only",
            neutralized=restore_result.ok,
            neutralize_payload={
                "restore_exchange": maker_exchange_name,
                "restore_side": restore_side,
                "restore_qty": maker_closed_qty,
                "restore_ok": restore_result.ok,
            },
        )
        record_live_event(
            config,
            "pair_close_maker_then_taker_failed",
            pair=pair_event_payload(pair),
            maker_leg=flatten_leg_result_payload(maker_leg),
            taker_leg=flatten_leg_result_payload(taker_result),
            restore_leg=flatten_leg_result_payload(restore_result),
        )
        return pair_result

    record_live_event(
        config,
        "pair_close_maker_then_taker_matched",
        pair=pair_event_payload(pair),
        maker_leg=flatten_leg_result_payload(maker_leg),
        taker_leg=flatten_leg_result_payload(taker_result),
        maker_closed_qty=maker_closed_qty,
        remaining_pair_qty=max(pair.qty - maker_closed_qty, 0.0),
    )

    if maker_closed_qty + qty_match_tolerance(pair.qty) >= pair.qty:
        return PairFlattenResult(
            ok=True,
            leg_results=[maker_leg, taker_result],
            other_leg_action="closed",
        )

    record_live_event(
        config,
        "pair_close_maker_partial_then_fallback",
        pair=pair_event_payload(pair),
        maker_closed_qty=maker_closed_qty,
        remaining_pair_qty=max(pair.qty - maker_closed_qty, 0.0),
    )
    return None


async def close_pair_limit_then_hedge(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> bool:
    pair.assumed_flat_exchanges = []
    print_verbose(
        config,
        f"[strategy][close] pair flatten 시작 pair_id={pair.pair_id} "
        f"symbol={pair.symbol} long={pair.long_exchange} short={pair.short_exchange}"
    )

    flatten_result = await close_pair_maker_then_taker_once(pair, exchanges, config)
    if flatten_result is None:
        flatten_result = await emergency_flatten_pair_state(
            pair,
            exchanges,
            aggressive_bps=config.emergency_flatten_bps,
            config=config,
            close_first_leg_role=config.close_first_leg_role,
            restore_neutrality=True,
        )
    flattened = flatten_result.ok
    forced_flat_exchanges = flatten_result.forced_flat_exchanges

    close_state, long_qty_after, short_qty_after, balanced_qty_after, residual_gap = await settle_pair_close_state(
        pair,
        exchanges,
        config,
        reference_qty=pair.qty,
        reason_prefix="strategy_close_post",
        forced_flat_exchanges=forced_flat_exchanges,
    )
    pair.qty = balanced_qty_after
    if flatten_result.failed_leg is not None:
        failed_payload = flatten_leg_result_payload(flatten_result.failed_leg)
        log_danger(
            title="페어종료레그실패",
            id=pair.pair_id,
            symbol=pair.symbol,
            failed_exchange=flatten_result.failed_leg.exchange_name,
            failed_side=flatten_result.failed_leg.side,
            failed_qty=f"{flatten_result.failed_leg.requested_qty:.6f}",
            reason=flatten_result.failed_leg.failure_reason,
            other_leg_action=flatten_result.other_leg_action,
            long_after=f"{long_qty_after:.6f}",
            short_after=f"{short_qty_after:.6f}",
            net_qty=f"{(long_qty_after + short_qty_after):.6f}",
        )
        record_live_event(
            config,
            "pair_close_leg_failed",
            pair=pair_event_payload(pair),
            failed_leg=failed_payload,
            other_leg_action=flatten_result.other_leg_action,
            close_state=close_state,
            long_qty_after=long_qty_after,
            short_qty_after=short_qty_after,
            net_qty_after=long_qty_after + short_qty_after,
            neutralized=flatten_result.neutralized,
            neutralize=flatten_result.neutralize_payload,
        )
    if close_state == "closed":
        pair.status = "closed"
        pair.assumed_flat_exchanges = [pair.long_exchange, pair.short_exchange]
        request_account_refresh(
            exchanges[pair.long_exchange],
            "pair_close_complete",
            priority="urgent",
            symbol=pair.symbol,
        )
        request_account_refresh(
            exchanges[pair.short_exchange],
            "pair_close_complete",
            priority="urgent",
            symbol=pair.symbol,
        )
        log_pair_closed(pair_id=pair.pair_id)
        record_live_event(config, "pair_closed", pair=pair_event_payload(pair))
        return True

    if close_state == "balanced_open":
        pair.status = "close_pending"
        print(
            f"[페어종료][보류] pair_id={pair.pair_id} 사유=균형포지션잔존 "
            f"잔여페어수량={pair.qty:.6f} 청산시도성공={flattened}"
        )
        record_live_event(
            config,
            "pair_close_pending",
            pair=pair_event_payload(pair),
            close_state=close_state,
            long_qty_after=long_qty_after,
            short_qty_after=short_qty_after,
            residual_gap=residual_gap,
        )
        return False

    pair.status = "close_imbalance"
    log_danger(
        title="청산불균형",
        id=pair.pair_id,
        long_after=f"{long_qty_after:.6f}",
        short_after=f"{short_qty_after:.6f}",
        residual_gap=f"{residual_gap:.6f}",
        state=close_state,
        flattened=flattened,
        cause=(flatten_result.failed_leg.failure_reason if flatten_result.failed_leg else close_state),
        other_leg_action=flatten_result.other_leg_action,
    )
    pair.status = "close_pending"
    print(
        f"[페어종료][보류] pair_id={pair.pair_id} 사유=청산후불균형 "
        f"롱수량={long_qty_after:.6f} 숏수량={short_qty_after:.6f} "
        f"상태={close_state}"
    )
    record_live_event(
        config,
        "pair_close_pending",
        pair=pair_event_payload(pair),
        close_state=close_state,
        long_qty_after=long_qty_after,
        short_qty_after=short_qty_after,
        residual_gap=residual_gap,
        failed_leg=flatten_leg_result_payload(flatten_result.failed_leg),
        other_leg_action=flatten_result.other_leg_action,
        neutralized=flatten_result.neutralized,
    )
    return False


async def manage_open_pairs(
    open_pairs: dict[str, OpenPairState],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int],
    symbol_recent_close_ms: dict[str, int] | None = None,
) -> None:
    for pair_id, pair in list(open_pairs.items()):
        long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
        short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
        action_inputs_fresh = pair_action_inputs_are_fresh(long_snapshot, short_snapshot, config)
        long_qty = effective_position_qty(
            long_snapshot,
            exchanges[pair.long_exchange],
            pair.symbol,
            config,
            assumed_flat_until_ms=assumed_flat_until_ms,
        )
        short_qty = effective_position_qty(
            short_snapshot,
            exchanges[pair.short_exchange],
            pair.symbol,
            config,
            assumed_flat_until_ms=assumed_flat_until_ms,
        )
        if abs(long_qty) <= EPS and abs(short_qty) <= EPS:
            pair.status = "closed"
            log_pair_closed_detected(pair_id=pair_id)
            record_live_event(config, "pair_closed_detected", pair=pair_event_payload(pair))
            mark_symbol_recently_closed(symbol_recent_close_ms, pair.symbol)
            del open_pairs[pair_id]
            continue

        if long_qty <= EPS or short_qty >= -EPS:
            await refresh_symbol_state(exchanges[pair.long_exchange], pair.symbol, "strategy_manage_pair_long_mismatch")
            await refresh_symbol_state(exchanges[pair.short_exchange], pair.symbol, "strategy_manage_pair_short_mismatch")
            long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
            short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
            action_inputs_fresh = pair_action_inputs_are_fresh(long_snapshot, short_snapshot, config)
            long_qty = effective_position_qty(
                long_snapshot,
                exchanges[pair.long_exchange],
                pair.symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            short_qty = effective_position_qty(
                short_snapshot,
                exchanges[pair.short_exchange],
                pair.symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            if abs(long_qty) <= EPS and abs(short_qty) <= EPS:
                pair.status = "closed"
                log_pair_closed_detected(pair_id=pair_id)
                record_live_event(config, "pair_closed_detected", pair=pair_event_payload(pair))
                mark_symbol_recently_closed(symbol_recent_close_ms, pair.symbol)
                del open_pairs[pair_id]
                continue
            if not action_inputs_fresh:
                pair.status = "await_account_sync"
                continue
            current_ms = now_ms()
            residual_exchange_name = pair.long_exchange if abs(long_qty) >= abs(short_qty) else pair.short_exchange
            residual_exchange = exchanges[residual_exchange_name]
            residual_snapshot = long_snapshot if residual_exchange_name == pair.long_exchange else short_snapshot
            residual_qty_signed = long_qty if residual_exchange_name == pair.long_exchange else short_qty
            residual_side = "sell" if residual_qty_signed > 0 else "buy"
            residual_reference_qty = max(pair.requested_qty, pair.qty, abs(long_qty), abs(short_qty))
            is_micro_residual, micro_payload = classify_micro_residual(
                residual_exchange,
                pair.symbol,
                residual_side,
                abs(residual_qty_signed),
                residual_reference_qty,
                config,
                aggressive_bps=config.emergency_flatten_bps,
                snapshot=residual_snapshot,
            )
            pair.status = "micro_residual_cleanup" if is_micro_residual else "direction_mismatch"
            if current_ms - pair.last_direction_mismatch_log_ms >= int(config.danger_log_cooldown_sec * 1000):
                if is_micro_residual:
                    print(
                        f"[페어][소량잔량] pair_id={pair_id} "
                        f"거래소={residual_exchange_name} 방향={residual_side} "
                        f"잔량={abs(residual_qty_signed):.6f} "
                        f"명목USD={micro_payload['notional_usd']:.4f} "
                        f"비율={micro_payload['ratio']:.6f} "
                        f"롱수량={long_qty:.6f} 숏수량={short_qty:.6f}"
                    )
                    record_live_event(
                        config,
                        "pair_direction_micro_residual_detected",
                        pair=pair_event_payload(pair),
                        residual_exchange=residual_exchange_name,
                        residual_side=residual_side,
                        residual_qty=abs(residual_qty_signed),
                        long_qty=long_qty,
                        short_qty=short_qty,
                        **micro_payload,
                    )
                else:
                    log_danger(
                        title="페어방향불일치",
                        id=pair_id,
                        long_qty=f"{long_qty:.6f}",
                        short_qty=f"{short_qty:.6f}",
                    )
                pair.last_direction_mismatch_log_ms = current_ms
            if current_ms < pair.next_recovery_retry_ms:
                continue

            pair.next_recovery_retry_ms = current_ms + int(config.recovery_retry_cooldown_sec * 1000)
            flatten_result = await emergency_flatten_pair_state(
                pair,
                exchanges,
                aggressive_bps=config.emergency_flatten_bps,
                config=config,
                close_first_leg_role=config.close_first_leg_role,
                restore_neutrality=True,
            )
            flattened = flatten_result.ok
            forced_flat_exchanges = flatten_result.forced_flat_exchanges
            mismatch_state, long_qty_after, short_qty_after, balanced_qty_after, residual_gap = await settle_pair_close_state(
                pair,
                exchanges,
                config,
                reference_qty=max(pair.qty, abs(long_qty), abs(short_qty)),
                reason_prefix="strategy_manage_pair_direction_mismatch",
                forced_flat_exchanges=forced_flat_exchanges,
            )
            pair.qty = balanced_qty_after
            if flatten_result.failed_leg is not None:
                event_type = "pair_micro_residual_cleanup_failed" if is_micro_residual else "pair_close_leg_failed"
                record_live_event(
                    config,
                    event_type,
                    pair=pair_event_payload(pair),
                    failed_leg=flatten_leg_result_payload(flatten_result.failed_leg),
                    other_leg_action=flatten_result.other_leg_action,
                    close_state=mismatch_state,
                    long_qty_after=long_qty_after,
                    short_qty_after=short_qty_after,
                    net_qty_after=long_qty_after + short_qty_after,
                    neutralized=flatten_result.neutralized,
                    neutralize=flatten_result.neutralize_payload,
                    micro_residual=is_micro_residual,
                )
            if flattened and mismatch_state == "closed":
                pair.status = "closed"
                pair.assumed_flat_exchanges = sorted(forced_flat_exchanges)
                register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
                if is_micro_residual:
                    print(f"[페어][종료완료] pair_id={pair_id} 사유=소량잔량정리")
                    event_type = "pair_closed_after_micro_residual_cleanup"
                else:
                    print(f"[복구][페어종료완료] pair_id={pair_id}")
                    event_type = "pair_closed_after_recovery"
                record_live_event(
                    config,
                    event_type,
                    pair=pair_event_payload(pair),
                    forced_flat_exchanges=sorted(forced_flat_exchanges),
                    micro_residual=is_micro_residual,
                )
                mark_symbol_recently_closed(symbol_recent_close_ms, pair.symbol)
                del open_pairs[pair_id]
                continue
            if mismatch_state == "balanced_open":
                pair.status = "open"
                print(
                    f"[복구][페어유지] pair_id={pair_id} "
                    f"잔여페어수량={pair.qty:.6f}"
                )
                record_live_event(config, "pair_recovered_balanced_open", pair=pair_event_payload(pair))
                continue
            pair.status = "recovery_pending"
            if current_ms - pair.last_direction_mismatch_log_ms >= int(config.danger_log_cooldown_sec * 1000):
                print(
                    f"[복구][대기] pair_id={pair_id} "
                    f"롱수량={long_qty_after:.6f} 숏수량={short_qty_after:.6f} "
                    f"상태={mismatch_state} 잔량차이={residual_gap:.6f}"
                )
                pair.last_direction_mismatch_log_ms = current_ms
            continue

        balanced_qty = min(abs(long_qty), abs(short_qty))
        pair.qty = balanced_qty
        long_bid_now, short_ask_now, close_edge_now = current_pair_close_edge(pair, exchanges)
        close_pnl_breakdown = close_pnl_breakdown_usd(
            pair,
            config,
            long_bid=long_bid_now,
            short_ask=short_ask_now,
        )
        close_pnl_now = close_pnl_breakdown.net_close_pnl_usd
        _, _, open_edge_now = current_pair_open_edge(pair, exchanges)
        hold_pnl = estimate_hold_pnl_next_funding(pair, exchanges, config)
        entry_age_sec = max((now_ms() - pair.entry_time_ms) / 1000.0, 0.0)
        close_reference_notional_usd = current_pair_reference_notional_usd(pair, exchanges)
        close_evaluation = evaluate_close_policy(
            pair,
            config,
            hold_pnl_usd=hold_pnl,
            entry_age_sec=entry_age_sec,
            close_pnl_usd=close_pnl_now,
            close_edge_usd=open_edge_now,
            reference_notional_usd=close_reference_notional_usd,
            action_inputs_fresh=action_inputs_fresh,
        )
        decision = close_evaluation.decision
        close_thresholds = close_evaluation.thresholds
        close_reference_notional_usd = close_evaluation.reference_notional_usd
        previous_close_reason = pair.last_close_reason
        pair.last_close_pnl_usd = close_evaluation.close_pnl_usd
        pair.last_close_edge_usd = close_evaluation.close_edge_usd
        pair.last_close_reason = decision
        if pair.last_hold_decision != decision or abs(pair.last_hold_pnl_usd - hold_pnl) >= 0.01:
            pair.last_hold_decision = decision
            pair.last_hold_pnl_usd = hold_pnl
            print_verbose(
                config,
                f"[strategy][pair] pair_id={pair_id} symbol={pair.symbol} "
                f"decision={decision} hold_pnl_next_funding={hold_pnl:.4f} "
                f"hold_buffer_usd={close_thresholds['hold_buffer_usd']:.4f} "
                f"take_profit_usd={close_thresholds['take_profit_usd']:.4f} "
                f"stop_loss_usd={close_thresholds['stop_loss_usd']:.4f} "
                f"close_reference_notional_usd={close_reference_notional_usd:.4f} "
                f"close_pnl_now={pair.last_close_pnl_usd:.4f} "
                f"open_edge_now={pair.last_close_edge_usd:.4f} "
                f"close_edge_now={close_edge_now:.4f} "
                f"entry_age_sec={entry_age_sec:.1f} min_hold_sec={config.min_hold_sec:.1f}"
            )

        if config.auto_close_enabled and close_evaluation.should_auto_close:
            current_ms = now_ms()
            if not spread_close_signal_confirmed(pair, decision, config, current_ms):
                if previous_close_reason != decision or pair.close_signal_confirm_count <= 1:
                    record_live_event(
                        config,
                        "pair_close_signal_waiting_confirm",
                        pair=pair_event_payload(pair),
                        reason=decision,
                        confirm_count=pair.close_signal_confirm_count,
                        required_count=config.close_spread_confirm_count,
                        first_seen_ms=pair.close_signal_first_seen_ms,
                        confirm_sec=config.close_spread_confirm_sec,
                    )
                continue
            if pair.close_fee_gate_first_seen_ms <= 0 or previous_close_reason != decision:
                pair.close_fee_gate_first_seen_ms = current_ms
                pair.close_fee_gate_defer_count = 0
            defer_age_sec = max((current_ms - pair.close_fee_gate_first_seen_ms) / 1000.0, 0.0)
            fee_gate_evaluation = evaluate_normal_close_fee_gate(
                config=config,
                decision=decision,
                hold_pnl_usd=hold_pnl,
                pnl=close_pnl_breakdown,
                defer_age_sec=defer_age_sec,
                defer_count=pair.close_fee_gate_defer_count + 1,
            )
            if fee_gate_evaluation.defer:
                pair.close_fee_gate_defer_count += 1
                fee_gate_metrics = dict(fee_gate_evaluation.metrics)
                fee_gate_metrics["defer_count"] = pair.close_fee_gate_defer_count
                fee_gate_metrics["defer_age_sec"] = defer_age_sec
                print_verbose(
                    config,
                    f"[strategy][close][fee-gate] pair_id={pair_id} symbol={pair.symbol} "
                    f"reason={decision} net_close_pnl={close_pnl_breakdown.net_close_pnl_usd:.4f} "
                    f"fee={close_pnl_breakdown.close_fee_cost_usd:.4f} "
                    f"slippage={close_pnl_breakdown.close_slippage_cost_usd:.4f} "
                    f"hold_pnl={hold_pnl:.4f} defer_count={pair.close_fee_gate_defer_count}"
                )
                record_live_event(
                    config,
                    "normal_close_deferred_by_fee",
                    pair=pair_event_payload(pair),
                    reason=fee_gate_evaluation.reason,
                    **fee_gate_metrics,
                )
                continue
            if fee_gate_evaluation.reason != "max_defer_elapsed":
                pair.close_fee_gate_first_seen_ms = 0
                pair.close_fee_gate_defer_count = 0
            if current_ms < pair.next_auto_close_retry_ms:
                continue
            pair.next_auto_close_retry_ms = current_ms + int(config.auto_close_retry_cooldown_sec * 1000)
            log_pair_close_trigger(pair_id=pair_id, symbol=pair.symbol, reason=decision)
            record_live_event(
                config,
                "pair_auto_close_trigger",
                pair=pair_event_payload(pair),
                reason=decision,
                entry_age_sec=entry_age_sec,
                close_thresholds=close_thresholds,
            )
            ok = await close_pair_limit_then_hedge(pair, exchanges, config)
            if ok and pair.status == "closed":
                register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
                mark_symbol_recently_closed(symbol_recent_close_ms, pair.symbol)
                del open_pairs[pair_id]
            elif not ok:
                print(
                    f"[페어종료][경고] 자동청산미완료 pair_id={pair_id} "
                    f"상태={pair.status}"
                )
                record_live_event(
                    config,
                    "pair_auto_close_not_completed",
                    pair=pair_event_payload(pair),
                    reason=decision,
                )


def describe_live_plan(candidate: PairCandidate, config: StrategyConfig) -> str:
    first_exchange, first_side = first_leg_plan(candidate, config)
    hedge_exchange, hedge_side = hedge_leg_plan(candidate, config)
    hedge_mode = (
        "market_immediate"
        if config.hedge_market_immediate
        else f"requote={config.hedge_requote_count}x{config.hedge_requote_timeout_sec}s"
    )
    return (
        f"symbol={candidate.symbol} qty={candidate.executable_qty:.6f} "
        f"first_leg={first_exchange}:{first_side} timeout={config.first_leg_timeout_sec}s "
        f"entry_price_mode={config.entry_first_leg_price_mode} "
        f"hedge={hedge_exchange}:{hedge_side} mode={hedge_mode}"
    )


async def console_confirm_entry(candidate: PairCandidate, config: StrategyConfig) -> bool:
    if config.approval_mode != "console" or not sys.stdin.isatty():
        return True
    prompt = (
        f"\n[strategy][confirm] {describe_live_plan(candidate, config)}\n"
        "execute? [y/N]: "
    )
    answer = await asyncio.to_thread(input, prompt)
    return answer.strip().lower() in {"y", "yes"}


async def console_confirm_close(pair: OpenPairState, config: StrategyConfig) -> bool:
    if config.approval_mode != "console" or not sys.stdin.isatty():
        return True
    prompt = (
        f"\n[strategy][confirm-close] pair_id={pair.pair_id} symbol={pair.symbol} "
        f"qty={pair.qty:.6f} long={pair.long_exchange} short={pair.short_exchange}\n"
        "close? [y/N]: "
    )
    answer = await asyncio.to_thread(input, prompt)
    return answer.strip().lower() in {"y", "yes"}


async def execute_control_plane_command(
    control_service: StrategyControlService,
    command: StrategyControlCommand,
) -> None:
    try:
        if command.command_type == "paper-order":
            result = await console_paper_order(control_service.runtime_adapter, command.payload)
        elif command.command_type == "live-limit":
            result = await console_live_limit_order(
                control_service.runtime_adapter,
                command.payload,
                ops_token=command.ops_token,
            )
        elif command.command_type == "live-market":
            result = await console_live_market_order(
                control_service.runtime_adapter,
                command.payload,
                ops_token=command.ops_token,
            )
        elif command.command_type == "cancel-symbol":
            result = await console_cancel_symbol(
                control_service.runtime_adapter,
                command.payload,
                ops_token=command.ops_token,
            )
        elif command.command_type == "flatten-symbol":
            result = await console_flatten_symbol(
                control_service.runtime_adapter,
                command.payload,
                ops_token=command.ops_token,
            )
        elif command.command_type == "rebalance-hedge":
            result = await console_rebalance_hedge(
                control_service.runtime_adapter,
                command.payload,
                ops_token=command.ops_token,
            )
        elif command.command_type == "pause-entry":
            assert_action_token(command.ops_token)
            request = command.payload
            result = {"ok": True, "entry_control": write_entry_control(True, request.reason)}
        elif command.command_type == "resume-entry":
            assert_action_token(command.ops_token)
            request = command.payload
            result = {"ok": True, "entry_control": write_entry_control(False, request.reason)}
        else:
            raise ValueError(f"unsupported control command: {command.command_type}")
    except Exception as exc:
        control_service.last_error = str(exc)
        if not command.response_future.done():
            command.response_future.set_exception(exc)
        return

    if not command.response_future.done():
        command.response_future.set_result(result)


async def drain_control_plane_commands(control_service: StrategyControlService) -> None:
    while True:
        try:
            command = control_service.command_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        await execute_control_plane_command(control_service, command)


async def handle_command(
    command: str,
    candidates: list[PairCandidate],
    open_pairs: dict[str, OpenPairState],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int],
    last_entry_order_submit_ms_by_key: dict[tuple[str, str, str], int] | None = None,
    symbol_recent_close_ms: dict[str, int] | None = None,
    entry_control_store: EntryControlStore | None = None,
    startup_block_reasons: tuple[str, ...] = (),
) -> bool:
    if not command:
        return True
    if command in {"quit", "exit"}:
        return False
    if command in {"list", "ls"}:
        print(render_candidate_table(candidates, config.max_candidates_print))
        return True
    if command in {"pairs", "pair"}:
        print(render_open_pairs(open_pairs))
        return True
    if command.startswith("show "):
        try:
            index = int(command.split()[1]) - 1
        except Exception:
            print("[strategy][show] usage: show <idx>")
            return True
        if index < 0 or index >= len(candidates):
            print("[strategy][show] invalid candidate index")
            return True
        print(render_candidate_detail(candidates[index]))
        return True
    if command.startswith("enter "):
        try:
            index = int(command.split()[1]) - 1
        except Exception:
            print("[strategy][enter] usage: enter <idx>")
            return True
        if index < 0 or index >= len(candidates):
            print("[strategy][enter] invalid candidate index")
            return True
        candidate = candidates[index]
        if not candidate.can_enter:
            print(f"[strategy][enter] blocked candidate reasons={candidate.block_reasons}")
            return True
        risk_decision = evaluate_candidate_risk_now(
            candidate,
            open_pairs,
            exchanges,
            config,
            entry_control_store,
            balance_reports_by_exchange=collect_balance_reports_by_exchange(exchanges),
            startup_block_reasons=startup_block_reasons,
            manual_locked_symbols=entry_locked_symbols(config),
        )
        if not risk_decision.allowed:
            apply_risk_decision_to_candidate(candidate, risk_decision)
            record_entry_risk_blocked(config, candidate, risk_decision)
            print(f"[strategy][enter] risk blocked candidate reasons={list(risk_decision.reasons)}")
            return True
        if not await console_confirm_entry(candidate, config):
            print("[strategy][enter] cancelled")
            return True
        if not config.enable_live:
            print(f"[strategy][paper] live disabled: {describe_live_plan(candidate, config)}")
            return True
        pair = await enter_pair_limit_then_hedge(
            candidate,
            exchanges,
            config,
            assumed_flat_until_ms=assumed_flat_until_ms,
            last_entry_order_submit_ms_by_key=last_entry_order_submit_ms_by_key,
        )
        if pair is not None:
            open_pairs[pair.pair_id] = pair
        return True
    if command.startswith("close "):
        token = command.split(maxsplit=1)[1].strip()
        pair = find_open_pair(open_pairs, token)
        if pair is None:
            print("[strategy][close] pair not found")
            return True
        if not await console_confirm_close(pair, config):
            print("[strategy][close] cancelled")
            return True
        if not config.enable_live:
            print(f"[strategy][paper] live disabled: close pair_id={pair.pair_id}")
            return True
        ok = await close_pair_limit_then_hedge(pair, exchanges, config)
        if ok and pair.status == "closed":
            register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
            mark_symbol_recently_closed(symbol_recent_close_ms, pair.symbol)
            open_pairs.pop(pair.pair_id, None)
        elif not ok:
            print(f"[strategy][close][warn] close_not_completed pair_id={pair.pair_id} status={pair.status}")
        return True
    if command in {"help", "h", "?"}:
        print("commands: list | show <idx> | enter <idx> | pairs | close <pair> | quit")
        return True
    print(f"[strategy][console] unknown command: {command}")
    return True


def collect_candidates(
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    open_pairs: dict[str, OpenPairState] | None = None,
    entry_control_store: EntryControlStore | None = None,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
    symbol_recent_close_ms: dict[str, int] | None = None,
    recent_close_block_logged_ms: dict[str, int] | None = None,
    startup_block_reasons: tuple[str, ...] = (),
    manual_locked_symbols: set[str] | None = None,
) -> list[PairCandidate]:
    candidate_build_hooks = CandidateBuildHooks(
        compute_common_executable_qty=compute_common_executable_qty,
        exchange_min_order_notional_usd=exchange_min_order_notional_usd,
        effective_position_qty=effective_position_qty,
        extract_health_can_trade=extract_health_can_trade,
        extract_health_block_reasons=extract_health_block_reasons,
        has_open_orders=has_open_orders,
        pair_entry_margin_guard_reasons=pair_entry_margin_guard_reasons,
    )
    collection_hooks = CandidateCollectionHooks(
        collect_balance_reports_by_exchange=collect_balance_reports_by_exchange,
        apply_open_pair_capacity_blocks=apply_open_pair_capacity_blocks,
        apply_recent_symbol_close_blocks=apply_recent_symbol_close_blocks,
        apply_risk_gate_blocks=apply_risk_gate_blocks,
        candidate_build_hooks=candidate_build_hooks,
    )
    return collect_pair_candidates(
        symbols,
        exchanges,
        config,
        collection_hooks,
        open_pairs,
        entry_control_store,
        assumed_flat_until_ms,
        symbol_recent_close_ms,
        recent_close_block_logged_ms,
        startup_block_reasons,
        manual_locked_symbols,
    )


def print_strategy_config(config: StrategyConfig, symbols: list[str]) -> None:
    live_mode = "LIVE" if config.enable_live else "PAPER"
    effective_funding_floor = (
        "disabled"
        if config.min_effective_funding_usd is None
        else f"{config.min_effective_funding_usd}"
    )
    if simple_stdout_enabled(config):
        log_strategy_config_summary(config=config, symbols=symbols)
        return
    print("\n[strategy][config]")
    print(f"mode={live_mode}")
    print(f"exchanges={config.exchange_names}")
    print(f"allowed_pairs={config.allowed_pairs}")
    print(f"symbols={symbols[:20]}{'...' if len(symbols) > 20 else ''}")
    print(f"order_notional_usd={config.order_notional_usd}")
    print(
        f"first_leg_timeout_sec={config.first_leg_timeout_sec} "
        f"entry_first_leg_price_mode={config.entry_first_leg_price_mode} "
        f"entry_spread_bps={config.entry_spread_bps} "
        f"entry_cancel_buffer_bps={config.entry_cancel_buffer_bps} "
        f"entry_first_leg_marketable_bps={config.entry_first_leg_marketable_bps} "
        f"hedge_market_immediate={config.hedge_market_immediate} "
        f"hedge_fast_ack={config.hedge_fast_ack} "
        f"hedge_requote_timeout_sec={config.hedge_requote_timeout_sec} "
        f"hedge_requote_count={config.hedge_requote_count} "
        f"entry_order_settle_sec={config.entry_order_settle_sec} "
        f"entry_post_fill_edge_check_enabled={config.entry_post_fill_edge_check_enabled} "
        f"entry_post_fill_min_net_edge_bps={config.entry_post_fill_min_net_edge_bps} "
        f"entry_order_cooldown_sec={config.entry_order_cooldown_sec} "
        f"symbol_recent_close_cooldown_sec={config.symbol_recent_close_cooldown_sec}"
    )
    print(
        f"approval_mode={config.approval_mode} "
        f"auto_enter_enabled={config.auto_enter_enabled} "
        f"maker_entry_engine_enabled={config.maker_entry_engine_enabled} "
        f"maker_entry_engine_max_open_orders={config.maker_entry_engine_max_open_orders} "
        f"maker_entry_engine_requote_sec={config.maker_entry_engine_requote_sec} "
        f"maker_entry_engine_replace_edge_bps={config.maker_entry_engine_replace_edge_bps} "
        f"maker_entry_engine_queue_edge_gap_bps={config.maker_entry_engine_queue_edge_gap_bps} "
        f"maker_entry_engine_transient_pause_sec={config.maker_entry_engine_transient_pause_sec} "
        f"maker_entry_engine_stale_pause_sec={config.maker_entry_engine_stale_pause_sec} "
        f"maker_entry_q4_fast_hedge={config.maker_entry_q4_fast_hedge} "
        f"hedge_fast_ack={config.hedge_fast_ack} "
        f"auto_enter_cooldown_sec={config.auto_enter_cooldown_sec} "
        f"render_interval_sec={config.render_interval_sec} "
        f"recovery_retry_cooldown_sec={config.recovery_retry_cooldown_sec} "
        f"auto_close_retry_cooldown_sec={config.auto_close_retry_cooldown_sec} "
        f"action_account_fresh_sec={config.action_account_fresh_sec} "
        f"account_imbalance_hedge_enabled={config.account_imbalance_hedge_enabled} "
        f"account_imbalance_cooldown_sec={config.account_imbalance_cooldown_sec} "
        f"graceful_shutdown={config.graceful_shutdown_enabled} "
        f"bootstrap_on_entry={config.bootstrap_on_entry} "
        f"first_leg_role={config.first_leg_role} "
        f"close_first_leg_role={config.close_first_leg_role} "
        f"close_execution_mode={config.close_execution_mode} "
        f"close_maker_timeout_sec={config.close_maker_timeout_sec} "
        f"close_maker_reasons={config.close_maker_reasons} "
        f"max_open_pairs={config.max_open_pairs} "
        f"assumed_flat_grace_sec={config.assumed_flat_grace_sec} "
        f"live_event_log_path={config.live_event_log_path}"
    )
    print(
        f"entry_control_file={config.entry_control_file} "
        f"entry_control_fail_closed={config.entry_control_fail_closed} "
        f"position_ownership_file={config.position_ownership_file} "
        f"leverage={config.leverage} "
        f"margin_mode={config.margin_mode} "
        f"startup_margin_mode_guard_enabled={config.startup_margin_mode_guard_enabled}"
    )
    print(
        f"max_gross_leverage={config.max_gross_leverage} "
        f"max_long_leverage={config.max_long_leverage} "
        f"max_short_leverage={config.max_short_leverage}"
    )
    print(
        f"rate_limit_guard_enabled={config.rate_limit_guard_enabled} "
        f"rate_limit_guard_usage_threshold={config.rate_limit_guard_usage_threshold}"
    )
    print(
        f"entry_threshold_usd={config.min_expected_net_edge_usd} "
        f"entry_threshold_bps={config.min_expected_net_edge_bps} "
        f"min_effective_funding_usd={effective_funding_floor} "
        f"require_funding_payment={config.require_funding_payment} "
        f"entry_funding_forecast_sec={config.entry_funding_forecast_sec} "
        f"hold_buffer_usd={config.hold_buffer_usd} "
        f"hold_buffer_bps={config.hold_buffer_bps} "
        f"min_hold_sec={config.min_hold_sec} "
        f"close_candidate_min_age_sec={config.close_candidate_min_age_sec} "
        f"close_spread_confirm_count={config.close_spread_confirm_count} "
        f"close_spread_confirm_sec={config.close_spread_confirm_sec} "
        f"max_hold_sec={config.max_hold_sec} "
        f"take_profit_usd={config.take_profit_usd} "
        f"take_profit_bps={config.take_profit_bps} "
        f"stop_loss_usd={config.stop_loss_usd} "
        f"stop_loss_bps={config.stop_loss_bps} "
        f"position_stale_ignore_sec={config.position_stale_ignore_sec}"
    )
    print(
        f"entry_fee_bps={config.entry_fee_bps} "
        f"close_fee_bps={config.close_fee_bps} "
        f"close_slippage_bps={config.close_slippage_bps} "
        f"normal_close_fee_gate_enabled={config.normal_close_fee_gate_enabled} "
        f"normal_close_fee_gate_min_net_usd={config.normal_close_fee_gate_min_net_usd} "
        f"normal_close_fee_gate_max_defer_sec={config.normal_close_fee_gate_max_defer_sec} "
        f"risk_buffer_bps={config.risk_buffer_bps} "
        f"stale_data_penalty_bps={config.stale_data_penalty_bps}"
    )
    print(
        f"funding_missing_confidence={config.funding_missing_confidence} "
        f"funding_alignment_min_factor={config.funding_alignment_min_factor} "
        f"funding_alignment_full_penalty_sec={config.funding_alignment_full_penalty_sec} "
        f"funding_time_decay_min_factor={config.funding_time_decay_min_factor} "
        f"funding_time_decay_full_penalty_sec={config.funding_time_decay_full_penalty_sec}"
    )


async def strategy_loop() -> None:
    config = load_strategy_config()
    install_runtime_log_filter()

    def _persistence_log(message: str, payload: Any | None = None) -> None:
        print_verbose(None, message if payload is None else f"{message} {payload}")

    runtime_persistence = build_runtime_persistence_from_env(log=_persistence_log)

    symbols = await resolve_strategy_symbols(config)
    exchanges = build_strategy_exchanges(config, symbols)
    entry_control_store = EntryControlStore(
        config.entry_control_file,
        fail_closed_on_error=config.entry_control_fail_closed,
    )

    snapshot_task: asyncio.Task[Any] | None = None
    console_task: asyncio.Task[Any] | None = None
    listener_tasks: list[asyncio.Task[Any]] = []

    command_queue: asyncio.Queue[str] = asyncio.Queue()
    control_command_queue: asyncio.Queue[StrategyControlCommand] = asyncio.Queue()
    wake_queue: asyncio.Queue[tuple[str, str, int]] = asyncio.Queue()

    open_pairs: dict[str, OpenPairState] = {}
    assumed_flat_until_ms: dict[tuple[str, str], int] = {}
    account_imbalance_next_retry_ms: dict[tuple[str, str, str, float], int] = {}
    last_entry_order_submit_ms_by_key: dict[tuple[str, str, str], int] = {}
    symbol_recent_close_ms: dict[str, int] = {}
    recent_close_block_logged_ms: dict[str, int] = {}
    shutdown_requested = asyncio.Event()
    shutdown_reason: dict[str, str] = {"reason": "normal_exit"}
    installed_signal_handlers: list[signal.Signals] = []
    maker_entry_engine = MakerEntryEngine(
        MakerEntryHooks(
            first_leg_plan=first_leg_plan,
            hedge_leg_plan=hedge_leg_plan,
            target_price=required_edge_entry_target_price,
            hedge_price_preview=marketable_limit_price_with_bps,
            common_executable_qty=compute_common_executable_qty,
            extract_position_qty=extract_position_qty,
            position_fill_delta=position_fill_delta,
            extract_order_uuid=extract_order_uuid,
            is_waiting_order_active=is_waiting_order_active,
            price_changed=entry_limit_price_changed,
            cancel_if_open=cancel_if_open,
            bootstrap_candidate=bootstrap_candidate_exchanges,
            complete_after_first_fill=complete_entry_after_maker_first_fill,
            record_event=lambda event_type, **payload: record_live_event(config, event_type, **payload),
        )
    )
    control_service = StrategyControlService(
        config=config,
        exchanges=exchanges,
        symbols=symbols,
        open_pairs=open_pairs,
        entry_control_store=entry_control_store,
        assumed_flat_until_ms=assumed_flat_until_ms,
        symbol_recent_close_ms=symbol_recent_close_ms,
        wake_queue=wake_queue,
        command_queue=control_command_queue,
    )
    control_server = EmbeddedControlServer(control_service)

    last_signature: tuple[Any, ...] | None = None
    last_render_ts = 0.0
    last_recompute_ts = 0.0
    last_auto_enter_ts = 0.0
    startup_block_reasons: tuple[str, ...] = ()

    try:
        installed_signal_handlers = install_shutdown_signal_handlers(
            shutdown_requested,
            shutdown_reason,
            wake_queue,
        )

        await start_strategy_exchanges(exchanges)
        await control_server.start()
        startup_block_reasons = await check_startup_margin_mode_guard(
            exchanges=exchanges,
            symbols=symbols,
            margin_mode=config.margin_mode,
            enabled=config.startup_margin_mode_guard_enabled,
            record_event=lambda event_type, **payload: record_live_event(config, event_type, **payload),
        )
        control_service.set_startup_block_reasons(startup_block_reasons)
        startup_bootstrap_failures = await bootstrap_strategy_symbols_once(exchanges, symbols, config)
        log_startup_bootstrap_summary(
            exchange_count=len(exchanges),
            symbol_count=len(symbols),
            leverage=config.leverage,
            failures=startup_bootstrap_failures,
        )

        if runtime_persistence.enabled:
            snapshot_task = asyncio.create_task(
                runtime_persistence.run_snapshot_loop(
                    exchanges,
                    symbols,
                ),
                name="strategy-snapshot-loop",
            )

        print_strategy_config(config, symbols)
        record_live_event(
            config,
            "runtime_started",
            mode="LIVE" if config.enable_live else "PAPER",
            exchanges=config.exchange_names,
            allowed_pairs=config.allowed_pairs,
            symbols=symbols,
            order_notional_usd=config.order_notional_usd,
        )

        console_task = asyncio.create_task(
            console_command_reader(command_queue),
            name="strategy-console",
        )
        listener_tasks = await attach_strategy_listeners(exchanges, symbols, wake_queue)

        while not shutdown_requested.is_set():
            now = time.monotonic()
            wait_timeout = max(config.scan_interval_sec, 0.05)

            if (now - last_recompute_ts) < config.min_recompute_interval_sec:
                wait_timeout = min(
                    wait_timeout,
                    config.min_recompute_interval_sec - (now - last_recompute_ts),
                )

            try:
                await asyncio.wait_for(wake_queue.get(), timeout=wait_timeout)
                while True:
                    try:
                        wake_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
            except TimeoutError:
                pass

            if shutdown_requested.is_set():
                break

            await drain_control_plane_commands(control_service)
            if shutdown_requested.is_set():
                break

            prune_assumed_flat(assumed_flat_until_ms)
            manual_locked_symbols = entry_locked_symbols(config)

            candidates = collect_candidates(
                symbols,
                exchanges,
                config,
                open_pairs=open_pairs,
                entry_control_store=entry_control_store,
                assumed_flat_until_ms=assumed_flat_until_ms,
                symbol_recent_close_ms=symbol_recent_close_ms,
                recent_close_block_logged_ms=recent_close_block_logged_ms,
                startup_block_reasons=startup_block_reasons,
                manual_locked_symbols=manual_locked_symbols,
            )

            last_recompute_ts = time.monotonic()
            signature = build_candidate_signature(
                candidates,
                top_n=min(5, config.max_candidates_print),
            )

            now = time.monotonic()
            if signature != last_signature:
                last_signature = signature

            if (now - last_render_ts) >= config.render_interval_sec:
                record_live_event(
                    config,
                    "candidate_scan_summary",
                    **build_candidate_scan_summary(candidates),
                )
                if not simple_stdout_enabled(config):
                    print(
                        render_candidate_table(
                            candidates,
                            config.max_candidates_print,
                            show_command_hint=not (config.enable_live and config.auto_enter_enabled),
                        )
                )
                last_render_ts = now

            if shutdown_requested.is_set():
                break

            if not shutdown_requested.is_set() and config.enable_live and config.auto_enter_enabled:
                maker_pairs = await maker_entry_engine.step(
                    candidates=candidates,
                    open_pairs=open_pairs,
                    exchanges=exchanges,
                    config=config,
                    symbols=symbols,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                    manual_locked_symbols=manual_locked_symbols,
                )
                for pair in maker_pairs:
                    open_pairs[pair.pair_id] = pair

            if not shutdown_requested.is_set():
                recovery_skip_symbols = set(maker_entry_engine.active_symbols()) | set(manual_locked_symbols)
                await recover_account_imbalances(
                    symbols=symbols,
                    exchanges=exchanges,
                    config=config,
                    open_pairs=open_pairs,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                    next_retry_ms_by_key=account_imbalance_next_retry_ms,
                    skip_symbols=recovery_skip_symbols,
                )
                reconcile_orphan_markers(exchanges, config)

            if not shutdown_requested.is_set() and config.enable_live and config.auto_enter_enabled:
                active_open_pairs = [
                    pair
                    for pair in open_pairs.values()
                    if pair.status not in {"closed", "failed"}
                ]
                available_slots = (
                    len(candidates)
                    if config.max_open_pairs <= 0
                    else max(config.max_open_pairs - len(active_open_pairs), 0)
                )

                now = time.monotonic()
                if available_slots > 0 and (now - last_auto_enter_ts) >= config.auto_enter_cooldown_sec:
                    active_symbols = {pair.symbol for pair in active_open_pairs}
                    selected_candidates: list[PairCandidate] = []
                    selected_symbols: set[str] = set()

                    for candidate in candidates:
                        if not candidate.can_enter:
                            continue
                        if maker_entry_engine.is_managed_symbol(candidate.symbol, config, symbols):
                            continue
                        if candidate.symbol in active_symbols or candidate.symbol in selected_symbols:
                            continue
                        selected_candidates.append(candidate)
                        selected_symbols.add(candidate.symbol)
                        if len(selected_candidates) >= available_slots:
                            break

                    if selected_candidates:
                        last_auto_enter_ts = now

                    for candidate in selected_candidates:
                        if shutdown_requested.is_set():
                            break
                        risk_decision = evaluate_candidate_risk_now(
                            candidate,
                            open_pairs,
                            exchanges,
                            config,
                            entry_control_store,
                            balance_reports_by_exchange=collect_balance_reports_by_exchange(exchanges),
                            startup_block_reasons=startup_block_reasons,
                            manual_locked_symbols=manual_locked_symbols,
                        )
                        if not risk_decision.allowed:
                            apply_risk_decision_to_candidate(candidate, risk_decision)
                            record_entry_risk_blocked(config, candidate, risk_decision)
                            log_entry_blocked(symbol=candidate.symbol, reasons=list(risk_decision.reasons))
                            continue
                        log_entry_attempt(
                            symbol=candidate.symbol,
                            long_exchange=candidate.long_exchange,
                            short_exchange=candidate.short_exchange,
                            expected_net_edge_usd=candidate.expected_net_edge_usd,
                            expected_net_edge_bps=candidate.expected_net_edge_bps,
                        )

                        pair = await enter_pair_limit_then_hedge(
                            candidate,
                            exchanges,
                            config,
                            assumed_flat_until_ms=assumed_flat_until_ms,
                            last_entry_order_submit_ms_by_key=last_entry_order_submit_ms_by_key,
                        )
                        if pair is not None:
                            open_pairs[pair.pair_id] = pair

            while True:
                try:
                    command = command_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                should_continue = await handle_command(
                    command,
                    candidates,
                    open_pairs,
                    exchanges,
                    config,
                    assumed_flat_until_ms,
                    last_entry_order_submit_ms_by_key,
                    symbol_recent_close_ms,
                    entry_control_store,
                    startup_block_reasons,
                )
                if not should_continue:
                    shutdown_reason["reason"] = "console_command"
                    shutdown_requested.set()
                    break

            if shutdown_requested.is_set():
                break

            await manage_open_pairs(
                open_pairs,
                exchanges,
                config,
                assumed_flat_until_ms,
                symbol_recent_close_ms,
            )

    finally:
        try:
            await graceful_shutdown_flatten(
                reason=shutdown_reason.get("reason") or "normal_exit",
                symbols=symbols,
                exchanges=exchanges,
                open_pairs=open_pairs,
                config=config,
                assumed_flat_until_ms=assumed_flat_until_ms,
                manual_locked_symbols=active_manual_symbols(config.position_ownership_file),
            )
        except Exception as exc:
            print(f"[종료][정리오류] 오류={exc}")
            record_live_event(
                config,
                "shutdown_cleanup_error",
                reason=shutdown_reason.get("reason") or "normal_exit",
                error=str(exc),
                open_pair_ids=sorted(open_pairs.keys()),
            )
        finally:
            remove_shutdown_signal_handlers(installed_signal_handlers)

        record_live_event(
            config,
            "runtime_stopped",
            open_pair_ids=sorted(open_pairs.keys()),
            reason=shutdown_reason.get("reason") or "normal_exit",
        )
        flush_api_pressure(force=True)

        if snapshot_task is not None:
            snapshot_task.cancel()
            try:
                await snapshot_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                print(f"[snapshot-loop][stop][error] {exc}")

        if console_task is not None:
            console_task.cancel()
            try:
                await console_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                print(f"[strategy][console-stop][error] {exc}")

        for task in listener_tasks:
            task.cancel()

        for task in listener_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                print(f"[strategy][listener-stop][error] {exc}")

        await control_server.stop()
        await stop_strategy_exchanges(exchanges)

async def main() -> None:
    await strategy_loop()


if __name__ == "__main__":
    asyncio.run(main())
