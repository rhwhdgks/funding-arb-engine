import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from src.ops.persistence.live_snapshot import persist_exchange_snapshots
from src.core.exchange_registry import SUPPORTED_EXCHANGES, build_exchange

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
SUPPORTED_SYMBOLS_CACHE_DIR = PROJECT_ROOT / "logs" / "supported_symbols_cache"
EPS = 1e-9
DEFAULT_ALLOWED_PAIRS = [("binance", "okx")]
DEFAULT_ENTRY_FEE_BPS = {
    "binance": 2.0,
    "okx": 2.0,
    "gateio": 8.0,
}
DEFAULT_CLOSE_FEE_BPS = {
    "binance": 2.0,
    "okx": 2.0,
    "gateio": 8.0,
}


@dataclass
class StrategyConfig:
    exchange_names: list[str]
    symbol_input: str
    allowed_pairs: list[tuple[str, str]]
    leverage: int = 1
    margin_mode: str = "cross"
    order_notional_usd: float = 100.0
    scan_interval_sec: float = 1.0
    first_leg_timeout_sec: float = 2.0
    hedge_requote_timeout_sec: float = 0.7
    hedge_requote_count: int = 2
    close_first_leg_role: str = "short"
    close_aggressive_bps: float = 8.0
    emergency_flatten_bps: float = 20.0
    action_settle_wait_sec: float = 0.6
    min_recompute_interval_sec: float = 0.2
    min_expected_net_edge_usd: float = 0.0
    min_expected_net_edge_bps: float = 0.0
    min_effective_funding_usd: float | None = None
    hold_buffer_usd: float = 0.0
    min_hold_sec: float = 300.0
    close_candidate_min_age_sec: float = 1800.0
    max_hold_sec: float = 14400.0
    take_profit_usd: float = 0.10
    stop_loss_usd: float = -0.25
    position_stale_ignore_sec: float = 20.0
    close_slippage_bps: float = 0.0
    risk_buffer_bps: float = 1.0
    stale_data_penalty_bps: float = 0.5
    funding_missing_confidence: float = 0.35
    funding_alignment_min_factor: float = 0.25
    funding_alignment_full_penalty_sec: float = 14400.0
    funding_time_decay_min_factor: float = 0.35
    funding_time_decay_full_penalty_sec: float = 28800.0
    approval_mode: str = "console"
    enable_live: bool = False
    auto_enter_enabled: bool = False
    auto_close_enabled: bool = False
    bootstrap_on_entry: bool = True
    max_candidates_print: int = 10
    max_open_pairs: int = 1
    assumed_flat_grace_sec: float = 8.0
    auto_enter_cooldown_sec: float = 15.0
    render_interval_sec: float = 30.0
    recovery_retry_cooldown_sec: float = 15.0
    auto_close_retry_cooldown_sec: float = 15.0
    danger_log_cooldown_sec: float = 10.0
    action_account_fresh_sec: float = 2.5
    live_event_log_path: Path = field(default_factory=lambda: PROJECT_ROOT / "logs" / "strategy_live.jsonl")
    entry_fee_bps: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_ENTRY_FEE_BPS))
    close_fee_bps: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_CLOSE_FEE_BPS))
    first_leg_role: str = "long"


@dataclass
class PairCandidate:
    symbol: str
    long_exchange: str
    short_exchange: str
    long_ask: float
    short_bid: float
    mid_reference: float
    target_qty: float
    executable_qty: float
    notional_usd: float
    long_min_order_notional_usd: float
    short_min_order_notional_usd: float
    entry_open_edge_usd: float
    entry_open_edge_bps: float
    open_fee_cost_usd: float
    raw_funding_pnl_usd: float
    effective_funding_pnl_usd: float
    funding_confidence: float
    funding_alignment_factor: float
    funding_time_decay_factor: float
    expected_close_cost_usd: float
    risk_buffer_usd: float
    stale_data_penalty_usd: float
    expected_net_edge_usd: float
    expected_net_edge_bps: float
    funding_long_rate: float
    funding_short_rate: float
    long_next_funding_ms: int
    short_next_funding_ms: int
    can_enter: bool
    block_reasons: list[str] = field(default_factory=list)

    @property
    def id(self) -> str:
        return f"{self.symbol}:{self.long_exchange}:long:{self.short_exchange}:short"


@dataclass
class OpenPairState:
    pair_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    qty: float
    requested_qty: float
    long_entry_price: float
    short_entry_price: float
    entry_time_ms: int
    entry_net_edge_usd: float
    funding_long_rate: float
    funding_short_rate: float
    status: str = "open"
    assumed_flat_exchanges: list[str] = field(default_factory=list)
    last_hold_decision: str = ""
    last_hold_pnl_usd: float = 0.0
    last_close_pnl_usd: float = 0.0
    last_close_edge_usd: float = 0.0
    last_close_reason: str = ""
    last_direction_mismatch_log_ms: int = 0
    next_recovery_retry_ms: int = 0
    next_auto_close_retry_ms: int = 0

def _env_flag(key: str, default: str = "0") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_snapshot_config() -> dict[str, Any]:
    interval_sec = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "120").strip())
    funding_limit = int(os.getenv("SNAPSHOT_FUNDING_LIMIT", "30").strip())

    return {
        "enabled": _env_flag("ENABLE_DB_SNAPSHOTS", "1"),
        "interval_sec": max(interval_sec, 10.0),
        "funding_limit": max(funding_limit, 1),
        "persist_funding": _env_flag("SNAPSHOT_PERSIST_FUNDING", "1"),
        "persist_balance": _env_flag("SNAPSHOT_PERSIST_BALANCE", "1"),
        "persist_positions": _env_flag("SNAPSHOT_PERSIST_POSITIONS", "1"),
    }


async def run_snapshot_loop(
    exchanges: dict[str, Any],
    symbols: list[str],
    *,
    config: dict[str, Any],
) -> None:
    if not config.get("enabled", False):
        print("[snapshot-loop] disabled")
        return

    interval_sec = float(config["interval_sec"])
    funding_limit = int(config["funding_limit"])
    persist_funding = bool(config["persist_funding"])
    persist_balance = bool(config["persist_balance"])
    persist_positions = bool(config["persist_positions"])

    print(
        "[snapshot-loop] start",
        {
            "interval_sec": interval_sec,
            "funding_limit": funding_limit,
            "persist_funding": persist_funding,
            "persist_balance": persist_balance,
            "persist_positions": persist_positions,
            "symbols": symbols,
            "exchanges": sorted(exchanges.keys()),
        },
    )

    while True:
        started_ms = int(time.time() * 1000)
        for exchange_name, exchange in exchanges.items():
            try:
                summary = await persist_exchange_snapshots(
                    exchange_name=exchange_name,
                    exchange=exchange,
                    symbols=symbols,
                    funding_limit=funding_limit,
                    persist_funding=persist_funding,
                    persist_balance=persist_balance,
                    persist_positions=persist_positions,
                )
                print(f"[snapshot-loop][{exchange_name}] {summary}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[snapshot-loop][{exchange_name}][error] {exc}")

        elapsed_sec = max((int(time.time() * 1000) - started_ms) / 1000.0, 0.0)
        sleep_sec = max(interval_sec - elapsed_sec, 1.0)
        await asyncio.sleep(sleep_sec)

def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def normalize_exchange_name(name: str) -> str:
    normalized = str(name).strip().lower()
    if normalized not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {name}")
    return normalized


def normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def now_ms() -> int:
    return int(time.time() * 1000)


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


def parse_allowed_pairs(value: str | None, exchange_names: list[str]) -> list[tuple[str, str]]:
    if not value:
        pairs = list(DEFAULT_ALLOWED_PAIRS)
    else:
        pairs = []
        for raw in parse_csv(value):
            token = raw.replace("/", "-").replace(":", "-").lower()
            if "-" not in token:
                continue
            left, right = token.split("-", 1)
            left = normalize_exchange_name(left)
            right = normalize_exchange_name(right)
            if left == right:
                continue
            key = tuple(sorted((left, right)))
            if key not in pairs:
                pairs.append(key)

    filtered: list[tuple[str, str]] = []
    selected = set(exchange_names)
    for left, right in pairs:
        if left in selected and right in selected:
            filtered.append((left, right))
    return filtered


def load_fee_bps_map(
    prefix: str,
    defaults: dict[str, float],
    fallback_prefixes: tuple[str, ...] = (),
) -> dict[str, float]:
    fee_bps = dict(defaults)
    for exchange_name in SUPPORTED_EXCHANGES:
        value = None
        for env_prefix in (prefix, *fallback_prefixes):
            env_key = f"{env_prefix}_{exchange_name.upper()}"
            if os.getenv(env_key):
                value = float(os.getenv(env_key, "0") or 0.0)
                break
        if value is not None:
            fee_bps[exchange_name] = value
    return fee_bps


def load_strategy_config() -> StrategyConfig:
    exchange_names = [normalize_exchange_name(name) for name in parse_csv(os.getenv("STRATEGY_EXCHANGES", "binance,okx"))]
    if len(exchange_names) < 2:
        raise ValueError("STRATEGY_EXCHANGES must include at least two exchanges")

    symbol_input = os.getenv("STRATEGY_SYMBOLS", "ALL").strip()
    raw_min_effective_funding_usd = os.getenv("STRATEGY_MIN_EFFECTIVE_FUNDING_USD")
    entry_fee_bps = load_fee_bps_map(
        "STRATEGY_ENTRY_FEE_BPS",
        DEFAULT_ENTRY_FEE_BPS,
        fallback_prefixes=("STRATEGY_MAKER_FEE_BPS",),
    )
    close_fee_bps = load_fee_bps_map(
        "STRATEGY_CLOSE_FEE_BPS",
        DEFAULT_CLOSE_FEE_BPS,
        fallback_prefixes=("STRATEGY_ENTRY_FEE_BPS", "STRATEGY_MAKER_FEE_BPS"),
    )

    config = StrategyConfig(
        exchange_names=exchange_names,
        symbol_input=symbol_input,
        allowed_pairs=parse_allowed_pairs(os.getenv("STRATEGY_ALLOWED_PAIRS"), exchange_names),
        leverage=int(os.getenv("STRATEGY_LEVERAGE", "1") or 1),
        margin_mode=os.getenv("STRATEGY_MARGIN_MODE", "cross").strip().lower(),
        order_notional_usd=float(os.getenv("STRATEGY_ORDER_NOTIONAL_USD", "100") or 100.0),
        scan_interval_sec=float(os.getenv("STRATEGY_SCAN_INTERVAL_SEC", "1.0") or 1.0),
        first_leg_timeout_sec=float(os.getenv("STRATEGY_FIRST_LEG_TIMEOUT_SEC", "2.0") or 2.0),
        hedge_requote_timeout_sec=float(os.getenv("STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC", "0.7") or 0.7),
        hedge_requote_count=int(os.getenv("STRATEGY_HEDGE_REQUOTE_COUNT", "2") or 2),
        close_first_leg_role=os.getenv("STRATEGY_CLOSE_FIRST_LEG_ROLE", "short").strip().lower(),
        close_aggressive_bps=float(os.getenv("STRATEGY_CLOSE_AGGRESSIVE_BPS", "8.0") or 8.0),
        emergency_flatten_bps=float(os.getenv("STRATEGY_EMERGENCY_FLATTEN_BPS", "20.0") or 20.0),
        action_settle_wait_sec=float(os.getenv("STRATEGY_ACTION_SETTLE_WAIT_SEC", "0.6") or 0.6),
        min_recompute_interval_sec=float(os.getenv("STRATEGY_MIN_RECOMPUTE_INTERVAL_SEC", "0.2") or 0.2),
        min_expected_net_edge_usd=float(os.getenv("STRATEGY_MIN_EXPECTED_NET_EDGE_USD", "0.0") or 0.0),
        min_expected_net_edge_bps=float(os.getenv("STRATEGY_MIN_EXPECTED_NET_EDGE_BPS", "0.0") or 0.0),
        min_effective_funding_usd=(
            float(raw_min_effective_funding_usd or 0.0)
            if raw_min_effective_funding_usd not in (None, "")
            else None
        ),
        hold_buffer_usd=float(os.getenv("STRATEGY_HOLD_BUFFER_USD", "0.0") or 0.0),
        min_hold_sec=float(os.getenv("STRATEGY_MIN_HOLD_SEC", "300.0") or 300.0),
        close_candidate_min_age_sec=float(
            os.getenv("STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC", "1800.0") or 1800.0
        ),
        max_hold_sec=float(os.getenv("STRATEGY_MAX_HOLD_SEC", "14400.0") or 14400.0),
        take_profit_usd=float(os.getenv("STRATEGY_TAKE_PROFIT_USD", "0.10") or 0.10),
        stop_loss_usd=float(os.getenv("STRATEGY_STOP_LOSS_USD", "-0.25") or -0.25),
        position_stale_ignore_sec=float(os.getenv("STRATEGY_POSITION_STALE_IGNORE_SEC", "20.0") or 20.0),
        close_slippage_bps=float(os.getenv("STRATEGY_CLOSE_SLIPPAGE_BPS", "0.0") or 0.0),
        risk_buffer_bps=float(os.getenv("STRATEGY_RISK_BUFFER_BPS", "1.0") or 1.0),
        stale_data_penalty_bps=float(os.getenv("STRATEGY_STALE_DATA_PENALTY_BPS", "0.5") or 0.5),
        funding_missing_confidence=float(os.getenv("STRATEGY_FUNDING_MISSING_CONFIDENCE", "0.35") or 0.35),
        funding_alignment_min_factor=float(os.getenv("STRATEGY_FUNDING_ALIGNMENT_MIN_FACTOR", "0.25") or 0.25),
        funding_alignment_full_penalty_sec=float(os.getenv("STRATEGY_FUNDING_ALIGNMENT_FULL_PENALTY_SEC", "14400") or 14400.0),
        funding_time_decay_min_factor=float(os.getenv("STRATEGY_FUNDING_TIME_DECAY_MIN_FACTOR", "0.35") or 0.35),
        funding_time_decay_full_penalty_sec=float(os.getenv("STRATEGY_FUNDING_TIME_DECAY_FULL_PENALTY_SEC", "28800") or 28800.0),
        approval_mode=os.getenv("STRATEGY_APPROVAL_MODE", "console").strip().lower(),
        enable_live=parse_bool(os.getenv("STRATEGY_ENABLE_LIVE"), False),
        auto_enter_enabled=parse_bool(os.getenv("STRATEGY_AUTO_ENTER_ENABLED"), False),
        auto_close_enabled=parse_bool(os.getenv("STRATEGY_AUTO_CLOSE_ENABLED"), False),
        bootstrap_on_entry=parse_bool(os.getenv("STRATEGY_BOOTSTRAP_ON_ENTRY"), True),
        max_candidates_print=int(os.getenv("STRATEGY_MAX_CANDIDATES_PRINT", "10") or 10),
        max_open_pairs=int(os.getenv("STRATEGY_MAX_OPEN_PAIRS", "1") or 1),
        assumed_flat_grace_sec=float(os.getenv("STRATEGY_ASSUMED_FLAT_GRACE_SEC", "8.0") or 8.0),
        auto_enter_cooldown_sec=float(os.getenv("STRATEGY_AUTO_ENTER_COOLDOWN_SEC", "15.0") or 15.0),
        render_interval_sec=float(os.getenv("STRATEGY_RENDER_INTERVAL_SEC", "30.0") or 30.0),
        recovery_retry_cooldown_sec=float(os.getenv("STRATEGY_RECOVERY_RETRY_COOLDOWN_SEC", "15.0") or 15.0),
        auto_close_retry_cooldown_sec=float(os.getenv("STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC", "15.0") or 15.0),
        danger_log_cooldown_sec=float(os.getenv("STRATEGY_DANGER_LOG_COOLDOWN_SEC", "10.0") or 10.0),
        action_account_fresh_sec=float(os.getenv("STRATEGY_ACTION_ACCOUNT_FRESH_SEC", "2.5") or 2.5),
        live_event_log_path=Path(
            os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH", str(PROJECT_ROOT / "logs" / "strategy_live.jsonl"))
        ),
        entry_fee_bps=entry_fee_bps,
        close_fee_bps=close_fee_bps,
        first_leg_role=os.getenv("STRATEGY_FIRST_LEG_ROLE", "long").strip().lower(),
    )
    if not config.allowed_pairs:
        raise ValueError("no allowed pairs remain after filtering STRATEGY_ALLOWED_PAIRS")
    if config.first_leg_role not in {"long", "short"}:
        raise ValueError("STRATEGY_FIRST_LEG_ROLE must be 'long' or 'short'")
    if config.close_first_leg_role not in {"long", "short"}:
        raise ValueError("STRATEGY_CLOSE_FIRST_LEG_ROLE must be 'long' or 'short'")
    return config


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
        print(f"[strategy][run] exchange={exchange_name}")


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


def extract_best_bid(snapshot: dict[str, Any]) -> float:
    orderbook = snapshot.get("orderbook", {}) or {}
    tick = snapshot.get("tick", {}) or {}
    for source in (orderbook, tick):
        for key in ("best_bid", "bid", "bid_price"):
            value = _safe_float(source.get(key), 0.0)
            if value > 0:
                return value
    return 0.0


def extract_best_ask(snapshot: dict[str, Any]) -> float:
    orderbook = snapshot.get("orderbook", {}) or {}
    tick = snapshot.get("tick", {}) or {}
    for source in (orderbook, tick):
        for key in ("best_ask", "ask", "ask_price"):
            value = _safe_float(source.get(key), 0.0)
            if value > 0:
                return value
    return 0.0


def extract_mid_price(snapshot: dict[str, Any]) -> float:
    bid = extract_best_bid(snapshot)
    ask = extract_best_ask(snapshot)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    tick = snapshot.get("tick", {}) or {}
    for key in ("last", "last_price", "price"):
        value = _safe_float(tick.get(key), 0.0)
        if value > 0:
            return value
    position = snapshot.get("position", {}) or {}
    for key in ("mark_price", "avg_price", "entry_price"):
        value = _safe_float(position.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def extract_funding_rate(snapshot: dict[str, Any]) -> float:
    funding = snapshot.get("funding", {}) or {}
    for key in ("funding_rate", "fundingRate", "rate"):
        value = funding.get(key)
        if value is not None:
            return _safe_float(value, 0.0)
    return 0.0


def extract_next_funding_ms(snapshot: dict[str, Any]) -> int:
    funding = snapshot.get("funding", {}) or {}
    for key in ("funding_next_apply", "next_funding_time", "nextFundingTime"):
        value = funding.get(key)
        if value is not None:
            try:
                return int(float(value))
            except Exception:
                return 0
    return 0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def seconds_until(timestamp_ms: int, now_ms: int | None = None) -> float:
    if timestamp_ms <= 0:
        return 0.0
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return max((timestamp_ms - now_ms) / 1000.0, 0.0)


def compute_funding_confidence(
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    config: StrategyConfig,
) -> float:
    if long_next_funding_ms <= 0 or short_next_funding_ms <= 0:
        return clamp(config.funding_missing_confidence, 0.0, 1.0)
    return 1.0


def compute_funding_alignment_factor(
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    config: StrategyConfig,
) -> float:
    min_factor = clamp(config.funding_alignment_min_factor, 0.0, 1.0)
    penalty_window_sec = max(config.funding_alignment_full_penalty_sec, 1.0)
    if long_next_funding_ms <= 0 or short_next_funding_ms <= 0:
        return min_factor
    gap_sec = abs(long_next_funding_ms - short_next_funding_ms) / 1000.0
    penalty_ratio = clamp(gap_sec / penalty_window_sec, 0.0, 1.0)
    return max(min_factor, 1.0 - penalty_ratio * (1.0 - min_factor))


def compute_funding_time_decay_factor(
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    config: StrategyConfig,
    now_ms: int | None = None,
) -> float:
    min_factor = clamp(config.funding_time_decay_min_factor, 0.0, 1.0)
    penalty_window_sec = max(config.funding_time_decay_full_penalty_sec, 1.0)
    if long_next_funding_ms <= 0 or short_next_funding_ms <= 0:
        return min_factor
    farthest_event_sec = max(
        seconds_until(long_next_funding_ms, now_ms=now_ms),
        seconds_until(short_next_funding_ms, now_ms=now_ms),
    )
    penalty_ratio = clamp(farthest_event_sec / penalty_window_sec, 0.0, 1.0)
    return max(min_factor, 1.0 - penalty_ratio * (1.0 - min_factor))


def compute_effective_funding_components(
    *,
    long_rate: float,
    short_rate: float,
    long_notional: float,
    short_notional: float,
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    config: StrategyConfig,
    now_ms: int | None = None,
) -> tuple[float, float, float, float, float]:
    raw_funding_pnl_usd = (-long_rate * long_notional) + (short_rate * short_notional)
    funding_confidence = compute_funding_confidence(long_next_funding_ms, short_next_funding_ms, config)
    funding_alignment_factor = compute_funding_alignment_factor(long_next_funding_ms, short_next_funding_ms, config)
    funding_time_decay_factor = compute_funding_time_decay_factor(
        long_next_funding_ms,
        short_next_funding_ms,
        config,
        now_ms=now_ms,
    )
    effective_funding_pnl_usd = (
        raw_funding_pnl_usd
        * funding_confidence
        * funding_alignment_factor
        * funding_time_decay_factor
    )
    return (
        raw_funding_pnl_usd,
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        effective_funding_pnl_usd,
    )


def extract_position_qty(snapshot: dict[str, Any]) -> float:
    position = snapshot.get("position", {}) or {}
    for key in ("qty", "position_amt", "size"):
        if key in position:
            return _safe_float(position.get(key), 0.0)
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
    exchange_name: str,
    symbol: str,
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> float:
    if is_assumed_flat(assumed_flat_until_ms, exchange_name, symbol):
        return 0.0
    qty = extract_position_qty(snapshot)
    if abs(qty) <= EPS:
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


def compute_open_cost_usd(
    long_exchange: str,
    short_exchange: str,
    long_notional: float,
    short_notional: float,
    config: StrategyConfig,
) -> float:
    return (
        long_notional * config.entry_fee_bps.get(long_exchange, 0.0) / 10000.0
        + short_notional * config.entry_fee_bps.get(short_exchange, 0.0) / 10000.0
    )


def compute_expected_close_cost_usd(
    long_exchange: str,
    short_exchange: str,
    reference_notional: float,
    config: StrategyConfig,
) -> float:
    close_fee_cost_usd = (
        reference_notional * config.close_fee_bps.get(long_exchange, 0.0) / 10000.0
        + reference_notional * config.close_fee_bps.get(short_exchange, 0.0) / 10000.0
    )
    close_slippage_cost_usd = reference_notional * (config.close_slippage_bps * 2.0) / 10000.0
    return close_fee_cost_usd + close_slippage_cost_usd


def compute_stale_data_penalty_usd(
    reference_notional: float,
    funding_confidence: float,
    funding_alignment_factor: float,
    funding_time_decay_factor: float,
    config: StrategyConfig,
) -> float:
    quality = clamp(
        funding_confidence * funding_alignment_factor * funding_time_decay_factor,
        0.0,
        1.0,
    )
    return reference_notional * config.stale_data_penalty_bps / 10000.0 * (1.0 - quality)


def compute_pair_candidate(
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    snapshots_by_exchange: dict[str, dict[str, Any]],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> PairCandidate:
    long_snapshot = snapshots_by_exchange[long_exchange][symbol]
    short_snapshot = snapshots_by_exchange[short_exchange][symbol]
    long_exchange_obj = exchanges[long_exchange]
    short_exchange_obj = exchanges[short_exchange]

    long_ask = extract_best_ask(long_snapshot)
    short_bid = extract_best_bid(short_snapshot)
    mid_reference = (long_ask + short_bid) / 2.0 if long_ask > 0 and short_bid > 0 else max(long_ask, short_bid, 0.0)
    target_qty = (config.order_notional_usd / mid_reference) if mid_reference > 0 else 0.0
    executable_qty = compute_common_executable_qty(
        long_exchange_obj,
        short_exchange_obj,
        symbol,
        target_qty,
        left_price=long_ask,
        right_price=short_bid,
    )
    notional_long = executable_qty * long_ask
    notional_short = executable_qty * short_bid
    reference_notional = max((notional_long + notional_short) / 2.0, 0.0)
    long_min_order_notional_usd = exchange_min_order_notional_usd(long_exchange_obj, symbol, long_ask)
    short_min_order_notional_usd = exchange_min_order_notional_usd(short_exchange_obj, symbol, short_bid)

    funding_long_rate = extract_funding_rate(long_snapshot)
    funding_short_rate = extract_funding_rate(short_snapshot)
    long_next_funding_ms = extract_next_funding_ms(long_snapshot)
    short_next_funding_ms = extract_next_funding_ms(short_snapshot)
    now_ms = int(time.time() * 1000)

    entry_unit_edge = short_bid - long_ask
    entry_open_edge_usd = executable_qty * entry_unit_edge
    entry_open_edge_bps = (entry_unit_edge / mid_reference * 10000.0) if mid_reference > 0 else 0.0

    open_fee_cost_usd = compute_open_cost_usd(
        long_exchange,
        short_exchange,
        notional_long,
        notional_short,
        config,
    )
    expected_close_cost_usd = compute_expected_close_cost_usd(
        long_exchange,
        short_exchange,
        reference_notional,
        config,
    )
    risk_buffer_usd = reference_notional * config.risk_buffer_bps / 10000.0
    (
        raw_funding_pnl_usd,
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        effective_funding_pnl_usd,
    ) = compute_effective_funding_components(
        long_rate=funding_long_rate,
        short_rate=funding_short_rate,
        long_notional=notional_long,
        short_notional=notional_short,
        long_next_funding_ms=long_next_funding_ms,
        short_next_funding_ms=short_next_funding_ms,
        config=config,
        now_ms=now_ms,
    )
    stale_data_penalty_usd = compute_stale_data_penalty_usd(
        reference_notional,
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        config,
    )

    expected_net_edge_usd = (
        entry_open_edge_usd
        + effective_funding_pnl_usd
        - open_fee_cost_usd
        - expected_close_cost_usd
        - risk_buffer_usd
        - stale_data_penalty_usd
    )
    expected_net_edge_bps = (expected_net_edge_usd / reference_notional * 10000.0) if reference_notional > 0 else 0.0

    block_reasons: list[str] = []
    if long_ask <= 0:
        block_reasons.append(f"{long_exchange}:ask_missing")
    if short_bid <= 0:
        block_reasons.append(f"{short_exchange}:bid_missing")
    if not extract_health_can_trade(long_snapshot):
        reasons = extract_health_block_reasons(long_snapshot) or ["cannot_trade"]
        block_reasons.append(f"{long_exchange}:{'|'.join(reasons)}")
    if not extract_health_can_trade(short_snapshot):
        reasons = extract_health_block_reasons(short_snapshot) or ["cannot_trade"]
        block_reasons.append(f"{short_exchange}:{'|'.join(reasons)}")
    if abs(effective_position_qty(long_snapshot, long_exchange, symbol, config, assumed_flat_until_ms)) > EPS:
        block_reasons.append(f"{long_exchange}:existing_position")
    if abs(effective_position_qty(short_snapshot, short_exchange, symbol, config, assumed_flat_until_ms)) > EPS:
        block_reasons.append(f"{short_exchange}:existing_position")
    if has_open_orders(long_snapshot):
        block_reasons.append(f"{long_exchange}:open_orders_exist")
    if has_open_orders(short_snapshot):
        block_reasons.append(f"{short_exchange}:open_orders_exist")
    if target_qty <= EPS:
        block_reasons.append("target_qty_zero")
    if executable_qty <= EPS:
        block_reasons.append("common_executable_qty_zero")
    if (
        config.min_effective_funding_usd is not None
        and effective_funding_pnl_usd < config.min_effective_funding_usd
    ):
        block_reasons.append("effective_funding_below_usd_threshold")
    if expected_net_edge_usd < config.min_expected_net_edge_usd:
        block_reasons.append("net_edge_below_usd_threshold")
    if expected_net_edge_bps < config.min_expected_net_edge_bps:
        block_reasons.append("net_edge_below_bps_threshold")

    can_enter = len(block_reasons) == 0
    return PairCandidate(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        long_ask=long_ask,
        short_bid=short_bid,
        mid_reference=mid_reference,
        target_qty=target_qty,
        executable_qty=executable_qty,
        notional_usd=reference_notional,
        long_min_order_notional_usd=long_min_order_notional_usd,
        short_min_order_notional_usd=short_min_order_notional_usd,
        entry_open_edge_usd=entry_open_edge_usd,
        entry_open_edge_bps=entry_open_edge_bps,
        open_fee_cost_usd=open_fee_cost_usd,
        raw_funding_pnl_usd=raw_funding_pnl_usd,
        effective_funding_pnl_usd=effective_funding_pnl_usd,
        funding_confidence=funding_confidence,
        funding_alignment_factor=funding_alignment_factor,
        funding_time_decay_factor=funding_time_decay_factor,
        expected_close_cost_usd=expected_close_cost_usd,
        risk_buffer_usd=risk_buffer_usd,
        stale_data_penalty_usd=stale_data_penalty_usd,
        expected_net_edge_usd=expected_net_edge_usd,
        expected_net_edge_bps=expected_net_edge_bps,
        funding_long_rate=funding_long_rate,
        funding_short_rate=funding_short_rate,
        long_next_funding_ms=long_next_funding_ms,
        short_next_funding_ms=short_next_funding_ms,
        can_enter=can_enter,
        block_reasons=block_reasons,
    )


def rank_candidates(candidates: list[PairCandidate]) -> list[PairCandidate]:
    return sorted(
        candidates,
        key=lambda item: (
            0 if item.can_enter else 1,
            -item.expected_net_edge_usd,
            -item.effective_funding_pnl_usd,
            item.symbol,
            item.long_exchange,
            item.short_exchange,
        ),
    )


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


def candidate_pair_key(candidate: PairCandidate) -> tuple[str, str, str]:
    return (candidate.symbol, candidate.long_exchange, candidate.short_exchange)


def pair_runtime_key(pair: OpenPairState) -> tuple[str, str, str]:
    return (pair.symbol, pair.long_exchange, pair.short_exchange)


def apply_open_pair_blocks(
    candidates: list[PairCandidate],
    open_pairs: dict[str, OpenPairState],
    config: StrategyConfig,
) -> list[PairCandidate]:
    active_keys = {
        pair_runtime_key(pair)
        for pair in open_pairs.values()
        if pair.status not in {"closed", "failed"}
    }
    active_symbols = {
        pair.symbol
        for pair in open_pairs.values()
        if pair.status not in {"closed", "failed"}
    }
    for candidate in candidates:
        if candidate_pair_key(candidate) in active_keys and "pair_already_open" not in candidate.block_reasons:
            candidate.block_reasons.append("pair_already_open")
            candidate.can_enter = False
        if candidate.symbol in active_symbols and "symbol_already_open" not in candidate.block_reasons:
            candidate.block_reasons.append("symbol_already_open")
            candidate.can_enter = False
        if len(active_keys) >= config.max_open_pairs and "max_open_pairs_reached" not in candidate.block_reasons:
            candidate.block_reasons.append("max_open_pairs_reached")
            candidate.can_enter = False
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


def first_leg_plan(candidate: PairCandidate, config: StrategyConfig) -> tuple[str, str]:
    role = config.first_leg_role
    if role == "short":
        return candidate.short_exchange, "sell"
    return candidate.long_exchange, "buy"


def hedge_leg_plan(candidate: PairCandidate, config: StrategyConfig) -> tuple[str, str]:
    first_exchange, _ = first_leg_plan(candidate, config)
    if first_exchange == candidate.long_exchange:
        return candidate.short_exchange, "sell"
    return candidate.long_exchange, "buy"


def marketable_limit_price(exchange: Any, symbol: str, side: str) -> float:
    snapshot = exchange.get_symbol_snapshot(symbol)
    if side.lower() == "buy":
        return extract_best_ask(snapshot)
    return extract_best_bid(snapshot)


def marketable_limit_price_with_bps(exchange: Any, symbol: str, side: str, bps: float) -> float:
    base_price = marketable_limit_price(exchange, symbol, side)
    if base_price <= 0:
        return 0.0
    if side.lower() == "buy":
        return base_price * (1.0 + (bps / 10000.0))
    return base_price * (1.0 - (bps / 10000.0))


def normalize_exchange_qty(exchange: Any, symbol: str, qty: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    min_qty = _safe_float(meta.get("min_qty"), 0.0)
    max_qty = _safe_float(meta.get("max_qty"), 0.0)

    normalized = floor_by_step(abs(qty), step_size) if step_size > 0 else abs(qty)
    if max_qty > 0:
        normalized = min(normalized, max_qty)
    if min_qty > 0 and normalized + EPS < min_qty:
        return 0.0
    return normalized


def exchange_min_order_notional_usd(exchange: Any, symbol: str, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    explicit_min_notional = _safe_float(meta.get("min_notional"), 0.0)
    min_qty = _safe_float(meta.get("min_qty"), 0.0)
    implied_min_notional = (min_qty * price) if min_qty > 0 and price > 0 else 0.0
    return max(explicit_min_notional, implied_min_notional)


def exchange_min_executable_qty(exchange: Any, symbol: str, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    min_qty = _safe_float(meta.get("min_qty"), 0.0)
    min_notional = exchange_min_order_notional_usd(exchange, symbol, price)
    min_qty_from_notional = (min_notional / price) if min_notional > 0 and price > 0 else 0.0
    required_qty = max(min_qty, min_qty_from_notional)
    if required_qty <= 0:
        return 0.0
    if step_size > 0:
        return ceil_by_step(required_qty, step_size)
    return required_qty


def normalize_exchange_qty_for_order(exchange: Any, symbol: str, qty: float, price: float) -> float:
    meta = getattr(exchange, "ticker_info", {}).get(symbol, {}) or {}
    step_size = _safe_float(meta.get("step_size"), 0.0)
    max_qty = _safe_float(meta.get("max_qty"), 0.0)

    normalized = floor_by_step(abs(qty), step_size) if step_size > 0 else abs(qty)
    if max_qty > 0:
        normalized = min(normalized, max_qty)

    required_qty = exchange_min_executable_qty(exchange, symbol, price)
    if required_qty > 0 and normalized + EPS < required_qty:
        return 0.0
    return normalized


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
    deadline = time.monotonic() + max(timeout_sec, 0.0)
    filled_qty = 0.0
    while time.monotonic() < deadline:
        post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        filled_qty = position_fill_delta(pre_qty, post_qty, side)
        if filled_qty + EPS >= target_qty:
            return filled_qty
        await asyncio.sleep(0.1)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return position_fill_delta(pre_qty, post_qty, side)


async def wait_for_position_close(
    exchange: Any,
    symbol: str,
    side: str,
    pre_qty: float,
    target_qty: float,
    timeout_sec: float,
) -> float:
    deadline = time.monotonic() + max(timeout_sec, 0.0)
    closed_qty = 0.0
    while time.monotonic() < deadline:
        post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        closed_qty = position_close_delta(pre_qty, post_qty, side)
        if closed_qty + EPS >= target_qty:
            return closed_qty
        await asyncio.sleep(0.1)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return position_close_delta(pre_qty, post_qty, side)


def extract_order_uuid(order_result: dict[str, Any] | None) -> str | None:
    if not isinstance(order_result, dict):
        return None
    for key in ("uuid", "client_order_id", "order_id"):
        value = order_result.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def extract_order_avg_price(order_result: dict[str, Any] | None, fallback_price: float) -> float:
    if not isinstance(order_result, dict):
        return fallback_price
    for key in ("avg_price", "price", "fill_price"):
        value = _safe_float(order_result.get(key), 0.0)
        if value > 0:
            return value
    return fallback_price


def pair_entry_spread_usd(pair: OpenPairState) -> float:
    return pair.qty * (pair.short_entry_price - pair.long_entry_price)


def current_pair_close_edge(pair: OpenPairState, exchanges: dict[str, Any]) -> tuple[float, float, float]:
    long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
    short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
    long_bid = extract_best_bid(long_snapshot)
    short_ask = extract_best_ask(short_snapshot)
    close_edge_usd = pair.qty * (long_bid - short_ask) if long_bid > 0 and short_ask > 0 else 0.0
    return long_bid, short_ask, close_edge_usd


async def bootstrap_candidate_exchanges(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> None:
    if not config.bootstrap_on_entry:
        return
    for exchange_name in {candidate.long_exchange, candidate.short_exchange}:
        await exchanges[exchange_name].bootstrap_symbol(
            symbol=candidate.symbol,
            leverage=config.leverage,
            margin_mode=config.margin_mode,
        )


async def cancel_if_open(exchange: Any, symbol: str, order_result: dict[str, Any] | None) -> None:
    order_uuid = extract_order_uuid(order_result)
    if not order_uuid:
        return
    try:
        await exchange.cancel_order(symbol, order_uuid)
    except Exception as exc:
        print(f"[strategy][cancel_if_open][warn] symbol={symbol} uuid={order_uuid} error={exc}")


async def resolve_executed_qty_from_order_status(exchange: Any, order_result: dict[str, Any] | None, target_qty: float) -> float:
    if not order_result or not hasattr(exchange, "get_order_status"):
        return 0.0
    try:
        status = await exchange.get_order_status(order_result)
    except Exception as exc:
        print(f"[strategy][order_status][warn] error={exc}")
        return 0.0
    if not isinstance(status, dict) or status.get("error"):
        return 0.0
    executed_qty = _safe_float(status.get("executed_qty"), 0.0)
    if executed_qty > EPS:
        return min(executed_qty, target_qty)
    status_text = str(status.get("status") or "").upper()
    if status_text in {"FILLED", "FULLY_FILLED"}:
        return target_qty
    return 0.0


async def resolve_executed_qty_from_recent_trades(
    exchange: Any,
    symbol: str,
    order_result: dict[str, Any] | None,
    target_qty: float,
) -> float:
    if not order_result or not hasattr(exchange, "get_trade_history"):
        return 0.0
    order_id = order_result.get("order_id")
    if order_id in (None, ""):
        return 0.0
    try:
        rows = await exchange.get_trade_history(symbol, limit=30)
    except Exception as exc:
        print(f"[strategy][trade_lookup][warn] symbol={symbol} error={exc}")
        return 0.0
    if not isinstance(rows, list):
        return 0.0
    matched_qty = 0.0
    for row in rows:
        if str(row.get("order_id")) != str(order_id):
            continue
        matched_qty += abs(_safe_float(row.get("qty"), 0.0))
    if matched_qty <= EPS:
        return 0.0
    return min(matched_qty, target_qty)


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
    await refresh_symbol_state(exchange, symbol, reason)
    post_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    position_qty = (
        position_close_delta(pre_qty, post_qty, side)
        if is_close
        else position_fill_delta(pre_qty, post_qty, side)
    )
    status_qty = await resolve_executed_qty_from_order_status(exchange, order_result, target_qty)
    trade_qty = await resolve_executed_qty_from_recent_trades(exchange, symbol, order_result, target_qty)
    return max(position_qty, status_qty, trade_qty)


def settle_grace_sec(config: StrategyConfig) -> float:
    return max(1.5, config.action_settle_wait_sec * 4.0)


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


async def refresh_symbol_state(exchange: Any, symbol: str, reason: str) -> None:
    reconcile = getattr(exchange, "reconcile_state", None)
    if not callable(reconcile):
        return
    try:
        await reconcile(include_account=True, include_open_orders=True, include_funding=False, reason=reason)
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
        long_qty = effective_position_qty(long_snapshot, pair.long_exchange, pair.symbol, config)
        short_qty = effective_position_qty(short_snapshot, pair.short_exchange, pair.symbol, config)
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


async def emergency_flatten_symbol_state(
    exchange: Any,
    symbol: str,
    aggressive_bps: float = 20.0,
) -> tuple[bool, bool]:
    for attempt in range(3):
        await refresh_symbol_state(exchange, symbol, f"strategy_emergency_flatten_{attempt + 1}")
        current_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
        if abs(current_qty) <= EPS:
            return (True, False)

        side = "sell" if current_qty > 0 else "buy"
        try:
            print(
                f"[strategy][emergency_flatten] attempt={attempt + 1} "
                f"exchange={getattr(exchange, 'persist_name', getattr(exchange, 'name', 'unknown'))} "
                f"symbol={symbol} side={side} qty={abs(current_qty):.6f}"
            )
            market_result = await exchange.market_order(
                symbol,
                side,
                abs(current_qty),
                is_close=use_reduce_only_market_close(exchange, current_qty, side),
            )
            if not (market_result and market_result.get("error")):
                resolved_close_qty = await resolve_effective_order_qty(
                    exchange=exchange,
                    symbol=symbol,
                    side=side,
                    pre_qty=current_qty,
                    target_qty=abs(current_qty),
                    order_result=market_result,
                    is_close=True,
                    reason=f"strategy_emergency_flatten_market_{attempt + 1}",
                )
                if resolved_close_qty + qty_match_tolerance(abs(current_qty)) >= abs(current_qty):
                    return (True, False)
                await asyncio.sleep(1.2)
                current_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
                if abs(current_qty) <= EPS:
                    return (True, False)
            else:
                if error_means_no_position_to_close(market_result):
                    return (True, True)
                print(
                    f"[strategy][emergency_flatten][market_warn] "
                    f"exchange={getattr(exchange, 'persist_name', getattr(exchange, 'name', 'unknown'))} "
                    f"symbol={symbol} result={market_result}"
                )
                await asyncio.sleep(0.8)
        except Exception as exc:
            print(f"[strategy][emergency_flatten][market_warn] symbol={symbol} error={exc}")
            await asyncio.sleep(0.8)

    remaining_qty = extract_position_qty(exchange.get_symbol_snapshot(symbol))
    return (abs(remaining_qty) <= EPS, False)


async def emergency_flatten_symbol(exchange: Any, symbol: str, aggressive_bps: float = 20.0) -> bool:
    ok, _ = await emergency_flatten_symbol_state(exchange, symbol, aggressive_bps=aggressive_bps)
    return ok


async def emergency_flatten_pair_state(
    pair_like: Any,
    exchanges: dict[str, Any],
    aggressive_bps: float = 20.0,
) -> tuple[bool, set[str]]:
    symbol = pair_like.symbol
    long_ok, long_assumed_flat = await emergency_flatten_symbol_state(
        exchanges[pair_like.long_exchange],
        symbol,
        aggressive_bps=aggressive_bps,
    )
    short_ok, short_assumed_flat = await emergency_flatten_symbol_state(
        exchanges[pair_like.short_exchange],
        symbol,
        aggressive_bps=aggressive_bps,
    )
    forced_flat_exchanges: set[str] = set()
    if long_assumed_flat:
        forced_flat_exchanges.add(pair_like.long_exchange)
    if short_assumed_flat:
        forced_flat_exchanges.add(pair_like.short_exchange)
    return (bool(long_ok and short_ok), forced_flat_exchanges)


async def emergency_flatten_pair(pair_like: Any, exchanges: dict[str, Any], aggressive_bps: float = 20.0) -> bool:
    ok, _ = await emergency_flatten_pair_state(pair_like, exchanges, aggressive_bps=aggressive_bps)
    return ok


def close_first_leg_plan(pair: OpenPairState, config: StrategyConfig) -> tuple[str, str]:
    if config.close_first_leg_role == "long":
        return pair.long_exchange, "sell"
    return pair.short_exchange, "buy"


def close_hedge_leg_plan(pair: OpenPairState, config: StrategyConfig) -> tuple[str, str]:
    first_exchange, _ = close_first_leg_plan(pair, config)
    if first_exchange == pair.long_exchange:
        return pair.short_exchange, "buy"
    return pair.long_exchange, "sell"


async def enter_pair_limit_then_hedge(
    candidate: PairCandidate,
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> OpenPairState | None:
    symbol = candidate.symbol
    first_exchange_name, first_side = first_leg_plan(candidate, config)
    hedge_exchange_name, hedge_side = hedge_leg_plan(candidate, config)
    first_exchange = exchanges[first_exchange_name]
    hedge_exchange = exchanges[hedge_exchange_name]

    await bootstrap_candidate_exchanges(candidate, exchanges, config)

    first_price = marketable_limit_price(first_exchange, symbol, first_side)
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
            f"[strategy][enter][abort] common_executable_qty_zero symbol={symbol} "
            f"requested={candidate.target_qty:.6f} candidate_exec={candidate.executable_qty:.6f}"
        )
        return None

    pre_qty_first = extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
    pre_qty_hedge = extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol))

    if first_price <= 0:
        print(f"[strategy][enter][abort] first leg price missing exchange={first_exchange_name} symbol={symbol}")
        return None

    print(
        f"[strategy][enter] first_leg exchange={first_exchange_name} side={first_side} "
        f"qty={executable_qty:.6f} requested={candidate.target_qty:.6f} price={first_price}"
    )
    first_result = await first_exchange.order(symbol, first_side, executable_qty, first_price)
    if not first_result or (isinstance(first_result, dict) and first_result.get("error")):
        print(f"[strategy][enter][abort] first_leg_failed result={first_result}")
        return None

    first_filled_qty = await wait_for_position_fill(
        first_exchange,
        symbol,
        first_side,
        pre_qty_first,
        executable_qty,
        config.first_leg_timeout_sec,
    )
    if first_filled_qty + EPS < executable_qty:
        first_filled_qty = max(
            first_filled_qty,
            await resolve_effective_order_qty(
                exchange=first_exchange,
                symbol=symbol,
                side=first_side,
                pre_qty=pre_qty_first,
                target_qty=executable_qty,
                order_result=first_result,
                is_close=False,
                reason="strategy_entry_first_probe_before_cancel",
            ),
        )
    if first_filled_qty + EPS < executable_qty:
        await cancel_if_open(first_exchange, symbol, first_result)
        await asyncio.sleep(config.action_settle_wait_sec)
        first_filled_qty = max(
            first_filled_qty,
            await resolve_effective_order_qty(
                exchange=first_exchange,
                symbol=symbol,
                side=first_side,
                pre_qty=pre_qty_first,
                target_qty=executable_qty,
                order_result=first_result,
                is_close=False,
                reason="strategy_entry_first_after_cancel",
            ),
        )
        if first_filled_qty + EPS < executable_qty:
            first_filled_qty = max(
                first_filled_qty,
                await wait_for_position_fill(
                    first_exchange,
                    symbol,
                    first_side,
                    pre_qty_first,
                    executable_qty,
                    settle_grace_sec(config),
                ),
            )

    if first_filled_qty <= EPS:
        print(f"[strategy][enter][abort] first_leg_no_fill symbol={symbol}")
        return None

    hedge_min_qty = exchange_min_executable_qty(hedge_exchange, symbol, hedge_price_preview)
    hedgeable_first_fill_qty = normalize_exchange_qty_for_order(hedge_exchange, symbol, first_filled_qty, hedge_price_preview)
    if hedgeable_first_fill_qty + EPS < first_filled_qty:
        print(
            f"[strategy][enter][abort] first_fill_not_hedgeable symbol={symbol} "
            f"first_filled={first_filled_qty:.6f} hedgeable={hedgeable_first_fill_qty:.6f} "
            f"hedge_min_qty={hedge_min_qty:.6f}"
        )
        flattened = await emergency_flatten_symbol(
            first_exchange,
            symbol,
            aggressive_bps=config.emergency_flatten_bps,
        )
        if not flattened:
            print(f"[strategy][danger] failed_to_flatten_unhedgeable_first_fill symbol={symbol}")
        return None

    hedged_qty = 0.0
    hedge_price_used = 0.0
    remaining_qty = max(first_filled_qty - hedged_qty, 0.0)

    for attempt in range(config.hedge_requote_count + 1):
        if remaining_qty <= EPS:
            break
        hedge_price = marketable_limit_price_with_bps(hedge_exchange, symbol, hedge_side, config.close_aggressive_bps)
        if hedge_price <= 0:
            break
        hedge_price_used = hedge_price
        print(
            f"[strategy][hedge] attempt={attempt + 1} exchange={hedge_exchange_name} side={hedge_side} "
            f"qty={remaining_qty:.6f} price={hedge_price}"
        )
        hedge_result = await hedge_exchange.order(symbol, hedge_side, remaining_qty, hedge_price)
        if hedge_result and not hedge_result.get("error"):
            await wait_for_position_fill(
                hedge_exchange,
                symbol,
                hedge_side,
                pre_qty_hedge,
                first_filled_qty,
                config.hedge_requote_timeout_sec,
            )
            hedged_qty = await resolve_effective_order_qty(
                exchange=hedge_exchange,
                symbol=symbol,
                side=hedge_side,
                pre_qty=pre_qty_hedge,
                target_qty=first_filled_qty,
                order_result=hedge_result,
                is_close=False,
                reason="strategy_entry_hedge_probe_before_cancel",
            )
            remaining_qty = max(first_filled_qty - hedged_qty, 0.0)
            if remaining_qty > EPS:
                await cancel_if_open(hedge_exchange, symbol, hedge_result)
                await asyncio.sleep(config.action_settle_wait_sec)
                hedged_qty = await resolve_effective_order_qty(
                    exchange=hedge_exchange,
                    symbol=symbol,
                    side=hedge_side,
                    pre_qty=pre_qty_hedge,
                    target_qty=first_filled_qty,
                    order_result=hedge_result,
                    is_close=False,
                    reason="strategy_entry_hedge_after_cancel",
                )
                if remaining_qty > EPS:
                    hedged_qty = max(
                        hedged_qty,
                        await wait_for_position_fill(
                            hedge_exchange,
                            symbol,
                            hedge_side,
                            pre_qty_hedge,
                            first_filled_qty,
                            settle_grace_sec(config),
                        ),
                    )
                remaining_qty = max(first_filled_qty - hedged_qty, 0.0)

    if remaining_qty > EPS:
        print(
            f"[strategy][hedge][market] exchange={hedge_exchange_name} side={hedge_side} "
            f"qty={remaining_qty:.6f}"
        )
        market_result = await hedge_exchange.market_order(symbol, hedge_side, remaining_qty)
        if market_result and market_result.get("error"):
            print(f"[strategy][hedge][market][error] result={market_result}")
        await asyncio.sleep(config.action_settle_wait_sec)
        await refresh_symbol_state(hedge_exchange, symbol, "strategy_entry_hedge_market")
        hedged_qty = position_fill_delta(
            pre_qty_hedge,
            extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol)),
            hedge_side,
        )
        remaining_qty = max(first_filled_qty - hedged_qty, 0.0)
        hedge_price_used = extract_order_avg_price(market_result, hedge_price_used)

    if remaining_qty > EPS:
        print(
            f"[strategy][danger] hedge_incomplete symbol={symbol} first_filled={first_filled_qty:.6f} "
            f"hedged={hedged_qty:.6f} remaining={remaining_qty:.6f}"
        )
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        return None

    await refresh_symbol_state(exchanges[candidate.long_exchange], symbol, "strategy_entry_post_open_long")
    await refresh_symbol_state(exchanges[candidate.short_exchange], symbol, "strategy_entry_post_open_short")
    long_qty_after = extract_position_qty(exchanges[candidate.long_exchange].get_symbol_snapshot(symbol))
    short_qty_after = extract_position_qty(exchanges[candidate.short_exchange].get_symbol_snapshot(symbol))
    if long_qty_after <= EPS or short_qty_after >= -EPS:
        print(
            f"[strategy][danger] direction_mismatch_after_entry symbol={symbol} "
            f"long_exchange={candidate.long_exchange} long_qty={long_qty_after:.6f} "
            f"short_exchange={candidate.short_exchange} short_qty={short_qty_after:.6f}"
        )
        await emergency_flatten_pair(candidate, exchanges, aggressive_bps=config.emergency_flatten_bps)
        return None

    long_entry_price = first_price if first_exchange_name == candidate.long_exchange else hedge_price_used
    short_entry_price = hedge_price_used if hedge_exchange_name == candidate.short_exchange else first_price
    pair_qty = min(abs(long_qty_after), abs(short_qty_after))
    pair_id = f"{symbol}:{candidate.long_exchange}:{candidate.short_exchange}:{int(time.time() * 1000)}"

    print(
        f"[strategy][open] pair_id={pair_id} symbol={symbol} qty={pair_qty:.6f} "
        f"long={candidate.long_exchange} short={candidate.short_exchange}"
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
        now_ms=now_ms,
    )
    stale_data_penalty_usd = compute_stale_data_penalty_usd(
        max((long_notional + short_notional) / 2.0, 0.0),
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        config,
    )
    return (
        estimate_close_pnl_now(pair, exchanges, config)
        + effective_funding_pnl_usd
        - stale_data_penalty_usd
    )


def estimate_close_pnl_now(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> float:
    long_bid, short_ask, _ = current_pair_close_edge(pair, exchanges)
    if long_bid <= 0 or short_ask <= 0:
        return -1e18

    gross_pnl = pair.qty * (
        (long_bid - short_ask) - (pair.long_entry_price - pair.short_entry_price)
    )
    long_notional = pair.qty * long_bid
    short_notional = pair.qty * short_ask
    close_fee_cost = (
        long_notional * config.close_fee_bps.get(pair.long_exchange, 0.0) / 10000.0
        + short_notional * config.close_fee_bps.get(pair.short_exchange, 0.0) / 10000.0
    )
    close_slippage = ((long_notional + short_notional) / 2.0) * (config.close_slippage_bps * 2.0) / 10000.0
    return gross_pnl - close_fee_cost - close_slippage


def decide_pair_close_reason(
    pair: OpenPairState,
    config: StrategyConfig,
    hold_pnl: float,
    entry_age_sec: float,
) -> str:
    if entry_age_sec < config.min_hold_sec:
        return "hold_grace"
    if pair.last_close_pnl_usd <= config.stop_loss_usd:
        return "stop_loss"
    if pair.last_close_pnl_usd >= config.take_profit_usd:
        return "take_profit"
    if entry_age_sec >= config.max_hold_sec:
        return "max_hold"
    if hold_pnl > config.hold_buffer_usd:
        return "hold"
    if pair.last_close_edge_usd > 0:
        return "hold_spread_support"
    if entry_age_sec < config.close_candidate_min_age_sec:
        return "hold_close_age_gate"
    return "close_candidate"


async def close_pair_limit_then_hedge(
    pair: OpenPairState,
    exchanges: dict[str, Any],
    config: StrategyConfig,
) -> bool:
    pair.assumed_flat_exchanges = []
    symbol = pair.symbol
    first_exchange_name, first_side = close_first_leg_plan(pair, config)
    hedge_exchange_name, hedge_side = close_hedge_leg_plan(pair, config)
    first_exchange = exchanges[first_exchange_name]
    hedge_exchange = exchanges[hedge_exchange_name]

    pre_qty_first = extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
    pre_qty_hedge = extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol))
    target_close_qty = min(abs(pre_qty_first), abs(pre_qty_hedge), pair.qty)
    first_price = marketable_limit_price_with_bps(first_exchange, symbol, first_side, config.close_aggressive_bps)
    hedge_price_preview = marketable_limit_price_with_bps(hedge_exchange, symbol, hedge_side, config.close_aggressive_bps)
    target_close_qty = compute_common_executable_qty(
        first_exchange,
        hedge_exchange,
        symbol,
        target_close_qty,
        left_price=first_price,
        right_price=hedge_price_preview,
    )
    if target_close_qty <= EPS:
        print(f"[strategy][close] nothing to close pair_id={pair.pair_id}")
        record_live_event(config, "pair_close_noop", pair=pair_event_payload(pair))
        return True

    if first_price <= 0:
        print(f"[strategy][close][abort] first close price missing exchange={first_exchange_name} symbol={symbol}")
        return False

    print(
        f"[strategy][close] first_leg exchange={first_exchange_name} side={first_side} "
        f"qty={target_close_qty:.6f} price={first_price}"
    )
    first_result = await first_exchange.order(symbol, first_side, target_close_qty, first_price)
    if not first_result or (isinstance(first_result, dict) and first_result.get("error")):
        print(f"[strategy][close][abort] first_leg_failed result={first_result}")
        return False

    first_closed_qty = await wait_for_position_close(
        first_exchange,
        symbol,
        first_side,
        pre_qty_first,
        target_close_qty,
        config.first_leg_timeout_sec,
    )
    if first_closed_qty + EPS < target_close_qty:
        first_closed_qty = max(
            first_closed_qty,
            await resolve_effective_order_qty(
                exchange=first_exchange,
                symbol=symbol,
                side=first_side,
                pre_qty=pre_qty_first,
                target_qty=target_close_qty,
                order_result=first_result,
                is_close=True,
                reason="strategy_close_first_probe_before_cancel",
            ),
        )
    if first_closed_qty + EPS < target_close_qty:
        await cancel_if_open(first_exchange, symbol, first_result)
        await asyncio.sleep(config.action_settle_wait_sec)
        first_closed_qty = max(
            first_closed_qty,
            await resolve_effective_order_qty(
                exchange=first_exchange,
                symbol=symbol,
                side=first_side,
                pre_qty=pre_qty_first,
                target_qty=target_close_qty,
                order_result=first_result,
                is_close=True,
                reason="strategy_close_first_after_cancel",
            ),
        )
        if first_closed_qty + EPS < target_close_qty:
            first_closed_qty = max(
                first_closed_qty,
                await wait_for_position_close(
                    first_exchange,
                    symbol,
                    first_side,
                    pre_qty_first,
                    target_close_qty,
                    settle_grace_sec(config),
                ),
            )

    if first_closed_qty <= EPS:
        pair.status = "open"
        print(f"[strategy][close] first_leg_no_fill pair_id={pair.pair_id}")
        return False

    hedged_qty = 0.0
    remaining_qty = max(first_closed_qty, 0.0)
    for attempt in range(config.hedge_requote_count + 1):
        outstanding = max(first_closed_qty - hedged_qty, 0.0)
        if outstanding <= EPS:
            break
        hedge_price = marketable_limit_price_with_bps(hedge_exchange, symbol, hedge_side, config.close_aggressive_bps)
        if hedge_price <= 0:
            break
        print(
            f"[strategy][close-hedge] attempt={attempt + 1} exchange={hedge_exchange_name} side={hedge_side} "
            f"qty={outstanding:.6f} price={hedge_price}"
        )
        hedge_result = await hedge_exchange.order(symbol, hedge_side, outstanding, hedge_price)
        if hedge_result and not hedge_result.get("error"):
            await wait_for_position_close(
                hedge_exchange,
                symbol,
                hedge_side,
                pre_qty_hedge,
                first_closed_qty,
                config.hedge_requote_timeout_sec,
            )
            hedged_qty = await resolve_effective_order_qty(
                exchange=hedge_exchange,
                symbol=symbol,
                side=hedge_side,
                pre_qty=pre_qty_hedge,
                target_qty=first_closed_qty,
                order_result=hedge_result,
                is_close=True,
                reason="strategy_close_hedge_probe_before_cancel",
            )
            remaining_qty = max(first_closed_qty - hedged_qty, 0.0)
            if remaining_qty > EPS:
                await cancel_if_open(hedge_exchange, symbol, hedge_result)
                await asyncio.sleep(config.action_settle_wait_sec)
                hedged_qty = await resolve_effective_order_qty(
                    exchange=hedge_exchange,
                    symbol=symbol,
                    side=hedge_side,
                    pre_qty=pre_qty_hedge,
                    target_qty=first_closed_qty,
                    order_result=hedge_result,
                    is_close=True,
                    reason="strategy_close_hedge_after_cancel",
                )
                if remaining_qty > EPS:
                    hedged_qty = max(
                        hedged_qty,
                        await wait_for_position_close(
                            hedge_exchange,
                            symbol,
                            hedge_side,
                            pre_qty_hedge,
                            first_closed_qty,
                            settle_grace_sec(config),
                        ),
                    )
                remaining_qty = max(first_closed_qty - hedged_qty, 0.0)

    if remaining_qty > EPS:
        print(
            f"[strategy][close-hedge][market] exchange={hedge_exchange_name} side={hedge_side} "
            f"qty={remaining_qty:.6f}"
        )
        current_hedge_qty = extract_position_qty(hedge_exchange.get_symbol_snapshot(symbol))
        market_result = await hedge_exchange.market_order(
            symbol,
            hedge_side,
            remaining_qty,
            is_close=use_reduce_only_market_close(hedge_exchange, current_hedge_qty, hedge_side),
        )
        if market_result and market_result.get("error"):
            print(f"[strategy][close-hedge][market][error] result={market_result}")
        await asyncio.sleep(config.action_settle_wait_sec)
        hedged_qty = max(
            hedged_qty,
            await resolve_effective_order_qty(
                exchange=hedge_exchange,
                symbol=symbol,
                side=hedge_side,
                pre_qty=pre_qty_hedge,
                target_qty=first_closed_qty,
                order_result=market_result,
                is_close=True,
                reason="strategy_close_hedge_market",
            ),
        )
        remaining_qty = max(first_closed_qty - hedged_qty, 0.0)

    close_state, long_qty_after, short_qty_after, balanced_qty_after, residual_gap = await settle_pair_close_state(
        pair,
        exchanges,
        config,
        reference_qty=max(pair.qty, first_closed_qty),
        reason_prefix="strategy_close_post",
    )
    pair.qty = balanced_qty_after
    if close_state == "closed":
        pair.status = "closed"
        pair.assumed_flat_exchanges = [pair.long_exchange, pair.short_exchange]
        print(f"[strategy][close] pair_closed pair_id={pair.pair_id}")
        record_live_event(config, "pair_closed", pair=pair_event_payload(pair))
        return True

    if close_state == "balanced_open":
        pair.status = "open"
        print(
            f"[strategy][close] partial_close_done pair_id={pair.pair_id} remaining_pair_qty={pair.qty:.6f}"
        )
        record_live_event(config, "pair_partial_close_done", pair=pair_event_payload(pair))
        return True

    pair.status = "close_imbalance"
    print(
        f"[strategy][danger] close_imbalance pair_id={pair.pair_id} "
        f"long_qty_after={long_qty_after:.6f} short_qty_after={short_qty_after:.6f} "
        f"residual_gap={residual_gap:.6f} remaining_qty={remaining_qty:.6f} state={close_state}"
    )
    flattened, forced_flat_exchanges = await emergency_flatten_pair_state(
        pair,
        exchanges,
        aggressive_bps=config.emergency_flatten_bps,
    )
    close_state, long_qty_after, short_qty_after, balanced_qty_after, residual_gap = await settle_pair_close_state(
        pair,
        exchanges,
        config,
        reference_qty=max(pair.qty, first_closed_qty),
        reason_prefix="strategy_close_post_emergency",
        forced_flat_exchanges=forced_flat_exchanges,
    )
    pair.qty = balanced_qty_after
    if flattened and close_state == "closed":
        pair.status = "closed"
        pair.assumed_flat_exchanges = [pair.long_exchange, pair.short_exchange]
        print(f"[strategy][close] pair_closed_after_emergency pair_id={pair.pair_id}")
        record_live_event(config, "pair_closed_after_emergency", pair=pair_event_payload(pair))
        return True

    pair.status = "close_pending"
    if close_state == "balanced_open":
        pair.status = "open"
        print(
            f"[strategy][close] partial_close_done_after_emergency pair_id={pair.pair_id} "
            f"remaining_pair_qty={pair.qty:.6f}"
        )
        record_live_event(config, "pair_partial_close_done_after_emergency", pair=pair_event_payload(pair))
        return True
    print(
        f"[strategy][close][warn] close_pending pair_id={pair.pair_id} "
        f"long_qty_after={long_qty_after:.6f} short_qty_after={short_qty_after:.6f} "
        f"state={close_state}"
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


async def manage_open_pairs(
    open_pairs: dict[str, OpenPairState],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int],
) -> None:
    for pair_id, pair in list(open_pairs.items()):
        long_snapshot = exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)
        short_snapshot = exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)
        action_inputs_fresh = pair_action_inputs_are_fresh(long_snapshot, short_snapshot, config)
        long_qty = effective_position_qty(
            long_snapshot,
            pair.long_exchange,
            pair.symbol,
            config,
            assumed_flat_until_ms=assumed_flat_until_ms,
        )
        short_qty = effective_position_qty(
            short_snapshot,
            pair.short_exchange,
            pair.symbol,
            config,
            assumed_flat_until_ms=assumed_flat_until_ms,
        )
        if abs(long_qty) <= EPS and abs(short_qty) <= EPS:
            pair.status = "closed"
            print(f"[strategy][pair] closed pair_id={pair_id}")
            record_live_event(config, "pair_closed_detected", pair=pair_event_payload(pair))
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
                pair.long_exchange,
                pair.symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            short_qty = effective_position_qty(
                short_snapshot,
                pair.short_exchange,
                pair.symbol,
                config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            if abs(long_qty) <= EPS and abs(short_qty) <= EPS:
                pair.status = "closed"
                print(f"[strategy][pair] closed pair_id={pair_id}")
                record_live_event(config, "pair_closed_detected", pair=pair_event_payload(pair))
                del open_pairs[pair_id]
                continue
            if not action_inputs_fresh:
                pair.status = "await_account_sync"
                continue
            pair.status = "direction_mismatch"
            current_ms = now_ms()
            if current_ms - pair.last_direction_mismatch_log_ms >= int(config.danger_log_cooldown_sec * 1000):
                print(
                    f"[strategy][danger] pair_direction_mismatch pair_id={pair_id} "
                    f"long_qty={long_qty:.6f} short_qty={short_qty:.6f}"
                )
                pair.last_direction_mismatch_log_ms = current_ms
            if current_ms < pair.next_recovery_retry_ms:
                continue

            pair.next_recovery_retry_ms = current_ms + int(config.recovery_retry_cooldown_sec * 1000)
            flattened, forced_flat_exchanges = await emergency_flatten_pair_state(
                pair,
                exchanges,
                aggressive_bps=config.emergency_flatten_bps,
            )
            mismatch_state, long_qty_after, short_qty_after, balanced_qty_after, residual_gap = await settle_pair_close_state(
                pair,
                exchanges,
                config,
                reference_qty=max(pair.qty, abs(long_qty), abs(short_qty)),
                reason_prefix="strategy_manage_pair_direction_mismatch",
                forced_flat_exchanges=forced_flat_exchanges,
            )
            pair.qty = balanced_qty_after
            if flattened and mismatch_state == "closed":
                pair.status = "closed"
                pair.assumed_flat_exchanges = sorted(forced_flat_exchanges)
                register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
                print(f"[strategy][pair] closed_after_recovery pair_id={pair_id}")
                record_live_event(
                    config,
                    "pair_closed_after_recovery",
                    pair=pair_event_payload(pair),
                    forced_flat_exchanges=sorted(forced_flat_exchanges),
                )
                del open_pairs[pair_id]
                continue
            if mismatch_state == "balanced_open":
                pair.status = "open"
                print(
                    f"[strategy][pair] recovered_balanced_open pair_id={pair_id} "
                    f"remaining_pair_qty={pair.qty:.6f}"
                )
                record_live_event(config, "pair_recovered_balanced_open", pair=pair_event_payload(pair))
                continue
            pair.status = "recovery_pending"
            if current_ms - pair.last_direction_mismatch_log_ms >= int(config.danger_log_cooldown_sec * 1000):
                print(
                    f"[strategy][pair][warn] recovery_pending pair_id={pair_id} "
                    f"long_qty_after={long_qty_after:.6f} short_qty_after={short_qty_after:.6f} "
                    f"state={mismatch_state} residual_gap={residual_gap:.6f}"
                )
                pair.last_direction_mismatch_log_ms = current_ms
            continue

        balanced_qty = min(abs(long_qty), abs(short_qty))
        pair.qty = balanced_qty
        pair.last_close_pnl_usd = estimate_close_pnl_now(pair, exchanges, config)
        _, _, pair.last_close_edge_usd = current_pair_close_edge(pair, exchanges)

        hold_pnl = estimate_hold_pnl_next_funding(pair, exchanges, config)
        entry_age_sec = max((now_ms() - pair.entry_time_ms) / 1000.0, 0.0)
        decision = "await_account_sync" if not action_inputs_fresh else decide_pair_close_reason(pair, config, hold_pnl, entry_age_sec)
        pair.last_close_reason = decision
        if pair.last_hold_decision != decision or abs(pair.last_hold_pnl_usd - hold_pnl) >= 0.01:
            pair.last_hold_decision = decision
            pair.last_hold_pnl_usd = hold_pnl
            print(
                f"[strategy][pair] pair_id={pair_id} symbol={pair.symbol} "
                f"decision={decision} hold_pnl_next_funding={hold_pnl:.4f} "
                f"hold_buffer_usd={config.hold_buffer_usd:.4f} "
                f"close_pnl_now={pair.last_close_pnl_usd:.4f} "
                f"close_edge_now={pair.last_close_edge_usd:.4f} "
                f"entry_age_sec={entry_age_sec:.1f} min_hold_sec={config.min_hold_sec:.1f}"
            )

        if config.auto_close_enabled and decision in {"stop_loss", "take_profit", "max_hold", "close_candidate"}:
            current_ms = now_ms()
            if current_ms < pair.next_auto_close_retry_ms:
                continue
            pair.next_auto_close_retry_ms = current_ms + int(config.auto_close_retry_cooldown_sec * 1000)
            print(f"[strategy][pair] auto_close_trigger pair_id={pair_id} reason={decision}")
            record_live_event(
                config,
                "pair_auto_close_trigger",
                pair=pair_event_payload(pair),
                reason=decision,
                entry_age_sec=entry_age_sec,
            )
            ok = await close_pair_limit_then_hedge(pair, exchanges, config)
            if ok and pair.status == "closed":
                register_pair_assumed_flat(pair, assumed_flat_until_ms, config)
                del open_pairs[pair_id]
            elif not ok:
                print(
                    f"[strategy][pair][warn] auto_close_not_completed pair_id={pair_id} "
                    f"status={pair.status}"
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
    return (
        f"symbol={candidate.symbol} qty={candidate.executable_qty:.6f} "
        f"first_leg={first_exchange}:{first_side} timeout={config.first_leg_timeout_sec}s "
        f"hedge={hedge_exchange}:{hedge_side} requote={config.hedge_requote_count}x{config.hedge_requote_timeout_sec}s"
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


async def handle_command(
    command: str,
    candidates: list[PairCandidate],
    open_pairs: dict[str, OpenPairState],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    assumed_flat_until_ms: dict[tuple[str, str], int],
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
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
) -> list[PairCandidate]:
    snapshots_by_exchange: dict[str, dict[str, Any]] = {
        exchange_name: {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
        for exchange_name, exchange in exchanges.items()
    }
    candidates: list[PairCandidate] = []
    for left, right in config.allowed_pairs:
        for symbol in symbols:
            candidates.append(
                compute_pair_candidate(
                    symbol,
                    left,
                    right,
                    snapshots_by_exchange,
                    exchanges,
                    config,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                )
            )
            candidates.append(
                compute_pair_candidate(
                    symbol,
                    right,
                    left,
                    snapshots_by_exchange,
                    exchanges,
                    config,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                )
            )
    ranked = rank_candidates(candidates)
    if open_pairs is not None:
        ranked = apply_open_pair_blocks(ranked, open_pairs, config)
    return ranked


def print_strategy_config(config: StrategyConfig, symbols: list[str]) -> None:
    live_mode = "LIVE" if config.enable_live else "PAPER"
    effective_funding_floor = (
        "disabled"
        if config.min_effective_funding_usd is None
        else f"{config.min_effective_funding_usd}"
    )
    print("\n[strategy][config]")
    print(f"mode={live_mode}")
    print(f"exchanges={config.exchange_names}")
    print(f"allowed_pairs={config.allowed_pairs}")
    print(f"symbols={symbols[:20]}{'...' if len(symbols) > 20 else ''}")
    print(f"order_notional_usd={config.order_notional_usd}")
    print(
        f"first_leg_timeout_sec={config.first_leg_timeout_sec} "
        f"hedge_requote_timeout_sec={config.hedge_requote_timeout_sec} "
        f"hedge_requote_count={config.hedge_requote_count}"
    )
    print(
        f"approval_mode={config.approval_mode} "
        f"auto_enter_enabled={config.auto_enter_enabled} "
        f"auto_enter_cooldown_sec={config.auto_enter_cooldown_sec} "
        f"render_interval_sec={config.render_interval_sec} "
        f"recovery_retry_cooldown_sec={config.recovery_retry_cooldown_sec} "
        f"auto_close_retry_cooldown_sec={config.auto_close_retry_cooldown_sec} "
        f"action_account_fresh_sec={config.action_account_fresh_sec} "
        f"bootstrap_on_entry={config.bootstrap_on_entry} "
        f"first_leg_role={config.first_leg_role} "
        f"close_first_leg_role={config.close_first_leg_role} "
        f"max_open_pairs={config.max_open_pairs} "
        f"assumed_flat_grace_sec={config.assumed_flat_grace_sec} "
        f"live_event_log_path={config.live_event_log_path}"
    )
    print(
        f"entry_threshold_usd={config.min_expected_net_edge_usd} "
        f"entry_threshold_bps={config.min_expected_net_edge_bps} "
        f"min_effective_funding_usd={effective_funding_floor} "
        f"hold_buffer_usd={config.hold_buffer_usd} "
        f"min_hold_sec={config.min_hold_sec} "
        f"close_candidate_min_age_sec={config.close_candidate_min_age_sec} "
        f"max_hold_sec={config.max_hold_sec} "
        f"take_profit_usd={config.take_profit_usd} "
        f"stop_loss_usd={config.stop_loss_usd} "
        f"position_stale_ignore_sec={config.position_stale_ignore_sec}"
    )
    print(
        f"entry_fee_bps={config.entry_fee_bps} "
        f"close_fee_bps={config.close_fee_bps} "
        f"close_slippage_bps={config.close_slippage_bps} "
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
    symbols = await resolve_strategy_symbols(config)
    exchanges = build_strategy_exchanges(config, symbols)
    await start_strategy_exchanges(exchanges)
    snapshot_config = load_snapshot_config()
    snapshot_task: asyncio.Task | None = None

    # exchanges가 이미 build/run 된 뒤
    if snapshot_config.get("enabled", False):
        snapshot_task = asyncio.create_task(
            run_snapshot_loop(
                exchanges,
                symbols,
                config=snapshot_config,
            )
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

    command_queue: asyncio.Queue[str] = asyncio.Queue()
    wake_queue: asyncio.Queue[tuple[str, str, int]] = asyncio.Queue()
    console_task = asyncio.create_task(console_command_reader(command_queue), name="strategy-console")
    listener_tasks = await attach_strategy_listeners(exchanges, symbols, wake_queue)
    open_pairs: dict[str, OpenPairState] = {}
    assumed_flat_until_ms: dict[tuple[str, str], int] = {}
    last_signature: tuple[Any, ...] | None = None
    last_render_ts = 0.0
    last_recompute_ts = 0.0
    last_auto_enter_ts = 0.0
    last_auto_enter_candidate_id: str | None = None

    try:
        while True:
            now = time.monotonic()
            wait_timeout = max(config.scan_interval_sec, 0.05)
            if (now - last_recompute_ts) < config.min_recompute_interval_sec:
                wait_timeout = min(wait_timeout, config.min_recompute_interval_sec - (now - last_recompute_ts))
            try:
                await asyncio.wait_for(wake_queue.get(), timeout=wait_timeout)
                while True:
                    try:
                        wake_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
            except TimeoutError:
                pass

            prune_assumed_flat(assumed_flat_until_ms)
            candidates = collect_candidates(
                symbols,
                exchanges,
                config,
                open_pairs=open_pairs,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            last_recompute_ts = time.monotonic()
            signature = build_candidate_signature(candidates, top_n=min(5, config.max_candidates_print))
            now = time.monotonic()
            if signature != last_signature:
                last_signature = signature
            if (now - last_render_ts) >= config.render_interval_sec:
                print(
                    render_candidate_table(
                        candidates,
                        config.max_candidates_print,
                        show_command_hint=not (config.enable_live and config.auto_enter_enabled),
                    )
                )
                last_render_ts = now

            if config.enable_live and config.auto_enter_enabled:
                active_open_pairs = [
                    pair for pair in open_pairs.values() if pair.status not in {"closed", "failed"}
                ]
                available_slots = max(config.max_open_pairs - len(active_open_pairs), 0)
                now = time.monotonic()
                if available_slots > 0 and (now - last_auto_enter_ts) >= config.auto_enter_cooldown_sec:
                    active_symbols = {pair.symbol for pair in active_open_pairs}
                    selected_candidates: list[PairCandidate] = []
                    selected_symbols: set[str] = set()
                    for candidate in candidates:
                        if not candidate.can_enter:
                            continue
                        if candidate.symbol in active_symbols or candidate.symbol in selected_symbols:
                            continue
                        selected_candidates.append(candidate)
                        selected_symbols.add(candidate.symbol)
                        if len(selected_candidates) >= available_slots:
                            break
                    if selected_candidates:
                        last_auto_enter_ts = now
                        last_auto_enter_candidate_id = ",".join(candidate.id for candidate in selected_candidates)
                    for candidate in selected_candidates:
                        print(
                            f"[strategy][auto-enter] candidate_id={candidate.id} "
                            f"symbol={candidate.symbol} net_usd={candidate.expected_net_edge_usd:.4f} "
                            f"net_bps={candidate.expected_net_edge_bps:.2f}"
                        )
                        pair = await enter_pair_limit_then_hedge(
                            candidate,
                            exchanges,
                            config,
                            assumed_flat_until_ms=assumed_flat_until_ms,
                        )
                        if pair is not None:
                            open_pairs[pair.pair_id] = pair
                    if selected_candidates:
                        last_auto_enter_candidate_id = None

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
                )
                if not should_continue:
                    return

            await manage_open_pairs(open_pairs, exchanges, config, assumed_flat_until_ms)
    finally:
        record_live_event(
            config,
            "runtime_stopped",
            open_pair_ids=sorted(open_pairs.keys()),
        )
        console_task.cancel()
        if snapshot_task is not None:
            snapshot_task.cancel()
            try:
                await snapshot_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                print(f"[snapshot-loop][stop][error] {exc}")
        for task in listener_tasks:
            task.cancel()
        await stop_strategy_exchanges(exchanges)


async def main() -> None:
    await strategy_loop()


if __name__ == "__main__":
    asyncio.run(main())
