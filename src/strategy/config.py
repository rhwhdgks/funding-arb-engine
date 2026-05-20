import os
from pathlib import Path

from src.core.exchange_registry import SUPPORTED_EXCHANGES
from src.strategy.models import (
    DEFAULT_ALLOWED_PAIRS,
    DEFAULT_CLOSE_FEE_BPS,
    DEFAULT_ENTRY_FEE_BPS,
    PROJECT_ROOT,
    StrategyConfig,
)

DEFAULT_HOLD_BUFFER_BPS = 2.0
DEFAULT_TAKE_PROFIT_BPS = 6.0
DEFAULT_STOP_LOSS_BPS = 8.0


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def parse_positive_int_env(key: str, default: int) -> int:
    raw = os.getenv(key, str(default))
    try:
        value = int(str(raw).strip())
    except Exception as exc:
        raise ValueError(f"{key} must be an integer >= 1") from exc
    if value < 1:
        raise ValueError(f"{key} must be an integer >= 1")
    return value


def parse_optional_float_env(key: str) -> float | None:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(str(raw).strip())
    except Exception as exc:
        raise ValueError(f"{key} must be a number") from exc


def normalize_exchange_name(name: str) -> str:
    normalized = str(name).strip().lower()
    if normalized not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {name}")
    return normalized


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
    raw_hold_buffer_bps = parse_optional_float_env("STRATEGY_HOLD_BUFFER_BPS")
    raw_take_profit_bps = parse_optional_float_env("STRATEGY_TAKE_PROFIT_BPS")
    raw_stop_loss_bps = parse_optional_float_env("STRATEGY_STOP_LOSS_BPS")
    hold_buffer_bps = abs(raw_hold_buffer_bps if raw_hold_buffer_bps is not None else DEFAULT_HOLD_BUFFER_BPS)
    take_profit_bps = abs(raw_take_profit_bps if raw_take_profit_bps is not None else DEFAULT_TAKE_PROFIT_BPS)
    stop_loss_bps = -abs(raw_stop_loss_bps if raw_stop_loss_bps is not None else DEFAULT_STOP_LOSS_BPS)
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
    raw_entry_spread_bps = os.getenv("STRATEGY_ENTRY_SPREAD_BPS")
    if raw_entry_spread_bps in (None, ""):
        raw_entry_spread_bps = os.getenv("STRATEGY_MIN_EXPECTED_NET_EDGE_BPS", "0.0")

    config = StrategyConfig(
        exchange_names=exchange_names,
        symbol_input=symbol_input,
        allowed_pairs=parse_allowed_pairs(os.getenv("STRATEGY_ALLOWED_PAIRS"), exchange_names),
        leverage=parse_positive_int_env("STRATEGY_LEVERAGE", 1),
        margin_mode=os.getenv("STRATEGY_MARGIN_MODE", "cross").strip().lower(),
        order_notional_usd=float(os.getenv("STRATEGY_ORDER_NOTIONAL_USD", "100") or 100.0),
        scan_interval_sec=float(os.getenv("STRATEGY_SCAN_INTERVAL_SEC", "1.0") or 1.0),
        first_leg_timeout_sec=float(os.getenv("STRATEGY_FIRST_LEG_TIMEOUT_SEC", "2.0") or 2.0),
        hedge_market_immediate=os.getenv("STRATEGY_HEDGE_MARKET_IMMEDIATE", "1").strip().lower() in {"1", "true", "yes", "on"},
        hedge_requote_timeout_sec=float(os.getenv("STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC", "0.7") or 0.7),
        hedge_requote_count=int(os.getenv("STRATEGY_HEDGE_REQUOTE_COUNT", "2") or 2),
        close_first_leg_role=os.getenv("STRATEGY_CLOSE_FIRST_LEG_ROLE", "short").strip().lower(),
        close_execution_mode=os.getenv("STRATEGY_CLOSE_EXECUTION_MODE", "safe_taker").strip().lower(),
        close_maker_timeout_sec=float(os.getenv("STRATEGY_CLOSE_MAKER_TIMEOUT_SEC", "2.0") or 2.0),
        close_maker_price_bps=float(os.getenv("STRATEGY_CLOSE_MAKER_PRICE_BPS", "0.0") or 0.0),
        close_maker_reasons=parse_csv(os.getenv("STRATEGY_CLOSE_MAKER_REASONS", "close_candidate,take_profit")),
        close_aggressive_bps=float(os.getenv("STRATEGY_CLOSE_AGGRESSIVE_BPS", "8.0") or 8.0),
        emergency_flatten_bps=float(os.getenv("STRATEGY_EMERGENCY_FLATTEN_BPS", "20.0") or 20.0),
        entry_first_leg_price_mode=os.getenv("STRATEGY_ENTRY_FIRST_LEG_PRICE_MODE", "passive").strip().lower(),
        entry_first_leg_marketable_bps=float(os.getenv("STRATEGY_ENTRY_FIRST_LEG_MARKETABLE_BPS", "0.0") or 0.0),
        entry_spread_bps=float(raw_entry_spread_bps or 0.0),
        entry_cancel_buffer_bps=float(os.getenv("STRATEGY_ENTRY_CANCEL_BUFFER_BPS", "0.0") or 0.0),
        action_settle_wait_sec=float(os.getenv("STRATEGY_ACTION_SETTLE_WAIT_SEC", "0.6") or 0.6),
        entry_order_settle_sec=float(os.getenv("STRATEGY_ENTRY_ORDER_SETTLE_SEC", "1.2") or 1.2),
        entry_order_cooldown_sec=float(os.getenv("STRATEGY_ENTRY_ORDER_COOLDOWN_SEC", "1.0") or 1.0),
        symbol_recent_close_cooldown_sec=float(
            os.getenv("STRATEGY_SYMBOL_RECENT_CLOSE_COOLDOWN_SEC", "3.0") or 3.0
        ),
        min_recompute_interval_sec=float(os.getenv("STRATEGY_MIN_RECOMPUTE_INTERVAL_SEC", "0.2") or 0.2),
        min_expected_net_edge_usd=float(os.getenv("STRATEGY_MIN_EXPECTED_NET_EDGE_USD", "0.0") or 0.0),
        min_expected_net_edge_bps=float(os.getenv("STRATEGY_MIN_EXPECTED_NET_EDGE_BPS", "0.0") or 0.0),
        min_effective_funding_usd=(
            float(raw_min_effective_funding_usd or 0.0)
            if raw_min_effective_funding_usd not in (None, "")
            else None
        ),
        require_funding_payment=parse_bool(os.getenv("STRATEGY_REQUIRE_FUNDING_PAYMENT"), False),
        hold_buffer_usd=float(os.getenv("STRATEGY_HOLD_BUFFER_USD", "0.0") or 0.0),
        hold_buffer_bps=hold_buffer_bps,
        min_hold_sec=float(os.getenv("STRATEGY_MIN_HOLD_SEC", "300.0") or 300.0),
        close_candidate_min_age_sec=float(
            os.getenv("STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC", "1800.0") or 1800.0
        ),
        max_hold_sec=float(os.getenv("STRATEGY_MAX_HOLD_SEC", "14400.0") or 14400.0),
        take_profit_usd=float(os.getenv("STRATEGY_TAKE_PROFIT_USD", "0.10") or 0.10),
        stop_loss_usd=float(os.getenv("STRATEGY_STOP_LOSS_USD", "-0.25") or -0.25),
        take_profit_bps=take_profit_bps,
        stop_loss_bps=stop_loss_bps,
        position_stale_ignore_sec=float(os.getenv("STRATEGY_POSITION_STALE_IGNORE_SEC", "20.0") or 20.0),
        close_slippage_bps=float(os.getenv("STRATEGY_CLOSE_SLIPPAGE_BPS", "0.0") or 0.0),
        risk_buffer_bps=float(os.getenv("STRATEGY_RISK_BUFFER_BPS", "1.0") or 1.0),
        margin_guard_enabled=parse_bool(os.getenv("STRATEGY_MARGIN_GUARD_ENABLED"), True),
        margin_guard_safety_factor=float(os.getenv("STRATEGY_MARGIN_GUARD_SAFETY_FACTOR", "1.02") or 1.02),
        margin_guard_extra_usd=float(os.getenv("STRATEGY_MARGIN_GUARD_EXTRA_USD", "5.0") or 5.0),
        startup_margin_mode_guard_enabled=parse_bool(
            os.getenv("STRATEGY_MARGIN_MODE_GUARD_ENABLED", os.getenv("STRATEGY_STARTUP_MARGIN_MODE_GUARD_ENABLED")),
            True,
        ),
        max_gross_leverage=float(os.getenv("STRATEGY_MAX_GROSS_LEVERAGE", "0") or 0.0),
        max_long_leverage=float(os.getenv("STRATEGY_MAX_LONG_LEVERAGE", "0") or 0.0),
        max_short_leverage=float(os.getenv("STRATEGY_MAX_SHORT_LEVERAGE", "0") or 0.0),
        rate_limit_guard_enabled=parse_bool(os.getenv("STRATEGY_RATE_LIMIT_GUARD_ENABLED"), True),
        rate_limit_guard_usage_threshold=float(os.getenv("STRATEGY_RATE_LIMIT_USAGE_THRESHOLD", "0.85") or 0.85),
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
        micro_residual_info_notional_usd=float(
            os.getenv("STRATEGY_MICRO_RESIDUAL_INFO_NOTIONAL_USD", "50.0") or 50.0
        ),
        micro_residual_info_ratio=float(os.getenv("STRATEGY_MICRO_RESIDUAL_INFO_RATIO", "0.02") or 0.02),
        action_account_fresh_sec=float(os.getenv("STRATEGY_ACTION_ACCOUNT_FRESH_SEC", "2.5") or 2.5),
        account_imbalance_hedge_enabled=parse_bool(os.getenv("STRATEGY_ACCOUNT_IMBALANCE_HEDGE_ENABLED"), True),
        account_imbalance_cooldown_sec=float(os.getenv("STRATEGY_ACCOUNT_IMBALANCE_COOLDOWN_SEC", "60.0") or 60.0),
        graceful_shutdown_enabled=parse_bool(os.getenv("STRATEGY_GRACEFUL_SHUTDOWN_ENABLED"), True),
        graceful_shutdown_cancel_orders=parse_bool(os.getenv("STRATEGY_GRACEFUL_SHUTDOWN_CANCEL_ORDERS"), True),
        graceful_shutdown_flatten_positions=parse_bool(os.getenv("STRATEGY_GRACEFUL_SHUTDOWN_FLATTEN_POSITIONS"), True),
        entry_control_file=Path(
            os.getenv("STRATEGY_ENTRY_CONTROL_FILE", str(PROJECT_ROOT / "runtime" / "entry_control.json"))
        ),
        entry_control_fail_closed=parse_bool(os.getenv("STRATEGY_ENTRY_CONTROL_FAIL_CLOSED"), True),
        live_event_log_path=Path(
            os.getenv("STRATEGY_LIVE_EVENT_LOG_PATH", str(PROJECT_ROOT / "logs" / "strategy_live.jsonl"))
        ),
        entry_fee_bps=entry_fee_bps,
        close_fee_bps=close_fee_bps,
        first_leg_role=os.getenv("STRATEGY_FIRST_LEG_ROLE", "long").strip().lower(),
        stdout_log_style=os.getenv("STRATEGY_STDOUT_LOG_STYLE", "verbose").strip().lower(),
    )
    if not config.allowed_pairs:
        raise ValueError("no allowed pairs remain after filtering STRATEGY_ALLOWED_PAIRS")
    if config.first_leg_role not in {"long", "short"}:
        raise ValueError("STRATEGY_FIRST_LEG_ROLE must be 'long' or 'short'")
    if config.close_first_leg_role not in {"long", "short"}:
        raise ValueError("STRATEGY_CLOSE_FIRST_LEG_ROLE must be 'long' or 'short'")
    if config.close_execution_mode not in {"safe_taker", "maker_then_taker"}:
        raise ValueError("STRATEGY_CLOSE_EXECUTION_MODE must be 'safe_taker' or 'maker_then_taker'")
    if config.close_maker_timeout_sec < 0:
        raise ValueError("STRATEGY_CLOSE_MAKER_TIMEOUT_SEC must be >= 0")
    if config.close_maker_price_bps < 0:
        raise ValueError("STRATEGY_CLOSE_MAKER_PRICE_BPS must be >= 0")
    if config.entry_first_leg_price_mode not in {"passive", "marketable", "spread_buffer"}:
        raise ValueError("STRATEGY_ENTRY_FIRST_LEG_PRICE_MODE must be 'passive', 'marketable', or 'spread_buffer'")
    if config.entry_first_leg_marketable_bps < 0:
        raise ValueError("STRATEGY_ENTRY_FIRST_LEG_MARKETABLE_BPS must be >= 0")
    if config.entry_spread_bps < 0 or config.entry_cancel_buffer_bps < 0:
        raise ValueError("STRATEGY_ENTRY_SPREAD_BPS and STRATEGY_ENTRY_CANCEL_BUFFER_BPS must be >= 0")
    if config.stdout_log_style not in {"verbose", "simple"}:
        raise ValueError("STRATEGY_STDOUT_LOG_STYLE must be 'verbose' or 'simple'")
    if config.max_gross_leverage < 0 or config.max_long_leverage < 0 or config.max_short_leverage < 0:
        raise ValueError("STRATEGY_MAX_*_LEVERAGE values must be >= 0")
    return config
