from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]

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
DEFAULT_TAKER_FEE_BPS = {
    "binance": 5.0,
    "okx": 5.0,
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
    hedge_market_immediate: bool = True
    hedge_requote_timeout_sec: float = 0.7
    hedge_requote_count: int = 2
    close_first_leg_role: str = "short"
    close_execution_mode: str = "safe_taker"
    close_maker_timeout_sec: float = 2.0
    close_maker_price_bps: float = 0.0
    close_maker_reasons: list[str] = field(default_factory=lambda: ["close_candidate", "take_profit"])
    close_aggressive_bps: float = 8.0
    emergency_flatten_bps: float = 20.0
    entry_first_leg_price_mode: str = "passive"
    entry_first_leg_marketable_bps: float = 0.0
    entry_spread_bps: float = 0.0
    entry_cancel_buffer_bps: float = 0.0
    action_settle_wait_sec: float = 0.6
    entry_order_settle_sec: float = 1.2
    entry_order_cooldown_sec: float = 1.0
    symbol_recent_close_cooldown_sec: float = 3.0
    min_recompute_interval_sec: float = 0.2
    min_expected_net_edge_usd: float = 0.0
    min_expected_net_edge_bps: float = 0.0
    min_effective_funding_usd: float | None = None
    require_funding_payment: bool = False
    hold_buffer_usd: float = 0.0
    hold_buffer_bps: float | None = 2.0
    min_hold_sec: float = 300.0
    close_candidate_min_age_sec: float = 1800.0
    max_hold_sec: float = 14400.0
    take_profit_usd: float = 0.10
    stop_loss_usd: float = -0.25
    take_profit_bps: float | None = 6.0
    stop_loss_bps: float | None = -8.0
    position_stale_ignore_sec: float = 20.0
    close_slippage_bps: float = 0.0
    risk_buffer_bps: float = 1.0
    margin_guard_enabled: bool = True
    margin_guard_safety_factor: float = 1.02
    margin_guard_extra_usd: float = 5.0
    startup_margin_mode_guard_enabled: bool = True
    max_gross_leverage: float = 0.0
    max_long_leverage: float = 0.0
    max_short_leverage: float = 0.0
    rate_limit_guard_enabled: bool = True
    rate_limit_guard_usage_threshold: float = 0.85
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
    micro_residual_info_notional_usd: float = 50.0
    micro_residual_info_ratio: float = 0.02
    action_account_fresh_sec: float = 2.5
    account_imbalance_hedge_enabled: bool = True
    account_imbalance_cooldown_sec: float = 60.0
    graceful_shutdown_enabled: bool = True
    graceful_shutdown_cancel_orders: bool = True
    graceful_shutdown_flatten_positions: bool = True
    entry_control_file: Path = field(default_factory=lambda: PROJECT_ROOT / "runtime" / "entry_control.json")
    entry_control_fail_closed: bool = True
    live_event_log_path: Path = field(default_factory=lambda: PROJECT_ROOT / "logs" / "strategy_live.jsonl")
    entry_fee_bps: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_ENTRY_FEE_BPS))
    close_fee_bps: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_CLOSE_FEE_BPS))
    taker_fee_bps: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_TAKER_FEE_BPS))
    first_leg_role: str = "long"
    stdout_log_style: str = "verbose"


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


@dataclass
class FlattenLegResult:
    exchange_name: str
    symbol: str
    role: str
    side: str = ""
    requested_qty: float = 0.0
    start_qty: float = 0.0
    end_qty: float = 0.0
    closed_qty: float = 0.0
    ok: bool = False
    assumed_flat: bool = False
    attempted_order: bool = False
    failure_reason: str = ""
    error_code: str = ""
    error_message: str = ""
    temporarily_unavailable: bool = False


@dataclass
class PairFlattenResult:
    ok: bool
    forced_flat_exchanges: set[str] = field(default_factory=set)
    leg_results: list[FlattenLegResult] = field(default_factory=list)
    failed_leg: FlattenLegResult | None = None
    other_leg_action: str = "none"
    neutralized: bool = False
    neutralize_payload: dict[str, Any] = field(default_factory=dict)

    def __iter__(self):
        yield self.ok
        yield self.forced_flat_exchanges
