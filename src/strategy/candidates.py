import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable

from src.strategy.close_policy import close_fee_cost_usd
from src.strategy.entry_policy import evaluate_entry_policy
from src.strategy.funding_events import (
    estimate_pair_funding_events,
    legacy_prorated_funding_pnl_usd,
    normalize_funding_interval_hours,
    per_settlement_direction_pnl_usd,
)
from src.strategy.models import PairCandidate, StrategyConfig


EPS = 1e-9


@dataclass(frozen=True)
class CandidateBuildHooks:
    compute_common_executable_qty: Callable[..., float]
    exchange_min_order_notional_usd: Callable[..., float]
    effective_position_qty: Callable[..., float]
    extract_health_can_trade: Callable[..., bool]
    extract_health_block_reasons: Callable[..., list[str]]
    has_open_orders: Callable[..., bool]
    pair_entry_margin_guard_reasons: Callable[..., list[str]]


@dataclass(frozen=True)
class CandidateCollectionHooks:
    collect_balance_reports_by_exchange: Callable[..., dict[str, dict[str, Any]]]
    apply_open_pair_capacity_blocks: Callable[..., list[PairCandidate]]
    apply_recent_symbol_close_blocks: Callable[..., list[PairCandidate]]
    apply_risk_gate_blocks: Callable[..., list[PairCandidate]]
    candidate_build_hooks: CandidateBuildHooks


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _market_source_time_ms(source: dict[str, Any]) -> int:
    for key in ("desktop_time", "server_time", "event_time", "transaction_time"):
        try:
            value = int(source.get(key) or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    return 0


def _ordered_top_of_book_sources(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    orderbook = snapshot.get("orderbook", {}) or {}
    tick = snapshot.get("tick", {}) or {}
    sources = [source for source in (orderbook, tick) if isinstance(source, dict) and source]
    # Prefer the newest top-of-book source. This keeps maker quotes aligned with
    # the same data source that refreshed market-data health.
    return sorted(sources, key=_market_source_time_ms, reverse=True)


def extract_best_bid(snapshot: dict[str, Any]) -> float:
    for source in _ordered_top_of_book_sources(snapshot):
        for key in ("best_bid", "bid", "bid_price"):
            value = _safe_float(source.get(key), 0.0)
            if value > 0:
                return value
    return 0.0


def extract_best_ask(snapshot: dict[str, Any]) -> float:
    for source in _ordered_top_of_book_sources(snapshot):
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


def extract_funding_interval(snapshot: dict[str, Any]) -> tuple[int | None, str]:
    funding = snapshot.get("funding", {}) or {}
    interval = normalize_funding_interval_hours(funding.get("funding_interval_hours"))
    source = str(funding.get("funding_interval_source") or "unknown")
    return interval, source


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


FUNDING_SETTLEMENT_SEC = 28800.0


def compute_effective_funding_components(
    *,
    long_rate: float,
    short_rate: float,
    long_notional: float,
    short_notional: float,
    long_next_funding_ms: int,
    short_next_funding_ms: int,
    config: StrategyConfig,
    forecast_hold_sec: float,
    now_ms: int | None = None,
) -> tuple[float, float, float, float, float]:
    direction_pnl_usd = per_settlement_direction_pnl_usd(
        long_rate=long_rate,
        short_rate=short_rate,
        long_notional_usd=long_notional,
        short_notional_usd=short_notional,
    )
    raw_funding_pnl_usd = legacy_prorated_funding_pnl_usd(
        direction_pnl_usd=direction_pnl_usd,
        forecast_hold_sec=forecast_hold_sec,
        settlement_sec=FUNDING_SETTLEMENT_SEC,
    )
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


def compute_open_cost_usd(
    long_exchange: str,
    short_exchange: str,
    long_notional: float,
    short_notional: float,
    config: StrategyConfig,
) -> float:
    # Entry: first leg pays maker when posted passively (passive / spread_buffer)
    # and taker when crossed (marketable). Hedge leg is taker on
    # hedge_market_immediate, otherwise maker.
    if config.first_leg_role == "long":
        first_ex, first_notional = long_exchange, long_notional
        hedge_ex, hedge_notional = short_exchange, short_notional
    else:
        first_ex, first_notional = short_exchange, short_notional
        hedge_ex, hedge_notional = long_exchange, long_notional
    if config.entry_first_leg_price_mode == "marketable":
        first_fee_bps = config.taker_fee_bps.get(first_ex, 0.0)
    else:
        first_fee_bps = config.entry_fee_bps.get(first_ex, 0.0)
    if config.hedge_market_immediate:
        hedge_fee_bps = config.taker_fee_bps.get(hedge_ex, 0.0)
    else:
        hedge_fee_bps = config.entry_fee_bps.get(hedge_ex, 0.0)
    return (
        first_notional * first_fee_bps / 10000.0
        + hedge_notional * hedge_fee_bps / 10000.0
    )


def compute_expected_close_cost_usd(
    long_exchange: str,
    short_exchange: str,
    reference_notional: float,
    config: StrategyConfig,
) -> float:
    close_fee_cost = close_fee_cost_usd(
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        long_notional_usd=reference_notional,
        short_notional_usd=reference_notional,
        config=config,
    )
    close_slippage_cost_usd = reference_notional * (config.close_slippage_bps * 2.0) / 10000.0
    return close_fee_cost + close_slippage_cost_usd


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
    hooks: CandidateBuildHooks,
    balance_reports_by_exchange: dict[str, dict[str, Any]] | None = None,
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
    executable_qty = hooks.compute_common_executable_qty(
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
    long_min_order_notional_usd = hooks.exchange_min_order_notional_usd(long_exchange_obj, symbol, long_ask)
    short_min_order_notional_usd = hooks.exchange_min_order_notional_usd(short_exchange_obj, symbol, short_bid)

    funding_long_rate = extract_funding_rate(long_snapshot)
    funding_short_rate = extract_funding_rate(short_snapshot)
    long_next_funding_ms = extract_next_funding_ms(long_snapshot)
    short_next_funding_ms = extract_next_funding_ms(short_snapshot)
    long_funding_interval_hours, long_funding_interval_source = extract_funding_interval(long_snapshot)
    short_funding_interval_hours, short_funding_interval_source = extract_funding_interval(short_snapshot)
    current_ms = int(time.time() * 1000)

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
        legacy_prorated_funding_usd,
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
        forecast_hold_sec=config.entry_funding_forecast_sec,
        now_ms=current_ms,
    )
    funding_direction_pnl_usd = per_settlement_direction_pnl_usd(
        long_rate=funding_long_rate,
        short_rate=funding_short_rate,
        long_notional_usd=notional_long,
        short_notional_usd=notional_short,
    )
    entry_complete_ms = current_ms + int(max(config.funding_event_entry_buffer_sec, 0.0) * 1000)
    event_estimate = estimate_pair_funding_events(
        long_rate=funding_long_rate,
        short_rate=funding_short_rate,
        long_notional_usd=notional_long,
        short_notional_usd=notional_short,
        long_next_funding_ms=long_next_funding_ms,
        short_next_funding_ms=short_next_funding_ms,
        long_interval_hours=long_funding_interval_hours,
        short_interval_hours=short_funding_interval_hours,
        long_interval_source=long_funding_interval_source,
        short_interval_source=short_funding_interval_source,
        entry_complete_ms=entry_complete_ms,
        forecast_end_ms=current_ms + int(max(config.max_hold_sec, 0.0) * 1000),
    )
    # Entry duration is unknown. Keep the next-event cashflow for shadow
    # observability, but do not add or subtract it from entry expected edge.
    event_funding_pnl_usd = 0.0
    selected_funding_pnl_usd = (
        event_funding_pnl_usd
        if config.funding_event_model_enabled
        else effective_funding_pnl_usd
    )
    stale_data_penalty_usd = compute_stale_data_penalty_usd(
        reference_notional,
        funding_confidence,
        funding_alignment_factor,
        funding_time_decay_factor,
        config,
    )

    cost_without_funding_usd = (
        entry_open_edge_usd
        - open_fee_cost_usd
        - expected_close_cost_usd
        - risk_buffer_usd
        - stale_data_penalty_usd
    )
    legacy_expected_net_edge_usd = cost_without_funding_usd + effective_funding_pnl_usd
    event_expected_net_edge_usd = cost_without_funding_usd + event_funding_pnl_usd
    expected_net_edge_usd = cost_without_funding_usd + selected_funding_pnl_usd
    expected_net_edge_bps = (expected_net_edge_usd / reference_notional * 10000.0) if reference_notional > 0 else 0.0
    legacy_expected_net_edge_bps = (
        legacy_expected_net_edge_usd / reference_notional * 10000.0 if reference_notional > 0 else 0.0
    )
    event_expected_net_edge_bps = (
        event_expected_net_edge_usd / reference_notional * 10000.0 if reference_notional > 0 else 0.0
    )

    block_reasons: list[str] = []
    if long_ask <= 0:
        block_reasons.append(f"{long_exchange}:ask_missing")
    if short_bid <= 0:
        block_reasons.append(f"{short_exchange}:bid_missing")
    if not hooks.extract_health_can_trade(long_snapshot):
        reasons = hooks.extract_health_block_reasons(long_snapshot) or ["cannot_trade"]
        block_reasons.append(f"{long_exchange}:{'|'.join(reasons)}")
    if not hooks.extract_health_can_trade(short_snapshot):
        reasons = hooks.extract_health_block_reasons(short_snapshot) or ["cannot_trade"]
        block_reasons.append(f"{short_exchange}:{'|'.join(reasons)}")
    if abs(hooks.effective_position_qty(long_snapshot, long_exchange_obj, symbol, config, assumed_flat_until_ms)) > EPS:
        block_reasons.append(f"{long_exchange}:existing_position")
    if abs(hooks.effective_position_qty(short_snapshot, short_exchange_obj, symbol, config, assumed_flat_until_ms)) > EPS:
        block_reasons.append(f"{short_exchange}:existing_position")
    if hooks.has_open_orders(long_snapshot):
        block_reasons.append(f"{long_exchange}:open_orders_exist")
    if hooks.has_open_orders(short_snapshot):
        block_reasons.append(f"{short_exchange}:open_orders_exist")
    if target_qty <= EPS:
        block_reasons.append("target_qty_zero")
    if executable_qty <= EPS:
        block_reasons.append("common_executable_qty_zero")
    block_reasons.extend(
        hooks.pair_entry_margin_guard_reasons(
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            long_exchange_obj=long_exchange_obj,
            short_exchange_obj=short_exchange_obj,
            long_notional_usd=notional_long,
            short_notional_usd=notional_short,
            config=config,
            balance_reports_by_exchange=balance_reports_by_exchange,
        )
    )
    legacy_entry_decision = evaluate_entry_policy(
        config=config,
        base_reasons=block_reasons,
        raw_funding_pnl_usd=legacy_prorated_funding_usd,
        funding_direction_pnl_usd=funding_direction_pnl_usd,
        effective_funding_pnl_usd=effective_funding_pnl_usd,
        expected_net_edge_usd=legacy_expected_net_edge_usd,
        expected_net_edge_bps=legacy_expected_net_edge_bps,
    )
    event_base_reasons = list(block_reasons)
    if not event_estimate.schedule_complete:
        event_base_reasons.append("funding_schedule_incomplete")
    entry_buffer_end_ms = current_ms + int(max(config.funding_event_entry_buffer_sec, 0.0) * 1000)
    if any(
        current_ms < funding_ms <= entry_buffer_end_ms
        for funding_ms in (long_next_funding_ms, short_next_funding_ms)
    ):
        event_base_reasons.append("funding_event_too_close")
    event_entry_decision = evaluate_entry_policy(
        config=config,
        base_reasons=event_base_reasons,
        raw_funding_pnl_usd=event_funding_pnl_usd,
        funding_direction_pnl_usd=funding_direction_pnl_usd,
        effective_funding_pnl_usd=event_funding_pnl_usd,
        expected_net_edge_usd=event_expected_net_edge_usd,
        expected_net_edge_bps=event_expected_net_edge_bps,
    )
    entry_decision = event_entry_decision if config.funding_event_model_enabled else legacy_entry_decision
    block_reasons = entry_decision.reasons
    can_enter = entry_decision.can_enter
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
        raw_funding_pnl_usd=selected_funding_pnl_usd,
        effective_funding_pnl_usd=selected_funding_pnl_usd,
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
        funding_direction_pnl_usd=funding_direction_pnl_usd,
        legacy_prorated_funding_pnl_usd=legacy_prorated_funding_usd,
        funding_event_raw_pnl_usd=event_estimate.pnl_usd,
        funding_event_pnl_usd=event_funding_pnl_usd,
        funding_event_count=event_estimate.event_count,
        funding_event_next_time_ms=event_estimate.next_event_time_ms,
        funding_event_schedule_complete=event_estimate.schedule_complete,
        funding_event_model_enabled=config.funding_event_model_enabled,
        long_funding_interval_hours=long_funding_interval_hours,
        short_funding_interval_hours=short_funding_interval_hours,
        long_funding_interval_source=long_funding_interval_source,
        short_funding_interval_source=short_funding_interval_source,
        legacy_expected_net_edge_usd=legacy_expected_net_edge_usd,
        legacy_expected_net_edge_bps=legacy_expected_net_edge_bps,
        event_expected_net_edge_usd=event_expected_net_edge_usd,
        event_expected_net_edge_bps=event_expected_net_edge_bps,
        legacy_can_enter=legacy_entry_decision.can_enter,
        event_can_enter=event_entry_decision.can_enter,
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


def normalize_candidate_block_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    if not normalized:
        return "unknown_block_reason"
    if "(" in normalized:
        normalized = normalized.split("(", 1)[0]
    return normalized


def candidate_scan_item(candidate: PairCandidate | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    return {
        "candidate_id": candidate.id,
        "symbol": candidate.symbol,
        "long_exchange": candidate.long_exchange,
        "short_exchange": candidate.short_exchange,
        "pair": f"{candidate.long_exchange}L/{candidate.short_exchange}S",
        "qty": candidate.executable_qty,
        "notional_usd": candidate.notional_usd,
        "expected_net_edge_usd": candidate.expected_net_edge_usd,
        "expected_net_edge_bps": candidate.expected_net_edge_bps,
        "effective_funding_pnl_usd": candidate.effective_funding_pnl_usd,
        "funding_model": "event" if candidate.funding_event_model_enabled else "legacy_prorated",
        "funding_direction_pnl_usd": candidate.funding_direction_pnl_usd,
        "legacy_prorated_funding_pnl_usd": candidate.legacy_prorated_funding_pnl_usd,
        "funding_event_raw_pnl_usd": candidate.funding_event_raw_pnl_usd,
        "funding_event_pnl_usd": candidate.funding_event_pnl_usd,
        "funding_event_count": candidate.funding_event_count,
        "funding_event_next_time_ms": candidate.funding_event_next_time_ms,
        "legacy_expected_net_edge_bps": candidate.legacy_expected_net_edge_bps,
        "event_expected_net_edge_bps": candidate.event_expected_net_edge_bps,
        "legacy_can_enter": candidate.legacy_can_enter,
        "event_can_enter": candidate.event_can_enter,
        "funding_shadow_admission_changed": candidate.legacy_can_enter != candidate.event_can_enter,
        "can_enter": candidate.can_enter,
        "block_reasons": list(candidate.block_reasons[:5]),
        "normalized_block_reasons": [
            normalize_candidate_block_reason(reason)
            for reason in candidate.block_reasons[:5]
        ],
    }


def build_candidate_scan_summary(candidates: list[PairCandidate]) -> dict[str, Any]:
    total = len(candidates)
    enterable = [candidate for candidate in candidates if candidate.can_enter]
    blocked = [candidate for candidate in candidates if not candidate.can_enter]
    legacy_enterable = [candidate for candidate in candidates if candidate.legacy_can_enter]
    event_enterable = [candidate for candidate in candidates if candidate.event_can_enter]
    funding_shadow_changed = [
        candidate
        for candidate in candidates
        if candidate.legacy_can_enter != candidate.event_can_enter
    ]

    reason_counter: Counter[str] = Counter()
    for candidate in blocked:
        reasons = candidate.block_reasons or ["blocked_without_reason"]
        for reason in reasons:
            reason_counter[normalize_candidate_block_reason(reason)] += 1

    top_candidate = candidates[0] if candidates else None
    top_enterable = enterable[0] if enterable else None
    top_blocked = blocked[0] if blocked else None

    return {
        "total_candidates": total,
        "enterable_candidates": len(enterable),
        "blocked_candidates": len(blocked),
        "legacy_enterable_candidates": len(legacy_enterable),
        "event_enterable_candidates": len(event_enterable),
        "funding_shadow_changed_candidates": len(funding_shadow_changed),
        "top_candidate": candidate_scan_item(top_candidate),
        "top_enterable_candidate": candidate_scan_item(top_enterable),
        "top_blocked_candidate": candidate_scan_item(top_blocked),
        "top_block_reason": reason_counter.most_common(1)[0][0] if reason_counter else "",
        "reason_counts": dict(reason_counter.most_common(12)),
    }


def collect_pair_candidates(
    symbols: list[str],
    exchanges: dict[str, Any],
    config: StrategyConfig,
    hooks: CandidateCollectionHooks,
    open_pairs: dict[str, Any] | None = None,
    entry_control_store: Any | None = None,
    assumed_flat_until_ms: dict[tuple[str, str], int] | None = None,
    symbol_recent_close_ms: dict[str, int] | None = None,
    recent_close_block_logged_ms: dict[str, int] | None = None,
    startup_block_reasons: tuple[str, ...] = (),
    manual_locked_symbols: set[str] | None = None,
) -> list[PairCandidate]:
    snapshots_by_exchange: dict[str, dict[str, Any]] = {
        exchange_name: {symbol: exchange.get_symbol_snapshot(symbol) for symbol in symbols}
        for exchange_name, exchange in exchanges.items()
    }
    balance_reports_by_exchange = hooks.collect_balance_reports_by_exchange(exchanges)
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
                    hooks.candidate_build_hooks,
                    balance_reports_by_exchange=balance_reports_by_exchange,
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
                    hooks.candidate_build_hooks,
                    balance_reports_by_exchange=balance_reports_by_exchange,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                )
            )

    ranked = rank_candidates(candidates)
    if open_pairs is not None:
        ranked = hooks.apply_open_pair_capacity_blocks(ranked, open_pairs, config)
    ranked = hooks.apply_recent_symbol_close_blocks(
        ranked,
        exchanges,
        config,
        symbol_recent_close_ms,
        recent_close_block_logged_ms,
        assumed_flat_until_ms,
    )
    return hooks.apply_risk_gate_blocks(
        ranked,
        open_pairs,
        exchanges,
        config,
        entry_control_store,
        balance_reports_by_exchange,
        startup_block_reasons,
        manual_locked_symbols,
    )
