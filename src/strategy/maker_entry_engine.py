from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from src.strategy.models import OpenPairState, PairCandidate, StrategyConfig


EPS = 1e-9
RATE_LIMIT_PAUSE_MS = 60_000
QUEUE_LOG_INTERVAL_MS = 60_000
TRANSIENT_ERROR_MARKERS = (
    "503",
    "50001",
    "50013",
    "service temporarily unavailable",
    "temporarily unavailable",
    "http_503",
)
SOFT_PREPOSITION_REASONS = {
    "net_edge_below_usd_threshold",
    "net_edge_below_bps_threshold",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


@dataclass
class MakerOrderState:
    candidate: PairCandidate
    first_exchange_name: str
    first_side: str
    hedge_exchange_name: str
    hedge_side: str
    order_result: dict[str, Any] | None
    order_id: str | None
    target_qty: float
    price: float
    pre_qty_first: float
    pre_qty_hedge: float
    submitted_ms: int
    disappeared_ms: int = 0


@dataclass(frozen=True)
class MakerEntryHooks:
    first_leg_plan: Callable[[PairCandidate, StrategyConfig], tuple[str, str]]
    hedge_leg_plan: Callable[[PairCandidate, StrategyConfig], tuple[str, str]]
    target_price: Callable[[PairCandidate, str, str, dict[str, Any], StrategyConfig], float]
    hedge_price_preview: Callable[[Any, str, str, float], float]
    common_executable_qty: Callable[..., float]
    extract_position_qty: Callable[[dict[str, Any]], float]
    position_fill_delta: Callable[[float, float, str], float]
    extract_order_uuid: Callable[[dict[str, Any] | None], str | None]
    is_waiting_order_active: Callable[[Any, dict[str, Any] | None], bool]
    price_changed: Callable[[Any, str, float, float, StrategyConfig], bool]
    cancel_if_open: Callable[[Any, str, dict[str, Any] | None], Awaitable[None]]
    bootstrap_candidate: Callable[[PairCandidate, dict[str, Any], StrategyConfig], Awaitable[list[dict[str, Any]]]]
    complete_after_first_fill: Callable[..., Awaitable[OpenPairState | None]]
    record_event: Callable[..., None]


class MakerEntryEngine:
    """Question4-style limited maker entry engine.

    This engine only owns the pre-posted maker order. Once it sees a fill, it
    delegates hedge/pair construction to strategy hooks so recovery, DB, and
    shutdown behavior stay shared.
    """

    def __init__(self, hooks: MakerEntryHooks):
        self.hooks = hooks
        self.active: MakerOrderState | None = None
        self.last_submit_ms_by_key: dict[tuple[str, str, str], int] = {}
        self.paused_until_ms_by_symbol: dict[str, int] = {}
        self.last_queue_log_signature: tuple[Any, ...] | None = None
        self.last_queue_log_ms = 0

    def managed_symbols(self, config: StrategyConfig, symbols: list[str]) -> set[str]:
        if not config.maker_entry_engine_enabled:
            return set()
        configured = {symbol.upper() for symbol in config.maker_entry_engine_symbols if symbol}
        if "ALL" in configured:
            return {symbol.upper() for symbol in symbols}
        if configured:
            return configured
        return {symbol.upper() for symbol in symbols[:1]}

    def is_managed_symbol(self, symbol: str, config: StrategyConfig, symbols: list[str]) -> bool:
        return symbol.upper() in self.managed_symbols(config, symbols)

    def active_symbols(self) -> set[str]:
        if self.active is None:
            return set()
        return {self.active.candidate.symbol.upper()}

    async def cancel_active(self, exchanges: dict[str, Any], symbol: str, reason: str, config: StrategyConfig) -> None:
        state = self.active
        if state is None:
            return
        if "rate_limit_pressure" in reason:
            self._pause_symbol(symbol, reason)
        elif "stale_orderbook" in reason:
            self._pause_symbol_for_ms(
                symbol,
                reason,
                int(max(config.maker_entry_engine_stale_pause_sec, 0.0) * 1000),
            )
        exchange = exchanges[state.first_exchange_name]
        await self.hooks.cancel_if_open(exchange, symbol, state.order_result)
        self.hooks.record_event(
            "maker_entry_order_cancelled",
            candidate_id=state.candidate.id,
            symbol=symbol,
            exchange=state.first_exchange_name,
            side=state.first_side,
            order_id=state.order_id,
            price=state.price,
            qty=state.target_qty,
            reason=reason,
        )
        self.active = None

    def _pause_symbol(self, symbol: str, reason: str) -> None:
        self._pause_symbol_for_ms(symbol, reason, RATE_LIMIT_PAUSE_MS)

    def _pause_symbol_for_ms(self, symbol: str, reason: str, pause_ms: int) -> None:
        if pause_ms <= 0:
            return
        now_ms = int(time.time() * 1000)
        pause_until_ms = now_ms + pause_ms
        current_pause_until_ms = self.paused_until_ms_by_symbol.get(symbol.upper(), 0)
        if current_pause_until_ms > now_ms:
            return
        self.paused_until_ms_by_symbol[symbol.upper()] = pause_until_ms
        self.hooks.record_event(
            "maker_entry_symbol_paused",
            symbol=symbol,
            reason=reason,
            pause_until_ms=pause_until_ms,
            pause_ms=pause_ms,
        )

    def _symbol_pause_active(self, symbol: str) -> bool:
        now_ms = int(time.time() * 1000)
        pause_until_ms = self.paused_until_ms_by_symbol.get(symbol.upper(), 0)
        if pause_until_ms <= now_ms:
            self.paused_until_ms_by_symbol.pop(symbol.upper(), None)
            return False
        return True

    async def step(
        self,
        *,
        candidates: list[PairCandidate],
        open_pairs: dict[str, OpenPairState],
        exchanges: dict[str, Any],
        config: StrategyConfig,
        symbols: list[str],
        assumed_flat_until_ms: dict[tuple[str, str], int],
    ) -> list[OpenPairState]:
        if not config.maker_entry_engine_enabled:
            return []

        managed_symbols = self.managed_symbols(config, symbols)
        if not managed_symbols:
            return []

        opened: list[OpenPairState] = []
        if self.active is not None:
            pair = await self._reconcile_active_order(
                candidates=candidates,
                open_pairs=open_pairs,
                exchanges=exchanges,
                config=config,
                assumed_flat_until_ms=assumed_flat_until_ms,
            )
            if pair is not None:
                opened.append(pair)
            if self.active is not None:
                replacement = self._replacement_candidate(
                    candidates=candidates,
                    open_pairs=open_pairs,
                    managed_symbols=managed_symbols,
                    config=config,
                )
                if replacement is not None:
                    state = self.active
                    edge_improvement_bps = replacement.expected_net_edge_bps - self._candidate_score_bps(
                        self._current_candidate_for_state(candidates, state) or state.candidate
                    )
                    self.hooks.record_event(
                        "maker_entry_replacement_selected",
                        active_candidate_id=state.candidate.id,
                        active_symbol=state.candidate.symbol,
                        replacement_candidate_id=replacement.id,
                        replacement_symbol=replacement.symbol,
                        edge_improvement_bps=edge_improvement_bps,
                        replace_threshold_bps=config.maker_entry_engine_replace_edge_bps,
                    )
                    await self.cancel_active(
                        exchanges,
                        state.candidate.symbol,
                        f"better_candidate:{replacement.symbol}:{edge_improvement_bps:.2f}bps",
                        config,
                    )
            return opened

        if config.maker_entry_engine_max_open_orders <= 0:
            return opened
        if any(pair.symbol.upper() in managed_symbols and pair.status not in {"closed", "failed"} for pair in open_pairs.values()):
            return opened

        queue = self._candidate_queue(candidates, open_pairs, managed_symbols, config)
        candidate = queue[0] if queue else None
        if candidate is None:
            return opened

        self._record_queue_selection_if_needed(candidate, queue, managed_symbols)
        await self._submit_maker_order(candidate, exchanges, config)
        return opened

    def _record_queue_selection_if_needed(
        self,
        candidate: PairCandidate,
        queue: list[PairCandidate],
        managed_symbols: set[str],
    ) -> None:
        now_ms = int(time.time() * 1000)
        signature = tuple(
            (
                item.id,
                round(float(item.expected_net_edge_bps or 0.0), 4),
                round(float(item.entry_open_edge_bps or 0.0), 4),
            )
            for item in queue[:5]
        )
        if (
            signature == self.last_queue_log_signature
            and self.last_queue_log_ms + QUEUE_LOG_INTERVAL_MS > now_ms
        ):
            return
        self.last_queue_log_signature = signature
        self.last_queue_log_ms = now_ms
        self.hooks.record_event(
            "maker_entry_candidate_queue_selected",
            selected_candidate_id=candidate.id,
            selected_symbol=candidate.symbol,
            queue_size=len(queue),
            managed_symbol_count=len(managed_symbols),
            top_candidates=[
                {
                    "candidate_id": item.id,
                    "symbol": item.symbol,
                    "expected_net_edge_bps": item.expected_net_edge_bps,
                    "expected_open_edge_bps": item.entry_open_edge_bps,
                }
                for item in queue[:5]
            ],
        )

    def _candidate_queue(
        self,
        candidates: list[PairCandidate],
        open_pairs: dict[str, OpenPairState],
        managed_symbols: set[str],
        config: StrategyConfig,
    ) -> list[PairCandidate]:
        active_pair_symbols = {
            pair.symbol.upper()
            for pair in open_pairs.values()
            if pair.status not in {"closed", "failed"}
        }
        queue: list[PairCandidate] = []
        for candidate in candidates:
            symbol = candidate.symbol.upper()
            if symbol not in managed_symbols:
                continue
            if symbol in active_pair_symbols:
                continue
            if self._symbol_pause_active(symbol):
                continue
            if not self._candidate_allowed_for_preposition(candidate):
                continue
            if not self._candidate_near_edge_threshold(candidate, config):
                continue
            queue.append(candidate)
        return queue

    def _candidate_near_edge_threshold(self, candidate: PairCandidate, config: StrategyConfig) -> bool:
        if candidate.can_enter:
            return True
        min_queue_edge_bps = (
            float(config.min_expected_net_edge_bps)
            - float(config.maker_entry_engine_queue_edge_gap_bps)
        )
        return float(candidate.expected_net_edge_bps) >= min_queue_edge_bps

    def _replacement_candidate(
        self,
        *,
        candidates: list[PairCandidate],
        open_pairs: dict[str, OpenPairState],
        managed_symbols: set[str],
        config: StrategyConfig,
    ) -> PairCandidate | None:
        state = self.active
        if state is None:
            return None
        now_ms = int(time.time() * 1000)
        requote_ms = int(max(config.maker_entry_engine_requote_sec, 0.0) * 1000)
        if state.submitted_ms + requote_ms > now_ms:
            return None

        queue = self._candidate_queue(candidates, open_pairs, managed_symbols, config)
        if not queue:
            return None
        best = queue[0]
        if (
            best.symbol == state.candidate.symbol
            and best.long_exchange == state.candidate.long_exchange
            and best.short_exchange == state.candidate.short_exchange
        ):
            return None

        current = self._current_candidate_for_state(candidates, state) or state.candidate
        improvement_bps = self._candidate_score_bps(best) - self._candidate_score_bps(current)
        if improvement_bps < config.maker_entry_engine_replace_edge_bps:
            return None
        return best

    def _candidate_score_bps(self, candidate: PairCandidate) -> float:
        return float(candidate.expected_net_edge_bps)

    async def _submit_maker_order(
        self,
        candidate: PairCandidate,
        exchanges: dict[str, Any],
        config: StrategyConfig,
    ) -> None:
        first_exchange_name, first_side = self.hooks.first_leg_plan(candidate, config)
        hedge_exchange_name, hedge_side = self.hooks.hedge_leg_plan(candidate, config)
        first_exchange = exchanges[first_exchange_name]
        hedge_exchange = exchanges[hedge_exchange_name]

        now_ms = int(time.time() * 1000)
        submit_key = (first_exchange_name, candidate.symbol.upper(), first_side.lower())
        cooldown_ms = int(max(config.entry_order_cooldown_sec, config.maker_entry_engine_requote_sec, 0.0) * 1000)
        if self.last_submit_ms_by_key.get(submit_key, 0) + cooldown_ms > now_ms:
            return

        failures = await self.hooks.bootstrap_candidate(candidate, exchanges, config)
        if failures:
            self.hooks.record_event(
                "maker_entry_bootstrap_failed",
                candidate_id=candidate.id,
                symbol=candidate.symbol,
                first_exchange=first_exchange_name,
                hedge_exchange=hedge_exchange_name,
                failures=failures,
            )
            return

        first_price = self.hooks.target_price(candidate, first_exchange_name, first_side, exchanges, config)
        hedge_preview_price = self.hooks.hedge_price_preview(hedge_exchange, candidate.symbol, hedge_side, config.close_aggressive_bps)
        target_qty = self.hooks.common_executable_qty(
            first_exchange,
            hedge_exchange,
            candidate.symbol,
            candidate.executable_qty if candidate.executable_qty > EPS else candidate.target_qty,
            left_price=first_price,
            right_price=hedge_preview_price,
        )
        if first_price <= EPS or target_qty <= EPS:
            self.hooks.record_event(
                "maker_entry_order_skipped",
                candidate_id=candidate.id,
                symbol=candidate.symbol,
                reason="price_or_qty_missing",
                first_price=first_price,
                target_qty=target_qty,
            )
            return

        self.hooks.record_event(
            "maker_entry_preposition_candidate_selected",
            candidate_id=candidate.id,
            symbol=candidate.symbol,
            can_enter=candidate.can_enter,
            block_reasons=list(candidate.block_reasons),
            expected_net_edge_bps=candidate.expected_net_edge_bps,
            expected_open_edge_bps=candidate.entry_open_edge_bps,
            target_price=first_price,
            target_qty=target_qty,
        )

        pre_qty_first = self.hooks.extract_position_qty(first_exchange.get_symbol_snapshot(candidate.symbol))
        pre_qty_hedge = self.hooks.extract_position_qty(hedge_exchange.get_symbol_snapshot(candidate.symbol))
        order_result = await first_exchange.order(candidate.symbol, first_side, target_qty, first_price)
        if isinstance(order_result, dict) and order_result.get("error"):
            error_text = str(order_result.get("error") or order_result.get("msg") or order_result)
            if "-1003" in error_text or "rate" in error_text.lower() or "too many" in error_text.lower():
                self._pause_symbol(candidate.symbol, "submit_failed_rate_limit")
            elif self._is_transient_submit_error(error_text):
                self._pause_symbol_for_ms(
                    candidate.symbol,
                    "submit_failed_transient",
                    int(max(config.maker_entry_engine_transient_pause_sec, 0.0) * 1000),
                )
            self.hooks.record_event(
                "maker_entry_order_submit_failed",
                candidate_id=candidate.id,
                symbol=candidate.symbol,
                exchange=first_exchange_name,
                side=first_side,
                qty=target_qty,
                price=first_price,
                result=order_result,
            )
            return

        order_id = self.hooks.extract_order_uuid(order_result)
        self.last_submit_ms_by_key[submit_key] = now_ms
        self.active = MakerOrderState(
            candidate=candidate,
            first_exchange_name=first_exchange_name,
            first_side=first_side,
            hedge_exchange_name=hedge_exchange_name,
            hedge_side=hedge_side,
            order_result=order_result if isinstance(order_result, dict) else None,
            order_id=order_id,
            target_qty=target_qty,
            price=first_price,
            pre_qty_first=pre_qty_first,
            pre_qty_hedge=pre_qty_hedge,
            submitted_ms=now_ms,
        )
        self.hooks.record_event(
            "maker_entry_order_submitted",
            candidate_id=candidate.id,
            symbol=candidate.symbol,
            exchange=first_exchange_name,
            side=first_side,
            hedge_exchange=hedge_exchange_name,
            hedge_side=hedge_side,
            order_id=order_id,
            qty=target_qty,
            price=first_price,
            expected_net_edge_bps=candidate.expected_net_edge_bps,
            expected_open_edge_bps=candidate.entry_open_edge_bps,
        )

    def _is_transient_submit_error(self, error_text: str) -> bool:
        normalized = str(error_text or "").lower()
        return any(marker in normalized for marker in TRANSIENT_ERROR_MARKERS)

    async def _reconcile_active_order(
        self,
        *,
        candidates: list[PairCandidate],
        open_pairs: dict[str, OpenPairState],
        exchanges: dict[str, Any],
        config: StrategyConfig,
        assumed_flat_until_ms: dict[tuple[str, str], int],
    ) -> OpenPairState | None:
        state = self.active
        if state is None:
            return None
        symbol = state.candidate.symbol
        first_exchange = exchanges[state.first_exchange_name]

        if any(pair.symbol == symbol and pair.status not in {"closed", "failed"} for pair in open_pairs.values()):
            await self.cancel_active(exchanges, symbol, "symbol_pair_exists", config)
            return None

        post_qty_first = self.hooks.extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
        filled_qty = self.hooks.position_fill_delta(state.pre_qty_first, post_qty_first, state.first_side)
        if filled_qty > EPS:
            await self.hooks.cancel_if_open(first_exchange, symbol, state.order_result)
            self.hooks.record_event(
                "maker_entry_fill_detected",
                candidate_id=state.candidate.id,
                symbol=symbol,
                first_exchange=state.first_exchange_name,
                first_side=state.first_side,
                order_id=state.order_id,
                target_qty=state.target_qty,
                filled_qty=filled_qty,
                price=state.price,
            )
            self.active = None
            return await self.hooks.complete_after_first_fill(
                candidate=state.candidate,
                exchanges=exchanges,
                config=config,
                first_exchange_name=state.first_exchange_name,
                first_side=state.first_side,
                hedge_exchange_name=state.hedge_exchange_name,
                hedge_side=state.hedge_side,
                pre_qty_first=state.pre_qty_first,
                pre_qty_hedge=state.pre_qty_hedge,
                first_filled_qty=filled_qty,
                first_price_used=state.price,
                executable_qty=state.target_qty,
                assumed_flat_until_ms=assumed_flat_until_ms,
                entry_started_ms=state.submitted_ms,
                source="maker_entry_engine",
            )

        candidate = self._current_candidate_for_state(candidates, state)
        if candidate is None:
            await self.cancel_active(exchanges, symbol, "candidate_missing", config)
            return None
        hard_reasons = self._hard_blocking_reasons_excluding_own_order(candidate, state)
        if hard_reasons:
            await self.cancel_active(exchanges, symbol, "candidate_blocked:" + "|".join(hard_reasons[:3]), config)
            return None
        if "risk:rate_limit_pressure" in candidate.block_reasons:
            self._pause_symbol(symbol, "risk:rate_limit_pressure")
            return None

        active_on_exchange = self.hooks.is_waiting_order_active(first_exchange, state.order_result)
        now_ms = int(time.time() * 1000)
        if not active_on_exchange:
            if state.disappeared_ms <= 0:
                state.disappeared_ms = now_ms
                self.hooks.record_event(
                    "maker_entry_order_disappeared_settle_started",
                    candidate_id=state.candidate.id,
                    symbol=symbol,
                    exchange=state.first_exchange_name,
                    side=state.first_side,
                    order_id=state.order_id,
                )
                return None
            settle_ms = int(max(config.entry_order_settle_sec, 0.5) * 1000)
            if state.disappeared_ms + settle_ms > now_ms:
                return None
            self.hooks.record_event(
                "maker_entry_order_disappeared_without_fill",
                candidate_id=state.candidate.id,
                symbol=symbol,
                exchange=state.first_exchange_name,
                side=state.first_side,
                order_id=state.order_id,
            )
            self.active = None
            return None
        state.disappeared_ms = 0

        if state.submitted_ms + int(max(config.maker_entry_engine_requote_sec, 0.0) * 1000) > now_ms:
            return None
        if self._symbol_pause_active(symbol):
            return None

        next_price = self.hooks.target_price(candidate, state.first_exchange_name, state.first_side, exchanges, config)
        if next_price <= EPS:
            await self.cancel_active(exchanges, symbol, "target_price_missing", config)
            return None
        if self.hooks.price_changed(first_exchange, symbol, state.price, next_price, config):
            await self.cancel_active(exchanges, symbol, "requote", config)
        return None

    def _current_candidate_for_state(
        self,
        candidates: list[PairCandidate],
        state: MakerOrderState,
    ) -> PairCandidate | None:
        for candidate in candidates:
            if (
                candidate.symbol == state.candidate.symbol
                and candidate.long_exchange == state.candidate.long_exchange
                and candidate.short_exchange == state.candidate.short_exchange
            ):
                return candidate
        return None

    def _candidate_allowed_for_preposition(self, candidate: PairCandidate) -> bool:
        if candidate.can_enter:
            return True
        if candidate.executable_qty <= EPS or candidate.target_qty <= EPS:
            return False
        if candidate.long_ask <= EPS or candidate.short_bid <= EPS or candidate.mid_reference <= EPS:
            return False
        return not self._hard_blocking_reasons(candidate.block_reasons)

    def _hard_blocking_reasons(self, reasons: list[str]) -> list[str]:
        return [reason for reason in reasons if reason not in SOFT_PREPOSITION_REASONS]

    def _hard_blocking_reasons_excluding_own_order(
        self,
        candidate: PairCandidate,
        state: MakerOrderState,
    ) -> list[str]:
        own_order_reason = f"{state.first_exchange_name}:open_orders_exist"
        ignored = {
            own_order_reason,
            "risk:symbol_side_order_exists",
            "risk:rate_limit_pressure",
        }
        return [
            reason
            for reason in self._hard_blocking_reasons(candidate.block_reasons)
            if reason not in ignored
        ]
