from __future__ import annotations

import time
import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from src.core.action_gate import ActionGate
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


def _price_move_bps(previous_price: float, target_price: float) -> float:
    if previous_price <= EPS or target_price <= EPS:
        return float("inf")
    return abs(target_price - previous_price) / previous_price * 10000.0


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
    intent_id: str = ""
    disappeared_ms: int = 0
    cancel_retry_after_ms: int = 0


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
    cancel_if_open: Callable[[Any, str, dict[str, Any] | None], Awaitable[Any]]
    bootstrap_candidate: Callable[[PairCandidate, dict[str, Any], StrategyConfig], Awaitable[list[dict[str, Any]]]]
    complete_after_first_fill: Callable[..., Awaitable[OpenPairState | None]]
    record_event: Callable[..., None]


class MakerEntryEngine:
    """Limited maker entry engine.

    This engine only owns the pre-posted maker order. Once it sees a fill, it
    delegates hedge/pair construction to strategy hooks so recovery, DB, and
    shutdown behavior stay shared.
    """

    def __init__(self, hooks: MakerEntryHooks, *, action_gate: ActionGate | None = None):
        self.hooks = hooks
        self.action_gate = action_gate
        self.active: MakerOrderState | None = None
        self.last_submit_ms_by_key: dict[tuple[str, str, str], int] = {}
        self.paused_until_ms_by_symbol: dict[str, int] = {}
        self.last_queue_log_signature: tuple[Any, ...] | None = None
        self.last_queue_log_ms = 0

    def managed_symbols(self, config: StrategyConfig, symbols: list[str]) -> set[str]:
        if not config.maker_entry_engine_enabled:
            return set()
        return {symbol.upper() for symbol in symbols}

    def is_managed_symbol(self, symbol: str, config: StrategyConfig, symbols: list[str]) -> bool:
        return symbol.upper() in self.managed_symbols(config, symbols)

    def active_symbols(self) -> set[str]:
        if self.active is None:
            return set()
        return {self.active.candidate.symbol.upper()}

    def _release_state_lease(self, state: MakerOrderState, note: str) -> None:
        if self.action_gate is None or not state.intent_id:
            return
        self.action_gate.release_symbol(
            owner_id=state.intent_id,
            symbol=state.candidate.symbol,
            note=note,
        )

    def _clear_active(self, state: MakerOrderState, note: str) -> None:
        if self.active is state:
            self.active = None
        self._release_state_lease(state, note)

    async def _complete_filled_state(
        self,
        *,
        state: MakerOrderState,
        exchanges: dict[str, Any],
        config: StrategyConfig,
        assumed_flat_until_ms: dict[tuple[str, str], int],
        filled_qty: float,
    ) -> OpenPairState | None:
        if self.active is state:
            self.active = None
        try:
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
                first_order_id=state.order_id,
                first_client_order_id=state.order_id,
            )
        finally:
            self._release_state_lease(state, "first_fill_completed")

    async def cancel_active(
        self,
        exchanges: dict[str, Any],
        symbol: str,
        reason: str,
        config: StrategyConfig,
        assumed_flat_until_ms: dict[tuple[str, str], int],
    ) -> OpenPairState | None:
        state = self.active
        if state is None:
            return None
        now_ms = int(time.time() * 1000)
        if state.cancel_retry_after_ms > now_ms:
            return None
        if "rate_limit_pressure" in reason:
            self._pause_symbol(symbol, reason)
        elif "stale_orderbook" in reason:
            self._pause_symbol_for_ms(
                symbol,
                reason,
                int(max(config.maker_entry_engine_stale_pause_sec, 0.0) * 1000),
            )
        exchange = exchanges[state.first_exchange_name]
        cancel_result = await self.hooks.cancel_if_open(exchange, symbol, state.order_result)
        if not self._cancel_result_is_terminal(cancel_result):
            retry_ms = self._cancel_retry_ms(cancel_result, config)
            state.cancel_retry_after_ms = int(time.time() * 1000) + retry_ms
            self.hooks.record_event(
                "maker_entry_order_cancel_failed",
                candidate_id=state.candidate.id,
                symbol=symbol,
                exchange=state.first_exchange_name,
                side=state.first_side,
                order_id=state.order_id,
                price=state.price,
                qty=state.target_qty,
                reason=reason,
                cancel_result=cancel_result,
                retry_after_ms=state.cancel_retry_after_ms,
            )
            return None

        pair = await self._complete_if_fill_visible(
            state=state,
            exchanges=exchanges,
            config=config,
            assumed_flat_until_ms=assumed_flat_until_ms,
            reason=f"after_cancel:{reason}",
        )
        if pair is not None:
            return pair
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
            cancel_result=cancel_result,
        )
        self._clear_active(state, f"cancelled:{reason}")
        return None

    def _cancel_result_is_terminal(self, result: Any) -> bool:
        if not isinstance(result, dict):
            return result is not None
        error = str(result.get("error") or "").strip().lower()
        state = str(result.get("state") or result.get("status") or "").strip().lower()
        if state in {"not_exist", "canceled", "cancelled", "filled", "closed"}:
            return True
        if error in {"not_exist", "order_not_exist", "already_cancelled", "already_canceled"}:
            return True
        code = result.get("code")
        if code in (-2011, -2013, "-2011", "-2013"):
            return True
        text = str(result or "").lower()
        if "51400" in text or "filled, canceled or does not exist" in text:
            return True
        # An ACK-shaped payload without an explicit terminal state is UNKNOWN.
        # Keep the maker lease and reconcile instead of allowing a replacement.
        return False

    def _cancel_retry_ms(self, result: Any, config: StrategyConfig) -> int:
        text = str(result or "").lower()
        if "cancel_cooldown" in text:
            return 1_000
        if self._is_transient_submit_error(text):
            return int(max(config.maker_entry_engine_transient_pause_sec, 1.0) * 1000)
        if "rate" in text or "-1003" in text or "too many" in text:
            return RATE_LIMIT_PAUSE_MS
        return 2_000

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
        manual_locked_symbols: set[str] | None = None,
    ) -> list[OpenPairState]:
        if not config.maker_entry_engine_enabled:
            return []

        managed_symbols = self.managed_symbols(config, symbols)
        manual_locked_symbols = {symbol.upper() for symbol in (manual_locked_symbols or set())}
        if not managed_symbols:
            return []

        opened: list[OpenPairState] = []
        if self.active is not None:
            if self.action_gate is not None and self.active.intent_id:
                self.action_gate.touch_symbol(
                    owner_id=self.active.intent_id,
                    symbol=self.active.candidate.symbol,
                    note="maker_order_active",
                )
            if self.active.candidate.symbol.upper() in manual_locked_symbols:
                cancelled_pair = await self.cancel_active(
                    exchanges,
                    self.active.candidate.symbol,
                    "manual_position_locked",
                    config,
                    assumed_flat_until_ms,
                )
                if cancelled_pair is not None:
                    opened.append(cancelled_pair)
                return opened
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
                    manual_locked_symbols=manual_locked_symbols,
                    config=config,
                )
                if replacement is not None:
                    state = self.active
                    current = self._current_candidate_for_state(candidates, state) or state.candidate
                    edge_improvement_bps = replacement.expected_net_edge_bps - self._candidate_score_bps(
                        current
                    )
                    self.hooks.record_event(
                        "maker_entry_replacement_selected",
                        active_candidate_id=state.candidate.id,
                        active_symbol=state.candidate.symbol,
                        replacement_candidate_id=replacement.id,
                        replacement_symbol=replacement.symbol,
                        edge_improvement_bps=edge_improvement_bps,
                        replace_threshold_bps=config.maker_entry_engine_replace_edge_bps,
                        current_expected_net_edge_bps=current.expected_net_edge_bps,
                        replacement_expected_net_edge_bps=replacement.expected_net_edge_bps,
                    )
                    cancelled_pair = await self.cancel_active(
                        exchanges,
                        state.candidate.symbol,
                        f"better_candidate:{replacement.symbol}:{edge_improvement_bps:.2f}bps",
                        config,
                        assumed_flat_until_ms,
                    )
                    if cancelled_pair is not None:
                        opened.append(cancelled_pair)
            return opened

        if config.maker_entry_engine_max_open_orders <= 0:
            return opened
        active_pair_count = sum(
            1 for pair in open_pairs.values() if pair.status not in {"closed", "failed"}
        )
        if config.max_open_pairs > 0 and active_pair_count >= config.max_open_pairs:
            return opened

        queue = self._candidate_queue(candidates, open_pairs, managed_symbols, config, manual_locked_symbols)
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
        manual_locked_symbols: set[str] | None = None,
    ) -> list[PairCandidate]:
        manual_locked_symbols = {symbol.upper() for symbol in (manual_locked_symbols or set())}
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
            if symbol in manual_locked_symbols:
                continue
            if symbol in active_pair_symbols:
                continue
            if self._symbol_pause_active(symbol):
                continue
            if not self._candidate_allowed_for_preposition(candidate, config):
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

    def _active_edge_still_valid(self, candidate: PairCandidate, config: StrategyConfig) -> bool:
        min_keep_edge_bps = (
            float(config.min_expected_net_edge_bps)
            - float(config.maker_entry_engine_keep_edge_buffer_bps)
        )
        return float(candidate.expected_net_edge_bps) >= min_keep_edge_bps

    def _replacement_candidate(
        self,
        *,
        candidates: list[PairCandidate],
        open_pairs: dict[str, OpenPairState],
        managed_symbols: set[str],
        config: StrategyConfig,
        manual_locked_symbols: set[str] | None = None,
    ) -> PairCandidate | None:
        state = self.active
        if state is None:
            return None
        now_ms = int(time.time() * 1000)
        requote_ms = int(max(config.maker_entry_engine_requote_sec, 0.0) * 1000)
        if state.submitted_ms + requote_ms > now_ms:
            return None

        queue = self._candidate_queue(candidates, open_pairs, managed_symbols, config, manual_locked_symbols)
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
        intent_id = f"maker:{candidate.id}:{now_ms}"
        lease_acquired = False

        def release_submit_lease(note: str) -> None:
            if self.action_gate is not None and lease_acquired:
                self.action_gate.release_symbol(
                    owner_id=intent_id,
                    symbol=candidate.symbol,
                    note=note,
                )

        if self.action_gate is not None:
            lease_acquired, lease_reason = self.action_gate.try_acquire_symbol(
                owner_id=intent_id,
                symbol=candidate.symbol,
                domain="maker_entry",
                note="maker_first_leg_submit",
            )
            if not lease_acquired:
                self.hooks.record_event(
                    "maker_entry_order_blocked_by_action_lease",
                    candidate_id=candidate.id,
                    symbol=candidate.symbol,
                    reason=lease_reason,
                )
                return

        submit_key = (first_exchange_name, candidate.symbol.upper(), first_side.lower())
        cooldown_ms = int(max(config.entry_order_cooldown_sec, config.maker_entry_engine_requote_sec, 0.0) * 1000)
        if self.last_submit_ms_by_key.get(submit_key, 0) + cooldown_ms > now_ms:
            release_submit_lease("submit_cooldown")
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
            release_submit_lease("bootstrap_failed")
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
            release_submit_lease("price_or_qty_missing")
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
        try:
            order_result = await self._submit_maker_limit_order(
                first_exchange,
                candidate.symbol,
                first_side,
                target_qty,
                first_price,
            )
        except Exception:
            release_submit_lease("submit_exception")
            raise
        if isinstance(order_result, dict):
            order_result.setdefault("owner", "maker_entry_engine")
            order_result.setdefault("strategy_owner", "maker_entry_engine")
            order_result.setdefault("strategy_order_role", "maker_first_leg")
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
            release_submit_lease("submit_failed")
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
            intent_id=intent_id,
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

    async def _submit_maker_limit_order(
        self,
        exchange: Any,
        symbol: str,
        side: str,
        qty: float,
        price: float,
    ) -> Any:
        try:
            return await exchange.order(
                symbol,
                side,
                qty,
                price,
                owner="maker_entry_engine",
                strategy_order_role="maker_first_leg",
            )
        except TypeError as exc:
            if "owner" not in str(exc) and "strategy_order_role" not in str(exc):
                raise
            result = await exchange.order(symbol, side, qty, price)
            if isinstance(result, dict):
                result.setdefault("owner", "maker_entry_engine")
                result.setdefault("strategy_owner", "maker_entry_engine")
                result.setdefault("strategy_order_role", "maker_first_leg")
            return result

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
        latest_candidate = self._current_candidate_for_state(candidates, state)
        if latest_candidate is not None:
            # Keep execution identifiers/qty on MakerOrderState, but use the
            # newest in-memory market/funding snapshot for post-fill analysis.
            state.candidate = latest_candidate

        if any(pair.symbol == symbol and pair.status not in {"closed", "failed"} for pair in open_pairs.values()):
            return await self.cancel_active(exchanges, symbol, "symbol_pair_exists", config, assumed_flat_until_ms)

        post_qty_first = self.hooks.extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
        filled_qty = self.hooks.position_fill_delta(state.pre_qty_first, post_qty_first, state.first_side)
        if filled_qty > EPS:
            cancel_result = None
            if filled_qty < state.target_qty - EPS:
                cancel_result = await self.hooks.cancel_if_open(first_exchange, symbol, state.order_result)
                if not self._cancel_result_is_terminal(cancel_result):
                    retry_ms = self._cancel_retry_ms(cancel_result, config)
                    state.cancel_retry_after_ms = int(time.time() * 1000) + retry_ms
                    self.hooks.record_event(
                        "maker_entry_order_cancel_failed",
                        candidate_id=state.candidate.id,
                        symbol=symbol,
                        exchange=state.first_exchange_name,
                        side=state.first_side,
                        order_id=state.order_id,
                        price=state.price,
                        qty=state.target_qty,
                        reason="filled_residual_cancel_failed",
                        filled_qty=filled_qty,
                        cancel_result=cancel_result,
                        retry_after_ms=state.cancel_retry_after_ms,
                    )
                    return None
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
                cancel_result=cancel_result,
            )
            return await self._complete_filled_state(
                state=state,
                exchanges=exchanges,
                config=config,
                assumed_flat_until_ms=assumed_flat_until_ms,
                filled_qty=filled_qty,
            )

        candidate = self._current_candidate_for_state(candidates, state)
        if candidate is None:
            return await self.cancel_active(exchanges, symbol, "candidate_missing", config, assumed_flat_until_ms)
        hard_reasons = self._hard_blocking_reasons_excluding_own_order(candidate, state, exchanges, config)
        if hard_reasons:
            return await self.cancel_active(
                exchanges,
                symbol,
                "candidate_blocked:" + "|".join(hard_reasons[:3]),
                config,
                assumed_flat_until_ms,
            )
        if config.maker_entry_engine_require_full_admission:
            admission_reasons = self._admission_reasons_excluding_own_order(
                candidate,
                state,
                exchanges,
                config,
            )
            if admission_reasons:
                return await self.cancel_active(
                    exchanges,
                    symbol,
                    "candidate_admission_failed:" + "|".join(admission_reasons[:3]),
                    config,
                    assumed_flat_until_ms,
                )
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
            cancel_result = await self.hooks.cancel_if_open(first_exchange, symbol, state.order_result)
            if not self._cancel_result_is_terminal(cancel_result):
                retry_ms = self._cancel_retry_ms(cancel_result, config)
                state.cancel_retry_after_ms = int(time.time() * 1000) + retry_ms
                self.hooks.record_event(
                    "maker_entry_order_cancel_failed",
                    candidate_id=state.candidate.id,
                    symbol=symbol,
                    exchange=state.first_exchange_name,
                    side=state.first_side,
                    order_id=state.order_id,
                    price=state.price,
                    qty=state.target_qty,
                    reason="order_disappeared_without_fill",
                    cancel_result=cancel_result,
                    retry_after_ms=state.cancel_retry_after_ms,
                )
                return None
            pair = await self._complete_if_fill_visible(
                state=state,
                exchanges=exchanges,
                config=config,
                assumed_flat_until_ms=assumed_flat_until_ms,
                reason="after_cancel:order_disappeared_without_fill",
            )
            if pair is not None:
                return pair
            self.hooks.record_event(
                "maker_entry_order_cancelled",
                candidate_id=state.candidate.id,
                symbol=symbol,
                exchange=state.first_exchange_name,
                side=state.first_side,
                order_id=state.order_id,
                price=state.price,
                qty=state.target_qty,
                reason="order_disappeared_without_fill",
                cancel_result=cancel_result,
            )
            self._clear_active(state, "disappeared_terminal_no_fill")
            return None
        state.disappeared_ms = 0

        if state.submitted_ms + int(max(config.maker_entry_engine_requote_sec, 0.0) * 1000) > now_ms:
            return None
        if self._symbol_pause_active(symbol):
            return None
        if not self._active_edge_still_valid(candidate, config):
            return await self.cancel_active(
                exchanges,
                symbol,
                "edge_deteriorated",
                config,
                assumed_flat_until_ms,
            )

        next_price = self.hooks.target_price(candidate, state.first_exchange_name, state.first_side, exchanges, config)
        if next_price <= EPS:
            return await self.cancel_active(exchanges, symbol, "target_price_missing", config, assumed_flat_until_ms)
        if self._should_requote_active(first_exchange, symbol, state, candidate, next_price, config):
            return await self.cancel_active(exchanges, symbol, "requote", config, assumed_flat_until_ms)
        return None

    def _should_requote_active(
        self,
        first_exchange: Any,
        symbol: str,
        state: MakerOrderState,
        candidate: PairCandidate,
        next_price: float,
        config: StrategyConfig,
    ) -> bool:
        if not self.hooks.price_changed(first_exchange, symbol, state.price, next_price, config):
            return False
        price_move_bps = _price_move_bps(state.price, next_price)
        if price_move_bps < float(config.maker_entry_engine_replace_price_bps):
            return False
        edge_improvement_bps = self._candidate_score_bps(candidate) - self._candidate_score_bps(state.candidate)
        if edge_improvement_bps < float(config.maker_entry_engine_replace_edge_bps):
            return False
        self.hooks.record_event(
            "maker_entry_requote_selected",
            candidate_id=state.candidate.id,
            symbol=symbol,
            exchange=state.first_exchange_name,
            side=state.first_side,
            order_id=state.order_id,
            previous_price=state.price,
            next_price=next_price,
            price_move_bps=price_move_bps,
            edge_improvement_bps=edge_improvement_bps,
            replace_price_threshold_bps=config.maker_entry_engine_replace_price_bps,
            replace_edge_threshold_bps=config.maker_entry_engine_replace_edge_bps,
        )
        return True

    async def _complete_if_fill_visible(
        self,
        *,
        state: MakerOrderState,
        exchanges: dict[str, Any],
        config: StrategyConfig,
        assumed_flat_until_ms: dict[tuple[str, str], int],
        reason: str,
    ) -> OpenPairState | None:
        first_exchange = exchanges[state.first_exchange_name]
        symbol = state.candidate.symbol
        settle_ms = int(max(config.entry_order_settle_sec, config.action_settle_wait_sec * 2.0, 0.5) * 1000)
        deadline_ms = int(time.time() * 1000) + settle_ms
        sleep_sec = max(min(float(config.action_settle_wait_sec), 0.25), 0.05)
        while True:
            post_qty_first = self.hooks.extract_position_qty(first_exchange.get_symbol_snapshot(symbol))
            filled_qty = self.hooks.position_fill_delta(state.pre_qty_first, post_qty_first, state.first_side)
            if filled_qty > EPS:
                requester = getattr(first_exchange, "request_account_refresh", None)
                if callable(requester):
                    try:
                        requester("maker_first_leg_fill", priority="urgent", symbol=symbol)
                    except Exception as exc:
                        print(
                            f"[maker_entry_engine][account_refresh][warn] "
                            f"exchange={state.first_exchange_name} symbol={symbol} error={exc!r}"
                        )
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
                    reason=reason,
                )
                return await self._complete_filled_state(
                    state=state,
                    exchanges=exchanges,
                    config=config,
                    assumed_flat_until_ms=assumed_flat_until_ms,
                    filled_qty=filled_qty,
                )
            if int(time.time() * 1000) >= deadline_ms:
                return None
            await asyncio.sleep(sleep_sec)

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

    def _candidate_allowed_for_preposition(
        self,
        candidate: PairCandidate,
        config: StrategyConfig,
    ) -> bool:
        if candidate.can_enter:
            return True
        if config.maker_entry_engine_require_full_admission:
            return False
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
        exchanges: dict[str, Any],
        config: StrategyConfig,
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
            and not self._is_active_stale_within_grace(reason, candidate.symbol, exchanges, config)
        ]

    def _admission_reasons_excluding_own_order(
        self,
        candidate: PairCandidate,
        state: MakerOrderState,
        exchanges: dict[str, Any],
        config: StrategyConfig,
    ) -> list[str]:
        ignored = {
            f"{state.first_exchange_name}:open_orders_exist",
            "risk:symbol_side_order_exists",
            "risk:rate_limit_pressure",
        }
        reasons = [
            reason
            for reason in candidate.block_reasons
            if reason not in ignored
            and not self._is_active_stale_within_grace(
                reason,
                candidate.symbol,
                exchanges,
                config,
            )
        ]
        if self._has_foreign_first_leg_order(state, exchanges):
            reasons.append(f"{state.first_exchange_name}:foreign_open_orders_exist")
        return reasons

    def _has_foreign_first_leg_order(
        self,
        state: MakerOrderState,
        exchanges: dict[str, Any],
    ) -> bool:
        exchange = exchanges.get(state.first_exchange_name)
        if exchange is None:
            return True
        own_order_id = str(state.order_id or "")
        symbol = state.candidate.symbol.upper()
        rows: list[dict[str, Any]] = list(getattr(exchange, "waiting_list", []) or [])
        snapshot = getattr(exchange, "open_orders_snapshot", {}) or {}
        if isinstance(snapshot, dict):
            for key, snapshot_rows in snapshot.items():
                if self._normalized_order_symbol(str(key)) == symbol:
                    rows.extend(snapshot_rows or [])

        seen_ids: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_symbol = self._normalized_order_symbol(
                str(row.get("ticker") or row.get("symbol") or row.get("instId") or "")
            )
            if row_symbol != symbol:
                continue
            row_id = str(self.hooks.extract_order_uuid(row) or "")
            if row_id:
                if row_id in seen_ids:
                    continue
                seen_ids.add(row_id)
                if own_order_id and row_id == own_order_id:
                    continue
            # An unidentified same-symbol open order cannot safely be assumed
            # to be the engine-owned order.
            return True
        return False

    def _normalized_order_symbol(self, value: str) -> str:
        normalized = str(value or "").upper().split("-", 1)[0]
        for suffix in ("USDT", "USDC", "USD"):
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
                break
        return normalized

    def _is_active_stale_within_grace(
        self,
        reason: str,
        symbol: str,
        exchanges: dict[str, Any],
        config: StrategyConfig,
    ) -> bool:
        if not reason.endswith(":stale_orderbook"):
            return False
        exchange_name = reason.split(":", 1)[0]
        exchange = exchanges.get(exchange_name)
        if exchange is None:
            return False
        try:
            health = (exchange.get_symbol_snapshot(symbol) or {}).get("health") or {}
        except Exception:
            return False
        stale_age_ms = _safe_float(health.get("stale_age_ms"), -1.0)
        if stale_age_ms < 0:
            return False
        active_stale_ms = float(config.maker_entry_engine_active_stale_sec) * 1000.0
        return stale_age_ms <= active_stale_ms
