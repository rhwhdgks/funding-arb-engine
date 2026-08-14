from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import uvicorn

from src.core.api_pressure import current_api_pressure_summary
from src.ops.persistence.account_context import resolve_account_label, resolve_logic_name
from src.ops_console.action_gate import live_action_window_status, set_active_strategy_symbols_provider
from src.ops_console.runtime import (
    PROJECT_ROOT,
    action_token_required,
    console_symbols_source,
    exchange_action_error,
    parse_bool,
    parse_positive_float,
)
from src.ops_console.state import build_exchange_state, normalize_symbol, now_ms
from src.ops_console.strategy_dashboard import (
    DANGER_EVENT_TYPES,
    KEY_EVENT_TYPES,
    _active_orphans,
    compact_api_pressure,
    compact_candidate,
    compact_event,
    latest_artifacts,
    realized_pnl_summary,
)

if TYPE_CHECKING:
    from src.core.action_gate import ActionGate
    from src.core.order_intent import OrderIntentJournal
    from src.strategy.control import EntryControlStore
    from src.strategy.models import OpenPairState, StrategyConfig


_ACTIVE_SERVICE: "StrategyControlService | None" = None


def active_service() -> "StrategyControlService | None":
    return _ACTIVE_SERVICE


def set_active_service(service: "StrategyControlService | None") -> None:
    global _ACTIVE_SERVICE
    _ACTIVE_SERVICE = service


def record_control_plane_event(row: dict[str, Any]) -> None:
    service = active_service()
    if service is not None:
        service.record_event(row)


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _payload_dict(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return dict(payload)
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        try:
            return dict(model_dump())
        except Exception:
            return {}
    legacy_dict = getattr(payload, "dict", None)
    if callable(legacy_dict):
        try:
            return dict(legacy_dict())
        except Exception:
            return {}
    return {}


def command_payload_summary(payload: Any) -> dict[str, Any]:
    raw = _payload_dict(payload)
    allowed_keys = {
        "exchange",
        "symbol",
        "side",
        "order_type",
        "quantity",
        "price",
        "reduce_only",
        "hedge_mode",
        "hedge_exchange",
        "ownership_mode",
        "confirm",
        "wait_sec",
        "reason",
    }
    summary: dict[str, Any] = {}
    for key in sorted(allowed_keys):
        if key not in raw:
            continue
        value = raw.get(key)
        if value is None:
            continue
        summary[key] = value
    return summary


def new_control_command_id() -> str:
    return f"cmd-{now_ms()}-{uuid.uuid4().hex[:8]}"


@dataclass
class StrategyControlCommand:
    command_type: str
    payload: Any
    response_future: asyncio.Future[Any]
    ops_token: str | None = None
    operator: str = "local_operator"
    enqueued_at_ms: int = field(default_factory=now_ms)
    command_id: str = field(default_factory=new_control_command_id)


class StrategyControlProjection:
    def __init__(self, *, started_at_ms: int, recent_limit: int = 50) -> None:
        self.started_at_ms = started_at_ms
        self.event_counts: Counter[str] = Counter()
        self.latest_by_type: dict[str, dict[str, Any]] = {}
        self.recent_events: deque[dict[str, Any]] = deque(maxlen=recent_limit)
        self.last_event_ms: int = 0
        self.last_error: str = ""
        self.pnl_cache: dict[str, Any] = {
            "ok": False,
            "account_label": resolve_account_label(),
            "today_realized_usd": 0.0,
            "today_fee_usd": 0.0,
            "today_trades": 0,
            "run_realized_usd": 0.0,
            "run_fee_usd": 0.0,
            "run_trades": 0,
            "by_symbol": [],
        }
        self.pnl_updated_at_ms: int = 0
        self.api_pressure_cache: dict[str, Any] = {}
        self.api_pressure_updated_at_ms: int = 0

    @property
    def rows_loaded(self) -> int:
        return sum(self.event_counts.values())

    def record_event(self, row: dict[str, Any]) -> None:
        event_type = str(row.get("event_type") or "unknown")
        self.event_counts[event_type] += 1
        self.latest_by_type[event_type] = dict(row)
        self.recent_events.append(compact_event(row))
        self.last_event_ms = _safe_int(row.get("ts_ms"), now_ms())

    def update_pnl_cache(self, payload: dict[str, Any]) -> None:
        self.pnl_cache = dict(payload)
        self.pnl_updated_at_ms = now_ms()

    def update_api_pressure(self, payload: dict[str, Any] | None) -> None:
        if not isinstance(payload, dict) or not payload:
            return
        self.api_pressure_cache = dict(payload)
        self.api_pressure_updated_at_ms = now_ms()


class _EmbeddedExchangeSession:
    def __init__(self, exchange: Any) -> None:
        self.exchange = exchange

    async def ensure_started(self) -> None:
        return None


class StrategyConsoleRuntimeAdapter:
    def __init__(self, service: "StrategyControlService") -> None:
        self.service = service
        self.exchange_names = list(service.exchange_names)
        self.symbols = list(service.symbols)

    async def snapshot(self) -> dict[str, Any]:
        return await self.service.build_console_state()

    def session(self, exchange_name: str) -> _EmbeddedExchangeSession:
        normalized = str(exchange_name or "").strip().lower()
        exchange = self.service.exchanges.get(normalized)
        if exchange is None:
            raise ValueError(f"exchange not configured: {exchange_name}")
        return _EmbeddedExchangeSession(exchange)

    async def execute_action(
        self,
        exchange_name: str,
        symbol: str,
        action_name: str,
        action: Any,
        *,
        wait_sec: float = 0.0,
    ) -> dict[str, Any]:
        normalized_exchange = str(exchange_name or "").strip().lower()
        exchange = self.service.exchanges.get(normalized_exchange)
        if exchange is None:
            raise ValueError(f"exchange not configured: {exchange_name}")

        normalized_symbol = normalize_symbol(symbol)
        timeout_sec = float(os.getenv("OPS_CONSOLE_ACTION_TIMEOUT_SEC", "10.0"))
        before = self.service.build_single_exchange_state(normalized_exchange)
        result = await asyncio.wait_for(action(exchange), timeout=timeout_sec)
        error = exchange_action_error(result)
        if error:
            raise ValueError(
                f"{action_name} failed on {normalized_exchange}:{normalized_symbol}: {error}"
            )
        if wait_sec > 0:
            await asyncio.sleep(wait_sec)
        after = self.service.build_single_exchange_state(normalized_exchange)
        return {
            "ok": True,
            "action": action_name,
            "exchange": normalized_exchange,
            "symbol": normalized_symbol,
            "action_result": result,
            "before": before,
            "after": after,
        }


class StrategyControlService:
    def __init__(
        self,
        *,
        config: "StrategyConfig",
        exchanges: dict[str, Any],
        symbols: list[str],
        open_pairs: dict[str, "OpenPairState"],
        entry_control_store: "EntryControlStore",
        assumed_flat_until_ms: dict[tuple[str, str], int],
        symbol_recent_close_ms: dict[str, int],
        wake_queue: asyncio.Queue[tuple[str, str, int]],
        command_queue: asyncio.Queue[StrategyControlCommand],
        action_gate: "ActionGate | None" = None,
        order_intent_journal: "OrderIntentJournal | None" = None,
        event_sink: Callable[..., None] | None = None,
    ) -> None:
        self.config = config
        self.exchanges = exchanges
        self.exchange_names = list(exchanges.keys())
        self.symbols = [normalize_symbol(symbol) for symbol in symbols]
        self.open_pairs = open_pairs
        self.entry_control_store = entry_control_store
        self.assumed_flat_until_ms = assumed_flat_until_ms
        self.symbol_recent_close_ms = symbol_recent_close_ms
        self.wake_queue = wake_queue
        self.command_queue = command_queue
        self.action_gate = action_gate
        self.order_intent_journal = order_intent_journal
        self.event_sink = event_sink
        self.stale_ms = int(os.getenv("OPS_CONSOLE_STALE_MS", "1500"))
        self.account_label = resolve_account_label()
        self.logic = resolve_logic_name()
        self.started_at_ms = now_ms()
        self.started_at_iso = _utc_now_iso()
        self.startup_block_reasons: tuple[str, ...] = ()
        self.last_error: str = ""
        self.state_last_built_ms: int = 0
        self.extra_active_symbols_provider = None
        self.projection = StrategyControlProjection(started_at_ms=self.started_at_ms)
        self.runtime_adapter = StrategyConsoleRuntimeAdapter(self)
        self._refresh_task: asyncio.Task[Any] | None = None
        self._stopping = False

    async def start(self) -> None:
        set_active_service(self)
        set_active_strategy_symbols_provider(self.active_strategy_symbols)
        self._refresh_task = asyncio.create_task(
            self._background_refresh_loop(),
            name="strategy-control-refresh",
        )

    async def stop(self) -> None:
        self._stopping = True
        if active_service() is self:
            set_active_service(None)
        set_active_strategy_symbols_provider(None)
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None

    async def _background_refresh_loop(self) -> None:
        interval_sec = max(float(os.getenv("OPS_CONSOLE_BACKGROUND_REFRESH_SEC", "5.0")), 1.0)
        while True:
            try:
                pnl = await asyncio.to_thread(realized_pnl_summary, self.account_label, self.started_at_ms)
                self.projection.update_pnl_cache(pnl)
                self.projection.update_api_pressure(current_api_pressure_summary())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
            await asyncio.sleep(interval_sec)

    def set_startup_block_reasons(self, reasons: tuple[str, ...]) -> None:
        self.startup_block_reasons = tuple(reasons or ())

    def active_strategy_symbols(self) -> dict[str, str]:
        active: dict[str, str] = {}
        for pair_id, pair in self.open_pairs.items():
            if getattr(pair, "status", "") in {"closed", "failed"}:
                continue
            active[str(pair_id)] = normalize_symbol(getattr(pair, "symbol", ""))
        provider = self.extra_active_symbols_provider
        if callable(provider):
            try:
                for symbol in provider() or set():
                    normalized = normalize_symbol(str(symbol))
                    if normalized:
                        active[f"maker:{normalized}"] = normalized
            except Exception as exc:
                self.last_error = f"active maker symbol provider failed: {exc!r}"
        return active

    def record_event(self, row: dict[str, Any]) -> None:
        self.projection.record_event(row)

    def audit_event(self, event_type: str, **payload: Any) -> None:
        if self.event_sink is not None:
            self.event_sink(event_type, **payload)
            return
        row = {
            "ts_ms": now_ms(),
            "ts_utc": _utc_now_iso(),
            "event_type": event_type,
            **payload,
        }
        self.record_event(row)

    def audit_command_event(
        self,
        event_type: str,
        command: StrategyControlCommand,
        **payload: Any,
    ) -> None:
        self.audit_event(
            event_type,
            command_id=command.command_id,
            command_type=command.command_type,
            enqueued_at_ms=command.enqueued_at_ms,
            payload_summary=command_payload_summary(command.payload),
            ops_token_present=bool(command.ops_token),
            operator=command.operator,
            **payload,
        )

    def build_single_exchange_state(self, exchange_name: str) -> dict[str, Any]:
        exchange = self.exchanges[exchange_name]
        return build_exchange_state(exchange_name, exchange, self.symbols, stale_ms=self.stale_ms)

    async def build_console_state(self) -> dict[str, Any]:
        generated_at_ms = now_ms()
        exchange_states: dict[str, Any] = {}
        ages: list[int] = []
        errors: list[str] = []
        for exchange_name in self.exchange_names:
            try:
                state = self.build_single_exchange_state(exchange_name)
                exchange_states[exchange_name] = state
                for symbol_state in (state.get("symbols") or {}).values():
                    orderbook = symbol_state.get("orderbook") or {}
                    age_ms = orderbook.get("age_ms")
                    if age_ms is not None:
                        ages.append(_safe_int(age_ms))
            except Exception as exc:
                errors.append(f"{exchange_name}:{exc}")
                exchange_states[exchange_name] = {
                    "exchange": exchange_name,
                    "error": str(exc),
                    "symbols": {},
                }
        self.state_last_built_ms = generated_at_ms
        if errors:
            self.last_error = "; ".join(errors)[:400]
        source_status = "stopping" if self._stopping else ("degraded" if errors else "live")
        return {
            "timestamp_ms": generated_at_ms,
            "timestamp_utc": _utc_now_iso(),
            "generated_at_ms": generated_at_ms,
            "data_age_ms": max(ages) if ages else 0,
            "source_status": source_status,
            "source": "embedded_strategy",
            "exchange_owner": "strategy",
            "standalone_enabled": False,
            "last_error": self.last_error or None,
            "mode": os.getenv("STRATEGY_MODE", "LIVE"),
            "symbols": self.symbols,
            "exchanges": exchange_states,
            "runtime": {
                "exchange_names": self.exchange_names,
                "symbols": self.symbols,
                "symbols_source": console_symbols_source(),
                "stale_ms": self.stale_ms,
                "strategy_leverage": parse_positive_float(os.getenv("STRATEGY_LEVERAGE"), 1.0),
                "entry_control": self.entry_control_dict(),
                "web_trading_enabled": parse_bool(os.getenv("ENABLE_WEB_TRADING"), False),
                "token_required": action_token_required(),
                "live_action_window": live_action_window_status(),
                "account_label": self.account_label,
                "logic": self.logic,
                "source": "embedded_strategy",
                "exchange_owner": "strategy",
                "standalone_enabled": False,
            },
        }

    def entry_control_dict(self) -> dict[str, Any]:
        state = self.entry_control_store.read_state()
        return {
            "entry_paused": state.entry_paused,
            "reason": state.reason,
            "updated_at_ms": state.updated_at_ms,
            "updated_by": state.updated_by,
            "source_path": state.source_path,
            "error": state.error,
        }

    def build_strategy_state(self) -> dict[str, Any]:
        now = now_ms()
        latest_candidate = self.projection.latest_by_type.get("candidate_scan_summary") or {}
        latest_shutdown = self.projection.latest_by_type.get("shutdown_cleanup_done") or {}
        api_pressure = self.projection.api_pressure_cache or {}
        danger_counts = {
            event_name: self.projection.event_counts.get(event_name, 0)
            for event_name in sorted(DANGER_EVENT_TYPES)
        }
        age_candidates = [
            age
            for age in (
                now - self.projection.last_event_ms if self.projection.last_event_ms > 0 else None,
                now - self.projection.pnl_updated_at_ms if self.projection.pnl_updated_at_ms > 0 else None,
                now - self.projection.api_pressure_updated_at_ms if self.projection.api_pressure_updated_at_ms > 0 else None,
            )
            if age is not None
        ]
        source_status = "stopping" if self._stopping else ("degraded" if self.last_error else "live")
        log_dir = Path(os.getenv("STRATEGY_LIVE_LOG_DIR") or PROJECT_ROOT / "logs")
        pid_file = Path(os.getenv("STRATEGY_LIVE_PID_FILE") or (log_dir / "strategy_live.pid"))
        meta_file = Path(os.getenv("STRATEGY_LIVE_RUN_META_FILE") or (log_dir / "strategy_live.run.json"))
        stdout_log = Path(os.getenv("STRATEGY_LIVE_STDOUT_LOG") or (log_dir / "strategy_live.out"))
        jsonl_log = self.config.live_event_log_path
        return {
            "timestamp_ms": now,
            "generated_at_ms": now,
            "data_age_ms": max(age_candidates) if age_candidates else 0,
            "source_status": source_status,
            "source": "embedded_strategy",
            "exchange_owner": "strategy",
            "standalone_enabled": False,
            "last_error": self.last_error or None,
            "paths": {
                "log_dir": str(log_dir),
                "pid_file": str(pid_file),
                "meta_file": str(meta_file),
                "jsonl_log": str(jsonl_log),
                "stdout_log": str(stdout_log),
            },
            "runtime": {
                "path": str(pid_file),
                "pid": os.getpid(),
                "running": True,
                "started_at_ms": self.started_at_ms,
                "started_at_iso": self.started_at_iso,
                "duration": f"{max((now - self.started_at_ms) / 1000.0, 0.0):.1f}s",
                "execution_profile": os.getenv("STRATEGY_EXECUTION_PROFILE") or "strategy_attached_console",
                "env_file": os.getenv("STRATEGY_ENV_FILE"),
                "symbols": self.symbols,
                "exchanges": self.exchange_names,
                "account_label": self.account_label,
                "logic": self.logic,
                "runtime_stopped": False,
                "entry_control": self.entry_control_dict(),
                "source": "embedded_strategy",
                "exchange_owner": "strategy",
                "standalone_enabled": False,
            },
            "health": {
                "danger_total": sum(danger_counts.values()),
                "danger_counts": danger_counts,
                "shutdown_remaining_positions": latest_shutdown.get("remaining_positions"),
                "shutdown_pending_pair_ids": latest_shutdown.get("pending_pair_ids"),
            },
            "events": {
                "rows_loaded": self.projection.rows_loaded,
                "counts": {event: self.projection.event_counts.get(event, 0) for event in KEY_EVENT_TYPES},
                "top_counts": dict(self.projection.event_counts.most_common(20)),
                "recent": list(self.projection.recent_events)[-25:],
            },
            "candidate": {
                "latest_ts_utc": latest_candidate.get("ts_utc"),
                "total_candidates": latest_candidate.get("total_candidates"),
                "enterable_candidates": latest_candidate.get("enterable_candidates"),
                "blocked_candidates": latest_candidate.get("blocked_candidates"),
                "top_candidate": compact_candidate(latest_candidate.get("top_candidate")),
                "top_enterable_candidate": compact_candidate(latest_candidate.get("top_enterable_candidate")),
                "top_blocked_candidate": compact_candidate(latest_candidate.get("top_blocked_candidate")),
                "top_block_reason": latest_candidate.get("top_block_reason"),
                "reason_counts": latest_candidate.get("reason_counts") or {},
            },
            "api_pressure": {
                "latest_ts_utc": api_pressure.get("ts_utc"),
                "window_sec": api_pressure.get("window_sec"),
                "exchanges": compact_api_pressure(api_pressure),
            },
            "pnl": dict(self.projection.pnl_cache),
            "orphans": _active_orphans(),
            "artifacts": latest_artifacts(log_dir),
            "meta": {
                "embedded": True,
                "source": "embedded_strategy",
                "exchange_owner": "strategy",
                "standalone_enabled": False,
                "startup_block_reasons": list(self.startup_block_reasons),
                "open_pair_ids": sorted(self.open_pairs.keys()),
                "assumed_flat_keys": [
                    {"exchange": exchange, "symbol": symbol, "until_ms": until_ms}
                    for (exchange, symbol), until_ms in sorted(self.assumed_flat_until_ms.items())
                ],
                "recent_close_symbols": dict(self.symbol_recent_close_ms),
                "action_gate": self.action_gate.snapshot() if self.action_gate is not None else {},
                "order_intents": (
                    self.order_intent_journal.snapshot()
                    if self.order_intent_journal is not None
                    else {}
                ),
            },
        }

    def build_health_check(self) -> dict[str, Any]:
        state = self.build_strategy_state()
        generated_at_ms = now_ms()
        events = state.get("events") or {}
        counts = events.get("counts") or {}
        top_counts = events.get("top_counts") or {}
        health = state.get("health") or {}
        runtime = state.get("runtime") or {}
        api_pressure = state.get("api_pressure") or {}
        checks: list[dict[str, Any]] = []

        def event_count(name: str) -> int:
            return _safe_int(counts.get(name, top_counts.get(name, 0)))

        def add_check(status: str, name: str, detail: str, metrics: dict[str, Any] | None = None) -> None:
            checks.append(
                {
                    "status": status,
                    "name": name,
                    "detail": detail,
                    "metrics": metrics or {},
                }
            )

        hard_event_counts = {
            event_name: event_count(event_name)
            for event_name in sorted(DANGER_EVENT_TYPES | {"pair_orphan_marked"})
        }
        hard_total = sum(hard_event_counts.values())
        add_check(
            "PASS" if hard_total == 0 else "FAIL",
            "hard_failure_events_zero",
            f"count={hard_total}",
            hard_event_counts,
        )

        orphan_count = len(state.get("orphans") or [])
        add_check(
            "PASS" if orphan_count == 0 else "FAIL",
            "active_orphans_empty",
            f"count={orphan_count}",
        )

        source_status = str(state.get("source_status") or "")
        add_check(
            "PASS" if source_status in {"live", "stopping"} else "WARN",
            "strategy_projection_live",
            f"source_status={source_status or '-'}",
        )

        max_data_age_ms = _safe_int(os.getenv("OPS_CONSOLE_HEALTH_MAX_DATA_AGE_MS"), 15000)
        data_age_ms = _safe_int(state.get("data_age_ms"))
        add_check(
            "PASS" if data_age_ms <= max_data_age_ms else "WARN",
            "strategy_projection_fresh",
            f"data_age_ms={data_age_ms} max={max_data_age_ms}",
        )

        entry_control = runtime.get("entry_control") or {}
        entry_error = str(entry_control.get("error") or "")
        add_check(
            "PASS" if not entry_error else "WARN",
            "entry_control_readable",
            entry_error or f"entry_paused={bool(entry_control.get('entry_paused'))}",
        )

        startup_block_reasons = (state.get("meta") or {}).get("startup_block_reasons") or []
        add_check(
            "PASS" if not startup_block_reasons else "FAIL",
            "startup_block_reasons_empty",
            f"count={len(startup_block_reasons)}",
            {"reasons": startup_block_reasons},
        )

        order_intents = (state.get("meta") or {}).get("order_intents") or {}
        unresolved_intents = order_intents.get("unresolved") or []
        add_check(
            "PASS" if not unresolved_intents else "FAIL",
            "unresolved_order_intents_empty",
            f"count={len(unresolved_intents)}",
            {"intents": unresolved_intents[:10]},
        )

        token_configured = bool(os.getenv("OPS_CONSOLE_ACTION_TOKEN", "").strip())
        token_required = action_token_required()
        add_check(
            "PASS" if token_required and token_configured else "FAIL",
            "ops_console_action_token_configured",
            f"required={token_required} configured={token_configured}",
        )

        api_failures = 0
        api_warnings = 0
        api_metrics: dict[str, Any] = {}
        for exchange_name, row in (api_pressure.get("exchanges") or {}).items():
            max_usage = max(
                _safe_float(row.get("max_request_weight_usage_1m")),
                _safe_float(row.get("max_order_count_usage_1m")),
                _safe_float(row.get("max_order_count_usage_10s")),
            )
            violations = _safe_int(row.get("rate_limit_violations"))
            bans = _safe_int(row.get("ip_ban_events"))
            retry_exhausted = _safe_int(row.get("retry_exhausted"))
            if violations > 0 or bans > 0:
                api_failures += 1
            elif max_usage >= 0.85 or retry_exhausted > 0:
                api_warnings += 1
            api_metrics[str(exchange_name)] = {
                "max_usage": max_usage,
                "rate_limit_violations": violations,
                "ip_ban_events": bans,
                "retry_exhausted": retry_exhausted,
            }
        api_status = "FAIL" if api_failures else ("WARN" if api_warnings else "PASS")
        add_check(
            api_status,
            "api_pressure_within_limits",
            f"failures={api_failures} warnings={api_warnings}",
            api_metrics,
        )

        open_pair_ids = (state.get("meta") or {}).get("open_pair_ids") or []
        add_check(
            "INFO",
            "open_pairs_current",
            f"count={len(open_pair_ids)}",
            {"pair_ids": open_pair_ids},
        )

        failures = [row for row in checks if row.get("status") == "FAIL"]
        warnings = [row for row in checks if row.get("status") == "WARN"]
        verdict = "FAIL" if failures else ("WARN" if warnings else "PASS")
        return {
            "ok": True,
            "verdict": verdict,
            "generated_at_ms": generated_at_ms,
            "generated_at_utc": _utc_now_iso(),
            "data_age_ms": data_age_ms,
            "source_status": source_status,
            "failures": len(failures),
            "warnings": len(warnings),
            "checks": checks,
            "summary": {
                "running": bool(runtime.get("running")),
                "duration": runtime.get("duration"),
                "symbols": runtime.get("symbols") or [],
                "open_pairs": len(open_pair_ids),
                "danger_total": _safe_int(health.get("danger_total")),
                "orphans": orphan_count,
            },
        }

    async def submit_command(
        self,
        command_type: str,
        payload: Any,
        *,
        ops_token: str | None,
        operator: str = "local_operator",
        timeout_sec: float | None = None,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        command = StrategyControlCommand(
            command_type=command_type,
            payload=payload,
            response_future=future,
            ops_token=ops_token,
            operator=str(operator or "local_operator")[:64],
        )
        self.command_queue.put_nowait(command)
        with contextlib.suppress(asyncio.QueueFull):
            self.wake_queue.put_nowait(("control", command_type, now_ms()))
        wait_timeout = timeout_sec or float(os.getenv("OPS_CONSOLE_COMMAND_TIMEOUT_SEC", "30.0"))
        self.audit_command_event(
            "ops_console_command_enqueued",
            command,
            queue_size=self.command_queue.qsize(),
            wait_timeout_sec=wait_timeout,
        )
        try:
            result = await asyncio.wait_for(future, timeout=wait_timeout)
            if isinstance(result, dict):
                enriched = dict(result)
                enriched.setdefault("command_id", command.command_id)
                return enriched
            return {"ok": True, "command_id": command.command_id, "result": result}
        except asyncio.TimeoutError:
            self.audit_command_event(
                "ops_console_command_wait_timeout",
                command,
                wait_timeout_sec=wait_timeout,
                age_ms=now_ms() - command.enqueued_at_ms,
            )
            raise


class EmbeddedControlServer:
    def __init__(self, service: StrategyControlService) -> None:
        self.service = service
        self.host = os.getenv("OPS_CONSOLE_HOST", "127.0.0.1")
        self.port = int(float(os.getenv("OPS_CONSOLE_PORT", "8010")))
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[Any] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        await self.service.start()
        try:
            from src.ops_console.app import app

            config = uvicorn.Config(
                app,
                host=self.host,
                port=self.port,
                log_level="warning",
                access_log=False,
            )
            self._server = uvicorn.Server(config)
            self._task = asyncio.create_task(self._server.serve(), name="strategy-embedded-control-server")
            await asyncio.sleep(0.2)
            if self._task.done():
                exc = self._task.exception()
                if exc is not None:
                    raise exc
        except Exception:
            await self.service.stop()
            self._task = None
            self._server = None
            raise

    async def stop(self) -> None:
        try:
            if self._server is not None:
                self._server.should_exit = True
            if self._task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._task
        finally:
            self._task = None
            self._server = None
            await self.service.stop()
