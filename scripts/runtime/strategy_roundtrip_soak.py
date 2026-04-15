import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategy import (  # noqa: E402
    collect_candidates,
    load_strategy_config,
    prune_assumed_flat,
    register_pair_assumed_flat,
    resolve_strategy_symbols,
    build_strategy_exchanges,
    start_strategy_exchanges,
    stop_strategy_exchanges,
    enter_pair_limit_then_hedge,
    close_pair_limit_then_hedge,
    extract_position_qty,
)


LOG_PATH = Path(os.getenv("STRATEGY_ROUNDTRIP_LOG_PATH", ROOT / "logs" / "strategy_roundtrip_soak.jsonl"))


def json_default(value):
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"unsupported type: {type(value)!r}")


def append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")


def build_exchange_state_row(exchange: Any, symbols: list[str]) -> dict[str, Any]:
    position_qty_by_symbol: dict[str, float] = {}
    open_orders_by_symbol: dict[str, int] = {}
    total_position_abs = 0.0
    total_open_orders = 0
    for symbol in symbols:
        snapshot = exchange.get_symbol_snapshot(symbol)
        qty = extract_position_qty(snapshot)
        open_orders = len(snapshot.get("open_orders", []) or [])
        position_qty_by_symbol[symbol] = qty
        open_orders_by_symbol[symbol] = open_orders
        total_position_abs += abs(float(qty or 0.0))
        total_open_orders += int(open_orders)
    return {
        "position_qty": total_position_abs,
        "position_qty_by_symbol": position_qty_by_symbol,
        "open_orders": total_open_orders,
        "open_orders_by_symbol": open_orders_by_symbol,
    }


async def collect_fresh_exchange_state(exchange_names: list[str], symbols: list[str]) -> dict[str, dict]:
    fresh = {}
    exchanges = build_strategy_exchanges(load_strategy_config(), symbols)
    try:
        filtered = {name: exchanges[name] for name in exchange_names}
        await start_strategy_exchanges(filtered)
        await asyncio.sleep(1.8)
        for exchange_name, exchange in filtered.items():
            fresh[exchange_name] = build_exchange_state_row(exchange, symbols)
    finally:
        await stop_strategy_exchanges(exchanges)
    return fresh


def collect_runtime_exchange_state(exchanges: dict[str, Any], exchange_names: list[str], symbols: list[str]) -> dict[str, dict]:
    state: dict[str, dict] = {}
    for exchange_name in exchange_names:
        exchange = exchanges[exchange_name]
        state[exchange_name] = build_exchange_state_row(exchange, symbols)
    return state


async def wait_for_candidate(symbols, exchanges, config, assumed_flat_until_ms, timeout_sec: float):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        prune_assumed_flat(assumed_flat_until_ms)
        candidates = collect_candidates(
            symbols,
            exchanges,
            config,
            open_pairs={},
            assumed_flat_until_ms=assumed_flat_until_ms,
        )
        usable = [candidate for candidate in candidates if candidate.can_enter]
        if usable:
            return usable[0], candidates
        await asyncio.sleep(0.5)
    return None, []


async def run_cycle(cycle_idx: int, symbols, exchanges, config, assumed_flat_until_ms, scan_timeout_sec: float) -> dict:
    cycle_row: dict = {
        "cycle": cycle_idx,
        "started_at_ms": int(time.time() * 1000),
        "symbols_scanned": list(symbols),
    }
    candidate, candidates = await wait_for_candidate(symbols, exchanges, config, assumed_flat_until_ms, scan_timeout_sec)
    if candidate is None:
        cycle_row.update(
            {
                "ok": False,
                "stage": "candidate",
                "reason": "no_candidate",
                "candidate_count": len(candidates),
            }
        )
        return cycle_row

    cycle_row.update(
        {
            "symbol": candidate.symbol,
            "candidate_id": candidate.id,
            "candidate_qty": candidate.executable_qty,
            "candidate_net_edge_usd": candidate.expected_net_edge_usd,
            "long_exchange": candidate.long_exchange,
            "short_exchange": candidate.short_exchange,
        }
    )

    pair = await enter_pair_limit_then_hedge(candidate, exchanges, config)
    cycle_row["entered"] = pair is not None
    if pair is None:
        cycle_row.update({"ok": False, "stage": "enter", "reason": "enter_failed"})
        return cycle_row

    await asyncio.sleep(1.5)
    close_ok = await close_pair_limit_then_hedge(pair, exchanges, config)
    cycle_row.update(
        {
            "pair_id": pair.pair_id,
            "close_ok": bool(close_ok),
            "pair_status": pair.status,
            "pair_qty": pair.qty,
            "assumed_flat_exchanges": list(pair.assumed_flat_exchanges),
        }
    )
    if pair.status == "closed":
        register_pair_assumed_flat(pair, assumed_flat_until_ms, config)

    await asyncio.sleep(2.0)
    cycle_row["local_state"] = {
        pair.long_exchange: extract_position_qty(exchanges[pair.long_exchange].get_symbol_snapshot(pair.symbol)),
        pair.short_exchange: extract_position_qty(exchanges[pair.short_exchange].get_symbol_snapshot(pair.symbol)),
    }

    return cycle_row


async def main() -> int:
    config = load_strategy_config()
    if not config.enable_live:
        raise SystemExit("STRATEGY_ENABLE_LIVE=1 required")

    symbols = await resolve_strategy_symbols(config)

    cycles = int(os.getenv("STRATEGY_ROUNDTRIP_CYCLES", "3") or 3)
    scan_timeout_sec = float(os.getenv("STRATEGY_ROUNDTRIP_SCAN_TIMEOUT_SEC", "30") or 30.0)
    sleep_between_cycles_sec = float(os.getenv("STRATEGY_ROUNDTRIP_SLEEP_BETWEEN_SEC", "2") or 2.0)
    assumed_flat_until_ms: dict[tuple[str, str], int] = {}

    exchanges = build_strategy_exchanges(config, symbols)
    failures = 0
    rows: list[dict] = []
    try:
        await start_strategy_exchanges(exchanges)
        await asyncio.sleep(2.5)
        for cycle_idx in range(1, cycles + 1):
            row = await run_cycle(cycle_idx, symbols, exchanges, config, assumed_flat_until_ms, scan_timeout_sec)
            if row.get("stage") == "candidate" and row.get("reason") == "no_candidate":
                fresh_state = collect_runtime_exchange_state(exchanges, config.exchange_names, symbols)
                fresh_state_source = "resident"
            else:
                fresh_state_source = "fresh"
                try:
                    fresh_state = await collect_fresh_exchange_state(config.exchange_names, symbols)
                except Exception as exc:
                    fresh_state = collect_runtime_exchange_state(exchanges, config.exchange_names, symbols)
                    fresh_state_source = "resident_fallback"
                    row["fresh_state_error"] = repr(exc)
            row["fresh_state"] = fresh_state
            row["fresh_state_source"] = fresh_state_source
            row["fresh_all_flat"] = all(
                abs(float(state.get("position_qty", 0.0))) <= 1e-9 and int(state.get("open_orders", 0)) == 0
                for state in fresh_state.values()
            )
            row["finished_at_ms"] = int(time.time() * 1000)
            if row.get("stage") == "candidate" and row.get("reason") == "no_candidate":
                row["ok"] = bool(row["fresh_all_flat"])
                row["result"] = "no_candidate"
            else:
                row["ok"] = bool(row.get("close_ok")) and row["fresh_all_flat"]
                row["result"] = "roundtrip" if row.get("entered") else "enter_failed"
            if not row["ok"]:
                failures += 1
            rows.append(row)
            append_jsonl(LOG_PATH, row)
            print(json.dumps(row, ensure_ascii=False, default=json_default), flush=True)
            if cycle_idx < cycles:
                await asyncio.sleep(sleep_between_cycles_sec)
    finally:
        await stop_strategy_exchanges(exchanges)

    summary = {
        "cycles": cycles,
        "failures": failures,
        "log_path": str(LOG_PATH),
        "symbols": symbols,
    }
    print(json.dumps({"summary": summary}, ensure_ascii=False), flush=True)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
