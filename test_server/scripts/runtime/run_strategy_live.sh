#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_ROOT/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

LOG_DIR="${STRATEGY_LIVE_LOG_DIR:-$PROJECT_ROOT/logs}"
PID_FILE="${STRATEGY_LIVE_PID_FILE:-$LOG_DIR/strategy_live.pid}"
LOCK_FILE="${STRATEGY_LIVE_LOCK_FILE:-$LOG_DIR/strategy_live.lock}"
STDOUT_LOG="${STRATEGY_LIVE_STDOUT_LOG:-$LOG_DIR/strategy_live.out}"
JSONL_LOG="${STRATEGY_LIVE_EVENT_LOG_PATH:-$LOG_DIR/strategy_live.jsonl}"
RUN_META_FILE="${STRATEGY_LIVE_RUN_META_FILE:-$LOG_DIR/strategy_live.run.json}"
PYTHON_BIN="${STRATEGY_PYTHON:-$REPO_ROOT/.venv/bin/python}"
RUN_DURATION="${STRATEGY_RUN_DURATION:-6h}"
ACTION="${1:-start}"

mkdir -p "$LOG_DIR"

: "${STRATEGY_ENABLE_LIVE:=1}"
: "${STRATEGY_APPROVAL_MODE:=auto}"
: "${STRATEGY_AUTO_ENTER_ENABLED:=1}"
: "${STRATEGY_AUTO_CLOSE_ENABLED:=1}"
: "${STRATEGY_EXCHANGES:=binance,okx}"
: "${STRATEGY_ALLOWED_PAIRS:=binance-okx}"
: "${STRATEGY_SYMBOLS:=BTC,ETH,SOL,XRP,DOGE,ARB,LINK}"
: "${STRATEGY_ORDER_NOTIONAL_USD:=500}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_USD:=-0.10}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_BPS:=-10}"
: "${STRATEGY_HOLD_BUFFER_USD:=0.00}"
: "${STRATEGY_RISK_BUFFER_BPS:=0}"
: "${STRATEGY_AUTO_ENTER_COOLDOWN_SEC:=5}"
: "${STRATEGY_FIRST_LEG_TIMEOUT_SEC:=4.0}"
: "${STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC:=1.0}"
: "${STRATEGY_HEDGE_REQUOTE_COUNT:=3}"
: "${STRATEGY_MAX_OPEN_PAIRS:=4}"
: "${STRATEGY_MIN_HOLD_SEC:=60}"
: "${STRATEGY_MAX_HOLD_SEC:=14400}"
: "${STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC:=300}"
: "${STRATEGY_TAKE_PROFIT_USD:=0.05}"
: "${STRATEGY_STOP_LOSS_USD:=-0.20}"
: "${STRATEGY_RENDER_INTERVAL_SEC:=15}"
: "${STRATEGY_RECOVERY_RETRY_COOLDOWN_SEC:=20}"
: "${STRATEGY_DANGER_LOG_COOLDOWN_SEC:=20}"
: "${STRATEGY_ASSUMED_FLAT_GRACE_SEC:=8}"
: "${STRATEGY_POSITION_STALE_IGNORE_SEC:=20}"
: "${STRATEGY_ACTION_ACCOUNT_FRESH_SEC:=15}"
: "${STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC:=20}"
: "${STRATEGY_SYMBOL_REENTRY_COOLDOWN_SEC:=180}"

is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  mapfile -t pids < "$PID_FILE" || return 1
  local live=0
  for pid in "${pids[@]}"; do
    [[ -n "${pid:-}" ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      live=$((live + 1))
    fi
  done
  [[ $live -gt 0 ]]
}

write_run_meta() {
  "$PYTHON_BIN" - <<PY
import json, time
from pathlib import Path
Path("$RUN_META_FILE").write_text(json.dumps({
    "started_at_ms": int(time.time() * 1000),
    "symbols": "$STRATEGY_SYMBOLS",
    "exchanges": "$STRATEGY_EXCHANGES",
    "allowed_pairs": "$STRATEGY_ALLOWED_PAIRS",
    "jsonl_log": "$JSONL_LOG",
    "stdout_log": "$STDOUT_LOG",
}, ensure_ascii=False), encoding="utf-8")
PY
}

case "$ACTION" in
  start)
    if is_running; then
      echo "already running"
      exit 0
    fi
    : > "$STDOUT_LOG"
    : > "$JSONL_LOG"
    write_run_meta
    (
      flock -n 9 || exit 1
      PYTHONPATH="$PROJECT_ROOT:$REPO_ROOT:${PYTHONPATH:-}" \
      timeout "$RUN_DURATION" \
      env \
        STRATEGY_ENABLE_LIVE="$STRATEGY_ENABLE_LIVE" \
        STRATEGY_APPROVAL_MODE="$STRATEGY_APPROVAL_MODE" \
        STRATEGY_AUTO_ENTER_ENABLED="$STRATEGY_AUTO_ENTER_ENABLED" \
        STRATEGY_AUTO_CLOSE_ENABLED="$STRATEGY_AUTO_CLOSE_ENABLED" \
        STRATEGY_EXCHANGES="$STRATEGY_EXCHANGES" \
        STRATEGY_ALLOWED_PAIRS="$STRATEGY_ALLOWED_PAIRS" \
        STRATEGY_SYMBOLS="$STRATEGY_SYMBOLS" \
        STRATEGY_ORDER_NOTIONAL_USD="$STRATEGY_ORDER_NOTIONAL_USD" \
        STRATEGY_MIN_EXPECTED_NET_EDGE_USD="$STRATEGY_MIN_EXPECTED_NET_EDGE_USD" \
        STRATEGY_MIN_EXPECTED_NET_EDGE_BPS="$STRATEGY_MIN_EXPECTED_NET_EDGE_BPS" \
        STRATEGY_HOLD_BUFFER_USD="$STRATEGY_HOLD_BUFFER_USD" \
        STRATEGY_RISK_BUFFER_BPS="$STRATEGY_RISK_BUFFER_BPS" \
        STRATEGY_AUTO_ENTER_COOLDOWN_SEC="$STRATEGY_AUTO_ENTER_COOLDOWN_SEC" \
        STRATEGY_FIRST_LEG_TIMEOUT_SEC="$STRATEGY_FIRST_LEG_TIMEOUT_SEC" \
        STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC="$STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC" \
        STRATEGY_HEDGE_REQUOTE_COUNT="$STRATEGY_HEDGE_REQUOTE_COUNT" \
        STRATEGY_MAX_OPEN_PAIRS="$STRATEGY_MAX_OPEN_PAIRS" \
        STRATEGY_MIN_HOLD_SEC="$STRATEGY_MIN_HOLD_SEC" \
        STRATEGY_MAX_HOLD_SEC="$STRATEGY_MAX_HOLD_SEC" \
        STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC="$STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC" \
        STRATEGY_TAKE_PROFIT_USD="$STRATEGY_TAKE_PROFIT_USD" \
        STRATEGY_STOP_LOSS_USD="$STRATEGY_STOP_LOSS_USD" \
        STRATEGY_RENDER_INTERVAL_SEC="$STRATEGY_RENDER_INTERVAL_SEC" \
        STRATEGY_RECOVERY_RETRY_COOLDOWN_SEC="$STRATEGY_RECOVERY_RETRY_COOLDOWN_SEC" \
        STRATEGY_DANGER_LOG_COOLDOWN_SEC="$STRATEGY_DANGER_LOG_COOLDOWN_SEC" \
        STRATEGY_ASSUMED_FLAT_GRACE_SEC="$STRATEGY_ASSUMED_FLAT_GRACE_SEC" \
        STRATEGY_POSITION_STALE_IGNORE_SEC="$STRATEGY_POSITION_STALE_IGNORE_SEC" \
        STRATEGY_ACTION_ACCOUNT_FRESH_SEC="$STRATEGY_ACTION_ACCOUNT_FRESH_SEC" \
        STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC="$STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC" \
        STRATEGY_SYMBOL_REENTRY_COOLDOWN_SEC="$STRATEGY_SYMBOL_REENTRY_COOLDOWN_SEC" \
        STRATEGY_LIVE_EVENT_LOG_PATH="$JSONL_LOG" \
        "$PYTHON_BIN" -u "$PROJECT_ROOT/strategy.py" >> "$STDOUT_LOG" 2>&1
    ) 9>"$LOCK_FILE" &
    sleep 1
    pgrep -f "$PROJECT_ROOT/strategy.py|$RUN_DURATION|$LOCK_FILE" | sort -u > "$PID_FILE" || true
    echo "started"
    ;;
  stop)
    if ! [[ -f "$PID_FILE" ]]; then
      echo "not running"
      exit 0
    fi
    mapfile -t pids < "$PID_FILE" || true
    if [[ ${#pids[@]} -eq 0 ]]; then
      rm -f "$PID_FILE"
      echo "not running"
      exit 0
    fi
    kill "${pids[@]}" 2>/dev/null || true
    sleep 1
    kill -9 "${pids[@]}" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "stopped"
    ;;
  status)
    if is_running; then
      mapfile -t pids < "$PID_FILE"
      echo "running pid=${pids[0]} count=${#pids[@]}"
    else
      echo "not running"
    fi
    ;;
  logs)
    tail -f "$STDOUT_LOG"
    ;;
  jsonl)
    tail -f "$JSONL_LOG"
    ;;
  report)
    PYTHONPATH="$PROJECT_ROOT:$REPO_ROOT:${PYTHONPATH:-}" \
    TARGET_EXCHANGES="$STRATEGY_EXCHANGES" SYMBOLS="$STRATEGY_SYMBOLS" \
    "$PYTHON_BIN" -u "$REPO_ROOT/scripts/ops/report.py" --wait-ws-sec 1 --trade-limit 3
    ;;
  ledger)
    PYTHONPATH="$PROJECT_ROOT:$REPO_ROOT:${PYTHONPATH:-}" \
    "$PYTHON_BIN" -u "$REPO_ROOT/scripts/runtime/strategy_pair_ledger.py" \
      --run-meta "$RUN_META_FILE" \
      --jsonl-path "$JSONL_LOG" \
      --limit 10
    ;;
  summary)
    PYTHONPATH="$PROJECT_ROOT:$REPO_ROOT:${PYTHONPATH:-}" \
    "$PYTHON_BIN" -u "$REPO_ROOT/scripts/runtime/strategy_pair_ledger.py" \
      --run-meta "$RUN_META_FILE" \
      --jsonl-path "$JSONL_LOG" \
      --limit 5
    ;;
  *)
    echo "usage: $0 {start|stop|status|logs|jsonl|report|ledger|summary}"
    exit 1
    ;;
esac
