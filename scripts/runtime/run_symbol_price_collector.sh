#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

STRATEGY_ENV_FILE="${STRATEGY_ENV_FILE:-$PROJECT_ROOT/.env}"
if [[ -f "$STRATEGY_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$STRATEGY_ENV_FILE"
  set +a
fi

PYTHON_BIN="${STRATEGY_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
SCRIPT_PATH="${SYMBOL_PRICE_SCRIPT_PATH:-$PROJECT_ROOT/scripts/runtime/symbol_price_collector.py}"
SCRIPT_PATH_ABS="$(python3 - <<PY
from pathlib import Path
print(Path(r"""$SCRIPT_PATH""").expanduser().resolve())
PY
)"

LOG_DIR="${SYMBOL_PRICE_LOG_DIR:-${STRATEGY_LIVE_LOG_DIR:-$PROJECT_ROOT/logs}}"
PID_FILE="${SYMBOL_PRICE_PID_FILE:-$LOG_DIR/symbol_price_collector.pid}"
LOCK_FILE="${SYMBOL_PRICE_LOCK_FILE:-$LOG_DIR/symbol_price_collector.lock}"
STDOUT_LOG="${SYMBOL_PRICE_STDOUT_LOG:-$LOG_DIR/symbol_price_collector.out}"
RUN_DURATION="${SYMBOL_PRICE_RUN_DURATION:-}"

ACTION="${1:-start}"

: "${SYMBOL_PRICE_EXCHANGES:=binance,okx}"
: "${SYMBOL_PRICE_SYMBOLS:=ALL}"
: "${SYMBOL_PRICE_INTERVAL_SEC:=60}"
: "${SYMBOL_PRICE_IMPACT_NOTIONAL_USD:=20000}"
: "${SYMBOL_PRICE_WS_STALE_SEC:=15}"
: "${SYMBOL_PRICE_WS_FIRST:=1}"

usage() {
  cat <<'EOF'
usage: scripts/runtime/run_symbol_price_collector.sh [start|stop|status|logs|once]

commands:
  start   start background symbol_price collector
  stop    stop running collector
  status  show whether collector is running
  logs    tail -f collector stdout log
  once    collect one bucket in foreground
EOF
}

ensure_runtime() {
  mkdir -p "$LOG_DIR"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[run_symbol_price] missing python: $PYTHON_BIN" >&2
    exit 1
  fi
}

running_pids() {
  local candidates=()
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      candidates+=("$pid")
    fi
  fi
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && candidates+=("$pid")
  done < <(pgrep -f "$SCRIPT_PATH_ABS" || true)

  if [[ ${#candidates[@]} -eq 0 ]]; then
    return 1
  fi
  printf '%s\n' "${candidates[@]}" | awk '!seen[$0]++'
}

run_command() {
  env \
    STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" \
    SYMBOL_PRICE_EXCHANGES="$SYMBOL_PRICE_EXCHANGES" \
    SYMBOL_PRICE_SYMBOLS="$SYMBOL_PRICE_SYMBOLS" \
    SYMBOL_PRICE_INTERVAL_SEC="$SYMBOL_PRICE_INTERVAL_SEC" \
    SYMBOL_PRICE_IMPACT_NOTIONAL_USD="$SYMBOL_PRICE_IMPACT_NOTIONAL_USD" \
    SYMBOL_PRICE_WS_STALE_SEC="$SYMBOL_PRICE_WS_STALE_SEC" \
    SYMBOL_PRICE_WS_FIRST="$SYMBOL_PRICE_WS_FIRST" \
    "$PYTHON_BIN" -u "$SCRIPT_PATH_ABS" "$@"
}

start_run() {
  ensure_runtime

  if pids="$(running_pids 2>/dev/null || true)" && [[ -n "${pids:-}" ]]; then
    first_pid="$(printf '%s\n' "$pids" | head -n 1)"
    count="$(printf '%s\n' "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "[run_symbol_price] already running pid=$first_pid count=$count"
    exit 1
  fi

  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$ts] symbol_price collector start script=${SCRIPT_PATH_ABS}" >> "$STDOUT_LOG"

  detach_cmd=(nohup)
  if command -v setsid >/dev/null 2>&1; then
    detach_cmd=(nohup setsid)
  fi

  if [[ -n "$RUN_DURATION" ]]; then
    "${detach_cmd[@]}" env \
      STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" \
      SYMBOL_PRICE_EXCHANGES="$SYMBOL_PRICE_EXCHANGES" \
      SYMBOL_PRICE_SYMBOLS="$SYMBOL_PRICE_SYMBOLS" \
      SYMBOL_PRICE_INTERVAL_SEC="$SYMBOL_PRICE_INTERVAL_SEC" \
      SYMBOL_PRICE_IMPACT_NOTIONAL_USD="$SYMBOL_PRICE_IMPACT_NOTIONAL_USD" \
      SYMBOL_PRICE_WS_STALE_SEC="$SYMBOL_PRICE_WS_STALE_SEC" \
      SYMBOL_PRICE_WS_FIRST="$SYMBOL_PRICE_WS_FIRST" \
      flock -n "$LOCK_FILE" \
      timeout "$RUN_DURATION" \
      "$PYTHON_BIN" -u "$SCRIPT_PATH_ABS" \
      >> "$STDOUT_LOG" 2>&1 < /dev/null &
  else
    "${detach_cmd[@]}" env \
      STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" \
      SYMBOL_PRICE_EXCHANGES="$SYMBOL_PRICE_EXCHANGES" \
      SYMBOL_PRICE_SYMBOLS="$SYMBOL_PRICE_SYMBOLS" \
      SYMBOL_PRICE_INTERVAL_SEC="$SYMBOL_PRICE_INTERVAL_SEC" \
      SYMBOL_PRICE_IMPACT_NOTIONAL_USD="$SYMBOL_PRICE_IMPACT_NOTIONAL_USD" \
      SYMBOL_PRICE_WS_STALE_SEC="$SYMBOL_PRICE_WS_STALE_SEC" \
      SYMBOL_PRICE_WS_FIRST="$SYMBOL_PRICE_WS_FIRST" \
      flock -n "$LOCK_FILE" \
      "$PYTHON_BIN" -u "$SCRIPT_PATH_ABS" \
      >> "$STDOUT_LOG" 2>&1 < /dev/null &
  fi

  pid=$!
  echo "$pid" > "$PID_FILE"
  disown "$pid" 2>/dev/null || true
  sleep 0.5
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "[run_symbol_price] failed to start; see $STDOUT_LOG" >&2
    exit 1
  fi
  echo "[run_symbol_price] started pid=$pid"
  echo "[run_symbol_price] stdout=$STDOUT_LOG"
}

stop_run() {
  if ! pids="$(running_pids 2>/dev/null || true)" || [[ -z "${pids:-}" ]]; then
    echo "[run_symbol_price] not running"
    rm -f "$PID_FILE"
    exit 0
  fi

  mapfile -t pid_array < <(printf '%s\n' "$pids" | sed '/^$/d' | awk '!seen[$0]++')
  kill "${pid_array[@]}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    still_running=0
    for pid in "${pid_array[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        still_running=1
        break
      fi
    done
    if [[ "$still_running" -eq 0 ]]; then
      rm -f "$PID_FILE"
      echo "[run_symbol_price] stopped pids=$(printf '%s ' "${pid_array[@]}")"
      return 0
    fi
    sleep 0.5
  done

  kill -9 "${pid_array[@]}" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "[run_symbol_price] killed pids=$(printf '%s ' "${pid_array[@]}")"
}

show_status() {
  if pids="$(running_pids 2>/dev/null || true)" && [[ -n "${pids:-}" ]]; then
    first_pid="$(printf '%s\n' "$pids" | head -n 1)"
    count="$(printf '%s\n' "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "[run_symbol_price] running pid=$first_pid count=$count"
    printf '%s\n' "$pids" | sed 's/^/[run_symbol_price] pid=/'
  else
    echo "[run_symbol_price] not running"
  fi
}

case "$ACTION" in
  start)
    start_run
    ;;
  stop)
    stop_run
    ;;
  status)
    show_status
    ;;
  logs)
    ensure_runtime
    touch "$STDOUT_LOG"
    tail -f "$STDOUT_LOG"
    ;;
  once)
    ensure_runtime
    run_command --once
    ;;
  *)
    usage
    exit 1
    ;;
esac
