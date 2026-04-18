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

STRATEGY_SCRIPT_PATH="${STRATEGY_SCRIPT_PATH:-$PROJECT_ROOT/strategy.py}"
STRATEGY_WORKDIR="${STRATEGY_WORKDIR:-$PROJECT_ROOT}"
STRATEGY_SCRIPT_PATH_ABS="$(python3 - <<PY
from pathlib import Path
print(Path(r"""$STRATEGY_SCRIPT_PATH""").expanduser().resolve())
PY
)"
STRATEGY_WORKDIR_ABS="$(python3 - <<PY
from pathlib import Path
print(Path(r"""$STRATEGY_WORKDIR""").expanduser().resolve())
PY
)"
PYTHON_BIN="${STRATEGY_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"

LOG_DIR="${STRATEGY_LIVE_LOG_DIR:-$PROJECT_ROOT/logs}"
PID_FILE="${STRATEGY_LIVE_PID_FILE:-$LOG_DIR/strategy_live.pid}"
LOCK_FILE="${STRATEGY_LIVE_LOCK_FILE:-$LOG_DIR/strategy_live.lock}"
STDOUT_LOG="${STRATEGY_LIVE_STDOUT_LOG:-$LOG_DIR/strategy_live.out}"
JSONL_LOG="${STRATEGY_LIVE_EVENT_LOG_PATH:-$LOG_DIR/strategy_live.jsonl}"
RUN_META_FILE="${STRATEGY_LIVE_RUN_META_FILE:-$LOG_DIR/strategy_live.run.json}"
RUN_DURATION="${STRATEGY_RUN_DURATION:-24h}"

ACTION="${1:-start}"

: "${STRATEGY_ENABLE_LIVE:=1}"
: "${STRATEGY_APPROVAL_MODE:=auto}"
: "${STRATEGY_AUTO_ENTER_ENABLED:=1}"
: "${STRATEGY_AUTO_CLOSE_ENABLED:=1}"
: "${STRATEGY_EXCHANGES:=binance,okx}"
: "${STRATEGY_ALLOWED_PAIRS:=binance-okx}"
: "${STRATEGY_SYMBOLS:=ETH,SOL,XRP,DOGE,SEI,PENGU,ARB,LINK}"
: "${STRATEGY_ORDER_NOTIONAL_USD:=50}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_USD:=0.04}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_BPS:=4}"
: "${STRATEGY_HOLD_BUFFER_USD:=0.02}"
: "${STRATEGY_RISK_BUFFER_BPS:=4}"
: "${STRATEGY_AUTO_ENTER_COOLDOWN_SEC:=30}"
: "${STRATEGY_MIN_HOLD_SEC:=300}"
: "${STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC:=3600}"
: "${STRATEGY_MAX_HOLD_SEC:=14400}"
: "${STRATEGY_TAKE_PROFIT_USD:=0.18}"
: "${STRATEGY_STOP_LOSS_USD:=-0.12}"
: "${STRATEGY_MAX_OPEN_PAIRS:=2}"
: "${STRATEGY_POSITION_STALE_IGNORE_SEC:=20}"
: "${STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC:=15}"
: "${STRATEGY_ACTION_ACCOUNT_FRESH_SEC:=2.5}"

usage() {
  cat <<'EOF'
usage: scripts/runtime/run_strategy_live.sh [start|stop|status|logs|jsonl|summary|report]

commands:
  start   start background live strategy runtime
  stop    stop running live strategy runtime
  status  show whether the live strategy runtime is running
  logs    tail -f stdout log
  jsonl   tail -f structured live event jsonl
  summary print current-run live event summary
  report  show current exchange report snapshot
EOF
}

ensure_runtime() {
  mkdir -p "$LOG_DIR"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[run_strategy_live] missing python: $PYTHON_BIN" >&2
    exit 1
  fi
}

running_pid() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "$pid"
      return 0
    fi
  fi
  local detected_pid
  detected_pid="$(pgrep -fo "$STRATEGY_SCRIPT_PATH_ABS" || true)"
  if [[ -n "$detected_pid" ]] && kill -0 "$detected_pid" 2>/dev/null; then
    echo "$detected_pid" > "$PID_FILE"
    echo "$detected_pid"
    return 0
  fi
  return 1
}

running_pids() {
  local candidates=()
  if pid="$(running_pid 2>/dev/null || true)"; then
    if [[ -n "${pid:-}" ]]; then
      candidates+=("$pid")
    fi
  fi

  while IFS= read -r pid; do
    [[ -n "$pid" ]] && candidates+=("$pid")
  done < <(pgrep -f "$STRATEGY_SCRIPT_PATH_ABS" || true)

  while IFS= read -r pid; do
    [[ -n "$pid" ]] && candidates+=("$pid")
  done < <(pgrep -f "$STRATEGY_SCRIPT_PATH_ABS" || true)

  if [[ ${#candidates[@]} -eq 0 ]]; then
    return 1
  fi

  printf '%s\n' "${candidates[@]}" | awk '!seen[$0]++'
}

start_run() {
  ensure_runtime

  if pids="$(running_pids 2>/dev/null || true)" && [[ -n "${pids:-}" ]]; then
    first_pid="$(printf '%s\n' "$pids" | head -n 1)"
    count="$(printf '%s\n' "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "[run_strategy_live] already running pid=$first_pid count=$count"
    exit 1
  fi

  rm -f "$PID_FILE"
  started_at_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
  started_at_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  cat > "$RUN_META_FILE" <<EOF
{
  "started_at_ms": ${started_at_ms},
  "started_at_iso": "${started_at_iso}",
  "duration": "${RUN_DURATION}",
  "env_file": "${STRATEGY_ENV_FILE}",
  "script_path": "${STRATEGY_SCRIPT_PATH_ABS}",
  "stdout_log": "${STDOUT_LOG}",
  "jsonl_log": "${JSONL_LOG}",
  "symbols": "${STRATEGY_SYMBOLS}",
  "exchanges": "${STRATEGY_EXCHANGES}"
}
EOF

  (
    trap 'rm -f "$PID_FILE"' EXIT
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] strategy live runtime start duration=${RUN_DURATION} script=${STRATEGY_SCRIPT_PATH_ABS}" >> "$STDOUT_LOG"

    cd "$STRATEGY_WORKDIR_ABS"
    flock -n "$LOCK_FILE" env \
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
      STRATEGY_MIN_HOLD_SEC="$STRATEGY_MIN_HOLD_SEC" \
      STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC="$STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC" \
      STRATEGY_MAX_HOLD_SEC="$STRATEGY_MAX_HOLD_SEC" \
      STRATEGY_TAKE_PROFIT_USD="$STRATEGY_TAKE_PROFIT_USD" \
      STRATEGY_STOP_LOSS_USD="$STRATEGY_STOP_LOSS_USD" \
      STRATEGY_MAX_OPEN_PAIRS="$STRATEGY_MAX_OPEN_PAIRS" \
      STRATEGY_POSITION_STALE_IGNORE_SEC="$STRATEGY_POSITION_STALE_IGNORE_SEC" \
      STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC="$STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC" \
      STRATEGY_ACTION_ACCOUNT_FRESH_SEC="$STRATEGY_ACTION_ACCOUNT_FRESH_SEC" \
      STRATEGY_LIVE_EVENT_LOG_PATH="$JSONL_LOG" \
      STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" \
      timeout "$RUN_DURATION" \
      "$PYTHON_BIN" -u "$STRATEGY_SCRIPT_PATH_ABS" >> "$STDOUT_LOG" 2>&1

    status=$?
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] strategy live runtime exit status=${status}" >> "$STDOUT_LOG"
    exit "$status"
  ) &

  pid=$!
  echo "$pid" > "$PID_FILE"
  disown "$pid" 2>/dev/null || true
  echo "[run_strategy_live] started pid=$pid"
  echo "[run_strategy_live] stdout=$STDOUT_LOG"
  echo "[run_strategy_live] jsonl=$JSONL_LOG"
}

stop_run() {
  if ! pids="$(running_pids 2>/dev/null || true)" || [[ -z "${pids:-}" ]]; then
    echo "[run_strategy_live] not running"
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
      echo "[run_strategy_live] stopped pids=$(printf '%s ' "${pid_array[@]}")"
      return 0
    fi
    sleep 0.5
  done

  kill -9 "${pid_array[@]}" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "[run_strategy_live] killed pids=$(printf '%s ' "${pid_array[@]}")"
}

show_status() {
  if pids="$(running_pids 2>/dev/null || true)" && [[ -n "${pids:-}" ]]; then
    first_pid="$(printf '%s\n' "$pids" | head -n 1)"
    count="$(printf '%s\n' "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "[run_strategy_live] running pid=$first_pid count=$count"
    printf '%s\n' "$pids" | sed 's/^/[run_strategy_live] pid=/' 
  else
    echo "[run_strategy_live] not running"
  fi
}

show_report() {
  ensure_runtime
  TARGET_EXCHANGES="$STRATEGY_EXCHANGES" SYMBOLS="$STRATEGY_SYMBOLS" STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/report.py --wait-ws-sec 1 --trade-limit 3
}

show_summary() {
  ensure_runtime
  if [[ ! -f "$RUN_META_FILE" ]]; then
    echo "[run_strategy_live] missing run meta: $RUN_META_FILE"
    exit 1
  fi
  if [[ ! -f "$JSONL_LOG" ]]; then
    echo "[run_strategy_live] missing jsonl log: $JSONL_LOG"
    exit 1
  fi
  python3 - <<PY
import json
from collections import Counter
from pathlib import Path

run_meta = json.loads(Path(r"$RUN_META_FILE").read_text(encoding="utf-8"))
started_at_ms = int(run_meta.get("started_at_ms") or 0)
rows = [json.loads(line) for line in Path(r"$JSONL_LOG").read_text(encoding="utf-8").splitlines() if line.strip()]
run_rows = [row for row in rows if int(row.get("ts_ms") or 0) >= started_at_ms]
counter = Counter(str(row.get("event_type") or "unknown") for row in run_rows)

print("run_started_at_iso =", run_meta.get("started_at_iso"))
print("run_rows =", len(run_rows))
for key in sorted(counter):
    print(f"{key} =", counter[key])
print("recent =")
for row in run_rows[-10:]:
    print(json.dumps(row, ensure_ascii=False))
PY
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
  jsonl)
    ensure_runtime
    touch "$JSONL_LOG"
    tail -f "$JSONL_LOG"
    ;;
  summary)
    show_summary
    ;;
  report)
    show_report
    ;;
  *)
    usage
    exit 1
    ;;
esac
