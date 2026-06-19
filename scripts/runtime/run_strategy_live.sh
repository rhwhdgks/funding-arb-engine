#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

STRATEGY_ENV_FILE="${STRATEGY_ENV_FILE:-$PROJECT_ROOT/.env}"
STRATEGY_RUN_DURATION_OVERRIDE="${STRATEGY_RUN_DURATION:-}"

trim_env_part() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

load_env_file() {
  local env_file="$1"
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" == *"="* ]] || continue

    key="$(trim_env_part "${line%%=*}")"
    value="$(trim_env_part "${line#*=}")"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue

    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "$key=$value"
  done < "$env_file"
}

if [[ -f "$STRATEGY_ENV_FILE" ]]; then
  load_env_file "$STRATEGY_ENV_FILE"
fi

if [[ -n "$STRATEGY_RUN_DURATION_OVERRIDE" ]]; then
  export STRATEGY_RUN_DURATION="$STRATEGY_RUN_DURATION_OVERRIDE"
fi

STRATEGY_SCRIPT_PATH="${STRATEGY_SCRIPT_PATH:-$PROJECT_ROOT/main.py}"
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

is_forever_duration() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    0|forever|infinite|infinity|none|unlimited)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ACTION="${1:-start}"
if [[ $# -gt 0 ]]; then
  shift
fi

: "${STRATEGY_ENABLE_LIVE:=1}"
: "${STRATEGY_APPROVAL_MODE:=auto}"
: "${STRATEGY_AUTO_ENTER_ENABLED:=1}"
: "${STRATEGY_AUTO_CLOSE_ENABLED:=1}"
: "${STRATEGY_MAKER_ENGINE_ENABLED:=0}"
: "${STRATEGY_EXECUTION_PROFILE:=}"
: "${STRATEGY_ACCOUNT_LABEL:=real}"
: "${STRATEGY_LOGIC:=funding fee}"
: "${STRATEGY_EXCHANGES:=binance,okx}"
: "${STRATEGY_ALLOWED_PAIRS:=binance-okx}"
: "${STRATEGY_SYMBOLS:=ETH,SOL,XRP,DOGE,SEI,PENGU,ARB,LINK}"
: "${STRATEGY_LEVERAGE:=3}"
: "${STRATEGY_MAX_GROSS_LEVERAGE:=3.0}"
: "${STRATEGY_MAX_LONG_LEVERAGE:=0}"
: "${STRATEGY_MAX_SHORT_LEVERAGE:=0}"
: "${STRATEGY_RATE_LIMIT_GUARD_ENABLED:=1}"
: "${STRATEGY_RATE_LIMIT_USAGE_THRESHOLD:=0.85}"
: "${STRATEGY_ORDER_NOTIONAL_USD:=50}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_USD:=0.04}"
: "${STRATEGY_MIN_EXPECTED_NET_EDGE_BPS:=10}"
: "${STRATEGY_ENTRY_FUNDING_FORECAST_SEC:=300}"
: "${STRATEGY_HOLD_BUFFER_USD:=0.02}"
: "${STRATEGY_RISK_BUFFER_BPS:=4}"
: "${STRATEGY_AUTO_ENTER_COOLDOWN_SEC:=30}"
: "${STRATEGY_MIN_HOLD_SEC:=300}"
: "${STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC:=3600}"
: "${STRATEGY_MAX_HOLD_SEC:=43200}"
: "${STRATEGY_TAKE_PROFIT_USD:=0.18}"
: "${STRATEGY_STOP_LOSS_USD:=-0.12}"
: "${STRATEGY_TAKE_PROFIT_BPS:=6}"
: "${STRATEGY_STOP_LOSS_BPS:=8}"
: "${STRATEGY_HOLD_BUFFER_BPS:=2}"
: "${STRATEGY_MAX_OPEN_PAIRS:=5}"
: "${STRATEGY_POSITION_STALE_IGNORE_SEC:=20}"
: "${STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC:=15}"
: "${STRATEGY_ACTION_ACCOUNT_FRESH_SEC:=2.5}"
: "${STRATEGY_GRACEFUL_SHUTDOWN_ENABLED:=1}"
: "${STRATEGY_GRACEFUL_SHUTDOWN_CANCEL_ORDERS:=1}"
: "${STRATEGY_GRACEFUL_SHUTDOWN_FLATTEN_POSITIONS:=1}"
: "${STRATEGY_STOP_GRACE_SEC:=180}"
: "${ENABLE_DB_PERSISTENCE:=0}"
: "${MARIADB_HOST:=}"
: "${MARIADB_PORT:=3306}"
: "${MARIADB_USER:=}"
: "${MARIADB_PASSWORD:=}"
: "${MARIADB_DATABASE:=trading_dev}"

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    1|true|yes|on|y)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

normalize_execution_profile() {
  local profile
  profile="$(printf '%s' "${STRATEGY_EXECUTION_PROFILE:-}" | tr '[:upper:]' '[:lower:]')"
  case "$profile" in
    monitor|observe|observer|no-entry|no_entry)
      STRATEGY_EXECUTION_PROFILE="monitor"
      STRATEGY_AUTO_ENTER_ENABLED=0
      STRATEGY_MAKER_ENGINE_ENABLED=0
      ;;
    auto|auto-trade|auto_trade|live)
      STRATEGY_EXECUTION_PROFILE="auto_trade"
      ;;
    dry-run|dry_run|paper)
      STRATEGY_EXECUTION_PROFILE="dry_run"
      STRATEGY_ENABLE_LIVE=0
      ;;
    "")
      if ! is_truthy "$STRATEGY_ENABLE_LIVE"; then
        STRATEGY_EXECUTION_PROFILE="dry_run"
      elif is_truthy "$STRATEGY_AUTO_ENTER_ENABLED" || is_truthy "$STRATEGY_MAKER_ENGINE_ENABLED"; then
        STRATEGY_EXECUTION_PROFILE="auto_trade"
      else
        STRATEGY_EXECUTION_PROFILE="monitor"
      fi
      ;;
    *)
      echo "[run_strategy_live] invalid STRATEGY_EXECUTION_PROFILE=${STRATEGY_EXECUTION_PROFILE}" >&2
      echo "[run_strategy_live] expected one of: auto_trade, monitor, dry_run" >&2
      exit 1
      ;;
  esac
}

normalize_execution_profile

usage() {
  cat <<'EOF'
usage: scripts/runtime/run_strategy_live.sh [start|stop|status|logs|jsonl|summary|report|health|db-smoke|drill|final-report|opportunity-report]

commands:
  start   start background live strategy runtime
  stop    stop running live strategy runtime
  status  show whether the live strategy runtime is running
  logs    tail -f stdout log
  jsonl   tail -f structured live event jsonl
  summary print current-run live event summary
  report  show current exchange report snapshot
  health  show ops health for runtime, collector, DB, and recent logs
  db-smoke  verify DB persistence is isolated from core runtime paths
  drill   run non-destructive ops drill checks; pass --apply-pause-drill for pause/resume mutation
  final-report  write final PASS/FAIL report for the latest run
  opportunity-report  write candidate opportunity/no-entry report for the latest run

execution profiles:
  STRATEGY_EXECUTION_PROFILE=auto_trade  normal live auto-entry runtime
  STRATEGY_EXECUTION_PROFILE=monitor     live reads/close safety, no new entries
  STRATEGY_EXECUTION_PROFILE=dry_run     no live execution
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
  "execution_profile": "${STRATEGY_EXECUTION_PROFILE}",
  "enable_live": "${STRATEGY_ENABLE_LIVE}",
  "approval_mode": "${STRATEGY_APPROVAL_MODE}",
  "auto_enter_enabled": "${STRATEGY_AUTO_ENTER_ENABLED}",
  "auto_close_enabled": "${STRATEGY_AUTO_CLOSE_ENABLED}",
  "maker_entry_engine_enabled": "${STRATEGY_MAKER_ENGINE_ENABLED}",
  "symbols": "${STRATEGY_SYMBOLS}",
  "exchanges": "${STRATEGY_EXCHANGES}",
  "account_label": "${STRATEGY_ACCOUNT_LABEL}",
  "logic": "${STRATEGY_LOGIC}",
  "leverage": "${STRATEGY_LEVERAGE}",
  "order_notional_usd": "${STRATEGY_ORDER_NOTIONAL_USD}",
  "max_open_pairs": "${STRATEGY_MAX_OPEN_PAIRS}",
  "max_gross_leverage": "${STRATEGY_MAX_GROSS_LEVERAGE}",
  "max_long_leverage": "${STRATEGY_MAX_LONG_LEVERAGE}",
  "max_short_leverage": "${STRATEGY_MAX_SHORT_LEVERAGE}",
  "db_persistence": "${ENABLE_DB_PERSISTENCE}",
  "db_database": "${MARIADB_DATABASE}",
  "rate_limit_guard_enabled": "${STRATEGY_RATE_LIMIT_GUARD_ENABLED}",
  "rate_limit_usage_threshold": "${STRATEGY_RATE_LIMIT_USAGE_THRESHOLD}",
  "entry_funding_forecast_sec": "${STRATEGY_ENTRY_FUNDING_FORECAST_SEC}",
  "take_profit_bps": "${STRATEGY_TAKE_PROFIT_BPS}",
  "stop_loss_bps": "${STRATEGY_STOP_LOSS_BPS}",
  "hold_buffer_bps": "${STRATEGY_HOLD_BUFFER_BPS}"
}
EOF

  (
    trap 'rm -f "$PID_FILE"' EXIT
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] [런타임][시작] 기간=${RUN_DURATION} 스크립트=${STRATEGY_SCRIPT_PATH_ABS}" >> "$STDOUT_LOG"

    cd "$STRATEGY_WORKDIR_ABS"
    run_cmd=()
    if is_forever_duration "$RUN_DURATION"; then
      # Keep the same process wrapper as timed runs, but avoid operationally meaningful forced exits.
      run_cmd=(timeout 3650d "$PYTHON_BIN" -u "$STRATEGY_SCRIPT_PATH_ABS")
    else
      run_cmd=(timeout "$RUN_DURATION" "$PYTHON_BIN" -u "$STRATEGY_SCRIPT_PATH_ABS")
    fi

    set +e
    flock -n "$LOCK_FILE" env \
      STRATEGY_ENABLE_LIVE="$STRATEGY_ENABLE_LIVE" \
      STRATEGY_APPROVAL_MODE="$STRATEGY_APPROVAL_MODE" \
      STRATEGY_EXECUTION_PROFILE="$STRATEGY_EXECUTION_PROFILE" \
      STRATEGY_AUTO_ENTER_ENABLED="$STRATEGY_AUTO_ENTER_ENABLED" \
      STRATEGY_AUTO_CLOSE_ENABLED="$STRATEGY_AUTO_CLOSE_ENABLED" \
      STRATEGY_MAKER_ENGINE_ENABLED="$STRATEGY_MAKER_ENGINE_ENABLED" \
      STRATEGY_ACCOUNT_LABEL="$STRATEGY_ACCOUNT_LABEL" \
      STRATEGY_LOGIC="$STRATEGY_LOGIC" \
      STRATEGY_EXCHANGES="$STRATEGY_EXCHANGES" \
      STRATEGY_ALLOWED_PAIRS="$STRATEGY_ALLOWED_PAIRS" \
      STRATEGY_SYMBOLS="$STRATEGY_SYMBOLS" \
      STRATEGY_LEVERAGE="$STRATEGY_LEVERAGE" \
      STRATEGY_MAX_GROSS_LEVERAGE="$STRATEGY_MAX_GROSS_LEVERAGE" \
      STRATEGY_MAX_LONG_LEVERAGE="$STRATEGY_MAX_LONG_LEVERAGE" \
      STRATEGY_MAX_SHORT_LEVERAGE="$STRATEGY_MAX_SHORT_LEVERAGE" \
      STRATEGY_RATE_LIMIT_GUARD_ENABLED="$STRATEGY_RATE_LIMIT_GUARD_ENABLED" \
      STRATEGY_RATE_LIMIT_USAGE_THRESHOLD="$STRATEGY_RATE_LIMIT_USAGE_THRESHOLD" \
      STRATEGY_ORDER_NOTIONAL_USD="$STRATEGY_ORDER_NOTIONAL_USD" \
      STRATEGY_MIN_EXPECTED_NET_EDGE_USD="$STRATEGY_MIN_EXPECTED_NET_EDGE_USD" \
      STRATEGY_MIN_EXPECTED_NET_EDGE_BPS="$STRATEGY_MIN_EXPECTED_NET_EDGE_BPS" \
      STRATEGY_ENTRY_FUNDING_FORECAST_SEC="$STRATEGY_ENTRY_FUNDING_FORECAST_SEC" \
      STRATEGY_HOLD_BUFFER_USD="$STRATEGY_HOLD_BUFFER_USD" \
      STRATEGY_RISK_BUFFER_BPS="$STRATEGY_RISK_BUFFER_BPS" \
      STRATEGY_AUTO_ENTER_COOLDOWN_SEC="$STRATEGY_AUTO_ENTER_COOLDOWN_SEC" \
      STRATEGY_MIN_HOLD_SEC="$STRATEGY_MIN_HOLD_SEC" \
      STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC="$STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC" \
      STRATEGY_MAX_HOLD_SEC="$STRATEGY_MAX_HOLD_SEC" \
      STRATEGY_TAKE_PROFIT_USD="$STRATEGY_TAKE_PROFIT_USD" \
      STRATEGY_STOP_LOSS_USD="$STRATEGY_STOP_LOSS_USD" \
      STRATEGY_TAKE_PROFIT_BPS="$STRATEGY_TAKE_PROFIT_BPS" \
      STRATEGY_STOP_LOSS_BPS="$STRATEGY_STOP_LOSS_BPS" \
      STRATEGY_HOLD_BUFFER_BPS="$STRATEGY_HOLD_BUFFER_BPS" \
      STRATEGY_MAX_OPEN_PAIRS="$STRATEGY_MAX_OPEN_PAIRS" \
      STRATEGY_POSITION_STALE_IGNORE_SEC="$STRATEGY_POSITION_STALE_IGNORE_SEC" \
      STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC="$STRATEGY_AUTO_CLOSE_RETRY_COOLDOWN_SEC" \
      STRATEGY_ACTION_ACCOUNT_FRESH_SEC="$STRATEGY_ACTION_ACCOUNT_FRESH_SEC" \
      STRATEGY_GRACEFUL_SHUTDOWN_ENABLED="$STRATEGY_GRACEFUL_SHUTDOWN_ENABLED" \
      STRATEGY_GRACEFUL_SHUTDOWN_CANCEL_ORDERS="$STRATEGY_GRACEFUL_SHUTDOWN_CANCEL_ORDERS" \
      STRATEGY_GRACEFUL_SHUTDOWN_FLATTEN_POSITIONS="$STRATEGY_GRACEFUL_SHUTDOWN_FLATTEN_POSITIONS" \
      ENABLE_DB_PERSISTENCE="$ENABLE_DB_PERSISTENCE" \
      MARIADB_HOST="$MARIADB_HOST" \
      MARIADB_PORT="$MARIADB_PORT" \
      MARIADB_USER="$MARIADB_USER" \
      MARIADB_PASSWORD="$MARIADB_PASSWORD" \
      MARIADB_DATABASE="$MARIADB_DATABASE" \
      STRATEGY_LIVE_EVENT_LOG_PATH="$JSONL_LOG" \
      STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" \
      "${run_cmd[@]}" >> "$STDOUT_LOG" 2>&1

    status=$?
    set -e
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] [런타임][종료] 상태코드=${status}" >> "$STDOUT_LOG"
    exit "$status"
  ) &

  pid=$!
  echo "$pid" > "$PID_FILE"
  disown "$pid" 2>/dev/null || true
  sleep 0.5
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "[run_strategy_live] failed to start; see $STDOUT_LOG" >&2
    exit 1
  fi
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
  signal_pids=()
  for pid in "${pid_array[@]}"; do
    cmdline="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$cmdline" == *python*"$STRATEGY_SCRIPT_PATH_ABS"* ]]; then
      signal_pids+=("$pid")
    fi
  done
  if [[ ${#signal_pids[@]} -eq 0 ]]; then
    signal_pids=("${pid_array[@]}")
  fi

  echo "[run_strategy_live] stopping signal_pids=$(printf '%s ' "${signal_pids[@]}") all_pids=$(printf '%s ' "${pid_array[@]}") grace_sec=${STRATEGY_STOP_GRACE_SEC}"
  kill -TERM "${signal_pids[@]}" 2>/dev/null || true
  stop_deadline=$((SECONDS + STRATEGY_STOP_GRACE_SEC))
  while [[ "$SECONDS" -lt "$stop_deadline" ]]; do
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

show_health() {
  ensure_runtime
  STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/ops_health_report.py --env-file "$STRATEGY_ENV_FILE" "$@"
}

show_db_smoke() {
  ensure_runtime
  STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/db_persistence_smoke.py "$@"
}

show_drill() {
  ensure_runtime
  STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/ops_drill.py --env-file "$STRATEGY_ENV_FILE" "$@"
}

show_final_report() {
  ensure_runtime
  STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/final_run_report.py --env-file "$STRATEGY_ENV_FILE" --meta "$RUN_META_FILE" "$@"
}

show_opportunity_report() {
  ensure_runtime
  STRATEGY_ENV_FILE="$STRATEGY_ENV_FILE" "$PYTHON_BIN" -u scripts/ops/candidate_opportunity_report.py --env-file "$STRATEGY_ENV_FILE" --meta "$RUN_META_FILE" "$@"
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
  health)
    show_health "$@"
    ;;
  db-smoke)
    show_db_smoke "$@"
    ;;
  drill)
    show_drill "$@"
    ;;
  final-report)
    show_final_report "$@"
    ;;
  opportunity-report)
    show_opportunity_report "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac
