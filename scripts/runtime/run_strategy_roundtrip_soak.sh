#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="${STRATEGY_RUN_LOG_DIR:-$PROJECT_ROOT/logs}"
PID_FILE="${STRATEGY_RUN_PID_FILE:-$LOG_DIR/strategy_roundtrip_24h.pid}"
LOCK_FILE="${STRATEGY_RUN_LOCK_FILE:-$LOG_DIR/strategy_roundtrip_24h.lock}"
STDOUT_LOG="${STRATEGY_RUN_STDOUT_LOG:-$LOG_DIR/strategy_roundtrip_24h.out}"
JSONL_LOG="${STRATEGY_ROUNDTRIP_LOG_PATH:-$LOG_DIR/strategy_roundtrip_24h.jsonl}"
RUN_META_FILE="${STRATEGY_RUN_META_FILE:-$LOG_DIR/strategy_roundtrip_24h.run.json}"
BALANCE_SNAPSHOT_SCRIPT="${STRATEGY_BALANCE_SNAPSHOT_SCRIPT:-$PROJECT_ROOT/scripts/runtime/strategy_balance_snapshot.py}"
RUN_DURATION="${STRATEGY_RUN_DURATION:-24h}"

ACTION="${1:-start}"

: "${STRATEGY_ENABLE_LIVE:=1}"
: "${STRATEGY_ORDER_NOTIONAL_USD:=10}"
: "${STRATEGY_SYMBOLS:=ORDER}"
: "${STRATEGY_ALLOWED_PAIRS:=binance-okx}"
: "${STRATEGY_EXCHANGES:=binance,okx}"
: "${STRATEGY_APPROVAL_MODE:=auto}"
: "${STRATEGY_ASSUMED_FLAT_GRACE_SEC:=8}"
: "${STRATEGY_ROUNDTRIP_CYCLES:=999999}"
: "${STRATEGY_ROUNDTRIP_SCAN_TIMEOUT_SEC:=30}"
: "${STRATEGY_ROUNDTRIP_SLEEP_BETWEEN_SEC:=2}"


usage() {
  cat <<'EOF'
usage: scripts/runtime/run_strategy_roundtrip_soak.sh [start|stop|status|logs|jsonl|summary]

commands:
  start   start background 24h live roundtrip soak
  stop    stop running soak process
  status  show whether the soak process is running
  logs    tail -f stdout log
  jsonl   tail -f jsonl result log
  summary print recent jsonl summary
EOF
}


ensure_runtime() {
  mkdir -p "$LOG_DIR"
  if [[ ! -x ".venv/bin/python" ]]; then
    echo "[run_strategy_roundtrip_soak] missing .venv/bin/python" >&2
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
  detected_pid="$(pgrep -fo '.venv/bin/python -u scripts/runtime/strategy_roundtrip_soak.py' || true)"
  if [[ -n "$detected_pid" ]] && kill -0 "$detected_pid" 2>/dev/null; then
    echo "$detected_pid" > "$PID_FILE"
    echo "$detected_pid"
    return 0
  fi
  return 1
}


start_run() {
  ensure_runtime

  if pid="$(running_pid)"; then
    echo "[run_strategy_roundtrip_soak] already running pid=$pid"
    exit 1
  fi

  rm -f "$PID_FILE"

  (
    trap 'rm -f "$PID_FILE"' EXIT
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    run_started_at_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
    start_balances_json="$(
      .venv/bin/python "$BALANCE_SNAPSHOT_SCRIPT" \
        --exchanges "$STRATEGY_EXCHANGES" \
        --symbols "$STRATEGY_SYMBOLS" \
        --wait-ws-sec 1.5 2>/dev/null || true
    )"
    START_BALANCES_JSON="$start_balances_json" python3 - <<PY > "$RUN_META_FILE"
import json
import os

raw = os.getenv("START_BALANCES_JSON", "").strip()
try:
    start_balances = json.loads(raw) if raw else None
except Exception:
    start_balances = {"_raw": raw}

meta = {
    "run_started_at_iso": "$ts",
    "run_started_at_ms": $run_started_at_ms,
    "stdout_log": "$STDOUT_LOG",
    "jsonl_log": "$JSONL_LOG",
    "exchange_names": "$STRATEGY_EXCHANGES".split(","),
    "symbols": "$STRATEGY_SYMBOLS",
    "start_balances": start_balances,
}
print(json.dumps(meta, ensure_ascii=False))
PY
    echo "[$ts] strategy roundtrip soak start duration=${RUN_DURATION}" >> "$STDOUT_LOG"

    flock -n "$LOCK_FILE" env \
      STRATEGY_ENABLE_LIVE="$STRATEGY_ENABLE_LIVE" \
      STRATEGY_ORDER_NOTIONAL_USD="$STRATEGY_ORDER_NOTIONAL_USD" \
      STRATEGY_SYMBOLS="$STRATEGY_SYMBOLS" \
      STRATEGY_ALLOWED_PAIRS="$STRATEGY_ALLOWED_PAIRS" \
      STRATEGY_EXCHANGES="$STRATEGY_EXCHANGES" \
      STRATEGY_APPROVAL_MODE="$STRATEGY_APPROVAL_MODE" \
      STRATEGY_ASSUMED_FLAT_GRACE_SEC="$STRATEGY_ASSUMED_FLAT_GRACE_SEC" \
      STRATEGY_ROUNDTRIP_CYCLES="$STRATEGY_ROUNDTRIP_CYCLES" \
      STRATEGY_ROUNDTRIP_SCAN_TIMEOUT_SEC="$STRATEGY_ROUNDTRIP_SCAN_TIMEOUT_SEC" \
      STRATEGY_ROUNDTRIP_SLEEP_BETWEEN_SEC="$STRATEGY_ROUNDTRIP_SLEEP_BETWEEN_SEC" \
      STRATEGY_ROUNDTRIP_LOG_PATH="$JSONL_LOG" \
      timeout "$RUN_DURATION" \
      .venv/bin/python -u scripts/runtime/strategy_roundtrip_soak.py >> "$STDOUT_LOG" 2>&1

    status=$?
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] strategy roundtrip soak exit status=${status}" >> "$STDOUT_LOG"
    exit "$status"
  ) &

  pid=$!
  echo "$pid" > "$PID_FILE"
  disown "$pid" 2>/dev/null || true
  echo "[run_strategy_roundtrip_soak] started pid=$pid"
  echo "[run_strategy_roundtrip_soak] stdout=$STDOUT_LOG"
  echo "[run_strategy_roundtrip_soak] jsonl=$JSONL_LOG"
}


stop_run() {
  if ! pid="$(running_pid)"; then
    echo "[run_strategy_roundtrip_soak] not running"
    rm -f "$PID_FILE"
    exit 0
  fi

  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "[run_strategy_roundtrip_soak] stopped pid=$pid"
      return 0
    fi
    sleep 0.5
  done

  kill -9 "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "[run_strategy_roundtrip_soak] killed pid=$pid"
}


show_status() {
  if pid="$(running_pid)"; then
    echo "[run_strategy_roundtrip_soak] running pid=$pid"
  else
    echo "[run_strategy_roundtrip_soak] not running"
  fi
}


show_summary() {
  if [[ ! -f "$JSONL_LOG" ]]; then
    echo "[run_strategy_roundtrip_soak] missing jsonl log: $JSONL_LOG"
    exit 1
  fi
  python3 - <<PY
import datetime
import json
import os
import re
import subprocess
from pathlib import Path

def parse_maybe_noisy_json(raw):
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except Exception:
            continue
    return None

def normalize_balance_snapshot_map(raw):
    parsed = parse_maybe_noisy_json(raw)
    if isinstance(parsed, dict) and "_raw" in parsed and len(parsed) == 1:
        parsed = parse_maybe_noisy_json(parsed.get("_raw"))
    if not isinstance(parsed, dict):
        return None
    normalized = {}
    for exchange_name, row in parsed.items():
        if not isinstance(row, dict):
            continue
        normalized[exchange_name] = row
    return normalized or None

jsonl_path = Path(r"$JSONL_LOG")
stdout_path = Path(r"$STDOUT_LOG")
meta_path = Path(r"$RUN_META_FILE")
rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip()]

run_started_at_ms = None
run_started_at_iso = None
if meta_path.exists():
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        run_started_at_ms = int(meta.get("run_started_at_ms") or 0) or None
        run_started_at_iso = meta.get("run_started_at_iso")
    except Exception:
        pass

if run_started_at_ms is None and stdout_path.exists():
    text = stdout_path.read_text(encoding="utf-8", errors="ignore")
    matches = list(re.finditer(r"^\\[(.*?)\\] strategy roundtrip soak start duration=.*$", text, re.M))
    if matches:
        run_started_at_iso = matches[-1].group(1)
        try:
            dt = datetime.datetime.strptime(run_started_at_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
            run_started_at_ms = int(dt.timestamp() * 1000)
        except Exception:
            run_started_at_ms = None

run_rows = rows if run_started_at_ms is None else [row for row in rows if int(row.get("started_at_ms") or 0) >= run_started_at_ms]

start_balances = meta.get("start_balances") if 'meta' in locals() else None
start_balances = normalize_balance_snapshot_map(start_balances)
current_balances = None
balance_delta_total = {}
balance_delta_equity = {}
balance_delta_total_sum = None
balance_delta_equity_sum = None

symbols = None
exchange_names = None
if 'meta' in locals():
    symbols = meta.get("symbols") or meta.get("symbol")
    exchange_names = meta.get("exchange_names")

if symbols and exchange_names:
    cmd = [
        ".venv/bin/python",
        r"$BALANCE_SNAPSHOT_SCRIPT",
        "--exchanges",
        ",".join(exchange_names),
        "--symbols",
        symbols,
        "--wait-ws-sec",
        "1.5",
    ]
    try:
        output = subprocess.check_output(cmd, cwd=r"$PROJECT_ROOT", text=True, stderr=subprocess.DEVNULL)
        current_balances = normalize_balance_snapshot_map(output)
    except Exception:
        current_balances = None

if isinstance(start_balances, dict) and isinstance(current_balances, dict):
    total_sum = 0.0
    equity_sum = 0.0
    total_ok = False
    equity_ok = False
    for exchange_name in sorted(set(start_balances) | set(current_balances)):
        before = start_balances.get(exchange_name) or {}
        after = current_balances.get(exchange_name) or {}
        before_total = before.get("total_balance")
        after_total = after.get("total_balance")
        before_equity = before.get("actual_equity_usd")
        after_equity = after.get("actual_equity_usd")
        if before_total is not None and after_total is not None and "error" not in before and "error" not in after:
            delta_total = float(after_total) - float(before_total)
            balance_delta_total[exchange_name] = delta_total
            total_sum += delta_total
            total_ok = True
        if before_equity is not None and after_equity is not None and "error" not in before and "error" not in after:
            delta_equity = float(after_equity) - float(before_equity)
            balance_delta_equity[exchange_name] = delta_equity
            equity_sum += delta_equity
            equity_ok = True
    if total_ok:
        balance_delta_total_sum = total_sum
    if equity_ok:
        balance_delta_equity_sum = equity_sum

print("rows_total =", len(rows))
print("run_started_at_iso =", run_started_at_iso)
print("run_rows =", len(run_rows))
print("run_ok =", sum(1 for row in run_rows if row.get("ok") is True))
print("run_roundtrip =", sum(1 for row in run_rows if row.get("result") == "roundtrip"))
print("run_no_candidate =", sum(1 for row in run_rows if row.get("result") == "no_candidate"))
print("start_balances =", json.dumps(start_balances, ensure_ascii=False) if start_balances is not None else "unavailable")
print("current_balances =", json.dumps(current_balances, ensure_ascii=False) if current_balances is not None else "unavailable")
print("delta_total_balance =", json.dumps(balance_delta_total, ensure_ascii=False) if balance_delta_total else "unavailable")
print("delta_actual_equity =", json.dumps(balance_delta_equity, ensure_ascii=False) if balance_delta_equity else "unavailable")
print("delta_total_balance_sum =", balance_delta_total_sum if balance_delta_total_sum is not None else "unavailable")
print("delta_actual_equity_sum =", balance_delta_equity_sum if balance_delta_equity_sum is not None else "unavailable")
print("recent =")
for row in run_rows[-5:]:
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
  *)
    usage
    exit 1
    ;;
esac
