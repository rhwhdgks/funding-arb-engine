#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

CMD="${1:-start}"
ENV_FILE="${STRATEGY_ENV_FILE:-.env}"

env_file_value() {
  local key="$1"
  local path="$ENV_FILE"
  if [[ "$path" != /* ]]; then
    path="$PROJECT_ROOT/$path"
  fi
  [[ -f "$path" ]] || return 1
  grep -E "^${key}=" "$path" | tail -n 1 | cut -d= -f2- | tr -d '\r'
}

HOST="${OPS_CONSOLE_HOST:-$(env_file_value OPS_CONSOLE_HOST || true)}"
HOST="${HOST:-0.0.0.0}"
PORT="${OPS_CONSOLE_PORT:-$(env_file_value OPS_CONSOLE_PORT || true)}"
PORT="${PORT:-8010}"
LOG_DIR="${OPS_CONSOLE_LOG_DIR:-$PROJECT_ROOT/logs}"
PID_FILE="${OPS_CONSOLE_PID_FILE:-$LOG_DIR/ops_console.pid}"
STDOUT_LOG="${OPS_CONSOLE_STDOUT_LOG:-$LOG_DIR/ops_console.out}"
PYTHON_BIN="${OPS_CONSOLE_PYTHON_BIN:-$PROJECT_ROOT/.venv/bin/python}"

mkdir -p "$LOG_DIR"

is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

client_host="$HOST"
if [[ "$client_host" == "0.0.0.0" || "$client_host" == "::" || -z "$client_host" ]]; then
  client_host="127.0.0.1"
fi
HEALTH_URL="${OPS_CONSOLE_HEALTH_URL:-http://$client_host:$PORT/healthz}"

health_check() {
  local python_bin="$PYTHON_BIN"
  if [[ ! -x "$python_bin" ]]; then
    python_bin="$(command -v python3)"
  fi
  OPS_CONSOLE_HEALTH_URL="$HEALTH_URL" "$python_bin" - <<'PY'
import json
import os
from urllib.request import Request, urlopen

url = os.environ["OPS_CONSOLE_HEALTH_URL"]
try:
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=2.0) as response:
        body = response.read(4096).decode("utf-8", errors="replace")
        payload = json.loads(body)
        ok = 200 <= response.status < 300 and bool(payload.get("ok", True))
        print(json.dumps({
            "ok": ok,
            "status": response.status,
            "payload": payload,
            "url": url,
        }, ensure_ascii=False))
        raise SystemExit(0 if ok else 1)
except Exception as exc:
    print(json.dumps({"ok": False, "error": repr(exc), "url": url}, ensure_ascii=False))
    raise SystemExit(1)
PY
}

case "$CMD" in
  start)
    if is_running; then
      echo "[run_ops_console] legacy standalone pid detected pid=$(cat "$PID_FILE")" >&2
      echo "[run_ops_console] standalone console is disabled; run '$0 stop' to remove it, then start the strategy runtime" >&2
      exit 1
    fi
    if health_json="$(health_check 2>/dev/null)"; then
      echo "[run_ops_console] embedded console already available url=$HEALTH_URL"
      echo "[run_ops_console] payload=$health_json"
      exit 0
    fi
    echo "[run_ops_console] strategy-attached console not reachable at $HEALTH_URL" >&2
    echo "[run_ops_console] start the strategy runtime first; standalone console startup is disabled" >&2
    exit 1
    ;;
  stop)
    if ! is_running; then
      if health_json="$(health_check 2>/dev/null)"; then
        echo "[run_ops_console] embedded console is attached to the strategy process; stop the strategy runtime instead"
        echo "[run_ops_console] payload=$health_json"
        exit 0
      fi
      echo "[run_ops_console] not running"
      rm -f "$PID_FILE"
      exit 0
    fi
    pid="$(cat "$PID_FILE")"
    echo "[run_ops_console] stopping pid=$pid"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "[run_ops_console] stopped"
        exit 0
      fi
      sleep 0.5
    done
    echo "[run_ops_console] force stopping pid=$pid"
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    ;;
  restart)
    "$0" stop
    "$0" start
    ;;
  status)
    if is_running; then
      echo "[run_ops_console] legacy standalone pid detected pid=$(cat "$PID_FILE")" >&2
      echo "[run_ops_console] standalone console is disabled; run '$0 stop' to remove it" >&2
      exit 1
    elif health_json="$(health_check 2>/dev/null)"; then
      echo "[run_ops_console] running embedded url=$HEALTH_URL"
      echo "[run_ops_console] payload=$health_json"
    else
      echo "[run_ops_console] not running"
      exit 1
    fi
    ;;
  health)
    if health_json="$(health_check)"; then
      echo "[run_ops_console] health ok payload=$health_json"
    else
      echo "[run_ops_console] health failed payload=$health_json" >&2
      exit 1
    fi
    ;;
  logs)
    echo "[run_ops_console] standalone backend logs are disabled; inspect the strategy stdout/jsonl logs for embedded console activity" >&2
    echo "[run_ops_console] examples:" >&2
    echo "  STRATEGY_ENV_FILE=$ENV_FILE scripts/runtime/run_strategy_live.sh logs" >&2
    echo "  STRATEGY_ENV_FILE=$ENV_FILE scripts/runtime/run_strategy_live.sh jsonl" >&2
    exit 1
    ;;
  *)
    echo "usage: scripts/runtime/run_ops_console.sh [start|stop|restart|status|health|logs]" >&2
    exit 2
    ;;
esac
