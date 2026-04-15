#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

mkdir -p logs

if [[ ! -x ".venv/bin/python" ]]; then
  echo "[run_runtime_soak_loop] missing .venv/bin/python" >&2
  exit 1
fi

: "${SOAK_LOOP_DELAY_SEC:=30}"
: "${SOAK_MAX_FAILURES:=0}"
: "${SOAK_JSONL_PATH:=logs/runtime_soak.jsonl}"
: "${SOAK_STDOUT_LOG:=logs/runtime_soak.log}"

failure_count=0

while true; do
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$ts] runtime soak start" | tee -a "$SOAK_STDOUT_LOG"

  if SOAK_LOG_PATH="$SOAK_JSONL_PATH" .venv/bin/python -u test/recovery/runtime_soak_smoke.py >> "$SOAK_STDOUT_LOG" 2>&1; then
    echo "[$ts] runtime soak ok" | tee -a "$SOAK_STDOUT_LOG"
    failure_count=0
  else
    status=$?
    failure_count=$((failure_count + 1))
    echo "[$ts] runtime soak failed status=${status} failure_count=${failure_count}" | tee -a "$SOAK_STDOUT_LOG"

    if [[ "${SOAK_MAX_FAILURES}" -gt 0 && "${failure_count}" -ge "${SOAK_MAX_FAILURES}" ]]; then
      echo "[$ts] runtime soak aborting after ${failure_count} consecutive failures" | tee -a "$SOAK_STDOUT_LOG"
      exit "$status"
    fi
  fi

  sleep "${SOAK_LOOP_DELAY_SEC}"
done
