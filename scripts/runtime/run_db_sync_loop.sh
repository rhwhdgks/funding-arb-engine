#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

mkdir -p logs

if [[ ! -x ".venv/bin/python" ]]; then
  echo "[run_db_sync_loop] missing .venv/bin/python" >&2
  exit 1
fi

: "${SYNC_INTERVAL_SEC:=300}"

while true; do
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$ts] db sync start" | tee -a "logs/db_sync.log"

  if .venv/bin/python -u scripts/db/sync_db.py >> "logs/db_sync.log" 2>&1; then
    echo "[$ts] db sync ok" | tee -a "logs/db_sync.log"
  else
    status=$?
    echo "[$ts] db sync failed status=${status}" | tee -a "logs/db_sync.log"
  fi

  sleep "${SYNC_INTERVAL_SEC}"
done
