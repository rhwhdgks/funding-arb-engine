#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -f ".env" ]]; then
  echo "[preflight] missing .env" >&2
  exit 1
fi

if [[ ! -x ".venv/bin/python" ]]; then
  echo "[preflight] missing .venv/bin/python" >&2
  exit 1
fi

echo "[preflight] project_root=$PROJECT_ROOT"
echo "[preflight] python=$(.venv/bin/python --version)"

echo "[preflight] import check"
.venv/bin/python - <<'PY'
import aiohttp
import fastapi
import mariadb
import uvicorn
import websockets
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
print("import_ok")
PY

echo "[preflight] schema check"
.venv/bin/python scripts/db/init_db_schema.py

echo "[preflight] live report smoke"
.venv/bin/python -u scripts/ops/report.py \
  --exchanges "${TARGET_EXCHANGES:-${TARGET_EXCHANGE:-okx}}" \
  --symbols "${SYMBOLS:-${SYMBOL:-BTC}}" \
  --wait-ws-sec "${REPORT_WAIT_WS_SEC:-6}" \
  --trade-limit "${REPORT_TRADE_LIMIT:-3}"

echo "[preflight] done"
