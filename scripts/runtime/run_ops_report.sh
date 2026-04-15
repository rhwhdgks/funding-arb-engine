#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "[run_ops_report] missing .venv/bin/python" >&2
  exit 1
fi

exec .venv/bin/python -u scripts/ops/report.py "$@"
