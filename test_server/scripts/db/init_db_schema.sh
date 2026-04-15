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

PYTHON_BIN="${STRATEGY_PYTHON:-$REPO_ROOT/.venv/bin/python}"
PYTHONPATH="$PROJECT_ROOT:$REPO_ROOT:${PYTHONPATH:-}" "$PYTHON_BIN" -u "$REPO_ROOT/scripts/db/init_db_schema.py"
