#!/usr/bin/env bash
set -euo pipefail

COLLECTOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ROOT="${FUNDING_FEE_PROJECT_ROOT:-}"
if [[ -z "$PROJECT_ROOT" ]]; then
  if [[ -x "$COLLECTOR_DIR/../scripts/runtime/run_symbol_price_collector.sh" ]]; then
    PROJECT_ROOT="$(cd "$COLLECTOR_DIR/.." && pwd)"
  else
    echo "[market_data_collector] FUNDING_FEE_PROJECT_ROOT is required when this folder is outside the repo" >&2
    exit 1
  fi
fi

RUNNER="$PROJECT_ROOT/scripts/runtime/run_symbol_price_collector.sh"
if [[ ! -x "$RUNNER" ]]; then
  echo "[market_data_collector] missing runner: $RUNNER" >&2
  exit 1
fi

ENV_FILE="${MARKET_DATA_ENV_FILE:-$COLLECTOR_DIR/.env.collector}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[market_data_collector] missing env file: $ENV_FILE" >&2
  echo "[market_data_collector] copy .env.collector.example to .env.collector and fill DB values" >&2
  exit 1
fi

export STRATEGY_ENV_FILE="$ENV_FILE"
export SYMBOL_PRICE_LOG_DIR="${SYMBOL_PRICE_LOG_DIR:-$COLLECTOR_DIR/logs}"
export SYMBOL_PRICE_PID_FILE="${SYMBOL_PRICE_PID_FILE:-$SYMBOL_PRICE_LOG_DIR/symbol_price_collector.pid}"
export SYMBOL_PRICE_LOCK_FILE="${SYMBOL_PRICE_LOCK_FILE:-$SYMBOL_PRICE_LOG_DIR/symbol_price_collector.lock}"
export SYMBOL_PRICE_STDOUT_LOG="${SYMBOL_PRICE_STDOUT_LOG:-$SYMBOL_PRICE_LOG_DIR/symbol_price_collector.out}"

if [[ "$#" -eq 0 ]]; then
  set -- start
fi

exec "$RUNNER" "$@"
