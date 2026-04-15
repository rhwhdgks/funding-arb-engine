# Operations Layer

## What changed
- Added MariaDB schema for:
  - `trade_history`
  - `funding_history`
  - `order_history`
  - `order_events`
  - `balance_snapshots`
  - `position_snapshots`
- Added Python persistence helpers in `src/ops/persistence/`
- Added sync scripts for polling -> upsert DB sync
- Added a minimal live CLI report script
- Added a minimal FastAPI browser viewer
- Added browser-triggered order/cancel endpoints behind an env flag
- Added long-running runtime soak loop helpers and JSONL soak summary tooling

## Table intent

### `trade_history`
- One row per exchange trade fill
- Common fields across exchanges:
  - exchange name
  - symbol
  - trade id
  - order id / client order id
  - side / role / position side
  - price / qty / qty_contract / quote_qty / fee
  - create time
  - raw payload JSON
- Duplicate prevention:
  - unique key on `(exchange_name, symbol, trade_id)`

### `funding_history`
- One row per funding timestamp
- Common fields:
  - exchange name
  - symbol
  - funding time
  - funding rate
  - realized rate
  - mark/index price when available
  - raw payload JSON
- Duplicate prevention:
  - unique key on `(exchange_name, symbol, funding_time_ms)`

### `order_history`
- First stable step is snapshot-style order persistence, not full event sourcing
- Current sync stores:
  - current open-order snapshot from exchange interface
  - latest known status/state
  - first seen / last seen timestamps
  - `is_open`
  - raw payload JSON
- Duplicate prevention:
  - unique key on `(exchange_name, symbol, order_uid)`
- Current `order_uid` source:
  - `order_id`
  - else `uuid`
  - else `client_order_id`
  - else `identifier`

### `order_events`
- Append-only event ledger for order lifecycle tracking
- Current first step stores:
  - `reconcile_seen`
  - `reconcile_missing`
- Local exchange hooks now also write:
  - `order_submit`
  - `order_ack`
  - `order_rejected`
  - `cancel_submit`
  - `cancel_ack`
  - `cancel_rejected`
  - `cancel_all_submit`
  - `cancel_all_ack`
  - `cancel_all_rejected`
- Order status polling can now also emit:
  - `order_live`
  - `partial_fill`
  - `fill`
  - `order_canceled`
  - `order_rejected`
  - `order_expired`
  - `order_missing`
  - `order_status_update`
- Common fields:
  - exchange name
  - symbol
  - order uid / order id / client order id
  - event type / event source
  - status / state
  - price / qty / executed_qty / left_qty
  - event time
  - raw payload JSON
- Duplicate prevention:
  - unique `event_key` hash based on normalized order state
  - repeated syncs with the same unchanged order state should not keep appending duplicate rows

### `balance_snapshots`
- Append-only balance/equity time-series for operations tracking
- Current rows store:
  - exchange name
  - tracked symbol scope
  - wallet / available / locked / margin
  - exchange unrealized PnL
  - theoretical unrealized PnL
  - actual equity / theoretical equity / equity gap
  - long / short / gross / net notional
  - position count
  - raw payload JSON
- Duplicate prevention:
  - unique key on `(exchange_name, tracked_symbols_csv, snapshot_time_ms)`

### `position_snapshots`
- Append-only per-symbol position time-series for operations tracking
- Current rows store:
  - exchange name
  - tracked symbol scope
  - symbol
  - qty / side
  - entry price / mark price / mark source
  - mark notional
  - exchange unrealized PnL
  - theoretical unrealized PnL
  - unrealized PnL gap
  - raw payload JSON
- Duplicate prevention:
  - unique key on `(exchange_name, tracked_symbols_csv, symbol, snapshot_time_ms)`

## Current sync model
- This layer intentionally uses simple polling + upsert first
- It does **not** replace exchange execution code
- It does **not** introduce a base class
- It does **not** require websocket event persistence first
- Current shape is hybrid:
  - `order_history` for current-state reads
  - `order_events` for append-only lifecycle breadcrumbs

## Scripts

### Initialize schema
```bash
.venv/bin/python scripts/db/init_db_schema.py
```

### Sync exchange data to DB
```bash
.venv/bin/python scripts/db/sync_db.py --exchanges okx --symbols BTC,ETH
```

### Minimal live report
```bash
.venv/bin/python scripts/ops/report.py --exchanges binance,gateio,okx --symbols BTC,ETH
```
- Includes a common `balance_report` layer across Binance / Gate.io / OKX
- Current fields include:
  - wallet / available / locked / margin
  - exchange unrealized PnL
  - theoretical unrealized PnL
  - actual equity / theoretical equity / equity gap
  - long / short / gross / net notional
  - per-symbol valuation rows with mark source

### Browser viewer
```bash
.venv/bin/python scripts/ops/web.py
```
- The live screen also shows the same common balance decomposition
- DB history now also includes recent `balance_snapshots`
- DB history now also includes recent `position_snapshots`

### Browser trading actions
- Disabled by default
- Enable with:
```bash
ENABLE_WEB_TRADING=1
```
- Current browser actions:
  - limit order
  - cancel order
  - cancel all for symbol

### Server preflight
```bash
TARGET_EXCHANGES=okx SYMBOLS=ETH ./scripts/runtime/server_preflight.sh
```

### Runtime soak loop
```bash
bash scripts/runtime/run_runtime_soak_loop.sh
```
- Runs `test/recovery/runtime_soak_smoke.py` in a loop
- Appends detailed cycle rows to `SOAK_JSONL_PATH`
- Appends plain-text loop logs to `SOAK_STDOUT_LOG`

### Runtime soak summary
```bash
.venv/bin/python scripts/ops/soak_report.py --path logs/runtime_soak.jsonl
```
- Summarizes the latest per-exchange soak state from JSONL rows
- Falls back to inferred per-exchange summary when older logs do not yet contain a terminal `soak_summary` row

## What is completed
- MariaDB schema draft implemented
- Trade history upsert sync implemented
- Funding history upsert sync implemented
- Open-order snapshot upsert sync implemented
- Order event append-only sync implemented for reconcile events
- Balance snapshot append-only sync implemented
- Position snapshot append-only sync implemented
- Best-effort local order/cancel event hooks added to Binance / Gate.io / OKX execution paths
- REST-polled order lifecycle tracking added to Binance / Gate.io / OKX waiting-list paths
- `order_events.raw_json` now stores a `storage_version=2` envelope with:
  - normalized event payload
  - exchange raw payload
  - previous/transition/request context when available
- submit-stage request payload now carries an exchange-facing `client_order_id` so `order_submit -> order_ack` is easier to stitch by order id
- Direct lifecycle follow-up events now also emit:
  - `order_live` after successful open-order acknowledgements
  - `order_canceled` after successful single cancel / cancel-all close-out
  - market orders can now do a short status/trade follow-up to emit `partial_fill` or `fill` when the initial ack is not terminal
- successful single cancel now also prunes the local `open_orders_snapshot` immediately, instead of waiting for the next open-order poll
- OKX trade-history -> DB upsert verified with real fills
- OKX order_history open -> closed transition verified
- OKX order_events reconcile_seen -> reconcile_missing verified
- Offline smoke verified for polled `partial_fill -> fill -> order_missing` transitions on Binance / Gate.io / OKX
- Runtime soak / restart / reconcile smoke verified on:
  - Binance / BTC
  - Gate.io / BTC
  - OKX / ETH
- Reconcile guard hardening added on Binance / Gate.io / OKX:
  - periodic guard loop in each exchange runtime
  - account snapshot staleness guard
  - open-order snapshot staleness guard
  - waiting-list vs latest open-order snapshot drift detection
- Reconcile guard smoke verified on:
  - Binance / BTC
  - Gate.io / BTC
  - OKX / ETH
- Live OKX cancel-race smoke verified:
  - `order_submit -> order_ack -> order_live -> cancel_submit -> cancel_ack -> order_canceled`
  - open-order view cleaned immediately after cancel success
- Minimal live CLI report implemented in readable text form
- Minimal FastAPI browser viewer implemented for live + DB views
- Browser viewer now also includes:
  - top-level live overview cards
  - balance/position/trade/event trend cards
  - runtime soak status panel sourced from JSONL soak logs
- Browser order/cancel actions verified with live OKX test order + cancel
- `server_preflight.sh` smoke-check verified on:
  - Binance / BTC
  - Gate.io / BTC
  - OKX / ETH
- `sync_db.py` confirmed DB upsert rows for:
  - Binance `trade_history` + `funding_history`
  - Gate.io `trade_history` + `funding_history`
  - OKX `trade_history` + `funding_history`
- Recent balance/equity snapshots can now be persisted and viewed from the browser

## What is still prototype / draft
- `order_history` is snapshot-based, not a full event ledger
- `order_events` is still best-effort for local submit/cancel hooks and should not be treated as the sole source of truth yet
- DB schema should still be treated as evolvable
- Funding sync currently focuses on historical polling, not realtime writes
- No large dashboard yet; CLI report is the first observability layer
- Live partial-fill is still not force-verified with a large enough real order; current proof for that path is still synthetic smoke + market fill live checks
