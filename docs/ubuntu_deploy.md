# Ubuntu Deploy Draft

## Goal
- Bring up the persistence/reporting layer on Ubuntu without changing exchange execution core
- Validate:
  - Python venv
  - `.env`
  - MariaDB connectivity
  - schema initialization
- polling DB sync
- live CLI report
- browser viewer via FastAPI
- long-running runtime soak loop

## 1. System packages
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-dev build-essential pkg-config libmariadb-dev mariadb-client
```

## 2. Project setup
```bash
sudo mkdir -p /opt/funding-fee
sudo chown -R "$USER":"$USER" /opt/funding-fee
cd /opt/funding-fee
# copy project files here first
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 3. Environment file
- Copy `.env`
- Fill exchange API keys
- Fill MariaDB connection values
- Set `TARGET_EXCHANGES`, `SYMBOLS`, `SYNC_INTERVAL_SEC` as needed
- Set `SYNC_BALANCE=1` if you want balance/equity time-series persistence
- Set `SYNC_POSITIONS=1` if you want per-symbol position time-series persistence
- Set `ENABLE_WEB_TRADING=1` if you want browser order/cancel actions

Example:
```bash
cp .env .env.server
vi .env
```

Recommended soak additions:
```bash
echo 'SOAK_JSONL_PATH=logs/runtime_soak.jsonl' >> .env
echo 'SOAK_STDOUT_LOG=logs/runtime_soak.log' >> .env
echo 'SOAK_LOOP_DELAY_SEC=30' >> .env
echo 'SOAK_MAX_FAILURES=0' >> .env
```

## 4. MariaDB schema
```bash
.venv/bin/python scripts/db/init_db_schema.py
```

## 5. Manual run examples
```bash
.venv/bin/python scripts/db/sync_db.py --exchanges okx --symbols BTC,ETH
.venv/bin/python scripts/ops/report.py --exchanges okx --symbols BTC,ETH
.venv/bin/python scripts/ops/web.py
```
- `sync_db.py` now also persists `balance_snapshots` and `position_snapshots` by default unless disabled with `--no-sync-balance` or `--no-sync-positions`

Open in browser:
```bash
http://<SERVER_IP>:8000
```

## 6. Recommended first-run order
Run these in order on the server:
```bash
cd /opt/funding-fee
.venv/bin/python scripts/db/init_db_schema.py
.venv/bin/python -u scripts/db/sync_db.py --exchanges okx --symbols ETH
.venv/bin/python -u scripts/ops/report.py --exchanges okx --symbols ETH
```

If you want one command for smoke-check:
```bash
chmod +x scripts/runtime/server_preflight.sh
TARGET_EXCHANGES=okx SYMBOLS=ETH ./scripts/runtime/server_preflight.sh
```

If you want the web viewer immediately:
```bash
chmod +x scripts/runtime/run_ops_web.sh
.venv/bin/python scripts/ops/web.py
```

If you want long-running runtime soak immediately:
```bash
chmod +x scripts/runtime/run_runtime_soak_loop.sh
bash scripts/runtime/run_runtime_soak_loop.sh
```

If browser order/cancel should be enabled:
```bash
echo 'ENABLE_WEB_TRADING=1' >> .env
echo 'WEB_ACTION_WAIT_WS_SEC=4' >> .env
```

## 7. Logs
- Sync loop writes to:
  - `logs/db_sync.log`
- Runtime soak loop writes to:
  - `logs/runtime_soak.log`
  - `logs/runtime_soak.jsonl`

## 8. systemd
```bash
sudo cp deploy/systemd/oms-db-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable oms-db-sync.service
sudo systemctl start oms-db-sync.service
sudo systemctl status oms-db-sync.service
```

Useful commands:
```bash
sudo journalctl -u oms-db-sync.service -f
tail -f /opt/funding-fee/logs/db_sync.log
```

Optional web service:
```bash
sudo cp deploy/systemd/ops-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ops-web.service
sudo systemctl start ops-web.service
sudo systemctl status ops-web.service
```

Web logs:
```bash
sudo journalctl -u ops-web.service -f
tail -f /opt/funding-fee/logs/ops_web.log
```

Optional runtime soak service:
```bash
sudo cp deploy/systemd/oms-runtime-soak.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable oms-runtime-soak.service
sudo systemctl start oms-runtime-soak.service
sudo systemctl status oms-runtime-soak.service
```

Runtime soak logs:
```bash
sudo journalctl -u oms-runtime-soak.service -f
tail -f /opt/funding-fee/logs/runtime_soak.log
.venv/bin/python scripts/ops/soak_report.py --path /opt/funding-fee/logs/runtime_soak.jsonl
```

## 9. Operational assumptions
- Exchange execution core remains separate from persistence layer
- DB sync is polling + upsert first
- Order persistence is currently snapshot-based
- CLI report is the first observability layer, not a full dashboard
