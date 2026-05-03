# Test Server Runtime

`test_server`는 이제 로직 복사본을 들고 있지 않습니다.

분리되는 건 딱 두 가지입니다.
- `test_server/.env`
- `test_server/main.py`

나머지 실행 로직, DB 초기화, 리포트, persistence는 모두 루트 코드를 공유합니다.

## 실행

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh start
```

## 상태 확인

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh status
```

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh logs
```

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh jsonl
```

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh report
```

## 중지

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_SCRIPT_PATH=test_server/main.py \
scripts/runtime/run_strategy_live.sh stop
```

## 신규 진입 일시정지

실행 중 신규 진입만 막고, 기존 pair close / recovery / shutdown cleanup은 계속 허용합니다.

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh on manual_pause
```

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh off manual_resume
```

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh status
```

## DB 스키마 초기화

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env \
.venv/bin/python -u scripts/db/init_db_schema.py
```
