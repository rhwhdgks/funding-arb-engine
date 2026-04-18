# Repo Map

## 한 줄 요약
- 이 레포는 지금 `전략 런타임`, `거래소 코어`, `운영/저장`, `실행 스크립트`, `참고 자료`로 나뉩니다.
- 실제로 자주 보는 곳은 생각보다 적습니다.

## 1. 제일 먼저 보면 좋은 파일

### [main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/main.py)
- 현재 메인 전략 진입점입니다.
- 실거래/테스트 서버 모두 결국 이 파일을 시작점으로 봅니다.

### [strategy.py](/home/jonghan/findalpha/arbitrage/funding%20fee/strategy.py)
- 실제 전략 구현이 들어 있는 파일입니다.
- `main.py`가 이 파일을 호출합니다.

### [fx.js](/home/jonghan/findalpha/arbitrage/funding%20fee/fx.js)
- USD/KRW 환율 수집기입니다.
- 전략 런타임과 별개로 돌립니다.

## 2. 거래소 코어

### [src/binance_f_module](/home/jonghan/findalpha/arbitrage/funding%20fee/src/binance_f_module)
### [src/okx_f_module](/home/jonghan/findalpha/arbitrage/funding%20fee/src/okx_f_module)
### [src/gateio_f_module](/home/jonghan/findalpha/arbitrage/funding%20fee/src/gateio_f_module)
- 거래소별 핵심 구현입니다.
- 공통적으로 아래 3파일 구조를 가집니다.
  - `api.py`: REST/signing/정규화
  - `websocket_manager.py`: WS 연결/구독/재연결
  - `exchange.py`: 실행 엔진/로컬 상태/공통 인터페이스

### [src/hyperliquid_f_module](/home/jonghan/findalpha/arbitrage/funding%20fee/src/hyperliquid_f_module)
- 현재 메인 흐름보다는 보조/실험 성격이 강한 거래소 구현입니다.

## 3. 공통 코어

### [src/core](/home/jonghan/findalpha/arbitrage/funding%20fee/src/core)
- 상위 공통 규약/로더 영역입니다.
- 주요 파일:
  - [exchange_registry.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/core/exchange_registry.py)
  - [exchange_contract.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/core/exchange_contract.py)
  - [action_gate.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/core/action_gate.py)

## 4. 운영 저장 / 리포트

### [src/ops/persistence](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence)
- DB 적재, snapshot, report 보조 레이어입니다.
- 현재 기준 주요 파일:
  - [db.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/db.py)
  - [event_hooks.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/event_hooks.py)
  - [live_snapshot.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/live_snapshot.py)
  - [live_report.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/live_report.py)
  - [balance_report.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/balance_report.py)
  - [trading_dev_normalizers.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/trading_dev_normalizers.py)
  - [trading_dev_store.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/trading_dev_store.py)

### [src/ops/web](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/web)
- 웹 운영 화면 관련 코드입니다.

## 5. 실행 스크립트

### [scripts/runtime](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime)
- 장시간 실행/런처 모음입니다.
- 자주 쓰는 파일:
  - [run_strategy_live.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_strategy_live.sh)
  - [run_fx_collector.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_fx_collector.sh)
  - [run_symbol_price_collector.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_symbol_price_collector.sh)
  - [server_preflight.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/server_preflight.sh)

### [scripts/ops](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/ops)
- CLI 리포트/운영용 진입점입니다.
- 자주 쓰는 파일:
  - [report.py](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/ops/report.py)
  - [web.py](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/ops/web.py)

### [scripts/db](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/db)
- DB 스키마 초기화용 스크립트입니다.
- 현재 기준 핵심은 [init_db_schema.py](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/db/init_db_schema.py)입니다.

## 6. DB / 스키마

### [sql](/home/jonghan/findalpha/arbitrage/funding%20fee/sql)
- DB 스키마 정의입니다.
- 주요 파일:
  - [mariadb_schema.sql](/home/jonghan/findalpha/arbitrage/funding%20fee/sql/mariadb_schema.sql)
  - [trading_dev_schema.sql](/home/jonghan/findalpha/arbitrage/funding%20fee/sql/trading_dev_schema.sql)

## 7. 테스트 서버

### [test_server](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server)
- 여기서는 코드 복사본을 들고 있지 않습니다.
- 분리되는 건 주로:
  - [test_server/.env](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server/.env)
  - [test_server/main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server/main.py)
  - [test_server/README.md](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server/README.md)

## 8. 참고 자료

### [reference](/home/jonghan/findalpha/arbitrage/funding%20fee/reference)
- 현재는 [reference/question4](/home/jonghan/findalpha/arbitrage/funding%20fee/reference/question4) 레퍼런스를 여기에 모아뒀습니다.
- 실운영 코드가 아니라 비교/아이디어 참고용입니다.

## 9. 지금 기준으로 무시해도 되는 것

### [logs](/home/jonghan/findalpha/arbitrage/funding%20fee/logs)
- 실행 산출물입니다.
- 구조를 읽을 때는 안 봐도 됩니다.

### `.venv`, `__pycache__`
- 개발 런타임/캐시입니다.
- 소스 구조 자체와는 무관합니다.

## 10. 추천 동선

### 전략 동작을 볼 때
1. [main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/main.py)
2. [strategy.py](/home/jonghan/findalpha/arbitrage/funding%20fee/strategy.py)
3. [src/okx_f_module/exchange.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/okx_f_module/exchange.py)
4. [src/binance_f_module/exchange.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/binance_f_module/exchange.py)

### DB 적재를 볼 때
1. [src/ops/persistence/trading_dev_normalizers.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/trading_dev_normalizers.py)
2. [src/ops/persistence/trading_dev_store.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/trading_dev_store.py)
3. [src/ops/persistence/event_hooks.py](/home/jonghan/findalpha/arbitrage/funding%20fee/src/ops/persistence/event_hooks.py)

### 실행 스크립트를 볼 때
1. [scripts/runtime/run_strategy_live.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_strategy_live.sh)
2. [scripts/runtime/run_fx_collector.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_fx_collector.sh)
3. [scripts/runtime/run_symbol_price_collector.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_symbol_price_collector.sh)
