# Repo Map

## 한 줄 요약
- 지금 레포는 크게 `거래소 코어`, `운영 레이어`, `실행 스크립트`, `검증`, `문서/배포`로 나뉩니다.
- 실제로 자주 보는 곳은 생각보다 많지 않습니다.

## 1. 제일 중요한 폴더

### `src/binance_f_module`
### `src/gateio_f_module`
### `src/okx_f_module`
- 거래소 코어입니다.
- 각 거래소는 같은 3파일 구조를 유지합니다.
  - `api.py`: REST/signing/정규화
  - `websocket_manager.py`: WS 연결/구독/재연결
  - `exchange.py`: 실행 엔진/로컬 상태/공통 인터페이스
- OMS/MD 핵심을 볼 때는 여기부터 보면 됩니다.

### `src/ops/persistence`
- 운영 저장 레이어입니다.
- 거래소 코어를 바꾸지 않고 DB 저장/리포트 수집을 옆에 붙인 영역입니다.
- 주요 파일:
  - `db.py`: MariaDB 저장/조회
  - `sync.py`: polling -> upsert/snapshot sync
  - `event_hooks.py`: order event 기록
  - `balance_report.py`: 공통 잔고 분해
  - `live_report.py`: CLI/Web용 live snapshot 수집

### `src/ops/web`
- 브라우저 운영 화면입니다.
- 주요 파일:
  - `app.py`: FastAPI + HTML/JS
  - `actions.py`: 웹 주문/취소 액션 래퍼

### `src/core`
- 상위 공통 규약/로더 영역입니다.
- 주요 파일:
  - `exchange_registry.py`: 거래소 이름 -> Exchange 클래스 로더
  - `exchange_contract.py`: 공통 인터페이스 smoke용 규약

## 2. 실행용 폴더

### `scripts`
- 직접 실행하는 entrypoint 모음입니다.
- 하위 폴더:
  - `scripts/db`: schema/sync
  - `scripts/ops`: report/web/soak summary
  - `scripts/runtime`: long-running loop와 preflight
- 자주 쓰는 파일:
  - `scripts/db/sync_db.py`: DB 적재
  - `scripts/ops/report.py`: CLI 리포트
  - `scripts/ops/web.py`: 웹 서버 실행
  - `scripts/runtime/server_preflight.sh`: 서버 smoke-check
  - `scripts/runtime/run_db_sync_loop.sh`: DB sync 루프
  - `scripts/runtime/run_runtime_soak_loop.sh`: 장시간 soak 루프
  - `scripts/ops/soak_report.py`: soak 로그 요약

## 3. 검증용 폴더

### `test/binance`
### `test/gateio`
### `test/okx`
- 거래소별 실동작 smoke입니다.
- 주문/취소/체결/펀딩/OHLCV/WS 검증용입니다.

### `test/interfaces`
- 공통 인터페이스 smoke입니다.
- 거래소별 메서드/출력 형식이 맞는지 봅니다.

### `test/persistence`
- DB/event/balance/position 저장 검증입니다.

### `test/recovery`
- reconcile/restart/cancel-race/soak 같은 안정화 검증입니다.

### `test/marketdata`
- 전종목 WS smoke입니다.

## 4. 문서/배포 폴더

### `docs`
- 현재 상태, env, 배포, 운영 구조 설명입니다.
- 먼저 보면 좋은 문서:
  - `docs/current_status.md`
  - `docs/execution_coverage.md`
  - `docs/operations_layer.md`
  - `docs/env_vars.md`
  - `docs/ubuntu_deploy.md`

### `deploy/systemd`
- Ubuntu systemd 서비스 초안입니다.

### `sql`
- MariaDB schema입니다.

## 5. 지금 당장 무시해도 되는 것

### `logs`
- 실행 로그입니다.
- 코드 구조를 이해할 때는 안 봐도 됩니다.

### `__pycache__`
- 파이썬 실행 캐시입니다.
- 소스가 아닙니다.

### `.venv`
- 가상환경입니다.
- 레포 구조를 이해할 때는 무시하면 됩니다.

## 6. 지금 기준 추천 동선

### 거래소 코어를 볼 때
1. `src/binance_f_module`
2. `src/gateio_f_module`
3. `src/okx_f_module`

### 운영 저장 흐름을 볼 때
1. `src/ops/persistence/sync.py`
2. `src/ops/persistence/db.py`
3. `scripts/db/sync_db.py`

### 웹 화면을 볼 때
1. `src/ops/web/app.py`
2. `src/ops/web/actions.py`
3. `scripts/ops/web.py`

### 안정화 검증을 볼 때
1. `test/recovery`
2. `test/persistence`
3. `test/interfaces`

## 7. 나중에 정리 후보
- `scripts` entrypoint 수를 더 줄이는 작업
- 오래된/덜 쓰는 테스트를 `archive` 성격으로 묶는 작업
- 웹/운영 문서를 더 압축해서 진입 문서를 하나로 줄이는 작업

중요:
- 지금은 일부러 큰 폴더 리팩터링은 하지 않았습니다.
- 실행 코어가 안정화된 뒤에 구조 정리를 따로 하는 게 더 안전합니다.
