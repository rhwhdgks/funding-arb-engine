# Environment Variables

원칙: `.env`에는 거래 실행에 직접 필요한 값만 둔다. 세부 튜닝은 코드 기본값을 우선 사용하고, 실제로 자주 바꾸는 값만 env로 노출한다.

## Root `.env`

공통 DB/거래소 키와 기본 전략 핵심값만 둔다.

### MariaDB
- `MARIADB_HOST`
- `MARIADB_PORT`
- `MARIADB_USER`
- `MARIADB_PASSWORD`
- `MARIADB_DATABASE`

### Exchange Credentials
- `BINANCE_ACCESS_KEY`
- `BINANCE_SECRET_KEY`
- `OKX_ACCESS_KEY`
- `OKX_SECRET_KEY`
- `OKX_PASSPHRASE`

### Strategy Core
- `STRATEGY_ACCOUNT_LABEL`
- `STRATEGY_LOGIC`
- `STRATEGY_EXCHANGES`
- `STRATEGY_ALLOWED_PAIRS`
- `STRATEGY_SYMBOLS`
- `STRATEGY_LEVERAGE`
- `STRATEGY_MARGIN_MODE`
- `STRATEGY_ORDER_NOTIONAL_USD`
- `STRATEGY_MAX_OPEN_PAIRS`
- `STRATEGY_MAX_GROSS_LEVERAGE`
- `STRATEGY_MAX_LONG_LEVERAGE`
- `STRATEGY_MAX_SHORT_LEVERAGE`

### Entry Control
- `STRATEGY_ENTRY_CONTROL_FILE`
- `STRATEGY_ENTRY_CONTROL_FAIL_CLOSED`

## `test_server/.env`

테스트 서버는 root와 같은 핵심값에 더해 로그 경로와 테스트 endpoint만 추가로 둔다.

### Runtime Files
- `STRATEGY_LIVE_LOG_DIR`
- `STRATEGY_LIVE_PID_FILE`
- `STRATEGY_LIVE_LOCK_FILE`
- `STRATEGY_LIVE_STDOUT_LOG`
- `STRATEGY_LIVE_EVENT_LOG_PATH`
- `STRATEGY_LIVE_RUN_META_FILE`
- `STRATEGY_STDOUT_LOG_STYLE`

### Test Endpoints
- `BINANCE_BASE_URL`
- `BINANCE_FUTURES_WS_PUBLIC_URL`
- `OKX_BASE_URL`
- `OKX_SIMULATED_TRADING`
- `OKX_WS_PUBLIC_URL`

### Run Policy
- `STRATEGY_RUN_DURATION`
- `STRATEGY_ENABLE_LIVE`
- `STRATEGY_APPROVAL_MODE`
- `STRATEGY_AUTO_ENTER_ENABLED`
- `STRATEGY_AUTO_CLOSE_ENABLED`

### Entry / Close Policy
- `STRATEGY_AUTO_ENTER_COOLDOWN_SEC`
- `STRATEGY_FIRST_LEG_TIMEOUT_SEC`
- `STRATEGY_HEDGE_REQUOTE_TIMEOUT_SEC`
- `STRATEGY_HEDGE_REQUOTE_COUNT`
- `STRATEGY_ACTION_ACCOUNT_FRESH_SEC`
- `STRATEGY_MIN_EXPECTED_NET_EDGE_USD`
- `STRATEGY_MIN_EXPECTED_NET_EDGE_BPS`
- `STRATEGY_REQUIRE_FUNDING_PAYMENT`
- `STRATEGY_MIN_HOLD_SEC`
- `STRATEGY_CLOSE_CANDIDATE_MIN_AGE_SEC`
- `STRATEGY_TAKE_PROFIT_BPS`
- `STRATEGY_STOP_LOSS_BPS`
- `STRATEGY_HOLD_BUFFER_BPS`
- `STRATEGY_TAKE_PROFIT_USD`
- `STRATEGY_STOP_LOSS_USD`

## Removed From Main Env

아래 계열은 기본 env에서 제외한다.

- GateIO / Hyperliquid credentials: 현재 기본 전략 경로가 Binance/OKX라 제외
- Notion tokens: trading runtime 필수값이 아니라 `.env.notion`에 분리
- Ops web settings: 웹 서버를 돌릴 때 별도 shell/env로 주입
- Legacy single-symbol keys: `SYMBOL`, `ENTRY_QTY`, `LEVERAGE`, `MARGIN_MODE` 대신 `STRATEGY_*` 사용
- Low-level websocket/reconcile tuning: 코드 기본값 사용

## Notion Env

Notion direct API 스크립트는 root `.env`를 먼저 읽고, 그 다음 ignored 파일인 `.env.notion`을 override 로드한다.

- `NOTION_TOKEN`
- `NOTION_DISPLAY_DATABASE_ID`
- `NOTION_INDEX_DATABASE_ID`
- `NOTION_VERSION`

## Notes

- `STRATEGY_ACCOUNT_LABEL`은 DB row를 `test`/`real` 계좌별로 분리하는 값이다.
- `STRATEGY_LOGIC`은 같은 계좌 안에서 전략/로직을 구분하는 값이며 현재 기본값은 `funding fee`다.
- `STRATEGY_LEVERAGE`는 거래소에 설정할 주문 레버리지다. `.env`를 바꾼 뒤 strategy를 재시작해야 반영된다.
- `STRATEGY_MAX_GROSS_LEVERAGE`는 계정 전체 gross exposure cap이다. `0`이면 비활성화다.
- `STRATEGY_MAX_LONG_LEVERAGE`, `STRATEGY_MAX_SHORT_LEVERAGE`도 `0`이면 비활성화다.
- Exposure cap은 신규 진입만 막고, close/recovery/shutdown flatten은 막지 않는다.
