# Environment Variables

이 문서는 실제 값 없이 환경변수의 역할과 분리 원칙만 설명합니다. 공개 문서에는 API key, token, password, 실계좌 식별 정보, DB 접속 상세값을 쓰지 않습니다.

## 기본 원칙

- 실서버는 루트 `.env`, 테스트 서버는 `test_server/.env`를 사용합니다.
- `.env`, `.env.notion`, `test_server/.env`는 git에 올리지 않습니다.
- 런처는 `STRATEGY_ENV_FILE`로 사용할 env 파일을 고릅니다.
- 자주 바꾸는 운영값만 env로 노출하고, 낮은 수준의 튜닝값은 코드 기본값을 우선합니다.
- DB 설정은 명시적 opt-in입니다. `ENABLE_DB_PERSISTENCE=1`(또는 `true`, `on`)이고 DB 접속값이 채워진 경우에만 DB write를 켭니다.
- DB 변수가 없거나 `ENABLE_DB_PERSISTENCE=0`이면 런타임은 DB write를 skip하고 전략과 비DB 리포트 일부를 계속 실행할 수 있습니다.

## 파일별 역할

| 파일 | 역할 |
| --- | --- |
| `.env` | 실서버 실행용 DB/거래소/전략 운영값 |
| `test_server/.env` | 테스트 서버 실행용 endpoint, 로그 경로, runtime 파일 경로 |
| `.env.notion` | Notion 연동용 별도 값 |
| shell 환경변수 | 일회성 실행 duration, label, collector 옵션 override |

## Root `.env`

루트 env는 실서버 운영값을 담습니다. 값 자체는 문서에 적지 않고, 역할만 관리합니다.

| 계열 | 역할 |
| --- | --- |
| DB 접속 계열 | MariaDB host/port/user/password/database, persistence on/off |
| 거래소 인증 계열 | Binance/OKX 등 private REST 호출에 필요한 인증값 |
| 전략 대상 계열 | account label, strategy logic, exchanges, allowed pairs, symbols |
| 주문 규모 계열 | leverage, margin mode, order notional, max open pairs |
| 리스크 계열 | gross/long/short exposure cap, rate-limit guard threshold |
| 진입/청산 계열 | expected edge, funding forecast, hold, take-profit, stop-loss, close mode |
| 런타임 계열 | log path, pid/lock/run metadata path, stdout style |
| entry control 계열 | pause control 파일 경로, fail-closed 여부 |
| Ops Console 계열 | 수동 주문 콘솔 host/port, 실거래 버튼, action token |

## `test_server/.env`

테스트 서버 env는 루트 코드와 같은 전략을 실행하되, 아래 항목을 테스트용으로 분리합니다.

- 테스트 endpoint와 simulated trading 관련 설정
- 로그, PID, lock, JSONL, run metadata 경로
- 테스트용 symbol/order notional/duration
- 테스트 서버용 account label
- auto enter/auto close/approval mode

테스트 서버라고 해도 주문 실행 경로는 실제 거래소 테스트 환경을 호출할 수 있습니다. 중지할 때는 프로세스를 직접 죽이기보다 런처의 `stop`을 사용합니다.

## DB Persistence

- 운영 DB는 `trading_dev` 하나를 사용하고, row 구분은 `STRATEGY_ACCOUNT_LABEL`과 `STRATEGY_LOGIC`으로 합니다.
- `STRATEGY_ACCOUNT_LABEL`은 계정/환경 구분값입니다. 예: `real`, `test`.
- `STRATEGY_LOGIC`은 전략 로직 구분값입니다. 기본값은 `funding fee`입니다.
- `ENABLE_DB_PERSISTENCE`를 비우거나 `0`으로 두면 DB 접속값이 있더라도 전략 DB write는 비활성화됩니다.
- DB를 켤 때만 `ENABLE_DB_PERSISTENCE=1`과 `MARIADB_HOST`, `MARIADB_USER`, `MARIADB_DATABASE=trading_dev`를 채웁니다. `MARIADB_PORT` 기본값은 `3306`입니다.

## Notion Env

Notion 연동은 trading runtime 필수값이 아닙니다. 별도 env 파일로 분리하고, 운영 코드 설명이나 공개 문서에는 토큰 값을 적지 않습니다.

## Normal Close Fee Gate

normal close fee gate는 정상 자동청산에서만 수수료/슬리피지 대비 net PnL이 나쁠 때 청산을 잠깐 보류합니다. recovery, shutdown flatten, manual emergency close는 이 gate를 타지 않습니다.

| 변수 | 역할 |
| --- | --- |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_ENABLED` | `1`이면 normal close fee gate 활성화. 기본값은 비활성 |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_MIN_NET_USD` | close net PnL이 이 값보다 낮으면 보류 후보 |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_MAX_DEFER_SEC` | fee gate로 보류할 수 있는 최대 시간 |

## Collector Env

| 계열 | 역할 |
| --- | --- |
| `SYMBOL_PRICE_*` | symbol price collector의 대상 거래소, 심볼, 주기, 로그/runtime 파일 |
| `FX_*` | FX collector의 실행 주기, 로그/runtime 파일, node 실행 경로 |

보조 수집기는 전략과 별도로 start/stop/status/logs/once 명령을 지원합니다. 자세한 명령은 [runbook.md](runbook.md)를 봅니다.

## Ops Console Env

| 변수 | 역할 |
| --- | --- |
| `OPS_CONSOLE_HOST` | embedded control server 또는 standalone console의 bind host. 로컬 전용은 `127.0.0.1`, 외부 장비 접속은 `0.0.0.0` |
| `OPS_CONSOLE_PORT` | embedded control server 또는 standalone console의 포트. 기본 `8010` |
| `ENABLE_WEB_TRADING` | console 실주문 버튼 활성화 여부 |
| `OPS_CONSOLE_ACTION_TOKEN_REQUIRED` | 실행 요청에 `X-OPS-TOKEN`을 요구할지 여부 |
| `OPS_CONSOLE_ACTION_TOKEN` | 주문/청산/entry pause 실행 요청용 토큰 |
| `OPS_CONSOLE_STALE_MS` | console 주문 전 market data stale 판단 기준 |
| `OPS_CONSOLE_FORCE_STANDALONE` | `1`이면 전략 내장형 대신 legacy standalone console backend 강제 실행. 기본 운영에서는 비워둠 |

기본 운영은 `OPS_CONSOLE_HOST=127.0.0.1`입니다. 같은 PC 브라우저에서만 console을 볼 수 있고, 별도 token 없이도 로컬 운영이 가능합니다. 폰이나 다른 장비에서 접속하려면 VPN/터널과 action token을 같이 둡니다.

현재 기본 구조는 `전략 내장형 control plane`입니다.

- 전략이 실행 중이면 `OPS_CONSOLE_*` 값은 전략 프로세스가 직접 사용합니다.
- `run_ops_console.sh status/health`는 embedded console도 정상 상태로 인식합니다.
- standalone console은 거래소 세션을 따로 여는 legacy 디버깅 경로이므로, 장기 운영에서는 기본값으로 두지 않습니다.

## 운영 메모

- `STRATEGY_LEVERAGE`와 margin mode 계열 값은 재시작 후 bootstrap 경로에서 반영됩니다.
- exposure cap은 신규 진입만 막고, close/recovery/shutdown flatten은 막지 않아야 합니다.
- `STRATEGY_RUN_DURATION=forever`는 무기한 실행 의도로 사용합니다. final report는 stop 이후 생성합니다.
- env를 바꾼 뒤에는 실행 중 프로세스가 새 값을 읽는지 가정하지 말고 재시작 여부를 확인합니다.
