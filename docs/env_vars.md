# Environment Variables

이 문서는 실제 값 없이 환경변수의 역할과 분리 원칙만 설명합니다. 공개 문서에는 API key, token, password, 실계좌 식별 정보, DB 접속 상세값을 쓰지 않습니다.

## 기본 원칙

- 실서버는 루트 `.env`, 테스트 서버는 `test_server/.env`를 사용합니다.
- `.env`, `test_server/.env`, collector private env는 git에 올리지 않습니다.
- 런처는 `STRATEGY_ENV_FILE`로 사용할 env 파일을 고릅니다.
- 자주 바꾸는 운영값만 env로 노출하고, 낮은 수준의 튜닝값은 코드 기본값을 우선합니다.
- DB 설정은 명시적 opt-in입니다. `ENABLE_DB_PERSISTENCE=1`(또는 `true`, `on`)이고 DB 접속값이 채워진 경우에만 DB write를 켭니다.
- DB 변수가 없거나 `ENABLE_DB_PERSISTENCE=0`이면 런타임은 DB write를 skip하고 전략과 비DB 리포트 일부를 계속 실행할 수 있습니다.

## 파일별 역할

| 파일 | 역할 |
| --- | --- |
| `.env` | 실서버 실행용 DB/거래소/전략 운영값 |
| `test_server/.env` | 테스트 서버 실행용 endpoint, 로그 경로, runtime 파일 경로 |
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

## Normal Close Fee Gate

normal close fee gate는 정상 자동청산에서만 수수료/슬리피지 대비 net PnL이 나쁠 때 청산을 잠깐 보류합니다. recovery, shutdown flatten, manual emergency close는 이 gate를 타지 않습니다.

| 변수 | 역할 |
| --- | --- |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_ENABLED` | `1`이면 normal close fee gate 활성화. 기본값은 비활성 |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_MIN_NET_USD` | close net PnL이 이 값보다 낮으면 보류 후보 |
| `STRATEGY_NORMAL_CLOSE_FEE_GATE_MAX_DEFER_SEC` | fee gate로 보류할 수 있는 최대 시간 |

## Funding Source

전략은 기본적으로 거래소 REST에서 funding rate를 직접 갱신합니다. API 사용량을 줄이고 싶으면 market data collector가 `symbol_price`에 같이 저장한 funding snapshot을 읽도록 바꿀 수 있습니다.

| 변수 | 역할 |
| --- | --- |
| `STRATEGY_FUNDING_SOURCE` | `runtime`이면 기존 REST 직접 조회. `symbol_price`, `collector`, `db`이면 `symbol_price` 최신 funding 컬럼 사용 |
| `STRATEGY_FUNDING_MAX_AGE_SEC` | `symbol_price` funding snapshot을 유효하다고 볼 최대 age |
| `STRATEGY_COLLECTOR_FUNDING_REFRESH_SEC` | strategy가 DB funding snapshot을 다시 읽는 최소 주기 |
| `STRATEGY_FUNDING_EVENT_MODEL_ENABLED` | `1`이면 진입 expected edge에서 펀딩을 제외하고, 실제 다음 정산 이벤트는 보유/청산 판단과 shadow 분석에만 사용. 기본 `0` shadow |
| `STRATEGY_FUNDING_EVENT_ENTRY_BUFFER_SEC` | 정산 직전 진입 실패를 피하기 위해 예상 진입 완료 시각에 더하는 안전 버퍼 |
| `STRATEGY_CLOSE_POSITIVE_FUNDING_WAIT_ENABLED` | 정상 spread close 시 가까운 다음 정산의 순수령이 양수면 제한적으로 대기 |
| `STRATEGY_CLOSE_POSITIVE_FUNDING_WAIT_MAX_SEC` | 양수 funding 정산을 기다릴 최대 남은 시간 |

`STRATEGY_ENTRY_FUNDING_FORECAST_SEC`는 legacy 8시간 비례 모델과 shadow 비교에만 사용합니다. Event 모델은 알 수 없는 실제 보유시간을 가정하지 않으며, 수익과 비용 모두 진입 기대수익에 넣지 않습니다.

`symbol_price` 모드는 collector가 먼저 실행 중이어야 의미가 있습니다. collector는 `funding_rate` 테이블에는 히스토리용 row를 쓰고, `symbol_price`에는 전략 조회용 최신 `funding_rate`, `next_funding_time_ms`, `funding_updated_at_ms`를 같이 저장합니다.

## Maker Entry Engine

maker entry engine은 active maker 주문을 1개 유지하되, 의미 없는 cancel/replace를 줄입니다. 체결 감지 polling과 hedge 속도는 늦추지 않습니다.

| 변수 | 역할 |
| --- | --- |
| `STRATEGY_MAKER_ENGINE_REQUOTE_SEC` | active maker 주문을 재평가하는 최소 간격 |
| `STRATEGY_MAKER_ENGINE_REPLACE_EDGE_BPS` | 다른 후보/현재 후보가 이 bps 이상 개선될 때만 교체 후보 |
| `STRATEGY_MAKER_ENGINE_REPLACE_PRICE_BPS` | normalized target price가 바뀌어도 가격 이동폭이 이 bps 미만이면 재호가 생략 |
| `STRATEGY_MAKER_ENGINE_REQUIRE_FULL_ADMISSION` | maker submit과 active 유지 모두 최신 scanner admission을 통과하도록 강제 |

Binance open-order reconcile은 active maker/order/position 심볼만 심볼별로 확인하고, 외부 수동 주문 탐지를 위해 `BINANCE_OPEN_ORDERS_FULL_RECONCILE_SEC`마다 global snapshot을 한 번 가져옵니다. active order의 개별 status poll과 hedge settle 주기는 변경하지 않습니다.

## Collector Env

| 계열 | 역할 |
| --- | --- |
| `SYMBOL_PRICE_*` | symbol price collector의 대상 거래소, 심볼, 주기, 로그/runtime 파일 |
| `MARKET_DATA_ENV_FILE` | `market_data_collector/run_market_data_collector.sh`가 사용할 collector 전용 env 파일 |
| `FUNDING_FEE_PROJECT_ROOT` | collector 폴더를 repo 밖에서 실행할 때 원본 프로젝트 경로 지정 |

보조 수집기는 전략과 별도로 start/stop/status/logs/once 명령을 지원합니다. 자세한 명령은 [runbook.md](runbook.md)를 봅니다.

## Ops Console Env

| 변수 | 역할 |
| --- | --- |
| `OPS_CONSOLE_HOST` | strategy-attached embedded control server의 bind host. 로컬 전용은 `127.0.0.1`, 외부 장비 접속은 `0.0.0.0` |
| `OPS_CONSOLE_PORT` | strategy-attached embedded control server의 포트. 기본 `8010` |
| `ENABLE_WEB_TRADING` | console 실주문 버튼 활성화 여부 |
| `OPS_CONSOLE_ACTION_TOKEN_REQUIRED` | 실행 요청에 `X-OPS-TOKEN`을 요구할지 여부 |
| `OPS_CONSOLE_ACTION_TOKEN` | 주문/청산/entry pause 실행 요청용 토큰 |
| `OPS_CONSOLE_LIVE_ACTION_WINDOW_SEC` | 토큰 확인 후 수동 실행을 허용하는 짧은 시간. 기본 300초 |
| `OPS_CONSOLE_LIVE_ACTION_WINDOW_MAX_SEC` | action window 요청의 최대 허용 시간 |
| `OPS_CONSOLE_ALLOWED_ORIGINS` | 상태 변경 요청을 허용할 브라우저 Origin 목록 |
| `OPS_CONSOLE_STALE_MS` | console 주문 전 market data stale 판단 기준 |

기본 운영은 `OPS_CONSOLE_HOST=127.0.0.1`이며 로컬에서도 action token을 필수로 둡니다. 조회는 가능하지만 주문/취소/청산은 올바른 token으로 action window를 연 동안만 실행됩니다. 폰이나 다른 장비에서 접속할 때는 VPN/SSH 터널, TLS 종단, 강한 token을 함께 사용하고 포트를 인터넷에 직접 노출하지 않습니다.

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

생성한 값은 private `.env`의 `OPS_CONSOLE_ACTION_TOKEN`에만 저장하며 문서, 로그, Git에는 남기지 않습니다.

## Order Intent / Funding Cashflow

| 변수 | 역할 |
| --- | --- |
| `STRATEGY_ORDER_INTENT_JOURNAL` | DB와 독립적인 append-only 주문 intent journal 경로 |
| `STRATEGY_ORDER_INTENT_FSYNC` | submit 전 journal을 디스크에 동기화할지 여부. live 기본값 `1` |
| `STRATEGY_ORDER_INTENT_UNKNOWN_GRACE_SEC` | 주문 조회가 missing인 경우 flat/cancel 확정 전 기다리는 시간 |
| `STRATEGY_FUNDING_CASHFLOW_ENABLED` | 거래소의 실제 funding 지급/수취 내역 수집 여부 |
| `STRATEGY_FUNDING_CASHFLOW_INTERVAL_SEC` | 실제 funding cashflow 조회 주기. 기본 300초 |
| `STRATEGY_FUNDING_CASHFLOW_BACKFILL_SEC` | 재시작 시 중복키 기반으로 다시 조회할 기간 |

현재 기본 구조는 `전략 내장형 control plane`입니다.

- 전략이 실행 중이면 `OPS_CONSOLE_*` 값은 전략 프로세스가 직접 사용합니다.
- `run_ops_console.sh status/health`는 embedded console도 정상 상태로 인식합니다.
- 전략이 꺼져 있으면 console도 사용할 수 없습니다. standalone console은 거래소 세션 분리 리스크 때문에 사용하지 않습니다.

## 운영 메모

- `STRATEGY_LEVERAGE`와 margin mode 계열 값은 재시작 후 bootstrap 경로에서 반영됩니다.
- exposure cap은 신규 진입만 막고, close/recovery/shutdown flatten은 막지 않아야 합니다.
- `STRATEGY_RUN_DURATION=forever`는 무기한 실행 의도로 사용합니다. final report는 stop 이후 생성합니다.
- env를 바꾼 뒤에는 실행 중 프로세스가 새 값을 읽는지 가정하지 말고 재시작 여부를 확인합니다.
