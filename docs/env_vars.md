# Environment Variables

이 문서는 실제 값 없이 환경변수의 역할과 분리 원칙만 설명합니다. 공개 문서에는 API key, token, password, 실계좌 식별 정보, DB 접속 상세값을 쓰지 않습니다.

## 기본 원칙

- 실서버는 루트 `.env`, 테스트 서버는 `test_server/.env`를 사용합니다.
- `.env`, `.env.notion`, `test_server/.env`는 git에 올리지 않습니다.
- 런처는 `STRATEGY_ENV_FILE`로 사용할 env 파일을 고릅니다.
- 자주 바꾸는 운영값만 env로 노출하고, 낮은 수준의 튜닝값은 코드 기본값을 우선합니다.
- DB 설정은 선택사항입니다. DB 변수가 없거나 persistence가 비활성화되면 런타임은 DB write를 skip하고 계속 실행할 수 있습니다.

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

## `test_server/.env`

테스트 서버 env는 루트 코드와 같은 전략을 실행하되, 아래 항목을 테스트용으로 분리합니다.

- 테스트 endpoint와 simulated trading 관련 설정
- 로그, PID, lock, JSONL, run metadata 경로
- 테스트용 symbol/order notional/duration
- 테스트 서버용 account label
- auto enter/auto close/approval mode

테스트 서버라고 해도 주문 실행 경로는 실제 거래소 테스트 환경을 호출할 수 있습니다. 중지할 때는 프로세스를 직접 죽이기보다 런처의 `stop`을 사용합니다.

## Notion Env

Notion 연동은 trading runtime 필수값이 아닙니다. 별도 env 파일로 분리하고, 운영 코드 설명이나 공개 문서에는 토큰 값을 적지 않습니다.

## Collector Env

| 계열 | 역할 |
| --- | --- |
| `SYMBOL_PRICE_*` | symbol price collector의 대상 거래소, 심볼, 주기, 로그/runtime 파일 |
| `FX_*` | FX collector의 실행 주기, 로그/runtime 파일, node 실행 경로 |

보조 수집기는 전략과 별도로 start/stop/status/logs/once 명령을 지원합니다. 자세한 명령은 [runbook.md](runbook.md)를 봅니다.

## 운영 메모

- `STRATEGY_LEVERAGE`와 margin mode 계열 값은 재시작 후 bootstrap 경로에서 반영됩니다.
- exposure cap은 신규 진입만 막고, close/recovery/shutdown flatten은 막지 않아야 합니다.
- `STRATEGY_RUN_DURATION=forever`는 무기한 실행 의도로 사용합니다. final report는 stop 이후 생성합니다.
- env를 바꾼 뒤에는 실행 중 프로세스가 새 값을 읽는지 가정하지 말고 재시작 여부를 확인합니다.
