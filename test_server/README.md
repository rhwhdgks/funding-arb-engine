# Test Server

테스트 서버는 루트 코드를 공유하고 `test_server/.env`로 endpoint, 로그, runtime 경로를 분리합니다. 테스트 서버라도 주문 실행 경로는 실제 거래소 테스트 환경을 호출할 수 있으므로, 중지는 런처의 `stop`을 사용합니다.

## 실행

프로젝트 루트에서 실행합니다.

```bash
cd funding-arb-engine
cp test_server/.env.example test_server/.env
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
```

장시간 테스트가 필요하면 duration만 바꿉니다.

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=12h scripts/runtime/run_strategy_live.sh start
```

## 상태와 로그

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh logs
```

structured event 로그를 직접 볼 때:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh jsonl
```

## 중지

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh stop
```

정상 중지 후에는 shutdown cleanup event, 남은 position/open order, pending pair 여부를 final report에서 확인합니다.

## 최종 리포트

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh final-report --label test_smoke
```

체결이 없는 run도 정상일 수 있습니다. 이 경우에도 overfill, close pending, danger, live position/open order가 남지 않는지 확인합니다.

## 신규 진입 pause/resume

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh on manual_pause
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh off manual_resume
```

## 참고

- 자세한 운영 절차는 [../docs/runbook.md](../docs/runbook.md)를 봅니다.
- 환경변수 역할은 [../docs/env_vars.md](../docs/env_vars.md)를 봅니다.
- 저장소 구조는 [../docs/repo_map.md](../docs/repo_map.md)를 봅니다.
