# Test Server

테스트 서버는 루트 코드를 그대로 공유하고, `test_server/.env`만 별도로 사용합니다.

## 실행

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
```

## 상태 확인

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh summary
```

## 로그 확인

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh logs
```

## 중지

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh stop
```

## 최종 리포트

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh final-report --label test_smoke
```

## 신규 진입 pause/resume

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh on manual_pause
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh off manual_resume
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/entry_pause.sh status
```

## 참고

- 자세한 운영 절차는 [../docs/runbook.md](../docs/runbook.md)를 봅니다.
- 환경변수 설명은 [../docs/env_vars.md](../docs/env_vars.md)를 봅니다.
- 체결이 없는 테스트 run은 정상입니다. final report에서 open order, position, danger, overfill이 없으면 운영 안정성은 PASS입니다.
