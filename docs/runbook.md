# 운영 Runbook

이 문서는 실서버 또는 테스트 서버에서 전략을 실행하고, 중지하고, 리포트로 상태를 판단하는 절차를 정리합니다. 프로젝트 개요는 [../README.md](../README.md), 파일 지도는 [repo_map.md](repo_map.md)를 봅니다.

## 운영 원칙

- 신규 진입보다 포지션 중립성과 안전 종료를 우선합니다.
- close, recovery, shutdown flatten은 entry pause나 신규 진입 gate 때문에 막히면 안 됩니다.
- 체결이 없는 run은 정상일 수 있습니다. 시장 조건이 맞지 않으면 pair가 열리지 않는 것이 맞습니다.
- final report는 실행 안정성을 판단하는 도구입니다. 수익성 평가는 별도 분석으로 봅니다.
- `.env` 계열 파일과 runtime/log 산출물은 공개 저장소에 올리지 않습니다.

## 실행 전 확인

프로젝트 루트로 이동합니다.

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
```

기본 컴파일과 런처 문법을 확인합니다.

```bash
.venv/bin/python -m py_compile main.py strategy.py src/strategy/config.py src/strategy/candidates.py src/strategy/close_policy.py src/strategy/execution.py
bash -n scripts/runtime/run_strategy_live.sh
```

이미 실행 중인 전략이 있는지 확인합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
```

## 전략 실행

실서버 12시간 run:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=12h scripts/runtime/run_strategy_live.sh start
```

실서버 무기한 run:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=forever scripts/runtime/run_strategy_live.sh start
```

테스트 서버 30분 smoke:

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
```

테스트 서버 장시간 run:

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=12h scripts/runtime/run_strategy_live.sh start
```

`STRATEGY_RUN_DURATION=forever`는 시간 제한 없는 실행 의도입니다. final report는 직접 stop한 뒤 생성합니다.

## 실행 중 확인

상태:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
```

stdout 로그:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh logs
```

structured event 로그:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh jsonl
```

현재 run 요약:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
```

현재 exchange/account snapshot:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh report
```

후보와 미진입 reason 요약:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh opportunity-report --label latest
```

테스트 서버는 같은 명령에서 env 파일만 바꿉니다.

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh summary
```

## 신규 진입 일시정지

운영 중 시장 이상, 호출량 압박, 수동 점검이 필요하면 신규 진입만 멈춥니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on manual_pause
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off manual_resume
```

pause 상태에서 기대하는 동작:

- 신규 pair는 열리지 않아야 합니다.
- 기존 pair close/recovery는 계속 진행되어야 합니다.
- shutdown cleanup은 계속 허용되어야 합니다.

## 안전 중지

전략 중지는 런처의 `stop`을 사용합니다. 이 경로는 SIGTERM 후 open order 취소, tracked pair close, flatten, shutdown event 기록을 시도합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh stop
```

테스트 서버 중지:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh stop
```

정상 종료 기대값:

- `shutdown_cleanup_started` 이벤트가 남습니다.
- `shutdown_cleanup_done` 이벤트가 남습니다.
- `remaining_positions=[]`입니다.
- `pending_pair_ids=[]`입니다.
- live report 기준 position/open order가 남지 않습니다.

## 최종 리포트

run 종료 후 final report를 생성합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_12h
```

테스트 서버 final report:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh final-report --label test_smoke
```

리포트 마지막 줄의 형태:

```text
final_verdict=PASS warnings=0 failures=0 blocking=0 output=...
```

warning을 임시 허용해야 하는 경우에만 명시적으로 옵션을 붙입니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_12h_allowwarn --allow-warnings
```

`--allow-warnings`는 운영 판단을 완화하는 옵션입니다. 기본 리포트가 깨끗하게 PASS가 되도록 만드는 것이 원칙입니다.

## PASS/FAIL 판단 기준

PASS로 보는 주요 신호:

- 요청 duration만큼 run이 진행되거나 의도한 stop으로 종료됩니다.
- `pair_entry_overfill_detected=0`
- `pair_entry_overfill_flatten_failed=0`
- `pair_close_leg_failed=0`
- `pair_close_pending=0`
- `danger_log_lines=0`
- `remaining_positions=[]`
- `pending_pair_ids=[]`
- DB open-like orders가 0입니다.
- Binance/OKX live positions가 0입니다.
- Binance/OKX live open orders가 0입니다.
- symbol price collector freshness가 정상입니다.

FAIL로 보는 주요 신호:

- overfill 발생 후 flatten이 실패했습니다.
- close leg failure 후 neutralize/recovery가 실패했습니다.
- shutdown 후 position 또는 open order가 남았습니다.
- DB에 open-like order가 남았습니다.
- live exchange check가 실패하거나 포지션이 남았습니다.
- strategy runtime이 의도한 duration 전에 비정상 종료됐습니다.

정상 empty-run:

- 후보 조건이 맞지 않아 pair가 열리지 않은 경우입니다.
- order/trade가 없거나 주문이 전부 취소/미존재 상태인 경우가 포함될 수 있습니다.
- 이 경우 수익성 평가는 불가능하지만, 종료 안정성과 flat check는 평가할 수 있습니다.

## Rate Limit 대응

Binance 또는 OKX 호출량 압박이 반복되면 우선 신규 진입을 멈춥니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on rate_limit_pressure
```

그 다음 상태를 확인합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh report
```

position과 open order가 없고 호출량 압박이 해소되면 resume합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off rate_limit_recovered
```

position이 있으면 무작정 프로세스를 죽이지 말고 close/recovery/shutdown 상태를 먼저 확인합니다.

## 보조 수집기

symbol price collector:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh start
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh stop
```

symbol price 1회 수집:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh once
```

FX collector:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh start
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh stop
```

FX 1회 수집:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh once
```

## DB 없는 환경

DB를 쓰지 않는 환경에서는 DB 접속 계열 변수를 비우거나 persistence를 비활성화합니다.

```env
ENABLE_DB_PERSISTENCE=0
```

이 경우 전략 실행은 계속되지만 DB 적재와 DB 기반 reconcile은 skip 또는 빈 결과로 볼 수 있습니다. live exchange flat check는 DB와 무관하게 확인할 수 있습니다.

## 운영 후 정리

run 종료 후 아래 순서로 봅니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_12h
```

리포트에서 먼저 볼 항목:

- Verdict
- Event Verdict
- Strategy Performance
- Entry Submit Price Age
- DB Verdict
- Live Exchange Flat
- Symbol Price Collector

운영 안정성은 Event Verdict와 Live Exchange Flat을 우선하고, 전략 수익성은 closed pair table과 PnL 관련 항목을 별도로 봅니다.
