# 운영 Runbook

이 문서는 실서버 또는 테스트 서버에서 전략을 실행하고, 중단하고, 리포트를 보고, 이상 상태를 판단하는 기준을 정리합니다.

## 1. 운영 원칙

현재 시스템의 1순위는 안정성입니다.

- 신규 진입보다 포지션 중립성과 안전 종료가 우선입니다.
- close, recovery, shutdown flatten은 risk gate나 pause 때문에 막히면 안 됩니다.
- 체결이 없는 run은 정상입니다. 시장 조건이 안 맞으면 pair가 안 열리는 것이 맞습니다.
- final report는 hard failure를 찾는 도구입니다. 수익성 평가는 별도 전략 분석에서 봅니다.
- `.env`에는 실서버 키가 들어갈 수 있으므로 commit하지 않습니다.

## 2. 실행 전 확인

프로젝트 루트로 이동합니다.

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
```

현재 코드가 실행 가능한지 확인합니다.

```bash
.venv/bin/python -m py_compile \
  main.py \
  strategy.py \
  src/strategy/config.py \
  src/strategy/candidates.py \
  src/strategy/close_policy.py \
  src/strategy/execution.py
```

런처 문법을 확인합니다.

```bash
bash -n scripts/runtime/run_strategy_live.sh
```

이미 실행 중인 전략이 있는지 확인합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
```

## 3. 실서버 실행

12시간 실서버 run:

```bash
STRATEGY_ENV_FILE=.env \
STRATEGY_RUN_DURATION=12h \
scripts/runtime/run_strategy_live.sh start
```

24시간 실서버 run:

```bash
STRATEGY_ENV_FILE=.env \
STRATEGY_RUN_DURATION=24h \
scripts/runtime/run_strategy_live.sh start
```

무기한 실서버 run:

```bash
STRATEGY_ENV_FILE=.env \
STRATEGY_RUN_DURATION=forever \
scripts/runtime/run_strategy_live.sh start
```

무기한 run은 `timeout` 없이 실행됩니다. 열린 pair는 시간 제한 때문에 닫히지 않고, 전략의 청산 조건이나 사용자의 stop 요청에 의해서만 종료됩니다.

상태 확인:

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

## 4. 테스트 서버 실행

테스트 서버는 `test_server/.env`만 다르고 코드는 루트와 공유합니다.

30분 smoke:

```bash
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_RUN_DURATION=30m \
scripts/runtime/run_strategy_live.sh start
```

12시간 테스트:

```bash
STRATEGY_ENV_FILE=test_server/.env \
STRATEGY_RUN_DURATION=12h \
scripts/runtime/run_strategy_live.sh start
```

상태 확인:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
```

final report:

```bash
STRATEGY_ENV_FILE=test_server/.env \
scripts/runtime/run_strategy_live.sh final-report --label test_12h
```

## 5. 안전 중지

전략을 중지할 때는 런처의 stop을 사용합니다. stop은 shutdown cleanup을 통해 open order 취소와 잔여 포지션 flatten을 시도합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh stop
```

정상 종료 기대값:

- `shutdown_cleanup_started` 이벤트가 남습니다.
- `shutdown_cleanup_done` 이벤트가 남습니다.
- `remaining_positions=[]`입니다.
- `pending_pair_ids=[]`입니다.
- live report 기준 양 거래소 open orders가 0입니다.

## 6. 신규 진입 일시정지

운영 중 시장이 이상하거나 API rate limit 압박이 보이면 신규 진입만 멈출 수 있습니다. 이 상태에서도 기존 pair close, recovery, shutdown은 계속 허용됩니다.

pause:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on manual_pause
```

resume:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off manual_resume
```

status:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh status
```

pause 상태에서 기대값:

- 신규 `pair_opened`가 없어야 합니다.
- 기존 pair의 close/recovery는 계속 진행되어야 합니다.
- candidate에는 `risk:entry_paused` 계열 reason이 붙을 수 있습니다.

## 7. 최종 리포트

run 종료 후 final report를 생성합니다.

```bash
STRATEGY_ENV_FILE=.env \
scripts/runtime/run_strategy_live.sh final-report --label real_12h
```

무기한 run의 final report는 stop 이후에 생성합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh stop
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_forever_ended
```

live exchange check를 포함한 기본 리포트를 기준으로 봅니다. report 마지막 줄에 아래처럼 나옵니다.

```text
final_verdict=PASS warnings=0 failures=0 blocking=0 output=...
```

체결이 없는 run도 정상입니다. 이 경우 `pair_opened=0`, `pair_closed=0`, `trade_rows_present rows=0 empty_run_ok`로 표시됩니다.

warning을 임시로 허용해야 하는 상황이면 명시적으로 옵션을 붙입니다.

```bash
STRATEGY_ENV_FILE=.env \
scripts/runtime/run_strategy_live.sh final-report --label real_12h_allowwarn --allow-warnings
```

`--allow-warnings`는 운영 판단을 완화하는 옵션입니다. 기본 리포트가 PASS가 되도록 만드는 것이 원칙입니다.

## 8. PASS/FAIL 판단 기준

PASS로 보는 기준:

- run이 요청 duration만큼 종료됩니다.
- `pair_entry_overfill_detected=0`
- `pair_entry_overfill_flatten_failed=0`
- `pair_close_leg_failed=0`
- `pair_close_pending=0`
- `danger_log_lines=0`
- `remaining_positions=[]`
- `pending_pair_ids=[]`
- DB open-like orders가 0입니다.
- Binance live positions가 0입니다.
- Binance live open orders가 0입니다.
- OKX live positions가 0입니다.
- OKX live open orders가 0입니다.
- symbol price collector가 fresh입니다.

FAIL로 보는 기준:

- overfill이 발생했는데 flatten이 실패했습니다.
- close leg failure 후 neutralize/recovery가 실패했습니다.
- shutdown 후 position 또는 open order가 남았습니다.
- DB에 open-like order가 남았습니다.
- live exchange check가 실패하거나 포지션이 남았습니다.
- strategy runtime이 duration 전에 비정상 종료됐습니다.

정상 empty-run:

- 후보 조건이 안 맞아 pair가 열리지 않은 경우입니다.
- order/trade가 없거나, 주문이 전부 `CANCELED` 또는 `NOT_EXIST`인 경우가 포함됩니다.
- 이 경우 수익성 평가는 불가능하지만 운영 안정성 평가는 가능합니다.

## 9. 자주 보는 로그

진입 시도:

```text
[진입][시도] 심볼=SUI 롱=okx 숏=binance 예상손익USD=... 예상bps=...
```

페어 오픈:

```text
[페어][오픈] id=... 심볼=... 수량=...
```

페어 종료:

```text
[페어][청산트리거] id=... 사유=...
[페어][종료완료] id=...
```

정상 shutdown:

```text
[strategy][shutdown] 안전 종료 정리 시작 ...
[strategy][shutdown] 안전 종료 정리 완료 positions=flat
```

일시적인 OKX websocket reconnect:

```text
[OKXFuture WS] reconnect after error: no close frame received or sent
```

이 로그 하나만으로는 실패로 보지 않습니다. stale이 길게 지속되거나 주문/청산 실패와 연결될 때만 문제로 봅니다.

## 10. Rate Limit 대응

Binance `-1003` 또는 OKX `Too Many Requests`가 반복되면 API 사용량 압박입니다.

우선 신규 진입을 멈춥니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on rate_limit_pressure
```

그 다음 상태를 봅니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh report
```

포지션이 없고 open order도 없으면 잠시 대기 후 resume할 수 있습니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off rate_limit_recovered
```

포지션이 있으면 stop보다 close/recovery 상태를 먼저 확인합니다. 무작정 프로세스를 죽이지 않습니다.

## 11. 보조 수집기

symbol price collector 시작:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh start
```

symbol price collector 상태:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh status
```

symbol price collector 중지:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh stop
```

FX collector 시작:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh start
```

FX collector 상태:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh status
```

## 12. DB 없는 환경에서 실행

다른 사람이 DB 없이 실행할 수 있습니다.

DB를 안 쓰려면 `.env`에서 `MARIADB_*`를 비우거나 제거합니다. 강제로 끄려면 아래를 둡니다.

```env
ENABLE_DB_PERSISTENCE=0
```

이 경우:

- 전략 실행은 계속됩니다.
- order/trade/funding/balance DB 적재는 skip됩니다.
- DB 기반 리포트는 빈 결과 또는 skip 성격으로 봅니다.
- live exchange flat check는 DB와 무관하게 실행 가능합니다.

## 13. 운영 후 정리

run 종료 후 아래 순서로 봅니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_12h
```

리포트에서 먼저 볼 섹션:

- Verdict
- Event Verdict
- Strategy Performance
- Entry Submit Price Age
- DB Verdict
- Live Exchange Flat
- Symbol Price Collector

전략 수익성을 볼 때는 `Strategy Performance`의 closed pair table과 `total_close_pnl_usd`를 봅니다. 운영 안정성을 볼 때는 Event Verdict와 Live Exchange Flat을 우선합니다.
