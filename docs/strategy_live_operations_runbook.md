# Strategy Live Operations Runbook

이 문서는 새 전략을 실서버에 올릴 때 사용하는 운영 절차입니다. 전체 운영 명령은 [runbook.md](runbook.md)에 있고, 이 문서는 전략 실험을 실거래로 옮기는 반복 절차에 집중합니다.

## 운영 목표

- 전략 변경은 작게 합니다.
- live run 전 test server smoke를 거칩니다.
- 실거래 중에는 신규 진입 pause, health check, console 수동 정리 경로를 유지합니다.
- run 종료 후 final report와 entry quality를 보고 keep/revert/tune을 결정합니다.

## 실행 전 체크리스트

| 항목 | 기준 |
| --- | --- |
| Git 상태 | 의도한 변경만 있음 또는 clean |
| Env | `.env`와 `test_server/.env`를 구분 |
| Symbol scope | 실서버 strategy 대상 심볼을 명확히 제한 |
| Leverage | `STRATEGY_LEVERAGE` 확인 |
| Exposure cap | `STRATEGY_MAX_GROSS_LEVERAGE` 확인 |
| Max pairs | 실험 단계에 맞는 `STRATEGY_MAX_OPEN_PAIRS` |
| Console | embedded control plane 사용 |
| DB | optional. DB 실패가 runtime을 죽이면 안 됨 |

정적 검사:

```bash
.venv/bin/python -m py_compile main.py strategy.py src/strategy/config.py src/strategy/entry_policy.py src/strategy/order_price_policy.py src/strategy/close_policy.py
bash -n scripts/runtime/run_strategy_live.sh
```

## Test Server Smoke

새 전략은 먼저 test server에서 짧게 봅니다.

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
```

확인:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh health
```

종료:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh stop
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh final-report --label test_strategy_smoke
```

통과 기준:

- hard failure 0
- shutdown flat
- DB open-like order 0
- console health 정상
- no-entry면 차단 reason이 설명 가능

## Real Short Run

test server가 통과하면 실서버 짧은 run을 합니다.

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=2h scripts/runtime/run_strategy_live.sh start
```

실행 중 확인:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh summary
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh report
```

콘솔:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh health
```

브라우저:

```text
http://127.0.0.1:8010
```

대시보드에서 확인할 것:

- 런타임 RUNNING
- danger 0
- orphan 0
- API pressure 위험 아님
- Health check PASS 또는 설명 가능한 WARN
- 최근 이벤트에 close pending/overfill/rollback 반복 없음

## Real 24h Validation

짧은 run이 통과하면 24시간 run으로 봅니다.

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=24h scripts/runtime/run_strategy_live.sh start
```

종료 후:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_strategy_24h
```

최종 판단:

- 운영 안정성은 final report PASS 여부로 봅니다.
- 전략 품질은 [strategy_evaluation_criteria.md](strategy_evaluation_criteria.md) 기준으로 봅니다.
- 수익성 결론은 pair별 PnL 리포트와 연결해서 봅니다.

## 실행 중 개입 기준

### 신규 진입만 멈출 때

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on manual_pause
```

사용 상황:

- rate-limit 압박이 커짐
- 시장이 급격히 흔들림
- 설정 확인이 필요함
- 이미 열린 pair는 관리하되 새 pair는 막고 싶음

해제:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off manual_resume
```

### 전략을 안전 종료할 때

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh stop
```

기대:

- open order cancel
- tracked pair close
- leftover flatten
- orphan marker 기록 또는 clear
- `shutdown_cleanup_done`

### 콘솔로 수동 정리할 때

사용 가능한 경로:

- `cancel-symbol`
- `flatten-symbol`
- `rebalance-hedge`
- `pause-entry`
- `resume-entry`
- `live-limit`
- `live-market`

원칙:

- 수동 주문은 기본 `manual_only`로 둡니다.
- strategy active pair가 있는 symbol에 신규 수동 진입하지 않습니다.
- 안전 정리 버튼은 active pair conflict보다 우선할 수 있습니다.
- 수동 포지션은 자동 전략 pair가 아닙니다.

## 장애별 대응

| 상황 | 우선 조치 |
| --- | --- |
| `pair_close_pending` | 신규 진입 pause, pair 상태 확인, 필요 시 console flatten |
| `pair_close_leg_failed` | 신규 진입 pause, 반대 leg/recovery 상태 확인 |
| active orphan 발생 | 해당 symbol 신규 진입 금지 유지, 수동 정리 또는 recovery 확인 |
| rate-limit pressure | 신규 maker submit 빈도/심볼 수 확인, pause 후 안정화 |
| account snapshot stale | account interval, exchange 장애, websocket 상태 확인 |
| console health unavailable | 전략 프로세스가 attached console을 띄웠는지 확인 |
| no-entry 반복 | 시스템 문제와 전략 threshold 문제를 분리 |

## Run 종료 후 기록

run이 끝나면 아래를 남깁니다.

- env file
- duration
- symbols
- max pairs/leverage/exposure cap
- final verdict
- pair open/close count
- entry quality 요약
- hard failure count
- 다음 결정: keep, tune, revert, expand

## 커밋 기준

커밋 전:

```bash
git status --short
.venv/bin/python -m py_compile main.py strategy.py src/strategy/config.py
```

커밋에 포함할 것:

- 전략 policy/config 변경
- 관련 문서 변경
- 테스트/리포트 요약

커밋에 포함하지 않을 것:

- `.env`
- runtime file
- log file
- DB dump
- API key/token

관련 문서:

- [strategy_experiment_loop.md](strategy_experiment_loop.md)
- [strategy_evaluation_criteria.md](strategy_evaluation_criteria.md)
- [runbook.md](runbook.md)
