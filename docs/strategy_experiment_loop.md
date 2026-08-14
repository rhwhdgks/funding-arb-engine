# Strategy Experiment Loop

이 문서는 운영 시스템을 고정한 상태에서 전략만 바꿔 실험하는 기준을 정리합니다. 목표는 `strategy.py`, exchange adapter, DB writer, recovery, shutdown, console을 매번 건드리지 않고도 새 전략을 안전하게 검증하는 것입니다.

## 핵심 원칙

- 전략 변경은 가능한 한 `src/strategy` 아래 policy 모듈에만 둡니다.
- 주문 실행, 헷지, 복구, shutdown, DB persistence, console control plane은 실험 대상이 아닙니다.
- 한 번에 하나의 가설만 바꿉니다.
- 수익성 판단과 시스템 안정성 판단을 분리합니다.
- no-entry run은 시스템 정상일 수 있지만 전략 성능 샘플로는 부족합니다.

## 전략 변경 허용 구역

| 영역 | 파일 | 역할 |
| --- | --- | --- |
| 진입 조건 | `src/strategy/entry_policy.py` | expected edge, funding direction, post-fill quality 판단 |
| 진입 가격 | `src/strategy/order_price_policy.py` | maker 가격, buffer, cancel/requote 기준 |
| 청산 조건 | `src/strategy/close_policy.py` | gap, funding, fee gate, max hold 기준 |
| 공통 룰 | `src/strategy/rules.py` | 전략 판단에 필요한 순수 계산/보조 룰 |
| 후보 생성 | `src/strategy/candidates.py` | 후보 집계와 score 계산 |
| 설정 로딩 | `src/strategy/config.py`, `src/strategy/models.py` | env 값을 config/model로 연결 |

## 전략 변경 금지 구역

| 영역 | 이유 |
| --- | --- |
| exchange adapter | 전략 실험마다 거래소 I/O contract가 바뀌면 원인 분석이 어려워짐 |
| `src/strategy/execution.py` | 체결 안정성 hot path. 전략 개선과 분리 |
| `src/strategy/hedge.py` | 중립성 보장 경로. 수익성 실험 때문에 느슨해지면 안 됨 |
| recovery/shutdown | 손실 방지 안전장치. 전략과 독립 |
| DB writer/schema | 전략 실험마다 schema가 바뀌면 리포트 비교가 불가능 |
| ops console command path | 수동 조작/헬스 체크는 운영 계층 |

예외적으로 execution/hedge를 바꿔야 한다면 그 작업은 `전략 실험`이 아니라 `OMS 안정성 변경`으로 분류합니다. 이 경우 test server smoke와 final report를 먼저 통과시켜야 합니다.

## 실험 단위

각 실험은 아래 정보를 먼저 정합니다.

| 항목 | 예시 |
| --- | --- |
| Hypothesis | BNB에서 maker 유지 시간을 줄이면 actual edge 하락이 줄어든다 |
| Target symbols | `BNB` 또는 `BNB,SOL` |
| Env profile | `test_server/.env` 또는 `.env` |
| Changed knobs | `STRATEGY_MIN_EXPECTED_NET_EDGE_BPS`, maker quote/cancel 설정 |
| Fixed knobs | leverage, max open pairs, exposure cap, shutdown mode |
| Runtime | smoke 30m, validation 6h, final 24h |
| Success metric | hard failure 0, actual edge negative 감소, hedge latency 개선 |

## 표준 실험 순서

1. 변경 전 기준을 기록합니다.
2. policy/config만 수정합니다.
3. 정적 검사를 돌립니다.
4. test server smoke를 먼저 돌립니다.
5. final report와 entry quality를 봅니다.
6. 문제가 없으면 실서버 짧은 run으로 옮깁니다.
7. 실서버 24h run은 마지막 검증으로 봅니다.
8. 결과를 기준으로 keep/revert/tune을 결정합니다.

정적 검사:

```bash
.venv/bin/python -m py_compile main.py strategy.py src/strategy/config.py src/strategy/entry_policy.py src/strategy/order_price_policy.py src/strategy/close_policy.py
bash -n scripts/runtime/run_strategy_live.sh
```

테스트 서버 smoke:

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh final-report --label strategy_smoke
```

실서버 짧은 run:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=2h scripts/runtime/run_strategy_live.sh start
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_strategy_2h
```

실서버 24h:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=24h scripts/runtime/run_strategy_live.sh start
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_strategy_24h
```

## 실험 중 고정해야 하는 운영 기준

- `pair_entry_overfill_detected=0`
- `pair_close_leg_failed=0`
- `pair_close_pending=0`
- active orphan `0`
- shutdown 후 remaining positions `[]`
- DB open-like order `0`
- API pressure가 신규 주문/헷지 판단을 막을 정도로 악화되지 않음

위 기준이 깨지면 전략 성능 분석보다 시스템 안정성 원인 분석을 먼저 합니다.

## 변경 판단

| 결과 | 판단 |
| --- | --- |
| hard failure 발생 | 전략 결과와 무관하게 실패 |
| no-entry | 시스템은 정상 가능. 전략 성능 샘플은 부족 |
| entry는 됐지만 actual edge가 반복 음수 | 진입 가격/hedge latency/symbol 품질 재검토 |
| expected와 actual 차이가 크지만 원인이 분류됨 | 다음 실험으로 tuning 가능 |
| 손익이 나쁘고 원인도 불명확 | 전략 기준 보류 |
| 같은 설정으로 24h PASS + 원인 설명 가능 | 다음 심볼 또는 다음 파라미터로 확장 |

## 확장 순서

1. 단일 심볼에서 체결 품질 기준선을 잡습니다.
2. 같은 심볼에서 청산/gap/fee 기준을 고정합니다.
3. 후보 1개를 추가합니다.
4. 3~4개 shortlist로 확장합니다.
5. 10개 이상은 collector 감시와 strategy 대상 심볼을 분리합니다.

심볼을 늘리는 것은 성능 개선이 아니라 복잡도 증가입니다. 체결 품질이 설명되지 않는 상태에서 심볼을 늘리지 않습니다.

## 기록 규칙

실험이 끝나면 최소한 아래를 남깁니다.

- env 파일명
- 변경한 config 값
- run duration
- pair open/close count
- hard failure count
- expected vs actual edge 요약
- hedge latency 요약
- keep/revert/tune 결정

관련 문서:

- [strategy_evaluation_criteria.md](strategy_evaluation_criteria.md)
- [strategy_live_operations_runbook.md](strategy_live_operations_runbook.md)
