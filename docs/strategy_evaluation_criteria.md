# Strategy Evaluation Criteria

이 문서는 새 전략을 붙였을 때 “좋은 전략인지” 판단하는 기준입니다. 운영 안정성 PASS와 전략 수익성 PASS는 분리합니다.

## 판단 계층

| 계층 | 질문 | 대표 근거 |
| --- | --- | --- |
| System validity | 시스템이 안전하게 돌았는가 | final report, health check, shutdown state |
| Entry quality | 예상 edge가 실제 체결 후에도 살아있는가 | `entry_quality_analysis` |
| Close quality | 청산 이유와 결과가 설명되는가 | close reason, realized PnL, fee/slippage |
| Symbol quality | 특정 심볼이 반복적으로 나쁜가 | symbol별 actual edge, hedge latency |
| Strategy decision | 이 설정을 유지/확장할 가치가 있는가 | 위 결과 종합 |

## System Validity

전략 평가 전에 아래가 먼저 통과해야 합니다.

- `final_verdict=PASS`
- `pair_entry_overfill_detected=0`
- `pair_close_leg_failed=0`
- `pair_close_pending=0`
- active orphan `0`
- live open order 잔존 `0`
- DB open-like order 잔존 `0`
- API pressure/IP ban hard failure 없음

이 조건이 깨지면 전략이 좋아도 해당 run은 전략 평가 샘플로 쓰지 않습니다.

## No-entry Run 해석

체결이 없는 run은 운영 안정성 기준으로는 정상일 수 있습니다. 시장 조건이 맞지 않으면 들어가지 않는 것이 맞습니다.

다만 전략 품질 판단에는 한계가 있습니다.

- System score: 평가 가능
- Strategy profitability: 평가 불충분
- Candidate threshold sanity: 후보 차단 reason으로 일부 평가 가능

no-entry가 반복되면 아래를 봅니다.

- threshold가 너무 높은지
- target symbol이 너무 좁은지
- maker queue가 stale/rate-limit 때문에 자주 멈추는지
- 실제 market에는 기회가 있었는데 후보 계산이 못 잡는지

## Entry Quality 핵심 지표

`entry_quality_analysis`는 pair open 직후 예상과 실제 체결 결과를 비교하는 핵심 이벤트입니다.

| 지표 | 의미 |
| --- | --- |
| `expected_net_edge_bps` | 후보 선정 시 기대한 net edge |
| `expected_open_edge_bps` | 진입 당시 가격 갭 기준 예상 open edge |
| `actual_open_edge_bps` | 실제 long/short 체결가 기준 open edge |
| `entry_edge_slippage_bps` | actual과 expected 차이 |
| `first_submit_latency_ms` | 후보 선정 후 첫 주문 제출까지 지연 |
| `first_fill_to_hedge_fill_ms` | 첫 체결 후 hedge 체결까지 지연 |
| `total_entry_elapsed_ms` | 전체 진입 소요 시간 |

좋은 진입은 아래 조건을 만족해야 합니다.

- expected가 높은데 actual이 반복적으로 음수로 바뀌지 않음
- hedge latency가 특정 심볼에서 과도하게 튀지 않음
- slippage가 심볼별로 설명 가능함
- 손실이 발생하면 `maker wait`, `hedge spread`, `stale candidate`, `fee drag`, `rate-limit delay` 중 하나로 분류 가능함

## Close Quality 핵심 지표

청산은 수익 최적화와 안전 정리가 섞이면 안 됩니다.

| close 유형 | 판단 |
| --- | --- |
| normal close | gap, funding, fee/slippage를 보고 보류 가능 |
| risk close | fee gate와 무관하게 즉시 가능해야 함 |
| shutdown close | 안전 종료 우선. 수익성 기준으로 막지 않음 |
| recovery close | 중립성 복구 우선. 수익성 기준으로 막지 않음 |
| manual close | 사용자 명령 우선. 결과만 기록 |

normal close에서 봐야 할 것:

- close reason이 실제 gap/funding 상태와 맞는지
- `normal_close_deferred_by_fee`가 과도하게 반복되지 않는지
- max defer 이후에는 정상 close가 진행되는지
- fee 포함 손익이 별도 pair PnL 리포트와 연결되는지

## Symbol Quality 기준

심볼은 진입 빈도만으로 평가하지 않습니다.

| 상태 | 기준 |
| --- | --- |
| Promote | actual edge가 안정적이고 hedge latency가 낮음 |
| Watch | 기회는 있지만 slippage 또는 spread가 가끔 큼 |
| Penalize | expected 대비 actual 악화가 반복됨 |
| Remove | 갭이 커 보여도 hedge spread/fee 때문에 반복 손실 |

예시:

- BNB처럼 기회가 잦으면 fast path 대상이 될 수 있습니다.
- 특정 코인이 hedge spread가 계속 크면 아무리 코드가 좋아도 전략 대상에서 빼는 것이 맞습니다.
- collector 대상과 strategy 대상은 분리할 수 있습니다.

## Rate-limit 평가

rate-limit는 전략 품질을 직접 깎습니다.

문제로 보는 신호:

- `risk:rate_limit_pressure`로 진입 차단 반복
- account snapshot stale로 pair 편입 지연
- OKX/Binance retry exhausted 반복
- maker cancel/re-submit이 짧은 시간에 과도하게 반복

허용 가능한 신호:

- 일시적인 pause
- 신규 maker submit만 잠깐 멈춤
- close/recovery/shutdown은 계속 허용됨

## 최소 Sample 기준

| 목적 | 최소 기준 |
| --- | --- |
| 시스템 smoke | 30m, 체결 없어도 가능 |
| 전략 방향성 | pair 3~5개 이상 또는 후보 차단 reason이 충분해야 함 |
| 심볼 평가 | symbol별 pair 3개 이상부터 참고 |
| 24h 승인 | hard failure 0 + entry quality 원인 설명 가능 |

샘플이 적으면 결론도 제한적으로 냅니다. “손실 1회”와 “반복 손실”은 다르게 봅니다.

## 전략 유지/수정/폐기 결정

| 결과 | 결정 |
| --- | --- |
| System FAIL | 전략 판단 중단, 운영 문제부터 수정 |
| Actual edge mostly positive | 전략 유지 또는 확장 검토 |
| Actual edge negative 반복 | 진입 가격/hedge latency/symbol 제외 검토 |
| Close reason 불명확 | close policy 수정 전 확장 금지 |
| Rate-limit 압박 반복 | 심볼 수/queue/requote 조정 |
| 손익 나쁨 + 원인 설명 가능 | 다음 정책 실험으로 진행 |
| 손익 나쁨 + 원인 불명확 | 전략 보류 |

## 리포트 확인 명령

최종 리포트:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_strategy_eval
```

실행 중 대시보드:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh health
```

JSONL 직접 확인이 필요할 때:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh jsonl
```

관련 문서:

- [strategy_experiment_loop.md](strategy_experiment_loop.md)
- [strategy_live_operations_runbook.md](strategy_live_operations_runbook.md)
- [operator_log_guide.md](operator_log_guide.md)
