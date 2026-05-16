# Funding Fee Arbitrage Runtime

Binance Futures와 OKX Futures 사이에서 funding/가격 괴리를 보고 페어 진입, 헷지, 청산, 리포트를 수행하는 운영용 런타임입니다.

현재 기준 목표는 수익 전략 자체보다 **실행 안정성**입니다. 즉 한쪽 주문만 남는 상황, overfill, close pending, stale state, DB 정합성 문제를 줄이고, run 종료 후 final report로 PASS/FAIL을 판단하는 구조입니다.

## 현재 운영 기준

- 메인 진입점은 [main.py](main.py)입니다.
- 실제 전략 루프는 [strategy.py](strategy.py)에 있습니다.
- 전략 설정은 `.env` 또는 `test_server/.env`에서 읽습니다.
- 실서버와 테스트 서버는 같은 코드를 공유합니다.
- 테스트 서버는 `test_server/.env`만 다르고, 코드 복사본을 따로 운영하지 않습니다.
- DB는 선택값입니다. DB 설정이 없으면 DB writer는 꺼지고 전략 런타임은 계속 실행됩니다.
- 체결이 없는 run은 정상 run으로 봅니다. 기회가 없어서 pair가 열리지 않은 run도 PASS 대상입니다.

## 주요 파일

- [main.py](main.py): 전략 실행 진입점
- [strategy.py](strategy.py): 전략 런타임, pair 관리, shutdown cleanup
- [src/strategy/candidates.py](src/strategy/candidates.py): 후보 계산
- [src/strategy/execution.py](src/strategy/execution.py): 주문 실행 보조
- [src/strategy/close_policy.py](src/strategy/close_policy.py): 청산 조건 판단
- [src/strategy/risk_gate.py](src/strategy/risk_gate.py): 신규 진입 차단 조건
- [scripts/runtime/run_strategy_live.sh](scripts/runtime/run_strategy_live.sh): start/stop/status/logs/final-report 런처
- [scripts/ops/final_run_report.py](scripts/ops/final_run_report.py): 최종 PASS/FAIL 리포트
- [docs/runbook.md](docs/runbook.md): 운영 절차
- [docs/env_vars.md](docs/env_vars.md): 환경변수 설명
- [docs/repo_map.md](docs/repo_map.md): 파일 구조 설명

## 빠른 시작

가상환경과 의존성이 준비되어 있다고 가정합니다.

```bash
cd "/home/jonghan/findalpha/arbitrage/funding fee"
```

실서버 12시간 실행:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=12h scripts/runtime/run_strategy_live.sh start
```

실서버 무기한 실행:

```bash
STRATEGY_ENV_FILE=.env STRATEGY_RUN_DURATION=forever scripts/runtime/run_strategy_live.sh start
```

테스트 서버 30분 실행:

```bash
STRATEGY_ENV_FILE=test_server/.env STRATEGY_RUN_DURATION=30m scripts/runtime/run_strategy_live.sh start
```

상태 확인:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh status
```

로그 확인:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh logs
```

중지:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh stop
```

최종 리포트:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh final-report --label real_12h
```

`STRATEGY_RUN_DURATION=forever`로 실행한 경우 final report는 직접 stop한 뒤 생성합니다.

## PASS 기준

기본적으로 아래가 만족되면 운영 안정성은 PASS로 봅니다.

- `pair_entry_overfill_detected=0`
- `pair_close_leg_failed=0`
- `pair_close_pending=0`
- `danger_log_lines=0`
- shutdown 후 `remaining_positions=[]`
- shutdown 후 `pending_pair_ids=[]`
- Binance/OKX live positions `0`
- Binance/OKX live open orders `0`
- DB open-like orders `0`
- symbol price collector fresh

체결이 없어서 `trade_history`가 비어 있는 것은 실패가 아닙니다. 이 경우 final report의 Strategy Performance에는 `pair_opened=0`, `pair_closed=0`으로 표시됩니다.

## 현재 기본 전략 파라미터

중요 값은 env에서 바꿉니다.

- `STRATEGY_SYMBOLS`: 거래 대상 심볼 목록
- `STRATEGY_ORDER_NOTIONAL_USD`: 주문 기준 notional
- `STRATEGY_LEVERAGE`: 거래소에 설정할 레버리지
- `STRATEGY_MAX_GROSS_LEVERAGE`: 계정 전체 gross exposure cap
- `STRATEGY_MIN_EXPECTED_NET_EDGE_BPS`: 진입 최소 기대 edge
- `STRATEGY_REQUIRE_FUNDING_PAYMENT`: funding을 내는 방향만 허용할지
- `STRATEGY_TAKE_PROFIT_BPS`: 기본 `6`
- `STRATEGY_STOP_LOSS_BPS`: 기본 `8`, 내부에서는 `-8bps`로 처리
- `STRATEGY_HOLD_BUFFER_BPS`: 기본 `2`

## 운영 중 신규 진입만 멈추기

기존 포지션의 close/recovery/shutdown은 유지하고 신규 진입만 막습니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh on manual_pause
```

재개:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh off manual_resume
```

상태:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/entry_pause.sh status
```

## 보조 수집기

symbol price collector:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_symbol_price_collector.sh start
```

FX collector:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_fx_collector.sh start
```

## 자세한 운영 문서

실제 운영 절차, 장애 대응, 리포트 해석은 [docs/runbook.md](docs/runbook.md)를 기준으로 봅니다.
