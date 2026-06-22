# 운영 Runbook

이 문서는 실서버 또는 테스트 서버에서 전략을 실행하고, 중지하고, 리포트로 상태를 판단하는 절차를 정리합니다. 프로젝트 개요는 [../README.md](../README.md), 파일 지도는 [repo_map.md](repo_map.md)를 봅니다.

## 운영 원칙

- 신규 진입보다 포지션 중립성과 안전 종료를 우선합니다.
- close, recovery, shutdown flatten은 entry pause나 신규 진입 gate 때문에 막히면 안 됩니다.
- 체결이 없는 run은 정상일 수 있습니다. 시장 조건이 맞지 않으면 pair가 열리지 않는 것이 맞습니다.
- final report는 실행 안정성을 판단하는 도구입니다. 수익성 평가는 별도 분석으로 봅니다.
- `.env` 계열 파일과 runtime/log 산출물은 공개 저장소에 올리지 않습니다.
- 전략 프로세스가 exchange session의 유일한 owner입니다. ops console은 기본적으로 전략 내장 control plane에 붙습니다.

## Control Plane 기준

- 기본 운영 경로는 `embedded console`입니다.
- `run_ops_console.sh`는 기본적으로 새 거래소 세션을 만들지 않습니다.
- 수동 주문, cancel, flatten, rebalance, pause/resume은 전략 내부 command queue로 위임됩니다.
- embedded 모드에서는 console 조회와 액션이 전략과 같은 in-memory state를 사용합니다.
- standalone console은 사용하지 않습니다. 전략이 꺼져 있으면 console도 unavailable 상태가 되는 것이 정상입니다.

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

## Ops Console 수동 주문

ops console은 `전략에 붙는 control plane`입니다. 운영 순서는 `전략 시작 -> console health 확인 -> 브라우저 접속`입니다.

로컬 PC에서만 볼 때는 `.env`에 아래 값을 둡니다.

```env
OPS_CONSOLE_HOST=127.0.0.1
OPS_CONSOLE_PORT=8010
```

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh start
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh health
```

접속 주소:

```text
http://127.0.0.1:8010
```

테스트 서버:

```bash
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_strategy_live.sh start
STRATEGY_ENV_FILE=test_server/.env scripts/runtime/run_ops_console.sh health
```

embedded 모드에서 `run_ops_console.sh start`를 호출하면:

- 전략이 이미 실행 중이면 `embedded console already available`만 출력하고 끝납니다.
- 전략이 꺼져 있으면 `strategy-attached console not reachable`로 실패합니다.
- 이 동작이 정상입니다. 별도 backend를 또 띄우지 않는 것이 목적입니다.

standalone backend는 제거했습니다. 누군가 직접 `uvicorn src.ops_console.app:app`을 실행해도 거래소 session을 만들지 않고 `strategy_attached_console_required`로 API를 거절합니다.

console 액션은 모두 `command_id` 기준으로 JSONL audit 이벤트를 남깁니다.

- `ops_console_command_enqueued`: HTTP 요청이 전략 command queue에 들어감
- `ops_console_command_started`: 전략 loop가 명령 실행 시작
- `ops_console_command_completed`: 명령 성공
- `ops_console_command_failed`: 명령 실패
- `ops_console_command_wait_timeout`: HTTP 응답 대기 timeout
- `ops_console_command_expired` / `ops_console_command_skipped`: 오래된 명령 또는 이미 취소된 명령 미실행

수동 주문/취소/청산 사고를 볼 때는 먼저 `command_id`로 위 이벤트들을 묶어서 확인합니다. 성공 응답에도 같은 `command_id`가 포함됩니다.

폰이나 다른 장비에서 접속해야 하는 경우:

- `OPS_CONSOLE_HOST=0.0.0.0`로 열어야 합니다.
- 포트포워딩으로 인터넷에 직접 노출하지 말고 Tailscale, WireGuard, Cloudflare Tunnel 같은 제한된 경로를 사용합니다.
- 실거래 버튼을 켤 때는 `OPS_CONSOLE_ACTION_TOKEN_REQUIRED=1`과 `OPS_CONSOLE_ACTION_TOKEN`을 설정합니다.
- 현재 action token은 주문/청산/entry pause 같은 실행 요청을 보호합니다. 조회 API까지 외부에 숨겨야 한다면 별도 인증 계층을 둡니다.

수동 주문의 헷지 모드:

- `헷지 안 함`: 선택한 거래소에만 주문을 냅니다.
- `체결 시 자동 헷지`: 첫 주문 전후 포지션 delta가 확인된 경우에만 반대 거래소에 반대 방향 market hedge를 냅니다.
- 지정가 주문에서 아직 체결 delta가 없으면 hedge는 `primary_fill_delta_zero`로 skip됩니다.
- 자동 hedge 실패 시에는 첫 거래소 포지션 rollback flatten을 시도합니다.
- console 주문은 전략 pair로 등록되지 않습니다.

수동 주문의 관리 모드:

- `manual_only`: 기본값입니다. 수동 지정가 주문 또는 체결 포지션을 `position_ownership.json`에 수동 관리 대상으로 기록합니다.
- `manual_only`로 기록된 심볼은 strategy 신규 진입, maker pre-position, account imbalance recovery, shutdown flatten 대상에서 제외됩니다.
- `unmanaged`: ownership 기록을 남기지 않습니다. 일반 계좌 포지션처럼 strategy recovery/shutdown 경로에 잡힐 수 있으므로 테스트나 특수 상황에서만 씁니다.
- 수동으로 만든 포지션은 console `포지션 시장가 정리` 또는 별도 운영 절차로 정리해야 합니다.
- 수동 포지션이 정리되면 console flatten이 같은 ownership 파일을 `closed`로 갱신합니다.

주의:

- 실서버 `.env`에서 `ENABLE_WEB_TRADING=1`이면 console 버튼으로 실제 주문이 나갈 수 있습니다.
- 로컬 전용 운영은 `OPS_CONSOLE_HOST=127.0.0.1`을 기본으로 둡니다.
- embedded 모드에서는 console 별도 로그 파일이 없습니다. console 동작 확인은 `run_strategy_live.sh logs` 또는 `jsonl`로 봅니다.
- console 신규 live order는 전략이 같은 symbol active pair를 들고 있으면 차단됩니다.
- `cancel-symbol`, `flatten-symbol`은 정리 액션이므로 active pair conflict check를 타지 않습니다.
- ownership 파일 경로는 기본적으로 `STRATEGY_ENTRY_CONTROL_FILE`과 같은 폴더의 `position_ownership.json`입니다. 필요하면 `POSITION_OWNERSHIP_FILE`로 명시합니다.
- 테스트 서버 BNB는 OKX에서 `51155 local compliance restrictions`로 막힐 수 있습니다. 콘솔 자동 헷지 smoke는 OKX에서 주문 가능한 심볼로 확인합니다.

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
- order/trade DB reconcile이 `PASS`입니다.
- Binance/OKX live positions가 0입니다.
- Binance/OKX live open orders가 0입니다.
- symbol price collector freshness가 정상입니다.

## 운영 Health 점검

장기 run 중간 점검이나 monitor-only 상태에서는 final report보다 health report를 먼저 봅니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh health
```

ops console만 별도로 확인할 때는 다음을 사용합니다.

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh status
STRATEGY_ENV_FILE=.env scripts/runtime/run_ops_console.sh health
```

주요 기준:

- `FAIL`은 즉시 원인 확인 대상입니다.
- `WARN`은 운영자가 판단해서 유지할 수 있는 상태입니다.
- strategy가 꺼져 있고 symbol price collector만 도는 상태는 `strategy_process_running=WARN`으로 나올 수 있습니다.
- `/healthz`가 `source_status=strategy_attached`로 응답하면 `ops_console_healthz_ok=PASS`입니다.
- standalone pid는 정상 운영 상태가 아닙니다. 발견되면 중지하고 전략 내장 console만 사용합니다.
- `okx_symbol_price_impact_complete`가 `USDC` 1개 결측으로만 뜨는 경우는 모니터 품질 경고이며, 주문/헷지 failure는 아닙니다.
- `pair_close_pending`, `pair_close_leg_failed`, `pair_entry_overfill_detected`, `strategy_danger_log_lines`가 `FAIL`이면 신규 진입보다 정리 상태를 먼저 확인합니다.

Health 항목별 판단:

| 항목 | PASS | WARN | FAIL |
| --- | --- | --- | --- |
| `strategy_process_running` | 전략 실행 중 | monitor-only 또는 의도적으로 꺼둠 | 해당 없음 |
| `symbol_price_collector_process_running` | collector 실행 중 | collector optional 설정일 때 미실행 | collector required인데 미실행 |
| `ops_console_process_running` | embedded health 기준으로 console 사용 가능 | console optional 설정일 때 미실행 | console required인데 미실행 또는 legacy pid 발견 |
| `ops_console_healthz_ok` | `/healthz` 응답 정상 | console 미실행 또는 optional 상태 | console required 또는 실행 중인데 응답 실패 |
| `*_symbol_price_fresh` | 최신 bucket이 freshness 기준 이내 | DB 적재 지연 또는 collector 정지 의심 | 해당 없음 |
| `*_symbol_price_impact_complete` | 모든 심볼 impact 가격 있음 | 일부 심볼 impact 가격 결측 | 해당 없음 |
| `pair_entry_overfill_detected_zero` | overfill 없음 | 해당 없음 | overfill 발생 |
| `pair_close_leg_failed_zero` | leg 청산 실패 없음 | 해당 없음 | 한쪽 leg 청산 실패 |
| `pair_close_pending_zero` | 청산 보류 없음 | 해당 없음 | 청산 보류 발생 |
| `strategy_danger_log_lines_zero` | 위험 로그 없음 | 전략이 꺼진 과거 로그에만 존재 | 실행 중 위험 로그 존재 |
| `exchange_ip_ban_lines_zero` | ban 흔적 없음 | 전략이 꺼진 과거 로그에만 존재 | 실행 중 ban 흔적 존재 |
| `rate_limit_pressure_lines_zero` | rate-limit 압박 없음 | circuit breaker/pause 발생 | IP ban 또는 거래 불능으로 이어짐 |

## DB Reconcile 해석

final report의 DB Verdict는 현재 run 기준 `order_history`와 `trade_history`가 운영 판단에 충분히 맞는지 확인합니다.

- `run_open_like_orders_zero=PASS`가 핵심입니다. 최신 run에 `NEW/LIVE/PARTIALLY_FILLED/OPEN` 잔존이 없어야 합니다.
- `filled_orders_have_trade_rows=PASS`는 체결 주문이 trade row와 연결된 상태입니다.
- Binance처럼 order row가 client id, trade row가 exchange order id로 들어오는 경우가 있습니다. 이때 exact id link가 실패해도 같은 exchange/symbol/side, 근접 timestamp, 동일 체결 수량이면 `Inferred Order/Trade Links`로 정합성을 인정합니다.
- inferred link는 원본 DB를 고치는 것이 아니라 reconcile report의 판정 보강입니다. 실제 미해결 row는 `Unresolved Unlinked Trades`에 남아야 합니다.

## 운영 Drill

read-only drill:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh drill
```

strategy 실행 여부까지 강제 확인:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh drill --expect-strategy-running
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh drill --expect-strategy-stopped
```

pause/resume까지 실제 적용하는 drill:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh drill --apply-pause-drill
```

기대값:

- `ops_drill_verdict=PASS`
- `strategy_status`와 `collector_status`가 기대 상태와 맞습니다.
- `ops_console_status`가 기대 상태와 맞습니다.
- `entry_control_readable=PASS`
- `ops_console_active_pair_blocks_live_entry=PASS`
- `ops_console_closed_pair_releases_live_entry=PASS`
- `ops_health_failures_zero=PASS`
- `db_persistence_smoke_passed=PASS`
- `--apply-pause-drill`을 쓴 경우, `entry_pause_on_applied`, `entry_pause_off_applied`, `entry_control_restored`가 모두 `PASS`입니다.

주의:

- 기본 drill은 상태 확인만 하고 파일을 바꾸지 않습니다.
- active pair conflict drill은 synthetic JSONL 파일만 만들고 실제 주문을 내지 않습니다.
- ops console의 신규 live order는 전략이 같은 symbol active pair를 들고 있으면 차단됩니다.
- `cancel-symbol`, `flatten-symbol`은 정리용 액션이므로 active pair conflict check를 타지 않습니다.
- pause/resume drill은 `entry_control.json`을 잠깐 바꾼 뒤 원래 payload로 복원합니다.
- 실제 열린 pair가 있는 상황에서는 stop drill을 자동화하지 않습니다. stop은 operator가 `health`와 live report를 먼저 본 뒤 실행합니다.

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

DB persistence는 명시적 opt-in입니다. DB를 쓰지 않는 환경에서는 아래처럼 두면 되고, DB를 켤 때만 `ENABLE_DB_PERSISTENCE=1`과 `MARIADB_DATABASE=trading_dev`를 포함한 접속값을 채웁니다.

```env
ENABLE_DB_PERSISTENCE=0
```

이 경우 전략 실행은 계속되지만 DB 적재와 DB 기반 reconcile은 skip 또는 빈 결과로 볼 수 있습니다. live exchange flat check는 DB와 무관하게 확인할 수 있습니다.

DB 장애 격리 smoke:

```bash
STRATEGY_ENV_FILE=.env scripts/runtime/run_strategy_live.sh db-smoke
```

기대값:

- `db_disabled_no_env`가 `PASS`입니다.
- `db_enabled_but_required_env_missing`가 `PASS`입니다.
- `db_configured_but_unreachable`가 `PASS`입니다.
- DB가 비활성/미설정이면 persistence가 `db_disabled`로 skip됩니다.
- DB가 설정됐지만 접속 불가이면 order/trade/entry-quality writer는 skip 로그만 남기고 프로세스를 죽이지 않습니다.

주의:

- `symbol_price_collector`는 DB 적재용 보조 수집기라서 DB가 없으면 실행 대상이 아닙니다.
- 전략 런타임은 DB가 없어도 주문, 헷지, 청산 경로가 DB에 의존하지 않아야 합니다.

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
