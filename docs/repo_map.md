# Repo Map

이 문서는 저장소를 처음 보는 사람이 어느 파일부터 보면 되는지 빠르게 잡기 위한 지도입니다. 운영 명령은 [runbook.md](runbook.md), 현재 상태와 우선순위는 [current_status.md](current_status.md)를 기준으로 봅니다.

## 추천 읽기 순서

1. [../README.md](../README.md): 프로젝트 목적과 전체 그림
2. [current_status.md](current_status.md): 현재 완료 상태와 다음 우선순위
3. [strategy_quality_roadmap.md](strategy_quality_roadmap.md): 전략 품질 100점 목표와 작업 순서
4. [question4_comparison.md](question4_comparison.md): question4 대비 속도, 안정성, 구현 방식 비교
5. [runbook.md](runbook.md): 실행, 중지, 리포트, 장애 대응 절차
6. [env_vars.md](env_vars.md): 환경 파일 분리와 변수 역할
7. [development_requirements_checklist.md](development_requirements_checklist.md): 개발요구사항 대비 반영 상태
8. [presentation_summary.md](presentation_summary.md): 발표용 요약

## 핵심 진입점

| 경로 | 역할 |
| --- | --- |
| [../main.py](../main.py) | 전략 실행 진입점 |
| [../strategy.py](../strategy.py) | pair 후보 계산, 진입/청산/recovery/shutdown 관리 |
| [../test_server/main.py](../test_server/main.py) | 테스트 서버용 진입점 |
| [../fx.js](../fx.js) | USD/KRW 환율 수집기 |

## 코드 구조

| 경로 | 역할 |
| --- | --- |
| [../src/binance_f_module](../src/binance_f_module) | Binance Futures REST/WS/exchange adapter |
| [../src/okx_f_module](../src/okx_f_module) | OKX Futures REST/WS/exchange adapter |
| [../src/gateio_f_module](../src/gateio_f_module) | Gate.io Futures adapter |
| [../src/hyperliquid_f_module](../src/hyperliquid_f_module) | Hyperliquid Futures adapter |
| [../src/core](../src/core) | exchange registry, contract, action gate 등 공통 규약 |
| [../src/strategy](../src/strategy) | candidate, entry/close policy, hedge, risk gate, exposure guard, execution helper |
| [../src/ops/persistence](../src/ops/persistence) | DB 적재, snapshot, live/final report 보조 |
| [../src/ops/web](../src/ops/web) | 운영 웹 화면 지원 코드 |

거래소 모듈은 기본적으로 아래 3파일 구조를 따릅니다.

```text
api.py                  # REST/signing/응답 정규화
websocket_manager.py    # WS 연결/구독/재연결/market data state
exchange.py             # REST + WS 상태를 묶은 실행 adapter
```

## 실행 스크립트

| 경로 | 역할 |
| --- | --- |
| [../scripts/runtime/run_strategy_live.sh](../scripts/runtime/run_strategy_live.sh) | 전략 start/stop/status/logs/jsonl/summary/report/final-report |
| [../scripts/runtime/entry_pause.sh](../scripts/runtime/entry_pause.sh) | 신규 진입 pause/resume/status |
| [../scripts/runtime/run_symbol_price_collector.sh](../scripts/runtime/run_symbol_price_collector.sh) | symbol price collector start/stop/status/logs/once |
| [../scripts/runtime/run_fx_collector.sh](../scripts/runtime/run_fx_collector.sh) | FX collector start/stop/status/logs/once |
| [../scripts/runtime/server_preflight.sh](../scripts/runtime/server_preflight.sh) | 서버 실행 전 점검 스크립트 |

## 운영/리포트 스크립트

| 경로 | 역할 |
| --- | --- |
| [../scripts/ops/report.py](../scripts/ops/report.py) | 현재 exchange/account/report snapshot |
| [../scripts/ops/final_run_report.py](../scripts/ops/final_run_report.py) | 종료 run의 PASS/FAIL 리포트 |
| [../scripts/ops/order_trade_reconcile_report.py](../scripts/ops/order_trade_reconcile_report.py) | 주문/체결 reconcile 보조 |
| [../scripts/ops/candidate_opportunity_report.py](../scripts/ops/candidate_opportunity_report.py) | 후보/미진입 reason 요약 |
| [../scripts/ops/web.py](../scripts/ops/web.py) | 운영 웹 진입점 |

## DB와 스키마

| 경로 | 역할 |
| --- | --- |
| [../sql/trading_dev_schema.sql](../sql/trading_dev_schema.sql) | 현재 운영 기준 MariaDB 스키마 |
| [../sql/mariadb_schema.sql](../sql/mariadb_schema.sql) | 기존/참고 스키마 |
| [../scripts/db/init_db_schema.py](../scripts/db/init_db_schema.py) | DB 스키마 초기화 |

DB 설정이 없어도 전략 런타임은 실행될 수 있습니다. DB 사용 여부와 환경변수 역할은 [env_vars.md](env_vars.md)를 봅니다.

## 테스트 서버

| 경로 | 역할 |
| --- | --- |
| [../test_server](../test_server) | 테스트 서버용 env, logs, runtime 분리 |
| [../test_server/README.md](../test_server/README.md) | 테스트 서버 실행법과 주의사항 |
| [../test_server/main.py](../test_server/main.py) | 테스트 서버 진입점 |

`test_server/.env`는 실제 파일이 있을 수 있지만, 공개/공유 문서에는 값을 적지 않습니다.

## 참고 자료

| 경로 | 역할 |
| --- | --- |
| [strategy_quality_roadmap.md](strategy_quality_roadmap.md) | 전략 품질 100점 목표와 우선순위 |
| [question4_comparison.md](question4_comparison.md) | question4와 현재 구조의 속도/안정성/구현 방식 비교 |
| [../reference/개발요구사항.txt](../reference/개발요구사항.txt) | 원 개발 요구사항 |
| [../reference/question4](../reference/question4) | 구현 비교와 아이디어 참고용 레퍼런스 |
| [../reference/README.md](../reference/README.md) | reference 폴더 사용 원칙 |

## 생성물과 캐시

| 경로 | 설명 |
| --- | --- |
| `logs/` | 실행 로그와 리포트 산출물 |
| `runtime/` | pause control 등 런타임 상태 파일 |
| `.venv/`, `__pycache__/` | 개발 환경/캐시 |

위 경로는 구조 파악 시 먼저 볼 필요가 없습니다.
