# Documentation

문서는 목적별로 아래 순서로 읽으면 됩니다.

## 시작하기

- [repo_map.md](repo_map.md): 디렉터리 구조와 추천 코드 탐색 순서
- [env_vars.md](env_vars.md): 실행 환경, credential, DB 및 strategy 설정
- [runbook.md](runbook.md): 시작, 상태 확인, 종료, 리포트, 장애 대응
- [../test_server/README.md](../test_server/README.md): 테스트 서버 smoke/soak 실행

## 운영 안정성

- [operator_log_guide.md](operator_log_guide.md): 주요 event와 danger/warning 해석
- [security_release.md](security_release.md): secret 점검과 release artifact 절차

## 전략 실험

- [strategy_experiment_loop.md](strategy_experiment_loop.md): 전략 변경부터 검증까지의 표준 루프
- [strategy_evaluation_criteria.md](strategy_evaluation_criteria.md): expected/actual edge와 체결 품질 기준
- [strategy_live_operations_runbook.md](strategy_live_operations_runbook.md): 새 전략의 실거래 전환 절차

실행 중 생성되는 JSONL, DB snapshot, final report와 credential 파일은 문서 소스가 아니며 Git에 포함하지 않습니다.
