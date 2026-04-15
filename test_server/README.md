# Test Server Runtime

이 폴더는 기존 라이브 코드를 건드리지 않고, 테스트 서버/데모 계정으로 별도 런타임을 돌리기 위한 전용 폴더입니다.

핵심 원칙:
- 기존 루트 코드 수정 없이 별도 실행
- 별도 `.env`
- 별도 `logs/`
- 별도 DB 권장

## 준비

1. `test_server/.env.example`를 `test_server/.env`로 복사
2. 테스트/데모 키 입력
3. 가능하면 `MARIADB_DATABASE`도 별도 DB 사용

## DB 스키마 초기화

```bash
cd test_server
scripts/db/init_db_schema.sh
```

## 실행

```bash
cd test_server
scripts/runtime/run_strategy_live.sh start
```

## 확인

```bash
cd test_server
scripts/runtime/run_strategy_live.sh status
scripts/runtime/run_strategy_live.sh logs
scripts/runtime/run_strategy_live.sh jsonl
scripts/runtime/run_strategy_live.sh report
scripts/runtime/run_strategy_live.sh ledger
```

## 중지

```bash
cd test_server
scripts/runtime/run_strategy_live.sh stop
```
