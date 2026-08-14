# Security And Release

## Secret 원칙

- `.env`, `test_server/.env`, `.env.notion`, collector private env는 Git과 배포 압축에서 제외합니다.
- API key, secret, passphrase, DB password, action token은 예제와 문서에 실제 값을 넣지 않습니다.
- secret이 Git, archive, 메신저, 화면 공유에 노출되면 삭제가 아니라 **폐기 후 재발급**이 필요합니다.
- 거래소 키는 출금 권한을 끄고 필요한 futures trade/read 권한만 허용합니다.
- 가능하면 거래소 IP allowlist를 사용합니다.

## Ops Console

- 기본 bind는 `127.0.0.1`입니다.
- 상태 변경은 token과 짧은 action window가 모두 있어야 합니다.
- write request Origin은 allowlist로 제한합니다.
- 외부 접속은 인터넷 직접 공개 대신 VPN 또는 SSH tunnel을 사용합니다.
- 전략 프로세스가 유일한 exchange owner이며 standalone console은 사용하지 않습니다.

## Release 절차

1. 현재 tracked/untracked 공개 후보와 전체 Git 이력을 secret audit합니다.
2. 테스트와 정적 검사를 통과시킵니다.
3. 의도한 변경만 commit합니다.
4. worktree가 clean인지 확인합니다.
5. release builder로 archive와 manifest를 생성합니다.
6. 별도 verifier로 파일 목록, hash, secret scan을 다시 검증합니다.

```bash
.venv/bin/python scripts/release/audit_repository.py --include-untracked
.venv/bin/python scripts/release/audit_repository.py --history
.venv/bin/python -m unittest discover -s tests -v
git status --short
.venv/bin/python scripts/release/build_handoff_artifact.py --output dist/funding-arb.tar.xz
.venv/bin/python scripts/release/verify_handoff_artifact.py dist/funding-arb.tar.xz
```

builder가 dirty worktree를 거절하는 것은 정상입니다. 임시 파일을 억지로 포함하지 말고 먼저 commit scope를 정리합니다.

과거 commit에서 secret이 검출되면 현재 파일에서 삭제하는 것만으로 충분하지 않습니다. 해당 credential을 먼저 폐기/재발급한 뒤, 저장소 소유자 승인하에 history rewrite와 force-push를 수행합니다. 이미 clone한 작업자에게는 새 이력을 다시 clone하도록 안내합니다.

## 전달받은 사람의 검증

- archive verifier를 먼저 실행합니다.
- `.env.example`에서 private `.env`를 새로 만들고 본인 키만 넣습니다.
- DB를 쓰지 않으면 `ENABLE_DB_PERSISTENCE=0`으로 둡니다.
- test server smoke와 shutdown flat을 확인한 뒤 live 권한을 사용합니다.
- 전달자의 기존 API key를 재사용하지 않습니다.
