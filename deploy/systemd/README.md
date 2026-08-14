# systemd deployment

`funding-arb.service.example`은 전략과 embedded Ops Console을 하나의 프로세스로 실행하는 예제입니다.

설치 전 다음 값을 운영 환경에 맞게 수정합니다.

- `User`, `Group`
- `WorkingDirectory`
- `EnvironmentFile`
- `ExecStart`

그다음 확장자에서 `.example`을 제거해 systemd unit directory에 설치합니다.

```bash
sudo cp deploy/systemd/funding-arb.service.example /etc/systemd/system/funding-arb.service
sudo systemctl daemon-reload
sudo systemctl enable --now funding-arb.service
sudo systemctl status funding-arb.service
```

실계좌 키가 들어 있는 `.env`의 권한은 소유자 읽기/쓰기만 허용하는 것을 권장합니다.

```bash
chmod 600 .env
```
