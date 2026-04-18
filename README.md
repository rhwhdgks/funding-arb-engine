# trading

현재 실사용 기준 진입점은 아래처럼 보면 됩니다.

- [main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/main.py): 메인 전략 진입점
- [strategy.py](/home/jonghan/findalpha/arbitrage/funding%20fee/strategy.py): 실제 전략 구현
- [fx.js](/home/jonghan/findalpha/arbitrage/funding%20fee/fx.js): 환율 수집기

핵심 폴더:

- [src](/home/jonghan/findalpha/arbitrage/funding%20fee/src): 거래소 코어와 공통 로직
- [scripts](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts): 실행/운영용 런처
- [sql](/home/jonghan/findalpha/arbitrage/funding%20fee/sql): DB 스키마
- [test_server](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server): 테스트 서버 전용 env + wrapper
- [reference](/home/jonghan/findalpha/arbitrage/funding%20fee/reference): 참고용 코드/자료

참고:

- 실제 운영 흐름은 [scripts/runtime/run_strategy_live.sh](/home/jonghan/findalpha/arbitrage/funding%20fee/scripts/runtime/run_strategy_live.sh) 기준으로 보는 게 가장 정확하고, 기본 실행 대상도 [main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/main.py)입니다.
- 테스트 서버는 [test_server/main.py](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server/main.py)와 [test_server/.env](/home/jonghan/findalpha/arbitrage/funding%20fee/test_server/.env)만 따로 두고, 나머지 코드는 루트를 공유합니다.
