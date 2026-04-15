# Futures-Futures Spread Strategy Draft

## 한 줄 요약
- 목표는 같은 기초자산의 선물 가격 간극과 funding 차이를 동시에 먹는 `거래소 간 선선차` 전략입니다.
- 핵심은 `지금 들어가면 기대 순이익이 얼마인지`를 보수적으로 계산하고, 그 값이 충분할 때만 진입하는 것입니다.
- 기존 Binance / Gate / OKX 실행 코어는 그대로 두고, 전략은 상위 레이어에서 별도로 붙입니다.
- 현재 초안 기준으로 고정한 선택:
  - 진입은 `지정가`
  - 초기 운영은 `반자동`
  - 보유 horizon은 `다음 1회 funding`
  - 이후에는 `다음 1회 funding 기준 순 기대값이 +일 때 자동 유지` 방향으로 확장
  - 구현 시작은 `strategy.py` 단일 파일
  - 초기 승인 방식은 `콘솔 승인`
  - 초기 버전은 `유동성 컷 없이` 사용자가 지정한 심볼 또는 `ALL`을 그대로 스캔
  - 한쪽 다리 지정가 체결 후 반대 다리 수량이 부족하면 `재호가 -> 최종 시장가 hedge`
  - 첫 다리 지정가 timeout 기본값은 `2.0초`
  - 반대 다리 재호가 timeout 기본값은 `0.7초`
  - 반대 다리 재호가 횟수 기본값은 `2번`
  - 시작 거래소 쌍 기본값은 `binance-okx`

## 1. 전략 개념

예시:
- Binance BTC 선물이 상대적으로 싸다
- OKX BTC 선물이 상대적으로 비싸다
- 그러면:
  - Binance long
  - OKX short

수익 원천은 3개입니다.
- 진입 시점의 실행 가능 가격 간극
- 보유 중 funding carry
- 이후 스프레드 축소

이 전략은 단순히 mark price 차이만 보는 것이 아니라, 실제 진입 가능한 가격 기준으로 계산해야 합니다.

## 2. 지금 레포에서의 위치

현재 선호 방향:
- 처음엔 레포 루트에 `strategy.py` 하나로 붙임
- 즉 `funding fee/strategy.py` 형태로 시작
- `main.py`는 그대로 두거나, 나중에 얇은 wrapper만 두는 방식으로 감

이유:
- 지금은 전략 초안과 first MVP를 빠르게 붙여보는 단계
- 아직 전략 구조가 완전히 확정되지 않았음
- 처음부터 폴더를 크게 쪼개면 오히려 과할 수 있음

즉 초기 구현은 이렇게 보는 게 좋습니다.
```text
strategy.py
```
- config
- universe 계산
- spread/funding pnl 계산
- 반자동 signal 출력
- 지정가 진입 호출
- pair state 추적

그리고 나중에 커지면 그때 아래처럼 분리합니다.
```text
src/
  strategy/
    futures_spread/
      config.py
      universe.py
      signal.py
      pnl_model.py
      executor.py
      runtime.py
      types.py
```

즉 현재 권장은:
- 1차: `strategy.py` 단일 파일
- 2차: 커지면 `src/strategy/futures_spread/*`로 분리

## 3. 이 전략의 핵심 출력

전략이 심볼마다 계산해야 하는 최소 출력:
- `symbol`
- `long_exchange`
- `short_exchange`
- `long_ask`
- `short_bid`
- `entry_open_edge_usd`
- `entry_open_edge_bps`
- `open_fee_cost_usd`
- `expected_funding_pnl_usd`
- `expected_close_cost_usd`
- `risk_buffer_usd`
- `expected_net_edge_usd`
- `expected_net_edge_bps`
- `hold_horizon_type`
- `hold_horizon_label`
- `can_enter`
- `block_reasons`

즉, 질문을 이렇게 바꾸면 됩니다.
- "이 종목을 지금 A long / B short로 잡으면, 예상 순이익이 플러스인가?"

## 4. 기대수익 계산 초안

### 4.1 실행 가능 진입 간극

롱 거래소 `L`, 숏 거래소 `S`일 때:

```text
entry_open_edge_usd = short_bid_S - long_ask_L
entry_open_edge_bps = entry_open_edge_usd / mid_reference * 10000
```

여기서 중요한 점:
- `mid`가 아니라 실제 executable 가격을 사용
- long은 `ask`
- short는 `bid`

즉 보이는 스프레드가 아니라, 바로 들어갔을 때 실제 먹을 수 있는 간극만 씁니다.

### 4.2 진입 funding 기대값

funding rate가 양수면:
- long이 지불
- short가 수취

단일 다리 기준 기대 funding cashflow:

```text
funding_cashflow = - position_sign * funding_rate * notional
```

여기서:
- `position_sign = +1` for long
- `position_sign = -1` for short

2다리 합치면:

```text
expected_funding_pnl_usd
  = (- funding_long_exchange * long_notional)
  + (+ funding_short_exchange * short_notional)
```

현재 기본 horizon:
- `다음 1회 funding`

즉 현재 계산 질문은 이겁니다.
- "지금 진입해서 다음 funding 한 번까지 보유한다고 가정하면, 순 기대값이 +인가?"

이후 고도화:
- `N회 funding`
- `N시간 horizon`
- `spread 회귀 기반 horizon`
으로 확장 가능

### 4.3 비용

최소 비용 구성:
- `open_fee_cost_usd`
- `expected_close_fee_cost_usd`
- `slippage_buffer_usd`
- `leg_risk_buffer_usd`
- `funding_uncertainty_buffer_usd`

실전용 보수 계산:

```text
expected_close_cost_usd
  = close_fee_cost_usd + close_slippage_buffer_usd

risk_buffer_usd
  = leg_risk_buffer_usd + drift_buffer_usd + liquidity_buffer_usd
```

### 4.4 최종 진입 점수

```text
expected_net_edge_usd
  = entry_open_edge_usd
  + expected_funding_pnl_usd
  - open_fee_cost_usd
  - expected_close_cost_usd
  - risk_buffer_usd
```

진입 조건 예시:

```text
can_enter =
  health_ok
  and symbol_supported_on_both
  and liquidity_ok
  and expected_net_edge_usd >= min_net_edge_usd
  and expected_net_edge_bps >= min_net_edge_bps
```

## 5. 심볼 / 거래소 유니버스

종목은 "뭐든지 될 수 있다"는 요구에 맞추려면, 전략 레이어가 공통 유니버스를 따로 계산해야 합니다.

필수 조건:
- 거래소 내부 심볼은 공통 base symbol 기준 사용
  - `BTC`, `ETH` 같은 내부 심볼
- 실제 raw symbol 변환은 각 exchange module이 처리
- 전략은 `base symbol`만 다룸

유니버스 구성 단계:
1. 각 거래소가 실제 지원하는 내부 symbol 집합 확보
2. 거래소 교집합 계산
3. 유동성 / 최소수량 / 계약크기 확인
4. 실행 가능한 후보 심볼만 추림

사용자 설정 방향:
- 사용자가 직접 심볼 리스트를 지정 가능
- `ALL` 같은 옵션을 두면 선택된 거래소들의 공통 지원 심볼 전체를 스캔
- 초기 버전은 유동성 컷을 따로 두지 않음
- 유동성 컷은 나중에 옵션으로 추가

즉 "전종목"은 실제로는:
- `공통 지원`
- `건강 상태 양호`
- `실행 수량 맞춤 가능`
인 심볼 집합입니다.

## 6. 전략 런타임 흐름

### 6.1 스캐너 단계

전략은 이벤트 기반 + 폴링 보조가 좋습니다.

흐름:
1. 거래소 객체 생성
2. `run()` 시작
3. 심볼별 listener 등록
4. 이벤트가 올 때마다 최신 snapshot 읽기
5. 거래소 쌍별 spread / funding / health 계산
6. 후보 리스트 정렬
7. 기준 이상만 신호로 출력

사용할 입력:
- `get_symbol_snapshot(symbol)`
- `get_account_snapshot()`
- `get_open_order_map()`
- `funding_now`
- `hedge_state` 또는 `get_health_snapshot()`

### 6.2 진입 단계

현재 합의된 초기 단계:
- `반자동`
- 전략이 추천 pair와 기대 순이익을 계산
- 콘솔에서 사용자가 승인하면 실제 진입
- 즉 `paper only`보다는 한 단계 앞이지만, `완전 자동`은 아님

live execution 순서 초안:
1. 마지막 health gate 재검사
2. 양쪽 호가 다시 읽기
3. 진입 기대수익 재계산
4. 양쪽 지정가 파라미터 계산
5. 첫 다리 지정가 주문
6. 첫 다리가 원하는 수량만큼 체결되는지 확인
7. 반대 다리 지정가 주문
8. 반대 다리 수량이 부족하면 재호가
9. 재호가 이후에도 부족 수량이 남으면 반대 거래소에서 시장가 hedge
10. pair position 저장

진입 기본 원칙:
- 초기 진입은 `시장가`가 아니라 `지정가`
- 첫 버전에서는 지정가 체결 관리와 leg risk 관리가 핵심
- 최종 rescue는 반대 거래소 `시장가 hedge`를 허용
- post-only/IOC/FOK 같은 고급 옵션은 나중에 거래소별로 확장

## 7. 실행 방식 초안

초기 버전은 `완전 자동 저지연 HFT`로 출발하면 위험합니다.

추천 단계:

### Phase 1
- operator confirm 기반 반자동
- 추천 pair와 expected net edge 출력
- 승인 시만 실제 지정가 주문

### Phase 2
- 자동 진입
- 단, 소액 / 소수 심볼 / 소수 거래소 쌍만

### Phase 3
- 자동 유지 / 자동 청산
- 단, 기준은 단순히 `funding 부호`가 아니라:
  - `다음 1회 funding 기준 expected_hold_pnl > 0`
  - `청산 비용 반영 후에도 +`
  - `health / drift / stale / 호출량 block 없음`

## 8. pair position 모델

전략은 단일 주문이 아니라 `페어 포지션`을 관리해야 합니다.

최소 필드:
- `pair_id`
- `symbol`
- `long_exchange`
- `short_exchange`
- `long_order_id`
- `short_order_id`
- `long_qty`
- `short_qty`
- `entry_time_ms`
- `entry_spread_usd`
- `entry_funding_snapshot`
- `status`

상태 예시:
- `detected`
- `arming`
- `entering`
- `partially_hedged`
- `open`
- `closing`
- `closed`
- `failed`

## 9. 리스크 가드

속도보다 먼저 필요한 가드:
- 양 거래소 health 둘 다 `can_trade=True`
- drift/stale block
- 호출량 block
- 심볼 allowlist
- 거래소 쌍 allowlist
- 최대 동시 페어 수
- 최대 gross notional
- 최대 거래소별 노출
- rescue timeout
- emergency stop

특히 필요한 것:
- 한쪽만 체결됐을 때 즉시 반대 hedge
- partial fill 시 남은 수량 자동 보정
- funding 직전/직후 불안정 시간 회피 옵션
- 유지 조건을 `funding +` 자체가 아니라 `다음 1회 funding 기준 순 기대값 +`로 판단
- 초기 유지 조건은 일단 `0보다 크면 유지`

## 10. 구현 파일 초안

초기 1차 구현은 단일 파일 기준으로 갑니다.

```text
strategy.py
```

이 안에 최소한 아래 섹션이 들어가면 됩니다.
- `StrategyConfig`
  - 거래소 목록
  - 심볼 목록 또는 `ALL`
  - 거래소 쌍 allowlist
  - min expected edge
  - 지정가 timeout / retry
  - 기본값:
    - `first_leg_timeout_sec = 2.0`
    - `hedge_requote_timeout_sec = 0.7`
    - `hedge_requote_count = 2`
    - `allowed_pairs = [("binance", "okx")]`
- `build_strategy_exchanges()`
  - 사용할 거래소 객체 생성 / run
- `resolve_strategy_symbols()`
  - 직접 지정 심볼 또는 공통 심볼 전체 계산
- `compute_pair_candidate()`
  - 거래소 쌍별 spread / funding / cost / risk 계산
- `rank_candidates()`
  - 기회 정렬
- `print_candidates()` 또는 `render_candidates()`
  - 반자동 판단용 출력
- `console_confirm_entry()`
  - 콘솔 승인 입력
- `enter_pair_limit_then_hedge()`
  - 첫 다리 지정가
  - 반대 다리 지정가
  - 부족 수량 재호가
  - 최종 시장가 hedge
- `close_pair_limit_then_hedge()`
  - 보유 pair 청산
  - 첫 다리 reduce 지정가
  - 반대 다리 reduce 지정가
  - 부족 수량 재호가
  - 최종 시장가 hedge
- `manage_open_pairs()`
  - 다음 1회 funding 기준 유지/청산 판단
- `strategy_loop()`
  - 전체 실행 루프

나중에 코드가 커지면 그때 아래처럼 분리합니다.
```text
src/strategy/futures_spread/types.py
src/strategy/futures_spread/pnl_model.py
src/strategy/futures_spread/universe.py
src/strategy/futures_spread/signal.py
src/strategy/futures_spread/runtime.py
src/strategy/futures_spread/executor.py
scripts/ops/spread_scan.py
```

## 11. `main.py`와의 관계

현재는:
- `main.py`에 바로 넣지 않음
- `strategy.py`를 별도 실행 파일로 둠

이유:
- `main.py`는 아직 placeholder 성격이 강함
- 전략과 기존 예제 실행 경로를 분리하는 편이 안전함

즉 현재 권장:
- `python strategy.py`

그리고 나중에 원하면:
- `main.py`가 `strategy.py`를 호출하는 얇은 wrapper가 될 수 있음

이게 AGENTS 원칙과도 맞습니다.
- 실행 코어 유지
- 전략 로직을 exchange module 안에 깊게 섞지 않음

## 12. 첫 구현 MVP 제안

첫 구현은 여기까지만 권장:
- 공통 유니버스 계산
- 거래소 쌍별 현재 executable spread 계산
- funding 반영 expected net edge 계산
- 상위 N개 기회 출력
- 반자동 승인용 signal 출력
- 진입은 지정가 기준
- 승인 방식은 콘솔 기준
- 부족 수량 hedge는 최종적으로 시장가 허용

아직 하지 않는 것:
- 완전 자동 실주문
- multi-pair 자동 리밸런싱
- 고급 최적화

즉 첫 MVP 질문은 이겁니다.
- "지금 어떤 심볼을 어떤 거래소 쌍으로 들어가면, 보수적으로 봐도 기대 순이익이 남나?"
- "그리고 그 후보를 내가 승인했을 때, 지정가로 진입시킬 수 있나?"

## 13. 검토 포인트

초안 검토 시 결정이 필요한 항목:
- 자동 유지/청산에서 `0보다 크면 유지` 외에 추가 버퍼를 둘지

지금 당신이 다시 보면 좋은 포인트:
1. 기본 timeout 값
   - 첫 다리 `2.0초`
   - 반대 다리 재호가 `0.7초`
2. 재호가 횟수
   - `2번`
3. 시작 거래소 쌍
   - 우선 `binance-okx`
4. 자동 유지 기준에 나중에 버퍼를 추가할지
5. `ALL` 스캔을 언제부터 유동성 컷과 함께 묶을지

## 14. 추천 결론

가장 안전한 순서:
1. scanner + expected net edge 계산기
2. 반자동 signal 출력
3. 지정가 진입 executor
4. pair state model
5. 자동 유지/청산

즉 지금 단계에서는 `완전 자동 진입`보다 `정확한 기대수익 계산기 + 반자동 지정가 진입`이 먼저입니다.
