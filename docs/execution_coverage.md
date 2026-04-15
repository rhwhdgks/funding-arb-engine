# Execution Coverage Checklist

This checklist tracks the current OMS/MD execution coverage across Binance Futures, Gate.io Futures, and OKX Futures.

Status legend:
- `done`: implemented and verified in the current architecture
- `mostly_done`: implemented in the managed-symbol universe, with some observability or hardening still in progress
- `in_progress`: partially implemented, needs more work to be treated as stable

## Current checklist

| Item | Status | Notes |
|---|---|---|
| 지정가 주문 | `done` | Common `order()` path across Binance / Gate.io / OKX |
| 전종목 orderbook websocket | `done` | Full-universe readiness now uses REST best bid/ask prefill followed by websocket updates; full-universe smoke reached 100% matched on Binance / Gate.io / OKX |
| 서버시간-PC시간 오차 점검 | `done` | Drift-based health gating is active |
| 잔고 및 포지션 점검 | `done` | Includes common balance decomposition layer |
| 전종목 / 단일종목 미체결 주문 확인 | `done` | `open_orders_snapshot` + `waiting_list` merged view aligned for all-symbol and per-symbol reads |
| 주문 취소 / 전체 취소 | `done` | Single cancel + cancel-all verified |
| 체결 주문 조회 | `done` | Common `get_trades()` / `get_trade_history()` |
| 초기 세팅(set_leverage, cross margin) | `done` | Common `bootstrap_symbol()` |
| 거래소별 종목별 1분봉 조회 | `done` | Common `get_ohlcv_snapshot()` |
| 호출량 점검과 주문 block 조건 | `done` | Common health snapshot + rate-limit visibility now aligned across Binance / Gate.io / OKX |
| 실시간 / historical funding rate 조회 | `done` | Live funding + preload history supported |
| 전종목 tick websocket | `done` | Full-universe REST prefill + websocket updates verified at 100% matched on Binance / Gate.io / OKX |
| 주문 input/output 통일 및 추상화 | `done` | Common public interface is codified in `src/core/exchange_registry.py`; normalized account/symbol/trade keys pass the shared interface smoke script |
| 여러 거래소를 수용할 수 있는 구조 | `done` | Exchange registry + shared ops/persistence/web layers now build Binance / Gate.io / OKX from the same top-level path without `BaseExchange` |

## Current focus
- Keep hardening and recovery work without disturbing the working execution core

## Recent progress notes
- Full-universe websocket smoke script:
  - `test/marketdata/all_symbols_ws_smoke.py`
- Initial REST market prefill added before websocket updates:
  - Binance: `/fapi/v1/ticker/bookTicker`
  - Gate.io: `/futures/usdt/tickers`
  - OKX: `/api/v5/market/tickers?instType=SWAP`
- Full-universe smoke verified after REST prefill:
  - Binance orderbook only: 542/542 matched within 4s
  - Binance tick only: 542/542 matched within 4s
  - Gate.io orderbook only: 642/642 matched within 4s
  - Gate.io tick only: 642/642 matched within 4s
  - OKX orderbook only: 285/285 matched within 4s
  - OKX tick only: 285/285 matched within 4s
  - Binance orderbook+tick: 542/542 matched within 4s
  - Gate.io orderbook+tick: 642/642 matched within 4s
  - OKX orderbook+tick: 285/285 matched within 4s
- Shared public-interface smoke added:
  - `test/interfaces/common_interface_smoke.py`
- Shared interface smoke verified:
  - Binance / Gate.io / OKX all expose the expected methods
  - required account/symbol snapshot keys passed
  - shared trade normalization checks passed on Gate.io / OKX
- Recovery/reconcile hardening:
  - startup reconcile added to Binance / Gate.io / OKX
  - explicit `reconcile_state()` and `get_reconcile_snapshot()` added
  - websocket stale/recovered auto reconcile added with cooldown guard
  - periodic reconcile guard added
  - account/open-order stale guard added
  - waiting-list vs open-order snapshot drift guard added
  - restart recovery smoke verified on OKX with a real open order carried across a new exchange instance
  - websocket auto-reconcile smoke verified:
    - Binance: `ws_stale -> ws_recovered`
    - Gate.io: `ws_stale -> ws_recovered`
    - OKX: `ws_stale -> ws_recovered`
  - reconcile guard smoke verified:
    - Binance / BTC
    - Gate.io / BTC
    - OKX / ETH
  - short runtime soak / restart / reconcile smoke verified:
    - Binance / BTC
    - Gate.io / BTC
    - OKX / ETH
- Order lifecycle traceability hardening:
  - successful order acknowledgements can now emit direct `order_live` follow-up events
  - successful single cancel / cancel-all close-out can now emit direct `order_canceled` follow-up events
  - waiting-list status polling now keeps a per-order cache and updates the local waiting row with the latest polled payload
  - trade-based market-order follow-up can now emit `partial_fill -> fill` progression
  - polled status transitions can emit `partial_fill`, `fill`, `order_canceled`, `order_missing`, and `order_status_update`
  - offline smoke verified `partial_fill -> fill -> order_missing` transitions on Binance / Gate.io / OKX
  - submit-stage request payload now keeps exchange-facing `client_order_id` for better `order_submit -> order_ack` stitching
  - cancel success now prunes local open-order snapshot immediately
  - live OKX cancel-race smoke verified:
    - `order_submit -> order_ack -> order_live -> cancel_submit -> cancel_ack -> order_canceled`
