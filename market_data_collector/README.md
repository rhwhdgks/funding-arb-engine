# Market Data Collector

`symbol_price`와 `funding_rate`를 전략 런타임과 분리해서 돌리는 전용 실행 폴더입니다.

이 폴더의 원칙:

- collector는 public market data만 수집합니다.
- collector는 주문, 헷지, 청산, 콘솔 액션을 실행하지 않습니다.
- `symbol_price`, `funding_rate`는 collector ownership으로 봅니다.
- `order_history`, `trade_history`, `balance_snapshot`, `strategy_entry_quality`는 strategy ownership으로 봅니다.
- 20~200개 심볼 감시는 collector에서 하고, strategy는 실제 주문 대상 5~15개만 운영합니다.

## Setup

```bash
cp market_data_collector/.env.collector.example market_data_collector/.env.collector
```

`market_data_collector/.env.collector`에서 DB 값과 심볼 목록만 채웁니다.

## Commands

```bash
market_data_collector/run_market_data_collector.sh once
market_data_collector/run_market_data_collector.sh start
market_data_collector/run_market_data_collector.sh status
market_data_collector/run_market_data_collector.sh logs
market_data_collector/run_market_data_collector.sh stop
```

기본 로그와 pid 파일은 `market_data_collector/logs/` 아래에 생성됩니다.

## External Folder Mode

이 폴더를 repo 밖으로 복사해서 돌릴 수도 있습니다. 그 경우 원본 프로젝트 경로만 지정합니다.

```bash
FUNDING_FEE_PROJECT_ROOT="/path/to/funding-arb-engine" \
MARKET_DATA_ENV_FILE="$HOME/market-data-collector/.env.collector" \
$HOME/market-data-collector/run_market_data_collector.sh start
```

## Strategy Separation

전략 `.env`에는 실제 주문 대상만 남깁니다.

```env
STRATEGY_SYMBOLS=XLM,SUI,LINK,BNB,ETH
```

collector `.env.collector`에는 감시 대상을 넓게 둡니다.

```env
SYMBOL_PRICE_SYMBOLS=BTC,ETH,XRP,BNB,USDC,SOL,TRX,DOGE,AAVE,ADA,BCH,ATOM,LINK,OP,XLM,TON,LTC,AVAX,HBAR,SUI
```

나중에 200심볼로 확장할 때도 전략에 200개를 직접 넣지 말고, collector에서 DB에 적재한 뒤 상위 후보만 strategy active universe로 넘기는 구조를 유지합니다.
