import asyncio
import time
from dotenv import load_dotenv

from src.binance_f_module.exchange import Exchange as BinanceFuturesExchange
from src.hyperliquid_f_module.exchange import Exchange as HyperliquidExchange

# 나중에 추가
# from src.gateio_f_module.exchange import Exchange as GateioFuturesExchange

load_dotenv()


def build_exchanges(symbols: list[str]) -> dict:
    """
    사용할 거래소 객체들을 생성해서 반환
    """
    exchanges = {
        "BINANCE": BinanceFuturesExchange(symbols),
        "HYPERLIQUID": HyperliquidExchange(symbols),
        # "GATEIO": GateioFuturesExchange(symbols),
    }
    return exchanges


async def start_all_exchanges(exchanges: dict):
    """
    각 거래소 run() 시작
    """
    for exchange_name, exchange in exchanges.items():
        try:
            await exchange.run()
            print(f"[run] started exchange={exchange_name}")
        except Exception as e:
            print(f"[run][error] exchange={exchange_name}: {e}")


async def bootstrap_all_symbols(exchanges: dict, symbols: list[str], leverage: int, margin_mode: str):
    """
    각 거래소 / 각 심볼에 대해 
    - 레버리지
    - 마진 모드
    초기 세팅
    """
    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            try:
                result = await exchange.bootstrap_symbol(
                    symbol=symbol,
                    leverage=leverage,
                    margin_mode=margin_mode,
                )
                print(f"[bootstrap] exchange={exchange_name} symbol={symbol} result={result}")
            except Exception as e:
                print(f"[bootstrap][error] exchange={exchange_name} symbol={symbol}: {e}")

LAST_STATE = {}
async def placeholder_strategy(symbol: str, exchanges: dict):
    snapshot = {}

    for exchange_name, exchange in exchanges.items():
        funding = exchange.funding_now.get(symbol) or {}
        position = exchange.position_snapshot.get(symbol) or {}
        health = exchange.hedge_state.get(symbol) or {}

        snapshot[exchange_name] = {
            "funding_rate": funding.get("funding_rate"),
            "qty": position.get("qty",0.0),
            "can_trade": health.get("can_trade"),
        }

    prev = LAST_STATE.get(symbol)
    if prev == snapshot:
        return

    LAST_STATE[symbol] = snapshot

    print(f"\n[strategy-placeholder] symbol={symbol}")
    for exchange_name, state in snapshot.items():
        print(f"[{exchange_name}] {state}")


async def listen_to_symbol(symbol: str, exchange_name: str, exchange, exchanges: dict):
    """
    특정 거래소 / 특정 심볼 이벤트 큐를 듣는다.
    오더북 이벤트가 오면 placeholder strategy 호출
    """
    q = exchange.register_listener(symbol)

    while True:
        _ = await q.get()

        try:
            await placeholder_strategy(symbol, exchanges)
        except Exception as e:
            print(f"[listener][error] exchange={exchange_name} symbol={symbol}: {e}")


async def attach_all_listeners(exchanges: dict, symbols: list[str]):
    """
    거래소 x 심볼 조합별 이벤트 리스너 등록
    """
    for exchange_name, exchange in exchanges.items():
        for symbol in symbols:
            asyncio.create_task(listen_to_symbol(symbol, exchange_name, exchange, exchanges))
            print(f"[listener] attached exchange={exchange_name} symbol={symbol}")


async def main():
    config = load_runtime_config()
    print("[config]", config)

    symbols = config.symbols

    # 1. 거래소 객체 생성
    exchanges = build_exchanges(symbols)

    # 2. 거래소 run 시작
    await start_all_exchanges(exchanges)

    # 3. 심볼별 bootstrap
    await bootstrap_all_symbols(
        exchanges=exchanges,
        symbols=symbols,
        leverage=config.leverage,
        margin_mode=config.margin_mode,
    )

    # 4. 이벤트 리스너 부착
    await attach_all_listeners(exchanges, symbols)

    await asyncio.sleep(5)

    # 5. 바이낸스 주문 lifecycle 테스트
    if "BINANCE" in exchanges:
        await test_order_lifecycle("BINANCE", exchanges["BINANCE"], symbols[0])
        print("\n[TEST] cancel_all_orders start")
        cancel_all_result = await exchanges["BINANCE"].cancel_all_orders(symbols[0])
        print(f"[TEST] cancel_all_result={cancel_all_result}")

        await asyncio.sleep(2)

        open_orders_after_all_cancel = await exchanges["BINANCE"].api_get_open_orders_for_test(symbols[0])
        print(f"[TEST] open_orders_after_all_cancel={open_orders_after_all_cancel}")
        print(f"[TEST] waiting_list_after_all_cancel={exchanges['BINANCE'].waiting_list}")

    # 5. 프로그램 유지
    while True:
        await asyncio.sleep(60)
    
async def test_order_lifecycle(exchange_name: str, exchange, symbol: str):
    print(f"\n[TEST] start exchange={exchange_name} symbol={symbol}")

    ob = exchange.ob.get(symbol)
    
    if not ob:
        print("[TEST] no orderbook")
        return
    best_bid = ob.get("best_bid")
    best_ask = ob.get("best_ask")

    if best_bid is None or best_ask is None:
        print(f"[TEST] invalid ob={ob}")
        return

    # 바로 체결 안 되게 현재 bid보다 충분히 낮은 가격에 buy 지정가
    test_price = round(best_bid * 0.95, 1)
    test_qty = 0.002

    print(f"[TEST] place buy qty={test_qty} price={test_price}")
    order_result = await exchange.order(symbol, "buy", test_qty, test_price)
    print(f"[TEST] order_result={order_result}")

    if not order_result or "error" in order_result:
        print("[TEST] order failed")
        return

    uuid = order_result["uuid"]

    await asyncio.sleep(2)

    status = await exchange.api_get_order_for_test(symbol, uuid)
    print(f"[TEST] get_order={status}")

    open_orders = await exchange.api_get_open_orders_for_test(symbol)
    print(f"[TEST] open_orders={open_orders}")

    await asyncio.sleep(1)

    cancel_result = await exchange.cancel_order(symbol, uuid)
    print(f"[TEST] cancel_result={cancel_result}")

    await asyncio.sleep(2)

    status_after = await exchange.api_get_order_for_test(symbol, uuid)
    print(f"[TEST] get_order_after_cancel={status_after}")

    open_orders_after = await exchange.api_get_open_orders_for_test(symbol)
    print(f"[TEST] open_orders_after_cancel={open_orders_after}")

    print(f"[TEST] waiting_list={exchange.waiting_list}")
    print(f"[TEST] end exchange={exchange_name} symbol={symbol}")


if __name__ == "__main__":
    asyncio.run(main())