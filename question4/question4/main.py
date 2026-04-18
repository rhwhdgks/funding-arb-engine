import asyncio
from src.binance_f_module.exchange import Exchange as BinanceFutureExchange
from src.binance_s_module.exchange import Exchange as BinanceSpotExchange
from src.gateio_f_module.exchange import Exchange as GateioFutureExchange
from src.gateio_s_module.exchange import Exchange as GateioSpotExchange
from dotenv import load_dotenv
import os
from src.strategy import Strategy

load_dotenv()

TICKER = os.getenv("TICKER")
MAX_QTY = float(os.getenv("MAX_QTY"))
TRADE_QTY = float(os.getenv("TRADE_QTY"))
SPREAD = float(os.getenv("SPREAD"))
CANCEL_BUFFER = float(os.getenv("CANCEL_BUFFER")) if os.getenv("CANCEL_BUFFER") is not None else None
MAKER_EXCHANGE = os.getenv("MAKER_EXCHANGE")
TAKER_EXCHANGE = os.getenv("TAKER_EXCHANGE")


async def listen_to_event(role, exch, oppo, strategy):
    q = exch.register_listener(TICKER)
    while True:
        _ = await q.get()
        if role == "maker":
            asyncio.create_task(strategy(exch, oppo))
        else:
            asyncio.create_task(strategy(oppo, exch))

async def main():
    maker = GateioFutureExchange([TICKER]) if MAKER_EXCHANGE=="Gate.io" else BinanceFutureExchange([TICKER])
    taker = GateioSpotExchange([TICKER])   if TAKER_EXCHANGE=="Gate.io" else BinanceSpotExchange([TICKER])

    # 전략 객체 생성
    strategy = Strategy(TICKER, MAX_QTY, TRADE_QTY, SPREAD, CANCEL_BUFFER)

    await asyncio.gather(maker.run(), taker.run())

    asyncio.create_task(listen_to_event("maker", maker, taker, strategy))
    asyncio.create_task(listen_to_event("taker", taker, maker, strategy))

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())