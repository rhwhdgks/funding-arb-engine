from __future__ import annotations

import unittest

from src.binance_f_module.exchange import Exchange as BinanceExchange
from src.core.exchange_registry import validate_exchange_interface, validate_exchange_snapshot_shape
from src.okx_f_module.exchange import Exchange as OkxExchange


class AdapterContractTests(unittest.TestCase):
    def test_binance_and_okx_expose_strategy_contract(self) -> None:
        for exchange_class in (BinanceExchange, OkxExchange):
            exchange = exchange_class(["ETH"])
            with self.subTest(exchange=exchange.persist_name):
                self.assertTrue(validate_exchange_interface(exchange)["ok"])
                self.assertTrue(validate_exchange_snapshot_shape(exchange, "ETH")["ok"])


if __name__ == "__main__":
    unittest.main()
