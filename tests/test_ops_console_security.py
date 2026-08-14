from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from src.ops_console import action_gate
from src.ops_console.runtime import action_token_required


class OpsConsoleSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        action_gate._LIVE_ACTION_WINDOW_UNTIL_MS = 0
        action_gate._LIVE_ACTION_WINDOW_OPERATOR = ""

    def test_action_token_is_required_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(action_token_required())

    def test_live_action_requires_token_and_explicit_window(self) -> None:
        snapshot = {
            "exchanges": {
                "binance": {
                    "symbols": {
                        "ETH": {
                            "stale": False,
                            "block_reasons": [],
                            "orderbook": {"age_ms": 1},
                        }
                    }
                }
            }
        }
        env = {
            "ENABLE_WEB_TRADING": "1",
            "OPS_CONSOLE_ACTION_TOKEN_REQUIRED": "1",
            "OPS_CONSOLE_ACTION_TOKEN": "test-only-token",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(PermissionError):
                action_gate.assert_live_action_allowed(
                    snapshot,
                    exchange="binance",
                    symbol="ETH",
                    ops_token="test-only-token",
                    check_strategy_conflict=False,
                )
            action_gate.enable_live_action_window("test-only-token", operator="unit-test", duration_sec=5)
            action_gate.assert_live_action_allowed(
                snapshot,
                exchange="binance",
                symbol="ETH",
                ops_token="test-only-token",
                check_strategy_conflict=False,
            )


if __name__ == "__main__":
    unittest.main()
