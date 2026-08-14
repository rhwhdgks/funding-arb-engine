from __future__ import annotations

import unittest

from scripts.ops.final_run_report import build_funding_cashflow_report
from src.ops.persistence.db import insert_cashflow_row


class FundingCashflowReportTests(unittest.TestCase):
    def test_funding_db_write_preserves_negative_amount(self) -> None:
        class Cursor:
            values = None

            def execute(self, _sql, values):
                self.values = values

            def close(self):
                return None

        class Connection:
            def __init__(self):
                self.cursor_instance = Cursor()

            def cursor(self):
                return self.cursor_instance

            def commit(self):
                return None

        conn = Connection()
        insert_cashflow_row(
            conn,
            {
                "movement_key": "funding:okx:1",
                "flow_type": "funding_payment",
                "asset_code": "USDT",
                "amount": -0.25,
                "event_time_ms": 1,
            },
        )
        self.assertEqual(conn.cursor_instance.values[5], -0.25)

    def test_signed_amounts_and_account_scope_are_preserved(self) -> None:
        rows = [
            {
                "exchange_name": "binance",
                "amount": -1.25,
                "raw": {"account_label": "real", "logic": "funding fee", "pair_id": "p1"},
            },
            {
                "exchange_name": "okx",
                "amount": 0.75,
                "raw": {"account_label": "real", "logic": "funding fee", "pair_id": "p1"},
            },
            {
                "exchange_name": "binance",
                "amount": 99.0,
                "raw": {"account_label": "test", "logic": "funding fee", "pair_id": "other"},
            },
        ]
        report = build_funding_cashflow_report(
            rows,
            run_meta={"account_label": "real", "logic": "funding fee"},
        )
        self.assertEqual(report["row_count"], 2)
        self.assertAlmostEqual(report["net_amount_usd"], -0.5)
        self.assertEqual(report["unattributed_count"], 0)
        self.assertEqual(report["verdict"], "PASS")

    def test_unattributed_payment_is_warning_not_failure(self) -> None:
        report = build_funding_cashflow_report(
            [{"exchange_name": "okx", "amount": -0.1, "raw": {"account_label": "real", "logic": "funding fee"}}],
            run_meta={"account_label": "real", "logic": "funding fee"},
        )
        self.assertEqual(report["verdict"], "WARN")
        self.assertEqual(report["fail_count"], 0)

    def test_missing_expected_sync_is_warning_even_without_payments(self) -> None:
        report = build_funding_cashflow_report(
            [],
            run_meta={"account_label": "real", "logic": "funding fee"},
            sync_count=0,
            sync_failure_count=0,
            sync_expected=True,
        )
        self.assertEqual(report["verdict"], "WARN")
        self.assertEqual(report["row_count"], 0)


if __name__ == "__main__":
    unittest.main()
