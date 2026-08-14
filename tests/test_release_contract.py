from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

from scripts.release.build_handoff_artifact import PROJECT_ROOT, secret_findings
from src.strategy.config import validate_live_runtime_env


class ReleaseContractTests(unittest.TestCase):
    def test_live_startup_fails_fast_when_credentials_are_missing(self) -> None:
        config = SimpleNamespace(enable_live=True, exchange_names=["binance", "okx"])
        with patch.dict("os.environ", {"ENABLE_WEB_TRADING": "0"}, clear=True):
            with self.assertRaisesRegex(ValueError, "credentials missing"):
                validate_live_runtime_env(config)  # type: ignore[arg-type]

    def test_secret_scanner_ignores_runtime_reads_but_catches_literals(self) -> None:
        self.assertEqual(
            secret_findings("api.py", b'BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")\n'),
            [],
        )
        self.assertEqual(
            secret_findings("run.sh", b'MARIADB_PASSWORD="$MARIADB_PASSWORD" \\\n'),
            [],
        )
        self.assertTrue(secret_findings("api.py", b'BINANCE_SECRET_KEY = "hardcoded-secret-value"\n'))
        self.assertTrue(secret_findings(".env.example", b'BINANCE_SECRET_KEY=hardcoded-secret-value\n'))

    def test_secret_scanner_allows_examples_but_rejects_private_env_variants(self) -> None:
        self.assertEqual(secret_findings(".env.example", b"BINANCE_SECRET_KEY=\n"), [])
        self.assertEqual(secret_findings("service/.env.local.example", b"TOKEN=\n"), [])
        self.assertTrue(secret_findings("service/.env.local", b"TOKEN=\n"))

    def test_secret_scanner_detects_private_key_material(self) -> None:
        marker = b"-----BEGIN " + b"PRIVATE KEY-----\n"
        self.assertTrue(secret_findings("key.pem", marker))

    def test_examples_have_no_embedded_secrets(self) -> None:
        examples = [
            PROJECT_ROOT / ".env.example",
            PROJECT_ROOT / "test_server" / ".env.example",
            PROJECT_ROOT / "market_data_collector" / ".env.collector.example",
        ]
        findings: list[str] = []
        for path in examples:
            findings.extend(secret_findings(str(path.relative_to(PROJECT_ROOT)), path.read_bytes()))
        self.assertEqual(findings, [])

    def test_example_credential_names_match_runtime(self) -> None:
        text = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
        required = {
            "BINANCE_ACCESS_KEY",
            "BINANCE_SECRET_KEY",
            "OKX_ACCESS_KEY",
            "OKX_SECRET_KEY",
            "OKX_PASSPHRASE",
        }
        present = {
            line.split("=", 1)[0].strip()
            for line in text.splitlines()
            if "=" in line and not line.lstrip().startswith("#")
        }
        self.assertTrue(required <= present)

    def test_all_runtime_scripts_are_tracked(self) -> None:
        runtime_dir = PROJECT_ROOT / "scripts" / "runtime"
        tracked = {
            line.strip()
            for line in __import__("subprocess")
            .check_output(["git", "ls-files"], cwd=PROJECT_ROOT, text=True)
            .splitlines()
        }
        missing = [
            str(path.relative_to(PROJECT_ROOT))
            for path in runtime_dir.iterdir()
            if path.is_file() and str(path.relative_to(PROJECT_ROOT)) not in tracked
        ]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
