from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

from src.ops.persistence.db import db_persistence_enabled
from src.ops.persistence.live_snapshot import persist_exchange_snapshots


RuntimeLog = Callable[[str, Any | None], None]


def _env_flag(key: str, default: str = "0") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class SnapshotPersistenceConfig:
    enabled: bool
    interval_sec: float
    funding_limit: int
    persist_funding: bool
    persist_balance: bool
    persist_positions: bool

    @classmethod
    def from_env(cls) -> "SnapshotPersistenceConfig":
        interval_sec = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "120").strip())
        funding_limit = int(os.getenv("SNAPSHOT_FUNDING_LIMIT", "30").strip())
        return cls(
            enabled=db_persistence_enabled() and _env_flag("ENABLE_DB_SNAPSHOTS", "1"),
            interval_sec=max(interval_sec, 10.0),
            funding_limit=max(funding_limit, 1),
            persist_funding=_env_flag("SNAPSHOT_PERSIST_FUNDING", "1"),
            persist_balance=_env_flag("SNAPSHOT_PERSIST_BALANCE", "1"),
            persist_positions=_env_flag("SNAPSHOT_PERSIST_POSITIONS", "1"),
        )


class RuntimePersistence:
    def __init__(self, *, log: RuntimeLog | None = None) -> None:
        self._log = log

    @property
    def enabled(self) -> bool:
        return False

    def log(self, message: str, payload: Any | None = None) -> None:
        if self._log is not None:
            self._log(message, payload)
            return
        if payload is None:
            print(message)
        else:
            print(f"{message} {payload}")

    async def run_snapshot_loop(self, exchanges: dict[str, Any], symbols: list[str]) -> None:
        self.log("[snapshot-loop] disabled")


class NoopRuntimePersistence(RuntimePersistence):
    pass


class DbRuntimePersistence(RuntimePersistence):
    def __init__(self, config: SnapshotPersistenceConfig, *, log: RuntimeLog | None = None) -> None:
        super().__init__(log=log)
        self.config = config

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    async def run_snapshot_loop(self, exchanges: dict[str, Any], symbols: list[str]) -> None:
        if not self.config.enabled:
            self.log("[snapshot-loop] disabled")
            return

        self.log(
            "[snapshot-loop] start",
            {
                "interval_sec": self.config.interval_sec,
                "funding_limit": self.config.funding_limit,
                "persist_funding": self.config.persist_funding,
                "persist_balance": self.config.persist_balance,
                "persist_positions": self.config.persist_positions,
                "symbols": symbols,
                "exchanges": sorted(exchanges.keys()),
            },
        )

        while True:
            started_ms = int(time.time() * 1000)

            for exchange_name, exchange in exchanges.items():
                try:
                    summary = await persist_exchange_snapshots(
                        exchange_name=exchange_name,
                        exchange=exchange,
                        symbols=symbols,
                        funding_limit=self.config.funding_limit,
                        persist_funding=self.config.persist_funding,
                        persist_balance=self.config.persist_balance,
                        persist_positions=self.config.persist_positions,
                    )
                    self.log(f"[snapshot-loop][{exchange_name}]", summary)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"[snapshot-loop][{exchange_name}][error] {exc}")

            elapsed_sec = max((int(time.time() * 1000) - started_ms) / 1000.0, 0.0)
            sleep_sec = max(self.config.interval_sec - elapsed_sec, 1.0)
            await asyncio.sleep(sleep_sec)


def build_runtime_persistence_from_env(*, log: RuntimeLog | None = None) -> RuntimePersistence:
    config = SnapshotPersistenceConfig.from_env()
    if not config.enabled:
        return NoopRuntimePersistence(log=log)
    return DbRuntimePersistence(config, log=log)
