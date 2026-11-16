"""Runtime configuration loaded from environment variables.

The instrument roster is fixed at process start (single-instance lab service);
each entry maps the instrument name used in URLs to a TCP SCPI endpoint.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentConfig:
    name: str
    host: str
    port: int


def _default_instruments() -> tuple[InstrumentConfig, ...]:
    # Host names match the docker-compose services; ports are the raw TCP
    # SCPI ports exposed by the two independent simulators.
    return (
        InstrumentConfig("inst1", os.environ.get("INST1_HOST", "inst1"), int(os.environ.get("INST1_PORT", "5025"))),
        InstrumentConfig("inst2", os.environ.get("INST2_HOST", "inst2"), int(os.environ.get("INST2_PORT", "5025"))),
    )


class Settings:
    def __init__(self) -> None:
        self.db_path: str = os.environ.get("DB_PATH", "/data/acquisitions.db")
        self.command_timeout: float = float(os.environ.get("SCPI_COMMAND_TIMEOUT", "5.0"))
        self.restore_attempts: int = int(os.environ.get("RESTORE_ATTEMPTS", "3"))
        self.restore_backoff: float = float(os.environ.get("RESTORE_BACKOFF", "0.5"))
        self.startup_recovery_attempts: int = int(os.environ.get("STARTUP_RECOVERY_ATTEMPTS", "10"))
        self.startup_recovery_backoff: float = float(os.environ.get("STARTUP_RECOVERY_BACKOFF", "2.0"))
        self.instruments: tuple[InstrumentConfig, ...] = _default_instruments()

    def instrument(self, name: str) -> InstrumentConfig | None:
        for item in self.instruments:
            if item.name == name:
                return item
        return None


settings = Settings()
