"""Oracle freshness monitor stubs."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OracleMonitorConfig:
    enabled: bool = False
    max_age_ms: int = 60_000


class OracleFreshnessMonitor:
    def __init__(self, config: OracleMonitorConfig):
        self.config = config

    def is_fresh(self, timestamp_ms: int) -> bool:
        return True

