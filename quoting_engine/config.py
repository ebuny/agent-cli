"""Configuration models for the lightweight quoting engine."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SpreadParams:
    min_spread_bps: float = 4.0
    max_spread_bps: float = 40.0
    level_spacing_bps: float = 3.0


@dataclass
class LadderParams:
    s0: float = 1.0
    num_levels: int = 3
    size_decay: float = 0.7


@dataclass
class LiquidationDetectorConfig:
    enabled: bool = False
    oi_drop_threshold_pct: float = 5.0
    spread_mult: float = 2.5
    size_mult: float = 0.4
    cooldown_ticks: int = 15


@dataclass
class MarketConfig:
    spread: SpreadParams = field(default_factory=SpreadParams)
    ladder: LadderParams = field(default_factory=LadderParams)
    liquidation_detector: LiquidationDetectorConfig = field(
        default_factory=LiquidationDetectorConfig,
    )

