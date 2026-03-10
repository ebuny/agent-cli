"""Funding rate feed helpers."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import List, Optional


@dataclass
class FundingRateSample:
    source: str
    value: float


class HyperliquidFundingRate:
    def __init__(self):
        self.value: Optional[float] = None

    def update(self, value: float) -> None:
        self.value = value

    def latest(self) -> Optional[FundingRateSample]:
        if self.value is None:
            return None
        return FundingRateSample(source="hyperliquid", value=self.value)


class PushFundingRate:
    def __init__(self, source: str):
        self.source = source
        self.value: Optional[float] = None

    def update(self, value: float) -> None:
        self.value = value

    def latest(self) -> Optional[FundingRateSample]:
        if self.value is None:
            return None
        return FundingRateSample(source=self.source, value=self.value)


class CrossVenueFundingRate:
    def __init__(self, sources: List[object]):
        self.sources = sources

    def refresh(self) -> None:
        return

    def latest(self) -> Optional[FundingRateSample]:
        values = []
        for source in self.sources:
            latest = source.latest() if hasattr(source, "latest") else None
            if latest is not None:
                values.append(latest.value)
        if not values:
            return None
        return FundingRateSample(source="median", value=float(median(values)))

