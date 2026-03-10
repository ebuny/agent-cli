"""Simple microprice calculator."""
from __future__ import annotations


class L2MicropriceCalculator:
    def compute(self, bid: float, ask: float) -> float:
        if bid <= 0 or ask <= 0:
            return max(bid, ask, 0.0)
        return (bid + ask) / 2

