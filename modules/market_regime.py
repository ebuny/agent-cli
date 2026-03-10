"""Market regime classification for research scoring and live allocation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from common.models import MarketSnapshot


@dataclass
class RegimeProfile:
    label: str = "unknown"
    trend_pct: float = 0.0
    realized_vol_pct: float = 0.0
    funding_bps: float = 0.0
    oi_change_pct: float = 0.0

    def to_dict(self) -> Dict[str, float | str]:
        return {
            "label": self.label,
            "trend_pct": round(self.trend_pct, 4),
            "realized_vol_pct": round(self.realized_vol_pct, 4),
            "funding_bps": round(self.funding_bps, 4),
            "oi_change_pct": round(self.oi_change_pct, 4),
        }


class MarketRegimeClassifier:
    """Classify a recent snapshot window into a coarse trading regime."""

    def classify(self, snapshots: Sequence[MarketSnapshot]) -> RegimeProfile:
        if len(snapshots) < 2:
            return RegimeProfile()

        first = snapshots[0]
        last = snapshots[-1]
        first_mid = float(first.mid_price or 0.0)
        last_mid = float(last.mid_price or 0.0)
        trend_pct = ((last_mid - first_mid) / first_mid * 100.0) if first_mid else 0.0

        returns: List[float] = []
        funding_values: List[float] = []
        oi_values: List[float] = []
        prev_mid = first_mid
        for snap in snapshots:
            mid = float(snap.mid_price or 0.0)
            if prev_mid:
                returns.append(abs((mid - prev_mid) / prev_mid * 100.0))
            prev_mid = mid or prev_mid
            funding_values.append(abs(float(snap.funding_rate or 0.0)) * 10_000.0)
            if float(snap.open_interest or 0.0) > 0:
                oi_values.append(float(snap.open_interest or 0.0))

        realized_vol = sum(returns) / len(returns) if returns else 0.0
        funding_bps = sum(funding_values) / len(funding_values) if funding_values else 0.0
        oi_change_pct = 0.0
        if len(oi_values) >= 2 and oi_values[0]:
            oi_change_pct = (oi_values[-1] - oi_values[0]) / oi_values[0] * 100.0

        if funding_bps >= 2.0 and abs(trend_pct) <= 3.0 and realized_vol <= 2.0:
            label = "carry"
        elif realized_vol >= 3.0 or abs(trend_pct) >= 8.0 or abs(oi_change_pct) >= 20.0:
            label = "volatile"
        elif trend_pct >= 2.0:
            label = "trend_up"
        elif trend_pct <= -2.0:
            label = "trend_down"
        else:
            label = "balanced"

        return RegimeProfile(
            label=label,
            trend_pct=trend_pct,
            realized_vol_pct=realized_vol,
            funding_bps=funding_bps,
            oi_change_pct=oi_change_pct,
        )


def summarize_regime_support(
    regime_counts: Dict[str, int],
    profitable_regime_counts: Dict[str, int],
) -> List[str]:
    ranked = []
    for regime, total in regime_counts.items():
        profitable = profitable_regime_counts.get(regime, 0)
        hit_rate = profitable / total if total else 0.0
        ranked.append((hit_rate, profitable, total, regime))
    ranked.sort(key=lambda item: (-item[0], -item[1], item[3]))
    return [item[3] for item in ranked if item[1] > 0]
