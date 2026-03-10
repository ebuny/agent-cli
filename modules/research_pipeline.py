"""Historical dataset loading, walk-forward aggregation, and capital allocation."""
from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from common.models import MarketSnapshot
from modules.market_regime import summarize_regime_support
from modules.replay_engine import WalkForwardFoldResult


_ALIASES = {
    "timestamp": "timestamp_ms",
    "ts": "timestamp_ms",
    "mid": "mid_price",
    "mark_price": "mid_price",
    "volume": "volume_24h",
    "vol24h": "volume_24h",
    "funding": "funding_rate",
    "oi": "open_interest",
}


@dataclass
class HistoricalDataset:
    name: str
    instrument: str
    snapshots: List[MarketSnapshot]
    source_path: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WalkForwardAggregate:
    folds: int = 0
    avg_validation_pnl: float = 0.0
    total_validation_pnl: float = 0.0
    avg_validation_drawdown: float = 0.0
    avg_validation_profit_factor: float = 0.0
    avg_validation_win_rate: float = 0.0
    avg_validation_fdr: float = 0.0
    positive_fold_ratio: float = 0.0
    validation_round_trips: int = 0
    profitable_folds: int = 0
    regime_counts: Dict[str, int] = field(default_factory=dict)
    profitable_regime_counts: Dict[str, int] = field(default_factory=dict)
    supported_regimes: List[str] = field(default_factory=list)
    dominant_regime: str = ""
    regime_breadth: int = 0


@dataclass
class StrategyScorecard:
    strategy_id: str
    score: float
    enabled: bool
    avg_validation_pnl: float
    avg_validation_drawdown: float
    avg_validation_profit_factor: float
    avg_validation_win_rate: float
    avg_validation_fdr: float
    positive_fold_ratio: float
    validation_round_trips: int
    dominant_regime: str = ""
    supported_regimes: List[str] = field(default_factory=list)
    regime_breadth: int = 0
    regime_alignment_score: float = 0.0
    reasons: List[str] = field(default_factory=list)


@dataclass
class AllocationDecision:
    strategy_id: str
    enabled: bool
    capital_usd: float
    weight: float
    score: float
    reasons: List[str] = field(default_factory=list)


class HistoricalDatasetLoader:
    """Load historical market snapshots from JSON, JSONL, or CSV."""

    def load(
        self,
        path: str,
        instrument: Optional[str] = None,
        name: Optional[str] = None,
    ) -> HistoricalDataset:
        source = Path(path)
        suffix = source.suffix.lower()

        if suffix == ".csv":
            records = self._load_csv(source)
        elif suffix in (".jsonl", ".ndjson"):
            records = self._load_jsonl(source)
        elif suffix == ".json":
            records = self._load_json(source)
        else:
            raise ValueError(f"Unsupported dataset format: {source.suffix}")

        snapshots = [
            self._to_snapshot(record, instrument=instrument)
            for record in records
            if self._record_matches_instrument(record, instrument)
        ]
        snapshots.sort(key=lambda s: s.timestamp_ms)

        dataset_instrument = instrument or (snapshots[0].instrument if snapshots else "")
        return HistoricalDataset(
            name=name or source.stem,
            instrument=dataset_instrument,
            snapshots=snapshots,
            source_path=str(source),
            metadata={"rows": len(snapshots), "format": suffix.lstrip(".")},
        )

    @staticmethod
    def _load_csv(path: Path) -> List[Dict[str, Any]]:
        with path.open(newline="") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
        records = []
        with path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    @staticmethod
    def _load_json(path: Path) -> List[Dict[str, Any]]:
        with path.open() as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if "snapshots" in data and isinstance(data["snapshots"], list):
                return data["snapshots"]
            raise ValueError("JSON dataset must be a list or contain a 'snapshots' list")
        raise ValueError("Unsupported JSON dataset structure")

    def _to_snapshot(self, record: Dict[str, Any], instrument: Optional[str] = None) -> MarketSnapshot:
        normalized = {}
        for key, value in record.items():
            canonical = _ALIASES.get(key, key)
            normalized[canonical] = value

        resolved_instrument = instrument or normalized.get("instrument", "ETH-PERP")
        return MarketSnapshot(
            instrument=str(resolved_instrument),
            mid_price=float(normalized.get("mid_price", 0) or 0),
            bid=float(normalized.get("bid", normalized.get("mid_price", 0)) or 0),
            ask=float(normalized.get("ask", normalized.get("mid_price", 0)) or 0),
            spread_bps=float(normalized.get("spread_bps", 0) or 0),
            timestamp_ms=int(float(normalized.get("timestamp_ms", 0) or 0)),
            volume_24h=float(normalized.get("volume_24h", 0) or 0),
            funding_rate=float(normalized.get("funding_rate", 0) or 0),
            open_interest=float(normalized.get("open_interest", 0) or 0),
        )

    @staticmethod
    def _record_matches_instrument(record: Dict[str, Any], instrument: Optional[str]) -> bool:
        if not instrument:
            return True
        return str(record.get("instrument", instrument)) == instrument


def aggregate_walk_forward_results(results: Sequence[WalkForwardFoldResult]) -> WalkForwardAggregate:
    """Aggregate validation metrics across walk-forward folds."""
    if not results:
        return WalkForwardAggregate()

    folds = len(results)
    total_pnl = sum(r.validation.total_pnl for r in results)
    total_drawdown = sum(r.validation.max_drawdown for r in results)
    total_pf = 0.0
    total_wr = 0.0
    total_fdr = 0.0
    positive = 0
    total_round_trips = 0
    regime_counts: Dict[str, int] = {}
    profitable_regime_counts: Dict[str, int] = {}

    for result in results:
        metrics = result.validation.metrics
        total_pf += _finite_profit_factor(metrics.net_profit_factor)
        total_wr += metrics.win_rate
        total_fdr += metrics.fdr
        total_round_trips += metrics.total_round_trips
        regime = result.validation_regime or "unknown"
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        if result.validation.total_pnl > 0:
            positive += 1
            profitable_regime_counts[regime] = profitable_regime_counts.get(regime, 0) + 1

    supported_regimes = summarize_regime_support(regime_counts, profitable_regime_counts)
    dominant_regime = max(
        regime_counts.items(),
        key=lambda item: (item[1], profitable_regime_counts.get(item[0], 0), item[0]),
    )[0]

    return WalkForwardAggregate(
        folds=folds,
        avg_validation_pnl=total_pnl / folds,
        total_validation_pnl=total_pnl,
        avg_validation_drawdown=total_drawdown / folds,
        avg_validation_profit_factor=total_pf / folds,
        avg_validation_win_rate=total_wr / folds,
        avg_validation_fdr=total_fdr / folds,
        positive_fold_ratio=positive / folds,
        validation_round_trips=total_round_trips,
        profitable_folds=positive,
        regime_counts=regime_counts,
        profitable_regime_counts=profitable_regime_counts,
        supported_regimes=supported_regimes,
        dominant_regime=dominant_regime,
        regime_breadth=len(supported_regimes),
    )


class StrategyScorer:
    """Score walk-forward validation results for capital allocation."""

    def __init__(
        self,
        min_round_trips: int = 1,
        min_positive_fold_ratio: float = 0.5,
        max_fdr: float = 35.0,
    ):
        self.min_round_trips = min_round_trips
        self.min_positive_fold_ratio = min_positive_fold_ratio
        self.max_fdr = max_fdr

    def score(self, strategy_id: str, aggregate: WalkForwardAggregate) -> StrategyScorecard:
        reasons: List[str] = []
        enabled = True

        if aggregate.validation_round_trips < self.min_round_trips:
            enabled = False
            reasons.append("insufficient_round_trips")
        if aggregate.avg_validation_pnl <= 0:
            enabled = False
            reasons.append("non_positive_validation_pnl")
        if aggregate.positive_fold_ratio < self.min_positive_fold_ratio:
            enabled = False
            reasons.append("low_fold_consistency")
        if aggregate.avg_validation_fdr > self.max_fdr:
            enabled = False
            reasons.append("fee_drag_too_high")
        if aggregate.profitable_folds > 0 and aggregate.regime_breadth == 0:
            enabled = False
            reasons.append("no_supported_regime")
        if aggregate.profitable_folds > 1 and aggregate.regime_breadth == 1 and aggregate.positive_fold_ratio < 0.67:
            enabled = False
            reasons.append("narrow_regime_edge")

        drawdown_denom = max(aggregate.avg_validation_drawdown, 1.0)
        risk_adjusted = max(aggregate.avg_validation_pnl, 0.0) / drawdown_denom
        regime_alignment = min(float(aggregate.regime_breadth), 3.0) / 3.0
        score = (
            risk_adjusted * 40.0
            + min(aggregate.avg_validation_profit_factor, 3.0) * 20.0
            + aggregate.positive_fold_ratio * 25.0
            + (aggregate.avg_validation_win_rate / 100.0) * 15.0
        )
        score += regime_alignment * 10.0
        score -= max(aggregate.avg_validation_fdr - 20.0, 0.0) * 0.5
        score = max(score, 0.0)

        if enabled:
            reasons.append("eligible_for_allocation")

        return StrategyScorecard(
            strategy_id=strategy_id,
            score=round(score, 2),
            enabled=enabled,
            avg_validation_pnl=round(aggregate.avg_validation_pnl, 4),
            avg_validation_drawdown=round(aggregate.avg_validation_drawdown, 4),
            avg_validation_profit_factor=round(aggregate.avg_validation_profit_factor, 4),
            avg_validation_win_rate=round(aggregate.avg_validation_win_rate, 2),
            avg_validation_fdr=round(aggregate.avg_validation_fdr, 2),
            positive_fold_ratio=round(aggregate.positive_fold_ratio, 4),
            validation_round_trips=aggregate.validation_round_trips,
            dominant_regime=aggregate.dominant_regime,
            supported_regimes=list(aggregate.supported_regimes),
            regime_breadth=aggregate.regime_breadth,
            regime_alignment_score=round(regime_alignment, 4),
            reasons=reasons,
        )


class StrategyCapitalAllocator:
    """Allocate capital across scored strategies."""

    def __init__(
        self,
        total_capital_usd: float,
        reserve_pct: float = 0.1,
        max_strategy_pct: float = 0.5,
    ):
        self.total_capital_usd = total_capital_usd
        self.reserve_pct = reserve_pct
        self.max_strategy_pct = max_strategy_pct

    def allocate(self, scorecards: Sequence[StrategyScorecard]) -> List[AllocationDecision]:
        pool = self.total_capital_usd * max(0.0, 1.0 - self.reserve_pct)
        enabled = [s for s in scorecards if s.enabled and s.score > 0]
        disabled = [s for s in scorecards if s not in enabled]

        allocations: Dict[str, float] = {s.strategy_id: 0.0 for s in scorecards}
        if enabled and pool > 0:
            allocations.update(self._capped_proportional_allocations(enabled, pool))

        decisions = [
            AllocationDecision(
                strategy_id=s.strategy_id,
                enabled=s.enabled and allocations.get(s.strategy_id, 0.0) > 0,
                capital_usd=round(allocations.get(s.strategy_id, 0.0), 2),
                weight=round((allocations.get(s.strategy_id, 0.0) / pool) if pool > 0 else 0.0, 4),
                score=s.score,
                reasons=list(s.reasons),
            )
            for s in scorecards
        ]

        for decision in decisions:
            if decision.strategy_id in {s.strategy_id for s in disabled}:
                decision.reasons = [r for r in decision.reasons if r != "eligible_for_allocation"]
        decisions.sort(key=lambda d: (-d.capital_usd, -d.score, d.strategy_id))
        return decisions

    def _capped_proportional_allocations(
        self,
        scorecards: Sequence[StrategyScorecard],
        pool: float,
    ) -> Dict[str, float]:
        remaining = {s.strategy_id: s.score for s in scorecards}
        allocations = {s.strategy_id: 0.0 for s in scorecards}
        cap = pool * self.max_strategy_pct
        remaining_pool = pool

        while remaining and remaining_pool > 1e-9:
            total_score = sum(remaining.values())
            if total_score <= 0:
                break

            capped: List[str] = []
            for strategy_id, score in list(remaining.items()):
                proposed = remaining_pool * (score / total_score)
                available_cap = cap - allocations[strategy_id]
                take = min(proposed, max(available_cap, 0.0))
                allocations[strategy_id] += take
                if allocations[strategy_id] >= cap - 1e-9:
                    capped.append(strategy_id)

            allocated_total = sum(allocations.values())
            remaining_pool = max(pool - allocated_total, 0.0)

            if not capped:
                break
            for strategy_id in capped:
                remaining.pop(strategy_id, None)

        return allocations


def _finite_profit_factor(value: float) -> float:
    if math.isinf(value):
        return 3.0
    if math.isnan(value):
        return 0.0
    return max(value, 0.0)
