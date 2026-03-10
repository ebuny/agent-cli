"""Tests for historical dataset loading and allocation pipeline."""
from __future__ import annotations

import json

from modules.replay_engine import WalkForwardFold, WalkForwardFoldResult, ReplayResult
from modules.research_pipeline import (
    HistoricalDatasetLoader,
    StrategyCapitalAllocator,
    StrategyScorer,
    aggregate_walk_forward_results,
)
from modules.howl_engine import HowlMetrics


def test_load_jsonl_dataset(tmp_path):
    path = tmp_path / "eth.jsonl"
    rows = [
        {"instrument": "ETH-PERP", "timestamp_ms": 2, "mid_price": 101, "bid": 100, "ask": 102},
        {"instrument": "ETH-PERP", "timestamp_ms": 1, "mid_price": 100, "bid": 99, "ask": 101},
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows))

    dataset = HistoricalDatasetLoader().load(str(path))

    assert dataset.instrument == "ETH-PERP"
    assert len(dataset.snapshots) == 2
    assert dataset.snapshots[0].timestamp_ms == 1


def test_load_csv_dataset_with_aliases(tmp_path):
    path = tmp_path / "sol.csv"
    path.write_text(
        "instrument,ts,mid,bid,ask,volume,funding,oi\n"
        "SOL-PERP,1,100,99,101,1000,0.0001,5000\n"
    )

    dataset = HistoricalDatasetLoader().load(str(path))

    snap = dataset.snapshots[0]
    assert snap.instrument == "SOL-PERP"
    assert snap.mid_price == 100.0
    assert snap.volume_24h == 1000.0
    assert snap.open_interest == 5000.0


def _fold_result(pnl: float, drawdown: float, win_rate: float, pf: float, fdr: float, round_trips: int):
    metrics = HowlMetrics(
        total_round_trips=round_trips,
        win_rate=win_rate,
        net_profit_factor=pf,
        fdr=fdr,
    )
    replay = ReplayResult(
        ticks=10,
        metrics=metrics,
        total_pnl=pnl,
        max_drawdown=drawdown,
    )
    return WalkForwardFoldResult(
        fold=WalkForwardFold(fold=1, train_start=0, train_end=5, validation_start=5, validation_end=10),
        train=replay,
        validation=replay,
        validation_regime="trend_up" if pnl >= 0 else "trend_down",
    )


def test_aggregate_walk_forward_results():
    results = [
        _fold_result(100.0, 20.0, 60.0, 1.5, 10.0, 3),
        _fold_result(-20.0, 30.0, 40.0, 0.8, 15.0, 2),
    ]
    agg = aggregate_walk_forward_results(results)

    assert agg.folds == 2
    assert agg.avg_validation_pnl == 40.0
    assert agg.positive_fold_ratio == 0.5
    assert agg.validation_round_trips == 5
    assert agg.dominant_regime in {"trend_up", "trend_down"}
    assert "trend_up" in agg.regime_counts


def test_strategy_scorer_filters_unprofitable_edges():
    agg = aggregate_walk_forward_results([
        _fold_result(-10.0, 20.0, 30.0, 0.7, 12.0, 3),
        _fold_result(5.0, 40.0, 45.0, 0.9, 18.0, 2),
    ])
    scorecard = StrategyScorer(min_round_trips=3).score("scanner_edge", agg)

    assert not scorecard.enabled
    assert "non_positive_validation_pnl" in scorecard.reasons


def test_strategy_scorer_exposes_supported_regimes():
    agg = aggregate_walk_forward_results([
        _fold_result(120.0, 20.0, 60.0, 1.8, 10.0, 3),
        _fold_result(90.0, 18.0, 55.0, 1.6, 12.0, 2),
    ])
    scorecard = StrategyScorer().score("carry_edge", agg)

    assert scorecard.enabled
    assert scorecard.supported_regimes == ["trend_up"]
    assert scorecard.regime_breadth == 1


def test_capital_allocator_respects_caps():
    scorer = StrategyScorer()
    strong = scorer.score("strong", aggregate_walk_forward_results([
        _fold_result(120.0, 20.0, 60.0, 1.8, 10.0, 3),
        _fold_result(110.0, 15.0, 55.0, 1.6, 12.0, 2),
    ]))
    medium = scorer.score("medium", aggregate_walk_forward_results([
        _fold_result(60.0, 20.0, 55.0, 1.4, 14.0, 3),
        _fold_result(50.0, 18.0, 52.0, 1.3, 16.0, 2),
    ]))
    weak = scorer.score("weak", aggregate_walk_forward_results([
        _fold_result(-5.0, 20.0, 40.0, 0.8, 10.0, 1),
        _fold_result(-1.0, 10.0, 45.0, 0.9, 12.0, 1),
    ]))

    decisions = StrategyCapitalAllocator(
        total_capital_usd=10000,
        reserve_pct=0.1,
        max_strategy_pct=0.5,
    ).allocate([strong, medium, weak])

    assert decisions[0].capital_usd <= 4500.0
    assert sum(d.capital_usd for d in decisions) <= 9000.0
    weak_decision = next(d for d in decisions if d.strategy_id == "weak")
    assert weak_decision.capital_usd == 0.0
