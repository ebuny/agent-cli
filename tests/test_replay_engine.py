"""Tests for replay and walk-forward evaluation."""
from __future__ import annotations

from common.models import MarketSnapshot, StrategyDecision
from modules.replay_engine import (
    StrategyReplayEngine,
    WalkForwardEvaluator,
    build_walk_forward_folds,
)
from sdk.strategy_sdk.base import BaseStrategy, StrategyContext


def _snap(mid: float, ts: int) -> MarketSnapshot:
    return MarketSnapshot(
        instrument="ETH-PERP",
        mid_price=mid,
        bid=mid - 1,
        ask=mid + 1,
        timestamp_ms=ts,
    )


class BuyThenSellStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(strategy_id="replay_test")

    def on_tick(self, snapshot: MarketSnapshot, context: StrategyContext | None = None):
        round_no = context.round_number if context else 0
        if round_no == 1 and (context.position_qty if context else 0) == 0:
            return [StrategyDecision(
                action="place_order",
                instrument=snapshot.instrument,
                side="buy",
                size=1.0,
                limit_price=snapshot.ask,
                meta={"signal": "entry"},
            )]
        if round_no == 3 and (context.position_qty if context else 0) > 0:
            return [StrategyDecision(
                action="place_order",
                instrument=snapshot.instrument,
                side="sell",
                size=1.0,
                limit_price=snapshot.bid,
                meta={"signal": "exit"},
            )]
        return []


def test_strategy_replay_generates_positive_pnl():
    engine = StrategyReplayEngine(BuyThenSellStrategy())
    result = engine.run([
        _snap(100, 1),
        _snap(103, 2),
        _snap(106, 3),
    ])

    assert len(result.trades) == 2
    assert result.realized_pnl > 0
    assert result.metrics.total_round_trips == 1
    assert result.metrics.win_rate == 100.0


def test_walk_forward_fold_builder():
    snapshots = [_snap(100 + i, i) for i in range(12)]
    folds = build_walk_forward_folds(snapshots, train_size=4, validation_size=2, step_size=2)

    assert len(folds) == 4
    assert folds[0].train_start == 0
    assert folds[0].validation_start == 4
    assert folds[-1].validation_end == 12


def test_walk_forward_evaluator_runs_folds():
    snapshots = [_snap(100 + i * 2, i) for i in range(12)]
    evaluator = WalkForwardEvaluator(BuyThenSellStrategy)
    results = evaluator.evaluate(snapshots, train_size=4, validation_size=3, step_size=3)

    assert len(results) == 2
    assert all(r.train.ticks == 4 for r in results)
    assert all(r.validation.ticks == 3 for r in results)
    assert all(r.validation_regime for r in results)
