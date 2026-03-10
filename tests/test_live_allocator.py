"""Tests for regime-aware live allocation planning."""
from __future__ import annotations

import json

from modules.live_allocator import LiveAllocator


def test_live_allocator_filters_snapshot_by_current_regime(tmp_path):
    snapshot_path = tmp_path / "latest_allocation.json"
    snapshot_path.write_text(json.dumps({
        "summary": {"deployable_capital_usd": 9000.0},
        "routing_policy": {
            "generated_at": "2026-03-09T00:00:00+00:00",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up", "volatile"],
                    "allowed_instruments": ["ETH-PERP"],
                    "min_signal_score": 205.0,
                    "capital_usd": 3000.0,
                    "weight": 0.33,
                    "score": 98.0,
                    "reasons": ["eligible_for_allocation"],
                }
            ],
        },
        "strategies": [
            {
                "strategy_id": "basis_arb",
                "scorecard": {
                    "strategy_id": "basis_arb",
                    "score": 120.0,
                    "enabled": True,
                    "avg_validation_pnl": 100.0,
                    "avg_validation_drawdown": 20.0,
                    "avg_validation_profit_factor": 1.5,
                    "avg_validation_win_rate": 55.0,
                    "avg_validation_fdr": 10.0,
                    "positive_fold_ratio": 0.7,
                    "validation_round_trips": 6,
                    "dominant_regime": "carry",
                    "supported_regimes": ["carry"],
                    "regime_breadth": 1,
                    "regime_alignment_score": 0.33,
                    "reasons": ["eligible_for_allocation"],
                },
                "allocation": {
                    "strategy_id": "basis_arb",
                    "enabled": True,
                    "capital_usd": 4000.0,
                    "weight": 0.44,
                    "score": 120.0,
                    "reasons": ["eligible_for_allocation"],
                },
            },
            {
                "strategy_id": "momentum_breakout",
                "scorecard": {
                    "strategy_id": "momentum_breakout",
                    "score": 98.0,
                    "enabled": True,
                    "avg_validation_pnl": 80.0,
                    "avg_validation_drawdown": 25.0,
                    "avg_validation_profit_factor": 1.4,
                    "avg_validation_win_rate": 52.0,
                    "avg_validation_fdr": 11.0,
                    "positive_fold_ratio": 0.6,
                    "validation_round_trips": 5,
                    "dominant_regime": "trend_up",
                    "supported_regimes": ["trend_up", "volatile"],
                    "regime_breadth": 2,
                    "regime_alignment_score": 0.66,
                    "reasons": ["eligible_for_allocation"],
                },
                "allocation": {
                    "strategy_id": "momentum_breakout",
                    "enabled": True,
                    "capital_usd": 3000.0,
                    "weight": 0.33,
                    "score": 98.0,
                    "reasons": ["eligible_for_allocation"],
                },
            },
        ],
    }))

    allocator = LiveAllocator()
    plan = allocator.plan(
        snapshot_path=str(snapshot_path),
        regime_override="carry",
    )

    assert plan.current_regime.label == "carry"
    assert plan.allocated_capital_usd == 4000.0
    enabled = [item for item in plan.decisions if item.enabled]
    assert [item.strategy_id for item in enabled] == ["basis_arb"]
    assert plan.routing_policy.rules == []
    blocked = next(item for item in plan.decisions if item.strategy_id == "momentum_breakout")
    assert "regime_mismatch:carry" in blocked.reasons


def test_live_allocator_preserves_thresholds_for_matching_regime(tmp_path):
    snapshot_path = tmp_path / "latest_allocation.json"
    snapshot_path.write_text(json.dumps({
        "summary": {"deployable_capital_usd": 9000.0},
        "routing_policy": {
            "generated_at": "2026-03-09T00:00:00+00:00",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["SOL-PERP"],
                    "min_signal_score": 215.0,
                    "capital_usd": 3000.0,
                    "weight": 1.0,
                    "score": 98.0,
                    "reasons": ["eligible_for_allocation"],
                }
            ],
        },
        "strategies": [
            {
                "strategy_id": "momentum_breakout",
                "scorecard": {
                    "strategy_id": "momentum_breakout",
                    "score": 98.0,
                    "enabled": True,
                    "avg_validation_pnl": 80.0,
                    "avg_validation_drawdown": 25.0,
                    "avg_validation_profit_factor": 1.4,
                    "avg_validation_win_rate": 52.0,
                    "avg_validation_fdr": 11.0,
                    "positive_fold_ratio": 0.6,
                    "validation_round_trips": 5,
                    "dominant_regime": "trend_up",
                    "supported_regimes": ["trend_up"],
                    "regime_breadth": 1,
                    "regime_alignment_score": 0.33,
                    "reasons": ["eligible_for_allocation"],
                },
                "allocation": {
                    "strategy_id": "momentum_breakout",
                    "enabled": True,
                    "capital_usd": 3000.0,
                    "weight": 1.0,
                    "score": 98.0,
                    "reasons": ["eligible_for_allocation"],
                },
            },
        ],
    }))

    plan = LiveAllocator().plan(
        snapshot_path=str(snapshot_path),
        regime_override="trend_up",
    )

    assert len(plan.routing_policy.rules) == 1
    assert plan.routing_policy.rules[0].min_signal_score == 215.0


def test_live_allocator_applies_feedback_blocking_sources(tmp_path):
    snapshot_path = tmp_path / "latest_allocation.json"
    snapshot_path.write_text(json.dumps({
        "summary": {"deployable_capital_usd": 5000.0},
        "routing_policy": {
            "generated_at": "2026-03-09T00:00:00+00:00",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["ETH-PERP"],
                    "min_signal_score": 200.0,
                    "capital_usd": 3000.0,
                    "weight": 0.6,
                    "score": 90.0,
                    "reasons": ["eligible_for_allocation"],
                },
                {
                    "strategy_id": "liquidation_mm",
                    "source": "movers_immediate",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["SOL-PERP"],
                    "min_signal_score": 0.0,
                    "capital_usd": 2000.0,
                    "weight": 0.4,
                    "score": 88.0,
                    "reasons": ["eligible_for_allocation"],
                },
            ],
        },
        "strategies": [
            {
                "strategy_id": "momentum_breakout",
                "scorecard": {
                    "strategy_id": "momentum_breakout",
                    "score": 90.0,
                    "enabled": True,
                    "avg_validation_pnl": 80.0,
                    "avg_validation_drawdown": 25.0,
                    "avg_validation_profit_factor": 1.4,
                    "avg_validation_win_rate": 52.0,
                    "avg_validation_fdr": 11.0,
                    "positive_fold_ratio": 0.6,
                    "validation_round_trips": 5,
                    "dominant_regime": "trend_up",
                    "supported_regimes": ["trend_up"],
                    "regime_breadth": 1,
                    "regime_alignment_score": 0.33,
                    "reasons": ["eligible_for_allocation"],
                },
                "allocation": {
                    "strategy_id": "momentum_breakout",
                    "enabled": True,
                    "capital_usd": 3000.0,
                    "weight": 0.6,
                    "score": 90.0,
                    "reasons": ["eligible_for_allocation"],
                },
            },
            {
                "strategy_id": "liquidation_mm",
                "scorecard": {
                    "strategy_id": "liquidation_mm",
                    "score": 88.0,
                    "enabled": True,
                    "avg_validation_pnl": 50.0,
                    "avg_validation_drawdown": 20.0,
                    "avg_validation_profit_factor": 1.3,
                    "avg_validation_win_rate": 50.0,
                    "avg_validation_fdr": 12.0,
                    "positive_fold_ratio": 0.5,
                    "validation_round_trips": 4,
                    "dominant_regime": "trend_up",
                    "supported_regimes": ["trend_up"],
                    "regime_breadth": 1,
                    "regime_alignment_score": 0.4,
                    "reasons": ["eligible_for_allocation"],
                },
                "allocation": {
                    "strategy_id": "liquidation_mm",
                    "enabled": True,
                    "capital_usd": 2000.0,
                    "weight": 0.4,
                    "score": 88.0,
                    "reasons": ["eligible_for_allocation"],
                },
            },
        ],
    }))

    judge_path = tmp_path / "judge.json"
    judge_path.write_text(json.dumps({
        "timestamp_ms": 0,
        "round_trips_evaluated": 12,
        "false_positive_rates": {"scanner": 65.0},
        "findings": [],
    }))

    plan = LiveAllocator().plan(
        snapshot_path=str(snapshot_path),
        regime_override="trend_up",
        feedback_enable=True,
        feedback_trades_path=None,
        feedback_judge_path=str(judge_path),
    )

    blocked = next(item for item in plan.decisions if item.strategy_id == "momentum_breakout")
    assert blocked.enabled is False
    assert "feedback_blocked_sources:scanner" in blocked.reasons
    assert plan.routing_policy.rules[0].source == "movers_immediate"
