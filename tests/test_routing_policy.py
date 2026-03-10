"""Tests for research-driven runtime routing policy generation."""
from __future__ import annotations

from modules.research_pipeline import StrategyScorecard
from modules.research_runner import DatasetResearchResult, StrategyResearchResult
from modules.routing_policy import ResearchRoutingPolicyBuilder


def test_routing_policy_builder_emits_rules_for_enabled_directional_strategy():
    strategy_result = StrategyResearchResult(
        strategy_id="momentum_breakout",
        strategy_path="strategies.momentum_breakout:MomentumBreakoutStrategy",
        datasets=["btc", "eth"],
        dataset_breakdown=[
            DatasetResearchResult(
                dataset="btc",
                instrument="BTC-PERP",
                folds=2,
                scorecard=StrategyScorecard(
                    strategy_id="momentum_breakout@btc",
                    score=120.0,
                    enabled=True,
                    avg_validation_pnl=100.0,
                    avg_validation_drawdown=20.0,
                    avg_validation_profit_factor=1.5,
                    avg_validation_win_rate=55.0,
                    avg_validation_fdr=10.0,
                    positive_fold_ratio=0.75,
                    validation_round_trips=4,
                    dominant_regime="trend_up",
                    supported_regimes=["trend_up"],
                    regime_breadth=1,
                    regime_alignment_score=0.33,
                    reasons=["eligible_for_allocation"],
                ),
            ),
            DatasetResearchResult(
                dataset="eth",
                instrument="ETH-PERP",
                folds=2,
                scorecard=StrategyScorecard(
                    strategy_id="momentum_breakout@eth",
                    score=0.0,
                    enabled=False,
                    avg_validation_pnl=-10.0,
                    avg_validation_drawdown=20.0,
                    avg_validation_profit_factor=0.5,
                    avg_validation_win_rate=40.0,
                    avg_validation_fdr=10.0,
                    positive_fold_ratio=0.25,
                    validation_round_trips=2,
                    dominant_regime="trend_down",
                    supported_regimes=[],
                    regime_breadth=0,
                    regime_alignment_score=0.0,
                    reasons=["non_positive_validation_pnl"],
                ),
            ),
        ],
        scorecard=StrategyScorecard(
            strategy_id="momentum_breakout",
            score=120.0,
            enabled=True,
            avg_validation_pnl=100.0,
            avg_validation_drawdown=20.0,
            avg_validation_profit_factor=1.5,
            avg_validation_win_rate=55.0,
            avg_validation_fdr=10.0,
            positive_fold_ratio=0.75,
            validation_round_trips=6,
            dominant_regime="trend_up",
            supported_regimes=["trend_up"],
            regime_breadth=1,
            regime_alignment_score=0.33,
            reasons=["eligible_for_allocation"],
        ),
    )

    policy = ResearchRoutingPolicyBuilder().build(
        generated_at="2026-03-09T00:00:00+00:00",
        strategy_results=[strategy_result],
        allocations=[{
            "strategy_id": "momentum_breakout",
            "enabled": True,
            "capital_usd": 3000.0,
            "weight": 1.0,
            "score": 120.0,
            "reasons": ["eligible_for_allocation"],
        }],
        edge_report={
            "thresholds": [
                {
                    "source": "scanner",
                    "instrument": "BTC-PERP",
                    "min_signal_score": 210.0,
                },
                {
                    "source": "movers_signal",
                    "instrument": "*",
                    "min_signal_score": 82.0,
                },
            ],
        },
    )

    assert len(policy.rules) == 2
    assert {rule.source for rule in policy.rules} == {"scanner", "movers_signal"}
    assert all(rule.allowed_instruments == ["BTC-PERP"] for rule in policy.rules)
    assert all(rule.allowed_regimes == ["trend_up"] for rule in policy.rules)
    scanner_rule = next(rule for rule in policy.rules if rule.source == "scanner")
    movers_rule = next(rule for rule in policy.rules if rule.source == "movers_signal")
    assert scanner_rule.min_signal_score == 210.0
    assert movers_rule.min_signal_score == 82.0
