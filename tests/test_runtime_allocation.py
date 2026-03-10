"""Tests for runtime allocation gating and source routing."""
from __future__ import annotations

import json

from modules.runtime_allocation import RuntimeAllocationLoader


def test_runtime_allocation_loader_derives_allowed_sources(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "deployable_capital_usd": 9000.0,
        "allocated_capital_usd": 4000.0,
        "enabled_strategies": 2,
        "routing_policy": {
            "generated_at": "2026-03-09T00:00:00+00:00",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["ETH-PERP"],
                    "min_signal_score": 205.0,
                    "capital_usd": 2500.0,
                    "weight": 0.625,
                    "score": 100.0,
                    "reasons": ["eligible_for_allocation"],
                },
                {
                    "strategy_id": "momentum_breakout",
                    "source": "movers_signal",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["ETH-PERP"],
                    "min_signal_score": 80.0,
                    "capital_usd": 2500.0,
                    "weight": 0.625,
                    "score": 100.0,
                    "reasons": ["eligible_for_allocation"],
                },
                {
                    "strategy_id": "liquidation_mm",
                    "source": "movers_immediate",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["BTC-PERP"],
                    "min_signal_score": 100.0,
                    "capital_usd": 1500.0,
                    "weight": 0.375,
                    "score": 80.0,
                    "reasons": ["eligible_for_allocation"],
                },
            ],
        },
        "decisions": [
            {
                "strategy_id": "momentum_breakout",
                "enabled": True,
                "capital_usd": 2500.0,
            },
            {
                "strategy_id": "liquidation_mm",
                "enabled": True,
                "capital_usd": 1500.0,
            },
        ],
    }))

    gate = RuntimeAllocationLoader().load(
        plan_path=str(plan_path),
        total_budget_usd=10_000.0,
        max_slots=2,
    )

    assert gate.allow_entries
    assert gate.allowed_entry_sources == ["movers_immediate", "movers_signal", "scanner"]
    assert any(rule["min_signal_score"] == 205.0 for rule in gate.routing_rules if rule["source"] == "scanner")


def test_runtime_allocation_loader_blocks_non_wolf_compatible_edges(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "carry"},
        "deployable_capital_usd": 9000.0,
        "allocated_capital_usd": 4000.0,
        "enabled_strategies": 1,
        "routing_policy": {
            "generated_at": "2026-03-09T00:00:00+00:00",
            "rules": [],
        },
        "decisions": [
            {
                "strategy_id": "basis_arb",
                "enabled": True,
                "capital_usd": 4000.0,
            }
        ],
    }))

    gate = RuntimeAllocationLoader().load(
        plan_path=str(plan_path),
        total_budget_usd=10_000.0,
        max_slots=2,
    )

    assert not gate.allow_entries
    assert gate.allowed_entry_sources == []
    assert "no_wolf_compatible_edge" in gate.reasons
