"""Tests for single-strategy allocation gating."""
from __future__ import annotations

import json

from modules.runtime_strategy_gate import RuntimeStrategyGateLoader


def test_runtime_strategy_gate_blocks_missing_plan(tmp_path):
    loader = RuntimeStrategyGateLoader()
    gate = loader.load(
        plan_path=str(tmp_path / "missing.json"),
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
    )
    assert gate.configured
    assert not gate.allow_entries
    assert "allocation_plan_missing" in gate.reasons


def test_runtime_strategy_gate_blocks_disabled_strategy(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "decisions": [
            {"strategy_id": "mean_reversion", "enabled": False, "capital_usd": 0.0},
        ],
        "routing_policy": {"rules": []},
    }))
    gate = RuntimeStrategyGateLoader().load(
        plan_path=str(plan_path),
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
    )
    assert gate.configured
    assert not gate.allow_entries
    assert "strategy_not_enabled" in gate.reasons


def test_runtime_strategy_gate_blocks_instrument_outside_policy(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "decisions": [
            {"strategy_id": "momentum_breakout", "enabled": True, "capital_usd": 1000.0},
        ],
        "routing_policy": {
            "rules": [
                {"strategy_id": "momentum_breakout", "allowed_instruments": ["SOL-PERP"]},
            ],
        },
    }))
    gate = RuntimeStrategyGateLoader().load(
        plan_path=str(plan_path),
        strategy_id="momentum_breakout",
        instrument="ETH-PERP",
    )
    assert gate.configured
    assert not gate.allow_entries
    assert "instrument_not_allowed" in gate.reasons


def test_runtime_strategy_gate_allows_enabled_strategy(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "decisions": [
            {"strategy_id": "mean_reversion", "enabled": True, "capital_usd": 2500.0},
        ],
        "routing_policy": {"rules": []},
    }))
    gate = RuntimeStrategyGateLoader().load(
        plan_path=str(plan_path),
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
    )
    assert gate.configured
    assert gate.allow_entries
