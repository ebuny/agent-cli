"""Tests for portfolio-level allocator."""
from __future__ import annotations

import json
import time

from modules.portfolio_allocator import PortfolioAllocator


def _write_plan(path, total_cap=1000.0, decisions=None):
    payload = {
        "allocated_capital_usd": total_cap,
        "decisions": decisions or [],
    }
    path.write_text(json.dumps(payload))


def test_portfolio_allocator_blocks_missing_plan(tmp_path):
    allocator = PortfolioAllocator(state_db_path=str(tmp_path / "state.db"), ttl_ms=1000)
    gate = allocator.refresh(
        plan_path=str(tmp_path / "missing.json"),
        runner_id="r1",
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
        requested_capital_usd=500.0,
    )
    assert gate.configured
    assert not gate.allow_entries
    assert "portfolio_plan_missing" in gate.reasons


def test_portfolio_allocator_respects_total_and_strategy_caps(tmp_path):
    plan_path = tmp_path / "plan.json"
    _write_plan(plan_path, total_cap=1000.0, decisions=[
        {"strategy_id": "mean_reversion", "capital_usd": 600.0},
        {"strategy_id": "momentum_breakout", "capital_usd": 400.0},
    ])

    allocator = PortfolioAllocator(state_db_path=str(tmp_path / "state.db"), ttl_ms=10_000)
    gate_a = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="r1",
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
        requested_capital_usd=500.0,
    )
    gate_b = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="r2",
        strategy_id="momentum_breakout",
        instrument="SOL-PERP",
        requested_capital_usd=500.0,
    )
    gate_c = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="r3",
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
        requested_capital_usd=500.0,
    )

    assert gate_a.allow_entries
    assert gate_a.approved_capital_usd == 500.0
    assert gate_b.allow_entries
    assert gate_b.approved_capital_usd == 400.0
    assert gate_c.allow_entries
    assert gate_c.approved_capital_usd == 100.0


def test_portfolio_allocator_prunes_stale_allocations(tmp_path):
    plan_path = tmp_path / "plan.json"
    _write_plan(plan_path, total_cap=500.0, decisions=[
        {"strategy_id": "mean_reversion", "capital_usd": 500.0},
    ])

    allocator = PortfolioAllocator(state_db_path=str(tmp_path / "state.db"), ttl_ms=10_000)
    gate = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="r1",
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
        requested_capital_usd=400.0,
    )
    assert gate.allow_entries
    # Force the existing allocation to look stale.
    state_db = allocator.state_db
    stored = state_db.get("portfolio_allocations")
    stored["r1"]["updated_at_ms"] = int(time.time() * 1000) - 20_000
    state_db.put("portfolio_allocations", stored)
    gate2 = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="r2",
        strategy_id="mean_reversion",
        instrument="ETH-PERP",
        requested_capital_usd=500.0,
    )
    assert gate2.allow_entries
    assert gate2.approved_capital_usd == 500.0


def test_portfolio_allocator_allows_global_strategy_id(tmp_path):
    plan_path = tmp_path / "plan.json"
    _write_plan(plan_path, total_cap=800.0, decisions=[])

    allocator = PortfolioAllocator(state_db_path=str(tmp_path / "state.db"), ttl_ms=10_000)
    gate = allocator.refresh(
        plan_path=str(plan_path),
        runner_id="wolf",
        strategy_id="*",
        instrument="*",
        requested_capital_usd=300.0,
    )
    assert gate.allow_entries
    assert gate.approved_capital_usd == 300.0
