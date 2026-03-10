"""Runner-level tests for WOLF execution wiring."""
from __future__ import annotations

import json

from cli.hl_adapter import DirectMockProxy
from modules.runtime_allocation import RuntimeAllocationGate
from modules.wolf_config import WolfConfig
from modules.wolf_engine import WolfAction
from skills.wolf.scripts.standalone_runner import WolfRunner


def test_twap_entry_completes_and_activates_slot(tmp_path, monkeypatch):
    monkeypatch.setattr("execution.twap.random.random", lambda: 0.5)
    monkeypatch.setattr("execution.twap.random.uniform", lambda _a, _b: 0.0)

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(max_slots=1, twap_threshold_usd=1.0, twap_duration_ticks=5, twap_urgency=1.0),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )
    runner.state.tick_count = 1

    action = WolfAction(
        action="enter",
        slot_id=0,
        instrument="ETH-PERP",
        direction="long",
        reason="scanner: score=200",
        source="scanner",
        signal_score=200,
        execution_algo="twap",
    )

    runner._execute_enter(action)
    assert runner.state.slots[0].status == "entering"
    assert len(runner.state.entry_queue) == 1

    for _ in range(6):
        runner._process_pending_entries()
        if runner.state.slots[0].status == "active":
            break

    slot = runner.state.slots[0]
    assert slot.status == "active"
    assert slot.entry_size > 0
    assert runner.state.entry_queue == []
    assert 0 in runner.dsl_guards

    trades = runner.trade_log.read_all()
    assert trades
    assert all(t.get("execution_algo") == "twap" for t in trades)


def test_allocation_gate_blocks_entry_when_no_validated_edge(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "volatile"},
        "deployable_capital_usd": 5000.0,
        "allocated_capital_usd": 0.0,
        "enabled_strategies": 0,
        "routing_policy": {"generated_at": "", "rules": []},
        "decisions": [],
    }))

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            max_slots=1,
            allocation_enforce=True,
            allocation_plan_path=str(plan_path),
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )

    action = WolfAction(
        action="enter",
        slot_id=0,
        instrument="ETH-PERP",
        direction="long",
        reason="scanner: score=200",
        source="scanner",
        signal_score=200,
    )
    runner._execute_enter(action)

    slot = runner.state.slots[0]
    assert slot.status == "empty"
    assert runner.trade_log.read_all() == []


def test_allocation_gate_caps_entry_margin(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "carry"},
        "deployable_capital_usd": 5000.0,
        "allocated_capital_usd": 1000.0,
        "enabled_strategies": 1,
        "routing_policy": {
            "generated_at": "",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["carry"],
                    "allowed_instruments": ["ETH-PERP"],
                    "capital_usd": 1000.0,
                    "weight": 1.0,
                    "score": 90.0,
                    "reasons": ["eligible_for_allocation"],
                }
            ],
        },
        "decisions": [
            {
                "strategy_id": "momentum_breakout",
                "enabled": True,
                "capital_usd": 1000.0,
                "weight": 1.0,
                "reasons": ["eligible_for_allocation"],
            }
        ],
    }))

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            total_budget=10_000.0,
            max_slots=2,
            allocation_enforce=True,
            allocation_plan_path=str(plan_path),
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )
    runner.state.tick_count = 1

    action = WolfAction(
        action="enter",
        slot_id=0,
        instrument="ETH-PERP",
        direction="long",
        reason="scanner: score=200",
        source="scanner",
        signal_score=200,
    )
    runner._execute_enter(action)

    slot = runner.state.slots[0]
    assert slot.status == "active"
    assert slot.margin_allocated == 500.0
    assert slot.entry_size == 2.0


def test_allocation_source_routing_filters_wolf_inputs(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "deployable_capital_usd": 5000.0,
        "allocated_capital_usd": 2000.0,
        "enabled_strategies": 1,
        "routing_policy": {
            "generated_at": "",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["SOL-PERP"],
                    "min_signal_score": 185.0,
                    "capital_usd": 2000.0,
                    "weight": 1.0,
                    "score": 95.0,
                    "reasons": ["eligible_for_allocation"],
                },
                {
                    "strategy_id": "momentum_breakout",
                    "source": "movers_signal",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["DOGE-PERP"],
                    "min_signal_score": 75.0,
                    "capital_usd": 2000.0,
                    "weight": 1.0,
                    "score": 95.0,
                    "reasons": ["eligible_for_allocation"],
                }
            ],
        },
        "decisions": [
            {
                "strategy_id": "momentum_breakout",
                "enabled": True,
                "capital_usd": 2000.0,
                "weight": 1.0,
                "reasons": ["eligible_for_allocation"],
            }
        ],
    }))

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            max_slots=1,
            allocation_enforce=True,
            allocation_plan_path=str(plan_path),
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )

    movers, scanner, smart_money = runner._apply_source_routing(
        movers_signals=[
            {"asset": "ETH", "signal_type": "IMMEDIATE_MOVER", "direction": "LONG", "confidence": 100},
            {"asset": "DOGE", "signal_type": "OI_BREAKOUT", "direction": "LONG", "confidence": 80},
        ],
        scanner_opps=[{"asset": "SOL", "direction": "LONG", "final_score": 190}],
        smart_money_signals=[{"asset": "BTC", "signal_type": "SMART_MONEY", "direction": "LONG", "confidence": 75}],
    )

    assert len(movers) == 1
    assert movers[0]["asset"] == "DOGE"
    assert len(scanner) == 1
    assert smart_money == []


def test_allocation_source_routing_blocks_low_quality_signals(tmp_path):
    plan_path = tmp_path / "live_plan.json"
    plan_path.write_text(json.dumps({
        "current_regime": {"label": "trend_up"},
        "deployable_capital_usd": 5000.0,
        "allocated_capital_usd": 2000.0,
        "enabled_strategies": 1,
        "routing_policy": {
            "generated_at": "",
            "rules": [
                {
                    "strategy_id": "momentum_breakout",
                    "source": "scanner",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["SOL-PERP"],
                    "min_signal_score": 210.0,
                    "capital_usd": 2000.0,
                    "weight": 1.0,
                    "score": 95.0,
                    "reasons": ["eligible_for_allocation", "empirical_min_signal_score"],
                },
                {
                    "strategy_id": "momentum_breakout",
                    "source": "movers_signal",
                    "allowed_regimes": ["trend_up"],
                    "allowed_instruments": ["DOGE-PERP"],
                    "min_signal_score": 85.0,
                    "capital_usd": 2000.0,
                    "weight": 1.0,
                    "score": 95.0,
                    "reasons": ["eligible_for_allocation", "empirical_min_signal_score"],
                }
            ],
        },
        "decisions": [
            {
                "strategy_id": "momentum_breakout",
                "enabled": True,
                "capital_usd": 2000.0,
                "weight": 1.0,
                "reasons": ["eligible_for_allocation"],
            }
        ],
    }))

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            max_slots=1,
            allocation_enforce=True,
            allocation_plan_path=str(plan_path),
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )

    movers, scanner, smart_money = runner._apply_source_routing(
        movers_signals=[
            {"asset": "DOGE", "signal_type": "OI_BREAKOUT", "direction": "LONG", "confidence": 80},
            {"asset": "DOGE", "signal_type": "OI_BREAKOUT", "direction": "LONG", "confidence": 90},
        ],
        scanner_opps=[
            {"asset": "SOL", "direction": "LONG", "final_score": 205},
            {"asset": "SOL", "direction": "LONG", "final_score": 215},
        ],
        smart_money_signals=[],
    )

    assert len(movers) == 1
    assert movers[0]["confidence"] == 90
    assert len(scanner) == 1
    assert scanner[0]["final_score"] == 215
    assert smart_money == []


def test_execution_health_blocks_entries_when_fill_ratio_too_low(tmp_path):
    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            execution_health_enabled=True,
            execution_health_min_attempts=2,
            execution_health_min_fill_ratio=0.9,
            execution_health_max_api_error_rate=0.5,
            execution_health_max_avg_slippage_bps=200.0,
            execution_health_max_p95_slippage_bps=200.0,
            execution_health_refresh_ticks=1,
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )

    runner.execution_health.record_attempt(
        instrument="SOL-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=False,
    )
    runner.execution_health.record_attempt(
        instrument="SOL-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=False,
    )
    runner._refresh_execution_gate(force=True)

    assert not runner._execution_gate.allow_entries


def test_execution_health_blocks_specific_edges_not_all(tmp_path):
    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            allocation_enforce=True,
            execution_health_enabled=True,
            execution_health_min_attempts=2,
            execution_health_min_fill_ratio=0.8,
            execution_health_max_api_error_rate=0.5,
            execution_health_max_avg_slippage_bps=200.0,
            execution_health_max_p95_slippage_bps=200.0,
            execution_health_refresh_ticks=1,
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )
    runner._allocation_gate = RuntimeAllocationGate(
        configured=True,
        allow_entries=True,
        allowed_entry_sources=["scanner", "movers_signal"],
        routing_rules=[
            {"source": "scanner", "allowed_instruments": ["SOL-PERP"]},
            {"source": "movers_signal", "allowed_instruments": ["DOGE-PERP"]},
        ],
    )

    runner.execution_health.record_attempt(
        instrument="SOL-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=False,
    )
    runner.execution_health.record_attempt(
        instrument="SOL-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=False,
    )
    for _ in range(8):
        runner.execution_health.record_attempt(
            instrument="DOGE-PERP",
            source="movers_signal",
            side="buy",
            requested_qty=1.0,
            success=True,
        )
        runner.execution_health.record_fill(
            instrument="DOGE-PERP",
            source="movers_signal",
            side="buy",
            requested_qty=1.0,
            filled_qty=1.0,
            fill_price=100.0,
            mid_price=100.0,
        )
    runner._refresh_execution_gate(force=True)

    assert runner._execution_gate.allow_entries
    assert not runner._is_entry_allowed("scanner", "SOL-PERP", signal_score=200.0)
    assert runner._is_entry_allowed("movers_signal", "DOGE-PERP", signal_score=200.0)


def test_portfolio_gate_blocks_wolf_entries(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps({
        "allocated_capital_usd": 0.0,
        "decisions": [],
    }))

    runner = WolfRunner(
        hl=DirectMockProxy(),
        config=WolfConfig(
            allocation_enforce=False,
            portfolio_enforce=True,
            portfolio_plan_path=str(plan_path),
            portfolio_refresh_ticks=1,
            portfolio_ttl_ticks=5,
        ),
        tick_interval=0.0,
        data_dir=str(tmp_path / "wolf"),
        resume=False,
    )
    runner._refresh_portfolio_gate(force=True)
    assert runner._portfolio_gate.configured
    assert not runner._portfolio_gate.allow_entries

    action = WolfAction(
        action="enter",
        slot_id=0,
        instrument="ETH-PERP",
        direction="long",
        reason="scanner: score=200",
        source="scanner",
        signal_score=200,
    )
    runner._execute_enter(action)
    slot = runner.state.slots[0]
    assert slot.status == "empty"
