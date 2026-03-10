"""Tests for allocator feedback adjustments."""
from __future__ import annotations

import json

from modules.allocator_feedback import AllocatorFeedbackEngine


def test_allocator_feedback_applies_negative_pnl_multiplier(tmp_path):
    trades_path = tmp_path / "trades.jsonl"
    trades_path.write_text("\n".join([
        json.dumps({
            "instrument": "ETH-PERP",
            "side": "buy",
            "price": 100,
            "quantity": 1,
            "fee": 0,
            "timestamp_ms": 1,
            "strategy": "wolf",
            "meta": "entry:scanner",
        }),
        json.dumps({
            "instrument": "ETH-PERP",
            "side": "sell",
            "price": 90,
            "quantity": 1,
            "fee": 0,
            "timestamp_ms": 2,
            "strategy": "wolf",
            "meta": "exit",
        }),
    ]))

    engine = AllocatorFeedbackEngine(min_round_trips=1)
    adjustment = engine.evaluate(trades_path=str(trades_path))

    assert adjustment.capital_multiplier == 0.5
    assert "howl_negative_pnl_low_wr" in adjustment.reasons


def test_allocator_feedback_blocks_sources_from_judge(tmp_path):
    report_path = tmp_path / "judge.json"
    report_path.write_text(json.dumps({
        "timestamp_ms": 0,
        "round_trips_evaluated": 10,
        "false_positive_rates": {
            "scanner": 70.0,
            "movers_signal": 45.0,
        },
        "findings": [],
    }))

    engine = AllocatorFeedbackEngine(min_round_trips=1)
    adjustment = engine.evaluate(trades_path=None, judge_report_path=str(report_path))

    assert "scanner" in adjustment.blocked_sources
    assert adjustment.capital_multiplier == 0.85
