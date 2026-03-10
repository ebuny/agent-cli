"""Tests for execution health tracking and metrics."""
from __future__ import annotations

from modules.execution_health import ExecutionHealthTracker


def test_execution_health_metrics_fill_ratio_and_slippage(tmp_path):
    tracker = ExecutionHealthTracker(path=str(tmp_path / "health.jsonl"), max_events=10)

    tracker.record_attempt(
        instrument="ETH-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=True,
    )
    tracker.record_fill(
        instrument="ETH-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        filled_qty=1.0,
        fill_price=101.0,
        mid_price=100.0,
    )
    tracker.record_attempt(
        instrument="ETH-PERP",
        source="scanner",
        side="buy",
        requested_qty=1.0,
        success=False,
        error="no_fill",
    )

    metrics = tracker.compute_metrics()

    assert metrics.attempts == 2
    assert metrics.successes == 1
    assert metrics.failures == 1
    assert metrics.requested_qty == 2.0
    assert metrics.filled_qty == 1.0
    assert metrics.fill_ratio == 0.5
    assert metrics.avg_slippage_bps == 100.0
    assert metrics.p95_slippage_bps == 100.0
    assert metrics.api_error_rate == 0.5
