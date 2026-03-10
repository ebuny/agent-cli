"""Tests for research runner orchestration."""
from __future__ import annotations

import json

from modules.research_runner import ResearchRunner


def _write_dataset(path, instrument="ETH-PERP", rows=12):
    data = []
    for idx in range(rows):
        data.append({
            "instrument": instrument,
            "timestamp_ms": idx + 1,
            "mid_price": 100 + idx * 2,
            "bid": 99 + idx * 2,
            "ask": 101 + idx * 2,
            "volume_24h": 1000,
            "funding_rate": 0.0,
            "open_interest": 10000,
        })
    path.write_text("\n".join(json.dumps(row) for row in data))


def test_research_runner_emits_report_and_latest(tmp_path):
    dataset = tmp_path / "eth.jsonl"
    _write_dataset(dataset)
    edge_report = tmp_path / "wolf_edge_report.json"
    edge_report.write_text(json.dumps({
        "thresholds": [
            {
                "source": "scanner",
                "instrument": "ETH-PERP",
                "min_signal_score": 205.0,
            }
        ]
    }))

    research_dir = tmp_path / "research"
    runner = ResearchRunner(data_dir=str(research_dir))
    report = runner.run(
        strategy_names=["basis_arb", "mean_reversion"],
        dataset_paths=[str(dataset)],
        train_size=4,
        validation_size=2,
        step_size=2,
        capital_usd=5000,
        reserve_pct=0.1,
        max_strategy_pct=0.6,
        wolf_edge_report_path=str(edge_report),
    )

    assert len(report.strategy_results) == 2
    assert len(report.allocations) == 2
    assert report.summary.deployable_capital_usd == 4500.0
    assert report.summary.reserved_capital_usd == 500.0
    assert report.summary.top_strategy_id
    assert report.artifacts["report_path"]
    assert report.artifacts["deployment_snapshot_path"]
    assert report.artifacts["wolf_edge_report_path"] == str(edge_report)
    assert (research_dir / "latest_allocation.json").exists()
    assert all(result.dataset_breakdown for result in report.strategy_results)
    assert report.strategy_results[0].dataset_breakdown[0].dataset == "eth"
    assert report.strategy_results[0].scorecard is not None
    assert hasattr(report.strategy_results[0].scorecard, "supported_regimes")

    latest = runner.latest_report()
    assert latest is not None
    assert len(latest.allocations) == 2
    assert latest.summary.deployable_capital_usd == 4500.0
    assert latest.artifacts["deployment_snapshot_path"].endswith("latest_allocation.json")

    # Non-report artifacts should not shadow the latest report lookup.
    (research_dir / "live_allocation_plan.json").write_text(json.dumps({
        "current_regime": {"label": "volatile"},
        "decisions": [],
    }))
    latest_again = runner.latest_report()
    assert latest_again is not None
    assert len(latest_again.strategy_results) == 2

    snapshot = json.loads((research_dir / "latest_allocation.json").read_text())
    assert "routing_policy" in snapshot
    assert isinstance(snapshot["routing_policy"]["rules"], list)
    assert all("min_signal_score" in rule for rule in snapshot["routing_policy"]["rules"])
