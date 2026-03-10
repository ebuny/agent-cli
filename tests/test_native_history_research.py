"""Tests for native history ingestion into research artifacts."""
from __future__ import annotations

import json

from modules.native_history_research import (
    NativeHistoryResearchBuilder,
    WolfTradeAttributionAnalyzer,
)


def test_native_history_builder_merges_scanner_and_movers(tmp_path):
    scanner_path = tmp_path / "scanner-history.json"
    movers_path = tmp_path / "movers-history.json"

    scanner_history = [
        {
            "scan_time_ms": 1000,
            "btc_macro": {"trend": "up", "strength": 8},
            "opportunities": [
                {
                    "asset": "ETH",
                    "direction": "LONG",
                    "final_score": 210.0,
                    "raw_score": 195.0,
                    "pillar_scores": {"market_structure": 70, "technicals": 50, "funding": 25},
                    "market_data": {
                        "vol24h": 2000000,
                        "oi": 100000,
                        "funding_rate": 0.0001,
                        "mark_price": 2500,
                    },
                }
            ],
        },
        {
            "scan_time_ms": 2000,
            "btc_macro": {"trend": "down", "strength": 4},
            "opportunities": [
                {
                    "asset": "ETH",
                    "direction": "SHORT",
                    "final_score": 180.0,
                    "raw_score": 170.0,
                    "pillar_scores": {"market_structure": 55, "technicals": 45, "funding": 20},
                    "market_data": {
                        "vol24h": 2500000,
                        "oi": 110000,
                        "funding_rate": -0.0002,
                        "mark_price": 2450,
                    },
                }
            ],
        },
    ]
    scanner_path.write_text(json.dumps(scanner_history))

    movers_history = [
        {
            "scan_time_ms": 1000,
            "signals": [
                {
                    "asset": "ETH",
                    "signal_type": "IMMEDIATE_MOVER",
                    "direction": "LONG",
                    "confidence": 100,
                    "oi_delta_pct": 12.5,
                    "volume_surge_ratio": 5.2,
                    "price_change_pct": 3.1,
                }
            ],
            "snapshots": [
                {
                    "asset": "ETH",
                    "timestamp_ms": 1000,
                    "open_interest": 100000,
                    "volume_24h": 2000000,
                    "funding_rate": 0.0001,
                    "mark_price": 2500,
                },
                {
                    "asset": "BTC",
                    "timestamp_ms": 1000,
                    "open_interest": 500000,
                    "volume_24h": 4000000,
                    "funding_rate": 0.0,
                    "mark_price": 50000,
                },
            ],
        },
        {
            "scan_time_ms": 2000,
            "signals": [],
            "snapshots": [
                {
                    "asset": "ETH",
                    "timestamp_ms": 2000,
                    "open_interest": 110000,
                    "volume_24h": 2500000,
                    "funding_rate": -0.0002,
                    "mark_price": 2450,
                },
                {
                    "asset": "BTC",
                    "timestamp_ms": 2000,
                    "open_interest": 505000,
                    "volume_24h": 4100000,
                    "funding_rate": 0.00001,
                    "mark_price": 50100,
                },
            ],
        },
    ]
    movers_path.write_text(json.dumps(movers_history))

    report = NativeHistoryResearchBuilder().build(
        output_dir=str(tmp_path / "datasets"),
        scanner_history_path=str(scanner_path),
        movers_history_path=str(movers_path),
        min_points=2,
    )

    assert len(report.dataset_artifacts) == 2
    eth_artifact = next(item for item in report.dataset_artifacts if item.instrument == "ETH-PERP")
    assert sorted(eth_artifact.source_types) == ["movers", "scanner"]

    eth_rows = [
        json.loads(line)
        for line in (tmp_path / "datasets" / "eth_perp.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(eth_rows) == 2
    assert eth_rows[0]["scanner_score"] == 210.0
    assert eth_rows[0]["mover_signal_type"] == "IMMEDIATE_MOVER"
    assert eth_rows[1]["btc_macro_trend"] == "down"


def test_wolf_trade_attribution_analyzer_summarizes_sources(tmp_path):
    trades_path = tmp_path / "trades.jsonl"
    trades = [
        {
            "tick": 1,
            "oid": "1",
            "instrument": "ETH-PERP",
            "side": "buy",
            "price": 100.0,
            "quantity": 1.0,
            "timestamp_ms": 1000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "entry:scanner",
            "entry_signal_score": 200.0,
        },
        {
            "tick": 2,
            "oid": "2",
            "instrument": "ETH-PERP",
            "side": "sell",
            "price": 110.0,
            "quantity": 1.0,
            "timestamp_ms": 2000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "dsl_close",
        },
        {
            "tick": 3,
            "oid": "3",
            "instrument": "BTC-PERP",
            "side": "sell",
            "price": 200.0,
            "quantity": 2.0,
            "timestamp_ms": 3000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "entry:movers_immediate",
            "entry_signal_score": 100.0,
        },
        {
            "tick": 4,
            "oid": "4",
            "instrument": "BTC-PERP",
            "side": "buy",
            "price": 190.0,
            "quantity": 2.0,
            "timestamp_ms": 4500,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "take_profit",
        },
    ]
    trades_path.write_text("\n".join(json.dumps(item) for item in trades))

    analyzer = WolfTradeAttributionAnalyzer()
    report = analyzer.analyze(str(trades_path))

    assert report.total_trades == 4
    assert report.total_round_trips == 2
    assert [item.key for item in report.by_source] == ["movers_immediate", "scanner"]
    assert report.by_source[0].net_pnl == 20.0
    assert report.by_source[1].net_pnl == 10.0
    assert report.thresholds
    assert any(item.source == "scanner" and item.instrument == "ETH-PERP" for item in report.thresholds)

    saved_path = analyzer.save_report(report, output_dir=str(tmp_path / "reports"))
    saved = json.loads(saved_path.read_text())
    assert saved["total_round_trips"] == 2
    assert saved["by_instrument"][0]["key"] in {"BTC-PERP", "ETH-PERP"}
    assert "thresholds" in saved


def test_wolf_trade_attribution_analyzer_recommends_signal_floor_from_history(tmp_path):
    trades_path = tmp_path / "trades.jsonl"
    trades = [
        {
            "tick": 1,
            "oid": "1",
            "instrument": "ETH-PERP",
            "side": "buy",
            "price": 100.0,
            "quantity": 1.0,
            "timestamp_ms": 1000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "entry:scanner",
            "entry_signal_score": 220.0,
        },
        {
            "tick": 2,
            "oid": "2",
            "instrument": "ETH-PERP",
            "side": "sell",
            "price": 110.0,
            "quantity": 1.0,
            "timestamp_ms": 2000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "take_profit",
        },
        {
            "tick": 3,
            "oid": "3",
            "instrument": "ETH-PERP",
            "side": "buy",
            "price": 120.0,
            "quantity": 1.0,
            "timestamp_ms": 3000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "entry:scanner",
            "entry_signal_score": 150.0,
        },
        {
            "tick": 4,
            "oid": "4",
            "instrument": "ETH-PERP",
            "side": "sell",
            "price": 112.0,
            "quantity": 1.0,
            "timestamp_ms": 4000,
            "fee": 0.0,
            "strategy": "wolf",
            "meta": "hard_stop",
        },
    ]
    trades_path.write_text("\n".join(json.dumps(item) for item in trades))

    report = WolfTradeAttributionAnalyzer().analyze(str(trades_path))

    threshold = next(
        item for item in report.thresholds
        if item.source == "scanner" and item.instrument == "ETH-PERP"
    )
    assert threshold.round_trips == 2
    assert threshold.profitable_samples == 1
    assert threshold.losing_samples == 1
    assert threshold.min_signal_score == 220.0
    assert "floored_above_avg_losing_score" in threshold.reasons
