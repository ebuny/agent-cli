"""Tests for paper validation aggregation."""
from __future__ import annotations

import json

from modules.paper_validation import run_paper_validation


def _write_trade(path, side, price, ts, meta=""):
    path.write_text(
        (path.read_text() if path.exists() else "")
        + json.dumps({
            "instrument": "ETH-PERP",
            "side": side,
            "price": price,
            "quantity": 1,
            "timestamp_ms": ts,
            "fee": 0,
            "strategy": "test",
            "meta": meta,
        }) + "\n"
    )


def test_paper_validation_combines_metrics(tmp_path):
    d1 = tmp_path / "s1"
    d2 = tmp_path / "s2"
    d1.mkdir()
    d2.mkdir()

    t1 = d1 / "trades.jsonl"
    t2 = d2 / "trades.jsonl"

    _write_trade(t1, "buy", 100, 1, "entry:scanner")
    _write_trade(t1, "sell", 110, 2, "exit")
    _write_trade(t2, "buy", 200, 3, "entry:scanner")
    _write_trade(t2, "sell", 190, 4, "exit")

    result = run_paper_validation(
        data_dirs=[str(d1), str(d2)],
        output_dir=str(tmp_path / "out"),
    )

    assert result.combined["total_round_trips"] == 2
    assert result.combined["total_trades"] == 4
    assert result.report_path
    assert result.json_path
