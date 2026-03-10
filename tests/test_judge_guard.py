"""Tests for JudgeGuard memory integration."""

from modules.judge_engine import JudgeReport
from modules.judge_guard import JudgeGuard
from modules.memory_guard import MemoryGuard


def test_apply_to_memory_overwrites_playbook_snapshot(tmp_path):
    judge = JudgeGuard(data_dir=str(tmp_path / "wolf"))
    memory = MemoryGuard(data_dir=str(tmp_path / "memory"))

    stats = {
        "ETH-PERP:scanner": {
            "instrument": "ETH-PERP",
            "source": "scanner",
            "count": 2,
            "wins": 1,
            "total_pnl": 12.5,
            "total_roe": 8.0,
        }
    }

    report = JudgeReport(timestamp_ms=1, playbook_stats=stats)
    judge.apply_to_memory(report, memory)
    judge.apply_to_memory(JudgeReport(timestamp_ms=2, playbook_stats=stats), memory)

    playbook = memory.load_playbook()
    entry = playbook.get("ETH-PERP", "scanner")

    assert entry is not None
    assert entry.trade_count == 2
    assert entry.win_count == 1
    assert entry.total_pnl == 12.5
    assert entry.total_roe == 8.0
