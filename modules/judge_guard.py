"""Judge guard — I/O bridge for the evaluator system.

Handles persistence of judge reports and integration with memory system.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import List, Optional

from modules.judge_engine import JudgeEngine, JudgeReport
from parent.store import JSONLStore


class JudgeGuard:
    """Runs judge evaluation and persists reports."""

    def __init__(self, data_dir: str = "data/wolf"):
        self.data_dir = Path(data_dir)
        self._report_dir = self.data_dir / "judge"
        self._report_dir.mkdir(parents=True, exist_ok=True)
        self._engine = JudgeEngine()

    def run_evaluation(
        self,
        trade_log: JSONLStore,
        closed_slots: Optional[List[dict]] = None,
    ) -> JudgeReport:
        """Load trades and run judge evaluation."""
        trades = trade_log.read_all()
        return self._engine.evaluate(trades, closed_slots)

    def save_report(self, report: JudgeReport) -> Path:
        """Save report to timestamped JSON file."""
        from datetime import datetime, timezone
        ts = datetime.fromtimestamp(
            report.timestamp_ms / 1000, tz=timezone.utc,
        ).strftime("%Y-%m-%d-%H%M")
        path = self._report_dir / f"{ts}.json"
        path.write_text(json.dumps(report.to_dict(), indent=2, default=str))
        return path

    def read_latest_report(self) -> Optional[JudgeReport]:
        """Read the most recent judge report."""
        reports = sorted(self._report_dir.glob("*.json"), reverse=True)
        if not reports:
            return None
        try:
            data = json.loads(reports[0].read_text())
            return JudgeReport.from_dict(data)
        except (json.JSONDecodeError, KeyError):
            return None

    def apply_to_memory(self, report: JudgeReport, memory_guard) -> None:
        """Write judge findings to memory as events and update playbook."""
        from modules.memory_engine import MemoryEngine

        engine = MemoryEngine()

        # Log judge finding event
        rec_summaries = [r.get("summary", "") for r in report.config_recommendations]
        event = engine.create_judge_event(
            findings_count=len(report.findings),
            false_positive_rates=report.false_positive_rates,
            recommendations=rec_summaries,
        )
        memory_guard.log_event(event)

        # Update playbook with per-instrument stats
        playbook = memory_guard.load_playbook()
        for key, stats in report.playbook_stats.items():
            from modules.memory_engine import PlaybookEntry
            existing = playbook.entries.get(key)
            if not existing:
                existing = PlaybookEntry(
                    instrument=stats.get("instrument", ""),
                    signal_source=stats.get("source", ""),
                )

            # Judge reports are already full-history aggregates, so overwrite
            # rather than additively merging and double-counting on each run.
            existing.trade_count = stats.get("count", 0)
            existing.win_count = stats.get("wins", 0)
            existing.total_pnl = stats.get("total_pnl", 0.0)
            existing.total_roe = stats.get("total_roe", 0.0)
            existing.last_updated_ms = report.timestamp_ms
            playbook.entries[key] = existing

        memory_guard.save_playbook(playbook)
