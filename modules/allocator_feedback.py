"""Allocator feedback engine — apply HOWL + Judge evidence to live plans."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.howl_engine import HowlEngine, TradeRecord
from modules.judge_engine import JudgeReport
from modules.judge_guard import JudgeGuard


@dataclass
class FeedbackAdjustment:
    """Summary of runtime evidence adjustments to allocation plans."""
    capital_multiplier: float = 1.0
    blocked_sources: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capital_multiplier": round(self.capital_multiplier, 2),
            "blocked_sources": list(self.blocked_sources),
            "reasons": list(self.reasons),
            "metrics": dict(self.metrics),
        }


class AllocatorFeedbackEngine:
    """Compute capital and routing adjustments from live performance evidence."""

    def __init__(
        self,
        min_round_trips: int = 5,
        fp_block_threshold: float = 60.0,
        fp_reduce_threshold: float = 40.0,
        fp_reduce_multiplier: float = 0.85,
        negative_multiplier: float = 0.7,
        negative_low_wr_multiplier: float = 0.5,
        fdr_warn_multiplier: float = 0.8,
        fdr_block_multiplier: float = 0.5,
        fee_emergency_multiplier: float = 0.0,
    ):
        self.min_round_trips = min_round_trips
        self.fp_block_threshold = fp_block_threshold
        self.fp_reduce_threshold = fp_reduce_threshold
        self.fp_reduce_multiplier = fp_reduce_multiplier
        self.negative_multiplier = negative_multiplier
        self.negative_low_wr_multiplier = negative_low_wr_multiplier
        self.fdr_warn_multiplier = fdr_warn_multiplier
        self.fdr_block_multiplier = fdr_block_multiplier
        self.fee_emergency_multiplier = fee_emergency_multiplier

    def evaluate(
        self,
        trades_path: Optional[str],
        judge_report_path: Optional[str] = None,
        data_dir: Optional[str] = None,
    ) -> FeedbackAdjustment:
        adjustment = FeedbackAdjustment()

        metrics = self._load_howl_metrics(trades_path)
        if metrics:
            adjustment.metrics = metrics
            self._apply_howl_metrics(metrics, adjustment)
        else:
            adjustment.reasons.append("howl_trades_missing")

        judge_report = self._load_judge_report(trades_path, judge_report_path, data_dir)
        if judge_report:
            self._apply_judge_report(judge_report, adjustment)
        else:
            adjustment.reasons.append("judge_report_missing")

        adjustment.blocked_sources = sorted(set(adjustment.blocked_sources))
        return adjustment

    def _load_howl_metrics(self, trades_path: Optional[str]) -> Dict[str, Any]:
        if not trades_path:
            return {}
        path = Path(trades_path)
        if not path.exists():
            return {}
        trades = []
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        records = [TradeRecord.from_dict(t) for t in trades]
        metrics = HowlEngine().compute(records)
        return {
            "total_round_trips": metrics.total_round_trips,
            "win_rate": round(metrics.win_rate, 2),
            "net_pnl": round(metrics.net_pnl, 2),
            "gross_pnl": round(metrics.gross_pnl, 2),
            "total_fees": round(metrics.total_fees, 2),
            "fdr": round(metrics.fdr, 2),
        }

    def _apply_howl_metrics(self, metrics: Dict[str, Any], adjustment: FeedbackAdjustment) -> None:
        total_round_trips = int(metrics.get("total_round_trips", 0))
        if total_round_trips < self.min_round_trips:
            adjustment.reasons.append(f"howl_insufficient_round_trips:{total_round_trips}")
            return

        gross_pnl = float(metrics.get("gross_pnl", 0.0))
        total_fees = float(metrics.get("total_fees", 0.0))
        net_pnl = float(metrics.get("net_pnl", 0.0))
        win_rate = float(metrics.get("win_rate", 0.0))
        fdr = float(metrics.get("fdr", 0.0))

        if total_fees > abs(gross_pnl) and total_round_trips >= self.min_round_trips:
            self._apply_multiplier(adjustment, self.fee_emergency_multiplier, "howl_fees_exceed_gross")

        if net_pnl < 0 and win_rate < 40:
            self._apply_multiplier(adjustment, self.negative_low_wr_multiplier, "howl_negative_pnl_low_wr")
        elif net_pnl < 0:
            self._apply_multiplier(adjustment, self.negative_multiplier, "howl_negative_pnl")

        if fdr > 30:
            self._apply_multiplier(adjustment, self.fdr_block_multiplier, "howl_fdr_high")
        elif fdr > 20:
            self._apply_multiplier(adjustment, self.fdr_warn_multiplier, "howl_fdr_elevated")

    def _load_judge_report(
        self,
        trades_path: Optional[str],
        judge_report_path: Optional[str],
        data_dir: Optional[str],
    ) -> Optional[JudgeReport]:
        if judge_report_path:
            path = Path(judge_report_path)
            if not path.exists():
                return None
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError:
                return None
            return JudgeReport.from_dict(data)

        if data_dir:
            guard = JudgeGuard(data_dir=data_dir)
            return guard.read_latest_report()

        if trades_path:
            trade_dir = Path(trades_path).parent
            guard = JudgeGuard(data_dir=str(trade_dir))
            return guard.read_latest_report()

        return None

    def _apply_judge_report(self, report: JudgeReport, adjustment: FeedbackAdjustment) -> None:
        for source, rate in (report.false_positive_rates or {}).items():
            rate_val = float(rate or 0.0)
            if rate_val >= self.fp_block_threshold:
                adjustment.blocked_sources.append(str(source))
                adjustment.reasons.append(f"judge_blocked_source:{source}:{rate_val:.0f}%")
            elif rate_val >= self.fp_reduce_threshold:
                self._apply_multiplier(
                    adjustment,
                    self.fp_reduce_multiplier,
                    f"judge_reduce_source:{source}:{rate_val:.0f}%",
                )

    @staticmethod
    def _apply_multiplier(adjustment: FeedbackAdjustment, multiplier: float, reason: str) -> None:
        multiplier = float(multiplier)
        if multiplier < adjustment.capital_multiplier:
            adjustment.capital_multiplier = multiplier
            adjustment.reasons.append(reason)
