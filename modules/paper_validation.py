"""Paper validation — aggregate HOWL metrics across paper-mode runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.howl_engine import HowlEngine, TradeRecord
from modules.howl_reporter import HowlReporter


@dataclass
class PaperStrategySummary:
    data_dir: str
    total_trades: int = 0
    total_round_trips: int = 0
    win_rate: float = 0.0
    net_pnl: float = 0.0
    fdr: float = 0.0
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data_dir": self.data_dir,
            "total_trades": self.total_trades,
            "total_round_trips": self.total_round_trips,
            "win_rate": round(self.win_rate, 2),
            "net_pnl": round(self.net_pnl, 2),
            "fdr": round(self.fdr, 2),
            "summary": self.summary,
        }


@dataclass
class PaperValidationResult:
    generated_at: str
    data_dirs: List[str] = field(default_factory=list)
    strategies: List[PaperStrategySummary] = field(default_factory=list)
    combined: Dict[str, Any] = field(default_factory=dict)
    report_path: str = ""
    json_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "data_dirs": list(self.data_dirs),
            "strategies": [s.to_dict() for s in self.strategies],
            "combined": dict(self.combined),
            "report_path": self.report_path,
            "json_path": self.json_path,
        }


def run_paper_validation(
    data_dirs: List[str],
    output_dir: str = "data/howl",
    since: Optional[str] = None,
) -> PaperValidationResult:
    """Run HOWL over multiple data dirs and write a consolidated report."""
    since_ms = _parse_since_ms(since)
    all_trades: List[TradeRecord] = []
    summaries: List[PaperStrategySummary] = []

    reporter = HowlReporter()
    engine = HowlEngine()

    for data_dir in data_dirs:
        trades = _load_trades(data_dir, since_ms)
        if not trades:
            summaries.append(PaperStrategySummary(
                data_dir=data_dir,
                summary="No trades found",
            ))
            continue
        metrics = engine.compute(trades)
        summaries.append(PaperStrategySummary(
            data_dir=data_dir,
            total_trades=metrics.total_trades,
            total_round_trips=metrics.total_round_trips,
            win_rate=metrics.win_rate,
            net_pnl=metrics.net_pnl,
            fdr=metrics.fdr,
            summary=reporter.distill(metrics),
        ))
        all_trades.extend(trades)

    if not all_trades:
        raise ValueError("No trades found across provided data dirs.")

    combined_metrics = engine.compute(all_trades)
    combined_summary = reporter.distill(combined_metrics)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_text = _render_report(generated_at, data_dirs, summaries, combined_metrics, combined_summary)
    report_text = _sanitize_text(report_text)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"paper_validation_{generated_at}.md"
    json_path = out_dir / f"paper_validation_{generated_at}.json"
    report_path.write_text(report_text)
    json_path.write_text(json.dumps({
        "generated_at": generated_at,
        "data_dirs": data_dirs,
        "strategies": [s.to_dict() for s in summaries],
        "combined": {
            "total_trades": combined_metrics.total_trades,
            "total_round_trips": combined_metrics.total_round_trips,
            "win_rate": round(combined_metrics.win_rate, 2),
            "net_pnl": round(combined_metrics.net_pnl, 2),
            "fdr": round(combined_metrics.fdr, 2),
            "summary": _sanitize_text(combined_summary),
        },
    }, indent=2))

    return PaperValidationResult(
        generated_at=generated_at,
        data_dirs=data_dirs,
        strategies=summaries,
        combined={
            "total_trades": combined_metrics.total_trades,
            "total_round_trips": combined_metrics.total_round_trips,
            "win_rate": round(combined_metrics.win_rate, 2),
            "net_pnl": round(combined_metrics.net_pnl, 2),
            "fdr": round(combined_metrics.fdr, 2),
            "summary": _sanitize_text(combined_summary),
        },
        report_path=str(report_path),
        json_path=str(json_path),
    )


def _load_trades(data_dir: str, since_ms: int) -> List[TradeRecord]:
    trades_path = Path(data_dir) / "trades.jsonl"
    if not trades_path.exists():
        return []
    records: List[TradeRecord] = []
    for line in trades_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        record = TradeRecord.from_dict(data)
        if since_ms and record.timestamp_ms < since_ms:
            continue
        records.append(record)
    return records


def _parse_since_ms(since: Optional[str]) -> int:
    if not since:
        return 0
    try:
        since_dt = datetime.strptime(since, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.") from exc
    return int(since_dt.timestamp() * 1000)


def _render_report(
    date: str,
    data_dirs: List[str],
    summaries: List[PaperStrategySummary],
    combined_metrics,
    combined_summary: str,
) -> str:
    lines = [
        f"# WOLF Stack Paper Validation - {date}",
        "",
        "Data dirs:",
    ]
    for path in data_dirs:
        lines.append(f"- {path}")
    lines.append("")
    lines.append("## Combined Summary")
    lines.append(_sanitize_text(combined_summary))
    lines.append("")
    lines.append("## Per-Strategy")
    lines.append("| Data Dir | Trades | Round Trips | Win Rate | Net PnL | FDR | Summary |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for item in summaries:
        lines.append(
            f"| {item.data_dir} | {item.total_trades} | {item.total_round_trips} | "
            f"{item.win_rate:.2f}% | ${item.net_pnl:.2f} | {item.fdr:.2f}% | "
            f"{_sanitize_text(item.summary)} |"
        )
    lines.append("")
    lines.append("## Combined Metrics")
    lines.append(f"- Total trades: {combined_metrics.total_trades}")
    lines.append(f"- Total round trips: {combined_metrics.total_round_trips}")
    lines.append(f"- Win rate: {combined_metrics.win_rate:.2f}%")
    lines.append(f"- Net PnL: ${combined_metrics.net_pnl:.2f}")
    lines.append(f"- FDR: {combined_metrics.fdr:.2f}%")
    return "\n".join(lines)


def _sanitize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("∞", "inf").replace("—", "-")
    return text.encode("ascii", "replace").decode("ascii")
