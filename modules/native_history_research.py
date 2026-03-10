"""Build replayable research datasets from native scanner/movers/WOLF history."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from modules.judge_engine import JudgeEngine


@dataclass
class NativeDatasetArtifact:
    instrument: str
    rows: int
    source_types: List[str] = field(default_factory=list)
    path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NativeDatasetBuildReport:
    output_dir: str
    dataset_artifacts: List[NativeDatasetArtifact] = field(default_factory=list)
    skipped_instruments: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "output_dir": self.output_dir,
            "dataset_artifacts": [item.to_dict() for item in self.dataset_artifacts],
            "skipped_instruments": dict(self.skipped_instruments),
        }


@dataclass
class WolfEdgeSummary:
    key: str
    round_trips: int
    win_rate: float
    net_pnl: float
    avg_roe_pct: float
    avg_entry_score: float
    avg_holding_ms: float
    instruments: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "round_trips": self.round_trips,
            "win_rate": round(self.win_rate, 2),
            "net_pnl": round(self.net_pnl, 4),
            "avg_roe_pct": round(self.avg_roe_pct, 4),
            "avg_entry_score": round(self.avg_entry_score, 2),
            "avg_holding_ms": round(self.avg_holding_ms, 2),
            "instruments": list(self.instruments),
        }


@dataclass
class WolfSignalThreshold:
    source: str
    instrument: str = "*"
    round_trips: int = 0
    profitable_samples: int = 0
    losing_samples: int = 0
    min_signal_score: float = 0.0
    avg_profitable_entry_score: float = 0.0
    avg_losing_entry_score: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "instrument": self.instrument,
            "round_trips": self.round_trips,
            "profitable_samples": self.profitable_samples,
            "losing_samples": self.losing_samples,
            "min_signal_score": round(self.min_signal_score, 2),
            "avg_profitable_entry_score": round(self.avg_profitable_entry_score, 2),
            "avg_losing_entry_score": round(self.avg_losing_entry_score, 2),
            "reasons": list(self.reasons),
        }


@dataclass
class WolfEdgeReport:
    source_path: str
    total_trades: int
    total_round_trips: int
    by_source: List[WolfEdgeSummary] = field(default_factory=list)
    by_instrument: List[WolfEdgeSummary] = field(default_factory=list)
    thresholds: List[WolfSignalThreshold] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_path": self.source_path,
            "total_trades": self.total_trades,
            "total_round_trips": self.total_round_trips,
            "by_source": [item.to_dict() for item in self.by_source],
            "by_instrument": [item.to_dict() for item in self.by_instrument],
            "thresholds": [item.to_dict() for item in self.thresholds],
        }


class NativeHistoryResearchBuilder:
    """Convert native scanner and movers histories into replayable datasets."""

    def build(
        self,
        output_dir: str,
        scanner_history_path: Optional[str] = None,
        movers_history_path: Optional[str] = None,
        instrument: Optional[str] = None,
        min_points: int = 8,
    ) -> NativeDatasetBuildReport:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        grouped: Dict[str, Dict[int, Dict[str, Any]]] = {}
        source_types: Dict[str, set[str]] = {}

        if movers_history_path:
            for scan in self._load_history(movers_history_path):
                self._merge_movers_scan(
                    scan=scan,
                    grouped=grouped,
                    source_types=source_types,
                    instrument_filter=instrument,
                )

        if scanner_history_path:
            for scan in self._load_history(scanner_history_path):
                self._merge_scanner_scan(
                    scan=scan,
                    grouped=grouped,
                    source_types=source_types,
                    instrument_filter=instrument,
                )

        dataset_artifacts: List[NativeDatasetArtifact] = []
        skipped: Dict[str, str] = {}
        for resolved_instrument, rows_by_ts in sorted(grouped.items()):
            rows = [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
            if len(rows) < min_points:
                skipped[resolved_instrument] = f"insufficient_points:{len(rows)}<{min_points}"
                continue

            dataset_path = output_path / f"{_slugify_instrument(resolved_instrument)}.jsonl"
            dataset_path.write_text("\n".join(json.dumps(row) for row in rows))
            dataset_artifacts.append(NativeDatasetArtifact(
                instrument=resolved_instrument,
                rows=len(rows),
                source_types=sorted(source_types.get(resolved_instrument, set())),
                path=str(dataset_path),
            ))

        return NativeDatasetBuildReport(
            output_dir=str(output_path),
            dataset_artifacts=dataset_artifacts,
            skipped_instruments=skipped,
        )

    @staticmethod
    def _load_history(path: str) -> List[Dict[str, Any]]:
        raw = Path(path).read_text().strip()
        if not raw:
            return []
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError(f"History file must contain a list: {path}")
        return data

    def _merge_movers_scan(
        self,
        scan: Dict[str, Any],
        grouped: Dict[str, Dict[int, Dict[str, Any]]],
        source_types: Dict[str, set[str]],
        instrument_filter: Optional[str],
    ) -> None:
        scan_time_ms = int(scan.get("scan_time_ms", 0) or 0)
        signals_by_asset = {
            str(item.get("asset", "")): item
            for item in scan.get("signals", [])
            if item.get("asset")
        }
        for snapshot in scan.get("snapshots", []):
            asset = str(snapshot.get("asset", ""))
            if not asset:
                continue
            resolved_instrument = _asset_to_instrument(asset)
            if instrument_filter and resolved_instrument != instrument_filter:
                continue

            timestamp_ms = int(snapshot.get("timestamp_ms", scan_time_ms) or scan_time_ms)
            row = self._get_or_create_row(grouped, resolved_instrument, timestamp_ms)
            row.update({
                "mid_price": float(snapshot.get("mark_price", 0) or 0),
                "bid": float(snapshot.get("mark_price", 0) or 0),
                "ask": float(snapshot.get("mark_price", 0) or 0),
                "spread_bps": 0.0,
                "volume_24h": float(snapshot.get("volume_24h", 0) or 0),
                "funding_rate": float(snapshot.get("funding_rate", 0) or 0),
                "open_interest": float(snapshot.get("open_interest", 0) or 0),
                "source_movers": True,
            })

            signal = signals_by_asset.get(asset)
            if signal:
                row.update({
                    "mover_signal_type": str(signal.get("signal_type", "")),
                    "mover_signal_direction": str(signal.get("direction", "")),
                    "mover_confidence": float(signal.get("confidence", 0) or 0),
                    "mover_oi_delta_pct": float(signal.get("oi_delta_pct", 0) or 0),
                    "mover_volume_surge_ratio": float(signal.get("volume_surge_ratio", 0) or 0),
                    "mover_price_change_pct": float(signal.get("price_change_pct", 0) or 0),
                })

            source_types.setdefault(resolved_instrument, set()).add("movers")

    def _merge_scanner_scan(
        self,
        scan: Dict[str, Any],
        grouped: Dict[str, Dict[int, Dict[str, Any]]],
        source_types: Dict[str, set[str]],
        instrument_filter: Optional[str],
    ) -> None:
        scan_time_ms = int(scan.get("scan_time_ms", 0) or 0)
        btc_macro = scan.get("btc_macro", {}) or {}
        for opportunity in scan.get("opportunities", []):
            asset = str(opportunity.get("asset", ""))
            if not asset:
                continue
            resolved_instrument = _asset_to_instrument(asset)
            if instrument_filter and resolved_instrument != instrument_filter:
                continue

            market_data = opportunity.get("market_data", {}) or {}
            row = self._get_or_create_row(grouped, resolved_instrument, scan_time_ms)
            row.update({
                "mid_price": float(market_data.get("mark_price", row.get("mid_price", 0)) or 0),
                "bid": float(market_data.get("mark_price", row.get("bid", 0)) or 0),
                "ask": float(market_data.get("mark_price", row.get("ask", 0)) or 0),
                "spread_bps": 0.0,
                "volume_24h": float(market_data.get("vol24h", row.get("volume_24h", 0)) or 0),
                "funding_rate": float(market_data.get("funding_rate", row.get("funding_rate", 0)) or 0),
                "open_interest": float(market_data.get("oi", row.get("open_interest", 0)) or 0),
                "source_scanner": True,
                "scanner_score": float(opportunity.get("final_score", 0) or 0),
                "scanner_raw_score": float(opportunity.get("raw_score", 0) or 0),
                "scanner_direction": str(opportunity.get("direction", "")),
                "scanner_market_structure_score": float(
                    (opportunity.get("pillar_scores", {}) or {}).get("market_structure", 0) or 0
                ),
                "scanner_technicals_score": float(
                    (opportunity.get("pillar_scores", {}) or {}).get("technicals", 0) or 0
                ),
                "scanner_funding_score": float(
                    (opportunity.get("pillar_scores", {}) or {}).get("funding", 0) or 0
                ),
                "btc_macro_trend": str(btc_macro.get("trend", "")),
                "btc_macro_strength": float(btc_macro.get("strength", 0) or 0),
            })
            source_types.setdefault(resolved_instrument, set()).add("scanner")

    @staticmethod
    def _get_or_create_row(
        grouped: Dict[str, Dict[int, Dict[str, Any]]],
        instrument: str,
        timestamp_ms: int,
    ) -> Dict[str, Any]:
        by_ts = grouped.setdefault(instrument, {})
        if timestamp_ms not in by_ts:
            by_ts[timestamp_ms] = {
                "instrument": instrument,
                "timestamp_ms": timestamp_ms,
                "mid_price": 0.0,
                "bid": 0.0,
                "ask": 0.0,
                "spread_bps": 0.0,
                "volume_24h": 0.0,
                "funding_rate": 0.0,
                "open_interest": 0.0,
            }
        return by_ts[timestamp_ms]


class WolfTradeAttributionAnalyzer:
    """Summarize actual WOLF trade outcomes by source and instrument."""

    def analyze(self, trades_path: str) -> WolfEdgeReport:
        trades = self._load_trades(trades_path)
        pairs = JudgeEngine._pair_trades(trades)
        return WolfEdgeReport(
            source_path=trades_path,
            total_trades=len(trades),
            total_round_trips=len(pairs),
            by_source=self._summarize(
                pairs,
                key_fn=lambda pair: str(pair.get("entry_source", "unknown") or "unknown"),
                include_instruments=True,
            ),
            by_instrument=self._summarize(
                pairs,
                key_fn=lambda pair: str(pair.get("instrument", "") or "unknown"),
                include_instruments=False,
            ),
            thresholds=self._recommend_thresholds(pairs),
        )

    def save_report(self, report: WolfEdgeReport, output_dir: str) -> Path:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        path = output_path / "wolf_edge_report.json"
        path.write_text(json.dumps(report.to_dict(), indent=2))
        return path

    @staticmethod
    def _load_trades(path: str) -> List[Dict[str, Any]]:
        trades: List[Dict[str, Any]] = []
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if line:
                trades.append(json.loads(line))
        return trades

    @staticmethod
    def _summarize(
        pairs: Sequence[Dict[str, Any]],
        key_fn,
        include_instruments: bool,
    ) -> List[WolfEdgeSummary]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for pair in pairs:
            grouped.setdefault(key_fn(pair), []).append(pair)

        summaries: List[WolfEdgeSummary] = []
        for key, items in grouped.items():
            wins = sum(1 for item in items if float(item.get("pnl", 0) or 0) > 0)
            summaries.append(WolfEdgeSummary(
                key=key,
                round_trips=len(items),
                win_rate=(wins / len(items) * 100.0) if items else 0.0,
                net_pnl=sum(float(item.get("pnl", 0) or 0) for item in items),
                avg_roe_pct=_avg(float(item.get("roe_pct", 0) or 0) for item in items),
                avg_entry_score=_avg(float(item.get("entry_score", 0) or 0) for item in items),
                avg_holding_ms=_avg(float(item.get("holding_ms", 0) or 0) for item in items),
                instruments=sorted({str(item.get("instrument", "")) for item in items}) if include_instruments else [],
            ))
        summaries.sort(key=lambda item: (-item.net_pnl, -item.win_rate, item.key))
        return summaries

    def _recommend_thresholds(self, pairs: Sequence[Dict[str, Any]]) -> List[WolfSignalThreshold]:
        thresholds: List[WolfSignalThreshold] = []
        thresholds.extend(self._build_thresholds(
            pairs,
            key_fn=lambda pair: (
                _normalize_entry_source(str(pair.get("entry_source", "unknown") or "unknown")),
                str(pair.get("instrument", "") or "*"),
            ),
        ))
        thresholds.extend(self._build_thresholds(
            pairs,
            key_fn=lambda pair: (
                _normalize_entry_source(str(pair.get("entry_source", "unknown") or "unknown")),
                "*",
            ),
        ))
        thresholds.sort(key=lambda item: (item.source, item.instrument))
        return thresholds

    def _build_thresholds(self, pairs: Sequence[Dict[str, Any]], key_fn) -> List[WolfSignalThreshold]:
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for pair in pairs:
            source, instrument = key_fn(pair)
            if not source:
                continue
            grouped.setdefault((source, instrument), []).append(pair)

        thresholds: List[WolfSignalThreshold] = []
        for (source, instrument), items in grouped.items():
            scored_items = [
                item for item in items
                if float(item.get("entry_score", 0) or 0) > 0
            ]
            if not scored_items:
                continue

            profitable_scores = [
                float(item.get("entry_score", 0) or 0)
                for item in scored_items
                if float(item.get("pnl", 0) or 0) > 0
            ]
            losing_scores = [
                float(item.get("entry_score", 0) or 0)
                for item in scored_items
                if float(item.get("pnl", 0) or 0) <= 0
            ]
            if not profitable_scores:
                continue

            baseline = _percentile(sorted(profitable_scores), 0.25)
            reasons = ["profitable_score_floor_p25"]
            if len(profitable_scores) == 1:
                baseline = profitable_scores[0]
                reasons = ["single_profitable_score_floor"]
            if losing_scores:
                baseline = max(baseline, _avg(losing_scores))
                reasons.append("floored_above_avg_losing_score")

            thresholds.append(WolfSignalThreshold(
                source=source,
                instrument=instrument,
                round_trips=len(items),
                profitable_samples=len(profitable_scores),
                losing_samples=len(losing_scores),
                min_signal_score=baseline,
                avg_profitable_entry_score=_avg(profitable_scores),
                avg_losing_entry_score=_avg(losing_scores),
                reasons=reasons,
            ))
        return thresholds


def _asset_to_instrument(asset: str) -> str:
    asset = asset.strip()
    if not asset:
        return ""
    if asset.endswith("-PERP") or asset.endswith("-USDYP"):
        return asset
    return f"{asset}-PERP"


def _slugify_instrument(instrument: str) -> str:
    return instrument.lower().replace(":", "_").replace("-", "_")


def _avg(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals) if vals else 0.0


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    index = (len(values) - 1) * max(0.0, min(quantile, 1.0))
    low = int(index)
    high = min(low + 1, len(values) - 1)
    fraction = index - low
    return float(values[low] + (values[high] - values[low]) * fraction)


def _normalize_entry_source(source: str) -> str:
    if source.startswith("smart_money:"):
        return "smart_money"
    return source
