"""Orchestrates historical strategy evaluation and allocation reports."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from cli.strategy_registry import STRATEGY_REGISTRY, resolve_strategy_path
from modules.replay_engine import WalkForwardEvaluator, WalkForwardFoldResult
from modules.research_pipeline import (
    AllocationDecision,
    HistoricalDataset,
    HistoricalDatasetLoader,
    StrategyCapitalAllocator,
    StrategyScorecard,
    aggregate_walk_forward_results,
)
from modules.routing_policy import ResearchRoutingPolicyBuilder
from sdk.strategy_sdk.loader import load_strategy


@dataclass
class DatasetResearchResult:
    dataset: str
    instrument: str
    folds: int
    scorecard: Optional[StrategyScorecard] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "instrument": self.instrument,
            "folds": self.folds,
            "scorecard": asdict(self.scorecard) if self.scorecard else None,
        }


@dataclass
class StrategyResearchResult:
    strategy_id: str
    strategy_path: str
    datasets: List[str] = field(default_factory=list)
    fold_results: List[WalkForwardFoldResult] = field(default_factory=list)
    dataset_breakdown: List[DatasetResearchResult] = field(default_factory=list)
    scorecard: Optional[StrategyScorecard] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_path": self.strategy_path,
            "datasets": list(self.datasets),
            "dataset_breakdown": [item.to_dict() for item in self.dataset_breakdown],
            "fold_results": [
                {
                    "fold": asdict(result.fold),
                    "validation_regime": result.validation_regime,
                    "train": {
                        "ticks": result.train.ticks,
                        "total_pnl": result.train.total_pnl,
                        "max_drawdown": result.train.max_drawdown,
                        "metrics": asdict(result.train.metrics),
                    },
                    "validation": {
                        "ticks": result.validation.ticks,
                        "total_pnl": result.validation.total_pnl,
                        "max_drawdown": result.validation.max_drawdown,
                        "metrics": asdict(result.validation.metrics),
                    },
                }
                for result in self.fold_results
            ],
            "scorecard": asdict(self.scorecard) if self.scorecard else None,
        }


@dataclass
class ResearchSummary:
    deployable_capital_usd: float
    reserved_capital_usd: float
    allocated_capital_usd: float
    enabled_strategies: int
    disabled_strategies: int
    top_strategy_id: str = ""
    top_score: float = 0.0


@dataclass
class ResearchReport:
    generated_at: str
    datasets: List[Dict[str, Any]]
    strategy_results: List[StrategyResearchResult]
    allocations: List[AllocationDecision]
    summary: ResearchSummary
    config: Dict[str, Any]
    artifacts: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "datasets": self.datasets,
            "strategy_results": [result.to_dict() for result in self.strategy_results],
            "allocations": [asdict(a) for a in self.allocations],
            "summary": asdict(self.summary),
            "config": self.config,
            "artifacts": self.artifacts,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResearchReport":
        strategy_results = []
        for item in data.get("strategy_results", []):
            scorecard = item.get("scorecard")
            dataset_breakdown = []
            for dataset_item in item.get("dataset_breakdown", []):
                dataset_scorecard = dataset_item.get("scorecard")
                dataset_breakdown.append(DatasetResearchResult(
                    dataset=dataset_item.get("dataset", ""),
                    instrument=dataset_item.get("instrument", ""),
                    folds=dataset_item.get("folds", 0),
                    scorecard=StrategyScorecard(**dataset_scorecard) if dataset_scorecard else None,
                ))
            strategy_results.append(StrategyResearchResult(
                strategy_id=item.get("strategy_id", ""),
                strategy_path=item.get("strategy_path", ""),
                datasets=item.get("datasets", []),
                fold_results=[],
                dataset_breakdown=dataset_breakdown,
                scorecard=StrategyScorecard(**scorecard) if scorecard else None,
            ))
        allocations = [AllocationDecision(**item) for item in data.get("allocations", [])]
        summary_data = data.get("summary", {})
        summary = ResearchSummary(
            deployable_capital_usd=summary_data.get("deployable_capital_usd", 0.0),
            reserved_capital_usd=summary_data.get("reserved_capital_usd", 0.0),
            allocated_capital_usd=summary_data.get("allocated_capital_usd", 0.0),
            enabled_strategies=summary_data.get("enabled_strategies", 0),
            disabled_strategies=summary_data.get("disabled_strategies", 0),
            top_strategy_id=summary_data.get("top_strategy_id", ""),
            top_score=summary_data.get("top_score", 0.0),
        )
        return cls(
            generated_at=data.get("generated_at", ""),
            datasets=data.get("datasets", []),
            strategy_results=strategy_results,
            allocations=allocations,
            summary=summary,
            config=data.get("config", {}),
            artifacts=data.get("artifacts", {}),
        )


class ResearchRunner:
    """Evaluate strategies over historical datasets and emit an allocation report."""

    def __init__(
        self,
        data_dir: str = "data/research",
        dataset_loader: Optional[HistoricalDatasetLoader] = None,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.dataset_loader = dataset_loader or HistoricalDatasetLoader()

    def run(
        self,
        strategy_names: Sequence[str],
        dataset_paths: Sequence[str],
        train_size: int,
        validation_size: int,
        step_size: Optional[int],
        capital_usd: float,
        reserve_pct: float,
        max_strategy_pct: float,
        instrument: Optional[str] = None,
        wolf_edge_report_path: Optional[str] = None,
    ) -> ResearchReport:
        datasets = [
            self.dataset_loader.load(path, instrument=instrument)
            for path in dataset_paths
        ]
        edge_report = self._load_optional_json(wolf_edge_report_path)

        scorer = __import__("modules.research_pipeline", fromlist=["StrategyScorer"]).StrategyScorer()
        allocator = StrategyCapitalAllocator(
            total_capital_usd=capital_usd,
            reserve_pct=reserve_pct,
            max_strategy_pct=max_strategy_pct,
        )

        strategy_results: List[StrategyResearchResult] = []
        for strategy_name in strategy_names:
            strategy_path = resolve_strategy_path(strategy_name)
            strategy_cls = load_strategy(strategy_path)
            default_params = dict(STRATEGY_REGISTRY.get(strategy_name, {}).get("params", {}))

            fold_results: List[WalkForwardFoldResult] = []
            dataset_names: List[str] = []
            dataset_breakdown: List[DatasetResearchResult] = []
            for dataset in datasets:
                evaluator = WalkForwardEvaluator(
                    strategy_factory=lambda cls=strategy_cls, params=default_params, sid=strategy_name: cls(
                        strategy_id=sid,
                        **params,
                    ),
                    instrument=dataset.instrument,
                )
                results = evaluator.evaluate(
                    dataset.snapshots,
                    train_size=train_size,
                    validation_size=validation_size,
                    step_size=step_size,
                )
                if results:
                    fold_results.extend(results)
                    dataset_names.append(dataset.name)
                dataset_aggregate = aggregate_walk_forward_results(results)
                dataset_breakdown.append(DatasetResearchResult(
                    dataset=dataset.name,
                    instrument=dataset.instrument,
                    folds=len(results),
                    scorecard=scorer.score(f"{strategy_name}@{dataset.name}", dataset_aggregate),
                ))

            aggregate = aggregate_walk_forward_results(fold_results)
            scorecard = scorer.score(strategy_name, aggregate)
            strategy_results.append(StrategyResearchResult(
                strategy_id=strategy_name,
                strategy_path=strategy_path,
                datasets=dataset_names,
                fold_results=fold_results,
                dataset_breakdown=dataset_breakdown,
                scorecard=scorecard,
            ))

        allocations = allocator.allocate([
            result.scorecard for result in strategy_results if result.scorecard is not None
        ])

        summary = self._build_summary(
            allocations=allocations,
            capital_usd=capital_usd,
            reserve_pct=reserve_pct,
            strategy_results=strategy_results,
        )

        report = ResearchReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            datasets=[self._dataset_summary(dataset) for dataset in datasets],
            strategy_results=sorted(
                strategy_results,
                key=lambda item: (-(item.scorecard.score if item.scorecard else 0.0), item.strategy_id),
            ),
            allocations=allocations,
            summary=summary,
            config={
                "train_size": train_size,
                "validation_size": validation_size,
                "step_size": step_size or validation_size,
                "capital_usd": capital_usd,
                "reserve_pct": reserve_pct,
                "max_strategy_pct": max_strategy_pct,
                "instrument": instrument or "",
                "wolf_edge_report_path": wolf_edge_report_path or "",
            },
        )
        report_path = self.save_report(report)
        snapshot_path = self.save_deployment_snapshot(
            report,
            report_path=report_path,
            edge_report=edge_report,
        )
        report.artifacts["report_path"] = str(report_path)
        report.artifacts["deployment_snapshot_path"] = str(snapshot_path)
        if wolf_edge_report_path:
            report.artifacts["wolf_edge_report_path"] = wolf_edge_report_path
        report_path.write_text(json.dumps(report.to_dict(), indent=2, default=str))
        return report

    def save_report(self, report: ResearchReport) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        path = self.data_dir / f"{ts}.json"
        path.write_text(json.dumps(report.to_dict(), indent=2, default=str))
        return path

    def save_deployment_snapshot(
        self,
        report: ResearchReport,
        report_path: Optional[Path] = None,
        edge_report: Optional[Dict[str, Any]] = None,
    ) -> Path:
        snapshot_path = self.data_dir / "latest_allocation.json"
        payload = {
            "generated_at": report.generated_at,
            "source_report": str(report_path) if report_path else "",
            "summary": asdict(report.summary),
            "config": report.config,
            "strategies": [
                {
                    "strategy_id": result.strategy_id,
                    "scorecard": asdict(result.scorecard) if result.scorecard else None,
                    "allocation": next(
                        (
                            asdict(item) for item in report.allocations
                            if item.strategy_id == result.strategy_id
                        ),
                        None,
                    ),
                }
                for result in report.strategy_results
            ],
            "routing_policy": ResearchRoutingPolicyBuilder().build(
                generated_at=report.generated_at,
                strategy_results=report.strategy_results,
                allocations=[asdict(item) for item in report.allocations],
                edge_report=edge_report,
            ).to_dict(),
            "active_allocations": [
                asdict(item) for item in report.allocations if item.enabled and item.capital_usd > 0
            ],
            "disabled_allocations": [
                asdict(item) for item in report.allocations if not item.enabled or item.capital_usd <= 0
            ],
        }
        snapshot_path.write_text(json.dumps(payload, indent=2, default=str))
        return snapshot_path

    def latest_report(self) -> Optional[ResearchReport]:
        files = sorted(self.data_dir.glob("*.json"), reverse=True)
        for path in files:
            if path.name == "latest_allocation.json":
                continue
            data = json.loads(path.read_text())
            if "strategy_results" in data and "allocations" in data:
                return ResearchReport.from_dict(data)
        return None

    @staticmethod
    def _load_optional_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
        if not path:
            return None
        return json.loads(Path(path).read_text())

    @staticmethod
    def _dataset_summary(dataset: HistoricalDataset) -> Dict[str, Any]:
        return {
            "name": dataset.name,
            "instrument": dataset.instrument,
            "rows": len(dataset.snapshots),
            "source_path": dataset.source_path,
            "metadata": dataset.metadata,
        }

    @staticmethod
    def _build_summary(
        allocations: Sequence[AllocationDecision],
        capital_usd: float,
        reserve_pct: float,
        strategy_results: Sequence[StrategyResearchResult],
    ) -> ResearchSummary:
        allocated_capital = sum(item.capital_usd for item in allocations)
        enabled = sum(1 for item in allocations if item.enabled and item.capital_usd > 0)
        disabled = max(len(strategy_results) - enabled, 0)
        top = next(
            iter(
                sorted(
                    (
                        item.scorecard for item in strategy_results
                        if item.scorecard is not None
                    ),
                    key=lambda scorecard: (-scorecard.score, scorecard.strategy_id),
                )
            ),
            None,
        )
        return ResearchSummary(
            deployable_capital_usd=round(capital_usd * max(0.0, 1.0 - reserve_pct), 2),
            reserved_capital_usd=round(capital_usd * max(reserve_pct, 0.0), 2),
            allocated_capital_usd=round(allocated_capital, 2),
            enabled_strategies=enabled,
            disabled_strategies=disabled,
            top_strategy_id=top.strategy_id if top else "",
            top_score=top.score if top else 0.0,
        )
