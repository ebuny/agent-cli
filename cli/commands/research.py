"""hl research — offline historical evaluation and capital allocation."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional

import typer

research_app = typer.Typer(no_args_is_help=True)


@research_app.command("ingest")
def research_ingest(
    scanner_history: Optional[Path] = typer.Option(
        None,
        "--scanner-history",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Scanner history JSON to convert into replayable datasets.",
    ),
    movers_history: Optional[Path] = typer.Option(
        None,
        "--movers-history",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Movers history JSON to convert into replayable datasets.",
    ),
    wolf_trades: Optional[Path] = typer.Option(
        None,
        "--wolf-trades",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="WOLF trades.jsonl to summarize into edge attribution.",
    ),
    instrument: Optional[str] = typer.Option(None, "--instrument", "-i", help="Only emit artifacts for one instrument"),
    min_points: int = typer.Option(8, "--min-points", help="Minimum rows required to write a dataset"),
    output_dir: str = typer.Option("data/research/native", "--output-dir", help="Directory for generated datasets and reports"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON"),
):
    """Convert native scanner/movers/WOLF history into research artifacts."""
    if not scanner_history and not movers_history and not wolf_trades:
        typer.echo("Provide at least one of --scanner-history, --movers-history, or --wolf-trades.")
        raise typer.Exit(1)

    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from modules.native_history_research import (
        NativeHistoryResearchBuilder,
        WolfTradeAttributionAnalyzer,
    )

    payload = {
        "datasets": [],
        "skipped_instruments": {},
        "wolf_edge_report": None,
        "artifacts": {},
    }

    if scanner_history or movers_history:
        datasets_dir = Path(output_dir) / "datasets"
        build_report = NativeHistoryResearchBuilder().build(
            output_dir=str(datasets_dir),
            scanner_history_path=str(scanner_history) if scanner_history else None,
            movers_history_path=str(movers_history) if movers_history else None,
            instrument=instrument,
            min_points=min_points,
        )
        payload["datasets"] = [item.to_dict() for item in build_report.dataset_artifacts]
        payload["skipped_instruments"] = dict(build_report.skipped_instruments)
        payload["artifacts"]["datasets_dir"] = build_report.output_dir

    if wolf_trades:
        analyzer = WolfTradeAttributionAnalyzer()
        edge_report = analyzer.analyze(str(wolf_trades))
        edge_report_path = analyzer.save_report(edge_report, output_dir=output_dir)
        payload["wolf_edge_report"] = edge_report.to_dict()
        payload["artifacts"]["wolf_edge_report_path"] = str(edge_report_path)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    ingest_report_path = output_path / "ingest_report.json"
    ingest_report_path.write_text(json.dumps(payload, indent=2))
    payload["artifacts"]["ingest_report_path"] = str(ingest_report_path)

    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return

    if payload["datasets"]:
        typer.echo(f"Datasets written: {len(payload['datasets'])}")
        typer.echo(f"Dataset dir: {payload['artifacts'].get('datasets_dir', output_dir)}")
        typer.echo(f"Ingest report: {payload['artifacts'].get('ingest_report_path', '')}")
        for item in payload["datasets"]:
            typer.echo(
                f"{item['instrument']}: rows={item['rows']} "
                f"sources={','.join(item['source_types'])} path={item['path']}"
            )
    elif scanner_history or movers_history:
        typer.echo("No datasets written.")

    if payload["skipped_instruments"]:
        typer.echo("")
        typer.echo("Skipped instruments:")
        for name, reason in sorted(payload["skipped_instruments"].items()):
            typer.echo(f"{name}: {reason}")

    if payload["wolf_edge_report"]:
        report = payload["wolf_edge_report"]
        typer.echo("")
        typer.echo(
            f"WOLF edge report: trades={report['total_trades']} "
            f"round_trips={report['total_round_trips']}"
        )
        typer.echo(f"Ingest report: {payload['artifacts'].get('ingest_report_path', '')}")
        typer.echo(f"Saved: {payload['artifacts'].get('wolf_edge_report_path', '')}")
        for item in report["by_source"]:
            typer.echo(
                f"{item['key']}: round_trips={item['round_trips']} "
                f"win_rate={item['win_rate']:.2f}% net_pnl={item['net_pnl']:.2f}"
            )


@research_app.command("run")
def research_run(
    strategy: List[str] = typer.Option(
        ...,
        "--strategy",
        "-s",
        help="Strategy name to evaluate. Repeat for multiple strategies.",
    ),
    dataset: List[Path] = typer.Option(
        ...,
        "--dataset",
        "-d",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Historical dataset path (.csv, .json, .jsonl). Repeat for multiple datasets.",
    ),
    train_size: int = typer.Option(200, "--train-size", help="Training window size in snapshots"),
    validation_size: int = typer.Option(50, "--validation-size", help="Validation window size in snapshots"),
    step_size: Optional[int] = typer.Option(None, "--step-size", help="Walk-forward step size (default: validation size)"),
    capital_usd: float = typer.Option(10_000.0, "--capital", help="Capital pool for allocation recommendations"),
    reserve_pct: float = typer.Option(0.1, "--reserve-pct", help="Reserve capital percentage"),
    max_strategy_pct: float = typer.Option(0.5, "--max-strategy-pct", help="Max allocation per strategy"),
    instrument: Optional[str] = typer.Option(None, "--instrument", "-i", help="Force instrument for all datasets"),
    wolf_edge_report: Optional[Path] = typer.Option(
        None,
        "--wolf-edge-report",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional WOLF edge attribution report to derive empirical signal-quality floors.",
    ),
    data_dir: str = typer.Option("data/research", "--data-dir", help="Directory for saved research reports"),
    json_output: bool = typer.Option(False, "--json", help="Emit report JSON"),
):
    """Evaluate strategies on historical data and emit ranked allocations."""
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from modules.research_runner import ResearchRunner

    runner = ResearchRunner(data_dir=data_dir)
    report = runner.run(
        strategy_names=strategy,
        dataset_paths=[str(path) for path in dataset],
        train_size=train_size,
        validation_size=validation_size,
        step_size=step_size,
        capital_usd=capital_usd,
        reserve_pct=reserve_pct,
        max_strategy_pct=max_strategy_pct,
        instrument=instrument,
        wolf_edge_report_path=str(wolf_edge_report) if wolf_edge_report else None,
    )

    if json_output:
        typer.echo(json.dumps(report.to_dict(), indent=2))
        return

    typer.echo(f"Research run: {report.generated_at}")
    typer.echo(f"Datasets: {len(report.datasets)}  |  Strategies: {len(report.strategy_results)}")
    typer.echo(
        f"Deployable: ${report.summary.deployable_capital_usd:,.2f}  |  "
        f"Allocated: ${report.summary.allocated_capital_usd:,.2f}  |  "
        f"Reserve: ${report.summary.reserved_capital_usd:,.2f}"
    )
    if report.summary.top_strategy_id:
        typer.echo(
            f"Top strategy: {report.summary.top_strategy_id} "
            f"(score={report.summary.top_score:.2f})"
        )
    if report.artifacts.get("deployment_snapshot_path"):
        typer.echo(f"Deployment snapshot: {report.artifacts['deployment_snapshot_path']}")
    typer.echo("")

    typer.echo(f"{'Strategy':<22} {'Score':<8} {'Enabled':<8} {'AvgPnL':<10} {'DD':<10} {'PF':<8} {'FDR':<8} {'PosFolds':<9}")
    typer.echo("-" * 95)
    for result in report.strategy_results:
        score = result.scorecard
        if score is None:
            continue
        typer.echo(
            f"{result.strategy_id:<22} {score.score:<8.2f} {str(score.enabled):<8} "
            f"{score.avg_validation_pnl:<10.2f} {score.avg_validation_drawdown:<10.2f} "
            f"{score.avg_validation_profit_factor:<8.2f} {score.avg_validation_fdr:<8.2f} "
            f"{score.positive_fold_ratio:<9.2f}"
        )
        typer.echo(
            f"  regimes={','.join(score.supported_regimes) or '-'} "
            f"dominant={score.dominant_regime or '-'} breadth={score.regime_breadth}"
        )
        for dataset_result in result.dataset_breakdown:
            dataset_score = dataset_result.scorecard
            if dataset_score is None:
                continue
            typer.echo(
                f"  {dataset_result.dataset:<20} score={dataset_score.score:<7.2f} "
                f"folds={dataset_result.folds:<3} pnl={dataset_score.avg_validation_pnl:<9.2f} "
                f"enabled={dataset_score.enabled}"
            )

    typer.echo("")
    typer.echo(f"{'Allocation':<22} {'Capital':<12} {'Weight':<8} {'Enabled':<8} {'Reasons'}")
    typer.echo("-" * 95)
    for allocation in report.allocations:
        typer.echo(
            f"{allocation.strategy_id:<22} ${allocation.capital_usd:<11,.2f} {allocation.weight:<8.2f} "
            f"{str(allocation.enabled):<8} {', '.join(allocation.reasons)}"
        )


@research_app.command("allocate-live")
def research_allocate_live(
    snapshot: Path = typer.Option(
        Path("data/research/latest_allocation.json"),
        "--snapshot",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Research allocation snapshot generated by `research run`.",
    ),
    dataset: Optional[Path] = typer.Option(
        None,
        "--dataset",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Recent market dataset used to classify the current regime.",
    ),
    regime: Optional[str] = typer.Option(None, "--regime", help="Override current regime label"),
    recent_window: int = typer.Option(12, "--recent-window", help="Number of recent snapshots to inspect for regime detection"),
    output_path: str = typer.Option("data/research/live_allocation_plan.json", "--output-path", help="Path for saved live allocation plan"),
    feedback_enable: bool = typer.Option(
        True,
        "--feedback/--no-feedback",
        help="Apply HOWL/Judge feedback to the live plan.",
    ),
    feedback_trades: Optional[Path] = typer.Option(
        Path("data/wolf/trades.jsonl"),
        "--feedback-trades",
        help="Trades JSONL used for HOWL feedback adjustments.",
    ),
    feedback_judge: Optional[Path] = typer.Option(
        None,
        "--feedback-judge",
        help="Optional judge report JSON to apply source-level gating.",
    ),
    feedback_min_round_trips: int = typer.Option(
        5,
        "--feedback-min-round-trips",
        help="Minimum round trips required before feedback is applied.",
    ),
    feedback_fp_block: float = typer.Option(
        60.0,
        "--feedback-fp-block",
        help="False-positive rate threshold to block a signal source.",
    ),
    feedback_fp_reduce: float = typer.Option(
        40.0,
        "--feedback-fp-reduce",
        help="False-positive rate threshold to reduce capital allocation.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON"),
):
    """Filter a research snapshot into a live deployment plan for the current regime."""
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from modules.live_allocator import LiveAllocator

    allocator = LiveAllocator()
    plan = allocator.plan(
        snapshot_path=str(snapshot),
        dataset_path=str(dataset) if dataset else None,
        regime_override=regime,
        recent_window=recent_window,
        feedback_enable=feedback_enable,
        feedback_trades_path=str(feedback_trades) if feedback_trades else None,
        feedback_judge_path=str(feedback_judge) if feedback_judge else None,
        feedback_min_round_trips=feedback_min_round_trips,
        feedback_fp_block_threshold=feedback_fp_block,
        feedback_fp_reduce_threshold=feedback_fp_reduce,
    )
    allocator.save_plan(plan, output_path=output_path)

    if json_output:
        typer.echo(json.dumps(plan.to_dict(), indent=2))
        return

    typer.echo(f"Current regime: {plan.current_regime.label}")
    typer.echo(
        f"Deployable: ${plan.deployable_capital_usd:,.2f}  |  "
        f"Allocated: ${plan.allocated_capital_usd:,.2f}  |  "
        f"Enabled strategies: {plan.enabled_strategies}"
    )
    typer.echo(f"Routing rules: {len(plan.routing_policy.rules)}")
    if plan.feedback:
        feedback = plan.feedback
        blocked = ",".join(feedback.blocked_sources) if feedback.blocked_sources else "-"
        reasons = ", ".join(feedback.reasons) if feedback.reasons else "-"
        typer.echo(f"Feedback: capital x{feedback.capital_multiplier:.2f} blocked={blocked} reasons={reasons}")
    typer.echo(f"Plan saved: {output_path}")
    typer.echo("")
    for item in plan.decisions:
        typer.echo(
            f"{item.strategy_id}: capital=${item.capital_usd:,.2f} "
            f"enabled={item.enabled} regimes={','.join(item.supported_regimes) or '-'} "
            f"reasons={', '.join(item.reasons) if item.reasons else '-'}"
        )
    if plan.routing_policy.rules:
        typer.echo("")
        for rule in plan.routing_policy.rules:
            typer.echo(
                f"rule {rule.strategy_id}/{rule.source}: "
                f"instruments={','.join(rule.allowed_instruments) or '-'} "
                f"regimes={','.join(rule.allowed_regimes) or '-'} "
                f"min_score={rule.min_signal_score:.2f}"
            )


@research_app.command("status")
def research_status(
    data_dir: str = typer.Option("data/research", "--data-dir", help="Directory for saved research reports"),
    json_output: bool = typer.Option(False, "--json", help="Emit report JSON"),
):
    """Show the latest saved research report."""
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from modules.research_runner import ResearchRunner

    report = ResearchRunner(data_dir=data_dir).latest_report()
    if report is None:
        typer.echo("No research reports found.")
        raise typer.Exit(1)

    if json_output:
        typer.echo(json.dumps(report.to_dict(), indent=2))
        return

    typer.echo(f"Latest research report: {report.generated_at}")
    typer.echo(f"Datasets: {len(report.datasets)}  |  Strategies: {len(report.strategy_results)}")
    typer.echo(
        f"Deployable: ${report.summary.deployable_capital_usd:,.2f}  |  "
        f"Allocated: ${report.summary.allocated_capital_usd:,.2f}  |  "
        f"Reserve: ${report.summary.reserved_capital_usd:,.2f}"
    )
    if report.summary.top_strategy_id:
        typer.echo(
            f"Top strategy: {report.summary.top_strategy_id} "
            f"(score={report.summary.top_score:.2f})"
        )
    if report.artifacts.get("deployment_snapshot_path"):
        typer.echo(f"Deployment snapshot: {report.artifacts['deployment_snapshot_path']}")
    typer.echo("")
    for allocation in report.allocations:
        typer.echo(
            f"{allocation.strategy_id}: capital=${allocation.capital_usd:,.2f} "
            f"weight={allocation.weight:.2f} enabled={allocation.enabled}"
        )
