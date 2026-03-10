"""hl paper — paper-mode validation and reporting."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional

import typer

paper_app = typer.Typer(no_args_is_help=True)


@paper_app.command("validate")
def paper_validate(
    data_dir: List[str] = typer.Option(
        [
            "data/cli/carry/funding_arb",
            "data/cli/carry/basis_arb",
            "data/cli/carry/hedge_agent",
        ],
        "--data-dir",
        help="Data directory with trades.jsonl (repeatable).",
    ),
    since: Optional[str] = typer.Option(
        None,
        "--since",
        help="Only include trades after this date (YYYY-MM-DD).",
    ),
    output_dir: str = typer.Option(
        "data/howl",
        "--output-dir",
        help="Output directory for paper validation reports.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
):
    """Run HOWL across multiple strategy data dirs and emit a combined report."""
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from modules.paper_validation import run_paper_validation

    try:
        result = run_paper_validation(data_dirs=data_dir, output_dir=output_dir, since=since)
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.to_dict(), indent=2))
        return

    combined = result.combined
    typer.echo(f"Paper validation: {result.generated_at}")
    typer.echo(
        f"Combined: trades={combined.get('total_trades')} "
        f"round_trips={combined.get('total_round_trips')} "
        f"win_rate={combined.get('win_rate')}% "
        f"net_pnl=${combined.get('net_pnl')} "
        f"fdr={combined.get('fdr')}%"
    )
    typer.echo(f"Report: {result.report_path}")
    typer.echo(f"JSON: {result.json_path}")
