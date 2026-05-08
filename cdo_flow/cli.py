from __future__ import annotations

import sys
from pathlib import Path

import click
from rich.console import Console

console = Console()


@click.group()
@click.version_option(package_name="cdo-flow")
def cli() -> None:
    """cdo-flow: CDO-based climate analysis workflow orchestration."""


@cli.command()
@click.argument("workflow_file", type=click.Path(exists=True, path_type=Path))
@click.option("--backend", default="local", show_default=True, help="Execution backend.")
@click.option("--params", "-p", multiple=True, metavar="KEY=VAL", help="Workflow parameters.")
@click.option("--inputs", "-i", multiple=True, metavar="KEY=PATH", help="Workflow inputs.")
@click.option("--dry-run", is_flag=True, help="Print execution plan without running.")
@click.option("--max-workers", type=int, default=None, help="Max parallel workers.")
@click.option("--run-id", default=None, help="Custom run identifier.")
def run(
    workflow_file: Path,
    backend: str,
    params: tuple[str, ...],
    inputs: tuple[str, ...],
    dry_run: bool,
    max_workers: int | None,
    run_id: str | None,
) -> None:
    """Run a workflow from a YAML file."""
    from cdo_flow.config.loader import load_workflow

    wf = load_workflow(workflow_file)

    parsed_params = _parse_key_val(params)
    parsed_inputs = _parse_key_val(inputs)

    if dry_run:
        wf.dry_run(inputs=parsed_inputs, params=parsed_params)
        return

    result = wf.run(
        inputs=parsed_inputs,
        params=parsed_params,
        backend=backend,
        max_workers=max_workers,
        run_id=run_id,
    )

    if result:
        console.print(f"\n[bold green]SUCCESS[/bold green] — run dir: {result.run_dir}")
    else:
        console.print(f"\n[bold red]FAILED[/bold red] — failed steps: {result.failed_steps}")
        sys.exit(1)


@cli.command()
@click.argument("workflow_file", type=click.Path(exists=True, path_type=Path))
def validate(workflow_file: Path) -> None:
    """Validate a workflow YAML file."""
    from cdo_flow.config.loader import load_workflow
    from cdo_flow.exceptions import WorkflowValidationError

    try:
        wf = load_workflow(workflow_file)
    except WorkflowValidationError as e:
        console.print(f"[bold red]Validation failed:[/bold red]")
        for err in e.errors:
            console.print(f"  [red]•[/red] {err}")
        sys.exit(1)

    errors = wf.validate()
    if errors:
        console.print("[bold red]Validation failed:[/bold red]")
        for err in errors:
            console.print(f"  [red]•[/red] {err}")
        sys.exit(1)

    console.print(f"[bold green]Workflow is valid.[/bold green] ({len(wf._steps)} steps)")


def _parse_key_val(pairs: tuple[str, ...]) -> dict[str, str]:
    result = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.BadParameter(f"Expected KEY=VALUE, got: {pair!r}")
        k, _, v = pair.partition("=")
        result[k.strip()] = v.strip()
    return result
