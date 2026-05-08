from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

_AT_REF = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)\.output$")


def _map_positional_inputs(files: tuple[str, ...], wf) -> dict[str, str]:
    """Map positional FILE args to named workflow input slots.

    Key order comes from the YAML ``inputs:`` list if declared, otherwise
    from first-occurrence of non-@ref input values across all steps.
    """
    if not files:
        return {}

    if wf.declared_inputs:
        keys = list(wf.declared_inputs)
    else:
        seen: set[str] = set()
        keys = []
        for step in wf._steps:
            for val in step.inputs.values():
                val_str = str(val).strip()
                if not _AT_REF.match(val_str) and val_str not in seen:
                    seen.add(val_str)
                    keys.append(val_str)

    if not keys:
        raise click.UsageError(
            "Cannot map positional files — no workflow inputs detected. "
            "Use -i key=path instead."
        )

    if len(files) > len(keys):
        raise click.UsageError(
            f"{len(files)} positional file(s) given but workflow only has "
            f"{len(keys)} input slot(s): {', '.join(keys)}"
        )

    return dict(zip(keys, files))


@click.group()
@click.version_option(package_name="cdo-flow")
def cli() -> None:
    """cdo-flow: CDO-based climate analysis workflow orchestration."""


@cli.command()
@click.argument("workflow_file", type=click.Path(exists=True, path_type=Path))
@click.argument("files", nargs=-1, type=click.Path())
@click.option("--backend", default="local", show_default=True, help="Execution backend.")
@click.option("--params", "-p", multiple=True, metavar="KEY=VAL", help="Workflow parameters.")
@click.option("--inputs", "-i", multiple=True, metavar="KEY=PATH", help="Workflow inputs (KEY=PATH).")
@click.option("--dry-run", is_flag=True, help="Print execution plan without running.")
@click.option("--max-workers", type=int, default=None, help="Max parallel workers.")
@click.option("--run-id", default=None, help="Custom run identifier.")
def run(
    workflow_file: Path,
    files: tuple[str, ...],
    backend: str,
    params: tuple[str, ...],
    inputs: tuple[str, ...],
    dry_run: bool,
    max_workers: int | None,
    run_id: str | None,
) -> None:
    """Run a workflow from a YAML file.

    Positional FILES are mapped to workflow input slots in the order declared
    in the YAML ``inputs:`` section, or by first-occurrence order if that
    section is absent.

    \b
    Examples:
      cdo-flow run wf.yml a.nc b.nc
      cdo-flow run wf.yml a.nc -i ref=b.nc
      cdo-flow run wf.yml -i data=a.nc -i ref=b.nc
    """
    from cdo_flow.config.loader import load_workflow

    wf = load_workflow(workflow_file)

    positional_inputs = _map_positional_inputs(files, wf)
    parsed_inputs = _parse_key_val(inputs)
    # Explicit -i flags take precedence over positional args
    merged_inputs = {**positional_inputs, **parsed_inputs}
    parsed_params = _parse_key_val(params)

    if dry_run:
        wf.dry_run(inputs=merged_inputs, params=parsed_params)
        return

    result = wf.run(
        inputs=merged_inputs,
        params=parsed_params,
        backend=backend,
        max_workers=max_workers,
        run_id=run_id,
        show_progress=True,
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


@cli.command()
@click.argument("run_dir", type=click.Path(exists=True, path_type=Path))
def inspect(run_dir: Path) -> None:
    """Inspect a past run directory (reads provenance.json)."""
    prov_file = run_dir / "provenance.json"
    if not prov_file.exists():
        console.print(f"[bold red]Error:[/bold red] No provenance.json found in {run_dir}")
        sys.exit(1)

    prov = json.loads(prov_file.read_text())

    # Header panel
    wf_name = prov.get("workflow_name", "?")
    run_id = prov.get("run_id", "?")
    status = prov.get("status", "?")
    start = prov.get("start_time", "?")
    end = prov.get("end_time", "?")
    try:
        from datetime import datetime
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        total_dur = f"{(e - s).total_seconds():.1f}s"
    except Exception:
        total_dur = "-"

    status_color = "green" if status == "SUCCESS" else "red"
    header_text = (
        f"[bold]Workflow:[/bold] {wf_name}   "
        f"[bold]Run:[/bold] {run_id}   "
        f"[bold]Status:[/bold] [{status_color}]{status}[/{status_color}]   "
        f"[bold]Duration:[/bold] {total_dur}"
    )
    console.print(Panel(header_text, title="[bold]Run Summary[/bold]", expand=False))

    # Per-step table
    steps_data = prov.get("steps", {})
    table = Table("Step", "Status", "Duration", "Outputs", "Command")
    failed_steps = []

    for step_name, rec in steps_data.items():
        step_status = str(rec.get("state", "?"))
        if hasattr(step_status, "value"):
            step_status = step_status.value  # type: ignore
        # Remove enum prefix like "StepState.DONE"
        if "." in step_status:
            step_status = step_status.split(".")[-1]

        # Duration
        try:
            from datetime import datetime
            s_t = rec.get("start_time")
            e_t = rec.get("end_time")
            if s_t and e_t:
                step_dur = f"{(datetime.fromisoformat(e_t) - datetime.fromisoformat(s_t)).total_seconds():.2f}s"
            else:
                step_dur = "-"
        except Exception:
            step_dur = "-"

        # Outputs
        outputs = rec.get("outputs", {})
        if outputs:
            out_names = ", ".join(k for k in outputs if k != "output")
            if not out_names:
                out_names = ", ".join(list(outputs.keys())[:3])
        else:
            out_names = "-"

        # Command (truncated)
        cmd = rec.get("command") or "-"
        if len(cmd) > 60:
            cmd = cmd[:57] + "..."

        color = {"DONE": "green", "FAILED": "red", "SKIPPED": "yellow"}.get(step_status, "white")
        table.add_row(
            step_name,
            f"[{color}]{step_status}[/{color}]",
            step_dur,
            out_names,
            cmd,
        )

        if step_status == "FAILED":
            failed_steps.append((step_name, rec))

    console.print(table)

    # Show stderr for failed steps
    for step_name, rec in failed_steps:
        stderr = rec.get("stderr") or rec.get("error") or ""
        if stderr:
            console.print(
                Panel(
                    stderr.strip(),
                    title=f"[bold red]{step_name} — error output[/bold red]",
                    border_style="red",
                )
            )


@cli.command()
@click.option(
    "--output", "-o",
    default="workflow.yml",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Output YAML file path.",
)
def create(output: Path) -> None:
    """Launch the interactive workflow creation wizard (TUI)."""
    try:
        from cdo_flow.tui.creation_wizard import WorkflowCreationWizard
    except ImportError:
        console.print("[bold red]Error:[/bold red] textual is required for the creation wizard.")
        console.print("Install it with: pip install textual")
        sys.exit(1)

    app = WorkflowCreationWizard(output_path=output)
    app.run()


@cli.command()
@click.argument("runs_dir", default="./runs", type=click.Path(path_type=Path))
def history(runs_dir: Path) -> None:
    """Browse past run history in an interactive TUI."""
    runs_path = Path(runs_dir)
    if not runs_path.exists():
        console.print(f"[bold red]Error:[/bold red] Directory not found: {runs_path}")
        sys.exit(1)

    try:
        from cdo_flow.tui.history_browser import RunHistoryBrowser
    except ImportError:
        console.print("[bold red]Error:[/bold red] textual is required for the history browser.")
        console.print("Install it with: pip install textual")
        sys.exit(1)

    app = RunHistoryBrowser(runs_dir=runs_path)
    app.run()


def _parse_key_val(pairs: tuple[str, ...]) -> dict[str, str]:
    result = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.BadParameter(f"Expected KEY=VALUE, got: {pair!r}")
        k, _, v = pair.partition("=")
        result[k.strip()] = v.strip()
    return result
