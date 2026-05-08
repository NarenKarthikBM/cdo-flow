from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from cdo_flow.dag import build_dag, detect_cycle
from cdo_flow.exceptions import WorkflowValidationError
from cdo_flow.step import BaseStep, CdoStep, PythonStep, ResourceSpec, StepState

if TYPE_CHECKING:
    from python_cdo_wrapper.query import CDOQueryTemplate


@dataclass
class WorkflowResult:
    status: str
    outputs: dict[str, Path]
    provenance: dict
    run_dir: Path
    failed_steps: list[str] = field(default_factory=list)
    skipped_steps: list[str] = field(default_factory=list)
    output_path: Path | None = None

    def __bool__(self) -> bool:
        return self.status == "SUCCESS"


class Workflow:
    def __init__(
        self,
        name: str,
        description: str = "",
        run_dir: str | Path = "./runs",
        cdo_options: dict | None = None,
        keep_intermediates: bool = True,
        tags: list[str] | None = None,
        declared_inputs: list[str] | None = None,
        output_path: str | Path | None = None,
        params: dict | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.run_dir = Path(run_dir)
        self.cdo_options: dict = cdo_options or {}
        self.keep_intermediates = keep_intermediates
        self.tags: list[str] = tags or []
        self.declared_inputs: list[str] = declared_inputs or []
        self.output_path: Path | None = Path(output_path) if output_path else None
        self.params: dict = params or {}
        self._steps: list[BaseStep] = []

    def add_step(
        self,
        name: str,
        fn: Callable | None = None,
        *,
        chain: CDOQueryTemplate | None = None,
        inputs: dict[str, str | Path] | None = None,
        output: list[str] | str | None = None,
        depends_on: list[str] | None = None,
        resources: dict | ResourceSpec | None = None,
        tags: list[str] | None = None,
        keep: bool | None = None,
    ) -> Workflow:
        inputs = inputs or {}
        depends_on = depends_on or []
        tags = tags or []
        output_names = ([output] if isinstance(output, str) else list(output)) if output else []

        # Resolve resources
        if resources is None:
            res = ResourceSpec()
        elif isinstance(resources, dict):
            res = ResourceSpec(**resources)
        else:
            res = resources

        if fn is not None and hasattr(fn, "_cdo_flow_meta"):
            meta = fn._cdo_flow_meta
            step_type = meta.step_type
            effective_keep = keep if keep is not None else meta.keep
            effective_resources = res if resources is not None else (meta.resources or res)
            effective_tags = tags or meta.tags or []
            effective_name = meta.name or name

            if step_type is CdoStep:
                step = CdoStep(
                    name=effective_name,
                    inputs=inputs,
                    output_names=output_names,
                    depends_on=depends_on,
                    resources=effective_resources,
                    tags=effective_tags,
                    keep=effective_keep,
                    fn=fn,
                    chain=chain,
                    cdo_options=self.cdo_options.copy(),
                )
            else:
                step = PythonStep(
                    name=effective_name,
                    inputs=inputs,
                    output_names=output_names,
                    depends_on=depends_on,
                    resources=effective_resources,
                    tags=effective_tags,
                    keep=effective_keep,
                    fn=fn,
                )
        elif chain is not None:
            step = CdoStep(
                name=name,
                inputs=inputs,
                output_names=output_names,
                depends_on=depends_on,
                resources=res,
                tags=tags,
                keep=keep,
                fn=None,
                chain=chain,
                cdo_options=self.cdo_options.copy(),
            )
        elif fn is not None:
            # Plain callable without decorator — treat as PythonStep
            step = PythonStep(
                name=name,
                inputs=inputs,
                output_names=output_names,
                depends_on=depends_on,
                resources=res,
                tags=tags,
                keep=keep,
                fn=fn,
            )
        else:
            raise ValueError(f"add_step '{name}': must provide fn or chain")

        self._steps.append(step)
        return self

    def validate(self) -> list[str]:
        errors: list[str] = []
        try:
            deps = build_dag(self._steps)
            detect_cycle(deps)
        except Exception as e:
            errors.append(str(e))
        return errors

    def dry_run(self, inputs: dict | None = None, params: dict | None = None) -> None:
        from rich.console import Console
        from rich.table import Table

        from cdo_flow.dag import build_dag, topological_sort

        errors = self.validate()
        if errors:
            raise WorkflowValidationError(errors)

        deps = build_dag(self._steps)
        ordered = topological_sort(self._steps, deps)

        console = Console()
        console.print(f"\n[bold]Workflow:[/bold] {self.name}")
        table = Table("Step", "Type", "Inputs", "Depends On", title="Execution Plan")
        for step in ordered:
            step_type = type(step).__name__
            inp = ", ".join(f"{k}={v}" for k, v in step.inputs.items())
            dep_str = ", ".join(step.depends_on) or "-"
            table.add_row(step.name, step_type, inp or "-", dep_str)
        console.print(table)

    def run(
        self,
        inputs: dict | None = None,
        params: dict | None = None,
        backend: str = "local",
        max_workers: int | None = None,
        run_id: str | None = None,
        on_event: Callable | None = None,
        show_progress: bool = True,
    ) -> WorkflowResult:
        errors = self.validate()
        if errors:
            raise WorkflowValidationError(errors)

        if backend == "local":
            from cdo_flow.executors.local import LocalExecutor
            executor = LocalExecutor()
        elif backend == "snakemake":
            from cdo_flow.executors.snakemake import SnakemakeExecutor
            executor = SnakemakeExecutor()
        else:
            raise ValueError(f"Unknown backend: {backend!r}")

        return executor.run(
            workflow=self,
            inputs=inputs or {},
            params=params or {},
            run_id=run_id,
            max_workers=max_workers,
            on_event=on_event,
            show_progress=show_progress,
        )

    def map(
        self,
        inputs_list: list[dict],
        params: dict | None = None,
        backend: str = "local",
        max_workers: int | None = None,
    ) -> list[WorkflowResult]:
        """Run this workflow for each set of inputs in parallel (ensemble execution).

        Each member gets its own timestamped run directory. Rich progress bars
        show per-member progress updated step-by-step.

        Args:
            inputs_list: List of input dicts, one per ensemble member.
            params: Shared parameters passed to every member.
            backend: Execution backend (currently only "local").
            max_workers: Max concurrent ensemble members.

        Returns:
            List of WorkflowResult in the same order as inputs_list.
        """
        from rich.live import Live
        from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn
        from rich.table import Table

        from datetime import datetime, timezone

        errors = self.validate()
        if errors:
            raise WorkflowValidationError(errors)

        total_steps = len(self._steps)
        params = params or {}

        # Generate a unique base timestamp so member run_ids won't collide
        # even when all threads start within the same second.
        base_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # Per-member state for progress display
        member_progress: dict[int, dict] = {
            i: {"completed": 0, "status": "PENDING", "current_step": ""}
            for i in range(len(inputs_list))
        }

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn("[dim]{task.fields[status]}"),
            TextColumn("[italic dim]{task.fields[current_step]}"),
        )

        task_ids: list[TaskID] = []
        for i in range(len(inputs_list)):
            tid = progress.add_task(
                f"member_{i:03d}",
                total=total_steps,
                status="PENDING",
                current_step="",
            )
            task_ids.append(tid)

        results: list[WorkflowResult | Exception] = [None] * len(inputs_list)  # type: ignore

        def run_member(idx: int, member_inputs: dict) -> WorkflowResult:
            member_run_id = f"{base_run_id}_m{idx:03d}"

            def on_event(event) -> None:
                state = event.state.value
                step_name = event.step_name
                if event.type == "step_done":
                    member_progress[idx]["completed"] += 1
                    member_progress[idx]["current_step"] = ""
                elif event.type == "step_failed":
                    member_progress[idx]["status"] = "FAILED"
                    member_progress[idx]["current_step"] = f"failed: {step_name}"
                elif event.type == "step_started":
                    member_progress[idx]["current_step"] = step_name
                    member_progress[idx]["status"] = "RUNNING"
                progress.update(
                    task_ids[idx],
                    completed=member_progress[idx]["completed"],
                    status=member_progress[idx]["status"],
                    current_step=member_progress[idx]["current_step"],
                )

            return self.run(
                inputs=member_inputs,
                params=params,
                backend=backend,
                run_id=member_run_id,
                on_event=on_event,
                show_progress=False,
            )

        with Live(progress, refresh_per_second=8):
            for i in range(len(inputs_list)):
                progress.update(task_ids[i], status="PENDING")

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_idx = {
                    pool.submit(run_member, i, inp): i
                    for i, inp in enumerate(inputs_list)
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    exc = future.exception()
                    if exc is not None:
                        results[idx] = exc
                        progress.update(task_ids[idx], status="ERROR", current_step=str(exc))
                    else:
                        res = future.result()
                        results[idx] = res
                        final_status = res.status
                        progress.update(
                            task_ids[idx],
                            completed=total_steps,
                            status=final_status,
                            current_step="",
                        )

        # Re-raise if any member had an unexpected exception (not a workflow failure)
        final_results: list[WorkflowResult] = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                raise RuntimeError(f"Ensemble member {i} raised an exception: {r}") from r
            final_results.append(r)  # type: ignore

        return final_results
