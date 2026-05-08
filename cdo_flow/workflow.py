from __future__ import annotations

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
    ) -> None:
        self.name = name
        self.description = description
        self.run_dir = Path(run_dir)
        self.cdo_options: dict = cdo_options or {}
        self.keep_intermediates = keep_intermediates
        self.tags: list[str] = tags or []
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
        )
