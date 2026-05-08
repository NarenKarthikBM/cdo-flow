from __future__ import annotations

import subprocess
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.table import Table

from cdo_flow.dag import (
    build_dag,
    detect_cycle,
    get_downstream_steps,
    get_leaf_steps,
    get_ready_steps,
    topological_sort,
)
from cdo_flow.exceptions import InputFileNotFoundError, StepExecutionError
from cdo_flow.executors.base import BaseExecutor
from cdo_flow.provenance import ProvenanceBuilder, RunDirectory
from cdo_flow.step import (
    BaseStep,
    CdoStep,
    PythonStep,
    StepContext,
    StepExecutionRecord,
    StepState,
)

if TYPE_CHECKING:
    from cdo_flow.workflow import Workflow, WorkflowResult

try:
    from python_cdo_wrapper import CDO
    from python_cdo_wrapper.exceptions import CDOExecutionError
    from python_cdo_wrapper.query import CDOQuery
except ImportError:
    CDO = None  # type: ignore
    CDOExecutionError = None  # type: ignore
    CDOQuery = None  # type: ignore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def execute_step(
    step: BaseStep,
    ctx: StepContext,
    cdo_options: dict,
) -> StepExecutionRecord:
    """Top-level function (module-level for pickling) that executes a single step."""
    record = StepExecutionRecord(step_name=step.name, state=StepState.RUNNING)
    record.start_time = _now_iso()

    try:
        if isinstance(step, CdoStep):
            from python_cdo_wrapper import CDO as _CDO
            from python_cdo_wrapper.exceptions import CDOExecutionError as _CDOExecError

            cdo_inst = _CDO(env={"OMP_NUM_THREADS": str(step.resources.cpus)}, **cdo_options)

            if step.fn is not None:
                step.fn(ctx, cdo_inst)
            elif step.chain is not None:
                # Resolve the primary input
                primary_input = next(iter(ctx.inputs.values())) if ctx.inputs else None
                if primary_input is None:
                    raise ValueError(f"CdoStep '{step.name}' with chain requires at least one input")
                # Determine output name
                out_name = step.output_names[0] if step.output_names else "output.nc"
                output_path = ctx.output(out_name)
                bound = step.chain.apply(str(primary_input), cdo_inst)
                bound.to_file(str(output_path))
            else:
                raise ValueError(f"CdoStep '{step.name}' has neither fn nor chain")

        elif isinstance(step, PythonStep):
            if step.fn is not None:
                step.fn(ctx)
            elif step.script_path is not None:
                import json
                import tempfile

                ctx_data = {
                    "inputs": {k: str(v) for k, v in ctx.inputs.items()},
                    "run_dir": str(ctx.run_dir),
                    "step_name": ctx.step_name,
                    "workflow_name": ctx.workflow_name,
                    "params": ctx.params,
                }
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                    json.dump(ctx_data, f)
                    ctx_file = f.name
                result = subprocess.run(
                    ["python3", str(step.script_path), ctx_file],
                    capture_output=True, text=True
                )
                record.command = f"python3 {step.script_path} {ctx_file}"
                record.stdout = result.stdout
                record.stderr = result.stderr
                record.exit_code = result.returncode
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Script '{step.script_path}' exited with code {result.returncode}:\n{result.stderr}"
                    )
            else:
                raise ValueError(f"PythonStep '{step.name}' has neither fn nor script_path")
        else:
            raise ValueError(f"Unknown step type: {type(step)}")

        # Collect outputs: scan step output dir
        outputs: dict[str, Path] = {}
        if ctx._keep:
            out_dir = ctx.run_dir / step.name
        else:
            out_dir = Path(ctx._temp_dir) if ctx._temp_dir else ctx.run_dir / step.name

        if out_dir.exists():
            for p in sorted(out_dir.iterdir()):
                if p.is_file():
                    outputs[p.name] = p
        # Also use "output" key as the canonical output
        if outputs:
            outputs["output"] = next(iter(outputs.values()))

        record.outputs = outputs
        record.state = StepState.DONE
        record.end_time = _now_iso()

    except Exception as exc:
        record.state = StepState.FAILED
        record.end_time = _now_iso()
        record.error = traceback.format_exc()

        # Extract CDO-specific info if available
        try:
            from python_cdo_wrapper.exceptions import CDOExecutionError as _CDOExecError
            if isinstance(exc, _CDOExecError):
                record.command = exc.command
                record.exit_code = exc.returncode
                record.stdout = exc.stdout
                record.stderr = exc.stderr
                raise StepExecutionError(step.name, exc) from exc
        except ImportError:
            pass

        raise StepExecutionError(step.name, exc) from exc

    return record


def _resolve_step_inputs(
    step: BaseStep,
    execution_state: dict[str, StepExecutionRecord],
    workflow_inputs: dict[str, str | Path],
) -> dict[str, Path]:
    """Resolve @step.output refs and literal paths for a step's inputs."""
    import re
    AT_REF = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)\.output$")

    resolved: dict[str, Path] = {}
    for key, val in step.inputs.items():
        val_str = str(val).strip()
        m = AT_REF.match(val_str)
        if m:
            dep_name = m.group(1)
            dep_record = execution_state.get(dep_name)
            if dep_record is None or not dep_record.outputs:
                raise RuntimeError(
                    f"Step '{step.name}' input '{key}': dependency '{dep_name}' has no outputs"
                )
            resolved[key] = dep_record.outputs["output"]
        else:
            # Check workflow-level inputs first
            if val_str in workflow_inputs:
                resolved[key] = Path(workflow_inputs[val_str])
            else:
                p = Path(val_str)
                if not p.exists():
                    raise InputFileNotFoundError(step.name, key, val_str)
                resolved[key] = p
    return resolved


class LocalExecutor(BaseExecutor):
    def validate(self, workflow: Workflow) -> list[str]:
        return workflow.validate()

    def run(
        self,
        workflow: Workflow,
        inputs: dict,
        params: dict,
        run_id: str | None = None,
        max_workers: int | None = None,
    ) -> WorkflowResult:
        from cdo_flow.workflow import WorkflowResult

        steps = workflow._steps
        deps = build_dag(steps)
        detect_cycle(deps)

        # Force leaf steps to keep=True
        leaf_names = get_leaf_steps(steps, deps)
        for step in steps:
            if step.name in leaf_names and step.keep is None:
                step.keep = True
            elif step.keep is None:
                step.keep = workflow.keep_intermediates

        run_dir = RunDirectory(workflow.run_dir, workflow.name, run_id)
        prov = ProvenanceBuilder(workflow.name, run_dir.run_id)

        execution_state: dict[str, StepExecutionRecord] = {}
        completed: set[str] = set()
        running: set[str] = set()
        failed_or_skipped: set[str] = set()

        console = Console()
        console.print(f"\n[bold blue]Running workflow:[/bold blue] {workflow.name}")

        futures: dict[Any, str] = {}

        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            while True:
                # Submit ready steps
                ready = get_ready_steps(steps, deps, completed, running, failed_or_skipped)
                for step in ready:
                    try:
                        resolved_inputs = _resolve_step_inputs(step, execution_state, inputs)
                    except Exception as e:
                        prov.record_step_failed(step.name, str(e))
                        failed_or_skipped.add(step.name)
                        for ds in get_downstream_steps(step.name, deps):
                            prov.record_step_skipped(ds)
                            failed_or_skipped.add(ds)
                        continue

                    keep = step.keep if step.keep is not None else workflow.keep_intermediates
                    ctx = StepContext(
                        inputs=resolved_inputs,
                        run_dir=run_dir.path,
                        step_name=step.name,
                        workflow_name=workflow.name,
                        params=params,
                        _keep=keep,
                    )

                    prov.record_step_start(step.name)
                    future = pool.submit(execute_step, step, ctx, workflow.cdo_options)
                    futures[future] = step.name
                    running.add(step.name)

                # Collect completed futures (with short timeout to allow re-checking ready)
                if not futures:
                    # Check if there's anything left to do
                    all_names = {s.name for s in steps}
                    terminal = completed | failed_or_skipped
                    remaining = all_names - terminal
                    if not remaining:
                        break
                    # Check for steps that can never be ready (deps failed)
                    for step in steps:
                        if step.name not in terminal:
                            if deps[step.name].intersection(failed_or_skipped):
                                prov.record_step_skipped(step.name)
                                failed_or_skipped.add(step.name)
                    all_names = {s.name for s in steps}
                    if all_names.issubset(completed | failed_or_skipped):
                        break
                    # If nothing is running and nothing is ready, we're stuck
                    new_ready = get_ready_steps(steps, deps, completed, running, failed_or_skipped)
                    if not new_ready:
                        break
                    continue

                done_futures = []
                for future in list(futures):
                    if future.done():
                        done_futures.append(future)

                if not done_futures:
                    # Wait for any future to complete
                    import time
                    time.sleep(0.05)
                    continue

                for future in done_futures:
                    step_name = futures.pop(future)
                    running.discard(step_name)
                    exc = future.exception()
                    if exc is not None:
                        completed_record = StepExecutionRecord(
                            step_name=step_name,
                            state=StepState.FAILED,
                            error=str(exc),
                        )
                        if isinstance(exc, StepExecutionError):
                            completed_record.command = exc.command
                            completed_record.exit_code = exc.returncode
                            completed_record.stdout = exc.stdout
                            completed_record.stderr = exc.stderr
                        execution_state[step_name] = completed_record
                        failed_or_skipped.add(step_name)
                        prov.record_step_failed(
                            step_name,
                            str(exc),
                            command=getattr(exc, "command", None),
                            exit_code=getattr(exc, "returncode", None),
                            stdout=getattr(exc, "stdout", None),
                            stderr=getattr(exc, "stderr", None),
                        )
                        # Mark all downstream as skipped
                        for ds in get_downstream_steps(step_name, deps):
                            if ds not in failed_or_skipped and ds not in completed:
                                prov.record_step_skipped(ds)
                                failed_or_skipped.add(ds)
                    else:
                        record = future.result()
                        execution_state[step_name] = record
                        completed.add(step_name)
                        prov.record_step_done(
                            step_name,
                            record.outputs,
                            command=record.command,
                            exit_code=record.exit_code,
                            stdout=record.stdout,
                            stderr=record.stderr,
                        )
                        # Cleanup temp dirs for keep=False steps when all consumers done
                        step_obj = next(s for s in steps if s.name == step_name)
                        if not (step_obj.keep if step_obj.keep is not None else workflow.keep_intermediates):
                            consumers = get_downstream_steps(step_name, deps)
                            if consumers.issubset(completed | failed_or_skipped):
                                run_dir.cleanup_temp_dir(step_name)

        # Determine final status
        all_names = {s.name for s in steps}
        failed_names = [n for n in all_names if execution_state.get(n, StepExecutionRecord(n)).state == StepState.FAILED]
        skipped_names = [n for n in all_names if n in failed_or_skipped and n not in {n2 for n2 in failed_names}]
        status = "SUCCESS" if not failed_names else "FAILED"

        # Collect leaf outputs
        result_outputs: dict[str, Path] = {}
        for leaf_name in leaf_names:
            rec = execution_state.get(leaf_name)
            if rec and rec.outputs:
                result_outputs[leaf_name] = rec.outputs.get("output", next(iter(rec.outputs.values())))

        prov_data = prov.build(status)
        run_dir.write_provenance(prov_data)

        # Print summary table
        table = Table("Step", "Status", "Duration", title=f"Workflow Summary: {workflow.name}")
        for step in steps:
            rec = execution_state.get(step.name)
            if rec:
                state_str = rec.state.value if hasattr(rec.state, "value") else str(rec.state)
                if rec.start_time and rec.end_time:
                    from datetime import datetime
                    try:
                        start = datetime.fromisoformat(rec.start_time)
                        end = datetime.fromisoformat(rec.end_time)
                        dur = f"{(end - start).total_seconds():.2f}s"
                    except Exception:
                        dur = "-"
                else:
                    dur = "-"
            else:
                state_str = StepState.SKIPPED.value
                dur = "-"

            color = {"DONE": "green", "FAILED": "red", "SKIPPED": "yellow"}.get(state_str, "white")
            table.add_row(step.name, f"[{color}]{state_str}[/{color}]", dur)

        console.print(table)

        return WorkflowResult(
            status=status,
            outputs=result_outputs,
            provenance=prov_data,
            run_dir=run_dir.path,
            failed_steps=failed_names,
            skipped_steps=skipped_names,
        )
