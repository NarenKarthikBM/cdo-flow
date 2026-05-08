from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from cdo_flow.config.schema import OperatorChainEntry, WorkflowSchema
from cdo_flow.exceptions import WorkflowValidationError
from cdo_flow.step import CdoStep, PythonStep, ResourceSpec


def _build_chain_from_yaml(entries: list[OperatorChainEntry]):
    """Build a CDOQueryTemplate from YAML operator chain entries."""
    from python_cdo_wrapper.operators.base import OperatorSpec
    from python_cdo_wrapper.query import CDOQueryTemplate

    specs = tuple(OperatorSpec(e.op, args=tuple(e.args)) for e in entries)
    return CDOQueryTemplate(operators=specs)


def load_workflow(path: str | Path):
    """Load a Workflow from a YAML file."""
    content = Path(path).read_text()
    return load_workflow_from_string(content)


def load_workflow_from_string(content: str):
    """Load a Workflow from a YAML string. Convenience for tests."""
    from cdo_flow.workflow import Workflow

    data = yaml.safe_load(content)

    try:
        schema = WorkflowSchema.model_validate(data)
    except ValidationError as e:
        errors = [str(err["msg"]) for err in e.errors()]
        raise WorkflowValidationError(errors) from e

    wf = Workflow(
        name=schema.name,
        description=schema.description,
        run_dir=schema.run_dir,
        cdo_options=schema.cdo_options,
        keep_intermediates=schema.keep_intermediates,
        tags=schema.tags,
        declared_inputs=schema.inputs,
        output_path=schema.output_path,
        params=schema.params,
    )

    for step_schema in schema.steps:
        res = ResourceSpec(
            cpus=step_schema.resources.cpus,
            mem_gb=step_schema.resources.mem_gb,
            walltime=step_schema.resources.walltime,
        )
        output = (
            [step_schema.output] if isinstance(step_schema.output, str)
            else step_schema.output or []
        )

        if step_schema.type == "cdo":
            chain = _build_chain_from_yaml(step_schema.operator_chain)
            step = CdoStep(
                name=step_schema.id,
                inputs=dict(step_schema.inputs),
                output_names=output,
                depends_on=list(step_schema.depends_on),
                resources=res,
                tags=list(step_schema.tags),
                keep=step_schema.keep,
                fn=None,
                chain=chain,
                cdo_options=wf.cdo_options.copy(),
            )
        else:
            step = PythonStep(
                name=step_schema.id,
                inputs=dict(step_schema.inputs),
                output_names=output,
                depends_on=list(step_schema.depends_on),
                resources=res,
                tags=list(step_schema.tags),
                keep=step_schema.keep,
                fn=None,
                script_path=Path(step_schema.script),
            )

        wf._steps.append(step)

    return wf
