from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator

_AT_REF_PATTERN = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)\.output$")


class OperatorChainEntry(BaseModel):
    op: str
    args: list[str | int | float] = []


class ResourceSpecSchema(BaseModel):
    cpus: int = 1
    mem_gb: float = 4.0
    walltime: str = "01:00:00"


class StepSchema(BaseModel):
    id: str
    type: Literal["cdo", "python"]
    inputs: dict[str, str] = {}
    operator_chain: list[OperatorChainEntry] | None = None
    script: str | None = None
    output: list[str] | str | None = None
    depends_on: list[str] = []
    resources: ResourceSpecSchema = ResourceSpecSchema()
    tags: list[str] = []
    keep: bool | None = None

    @model_validator(mode="after")
    def validate_type_requirements(self) -> StepSchema:
        if self.type == "cdo" and not self.operator_chain:
            raise ValueError(f"Step '{self.id}' of type 'cdo' requires 'operator_chain'")
        if self.type == "python" and not self.script:
            raise ValueError(f"Step '{self.id}' of type 'python' requires 'script'")
        return self


class WorkflowSchema(BaseModel):
    name: str
    description: str = ""
    run_dir: str = "./runs"
    keep_intermediates: bool = True
    cdo_options: dict[str, Any] = {}
    tags: list[str] = []
    steps: list[StepSchema]

    @field_validator("steps")
    @classmethod
    def no_duplicate_ids(cls, steps: list[StepSchema]) -> list[StepSchema]:
        seen: set[str] = set()
        for step in steps:
            if step.id in seen:
                raise ValueError(f"Duplicate step id: '{step.id}'")
            seen.add(step.id)
        return steps

    @model_validator(mode="after")
    def validate_cross_references(self) -> WorkflowSchema:
        step_ids = {s.id for s in self.steps}
        errors: list[str] = []
        for step in self.steps:
            for key, val in step.inputs.items():
                m = _AT_REF_PATTERN.match(val.strip())
                if m:
                    ref_id = m.group(1)
                    if ref_id not in step_ids:
                        errors.append(
                            f"Step '{step.id}' input '{key}' references unknown step '@{ref_id}.output'"
                        )
            for dep in step.depends_on:
                if dep not in step_ids:
                    errors.append(
                        f"Step '{step.id}' depends_on references unknown step '{dep}'"
                    )
        if errors:
            raise ValueError("\n".join(errors))
        return self
