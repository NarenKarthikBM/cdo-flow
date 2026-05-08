from __future__ import annotations

from typing import TYPE_CHECKING

from cdo_flow.executors.base import BaseExecutor

if TYPE_CHECKING:
    from cdo_flow.workflow import Workflow, WorkflowResult


class SnakemakeExecutor(BaseExecutor):
    def run(
        self,
        workflow: Workflow,
        inputs: dict,
        params: dict,
        run_id: str | None = None,
        max_workers: int | None = None,
    ) -> WorkflowResult:
        raise NotImplementedError("SnakemakeExecutor not available in v0.1")

    def validate(self, workflow: Workflow) -> list[str]:
        raise NotImplementedError("SnakemakeExecutor not available in v0.1")
