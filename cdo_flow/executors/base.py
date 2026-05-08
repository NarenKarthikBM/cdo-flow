from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cdo_flow.workflow import Workflow, WorkflowResult


class BaseExecutor(ABC):
    @abstractmethod
    def run(
        self,
        workflow: Workflow,
        inputs: dict,
        params: dict,
        run_id: str | None = None,
        max_workers: int | None = None,
    ) -> WorkflowResult: ...

    @abstractmethod
    def validate(self, workflow: Workflow) -> list[str]: ...
