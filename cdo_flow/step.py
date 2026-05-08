from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from python_cdo_wrapper.query import CDOQueryTemplate


class StepState(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class ResourceSpec:
    cpus: int = 1
    mem_gb: float = 4.0
    walltime: str = "01:00:00"


@dataclass
class StepContext:
    inputs: dict[str, Path]
    run_dir: Path
    step_name: str
    workflow_name: str
    params: dict
    _keep: bool = field(default=True, repr=False)
    _temp_dir: str | None = field(default=None, repr=False)

    def output(self, filename: str) -> Path:
        if self._keep:
            out_dir = self.run_dir / self.step_name
        else:
            if self._temp_dir is None:
                self._temp_dir = tempfile.mkdtemp(prefix=f"cdo_flow_{self.step_name}_")
            out_dir = Path(self._temp_dir)
        full_path = out_dir / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path

    def path(self, relative: str) -> Path:
        return self.run_dir / relative


@dataclass
class BaseStep:
    name: str
    inputs: dict[str, str | Path] = field(default_factory=dict)
    output_names: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    resources: ResourceSpec = field(default_factory=ResourceSpec)
    tags: list[str] = field(default_factory=list)
    keep: bool | None = None
    fn: Callable | None = None


@dataclass
class CdoStep(BaseStep):
    chain: CDOQueryTemplate | None = None
    cdo_options: dict = field(default_factory=dict)


@dataclass
class PythonStep(BaseStep):
    script_path: Path | None = None


@dataclass
class StepExecutionRecord:
    step_name: str
    state: StepState = StepState.PENDING
    start_time: str | None = None
    end_time: str | None = None
    outputs: dict[str, Path] = field(default_factory=dict)
    command: str | None = None
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    error: str | None = None
