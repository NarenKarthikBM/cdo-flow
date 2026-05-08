from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from cdo_flow.__about__ import __version__
from cdo_flow.step import StepState


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunDirectory:
    def __init__(self, base_dir: str | Path, workflow_name: str, run_id: str | None = None) -> None:
        self.workflow_name = workflow_name
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.path = Path(base_dir) / f"{workflow_name}__{self.run_id}"
        self.path.mkdir(parents=True, exist_ok=True)
        self._temp_dirs: dict[str, Path] = {}

    def step_dir(self, step_name: str) -> Path:
        d = self.path / step_name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def temp_step_dir(self, step_name: str) -> Path:
        if step_name not in self._temp_dirs:
            d = Path(tempfile.mkdtemp(prefix=f"cdo_flow_{step_name}_"))
            self._temp_dirs[step_name] = d
        return self._temp_dirs[step_name]

    def cleanup_temp_dir(self, step_name: str) -> None:
        d = self._temp_dirs.pop(step_name, None)
        if d and d.exists():
            shutil.rmtree(d, ignore_errors=True)

    def write_provenance(self, record: dict) -> Path:
        prov_path = self.path / "provenance.json"
        prov_path.write_text(json.dumps(record, indent=2, default=str))
        return prov_path


class ProvenanceBuilder:
    def __init__(self, workflow_name: str, run_id: str) -> None:
        self.workflow_name = workflow_name
        self.run_id = run_id
        self._steps: dict[str, dict] = {}
        self._start_time = _now_iso()

    def record_step_start(self, step_name: str) -> None:
        self._steps[step_name] = {
            "step_name": step_name,
            "state": StepState.RUNNING,
            "start_time": _now_iso(),
        }

    def record_step_done(
        self,
        step_name: str,
        outputs: dict[str, Path],
        command: str | None = None,
        exit_code: int | None = None,
        stdout: str | None = None,
        stderr: str | None = None,
    ) -> None:
        rec = self._steps.setdefault(step_name, {})
        rec.update(
            state=StepState.DONE,
            end_time=_now_iso(),
            outputs={k: str(v) for k, v in outputs.items()},
            command=command,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    def record_step_failed(
        self,
        step_name: str,
        error: str,
        command: str | None = None,
        exit_code: int | None = None,
        stdout: str | None = None,
        stderr: str | None = None,
    ) -> None:
        rec = self._steps.setdefault(step_name, {})
        rec.update(
            state=StepState.FAILED,
            end_time=_now_iso(),
            error=error,
            command=command,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    def record_step_skipped(self, step_name: str) -> None:
        self._steps[step_name] = {
            "step_name": step_name,
            "state": StepState.SKIPPED,
            "end_time": _now_iso(),
        }

    def build(self, status: str) -> dict:
        cdo_version = _get_cdo_version()
        pcw_version = _get_pcw_version()
        return {
            "workflow_name": self.workflow_name,
            "run_id": self.run_id,
            "status": status,
            "start_time": self._start_time,
            "end_time": _now_iso(),
            "cdo_flow_version": __version__,
            "python_cdo_wrapper_version": pcw_version,
            "cdo_version": cdo_version,
            "steps": self._steps,
        }


def _get_cdo_version() -> str | None:
    try:
        from python_cdo_wrapper import get_cdo_version
        return get_cdo_version()
    except Exception:
        return None


def _get_pcw_version() -> str | None:
    try:
        from python_cdo_wrapper import __version__ as v
        return v
    except Exception:
        return None
