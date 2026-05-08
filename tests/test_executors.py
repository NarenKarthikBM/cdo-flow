from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdo_flow.decorators import python_step
from cdo_flow.step import StepState
from cdo_flow.workflow import Workflow


@python_step
def write_file(ctx):
    out = ctx.output("result.txt")
    out.write_text("hello from write_file")


@python_step
def read_and_write(ctx):
    src = ctx.inputs["data"]
    content = Path(src).read_text()
    out = ctx.output("result.txt")
    out.write_text(f"processed: {content}")


@python_step
def failing_step(ctx):
    raise RuntimeError("intentional failure")


@python_step
def downstream_step(ctx):
    out = ctx.output("result.txt")
    out.write_text("downstream")


@python_step
def step_a(ctx):
    ctx.output("a.txt").write_text("a")


@python_step
def step_b(ctx):
    ctx.output("b.txt").write_text("b")


class TestLinearWorkflow:
    def test_linear_workflow_success(self, tmp_path):
        wf = Workflow("linear", run_dir=tmp_path)
        wf.add_step("write", write_file)
        wf.add_step("read", read_and_write, inputs={"data": "@write.output"})
        result = wf.run()
        assert result.status == "SUCCESS"
        assert bool(result)
        assert "read" in result.outputs
        assert result.outputs["read"].exists()

    def test_result_status_success(self, tmp_path):
        wf = Workflow("ok", run_dir=tmp_path)
        wf.add_step("write", write_file)
        result = wf.run()
        assert result.status == "SUCCESS"


class TestParallelSteps:
    def test_parallel_steps(self, tmp_path):
        wf = Workflow("parallel", run_dir=tmp_path)
        wf.add_step("a", step_a)
        wf.add_step("b", step_b)
        result = wf.run(max_workers=2)
        assert result.status == "SUCCESS"
        assert "a" in result.outputs
        assert "b" in result.outputs


class TestFailureHandling:
    def test_failure_marks_downstream_skipped(self, tmp_path):
        wf = Workflow("fail", run_dir=tmp_path)
        wf.add_step("fail_step", failing_step)
        wf.add_step("ds", downstream_step, inputs={"x": "@fail_step.output"})
        result = wf.run()
        assert result.status == "FAILED"
        assert "fail_step" in result.failed_steps
        assert "ds" in result.skipped_steps

    def test_result_status_failed(self, tmp_path):
        wf = Workflow("fail2", run_dir=tmp_path)
        wf.add_step("fail_step", failing_step)
        result = wf.run()
        assert result.status == "FAILED"
        assert not bool(result)


class TestProvenance:
    def test_provenance_written(self, tmp_path):
        wf = Workflow("prov", run_dir=tmp_path)
        wf.add_step("write", write_file)
        result = wf.run()
        prov_file = result.run_dir / "provenance.json"
        assert prov_file.exists()

    def test_provenance_content(self, tmp_path):
        wf = Workflow("prov2", run_dir=tmp_path)
        wf.add_step("write", write_file)
        result = wf.run()
        prov = result.provenance
        assert prov["status"] == "SUCCESS"
        assert "write" in prov["steps"]
        assert prov["steps"]["write"]["state"] == StepState.DONE
        assert "start_time" in prov["steps"]["write"]
        assert "cdo_flow_version" in prov


class TestKeepFalse:
    def test_keep_false_temp_deleted(self, tmp_path):
        """Intermediate step with keep=False should have its temp dir cleaned up."""
        wf = Workflow("keep_test", run_dir=tmp_path)
        wf.add_step("inter", write_file, keep=False)
        wf.add_step("final", read_and_write, inputs={"data": "@inter.output"})
        result = wf.run()
        # Final step should succeed
        assert result.status == "SUCCESS"
        # Intermediate step dir should NOT be in the run dir (it was temp)
        inter_dir = result.run_dir / "inter"
        assert not inter_dir.exists()


class TestStepContext:
    def test_ctx_output_creates_dirs(self, tmp_path):
        from cdo_flow.step import StepContext

        ctx = StepContext(
            inputs={},
            run_dir=tmp_path,
            step_name="test_step",
            workflow_name="test_wf",
            params={},
            _keep=True,
        )
        out = ctx.output("nested/file.txt")
        assert out.parent.exists()
        assert out == tmp_path / "test_step" / "nested" / "file.txt"
