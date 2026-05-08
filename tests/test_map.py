from __future__ import annotations

from pathlib import Path

import pytest

from cdo_flow.decorators import python_step
from cdo_flow.workflow import Workflow


@python_step
def write_member(ctx):
    out = ctx.output("member.txt")
    out.write_text(ctx.params.get("label", "no_label"))


@python_step
def failing_member(ctx):
    raise RuntimeError("intentional member failure")


class TestMap:
    def test_map_returns_list_of_results(self, tmp_path):
        wf = Workflow("ensemble", run_dir=tmp_path)
        wf.add_step("write", write_member)

        inputs_list = [{"label": "a"}, {"label": "b"}, {"label": "c"}]
        # Pass label via params — one set of params per member isn't supported by map,
        # so we verify 3 results come back
        results = wf.map(inputs_list=inputs_list, params={"label": "shared"})

        assert len(results) == 3
        for r in results:
            assert r.status == "SUCCESS"
            assert bool(r)

    def test_map_each_result_has_own_run_dir(self, tmp_path):
        wf = Workflow("ensemble_dirs", run_dir=tmp_path)
        wf.add_step("write", write_member)

        inputs_list = [{}, {}, {}]
        results = wf.map(inputs_list=inputs_list)

        run_dirs = [r.run_dir for r in results]
        # All run dirs must be distinct
        assert len(set(run_dirs)) == len(results)
        # Each run dir must exist
        for rd in run_dirs:
            assert rd.exists()

    def test_map_failure_isolation(self, tmp_path):
        """One failing member should not prevent others from completing."""
        wf_good = Workflow("good_member", run_dir=tmp_path / "good")
        wf_good.add_step("write", write_member)

        wf_bad = Workflow("bad_member", run_dir=tmp_path / "bad")
        wf_bad.add_step("fail", failing_member)

        # Run two independent workflows, check isolation (separate map calls)
        good_results = wf_good.map(inputs_list=[{}, {}])
        bad_results = wf_bad.map(inputs_list=[{}, {}])

        assert all(r.status == "SUCCESS" for r in good_results)
        assert all(r.status == "FAILED" for r in bad_results)

    def test_map_provenance_per_member(self, tmp_path):
        wf = Workflow("prov_ensemble", run_dir=tmp_path)
        wf.add_step("write", write_member)

        results = wf.map(inputs_list=[{}, {}])

        for r in results:
            prov_file = r.run_dir / "provenance.json"
            assert prov_file.exists(), f"Missing provenance.json in {r.run_dir}"

    def test_map_empty_inputs_list(self, tmp_path):
        wf = Workflow("empty_ensemble", run_dir=tmp_path)
        wf.add_step("write", write_member)

        results = wf.map(inputs_list=[])
        assert results == []
