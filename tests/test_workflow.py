from __future__ import annotations

import pytest

from cdo_flow.decorators import cdo_step, python_step
from cdo_flow.step import CdoStep, PythonStep, ResourceSpec
from cdo_flow.workflow import Workflow


@cdo_step
def sample_cdo(ctx, cdo):
    pass


@python_step
def sample_python(ctx):
    pass


class TestAddStep:
    def test_add_step_decorated_cdo_fn(self):
        wf = Workflow("test")
        wf.add_step("regrid", sample_cdo)
        assert len(wf._steps) == 1
        assert isinstance(wf._steps[0], CdoStep)
        assert wf._steps[0].fn is sample_cdo

    def test_add_step_decorated_python_fn(self):
        wf = Workflow("test")
        wf.add_step("proc", sample_python)
        assert len(wf._steps) == 1
        assert isinstance(wf._steps[0], PythonStep)

    def test_add_step_inline_chain(self):
        from python_cdo_wrapper.query import CDOQueryTemplate
        wf = Workflow("test")
        chain = CDOQueryTemplate().select_var("tas")
        wf.add_step("select", chain=chain, inputs={"data": "/some/file.nc"})
        assert isinstance(wf._steps[0], CdoStep)
        assert wf._steps[0].chain is chain

    def test_add_step_returns_self_for_chaining(self):
        wf = Workflow("test")
        result = wf.add_step("a", sample_python)
        assert result is wf

    def test_resources_dict_coerced(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python, resources={"cpus": 4, "mem_gb": 8.0})
        assert wf._steps[0].resources == ResourceSpec(cpus=4, mem_gb=8.0)

    def test_resources_spec_preserved(self):
        wf = Workflow("test")
        res = ResourceSpec(cpus=2)
        wf.add_step("a", sample_python, resources=res)
        assert wf._steps[0].resources is res


class TestValidate:
    def test_validate_valid(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python)
        wf.add_step("b", sample_python, inputs={"x": "@a.output"})
        errors = wf.validate()
        assert errors == []

    def test_validate_cycle(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python, depends_on=["b"])
        wf.add_step("b", sample_python, depends_on=["a"])
        errors = wf.validate()
        assert len(errors) == 1
        assert "cycle" in errors[0].lower() or "Cycle" in errors[0]

    def test_validate_missing_ref(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python, inputs={"x": "@nonexistent.output"})
        errors = wf.validate()
        assert len(errors) == 1


class TestKeepBehavior:
    def test_keep_none_stored_as_sentinel(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python)
        assert wf._steps[0].keep is None

    def test_keep_explicit(self):
        wf = Workflow("test")
        wf.add_step("a", sample_python, keep=False)
        assert wf._steps[0].keep is False
