from __future__ import annotations

import pytest
from pydantic import ValidationError

from cdo_flow.config.loader import load_workflow_from_string
from cdo_flow.config.schema import WorkflowSchema
from cdo_flow.exceptions import WorkflowValidationError
from cdo_flow.step import CdoStep, PythonStep

SIMPLE_YAML = """
name: test_workflow
run_dir: ./test_runs
steps:
  - id: select
    type: cdo
    inputs:
      data: /path/to/input.nc
    operator_chain:
      - op: selname
        args: [tas]
    output: selected.nc

  - id: process
    type: python
    inputs:
      data: "@select.output"
    script: scripts/process.py
    depends_on: [select]
"""

CDO_ONLY_YAML = """
name: cdo_test
steps:
  - id: regrid
    type: cdo
    inputs:
      data: /data/input.nc
    operator_chain:
      - op: remapbil
        args: [r360x180]
      - op: selname
        args: [tas, pr]
    output: regridded.nc
"""

PYTHON_ONLY_YAML = """
name: py_test
steps:
  - id: analyze
    type: python
    script: scripts/analyze.py
"""

AT_REF_YAML = """
name: ref_test
steps:
  - id: step1
    type: cdo
    operator_chain:
      - op: selname
        args: [tas]
    output: out.nc

  - id: step2
    type: python
    inputs:
      prev: "@step1.output"
    script: scripts/s2.py
"""

DUPLICATE_IDS_YAML = """
name: dup_test
steps:
  - id: step1
    type: python
    script: s.py
  - id: step1
    type: python
    script: s.py
"""

MISSING_REF_YAML = """
name: missing_test
steps:
  - id: step1
    type: python
    inputs:
      data: "@nonexistent.output"
    script: s.py
"""

CDO_MISSING_CHAIN_YAML = """
name: no_chain_test
steps:
  - id: step1
    type: cdo
    inputs:
      data: /data/file.nc
"""


class TestLoadSimpleYaml:
    def test_load_simple_yaml(self):
        wf = load_workflow_from_string(SIMPLE_YAML)
        assert wf.name == "test_workflow"
        assert len(wf._steps) == 2

    def test_cdo_step_created(self):
        wf = load_workflow_from_string(SIMPLE_YAML)
        assert isinstance(wf._steps[0], CdoStep)
        assert wf._steps[0].name == "select"
        assert wf._steps[0].chain is not None

    def test_python_step_created(self):
        wf = load_workflow_from_string(SIMPLE_YAML)
        step = wf._steps[1]
        assert isinstance(step, PythonStep)
        assert step.name == "process"
        assert step.script_path is not None
        assert str(step.script_path) == "scripts/process.py"

    def test_at_refs_preserved(self):
        wf = load_workflow_from_string(AT_REF_YAML)
        step2 = next(s for s in wf._steps if s.name == "step2")
        assert step2.inputs["prev"] == "@step1.output"

    def test_depends_on_loaded(self):
        wf = load_workflow_from_string(SIMPLE_YAML)
        process_step = wf._steps[1]
        assert "select" in process_step.depends_on


class TestSchemaValidation:
    def test_schema_duplicate_ids(self):
        with pytest.raises(WorkflowValidationError):
            load_workflow_from_string(DUPLICATE_IDS_YAML)

    def test_schema_missing_ref(self):
        with pytest.raises(WorkflowValidationError):
            load_workflow_from_string(MISSING_REF_YAML)

    def test_schema_cdo_missing_chain(self):
        with pytest.raises(WorkflowValidationError):
            load_workflow_from_string(CDO_MISSING_CHAIN_YAML)


class TestOperatorChain:
    def test_operator_chain_specs(self):
        """Operator chain entries should produce a CDOQueryTemplate with OperatorSpecs."""
        from python_cdo_wrapper.operators.base import OperatorSpec
        wf = load_workflow_from_string(CDO_ONLY_YAML)
        step = wf._steps[0]
        assert isinstance(step, CdoStep)
        chain = step.chain
        ops = chain._operators
        assert len(ops) == 2
        assert ops[0].name == "remapbil"
        assert ops[0].args == ("r360x180",)
        assert ops[1].name == "selname"
        assert set(ops[1].args) == {"tas", "pr"}

    def test_load_from_string(self):
        wf = load_workflow_from_string(PYTHON_ONLY_YAML)
        assert wf.name == "py_test"
        assert len(wf._steps) == 1
