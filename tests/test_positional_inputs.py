from __future__ import annotations

import pytest
from click.testing import CliRunner

from cdo_flow.cli import _map_positional_inputs, cli
from cdo_flow.config.loader import load_workflow_from_string
from cdo_flow.decorators import python_step
from cdo_flow.workflow import Workflow


# ── _map_positional_inputs unit tests ────────────────────────────────────────

YAML_WITH_DECLARED_INPUTS = """
name: declared_wf
inputs:
  - data
  - reference
steps:
  - id: process
    type: python
    inputs:
      data: data
      ref: reference
    script: scripts/p.py
"""

YAML_WITHOUT_DECLARED_INPUTS = """
name: auto_detect_wf
steps:
  - id: step1
    type: python
    inputs:
      src: MY_DATA
    script: scripts/s1.py
  - id: step2
    type: python
    inputs:
      prev: "@step1.output"
      ref: MY_REF
    script: scripts/s2.py
"""


class TestMapPositionalInputsDeclared:
    def test_maps_files_to_declared_keys(self):
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        result = _map_positional_inputs(("a.nc", "b.nc"), wf)
        assert result == {"data": "a.nc", "reference": "b.nc"}

    def test_partial_mapping_allowed(self):
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        result = _map_positional_inputs(("a.nc",), wf)
        assert result == {"data": "a.nc"}

    def test_empty_files_returns_empty(self):
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        assert _map_positional_inputs((), wf) == {}

    def test_too_many_files_raises(self):
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        with pytest.raises(Exception, match="3 positional file"):
            _map_positional_inputs(("a.nc", "b.nc", "c.nc"), wf)


class TestMapPositionalInputsAutoDetect:
    def test_auto_detects_non_ref_values(self):
        wf = load_workflow_from_string(YAML_WITHOUT_DECLARED_INPUTS)
        result = _map_positional_inputs(("a.nc", "b.nc"), wf)
        # MY_DATA is first occurrence, MY_REF is second; @ref is skipped
        assert result == {"MY_DATA": "a.nc", "MY_REF": "b.nc"}

    def test_auto_detects_single_file(self):
        wf = load_workflow_from_string(YAML_WITHOUT_DECLARED_INPUTS)
        result = _map_positional_inputs(("a.nc",), wf)
        assert result == {"MY_DATA": "a.nc"}

    def test_skips_at_refs(self):
        wf = load_workflow_from_string(YAML_WITHOUT_DECLARED_INPUTS)
        keys_detected = list(_map_positional_inputs(("a.nc", "b.nc"), wf).keys())
        assert "@step1.output" not in keys_detected

    def test_no_detectable_inputs_raises(self):
        # Construct a workflow whose only inputs are @ref — nothing to auto-detect
        from cdo_flow.step import PythonStep
        wf = Workflow("no_inputs_wf")
        step = PythonStep(name="s", inputs={"prev": "@other.output"})
        wf._steps.append(step)
        with pytest.raises(Exception, match="no workflow inputs detected"):
            _map_positional_inputs(("a.nc",), wf)


class TestMapPositionalInputsPrecedence:
    def test_explicit_flag_overrides_positional(self):
        """Simulates merging: positional first, -i flags on top."""
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        positional = _map_positional_inputs(("a.nc",), wf)
        explicit = {"data": "override.nc"}
        merged = {**positional, **explicit}
        assert merged["data"] == "override.nc"


# ── YAML schema: declared inputs parsed by loader ────────────────────────────

class TestDeclaredInputsLoaded:
    def test_declared_inputs_on_workflow(self):
        wf = load_workflow_from_string(YAML_WITH_DECLARED_INPUTS)
        assert wf.declared_inputs == ["data", "reference"]

    def test_no_declared_inputs_defaults_to_empty(self):
        wf = load_workflow_from_string(YAML_WITHOUT_DECLARED_INPUTS)
        assert wf.declared_inputs == []

    def test_workflow_constructor_default(self):
        wf = Workflow("test")
        assert wf.declared_inputs == []


# ── CLI integration via CliRunner ─────────────────────────────────────────────

class TestCliPositionalFiles:
    def test_too_many_files_exits_with_error(self, tmp_path):
        """CLI should exit non-zero with a clear error for too many files."""
        yaml_content = """
name: wf
inputs:
  - data
steps:
  - id: s
    type: python
    inputs:
      data: data
    script: scripts/s.py
"""
        wf_file = tmp_path / "wf.yml"
        wf_file.write_text(yaml_content)

        runner = CliRunner()
        result = runner.invoke(cli, ["run", str(wf_file), "a.nc", "b.nc"])
        assert result.exit_code != 0
        assert "2 positional file" in result.output or "positional" in result.output.lower()

    def test_positional_files_reach_executor(self, tmp_path):
        """Files passed positionally should be resolved to the correct keys."""
        from cdo_flow.decorators import python_step

        # Write a real step that checks its input key
        script = tmp_path / "check.py"
        script.write_text(
            "import json, sys\n"
            "ctx = json.load(open(sys.argv[1]))\n"
            "assert 'data' in ctx['inputs'], ctx['inputs']\n"
        )

        input_file = tmp_path / "input.txt"
        input_file.write_text("hello")

        yaml_content = f"""
name: pos_test
inputs:
  - data
steps:
  - id: check
    type: python
    inputs:
      data: data
    script: {script}
"""
        wf_file = tmp_path / "wf.yml"
        wf_file.write_text(yaml_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["run", str(wf_file), str(input_file)],
        )
        assert result.exit_code == 0, result.output
