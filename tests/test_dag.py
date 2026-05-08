from __future__ import annotations

import pytest

from cdo_flow.dag import (
    build_dag,
    detect_cycle,
    get_downstream_steps,
    get_leaf_steps,
    get_ready_steps,
    topological_sort,
)
from cdo_flow.exceptions import CycleDetectedError, MissingStepError
from cdo_flow.step import PythonStep


def ps(name, inputs=None, depends_on=None):
    return PythonStep(name=name, inputs=inputs or {}, depends_on=depends_on or [])


class TestBuildDag:
    def test_build_dag_infers_deps_from_at_refs(self):
        steps = [ps("regrid"), ps("select", inputs={"data": "@regrid.output"})]
        deps = build_dag(steps)
        assert "regrid" in deps["select"]

    def test_build_dag_merges_explicit_depends_on(self):
        steps = [ps("a"), ps("b", depends_on=["a"])]
        deps = build_dag(steps)
        assert "a" in deps["b"]

    def test_build_dag_raises_on_missing_step_ref(self):
        steps = [ps("a", inputs={"x": "@nonexistent.output"})]
        with pytest.raises(MissingStepError):
            build_dag(steps)

    def test_build_dag_no_deps_empty(self):
        steps = [ps("a"), ps("b")]
        deps = build_dag(steps)
        assert deps["a"] == set()
        assert deps["b"] == set()


class TestDetectCycle:
    def test_detect_cycle_simple(self):
        deps = {"a": {"b"}, "b": {"a"}}
        with pytest.raises(CycleDetectedError) as exc_info:
            detect_cycle(deps)
        assert len(exc_info.value.cycle) >= 2

    def test_detect_cycle_transitive(self):
        deps = {"a": {"b"}, "b": {"c"}, "c": {"a"}}
        with pytest.raises(CycleDetectedError):
            detect_cycle(deps)

    def test_detect_cycle_none_for_linear(self):
        deps = {"a": set(), "b": {"a"}, "c": {"b"}}
        detect_cycle(deps)  # Should not raise

    def test_detect_cycle_none_for_diamond(self):
        deps = {"a": set(), "b": {"a"}, "c": {"a"}, "d": {"b", "c"}}
        detect_cycle(deps)  # Should not raise


class TestTopologicalSort:
    def test_topological_sort_linear(self):
        steps = [ps("a"), ps("b"), ps("c")]
        deps = {"a": set(), "b": {"a"}, "c": {"b"}}
        ordered = topological_sort(steps, deps)
        names = [s.name for s in ordered]
        assert names.index("a") < names.index("b") < names.index("c")

    def test_topological_sort_diamond(self):
        steps = [ps("a"), ps("b"), ps("c"), ps("d")]
        deps = {"a": set(), "b": {"a"}, "c": {"a"}, "d": {"b", "c"}}
        ordered = topological_sort(steps, deps)
        names = [s.name for s in ordered]
        assert names.index("a") < names.index("b")
        assert names.index("a") < names.index("c")
        assert names.index("b") < names.index("d")
        assert names.index("c") < names.index("d")


class TestGetReadySteps:
    def test_get_ready_steps(self):
        steps = [ps("a"), ps("b", depends_on=["a"]), ps("c", depends_on=["b"])]
        deps = build_dag(steps)
        ready = get_ready_steps(steps, deps, completed=set(), running=set(), failed_or_skipped=set())
        assert [s.name for s in ready] == ["a"]

        ready2 = get_ready_steps(steps, deps, completed={"a"}, running=set(), failed_or_skipped=set())
        assert [s.name for s in ready2] == ["b"]

    def test_get_ready_excludes_running(self):
        steps = [ps("a"), ps("b")]
        deps = {"a": set(), "b": set()}
        ready = get_ready_steps(steps, deps, completed=set(), running={"a"}, failed_or_skipped=set())
        assert len(ready) == 1
        assert ready[0].name == "b"


class TestGetLeafSteps:
    def test_get_leaf_steps(self):
        steps = [ps("a"), ps("b", depends_on=["a"]), ps("c", depends_on=["a"])]
        deps = build_dag(steps)
        leaves = get_leaf_steps(steps, deps)
        assert leaves == {"b", "c"}

    def test_single_step_is_leaf(self):
        steps = [ps("solo")]
        deps = build_dag(steps)
        assert get_leaf_steps(steps, deps) == {"solo"}


class TestGetDownstreamSteps:
    def test_get_downstream_steps(self):
        deps = {"a": set(), "b": {"a"}, "c": {"b"}, "d": {"b"}}
        downstream = get_downstream_steps("a", deps)
        assert downstream == {"b", "c", "d"}

    def test_no_downstream(self):
        deps = {"a": set(), "b": {"a"}}
        assert get_downstream_steps("b", deps) == set()
