from __future__ import annotations

import re
from collections import deque
from typing import TYPE_CHECKING

from cdo_flow.exceptions import CycleDetectedError, MissingStepError

if TYPE_CHECKING:
    from cdo_flow.step import BaseStep

# Matches @step_id.output in input values
_AT_REF_PATTERN = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)\.output$")


def _parse_at_ref(value: str) -> str | None:
    """Return step_id if value is an @step_id.output ref, else None."""
    if isinstance(value, str):
        m = _AT_REF_PATTERN.match(value.strip())
        if m:
            return m.group(1)
    return None


def build_dag(steps: list[BaseStep]) -> dict[str, set[str]]:
    """Build dependency map: step_name -> set of step names it depends on."""
    step_names = {s.name for s in steps}
    deps: dict[str, set[str]] = {s.name: set() for s in steps}

    for step in steps:
        # Infer deps from @step_id.output refs in inputs
        for val in step.inputs.values():
            ref = _parse_at_ref(str(val))
            if ref is not None:
                if ref not in step_names:
                    raise MissingStepError(ref, step.name)
                deps[step.name].add(ref)

        # Merge explicit depends_on
        for dep in step.depends_on:
            if dep not in step_names:
                raise MissingStepError(dep, step.name)
            deps[step.name].add(dep)

    return deps


def detect_cycle(deps: dict[str, set[str]]) -> None:
    """Raise CycleDetectedError if a cycle exists in deps."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in deps}
    path: list[str] = []

    def dfs(node: str) -> None:
        color[node] = GRAY
        path.append(node)
        for neighbor in deps.get(node, set()):
            if color[neighbor] == GRAY:
                cycle_start = path.index(neighbor)
                raise CycleDetectedError(path[cycle_start:] + [neighbor])
            if color[neighbor] == WHITE:
                dfs(neighbor)
        path.pop()
        color[node] = BLACK

    for node in list(deps):
        if color[node] == WHITE:
            dfs(node)


def topological_sort(steps: list[BaseStep], deps: dict[str, set[str]]) -> list[BaseStep]:
    """Return steps in topological order using Kahn's algorithm."""
    step_map = {s.name: s for s in steps}
    in_degree = {s.name: 0 for s in steps}

    # Build reverse map: who depends on each node
    reverse: dict[str, set[str]] = {s.name: set() for s in steps}
    for node, node_deps in deps.items():
        for dep in node_deps:
            reverse[dep].add(node)
            in_degree[node] += 0  # already zero
        in_degree[node] = len(node_deps)

    queue = deque(name for name, deg in in_degree.items() if deg == 0)
    result: list[BaseStep] = []

    while queue:
        name = queue.popleft()
        result.append(step_map[name])
        for dependent in sorted(reverse[name]):  # sorted for determinism
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    return result


def get_ready_steps(
    all_steps: list[BaseStep],
    deps: dict[str, set[str]],
    completed: set[str],
    running: set[str],
    failed_or_skipped: set[str],
) -> list[BaseStep]:
    """Return steps whose deps are all completed and which aren't running/done/failed."""
    terminal = completed | running | failed_or_skipped
    ready = []
    for step in all_steps:
        if step.name in terminal:
            continue
        if deps[step.name].issubset(completed):
            # Check none of its deps failed (would cause it to be skipped)
            if not deps[step.name].intersection(failed_or_skipped):
                ready.append(step)
    return ready


def get_leaf_steps(steps: list[BaseStep], deps: dict[str, set[str]]) -> set[str]:
    """Return step names that no other step depends on."""
    has_consumers: set[str] = set()
    for node_deps in deps.values():
        has_consumers.update(node_deps)
    return {s.name for s in steps} - has_consumers


def get_downstream_steps(step_name: str, deps: dict[str, set[str]]) -> set[str]:
    """Return all transitive dependents of step_name."""
    # Build reverse map
    reverse: dict[str, set[str]] = {n: set() for n in deps}
    for node, node_deps in deps.items():
        for dep in node_deps:
            reverse[dep].add(node)

    visited: set[str] = set()
    queue = deque(reverse.get(step_name, set()))
    while queue:
        node = queue.popleft()
        if node not in visited:
            visited.add(node)
            queue.extend(reverse.get(node, set()) - visited)
    return visited
