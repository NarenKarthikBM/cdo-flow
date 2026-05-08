from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Callable

from cdo_flow.step import CdoStep, PythonStep, ResourceSpec


@dataclass
class _CdoFlowMeta:
    step_type: type
    name: str | None
    resources: ResourceSpec | None
    tags: list[str]
    keep: bool | None


def cdo_step(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    resources: ResourceSpec | dict | None = None,
    tags: list[str] | None = None,
    keep: bool | None = None,
) -> Callable:
    """Decorator to mark a function as a CDO workflow step.

    Usage:
        @cdo_step
        def my_step(ctx, cdo): ...

        @cdo_step(name="my_step", resources=ResourceSpec(cpus=4))
        def my_step(ctx, cdo): ...
    """
    def decorator(f: Callable) -> Callable:
        res = ResourceSpec(**resources) if isinstance(resources, dict) else resources
        f._cdo_flow_meta = _CdoFlowMeta(
            step_type=CdoStep,
            name=name,
            resources=res,
            tags=tags or [],
            keep=keep,
        )

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        wrapper._cdo_flow_meta = f._cdo_flow_meta
        return wrapper

    if fn is not None:
        # Bare @cdo_step usage
        return decorator(fn)
    return decorator


def python_step(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    resources: ResourceSpec | dict | None = None,
    tags: list[str] | None = None,
    keep: bool | None = None,
) -> Callable:
    """Decorator to mark a function as a Python workflow step.

    Usage:
        @python_step
        def my_step(ctx): ...

        @python_step(name="my_step")
        def my_step(ctx): ...
    """
    def decorator(f: Callable) -> Callable:
        res = ResourceSpec(**resources) if isinstance(resources, dict) else resources
        f._cdo_flow_meta = _CdoFlowMeta(
            step_type=PythonStep,
            name=name,
            resources=res,
            tags=tags or [],
            keep=keep,
        )

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        wrapper._cdo_flow_meta = f._cdo_flow_meta
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator
