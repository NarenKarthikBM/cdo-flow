from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

from cdo_flow.step import StepState


@dataclass
class StepEvent:
    type: Literal["step_started", "step_done", "step_failed", "step_skipped"]
    step_name: str
    state: StepState
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float | None = None
    outputs: dict[str, str] | None = None  # str paths for serialisability
    error: str | None = None
    command: str | None = None
