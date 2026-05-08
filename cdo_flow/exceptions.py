from __future__ import annotations


class CdoFlowError(Exception):
    """Base exception for cdo-flow."""


class CycleDetectedError(CdoFlowError):
    """Raised when a cycle is detected in the workflow DAG."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__(f"Cycle detected in workflow DAG: {' -> '.join(cycle)}")


class MissingStepError(CdoFlowError):
    """Raised when a step references a non-existent step via @step.output."""

    def __init__(self, ref: str, referencing_step: str) -> None:
        self.ref = ref
        self.referencing_step = referencing_step
        super().__init__(
            f"Step '{referencing_step}' references unknown step '{ref}' via @{ref}.output"
        )


class StepExecutionError(CdoFlowError):
    """Raised when a step fails during execution."""

    def __init__(self, step_name: str, cause: Exception) -> None:
        self.step_name = step_name
        self.cause = cause
        super().__init__(f"Step '{step_name}' failed: {cause}")

    @property
    def command(self) -> str | None:
        return getattr(self.cause, "command", None)

    @property
    def returncode(self) -> int | None:
        return getattr(self.cause, "returncode", None)

    @property
    def stdout(self) -> str | None:
        return getattr(self.cause, "stdout", None)

    @property
    def stderr(self) -> str | None:
        return getattr(self.cause, "stderr", None)


class WorkflowValidationError(CdoFlowError):
    """Raised when workflow validation fails."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("Workflow validation failed:\n" + "\n".join(f"  - {e}" for e in errors))


class InputFileNotFoundError(CdoFlowError):
    """Raised when a literal input file does not exist."""

    def __init__(self, step_name: str, input_key: str, path: str) -> None:
        self.step_name = step_name
        self.input_key = input_key
        super().__init__(
            f"Step '{step_name}' input '{input_key}': file not found: {path}"
        )
