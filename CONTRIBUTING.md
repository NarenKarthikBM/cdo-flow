# Contributing to cdo-flow

Thank you for considering a contribution. This document covers how to set up a development environment, run tests, follow code conventions, and submit changes.

## Table of contents

- [Development setup](#development-setup)
- [Running tests](#running-tests)
- [Code style](#code-style)
- [Project structure](#project-structure)
- [Submitting changes](#submitting-changes)
- [Reporting issues](#reporting-issues)

---

## Development setup

**Prerequisites:** Python >= 3.10, [CDO](https://mpimet.mpg.de/cdo) on `$PATH`.

```bash
git clone https://github.com/narenkarthikbm/cdo-flow.git
cd cdo-flow
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

The `dev` extra installs `pytest`, `pytest-mock`, `pytest-cov`, `ruff`, and `mypy`.

---

## Running tests

```bash
pytest                          # run all tests
pytest tests/test_dag.py        # single file
pytest --cov=cdo_flow           # with coverage report
```

Tests that require CDO to actually execute are marked `@pytest.mark.integration` and are skipped by default unless CDO is on `$PATH` and you pass `-m integration`.

---

## Code style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [mypy](https://mypy-lang.org/) for type checking.

```bash
ruff check .                    # lint
ruff format .                   # format
mypy cdo_flow                   # type check
```

Configuration lives in `pyproject.toml` under `[tool.ruff]`.

Key conventions:
- Line length: 100 characters
- Target Python: 3.10+
- Use `from __future__ import annotations` at the top of every module
- Public functions and classes must have type annotations
- No bare `except:` — always catch a specific exception type

---

## Project structure

```
cdo_flow/
  __init__.py          Public API exports
  workflow.py          Workflow class + WorkflowResult
  step.py              Step dataclasses, StepContext, StepState
  decorators.py        @cdo_step, @python_step
  dag.py               DAG build, cycle detection, topological sort
  events.py            StepEvent dataclass
  provenance.py        RunDirectory + ProvenanceBuilder
  exceptions.py        Exception hierarchy
  cli.py               Click CLI (run, validate, inspect, create, history)
  executors/
    base.py            BaseExecutor ABC
    local.py           LocalExecutor (ProcessPoolExecutor)
    snakemake.py       SnakemakeExecutor (stub, v0.3)
  config/
    schema.py          Pydantic validation schemas
    loader.py          YAML → Workflow loader
  tui/
    execution_view.py  Live execution TUI (textual)
    creation_wizard.py Workflow creation wizard (textual)
    history_browser.py Run history browser (textual)
tests/
  test_dag.py
  test_workflow.py
  test_executor.py
  test_map.py
  test_config.py
```

---

## Submitting changes

1. **Open an issue first** for non-trivial changes to discuss the approach before writing code.
2. Fork the repository and create a branch from `main`:
   ```bash
   git checkout -b fix/my-fix
   git checkout -b feat/my-feature
   ```
3. Make your changes. Add or update tests for any new behaviour.
4. Ensure all checks pass locally:
   ```bash
   ruff check . && ruff format --check . && mypy cdo_flow && pytest
   ```
5. Commit with a short, imperative subject line:
   ```
   fix: handle missing provenance.json in inspect command
   feat: add --timeout option to run command
   ```
6. Open a pull request against `main`. Fill in the PR template.

### Pull request checklist

- [ ] Tests added or updated
- [ ] `ruff` and `mypy` pass with no new errors
- [ ] `CHANGELOG.md` entry added (if one exists)
- [ ] Public API changes reflected in `README.md`

---

## Reporting issues

Use the [GitHub issue tracker](https://github.com/narenkarthikbm/cdo-flow/issues).

Include:
- `cdo-flow --version` output
- `cdo --version` output
- Python version (`python --version`)
- A minimal reproducible example (YAML or Python snippet + error message)

---

## Questions

Open a [GitHub Discussion](https://github.com/narenkarthikbm/cdo-flow/discussions) for questions that are not bug reports or feature requests.
