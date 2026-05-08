from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cdo_flow.step import CdoStep, PythonStep, ResourceSpec


@pytest.fixture
def mock_cdo_class(monkeypatch):
    with patch("cdo_flow.executors.local.CDO") as MockCDO:
        instance = MagicMock()
        MockCDO.return_value = instance
        yield instance


def make_python_step(name, inputs=None, depends_on=None, keep=None):
    return PythonStep(
        name=name,
        inputs=inputs or {},
        depends_on=depends_on or [],
        keep=keep,
    )


def make_cdo_step(name, inputs=None, depends_on=None, chain=None, keep=None):
    return CdoStep(
        name=name,
        inputs=inputs or {},
        depends_on=depends_on or [],
        chain=chain,
        keep=keep,
    )
