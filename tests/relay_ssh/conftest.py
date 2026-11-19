"""Shared setup for the ``ssh`` adapter tests.

The fixture modules need W2's SSH sites. They are collected only when the
fixtures' env file exists, at ``$CLIO_RELAY_FIXTURES_ENV`` or the default path.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

DEFAULT_ENV = Path.home() / "clio-relay-rework" / "state" / "w2-fixtures" / "fixtures.env"


def fixtures_env_path() -> Path | None:
    configured = os.environ.get("CLIO_RELAY_FIXTURES_ENV")
    path = Path(configured) if configured else DEFAULT_ENV
    return path if path.is_file() else None


def read_fixtures_env() -> dict[str, str]:
    path = fixtures_env_path()
    assert path is not None
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


collect_ignore = (
    []
    if fixtures_env_path() is not None
    else [
        "test_ssh_fixtures.py",
        "test_ssh_conformance.py",
        "test_ssh_store_fixtures.py",
        "test_ssh_link_fixtures.py",
    ]
)


@pytest.fixture(scope="session")
def fx() -> dict[str, str]:
    return read_fixtures_env()
