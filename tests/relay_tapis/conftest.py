"""Shared setup for the ``tapis`` adapter tests.

Every module runs against the Tapis stub in ``tests/fixtures/tapis_stub``. The
modules that also log in to W2's SSH fixture are collected only when the
fixtures' env file exists, at ``$CLIO_RELAY_FIXTURES_ENV`` or the default path.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths
from tests.fixtures.tapis_stub import TapisStub

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


collect_ignore = [] if fixtures_env_path() is not None else ["test_tapis_register.py"]


@pytest.fixture(scope="session")
def fx() -> dict[str, str]:
    return read_fixtures_env()


@pytest.fixture(scope="module")
def stub() -> Iterator[TapisStub]:
    with TapisStub() as running:
        yield running


@pytest.fixture
def relay_paths(tmp_path: Path) -> RelayPaths:
    return RelayPaths(config_dir=tmp_path / "config", state_dir=tmp_path / "state")


def add_site(paths: RelayPaths, data: dict[str, Any]) -> SiteEntry:
    return SiteStore(paths.sites_file).add(SiteEntry.model_validate(data))
