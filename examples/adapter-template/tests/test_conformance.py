"""Run relay's conformance kit against the Acme adapter and the fake facility."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fake_facility import FakeFacility, running
from relay_acme import AcmeAdapter

from clio_relay.adapters import registry
from clio_relay.identity.store import TokenStore
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget

TOKEN = "fake-acme-token"


@pytest.fixture
def facility_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Start the fake facility and give relay its token."""
    monkeypatch.setenv("CLIO_RELAY_KEYRING", "memory")
    TokenStore().set_secret("acme-token", TOKEN)
    # Installed, the entry point in pyproject.toml registers the adapter; this
    # registers it when the tests run from a checkout without installing it.
    registry.register(AcmeAdapter)
    with running(FakeFacility(tmp_path / "facility", TOKEN)) as url:
        yield url


class TestAcme(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path, facility_url: str) -> ConformanceTarget:
        return ConformanceTarget(adapter="acme", config={"url": facility_url}, state_dir=tmp_path)
