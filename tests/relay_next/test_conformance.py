"""The conformance kit run against the adapters relay ships (RL7)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.adapters.local import LocalAdapter
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget
from clio_relay.testing.fixture_adapter import FixtureAdapter

pytestmark = pytest.mark.release_platform("posix")


class TestFixtureAdapter(AdapterConformance):
    adapter = FixtureAdapter

    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="fixture",
            config={"queue_s": 0.2, "run_s": 0.4},
            state_dir=tmp_path,
            job_timeout_s=20,
            poll_s=0.1,
        )


class TestLocalSupervised(AdapterConformance):
    adapter = LocalAdapter

    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="local",
            config={"scheduler": "none"},
            state_dir=tmp_path,
            job_timeout_s=30,
            poll_s=0.2,
        )
