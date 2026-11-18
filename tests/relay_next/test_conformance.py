"""The conformance kit run against the adapters relay ships (RL7)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget

pytestmark = pytest.mark.release_platform("posix")


class TestFixtureAdapter(AdapterConformance):
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
    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="local",
            config={"scheduler": "none"},
            state_dir=tmp_path,
            job_timeout_s=30,
            poll_s=0.2,
        )
