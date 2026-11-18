"""The conformance kit run against the adapters relay ships (RL7)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from clio_relay.jobs.records import new_id
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


_SLURM_DIR = os.environ.get("RELAY_TEST_SLURM_DIR")


@pytest.mark.skipif(
    _SLURM_DIR is None,
    reason="set RELAY_TEST_SLURM_DIR to a directory compute nodes share to run the Slurm suite",
)
class TestLocalSlurm(AdapterConformance):
    @pytest.fixture
    def target(self) -> ConformanceTarget:
        assert _SLURM_DIR is not None
        state = Path(_SLURM_DIR) / new_id("conformance")
        config: dict[str, object] = {"scheduler": "slurm"}
        return ConformanceTarget(
            adapter="local",
            config=config,
            state_dir=state,
            job_timeout_s=float(os.environ.get("RELAY_TEST_SLURM_TIMEOUT_S", "300")),
            poll_s=2.0,
        )
