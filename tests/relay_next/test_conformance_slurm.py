"""The conformance kit against real Slurm (RL7).

Collected only when RELAY_TEST_SLURM_DIR names a directory the compute nodes
share; tests/relay_next/conftest.py leaves this module out otherwise.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from clio_relay.jobs.records import new_id
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget

pytestmark = pytest.mark.release_platform("posix")


class TestLocalSlurm(AdapterConformance):
    @pytest.fixture
    def target(self) -> ConformanceTarget:
        state = Path(os.environ["RELAY_TEST_SLURM_DIR"]) / new_id("conformance")
        config: dict[str, object] = {"scheduler": "slurm"}
        return ConformanceTarget(
            adapter="local",
            config=config,
            state_dir=state,
            job_timeout_s=float(os.environ.get("RELAY_TEST_SLURM_TIMEOUT_S", "300")),
            poll_s=2.0,
        )
