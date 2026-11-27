"""LQ4 for ``ssh``: the conformance kit on W2's fx-ssh, fx-slurm and fx-pbs sites.

Collected only where the fixtures' env file exists (see conftest.py).
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from clio_relay.adapters.ssh.adapter import SshAdapter
from clio_relay.jobs.records import new_id
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget

pytestmark = pytest.mark.release_platform("posix")

RUN_DIR = f".clio-relay-test/{new_id('w7')}"


def ssh_target(
    fx: dict[str, str], tmp_path: Path, profile: str, scheduler: str, **extra: object
) -> ConformanceTarget:
    config: dict[str, object] = {
        "ssh_profile": profile,
        "ssh_config": fx["FX_SSH_CONFIG"],
        "remote_dir": f"{RUN_DIR}/{profile}",
        "scheduler": scheduler,
    }
    return ConformanceTarget(
        adapter="ssh",
        config=config,
        state_dir=tmp_path,
        site_id=profile,
        **extra,  # pyright: ignore[reportArgumentType]
    )


@pytest.fixture(scope="module", autouse=True)
def _clean_remote(fx: dict[str, str]) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    yield
    if os.environ.get("RELAY_SSH_KEEP"):
        return
    for profile in ("fx-ssh", "fx-slurm", "fx-pbs"):
        subprocess.run(
            ["ssh", "-F", fx["FX_SSH_CONFIG"], "-o", "BatchMode=yes", profile, f"rm -rf {RUN_DIR}"],
            check=False,
            capture_output=True,
            timeout=120,
        )


class TestSshDetached(AdapterConformance):
    adapter = SshAdapter

    @pytest.fixture
    def target(self, fx: dict[str, str], tmp_path: Path) -> ConformanceTarget:
        return ssh_target(fx, tmp_path, "fx-ssh", "auto", poll_s=1.0, job_timeout_s=120)


class TestSshSlurm(AdapterConformance):
    adapter = SshAdapter

    @pytest.fixture
    def target(self, fx: dict[str, str], tmp_path: Path) -> ConformanceTarget:
        return ssh_target(fx, tmp_path, "fx-slurm", "auto", poll_s=2.0, job_timeout_s=300)


class TestSshPbs(AdapterConformance):
    adapter = SshAdapter

    @pytest.fixture
    def target(self, fx: dict[str, str], tmp_path: Path) -> ConformanceTarget:
        return ssh_target(fx, tmp_path, "fx-pbs", "auto", poll_s=2.0, job_timeout_s=300)
