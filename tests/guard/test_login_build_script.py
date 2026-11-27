"""The site script's install placement: JARVIS's build on the login host, or its job.

JARVIS's install report says where the site builds (``placement.build.choice``).
When that is the login host, the site script starts JARVIS's own ``ppl install
--answers`` there detached, under the supervisor files the adapters follow, and
sets no limits; otherwise the build is JARVIS's install job (``+submit``).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from clio_relay.jarvis_submit import PIPELINE_FILE, parse_handle

from .test_scripts import (
    PIPELINE,
    PLACEMENT,
    _env,  # pyright: ignore[reportPrivateUsage]
    _jarvis,  # pyright: ignore[reportPrivateUsage]
    _job_dir,  # pyright: ignore[reportPrivateUsage]
    _python,  # pyright: ignore[reportPrivateUsage]
    _site,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.release_platform("posix")

MISSING = '[{"kind":"not_installed","answerable":false,"package":"pkg"}]'


def _placed(build: str, *, defer: bool = False) -> str:
    return json.dumps({"build": {"choice": build}, "evidence": {"limits": {"defer": defer}}})


def _install(tmp_path: Path, **extra: str) -> tuple[Path, subprocess.CompletedProcess[str]]:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    _python(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_INSTALL_RC="3", **extra)
    answers, install = '{"sha256:p":"login_limited"}', "name: relay_op_x_install\n"
    done = _site(
        job, env, digest, "demo.hello", "jarvis", "op_x-install", answers, install, mode="install"
    )
    return job, done


def _ended(directory: Path) -> dict[str, object]:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = directory / "state.json"
        if (directory / "exit_code").is_file() and state.is_file():
            return json.loads(state.read_text())
        time.sleep(0.1)
    raise AssertionError(f"the login build in {directory} did not end")


def test_an_install_placed_on_the_login_host_starts_jarvis_build_detached(
    tmp_path: Path,
) -> None:
    job, done = _install(tmp_path, JARVIS_ISSUES=MISSING, JARVIS_PLACEMENT=_placed("login_limited"))
    assert done.returncode == 0, done.stderr
    started = json.loads(done.stdout.strip().splitlines()[-1])
    assert started["schema_version"] == "clio-relay.login-build.v1"
    directory = job / "relay.login-build"
    assert started["dir"] == str(directory) and isinstance(started["pid"], int)
    assert _ended(directory)["state"] == "completed"
    assert (directory / "exit_code").read_text().strip() == "0"
    log = (tmp_path / "jarvis.log").read_text()
    pipeline = (
        f"ppl install yaml {job}/relay.install-pipeline.yaml --answers {job}/relay.answers.json"
    )
    assert f"{pipeline} +check +json" in log
    # JARVIS's own build, which sets and applies the limits: relay adds none and no job.
    assert any(line.endswith(f"{pipeline} +json") for line in log.splitlines())
    assert "+submit" not in log
    assert not (job / "relay.install.json").exists()


def test_a_cancel_stops_the_login_build_and_says_so(tmp_path: Path) -> None:
    job, done = _install(
        tmp_path,
        JARVIS_ISSUES=MISSING,
        JARVIS_PLACEMENT=_placed("login_limited"),
        JARVIS_BUILD_SLEEP="30",
    )
    assert done.returncode == 0, done.stderr
    directory = job / "relay.login-build"
    deadline = time.monotonic() + 20
    while not (directory / "child_pid").is_file() and time.monotonic() < deadline:
        time.sleep(0.1)
    (directory / "cancel").write_text("")
    # The ssh cancel script's signal: the whole process group of JARVIS's build.
    os.kill(-int((directory / "child_pid").read_text()), 15)
    state = _ended(directory)
    assert state["state"] == "cancelled" and state["exit_code"] != 0


@pytest.mark.parametrize(
    "placement", [_placed("compute_job"), _placed("login_limited", defer=True), "null"]
)
def test_an_install_jarvis_did_not_place_here_is_its_job(tmp_path: Path, placement: str) -> None:
    job, done = _install(tmp_path, JARVIS_ISSUES=MISSING, JARVIS_PLACEMENT=placement)
    assert done.returncode == 0, done.stderr
    assert "+submit --execution-id op_x-install +json" in (tmp_path / "jarvis.log").read_text()
    assert not (job / "relay.login-build").exists()
    handle = parse_handle((job / "relay.install.json").read_text())
    assert handle is not None and handle["execution_id"] == "install"


def test_an_install_with_a_placement_question_asks_it_first(tmp_path: Path) -> None:
    job, done = _install(tmp_path, JARVIS_ISSUES=PLACEMENT)
    assert done.returncode == 3, done.stderr
    assert "+submit" not in (tmp_path / "jarvis.log").read_text()
    assert not (job / "relay.login-build").exists()
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["issues"] == json.loads(PLACEMENT)
