"""The site script's plan mode, and the submit bound to the plan the person said yes to.

Only relay's pinned JARVIS plans: an older one ignores an unknown ``+plan`` and
would submit, so a JARVIS the site's setting names submits unbound
(``test_submit_hands_the_pipeline_to_jarvis_ppl_submit``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from clio_relay.jarvis_plan import BIND_ENV, EXIT_PLANNED, PLAN_FILE, planned
from clio_relay.jarvis_submit import FALLBACK_ENV, PIN_ENV, PIPELINE_FILE, SUBMISSION_FILE

from .test_scripts import (
    PIPELINE,
    _env,  # pyright: ignore[reportPrivateUsage]
    _jarvis,  # pyright: ignore[reportPrivateUsage]
    _job_dir,  # pyright: ignore[reportPrivateUsage]
    _site,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.release_platform("posix")

PIN = "ab12cd"


def _pinned(tmp_path: Path, **extra: str) -> dict[str, str]:
    """The stand-in as relay's own JARVIS, at the pin relay asks for."""
    jarvis = _jarvis(tmp_path)
    (jarvis.parent / "pin").write_text(PIN + "\n")
    return _env(tmp_path, tmp_path / "bin", **{FALLBACK_ENV: str(jarvis), PIN_ENV: PIN}, **extra)


def test_plan_makes_jarvis_plan_and_submits_nothing(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _pinned(tmp_path, JARVIS_PLAN_SHA="feed")
    done = _site(job, env, digest, "demo.hello", "", "op_x", mode="plan")
    assert done.returncode == EXIT_PLANNED, done.stderr
    plan = planned(done.returncode, done.stdout)
    assert plan is not None and plan["plan_sha256"] == "feed"
    log = (tmp_path / "jarvis.log").read_text().splitlines()
    assert log[-1].endswith(f"ppl submit {job}/{PIPELINE_FILE} --execution-id op_x +plan +json")
    assert sum("ppl submit" in line for line in log) == 1
    assert "+check" not in "\n".join(log)
    assert not (job / SUBMISSION_FILE).exists()


def test_a_jarvis_that_cannot_plan_makes_no_plan_and_submits_unbound(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _pinned(tmp_path, JARVIS_PLAN_RC="2")
    done = _site(job, env, digest, "demo.hello", "", "op_x", mode="plan")
    assert done.returncode == 2
    assert planned(done.returncode, done.stdout) is None
    done = _site(job, env, digest, "demo.hello", "", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    last = (tmp_path / "jarvis.log").read_text().splitlines()[-1]
    assert last.endswith("--execution-id op_x +json") and "--plan_sha256" not in last
    assert not (job / PLAN_FILE).exists()


def test_a_bound_submit_names_the_plan_the_person_said_yes_to(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _pinned(tmp_path, **{BIND_ENV: "sha256:c0ffee"})
    done = _site(job, env, digest, "demo.hello", "", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    log = (tmp_path / "jarvis.log").read_text()
    assert "+plan" not in log
    assert "--execution-id op_x --plan_sha256 c0ffee +json" in log


def test_the_pinned_jarvis_plans_the_submit_and_binds_it(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _pinned(tmp_path)
    done = _site(job, env, digest, "demo.hello", "", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    submits = [
        line for line in (tmp_path / "jarvis.log").read_text().splitlines() if "ppl submit" in line
    ]
    assert submits[0].endswith("--execution-id op_x +plan +json")
    assert submits[1].endswith("--execution-id op_x --plan_sha256 ab12 +json")
    assert json.loads((job / PLAN_FILE).read_text())["plan_sha256"] == "ab12"
