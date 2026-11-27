"""JARVIS's install job outlasting its partition is the person's choice, not a failure.

jarvis-cd sizes the install job's walltime from its build estimate. When the
estimate is longer than the partition's ``MaxTime``, ``ppl install +submit``
exits 3 with an ``install_time_exceeds_partition`` refusal and candidates.
Relay asks the person which one, then sets the pick in the install pipeline's
``scheduler.install`` (a partition, or a time to build in parts) or runs a
check job to refresh JARVIS's estimate.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import install_time, jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import ask_text
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op
from clio_relay.surface.installing import install_kind
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

PARAVIEW = JobSpec(package=PackageRef(name="demo.paraview", version="1", sha256="b" * 64))
ISSUE: dict[str, Any] = {
    "schema": "jarvis.issue.v1",
    "issue_id": "sha256:" + "e" * 64,
    "kind": install_time.KIND,
    "subject": "scheduler.install.time",
    "message": (
        "the install job of p needs an estimated 3 d 2 h (startup 10 min + 2 d 1 h), longer "
        "than partition compute's MaxTime 2-00:00:00; JARVIS does not cut a build short"
    ),
    "identity": {"pipeline": "p", "partition": "compute", "max_time_s": 172800},
    "field": "scheduler.install",
    "details": {
        "needed_seconds": 266400.0,
        "max_time": "2-00:00:00",
        "partition": "compute",
        "estimate": {"human": "2 d 1 h", "formula": "(10 min + 2 d 1 h) x 1.5"},
        "already_built": 3,
    },
    "candidates": [
        {
            "set": {"scheduler.install.partition": "debug"},
            "does": "partition debug allows 4-00:00:00",
        },
        {
            "set": {"scheduler.install.time": "2-00:00:00"},
            "does": "submit up to MaxTime; if it times out, resubmit",
        },
        {
            "command": "jarvis ppl install yaml FILE +check +submit",
            "does": "refresh the check so the estimate counts only what is missing",
        },
    ],
    "remedy": "set scheduler.install.partition to one whose MaxTime fits, then submit again",
}
OUTPUT = (
    "install job time not set\n"
    + json.dumps(
        {
            "schema_version": "jarvis.refusal.v1",
            "refused": True,
            "exit_code": 3,
            "issues": [ISSUE],
            "install_time": {"needed_seconds": 266400.0},
        }
    )
    + "\n"
)


def _refused() -> Refusal:
    with pytest.raises(Refusal) as raised:
        jarvis_exits.refuse(3, OUTPUT, "", spec=PARAVIEW, site="fx", install_walltime_s=None)
    return raised.value


def test_the_exit_is_an_install_ask_with_jarvis_candidates_as_options() -> None:
    refusal = _refused()
    assert install_kind(refusal) == "software"
    assert refusal.data["kind"] == install_time.KIND
    (asked,) = jarvis_report.choice_issues(refusal.data["install_report"])
    assert jarvis_report.blocking_issues(refusal.data["install_report"]) == []
    options = jarvis_report.choice_details(asked)
    assert [(item.value, item.label) for item in options] == [
        ("partition=debug", "Build in partition debug"),
        ("time=2-00:00:00", "Submit up to 2-00:00:00, in parts"),
        (install_time.CHECK_AGAIN, "Check again first"),
    ]
    assert options[0].description == "Partition debug allows 4-00:00:00."
    question = jarvis_report.choice_message(asked)
    assert question == (
        "The build needs about 3 d 2 h, longer than compute allows (2-00:00:00): "
        "how should JARVIS run it?"
    )
    assert "does not cut a build short" in (jarvis_report.choice_detail(asked) or "")
    assert jarvis_report.choice_evidence(asked) == [
        "estimate: (10 min + 2 d 1 h) x 1.5",
        "Spack nodes already built by earlier attempts: 3",
    ]


def test_a_pick_sets_scheduler_install_and_jarvis_gets_no_answer_for_it() -> None:
    assert install_time.picked("partition=debug") == {"partition": "debug"}
    assert install_time.picked("time=2-00:00:00") == {"time": "2-00:00:00"}
    assert install_time.picked(install_time.CHECK_AGAIN) is None
    document: dict[str, object] = {
        "name": "p",
        "scheduler": {"type": "slurm", "install": {"partition": "compute", "mem": 0}},
    }
    answers: dict[str, Any] = {jarvis_submit.INSTALL_SCHEDULER: {"partition": "debug"}}
    pipeline = jarvis_submit.install_pipeline(document, None, answers)
    assert "partition: debug" in pipeline and "compute" not in pipeline and "mem: 0" in pipeline
    command = jarvis_submit.install_command(
        PARAVIEW, operation_id="op_x", jarvis="", answers=answers, pipeline=pipeline
    )
    assert jarvis_submit.INSTALL_SCHEDULER not in command


@dataclass
class _Site:
    exceeds: bool = True
    installs: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    calls: list[str] = field(default_factory=list[str])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        report: dict[str, Any] = {
            "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
            "satisfied": False,
            "installers": {"spack": {"packages": {"pkg": {"status": "missing", "missing": ["x"]}}}},
            "issues": [],
        }
        raise jarvis_report.install_needed(report, site="fx")

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        state.installs.append(dict(answers))
        if jarvis_submit.is_recheck(answers):
            state.calls.append("recheck")
            return JobHandle(adapter="fixture", native_id="check", data={"install": True})
        state.calls.append("install")
        if state.exceeds and jarvis_submit.INSTALL_SCHEDULER not in answers:
            jarvis_exits.refuse(3, OUTPUT, "", spec=spec, site="fx", install_walltime_s=None)
        return JobHandle(adapter="fixture", native_id="install", data={"install": True})

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        return JobStatus(state=JobState.RUNNING)

    for name, value in {
        "install_check": install_check,
        "install": install,
        "status": status,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


async def _to_the_install_time_ask(service: RelayService) -> str:
    operation_id = (await install_op.start(service, "fx", PARAVIEW)).operation_id
    (approve,) = service.outstanding(service.operation(operation_id))
    assert approve.kind == "approve"
    service.answer(approve.ask_id, "accept", {"approve": True}, by="test")
    await service.advance(operation_id)
    record = service.operation(operation_id)
    assert record.state == OperationState.AWAITING_INPUT, record
    return operation_id


def test_the_person_picks_a_partition_and_the_build_is_submitted_there(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = await _to_the_install_time_ask(service)
        (choice,) = service.outstanding(service.operation(operation_id))
        assert choice.kind == "choose"
        assert "how should JARVIS run it?" in ask_text(choice)
        service.answer(choice.ask_id, "accept", {"constraint": "partition=debug"}, by="test")
        await service.advance(operation_id)
        record = service.operation(operation_id)
        assert record.state == OperationState.QUEUED, record
        assert site.calls == ["check", "install", "install"]
        last = site.installs[-1]
        assert last[jarvis_submit.INSTALL_SCHEDULER] == {"partition": "debug"}
        assert ISSUE["issue_id"] not in last
        state = (record.result or {})["install"]
        assert state["install_scheduler"] == {"partition": "debug"}
        assert state["missing_keys"], state
        await service.close()

    asyncio.run(scenario())


def test_check_again_runs_a_check_job_and_sets_no_partition(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = await _to_the_install_time_ask(service)
        (choice,) = service.outstanding(service.operation(operation_id))
        value = install_time.CHECK_AGAIN
        service.answer(choice.ask_id, "accept", {"constraint": value}, by="test")
        await service.advance(operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert site.calls == ["check", "install", "recheck"]
        assert jarvis_submit.INSTALL_SCHEDULER not in site.installs[-1]
        await service.close()

    asyncio.run(scenario())
