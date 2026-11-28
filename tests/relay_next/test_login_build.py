"""A build the site placed on its login host runs there as JARVIS's own background task.

JARVIS's install report says where it builds (``placement.build.choice``), from
the site's answer to its ``install_placement`` question. When that is the login
host (``login_limited``), the site script starts JARVIS's ``ppl install
--answers`` there detached and returns at once: relay follows it like an install
job, with no time limit, sets no limits of its own, and only a cancel stops it.
A compute_job placement still goes through ``ppl install +submit``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import install_wording, jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import ask_text
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_logs, install_op
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

PACKAGE = JobSpec(package=PackageRef(name="demo.paraview", version="1", sha256="c" * 64))
STARTED = {
    "schema_version": jarvis_exits.LOGIN_BUILD_SCHEMA,
    "dir": "/home/u/job/relay.login-build",
    "pid": 4242,
    "host": "login1",
    "log": "/home/u/job/shared/p/install/install.log",
}


def _report(*, missing: bool, choice: str | None = "login_limited") -> dict[str, Any]:
    package: dict[str, Any] = {
        "status": "missing" if missing else "satisfied",
        "missing": ["paraview"] if missing else None,
    }
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": not missing,
        "installers": {"spack": {"packages": {"pkg": package}}},
        "issues": [],
        "placement": {"build": {"choice": choice, "questions": []}},
    }


def test_the_site_script_start_document_is_a_login_build_handle() -> None:
    printed = "JARVIS checking\n" + json.dumps(STARTED)
    ssh = jarvis_exits.without_job("ssh", 0, printed, None, "detached", {"root": "/r"})
    assert ssh is not None and ssh.native_id == "pid:4242"
    assert ssh.data["mode"] == "detached" and ssh.data["job_dir"] == STARTED["dir"]
    assert ssh.data["root"] == "/r" and "supervisor_pid" not in ssh.data
    local = jarvis_exits.without_job("local", "0", printed, None, "supervised", {})
    assert local is not None and local.data["supervisor_pid"] == 4242
    # Through a site worker the handle is wrapped; relay still knows it is a login build.
    wrapped = JobHandle(adapter="worker", data={"remote": local.model_dump(mode="json")})
    assert jarvis_exits.built_here(wrapped) == {
        "pid": 4242,
        "host": "login1",
        "log": STARTED["log"],
        "dir": STARTED["dir"],
    }
    assert not jarvis_exits.ran_here(wrapped)
    # A failed start or a report is not a login build.
    assert jarvis_exits.login_build("ssh", 1, json.dumps(STARTED), mode="detached") is None
    report = json.dumps(_report(missing=False))
    here = jarvis_exits.without_job("ssh", 0, report, None, "detached", {})
    assert (
        here is not None and jarvis_exits.ran_here(here) and jarvis_exits.built_here(here) is None
    )


def test_an_install_job_log_is_found_through_a_site_worker() -> None:
    """The worker wraps the site's handle; the install job's log is still read."""
    execution = "/home/u/runs/op_x/relay_op_x/executions/op_x-install-2"
    site = JobHandle(adapter="local", native_id="29154", data={"execution_dir": execution})
    wrapped = JobHandle(adapter="worker", data={"remote": site.model_dump(mode="json")})
    assert install_logs.log_path(wrapped) == f"{execution}/install/install.log"
    assert install_logs.log_path(site) == install_logs.log_path(wrapped)


def test_the_approval_says_where_jarvis_builds() -> None:
    def said(choice: str | None, questions: list[str] | None = None) -> str:
        report = _report(missing=True, choice=choice)
        report["placement"]["build"]["questions"] = questions or []
        return install_wording.install_message(
            report, site="Lab", package="demo.paraview", walltime="04:00:00"
        )

    login = said("login_limited")
    assert "on Lab's login host under its own limits" in login
    assert "no time limit that you can stop" in login and "install job" not in login
    assert "as an install job of up to 04:00:00" in said("compute_job")
    assert "as an install job of up to 04:00:00" in said(None)
    assert "is one of its questions" in said(None, ["sha256:b"])


def test_a_refused_login_build_says_why_and_is_final() -> None:
    printed = json.dumps(
        {
            "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
            "refused": {"kind": "build_outside_allocation", "what": "build", "packages": ["pv"]},
        },
        indent=2,
    )
    refusal = jarvis_exits.build_refused(printed, site="Lab")
    assert refusal.data["packages"] == ["pv"] and refusal.data["retry"] is False
    assert "build placement is an install job" in refusal.message
    assert "had no room for JARVIS's limits" in refusal.message


@dataclass
class _Site:
    built: bool = False
    end: JobStatus = field(default_factory=lambda: JobStatus(state=JobState.COMPLETED, exit_code=0))
    polls: int = 0
    calls: list[str] = field(default_factory=list[str])
    cancelled: list[JobHandle] = field(default_factory=list[JobHandle])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        if not state.built:
            raise jarvis_report.install_needed(_report(missing=True), site="fx")
        return _report(missing=False)

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        state.calls.append("install")
        found = jarvis_exits.without_job("fixture", 0, json.dumps(STARTED), None, "detached", {})
        assert found is not None
        return found

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        assert handle.native_id == "pid:4242"
        state.polls += 1
        if state.polls == 1:
            return JobStatus(state=JobState.RUNNING)
        state.built = state.end.state == JobState.COMPLETED
        return state.end

    async def cancel(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        state.cancelled.append(handle)
        return JobStatus(state=JobState.CANCELLED, exit_code=143)

    for name, value in {
        "install_check": install_check,
        "install": install,
        "status": status,
        "cancel": cancel,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


async def _approved(service: RelayService) -> str:
    operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
    (approve,) = service.outstanding(service.operation(operation_id))
    assert approve.kind == "approve"
    assert "login host under its own limits" in ask_text(approve)
    service.answer(approve.ask_id, "accept", {"approve": True}, by="test")
    record = await service.advance(operation_id)
    assert record.state == OperationState.QUEUED, record
    assert "login host under its own limits" in str((record.result or {})["message"])
    return operation_id


def test_a_login_build_is_followed_until_it_ends_then_checked(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = await _approved(service)
        running = await service.refresh(operation_id)
        assert running.state == OperationState.RUNNING
        assert running.history[-1].note == "JARVIS is building on the login host"
        done = await service.refresh(operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert site.calls == ["check", "install", "check"]
        await service.close()

    asyncio.run(scenario())


def test_a_login_build_jarvis_refused_fails_final(relay_paths: RelayPaths, site: _Site) -> None:
    site.end = JobStatus(state=JobState.FAILED, exit_code=jarvis_submit.EXIT_BUILD_REFUSED)

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = await _approved(service)
        await service.refresh(operation_id)
        failed = await service.refresh(operation_id)
        assert failed.state == OperationState.FAILED, failed
        assert failed.error is not None and failed.error["reason"] == "policy_refused"
        assert site.calls == ["check", "install"]
        await service.close()

    asyncio.run(scenario())


def test_only_a_cancel_stops_a_login_build(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = await _approved(service)
        await service.refresh(operation_id)
        cancelled = await service.cancel(operation_id)
        assert cancelled.state == OperationState.CANCELLED, cancelled
        (handle,) = site.cancelled
        assert jarvis_exits.built_here(handle) is not None
        await service.close()

    asyncio.run(scenario())
