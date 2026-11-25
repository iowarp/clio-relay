"""Every Spack check runs in a job; the login host only reads JARVIS's record of it.

JARVIS's light ``ppl install +check`` on a scheduled pipeline's submitting host
runs no Spack: it answers from the ``install-check.json`` a job's check wrote.
With no matching record it says ``install_unchecked``, and relay asks nobody:
it submits a check job (``recheck``, ``ppl install +check +submit``) and reads
the record again once that job ends. JARVIS cannot tell whether a package is
installed until it knows which MPI to use: the record then holds the question,
and relay asks only it, then runs the check job again with the answer. When the
kept store has the software the operation goes ahead with no install ask and no
build; when it does not, relay asks to install what the check found missing.
"""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import ask_text
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

XCOMPACT = JobSpec(package=PackageRef(name="demo.xcompact3d", version="1", sha256="a" * 64))
ISSUE: dict[str, Any] = {
    "issue_id": "sha256:" + "1" * 16,
    "kind": "ambiguous_provider",
    "scope": "site",
    "answerable": True,
    "question": "Which MPI should JARVIS use?",
    "candidates": [{"constraint": "^openmpi@5.0.5"}, {"constraint": "^mpich@4.2"}],
}
UNCHECKED: dict[str, Any] = {
    "issue_id": "sha256:" + "2" * 16,
    "kind": jarvis_report.UNCHECKED,
    "scope": "site",
    "subject": "install_check",
    "answerable": False,
    "packages": ["pkg"],
    "reason": "no_record",
    "hint": "check inside an allocation: `jarvis ppl install yaml FILE +check +submit`",
}


def _report(status: str, issues: list[dict[str, Any]]) -> dict[str, Any]:
    package: dict[str, Any] = {
        "status": status,
        "missing": ["fftw"] if status == "missing" else None,
    }
    installers = {} if status == "unchecked" else {"spack": {"packages": {"pkg": package}}}
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": status in ("satisfied", "installed"),
        "installers": installers,
        "issues": issues,
    }


@dataclass
class _Site:
    """A stand-in JARVIS whose login-host check reads only what a check job recorded.

    ``record`` is the outcome of the last check job: ``None`` (no record yet),
    ``needs_decision`` (the MPI question), ``missing`` or ``satisfied``.
    """

    has_software: bool = True
    records: bool = True
    record: str | None = None
    answers: dict[str, Any] = field(default_factory=dict[str, Any])
    calls: list[str] = field(default_factory=list[str])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        if state.record is None:
            raise jarvis_report.install_needed(_report("unchecked", [UNCHECKED]), site="fx")
        if state.record == "needs_decision":
            raise jarvis_report.install_needed(_report("needs_decision", [ISSUE]), site="fx")
        if state.record == "missing":
            raise jarvis_report.install_needed(_report("missing", []), site="fx")
        return _report("satisfied", [])

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        if jarvis_submit.is_recheck(answers):
            state.calls.append("recheck")
            state.answers = answers
            return JobHandle(adapter="fixture", native_id="check", data={"install": True})
        state.calls.append("install")
        return JobHandle(adapter="fixture", native_id="install", data={"install": True})

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        if handle.native_id != "check":
            return JobStatus(state=JobState.RUNNING)
        # The check job ends and records what it found, with the answers it was given.
        if state.records:
            answered = ISSUE["issue_id"] in state.answers
            assert not answered or state.answers[ISSUE["issue_id"]]["constraint"] == "^mpich@4.2"
            software = "satisfied" if state.has_software else "missing"
            state.record = software if answered else "needs_decision"
        return JobStatus(state=JobState.COMPLETED, exit_code=0)

    for name, value in {
        "install_check": install_check,
        "install": install,
        "status": status,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


def _kinds(service: RelayService, operation_id: str) -> list[str]:
    return [ask.kind for ask in service.outstanding(service.operation(operation_id))]


async def _answer_mpi(service: RelayService, operation_id: str) -> None:
    (choice,) = service.outstanding(service.operation(operation_id))
    service.answer(choice.ask_id, "accept", {"constraint": "^mpich@4.2"}, by="test")
    await service.advance(operation_id)


async def _checked_in_a_job(service: RelayService, operation_id: str) -> None:
    """The site had no record: relay asked nobody and submitted the check job."""
    record = service.operation(operation_id)
    assert record.state == OperationState.QUEUED, record
    assert _kinds(service, operation_id) == []
    assert "checks the package's software in a job" in str((record.result or {})["message"])
    await service.refresh(operation_id)


def test_no_record_runs_a_check_job_then_asks_only_the_question(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        await _checked_in_a_job(service, operation_id)
        record = service.operation(operation_id)
        assert record.state == OperationState.AWAITING_INPUT
        assert _kinds(service, operation_id) == ["choose"]
        assert "needs a choice" in str(record.history[-1].note)
        await _answer_mpi(service, operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        done = await service.refresh(operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert done.result is not None
        assert done.result["installed"]["reuse"] == ["demo.xcompact3d"]
        assert site.calls == ["check", "recheck", "check", "recheck", "check"]
        await service.close()

    asyncio.run(scenario())


def test_a_check_job_that_finds_software_missing_asks_to_install_it(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.has_software = False

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        await _checked_in_a_job(service, operation_id)
        await _answer_mpi(service, operation_id)
        await service.refresh(operation_id)
        assert service.operation(operation_id).state == OperationState.AWAITING_INPUT
        (approve,) = service.outstanding(service.operation(operation_id))
        assert approve.kind == "approve"
        assert "does not have yet" in ask_text(approve)
        service.answer(approve.ask_id, "accept", {"approve": True}, by="test")
        await service.advance(operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert site.calls == ["check", "recheck", "check", "recheck", "check", "install"]
        await service.close()

    asyncio.run(scenario())


def test_a_record_that_still_does_not_match_after_the_job_fails_typed(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.records = False

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        await _checked_in_a_job(service, operation_id)
        failed = service.operation(operation_id)
        assert failed.state == OperationState.FAILED, failed
        assert failed.error is not None
        assert "still finds no matching record" in failed.error["message"]
        assert "no_record" in failed.error["message"]
        assert site.calls == ["check", "recheck", "check"]
        await service.close()

    asyncio.run(scenario())


def test_install_unchecked_is_neither_blocking_nor_a_question() -> None:
    report = _report("unchecked", [UNCHECKED])
    assert jarvis_report.blocking_issues(report) == []
    assert jarvis_report.choice_issues(report) == []
    assert jarvis_report.unchecked(report) == [UNCHECKED]
    assert not jarvis_report.software_missing(report)
    refusal = jarvis_report.install_needed(report, site="fx")
    assert "no check of the package's software" in refusal.message


def test_the_summary_keeps_undecided_packages_out_of_the_build() -> None:
    summary = jarvis_report.install_summary(_report("needs_decision", [ISSUE]))
    assert summary["build"] == [] and summary["undecided"] == ["pkg"]
    assert not jarvis_report.software_missing(_report("needs_decision", [ISSUE]))
    assert jarvis_report.software_missing(_report("missing", []))
    assert jarvis_report.install_summary(_report("installed", []))["reuse"] == ["pkg"]


def test_a_recheck_command_is_a_check_job_with_the_answers() -> None:
    answers = {ISSUE["issue_id"]: {"constraint": "^mpich@4.2"}, jarvis_submit.RECHECK: True}
    command = jarvis_submit.install_command(
        XCOMPACT, operation_id="op_x", jarvis="", answers=answers, pipeline="name: install\n"
    )
    argv = shlex.split(command)
    at = argv.index("recheck")
    assert argv[at + 2] == "op_x-install"
    assert jarvis_submit.RECHECK not in argv[at + 4]
    assert argv[at + 4] == f'{{"{ISSUE["issue_id"]}":{{"constraint":"^mpich@4.2"}}}}'
    assert argv[at + 5] == "name: install\n"
    install = shlex.split(
        jarvis_submit.install_command(XCOMPACT, operation_id="op_x", jarvis="", answers={})
    )
    assert "install" in install and "recheck" not in install


def test_a_check_job_gets_a_short_walltime() -> None:
    document: dict[str, object] = {"name": "p", "scheduler": {"type": "slurm"}}
    check = jarvis_submit.install_pipeline(
        document, XCOMPACT, 4 * 3600, {jarvis_submit.RECHECK: True}
    )
    build = jarvis_submit.install_pipeline(document, XCOMPACT, 4 * 3600, {})
    assert "time: 01:00:00" in check
    assert "time: 04:00:00" in build
