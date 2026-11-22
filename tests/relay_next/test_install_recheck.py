"""Relay asks to install software only when JARVIS's check names something to build.

JARVIS cannot tell whether a package is installed until it knows which MPI to
use: its ``+check`` reports the package ``needs_decision`` with the question,
and nothing missing. Relay then asks only the question. With the answer, JARVIS
checks again (``recheck``) and builds nothing: when the kept store has the
software the operation goes ahead with no install ask and no install job, and
when it does not relay asks to install what that check found missing.
"""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal
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


def _report(status: str, issues: list[dict[str, Any]]) -> dict[str, Any]:
    package: dict[str, Any] = {
        "status": status,
        "missing": ["fftw"] if status == "missing" else None,
    }
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": status in ("satisfied", "installed"),
        "installers": {"spack": {"packages": {"pkg": package}}},
        "issues": issues,
    }


@dataclass
class _Site:
    """A stand-in JARVIS that needs the MPI answer before it can tell what is installed."""

    has_software: bool = True
    calls: list[str] = field(default_factory=list[str])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        raise jarvis_report.install_needed(_report("needs_decision", [ISSUE]), site="fx")

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        if answers.get(jarvis_submit.RECHECK):
            state.calls.append("recheck")
            assert answers[ISSUE["issue_id"]]["constraint"] == "^mpich@4.2"
            if state.has_software:
                raise jarvis_report.software_installed(_report("satisfied", []), site="fx")
            raise jarvis_report.install_needed(_report("missing", []), site="fx")
        state.calls.append("install")
        return JobHandle(adapter="fixture", native_id="install")

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        return JobStatus(state=JobState.RUNNING)

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


def test_a_question_alone_is_asked_without_an_install_ask(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        record = service.operation(operation_id)
        assert record.state == OperationState.AWAITING_INPUT
        assert _kinds(service, operation_id) == ["choose"]
        assert "needs a choice" in str(record.history[-1].note)
        await _answer_mpi(service, operation_id)
        done = service.operation(operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert done.result is not None
        assert done.result["installed"]["reuse"] == ["demo.xcompact3d"]
        assert site.calls == ["check", "recheck"]
        await service.close()

    asyncio.run(scenario())


def test_a_recheck_that_finds_software_missing_asks_to_install_it(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.has_software = False

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        await _answer_mpi(service, operation_id)
        assert service.operation(operation_id).state == OperationState.AWAITING_INPUT
        (approve,) = service.outstanding(service.operation(operation_id))
        assert approve.kind == "approve"
        assert "does not have yet" in approve.message
        service.answer(approve.ask_id, "accept", {"approve": True}, by="test")
        await service.advance(operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert site.calls == ["check", "recheck", "install"]
        await service.close()

    asyncio.run(scenario())


def test_the_summary_keeps_undecided_packages_out_of_the_build() -> None:
    summary = jarvis_report.install_summary(_report("needs_decision", [ISSUE]))
    assert summary["build"] == [] and summary["undecided"] == ["pkg"]
    assert not jarvis_report.software_missing(_report("needs_decision", [ISSUE]))
    assert jarvis_report.software_missing(_report("missing", []))
    assert jarvis_report.install_summary(_report("installed", []))["reuse"] == ["pkg"]


def test_a_recheck_command_checks_the_run_pipeline_with_the_answers() -> None:
    answers = {ISSUE["issue_id"]: {"constraint": "^mpich@4.2"}, jarvis_submit.RECHECK: True}
    command = jarvis_submit.install_command(
        XCOMPACT, operation_id="op_x", jarvis="", answers=answers, pipeline="name: install\n"
    )
    argv = shlex.split(command)
    at = argv.index("recheck")
    assert argv[at + 2] == "op_x-install"
    assert jarvis_submit.RECHECK not in argv[at + 4]
    assert argv[at + 4 :] == [f'{{"{ISSUE["issue_id"]}":{{"constraint":"^mpich@4.2"}}}}']
    install = shlex.split(
        jarvis_submit.install_command(XCOMPACT, operation_id="op_x", jarvis="", answers={})
    )
    assert "install" in install and "recheck" not in install


def test_exit_7_is_the_software_being_there() -> None:
    report = '{"schema_version":"jarvis.install-report.v1","satisfied":true}'
    with pytest.raises(Refusal) as raised:
        jarvis_exits.refuse(
            jarvis_submit.EXIT_INSTALLED,
            report,
            "",
            spec=XCOMPACT,
            site="fx",
            install_walltime_s=3600,
        )
    assert jarvis_report.is_installed(raised.value)
