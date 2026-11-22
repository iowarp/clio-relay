"""Operations on one site that get the same JARVIS site-scope question share one ask.

Two installs started together both get JARVIS's "which MPI" question: the same
``issue_id`` with the same candidates, scoped to the site. Relay asks it once;
both operations hold the ask, and the one answer reaches both installs. A
question that offers other candidates, or one scoped to a package, is asked on
its own. Cancelling one holder leaves the ask to the other.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import jarvis_submit
from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op, install_share
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

XCOMPACT = JobSpec(package=PackageRef(name="demo.xcompact3d", version="1", sha256="a" * 64))
PVSERVER = JobSpec(package=PackageRef(name="demo.pvserver", version="1", sha256="b" * 64))
MPIS = [{"constraint": "^openmpi@5.0.5"}, {"constraint": "^mpich@4.2"}]


def _issue(scope: str = "site", candidates: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "issue_id": "sha256:" + ("1" * 16 if candidates is None else "2" * 16),
        "kind": "ambiguous_provider",
        "scope": scope,
        "answerable": True,
        "question": "Which MPI should JARVIS use?",
        "candidates": MPIS if candidates is None else candidates,
    }


def _report(satisfied: bool, issues: list[dict[str, Any]]) -> dict[str, Any]:
    package: dict[str, Any] = {"status": "satisfied" if satisfied else "missing"}
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": satisfied,
        "installers": {"spack": {"packages": {"pkg": package}}},
        "issues": [] if satisfied else issues,
    }


@dataclass
class _Site:
    """A stand-in JARVIS that asks each package its MPI question until it installs."""

    issues: dict[str, list[dict[str, Any]]] = field(default_factory=dict[str, list[dict[str, Any]]])
    installed: set[str] = field(default_factory=set[str])
    answers: dict[str, dict[str, Any]] = field(default_factory=dict[str, dict[str, Any]])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        await asyncio.sleep(0)
        name = spec.package.name if spec.package else ""
        if name in state.installed:
            return _report(True, [])
        issues = state.issues.get(name, [_issue()])
        raise jarvis_submit.install_needed(_report(False, issues), site="fx")

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        name = spec.package.name if spec.package else ""
        state.answers[name] = answers
        return JobHandle(adapter="fixture", native_id=f"install:{name}")

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        state.installed.add(str(handle.native_id).removeprefix("install:"))
        return JobStatus(state=JobState.COMPLETED, exit_code=0)

    for name, value in {
        "install_check": install_check,
        "install": install,
        "status": status,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


async def _both(service: RelayService) -> tuple[str, str]:
    first, second = await asyncio.gather(
        install_op.start(service, "fx", XCOMPACT), install_op.start(service, "fx", PVSERVER)
    )
    return first.operation_id, second.operation_id


def _choices(service: RelayService) -> list[Ask]:
    return [ask for ask in service.asks.all() if ask.kind == "choose"]


def _approve_own(service: RelayService, operation_id: str) -> None:
    for ask in service.outstanding(service.operation(operation_id)):
        if ask.kind == "approve":
            service.answer(ask.ask_id, "accept", {"approve": True}, by="test")


def test_the_same_site_question_is_asked_once_and_answers_both(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (choice,) = _choices(service)
        assert choice.binding["kind"] == install_share.CHOICE_KIND
        assert choice.binding["issue_id"] == _issue()["issue_id"]
        assert choice.binding["options"] == ["^openmpi@5.0.5", "^mpich@4.2"]
        for operation_id in (first, second):
            record = service.operation(operation_id)
            assert record.state == OperationState.AWAITING_INPUT
            assert choice.ask_id in record.asks
        assert sorted(install_share.holders(service, choice)) == sorted([first, second])
        service.answer(choice.ask_id, "accept", {"constraint": "^mpich@4.2"}, by="test")
        for operation_id in (first, second):
            _approve_own(service, operation_id)
            await service.advance(operation_id)
            assert service.operation(operation_id).state == OperationState.QUEUED
        for name in ("demo.xcompact3d", "demo.pvserver"):
            picked = site.answers[name][_issue()["issue_id"]]
            assert picked["constraint"] == "^mpich@4.2"
            assert picked["ask_id"] == choice.ask_id
        for operation_id in (first, second):
            done = await service.refresh(operation_id)
            assert done.state == OperationState.COMPLETED, done
        await service.close()

    asyncio.run(scenario())


def test_an_answered_question_covers_an_operation_that_gets_it_later(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        (choice,) = _choices(service)
        service.answer(choice.ask_id, "accept", {"constraint": "^openmpi@5.0.5"}, by="test")
        second = (await install_op.start(service, "fx", PVSERVER)).operation_id
        assert _choices(service) == [service.asks.get(choice.ask_id)]
        assert choice.ask_id in service.operation(second).asks
        _approve_own(service, second)
        await service.advance(second)
        assert site.answers["demo.pvserver"][_issue()["issue_id"]]["constraint"] == (
            "^openmpi@5.0.5"
        )
        assert service.operation(first).state == OperationState.AWAITING_INPUT
        await service.close()

    asyncio.run(scenario())


def test_other_candidates_or_a_package_question_are_asked_on_their_own(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.issues["demo.pvserver"] = [_issue(candidates=[*MPIS, {"constraint": "^intel-mpi"}])]

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        await _both(service)
        assert len(_choices(service)) == 2
        await service.close()

    asyncio.run(scenario())
    site.issues.clear()
    site.issues["demo.xcompact3d"] = [_issue(scope="package")]
    site.issues["demo.pvserver"] = [_issue(scope="package")]

    async def packages() -> None:
        service = RelayService(relay_paths)
        before = len(_choices(service))
        await _both(service)
        fresh = _choices(service)[before:]
        assert len(fresh) == 2
        assert all("scope" not in ask.binding for ask in fresh)
        await service.close()

    asyncio.run(packages())


def test_cancelling_one_holder_leaves_the_question_to_the_other(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (choice,) = _choices(service)
        assert (await service.cancel(first)).state == OperationState.CANCELLED
        assert service.asks.get(choice.ask_id).state == "pending"  # pyright: ignore[reportOptionalMemberAccess]
        assert install_share.holders(service, choice) == [second]
        await service.close()

    asyncio.run(scenario())


def test_a_declined_question_cancels_every_holder(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (choice,) = _choices(service)
        service.answer(choice.ask_id, "decline", by="test")
        for operation_id in (first, second):
            assert (await service.advance(operation_id)).state == OperationState.CANCELLED
        await service.close()

    asyncio.run(scenario())
