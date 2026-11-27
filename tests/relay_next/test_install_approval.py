"""JARVIS's questions come first; the install approval comes last, once.

JARVIS asks its install questions a few at a time (where checks run, which
Spack, where the build runs), and only once they are answered does it know
the spec it builds and the time it takes. Relay asks each round's questions on
their own, and the approval once JARVIS's check asks nothing more, so the one
approval carries JARVIS's spec and time. A later round asks it again only when
JARVIS names more to build than the person approved.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import ask_text
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

PACKAGE = JobSpec(package=PackageRef(name="demo.fio", version="1", sha256="c" * 64))


def _question(n: int) -> dict[str, Any]:
    return {
        "issue_id": f"sha256:{n}" + "0" * 15,
        "kind": "ambiguous_provider",
        "scope": "site",
        "answerable": True,
        "question": f"Question {n} for JARVIS?",
        "candidates": [{"constraint": f"q{n}-a"}, {"constraint": f"q{n}-b"}],
    }


def _report(issues: list[dict[str, Any]], specs: list[str]) -> dict[str, Any]:
    packages = {
        f"p{n}": {"status": "missing", "spec": spec, "missing": [spec]}
        for n, spec in enumerate(specs)
    }
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": False,
        "installers": {"spack": {"packages": packages}},
        "issues": issues,
    }


@dataclass
class _Site:
    """A stand-in JARVIS that asks one question per check, then needs ``specs`` built.

    Its check runs on the login host (no check job). ``install_asks`` is what the
    install command itself stops on, one refusal per install, as JARVIS's
    ``+submit`` does when an answer opens a new question.
    """

    questions: list[dict[str, Any]] = field(default_factory=lambda: [_question(1), _question(2)])
    specs: list[str] = field(default_factory=lambda: ["fio@3.37"])
    install_asks: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    answered: set[str] = field(default_factory=set[str])
    calls: list[str] = field(default_factory=list[str])
    forgets: bool = False
    """Like JARVIS for an answer it does not record per site: it holds only for that call."""
    rejects: set[str] = field(default_factory=set[str])
    """``issue=constraint`` answers the check turns down once, so it asks the issue again."""
    sent: list[dict[str, str]] = field(default_factory=list[dict[str, str]])

    def refusal(self) -> Any:
        open_ = [issue for issue in self.questions if issue["issue_id"] not in self.answered]
        walltime = None if open_ else "00:10:00"
        return jarvis_report.install_needed(
            _report(open_[:1], self.specs), site="fx", walltime=walltime
        )


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        raise state.refusal()

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        sent = {
            key: str(value["constraint"])
            for key, value in answers.items()
            if key.startswith("sha256:")
        }
        state.sent.append(sent)
        if state.forgets:
            state.answered = set()
        for key, constraint in sent.items():
            if f"{key}={constraint}" in state.rejects:
                state.rejects.discard(f"{key}={constraint}")
            else:
                state.answered.add(key)
        if jarvis_submit.is_recheck(answers):
            state.calls.append("recheck")
            printed = json.dumps(_report([], state.specs))
            here = jarvis_exits.checked_here("fixture", 0, printed, None)
            assert here is not None
            return here
        state.calls.append("install")
        if state.install_asks:
            state.questions.append(state.install_asks.pop(0))
            raise state.refusal()
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


def _kinds(service: RelayService, operation_id: str) -> list[str]:
    return [ask.kind for ask in service.outstanding(service.operation(operation_id))]


async def _answer_question(service: RelayService, operation_id: str, pick: int = 0) -> None:
    (choice,) = service.outstanding(service.operation(operation_id))
    assert choice.kind == "choose", choice
    option = choice.binding["options"][pick]
    service.answer(choice.ask_id, "accept", {"constraint": option}, by="test")
    await service.advance(operation_id)


async def _approve(service: RelayService, operation_id: str) -> str:
    (approve,) = service.outstanding(service.operation(operation_id))
    assert approve.kind == "approve", approve
    service.answer(approve.ask_id, "accept", {"approve": True}, by="test")
    await service.advance(operation_id)
    return ask_text(approve)


def _approvals(service: RelayService) -> int:
    return sum(1 for ask in service.asks.all() if ask.kind == "approve")


def test_the_questions_come_first_and_the_approval_last_with_the_spec_and_time(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
        assert _kinds(service, operation_id) == ["choose"]
        await _answer_question(service, operation_id)
        assert _kinds(service, operation_id) == ["choose"]
        await _answer_question(service, operation_id)
        text = await _approve(service, operation_id)
        assert "fio@3.37" in text and "up to 00:10:00" in text, text
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert _approvals(service) == 1
        assert site.calls == ["check", "recheck", "check", "recheck", "check", "install"]
        await service.close()

    asyncio.run(scenario())


def test_a_question_after_the_approval_is_asked_alone_and_the_install_goes_on(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.install_asks = [_question(3)]

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
        await _answer_question(service, operation_id)
        await _answer_question(service, operation_id)
        await _approve(service, operation_id)
        assert _kinds(service, operation_id) == ["choose"]
        await _answer_question(service, operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert _approvals(service) == 1
        assert site.calls[-2:] == ["install", "install"]
        await service.close()

    asyncio.run(scenario())


def test_the_approval_is_asked_again_only_when_jarvis_names_more_to_build(
    relay_paths: RelayPaths, site: _Site
) -> None:
    site.install_asks = [_question(3)]

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
        await _answer_question(service, operation_id)
        await _answer_question(service, operation_id)
        site.specs.append("libaio@0.3.113")
        await _approve(service, operation_id)
        assert _kinds(service, operation_id) == ["choose"]
        await _answer_question(service, operation_id)
        text = await _approve(service, operation_id)
        assert "libaio@0.3.113" in text, text
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert _approvals(service) == 2
        await service.close()

    asyncio.run(scenario())


def test_every_round_sends_all_the_answers_so_far_the_newest_for_an_issue(
    relay_paths: RelayPaths, site: _Site
) -> None:
    # RF's rerun 8: a later round sent only its own answer, so JARVIS asked the compiler again.
    site.questions.append(_question(3))
    site.forgets = True
    first = site.questions[0]["issue_id"]
    site.rejects = {f"{first}=q1-a"}

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
        await _answer_question(service, operation_id)
        await _answer_question(service, operation_id, pick=1)
        await _answer_question(service, operation_id)
        await _answer_question(service, operation_id)
        await _approve(service, operation_id)
        assert service.operation(operation_id).state == OperationState.QUEUED
        assert _approvals(service) == 1
        ids = [issue["issue_id"] for issue in site.questions]
        assert site.sent == [
            {ids[0]: "q1-a"},
            {ids[0]: "q1-b"},
            {ids[0]: "q1-b", ids[1]: "q2-a"},
            {ids[0]: "q1-b", ids[1]: "q2-a", ids[2]: "q3-a"},
            {ids[0]: "q1-b", ids[1]: "q2-a", ids[2]: "q3-a"},
        ]
        await service.close()

    asyncio.run(scenario())
