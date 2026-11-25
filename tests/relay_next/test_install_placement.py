"""Relay relays JARVIS's install placement question; it has no placement policy of its own.

JARVIS asks where a site runs its Spack checks (``install_placement``): on the
login host under its limits, in a compute job, or asking each time, with its
evidence and recommendation. Relay shows that question once per site, passes
the answer back on the command that records it (``ppl install +check
--answers`` on the submitting host) and follows what JARVIS did: when the site
chose the login host the check ran there and no check job is sent.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from clio_relay import jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import (
    CHOICE_META,
    Ask,
    ChoiceOption,
    choice_ask,
    schema_options,
)
from clio_relay.jobs.handles import JobHandle, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op
from clio_relay.surface.elicit import input_required
from clio_relay.surface.install_share import CHOICE_FIELD, choice_answers, choice_binding, offer_map
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

PACKAGE = JobSpec(package=PackageRef(name="demo.gray_scott", version="1", sha256="b" * 64))
PLACEMENT: dict[str, Any] = {
    "issue_id": "sha256:" + "5" * 16,
    "kind": "install_placement",
    "scope": "site",
    "answerable": True,
    "subject": "install_placement.check.short",
    "phase": "check",
    "once": False,
    "question": (
        "Where should JARVIS run the Spack check of short packages at this site: "
        "pkg (~60 s)? Recommended: login_limited."
    ),
    "candidates": [
        {"id": "a", "constraint": "login_limited", "does": "check here", "recommended": True},
        {"id": "b", "constraint": "compute_job", "does": "check in a job", "recommended": False},
        {"id": "c", "constraint": "ask_each_time", "does": "ask per run", "recommended": False},
    ],
    "evidence": [f"evidence {n}" for n in range(8)],
    "package_overrides": [
        {"package": "paraview", "phase": "check", "issue_id": "sha256:pv", "subject": "x"}
    ],
}
OCCASION: dict[str, Any] = {
    **PLACEMENT,
    "issue_id": "sha256:" + "6" * 16,
    "subject": "install_placement.check.short.occasion",
    "once": True,
    "package_overrides": [],
}


def _report(issues: list[dict[str, Any]], *, satisfied: bool = False) -> dict[str, Any]:
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": satisfied,
        "installers": {"spack": {"packages": {"pkg": {"status": "satisfied"}}}},
        "issues": issues,
    }


@dataclass
class _Site:
    """A stand-in JARVIS that asks where checks run until an answer is recorded."""

    choice: str | None = None
    calls: list[str] = field(default_factory=list[str])
    answers: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        if state.choice is None:
            raise jarvis_report.install_needed(_report([PLACEMENT]), site="fx")
        return _report([], satisfied=True)

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        assert jarvis_submit.is_recheck(answers)
        state.calls.append("recheck")
        state.answers.append(answers)
        answer = answers.get(PLACEMENT["issue_id"])
        state.choice = answer["constraint"] if answer else None
        # JARVIS checked on the login host, within its limits: no job.
        printed = json.dumps(_report([], satisfied=True))
        here = jarvis_exits.checked_here("fixture", 0, printed, None)
        assert here is not None
        return here

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        raise AssertionError("no job runs when the site checks on the login host")

    for name, value in {
        "install_check": install_check,
        "install": install,
        "status": status,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


def test_a_login_check_answer_needs_no_check_job(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        operation_id = (await install_op.start(service, "fx", PACKAGE)).operation_id
        (choice,) = service.outstanding(service.operation(operation_id))
        assert choice.kind == "choose"
        assert "Login host, now (recommended): Runs on the login host" in choice.message
        assert "login_limited" not in choice.message
        assert "compute_job for paraview only" in str(choice.requested_schema)
        field = (choice.requested_schema or {})["properties"][CHOICE_FIELD]
        assert field["title"] == "Where JARVIS runs Spack checks at this site"
        assert field["default"] == "login_limited"
        assert {"const": "compute_job", "title": "Compute node job"}.items() <= field["oneOf"][
            1
        ].items()
        typed = (choice.meta or {})[CHOICE_META][choice.ask_id][CHOICE_FIELD]
        assert typed["recommended"] == "login_limited"
        assert [item["label"] for item in typed["options"]][:3] == [
            "Login host, now",
            "Compute node job",
            "Ask each time",
        ]
        service.answer(choice.ask_id, "accept", {CHOICE_FIELD: "login_limited"}, by="test")
        done = await service.advance(operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert site.calls == ["check", "recheck", "check"]
        (answers,) = site.answers
        assert answers[PLACEMENT["issue_id"]]["constraint"] == "login_limited"
        await service.close()

    asyncio.run(scenario())


def test_the_question_shows_jarvis_evidence_and_what_each_choice_does() -> None:
    message = jarvis_report.choice_message(PLACEMENT)
    assert message.startswith(PLACEMENT["question"].replace("login_limited", "Login host, now"))
    assert "Compute node job: Runs the check in a short 1-node batch job" in message
    assert "JARVIS records the answer for the site's later installs" in message
    assert "evidence 5" in message and "evidence 6" not in message
    assert "for <package> only" in message and "(paraview)" in message
    occasion = jarvis_report.choice_message(OCCASION)
    assert "for this run only" in occasion and "later installs" not in occasion


def test_each_placement_option_says_where_it_runs_what_it_costs_and_its_limits() -> None:
    issue = {
        **PLACEMENT,
        "recommended": "compute_job",
        "recommendation_reason": "a short compute job keeps the login host light",
        "login_host": {"host": "ares-login"},
        "limits": {"nice": 10, "jobs": 2, "memory_mb": 4096, "defer": False},
        "queue": {"measured": True, "wait": "2 min"},
    }
    login, compute, each, alone_login, alone_compute, _ = jarvis_report.choice_details(issue)
    assert login.label == "Login host, now" and not login.recommended
    assert "ares-login" in (login.description or "")
    assert "nice 10, 2 job(s), up to 4096 MB" in (login.description or "")
    assert "no queue wait" in (login.description or "")
    assert compute.recommended and "about 2 min in the queue" in (compute.description or "")
    assert "allocation" in (compute.description or "")
    assert each.label == "Ask each time"
    assert alone_login.value == "login_limited for paraview only"
    assert alone_login.label == "Login host, now for paraview only"
    assert not alone_compute.recommended
    assert jarvis_report.choice_title(issue) == "Where JARVIS runs Spack checks at this site"
    assert jarvis_report.recommended_reason(issue) == issue["recommendation_reason"]
    assert [item.value for item in jarvis_report.choice_details(issue)] == (
        jarvis_report.choice_offers(issue)
    )
    assert jarvis_report.choice_title(OCCASION) == "Where JARVIS runs this Spack check"


def test_a_spack_candidate_says_where_it_comes_from_in_words() -> None:
    issue = {
        "kind": "spack_missing",
        "question": "Which Spack should jarvis use on this site?",
        "scope": "site",
        "candidates": [
            {"constraint": "/opt/site/spack", "source": "site_modules", "version": "0.23.1"},
            {"constraint": "/home/me/spack", "source": "well_known", "version": "0.23.1"},
            {"constraint": "/x/spack", "source": "something_new"},
        ],
    }
    site, mine, new = jarvis_report.choice_details(issue)
    assert site.label == "/opt/site/spack"
    assert site.description == (
        "The site's published Spack, whose modules the login host lists; version 0.23.1."
    )
    assert mine.description == (
        "Found at a usual Spack location; the site does not publish it; version 0.23.1."
    )
    assert new.description == "Something_new."
    message = jarvis_report.choice_message(issue)
    assert "site_modules" not in message and "Well_known" not in message
    assert jarvis_report.choice_title(issue) == "Spack to use"


def test_a_choice_ask_is_a_titled_select_with_typed_options() -> None:
    options = [
        ChoiceOption(value="a", label="Option A", description="Does A.", recommended=True),
        ChoiceOption(value="b", label="Option B"),
    ]
    ask = choice_ask(
        ask_id="ask_1",
        site="fx",
        operation_id="op",
        message="Pick",
        field_name="pick",
        options=options,
        title="What to pick",
        recommended_reason="A is cheaper",
    )
    field = (ask.requested_schema or {})["properties"]["pick"]
    assert field == {
        "type": "string",
        "title": "What to pick",
        "oneOf": [
            {"const": "a", "title": "Option A", "description": "Does A."},
            {"const": "b", "title": "Option B"},
        ],
        "default": "a",
    }
    assert schema_options(ask.requested_schema) == options
    typed = (ask.meta or {})[CHOICE_META]["ask_1"]["pick"]
    assert typed["field"] == "pick" and typed["recommended_reason"] == "A is cheaper"
    other = choice_ask(
        ask_id="ask_2", site="fx", operation_id="op", message="m", field_name="x", options=["y"]
    )
    merged = input_required([ask, other], "state").meta or {}
    assert set(merged[CHOICE_META]) == {"ask_1", "ask_2"}
    # A form that names only values still reads, each its own label.
    assert schema_options({"properties": {"x": {"type": "string", "enum": ["y"]}}}) == [
        ChoiceOption(value="y", label="y")
    ]


def test_a_question_for_this_run_only_is_not_shared() -> None:
    assert choice_binding("fx", PLACEMENT) is not None
    assert choice_binding("fx", OCCASION) is None
    assert jarvis_report.choice_offers(OCCASION) == jarvis_report.choice_options(OCCASION)


class _Asks:
    def __init__(self, asks: list[Ask]) -> None:
        self._asks = {ask.ask_id: ask for ask in asks}

    def get(self, ask_id: str) -> Ask | None:
        return self._asks.get(ask_id)


class _Service:
    def __init__(self, asks: list[Ask]) -> None:
        self.asks = _Asks(asks)


def _answered(ask_id: str, value: str) -> Ask:
    return Ask(
        ask_id=ask_id,
        kind="choose",
        site="fx",
        operation_id="op",
        message="m",
        state="accepted",
        answer={"action": "accept", "content": {CHOICE_FIELD: value}},
    )


def test_a_per_package_option_answers_that_package_only() -> None:
    offers = offer_map(PLACEMENT)
    assert offers["compute_job for paraview only"] == ("sha256:pv", "compute_job")
    state = {"choices": {"a1": PLACEMENT["issue_id"]}, "offers": {"a1": offers}}
    service: Any = _Service([_answered("a1", "compute_job for paraview only")])
    answers = choice_answers(service, state)
    assert answers == {
        "sha256:pv": {"constraint": "compute_job", "answered_by": "person", "ask_id": "a1"}
    }
    # A shared ask's option for a package this operation does not have answers nothing.
    service = _Service([_answered("a1", "compute_job for other only")])
    assert choice_answers(service, state) == {}
