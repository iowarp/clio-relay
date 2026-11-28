"""The job card's start and JARVIS's advisories, from JARVIS's record of the job."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from clio_relay import jarvis_plan
from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.records import OperationRecord, OperationState, StateChange
from clio_relay.surface import job_card

from .test_job_plan import _document  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService


def _advisory(issue_id: str, message: str = "launching over TCP") -> dict[str, Any]:
    return {
        "schema_version": "jarvis.issue.v1",
        "kind": "site_fact_uncertain",
        "issue_id": issue_id,
        "subject": "net",
        "field": "resource_graph.site_facts.net.launch",
        "severity": "advisory",
        "blocking": False,
        "reason_codes": ["ucx_device_unusable"],
        "message": message,
        "remedy": f"jarvis rg facts answer {issue_id} --value accept --reason <why>",
        "details": {
            "fact": "net.launch",
            "pick": {"data": "10.0.0.0/24", "consequence_if_wrong": ["slower MPI"]},
            "probes": [{"command": "ucx_info -d", "why": "x"}],
        },
    }


def test_advisories_are_jarvis_warnings_without_their_evidence() -> None:
    plan = _document(warnings=[_advisory("sha256:aa"), "a plain warning", _advisory("sha256:aa")])
    first, plain = jarvis_plan.advisories(plan)
    assert first == {
        "issue_id": "sha256:aa",
        "kind": "site_fact_uncertain",
        "subject": "net",
        "fact": "net.launch",
        "reason_codes": ["ucx_device_unusable"],
        "message": "launching over TCP",
        "pick": {"data": "10.0.0.0/24"},
        "consequence_if_wrong": ["slower MPI"],
        "remedy": "jarvis rg facts answer sha256:aa --value accept --reason <why>",
        "agent_guidance": None,
        "probes": [{"command": "ucx_info -d", "why": "x"}],
        "settled_at_launch": False,
    }
    assert plain["message"] == "a plain warning" and plain["issue_id"] is None
    assert jarvis_plan.summary(plan).splitlines()[1:] == [
        "JARVIS notes: launching over TCP",
        "JARVIS notes: a plain warning",
    ]


def test_the_person_sees_no_instruction_meant_for_the_agent() -> None:
    told = _advisory(
        "sha256:aa",
        "The MPI launch network rests on uncertain cluster facts: data 10.0.0.0/24 has "
        "an RDMA device that is not ACTIVE on every node, so UCX_NET_DEVICES is not set. "
        "Investigate with details.probes, propose a value, and have the person confirm it.",
    )
    (advisory,) = jarvis_plan.advisories(_document(warnings=[told]))
    assert advisory["message"] == (
        "The MPI launch network rests on uncertain cluster facts: data 10.0.0.0/24 has "
        "an RDMA device that is not ACTIVE on every node, so UCX_NET_DEVICES is not set."
    )
    assert advisory["agent_guidance"] == (
        "Investigate with details.probes, propose a value, and have the person confirm it."
    )
    assert "details.probes" not in jarvis_plan.summary(_document(warnings=[told]))
    split = {**told, "person_message": "TCP for now", "agent_guidance": "check ucx_info"}
    (given,) = jarvis_plan.advisories(_document(warnings=[split]))
    assert (given["message"], given["agent_guidance"]) == ("TCP for now", "check ucx_info")


def test_an_advisory_jarvis_settles_at_launch_is_for_the_agent_only() -> None:
    larger = _advisory(
        "sha256:aa",
        "The MPI launch network rests on uncertain cluster facts: the job asks 3 nodes but "
        "the graph was collected on 2. Investigate with details.probes, propose a value, "
        "and have the person confirm it.",
    )
    larger["reason_codes"] = ["hosts_not_in_graph"]
    larger["details"]["reasons"] = [
        {"code": "hosts_not_in_graph", "plan": "collect_at_launch", "severity": "advisory"}
    ]
    plan = _document(warnings=[larger, _advisory("sha256:bb")])
    (shown,) = jarvis_plan.advisories(plan)
    assert shown["issue_id"] == "sha256:bb"
    (settled,) = jarvis_plan.advisories(plan, settled=True)
    assert settled["issue_id"] == "sha256:aa" and settled["settled_at_launch"] is True
    assert settled["message"] == (
        "The MPI launch network rests on uncertain cluster facts: the job asks 3 nodes but "
        "the graph was collected on 2."
    )
    assert "collected on 2" not in jarvis_plan.summary(plan)
    assert jarvis_plan.changes(_document(warnings=[]), _document(warnings=[larger])) == []
    larger["details"]["reasons"].append({"code": "ucx_device_unusable"})
    (mixed,) = jarvis_plan.advisories(_document(warnings=[larger]))
    assert mixed["settled_at_launch"] is False
    assert jarvis_plan.advisories(_document(warnings=[larger]), settled=True) == []


def test_once_submitted_the_record_says_which_advisories_stand() -> None:
    facts = {
        "schema_version": "jarvis.site-facts.v1",
        "issues": [_advisory("sha256:aa"), _advisory("sha256:bb"), _advisory("sha256:cc")],
        "advisories": [
            _advisory("sha256:aa"),
            _advisory("sha256:bb"),
            _advisory("sha256:cc", "launch: TCP on n3"),
        ],
        "confirmed": ["sha256:aa"],
        "superseded_by_launch": ["sha256:bb"],
    }
    plan = _document(warnings=[_advisory("sha256:aa")])
    (left,) = jarvis_plan.advisories(plan, facts)
    assert (left["issue_id"], left["message"]) == ("sha256:cc", "launch: TCP on n3")
    assert jarvis_plan.advisories(None) == []


def test_jarvis_scheduler_start_is_this_jobs_only() -> None:
    record = {
        "state": "running",
        "metadata": {
            "scheduler_job": {
                "provider": "slurm",
                "job_id": "28303",
                "started_at": "2026-11-22T23:27:06Z",
                "source": "SLURM_JOB_START_TIME",
            }
        },
    }
    started = record_status(record, job_id="28303").started_at
    assert started == datetime(2026, 11, 22, 23, 27, 6, tzinfo=UTC)
    assert record_status(record, job_id="28302").started_at is None
    assert record_status(record).started_at is None
    assert record_status({"state": "running"}, job_id="28303").started_at is None


def _at(minute: int) -> datetime:
    return datetime(2026, 11, 22, 23, minute, tzinfo=UTC)


def _record(*states: tuple[OperationState, int], **result: Any) -> OperationRecord:
    record = OperationRecord(
        operation_id="op_1",
        kind="run",
        site="fx",
        adapter="fixture",
        spec={"executable": "echo"},
        spec_digest="d",
        state=states[-1][0],
        handle=JobHandle(adapter="fixture", native_id="77"),
        result=result,
    )
    record.history = [StateChange(state=state, at=_at(minute)) for state, minute in states]
    return record


def test_the_card_starts_with_the_job_not_with_an_install_before_it() -> None:
    started = job_card._started  # pyright: ignore[reportPrivateUsage]
    run = OperationState.RUNNING
    after_install = (
        (OperationState.QUEUED, 1),
        (run, 2),
        (OperationState.AWAITING_INPUT, 20),
        (OperationState.SUBMITTING, 21),
        (OperationState.QUEUED, 22),
        (run, 25),
    )
    assert started(_record(*after_install)) == _at(25)
    inside = [
        {"component": "a", "state": "running", "hosts": ["n1"], "at": "2026-11-22T23:24:30Z"},
        {"component": "b", "state": "waiting", "hosts": ["n2"], "at": "2026-11-22T23:24:00Z"},
        {"component": "c", "state": "waiting", "hosts": None, "at": "2026-11-22T23:21:00Z"},
    ]
    assert started(_record(*after_install, components=inside)) == _at(24)
    scheduler = _at(23).isoformat()
    assert started(_record(*after_install, components=inside, started_at=scheduler)) == _at(23)
    assert started(_record(*after_install, started_at="not a time")) == _at(25)
    assert started(_record((OperationState.QUEUED, 1))) is None
    ended = (*after_install, (OperationState.COMPLETED, 40))
    assert started(_record(*ended)) == _at(25)


class _Titles:
    """A service stand-in that knows the packages' contract titles."""

    def __init__(self, titles: dict[str, str]) -> None:
        self.known = titles
        self.packages = self

    def titles(self, site: str) -> dict[str, str]:
        return self.known


def _install(spec: dict[str, Any], **result: Any) -> OperationRecord:
    record = _record((OperationState.RUNNING, 1), **result)
    record.kind = "install"
    record.spec = spec
    return record


def test_an_install_is_labelled_with_what_it_installs() -> None:
    label = job_card._label  # pyright: ignore[reportPrivateUsage]
    service = cast("RelayService", _Titles({"clio_jarvis.lammps": "LAMMPS"}))
    spec = {"package": {"name": "clio_jarvis.lammps"}}
    assert label(service, _install(spec)) == "Install LAMMPS"
    assert label(cast("RelayService", _Titles({})), _install(spec)) == "Install lammps"
    built: dict[str, Any] = {
        "summary": {
            "build": [
                {
                    "package": "lammps",
                    "method": "spack",
                    "status": "missing",
                    "spec": "lammps@20240829.1 +mpi ^openmpi",
                    "missing": [],
                },
                {"package": "adios2", "method": "spack", "status": "missing", "spec": None},
                {"package": "tool", "method": "pip", "status": "missing", "spec": "tool==2.1 x"},
            ],
            "reuse": ["openmpi"],
            "undecided": [],
        }
    }
    found = label(service, _install(spec, install=built))
    assert found == "Install lammps@20240829.1, adios2, tool==2.1 x"
    reused = {"summary": {"build": [], "reuse": ["lammps"], "undecided": []}}
    assert label(service, _install(spec, install=reused)) == "Install LAMMPS"
    assert label(service, _record((OperationState.RUNNING, 1))) is None
    assert label(service, _install({"executable": "echo"})) is None
