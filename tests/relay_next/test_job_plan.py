"""JARVIS's own plan fills a package job's card, binds the yes, and its states fill the card."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

from clio_relay import jarvis_plan
from clio_relay.adapters.hooks import Ask, ask_text
from clio_relay.adapters.ssh.scripts import jarvis_status_script
from clio_relay.guard.reviews import Review
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import confirm, job_card, plan_card
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site
from .test_job_card import FLAG, Person, _client  # pyright: ignore[reportPrivateUsage]

SHA = "a" * 64
SPEC = JobSpec(package=PackageRef(name="demo.hello", version="1", sha256=SHA))


def _document(sha: str = "5eed", **extra: Any) -> dict[str, Any]:
    return {
        "schema_version": jarvis_plan.PLAN_SCHEMA,
        "plan_sha256": sha,
        "action": "run",
        "scheduler": "slurm",
        "partition": "compute",
        "account": None,
        "qos": None,
        "time": "01:00:00",
        "time_limit_s": 3600,
        "nodes": 3,
        "components": [
            {
                "id": "solver",
                "package": "demo.hello",
                "label": "Hello solver",
                "nodes": 2,
                "ranks": 8,
                "host_slice": "0:2",
                "node_range": {"first": 0, "end": 2},
                "role": "batch",
            },
            {
                "id": "viewer",
                "package": "demo.view",
                "label": "Viewer",
                "nodes": 1,
                "ranks": 1,
                "host_slice": "2:3",
                "node_range": {"first": 2, "end": 3},
                "role": "service",
            },
        ],
        "couplings": [
            {
                "from": "solver",
                "to": "viewer",
                "kind": "stream",
                "label": "BP",
                "source": "pipeline",
            }
        ],
        "script": {"path": "/x/job.sh", "text": "#!/bin/bash\njarvis ppl run\n", "sha256": "5c"},
        "input_sha256": "11",
        "issues": [],
        "exit_code": 0,
        "would_submit": True,
        "submit_with": ["sbatch", "/x/job.sh"],
        **extra,
    }


def _line(document: dict[str, Any]) -> str:
    return "log line\n" + json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n"


def _state(component: str, state: str, hosts: list[str] | None = None, **extra: Any) -> str:
    entry = {
        "schema_version": jarvis_plan.STATE_SCHEMA,
        "component": component,
        "package": "demo.hello",
        "state": state,
        "hosts": hosts,
        "at": "2026-11-20T10:00:00Z",
        "reason": None,
        **extra,
    }
    return json.dumps(entry)


def test_a_plan_keeps_what_the_card_shows() -> None:
    plan = jarvis_plan.parse(_line(_document()))
    assert plan is not None
    assert plan["plan_sha256"] == "5eed"
    assert [c["id"] for c in plan["components"]] == ["solver", "viewer"]
    assert plan["script"]["text"].startswith("#!/bin/bash")
    assert "submit_with" not in plan and "input_sha256" not in plan
    long = _document(script={"path": "/x", "text": "x" * (jarvis_plan.SCRIPT_LIMIT + 1)})
    kept = jarvis_plan.parse(_line(long))
    assert kept is not None and kept["script"]["text"] is None
    assert jarvis_plan.parse('{"schema_version":"jarvis.execution.handle.v1"}') is None


def test_a_plan_keeps_jarvis_cores_unaltered_and_says_what_they_mean() -> None:
    meaning = "each rank claims 1 whole core(s): 16 ranks per node claim 16 of the 20 cores"
    cores = {
        "schema_version": jarvis_plan.CORES_SCHEMA,
        "partition": "compute",
        "source": "sinfo",
        "threads_per_core": 2,
        "physical_cores_per_node": 20,
        "cores_per_rank": 1,
        "cores_claimed_per_node": 16,
        "fits": True,
        "exclusive": False,
        "meaning": meaning,
    }
    plan = jarvis_plan.parse(_line(_document(cores=cores)))
    assert plan is not None and plan["cores"] == cores
    assert f"Cores: {meaning}" in jarvis_plan.summary(plan)
    other = jarvis_plan.parse(_line(_document(cores={"schema_version": "x", "meaning": "y"})))
    assert other is not None and "cores" not in other
    wider = {**cores, "meaning": "each rank claims 2 whole core(s)"}
    assert f"Cores: {meaning}, now each rank claims 2 whole core(s)" in jarvis_plan.changes(
        plan, {**plan, "cores": wider}
    )


def test_only_the_plan_mode_exit_is_a_plan() -> None:
    text = _line(_document())
    assert jarvis_plan.planned(jarvis_plan.EXIT_PLANNED, text) is not None
    assert jarvis_plan.planned("10", text) is not None
    assert jarvis_plan.planned(0, text) is None
    assert jarvis_plan.planned(10, "no plan") is None


def test_a_bound_spec_names_the_plan_and_an_unplanned_one_is_untouched() -> None:
    spec = jarvis_plan.bound(SPEC, {"plan_sha256": "5eed"})
    assert spec.environment[jarvis_plan.BIND_ENV] == "sha256:5eed"
    assert jarvis_plan.bound(SPEC, None) is SPEC
    assert jarvis_plan.bound(SPEC, {"plan_sha256": ""}) is SPEC


def test_only_a_changed_plan_is_a_changed_plan() -> None:
    changed = Refusal(RefusalReason.INVALID_SPEC, "x", kinds=[jarvis_plan.PLAN_CHANGED])
    assert jarvis_plan.changed(changed)
    assert not jarvis_plan.changed(Refusal(RefusalReason.INVALID_SPEC, "x", kinds=["other"]))
    assert not jarvis_plan.changed(Refusal(RefusalReason.INVALID_SPEC, "x"))


def test_the_first_terminal_state_of_a_component_wins() -> None:
    text = "\n".join(
        [
            _state("solver", "waiting"),
            _state("solver", "running", ["n1", "n2"]),
            "not json",
            json.dumps({"schema_version": "other", "component": "solver", "state": "x"}),
            _state("viewer", "starting", ["n3"]),
            _state("solver", "done", ["n1", "n2"], reason="returned"),
            _state("solver", "cancelled", None, reason="late"),
        ]
    )
    folded = {entry["component"]: entry for entry in jarvis_plan.component_states(text)}
    assert folded["solver"]["state"] == "done"
    assert folded["solver"]["reason"] == "returned"
    assert folded["solver"]["hosts"] == ["n1", "n2"]
    assert folded["viewer"]["state"] == "starting"
    assert list(folded) == ["solver", "viewer"]


def test_the_card_roles_are_jarvis_roles() -> None:
    plan = _document()
    assert jarvis_plan.role(plan, {"role": "batch"}) == "compute"
    assert jarvis_plan.role(plan, {"role": "service"}) == "service"
    assert jarvis_plan.role({"action": "install_check"}, {"role": "batch"}) == "install"


def test_the_summary_names_the_directives_and_components() -> None:
    line = jarvis_plan.summary(_document())
    assert line == (
        "JARVIS's plan: partition compute, account the site default, time 01:00:00; "
        "starts Hello solver on 2 nodes, then Viewer on 1 node; Hello solver feeds Viewer (BP)"
    )


def test_the_status_script_reads_the_component_states_when_asked() -> None:
    plain = jarvis_status_script("/j", "op_x")
    assert "@@components" not in plain
    states = jarvis_status_script("/j", "op_x", states="/e/op_x/components.jsonl")
    assert "@@components" in states
    assert "/e/op_x/components.jsonl" in states


class _Jarvis:
    def __init__(self) -> None:
        self.plans: list[str] = []
        self.submitted: list[JobSpec] = []
        self.sha = "5eed"
        self.refuse_once = False
        self.status = JobStatus(state=JobState.QUEUED)


@pytest.fixture
def jarvis(monkeypatch: pytest.MonkeyPatch, relay_paths: RelayPaths) -> Iterator[_Jarvis]:
    """A site whose JARVIS plans and submits package jobs, the package already reviewed."""
    state = _Jarvis()

    async def plan_job(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.plans.append(operation_id)
        return jarvis_plan.parse(_line(_document(state.sha)))

    async def run(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> JobHandle:
        if state.refuse_once:
            state.refuse_once = False
            state.sha = "beef"
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                "JARVIS refused: the plan changed",
                kinds=[jarvis_plan.PLAN_CHANGED],
            )
        state.submitted.append(spec)
        plan = jarvis_plan.parse(_line(_document(state.sha)))
        return JobHandle(adapter="fixture", native_id="77", data={jarvis_plan.KEY: plan})

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        return state.status

    for name, value in {"plan_job": plan_job, "run": run, "status": status}.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    add_site(relay_paths)
    guard: Any = RelayService(relay_paths).guard
    guard.reviews.put(
        Review(site="fx", package="demo.hello", version="1", sha256=SHA, answered_by="test")
    )
    yield state


def _card(service: RelayService, record: OperationRecord) -> Ask:
    (ask,) = service.outstanding(service.operation(record.operation_id))
    assert ask.binding["kind"] == confirm.CARD_KIND
    return ask


def test_a_package_card_carries_jarvis_plan_and_the_yes_binds_it(
    relay_paths: RelayPaths, jarvis: _Jarvis
) -> None:
    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", SPEC, confirm=True)
        ask = _card(service, record)
        assert jarvis.plans == [record.operation_id]
        assert ask.binding["plan_sha256"] == "5eed"
        assert jarvis_plan.summary(_document()) in ask_text(ask)
        card: dict[str, Any] = dict((ask.meta or {})[job_card.JOB_META])
        assert card["source"] == "jarvis"
        assert card["phase"] == "planned"
        assert (card["partition"], card["time_limit_s"], card["scheduler"]) == (
            "compute",
            3600,
            "slurm",
        )
        assert [(c["id"], c["role"], c["nodes"], c["state"]) for c in card["components"]] == [
            ("solver", "compute", 2, "planned"),
            ("viewer", "service", 1, "planned"),
        ]
        assert card["couplings"] == [
            {
                "from": "solver",
                "to": "viewer",
                "kind": "stream",
                "label": "BP",
                "source": "pipeline",
            }
        ]
        assert card["script"]["sha256"] == "5c"
        assert jarvis.submitted == []
        service.answer(ask.ask_id, "accept", {"approve": "approve"}, by="test")
        record = await service.advance(record.operation_id)
        (submitted,) = jarvis.submitted
        assert submitted.environment[jarvis_plan.BIND_ENV] == "sha256:5eed"
        assert record.state == OperationState.QUEUED, record
        queued = job_card.snapshot(service, record) or {}
        assert queued["phase"] == "queued" and queued["job_id"] == "77"
        assert [c["state"] for c in queued["components"]] == ["waiting", "waiting"]
        await service.close()

    asyncio.run(scenario())


def test_a_changed_plan_asks_once_more_with_the_new_plan(
    relay_paths: RelayPaths, jarvis: _Jarvis
) -> None:
    jarvis.refuse_once = True

    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", SPEC, confirm=True)
        first = _card(service, record)
        service.answer(first.ask_id, "accept", {"approve": "approve"}, by="test")
        record = await service.advance(record.operation_id)
        assert record.state == OperationState.AWAITING_INPUT, record
        second = _card(service, record)
        assert second.ask_id != first.ask_id
        assert second.message.startswith(plan_card.CHANGED)
        assert second.binding["plan_sha256"] == "beef"
        assert (record.result or {})[plan_card.ROUNDS] == 1
        assert ((second.meta or {})[job_card.JOB_META])["source"] == "jarvis"
        assert jarvis.submitted == []
        service.answer(second.ask_id, "accept", {"approve": "approve"}, by="test")
        record = await service.advance(record.operation_id)
        (submitted,) = jarvis.submitted
        assert submitted.environment[jarvis_plan.BIND_ENV] == "sha256:beef"
        assert record.state == OperationState.QUEUED, record
        await service.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_over_mcp_a_changed_plan_is_a_second_card_and_one_submit(
    relay_paths: RelayPaths, jarvis: _Jarvis, mode: str
) -> None:
    jarvis.refuse_once = True
    person = Person()

    async def scenario() -> None:
        async with _client(create_server(relay_paths), mode, person) as client:
            spec = SPEC.model_dump(mode="json", exclude_none=True)
            result = await client.call_tool(
                "run", {"site": "fx", "spec": spec, "wait_s": 0}, meta=FLAG
            )
            view = result.structured_content or {}
            assert view["state"] == "queued", view
            assert view["job"]["source"] == "jarvis"

    asyncio.run(scenario())
    assert len(person.shown) == 2
    assert person.shown[1].message.startswith(plan_card.CHANGED)
    (submitted,) = jarvis.submitted
    assert submitted.environment[jarvis_plan.BIND_ENV] == "sha256:beef"


def test_a_site_jarvis_cannot_plan_for_shows_relay_card(
    relay_paths: RelayPaths, jarvis: _Jarvis, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_plan(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        raise RuntimeError("no JARVIS here")

    monkeypatch.setattr(FixtureAdapter, "plan_job", no_plan)

    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", SPEC, confirm=True)
        ask = _card(service, record)
        assert "plan_sha256" not in ask.binding
        assert ((ask.meta or {})[job_card.JOB_META])["source"] == "relay"
        service.answer(ask.ask_id, "accept", {"approve": "approve"}, by="test")
        await service.advance(record.operation_id)
        (submitted,) = jarvis.submitted
        assert jarvis_plan.BIND_ENV not in submitted.environment
        await service.close()

    asyncio.run(scenario())


def test_the_card_fills_from_jarvis_component_states(
    relay_paths: RelayPaths, jarvis: _Jarvis
) -> None:
    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", SPEC)
        assert jarvis.plans == []
        (submitted,) = jarvis.submitted
        assert jarvis_plan.BIND_ENV not in submitted.environment
        lines = "\n".join(
            [
                _state("solver", "running", ["n1", "n2"]),
                _state("viewer", "starting", ["n3"]),
            ]
        )
        jarvis.status = JobStatus(
            state=JobState.RUNNING,
            components=jarvis_plan.component_states(lines),
            started_at=datetime(2026, 11, 20, 9, 59, 30, tzinfo=UTC),
        )
        record = await service.refresh(record.operation_id)
        card = job_card.snapshot(service, record) or {}
        assert card["phase"] == "running" and card["source"] == "jarvis"
        assert card["started_at"] == "2026-11-20T09:59:30+00:00"
        solver, viewer = card["components"]
        assert (solver["state"], solver["nodes_allocated"], solver["nodes_running"]) == (
            "running",
            2,
            2,
        )
        assert solver["hosts"] == ["n1", "n2"]
        assert (viewer["state"], viewer["nodes_allocated"], viewer["nodes_running"]) == (
            "starting",
            1,
            0,
        )
        jarvis.status = JobStatus(
            state=JobState.COMPLETED,
            exit_code=0,
            components=jarvis_plan.component_states(
                lines + "\n" + _state("solver", "done", ["n1", "n2"], reason="returned")
            ),
            started_at=datetime(2026, 11, 20, 9, 59, 30, tzinfo=UTC),
        )
        record = await service.refresh(record.operation_id)
        ended = job_card.snapshot(service, record) or {}
        assert ended["phase"] == "ended" and ended["outcome"] == "succeeded"
        assert [c["state"] for c in ended["components"]] == ["done", "done"]
        assert ended["components"][0]["reason"] == "returned"
        assert ended["revision"] > card["revision"]
        assert ended["started_at"] == card["started_at"] and ended["advisories"] == []
        await service.close()

    asyncio.run(scenario())


def test_without_a_plan_the_card_shows_a_crashed_service_apart(
    relay_paths: RelayPaths, jarvis: _Jarvis, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def run(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> JobHandle:
        jarvis.submitted.append(spec)
        return JobHandle(adapter="fixture", native_id="78", data={})

    monkeypatch.setattr(FixtureAdapter, "run", run)
    guard: Any = RelayService(relay_paths).guard
    guard.reviews.put(
        Review(site="fx", package="demo.view", version="1", sha256=SHA, answered_by="test")
    )
    spec = JobSpec(
        packages=[
            PackageRef(name="demo.view", version="1", sha256=SHA, service=True),
            PackageRef(name="demo.hello", version="1", sha256=SHA),
        ]
    )

    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", spec)
        lines = "\n".join(
            [
                _state("view", "failed", ["n1"], reason="service crashed: signal 11"),
                _state("hello", "done", ["n1"], reason="returned"),
            ]
        )
        jarvis.status = JobStatus(
            state=JobState.COMPLETED, exit_code=0, components=jarvis_plan.component_states(lines)
        )
        record = await service.refresh(record.operation_id)
        ended = job_card.snapshot(service, record) or {}
        assert ended["source"] == "relay" and ended["outcome"] == "succeeded"
        view, hello = ended["components"]
        assert (view["id"], view["role"], view["state"]) == ("view", "service", "failed")
        assert view["reason"] == "service crashed: signal 11"
        assert view["hosts"] == ["n1"] and view["nodes_running"] == 0
        assert (hello["state"], hello["reason"]) == ("done", "returned")
        await service.close()

    asyncio.run(scenario())


def test_an_unplanned_record_has_no_plan() -> None:
    record = OperationRecord(
        operation_id="op_1",
        kind="run",
        site="fx",
        adapter="fixture",
        spec={"executable": "echo"},
        spec_digest="d",
        state=OperationState.QUEUED,
    )
    assert plan_card.plan_of(record) is None
    assert plan_card.bound(SPEC, record) is SPEC
