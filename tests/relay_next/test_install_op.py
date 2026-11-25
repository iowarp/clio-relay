"""Install a package's software ahead of any run.

The ``install`` operation asks JARVIS ``ppl install +check``. When the software
is there it completes and nothing ran. When it is not, it takes a run's install
path: the same asks, then ``ppl install +submit`` as a job, then the check
again. When the site has no JARVIS, relay asks to install jarvis-cd first.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from fastmcp import Client

from clio_relay import install_wording, jarvis_exits, jarvis_report, jarvis_submit
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op, installing, interrupted, server
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

SPEC = JobSpec(package=PackageRef(name="demo.hello", version="1", sha256="a" * 64))


def _report(satisfied: bool) -> dict[str, Any]:
    status = "satisfied" if satisfied else "missing"
    package: dict[str, Any] = {"status": status, "spec": "hello@2.12"}
    if not satisfied:
        package["missing"] = ["hello@2.12", "zlib@1.3"]
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": satisfied,
        "installers": {"spack": {"packages": {"pkg": package}}},
        "issues": [],
    }


@dataclass
class _Site:
    """What the stand-in JARVIS has on the site, and what relay asked of it."""

    jarvis: bool = True
    installed: bool = False
    refuse: Refusal | None = None
    calls: list[str] = field(default_factory=list[str])


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        if state.refuse is not None:
            raise state.refuse
        if not state.jarvis:
            raise jarvis_submit.missing_jarvis('{"missing":"jarvis","tried":"/h/j"}', site="fx")
        if not state.installed:
            raise jarvis_report.install_needed(_report(False), site="fx", walltime="01:00:00")
        return _report(True)

    async def install(
        self: FixtureAdapter, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        state.calls.append("install")
        return JobHandle(adapter="fixture", native_id="install-1")

    async def install_jarvis(self: FixtureAdapter) -> str:
        state.calls.append("install_jarvis")
        state.jarvis = True
        return "/h/.clio-relay/jarvis-cd/bin/jarvis"

    real_status = FixtureAdapter.status

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        if handle.native_id != "install-1":
            return await real_status(self, handle)
        state.installed = True
        return JobStatus(state=JobState.COMPLETED, exit_code=0)

    async def run(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> JobHandle:
        raise AssertionError("an install launches no app")

    for name, value in {
        "install_check": install_check,
        "install": install,
        "install_jarvis": install_jarvis,
        "status": status,
        "run": run,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


async def _approve(service: RelayService, operation_id: str) -> None:
    record = service.operation(operation_id)
    assert record.state == OperationState.AWAITING_INPUT, record
    (ask,) = service.outstanding(record)
    assert ask.kind == "approve"
    service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    await service.advance(operation_id)


def test_installed_software_completes_and_nothing_runs(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        site.installed = True
        add_site(relay_paths, spend="always")
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        done = service.operation(record.operation_id)
        assert done.kind == "install"
        assert done.state == OperationState.COMPLETED, done
        result = done.result or {}
        assert result["installed"]["reuse"] == ["demo.hello"]
        assert "charge" not in result
        assert service.outstanding(done) == []
        assert "nothing ran" in result["message"]
        assert site.calls == ["check"]
        await service.close()

    asyncio.run(scenario())


def test_missing_software_is_asked_then_installed_as_a_job(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        asked = service.operation(record.operation_id)
        (ask,) = service.outstanding(asked)
        assert "nothing runs after it" in ask.message
        await _approve(service, record.operation_id)
        assert service.operation(record.operation_id).state == OperationState.QUEUED
        done = await service.refresh(record.operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert site.calls == ["check", "install", "check"]
        assert "nothing ran" in (done.result or {})["message"]
        await service.close()

    asyncio.run(scenario())


def test_a_site_without_jarvis_gets_it_first(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        site.jarvis = False
        site.installed = True
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        (ask,) = service.outstanding(service.operation(record.operation_id))
        assert ask.binding["jarvis"] == jarvis_submit.JARVIS_PIN
        assert "Install it?" in ask.message
        await _approve(service, record.operation_id)
        done = service.operation(record.operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert site.calls == ["check", "install_jarvis", "check"]
        await service.close()

    asyncio.run(scenario())


def test_a_site_jarvis_cannot_submit_to_fails_typed(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        site.refuse = jarvis_exits.install_only_refused("local", site="fx")
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        done = service.operation(record.operation_id)
        assert done.state == OperationState.FAILED
        assert (done.error or {})["reason"] == RefusalReason.ADAPTER_UNAVAILABLE.value
        await service.close()

    asyncio.run(scenario())


def test_install_takes_a_package_and_an_adapter_that_installs(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        with pytest.raises(Refusal) as no_steps:
            await install_op.start(service, "fx", JobSpec(executable="echo"))
        assert no_steps.value.reason == RefusalReason.INVALID_SPEC
        with pytest.raises(Refusal) as cannot:
            await install_op.start(service, "fx", SPEC)
        assert cannot.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED
        assert service.operations.ids() == []
        await service.close()

    asyncio.run(scenario())


def test_install_is_a_task_tool_that_changes_the_site(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        listed = tools["install"].model_dump(by_alias=True, mode="json", exclude_none=True)
        assert listed["annotations"].get("readOnlyHint") is not True
        assert "ahead of" in (tools["install"].description or "")

    assert "install" in server.TASK_TOOLS
    asyncio.run(scenario())


def test_the_install_ask_says_no_run_follows() -> None:
    report = _report(False)
    names = {"pkg": "demo.hello"}
    alone = install_wording.install_message(
        report, site="Ares", package="demo.hello", names=names, walltime=None, run=False
    )
    before = install_wording.install_message(
        report, site="Ares", package="demo.hello", names=names, walltime=None
    )
    assert "nothing runs after it" in alone
    assert "before the run" in before
    assert "before the run" not in alone


def test_the_install_ask_names_each_method_jarvis_resolved() -> None:
    report: dict[str, Any] = {
        "installers": {
            "spack": {"packages": {"sim": {"status": "missing", "spec": "lammps@2Aug2023"}}},
            "pip": {"packages": {"tool": {"status": "missing", "query": ["rich-cli", "pyyaml"]}}},
            "container": {"packages": {"box": {"status": "missing", "query": ["alpine:3.20"]}}},
        },
        "issues": [],
    }
    summary = jarvis_report.install_summary(report)
    assert {item["method"]: item["spec"] for item in summary["build"]} == {
        "spack": "lammps@2Aug2023",
        "pip": "rich-cli pyyaml",
        "container": "alpine:3.20",
    }
    message = install_wording.install_message(
        report, site="Ares", package="demo.tool", names={}, walltime=None
    )
    assert "To build: lammps@2Aug2023." in message
    assert "To install: rich-cli pyyaml (with pip), alpine:3.20 (with container)." in message
    refusal = jarvis_report.install_needed(report, site="Ares")
    assert "JARVIS installs 3 package(s)" in refusal.message


def _left_by_a_dead_process(service: RelayService, operation_id: str, stage: str) -> None:
    """Leave the record in ``submitting`` at ``stage``, as a relay that died there leaves it."""
    with service.operations.locked(operation_id):
        stored = service.operation(operation_id)
        state = {**installing.state_of(stored), "stage": stage}
        state.pop("handle", None)
        stored.result = {**(stored.result or {}), installing.INSTALL_KEY: state}
        stored.move(OperationState.SUBMITTING)
        stored.updated_at = stored.updated_at - interrupted.GRACE * 2
        service.operations.put(operation_id, stored)


def test_a_check_a_dead_process_left_runs_again(relay_paths: RelayPaths, site: _Site) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        await _approve(service, record.operation_id)
        site.installed = True
        _left_by_a_dead_process(service, record.operation_id, "done")
        done = await service.refresh(record.operation_id)
        assert done.state == OperationState.COMPLETED, done
        assert site.calls == ["check", "install", "check"]
        assert any("JARVIS checks again" in (change.note or "") for change in done.history)
        await service.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("listed", [False, True])
def test_an_install_start_a_dead_process_left_starts_again_once_no_job_is_listed(
    relay_paths: RelayPaths, site: _Site, monkeypatch: pytest.MonkeyPatch, listed: bool
) -> None:
    async def live_jobs(self: FixtureAdapter, known: list[str]) -> list[dict[str, str]]:
        jobs = [{"native_id": "9", "name": f"relay-{op}", "state": "RUNNING"} for op in ops]
        return jobs if listed else []

    monkeypatch.setattr(FixtureAdapter, "live_jobs", live_jobs, raising=False)
    ops: list[str] = []

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        ops.append(record.operation_id)
        await _approve(service, record.operation_id)
        _left_by_a_dead_process(service, record.operation_id, "starting")
        after = await service.refresh(record.operation_id)
        if listed:
            assert after.state == OperationState.SUBMITTING
            assert site.calls == ["check", "install"]
        else:
            assert after.state == OperationState.QUEUED, after
            assert site.calls == ["check", "install", "install"]
        await service.close()

    asyncio.run(scenario())


def test_the_check_exit_is_the_install_report_or_a_typed_refusal() -> None:
    line = '{"schema_version":"jarvis.install-report.v1","satisfied":true,"installers":{}}'
    assert jarvis_exits.checked(f"noise\n{line}\n")["satisfied"] is True
    with pytest.raises(Refusal) as unreadable:
        jarvis_exits.checked("no report")
    assert unreadable.value.reason == RefusalReason.SUBMIT_FAILED
    missing = line.replace("true", "false")
    with pytest.raises(Refusal) as needed:
        jarvis_exits.refuse(3, missing, "", spec=SPEC, site="fx", install_walltime_s=3600)
    assert needed.value.data["needs"] == jarvis_submit.NEEDS_INSTALL
    jarvis_exits.refuse(0, line, "", spec=SPEC, site="fx", install_walltime_s=3600)
    refused = jarvis_exits.install_only_refused("local", site="fx")
    assert refused.data["needs"] == "a Slurm site"


@pytest.mark.parametrize("exit_code", [0, jarvis_submit.EXIT_DECISION])
def test_concurrent_observers_of_an_ended_install_job_check_it_once(
    relay_paths: RelayPaths, site: _Site, monkeypatch: pytest.MonkeyPatch, exit_code: int
) -> None:
    """Two ``tasks/get`` at once on one ended job: one resubmits, the other leaves it be."""
    check = FixtureAdapter.install_check

    async def slow_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        if site.calls.count("check"):
            await asyncio.sleep(0.05)  # the other observer runs while this check is out
        return await check(self, spec, operation_id=operation_id)

    async def ended(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        await asyncio.sleep(0)  # a real look goes over the network: both observers see the end
        state = JobState.COMPLETED if exit_code == 0 else JobState.FAILED
        return JobStatus(state=state, exit_code=exit_code)

    monkeypatch.setattr(FixtureAdapter, "install_check", slow_check)
    monkeypatch.setattr(FixtureAdapter, "status", ended)

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        record = await install_op.start(service, "fx", SPEC)
        await _approve(service, record.operation_id)
        asked = len(service.operation(record.operation_id).asks)
        await asyncio.gather(*(service.refresh(record.operation_id) for _ in range(2)))
        after = service.operation(record.operation_id)
        assert site.calls == ["check", "install", "check"]
        # The check found the software still missing: it asks once, not once per observer.
        assert after.state == OperationState.AWAITING_INPUT, after
        assert len(after.asks) == asked + 1
        assert len(service.outstanding(after)) == 1
        assert [change.state for change in after.history].count(OperationState.SUBMITTING) == 2
        await service.close()

    asyncio.run(scenario())


def test_the_submit_lease_is_the_call_chain_s_alone(relay_paths: RelayPaths) -> None:
    """A nested call takes the lease again; another coroutine finds it held and waits for none."""

    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        store = service.operations
        holding, release = asyncio.Event(), asyncio.Event()

        async def holder() -> None:
            with store.submitting("op_1"), store.submitting("op_1"):  # nested: it resubmits
                holding.set()
                await release.wait()

        async def other() -> bool:
            await holding.wait()
            taken = store.claim_submit("op_1") is not None
            release.set()
            return taken

        _, taken = await asyncio.gather(holder(), other())
        assert taken is False
        assert store.submit_in_hand("op_1") is False
        lease = store.claim_submit("op_1")
        assert lease is not None
        with lease:
            assert store.submit_in_hand("op_1") is True
        await service.close()

    asyncio.run(scenario())
