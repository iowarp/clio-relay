"""An agent finds a package by name and sees a running package job's step and outputs.

R1: ``describe(site=)`` lists the staged packages and ``run`` pins one named
alone. R2: a package job's JARVIS records give its step and outputs on any
adapter with ``files``. R3: each look at a job says when it was and what the
site said, and a look that fails says so instead of leaving the old answer.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.hooks import set_hook
from clio_relay.guard import create_guard
from clio_relay.jarvis_site import archive_package
from clio_relay.jobs.handles import VIEW_METADATA, JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.packages import stage
from clio_relay.surface.describing import site_report
from clio_relay.surface.package_progress import JARVIS_SHARED, with_package_progress
from clio_relay.surface.progress import progress_message
from clio_relay.surface.results import operation_result
from clio_relay.surface.server import RelayTasksExtension
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from ..guard.conftest import make_repo
from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

VIEW: dict[str, Any] = {
    "schema": "clio.view-descriptor.v1",
    "name": "snapshots",
    "path": "/scratch/job/run/data.bp5",
    "path_kind": "directory",
    "format": "adios2-bp5",
    "reader": "ADIOS2VTXReader",
    "growing": True,
}


@pytest.fixture
def guarded() -> Iterator[None]:
    set_hook("guard", create_guard)
    yield
    set_hook("guard", None)


async def test_describe_lists_packages_and_run_pins_one_named_alone(
    relay_paths: RelayPaths, tmp_path: Path, guarded: None
) -> None:
    add_site(relay_paths, policy="guarded")
    repo = make_repo(tmp_path / "src")
    entry = stage(
        relay_paths.state_dir, archive_package(repo, "demo.hello"), "demo.hello", version="0.1.0"
    )
    service = RelayService(relay_paths)
    [listed] = (await site_report(service, service.site("fx")))["packages"]
    assert (listed["name"], listed["version"], listed["sha256"]) == (
        "demo.hello",
        "0.1.0",
        entry.sha256,
    )
    assert listed["parameters"]["properties"]["out"]["default"] == "out.txt"
    assert listed["reviewed"] is False

    record = await service.start_run(
        "fx", JobSpec(package=PackageRef(name="demo.hello", parameters={"out": "x.txt"}))
    )
    assert record.spec["package"]["version"] == "0.1.0"
    assert record.spec["package"]["sha256"] == entry.sha256
    assert record.state == OperationState.AWAITING_INPUT
    reviews = [
        ask for ask in service.outstanding(record) if ask.binding.get("kind") == "package_review"
    ]
    assert [ask.binding["package_hash"] for ask in reviews] == [entry.sha256]

    with pytest.raises(Refusal) as raised:
        await service.start_run("fx", JobSpec(package=PackageRef(name="demo.missing")))
    assert raised.value.reason == RefusalReason.INVALID_SPEC
    assert "demo.hello" in raised.value.message
    await service.close()


def _jsonl(path: Path, lines: list[dict[str, Any]], *, partial: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(line) + "\n" for line in lines) + partial)


def _execution(job_dir: Path, name: str) -> Path:
    execution = job_dir / JARVIS_SHARED / "relay_abc" / "executions" / name
    execution.mkdir(parents=True)
    return execution


def _artifact(state: str, size: int) -> dict[str, Any]:
    return {
        "schema_version": "jarvis.artifact.v1",
        "artifact_id": "art-1",
        "logical_name": "snapshots",
        "state": state,
        "format": "adios2-bp5",
        "size_bytes": size,
        "location": {"kind": "path", "value": VIEW["path"]},
        "metadata": {"clio": {"view": VIEW}},
    }


async def test_a_package_jobs_records_give_its_step_and_outputs(
    relay_paths: RelayPaths, tmp_path: Path
) -> None:
    add_site(relay_paths, "lab", adapter="local", config={"scheduler": "none"})
    service = RelayService(relay_paths)
    adapter = service.adapter_named(service.site("lab"), "local")
    job_dir = tmp_path / "job"
    old = _execution(job_dir, "old")
    _jsonl(
        old / "progress" / "pkg-1.jsonl", [{"schema_version": "jarvis.progress.v1", "current": 99}]
    )
    execution = _execution(job_dir, "new")
    step = {
        "schema_version": "jarvis.progress.v1",
        "package_name": "pkg",
        "state": "running",
        "unit": "step",
        "total": 10,
        "message": "wrote snapshot 3",
        "metadata": {"clio": {"step": 3, "output": "snapshots", "view": VIEW}},
    }
    _jsonl(
        execution / "progress" / "pkg-1.jsonl",
        [
            {**step, "sequence": 1, "current": 1, "observed_at_epoch": 1_000.0},
            {**step, "sequence": 2, "current": 3, "observed_at_epoch": 1_600.0},
        ],
        partial='{"schema_version": "jarvis.prog',
    )
    (execution / "progress" / "pkg-1.jsonl.lock").write_text("")
    _jsonl(
        execution / "artifacts" / "pkg-1.jsonl",
        [_artifact("growing", 10), _artifact("complete", 20)],
    )
    os.utime(old, (1_000, 1_000))
    os.utime(execution, (2_000, 2_000))
    record = OperationRecord.model_validate(
        {
            "operation_id": "op_pkg",
            "kind": "run",
            "site": "lab",
            "adapter": "local",
            "spec": {
                "package": {"name": "demo.hello", "version": "1", "sha256": "a" * 64},
                "attributes": {"duration": 2_400},
            },
            "spec_digest": "sha256:0",
            "state": "running",
            "handle": JobHandle(adapter="local", data={"job_dir": str(job_dir)}).model_dump(),
        }
    )
    stdout = OutputRef(name="stdout", path=str(job_dir / "stdout"))
    seen = await with_package_progress(
        adapter,
        record,
        JobStatus(
            state=JobState.RUNNING,
            outputs=[stdout],
            started_at=datetime.fromtimestamp(900, UTC),
        ),
    )
    assert seen.progress is not None
    assert (seen.progress.step, seen.progress.total) == (3, 10)
    assert seen.progress.message == "wrote snapshot 3"
    assert seen.progress.data["execution"].endswith("/executions/new")
    # 300 s a step from step 1 to 3; 7 more end at 3700, past the limit's end at 3300.
    pace = seen.progress.data["pace"]
    assert (pace["seconds_per_step"], pace["remaining_steps"]) == (300.0, 7)
    assert pace["projected_end_at"] == datetime.fromtimestamp(3_700, UTC).isoformat()
    assert (pace["time_limit_s"], pace["limit_margin_s"]) == (2_400, -400)
    record.result = {"progress": seen.progress.model_dump(mode="json")}
    assert "measured 5 min per step over steps 1 to 3" in str(progress_message(record))
    assert "6 min after the job's time limit ends it" in str(progress_message(record))
    [snapshot] = seen.progress.outputs
    assert (snapshot.name, snapshot.path, snapshot.metadata) == (
        "snapshots",
        VIEW["path"],
        {VIEW_METADATA: VIEW},
    )
    assert [item.name for item in seen.outputs] == ["stdout", "snapshots"]
    assert seen.outputs[1].size == 20
    assert seen.outputs[1].metadata["state"] == "complete"

    record.result = {"progress": seen.progress.model_dump(mode="json")}
    os.utime(old, (3_000, 3_000))
    again = await with_package_progress(adapter, record, JobStatus(state=JobState.COMPLETED))
    assert again.progress is not None and again.progress.step == 3, "the execution is kept"

    plain = record.model_copy(update={"spec": {"executable": "echo"}})
    assert (
        await with_package_progress(adapter, plain, JobStatus(state=JobState.RUNNING))
    ).progress is None
    missing = record.model_copy(
        update={
            "result": None,
            "handle": JobHandle(adapter="local", data={"job_dir": str(tmp_path / "none")}),
        }
    )
    unread = JobStatus(state=JobState.RUNNING)
    assert await with_package_progress(adapter, missing, unread) is unread
    await service.close()


async def _running(service: RelayService, operation_id: str) -> OperationRecord:
    for _ in range(100):
        record = service.operation(operation_id)
        if record.handle is not None:
            return record
        await asyncio.sleep(0.05)
    raise AssertionError("the job never got a handle")


async def test_each_look_says_when_and_what_and_a_failed_look_says_so(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", JobSpec(executable="sleep", arguments=["30"]))
    record = await _running(service, record.operation_id)
    adapter = service.adapter_named(service.site(record.site), record.adapter)
    answer: list[JobStatus | Exception] = [
        JobStatus(state=JobState.RUNNING, native_state="RUNNING", message="Slurm says RUNNING")
    ]

    async def status(handle: JobHandle) -> JobStatus:
        if isinstance(answer[0], Exception):
            raise answer[0]
        return answer[0]

    setattr(adapter, "status", status)  # noqa: B010
    tasks = RelayTasksExtension(service)

    first = await service.refresh(record.operation_id)
    result = first.result or {}
    assert result["message"] == "Slurm says RUNNING"
    assert result["native_state"] == "RUNNING"
    looked = result["observed_at"]
    assert (await tasks.task_result(first)).status_message == (
        "relay operation is running: Slurm says RUNNING"
    )
    text = operation_result(service.view(first)).content[0]
    assert f"relay last looked at {looked}; Slurm says RUNNING" in getattr(text, "text", "")

    answer[0] = RuntimeError("ssh: connection reset")
    failed = await service.refresh(record.operation_id)
    assert failed.state == OperationState.RUNNING
    assert (failed.result or {})["message"] == (
        "relay could not observe the job: RuntimeError: ssh: connection reset"
    )
    assert (failed.result or {})["observation_failed_at"]
    assert "could not observe" in str((await tasks.task_result(failed)).status_message)

    answer[0] = JobStatus(state=JobState.RUNNING, native_state="RUNNING")
    quiet = await service.refresh(record.operation_id)
    result = quiet.result or {}
    assert "message" not in result
    assert "observation_failed_at" not in result
    assert result["observed_at"] >= looked
    await service.cancel(record.operation_id)
    await service.close()
