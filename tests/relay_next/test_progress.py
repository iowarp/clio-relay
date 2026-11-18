"""A running job's steps and view descriptors reach the operation and its task (RL40, RL43)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from clio_relay.jobs.handles import (
    PROVENANCE_METADATA,
    VIEW_METADATA,
    JobHandle,
    JobProgress,
    JobState,
    JobStatus,
    OutputRef,
)
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.progress import progress_message
from clio_relay.surface.server import RelayTasksExtension
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

VIEW: dict[str, Any] = {
    "schema": "clio.view-descriptor.v1",
    "path": "/scratch/run/data.bp",
    "path_kind": "directory",
    "format": "adios2-bp5",
    "reader": "ADIOS2VTXReader",
    "growing": True,
}
PROVENANCE: dict[str, Any] = {"package": "xcompact3d", "version": "1.0", "hash": "sha256:abc"}
STEP = JobProgress(
    step=3,
    total=10,
    message="wrote snapshot 3",
    outputs=[
        OutputRef(
            name="snapshots",
            path="/scratch/run/data.bp",
            metadata={VIEW_METADATA: VIEW, PROVENANCE_METADATA: PROVENANCE},
        ),
        OutputRef(name="log", path="/scratch/run/log.txt"),
    ],
    data={"time": 0.3},
)


async def test_output_metadata_is_optional_and_kept_as_given() -> None:
    assert OutputRef(name="a", path="/a").metadata == {}
    dumped = STEP.outputs[0].model_dump(mode="json")
    assert dumped["metadata"] == {"view": VIEW, "provenance": PROVENANCE}
    assert OutputRef.model_validate(dumped) == STEP.outputs[0]
    assert JobStatus(state=JobState.RUNNING).progress is None


async def _running(service: RelayService, operation_id: str) -> OperationRecord:
    for _ in range(100):
        record = service.operation(operation_id)
        if record.handle is not None:
            return record
        await asyncio.sleep(0.05)
    raise AssertionError("the job never got a handle")


async def test_a_reported_step_reaches_the_operation_and_the_task(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", JobSpec(executable="sleep", arguments=["30"]))
    record = await _running(service, record.operation_id)
    adapter = service.adapter_named(service.site(record.site), record.adapter)

    async def status(handle: JobHandle) -> JobStatus:
        return JobStatus(state=JobState.RUNNING, outputs=STEP.outputs, progress=STEP)

    setattr(adapter, "status", status)  # noqa: B010
    seen = await service.refresh(record.operation_id)
    stored = (seen.result or {})["progress"]
    expected = STEP.model_dump(mode="json")
    assert {key: stored[key] for key in ("step", "total", "message", "data")} == {
        key: expected[key] for key in ("step", "total", "message", "data")
    }
    assert [item["name"] for item in stored["outputs"]] == ["snapshots", "log"]
    assert stored["outputs"][1]["metadata"]["provenance"]["operation_id"] == record.operation_id
    view = service.view(seen)
    assert view["result"]["progress"]["outputs"][0]["metadata"]["view"] == VIEW
    assert view["outputs"][0]["metadata"]["provenance"] == PROVENANCE
    task = await RelayTasksExtension(service).task_result(seen)
    assert task.status_message == (
        "relay operation is running: step 3 of 10; wrote snapshot 3; views: snapshots"
    )
    await service.cancel(record.operation_id)
    await service.close()


async def test_a_task_without_steps_keeps_its_plain_status(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", JobSpec(executable="sleep", arguments=["30"]))
    record = await _running(service, record.operation_id)
    assert progress_message(record) is None
    task = await RelayTasksExtension(service).task_result(record)
    assert task.status_message == f"relay operation is {record.state.value}"
    await service.cancel(record.operation_id)
    await service.close()


async def test_the_status_line_names_only_what_the_step_reported() -> None:
    record = OperationRecord.model_validate(
        {
            "operation_id": "op",
            "kind": "run",
            "site": "fx",
            "adapter": "local",
            "spec": {},
            "spec_digest": "sha256:0",
            "state": "running",
            "result": {"progress": {"message": "meshing"}},
        }
    )
    assert progress_message(record) == "meshing"
    record.result = {"progress": {"step": 2}}
    assert progress_message(record) == "step 2"
    record.result = {"progress": {}}
    assert progress_message(record) is None
