"""A package job's typed failure reaches the agent with its message, not a bare exit.

JARVIS collects the net facts at launch, inside the allocation, when the graph
cannot vouch for the launch hosts. When a host returns none, it refuses the
launch with a typed issue in the execution record's ``metadata.issues``. Relay
carries that issue and its exit code into the operation's result untouched.
A package's own failure (an exception JARVIS records as the execution's
``error``) reaches the result's message the same way. A package service that
did not start ends with that message, even when its connector was signalled.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.serve.report import final_message
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from ..serve.helpers import frp_plan
from .conftest import add_site

ISSUE: dict[str, Any] = {
    "schema_version": "jarvis.issue.v1",
    "kind": "resource_graph_collect_partial",
    "field": "resource_graph.net",
    "message": "comp-05 returned no net facts, so the launch did not go out",
    "details": {
        "stage": "launch",
        "collected_hosts": ["comp-03", "comp-04", "comp-06"],
        "failed_hosts": [{"host": "comp-05", "via": "ssh", "exit_code": 255}],
    },
}
DIVERGED = (
    "xcompact3d diverged: Velocity diverged! SIMULATION IS STOPPED! at step 412/2000 "
    "(time 4.12); dt 0.01, grid 129x65x65, suggested_dt 0.004; snapshots 2/20; "
    "details in /scratch/run/clio-run-result.json"
)
NOT_STARTED = (
    "pvserver did not start: mpi_wireup_failed: btl_tcp process identifier mismatch\n"
    "ranks 8 on comp-03,comp-04; start timeout 270s"
)


async def _failed_run(relay_paths: RelayPaths, jarvis_record: dict[str, Any]) -> dict[str, Any]:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = OperationRecord.model_validate(
        {
            "operation_id": "op_launch",
            "kind": "run",
            "site": "fx",
            "adapter": "fixture",
            "spec": {"package": {"name": "demo.hello", "version": "1", "sha256": "a" * 64}},
            "spec_digest": "sha256:0",
            "state": "running",
            "handle": JobHandle(adapter="fixture", native_id="27999").model_dump(),
        }
    )
    service.operations.put(record.operation_id, record)
    adapter = service.adapter_named(service.site("fx"), "fixture")

    async def status(handle: JobHandle) -> JobStatus:
        return record_status(jarvis_record)

    setattr(adapter, "status", status)  # noqa: B010

    done = await service.refresh(record.operation_id)
    assert done.state == OperationState.FAILED
    view = service.view(done)
    await service.close()
    return {"exit_code": done.exit_code, "result": done.result or {}, "view": view}


@pytest.mark.asyncio
async def test_a_launch_refusal_reaches_the_operation_as_typed_issues(
    relay_paths: RelayPaths,
) -> None:
    jarvis_record = {"state": "failed", "return_code": 5, "metadata": {"issues": [ISSUE]}}
    done = await _failed_run(relay_paths, jarvis_record)
    assert done["exit_code"] == 5
    assert done["result"]["issues"] == [ISSUE]
    assert "comp-05 returned no net facts" in done["result"]["message"]
    assert done["view"]["result"]["issues"] == [ISSUE]


@pytest.mark.asyncio
async def test_a_diverged_run_reaches_the_operation_with_its_message(
    relay_paths: RelayPaths,
) -> None:
    jarvis_record = {"state": "failed", "return_code": 1, "error": DIVERGED}
    done = await _failed_run(relay_paths, jarvis_record)
    assert done["exit_code"] == 1
    assert done["result"]["message"] == DIVERGED
    assert "clio-run-result.json" in done["view"]["result"]["message"]


@pytest.mark.parametrize("reason", ["signal", "ended", "exited"])
def test_a_package_service_that_did_not_start_ends_with_its_cause(
    tmp_path: Path, reason: str
) -> None:
    plan = frp_plan(tmp_path).model_copy(update={"package": True})
    status = record_status({"state": "failed", "return_code": 1, "error": NOT_STARTED})
    assert status.state == JobState.FAILED
    message = final_message(plan, status, {"reason": reason, "exit_code": 1})
    assert "pvserver did not start: mpi_wireup_failed: btl_tcp" in message
    assert message.count("pvserver did not start") == 1


def test_a_plain_service_keeps_its_connector_reason(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    status = JobStatus(state=JobState.FAILED, message="Slurm says FAILED")
    assert final_message(plan, status, {"reason": "signal"}) == "the job was signalled"
