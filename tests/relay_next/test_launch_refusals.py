"""A JARVIS launch refusal inside a submitted job reaches the agent as typed issues.

JARVIS collects the net facts at launch, inside the allocation, when the graph
cannot vouch for the launch hosts. When a host returns none, it refuses the
launch with a typed issue in the execution record's ``metadata.issues``. Relay
carries that issue and its exit code into the operation's result untouched.
"""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay.jarvis_submit import record_status
from clio_relay.jobs.handles import JobHandle, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.asyncio

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


async def test_a_launch_refusal_reaches_the_operation_as_typed_issues(
    relay_paths: RelayPaths,
) -> None:
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
    jarvis_record = {"state": "failed", "return_code": 5, "metadata": {"issues": [ISSUE]}}

    async def status(handle: JobHandle) -> JobStatus:
        return record_status(jarvis_record)

    setattr(adapter, "status", status)  # noqa: B010

    done = await service.refresh(record.operation_id)
    assert done.state == OperationState.FAILED
    assert done.exit_code == 5
    result = done.result or {}
    assert result["issues"] == [ISSUE]
    assert "comp-05 returned no net facts" in result["message"]
    view = service.view(done)
    assert view["result"]["issues"] == [ISSUE]
    await service.close()
