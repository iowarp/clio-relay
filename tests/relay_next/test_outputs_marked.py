"""Relay marks every stored output with provenance and checks view descriptors (RL43)."""

from __future__ import annotations

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
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.outputs import VIEW_PROBLEMS_METADATA, marked_status
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

GOOD_VIEW = {
    "schema": "clio.view-descriptor.v1",
    "path": "/scratch/run/gs.bp",
    "path_kind": "directory",
    "format": "adios2-bp5",
    "reader": "ADIOS2VTXReader",
    "growing": True,
}


def _record() -> OperationRecord:
    return OperationRecord(
        operation_id="op_1",
        kind="run",
        site="lab",
        adapter="ssh",
        spec={},
        spec_digest="c" * 64,
        handle=JobHandle(adapter="ssh", native_id="27722"),
        state=OperationState.RUNNING,
    )


async def test_progress_views_are_checked_and_outputs_carry_provenance() -> None:
    bad_view = {**GOOD_VIEW, "path": "relative/gs.bp"}
    status = JobStatus(
        state=JobState.RUNNING,
        outputs=[OutputRef(name="log", path="/run/log")],
        progress=JobProgress(
            step=3,
            outputs=[
                OutputRef(name="gs.bp", path="/run/gs.bp", metadata={VIEW_METADATA: GOOD_VIEW}),
                OutputRef(name="old.bp", path="/run/old.bp", metadata={VIEW_METADATA: bad_view}),
            ],
        ),
    )
    marked = marked_status(_record(), status)
    assert marked.outputs[0].metadata[PROVENANCE_METADATA]["operation_id"] == "op_1"
    assert marked.progress is not None
    good, bad = marked.progress.outputs
    assert good.metadata[VIEW_METADATA] == GOOD_VIEW
    assert good.metadata[PROVENANCE_METADATA]["native_id"] == "27722"
    assert VIEW_METADATA not in bad.metadata
    assert any(item.startswith("path:") for item in bad.metadata[VIEW_PROBLEMS_METADATA])
    assert marked.progress.step == 3


async def test_a_packages_own_provenance_is_kept() -> None:
    own = {"schema": "package.v1"}
    status = JobStatus(
        state=JobState.COMPLETED,
        outputs=[OutputRef(name="a", metadata={PROVENANCE_METADATA: own})],
    )
    assert marked_status(_record(), status).outputs[0].metadata[PROVENANCE_METADATA] == own


async def test_run_outputs_carry_relays_provenance(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "lab", adapter="local", config={"scheduler": "none"})
    service = RelayService(relay_paths)
    record = await service.start_run("lab", JobSpec(executable="sh", arguments=["-c", "echo hi"]))
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED, done
    assert done.outputs
    for output in done.outputs:
        block = output.metadata[PROVENANCE_METADATA]
        assert block["operation_id"] == record.operation_id
        assert (block["site"], block["adapter"]) == ("lab", "local")
        assert block["spec_digest"] == record.spec_digest
    viewed = service.view(service.operation(record.operation_id))
    assert all(PROVENANCE_METADATA in item["metadata"] for item in viewed["outputs"])
    await service.close()
