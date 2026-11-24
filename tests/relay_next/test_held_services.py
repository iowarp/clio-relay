"""A new job of a site asks once whether to end the site's running services at its submit."""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface import held_services
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

ECHO = JobSpec(executable="echo", arguments=["hi"])


def _service(
    paths: RelayPaths, *, running: bool = True, spend: str = "never"
) -> tuple[RelayService, list[str]]:
    SiteStore(paths.sites_file).add(
        SiteEntry.model_validate(
            {
                "id": "fx",
                "adapter": "fixture",
                "config": {"queue_s": 0.1, "run_s": 0.1},
                "policy": "open",
                "spend": {"ask": spend},
            }
        )
    )
    service = RelayService(paths)
    pilot = OperationRecord(
        operation_id="op_pilot",
        kind="serve",
        site="fx",
        adapter="fixture",
        spec={"job": {"executable": "pvserver"}},
        spec_digest="d",
        state=OperationState.RUNNING if running else OperationState.COMPLETED,
    )
    service.operations.put(pilot.operation_id, pilot)
    cancelled: list[str] = []

    async def _cancel(operation_id: str) -> OperationRecord:
        cancelled.append(operation_id)
        record = service.operation(operation_id)
        record.move(OperationState.CANCELLED)
        return record

    service.cancel = _cancel  # type: ignore[method-assign]
    return service, cancelled


@pytest.mark.asyncio
async def test_no_running_service_means_no_ask(relay_paths: RelayPaths) -> None:
    service, cancelled = _service(relay_paths, running=False)
    record = await service.start_run("fx", ECHO)
    assert record.state != OperationState.AWAITING_INPUT
    assert held_services.KEY not in (record.result or {})
    assert cancelled == []
    await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "content", "ends"),
    [
        ("accept", {held_services.FIELD: held_services.END}, True),
        ("accept", {held_services.FIELD: held_services.KEEP}, False),
        ("decline", None, False),
    ],
)
async def test_the_person_chooses_whether_the_running_service_ends(
    relay_paths: RelayPaths, action: str, content: dict[str, Any] | None, ends: bool
) -> None:
    service, cancelled = _service(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.AWAITING_INPUT
    (ask,) = service.outstanding(record)
    assert "op_pilot" in ask.message and "pvserver" in ask.message
    schema = ask.requested_schema
    assert schema is not None and schema["properties"][held_services.FIELD]
    service.answer(ask.ask_id, action, content, by="test")
    record = await service.advance(record.operation_id)
    assert record.state not in (OperationState.AWAITING_INPUT, OperationState.FAILED)
    assert cancelled == (["op_pilot"] if ends else [])
    held = service.operation(record.operation_id).result or {}
    assert held[held_services.KEY]["ended"] == cancelled
    await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "content", "ends"),
    [
        ("accept", {"approve": "approve", held_services.FIELD: held_services.END}, True),
        ("accept", {"approve": "approve", held_services.FIELD: held_services.KEEP}, False),
    ],
)
async def test_the_choice_rides_in_the_jobs_own_approval(
    relay_paths: RelayPaths, action: str, content: dict[str, Any], ends: bool
) -> None:
    service, cancelled = _service(relay_paths, spend="always")
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.AWAITING_INPUT
    (ask,) = service.outstanding(record)  # one ask: the approval carries the choice
    assert ask.kind == "approve"
    schema = ask.requested_schema
    assert schema is not None
    choice = schema["properties"][held_services.FIELD]
    assert choice["title"] == held_services.TITLE and "op_pilot" in choice["description"]
    assert held_services.ask_ids(service.operation(record.operation_id)) == set()
    service.answer(ask.ask_id, action, content, by="test")
    record = await service.advance(record.operation_id)
    assert record.state not in (OperationState.AWAITING_INPUT, OperationState.FAILED)
    assert cancelled == (["op_pilot"] if ends else [])
    await service.close()


@pytest.mark.asyncio
async def test_the_choice_asked_alone_is_titled(relay_paths: RelayPaths) -> None:
    service, _ = _service(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    schema = ask.requested_schema
    assert schema is not None
    assert schema["properties"][held_services.FIELD]["title"] == held_services.TITLE
    await service.close()
