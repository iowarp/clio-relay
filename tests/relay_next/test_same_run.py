"""A job submitted again while the same job is live: its approval names the live one."""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay.jobs import declines
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface import same_run
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

ECHO = JobSpec(executable="echo", arguments=["hi"])
YES = {"approve": "approve"}


async def _approved(service: RelayService, job: JobSpec, content: dict[str, Any]) -> Any:
    record = await service.start_run("fx", job)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "accept", content, by="test")
    return ask, await service.advance(record.operation_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", [same_run.USE, same_run.ANOTHER])
async def test_the_person_uses_the_live_job_or_starts_another(
    relay_paths: RelayPaths, choice: str
) -> None:
    SiteStore(relay_paths.sites_file).add(
        SiteEntry.model_validate(
            {
                "id": "fx",
                "adapter": "fixture",
                "config": {"queue_s": 0.1, "run_s": 60},
                "policy": "open",
                "spend": {"ask": "always"},
            }
        )
    )
    service = RelayService(relay_paths)
    try:
        first_ask, first = await _approved(service, ECHO, YES)
        assert first.state in same_run.LIVE
        schema: dict[str, Any] = first_ask.requested_schema or {}
        assert same_run.FIELD not in schema.get("properties", {})

        other = await service.start_run("fx", JobSpec(executable="echo", arguments=["other"]))
        assert same_run.KEY not in (other.result or {})

        record = await service.start_run("fx", ECHO)
        (ask,) = service.outstanding(record)
        assert ask.kind == "approve"
        fact = (ask.meta or {})[same_run.META]
        assert fact["operation_id"] == first.operation_id
        assert fact["state"] in {state.value for state in same_run.LIVE}
        field = (ask.requested_schema or {})["properties"][same_run.FIELD]
        assert field["title"] == same_run.TITLE and first.operation_id in field["description"]

        service.answer(ask.ask_id, "accept", {**YES, same_run.FIELD: choice}, by="test")
        record = await service.advance(record.operation_id)
        if choice == same_run.USE:
            assert record.state == OperationState.CANCELLED
            outcome = declines.outcome(record)
            assert outcome is not None
            assert outcome["use_operation_id"] == first.operation_id
            assert first.operation_id in outcome["not_done"]
        else:
            assert record.state in same_run.LIVE
    finally:
        await service.close()
