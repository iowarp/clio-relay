"""How relay keeps watching a service: its latest word, failed looks and restarts (W9)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from fastmcp import Client
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)

from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import HookContext, set_hook
from clio_relay.jobs.handles import Endpoint, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.spec import ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")

SPEC = ServiceSpec.model_validate(
    {"job": {"executable": "sleep", "arguments": ["300"]}, "ports": [{"name": "viz", "port": 1}]}
)


class ScriptedServe:
    """A service runtime whose next look returns, or raises, whatever the test sets."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        self.next: JobStatus | Exception = JobStatus(
            state=JobState.RUNNING, message="frpc: the rendezvous refused the certificate"
        )
        self.looks = 0

    def attach(self, server: Any, changes: Any) -> None:
        return None

    async def start(
        self, site: SiteEntry, adapter: BaseAdapter[Any], spec: ServiceSpec, record: OperationRecord
    ) -> Endpoint | None:
        return None

    async def status(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        self.looks += 1
        if isinstance(self.next, Exception):
            raise self.next
        return self.next, None

    async def stop(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> JobStatus:
        return JobStatus(state=JobState.CANCELLED)

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        return None

    async def close(self) -> None:
        return None


@pytest.fixture
def scripted() -> Iterator[list[ScriptedServe]]:
    made: list[ScriptedServe] = []

    def factory(context: HookContext) -> ScriptedServe:
        made.append(ScriptedServe(context))
        return made[-1]

    set_hook("serve", factory)
    yield made
    set_hook("serve", None)


def _lab(paths: RelayPaths) -> None:
    add_site(paths, "lab", adapter="local", config={"scheduler": "none"})


def test_a_task_shows_why_a_service_is_not_ready(
    relay_paths: RelayPaths, scripted: list[ScriptedServe]
) -> None:
    _lab(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(
                client, "serve", {"site": "lab", "spec": SPEC.model_dump(mode="json")}
            )
            message = ""
            for _ in range(100):
                message = (await task.status()).status_message or ""
                if "certificate" in message:
                    break
                await asyncio.sleep(0.1)
            assert message == "the service is running: frpc: the rendezvous refused the certificate"
            await client.call_tool("serve", {"stop": task.task_id})

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_a_failed_look_keeps_the_service_open_and_says_why(
    relay_paths: RelayPaths, scripted: list[ScriptedServe]
) -> None:
    _lab(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_serve("lab", SPEC)
    scripted[-1].next = RuntimeError("the job store is unreadable")
    seen = await service.refresh(record.operation_id)
    assert not seen.state.final
    assert (seen.result or {})["message"] == (
        "relay could not observe the service: RuntimeError: the job store is unreadable"
    )
    await service.close()


@pytest.mark.asyncio
async def test_open_services_are_observed_once_at_startup(
    relay_paths: RelayPaths, scripted: list[ScriptedServe]
) -> None:
    _lab(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_serve("lab", SPEC)
    assert not service.operation(record.operation_id).state.final
    await service.close()

    after = RelayService(relay_paths)
    scripted[-1].next = JobStatus(state=JobState.FAILED, exit_code=1, message="the job ended")
    await after.observe_open_services()
    assert after.operation(record.operation_id).state == OperationState.FAILED
    await after.close()


def test_the_server_observes_open_services_when_it_starts(
    relay_paths: RelayPaths, scripted: list[ScriptedServe]
) -> None:
    _lab(relay_paths)

    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_serve("lab", SPEC)
        await service.close()
        server = create_server(relay_paths)
        scripted[-1].next = JobStatus(state=JobState.COMPLETED, exit_code=0)
        records = service.operations
        async with Client(server, mode="legacy"):
            for _ in range(100):
                seen = records.get(record.operation_id)
                if seen is not None and seen.state.final:
                    break
                await asyncio.sleep(0.05)
        seen = records.get(record.operation_id)
        assert seen is not None and seen.state == OperationState.COMPLETED

    asyncio.run(scenario())
