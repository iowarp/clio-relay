"""The guard's package reviews become approvals, and its prepare step shapes what runs."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import GuardDecision, HookContext, set_hook
from clio_relay.jobs.handles import Endpoint, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.stubs import DefaultGuard

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

ECHO = JobSpec(executable="echo", arguments=["as-asked"])
REVIEW: dict[str, Any] = {
    "message": "Run hello 1.0 from the lab's package store?",
    "binding": {
        "kind": "package_review",
        "package": "hello",
        "version": "1.0",
        "package_hash": "sha256:abc",
        "source": "lab-store",
    },
}


class ReviewingGuard(DefaultGuard):
    """Asks for one package review and swaps the command for the reviewed package."""

    refuse_prepare = False

    def __init__(self, context: HookContext) -> None:
        super().__init__(context)
        self.prepared: list[tuple[str, JobSpec, str, bool]] = []

    async def check_run(self, site: SiteEntry, spec: JobSpec) -> GuardDecision:
        return GuardDecision(allowed=True, data={"asks": [REVIEW]})

    async def prepare(
        self,
        site: SiteEntry,
        spec: JobSpec,
        *,
        adapter: BaseAdapter[Any],
        service: bool = False,
    ) -> JobSpec:
        self.prepared.append((site.id, spec, adapter.name, service))
        if self.refuse_prepare:
            raise Refusal(RefusalReason.POLICY_REFUSED, "the package store is unreachable")
        return spec.model_copy(update={"arguments": ["from-the-package"]})


class StartedServe:
    """A service runtime that remembers the spec it was asked to start."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        self.started: list[ServiceSpec] = []

    def attach(self, server: Any, changes: Any) -> None:
        return None

    async def start(
        self, site: SiteEntry, adapter: BaseAdapter[Any], spec: ServiceSpec, record: OperationRecord
    ) -> Endpoint | None:
        self.started.append(spec)
        return None

    async def status(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        return JobStatus(state=JobState.RUNNING), None

    async def stop(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> JobStatus:
        return JobStatus(state=JobState.CANCELLED)

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        return None

    async def close(self) -> None:
        return None


@pytest.fixture
def guards() -> Iterator[list[ReviewingGuard]]:
    made: list[ReviewingGuard] = []

    def factory(context: HookContext) -> ReviewingGuard:
        made.append(ReviewingGuard(context))
        return made[-1]

    set_hook("guard", factory)
    yield made
    set_hook("guard", None)


async def _approve_all(service: RelayService, record: OperationRecord) -> OperationRecord:
    for ask in service.outstanding(record):
        service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    return await service.advance(record.operation_id)


async def test_a_package_review_is_an_approval_bound_to_the_spec(
    relay_paths: RelayPaths, guards: list[ReviewingGuard]
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.AWAITING_INPUT
    (ask,) = service.outstanding(record)
    assert ask.kind == "approve"
    assert ask.message == REVIEW["message"]
    assert ask.binding == {
        **REVIEW["binding"],
        "site": "fx",
        "spec_digest": record.spec_digest,
    }
    assert guards[-1].prepared == []
    advanced = await _approve_all(service, record)
    assert advanced.state == OperationState.QUEUED
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED, done
    assert "from-the-package" in service.view(done)["result"]["stdout_tail"]
    assert done.spec == ECHO.model_dump(mode="json")
    assert done.spec_digest == record.spec_digest
    assert guards[-1].prepared == [("fx", ECHO, record.adapter, False)]
    await service.close()


async def test_a_declined_review_never_submits(
    relay_paths: RelayPaths, guards: list[ReviewingGuard]
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "decline", by="test")
    final = await service.advance(record.operation_id)
    assert final.state == OperationState.CANCELLED
    assert final.error is not None
    assert final.error["reason"] == RefusalReason.DECLINED.value
    assert final.handle is None
    assert guards[-1].prepared == []
    await service.close()


async def test_a_refused_prepare_fails_the_operation(
    relay_paths: RelayPaths, guards: list[ReviewingGuard]
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    guards[-1].refuse_prepare = True
    record = await service.start_run("fx", ECHO)
    failed = await _approve_all(service, record)
    assert failed.state == OperationState.FAILED
    assert failed.error is not None
    assert failed.error["reason"] == RefusalReason.POLICY_REFUSED.value
    assert "package store is unreachable" in failed.error["message"]
    assert failed.handle is None
    await service.close()


async def test_a_service_starts_the_prepared_job(
    relay_paths: RelayPaths, guards: list[ReviewingGuard]
) -> None:
    started: list[StartedServe] = []

    def serve_factory(context: HookContext) -> StartedServe:
        started.append(StartedServe(context))
        return started[-1]

    set_hook("serve", serve_factory)
    try:
        add_site(relay_paths, "lab", adapter="local", config={"scheduler": "none"})
        service = RelayService(relay_paths)
        spec = ServiceSpec.model_validate(
            {"job": {"executable": "pvserver"}, "ports": [{"name": "viz", "port": 1}]}
        )
        record = await service.start_serve("lab", spec)
        await _approve_all(service, record)
        (service_spec,) = started[-1].started
        assert service_spec.job.arguments == ["from-the-package"]
        assert service_spec.ports == spec.ports
        ((_, _, _, as_service),) = guards[-1].prepared
        assert as_service is True
        assert service.operation(record.operation_id).spec == spec.model_dump(mode="json")
        await service.close()
    finally:
        set_hook("serve", None)


async def test_adapters_run_the_agents_command_unless_they_say_otherwise() -> None:
    assert BaseAdapter.runs_packages is False
