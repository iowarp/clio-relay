"""The operations service: asks, approval binding, submit-once, restart and cancel."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from clio_relay.adapters.hooks import Ask, ask_text
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobAttributes, JobSpec
from clio_relay.surface import interrupted
from clio_relay.surface.service import SUBMIT_GRACE, RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site, answer_page

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

ECHO = JobSpec(executable="echo", arguments=["hello-relay"])


async def test_open_site_runs_without_asking(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.QUEUED
    assert record.asks == []
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED, done
    view = service.view(done)
    assert view["exit_code"] == 0
    assert "hello-relay" in view["result"]["stdout_tail"]
    assert view["result"]["charge"]["node_hours"] is None
    await service.close()


@pytest.mark.usefixtures("stub_guard")
async def test_guarded_site_refuses_without_a_guard(relay_paths: RelayPaths) -> None:
    guard: Any = RelayService(relay_paths).guard
    assert guard.context.settings["config_dir"] == str(relay_paths.config_dir)
    add_site(relay_paths, policy="guarded")
    service = RelayService(relay_paths)
    with pytest.raises(Refusal) as refused:
        await service.start_run("fx", ECHO)
    assert refused.value.reason == RefusalReason.GUARD_UNAVAILABLE
    assert service.operations.ids() == []


async def test_unknown_site_refuses(relay_paths: RelayPaths) -> None:
    service = RelayService(relay_paths)
    with pytest.raises(Refusal) as refused:
        await service.start_run("nope", ECHO)
    assert refused.value.reason == RefusalReason.SITE_NOT_FOUND


async def test_spend_approval_then_submit(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.AWAITING_INPUT
    (ask,) = service.outstanding(record)
    assert ask.kind == "approve"
    assert ask.binding["spec_digest"] == record.spec_digest
    assert ask.binding["site"] == "fx"
    assert "unknown" in ask_text(ask)
    service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    advanced = await service.advance(record.operation_id)
    assert advanced.state == OperationState.QUEUED
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    await service.close()


async def test_threshold_only_asks_above_it(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend=10.0)
    service = RelayService(relay_paths)
    small = await service.start_run(
        "fx", ECHO.model_copy(update={"attributes": JobAttributes(duration=timedelta(minutes=10))})
    )
    assert small.state == OperationState.QUEUED
    big = JobSpec.model_validate(
        {
            "executable": "echo",
            "resources": {"node_count": 8},
            "attributes": {"duration": "PT4H"},
        }
    )
    asked = await service.start_run("fx", big)
    assert asked.state == OperationState.AWAITING_INPUT


@pytest.mark.parametrize("how", ["decline", "approve_false", "cancel"])
async def test_declined_approval_cancels(relay_paths: RelayPaths, how: str) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    if how == "decline":
        service.answer(ask.ask_id, "decline", by="test")
    elif how == "approve_false":
        service.answer(ask.ask_id, "accept", {"approve": False}, by="test")
    else:
        service.answer(ask.ask_id, "cancel", by="test")
    final = await service.advance(record.operation_id)
    assert final.state == OperationState.CANCELLED
    assert final.error is not None
    assert final.error["reason"] == RefusalReason.DECLINED.value
    assert final.handle is None


async def test_answer_is_recorded_once(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "accept", {"approve": True}, by="first")
    second = service.answer(ask.ask_id, "decline", by="second")
    assert second.state == "accepted"
    assert second.answered_by == "first"


async def test_tampered_binding_is_refused(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    with service.asks.locked(ask.ask_id):
        stored = service.asks.get(ask.ask_id)
        assert stored is not None
        stored.binding = {**stored.binding, "spec_digest": "0" * 64}
        service.asks.put(ask.ask_id, stored)
    final = await service.advance(record.operation_id)
    assert final.state == OperationState.FAILED
    assert final.error is not None
    assert final.error["reason"] == RefusalReason.APPROVAL_MISMATCH.value


async def test_submit_happens_once(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    calls: list[str] = []
    original = FixtureAdapter.run

    async def counted(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        calls.append(operation_id)
        await asyncio.sleep(0.2)
        return await original(self, spec, operation_id=operation_id)

    monkeypatch.setattr(FixtureAdapter, "run", counted)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    await asyncio.gather(*(service.advance(record.operation_id) for _ in range(5)))
    assert calls == [record.operation_id]


async def test_restart_observes_the_same_operation(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.3, "run_s": 0.5})
    first = RelayService(relay_paths)
    record = await first.start_run("fx", ECHO)
    await first.close()
    second = RelayService(relay_paths)
    done = await second.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    assert done.handle is not None and record.handle is not None
    assert done.handle.native_id == record.handle.native_id
    states = [change.state for change in done.history]
    assert states[0] == OperationState.AWAITING_INPUT
    assert states[-1] == OperationState.COMPLETED


async def test_interrupted_submit_is_reconciled(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    site = service.site("fx")
    adapter = service.adapter_named(site, record.adapter)
    await adapter.run(ECHO, operation_id=record.operation_id)
    with service.operations.locked(record.operation_id):
        stored = service.operation(record.operation_id)
        stored.move(OperationState.SUBMITTING)
        service.operations.put(record.operation_id, stored)
    reconciled = await service.refresh(record.operation_id)
    assert reconciled.state == OperationState.QUEUED
    assert reconciled.handle is not None


def _cut_short(service: RelayService, operation_id: str, restarts: int = 0) -> None:
    """Leave the record as a relay process that died mid-submit leaves it."""
    with service.operations.locked(operation_id):
        stored = service.operation(operation_id)
        stored.move(OperationState.SUBMITTING)
        stored.updated_at = stored.updated_at - SUBMIT_GRACE * 2
        if restarts:
            stored.result = {**(stored.result or {}), interrupted.RESTARTS_KEY: restarts}
        service.operations.put(operation_id, stored)


async def test_interrupted_submit_with_no_job_is_submitted_again(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
    _cut_short(service, record.operation_id)
    again = await service.refresh(record.operation_id)
    assert again.state == OperationState.QUEUED, again
    assert again.handle is not None
    assert any("submits it again" in (change.note or "") for change in again.history)
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    await service.close()


async def test_a_submit_cut_short_too_often_fails(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    _cut_short(service, record.operation_id, restarts=interrupted.MAX_RESTARTS)
    failed = await service.refresh(record.operation_id)
    assert failed.state == OperationState.FAILED
    assert "cut short" in (failed.error or {})["message"]
    assert failed.handle is None


async def test_a_live_submitter_is_left_alone(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    _cut_short(service, record.operation_id)
    with service.operations.submitting(record.operation_id):
        held = await service.refresh(record.operation_id)
    assert held.state == OperationState.SUBMITTING
    assert held.handle is None


async def test_cancel_before_submit(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    cancelled = await service.cancel(record.operation_id)
    assert cancelled.state == OperationState.CANCELLED
    assert all(ask.state == "cancelled" for ask in service.operation_asks(cancelled))
    assert (await service.advance(record.operation_id)).state == OperationState.CANCELLED


async def test_cancel_a_running_job(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", JobSpec(executable="sleep", arguments=["300"]))
    cancelled = await service.cancel(record.operation_id)
    assert cancelled.state == OperationState.CANCELLED


async def test_sign_in_page_completes_the_url_ask(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.2, "sign_in": True})
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    assert ask.kind == "url"
    assert ask.url is not None and ask.url.startswith("http://127.0.0.1:")
    assert await asyncio.to_thread(answer_page, ask.url) == 200
    stored: Ask | None = service.asks.get(ask.ask_id)
    assert stored is not None and stored.completed_at is not None
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    assert await asyncio.to_thread(answer_page_status, ask.url) == 404
    await service.close()


def answer_page_status(url: str) -> int:
    import httpx

    return httpx.get(url, timeout=10).status_code


async def test_a_sign_in_finished_elsewhere_closes_the_url_ask(relay_paths: RelayPaths) -> None:
    """A device-flow sign-in finishes at the provider; nobody answers relay's ask."""
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.2, "sign_in": True})
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    (ask,) = service.outstanding(record)
    assert ask.kind == "url"
    assert (await service.advance(record.operation_id)).state == OperationState.AWAITING_INPUT
    adapter = service.adapter_named(service.site("fx"), record.adapter)
    await adapter.sign_in_done({})
    await service.advance(record.operation_id)
    stored: Ask | None = service.asks.get(ask.ask_id)
    assert stored is not None and stored.state == "accepted"
    assert stored.completed_at is not None
    done = await service.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    await service.close()


async def test_exec_and_describe(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    add_site(relay_paths, "fx-sign", config={"queue_s": 0.1, "run_s": 0.2, "sign_in": True})
    add_site(relay_paths, "fx-gone", adapter="no-such-adapter")
    service = RelayService(relay_paths)
    listed = await service.describe()
    status = {site["id"]: site["status"] for site in listed["sites"]}
    assert status["fx"] == {"state": "ready"}
    assert status["fx-sign"]["state"] == "needs_sign_in"
    assert status["fx-gone"]["state"] == "unavailable"
    assert status["fx-gone"]["reason"]
    described = await service.describe("fx")
    assert described["site"]["id"] == "fx"
    result = await service.exec("fx", ["echo", "hi"], timeout_s=5, max_output_bytes=100)
    assert result["exit_code"] == 0
    assert "hi" in result["stdout"]


async def test_adapters_set_up_a_saved_site(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []

    async def on_site_added(self: FixtureAdapter, site: Any) -> dict[str, Any]:
        seen.append(site.id)
        if site.id == "fx-early":
            raise Refusal(RefusalReason.NEEDS_SIGN_IN, "sign in first")
        return {"registered": site.id}

    monkeypatch.setattr(FixtureAdapter, "on_site_added", on_site_added, raising=False)
    add_site(relay_paths)
    add_site(relay_paths, "fx-early")
    service = RelayService(relay_paths)
    assert await service.site_added("fx") == {"fixture": {"registered": "fx"}}
    early = await service.site_added("fx-early")
    assert early["fixture"]["refusal"]["reason"] == "needs_sign_in"
    assert seen == ["fx", "fx-early"]
    await service.close()
