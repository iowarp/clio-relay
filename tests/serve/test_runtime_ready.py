"""A served service is reported up as soon as it is ready, while its job runs on."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.serve import runtime as runtime_module
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.runtime import ServeRuntime

from .helpers import frp_plan


def runtime_with(tmp_path: Path, plan: ServePlan) -> ServeRuntime:
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    return runtime


def test_a_task_poll_answers_with_the_followers_look_instead_of_waiting_for_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A poll never queues behind the follower's round, so readiness is seen at once."""
    monkeypatch.setattr(runtime_module, "FOLLOW_FIRST_S", 0.0)
    handle = JobHandle(adapter="fixture", native_id="1", data={"job_dir": "/j"})
    plan = frp_plan(tmp_path).model_copy(update={"job_dir": "/j", "handle": handle})
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    runtime._resumed = True  # pyright: ignore[reportPrivateUsage]
    record = OperationRecord(
        operation_id=plan.operation_id,
        kind="serve",
        site="fx",
        adapter="fixture",
        spec={},
        spec_digest="e" * 64,
        state=OperationState.RUNNING,
    )
    inside = asyncio.Event()
    release = asyncio.Event()
    served = plan.endpoint(note="serving", connect_command=None)

    async def bring_up(_plan: ServePlan, _adapter: Any, status: JobStatus) -> Any:
        inside.set()
        await release.wait()
        plan.ready_at = runtime_module.utc_now()
        return status.model_copy(update={"message": "serving"}), served

    class Adapter:
        async def status(self, _handle: JobHandle) -> JobStatus:
            return JobStatus(state=JobState.RUNNING)

    async def scenario() -> None:
        runtime._follow(  # pyright: ignore[reportPrivateUsage]
            plan, Adapter(), handle, JobStatus(state=JobState.RUNNING, message="starting: x")
        )
        runtime._running = bring_up  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
        await inside.wait()
        seen, endpoint = await asyncio.wait_for(runtime.status(None, Adapter(), record), 1)
        assert endpoint is None and seen.message == "starting: x"
        release.set()
        await runtime._followers[plan.operation_id]  # pyright: ignore[reportPrivateUsage]
        _, endpoint = await asyncio.wait_for(runtime.status(None, Adapter(), record), 1)
        assert endpoint == served

    asyncio.run(scenario())


def test_an_ssh_package_service_is_up_when_jarvis_saw_it_ready(tmp_path: Path) -> None:
    """Relay forwards to the node JARVIS names, without the connector's ready file."""
    handle = JobHandle(adapter="fixture", native_id="1", data={"job_dir": "/j"})
    plan = frp_plan(tmp_path).model_copy(
        update={"job_dir": "/j", "handle": handle, "mode": "ssh", "package": True}
    )
    plan.ports[0].remote_port = 0
    runtime = runtime_with(tmp_path, plan)
    reads: list[str] = []
    document = {
        "service_endpoints": [
            {"name": "web", "source": "progress_ready", "port": 48385, "host": "ares-comp-03"}
        ]
    }

    class Files:
        async def write(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class Adapter:
        reads_service_runtimes = True

        async def service_runtimes(self, _handle: JobHandle) -> dict[str, Any]:
            return document

        async def files(self) -> Files:
            return Files()

    async def read(_plan: ServePlan, _adapter: Any, name: str, *, tail: int = 0) -> bytes | None:
        del tail
        reads.append(name)
        return None

    async def host_keys(_adapter: Any, _plan: ServePlan) -> list[str]:
        return []

    class Bound:
        def bound(self) -> bool:
            return False

        def last_error(self) -> str | None:
            return None

    async def ensure_forward(_plan: ServePlan, _adapter: Any) -> Any:
        return Bound()

    runtime._read = read  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    runtime._host_keys = host_keys  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    runtime._ensure_forward = ensure_forward  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    status, _ = asyncio.run(
        runtime._running(plan, Adapter(), JobStatus(state=JobState.RUNNING))  # pyright: ignore[reportPrivateUsage]
    )
    assert "ready.json" not in reads
    assert (plan.host, plan.ports[0].remote_port) == ("ares-comp-03", 48385)
    assert plan.ready_at is not None
    assert status.message is not None and status.message.startswith("the service is up")
