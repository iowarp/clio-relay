"""Tests for the generic reserved remote-MCP control-query lane."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event

import pytest
from pydantic import ValidationError

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
    McpOperation,
    RelayJob,
)
from clio_relay.storage_runtime import storage_managed_queue


def test_control_query_model_is_fail_closed_but_allows_discovery() -> None:
    """Only artifact-bound calls or contract discovery can enter control capacity."""
    assert McpCallSpec(server="science", tool="inspect").admission_class is (
        McpAdmissionClass.WORKLOAD
    )

    with pytest.raises(ValidationError, match="expected server artifact digest"):
        McpCallSpec(
            server="science",
            tool="inspect",
            admission_class=McpAdmissionClass.CONTROL_QUERY,
        )

    control_call = McpCallSpec(
        server="science",
        expected_server_artifact_digest="a" * 64,
        tool="inspect",
        admission_class=McpAdmissionClass.CONTROL_QUERY,
    )
    discovery = McpCallSpec(
        server="science",
        operation=McpOperation.TOOLS_LIST,
        admission_class=McpAdmissionClass.CONTROL_QUERY,
    )

    assert control_call.admission_class is McpAdmissionClass.CONTROL_QUERY
    assert discovery.admission_class is McpAdmissionClass.CONTROL_QUERY
    with pytest.raises(ValidationError):
        McpCallSpec.model_validate(
            {
                "server": "science",
                "tool": "inspect",
                "admission_class": "reserved_by_caller",
            }
        )


def test_queue_lanes_are_strict_and_reject_endpoint_lane_changes(tmp_path: Path) -> None:
    """Control slots never consume workload or kind/spec-mismatched records."""
    queue = ClioCoreQueue(tmp_path / "core")
    mismatched = queue.submit_job(
        RelayJob(
            cluster="alpha",
            kind=JobKind.JARVIS,
            spec=_control_spec("mismatched"),
            idempotency_key="mismatched-control-spec",
        )
    )
    workload = queue.submit_job(_mcp_job("workload", McpAdmissionClass.WORKLOAD))
    control = queue.submit_job(_mcp_job("control", McpAdmissionClass.CONTROL_QUERY))
    discovery = queue.submit_job(
        RelayJob(
            cluster="alpha",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="science",
                operation=McpOperation.TOOLS_LIST,
                admission_class=McpAdmissionClass.CONTROL_QUERY,
            ),
            idempotency_key="control-discovery",
        )
    )

    control_lease = queue.acquire_next_job(
        "control-worker",
        cluster="alpha",
        mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
        mcp_admission_limit=1,
    )
    workload_lease = queue.acquire_next_job(
        "workload-worker",
        cluster="alpha",
        mcp_admission_class=McpAdmissionClass.WORKLOAD,
    )

    assert control_lease is not None and control_lease.job_id == control.job_id
    assert workload_lease is not None and workload_lease.job_id == mismatched.job_id
    assert queue.get_job(workload.job_id).state is JobState.QUEUED
    with pytest.raises(QueueConflictError, match="does not match"):
        queue.acquire_next_job(
            "workload-worker",
            cluster="alpha",
            mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
        )
    queue.release_lease(workload_lease.lease_id)
    next_workload_lease = queue.acquire_next_job(
        "next-workload-worker",
        cluster="alpha",
        mcp_admission_class=McpAdmissionClass.WORKLOAD,
    )
    assert next_workload_lease is not None
    assert next_workload_lease.job_id == workload.job_id
    queue.release_lease(next_workload_lease.lease_id)
    assert (
        queue.acquire_next_job(
            "empty-workload-worker",
            cluster="alpha",
            mcp_admission_class=McpAdmissionClass.WORKLOAD,
        )
        is None
    )
    queue.release_lease(control_lease.lease_id)
    discovery_lease = queue.acquire_next_job(
        "discovery-control-worker",
        cluster="alpha",
        mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
        mcp_admission_limit=1,
    )
    assert discovery_lease is not None
    assert discovery_lease.job_id == discovery.job_id


def test_control_query_limit_is_atomic_across_queue_instances(tmp_path: Path) -> None:
    """Independent worker slots cannot race past the durable control cap."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_mcp_job("control-one", McpAdmissionClass.CONTROL_QUERY))
    queue.submit_job(_mcp_job("control-two", McpAdmissionClass.CONTROL_QUERY))
    barrier = Barrier(2)

    def acquire(index: int) -> str | None:
        instance = ClioCoreQueue(root)
        barrier.wait()
        lease = instance.acquire_next_job(
            f"control-worker-{index}",
            cluster="alpha",
            mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
            mcp_admission_limit=1,
        )
        return None if lease is None else lease.job_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(acquire, (1, 2)))

    assert sum(result is not None for result in results) == 1
    assert len(queue.list_leases(cluster="alpha")) == 1


def test_workload_mcp_kind_cap_cannot_consume_reserved_control_capacity(
    tmp_path: Path,
) -> None:
    """A workload MCP ceiling remains independent from the reserved query lane."""
    queue = ClioCoreQueue(tmp_path / "core")
    workload = queue.submit_job(_mcp_job("workload", McpAdmissionClass.WORKLOAD))
    control = queue.submit_job(_mcp_job("control", McpAdmissionClass.CONTROL_QUERY))
    kind_limit = {JobKind.MCP_CALL: 1}

    workload_lease = queue.acquire_next_job(
        "workload-worker",
        cluster="alpha",
        kind_concurrency=kind_limit,
        mcp_admission_class=McpAdmissionClass.WORKLOAD,
    )
    control_lease = queue.acquire_next_job(
        "control-worker",
        cluster="alpha",
        kind_concurrency=kind_limit,
        mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
        mcp_admission_limit=1,
    )

    assert workload_lease is not None and workload_lease.job_id == workload.job_id
    assert control_lease is not None and control_lease.job_id == control.job_id
    assert len(queue.list_leases(cluster="alpha")) == 2


def test_storage_managed_queue_preserves_strict_lane_filter(tmp_path: Path) -> None:
    """The production storage wrapper leases from the same reserved lane contract."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=50,
        spool_max_log_bytes_per_job=100,
        storage_minimum_free_bytes=0,
        storage_max_job_reservation_bytes=1_000,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
    )
    queue = storage_managed_queue(settings)
    try:
        workload = queue.submit_job(_mcp_job("managed-workload", McpAdmissionClass.WORKLOAD))
        control = queue.submit_job(_mcp_job("managed-control", McpAdmissionClass.CONTROL_QUERY))

        lease = queue.acquire_next_job(
            "managed-control-worker",
            cluster="alpha",
            mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
            mcp_admission_limit=1,
        )

        assert lease is not None and lease.job_id == control.job_id
        assert queue.get_job(workload.job_id).state is JobState.QUEUED
    finally:
        queue.close()


def test_storage_managed_workload_cap_preserves_reserved_control_capacity(
    tmp_path: Path,
) -> None:
    """Production storage admission keeps workload and control MCP caps disjoint."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=50,
        spool_max_log_bytes_per_job=100,
        storage_minimum_free_bytes=0,
        storage_max_job_reservation_bytes=1_000,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
    )
    queue = storage_managed_queue(settings)
    try:
        workload = queue.submit_job(_mcp_job("managed-workload", McpAdmissionClass.WORKLOAD))
        control = queue.submit_job(_mcp_job("managed-control", McpAdmissionClass.CONTROL_QUERY))
        kind_limit = {JobKind.MCP_CALL: 1}

        workload_lease = queue.acquire_next_job(
            "managed-workload-worker",
            cluster="alpha",
            kind_concurrency=kind_limit,
            mcp_admission_class=McpAdmissionClass.WORKLOAD,
        )
        control_lease = queue.acquire_next_job(
            "managed-control-worker",
            cluster="alpha",
            kind_concurrency=kind_limit,
            mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
            mcp_admission_limit=1,
        )

        assert workload_lease is not None and workload_lease.job_id == workload.job_id
        assert control_lease is not None and control_lease.job_id == control.job_id
        assert len(queue.list_leases(cluster="alpha")) == 2
    finally:
        queue.close()


def test_reserved_query_finishes_while_workload_keeps_lease_and_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A query overlaps a live workload without canceling or stealing ownership."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    source = queue.submit_job(
        RelayJob(
            cluster="alpha",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["long-science-run"]),
            idempotency_key="long-science-run",
        )
    )
    query = queue.submit_job(_mcp_job("live-status", McpAdmissionClass.CONTROL_QUERY))
    provider = _BlockingLaneProvider()
    workload_worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="alpha",
        queue=queue,
        provider=provider,
    )
    control_worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="alpha",
        queue=queue,
        provider=provider,
    )
    workload_worker.lease_renew_seconds = 0
    control_worker.lease_renew_seconds = 0

    def forbidden_reconciliation() -> None:
        raise AssertionError("a control-query slot must not reconcile workload cancellation")

    monkeypatch.setattr(
        control_worker,
        "_reconcile_canceled_scheduler_jobs",
        forbidden_reconciliation,
    )
    monkeypatch.setattr(
        control_worker,
        "_reconcile_pending_execution_cleanup",
        forbidden_reconciliation,
    )
    workload_worker.register()
    control_worker.register()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            source_future = executor.submit(workload_worker.run_once)
            assert provider.source_started.wait(timeout=10)
            source_lease = _lease_for_job(queue, source.job_id)
            assert source_lease is not None
            assert queue.get_job(source.job_id).state is JobState.RUNNING

            query_result = control_worker.run_once(
                mcp_admission_class=McpAdmissionClass.CONTROL_QUERY,
                mcp_admission_limit=1,
            )

            assert query_result is not None
            assert query_result.job_id == query.job_id
            assert query_result.state is JobState.SUCCEEDED
            assert provider.query_started.is_set()
            live_source = queue.get_job(source.job_id)
            renewed_source_lease = _lease_for_job(queue, source.job_id)
            assert live_source.state is JobState.RUNNING
            assert live_source.leased_by == source_lease.endpoint_id
            assert "cancellation_request" not in live_source.metadata
            assert renewed_source_lease is not None
            assert renewed_source_lease.lease_id == source_lease.lease_id
            assert renewed_source_lease.expires_at >= source_lease.expires_at
            assert _lease_for_job(queue, query.job_id) is None

            source_events, _ = queue.read_event_page(source.job_id, limit=100)
            assert all("cancel" not in event.event_type for event in source_events)
            provider.release_source.set()
            source_result = source_future.result(timeout=10)

        assert source_result is not None
        assert source_result.state is JobState.SUCCEEDED
    finally:
        provider.release_source.set()
        workload_worker.close()
        control_worker.close()


def test_supervised_slots_publish_total_and_disjoint_lane_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parent and child records expose total capacity and each slot's exact lane."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    parent = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="alpha",
        concurrency=3,
        control_query_concurrency=1,
        queue=queue,
    )
    parent_endpoint = parent.register()
    observed: list[tuple[dict[str, object], dict[str, object]]] = []

    class StopSlot(Exception):
        """Stop one synthetic supervised slot after its first dispatch attempt."""

    def stop_slot(self: EndpointWorker, **kwargs: object) -> None:
        assert self.endpoint is not None
        observed.append((dict(self.endpoint.metadata), kwargs))
        self.close()
        raise StopSlot

    monkeypatch.setattr(EndpointWorker, "run_once", stop_slot)
    try:
        with pytest.raises(StopSlot):
            parent._serve_worker_slot(  # pyright: ignore[reportPrivateUsage]
                0,
                0,
                McpAdmissionClass.WORKLOAD,
            )
        with pytest.raises(StopSlot):
            parent._serve_worker_slot(  # pyright: ignore[reportPrivateUsage]
                2,
                0,
                McpAdmissionClass.CONTROL_QUERY,
            )
    finally:
        parent.close()

    assert parent_endpoint.metadata["concurrency"] == 3
    assert parent_endpoint.metadata["workload_concurrency"] == 2
    assert parent_endpoint.metadata["control_query_concurrency"] == 1
    assert parent_endpoint.metadata["worker_supervisor"] is True
    workload_metadata, workload_call = observed[0]
    control_metadata, control_call = observed[1]
    assert workload_metadata["parent_endpoint_id"] == parent_endpoint.endpoint_id
    assert workload_metadata["mcp_admission_class"] == "workload"
    assert workload_metadata["workload_concurrency"] == 1
    assert workload_metadata["control_query_concurrency"] == 0
    assert workload_call == {
        "mcp_admission_class": McpAdmissionClass.WORKLOAD,
        "mcp_admission_limit": None,
    }
    assert control_metadata["parent_endpoint_id"] == parent_endpoint.endpoint_id
    assert control_metadata["mcp_admission_class"] == "control_query"
    assert control_metadata["workload_concurrency"] == 0
    assert control_metadata["control_query_concurrency"] == 1
    assert control_call == {
        "mcp_admission_class": McpAdmissionClass.CONTROL_QUERY,
        "mcp_admission_limit": 1,
    }


@pytest.mark.parametrize(
    ("concurrency", "control_query_concurrency"),
    [(True, 0), (1, 1), (3, -1), (3, True)],
)
def test_endpoint_rejects_invalid_lane_capacity(
    tmp_path: Path,
    concurrency: int,
    control_query_concurrency: int,
) -> None:
    """Runtime construction retains at least one valid workload slot."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    with pytest.raises(ConfigurationError):
        EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="alpha",
            concurrency=concurrency,
            control_query_concurrency=control_query_concurrency,
        )


def _mcp_job(key: str, admission_class: McpAdmissionClass) -> RelayJob:
    """Build one generic remote-MCP job for a strict worker lane."""
    return RelayJob(
        cluster="alpha",
        kind=JobKind.MCP_CALL,
        spec=(
            McpCallSpec(server="science", tool=key)
            if admission_class is McpAdmissionClass.WORKLOAD
            else _control_spec(key)
        ),
        idempotency_key=key,
    )


def _control_spec(tool: str) -> McpCallSpec:
    """Build one artifact-bound, non-destructive control call specification."""
    return McpCallSpec(
        server="science",
        expected_server_artifact_digest="a" * 64,
        tool=tool,
        admission_class=McpAdmissionClass.CONTROL_QUERY,
    )


def _lease_for_job(queue: ClioCoreQueue, job_id: str) -> Lease | None:
    """Return the one active lease for a job, if present."""
    return next(
        (lease for lease in queue.list_leases(cluster="alpha") if lease.job_id == job_id),
        None,
    )


class _BlockingLaneProvider(JarvisCdProvider):
    """Keep a workload live while allowing one remote-MCP query to finish."""

    def __init__(self) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.source_started = Event()
        self.query_started = Event()
        self.release_source = Event()

    def run_command_streaming(
        self,
        command: list[str],
        *,
        process_label: str = "contained command",
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        credential_payload: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Complete one endpoint-owned MCP query without invoking a real server."""
        del (
            process_label,
            cwd,
            env,
            credential_payload,
            on_stdout,
            on_stderr,
            should_cancel,
            timeout_seconds,
            on_timeout,
        )
        if on_start is not None:
            on_start(1002)
        self.query_started.set()
        if on_poll is not None:
            on_poll()
        return subprocess.CompletedProcess(command, 0, "", "")

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, env, on_stdout, on_stderr, timeout_seconds, on_timeout
        document = pipeline_path.read_text(encoding="utf-8")
        if on_start is not None:
            on_start(1001)
        if "clio_relay.mcp_call" in document:
            self.query_started.set()
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

        self.source_started.set()
        deadline = time.monotonic() + 10
        while not self.release_source.wait(0.01):
            if should_cancel is not None and should_cancel():
                return subprocess.CompletedProcess(["jarvis"], -15, "", "")
            if time.monotonic() >= deadline:
                return subprocess.CompletedProcess(["jarvis"], 124, "", "timed out")
            if on_poll is not None:
                on_poll()
        return subprocess.CompletedProcess(["jarvis"], 0, "", "")
