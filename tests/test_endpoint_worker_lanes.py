"""Tests for the worker-slot lane resilience fix (clio-relay#238).

Two layers:

1. Fast, isolated unit tests of :mod:`clio_relay.adapters.worker.endpoint_worker_lanes`
   itself (``run_worker_lane_iteration``) against a small recording fake --
   no queue, no worker, no subprocess.
2. A process-level test that starts the real ``clio-relay worker endpoint start
   --role worker`` CLI against a temp core (issue #238's acceptance item
   1): daemon-mode registers every slot and keeps heartbeating, and an
   admitted ``remote_agent`` job reaches ``attempts == 1``,
   ``leased_by == <endpoint>``, and a terminal state.

Before the fix (no exception handling at all around ``_serve_worker_slot``'s
per-slot loop), the daemon-mode process would never bring a slot up to a
stable, heartbeating registration.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import cast

import pytest

from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.endpoint_worker_lanes import (
    LANE_LAST_ERROR_METADATA_KEY,
    WORKER_LANE_ITERATION_FAILED,
    run_worker_lane_iteration,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    RelayJob,
    RemoteAgentTaskSpec,
)

# ---------------------------------------------------------------------------
# Layer 1: isolated unit tests against small recording fakes.
# ---------------------------------------------------------------------------


class _RecordingRegistrationQueue:
    """Minimal ``register_endpoint`` recorder satisfying ``WorkerLaneRegistrationQueue``."""

    def __init__(self) -> None:
        self.registrations: list[EndpointRegistration] = []

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration:
        self.registrations.append(endpoint)
        return endpoint


def _endpoint(**metadata: object) -> EndpointRegistration:
    return EndpointRegistration(
        role=EndpointRole.WORKER,
        cluster="ares",
        hostname="test-host",
        pid=1234,
        metadata=dict(metadata),
    )


def test_run_worker_lane_iteration_success_is_a_pure_passthrough() -> None:
    queue = _RecordingRegistrationQueue()
    endpoint = _endpoint(worker_slot=0)
    calls: list[int] = []

    result = run_worker_lane_iteration(
        lambda: calls.append(1),
        queue=queue,
        endpoint=endpoint,
    )

    assert calls == [1]
    assert result is endpoint
    assert queue.registrations == []


def test_run_worker_lane_iteration_contains_failure_and_records_typed_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    queue = _RecordingRegistrationQueue()
    endpoint = _endpoint(worker_slot=0)

    def _iterate() -> None:
        raise ConfigurationError("JARVIS-CD executable not found: jarvis")

    with caplog.at_level("ERROR", logger="clio_relay.adapters.worker.endpoint_worker_lanes"):
        result = run_worker_lane_iteration(_iterate, queue=queue, endpoint=endpoint)

    # The iteration's exception never escapes -- this is the whole point:
    # the caller (_serve_worker_slot's while loop) keeps running.
    assert len(queue.registrations) == 1
    failure = cast(dict[str, object], result.metadata[LANE_LAST_ERROR_METADATA_KEY])
    assert failure["reason"] == WORKER_LANE_ITERATION_FAILED
    assert failure["endpoint_id"] == endpoint.endpoint_id
    assert "JARVIS-CD executable not found" in cast(str, failure["detail"])
    assert any("worker lane iteration failed" in record.message for record in caplog.records)


def test_run_worker_lane_iteration_clears_lane_last_error_once_healthy_again() -> None:
    queue = _RecordingRegistrationQueue()
    failing_endpoint = _endpoint(worker_slot=0)

    def _fails() -> None:
        raise RelayError("transient")

    after_failure = run_worker_lane_iteration(_fails, queue=queue, endpoint=failing_endpoint)
    assert LANE_LAST_ERROR_METADATA_KEY in after_failure.metadata

    after_recovery = run_worker_lane_iteration(
        lambda: None,
        queue=queue,
        endpoint=after_failure,
    )

    assert LANE_LAST_ERROR_METADATA_KEY not in after_recovery.metadata
    assert len(queue.registrations) == 2


def _submit_remote_agent_job(
    queue: ClioCoreQueue, *, cluster: str, idempotency_key: str
) -> RelayJob:
    return queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="prompt.md"),
            idempotency_key=idempotency_key,
        )
    )


# ---------------------------------------------------------------------------
# Layer 2: process-level acceptance (issue #238's own acceptance item 1).
# ---------------------------------------------------------------------------

_BRINGUP_FLAGS = [
    "--concurrency",
    "2",
    "--kind-concurrency",
    "remote_agent=2",
    "--kind-concurrency",
    "jarvis=2",
]

_DAEMON_STARTUP_TIMEOUT_SECONDS = 30.0
_HEARTBEAT_POLL_SECONDS = 0.5
_JOB_TERMINAL_TIMEOUT_SECONDS = 30.0


def _spawn_daemon_worker(
    *,
    cluster: str,
    core_dir: Path,
    spool_dir: Path,
    stdout_log: Path,
    stderr_log: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    env = dict(os.environ)
    env["CLIO_RELAY_CORE_DIR"] = str(core_dir)
    env["CLIO_RELAY_SPOOL_DIR"] = str(spool_dir)
    if extra_env:
        env.update(extra_env)
    cmd = [
        sys.executable,
        "-c",
        "from clio_relay.adapters.worker.cli import worker_app; worker_app()",
        "endpoint",
        "start",
        "--role",
        "worker",
        "--cluster",
        cluster,
        "--scheduler-provider",
        "external",
        *_BRINGUP_FLAGS,
    ]
    with stdout_log.open("wb") as stdout_handle, stderr_log.open("wb") as stderr_handle:
        return subprocess.Popen(  # noqa: SIM115 - caller owns lifetime, terminated in `finally`
            cmd,
            stdout=stdout_handle,
            stderr=stderr_handle,
            env=env,
        )


def _wait_for_worker_slots(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    timeout_seconds: float,
) -> dict[int, EndpointRegistration]:
    """Poll until both worker slots are registered."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        slots = {
            cast(int, endpoint.metadata["worker_slot"]): endpoint
            for endpoint in queue.list_endpoints(cluster=cluster)
            if endpoint.metadata.get("worker_slot") is not None
        }
        if {0, 1} <= slots.keys():
            return slots
        time.sleep(_HEARTBEAT_POLL_SECONDS)
    pytest.fail(
        "daemon worker did not register both worker slots "
        f"within {timeout_seconds:g}s (clio-relay#238 regression)"
    )


def _wait_for_job_terminal(
    queue: ClioCoreQueue,
    *,
    job_id: str,
    timeout_seconds: float,
) -> RelayJob:
    deadline = time.monotonic() + timeout_seconds
    last_seen: RelayJob | None = None
    while time.monotonic() < deadline:
        last_seen = queue.get_job(job_id)
        if last_seen.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}:
            return last_seen
        time.sleep(_HEARTBEAT_POLL_SECONDS)
    pytest.fail(
        f"job {job_id} did not reach a terminal state within {timeout_seconds:g}s "
        f"(last observed state: {last_seen.state if last_seen else 'unknown'})"
    )


def _terminate(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def test_daemon_worker_registers_all_slots_and_remote_agent_job_reaches_terminal(
    tmp_path: Path,
) -> None:
    """clio-relay#238 acceptance item 1: a real daemon-mode worker started
    with the bring-up flag set registers every worker slot and keeps
    heartbeating; an admitted remote_agent job reaches
    attempts==1, leased_by==<endpoint>, and a terminal state.
    """
    cluster = "test-238-registration"
    core_dir = tmp_path / "core"
    spool_dir = tmp_path / "spool"
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    process = _spawn_daemon_worker(
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        stdout_log=logs_dir / "worker.out.log",
        stderr_log=logs_dir / "worker.err.log",
    )
    try:
        queue = ClioCoreQueue(core_dir)
        slots = _wait_for_worker_slots(
            queue,
            cluster=cluster,
            timeout_seconds=_DAEMON_STARTUP_TIMEOUT_SECONDS,
        )
        assert process.poll() is None, "worker process exited unexpectedly during startup"

        # last_seen_at advances across 2+ polls for the first slot.
        workload_endpoint_id = slots[0].endpoint_id
        first_heartbeat = queue.get_endpoint(workload_endpoint_id)
        assert first_heartbeat is not None
        time.sleep(2.5)
        second_heartbeat = queue.get_endpoint(workload_endpoint_id)
        assert second_heartbeat is not None
        time.sleep(2.5)
        third_heartbeat = queue.get_endpoint(workload_endpoint_id)
        assert third_heartbeat is not None
        assert second_heartbeat.last_seen_at > first_heartbeat.last_seen_at
        assert third_heartbeat.last_seen_at > second_heartbeat.last_seen_at
        assert process.poll() is None, "worker process died after registering"

        job = _submit_remote_agent_job(queue, cluster=cluster, idempotency_key="daemon-lease-proof")
        terminal = _wait_for_job_terminal(
            queue,
            job_id=job.job_id,
            timeout_seconds=_JOB_TERMINAL_TIMEOUT_SECONDS,
        )
        assert terminal.attempts == 1
        assert terminal.leased_by in {slots[0].endpoint_id, slots[1].endpoint_id}
        assert terminal.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
    finally:
        _terminate(process)
