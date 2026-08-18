"""Tests for the worker-slot lane resilience fix (clio-relay#238).

Three layers, matching the module's own two-part fix plus the exact
crash it replaces:

1. Fast, isolated unit tests of :mod:`clio_relay.endpoint_worker_lanes`
   itself (``quarantine_relay_error`` / ``run_worker_lane_iteration``)
   against small recording fakes -- no queue, no worker, no subprocess.
2. An in-process integration test that seeds a poisoned durable
   ``jarvis_execution_recovery`` record on a real
   ``ClioCoreQueue``/``EndpointWorker`` pair and proves
   ``_reconcile_pending_execution_cleanup`` no longer raises, quarantines
   it with a typed event, and the worker keeps leasing new jobs.
3. Process-level tests that start the real ``clio-relay endpoint start
   --role worker`` CLI against a temp core (issue #238's acceptance item
   1): daemon-mode registers every slot and keeps heartbeating, and an
   admitted ``remote_agent`` job reaches ``attempts == 1``,
   ``leased_by == <endpoint>``, and a terminal state -- plus the
   poisoned-record variant seeded before the daemon starts.

Before the fix (``endpoint.py``'s unguarded call to
``_durable_jarvis_execution_recovery`` inside
``_reconcile_pending_execution_cleanup``, and no exception handling at all
around ``_serve_worker_slot``'s per-slot loop), test 2 raised
``RelayError: JARVIS execution recovery intent is invalid for ...``
directly out of the reconcile call, and test 3's daemon-mode process would
never bring the workload slot up to a stable, heartbeating registration
(the process-level poisoned-record variant would hang rather than
register) -- both confirmed red against the pre-fix tree before this
module existed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.endpoint_worker_lanes import (
    LANE_LAST_ERROR_METADATA_KEY,
    RECOVERY_RECORD_QUARANTINED,
    WORKER_LANE_ITERATION_FAILED,
    quarantine_relay_error,
    run_worker_lane_iteration,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
)

# ---------------------------------------------------------------------------
# Layer 1: isolated unit tests against small recording fakes.
# ---------------------------------------------------------------------------


class _RecordingEventQueue:
    """Minimal ``append_event`` recorder satisfying ``WorkerLaneEventQueue``."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, dict[str, object] | None]] = []

    def append_event(
        self,
        job_id: str,
        event_type: str,
        message: str,
        *,
        payload: dict[str, object] | None = None,
    ) -> object:
        self.calls.append((job_id, event_type, message, payload))
        return None


class _RecordingRegistrationQueue:
    """Minimal ``register_endpoint`` recorder satisfying ``WorkerLaneRegistrationQueue``."""

    def __init__(self) -> None:
        self.registrations: list[EndpointRegistration] = []

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration:
        self.registrations.append(endpoint)
        return endpoint


def test_quarantine_relay_error_passes_through_on_success() -> None:
    queue = _RecordingEventQueue()

    result = quarantine_relay_error(
        lambda: {"ok": True},
        queue=queue,
        job_id="job_test",
        task_id="task_test",
        context="unit-test",
    )

    assert result == {"ok": True}
    assert queue.calls == []


def test_quarantine_relay_error_quarantines_relay_error_instead_of_raising() -> None:
    queue = _RecordingEventQueue()

    def _poisoned() -> None:
        raise RelayError("JARVIS execution recovery intent is invalid for task_poison")

    result = quarantine_relay_error(
        _poisoned,
        queue=queue,
        job_id="job_poison",
        task_id="task_poison",
        context="unit-test",
    )

    assert result is None
    assert len(queue.calls) == 1
    job_id, event_type, message, payload = queue.calls[0]
    assert job_id == "job_poison"
    assert event_type == "jarvis.execution_recovery_quarantined"
    assert "unit-test" in message
    assert payload is not None
    assert payload["reason"] == RECOVERY_RECORD_QUARANTINED
    assert payload["task_id"] == "task_poison"
    assert payload["job_id"] == "job_poison"
    assert payload["context"] == "unit-test"
    assert "invalid for task_poison" in cast(str, payload["detail"])


def test_quarantine_relay_error_does_not_swallow_non_relay_errors() -> None:
    queue = _RecordingEventQueue()

    def _unrelated_bug() -> None:
        raise ValueError("not a recovery-intent problem")

    with pytest.raises(ValueError, match="not a recovery-intent problem"):
        quarantine_relay_error(
            _unrelated_bug,
            queue=queue,
            job_id="job_x",
            task_id="task_x",
            context="unit-test",
        )
    assert queue.calls == []


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

    with caplog.at_level("ERROR", logger="clio_relay.endpoint_worker_lanes"):
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


# ---------------------------------------------------------------------------
# Layer 2: in-process integration against a real ClioCoreQueue/EndpointWorker.
# ---------------------------------------------------------------------------


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


def _seed_poisoned_recovery_record(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    idempotency_key: str,
) -> tuple[RelayJob, RelayTask]:
    """Seed one task discoverable by ``scan_execution_cleanup`` with an
    invalid ``jarvis_execution_recovery`` record -- clio-relay#238's exact
    repro shape (a record that fails ``_durable_jarvis_execution_recovery``'s
    validation because its route/schema does not match what was persisted).
    """
    job = _submit_remote_agent_job(queue, cluster=cluster, idempotency_key=idempotency_key)
    task = queue.append_task(
        RelayTask(job_id=job.job_id, name="poisoned-recovery", metadata={"cluster": cluster})
    )
    queue.register_execution_cleanup(
        task.task_id,
        {
            "jarvis_execution_recovery": {
                "schema_version": "not-a-real-schema",
                "state": "not-a-real-state",
            }
        },
    )
    return job, task


def test_reconcile_pending_execution_cleanup_quarantines_poisoned_record_and_continues(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    poison_job, poison_task = _seed_poisoned_recovery_record(
        queue, cluster="ares", idempotency_key="poisoned-recovery-record"
    )

    worker = EndpointWorker(
        role=EndpointRole.WORKER, settings=settings, cluster="ares", queue=queue
    )
    worker.register()

    # Pre-fix this raised RelayError straight out of the reconcile scan
    # (endpoint.py's unguarded first fetch, quarantine_relay_error.py's
    # docstring). It must not raise now.
    cast(Any, worker)._reconcile_pending_execution_cleanup()  # noqa: SLF001

    events, _ = queue.drain_events(Cursor(job_id=poison_job.job_id), limit=50)
    quarantine_events = [
        e for e in events if e.event_type == "jarvis.execution_recovery_quarantined"
    ]
    assert len(quarantine_events) == 1
    payload = quarantine_events[0].payload
    assert payload["reason"] == RECOVERY_RECORD_QUARANTINED
    assert payload["task_id"] == poison_task.task_id
    assert payload["job_id"] == poison_job.job_id

    # The lane continues: a fresh job submitted after the poison still
    # leases and reaches a terminal state through the SAME worker instance.
    fresh_job = _submit_remote_agent_job(
        queue, cluster="ares", idempotency_key="post-quarantine-job"
    )
    result = worker.run_once()

    assert result is not None
    assert result.job_id == fresh_job.job_id
    assert result.attempts == 1
    assert worker.endpoint is not None
    assert result.leased_by == worker.endpoint.endpoint_id
    assert result.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}


# ---------------------------------------------------------------------------
# Layer 3: process-level acceptance (issue #238's own acceptance item 1,
# plus the poisoned-record variant run through the real daemon).
# ---------------------------------------------------------------------------

_BRINGUP_FLAGS = [
    "--concurrency",
    "2",
    "--control-query-concurrency",
    "1",
    "--kind-concurrency",
    "mcp_call=2",
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
        "from clio_relay.cli import app; app()",
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


def _wait_for_workload_and_control_query_slots(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    timeout_seconds: float,
) -> dict[str, EndpointRegistration]:
    """Poll until both a workload and a control-query slot are registered."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        slots = {
            cast(str, endpoint.metadata.get("mcp_admission_class")): endpoint
            for endpoint in queue.list_endpoints(cluster=cluster)
            if endpoint.metadata.get("worker_slot") is not None
        }
        if "workload" in slots and "control_query" in slots:
            return slots
        time.sleep(_HEARTBEAT_POLL_SECONDS)
    pytest.fail(
        "daemon worker did not register both workload and control-query slots "
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
    with the bring-up flag set registers BOTH the workload and control-query
    slots and keeps heartbeating; an admitted remote_agent job reaches
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
        slots = _wait_for_workload_and_control_query_slots(
            queue,
            cluster=cluster,
            timeout_seconds=_DAEMON_STARTUP_TIMEOUT_SECONDS,
        )
        assert process.poll() is None, "worker process exited unexpectedly during startup"

        # last_seen_at advances across 2+ polls for the workload slot.
        workload_endpoint_id = slots["workload"].endpoint_id
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
        assert terminal.leased_by in {
            slots["workload"].endpoint_id,
            slots["control_query"].endpoint_id,
        }
        assert terminal.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
    finally:
        _terminate(process)


def test_daemon_worker_survives_poisoned_recovery_record_seeded_before_start(
    tmp_path: Path,
) -> None:
    """clio-relay#238 acceptance item 2, run through the real daemon: a
    poisoned recovery record seeded before the worker starts must not
    prevent registration, must be quarantined with a typed event, and a
    fresh job must still lease and reach a terminal state.
    """
    cluster = "test-238-poisoned"
    core_dir = tmp_path / "core"
    spool_dir = tmp_path / "spool"
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()

    seed_queue = ClioCoreQueue(core_dir)
    poison_job, poison_task = _seed_poisoned_recovery_record(
        seed_queue, cluster=cluster, idempotency_key="daemon-poisoned-recovery"
    )

    process = _spawn_daemon_worker(
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        stdout_log=logs_dir / "worker.out.log",
        stderr_log=logs_dir / "worker.err.log",
    )
    try:
        queue = ClioCoreQueue(core_dir)
        slots = _wait_for_workload_and_control_query_slots(
            queue,
            cluster=cluster,
            timeout_seconds=_DAEMON_STARTUP_TIMEOUT_SECONDS,
        )
        assert process.poll() is None, "worker exited unexpectedly despite the poisoned record"

        # The quarantine event must appear on the poisoned job's own event log.
        deadline = time.monotonic() + _DAEMON_STARTUP_TIMEOUT_SECONDS
        quarantine_events: list[RelayEvent] = []
        while time.monotonic() < deadline and not quarantine_events:
            events, _ = queue.drain_events(Cursor(job_id=poison_job.job_id), limit=50)
            quarantine_events = [
                e for e in events if e.event_type == "jarvis.execution_recovery_quarantined"
            ]
            if not quarantine_events:
                time.sleep(_HEARTBEAT_POLL_SECONDS)
        assert quarantine_events, "no typed quarantine event observed for the poisoned record"
        payload = quarantine_events[0].payload
        assert payload["task_id"] == poison_task.task_id

        # Heartbeat still advances despite the poison sitting in the queue.
        workload_endpoint_id = slots["workload"].endpoint_id
        before = queue.get_endpoint(workload_endpoint_id)
        assert before is not None
        time.sleep(2.5)
        after = queue.get_endpoint(workload_endpoint_id)
        assert after is not None
        assert after.last_seen_at > before.last_seen_at

        # A fresh job still leases and reaches a terminal state.
        job = _submit_remote_agent_job(
            queue, cluster=cluster, idempotency_key="post-poison-daemon-job"
        )
        terminal = _wait_for_job_terminal(
            queue,
            job_id=job.job_id,
            timeout_seconds=_JOB_TERMINAL_TIMEOUT_SECONDS,
        )
        assert terminal.attempts == 1
        assert terminal.leased_by is not None
        assert terminal.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
    finally:
        _terminate(process)
