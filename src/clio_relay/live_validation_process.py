"""Worker process/task/lease discovery and observation for live validation.

Owns the "prove a real worker process is behind this job" concern the
``queue_validation`` live fixture depends on: bounded task/lease/endpoint
scans, the :class:`_WorkerProcessObservation` evidence record, waiting for a
submitted job to become an observable running worker process, decoding the
bounded command's own stdout marker, waiting for an idle worker slot to
cycle while a kind-capped job stays refused, waiting for a worker's complete
cancellation (lease released, both PIDs gone), durable job-event iteration/
lookup, and the cross-platform process-liveness probe. Moved verbatim out
of ``queue_validation.py`` (iowarp/clio-relay#231-style split); no behavior
changed.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.live_validation_constants import (
    PROCESS_DISCOVERY_TIMEOUT_SECONDS,
    VALIDATION_MARKER_SCHEMA,
)
from clio_relay.live_validation_support import _mapping, _require
from clio_relay.models import (
    TERMINAL_STATES,
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    Lease,
    RelayEvent,
    RelayTask,
)
from clio_relay.queue_management import MAX_SCAN_LIMIT, worker_status


def _complete_job_tasks(queue: ClioCoreQueue, job_id: str) -> list[RelayTask]:
    """Return a bounded complete task snapshot for one validation-owned job."""
    tasks, truncated = queue.scan_job_tasks(job_id, limit=MAX_SCAN_LIMIT)
    if truncated:
        raise RelayError(
            f"validation task discovery exceeds the bounded limit {MAX_SCAN_LIMIT}: {job_id}"
        )
    return tasks


def _complete_job_leases(queue: ClioCoreQueue, job_id: str) -> list[Lease]:
    """Return a bounded complete lease snapshot for one validation-owned job."""
    leases, truncated = queue.scan_job_leases(job_id, limit=MAX_SCAN_LIMIT)
    if truncated:
        raise RelayError(
            f"validation lease discovery exceeds the bounded limit {MAX_SCAN_LIMIT}: {job_id}"
        )
    return leases


def _complete_cluster_endpoints(
    queue: ClioCoreQueue,
    cluster: str,
) -> list[EndpointRegistration]:
    """Return a bounded complete endpoint snapshot for validation ownership checks."""
    endpoints, truncated = queue.scan_endpoints(limit=MAX_SCAN_LIMIT, cluster=cluster)
    if truncated:
        raise RelayError(
            f"validation endpoint discovery exceeds the bounded limit {MAX_SCAN_LIMIT}: {cluster}"
        )
    return endpoints


@dataclass(frozen=True)
class _WorkerProcessObservation:
    """Live worker ownership and process evidence for one validation job."""

    job_id: str
    role: str
    task_id: str
    lease_id: str
    endpoint_id: str
    worker_slot: int
    outer_pid: int
    child_pid: int
    marker: dict[str, object]

    def as_metadata(self) -> dict[str, object]:
        """Return machine-readable process ownership evidence."""
        return {
            "job_id": self.job_id,
            "role": self.role,
            "task_id": self.task_id,
            "lease_id": self.lease_id,
            "endpoint_id": self.endpoint_id,
            "worker_slot": self.worker_slot,
            "outer_pid": self.outer_pid,
            "child_pid": self.child_pid,
            "marker": self.marker,
        }


def _wait_for_worker_process_started(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cluster: str,
    report_id: str,
    registered_endpoint_ids: set[str],
    timeout_seconds: float,
    poll_seconds: float,
) -> _WorkerProcessObservation:
    deadline = time.monotonic() + timeout_seconds
    last_state = "unobserved"
    while time.monotonic() < deadline:
        job = queue.get_job(job_id)
        last_state = job.state.value
        if job.state in TERMINAL_STATES:
            raise RelayError(f"validation process terminated before observation: {job_id}")
        tasks = [
            task for task in _complete_job_tasks(queue, job_id) if task.state is JobState.RUNNING
        ]
        leases = [
            lease
            for lease in _complete_job_leases(queue, job_id)
            if lease.job_id == job_id and not lease.is_expired()
        ]
        outer_pid, marker = _process_markers(queue, job_id, report_id=report_id)
        if job.state is JobState.RUNNING and len(tasks) == 1 and len(leases) == 1:
            lease = leases[0]
            endpoint = next(
                (
                    item
                    for item in _complete_cluster_endpoints(queue, cluster)
                    if item.endpoint_id == lease.endpoint_id
                    and item.endpoint_id in registered_endpoint_ids
                    and item.role is EndpointRole.WORKER
                ),
                None,
            )
            worker_slot = None if endpoint is None else endpoint.metadata.get("worker_slot")
            child_pid = None if marker is None else marker.get("child_pid")
            role = None if marker is None else marker.get("role")
            if (
                endpoint is not None
                and isinstance(worker_slot, int)
                and isinstance(outer_pid, int)
                and isinstance(child_pid, int)
                and isinstance(role, str)
                and _process_exists(outer_pid)
                and _process_exists(child_pid)
            ):
                return _WorkerProcessObservation(
                    job_id=job_id,
                    role=role,
                    task_id=tasks[0].task_id,
                    lease_id=lease.lease_id,
                    endpoint_id=endpoint.endpoint_id,
                    worker_slot=worker_slot,
                    outer_pid=outer_pid,
                    child_pid=child_pid,
                    marker=cast(dict[str, object], marker),
                )
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(f"worker process {job_id} did not become observable; state={last_state}")


def _process_markers(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    report_id: str,
) -> tuple[int | None, dict[str, object] | None]:
    outer_pid: int | None = None
    stdout = ""
    for event in _iter_job_events(queue, job_id):
        if event.event_type == "execution.started":
            candidate = event.payload.get("pid")
            if isinstance(candidate, int):
                outer_pid = candidate
        if event.event_type == "stdout.delta":
            text = event.payload.get("text")
            if isinstance(text, str) and len(stdout) < 65_536:
                stdout += text[: 65_536 - len(stdout)]
    marker: dict[str, object] | None = None
    for line in stdout.splitlines():
        try:
            candidate = cast(object, json.loads(line))
        except json.JSONDecodeError:
            continue
        if not isinstance(candidate, dict):
            continue
        typed = {str(key): value for key, value in cast(dict[object, object], candidate).items()}
        if (
            typed.get("schema_version") == VALIDATION_MARKER_SCHEMA
            and typed.get("report_id") == report_id
        ):
            marker = typed
    return outer_pid, marker


def _wait_for_worker_admission_cycle(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    overflow_job_id: str,
    kind: JobKind,
    kind_limit: int,
    heartbeat_snapshot: dict[str, datetime],
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    _require(bool(heartbeat_snapshot), "no idle worker slot was available for overflow proof")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        overflow = queue.get_job(overflow_job_id)
        if overflow.state is not JobState.QUEUED:
            raise RelayError(f"live worker admitted overflow job: {overflow.state.value}")
        current = {item.endpoint_id: item for item in _complete_cluster_endpoints(queue, cluster)}
        advanced = [
            endpoint_id
            for endpoint_id, before in heartbeat_snapshot.items()
            if (endpoint := current.get(endpoint_id)) is not None and endpoint.last_seen_at > before
        ]
        if advanced:
            time.sleep(min(0.25, max(0.05, poll_seconds)))
            overflow = queue.get_job(overflow_job_id)
            active = _mapping(
                worker_status(queue, cluster=cluster).get("active_leases_by_kind"),
                "active leases by kind",
            ).get(kind.value)
            overflow_leases = [
                lease
                for lease in _complete_job_leases(queue, overflow_job_id)
                if lease.job_id == overflow_job_id
            ]
            _require(overflow.state is JobState.QUEUED, "overflow changed state after worker cycle")
            _require(not overflow_leases, "overflow gained a durable lease after worker cycle")
            _require(active == kind_limit, "active leases no longer matched the kind cap")
            return {
                "observed_endpoint_ids": advanced,
                "heartbeat_before": {
                    key: value.isoformat() for key, value in heartbeat_snapshot.items()
                },
                "heartbeat_after": {
                    key: current[key].last_seen_at.isoformat() for key in advanced if key in current
                },
                "overflow_state": overflow.state.value,
                "overflow_lease_count": 0,
                "active_kind_leases": active,
            }
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    raise TimeoutError("no idle registered worker slot cycled while overflow remained queued")


def _wait_for_worker_cancellation(
    queue: ClioCoreQueue,
    observation: _WorkerProcessObservation,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_task_state = "unobserved"
    while time.monotonic() < deadline:
        task = queue.get_task(observation.task_id)
        last_task_state = task.state.value
        leases = _complete_job_leases(queue, observation.job_id)
        event_types = {event.event_type for event in _iter_job_events(queue, observation.job_id)}
        outer_alive = _process_exists(observation.outer_pid)
        child_alive = _process_exists(observation.child_pid)
        if (
            task.state is JobState.CANCELED
            and not leases
            and "execution.canceled" in event_types
            and not outer_alive
            and not child_alive
        ):
            return {
                "worker_cancellation_acknowledged": True,
                "execution_canceled_event": True,
                "lease_released": True,
                "outer_process_exited": True,
                "child_process_exited": True,
                "residual_process_count": 0,
            }
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(
        f"worker cancellation was incomplete for {observation.job_id}; "
        f"task={last_task_state} outer_alive={_process_exists(observation.outer_pid)} "
        f"child_alive={_process_exists(observation.child_pid)}"
    )


def _latest_cancel_request(queue: ClioCoreQueue, job_id: str) -> RelayEvent | None:
    latest: RelayEvent | None = None
    for event in _iter_job_events(queue, job_id):
        if event.event_type == "job.cancel_requested":
            latest = event
    return latest


def _scheduler_cancel_events(queue: ClioCoreQueue, job_id: str) -> list[RelayEvent]:
    return [
        event
        for event in _iter_job_events(queue, job_id)
        if event.event_type in {"scheduler.cancel_requested", "scheduler.cancel_failed"}
    ]


def _iter_job_events(queue: ClioCoreQueue, job_id: str) -> list[RelayEvent]:
    next_seq = 1
    result: list[RelayEvent] = []
    while True:
        events, advanced = queue.read_event_page(job_id, next_seq=next_seq, limit=1_000)
        if not events:
            return result
        result.extend(events)
        if advanced <= next_seq:
            raise RelayError(f"event pagination did not advance for job {job_id}")
        next_seq = advanced


def _process_exists(process_id: int) -> bool:
    if process_id <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {process_id}", "/FO", "CSV", "/NH"],
                check=False,
                capture_output=True,
                text=True,
                timeout=PROCESS_DISCOVERY_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired):
            return True
        if result.returncode != 0:
            # An errored probe (e.g. tasklist "Access denied") is not
            # evidence of absence — mirror the OSError branch above and
            # assume the process is alive rather than falsely settling it.
            return True
        return f'"{process_id}"' in result.stdout
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True
