"""Canonical live validation for production relay queue management."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import uuid4

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    TERMINAL_STATES,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    RelayEvent,
    RelayJob,
    RelayTask,
    SchedulerPhase,
    SchedulerStatus,
    utc_now,
)
from clio_relay.queue_management import (
    MAX_RESULT_LIMIT,
    MAX_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    list_queue_jobs,
    worker_status,
)
from clio_relay.scheduler_providers import SchedulerValidationProvider
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
)

VALIDATION_KIND_LIMIT = 2
VALIDATION_MINIMUM_TOTAL_CONCURRENCY = 3
VALIDATION_COMMAND_SECONDS = 300
VALIDATION_MARKER_SCHEMA = "clio-relay.queue-validation-process.v1"
MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS = 600.0
MAX_VALIDATION_POLL_SECONDS = 10.0
PROCESS_DISCOVERY_TIMEOUT_SECONDS = 5.0


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


def run_queue_management_validation(
    queue: ClioCoreQueue,
    *,
    job_id: str | None,
    cluster: str,
    older_than_seconds: int,
    scan_limit: int,
    kind: JobKind | None = None,
    scheduler_provider: SchedulerValidationProvider | None = None,
    scheduler_run_seconds: int = 5,
    scheduler_timeout_seconds: float = 120.0,
    scheduler_poll_seconds: float = 1.0,
    launcher: str | None = None,
    install_source: str | None = None,
    artifact_sha256: str | None = None,
) -> LiveValidationReport:
    """Exercise queue behavior with real workers and bounded harmless processes.

    The live fixture requires a worker service with at least three slots and an
    explicit ``jarvis=2`` kind cap. Two report-owned bounded commands must run on
    distinct registered worker slots. A third command must remain queued while
    an otherwise idle slot cycles, proving the kind cap rather than only total
    worker capacity. The validator then executes exact stale cleanup and cancels
    one real running task while a held provider job remains pending. Success
    requires worker cancellation acknowledgment, lease release, and both the
    outer JARVIS and embedded command PIDs to disappear.

    ``job_id`` is an optional expendable compatibility anchor. It is canceled
    before the controlled live fixtures and is never executed or copied.
    """
    _validate_options(
        older_than_seconds=older_than_seconds,
        scan_limit=scan_limit,
        scheduler_run_seconds=scheduler_run_seconds,
        scheduler_timeout_seconds=scheduler_timeout_seconds,
        scheduler_poll_seconds=scheduler_poll_seconds,
    )
    report = new_live_validation_report(
        scenario="queue-management",
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode="queue_management_acceptance",
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    queue.initialize()
    owned_jobs: dict[str, str] = {}
    process_observations: dict[str, _WorkerProcessObservation] = {}
    scheduler_job_id: str | None = None
    scheduler_terminal = False
    primary_error: Exception | None = None

    try:
        if kind is not None:
            _require(
                kind is JobKind.JARVIS,
                "live process validation requires the bounded JARVIS command kind",
            )
        if job_id is not None:
            _cancel_optional_anchor(recorder, queue, job_id=job_id, cluster=cluster)
            owned_jobs[job_id] = "queue-validation-anchor"

        result_limit = min(MAX_RESULT_LIMIT, scan_limit)
        capacity = worker_status(queue, cluster=cluster)
        kind_limit, configured_total = _controlled_capacity(capacity, JobKind.JARVIS)
        _require(
            kind_limit == VALIDATION_KIND_LIMIT,
            f"live validation requires jarvis kind concurrency {VALIDATION_KIND_LIMIT}, "
            f"found {kind_limit}",
        )
        _require(
            configured_total >= VALIDATION_MINIMUM_TOTAL_CONCURRENCY,
            "live kind-cap proof requires at least three worker slots",
        )
        containment = _controlled_process_containment(capacity)
        with recorder.check(
            "queue.worker-containment-enforced",
            "verify every live worker uses kernel-enforced descendant containment",
        ) as evidence:
            evidence.append(
                _evidence(
                    "worker_process_containment",
                    f"relay-worker://{cluster}/process-containment",
                    containment,
                )
            )
        _require_quiet_validation_queue(queue, cluster=cluster)
        heartbeat_snapshot = _worker_heartbeat_snapshot(capacity)
        _require(
            len(heartbeat_snapshot) >= VALIDATION_MINIMUM_TOTAL_CONCURRENCY,
            "worker status did not expose three live worker-slot endpoints",
        )

        running_jobs = [
            queue.submit_job(
                _validation_execution_job(
                    cluster=cluster,
                    report_id=report.report_id,
                    role="scheduler-preservation-target" if index == 0 else "parallel-peer",
                    index=index,
                )
            )
            for index in range(VALIDATION_KIND_LIMIT)
        ]
        owned_jobs[running_jobs[0].job_id] = "queue-management-running-target"
        owned_jobs[running_jobs[1].job_id] = "queue-concurrency-parallel-peer"

        with recorder.check(
            "queue.kind-concurrency-parallel",
            "observe two bounded commands running on distinct live worker slots",
        ) as evidence:
            for running_job in running_jobs:
                process_observations[running_job.job_id] = _wait_for_worker_process_started(
                    queue,
                    running_job.job_id,
                    cluster=cluster,
                    report_id=report.report_id,
                    registered_endpoint_ids=set(heartbeat_snapshot),
                    timeout_seconds=scheduler_timeout_seconds,
                    poll_seconds=scheduler_poll_seconds,
                )
            observations = list(process_observations.values())
            _require(
                len({item.endpoint_id for item in observations}) == VALIDATION_KIND_LIMIT,
                "parallel jobs were not owned by distinct worker slots",
            )
            _require(
                len({item.child_pid for item in observations}) == VALIDATION_KIND_LIMIT,
                "parallel jobs did not expose distinct child processes",
            )
            evidence.append(
                _evidence(
                    "live_worker_parallelism",
                    f"relay-worker://{cluster}/kind/jarvis/parallel",
                    {
                        "kind": JobKind.JARVIS.value,
                        "configured_kind_limit": kind_limit,
                        "configured_total": configured_total,
                        "processes": [item.as_metadata() for item in observations],
                    },
                )
            )

        overflow = queue.submit_job(
            _validation_execution_job(
                cluster=cluster,
                report_id=report.report_id,
                role="kind-capacity-overflow",
                index=VALIDATION_KIND_LIMIT,
            )
        )
        owned_jobs[overflow.job_id] = "queue-concurrency-overflow"
        busy_endpoint_ids = {item.endpoint_id for item in process_observations.values()}
        idle_heartbeat_snapshot = {
            endpoint_id: observed_at
            for endpoint_id, observed_at in heartbeat_snapshot.items()
            if endpoint_id not in busy_endpoint_ids
        }

        with recorder.check(
            "queue.kind-concurrency-worker-enforced",
            "observe an idle live worker slot refuse a third JARVIS job at the kind cap",
        ) as evidence:
            worker_observation = _wait_for_worker_admission_cycle(
                queue,
                cluster=cluster,
                overflow_job_id=overflow.job_id,
                kind=JobKind.JARVIS,
                kind_limit=kind_limit,
                heartbeat_snapshot=idle_heartbeat_snapshot,
                timeout_seconds=scheduler_timeout_seconds,
                poll_seconds=scheduler_poll_seconds,
            )
            capacity_metadata: dict[str, object] = {
                "configured_concurrency": configured_total,
                "kind_concurrency": {JobKind.JARVIS.value: kind_limit},
                "kind_concurrency_consistent": True,
                "process_containment": containment,
                "controlled_probe": {
                    "kind": JobKind.JARVIS.value,
                    "active_before": 0,
                    "active_at_cap": kind_limit,
                    "running_processes": [
                        item.as_metadata() for item in process_observations.values()
                    ],
                    "overflow_job_id": overflow.job_id,
                    "overflow_lease_acquired": False,
                    "live_worker_observation": worker_observation,
                },
            }
            recorder.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{cluster}:capacity",
                    role="cluster_worker",
                    cluster=cluster,
                    state="running",
                    metadata=capacity_metadata,
                )
            )
            evidence.append(
                _evidence(
                    "live_worker_admission",
                    f"relay-worker://{cluster}/kind/jarvis/overflow",
                    worker_observation,
                )
            )

        _cancel_queued_validation_job(
            recorder,
            queue,
            overflow,
            cluster=cluster,
            role=owned_jobs[overflow.job_id],
            action="cancel_after_live_admission_refusal",
        )

        stale_created_at = utc_now() - timedelta(seconds=older_than_seconds + 1)
        stale_target = queue.submit_job(
            _validation_execution_job(
                cluster=cluster,
                report_id=report.report_id,
                role="stale-cleanup-target",
                index=VALIDATION_KIND_LIMIT + 1,
                created_at=stale_created_at,
            )
        )
        owned_jobs[stale_target.job_id] = "queue-management-target"

        _validate_bounded_listing(
            recorder,
            queue,
            stale_target,
            cluster=cluster,
            limit=result_limit,
            scan_limit=scan_limit,
        )
        _validate_specific_diagnosis(
            recorder,
            queue,
            stale_target,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            scan_limit=scan_limit,
        )
        _validate_stale_cleanup(
            recorder,
            queue,
            stale_target,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            scan_limit=scan_limit,
        )

        _require(scheduler_provider is not None, "queue validation requires a scheduler provider")
        live_scheduler = cast(SchedulerValidationProvider, scheduler_provider)
        scheduler_job_id = live_scheduler.submit_held_validation_job(
            job_name=f"clio-relay-queue-{uuid4().hex[:12]}",
            run_seconds=scheduler_run_seconds,
        )
        scheduler_before = _wait_for_scheduler_phase(
            live_scheduler,
            scheduler_job_id,
            required={SchedulerPhase.PENDING},
            timeout_seconds=scheduler_timeout_seconds,
            poll_seconds=scheduler_poll_seconds,
        )

        running_target = running_jobs[0]
        running_observation = process_observations[running_target.job_id]
        queue.update_task_metadata(
            running_observation.task_id,
            {
                "scheduler": live_scheduler.name,
                "scheduler_job_ids": [scheduler_job_id],
                "scheduler_status": scheduler_before.model_dump(mode="json"),
                "owned_validation_scheduler_job": True,
            },
        )

        with recorder.check(
            "queue.cancel-running-worker-process",
            "cancel a worker-owned running process and verify complete termination",
        ) as evidence:
            cancellation = cancel_queue_job(
                queue,
                running_target.job_id,
                cluster=cluster,
                scheduler_policy="relay-only",
            )
            _require(
                cancellation.get("scheduler_cancel_requested") is False,
                "relay-only cancellation requested scheduler cancellation",
            )
            termination = _wait_for_worker_cancellation(
                queue,
                running_observation,
                timeout_seconds=scheduler_timeout_seconds,
                poll_seconds=scheduler_poll_seconds,
            )
            canceled_job = queue.get_job(running_target.job_id)
            request = _mapping(
                canceled_job.metadata.get("cancellation_request"),
                "durable cancellation request",
            )
            _require(
                request.get("previous_state") == JobState.RUNNING.value
                and request.get("cancel_scheduler") is False,
                "durable request did not record running relay-only semantics",
            )
            _record_job_cleanup(
                recorder,
                canceled_job,
                role=owned_jobs[canceled_job.job_id],
                initial_state=JobState.RUNNING,
                action="cancel_running_worker_process",
                task_id=running_observation.task_id,
                metadata={
                    **running_observation.as_metadata(),
                    **termination,
                },
            )
            evidence.append(
                _evidence(
                    "worker_process_cancellation",
                    f"relay-job://{cluster}/{running_target.job_id}/process",
                    {
                        **running_observation.as_metadata(),
                        **termination,
                        "scheduler_job_id": scheduler_job_id,
                        "scheduler_cancel_requested": False,
                    },
                )
            )

        with recorder.check(
            "queue.scheduler-preserved-default",
            "observe the same live scheduler job after worker-process cancellation",
        ) as evidence:
            scheduler_after = live_scheduler.poll(scheduler_job_id)
            _require(
                scheduler_after.scheduler_job_id == scheduler_before.scheduler_job_id,
                "scheduler identity changed after relay-only cancellation",
            )
            _require(
                scheduler_after.scheduler == live_scheduler.name,
                "scheduler provider identity changed after relay-only cancellation",
            )
            _require(
                scheduler_after.phase is SchedulerPhase.PENDING,
                f"held scheduler job was not preserved: {scheduler_after.phase.value}",
            )
            cancel_event = _latest_cancel_request(queue, running_target.job_id)
            _require(cancel_event is not None, "relay cancellation event was not recorded")
            cancel_event = cast(RelayEvent, cancel_event)
            _require(
                cancel_event.payload.get("cancel_scheduler") is False,
                "cancellation event did not preserve scheduler work",
            )
            _require(
                not _scheduler_cancel_events(queue, running_target.job_id),
                "relay-only path emitted a scheduler cancellation event",
            )
            released = live_scheduler.release_validation_job(scheduler_job_id)
            _require(
                released.returncode == 0,
                released.stderr.strip() or "scheduler validation job release failed",
            )
            scheduler_completed = _wait_for_scheduler_phase(
                live_scheduler,
                scheduler_job_id,
                required={SchedulerPhase.COMPLETED},
                timeout_seconds=scheduler_timeout_seconds,
                poll_seconds=scheduler_poll_seconds,
            )
            scheduler_terminal = True
            recorder.add_resource(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    role="queue-preservation-fixture",
                    cluster=cluster,
                    state=scheduler_completed.phase.value,
                    provider=live_scheduler.name,
                    metadata={
                        "owned_validation_job": True,
                        "relay_cancel_requested": True,
                        "scheduler_cancel_requested": False,
                        "observed_before_relay_cancel": scheduler_before.model_dump(mode="json"),
                        "observed_after_relay_cancel": scheduler_after.model_dump(mode="json"),
                        "cleanup_observation": scheduler_completed.model_dump(mode="json"),
                    },
                )
            )
            recorder.report.cleanup.actions.append(
                {
                    "kind": "scheduler_job",
                    "resource_id": scheduler_job_id,
                    "action": "release_and_wait",
                    "outcome": "completed",
                    "provider": live_scheduler.name,
                    "scheduler_cancel_requested": False,
                }
            )
            evidence.append(
                _evidence(
                    "scheduler_preservation",
                    f"scheduler-job://{live_scheduler.name}/{scheduler_job_id}",
                    {
                        "phase_before_relay_cancel": scheduler_before.phase.value,
                        "phase_after_relay_cancel": scheduler_after.phase.value,
                        "cleanup_phase": scheduler_completed.phase.value,
                        "cancel_scheduler": cancel_event.payload.get("cancel_scheduler"),
                        "scheduler_cancel_event_count": 0,
                    },
                )
            )

        peer = running_jobs[1]
        peer_observation = process_observations[peer.job_id]
        cancel_queue_job(queue, peer.job_id, cluster=cluster, scheduler_policy="relay-only")
        peer_termination = _wait_for_worker_cancellation(
            queue,
            peer_observation,
            timeout_seconds=scheduler_timeout_seconds,
            poll_seconds=scheduler_poll_seconds,
        )
        _record_job_cleanup(
            recorder,
            queue.get_job(peer.job_id),
            role=owned_jobs[peer.job_id],
            initial_state=JobState.RUNNING,
            action="cancel_parallel_validation_peer",
            task_id=peer_observation.task_id,
            metadata={**peer_observation.as_metadata(), **peer_termination},
        )
    except Exception as exc:
        primary_error = exc

    cleanup_error = _cleanup_validation_resources(
        recorder,
        queue,
        cluster=cluster,
        owned_jobs=owned_jobs,
        process_observations=process_observations,
        scheduler_provider=scheduler_provider,
        scheduler_job_id=scheduler_job_id,
        scheduler_terminal=scheduler_terminal,
        timeout_seconds=scheduler_timeout_seconds,
        poll_seconds=scheduler_poll_seconds,
    )
    final_error = _combined_error(primary_error, cleanup_error)
    recorder.finish(final_error)
    return report


def _cancel_optional_anchor(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    *,
    job_id: str,
    cluster: str,
) -> None:
    anchor = queue.get_job(job_id)
    _require_cluster(anchor, cluster)
    _require(
        anchor.state is JobState.QUEUED,
        f"queue validation anchor must be queued, found {anchor.state.value}",
    )
    result = cancel_queue_job(queue, job_id, cluster=cluster, scheduler_policy="relay-only")
    _require(
        result.get("scheduler_cancel_requested") is False,
        "validation anchor cancellation requested scheduler work",
    )
    _record_job_cleanup(
        recorder,
        queue.get_job(job_id),
        role="queue-validation-anchor",
        initial_state=JobState.QUEUED,
        action="cancel_expendable_anchor",
    )


def _validate_bounded_listing(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    target: RelayJob,
    *,
    cluster: str,
    limit: int,
    scan_limit: int,
) -> None:
    with recorder.check(
        "queue.list-bounded",
        "list a bounded JARVIS queue window without prior discovery state",
    ) as evidence:
        listing = list_queue_jobs(
            queue,
            cluster=cluster,
            kind=JobKind.JARVIS,
            limit=limit,
            scan_limit=scan_limit,
        )
        _require(target.job_id in _listed_job_ids(listing), "stale target was outside listing")
        evidence.append(
            _evidence(
                "queue_snapshot",
                f"relay-queue://{cluster}?kind=jarvis",
                {
                    "target_job_id": target.job_id,
                    "kind": JobKind.JARVIS.value,
                    "count": listing["count"],
                    "source_cursor": listing["source_cursor"],
                    "source_limit": listing["source_limit"],
                    "source_next_cursor": listing["source_next_cursor"],
                    "source_total": listing["source_total"],
                    "scan_limit": listing["scan_limit"],
                    "scan_truncated": listing["scan_truncated"],
                    "result_truncated": listing["result_truncated"],
                },
            )
        )


def _validate_specific_diagnosis(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    target: RelayJob,
    *,
    cluster: str,
    older_than_seconds: int,
    scan_limit: int,
) -> None:
    with recorder.check(
        "queue.diagnose-specific-reason",
        "diagnose one exact stale queued job with a coherent reason",
    ) as evidence:
        diagnosis = diagnose_job(
            queue,
            target.job_id,
            cluster=cluster,
            stale_after_seconds=older_than_seconds,
            scan_limit=scan_limit,
        )
        job = _mapping(diagnosis.get("job"), "diagnosis job")
        queue_evidence = _mapping(diagnosis.get("queue"), "diagnosis queue")
        lease_evidence = _mapping(diagnosis.get("lease"), "diagnosis lease")
        worker_evidence = _mapping(diagnosis.get("worker"), "diagnosis worker")
        scheduler_evidence = _list(diagnosis.get("scheduler"), "scheduler evidence")
        _require(job.get("job_id") == target.job_id, "diagnosis returned another job")
        _require(
            diagnosis.get("reason") == "queued_beyond_threshold",
            f"unexpected diagnosis reason: {diagnosis.get('reason')}",
        )
        _require(diagnosis.get("stale") is True, "diagnosis did not mark target stale")
        _require(
            queue_evidence.get("state") == JobState.QUEUED.value
            and queue_evidence.get("jobs_ahead") == 0,
            "queued-beyond-threshold reason conflicts with queue position",
        )
        _require(lease_evidence.get("present") is False, "queued target unexpectedly leased")
        healthy = worker_evidence.get("healthy_worker_count")
        _require(isinstance(healthy, int) and healthy > 0, "diagnosis lacks a healthy worker")
        _require(not scheduler_evidence, "queued target unexpectedly had scheduler work")
        evidence.append(
            _evidence(
                "queue_diagnosis",
                f"relay-job://{cluster}/{target.job_id}/diagnosis",
                {
                    "reason": diagnosis["reason"],
                    "stale": diagnosis["stale"],
                    "age_seconds": diagnosis["age_seconds"],
                    "queue": queue_evidence,
                    "lease": lease_evidence,
                    "healthy_worker_count": healthy,
                    "scheduler_observation_count": 0,
                },
            )
        )


def _validate_stale_cleanup(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    target: RelayJob,
    *,
    cluster: str,
    older_than_seconds: int,
    scan_limit: int,
) -> None:
    with recorder.check(
        "queue.stale-dry-run",
        "preview exact stale queued cancellation without mutating state",
    ) as evidence:
        before = queue.get_job(target.job_id)
        preview = cleanup_stale_jobs(
            queue,
            cluster=cluster,
            job_id=target.job_id,
            older_than_seconds=older_than_seconds,
            kind=JobKind.JARVIS,
            dry_run=True,
            cancel_queued=True,
            limit=1,
            scan_limit=scan_limit,
        )
        _require(before == queue.get_job(target.job_id), "stale preview changed target")
        plan = _plan_for_job(preview, target.job_id)
        _require(plan.get("action") == "cancel_queued_relay_job", "wrong stale action")
        _require(
            preview.get("scheduler_cancel_requested") is False,
            "stale preview requested scheduler cancellation",
        )
        evidence.append(
            _evidence(
                "queue_cleanup_preview",
                f"relay-job://{cluster}/{target.job_id}/stale-preview",
                {
                    "job_id": preview.get("job_id"),
                    "action": plan.get("action"),
                    "dry_run": preview.get("dry_run"),
                    "scheduler_cancel_requested": False,
                },
            )
        )

    with recorder.check(
        "queue.stale-cleanup-executed",
        "execute exact stale cleanup without scheduler cancellation",
    ) as evidence:
        executed = cleanup_stale_jobs(
            queue,
            cluster=cluster,
            job_id=target.job_id,
            older_than_seconds=older_than_seconds,
            kind=JobKind.JARVIS,
            dry_run=False,
            cancel_queued=True,
            limit=1,
            scan_limit=scan_limit,
        )
        plan = _plan_for_job(executed, target.job_id)
        canceled = queue.get_job(target.job_id)
        _require(executed.get("dry_run") is False, "stale cleanup remained a preview")
        _require(executed.get("canceled_count") == 1, "stale cleanup canceled no exact job")
        _require(canceled.state is JobState.CANCELED, "stale target survived cleanup")
        _require(
            executed.get("scheduler_cancel_requested") is False,
            "stale cleanup requested scheduler cancellation",
        )
        _record_job_cleanup(
            recorder,
            canceled,
            role="queue-management-target",
            initial_state=JobState.QUEUED,
            action="execute_stale_cleanup",
        )
        evidence.append(
            _evidence(
                "queue_cleanup_execution",
                f"relay-job://{cluster}/{target.job_id}/stale-cleanup",
                {
                    "job_id": executed.get("job_id"),
                    "action": plan.get("action"),
                    "dry_run": executed.get("dry_run"),
                    "canceled_count": executed.get("canceled_count"),
                    "final_state": canceled.state.value,
                    "scheduler_cancel_requested": False,
                },
            )
        )


def _cancel_queued_validation_job(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    cluster: str,
    role: str,
    action: str,
) -> None:
    result = cancel_queue_job(queue, job.job_id, cluster=cluster, scheduler_policy="relay-only")
    _require(
        result.get("scheduler_cancel_requested") is False,
        "queued validation cleanup requested scheduler cancellation",
    )
    _record_job_cleanup(
        recorder,
        queue.get_job(job.job_id),
        role=role,
        initial_state=JobState.QUEUED,
        action=action,
    )


def _validation_execution_job(
    *,
    cluster: str,
    report_id: str,
    role: str,
    index: int,
    created_at: datetime | None = None,
) -> RelayJob:
    """Build a bounded command that emits its private child identity then sleeps."""
    timestamp = created_at or utc_now()
    marker = {
        "schema_version": VALIDATION_MARKER_SCHEMA,
        "report_id": report_id,
        "role": role,
        "index": index,
    }
    script = (
        "import json,os,time;"
        f"marker={marker!r};"
        "marker['child_pid']=os.getpid();"
        "print(json.dumps(marker,sort_keys=True),flush=True);"
        f"time.sleep({VALIDATION_COMMAND_SECONDS})"
    )
    return RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(
            command=[sys.executable, "-u", "-c", script],
            timeout_seconds=VALIDATION_COMMAND_SECONDS,
        ),
        idempotency_key=f"queue-validation:{report_id}:{role}:{index}",
        created_at=timestamp,
        updated_at=timestamp,
        metadata={
            "queue_validation": {
                **marker,
                "bounded": True,
                "execute": role in {"scheduler-preservation-target", "parallel-peer"},
            }
        },
    )


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


def _cleanup_validation_resources(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    *,
    cluster: str,
    owned_jobs: dict[str, str],
    process_observations: dict[str, _WorkerProcessObservation],
    scheduler_provider: SchedulerValidationProvider | None,
    scheduler_job_id: str | None,
    scheduler_terminal: bool,
    timeout_seconds: float,
    poll_seconds: float,
) -> Exception | None:
    errors: list[str] = []
    for owned_job_id, role in owned_jobs.items():
        try:
            job = queue.get_job(owned_job_id)
            initial_state = job.state
            if job.state not in TERMINAL_STATES:
                cancel_queue_job(
                    queue,
                    owned_job_id,
                    cluster=cluster,
                    scheduler_policy="relay-only",
                )
                job = queue.get_job(owned_job_id)
            observation = process_observations.get(owned_job_id)
            metadata: dict[str, object] = {}
            if observation is not None:
                metadata = {
                    **observation.as_metadata(),
                    **_wait_for_worker_cancellation(
                        queue,
                        observation,
                        timeout_seconds=timeout_seconds,
                        poll_seconds=poll_seconds,
                    ),
                }
            if not any(
                resource.kind == "relay_job" and resource.resource_id == owned_job_id
                for resource in recorder.report.resources
            ):
                _record_job_cleanup(
                    recorder,
                    job,
                    role=role,
                    initial_state=initial_state,
                    action="cancel_validation_residual",
                    task_id=None if observation is None else observation.task_id,
                    metadata=metadata,
                )
        except Exception as exc:
            errors.append(f"relay job {owned_job_id}: {exc}")
            recorder.report.cleanup.remaining_resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=owned_job_id,
                    role=role,
                    cluster=cluster,
                    state="process_residual" if owned_job_id in process_observations else "unknown",
                )
            )
    if scheduler_job_id is not None and not scheduler_terminal:
        if scheduler_provider is None:
            errors.append(f"scheduler job {scheduler_job_id}: provider unavailable")
        else:
            try:
                canceled = scheduler_provider.cancel(scheduler_job_id)
                _require(
                    canceled.returncode == 0,
                    canceled.stderr.strip() or "scheduler fixture cancellation failed",
                )
                terminal = _wait_for_scheduler_phase(
                    scheduler_provider,
                    scheduler_job_id,
                    required={SchedulerPhase.CANCELED, SchedulerPhase.COMPLETED},
                    timeout_seconds=min(60.0, timeout_seconds),
                    poll_seconds=poll_seconds,
                )
                recorder.report.cleanup.cancel_scheduler_jobs = True
                recorder.report.cleanup.actions.append(
                    {
                        "kind": "scheduler_job",
                        "resource_id": scheduler_job_id,
                        "action": "cancel_failure_fixture",
                        "outcome": terminal.phase.value,
                        "provider": scheduler_provider.name,
                    }
                )
            except Exception as exc:
                errors.append(f"scheduler job {scheduler_job_id}: {exc}")
                recorder.report.cleanup.cancel_scheduler_jobs = True
                recorder.report.cleanup.remaining_resources.append(
                    ValidationResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        role="validation_cleanup_residual",
                        cluster=cluster,
                        state="unknown",
                        provider=scheduler_provider.name,
                    )
                )
    return (
        None if not errors else RelayError("queue validation cleanup failed: " + "; ".join(errors))
    )


def _controlled_capacity(capacity: dict[str, object], kind: JobKind) -> tuple[int, int]:
    worker_count = capacity.get("worker_count")
    configured_total = capacity.get("configured_concurrency")
    configured_by_kind = _mapping(
        capacity.get("configured_kind_concurrency"),
        "configured kind concurrency",
    )
    kind_limit = configured_by_kind.get(kind.value)
    _require(isinstance(worker_count, int) and worker_count > 0, "no fresh worker slots")
    _require(
        isinstance(configured_total, int) and configured_total > 0,
        "worker total concurrency is not bounded",
    )
    _require(
        capacity.get("kind_concurrency_consistent") is True,
        "fresh workers disagree on kind concurrency",
    )
    _require(
        isinstance(kind_limit, int) and kind_limit > 0,
        f"no explicit concurrency limit for {kind.value}",
    )
    return cast(int, kind_limit), cast(int, configured_total)


def _controlled_process_containment(capacity: dict[str, object]) -> dict[str, object]:
    workers = _list(capacity.get("workers"), "worker registrations")
    _require(bool(workers), "worker status exposed no live containment identities")
    modes: set[str] = set()
    endpoint_ids: list[str] = []
    for raw_worker in workers:
        worker = _mapping(raw_worker, "worker registration")
        endpoint_id = worker.get("endpoint_id")
        metadata = _mapping(worker.get("metadata"), "worker metadata")
        containment = _mapping(
            metadata.get("process_containment"),
            "worker process containment",
        )
        mode = containment.get("mode")
        _require(isinstance(endpoint_id, str), "worker containment omitted endpoint identity")
        _require(isinstance(mode, str) and bool(mode), "worker containment omitted provider mode")
        _require(
            containment.get("enforceable") is True,
            f"worker {endpoint_id} lacks kernel-enforced process containment: "
            f"{containment.get('reason')}",
        )
        endpoint_ids.append(cast(str, endpoint_id))
        modes.add(cast(str, mode))
    _require(len(modes) == 1, f"live workers disagree on containment mode: {sorted(modes)}")
    return {
        "enforceable": True,
        "mode": next(iter(modes)),
        "worker_endpoint_ids": sorted(endpoint_ids),
        "worker_count": len(endpoint_ids),
    }


def _worker_heartbeat_snapshot(capacity: dict[str, object]) -> dict[str, datetime]:
    workers = _list(capacity.get("workers"), "worker registrations")
    snapshot: dict[str, datetime] = {}
    for raw_worker in workers:
        worker = _mapping(raw_worker, "worker registration")
        metadata = _mapping(worker.get("metadata"), "worker metadata")
        endpoint_id = worker.get("endpoint_id")
        observed_at = worker.get("last_seen_at")
        if not isinstance(endpoint_id, str) or not isinstance(metadata.get("worker_slot"), int):
            continue
        if not isinstance(observed_at, str):
            raise RelayError(f"worker slot {endpoint_id} omitted last_seen_at")
        snapshot[endpoint_id] = datetime.fromisoformat(observed_at)
    return snapshot


def _require_quiet_validation_queue(queue: ClioCoreQueue, *, cluster: str) -> None:
    indexed_active, truncated = queue.scan_active_jobs(limit=MAX_SCAN_LIMIT)
    _require(not truncated, "active queue exceeds the validation scan bound")
    active = [
        job for job in indexed_active if job.cluster == cluster and job.state not in TERMINAL_STATES
    ]
    _require(
        not active,
        "controlled live validation requires an otherwise empty relay queue; active jobs="
        + ",".join(job.job_id for job in active[:20]),
    )


def _record_job_cleanup(
    recorder: ValidationRecorder,
    job: RelayJob,
    *,
    role: str,
    initial_state: JobState,
    action: str,
    task_id: str | None = None,
    metadata: dict[str, object] | None = None,
) -> None:
    if not any(
        resource.kind == "relay_job" and resource.resource_id == job.job_id
        for resource in recorder.report.resources
    ):
        recorder.add_resource(
            ValidationResource(
                kind="relay_job",
                resource_id=job.job_id,
                role=role,
                cluster=job.cluster,
                state=job.state.value,
                metadata={
                    "kind": job.kind.value,
                    "initial_state": initial_state.value,
                    "task_id": task_id,
                    "scheduler_cancel_requested": False,
                    **(metadata or {}),
                },
            )
        )
    if not any(
        item.get("kind") == "relay_job"
        and item.get("resource_id") == job.job_id
        and item.get("action") == action
        for item in recorder.report.cleanup.actions
    ):
        recorder.report.cleanup.actions.append(
            {
                "kind": "relay_job",
                "resource_id": job.job_id,
                "action": action,
                "outcome": job.state.value,
                "scheduler_cancel_requested": False,
            }
        )


def _wait_for_scheduler_phase(
    provider: SchedulerValidationProvider,
    scheduler_job_id: str,
    *,
    required: set[SchedulerPhase],
    timeout_seconds: float,
    poll_seconds: float,
) -> SchedulerStatus:
    deadline = time.monotonic() + timeout_seconds
    last_status: SchedulerStatus | None = None
    while time.monotonic() < deadline:
        last_status = provider.poll(scheduler_job_id)
        _require(
            last_status.scheduler_job_id == scheduler_job_id,
            "scheduler provider returned another job identity",
        )
        _require(
            last_status.scheduler == provider.name,
            "scheduler provider returned another provider identity",
        )
        if last_status.phase in required:
            return last_status
        if last_status.phase in {
            SchedulerPhase.COMPLETED,
            SchedulerPhase.CANCELED,
            SchedulerPhase.FAILED,
        }:
            break
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    last_phase = "unobserved" if last_status is None else last_status.phase.value
    expected = ",".join(sorted(phase.value for phase in required))
    raise TimeoutError(
        f"scheduler job {scheduler_job_id} did not reach {expected}; last phase={last_phase}"
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


def _listed_job_ids(listing: dict[str, object]) -> set[str]:
    ids: set[str] = set()
    for raw_item in _list(listing.get("jobs"), "queue listing"):
        item = _mapping(raw_item, "queue listing item")
        job = _mapping(item.get("job"), "listed job")
        job_id = job.get("job_id")
        if isinstance(job_id, str):
            ids.add(job_id)
    return ids


def _plan_for_job(preview: dict[str, object], job_id: str) -> dict[str, object]:
    for raw_plan in _list(preview.get("planned"), "cleanup plan"):
        plan = _mapping(raw_plan, "cleanup action")
        if plan.get("job_id") == job_id:
            return plan
    raise RelayError(f"stale cleanup result omitted validation target {job_id}")


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RelayError(f"{label} is not an object")
    return {str(key): item for key, item in cast(dict[object, object], value).items()}


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise RelayError(f"{label} is not an array")
    return cast(list[object], value)


def _require_cluster(job: RelayJob, cluster: str) -> None:
    if job.cluster != cluster:
        raise ConfigurationError(
            f"job {job.job_id} belongs to cluster {job.cluster}, not requested cluster {cluster}"
        )


def _validate_options(
    *,
    older_than_seconds: int,
    scan_limit: int,
    scheduler_run_seconds: int,
    scheduler_timeout_seconds: float,
    scheduler_poll_seconds: float,
) -> None:
    if older_than_seconds < 1:
        raise ValueError("older_than_seconds must be at least 1")
    if scan_limit < 1:
        raise ValueError("scan_limit must be at least 1")
    if scheduler_run_seconds < 5 or scheduler_run_seconds > 300:
        raise ValueError("scheduler_run_seconds must be between 5 and 300")
    if not 0 < scheduler_timeout_seconds <= MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS:
        raise ValueError(
            "scheduler_timeout_seconds must be greater than zero and no more than "
            f"{MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS:g}"
        )
    if not 0 < scheduler_poll_seconds <= MAX_VALIDATION_POLL_SECONDS:
        raise ValueError(
            "scheduler_poll_seconds must be greater than zero and no more than "
            f"{MAX_VALIDATION_POLL_SECONDS:g}"
        )


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
        return result.returncode == 0 and f'"{process_id}"' in result.stdout
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _combined_error(primary: Exception | None, cleanup: Exception | None) -> Exception | None:
    if primary is None:
        return cleanup
    if cleanup is None:
        return primary
    return RelayError(f"{primary}; additionally, {cleanup}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RelayError(message)


def _evidence(kind: str, reference: str, payload: dict[str, object]) -> EvidenceReference:
    return EvidenceReference(
        kind=kind,
        reference=reference,
        excerpt=json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")),
    )
