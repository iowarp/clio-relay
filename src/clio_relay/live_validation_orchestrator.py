"""The live queue-management validation entry point.

Owns ``run_queue_management_validation`` -- the single public surface the
``queue_validation`` facade re-exports -- and nothing else. It is one
atomic control flow (submit/observe/cancel/audit against a real worker
fleet and a real scheduler provider, with unconditional best-effort
cleanup) that composes every sibling ``live_validation_*`` owner module
(``_support``/``_process``/``_capacity``/``_jobs``/``_cleanup``); splitting
its own body further would cut across that single sequential narrative
rather than along a real seam, so it stays one module even above the
150-500 sweet spot (same precedent as
``endpoint_recovery_directory.py``/``endpoint_jarvis_recovery.py`` in the
``#231`` decomposition: one real, undividable concern, still comfortably
under the 800-line cap). Moved verbatim out of ``queue_validation.py``
(iowarp/clio-relay#231-style split); no behavior changed.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import cast
from uuid import uuid4

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.live_validation_capacity import (
    _controlled_capacity,
    _controlled_process_containment,
    _require_quiet_validation_queue,
    _validate_lease_capacity_audit,
    _worker_heartbeat_snapshot,
)
from clio_relay.live_validation_cleanup import (
    _cleanup_validation_resources,
    _wait_for_scheduler_phase,
)
from clio_relay.live_validation_constants import (
    VALIDATION_KIND_LIMIT,
    VALIDATION_MINIMUM_TOTAL_CONCURRENCY,
)
from clio_relay.live_validation_jobs import (
    _cancel_optional_anchor,
    _cancel_queued_validation_job,
    _record_job_cleanup,
    _validate_bounded_listing,
    _validate_specific_diagnosis,
    _validate_stale_cleanup,
    _validation_execution_job,
)
from clio_relay.live_validation_process import (
    _latest_cancel_request,
    _scheduler_cancel_events,
    _wait_for_worker_admission_cycle,
    _wait_for_worker_cancellation,
    _wait_for_worker_process_started,
    _WorkerProcessObservation,
)
from clio_relay.live_validation_support import (
    _combined_error,
    _evidence,
    _mapping,
    _require,
    _validate_options,
)
from clio_relay.models import JobKind, JobState, RelayEvent, SchedulerPhase, utc_now
from clio_relay.queue_management import MAX_RESULT_LIMIT, cancel_queue_job, worker_status
from clio_relay.scheduler_providers import SchedulerValidationProvider
from clio_relay.validation_report import (
    CleanupEvidence,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
)


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
        # A just-registered fleet's slot heartbeats race this first read, so retry
        # a transient not-yet-complete generation on the usual bounded budget.
        capacity_deadline = time.monotonic() + scheduler_timeout_seconds
        while True:
            capacity = worker_status(queue, cluster=cluster)
            settled = (
                capacity.get("worker_generation_id") is None
                or capacity.get("worker_generation_complete") is not False
            )
            if settled or time.monotonic() >= capacity_deadline:
                break
            time.sleep(min(scheduler_poll_seconds, max(0.0, capacity_deadline - time.monotonic())))
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
        _validate_lease_capacity_audit(
            recorder,
            queue,
            check_id="queue.lease-capacity-audit-initial",
            cluster=cluster,
        )
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
    capacity_audit_error: Exception | None = None
    try:
        _validate_lease_capacity_audit(
            recorder,
            queue,
            check_id="queue.lease-capacity-audit-final",
            cluster=cluster,
        )
    except Exception as exc:
        capacity_audit_error = exc
    final_error = _combined_error(
        _combined_error(primary_error, cleanup_error),
        capacity_audit_error,
    )
    recorder.finish(final_error)
    return report
