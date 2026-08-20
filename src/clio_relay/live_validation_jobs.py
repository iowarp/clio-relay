"""Per-job validation checks and lifecycle helpers for queue validation.

Owns the "one relay job's own lifecycle proof" concern the live fixture
walks through: canceling the optional expendable anchor job, listing a
bounded JARVIS queue window, diagnosing one exact stale queued job with a
coherent reason, previewing then executing exact stale cleanup, canceling a
still-queued validation job, building the bounded validation command job
itself (the script that emits its own child PID marker then sleeps),
recording one job's cleanup evidence/action once, and the small listing/
plan lookup helpers those checks share. Moved verbatim out of
``queue_validation.py`` (iowarp/clio-relay#231-style split); no behavior
changed.
"""

from __future__ import annotations

import sys
from datetime import datetime

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.live_validation_constants import (
    VALIDATION_COMMAND_SECONDS,
    VALIDATION_MARKER_SCHEMA,
)
from clio_relay.live_validation_support import (
    _evidence,
    _list,
    _mapping,
    _require,
    _require_cluster,
)
from clio_relay.models import JarvisRunSpec, JobKind, JobState, RelayJob, utc_now
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    list_queue_jobs,
)
from clio_relay.validation_report import ValidationRecorder, ValidationResource


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
        admission_evidence = _mapping(queue_evidence.get("admission"), "queue admission")
        lease_evidence = _mapping(diagnosis.get("lease"), "diagnosis lease")
        worker_evidence = _mapping(diagnosis.get("worker"), "diagnosis worker")
        scheduler_evidence = _list(diagnosis.get("scheduler"), "scheduler evidence")
        _require(job.get("job_id") == target.job_id, "diagnosis returned another job")
        _require(
            diagnosis.get("reason") == "waiting_for_kind_capacity",
            f"unexpected diagnosis reason: {diagnosis.get('reason')}",
        )
        _require(diagnosis.get("stale") is True, "diagnosis did not mark target stale")
        _require(
            queue_evidence.get("state") == JobState.QUEUED.value
            and queue_evidence.get("jobs_ahead") == 0,
            "kind-capacity reason conflicts with queue position",
        )
        _require(
            admission_evidence.get("analysis_complete") is True
            and admission_evidence.get("target_admissible_now") is False
            and admission_evidence.get("target_ineligibility") == "kind_capacity_saturated",
            "kind-capacity reason conflicts with admission evidence",
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
                    "admission": admission_evidence,
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
