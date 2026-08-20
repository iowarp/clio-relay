"""Execution-start bookkeeping and the pending-execution-cleanup reconciliation scan.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_append_execution_start`` records the launched process's pid;
``_reconcile_pending_execution_cleanup``/``_record_execution_cleanup_scan`` are the
restart-time scan over tasks whose sidecar cleanup never completed, batched by
``EXECUTION_CLEANUP_SCAN_LIMIT``.

``EXECUTION_CLEANUP_SCAN_LIMIT`` moves here with its only two call sites rather than
staying an ``endpoint.py`` constant: ``tests/test_endpoint.py`` monkeypatches it
(``monkeypatch.setattr(endpoint_execution_lifecycle, "EXECUTION_CLEANUP_SCAN_LIMIT",
...)``), and a bare-name read only observes a patch on the module its own globals
resolve through -- the same rule the #231 endpoint-split slice 6 note in
``scripts/check_file_size.py`` documents for ``jarvis_mcp_command``. ``endpoint.py``
re-exports the name verbatim so any other reader of
``endpoint.EXECUTION_CLEANUP_SCAN_LIMIT`` keeps working unchanged.
"""

from __future__ import annotations

import functools
import os
import socket

from clio_relay import process_containment
from clio_relay.core_queue import DEFAULT_EXACT_RECORD_LIMIT
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_dispatch_refusal_detail,
    _durable_jarvis_execution_recovery,
    _durable_runtime_recovery_state,
)
from clio_relay.endpoint_recovery_directory import (
    _jarvis_execution_recovery_retry_due,
)
from clio_relay.endpoint_scheduler_metadata import (
    _owned_scheduler_job_ids_from_metadata,
)
from clio_relay.endpoint_sidecar_types import (
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.endpoint_worker_lanes import (
    quarantine_relay_error,
)
from clio_relay.errors import RelayError
from clio_relay.jarvis_dispatch_failure import (
    JARVIS_DISPATCH_REFUSAL_RESOLUTION,
)
from clio_relay.models import (
    JobState,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)
from clio_relay.spool import JobSpool

EXECUTION_CLEANUP_SCAN_LIMIT = DEFAULT_EXACT_RECORD_LIMIT + 1


class ExecutionLifecycleMixin:
    """Mixin: ExecutionLifecycle methods split from EndpointWorker (clio-relay#231)."""

    def _append_execution_start(self, job: RelayJob, task: RelayTask, pid: int) -> None:
        start_identity = process_containment.process_start_identity(pid)
        if start_identity is None:
            start_identity = f"process-not-observed:{pid}"
        started_at = utc_now().isoformat()
        execution = {
            "schema_version": "clio-relay.execution-ownership.v1",
            "pid": pid,
            "hostname": socket.gethostname(),
            "process_start_identity": start_identity,
            "process_group_id": pid if os.name != "nt" else None,
            "started_at": started_at,
            "endpoint_id": None if self.endpoint is None else self.endpoint.endpoint_id,
            "containment": process_containment.owned_process_metadata(pid),
        }
        current_task = self.queue.get_task(task.task_id)
        recovery_intent = _durable_jarvis_execution_recovery(job, current_task)
        start_metadata: dict[str, object] = {"execution_ownership": execution}
        if recovery_intent is not None and recovery_intent["state"] == "pending":
            start_metadata["jarvis_execution_recovery"] = {
                **recovery_intent,
                "dispatch_state": "started",
                "dispatch_started_at": started_at,
            }
        self.queue.update_task_metadata(task.task_id, start_metadata)
        self.queue.append_event(
            job.job_id,
            "execution.started",
            f"JARVIS-CD process started: {pid}",
            payload={
                "pid": pid,
                "hostname": execution["hostname"],
                "process_group_id": execution["process_group_id"],
                "task_id": task.task_id,
            },
        )

    def _reconcile_pending_execution_cleanup(self) -> None:
        """Retry durable cleanup for attempts whose worker lease is no longer live."""
        pending, has_more = self.queue.scan_execution_cleanup(
            cluster=self.cluster,
            limit=EXECUTION_CLEANUP_SCAN_LIMIT,
        )
        eligible = 0
        completed = 0
        failures: list[str] = []
        for marker in pending:
            task = self.queue.get_task(marker.task_id)
            repair_metadata: dict[str, object] = {}
            for key in ("execution_sidecars", "execution_cleanup"):
                if key not in task.metadata and key in marker.metadata:
                    repair_metadata[key] = marker.metadata[key]
            if repair_metadata:
                task = self.queue.update_task_metadata(task.task_id, repair_metadata)
            job = self.queue.get_job(task.job_id)
            leases, leases_truncated = self.queue.scan_job_leases(
                job.job_id,
                limit=EXECUTION_CLEANUP_SCAN_LIMIT,
            )
            if leases_truncated:
                failures.append(f"{task.task_id}: lease scan exceeded its safety bound")
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_cleanup_failed",
                    "Restart cleanup could not prove the job lease set",
                    payload={"task_id": task.task_id, "has_more": has_more},
                )
                continue
            if any(not lease.is_expired() for lease in leases):
                continue
            # clio-relay#238: this fetch sits outside the per-marker
            # try/except below and previously killed the calling worker slot
            # on one poisoned record -- see quarantine_relay_error's docstring.
            recovery_intent = quarantine_relay_error(
                functools.partial(_durable_jarvis_execution_recovery, job, task),
                queue=self.queue,
                job_id=job.job_id,
                task_id=task.task_id,
                context="execution_cleanup_reconciliation",
            )
            if (
                recovery_intent is not None
                and recovery_intent["state"] == "pending"
                and not _jarvis_execution_recovery_retry_due(recovery_intent)
            ):
                continue
            eligible += 1
            try:
                recovered_dispatch = False
                dispatch_not_released = False
                dispatch_refused = False
                recovered_runtime: JarvisRuntimeMetadata | None = None
                recovery_spool: JobSpool | None = None
                process_id = self._terminate_recorded_execution(
                    task,
                    allow_unstarted=True,
                )
                task = self.queue.get_task(task.task_id)
                self._terminate_recorded_jarvis_recovery_query(job, task)
                task = self.queue.get_task(task.task_id)
                recovery_intent = _durable_jarvis_execution_recovery(job, task)
                if (
                    recovery_intent is not None
                    and recovery_intent["state"] == "pending"
                    and recovery_intent["dispatch_state"] == "prepared"
                ):
                    dispatch_not_released = True
                    self.queue.append_event(
                        job.job_id,
                        "jarvis.dispatch_not_released",
                        "Restart cleanup proved the JARVIS dispatch was never released",
                        payload={
                            "task_id": task.task_id,
                            "execution_id": recovery_intent["execution_id"],
                            "recovery_query_attempted": False,
                        },
                    )
                elif recovery_intent is not None and recovery_intent["state"] == "pending":
                    recovery_spool = JobSpool(
                        self.settings.spool_dir,
                        job,
                        max_log_bytes_per_stream=(self.settings.spool_max_log_bytes_per_stream),
                        max_log_bytes_per_job=self.settings.spool_max_log_bytes_per_job,
                    )
                    previous_runtime, previous_digests = _durable_runtime_recovery_state(task)
                    recovered_state: list[JarvisRuntimeMetadata | None] = [previous_runtime]
                    recovered_scheduler_job_ids = _owned_scheduler_job_ids_from_metadata(
                        task.metadata,
                        relay_job_id=job.job_id,
                        task_id=task.task_id,
                    )
                    recorded_refusal = self._recorded_jarvis_dispatch_refusal(
                        job,
                        spool=recovery_spool,
                    )
                    with self._jarvis_execution_recovery_claim(job, task=task):
                        if recorded_refusal is not None:
                            self._refuse_jarvis_execution_recovery(
                                job,
                                task_id=task.task_id,
                                spool=recovery_spool,
                                refusal=recorded_refusal,
                            )
                            dispatch_refused = True
                        else:
                            recovered_dispatch = self._recover_jarvis_execution(
                                job,
                                task_id=task.task_id,
                                spool=recovery_spool,
                                state=recovered_state,
                                digests=previous_digests,
                                scheduler_job_ids=recovered_scheduler_job_ids,
                            )
                    recovered_runtime = recovered_state[0]
                    task = self.queue.get_task(task.task_id)
                    recovery_intent = _durable_jarvis_execution_recovery(job, task)
                elif (
                    recovery_intent is not None
                    and recovery_intent.get("resolution") == JARVIS_DISPATCH_REFUSAL_RESOLUTION
                ):
                    dispatch_refused = True
                elif recovery_intent is not None:
                    recovery_spool = JobSpool(
                        self.settings.spool_dir,
                        job,
                        max_log_bytes_per_stream=(self.settings.spool_max_log_bytes_per_stream),
                        max_log_bytes_per_job=self.settings.spool_max_log_bytes_per_job,
                    )
                    recovered_runtime = self._validated_recovered_jarvis_dispatch(
                        job,
                        task=task,
                        spool=recovery_spool,
                    )
                    recovered_dispatch = True
                if recovery_intent is None:
                    self._reconcile_recorded_scheduler_submission(job, task)
                elif dispatch_not_released or dispatch_refused:
                    pass
                elif recovery_intent["state"] != "resolved" or not recovered_dispatch:
                    raise SchedulerSubmissionUnresolvedError(
                        "JARVIS execution response recovery remains pending"
                    )
                cancellation_requested = job.state == JobState.CANCELED or isinstance(
                    job.metadata.get("cancellation_request"),
                    dict,
                )
                if (
                    cancellation_requested
                    and self._scheduler_cancel_was_requested(job.job_id)
                    and recovery_intent is not None
                    and not dispatch_not_released
                    and not dispatch_refused
                ):
                    owned_ids = self._durable_scheduler_job_ids(
                        job,
                        task.task_id,
                        [],
                    )
                    if owned_ids:
                        self._cancel_scheduler_jobs(job, owned_ids)
                    else:
                        self._record_scheduler_cancel_refused(job)
                cleanup_metadata = self._remove_recorded_execution_sidecars(job, task)
                if recovered_dispatch and not cancellation_requested:
                    if recovery_spool is None or recovered_runtime is None:
                        raise RelayError(
                            "resolved JARVIS execution recovery omitted its durable result"
                        )
                    self._finalize_recovered_jarvis_dispatch(
                        job,
                        task=task,
                        spool=recovery_spool,
                        runtime_metadata=recovered_runtime,
                    )
                    task = self.queue.get_task(task.task_id)
                if task.state not in {
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                    JobState.CANCELED,
                }:
                    target_state = (
                        JobState.SUCCEEDED
                        if recovered_dispatch and not cancellation_requested
                        else JobState.CANCELED
                        if cancellation_requested
                        else JobState.FAILED
                    )
                    self.queue.update_task_state(
                        task.task_id,
                        target_state,
                        message=(
                            f"Recovered durable JARVIS response: {task.name}"
                            if target_state is JobState.SUCCEEDED
                            else f"Recovered task cancellation after worker restart: {task.name}"
                            if cancellation_requested
                            else f"Closed stale execution attempt after worker restart: {task.name}"
                        ),
                        metadata={
                            "restart_cleanup_recovered": True,
                            "mcp_dispatch_recovered": recovered_dispatch,
                        },
                    )
                if recovered_dispatch and not cancellation_requested:
                    current_job = self.queue.get_job(job.job_id)
                    if current_job.state is not JobState.SUCCEEDED:
                        self.queue.update_job_state(
                            job.job_id,
                            JobState.SUCCEEDED,
                            message="JARVIS MCP response recovered from durable execution",
                        )
                elif dispatch_refused and not cancellation_requested:
                    current_job = self.queue.get_job(job.job_id)
                    if current_job.state not in {
                        JobState.FAILED,
                        JobState.CANCELED,
                    }:
                        self.queue.update_job_state(
                            job.job_id,
                            JobState.FAILED,
                            message="JARVIS run failed",
                            error=_durable_jarvis_dispatch_refusal_detail(task),
                        )
                elif dispatch_not_released and not cancellation_requested:
                    current_job = self.queue.get_job(job.job_id)
                    if current_job.state not in {
                        JobState.FAILED,
                        JobState.CANCELED,
                    }:
                        self.queue.update_job_state(
                            job.job_id,
                            JobState.FAILED,
                            message="JARVIS dispatch was never released",
                            error="owned JARVIS process did not reach dispatch release",
                        )
                self.queue.acknowledge_execution_cleanup(
                    job.job_id,
                    task.task_id,
                    metadata={
                        **cleanup_metadata,
                        "restart_cleanup_acknowledged": True,
                        "restart_cleanup_at": utc_now().isoformat(),
                    },
                )
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_reconciled",
                    "Prior worker execution and sidecars were proven cleaned",
                    payload={
                        "task_id": task.task_id,
                        "pid": process_id,
                        "hostname": socket.gethostname(),
                        "cancellation_requested": cancellation_requested,
                        "has_more": has_more,
                    },
                )
                if cancellation_requested and job.state != JobState.CANCELED:
                    self.queue.acknowledge_job_cancellation(job.job_id)
                    self.queue.append_event(
                        job.job_id,
                        "job.cancel_acknowledged",
                        "Cancellation acknowledged after restart cleanup",
                    )
                completed += 1
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                failures.append(f"{task.task_id}: {detail}")
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_cleanup_failed",
                    "Restart cleanup failed and remains queued for retry",
                    payload={
                        "task_id": task.task_id,
                        "error": detail,
                        "has_more": has_more,
                    },
                )
        self._record_execution_cleanup_scan(
            batch_size=len(pending),
            eligible=eligible,
            completed=completed,
            failed=len(failures),
            has_more=has_more,
        )

    def _record_execution_cleanup_scan(
        self,
        *,
        batch_size: int,
        eligible: int,
        completed: int,
        failed: int,
        has_more: bool,
    ) -> None:
        """Publish bounded cleanup progress in the durable worker registration."""
        if self.endpoint is None:
            return
        metadata = dict(self.endpoint.metadata)
        metadata["execution_cleanup_scan"] = {
            "schema_version": "clio-relay.execution-cleanup-scan.v1",
            "observed_at": utc_now().isoformat(),
            "batch_limit": EXECUTION_CLEANUP_SCAN_LIMIT,
            "batch_size": batch_size,
            "eligible": eligible,
            "completed": completed,
            "failed": failed,
            "has_more": has_more,
        }
        self.endpoint = self.queue.register_endpoint(
            self.endpoint.model_copy(update={"metadata": metadata})
        )
