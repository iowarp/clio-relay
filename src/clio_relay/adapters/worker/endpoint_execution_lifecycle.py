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

import os
import socket

from clio_relay import process_containment
from clio_relay.adapters.worker.core_queue import DEFAULT_EXACT_RECORD_LIMIT
from clio_relay.models import (
    JobState,
    RelayJob,
    RelayTask,
    utc_now,
)

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
        self.queue.update_task_metadata(task.task_id, {"execution_ownership": execution})
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
            eligible += 1
            try:
                process_id = self._terminate_recorded_execution(
                    task,
                    allow_unstarted=True,
                )
                task = self.queue.get_task(task.task_id)
                self._reconcile_recorded_scheduler_submission(job, task)
                cancellation_requested = job.state == JobState.CANCELED or isinstance(
                    job.metadata.get("cancellation_request"),
                    dict,
                )
                cleanup_metadata = self._remove_recorded_execution_sidecars(job, task)
                if task.state not in {
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                    JobState.CANCELED,
                }:
                    target_state = JobState.CANCELED if cancellation_requested else JobState.FAILED
                    self.queue.update_task_state(
                        task.task_id,
                        target_state,
                        message=(
                            f"Recovered task cancellation after worker restart: {task.name}"
                            if cancellation_requested
                            else f"Closed stale execution attempt after worker restart: {task.name}"
                        ),
                        metadata={"restart_cleanup_recovered": True},
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
