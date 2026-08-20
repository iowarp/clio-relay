"""Endpoint serve loop: single/multi-slot polling, lease renewal, and the single-cluster
worker lock.

Owner module for iowarp/clio-relay#231's endpoint decomposition (``EndpointWorker``
split). Covers ``run_once``'s outer poll-and-run cycle, unhandled-job-failure
bookkeeping, ``serve_forever`` and its multi-slot fan-out
(``_serve_worker_slots``/``_serve_worker_slot``), lease-renewal cadence
(``_renew_lease_if_needed``), the single-cluster worker file lock
(``_single_cluster_worker_lock``), and ``_run_job`` (the per-job entry point
``run_once`` calls: sidecar cleanup around ``self._run_job_impl``, on success or failure
alike).

``EndpointWorker`` (still resident in ``endpoint.py``) composes this mixin; every method
here calls sibling ``EndpointWorker`` methods (``self._run_job_impl``,
``self._reconcile_pending_execution_cleanup``, ...) resolved through the composed
instance's MRO, not through any import.
"""

from __future__ import annotations

import concurrent.futures
import functools
import os
import socket
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock, Timeout

from clio_relay import process_containment
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _close_runtime_sidecar_anchors,
    _execution_cleanup_ack_metadata,
    _remove_execution_sidecars,
)
from clio_relay.endpoint_jarvis_recovery import (
    _jarvis_execution_recovery_is_pending,
)
from clio_relay.endpoint_sidecar_types import (
    EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS,
    SchedulerSubmissionUnresolvedError,
    _RuntimeSidecarAnchor,
)
from clio_relay.endpoint_worker_environment import (
    _worker_installation_snapshot,
)
from clio_relay.endpoint_worker_lanes import (
    run_worker_lane_iteration,
)
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
    logical_filesystem_text,
)
from clio_relay.identifiers import filesystem_key
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JobState,
    Lease,
    McpAdmissionClass,
    RelayJob,
)
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import (
    StorageRuntimeViolation,
)
from clio_relay.worker_concurrency import (
    kind_concurrency_metadata,
)


class ServeLoopMixin:
    """Mixin: ServeLoop methods split from EndpointWorker (clio-relay#231)."""

    def run_once(
        self,
        *,
        mcp_admission_class: McpAdmissionClass = McpAdmissionClass.WORKLOAD,
        mcp_admission_limit: int | None = None,
    ) -> RelayJob | None:
        """Run one job from a strict workload or reserved MCP control lane."""
        self._require_open_queue_identity()
        if self.role != EndpointRole.WORKER:
            return None
        endpoint = self.endpoint or self.register()
        self.endpoint = self.queue.register_endpoint(endpoint)
        self.queue.recover_stale_jobs(cluster=self.cluster)
        workload_lane = mcp_admission_class is McpAdmissionClass.WORKLOAD
        if workload_lane:
            self._reconcile_canceled_scheduler_jobs()
        if (
            workload_lane
            and self.reconcile_execution_cleanup
            and self._foreground_jobs_since_cleanup >= EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS
        ):
            self._reconcile_pending_execution_cleanup()
            self._foreground_jobs_since_cleanup = 0
        lease = self.queue.acquire_next_job(
            endpoint.endpoint_id,
            cluster=self.cluster,
            ttl_seconds=self.lease_ttl_seconds,
            kind_concurrency=self.kind_concurrency,
            mcp_admission_class=mcp_admission_class,
            mcp_admission_limit=mcp_admission_limit,
        )
        if lease is None:
            if workload_lane and self.reconcile_execution_cleanup:
                self._reconcile_pending_execution_cleanup()
                self._foreground_jobs_since_cleanup = 0
            return None
        job = self.queue.get_job(lease.job_id)
        try:
            try:
                self._run_job(job, lease)
            except Exception as exc:
                self._record_unhandled_job_failure(job, exc)
        finally:
            self.queue.release_lease(lease.lease_id)
        if workload_lane and self.reconcile_execution_cleanup:
            self._foreground_jobs_since_cleanup += 1
        return self.queue.get_job(job.job_id)

    def _record_unhandled_job_failure(self, job: RelayJob, error: Exception) -> None:
        detail = logical_filesystem_text(f"{type(error).__name__}: {error}")
        current = self.queue.get_job(job.job_id)
        if current.state == JobState.CANCELED:
            self.queue.append_event(
                job.job_id,
                "worker.job_error_after_cancel",
                "Worker caught an execution error after cancellation",
                payload={"error": detail},
            )
            return
        if isinstance(error, SchedulerSubmissionUnresolvedError):
            pending_recovery_task_ids: list[str] = []
            for task in self._bounded_job_tasks(job.job_id):
                try:
                    recovery_pending = _jarvis_execution_recovery_is_pending(
                        current,
                        task,
                    )
                except RelayError:
                    recovery_pending = False
                if recovery_pending:
                    pending_recovery_task_ids.append(task.task_id)
            if pending_recovery_task_ids:
                self.queue.append_event(
                    job.job_id,
                    "jarvis.execution_reconciliation_deferred",
                    "Relay response remains nonterminal while JARVIS ownership is queried",
                    payload={
                        "task_ids": pending_recovery_task_ids,
                        "error": detail,
                        "scheduler_cancel_requested": self._scheduler_cancel_was_requested(
                            job.job_id
                        ),
                    },
                )
                return
        if isinstance(current.metadata.get("cancellation_request"), dict):
            self.queue.append_event(
                job.job_id,
                "cancellation.cleanup_failed",
                "Worker cancellation cleanup failed; job will fail rather than acknowledge cancel",
                payload={"error": detail},
            )
        for task in self._bounded_job_tasks(job.job_id):
            if task.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}:
                continue
            self.queue.update_task_state(
                task.task_id,
                JobState.FAILED,
                message=f"Task failed after unhandled worker error: {detail}",
                metadata={"worker_error": detail},
            )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message=f"Worker execution failed: {detail}",
            error=detail,
        )

    def serve_forever(self, *, poll_seconds: float = 2.0) -> None:
        """Run the endpoint loop until interrupted."""
        self._require_open_queue_identity()
        self.register()
        if self.role == EndpointRole.DESKTOP:
            while True:
                time.sleep(poll_seconds)
        with self._single_cluster_worker_lock():
            if self.concurrency > 1:
                self._serve_worker_slots(poll_seconds=poll_seconds)
                return
            while True:
                self.run_once()
                time.sleep(poll_seconds)

    def _serve_worker_slots(self, *, poll_seconds: float) -> None:
        slot_admission_classes = [
            *([McpAdmissionClass.WORKLOAD] * self.workload_concurrency),
            *([McpAdmissionClass.CONTROL_QUERY] * self.control_query_concurrency),
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [
                executor.submit(
                    self._serve_worker_slot,
                    index,
                    poll_seconds,
                    admission_class,
                )
                for index, admission_class in enumerate(slot_admission_classes)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def _serve_worker_slot(
        self,
        index: int,
        poll_seconds: float,
        admission_class: McpAdmissionClass,
    ) -> None:
        from clio_relay.endpoint import EndpointWorker

        workload_lane = admission_class is McpAdmissionClass.WORKLOAD
        worker = EndpointWorker(
            role=self.role,
            settings=self.settings,
            cluster=self.cluster,
            concurrency=1,
            control_query_concurrency=0,
            kind_concurrency=self.kind_concurrency,
            queue=self.queue,
            scheduler_provider=self.scheduler_provider,
            storage_runtime=self.storage_runtime,
            reconcile_execution_cleanup=workload_lane and index == 0,
        )
        endpoint = EndpointRegistration(
            role=self.role,
            cluster=self.cluster,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            metadata={
                "worker_slot": index,
                "parent_endpoint_id": None if self.endpoint is None else self.endpoint.endpoint_id,
                "concurrency": 1,
                "workload_concurrency": 1 if workload_lane else 0,
                "control_query_concurrency": 0 if workload_lane else 1,
                "mcp_admission_class": admission_class.value,
                "kind_concurrency": kind_concurrency_metadata(self.kind_concurrency),
                "process_containment": process_containment.containment_capability(),
                "installation_info": _worker_installation_snapshot(),
                "scheduler_provider": (
                    self.scheduler_provider.name
                    if self.scheduler_provider is not None
                    else "external"
                ),
            },
        )
        worker.endpoint = self.queue.register_endpoint(endpoint)
        registered_endpoint = worker.endpoint
        admission_limit = (
            self.control_query_concurrency
            if admission_class is McpAdmissionClass.CONTROL_QUERY
            else None
        )
        while True:
            # clio-relay#238: contain any otherwise-unhandled exception here
            # instead of a silent per-slot thread death (endpoint_worker_lanes).
            registered_endpoint = run_worker_lane_iteration(
                functools.partial(
                    worker.run_once,
                    mcp_admission_class=admission_class,
                    mcp_admission_limit=admission_limit,
                ),
                queue=self.queue,
                endpoint=registered_endpoint,
            )
            worker.endpoint = registered_endpoint
            time.sleep(poll_seconds)

    def _run_job(self, job: RelayJob, lease: Lease) -> None:
        sidecars: list[Path] = []
        sidecar_anchors: dict[Path, _RuntimeSidecarAnchor] = {}
        sidecar_task_ids: list[str] = []
        runtime_spools: list[JobSpool] = []
        primary_error: BaseException | None = None
        try:
            self._run_job_impl(
                job,
                lease,
                sidecars=sidecars,
                sidecar_anchors=sidecar_anchors,
                sidecar_task_ids=sidecar_task_ids,
                runtime_spools=runtime_spools,
            )
        except BaseException as exc:
            primary_error = exc
        if (
            primary_error is not None
            and not isinstance(primary_error, StorageRuntimeViolation)
            and runtime_spools
        ):
            try:
                self._check_runtime_storage(
                    job,
                    runtime_spools[0],
                    force_job_scan=True,
                )
            except BaseException as storage_error:
                primary_error = RelayError(
                    f"{primary_error}; final storage guard also failed: {storage_error}"
                )
        cleanup_error: Exception | None = None
        if sidecars:
            if isinstance(primary_error, SchedulerSubmissionUnresolvedError):
                _close_runtime_sidecar_anchors(sidecar_anchors)
                self.queue.append_event(
                    job.job_id,
                    "scheduler.reconciliation_pending",
                    "Execution evidence is retained until scheduler or direct intent resolves",
                    payload={
                        "task_ids": list(sidecar_task_ids),
                        "sidecar_count": len(sidecars),
                    },
                )
            else:
                try:
                    quarantined = _remove_execution_sidecars(
                        sidecars,
                        spool_path=self.settings.spool_dir / job.job_id,
                        expected_anchors=sidecar_anchors,
                        on_quarantined=lambda source, quarantine: (
                            self._stage_execution_sidecar_quarantine(
                                job.job_id,
                                sidecar_task_ids,
                                source,
                                quarantine,
                            )
                        ),
                    )
                    for task_id in sidecar_task_ids:
                        task = self.queue.get_task(task_id)
                        self.queue.acknowledge_execution_cleanup(
                            job.job_id,
                            task_id,
                            metadata=_execution_cleanup_ack_metadata(task, quarantined),
                        )
                    self.queue.append_event(
                        job.job_id,
                        "execution.sidecars_quarantined",
                        "Relay execution sidecars securely quarantined",
                        payload={"sidecar_count": len(sidecars)},
                    )
                except Exception as exc:
                    cleanup_error = exc
        if self.storage_runtime is not None:
            self.storage_runtime.forget_running_job(job.job_id)
        if primary_error is not None and cleanup_error is not None:
            raise RelayError(
                f"{primary_error}; execution sidecar cleanup also failed: {cleanup_error}"
            ) from primary_error
        if cleanup_error is not None:
            raise cleanup_error
        if primary_error is not None:
            raise primary_error

    def _renew_lease_if_needed(self, lease: Lease, last_renewed_at: list[float]) -> None:
        now = time.monotonic()
        if now - last_renewed_at[0] < self.lease_renew_seconds:
            return
        if self.endpoint is None:
            raise QueueConflictError(
                f"worker endpoint disappeared before lease heartbeat: {lease.endpoint_id}"
            )
        if self.endpoint.endpoint_id != lease.endpoint_id:
            raise QueueConflictError(
                "worker endpoint identity does not match the running lease: "
                f"{self.endpoint.endpoint_id} != {lease.endpoint_id}"
            )
        self.endpoint = self.queue.register_endpoint(self.endpoint)
        renewed = self.queue.renew_lease(
            lease.lease_id,
            ttl_seconds=self.lease_ttl_seconds,
        )
        if renewed is None:
            raise QueueConflictError(
                f"running lease disappeared before heartbeat: {lease.lease_id}"
            )
        if renewed.job_id != lease.job_id or renewed.endpoint_id != lease.endpoint_id:
            raise QueueConflictError(
                f"running lease identity changed before heartbeat: {lease.lease_id}"
            )
        last_renewed_at[0] = now

    @contextmanager
    def _single_cluster_worker_lock(self) -> Generator[None, None, None]:
        cluster_key = filesystem_key(self.cluster, domain="cluster")
        lock_path = self.settings.core_dir / f"{cluster_key}-worker.lock"
        lock = FileLock(str(internal_filesystem_path(lock_path)), timeout=0)
        try:
            lock.acquire()
        except Timeout as exc:
            raise ConfigurationError(
                f"another {self.cluster} endpoint worker is already active"
            ) from exc
        try:
            yield
        finally:
            lock.release()
