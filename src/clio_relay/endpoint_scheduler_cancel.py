"""Scheduler-job-id capture, cancellation predicates, execution timeout, and scheduler
status polling.

Owner module for iowarp/clio-relay#231's endpoint decomposition. Covers scraping
scheduler job ids from stdout (``_capture_scheduler_job_ids``), the
should-cancel/cancellation-requested predicates the running process polls
(``_should_cancel_job``/``_job_cancellation_requested``), the execution-timeout handler,
refused-cancel bookkeeping, and the durable scheduler-status refresh/record pair.
"""

from __future__ import annotations

import time
from typing import Any, cast

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.endpoint_jarvis_recovery import (
    _jarvis_execution_recovery_is_pending,
    _trusted_jarvis_mcp_route,
)
from clio_relay.endpoint_scheduler_metadata import (
    _owned_scheduler_job_ids_from_metadata,
    _task_id_for_scheduler_job,
    _task_scheduler_status,
)
from clio_relay.endpoint_worker_environment import (
    _extract_scheduler_job_id,
    _normalized_scheduler_status,
    _scheduler_name_from_job,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    JobKind,
    McpCallSpec,
    RelayJob,
    SchedulerStatus,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
    legacy_scheduler_runtime_metadata,
)


class SchedulerCancelMixin:
    """Mixin: SchedulerCancel methods split from EndpointWorker (clio-relay#231)."""

    def _capture_scheduler_job_ids(
        self,
        job: RelayJob,
        text: str,
        scheduler_job_ids: list[str],
        *,
        scheduler_task_id: str | None,
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] | None,
        runtime_metadata_digests: set[str] | None,
    ) -> None:
        if (
            runtime_metadata_state is not None
            and runtime_metadata_state[0] is not None
            and runtime_metadata_state[0].source
            in {
                RuntimeMetadataSource.JARVIS_MCP,
                RuntimeMetadataSource.JARVIS_SIDECAR,
            }
        ):
            return
        for line in text.splitlines():
            job_id = _extract_scheduler_job_id(line)
            if job_id is None or job_id in scheduler_job_ids:
                continue
            if (
                scheduler_task_id is not None
                and runtime_metadata_state is not None
                and runtime_metadata_digests is not None
            ):
                self._persist_runtime_metadata(
                    job,
                    task_id=scheduler_task_id,
                    metadata=legacy_scheduler_runtime_metadata(
                        scheduler_job_id=job_id,
                        scheduler_provider=_scheduler_name_from_job(job) or "external",
                    ),
                    state=runtime_metadata_state,
                    digests=runtime_metadata_digests,
                    scheduler_job_ids=scheduler_job_ids,
                )

    def _should_cancel_job(
        self,
        job: RelayJob,
        *,
        task_id: str,
        scheduler_job_ids: list[str],
        scheduler_cancel_attempted: list[bool],
    ) -> bool:
        canceled = self._job_cancellation_requested(job.job_id)
        if not canceled or scheduler_cancel_attempted[0]:
            return canceled
        if self._scheduler_cancel_was_requested(job.job_id):
            owned_ids = self._durable_scheduler_job_ids(job, task_id, scheduler_job_ids)
            if owned_ids:
                scheduler_cancel_attempted[0] = True
                self._cancel_scheduler_jobs(job, owned_ids)
            elif _jarvis_execution_recovery_is_pending(
                job,
                self.queue.get_task(task_id),
            ):
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_identity_pending",
                    "Explicit scheduler cancellation is waiting for JARVIS ownership recovery",
                    payload={"task_id": task_id},
                )
            else:
                scheduler_cancel_attempted[0] = True
                self._record_scheduler_cancel_refused(job)
        return True

    def _job_cancellation_requested(self, job_id: str) -> bool:
        """Return whether an active or acknowledged job has a durable cancel request."""
        request = self.queue.get_job(job_id).metadata.get("cancellation_request")
        return isinstance(request, dict)

    def _handle_execution_timeout(
        self,
        job: RelayJob,
        *,
        task_id: str,
        scheduler_job_ids: list[str],
        scheduler_cancel_attempted: list[bool],
    ) -> None:
        route_valid, _route_reason = _trusted_jarvis_mcp_route(job)
        if route_valid:
            self.queue.append_event(
                job.job_id,
                "mcp.dispatch_timeout",
                "JARVIS MCP dispatch exceeded timeout_seconds; workload ownership will be queried",
                payload={
                    "task_id": task_id,
                    "scheduler_cancel_requested": False,
                },
            )
            return
        if job.kind is JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec):
            self.queue.append_event(
                job.job_id,
                "mcp.dispatch_timeout",
                "Endpoint MCP operation exceeded timeout_seconds; contained process terminated",
                payload={
                    "task_id": task_id,
                    "scheduler_cancel_requested": False,
                },
            )
            return
        durable_scheduler_job_ids = self._durable_scheduler_job_ids(
            job,
            task_id,
            scheduler_job_ids,
        )
        self.queue.append_event(
            job.job_id,
            "execution.timeout",
            "JARVIS-CD process exceeded timeout_seconds",
            payload={"scheduler_job_ids": durable_scheduler_job_ids},
        )
        self.queue.ensure_scheduler_cancel_pending(
            job.job_id,
            reason="execution_timeout",
        )
        if durable_scheduler_job_ids and not scheduler_cancel_attempted[0]:
            self._cancel_scheduler_jobs(job, durable_scheduler_job_ids)
            scheduler_cancel_attempted[0] = True
        elif not durable_scheduler_job_ids and not scheduler_cancel_attempted[0]:
            self._record_scheduler_cancel_refused(job)
            self.queue.complete_scheduler_cancel_identity_scan(
                job.job_id,
                cluster=job.cluster,
            )
            scheduler_cancel_attempted[0] = True

    def _record_scheduler_cancel_refused(
        self,
        job: RelayJob,
        *,
        scheduler_job_id: str | None = None,
        metadata_source: str | None = None,
    ) -> None:
        runtime_metadata = job.metadata.get("runtime_metadata")
        observed_scheduler_job_id = scheduler_job_id
        if observed_scheduler_job_id is None and isinstance(runtime_metadata, dict):
            typed_runtime = cast(dict[str, Any], runtime_metadata)
            candidate = typed_runtime.get("scheduler_job_id")
            if isinstance(candidate, str):
                observed_scheduler_job_id = candidate
            source = typed_runtime.get("source")
            if metadata_source is None and isinstance(source, str):
                metadata_source = source
        if observed_scheduler_job_id is None:
            return
        self.queue.append_event(
            job.job_id,
            "scheduler.cancel_refused",
            "Refused scheduler cancellation because no owned scheduler identity was available",
            payload={
                "scheduler_job_id": observed_scheduler_job_id,
                "metadata_source": metadata_source,
                "ownership_verified": False,
            },
        )

    def _refresh_scheduler_status(
        self,
        job: RelayJob,
        scheduler_job_ids: list[str],
        *,
        task_id: str | None,
        force: bool = False,
    ) -> None:
        provider = self._scheduler_provider_for_job(job)
        for scheduler_job_id in scheduler_job_ids:
            poll_key = (job.job_id, scheduler_job_id)
            now = time.monotonic()
            last_poll = self._scheduler_last_poll.get(poll_key)
            if (
                not force
                and last_poll is not None
                and now - last_poll < self.scheduler_poll_interval_seconds
            ):
                continue
            self._scheduler_last_poll[poll_key] = now
            try:
                status = provider.poll(scheduler_job_id)
            except RelayError as exc:
                error_detail = bounded_error_detail(str(exc)) or type(exc).__name__
                self.queue.append_event(
                    job.job_id,
                    "scheduler.poll_failed",
                    f"Scheduler status polling failed for {scheduler_job_id}: {error_detail}",
                    payload={
                        "scheduler": provider.name,
                        "scheduler_job_id": scheduler_job_id,
                        "error": error_detail,
                    },
                )
                continue
            self._record_scheduler_status(
                job,
                scheduler_job_ids,
                scheduler_job_id,
                status,
                task_id=task_id,
            )

    def _record_scheduler_status(
        self,
        job: RelayJob,
        scheduler_job_ids: list[str],
        scheduler_job_id: str,
        status: SchedulerStatus,
        *,
        task_id: str | None,
    ) -> None:
        provider = self._scheduler_provider_for_job(job)
        status = _normalized_scheduler_status(
            status,
            expected_scheduler=provider.name,
            expected_scheduler_job_id=scheduler_job_id,
        )
        tasks = self._bounded_job_tasks(job.job_id)
        target_task_id = task_id or _task_id_for_scheduler_job(tasks, scheduler_job_id)
        if target_task_id is None:
            return
        previous = _task_scheduler_status(
            tasks,
            target_task_id,
            scheduler_job_id,
        )
        status_payload = status.model_dump(mode="json")
        self.queue.update_task_metadata(
            target_task_id,
            {
                "scheduler": status.scheduler,
                "scheduler_job_ids": list(scheduler_job_ids),
                "scheduler_status": status_payload,
            },
        )
        previous_phase = previous.get("phase") if previous is not None else None
        if previous_phase == status.phase.value:
            return
        self.queue.append_event(
            job.job_id,
            f"scheduler.{status.phase.value}",
            f"Scheduler job {scheduler_job_id} is {status.phase.value}",
            payload=status_payload,
        )

    def _durable_scheduler_job_ids(
        self,
        job: RelayJob,
        task_id: str,
        scheduler_job_ids: list[str],
    ) -> list[str]:
        ids: list[str] = []
        for task in self._bounded_job_tasks(job.job_id):
            if task.task_id != task_id:
                continue
            for item in _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            ):
                if item not in ids:
                    ids.append(item)
        for scheduler_job_id in scheduler_job_ids:
            if scheduler_job_id in ids:
                continue
            if self._scheduler_job_id_is_owned(job, scheduler_job_id):
                ids.append(scheduler_job_id)
        return ids
