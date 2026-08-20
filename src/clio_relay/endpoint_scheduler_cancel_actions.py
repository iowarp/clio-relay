"""Scheduler cancellation actions: submit cancel, reconcile canceled jobs, and confirm
cancellation.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_cancel_scheduler_jobs`` submits the provider cancel with retry/backoff (the
``scheduler_cancel_max_attempts`` family of class attributes still resident on
``EndpointWorker``); ``_reconcile_canceled_scheduler_jobs``/
``_reconcile_canceled_scheduler_job`` and ``_confirm_scheduler_cancellation`` are the
restart-time confirmation scan; the remaining four are small shared predicates/lookups
(owned job id, bounded task listing, cancel-requested check, provider resolution).
"""

from __future__ import annotations

import subprocess
from typing import Any, cast

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.core_queue import DEFAULT_EXACT_RECORD_LIMIT
from clio_relay.endpoint_scheduler_metadata import (
    _owned_scheduler_job_ids_from_metadata,
    _scheduler_job_ids_from_metadata,
)
from clio_relay.endpoint_worker_environment import (
    _normalized_scheduler_status,
    _scheduler_name_from_job,
    _scheduler_status_is_not_found,
)
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.models import (
    JobState,
    RelayJob,
    RelayTask,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    SchedulerStatus,
    utc_now,
)
from clio_relay.scheduler_providers import (
    SchedulerProvider,
    provider_for_scheduler,
)


class SchedulerCancelActionsMixin:
    """Mixin: SchedulerCancelActions methods split from EndpointWorker (clio-relay#231)."""

    def _cancel_scheduler_jobs(self, job: RelayJob, scheduler_job_ids: list[str]) -> None:
        if not scheduler_job_ids:
            return
        pending = self.queue.ensure_scheduler_cancel_pending(
            job.job_id,
            reason="scheduler_cancel",
        )
        if pending.complete:
            return
        provider = self._scheduler_provider_for_job(job)
        for scheduler_job_id in scheduler_job_ids:
            ownership_verified = self._scheduler_job_id_is_owned(job, scheduler_job_id)
            registration = self.queue.register_scheduler_cancel_identity_once(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                ownership_verified=ownership_verified,
            )
            if not ownership_verified and registration.disposition_created:
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_refused",
                    f"Refused scheduler cancellation without ownership proof: {scheduler_job_id}",
                    payload={
                        "scheduler": _scheduler_name_from_job(job),
                        "scheduler_job_id": scheduler_job_id,
                        "ownership_verified": False,
                    },
                )
        finalized = self.queue.finalize_scheduler_cancel_identities(
            job.job_id,
            cluster=job.cluster,
        )
        if finalized.complete:
            return
        now = utc_now()
        confirmation_dispositions = [
            item
            for item in finalized.dispositions
            if item.state is SchedulerCancelDispositionState.CANCEL_REQUESTED
            and (item.next_attempt_at is None or item.next_attempt_at <= now)
        ]
        for disposition in confirmation_dispositions:
            self._confirm_scheduler_cancellation(
                job,
                provider,
                disposition.scheduler_job_id,
            )
        due_dispositions = [
            item
            for item in finalized.dispositions
            if item.state is SchedulerCancelDispositionState.PENDING
            or (
                item.state is SchedulerCancelDispositionState.RETRY_WAIT
                and (item.next_attempt_at is None or item.next_attempt_at <= now)
            )
        ]
        for disposition in due_dispositions:
            scheduler_job_id = disposition.scheduler_job_id
            claim = self.queue.claim_scheduler_cancel_attempt(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                lease_seconds=self.scheduler_cancel_claim_lease_seconds,
                now=utc_now(),
            )
            if claim is None:
                continue
            try:
                result = provider.cancel(scheduler_job_id)
            except (OSError, RelayError) as exc:
                result = subprocess.CompletedProcess(
                    [provider.name, scheduler_job_id],
                    1,
                    "",
                    str(exc),
                )
            error_detail = bounded_error_detail(result.stderr) if result.stderr else None
            attempt = claim.attempt
            retry_delay = min(
                self.scheduler_cancel_retry_base_seconds * 2 ** (attempt - 1),
                self.scheduler_cancel_retry_max_seconds,
            )
            recorded = self.queue.record_scheduler_cancel_attempt(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                claim_id=claim.claim_id,
                accepted=result.returncode == 0,
                error=error_detail,
                max_attempts=self.scheduler_cancel_max_attempts,
                retry_delay_seconds=retry_delay,
                now=utc_now(),
            )
            if recorded is None:
                continue
            if result.returncode == 0:
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_requested",
                    f"Requested scheduler cancellation: {scheduler_job_id}",
                    payload={
                        "scheduler": provider.name,
                        "scheduler_job_id": scheduler_job_id,
                    },
                )
                self._confirm_scheduler_cancellation(job, provider, scheduler_job_id)
                continue
            self.queue.append_event(
                job.job_id,
                "scheduler.cancel_failed",
                f"Scheduler cancellation failed: {scheduler_job_id}",
                payload={
                    "scheduler": provider.name,
                    "scheduler_job_id": scheduler_job_id,
                    "returncode": result.returncode,
                    "stderr": error_detail,
                    "attempt": attempt,
                    "max_attempts": self.scheduler_cancel_max_attempts,
                    "retryable": attempt < self.scheduler_cancel_max_attempts,
                    "retry_delay_seconds": retry_delay,
                },
            )

    def _reconcile_canceled_scheduler_jobs(self) -> None:
        pending_records, _ = self.queue.scan_due_scheduler_cancellations(
            cluster=self.cluster,
            limit=100,
            now=utc_now(),
        )
        for pending_record in pending_records:
            self._reconcile_canceled_scheduler_job(pending_record)

    def _reconcile_canceled_scheduler_job(
        self,
        pending_record: SchedulerCancelPending,
    ) -> None:
        """Resolve one durable scheduler-cancellation record idempotently."""
        try:
            job = self.queue.get_job(pending_record.job_id)
        except RelayError:
            return
        if pending_record.reason == "operator_request" and not self._scheduler_cancel_was_requested(
            job.job_id
        ):
            try:
                self.queue.complete_scheduler_cancel_identity_scan(
                    job.job_id,
                    cluster=self.cluster,
                    superseded=True,
                )
            except QueueConflictError:
                completed = self.queue.get_scheduler_cancel_disposition(
                    job.job_id,
                    cluster=job.cluster,
                )
                if completed is None:
                    raise
            return
        observed_ids: set[str] = set()
        owned_ids: set[str] = set()
        for task in self._bounded_job_tasks(job.job_id):
            observed_scheduler_job_ids = _scheduler_job_ids_from_metadata(task.metadata)
            scheduler_job_ids = _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            )
            observed_ids.update(observed_scheduler_job_ids)
            owned_ids.update(scheduler_job_ids)
        if pending_record.identity_resolution == "pending":
            provider = self._scheduler_provider_for_job(job)
            newly_refused: list[str] = []
            try:
                for scheduler_job_id in sorted(observed_ids):
                    ownership_verified = scheduler_job_id in owned_ids
                    registration = self.queue.register_scheduler_cancel_identity_once(
                        job.job_id,
                        cluster=job.cluster,
                        scheduler_job_id=scheduler_job_id,
                        provider=provider.name,
                        ownership_verified=ownership_verified,
                    )
                    if not ownership_verified and registration.disposition_created:
                        newly_refused.append(scheduler_job_id)
                if observed_ids:
                    self.queue.finalize_scheduler_cancel_identities(
                        job.job_id,
                        cluster=job.cluster,
                    )
                elif job.state in {
                    JobState.CANCELED,
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                }:
                    self.queue.complete_scheduler_cancel_identity_scan(
                        job.job_id,
                        cluster=job.cluster,
                    )
                    return
                else:
                    return
            except QueueConflictError:
                completed = self.queue.get_scheduler_cancel_disposition(
                    job.job_id,
                    cluster=job.cluster,
                )
                if completed is not None:
                    return
                raise
            for scheduler_job_id in newly_refused:
                self._record_scheduler_cancel_refused(
                    job,
                    scheduler_job_id=scheduler_job_id,
                    metadata_source="unverified_durable_metadata",
                )
        if owned_ids:
            self._cancel_scheduler_jobs(job, sorted(owned_ids))

    def _confirm_scheduler_cancellation(
        self,
        job: RelayJob,
        provider: SchedulerProvider,
        scheduler_job_id: str,
    ) -> None:
        """Poll one accepted cancellation until the exact scheduler id is terminal."""
        claim = self.queue.claim_scheduler_cancel_confirmation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider=provider.name,
            lease_seconds=self.scheduler_cancel_confirmation_claim_lease_seconds,
            now=utc_now(),
        )
        if claim is None:
            return
        try:
            status = provider.poll(scheduler_job_id)
        except RelayError as exc:
            error_detail = bounded_error_detail(str(exc)) or type(exc).__name__
            status = SchedulerStatus(
                scheduler=provider.name,
                scheduler_job_id=scheduler_job_id,
                phase=SchedulerPhase.UNKNOWN,
                reason="scheduler cancellation confirmation failed",
                queue_position_note=error_detail,
            )
        status = _normalized_scheduler_status(
            status,
            expected_scheduler=provider.name,
            expected_scheduler_job_id=scheduler_job_id,
        )
        if status.phase == SchedulerPhase.UNKNOWN:
            status = status.model_copy(
                update={
                    "reason": "scheduler cancellation requested; confirmation pending",
                    "queue_position_note": (
                        status.queue_position_note
                        or "provider did not return a terminal scheduler record yet"
                    ),
                }
            )
        retry_delay = min(
            self.scheduler_cancel_retry_base_seconds * 2 ** (claim.confirmation_attempt - 1),
            self.scheduler_cancel_retry_max_seconds,
        )
        recorded = self.queue.record_scheduler_cancel_observation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider=provider.name,
            claim_id=claim.claim_id,
            phase=status.phase,
            not_found=_scheduler_status_is_not_found(status),
            error=status.queue_position_note,
            max_confirmation_attempts=self.scheduler_cancel_confirmation_max_attempts,
            retry_delay_seconds=retry_delay,
            now=utc_now(),
        )
        if recorded is None:
            return
        self._record_scheduler_status(
            job,
            [scheduler_job_id],
            scheduler_job_id,
            status,
            task_id=None,
        )

    def _scheduler_job_id_is_owned(self, job: RelayJob, scheduler_job_id: str) -> bool:
        return any(
            scheduler_job_id
            in _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            )
            for task in self._bounded_job_tasks(job.job_id)
        )

    def _bounded_job_tasks(self, job_id: str) -> list[RelayTask]:
        """Read an exact job's tasks within the worker safety bound."""
        tasks, truncated = self.queue.scan_job_tasks(
            job_id,
            limit=DEFAULT_EXACT_RECORD_LIMIT,
        )
        if truncated:
            raise RelayError(f"job task index exceeded its safety bound: {job_id}")
        return tasks

    def _scheduler_cancel_was_requested(self, job_id: str) -> bool:
        job = self.queue.get_job(job_id)
        request = job.metadata.get("cancellation_request")
        if isinstance(request, dict):
            typed_request = cast(dict[str, Any], request)
            if typed_request.get("schema_version") == "clio-relay.cancellation-request.v1":
                cancel_scheduler = typed_request.get("cancel_scheduler")
                if isinstance(cancel_scheduler, bool):
                    return cancel_scheduler
        return False

    def _scheduler_provider_for_job(self, job: RelayJob) -> SchedulerProvider:
        runtime_metadata = job.metadata.get("runtime_metadata")
        structured_name: str | None = None
        if isinstance(runtime_metadata, dict):
            candidate = cast(dict[str, Any], runtime_metadata).get("scheduler_provider")
            if isinstance(candidate, str) and candidate.strip():
                structured_name = candidate
        if self.scheduler_provider is not None:
            if structured_name is not None:
                normalized_name = structured_name.strip().lower().replace("_", "-")
                if normalized_name in {"none", "unmanaged"}:
                    normalized_name = "external"
                if normalized_name != self.scheduler_provider.name:
                    raise ConfigurationError(
                        "JARVIS runtime metadata scheduler provider does not match the "
                        f"configured worker provider: {normalized_name} != "
                        f"{self.scheduler_provider.name}"
                    )
            return self.scheduler_provider
        if structured_name is not None:
            return provider_for_scheduler(structured_name)
        return provider_for_scheduler(_scheduler_name_from_job(job))
