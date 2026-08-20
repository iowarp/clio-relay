"""Terminal-release wrapping for state transitions, recovery, and leasing.

Owns :class:`StorageManagedQueueLeasingMixin`, the method group covering
every path that can move a job into a terminal state outside the admission
boundary (``update_job_state``, cancellation, stale-lease recovery) plus the
three worker-facing lease-acquisition entry points
(``acquire_next_job``/``acquire_job``/``submit_and_acquire_job``) and their
shared unlocked snapshot/replay helpers.

Extends :class:`~clio_relay.core_queue.ClioCoreQueue` directly (see
:mod:`storage_managed_queue_admission` for why); composed into
``StorageManagedQueue`` alongside
:class:`~clio_relay.storage_managed_queue_admission.StorageManagedQueueAdmissionMixin`
and never instantiated on its own. ``submit_job`` and ``_release_reservation``
live on that sibling mixin -- reached here only through ``self`` at runtime
via the composed class's real MRO, so their cross-mixin calls are declared
under ``TYPE_CHECKING`` rather than imported.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING

from clio_relay.core_queue import (
    MAX_ACTIVE_JOB_RECORDS,
    MAX_LIVE_LEASE_RECORDS,
    ClioCoreQueue,
)
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    RelayJob,
)
from clio_relay.queue_lease_admission import (
    _job_matches_mcp_admission_class,  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.worker_concurrency import KindConcurrencyInput, normalize_kind_concurrency

_MANAGED_UNSET = object()


class StorageManagedQueueLeasingMixin(ClioCoreQueue):
    """Own terminal-release wrapping for transitions, recovery, and leasing."""

    if TYPE_CHECKING:

        def submit_job(self, job: RelayJob) -> RelayJob: ...
        def _release_reservation(self, job_id: str, *, terminal_job: RelayJob | None) -> None: ...

    @contextmanager
    def _acquire_lock_with_replay(self) -> Generator[None]:
        """Replay under the core lock and release terminal storage after unlocking."""
        replayed: list[RelayJob] = []
        try:
            with self._lock:  # pyright: ignore[reportPrivateUsage]
                replayed = self._recover_pending_transitions_unlocked()  # pyright: ignore[reportPrivateUsage]
                yield
        finally:
            for job in replayed:
                if job.state in TERMINAL_STATES:
                    self._release_reservation(job.job_id, terminal_job=job)

    def _lease_admission_snapshot_unlocked(
        self,
        endpoint_id: str,
        *,
        cluster: str,
    ) -> tuple[Lease | None, dict[JobKind, int], int]:
        refs, truncated = self._scan_expiry_refs(  # pyright: ignore[reportPrivateUsage]
            limit=MAX_LIVE_LEASE_RECORDS
        )
        if truncated:
            raise QueueConflictError("lease expiry index exceeded its safety bound")
        active = self._active_lease_for_endpoint(  # pyright: ignore[reportPrivateUsage]
            endpoint_id,
            expiry_refs=refs,
        )
        counts, global_total = self._lease_capacity_snapshot(  # pyright: ignore[reportPrivateUsage]
            cluster=cluster,
            expiry_refs=refs,
        )
        return active, counts, global_total

    def update_job_state(
        self,
        job_id: str,
        state: JobState,
        *,
        message: str | None = None,
        error: str | None = None,
        leased_by: str | None | object = _MANAGED_UNSET,
    ) -> RelayJob:
        """Release a reservation immediately after a terminal state commits."""
        if leased_by is _MANAGED_UNSET:
            saved = super().update_job_state(
                job_id,
                state,
                message=message,
                error=error,
            )
        else:
            saved = super().update_job_state(
                job_id,
                state,
                message=message,
                error=error,
                leased_by=leased_by,
            )
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved

    def cancel_job_if_active(
        self,
        job_id: str,
        *,
        cancel_scheduler: bool,
        expected_state: JobState | None = None,
        expected_updated_at: datetime | None = None,
    ) -> tuple[RelayJob, bool]:
        """Release storage after an atomic queued-job cancellation terminalizes."""
        saved, changed = super().cancel_job_if_active(
            job_id,
            cancel_scheduler=cancel_scheduler,
            expected_state=expected_state,
            expected_updated_at=expected_updated_at,
        )
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved, changed

    def acknowledge_job_cancellation(self, job_id: str) -> RelayJob:
        """Release storage after cancellation cleanup reaches its terminal commit."""
        saved = super().acknowledge_job_cancellation(job_id)
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved

    def recover_stale_jobs(self, *, cluster: str, max_attempts: int = 3) -> list[RelayJob]:
        """Release reservations for jobs failed by bounded stale-lease recovery."""
        recovered = super().recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        for job in recovered:
            if job.state in TERMINAL_STATES:
                self._release_reservation(job.job_id, terminal_job=job)
        return recovered

    def recover_stale_job(
        self,
        job_id: str,
        *,
        cluster: str,
        max_attempts: int = 3,
    ) -> RelayJob | None:
        """Release storage when exact stale-lease recovery terminalizes a job."""
        recovered = super().recover_stale_job(
            job_id,
            cluster=cluster,
            max_attempts=max_attempts,
        )
        if recovered is not None and recovered.state in TERMINAL_STATES:
            self._release_reservation(recovered.job_id, terminal_job=recovered)
        return recovered

    def acquire_next_job(
        self,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
        mcp_admission_class: McpAdmissionClass | None = None,
        mcp_admission_limit: int | None = None,
    ) -> Lease | None:
        """Recover stale work, then lease atomically from one strict worker lane."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        if mcp_admission_class is not None and not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            mcp_admission_class,
            McpAdmissionClass,
        ):
            raise ConfigurationError("worker MCP admission class is invalid")
        if mcp_admission_limit is not None:
            if mcp_admission_class is None:
                raise ConfigurationError("worker MCP admission limit requires an admission class")
            if (
                isinstance(mcp_admission_limit, bool)
                or not isinstance(mcp_admission_limit, int)  # pyright: ignore[reportUnnecessaryIsInstance]
                or mcp_admission_limit < 1
            ):
                raise ConfigurationError("worker MCP admission limit must be at least 1")
        self.recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        self.initialize()
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=cluster,
            )
            if active is not None:
                active_job = self.get_job(active.job_id)
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    active_job,
                    mcp_admission_class,
                ):
                    raise QueueConflictError(
                        "endpoint active lease does not match its MCP admission lane: "
                        f"{endpoint_id}"
                    )
                return active
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            mcp_admission_at_limit = False
            active_mcp_workload_count: int | None = None
            if mcp_admission_class is not None and mcp_admission_limit is not None:
                mcp_admission_at_limit = (
                    self._active_mcp_admission_count_unlocked(
                        cluster=cluster,
                        admission_class=mcp_admission_class,
                        expiry_refs=None,
                    )
                    >= mcp_admission_limit
                )
            queued_jobs, truncated = self._scan_many(  # pyright: ignore[reportPrivateUsage]
                self._storage_root / "jobs_queued",  # pyright: ignore[reportPrivateUsage]
                RelayJob,
                limit=MAX_ACTIVE_JOB_RECORDS,
            )
            if truncated:
                raise QueueConflictError("queued job index exceeded its safety bound")
            for queued in sorted(
                queued_jobs,
                key=self._job_submission_order_key_unlocked,  # pyright: ignore[reportPrivateUsage]
            ):
                if queued.cluster != cluster or queued.state is not JobState.QUEUED:
                    continue
                if queued.kind is JobKind.INPUT_INGEST:
                    continue
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    queued,
                    mcp_admission_class,
                ):
                    continue
                if mcp_admission_at_limit and queued.kind is JobKind.MCP_CALL:
                    continue
                if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                    queued.cluster,
                    queued.job_id,
                ):
                    continue
                kind_limit = normalized.get(queued.kind)
                active_kind_count = active_counts.get(queued.kind, 0)
                if queued.kind is JobKind.MCP_CALL and mcp_admission_class is not None:
                    if mcp_admission_class is McpAdmissionClass.CONTROL_QUERY:
                        kind_limit = None
                    else:
                        if active_mcp_workload_count is None:
                            active_mcp_workload_count = self._active_mcp_admission_count_unlocked(
                                cluster=cluster,
                                admission_class=McpAdmissionClass.WORKLOAD,
                                expiry_refs=None,
                            )
                        active_kind_count = active_mcp_workload_count
                if kind_limit is not None and active_kind_count >= kind_limit:
                    continue
                return self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                    queued,
                    endpoint_id,
                    ttl_seconds=ttl_seconds,
                    validated_global_total=global_lease_total,
                )
        return None

    def acquire_job(
        self,
        job_id: str,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> Lease | None:
        """Recover first, then lease only the exact requested job."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        self.recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        self.initialize()
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=cluster,
            )
            if active is not None:
                return active if active.job_id == job_id else None
            job = self.get_job(job_id)
            if job.cluster != cluster or job.state is not JobState.QUEUED:
                return None
            if job.kind is JobKind.INPUT_INGEST:
                return None
            if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                job.cluster,
                job.job_id,
            ):
                return None
            kind_limit = normalized.get(job.kind)
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            if kind_limit is not None and active_counts.get(job.kind, 0) >= kind_limit:
                return None
            return self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                job,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )

    def submit_and_acquire_job(
        self,
        job: RelayJob,
        endpoint_id: str,
        *,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> tuple[RelayJob, Lease | None]:
        """Reserve outside the core lock, then attempt an exact controlled lease."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        submitted = self.submit_job(job)
        if submitted.kind is JobKind.INPUT_INGEST:
            return submitted, None
        self.recover_stale_jobs(cluster=submitted.cluster, max_attempts=max_attempts)
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=submitted.cluster,
            )
            if active is not None:
                return submitted, active if active.job_id == submitted.job_id else None
            current = self.get_job(submitted.job_id)
            if current.state is not JobState.QUEUED:
                return current, None
            if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                current.cluster,
                current.job_id,
            ):
                return current, None
            kind_limit = normalized.get(current.kind)
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return current, None
            if kind_limit is not None and active_counts.get(current.kind, 0) >= kind_limit:
                return current, None
            lease = self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                current,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )
            return self.get_job(current.job_id), lease
