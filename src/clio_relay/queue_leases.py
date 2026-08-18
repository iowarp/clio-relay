"""Lease lifecycle ownership: listing, renewal, release, recovery entrypoints.

Owns ``list_leases``, ``scan_leases``, ``scan_job_leases``, ``renew_lease``,
``release_lease``, ``recover_stale_jobs``, ``recover_stale_job``, and
``_reconcile_lease_acquire_intent_unlocked``. The two stale-recovery public
entrypoints land here rather than on ``queue_lease_recovery`` because
``recover_stale_job`` needs this owner's own ``scan_job_leases`` as its first
step; ``queue_lease_recovery`` (this owner's predecessor rank) hosts the
engine those methods call into, plus the shared ``_delete_lease_unlocked``
primitive this owner's ``release_lease``/``recover_stale_job`` also use (see
that module's docstring for the two-owner-cycle rationale). The worker-lane
admission/acquisition path (``acquire_next_job``, ``acquire_job``,
``submit_and_acquire_job``) is a same-rank peer in ``queue_lease_admission``
(#231 file-size split; zero call-graph overlap with this owner).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from clio_relay import queue_context, queue_layout, queue_store_read, queue_store_write
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import JobKind, JobState, Lease, RelayJob


class QueueLeasesMixin:
    """Own lease listing, renewal, release, and the stale-recovery entrypoints."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:
        from clio_relay.queue_lease_records import LeaseIndexIdentity as _StubIdentity

        def initialize(self) -> None: ...
        def _require_index_migration_complete(self) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _job_index_exists(self, job_id: str) -> bool: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _lease_capacity_migration_complete_unlocked(self) -> bool: ...
        def get_job(self, job_id: str) -> RelayJob: ...
        def _lease_index_identity(self, lease: Lease, *, job: RelayJob) -> _StubIdentity: ...
        def _sync_lease_operational_indexes_unlocked(
            self, lease: Lease, *, job: RelayJob, previous_lease: Lease | None = None
        ) -> _StubIdentity: ...
        def _delete_lease_operational_indexes_unlocked(
            self, identity: _StubIdentity, *, allow_foreign_manifest: bool = False
        ) -> None: ...
        def _prepare_lease_capacity_transition_unlocked(
            self,
            *,
            scope_deltas: dict[tuple[str, JobKind], int],
            include_rollback: bool = False,
        ) -> dict[str, object]: ...
        def _apply_lease_capacity_transition_unlocked(
            self, transition_value: object, *, target: Literal["after", "rollback"], label: str
        ) -> object: ...
        def _before_lease_capacity_intent_removal(self, _kind: str, _path: Path) -> None: ...
        def _recover_stale_jobs_unlocked(
            self, *, cluster: str, max_attempts: int
        ) -> list[RelayJob]: ...
        def _recover_expired_leases_unlocked(
            self, job: RelayJob, expired: list[Lease], *, max_attempts: int
        ) -> RelayJob: ...
        def _delete_lease_unlocked(
            self,
            lease: Lease,
            *,
            job: RelayJob | None = None,
            intent_path: Path | None = None,
            identity: _StubIdentity | None = None,
            finalize_intent: bool = True,
        ) -> None: ...
        def _job_has_pending_execution_cleanup_after_migration_unlocked(
            self, cluster: str, job_id: str
        ) -> bool: ...
        def _job_has_scheduler_observation_unlocked(self, job: RelayJob) -> bool: ...
        def _sync_job_derived_unlocked(self, job: RelayJob) -> None: ...

    def list_leases(self, cluster: str | None = None) -> list[Lease]:
        """Return active and expired leases, optionally filtered by job cluster."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            leases = list(
                queue_store_read.read_many(
                    self._storage_root / "leases",
                    Lease,
                    identity_field="lease_id",
                )
            )
            if cluster is not None:
                matched: list[Lease] = []
                for lease in leases:
                    try:
                        job = self.get_job(lease.job_id)
                    except NotFoundError:
                        continue
                    if job.cluster == cluster:
                        matched.append(lease)
                leases = matched
            return sorted(leases, key=lambda lease: lease.acquired_at)

    def scan_leases(
        self,
        *,
        limit: int,
        cluster: str | None = None,
    ) -> tuple[list[Lease], bool]:
        """Read a bounded durable lease snapshot."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            leases, truncated = queue_store_read.scan_many(
                self._storage_root / "leases",
                Lease,
                limit=limit,
                identity_field="lease_id",
            )
            if cluster is not None:
                matched: list[Lease] = []
                for lease in leases:
                    try:
                        job = self.get_job(lease.job_id)
                    except NotFoundError:
                        continue
                    if job.cluster == cluster:
                        matched.append(lease)
                leases = matched
            return sorted(leases, key=lambda lease: lease.acquired_at), truncated

    def scan_job_leases(self, job_id: str, *, limit: int) -> tuple[list[Lease], bool]:
        """Read bounded leases from the exact per-job index under writer exclusion."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            directory = (
                self._storage_root / "leases_by_job" / queue_layout.QueueLayout.durable_key(job_id)
            )
            if self._job_index_exists(job_id):
                leases, truncated = queue_store_read.scan_many(directory, Lease, limit=limit)
                return sorted(leases, key=lambda lease: lease.acquired_at), truncated
            leases, truncated = queue_store_read.scan_many(
                self._storage_root / "leases", Lease, limit=limit
            )
            return [lease for lease in leases if lease.job_id == job_id], truncated

    def renew_lease(self, lease_id: str, *, ttl_seconds: int = 300) -> Lease | None:
        """Extend an active lease TTL."""
        lease_id = queue_layout.QueueLayout.require_durable_record_id(lease_id, field="lease_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "leases" / f"{lease_id}.json"
            lease = self._store_adapter.read_optional(path, Lease)
            if lease is None:
                return None
            if lease.lease_id != lease_id:
                raise QueueConflictError(f"canonical lease identity mismatch: {path}")
            job = self.get_job(lease.job_id)
            renewed = Lease.new(lease.job_id, lease.endpoint_id, ttl_seconds)
            renewed = renewed.model_copy(update={"lease_id": lease.lease_id})
            capacity_transition = self._prepare_lease_capacity_transition_unlocked(scope_deltas={})
            intent_path = self._write_transition_intent_unlocked(
                "lease_sync",
                renewed.lease_id,
                {
                    "lease": renewed.model_dump(mode="json"),
                    "previous_lease": lease.model_dump(mode="json"),
                    "job": job.model_dump(mode="json"),
                    "lease_capacity_transition": capacity_transition,
                },
            )
            self._store_adapter.write(path, renewed)
            self._store_adapter.write(
                self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
                renewed,
            )
            self._sync_lease_operational_indexes_unlocked(
                renewed,
                job=job,
                previous_lease=lease,
            )
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"lease renewal {renewed.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_sync", intent_path)
            queue_store_write.unlink_durable_path(intent_path, missing_ok=True)
            return renewed

    def recover_stale_jobs(self, *, cluster: str, max_attempts: int = 3) -> list[RelayJob]:
        """Requeue or fail jobs whose worker lease expired."""
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            return self._recover_stale_jobs_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )

    def recover_stale_job(
        self,
        job_id: str,
        *,
        cluster: str,
        max_attempts: int = 3,
    ) -> RelayJob | None:
        """Recover exactly one job when its durable worker lease is expired."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.cluster != cluster:
                raise QueueConflictError(
                    f"job {job_id} belongs to cluster {job.cluster}, "
                    f"not requested cluster {cluster}"
                )
            job_leases, leases_truncated = self.scan_job_leases(
                job_id,
                limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT,
            )
            if leases_truncated:
                raise QueueConflictError(f"job lease index exceeded its safety bound: {job_id}")
            expired = [lease for lease in job_leases if lease.is_expired()]
            if not expired:
                return None
            if job.state not in {JobState.LEASED, JobState.RUNNING}:
                for lease in expired:
                    self._delete_lease_unlocked(lease, job=job)
                return None
            if self._job_has_pending_execution_cleanup_after_migration_unlocked(
                cluster,
                job.job_id,
            ):
                return None
            if self._job_has_scheduler_observation_unlocked(job):
                return None
            return self._recover_expired_leases_unlocked(
                job,
                expired,
                max_attempts=max_attempts,
            )

    def release_lease(self, lease_id: str) -> None:
        """Remove a lease record."""
        lease_id = queue_layout.QueueLayout.require_durable_record_id(lease_id, field="lease_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "leases" / f"{lease_id}.json"
            lease = self._store_adapter.read_optional(path, Lease)
            if lease is not None:
                if lease.lease_id != lease_id:
                    raise QueueConflictError(f"canonical lease identity mismatch: {path}")
                self._delete_lease_unlocked(lease)

    def _reconcile_lease_acquire_intent_unlocked(
        self,
        path: Path,
        payload: dict[str, object],
    ) -> None:
        """Abort an interrupted lease handoff unless later canonical work superseded it."""
        lease = Lease.model_validate(payload.get("lease"))
        original_job = RelayJob.model_validate(payload.get("original_job"))
        target_job = RelayJob.model_validate(payload.get("target_job"))
        target_updated_at = payload.get("target_updated_at")
        if (
            lease.job_id != original_job.job_id
            or target_job.job_id != original_job.job_id
            or target_job.cluster != original_job.cluster
            or target_job.kind != original_job.kind
            or target_job.state is not JobState.LEASED
            or target_job.leased_by != lease.endpoint_id
            or target_job.updated_at.isoformat() != target_updated_at
            or not isinstance(target_updated_at, str)
        ):
            raise QueueConflictError(f"lease acquisition intent identity mismatch: {path}")
        current = self._store_adapter.read_optional(
            self._storage_root / "jobs" / f"{lease.job_id}.json",
            RelayJob,
        )
        target_is_current = (
            current is not None
            and current.updated_at.isoformat() == target_updated_at
            and current.state is JobState.LEASED
            and current.leased_by == lease.endpoint_id
        )
        if target_is_current:
            self._store_adapter.write(
                self._storage_root / "jobs" / f"{original_job.job_id}.json", original_job
            )
            self._sync_job_derived_unlocked(original_job)
        lease_path = self._storage_root / "leases" / f"{lease.lease_id}.json"
        indexed_path = self._job_record_path("leases_by_job", lease.job_id, lease.lease_id)
        identity = self._lease_index_identity(lease, job=original_job)
        preserve_acquisition = (
            current is not None
            and not target_is_current
            and (
                current.state in {JobState.LEASED, JobState.RUNNING}
                and current.leased_by == lease.endpoint_id
            )
        )
        if preserve_acquisition:
            assert current is not None
            self._store_adapter.write(lease_path, lease)
            self._store_adapter.write(indexed_path, lease)
            self._sync_lease_operational_indexes_unlocked(lease, job=current)
        else:
            queue_store_write.unlink_durable_path(lease_path, missing_ok=True)
            queue_store_write.unlink_durable_path(indexed_path, missing_ok=True)
            self._delete_lease_operational_indexes_unlocked(
                identity,
                allow_foreign_manifest=True,
            )
        capacity_transition = payload.get("lease_capacity_transition")
        if capacity_transition is not None:
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after" if preserve_acquisition else "rollback",
                label=f"lease acquisition recovery {lease.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_acquire", path)
        elif self._lease_capacity_migration_complete_unlocked():
            raise QueueConflictError(f"lease acquisition intent has no capacity transition: {path}")
        queue_store_write.unlink_durable_path(path, missing_ok=True)
