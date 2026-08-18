"""Lease admission/acquisition ownership: the worker-lane leasing path.

Owns ``acquire_next_job``, ``acquire_job``, ``submit_and_acquire_job``, and
their private collaborators (``_lease_job_unlocked``, the MCP admission-lane
matcher/counter, the per-endpoint active-lease lookup). Split out of
``queue_leases.py`` (facade surface #231 file-size gate, #774 ratchet):
admission has zero call-graph dependency on that owner's listing/renewal/
release/recovery-entrypoint methods and vice versa, so the split is a clean
peer separation, not a forced one -- both land at the same CQ15 rank.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from clio_relay import (
    queue_context,
    queue_layout,
    queue_lease_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.models import (
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
    RelayJob,
    utc_now,
)
from clio_relay.worker_concurrency import KindConcurrencyInput, normalize_kind_concurrency

_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseExpiryReference = queue_layout.LeaseExpiryReference


def _job_matches_mcp_admission_class(
    job: RelayJob,
    admission_class: McpAdmissionClass,
) -> bool:
    """Match one durable job to a strict MCP worker lane.

    Non-MCP and kind/spec-mismatched jobs remain workload so the ordinary lane
    can fail them explicitly. They can never enter the privileged control
    lane.
    """
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        return admission_class is McpAdmissionClass.WORKLOAD
    return job.spec.admission_class is admission_class


class QueueLeaseAdmissionMixin:
    """Own the worker-lane lease admission/acquisition path."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def initialize(self) -> None: ...
        def _require_index_migration_complete(self) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _job_has_pending_execution_cleanup_unlocked(
            self, cluster: str, job_id: str
        ) -> bool: ...
        def _job_submission_order_key_unlocked(self, job: RelayJob) -> tuple[int, object, str]: ...
        def get_job(self, job_id: str) -> RelayJob: ...
        def submit_job(self, job: RelayJob) -> RelayJob: ...
        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> object: ...
        def _write_job_unlocked(self, job: RelayJob) -> None: ...
        def _validate_lease_index_identity(
            self, lease: Lease, identity: _LeaseIndexIdentity
        ) -> None: ...
        def _read_lease_index_identity_by_token(
            self, lease_token: str, identity_token: str | None = None
        ) -> _LeaseIndexIdentity: ...
        def _sync_lease_operational_indexes_unlocked(
            self, lease: Lease, *, job: RelayJob, previous_lease: Lease | None = None
        ) -> _LeaseIndexIdentity: ...
        def _scan_expiry_refs(self, *, limit: int) -> tuple[list[_LeaseExpiryReference], bool]: ...
        def _scan_lease_endpoint_refs(
            self, endpoint_id: str, *, limit: int
        ) -> tuple[list[tuple[str, str]], bool]: ...
        def _require_empty_lease_ref(self, path: Path, *, label: str) -> None: ...
        def _lease_identity_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_cluster_kind_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_expiry_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_capacity_snapshot(
            self, *, cluster: str, expiry_refs: list[_LeaseExpiryReference] | None = None
        ) -> tuple[dict[JobKind, int], int]: ...
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
        def _recover_stale_jobs_for_admission_unlocked(
            self, *, cluster: str, max_attempts: int
        ) -> list[_LeaseExpiryReference] | None: ...

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
        """Lease the next queued job accepted by one atomic worker lane.

        ``mcp_admission_class`` is a strict lane filter.  Workload lanes accept
        every non-MCP job plus workload-class MCP jobs; control lanes accept
        only explicitly classified MCP control queries.  The optional limit is
        checked against active durable leases while the same queue lock selects
        and leases the next job.
        """
        endpoint_id = queue_layout.QueueLayout.require_durable_record_id(
            endpoint_id, field="endpoint_id"
        )
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
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
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
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
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= queue_layout.MAX_LIVE_LEASE_RECORDS:
                return None
            mcp_admission_at_limit = False
            active_mcp_workload_count: int | None = None
            if mcp_admission_class is not None and mcp_admission_limit is not None:
                mcp_admission_at_limit = (
                    self._active_mcp_admission_count_unlocked(
                        cluster=cluster,
                        admission_class=mcp_admission_class,
                        expiry_refs=reusable_expiry_refs,
                    )
                    >= mcp_admission_limit
                )
            queued_jobs, _ = queue_store_read.scan_many(
                self._storage_root / "jobs_queued",
                RelayJob,
                limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            )
            for job in sorted(queued_jobs, key=self._job_submission_order_key_unlocked):
                if job.cluster != cluster or job.state != JobState.QUEUED:
                    continue
                if job.kind is JobKind.INPUT_INGEST:
                    continue
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    job,
                    mcp_admission_class,
                ):
                    continue
                if mcp_admission_at_limit and job.kind is JobKind.MCP_CALL:
                    continue
                if self._job_has_pending_execution_cleanup_unlocked(job.cluster, job.job_id):
                    continue
                kind_limit = normalized_kind_concurrency.get(job.kind)
                active_kind_count = active_counts.get(job.kind, 0)
                if job.kind is JobKind.MCP_CALL and mcp_admission_class is not None:
                    if mcp_admission_class is McpAdmissionClass.CONTROL_QUERY:
                        # Control queries have their own explicit, atomic admission cap.
                        # A workload MCP ceiling must never consume the reserved lane.
                        kind_limit = None
                    else:
                        if active_mcp_workload_count is None:
                            active_mcp_workload_count = self._active_mcp_admission_count_unlocked(
                                cluster=cluster,
                                admission_class=McpAdmissionClass.WORKLOAD,
                                expiry_refs=reusable_expiry_refs,
                            )
                        active_kind_count = active_mcp_workload_count
                if kind_limit is not None and active_kind_count >= kind_limit:
                    continue
                return self._lease_job_unlocked(
                    job,
                    endpoint_id,
                    ttl_seconds=ttl_seconds,
                    validated_global_total=global_lease_total,
                )
        return None

    def _active_mcp_admission_count_unlocked(
        self,
        *,
        cluster: str,
        admission_class: McpAdmissionClass,
        expiry_refs: list[_LeaseExpiryReference] | None,
    ) -> int:
        """Count one MCP admission class from bounded, validated live leases."""
        if expiry_refs is None:
            expiry_refs, truncated = self._scan_expiry_refs(
                limit=queue_layout.MAX_LIVE_LEASE_RECORDS
            )
            if truncated:
                raise QueueConflictError("lease expiry index exceeded its safety bound")
        cluster_token = queue_lease_records.lease_cluster_token(cluster)
        count = 0
        for (
            expires_key,
            indexed_cluster,
            job_kind,
            endpoint_token,
            job_token,
            lease_token,
            identity_token,
        ) in expiry_refs:
            if indexed_cluster != cluster_token or job_kind is not JobKind.MCP_CALL:
                continue
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            if (
                identity.cluster != cluster
                or identity.job_kind is not JobKind.MCP_CALL
                or queue_lease_records.lease_endpoint_token(identity.endpoint_id) != endpoint_token
                or queue_lease_records.lease_job_token(identity.job_id) != job_token
                or queue_lease_records.lease_expiry_key(identity.expires_at) != expires_key
            ):
                raise QueueConflictError(
                    f"lease expiry admission identity mismatch: {identity.lease_id}"
                )
            lease = self._store_adapter.read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(
                    f"lease expiry admission index is orphaned: {identity.lease_id}"
                )
            self._validate_lease_index_identity(lease, identity)
            job = self.get_job(identity.job_id)
            if (
                job.cluster != cluster
                or job.kind is not JobKind.MCP_CALL
                or job.leased_by != identity.endpoint_id
            ):
                raise QueueConflictError(
                    f"active MCP admission lease changed job identity: {identity.lease_id}"
                )
            if _job_matches_mcp_admission_class(job, admission_class):
                count += 1
        return count

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
        """Atomically lease one exact queued job when its kind has capacity.

        Unlike :meth:`acquire_next_job`, this method never leases a different
        operator or validation workload while attempting an exact admission.
        """
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        endpoint_id = queue_layout.QueueLayout.require_durable_record_id(
            endpoint_id, field="endpoint_id"
        )
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
            )
            if active is not None:
                if active.job_id == job_id:
                    return active
                return None
            job = self.get_job(job_id)
            if job.cluster != cluster or job.state != JobState.QUEUED:
                return None
            if job.kind is JobKind.INPUT_INGEST:
                return None
            if self._job_has_pending_execution_cleanup_unlocked(job.cluster, job.job_id):
                return None
            kind_limit = normalized_kind_concurrency.get(job.kind)
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= queue_layout.MAX_LIVE_LEASE_RECORDS:
                return None
            if kind_limit is not None and active_counts.get(job.kind, 0) >= kind_limit:
                return None
            return self._lease_job_unlocked(
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
        """Atomically submit and attempt an exact lease for controlled admission.

        The submitted job remains queued when its configured kind limit is
        saturated. Holding the queue lock across both operations prevents a
        worker from executing a bounded admission probe between submission and
        the exact lease decision.
        """
        queue_layout.QueueLayout.require_durable_record_id(job.job_id, field="job_id")
        endpoint_id = queue_layout.QueueLayout.require_durable_record_id(
            endpoint_id, field="endpoint_id"
        )
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            submitted = self.submit_job(job)
            if submitted.cluster != job.cluster:
                raise QueueConflictError(
                    f"submitted job {submitted.job_id} changed cluster identity"
                )
            if submitted.kind is JobKind.INPUT_INGEST:
                return submitted, None
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=submitted.cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
            )
            if active is not None:
                return submitted, active if active.job_id == submitted.job_id else None
            submitted = self.get_job(submitted.job_id)
            if submitted.state != JobState.QUEUED:
                return submitted, None
            if self._job_has_pending_execution_cleanup_unlocked(
                submitted.cluster,
                submitted.job_id,
            ):
                return submitted, None
            kind_limit = normalized_kind_concurrency.get(submitted.kind)
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=submitted.cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= queue_layout.MAX_LIVE_LEASE_RECORDS:
                return submitted, None
            if kind_limit is not None and active_counts.get(submitted.kind, 0) >= kind_limit:
                return submitted, None
            lease = self._lease_job_unlocked(
                submitted,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )
            return self.get_job(submitted.job_id), lease

    def _lease_job_unlocked(
        self,
        job: RelayJob,
        endpoint_id: str,
        *,
        ttl_seconds: int,
        validated_global_total: int | None = None,
    ) -> Lease:
        """Persist one lease and its job transition while the queue lock is held."""
        if job.kind is JobKind.INPUT_INGEST:
            raise QueueConflictError("input ingest jobs are never worker-leased")
        if validated_global_total is None:
            _counts, validated_global_total = self._lease_capacity_snapshot(cluster=job.cluster)
        if validated_global_total >= queue_layout.MAX_LIVE_LEASE_RECORDS:
            raise QueueConflictError(
                "active lease population reached its safety bound of "
                f"{queue_layout.MAX_LIVE_LEASE_RECORDS} records"
            )
        lease = Lease.new(job.job_id, endpoint_id, ttl_seconds)
        leased_job = job.model_copy(
            update={
                "state": JobState.LEASED,
                "leased_by": endpoint_id,
                "attempts": job.attempts + 1,
                "updated_at": utc_now(),
            }
        )
        capacity_transition = self._prepare_lease_capacity_transition_unlocked(
            scope_deltas={(job.cluster, job.kind): 1},
            include_rollback=True,
        )
        intent_path = self._write_transition_intent_unlocked(
            "lease_acquire",
            lease.lease_id,
            {
                "job_id": job.job_id,
                "lease": lease.model_dump(mode="json"),
                "original_job": job.model_dump(mode="json"),
                "target_job": leased_job.model_dump(mode="json"),
                "target_updated_at": leased_job.updated_at.isoformat(),
                "lease_capacity_transition": capacity_transition,
            },
        )
        self._write_job_unlocked(leased_job)
        self._store_adapter.write(self._storage_root / "leases" / f"{lease.lease_id}.json", lease)
        self._store_adapter.write(
            self._job_record_path("leases_by_job", job.job_id, lease.lease_id), lease
        )
        self._sync_lease_operational_indexes_unlocked(lease, job=leased_job)
        self._after_lease_operational_index_write(lease)
        self._apply_lease_capacity_transition_unlocked(
            capacity_transition,
            target="after",
            label=f"lease acquisition {lease.lease_id}",
        )
        self._before_lease_capacity_intent_removal("lease_acquire", intent_path)
        queue_store_write.unlink_durable_path(intent_path, missing_ok=True)
        self.append_event(
            job.job_id,
            "job.leased",
            f"Job leased by {endpoint_id}",
            locked=True,
            payload={"lease_id": lease.lease_id},
        )
        return lease

    def _after_lease_operational_index_write(self, _lease: Lease) -> None:
        """Fault-injection seam after every acquisition index is durable."""

    def _active_lease_for_endpoint(
        self,
        endpoint_id: str,
        *,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> Lease | None:
        if expiry_refs is None:
            expiry_refs, expiry_truncated = self._scan_expiry_refs(
                limit=queue_layout.MAX_LIVE_LEASE_RECORDS
            )
            if expiry_truncated:
                raise QueueConflictError("lease expiry index exceeded its safety bound")
        lease_refs, truncated = self._scan_lease_endpoint_refs(
            endpoint_id,
            limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
        )
        if truncated:
            raise QueueConflictError("lease endpoint index exceeded its safety bound")
        endpoint_token = queue_lease_records.lease_endpoint_token(endpoint_id)
        expected_refs = [
            (lease_token, identity_token)
            for (
                _expires,
                _cluster_token,
                _kind,
                indexed_endpoint_token,
                _job_token,
                lease_token,
                identity_token,
            ) in expiry_refs
            if indexed_endpoint_token == endpoint_token
        ]
        if len(expected_refs) != len(set(expected_refs)):
            raise QueueConflictError(
                f"lease expiry index duplicates endpoint identity: {endpoint_id}"
            )
        if set(lease_refs) != set(expected_refs):
            raise QueueConflictError(f"lease endpoint and expiry indexes disagree: {endpoint_id}")
        active: list[Lease] = []
        for lease_token, identity_token in lease_refs:
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            lease = self._store_adapter.read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(f"lease endpoint index is orphaned: {identity.lease_id}")
            self._validate_lease_index_identity(lease, identity)
            if identity.endpoint_id != endpoint_id:
                raise QueueConflictError(
                    f"lease endpoint index identity mismatch: {identity.lease_id}"
                )
            self._require_empty_lease_ref(
                self._lease_identity_ref_path(identity),
                label="lease identity reference",
            )
            self._require_empty_lease_ref(
                self._lease_cluster_kind_ref_path(identity),
                label="lease cluster-kind reference",
            )
            self._require_empty_lease_ref(
                self._lease_expiry_ref_path(identity),
                label="lease expiry reference",
            )
            if not lease.is_expired():
                active.append(lease)
        if len(active) > 1:
            raise QueueConflictError(f"endpoint has multiple active durable leases: {endpoint_id}")
        return active[0] if active else None
