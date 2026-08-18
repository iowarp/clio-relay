"""Stale-lease recovery engine and the shared canonical-lease delete primitive.

Owns the expired-lease sweep used both by explicit ``recover_stale_jobs``/
``recover_stale_job`` calls and by ``queue_leases``' pre-admission recovery
(``acquire_next_job`` et al. must reconcile stale leases before computing
capacity). ``_delete_lease_unlocked`` -- canonical lease delete plus every
operational-index and capacity-transition side effect -- lives here rather
than on ``queue_leases`` despite deleting a lease being conceptually a
leases-CRUD concern: this owner's stale sweep (itself a dependency of
``queue_leases.acquire_next_job``) calls it three times internally, while
``queue_leases`` needs it at only two call sites (``release_lease``,
``recover_stale_job``). ``queue_leases`` self-calling ``self.
_delete_lease_unlocked`` resolves through the inherited
``QueueLeaseRecoveryMixin`` method, and only that direction is rank-legal
since ``queue_leases`` also needs this owner's stale-sweep engine for
admission and must rank after it. Hosting the shared primitive at the
earlier rank breaks what would otherwise be a genuine two-owner call cycle
(mirrors ledger §9.3's ``get_job``/``get_artifact`` resolution: hoist the
primitive to whichever side the topology requires, not to whichever side
"conceptually" owns it).
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_lease_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    JobKind,
    JobState,
    Lease,
    RelayEvent,
    RelayJob,
    utc_now,
)

_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseExpiryReference = queue_layout.LeaseExpiryReference


def _stable_ref_token(*values: str) -> str:
    encoded = "\x00".join(values).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


class QueueLeaseRecoveryMixin:
    """Own the stale-lease recovery engine and the canonical delete-lease primitive."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _read_job_index(self, job_id: str) -> dict[str, object] | None: ...
        def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _lease_capacity_migration_complete_unlocked(self) -> bool: ...
        def _job_has_pending_execution_cleanup_after_migration_unlocked(
            self, cluster: str, job_id: str
        ) -> bool: ...
        def _write_job_unlocked(self, job: RelayJob) -> None: ...
        def _next_event_seq(self, job_id: str, event_dir: Path) -> int: ...
        def get_job(self, job_id: str) -> RelayJob: ...
        def _scan_expiry_refs(self, *, limit: int) -> tuple[list[_LeaseExpiryReference], bool]: ...
        def _lease_index_identity(self, lease: Lease, *, job: RelayJob) -> _LeaseIndexIdentity: ...
        def _read_lease_index_identity(self, lease_id: str) -> _LeaseIndexIdentity: ...
        def _read_lease_index_identity_by_token(
            self, lease_token: str, identity_token: str | None = None
        ) -> _LeaseIndexIdentity: ...
        def _validate_lease_index_identity(
            self, lease: Lease, identity: _LeaseIndexIdentity
        ) -> None: ...
        def _delete_lease_operational_indexes_unlocked(
            self, identity: _LeaseIndexIdentity, *, allow_foreign_manifest: bool = False
        ) -> None: ...
        def _require_empty_lease_ref(self, path: Path, *, label: str) -> None: ...
        def _lease_index_path(self, lease_id: str) -> Path: ...
        def _lease_identity_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_endpoint_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_endpoint_guard_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_cluster_kind_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
        def _lease_expiry_ref_path(self, identity: _LeaseIndexIdentity) -> Path: ...
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
        self,
        *,
        cluster: str,
        max_attempts: int,
    ) -> list[_LeaseExpiryReference] | None:
        """Recover stale work and retain an unchanged bounded expiry snapshot."""
        refs, truncated = self._scan_expiry_refs(limit=queue_layout.MAX_LIVE_LEASE_RECORDS)
        if truncated:
            raise QueueConflictError("lease recovery index exceeded its safety bound")
        _recovered, changed = self._recover_stale_jobs_from_expiry_refs_unlocked(
            cluster=cluster,
            max_attempts=max_attempts,
            refs=refs,
        )
        return None if changed else refs

    def _due_expired_leases_unlocked(
        self,
        *,
        cluster: str,
        now: datetime,
        refs: list[_LeaseExpiryReference] | None = None,
    ) -> list[Lease]:
        if refs is None:
            refs, truncated = self._scan_expiry_refs(limit=queue_layout.MAX_LIVE_LEASE_RECORDS)
            if truncated:
                raise QueueConflictError("lease recovery index exceeded its safety bound")
        due_key = queue_lease_records.lease_expiry_key(now)
        cluster_token = queue_lease_records.lease_cluster_token(cluster)
        due: list[Lease] = []
        for (
            expires_key,
            indexed_cluster,
            kind,
            endpoint_token,
            job_token,
            lease_token,
            identity_token,
        ) in refs:
            if indexed_cluster != cluster_token or expires_key > due_key:
                continue
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            lease = self._store_adapter.read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(f"lease expiry index is orphaned: {identity.lease_id}")
            self._validate_lease_index_identity(lease, identity)
            if (
                identity.cluster != cluster
                or identity.job_kind != kind
                or queue_lease_records.lease_endpoint_token(identity.endpoint_id) != endpoint_token
                or queue_lease_records.lease_job_token(identity.job_id) != job_token
                or queue_lease_records.lease_expiry_key(identity.expires_at) != expires_key
            ):
                raise QueueConflictError(
                    f"lease expiry index identity mismatch: {identity.lease_id}"
                )
            self._require_empty_lease_ref(
                self._lease_identity_ref_path(identity),
                label="lease identity reference",
            )
            self._require_empty_lease_ref(
                self._lease_endpoint_ref_path(identity),
                label="lease endpoint reference",
            )
            self._require_empty_lease_ref(
                self._lease_endpoint_guard_path(identity),
                label="lease endpoint guard",
            )
            self._require_empty_lease_ref(
                self._lease_cluster_kind_ref_path(identity),
                label="lease cluster-kind reference",
            )
            if lease.is_expired(now):
                due.append(lease)
        return sorted(due, key=lambda lease: (lease.expires_at, lease.lease_id))

    def _recover_stale_jobs_unlocked(self, *, cluster: str, max_attempts: int) -> list[RelayJob]:
        refs, truncated = self._scan_expiry_refs(limit=queue_layout.MAX_LIVE_LEASE_RECORDS)
        if truncated:
            raise QueueConflictError("lease recovery index exceeded its safety bound")
        recovered, _changed = self._recover_stale_jobs_from_expiry_refs_unlocked(
            cluster=cluster,
            max_attempts=max_attempts,
            refs=refs,
        )
        return recovered

    def _recover_stale_jobs_from_expiry_refs_unlocked(
        self,
        *,
        cluster: str,
        max_attempts: int,
        refs: list[_LeaseExpiryReference],
    ) -> tuple[list[RelayJob], bool]:
        """Recover from one bounded expiry snapshot and report index mutation."""
        recovered: list[RelayJob] = []
        changed = False
        leases_by_job: dict[str, list[Lease]] = {}
        now = utc_now()
        for lease in self._due_expired_leases_unlocked(
            cluster=cluster,
            now=now,
            refs=refs,
        ):
            leases_by_job.setdefault(lease.job_id, []).append(lease)
        for job_id, leases in leases_by_job.items():
            job = self._store_adapter.read_optional(
                self._storage_root / "jobs" / f"{job_id}.json", RelayJob
            )
            if job is None:
                for lease in leases:
                    self._delete_lease_unlocked(lease)
                    changed = True
                continue
            if job.cluster != cluster:
                raise QueueConflictError(
                    f"lease expiry cluster identity mismatch: {job.job_id}/{cluster}"
                )
            if job.state in TERMINAL_STATES or job.state not in {
                JobState.LEASED,
                JobState.RUNNING,
            }:
                for lease in leases:
                    self._delete_lease_unlocked(lease, job=job)
                    changed = True
                continue
            if self._job_has_pending_execution_cleanup_after_migration_unlocked(
                cluster,
                job.job_id,
            ):
                continue
            if self._job_has_scheduler_observation_unlocked(job):
                continue
            updated = self._recover_expired_leases_unlocked(
                job,
                leases,
                max_attempts=max_attempts,
            )
            recovered.append(updated)
            changed = True
        return recovered, changed

    def _job_has_scheduler_observation_unlocked(self, job: RelayJob) -> bool:
        index = self._read_job_index(job.job_id)
        if index is None or index.get("retention_schema_version") != (
            queue_layout.RETENTION_INDEX_SCHEMA
        ):
            raise QueueConflictError(
                f"scheduler observation index is unavailable for job: {job.job_id}"
            )
        for family in ("scheduler_protections_by_job", "scheduler_refs_by_job"):
            paths = queue_store_read.bounded_json_record_paths(
                self._storage_root / family / queue_layout.QueueLayout.durable_key(job.job_id),
                limit=queue_layout.MAX_BOUNDED_SCAN_RECORDS,
                label=f"{family} for {job.job_id}",
            )
            if paths:
                return True
        return False

    def _recover_expired_leases_unlocked(
        self,
        job: RelayJob,
        expired: list[Lease],
        *,
        max_attempts: int,
    ) -> RelayJob:
        """Apply one intent-first job transition and all associated lease deletions."""
        if not expired:
            raise ValueError("stale recovery requires at least one expired lease")
        previous_state = job.state
        if previous_state not in {JobState.LEASED, JobState.RUNNING}:
            raise QueueConflictError(f"job is not recoverable from a worker lease: {job.job_id}")
        if job.attempts >= max_attempts:
            updated = job.model_copy(
                update={
                    "state": JobState.FAILED,
                    "leased_by": None,
                    "updated_at": utc_now(),
                    "last_error": "expired lease exceeded retry limit",
                }
            )
            event_type = "job.failed"
            message = "Job failed after expired lease retry limit"
        else:
            updated = job.model_copy(
                update={
                    "state": JobState.QUEUED,
                    "leased_by": None,
                    "updated_at": utc_now(),
                }
            )
            event_type = "job.requeued"
            message = "Job requeued after expired worker lease"
        lease_ids = [lease.lease_id for lease in expired]
        if len(set(lease_ids)) != len(lease_ids) or any(
            lease.job_id != job.job_id for lease in expired
        ):
            raise QueueConflictError(f"stale recovery lease identity mismatch: {job.job_id}")
        for lease in expired:
            expected = self._lease_index_identity(lease, job=job)
            indexed = self._read_lease_index_identity(lease.lease_id)
            self._validate_lease_index_identity(lease, indexed)
            if indexed != expected:
                raise QueueConflictError(
                    f"stale recovery lease index changed identity: {lease.lease_id}"
                )
            for path, label in (
                (self._lease_identity_ref_path(indexed), "lease identity reference"),
                (self._lease_endpoint_ref_path(indexed), "lease endpoint reference"),
                (self._lease_endpoint_guard_path(indexed), "lease endpoint guard"),
                (
                    self._lease_cluster_kind_ref_path(indexed),
                    "lease cluster-kind reference",
                ),
                (self._lease_expiry_ref_path(indexed), "lease expiry reference"),
            ):
                self._require_empty_lease_ref(path, label=label)
        event_dir = self._storage_root / "events" / job.job_id
        event = RelayEvent(
            job_id=job.job_id,
            seq=self._next_event_seq(job.job_id, event_dir),
            event_type=event_type,
            message=message,
            payload={
                "state": updated.state.value,
                "expired_lease_ids": lease_ids,
                "previous_state": previous_state.value,
                **(
                    {"error": "expired lease exceeded retry limit"}
                    if updated.state == JobState.FAILED
                    else {}
                ),
            },
        )
        transition_identity = _stable_ref_token(
            job.job_id,
            updated.updated_at.isoformat(),
            *sorted(lease_ids),
        )
        capacity_transition = self._prepare_lease_capacity_transition_unlocked(
            scope_deltas={(job.cluster, job.kind): -len(expired)}
        )
        intent_payload: dict[str, object] = {
            "job_id": job.job_id,
            "original_job": job.model_dump(mode="json"),
            "target_job": updated.model_dump(mode="json"),
            "leases": [lease.model_dump(mode="json") for lease in expired],
            "event": event.model_dump(mode="json"),
            "lease_capacity_transition": capacity_transition,
        }
        intent_path = self._write_transition_intent_unlocked(
            "stale_lease_recovery",
            transition_identity,
            intent_payload,
        )
        return self._apply_stale_lease_recovery_intent_unlocked(
            intent_path,
            intent_payload,
        )

    def _apply_stale_lease_recovery_intent_unlocked(
        self,
        intent_path: Path,
        payload: dict[str, object],
    ) -> RelayJob:
        """Replay an exact stale job transition and every lease/index deletion."""
        original = RelayJob.model_validate(payload.get("original_job"))
        target = RelayJob.model_validate(payload.get("target_job"))
        event = RelayEvent.model_validate(payload.get("event"))
        raw_leases = payload.get("leases")
        if not isinstance(raw_leases, list):
            raise QueueConflictError(f"invalid stale recovery leases: {intent_path}")
        leases = [Lease.model_validate(item) for item in cast(list[object], raw_leases)]
        target_updates: dict[str, object] = {
            "state": target.state,
            "leased_by": None,
            "updated_at": target.updated_at,
        }
        if target.state is JobState.FAILED:
            target_updates["last_error"] = "expired lease exceeded retry limit"
        expected_target = original.model_copy(update=target_updates)
        expected_event_type = "job.failed" if target.state is JobState.FAILED else "job.requeued"
        expected_message = (
            "Job failed after expired lease retry limit"
            if target.state is JobState.FAILED
            else "Job requeued after expired worker lease"
        )
        expected_payload: dict[str, object] = {
            "state": target.state.value,
            "expired_lease_ids": [lease.lease_id for lease in leases],
            "previous_state": original.state.value,
        }
        if target.state is JobState.FAILED:
            expected_payload["error"] = "expired lease exceeded retry limit"
        if (
            payload.get("job_id") != original.job_id
            or target.job_id != original.job_id
            or target.cluster != original.cluster
            or target.kind != original.kind
            or original.state not in {JobState.LEASED, JobState.RUNNING}
            or target.state not in {JobState.QUEUED, JobState.FAILED}
            or target.leased_by is not None
            or target != expected_target
            or event.job_id != original.job_id
            or event.event_type != expected_event_type
            or event.message != expected_message
            or event.payload != expected_payload
            or event.seq < 1
            or not leases
            or len({lease.lease_id for lease in leases}) != len(leases)
            or any(lease.job_id != original.job_id for lease in leases)
        ):
            raise QueueConflictError(f"stale recovery intent identity mismatch: {intent_path}")
        current = self._store_adapter.read_optional(
            self._storage_root / "jobs" / f"{original.job_id}.json",
            RelayJob,
        )
        if current != original and current != target:
            raise QueueConflictError(
                f"stale recovery job changed after intent creation: {original.job_id}"
            )
        for lease in leases:
            canonical_lease = self._store_adapter.read_optional(
                self._storage_root / "leases" / f"{lease.lease_id}.json",
                Lease,
            )
            if canonical_lease is not None and canonical_lease != lease:
                raise QueueConflictError(
                    f"stale recovery canonical lease changed: {lease.lease_id}"
                )
            index_path = self._lease_index_path(lease.lease_id)
            if canonical_lease is None and current == original:
                raise QueueConflictError(
                    f"stale recovery canonical lease is missing: {lease.lease_id}"
                )
            if os.path.lexists(index_path):
                expected_identity = self._lease_index_identity(lease, job=original)
                indexed_identity = self._read_lease_index_identity(lease.lease_id)
                if indexed_identity != expected_identity:
                    raise QueueConflictError(
                        f"stale recovery lease index changed: {lease.lease_id}"
                    )
            elif canonical_lease is not None:
                raise QueueConflictError(f"stale recovery lease index is missing: {lease.lease_id}")
        self._before_stale_recovery_job_write(target, leases)
        self._write_job_unlocked(target)
        self._write_recovery_event_unlocked(event)
        self._after_stale_recovery_job_write(target, leases)
        for lease in leases:
            identity = self._lease_index_identity(lease, job=original)
            self._delete_lease_unlocked(
                lease,
                job=original,
                intent_path=intent_path,
                identity=identity,
                finalize_intent=False,
            )
        capacity_transition = payload.get("lease_capacity_transition")
        if capacity_transition is not None:
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"stale lease recovery {original.job_id}",
            )
            self._before_lease_capacity_intent_removal(
                "stale_lease_recovery",
                intent_path,
            )
        elif self._lease_capacity_migration_complete_unlocked():
            raise QueueConflictError(
                f"stale recovery intent has no capacity transition: {intent_path}"
            )
        queue_store_write.unlink_durable_path(intent_path, missing_ok=True)
        return target

    def _write_recovery_event_unlocked(self, event: RelayEvent) -> None:
        event_path = self._storage_root / "events" / event.job_id / f"{event.seq:020d}.json"
        existing = self._store_adapter.read_optional(event_path, RelayEvent)
        if existing is not None and existing != event:
            raise QueueConflictError(
                f"stale recovery event sequence changed: {event.job_id}/{event.seq}"
            )
        if existing is None:
            self._store_adapter.write(event_path, event)
        index = self._read_job_index(event.job_id)
        if index is None:
            raise QueueConflictError(f"stale recovery job index is missing: {event.job_id}")
        if queue_index_state.index_integer(index, "latest_event_seq") < event.seq:
            self._update_job_index_unlocked(event.job_id, latest_event_seq=event.seq)

    def _before_stale_recovery_job_write(
        self,
        _target: RelayJob,
        _leases: list[Lease],
    ) -> None:
        """Fault-injection seam after intent persistence and before the job write."""

    def _after_stale_recovery_job_write(
        self,
        _target: RelayJob,
        _leases: list[Lease],
    ) -> None:
        """Fault-injection seam after the job/event write and before lease deletion."""

    def _delete_lease_unlocked(
        self,
        lease: Lease,
        *,
        job: RelayJob | None = None,
        intent_path: Path | None = None,
        identity: _LeaseIndexIdentity | None = None,
        finalize_intent: bool = True,
    ) -> None:
        """Delete a canonical lease and every exact index through one replayable intent."""
        if identity is None:
            if job is None:
                try:
                    job = self.get_job(lease.job_id)
                except NotFoundError:
                    identity = self._read_lease_index_identity(lease.lease_id)
            if identity is None:
                if job is None:
                    raise QueueConflictError(
                        f"cannot resolve lease operational identity: {lease.lease_id}"
                    )
                identity = self._lease_index_identity(lease, job=job)
        self._validate_lease_index_identity(lease, identity)
        index_path = self._lease_index_path(lease.lease_id)
        if os.path.lexists(index_path):
            indexed = self._read_lease_index_identity(lease.lease_id)
            if indexed != identity:
                raise QueueConflictError(
                    f"lease operational identity changed before deletion: {lease.lease_id}"
                )
        elif intent_path is None:
            raise QueueConflictError(
                f"lease operational index is missing before deletion: {lease.lease_id}"
            )
        owned_intent = intent_path
        capacity_transition: object | None = None
        if owned_intent is None:
            capacity_transition = self._prepare_lease_capacity_transition_unlocked(
                scope_deltas={(identity.cluster, identity.job_kind): -1}
            )
            owned_intent = self._write_transition_intent_unlocked(
                "lease_delete",
                lease.lease_id,
                {
                    "job_id": lease.job_id,
                    "lease_id": lease.lease_id,
                    "lease": lease.model_dump(mode="json"),
                    "index": queue_lease_records.lease_index_document(identity),
                    "lease_capacity_transition": capacity_transition,
                },
            )
        queue_store_write.unlink_durable_path(
            self._storage_root / "leases" / f"{lease.lease_id}.json",
            missing_ok=True,
        )
        self._after_lease_canonical_delete(lease)
        queue_store_write.unlink_durable_path(
            self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
            missing_ok=True,
        )
        self._delete_lease_operational_indexes_unlocked(identity)
        self._after_lease_index_delete(lease)
        if finalize_intent:
            if capacity_transition is None:
                raise QueueConflictError(
                    f"lease deletion has no capacity transition: {lease.lease_id}"
                )
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"lease deletion {lease.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_delete", owned_intent)
            queue_store_write.unlink_durable_path(owned_intent, missing_ok=True)

    def _after_lease_canonical_delete(self, _lease: Lease) -> None:
        """Fault-injection seam after the canonical lease record is removed."""

    def _after_lease_index_delete(self, _lease: Lease) -> None:
        """Fault-injection seam after every derived lease index is removed."""
