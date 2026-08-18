"""Durable lease-capacity aggregate/checkpoint pair ownership.

Owns the fixed two-file capacity journal (``lease_capacity/aggregate.json``
and ``lease_capacity/checkpoint.json``): its directory/path validation,
mutual read/write primitives, transition prepare/apply, the canonical-lease
scan that derives exact scoped counts, and every O(1) or exact admission
snapshot read from it. ``lease_admission_capacity_snapshot`` is the public
facade entrypoint; the other snapshot helpers are internal collaborators for
``queue_leases``' acquisition path (design doc CQ15 row groups "lease
capacity state/audit, indexes, leases, recovery, scheduler claims" as one
slice with several sub-owners).
"""

from __future__ import annotations

import json
import logging
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from clio_relay import (
    queue_context,
    queue_layout,
    queue_lease_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import JobKind, Lease, RelayJob

logger = logging.getLogger(__name__)

_LeaseCapacityAggregate = queue_lease_records.LeaseCapacityAggregate
_LeaseCapacityCheckpoint = queue_lease_records.LeaseCapacityCheckpoint
_LeaseCapacityPair = queue_lease_records.LeaseCapacityPair
_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseExpiryReference = queue_layout.LeaseExpiryReference


def _read_unique_json_document(path: Path) -> object:
    """Read JSON while rejecting duplicate keys at every object depth.

    Private duplicate of the facade-resident reader of the same name:
    ``core_queue.py``'s copy stays live as the ``document_reader`` callback
    for ``_read_sealed_index_migration_state`` (unmoved startup code), so it
    cannot be deleted. Matches the established per-owner duplication idiom
    already used for small, dependency-free helpers such as
    ``_stable_ref_token``.
    """

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise QueueConflictError(f"duplicate JSON key {key!r} in {path}")
            document[key] = value
        return document

    try:
        return json.loads(
            queue_store_read.read_bounded_record_bytes(path),
            object_pairs_hook=unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise queue_conflict_from_cause(
            f"invalid JSON record {path}",
            cause=exc,
            logger=logger,
        ) from exc


class QueueLeaseCapacityStateMixin:
    """Own the durable lease-capacity aggregate/checkpoint pair."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def initialize(self) -> None: ...
        def _require_index_migration_complete(self) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _lease_index_identity(self, lease: Lease, *, job: RelayJob) -> _LeaseIndexIdentity: ...
        def _scan_lease_scope_refs(
            self,
            directory: Path,
            *,
            scope: tuple[str, ...],
            limit: int,
            label: str,
        ) -> tuple[list[tuple[str, str]], bool]: ...
        def _scan_expiry_refs(self, *, limit: int) -> tuple[list[_LeaseExpiryReference], bool]: ...
        def _scan_lease_identity_refs(
            self, *, limit: int
        ) -> tuple[list[tuple[str, str]], bool]: ...
        def _lease_cluster_kind_directory(self, cluster: str, kind: JobKind) -> Path: ...

    def _lease_capacity_directory(self) -> Path:
        return self._storage_root / "lease_capacity"

    def _lease_capacity_record_paths_unlocked(
        self,
        *,
        allow_missing: bool,
    ) -> dict[str, Path]:
        """Validate the fixed two-file aggregate inventory without following links."""
        directory = self._lease_capacity_directory()
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            if allow_missing:
                return {}
            raise QueueConflictError(f"lease capacity directory is missing: {directory}") from None
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(f"lease capacity directory is unsafe: {directory}")
        if os.name != "nt" and hasattr(os, "geteuid") and directory_stat.st_uid != os.geteuid():
            raise QueueConflictError(f"lease capacity directory is not owned: {directory}")
        allowed = {"aggregate.json", "checkpoint.json"}
        paths: dict[str, Path] = {}
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(paths) >= 2:
                        raise QueueConflictError(
                            "lease capacity directory exceeds its fixed two-record inventory"
                        )
                    path = Path(entry.path)
                    entry_stat = os.lstat(path)
                    if entry.name not in allowed:
                        raise QueueConflictError(
                            f"lease capacity directory contains an unexpected record: {path}"
                        )
                    queue_layout.validate_record_stat(entry_stat, path=path)
                    if entry_stat.st_size > queue_layout.MAX_LEASE_CAPACITY_RECORD_BYTES:
                        raise QueueConflictError(
                            f"lease capacity record exceeds its byte bound: {path}"
                        )
                    paths[entry.name] = path
        except QueueConflictError:
            raise
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot inspect lease capacity directory",
                cause=exc,
                logger=logger,
            ) from exc
        if not allow_missing and set(paths) != allowed:
            missing = ", ".join(sorted(allowed - set(paths)))
            raise QueueConflictError(f"lease capacity pair is incomplete; missing {missing}")
        return paths

    def _read_lease_capacity_components_unlocked(
        self,
        *,
        allow_missing: bool,
    ) -> tuple[_LeaseCapacityAggregate | None, _LeaseCapacityCheckpoint | None]:
        paths = self._lease_capacity_record_paths_unlocked(allow_missing=allow_missing)
        aggregate_path = paths.get("aggregate.json")
        checkpoint_path = paths.get("checkpoint.json")
        aggregate = (
            None
            if aggregate_path is None
            else queue_lease_records.lease_capacity_aggregate_from_document(
                _read_unique_json_document(aggregate_path),
                label=f"lease capacity aggregate {aggregate_path}",
            )
        )
        checkpoint = (
            None
            if checkpoint_path is None
            else queue_lease_records.lease_capacity_checkpoint_from_document(
                _read_unique_json_document(checkpoint_path),
                label=f"lease capacity checkpoint {checkpoint_path}",
            )
        )
        return aggregate, checkpoint

    def _read_lease_capacity_aggregate_unlocked(self) -> _LeaseCapacityPair:
        """Read and mutually validate the fixed aggregate/checkpoint pair."""
        aggregate, checkpoint = self._read_lease_capacity_components_unlocked(allow_missing=False)
        if aggregate is None or checkpoint is None:
            raise QueueConflictError("lease capacity pair is incomplete")
        pair = _LeaseCapacityPair(aggregate=aggregate, checkpoint=checkpoint)
        queue_lease_records.validate_lease_capacity_pair(pair, label="lease capacity pair")
        return pair

    def _write_lease_capacity_pair_unlocked(self, pair: _LeaseCapacityPair) -> None:
        """Atomically replace each side of a journal-protected capacity pair."""
        queue_lease_records.validate_lease_capacity_pair(pair, label="lease capacity write")
        directory = self._lease_capacity_directory()
        queue_store_write.require_safe_write_directory(self._storage_root, directory)
        queue_store_write.write_json(
            self._storage_root,
            directory / "aggregate.json",
            queue_lease_records.lease_capacity_aggregate_document(pair.aggregate),
        )
        self._after_lease_capacity_aggregate_write(pair.aggregate)
        queue_store_write.write_json(
            self._storage_root,
            directory / "checkpoint.json",
            queue_lease_records.lease_capacity_checkpoint_document(pair.checkpoint),
        )
        self._after_lease_capacity_checkpoint_write(pair.checkpoint)

    def _after_lease_capacity_aggregate_write(
        self,
        _aggregate: _LeaseCapacityAggregate,
    ) -> None:
        """Fault-injection seam after the aggregate replacement."""

    def _after_lease_capacity_checkpoint_write(
        self,
        _checkpoint: _LeaseCapacityCheckpoint,
    ) -> None:
        """Fault-injection seam after the checkpoint replacement."""

    def _before_lease_capacity_intent_removal(self, _kind: str, _path: Path) -> None:
        """Fault-injection seam after convergence and before journal removal."""

    def _prepare_lease_capacity_transition_unlocked(
        self,
        *,
        scope_deltas: dict[tuple[str, JobKind], int],
        include_rollback: bool = False,
    ) -> dict[str, object]:
        """Create exact before/after generations for one lease transition."""
        before = self._read_lease_capacity_aggregate_unlocked()
        counts = {
            cluster_token: dict(kind_counts)
            for cluster_token, kind_counts in before.aggregate.cluster_kind_counts.items()
        }
        for (cluster, kind), delta in scope_deltas.items():
            if isinstance(delta, bool) or delta == 0:
                raise QueueConflictError(
                    "lease capacity transition delta must be a nonzero integer"
                )
            cluster_token = queue_lease_records.lease_cluster_token(cluster)
            kind_counts = counts.setdefault(cluster_token, {})
            next_count = kind_counts.get(kind, 0) + delta
            if next_count < 0:
                raise QueueConflictError(
                    f"lease capacity transition underflow: {cluster}/{kind.value}"
                )
            if next_count == 0:
                kind_counts.pop(kind, None)
            else:
                kind_counts[kind] = next_count
            if not kind_counts:
                counts.pop(cluster_token, None)
        after = queue_lease_records.new_lease_capacity_pair(
            counts,
            epoch_id=before.aggregate.epoch_id,
            generation=before.aggregate.generation + 1,
        )
        transition: dict[str, object] = {
            "before": queue_lease_records.lease_capacity_pair_payload(before),
            "after": queue_lease_records.lease_capacity_pair_payload(after),
        }
        if include_rollback:
            rollback = queue_lease_records.new_lease_capacity_pair(
                before.aggregate.cluster_kind_counts,
                epoch_id=before.aggregate.epoch_id,
                generation=after.aggregate.generation + 1,
            )
            transition["rollback"] = queue_lease_records.lease_capacity_pair_payload(rollback)
        return transition

    def _apply_lease_capacity_transition_unlocked(
        self,
        transition_value: object,
        *,
        target: Literal["after", "rollback"],
        label: str,
    ) -> _LeaseCapacityPair:
        """Converge a possibly torn pair when every component is journal-authorized."""
        if not isinstance(transition_value, dict):
            raise QueueConflictError(f"{label} has no lease capacity transition")
        transition = cast(dict[str, object], transition_value)
        allowed_fields = {"before", "after", "rollback"}
        if not {"before", "after"}.issubset(transition) or not set(transition).issubset(
            allowed_fields
        ):
            raise QueueConflictError(f"{label} lease capacity transition is invalid")
        pairs = {
            name: queue_lease_records.lease_capacity_pair_from_payload(
                value, label=f"{label} {name}"
            )
            for name, value in transition.items()
        }
        selected = pairs.get(target)
        if selected is None:
            raise QueueConflictError(f"{label} has no authorized {target} capacity generation")
        aggregates = tuple(pair.aggregate for pair in pairs.values())
        checkpoints = tuple(pair.checkpoint for pair in pairs.values())
        current_aggregate, current_checkpoint = self._read_lease_capacity_components_unlocked(
            allow_missing=True
        )
        if current_aggregate is not None and not any(
            current_aggregate == aggregate for aggregate in aggregates
        ):
            raise QueueConflictError(f"{label} found an unauthorized aggregate generation")
        if current_checkpoint is not None and not any(
            current_checkpoint == checkpoint for checkpoint in checkpoints
        ):
            raise QueueConflictError(f"{label} found an unauthorized checkpoint generation")
        if current_aggregate is None and current_checkpoint is None:
            raise QueueConflictError(f"{label} found both capacity records missing")
        self._write_lease_capacity_pair_unlocked(selected)
        return selected

    def _canonical_lease_capacity_records_unlocked(
        self,
        *,
        limit: int,
    ) -> tuple[
        list[tuple[Lease, RelayJob, _LeaseIndexIdentity]],
        dict[str, dict[JobKind, int]],
    ]:
        """Read bounded canonical leases and derive their exact aggregate scopes."""
        leases, truncated = queue_store_read.scan_many(
            self._storage_root / "leases",
            Lease,
            limit=limit,
        )
        if truncated:
            raise QueueConflictError(
                f"lease capacity rebuild exceeded its safety bound of {limit} records"
            )
        indexed: list[tuple[Lease, RelayJob, _LeaseIndexIdentity]] = []
        counts: dict[str, dict[JobKind, int]] = {}
        clusters_by_token: dict[str, str] = {}
        references: set[tuple[str, str]] = set()
        lease_tokens: set[str] = set()
        for lease in leases:
            job = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "jobs" / f"{lease.job_id}.json",
                RelayJob,
            )
            if job is None:
                raise QueueConflictError(
                    f"lease capacity rebuild cannot resolve job: {lease.lease_id}/{lease.job_id}"
                )
            identity = self._lease_index_identity(lease, job=job)
            reference = queue_lease_records.lease_reference(identity)
            if reference in references or reference[0] in lease_tokens:
                raise QueueConflictError(
                    f"lease capacity rebuild found an identity collision: {lease.lease_id}"
                )
            references.add(reference)
            lease_tokens.add(reference[0])
            cluster_token = queue_lease_records.lease_cluster_token(job.cluster)
            previous_cluster = clusters_by_token.setdefault(cluster_token, job.cluster)
            if previous_cluster != job.cluster:
                raise QueueConflictError(
                    "lease capacity rebuild found a cluster-token collision: "
                    f"{previous_cluster}/{job.cluster}"
                )
            kind_counts = counts.setdefault(cluster_token, {})
            kind_counts[job.kind] = kind_counts.get(job.kind, 0) + 1
            indexed.append((lease, job, identity))
        return indexed, queue_lease_records.normalize_lease_capacity_counts(counts)

    def _active_lease_counts_by_kind(self, *, cluster: str) -> dict[JobKind, int]:
        """Count structurally validated refs without opening global lease JSON."""
        counts, _global_total = self._lease_capacity_snapshot(cluster=cluster)
        return counts

    def lease_admission_capacity_snapshot(
        self,
        *,
        cluster: str,
    ) -> tuple[dict[JobKind, int], int]:
        """Return structurally validated pre-recovery lease admission counts."""
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            counts, global_total = self._lease_capacity_snapshot(cluster=cluster)
            return dict(counts), global_total

    def _lease_capacity_snapshot(
        self,
        *,
        cluster: str,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> tuple[dict[JobKind, int], int]:
        """Return O(1) journaled admission counts from two fixed records."""
        del expiry_refs
        pair = self._read_lease_capacity_aggregate_unlocked()
        counts = pair.aggregate.cluster_kind_counts.get(
            queue_lease_records.lease_cluster_token(cluster), {}
        )
        return dict(counts), pair.aggregate.global_live_leases

    def _exact_lease_capacity_snapshot(
        self,
        *,
        cluster: str,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> tuple[dict[JobKind, int], int]:
        """Audit exact expiry, identity, and cluster-kind operational indexes."""
        if expiry_refs is None:
            expiry_refs, expiry_truncated = self._scan_expiry_refs(
                limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
            )
            if expiry_truncated:
                raise QueueConflictError(
                    "active lease population exceeded its safety bound of "
                    f"{queue_layout.MAX_LIVE_LEASE_RECORDS} records"
                )
        expiry_pairs = [
            (lease_token, identity_token) for *_, lease_token, identity_token in expiry_refs
        ]
        if len(set(expiry_pairs)) != len(expiry_pairs) or len(
            {lease_token for lease_token, _identity_token in expiry_pairs}
        ) != len(expiry_pairs):
            raise QueueConflictError("lease expiry index contains duplicate identities")
        identity_refs, identity_truncated = self._scan_lease_identity_refs(
            limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
        )
        if identity_truncated:
            raise QueueConflictError(
                "active lease population exceeded its safety bound of "
                f"{queue_layout.MAX_LIVE_LEASE_RECORDS} records"
            )
        if set(identity_refs) != set(expiry_pairs):
            raise QueueConflictError("lease identity and expiry indexes disagree")
        cluster_token = queue_lease_records.lease_cluster_token(cluster)
        expected_by_kind: dict[JobKind, set[tuple[str, str]]] = {kind: set() for kind in JobKind}
        for (
            _expires,
            indexed_cluster,
            kind,
            _endpoint_token,
            _job_token,
            lease_token,
            identity_token,
        ) in expiry_refs:
            if indexed_cluster == cluster_token:
                expected_by_kind[kind].add((lease_token, identity_token))
        counts: dict[JobKind, int] = {}
        total = 0
        for kind in JobKind:
            lease_refs, truncated = self._scan_lease_scope_refs(
                self._lease_cluster_kind_directory(cluster, kind),
                scope=("cluster-kind", cluster_token, kind.value),
                limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
                label=f"lease cluster-kind index {cluster}/{kind.value}",
            )
            if truncated:
                raise QueueConflictError(
                    "active lease population exceeded its safety bound of "
                    f"{queue_layout.MAX_LIVE_LEASE_RECORDS} records"
                )
            observed = set(lease_refs)
            if observed != expected_by_kind[kind]:
                raise QueueConflictError(
                    f"lease cluster-kind and expiry indexes disagree: {cluster}/{kind.value}"
                )
            if observed:
                counts[kind] = len(observed)
                total += len(observed)
        if total > queue_layout.MAX_LIVE_LEASE_RECORDS:
            raise QueueConflictError(
                "active lease population exceeded its safety bound of "
                f"{queue_layout.MAX_LIVE_LEASE_RECORDS} records"
            )
        return counts, len(expiry_refs)
