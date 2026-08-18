"""Lease-capacity repair and audit orchestration.

Owns the two public entrypoints that reconcile the durable capacity journal
(``queue_lease_capacity_state``) against every operational index
(``queue_lease_indexes``) and the canonical lease-record family:
``repair_lease_operational_indexes`` (rebuild-from-canonical, used at
startup/migration and by the operator repair endpoint) and
``audit_lease_capacity`` (read-only comparison, used for diagnostics). The
repair path's lifecycle-index convergence resolves through the module-level
``queue_lease_indexes.sync_operational_indexes`` lookup (not a bound
``self.`` call) so a test can patch exactly that seam -- design doc CQ15 row:
"Patch ``queue_lease_capacity_audit.queue_lease_indexes.sync_operational_
indexes``, then each lifecycle/recovery job-write lookup."
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_lease_indexes,
    queue_lease_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import JobKind, Lease, RelayJob, utc_now

logger = logging.getLogger(__name__)
_LeaseCapacityPair = queue_lease_records.LeaseCapacityPair
_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity


class QueueLeaseCapacityAuditMixin:
    """Own lease-capacity repair and read-only audit orchestration."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def initialize(self) -> None: ...
        def _require_index_migration_complete(self) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _read_index_migration_state(self) -> dict[str, object]: ...
        def _write_index_migration_state(self, state: dict[str, object]) -> None: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _canonical_lease_capacity_records_unlocked(
            self, *, limit: int
        ) -> tuple[
            list[tuple[Lease, RelayJob, _LeaseIndexIdentity]],
            dict[str, dict[JobKind, int]],
        ]: ...
        def _lease_capacity_record_paths_unlocked(
            self, *, allow_missing: bool
        ) -> dict[str, Path]: ...
        def _write_lease_capacity_pair_unlocked(self, pair: _LeaseCapacityPair) -> None: ...
        def _read_lease_capacity_aggregate_unlocked(self) -> _LeaseCapacityPair: ...
        def _before_lease_capacity_intent_removal(self, _kind: str, _path: Path) -> None: ...
        def _read_lease_index_identity_by_token(
            self, lease_token: str, identity_token: str | None = None
        ) -> _LeaseIndexIdentity: ...
        def _require_safe_lease_index_directory(self, directory: Path, *, create: bool) -> bool: ...
        def _scan_expiry_refs(
            self, *, limit: int
        ) -> tuple[list[queue_layout.LeaseExpiryReference], bool]: ...
        def _scan_lease_identity_refs(
            self, *, limit: int
        ) -> tuple[list[tuple[str, str]], bool]: ...
        def _scan_lease_scope_refs(
            self,
            directory: Path,
            *,
            scope: tuple[str, ...],
            limit: int,
            label: str,
        ) -> tuple[list[tuple[str, str]], bool]: ...
        def _scan_lease_endpoint_refs(
            self, endpoint_id: str, *, limit: int
        ) -> tuple[list[tuple[str, str]], bool]: ...

    def repair_lease_operational_indexes(
        self,
        *,
        limit: int = queue_layout.MAX_LIVE_LEASE_RECORDS,
    ) -> dict[str, object]:
        """Rebuild and prune every lease operational index under one durable intent."""
        if limit < 1 or limit > queue_layout.MAX_LIVE_LEASE_RECORDS:
            raise ValueError(
                "lease index repair limit must be between 1 and "
                f"{queue_layout.MAX_LIVE_LEASE_RECORDS}"
            )
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            intent_path, repair_payload = self._prepare_lease_capacity_rebuild_intent_unlocked(
                identity="operator",
                limit=limit,
            )
            record_count = self._apply_lease_index_repair_intent_unlocked(
                intent_path,
                repair_payload,
            )
            capacity = self._read_lease_capacity_aggregate_unlocked()
            state = self._read_index_migration_state()
            raw_checkpoint = state.get("lease_operational_repair")
            if not isinstance(raw_checkpoint, dict):
                raise QueueConflictError("lease operational-index repair checkpoint is invalid")
            cast(dict[str, object], raw_checkpoint).update(
                {
                    "complete": True,
                    "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
                    "record_count": record_count,
                }
            )
            state["lease_capacity_aggregate"] = {
                "complete": True,
                "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                "epoch_id": capacity.aggregate.epoch_id,
                "generation": capacity.aggregate.generation,
                "record_count": record_count,
            }
            state["complete"] = queue_index_state.index_migration_components_complete(state)
            self._write_index_migration_state(state)
        return {
            "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
            "capacity_schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
            "capacity_epoch_id": capacity.aggregate.epoch_id,
            "capacity_generation": capacity.aggregate.generation,
            "record_count": record_count,
            "complete": True,
        }

    def audit_lease_capacity(
        self,
        *,
        limit: int = queue_layout.MAX_LIVE_LEASE_RECORDS,
    ) -> dict[str, object]:
        """Compare canonical leases, every operational index, and the aggregate."""
        if limit < 1 or limit > queue_layout.MAX_LIVE_LEASE_RECORDS:
            raise ValueError(
                "lease capacity audit limit must be between 1 and "
                f"{queue_layout.MAX_LIVE_LEASE_RECORDS}"
            )
        try:
            self.initialize()
            with self._lock:
                self._recover_pending_transitions_unlocked()
                self._require_index_migration_complete()
                return self._audit_lease_capacity_unlocked(limit=limit)
        except (OSError, QueueConflictError) as exc:
            return {
                "schema_version": queue_layout.LEASE_CAPACITY_AUDIT_SCHEMA,
                "valid": False,
                "scan_truncated": False,
                "result_truncated": False,
                "limit": limit,
                "checked_at": utc_now().isoformat(),
                "mismatches": [
                    {
                        "type": "audit_error",
                        "detail": bounded_error_detail(str(exc)) or type(exc).__name__,
                    }
                ],
            }

    def _audit_lease_capacity_unlocked(self, *, limit: int) -> dict[str, object]:
        indexed, canonical_counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        mismatches: list[dict[str, object]] = []
        result_truncated = False

        def mismatch(kind: str, **details: object) -> None:
            nonlocal result_truncated
            if len(mismatches) >= 100:
                result_truncated = True
                return
            mismatches.append({"type": kind, **details})

        expected_by_reference = {
            queue_lease_records.lease_reference(identity): identity
            for _lease, _job, identity in indexed
        }
        expected_references = set(expected_by_reference)
        expiry_refs, expiry_truncated = self._scan_expiry_refs(limit=limit)
        identity_refs, identity_truncated = self._scan_lease_identity_refs(limit=limit)
        scan_truncated = expiry_truncated or identity_truncated
        observed_expiry_references = {
            (lease_token, identity_token) for *_, lease_token, identity_token in expiry_refs
        }
        observed_identity_references = set(identity_refs)
        for label, observed in (
            ("expiry", observed_expiry_references),
            ("identity", observed_identity_references),
        ):
            for reference in sorted(expected_references - observed):
                mismatch(
                    "missing_operational_reference",
                    index=label,
                    reference=".".join(reference),
                )
            for reference in sorted(observed - expected_references):
                mismatch(
                    "orphaned_operational_reference",
                    index=label,
                    reference=".".join(reference),
                )

        manifest_paths = queue_store_read.bounded_json_record_paths(
            self._storage_root / "lease_indexes",
            limit=limit,
            label="lease operational manifest index",
        )
        observed_manifest_references: set[tuple[str, str]] = set()
        for path in manifest_paths:
            lease_token = path.stem
            identity = self._read_lease_index_identity_by_token(lease_token)
            reference = queue_lease_records.lease_reference(identity)
            if reference in observed_manifest_references:
                mismatch(
                    "duplicate_operational_manifest",
                    lease_id=identity.lease_id,
                    reference=".".join(reference),
                )
            observed_manifest_references.add(reference)
            expected_identity = expected_by_reference.get(reference)
            if expected_identity != identity:
                mismatch(
                    "operational_manifest_mismatch",
                    lease_id=identity.lease_id,
                    reference=".".join(reference),
                )
        for reference in sorted(expected_references - observed_manifest_references):
            mismatch("missing_operational_manifest", reference=".".join(reference))

        expected_by_scope: dict[tuple[str, JobKind], set[tuple[str, str]]] = {}
        cluster_labels: dict[str, str] = {}
        expected_by_endpoint: dict[str, set[tuple[str, str]]] = {}
        endpoint_labels: dict[str, str] = {}
        for reference, identity in expected_by_reference.items():
            cluster_token = queue_lease_records.lease_cluster_token(identity.cluster)
            cluster_labels[cluster_token] = identity.cluster
            expected_by_scope.setdefault((cluster_token, identity.job_kind), set()).add(reference)
            endpoint_token = queue_lease_records.lease_endpoint_token(identity.endpoint_id)
            endpoint_labels[endpoint_token] = identity.endpoint_id
            expected_by_endpoint.setdefault(endpoint_token, set()).add(reference)

        observed_scope_references: dict[tuple[str, JobKind], set[tuple[str, str]]] = {}
        scope_root = self._storage_root / "leases_by_cluster_kind"
        self._require_safe_lease_index_directory(scope_root, create=False)
        scope_entries = 0
        with os.scandir(scope_root) as cluster_entries:
            for cluster_entry in cluster_entries:
                scope_entries += 1
                if scope_entries > queue_layout.MAX_LEASE_CAPACITY_SCOPES:
                    raise QueueConflictError("lease cluster-kind index exceeds its scope bound")
                cluster_path = Path(cluster_entry.path)
                cluster_stat = os.lstat(cluster_path)
                if (
                    not queue_lease_records.is_short_ref_token(cluster_entry.name)
                    or not stat.S_ISDIR(cluster_stat.st_mode)
                    or queue_layout.record_is_reparse(cluster_stat)
                ):
                    raise QueueConflictError(
                        f"lease cluster-kind index contains an unsafe cluster scope: {cluster_path}"
                    )
                self._require_safe_lease_index_directory(cluster_path, create=False)
                with os.scandir(cluster_path) as kind_entries:
                    for kind_entry in kind_entries:
                        scope_entries += 1
                        if scope_entries > queue_layout.MAX_LEASE_CAPACITY_SCOPES * 2:
                            raise QueueConflictError(
                                "lease cluster-kind index exceeds its scope bound"
                            )
                        try:
                            kind = JobKind(kind_entry.name)
                        except ValueError as exc:
                            raise QueueConflictError(
                                f"lease cluster-kind index has an invalid kind: {kind_entry.path}"
                            ) from exc
                        kind_path = Path(kind_entry.path)
                        kind_stat = os.lstat(kind_path)
                        if not stat.S_ISDIR(kind_stat.st_mode) or queue_layout.record_is_reparse(
                            kind_stat
                        ):
                            raise QueueConflictError(
                                "lease cluster-kind index contains an unsafe kind scope: "
                                f"{kind_path}"
                            )
                        references, truncated = self._scan_lease_scope_refs(
                            kind_path,
                            scope=("cluster-kind", cluster_entry.name, kind.value),
                            limit=limit,
                            label=(f"lease cluster-kind index {cluster_entry.name}/{kind.value}"),
                        )
                        scan_truncated = scan_truncated or truncated
                        observed_scope_references[(cluster_entry.name, kind)] = set(references)
        for scope in sorted(
            set(expected_by_scope) | set(observed_scope_references),
            key=lambda item: (item[0], item[1].value),
        ):
            expected = expected_by_scope.get(scope, set())
            observed = observed_scope_references.get(scope, set())
            if expected != observed:
                mismatch(
                    "cluster_kind_scope_mismatch",
                    cluster_token=scope[0],
                    cluster=cluster_labels.get(scope[0]),
                    job_kind=scope[1].value,
                    expected_count=len(expected),
                    observed_count=len(observed),
                )

        endpoint_root = self._storage_root / "leases_by_endpoint"
        self._require_safe_lease_index_directory(endpoint_root, create=False)
        observed_endpoint_tokens: set[str] = set()
        with os.scandir(endpoint_root) as endpoint_entries:
            for endpoint_entry in endpoint_entries:
                if len(observed_endpoint_tokens) >= limit:
                    scan_truncated = True
                    break
                endpoint_path = Path(endpoint_entry.path)
                endpoint_stat = os.lstat(endpoint_path)
                if (
                    not queue_lease_records.is_short_ref_token(endpoint_entry.name)
                    or not stat.S_ISDIR(endpoint_stat.st_mode)
                    or queue_layout.record_is_reparse(endpoint_stat)
                ):
                    raise QueueConflictError(
                        f"lease endpoint index contains an unsafe scope: {endpoint_path}"
                    )
                observed_endpoint_tokens.add(endpoint_entry.name)
        for endpoint_token in sorted(set(expected_by_endpoint) | observed_endpoint_tokens):
            endpoint_id = endpoint_labels.get(endpoint_token)
            if endpoint_id is None:
                mismatch("orphaned_endpoint_scope", endpoint_token=endpoint_token)
                continue
            observed, truncated = self._scan_lease_endpoint_refs(endpoint_id, limit=limit)
            scan_truncated = scan_truncated or truncated
            expected = expected_by_endpoint[endpoint_token]
            if set(observed) != expected:
                mismatch(
                    "endpoint_scope_mismatch",
                    endpoint_token=endpoint_token,
                    endpoint_id=endpoint_id,
                    expected_count=len(expected),
                    observed_count=len(observed),
                )

        aggregate_pair = self._read_lease_capacity_aggregate_unlocked()
        aggregate_counts = aggregate_pair.aggregate.cluster_kind_counts
        all_capacity_scopes = {
            (cluster_token, kind)
            for cluster_token, kind_counts in canonical_counts.items()
            for kind in kind_counts
        } | {
            (cluster_token, kind)
            for cluster_token, kind_counts in aggregate_counts.items()
            for kind in kind_counts
        }
        for cluster_token, kind in sorted(
            all_capacity_scopes,
            key=lambda item: (item[0], item[1].value),
        ):
            expected_count = canonical_counts.get(cluster_token, {}).get(kind, 0)
            observed_count = aggregate_counts.get(cluster_token, {}).get(kind, 0)
            if expected_count != observed_count:
                mismatch(
                    "aggregate_scope_mismatch",
                    cluster_token=cluster_token,
                    cluster=cluster_labels.get(cluster_token),
                    job_kind=kind.value,
                    expected_count=expected_count,
                    observed_count=observed_count,
                )
        if aggregate_pair.aggregate.global_live_leases != len(indexed):
            mismatch(
                "aggregate_global_mismatch",
                expected_count=len(indexed),
                observed_count=aggregate_pair.aggregate.global_live_leases,
            )
        return {
            "schema_version": queue_layout.LEASE_CAPACITY_AUDIT_SCHEMA,
            "valid": not mismatches and not scan_truncated,
            "scan_truncated": scan_truncated,
            "result_truncated": result_truncated,
            "limit": limit,
            "checked_at": utc_now().isoformat(),
            "canonical": {
                "global_live_leases": len(indexed),
                "cluster_kind_counts": queue_lease_records.serialized_lease_capacity_counts(
                    canonical_counts
                ),
            },
            "operational_indexes": {
                "manifests": len(observed_manifest_references),
                "identity_references": len(observed_identity_references),
                "expiry_references": len(observed_expiry_references),
                "cluster_kind_references": sum(
                    len(references) for references in observed_scope_references.values()
                ),
                "endpoint_references": sum(
                    len(references) for references in expected_by_endpoint.values()
                ),
            },
            "aggregate": {
                "epoch_id": aggregate_pair.aggregate.epoch_id,
                "generation": aggregate_pair.aggregate.generation,
                "checkpoint_id": aggregate_pair.aggregate.checkpoint_id,
                "global_live_leases": aggregate_pair.aggregate.global_live_leases,
                "cluster_kind_counts": queue_lease_records.serialized_lease_capacity_counts(
                    aggregate_counts
                ),
                "document_sha256": aggregate_pair.aggregate.document_sha256,
                "checkpoint_document_sha256": aggregate_pair.checkpoint.document_sha256,
            },
            "mismatches": mismatches,
        }

    def _apply_lease_index_repair_intent_unlocked(
        self,
        intent_path: Path,
        payload: dict[str, object],
    ) -> int:
        limit = payload.get("limit")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > queue_layout.MAX_LIVE_LEASE_RECORDS
        ):
            raise QueueConflictError(f"invalid lease index repair intent: {intent_path}")
        indexed, counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        raw_target = payload.get("lease_capacity_rebuild")
        if raw_target is None:
            migration_state = self._read_index_migration_state()
            raw_capacity_checkpoint = migration_state.get("lease_capacity_aggregate")
            if (
                isinstance(raw_capacity_checkpoint, dict)
                and cast(dict[str, object], raw_capacity_checkpoint).get("complete") is True
            ):
                raise QueueConflictError(
                    f"lease index repair intent has no capacity target: {intent_path}"
                )
            target = queue_lease_records.new_lease_capacity_pair(counts, generation=1)
        else:
            target = queue_lease_records.lease_capacity_pair_from_payload(
                raw_target,
                label=f"lease index repair capacity target {intent_path}",
            )
        if (
            target.aggregate.cluster_kind_counts != counts
            or target.aggregate.global_live_leases != len(indexed)
        ):
            raise QueueConflictError(
                f"lease index repair capacity target disagrees with canonical leases: {intent_path}"
            )
        self._clear_lease_operational_indexes_unlocked()
        for lease, job, _identity in indexed:
            queue_lease_indexes.sync_operational_indexes(
                cast(queue_lease_indexes.QueueLeaseIndexesMixin, self), lease, job=job
            )
        self._lease_capacity_record_paths_unlocked(allow_missing=True)
        self._write_lease_capacity_pair_unlocked(target)
        restore_complete = payload.get("restore_migration_complete", False)
        if not isinstance(restore_complete, bool):
            raise QueueConflictError(
                f"lease index repair migration policy is invalid: {intent_path}"
            )
        migration_state = self._read_index_migration_state()
        migration_state["lease_operational_repair"] = {
            "complete": True,
            "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
            "record_count": len(indexed),
        }
        migration_state["lease_capacity_aggregate"] = {
            "complete": True,
            "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
            "epoch_id": target.aggregate.epoch_id,
            "generation": target.aggregate.generation,
            "record_count": len(indexed),
        }
        if restore_complete:
            migration_state["complete"] = queue_index_state.index_migration_components_complete(
                migration_state
            )
        self._write_index_migration_state(migration_state)
        self._before_lease_capacity_intent_removal("lease_index_repair", intent_path)
        queue_store_write.unlink_durable_path(intent_path, missing_ok=True)
        return len(indexed)

    def _clear_lease_operational_indexes_unlocked(self) -> None:
        roots = tuple(
            self._storage_root / family
            for family in (
                "lease_indexes",
                "lease_identity_refs",
                "leases_by_endpoint",
                "leases_by_cluster_kind",
                "leases_by_expiry",
            )
        )
        files: list[Path] = []
        directories: list[Path] = []
        remaining = queue_layout.MAX_LIVE_LEASE_RECORDS * 8 + 10_000

        def inspect(directory: Path, *, depth: int) -> None:
            nonlocal remaining
            if depth > 3:
                raise QueueConflictError(
                    f"lease operational index exceeds its maximum depth: {directory}"
                )
            self._require_safe_lease_index_directory(directory, create=depth == 0)
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        remaining -= 1
                        if remaining < 0:
                            raise QueueConflictError(
                                "lease operational index repair exceeded its entry bound"
                            )
                        entry_path = Path(entry.path)
                        entry_stat = os.lstat(entry.path)
                        if stat.S_ISDIR(entry_stat.st_mode) and not queue_layout.record_is_reparse(
                            entry_stat
                        ):
                            inspect(entry_path, depth=depth + 1)
                            directories.append(entry_path)
                            continue
                        if (
                            not stat.S_ISREG(entry_stat.st_mode)
                            or queue_layout.record_is_reparse(entry_stat)
                            or entry_stat.st_nlink != 1
                        ):
                            raise QueueConflictError(
                                f"lease operational index contains an unsafe entry: {entry_path}"
                            )
                        files.append(entry_path)
            except OSError as exc:
                raise queue_conflict_from_cause(
                    f"cannot inspect lease operational index {directory}",
                    cause=exc,
                    logger=logger,
                ) from exc

        for root in roots:
            inspect(root, depth=0)
        for path in files:
            queue_store_write.unlink_durable_path(path)
        for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            path.rmdir()

    def _prepare_lease_capacity_rebuild_intent_unlocked(
        self,
        *,
        identity: str,
        limit: int,
    ) -> tuple[Path, dict[str, object]]:
        """Persist a deterministic target epoch before any repair-side mutation."""
        _indexed, counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        target = queue_lease_records.new_lease_capacity_pair(counts, generation=1)
        payload: dict[str, object] = {
            "limit": limit,
            "lease_capacity_rebuild": queue_lease_records.lease_capacity_pair_payload(target),
            "restore_migration_complete": identity == "operator",
        }
        return (
            self._write_transition_intent_unlocked(
                "lease_index_repair",
                identity,
                payload,
            ),
            payload,
        )
