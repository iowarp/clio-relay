"""Durable lease operational-index ownership: identity, refs, scans, sync.

Owns lease-index identity derivation/validation, every zero-byte reference
path (endpoint, cluster-kind, expiry, identity), the manifest read/write
primitives, the bounded directory scans that back the capacity audit, and
the sync/delete convergence that keeps those refs consistent with one
canonical lease. ``sync_operational_indexes`` is a module-level function
(not only a bound method) because ``queue_lease_capacity_audit.py``'s repair
path must resolve it through a lookup a test can patch on this module --
design doc CQ15 row: "Patch ``queue_lease_capacity_audit.queue_lease_indexes.
sync_operational_indexes``, then each lifecycle/recovery job-write lookup."
The bound method ``_sync_lease_operational_indexes_unlocked`` stays a real,
directly patchable instance method (``queue_transition_crash_fixture.py``
and ``test_operational_indexes.py`` call/patch it by that exact name) and is
a thin wrapper over the module function, matching the ``queue_jobs.write_job``
precedent. ``lease_operational_records_present`` also lives here per the
design's disjoint-inventory note even though its only callers today remain
facade-resident startup/migration code (not yet split); it is an I/O-bearing
scan of the same five index families this owner already manages.

CQ20 dissolution: this owner's two remaining facade-wrapper calls each had
exactly one caller in the whole codebase, right here. The bounded manifest
write (``_write_text``) now calls ``queue_store_write.write_text`` directly,
module-qualified; the root-descriptor stat (``_storage_root_stat``) now
calls the composed ``self._layout.storage_root_stat()`` directly, matching
``queue_legacy_audit.py``'s own established ``self._layout`` usage.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING

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
_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseExpiryReference = queue_layout.LeaseExpiryReference


def lease_operational_records_present(root: Path) -> bool:
    """Return whether any lease operational-index family holds a record."""
    for family in (
        "lease_indexes",
        "lease_identity_refs",
        "leases_by_endpoint",
        "leases_by_cluster_kind",
        "leases_by_expiry",
    ):
        directory = root / family
        try:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    return True
        except FileNotFoundError:
            continue
        except OSError:
            return True
    return False


def sync_operational_indexes(
    queue: QueueLeaseIndexesMixin,
    lease: Lease,
    *,
    job: RelayJob,
    previous_lease: Lease | None = None,
) -> _LeaseIndexIdentity:
    """Converge exact endpoint, cluster-kind, and expiry refs for one lease."""
    identity = queue._lease_index_identity(lease, job=job)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    previous: _LeaseIndexIdentity | None = None
    if previous_lease is not None:
        previous = queue._lease_index_identity(previous_lease, job=job)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        if (
            previous.lease_id != identity.lease_id
            or previous.job_id != identity.job_id
            or previous.endpoint_id != identity.endpoint_id
        ):
            raise QueueConflictError(
                f"lease renewal changed immutable identity: {identity.lease_id}"
            )
        for stale_path in (
            queue._lease_endpoint_ref_path(previous),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_endpoint_guard_path(previous),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_cluster_kind_ref_path(previous),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_expiry_ref_path(previous),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_identity_ref_path(previous),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ):
            if stale_path not in {
                queue._lease_endpoint_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._lease_endpoint_guard_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._lease_cluster_kind_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._lease_expiry_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._lease_identity_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            }:
                queue._require_safe_lease_index_directory(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    stale_path.parent,
                    create=False,
                )
                queue_store_write.unlink_durable_path(stale_path, missing_ok=True)
    queue._write_lease_index_identity_unlocked(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    for path in (
        queue._lease_identity_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._lease_endpoint_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._lease_endpoint_guard_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._lease_cluster_kind_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._lease_expiry_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    ):
        queue._require_safe_lease_index_directory(path.parent, create=True)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue_store_write.write_text(
            queue._storage_root,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            path,
            "",
        )
    return identity


class QueueLeaseIndexesMixin:
    """Own lease operational-index identity, refs, scans, and convergence."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol
    _layout: queue_layout.QueueLayout

    if TYPE_CHECKING:

        def _write_json(self, path: Path, record: dict[str, object]) -> None: ...

    def _lease_index_identity(
        self,
        lease: Lease,
        *,
        job: RelayJob,
    ) -> _LeaseIndexIdentity:
        """Bind a lease to the immutable job attributes used by operational indexes."""
        if lease.job_id != job.job_id:
            raise QueueConflictError(f"lease job identity mismatch: {lease.lease_id}/{job.job_id}")
        for value, label in (
            (lease.lease_id, "lease id"),
            (lease.job_id, "lease job id"),
            (lease.endpoint_id, "lease endpoint id"),
        ):
            queue_layout.QueueLayout.require_durable_record_id(value, field=label.replace(" ", "_"))
        return _LeaseIndexIdentity(
            lease_id=lease.lease_id,
            job_id=lease.job_id,
            endpoint_id=lease.endpoint_id,
            cluster=job.cluster,
            job_kind=job.kind,
            expires_at=lease.expires_at,
        )

    def _lease_index_path(self, lease_id: str) -> Path:
        return self._lease_index_path_from_token(queue_lease_records.lease_index_token(lease_id))

    def _lease_index_path_from_token(self, lease_token: str) -> Path:
        return self._storage_root / "lease_indexes" / f"{lease_token}.json"

    def _lease_identity_ref_path(
        self,
        identity: _LeaseIndexIdentity,
    ) -> Path:
        lease_token, identity_token = queue_lease_records.lease_reference(identity)
        return self._lease_identity_ref_path_from_tokens(lease_token, identity_token)

    def _lease_identity_ref_path_from_tokens(
        self,
        lease_token: str,
        identity_token: str,
    ) -> Path:
        return self._storage_root / "lease_identity_refs" / f"{lease_token}.{identity_token}.ref"

    def _lease_endpoint_directory(self, endpoint_id: str) -> Path:
        return self._lease_endpoint_directory_from_token(
            queue_lease_records.lease_endpoint_token(endpoint_id)
        )

    def _lease_endpoint_directory_from_token(self, endpoint_token: str) -> Path:
        return self._storage_root / "leases_by_endpoint" / endpoint_token

    def _lease_cluster_kind_directory(self, cluster: str, kind: JobKind) -> Path:
        return (
            self._storage_root
            / "leases_by_cluster_kind"
            / queue_lease_records.lease_cluster_token(cluster)
            / kind.value
        )

    def _lease_endpoint_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_endpoint_directory(
            identity.endpoint_id
        ) / queue_lease_records.lease_scope_ref_name(
            identity,
            "endpoint",
            queue_lease_records.lease_endpoint_token(identity.endpoint_id),
        )

    def _lease_endpoint_guard_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_endpoint_ref_path(identity).with_suffix(".guard")

    def _lease_cluster_kind_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_cluster_kind_directory(
            identity.cluster,
            identity.job_kind,
        ) / queue_lease_records.lease_scope_ref_name(
            identity,
            "cluster-kind",
            queue_lease_records.lease_cluster_token(identity.cluster),
            identity.job_kind.value,
        )

    def _lease_expiry_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return (
            self._storage_root
            / "leases_by_expiry"
            / queue_lease_records.lease_expiry_ref_name(identity)
        )

    def _write_lease_index_identity_unlocked(self, identity: _LeaseIndexIdentity) -> None:
        path = self._lease_index_path(identity.lease_id)
        self._require_safe_lease_index_directory(path.parent, create=True)
        if os.path.lexists(path):
            existing = self._read_lease_index_identity_by_token(
                queue_lease_records.lease_index_token(identity.lease_id)
            )
            if existing.lease_id != identity.lease_id:
                raise QueueConflictError(
                    f"lease operational index token collision: {identity.lease_id}"
                )
        self._write_json(
            path,
            queue_lease_records.lease_index_document(identity),
        )

    def _read_lease_index_identity(self, lease_id: str) -> _LeaseIndexIdentity:
        identity = self._read_lease_index_identity_by_token(
            queue_lease_records.lease_index_token(lease_id)
        )
        if identity.lease_id != lease_id:
            raise QueueConflictError(
                f"lease operational index identity mismatch: {self._lease_index_path(lease_id)}"
            )
        return identity

    def _read_lease_index_identity_by_token(
        self,
        lease_token: str,
        identity_token: str | None = None,
    ) -> _LeaseIndexIdentity:
        path = self._lease_index_path_from_token(lease_token)
        self._require_safe_lease_index_directory(path.parent, create=False)
        try:
            raw = queue_store_read.read_json_document(path)
        except FileNotFoundError as exc:
            raise QueueConflictError(f"lease operational index is missing: {lease_token}") from exc
        identity = queue_lease_records.lease_index_identity_from_document(
            raw,
            label=f"lease operational index {path}",
        )
        if queue_lease_records.lease_index_token(identity.lease_id) != lease_token:
            raise QueueConflictError(f"lease operational index identity mismatch: {path}")
        if (
            identity_token is not None
            and queue_lease_records.lease_identity_token(identity) != identity_token
        ):
            raise QueueConflictError(f"lease operational index binding mismatch: {path}")
        return identity

    def _validate_lease_index_identity(
        self,
        lease: Lease,
        identity: _LeaseIndexIdentity,
    ) -> None:
        if (
            lease.lease_id != identity.lease_id
            or lease.job_id != identity.job_id
            or lease.endpoint_id != identity.endpoint_id
            or lease.expires_at != identity.expires_at
        ):
            raise QueueConflictError(
                f"canonical lease and operational index disagree: {lease.lease_id}"
            )

    def _sync_lease_operational_indexes_unlocked(
        self,
        lease: Lease,
        *,
        job: RelayJob,
        previous_lease: Lease | None = None,
    ) -> _LeaseIndexIdentity:
        """Converge exact endpoint, cluster-kind, and expiry refs for one lease."""
        return sync_operational_indexes(self, lease, job=job, previous_lease=previous_lease)

    def _delete_lease_operational_indexes_unlocked(
        self,
        identity: _LeaseIndexIdentity,
        *,
        allow_foreign_manifest: bool = False,
    ) -> None:
        index_path = self._lease_index_path(identity.lease_id)
        self._require_safe_lease_index_directory(index_path.parent, create=False)
        owns_manifest = os.path.lexists(index_path)
        if owns_manifest:
            indexed = self._read_lease_index_identity_by_token(
                queue_lease_records.lease_index_token(identity.lease_id)
            )
            if indexed != identity:
                if not allow_foreign_manifest:
                    raise QueueConflictError(
                        f"lease operational index token is occupied: {identity.lease_id}"
                    )
                owns_manifest = False
        for path in (
            self._lease_endpoint_ref_path(identity),
            self._lease_endpoint_guard_path(identity),
            self._lease_cluster_kind_ref_path(identity),
            self._lease_expiry_ref_path(identity),
            self._lease_identity_ref_path(identity),
        ):
            self._require_safe_lease_index_directory(path.parent, create=False)
            queue_store_write.unlink_durable_path(path, missing_ok=True)
        endpoint_directory = self._lease_endpoint_directory(identity.endpoint_id)
        if endpoint_directory.exists():
            with os.scandir(endpoint_directory) as entries:
                endpoint_empty = next(entries, None) is None
            if endpoint_empty:
                endpoint_directory.rmdir()
        if owns_manifest and os.path.lexists(index_path):
            queue_store_write.unlink_durable_path(index_path)

    def _require_safe_lease_index_directory(
        self,
        directory: Path,
        *,
        create: bool,
    ) -> bool:
        try:
            relative = directory.relative_to(self._storage_root)
        except ValueError as exc:
            raise QueueConflictError(
                f"lease index directory escaped queue root: {directory}"
            ) from exc
        if not relative.parts or relative.parts[0] not in {
            "lease_indexes",
            "lease_identity_refs",
            "leases_by_endpoint",
            "leases_by_cluster_kind",
            "leases_by_expiry",
        }:
            raise QueueConflictError(f"unsupported lease index directory: {directory}")
        try:
            root_stat = self._layout.storage_root_stat()
        except FileNotFoundError as exc:
            raise QueueConflictError(f"queue root is missing: {self._storage_root}") from exc
        if not stat.S_ISDIR(root_stat.st_mode) or queue_layout.record_is_reparse(root_stat):
            raise QueueConflictError(f"queue root is unsafe: {self._storage_root}")
        current = self._storage_root
        for part in relative.parts:
            current /= part
            try:
                current_stat = os.lstat(current)
            except FileNotFoundError:
                if not create:
                    return False
                current.mkdir()
                current_stat = os.lstat(current)
            if not stat.S_ISDIR(current_stat.st_mode) or queue_layout.record_is_reparse(
                current_stat
            ):
                raise QueueConflictError(f"lease index ancestry is unsafe: {current}")
        return True

    def _scan_lease_scope_refs(
        self,
        directory: Path,
        *,
        scope: tuple[str, ...],
        limit: int,
        label: str,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Enumerate structurally bound zero-byte refs without opening lease JSON."""
        if limit < 1:
            raise ValueError("lease reference scan limit must be at least 1")
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(f"{label} is not a safe directory: {directory}")
        self._require_safe_lease_index_directory(directory, create=False)
        lease_refs: list[tuple[str, str]] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(lease_refs) >= limit:
                        return sorted(lease_refs), True
                    lease_ref = queue_lease_records.lease_reference_from_scope_ref(
                        entry.name, *scope
                    )
                    entry_stat = os.lstat(entry.path)
                    if (
                        lease_ref is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or queue_layout.record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            f"{label} contains an unsafe lease reference: {entry.path}"
                        )
                    lease_refs.append(lease_ref)
        except OSError as exc:
            raise queue_conflict_from_cause(
                f"cannot scan {label}",
                cause=exc,
                logger=logger,
            ) from exc
        return sorted(lease_refs), False

    def _scan_expiry_refs(
        self,
        *,
        limit: int,
    ) -> tuple[list[_LeaseExpiryReference], bool]:
        """Enumerate bounded expiry identities entirely from validated filenames."""
        directory = self._storage_root / "leases_by_expiry"
        self._require_safe_lease_index_directory(directory, create=False)
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(f"lease expiry index is not a safe directory: {directory}")
        refs: list[_LeaseExpiryReference] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(refs) >= limit:
                        return sorted(refs), True
                    parsed = queue_lease_records.parse_lease_expiry_ref_name(entry.name)
                    entry_stat = os.lstat(entry.path)
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or queue_layout.record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            f"lease expiry index contains an unsafe reference: {entry.path}"
                        )
                    refs.append(parsed)
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot scan lease expiry index",
                cause=exc,
                logger=logger,
            ) from exc
        return sorted(refs), False

    def _scan_lease_identity_refs(
        self,
        *,
        limit: int,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Enumerate bounded identity sentinels without opening manifest JSON."""
        if limit < 1:
            raise ValueError("lease identity reference scan limit must be at least 1")
        directory = self._storage_root / "lease_identity_refs"
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(
                f"lease identity reference index is not a safe directory: {directory}"
            )
        self._require_safe_lease_index_directory(directory, create=False)
        refs: list[tuple[str, str]] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(refs) >= limit:
                        return sorted(refs), True
                    parsed = queue_lease_records.parse_lease_identity_ref_name(entry.name)
                    entry_stat = os.lstat(entry.path)
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or queue_layout.record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            "lease identity reference index contains an unsafe "
                            f"reference: {entry.path}"
                        )
                    refs.append(parsed)
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot scan lease identity reference index",
                cause=exc,
                logger=logger,
            ) from exc
        return sorted(refs), False

    def _scan_lease_endpoint_refs(
        self,
        endpoint_id: str,
        *,
        limit: int,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Validate redundant refs from exactly one endpoint shard."""
        if limit < 1:
            raise ValueError("lease endpoint reference scan limit must be at least 1")
        directory = self._lease_endpoint_directory(endpoint_id)
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(f"lease endpoint index is not a safe directory: {directory}")
        self._require_safe_lease_index_directory(directory, create=False)
        endpoint_token = queue_lease_records.lease_endpoint_token(endpoint_id)
        references: set[tuple[str, str]] = set()
        guards: set[tuple[str, str]] = set()
        file_count = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    file_count += 1
                    if file_count > limit * 2:
                        return sorted(references), True
                    entry_stat = os.lstat(entry.path)
                    if entry.name.endswith(".guard"):
                        parsed = queue_lease_records.lease_reference_from_scope_ref(
                            f"{entry.name[: -len('.guard')]}.ref",
                            "endpoint",
                            endpoint_token,
                        )
                        target = guards
                    else:
                        parsed = queue_lease_records.lease_reference_from_scope_ref(
                            entry.name,
                            "endpoint",
                            endpoint_token,
                        )
                        target = references
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or queue_layout.record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                        or parsed in target
                    ):
                        raise QueueConflictError(
                            f"lease endpoint index contains an unsafe reference: {entry.path}"
                        )
                    target.add(parsed)
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot scan lease endpoint index",
                cause=exc,
                logger=logger,
            ) from exc
        if references != guards:
            raise QueueConflictError(
                f"lease endpoint references and guards disagree: {endpoint_id}"
            )
        return sorted(references), False

    def _require_empty_lease_ref(
        self,
        path: Path,
        *,
        label: str,
    ) -> None:
        self._require_safe_lease_index_directory(path.parent, create=False)
        try:
            entry_stat = os.lstat(path)
        except FileNotFoundError as exc:
            raise QueueConflictError(f"{label} is missing: {path}") from exc
        if (
            not stat.S_ISREG(entry_stat.st_mode)
            or queue_layout.record_is_reparse(entry_stat)
            or entry_stat.st_size != 0
            or entry_stat.st_nlink != 1
        ):
            raise QueueConflictError(f"{label} is unsafe: {path}")
