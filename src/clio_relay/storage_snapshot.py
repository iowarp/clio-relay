"""Status/health assembly mixin for ``StoragePolicy``.

Owns the read-only side of admission: the cross-process admission lock, the
composed ``status()`` report, the full-tree-scan-backed volume accounting
every reservation mutation's pre-commit safety check (``_snapshot``) builds
on, and the cheap runtime free-space guard (``check_runtime_free_space``/
``_runtime_volume_status``, a per-volume ``shutil.disk_usage`` query with no
tree scan or ledger read) workers poll on a fixed interval. The bounded tree
scan itself (``StoragePolicy._stable_tree_snapshot``, which retries only
transient ``SCAN_CHANGED`` churn) stays on the facade because it calls the
directly monkeypatched ``scan_tree`` by bare name -- see ``storage_policy.
py``'s docstring; every method here reaches it through
``self._stable_tree_snapshot()``, resolved through the composed class.
"""

from __future__ import annotations

import shutil
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.storage_file_io import (
    _prepare_lock_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _safe_lstat,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _validate_private_regular_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _volume_id,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.storage_policy_types import (
    ReservationRecord,
    StorageDecision,
    StorageLimits,
    StoragePolicyError,
    StorageReason,
    StorageStatus,
    StorageTreeSnapshot,
    TreeUsage,
    VolumeStatus,
    _error_decision,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)

if TYPE_CHECKING:
    from clio_relay.storage_policy_types import (
        _LedgerState,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    )


class StorageSnapshotMixin:
    """Own admission serialization, the status report, and volume accounting."""

    admission_lock_path: Path
    core_root: Path
    spool_root: Path
    limits: StorageLimits

    if TYPE_CHECKING:

        def _stable_tree_snapshot(self) -> tuple[TreeUsage, TreeUsage]: ...

        @contextmanager
        def _ledger_lock(self) -> Generator[None, None, None]: ...

        def _read_ledger(self) -> _LedgerState: ...

    @contextmanager
    def admission_lock(self) -> Generator[None, None, None]:
        """Serialize queue identity preparation with cross-process admission.

        The queue lock is intentionally not held while the bounded storage scan
        executes.  This separate lock closes the idempotency race between relay
        producers which share the same production storage policy.
        """
        _prepare_lock_file(self.admission_lock_path)
        lock = FileLock(
            str(internal_filesystem_path(self.admission_lock_path, force_extended=True)),
            timeout=float(self.limits.lock_timeout_seconds),
        )
        try:
            with lock:
                _validate_private_regular_file(self.admission_lock_path, allow_empty=True)
                yield
        except FileLockTimeout as exc:
            raise StoragePolicyError(
                StorageReason.LOCK_TIMEOUT,
                "storage admission coordinator lock timed out",
                details={"timeout_seconds": float(self.limits.lock_timeout_seconds)},
            ) from exc

    def status(self) -> StorageDecision:
        """Return current bounded usage, reservations, and admission health."""
        try:
            tree_snapshot = self.capture_admission_snapshot()
            with self._ledger_lock():
                ledger = self._read_ledger()
                snapshot = self._snapshot(
                    ledger.reservations,
                    ledger.generation,
                    tree_snapshot=tree_snapshot,
                )
            return StorageDecision(
                allowed=snapshot.healthy,
                reason=snapshot.reason,
                message=(
                    "storage policy is healthy"
                    if snapshot.healthy
                    else "storage policy is over a configured safety threshold"
                ),
                status=snapshot,
            )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def capture_admission_snapshot(self) -> StorageTreeSnapshot:
        """Scan bounded storage trees without holding an admission or ledger lock."""

        core, spool = self._stable_tree_snapshot()
        return StorageTreeSnapshot(core=core, spool=spool)

    def check_runtime_free_space(self) -> StorageDecision:
        """Query free bytes without scanning storage trees or reading the ledger."""
        try:
            volumes = self._runtime_volume_status()
            unsafe_volumes = [volume for volume in volumes if volume["healthy"] is False]
            if unsafe_volumes:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.FILESYSTEM_FREE_RESERVE,
                    message="filesystem free space crossed the runtime safety reserve",
                    details={"volumes": unsafe_volumes},
                )
            return StorageDecision(
                allowed=True,
                reason=StorageReason.HEALTHY,
                message="runtime filesystem free-space guard is healthy",
                details={"volumes": volumes},
            )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def _runtime_volume_status(self) -> list[dict[str, object]]:
        volume_free: dict[str, int] = {}
        volume_families: dict[str, list[str]] = {}
        for family, root in (("core", self.core_root), ("spool", self.spool_root)):
            root_stat = _safe_lstat(root, root=True)
            volume_id = _volume_id(root, root_stat)
            volume_families.setdefault(volume_id, []).append(family)
            if volume_id in volume_free:
                continue
            try:
                volume_free[volume_id] = int(
                    shutil.disk_usage(internal_filesystem_path(root, force_extended=True)).free
                )
            except OSError as exc:
                raise StoragePolicyError(
                    StorageReason.FILESYSTEM_QUERY_FAILED,
                    "filesystem free space could not be queried",
                    details={"volume_id": volume_id, "error": type(exc).__name__},
                ) from exc
        return [
            {
                "volume_id": volume_id,
                "storage_families": sorted(volume_families[volume_id]),
                "free_bytes": free_bytes,
                "minimum_free_bytes": self.limits.minimum_free_bytes,
                "healthy": free_bytes >= self.limits.minimum_free_bytes,
            }
            for volume_id, free_bytes in sorted(volume_free.items())
        ]

    def _snapshot(
        self,
        reservations: tuple[ReservationRecord, ...],
        ledger_generation: int,
        *,
        tree_snapshot: StorageTreeSnapshot | None = None,
    ) -> StorageStatus:
        if tree_snapshot is None:
            core, spool = self._stable_tree_snapshot()
        else:
            core, spool = tree_snapshot.core, tree_snapshot.spool
            if core.root != str(self.core_root) or spool.root != str(self.spool_root):
                raise StoragePolicyError(
                    StorageReason.INVALID_REQUEST,
                    "storage tree snapshot belongs to different configured roots",
                )
        reserved_core = sum(record.core_bytes for record in reservations)
        reserved_spool = sum(record.spool_bytes for record in reservations)
        volumes = self._volume_status(reserved_core, reserved_spool)
        reason = StorageReason.HEALTHY
        if core.bytes + reserved_core > self.limits.core_high_water_bytes:
            reason = StorageReason.CORE_HIGH_WATER
        elif spool.bytes + reserved_spool > self.limits.spool_high_water_bytes:
            reason = StorageReason.SPOOL_HIGH_WATER
        elif (
            core.bytes + spool.bytes + reserved_core + reserved_spool
            > self.limits.total_high_water_bytes
        ):
            reason = StorageReason.TOTAL_HIGH_WATER
        elif any(not volume.healthy for volume in volumes):
            reason = StorageReason.FILESYSTEM_FREE_RESERVE
        return StorageStatus(
            healthy=reason is StorageReason.HEALTHY,
            reason=reason,
            core=core,
            spool=spool,
            reserved_core_bytes=reserved_core,
            reserved_spool_bytes=reserved_spool,
            reservation_count=len(reservations),
            ledger_generation=ledger_generation,
            volumes=volumes,
            limits=self.limits,
        )

    def _volume_status(self, reserved_core: int, reserved_spool: int) -> tuple[VolumeStatus, ...]:
        roots = (
            ("core", self.core_root, reserved_core),
            ("spool", self.spool_root, reserved_spool),
        )
        grouped: dict[str, list[tuple[str, Path, int]]] = {}
        for family, root, reserved in roots:
            root_stat = _safe_lstat(root, root=True)
            volume_id = _volume_id(root, root_stat)
            grouped.setdefault(volume_id, []).append((family, root, reserved))
        result: list[VolumeStatus] = []
        for volume_id in sorted(grouped):
            members = grouped[volume_id]
            try:
                free = int(
                    shutil.disk_usage(
                        internal_filesystem_path(members[0][1], force_extended=True)
                    ).free
                )
            except OSError as exc:
                raise StoragePolicyError(
                    StorageReason.FILESYSTEM_QUERY_FAILED,
                    "filesystem free space could not be queried",
                    details={"volume_id": volume_id, "error": type(exc).__name__},
                ) from exc
            reserved = sum(member[2] for member in members)
            result.append(
                VolumeStatus(
                    volume_id=volume_id,
                    storage_families=tuple(sorted(member[0] for member in members)),
                    free_bytes=free,
                    reserved_bytes=reserved,
                    available_after_reservations_bytes=free - reserved,
                    minimum_free_bytes=self.limits.minimum_free_bytes,
                )
            )
        return tuple(result)
