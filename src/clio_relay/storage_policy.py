"""Durable storage admission and accounting for relay queue data.

The policy deliberately has no knowledge of clusters, schedulers, or workloads.  It
accounts the two operator-configured relay storage trees, reserves expected growth
per job, and rejects admission when a bounded safety check cannot be completed.

The ledger checksum detects corruption and torn/manual edits. It is not a MAC,
signature, or authenticity proof; filesystem ownership and private permissions are
the trust boundary for local policy state.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, Literal, cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

_LEDGER_SCHEMA: Final = "clio-relay.storage-reservations.v1"
_STATUS_SCHEMA: Final = "clio-relay.storage-status.v1"
_DECISION_SCHEMA: Final = "clio-relay.storage-decision.v1"
_JOB_ID_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MIB: Final = 1024**2
_GIB: Final = 1024**3
_TIB: Final = 1024**4
_FILE_ATTRIBUTE_REPARSE_POINT: Final = 0x400
DEFAULT_JOB_CORE_ALLOWANCE_BYTES: Final = 64 * _MIB
DEFAULT_JOB_RESULT_ALLOWANCE_BYTES: Final = 256 * _MIB
DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS: Final = 5.0
STORAGE_SNAPSHOT_SCAN_ATTEMPTS: Final = 25
STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS: Final = 0.01


class StorageReason(StrEnum):
    """Stable machine-readable outcomes returned by the storage policy."""

    HEALTHY = "healthy"
    RESERVED = "reserved"
    RESERVATION_IDEMPOTENT = "reservation_idempotent"
    RESERVATION_RELEASED = "reservation_released"
    RESERVATION_ABSENT = "reservation_absent"
    RECONCILED = "reconciled"
    JOB_RESERVATION_EXCEEDED = "job_reservation_exceeded"
    INVALID_REQUEST = "invalid_request"
    RESERVATION_CONFLICT = "reservation_conflict"
    PER_JOB_LIMIT = "per_job_limit"
    CORE_HIGH_WATER = "core_high_water"
    SPOOL_HIGH_WATER = "spool_high_water"
    TOTAL_HIGH_WATER = "total_high_water"
    FILESYSTEM_FREE_RESERVE = "filesystem_free_reserve"
    LEDGER_MALFORMED = "ledger_malformed"
    LEDGER_OVERSIZED = "ledger_oversized"
    LEDGER_UNSAFE = "ledger_unsafe"
    LEDGER_CAPACITY = "ledger_capacity"
    LOCK_TIMEOUT = "lock_timeout"
    PERSISTENCE_FAILURE = "persistence_failure"
    SCAN_ROOT_INVALID = "scan_root_invalid"
    SCAN_UNSAFE_ENTRY = "scan_unsafe_entry"
    SCAN_CHANGED = "scan_changed"
    SCAN_IO_ERROR = "scan_io_error"
    SCAN_ENTRY_LIMIT = "scan_entry_limit"
    SCAN_DEPTH_LIMIT = "scan_depth_limit"
    SCAN_BYTE_LIMIT = "scan_byte_limit"
    FILESYSTEM_QUERY_FAILED = "filesystem_query_failed"


class StoragePolicyError(RuntimeError):
    """Internal fail-closed error with a stable public reason code."""

    def __init__(
        self,
        reason: StorageReason,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, object]:
        """Return a machine-readable error representation."""
        return {
            "reason": self.reason.value,
            "message": str(self),
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class StorageLimits:
    """Configurable production limits for storage admission.

    Defaults leave room for ordinary CI filesystems while still imposing finite
    bounds. Sites with large artifacts should explicitly raise these values.
    """

    core_high_water_bytes: int = 10 * _GIB
    spool_high_water_bytes: int = 100 * _GIB
    total_high_water_bytes: int = 110 * _GIB
    minimum_free_bytes: int = _GIB
    max_job_reservation_bytes: int = 10 * _GIB
    max_scan_entries: int = 1_000_000
    max_scan_depth: int = 64
    max_scan_accounted_bytes: int = 2 * _TIB
    max_ledger_bytes: int = 8 * _MIB
    max_reservations: int = 50_000
    lock_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        integer_fields = (
            "core_high_water_bytes",
            "spool_high_water_bytes",
            "total_high_water_bytes",
            "minimum_free_bytes",
            "max_job_reservation_bytes",
            "max_scan_entries",
            "max_scan_depth",
            "max_scan_accounted_bytes",
            "max_ledger_bytes",
            "max_reservations",
        )
        for field_name in integer_fields:
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        positive_fields = (
            "core_high_water_bytes",
            "spool_high_water_bytes",
            "total_high_water_bytes",
            "max_job_reservation_bytes",
            "max_scan_entries",
            "max_scan_depth",
            "max_scan_accounted_bytes",
            "max_ledger_bytes",
            "max_reservations",
        )
        for field_name in positive_fields:
            if getattr(self, field_name) == 0:
                raise ValueError(f"{field_name} must be greater than zero")
        if not _is_non_boolean_number(self.lock_timeout_seconds):
            raise ValueError("lock_timeout_seconds must be a positive number")
        if not 0 < float(self.lock_timeout_seconds) <= 300:
            raise ValueError("lock_timeout_seconds must be greater than zero and at most 300")
        if self.total_high_water_bytes < max(
            self.core_high_water_bytes, self.spool_high_water_bytes
        ):
            raise ValueError(
                "total_high_water_bytes must be at least each individual high-water limit"
            )
        if self.max_scan_accounted_bytes < self.max_job_reservation_bytes:
            raise ValueError("max_scan_accounted_bytes must be at least max_job_reservation_bytes")

    def to_dict(self) -> dict[str, object]:
        """Serialize configured limits for reports and status APIs."""
        return {
            "core_high_water_bytes": self.core_high_water_bytes,
            "spool_high_water_bytes": self.spool_high_water_bytes,
            "total_high_water_bytes": self.total_high_water_bytes,
            "minimum_free_bytes": self.minimum_free_bytes,
            "max_job_reservation_bytes": self.max_job_reservation_bytes,
            "max_scan_entries": self.max_scan_entries,
            "max_scan_depth": self.max_scan_depth,
            "max_scan_accounted_bytes": self.max_scan_accounted_bytes,
            "max_ledger_bytes": self.max_ledger_bytes,
            "max_reservations": self.max_reservations,
            "lock_timeout_seconds": float(self.lock_timeout_seconds),
        }


@dataclass(frozen=True, slots=True)
class ReservationRecord:
    """Durable expected storage growth reserved for one relay job."""

    job_id: str
    core_bytes: int
    spool_bytes: int
    created_at: str

    @property
    def total_bytes(self) -> int:
        """Return the total reservation across both storage families."""
        return self.core_bytes + self.spool_bytes

    def to_dict(self) -> dict[str, object]:
        """Serialize the reservation without implementation-specific fields."""
        return {
            "job_id": self.job_id,
            "core_bytes": self.core_bytes,
            "spool_bytes": self.spool_bytes,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class TreeUsage:
    """Bounded logical-byte accounting for one storage tree."""

    root: str
    bytes: int
    files: int
    links: int
    directories: int
    entries: int

    def to_dict(self) -> dict[str, object]:
        """Serialize the tree accounting result."""
        return {
            "root": self.root,
            "bytes": self.bytes,
            "files": self.files,
            "links": self.links,
            "directories": self.directories,
            "entries": self.entries,
            "complete": True,
        }


@dataclass(frozen=True, slots=True)
class VolumeStatus:
    """Free-space accounting for one filesystem volume."""

    volume_id: str
    storage_families: tuple[str, ...]
    free_bytes: int
    reserved_bytes: int
    available_after_reservations_bytes: int
    minimum_free_bytes: int

    @property
    def healthy(self) -> bool:
        """Return whether the volume retains its configured free reserve."""
        return self.available_after_reservations_bytes >= self.minimum_free_bytes

    def to_dict(self) -> dict[str, object]:
        """Serialize the volume accounting result."""
        return {
            "volume_id": self.volume_id,
            "storage_families": list(self.storage_families),
            "free_bytes": self.free_bytes,
            "reserved_bytes": self.reserved_bytes,
            "available_after_reservations_bytes": self.available_after_reservations_bytes,
            "minimum_free_bytes": self.minimum_free_bytes,
            "healthy": self.healthy,
        }


@dataclass(frozen=True, slots=True)
class StorageStatus:
    """Complete storage snapshot used to explain an admission decision."""

    healthy: bool
    reason: StorageReason
    core: TreeUsage
    spool: TreeUsage
    reserved_core_bytes: int
    reserved_spool_bytes: int
    reservation_count: int
    ledger_generation: int
    volumes: tuple[VolumeStatus, ...]
    limits: StorageLimits

    def to_dict(self) -> dict[str, object]:
        """Serialize the snapshot for CLI, HTTP, MCP, or validation reports."""
        return {
            "schema": _STATUS_SCHEMA,
            "healthy": self.healthy,
            "reason": self.reason.value,
            "core": self.core.to_dict(),
            "spool": self.spool.to_dict(),
            "reserved_core_bytes": self.reserved_core_bytes,
            "reserved_spool_bytes": self.reserved_spool_bytes,
            "reserved_total_bytes": self.reserved_core_bytes + self.reserved_spool_bytes,
            "reservation_count": self.reservation_count,
            "ledger_generation": self.ledger_generation,
            "volumes": [volume.to_dict() for volume in self.volumes],
            "limits": self.limits.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class StorageDecision:
    """Machine-readable result for status and mutation operations."""

    allowed: bool
    reason: StorageReason
    message: str
    status: StorageStatus | None = None
    details: Mapping[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the decision with stable schema and reason values."""
        result: dict[str, object] = {
            "schema": _DECISION_SCHEMA,
            "allowed": self.allowed,
            "reason": self.reason.value,
            "message": self.message,
            "details": dict(self.details or {}),
        }
        if self.status is not None:
            result["status"] = self.status.to_dict()
        return result


@dataclass(frozen=True, slots=True)
class _LedgerState:
    generation: int
    reservations: tuple[ReservationRecord, ...]


def scan_tree(
    root: Path,
    *,
    max_entries: int,
    max_depth: int,
    max_accounted_bytes: int,
    link_policy: Literal["reject", "count"] = "reject",
) -> TreeUsage:
    """Account a tree without following links and within explicit scan bounds.

    Logical file sizes are counted once per directory entry. Consequently hard
    links are intentionally counted repeatedly, which is conservative for
    admission and prevents attacker-controlled inode de-duplication from hiding
    expected growth. ``link_policy="count"`` accounts a link's own logical size
    without traversing its target; core/state trees should retain the default
    strict rejection, while workload spools may safely contain output links.
    """
    _require_positive_bound("max_entries", max_entries)
    _require_positive_bound("max_depth", max_depth)
    _require_positive_bound("max_accounted_bytes", max_accounted_bytes)
    if link_policy not in {"reject", "count"}:
        raise ValueError("link_policy must be 'reject' or 'count'")
    normalized = Path(os.path.abspath(root))
    root_stat = _safe_lstat(normalized, root=True)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID,
            "storage root is not a directory",
            details={"root": str(normalized)},
        )

    total_bytes = 0
    file_count = 0
    link_count = 0
    directory_count = 1
    entry_count = 0
    stack: list[tuple[Path, int, int, int]] = [
        (normalized, 0, int(root_stat.st_dev), int(root_stat.st_ino))
    ]
    while stack:
        directory, depth, expected_device, expected_inode = stack.pop()
        try:
            with _scandir_verified(directory, expected_device, expected_inode) as entries:
                for entry in entries:
                    entry_count += 1
                    if entry_count > max_entries:
                        raise StoragePolicyError(
                            StorageReason.SCAN_ENTRY_LIMIT,
                            "storage tree exceeds the configured entry scan limit",
                            details={"root": str(normalized), "max_entries": max_entries},
                        )
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError as exc:
                        raise StoragePolicyError(
                            StorageReason.SCAN_CHANGED,
                            "storage tree changed while it was being accounted",
                            details={"root": str(normalized)},
                        ) from exc
                    except OSError as exc:
                        raise StoragePolicyError(
                            StorageReason.SCAN_IO_ERROR,
                            "storage entry could not be inspected",
                            details={"root": str(normalized), "error": type(exc).__name__},
                        ) from exc
                    child = directory / entry.name
                    if os.name == "nt":
                        # Windows DirEntry.stat() may report zero device/inode
                        # values. lstat supplies a stable file identity and still
                        # does not traverse a symlink or reparse point.
                        try:
                            entry_stat = os.lstat(child)
                        except FileNotFoundError as exc:
                            raise StoragePolicyError(
                                StorageReason.SCAN_CHANGED,
                                "storage tree changed while it was being accounted",
                                details={"root": str(normalized)},
                            ) from exc
                    if _is_link_or_reparse(entry_stat) or entry.is_symlink():
                        if link_policy == "reject":
                            raise StoragePolicyError(
                                StorageReason.SCAN_UNSAFE_ENTRY,
                                "storage tree contains a link or reparse point",
                                details={"root": str(normalized)},
                            )
                        link_count += 1
                        total_bytes += max(0, int(entry_stat.st_size))
                        if total_bytes > max_accounted_bytes:
                            raise StoragePolicyError(
                                StorageReason.SCAN_BYTE_LIMIT,
                                "storage tree exceeds the configured accounting byte limit",
                                details={
                                    "root": str(normalized),
                                    "max_accounted_bytes": max_accounted_bytes,
                                },
                            )
                        continue
                    if stat.S_ISDIR(entry_stat.st_mode):
                        child_depth = depth + 1
                        if child_depth > max_depth:
                            raise StoragePolicyError(
                                StorageReason.SCAN_DEPTH_LIMIT,
                                "storage tree exceeds the configured depth limit",
                                details={"root": str(normalized), "max_depth": max_depth},
                            )
                        directory_count += 1
                        stack.append(
                            (
                                child,
                                child_depth,
                                int(entry_stat.st_dev),
                                int(entry_stat.st_ino),
                            )
                        )
                    elif stat.S_ISREG(entry_stat.st_mode):
                        if entry_stat.st_size < 0:
                            raise StoragePolicyError(
                                StorageReason.SCAN_UNSAFE_ENTRY,
                                "storage entry reported a negative size",
                                details={"root": str(normalized)},
                            )
                        file_count += 1
                        total_bytes += int(entry_stat.st_size)
                        if total_bytes > max_accounted_bytes:
                            raise StoragePolicyError(
                                StorageReason.SCAN_BYTE_LIMIT,
                                "storage tree exceeds the configured accounting byte limit",
                                details={
                                    "root": str(normalized),
                                    "max_accounted_bytes": max_accounted_bytes,
                                },
                            )
                    else:
                        raise StoragePolicyError(
                            StorageReason.SCAN_UNSAFE_ENTRY,
                            "storage tree contains a non-regular entry",
                            details={"root": str(normalized)},
                        )
        except StoragePolicyError:
            raise
        except FileNotFoundError as exc:
            raise StoragePolicyError(
                StorageReason.SCAN_CHANGED,
                "storage tree changed while it was being accounted",
                details={"root": str(normalized)},
            ) from exc
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.SCAN_IO_ERROR,
                "storage directory could not be scanned",
                details={"root": str(normalized), "error": type(exc).__name__},
            ) from exc

    return TreeUsage(
        root=str(normalized),
        bytes=total_bytes,
        files=file_count,
        links=link_count,
        directories=directory_count,
        entries=entry_count,
    )


class StoragePolicy:
    """Coordinate bounded storage checks with a crash-safe reservation ledger."""

    def __init__(
        self,
        core_root: Path,
        spool_root: Path,
        *,
        state_root: Path | None = None,
        limits: StorageLimits | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.core_root = Path(os.path.abspath(core_root))
        self.spool_root = Path(os.path.abspath(spool_root))
        self.state_root = Path(
            os.path.abspath(state_root if state_root is not None else self.core_root / ".storage")
        )
        self.limits = limits or StorageLimits()
        self._clock = clock or (lambda: datetime.now(UTC))
        _reject_overlapping_roots(self.core_root, self.spool_root)
        _ensure_directory_no_links(self.state_root)
        self.ledger_path = self.state_root / "reservations.v1.json"
        self.lock_path = self.state_root / "reservations.v1.lock"
        self.admission_lock_path = self.state_root / "admission.v1.lock"

    @contextmanager
    def admission_lock(self) -> Generator[None, None, None]:
        """Serialize queue identity preparation with cross-process admission.

        The queue lock is intentionally not held while the bounded storage scan
        executes.  This separate lock closes the idempotency race between relay
        producers which share the same production storage policy.
        """
        _prepare_lock_file(self.admission_lock_path)
        lock = FileLock(
            str(self.admission_lock_path),
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
            with self._ledger_lock():
                ledger = self._read_ledger()
                snapshot = self._snapshot(ledger.reservations, ledger.generation)
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

    def reserve(self, job_id: str, *, core_bytes: int, spool_bytes: int) -> StorageDecision:
        """Atomically reserve expected core and spool growth for one job.

        Repeating the same request is idempotent. Reusing a job id with different
        amounts is rejected instead of silently resizing a live reservation.
        """
        try:
            _validate_job_id(job_id)
            _validate_reservation_bytes(core_bytes, spool_bytes, self.limits)
            with self._ledger_lock():
                ledger = self._read_ledger()
                by_job = {record.job_id: record for record in ledger.reservations}
                existing = by_job.get(job_id)
                if existing is not None:
                    if existing.core_bytes != core_bytes or existing.spool_bytes != spool_bytes:
                        return StorageDecision(
                            allowed=False,
                            reason=StorageReason.RESERVATION_CONFLICT,
                            message="job already has a different durable storage reservation",
                            details={"reservation": existing.to_dict()},
                        )
                    snapshot = self._snapshot(ledger.reservations, ledger.generation)
                    if not snapshot.healthy:
                        return StorageDecision(
                            allowed=False,
                            reason=snapshot.reason,
                            message=(
                                "existing reservation is idempotent, but current storage "
                                "pressure denies admission"
                            ),
                            status=snapshot,
                            details={
                                "reservation": existing.to_dict(),
                                "idempotent": True,
                            },
                        )
                    return StorageDecision(
                        allowed=True,
                        reason=StorageReason.RESERVATION_IDEMPOTENT,
                        message="the requested storage reservation already exists",
                        status=snapshot,
                        details={"reservation": existing.to_dict()},
                    )
                if len(ledger.reservations) >= self.limits.max_reservations:
                    return StorageDecision(
                        allowed=False,
                        reason=StorageReason.LEDGER_CAPACITY,
                        message="reservation ledger reached its configured record limit",
                        details={"max_reservations": self.limits.max_reservations},
                    )
                record = ReservationRecord(
                    job_id=job_id,
                    core_bytes=core_bytes,
                    spool_bytes=spool_bytes,
                    created_at=_format_timestamp(self._clock()),
                )
                proposed = tuple(
                    sorted((*ledger.reservations, record), key=lambda item: item.job_id)
                )
                snapshot = self._snapshot(proposed, ledger.generation + 1)
                if not snapshot.healthy:
                    return StorageDecision(
                        allowed=False,
                        reason=snapshot.reason,
                        message="storage reservation would violate a configured safety threshold",
                        status=snapshot,
                        details={"reservation": record.to_dict()},
                    )
                self._write_ledger(_LedgerState(ledger.generation + 1, proposed))
                return StorageDecision(
                    allowed=True,
                    reason=StorageReason.RESERVED,
                    message="storage was reserved for the job",
                    status=snapshot,
                    details={"reservation": record.to_dict()},
                )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def release(self, job_id: str) -> StorageDecision:
        """Idempotently release one job's durable storage reservation."""
        try:
            _validate_job_id(job_id)
            with self._ledger_lock():
                ledger = self._read_ledger()
                existing = next(
                    (record for record in ledger.reservations if record.job_id == job_id), None
                )
                if existing is None:
                    return StorageDecision(
                        allowed=True,
                        reason=StorageReason.RESERVATION_ABSENT,
                        message="job has no storage reservation",
                        details={"job_id": job_id, "ledger_generation": ledger.generation},
                    )
                retained = tuple(
                    record for record in ledger.reservations if record.job_id != job_id
                )
                generation = ledger.generation + 1
                self._write_ledger(_LedgerState(generation, retained))
                return StorageDecision(
                    allowed=True,
                    reason=StorageReason.RESERVATION_RELEASED,
                    message="job storage reservation was released",
                    details={
                        "reservation": existing.to_dict(),
                        "ledger_generation": generation,
                    },
                )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def verify_reservation(
        self,
        job_id: str,
        *,
        core_bytes: int,
        spool_bytes: int,
    ) -> StorageDecision:
        """Verify one existing reservation without performing a storage-tree scan.

        Idempotency replays are not new admission and therefore must not be refused
        merely because unrelated storage pressure appeared after the original job
        was accepted.  They still fail closed when the ledger is unsafe, absent, or
        disagrees with the durable estimate.
        """
        try:
            _validate_job_id(job_id)
            _validate_reservation_bytes(core_bytes, spool_bytes, self.limits)
            with self._ledger_lock():
                ledger = self._read_ledger()
                existing = next(
                    (record for record in ledger.reservations if record.job_id == job_id),
                    None,
                )
            if existing is None:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.RESERVATION_ABSENT,
                    message="active job has no durable storage reservation",
                    details={"job_id": job_id},
                )
            if existing.core_bytes != core_bytes or existing.spool_bytes != spool_bytes:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.RESERVATION_CONFLICT,
                    message="active job disagrees with its durable storage reservation",
                    details={
                        "reservation": existing.to_dict(),
                        "requested": {
                            "core_bytes": core_bytes,
                            "spool_bytes": spool_bytes,
                        },
                    },
                )
            return StorageDecision(
                allowed=True,
                reason=StorageReason.RESERVATION_IDEMPOTENT,
                message="active job storage reservation is present",
                details={
                    "reservation": existing.to_dict(),
                    "ledger_generation": ledger.generation,
                },
            )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def reconcile(self, active_job_ids: Iterable[str]) -> StorageDecision:
        """Release reservations for jobs absent from an authoritative active set.

        The caller owns the queue-specific definition of active. This storage
        module intentionally does not infer job state or scheduler behavior.
        """
        try:
            active = set(active_job_ids)
            if len(active) > self.limits.max_reservations:
                raise StoragePolicyError(
                    StorageReason.INVALID_REQUEST,
                    "active job set exceeds the configured reservation bound",
                    details={"max_reservations": self.limits.max_reservations},
                )
            for job_id in active:
                _validate_job_id(job_id)
            with self._ledger_lock():
                ledger = self._read_ledger()
                released = tuple(
                    sorted(
                        record.job_id
                        for record in ledger.reservations
                        if record.job_id not in active
                    )
                )
                if not released:
                    return StorageDecision(
                        allowed=True,
                        reason=StorageReason.RECONCILED,
                        message="reservation ledger already matches the active job set",
                        details={
                            "released_job_ids": [],
                            "ledger_generation": ledger.generation,
                        },
                    )
                retained = tuple(
                    record for record in ledger.reservations if record.job_id in active
                )
                generation = ledger.generation + 1
                self._write_ledger(_LedgerState(generation, retained))
                return StorageDecision(
                    allowed=True,
                    reason=StorageReason.RECONCILED,
                    message="stale storage reservations were released",
                    details={
                        "released_job_ids": list(released),
                        "ledger_generation": generation,
                    },
                )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def reconcile_reservations(
        self,
        active_reservations: Mapping[str, object],
    ) -> StorageDecision:
        """Atomically adopt active jobs and release reservations for inactive jobs.

        The caller must build ``active_reservations`` from the queue's authoritative
        nonterminal index before calling this method.  That keeps queue reads out of
        the storage-ledger critical section while allowing upgrades from older relay
        versions to adopt already-running jobs without one full-tree scan per job.

        Existing reservations are never resized implicitly.  A changed estimate for
        an active job is a conflict which must be resolved by an operator instead of
        silently reducing or stealing that job's reserved capacity.
        """
        try:
            if len(active_reservations) > self.limits.max_reservations:
                raise StoragePolicyError(
                    StorageReason.INVALID_REQUEST,
                    "active reservation set exceeds the configured reservation bound",
                    details={"max_reservations": self.limits.max_reservations},
                )
            normalized: dict[str, tuple[int, int]] = {}
            for job_id, amounts in active_reservations.items():
                _validate_job_id(job_id)
                if not isinstance(amounts, tuple):
                    raise StoragePolicyError(
                        StorageReason.INVALID_REQUEST,
                        "active reservation amounts must be a two-integer tuple",
                        details={"job_id": job_id},
                    )
                typed_amounts = cast(tuple[object, ...], amounts)
                if (
                    len(typed_amounts) != 2
                    or type(typed_amounts[0]) is not int
                    or type(typed_amounts[1]) is not int
                ):
                    raise StoragePolicyError(
                        StorageReason.INVALID_REQUEST,
                        "active reservation amounts must be a two-integer tuple",
                        details={"job_id": job_id},
                    )
                core_bytes, spool_bytes = cast(tuple[int, int], typed_amounts)
                _validate_reservation_bytes(core_bytes, spool_bytes, self.limits)
                normalized[job_id] = (core_bytes, spool_bytes)

            with self._ledger_lock():
                ledger = self._read_ledger()
                existing_by_job = {record.job_id: record for record in ledger.reservations}
                conflicts: list[dict[str, object]] = []
                for job_id, (core_bytes, spool_bytes) in sorted(normalized.items()):
                    existing = existing_by_job.get(job_id)
                    if existing is not None and (
                        existing.core_bytes != core_bytes or existing.spool_bytes != spool_bytes
                    ):
                        conflicts.append(
                            {
                                "job_id": job_id,
                                "existing": existing.to_dict(),
                                "requested": {
                                    "core_bytes": core_bytes,
                                    "spool_bytes": spool_bytes,
                                },
                            }
                        )
                if conflicts:
                    return StorageDecision(
                        allowed=False,
                        reason=StorageReason.RESERVATION_CONFLICT,
                        message="active jobs disagree with durable storage reservations",
                        details={"conflicts": conflicts},
                    )

                now = _format_timestamp(self._clock())
                proposed_records: list[ReservationRecord] = []
                adopted: list[str] = []
                for job_id, (core_bytes, spool_bytes) in sorted(normalized.items()):
                    existing = existing_by_job.get(job_id)
                    if existing is not None:
                        proposed_records.append(existing)
                        continue
                    adopted.append(job_id)
                    proposed_records.append(
                        ReservationRecord(
                            job_id=job_id,
                            core_bytes=core_bytes,
                            spool_bytes=spool_bytes,
                            created_at=now,
                        )
                    )
                released = sorted(set(existing_by_job) - set(normalized))
                proposed = tuple(proposed_records)
                changed = bool(adopted or released)
                generation = ledger.generation + (1 if changed else 0)
                snapshot = self._snapshot(proposed, generation)
                if not snapshot.healthy:
                    if changed:
                        # Reconciliation records reality; it is not new admission.
                        # Persist the authoritative active set even under pressure so
                        # a restart cannot make existing work disappear from accounting.
                        self._write_ledger(_LedgerState(generation, proposed))
                    return StorageDecision(
                        allowed=False,
                        reason=snapshot.reason,
                        message=(
                            "active storage reservations violate a configured safety threshold"
                        ),
                        status=snapshot,
                        details={
                            "adopted_job_ids": adopted,
                            "released_job_ids": released,
                            "persisted": changed,
                        },
                    )
                if changed:
                    self._write_ledger(_LedgerState(generation, proposed))
                return StorageDecision(
                    allowed=True,
                    reason=StorageReason.RECONCILED,
                    message=(
                        "active jobs and storage reservations were reconciled"
                        if changed
                        else "reservation ledger already matches active jobs"
                    ),
                    status=snapshot,
                    details={
                        "adopted_job_ids": adopted,
                        "released_job_ids": released,
                        "ledger_generation": generation,
                        "persisted": changed,
                    },
                )
        except StoragePolicyError as exc:
            return _error_decision(exc)

    def check_runtime_job(self, job_id: str, *, spool_path: Path) -> StorageDecision:
        """Run a bounded per-job growth and constant-time free-space safety check.

        This deliberately does not scan either complete storage tree.  Workers may
        call it at a fixed interval while a child is running: only the owned job
        spool is traversed, and filesystem free space is queried once per volume.
        """
        try:
            _validate_job_id(job_id)
            expected_path = self.spool_root / job_id
            if Path(os.path.abspath(spool_path)) != expected_path:
                raise StoragePolicyError(
                    StorageReason.INVALID_REQUEST,
                    "runtime spool path does not match the reserved job identity",
                    details={"job_id": job_id},
                )
            with self._ledger_lock():
                ledger = self._read_ledger()
                reservation = next(
                    (record for record in ledger.reservations if record.job_id == job_id),
                    None,
                )
            if reservation is None:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.RESERVATION_ABSENT,
                    message="active job has no durable storage reservation",
                    details={"job_id": job_id},
                )

            reservation_scan_bound = reservation.spool_bytes + 1
            scan_bound = min(
                self.limits.max_scan_accounted_bytes,
                max(1, reservation_scan_bound),
            )
            try:
                usage = scan_tree(
                    expected_path,
                    max_entries=self.limits.max_scan_entries,
                    max_depth=self.limits.max_scan_depth,
                    max_accounted_bytes=scan_bound,
                    link_policy="count",
                )
            except StoragePolicyError as exc:
                if (
                    exc.reason is StorageReason.SCAN_BYTE_LIMIT
                    and reservation_scan_bound <= self.limits.max_scan_accounted_bytes
                ):
                    return StorageDecision(
                        allowed=False,
                        reason=StorageReason.JOB_RESERVATION_EXCEEDED,
                        message="job spool exceeded its durable storage reservation",
                        details={
                            "job_id": job_id,
                            "reserved_spool_bytes": reservation.spool_bytes,
                        },
                    )
                raise
            if usage.bytes > reservation.spool_bytes:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.JOB_RESERVATION_EXCEEDED,
                    message="job spool exceeded its durable storage reservation",
                    details={
                        "job_id": job_id,
                        "spool_usage": usage.to_dict(),
                        "reserved_spool_bytes": reservation.spool_bytes,
                    },
                )

            volumes = self._runtime_volume_status()
            unsafe_volumes = [volume for volume in volumes if volume["healthy"] is False]
            if unsafe_volumes:
                return StorageDecision(
                    allowed=False,
                    reason=StorageReason.FILESYSTEM_FREE_RESERVE,
                    message="filesystem free space crossed the runtime safety reserve",
                    details={"job_id": job_id, "volumes": unsafe_volumes},
                )
            return StorageDecision(
                allowed=True,
                reason=StorageReason.HEALTHY,
                message="runtime storage guard is healthy",
                details={
                    "job_id": job_id,
                    "reservation": reservation.to_dict(),
                    "spool_usage": usage.to_dict(),
                    "volumes": volumes,
                },
            )
        except StoragePolicyError as exc:
            return _error_decision(exc)

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
                volume_free[volume_id] = int(shutil.disk_usage(root).free)
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

    @contextmanager
    def _ledger_lock(self) -> Generator[None, None, None]:
        _prepare_lock_file(self.lock_path)
        lock = FileLock(str(self.lock_path), timeout=float(self.limits.lock_timeout_seconds))
        try:
            with lock:
                _validate_private_regular_file(self.lock_path, allow_empty=True)
                yield
        except FileLockTimeout as exc:
            raise StoragePolicyError(
                StorageReason.LOCK_TIMEOUT,
                "storage reservation ledger lock timed out",
                details={"timeout_seconds": float(self.limits.lock_timeout_seconds)},
            ) from exc

    def _read_ledger(self) -> _LedgerState:
        try:
            os.lstat(self.ledger_path)
        except FileNotFoundError:
            return _LedgerState(generation=0, reservations=())
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage reservation ledger could not be inspected",
                details={"error": type(exc).__name__},
            ) from exc
        raw = _bounded_read_regular_file(self.ledger_path, self.limits.max_ledger_bytes)
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger is not valid UTF-8 JSON",
            ) from exc
        return _decode_ledger(decoded, self.limits)

    def _write_ledger(self, ledger: _LedgerState) -> None:
        encoded = _encode_ledger(ledger)
        if len(encoded) > self.limits.max_ledger_bytes:
            raise StoragePolicyError(
                StorageReason.LEDGER_OVERSIZED,
                "encoded storage reservation ledger exceeds its configured byte limit",
                details={"max_ledger_bytes": self.limits.max_ledger_bytes},
            )
        temporary: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".reservations.v1.", suffix=".tmp", dir=self.state_root
            )
            temporary = Path(temporary_name)
            try:
                os.chmod(temporary, 0o600)
                with os.fdopen(descriptor, "wb", closefd=True) as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                with suppress(OSError):
                    os.close(descriptor)
                raise
            _replace_file(temporary, self.ledger_path)
            temporary = None
            persisted = _bounded_read_regular_file(self.ledger_path, self.limits.max_ledger_bytes)
            if not hmac.compare_digest(persisted, encoded):
                raise StoragePolicyError(
                    StorageReason.PERSISTENCE_FAILURE,
                    "persisted storage reservation ledger differs from the committed bytes",
                )
            _fsync_directory(self.state_root)
        except StoragePolicyError:
            raise
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.PERSISTENCE_FAILURE,
                "storage reservation ledger could not be persisted atomically",
                details={"error": type(exc).__name__},
            ) from exc
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _snapshot(
        self, reservations: tuple[ReservationRecord, ...], ledger_generation: int
    ) -> StorageStatus:
        core, spool = self._stable_tree_snapshot()
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

    def _stable_tree_snapshot(self) -> tuple[TreeUsage, TreeUsage]:
        """Retry only transient tree changes while preserving bounded fail-closed scans."""
        for attempt in range(STORAGE_SNAPSHOT_SCAN_ATTEMPTS):
            try:
                core = scan_tree(
                    self.core_root,
                    max_entries=self.limits.max_scan_entries,
                    max_depth=self.limits.max_scan_depth,
                    max_accounted_bytes=self.limits.max_scan_accounted_bytes,
                )
                spool = scan_tree(
                    self.spool_root,
                    max_entries=self.limits.max_scan_entries,
                    max_depth=self.limits.max_scan_depth,
                    max_accounted_bytes=self.limits.max_scan_accounted_bytes,
                    link_policy="count",
                )
                return core, spool
            except StoragePolicyError as exc:
                final_attempt = attempt + 1 == STORAGE_SNAPSHOT_SCAN_ATTEMPTS
                if exc.reason is not StorageReason.SCAN_CHANGED or final_attempt:
                    raise
                time.sleep(STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS)
        raise AssertionError("bounded storage snapshot loop did not return or raise")

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
                free = int(shutil.disk_usage(members[0][1]).free)
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


def _error_decision(error: StoragePolicyError) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=error.reason,
        message=str(error),
        details=error.details,
    )


def _validate_job_id(job_id: str) -> None:
    if not _is_valid_job_id(job_id):
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "job_id must be 1-128 safe identifier characters",
        )


def _is_valid_job_id(value: object) -> bool:
    return isinstance(value, str) and _JOB_ID_PATTERN.fullmatch(value) is not None


def _is_non_boolean_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_reservation_bytes(core_bytes: int, spool_bytes: int, limits: StorageLimits) -> None:
    for name, value in (("core_bytes", core_bytes), ("spool_bytes", spool_bytes)):
        if type(value) is not int or value < 0:
            raise StoragePolicyError(
                StorageReason.INVALID_REQUEST,
                f"{name} must be a non-negative integer",
            )
    total = core_bytes + spool_bytes
    if total == 0:
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "a storage reservation must request at least one byte",
        )
    if total > limits.max_job_reservation_bytes:
        raise StoragePolicyError(
            StorageReason.PER_JOB_LIMIT,
            "job storage reservation exceeds the configured per-job limit",
            details={
                "requested_bytes": total,
                "max_job_reservation_bytes": limits.max_job_reservation_bytes,
            },
        )


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "storage policy clock must return a timezone-aware datetime",
        )
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at must be an RFC 3339 UTC timestamp",
        )
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at is invalid",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at must use UTC",
        )
    return value


def _encode_ledger(ledger: _LedgerState) -> bytes:
    payload: dict[str, object] = {
        "schema": _LEDGER_SCHEMA,
        "generation": ledger.generation,
        "reservations": [record.to_dict() for record in ledger.reservations],
    }
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    envelope = {**payload, "checksum": f"sha256:{digest}"}
    return json.dumps(envelope, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def _decode_ledger(value: object, limits: StorageLimits) -> _LedgerState:
    if not isinstance(value, dict):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an invalid envelope",
        )
    envelope = cast(dict[object, object], value)
    if set(envelope) != {
        "schema",
        "generation",
        "reservations",
        "checksum",
    }:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an invalid envelope",
        )
    if envelope["schema"] != _LEDGER_SCHEMA:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an unsupported schema",
        )
    generation = envelope["generation"]
    if type(generation) is not int or generation < 0:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger generation is invalid",
        )
    raw_reservations_value = envelope["reservations"]
    if not isinstance(raw_reservations_value, list):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger records must be a list",
        )
    raw_reservations = cast(list[object], raw_reservations_value)
    if len(raw_reservations) > limits.max_reservations:
        raise StoragePolicyError(
            StorageReason.LEDGER_CAPACITY,
            "storage reservation ledger exceeds its configured record limit",
            details={"max_reservations": limits.max_reservations},
        )
    payload = {
        "schema": envelope["schema"],
        "generation": generation,
        "reservations": raw_reservations,
    }
    checksum = envelope["checksum"]
    expected = "sha256:" + hashlib.sha256(_canonical_json(payload)).hexdigest()
    if not isinstance(checksum, str) or not hmac.compare_digest(checksum, expected):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger checksum does not match its content",
        )
    records: list[ReservationRecord] = []
    seen: set[str] = set()
    for raw_record in raw_reservations:
        if not isinstance(raw_record, dict):
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid record",
            )
        record_data = cast(dict[object, object], raw_record)
        if set(record_data) != {
            "job_id",
            "core_bytes",
            "spool_bytes",
            "created_at",
        }:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid record",
            )
        job_id = record_data["job_id"]
        try:
            _validate_job_id(cast(str, job_id))
        except StoragePolicyError as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid job id",
            ) from exc
        if cast(str, job_id) in seen:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains duplicate job ids",
            )
        core_bytes = record_data["core_bytes"]
        spool_bytes = record_data["spool_bytes"]
        if type(core_bytes) is not int or core_bytes < 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation core_bytes is invalid",
            )
        if type(spool_bytes) is not int or spool_bytes < 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation spool_bytes is invalid",
            )
        if core_bytes + spool_bytes == 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation cannot be empty",
            )
        if core_bytes + spool_bytes > limits.max_job_reservation_bytes:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "stored reservation exceeds the configured per-job limit",
            )
        created_at = _parse_timestamp(record_data["created_at"])
        record = ReservationRecord(
            job_id=cast(str, job_id),
            core_bytes=core_bytes,
            spool_bytes=spool_bytes,
            created_at=created_at,
        )
        records.append(record)
        seen.add(record.job_id)
    if [record.job_id for record in records] != sorted(record.job_id for record in records):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger records are not in canonical order",
        )
    return _LedgerState(generation=generation, reservations=tuple(records))


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _bounded_read_regular_file(path: Path, max_bytes: int) -> bytes:
    before = _validate_private_regular_file(path, allow_empty=False)
    if before.st_size > max_bytes:
        raise StoragePolicyError(
            StorageReason.LEDGER_OVERSIZED,
            "storage reservation ledger exceeds its configured byte limit",
            details={"max_ledger_bytes": max_bytes},
        )
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage reservation ledger could not be opened safely",
            details={"error": type(exc).__name__},
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or int(opened.st_dev) != int(before.st_dev)
            or int(opened.st_ino) != int(before.st_ino)
        ):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage reservation ledger identity changed while opening",
            )
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            int(after.st_dev) != int(opened.st_dev)
            or int(after.st_ino) != int(opened.st_ino)
            or int(after.st_size) != len(raw)
        ):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage reservation ledger changed while reading",
            )
        if len(raw) > max_bytes:
            raise StoragePolicyError(
                StorageReason.LEDGER_OVERSIZED,
                "storage reservation ledger exceeds its configured byte limit",
                details={"max_ledger_bytes": max_bytes},
            )
        return raw
    finally:
        os.close(descriptor)


def _validate_private_regular_file(path: Path, *, allow_empty: bool) -> os.stat_result:
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file could not be inspected",
            details={"error": type(exc).__name__},
        ) from exc
    if _is_link_or_reparse(result) or not stat.S_ISREG(result.st_mode) or result.st_nlink != 1:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file is not a private regular file",
        )
    if os.name != "nt" and (result.st_uid != os.geteuid() or stat.S_IMODE(result.st_mode) & 0o077):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file is not owner-private",
        )
    if not allow_empty and result.st_size == 0:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger is empty",
        )
    return result


def _prepare_lock_file(path: Path) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        _validate_private_regular_file(path, allow_empty=True)
        return
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage reservation lock file could not be created safely",
            details={"error": type(exc).__name__},
        ) from exc
    os.close(descriptor)
    with suppress(OSError):
        # Windows ACLs do not implement POSIX mode bits; file identity remains checked.
        os.chmod(path, 0o600)
    _validate_private_regular_file(path, allow_empty=True)


def _safe_lstat(path: Path, *, root: bool) -> os.stat_result:
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID if root else StorageReason.SCAN_CHANGED,
            "storage path could not be inspected",
            details={"root": str(path), "error": type(exc).__name__},
        ) from exc
    if _is_link_or_reparse(result):
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID if root else StorageReason.SCAN_UNSAFE_ENTRY,
            "storage path is a link or reparse point",
            details={"root": str(path)},
        )
    return result


@contextmanager
def _scandir_verified(
    path: Path, expected_device: int, expected_inode: int
) -> Generator[Iterator[os.DirEntry[str]], None, None]:
    before = _safe_lstat(path, root=False)
    if (
        not stat.S_ISDIR(before.st_mode)
        or int(before.st_dev) != expected_device
        or int(before.st_ino) != expected_inode
    ):
        raise StoragePolicyError(
            StorageReason.SCAN_CHANGED,
            "storage directory identity changed during accounting",
        )
    # On POSIX, opening the directory with O_NOFOLLOW binds the scan to the
    # verified directory object. Windows DirEntry metadata exposes reparse flags;
    # a post-scan identity check catches replacement races there.
    if os.name != "nt" and hasattr(os, "O_DIRECTORY"):
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.SCAN_CHANGED,
                "storage directory could not be opened without following links",
            ) from exc
        try:
            opened = os.fstat(descriptor)
            if (
                int(opened.st_dev) != expected_device
                or int(opened.st_ino) != expected_inode
                or not stat.S_ISDIR(opened.st_mode)
            ):
                raise StoragePolicyError(
                    StorageReason.SCAN_CHANGED,
                    "storage directory identity changed while opening",
                )
            with os.scandir(descriptor) as iterator:
                yield cast(Iterator[os.DirEntry[str]], iterator)
        finally:
            os.close(descriptor)
        return
    with os.scandir(path) as iterator:
        yield iterator
    after = _safe_lstat(path, root=False)
    if int(after.st_dev) != expected_device or int(after.st_ino) != expected_inode:
        raise StoragePolicyError(
            StorageReason.SCAN_CHANGED,
            "storage directory identity changed during accounting",
        )


def _is_link_or_reparse(result: os.stat_result) -> bool:
    attributes = int(getattr(result, "st_file_attributes", 0) or 0)
    return stat.S_ISLNK(result.st_mode) or bool(attributes & _FILE_ATTRIBUTE_REPARSE_POINT)


def _ensure_directory_no_links(path: Path) -> None:
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    if not cursor.exists():
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root has no existing filesystem ancestor",
        )
    existing = _safe_lstat(cursor, root=True)
    if not stat.S_ISDIR(existing.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root ancestor is not a directory",
        )
    _validate_state_parent_security(cursor)
    for component in reversed(missing):
        with suppress(FileExistsError):
            component.mkdir(mode=0o700)
        result = _safe_lstat(component, root=True)
        if not stat.S_ISDIR(result.st_mode):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state path is not a directory",
            )
        if os.name != "nt":
            try:
                os.chmod(component, 0o700)
            except OSError as exc:
                raise StoragePolicyError(
                    StorageReason.LEDGER_UNSAFE,
                    "storage policy state directory could not be made owner-private",
                ) from exc
    final = _safe_lstat(path, root=True)
    if not stat.S_ISDIR(final.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root is not a directory",
        )
    if os.name != "nt":
        try:
            os.chmod(path, 0o700)
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state root could not be made owner-private",
            ) from exc
        final = _safe_lstat(path, root=True)
        if final.st_uid != os.geteuid() or stat.S_IMODE(final.st_mode) != 0o700:
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state root is not owner-private",
            )
        _validate_state_parent_security(path.parent)


def _validate_state_parent_security(path: Path) -> None:
    if os.name == "nt":
        return
    result = _safe_lstat(path, root=True)
    if not stat.S_ISDIR(result.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state parent is not a directory",
        )
    mode = stat.S_IMODE(result.st_mode)
    writable_by_others = bool(mode & 0o022)
    sticky = bool(mode & stat.S_ISVTX)
    if writable_by_others and not sticky:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state parent permits unprotected replacement",
        )


def _reject_overlapping_roots(first: Path, second: Path) -> None:
    first_key = os.path.normcase(os.path.abspath(first))
    second_key = os.path.normcase(os.path.abspath(second))
    try:
        common = os.path.commonpath((first_key, second_key))
    except ValueError:
        return
    if common in {first_key, second_key}:
        raise ValueError("core_root and spool_root must be distinct, non-nested trees")


def _volume_id(path: Path, result: os.stat_result) -> str:
    if os.name == "nt":
        drive = os.path.splitdrive(str(path))[0].casefold()
        return f"volume:{drive}:{int(result.st_dev)}"
    return f"device:{int(result.st_dev)}"


def _require_positive_bound(name: str, value: int) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _replace_file(source: Path, destination: Path) -> None:
    attempts = 8 if os.name == "nt" else 1
    for attempt in range(attempts):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(min(0.01 * (2**attempt), 0.1))


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
