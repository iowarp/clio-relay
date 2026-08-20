"""Wire types, limits, and error vocabulary for the storage admission policy.

This module owns the data contracts ``storage_policy.py`` and its owner
modules share: the stable :class:`StorageReason` outcome vocabulary, the
fail-closed :class:`StoragePolicyError`, the configurable :class:`StorageLimits`,
and the frozen dataclasses that describe reservations, tree usage, volume
health, and the composed status/decision reports. None of these types touch
the filesystem or the reservation ledger themselves -- they are the shapes
every owner module (scan, ledger codec, file I/O, the reservation-ledger and
snapshot mixins) builds and consumes.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

_LEDGER_SCHEMA: Final = "clio-relay.storage-reservations.v1"
_STATUS_SCHEMA: Final = "clio-relay.storage-status.v1"
_DECISION_SCHEMA: Final = "clio-relay.storage-decision.v1"
_MIB: Final = 1024**2
_GIB: Final = 1024**3
_TIB: Final = 1024**4
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


def _is_non_boolean_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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
class StorageTreeSnapshot:
    """One stable bounded observation captured before admission serialization."""

    core: TreeUsage
    spool: TreeUsage


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
class _LedgerState:  # pyright: ignore[reportUnusedClass] -- used by the ledger codec/mixin/facade
    generation: int
    reservations: tuple[ReservationRecord, ...]


def _error_decision(  # pyright: ignore[reportUnusedFunction] -- called from the mixins/facade
    error: StoragePolicyError,
) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=error.reason,
        message=str(error),
        details=error.details,
    )
