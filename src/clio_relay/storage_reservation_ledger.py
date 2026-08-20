"""Reservation-ledger CRUD mixin for ``StoragePolicy``.

Owns the durable per-job reservation lifecycle: locking the ledger, reading
and decoding it, and the five public admission/mutation operations
(``reserve``/``release``/``verify_reservation``/``reconcile``/
``reconcile_reservations``) that build the proposed ledger state, check it
against current storage pressure, and hand off to ``StoragePolicy.
_write_ledger`` to persist it. ``_write_ledger`` itself stays on
``StoragePolicy`` in ``storage_policy.py`` (its call to the directly
monkeypatched ``_replace_file`` is a test seam -- see that module's
docstring); every method here reaches it through ``self._write_ledger(...)``,
which resolves through the composed class regardless of which file defines
it, so the seam is preserved without this mixin importing the facade back.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Generator, Iterable, Mapping
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.storage_file_io import (
    _bounded_read_regular_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _prepare_lock_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _validate_private_regular_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.storage_ledger_codec import (
    _decode_ledger,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _format_timestamp,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _validate_job_id,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _validate_reservation_bytes,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.storage_policy_types import (
    ReservationRecord,
    StorageDecision,
    StorageLimits,
    StoragePolicyError,
    StorageReason,
    StorageStatus,
    StorageTreeSnapshot,
    _error_decision,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _LedgerState,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)


class StorageReservationLedgerMixin:
    """Own the durable reservation ledger's lock, read path, and CRUD surface."""

    ledger_path: Path
    lock_path: Path
    limits: StorageLimits
    _clock: Callable[[], datetime]

    if TYPE_CHECKING:

        def _write_ledger(self, ledger: _LedgerState) -> None: ...

        def capture_admission_snapshot(self) -> StorageTreeSnapshot: ...

        def _snapshot(
            self,
            reservations: tuple[ReservationRecord, ...],
            ledger_generation: int,
            *,
            tree_snapshot: StorageTreeSnapshot | None = None,
        ) -> StorageStatus: ...

    @contextmanager
    def _ledger_lock(self) -> Generator[None, None, None]:
        _prepare_lock_file(self.lock_path)
        lock = FileLock(
            str(internal_filesystem_path(self.lock_path, force_extended=True)),
            timeout=float(self.limits.lock_timeout_seconds),
        )
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
            os.lstat(internal_filesystem_path(self.ledger_path, force_extended=True))
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

    def reserve(
        self,
        job_id: str,
        *,
        core_bytes: int,
        spool_bytes: int,
        tree_snapshot: StorageTreeSnapshot | None = None,
    ) -> StorageDecision:
        """Atomically reserve expected core and spool growth for one job.

        Repeating the same request is idempotent. Reusing a job id with different
        amounts is rejected instead of silently resizing a live reservation.
        """
        try:
            _validate_job_id(job_id)
            _validate_reservation_bytes(core_bytes, spool_bytes, self.limits)
            resolved_tree_snapshot = tree_snapshot or self.capture_admission_snapshot()
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
                    snapshot = self._snapshot(
                        ledger.reservations,
                        ledger.generation,
                        tree_snapshot=resolved_tree_snapshot,
                    )
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
                snapshot = self._snapshot(
                    proposed,
                    ledger.generation + 1,
                    tree_snapshot=resolved_tree_snapshot,
                )
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
