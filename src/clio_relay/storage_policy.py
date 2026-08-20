"""Durable storage admission and accounting for relay queue data.

The policy deliberately has no knowledge of clusters, schedulers, or workloads.  It
accounts the two operator-configured relay storage trees, reserves expected growth
per job, and rejects admission when a bounded safety check cannot be completed.

The ledger checksum detects corruption and torn/manual edits. It is not a MAC,
signature, or authenticity proof; filesystem ownership and private permissions are
the trust boundary for local policy state.

This module is now a facade (iowarp/clio-relay split/storage-policy-w2): the
data contracts live in ``storage_policy_types.py``, the durable ledger's
content codec lives in ``storage_ledger_codec.py``, the filesystem-identity
and durable-I/O primitives built on it live in ``storage_file_io.py``, and
``StoragePolicy``'s reservation-CRUD and status/health surfaces live in the
``StorageReservationLedgerMixin``/``StorageSnapshotMixin`` mixins
(``storage_reservation_ledger.py``/``storage_snapshot.py``) it composes below.
Every name the rest of the repository imports from ``clio_relay.storage_policy``
is re-exported here unchanged, so no other file's imports needed to move.

Typed deviation -- what stays resident here and why: ``scan_tree``,
``_scandir_verified``, and ``_replace_file`` are each individually
monkeypatched by name in the test suite via
``monkeypatch.setattr(storage_module, "<name>", ...)`` (``storage_module``
being this module, imported as ``clio_relay.storage_policy``). Python resolves
an unqualified name inside a function body through the *module that defines
the function*, not through whatever module re-exports it -- so moving
``scan_tree`` to an owner module while its caller stayed here (or vice versa)
would silently stop the patch from taking effect. The same applies to every
caller that reaches one of these three by bare name:
``StoragePolicy._stable_tree_snapshot``/``StoragePolicy.check_runtime_job``
(both call ``scan_tree``), ``scan_tree`` itself (calls ``_scandir_verified``),
and ``StoragePolicy._write_ledger`` (calls ``_replace_file``). All six stay
here as one connected cluster; everything else -- including every other
``StoragePolicy`` method -- calls back into this cluster through ``self.<name>``,
which resolves through the composed class's MRO regardless of which file
defines it, so it carries no such constraint and extracts freely.
"""

from __future__ import annotations

import hmac
import os

# Re-export only: storage_module.shutil is a monkeypatch seam (see module docstring).
import shutil as shutil
import stat
import tempfile
import time
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from clio_relay.filesystem_paths import (
    internal_filesystem_path,
    logical_filesystem_path,
)
from clio_relay.storage_file_io import (
    _bounded_read_regular_file,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _ensure_directory_no_links,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _fsync_directory,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _is_link_or_reparse,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _reject_overlapping_roots,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _require_positive_bound,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _safe_lstat,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.storage_ledger_codec import (
    _encode_ledger,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    _validate_job_id,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)

# --- re-exported public surface (verbatim; see module docstring) ---
# Every public name is also listed in __all__ below, which is how ruff's
# unused-import check recognizes these as intentional re-exports rather than
# dead imports.
from clio_relay.storage_policy_types import (
    DEFAULT_JOB_CORE_ALLOWANCE_BYTES,
    DEFAULT_JOB_RESULT_ALLOWANCE_BYTES,
    DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS,
    STORAGE_SNAPSHOT_SCAN_ATTEMPTS,
    STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS,
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
    _LedgerState,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.storage_reservation_ledger import StorageReservationLedgerMixin
from clio_relay.storage_snapshot import StorageSnapshotMixin

__all__ = [
    "DEFAULT_JOB_CORE_ALLOWANCE_BYTES",
    "DEFAULT_JOB_RESULT_ALLOWANCE_BYTES",
    "DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS",
    "STORAGE_SNAPSHOT_SCAN_ATTEMPTS",
    "STORAGE_SNAPSHOT_SCAN_RETRY_SECONDS",
    "ReservationRecord",
    "StorageDecision",
    "StorageLimits",
    "StoragePolicy",
    "StoragePolicyError",
    "StorageReason",
    "StorageStatus",
    "StorageTreeSnapshot",
    "TreeUsage",
    "VolumeStatus",
    "scan_tree",
]


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
    logical_root = logical_filesystem_path(Path(os.path.abspath(root)))
    normalized = internal_filesystem_path(logical_root, force_extended=True)
    root_stat = _safe_lstat(normalized, root=True)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID,
            "storage root is not a directory",
            details={"root": str(logical_root)},
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
                            details={"root": str(logical_root), "max_entries": max_entries},
                        )
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError as exc:
                        raise StoragePolicyError(
                            StorageReason.SCAN_CHANGED,
                            "storage tree changed while it was being accounted",
                            details={"root": str(logical_root)},
                        ) from exc
                    except OSError as exc:
                        raise StoragePolicyError(
                            StorageReason.SCAN_IO_ERROR,
                            "storage entry could not be inspected",
                            details={"root": str(logical_root), "error": type(exc).__name__},
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
                                details={"root": str(logical_root)},
                            ) from exc
                    if _is_link_or_reparse(entry_stat) or entry.is_symlink():
                        if link_policy == "reject":
                            raise StoragePolicyError(
                                StorageReason.SCAN_UNSAFE_ENTRY,
                                "storage tree contains a link or reparse point",
                                details={"root": str(logical_root)},
                            )
                        link_count += 1
                        total_bytes += max(0, int(entry_stat.st_size))
                        if total_bytes > max_accounted_bytes:
                            raise StoragePolicyError(
                                StorageReason.SCAN_BYTE_LIMIT,
                                "storage tree exceeds the configured accounting byte limit",
                                details={
                                    "root": str(logical_root),
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
                                details={"root": str(logical_root), "max_depth": max_depth},
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
                                details={"root": str(logical_root)},
                            )
                        file_count += 1
                        total_bytes += int(entry_stat.st_size)
                        if total_bytes > max_accounted_bytes:
                            raise StoragePolicyError(
                                StorageReason.SCAN_BYTE_LIMIT,
                                "storage tree exceeds the configured accounting byte limit",
                                details={
                                    "root": str(logical_root),
                                    "max_accounted_bytes": max_accounted_bytes,
                                },
                            )
                    else:
                        raise StoragePolicyError(
                            StorageReason.SCAN_UNSAFE_ENTRY,
                            "storage tree contains a non-regular entry",
                            details={"root": str(logical_root)},
                        )
        except StoragePolicyError:
            raise
        except FileNotFoundError as exc:
            raise StoragePolicyError(
                StorageReason.SCAN_CHANGED,
                "storage tree changed while it was being accounted",
                details={"root": str(logical_root)},
            ) from exc
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.SCAN_IO_ERROR,
                "storage directory could not be scanned",
                details={"root": str(logical_root), "error": type(exc).__name__},
            ) from exc

    return TreeUsage(
        root=str(logical_root),
        bytes=total_bytes,
        files=file_count,
        links=link_count,
        directories=directory_count,
        entries=entry_count,
    )


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


def _replace_file(source: Path, destination: Path) -> None:
    source = internal_filesystem_path(source, force_extended=True)
    destination = internal_filesystem_path(destination, force_extended=True)
    attempts = 8 if os.name == "nt" else 1
    for attempt in range(attempts):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(min(0.01 * (2**attempt), 0.1))


class StoragePolicy(StorageReservationLedgerMixin, StorageSnapshotMixin):
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
        self.core_root = Path(os.path.abspath(logical_filesystem_path(core_root)))
        self.spool_root = Path(os.path.abspath(logical_filesystem_path(spool_root)))
        self.state_root = Path(
            os.path.abspath(
                logical_filesystem_path(
                    state_root if state_root is not None else self.core_root / ".storage"
                )
            )
        )
        self.limits = limits or StorageLimits()
        self._clock = clock or (lambda: datetime.now(UTC))
        _reject_overlapping_roots(self.core_root, self.spool_root)
        _ensure_directory_no_links(self.state_root)
        self.ledger_path = self.state_root / "reservations.v1.json"
        self.lock_path = self.state_root / "reservations.v1.lock"
        self.admission_lock_path = self.state_root / "admission.v1.lock"

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
                prefix=".reservations.v1.",
                suffix=".tmp",
                dir=internal_filesystem_path(self.state_root, force_extended=True),
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
