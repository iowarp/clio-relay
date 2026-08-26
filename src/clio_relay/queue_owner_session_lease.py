"""Durable owner-session client-liveness lease storage (iowarp/clio-relay#277).

Owns the ``owner_sessions/<label>.lease.json`` sibling record --
create/renew (``touch_owner_session_lease``, the HTTP-traffic chokepoint),
read (``owner_session_lease_status``), terminal close
(``close_owner_session_lease``, used by both a clean client-driven teardown
and the worker's expiry sweep -- with two DIFFERENT reasons, never
conflated), and the bounded scan the sweep uses to find leases gone quiet
past their own TTL (``due_expired_owner_session_leases``).

Sibling to, and deliberately independent of,
``queue_owner_session_records.QueueOwnerSessionRecordsMixin``'s
active/closing/closed generation-admission state machine: the lease is a
liveness SIGNAL about a generation, not a second copy of whether that
generation is admitting work. Both live in the same ``owner_sessions/``
record family under the same per-owner-session label key.
"""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime
from pathlib import Path

from clio_relay import queue_context, queue_layout, queue_store_write
from clio_relay.errors import QueueConflictError
from clio_relay.models_owner_session_lease import OwnerSessionLease

#: Bounds one sweep cycle's worth of due-lease reads. A worker cycle runs
#: every ``poll_seconds`` (default 2s) forever, so an unbounded backlog
#: drains within a handful of cycles without risking one giant scan.
MAX_OWNER_SESSION_LEASE_SWEEP_BATCH = 256

#: Bounds the directory scan itself (defense in depth against a directory
#: that somehow accumulated far more sibling files than real owner sessions
#: ever could -- mirrors the spirit of ``queue_layout.MAX_ACTIVE_JOB_RECORDS``).
MAX_OWNER_SESSION_LEASE_RECORDS = 4_096


class QueueOwnerSessionLeaseMixin:
    """Own the owned-session client-liveness lease record."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    def _owner_session_lease_path(self, owner_session_id: str) -> Path:
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        return self._storage_root / "owner_sessions" / f"{session_label}.lease.json"

    def touch_owner_session_lease(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        cluster: str,
        ttl_seconds: int,
        now: datetime | None = None,
    ) -> OwnerSessionLease:
        """Create-or-renew the lease for one owned-session generation.

        This is the ONE renewal chokepoint: called from the owned-session
        API's shared authentication dependency on every authenticated
        request (attach, polls, submissions), never from a new client
        obligation. Three outcomes, all idempotent and safe under a
        concurrent sweep:

        * No record yet, or the record names a DIFFERENT (necessarily
          superseded) generation -- open a fresh ``open`` lease for this
          exact generation. Reachable only at genuine generation-start
          traffic, mirroring how ``.active.json`` itself moves across
          generations.
        * Record matches this generation and is still ``open`` -- advance
          ``last_seen_at``.
        * Record matches this generation but is already terminal (``closed``
          or ``expired``) -- a benign race with a concurrent close/sweep;
          left untouched (a terminal lease never reopens) and returned as-is.
          The caller (the HTTP request in flight) is never rejected because
          of this race -- see the module docstring.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        if not cluster:
            raise ValueError("cluster must not be empty")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        path = self._owner_session_lease_path(owner_session_id)
        with self._lock:
            existing = self._read_owner_session_lease_unlocked(path)
            if existing is not None and existing.session_generation_id == session_generation_id:
                if existing.status != "open":
                    return existing
                renewed = existing.model_copy(update={"last_seen_at": current_time})
                queue_store_write.write_model(self._storage_root, path, renewed)
                return renewed
            opened = OwnerSessionLease(
                owner_session_id=owner_session_id,
                session_generation_id=session_generation_id,
                cluster=cluster,
                ttl_seconds=ttl_seconds,
                status="open",
                opened_at=current_time,
                last_seen_at=current_time,
            )
            queue_store_write.write_model(self._storage_root, path, opened)
            return opened

    def owner_session_lease_status(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None = None,
    ) -> OwnerSessionLease | None:
        """Return the durable lease record, or ``None`` when none exists yet.

        A session started before this feature existed, or one whose API
        process never received a single authenticated request, legitimately
        has no lease record -- that is not an error.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if session_generation_id is not None:
            session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
                session_generation_id,
                field="session_generation_id",
            )
        self._store_adapter.initialize()
        path = self._owner_session_lease_path(owner_session_id)
        with self._lock:
            lease = self._read_owner_session_lease_unlocked(path)
        if lease is None:
            return None
        if lease.owner_session_id != owner_session_id:
            raise QueueConflictError(f"owner session lease identity mismatch: {owner_session_id}")
        if (
            session_generation_id is not None
            and lease.session_generation_id != session_generation_id
        ):
            return None
        return lease

    def close_owner_session_lease(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        reason: str,
        running_job_ids: list[str] | tuple[str, ...] = (),
        now: datetime | None = None,
    ) -> OwnerSessionLease | None:
        """Terminally close one lease with an exact, typed, non-relabelable reason.

        ``reason`` is ``"client_close"`` for an explicit, successful
        ``session teardown`` and ``"lease_expired"`` for the worker's TTL
        sweep -- the two DISTINCT typed paths the acceptance bar requires.
        Idempotent for a retry with the SAME reason (returns the existing
        terminal record unchanged); raises :class:`QueueConflictError` for a
        retry that disagrees with an already-recorded reason (a real bug at
        the call site, never silently relabeled). Returns ``None`` when no
        lease record exists at all -- see :meth:`owner_session_lease_status`.
        """
        if reason not in ("client_close", "lease_expired"):
            raise ValueError(f"invalid owner session lease close reason: {reason!r}")
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        job_ids = sorted(set(running_job_ids))
        if len(job_ids) > 1_000:
            raise ValueError("running_job_ids exceeds its supported capacity")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        path = self._owner_session_lease_path(owner_session_id)
        target_status = "closed" if reason == "client_close" else "expired"
        with self._lock:
            existing = self._read_owner_session_lease_unlocked(path)
            if existing is None:
                return None
            if existing.owner_session_id != owner_session_id:
                raise QueueConflictError(
                    f"owner session lease identity mismatch: {owner_session_id}"
                )
            if existing.session_generation_id != session_generation_id:
                raise QueueConflictError(
                    f"owner session lease generation does not match closure: {owner_session_id}"
                )
            if existing.status != "open":
                if existing.status == target_status and existing.close_reason == reason:
                    return existing
                raise QueueConflictError(
                    f"owner session lease is already terminal with a different reason: "
                    f"{owner_session_id}"
                )
            closed = existing.model_copy(
                update={
                    "status": target_status,
                    "closed_at": current_time,
                    "close_reason": reason,
                    "expired_with_running_jobs": reason == "lease_expired" and bool(job_ids),
                    "running_job_ids_at_close": job_ids,
                }
            )
            queue_store_write.write_model(self._storage_root, path, closed)
            return closed

    def due_expired_owner_session_leases(
        self,
        *,
        cluster: str,
        now: datetime | None = None,
        limit: int = MAX_OWNER_SESSION_LEASE_SWEEP_BATCH,
    ) -> list[OwnerSessionLease]:
        """Return every open lease for ``cluster`` that has gone quiet past its TTL.

        Bounded, deterministic (sorted by ``owner_session_id``), and
        read-only -- the caller (the worker sweep) performs the actual
        expiry consequences and then calls :meth:`close_owner_session_lease`.
        """
        if not cluster:
            raise ValueError("cluster must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        directory = self._storage_root / "owner_sessions"
        paths = self._bounded_owner_session_lease_paths(directory)
        due: list[OwnerSessionLease] = []
        with self._lock:
            for path in paths:
                lease = self._read_owner_session_lease_unlocked(path)
                if lease is None:
                    continue
                if lease.cluster != cluster:
                    continue
                if not lease.is_due(now=current_time):
                    continue
                due.append(lease)
        due.sort(key=lambda lease: lease.owner_session_id)
        return due[:limit]

    def _bounded_owner_session_lease_paths(self, directory: Path) -> list[Path]:
        """Return bounded ``*.lease.json`` children only.

        ``owner_sessions/`` is a SHARED record family
        (``queue_owner_session_records.py`` also keeps
        ``.active.json``/``.closing.json``/``.closed.json`` siblings there,
        plus a per-owner-session ``.closures/`` SUBDIRECTORY for generation
        closure history) -- unlike a single-record-kind family, a raw
        ``queue_store_read.bounded_json_record_paths`` scan is the wrong
        primitive here: it fails closed on ANY non-``.json``-regular-file
        entry, including that legitimate subdirectory. This filters to the
        lease suffix first and silently skips everything else, while still
        refusing to follow a symlink/reparse point and still bounding the
        scan.
        """
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return []
        if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
            directory_stat
        ):
            raise QueueConflictError(
                f"owner-session lease sweep is not a safe directory: {directory}"
            )
        paths: list[Path] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.name.endswith(".lease.json"):
                    continue
                if len(paths) >= MAX_OWNER_SESSION_LEASE_RECORDS:
                    raise QueueConflictError(
                        "owner-session lease sweep exceeded its safety bound of "
                        f"{MAX_OWNER_SESSION_LEASE_RECORDS} records"
                    )
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(entry_stat.st_mode) or queue_layout.record_is_reparse(
                    entry_stat
                ):
                    continue
                paths.append(Path(entry.path))
        return paths

    def _read_owner_session_lease_unlocked(self, path: Path) -> OwnerSessionLease | None:
        return self._store_adapter.read_optional(path, OwnerSessionLease)
