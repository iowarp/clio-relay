"""Durable owner-session client-liveness lease storage (iowarp/clio-relay#277).

Owns the ``owner_sessions/<label>.lease.json`` sibling record --
create/renew (``touch_owner_session_lease``, the HTTP-traffic chokepoint),
read (``owner_session_lease_status``), terminal close
(``close_owner_session_lease``, used by both a clean client-driven teardown
and the worker's expiry sweep -- with two DIFFERENT reasons, never
conflated), bounded-retry quarantine for a sweep that cannot safely proceed
(``record_owner_session_lease_sweep_failure``), the bounded scan the sweep
uses to find leases gone quiet past their own TTL
(``due_expired_owner_session_leases``), and best-effort housekeeping of
terminal records (``prune_terminal_owner_session_leases``).

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
from datetime import UTC, datetime, timedelta
from pathlib import Path

from clio_relay import queue_context, queue_layout, queue_store_write
from clio_relay.errors import QueueConflictError
from clio_relay.models_owner_session_lease import (
    MAX_OWNER_SESSION_LEASE_RUNNING_JOB_IDS,
    MAX_OWNER_SESSION_LEASE_SWEEP_ERROR_CHARS,
    MAX_OWNER_SESSION_SWEEP_ATTEMPTS,
    OwnerSessionLease,
)

#: Bounds one sweep cycle's worth of due-lease reads. A worker cycle runs
#: every ``poll_seconds`` (default 2s) forever, so an unbounded backlog
#: drains within a handful of cycles without risking one giant scan.
MAX_OWNER_SESSION_LEASE_SWEEP_BATCH = 256

#: Bounds the directory scan itself (defense in depth against a directory
#: that somehow accumulated far more sibling files than real owner sessions
#: ever could -- mirrors the spirit of ``queue_layout.MAX_ACTIVE_JOB_RECORDS``).
MAX_OWNER_SESSION_LEASE_RECORDS = 4_096

#: Bounds one pruning pass (MEDIUM 7): housekeeping is cheap and runs every
#: sweep cycle, so a small per-cycle cap drains a backlog over a handful of
#: cycles rather than doing unbounded work in one call.
MAX_OWNER_SESSION_LEASE_PRUNE_BATCH = 256

#: A quarter of the TTL is generous headroom against the worker's ~2s poll
#: cadence while still keeping ``last_seen_at`` fresh well before a lease
#: could ever be swept -- MEDIUM 5: renewal is an fsync'd write under the
#: global queue lock; skip it when it would not meaningfully change due-ness.
OWNER_SESSION_LEASE_RENEWAL_DEBOUNCE_FRACTION = 4

_VALID_CLOSE_REASONS = ("client_close", "lease_expired")


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
        request (attach, polls, submissions) and from the SSE/WebSocket
        poll loops on every tick
        (``http_api_owner_session_lease_renewal.renew_owner_session_lease``),
        never from a new client obligation. Four outcomes, all idempotent
        and safe under a concurrent sweep:

        * No record yet, or the record names a DIFFERENT (necessarily
          superseded) generation -- open a fresh ``open`` lease for this
          exact generation. Reachable only at genuine generation-start
          traffic, mirroring how ``.active.json`` itself moves across
          generations.
        * Record matches this generation, is still ``open``, and was
          renewed less than a quarter-TTL ago -- debounced: return the
          existing record unchanged, no write (MEDIUM 5).
        * Record matches this generation and is still ``open`` (past the
          debounce window) -- advance ``last_seen_at``.
        * Record matches this generation but is already terminal (``closed``,
          ``expired``, or ``quarantined``) -- a benign race with a
          concurrent close/sweep; left untouched (a terminal lease never
          reopens) and returned as-is. The caller (the HTTP request in
          flight) is never rejected because of this race -- see the module
          docstring.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        if not cluster:
            raise ValueError("cluster must not be empty")
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be 0 (expiry disabled) or positive")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        path = self._owner_session_lease_path(owner_session_id)
        with self._lock:
            existing = self._read_owner_session_lease_unlocked(path)
            if existing is not None and existing.session_generation_id == session_generation_id:
                if existing.status != "open":
                    return existing
                if existing.ttl_seconds == 0:
                    # Disabled lease (the default): never due, so last_seen_at
                    # freshness is meaningless -- skip the fsync'd rewrite
                    # every authenticated request would otherwise pay.
                    return existing
                debounce_window = timedelta(
                    seconds=existing.ttl_seconds / OWNER_SESSION_LEASE_RENEWAL_DEBOUNCE_FRACTION
                )
                if current_time - existing.last_seen_at < debounce_window:
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
        running_job_ids_truncated: bool = False,
        expected_last_seen_at: datetime | None = None,
        teardown_failed: bool = False,
        teardown_error: str | None = None,
        now: datetime | None = None,
    ) -> OwnerSessionLease | None:
        """Terminally close one lease with an exact, typed, non-relabelable reason.

        ``reason`` is ``"client_close"`` for an explicit, successful
        ``session teardown`` and ``"lease_expired"`` for the worker's TTL
        sweep -- the two DISTINCT typed paths the acceptance bar requires.
        (The THIRD terminal reason, ``"expiry_quarantined"``, is reached only
        through :meth:`record_owner_session_lease_sweep_failure` -- never
        through this method, so a caller cannot quarantine a lease by
        accident.) Idempotent for a retry with the SAME reason (returns the
        existing terminal record unchanged); raises
        :class:`QueueConflictError` for a retry that disagrees with an
        already-recorded reason (a real bug at the call site, never silently
        relabeled). Returns ``None`` when no lease record exists at all --
        see :meth:`owner_session_lease_status`.

        ``expected_last_seen_at`` is an optional compare-and-swap guard
        (BLOCKER 3): the sweep reads a lease's ``last_seen_at`` during its
        due-scan, then does slow work (quiesce, teardown) before calling
        this. A renewal landing in that window must not be silently
        overwritten by a close that is now stale. When given and it no
        longer matches the CURRENT record's ``last_seen_at``, this is a
        typed no-op -- the CURRENT (still ``open``) record is returned
        UNCHANGED rather than closed; a caller distinguishes "closed" from
        "CAS lost, renewed concurrently" by checking ``result.status``.
        """
        if reason not in _VALID_CLOSE_REASONS:
            raise ValueError(f"invalid owner session lease close reason: {reason!r}")
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        job_ids = sorted(set(running_job_ids))
        if len(job_ids) > MAX_OWNER_SESSION_LEASE_RUNNING_JOB_IDS:
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
            if expected_last_seen_at is not None and existing.last_seen_at != expected_last_seen_at:
                # BLOCKER 3: renewed concurrently since the caller's due-scan
                # read -- typed no-op, never overwrite a live renewal.
                return existing
            closed = existing.model_copy(
                update={
                    "status": target_status,
                    "closed_at": current_time,
                    "close_reason": reason,
                    "expired_with_running_jobs": reason == "lease_expired" and bool(job_ids),
                    "running_job_ids_at_close": job_ids,
                    "running_job_ids_truncated": running_job_ids_truncated,
                    # Review residual 1: the record must never claim a clean
                    # reap when the teardown call failed under it.
                    "teardown_failed": teardown_failed,
                    "teardown_error": (None if teardown_error is None else teardown_error[:512]),
                }
            )
            queue_store_write.write_model(self._storage_root, path, closed)
            return closed

    def record_owner_session_lease_sweep_failure(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        reason: str,
        max_attempts: int = MAX_OWNER_SESSION_SWEEP_ATTEMPTS,
        now: datetime | None = None,
    ) -> OwnerSessionLease | None:
        """Record one failed sweep attempt; quarantine after ``max_attempts``.

        BLOCKER 2: a lease whose recorded cleanup intent the sweep cannot
        safely honor (or any other per-attempt failure) must never retry
        forever with a fresh traceback every worker cycle. Each call
        increments ``sweep_failure_count`` and records the typed
        ``last_sweep_error``; once the count reaches ``max_attempts`` the
        lease transitions to the terminal ``quarantined`` status
        (``close_reason="expiry_quarantined"``) and the due-scan stops
        selecting it (:meth:`~models_owner_session_lease.OwnerSessionLease.
        is_due` is ``False`` for any non-``open`` status).

        Idempotent-safe: a lease already terminal is returned unchanged --
        this is failure bookkeeping on a still-open lease, never a
        resurrection of a closed/expired/quarantined one. Returns ``None``
        when no lease record exists.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        path = self._owner_session_lease_path(owner_session_id)
        bounded_reason = reason[:MAX_OWNER_SESSION_LEASE_SWEEP_ERROR_CHARS]
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
                    f"owner session lease generation does not match failure record: "
                    f"{owner_session_id}"
                )
            if existing.status != "open":
                return existing
            attempts = existing.sweep_failure_count + 1
            if attempts >= max_attempts:
                updated = existing.model_copy(
                    update={
                        "status": "quarantined",
                        "closed_at": current_time,
                        "close_reason": "expiry_quarantined",
                        "sweep_failure_count": attempts,
                        "last_sweep_error": bounded_reason,
                    }
                )
            else:
                updated = existing.model_copy(
                    update={
                        "sweep_failure_count": attempts,
                        "last_sweep_error": bounded_reason,
                    }
                )
            queue_store_write.write_model(self._storage_root, path, updated)
            return updated

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
        Raises :class:`QueueConflictError` if the ``owner_sessions/``
        directory holds more than :data:`MAX_OWNER_SESSION_LEASE_RECORDS`
        lease files -- a real safety bound, not a bug; the CALLER (the
        sweep) is responsible for containing that (MEDIUM 7) rather than
        this method silently truncating a scan it cannot prove is complete.
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

    def prune_terminal_owner_session_leases(
        self,
        *,
        cluster: str,
        older_than_seconds: int,
        now: datetime | None = None,
        limit: int = MAX_OWNER_SESSION_LEASE_PRUNE_BATCH,
    ) -> int:
        """Delete terminal lease records closed longer ago than the retention window.

        MEDIUM 7: nothing else ever removes a ``.lease.json`` file, so their
        count grows without bound as sessions come and go -- eventually
        tripping :meth:`due_expired_owner_session_leases`'s own safety bound
        and, unless the caller degrades gracefully, taking the worker's
        ``while True`` loop down with it. Mirrors
        ``queue_owner_session_records.py``'s unlink discipline for the SAME
        ``owner_sessions/`` family: bounded, best-effort housekeeping that
        never touches the ``.active``/``.closing``/``.closed`` siblings it
        does not own, and never removes an ``open`` lease (only
        ``closed``/``expired``/``quarantined`` ones, and only once past the
        retention window, so an operator has time to observe
        ``expired_with_running_jobs`` before the record disappears).
        Returns the number of records removed.
        """
        if not cluster:
            raise ValueError("cluster must not be empty")
        if older_than_seconds < 0:
            raise ValueError("older_than_seconds must not be negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._store_adapter.initialize()
        current_time = now or datetime.now(UTC)
        directory = self._storage_root / "owner_sessions"
        paths = self._bounded_owner_session_lease_paths(directory)
        pruned = 0
        with self._lock:
            for path in paths:
                if pruned >= limit:
                    break
                lease = self._read_owner_session_lease_unlocked(path)
                if lease is None or lease.cluster != cluster or lease.status == "open":
                    continue
                if lease.closed_at is None:
                    continue
                age_seconds = (current_time - lease.closed_at).total_seconds()
                if age_seconds < older_than_seconds:
                    continue
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                pruned += 1
        return pruned

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
