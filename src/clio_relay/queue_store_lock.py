"""Locking discipline and fixed durable-family sets for the core queue store."""

from __future__ import annotations

import json
import os
import stat
import threading
import time
from pathlib import Path
from types import TracebackType

from filelock import FileLock, Timeout

from clio_relay import queue_layout
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.filesystem_paths import logical_filesystem_path
from clio_relay.models import SchedulerPhase


class UnsafeQueueDirectoryProtection(ConfigurationError):
    """Pinned repair found a linked canonical family before queue initialization."""

    def __init__(self, *, family: str, path: Path, cause: OSError) -> None:
        self.family = family
        self.path = path
        super().__init__(
            "queue directory protections cannot be repaired through the pinned root: "
            f"{path}: {cause}"
        )


class LegacyQueueStateError(QueueConflictError):
    """Machine-readable refusal for unsafe pre-1.0 canonical queue state."""

    def __init__(
        self,
        *,
        family: str,
        path: Path,
        reason: str,
        action: str | None = None,
    ) -> None:
        self.report: dict[str, str] = {
            "schema_version": "clio-relay.legacy-state-audit.v1",
            "family": family,
            "path": str(logical_filesystem_path(path)),
            "reason": reason,
            "action": action
            or (
                "move the unsafe state aside or export records with portable durable IDs "
                "before retrying"
            ),
        }
        super().__init__(json.dumps(self.report, sort_keys=True))


def require_legacy_family_directory(storage_root: Path, family: str) -> Path | None:
    """Require a legacy family root to be an owner-private real directory."""
    path = storage_root / family
    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as error:
        reason = f"cannot inspect canonical family: {type(error).__name__}"
        raise LegacyQueueStateError(family=family, path=path, reason=reason) from error
    if not stat.S_ISDIR(details.st_mode) or queue_layout.record_is_reparse(details):
        reason = "canonical family is not an owned directory"
        raise LegacyQueueStateError(family=family, path=path, reason=reason)
    if os.name != "nt":
        getuid = getattr(os, "getuid", None)
        foreign_owner = callable(getuid) and details.st_uid != getuid()
        if foreign_owner or stat.S_IMODE(details.st_mode) & 0o077:
            reason = "queue directory is not owned by the current user"
            if not foreign_owner:
                reason = "queue directory is readable or writable by another user"
            raise LegacyQueueStateError(family=family, path=path, reason=reason)
    return path


class QueueSealRequiresExclusive(ConfigurationError):
    """Refuse to create the indexed-era seal without exclusive writer fencing."""


ORDER_FAMILIES = ("tasks", "artifacts", "progress")
GLOBAL_ORDER_FAMILIES = (
    "endpoints",
    "jobs",
    "gateway_sessions",
    "monitor_rules",
)
RETENTION_INDEX_FAMILIES = (
    "jobs",
    "tasks",
    "artifacts",
    "monitor_rules",
    "gateway_sessions",
)
OPERATIONAL_INDEX_FAMILIES = (
    "endpoints",
    "jobs",
    "gateway_sessions",
    "leases",
)
INITIALIZED_QUEUE_FAMILIES = (
    "endpoints",
    "endpoints_fresh",
    "endpoints_fresh_by_id",
    "jobs",
    "tasks",
    "leases",
    "lease_indexes",
    "lease_identity_refs",
    "leases_by_endpoint",
    "leases_by_cluster_kind",
    "leases_by_expiry",
    "lease_capacity",
    "events",
    "legacy_output_archives",
    "legacy_output_receipts",
    "legacy_output_retired",
    "artifacts",
    "artifact_user_order",
    "artifact_users",
    "progress",
    "task_events",
    "gateway_sessions",
    "gateway_reverse_refs_by_session",
    "gateways_by_artifact",
    "gateways_by_scheduler",
    "active_gateway_refs_by_job",
    "active_gateway_refs_by_session",
    "idempotency",
    "monitor_rules",
    "monitor_rules_by_job",
    "active_monitor_rules_by_job",
    "owner_sessions",
    "owner_session_jobs",
    "owner_session_legacy_jobs",
    "job_indexes",
    "tasks_by_job",
    "leases_by_job",
    "artifacts_by_job",
    "used_artifacts_by_job",
    "progress_by_job",
    "jobs_active",
    "jobs_queued",
    "task_event_heads",
    "migrations",
    "task_order_by_job",
    "transition_intents",
    "artifact_order_by_job",
    "progress_order_by_job",
    "active_tasks_by_job",
    "scheduler_refs_by_job",
    "scheduler_protections_by_job",
    "scheduler_jobs",
    "scheduler_cancel_pending",
    "scheduler_cancel_dispositions",
    "job_tombstones",
    "gc_runs",
    "gc_trash",
    "global_order",
)
ADDITIVE_QUEUE_FAMILIES = ("transforms", "mcp_tasks")
LEGACY_ONLY_QUEUE_FAMILIES = ("cursors",)
GC_TERMINAL_SCHEDULER_PHASES = {
    SchedulerPhase.COMPLETED.value,
    SchedulerPhase.FAILED.value,
    SchedulerPhase.CANCELED.value,
}


class FairBoundedFileLock:
    """Serialize local waiters fairly before taking one cross-process file lock.

    ``filelock`` retries a busy Windows lock by polling. Several hot threads in
    one relay process can repeatedly acquire the short-lived filesystem lock in
    those polling gaps and starve an older local waiter until its bounded
    timeout expires. Ticket admission prevents local overtaking while the
    underlying ``FileLock`` preserves cross-process exclusion. Both waits
    share one deadline so lock failure remains explicit and bounded.
    """

    def __init__(self, lock_file: str, *, timeout: float) -> None:
        self.lock_file = lock_file
        self.timeout = timeout
        self._file_lock = FileLock(lock_file, timeout=timeout)
        self._condition = threading.Condition()
        self._owner_thread_id: int | None = None
        self._owner_depth = 0
        self._next_ticket = 0
        self._serving_ticket = 0
        self._abandoned_tickets: set[int] = set()

    def __enter__(self) -> FairBoundedFileLock:
        self.acquire()
        return self

    @property
    def is_locked(self) -> bool:
        """Report this thread's underlying ``FileLock`` ownership state."""
        return self._file_lock.is_locked

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()

    def acquire(self, *, timeout: float | None = None) -> None:
        """Acquire local admission and the filesystem lock within one deadline."""
        bounded_timeout = self.timeout if timeout is None else timeout
        if bounded_timeout < 0:
            raise ValueError("lock timeout must be non-negative")
        deadline = time.monotonic() + bounded_timeout
        thread_id = threading.get_ident()
        with self._condition:
            if self._owner_thread_id == thread_id:
                self._owner_depth += 1
                return
            ticket = self._next_ticket
            self._next_ticket += 1
            while self._owner_thread_id is not None or ticket != self._serving_ticket:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._abandoned_tickets.add(ticket)
                    self._skip_abandoned_tickets_locked()
                    self._condition.notify_all()
                    raise Timeout(self.lock_file)
                self._condition.wait(timeout=remaining)
            self._owner_thread_id = thread_id
            self._owner_depth = 1

        try:
            remaining = max(0.0, deadline - time.monotonic())
            self._file_lock.acquire(timeout=remaining)
        except BaseException:
            with self._condition:
                self._owner_thread_id = None
                self._owner_depth = 0
                self._serving_ticket += 1
                self._skip_abandoned_tickets_locked()
                self._condition.notify_all()
            raise

    def release(self) -> None:
        """Release one reentrant level and admit the next local ticket."""
        thread_id = threading.get_ident()
        with self._condition:
            if self._owner_thread_id != thread_id or self._owner_depth == 0:
                raise RuntimeError("core queue lock released by a non-owner thread")
            if self._owner_depth > 1:
                self._owner_depth -= 1
                return
            self._file_lock.release()
            self._owner_thread_id = None
            self._owner_depth = 0
            self._serving_ticket += 1
            self._skip_abandoned_tickets_locked()
            self._condition.notify_all()

    def _skip_abandoned_tickets_locked(self) -> None:
        while self._serving_ticket in self._abandoned_tickets:
            self._abandoned_tickets.remove(self._serving_ticket)
            self._serving_ticket += 1
