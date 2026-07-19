"""Production queue admission and running-job storage coordination.

The filesystem policy is intentionally queue-agnostic.  This module supplies the
relay-specific pieces: deterministic per-job sizing, bounded startup adoption from
the authoritative active-job index, and a cheap running-child guard.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from clio_relay.core_queue import (
    DEFAULT_CORE_LOCK_TIMEOUT_SECONDS,
    MAX_ACTIVE_JOB_RECORDS,
    MAX_LIVE_LEASE_RECORDS,
    ClioCoreQueue,
    _job_matches_mcp_admission_class,  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.models import (
    TERMINAL_STATES,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    RelayJob,
    StorageReservationEstimate,
)
from clio_relay.storage_policy import (
    StorageDecision,
    StorageLimits,
    StoragePolicy,
    StoragePolicyError,
    StorageReason,
)
from clio_relay.worker_concurrency import KindConcurrencyInput, normalize_kind_concurrency
from clio_relay.worker_lifetime_lock import (
    LockedCoreIdentity,
    WorkerLifetimeLock,
    exclusive_migration_lifetime,
)

if TYPE_CHECKING:
    from clio_relay.config import RelaySettings

STORAGE_RUNTIME_STATUS_SCHEMA = "clio-relay.storage-runtime-status.v1"
_MANAGED_UNSET = object()
_MIGRATION_BATCH_SIZE = 10_000
_MIGRATION_FAMILY_BOUND = 20
_MIGRATION_FIXED_BATCHES = 32


class StorageRuntimeError(RelayError):
    """Base class for a stable machine-readable storage runtime failure."""

    def __init__(self, decision: StorageDecision) -> None:
        self.decision = decision
        super().__init__(json.dumps(decision.to_dict(), sort_keys=True, separators=(",", ":")))


class StorageAdmissionError(StorageRuntimeError):
    """Raised when a genuinely new queue admission cannot be reserved safely."""


class StorageRuntimeViolation(StorageRuntimeError):
    """Raised after a running child crosses a durable storage safety boundary."""


class _ActiveJobSource(Protocol):
    def scan_active_jobs(self, *, limit: int) -> tuple[list[RelayJob], bool]: ...


@dataclass(frozen=True, slots=True)
class StorageRuntimeConfig:
    """All settings needed to build one target-agnostic storage runtime."""

    core_root: Path
    spool_root: Path
    max_log_bytes_per_job: int
    job_core_allowance_bytes: int
    job_result_allowance_bytes: int
    runtime_check_interval_seconds: float
    limits: StorageLimits

    def __post_init__(self) -> None:
        object.__setattr__(self, "core_root", logical_filesystem_path(self.core_root))
        object.__setattr__(self, "spool_root", logical_filesystem_path(self.spool_root))
        for name in (
            "max_log_bytes_per_job",
            "job_core_allowance_bytes",
            "job_result_allowance_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if isinstance(self.runtime_check_interval_seconds, bool):
            raise ValueError("runtime_check_interval_seconds must be a positive number")
        if not 0 < float(self.runtime_check_interval_seconds) <= 300:
            raise ValueError(
                "runtime_check_interval_seconds must be greater than zero and at most 300"
            )
        if self.default_core_bytes + self.default_spool_bytes > (
            self.limits.max_job_reservation_bytes
        ):
            raise ValueError(
                "default log and allowance reservation exceeds max_job_reservation_bytes"
            )
        if self.default_core_bytes > self.limits.core_high_water_bytes:
            raise ValueError("default core reservation exceeds core_high_water_bytes")
        if self.default_spool_bytes > self.limits.spool_high_water_bytes:
            raise ValueError("default spool reservation exceeds spool_high_water_bytes")
        if self.default_core_bytes + self.default_spool_bytes > (
            self.limits.total_high_water_bytes
        ):
            raise ValueError("default storage reservation exceeds total_high_water_bytes")

    @property
    def default_core_bytes(self) -> int:
        """Reserve captured-output duplication plus bounded core record overhead."""
        return self.max_log_bytes_per_job + self.job_core_allowance_bytes

    @property
    def default_spool_bytes(self) -> int:
        """Reserve captured output plus bounded result, sidecar, and package output."""
        return self.max_log_bytes_per_job + self.job_result_allowance_bytes


class StorageRuntime:
    """Own durable admission readiness and running-child storage checks."""

    def __init__(
        self,
        config: StorageRuntimeConfig,
        *,
        policy: StoragePolicy | None = None,
    ) -> None:
        self.config = config
        internal_filesystem_path(config.core_root, force_extended=True).mkdir(
            parents=True,
            exist_ok=True,
        )
        internal_filesystem_path(config.spool_root, force_extended=True).mkdir(
            parents=True,
            exist_ok=True,
        )
        self.policy = policy or StoragePolicy(
            config.core_root,
            config.spool_root,
            limits=config.limits,
        )
        self.startup_reconciliation: StorageDecision | None = None
        self._last_job_check: dict[str, float] = {}
        self._runtime_check_lock = threading.Lock()

    def estimate(self, job: RelayJob) -> StorageReservationEstimate:
        """Resolve and validate the durable reservation for one submitted job."""
        estimate = job.storage_reservation or StorageReservationEstimate(
            core_bytes=self.config.default_core_bytes,
            spool_bytes=self.config.default_spool_bytes,
        )
        if estimate.core_bytes < self.config.default_core_bytes:
            raise StorageAdmissionError(
                _denied_decision(
                    StorageReason.INVALID_REQUEST,
                    "core estimate is below the configured log and record floor",
                    details={
                        "requested_core_bytes": estimate.core_bytes,
                        "minimum_core_bytes": self.config.default_core_bytes,
                    },
                )
            )
        if estimate.spool_bytes < self.config.default_spool_bytes:
            raise StorageAdmissionError(
                _denied_decision(
                    StorageReason.INVALID_REQUEST,
                    "spool estimate is below the configured log and result floor",
                    details={
                        "requested_spool_bytes": estimate.spool_bytes,
                        "minimum_spool_bytes": self.config.default_spool_bytes,
                    },
                )
            )
        if estimate.core_bytes + estimate.spool_bytes > (
            self.config.limits.max_job_reservation_bytes
        ):
            raise StorageAdmissionError(
                _denied_decision(
                    StorageReason.PER_JOB_LIMIT,
                    "job estimate exceeds the configured per-job reservation limit",
                    details={
                        "requested_total_bytes": estimate.core_bytes + estimate.spool_bytes,
                        "max_job_reservation_bytes": (self.config.limits.max_job_reservation_bytes),
                    },
                )
            )
        return estimate

    def reconcile_startup(self, queue: _ActiveJobSource) -> StorageDecision:
        """Adopt authoritative nonterminal jobs before opening new intake."""
        limit = self.config.limits.max_reservations + 1
        active_jobs, truncated = queue.scan_active_jobs(limit=limit)
        if truncated or len(active_jobs) > self.config.limits.max_reservations:
            decision = _denied_decision(
                StorageReason.LEDGER_CAPACITY,
                "active job index exceeds the configured reservation capacity",
                details={
                    "max_reservations": self.config.limits.max_reservations,
                    "observed_at_least": len(active_jobs),
                },
            )
            self.startup_reconciliation = decision
            return decision
        reservations: dict[str, object] = {}
        try:
            for job in active_jobs:
                estimate = self.estimate(job)
                reservations[job.job_id] = (estimate.core_bytes, estimate.spool_bytes)
        except StorageAdmissionError as exc:
            self.startup_reconciliation = exc.decision
            return exc.decision
        decision = self.policy.reconcile_reservations(reservations)
        self.startup_reconciliation = decision
        return decision

    def ensure_new_intake_allowed(self) -> None:
        """Fail closed when startup reconciliation did not establish safe intake."""
        decision = self.startup_reconciliation
        if decision is None:
            raise StorageAdmissionError(
                _denied_decision(
                    StorageReason.INVALID_REQUEST,
                    "storage startup reconciliation has not completed",
                )
            )
        if not decision.allowed:
            raise StorageAdmissionError(decision)

    def block_new_intake(self, decision: StorageDecision) -> None:
        """Persist an in-process fail-closed state after an accounting failure."""
        if decision.allowed:
            raise ValueError("intake can only be blocked with a denied storage decision")
        self.startup_reconciliation = decision

    def status(self) -> dict[str, object]:
        """Return bounded machine-readable startup and current policy status."""
        current = self.policy.status()
        startup = self.startup_reconciliation
        intake_allowed = bool(startup is not None and startup.allowed and current.allowed)
        reason = (
            startup.reason.value
            if startup is not None and not startup.allowed
            else current.reason.value
        )
        return {
            "schema": STORAGE_RUNTIME_STATUS_SCHEMA,
            "intake_allowed": intake_allowed,
            "reason": reason,
            "startup_reconciliation": (None if startup is None else startup.to_dict()),
            "current": current.to_dict(),
            "reservation_defaults": {
                "core_bytes": self.config.default_core_bytes,
                "spool_bytes": self.config.default_spool_bytes,
                "max_log_bytes_per_job": self.config.max_log_bytes_per_job,
                "job_core_allowance_bytes": self.config.job_core_allowance_bytes,
                "job_result_allowance_bytes": self.config.job_result_allowance_bytes,
            },
            "runtime_check_interval_seconds": float(self.config.runtime_check_interval_seconds),
        }

    def check_running_job(
        self,
        job_id: str,
        *,
        spool_path: Path,
        now: float | None = None,
        force_job_scan: bool = False,
    ) -> StorageDecision:
        """Check free bytes every poll and one owned job tree at a fixed interval."""
        free_space = self.policy.check_runtime_free_space()
        if not free_space.allowed:
            return free_space
        observed_at = time.monotonic() if now is None else now
        with self._runtime_check_lock:
            last_checked = self._last_job_check.get(job_id)
            due = (
                force_job_scan
                or last_checked is None
                or observed_at - last_checked >= float(self.config.runtime_check_interval_seconds)
            )
            if due:
                self._last_job_check[job_id] = observed_at
        if not due:
            return free_space
        return self.policy.check_runtime_job(job_id, spool_path=spool_path)

    def forget_running_job(self, job_id: str) -> None:
        """Discard in-memory guard timing after a child reaches a terminal path."""
        with self._runtime_check_lock:
            self._last_job_check.pop(job_id, None)


def storage_runtime_from_settings(settings: RelaySettings) -> StorageRuntime:
    """Build a production storage runtime from validated relay settings."""
    return StorageRuntime(
        StorageRuntimeConfig(
            core_root=settings.core_dir,
            spool_root=settings.spool_dir,
            max_log_bytes_per_job=settings.spool_max_log_bytes_per_job,
            job_core_allowance_bytes=settings.storage_job_core_allowance_bytes,
            job_result_allowance_bytes=settings.storage_job_result_allowance_bytes,
            runtime_check_interval_seconds=settings.storage_runtime_check_interval_seconds,
            limits=settings.storage_limits(),
        )
    )


class StorageManagedQueue(ClioCoreQueue):
    """Clio-core facade with durable reserve-before-admit and terminal release."""

    def __init__(
        self,
        root: Path,
        *,
        storage_runtime: StorageRuntime,
        writer_lifetime_lock: WorkerLifetimeLock | None = None,
        owns_writer_lifetime_lock: bool = False,
        lock_timeout_seconds: float = DEFAULT_CORE_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        self._closed = False
        if Path(root).absolute() != storage_runtime.config.core_root.absolute():
            raise ValueError("managed queue root must match the storage runtime core root")
        if owns_writer_lifetime_lock and writer_lifetime_lock is None:
            raise ValueError("an owned writer lifetime lock must be provided")
        if writer_lifetime_lock is not None:
            if not writer_lifetime_lock.acquired or writer_lifetime_lock.mode != "shared":
                raise ValueError("managed queue writer lifetime lock must hold shared ownership")
            root_stat = os.stat(root)
            lock_stat = os.stat(writer_lifetime_lock.core_dir)
            if (root_stat.st_dev, root_stat.st_ino) != (lock_stat.st_dev, lock_stat.st_ino):
                raise ValueError("managed queue root must match its writer lifetime lock")
        super().__init__(root, lock_timeout_seconds=lock_timeout_seconds)
        self.storage_runtime = storage_runtime
        self._writer_lifetime_lock = writer_lifetime_lock
        self._owns_writer_lifetime_lock = owns_writer_lifetime_lock

    def __getattribute__(self, name: str) -> object:
        """Reject every public queue operation after lifetime ownership ends."""
        value = super().__getattribute__(name)
        if name == "close" or name.startswith("_") or not callable(value):
            return value
        try:
            closed = super().__getattribute__("_closed")
        except AttributeError:
            return value
        if closed:
            raise ConfigurationError("managed queue is closed and cannot perform operations")
        return value

    @property
    def closed(self) -> bool:
        """Return whether this queue's writer lifetime has ended."""
        return self._closed

    def initialize(
        self,
        *,
        migrate_legacy_output: bool = False,
        locked_core: LockedCoreIdentity | None = None,
    ) -> None:
        """Initialize only while this managed queue retains writer ownership."""
        if self._closed:
            raise ConfigurationError("managed queue is closed and cannot perform operations")
        super().initialize(
            migrate_legacy_output=migrate_legacy_output,
            locked_core=locked_core,
        )

    def close(self) -> None:
        """Release queue-owned core writer lifetime ownership."""
        self._closed = True
        if not self._owns_writer_lifetime_lock:
            return
        self._owns_writer_lifetime_lock = False
        lifetime_lock = self._writer_lifetime_lock
        self._writer_lifetime_lock = None
        if lifetime_lock is not None:
            lifetime_lock.release()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    @contextmanager
    def _acquire_lock_with_replay(self) -> Generator[None]:
        """Replay under the core lock and release terminal storage after unlocking."""
        replayed: list[RelayJob] = []
        try:
            with self._lock:  # pyright: ignore[reportPrivateUsage]
                replayed = self._recover_pending_transitions_unlocked()  # pyright: ignore[reportPrivateUsage]
                yield
        finally:
            for job in replayed:
                if job.state in TERMINAL_STATES:
                    self._release_reservation(job.job_id, terminal_job=job)

    def _lease_admission_snapshot_unlocked(
        self,
        endpoint_id: str,
        *,
        cluster: str,
    ) -> tuple[Lease | None, dict[JobKind, int], int]:
        refs, truncated = self._scan_expiry_refs(  # pyright: ignore[reportPrivateUsage]
            limit=MAX_LIVE_LEASE_RECORDS
        )
        if truncated:
            raise QueueConflictError("lease expiry index exceeded its safety bound")
        active = self._active_lease_for_endpoint(  # pyright: ignore[reportPrivateUsage]
            endpoint_id,
            expiry_refs=refs,
        )
        counts, global_total = self._lease_capacity_snapshot(  # pyright: ignore[reportPrivateUsage]
            cluster=cluster,
            expiry_refs=refs,
        )
        return active, counts, global_total

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Reserve storage before a genuinely new idempotent queue admission."""
        estimate = self.storage_runtime.estimate(job)
        try:
            with self.storage_runtime.policy.admission_lock():
                resolution = self.resolve_idempotent_submission(job)
                canonical = job.model_copy(update={"job_id": resolution.canonical_job_id})
                if resolution.state in {"existing", "retired"}:
                    saved = super().submit_job(canonical)
                    return self._verify_existing_reservation(saved)
                if resolution.state not in {"new", "reserved"}:
                    raise StorageAdmissionError(
                        _denied_decision(
                            StorageReason.INVALID_REQUEST,
                            "queue returned an unsupported idempotency resolution state",
                            details={"state": resolution.state},
                        )
                    )
                self.storage_runtime.ensure_new_intake_allowed()
                decision = self.storage_runtime.policy.reserve(
                    canonical.job_id,
                    core_bytes=estimate.core_bytes,
                    spool_bytes=estimate.spool_bytes,
                )
                if not decision.allowed:
                    raise StorageAdmissionError(decision)
                try:
                    saved = super().submit_job(canonical)
                except BaseException:
                    self._release_failed_admission(canonical.job_id)
                    raise
                if saved.job_id != canonical.job_id:
                    self._release_reservation(canonical.job_id, terminal_job=None)
                    return self._verify_existing_reservation(saved)
                if saved.state in TERMINAL_STATES:
                    self._release_reservation(saved.job_id, terminal_job=saved)
                return saved
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def update_job_state(
        self,
        job_id: str,
        state: JobState,
        *,
        message: str | None = None,
        error: str | None = None,
        leased_by: str | None | object = _MANAGED_UNSET,
    ) -> RelayJob:
        """Release a reservation immediately after a terminal state commits."""
        if leased_by is _MANAGED_UNSET:
            saved = super().update_job_state(
                job_id,
                state,
                message=message,
                error=error,
            )
        else:
            saved = super().update_job_state(
                job_id,
                state,
                message=message,
                error=error,
                leased_by=leased_by,
            )
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved

    def cancel_job_if_active(
        self,
        job_id: str,
        *,
        cancel_scheduler: bool,
        expected_state: JobState | None = None,
        expected_updated_at: datetime | None = None,
    ) -> tuple[RelayJob, bool]:
        """Release storage after an atomic queued-job cancellation terminalizes."""
        saved, changed = super().cancel_job_if_active(
            job_id,
            cancel_scheduler=cancel_scheduler,
            expected_state=expected_state,
            expected_updated_at=expected_updated_at,
        )
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved, changed

    def acknowledge_job_cancellation(self, job_id: str) -> RelayJob:
        """Release storage after cancellation cleanup reaches its terminal commit."""
        saved = super().acknowledge_job_cancellation(job_id)
        if saved.state in TERMINAL_STATES:
            self._release_reservation(saved.job_id, terminal_job=saved)
        return saved

    def recover_stale_jobs(self, *, cluster: str, max_attempts: int = 3) -> list[RelayJob]:
        """Release reservations for jobs failed by bounded stale-lease recovery."""
        recovered = super().recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        for job in recovered:
            if job.state in TERMINAL_STATES:
                self._release_reservation(job.job_id, terminal_job=job)
        return recovered

    def recover_stale_job(
        self,
        job_id: str,
        *,
        cluster: str,
        max_attempts: int = 3,
    ) -> RelayJob | None:
        """Release storage when exact stale-lease recovery terminalizes a job."""
        recovered = super().recover_stale_job(
            job_id,
            cluster=cluster,
            max_attempts=max_attempts,
        )
        if recovered is not None and recovered.state in TERMINAL_STATES:
            self._release_reservation(recovered.job_id, terminal_job=recovered)
        return recovered

    def acquire_next_job(
        self,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
        mcp_admission_class: McpAdmissionClass | None = None,
        mcp_admission_limit: int | None = None,
    ) -> Lease | None:
        """Recover stale work, then lease atomically from one strict worker lane."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        if mcp_admission_class is not None and not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            mcp_admission_class,
            McpAdmissionClass,
        ):
            raise ConfigurationError("worker MCP admission class is invalid")
        if mcp_admission_limit is not None:
            if mcp_admission_class is None:
                raise ConfigurationError("worker MCP admission limit requires an admission class")
            if (
                isinstance(mcp_admission_limit, bool)
                or not isinstance(mcp_admission_limit, int)  # pyright: ignore[reportUnnecessaryIsInstance]
                or mcp_admission_limit < 1
            ):
                raise ConfigurationError("worker MCP admission limit must be at least 1")
        self.recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        self.initialize()
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=cluster,
            )
            if active is not None:
                active_job = self.get_job(active.job_id)
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    active_job,
                    mcp_admission_class,
                ):
                    raise QueueConflictError(
                        "endpoint active lease does not match its MCP admission lane: "
                        f"{endpoint_id}"
                    )
                return active
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            mcp_admission_at_limit = False
            active_mcp_workload_count: int | None = None
            if mcp_admission_class is not None and mcp_admission_limit is not None:
                mcp_admission_at_limit = (
                    self._active_mcp_admission_count_unlocked(
                        cluster=cluster,
                        admission_class=mcp_admission_class,
                        expiry_refs=None,
                    )
                    >= mcp_admission_limit
                )
            queued_jobs, truncated = self._scan_many(  # pyright: ignore[reportPrivateUsage]
                self._storage_root / "jobs_queued",  # pyright: ignore[reportPrivateUsage]
                RelayJob,
                limit=MAX_ACTIVE_JOB_RECORDS,
            )
            if truncated:
                raise QueueConflictError("queued job index exceeded its safety bound")
            for queued in sorted(
                queued_jobs,
                key=self._job_submission_order_key_unlocked,  # pyright: ignore[reportPrivateUsage]
            ):
                if queued.cluster != cluster or queued.state is not JobState.QUEUED:
                    continue
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    queued,
                    mcp_admission_class,
                ):
                    continue
                if mcp_admission_at_limit and queued.kind is JobKind.MCP_CALL:
                    continue
                if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                    queued.cluster,
                    queued.job_id,
                ):
                    continue
                kind_limit = normalized.get(queued.kind)
                active_kind_count = active_counts.get(queued.kind, 0)
                if queued.kind is JobKind.MCP_CALL and mcp_admission_class is not None:
                    if mcp_admission_class is McpAdmissionClass.CONTROL_QUERY:
                        kind_limit = None
                    else:
                        if active_mcp_workload_count is None:
                            active_mcp_workload_count = self._active_mcp_admission_count_unlocked(
                                cluster=cluster,
                                admission_class=McpAdmissionClass.WORKLOAD,
                                expiry_refs=None,
                            )
                        active_kind_count = active_mcp_workload_count
                if kind_limit is not None and active_kind_count >= kind_limit:
                    continue
                return self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                    queued,
                    endpoint_id,
                    ttl_seconds=ttl_seconds,
                    validated_global_total=global_lease_total,
                )
        return None

    def acquire_job(
        self,
        job_id: str,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> Lease | None:
        """Recover first, then lease only the exact requested job."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        self.recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
        self.initialize()
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=cluster,
            )
            if active is not None:
                return active if active.job_id == job_id else None
            job = self.get_job(job_id)
            if job.cluster != cluster or job.state is not JobState.QUEUED:
                return None
            if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                job.cluster,
                job.job_id,
            ):
                return None
            kind_limit = normalized.get(job.kind)
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            if kind_limit is not None and active_counts.get(job.kind, 0) >= kind_limit:
                return None
            return self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                job,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )

    def submit_and_acquire_job(
        self,
        job: RelayJob,
        endpoint_id: str,
        *,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> tuple[RelayJob, Lease | None]:
        """Reserve outside the core lock, then attempt an exact controlled lease."""
        normalized = normalize_kind_concurrency(kind_concurrency)
        submitted = self.submit_job(job)
        self.recover_stale_jobs(cluster=submitted.cluster, max_attempts=max_attempts)
        with self._acquire_lock_with_replay():
            self._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]
            active, active_counts, global_lease_total = self._lease_admission_snapshot_unlocked(
                endpoint_id,
                cluster=submitted.cluster,
            )
            if active is not None:
                return submitted, active if active.job_id == submitted.job_id else None
            current = self.get_job(submitted.job_id)
            if current.state is not JobState.QUEUED:
                return current, None
            if self._job_has_pending_execution_cleanup_unlocked(  # pyright: ignore[reportPrivateUsage]
                current.cluster,
                current.job_id,
            ):
                return current, None
            kind_limit = normalized.get(current.kind)
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return current, None
            if kind_limit is not None and active_counts.get(current.kind, 0) >= kind_limit:
                return current, None
            lease = self._lease_job_unlocked(  # pyright: ignore[reportPrivateUsage]
                current,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )
            return self.get_job(current.job_id), lease

    def _verify_existing_reservation(self, job: RelayJob) -> RelayJob:
        if job.state in TERMINAL_STATES:
            self._release_reservation(job.job_id, terminal_job=job)
            return job
        estimate = self.storage_runtime.estimate(job)
        decision = self.storage_runtime.policy.verify_reservation(
            job.job_id,
            core_bytes=estimate.core_bytes,
            spool_bytes=estimate.spool_bytes,
        )
        if not decision.allowed:
            self.storage_runtime.block_new_intake(decision)
            raise StorageAdmissionError(decision)
        return job

    def _release_failed_admission(self, job_id: str) -> None:
        try:
            existing = self.get_job(job_id)
        except NotFoundError:
            self._release_reservation(job_id, terminal_job=None)
            return
        if existing.state in TERMINAL_STATES:
            self._release_reservation(job_id, terminal_job=existing)

    def _release_reservation(
        self,
        job_id: str,
        *,
        terminal_job: RelayJob | None,
    ) -> None:
        decision = self.storage_runtime.policy.release(job_id)
        if decision.allowed:
            return
        self.storage_runtime.block_new_intake(decision)
        if terminal_job is None:
            raise StorageAdmissionError(decision)
        try:
            super().append_event(
                terminal_job.job_id,
                "storage.reservation_release_failed",
                "Terminal job storage reservation could not be released",
                payload=decision.to_dict(),
            )
        except RelayError:
            return


def storage_managed_queue(
    settings: RelaySettings,
    *,
    migrate_legacy_output: bool = False,
    writer_lifetime_lock: WorkerLifetimeLock | None = None,
) -> StorageManagedQueue:
    """Create a production queue under shared writer or exclusive migration ownership."""
    if migrate_legacy_output:
        if writer_lifetime_lock is not None:
            raise ValueError("migration cannot reuse shared writer lifetime ownership")
        with exclusive_migration_lifetime(settings.core_dir) as locked_core:
            pinned_settings = settings.model_copy(update={"core_dir": locked_core.root})
            runtime = storage_runtime_from_settings(pinned_settings)
            queue = StorageManagedQueue(locked_core.root, storage_runtime=runtime)
            queue.initialize(
                migrate_legacy_output=True,
                locked_core=locked_core,
            )
            _complete_bounded_index_migration(queue, runtime)
            runtime.reconcile_startup(queue)
            queue.close()
            return queue
    owned_lifetime_lock: WorkerLifetimeLock | None = None
    lifetime_lock = writer_lifetime_lock
    if lifetime_lock is None:
        owned_lifetime_lock = WorkerLifetimeLock(settings.core_dir, mode="shared").acquire()
        lifetime_lock = owned_lifetime_lock
    if not lifetime_lock.acquired or lifetime_lock.mode != "shared":
        raise ValueError("production queue requires acquired shared writer lifetime ownership")
    pinned_settings = settings.model_copy(update={"core_dir": lifetime_lock.core_dir})
    try:
        # Audit and initialize before StorageRuntime or StoragePolicy can create
        # `.storage`. A normal startup that encounters legacy output remains a
        # read-only refusal with respect to storage accounting and spool state.
        ClioCoreQueue(lifetime_lock.core_dir).initialize()
        runtime = storage_runtime_from_settings(pinned_settings)
        queue = StorageManagedQueue(
            lifetime_lock.core_dir,
            storage_runtime=runtime,
            writer_lifetime_lock=lifetime_lock,
            owns_writer_lifetime_lock=owned_lifetime_lock is not None,
        )
        queue.initialize()
        _complete_bounded_index_migration(queue, runtime)
        runtime.reconcile_startup(queue)
    except BaseException:
        if owned_lifetime_lock is not None:
            owned_lifetime_lock.release()
        raise
    return queue


def _complete_bounded_index_migration(
    queue: ClioCoreQueue,
    runtime: StorageRuntime,
) -> None:
    max_family_batches = (
        runtime.config.limits.max_scan_entries + _MIGRATION_BATCH_SIZE - 1
    ) // _MIGRATION_BATCH_SIZE
    max_batches = _MIGRATION_FIXED_BATCHES + _MIGRATION_FAMILY_BOUND * max_family_batches
    status = queue.index_migration_status()
    for _batch in range(max_batches):
        if status.get("complete") is True:
            return
        status = queue.migrate_indexes_batch(batch_size=_MIGRATION_BATCH_SIZE)
    if status.get("complete") is True:
        return
    raise StorageRuntimeError(
        _denied_decision(
            StorageReason.SCAN_ENTRY_LIMIT,
            "queue index migration exceeded its bounded startup work limit",
            details={
                "batch_size": _MIGRATION_BATCH_SIZE,
                "max_batches": max_batches,
                "max_scan_entries_per_family": runtime.config.limits.max_scan_entries,
            },
        )
    )


def _denied_decision(
    reason: StorageReason,
    message: str,
    *,
    details: dict[str, object] | None = None,
) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=reason,
        message=message,
        details=details,
    )


def _policy_error_decision(error: StoragePolicyError) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=error.reason,
        message=str(error),
        details=error.details,
    )
