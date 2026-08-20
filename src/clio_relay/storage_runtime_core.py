"""Target-agnostic storage runtime: admission readiness and running-child checks.

Owns :class:`StorageRuntimeConfig` (validated sizing/interval settings),
:class:`StorageRuntime` itself (durable startup reconciliation, per-job
reservation estimation, and the bounded running-child free-space/tree guard),
and the production factory that builds one from validated relay settings.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.models import RelayJob, StorageReservationEstimate
from clio_relay.storage_policy import (
    StorageDecision,
    StorageLimits,
    StoragePolicy,
    StorageReason,
)
from clio_relay.storage_runtime_errors import StorageAdmissionError, _denied_decision

if TYPE_CHECKING:
    from clio_relay.config import RelaySettings

STORAGE_RUNTIME_STATUS_SCHEMA = "clio-relay.storage-runtime-status.v1"


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
