"""Shared constants and bound validation for the queue diagnosis surface.

These are the small, universally-referenced building blocks every
queue-management entry point (listing, diagnosis, stale recovery, worker
status) is built on: the cancellation-policy type, the "job is still doing
something" state set, the default/maximum limit knobs, and the three guard
clauses that validate caller-supplied bounds and cluster scoping before any
CTE read happens.
"""

from __future__ import annotations

from typing import Literal

from clio_relay.errors import ConfigurationError
from clio_relay.models import JobState, RelayJob

QueueCancelPolicy = Literal["relay-only", "request-scheduler"]


ACTIVE_STATES = {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
DEFAULT_STALE_AFTER_SECONDS = 2 * 60 * 60
DEFAULT_RESULT_LIMIT = 100
DEFAULT_SCAN_LIMIT = 1_000
MAX_RESULT_LIMIT = 500
MAX_SCAN_LIMIT = 10_000
DEFAULT_STALE_SCAN_LIMIT = MAX_SCAN_LIMIT
DEFAULT_WORKER_FRESH_SECONDS = 60


def _require_job_cluster(  # pyright: ignore[reportUnusedFunction]
    job: RelayJob, cluster: str | None
) -> None:
    if cluster is not None and job.cluster != cluster:
        raise ConfigurationError(
            f"job {job.job_id} belongs to cluster {job.cluster}, not requested cluster {cluster}"
        )


def _validate_stale_after(value: int) -> None:  # pyright: ignore[reportUnusedFunction]
    if value < 1:
        raise ValueError("stale age threshold must be at least 1 second")


def _validate_bounds(  # pyright: ignore[reportUnusedFunction]
    *, limit: int, scan_limit: int
) -> None:
    if limit < 1 or limit > MAX_RESULT_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_RESULT_LIMIT}")
    if scan_limit < 1 or scan_limit > MAX_SCAN_LIMIT:
        raise ValueError(f"scan_limit must be between 1 and {MAX_SCAN_LIMIT}")
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
