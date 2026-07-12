"""Scheduler queue helpers.

Scheduler-specific polling and cancellation live in scheduler providers. This
module keeps relay-level queue status and a compatibility poll helper.
"""

from __future__ import annotations

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobState, RelayJob, SchedulerStatus
from clio_relay.scheduler_providers import provider_for_scheduler

MAX_QUEUE_POSITION_RECORDS = 10_000


def relay_queue_status(queue: ClioCoreQueue, job: RelayJob) -> dict[str, object]:
    """Return relay-level queue position for a job."""
    if job.state != JobState.QUEUED:
        return {"state": job.state.value, "jobs_ahead": None, "position": None}
    candidates, truncated = queue.scan_active_jobs(limit=MAX_QUEUE_POSITION_RECORDS)
    if truncated:
        raise QueueConflictError(
            "relay queue position exceeds the bounded active-job scan; "
            "run queue retention or increase indexed queue-position support"
        )
    jobs_ahead = 0
    found = False
    for candidate in candidates:
        if candidate.job_id == job.job_id:
            found = True
            break
        if candidate.cluster == job.cluster and candidate.state == JobState.QUEUED:
            jobs_ahead += 1
    if not found:
        raise QueueConflictError(
            f"queued job is absent from the bounded active-job index: {job.job_id}"
        )
    return {"state": job.state.value, "jobs_ahead": jobs_ahead, "position": jobs_ahead + 1}


def poll_slurm_status(scheduler_job_id: str) -> SchedulerStatus:
    """Compatibility wrapper for older callers that explicitly request SLURM."""
    return provider_for_scheduler("slurm").poll(scheduler_job_id)
