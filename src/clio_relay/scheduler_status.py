"""Scheduler queue helpers.

Scheduler-specific polling and cancellation live in scheduler providers. This
module keeps relay-level queue status and a compatibility poll helper.
"""

from __future__ import annotations

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JobState, RelayJob, SchedulerStatus
from clio_relay.scheduler_providers import provider_for_scheduler


def relay_queue_status(queue: ClioCoreQueue, job: RelayJob) -> dict[str, object]:
    """Return relay-level queue position for a job."""
    if job.state != JobState.QUEUED:
        return {"state": job.state.value, "jobs_ahead": None, "position": None}
    jobs_ahead = 0
    for candidate in queue.list_jobs():
        if candidate.job_id == job.job_id:
            break
        if candidate.cluster == job.cluster and candidate.state == JobState.QUEUED:
            jobs_ahead += 1
    return {"state": job.state.value, "jobs_ahead": jobs_ahead, "position": jobs_ahead + 1}


def poll_slurm_status(scheduler_job_id: str) -> SchedulerStatus:
    """Compatibility wrapper for older callers that explicitly request SLURM."""
    return provider_for_scheduler("slurm").poll(scheduler_job_id)
