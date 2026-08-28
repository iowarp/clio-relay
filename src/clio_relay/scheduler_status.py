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
    """Return relay-level queue position for a job.

    #290: the caller's ``job`` snapshot may already be stale by the time this
    runs (e.g. an observation loop that read it moments ago while a concurrent
    ``update_job_state`` call raced it out of the queued state and out of the
    active-job index). ``job`` is used only to identify which job to look up;
    the state actually reasoned about here (``current``) and the active-job
    scan are read together under one lock acquisition
    (:meth:`ClioCoreQueue.job_active_scan`), so a legitimate state transition
    racing this call can never be misreported as index corruption -- only a
    job the live record still says is queued, yet the index does not
    contain, raises.
    """
    current, candidates, truncated = queue.job_active_scan(
        job.job_id, limit=MAX_QUEUE_POSITION_RECORDS
    )
    if current.state != JobState.QUEUED:
        return {"state": current.state.value, "jobs_ahead": None, "position": None}
    if truncated:
        raise QueueConflictError(
            "relay queue position exceeds the bounded active-job scan; "
            "run queue retention or increase indexed queue-position support"
        )
    jobs_ahead = 0
    found = False
    for candidate in candidates:
        if candidate.job_id == current.job_id:
            found = True
            break
        if candidate.cluster == current.cluster and candidate.state == JobState.QUEUED:
            jobs_ahead += 1
    if not found:
        raise QueueConflictError(
            f"queued job is absent from the bounded active-job index: {current.job_id}"
        )
    return {"state": current.state.value, "jobs_ahead": jobs_ahead, "position": jobs_ahead + 1}


def poll_slurm_status(scheduler_job_id: str) -> SchedulerStatus:
    """Compatibility wrapper for older callers that explicitly request SLURM."""
    return provider_for_scheduler("slurm").poll(scheduler_job_id)
