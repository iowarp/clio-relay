"""clio-relay#214 review D1: ``ClioCoreQueue.latest_progress_window`` bounds.

``list_progress`` reads a job's ENTIRE progress family -- unbounded, proven
by the review to cost 1s at 100 records and 10.8s at 2000, and to raise
``QueueConflictError`` once a job's history exceeds ``MAX_BOUNDED_SCAN_
RECORDS``. ``latest_progress_window`` is the bounded replacement the
runtime-prediction poll loop must use instead: this module proves it
returns exactly the requested trailing tail, oldest-first, and never more
than ``limit`` records regardless of how much history exists.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JarvisRunSpec, JobKind, ProgressRecord, RelayJob


def _submitted_job(queue: ClioCoreQueue, *, idempotency_key: str) -> RelayJob:
    return queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key=idempotency_key,
        )
    )


def test_latest_progress_window_is_empty_for_a_job_with_no_progress(tmp_path: Path) -> None:
    """No progress recorded yet: an empty list, never an error."""
    queue = ClioCoreQueue(tmp_path)
    job = _submitted_job(queue, idempotency_key="no-progress")
    assert queue.latest_progress_window(job.job_id, limit=9) == []


def test_latest_progress_window_returns_all_when_under_the_limit(tmp_path: Path) -> None:
    """Fewer records than ``limit`` exist: every one comes back, oldest-first."""
    queue = ClioCoreQueue(tmp_path)
    job = _submitted_job(queue, idempotency_key="under-limit")
    for step in range(3):
        queue.append_progress(
            ProgressRecord(job_id=job.job_id, label="timestep", current=step, total=100)
        )
    window = queue.latest_progress_window(job.job_id, limit=9)
    assert [record.current for record in window] == [0, 1, 2]


def test_latest_progress_window_returns_only_the_bounded_recent_tail(tmp_path: Path) -> None:
    """More records than ``limit`` exist: only the most recent ``limit`` come back.

    clio-relay#214 review D1's core proof -- this must NEVER scale with the
    job's total progress history. 15 records are appended (comfortably more
    than a realistic sample_window+1), and a window of 5 is requested; the
    result is exactly the last 5, oldest-first, matching what
    ``list_progress()`` (the unbounded reference, safe to call here since
    this test's history is small) reports for the same tail.
    """
    queue = ClioCoreQueue(tmp_path)
    job = _submitted_job(queue, idempotency_key="over-limit")
    for step in range(15):
        queue.append_progress(
            ProgressRecord(job_id=job.job_id, label="timestep", current=step, total=100)
        )
    window = queue.latest_progress_window(job.job_id, limit=5)
    reference = queue.list_progress(job.job_id)
    assert len(window) == 5
    assert [record.current for record in window] == [record.current for record in reference[-5:]]
    assert [record.progress_id for record in window] == [
        record.progress_id for record in reference[-5:]
    ]


def test_latest_progress_window_rejects_a_nonpositive_limit(tmp_path: Path) -> None:
    """A caller bug (limit <= 0) fails loud and typed, never silently reads 0 or everything."""
    queue = ClioCoreQueue(tmp_path)
    job = _submitted_job(queue, idempotency_key="bad-limit")
    try:
        queue.latest_progress_window(job.job_id, limit=0)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for a non-positive window limit")
