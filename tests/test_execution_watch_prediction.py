"""clio-relay#214 review (D1/D2/D5): unit coverage for
``ExecutionWatchPredictionTracker`` -- the queue-coupled half of the
runtime-prediction capability. A lightweight fake queue (implementing only
the two methods the tracker calls) stands in for ``ClioCoreQueue``, so
these tests run without a real queue store and can precisely control what
each poll observes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from clio_relay.errors import QueueConflictError
from clio_relay.execution_watch_prediction import ExecutionWatchPredictionTracker
from clio_relay.models import ProgressRecord

_EPOCH = datetime(2026, 8, 1, tzinfo=UTC)


@dataclass
class _FakeQueue:
    """Minimal stand-in for ``ClioCoreQueue``'s two prediction-facing reads."""

    history: list[ProgressRecord] = field(default_factory=lambda: [])
    raise_error: bool = False
    window_read_count: int = 0
    probe_read_count: int = 0

    def latest_job_progress(self, job_id: str) -> tuple[ProgressRecord | None, int, bool]:
        self.probe_read_count += 1
        if self.raise_error:
            raise QueueConflictError("simulated bounded-read failure")
        if not self.history:
            return None, 0, False
        return self.history[-1], len(self.history), False

    def latest_progress_window(self, job_id: str, *, limit: int) -> list[ProgressRecord]:
        self.window_read_count += 1
        if self.raise_error:
            raise QueueConflictError("simulated bounded-read failure")
        return self.history[-limit:]


def _record(
    *, current: float, total: float, offset_seconds: float, label: str = "timestep"
) -> ProgressRecord:
    return ProgressRecord(
        job_id="job-tracker-test",
        label=label,
        current=current,
        total=total,
        created_at=_EPOCH + timedelta(seconds=offset_seconds),
    )


_STEADY_HISTORY = [_record(current=step, total=100, offset_seconds=step) for step in (0, 10, 20)]


def test_first_refresh_always_recomputes_and_writes() -> None:
    """Nothing written yet: the tracker always reads and reports should_write=True."""
    queue = _FakeQueue(history=list(_STEADY_HISTORY))
    tracker = ExecutionWatchPredictionTracker()
    prediction, should_write = tracker.refresh(queue, "job-tracker-test", force=False)
    assert prediction["status"] == "predicted"
    assert should_write is True
    assert queue.window_read_count == 1


def test_refresh_skips_the_bounded_read_when_progress_has_not_moved() -> None:
    """clio-relay#214 review D2: an unchanged latest record needs no recompute at all."""
    queue = _FakeQueue(history=list(_STEADY_HISTORY))
    tracker = ExecutionWatchPredictionTracker()
    first_prediction, first_should_write = tracker.refresh(queue, "job-tracker-test", force=False)
    assert first_should_write is True
    assert queue.window_read_count == 1

    second_prediction, second_should_write = tracker.refresh(queue, "job-tracker-test", force=False)
    assert second_prediction == first_prediction
    assert second_should_write is False
    # The cheap O(1) probe ran again, but the bounded window read did NOT.
    assert queue.probe_read_count == 2
    assert queue.window_read_count == 1


def test_refresh_recomputes_once_progress_advances() -> None:
    """A new progress record changes the O(1) probe's identity: recompute, don't skip."""
    queue = _FakeQueue(history=list(_STEADY_HISTORY))
    tracker = ExecutionWatchPredictionTracker()
    tracker.refresh(queue, "job-tracker-test", force=False)
    assert queue.window_read_count == 1

    queue.history.append(_record(current=30, total=100, offset_seconds=30))
    prediction, should_write = tracker.refresh(queue, "job-tracker-test", force=False)
    assert queue.window_read_count == 2
    assert prediction["status"] == "predicted"
    # The rate stayed constant (1.0 s/unit); predicted_remaining_seconds
    # moved from 80.0 -> 70.0 purely because one more unit of real work
    # completed -- a 12.5% move, under both materiality thresholds
    # (relative 20%, absolute 30s), so should_write is False.
    assert prediction["predicted_remaining_seconds"] == 70.0
    assert should_write is False


def test_refresh_force_always_writes_even_without_a_material_change() -> None:
    """``force=True`` (a phase change, or the final poll) always writes -- unconditionally."""
    queue = _FakeQueue(history=list(_STEADY_HISTORY))
    tracker = ExecutionWatchPredictionTracker()
    _first_prediction, first_should_write = tracker.refresh(queue, "job-tracker-test", force=True)
    assert first_should_write is True
    second_prediction, second_should_write = tracker.refresh(queue, "job-tracker-test", force=True)
    assert second_should_write is True
    assert queue.window_read_count == 2
    assert second_prediction["status"] == "predicted"


def test_refresh_reports_typed_absence_on_a_queue_failure_never_raises() -> None:
    """clio-relay#214 review D1: a failed bounded read is a typed absence, not an exception."""
    queue = _FakeQueue(raise_error=True)
    tracker = ExecutionWatchPredictionTracker()
    prediction, should_write = tracker.refresh(queue, "job-tracker-test", force=False)
    assert prediction["status"] == "absent"
    assert prediction["reason"] == "progress_history_unavailable"
    assert should_write is True  # nothing written yet


def test_refresh_reports_typed_absence_when_a_healthy_queue_starts_failing() -> None:
    """A queue that starts failing mid-watch degrades to the typed absence, not a crash."""
    queue = _FakeQueue(history=list(_STEADY_HISTORY))
    tracker = ExecutionWatchPredictionTracker()
    healthy_prediction, _ = tracker.refresh(queue, "job-tracker-test", force=True)
    assert healthy_prediction["status"] == "predicted"

    queue.raise_error = True
    prediction, should_write = tracker.refresh(queue, "job-tracker-test", force=True)
    assert prediction["status"] == "absent"
    assert prediction["reason"] == "progress_history_unavailable"
    assert should_write is True  # status flip (predicted -> absent) is always material


def test_preferred_label_stays_stable_across_refresh_calls() -> None:
    """clio-relay#214 review D5, via the stateful Tracker (not the pure function directly).

    Reuses the two-axis fixture that flaps under the pure function without
    stability threading -- the Tracker must NOT flap, because it remembers
    and re-supplies ``preferred_label`` on every call.
    """
    first_poll = [
        _record(current=0, total=100, offset_seconds=0, label="a"),
        _record(current=10, total=100, offset_seconds=10, label="a"),
        _record(current=1000, total=5000, offset_seconds=11, label="b"),
        _record(current=1500, total=5000, offset_seconds=15, label="b"),
        _record(current=20, total=100, offset_seconds=20, label="a"),
    ]
    queue = _FakeQueue(history=list(first_poll))
    tracker = ExecutionWatchPredictionTracker()
    first_prediction, _ = tracker.refresh(queue, "job-tracker-test", force=True)
    first_basis = first_prediction["basis"]
    assert isinstance(first_basis, dict)
    assert first_basis["label"] == "a"
    assert tracker.preferred_label == "a"

    queue.history.append(_record(current=2000, total=5000, offset_seconds=21, label="b"))
    second_prediction, _ = tracker.refresh(queue, "job-tracker-test", force=True)
    second_basis = second_prediction["basis"]
    assert isinstance(second_basis, dict)
    # Without stability this would flip to "b" (its record is now more
    # recent) -- the tracker keeps "a" because it is still a candidate.
    assert second_basis["label"] == "a"
    assert tracker.preferred_label == "a"
