"""clio-relay#214 review (D1/D2/D5): the queue-coupled half of the restored
runtime-prediction capability.

``application_runtime_prediction.py`` is pure (no I/O); this module is the
only caller expected to reach a real ``ClioCoreQueue``, and owns:

* D1 -- bounded reads only. Never ``ClioCoreQueue.list_progress`` (an
  UNBOUNDED per-job scan, proven to cost 10.8s at 2000 records and raise
  ``QueueConflictError`` past ``MAX_BOUNDED_SCAN_RECORDS``) -- always
  ``ClioCoreQueue.latest_progress_window(job_id, limit=...)``. A failed
  read (any :class:`~clio_relay.errors.RelayError`) is caught here and
  reported as the typed ``progress_history_unavailable`` absence, never
  raised into the watch loop.
* D2 -- :class:`ExecutionWatchPredictionTracker` only re-reads/recomputes
  when a cheap O(1) probe (``ClioCoreQueue.latest_job_progress``, an
  index-pointer read, never a scan) shows the job's latest progress record
  has changed since the last recompute, or the caller forces a refresh
  (the phase itself changed, or the final terminal poll). The caller only
  persists/emits when :func:`~clio_relay.application_runtime_prediction.
  prediction_materially_changed` says the refreshed value is worth it.
* D5 -- the tracker remembers which progress label axis was selected
  (``preferred_label``) and threads it back into every prediction call, so
  the axis stays stable across polls once chosen.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from clio_relay import application_runtime_prediction
from clio_relay.errors import RelayError

if TYPE_CHECKING:
    from clio_relay.models import ProgressRecord


class _PredictionQueue(Protocol):
    """The two bounded reads a prediction refresh needs -- a real ``ClioCoreQueue``
    satisfies this structurally, so does any test double that implements the
    same two methods; ``refresh`` depends on this narrow interface, never on
    the full queue facade.
    """

    def latest_job_progress(self, job_id: str) -> tuple[ProgressRecord | None, int, bool]:
        """Return the job's most recently recorded progress, its count, and truncation."""
        ...

    def latest_progress_window(self, job_id: str, *, limit: int) -> list[ProgressRecord]:
        """Return at most ``limit`` of the job's most recent progress records."""
        ...


#: clio-relay#214 review D1: the ONLY bounded read a poll loop may use --
#: exactly what the trimmed-mean algorithm can consume (``sample_window``
#: transitions need ``sample_window + 1`` samples), never more.
PROGRESS_WINDOW_READ_LIMIT = application_runtime_prediction.DEFAULT_SAMPLE_WINDOW + 1


@dataclass
class ExecutionWatchPredictionTracker:
    """Per-watch mutable state -- one instance per ``run_execution_watch`` call.

    Never shared across jobs or persisted between watches: D5's axis
    stability and D2's write-materiality both need memory across polls
    within ONE job's watch loop, and this is the only place that memory
    lives.
    """

    preferred_label: str | None = None
    last_written: dict[str, object] | None = None
    _last_probed_progress_id: str | None = field(default=None, repr=False)

    def refresh(
        self,
        queue: _PredictionQueue,
        job_id: str,
        *,
        force: bool,
    ) -> tuple[dict[str, object], bool]:
        """Return ``(prediction, should_write)`` for one poll.

        ``force=True`` (the phase itself changed, or the final terminal
        poll) always re-reads and recomputes. Otherwise a cheap O(1) probe
        gates the bounded-but-non-trivial recompute: if the job's latest
        progress record has not changed since the last recompute, the
        predictor is a pure function of unchanged input, so the last
        computed prediction is returned unchanged with
        ``should_write=False`` and no bounded window read at all.
        """
        try:
            latest, _count, _truncated = queue.latest_job_progress(job_id)
            probed_id = latest.progress_id if latest is not None else None
            if (
                not force
                and self.last_written is not None
                and probed_id == self._last_probed_progress_id
            ):
                return self.last_written, False
            self._last_probed_progress_id = probed_id
            window = queue.latest_progress_window(job_id, limit=PROGRESS_WINDOW_READ_LIMIT)
        except RelayError:
            prediction = application_runtime_prediction.absent_prediction(
                application_runtime_prediction.PROGRESS_HISTORY_UNAVAILABLE_REASON
            )
        else:
            prediction = application_runtime_prediction.application_runtime_prediction_for_progress(
                window, preferred_label=self.preferred_label
            )
            if prediction["status"] == "predicted":
                raw_basis = prediction["basis"]
                assert isinstance(raw_basis, dict)  # predicted always carries a basis
                basis = cast("dict[str, object]", raw_basis)
                label = basis.get("label")
                if isinstance(label, str):
                    self.preferred_label = label
        should_write = force or application_runtime_prediction.prediction_materially_changed(
            prediction, self.last_written
        )
        if should_write:
            self.last_written = prediction
        return prediction, should_write
