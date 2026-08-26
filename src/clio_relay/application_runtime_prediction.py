"""clio-relay#214: the restored runtime-prediction capability.

Predicts an application's remaining wall-clock duration from the first N
steps of its own structured progress observations -- grounded in the
partial-execution premise for iterative parallel applications (a short
partial run's per-step timing already predicts the whole, once a minimum
sample count has been observed).

ARCHAEOLOGY (clio-relay#214): a prediction implementation already existed --
``LammpsThermoProgressAdapter._prediction`` in the now-deleted
``src/clio_relay/package_adapters/lammps.py`` (created ``fd2ddc9``,
2026-07-08; deleted as unrelated collateral inside ``93095eb``'s
``feat!: complete production 1.0 relay contracts (#12)``, 2026-07-13 --
recover with ``git show 93095eb^:src/clio_relay/package_adapters/lammps.py``).
Its math is kept verbatim: require at least ``warmup_samples`` observations
in a bounded trailing ``sample_window`` before trusting a rate at all, take
a trimmed mean of that window's per-step wall-clock rate, multiply by the
remaining work. ``warmup_samples`` is a MINIMUM-SAMPLE GATE, not a discard
-- every sample in the window, including the earliest ones once the gate is
met, contributes to the trimmed-mean rate. (The recovered adapter's own
``prediction_method`` field, ``trimmed_mean_step_time_after_warmup``, was
equally imprecise about this; this module's ``basis["method"]`` is renamed
to ``trimmed_mean_recent_rate_after_minimum_samples`` to describe the real
behavior -- clio-relay#214 review D6. No math changed.) Only the data
SOURCE changed. The old adapter parsed LAMMPS's own thermo stdout inline
and kept a private in-memory sample list, coupling the (application-
generic) prediction algorithm to one application's log format. Package
progress parsing is now fully pluggable (``progress_adapters.py``'s
external entry-point protocol, landed after the LAMMPS adapter was
deleted) and every adapter's candidates already land as durable
:class:`~clio_relay.models.ProgressRecord`\\ s via ``queue_progress.py`` --
house rule 4 ("do NOT add a fifth store"; join an existing one) -- so this
module reads THAT structured history instead of re-parsing text, and works
for any package's progress adapter, not only LAMMPS's.

Composed onto the execution-phase payload beside ``application_verdict``
(:func:`clio_relay.execution_watch.execution_phase_job_metadata`) with the
same discipline: typed fields, no keyword/heuristic inference from prose,
and an explicit typed ``reason`` -- never a fabricated number -- whenever
the structured history does not yet support one. This module makes no
queue/store call of its own; the bounded read and the poll-loop
orchestration (materiality-gated writes, D2; the axis-stability state
across polls, D5) live in ``execution_watch_prediction.py``, which is the
only caller expected to reach a real queue.

clio-relay#214 review deviations from a naive verbatim port (all proven by
the reviewer against this module's first version):

* D3 (clock): the recovered adapter's time axis was the APPLICATION's own
  reported elapsed-seconds (LAMMPS's ``CPU`` thermo column). The generic
  successor's most literal analog is
  ``ProgressRecord.metadata["progress_observed_at_epoch"]`` -- a
  source-reported observation instant, validated by
  ``progress_provenance.validate_jarvis_execution_progress_metadata`` and
  populated by ``endpoint_progress_trust.py``'s two JARVIS-notification
  trust functions. ``ProgressRecord.created_at`` is the RELAY's
  persistence instant instead, and no producer sets it deliberately as an
  observation clock -- both of today's real producers (the package-log/
  sidecar ingest path, and the monitor-rule ``record_progress`` action)
  can batch-create many records in one tight write burst. Proven: 5000
  steps persisted ~1ms apart predicted 1.99s remaining for a
  million-step job at full confidence when ``created_at`` was used
  unconditionally. This module now prefers ``progress_observed_at_epoch``
  when EVERY sample in the window carries it, and falls back to
  ``created_at`` only with a distinct ``confidence="persistence_clock"``
  and a ``basis["clock"]`` field naming which was used -- never presented
  as equal to a source-reported timestamp.
* D4 (rate guard): the recovered adapter's guard was ``time_delta < 0``,
  which lets a same-instant pair (``time_delta == 0``) through as
  rate ``0.0`` -- proven: three same-instant records predicted "0.0
  seconds remaining" at ``confidence="observed"`` with 90 units of real
  work left. This module signs a no-fabrication contract the original
  (an advisory ETA display) did not; the guard is ``time_delta <= 0``,
  a deliberate deviation from verbatim.
* D5 (axis stability): see :func:`_select_series_label`.
"""

from __future__ import annotations

import math
import statistics
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clio_relay.models import ProgressRecord

#: clio-relay#214: the typed, additive runtime-prediction schema -- see
#: :func:`application_runtime_prediction_for_progress`.
APPLICATION_RUNTIME_PREDICTION_SCHEMA = "clio-relay.application-runtime-prediction.v1"

#: Minimum number of (current, observed_at) samples, within the trailing
#: ``sample_window``, required before a rate is trusted -- mirrors the
#: recovered adapter's own default. A GATE, not a discard -- see the module
#: docstring's D6 note.
DEFAULT_WARMUP_SAMPLES = 2

#: Bounded trailing window of progress samples the rate is computed over,
#: so a mid-run rate change (a checkpoint write, an I/O-bound phase) is
#: tracked instead of diluted by the whole run's history -- mirrors the
#: recovered adapter's own default. Also the read bound
#: ``execution_watch_prediction.py`` uses (clio-relay#214 review D1) --
#: this module never reads the store itself, but its caller should never
#: fetch more history than this algorithm can use.
DEFAULT_SAMPLE_WINDOW = 8

#: clio-relay#214 review D2's materiality thresholds -- see
#: :func:`prediction_materially_changed`.
PREDICTION_MATERIAL_RELATIVE_DELTA = 0.20
PREDICTION_MATERIAL_ABSOLUTE_SECONDS = 30.0

#: Closed typed-reason vocabulary for an absent prediction. Never extended
#: implicitly by a caller-supplied string -- a new cause needs a new
#: constant here, the same closed-vocabulary discipline
#: ``execution_watch._NON_TERMINAL_STATE_PHASE`` uses.
NO_PROGRESS_OBSERVATIONS_REASON = "no_progress_observations"
NO_DECLARED_TOTAL_REASON = "no_declared_total"
INSUFFICIENT_SAMPLES_REASON = "insufficient_samples"
NO_POSITIVE_RATE_SAMPLES_REASON = "no_positive_rate_samples"
#: clio-relay#214 review D1: a bounded progress read failed (a typed
#: RelayError from the queue, e.g. a stale/missing job index) -- reported
#: as an absent prediction, never a raised exception into the watch loop.
PROGRESS_HISTORY_UNAVAILABLE_REASON = "progress_history_unavailable"


def application_runtime_prediction_for_progress(
    progress_history: Sequence[ProgressRecord],
    *,
    warmup_samples: int = DEFAULT_WARMUP_SAMPLES,
    sample_window: int = DEFAULT_SAMPLE_WINDOW,
    preferred_label: str | None = None,
) -> dict[str, object]:
    """Predict remaining runtime from one job's own structured progress history.

    ``progress_history`` is the job's own :class:`~clio_relay.models.
    ProgressRecord` history, already ordered oldest-first -- the shape
    ``ClioCoreQueue.latest_progress_window`` returns (never
    ``list_progress``'s unbounded read; see the module docstring's D1
    note). This function makes no store call of its own and infers nothing
    from message prose, only from the structured ``label``/``current``/
    ``total``/``created_at``/``metadata["progress_observed_at_epoch"]``
    fields every progress source (durable, provider-validated) already
    carries.

    ``preferred_label``, held by the caller across polls, keeps the same
    progress axis selected as long as it remains a candidate -- see
    :func:`_select_series_label`.

    Returns a dict always shaped by :data:`APPLICATION_RUNTIME_PREDICTION_SCHEMA`,
    with ``status`` either ``"predicted"`` (``predicted_remaining_seconds``/
    ``confidence``/``basis`` populated, ``reason`` is ``None``) or
    ``"absent"`` (the numeric fields are ``None``, ``reason`` is one of this
    module's typed reason constants) -- never a fabricated number when the
    structured history does not yet support one.
    """
    if not progress_history:
        return absent_prediction(NO_PROGRESS_OBSERVATIONS_REASON)
    selected_label = _select_series_label(progress_history, preferred_label)
    if selected_label is None:
        return absent_prediction(NO_DECLARED_TOTAL_REASON)
    labeled = [record for record in progress_history if record.label == selected_label]
    total = _latest_declared_total(labeled)
    if total is None:
        # Unreachable given _select_series_label's own candidacy predicate;
        # kept as a defensive typed exit rather than an assertion, so a
        # future change to that predicate fails safe, not loud.
        return absent_prediction(NO_DECLARED_TOTAL_REASON)
    candidate_records = [record for record in labeled if record.current is not None]
    clock, is_source_clock = _observation_clock(candidate_records)
    samples: list[tuple[float, datetime]] = []
    for record in candidate_records:
        current = record.current
        if current is not None:
            samples.append((current, _observation_time(record, clock=clock)))
    windowed = samples[-sample_window:] if sample_window > 0 else samples
    if len(windowed) <= warmup_samples:
        return absent_prediction(INSUFFICIENT_SAMPLES_REASON)
    rates = _positive_rates(windowed)
    if not rates:
        return absent_prediction(NO_POSITIVE_RATE_SAMPLES_REASON)
    ordered = sorted(rates)
    trimmed = ordered[1:-1] if len(ordered) > 2 else ordered
    seconds_per_unit = statistics.fmean(trimmed)
    current = windowed[-1][0]
    remaining = max(0.0, total - current)
    if not is_source_clock:
        confidence = "persistence_clock"
    else:
        confidence = "observed" if len(rates) >= 2 else "low_sample"
    return {
        "schema_version": APPLICATION_RUNTIME_PREDICTION_SCHEMA,
        "status": "predicted",
        "reason": None,
        "predicted_remaining_seconds": remaining * seconds_per_unit,
        "confidence": confidence,
        "basis": {
            "method": "trimmed_mean_recent_rate_after_minimum_samples",
            "clock": clock,
            "label": selected_label,
            "unit": labeled[-1].unit,
            "current": current,
            "total": total,
            "remaining": remaining,
            "seconds_per_unit": seconds_per_unit,
            "min_seconds_per_unit": min(trimmed),
            "max_seconds_per_unit": max(trimmed),
            "rate_samples": len(rates),
            "trimmed_rate_samples": len(trimmed),
            "samples_considered": len(windowed),
            "warmup_samples": warmup_samples,
            "sample_window": sample_window,
        },
    }


def prediction_materially_changed(
    new: dict[str, object],
    old: dict[str, object] | None,
    *,
    relative_delta: float = PREDICTION_MATERIAL_RELATIVE_DELTA,
    absolute_seconds: float = PREDICTION_MATERIAL_ABSOLUTE_SECONDS,
) -> bool:
    """clio-relay#214 review D2's materiality rule for gating a durable write.

    Always material when ``old`` is ``None`` (nothing written yet for this
    watch) or either side's ``status`` differs (an absent<->predicted
    flip must always reach a served value). When both sides are
    ``"absent"``, material only if the typed ``reason`` differs. When both
    are ``"predicted"``, material when ``predicted_remaining_seconds``
    moves by more than ``relative_delta`` (20% of the previous estimate)
    OR by more than ``absolute_seconds`` (30s) -- either threshold alone
    is enough, so a large remaining estimate does not need a huge absolute
    swing, and a small one does not need a large relative swing, to count
    as a real change worth persisting.
    """
    if old is None:
        return True
    if new["status"] != old["status"]:
        return True
    if new["status"] != "predicted":
        return new["reason"] != old["reason"]
    new_remaining = new["predicted_remaining_seconds"]
    old_remaining = old["predicted_remaining_seconds"]
    if not isinstance(new_remaining, int | float) or not isinstance(old_remaining, int | float):
        return True
    delta = abs(new_remaining - old_remaining)
    if delta > absolute_seconds:
        return True
    return old_remaining > 0 and (delta / old_remaining) > relative_delta


def _select_series_label(
    progress_history: Sequence[ProgressRecord],
    preferred_label: str | None,
) -> str | None:
    """Select which progress label's series grounds the prediction.

    clio-relay#214 review D5: a job may report more than one progress axis
    (interleaved records with different ``label``s); picking the LAST
    record's label unconditionally flaps between axes emitted in
    alternation -- proven, predicted remaining swung 90.0 -> 8.0 between
    consecutive polls purely from emission order. A label is a candidate
    only if it has declared a total (:func:`_latest_declared_total`);
    among candidates, the one that most recently ADVANCED (its latest
    record's ``current`` exceeds its own previous record's, or it has only
    one record so far, so a brand-new axis is still visible) wins,
    tie-broken by recency of its latest record. ``preferred_label`` --
    held by the CALLER across polls -- keeps winning as long as it is
    STILL a candidate, so a momentarily-quieter real axis is not abandoned
    just because a different axis's record happens to be more recent this
    poll.
    """
    positions: dict[str, list[int]] = {}
    for index, record in enumerate(progress_history):
        positions.setdefault(record.label, []).append(index)
    groups = {
        label: [progress_history[index] for index in indices]
        for label, indices in positions.items()
    }
    candidates = [
        label for label, group in groups.items() if _latest_declared_total(group) is not None
    ]
    if not candidates:
        return None
    if preferred_label is not None and preferred_label in candidates:
        return preferred_label

    def _rank(label: str) -> tuple[bool, int]:
        group = groups[label]
        advancing = True
        if len(group) >= 2:
            previous_current = group[-2].current
            latest_current = group[-1].current
            advancing = (
                previous_current is not None
                and latest_current is not None
                and latest_current > previous_current
            )
        return (advancing, positions[label][-1])

    return max(candidates, key=_rank)


def _observation_clock(records: Sequence[ProgressRecord]) -> tuple[str, bool]:
    """Return (clock_name, is_source_reported) -- never mixed within one window.

    See the module docstring's D3 note. Only ``"progress_observed_at_epoch"``
    when EVERY record supplies a valid one; otherwise ``"created_at"`` for
    the WHOLE window -- comparing a source-reported instant against a
    persistence instant in the same delta would not be knowably safer than
    the created_at-only case this exists to fix.
    """
    if records and all(_source_observed_epoch(record) is not None for record in records):
        return "progress_observed_at_epoch", True
    return "created_at", False


def _source_observed_epoch(record: ProgressRecord) -> float | None:
    """Return the record's validated source-reported observation epoch, if any."""
    value = record.metadata.get("progress_observed_at_epoch")
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if not math.isfinite(value):
        return None
    return float(value)


def _observation_time(record: ProgressRecord, *, clock: str) -> datetime:
    """Return one record's timestamp on the chosen clock."""
    if clock == "progress_observed_at_epoch":
        epoch = _source_observed_epoch(record)
        if epoch is not None:
            return datetime.fromtimestamp(epoch, tz=UTC)
    return record.created_at


def _positive_rates(samples: Sequence[tuple[float, datetime]]) -> list[float]:
    """Return per-unit wall-clock rates for consecutive, forward-moving samples.

    clio-relay#214 review D4: both deltas must be STRICTLY positive -- a
    same-instant pair (``time_delta == 0``) is skipped, not counted as a
    ``0.0`` rate. Proven: without this, three same-instant records
    predicted "0.0 seconds remaining" at full confidence with real work
    left. A deliberate deviation from the recovered adapter's own
    ``time_delta < 0`` guard (see the module docstring).
    """
    rates: list[float] = []
    for (previous_step, previous_time), (step, timestamp) in zip(
        samples, samples[1:], strict=False
    ):
        step_delta = step - previous_step
        time_delta = (timestamp - previous_time).total_seconds()
        if step_delta <= 0 or time_delta <= 0:
            continue
        rates.append(time_delta / step_delta)
    return rates


def _latest_declared_total(records: Sequence[ProgressRecord]) -> float | None:
    """Return the most recently declared ``total`` in a label-matched series."""
    for record in reversed(records):
        if record.total is not None:
            return record.total
    return None


def absent_prediction(reason: str) -> dict[str, object]:
    """Return the typed, no-basis prediction shape for ``reason``.

    Public: ``execution_watch_prediction.py`` (the queue-coupled caller)
    also needs this shape for a queue-layer failure this module has no way
    to observe itself (:data:`PROGRESS_HISTORY_UNAVAILABLE_REASON`, D1) --
    never a fabricated number, regardless of which layer detected the
    absence.
    """
    return {
        "schema_version": APPLICATION_RUNTIME_PREDICTION_SCHEMA,
        "status": "absent",
        "reason": reason,
        "predicted_remaining_seconds": None,
        "confidence": None,
        "basis": None,
    }
