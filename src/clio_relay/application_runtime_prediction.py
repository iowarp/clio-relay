"""clio-relay#214: the restored runtime-prediction capability.

Predicts an application's remaining wall-clock duration from the first N
steps of its own structured progress observations -- grounded in the
partial-execution premise for iterative parallel applications (a short
partial run's per-step timing already predicts the whole, once a minimal
startup/warmup period has passed).

ARCHAEOLOGY (clio-relay#214): a prediction implementation already existed --
``LammpsThermoProgressAdapter._prediction`` in the now-deleted
``src/clio_relay/package_adapters/lammps.py`` (created ``fd2ddc9``,
2026-07-08; deleted as unrelated collateral inside ``93095eb``'s
``feat!: complete production 1.0 relay contracts (#12)``, 2026-07-13 --
recover with ``git show 93095eb^:src/clio_relay/package_adapters/lammps.py``).
Its algorithm is kept verbatim: discard an early ``warmup_samples`` count,
take a trimmed mean of the *recent* per-step wall-clock rate (a bounded
``sample_window``, so a rate change mid-run is tracked rather than diluted
by the whole history), multiply by the remaining work. Only the data SOURCE
changes. The old adapter parsed LAMMPS's own thermo stdout inline and kept a
private in-memory sample list, coupling the (application-generic) prediction
algorithm to one application's log format. Package progress parsing is now
fully pluggable (``progress_adapters.py``'s external entry-point protocol,
landed after the LAMMPS adapter was deleted) and every adapter's candidates
already land as durable :class:`~clio_relay.models.ProgressRecord`\\ s via
``queue_progress.py`` -- house rule 4 ("do NOT add a fifth store"; join an
existing one) -- so this module reads THAT structured history instead of
re-parsing text, and works for any package's progress adapter, not only
LAMMPS's.

Composed onto the execution-phase payload beside ``application_verdict``
(:func:`clio_relay.execution_watch.execution_phase_job_metadata`) with the
same discipline: typed fields, no keyword/heuristic inference from prose,
and an explicit typed ``reason`` -- never a fabricated number -- whenever
the structured history does not yet support one. This module makes no
network/queue call of its own; the caller supplies the already-read
``ProgressRecord`` history (:meth:`clio_relay.core_queue.ClioCoreQueue.
list_progress`), keeping this a pure, directly testable function.

Known residual gap (not created or closed by this module): the scheduler-
deferred ``jarvis_run`` watch loop (``execution_watch.run_execution_watch``)
does not currently run any package progress adapter over its console-tailed
stdout, so a purely scheduler-deferred job may have no progress history at
all yet -- this reports the typed ``no_progress_observations`` absence
rather than fabricating a number, exactly as designed, and the capability
activates automatically once that separate ingestion gap is closed for any
job whose progress the queue already records (today, that is the
synchronous/local execution path).
"""

from __future__ import annotations

import statistics
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from clio_relay.models import ProgressRecord

#: clio-relay#214: the typed, additive runtime-prediction schema -- see
#: :func:`application_runtime_prediction_for_progress`.
APPLICATION_RUNTIME_PREDICTION_SCHEMA = "clio-relay.application-runtime-prediction.v1"

#: Minimum number of (current, observed_at) samples, within the trailing
#: ``sample_window``, required before a rate is trusted -- mirrors the
#: recovered adapter's own default.
DEFAULT_WARMUP_SAMPLES = 2

#: Bounded trailing window of progress samples the rate is computed over,
#: so a mid-run rate change (a checkpoint write, an I/O-bound phase) is
#: tracked instead of diluted by the whole run's history -- mirrors the
#: recovered adapter's own default.
DEFAULT_SAMPLE_WINDOW = 8

#: Closed typed-reason vocabulary for an absent prediction. Never extended
#: implicitly by a caller-supplied string -- a new cause needs a new
#: constant here, the same closed-vocabulary discipline
#: ``execution_watch._NON_TERMINAL_STATE_PHASE`` uses.
NO_PROGRESS_OBSERVATIONS_REASON = "no_progress_observations"
NO_DECLARED_TOTAL_REASON = "no_declared_total"
INSUFFICIENT_SAMPLES_REASON = "insufficient_samples"
NO_POSITIVE_RATE_SAMPLES_REASON = "no_positive_rate_samples"


def application_runtime_prediction_for_progress(
    progress_history: Sequence[ProgressRecord],
    *,
    warmup_samples: int = DEFAULT_WARMUP_SAMPLES,
    sample_window: int = DEFAULT_SAMPLE_WINDOW,
) -> dict[str, object]:
    """Predict remaining runtime from one job's own structured progress history.

    ``progress_history`` is the job's own :class:`~clio_relay.models.
    ProgressRecord` history, already ordered oldest-first (the shape
    ``ClioCoreQueue.list_progress`` returns) -- this function makes no
    store call of its own and infers nothing from message prose, only from
    the structured ``label``/``current``/``total``/``created_at`` fields
    every progress source (durable, provider-validated) already carries.

    Only the most recently observed progress *label* is used -- a job may
    carry more than one progress axis (a package that reports both, say,
    ``"timestep"`` and ``"bytes_written"``), and mixing series with
    different units/domains would silently corrupt the rate. This is an
    exact structured-field match, never a prose/keyword guess at which
    series "looks like" the real one.

    Returns a dict always shaped by :data:`APPLICATION_RUNTIME_PREDICTION_SCHEMA`,
    with ``status`` either ``"predicted"`` (``predicted_remaining_seconds``/
    ``confidence``/``basis`` populated, ``reason`` is ``None``) or
    ``"absent"`` (the numeric fields are ``None``, ``reason`` is one of this
    module's typed reason constants) -- never a fabricated number when the
    structured history does not yet support one.
    """
    if not progress_history:
        return _absent(NO_PROGRESS_OBSERVATIONS_REASON)
    latest = progress_history[-1]
    labeled = [record for record in progress_history if record.label == latest.label]
    total = _latest_declared_total(labeled)
    if total is None:
        return _absent(NO_DECLARED_TOTAL_REASON)
    samples: list[tuple[float, datetime]] = []
    for record in labeled:
        current = record.current
        if current is not None:
            samples.append((current, record.created_at))
    windowed = samples[-sample_window:] if sample_window > 0 else samples
    if len(windowed) <= warmup_samples:
        return _absent(INSUFFICIENT_SAMPLES_REASON)
    rates = _positive_rates(windowed)
    if not rates:
        return _absent(NO_POSITIVE_RATE_SAMPLES_REASON)
    ordered = sorted(rates)
    trimmed = ordered[1:-1] if len(ordered) > 2 else ordered
    seconds_per_unit = statistics.fmean(trimmed)
    current = windowed[-1][0]
    remaining = max(0.0, total - current)
    return {
        "schema_version": APPLICATION_RUNTIME_PREDICTION_SCHEMA,
        "status": "predicted",
        "reason": None,
        "predicted_remaining_seconds": remaining * seconds_per_unit,
        "confidence": "observed" if len(rates) >= 2 else "low_sample",
        "basis": {
            "method": "trimmed_mean_step_rate_after_warmup",
            "label": latest.label,
            "unit": latest.unit,
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


def _positive_rates(samples: Sequence[tuple[float, datetime]]) -> list[float]:
    """Return per-unit wall-clock rates for consecutive, forward-moving samples."""
    rates: list[float] = []
    for (previous_step, previous_time), (step, timestamp) in zip(
        samples, samples[1:], strict=False
    ):
        step_delta = step - previous_step
        time_delta = (timestamp - previous_time).total_seconds()
        if step_delta <= 0 or time_delta < 0:
            continue
        rates.append(time_delta / step_delta)
    return rates


def _latest_declared_total(records: Sequence[ProgressRecord]) -> float | None:
    """Return the most recently declared ``total`` in a label-matched series."""
    for record in reversed(records):
        if record.total is not None:
            return record.total
    return None


def _absent(reason: str) -> dict[str, object]:
    """Return the typed, no-basis prediction shape -- never a fabricated number."""
    return {
        "schema_version": APPLICATION_RUNTIME_PREDICTION_SCHEMA,
        "status": "absent",
        "reason": reason,
        "predicted_remaining_seconds": None,
        "confidence": None,
        "basis": None,
    }
