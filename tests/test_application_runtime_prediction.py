"""clio-relay#214: unit coverage for the restored runtime-prediction capability.

Pure-function coverage over :func:`clio_relay.application_runtime_prediction.
application_runtime_prediction_for_progress` and
:func:`~clio_relay.application_runtime_prediction.prediction_materially_changed`
-- no queue, no filesystem, no subprocess. Every fixture constructs
:class:`~clio_relay.models.ProgressRecord`\\ s directly with explicit
``created_at`` (and, where relevant, ``progress_observed_at_epoch``)
timestamps so the derived per-step rate is exact and deterministic.

Includes the clio-relay#214 adversarial-review D2-D6 fixes: bounded-window
materiality (D2), the source-vs-persistence observation clock (D3), the
same-instant rate guard (D4), progress-axis selection stability (D5), and
the honest warmup-gate description/method name (D6). D1 (the bounded queue
read) is covered in ``tests/test_queue_progress_bounded_window.py`` and
``tests/test_execution_watch_prediction.py`` -- this module has no queue.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

from clio_relay import application_runtime_prediction as runtime_prediction
from clio_relay.models import ProgressRecord

_EPOCH = datetime(2026, 8, 1, tzinfo=UTC)
_EPOCH_SECONDS = _EPOCH.timestamp()


def _record(
    *,
    current: float | None,
    total: float | None,
    offset_seconds: float,
    label: str = "timestep",
    observed_epoch_offset: float | None = None,
) -> ProgressRecord:
    """Build one test ProgressRecord.

    ``offset_seconds`` sets ``created_at`` (the relay's persistence
    instant). ``observed_epoch_offset``, when given, additionally sets
    ``metadata["progress_observed_at_epoch"]`` (the source-reported
    instant) -- the two are independent so a test can make them diverge,
    the way a real batch-created progress log does (clio-relay#214 review
    D3).
    """
    metadata: dict[str, object] = {}
    if observed_epoch_offset is not None:
        metadata["progress_observed_at_epoch"] = _EPOCH_SECONDS + observed_epoch_offset
    return ProgressRecord(
        job_id="job-runtime-prediction-test",
        label=label,
        current=current,
        total=total,
        created_at=_EPOCH + timedelta(seconds=offset_seconds),
        metadata=metadata,
    )


def test_no_history_reports_no_progress_observations() -> None:
    """An empty history is a typed absence, never an error or a fabricated number."""
    prediction = runtime_prediction.application_runtime_prediction_for_progress([])
    assert prediction["schema_version"] == runtime_prediction.APPLICATION_RUNTIME_PREDICTION_SCHEMA
    assert prediction["status"] == "absent"
    assert prediction["reason"] == runtime_prediction.NO_PROGRESS_OBSERVATIONS_REASON
    assert prediction["predicted_remaining_seconds"] is None
    assert prediction["confidence"] is None
    assert prediction["basis"] is None


def test_no_declared_total_reports_typed_absence() -> None:
    """Progress is observed but no source ever declared a total: remaining work is unknowable."""
    history = [
        _record(current=0, total=None, offset_seconds=0),
        _record(current=10, total=None, offset_seconds=10),
        _record(current=20, total=None, offset_seconds=20),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "absent"
    assert prediction["reason"] == runtime_prediction.NO_DECLARED_TOTAL_REASON


def test_insufficient_samples_reports_typed_absence() -> None:
    """Exactly ``warmup_samples`` observations is not yet enough to trust a rate."""
    history = [
        _record(current=0, total=100, offset_seconds=0),
        _record(current=10, total=100, offset_seconds=10),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(
        history, warmup_samples=2
    )
    assert prediction["status"] == "absent"
    assert prediction["reason"] == runtime_prediction.INSUFFICIENT_SAMPLES_REASON


def test_degenerate_history_reports_no_positive_rate_samples() -> None:
    """Enough samples exist, but none advance -- a genuinely degenerate run, not fabricated."""
    history = [
        _record(current=5, total=100, offset_seconds=0),
        _record(current=5, total=100, offset_seconds=5),
        _record(current=5, total=100, offset_seconds=10),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "absent"
    assert prediction["reason"] == runtime_prediction.NO_POSITIVE_RATE_SAMPLES_REASON


def test_same_instant_records_never_produce_a_zero_rate() -> None:
    """clio-relay#214 review D4, proven: three same-instant records must never predict '0.0s'.

    Without the ``time_delta <= 0`` guard, three records sharing one
    ``created_at`` (a plausible batch-write) let ``time_delta == 0`` pairs
    through as a fabricated ``0.0`` rate -- "0.0 seconds remaining" with 90
    real units of work left. The fix reports the same typed absence as any
    other degenerate history.
    """
    history = [
        _record(current=0, total=100, offset_seconds=0),
        _record(current=45, total=100, offset_seconds=0),
        _record(current=90, total=100, offset_seconds=0),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "absent"
    assert prediction["reason"] == runtime_prediction.NO_POSITIVE_RATE_SAMPLES_REASON
    assert prediction["predicted_remaining_seconds"] is None


def test_predicted_with_observed_confidence() -> None:
    """A clean, constant-rate series on the SOURCE clock predicts exactly.

    ``created_at`` is deliberately identical for every record here (a
    batch write); only ``progress_observed_at_epoch`` carries the real
    1s/unit spacing -- proving the source clock, not the persistence
    clock, grounds the "observed" confidence (clio-relay#214 review D3).
    """
    history = [
        _record(
            current=step,
            total=100,
            offset_seconds=0,
            observed_epoch_offset=step,
        )
        for step in (0, 10, 20, 30, 40)
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["reason"] is None
    assert prediction["predicted_remaining_seconds"] == 60.0
    assert prediction["confidence"] == "observed"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    # clio-relay#214 review D6: the recovered adapter's own field name
    # (trimmed_mean_step_time_after_warmup) claimed a discard that never
    # happened; renamed to describe the real minimum-sample-gate behavior.
    assert basis["method"] == "trimmed_mean_recent_rate_after_minimum_samples"
    assert basis["clock"] == "progress_observed_at_epoch"
    assert basis["current"] == 40
    assert basis["total"] == 100
    assert basis["remaining"] == 60
    assert basis["seconds_per_unit"] == 1.0
    assert basis["rate_samples"] == 4
    assert basis["trimmed_rate_samples"] == 2


def test_persistence_clock_fallback_is_labeled_distinctly() -> None:
    """No record carries a source-reported epoch: fall back to created_at, but say so.

    clio-relay#214 review D3: the math is identical to the source-clock
    case above (the created_at spacing here IS the real spacing), but the
    confidence must still read "persistence_clock", not "observed" -- the
    predictor cannot itself know whether created_at reflects reality this
    time or not, so it never claims the higher-trust label without a
    source-reported clock backing it.
    """
    history = [
        _record(current=step, total=100, offset_seconds=step) for step in (0, 10, 20, 30, 40)
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["predicted_remaining_seconds"] == 60.0
    assert prediction["confidence"] == "persistence_clock"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["clock"] == "created_at"


def test_mixed_clock_window_falls_back_to_persistence_clock() -> None:
    """A window where only SOME records carry a source epoch never mixes clocks.

    clio-relay#214 review D3: comparing a source-reported instant against
    a persistence instant in the same delta would not be knowably safer
    than the created_at-only case this exists to fix -- the whole window
    falls back together.
    """
    history = [
        _record(current=0, total=100, offset_seconds=0, observed_epoch_offset=0),
        _record(current=10, total=100, offset_seconds=10),  # no epoch on this one
        _record(current=20, total=100, offset_seconds=20, observed_epoch_offset=20),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["confidence"] == "persistence_clock"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["clock"] == "created_at"


def test_reproduces_batch_created_records_defeat_persistence_clock() -> None:
    """clio-relay#214 review D3's headline proof, reproduced and fixed.

    5000 real steps, observations spanning them persisted in one tight
    batch write (created_at deltas 5000x smaller than the real per-step
    cadence): created_at alone would predict a wildly wrong (far too
    short) remaining time for a million-step job. The source-reported
    epoch (spaced 1s apart, as the application actually ran) is preferred,
    so the SAME data predicts the true 995000s.
    """
    history = [
        _record(
            current=step,
            total=1_000_000,
            offset_seconds=step * 0.0002,  # a tight batch write, not the real cadence
            observed_epoch_offset=step,  # the real 1s/step cadence
        )
        for step in (0, 1000, 2000, 3000, 4000, 5000)
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["confidence"] == "observed"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["clock"] == "progress_observed_at_epoch"
    assert basis["seconds_per_unit"] == 1.0
    assert prediction["predicted_remaining_seconds"] == 995_000.0


def test_predicted_with_low_sample_confidence() -> None:
    """One valid rate (a repeated ``current`` value drops the other pair) is 'low_sample'."""
    history = [
        _record(current=0, total=100, offset_seconds=0, observed_epoch_offset=0),
        _record(current=10, total=100, offset_seconds=5, observed_epoch_offset=5),
        # No forward progress since the previous sample -- this pair's rate
        # is dropped (step_delta <= 0), leaving exactly one valid rate.
        _record(current=10, total=100, offset_seconds=15, observed_epoch_offset=15),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["confidence"] == "low_sample"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["rate_samples"] == 1
    assert prediction["predicted_remaining_seconds"] == 45.0  # (100 - 10) * 0.5 s/unit


def test_most_recently_advancing_total_bearing_axis_is_selected() -> None:
    """A job reporting more than one progress axis never mixes the series.

    An unrelated, numerically incompatible ``"bytes"`` series is interleaved
    with the real ``"timestep"`` series; both declare totals, but
    ``"timestep"`` is the axis that most recently advanced -- an exact
    structured-field selection (clio-relay#214 review D5), never a
    prose/keyword guess at which series is "the real one".
    """
    history = [
        _record(current=5000, total=50000, offset_seconds=0, label="bytes"),
        _record(current=0, total=100, offset_seconds=1, label="timestep"),
        _record(current=6000, total=50000, offset_seconds=2, label="bytes"),
        _record(current=10, total=100, offset_seconds=11, label="timestep"),
        _record(current=20, total=100, offset_seconds=21, label="timestep"),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["label"] == "timestep"
    assert basis["current"] == 20
    assert basis["total"] == 100


def test_axis_selection_flaps_without_a_preferred_label() -> None:
    """clio-relay#214 review D5's headline proof: unconditional last-wins flaps.

    Two axes, ``"a"`` and ``"b"``, are each individually well-behaved and
    advancing, but interleaved so the WINDOW's trailing record alternates.
    Reproduced here as the naive symptom this module no longer has (the
    default selection is fresh each call, tie-broken by recency) -- the
    companion test below shows how a caller holds it stable.
    """
    ending_with_a = [
        _record(current=0, total=100, offset_seconds=0, label="a"),
        _record(current=10, total=100, offset_seconds=10, label="a"),
        _record(current=1000, total=5000, offset_seconds=11, label="b"),
        _record(current=1500, total=5000, offset_seconds=15, label="b"),
        _record(current=20, total=100, offset_seconds=20, label="a"),
    ]
    ending_with_b = [
        *ending_with_a,
        _record(current=2000, total=5000, offset_seconds=21, label="b"),
    ]
    first = runtime_prediction.application_runtime_prediction_for_progress(ending_with_a)
    second = runtime_prediction.application_runtime_prediction_for_progress(ending_with_b)
    first_basis = first["basis"]
    second_basis = second["basis"]
    assert isinstance(first_basis, dict)
    assert isinstance(second_basis, dict)
    assert first_basis["label"] == "a"
    assert second_basis["label"] == "b"  # the flap this module's caller must prevent


def test_preferred_label_holds_the_axis_stable_across_polls() -> None:
    """The SAME two windows as above, with the caller holding ``preferred_label`` stable.

    clio-relay#214 review D5: a caller (``ExecutionWatchPredictionTracker``)
    that remembers the label it selected last time and passes it back in
    keeps that axis selected as long as it is still a candidate -- axis
    "a" is never abandoned just because "b" gained a more recent record.
    """
    ending_with_a = [
        _record(current=0, total=100, offset_seconds=0, label="a"),
        _record(current=10, total=100, offset_seconds=10, label="a"),
        _record(current=1000, total=5000, offset_seconds=11, label="b"),
        _record(current=1500, total=5000, offset_seconds=15, label="b"),
        _record(current=20, total=100, offset_seconds=20, label="a"),
    ]
    ending_with_b = [
        *ending_with_a,
        _record(current=2000, total=5000, offset_seconds=21, label="b"),
    ]
    first = runtime_prediction.application_runtime_prediction_for_progress(
        ending_with_a, preferred_label=None
    )
    first_basis = first["basis"]
    assert isinstance(first_basis, dict)
    first_label = cast("dict[str, object]", first_basis)["label"]
    assert isinstance(first_label, str)
    second = runtime_prediction.application_runtime_prediction_for_progress(
        ending_with_b, preferred_label=first_label
    )
    second_basis = second["basis"]
    assert isinstance(second_basis, dict)
    assert second_basis["label"] == first_basis["label"] == "a"


def test_preferred_label_is_abandoned_once_it_stops_being_a_candidate() -> None:
    """Stability yields once the held axis genuinely stops qualifying (loses its total)."""
    history = [
        _record(current=0, total=None, offset_seconds=0, label="a"),
        _record(current=10, total=None, offset_seconds=10, label="a"),
        _record(current=0, total=100, offset_seconds=1, label="b"),
        _record(current=10, total=100, offset_seconds=11, label="b"),
        _record(current=20, total=100, offset_seconds=21, label="b"),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(
        history, preferred_label="a"
    )
    assert prediction["status"] == "predicted"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["label"] == "b"


def test_trimmed_mean_drops_the_outlier_rate() -> None:
    """A single slow outlier pair does not skew the prediction -- it is trimmed away."""
    offsets_and_steps = [(0, 0), (10, 10), (20, 20), (30, 30), (130, 31)]
    history = [
        _record(current=step, total=1000, offset_seconds=offset)
        for offset, step in offsets_and_steps
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["rate_samples"] == 4
    assert basis["trimmed_rate_samples"] == 2
    assert basis["seconds_per_unit"] == 1.0
    assert prediction["predicted_remaining_seconds"] == 969.0  # (1000 - 31) * 1.0


def test_sample_window_bounds_recent_history() -> None:
    """A bounded trailing window tracks a mid-run rate change instead of diluting it.

    The first five samples run at 10s/unit, the last three at 1s/unit. The
    default (8-sample) window blends both regimes; a caller-supplied
    ``sample_window=3`` sees only the recent, faster regime.
    """
    slow = list(zip(range(0, 500, 100), range(0, 50, 10), strict=True))
    fast = [(401, 41), (402, 42), (403, 43)]
    history = [
        _record(current=step, total=1000, offset_seconds=offset) for offset, step in slow + fast
    ]
    windowed = runtime_prediction.application_runtime_prediction_for_progress(
        history, sample_window=3
    )
    assert windowed["status"] == "predicted"
    windowed_basis = windowed["basis"]
    assert isinstance(windowed_basis, dict)
    assert windowed_basis["samples_considered"] == 3
    assert windowed_basis["seconds_per_unit"] == 1.0

    blended = runtime_prediction.application_runtime_prediction_for_progress(history)
    blended_basis = blended["basis"]
    assert isinstance(blended_basis, dict)
    assert blended_basis["samples_considered"] == 8
    assert blended_basis["seconds_per_unit"] != windowed_basis["seconds_per_unit"]


# --------------------------------------------------------------------------
# D2: write-materiality (prediction_materially_changed).
# --------------------------------------------------------------------------


def _predicted(remaining: float) -> dict[str, object]:
    return {
        "schema_version": runtime_prediction.APPLICATION_RUNTIME_PREDICTION_SCHEMA,
        "status": "predicted",
        "reason": None,
        "predicted_remaining_seconds": remaining,
        "confidence": "observed",
        "basis": {"label": "timestep"},
    }


def test_materiality_always_true_when_nothing_was_written_yet() -> None:
    assert runtime_prediction.prediction_materially_changed(_predicted(100.0), None) is True


def test_materiality_true_on_status_flip() -> None:
    absent = runtime_prediction.absent_prediction(
        runtime_prediction.NO_PROGRESS_OBSERVATIONS_REASON
    )
    assert runtime_prediction.prediction_materially_changed(_predicted(100.0), absent) is True
    assert runtime_prediction.prediction_materially_changed(absent, _predicted(100.0)) is True


def test_materiality_true_on_changed_absence_reason() -> None:
    no_history = runtime_prediction.absent_prediction(
        runtime_prediction.NO_PROGRESS_OBSERVATIONS_REASON
    )
    no_total = runtime_prediction.absent_prediction(runtime_prediction.NO_DECLARED_TOTAL_REASON)
    assert runtime_prediction.prediction_materially_changed(no_total, no_history) is True


def test_materiality_false_for_a_small_relative_and_absolute_move() -> None:
    """A move under both thresholds (20% relative, 30s absolute) is not material."""
    old = _predicted(1000.0)
    new = _predicted(1010.0)  # +1%, +10s -- under both thresholds
    assert runtime_prediction.prediction_materially_changed(new, old) is False


def test_materiality_true_past_the_relative_threshold() -> None:
    old = _predicted(100.0)
    new = _predicted(125.0)  # +25%, but only +25s
    assert runtime_prediction.prediction_materially_changed(new, old) is True


def test_materiality_true_past_the_absolute_threshold() -> None:
    old = _predicted(1000.0)
    new = _predicted(1035.0)  # +3.5%, but +35s
    assert runtime_prediction.prediction_materially_changed(new, old) is True
