"""clio-relay#214: unit coverage for the restored runtime-prediction capability.

Pure-function coverage over :func:`clio_relay.application_runtime_prediction.
application_runtime_prediction_for_progress` -- no queue, no filesystem, no
subprocess. Every fixture below constructs :class:`~clio_relay.models.
ProgressRecord`\\ s directly with explicit ``created_at`` timestamps so the
derived per-step rate is exact and deterministic, not dependent on wall-clock
timing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from clio_relay import application_runtime_prediction as runtime_prediction
from clio_relay.models import ProgressRecord

_EPOCH = datetime(2026, 8, 1, tzinfo=UTC)


def _record(
    *,
    current: float | None,
    total: float | None,
    offset_seconds: float,
    label: str = "timestep",
) -> ProgressRecord:
    return ProgressRecord(
        job_id="job-runtime-prediction-test",
        label=label,
        current=current,
        total=total,
        created_at=_EPOCH + timedelta(seconds=offset_seconds),
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


def test_predicted_with_observed_confidence() -> None:
    """A clean, constant-rate series predicts exactly and reports 'observed' confidence."""
    history = [
        _record(current=step, total=100, offset_seconds=step) for step in (0, 10, 20, 30, 40)
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["reason"] is None
    assert prediction["predicted_remaining_seconds"] == 60.0
    assert prediction["confidence"] == "observed"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["method"] == "trimmed_mean_step_rate_after_warmup"
    assert basis["current"] == 40
    assert basis["total"] == 100
    assert basis["remaining"] == 60
    assert basis["seconds_per_unit"] == 1.0
    assert basis["rate_samples"] == 4
    assert basis["trimmed_rate_samples"] == 2


def test_predicted_with_low_sample_confidence() -> None:
    """One valid rate (a repeated ``current`` value drops the other pair) is 'low_sample'."""
    history = [
        _record(current=0, total=100, offset_seconds=0),
        _record(current=10, total=100, offset_seconds=5),
        # No forward progress since the previous sample -- this pair's rate
        # is dropped (step_delta <= 0), leaving exactly one valid rate.
        _record(current=10, total=100, offset_seconds=15),
    ]
    prediction = runtime_prediction.application_runtime_prediction_for_progress(history)
    assert prediction["status"] == "predicted"
    assert prediction["confidence"] == "low_sample"
    basis = prediction["basis"]
    assert isinstance(basis, dict)
    assert basis["rate_samples"] == 1
    assert prediction["predicted_remaining_seconds"] == 45.0  # (100 - 10) * 0.5 s/unit


def test_only_latest_progress_label_series_is_used() -> None:
    """A job reporting more than one progress axis never mixes the series.

    An unrelated, numerically incompatible ``"bytes"`` series is interleaved
    with the real ``"timestep"`` series; only records matching the LATEST
    record's label are used -- an exact structured-field match, not a
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
