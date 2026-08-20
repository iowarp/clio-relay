"""Quantitative/phase semantics for one native JARVIS package-progress event.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): validates and compares individual
``jarvis.progress.v1`` events (state/label/current/total/unit/determinate
coherence, phase-signature equality, and non-regression across a phase). Used
exclusively by ``jarvis_mcp_validation_lifecycle_progress.py``'s package
progress monotonicity checks -- not part of the facade's public surface.
"""

from __future__ import annotations

import math
from typing import TypeGuard, cast

from clio_relay.jarvis_mcp_validation_core import JSON

_JARVIS_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)
_MAX_JARVIS_PROGRESS_IDENTITY_TEXT = 256


def _valid_jarvis_progress_semantics(progress: JSON) -> bool:
    """Validate the quantitative and phase semantics of one native progress event."""
    state = progress.get("state")
    label = progress.get("label")
    if (
        not isinstance(state, str)
        or state not in _JARVIS_PROGRESS_STATES
        or not isinstance(label, str)
        or not label.strip()
        or len(label) > _MAX_JARVIS_PROGRESS_IDENTITY_TEXT
    ):
        return False
    current = progress.get("current")
    total = progress.get("total")
    if current is not None and (not _finite_progress_number(current) or current < 0):
        return False
    if total is not None and (
        not _finite_progress_number(total)
        or total <= 0
        or current is None
        or not _finite_progress_number(current)
        or current > total
    ):
        return False
    unit = progress.get("unit")
    if unit is not None and (
        not isinstance(unit, str)
        or not unit.strip()
        or len(unit) > _MAX_JARVIS_PROGRESS_IDENTITY_TEXT
    ):
        return False
    determinate = progress.get("determinate")
    return isinstance(determinate, bool) and determinate is (
        current is not None and total is not None
    )


def _finite_progress_number(value: object) -> TypeGuard[int | float]:
    """Return whether a value is a finite, non-boolean JSON number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def _jarvis_progress_semantic_signature(progress: JSON) -> tuple[object, ...]:
    """Return fields that must not change without a new progress sequence."""
    return tuple(
        progress.get(field)
        for field in (
            "state",
            "label",
            "determinate",
            "current",
            "total",
            "unit",
        )
    )


def _jarvis_progress_transition_nonregressing(previous: JSON, current: JSON) -> bool:
    """Reject quantitative regression within a phase while allowing explicit phase changes."""
    previous_phase = (previous.get("state"), previous.get("label"))
    current_phase = (current.get("state"), current.get("label"))
    if current_phase != previous_phase:
        return True
    previous_unit = previous.get("unit")
    current_unit = current.get("unit")
    if previous_unit is not None and current_unit != previous_unit:
        return False
    previous_total = previous.get("total")
    current_total = current.get("total")
    if previous_total is not None and current_total != previous_total:
        return False
    previous_value = previous.get("current")
    current_value = current.get("current")
    return previous_value is None or bool(
        current_value is not None
        and cast(int | float, current_value) >= cast(int | float, previous_value)
    )


def _compact_package_progress(progress: JSON) -> JSON:
    """Keep validation reports bounded while retaining progress semantics."""
    return {
        field: progress.get(field)
        for field in (
            "schema_version",
            "execution_id",
            "package_id",
            "package_name",
            "state",
            "label",
            "sequence",
            "determinate",
            "current",
            "total",
            "unit",
        )
    }
