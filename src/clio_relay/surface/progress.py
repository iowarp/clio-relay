"""What a task says of a running job's latest step (RL43)."""

from __future__ import annotations

from math import floor, log10
from typing import Any, cast

from clio_relay.jarvis_submit import spoken_duration
from clio_relay.jobs.handles import VIEW_METADATA
from clio_relay.jobs.records import OperationRecord


def pace_note(step: dict[str, Any]) -> str | None:
    """Say the run's measured pace and where it projects, from ``data["pace"]``."""
    pace = cast(dict[str, Any], cast(dict[str, Any], step.get("data") or {}).get("pace") or {})
    per_step = pace.get("seconds_per_step")
    if not isinstance(per_step, int | float):
        return None
    unit = cast(dict[str, Any], step.get("data") or {}).get("unit") or "step"
    said = (
        f"measured {_spoken_rate(per_step)} per {unit} "
        f"over {unit}s {pace.get('from_step')} to {pace.get('to_step')}"
    )
    if pace.get("projected_end_at"):
        said += f", so the last {unit} lands about {pace['projected_end_at']}"
    margin = pace.get("limit_margin_s")
    if isinstance(margin, int) and margin < 0:
        said += f", {spoken_duration(-margin)} after the job's time limit ends it"
    elif isinstance(margin, int):
        said += f", {spoken_duration(margin)} before the job's time limit"
    return said


def _spoken_rate(seconds: float) -> str:
    """Say a step's time: in seconds to two figures under a minute, else as a duration."""
    if seconds >= 60:
        return spoken_duration(int(seconds))
    if seconds <= 0:
        return "0 s"
    return f"{seconds:.{max(0, 1 - floor(log10(seconds)))}f} s"


def progress_message(record: OperationRecord) -> str | None:
    """Name the latest step, its note and the outputs that have a view descriptor.

    The full step, with each output's descriptor, is in the operation's
    ``result["progress"]``; this is the short line for the task's status.
    """
    found: object = (record.result or {}).get("progress")
    if not isinstance(found, dict):
        return None
    step = cast(dict[str, Any], found)
    parts: list[str] = []
    if step.get("step") is not None:
        total = step.get("total")
        parts.append(f"step {step['step']}" + (f" of {total}" if total is not None else ""))
    if step.get("message"):
        parts.append(str(step["message"]))
    if note := pace_note(step):
        parts.append(note)
    outputs = cast(list[dict[str, Any]], step.get("outputs") or [])
    viewable = [
        item["name"]
        for item in outputs
        if cast(dict[str, Any], item.get("metadata") or {}).get(VIEW_METADATA)
    ]
    if viewable:
        parts.append("views: " + ", ".join(viewable))
    return "; ".join(parts) or None


def latest_note(record: OperationRecord) -> str | None:
    """Return the latest step, else the latest thing the site or relay said of the job.

    A job still waiting in the queue says where it stands instead: that is the
    news while it waits.
    """
    result = record.result or {}
    found: object = result.get("message")
    message = found if isinstance(found, str) and found else None
    if result.get("queue") and message:
        return message
    return progress_message(record) or message


def observation_line(view: dict[str, Any]) -> str | None:
    """Say when relay last looked at an open job and what it saw, for a tool's text."""
    result = cast(dict[str, Any], view.get("result") or {})
    observed = result.get("observed_at")
    parts: list[str] = []
    if observed:
        parts.append(f"relay last looked at {observed}")
    step = result.get("progress")
    if isinstance(step, dict) and cast(dict[str, Any], step).get("step") is not None:
        shown = cast(dict[str, Any], step)
        total = shown.get("total")
        parts.append(f"step {shown['step']}" + (f" of {total}" if total is not None else ""))
        if note := pace_note(shown):
            parts.append(note)
    if result.get("message"):
        parts.append(str(result["message"]))
    return "; ".join(parts) or None
