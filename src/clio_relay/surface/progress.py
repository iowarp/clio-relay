"""What a task says of a running job's latest step (RL43)."""

from __future__ import annotations

from typing import Any, cast

from clio_relay.jobs.handles import VIEW_METADATA
from clio_relay.jobs.records import OperationRecord


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
    if result.get("message"):
        parts.append(str(result["message"]))
    return "; ".join(parts) or None
