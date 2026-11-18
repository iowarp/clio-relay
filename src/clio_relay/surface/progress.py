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
    viewable = [item["name"] for item in outputs if (item.get("metadata") or {}).get(VIEW_METADATA)]
    if viewable:
        parts.append("views: " + ", ".join(viewable))
    return "; ".join(parts) or None
