"""A service job's compute steps, waited on as a task of their own.

A ``serve`` task completes at readiness, with the endpoint, and its job runs
on. A job that also computes, such as a solver beside the viewer that watches
it, has a second moment an agent waits for: its compute steps ending. With no
signal for it an agent polls the job's files and the scheduler, so relay gives
one: ``serve(service=<operation id>, until="steps")``.

- With tasks the call is a task, ``<operation id>.steps``, that completes when
  every compute step of the job has ended, or the job has. Its result carries
  each step's state, reason, hosts and time, the job's outputs and its card,
  and its status message names the steps as they end. A task client wakes on
  the completion, so the agent waits rather than polls.
- Without tasks the call waits up to ``wait_s`` and returns the same result,
  whose ``until.ended`` says whether the steps have ended.

The steps are the job card's ``compute`` components: a JARVIS job's step ends
when JARVIS records it, and any other job's ends with the job. So one path
serves every adapter and access means. The task's state is read from the
operation record, which every look at the job updates, so it survives a relay
restart; only its start and a cancel are kept, under ``result.step_wait``.
Cancelling the wait stops the waiting, not the job.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import mcp_types
from fastmcp.tools import ToolResult
from fastmcp_tasks.models import CancelTaskResult, CreateTaskResult, GetTaskResult
from mcp.shared.exceptions import MCPError
from pydantic import Field

from clio_relay import jarvis_plan
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.refusals import Refusal
from clio_relay.surface import job_card
from clio_relay.surface.results import operation_result

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

UNTIL = "steps"
SUFFIX = ".steps"
KEY = "step_wait"
WAIT_META = "ai.iowarp.relay/wait"
"""On a ``serve(until=...)`` result: the id of the task that waits."""
LOOK_S = 10.0
"""A poll looks at the job again only when relay's last look is older than this."""
POLL_S = 5.0
TTL_MS = 7 * 24 * 3600 * 1000
POLL_INTERVAL_MS = 5000

Until = Annotated[
    Literal["steps"] | None,
    Field(description="With service: wait for its compute steps to end"),
]
"""The ``serve`` tool's ``until`` argument."""


def task_id(operation_id: str) -> str:
    """Return the id of the task that waits for an operation's compute steps."""
    return operation_id + SUFFIX


def operation_of(task: str) -> str | None:
    """Return the operation a wait task belongs to, or ``None`` for any other task."""
    return task[: -len(SUFFIX)] if task.endswith(SUFFIX) and len(task) > len(SUFFIX) else None


def steps(service: RelayService, record: OperationRecord) -> list[dict[str, Any]]:
    """Return the job's compute steps, each in its latest state."""
    card = job_card.snapshot(service, record) or {}
    lines = {
        str(line.get("component")): line
        for line in cast(list[dict[str, Any]], (record.result or {}).get("components") or [])
    }
    return [
        {
            "id": item.get("id"),
            "label": item.get("label"),
            "package": item.get("package"),
            "state": item.get("state"),
            "reason": item.get("reason"),
            "hosts": item.get("hosts") or [],
            "at": lines.get(str(item.get("id")), {}).get("at"),
        }
        for item in cast(list[dict[str, Any]], card.get("components") or [])
        if item.get("role") == "compute"
    ]


def ended(record: OperationRecord, found: list[dict[str, Any]]) -> bool:
    """Say whether every compute step has ended, or the job has."""
    if record.state.final:
        return True
    return bool(found) and all(step["state"] in jarvis_plan.TERMINAL for step in found)


def message(record: OperationRecord, found: list[dict[str, Any]]) -> str:
    """Say where the compute steps stand, in the person's words."""
    if not found:
        if record.state.final:
            return f"the job is {record.state.value}"
        return "the job has no compute step besides its service, so this waits for the job to end"
    named = ", ".join(
        f"{step['label']} {step['state']}" + (f" ({step['reason']})" if step["reason"] else "")
        for step in found
    )
    if not ended(record, found):
        return f"waiting for the compute steps: {named}"
    after = f"the job is {record.state.value}" if record.state.final else "the service runs on"
    return f"the compute steps ended: {named}; {after}"


def _wait(record: OperationRecord) -> dict[str, Any]:
    found = (record.result or {}).get(KEY)
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def arm(service: RelayService, operation_id: str) -> OperationRecord:
    """Record that a wait started, unless one is already waiting."""
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        wait = _wait(record)
        if not wait or wait.get("cancelled_at"):
            record.result = {**(record.result or {}), KEY: {"armed_at": utc_now().isoformat()}}
            service.operations.put(operation_id, record)
        return record


async def look(service: RelayService, operation_id: str) -> OperationRecord:
    """Return the record, looking at the job first when relay's last look is stale."""
    record = service.operation(operation_id)
    seen = (record.result or {}).get("observed_at")
    with contextlib.suppress(TypeError, ValueError):
        if utc_now() - datetime.fromisoformat(str(seen)) < timedelta(seconds=LOOK_S):
            return record
    return await service.refresh(operation_id)


async def wait(service: RelayService, operation_id: str, wait_s: float) -> OperationRecord:
    """Without tasks: look until the compute steps end or ``wait_s`` runs out."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, wait_s)
    while True:
        record = await look(service, operation_id)
        if ended(record, steps(service, record)) or loop.time() >= deadline:
            return record
        await asyncio.sleep(min(POLL_S, max(0.05, deadline - loop.time())))


def result(service: RelayService, record: OperationRecord) -> ToolResult:
    """Render the wait: the steps first, then the operation as ``serve`` shows it."""
    found = steps(service, record)
    view = service.view(record)
    view["until"] = {"until": UNTIL, "ended": ended(record, found), "steps": found}
    base = operation_result(view)
    first = base.content[0] if base.content else None
    summary = first.text if isinstance(first, mcp_types.TextContent) else ""
    line = message(record, found)
    return ToolResult(
        content=[
            mcp_types.TextContent(type="text", text=f"{line[:1].upper()}{line[1:]}.\n{summary}")
        ],
        structured_content=view,
        meta={**(base.meta or {}), WAIT_META: task_id(record.operation_id)},
        is_error=bool(base.is_error) or any(step["state"] == "failed" for step in found),
    )


async def call(service: RelayService, operation_id: str, task: bool, wait_s: float) -> ToolResult:
    """``serve(service=..., until="steps")``. With tasks the server turns it into the task."""
    record = arm(service, operation_id)
    if not task:
        record = await wait(service, operation_id, wait_s)
    return result(service, record)


def _fields(record: OperationRecord, found: list[dict[str, Any]]) -> dict[str, Any]:
    wait_state = _wait(record)
    status = "completed" if ended(record, found) else "working"
    return {
        "task_id": task_id(record.operation_id),
        "status": "cancelled" if wait_state.get("cancelled_at") else status,
        "status_message": message(record, found),
        "created_at": wait_state.get("armed_at") or record.created_at.isoformat(),
        "last_updated_at": record.last_change.isoformat(),
        "ttl_ms": TTL_MS,
        "poll_interval_ms": POLL_INTERVAL_MS,
    }


def _armed(service: RelayService, task: str) -> str:
    operation_id = operation_of(task)
    record = service.operations.get(operation_id) if operation_id else None
    if operation_id is None or record is None or not _wait(record):
        raise MCPError(code=-32602, message=f"no task {task!r}")
    return operation_id


def created(service: RelayService, task: str) -> CreateTaskResult:
    """Return the task a ``serve(until=...)`` call became."""
    record = service.operation(_armed(service, task))
    return CreateTaskResult(**_fields(record, steps(service, record)))


def intercepted(
    service: RelayService, arguments: dict[str, Any], outcome: ToolResult
) -> CreateTaskResult | None:
    """For a task client: the task a ``serve(until=...)`` result stands for, else ``None``."""
    waiting = (outcome.meta or {}).get(WAIT_META)
    if not isinstance(waiting, str) or arguments.get("until") is None:
        return None
    return created(service, waiting)


async def get(service: RelayService, task: str) -> GetTaskResult:
    """``tasks/get`` for a wait: its status, and its result once the steps ended."""
    operation_id = _armed(service, task)
    try:
        record = await look(service, operation_id)
    except Refusal as refusal:
        raise MCPError(code=-32603, message=refusal.message, data=refusal.to_dict()) from refusal
    found = steps(service, record)
    fields = _fields(record, found)
    if (card := job_card.snapshot(service, record)) is not None:
        fields["_meta"] = {job_card.JOB_META: card}
    if fields["status"] != "completed":
        return job_card.JobTaskResult(**fields)
    shown = result(service, record)
    final = mcp_types.CallToolResult.model_validate(
        {
            "content": shown.content,
            "structuredContent": shown.structured_content,
            "isError": shown.is_error,
            "_meta": shown.meta,
        }
    )
    return job_card.JobTaskResult(
        **fields, result=final.model_dump(by_alias=True, mode="json", exclude_none=True)
    )


def cancel(service: RelayService, task: str) -> CancelTaskResult:
    """Stop waiting. The job runs on; a wait whose steps already ended stays completed."""
    operation_id = _armed(service, task)
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if not ended(record, steps(service, record)):
            stopped = {**_wait(record), "cancelled_at": utc_now().isoformat()}
            record.result = {**(record.result or {}), KEY: stopped}
            service.operations.put(operation_id, record)
    return CancelTaskResult()
