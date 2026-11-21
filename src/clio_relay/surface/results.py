"""How relay's tools render: the annotations they carry and an operation as a result."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import mcp_types
from fastmcp.tools import ToolResult

from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.surface.endpoints import address
from clio_relay.surface.progress import observation_line

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

OPERATION_META = "ai.iowarp.relay/operation"
SERVICE_META = "ai.iowarp.relay/service"

# Relay acts only on the sites the person registered, each under its own policy, and it
# asks itself for the decisions that policy names (spend, installs, reviews). So its
# world is closed in the MCP sense, and a client that accepts bounded changes without a
# question can run relay calls unattended while relay's own asks still reach the person.
# A read tool never changes anything, so a host may run it in a mode that only reads.
READ = mcp_types.ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=False)
CHANGE = mcp_types.ToolAnnotations(
    read_only_hint=False, destructive_hint=True, open_world_hint=False
)
START = mcp_types.ToolAnnotations(
    read_only_hint=False, destructive_hint=False, open_world_hint=False
)


def text(value: dict[str, Any]) -> str:
    """``value`` as the JSON a tool's text content shows."""
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _summary(view: dict[str, Any]) -> str:
    line = f"Operation {view['operation_id']} on {view['site']} is {view['state']}"
    if view.get("exit_code") is not None:
        line += f" with exit code {view['exit_code']}"
    line += "."
    result: dict[str, Any] = view.get("result") or {}
    if not OperationState(view["state"]).final:
        seen = observation_line(view)
        if seen:
            line += f"\n{seen}."
    endpoint: dict[str, Any] = result.get("endpoint") or {}
    ports: list[dict[str, Any]] = endpoint.get("ports") or []
    for port in ports:
        line += f"\n{port['name']}: {address(port)}"
    if ports and view["state"] == OperationState.RUNNING.value:
        line += (
            f"\nThe service is ready and keeps running until cancel(operation="
            f"{view['operation_id']!r}) or its job ends; describe(job=...) follows it."
        )
    tail: str | None = result.get("stdout_tail")
    if tail:
        line += f"\n\nstdout (tail):\n{tail}"
    if view.get("error"):
        line += f"\n\n{view['error'].get('reason')}: {view['error'].get('message')}"
    return line


def operation_result(view: dict[str, Any]) -> ToolResult:
    """Render an operation. A failed job is an error result; a cancelled one is not."""
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=_summary(view))],
        structured_content=view,
        meta={OPERATION_META: view["operation_id"]},
        is_error=view["state"] == OperationState.FAILED.value,
    )


def record_result(service: RelayService, record: OperationRecord) -> ToolResult:
    """Render an operation, with a service's panel data in ``_meta`` only (RL22)."""
    result = operation_result(service.view(record))
    if record.kind == "serve":
        ui = service.serve_hook.ui_data(record)
        if ui is not None:
            result.meta = {**(result.meta or {}), SERVICE_META: ui}
    return result
