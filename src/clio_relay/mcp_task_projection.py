"""SEP-2663 task status/timing constants and the relay state-map projection.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): this is
the pure, dependency-free table that projects a relay job's own observed
state onto the SEP-2663 task status vocabulary, plus the two durable-task
timing constants every projecting caller (``RelayMcpRuntime.task_status``,
``RelayTool``'s ``TaskConfig``, ``RelayTasksExtension``) shares.
"""

from __future__ import annotations

from typing import Final, Literal

TASK_POLL_INTERVAL_MS = 1_000
TASK_TTL_MS = 30 * 24 * 60 * 60 * 1_000

type McpTaskStatus = Literal[
    "working",
    "input_required",
    "completed",
    "failed",
    "cancelled",
]
type RelayStateMapRow = tuple[tuple[str, ...], McpTaskStatus, bool | None]

# Cross-repo federation contract: keep this table identical to clio-agent's
# RELAY_STATE_MAP. The source rationale is docs/mcp-tasks.md:99-114.
RELAY_STATE_MAP: Final[tuple[RelayStateMapRow, ...]] = (
    (("queued", "leased", "running"), "working", None),
    (("durable_input_round",), "input_required", None),
    (("succeeded",), "completed", False),
    (("tool_failure",), "completed", True),
    (("protocol_error",), "failed", None),
    (("canceled",), "cancelled", None),
)
_RELAY_STATE_PROJECTIONS: Final[dict[str, tuple[McpTaskStatus, bool | None]]] = {
    observation: (status, is_error)
    for observations, status, is_error in RELAY_STATE_MAP
    for observation in observations
}


def _relay_state_projection(observation: str) -> tuple[McpTaskStatus, bool | None]:
    """Return the committed MCP task projection for one relay observation."""
    try:
        return _RELAY_STATE_PROJECTIONS[observation]
    except KeyError as exc:
        raise ValueError(f"relay observation has no MCP task projection: {observation}") from exc
