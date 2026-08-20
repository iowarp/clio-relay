"""Post-admission agent input elicitation guard for durable MCP tasks.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): the
one-follow-up-round elicitation guard ``RelayMcpRuntime._park_agent_input``/
``_resume_agent_input`` re-enter (docs/mcp-tasks.md:147-153), plus the small
opt-in predicate deciding whether a given agent-task call requested it at
all. Pure and context-scoped -- no relay queue/storage dependency -- so it
has no reason to depend on ``mcp_task_runtime.py``; the runtime module
depends on this one instead.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import mcp_types

if TYPE_CHECKING:
    from fastmcp.server.context import Context

JSON = dict[str, Any]

_AGENT_TASK_TOOL_NAMES = frozenset({"relay_submit_agent", "relay_submit_remote_agent"})
_AGENT_INPUT_KEY = "agent_message"
_AGENT_INPUT_REQUEST_STATE_SCHEMA = "clio-relay.agent-message-input.v1"
_MAX_AGENT_INPUT_MESSAGE_BYTES = 128 * 1_024


def _agent_input_request_state(task_id: str) -> str:
    """Bind one post-admission agent input round to its durable relay task."""
    return json.dumps(
        {
            "schema_version": _AGENT_INPUT_REQUEST_STATE_SCHEMA,
            "task_id": task_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _post_admission_agent_input_guard(
    ctx: Context,
    *,
    task_id: str,
) -> mcp_types.InputRequiredResult | mcp_types.ElicitResult:
    """Ask for one follow-up agent message, then consume it on guarded re-entry."""
    responses = ctx.input_responses
    if responses is None:
        return mcp_types.InputRequiredResult(
            input_requests={
                _AGENT_INPUT_KEY: mcp_types.ElicitRequest(
                    params=mcp_types.ElicitRequestFormParams(
                        message="Send a follow-up message to the running remote agent.",
                        requested_schema={
                            "type": "object",
                            "properties": {
                                "message": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 32_768,
                                }
                            },
                            "required": ["message"],
                            "additionalProperties": False,
                        },
                    )
                )
            },
            request_state=_agent_input_request_state(task_id),
        )
    if ctx.request_state != _agent_input_request_state(task_id):
        raise ValueError("post-admission agent input lost its durable request state")
    raw_response = responses.get(_AGENT_INPUT_KEY)
    if raw_response is None:
        raise ValueError("post-admission agent input omitted its requested response")
    response = (
        raw_response
        if isinstance(raw_response, mcp_types.ElicitResult)
        else mcp_types.ElicitResult.model_validate(raw_response)
    )
    if response.action == "accept":
        content = response.content
        if not isinstance(content, dict):
            raise ValueError("accepted post-admission agent input omitted its content")
        message = content.get("message")
        if not isinstance(message, str) or not message:
            raise ValueError("accepted post-admission agent input omitted its message")
        if len(message.encode("utf-8")) > _MAX_AGENT_INPUT_MESSAGE_BYTES:
            raise ValueError("post-admission agent input message exceeds its byte limit")
    return response


def _requests_document(result: mcp_types.InputRequiredResult) -> dict[str, JSON]:
    """Serialize one bounded guard result for the durable task projection."""
    requests = result.input_requests or {}
    return {
        key: request.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=True,
        )
        for key, request in requests.items()
    }


def _agent_input_enabled(tool_name: str, arguments: JSON) -> bool:
    """Return whether an agent task explicitly opted into one follow-up round."""
    return tool_name in _AGENT_TASK_TOOL_NAMES and arguments.get("request_followup_message") is True
