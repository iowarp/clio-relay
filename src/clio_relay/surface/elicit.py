"""Tool results for refusals, and the ways relay puts an ask to the person.

A modern client gets a multi-round-trip input request, a legacy one gets an
elicitation, and a client with neither gets a typed refusal naming the ask.
"""

from __future__ import annotations

from typing import Any, cast

import mcp_types
from fastmcp import Context
from fastmcp.tools import ToolResult
from fastmcp.utilities.tasks import TASKS_EXTENSION_ID
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

from clio_relay.adapters.hooks import CHOICE_META, Ask
from clio_relay.jobs.refusals import Refusal


def refusal_result(refusal: Refusal) -> ToolResult:
    """Render a refusal as an error tool result with a typed reason."""
    return ToolResult(
        content=[
            mcp_types.TextContent(type="text", text=f"{refusal.reason.value}: {refusal.message}")
        ],
        structured_content={"refusal": refusal.to_dict()},
        is_error=True,
    )


def modern(ctx: Context) -> bool:
    """True on a connection that speaks the modern protocol."""
    rc = ctx.request_context
    return rc is not None and rc.protocol_version in MODERN_PROTOCOL_VERSIONS


def tasks_opted_in(ctx: Context) -> bool:
    """True when the client opted into the tasks extension."""
    return modern(ctx) and ctx.client_extension_settings(TASKS_EXTENSION_ID) is not None


def elicitation_modes(ctx: Context) -> set[str]:
    """The elicitation modes the client declared: form, url, both or none."""
    try:
        capabilities = ctx.session.client_capabilities
    except (AttributeError, RuntimeError):
        return set()
    elicitation = getattr(capabilities, "elicitation", None) if capabilities else None
    if elicitation is None:
        return set()
    form = getattr(elicitation, "form", None)
    url = getattr(elicitation, "url", None)
    modes: set[str] = set()
    if form is not None or url is None:
        modes.add("form")
    if url is not None:
        modes.add("url")
    return modes


def ask_mode(ask: Ask) -> str:
    """The elicitation mode an ask needs."""
    return "url" if ask.kind == "url" else "form"


def elicit_request(ask: Ask) -> mcp_types.ElicitRequest:
    """Return the elicitation for one ask."""
    if ask.kind == "url":
        return mcp_types.ElicitRequest(
            params=mcp_types.ElicitRequestURLParams(
                message=ask.message, url=ask.url or "", elicitation_id=ask.ask_id
            )
        )
    return mcp_types.ElicitRequest(
        params=mcp_types.ElicitRequestFormParams(
            message=ask.message,
            requested_schema=ask.requested_schema or {"type": "object", "properties": {}},
            _meta=cast("mcp_types.RequestParamsMeta | None", ask.meta),
        )
    )


def input_required(asks: list[Ask], request_state: str) -> mcp_types.InputRequiredResult:
    """The modern input request for ``asks``, with their ``_meta`` on the result.

    The 2026-07-28 schema gives an embedded input request no ``_meta``, so each
    ask's typed data (a job card, say) rides on the result's own ``_meta``.
    """
    meta: dict[str, Any] = {}
    for ask in asks:
        for key, value in (ask.meta or {}).items():
            if key == CHOICE_META and isinstance(meta.get(key), dict) and isinstance(value, dict):
                # Keyed by ask id, so each ask's options survive beside the others'.
                meta[key] = {**meta[key], **cast(dict[str, Any], value)}
            else:
                meta[key] = value
    return mcp_types.InputRequiredResult(
        input_requests={ask.ask_id: elicit_request(ask) for ask in asks},
        request_state=request_state,
        _meta=cast("mcp_types.RequestParamsMeta | None", meta or None),
    )


async def elicit_inline(ctx: Context, ask: Ask) -> mcp_types.ElicitResult:
    """Put one ask to a legacy client as an elicitation, with the ask's ``_meta``."""
    if ask.kind == "url":
        return await ctx.session.elicit_url(
            message=ask.message, url=ask.url or "", elicitation_id=ask.ask_id
        )
    return await ctx.session.send_request(elicit_request(ask), mcp_types.ElicitResult)


def elicit_document(ask: Ask) -> dict[str, Any]:
    """The elicitation for one ask, as JSON."""
    return elicit_request(ask).model_dump(by_alias=True, mode="json", exclude_none=True)


def response_parts(response: Any) -> tuple[str, dict[str, Any] | None]:
    """Split an elicitation answer into its action and content."""
    if isinstance(response, mcp_types.ElicitResult):
        return response.action, dict(response.content) if response.content else None
    if isinstance(response, dict):
        action = str(response.get("action", "cancel"))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        content = response.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return action, dict(content) if isinstance(content, dict) else None  # pyright: ignore[reportUnknownArgumentType]
    return "cancel", None
