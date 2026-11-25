"""Tool results for refusals, and the ways relay puts an ask to the person.

A modern client gets a multi-round-trip input request, a legacy one gets an
elicitation, and a client with neither gets a typed refusal naming the ask.
Each is worded by :func:`hooks.short_ask`: the question alone for a client that
declared it reads relay's typed ask ``_meta``, the question and its detail for
any other.
"""

from __future__ import annotations

from typing import Any, cast

import mcp_types
from fastmcp import Context
from fastmcp.server.extensions import read_client_extension_settings
from fastmcp.tools import ToolResult
from fastmcp.utilities.tasks import TASKS_EXTENSION_ID
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

from clio_relay.adapters.hooks import ASK_META, CHOICE_META, READS_ASK_META, Ask, short_ask
from clio_relay.jobs.refusals import Refusal

_BY_ASK = (CHOICE_META, ASK_META)
_LOOKUP_ERRORS = (AttributeError, RuntimeError, LookupError, TypeError)


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


def _declares(settings: Any, session: Any) -> bool:
    for identifier in READS_ASK_META:
        try:
            if settings(identifier) is not None:
                return True
        except _LOOKUP_ERRORS:
            pass
    try:
        capabilities = session.client_capabilities if session is not None else None
    except _LOOKUP_ERRORS:
        return False
    for group in ("extensions", "experimental"):
        found: Any = getattr(capabilities, group, None) if capabilities else None
        if isinstance(found, dict) and any(name in found for name in READS_ASK_META):
            return True
    return False


def reads_ask_meta(ctx: Context | None) -> bool:
    """True when the client declared it reads relay's typed ask and choice ``_meta``.

    The declaration is the ``io.iowarp.clio-relay/ask`` (or ``/choice``) client
    extension: in each request's ``_meta`` at ``2026-07-28``, in the session's
    ``extensions`` or ``experimental`` capabilities at ``2025-11-25``.
    """
    if ctx is None:
        return False
    try:
        session: Any = ctx.session
    except _LOOKUP_ERRORS:
        session = None
    return _declares(ctx.client_extension_settings, session)


def request_reads_ask_meta(rc: Any) -> bool:
    """:func:`reads_ask_meta` for a low-level request context (``tasks/get``)."""
    if rc is None:
        return False

    def settings(identifier: str) -> dict[str, Any] | None:
        return read_client_extension_settings(rc, identifier)

    return _declares(settings, getattr(rc, "session", None))


def elicit_request(ask: Ask, *, reads_meta: bool) -> mcp_types.ElicitRequest:
    """Return the elicitation for one ask, worded for whether the client reads its ``_meta``."""
    ask = short_ask(ask, reads_meta=reads_meta)
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


def input_required(
    asks: list[Ask], request_state: str, *, reads_meta: bool
) -> mcp_types.InputRequiredResult:
    """The modern input request for ``asks``, with their ``_meta`` on the result.

    The 2026-07-28 schema gives an embedded input request no ``_meta``, so each
    ask's typed data (a job card, say) rides on the result's own ``_meta``.
    """
    requests: dict[str, Any] = {
        ask.ask_id: elicit_request(ask, reads_meta=reads_meta) for ask in asks
    }
    meta: dict[str, Any] = {}
    for ask in asks:
        for key, value in (ask.meta or {}).items():
            if key in _BY_ASK and isinstance(meta.get(key), dict) and isinstance(value, dict):
                # Keyed by ask id, so each ask's options survive beside the others'.
                meta[key] = {**meta[key], **cast(dict[str, Any], value)}
            else:
                meta[key] = value
    return mcp_types.InputRequiredResult(
        input_requests=requests,
        request_state=request_state,
        _meta=cast("mcp_types.RequestParamsMeta | None", meta or None),
    )


async def elicit_inline(ctx: Context, ask: Ask) -> mcp_types.ElicitResult:
    """Put one ask to a legacy client as an elicitation, with the ask's ``_meta``."""
    request = elicit_request(ask, reads_meta=reads_ask_meta(ctx))
    if isinstance(request.params, mcp_types.ElicitRequestURLParams):
        return await ctx.session.elicit_url(
            message=request.params.message, url=ask.url or "", elicitation_id=ask.ask_id
        )
    return await ctx.session.send_request(request, mcp_types.ElicitResult)


def elicit_document(ask: Ask, *, reads_meta: bool) -> dict[str, Any]:
    """The elicitation for one ask, as JSON."""
    return elicit_request(ask, reads_meta=reads_meta).model_dump(
        by_alias=True, mode="json", exclude_none=True
    )


def response_parts(response: Any) -> tuple[str, dict[str, Any] | None]:
    """Split an elicitation answer into its action and content."""
    if isinstance(response, mcp_types.ElicitResult):
        return response.action, dict(response.content) if response.content else None
    if isinstance(response, dict):
        action = str(response.get("action", "cancel"))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        content = response.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return action, dict(content) if isinstance(content, dict) else None  # pyright: ignore[reportUnknownArgumentType]
    return "cancel", None
