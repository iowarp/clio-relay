"""Tools that end things: ``cancel`` (a job or a service) and ``site_clean``.

Each has a tool of its own, so a host's question names what the call does
instead of the tool that started the work. ``site_clean`` is
``clio-relay site clean`` over MCP: it keeps the Spack store, as the command
line does by default, and asks the person first on every site, in the call
when the client shows a form, else as a ``needs_approval`` refusal to retry
after the answer. The yes is bound to the site and to what the clean removes,
and is used once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.tools import InputRequiredToolResult, ToolResult
from pydantic import Field

from clio_relay import site_clean as cleaning
from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import canonical_digest
from clio_relay.surface import exec_plan
from clio_relay.surface.elicit import (
    elicit_request,
    elicitation_modes,
    modern,
    refusal_result,
    response_parts,
)
from clio_relay.surface.files_approval import NeedsPerson
from clio_relay.surface.results import CHANGE, record_result, text

if TYPE_CHECKING:
    from clio_relay.surface.server import RelayServer
    from clio_relay.surface.service import RelayService

CLEAN_KIND = "site_clean"


async def _approve(service: RelayService, planned: dict[str, Any]) -> Ask:
    """Return the accepted ask for this clean, asking the person when there is none."""
    binding = {
        "kind": CLEAN_KIND,
        "site": planned["site"],
        "plan_sha256": canonical_digest(planned),
    }
    digest = canonical_digest(binding)
    found = [
        ask
        for ask in service.asks.all()
        if ask.binding_digest == digest and ask.completed_at is None
    ]
    if found:
        ask = found[0]
    else:
        ask = await service.ask_provider.approve(
            site=service.site(str(planned["site"])),
            operation_id=None,
            message=cleaning.describe_plan(planned),
            binding=binding,
        )
        if service.asks.get(ask.ask_id) is None:
            service.asks.put(ask.ask_id, ask)
    if ask.state == "pending":
        raise NeedsPerson(ask)
    if ask.state != "accepted":
        exec_plan.used(service, ask)
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"cleaning {planned['site']} was not approved",
            site=planned["site"],
            ask_id=ask.ask_id,
        )
    return ask


async def _clean(service: RelayService, site: str) -> dict[str, Any]:
    planned = await cleaning.plan(service, site)
    ask = await _approve(service, planned)
    try:
        return await cleaning.clean(service, planned)
    finally:
        exec_plan.used(service, ask)


def _answer(service: RelayService, site: str, ask_id: str, responses: dict[str, Any]) -> None:
    """Record the answer a retried call carries, for this site's clean asks only."""
    ask = service.asks.get(ask_id)
    if ask is None or ask_id not in responses or ask.site != site:
        return
    if ask.binding.get("kind") != CLEAN_KIND:
        return
    action, content = response_parts(responses[ask_id])
    service.answer(ask_id, action, content, by="input_required")


def _needs_person(service: RelayService, ask: Ask) -> ToolResult:
    refusal = Refusal(
        RefusalReason.NEEDS_APPROVAL,
        "relay asks the person before it cleans a site. Open the page, or run "
        "`clio-relay asks answer`, then call site_clean again with the same site",
        ask_id=ask.ask_id,
        asks=[
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "message": ask.message,
                "page": service.ask_provider.page_address(ask),
            }
        ],
    )
    return refusal_result(refusal)


async def site_clean_tool(
    service: RelayService, ctx: Context, site: str
) -> ToolResult | InputRequiredToolResult:
    """Clean ``site`` once the person says yes."""
    try:
        if ctx.request_state and ctx.input_responses is not None:
            _answer(service, site, ctx.request_state, ctx.input_responses)
        try:
            cleaned = await _clean(service, site)
        except NeedsPerson as waiting:
            ask = waiting.ask
            if "form" not in elicitation_modes(ctx):
                return _needs_person(service, ask)
            if modern(ctx):
                return InputRequiredToolResult(
                    mcp_types.InputRequiredResult(
                        input_requests={ask.ask_id: elicit_request(ask)},
                        request_state=ask.ask_id,
                    )
                )
            response = await ctx.session.elicit_form(
                message=ask.message,
                requested_schema=ask.requested_schema or {"type": "object", "properties": {}},  # pyright: ignore[reportArgumentType]
            )
            action, content = response_parts(response)
            service.answer(ask.ask_id, action, content, by="elicitation")
            try:
                cleaned = await _clean(service, site)
            except NeedsPerson as still:
                return _needs_person(service, still.ask)
    except Refusal as refusal:
        return refusal_result(refusal)
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=text(cleaned))],
        structured_content=cleaned,
    )


def install(server: FastMCP[Any], relay: RelayServer) -> None:
    """Add ``cancel`` and ``site_clean`` to ``server``."""

    @server.tool(name="cancel", title="Cancel a job or stop a service", annotations=CHANGE)
    async def cancel(  # pyright: ignore[reportUnusedFunction]
        operation: Annotated[
            str, Field(description="The operation id run or serve returned, or its task id")
        ],
    ) -> ToolResult:
        """Cancel a job from run, or stop a service from serve, and return the
        operation as it ends. The same as run(cancel=...) and serve(stop=...)."""
        try:
            record = await relay.service.cancel(operation)
        except Refusal as refusal:
            return refusal_result(refusal)
        return record_result(relay.service, record)

    @server.tool(
        name="site_clean",
        title="Remove relay's deployment from a site",
        annotations=CHANGE,
    )
    async def site_clean(  # pyright: ignore[reportUnusedFunction]
        site: Annotated[str, Field(description="The site id")],
        ctx: Context,
    ) -> ToolResult | InputRequiredToolResult:
        """Remove what relay deployed on a site, so the next use deploys it again:
        relay stops its processes there, then deletes its worker, scripts, job
        directories and JARVIS bootstrap with JARVIS's site record, and drops its
        local state for the site. The Spack store stays. It refuses while a job of
        the site runs, cancels operations that wait on a person with no job yet,
        and asks the person first, naming what goes and what stays. The site stays
        registered."""
        return await site_clean_tool(relay.service, ctx, site)
