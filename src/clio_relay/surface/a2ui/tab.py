"""The Compute page: one resource, its app-only action tools, and how they run (RL31-RL33).

CLIO reads :data:`TAB_URI` when the person opens Compute in its left bar and
renders the A2UI it returns. Each control calls one of the page's tools with the
single-use token it carries. The tools are listed only to a client that declared
A2UI, always as app-only (``_meta.ui.visibility: ["app"]``), so a host keeps them
from the model and a client without A2UI sees relay's five verbs and nothing else
(LQ1). What each action does is in :mod:`clio_relay.surface.a2ui.actions`.

Every action goes through the same site store and ``site_added`` hook the CLI
uses, so the page can do nothing the command line cannot.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated, Any, cast

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.apps import AppConfig
from fastmcp.resources import ResourceContent, ResourceResult
from fastmcp.server.dependencies import get_context
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools import ToolResult
from pydantic import Field as PydanticField

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.identity.probe import load as load_probes
from clio_relay.identity.probe import probes_file
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui import adding, protocol, surfaces, table
from clio_relay.surface.a2ui.actions import Outcome, TabActions, as_dict, pointer
from clio_relay.surface.a2ui.forms import Form, form
from clio_relay.surface.a2ui.protocol import (
    MIME_TYPE,
    RELAY_CATALOG_ID,
    A2UIClient,
    Message,
    a2ui_client,
    embedded,
)
from clio_relay.surface.a2ui.surfaces import (
    ADD_PATH,
    ADD_SURFACE,
    CATALOG_PATH,
    SITES_SURFACE,
    SiteView,
    TabState,
)
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.describing import site_status
from clio_relay.surface.skills import skill_link

if TYPE_CHECKING:
    from clio_relay.adapters.base import BaseAdapter
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

TAB_URI = "a2ui://clio-relay/compute"
WATCH_INTERVAL_S = 2.0

_TITLES = {
    surfaces.OPEN_ADD: "Open the add-endpoint form",
    surfaces.ADD_SITE: "Add an endpoint",
    surfaces.ADD_FROM_CATALOG: "Add an endpoint from the catalog",
    surfaces.EDIT_SITE: "Edit an endpoint",
    surfaces.SAVE_SITE: "Save an endpoint",
    surfaces.CANCEL: "Cancel a form or a removal",
    surfaces.REMOVE_SITE: "Ask to remove an endpoint",
    surfaces.CONFIRM_REMOVE: "Remove an endpoint",
    surfaces.PROBE_SITE: "Probe an endpoint",
    surfaces.SWITCH_ADAPTER: "Change an endpoint's protocol",
    surfaces.SET_VALUE: "Change an endpoint's protocol or security, or a default",
    surfaces.CONNECT_SITE: "Sign in to an endpoint",
}


class ComputeTab(TabActions):
    """The page's state, rendering and actions for one relay server."""

    def __init__(self, service: RelayService, changes: ServerChanges) -> None:
        self.service = service
        self.changes = changes
        home = service.paths.state_dir / "a2ui"
        self.tokens = ActionTokens(home / "tokens")
        self.state = TabState()
        self._state_file = home / "compute-tab.json"
        self._lock = asyncio.Lock()
        self._seen: str | None = None
        self._watching: asyncio.Task[None] | None = None

    # What the tab remembers, shared by every relay process serving it

    def _load_state(self) -> None:
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = None
        self.state = TabState.from_dict(data)

    def _save_state(self) -> None:
        write_json_atomic(self._state_file, self.state.to_dict())

    # What the tab shows

    def forms(self) -> dict[str, Form]:
        """Return a form for each installed adapter, by name, marking the test-only ones."""
        found: dict[str, Form] = {}
        for name in registry.names():
            try:
                cls = registry.load(name)
                found[name] = form(name, cls.Config.model_json_schema(), listed=cls.listed)
            except Refusal:
                continue
        return found

    def _identities(self) -> Any:
        provider: Any = getattr(self.service.ask_provider, "identities", None)
        if callable(provider):
            try:
                return provider()
            except Exception:  # an ask provider without identities falls back to relay's
                logger.debug("the ask provider has no identities", exc_info=True)
        from clio_relay import identity

        return identity.shared()

    async def _identity(self, identities: Any, name: str) -> dict[str, Any]:
        provider: Any = identities.get(name) if identities is not None else None
        if provider is None:
            return {"provider": name, "title": name, "state": "signed_out"}
        try:
            status: Any = await provider.status()
        except Refusal as refusal:
            return {
                "provider": name,
                "title": name,
                "state": "signed_out",
                "detail": refusal.message,
            }
        return cast(dict[str, Any], status.model_dump(mode="json"))

    async def _reviewed(self, site: SiteEntry) -> int | None:
        counter: Any = getattr(self.service.guard, "reviewed_packages", None)
        if not callable(counter):
            return None
        try:
            found: Any = counter(site)
            if inspect.isawaitable(found):
                found = await found
            return len(cast(Sequence[Any], found))
        except Exception:  # a guard that cannot say leaves the count out
            logger.debug("the guard could not count reviewed packages", exc_info=True)
            return None

    async def _system_id(self, site: SiteEntry) -> tuple[bool, str | None]:
        try:
            adapter: BaseAdapter[Any] = self.service.adapter_for(site, "describe")
        except Refusal:
            return False, None
        finder: Any = getattr(adapter, "system_id", None)
        if not callable(finder):
            return False, None
        try:
            value: Any = finder()
            if inspect.isawaitable(value):
                value = await value
        except Exception:  # not signed in yet, so the system is not known
            return True, None
        return True, str(value) if value else None

    async def view(self, site: SiteEntry, identities: Any, probes: dict[str, Any]) -> SiteView:
        """Gather what the tab shows for one site."""
        status = await site_status(self.service, site)
        identity = await self._identity(identities, site.identity) if site.identity else None
        has_system_id, system_id = await self._system_id(site)
        provider = str(status.get("provider") or site.identity or "").lower()
        host = provider if provider and provider in self.service.paths.grant_files else None
        return SiteView(
            site=site,
            status=status,
            identity=identity,
            probe=cast(dict[str, Any] | None, probes.get(site.id)),
            skill=skill_link(site, self.service.paths.config_dir),
            reviewed=await self._reviewed(site),
            system_id=system_id,
            has_system_id=has_system_id,
            host_sign_in=host,
        )

    async def views(self) -> list[SiteView]:
        """Gather every site's view."""
        identities = self._identities()
        probes = load_probes(self.service.paths)
        found: list[SiteView] = []
        for site in self.service.sites.list():
            try:
                found.append(await self.view(site, identities, probes))
            except Exception as exc:  # one site that cannot be read must not hide the others
                logger.warning("reading site %s for the tab", site.id, exc_info=True)
                detail = f"Relay could not read this endpoint's state ({type(exc).__name__})"
                found.append(SiteView(site=site, status={"state": "unavailable", "detail": detail}))
        return found

    def _catalog(self) -> list[SiteEntry]:
        try:
            return list(catalog.sites())
        except Refusal as refusal:
            logger.warning("reading the catalog for the tab: %s", refusal.message)
            return []

    async def _surfaces(
        self, views: list[SiteView], client: A2UIClient
    ) -> dict[str, tuple[list[dict[str, Any]], dict[str, Any]]]:
        """Return both surfaces: the endpoint table for relay's catalog, cards for Basic."""
        forms = self.forms()
        build = table.endpoints_surface
        if client.catalog_id != RELAY_CATALOG_ID:
            build = surfaces.sites_surface
        return {
            SITES_SURFACE: build(views, self.state, forms, self.tokens),
            ADD_SURFACE: adding.add_surface(forms, self._catalog(), self.state, self.tokens),
        }

    async def read(self, client: A2UIClient | None) -> ResourceResult:
        """Return the whole tab: A2UI for a client that renders it, text otherwise."""
        self._watch()
        await asyncio.to_thread(self._load_state)
        views = await self.views()
        if client is None:
            return ResourceResult(
                [ResourceContent(surfaces.summary(views, self.state.notice), "text/plain")]
            )
        messages: list[Message] = []
        for surface_id, (components, data) in (await self._surfaces(views, client)).items():
            messages += protocol.create(client, surface_id, components, data)
        return ResourceResult([ResourceContent(json.dumps(messages), MIME_TYPE)])

    # Watching for changes made elsewhere

    def _snapshot(self) -> str:
        digest = hashlib.sha256()
        for path in (
            self.service.paths.sites_file,
            probes_file(self.service.paths),
            self._state_file,
        ):
            try:
                digest.update(path.read_bytes())
            except OSError:
                digest.update(b"-")
        return digest.hexdigest()

    def _watch(self) -> None:
        if self._seen is None:
            self._seen = self._snapshot()
        if self._watching is None or self._watching.done():
            self._watching = asyncio.get_running_loop().create_task(self.watch())

    async def watch(self, interval_s: float = WATCH_INTERVAL_S) -> None:
        """Refresh the tab when the CLI, a sign-in probe or another relay changes a site."""
        while True:
            await asyncio.sleep(interval_s)
            try:
                now = await asyncio.to_thread(self._snapshot)
            except Exception:  # read again next time
                continue
            if now != self._seen:
                self._seen = now
                await self.changes.resource_updated(TAB_URI)

    # Actions

    async def act(
        self,
        ctx: Context | None,
        action: str,
        token: str,
        values: Any,
        choice: dict[str, Any] | None = None,
    ) -> ToolResult:
        """Run one action the page rendered, if ``token`` allows it.

        ``choice`` is the column and value a table row's picker sent, if any.
        """
        grant = self.tokens.spend(token, action) if token else None
        if grant is None:
            # The process that rendered the tab may not be this one: changing the
            # shared state is what makes every reader refresh it.
            async with self._lock:
                await asyncio.to_thread(self._load_state)
                self.state.notice = "That button was out of date, so the page has been refreshed."
                await asyncio.to_thread(self._save_state)
                # Announced here, so the watcher must not announce the same write again.
                self._seen = await asyncio.to_thread(self._snapshot)
            await self.changes.resource_updated(TAB_URI)
            refusal = Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                "this button is out of date or was already used; the page has been refreshed",
                action=action,
            )
            return ToolResult(
                content=[
                    mcp_types.TextContent(
                        type="text", text=f"{refusal.reason.value}: {refusal.message}"
                    )
                ],
                structured_content={"refusal": refusal.to_dict()},
                is_error=True,
            )
        handler = self._handlers()[action]
        async with self._lock:
            await asyncio.to_thread(self._load_state)
            self.state.notice = ""
            try:
                outcome = await handler(grant.subject, as_dict(values) | (choice or {}))
            except Exception as exc:  # the tab says what failed rather than going blank
                logger.exception("the Compute page's %s action failed", action)
                self.state.notice = f"That did not work: relay hit {type(exc).__name__}."
                outcome = Outcome(ok=False)
            await asyncio.to_thread(self._save_state)
            self._seen = await asyncio.to_thread(self._snapshot)
            views = await self.views()
            client = a2ui_client(ctx)
            text = surfaces.summary(views, self.state.notice)
            content: list[Any] = [mcp_types.TextContent(type="text", text=text)]
            if client is not None:
                messages: list[Message] = []
                for surface_id, (components, data) in (await self._surfaces(views, client)).items():
                    messages += protocol.update(client, surface_id, components, None)
                    for path in self._resend(surface_id, outcome, data):
                        messages.append(
                            {
                                "version": client.version,
                                "updateDataModel": {
                                    "surfaceId": surface_id,
                                    "path": path,
                                    "value": pointer(data, path),
                                },
                            }
                        )
                content.append(embedded(TAB_URI, messages))
        return ToolResult(
            content=content, structured_content={"ok": outcome.ok, "notice": self.state.notice}
        )


class _HideFromPlainClients(Middleware):
    """Lists the tab and its tools only to a client that declared A2UI (LQ1)."""

    async def on_list_tools(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        tools: Sequence[Any] = await call_next(context)
        if a2ui_client(context.fastmcp_context) is not None:
            return tools
        return [tool for tool in tools if getattr(tool, "name", None) not in surfaces.ACTIONS]

    async def on_list_resources(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        resources: Sequence[Any] = await call_next(context)
        if a2ui_client(context.fastmcp_context) is not None:
            return resources
        return [item for item in resources if str(getattr(item, "uri", "")) != TAB_URI]


def _action_tool(server: FastMCP[Any], tab: ComputeTab, action: str) -> None:
    async def act(
        ctx: Context,
        token: Annotated[
            str, PydanticField(description="The single-use token the tab's button carries")
        ] = "",
        values: Annotated[
            dict[str, Any] | None, PydanticField(description="The form the button submits")
        ] = None,
        row: Annotated[str, PydanticField(description="The table row the action came from")] = "",
        column: Annotated[str, PydanticField(description="The table column picked in")] = "",
        value: Annotated[Any, PydanticField(description="The value picked")] = None,
    ) -> ToolResult:
        # A table row adds row, column and value; the token alone names the endpoint.
        choice = {"column": column, "value": value} if column else None
        del row
        return await tab.act(ctx, action, token, values, choice)

    server.tool(
        name=action,
        title=_TITLES[action],
        description=f"{_TITLES[action]} from the Compute page. Only the page calls this.",
        app=AppConfig(visibility=["app"]),
        annotations=mcp_types.ToolAnnotations(open_world_hint=False),
    )(act)


def install(server: FastMCP[Any], service: RelayService, changes: ServerChanges) -> ComputeTab:
    """Add the Compute page's resource, its app-only tools and their listing rule to ``server``."""
    tab = ComputeTab(service, changes)

    @server.resource(
        TAB_URI,
        name="compute-tab",
        title="Compute",
        description="Relay's endpoints, for CLIO's Compute page",
        mime_type=MIME_TYPE,
        annotations=mcp_types.Annotations(audience=["user"]),
    )
    async def compute_tab() -> ResourceResult:  # pyright: ignore[reportUnusedFunction]
        return await tab.read(a2ui_client(get_context()))

    for action in surfaces.ACTIONS:
        _action_tool(server, tab, action)
    server.add_middleware(_HideFromPlainClients())
    return tab
