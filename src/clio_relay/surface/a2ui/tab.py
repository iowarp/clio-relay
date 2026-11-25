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
import threading
from collections.abc import Callable, Sequence
from dataclasses import replace
from functools import partial
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
from clio_relay.identity.store import keyring_usable
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui import adding, cards, i18n, protocol, surfaces, table
from clio_relay.surface.a2ui.actions import Outcome, TabActions
from clio_relay.surface.a2ui.forms import Form, form
from clio_relay.surface.a2ui.protocol import (
    MIME_TYPE,
    RELAY_CATALOG_ID,
    A2UIClient,
    Message,
    a2ui_client,
    embedded,
    request_locale,
)
from clio_relay.surface.a2ui.surfaces import (
    ADD_SURFACE,
    SITES_SURFACE,
    SiteView,
    TabState,
)
from clio_relay.surface.a2ui.surfaces import defaults_state as _defaults_state
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.a2ui.tool_meta import ANNOTATIONS, TITLES
from clio_relay.surface.a2ui.values import as_dict, pointer
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.describing import site_status
from clio_relay.surface.skills import skill_link

if TYPE_CHECKING:
    from clio_relay.adapters.base import BaseAdapter
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

TAB_URI = "a2ui://clio-relay/compute"
WATCH_INTERVAL_S = 2.0
READ_BUDGET_S = 0.25
"""How long a read waits for the sites' live state before showing what it last knew."""

CHECKING = {"state": "checking"}
"""The status of a site relay has never read and is reading now."""

# The parts of a site's view that are not the site entry or its last test.
_KNOWN_FIELDS = (
    "status",
    "identity",
    "skill",
    "reviewed",
    "system_id",
    "has_system_id",
    "host_sign_in",
)


TOAST_META = "io.iowarp/toast"
"""The result ``_meta`` key of the toast a host shows for an action: message, tone, undo."""


def _fingerprint(site: SiteEntry) -> str:
    """Return what identifies the entry a site's view was read from."""
    return hashlib.sha256(site.model_dump_json().encode()).hexdigest()[:16]


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
        self._views_file = home / "compute-views.json"
        self._forms: tuple[tuple[str, ...], dict[str, Form]] | None = None
        # Each site's last read view, by id, with the fingerprint of the entry it read.
        self._known: dict[str, tuple[str, SiteView]] | None = None
        self._reading: dict[str, tuple[str, asyncio.Task[SiteView]]] = {}
        self._shown: dict[str, SiteView] = {}
        self._announcing: asyncio.Task[None] | None = None
        self._identity_locks: dict[str, asyncio.Lock] = {}

    # What the tab remembers, shared by every relay process serving it

    def _load_state(self) -> None:
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = None
        self.state = TabState.from_dict(data)
        try:
            defaults = self.service.sites.defaults()
        except Refusal:  # an unreadable defaults file reads as relay's own defaults
            return
        self.state.defaults = _defaults_state(defaults)

    def _save_state(self) -> None:
        write_json_atomic(self._state_file, self.state.to_dict())

    # What the tab shows

    def forms(self) -> dict[str, Form]:
        """Return a form for each installed adapter, by name, marking the test-only ones.

        The forms are built once for the adapters installed: building one imports its
        adapter and walks its settings schema.
        """
        names = tuple(registry.names())
        if self._forms is not None and self._forms[0] == names:
            return self._forms[1]
        found: dict[str, Form] = {}
        for name in names:
            try:
                cls = registry.load(name)
                found[name] = form(name, cls.Config.model_json_schema(), listed=cls.listed)
            except Refusal:
                continue
        self._forms = (names, found)
        return found

    def warm(self) -> None:
        """Do the first read's one-time work ahead of it: adapter imports, forms and keyring."""
        steps: list[Callable[[], object]] = [self.forms, keyring_usable]
        try:
            sites = self.service.sites.list()
        except Refusal:
            sites = []
        for site in sites:
            steps.append(partial(self.service.site_routes, site))
        for step in steps:
            try:
                step()
            except Exception:  # the read does the same work and reports what fails
                logger.debug("warming the Compute page", exc_info=True)

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
        found = await self._read_site(site, identities)
        return replace(found, probe=cast(dict[str, Any] | None, probes.get(site.id)))

    async def _read_site(self, site: SiteEntry, identities: Any) -> SiteView:
        # Sites that share a sign-in read it in turn: two at once could each start a
        # device code, or spend the same refresh token.
        lock = self._identity_locks.setdefault(site.identity or f"site:{site.id}", asyncio.Lock())
        async with lock:
            try:
                status = await site_status(self.service, site)
                identity = (
                    await self._identity(identities, site.identity) if site.identity else None
                )
                has_system_id, system_id = await self._system_id(site)
                reviewed = await self._reviewed(site)
            except Exception as exc:  # one site that cannot be read must not hide the others
                logger.warning("reading site %s for the tab", site.id, exc_info=True)
                detail = f"Relay could not read this endpoint's state ({type(exc).__name__})"
                return SiteView(site=site, status={"state": "unavailable", "detail": detail})
        provider = str(status.get("provider") or site.identity or "").lower()
        host = provider if provider and provider in self.service.paths.grant_files else None
        return SiteView(
            site=site,
            status=status,
            identity=identity,
            skill=skill_link(site, self.service.paths.config_dir),
            reviewed=reviewed,
            system_id=system_id,
            has_system_id=has_system_id,
            host_sign_in=host,
        )

    def _start(self, site: SiteEntry, identities: Any, *, again: bool) -> asyncio.Task[SiteView]:
        """Return a read of ``site``: the one in flight for this entry, unless ``again``."""
        key = _fingerprint(site)
        running = self._reading.get(site.id)
        if not again and running is not None and running[0] == key and not running[1].done():
            return running[1]
        task = asyncio.get_running_loop().create_task(self._read_site(site, identities))
        self._reading[site.id] = (key, task)
        task.add_done_callback(lambda done: self._learn(site.id, key, done))
        return task

    def _learn(self, site_id: str, key: str, done: asyncio.Task[SiteView]) -> None:
        running = self._reading.get(site_id)
        if running is None or running[1] is not done:  # a later read replaced this one
            return
        if done.cancelled() or done.exception() is not None:
            return
        self._known_views()[site_id] = (key, done.result())

    def _known_views(self) -> dict[str, tuple[str, SiteView]]:
        if self._known is None:
            self._known = {}
            try:
                data: Any = json.loads(self._views_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                data = {}
            stored = cast(dict[str, Any], data) if isinstance(data, dict) else {}
            for site in self.service.sites.list():
                entry: Any = stored.get(site.id)
                if not isinstance(entry, dict):
                    continue
                item = cast(dict[str, Any], entry)
                status: Any = item.get("status")
                if not isinstance(status, dict):
                    continue
                view = SiteView(
                    site=site,
                    status=cast(dict[str, Any], status),
                    identity=item.get("identity"),
                    skill=item.get("skill"),
                    reviewed=item.get("reviewed"),
                    system_id=item.get("system_id"),
                    has_system_id=bool(item.get("has_system_id")),
                    host_sign_in=item.get("host_sign_in"),
                )
                self._known[site.id] = (str(item.get("fingerprint", "")), view)
        return self._known

    def _store_known(self) -> None:
        known = self._known_views()
        data = {
            site_id: {"fingerprint": key, **{name: getattr(view, name) for name in _KNOWN_FIELDS}}
            for site_id, (key, view) in known.items()
        }
        try:
            write_json_atomic(self._views_file, data)
        except (OSError, TypeError, ValueError):  # the next process reads the sites again
            logger.debug("could not store the tab's site views", exc_info=True)

    async def views(self, budget_s: float | None = None) -> list[SiteView]:
        """Gather every site's view, reading the sites at once.

        With a ``budget_s`` a site still being read after it shows what relay last
        knew about it, and the page is announced again when the read changes that.
        Without one, every site is read anew and waited for.
        """
        identities = self._identities()
        probes = load_probes(self.service.paths)
        sites = self.service.sites.list()
        if self._known is None:
            await asyncio.to_thread(self._known_views)
        tasks = {site.id: self._start(site, identities, again=budget_s is None) for site in sites}
        if not tasks:  # asyncio.wait refuses an empty set: no sites, nothing to read
            pass
        elif budget_s is None:
            await asyncio.wait(tasks.values())
        else:
            await asyncio.wait(tasks.values(), timeout=budget_s)
        known = self._known_views()
        found: list[SiteView] = []
        late: list[asyncio.Task[SiteView]] = []
        for site in sites:
            task = tasks[site.id]
            if task.done():
                view = task.result()
            else:
                late.append(task)
                last = known.get(site.id)
                view = replace(last[1], site=site) if last is not None else None
                if view is None or last is None or last[0] != _fingerprint(site):
                    view = SiteView(site=site, status=dict(CHECKING))
            found.append(replace(view, probe=cast(dict[str, Any] | None, probes.get(site.id))))
        self._shown = {view.site.id: view for view in found}
        if late:
            if self._announcing is None or self._announcing.done():
                self._announcing = asyncio.get_running_loop().create_task(self._announce(late))
        else:
            await asyncio.to_thread(self._store_known)
        return found

    async def _announce(self, late: list[asyncio.Task[SiteView]]) -> None:
        """Announce the page once the sites still being read differ from what it showed."""
        await asyncio.wait(late)
        await asyncio.to_thread(self._store_known)
        changed = False
        for task in late:
            if task.cancelled() or task.exception() is not None:
                continue
            fresh = task.result()
            shown = self._shown.get(fresh.site.id)
            if shown is None or replace(shown, probe=None) != fresh:
                changed = True
        if changed:
            await self.changes.resource_updated(TAB_URI)

    def _catalog(self) -> list[SiteEntry]:
        try:
            return list(catalog.sites())
        except Refusal as refusal:
            logger.warning("reading the catalog for the tab: %s", refusal.message)
            return []

    async def _surfaces(
        self, views: list[SiteView], client: A2UIClient
    ) -> dict[str, tuple[list[dict[str, Any]], dict[str, Any]]]:
        """Return the surfaces: the page and its dialogs for relay's catalog, cards for Basic."""
        forms = self.forms()
        if client.catalog_id == RELAY_CATALOG_ID:
            inputs = await asyncio.to_thread(self.dialog_inputs)
            page = table.endpoints_surface(views, self.state, forms, self.tokens, inputs)
            return {SITES_SURFACE: page}
        return {
            SITES_SURFACE: cards.sites_surface(views, self.state, forms, self.tokens),
            ADD_SURFACE: adding.add_surface(forms, self._catalog(), self.state, self.tokens),
        }

    async def read(self, client: A2UIClient | None, ctx: Context | None = None) -> ResourceResult:
        """Return the whole tab: A2UI for a client that renders it, text otherwise."""
        spoken = i18n.use(request_locale(ctx))
        try:
            return await self._read(client)
        finally:
            i18n.reset(spoken)

    async def _read(self, client: A2UIClient | None) -> ResourceResult:
        self._watch()
        await asyncio.to_thread(self._load_state)
        views = await self.views(READ_BUDGET_S)
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
            self.service.sites.defaults_path,
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
        spoken = i18n.use(request_locale(ctx))
        try:
            return await self._act(ctx, action, token, values, choice)
        finally:
            i18n.reset(spoken)

    async def _act(
        self,
        ctx: Context | None,
        action: str,
        token: str,
        values: Any,
        choice: dict[str, Any] | None = None,
    ) -> ToolResult:
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
            toast = outcome.toast
            if toast is None and self.state.notice:
                tone = "positive" if outcome.ok else "negative"
                toast = {"message": self.state.notice.rstrip("."), "tone": tone}
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
            content=content,
            structured_content={"ok": outcome.ok, "notice": self.state.notice},
            meta={TOAST_META: toast} if toast else None,
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
        title=TITLES[action],
        description=f"{TITLES[action]} from the Compute page. Only the page calls this.",
        app=AppConfig(visibility=["app"]),
        annotations=ANNOTATIONS[action],
    )(act)


def install(server: FastMCP[Any], service: RelayService, changes: ServerChanges) -> ComputeTab:
    """Add the Compute page's resource, its app-only tools and their listing rule to ``server``."""
    tab = ComputeTab(service, changes)
    threading.Thread(target=tab.warm, name="compute-tab-warm", daemon=True).start()

    @server.resource(
        TAB_URI,
        name="compute-tab",
        title="Compute",
        description="Relay's endpoints, for CLIO's Compute page",
        mime_type=MIME_TYPE,
        annotations=mcp_types.Annotations(audience=["user"]),
    )
    async def compute_tab() -> ResourceResult:  # pyright: ignore[reportUnusedFunction]
        ctx = get_context()
        return await tab.read(a2ui_client(ctx), ctx)

    for action in surfaces.ACTIONS:
        _action_tool(server, tab, action)
    server.add_middleware(_HideFromPlainClients())
    return tab
