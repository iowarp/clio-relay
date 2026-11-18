"""The Compute tab: one resource, its app-only action tools, and what they do (RL31-RL33).

CLIO reads :data:`TAB_URI` when the person opens the tab and renders the A2UI it
returns. Each button calls one of the tab's tools with the single-use token the
button carries. The tools are listed only to a client that declared A2UI, always
as app-only (``_meta.ui.visibility: ["app"]``), so a host keeps them from the
model and a client without A2UI sees relay's five verbs and nothing else (LQ1).

Every action goes through the same site store and ``site_added`` hook the CLI
uses, so the tab can do nothing the command line cannot.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Any, cast

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.apps import AppConfig
from fastmcp.resources import ResourceContent, ResourceResult
from fastmcp.server.dependencies import get_context
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools import ToolResult
from pydantic import Field as PydanticField
from pydantic import ValidationError

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.identity.probe import load as load_probes
from clio_relay.identity.probe import probe_site, probes_file
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui import surfaces
from clio_relay.surface.a2ui.forms import Field, Form, FormError, form, parse, parse_value
from clio_relay.surface.a2ui.protocol import MIME_TYPE, A2UIClient, Message, a2ui_client, embedded
from clio_relay.surface.a2ui.surfaces import (
    ADD_FIELDS,
    ADD_SURFACE,
    EDIT_FIELDS,
    FORM_ERROR,
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
    surfaces.ADD_SITE: "Add a site",
    surfaces.ADD_FROM_CATALOG: "Add a site from the catalog",
    surfaces.EDIT_SITE: "Edit a site",
    surfaces.SAVE_SITE: "Save a site",
    surfaces.CANCEL: "Cancel an edit or a removal",
    surfaces.REMOVE_SITE: "Ask to remove a site",
    surfaces.CONFIRM_REMOVE: "Remove a site",
    surfaces.PROBE_SITE: "Probe a site",
    surfaces.SWITCH_ADAPTER: "Switch a site's adapter",
    surfaces.CONNECT_SITE: "Sign in to a site",
}


@dataclass
class Outcome:
    """What one action did, and which data-model paths the tab sends again, by surface."""

    ok: bool
    resend: dict[str, tuple[str, ...]] = field(default_factory=dict[str, tuple[str, ...]])


def pointer(data: dict[str, Any], path: str) -> Any:
    """Return the value at a JSON Pointer in ``data``, or None."""
    current: Any = data
    for part in path.strip("/").split("/"):
        if not isinstance(current, dict):
            return None
        current = cast(dict[str, Any], current).get(part.replace("~1", "/").replace("~0", "~"))
    return current


def _rendered_keys(fields: Sequence[Field], prefix: str) -> set[str]:
    return {prefix + item.key for item in fields}


def _located(errors: object, prefix: str, known: set[str]) -> dict[str, str]:
    """Return validation errors by the form field they belong to."""
    found: dict[str, str] = {}
    for raw in cast(list[Any], errors) if isinstance(errors, list) else []:
        if not isinstance(raw, dict):
            continue
        item = cast(dict[str, Any], raw)
        loc = [str(part) for part in cast(list[Any], item.get("loc") or [])]
        message = str(item.get("msg") or "Not valid")
        key = f"{prefix}{loc[0]}" if loc else FORM_ERROR
        if key not in known:
            key, message = FORM_ERROR, f"{' '.join(loc)} {message}".strip()
        found.setdefault(key, message)
    return found


def spend_ask(raw: Any) -> str | float:
    """Return the spend gate's setting from what the person typed."""
    text = str(raw or "").strip().lower() or "always"
    if text in ("always", "never"):
        return text
    try:
        value = float(text)
    except ValueError:
        raise FormError("spend", "Enter always, never, or a number of node-hours") from None
    if value <= 0:
        raise FormError("spend", "Enter a number of node-hours above 0")
    return value


def _common(
    fields: Sequence[Field], values: dict[str, Any], errors: dict[str, str]
) -> dict[str, Any]:
    """Parse the site fields of a form. Errors go to ``errors`` under ``site.<key>``."""
    parsed: dict[str, Any] = {}
    for item in fields:
        try:
            if item.key == "spend":
                parsed[item.key] = spend_ask(values.get(item.key))
            else:
                parsed[item.key] = parse_value(item, values.get(item.key))
        except FormError as exc:
            errors[f"site.{item.key}"] = str(exc)
    return parsed


def _config(
    adapter: str, values: dict[str, Any], errors: dict[str, str], prefix: str
) -> dict[str, Any] | None:
    """Parse and validate one adapter's settings, or return None with ``errors`` filled."""
    try:
        fields = form(adapter, registry.config_schema(adapter)).fields
    except Refusal as refusal:
        errors[FORM_ERROR] = refusal.message
        return None
    try:
        config = parse(fields, values)
    except FormError as exc:
        errors[prefix + exc.key] = str(exc)
        return None
    try:
        registry.validate_config(adapter, config)
    except Refusal as refusal:
        errors.update(_located(refusal.data.get("errors"), prefix, _rendered_keys(fields, prefix)))
        if not errors:
            errors[FORM_ERROR] = refusal.message
        return None
    return config


def _first(value: Any) -> str:
    if isinstance(value, list):
        items = cast(list[Any], value)
        return str(items[0]) if items else ""
    return "" if value is None else str(value)


def _dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


class ComputeTab:
    """The tab's state, rendering and actions for one relay server."""

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
        """Return a form for each installed adapter, by name."""
        found: dict[str, Form] = {}
        for name in registry.names():
            try:
                found[name] = form(name, registry.config_schema(name))
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
        return [await self.view(site, identities, probes) for site in self.service.sites.list()]

    def _catalog(self) -> list[SiteEntry]:
        try:
            return list(catalog.sites())
        except Refusal as refusal:
            logger.warning("reading the catalog for the tab: %s", refusal.message)
            return []

    async def _surfaces(
        self, views: list[SiteView]
    ) -> dict[str, tuple[list[dict[str, Any]], dict[str, Any]]]:
        forms = self.forms()
        return {
            SITES_SURFACE: surfaces.sites_surface(views, self.state, forms, self.tokens),
            ADD_SURFACE: surfaces.add_surface(forms, self._catalog(), self.state, self.tokens),
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
        for surface_id, (components, data) in (await self._surfaces(views)).items():
            messages += surfaces.create(client, surface_id, components, data)
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

    async def act(self, ctx: Context | None, action: str, token: str, values: Any) -> ToolResult:
        """Run one action the tab rendered, if ``token`` allows it."""
        grant = self.tokens.spend(token, action) if token else None
        if grant is None:
            # The process that rendered the tab may not be this one: changing the
            # shared state is what makes every reader refresh it.
            async with self._lock:
                await asyncio.to_thread(self._load_state)
                self.state.notice = "That button was out of date, so the tab has been refreshed."
                await asyncio.to_thread(self._save_state)
            await self.changes.resource_updated(TAB_URI)
            refusal = Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                "this button is out of date or was already used; the tab has been refreshed",
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
            outcome = await handler(grant.subject, _dict(values))
            await asyncio.to_thread(self._save_state)
            self._seen = await asyncio.to_thread(self._snapshot)
            views = await self.views()
            client = a2ui_client(ctx)
            text = surfaces.summary(views, self.state.notice)
            content: list[Any] = [mcp_types.TextContent(type="text", text=text)]
            if client is not None:
                messages: list[Message] = []
                for surface_id, (components, data) in (await self._surfaces(views)).items():
                    messages += surfaces.update(client, surface_id, components, None)
                    for path in outcome.resend.get(surface_id, ()):
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

    def _handlers(self) -> dict[str, Callable[[str, dict[str, Any]], Awaitable[Outcome]]]:
        return {
            surfaces.ADD_SITE: self.add_site,
            surfaces.ADD_FROM_CATALOG: self.add_from_catalog,
            surfaces.EDIT_SITE: self.edit_site,
            surfaces.SAVE_SITE: self.save_site,
            surfaces.CANCEL: self.cancel,
            surfaces.REMOVE_SITE: self.remove_site,
            surfaces.CONFIRM_REMOVE: self.confirm_remove,
            surfaces.PROBE_SITE: self.probe_site,
            surfaces.SWITCH_ADAPTER: self.switch_adapter,
            surfaces.CONNECT_SITE: self.connect_site,
        }

    def _fail(self, key: str, errors: dict[str, str]) -> Outcome:
        self.state.errors[key] = errors
        return Outcome(ok=False)

    def _site(self, site_id: str) -> SiteEntry | None:
        try:
            return self.service.sites.get(site_id)
        except Refusal as refusal:
            self.state.forget(site_id)
            self.state.notice = refusal.message.capitalize() + "."
            return None

    async def _saved(self, site: SiteEntry, done: str) -> None:
        """Run the adapters' set-up for a saved site, as ``relay site add`` does."""
        try:
            results = await self.service.site_added(site.id)
        except Refusal as refusal:
            self.state.notice = f"{done} Setting it up failed: {refusal.message}."
            return
        failed = [
            f"{name} could not set it up ({_dict(_dict(result).get('refusal')).get('message', '')})"
            for name, result in results.items()
            if "refusal" in _dict(result)
        ]
        self.state.notice = " ".join([done, *(item + "." for item in failed)])

    async def add_site(self, adapter: str, values: dict[str, Any]) -> Outcome:
        errors: dict[str, str] = {}
        common = _common(ADD_FIELDS, _dict(values.get("site")), errors)
        if not common.get("id") and "site.id" not in errors:
            errors["site.id"] = "Enter a site ID"
        config = _config(
            adapter, _dict(_dict(values.get("config")).get(adapter)), errors, f"config.{adapter}."
        )
        if errors or config is None:
            return self._fail(ADD_SURFACE, errors)
        known = _rendered_keys(ADD_FIELDS, "site.")
        try:
            entry = SiteEntry.model_validate(
                {
                    "id": common["id"],
                    "name": common.get("name"),
                    "adapter": adapter,
                    "config": config,
                    "policy": common.get("policy") or "guarded",
                    "spend": {"ask": common.get("spend", "always")},
                    "skill": common.get("skill"),
                }
            )
            added = self.service.sites.add(entry)
        except ValidationError as exc:
            return self._fail(ADD_SURFACE, _located(exc.errors(include_url=False), "site.", known))
        except Refusal as refusal:
            if refusal.reason is RefusalReason.SITE_EXISTS:
                return self._fail(ADD_SURFACE, {"site.id": f"A site named {common['id']} exists"})
            return self._fail(ADD_SURFACE, {FORM_ERROR: refusal.message})
        self.state.errors.pop(ADD_SURFACE, None)
        await self._saved(added, f"Added {added.title}.")
        return Outcome(ok=True, resend={ADD_SURFACE: ("/drafts/add",), SITES_SURFACE: ("/switch",)})

    async def add_from_catalog(self, _subject: str, values: dict[str, Any]) -> Outcome:
        catalog_id = _first(values.get("id"))
        site_id = str(values.get("site_id") or "").strip() or None
        if not catalog_id:
            return self._fail(ADD_SURFACE, {"catalog": "Choose a site from the catalog"})
        try:
            added = self.service.sites.add_from_catalog(catalog.site(catalog_id), site_id)
        except ValidationError:
            return self._fail(
                ADD_SURFACE, {"catalog": "A site ID takes lowercase letters, digits, - and _"}
            )
        except Refusal as refusal:
            return self._fail(ADD_SURFACE, {"catalog": refusal.message.capitalize()})
        self.state.errors.pop(ADD_SURFACE, None)
        await self._saved(added, f"Added {added.title}.")
        return Outcome(ok=True, resend={ADD_SURFACE: ("/catalog",), SITES_SURFACE: ("/switch",)})

    async def edit_site(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        if self._site(site_id) is None:
            return Outcome(ok=False)
        self.state.editing.add(site_id)
        self.state.confirming.discard(site_id)
        self.state.errors.pop(site_id, None)
        return Outcome(ok=True, resend={SITES_SURFACE: (f"/drafts/{site_id}",)})

    async def save_site(self, site_id: str, values: dict[str, Any]) -> Outcome:
        site = self._site(site_id)
        if site is None:
            return Outcome(ok=False)
        errors: dict[str, str] = {}
        common = _common(EDIT_FIELDS, _dict(values.get("site")), errors)
        drafts = _dict(values.get("adapters"))
        config = site.config
        if site.adapter in drafts:
            prefix = f"adapters.{site.adapter}."
            config = _config(site.adapter, _dict(drafts[site.adapter]), errors, prefix)
        alternatives: dict[str, dict[str, Any]] = {}
        order = [*site.alternatives, *(name for name in drafts if name not in site.alternatives)]
        for name in order:
            if name == site.adapter:
                continue
            draft = drafts.get(name)
            if draft is None:
                if name in site.alternatives:
                    alternatives[name] = site.alternatives[name]
                continue
            if not _dict(draft).get("use"):
                continue
            parsed = _config(name, _dict(draft), errors, f"adapters.{name}.")
            if parsed is not None:
                alternatives[name] = parsed
        if errors or config is None:
            return self._fail(site_id, errors)
        defaults = site.defaults.model_dump(mode="json") | {
            "account": common.get("account"),
            "queue": common.get("queue"),
        }
        changes = {
            "name": common.get("name"),
            "address": common.get("address"),
            "policy": common.get("policy") or site.policy,
            "spend": {"ask": common.get("spend", site.spend.ask)},
            "skill": common.get("skill"),
            "defaults": defaults,
            "config": config,
            "alternatives": alternatives,
        }
        try:
            saved = self.service.sites.edit(site_id, changes)
        except Refusal as refusal:
            known = _rendered_keys(EDIT_FIELDS, "site.")
            located = _located(refusal.data.get("errors"), "site.", known)
            return self._fail(site_id, located or {FORM_ERROR: refusal.message})
        self.state.editing.discard(site_id)
        self.state.errors.pop(site_id, None)
        await self._saved(saved, f"Saved {saved.title}.")
        return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})

    async def cancel(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        self.state.editing.discard(site_id)
        self.state.confirming.discard(site_id)
        self.state.errors.pop(site_id, None)
        return Outcome(ok=True)

    async def remove_site(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        if self._site(site_id) is None:
            return Outcome(ok=False)
        self.state.confirming.add(site_id)
        return Outcome(ok=True)

    async def confirm_remove(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        try:
            removed = self.service.sites.remove(site_id)
        except Refusal as refusal:
            self.state.forget(site_id)
            self.state.notice = refusal.message.capitalize() + "."
            return Outcome(ok=False)
        self.state.forget(site_id)
        self.state.notice = f"Removed {removed.title}."
        return Outcome(ok=True)

    async def probe_site(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        site = self._site(site_id)
        if site is None:
            return Outcome(ok=False)
        result = await probe_site(site, site.identity or "", "sites", self.service.paths)

        def record() -> None:
            current = load_probes(self.service.paths)
            current[site.id] = result.model_dump(mode="json")
            write_json_atomic(probes_file(self.service.paths), current)

        await asyncio.to_thread(record)
        found = surfaces.probe_text(result.state)
        self.state.notice = f"Probed {site.title}. {found}."
        return Outcome(ok=True)

    async def switch_adapter(self, site_id: str, values: dict[str, Any]) -> Outcome:
        site = self._site(site_id)
        if site is None:
            return Outcome(ok=False)
        adapter = _first(values.get("adapter")) or site.adapter
        if adapter == site.adapter:
            self.state.notice = f"{site.title} already uses {adapter}."
            return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})
        try:
            switched = self.service.sites.switch(site_id, adapter)
        except Refusal as refusal:
            return self._fail(site_id, {FORM_ERROR: refusal.message.capitalize()})
        self.state.errors.pop(site_id, None)
        await self._saved(switched, f"{switched.title} now uses {adapter}.")
        return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})

    async def connect_site(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        site = self._site(site_id)
        if site is None:
            return Outcome(ok=False)
        try:
            adapter = self.service.adapter_for(site, "describe")
            need = await adapter.sign_in_needed()
            if need is None:
                self.state.links.pop(site_id, None)
                self.state.notice = f"{site.title} needs no sign-in."
                return Outcome(ok=True)
            url = need.url
            if url is None:
                ask = await self.service.ask_provider.url(site=site, operation_id=None, need=need)
                url = ask.url
        except Refusal as refusal:
            return self._fail(site_id, {FORM_ERROR: refusal.message.capitalize()})
        if not url:
            return self._fail(site_id, {FORM_ERROR: "Relay has no sign-in page to offer here"})
        self.state.links[site_id] = url
        self.state.notice = f"Open the sign-in page for {site.title}."
        return Outcome(ok=True)


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
    ) -> ToolResult:
        return await tab.act(ctx, action, token, values)

    server.tool(
        name=action,
        title=_TITLES[action],
        description=f"{_TITLES[action]} from the Compute tab. Only the tab calls this.",
        app=AppConfig(visibility=["app"]),
        annotations=mcp_types.ToolAnnotations(open_world_hint=False),
    )(act)


def install(server: FastMCP[Any], service: RelayService, changes: ServerChanges) -> ComputeTab:
    """Add the Compute tab's resource, its app-only tools and their listing rule to ``server``."""
    tab = ComputeTab(service, changes)

    @server.resource(
        TAB_URI,
        name="compute-tab",
        title="Compute",
        description="Relay's sites, for CLIO's Compute tab",
        mime_type=MIME_TYPE,
        annotations=mcp_types.Annotations(audience=["user"]),
    )
    async def compute_tab() -> ResourceResult:  # pyright: ignore[reportUnusedFunction]
        return await tab.read(a2ui_client(get_context()))

    for action in surfaces.ACTIONS:
        _action_tool(server, tab, action)
    server.add_middleware(_HideFromPlainClients())
    return tab
