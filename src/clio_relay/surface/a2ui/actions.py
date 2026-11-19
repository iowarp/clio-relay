"""What the Compute page's actions do, each through the site store the CLI uses (RL32).

:class:`~clio_relay.surface.a2ui.tab.ComputeTab` checks an action's token and
hands the token's subject (an endpoint id, an adapter name, or a special subject
such as :data:`~clio_relay.surface.a2ui.surfaces.ADD_SUBJECT`) to one handler
here. A handler changes the site store or the page's own state and returns an
:class:`Outcome`; it never renders.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.identity.probe import load as load_probes
from clio_relay.identity.probe import probe_site, probes_file
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui import surfaces
from clio_relay.surface.a2ui.forms import FORM_ERROR, Form, FormError
from clio_relay.surface.a2ui.surfaces import (
    ADD_FIELDS,
    ADD_PATH,
    ADD_SUBJECT,
    ADD_SURFACE,
    CATALOG_ERROR,
    CATALOG_ID_ERROR,
    CATALOG_PATH,
    CATALOG_SITE_ERROR,
    DEFAULTS,
    DEFAULTS_SUBJECT,
    EDIT_FIELDS,
    SECURITY,
    SITES_SURFACE,
    TabState,
    defaults_state,
    protocol_name,
    spend_sentence,
)
from clio_relay.surface.a2ui.values import (
    as_dict,
    config_values,
    first,
    located,
    pointer,
    rendered_keys,
    sentence,
    site_values,
    spend_ask,
)

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)


@dataclass
class Outcome:
    """What one action did, and which data-model paths the tab sends again, by surface."""

    ok: bool
    resend: dict[str, tuple[str, ...]] = field(default_factory=dict[str, tuple[str, ...]])


class TabActions:
    """The page's actions. :class:`~clio_relay.surface.a2ui.tab.ComputeTab` supplies the rest."""

    service: RelayService
    state: TabState

    def forms(self) -> dict[str, Form]:
        """Return a form for each installed adapter, by name."""
        raise NotImplementedError

    def _handlers(self) -> dict[str, Callable[[str, dict[str, Any]], Awaitable[Outcome]]]:
        return {
            surfaces.OPEN_ADD: self.open_add,
            surfaces.ADD_SITE: self.add_site,
            surfaces.ADD_FROM_CATALOG: self.add_from_catalog,
            surfaces.EDIT_SITE: self.edit_site,
            surfaces.SAVE_SITE: self.save_site,
            surfaces.CANCEL: self.cancel,
            surfaces.REMOVE_SITE: self.remove_site,
            surfaces.CONFIRM_REMOVE: self.confirm_remove,
            surfaces.PROBE_SITE: self.probe_site,
            surfaces.SWITCH_ADAPTER: self.switch_adapter,
            surfaces.SET_VALUE: self.set_value,
            surfaces.CONNECT_SITE: self.connect_site,
        }

    def _resend(self, surface_id: str, outcome: Outcome, data: dict[str, Any]) -> list[str]:
        """Return the paths to send again: the action's, then every form's ``valid`` map.

        The ``valid`` maps go every time, so a field's check follows the errors the
        components were just built from.
        """
        if surface_id == ADD_SURFACE:
            forms = [ADD_PATH, CATALOG_PATH]
        else:
            forms = [f"/drafts/{site_id}" for site_id in sorted(self.state.editing)]
        paths = [*outcome.resend.get(surface_id, ()), *(f"{path}/valid" for path in forms)]
        return [path for path in paths if pointer(data, path) is not None]

    def _fail(self, key: str, errors: dict[str, str]) -> Outcome:
        self.state.errors[key] = errors
        return Outcome(ok=False)

    def _site(self, site_id: str) -> SiteEntry | None:
        try:
            return self.service.sites.get(site_id)
        except Refusal as refusal:
            self.state.forget(site_id)
            self.state.notice = sentence(refusal.message) + "."
            return None

    async def _saved(self, site: SiteEntry, done: str) -> None:
        """Run the adapters' set-up for a saved site, as ``relay site add`` does."""
        try:
            results = await self.service.site_added(site.id)
        except Refusal as refusal:
            self.state.notice = f"{done} Setting it up failed: {refusal.message}."
            return
        except Exception as exc:  # the site is saved; say the set-up did not finish
            logger.warning("setting up site %s", site.id, exc_info=True)
            self.state.notice = f"{done} Setting it up failed ({type(exc).__name__})."
            return
        failed: list[str] = []
        for name, result in results.items():
            if "refusal" in as_dict(result):
                message = as_dict(as_dict(result)["refusal"]).get("message", "")
                failed.append(f"{name} could not set it up ({message})")
        self.state.notice = " ".join([done, *(item + "." for item in failed)])

    async def add_site(self, adapter: str, values: dict[str, Any]) -> Outcome:
        errors: dict[str, str] = {}
        common = site_values(ADD_FIELDS, as_dict(values.get("site")), errors)
        if not common.get("id") and "site.id" not in errors:
            errors["site.id"] = "Enter an endpoint ID"
        config = config_values(
            adapter,
            as_dict(as_dict(values.get("config")).get(adapter)),
            errors,
            f"config.{adapter}.",
        )
        if errors or config is None:
            return self._fail(ADD_SURFACE, errors)
        known = rendered_keys(ADD_FIELDS, "site.")
        try:
            entry = SiteEntry.model_validate(
                {
                    "id": common["id"],
                    "name": common.get("name"),
                    "adapter": adapter,
                    "config": config,
                    "policy": common.get("policy") or self.state.default("security"),
                    "spend": {"ask": common.get("spend") or spend_ask(self.state.default("spend"))},
                    "skill": common.get("skill"),
                }
            )
            added = self.service.sites.add(entry)
        except ValidationError as exc:
            return self._fail(ADD_SURFACE, located(exc.errors(include_url=False), "site.", known))
        except Refusal as refusal:
            if refusal.reason is RefusalReason.SITE_EXISTS:
                message = f"An endpoint named {common['id']} exists"
                return self._fail(ADD_SURFACE, {"site.id": message})
            return self._fail(ADD_SURFACE, {FORM_ERROR: refusal.message})
        self._close_add()
        await self._saved(added, f"Added {added.title}.")
        return Outcome(ok=True, resend={ADD_SURFACE: ("/drafts/add",), SITES_SURFACE: ("/switch",)})

    async def add_from_catalog(self, _subject: str, values: dict[str, Any]) -> Outcome:
        catalog_id = first(values.get("id"))
        site_id = str(values.get("site_id") or "").strip() or None
        if not catalog_id:
            return self._fail(
                ADD_SURFACE, {CATALOG_SITE_ERROR: "Choose an endpoint from the catalog"}
            )
        try:
            added = self.service.sites.add_from_catalog(catalog.site(catalog_id), site_id)
        except ValidationError:
            message = "An endpoint ID takes lowercase letters, digits, - and _"
            return self._fail(ADD_SURFACE, {CATALOG_ID_ERROR: message})
        except Refusal as refusal:
            if refusal.reason is RefusalReason.SITE_EXISTS:
                taken = refusal.data.get("site") or site_id or catalog_id
                return self._fail(
                    ADD_SURFACE, {CATALOG_ID_ERROR: f"An endpoint named {taken} exists"}
                )
            return self._fail(ADD_SURFACE, {CATALOG_ERROR: sentence(refusal.message)})
        self._close_add()
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
        common = site_values(EDIT_FIELDS, as_dict(values.get("site")), errors)
        drafts = as_dict(values.get("adapters"))
        config = site.config
        if site.adapter in drafts:
            prefix = f"adapters.{site.adapter}."
            config = config_values(site.adapter, as_dict(drafts[site.adapter]), errors, prefix)
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
            if not as_dict(draft).get("use"):
                continue
            parsed = config_values(name, as_dict(draft), errors, f"adapters.{name}.")
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
            "spend": {"ask": common.get("spend") or site.spend.ask},
            "skill": common.get("skill"),
            "defaults": defaults,
            "config": config,
            "alternatives": alternatives,
        }
        try:
            saved = self.service.sites.edit(site_id, changes)
        except Refusal as refusal:
            known = rendered_keys(EDIT_FIELDS, "site.")
            errors = located(refusal.data.get("errors"), "site.", known)
            return self._fail(site_id, errors or {FORM_ERROR: refusal.message})
        self.state.editing.discard(site_id)
        self.state.errors.pop(site_id, None)
        done = f"Saved {saved.title}."
        wanted = self.state.pending.pop(site_id, None)
        if wanted and wanted in saved.alternatives:
            try:
                saved = self.service.sites.switch(site_id, wanted)
                done += f" It now uses {protocol_name(wanted)}."
            except Refusal as refusal:
                done += f" {sentence(refusal.message)}."
        await self._saved(saved, done)
        return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})

    def _close_add(self) -> None:
        self.state.adding = False
        self.state.errors.pop(ADD_SURFACE, None)

    async def open_add(self, _subject: str, _values: dict[str, Any]) -> Outcome:
        if self.state.adding:
            return Outcome(ok=True)
        self.state.adding = True
        self.state.errors.pop(ADD_SURFACE, None)
        return Outcome(ok=True, resend={ADD_SURFACE: (ADD_PATH, CATALOG_PATH)})

    async def cancel(self, site_id: str, _values: dict[str, Any]) -> Outcome:
        if site_id == ADD_SUBJECT:
            self._close_add()
            return Outcome(ok=True)
        self.state.pending.pop(site_id, None)
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
            self.state.notice = sentence(refusal.message) + "."
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
        adapter = first(values.get("adapter")) or site.adapter
        if adapter == site.adapter:
            self.state.notice = f"{site.title} already uses {protocol_name(adapter)}."
            return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})
        try:
            switched = self.service.sites.switch(site_id, adapter)
        except Refusal as refusal:
            return self._fail(site_id, {FORM_ERROR: sentence(refusal.message)})
        self.state.errors.pop(site_id, None)
        await self._saved(switched, f"{switched.title} now uses {protocol_name(adapter)}.")
        return Outcome(ok=True, resend={SITES_SURFACE: ("/switch",)})

    async def set_value(self, subject: str, values: dict[str, Any]) -> Outcome:
        """Change a default, or one endpoint's protocol, security or spending, from a picker."""
        if subject == DEFAULTS_SUBJECT:
            return self._set_defaults(values)
        site = self._site(subject)
        if site is None:
            return Outcome(ok=False)
        column = str(values.get("column") or "")
        value = first(values.get("value"))
        if column == "security":
            return await self._set_security(site, value)
        if column == "protocol":
            return await self._set_protocol(site, value)
        if column == "spend":
            return await self._set_spend(site, value)
        return self._fail(site.id, {FORM_ERROR: f"Relay cannot change {column or 'that'} here"})

    def _set_defaults(self, values: dict[str, Any]) -> Outcome:
        # A table row sends one column and its value; the Basic form sends every picker.
        if values.get("column"):
            picked = {str(values["column"]): values.get("value")}
        else:
            picked = {key: values[key] for key in DEFAULTS if key in values}
        listed = {name for name, item in self.forms().items() if item.listed}
        changed: dict[str, Any] = {}
        for column, raw in picked.items():
            value = first(raw)
            if (column == "protocol" and value in listed) or (
                column == "security" and value in SECURITY
            ):
                changed[column] = value
                continue
            if column == "spend":
                try:
                    changed[column] = spend_ask(value)
                except FormError:
                    pass
                else:
                    continue
            self.state.notice = "Relay has no such default for new endpoints."
            return Outcome(ok=False, resend={SITES_SURFACE: ("/defaults",)})
        try:
            saved = self.service.sites.set_defaults(
                adapter=changed.get("protocol"),
                policy=changed.get("security"),
                spend=changed.get("spend"),
            )
        except Refusal as refusal:
            self.state.notice = sentence(refusal.message)
            return Outcome(ok=False, resend={SITES_SURFACE: ("/defaults",)})
        self.state.defaults = defaults_state(saved)
        self.state.notice = "Saved the defaults for new endpoints."
        return Outcome(ok=True, resend={SITES_SURFACE: ("/defaults",)})

    async def _set_spend(self, site: SiteEntry, value: str) -> Outcome:
        try:
            ask = spend_ask(value)
        except FormError as exc:
            return self._fail(site.id, {FORM_ERROR: str(exc)})
        if ask == site.spend.ask:
            return Outcome(ok=True)
        try:
            saved = self.service.sites.edit(site.id, {"spend": {"ask": ask}})
        except Refusal as refusal:
            return self._fail(site.id, {FORM_ERROR: sentence(refusal.message)})
        self.state.errors.pop(site.id, None)
        await self._saved(saved, f"Relay now {spend_sentence(ask)} on {saved.title}.")
        return Outcome(ok=True)

    async def _set_security(self, site: SiteEntry, value: str) -> Outcome:
        if value not in SECURITY:
            return self._fail(site.id, {FORM_ERROR: "Choose Secure or Open"})
        if value == site.policy:
            return Outcome(ok=True)
        try:
            saved = self.service.sites.edit(site.id, {"policy": value})
        except Refusal as refusal:
            return self._fail(site.id, {FORM_ERROR: sentence(refusal.message)})
        self.state.errors.pop(site.id, None)
        await self._saved(saved, f"{saved.title} is now {SECURITY[value][0]}.")
        return Outcome(ok=True)

    async def _set_protocol(self, site: SiteEntry, value: str) -> Outcome:
        if value == site.adapter:
            return Outcome(ok=True)
        if value in site.alternatives:
            return await self.switch_adapter(site.id, {"adapter": [value]})
        if value not in self.forms():
            return self._fail(site.id, {FORM_ERROR: f"Relay has no protocol named {value}"})
        try:
            config = registry.validate_config(value, {})
        except Refusal:
            # It needs settings first: open the form, and use it once they are saved.
            self.state.pending[site.id] = value
            self.state.editing.add(site.id)
            self.state.confirming.discard(site.id)
            self.state.errors.pop(site.id, None)
            name = protocol_name(value)
            self.state.notice = f"Fill in {site.title}'s {name} settings, then save to use {name}."
            return Outcome(ok=True, resend={SITES_SURFACE: (f"/drafts/{site.id}",)})
        settings = config.model_dump(mode="json", exclude_defaults=True)
        try:
            self.service.sites.edit(
                site.id, {"alternatives": site.alternatives | {value: settings}}
            )
        except Refusal as refusal:
            return self._fail(site.id, {FORM_ERROR: sentence(refusal.message)})
        return await self.switch_adapter(site.id, {"adapter": [value]})

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
            return self._fail(site_id, {FORM_ERROR: sentence(refusal.message)})
        if not url:
            return self._fail(site_id, {FORM_ERROR: "Relay has no sign-in page to offer here"})
        self.state.links[site_id] = url
        self.state.notice = f"Open the sign-in page for {site.title}."
        return Outcome(ok=True)
