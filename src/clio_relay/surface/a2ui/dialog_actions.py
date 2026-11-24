"""What the Compute page's dialogs and toasts do (RL32).

The add dialog keeps its draft in the page's state, so a re-render shows what
the person picked. Test tries the endpoint the draft stands for without saving
it; Save endpoint adds it only when the last test passed on that same draft.
Adding and removing raise a toast with Undo, and a removed endpoint stays
offered in the Saved tab.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from clio_relay import catalog
from clio_relay.identity.probe import try_site
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui.components import event
from clio_relay.surface.a2ui.dialog import DialogInputs, passed
from clio_relay.surface.a2ui.forms import FORM_ERROR, Form, FormError
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.newsite import (
    CATALOG_TAB,
    REMOVED,
    SAVED_TAB,
    Built,
    build,
    draft_key,
    ready_line,
    removed_record,
    saved_items,
)
from clio_relay.surface.a2ui.surfaces import (
    ADD_DIALOG,
    ADD_SUBJECT,
    DEFAULTS_DIALOG,
    DEFAULTS_SUBJECT,
    EDIT_DIALOG,
    HOST_SIGN_IN,
    REMOVED_KEPT,
    SIGN_IN_DIALOG,
    SITES_SURFACE,
    UNDO,
    UNDO_SUBJECT,
    TabState,
    protocol_name,
)
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.a2ui.values import as_dict, first, sentence, spend_choice

if TYPE_CHECKING:
    from clio_relay.surface.a2ui.actions import Outcome
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

TEST_TIMEOUT_S = 90.0
"""How long Test waits for an endpoint to answer before it calls it unreachable."""
_DRAFT_KEYS = ("tab", "site", "saved", "savedProtocol", "protocol", "ownProtocol", "own", "config")
_PICKED = {"site", "saved", "protocol", "ownProtocol"}
DIALOG_PATHS = ("/dialogs", "/add", "/defaultsDraft")
"""The page's dialog data, sent again after every action since a component update has none."""


def _outcome(ok: bool, toast: dict[str, Any] | None = None) -> Outcome:
    from clio_relay.surface.a2ui.actions import Outcome

    return Outcome(ok=ok, resend={SITES_SURFACE: DIALOG_PATHS}, toast=toast)


def clean_draft(values: dict[str, Any]) -> dict[str, Any]:
    """Return the add dialog's draft as the page sent it, without the fields it adds."""
    found = {key: values[key] for key in _DRAFT_KEYS if key in values}
    found["save"] = as_dict(values.get("save"))
    return found


class DialogActions:
    """The dialogs' actions. :class:`~clio_relay.surface.a2ui.tab.ComputeTab` supplies the rest."""

    service: RelayService
    state: TabState
    tokens: ActionTokens

    def forms(self) -> dict[str, Form]:
        raise NotImplementedError

    async def _saved(self, site: SiteEntry, done: str) -> None:
        raise NotImplementedError

    def _set_defaults(self, values: dict[str, Any]) -> Outcome:
        raise NotImplementedError

    def _taken(self) -> list[str]:
        try:
            return [site.id for site in self.service.sites.list()]
        except Refusal:
            return []

    def dialog_inputs(self) -> DialogInputs:
        """Return what the dialogs list: the catalog's sites and the Saved tab's computers."""
        try:
            sites = catalog.grouped()
        except Refusal as refusal:
            logger.warning("reading the catalog for the add dialog: %s", refusal.message)
            sites = []
        return DialogInputs(sites=sites, saved=saved_items(self.state.removed, self._taken()))

    def toast(
        self, message: str, tone: str = "positive", undo: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return a toast; with ``undo``, it offers Undo, which reverses what ``undo`` names."""
        found: dict[str, Any] = {"message": message, "tone": tone}
        if undo is not None:
            self.state.undo = undo
            found["undo"] = {
                "label": _("Undo"),
                "action": event(UNDO, {"token": self.tokens.mint(UNDO, UNDO_SUBJECT)}),
            }
        return found

    # Opening and closing

    async def open_dialog(self, subject: str, _values: dict[str, Any]) -> Outcome:
        if subject == ADD_SUBJECT:
            if self.state.dialog != ADD_DIALOG:
                self.state.add = {}
            self.state.dialog = ADD_DIALOG
            self.state.errors.pop(ADD_DIALOG, None)
        elif subject == DEFAULTS_SUBJECT:
            self.state.dialog = DEFAULTS_DIALOG
            self.state.errors.pop(DEFAULTS_DIALOG, None)
        else:
            return _outcome(False)
        return _outcome(True)

    async def close_dialog(self, subject: str, _values: dict[str, Any]) -> Outcome:
        if subject == ADD_SUBJECT:
            self.state.add = {}
            self.state.errors.pop(ADD_DIALOG, None)
            if self.state.dialog == ADD_DIALOG:
                self.state.dialog = ""
        elif subject == DEFAULTS_SUBJECT:
            self.state.errors.pop(DEFAULTS_DIALOG, None)
            if self.state.dialog == DEFAULTS_DIALOG:
                self.state.dialog = ""
        return _outcome(True)

    def close_site_dialog(self, site_id: str) -> None:
        """Close the edit or sign-in dialog of ``site_id``, if it is the one open."""
        if self.state.dialog in (EDIT_DIALOG + site_id, SIGN_IN_DIALOG + site_id):
            self.state.dialog = ""

    # The add dialog

    def _set_draft(self, values: dict[str, Any]) -> dict[str, Any]:
        draft = clean_draft(values)
        self.state.add["draft"] = draft
        self.state.dialog = ADD_DIALOG
        return draft

    def _forget_test(self) -> None:
        self.state.add.pop("test", None)
        self.state.add.pop("facts", None)
        self.state.add.pop("view", None)

    async def pick(self, _subject: str, values: dict[str, Any]) -> Outcome:
        column = str(values.get("column") or "")
        if column not in _PICKED:
            return _outcome(False)
        draft = self._set_draft(values)
        draft[column] = first(values.get("value"))
        if column == "site":
            draft["tab"] = CATALOG_TAB
            found = {site.id: site for site in self.dialog_inputs().sites}
            group = found.get(draft["site"])
            draft["protocol"] = group.ways[0].adapter if group is not None else ""
        elif column == "saved":
            draft["tab"] = SAVED_TAB
            item = next(
                (item for item in self.dialog_inputs().saved if item.key == draft["saved"]), None
            )
            draft["savedProtocol"] = item.adapter if item is not None else ""
        self._forget_test()
        self.state.errors.pop(ADD_DIALOG, None)
        return _outcome(True)

    async def back(self, _subject: str, values: dict[str, Any]) -> Outcome:
        self._set_draft(values)
        self._forget_test()
        self.state.errors.pop(ADD_DIALOG, None)
        return _outcome(True)

    def _build(self, draft: dict[str, Any], final: bool = False) -> Built:
        inputs = self.dialog_inputs()
        return build(
            draft,
            self.service.sites,
            self.forms(),
            self.state.removed,
            final=final,
            sites=inputs.sites,
        )

    async def test_endpoint(self, _subject: str, values: dict[str, Any]) -> Outcome:
        draft = self._set_draft(values)
        self._forget_test()
        built = self._build(draft)
        if built.entry is None:
            self.state.errors[ADD_DIALOG] = built.errors
            return _outcome(False)
        self.state.errors.pop(ADD_DIALOG, None)
        entry = built.entry
        started = time.monotonic()
        try:
            state, detail, info = await asyncio.wait_for(
                try_site(entry, self.service.paths), TEST_TIMEOUT_S
            )
        except TimeoutError:
            state, detail, info = "unreachable", None, None
        seconds = time.monotonic() - started
        protocol = protocol_name(entry.adapter)
        test: dict[str, Any] = {"state": state, "key": draft_key(draft)}
        if state == "ready":
            test["line"] = ready_line(built.title, entry.adapter, info, seconds)
            if detail:
                test["detail"] = detail
            save = as_dict(draft.get("save"))
            # The draft holds every field, empty until the person fills it: fill the empty ones.
            filled = {"name": built.title, "policy": [self.state.default("security")]}
            from clio_relay.surface.a2ui.surfaces import spend_draft
            from clio_relay.surface.a2ui.values import spend_ask

            filled.update(spend_draft(spend_ask(self.state.default("spend"))))
            for key, value in filled.items():
                if save.get(key) in (None, "", []):
                    save[key] = value
            draft["save"] = save
        elif state == "needs_sign_in":
            provider = (entry.identity or entry.adapter).lower()
            test["line"] = _(
                "{protocol} needs your {provider} sign-in first",
                protocol=protocol,
                provider=provider.upper() if len(provider) <= 4 else provider.capitalize(),
            )
            if detail:
                test["detail"] = detail
            if provider in self.service.paths.grant_files:
                test["host_sign_in"] = HOST_SIGN_IN + provider
        elif state == "missing_allocation":
            test["line"] = _("Reached {site}, but this account has no allocation", site=built.title)
        elif state == "not_configured":
            test["line"] = _("Relay cannot use these {protocol} settings", protocol=protocol)
            if detail:
                test["detail"] = sentence(detail)
        else:
            where = built.address or built.title
            test["line"] = _("Can't reach {address}", address=where)
            if detail:
                test["detail"] = sentence(detail)
            elif state == "unreachable":
                test["detail"] = _("No answer in {seconds} s", seconds=f"{seconds:.0f}")
        self.state.add["test"] = test
        self.state.add["facts"] = built.facts
        self.state.add["view"] = "test"
        return _outcome(state == "ready")

    async def save_endpoint(self, _subject: str, values: dict[str, Any]) -> Outcome:
        draft = self._set_draft(values)
        if not passed(self.state):
            self.state.errors[ADD_DIALOG] = {FORM_ERROR: _("Test it again before you save it")}
            return _outcome(False)
        built = self._build(draft, final=True)
        if built.entry is None:
            self.state.errors[ADD_DIALOG] = built.errors
            return _outcome(False)
        try:
            added = self.service.sites.add(built.entry)
        except Refusal as refusal:
            if refusal.reason is RefusalReason.SITE_EXISTS:
                message = _("An endpoint named {id} exists", id=built.entry.id)
            else:
                message = sentence(refusal.message)
            self.state.errors[ADD_DIALOG] = {FORM_ERROR: message}
            return _outcome(False)
        picked = str(draft.get("saved") or "")
        if draft.get("tab") == SAVED_TAB and picked.startswith(REMOVED):
            self._unremember(added.id)
        self.state.add = {}
        self.state.errors.pop(ADD_DIALOG, None)
        self.state.dialog = ""
        done = _("Added {name}", name=added.title)
        self.state.notice = ""
        await self._saved(added, done + ".")
        message = self.state.notice.rstrip(".") or done
        self.state.notice = ""
        tone = "positive" if message == done else "warning"
        return _outcome(True, self.toast(message, tone, {"kind": "added", "id": added.id}))

    # Removing, the defaults and Undo

    def remember_removed(self, entry: SiteEntry) -> None:
        """Offer ``entry`` back in the Saved tab, newest first."""
        kept = [
            item for item in self.state.removed if as_dict(item.get("entry")).get("id") != entry.id
        ]
        self.state.removed = [removed_record(entry), *kept][:REMOVED_KEPT]

    def _unremember(self, site_id: str) -> None:
        self.state.removed = [
            item for item in self.state.removed if as_dict(item.get("entry")).get("id") != site_id
        ]

    async def save_defaults(self, _subject: str, values: dict[str, Any]) -> Outcome:
        picked: dict[str, Any] = {}
        protocol = first(values.get("protocol"))
        if protocol:
            picked["protocol"] = protocol
        policy = first(values.get("policy"))
        if policy:
            picked["security"] = policy
        try:
            spend = spend_choice(values.get("spend"), values.get("spend_limit"))
        except FormError as exc:
            self.state.errors[DEFAULTS_DIALOG] = {exc.key: str(exc), FORM_ERROR: str(exc)}
            return _outcome(False)
        if spend is not None:
            picked["spend"] = spend
        outcome = self._set_defaults(picked)
        if not outcome.ok:
            self.state.errors[DEFAULTS_DIALOG] = {FORM_ERROR: self.state.notice}
            self.state.notice = ""
            return _outcome(False)
        self.state.errors.pop(DEFAULTS_DIALOG, None)
        self.state.dialog = ""
        message = self.state.notice.rstrip(".")
        self.state.notice = ""
        return _outcome(True, self.toast(message))

    async def undo(self, _subject: str, _values: dict[str, Any]) -> Outcome:
        undo, self.state.undo = self.state.undo, {}
        kind = undo.get("kind")
        try:
            if kind == "added":
                gone = self.service.sites.remove(str(undo.get("id") or ""))
                self.state.forget(gone.id)
                return _outcome(True, self.toast(_("Removed {name}", name=gone.title)))
            if kind == "removed":
                entry = SiteEntry.model_validate(as_dict(undo.get("entry")))
                back = self.service.sites.add(entry)
                self._unremember(back.id)
                self.state.notice = ""
                done = _("Put {name} back", name=back.title)
                await self._saved(back, done + ".")
                message = self.state.notice.rstrip(".") or done
                self.state.notice = ""
                return _outcome(True, self.toast(message))
        except (Refusal, ValueError) as exc:
            text = exc.message if isinstance(exc, Refusal) else str(exc)
            return _outcome(False, self.toast(sentence(text), "negative"))
        return _outcome(False, self.toast(_("There is nothing to undo"), "neutral"))
