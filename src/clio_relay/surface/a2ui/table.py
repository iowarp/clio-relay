"""The Compute page, for a client that renders relay's catalog (RL32).

Laid out like CLIO's Settings > Tools. Under the page's heading, a line says what
the page is for, with Add endpoint beside it. Then one ``clio.choice-table.v1``
lists the person's endpoints, with its column headers even when there are none,
and above them a row of the defaults new endpoints start with, picked in place.
Each endpoint's row has inline Protocol, Security and Spending pickers, its
Status, and a menu of Connect, Test, Edit and Remove. Protocol offers only the
ways that endpoint has. A value the endpoint takes from the defaults is marked
inherited.

Everything else opens in a dialog (:mod:`clio_relay.surface.a2ui.dialog`), and
what an action did shows as a toast, with Undo when it can be undone. Nothing
opens in the page itself.

A client that renders only the Basic catalog gets the cards of
:mod:`clio_relay.surface.a2ui.cards` instead.
"""

from __future__ import annotations

from typing import Any

from clio_relay.surface.a2ui.components import TABLE, Tree, event
from clio_relay.surface.a2ui.dialog import (
    ADD,
    DialogInputs,
    add_data,
    add_dialog,
    add_trigger,
    dialog_flags,
)
from clio_relay.surface.a2ui.forms import FORM_ERROR, Form
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.site_dialogs import (
    edit_data,
    edit_dialog,
    sign_in_dialog,
)
from clio_relay.surface.a2ui.surfaces import (
    CONFIRM_REMOVE,
    CONNECT_SITE,
    DEFAULTS,
    DEFAULTS_SUBJECT,
    EDIT_DIALOG,
    EDIT_SITE,
    HOST_SIGN_IN,
    PROBE_SITE,
    PROTOCOLS,
    SECURITY,
    SECURITY_HELP,
    SET_VALUE,
    SIGN_IN_DIALOG,
    SPEND_HELP,
    SiteView,
    TabState,
    probe_text,
    protocol_name,
    protocol_order,
    spend_options,
)
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.a2ui.values import spend_ask, spend_value

# clio-workspace/v1 caps labels at 120 characters and descriptions at 500.
_LABEL = 120
_TEXT = 500
# Names from clio-workspace/v1's Icon enum.
_PROTOCOL_ICON = {
    "ssh": "terminal",
    "worker": "memory",
    "tapis": "hub",
    "globus": "public",
    "iri": "dns",
    "sky": "cloud",
    "local": "computer",
}
_SECURITY_LOOK = {"guarded": ("lock", "positive"), "open": ("lockOpen", "warning")}
_STATUS = {
    "ready": ("Ready", "Answers and needs nothing from you", "check", "positive"),
    "needs_sign_in": ("Sign-in needed", "Connect it from the row's menu", "warning", "warning"),
    "unavailable": ("Unavailable", "Relay cannot reach it now", "error", "negative"),
    "checking": ("Checking", "Relay is reading its state now", "refresh", "neutral"),
}


def _spend_options(state: TabState, views: list[SiteView]) -> list[dict[str, str]]:
    used = [view.site.spend.ask for view in views] + [spend_ask(state.default("spend"))]
    shown: list[dict[str, str]] = []
    for option in spend_options(used):
        ask = spend_ask(option["value"])
        if isinstance(ask, str):
            words = {"label": _(option["label"]), "description": _(option["description"])}
        else:
            words = {
                "label": _("Ask above {limit}", limit=f"{ask:g}"),
                "description": _("Over {limit} node-hours, or no walltime", limit=f"{ask:g}"),
            }
        shown.append(option | words)
    return shown


def _columns(
    forms: dict[str, Form], views: list[SiteView], state: TabState
) -> list[dict[str, Any]]:
    listed = [name for name, item in forms.items() if item.listed]
    used = [name for view in views for name in (view.site.adapter, *view.site.alternatives)]
    protocols: list[dict[str, Any]] = []
    for name in protocol_order([*listed, *used]):
        option: dict[str, Any] = {"value": name, "label": protocol_name(name)}
        if name in _PROTOCOL_ICON:
            option["icon"] = _PROTOCOL_ICON[name]
        if PROTOCOLS.get(name):
            option["description"] = _(PROTOCOLS[name][1])
        protocols.append(option)
    security = [
        {
            "value": value,
            "label": _(label),
            "description": _(description),
            "icon": _SECURITY_LOOK[value][0],
            "tone": _SECURITY_LOOK[value][1],
        }
        for value, (label, description) in SECURITY.items()
    ]
    status = [
        {
            "value": value,
            "label": _(label),
            "description": _(description),
            "icon": icon,
            "tone": tone,
        }
        for value, (label, description, icon, tone) in _STATUS.items()
    ]
    return [
        {
            "id": "protocol",
            "title": _("Protocol"),
            "help": _("How relay reaches the endpoint."),
            "options": protocols,
        },
        {"id": "security", "title": _("Security"), "help": _(SECURITY_HELP), "options": security},
        {
            "id": "spend",
            "title": _("Spending"),
            "help": _(SPEND_HELP),
            "options": _spend_options(state, views),
        },
        {"id": "status", "title": _("Status"), "editable": False, "options": status},
    ]


def _caption(view: SiteView) -> str | None:
    """Return the row's second line: what the status needs, and the last test."""
    parts: list[str] = []
    state = str(view.status.get("state", ""))
    if state == "needs_sign_in" and view.status.get("provider"):
        parts.append(_("Needs your {provider} sign-in", provider=str(view.status["provider"])))
    elif state != "ready" and view.status.get("detail"):
        parts.append(str(view.status["detail"]))
    if view.probe:
        found = _(probe_text(str(view.probe.get("state", "")))).lower()
        parts.append(_("The last test found it {found}", found=found))
    return ". ".join(parts) or None


def _menu(view: SiteView, tokens: ActionTokens) -> list[dict[str, Any]]:
    site = view.site
    items: list[dict[str, Any]] = []
    signed_out = view.identity is not None and view.identity.get("state") != "signed_in"
    if view.status.get("state") == "needs_sign_in" or signed_out:
        if view.host_sign_in:
            action = event(f"{HOST_SIGN_IN}{view.host_sign_in}", {})
        else:
            action = event(CONNECT_SITE, {"token": tokens.mint(CONNECT_SITE, site.id)})
        items.append({"id": "connect", "label": _("Connect"), "action": action})
    items += [
        {
            "id": "test",
            "label": _("Test"),
            "action": event(PROBE_SITE, {"token": tokens.mint(PROBE_SITE, site.id)}),
        },
        {
            "id": "edit",
            "label": _("Edit"),
            "action": event(EDIT_SITE, {"token": tokens.mint(EDIT_SITE, site.id)}),
        },
        {
            "id": "remove",
            "label": _("Remove"),
            "destructive": True,
            "action": event(CONFIRM_REMOVE, {"token": tokens.mint(CONFIRM_REMOVE, site.id)}),
        },
    ]
    return items


def _clip(text: str, limit: int) -> str:
    """Keep a row's text within the table's bounds, so one long name can't void the table."""
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "\u2026"


def _row(view: SiteView, state: TabState, tokens: ActionTokens) -> dict[str, Any]:
    site = view.site
    values = {
        "protocol": site.adapter,
        "security": site.policy,
        "spend": spend_value(site.spend.ask),
    }
    status = str(view.status.get("state", ""))
    if status in _STATUS:
        values["status"] = status
    inherited = [
        column for column in ("security", "spend") if values[column] == state.default(column)
    ]
    row: dict[str, Any] = {
        "id": site.id,
        "title": _clip(site.title, _LABEL),
        "values": values,
        "options": {"protocol": [site.adapter, *site.alternatives]},
        "context": {"token": tokens.mint(SET_VALUE, site.id)},
        "menu": _menu(view, tokens),
    }
    if inherited:
        row["inherited"] = inherited
    if site.address:
        row["subtitle"] = _clip(site.address, _LABEL)
    caption = _caption(view)
    if caption:
        row["caption"] = _clip(caption, _LABEL)
    error = state.errors.get(site.id, {}).get(FORM_ERROR)
    if error and not state.dialog.startswith(EDIT_DIALOG):
        row["error"] = _clip(error, _TEXT)
    return row


def _open_view(views: list[SiteView], state: TabState, prefix: str) -> SiteView | None:
    if not state.dialog.startswith(prefix):
        return None
    site_id = state.dialog[len(prefix) :]
    return next((view for view in views if view.site.id == site_id), None)


def endpoints_surface(
    views: list[SiteView],
    state: TabState,
    forms: dict[str, Form],
    tokens: ActionTokens,
    inputs: DialogInputs | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the page as components, and its data model."""
    tree = Tree()
    found = inputs or DialogInputs()
    adding = add_dialog(tree, state, forms, tokens, found, add_trigger(tree, tokens))
    lead = tree.text(
        "page-lead", _("Where relay runs jobs and services for your conversations."), "caption"
    )
    children = [tree.row("page-head", [lead, adding], align="center", justify="spaceBetween")]
    children.append(
        tree.add(
            "endpoints",
            TABLE,
            label=_("Endpoints"),
            columns=_columns(forms, views, state),
            rows=[_row(view, state, tokens) for view in views],
            defaults={
                "title": _("Defaults for new endpoints"),
                "values": {column: state.default(column) for column in DEFAULTS},
                "context": {"token": tokens.mint(SET_VALUE, DEFAULTS_SUBJECT)},
            },
            onChoose=event(SET_VALUE, {}),
            empty=_("No endpoints yet"),
        )
    )
    data: dict[str, Any] = {
        "dialogs": dialog_flags(state),
        ADD.strip("/"): add_data(state, forms),
        "drafts": {},
    }
    editing = _open_view(views, state, EDIT_DIALOG)
    if editing is not None:
        children.append(edit_dialog(tree, editing, state, forms, tokens))
        data["drafts"][editing.site.id] = edit_data(editing, state, forms)
    signing = _open_view(views, state, SIGN_IN_DIALOG)
    link = state.links.get(signing.site.id) if signing is not None else None
    if signing is not None and link:
        children.append(sign_in_dialog(tree, signing, link, tokens))
    tree.column("root", children)
    return tree.components, data
