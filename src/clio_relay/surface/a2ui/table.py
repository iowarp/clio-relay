"""The Compute page's endpoint table, for a client that renders relay's catalog (RL32).

The table is one ``clio.choice-table.v1`` component, laid out like CLIO's Tools
settings. A header row holds the defaults for new endpoints, then there is one
row per endpoint. Each row has inline Protocol, Security and Spending pickers, its Status,
and a menu of Probe, Edit and Remove. Picking a value calls
:data:`~clio_relay.surface.a2ui.surfaces.SET_VALUE` with the row's token, and the
host adds the column and value. An open edit form, remove confirmation or
sign-in link is rendered as a Basic card after the table, headed by its
endpoint's name, so the table keeps to the component's published schema.

A client that renders only the Basic catalog gets the cards of
:mod:`clio_relay.surface.a2ui.surfaces` instead.
"""

from __future__ import annotations

from typing import Any

from clio_relay.surface.a2ui.components import TABLE, Tree, event, open_url
from clio_relay.surface.a2ui.forms import FORM_ERROR, Form, valid_map
from clio_relay.surface.a2ui.surfaces import (
    ADD_SUBJECT,
    CANCEL,
    CONFIRM_REMOVE,
    CONNECT_SITE,
    DEFAULTS,
    DEFAULTS_SUBJECT,
    EDIT_ERROR_PREFIXES,
    EDIT_SITE,
    HOST_SIGN_IN,
    OPEN_ADD,
    PROBE_SITE,
    PROTOCOLS,
    REMOVE_SITE,
    SECURITY,
    SECURITY_HELP,
    SET_VALUE,
    SPEND_HELP,
    SiteView,
    TabState,
    edit_draft,
    edit_form,
    identity_rows,
    probe_text,
    protocol_name,
    protocol_order,
    settings_rows,
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
    "needs_sign_in": ("Needs a sign-in", "Connect it from the row's menu", "warning", "warning"),
    "unavailable": ("Unavailable", "Relay cannot reach it now", "error", "negative"),
}


def _spend_options(state: TabState, views: list[SiteView]) -> list[dict[str, str]]:
    used = [view.site.spend.ask for view in views] + [spend_ask(state.default("spend"))]
    return spend_options(used)


def _columns(
    forms: dict[str, Form], views: list[SiteView], state: TabState
) -> list[dict[str, Any]]:
    listed = [name for name, item in forms.items() if item.listed]
    names = protocol_order([*listed, *(view.site.adapter for view in views)])
    protocols: list[dict[str, Any]] = []
    for name in names:
        option: dict[str, Any] = {"value": name, "label": protocol_name(name)}
        if name in _PROTOCOL_ICON:
            option["icon"] = _PROTOCOL_ICON[name]
        if PROTOCOLS.get(name):
            option["description"] = PROTOCOLS[name][1]
        protocols.append(option)
    security = [
        {
            "value": value,
            "label": label,
            "description": description,
            "icon": _SECURITY_LOOK[value][0],
            "tone": _SECURITY_LOOK[value][1],
        }
        for value, (label, description) in SECURITY.items()
    ]
    status = [
        {"value": value, "label": label, "description": description, "icon": icon, "tone": tone}
        for value, (label, description, icon, tone) in _STATUS.items()
    ]
    return [
        {
            "id": "protocol",
            "title": "Protocol",
            "help": "How relay reaches the endpoint.",
            "options": protocols,
        },
        {"id": "security", "title": "Security", "help": SECURITY_HELP, "options": security},
        {
            "id": "spend",
            "title": "Spending",
            "help": SPEND_HELP,
            "options": _spend_options(state, views),
        },
        {"id": "status", "title": "Status", "editable": False, "options": status},
    ]


def _caption(view: SiteView) -> str | None:
    """Return the row's second line: what the status needs, and the last probe."""
    parts: list[str] = []
    state = str(view.status.get("state", ""))
    if state == "needs_sign_in" and view.status.get("provider"):
        parts.append(f"Needs a {view.status['provider']} sign-in")
    elif state != "ready" and view.status.get("detail"):
        parts.append(str(view.status["detail"]))
    if view.probe:
        found = probe_text(str(view.probe.get("state", ""))).lower()
        parts.append(f"The last probe found it {found}")
    return ". ".join(parts) or None


def _menu(view: SiteView, state: TabState, tokens: ActionTokens) -> list[dict[str, Any]]:
    site = view.site
    items: list[dict[str, Any]] = []
    signed_out = view.identity is not None and view.identity.get("state") != "signed_in"
    if view.status.get("state") == "needs_sign_in" or signed_out:
        if view.host_sign_in:
            action = event(f"{HOST_SIGN_IN}{view.host_sign_in}", {})
        else:
            action = event(CONNECT_SITE, {"token": tokens.mint(CONNECT_SITE, site.id)})
        items.append({"id": "connect", "label": "Connect", "action": action})
    items.append(
        {
            "id": "probe",
            "label": "Probe",
            "action": event(PROBE_SITE, {"token": tokens.mint(PROBE_SITE, site.id)}),
        }
    )
    if site.id not in state.editing:
        items.append(
            {
                "id": "edit",
                "label": "Edit",
                "action": event(EDIT_SITE, {"token": tokens.mint(EDIT_SITE, site.id)}),
            }
        )
    items.append(
        {
            "id": "remove",
            "label": "Remove",
            "destructive": True,
            "action": event(REMOVE_SITE, {"token": tokens.mint(REMOVE_SITE, site.id)}),
        }
    )
    return items


def _detail(
    tree: Tree, view: SiteView, state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> str | None:
    """Add an endpoint's open card: a sign-in link, a remove confirmation, the edit form."""
    site = view.site
    base = f"s-{site.id}"
    children: list[str] = []
    link = state.links.get(site.id)
    if link:
        children.append(tree.button(f"{base}-open", "Open the sign-in page", open_url(link)))
    if site.id in state.confirming:
        question = tree.text(
            f"{base}-confirm-q",
            f"Remove {site.title} from your endpoints? Its operations stay on record.",
        )
        yes = tree.button(
            f"{base}-confirm-yes",
            "Remove endpoint",
            event(CONFIRM_REMOVE, {"token": tokens.mint(CONFIRM_REMOVE, site.id)}),
            "primary",
        )
        no = tree.button(
            f"{base}-confirm-no", "Keep it", event(CANCEL, {"token": tokens.mint(CANCEL, site.id)})
        )
        children += [question, tree.row(f"{base}-confirm", [yes, no], justify="start")]
    if site.id in state.editing:
        facts = identity_rows(tree, base, view) + settings_rows(tree, base, view, fields=False)
        errors = state.errors.get(site.id, {})
        pending = state.pending.get(site.id, "")
        children.append(edit_form(tree, site, forms, tokens, errors, pending, facts))
    if not children:
        return None
    if site.id not in state.editing:
        # The edit form has its own heading.
        children.insert(0, tree.text(f"{base}-detail-title", site.title, "h4"))
    return tree.card(f"{base}-detail", tree.column(f"{base}-detail-body", children))


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
    row: dict[str, Any] = {
        "id": site.id,
        "title": _clip(site.title, _LABEL),
        "values": values,
        "context": {"token": tokens.mint(SET_VALUE, site.id)},
        "menu": _menu(view, state, tokens),
    }
    if site.address:
        row["subtitle"] = _clip(site.address, _LABEL)
    caption = _caption(view)
    if caption:
        row["caption"] = _clip(caption, _LABEL)
    error = state.errors.get(site.id, {}).get(FORM_ERROR)
    if error and site.id not in state.editing:
        row["error"] = _clip(error, _TEXT)
    return row


def endpoints_surface(
    views: list[SiteView], state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the endpoints surface as one table, and its data model."""
    tree = Tree()
    # The page header and the table's own label name the page, so there is no heading.
    children: list[str] = []
    if state.notice:
        children.append(tree.text("sites-notice", state.notice))
    rows = [_row(view, state, tokens) for view in views]
    children.append(
        tree.add(
            "endpoints",
            TABLE,
            label="Endpoints",
            columns=_columns(forms, views, state),
            defaults={
                "title": "Defaults for new endpoints",
                "values": {column: state.default(column) for column in DEFAULTS},
                "context": {"token": tokens.mint(SET_VALUE, DEFAULTS_SUBJECT)},
            },
            rows=rows,
            onChoose=event(SET_VALUE, {}),
            add={
                "label": "Add endpoint",
                "action": event(OPEN_ADD, {"token": tokens.mint(OPEN_ADD, ADD_SUBJECT)}),
            },
            empty="No endpoints yet.",
        )
    )
    for view in views:
        detail = _detail(tree, view, state, forms, tokens)
        if detail:
            children.append(detail)
    tree.column("root", children)
    data: dict[str, Any] = {
        "drafts": {
            view.site.id: {
                **edit_draft(view.site, forms, state.pending.get(view.site.id, "")),
                "valid": valid_map(state.errors.get(view.site.id, {}), EDIT_ERROR_PREFIXES),
            }
            for view in views
            if view.site.id in state.editing
        },
    }
    return tree.components, data
