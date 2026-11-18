"""The Compute page's endpoint table, for a client that renders relay's catalog (RL32).

The table is one ``clio.choice-table.v1`` component, laid out like CLIO's Tools
settings. A header row holds the defaults for new endpoints, then there is one
row per endpoint. Each row has inline Protocol and Security pickers, its Status,
and a menu of Probe, Edit and Remove. Picking a value calls
:data:`~clio_relay.surface.a2ui.surfaces.SET_VALUE` with the row's token, and the
host adds the column and value. An open edit form or remove confirmation is
rendered as Basic components under its row, through the row's ``detail``.

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
    SiteView,
    TabState,
    edit_draft,
    edit_form,
    identity_rows,
    probe_text,
    protocol_name,
    protocol_order,
    settings_rows,
)
from clio_relay.surface.a2ui.tokens import ActionTokens

_SECURITY_LOOK = {"guarded": ("lock", "positive"), "open": ("lockOpen", "warning")}
_STATUS = {
    "ready": ("Ready", "Answers and needs nothing from you", "check", "positive"),
    "needs_sign_in": ("Needs a sign-in", "Connect it from the row's menu", "warning", "warning"),
    "unavailable": ("Unavailable", "Relay cannot reach it now", "error", "negative"),
}


def _columns(forms: dict[str, Form], views: list[SiteView]) -> list[dict[str, Any]]:
    listed = [name for name, item in forms.items() if item.listed]
    names = protocol_order([*listed, *(view.site.adapter for view in views)])
    protocols: list[dict[str, Any]] = []
    for name in names:
        option: dict[str, Any] = {"value": name, "label": protocol_name(name)}
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
    """Add what shows under a row: a sign-in link, a remove confirmation, the edit form."""
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
        children += identity_rows(tree, base, view)
        children += settings_rows(tree, base, view, security=False)
        errors = state.errors.get(site.id, {})
        pending = state.pending.get(site.id, "")
        children.append(edit_form(tree, site, forms, tokens, errors, pending))
    if not children:
        return None
    return tree.column(f"{base}-detail", children)


def _row(
    tree: Tree, view: SiteView, state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> dict[str, Any]:
    site = view.site
    values = {"protocol": site.adapter, "security": site.policy}
    status = str(view.status.get("state", ""))
    if status in _STATUS:
        values["status"] = status
    row: dict[str, Any] = {
        "id": site.id,
        "title": site.title,
        "values": values,
        "context": {"token": tokens.mint(SET_VALUE, site.id)},
        "menu": _menu(view, state, tokens),
    }
    if site.address:
        row["subtitle"] = site.address
    caption = _caption(view)
    if caption:
        row["caption"] = caption
    error = state.errors.get(site.id, {}).get(FORM_ERROR)
    if error and site.id not in state.editing:
        row["error"] = error
    detail = _detail(tree, view, state, forms, tokens)
    if detail:
        row["detail"] = detail
    return row


def endpoints_surface(
    views: list[SiteView], state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the endpoints surface as one table, and its data model."""
    tree = Tree()
    children = [tree.text("sites-title", "Endpoints", "h2")]
    if state.notice:
        children.append(tree.text("sites-notice", state.notice))
    rows =  [_row(tree, view, state, forms, tokens) for view in views]
    children.append(
        tree.add(
            "endpoints",
            TABLE,
            label="Endpoints",
            columns=_columns(forms, views),
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
