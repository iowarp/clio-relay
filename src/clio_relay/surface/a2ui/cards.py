"""The Compute page as endpoint cards, for a client that renders only the Basic catalog (RL32).

A client that renders relay's own catalog gets the endpoint table of
:mod:`clio_relay.surface.a2ui.table` instead.
"""

from __future__ import annotations

from typing import Any

from clio_relay.surface.a2ui.components import Tree, bound, event, open_url
from clio_relay.surface.a2ui.forms import Form, form_error, valid_map
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
    REMOVE_SITE,
    SECURITY,
    SECURITY_HELP,
    SET_VALUE,
    SPEND_HELP,
    SWITCH_ADAPTER,
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
    status_text,
)
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.a2ui.values import spend_ask


def _status_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    rows = [tree.fact(f"{base}-status", "Status", status_text(view))]
    detail = view.status.get("detail")
    if detail and view.status.get("state") != "ready":
        rows.append(tree.text(f"{base}-status-detail", str(detail), "caption"))
    if view.probe:
        state = str(view.probe.get("state", ""))
        rows.append(tree.fact(f"{base}-probe", "Last probe", probe_text(state)))
        if view.probe.get("detail"):
            rows.append(tree.text(f"{base}-probe-detail", str(view.probe["detail"]), "caption"))
    return rows


def _switch_rows(tree: Tree, base: str, view: SiteView, tokens: ActionTokens) -> list[str]:
    site = view.site
    if not site.alternatives:
        return []
    names = [site.adapter, *site.alternatives]
    options = [{"label": protocol_name(name), "value": name} for name in names]
    picker = tree.add(
        f"{base}-adapter",
        "ChoicePicker",
        label="Protocol in use",
        options=options,
        value=bound(f"/switch/{site.id}/adapter"),
        variant="mutuallyExclusive",
        displayStyle="chips",
    )
    use = tree.button(
        f"{base}-switch",
        "Use this protocol",
        event(
            SWITCH_ADAPTER,
            {"token": tokens.mint(SWITCH_ADAPTER, site.id), "values": bound(f"/switch/{site.id}")},
        ),
    )
    return [picker, use]


def _connect_button(tree: Tree, base: str, view: SiteView, tokens: ActionTokens) -> str | None:
    site = view.site
    signed_out = view.identity is not None and view.identity.get("state") != "signed_in"
    if view.status.get("state") != "needs_sign_in" and not signed_out:
        return None
    if view.host_sign_in:
        action = event(f"{HOST_SIGN_IN}{view.host_sign_in}", {})
    else:
        action = event(CONNECT_SITE, {"token": tokens.mint(CONNECT_SITE, site.id)})
    return tree.button(f"{base}-connect", "Connect", action, "primary")


def protocol_options(forms: dict[str, Form], first: str = "") -> list[dict[str, str]]:
    """Return the protocols a new endpoint can use, as picker options."""
    names = protocol_order([name for name, item in forms.items() if item.listed], first)
    return [{"label": protocol_name(name), "value": name} for name in names]


def _defaults_section(
    tree: Tree, views: list[SiteView], state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> str:
    title = tree.text("defaults-title", "Defaults for new endpoints", "h4")
    protocol = tree.add(
        "defaults-protocol",
        "ChoicePicker",
        label="Protocol",
        options=protocol_options(forms),
        value=bound("/defaults/protocol"),
        variant="mutuallyExclusive",
        displayStyle="chips",
    )
    security = tree.add(
        "defaults-security",
        "ChoicePicker",
        label="Security",
        options=[{"label": label, "value": value} for value, (label, _) in SECURITY.items()],
        value=bound("/defaults/security"),
        variant="mutuallyExclusive",
        displayStyle="chips",
    )
    help_text = tree.text("defaults-security-help", SECURITY_HELP, "caption")
    used = [view.site.spend.ask for view in views] + [spend_ask(state.default("spend"))]
    spend = tree.add(
        "defaults-spend",
        "ChoicePicker",
        label="Spending",
        options=[
            {"label": option["label"], "value": option["value"]} for option in spend_options(used)
        ],
        value=bound("/defaults/spend"),
        variant="mutuallyExclusive",
        displayStyle="chips",
    )
    spend_help = tree.text("defaults-spend-help", SPEND_HELP, "caption")
    save = tree.button(
        "defaults-save",
        "Save defaults",
        event(
            SET_VALUE,
            {"token": tokens.mint(SET_VALUE, DEFAULTS_SUBJECT), "values": bound("/defaults")},
        ),
    )
    return tree.column("defaults", [title, protocol, security, help_text, spend, spend_help, save])


def site_card(
    tree: Tree,
    view: SiteView,
    state: TabState,
    forms: dict[str, Form],
    tokens: ActionTokens,
) -> str:
    """Add one site's card and return its id."""
    site = view.site
    base = f"s-{site.id}"
    children = [tree.text(f"{base}-title", site.title, "h3")]
    if site.address:
        children.append(tree.text(f"{base}-address", site.address, "caption"))
    protocol = protocol_name(site.adapter)
    children.append(tree.fact(f"{base}-adapter-in-use", "Protocol", protocol))
    children += _switch_rows(tree, base, view, tokens)
    children += identity_rows(tree, base, view)
    children += _status_rows(tree, base, view)
    children += settings_rows(tree, base, view)
    link = state.links.get(site.id)
    if link:
        children.append(tree.button(f"{base}-open", "Open the sign-in page", open_url(link)))
    errors = state.errors.get(site.id, {})
    if site.id not in state.editing:
        children += form_error(tree, base, errors)
    buttons: list[str] = []
    connect = _connect_button(tree, base, view, tokens)
    if connect:
        buttons.append(connect)
    buttons.append(
        tree.button(
            f"{base}-probe-button",
            "Probe",
            event(PROBE_SITE, {"token": tokens.mint(PROBE_SITE, site.id)}),
        )
    )
    if site.id not in state.editing:
        buttons.append(
            tree.button(
                f"{base}-edit", "Edit", event(EDIT_SITE, {"token": tokens.mint(EDIT_SITE, site.id)})
            )
        )
    buttons.append(
        tree.button(
            f"{base}-remove",
            "Remove",
            event(REMOVE_SITE, {"token": tokens.mint(REMOVE_SITE, site.id)}),
            "borderless",
        )
    )
    children.append(tree.row(f"{base}-buttons", buttons, justify="start"))
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
        children.append(question)
        children.append(tree.row(f"{base}-confirm", [yes, no], justify="start"))
    if site.id in state.editing:
        children.append(tree.divider(f"{base}-edit-divider"))
        pending = state.pending.get(site.id, "")
        children.append(edit_form(tree, site, forms, tokens, errors, pending))
    return tree.card(base, tree.column(f"{base}-body", children))


def sites_surface(
    views: list[SiteView], state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the endpoints surface as Basic components: a card per endpoint."""
    tree = Tree()
    add = tree.button(
        "sites-add",
        "Add endpoint",
        event(OPEN_ADD, {"token": tokens.mint(OPEN_ADD, ADD_SUBJECT)}),
        "primary",
    )
    title = tree.text("sites-title", "Endpoints", "h2")
    children = [tree.row("sites-header", [title, add], justify="spaceBetween", align="center")]
    if state.notice:
        children.append(tree.text("sites-notice", state.notice))
    children.append(_defaults_section(tree, views, state, forms, tokens))
    if not views:
        children.append(tree.text("sites-empty", "No endpoints yet."))
    for view in views:
        children.append(site_card(tree, view, state, forms, tokens))
    tree.column("root", children)
    data: dict[str, Any] = {
        "defaults": {column: [state.default(column)] for column in DEFAULTS},
        "switch": {view.site.id: {"adapter": [view.site.adapter]} for view in views},
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
