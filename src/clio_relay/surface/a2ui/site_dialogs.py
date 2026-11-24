"""The page's other dialogs: defaults for new endpoints, editing an endpoint, and signing in."""

from __future__ import annotations

from typing import Any

from clio_relay.surface.a2ui.components import DIALOG, Tree, bound, event, open_url
from clio_relay.surface.a2ui.dialog import (
    DEFAULTS_DRAFT,
)
from clio_relay.surface.a2ui.editform import edit_draft, edit_form, identity_rows, settings_rows
from clio_relay.surface.a2ui.forms import FORM_ERROR, Field, Form, field_rows, valid_map
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.surfaces import (
    CANCEL,
    CLOSE_DIALOG,
    DEFAULTS_DIALOG,
    DEFAULTS_SUBJECT,
    OPEN_DIALOG,
    SAVE_DEFAULTS,
    SAVE_SITE,
    SPEND_LIMIT_FIELD,
    SiteView,
    TabState,
    protocol_name,
    protocol_order,
)
from clio_relay.surface.a2ui.tokens import ActionTokens


def _defaults_fields(forms: dict[str, Form]) -> tuple[Field, ...]:
    from clio_relay.surface.a2ui.surfaces import POLICY, SPEND

    listed = protocol_order(name for name, item in forms.items() if item.listed)
    protocol = Field(
        "protocol",
        "Protocol",
        "choice",
        help="For a computer of your own. A catalog site starts with the one it suggests.",
        options=tuple(listed),
        labels=tuple(protocol_name(name) for name in listed),
    )
    spend = Field(
        SPEND.key,
        SPEND.label,
        "choice",
        help="Applies to endpoints you add later",
        options=SPEND.options,
        labels=SPEND.labels,
    )
    return (protocol, POLICY, spend, SPEND_LIMIT_FIELD)


def defaults_dialog(
    tree: Tree, state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> str:
    """Add the gear's dialog: the protocol, Security and Spending new endpoints start with."""
    errors = state.errors.get(DEFAULTS_DIALOG, {})
    rows = field_rows(
        tree, "def", _defaults_fields(forms), DEFAULTS_DRAFT, errors, "", DEFAULTS_DRAFT
    )
    content = tree.column("defaults-body", rows)
    trigger = tree.icon_button(
        "defaults-open",
        "settings",
        _("Defaults for new endpoints"),
        event(OPEN_DIALOG, {"token": tokens.mint(OPEN_DIALOG, DEFAULTS_SUBJECT)}),
    )
    status = errors.get(FORM_ERROR)
    return tree.add(
        "defaults-dialog",
        DIALOG,
        title=_("Defaults for new endpoints"),
        open=bound("/dialogs/defaults"),
        trigger=trigger,
        content=content,
        width="narrow",
        cancel={"label": _("Cancel")},
        confirm={
            "label": _("Save"),
            "action": event(
                SAVE_DEFAULTS,
                {
                    "token": tokens.mint(SAVE_DEFAULTS, DEFAULTS_SUBJECT),
                    "values": bound(DEFAULTS_DRAFT),
                },
            ),
        },
        onClose=event(CLOSE_DIALOG, {"token": tokens.mint(CLOSE_DIALOG, DEFAULTS_SUBJECT)}),
        **({"status": {"tone": "negative", "text": status}} if status else {}),
    )


def edit_dialog(
    tree: Tree, view: SiteView, state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> str:
    """Add one endpoint's edit dialog: its facts, settings and protocols."""
    site = view.site
    base = f"s-{site.id}"
    facts = identity_rows(tree, base, view) + settings_rows(tree, base, view, fields=False)
    errors = state.errors.get(site.id, {})
    pending = state.pending.get(site.id, "")
    content = edit_form(tree, site, forms, tokens, errors, pending, facts, in_dialog=True)
    path = f"/drafts/{site.id}"
    status = errors.get(FORM_ERROR)
    return tree.add(
        "edit-dialog",
        DIALOG,
        title=_("Edit {name}", name=site.title),
        open=bound("/dialogs/edit"),
        content=content,
        cancel={"label": _("Cancel")},
        confirm={
            "label": _("Save"),
            "action": event(
                SAVE_SITE, {"token": tokens.mint(SAVE_SITE, site.id), "values": bound(path)}
            ),
        },
        onClose=event(CANCEL, {"token": tokens.mint(CANCEL, site.id)}),
        **({"status": {"tone": "negative", "text": status}} if status else {}),
    )


def edit_data(view: SiteView, state: TabState, forms: dict[str, Form]) -> dict[str, Any]:
    from clio_relay.surface.a2ui.surfaces import EDIT_ERROR_PREFIXES

    site = view.site
    return {
        **edit_draft(site, forms, state.pending.get(site.id, "")),
        "valid": valid_map(state.errors.get(site.id, {}), EDIT_ERROR_PREFIXES),
    }


def sign_in_dialog(tree: Tree, view: SiteView, url: str, tokens: ActionTokens) -> str:
    """Add the dialog that sends the person to an endpoint's sign-in page."""
    site = view.site
    text = tree.text(
        "signin-text", _("Sign in to {name} in your browser, then come back here.", name=site.title)
    )
    button = tree.button("signin-open", _("Open the sign-in page"), open_url(url), "primary")
    return tree.add(
        "signin-dialog",
        DIALOG,
        title=_("Sign in to {name}", name=site.title),
        open=bound("/dialogs/signin"),
        content=tree.column("signin-body", [text, button]),
        width="narrow",
        cancel={"label": _("Close")},
        onClose=event(CANCEL, {"token": tokens.mint(CANCEL, site.id)}),
    )
