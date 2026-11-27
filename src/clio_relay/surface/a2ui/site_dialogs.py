"""The page's other dialogs: editing an endpoint, and signing in."""

from __future__ import annotations

from typing import Any

from clio_relay.surface.a2ui.components import DIALOG, Tree, bound, event, open_url
from clio_relay.surface.a2ui.editform import edit_draft, edit_form, identity_rows, settings_rows
from clio_relay.surface.a2ui.forms import FORM_ERROR, Form, valid_map
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.surfaces import (
    CANCEL,
    SAVE_SITE,
    SiteView,
    TabState,
)
from clio_relay.surface.a2ui.tokens import ActionTokens


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
