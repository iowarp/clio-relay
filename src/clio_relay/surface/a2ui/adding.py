"""The Compute tab's add-site surface (RL32, RL50).

Like :mod:`clio_relay.surface.a2ui.surfaces`, this builder is pure: it mints one
action token per button and returns components and a data model.
"""

from __future__ import annotations

from typing import Any

from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui.components import Tree, bound, event
from clio_relay.surface.a2ui.forms import Form, draft, field_rows, form_error
from clio_relay.surface.a2ui.surfaces import (
    ADD_FIELDS,
    ADD_FROM_CATALOG,
    ADD_SITE,
    ADD_SURFACE,
    TabState,
)
from clio_relay.surface.a2ui.tokens import ActionTokens

def add_draft(forms: dict[str, Form]) -> dict[str, Any]:
    """Return the add form's empty draft."""
    return {
        "site": draft(ADD_FIELDS, {"policy": "guarded", "spend": "always"}),
        "config": {name: draft(item.fields, {}) for name, item in forms.items() if item.listed},
    }


def add_surface(
    forms: dict[str, Form],
    catalog: list[SiteEntry],
    state: TabState,
    tokens: ActionTokens,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the add-site surface's components and data model."""
    tree = Tree()
    errors = state.errors.get(ADD_SURFACE, {})
    children = [tree.text("add-title", "Add a site", "h2")]
    if catalog:
        children.append(tree.text("add-catalog-title", "From the catalog", "h4"))
        children.append(
            tree.add(
                "add-catalog-site",
                "ChoicePicker",
                label="Site",
                options=[{"label": entry.title, "value": entry.id} for entry in catalog],
                value=bound("/catalog/id"),
                variant="mutuallyExclusive",
                displayStyle="chips",
            )
        )
        children.append(
            tree.add(
                "add-catalog-id",
                "TextField",
                label="Site ID (optional)",
                value=bound("/catalog/site_id"),
                variant="shortText",
            )
        )
        message = errors.get("catalog")
        if message:
            children.append(tree.text("add-catalog-error", message, "caption"))
        children.append(
            tree.button(
                "add-catalog",
                "Add from catalog",
                event(
                    ADD_FROM_CATALOG,
                    {"token": tokens.mint(ADD_FROM_CATALOG), "values": bound("/catalog")},
                ),
                "primary",
            )
        )
        children.append(tree.divider("add-divider"))
    children.append(tree.text("add-own-title", "Your own", "h4"))
    children += form_error(tree, "add", errors)
    children += field_rows(tree, "add-s", ADD_FIELDS, "/drafts/add/site", errors, "site.")
    children.append(tree.text("add-ways", "How relay reaches it", "h5"))
    tabs: list[dict[str, str]] = []
    for name, adapter_form in forms.items():
        if not adapter_form.listed:
            continue
        base = f"add-a-{name}"
        rows = field_rows(
            tree,
            base,
            adapter_form.fields,
            f"/drafts/add/config/{name}",
            errors,
            f"config.{name}.",
        )
        if not adapter_form.fields:
            rows.append(tree.text(f"{base}-none", "No settings", "caption"))
        rows.append(
            tree.button(
                f"{base}-add",
                "Add site",
                event(
                    ADD_SITE, {"token": tokens.mint(ADD_SITE, name), "values": bound("/drafts/add")}
                ),
                "primary",
            )
        )
        tabs.append({"title": name, "child": tree.column(f"{base}-col", rows)})
    if tabs:
        children.append(tree.add("add-tabs", "Tabs", tabs=tabs))
    tree.column("root", children)
    data: dict[str, Any] = {
        "catalog": {"id": [catalog[0].id] if catalog else [], "site_id": ""},
        "drafts": {"add": add_draft(forms)},
    }
    return tree.components, data

