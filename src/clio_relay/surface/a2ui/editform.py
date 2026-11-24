"""The edit form for one endpoint, and the facts a card or the edit dialog shows about it."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui.components import Tree, bound, event
from clio_relay.surface.a2ui.forms import Form, draft, field_rows, form_error
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.surfaces import (
    CANCEL,
    EDIT_FIELDS,
    SAVE_SITE,
    SIGN_IN_STATES,
    SiteView,
    duration_text,
    protocol_name,
    protocol_order,
    security_name,
    spend_draft,
    spend_text,
)
from clio_relay.surface.a2ui.tokens import ActionTokens


def _common_draft(site: SiteEntry) -> dict[str, Any]:
    values: dict[str, Any] = {
        "name": site.name,
        "address": site.address,
        "policy": site.policy,
        **spend_draft(site.spend.ask),
        "skill": site.skill,
        "account": site.defaults.account,
        "queue": site.defaults.queue,
    }
    return draft(EDIT_FIELDS, values)


def _edit_order(site: SiteEntry, forms: dict[str, Form], pending: str = "") -> list[str]:
    order = [site.adapter, *site.alternatives]
    if pending and pending not in order:
        order.insert(0, pending)  # the tab that opens first is the one to fill in
    rest = [name for name, item in forms.items() if item.listed and name not in order]
    return order + protocol_order(rest)


def edit_draft(site: SiteEntry, forms: dict[str, Form], pending: str = "") -> dict[str, Any]:
    """Return the edit form's starting draft for ``site``.

    ``pending`` is a protocol picked for it that needs settings first, so it starts in use.
    """
    adapters: dict[str, Any] = {}
    for name in _edit_order(site, forms):
        adapter_form = forms.get(name)
        if adapter_form is None:
            continue
        config = site.config if name == site.adapter else site.alternatives.get(name, {})
        use = name in site.alternatives or name == pending
        adapters[name] = {"use": use, **draft(adapter_form.fields, config)}
    return {"site": _common_draft(site), "adapters": adapters}


def edit_form(
    tree: Tree,
    site: SiteEntry,
    forms: dict[str, Form],
    tokens: ActionTokens,
    errors: dict[str, str],
    pending: str = "",
    facts: Sequence[str] = (),
    in_dialog: bool = False,
) -> str:
    """Add one endpoint's edit form and return its id; ``facts`` go under its heading.

    ``in_dialog`` leaves out the heading and the buttons, which the dialog holds.
    """
    base = f"e-{site.id}"
    path = f"/drafts/{site.id}"
    children = [] if in_dialog else [tree.text(f"{base}-title", f"Edit {site.title}", "h4")]
    children += facts
    children += form_error(tree, base, errors)
    children += field_rows(tree, f"{base}-s", EDIT_FIELDS, f"{path}/site", errors, "site.", path)
    children.append(tree.text(f"{base}-ways", _("Protocols"), "h5"))
    tabs: list[dict[str, str]] = []
    for name in _edit_order(site, forms, pending):
        adapter_form = forms.get(name)
        if adapter_form is None:
            continue
        tab_base = f"{base}-a-{name}"
        adapter_path = f"{path}/adapters/{name}"
        rows: list[str] = []
        if name == site.adapter:
            rows.append(tree.text(f"{tab_base}-inuse", _("In use"), "caption"))
        else:
            rows.append(
                tree.add(
                    f"{tab_base}-use",
                    "CheckBox",
                    label=_("Also reach it this way"),
                    value=bound(f"{adapter_path}/use"),
                )
            )
        rows += field_rows(
            tree, tab_base, adapter_form.fields, adapter_path, errors, f"adapters.{name}.", path
        )
        if not adapter_form.fields:
            rows.append(tree.text(f"{tab_base}-none", _("No settings"), "caption"))
        title = protocol_name(name)
        tabs.append({"title": title, "child": tree.column(f"{tab_base}-col", rows)})
    children.append(tree.add(f"{base}-tabs", "Tabs", tabs=tabs))
    if in_dialog:
        return tree.column(base, children)
    save = tree.button(
        f"{base}-save",
        "Save",
        event(SAVE_SITE, {"token": tokens.mint(SAVE_SITE, site.id), "values": bound(path)}),
        "primary",
    )
    cancel = tree.button(
        f"{base}-cancel", "Cancel", event(CANCEL, {"token": tokens.mint(CANCEL, site.id)})
    )
    children.append(tree.row(f"{base}-buttons", [save, cancel], justify="start"))
    return tree.column(base, children)


def identity_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    """Add the endpoint's identity and sign-in facts."""
    identity = view.identity
    if identity is None:
        return []
    rows = [tree.fact(f"{base}-identity", "Identity", str(identity.get("title") or ""))]
    state = str(identity.get("state") or "")
    shown = SIGN_IN_STATES.get(state, state)
    if state == "signed_in" and identity.get("account"):
        shown = f"Signed in as {identity['account']}"
    rows.append(tree.fact(f"{base}-signin", "Sign-in", shown))
    return rows


def settings_rows(tree: Tree, base: str, view: SiteView, fields: bool = True) -> list[str]:
    """Add the endpoint's settings as facts.

    ``fields`` False leaves out the settings the edit form shows as fields.
    """
    site = view.site
    rows: list[str] = []
    if fields:
        rows.append(tree.fact(f"{base}-policy", "Security", security_name(site.policy)))
    if view.reviewed is not None and site.policy == "guarded":
        rows.append(tree.fact(f"{base}-reviewed", "Reviewed packages", str(view.reviewed)))
    defaults = site.defaults
    if fields:
        rows.append(tree.fact(f"{base}-spend", "Spending", spend_text(site.spend.ask)))
        if defaults.account:
            rows.append(tree.fact(f"{base}-account", "Default account", defaults.account))
        if defaults.queue:
            rows.append(tree.fact(f"{base}-queue", "Default queue", defaults.queue))
    if defaults.duration is not None:
        rows.append(
            tree.fact(f"{base}-duration", "Default duration", duration_text(defaults.duration))
        )
    if view.has_system_id:
        shown = view.system_id or "Relay registers one after you sign in"
        rows.append(tree.fact(f"{base}-system", "System ID", shown))
    if view.skill:
        rows.append(tree.fact(f"{base}-skill", "Skill", str(view.skill.get("linked") or "")))
        note = view.skill.get("uri") or view.skill.get("detail")
        if note:
            rows.append(tree.text(f"{base}-skill-uri", str(note), "caption"))
    return rows
