"""The Compute page's dialogs: add an endpoint, the defaults, edit one, sign in (RL32).

Each is one ``clio.dialog.v1`` whose ``open`` is bound under ``/dialogs``. The
add dialog has three tabs (Catalog, Saved, Your own), each ending in Test. A
test that passes leads to the Save step: the name, Security and Spending, with
the defaults marked. Save stays off until the test passes on what is shown.
Every control calls one of the page's tools with a single-use token, as the
rest of the page does; the dialog's data lives under ``/add``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from clio_relay.catalog import CatalogSite
from clio_relay.surface.a2ui.components import DIALOG, PICKER, Tree, bound, event, open_url
from clio_relay.surface.a2ui.forms import FORM_ERROR, Field, Form, draft, field_rows, valid_map
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.newsite import (
    CATALOG_TAB,
    OWN_FIELDS,
    OWN_TAB,
    SAVE_FIELDS,
    SAVED_TAB,
    SavedItem,
    draft_key,
    shown_fields,
)
from clio_relay.surface.a2ui.surfaces import (
    ADD_DIALOG,
    ADD_SUBJECT,
    BACK,
    CLOSE_DIALOG,
    DEFAULTS_DIALOG,
    EDIT_DIALOG,
    OPEN_DIALOG,
    PICK,
    PROTOCOLS,
    SAVE_ENDPOINT,
    SECURITY,
    SECURITY_HELP,
    SIGN_IN_DIALOG,
    SPEND_PRESETS,
    TEST_ENDPOINT,
    TabState,
    protocol_name,
    protocol_order,
    spend_draft,
)
from clio_relay.surface.a2ui.tokens import ActionTokens
from clio_relay.surface.a2ui.values import as_dict

ADD = "/add"
DEFAULTS_DRAFT = "/defaultsDraft"
ADD_ERRORS = ("site.", "config.", "own.")
_TAB_ICONS = {CATALOG_TAB: "book", SAVED_TAB: "bookmark", OWN_TAB: "link"}
_SITES_SHOWN = 4
"""Sites the Catalog tab lists before "N more", until the person searches."""
_STEPS = ("Site", "Protocol", "Test", "Save")


@dataclass
class DialogInputs:
    """What the dialogs show besides the endpoints: the catalog and the Saved tab."""

    sites: list[CatalogSite] = field(default_factory=list[CatalogSite])
    saved: list[SavedItem] = field(default_factory=list[SavedItem])


def dialog_flags(state: TabState) -> dict[str, bool]:
    """Return ``/dialogs``: which dialog is open."""
    return {
        "add": state.dialog == ADD_DIALOG,
        "defaults": state.dialog == DEFAULTS_DIALOG,
        "edit": state.dialog.startswith(EDIT_DIALOG),
        "signin": state.dialog.startswith(SIGN_IN_DIALOG),
    }


def _draft(state: TabState) -> dict[str, Any]:
    return as_dict(state.add.get("draft"))


def _test(state: TabState) -> dict[str, Any]:
    return as_dict(state.add.get("test"))


def passed(state: TabState) -> bool:
    """Return whether the last test passed on the draft as it is now."""
    test = _test(state)
    return test.get("state") == "ready" and test.get("key") == draft_key(_draft(state))


def add_data(state: TabState, forms: dict[str, Form]) -> dict[str, Any]:
    """Return ``/add``: the draft the dialog binds to, with every field it shows."""
    current = _draft(state)
    data: dict[str, Any] = {
        "tab": str(current.get("tab") or CATALOG_TAB),
        "site": str(current.get("site") or ""),
        "saved": str(current.get("saved") or ""),
        "protocol": str(current.get("protocol") or ""),
        "ownProtocol": str(current.get("ownProtocol") or ""),
    }
    data["own"] = draft(OWN_FIELDS, as_dict(current.get("own")))
    configs = as_dict(current.get("config"))
    data["config"] = {}
    for name in {data["protocol"], data["ownProtocol"], *configs}:
        if not name:
            continue
        fields = shown_fields(forms.get(name))
        data["config"][name] = draft(fields, as_dict(configs.get(name)))
    data["save"] = draft(SAVE_FIELDS, as_dict(current.get("save")))
    errors = state.errors.get(ADD_DIALOG, {})
    data["valid"] = valid_map(errors, ADD_ERRORS)
    return data


def defaults_data(state: TabState) -> dict[str, Any]:
    """Return ``/defaultsDraft``: the defaults dialog's fields, from the saved defaults."""
    from clio_relay.surface.a2ui.values import spend_ask

    return {
        "protocol": [state.default("protocol")],
        "policy": [state.default("security")],
        **spend_draft(spend_ask(state.default("spend"))),
    }


# The add dialog


def _pick_action(tokens: ActionTokens, column: str) -> dict[str, Any]:
    return event(
        PICK, {"token": tokens.mint(PICK, ADD_SUBJECT), "column": column, "values": bound(ADD)}
    )


def _protocol_fields(
    tree: Tree, base: str, forms: dict[str, Form], adapter: str, errors: dict[str, str]
) -> list[str]:
    fields = shown_fields(forms.get(adapter))
    return field_rows(
        tree, base, fields, f"{ADD}/config/{adapter}", errors, f"config.{adapter}.", ADD
    )


def _catalog_tab(
    tree: Tree,
    state: TabState,
    forms: dict[str, Form],
    tokens: ActionTokens,
    inputs: DialogInputs,
) -> str:
    current = _draft(state)
    errors = state.errors.get(ADD_DIALOG, {})
    items = [
        {
            "value": site.id,
            "title": _(site.name),
            **({"subtitle": _(site.summary)} if site.summary else {}),
            "chips": [protocol_name(way.adapter) for way in site.ways],
        }
        for site in inputs.sites
    ]
    children = [
        tree.add(
            "add-site",
            PICKER,
            label=_("Site"),
            search=_("Search sites"),
            items=items,
            value=bound(f"{ADD}/site"),
            limit=_SITES_SHOWN,
            onPick=_pick_action(tokens, "site"),
            empty=_("No sites match"),
            error=errors.get("add.site"),
        )
    ]
    chosen = next((site for site in inputs.sites if site.id == current.get("site")), None)
    if chosen is not None:
        way = chosen.way(str(current.get("protocol") or "") or None)
        if len(chosen.ways) > 1:
            ways = [
                {
                    "value": item.adapter,
                    "title": protocol_name(item.adapter),
                    "subtitle": _(item.note)
                    if item.note
                    else _(PROTOCOLS[item.adapter][1])
                    if item.adapter in PROTOCOLS
                    else "",
                    **({"meta": _("Suggested")} if index == 0 else {}),
                }
                for index, item in enumerate(chosen.ways)
            ]
            children.append(
                tree.add(
                    "add-protocol",
                    PICKER,
                    label=_("Protocol"),
                    items=ways,
                    value=bound(f"{ADD}/protocol"),
                    onPick=_pick_action(tokens, "protocol"),
                )
            )
        children += _protocol_fields(tree, "add-c", forms, way.adapter, errors)
    return tree.column(CATALOG_TAB, children)


def _saved_tab(tree: Tree, state: TabState, tokens: ActionTokens, inputs: DialogInputs) -> str:
    errors = state.errors.get(ADD_DIALOG, {})
    items = [
        {
            "value": item.key,
            "title": item.title,
            **({"subtitle": item.subtitle} if item.subtitle else {}),
            "chips": [protocol_name(item.adapter)],
            "group": item.group,
        }
        for item in inputs.saved
    ]
    picker = tree.add(
        "add-saved-list",
        PICKER,
        label=_("Computer"),
        items=items,
        value=bound(f"{ADD}/saved"),
        onPick=_pick_action(tokens, "saved"),
        empty=_(
            "Nothing saved yet. SSH hosts you save in CLIO, hosts in ~/.ssh/config "
            "and endpoints you remove show here."
        ),
        error=errors.get("add.saved"),
    )
    return tree.column(SAVED_TAB, [picker])


def _own_tab(tree: Tree, state: TabState, forms: dict[str, Form], tokens: ActionTokens) -> str:
    current = _draft(state)
    errors = state.errors.get(ADD_DIALOG, {})
    children = field_rows(tree, "add-o", OWN_FIELDS, f"{ADD}/own", errors, "own.", ADD)
    listed = protocol_order(name for name, item in forms.items() if item.listed)
    names = [protocol_name(name) for name in listed if name != "local"]
    children.append(
        tree.add(
            "add-own-protocol",
            PICKER,
            label=_("Protocol"),
            hint=_("{names} or {last}", names=", ".join(names[:-1]), last=names[-1])
            if len(names) > 1
            else None,
            variant="select",
            items=[
                {
                    "value": name,
                    "title": protocol_name(name),
                    **({"subtitle": _(PROTOCOLS[name][1])} if name in PROTOCOLS else {}),
                }
                for name in listed
            ],
            value=bound(f"{ADD}/ownProtocol"),
            onPick=_pick_action(tokens, "ownProtocol"),
            error=errors.get("add.ownProtocol"),
        )
    )
    adapter = str(current.get("ownProtocol") or "")
    if adapter:
        children += _protocol_fields(tree, "add-of", forms, adapter, errors)
    return tree.column(OWN_TAB, children)


def _save_fields(tree: Tree, state: TabState) -> list[str]:
    errors = state.errors.get(ADD_DIALOG, {})
    fields = tuple(_marked(item, state) for item in SAVE_FIELDS)
    return field_rows(tree, "add-s", fields, f"{ADD}/save", errors, "site.", ADD)


def _marked(item: Field, state: TabState) -> Field:
    """Return a Security or Spending field with its default option marked (default)."""
    from dataclasses import replace

    from clio_relay.surface.a2ui.values import spend_ask

    if item.key == "policy":
        default = state.default("security")
    elif item.key == "spend":
        ask = spend_ask(state.default("spend"))
        default = "always" if ask == "always" else "never" if ask == "never" else "above"
    else:
        return item
    shown = item.labels or item.options
    labels = tuple(
        _("{label} (default)", label=_(label)) if value == default else label
        for value, label in zip(item.options, shown, strict=True)
    )
    return replace(item, labels=labels)


def _status(state: TabState) -> dict[str, Any] | None:
    test = _test(state)
    errors = state.errors.get(ADD_DIALOG, {})
    message = errors.get(FORM_ERROR) or errors.get("add")
    if message:
        return {"tone": "negative", "text": message}
    if not test.get("line"):
        return None
    tone = {
        "ready": "positive",
        "needs_sign_in": "warning",
        "missing_allocation": "warning",
    }.get(str(test.get("state")), "negative")
    status: dict[str, Any] = {"tone": tone, "text": str(test["line"])}
    if test.get("detail"):
        status["detail"] = str(test["detail"])
    if test.get("url"):
        status["action"] = {"label": _("Sign in"), "action": open_url(str(test["url"]))}
    elif test.get("host_sign_in"):
        status["action"] = {
            "label": _("Sign in"),
            "action": event(str(test["host_sign_in"]), {}),
        }
    return status


def _facts(raw: Any) -> list[tuple[str, str]]:
    """Return the test view's facts, as the state file keeps them: label and value pairs."""
    found: list[tuple[str, str]] = []
    for item in cast(list[Any], raw) if isinstance(raw, list) else []:
        pair = cast(list[Any], item) if isinstance(item, (list, tuple)) else []
        if len(pair) == 2 and isinstance(pair[0], str) and isinstance(pair[1], str) and pair[1]:
            found.append((pair[0], pair[1]))
    return found


def add_dialog(
    tree: Tree,
    state: TabState,
    forms: dict[str, Form],
    tokens: ActionTokens,
    inputs: DialogInputs,
    trigger: str | None,
) -> str:
    """Add the add-endpoint dialog and return its id."""
    test = _test(state)
    testing = state.add.get("view") == "test"
    ok = passed(state)
    steps = [_(step) for step in _STEPS]
    test_action = event(
        TEST_ENDPOINT, {"token": tokens.mint(TEST_ENDPOINT, ADD_SUBJECT), "values": bound(ADD)}
    )
    props: dict[str, Any] = {
        "description": _("Pick a site, then how relay reaches it. Test it, then save it."),
        "test": {
            "label": _("Test again") if test.get("line") else _("Test"),
            "action": test_action,
        },
        "confirm": {
            "label": _("Save endpoint"),
            "action": event(
                SAVE_ENDPOINT,
                {"token": tokens.mint(SAVE_ENDPOINT, ADD_SUBJECT), "values": bound(ADD)},
            ),
            "enabled": ok,
        },
        "status": _status(state),
    }
    if testing:
        facts = [
            tree.fact(f"add-fact-{index}", label, value)
            for index, (label, value) in enumerate(_facts(state.add.get("facts")))
        ]
        body = facts + (_save_fields(tree, state) if ok else [])
        props["content"] = tree.column("add-test", body)
        props["rail"] = {"steps": steps, "step": 3 if ok else 2}
        props["cancel"] = {
            "label": _("Back"),
            "action": event(BACK, {"token": tokens.mint(BACK, ADD_SUBJECT), "values": bound(ADD)}),
        }
    else:
        current = _draft(state)
        step = 1 if current.get("site") else 0
        props["tabs"] = [
            {
                "id": CATALOG_TAB,
                "title": _("Catalog"),
                "icon": _TAB_ICONS[CATALOG_TAB],
                "child": _catalog_tab(tree, state, forms, tokens, inputs),
            },
            {
                "id": SAVED_TAB,
                "title": _("Saved"),
                "icon": _TAB_ICONS[SAVED_TAB],
                "child": _saved_tab(tree, state, tokens, inputs),
            },
            {
                "id": OWN_TAB,
                "title": _("Your own"),
                "icon": _TAB_ICONS[OWN_TAB],
                "child": _own_tab(tree, state, forms, tokens),
            },
        ]
        props["activeTab"] = bound(f"{ADD}/tab")
        props["rail"] = {"steps": steps, "step": step, "tabs": [CATALOG_TAB]}
        props["cancel"] = {"label": _("Cancel")}
    return tree.add(
        "add-dialog",
        DIALOG,
        title=_("Add endpoint"),
        open=bound("/dialogs/add"),
        trigger=trigger,
        onClose=event(CLOSE_DIALOG, {"token": tokens.mint(CLOSE_DIALOG, ADD_SUBJECT)}),
        **{key: value for key, value in props.items() if value is not None},
    )


def add_trigger(tree: Tree, tokens: ActionTokens, component_id: str = "add-open") -> str:
    """Add the Add endpoint button that opens the add dialog."""
    child = tree.icon_label(f"{component_id}-child", "add", _("Add endpoint"))
    action = event(OPEN_DIALOG, {"token": tokens.mint(OPEN_DIALOG, ADD_SUBJECT)})
    return tree.add(component_id, "Button", child=child, action=action, variant="primary")


# The defaults dialog


# Edit and sign in


__all__ = [
    "ADD",
    "DEFAULTS_DRAFT",
    "SECURITY",
    "SECURITY_HELP",
    "SPEND_PRESETS",
    "DialogInputs",
    "add_data",
    "add_dialog",
    "add_trigger",
    "defaults_data",
    "dialog_flags",
    "passed",
]
