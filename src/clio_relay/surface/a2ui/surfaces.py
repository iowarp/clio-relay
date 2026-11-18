"""The Compute tab's two surfaces: the sites and the add-site form (RL32, RL50).

These builders are pure. They take what the tab knows about each site, mint one
action token per button, and return components and a data model. The tab
(:mod:`clio_relay.surface.a2ui.tab`) gathers the facts and wraps the result in
A2UI messages.

Text is English. No locale reaches an MCP server, so CLIO shows relay's words as
relay sends them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, cast

from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui.components import Tree, bound, event, open_url
from clio_relay.surface.a2ui.forms import Field, Form, draft
from clio_relay.surface.a2ui.protocol import A2UIClient, Message
from clio_relay.surface.a2ui.tokens import ActionTokens

SITES_SURFACE = "relay-sites"
ADD_SURFACE = "relay-add"

ADD_SITE = "compute_add_site"
ADD_FROM_CATALOG = "compute_add_from_catalog"
EDIT_SITE = "compute_edit_site"
SAVE_SITE = "compute_save_site"
CANCEL = "compute_cancel"
REMOVE_SITE = "compute_remove_site"
CONFIRM_REMOVE = "compute_confirm_remove"
PROBE_SITE = "compute_probe_site"
SWITCH_ADAPTER = "compute_switch_adapter"
CONNECT_SITE = "compute_connect_site"
ACTIONS = (
    ADD_SITE,
    ADD_FROM_CATALOG,
    EDIT_SITE,
    SAVE_SITE,
    CANCEL,
    REMOVE_SITE,
    CONFIRM_REMOVE,
    PROBE_SITE,
    SWITCH_ADAPTER,
    CONNECT_SITE,
)

HOST_SIGN_IN = "data_source/login/"
FORM_ERROR = ""

# Where each form keeps its ``valid`` map, and the error keys that live in it.
ADD_PATH = "/drafts/add"
ADD_ERROR_PREFIXES = ("site.", "config.")
EDIT_ERROR_PREFIXES = ("site.", "adapters.")
CATALOG_PATH = "/catalog"
CATALOG_ERROR = "catalog"
CATALOG_SITE_ERROR = "catalog.id"
CATALOG_ID_ERROR = "catalog.site_id"

POLICY = Field(
    "policy",
    "Policy",
    "choice",
    help="Guarded runs reviewed packages only. Open runs what the agent writes.",
    options=("guarded", "open"),
    labels=("Guarded", "Open"),
)
SPEND = Field(
    "spend",
    "Ask before spending",
    "text",
    help="always, never, or the node-hours above which relay asks",
)
SKILL = Field("skill", "Skill", "text", help="A skill name, or the path of your own Markdown skill")
NAME = Field("name", "Name", "text")
ADD_FIELDS = (
    Field("id", "Site ID", "text", help="Lowercase letters, digits, - and _", required=True),
    NAME,
    POLICY,
    SPEND,
    SKILL,
)
EDIT_FIELDS = (
    NAME,
    Field("address", "Address", "text"),
    POLICY,
    SPEND,
    SKILL,
    Field("account", "Default account", "text"),
    Field("queue", "Default queue", "text"),
)

_STATUS = {"ready": "Ready", "needs_sign_in": "Needs a sign-in", "unavailable": "Unavailable"}
_PROBE = {
    "ready": "Ready",
    "missing_allocation": "Missing an allocation",
    "needs_sign_in": "Needs a sign-in",
    "unreachable": "Unreachable",
    "not_configured": "Not configured",
}
_SIGN_IN = {"signed_in": "Signed in", "signed_out": "Signed out", "expired": "Expired"}


@dataclass(frozen=True)
class SiteView:
    """What the tab shows for one site, gathered before rendering."""

    site: SiteEntry
    status: dict[str, Any]
    identity: dict[str, Any] | None = None
    probe: dict[str, Any] | None = None
    skill: dict[str, Any] | None = None
    reviewed: int | None = None
    system_id: str | None = None
    has_system_id: bool = False
    host_sign_in: str | None = None


@dataclass
class TabState:
    """What the tab remembers between reads."""

    notice: str = ""
    editing: set[str] = field(default_factory=set[str])
    confirming: set[str] = field(default_factory=set[str])
    errors: dict[str, dict[str, str]] = field(default_factory=dict[str, dict[str, str]])
    links: dict[str, str] = field(default_factory=dict[str, str])

    def to_dict(self) -> dict[str, Any]:
        """Return the state as JSON-ready data."""
        return {
            "notice": self.notice,
            "editing": sorted(self.editing),
            "confirming": sorted(self.confirming),
            "errors": self.errors,
            "links": self.links,
        }

    @classmethod
    def from_dict(cls, data: Any) -> TabState:
        """Read a state :meth:`to_dict` wrote; anything unreadable reads as empty."""
        if not isinstance(data, dict):
            return cls()
        raw = cast(dict[str, Any], data)

        def names(key: str) -> set[str]:
            value = raw.get(key)
            items = cast(list[Any], value) if isinstance(value, list) else []
            return {str(item) for item in items}

        def mapping(value: Any) -> dict[str, Any]:
            return cast(dict[str, Any], value) if isinstance(value, dict) else {}

        return cls(
            notice=str(raw.get("notice") or ""),
            editing=names("editing"),
            confirming=names("confirming"),
            errors={
                str(key): {str(k): str(v) for k, v in mapping(value).items()}
                for key, value in mapping(raw.get("errors")).items()
            },
            links={str(k): str(v) for k, v in mapping(raw.get("links")).items()},
        )

    def forget(self, site_id: str) -> None:
        """Drop everything the tab holds for a site."""
        self.editing.discard(site_id)
        self.confirming.discard(site_id)
        self.errors.pop(site_id, None)
        self.links.pop(site_id, None)


# Text


def spend_text(site: SiteEntry) -> str:
    ask = site.spend.ask
    if ask == "always":
        return "Always"
    if ask == "never":
        return "Never"
    return f"Above {ask:g} node-hours"


def _duration(value: timedelta) -> str:
    minutes = value.total_seconds() / 60
    return f"{minutes / 60:g} h" if minutes >= 60 and minutes % 60 == 0 else f"{minutes:g} min"


def probe_text(state: str) -> str:
    """Return a probe state as a person reads it."""
    return _PROBE.get(state, state)


def status_text(view: SiteView) -> str:
    state = str(view.status.get("state", ""))
    shown = _STATUS.get(state, state)
    if state == "needs_sign_in" and view.status.get("provider"):
        return f"Needs a {view.status['provider']} sign-in"
    return shown


def summary(views: list[SiteView], notice: str = "") -> str:
    """Return the tab as text, for a client that renders no A2UI and for every result."""
    lines = [notice] if notice else []
    if not views:
        lines.append("No sites yet.")
    for view in views:
        site = view.site
        line = f"{site.title} ({site.id}) uses {site.adapter}"
        if site.alternatives:
            line += f", or {', '.join(site.alternatives)}"
        line += f". {status_text(view)}."
        if view.probe:
            found = _PROBE.get(str(view.probe.get("state", "")), "unknown")
            line += f" The last probe found it {found.lower()}."
        lines.append(line)
    return "\n".join(lines)


# Fields


def _segment(key: str) -> str:
    return key.replace("~", "~0").replace("/", "~1")


def valid_path(form_path: str, key: str) -> str:
    """Return where a form keeps whether the field under error ``key`` is valid."""
    return f"{form_path}/valid/{_segment(key)}"


def valid_map(errors: dict[str, str], prefixes: tuple[str, ...]) -> dict[str, bool]:
    """Return a form's ``valid`` data: false for each field with an error under ``prefixes``."""
    return {key: False for key in errors if key and key.startswith(prefixes)}


def _checks(error: str | None, check_path: str | None) -> dict[str, Any]:
    if not error or check_path is None:
        return {}
    return {"checks": [{"condition": bound(check_path), "message": error}]}


def field_components(
    tree: Tree,
    base: str,
    item: Field,
    path: str,
    error: str | None,
    check_path: str | None = None,
) -> list[str]:
    """Add one form field, its help and its error, and return their ids.

    Each field shows its own error, through a check bound to ``check_path``
    that the form's data sets false. Without a ``check_path`` the error is a caption.
    """
    label = f"{item.label} (required)" if item.required else item.label
    value = bound(path)
    checks = _checks(error, check_path)
    if item.kind == "bool":
        ids = [tree.add(base, "CheckBox", label=label, value=value, **checks)]
    elif item.kind == "choice":
        ids = [
            tree.add(
                base,
                "ChoicePicker",
                label=label,
                options=item.option_labels(),
                value=value,
                variant="mutuallyExclusive",
                displayStyle="chips",
                **checks,
            )
        ]
    else:
        variant = {
            "number": "number",
            "integer": "number",
            "lines": "longText",
            "yaml": "longText",
        }.get(item.kind, "shortText")
        ids = [tree.add(base, "TextField", label=label, value=value, variant=variant, **checks)]
    if error and not checks:
        ids.append(tree.text(f"{base}-error", error, "caption"))
    if item.help:
        ids.append(tree.text(f"{base}-help", item.help, "caption"))
    return ids


def _fields(
    tree: Tree,
    base: str,
    fields: tuple[Field, ...],
    path: str,
    errors: dict[str, str],
    prefix: str,
    form_path: str,
) -> list[str]:
    ids: list[str] = []
    for item in fields:
        key = prefix + item.key
        ids += field_components(
            tree,
            f"{base}-f-{item.key}",
            item,
            f"{path}/{item.key}",
            errors.get(key),
            valid_path(form_path, key),
        )
    return ids


def _form_error(tree: Tree, base: str, errors: dict[str, str]) -> list[str]:
    message = errors.get(FORM_ERROR)
    return [tree.text(f"{base}-error", message, "caption")] if message else []


# The sites surface


def _common_draft(site: SiteEntry) -> dict[str, Any]:
    values: dict[str, Any] = {
        "name": site.name,
        "address": site.address,
        "policy": site.policy,
        "spend": str(site.spend.ask),
        "skill": site.skill,
        "account": site.defaults.account,
        "queue": site.defaults.queue,
    }
    return draft(EDIT_FIELDS, values)


def _edit_order(site: SiteEntry, forms: dict[str, Form]) -> list[str]:
    order = [site.adapter, *site.alternatives]
    return order + [name for name in forms if name not in order]


def edit_draft(site: SiteEntry, forms: dict[str, Form]) -> dict[str, Any]:
    """Return the edit form's starting draft for ``site``."""
    adapters: dict[str, Any] = {}
    for name in _edit_order(site, forms):
        adapter_form = forms.get(name)
        if adapter_form is None:
            continue
        config = site.config if name == site.adapter else site.alternatives.get(name, {})
        adapters[name] = {"use": name in site.alternatives, **draft(adapter_form.fields, config)}
    return {"site": _common_draft(site), "adapters": adapters}


def _edit_form(
    tree: Tree,
    site: SiteEntry,
    forms: dict[str, Form],
    tokens: ActionTokens,
    errors: dict[str, str],
) -> str:
    base = f"e-{site.id}"
    path = f"/drafts/{site.id}"
    children = [tree.text(f"{base}-title", f"Edit {site.title}", "h4")]
    children += _form_error(tree, base, errors)
    children += _fields(tree, f"{base}-s", EDIT_FIELDS, f"{path}/site", errors, "site.", path)
    children.append(tree.text(f"{base}-ways", "Ways in", "h5"))
    tabs: list[dict[str, str]] = []
    for name in _edit_order(site, forms):
        adapter_form = forms.get(name)
        if adapter_form is None:
            continue
        tab_base = f"{base}-a-{name}"
        adapter_path = f"{path}/adapters/{name}"
        rows: list[str] = []
        if name == site.adapter:
            rows.append(tree.text(f"{tab_base}-inuse", "In use", "caption"))
        else:
            rows.append(
                tree.add(
                    f"{tab_base}-use",
                    "CheckBox",
                    label="Use as another way in",
                    value=bound(f"{adapter_path}/use"),
                )
            )
        rows += _fields(
            tree, tab_base, adapter_form.fields, adapter_path, errors, f"adapters.{name}.", path
        )
        if not adapter_form.fields:
            rows.append(tree.text(f"{tab_base}-none", "No settings", "caption"))
        tabs.append({"title": name, "child": tree.column(f"{tab_base}-col", rows)})
    children.append(tree.add(f"{base}-tabs", "Tabs", tabs=tabs))
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


def _identity_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    identity = view.identity
    if identity is None:
        return []
    rows = [tree.fact(f"{base}-identity", "Identity", str(identity.get("title") or ""))]
    state = str(identity.get("state") or "")
    shown = _SIGN_IN.get(state, state)
    if state == "signed_in" and identity.get("account"):
        shown = f"Signed in as {identity['account']}"
    rows.append(tree.fact(f"{base}-signin", "Sign-in", shown))
    return rows


def _status_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    rows = [tree.fact(f"{base}-status", "Status", status_text(view))]
    detail = view.status.get("detail")
    if detail and view.status.get("state") != "ready":
        rows.append(tree.text(f"{base}-status-detail", str(detail), "caption"))
    if view.probe:
        state = str(view.probe.get("state", ""))
        rows.append(tree.fact(f"{base}-probe", "Last probe", _PROBE.get(state, state)))
        if view.probe.get("detail"):
            rows.append(tree.text(f"{base}-probe-detail", str(view.probe["detail"]), "caption"))
    return rows


def _settings_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    site = view.site
    rows = [tree.fact(f"{base}-policy", "Policy", site.policy.capitalize())]
    if view.reviewed is not None and site.policy == "guarded":
        rows.append(tree.fact(f"{base}-reviewed", "Reviewed packages", str(view.reviewed)))
    rows.append(tree.fact(f"{base}-spend", "Ask before spending", spend_text(site)))
    defaults = site.defaults
    if defaults.account:
        rows.append(tree.fact(f"{base}-account", "Default account", defaults.account))
    if defaults.queue:
        rows.append(tree.fact(f"{base}-queue", "Default queue", defaults.queue))
    if defaults.duration is not None:
        rows.append(tree.fact(f"{base}-duration", "Default duration", _duration(defaults.duration)))
    if view.has_system_id:
        shown = view.system_id or "Relay registers one after you sign in"
        rows.append(tree.fact(f"{base}-system", "System ID", shown))
    if view.skill:
        rows.append(tree.fact(f"{base}-skill", "Skill", str(view.skill.get("linked") or "")))
        note = view.skill.get("uri") or view.skill.get("detail")
        if note:
            rows.append(tree.text(f"{base}-skill-uri", str(note), "caption"))
    return rows


def _switch_rows(tree: Tree, base: str, view: SiteView, tokens: ActionTokens) -> list[str]:
    site = view.site
    if not site.alternatives:
        return []
    options = [{"label": name, "value": name} for name in [site.adapter, *site.alternatives]]
    picker = tree.add(
        f"{base}-adapter",
        "ChoicePicker",
        label="Adapter in use",
        options=options,
        value=bound(f"/switch/{site.id}/adapter"),
        variant="mutuallyExclusive",
        displayStyle="chips",
    )
    use = tree.button(
        f"{base}-switch",
        "Switch adapter",
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
    children.append(tree.fact(f"{base}-adapter-in-use", "Adapter", site.adapter))
    children += _switch_rows(tree, base, view, tokens)
    children += _identity_rows(tree, base, view)
    children += _status_rows(tree, base, view)
    children += _settings_rows(tree, base, view)
    link = state.links.get(site.id)
    if link:
        children.append(tree.button(f"{base}-open", "Open the sign-in page", open_url(link)))
    errors = state.errors.get(site.id, {})
    if site.id not in state.editing:
        children += _form_error(tree, base, errors)
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
            f"Remove {site.title} from your sites? Its operations stay on record.",
        )
        yes = tree.button(
            f"{base}-confirm-yes",
            "Remove site",
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
        children.append(_edit_form(tree, site, forms, tokens, errors))
    return tree.card(base, tree.column(f"{base}-body", children))


def sites_surface(
    views: list[SiteView], state: TabState, forms: dict[str, Form], tokens: ActionTokens
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return the sites surface's components and data model."""
    tree = Tree()
    children = [tree.text("sites-title", "Sites", "h2")]
    if state.notice:
        children.append(tree.text("sites-notice", state.notice))
    if not views:
        children.append(tree.text("sites-empty", "No sites yet. Add one below."))
    for view in views:
        children.append(site_card(tree, view, state, forms, tokens))
    tree.column("root", children)
    data: dict[str, Any] = {
        "switch": {view.site.id: {"adapter": [view.site.adapter]} for view in views},
        "drafts": {
            view.site.id: {
                **edit_draft(view.site, forms),
                "valid": valid_map(state.errors.get(view.site.id, {}), EDIT_ERROR_PREFIXES),
            }
            for view in views
            if view.site.id in state.editing
        },
    }
    return tree.components, data


# The add surface


def add_draft(forms: dict[str, Form]) -> dict[str, Any]:
    """Return the add form's empty draft."""
    return {
        "site": draft(ADD_FIELDS, {"policy": "guarded", "spend": "always"}),
        "config": {name: draft(item.fields, {}) for name, item in forms.items()},
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
                **_checks(
                    errors.get(CATALOG_SITE_ERROR), valid_path(CATALOG_PATH, CATALOG_SITE_ERROR)
                ),
            )
        )
        children.append(
            tree.add(
                "add-catalog-id",
                "TextField",
                label="Site ID (optional)",
                value=bound("/catalog/site_id"),
                variant="shortText",
                **_checks(errors.get(CATALOG_ID_ERROR), valid_path(CATALOG_PATH, CATALOG_ID_ERROR)),
            )
        )
        message = errors.get(CATALOG_ERROR)
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
    children += _form_error(tree, "add", errors)
    children += _fields(tree, "add-s", ADD_FIELDS, "/drafts/add/site", errors, "site.", ADD_PATH)
    children.append(tree.text("add-ways", "How relay reaches it", "h5"))
    tabs: list[dict[str, str]] = []
    for name, adapter_form in forms.items():
        base = f"add-a-{name}"
        rows = _fields(
            tree,
            base,
            adapter_form.fields,
            f"/drafts/add/config/{name}",
            errors,
            f"config.{name}.",
            ADD_PATH,
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
        "catalog": {
            "id": [catalog[0].id] if catalog else [],
            "site_id": "",
            "valid": valid_map(errors, (CATALOG_ID_ERROR, CATALOG_SITE_ERROR)),
        },
        "drafts": {"add": {**add_draft(forms), "valid": valid_map(errors, ADD_ERROR_PREFIXES)}},
    }
    return tree.components, data


# Messages


def create(
    client: A2UIClient, surface_id: str, components: list[dict[str, Any]], data: dict[str, Any]
) -> list[Message]:
    """Return the messages that create a surface (a tab read)."""
    return [
        {
            "version": client.version,
            "createSurface": {"surfaceId": surface_id, "catalogId": client.catalog_id},
        },
        *update(client, surface_id, components, data),
    ]


def update(
    client: A2UIClient,
    surface_id: str,
    components: list[dict[str, Any]],
    data: dict[str, Any] | None,
) -> list[Message]:
    """Return the messages that redraw a surface CLIO already holds (a tool result).

    ``data`` None keeps the person's drafts as they are.
    """
    messages: list[Message] = [
        {
            "version": client.version,
            "updateComponents": {"surfaceId": surface_id, "components": components},
        }
    ]
    if data is not None:
        messages.append(
            {
                "version": client.version,
                "updateDataModel": {"surfaceId": surface_id, "path": "/", "value": data},
            }
        )
    return messages
