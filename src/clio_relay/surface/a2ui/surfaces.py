"""The Compute page's names, state and shared form pieces (RL32, RL50).

These builders are pure. They take what the page knows about each endpoint (a
site in relay's store), mint one action token per control, and return components
and a data model. The page (:mod:`clio_relay.surface.a2ui.tab`) gathers the facts
and wraps the result in A2UI messages. A client that renders relay's own catalog
gets the endpoint table of :mod:`clio_relay.surface.a2ui.table`; a Basic-only
client gets the cards of :mod:`clio_relay.surface.a2ui.cards`. The add form is in
:mod:`clio_relay.surface.a2ui.adding`.

Text is English. No locale reaches an MCP server, so CLIO shows relay's words as
relay sends them.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, cast

from clio_relay.sites.models import SiteEntry
from clio_relay.surface.a2ui.components import Tree, bound, event
from clio_relay.surface.a2ui.forms import Field, Form, draft, field_rows, form_error
from clio_relay.surface.a2ui.tokens import ActionTokens

SITES_SURFACE = "relay-sites"
ADD_SURFACE = "relay-add"

OPEN_ADD = "compute_open_add"
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
SET_VALUE = "compute_set_value"
ACTIONS = (
    OPEN_ADD,
    ADD_SITE,
    ADD_FROM_CATALOG,
    EDIT_SITE,
    SAVE_SITE,
    CANCEL,
    REMOVE_SITE,
    CONFIRM_REMOVE,
    PROBE_SITE,
    SWITCH_ADAPTER,
    SET_VALUE,
    CONNECT_SITE,
)

# Subjects that are not endpoints. An endpoint id never starts with "@".
ADD_SUBJECT = "@add"
DEFAULTS_SUBJECT = "@defaults"

HOST_SIGN_IN = "data_source/login/"

# Where each form keeps its ``valid`` map, and the error keys that live in it.
ADD_PATH = "/drafts/add"
ADD_ERROR_PREFIXES = ("site.", "config.")
EDIT_ERROR_PREFIXES = ("site.", "adapters.")
CATALOG_PATH = "/catalog"
CATALOG_ERROR = "catalog"
CATALOG_SITE_ERROR = "catalog.id"
CATALOG_ID_ERROR = "catalog.site_id"

# Each protocol (an adapter) by name, as a person reads it, with what it does.
PROTOCOLS = {
    "ssh": ("SSH", "Runs over an SSH connection to a login node"),
    "worker": ("Worker", "A relay worker on the endpoint runs the work"),
    "tapis": ("Tapis", "Runs through a Tapis tenant such as TACC's"),
    "globus": ("Globus", "Runs on a Globus Compute endpoint"),
    "iri": ("IRI", "Runs through a facility's IRI API"),
    "sky": ("SkyPilot", "Runs on cloud machines SkyPilot launches"),
    "local": ("Local", "Runs on the computer relay runs on"),
}
# Security is the site's policy; the stored values stay guarded and open.
SECURITY = {
    "guarded": ("Secure", "Runs only reviewed packages"),
    "open": ("Open", "Runs what the agent writes"),
}
SECURITY_HELP = "Secure runs only reviewed packages. Open runs what the agent writes."
DEFAULTS = {"protocol": "ssh", "security": "guarded"}

POLICY = Field(
    "policy",
    "Security",
    "choice",
    help=SECURITY_HELP,
    options=tuple(SECURITY),
    labels=tuple(label for label, _ in SECURITY.values()),
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
    Field("id", "Endpoint ID", "text", help="Lowercase letters, digits, - and _", required=True),
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


def protocol_name(adapter: str) -> str:
    """Return a protocol's name as a person reads it."""
    return PROTOCOLS.get(adapter, (adapter, ""))[0]


def protocol_order(names: Iterable[str], first: str = "") -> list[str]:
    """Return protocol names in the page's order: ``first``, the known ones, then the rest."""
    known = list(PROTOCOLS)

    def key(name: str) -> tuple[bool, int, str]:
        return (name != first, known.index(name) if name in known else len(known), name)

    return sorted(set(names), key=key)


def security_name(policy: str) -> str:
    """Return a site policy as the page's Security word."""
    return SECURITY.get(policy, (policy.capitalize(), ""))[0]


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
    adding: bool = False
    defaults: dict[str, str] = field(default_factory=dict[str, str])
    pending: dict[str, str] = field(default_factory=dict[str, str])
    """A protocol picked for an endpoint that has no settings for it yet, by endpoint."""

    def default(self, column: str) -> str:
        """Return the protocol or security a new endpoint starts with."""
        return self.defaults.get(column) or DEFAULTS[column]

    def to_dict(self) -> dict[str, Any]:
        """Return the state as JSON-ready data."""
        return {
            "notice": self.notice,
            "editing": sorted(self.editing),
            "confirming": sorted(self.confirming),
            "errors": self.errors,
            "links": self.links,
            "adding": self.adding,
            "defaults": self.defaults,
            "pending": self.pending,
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
            adding=raw.get("adding") is True,
            defaults={str(k): str(v) for k, v in mapping(raw.get("defaults")).items()},
            pending={str(k): str(v) for k, v in mapping(raw.get("pending")).items()},
        )

    def forget(self, site_id: str) -> None:
        """Drop everything the tab holds for a site."""
        self.editing.discard(site_id)
        self.confirming.discard(site_id)
        self.errors.pop(site_id, None)
        self.links.pop(site_id, None)
        self.pending.pop(site_id, None)


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
    """Return the page as text, for a client that renders no A2UI and for every result."""
    lines = [notice] if notice else []
    if not views:
        lines.append("No endpoints yet.")
    for view in views:
        site = view.site
        line = f"{site.title} ({site.id}) uses {protocol_name(site.adapter)}"
        if site.alternatives:
            line += f", or {', '.join(protocol_name(name) for name in site.alternatives)}"
        line += f". Security is {security_name(site.policy)}. {status_text(view)}."
        if view.probe:
            found = _PROBE.get(str(view.probe.get("state", "")), "unknown")
            line += f" The last probe found it {found.lower()}."
        lines.append(line)
    return "\n".join(lines)


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
) -> str:
    """Add one endpoint's edit form and return its id."""
    base = f"e-{site.id}"
    path = f"/drafts/{site.id}"
    children = [tree.text(f"{base}-title", f"Edit {site.title}", "h4")]
    children += form_error(tree, base, errors)
    children += field_rows(tree, f"{base}-s", EDIT_FIELDS, f"{path}/site", errors, "site.", path)
    children.append(tree.text(f"{base}-ways", "Protocols", "h5"))
    tabs: list[dict[str, str]] = []
    for name in _edit_order(site, forms, pending):
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
                    label="Also reach it this way",
                    value=bound(f"{adapter_path}/use"),
                )
            )
        rows += field_rows(
            tree, tab_base, adapter_form.fields, adapter_path, errors, f"adapters.{name}.", path
        )
        if not adapter_form.fields:
            rows.append(tree.text(f"{tab_base}-none", "No settings", "caption"))
        title = protocol_name(name)
        tabs.append({"title": title, "child": tree.column(f"{tab_base}-col", rows)})
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


def identity_rows(tree: Tree, base: str, view: SiteView) -> list[str]:
    """Add the endpoint's identity and sign-in facts."""
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


def settings_rows(tree: Tree, base: str, view: SiteView, security: bool = True) -> list[str]:
    """Add the endpoint's settings as facts; ``security`` False leaves Security out."""
    site = view.site
    rows: list[str] = []
    if security:
        rows.append(tree.fact(f"{base}-policy", "Security", security_name(site.policy)))
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
