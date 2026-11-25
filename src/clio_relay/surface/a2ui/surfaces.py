"""The Compute page's names, state and shared form pieces (RL32, RL50).

These builders are pure. They take what the page knows about each endpoint (a
site in relay's store), mint one action token per control, and return components
and a data model. The page (:mod:`clio_relay.surface.a2ui.tab`) gathers the facts
and wraps the result in A2UI messages. A client that renders relay's own catalog
gets the page of :mod:`clio_relay.surface.a2ui.table`, with its dialogs in
:mod:`clio_relay.surface.a2ui.dialog`; a Basic-only client gets the cards of
:mod:`clio_relay.surface.a2ui.cards` and the add form of
:mod:`clio_relay.surface.a2ui.adding`.

The page speaks the language the host sends with each request
(:mod:`clio_relay.surface.a2ui.i18n`), else English. The Basic cards are English.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, cast

from clio_relay.sites.models import SiteEntry, SpendAsk
from clio_relay.sites.store import NewSiteDefaults
from clio_relay.surface.a2ui.forms import Field
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.values import SPEND_ABOVE, SPEND_LIMIT, spend_value
from clio_relay.wording import with_article

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
OPEN_DIALOG = "compute_open_dialog"
PICK = "compute_pick"
TEST_ENDPOINT = "compute_test_endpoint"
SAVE_ENDPOINT = "compute_save_endpoint"
BACK = "compute_back"
CLOSE_DIALOG = "compute_close_dialog"
UNDO = "compute_undo"
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
    OPEN_DIALOG,
    PICK,
    TEST_ENDPOINT,
    SAVE_ENDPOINT,
    BACK,
    CLOSE_DIALOG,
    UNDO,
)

# Subjects that are not endpoints. An endpoint id never starts with "@".
ADD_SUBJECT = "@add"
DEFAULTS_SUBJECT = "@defaults"
UNDO_SUBJECT = "@undo"
DIALOG_SUBJECT = "@dialog"

# The page's dialogs: which one is open is TabState.dialog.
ADD_DIALOG = "add"
EDIT_DIALOG = "edit:"
SIGN_IN_DIALOG = "signin:"
# The add dialog's tabs, by the id of each tab's child.
ADD_TABS = ("add-catalog", "add-saved", "add-own")
REMOVED_KEPT = 10
"""Removed endpoints the add dialog's Saved tab offers back, newest first."""

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
# When relay asks before it submits a job. A threshold is a number of node-hours.
SPEND_HELP = (
    "When relay asks you before it submits a job. A job's node-hours are its nodes "
    "times its walltime: 8 nodes for 2 hours is 16."
)
SPEND_ALWAYS = ("Ask every time", "Asks before it submits any job")
SPEND_NEVER = (
    "Always allow",
    "Never asks, for unlimited allocations",
)
# Thresholds every Spending picker offers, besides the ones endpoints already use.
SPEND_PRESETS = (10.0, 100.0)
_SPEND_MAX = 20
"""Spending options at most, inside the endpoint table's 24 per column."""
DEFAULTS = {"protocol": "ssh", "security": "guarded", "spend": "always"}

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
    "Spending",
    "choice",
    help=SPEND_HELP,
    options=("always", SPEND_ABOVE, "never"),
    labels=(SPEND_ALWAYS[0], "Ask above a limit", SPEND_NEVER[0]),
)
SPEND_LIMIT_FIELD = Field(
    SPEND_LIMIT,
    "Limit (node-hours)",
    "number",
    help="With Ask above a limit, relay asks when a job may use more than this, "
    "or when it sets no walltime.",
)
SKILL = Field("skill", "Skill", "text", help="A skill name, or the path of your own Markdown skill")
NAME = Field("name", "Name", "text")
ADD_FIELDS = (
    Field("id", "Endpoint ID", "text", help="Lowercase letters, digits, - and _", required=True),
    NAME,
    POLICY,
    SPEND,
    SPEND_LIMIT_FIELD,
    SKILL,
)
EDIT_FIELDS = (
    NAME,
    Field("address", "Address", "text"),
    POLICY,
    SPEND,
    SPEND_LIMIT_FIELD,
    SKILL,
    Field("account", "Default account", "text"),
    Field("queue", "Default queue", "text"),
)

_STATUS = {
    "ready": "Ready",
    "needs_sign_in": "Needs a sign-in",
    "unavailable": "Unavailable",
    "checking": "Checking",
}
_PROBE = {
    "ready": "Ready",
    "missing_allocation": "Missing an allocation",
    "needs_sign_in": "Needs a sign-in",
    "unreachable": "Unreachable",
    "not_configured": "Not configured",
}
SIGN_IN_STATES = {"signed_in": "Signed in", "signed_out": "Signed out", "expired": "Expired"}


def protocol_name(adapter: str) -> str:
    """Return a protocol's name as a person reads it."""
    return PROTOCOLS.get(adapter, (adapter, ""))[0]


# Protocols named with an ordinary word, which the page words in the person's language.
_WORDED_PROTOCOLS = frozenset({"local"})
# The catalog's name for the computer relay runs on (catalog/sites/local.yaml).
LOCAL_SITE_NAME = "This computer"


def protocol_label(adapter: str) -> str:
    """Return a protocol's name in the page's language: "Local" is a word, "SSH" a name."""
    name = protocol_name(adapter)
    return _(name) if adapter in _WORDED_PROTOCOLS else name


def site_label(site: SiteEntry) -> str:
    """Return a site's name in the page's language when it is still the catalog's own."""
    if site.adapter == "local" and site.title == LOCAL_SITE_NAME:
        return _(LOCAL_SITE_NAME)
    return site.title


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
    """The site store's defaults for new endpoints, read on each load and never saved here."""
    pending: dict[str, str] = field(default_factory=dict[str, str])
    """A protocol picked for an endpoint that has no settings for it yet, by endpoint."""
    dialog: str = ""
    """The open dialog: ``add``, ``defaults``, ``edit:<id>`` or ``signin:<id>``."""
    add: dict[str, Any] = field(default_factory=dict[str, Any])
    """The add dialog's draft: its tab, what was picked, the values sent, the last test."""
    undo: dict[str, Any] = field(default_factory=dict[str, Any])
    """What the last toast's Undo reverses: an added id, or a removed entry."""
    removed: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    """Endpoints the person removed, newest first: ``{"entry": ..., "at": ...}``."""

    def default(self, column: str) -> str:
        """Return the protocol, security or spend setting a new endpoint starts with."""
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
            "pending": self.pending,
            "dialog": self.dialog,
            "add": self.add,
            "undo": self.undo,
            "removed": self.removed,
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
            pending={str(k): str(v) for k, v in mapping(raw.get("pending")).items()},
            dialog=str(raw.get("dialog") or ""),
            add=mapping(raw.get("add")),
            undo=mapping(raw.get("undo")),
            removed=[
                mapping(item)
                for item in cast(list[Any], raw.get("removed") or [])
                if isinstance(item, dict)
            ][:REMOVED_KEPT]
            if isinstance(raw.get("removed"), list)
            else [],
        )

    def forget(self, site_id: str) -> None:
        """Drop everything the tab holds for a site."""
        self.editing.discard(site_id)
        self.confirming.discard(site_id)
        self.errors.pop(site_id, None)
        self.links.pop(site_id, None)
        self.pending.pop(site_id, None)
        if self.dialog in (EDIT_DIALOG + site_id, SIGN_IN_DIALOG + site_id):
            self.dialog = ""


# Text


def spend_text(ask: SpendAsk) -> str:
    """Return a spend setting as the page's Spending words."""
    if ask == "always":
        return SPEND_ALWAYS[0]
    if ask == "never":
        return SPEND_NEVER[0]
    return f"Ask above {ask:g} node-hours"


def spend_sentence(ask: SpendAsk) -> str:
    """Return what relay does before it spends, as the rest of a sentence about relay."""
    if ask == "always":
        return "asks before every job"
    if ask == "never":
        return "submits jobs without asking"
    return f"asks before a job that may use more than {ask:g} node-hours"


def spend_options(used: Iterable[str | float]) -> list[dict[str, str]]:
    """Return the Spending choices: ask every time, each threshold, then always allow.

    The thresholds are the presets and those in ``used``, the settings endpoints
    and the defaults already have, so every current setting is a choice.
    """
    in_use = {ask for ask in used if not isinstance(ask, str)}
    thresholds = sorted(in_use | set(SPEND_PRESETS))
    if len(thresholds) > _SPEND_MAX - 2:
        thresholds = sorted(in_use)[: _SPEND_MAX - 2]
    options = [
        {
            "value": "always",
            "label": SPEND_ALWAYS[0],
            "description": SPEND_ALWAYS[1],
            "icon": "notifications",
            "tone": "positive",
        }
    ]
    options += [
        {
            "value": spend_value(limit),
            "label": f"Ask above {limit:g}",
            "description": f"Over {limit:g} node-hours, or no walltime",
            "icon": "payment",
            "tone": "neutral",
        }
        for limit in thresholds
    ]
    options.append(
        {
            "value": "never",
            "label": SPEND_NEVER[0],
            "description": SPEND_NEVER[1],
            "icon": "notificationsOff",
            "tone": "warning",
        }
    )
    return options


def defaults_state(defaults: NewSiteDefaults) -> dict[str, str]:
    """Return the site store's defaults for new endpoints as the page's column values."""
    return {
        "protocol": defaults.adapter,
        "security": defaults.policy,
        "spend": spend_value(defaults.spend),
    }


def spend_draft(ask: SpendAsk) -> dict[str, Any]:
    """Return a spend setting as the form's choice and limit."""
    if isinstance(ask, str):
        return {"spend": ask, SPEND_LIMIT: None}
    return {"spend": SPEND_ABOVE, SPEND_LIMIT: f"{ask:g}"}


def duration_text(value: timedelta) -> str:
    minutes = value.total_seconds() / 60
    return f"{minutes / 60:g} h" if minutes >= 60 and minutes % 60 == 0 else f"{minutes:g} min"


def probe_text(state: str) -> str:
    """Return a probe state as a person reads it."""
    return _PROBE.get(state, state)


def status_text(view: SiteView) -> str:
    state = str(view.status.get("state", ""))
    shown = _STATUS.get(state, state)
    if state == "needs_sign_in" and view.status.get("provider"):
        return sign_in_text(str(view.status["provider"]))
    return shown


def sign_in_text(provider: str) -> str:
    """Return what a site that needs ``provider``'s sign-in shows: "Needs an ssh sign-in"."""
    return f"Needs {with_article(provider)} sign-in"


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
        line += f". Security is {security_name(site.policy)}."
        line += f" Relay {spend_sentence(site.spend.ask)}. {status_text(view)}."
        if view.probe:
            found = _PROBE.get(str(view.probe.get("state", "")), "unknown")
            line += f" The last probe found it {found.lower()}."
        lines.append(line)
    return "\n".join(lines)


# The sites surface
