"""What the add dialog's draft stands for: the site it would add, and what testing it found.

The dialog has three tabs. Catalog picks a site the catalog knows and one of its
protocols. Saved picks a computer the person can already reach: an SSH host
saved in CLIO, a host in ``~/.ssh/config``, or an endpoint they removed. Your
own takes a name, an address and a protocol. Each builds one
:class:`~clio_relay.sites.models.SiteEntry` here, which the dialog tests and
then saves through the site store.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from clio_relay import catalog
from clio_relay.adapters.base import SiteInfo
from clio_relay.catalog import CatalogSite
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui import saved
from clio_relay.surface.a2ui.forms import Field, Form, FormError, parse
from clio_relay.surface.a2ui.i18n import _
from clio_relay.surface.a2ui.surfaces import (
    ADD_TABS,
    NAME,
    POLICY,
    SPEND,
    SPEND_LIMIT_FIELD,
    protocol_name,
    scheduler_name,
)
from clio_relay.surface.a2ui.values import as_dict, located, site_values

CATALOG_TAB, SAVED_TAB, OWN_TAB = ADD_TABS
SAVE_FIELDS = (NAME, POLICY, SPEND, SPEND_LIMIT_FIELD)
OWN_FIELDS = (
    Field("name", "Name", "text", required=True),
    Field("address", "Address", "text", help="A host name or an IP address"),
)
SAVED_HOST = "host:"
CLIO_HOST = "clio:"
REMOVED = "removed:"

_ID = re.compile(r"[^a-z0-9_-]+")


def slug(text: str) -> str:
    """Return an endpoint id made from a name: lowercase letters, digits, - and _."""
    found = _ID.sub("-", text.strip().lower()).strip("-_")
    return found or "endpoint"


def free_id(base: str, taken: Iterable[str]) -> str:
    """Return ``base``, or ``base-2`` and so on when an endpoint already has it."""
    used = set(taken)
    if base not in used:
        return base
    number = 2
    while f"{base}-{number}" in used:
        number += 1
    return f"{base}-{number}"


def shown_fields(form: Form | None) -> tuple[Field, ...]:
    """Return the settings the dialog asks for: a protocol's required ones."""
    return tuple(item for item in form.fields if item.required) if form else ()


def draft_key(add: dict[str, Any]) -> str:
    """Return what a test was run on, so a change after it asks for a new test."""
    tab = str(add.get("tab") or CATALOG_TAB)
    protocol = _protocol(add)
    picked = {
        "tab": tab,
        "site": add.get("site") if tab == CATALOG_TAB else None,
        "saved": add.get("saved") if tab == SAVED_TAB else None,
        "protocol": protocol,
        "own": as_dict(add.get("own")) if tab == OWN_TAB else None,
        "config": as_dict(as_dict(add.get("config")).get(protocol)),
    }
    text = json.dumps(picked, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _protocol(add: dict[str, Any]) -> str:
    tab = str(add.get("tab") or CATALOG_TAB)
    if tab == OWN_TAB:
        return str(add.get("ownProtocol") or "")
    if tab == SAVED_TAB:
        return str(add.get("savedProtocol") or "")
    return str(add.get("protocol") or "")


@dataclass(frozen=True)
class SavedItem:
    """One computer the Saved tab offers."""

    key: str
    title: str
    subtitle: str | None
    group: str
    adapter: str
    host: saved.SshHost | None = None
    removed: dict[str, Any] | None = None


def _when(at: str, now: datetime) -> str:
    try:
        then = datetime.fromisoformat(at)
    except ValueError:
        return _("removed earlier")
    if then.tzinfo is None:
        then = then.replace(tzinfo=UTC)
    days = (now.date() - then.astimezone(now.tzinfo).date()).days
    if days <= 0:
        return _("removed today")
    if days == 1:
        return _("removed yesterday")
    return _("removed on {date}", date=then.date().isoformat())


def saved_items(
    removed: list[dict[str, Any]],
    taken: Iterable[str],
    environ: dict[str, str] | None = None,
    now: datetime | None = None,
) -> list[SavedItem]:
    """Return the Saved tab: CLIO's SSH hosts, then ``~/.ssh/config``'s, then removed endpoints.

    A host an endpoint already reaches by its alias is left out.
    """
    used = set(taken)
    mine, others = saved.known_hosts(environ)
    items: list[SavedItem] = []
    for prefix, group, hosts in (
        (CLIO_HOST, _("Saved SSH hosts"), mine),
        (SAVED_HOST, _("From ~/.ssh/config"), others),
    ):
        for host in hosts:
            if host.alias in used:
                continue
            where = host.hostname
            if where and host.user:
                where = f"{host.user}@{where}"
            items.append(SavedItem(prefix + host.alias, host.alias, where, group, "ssh", host=host))
    moment = now or datetime.now(UTC)
    for index, item in enumerate(removed):
        entry = as_dict(item.get("entry"))
        site_id = str(entry.get("id") or "")
        if not site_id or site_id in used:
            continue
        adapter = str(entry.get("adapter") or "")
        title = str(entry.get("name") or site_id)
        subtitle = f"{protocol_name(adapter)}, {_when(str(item.get('at') or ''), moment)}"
        items.append(
            SavedItem(
                f"{REMOVED}{index}",
                title,
                subtitle,
                _("Removed from relay"),
                adapter,
                removed=entry,
            )
        )
    return items


@dataclass
class Built:
    """The site a draft would add, what to show about it, and what was wrong with it."""

    entry: SiteEntry | None
    errors: dict[str, str]
    facts: list[tuple[str, str]]
    title: str = ""
    address: str | None = None


def _config(
    form: Form | None, base: dict[str, Any], raw: dict[str, Any], errors: dict[str, str]
) -> dict[str, Any]:
    fields = shown_fields(form)
    try:
        typed = parse(fields, raw)
    except FormError as exc:
        errors[f"config.{form.adapter if form else ''}.{exc.key}"] = str(exc)
        return base
    for item in fields:
        if raw.get(item.key) in (None, "") and item.key in base:
            continue
        if item.key in typed:
            continue
        if item.required:
            errors[f"config.{form.adapter if form else ''}.{item.key}"] = _(
                "Enter {label}", label=_(item.label).lower()
            )
    return base | typed


def _detail(adapter: str, config: dict[str, Any]) -> str:
    """Return what the summary says after a protocol's name: ``SSH, host ares``."""
    name = protocol_name(adapter)
    host = config.get("ssh_profile")
    if adapter in ("ssh", "worker") and host:
        return _("{protocol}, host {host}", protocol=name, host=host)
    tenant = config.get("tenant_id")
    if adapter == "tapis" and tenant:
        return _("{protocol}, {tenant} tenant", protocol=name, tenant=str(tenant).upper())
    return name


def _changes(add: dict[str, Any], final: bool, errors: dict[str, str]) -> dict[str, Any]:
    """Return the Save step's name, security and spending, once a test has passed."""
    if not final:
        return {}
    values = site_values(SAVE_FIELDS, as_dict(add.get("save")), errors)
    changes: dict[str, Any] = {}
    if values.get("name"):
        changes["name"] = values["name"]
    if values.get("policy"):
        changes["policy"] = values["policy"]
    if values.get("spend") is not None:
        changes["spend"] = {"ask": values["spend"]}
    return changes


def build(
    add: dict[str, Any],
    store: SiteStore,
    forms: dict[str, Form],
    removed: list[dict[str, Any]],
    *,
    final: bool = False,
    sites: list[CatalogSite] | None = None,
    environ: dict[str, str] | None = None,
) -> Built:
    """Return the site the add dialog's draft stands for.

    ``final`` reads the Save step's name, security and spending too.
    """
    tab = str(add.get("tab") or CATALOG_TAB)
    errors: dict[str, str] = {}
    try:
        taken = [site.id for site in store.list()]
    except Refusal:
        taken = []
    changes = _changes(add, final, errors)
    config_drafts = as_dict(add.get("config"))
    if tab == CATALOG_TAB:
        found = {site.id: site for site in (sites if sites is not None else catalog.grouped())}
        group = found.get(str(add.get("site") or ""))
        if group is None:
            return Built(None, {"add.site": _("Choose a site")}, [])
        way = group.way(str(add.get("protocol") or "") or None)
        source = catalog.site(way.entry)
        base = source.to_site(None, way.adapter).config
        config = _config(
            forms.get(way.adapter), base, as_dict(config_drafts.get(way.adapter)), errors
        )
        facts = [
            (_("Site"), ", ".join(part for part in (group.name, group.address) if part)),
            (_("Protocol"), _detail(way.adapter, config)),
        ]
        if errors:
            return Built(None, errors, facts, group.name, group.address)
        site_id = free_id(group.id, taken)
        try:
            entry = store.from_catalog(
                source,
                site_id,
                way.adapter,
                {"config": config, "name": group.name} | changes,
            )
        except (ValidationError, Refusal) as exc:
            return Built(None, _refused(exc), facts, group.name, group.address)
        return Built(entry, {}, facts, entry.title, entry.address)
    if tab == SAVED_TAB:
        items = {item.key: item for item in saved_items(removed, taken, environ)}
        item = items.get(str(add.get("saved") or ""))
        if item is None:
            return Built(None, {"add.saved": _("Choose a computer")}, [])
        if item.removed is not None:
            raw = item.removed | changes
        else:
            host = item.host
            assert host is not None
            config: dict[str, Any] = {"ssh_profile": host.alias}
            file = saved.saved_file(environ)
            if item.key.startswith(CLIO_HOST) and file is not None:
                config["ssh_config"] = str(file)
            raw = {
                "id": free_id(slug(host.alias), taken),
                "name": host.alias,
                "address": host.hostname,
                "adapter": "ssh",
                "config": config,
            } | changes
        facts = [
            (_("Computer"), item.title),
            (_("Protocol"), _detail(item.adapter, as_dict(raw.get("config")))),
        ]
        return _validated(raw, store, facts)
    own = as_dict(add.get("own"))
    name = str(own.get("name") or "").strip()
    address = str(own.get("address") or "").strip() or None
    adapter = str(add.get("ownProtocol") or "")
    if not name:
        errors["own.name"] = _("Enter a name")
    if adapter not in forms:
        errors["add.ownProtocol"] = _("Choose a protocol")
    config = _config(forms.get(adapter), {}, as_dict(config_drafts.get(adapter)), errors)
    facts = [
        (_("Computer"), ", ".join(part for part in (name, address) if part)),
        (_("Protocol"), _detail(adapter, config) if adapter else ""),
    ]
    if errors:
        return Built(None, errors, facts, name, address)
    raw = {
        "id": free_id(slug(name), taken),
        "name": name,
        "address": address,
        "adapter": adapter,
        "config": config,
    } | changes
    return _validated(raw, store, facts)


def _validated(raw: dict[str, Any], store: SiteStore, facts: list[tuple[str, str]]) -> Built:
    defaults = store.defaults()
    raw.setdefault("policy", defaults.policy)
    raw.setdefault("spend", {"ask": defaults.spend})
    try:
        entry = SiteEntry.model_validate(raw)
    except ValidationError as exc:
        return Built(None, _refused(exc), facts, str(raw.get("name") or ""))
    return Built(entry, {}, facts, entry.title, entry.address)


def _refused(exc: ValidationError | Refusal) -> dict[str, str]:
    if isinstance(exc, Refusal):
        found = located(exc.data.get("errors"), "config.", set())
        return found or {"add": exc.message}
    found = located(exc.errors(include_url=False), "config.", set())
    return found or {"add": _("Relay cannot use these settings")}


def ready_line(title: str, adapter: str, info: SiteInfo | None, seconds: float) -> str:
    """Return a passed test's line: ``Reached Ares over SSH: Slurm, 4 partitions, 1.1 s``."""
    parts: list[str] = []
    scheduler = scheduler_name(info.scheduler) if info is not None else None
    if scheduler:
        parts.append(scheduler)
    if info is not None and info.partitions:
        count = len(info.partitions)
        parts.append(_("1 partition") if count == 1 else _("{n} partitions", n=count))
    parts.append(_("{seconds} s", seconds=f"{seconds:.1f}"))
    return _(
        "Reached {site} over {protocol}: {facts}",
        site=title,
        protocol=protocol_name(adapter),
        facts=", ".join(parts),
    )


def removed_record(entry: SiteEntry, now: datetime | None = None) -> dict[str, Any]:
    """Return what the Saved tab keeps of a removed endpoint."""
    moment = now or datetime.now(UTC)
    return {"entry": entry.model_dump(mode="json", exclude_none=True), "at": moment.isoformat()}
