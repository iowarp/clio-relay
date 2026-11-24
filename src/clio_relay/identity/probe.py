"""Probing the sites an identity unlocks, after a sign-in or a grant change (RL30).

Each site that names the identity, among the person's sites and the catalog's,
is tried once: build its adapter, see whether it still wants a sign-in, and ask
it to describe itself. The result is one of:

* ``ready``: it answered.
* ``missing_allocation``: the identity works but the account has no allocation
  there. An adapter says so with a refusal carrying ``allocation=False``.
* ``needs_sign_in``: the adapter still wants a sign-in of its own.
* ``unreachable``: it could not be reached or refused for another reason.
* ``not_configured``: the entry or its adapter is not usable here.

Results are written to ``<state>/identity/probes.json``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, SiteInfo
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths

logger = logging.getLogger(__name__)

ProbeState = Literal[
    "ready", "missing_allocation", "needs_sign_in", "unreachable", "not_configured"
]

_NOT_CONFIGURED = {
    RefusalReason.UNKNOWN_ADAPTER,
    RefusalReason.INVALID_CONFIG,
    RefusalReason.ADAPTER_UNAVAILABLE,
}


class ProbeResult(BaseModel):
    """One site's state after a probe."""

    site: str
    identity: str
    origin: Literal["sites", "catalog"]
    state: ProbeState
    detail: str | None = None
    at: datetime = Field(default_factory=utc_now)


def sites_for(
    identity: str, paths: RelayPaths
) -> list[tuple[SiteEntry, Literal["sites", "catalog"]]]:
    """Return the person's sites, then the catalog's, that name ``identity``."""
    found: list[tuple[SiteEntry, Literal["sites", "catalog"]]] = []
    seen: set[str] = set()
    try:
        mine = SiteStore(paths.sites_file).list()
    except Refusal as refusal:
        logger.warning("reading sites for a probe: %s", refusal.message)
        mine = []
    for entry in mine:
        if (entry.identity or "").lower() == identity.lower():
            found.append((entry, "sites"))
            seen.add(entry.id)
    try:
        shipped = catalog.sites()
    except Refusal as refusal:
        logger.warning("reading the catalog for a probe: %s", refusal.message)
        shipped = []
    for entry in shipped:
        if entry.id in seen or (entry.identity or "").lower() != identity.lower():
            continue
        found.append((entry.to_site(), "catalog"))
    return found


def _classify(refusal: Refusal) -> ProbeState:
    if refusal.data.get("allocation") is False:
        return "missing_allocation"
    if refusal.reason in (RefusalReason.NEEDS_SIGN_IN, RefusalReason.INTERACTIVE_AUTH_REQUIRED):
        return "needs_sign_in"
    if refusal.reason in _NOT_CONFIGURED:
        return "not_configured"
    return "unreachable"


async def try_site(
    entry: SiteEntry, paths: RelayPaths
) -> tuple[ProbeState, str | None, SiteInfo | None]:
    """Try one site, saved or not: its state, what that means, and what it said of itself."""
    try:
        context = AdapterContext(
            site_id=entry.id,
            state_dir=paths.adapter_dir(entry.id, entry.adapter),
            content=ContentStore(paths.content_dir),
        )
        adapter = registry.build(entry.adapter, entry.config, context)
        need = await adapter.sign_in_needed()
        if need is not None:
            return "needs_sign_in", need.message, None
        info = await adapter.describe()
    except Refusal as refusal:
        return _classify(refusal), refusal.message, None
    except Exception as exc:
        return "unreachable", type(exc).__name__, None
    if info.facts.get("allocation") is False:
        return "missing_allocation", "no allocation on this account", info
    accounts = info.accounts
    return "ready", f"{len(accounts)} account(s)" if accounts else None, info


async def probe_site(
    entry: SiteEntry,
    identity: str,
    origin: Literal["sites", "catalog"],
    paths: RelayPaths,
) -> ProbeResult:
    """Probe one site."""
    state, detail, _info = await try_site(entry, paths)
    return ProbeResult(site=entry.id, identity=identity, origin=origin, state=state, detail=detail)


def probes_file(paths: RelayPaths) -> Path:
    """Return where probe results are kept."""
    return paths.state_dir / "identity" / "probes.json"


def load(paths: RelayPaths) -> dict[str, Any]:
    """Return the last probe results, by site id."""
    path = probes_file(paths)
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}  # pyright: ignore[reportUnknownVariableType]


async def probe_identity(identity: str, paths: RelayPaths) -> list[ProbeResult]:
    """Probe every site that names ``identity`` and record the results."""
    results = [
        await probe_site(entry, identity, origin, paths)
        for entry, origin in sites_for(identity, paths)
    ]
    current = load(paths)
    for item in results:
        current[item.site] = item.model_dump(mode="json")
    write_json_atomic(probes_file(paths), current)
    return results
