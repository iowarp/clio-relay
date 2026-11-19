"""What ``describe`` reports for relay as a whole and for one site (LQ2, RL5, RL34)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import PackageRef
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.skills import skill_link, skills

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService


async def site_status(service: RelayService, site: SiteEntry) -> dict[str, Any]:
    """Return whether ``site`` is ready, waits for a sign-in, or cannot be used now.

    It asks the adapter that serves ``describe`` whether a sign-in is needed, which
    reads the local credential or connection state and does not run anything remote.
    """
    try:
        adapter = service.adapter_for(site, "describe")
        need = await adapter.sign_in_needed()
    except Refusal as refusal:
        return {"state": "unavailable", "reason": refusal.reason.value, "detail": refusal.message}
    if need is not None:
        return {"state": "needs_sign_in", "provider": need.provider, "detail": need.message}
    return {"state": "ready"}


async def relay_report(service: RelayService) -> dict[str, Any]:
    """List every site with its status, the installed adapters and the verbs."""
    entries = service.sites.list()
    statuses = await asyncio.gather(*(site_status(service, entry) for entry in entries))
    config_dir = service.paths.config_dir
    known = await asyncio.to_thread(skills, config_dir)
    return {
        "sites": [
            {
                "id": entry.id,
                "name": entry.title,
                "adapter": entry.adapter,
                "alternatives": sorted(entry.alternatives),
                "policy": entry.policy,
                "status": status,
                "skill": skill_link(entry, config_dir, known),
            }
            for entry, status in zip(entries, statuses, strict=True)
        ],
        "adapters": registry.names(),
        "verbs": list(VERBS),
    }


def package_listing(service: RelayService, site: SiteEntry) -> list[dict[str, Any]]:
    """List the newest staged version of each package, and whether this site reviewed it."""
    reviewed = getattr(service.guard, "reviewed", None)
    entries = service.packages.all()
    listed: list[dict[str, Any]] = []
    for entry in service.packages.newest():
        item = entry.model_dump(mode="json", exclude={"staged_at"})
        older = [other.version for other in entries if other.name == entry.name]
        if len(older) > 1:
            item["versions"] = older
        if site.policy == "guarded" and callable(reviewed):
            package = PackageRef(name=entry.name, version=entry.version, sha256=entry.sha256)
            item["reviewed"] = reviewed(site.id, package) is not None
        listed.append(item)
    return listed


async def site_report(service: RelayService, site: SiteEntry) -> dict[str, Any]:
    """Report one site: its entry, the route for each verb, and what its adapter says.

    ``packages`` are the JARVIS packages relay can run there. ``run`` takes one
    as ``package: {name, parameters}`` and pins its version and SHA-256.
    """
    described: dict[str, Any] = {
        "site": site.model_dump(mode="json"),
        "routes": [item.model_dump(mode="json") for item in service.site_routes(site)],
        "skill": await asyncio.to_thread(
            skill_link, site, service.paths.config_dir, with_text=True
        ),
        "packages": await asyncio.to_thread(package_listing, service, site),
    }
    try:
        adapter = service.adapter_for(site, "describe")
        described["info"] = (await adapter.describe()).model_dump(mode="json")
        need = await adapter.sign_in_needed()
        described["sign_in"] = None if need is None else need.model_dump(mode="json")
    except Refusal as refusal:
        described["info"] = None
        described["refusal"] = refusal.to_dict()
    return described
