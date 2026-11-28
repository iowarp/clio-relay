"""What ``describe`` reports for relay as a whole and for one site (LQ2, RL5, RL34)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from clio_relay import jarvis_submit
from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS
from clio_relay.jarvis_site import JARVIS_PIN
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import PackageRef
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import exec_plan, job_watch
from clio_relay.surface.job_card import site_jobs
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


async def package_listing(service: RelayService, site: SiteEntry) -> dict[str, Any]:
    """List the packages the site's JARVIS root holds now, and whether this site reviewed each.

    ``jarvis`` says whether the root is set up: its path and the jarvis-cd pin,
    or ``set_up: false`` with the setup step (:data:`~clio_relay.jarvis_submit.SETUP_STEP`).

    The listing runs on the site (:meth:`~clio_relay.packages.PackageCatalog.refresh`);
    when it cannot, the last one relay read is shown with the reason.
    """
    note: dict[str, Any] = {}
    try:
        adapter = service.adapter_for(site, "run")
        if not adapter.runs_packages:
            return {"packages": [], "packages_note": f"the {adapter.name} adapter runs no packages"}
        home = await adapter.jarvis_home()
        root = None if home is None else jarvis_submit.site_root(home)
        if await service.packages.refresh(site.id, adapter) is None:
            # A root that is not set up holds nothing, whatever relay listed before.
            setup = {"root": root, "set_up": False, "setup": jarvis_submit.SETUP_STEP}
            return {"packages": [], "jarvis": setup}
        note["jarvis"] = {"root": root, "set_up": True, "jarvis_cd": JARVIS_PIN}
    except Refusal as refusal:
        note["packages_note"] = (
            f"the last listing relay read; listing now failed: {refusal.message}"
        )
    reviewed = getattr(service.guard, "reviewed", None)
    listed: list[dict[str, Any]] = []
    for entry in service.packages.all(site.id):
        item = entry.model_dump(mode="json", exclude={"files"})
        if site.policy == "guarded" and callable(reviewed):
            package = PackageRef(name=entry.name, version=entry.version, sha256=entry.digest)
            item["reviewed"] = reviewed(site.id, package) is not None
        listed.append(item)
    return {"packages": listed, **note}


async def site_report(service: RelayService, site: SiteEntry) -> dict[str, Any]:
    """Report one site: its entry, the route for each verb, and what its adapter says.

    ``packages`` are the JARVIS packages the site's root holds, as the site lists
    them. ``run`` takes one as ``package: {name, parameters}`` and pins its
    version and review digest.
    The site's cluster facts are JARVIS's, not relay's: an adapter that reads
    them reports ``info.facts.resource_graph``, with the graph's path and
    ``source`` (``setting``, ``site`` for the graph the site publishes as
    ``$JARVIS_RESOURCE_GRAPH``, or ``relay`` for the home's own), the
    install decisions JARVIS records, and the net report of ``jarvis rg net
    show`` (``None`` until the graph has a ``net`` section). Only a site that
    publishes no graph is offered ``collect``. ``jobs`` are the
    site's open jobs and its latest ended ones, each as its job card, observed
    in the last 30 s (:func:`job_watch.current`).
    """
    await job_watch.current(service, site.id)
    described: dict[str, Any] = {
        "site": site.model_dump(mode="json"),
        "routes": [item.model_dump(mode="json") for item in service.site_routes(site)],
        "skill": await asyncio.to_thread(
            skill_link, site, service.paths.config_dir, with_text=True
        ),
        **(await package_listing(service, site)),
        "jobs": site_jobs(service, site.id),
    }
    try:
        adapter = service.adapter_for(site, "describe")
        info = (await adapter.describe()).model_dump(mode="json")
        _collect_on_default_queue(info, site.defaults.queue)
        exec_plan.offer(service, site.id, info)
        described["info"] = info
        need = await adapter.sign_in_needed()
        described["sign_in"] = None if need is None else need.model_dump(mode="json")
    except Refusal as refusal:
        described["info"] = None
        described["refusal"] = refusal.to_dict()
    return described


def _collect_on_default_queue(info: dict[str, Any], queue: str | None) -> None:
    """Send the graph collection the adapter offers to the site's default queue."""
    facts: object = info.get("facts")
    if not queue or not isinstance(facts, dict):
        return
    graph: object = cast(dict[str, Any], facts).get("resource_graph")
    if not isinstance(graph, dict):
        return
    collect: object = cast(dict[str, Any], graph).get("collect")
    if not isinstance(collect, dict):
        return
    offered = cast(dict[str, Any], collect)
    command: object = offered.get("exec")
    if isinstance(command, list):
        cast(list[str], command).append(queue)
        offered["queue"] = queue
