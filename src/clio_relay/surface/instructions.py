"""The server's ``instructions``: what relay does, and on which sites and packages.

Clients read ``instructions`` from ``initialize`` or ``server/discover``, and many
put it in the agent's system prompt. It names the sites and the staged packages,
so an agent asked to run a code on a site knows relay is the way before it
calls any tool. The text comes from the sites file and the package catalog, and
:func:`keep_current` refreshes it while the server runs.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from clio_relay.packages import PackageEntry
from clio_relay.sites.models import SiteEntry

if TYPE_CHECKING:
    from fastmcp import FastMCP

    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

GENERAL = (
    "Relay runs work on the sites in its sites file. Call describe first. "
    "run submits a job (a PSI/J JobSpec) and is a task when the client supports tasks; "
    "exec runs one short command; files lists, reads, writes and stages files; "
    "serve starts a service; packages lists the JARVIS packages relay can run and stages "
    "one (from relay's package sources, or a directory on the relay host) before its first "
    "run. "
    "Reads never change anything and never ask: describe (a site, an operation or a "
    "service with job=..., the staged packages with packages=true), exec_read (a command "
    "on relay's read-only list, such as squeue or jarvis sched info +json) and files_read "
    "(list, stat, read, has). "
    "cancel cancels a job or stops a service; site_clean removes relay's deployment from "
    "a site after the person's yes, keeping the Spack store. "
    "Refusals carry a typed reason in structuredContent.refusal. "
    "A site's guide, when it has one, comes back in describe(site=...) as skill.text; "
    "it is also the resource relay://sites/<site>/skill, which files cannot read."
)
MAX_LISTED = 12
SUMMARY_CHARS = 100
REFRESH_S = 5.0


def site_line(site: SiteEntry) -> str:
    """``ares (Ares; ssh, slurm, guarded)``."""
    traits = [site.adapter]
    scheduler = site.config.get("scheduler")
    if isinstance(scheduler, str) and scheduler:
        traits.append(scheduler)
    traits.append(site.policy)
    named = f"{site.name}; " if site.name and site.name.lower() != site.id.lower() else ""
    return f"{site.id} ({named}{', '.join(traits)})"


def package_line(entry: PackageEntry) -> str:
    """``repo.pkg 1.0 (the summary's first sentence)``."""
    summary = " ".join((entry.summary or "").split())
    first = summary.split(". ", 1)[0].rstrip(".")
    if len(first) > SUMMARY_CHARS:
        first = first[: SUMMARY_CHARS - 3].rstrip() + "..."
    return f"{entry.name} {entry.version}" + (f" ({first})" if first else "")


def _listed(lines: Iterable[str]) -> str:
    items = list(lines)
    shown = items[:MAX_LISTED]
    if len(items) > MAX_LISTED:
        shown.append(f"and {len(items) - MAX_LISTED} more (describe lists them all)")
    return "; ".join(shown)


def compose(sites: list[SiteEntry], packages: list[PackageEntry]) -> str:
    """The instructions for these sites and packages."""
    if not sites:
        return f"{GENERAL} No compute sites are configured yet."
    parts = [GENERAL, f"Compute sites configured for relay: {_listed(map(site_line, sites))}."]
    if packages:
        ordered = sorted(packages, key=lambda entry: entry.name)
        parts.append(f"Packages staged to run on them: {_listed(map(package_line, ordered))}.")
    parts.append(
        "To run a code or a package on these sites, use relay's describe and run tools "
        "instead of building or launching it by hand: describe(site=...) gives a site's "
        "packages with their parameters, and run takes package: {name, parameters}."
    )
    return " ".join(parts)


def current(service: RelayService) -> str:
    """Compose the instructions from what relay has now."""
    try:
        sites = service.sites.list()
    except Exception:  # a broken sites file is reported by describe, not here
        logger.debug("relay could not read the sites file for its instructions", exc_info=True)
        sites = []
    try:
        packages = service.packages.newest()
    except Exception:
        logger.debug("relay could not read the package catalog", exc_info=True)
        packages = []
    return compose(sites, packages)


async def keep_current(
    server: FastMCP[Any], service: RelayService, interval_s: float = REFRESH_S
) -> None:
    """Refresh ``server.instructions`` until cancelled.

    The CLI edits the sites file and stages packages from another process, so
    relay looks again every ``interval_s`` seconds. A client reads the text when
    it connects or discovers the server.
    """
    while True:
        await asyncio.sleep(interval_s)
        try:
            server.instructions = await asyncio.to_thread(current, service)
        except Exception:
            logger.debug("relay could not refresh its instructions", exc_info=True)
