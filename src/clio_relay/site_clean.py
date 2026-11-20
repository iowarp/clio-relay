"""``clio-relay site clean``: remove what relay deployed on a site, so the next use redeploys.

It stops relay's processes on the site first (:mod:`clio_relay.shutdown`), then
removes the worker and its binaries, relay's scripts and job directories, the
JARVIS bootstrap venv, and ``~/.clio-relay`` and ``~/.config/clio-relay``. The
person's Spack store (``<site home>/spack`` and ``<site home>/spack-config``) is
kept unless ``remove_spack`` says otherwise; rebuilding it takes hours. Then it
drops relay's local state for the site, so the next call signs in and places
everything again. It refuses while a job of the site still runs: its directory is
among what goes.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, Any

from clio_relay.adapters.base import CleansSite, ShutsDown
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.shutdown import shutdown

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.site_clean.v1"
COMMON_TARGETS = ("~/.clio-relay", "~/.config/clio-relay")
"""Relay's state on any site, whichever adapter reaches it."""


def _cleaners(service: RelayService, site_id: str) -> tuple[list[Any], list[str]]:
    """The site's adapters that can clean, primary first, and every adapter name."""
    site = service.site(site_id)
    names = [site.adapter, *site.alternatives]
    cleaners: list[Any] = []
    for name in names:
        try:
            adapter = service.adapter_named(site, name)
        except Refusal:
            continue
        if isinstance(adapter, CleansSite):
            cleaners.append(adapter)
    if not cleaners:
        raise Refusal(
            RefusalReason.VERB_NOT_IMPLEMENTED,
            f"no adapter of {site_id!r} can clean the site",
            site=site_id,
        )
    return cleaners, names


def plan(service: RelayService, site_id: str, *, remove_spack: bool = False) -> dict[str, Any]:
    """Return what clean would remove and keep, or refuse while a job of the site runs.

    The first adapter that can clean does it, with every cleaning adapter's paths.
    """
    running = [
        record.operation_id
        for record in service.operations.all()
        if record.site == site_id and record.kind in ("run", "serve") and not record.state.final
    ]
    if running:
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"jobs still run on {site_id}: {', '.join(running)}. Cancel them, or wait, "
            "before cleaning the site: their directories are among what goes",
            site=site_id,
        )
    cleaners, names = _cleaners(service, site_id)
    targets: list[str] = []
    keep: list[str] = []
    for cleaner in cleaners:
        found, kept = cleaner.clean_targets()
        targets += found
        keep += kept
    targets = list(dict.fromkeys([*targets, *COMMON_TARGETS, *_hop_dirs(service, site_id)]))
    return {
        "site": site_id,
        "adapter": cleaners[0].name,
        "adapters": names,
        "targets": targets,
        "keep": [] if remove_spack else list(dict.fromkeys(keep)),
    }


def _hop_dirs(service: RelayService, site_id: str) -> list[str]:
    try:
        from clio_relay.serve import config as serve_settings
    except ModuleNotFoundError:
        return []
    site = service.site(site_id)
    loaded = serve_settings.load(serve_settings.settings_path(service.paths.state_dir))
    try:
        serve = serve_settings.site_serve(loaded, site_id, getattr(site, "serve", None))
    except Refusal:
        return []
    return [serve.hop_dir]


def describe_plan(planned: dict[str, Any]) -> str:
    """Return the ask's text: what goes and what stays."""
    kept = ", ".join(planned["keep"]) or "nothing: the Spack store goes too"
    return (
        f"Clean {planned['site']}? Relay stops its processes there, then deletes "
        f"{', '.join(planned['targets'])} on the site, keeping {kept}. The next use "
        "places the worker and JARVIS again."
    )


async def clean(service: RelayService, planned: dict[str, Any]) -> dict[str, Any]:
    """Stop the site's relay processes, remove the deployment, then the local state.

    A cleaner that prunes over its own connection keeps it until the prune is done.
    """
    site_id = str(planned["site"])
    cleaners, _names = _cleaners(service, site_id)
    cleaner = cleaners[0]
    spare = (
        frozenset({cleaner.name})
        if getattr(cleaner, "clean_over_connection", False)
        else frozenset[str]()
    )
    stopped = await shutdown(service, {"site": site_id}, spare=spare)
    report = await cleaner.clean(list(planned["targets"]), list(planned["keep"]))
    if spare and isinstance(cleaner, ShutsDown):
        try:
            found = await cleaner.shutdown()
        except Refusal as refusal:
            stopped["failures"].append(
                {"site": site_id, "error": refusal.reason.value, "detail": refusal.message}
            )
        else:
            stopped["stopped"].extend({"site": site_id, **item} for item in found)
    local: list[str] = []
    for name in planned["adapters"]:
        directory = service.paths.adapter_dir(site_id, name)
        if directory.exists():
            shutil.rmtree(directory, ignore_errors=True)
            local.append(str(directory))
    return {
        "schema": SCHEMA,
        "site": site_id,
        "adapter": planned["adapter"],
        "shutdown": stopped,
        "removed": report.get("removed", []),
        "kept": report.get("kept", []),
        "absent": report.get("absent", []),
        "refused": report.get("refused", []),
        "local_removed": local,
    }
