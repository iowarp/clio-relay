"""A service's plan before anything starts: its path to the desktop and its ports.

The start and the plan JARVIS makes before the person's yes
(:meth:`clio_relay.serve.runtime.ServeRuntime.plan_job`) both use it, so the two
build the same job.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from clio_relay import jarvis_submit
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import ServiceSpec
from clio_relay.serve import config as serve_settings
from clio_relay.serve.config import Rendezvous, SiteServe, frp_unusable
from clio_relay.serve.plan import ServePlan, check_wrappable, plan_ports, service_name

logger = logging.getLogger(__name__)


def serve_plan(
    adapter: Any,
    spec: ServiceSpec,
    *,
    operation_id: str,
    settings_file: Path,
    entry: Any = None,
    choices: dict[str, Any] | None = None,
) -> ServePlan:
    """The service's plan before anything starts: no secret, no hop, nothing kept."""
    site_id = str(adapter.context.site_id)
    loaded = serve_settings.load(settings_file)
    site = serve_settings.site_serve(loaded, site_id, getattr(entry, "serve", None))
    check_wrappable(spec)
    package = bool(spec.job.steps())
    if package and not getattr(adapter, "runs_packages", False):
        raise jarvis_submit.not_run_here(str(adapter.name))
    mode = path_of(adapter, site, loaded.rendezvous)
    adopt = package and bool(getattr(adapter, "reads_service_runtimes", False))
    idle = spec.lifetime.idle_release
    return ServePlan(
        operation_id=operation_id,
        site=site_id,
        adapter=str(adapter.name),
        service=service_name(spec),
        mode=mode,
        ports=plan_ports(spec, adopt=adopt, choices=choices),
        rendezvous=loaded.rendezvous if mode == "frp" else None,
        site_serve=site,
        bound_to=spec.lifetime.bound_to,
        idle_release_s=idle.total_seconds() if idle else None,
        package=package,
    )


def path_of(adapter: Any, site: SiteServe, rendezvous: Rendezvous | None) -> Any:
    """Pick frp or ssh. ``auto`` takes frp only when it can work, and else ssh."""
    has_ssh = callable(getattr(adapter, "ssh_argv", None))
    site_id = str(adapter.context.site_id)
    if site.path == "frp":
        if rendezvous is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "no rendezvous host is set; see docs/rendezvous-host.md",
            )
        return "frp"
    if site.path == "auto" and rendezvous is not None:
        unusable = frp_unusable(site, rendezvous)
        if unusable is None:
            return "frp"
        if not has_ssh:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"frp cannot carry a port from {site_id}: {unusable}, and its adapter "
                "has no ssh to forward one",
                site=site_id,
            )
        logger.info("serve on %s: ssh, because frp %s", site_id, unusable)
        return "ssh"
    if has_ssh:
        return "ssh"
    raise Refusal(
        RefusalReason.INVALID_CONFIG,
        f"site {site_id!r} has no way to carry a port: set a rendezvous host "
        "(docs/rendezvous-host.md), or reach the site through an adapter that uses ssh",
        site=site_id,
    )
