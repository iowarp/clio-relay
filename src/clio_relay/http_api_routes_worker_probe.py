"""Worker/target identity read routes (iowarp/clio-relay#179 dial burn-down).

``cli_remote_worker_probe.py``'s ``_remote_worker_info``/``_remote_target_
identity`` previously reached these two facts exclusively through a
per-operation ``ssh ... clio-relay endpoint worker-info|target-info``
dial, even when a live owned-session channel already existed for a
re-verification (``cli_remote_worker_attach.py``, ``cli_scheduler.py``'s
scheduler-provider gate, ``cli_jarvis_mcp_validate.py``/``cli_remote_mcp_
validate.py``). Both facts are pure, side-effect-free reads of the
serving process's own local state (``worker_runtime_info``: durable
worker-heartbeat/installation records; the target document: hostname,
machine-id marker, scheduler identity) -- the SAME computation
``cli_endpoint.py``'s ``endpoint worker-info``/``target-info`` commands
already run locally on a cluster when NOT dialing remotely
(``remote_cli.should_execute_on_cluster`` false). These routes expose
that identical local capability over the held channel; they carry no
owner-session-scoped state so are reachable from any authenticated
channel, like ``GET /workers`` and ``GET /healthz``.

The MANY call sites of ``_remote_worker_info``/``_remote_target_identity``
that run before any session exists (``cli_session_start.py``,
``live_acceptance_transport.py``, bootstrap acceptance) have no channel to
ride by construction -- this is expected and correct: they take the typed
ssh fallback every time, exactly as clio-relay#179's design intends for a
cold CLI invocation.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import hashlib
import socket
from pathlib import Path

from fastapi import FastAPI
from fastapi.params import Depends

from clio_relay import door_errors
from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import ConfigurationError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.scheduler_providers import provider_for_scheduler
from clio_relay.worker_runtime_verification import worker_runtime_info

CLUSTER_TARGET_INFO_SCHEMA = "clio-relay.cluster-target-info.v1"


def _physical_site_marker_sha256(path: Path) -> str:
    """Hash the exact physical-site marker bytes used by operator pinning tools.

    Duplicated (not imported) from ``cli_endpoint._physical_site_marker_
    sha256`` deliberately: that module is a Typer command group with a much
    heavier, CLI-only import graph (``EndpointWorker`` and friends) that has
    no business loading into the HTTP API's dependency graph at app-import
    time for one four-line pure function.
    """
    try:
        marker = path.read_bytes()
    except OSError as exc:
        raise ConfigurationError(f"could not read physical site marker: {exc}") from exc
    if not marker.strip():
        raise ConfigurationError("physical site marker is empty")
    return hashlib.sha256(marker).hexdigest()


def register_worker_probe_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the worker-info/target-info identity read routes.

    ``ctx`` is accepted (unused) only to match every other
    ``register_*_routes`` call shape -- see module docstring.
    """

    @app.get("/worker-info", dependencies=[auth_dependency])
    def worker_info(
        cluster: str,
        freshness_seconds: float = 120.0,
        readiness_only: bool = False,
        pinned_install_receipt_path: str | None = None,
        dev_mode: bool = False,
    ) -> dict[str, object]:
        try:
            return worker_runtime_info(
                cluster=cluster,
                freshness_seconds=freshness_seconds,
                readiness_only=readiness_only,
                pinned_install_receipt_path=pinned_install_receipt_path,
                dev_mode=dev_mode_enabled(cluster_dev_mode=dev_mode),
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.get("/target-info", dependencies=[auth_dependency])
    def target_info(scheduler_provider: str = "external") -> dict[str, object]:
        try:
            provider = provider_for_scheduler(scheduler_provider)
            site_marker_sha256 = _physical_site_marker_sha256(Path("/etc/machine-id"))
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc
        return {
            "schema_version": CLUSTER_TARGET_INFO_SCHEMA,
            "hostname": socket.gethostname(),
            "fqdn": socket.getfqdn(),
            "site_marker_sha256": site_marker_sha256,
            "scheduler_provider": provider.name,
            "scheduler_cluster_name": provider.scheduler_cluster_name(),
        }


__all__ = ["CLUSTER_TARGET_INFO_SCHEMA", "register_worker_probe_routes"]
