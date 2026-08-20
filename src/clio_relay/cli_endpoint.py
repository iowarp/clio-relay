"""The ``endpoint`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the five
``endpoint_app`` commands (starting a desktop or worker endpoint, listing
durable endpoint registrations, rendering a systemd user service, fresh
worker identity, and physical/scheduler host attestation) move out of the
monolith into their own capped module, per ground rule 2 (SS2) --
``cli.py`` parses and renders only; this module does the same for its own
five commands and nothing more.

**Domain logic stays where it lives.** The commands below delegate to
``endpoint.EndpointWorker``, ``scheduler_providers.provider_for_scheduler``,
``core_queue.ClioCoreQueue``, and ``installation.worker_runtime_info``
exactly as they did inside ``cli.py`` -- all already-correct owner modules,
module-attribute imported since all four are audited patch-seam
collaborators (``tests/test_cli_patch_seam.py``). ``render_endpoint_user_
service``/``write_endpoint_user_service`` (``deployment.py``, not audited)
are imported directly, matching ``cli.py``'s own prior style.

**Fix-forward: ``_resolved_worker_capacity_policy``.** ``endpoint_start``
and ``endpoint_render_user_service`` call ``cli_cluster_deploy.
_resolved_worker_capacity_policy`` via ``import clio_relay.
cli_cluster_deploy as cli_cluster_deploy`` -- not ``cli.<symbol>``. That
function moved to ``cli_cluster_deploy.py`` in an earlier slice of this
same campaign, whose own docstring at the time (incorrectly) claimed
``cluster_install_endpoint_service`` was its only caller; these two
call sites here were left pointing at ``cli._resolved_worker_capacity_
policy``, which stopped existing the moment that slice landed. Caught by
this campaign's broader regression sweep and corrected in place.

**Reassigned patch-seam caller.** ``endpoint.EndpointWorker`` had exactly
one call site in the whole of ``cli.py`` -- ``endpoint_start`` itself --
unlike ``scheduler_providers.provider_for_scheduler`` (16 call sites, stays
``"cli"``) and ``core_queue.ClioCoreQueue``/``installation.
worker_runtime_info`` (both used by several other groups remaining in
``cli.py``, also stay ``"cli"``). This slice reassigns ``EndpointWorker``'s
``caller`` entry in ``AUDITED_COLLABORATORS`` from ``"cli"`` to
``"cli_endpoint"`` and registers this module in ``_GUARDED_CALLERS``, the
same bookkeeping this campaign already did for ``cli_api.py`` and
``cli_release.py``.

**What moves here as a private helper, and why.**
``_physical_site_marker_sha256`` had exactly one call site in the whole of
``cli.py`` -- ``endpoint_target_info`` itself -- unlike the cross-cutting
helpers left in ``cli.py`` (``_require_cluster``, ``_run_or_exit``,
``_kind_concurrency_options``, ``_public_json``, each with multiple call
sites across unrelated groups). A single-caller private helper is domain
logic for this group, not shared plumbing, so it moves with its only
caller, the same reasoning ``cli_api.py``'s ``_require_process_bound_
session_api_release`` documents. (``_resolved_worker_capacity_policy`` is
NOT cli.py-resident -- see the fix-forward note above.)

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
import json
import socket
from pathlib import Path
from typing import Annotated

import typer

import clio_relay.core_queue as core_queue
import clio_relay.endpoint as endpoint
import clio_relay.installation as installation
import clio_relay.scheduler_providers as scheduler_providers
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.deployment import render_endpoint_user_service, write_endpoint_user_service
from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import ConfigurationError
from clio_relay.models import EndpointRole, JobKind
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
endpoint_app = typer.Typer(no_args_is_help=True)


def _physical_site_marker_sha256(path: Path) -> str:
    """Hash the exact physical-site marker bytes used by operator pinning tools."""
    try:
        marker = path.read_bytes()
    except OSError as exc:
        raise ConfigurationError(f"could not read physical site marker: {exc}") from exc
    if not marker.strip():
        raise ConfigurationError("physical site marker is empty")
    return hashlib.sha256(marker).hexdigest()


@endpoint_app.command("start")
def endpoint_start(
    role: Annotated[EndpointRole, typer.Option(help="Endpoint role.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster name for worker endpoints."),
    ] = None,
    once: Annotated[bool, typer.Option(help="Run one worker iteration and exit.")] = False,
    concurrency: Annotated[
        int | None,
        typer.Option(
            help=(
                "Number of in-process worker slots for worker endpoints. Defaults to the "
                "cluster's registered worker_capacity for worker endpoints (clio-relay#219); "
                "1 without a configured cluster."
            )
        ),
    ] = None,
    control_query_concurrency: Annotated[
        int | None,
        typer.Option(
            help=(
                "Slots carved out of total capacity for control-class MCP queries. Defaults "
                "to the cluster's registered worker_capacity for worker endpoints "
                "(clio-relay#219) rather than an unpinned 0: 0 silently starves every "
                "control-class job (jarvis_describe and kin) with no typed reason -- it just "
                "never gets picked up."
            )
        ),
    ] = None,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    scheduler_provider: Annotated[
        str | None,
        typer.Option(help="Explicit scheduler provider for worker observation and cancellation."),
    ] = None,
) -> None:
    """Start a desktop or worker endpoint."""
    import clio_relay.cli as cli
    import clio_relay.cli_cluster_deploy as cli_cluster_deploy

    settings = RelaySettings.from_env()
    definition: ClusterDefinition | None = None
    if role == EndpointRole.WORKER:
        if cluster is None:
            raise typer.BadParameter("--cluster is required for worker endpoints")
        if scheduler_provider is None:
            definition = cli._require_cluster(cluster)
    if definition is not None:
        # clio-relay#219: resolve unpinned concurrency/control_query_concurrency from
        # the cluster's own registered WorkerCapacityPolicy (the same resolution
        # `endpoint render-user-service` already applies) instead of this command's
        # own disconnected CLI defaults -- a fresh worker deployment that never pins
        # these flags otherwise gets control_query_concurrency=0 and silently
        # starves every control-class job.
        capacity = cli_cluster_deploy._resolved_worker_capacity_policy(
            definition,
            concurrency=concurrency,
            control_query_concurrency=control_query_concurrency,
            kind_concurrency=kind_concurrency,
            clear_kind_concurrency=False,
        )
        resolved_concurrency = capacity.concurrency
        resolved_control_query_concurrency = capacity.control_query_concurrency
        resolved_kind_concurrency: dict[JobKind, int] = capacity.kind_concurrency
    else:
        # No cluster is configured (desktop role): preserve the historical
        # single-slot, no-reserved-control-capacity default exactly.
        # WorkerCapacityPolicy requires concurrency>=2, which does not fit this
        # role's single-slot default, so its own bounds are enforced directly.
        resolved_concurrency = 1 if concurrency is None else concurrency
        resolved_control_query_concurrency = (
            0 if control_query_concurrency is None else control_query_concurrency
        )
        if resolved_concurrency < 1:
            raise typer.BadParameter("--concurrency must be at least 1")
        if resolved_control_query_concurrency < 0:
            raise typer.BadParameter("--control-query-concurrency must not be negative")
        if resolved_control_query_concurrency >= resolved_concurrency:
            raise typer.BadParameter("--control-query-concurrency must be less than --concurrency")
        resolved_kind_concurrency = cli._kind_concurrency_options(kind_concurrency)
    if role == EndpointRole.WORKER and resolved_control_query_concurrency == 0:
        # clio-relay#219: a worker with zero control-query capacity accepts
        # describe-class submissions and never runs them -- indistinguishable
        # from slow until an operator discovers the knob. Warn loudly at
        # startup instead of leaving that silent.
        typer.echo(
            f"warning: worker {cluster or 'local'!r} is starting with "
            "control_query_concurrency=0; every describe-class control-query job "
            "submitted to it will queue forever with no typed reason. Pass "
            "--control-query-concurrency N (N >= 1) here, or bake it into the persisted "
            "systemd unit with 'clio-relay endpoint render-user-service --cluster "
            f"{cluster or '<cluster>'} --control-query-concurrency N'.",
            err=True,
        )
    selected_scheduler = scheduler_provider
    if selected_scheduler is None and definition is not None:
        selected_scheduler = definition.scheduler_provider
    worker = endpoint.EndpointWorker(
        role=role,
        settings=settings,
        cluster=cluster or "local",
        concurrency=resolved_concurrency,
        control_query_concurrency=resolved_control_query_concurrency,
        kind_concurrency=resolved_kind_concurrency,
        scheduler_provider=(
            scheduler_providers.provider_for_scheduler(selected_scheduler)
            if role == EndpointRole.WORKER
            else None
        ),
    )
    try:
        worker.register()
        if once:
            worker.run_once()
            return
        worker.serve_forever()
    finally:
        worker.close()


@endpoint_app.command("status")
def endpoint_status(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional endpoint cluster filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global endpoint source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum endpoint source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """Show one stable source window of durable endpoint registrations."""
    import clio_relay.cli as cli

    settings = RelaySettings.from_env()
    queue = core_queue.ClioCoreQueue(settings.core_dir)
    queue.initialize()
    endpoints, next_cursor, total = queue.list_endpoints_page(
        cursor=cursor,
        limit=limit,
        cluster=cluster,
    )
    typer.echo(
        cli._public_json(
            {
                "endpoints": [item.model_dump(mode="json") for item in endpoints],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_endpoint_sequence_high_water",
                "filters_apply_within_source_window": True,
                "core_dir": str(settings.core_dir),
                "spool_dir": str(settings.spool_dir),
            }
        )
    )


@endpoint_app.command("render-user-service")
def endpoint_render_user_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the systemd user service."),
    ] = None,
    concurrency: Annotated[
        int | None,
        typer.Option(help="Number of in-process worker slots for the user service."),
    ] = None,
    control_query_concurrency: Annotated[
        int | None,
        typer.Option(help="Slots reserved within total capacity for live control queries."),
    ] = None,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    clear_kind_concurrency: Annotated[
        bool,
        typer.Option(help="Clear every persisted per-kind override in the rendered unit."),
    ] = False,
) -> None:
    """Render a sudo-less systemd user service for a worker endpoint."""
    import clio_relay.cli as cli
    import clio_relay.cli_cluster_deploy as cli_cluster_deploy

    definition = cli._require_cluster(cluster)
    capacity = cli_cluster_deploy._resolved_worker_capacity_policy(
        definition,
        concurrency=concurrency,
        control_query_concurrency=control_query_concurrency,
        kind_concurrency=kind_concurrency,
        clear_kind_concurrency=clear_kind_concurrency,
    )
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=capacity.concurrency,
        control_query_concurrency=capacity.control_query_concurrency,
        kind_concurrency=capacity.kind_concurrency,
    )
    if output is None:
        typer.echo(service_text)
        return
    typer.echo(write_endpoint_user_service(output, service_text))


@endpoint_app.command("worker-info")
def endpoint_worker_info(
    cluster: Annotated[str, typer.Option(help="Configured worker cluster name.")],
    freshness_seconds: Annotated[
        float,
        typer.Option(help="Maximum acceptable durable worker heartbeat age."),
    ] = 120.0,
    readiness_only: Annotated[
        bool,
        typer.Option(help="Return bounded readiness flags without detailed installation records."),
    ] = False,
    pinned_install_receipt_path: Annotated[
        str | None,
        typer.Option(
            "--pinned-install-receipt-path",
            help=(
                "Cluster-registered relay_install_receipt path (this host's "
                "own pinned runtime) to verify the worker against, instead of "
                "this invocation's ambient current installation."
            ),
        ),
    ] = None,
    dev_mode: Annotated[
        bool,
        typer.Option(
            help=(
                "clio-relay#211: cluster-registered dev_mode, threaded in by the "
                "caller. Combined with CLIO_RELAY_DEV_MODE on this host either way."
            ),
        ),
    ] = False,
) -> None:
    """Report fresh process-bound identity for the active cluster worker."""
    import clio_relay.cli as cli

    cli._run_or_exit(
        lambda: typer.echo(
            json.dumps(
                installation.worker_runtime_info(
                    cluster=cluster,
                    freshness_seconds=freshness_seconds,
                    readiness_only=readiness_only,
                    pinned_install_receipt_path=pinned_install_receipt_path,
                    dev_mode=dev_mode_enabled(cluster_dev_mode=dev_mode),
                ),
                indent=2,
            )
        )
    )


@endpoint_app.command("target-info", hidden=True)
def endpoint_target_info(
    scheduler_provider: Annotated[
        str,
        typer.Option(help="Configured scheduler provider to attest."),
    ] = "external",
) -> None:
    """Report physical host and scheduler identity from the cluster process context."""
    import clio_relay.cli as cli

    def action() -> None:
        provider = scheduler_providers.provider_for_scheduler(scheduler_provider)
        scheduler_cluster_name = provider.scheduler_cluster_name()
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.cluster-target-info.v1",
                    "hostname": socket.gethostname(),
                    "fqdn": socket.getfqdn(),
                    "site_marker_sha256": _physical_site_marker_sha256(Path("/etc/machine-id")),
                    "scheduler_provider": provider.name,
                    "scheduler_cluster_name": scheduler_cluster_name,
                },
                indent=2,
            )
        )

    cli._run_or_exit(action)
