"""The ``cluster`` registry command group (iowarp/clio-relay#231).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)). ``cluster_app``
has ten commands spanning ~830 body lines -- past the point a single new
module stays in the 150-500-line sweet spot, so ground rule 2's "split
further if a group exceeds ~800" (the setup instructions for this campaign)
applies: this module owns the four **cluster registry CRUD** commands
(``list``/``add``/``pin-target``/``pin-runtime`` -- read, replace-whole, or
patch-one-field a local ``ClusterDefinition``); the six **deployment**
commands (``probe``/``bootstrap``/``install-app``/
``install-endpoint-service``/``restart-endpoint-service``/
``endpoint-service-status`` -- everything that reaches over SSH) are a
second, later slice's real seam, not this one's.

**Domain logic stays where it lives.** ``ClusterRegistry.mutate``/``.load``
(``cluster_config.py``) already are the storage primitives (SS5's
"Registry mutation" row) -- these commands build a validated
``ClusterDefinition``/``ClusterTargetIdentity`` and call them exactly as
they did inside ``cli.py``. None of ``cluster_config.py``'s symbols used
here are audited patch-seam collaborators (``tests/test_cli_patch_seam.py``),
so they are imported directly, matching ``cli.py``'s own prior style.

**What moves here as private helpers, and why.**
``_route_revision_before_edit``/``_warn_if_route_revision_changed`` (the
clio-relay#216 stale-MCP-cache warning) and ``_split_csv`` had every one of
their call sites inside this exact four-command group -- unlike the
cross-cutting helpers left in ``cli.py`` (``_require_cluster``,
``_none_if_blank``, ``_kind_concurrency_options``, each with call sites in
several other groups). Single-caller-group helpers are domain logic for
this group, not shared plumbing, the same reasoning ``cli_api.py``'s
``_require_process_bound_session_api_release`` and ``cli_endpoint.py``'s
``_physical_site_marker_sha256`` document.

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

import json
from typing import Annotated, cast

import typer
from pydantic import ValidationError

from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    ClusterTargetIdentity,
    DirectTransportConfig,
    FrpTransportConfig,
    WorkerCapacityPolicy,
    cluster_route_revision,
    default_registry_path,
)
from clio_relay.errors import ConfigurationError

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
cluster_app = typer.Typer(no_args_is_help=True)


def _route_revision_before_edit(cluster: str) -> str | None:
    """Return the cluster's current route revision, or None when unconfigured."""
    try:
        return cluster_route_revision(
            ClusterRegistry.load(default_registry_path()).require(cluster)
        )
    except ConfigurationError:
        return None


def _warn_if_route_revision_changed(cluster: str, *, before: str | None, after: str) -> None:
    """Warn loudly when an edit strands cached MCP discovery evidence (clio-relay#216).

    ``cluster_route_revision()`` covers every routing-relevant field except
    ``remote_mcp_servers``/``worker_capacity``. Every call routed through
    cached MCP discovery evidence (minted by ``remote-mcp refresh``) fails
    typed only once dispatched -- and only per call, well after this edit --
    unless the operator is warned here, at edit time, that a refresh is due.
    """
    if before is not None and before != after:
        typer.echo(
            f"warning: cluster {cluster!r} route revision changed "
            f"({before[:12]} -> {after[:12]}); cached MCP discovery evidence for this "
            f"cluster is now stale. Run 'clio-relay remote-mcp refresh --cluster {cluster}' "
            "before the next MCP call through it, or every call will fail typed (409) only "
            "after its full dispatch budget.",
            err=True,
        )


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@cluster_app.command("list")
def cluster_list() -> None:
    """List configured clusters."""
    registry = ClusterRegistry.load(default_registry_path())
    for name, definition in sorted(registry.clusters.items()):
        capacity = definition.worker_capacity
        typer.echo(
            f"{name} ssh={definition.ssh_host} profile={definition.bootstrap_profile} "
            f"worker_concurrency={capacity.concurrency} "
            f"control_query_concurrency={capacity.control_query_concurrency}"
        )


@cluster_app.command("add")
def cluster_add(
    name: Annotated[str, typer.Option(help="Cluster name used by relay jobs.")],
    ssh_host: Annotated[str, typer.Option(help="SSH host or alias for the cluster.")],
    bootstrap_profile: Annotated[
        str,
        typer.Option(help="Bootstrap profile for this cluster."),
    ] = "linux-user",
    core_dir: Annotated[
        str,
        typer.Option(help="Remote clio-core directory."),
    ] = "$HOME/.local/share/clio-relay/core",
    spool_dir: Annotated[
        str,
        typer.Option(help="Remote spool directory."),
    ] = "$HOME/.local/share/clio-relay/spool",
    jarvis_bin: Annotated[
        str | None,
        typer.Option(help="Remote JARVIS-CD executable path."),
    ] = None,
    jarvis_resource_graph_profile: Annotated[
        str | None,
        typer.Option(
            help=(
                "Exact JARVIS builtin resource-graph profile selected by the operator; "
                "relay never derives this from the cluster name."
            )
        ),
    ] = None,
    allow_jarvis_resource_graph_build: Annotated[
        bool,
        typer.Option(
            "--allow-jarvis-resource-graph-build/--no-allow-jarvis-resource-graph-build",
            help=(
                "Allow one benchmark-free first-install graph build only after JARVIS "
                "returns structured unavailable for the selected builtin profile."
            ),
        ),
    ] = False,
    spack_executable: Annotated[
        str | None,
        typer.Option(help="Absolute remote Spack executable used by the cluster-side JARVIS MCP."),
    ] = None,
    frpc_bin: Annotated[
        str | None,
        typer.Option(help="Remote frpc executable path."),
    ] = None,
    agent_bin: Annotated[
        str | None,
        typer.Option(help="Remote agent executable path."),
    ] = None,
    agent_adapter: Annotated[
        str,
        typer.Option(help="Remote agent adapter name."),
    ] = "exec",
    scheduler_provider: Annotated[
        str,
        typer.Option(
            help="Registered scheduler provider for relay-owned status/cancel operations."
        ),
    ] = "external",
    worker_concurrency: Annotated[
        int,
        typer.Option(help="Total slot capacity for the managed cluster worker service."),
    ] = 3,
    worker_control_query_concurrency: Annotated[
        int,
        typer.Option(help="Slots reserved within total worker capacity for live control queries."),
    ] = 1,
    worker_kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--worker-kind-concurrency",
            help="Per-kind managed-worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    agent_npm_package: Annotated[
        str | None,
        typer.Option(help="Optional npm package used to install the agent."),
    ] = None,
    agent_npm_bin: Annotated[
        str | None,
        typer.Option(help="Agent binary name provided by npm or PATH."),
    ] = None,
    frp_server_addr: Annotated[
        str,
        typer.Option(help="frps server address for this cluster transport."),
    ] = "",
    frp_server_port: Annotated[
        int,
        typer.Option(help="frps server port for this cluster transport."),
    ] = 443,
    frp_protocol: Annotated[
        str,
        typer.Option(help="frpc-to-frps transport protocol."),
    ] = "wss",
    frp_token_env: Annotated[
        str,
        typer.Option(help="Environment/local-secret key for the frp token."),
    ] = "CLIO_RELAY_FRP_TOKEN",
    stcp_secret_env: Annotated[
        str,
        typer.Option(help="Environment/local-secret key for the stcp secret."),
    ] = "CLIO_RELAY_STCP_SECRET",
    direct_transport: Annotated[
        bool,
        typer.Option(
            "--direct-transport/--no-direct-transport",
            help="Enable optional NAT-punching direct transport optimization.",
        ),
    ] = False,
    direct_transport_mode: Annotated[
        str,
        typer.Option(help="Direct transport mode. Currently only xtcp is supported."),
    ] = "xtcp",
    direct_transport_fallback: Annotated[
        str,
        typer.Option(help="Comma-separated direct transport fallback order ending in queue."),
    ] = "frp_stcp,queue",
    target_hostname: Annotated[
        list[str] | None,
        typer.Option(
            "--target-hostname",
            help="Expected remote hostname; repeat for accepted aliases.",
        ),
    ] = None,
    ssh_host_key_sha256: Annotated[
        list[str] | None,
        typer.Option(
            "--ssh-host-key-sha256",
            help="Expected SSH host-key SHA256 fingerprint; repeat for rotations.",
        ),
    ] = None,
    scheduler_cluster_name: Annotated[
        str | None,
        typer.Option(help="Expected scheduler-native cluster name, such as SLURM ClusterName."),
    ] = None,
    site_marker_sha256: Annotated[
        str | None,
        typer.Option(help="Expected SHA-256 of the remote /etc/machine-id site marker."),
    ] = None,
    dev_mode: Annotated[
        bool,
        typer.Option(
            "--dev-mode/--no-dev-mode",
            help=(
                "clio-relay#211: downgrade this cluster's install/identity/receipt/sha "
                "verification chain to warnings instead of raising. Never for production."
            ),
        ),
    ] = False,
) -> None:
    """Add or update a local cluster definition."""
    import clio_relay.cli as cli

    if (target_hostname is None) != (ssh_host_key_sha256 is None):
        raise typer.BadParameter(
            "--target-hostname and --ssh-host-key-sha256 must be provided together"
        )
    try:
        definition = ClusterDefinition(
            name=name,
            ssh_host=ssh_host,
            bootstrap_profile=bootstrap_profile,
            core_dir=core_dir,
            spool_dir=spool_dir,
            jarvis_bin=jarvis_bin,
            jarvis_resource_graph_profile=cli._none_if_blank(jarvis_resource_graph_profile),
            allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
            spack_executable=cli._none_if_blank(spack_executable),
            frpc_bin=frpc_bin,
            agent_bin=cli._none_if_blank(agent_bin),
            agent_adapter=agent_adapter,
            scheduler_provider=scheduler_provider,
            dev_mode=dev_mode,
            worker_capacity=WorkerCapacityPolicy(
                concurrency=worker_concurrency,
                control_query_concurrency=worker_control_query_concurrency,
                kind_concurrency=cli._kind_concurrency_options(
                    worker_kind_concurrency,
                    param_hint="--worker-kind-concurrency",
                ),
            ),
            target_identity=(
                ClusterTargetIdentity(
                    hostnames=target_hostname,
                    ssh_host_key_sha256=ssh_host_key_sha256,
                    scheduler_cluster_name=cli._none_if_blank(scheduler_cluster_name),
                    site_marker_sha256=cli._none_if_blank(site_marker_sha256),
                )
                if target_hostname is not None and ssh_host_key_sha256 is not None
                else None
            ),
            agent_npm_package=cli._none_if_blank(agent_npm_package),
            agent_npm_bin=cli._none_if_blank(agent_npm_bin),
            frp_transport=FrpTransportConfig(
                protocol=frp_protocol,
                server_addr=frp_server_addr,
                server_port=frp_server_port,
                token_env=frp_token_env,
                stcp_secret_env=stcp_secret_env,
                direct=DirectTransportConfig(
                    enabled=direct_transport,
                    mode=direct_transport_mode,
                    fallback_order=_split_csv(direct_transport_fallback),
                ),
            ),
        )
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    before_revision = _route_revision_before_edit(name)
    ClusterRegistry.mutate(
        default_registry_path(),
        lambda registry: registry.clusters.__setitem__(name, definition),
    )
    _warn_if_route_revision_changed(
        name,
        before=before_revision,
        after=cluster_route_revision(definition),
    )
    typer.echo(f"{name} ssh={ssh_host} profile={bootstrap_profile}")


@cluster_app.command("pin-target")
def cluster_pin_target(
    cluster: Annotated[str, typer.Option(help="Existing configured cluster name.")],
    target_hostname: Annotated[
        list[str] | None,
        typer.Option(
            "--target-hostname",
            help="Expected remote hostname; repeat for accepted aliases.",
        ),
    ] = None,
    ssh_host_key_sha256: Annotated[
        list[str] | None,
        typer.Option(
            "--ssh-host-key-sha256",
            help="Expected SSH host-key SHA256 fingerprint; repeat for key rotations.",
        ),
    ] = None,
    scheduler_cluster_name: Annotated[
        str | None,
        typer.Option(help="Expected scheduler-native cluster name."),
    ] = None,
    site_marker_sha256: Annotated[
        str | None,
        typer.Option(help="Expected SHA-256 of the remote physical site marker."),
    ] = None,
    clear: Annotated[
        bool,
        typer.Option(help="Remove only the existing physical target identity pin."),
    ] = False,
) -> None:
    """Pin or clear one cluster's physical target identity without replacing its config."""
    import clio_relay.cli as cli

    identity_arguments_present = any(
        value is not None
        for value in (
            target_hostname,
            ssh_host_key_sha256,
            scheduler_cluster_name,
            site_marker_sha256,
        )
    )
    if clear and identity_arguments_present:
        raise typer.BadParameter("--clear cannot be combined with target identity values")
    if not clear and (target_hostname is None or ssh_host_key_sha256 is None):
        raise typer.BadParameter(
            "--target-hostname and --ssh-host-key-sha256 are required unless --clear is used"
        )
    target_identity: ClusterTargetIdentity | None = None
    if not clear:
        assert target_hostname is not None
        assert ssh_host_key_sha256 is not None
        try:
            target_identity = ClusterTargetIdentity(
                hostnames=target_hostname,
                ssh_host_key_sha256=ssh_host_key_sha256,
                scheduler_cluster_name=cli._none_if_blank(scheduler_cluster_name),
                site_marker_sha256=cli._none_if_blank(site_marker_sha256),
            )
        except ValidationError as exc:
            raise typer.BadParameter(str(exc)) from exc

    def update_target_identity(registry: ClusterRegistry) -> None:
        registry.require(cluster).target_identity = target_identity

    before_revision = _route_revision_before_edit(cluster)
    registry = ClusterRegistry.mutate(default_registry_path(), update_target_identity)
    definition = registry.require(cluster)
    _warn_if_route_revision_changed(
        cluster,
        before=before_revision,
        after=cluster_route_revision(definition),
    )
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "ssh_host": definition.ssh_host,
                "target_identity": (
                    definition.target_identity.model_dump(mode="json")
                    if definition.target_identity is not None
                    else None
                ),
            },
            indent=2,
        )
    )


@cluster_app.command("pin-runtime")
def cluster_pin_runtime(
    cluster: Annotated[str, typer.Option(help="Existing configured cluster name.")],
    relay_executable: Annotated[
        str | None,
        typer.Option(help="Remote clio-relay executable path pinned to one generation."),
    ] = None,
    install_receipt: Annotated[
        str | None,
        typer.Option(help="Remote pinned install-receipt.json path for this cluster's generation."),
    ] = None,
    clear: Annotated[
        bool,
        typer.Option(help="Remove only the existing pinned runtime identity."),
    ] = False,
) -> None:
    """Pin or clear one cluster's runtime identity without replacing its config.

    Updates only ``relay_executable``/``relay_install_receipt`` on the
    existing entry -- every other field (``remote_mcp_servers``,
    ``target_identity``, worker capacity, transport config, ...) is
    preserved exactly. Unlike ``cluster add``, this never replaces the
    entry wholesale, so it is the sanctioned way to declare the per-cluster
    pin that session-start verification honors (clio-relay#205).
    """
    identity_arguments_present = relay_executable is not None or install_receipt is not None
    if clear and identity_arguments_present:
        raise typer.BadParameter("--clear cannot be combined with pinned runtime values")
    if not clear and not identity_arguments_present:
        raise typer.BadParameter(
            "--relay-executable or --install-receipt is required unless --clear is used"
        )
    default_relay_executable = cast(str, ClusterDefinition.model_fields["relay_executable"].default)

    def update_pinned_runtime(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if clear:
            definition.relay_executable = default_relay_executable
            definition.relay_install_receipt = None
        else:
            if relay_executable is not None:
                definition.relay_executable = relay_executable
            if install_receipt is not None:
                definition.relay_install_receipt = install_receipt

    before_revision = _route_revision_before_edit(cluster)
    try:
        registry = ClusterRegistry.mutate(default_registry_path(), update_pinned_runtime)
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    definition = registry.require(cluster)
    _warn_if_route_revision_changed(
        cluster,
        before=before_revision,
        after=cluster_route_revision(definition),
    )
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "relay_executable": definition.relay_executable,
                "relay_install_receipt": definition.relay_install_receipt,
            },
            indent=2,
        )
    )
