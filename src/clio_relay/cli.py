"""Command-line interface for clio-relay."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Callable
from json import JSONDecodeError
from pathlib import Path
from typing import Annotated, cast

import typer
import uvicorn
import yaml
from pydantic import ValidationError

from clio_relay.bootstrap import (
    bootstrap_cluster_over_ssh,
    install_cluster_app_over_ssh,
    install_local_frp,
    package_source_root,
)
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    DirectTransportConfig,
    FrpTransportConfig,
    default_registry_path,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.deployment import (
    install_endpoint_user_service_over_ssh,
    render_endpoint_user_service,
    write_endpoint_user_service,
)
from clio_relay.doctor import run_cluster_doctor, run_doctor
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_check import run_frpc_connection_check
from clio_relay.jarvis_mcp import jarvis_mcp_server, jarvis_mcp_server_args
from clio_relay.live_acceptance import LiveAcceptanceOptions, run_live_acceptance
from clio_relay.mcp_server import render_agent_mcp_profile, serve_stdio
from clio_relay.models import (
    Cursor,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
    ServiceRuntimeSpec,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_queue,
    list_queue_jobs,
    worker_status,
)
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)
from clio_relay.relay_ops import (
    cancel_job as request_cancel_job,
)
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    monitor_job,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)
from clio_relay.relay_ops import (
    job_status as get_job_status,
)
from clio_relay.remote_cli import (
    run_remote_clio,
    should_execute_on_cluster,
    stage_jarvis_yaml,
    write_remote_file,
)
from clio_relay.service_runtime import ServiceRuntimeSupervisor
from clio_relay.session_lifecycle import (
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
)

app = typer.Typer(no_args_is_help=True)
endpoint_app = typer.Typer(no_args_is_help=True)
relay_host_app = typer.Typer(no_args_is_help=True)
job_app = typer.Typer(no_args_is_help=True)
cluster_app = typer.Typer(no_args_is_help=True)
agent_app = typer.Typer(no_args_is_help=True)
monitor_app = typer.Typer(no_args_is_help=True)
api_app = typer.Typer(no_args_is_help=True)
session_app = typer.Typer(no_args_is_help=True)
gateway_app = typer.Typer(no_args_is_help=True)
queue_app = typer.Typer(no_args_is_help=True)
worker_app = typer.Typer(no_args_is_help=True)

app.add_typer(endpoint_app, name="endpoint")
app.add_typer(relay_host_app, name="relay-host")
app.add_typer(job_app, name="job")
app.add_typer(cluster_app, name="cluster")
app.add_typer(agent_app, name="agent")
app.add_typer(monitor_app, name="monitor")
app.add_typer(api_app, name="api")
app.add_typer(session_app, name="session")
app.add_typer(gateway_app, name="gateway")
app.add_typer(queue_app, name="queue")
app.add_typer(worker_app, name="worker")


@app.callback()
def main() -> None:
    """Run clio-relay commands."""


@app.command()
def init() -> None:
    """Initialize local queue, spool, and cluster registry files."""
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    settings.spool_dir.mkdir(parents=True, exist_ok=True)
    registry = ClusterRegistry.load(default_registry_path())
    typer.echo(
        f"initialized core={settings.core_dir} spool={settings.spool_dir} "
        f"clusters={','.join(sorted(registry.clusters))}"
    )


@relay_host_app.command("render-frps-config")
def render_frps(
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to CLIO_RELAY_FRP_TOKEN."),
    ] = None,
    bind_port: Annotated[int, typer.Option(help="frps bind port.")] = 7000,
    transport_protocol: Annotated[
        FrpTransportProtocol,
        typer.Option(help="frpc-to-frps transport protocol."),
    ] = FrpTransportProtocol.WSS,
    dashboard_port: Annotated[
        int | None,
        typer.Option(help="Optional frps dashboard port."),
    ] = None,
) -> None:
    """Render an frps config with no relay application state."""
    _run_or_exit(
        lambda: typer.echo(
            render_frps_config(
                FrpsConfig(
                    bind_port=bind_port,
                    token=_resolve_env_secret(token, "CLIO_RELAY_FRP_TOKEN", "frp token"),
                    transport_protocol=transport_protocol,
                    dashboard_port=dashboard_port,
                )
            )
        )
    )


@relay_host_app.command("render-frpc-config")
def render_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy name.")] = "relay-stcp",
) -> None:
    """Render an frpc config using the cluster's configured frp transport."""

    def action() -> None:
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        typer.echo(
            render_frpc_config(
                FrpcConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=_resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    proxy_name=proxy_name,
                    local_port=local_port,
                    secret_key=_resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    _run_or_exit(action)


@relay_host_app.command("test-frpc-connection")
def test_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy name.")] = "relay-stcp-live-check",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds frpc must stay connected before success."),
    ] = 10.0,
) -> None:
    """Run a live frpc login check for the cluster transport."""

    def action() -> None:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        config = FrpcConfig(
            server_addr=server_addr,
            server_port=transport.server_port,
            token=_resolve_env_secret(token, transport.token_env, "frp token"),
            transport_protocol=FrpTransportProtocol(transport.protocol),
            proxy_name=proxy_name,
            local_port=local_port,
            secret_key=_resolve_env_secret(secret_key, transport.stcp_secret_env, "stcp secret"),
        )
        _echo_lines(
            run_frpc_connection_check(
                frpc_bin=settings.frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
            )
        )

    _run_or_exit(action)


@relay_host_app.command("render-frpc-visitor-config")
def render_frpc_visitor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    server_name: Annotated[str, typer.Option(help="Cluster-side stcp proxy name.")] = "relay-stcp",
    visitor_name: Annotated[
        str,
        typer.Option(help="Desktop-side stcp visitor name."),
    ] = "relay-stcp-visitor",
    bind_addr: Annotated[
        str,
        typer.Option(help="Local desktop visitor bind address."),
    ] = "127.0.0.1",
) -> None:
    """Render a desktop-side frpc STCP visitor config."""

    def action() -> None:
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        typer.echo(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=_resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    visitor_name=visitor_name,
                    server_name=server_name,
                    bind_addr=bind_addr,
                    bind_port=bind_port,
                    secret_key=_resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    _run_or_exit(action)


@relay_host_app.command("test-http-transport")
def test_http_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy/server name.")] = "relay-http",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the transport."),
    ] = 30.0,
) -> None:
    """Run an end-to-end HTTP health check through frp STCP."""
    settings = RelaySettings.from_env()
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            run_frp_http_probe(
                cluster=cluster,
                definition=definition,
                frpc_bin=settings.frpc_bin,
                token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
                secret_key=_resolve_env_secret(
                    secret_key,
                    definition.frp_transport.stcp_secret_env,
                    "stcp secret",
                ),
                local_bind_port=local_bind_port,
                remote_api_port=remote_api_port,
                proxy_name=proxy_name,
                api_token=settings.api_token,
                timeout_seconds=timeout_seconds,
            )
        )
    )


@relay_host_app.command("test-direct-transport")
def test_direct_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp/xtcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[
        str,
        typer.Option(help="xtcp proxy/server name."),
    ] = "relay-http-direct",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through direct transport."),
    ] = 30.0,
    allow_stcp_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-stcp-fallback/--no-allow-stcp-fallback",
            help="Allow fallback to STCP if XTCP fails.",
        ),
    ] = False,
) -> None:
    """Run an end-to-end HTTP health check through frp XTCP direct transport."""
    settings = RelaySettings.from_env()
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            run_frp_direct_http_probe(
                cluster=cluster,
                definition=definition,
                frpc_bin=settings.frpc_bin,
                token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
                secret_key=_resolve_env_secret(
                    secret_key,
                    definition.frp_transport.stcp_secret_env,
                    "stcp/xtcp secret",
                ),
                local_bind_port=local_bind_port,
                remote_api_port=remote_api_port,
                proxy_name=proxy_name,
                api_token=settings.api_token,
                timeout_seconds=timeout_seconds,
                allow_stcp_fallback=allow_stcp_fallback,
            )
        )
    )


@relay_host_app.command("test-ssh-transport")
def test_ssh_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop SSH-forward bind port.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    session_id: Annotated[
        str,
        typer.Option(help="Owned remote relay session id for this probe."),
    ] = "relay-ssh-forward-test",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the SSH forward."),
    ] = 30.0,
    detach_remote: Annotated[
        bool,
        typer.Option(
            "--detach-remote/--teardown-remote",
            help="Leave the remote API session running after the local SSH probe exits.",
        ),
    ] = False,
) -> None:
    """Run an end-to-end HTTP health check through SSH local port forwarding."""
    settings = RelaySettings.from_env()
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            run_ssh_forward_http_probe(
                cluster=cluster,
                definition=definition,
                local_bind_port=local_bind_port,
                remote_api_port=remote_api_port,
                session_id=session_id,
                api_token=settings.api_token,
                timeout_seconds=timeout_seconds,
                detach_remote=detach_remote,
            )
        )
    )


@endpoint_app.command("start")
def endpoint_start(
    role: Annotated[EndpointRole, typer.Option(help="Endpoint role.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster name for worker endpoints."),
    ] = None,
    once: Annotated[bool, typer.Option(help="Run one worker iteration and exit.")] = False,
    concurrency: Annotated[
        int,
        typer.Option(help="Number of in-process worker slots for worker endpoints."),
    ] = 1,
) -> None:
    """Start a desktop or worker endpoint."""
    if concurrency < 1:
        raise typer.BadParameter("--concurrency must be at least 1")
    settings = RelaySettings.from_env()
    if role == EndpointRole.WORKER:
        if cluster is None:
            raise typer.BadParameter("--cluster is required for worker endpoints")
        _require_cluster(cluster)
    worker = EndpointWorker(
        role=role,
        settings=settings,
        cluster=cluster or "local",
        concurrency=concurrency,
    )
    worker.register()
    if once:
        worker.run_once()
        return
    worker.serve_forever()


@endpoint_app.command("status")
def endpoint_status() -> None:
    """Show local queue status."""
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    jobs = queue.list_jobs()
    typer.echo(f"jobs={len(jobs)} core={settings.core_dir} spool={settings.spool_dir}")
    for job in jobs:
        typer.echo(f"{job.job_id} {job.cluster} {job.kind.value} {job.state.value}")


@endpoint_app.command("render-user-service")
def endpoint_render_user_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the systemd user service."),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option(help="Number of in-process worker slots for the user service."),
    ] = 1,
) -> None:
    """Render a sudo-less systemd user service for a worker endpoint."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=concurrency,
    )
    if output is None:
        typer.echo(service_text)
        return
    typer.echo(write_endpoint_user_service(output, service_text))


@cluster_app.command("list")
def cluster_list() -> None:
    """List configured clusters."""
    registry = ClusterRegistry.load(default_registry_path())
    for name, definition in sorted(registry.clusters.items()):
        typer.echo(f"{name} ssh={definition.ssh_host} profile={definition.bootstrap_profile}")


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
) -> None:
    """Add or update a local cluster definition."""
    registry = ClusterRegistry.load(default_registry_path())
    try:
        registry.clusters[name] = ClusterDefinition(
            name=name,
            ssh_host=ssh_host,
            bootstrap_profile=bootstrap_profile,
            core_dir=core_dir,
            spool_dir=spool_dir,
            jarvis_bin=jarvis_bin,
            frpc_bin=frpc_bin,
            agent_bin=_none_if_blank(agent_bin),
            agent_adapter=agent_adapter,
            agent_npm_package=_none_if_blank(agent_npm_package),
            agent_npm_bin=_none_if_blank(agent_npm_bin),
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
    registry.save(default_registry_path())
    typer.echo(f"{name} ssh={ssh_host} profile={bootstrap_profile}")


@cluster_app.command("bootstrap")
def cluster_bootstrap(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    relay_wheel: Annotated[
        Path | None,
        typer.Option(
            "--relay-wheel",
            help="Local clio-relay wheel to include in the bootstrap archive.",
        ),
    ] = None,
) -> None:
    """Bootstrap a configured cluster's tools, relay package, and endpoint directories."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            bootstrap_cluster_over_ssh(
                bootstrap_profile=definition.bootstrap_profile,
                ssh_host=ssh_host or definition.ssh_host,
                source_root=package_source_root(),
                relay_wheel=relay_wheel,
                agent_adapter=definition.agent_adapter,
                agent_npm_package=definition.agent_npm_package,
                agent_npm_bin=definition.agent_npm_bin,
                agent_args=definition.agent_args,
            )
        )
    )


@cluster_app.command("install-app")
def cluster_install_app(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    app_name: Annotated[
        str,
        typer.Option("--app", help="Application runtime to install on the cluster."),
    ],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Install an explicit application runtime on a configured cluster."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            install_cluster_app_over_ssh(
                ssh_host=ssh_host or definition.ssh_host,
                app_name=app_name,
            )
        )
    )


@cluster_app.command("install-endpoint-service")
def cluster_install_endpoint_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    start: Annotated[bool, typer.Option(help="Restart the service after installing.")] = True,
    enable: Annotated[bool, typer.Option(help="Enable the user service.")] = True,
    concurrency: Annotated[
        int,
        typer.Option(help="Number of in-process worker slots for the user service."),
    ] = 1,
) -> None:
    """Install and optionally start a sudo-less worker endpoint service over SSH."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=concurrency,
    )
    _run_or_exit(
        lambda: _echo_lines(
            install_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                service_text=service_text,
                start=start,
                enable=enable,
            )
        )
    )


@session_app.command("start")
def session_start(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Replace an existing session API process."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Require CLIO_RELAY_API_TOKEN on the remote API."),
    ] = True,
) -> None:
    """Start an owned remote relay API session for detach/reattach workflows."""
    settings = RelaySettings.from_env()
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            start_remote_session(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                api_token=settings.api_token if require_token else None,
                replace=replace,
            )
        )
    )


@session_app.command("status")
def session_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
) -> None:
    """Inspect an owned remote relay API session."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(
                status_remote_session(definition=definition, session_id=session_id), indent=2
            )
        )
    )


@session_app.command("teardown")
def session_teardown(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    stop_worker: Annotated[
        bool,
        typer.Option(help="Also stop the persistent cluster worker service for this cluster."),
    ] = False,
    cancel_jobs: Annotated[
        bool | None,
        typer.Option(
            "--cancel-jobs/--keep-jobs",
            help="Cancel queued, leased, and running jobs for this cluster. Defaults to asking.",
        ),
    ] = None,
) -> None:
    """Stop owned remote relay session processes, optionally stopping the worker service."""
    definition = _require_cluster(cluster)
    if should_execute_on_cluster(definition):
        remote_args = [
            "session",
            "teardown",
            "--cluster",
            cluster,
            "--session-id",
            session_id,
            "--stop-worker" if stop_worker else "--no-stop-worker",
            "--cancel-jobs" if _resolve_cancel_jobs(cancel_jobs, cluster) else "--keep-jobs",
        ]
        _run_remote_or_exit(definition, remote_args)
        return

    def action() -> None:
        if _resolve_cancel_jobs(cancel_jobs, cluster):
            canceled = _cancel_active_cluster_jobs(
                ClioCoreQueue(RelaySettings.from_env().core_dir),
                cluster,
            )
            for job_id in canceled:
                typer.echo(f"job_canceled={job_id}")
        _echo_lines(
            teardown_remote_session(
                definition=definition,
                session_id=session_id,
                stop_worker=stop_worker,
                cluster=cluster,
            )
        )

    _run_or_exit(action)


@app.command("install-frp")
def install_frp(
    destination: Annotated[
        Path,
        typer.Option(help="Directory for frpc/frps binaries."),
    ] = Path(".tools/frp/bin"),
) -> None:
    """Download and install frp for the local desktop."""
    _run_or_exit(lambda: typer.echo(f"frpc={install_local_frp(destination)}"))


@job_app.command("submit")
def job_submit(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[Path, typer.Option(help="Path to JARVIS YAML.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    exclusive: Annotated[
        bool,
        typer.Option("--exclusive/--shared", help="Request exclusive scheduler allocation."),
    ] = False,
) -> None:
    """Submit a JARVIS pipeline job."""
    definition = _require_cluster(cluster)
    yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    if exclusive:
        yaml_text = _with_exclusive_scheduler(yaml_text)
    key = idempotency_key or _file_idempotency_key(jarvis_yaml, yaml_text)
    if should_execute_on_cluster(definition):
        remote_yaml = stage_jarvis_yaml(
            definition,
            jarvis_yaml=jarvis_yaml,
            pipeline_yaml_text=yaml_text,
            idempotency_key=key,
        )
        _run_remote_or_exit(
            definition,
            [
                "job",
                "submit",
                "--cluster",
                cluster,
                "--jarvis-yaml",
                remote_yaml,
                "--idempotency-key",
                key,
                "--exclusive" if exclusive else "--shared",
            ],
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_yaml=yaml_text),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@job_app.command("submit-pipeline")
def job_submit_pipeline(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    pipeline_name: Annotated[str, typer.Option(help="Existing JARVIS pipeline name.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
) -> None:
    """Submit an existing JARVIS pipeline by name on the target cluster."""
    definition = _require_cluster(cluster)
    key = idempotency_key or f"jarvis-pipeline:{cluster}:{pipeline_name}"
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(
            definition,
            [
                "job",
                "submit-pipeline",
                "--cluster",
                cluster,
                "--pipeline-name",
                pipeline_name,
                "--idempotency-key",
                key,
            ],
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name=pipeline_name),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@job_app.command("watch")
def job_watch(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job events from a cursor."""
    cursor = _job_event_cursor(cursor)
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "watch", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    for event in events:
        typer.echo(f"{event.seq} {event.created_at.isoformat()} {event.event_type} {event.message}")
    typer.echo(f"next_cursor={next_cursor.next_seq}")


@job_app.command("monitor")
def job_monitor(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job state and event stream data from a cursor as JSON."""
    cursor = _job_event_cursor(cursor)
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "monitor", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    result = monitor_job(
        ClioCoreQueue(RelaySettings.from_env().core_dir),
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@job_app.command("status")
def job_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read job, relay queue, and scheduler status as JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "status", job_id]):
        return
    result = get_job_status(ClioCoreQueue(RelaySettings.from_env().core_dir), job_id)
    typer.echo(json.dumps(result, indent=2))


@job_app.command("tasks")
def job_tasks(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """List durable task records for a job as JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "tasks", job_id]):
        return
    tasks = ClioCoreQueue(RelaySettings.from_env().core_dir).list_tasks(job_id)
    typer.echo(json.dumps([task.model_dump(mode="json") for task in tasks], indent=2))


@job_app.command("task-events")
def job_task_events(
    task_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="First task event sequence to read.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(help="Maximum task events to read.", min=1),
    ] = 100,
) -> None:
    """Read structured task timeline events from a cursor as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "task-events", task_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    events, next_cursor = ClioCoreQueue(RelaySettings.from_env().core_dir).drain_task_events(
        task_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            {
                "events": [event.model_dump(mode="json") for event in events],
                "next_cursor": next_cursor,
            },
            indent=2,
        )
    )


@job_app.command("record-task-event")
def job_record_task_event(
    task_id: str,
    event_type: Annotated[str, typer.Option(help="Structured task event type.")],
    label: Annotated[str, typer.Option(help="Short UI step label.")],
    summary: Annotated[str, typer.Option(help="Short event summary.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to record the event over SSH."),
    ] = None,
    status: Annotated[
        TaskEventStatus,
        typer.Option(help="Task step status."),
    ] = TaskEventStatus.RUNNING,
    detail: Annotated[str | None, typer.Option(help="Optional detail text.")] = None,
    path_ref: Annotated[
        list[str] | None,
        typer.Option(help="Path reference; repeat for multiple paths."),
    ] = None,
    artifact_ref: Annotated[
        list[str] | None,
        typer.Option(help="Artifact reference; repeat for multiple artifacts."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this task event."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Record a structured task timeline event."""
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    remote_args = [
        "job",
        "record-task-event",
        task_id,
        "--event-type",
        event_type,
        "--label",
        label,
        "--summary",
        summary,
        "--status",
        status.value,
        "--metadata-json",
        metadata_source,
    ]
    if detail is not None:
        remote_args.extend(["--detail", detail])
    for value in path_ref or []:
        remote_args.extend(["--path-ref", value])
    for value in artifact_ref or []:
        remote_args.extend(["--artifact-ref", value])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    event = ClioCoreQueue(RelaySettings.from_env().core_dir).append_task_event(
        TaskTimelineEvent(
            task_id=task_id,
            event_type=event_type,
            label=label,
            status=status,
            summary=summary,
            detail=detail,
            path_refs=path_ref or [],
            artifact_refs=artifact_ref or [],
            metadata=_json_object(metadata_source),
        )
    )
    typer.echo(event.model_dump_json(indent=2))


@job_app.command("wait")
def job_wait(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for terminal state."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Wait until a job reaches terminal state."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ],
    ):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    job = wait_for_terminal(
        queue,
        job_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    typer.echo(job.model_dump_json(indent=2))


@job_app.command("read-log")
def job_read_log(
    job_id: str,
    stream: Annotated[str, typer.Option(help="stdout or stderr.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    offset: Annotated[int, typer.Option(help="Byte offset.")] = 0,
    limit: Annotated[int, typer.Option(help="Maximum bytes.")] = 65536,
) -> None:
    """Read stdout or stderr from a job log by byte offset."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "read-log",
            job_id,
            "--stream",
            stream,
            "--offset",
            str(offset),
            "--limit",
            str(limit),
        ],
    ):
        return
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    if stream not in {"stdout", "stderr"}:
        raise typer.BadParameter("--stream must be stdout or stderr")
    result = read_job_log(
        settings,
        queue.get_job(job_id),
        stream_name="stdout" if stream == "stdout" else "stderr",
        offset=offset,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@job_app.command("read-artifact")
def job_read_artifact(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read an artifact payload as base64 JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "read-artifact", artifact_id]):
        return
    result = read_artifact_bytes(ClioCoreQueue(RelaySettings.from_env().core_dir), artifact_id)
    typer.echo(json.dumps(result, indent=2))


@job_app.command("list-artifacts")
def job_list_artifacts(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """List artifact references indexed for a job as JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "list-artifacts", job_id]):
        return
    artifacts = ClioCoreQueue(RelaySettings.from_env().core_dir).list_artifacts(job_id)
    typer.echo(json.dumps([artifact.model_dump(mode="json") for artifact in artifacts], indent=2))


@job_app.command("progress")
def job_progress(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """List structured progress observations for a job as JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "progress", job_id]):
        return
    progress = ClioCoreQueue(RelaySettings.from_env().core_dir).list_progress(job_id)
    typer.echo(json.dumps([item.model_dump(mode="json") for item in progress], indent=2))


@job_app.command("record-progress")
def job_record_progress(
    job_id: str,
    label: Annotated[str, typer.Option(help="Progress label.")] = "progress",
    current: Annotated[float | None, typer.Option(help="Current progress value.")] = None,
    total: Annotated[float | None, typer.Option(help="Total progress value.")] = None,
    unit: Annotated[str | None, typer.Option(help="Progress unit.")] = None,
    message: Annotated[str | None, typer.Option(help="Human-readable progress message.")] = None,
    source_event_seq: Annotated[
        int | None,
        typer.Option(help="Source event sequence for this progress observation."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this observation."),
    ] = "{}",
) -> None:
    """Record a structured progress observation for a job."""
    metadata = external_progress_metadata("external_cli", _json_object(metadata_json))
    progress = ClioCoreQueue(RelaySettings.from_env().core_dir).append_progress(
        ProgressRecord(
            job_id=job_id,
            label=label,
            current=current,
            total=total,
            unit=unit,
            message=message,
            source_event_seq=source_event_seq,
            metadata=metadata,
        )
    )
    typer.echo(progress.model_dump_json(indent=2))


@queue_app.command("list")
def queue_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    state: Annotated[
        JobState | None,
        typer.Option(help="Optional job state filter."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include succeeded, failed, and canceled jobs."),
    ] = False,
) -> None:
    """List relay queue jobs."""
    args = ["queue", "list"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if state is not None:
        args.extend(["--state", state.value])
    if include_terminal:
        args.append("--include-terminal")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(
        json.dumps(
            list_queue_jobs(
                queue,
                cluster=cluster,
                state=state,
                include_terminal=include_terminal,
            ),
            indent=2,
        )
    )


@queue_app.command("diagnose")
def queue_diagnose(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
) -> None:
    """Diagnose stuck relay queue state."""
    args = ["queue", "diagnose"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(json.dumps(diagnose_queue(queue, cluster=cluster), indent=2))


@queue_app.command("cancel")
def queue_cancel(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Request scheduler cancellation for already-submitted remote work.",
        ),
    ] = False,
) -> None:
    """Cancel a relay job with explicit scheduler policy."""
    args = ["queue", "cancel", job_id]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    args.append("--cancel-scheduler-job" if cancel_scheduler_job else "--keep-scheduler-job")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    result = cancel_queue_job(
        queue,
        job_id,
        scheduler_policy="request-scheduler" if cancel_scheduler_job else "relay-only",
    )
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("cleanup-stale")
def queue_cleanup_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose stale leases should be recovered.")],
    max_attempts: Annotated[
        int,
        typer.Option(help="Maximum attempts before expired leased jobs fail instead of requeue."),
    ] = 3,
    dry_run: Annotated[
        bool,
        typer.Option(help="Preview recoverable jobs without changing state."),
    ] = True,
) -> None:
    """Recover jobs with expired worker leases."""
    args = [
        "queue",
        "cleanup-stale",
        "--cluster",
        cluster,
        "--max-attempts",
        str(max_attempts),
    ]
    args.append("--dry-run" if dry_run else "--no-dry-run")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(
        json.dumps(
            cleanup_stale_jobs(
                queue,
                cluster=cluster,
                max_attempts=max_attempts,
                dry_run=dry_run,
            ),
            indent=2,
        )
    )


@worker_app.command("status")
def worker_status_command(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
) -> None:
    """Show registered worker capacity and leases."""
    args = ["worker", "status"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(json.dumps(worker_status(queue, cluster=cluster), indent=2))


@job_app.command("cancel")
def job_cancel(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Request scheduler cancellation for already-submitted remote work.",
        ),
    ] = False,
) -> None:
    """Cancel a queued or running job."""
    args = ["job", "cancel", job_id]
    if cancel_scheduler_job:
        args.append("--cancel-scheduler-job")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    job = request_cancel_job(
        ClioCoreQueue(RelaySettings.from_env().core_dir),
        job_id,
        cancel_scheduler=cancel_scheduler_job,
    )
    typer.echo(f"{job.job_id} {job.state.value}")


@gateway_app.command("create")
def gateway_create(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable session name.")],
    state: Annotated[
        GatewaySessionState,
        typer.Option(help="Initial gateway session state."),
    ] = GatewaySessionState.CREATED,
    scheduler: Annotated[str, typer.Option(help="Scheduler name.")] = "slurm",
    scheduler_job_id: Annotated[
        str | None,
        typer.Option(help="Scheduler job id if already known."),
    ] = None,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    gateway_json: Annotated[
        str,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = "{}",
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    resources_json: Annotated[
        str,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = "{}",
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this gateway session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Create a durable scheduler-backed gateway service session."""
    gateway_source = _json_text_from_option(gateway_json, gateway_json_file)
    resources_source = _json_text_from_option(resources_json, resources_json_file)
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    remote_args = [
        "gateway",
        "create",
        "--cluster",
        cluster,
        "--name",
        name,
        "--state",
        state.value,
        "--scheduler",
        scheduler,
        "--gateway-json",
        gateway_source,
        "--resources-json",
        resources_source,
        "--metadata-json",
        metadata_source,
    ]
    if scheduler_job_id is not None:
        remote_args.extend(["--scheduler-job-id", scheduler_job_id])
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).create_gateway_session(
        GatewaySession(
            cluster=cluster,
            name=name,
            state=state,
            scheduler=scheduler,
            scheduler_job_id=scheduler_job_id,
            queue_state=queue_state,
            node=node,
            stdout_uri=stdout_uri,
            stderr_uri=stderr_uri,
            log_uris=log_uri or [],
            gateway=_json_object(gateway_source),
            artifacts=artifact or [],
            requested_resources=_json_object(resources_source),
            metadata=_json_object(metadata_source),
        )
    )
    typer.echo(session.model_dump_json(indent=2))


@gateway_app.command("list")
def gateway_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
) -> None:
    """List durable gateway service sessions."""
    remote_args = ["gateway", "list"]
    if cluster is not None:
        remote_args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    sessions = ClioCoreQueue(RelaySettings.from_env().core_dir).list_gateway_sessions(
        cluster=cluster
    )
    typer.echo(json.dumps([session.model_dump(mode="json") for session in sessions], indent=2))


@gateway_app.command("get")
def gateway_get(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read a gateway service session."""
    remote_args = ["gateway", "get", session_id]
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).get_gateway_session(session_id)
    typer.echo(session.model_dump_json(indent=2))


@gateway_app.command("update")
def gateway_update(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
    state: Annotated[
        GatewaySessionState | None,
        typer.Option(help="Updated gateway session state."),
    ] = None,
    scheduler_job_id: Annotated[
        str | None,
        typer.Option(help="Scheduler job id."),
    ] = None,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    resources_json: Annotated[
        str | None,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = None,
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    gateway_json: Annotated[
        str | None,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = None,
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata to merge into this session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Update a gateway service session."""
    if gateway_json is not None and gateway_json_file is not None:
        raise typer.BadParameter("use either --gateway-json or --gateway-json-file, not both")
    if resources_json is not None and resources_json_file is not None:
        raise typer.BadParameter("use either --resources-json or --resources-json-file, not both")
    gateway_source = None
    if gateway_json is not None or gateway_json_file is not None:
        gateway_source = _json_text_from_option(gateway_json or "{}", gateway_json_file)
    resources_source = None
    if resources_json is not None or resources_json_file is not None:
        resources_source = _json_text_from_option(resources_json or "{}", resources_json_file)
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    remote_args = ["gateway", "update", session_id]
    if state is not None:
        remote_args.extend(["--state", state.value])
    if scheduler_job_id is not None:
        remote_args.extend(["--scheduler-job-id", scheduler_job_id])
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if resources_source is not None:
        remote_args.extend(["--resources-json", resources_source])
    if gateway_source is not None:
        remote_args.extend(["--gateway-json", gateway_source])
    remote_args.extend(["--metadata-json", metadata_source])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    updates: dict[str, object] = {}
    if scheduler_job_id is not None:
        updates["scheduler_job_id"] = scheduler_job_id
    if queue_state is not None:
        updates["queue_state"] = queue_state
    if node is not None:
        updates["node"] = node
    if stdout_uri is not None:
        updates["stdout_uri"] = stdout_uri
    if stderr_uri is not None:
        updates["stderr_uri"] = stderr_uri
    if log_uri is not None:
        updates["log_uris"] = log_uri
    if artifact is not None:
        updates["artifacts"] = artifact
    if resources_source is not None:
        updates["requested_resources"] = _json_object(resources_source)
    if gateway_source is not None:
        updates["gateway"] = _json_object(gateway_source)
    _run_or_exit(
        lambda: typer.echo(
            ClioCoreQueue(RelaySettings.from_env().core_dir)
            .update_gateway_session(
                session_id,
                state=state,
                metadata=_json_object(metadata_source),
                **updates,
            )
            .model_dump_json(indent=2)
        )
    )


@gateway_app.command("close")
def gateway_close(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
) -> None:
    """Mark a gateway service session closed."""
    if _try_remote_cluster_passthrough(cluster, ["gateway", "close", session_id]):
        return
    _run_or_exit(
        lambda: typer.echo(
            ClioCoreQueue(RelaySettings.from_env().core_dir)
            .close_gateway_session(session_id)
            .model_dump_json(indent=2)
        )
    )


@gateway_app.command("start-runtime")
def gateway_start_runtime(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable runtime session name.")],
    runtime_json_file: Annotated[
        Path,
        typer.Option(help="Path to a generic ServiceRuntimeSpec JSON document."),
    ],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
) -> None:
    """Start and bind a scheduler-backed streaming service runtime."""

    def action() -> None:
        definition = _require_cluster(cluster)
        if not runtime_json_file.exists():
            raise ConfigurationError(f"runtime spec does not exist: {runtime_json_file}")
        spec = ServiceRuntimeSpec.model_validate_json(
            runtime_json_file.read_text(encoding="utf-8-sig")
        )
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=ClioCoreQueue(settings.core_dir),
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        result = supervisor.start(name=name, spec=spec)
        typer.echo(result.session.model_dump_json(indent=2))

    _run_or_exit(action)


@gateway_app.command("stop-runtime")
def gateway_stop_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Cancel the scheduler job after closing relay connectors.",
        ),
    ] = False,
) -> None:
    """Stop owned runtime relay connectors and optionally cancel scheduler work."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=ClioCoreQueue(settings.core_dir),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.stop(
            session_id=session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        typer.echo(result.session.model_dump_json(indent=2))

    _run_or_exit(action)


@monitor_app.command("add-regex")
def monitor_add_regex(
    job_id: str,
    pattern: Annotated[str, typer.Option(help="Python regular expression to match.")],
    action: Annotated[
        MonitorRuleAction,
        typer.Option(help="Action to take when the rule matches."),
    ] = MonitorRuleAction.EMIT_EVENT,
    event_type: Annotated[
        list[str] | None,
        typer.Option(help="Event type to inspect; repeat for multiple types."),
    ] = None,
    action_payload_json: Annotated[
        str,
        typer.Option(help="JSON object used by actions such as submit_agent."),
    ] = "{}",
) -> None:
    """Create a generic regex monitor rule over a job event stream."""
    action_payload = _json_object(action_payload_json)
    rule = ClioCoreQueue(RelaySettings.from_env().core_dir).append_monitor_rule(
        MonitorRule(
            job_id=job_id,
            pattern=pattern,
            action=action,
            event_types=event_type or [],
            action_payload=action_payload,
        )
    )
    typer.echo(rule.model_dump_json(indent=2))


@monitor_app.command("list")
def monitor_list(
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional job id filter."),
    ] = None,
) -> None:
    """List durable monitor rules as JSON."""
    rules = ClioCoreQueue(RelaySettings.from_env().core_dir).list_monitor_rules(job_id)
    typer.echo(json.dumps([rule.model_dump(mode="json") for rule in rules], indent=2))


@monitor_app.command("run-once")
def monitor_run_once(
    limit: Annotated[int, typer.Option(help="Maximum events read per rule.")] = 100,
) -> None:
    """Evaluate enabled monitor rules once."""
    result = evaluate_monitor_rules(ClioCoreQueue(RelaySettings.from_env().core_dir), limit=limit)
    typer.echo(json.dumps(result, indent=2))


@agent_app.command("run")
def agent_run(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    prompt: Annotated[str, typer.Option(help="Prompt file path on the cluster.")],
    mcp_config: Annotated[
        str | None,
        typer.Option(help="Optional MCP config/profile path on the cluster."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
) -> None:
    """Submit a remote agent task on a configured cluster."""
    definition = _require_cluster(cluster)
    key = idempotency_key or f"agent:{cluster}:{prompt}:{mcp_config}"
    if should_execute_on_cluster(definition):
        args = [
            "agent",
            "run",
            "--cluster",
            cluster,
            "--prompt",
            prompt,
            "--idempotency-key",
            key,
        ]
        if mcp_config is not None:
            args.extend(["--mcp-config", mcp_config])
        _run_remote_or_exit(definition, args)
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=prompt, mcp_config_path=mcp_config),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@app.command("mcp-call")
def mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    server: Annotated[str, typer.Option(help="Remote MCP server name.")],
    tool: Annotated[str, typer.Option(help="Remote MCP tool name.")],
    server_arg: Annotated[
        list[str] | None,
        typer.Option(help="Additional remote MCP server argument. Repeatable."),
    ] = None,
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote MCP tool."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP call."),
    ] = None,
) -> None:
    """Submit a remote MCP tool call."""
    definition = _require_cluster(cluster)
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    digest = hashlib.sha256(
        json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    server_args = server_arg or []
    server_digest = hashlib.sha256(
        json.dumps([server, *server_args], separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    key = idempotency_key or f"mcp:{cluster}:{server_digest}:{tool}:{digest}"
    if should_execute_on_cluster(definition):
        remote_args = (
            f".local/share/clio-relay/desktop-submissions/mcp-{digest[:16]}/arguments.json"
        )
        write_remote_file(
            definition,
            remote_args,
            json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        _run_remote_or_exit(
            definition,
            [
                "mcp-call",
                "--cluster",
                cluster,
                "--server",
                server,
                "--tool",
                tool,
                "--arguments-json-file",
                remote_args,
                "--idempotency-key",
                key,
            ]
            + (["--timeout-seconds", str(timeout_seconds)] if timeout_seconds is not None else [])
            + [item for value in server_args for item in ("--server-arg", value)],
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@app.command("jarvis-mcp-call")
def jarvis_mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    tool: Annotated[str, typer.Option(help="JARVIS MCP tool name.")],
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the JARVIS MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the JARVIS MCP tool."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote JARVIS MCP call."),
    ] = None,
) -> None:
    """Submit a JARVIS MCP tool call that runs on the target cluster."""
    definition = _require_cluster(cluster)
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    digest = hashlib.sha256(
        json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    key = idempotency_key or f"mcp:{cluster}:jarvis:{tool}:{digest}"
    server = jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    if should_execute_on_cluster(definition):
        remote_args = (
            f".local/share/clio-relay/desktop-submissions/jarvis-mcp-{digest[:16]}/arguments.json"
        )
        write_remote_file(
            definition,
            remote_args,
            json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        _run_remote_or_exit(
            definition,
            [
                "mcp-call",
                "--cluster",
                cluster,
                "--server",
                server,
                "--tool",
                tool,
                "--arguments-json-file",
                remote_args,
                "--idempotency-key",
                key,
            ]
            + (["--timeout-seconds", str(timeout_seconds)] if timeout_seconds is not None else [])
            + [item for value in server_args for item in ("--server-arg", value)],
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@app.command("mcp-server")
def mcp_server() -> None:
    """Serve relay job tools over stdio MCP."""
    serve_stdio()


@api_app.command("start")
def api_start(
    host: Annotated[str, typer.Option(help="HTTP bind address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="HTTP bind port.")] = 8765,
    require_token: Annotated[
        bool,
        typer.Option(help="Fail if CLIO_RELAY_API_TOKEN is not configured."),
    ] = False,
) -> None:
    """Start the desktop-facing HTTP API."""
    if require_token and RelaySettings.from_env().api_token is None:
        raise typer.BadParameter("CLIO_RELAY_API_TOKEN is required with --require-token")
    uvicorn.run("clio_relay.http_api:app", host=host, port=port)


@agent_app.command("render-mcp-config")
def agent_render_mcp_config(
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the agent MCP profile TOML."),
    ] = None,
) -> None:
    """Render an agent profile that exposes the relay MCP tools."""
    rendered = render_agent_mcp_profile(settings=RelaySettings.from_env())
    if output is None:
        typer.echo(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    typer.echo(output)


@app.command("doctor")
def doctor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Check local or live cluster configuration."""
    definition = _require_cluster(cluster)

    def _run() -> None:
        _echo_lines(
            run_doctor(
                RelaySettings.from_env(),
                live=True,
                frps_addr=definition.frp_transport.server_addr,
            )
        )
        _echo_lines(run_cluster_doctor(definition))

    _run_or_exit(_run)


@app.command("live-test")
def live_test(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[
        Path | None,
        typer.Option(help="Configured acceptance JARVIS YAML. Overrides cluster config."),
    ] = None,
    monitor_pattern: Annotated[
        str | None,
        typer.Option(help="Regex expected to match stdout.delta during acceptance."),
    ] = None,
    progress_pattern: Annotated[
        str | None,
        typer.Option(help="Regex used to record structured progress from stdout.delta."),
    ] = None,
    progress_action_payload_json: Annotated[
        str,
        typer.Option(
            help="JSON object payload for progress monitor extraction, such as groups and units.",
        ),
    ] = "{}",
    agent_prompt: Annotated[
        str | None,
        typer.Option(help="Remote prompt path for optional agent acceptance."),
    ] = None,
    agent_mcp_config: Annotated[
        str | None,
        typer.Option(help="Remote MCP config path for optional agent acceptance."),
    ] = None,
    agent_child_jarvis_yaml: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Local JARVIS YAML the agent must submit through MCP. "
                "Generates a remote agent prompt with a fresh idempotency key."
            ),
        ),
    ] = None,
    require_agent_child_job: Annotated[
        bool | None,
        typer.Option(
            "--require-agent-child-job/--no-require-agent-child-job",
            help=(
                "Require optional agent acceptance to report and complete a child relay job. "
                "Defaults to enabled when --agent-mcp-config is set."
            ),
        ),
    ] = None,
    verify_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-transport/--no-verify-transport",
            help="Verify desktop-to-cluster HTTP reachability through configured frp transport.",
        ),
    ] = None,
    verify_direct_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-direct-transport/--no-verify-direct-transport",
            help="Verify desktop-to-cluster HTTP reachability through frp XTCP.",
        ),
    ] = None,
    allow_direct_transport_fallback: Annotated[
        bool | None,
        typer.Option(
            "--allow-direct-transport-fallback/--no-allow-direct-transport-fallback",
            help="Allow live direct transport acceptance to fall back to STCP.",
        ),
    ] = None,
    transport_token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    transport_secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local desktop visitor bind port for transport acceptance."),
    ] = None,
    transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote cluster API port for transport acceptance."),
    ] = None,
    transport_proxy_name: Annotated[
        str | None,
        typer.Option(help="frp proxy/server name for transport acceptance."),
    ] = None,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for acceptance jobs."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Run configurable live acceptance checks for a cluster."""
    definition = _require_cluster(cluster)
    should_verify_transport = (
        definition.live_test.verify_transport if verify_transport is None else verify_transport
    )
    should_verify_direct_transport = (
        definition.live_test.verify_direct_transport
        if verify_direct_transport is None
        else verify_direct_transport
    )
    should_allow_direct_transport_fallback = (
        definition.live_test.allow_direct_transport_fallback
        if allow_direct_transport_fallback is None
        else allow_direct_transport_fallback
    )
    needs_transport_secrets = should_verify_transport or should_verify_direct_transport

    def _run() -> None:
        settings = RelaySettings.from_env()
        _echo_lines(
            run_live_acceptance(
                LiveAcceptanceOptions(
                    cluster=cluster,
                    definition=definition,
                    jarvis_yaml=jarvis_yaml,
                    monitor_pattern=monitor_pattern,
                    progress_pattern=progress_pattern,
                    progress_action_payload=_json_object(progress_action_payload_json),
                    agent_prompt=agent_prompt,
                    agent_mcp_config=agent_mcp_config,
                    require_agent_child_job=require_agent_child_job,
                    agent_child_jarvis_yaml=agent_child_jarvis_yaml,
                    verify_transport=verify_transport,
                    verify_direct_transport=should_verify_direct_transport,
                    allow_direct_transport_fallback=should_allow_direct_transport_fallback,
                    transport_token=(
                        _resolve_env_secret(
                            transport_token,
                            definition.frp_transport.token_env,
                            "frp token",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_secret_key=(
                        _resolve_env_secret(
                            transport_secret_key,
                            definition.frp_transport.stcp_secret_env,
                            "stcp secret",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_frpc_bin=settings.frpc_bin,
                    transport_local_bind_port=transport_local_bind_port,
                    transport_remote_api_port=transport_remote_api_port,
                    transport_proxy_name=transport_proxy_name,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            )
        )

    _run_or_exit(_run)


def _file_idempotency_key(path: Path, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"jarvis:{path.resolve()}:{digest}"


def _none_if_blank(value: str | None) -> str | None:
    if value is None or value.strip() == "":
        return None
    return value


def _split_csv(value: str) -> list[str]:
    return [entry.strip() for entry in value.split(",") if entry.strip()]


def _json_object(value: str) -> dict[str, object]:
    source = Path(value[1:]).read_text(encoding="utf-8-sig") if value.startswith("@") else value
    try:
        loaded = cast(object, json.loads(source))
    except JSONDecodeError as exc:
        raise typer.BadParameter(f"value must be valid JSON: {exc.msg}") from exc
    if not isinstance(loaded, dict):
        raise typer.BadParameter("value must be a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], loaded).items()}


def _json_text_from_option(source: str, source_file: Path | None) -> str:
    if source_file is None:
        return source
    if source != "{}":
        raise typer.BadParameter("use either the JSON value option or the JSON file option")
    if not source_file.exists():
        raise typer.BadParameter(f"JSON file does not exist: {source_file}")
    return source_file.read_text(encoding="utf-8-sig")


def _with_exclusive_scheduler(pipeline_yaml: str) -> str:
    loaded = yaml.safe_load(pipeline_yaml)
    if not isinstance(loaded, dict):
        raise ConfigurationError("JARVIS YAML must be an object to request exclusive allocation")
    document = cast(dict[str, object], loaded)
    scheduler = document.get("scheduler")
    if scheduler is None:
        scheduler = {"name": "slurm"}
    if not isinstance(scheduler, dict):
        raise ConfigurationError("scheduler must be an object to request exclusive allocation")
    typed_scheduler = cast(dict[str, object], scheduler)
    typed_scheduler.setdefault("name", "slurm")
    typed_scheduler["exclusive"] = True
    document["scheduler"] = typed_scheduler
    return yaml.safe_dump(document, sort_keys=False)


def _resolve_cancel_jobs(cancel_jobs: bool | None, cluster: str) -> bool:
    if cancel_jobs is not None:
        return cancel_jobs
    return typer.confirm(
        f"Cancel queued or running jobs for cluster {cluster}?",
        default=False,
    )


def _cancel_active_cluster_jobs(queue: ClioCoreQueue, cluster: str) -> list[str]:
    canceled: list[str] = []
    for job in queue.list_jobs():
        if job.cluster != cluster or job.state not in {
            JobState.QUEUED,
            JobState.LEASED,
            JobState.RUNNING,
        }:
            continue
        canceled.append(request_cancel_job(queue, job.job_id).job_id)
    return canceled


def _echo_lines(lines: list[str]) -> None:
    for line in lines:
        typer.echo(_console_safe_text(line))


def _job_event_cursor(cursor: int) -> int:
    """Normalize CLI event cursors while preserving the durable cursor contract."""
    if cursor < 0:
        raise typer.BadParameter("cursor must be greater than or equal to 0")
    return 1 if cursor == 0 else cursor


def _console_safe_text(value: str) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def _try_remote_cluster_passthrough(cluster: str | None, args: list[str]) -> bool:
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not should_execute_on_cluster(definition):
        return False
    _run_remote_or_exit(definition, args)
    return True


def _run_remote_or_exit(definition: ClusterDefinition, args: list[str]) -> None:
    _run_or_exit(
        lambda: typer.echo(_console_safe_text(run_remote_clio(definition, args)), nl=False)
    )


def _require_cluster(cluster: str) -> ClusterDefinition:
    return ClusterRegistry.load(default_registry_path()).require(cluster)


def _require_frp_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay cluster add --frp-server-addr ...`"
    )


def _resolve_env_secret(value: str | None, env_name: str, label: str) -> str:
    resolved = value or os.getenv(env_name) or _local_secret(env_name)
    if resolved:
        return resolved
    raise ConfigurationError(
        f"{label} is required; pass it explicitly, set {env_name}, "
        f"or add {env_name} to .clio-relay/secrets.json"
    )


def _local_secret(env_name: str) -> str | None:
    path = Path(".clio-relay/secrets.json")
    if not path.exists():
        return None
    loaded = cast(object, json.loads(path.read_text(encoding="utf-8-sig")))
    if not isinstance(loaded, dict):
        raise ConfigurationError(".clio-relay/secrets.json must contain a JSON object")
    secrets = cast(dict[object, object], loaded)
    value = secrets.get(env_name)
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        raise ConfigurationError(
            f".clio-relay/secrets.json field must be a non-empty string: {env_name}"
        )
    return value


def _run_or_exit(action: Callable[[], None]) -> None:
    try:
        action()
    except (ConfigurationError, RelayError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
