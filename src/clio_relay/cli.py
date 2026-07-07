"""Command-line interface for clio-relay."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from json import JSONDecodeError
from pathlib import Path
from typing import Annotated, cast

import typer
import uvicorn

from clio_relay.bootstrap import bootstrap_cluster_over_ssh, install_local_frp
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
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
from clio_relay.live_acceptance import LiveAcceptanceOptions, run_live_acceptance
from clio_relay.mcp_server import render_codex_mcp_profile, serve_stdio
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    McpCallSpec,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
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
from clio_relay.transport_probe import run_frp_http_probe

app = typer.Typer(no_args_is_help=True)
endpoint_app = typer.Typer(no_args_is_help=True)
relay_host_app = typer.Typer(no_args_is_help=True)
job_app = typer.Typer(no_args_is_help=True)
cluster_app = typer.Typer(no_args_is_help=True)
agent_app = typer.Typer(no_args_is_help=True)
monitor_app = typer.Typer(no_args_is_help=True)
api_app = typer.Typer(no_args_is_help=True)

app.add_typer(endpoint_app, name="endpoint")
app.add_typer(relay_host_app, name="relay-host")
app.add_typer(job_app, name="job")
app.add_typer(cluster_app, name="cluster")
app.add_typer(agent_app, name="agent")
app.add_typer(monitor_app, name="monitor")
app.add_typer(api_app, name="api")


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
    definition = _require_cluster(cluster)
    transport = definition.frp_transport
    _run_or_exit(
        lambda: typer.echo(
            render_frpc_config(
                FrpcConfig(
                    server_addr=transport.server_addr,
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
    )


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
    settings = RelaySettings.from_env()
    definition = _require_cluster(cluster)
    transport = definition.frp_transport
    config = FrpcConfig(
        server_addr=transport.server_addr,
        server_port=transport.server_port,
        token=_resolve_env_secret(token, transport.token_env, "frp token"),
        transport_protocol=FrpTransportProtocol(transport.protocol),
        proxy_name=proxy_name,
        local_port=local_port,
        secret_key=_resolve_env_secret(secret_key, transport.stcp_secret_env, "stcp secret"),
    )
    _run_or_exit(
        lambda: _echo_lines(
            run_frpc_connection_check(
                frpc_bin=settings.frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
            )
        )
    )


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
    definition = _require_cluster(cluster)
    transport = definition.frp_transport
    _run_or_exit(
        lambda: typer.echo(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=transport.server_addr,
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
    )


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


@endpoint_app.command("start")
def endpoint_start(
    role: Annotated[EndpointRole, typer.Option(help="Endpoint role.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster name for worker endpoints."),
    ] = None,
    once: Annotated[bool, typer.Option(help="Run one worker iteration and exit.")] = False,
) -> None:
    """Start a desktop or worker endpoint."""
    settings = RelaySettings.from_env()
    if role == EndpointRole.WORKER:
        if cluster is None:
            raise typer.BadParameter("--cluster is required for worker endpoints")
        _require_cluster(cluster)
    worker = EndpointWorker(role=role, settings=settings, cluster=cluster or "local")
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
) -> None:
    """Render a sudo-less systemd user service for a worker endpoint."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(cluster=cluster, definition=definition)
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


@cluster_app.command("bootstrap")
def cluster_bootstrap(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Bootstrap a configured cluster's tools, relay package, and endpoint directories."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            bootstrap_cluster_over_ssh(
                bootstrap_profile=definition.bootstrap_profile,
                ssh_host=ssh_host or definition.ssh_host,
                source_root=Path.cwd(),
                agent_adapter=definition.agent_adapter,
                agent_npm_package=definition.agent_npm_package,
                agent_npm_bin=definition.agent_npm_bin,
                agent_args=definition.agent_args,
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
) -> None:
    """Install and optionally start a sudo-less worker endpoint service over SSH."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(cluster=cluster, definition=definition)
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
) -> None:
    """Submit a JARVIS pipeline job."""
    _require_cluster(cluster)
    yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    key = idempotency_key or _file_idempotency_key(jarvis_yaml, yaml_text)
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_yaml=yaml_text),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@job_app.command("watch")
def job_watch(
    job_id: str,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job events from a cursor."""
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    for event in events:
        typer.echo(f"{event.seq} {event.created_at.isoformat()} {event.event_type} {event.message}")
    typer.echo(f"next_cursor={next_cursor.next_seq}")


@job_app.command("monitor")
def job_monitor(
    job_id: str,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job state and event stream data from a cursor as JSON."""
    result = monitor_job(
        ClioCoreQueue(RelaySettings.from_env().core_dir),
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@job_app.command("tasks")
def job_tasks(
    job_id: str,
) -> None:
    """List durable task records for a job as JSON."""
    tasks = ClioCoreQueue(RelaySettings.from_env().core_dir).list_tasks(job_id)
    typer.echo(json.dumps([task.model_dump(mode="json") for task in tasks], indent=2))


@job_app.command("wait")
def job_wait(
    job_id: str,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for terminal state."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Wait until a job reaches terminal state."""
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
    offset: Annotated[int, typer.Option(help="Byte offset.")] = 0,
    limit: Annotated[int, typer.Option(help="Maximum bytes.")] = 65536,
) -> None:
    """Read stdout or stderr from a job log by byte offset."""
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
) -> None:
    """Read an artifact payload as base64 JSON."""
    result = read_artifact_bytes(ClioCoreQueue(RelaySettings.from_env().core_dir), artifact_id)
    typer.echo(json.dumps(result, indent=2))


@job_app.command("list-artifacts")
def job_list_artifacts(
    job_id: str,
) -> None:
    """List artifact references indexed for a job as JSON."""
    artifacts = ClioCoreQueue(RelaySettings.from_env().core_dir).list_artifacts(job_id)
    typer.echo(json.dumps([artifact.model_dump(mode="json") for artifact in artifacts], indent=2))


@job_app.command("progress")
def job_progress(
    job_id: str,
) -> None:
    """List structured progress observations for a job as JSON."""
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
    metadata = _json_object(metadata_json)
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


@job_app.command("cancel")
def job_cancel(job_id: str) -> None:
    """Cancel a queued or running job."""
    job = request_cancel_job(ClioCoreQueue(RelaySettings.from_env().core_dir), job_id)
    typer.echo(f"{job.job_id} {job.state.value}")


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
    prompt: Annotated[Path, typer.Option(help="Prompt file path on the cluster.")],
    mcp_config: Annotated[
        Path | None,
        typer.Option(help="Optional MCP config/profile path on the cluster."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
) -> None:
    """Submit a remote agent task on a configured cluster."""
    _require_cluster(cluster)
    key = idempotency_key or f"agent:{cluster}:{prompt}:{mcp_config}"
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
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote MCP tool."),
    ] = "{}",
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
) -> None:
    """Submit a remote MCP tool call."""
    _require_cluster(cluster)
    arguments = _json_object(arguments_json)
    digest = hashlib.sha256(
        json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    key = idempotency_key or f"mcp:{cluster}:{server}:{tool}:{digest}"
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(server=server, tool=tool, arguments=arguments),
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
        typer.Option(help="Optional path to write the Codex MCP profile TOML."),
    ] = None,
) -> None:
    """Render a Codex profile that exposes the relay MCP tools."""
    rendered = render_codex_mcp_profile(settings=RelaySettings.from_env())
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
                    verify_transport=verify_transport,
                    transport_token=(
                        _resolve_env_secret(
                            transport_token,
                            definition.frp_transport.token_env,
                            "frp token",
                        )
                        if should_verify_transport
                        else None
                    ),
                    transport_secret_key=(
                        _resolve_env_secret(
                            transport_secret_key,
                            definition.frp_transport.stcp_secret_env,
                            "stcp secret",
                        )
                        if should_verify_transport
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


def _json_object(value: str) -> dict[str, object]:
    source = Path(value[1:]).read_text(encoding="utf-8-sig") if value.startswith("@") else value
    try:
        loaded = cast(object, json.loads(source))
    except JSONDecodeError as exc:
        raise typer.BadParameter(f"value must be valid JSON: {exc.msg}") from exc
    if not isinstance(loaded, dict):
        raise typer.BadParameter("value must be a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], loaded).items()}


def _echo_lines(lines: list[str]) -> None:
    for line in lines:
        typer.echo(line)


def _require_cluster(cluster: str) -> ClusterDefinition:
    return ClusterRegistry.load(default_registry_path()).require(cluster)


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
