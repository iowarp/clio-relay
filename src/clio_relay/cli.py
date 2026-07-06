"""Command-line interface for clio-relay."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer

from clio_relay.bootstrap import bootstrap_cluster_over_ssh, install_local_frp
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.doctor import run_doctor
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    RemoteAgentTaskSpec,
)
from clio_relay.relay_host import FrpsConfig, render_frps_config

app = typer.Typer(no_args_is_help=True)
endpoint_app = typer.Typer(no_args_is_help=True)
relay_host_app = typer.Typer(no_args_is_help=True)
job_app = typer.Typer(no_args_is_help=True)
cluster_app = typer.Typer(no_args_is_help=True)
agent_app = typer.Typer(no_args_is_help=True)

app.add_typer(endpoint_app, name="endpoint")
app.add_typer(relay_host_app, name="relay-host")
app.add_typer(job_app, name="job")
app.add_typer(cluster_app, name="cluster")
app.add_typer(agent_app, name="agent")


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
    token: Annotated[str, typer.Option(help="frp authentication token.")],
    bind_port: Annotated[int, typer.Option(help="frps bind port.")] = 7000,
    dashboard_port: Annotated[
        int | None,
        typer.Option(help="Optional frps dashboard port."),
    ] = None,
) -> None:
    """Render an frps config with no relay application state."""
    config = FrpsConfig(bind_port=bind_port, token=token, dashboard_port=dashboard_port)
    typer.echo(render_frps_config(config))


@endpoint_app.command("start")
def endpoint_start(
    role: Annotated[EndpointRole, typer.Option(help="Endpoint role.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")] = "ares",
    once: Annotated[bool, typer.Option(help="Run one worker iteration and exit.")] = False,
) -> None:
    """Start a desktop or worker endpoint."""
    settings = RelaySettings.from_env()
    if role == EndpointRole.WORKER:
        _require_cluster(cluster)
    worker = EndpointWorker(role=role, settings=settings, cluster=cluster)
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


@cluster_app.command("list")
def cluster_list() -> None:
    """List configured clusters."""
    registry = ClusterRegistry.load(default_registry_path())
    for name, definition in sorted(registry.clusters.items()):
        typer.echo(f"{name} ssh={definition.ssh_host} profile={definition.bootstrap_profile}")


@cluster_app.command("bootstrap")
def cluster_bootstrap(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")] = "ares",
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
                agent_npm_package=definition.agent_npm_package,
                agent_npm_bin=definition.agent_npm_bin,
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


@job_app.command("cancel")
def job_cancel(job_id: str) -> None:
    """Cancel a queued or running job."""
    job = ClioCoreQueue(RelaySettings.from_env().core_dir).update_job_state(
        job_id,
        JobState.CANCELED,
    )
    typer.echo(f"{job.job_id} {job.state.value}")


@agent_app.command("run")
def agent_run(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    prompt: Annotated[Path, typer.Option(help="Prompt file path on the cluster.")],
    mcp_config: Annotated[Path, typer.Option(help="MCP config path on the cluster.")],
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
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
) -> None:
    """Submit a remote MCP tool call."""
    _require_cluster(cluster)
    key = idempotency_key or f"mcp:{cluster}:{server}:{tool}"
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(server=server, tool=tool),
        idempotency_key=key,
    )
    saved = ClioCoreQueue(RelaySettings.from_env().core_dir).submit_job(job)
    typer.echo(saved.job_id)


@app.command("doctor")
def doctor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")] = "ares",
) -> None:
    """Check local or live cluster configuration."""
    _require_cluster(cluster)
    _run_or_exit(lambda: _echo_lines(run_doctor(RelaySettings.from_env(), live=True)))


@app.command("live-test")
def live_test(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")] = "ares",
) -> None:
    """Run live acceptance preflight checks for a configured cluster."""
    _require_cluster(cluster)
    _run_or_exit(lambda: _echo_lines(run_doctor(RelaySettings.from_env(), live=True)))
    typer.echo("live preflight passed; submit a JARVIS smoke job to complete acceptance")


def _file_idempotency_key(path: Path, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"jarvis:{path.resolve()}:{digest}"


def _echo_lines(lines: list[str]) -> None:
    for line in lines:
        typer.echo(line)


def _require_cluster(cluster: str) -> ClusterDefinition:
    return ClusterRegistry.load(default_registry_path()).require(cluster)


def _run_or_exit(action: Callable[[], None]) -> None:
    try:
        action()
    except (ConfigurationError, RelayError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
