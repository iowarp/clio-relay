"""Command-line interface for clio-relay."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shlex
import socket
import stat
import subprocess
import sys
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path, PurePosixPath
from time import monotonic, sleep
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

import typer
import uvicorn
import yaml
from filelock import FileLock
from pydantic import ValidationError

from clio_relay.application_profiles import install_cluster_app_over_ssh
from clio_relay.bootstrap import (
    bootstrap_cluster_over_ssh,
    install_local_frp,
    package_source_root,
)
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    ClusterTargetIdentity,
    DirectTransportConfig,
    FrpTransportConfig,
    RemoteMcpContract,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
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
from clio_relay.errors import ConfigurationError, NotFoundError, RelayError
from clio_relay.frp_check import run_frpc_connection_check
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.installation import (
    attach_verified_worker_identity,
    installation_info,
    worker_runtime_info,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    jarvis_mcp_artifact_binding_from_entry,
    jarvis_mcp_env_from,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
)
from clio_relay.jarvis_mcp_validation import build_jarvis_mcp_validation_report
from clio_relay.live_acceptance import LiveAcceptanceOptions, run_live_acceptance
from clio_relay.mcp_server import (
    load_registered_remote_mcp_catalog,
    render_agent_mcp_profile,
    serve_stdio,
    static_mcp_tool_names,
)
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession, run_packaged_mcp_stdio_session
from clio_relay.models import (
    ArtifactUse,
    Cursor,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    McpOperation,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
    SchedulerPhase,
    ServiceRuntimeSpec,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.public_records import public_gateway_session
from clio_relay.queue_management import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
from clio_relay.queue_validation import run_queue_management_validation
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
from clio_relay.release_validation import (
    LocalReleaseValidationOptions,
    run_local_release_validation,
)
from clio_relay.remote_cli import (
    remote_command_timeout,
    remove_remote_file,
    run_remote_clio,
    run_remote_shell,
    should_execute_on_cluster,
    stage_jarvis_yaml,
    write_remote_file,
)
from clio_relay.remote_mcp import (
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    RemoteMcpAcceptanceReport,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpStructuredResultExpectation,
    VirtualRemoteMcpCatalog,
    build_remote_mcp_acceptance_report,
    build_remote_mcp_spack_fresh_install_transition_report,
    cache_entry_from_discovery_artifact,
    default_remote_mcp_cache_path,
    remote_mcp_execution_fingerprint,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.scheduler_providers import (
    allocation_connector_provider_for_scheduler,
    provider_for_scheduler,
    validation_provider_for_scheduler,
)
from clio_relay.scheduler_validation import run_scheduler_lifecycle_validation
from clio_relay.service_runtime import ServiceRuntimeStartResult, ServiceRuntimeSupervisor
from clio_relay.session_api import submit_owned_session_job
from clio_relay.session_lifecycle import (
    CleanupResource,
    SessionLifecycleReport,
    cleanup_connectors_cover_gateways,
    detach_remote_session,
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageManagedQueue,
    storage_managed_queue,
)
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    evaluate_release_gate,
    load_release_gate_policy,
    load_validation_report,
    new_live_validation_report,
    redact_sensitive_values,
    sha256_file,
    write_release_gate_result,
    write_validation_report,
)
from clio_relay.worker_concurrency import parse_kind_concurrency_options

MAX_INTERNAL_COLLECTION_RECORDS = 10_000
MAX_OWNER_GATEWAY_CLEANUP_PASSES = 4
DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS = 30.0
DEFAULT_RELAY_CANCEL_POLL_SECONDS = 0.25
MAX_RELAY_CANCEL_TIMEOUT_SECONDS = 3_600.0
REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS = 120.0
REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS = 20.0
SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS = 60.0
MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES = 128 * 1024
MAX_SPACK_CONFIGURATION_TREE_ENTRIES = 1_024
SCHEDULER_SENTINEL_ACTIVE_PHASES = frozenset({"submitted", "pending", "allocated", "running"})
SCHEDULER_SENTINEL_PRESERVED_PHASES = SCHEDULER_SENTINEL_ACTIVE_PHASES | {"completed"}
_ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE = "__clio_relay_acceptance_report_command__"


def _acceptance_report_command[CommandCallback: Callable[..., Any]](
    callback: CommandCallback,
) -> CommandCallback:
    """Mark a CLI callback as a canonical acceptance-report producer."""
    setattr(callback, _ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE, True)
    return callback


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
scheduler_app = typer.Typer(no_args_is_help=True)
remote_mcp_app = typer.Typer(no_args_is_help=True)
release_app = typer.Typer(no_args_is_help=True)
storage_app = typer.Typer(no_args_is_help=True)

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
app.add_typer(scheduler_app, name="scheduler")
app.add_typer(remote_mcp_app, name="remote-mcp")
app.add_typer(release_app, name="release")
app.add_typer(storage_app, name="storage")


@app.callback()
def main() -> None:
    """Run clio-relay commands."""


@storage_app.command("status")
def storage_status() -> None:
    """Return machine-readable storage admission readiness."""
    queue = storage_managed_queue(RelaySettings.from_env())
    typer.echo(json.dumps(queue.storage_runtime.status(), indent=2))


@app.command()
def init(
    migrate_legacy_output: Annotated[
        bool,
        typer.Option(
            help=(
                "Authorize migration of exact oversized v0.9 output events after every "
                "queue writer has been stopped and verified inactive."
            )
        ),
    ] = False,
) -> None:
    """Initialize local queue, spool, and cluster registry files."""
    settings = RelaySettings.from_env()
    storage_managed_queue(settings, migrate_legacy_output=migrate_legacy_output)
    registry = ClusterRegistry.load(default_registry_path())
    typer.echo(
        f"initialized core={settings.core_dir} spool={settings.spool_dir} "
        f"clusters={','.join(sorted(registry.clusters))}"
    )


@release_app.command("validate-local")
@_acceptance_report_command
def release_validate_local(
    project_root: Annotated[
        Path,
        typer.Option(help="Clean source checkout to validate."),
    ] = Path("."),
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    artifact_dir: Annotated[
        Path | None,
        typer.Option(help="Optional empty output directory for wheel and sdist artifacts."),
    ] = None,
    prebuilt_artifact_dir: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Reuse an exact build-once wheel, sdist, and SHA256SUMS directory; "
                "never build artifacts in this validation run."
            )
        ),
    ] = None,
) -> None:
    """Run the complete local release gate and persist evidence on failure."""
    report_path = report or default_report_path("local")
    seed_report = new_live_validation_report(
        scenario="local-release",
        cluster="local",
    )
    write_validation_report(seed_report, report_path)

    def _run() -> None:
        try:
            result = run_local_release_validation(
                LocalReleaseValidationOptions(
                    project_root=project_root,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    artifact_dir=artifact_dir,
                    prebuilt_artifact_dir=prebuilt_artifact_dir,
                    report_id=seed_report.report_id,
                )
            )
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None or result.report_id != seed_report.report_id:
                raise RelayError(
                    "local release validation did not persist the current invocation report"
                )
        except BaseException as exc:
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            _write_failed_acceptance_report(
                path=report_path,
                scenario="local-release",
                cluster="local",
                check_id="local-release.completed",
                summary="complete local release gate",
                error=exc,
                launcher=None,
                install_source=None,
                artifact=None,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        typer.echo(f"validation.status={result.status.value}")
        typer.echo(f"validation.report={report_path.resolve()}")

    _run_or_exit(_run)


@release_app.command("gate")
def release_gate(
    policy: Annotated[Path, typer.Option(help="Machine-readable 1.0 release policy.")],
    report: Annotated[
        list[Path] | None,
        typer.Option(help="Validation JSON report. Repeat for multiple reports."),
    ] = None,
    report_dir: Annotated[
        Path | None,
        typer.Option(help="Directory containing validation JSON reports."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(help="Optional JSON path for the gate decision."),
    ] = None,
    expected_artifact_sha256: Annotated[
        str | None,
        typer.Option(
            help=(
                "SHA-256 independently computed from the immutable candidate wheel. "
                "Every non-local report used by the gate must match it."
            )
        ),
    ] = None,
) -> None:
    """Reject a release unless every policy requirement has released-artifact evidence."""

    def _run() -> None:
        report_paths = list(report or [])
        if report_dir is not None:
            report_paths.extend(sorted(report_dir.glob("*.json")))
        unique_paths = list(dict.fromkeys(path.resolve() for path in report_paths))
        if not unique_paths:
            raise ConfigurationError("release gate requires --report or --report-dir")
        gate_policy = load_release_gate_policy(policy)
        reports = [load_validation_report(path) for path in unique_paths]
        result = evaluate_release_gate(
            gate_policy,
            reports,
            expected_artifact_sha256=expected_artifact_sha256,
        )
        if output is not None:
            write_release_gate_result(result, output)
        typer.echo(result.model_dump_json(indent=2))
        if not result.passed:
            raise typer.Exit(code=1)

    _run_or_exit(_run)


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
@_acceptance_report_command
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
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical frpc connection validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run a live frpc login check and persist canonical success or failure evidence."""

    canonical_report_path = validation_report or default_report_path(cluster)

    try:
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
            secret_key=_resolve_env_secret(
                secret_key,
                transport.stcp_secret_env,
                "stcp secret",
            ),
        )
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.frpc-connection.preflight",
            summary="validate frpc connection acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        _echo_lines(
            _run_frpc_connection_validation(
                cluster=cluster,
                proxy_name=proxy_name,
                frpc_bin=settings.frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
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
@_acceptance_report_command
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
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp STCP."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate HTTP transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="frp-relay",
                resource_id=proxy_name,
                resource_role="frp_stcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_frp_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=_resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
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
                ),
            )
        )
    )


@relay_host_app.command("test-direct-transport")
@_acceptance_report_command
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
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp XTCP direct transport."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate direct transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="frp-direct",
                resource_id=proxy_name,
                resource_role="frp_xtcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_frp_direct_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=_resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
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
                ),
            )
        )
    )


@relay_host_app.command("test-ssh-transport")
@_acceptance_report_command
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
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through SSH local port forwarding."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate SSH transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="ssh-forward",
                resource_id=session_id,
                resource_role="ssh_forward_probe",
                retain_remote_session=detach_remote,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_ssh_forward_http_probe(
                    cluster=cluster,
                    definition=definition,
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    session_id=session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    detach_remote=detach_remote,
                ),
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
    if concurrency < 1:
        raise typer.BadParameter("--concurrency must be at least 1")
    kind_limits = _kind_concurrency_options(kind_concurrency)
    settings = RelaySettings.from_env()
    definition: ClusterDefinition | None = None
    if role == EndpointRole.WORKER:
        if cluster is None:
            raise typer.BadParameter("--cluster is required for worker endpoints")
        if scheduler_provider is None:
            definition = _require_cluster(cluster)
    selected_scheduler = scheduler_provider
    if selected_scheduler is None and definition is not None:
        selected_scheduler = definition.scheduler_provider
    worker = EndpointWorker(
        role=role,
        settings=settings,
        cluster=cluster or "local",
        concurrency=concurrency,
        kind_concurrency=kind_limits,
        scheduler_provider=(
            provider_for_scheduler(selected_scheduler) if role == EndpointRole.WORKER else None
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
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    endpoints, next_cursor, total = queue.list_endpoints_page(
        cursor=cursor,
        limit=limit,
        cluster=cluster,
    )
    typer.echo(
        _public_json(
            {
                "endpoints": [endpoint.model_dump(mode="json") for endpoint in endpoints],
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
        int,
        typer.Option(help="Number of in-process worker slots for the user service."),
    ] = 1,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
) -> None:
    """Render a sudo-less systemd user service for a worker endpoint."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=concurrency,
        kind_concurrency=_kind_concurrency_options(kind_concurrency),
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
) -> None:
    """Report fresh process-bound identity for the active cluster worker."""
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(
                worker_runtime_info(
                    cluster=cluster,
                    freshness_seconds=freshness_seconds,
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

    def action() -> None:
        provider = provider_for_scheduler(scheduler_provider)
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

    _run_or_exit(action)


def _physical_site_marker_sha256(path: Path) -> str:
    """Hash the exact physical-site marker bytes used by operator pinning tools."""
    try:
        marker = path.read_bytes()
    except OSError as exc:
        raise ConfigurationError(f"could not read physical site marker: {exc}") from exc
    if not marker.strip():
        raise ConfigurationError("physical site marker is empty")
    return hashlib.sha256(marker).hexdigest()


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
) -> None:
    """Add or update a local cluster definition."""
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
            spack_executable=_none_if_blank(spack_executable),
            frpc_bin=frpc_bin,
            agent_bin=_none_if_blank(agent_bin),
            agent_adapter=agent_adapter,
            scheduler_provider=scheduler_provider,
            target_identity=(
                ClusterTargetIdentity(
                    hostnames=target_hostname,
                    ssh_host_key_sha256=ssh_host_key_sha256,
                    scheduler_cluster_name=_none_if_blank(scheduler_cluster_name),
                    site_marker_sha256=_none_if_blank(site_marker_sha256),
                )
                if target_hostname is not None and ssh_host_key_sha256 is not None
                else None
            ),
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
    ClusterRegistry.mutate(
        default_registry_path(),
        lambda registry: registry.clusters.__setitem__(name, definition),
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
                scheduler_cluster_name=_none_if_blank(scheduler_cluster_name),
                site_marker_sha256=_none_if_blank(site_marker_sha256),
            )
        except ValidationError as exc:
            raise typer.BadParameter(str(exc)) from exc

    def update_target_identity(registry: ClusterRegistry) -> None:
        registry.require(cluster).target_identity = target_identity

    registry = ClusterRegistry.mutate(default_registry_path(), update_target_identity)
    definition = registry.require(cluster)
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


@remote_mcp_app.command("register")
def remote_mcp_register(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Stable server registration name.")],
    command: Annotated[str, typer.Option(help="Remote stdio MCP executable.")],
    arg: Annotated[
        list[str] | None,
        typer.Option(help="Remote MCP command argument. Repeatable and passed without a shell."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
    ] = None,
    allow_tool: Annotated[
        list[str] | None,
        typer.Option(
            help="Exact remote tool name to virtualize. Repeatable; '*' explicitly allows all."
        ),
    ] = None,
    profile: Annotated[
        list[str] | None,
        typer.Option(help="Local MCP profile allowed to expose tools: user, admin, or operator."),
    ] = None,
    namespace: Annotated[
        str | None,
        typer.Option(help="Optional stable namespace used in generated local aliases."),
    ] = None,
    contract: Annotated[
        str | None,
        typer.Option(
            help=(
                "Optional audited semantic contract. Supported: clio-kit-spack-user-v2, "
                "clio-kit-scientific-catalog-user-v1."
            )
        ),
    ] = None,
    schema_cache_ttl_seconds: Annotated[
        int,
        typer.Option(help="Maximum age of a discovered schema before tools are hidden.", min=1),
    ] = 86_400,
    call_timeout_seconds: Annotated[
        int,
        typer.Option(
            help="Maximum duration of each virtual tools/call execution.",
            min=1,
            max=86_400,
        ),
    ] = 300,
    enabled: Annotated[
        bool,
        typer.Option("--enabled/--disabled", help="Enable this remote MCP registration."),
    ] = True,
    replace: Annotated[
        bool,
        typer.Option(help="Replace an existing registration with the same cluster and name."),
    ] = False,
) -> None:
    """Register an allowlisted remote MCP server for one cluster."""
    registry_path = default_registry_path()
    try:
        registration = RemoteMcpServerConfig(
            command=command,
            args=arg or [],
            env_from=_environment_references(env_from),
            namespace=namespace,
            contract=cast(RemoteMcpContract | None, contract),
            allow_tools=allow_tool or [],
            profiles=cast(list[RemoteMcpProfile], profile or ["admin"]),
            schema_cache_ttl_seconds=schema_cache_ttl_seconds,
            call_timeout_seconds=call_timeout_seconds,
            enabled=enabled,
        )
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name in definition.remote_mcp_servers and not replace:
            raise typer.BadParameter(
                f"remote MCP server is already registered for {cluster}: {name}; use --replace"
            )
        definition.remote_mcp_servers[name] = registration

    ClusterRegistry.mutate(registry_path, update_registry)
    cache = RemoteMcpSchemaCache.load(default_remote_mcp_cache_path(registry_path=registry_path))
    cached = cache.entry_for(cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registration": registration.model_dump(mode="json"),
                "execution_fingerprint": remote_mcp_execution_fingerprint(registration),
                "cache_reusable": (
                    cached is not None
                    and cached.execution_fingerprint
                    == remote_mcp_execution_fingerprint(registration)
                ),
                "reload_semantics": (
                    "configuration is read on the next local MCP tools/list; run remote-mcp "
                    "refresh before exposure when the cache is missing, stale, or command-changed"
                ),
            },
            indent=2,
        )
    )


@remote_mcp_app.command("unregister")
def remote_mcp_unregister(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
) -> None:
    """Remove a remote MCP registration and its local schema cache entry."""
    registry_path = default_registry_path()

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        del definition.remote_mcp_servers[name]

    ClusterRegistry.mutate(registry_path, update_registry)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    RemoteMcpSchemaCache.remove_entry(cache_path, cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registered": False,
                "cache_removed": True,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("list")
def remote_mcp_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
) -> None:
    """List registrations and cache freshness/provenance as JSON."""
    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    if cluster is not None:
        registry.require(cluster)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    cache = RemoteMcpSchemaCache.load(cache_path)
    registrations: list[dict[str, object]] = []
    for cluster_name, definition in sorted(registry.clusters.items()):
        if cluster is not None and cluster_name != cluster:
            continue
        for server_name, registration in sorted(definition.remote_mcp_servers.items()):
            entry = cache.entry_for(cluster_name, server_name)
            registrations.append(
                {
                    "cluster": cluster_name,
                    "server_name": server_name,
                    "registration": registration.model_dump(mode="json"),
                    "cache": _remote_mcp_cache_status(registration, entry),
                }
            )
    typer.echo(
        json.dumps(
            {
                "registry_path": str(registry_path),
                "cache_path": str(cache_path),
                "registrations": registrations,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("reload")
def remote_mcp_reload(
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile to render: user, admin, operator, or all."),
    ] = "user",
) -> None:
    """Reload local config/cache and report the exact next tools/list catalog."""
    if profile not in {"user", "admin", "operator", "all"}:
        raise typer.BadParameter("--profile must be user, admin, operator, or all")
    catalog = load_registered_remote_mcp_catalog(profile)
    typer.echo(
        json.dumps(
            {
                "profile": profile,
                "catalog_revision": catalog.revision,
                "tools": catalog.tool_definitions(),
                "issues": [issue.model_dump(mode="json") for issue in catalog.issues],
                "remote_discovery_performed": False,
                "mcp_server_restart_required": False,
                "client_action": "request tools/list again to observe this catalog revision",
            },
            indent=2,
        )
    )


@remote_mcp_app.command("refresh")
def remote_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP protocol session.", min=1),
    ] = 120,
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable discovery job.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable discovery job polling interval.", min=0.05),
    ] = 2,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Optional discovery submission idempotency key."),
    ] = None,
) -> None:
    """Discover a registered server through a durable MCP tools/list relay job."""
    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    definition = registry.require(cluster)
    try:
        registration = definition.remote_mcp_servers[name]
    except KeyError as exc:
        raise typer.BadParameter(
            f"remote MCP server is not registered for {cluster}: {name}"
        ) from exc
    if not registration.enabled:
        raise typer.BadParameter(f"remote MCP server is disabled for {cluster}: {name}")
    key = idempotency_key or f"remote-mcp-discovery:{cluster}:{name}:{uuid4().hex}"

    def action() -> None:
        if should_execute_on_cluster(definition):
            remote_args = [
                "mcp-call",
                "--cluster",
                cluster,
                "--server",
                registration.command,
                "--operation",
                McpOperation.TOOLS_LIST.value,
                "--idempotency-key",
                key,
            ]
            if timeout_seconds is not None:
                remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
            for item in registration.args:
                remote_args.extend(["--server-arg", item])
            for child_name, source_name in sorted(registration.env_from.items()):
                remote_args.extend(["--env-from", f"{child_name}={source_name}"])
            job_id = _last_nonempty_line(run_remote_clio(definition, remote_args))
            wait_result = _json_output(
                run_remote_clio(
                    definition,
                    [
                        "job",
                        "wait",
                        job_id,
                        "--timeout-seconds",
                        str(wait_timeout_seconds),
                        "--poll-seconds",
                        str(poll_seconds),
                    ],
                ),
                "remote discovery wait",
            )
            _require_discovery_success(wait_result, job_id)
            artifact, artifact_payload = _read_remote_mcp_result_artifact(
                definition,
                job_id,
            )
        else:
            queue = _managed_queue_from_env()
            job = queue.submit_job(
                RelayJob(
                    cluster=cluster,
                    kind=JobKind.MCP_CALL,
                    spec=McpCallSpec(
                        server=registration.command,
                        server_args=registration.args,
                        env_from=registration.env_from,
                        operation=McpOperation.TOOLS_LIST,
                        timeout_seconds=timeout_seconds,
                    ),
                    idempotency_key=key,
                )
            )
            terminal = wait_for_terminal(
                queue,
                job.job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            _require_discovery_success(terminal.model_dump(mode="json"), job.job_id)
            artifact, artifact_payload = _read_local_mcp_result_artifact(queue, job.job_id)
            job_id = job.job_id
        entry = cache_entry_from_discovery_artifact(
            cluster=cluster,
            server_name=name,
            registration=registration,
            discovery_job_id=job_id,
            artifact_id=str(artifact["artifact_id"]),
            artifact_sha256=cast(str | None, artifact.get("sha256")),
            artifact_payload=artifact_payload,
        )
        cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
        RemoteMcpSchemaCache.update_entry(cache_path, entry)
        catalogs = {
            profile_name: load_registered_remote_mcp_catalog(profile_name)
            for profile_name in registration.profiles
        }
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "server_name": name,
                    "discovery_job_id": job_id,
                    "cache_path": str(cache_path),
                    "cache_entry": entry.model_dump(mode="json"),
                    "profiles": {
                        profile_name: {
                            "catalog_revision": catalog.revision,
                            "virtual_tools": sorted(catalog.tools),
                        }
                        for profile_name, catalog in catalogs.items()
                    },
                    "mcp_server_restart_required": False,
                    "client_action": "request tools/list again to load the refreshed schemas",
                },
                indent=2,
                default=str,
            )
        )

    _run_or_exit(action)


@dataclass(frozen=True)
class _RemoteMcpValidationRoute:
    """One preflight-resolved virtual alias and its argument wrapping mode."""

    alias: str
    arguments_wrapped: bool


@dataclass(frozen=True)
class _RemoteMcpValidationPreflight:
    """Inputs and immutable routes resolved before any validation dispatch."""

    registry_path: Path
    registry: ClusterRegistry
    definition: ClusterDefinition
    remote_arguments: dict[str, Any]
    routes: dict[str, _RemoteMcpValidationRoute]
    result_expectation: RemoteMcpStructuredResultExpectation | None

    @property
    def fresh_spack_transition(self) -> bool:
        """Return whether this run requests disposable-store install proof."""
        return (
            self.result_expectation is not None
            and self.result_expectation.fresh_install_store_root is not None
        )


@dataclass(frozen=True)
class _RemoteMcpValidationCall:
    """One completed ordinary remote-MCP acceptance call and its protocol result."""

    report: RemoteMcpAcceptanceReport
    protocol_result: dict[str, Any] | None
    stdio_session: PackagedMcpStdioSession


def _resolve_remote_mcp_validation_route(
    *,
    catalog: VirtualRemoteMcpCatalog,
    cluster: str,
    server_name: str,
    remote_tool_name: str,
) -> _RemoteMcpValidationRoute:
    """Resolve exactly one fresh virtual alias before any MCP call is dispatched."""
    aliases = [
        alias
        for alias, virtual in catalog.tools.items()
        if virtual.remote_tool.name == remote_tool_name
        and cluster in virtual.routes
        and virtual.routes[cluster].server_name == server_name
    ]
    if len(aliases) != 1:
        raise typer.BadParameter(
            f"expected one fresh virtual alias for {cluster}/{server_name}/{remote_tool_name}, "
            f"found {len(aliases)}; run remote-mcp refresh and reload"
        )
    virtual = catalog.tools[aliases[0]]
    return _RemoteMcpValidationRoute(
        alias=aliases[0],
        arguments_wrapped=virtual.arguments_wrapped,
    )


@remote_mcp_app.command("validate")
@_acceptance_report_command
def remote_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    tool: Annotated[str, typer.Option(help="Allowlisted remote MCP tool name to call.")],
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote tool."),
    ] = None,
    result_expectation_json: Annotated[
        str,
        typer.Option(
            help=("Optional JSON object describing semantic expectations for structuredContent.")
        ),
    ] = "{}",
    result_expectation_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a structured-result expectation JSON object."),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and the virtual call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable virtual call.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    output_json: Annotated[
        Path | None,
        typer.Option(help="Optional path for the machine-readable acceptance report."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical release-evidence JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in canonical evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Call one virtual tool and emit canonical durable acceptance evidence."""
    canonical_report_path = validation_report or default_report_path(cluster)
    canonical_written = [False]

    def preflight() -> _RemoteMcpValidationPreflight:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        arguments_source = _json_text_from_option(arguments_json, arguments_json_file)
        remote_arguments = _json_object(arguments_source)
        result_expectation: RemoteMcpStructuredResultExpectation | None = None
        if result_expectation_json_file is not None or result_expectation_json != "{}":
            expectation_source = _json_text_from_option(
                result_expectation_json,
                result_expectation_json_file,
            )
            try:
                result_expectation = RemoteMcpStructuredResultExpectation.model_validate(
                    _json_object(expectation_source)
                )
            except ValidationError as exc:
                raise typer.BadParameter(
                    f"structured-result expectation is invalid: {exc.errors()[0]['msg']}"
                ) from exc
        registry_path = default_registry_path()
        registry = ClusterRegistry.load(registry_path)
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        registration = definition.remote_mcp_servers[name]
        if result_expectation is not None:
            if result_expectation.tool != tool:
                raise typer.BadParameter("structured-result expectation tool must match --tool")
            if registration.contract != result_expectation.contract:
                raise typer.BadParameter(
                    "structured-result expectation contract must match the registered contract"
                )
        catalog = load_registered_remote_mcp_catalog(profile)
        fresh_transition = (
            result_expectation is not None
            and result_expectation.fresh_install_store_root is not None
        )
        if fresh_transition:
            if result_expectation is None:
                raise typer.BadParameter("fresh Spack expectation is unavailable")
            if (
                remote_arguments.get("spec") != result_expectation.requested_spec
                or remote_arguments.get("reuse") is not False
            ):
                raise typer.BadParameter(
                    "fresh Spack validation arguments must submit the expected spec "
                    "with reuse=false"
                )
        required_tools = (
            ("spack_find", "spack_install", "spack_locate") if fresh_transition else (tool,)
        )
        routes = {
            remote_tool_name: _resolve_remote_mcp_validation_route(
                catalog=catalog,
                cluster=cluster,
                server_name=name,
                remote_tool_name=remote_tool_name,
            )
            for remote_tool_name in required_tools
        }
        requested_route = routes[tool]
        if not requested_route.arguments_wrapped and "cluster" in remote_arguments:
            raise typer.BadParameter(
                "flat remote tool arguments must not contain reserved key 'cluster'"
            )
        return _RemoteMcpValidationPreflight(
            registry_path=registry_path,
            registry=registry,
            definition=definition,
            remote_arguments=remote_arguments,
            routes=routes,
            result_expectation=result_expectation,
        )

    try:
        prepared = preflight()
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="remote-mcp.preflight",
            summary="validate virtual remote MCP acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        queue.initialize()
        execute_remotely = should_execute_on_cluster(prepared.definition)
        remote_install_info = _remote_worker_info(prepared.definition) if execute_remotely else None
        cache = RemoteMcpSchemaCache.load(
            default_remote_mcp_cache_path(registry_path=prepared.registry_path)
        )
        reserved_names = static_mcp_tool_names()
        if prepared.fresh_spack_transition:
            expectation = prepared.result_expectation
            if expectation is None or expectation.requested_spec is None:
                raise RelayError("fresh Spack transition expectation became unavailable")
            preinstall_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_find",
                route=prepared.routes["spack_find"],
                remote_arguments={"query": expectation.requested_spec},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            _require_passing_remote_mcp_call(preinstall_call, phase="preinstall find")
            _require_spack_preinstall_absent(
                preinstall_call.protocol_result,
                requested_spec=expectation.requested_spec,
            )
            preinstall_configuration = _collect_spack_configuration_observation(
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                expectation=expectation,
                phase="preinstall",
            )
            install_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_install",
                route=prepared.routes["spack_install"],
                remote_arguments=prepared.remote_arguments,
                result_expectation=expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            _require_passing_remote_mcp_call(install_call, phase="fresh install")
            postinstall_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_locate",
                route=prepared.routes["spack_locate"],
                remote_arguments={"spec": f"/{expectation.dag_hash}"},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            postinstall_configuration = _collect_spack_configuration_observation(
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                expectation=expectation,
                phase="postinstall",
            )
            report = build_remote_mcp_spack_fresh_install_transition_report(
                preinstall_report=preinstall_call.report,
                install_report=install_call.report,
                postinstall_report=postinstall_call.report,
                preinstall_protocol_result=preinstall_call.protocol_result,
                install_protocol_result=install_call.protocol_result,
                postinstall_protocol_result=postinstall_call.protocol_result,
                install_expectation=expectation,
                preinstall_configuration=preinstall_configuration,
                postinstall_configuration=postinstall_configuration,
            )
        else:
            requested_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name=tool,
                route=prepared.routes[tool],
                remote_arguments=prepared.remote_arguments,
                result_expectation=prepared.result_expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            report = requested_call.report
        canonical_report = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        if remote_install_info is not None:
            attach_verified_worker_identity(canonical_report, remote_install_info)
        write_validation_report(canonical_report, canonical_report_path)
        canonical_written[0] = True
        rendered = report.model_dump_json(indent=2)
        if output_json is not None:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(rendered + "\n", encoding="utf-8")
        typer.echo(rendered)
        if not report.passed:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not canonical_written[0]:
                failed_report = new_live_validation_report(
                    scenario="remote-mcp",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "remote-mcp.completed", "complete virtual remote MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    _run_or_exit(guarded_action)


def _execute_remote_mcp_validation_call(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    execute_remotely: bool,
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    cluster: str,
    server_name: str,
    profile: str,
    remote_tool_name: str,
    route: _RemoteMcpValidationRoute,
    remote_arguments: dict[str, Any],
    result_expectation: RemoteMcpStructuredResultExpectation | None,
    wait_timeout_seconds: float,
    poll_seconds: float,
    reserved_names: set[str],
) -> _RemoteMcpValidationCall:
    """Run one virtual alias and build its ordinary durable acceptance report."""
    stdio_session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool=route.alias,
        arguments=(
            {"cluster": cluster, "arguments": remote_arguments}
            if route.arguments_wrapped
            else {"cluster": cluster, **remote_arguments}
        ),
    )
    job_id = _mcp_response_job_id(stdio_session.tools_call_response)
    if execute_remotely:
        run_remote_clio(
            definition,
            [
                "job",
                "wait",
                job_id,
                "--timeout-seconds",
                str(wait_timeout_seconds),
                "--poll-seconds",
                str(poll_seconds),
            ],
        )
        call_status = _json_output(
            run_remote_clio(definition, ["job", "status", job_id]),
            "remote MCP validation job status",
        )
        artifacts = _remote_artifact_records(definition, job_id)
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        call_status = get_job_status(queue, job_id)
        artifacts = _complete_local_artifact_records(queue, job_id)
        mcp_result = _read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="provenance",
        )
    protocol_result = (
        cast(dict[str, Any], mcp_result["protocol_result"])
        if mcp_result is not None and isinstance(mcp_result.get("protocol_result"), dict)
        else None
    )
    report = build_remote_mcp_acceptance_report(
        registry=registry,
        cache=cache,
        cluster=cluster,
        server_name=server_name,
        remote_tool_name=remote_tool_name,
        profile=profile,
        call_job_id=job_id,
        call_status=call_status,
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        result_expectation=result_expectation,
        reserved_names=reserved_names,
        mcp_stdio_evidence=stdio_session.evidence(),
    )
    return _RemoteMcpValidationCall(
        report=report,
        protocol_result=protocol_result,
        stdio_session=stdio_session,
    )


def _require_passing_remote_mcp_call(
    call: _RemoteMcpValidationCall,
    *,
    phase: str,
) -> None:
    """Stop a transition before its next mutation when an earlier call failed."""
    if not call.report.passed:
        failed = [check.name for check in call.report.checks if not check.passed]
        raise RelayError(f"{phase} acceptance failed before next dispatch: {failed}")


def _require_spack_preinstall_absent(
    protocol_result: dict[str, Any] | None,
    *,
    requested_spec: str,
) -> None:
    """Require exact structured absence before dispatching the mutating install call."""
    structured = (
        cast(dict[str, Any], protocol_result.get("structuredContent"))
        if protocol_result is not None
        and isinstance(protocol_result.get("structuredContent"), dict)
        else None
    )
    if (
        protocol_result is None
        or protocol_result.get("isError") is True
        or structured is None
        or structured.get("schema_version") != "spack.mcp.result.v1"
        or structured.get("operation") != "find"
        or structured.get("query") != requested_spec
        or structured.get("count") != 0
        or isinstance(structured.get("count"), bool)
        or structured.get("packages") != []
    ):
        raise RelayError(
            "fresh Spack preinstall call did not prove count=0 and packages=[] "
            "for the exact requested spec"
        )


def _collect_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    execute_remotely: bool,
    expectation: RemoteMcpStructuredResultExpectation,
    phase: Literal["preinstall", "postinstall"],
) -> RemoteMcpSpackConfigurationObservation:
    """Collect one real, bounded wrapper/configuration manifest observation."""
    manifest_path = expectation.fresh_install_configuration_manifest_path
    expected_sha256 = expectation.fresh_install_configuration_sha256
    if manifest_path is None or expected_sha256 is None:
        raise RelayError("fresh Spack configuration expectation is incomplete")
    if execute_remotely:
        observation = _collect_remote_spack_configuration_observation(
            definition=definition,
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    else:
        observation = _collect_local_spack_configuration_observation(
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    if (
        observation.phase != phase
        or observation.manifest_path != manifest_path
        or observation.manifest_sha256 != expected_sha256
    ):
        raise RelayError("fresh Spack configuration observation does not match expectation")
    return observation


def _collect_remote_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect a configuration observation through one bounded Bash/SSH command."""
    script = _remote_spack_configuration_observer_script()
    command = " ".join(
        (
            "python3",
            "-c",
            shlex.quote(script),
            shlex.quote(phase),
            shlex.quote(manifest_path),
            shlex.quote(expected_sha256),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES),
            str(MAX_SPACK_CONFIGURATION_TREE_ENTRIES),
        )
    )
    with remote_command_timeout(SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS):
        output = run_remote_shell(definition, command)
    if len(output.encode("utf-8")) > MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES:
        raise RelayError("remote Spack configuration observation output exceeded its bound")
    payload = _json_output(output, f"{phase} Spack configuration observation")
    try:
        return RemoteMcpSpackConfigurationObservation.model_validate(payload)
    except ValidationError as exc:
        raise RelayError(
            f"remote Spack configuration observation is invalid: {exc.errors()[0]['msg']}"
        ) from exc


def _collect_local_spack_configuration_observation(
    *,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect the same evidence locally using POSIX no-follow file operations."""
    if os.name == "nt":
        raise RelayError(
            "local fresh Spack configuration observation requires a POSIX host; "
            "use the configured SSH target from Windows"
        )
    manifest = Path(manifest_path)
    base = manifest.parent
    _require_regular_nonsymlink_directory(base, label="configuration manifest directory")
    manifest_bytes, manifest_size = _read_bounded_regular_nonsymlink_file(
        manifest,
        maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
        label="configuration manifest",
        require_nonempty=True,
    )
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != expected_sha256:
        raise RelayError("configuration manifest SHA-256 does not match the expectation")
    declarations = _parse_spack_configuration_manifest(manifest_bytes)
    _require_exact_spack_configuration_component_set(base, declarations)
    components: list[dict[str, object]] = []
    for declared_sha256, relative_path in declarations:
        component_path = _safe_spack_configuration_component_path(base, relative_path)
        component_bytes, component_size = _read_bounded_regular_nonsymlink_file(
            component_path,
            maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
            label=f"configuration component {relative_path}",
            require_nonempty=False,
        )
        observed_sha256 = hashlib.sha256(component_bytes).hexdigest()
        if observed_sha256 != declared_sha256:
            raise RelayError(f"configuration component SHA-256 changed: {relative_path}")
        components.append(
            {
                "relative_path": relative_path,
                "sha256": observed_sha256,
                "size_bytes": component_size,
                "regular_file": True,
            }
        )
    return RemoteMcpSpackConfigurationObservation.model_validate(
        {
            "phase": phase,
            "manifest_path": manifest_path,
            "manifest_sha256": manifest_sha256,
            "manifest_size_bytes": manifest_size,
            "manifest_regular_file": True,
            "components": components,
        }
    )


_SPACK_MANIFEST_LINE = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")


def _parse_spack_configuration_manifest(payload: bytes) -> list[tuple[str, str]]:
    """Parse one strict, sorted GNU sha256sum manifest within fixed limits."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RelayError("configuration manifest must be UTF-8") from exc
    if not text.endswith("\n") or "\x00" in text:
        raise RelayError("configuration manifest must be newline-terminated text")
    declarations: list[tuple[str, str]] = []
    for line in text.splitlines():
        match = _SPACK_MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise RelayError("configuration manifest contains an invalid sha256sum line")
        relative_path = match.group(2)
        if not _is_canonical_spack_component_relative_path(relative_path):
            raise RelayError("configuration manifest contains an unsafe component path")
        declarations.append((match.group(1), relative_path))
    paths = [relative_path for _digest, relative_path in declarations]
    if not 1 <= len(paths) <= MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS:
        raise RelayError("configuration manifest component count is outside its bound")
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RelayError("configuration manifest component paths must be unique and sorted")
    return declarations


def _is_canonical_spack_component_relative_path(value: str) -> bool:
    """Return whether a manifest component is canonical and safely relative."""
    if (
        not value
        or len(value) > 1_024
        or value.startswith("/")
        or value == "."
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _safe_spack_configuration_component_path(base: Path, relative_path: str) -> Path:
    """Resolve a validated component while rejecting symlinked in-root parents."""
    if not _is_canonical_spack_component_relative_path(relative_path):
        raise RelayError("configuration component path is unsafe")
    current = base
    parts = PurePosixPath(relative_path).parts
    for part in parts[:-1]:
        current /= part
        _require_regular_nonsymlink_directory(
            current,
            label=f"configuration component parent {relative_path}",
        )
    return base.joinpath(*parts)


def _require_exact_spack_configuration_component_set(
    base: Path,
    declarations: list[tuple[str, str]],
) -> None:
    """Reject unmanifested files or symlinks in every covered configuration tree."""
    declared_paths = {relative_path for _digest, relative_path in declarations}
    covered_directories = sorted(
        {PurePosixPath(path).parts[0] for path in declared_paths if "/" in path}
    )
    observed_paths: set[str] = set()
    observed_entries = 0
    for relative_directory in covered_directories:
        directory = base / relative_directory
        _require_regular_nonsymlink_directory(
            directory,
            label=f"configuration tree {relative_directory}",
        )
        observed_entries += 1
        if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
            raise RelayError("configuration tree entry count exceeded its bound")
        pending = [directory]
        while pending:
            current = pending.pop()
            try:
                entries = os.scandir(current)
            except OSError as exc:
                raise RelayError(f"configuration tree is unavailable: {current}") from exc
            with entries:
                for entry in entries:
                    observed_entries += 1
                    if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
                        raise RelayError("configuration tree entry count exceeded its bound")
                    candidate = Path(entry.path)
                    relative_path = candidate.relative_to(base).as_posix()
                    if not _is_canonical_spack_component_relative_path(relative_path):
                        raise RelayError(
                            f"configuration tree entry has an unsafe path: {candidate}"
                        )
                    try:
                        metadata = candidate.lstat()
                    except OSError as exc:
                        raise RelayError(
                            f"configuration tree entry is unavailable: {candidate}"
                        ) from exc
                    if stat.S_ISLNK(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must not be a symbolic link: {candidate}"
                        )
                    if stat.S_ISDIR(metadata.st_mode):
                        pending.append(candidate)
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must be a regular file: {candidate}"
                        )
                    observed_paths.add(relative_path)
    expected_covered_paths = {
        path for path in declared_paths if PurePosixPath(path).parts[0] in covered_directories
    }
    if observed_paths != expected_covered_paths:
        raise RelayError(
            "configuration tree files do not exactly match the bounded manifest: "
            f"missing={sorted(expected_covered_paths - observed_paths)} "
            f"unexpected={sorted(observed_paths - expected_covered_paths)}"
        )


def _require_regular_nonsymlink_directory(path: Path, *, label: str) -> None:
    """Require one existing directory without following its final path entry."""
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RelayError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RelayError(f"{label} must be a non-symlink directory: {path}")


def _read_bounded_regular_nonsymlink_file(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
    require_nonempty: bool,
) -> tuple[bytes, int]:
    """Read one stable regular file through a no-follow descriptor within a byte cap."""
    nofollow = cast(int | None, getattr(os, "O_NOFOLLOW", None))
    if nofollow is None:
        raise RelayError(f"{label} cannot be verified without O_NOFOLLOW support")
    flags = os.O_RDONLY | nofollow | cast(int, getattr(os, "O_CLOEXEC", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RelayError(f"{label} is unavailable or is a symbolic link: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RelayError(f"{label} must be a regular file: {path}")
        if before.st_size > maximum_bytes or (require_nonempty and before.st_size < 1):
            raise RelayError(f"{label} size is outside its bound: {path}")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum_bytes:
                raise RelayError(f"{label} exceeded its byte bound while reading: {path}")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        stable_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if not stable_identity or observed != before.st_size:
            raise RelayError(f"{label} changed while it was observed: {path}")
        return b"".join(chunks), observed
    finally:
        os.close(descriptor)


def _remote_spack_configuration_observer_script() -> str:
    """Return the bounded POSIX observer executed by remote ``bash -lc``."""
    return r"""
import hashlib
import json
import os
import posixpath
import re
import stat
import sys

phase, manifest_path, expected_sha = sys.argv[1:4]
max_manifest, max_components, max_component, max_tree_entries = map(int, sys.argv[4:8])
line_pattern = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")

def safe_relative(value):
    return (
        bool(value)
        and len(value) <= 1024
        and not value.startswith("/")
        and value != "."
        and ".." not in value.split("/")
        and posixpath.normpath(value) == value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )

def require_directory(path):
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"not a non-symlink directory: {path}")

def read_regular(path, maximum, nonempty, retain_bytes=False):
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RuntimeError("O_NOFOLLOW is unavailable")
    descriptor = os.open(path, os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"not a regular file: {path}")
        if before.st_size > maximum or (nonempty and before.st_size < 1):
            raise RuntimeError(f"file size outside bound: {path}")
        digest = hashlib.sha256()
        chunks = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum:
                raise RuntimeError(f"file exceeded bound while reading: {path}")
            digest.update(chunk)
            if retain_bytes:
                chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or observed != before.st_size
        ):
            raise RuntimeError(f"file changed while observed: {path}")
        return digest.hexdigest(), observed, b"".join(chunks)
    finally:
        os.close(descriptor)

base = posixpath.dirname(manifest_path)
require_directory(base)
manifest_sha, manifest_size, manifest_bytes = read_regular(
    manifest_path, max_manifest, True, retain_bytes=True
)
if manifest_sha != expected_sha:
    raise RuntimeError("configuration manifest SHA-256 mismatch")
text = manifest_bytes.decode("utf-8")
if not text.endswith("\n") or "\x00" in text:
    raise RuntimeError("configuration manifest is not newline-terminated text")
declarations = []
for line in text.splitlines():
    match = line_pattern.fullmatch(line)
    if match is None or not safe_relative(match.group(2)):
        raise RuntimeError("configuration manifest line is invalid")
    declarations.append((match.group(1), match.group(2)))
paths = [relative_path for _digest, relative_path in declarations]
if not 1 <= len(paths) <= max_components:
    raise RuntimeError("configuration component count is outside its bound")
if paths != sorted(paths) or len(paths) != len(set(paths)):
    raise RuntimeError("configuration component paths must be unique and sorted")
declared_paths = set(paths)
covered_directories = sorted({path.split("/", 1)[0] for path in paths if "/" in path})
observed_paths = set()
observed_entries = 0
for relative_directory in covered_directories:
    directory = posixpath.join(base, relative_directory)
    require_directory(directory)
    observed_entries += 1
    if observed_entries > max_tree_entries:
        raise RuntimeError("configuration tree entry count exceeded its bound")
    pending = [directory]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                observed_entries += 1
                if observed_entries > max_tree_entries:
                    raise RuntimeError("configuration tree entry count exceeded its bound")
                candidate = entry.path
                relative_path = posixpath.relpath(candidate, base)
                if not safe_relative(relative_path):
                    raise RuntimeError(f"configuration tree entry has an unsafe path: {candidate}")
                metadata = os.lstat(candidate)
                if stat.S_ISLNK(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must not be a symbolic link: {candidate}"
                    )
                if stat.S_ISDIR(metadata.st_mode):
                    pending.append(candidate)
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must be a regular file: {candidate}"
                    )
                observed_paths.add(relative_path)
expected_covered_paths = {
    path for path in declared_paths if path.split("/", 1)[0] in covered_directories
}
if observed_paths != expected_covered_paths:
    missing = sorted(expected_covered_paths - observed_paths)
    unexpected = sorted(observed_paths - expected_covered_paths)
    raise RuntimeError(
        "configuration tree files do not exactly match the bounded manifest: "
        f"missing={missing} unexpected={unexpected}"
    )
components = []
for declared_sha, relative_path in declarations:
    current = base
    parts = relative_path.split("/")
    for part in parts[:-1]:
        current = posixpath.join(current, part)
        require_directory(current)
    component_path = posixpath.join(base, *parts)
    observed_sha, observed_size, _unused = read_regular(component_path, max_component, False)
    if observed_sha != declared_sha:
        raise RuntimeError(f"configuration component SHA-256 mismatch: {relative_path}")
    components.append({
        "relative_path": relative_path,
        "sha256": observed_sha,
        "size_bytes": observed_size,
        "regular_file": True,
    })
print(json.dumps({
    "schema_version": "clio-relay.spack-configuration-observation.v1",
    "phase": phase,
    "manifest_path": manifest_path,
    "manifest_sha256": manifest_sha,
    "manifest_size_bytes": manifest_size,
    "manifest_regular_file": True,
    "components": components,
}, sort_keys=True, separators=(",", ":")))
""".strip()


@cluster_app.command("bootstrap")
@_acceptance_report_command
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
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical cluster-bootstrap JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
) -> None:
    """Bootstrap a configured cluster's tools, relay package, and endpoint directories."""
    report_path = report or default_report_path(cluster)
    try:
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=report_path,
            scenario="cluster-bootstrap",
            cluster=cluster,
            check_id="cluster.bootstrap.preflight",
            summary="validate cluster bootstrap acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=relay_wheel,
        )
        raise

    def action() -> None:
        validation = new_live_validation_report(
            scenario="cluster-bootstrap",
            cluster=cluster,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=sha256_file(relay_wheel) if relay_wheel is not None else None,
        )
        recorder = ValidationRecorder(validation)
        try:
            with recorder.check(
                "cluster.bootstrap",
                "execute the real cluster bootstrap and retrieve its durable receipt",
            ) as evidence:
                lines = bootstrap_cluster_over_ssh(
                    bootstrap_profile=definition.bootstrap_profile,
                    ssh_host=ssh_host or definition.ssh_host,
                    source_root=package_source_root(),
                    cluster=definition.name,
                    core_dir=definition.core_dir,
                    spool_dir=definition.spool_dir,
                    relay_wheel=relay_wheel,
                    relay_artifact_sha256=validation.install_source.artifact_sha256,
                    agent_adapter=definition.agent_adapter,
                    agent_npm_package=definition.agent_npm_package,
                    agent_npm_bin=definition.agent_npm_bin,
                    agent_args=definition.agent_args,
                )
                receipt_lines = [
                    line for line in lines if line.startswith("bootstrap_receipt_json=")
                ]
                if len(receipt_lines) != 1:
                    raise RelayError(
                        "bootstrap did not return exactly one durable invocation receipt"
                    )
                receipt_references = [
                    line.partition("=")[2]
                    for line in lines
                    if line.startswith("bootstrap_receipt=")
                ]
                if len(receipt_references) != 1 or not receipt_references[0]:
                    raise RelayError(
                        "bootstrap did not return exactly one durable receipt reference"
                    )
                receipt = _json_output(
                    receipt_lines[0].partition("=")[2],
                    "bootstrap invocation receipt",
                )
                invocation_id = receipt.get("invocation_id")
                if not isinstance(invocation_id, str) or not invocation_id.startswith("bootstrap_"):
                    raise RelayError("bootstrap receipt omitted its unique invocation identity")
                evidence.append(
                    EvidenceReference(
                        kind="bootstrap_receipt",
                        reference=receipt_references[0],
                        metadata=receipt,
                    )
                )
                recorder.add_resource(
                    ValidationResource(
                        kind="bootstrap_invocation",
                        resource_id=invocation_id,
                        role="cluster_bootstrap",
                        cluster=cluster,
                        state="succeeded",
                        references=receipt_references,
                        metadata={
                            **receipt,
                            "ssh_host": ssh_host or definition.ssh_host,
                            "bootstrap_profile": definition.bootstrap_profile,
                            "output_sha256": hashlib.sha256("\n".join(lines).encode()).hexdigest(),
                        },
                    )
                )
            with recorder.check(
                "worker.target-identity",
                "verify the bootstrapped physical cluster against the operator pin",
            ) as target_evidence:
                target_definition = (
                    definition.model_copy(update={"ssh_host": ssh_host})
                    if ssh_host is not None
                    else definition
                )
                target_identity = _remote_target_identity(target_definition)
                target_evidence.append(
                    EvidenceReference(
                        kind="cluster_target",
                        reference=f"ssh-target:{target_definition.ssh_host}",
                        metadata=target_identity,
                    )
                )
            recorder.add_resource(
                ValidationResource(
                    kind="cluster_target",
                    resource_id=f"target:{cluster}",
                    role="physical_cluster_target",
                    cluster=cluster,
                    state="verified",
                    provider=definition.scheduler_provider,
                    metadata=target_identity,
                )
            )
        except BaseException as exc:
            recorder.finish(exc)
            recorder.write(report_path)
            raise
        recorder.finish()
        recorder.write(report_path)
        lines.append(f"validation.report={report_path.resolve()}")
        _echo_lines(lines)

    _run_or_exit(action)


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
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    require_persistent: Annotated[
        bool,
        typer.Option(
            "--require-persistent/--allow-login-scoped",
            help=(
                "Require systemd user lingering so the enabled worker survives all logouts. "
                "The login-scoped opt-out is diagnostic and not release-gate eligible."
            ),
        ),
    ] = True,
) -> None:
    """Install and optionally start a sudo-less worker endpoint service over SSH."""
    definition = _require_cluster(cluster)
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=concurrency,
        kind_concurrency=_kind_concurrency_options(kind_concurrency),
    )
    _run_or_exit(
        lambda: _echo_lines(
            install_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                service_text=service_text,
                start=start,
                enable=enable,
                require_persistent=require_persistent,
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
    if require_token and settings.api_token is None:
        raise typer.BadParameter(
            "CLIO_RELAY_API_TOKEN is required unless --no-require-token is explicit"
        )
    definition = _require_cluster(cluster)

    def action() -> None:
        with _session_transition_lock(cluster=cluster, session_id=session_id):
            lines = start_remote_session(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                api_token=settings.api_token if require_token else None,
                replace=replace,
            )
            _echo_lines(lines)

    _run_or_exit(action)


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


@session_app.command("submit-jarvis")
def session_submit_jarvis(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    pipeline_yaml_file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Local JARVIS pipeline YAML file.",
        ),
    ],
    idempotency_key: Annotated[str, typer.Option(help="Durable submission identity.")],
    timeout_seconds: Annotated[
        float,
        typer.Option(min=1, max=300, help="Bounded session API transport timeout."),
    ] = 30,
) -> None:
    """Submit JARVIS through the identity-proven exact-generation session API."""
    settings = RelaySettings.from_env().model_copy(
        update={
            "owner_session_id": session_id,
            "owner_session_generation_id": session_generation_id,
            "owner_session_cluster": cluster,
        }
    )
    definition = _require_cluster(cluster)

    def action() -> None:
        job = submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/jarvis",
            payload={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml_file.read_text(encoding="utf-8"),
                "idempotency_key": idempotency_key,
            },
            timeout_seconds=timeout_seconds,
        )
        typer.echo(json.dumps(job.model_dump(mode="json"), indent=2))

    _run_or_exit(action)


@session_app.command("quiesce-intake", hidden=True)
def session_quiesce_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    cleanup_operation_id: Annotated[
        str | None,
        typer.Option(help="Exact cleanup operation id selected by the desktop coordinator."),
    ] = None,
    cleanup_stop_worker: Annotated[
        bool,
        typer.Option(help="Persist worker-stop scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_jobs: Annotated[
        bool,
        typer.Option(help="Persist relay cancellation scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(help="Persist scheduler cancellation scope in the cleanup intent."),
    ] = False,
) -> None:
    """Durably stop one owned API session from accepting new work."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        cleanup_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=cleanup_stop_worker,
            cancel_jobs=cleanup_cancel_jobs,
            cancel_scheduler_jobs=cleanup_cancel_scheduler_jobs,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "quiesced",
                    "cleanup_intent": cleanup_intent,
                }
            )
        )

    _run_or_exit(action)


@session_app.command("admission-status", hidden=True)
def session_admission_status(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
) -> None:
    """Return machine-readable intake state for one exact session generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        typer.echo(
            json.dumps(
                queue.owner_session_generation_status(
                    session_id,
                    session_generation_id=session_generation_id,
                )
            )
        )

    _run_or_exit(action)


@session_app.command("prepare-start", hidden=True)
def session_prepare_start(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    candidate_generation_id: Annotated[
        str,
        typer.Option(help="Fresh candidate generation for an initial start or verified reopen."),
    ],
    recorded_generation_id: Annotated[
        str | None,
        typer.Option(help="Generation from verified durable API-session metadata, if present."),
    ] = None,
) -> None:
    """Atomically select the authoritative generation for an owned API start."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        generation_id = queue.prepare_owner_session_start(
            session_id,
            recorded_generation_id=recorded_generation_id,
            candidate_generation_id=candidate_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": generation_id,
                }
            )
        )

    _run_or_exit(action)


@session_app.command("resume-intake", hidden=True)
def session_resume_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact new or reopened relay session generation id."),
    ],
) -> None:
    """Clear durable intake quiescence for a new owned API generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        queue.clear_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "open",
                }
            )
        )

    _run_or_exit(action)


@session_app.command("mark-closed", hidden=True)
def session_mark_closed(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact verified relay session generation id."),
    ],
    legacy_unversioned_job_id: Annotated[
        list[str] | None,
        typer.Option(help="Exact verified legacy job id covered by this first upgraded teardown."),
    ] = None,
) -> None:
    """Durably close one verified, already-quiesced owner session generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_id or [],
        )
        payload = closure.model_dump(mode="json")
        if legacy_unversioned_job_id:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
        typer.echo(json.dumps(payload))

    _run_or_exit(action)


@session_app.command("detach")
@_acceptance_report_command
def session_detach(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Close the desktop attachment while retaining remote work and session processes."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="cleanup",
        cluster=cluster,
        mode="detach",
        resource_kind="owner_session",
        resource_id=session_id,
        action="detach",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)
    try:
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="cleanup",
            cluster=cluster,
            check_id="session.detach.preflight",
            summary="validate owned session detach inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=canonical_report[0],
        )
        raise

    def action() -> None:
        remote_execution = should_execute_on_cluster(definition)
        queue = _managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = _observe_worker_before_cleanup(definition)
        pre_detach_report = detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        pre_detach_canonical = pre_detach_report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = pre_detach_canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        session_generation_id = _verified_owner_session_detach(
            pre_detach_report,
            session_id=session_id,
        )
        if remote_execution:
            owned_jobs = _list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            owned_jobs = _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        gateway_reports = _cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="detach",
            cancel_scheduler_jobs=False,
        )
        if remote_execution:
            post_operation_jobs = _list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            post_operation_jobs = _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        report = detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        try:
            _verified_owner_session_detach(
                report,
                session_id=session_id,
                expected_session_generation_id=session_generation_id,
            )
        except RelayError as exc:
            detail = str(exc)
            if detail not in report.errors:
                report.errors.append(detail)
        report.resources.extend(
            _owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=False,
                cancel_scheduler_jobs=False,
                post_operation_jobs=post_operation_jobs,
            )
        )
        _merge_gateway_cleanup_resources(report, gateway_reports)
        payload = report.json_payload()
        payload["gateway_sessions"] = gateway_reports
        canonical = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        _write_remote_verified_report(
            canonical,
            definition,
            canonical_report_path,
            observed_worker_info=cleanup_worker_info,
            worker_observation_error=cleanup_worker_error,
        )
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if payload.get("ok") is not True or not canonical_ok:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.detach",
                summary="detach owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    def locked_action() -> None:
        with (
            remote_command_timeout(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS),
            _session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    _run_or_exit(locked_action)


@session_app.command("teardown")
@_acceptance_report_command
def session_teardown(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    stop_worker: Annotated[
        bool,
        typer.Option(help="Also stop the persistent cluster worker service for this cluster."),
    ] = False,
    cancel_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-jobs/--keep-jobs",
            help="Cancel active relay jobs. The safe default leaves all jobs running.",
        ),
    ] = False,
    cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-jobs/--keep-scheduler-jobs",
            help="Also request scheduler cancellation for canceled relay jobs.",
        ),
    ] = False,
    preserve_scheduler_job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--preserve-scheduler-job-id",
            help=(
                "Unrelated active scheduler job id that must remain uncanceled; repeat for "
                "multiple live-gate sentinels. Requires --cancel-jobs and "
                "--cancel-scheduler-jobs."
            ),
        ),
    ] = None,
    relay_cancel_timeout_seconds: Annotated[
        float,
        typer.Option(
            help="Maximum wait for worker-acknowledged relay cancellation cleanup.",
            min=0.01,
            max=MAX_RELAY_CANCEL_TIMEOUT_SECONDS,
        ),
    ] = DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS,
    relay_cancel_poll_seconds: Annotated[
        float,
        typer.Option(
            help="Polling interval while awaiting relay cancellation acknowledgment.",
            min=0.01,
            max=60.0,
        ),
    ] = DEFAULT_RELAY_CANCEL_POLL_SECONDS,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned remote relay session processes, optionally stopping the worker service."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="cleanup",
        cluster=cluster,
        mode="teardown",
        resource_kind="owner_session",
        resource_id=session_id,
        action="teardown",
        cancel_relay_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        stop_worker=stop_worker,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)
    try:
        definition = _require_cluster(cluster)
        scheduler_sentinel_ids = _normalize_scheduler_sentinel_ids(preserve_scheduler_job_ids or [])
        if cancel_scheduler_jobs and not cancel_jobs:
            raise typer.BadParameter(
                "--cancel-scheduler-jobs requires the separate --cancel-jobs flag"
            )
        if scheduler_sentinel_ids and not (cancel_jobs and cancel_scheduler_jobs):
            raise typer.BadParameter(
                "--preserve-scheduler-job-id requires both --cancel-jobs and "
                "--cancel-scheduler-jobs"
            )
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="cleanup",
            cluster=cluster,
            check_id="session.teardown.preflight",
            summary="validate owned session teardown inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=canonical_report[0],
        )
        raise

    def action() -> None:
        remote_execution = should_execute_on_cluster(definition)
        queue = _managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = _observe_worker_before_cleanup(definition)
        pre_teardown_status = status_remote_session(
            definition=definition,
            session_id=session_id,
        )
        session_generation_id = _verified_owner_session_generation(
            pre_teardown_status,
            session_id=session_id,
        )
        local_admission_session_id = _desktop_owner_session_admission_id(
            cluster=cluster,
            session_id=session_id,
        )
        if remote_execution:
            _assert_no_unscoped_desktop_admission_state(
                queue,
                cluster=cluster,
                session_id=session_id,
                session_generation_id=session_generation_id,
            )
        authoritative_admission = _owner_session_admission_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )
        local_cleanup_intent = queue.get_owner_session_cleanup_intent(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
        cleanup_operation_id = _select_owner_session_cleanup_operation(
            authoritative_status=authoritative_admission,
            local_intent=local_cleanup_intent,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        partial = seed_report
        partial.cleanup = CleanupEvidence(
            requested=True,
            mode="teardown",
            operation_id=cleanup_operation_id,
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_jobs and cancel_scheduler_jobs,
            stop_worker=stop_worker,
            actions=[
                {
                    "kind": "owner_session_admission",
                    "resource_id": f"{session_id}:{session_generation_id}",
                    "action": "quiesce",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
                {
                    "kind": "remote_relay_api",
                    "resource_id": session_id,
                    "action": "stop",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
            ],
        )
        admission_resource = ValidationResource(
            kind="owner_session_admission",
            resource_id=f"{session_id}:{session_generation_id}",
            role="cleanup_admission",
            cluster=cluster,
            state="pending",
            metadata={
                "operation_id": cleanup_operation_id,
                "local_admission_session_id": local_admission_session_id,
                "remote_execution": remote_execution,
            },
        )
        api_resource = ValidationResource(
            kind="remote_relay_api",
            resource_id=session_id,
            role="cleanup_target",
            cluster=cluster,
            state="running" if pre_teardown_status.get("running") is True else "stopped",
            metadata={
                "session_generation_id": session_generation_id,
                "ownership_verified": pre_teardown_status.get("ownership_verified") is True,
                "cleanup_operation_id": cleanup_operation_id,
            },
        )
        partial.resources.extend([admission_resource, api_resource])
        partial.cleanup.remaining_resources.extend([admission_resource, api_resource])
        canonical_report[0] = partial
        write_validation_report(partial, canonical_report_path)
        cleanup_intent = _quiesce_owner_session_intake(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            cleanup_operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        partial.resources[0] = partial.resources[0].model_copy(update={"state": "quiesced"})
        partial.cleanup.remaining_resources[0] = partial.resources[0]
        partial.cleanup.actions[0].update(
            {
                "outcome": "quiesced",
                "verified_after_operation": True,
            }
        )
        write_validation_report(partial, canonical_report_path)

        def list_owned_jobs(*, include_terminal: bool = False) -> list[_OwnedRelayJob]:
            if remote_execution:
                return _list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=session_generation_id,
                    include_terminal=include_terminal,
                )
            return _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=include_terminal,
            )

        def list_legacy_jobs() -> list[_OwnedRelayJob]:
            """Discover unversioned records without treating them as this generation's jobs."""
            if remote_execution:
                return _list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=None,
                    include_terminal=True,
                )
            return _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=None,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=True,
            )

        def read_owned_job(job_id: str) -> _OwnedRelayJob:
            return _read_owned_relay_job(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                job_id=job_id,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )

        legacy_jobs = list_legacy_jobs()
        if legacy_jobs:
            for legacy_job in legacy_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=legacy_job.job_id,
                    role="ambiguous_legacy_owner_session",
                    cluster=cluster,
                    state=legacy_job.relay_state.value,
                    provider=legacy_job.scheduler_provider,
                    metadata={
                        "ownership_verified": False,
                        "expected_owner_session_generation_id": session_generation_id,
                        "observed_owner_session_generation_id": None,
                        "mutation_refused": True,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
            write_validation_report(partial, canonical_report_path)
            raise RelayError(
                "owner-session cleanup found unversioned legacy jobs whose generation cannot be "
                "proven; no relay or scheduler cancellation was attempted: "
                + ", ".join(sorted(job.job_id for job in legacy_jobs))
            )

        owned_jobs = list_owned_jobs()
        if cancel_jobs:
            for job in owned_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=job.job_id,
                    role="cleanup_cancel_target",
                    cluster=cluster,
                    state=job.relay_state.value,
                    provider=job.scheduler_provider,
                    metadata={
                        "action": "cancel",
                        "ownership_verified": True,
                        "owner_session_generation_id": session_generation_id,
                        "cleanup_operation_id": cleanup_operation_id,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
                partial.cleanup.actions.append(
                    {
                        "kind": "relay_job",
                        "resource_id": job.job_id,
                        "action": "cancel",
                        "outcome": "pending",
                        "verified_after_operation": False,
                        "residual": True,
                    }
                )
            write_validation_report(partial, canonical_report_path)
        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        for scheduler_job_id in gateway_scheduler_job_ids:
            scheduler_resource = ValidationResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                role="gateway_cleanup_target",
                cluster=cluster,
                state="discovered",
                provider=definition.scheduler_provider,
                metadata={
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "ownership_verified": True,
                    "owner_session_generation_id": session_generation_id,
                    "cleanup_operation_id": cleanup_operation_id,
                },
            )
            partial.resources.append(scheduler_resource)
            partial.cleanup.remaining_resources.append(scheduler_resource)
            partial.cleanup.actions.append(
                {
                    "kind": "scheduler_job",
                    "resource_id": scheduler_job_id,
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                    "source": "gateway",
                }
            )
        if gateway_scheduler_job_ids:
            write_validation_report(partial, canonical_report_path)
        scheduler_sentinel_pre_phases = _preflight_scheduler_sentinels(
            definition,
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        canceled: list[str] = []
        if cancel_jobs:
            try:
                cancellation_targets = (
                    _cancel_remote_owned_jobs(definition, cluster, owned_jobs)
                    if remote_execution
                    else _cancel_local_owned_jobs(queue, owned_jobs)
                )
                canceled.extend(
                    _wait_for_owned_relay_cancellations(
                        cancellation_targets,
                        read_owned_job=read_owned_job,
                        timeout_seconds=relay_cancel_timeout_seconds,
                        poll_seconds=relay_cancel_poll_seconds,
                    )
                )
            except BaseException as exc:
                for action_evidence in partial.cleanup.actions:
                    if action_evidence.get("kind") == "relay_job":
                        action_evidence.update(
                            {
                                "outcome": "failed",
                                "verified_after_operation": False,
                                "residual": True,
                                "detail": str(exc),
                            }
                        )
                write_validation_report(partial, canonical_report_path)
                raise
            canceled_ids = set(canceled)
            for index, resource in enumerate(partial.resources):
                if resource.kind == "relay_job" and resource.resource_id in canceled_ids:
                    partial.resources[index] = resource.model_copy(update={"state": "canceled"})
            partial.cleanup.remaining_resources = [
                resource
                for resource in partial.cleanup.remaining_resources
                if not (resource.kind == "relay_job" and resource.resource_id in canceled_ids)
            ]
            for action_evidence in partial.cleanup.actions:
                if (
                    action_evidence.get("kind") == "relay_job"
                    and action_evidence.get("resource_id") in canceled_ids
                ):
                    action_evidence.update(
                        {
                            "outcome": "canceled",
                            "verified_after_operation": True,
                            "residual": False,
                        }
                    )
            write_validation_report(partial, canonical_report_path)
        report = teardown_remote_session(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=session_generation_id,
            expected_cleanup_operation_id=cast(str, cleanup_intent["operation_id"]),
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        )
        report.cleanup_operation_id = cast(str, cleanup_intent["operation_id"])
        report.cleanup_policy = {
            key: cast(bool, cleanup_intent[key])
            for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        report.relay_cancel_requested = cancel_jobs
        report.scheduler_cancel_requested = cancel_jobs and cancel_scheduler_jobs
        partial = report.to_live_validation_report(
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        partial = partial.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = partial
        post_api_jobs = list_owned_jobs(include_terminal=True)
        initial_job_ids = {job.job_id for job in owned_jobs}
        late_jobs = [job for job in post_api_jobs if job.job_id not in initial_job_ids]
        if cancel_jobs and late_jobs:
            late_targets = (
                _cancel_remote_owned_jobs(definition, cluster, late_jobs)
                if remote_execution
                else _cancel_local_owned_jobs(queue, late_jobs)
            )
            canceled.extend(
                _wait_for_owned_relay_cancellations(
                    late_targets,
                    read_owned_job=read_owned_job,
                    timeout_seconds=relay_cancel_timeout_seconds,
                    poll_seconds=relay_cancel_poll_seconds,
                )
            )
            owned_jobs.extend(late_jobs)

        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        _assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )

        gateway_reports = _cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="teardown",
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            scheduler_sentinel_ids=scheduler_sentinel_ids,
            owned_jobs=owned_jobs,
        )

        scheduler_jobs = list_owned_jobs(include_terminal=True)
        by_job_id: dict[str, _OwnedRelayJob] = {}
        for job in [*owned_jobs, *scheduler_jobs]:
            by_job_id.setdefault(job.job_id, job)
        owned_jobs = list(by_job_id.values())
        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        _assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        report.resources.extend(
            _owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=cancel_jobs,
                cancel_scheduler_jobs=cancel_scheduler_jobs,
                post_operation_jobs=scheduler_jobs,
            )
        )
        if cancel_jobs and cancel_scheduler_jobs:
            scheduler_resources, scheduler_errors = _cancel_owned_scheduler_jobs(
                definition,
                owned_jobs,
            )
            report.resources.extend(scheduler_resources)
            report.errors.extend(scheduler_errors)
        sentinel_resources, sentinel_errors = _scheduler_sentinel_preservation_resources(
            definition,
            scheduler_sentinel_pre_phases,
        )
        report.resources.extend(sentinel_resources)
        report.errors.extend(sentinel_errors)
        final_jobs = list_owned_jobs(include_terminal=True)
        if cancel_jobs:
            uncanceled = [
                job.job_id
                for job in final_jobs
                if job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
                or (
                    job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged
                )
            ]
            if uncanceled:
                report.errors.append(
                    "owned relay jobs remained active after final rescan: "
                    + ", ".join(sorted(uncanceled))
                )
        _merge_gateway_cleanup_resources(report, gateway_reports)
        _verify_owner_session_teardown(
            report,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
        )
        legacy_unversioned_job_ids: list[str] = []
        _mark_owner_session_closed(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
        )
        report.resources.append(
            CleanupResource(
                kind="owner_session",
                resource_id=f"{session_id}:{session_generation_id}",
                location=definition.ssh_host if remote_execution else str(queue.root),
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
                metadata={
                    "session_generation_id": session_generation_id,
                    "cleanup_operation_id": report.cleanup_operation_id,
                    "cleanup_policy": report.cleanup_policy,
                    "covered_legacy_job_ids": legacy_unversioned_job_ids,
                },
            )
        )
        payload = report.json_payload()
        payload["cleanup_evidence"] = report.to_cleanup_evidence(
            stop_worker=stop_worker
        ).model_dump(mode="json")
        payload["relay_jobs"] = {
            "cancel_requested": cancel_jobs,
            "scheduler_cancel_requested": cancel_jobs and cancel_scheduler_jobs,
            "canceled_job_ids": canceled,
        }
        payload["gateway_sessions"] = gateway_reports
        canonical = report.to_live_validation_report(
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        for job_id in canceled:
            canonical.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=job_id,
                    role="cleanup_cancel",
                    cluster=cluster,
                    state="canceled",
                )
            )
        _write_remote_verified_report(
            canonical,
            definition,
            canonical_report_path,
            observed_worker_info=cleanup_worker_info,
            worker_observation_error=cleanup_worker_error,
        )
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if payload.get("ok") is not True or not canonical_ok:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown",
                summary="teardown owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    def locked_action() -> None:
        with (
            remote_command_timeout(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS),
            _session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    _run_or_exit(locked_action)


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
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Content-pinned dependency as ARTIFACT_ID=SHA256. Repeatable.",
        ),
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
        yaml_text = _with_exclusive_scheduler(yaml_text, definition.scheduler_provider)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        _file_idempotency_key(jarvis_yaml, yaml_text)
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        remote_yaml = stage_jarvis_yaml(
            definition,
            jarvis_yaml=jarvis_yaml,
            pipeline_yaml_text=yaml_text,
            idempotency_key=key,
        )
        remote_command = [
            "job",
            "submit",
            "--cluster",
            cluster,
            "--jarvis-yaml",
            remote_yaml,
            "--idempotency-key",
            key,
            "--exclusive" if exclusive else "--shared",
        ]
        for value in used_artifact or []:
            remote_command.extend(["--used-artifact", value])
        _run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_yaml=yaml_text),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@job_app.command("submit-pipeline")
def job_submit_pipeline(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    pipeline_name: Annotated[str, typer.Option(help="Existing JARVIS pipeline name.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Content-pinned dependency as ARTIFACT_ID=SHA256. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit an existing JARVIS pipeline by name on the target cluster."""
    definition = _require_cluster(cluster)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"jarvis-pipeline:{cluster}:{pipeline_name}"
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        remote_command = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            pipeline_name,
            "--idempotency-key",
            key,
        ]
        for value in used_artifact or []:
            remote_command.extend(["--used-artifact", value])
        _run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name=pipeline_name),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
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
    cursor: Annotated[
        int,
        typer.Option(help="One-based task record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum task records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of durable task records for a job as JSON."""
    args = [
        "job",
        "tasks",
        job_id,
        "--cursor",
        str(cursor),
        "--limit",
        str(limit),
    ]
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    tasks, next_cursor, total = queue.list_tasks_page(
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            _record_page_payload(
                "tasks",
                [task.model_dump(mode="json") for task in tasks],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


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
    cursor: Annotated[
        int,
        typer.Option(help="One-based artifact record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of artifact references for a job as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "list-artifacts",
            job_id,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
        ],
    ):
        return
    artifacts, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_artifacts_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            _record_page_payload(
                "artifacts",
                [artifact.model_dump(mode="json") for artifact in artifacts],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@job_app.command("used-artifacts")
def job_used_artifacts(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Artifact ID cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum used-artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List content-pinned artifacts consumed by a job as JSON."""
    remote_args = ["job", "used-artifacts", job_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_used_artifacts_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_artifacts": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@job_app.command("used-by")
def job_used_by(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque edge cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum consuming-job records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List jobs that consumed a content-pinned artifact as JSON."""
    remote_args = ["job", "used-by", artifact_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_artifact_users_page(artifact_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_by": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@job_app.command("progress")
def job_progress(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based progress record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum progress records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of structured progress observations as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "progress",
            job_id,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
        ],
    ):
        return
    progress, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_progress_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            _record_page_payload(
                "progress",
                [item.model_dump(mode="json") for item in progress],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


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
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include succeeded, failed, and canceled jobs."),
    ] = False,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global job source cursor.", min=1),
    ] = 1,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """List relay queue jobs."""
    args = ["queue", "list"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if state is not None:
        args.extend(["--state", state.value])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if include_terminal:
        args.append("--include-terminal")
    args.extend(
        [
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
    )
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = list_queue_jobs(
            queue,
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("owner-jobs")
def queue_owner_jobs(
    owner_session_id: Annotated[
        str,
        typer.Option(help="Exact owner session id."),
    ],
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owner session generation; omit only for legacy membership."),
    ] = None,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include terminal generation members."),
    ] = False,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque owner-session membership cursor."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum source records returned.", min=1, max=500)] = (
        500
    ),
) -> None:
    """List one generation's durable job membership without global history."""
    args = [
        "queue",
        "owner-jobs",
        "--owner-session-id",
        owner_session_id,
        "--limit",
        str(limit),
    ]
    if owner_session_generation_id is not None:
        args.extend(["--owner-session-generation-id", owner_session_generation_id])
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if include_terminal:
        args.append("--include-terminal")
    if cursor is not None:
        args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=owner_session_generation_id,
            cursor=cursor,
            limit=limit,
            cluster=cluster,
            include_terminal=include_terminal,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        json.dumps(
            {
                "jobs": [job.model_dump(mode="json") for job in jobs],
                "owner_session_id": owner_session_id,
                "owner_session_generation_id": owner_session_generation_id,
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_window_count": source_window_count,
            },
            indent=2,
        )
    )


@queue_app.command("migrate-indexes")
def queue_migrate_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to migrate over SSH, or local storage."),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            help="Maximum flat records parsed in each crash-safe batch.", min=1, max=10_000
        ),
    ] = 500,
    all_batches: Annotated[
        bool,
        typer.Option("--all", help="Run bounded batches until migration completes."),
    ] = False,
) -> None:
    """Build v1 active and per-job indexes for an existing v0.9 queue."""
    args = ["queue", "migrate-indexes", "--batch-size", str(batch_size)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if all_batches:
        args.append("--all")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.migrate_indexes_batch(batch_size=batch_size)
        while all_batches and result.get("complete") is not True:
            result = queue.migrate_indexes_batch(batch_size=batch_size)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("migration-status")
def queue_migration_status(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local storage."),
    ] = None,
) -> None:
    """Read the crash-safe queue index migration checkpoint without mutation."""
    if _try_remote_cluster_passthrough(cluster, ["queue", "migration-status"]):
        return
    status_payload = ClioCoreQueue(RelaySettings.from_env().core_dir).index_migration_status()
    typer.echo(json.dumps(status_payload, indent=2))


@queue_app.command("repair-lease-indexes")
def queue_repair_lease_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to repair over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases rebuilt in the crash-safe repair.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Rebuild and prune exact endpoint, kind, identity, and expiry lease indexes."""
    args = ["queue", "repair-lease-indexes", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.repair_lease_operational_indexes(limit=limit)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("audit-lease-capacity")
def queue_audit_lease_capacity(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to audit over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases and index records audited.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Audit canonical leases, exact indexes, and the O(1) capacity aggregate."""
    args = ["queue", "audit-lease-capacity", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    report = ClioCoreQueue(RelaySettings.from_env().core_dir).audit_lease_capacity(limit=limit)
    typer.echo(json.dumps(report, indent=2))
    if report.get("valid") is not True:
        raise typer.Exit(code=1)


@queue_app.command("diagnose")
def queue_diagnose(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """Explain why one exact relay job is not progressing."""
    args = [
        "queue",
        "diagnose",
        job_id,
        "--older-than",
        older_than,
        "--scan-limit",
        str(scan_limit),
    ]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = diagnose_job(
            queue,
            job_id,
            cluster=cluster,
            stale_after_seconds=_parse_age_seconds(older_than),
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("stale")
def queue_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose active jobs should be inspected.")],
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional exact job to inspect without acting on neighboring jobs."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Discover stale relay jobs without changing queue or scheduler state."""
    args = [
        "queue",
        "stale",
        "--cluster",
        cluster,
        "--older-than",
        older_than,
        "--limit",
        str(limit),
        "--scan-limit",
        str(scan_limit),
    ]
    if job_id is not None:
        args.extend(["--job-id", job_id])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    try:
        result = discover_stale_jobs(
            ClioCoreQueue(RelaySettings.from_env().core_dir),
            cluster=cluster,
            older_than_seconds=_parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


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
    queue = _managed_queue_from_env()
    try:
        result = cancel_queue_job(
            queue,
            job_id,
            cluster=cluster,
            scheduler_policy="request-scheduler" if cancel_scheduler_job else "relay-only",
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("cleanup-stale")
def queue_cleanup_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose stale leases should be recovered.")],
    job_id: Annotated[
        str | None,
        typer.Option(
            help="Optional exact job; prevents neighboring stale jobs from being acted on."
        ),
    ] = None,
    max_attempts: Annotated[
        int,
        typer.Option(help="Maximum attempts before expired leased jobs fail instead of requeue."),
    ] = 3,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    cancel_queued: Annotated[
        bool,
        typer.Option(help="Explicitly cancel queued jobs older than the threshold."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(help="Preview recoverable jobs without changing state."),
    ] = True,
    limit: Annotated[int, typer.Option(help="Maximum jobs acted on.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Preview or recover stale jobs; queued cancellation is explicit and relay-only."""
    args = [
        "queue",
        "cleanup-stale",
        "--cluster",
        cluster,
        "--max-attempts",
        str(max_attempts),
        "--older-than",
        older_than,
        "--limit",
        str(limit),
        "--scan-limit",
        str(scan_limit),
    ]
    if job_id is not None:
        args.extend(["--job-id", job_id])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if cancel_queued:
        args.append("--cancel-queued")
    args.append("--dry-run" if dry_run else "--no-dry-run")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = _managed_queue_from_env()
    try:
        result = cleanup_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=_parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            max_attempts=max_attempts,
            dry_run=dry_run,
            cancel_queued=cancel_queued,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("retention-plan")
def queue_retention_plan(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Build a read-only terminal-job retention plan."""
    args = ["queue", "retention-plan", job_id]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    settings = RelaySettings.from_env()
    coordinator = TerminalRetentionCoordinator(
        ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    )
    plan = coordinator.plan(
        job_id,
        expected_updated_at=_optional_datetime(expected_updated_at),
    )
    typer.echo(
        json.dumps(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@queue_app.command("retention-status")
def queue_retention_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read the current crash-resumable retention phase without mutation."""
    if _try_remote_cluster_passthrough(cluster, ["queue", "retention-status", job_id]):
        return
    settings = RelaySettings.from_env()
    plan = TerminalRetentionCoordinator(
        ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    ).plan(job_id)
    typer.echo(
        json.dumps(
            {
                "job_id": job_id,
                "receipt_id": plan.receipt_id,
                "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
                "complete": plan.receipt_phase is not None
                and plan.receipt_phase.value == "complete",
                "eligible": plan.eligible,
                "protections": plan.protections,
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@queue_app.command("retention-collect")
def queue_retention_collect(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to collect over SSH."),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute/--dry-run",
            help="Advance retention; dry-run is the default and never mutates.",
        ),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(help="Maximum bounded retention actions.", min=1, max=100),
    ] = 100,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Preview or advance terminal retention without scheduler cancellation."""
    args = [
        "queue",
        "retention-collect",
        job_id,
        "--execute" if execute else "--dry-run",
        "--batch-size",
        str(batch_size),
    ]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if _try_remote_cluster_passthrough(cluster, args):
        return

    def action() -> None:
        settings = RelaySettings.from_env()
        queue: ClioCoreQueue = (
            storage_managed_queue(settings) if execute else ClioCoreQueue(settings.core_dir)
        )
        result = TerminalRetentionCoordinator(queue, settings.spool_dir).collect(
            job_id,
            execute=execute,
            batch_size=batch_size,
            expected_updated_at=_optional_datetime(expected_updated_at),
        )
        typer.echo(result.model_dump_json(indent=2))

    _run_or_exit(action)


@queue_app.command("validate")
@_acceptance_report_command
def queue_validate(
    cluster: Annotated[str, typer.Option(help="Cluster containing the live worker service.")],
    job_id: Annotated[
        str | None,
        typer.Argument(help="Optional expendable queued compatibility anchor."),
    ] = None,
    kind: Annotated[
        JobKind,
        typer.Option(help="Controlled process kind; 1.0 live validation requires jarvis."),
    ] = JobKind.JARVIS,
    older_than: Annotated[
        str,
        typer.Option(help="Age that makes the queued test job stale, such as 1m or 2h."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
    provider: Annotated[
        str | None,
        typer.Option(
            "--scheduler-provider",
            help="Explicit provider for the bounded scheduler-preservation fixture.",
        ),
    ] = None,
    scheduler_run_seconds: Annotated[
        int,
        typer.Option(help="Bounded scheduler fixture runtime after release.", min=5, max=300),
    ] = 5,
    scheduler_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time for each scheduler fixture transition.", min=0.1, max=600),
    ] = 120.0,
    scheduler_poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler fixture polling interval.", min=0.01, max=10),
    ] = 1.0,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical JSON report path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Acceptance launcher identity, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Acceptance install source override, such as pypi."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional exact wheel whose SHA-256 binds the acceptance report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_artifact_sha256: Annotated[
        str | None,
        typer.Option(hidden=True),
    ] = None,
    report_json_only: Annotated[
        bool,
        typer.Option(hidden=True),
    ] = False,
) -> None:
    """Validate real bounded queue admission, cleanup, and scheduler preservation."""
    resolved_report = report or default_report_path(cluster)
    artifact_sha256 = validation_artifact_sha256 or (
        sha256_file(validation_artifact) if validation_artifact is not None else None
    )

    def action() -> None:
        definition = _require_cluster(cluster)
        selected_provider = provider or definition.scheduler_provider
        if should_execute_on_cluster(definition):
            args = [
                "queue",
                "validate",
                "--cluster",
                cluster,
                "--kind",
                kind.value,
                "--older-than",
                older_than,
                "--scan-limit",
                str(scan_limit),
                "--scheduler-provider",
                selected_provider,
                "--scheduler-run-seconds",
                str(scheduler_run_seconds),
                "--scheduler-timeout-seconds",
                str(scheduler_timeout_seconds),
                "--scheduler-poll-seconds",
                str(scheduler_poll_seconds),
                "--report-json-only",
            ]
            if job_id is not None:
                args.insert(2, job_id)
            if validation_launcher is not None:
                args.extend(["--validation-launcher", validation_launcher])
            if validation_install_source is not None:
                args.extend(["--validation-install-source", validation_install_source])
            if artifact_sha256 is not None:
                args.extend(["--validation-artifact-sha256", artifact_sha256])
            canonical = LiveValidationReport.model_validate_json(
                run_remote_clio(definition, args).strip()
            )
            _write_remote_verified_report(canonical, definition, resolved_report)
            if markdown_report is not None:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        else:
            canonical = run_queue_management_validation(
                _managed_queue_from_env(),
                job_id=job_id,
                cluster=cluster,
                kind=kind,
                older_than_seconds=_parse_age_seconds(older_than),
                scan_limit=scan_limit,
                scheduler_provider=validation_provider_for_scheduler(selected_provider),
                scheduler_run_seconds=scheduler_run_seconds,
                scheduler_timeout_seconds=scheduler_timeout_seconds,
                scheduler_poll_seconds=scheduler_poll_seconds,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact_sha256=artifact_sha256,
            )
            if not report_json_only:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        if report_json_only:
            typer.echo(canonical.model_dump_json(indent=2))
            return
        typer.echo(f"validation.status={canonical.status.value}")
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(canonical.model_dump_json(indent=2))
        if canonical.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    try:
        action()
    except typer.Exit:
        raise
    except BaseException as exc:
        if not report_json_only:
            _write_failed_acceptance_report(
                path=resolved_report,
                scenario="queue-management",
                cluster=cluster,
                check_id="queue.completed",
                summary="complete queue-management acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
            )
        raise


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


@scheduler_app.command("status")
def scheduler_status_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Read and normalize one scheduler job through the configured provider."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "status",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            provider_for_scheduler(selected).poll(scheduler_job_id).model_dump_json(indent=2)
        )
    )


@scheduler_app.command("cancel")
def scheduler_cancel_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Explicitly request cancellation of one scheduler job through its provider."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "cancel",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = provider_for_scheduler(selected).cancel(scheduler_job_id)
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "cancel_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("connector-placement", hidden=True)
def scheduler_connector_placement_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Resolve one provider-verified host for an allocation-scoped connector."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-placement",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .connector_placement(scheduler_job_id)
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command(
    "connector-step-start",
    hidden=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def scheduler_connector_step_start_command(
    ctx: typer.Context,
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Crash-reconciliation marker for the connector step."),
    ],
    output_path: Annotated[
        str,
        typer.Option(help="Absolute cluster-side connector output path."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Launch one asynchronous provider-owned connector step."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    connector_command = list(ctx.args)
    if connector_command and connector_command[0] == "--":
        connector_command = connector_command[1:]
    args = [
        "scheduler",
        "connector-step-start",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
        "--output-path",
        output_path,
        "--",
        *connector_command,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .launch_connector_step(
                scheduler_job_id,
                placement_host=placement_host,
                step_marker=step_marker,
                command=connector_command,
                output_path=output_path,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-status", hidden=True)
def scheduler_connector_step_status_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Observe one exact allocation connector step."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-status",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .poll_connector_step(
                scheduler_job_id,
                scheduler_step_id=scheduler_step_id,
                placement_host=placement_host,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-cancel", hidden=True)
def scheduler_connector_step_cancel_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Cancel one exact connector step without canceling its allocation."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-cancel",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = allocation_connector_provider_for_scheduler(selected).cancel_connector_step(
            scheduler_job_id,
            scheduler_step_id=scheduler_step_id,
        )
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "scheduler_step_id": scheduler_step_id,
                    "cancel_requested": True,
                    "accepted": result.returncode == 0,
                    "returncode": result.returncode,
                    "stdout": result.stdout.strip(),
                    "stderr": result.stderr.strip(),
                },
                indent=2,
            )
        )
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("connector-step-reconcile", hidden=True)
def scheduler_connector_step_reconcile_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Exact connector step reconciliation marker."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Find an interrupted connector launch by exact provider marker."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-reconcile",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        step = allocation_connector_provider_for_scheduler(selected).find_connector_step(
            scheduler_job_id,
            step_marker=step_marker,
            placement_host=placement_host,
        )
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-reconciliation.v1",
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "step_marker": step_marker,
                    "placement_host": placement_host,
                    "found": step is not None,
                    "step": step.model_dump(mode="json") if step is not None else None,
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@scheduler_app.command("submit-held-validation", hidden=True)
def scheduler_submit_held_validation(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    job_name: Annotated[str, typer.Option(help="Unique bounded validation job name.")],
    run_seconds: Annotated[int, typer.Option(help="Bounded sleep duration.")] = 30,
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Submit one held provider-owned validation job."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "submit-held-validation",
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--job-name",
        job_name,
        "--run-seconds",
        str(run_seconds),
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        scheduler_job_id = validation_provider_for_scheduler(selected).submit_held_validation_job(
            job_name=job_name, run_seconds=run_seconds
        )
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "held": True,
                    "owned_validation_job": True,
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@scheduler_app.command("release-validation", hidden=True)
def scheduler_release_validation(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Release one exact held validation job."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "release-validation",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = validation_provider_for_scheduler(selected).release_validation_job(
            scheduler_job_id
        )
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "release_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("validate-lifecycle")
@_acceptance_report_command
def scheduler_validate_lifecycle(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
    run_seconds: Annotated[
        int,
        typer.Option(help="Bounded validation job runtime in seconds."),
    ] = 30,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Timeout for each required lifecycle phase."),
    ] = 180.0,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler polling interval."),
    ] = 1.0,
    report_path: Annotated[
        Path | None,
        typer.Option("--report", help="Canonical scheduler lifecycle JSON path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional Markdown rendering of the JSON report."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in scheduler evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Deterministically validate held-to-completed scheduler lifecycle semantics."""
    resolved_report = report_path or default_report_path(cluster)
    try:
        definition = _require_cluster(cluster)
        selected = provider or definition.scheduler_provider
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=resolved_report,
            scenario="scheduler-lifecycle",
            cluster=cluster,
            check_id="scheduler.preflight",
            summary="validate scheduler lifecycle acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    canonical_report: list[LiveValidationReport | None] = [None]

    def action() -> None:
        report = run_scheduler_lifecycle_validation(
            cluster=cluster,
            definition=definition,
            provider=selected,
            run_seconds=run_seconds,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = report
        if should_execute_on_cluster(definition):
            try:
                attach_verified_worker_identity(
                    report,
                    _remote_worker_info(definition),
                )
            except BaseException as exc:
                recorder = ValidationRecorder(report)
                recorder.record_failure(
                    "worker.identity",
                    "verify exact cluster worker artifact identity",
                    exc,
                )
                recorder.finish(exc)
                write_validation_report(report, resolved_report)
                raise
        write_validation_report(report, resolved_report)
        if markdown_report is not None:
            ValidationRecorder(report).write(resolved_report, markdown_report)
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(report.model_dump_json(indent=2))
        if report.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=resolved_report,
                scenario="scheduler-lifecycle",
                cluster=cluster,
                check_id="scheduler.completed",
                summary="complete scheduler lifecycle acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


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
        _managed_queue_from_env(),
        job_id,
        cancel_scheduler=cancel_scheduler_job,
    )
    typer.echo(f"{job.job_id} {job.state.value}")


_GENERIC_GATEWAY_RUNTIME_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_GENERIC_GATEWAY_CONNECTOR_KEYS = frozenset(
    {"browser_proxy", "desktop_connector", "remote_connector"}
)
_GENERIC_GATEWAY_OWNER_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _reject_generic_cli_gateway_runtime_fields(
    *,
    gateway: dict[str, object],
    metadata: dict[str, object],
) -> None:
    """Keep generic CLI gateway writes outside supervisor-owned runtime identity."""
    protected = [f"gateway.{key}" for key in sorted(_GENERIC_GATEWAY_RUNTIME_KEYS & gateway.keys())]
    transport = gateway.get("transport")
    if isinstance(transport, dict):
        protected.extend(
            f"gateway.transport.{key}"
            for key in sorted(_GENERIC_GATEWAY_CONNECTOR_KEYS & transport.keys())
        )
    protected.extend(
        f"metadata.{key}" for key in sorted(_GENERIC_GATEWAY_OWNER_METADATA_KEYS & metadata.keys())
    )
    if protected:
        raise typer.BadParameter(
            "generic gateway commands cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )


@gateway_app.command("create")
def gateway_create(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable session name.")],
    state: Annotated[
        GatewaySessionState,
        typer.Option(help="Initial gateway session state."),
    ] = GatewaySessionState.CREATED,
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
    gateway_payload = _json_object(gateway_source)
    metadata_payload = _json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload,
        metadata=metadata_payload,
    )
    remote_args = [
        "gateway",
        "create",
        "--cluster",
        cluster,
        "--name",
        name,
        "--state",
        state.value,
        "--gateway-json",
        gateway_source,
        "--resources-json",
        resources_source,
        "--metadata-json",
        metadata_source,
    ]
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
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).create_gateway_session(
        GatewaySession(
            cluster=cluster,
            name=name,
            state=state,
            queue_state=queue_state,
            node=node,
            stdout_uri=stdout_uri,
            stderr_uri=stderr_uri,
            log_uris=log_uri or [],
            gateway=gateway_payload,
            artifacts=artifact or [],
            requested_resources=_json_object(resources_source),
            metadata=metadata_payload,
        )
    )
    typer.echo(_public_json(public_gateway_session(session)))


def _local_gateway_session(
    session_id: str,
    *,
    cluster: str | None,
) -> GatewaySession | None:
    """Return a desktop-owned gateway record before considering remote passthrough."""
    queue = _local_gateway_queue()
    try:
        session = queue.get_gateway_session(session_id)
    except NotFoundError:
        return None
    if cluster is not None and session.cluster != cluster:
        return None
    return session


def _local_gateway_queue() -> ClioCoreQueue:
    """Open the desktop queue without resolving unrelated executable settings."""
    configured = os.getenv("CLIO_RELAY_CORE_DIR")
    if configured:
        core_dir = Path(configured).expanduser().resolve()
    else:
        bootstrap_dir = Path.home() / ".local" / "share" / "clio-relay" / "core"
        core_dir = bootstrap_dir.resolve() if bootstrap_dir.exists() else Path(".clio-relay/core")
    return ClioCoreQueue(core_dir)


@gateway_app.command("list")
def gateway_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global gateway source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum gateway source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
    desktop_cursor: Annotated[
        int | None,
        typer.Option(help="Optional desktop-owned gateway source cursor.", min=1),
    ] = None,
    cluster_cursor: Annotated[
        int | None,
        typer.Option(help="Optional cluster-owned gateway source cursor.", min=1),
    ] = None,
) -> None:
    """List bounded desktop and cluster gateway source windows."""

    def action() -> None:
        resolved_desktop_cursor = desktop_cursor or cursor
        resolved_cluster_cursor = cluster_cursor or cursor
        remote_args = [
            "gateway",
            "list",
            "--cursor",
            str(resolved_cluster_cursor),
            "--limit",
            str(limit),
        ]
        if cluster is not None:
            remote_args.extend(["--cluster", cluster])
        queue = _local_gateway_queue()
        desktop_sessions, desktop_next_cursor, desktop_total = queue.list_gateway_sessions_page(
            cursor=resolved_desktop_cursor,
            limit=limit,
            cluster=cluster,
        )
        cluster_sessions: list[GatewaySession] = []
        cluster_next_cursor: int | None = None
        cluster_total = 0
        query_remote = cluster is not None and _should_query_remote_cluster(cluster)
        if query_remote:
            assert cluster is not None
            definition = _require_cluster(cluster)
            cluster_sessions, cluster_next_cursor, cluster_total = _parse_gateway_page(
                run_remote_clio(definition, remote_args),
                limit=limit,
                expected_cluster=cluster,
            )
        combined = {session.session_id: session for session in cluster_sessions}
        combined.update({session.session_id: session for session in desktop_sessions})
        sessions = sorted(
            combined.values(),
            key=lambda session: (session.created_at, session.session_id),
        )
        typer.echo(
            _public_json(
                {
                    "gateway_sessions": [public_gateway_session(session) for session in sessions],
                    "source_cursor": cursor,
                    "source_limit": limit,
                    "source_next_cursor": (
                        (
                            desktop_next_cursor
                            if desktop_next_cursor == cluster_next_cursor
                            else None
                        )
                        if query_remote
                        else desktop_next_cursor
                    ),
                    "source_next_cursors": {
                        "desktop": desktop_next_cursor,
                        "cluster": cluster_next_cursor,
                    },
                    "source_cursors": {
                        "desktop": resolved_desktop_cursor,
                        "cluster": resolved_cluster_cursor,
                    },
                    "source_totals": {
                        "desktop": desktop_total,
                        "cluster": cluster_total,
                    },
                    "source_total": desktop_total + cluster_total,
                    "source_total_semantics": "sum_of_independent_gateway_source_high_waters",
                    "aggregate_record_limit": limit * 2,
                    "filters_apply_within_source_window": True,
                }
            )
        )

    _run_or_exit(action)


def _should_query_remote_cluster(cluster: str) -> bool:
    """Return whether a CLI read should include the configured remote store."""
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    return should_execute_on_cluster(_require_cluster(cluster))


def _parse_gateway_page(
    payload: str,
    *,
    limit: int,
    expected_cluster: str,
) -> tuple[list[GatewaySession], int | None, int]:
    """Validate a bounded current or legacy remote gateway-list response."""
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RelayError("remote gateway list did not return valid JSON") from exc
    if isinstance(decoded, list):
        raw_sessions = cast(list[object], decoded)
        next_cursor: int | None = None
        total = len(raw_sessions)
    elif isinstance(decoded, dict):
        page = cast(dict[str, object], decoded)
        raw = page.get("gateway_sessions")
        if not isinstance(raw, list):
            raise RelayError("remote gateway page omitted gateway_sessions")
        raw_sessions = cast(list[object], raw)
        raw_next_cursor = page.get("source_next_cursor")
        if raw_next_cursor is not None and not isinstance(raw_next_cursor, int):
            raise RelayError("remote gateway page has an invalid next cursor")
        next_cursor = raw_next_cursor
        raw_total = page.get("source_total")
        if not isinstance(raw_total, int) or raw_total < len(raw_sessions):
            raise RelayError("remote gateway page has an invalid source total")
        total = raw_total
    else:
        raise RelayError("remote gateway list must return an object or legacy array")
    if len(raw_sessions) > limit:
        raise RelayError(f"remote gateway page exceeds the requested {limit}-record limit")
    sessions = [GatewaySession.model_validate(item) for item in raw_sessions]
    if any(session.cluster != expected_cluster for session in sessions):
        raise RelayError("remote gateway page returned a different cluster")
    return sessions, next_cursor, total


@gateway_app.command("get")
def gateway_get(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read a gateway service session."""
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is not None:
        typer.echo(_public_json(public_gateway_session(local_session)))
        return
    remote_args = ["gateway", "get", session_id]
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).get_gateway_session(session_id)
    typer.echo(_public_json(public_gateway_session(session)))


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
    gateway_payload = _json_object(gateway_source) if gateway_source is not None else None
    metadata_payload = _json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload or {},
        metadata=metadata_payload,
    )
    remote_args = ["gateway", "update", session_id]
    if state is not None:
        remote_args.extend(["--state", state.value])
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
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    updates: dict[str, object] = {}
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
    if gateway_payload is not None:
        updates["gateway"] = gateway_payload
    _run_or_exit(
        lambda: typer.echo(
            _public_json(
                public_gateway_session(
                    ClioCoreQueue(RelaySettings.from_env().core_dir).update_gateway_session(
                        session_id,
                        state=state,
                        metadata=metadata_payload,
                        reject_relay_managed_fields=True,
                        **updates,
                    )
                )
            )
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
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(
        cluster, ["gateway", "close", session_id]
    ):
        return
    _run_or_exit(
        lambda: typer.echo(
            _public_json(
                public_gateway_session(
                    ClioCoreQueue(RelaySettings.from_env().core_dir).close_gateway_session(
                        session_id
                    )
                )
            )
        )
    )


@gateway_app.command("start-runtime")
@_acceptance_report_command
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
    owner_session_id: Annotated[
        str | None,
        typer.Option(help="Owned desktop relay session that controls this runtime."),
    ] = None,
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owned desktop relay session generation."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Start and bind a scheduler-backed streaming service runtime."""
    canonical_report_path = validation_report or default_report_path(cluster)
    report_id: list[str | None] = [None]

    def action() -> None:
        definition = _require_cluster(cluster)
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "--owner-session-id and --owner-session-generation-id must be provided together"
            )
        if not runtime_json_file.exists():
            raise ConfigurationError(f"runtime spec does not exist: {runtime_json_file}")
        spec = ServiceRuntimeSpec.model_validate_json(
            runtime_json_file.read_text(encoding="utf-8-sig")
        )
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=queue,
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )

        def start_runtime() -> ServiceRuntimeStartResult:
            if owner_session_id is None or owner_session_generation_id is None:
                return supervisor.start(name=name, spec=spec)
            remote_execution = should_execute_on_cluster(definition)
            local_admission_id = _desktop_owner_session_admission_id(
                cluster=cluster,
                session_id=owner_session_id,
            )
            if remote_execution:
                _assert_no_unscoped_desktop_admission_state(
                    queue,
                    cluster=cluster,
                    session_id=owner_session_id,
                    session_generation_id=owner_session_generation_id,
                )
            process_status = status_remote_session(
                definition=definition,
                session_id=owner_session_id,
            )
            _require_live_owner_session_for_gateway(
                process_status,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            authoritative_admission = _owner_session_admission_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            _require_owner_session_admission_open(
                authoritative_admission,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            local_admission = queue.mirror_owner_session_generation_open(
                local_admission_id,
                session_generation_id=owner_session_generation_id,
            )
            _require_owner_session_admission_open(
                local_admission,
                session_id=local_admission_id,
                session_generation_id=owner_session_generation_id,
            )

            # Reverify the authoritative boundary immediately before the first
            # durable gateway write. The desktop transition lock remains held
            # through all scheduler and connector side effects and rollback.
            process_status = status_remote_session(
                definition=definition,
                session_id=owner_session_id,
            )
            _require_live_owner_session_for_gateway(
                process_status,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            authoritative_admission = _owner_session_admission_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            _require_owner_session_admission_open(
                authoritative_admission,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
            )
            return supervisor.start(
                name=name,
                spec=spec,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
                owner_session_admission_id=local_admission_id,
            )

        if owner_session_id is not None:
            with _session_transition_lock(cluster=cluster, session_id=owner_session_id):
                result = start_runtime()
        else:
            result = start_runtime()
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        report_id[0] = canonical.report_id
        _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            report_already_written = False
            if report_id[0] is not None:
                with suppress(ConfigurationError):
                    report_already_written = (
                        load_validation_report(canonical_report_path).report_id == report_id[0]
                    )
            if not report_already_written:
                artifact_sha256: str | None = None
                if validation_artifact is not None:
                    with suppress(OSError):
                        artifact_sha256 = sha256_file(validation_artifact)
                failed_report = new_live_validation_report(
                    scenario="gateway-runtime",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=artifact_sha256,
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "gateway.start-runtime",
                    "start scheduler-backed gateway runtime",
                    exc,
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    _run_or_exit(guarded_action)


@gateway_app.command("browser-attach", hidden=True)
def gateway_browser_attach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ttl_seconds: Annotated[
        int,
        typer.Option(help="Short-lived browser capability lifetime in seconds."),
    ] = 1_800,
    bind_port: Annotated[
        int | None,
        typer.Option(help="Optional desktop loopback proxy port."),
    ] = None,
) -> None:
    """Issue one sandbox-browser attachment capability for a verified gateway."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_attach(
            session_id=session_id,
            ttl_seconds=ttl_seconds,
            bind_port=bind_port,
        )
        # This is the sole one-time capability output. Do not route it through
        # routine gateway serialization or persist it in the gateway record.
        typer.echo(result.model_dump_json(indent=2))

    _run_or_exit(action)


@gateway_app.command("browser-detach", hidden=True)
def gateway_browser_detach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    attachment_id: Annotated[
        str,
        typer.Option(help="Exact browser attachment identity to revoke."),
    ],
) -> None:
    """Revoke one exact browser capability and stop its owned proxy."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_detach(session_id=session_id, attachment_id=attachment_id)
        typer.echo(_public_json(result.model_dump(mode="json")))

    _run_or_exit(action)


@gateway_app.command("detach-runtime")
@_acceptance_report_command
def gateway_detach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime detach JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway detach evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop the owned desktop connector while retaining the remote runtime and job."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="detach",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="retain",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.detach(session_id=session_id)
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        if (
            result.errors
            or result.residual_resources
            or canonical.status is not ValidationStatus.PASSED
        ):
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.detach-runtime",
                summary="detach owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


@gateway_app.command("attach-runtime")
def gateway_attach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
) -> None:
    """Recreate the desktop connector for a detached owned runtime."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        typer.echo(
            _public_json(public_gateway_session(supervisor.attach(session_id=session_id).session))
        )

    _run_or_exit(action)


@gateway_app.command("stop-runtime")
@_acceptance_report_command
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
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime cleanup JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned runtime relay connectors and optionally cancel scheduler work."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="teardown",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="close",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=cancel_scheduler_job,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.stop(
            session_id=session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if result.errors or result.residual_resources or not canonical_ok:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.stop-runtime",
                summary="stop owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


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
    cursor: Annotated[
        int,
        typer.Option(help="One-based global monitor-rule source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum monitor-rule source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable source window of durable monitor rules as JSON."""
    rules, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_monitor_rules_page(
        cursor=cursor,
        limit=limit,
        job_id=job_id,
    )
    typer.echo(
        json.dumps(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
            },
            indent=2,
        )
    )


@monitor_app.command("run-once")
def monitor_run_once(
    limit: Annotated[int, typer.Option(help="Maximum events read per rule.")] = 100,
) -> None:
    """Evaluate enabled monitor rules once."""
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(evaluate_monitor_rules(_managed_queue_from_env(), limit=limit), indent=2)
        )
    )


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
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Content-pinned dependency as ARTIFACT_ID=SHA256. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit a remote agent task on a configured cluster."""
    definition = _require_cluster(cluster)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"agent:{cluster}:{prompt}:{mcp_config}" + _artifact_use_idempotency_suffix(artifact_uses)
    )
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
        for value in used_artifact or []:
            args.extend(["--used-artifact", value])
        _run_remote_or_exit(definition, args)
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=prompt, mcp_config_path=mcp_config),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@app.command("mcp-call")
def mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    server: Annotated[str, typer.Option(help="Remote MCP server name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="Remote MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="Remote MCP tool name. Required for tools/call."),
    ] = None,
    server_arg: Annotated[
        list[str] | None,
        typer.Option(help="Additional remote MCP server argument. Repeatable."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
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
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Content-pinned dependency as ARTIFACT_ID=SHA256. Repeatable.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time MCP server artifact SHA-256 binding."),
    ] = None,
) -> None:
    """Submit a durable remote MCP call or schema-discovery operation."""
    definition = _require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    server_args = server_arg or []
    environment_references = _environment_references(env_from)
    artifact_uses = _artifact_use_refs(used_artifact)
    server_digest = hashlib.sha256(
        json.dumps(
            {
                "server": server,
                "args": server_args,
                "env_from": environment_references,
                "expected_server_artifact_digest": expected_server_artifact_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    key = idempotency_key or (
        f"mcp:{cluster}:{server_digest}:{operation.value}:{tool}:{digest}"
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        remote_arguments_path: str | None = None
        remote_command = [
            "mcp-call",
            "--cluster",
            cluster,
            "--server",
            server,
            "--operation",
            operation.value,
            "--idempotency-key",
            key,
        ]
        if tool is not None:
            remote_arguments_path = (
                ".local/share/clio-relay/desktop-submissions/"
                f"mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_arguments_path])
        for child_name, source_name in sorted(environment_references.items()):
            remote_command.extend(["--env-from", f"{child_name}={source_name}"])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                ["--expected-server-artifact-digest", expected_server_artifact_digest]
            )
        for value in used_artifact or []:
            remote_command.extend(["--used-artifact", value])
        try:
            if remote_arguments_path is not None:
                write_remote_file(
                    definition,
                    remote_arguments_path,
                    json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
            _run_remote_or_exit(
                definition,
                remote_command
                + (
                    ["--timeout-seconds", str(timeout_seconds)]
                    if timeout_seconds is not None
                    else []
                )
                + [item for value in server_args for item in ("--server-arg", value)],
            )
        finally:
            if remote_arguments_path is not None:
                remove_remote_file(
                    definition,
                    remote_arguments_path,
                    remove_empty_parent=True,
                )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            env_from=environment_references,
            expected_server_artifact_digest=expected_server_artifact_digest,
            operation=operation,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@app.command("jarvis-mcp-call")
def jarvis_mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="JARVIS MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="JARVIS MCP tool name. Required for tools/call."),
    ] = None,
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
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Content-pinned dependency as ARTIFACT_ID=SHA256. Repeatable.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote JARVIS MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time JARVIS MCP artifact SHA-256 binding."),
    ] = None,
) -> None:
    """Submit a JARVIS MCP tool call that runs on the target cluster."""
    running_on_target = (
        os.getenv("CLIO_RELAY_CLI_MODE") == "local"
        and os.getenv("CLIO_RELAY_REMOTE_CLUSTER") == cluster
    )
    definition = None if running_on_target else _require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"mcp:{cluster}:jarvis:{operation.value}:{tool}:{digest}:"
        f"{expected_server_artifact_digest or 'unbound'}"
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if definition is not None and should_execute_on_cluster(definition):
        remote_args: str | None = None
        remote_command = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            operation.value,
            "--idempotency-key",
            key,
        ]
        if tool is not None:
            remote_args = (
                ".local/share/clio-relay/desktop-submissions/"
                f"jarvis-mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_args])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                ["--expected-server-artifact-digest", expected_server_artifact_digest]
            )
        for value in used_artifact or []:
            remote_command.extend(["--used-artifact", value])
        try:
            if remote_args is not None:
                write_remote_file(
                    definition,
                    remote_args,
                    json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
            _run_remote_or_exit(
                definition,
                remote_command
                + (
                    ["--timeout-seconds", str(timeout_seconds)]
                    if timeout_seconds is not None
                    else []
                ),
            )
        finally:
            if remote_args is not None:
                remove_remote_file(definition, remote_args, remove_empty_parent=True)
        return
    server = jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            env_from=jarvis_mcp_env_from(),
            expected_server_artifact_digest=expected_server_artifact_digest,
            operation=operation,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@app.command("jarvis-mcp-refresh")
def jarvis_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for durable tools/list discovery."),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Discovery job polling interval."),
    ] = 2,
) -> None:
    """Refresh the verified JARVIS contract and pre-launch artifact binding."""
    definition = _require_cluster(cluster)

    def action() -> None:
        queue = _managed_queue_from_env()
        queue.initialize()
        job_id, result, artifacts, artifact_payload = _run_jarvis_remote_contract_discovery(
            cluster=cluster,
            definition=definition,
            queue=queue,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        entry, binding = _persist_jarvis_remote_contract_discovery(
            cluster=cluster,
            discovery_job_id=job_id,
            result=result,
            artifacts=artifacts,
            artifact_payload=artifact_payload,
        )
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "discovery_job_id": job_id,
                    "schema_digest": entry.schema_digest,
                    "server_artifact_digest": binding,
                    "expires_at": entry.expires_at.isoformat(),
                    "tool_names": sorted(tool.name for tool in entry.tools),
                    "cache_path": str(
                        default_remote_mcp_cache_path(
                            registry_path=default_registry_path(),
                        )
                    ),
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@app.command("jarvis-mcp-validate")
@_acceptance_report_command
def jarvis_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    package_search_query: Annotated[
        str,
        typer.Option(
            help=("Non-blank application query used to prove bounded JARVIS package discovery."),
        ),
    ] = "",
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the virtual jarvis_run tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for virtual jarvis_run."),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and tools/call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable JARVIS MCP call.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical release-evidence JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in canonical evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Exercise JARVIS run/query semantics and persist release acceptance evidence."""
    report_path = report or default_report_path(cluster)
    report_written = [False]

    def preflight() -> tuple[dict[str, Any], ClusterDefinition, str]:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        normalized_package_search_query = " ".join(package_search_query.split())
        if not normalized_package_search_query:
            raise typer.BadParameter("--package-search-query must not be blank")
        if len(normalized_package_search_query) > 256:
            raise typer.BadParameter("--package-search-query must not exceed 256 characters")
        arguments_source = _json_text_from_option(arguments_json, arguments_json_file)
        arguments = _json_object(arguments_source)
        if "cluster" in arguments:
            raise typer.BadParameter(
                "JARVIS tool arguments must not contain reserved key 'cluster'"
            )
        if not isinstance(arguments.get("pipeline_id"), str):
            raise typer.BadParameter("jarvis-mcp-validate requires a string pipeline_id argument")
        return arguments, _require_cluster(cluster), normalized_package_search_query

    try:
        arguments, definition, normalized_package_search_query = preflight()
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="jarvis-mcp.preflight",
            summary="validate virtual JARVIS MCP acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        queue.initialize()
        (
            remote_discovery_job_id,
            remote_tools_list_result,
            remote_discovery_artifacts,
            remote_discovery_payload,
        ) = _run_jarvis_remote_contract_discovery(
            cluster=cluster,
            definition=definition,
            queue=queue,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _persist_jarvis_remote_contract_discovery(
            cluster=cluster,
            discovery_job_id=remote_discovery_job_id,
            result=remote_tools_list_result,
            artifacts=remote_discovery_artifacts,
            artifact_payload=remote_discovery_payload,
        )
        package_search = _run_jarvis_package_search_query(
            cluster=cluster,
            definition=definition,
            queue=queue,
            profile=profile,
            query=normalized_package_search_query,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        stdio_session = run_packaged_mcp_stdio_session(
            profile=profile,
            tool="jarvis_run",
            arguments={"cluster": cluster, **arguments},
        )
        tools_list_response = stdio_session.tools_list_response
        call_response = stdio_session.tools_call_response
        job_id = _mcp_response_job_id(call_response)
        remote_install_info: dict[str, object] | None = None
        if should_execute_on_cluster(definition):
            remote_install_info = _remote_worker_info(definition)
            call_status, progress, live_progress_observation = _wait_for_remote_jarvis_mcp_progress(
                definition,
                job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            artifacts = _remote_artifact_records(definition, job_id)
            mcp_result = _read_remote_json_artifact_kind(definition, artifacts, kind="mcp_result")
            provenance = _read_remote_json_artifact_kind(definition, artifacts, kind="provenance")
            runtime_metadata = _read_remote_json_artifact_kind(
                definition, artifacts, kind="runtime_metadata"
            )
        else:
            call_status, progress, live_progress_observation = _wait_for_local_jarvis_mcp_progress(
                queue,
                job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            artifacts = _complete_local_artifact_records(queue, job_id)
            mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
            provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
            runtime_metadata = _read_local_json_artifact_kind(
                queue, artifacts, kind="runtime_metadata"
            )
        pipeline_id = runtime_metadata.get("pipeline_id") if runtime_metadata else None
        execution_id = runtime_metadata.get("execution_id") if runtime_metadata else None
        if not isinstance(pipeline_id, str) or not pipeline_id:
            raise RelayError("JARVIS run metadata omitted the pipeline_id required for its query")
        if not isinstance(execution_id, str) or not execution_id:
            raise RelayError("JARVIS run metadata omitted the execution_id required for its query")
        execution_query = _run_post_run_jarvis_execution_query(
            cluster=cluster,
            definition=definition,
            queue=queue,
            profile=profile,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        validation = build_jarvis_mcp_validation_report(
            cluster=cluster,
            tool="jarvis_run",
            tools_list_response=tools_list_response,
            call_response=call_response,
            call_job_id=job_id,
            call_status=call_status,
            artifacts=artifacts,
            mcp_result=mcp_result,
            provenance=provenance,
            runtime_metadata=runtime_metadata,
            progress=progress,
            live_progress_observation=live_progress_observation,
            remote_tools_list_result=remote_tools_list_result,
            remote_discovery_job_id=remote_discovery_job_id,
            remote_discovery_artifacts=remote_discovery_artifacts,
            initialize_response=stdio_session.initialize_response,
            stdio_evidence=stdio_session.evidence(),
            package_search_query=normalized_package_search_query,
            package_search_tools_list_response=package_search.tools_list_response,
            package_search_call_response=package_search.call_response,
            package_search_call_job_id=package_search.call_job_id,
            package_search_call_status=package_search.call_status,
            package_search_artifacts=package_search.artifacts,
            package_search_mcp_result=package_search.mcp_result,
            package_search_provenance=package_search.provenance,
            package_search_initialize_response=package_search.initialize_response,
            package_search_stdio_evidence=package_search.stdio_evidence,
            query_tools_list_response=execution_query.tools_list_response,
            query_call_response=execution_query.call_response,
            query_call_job_id=execution_query.call_job_id,
            query_call_status=execution_query.call_status,
            query_artifacts=execution_query.artifacts,
            query_mcp_result=execution_query.mcp_result,
            query_provenance=execution_query.provenance,
            query_initialize_response=execution_query.initialize_response,
            query_stdio_evidence=execution_query.stdio_evidence,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        if remote_install_info is not None:
            attach_verified_worker_identity(validation, remote_install_info)
        write_validation_report(validation, report_path)
        report_written[0] = True
        typer.echo(validation.model_dump_json(indent=2))
        if validation.status.value != "passed":
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not report_written[0]:
                failed_report = new_live_validation_report(
                    scenario="remote-mcp",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "jarvis-mcp.completed", "complete virtual JARVIS MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(report_path)
            raise

    _run_or_exit(guarded_action)


@app.command("mcp-server")
def mcp_server(
    profile: Annotated[
        str,
        typer.Option(help="MCP tool profile: user, admin, operator, or all."),
    ] = "user",
) -> None:
    """Serve relay job tools over stdio MCP."""
    serve_stdio(profile=profile)


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


@app.command("installation-info")
def show_installation_info() -> None:
    """Print the current package identity and durable cluster install receipt."""
    _run_or_exit(lambda: typer.echo(json.dumps(installation_info(), indent=2, default=str)))


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
@_acceptance_report_command
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
    verify_ssh_transport: Annotated[
        bool,
        typer.Option(
            "--verify-ssh-transport/--no-verify-ssh-transport",
            help="Verify an owned SSH-forward transport and teardown path.",
        ),
    ] = False,
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
    ssh_transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local bind port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote API port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_session_id: Annotated[
        str | None,
        typer.Option(help="Owned remote session id for SSH-forward acceptance."),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering of the JSON report."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(
            help="Launcher evidence, such as uv-tool. Can use the validation environment."
        ),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(
            help="Explicit kind:reference install evidence, such as pypi:clio-relay==1.0.0."
        ),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel artifact whose SHA-256 is recorded in the report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_scenario: Annotated[
        str,
        typer.Option(help="Release-policy scenario recorded in the JSON report."),
    ] = "live-test",
    verify_cluster_deployment: Annotated[
        bool,
        typer.Option(
            "--verify-cluster-deployment/--no-verify-cluster-deployment",
            help="Require the matching installed worker version and a live worker execution.",
        ),
    ] = False,
    require_structured_runtime_metadata: Annotated[
        bool,
        typer.Option(
            "--require-structured-runtime-metadata/--allow-legacy-runtime-metadata",
            help="Require JARVIS-owned structured runtime and scheduler metadata.",
        ),
    ] = True,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for acceptance jobs."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Run configurable live acceptance checks for a cluster."""
    report_path = report or default_report_path(cluster)
    seed_report = new_live_validation_report(
        scenario=validation_scenario,
        cluster=cluster,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    write_validation_report(seed_report, report_path)
    try:
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
    except BaseException as exc:
        current_report = _load_current_acceptance_report(
            report_path,
            expected_report_id=seed_report.report_id,
        )
        _write_failed_acceptance_report(
            path=report_path,
            scenario=validation_scenario,
            cluster=cluster,
            check_id="live.preflight",
            summary="validate live acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=current_report or seed_report,
        )
        raise

    def _run() -> None:
        settings = RelaySettings.from_env()
        try:
            lines = run_live_acceptance(
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
                    verify_ssh_transport=verify_ssh_transport,
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
                    ssh_transport_local_bind_port=ssh_transport_local_bind_port,
                    ssh_transport_remote_api_port=ssh_transport_remote_api_port,
                    ssh_transport_session_id=ssh_transport_session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    validation_launcher=validation_launcher,
                    validation_install_source=validation_install_source,
                    validation_artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                    require_structured_runtime_metadata=require_structured_runtime_metadata,
                    validation_scenario=validation_scenario,
                    verify_cluster_deployment=verify_cluster_deployment,
                    report_id=seed_report.report_id,
                )
            )
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None:
                raise RelayError("live acceptance did not persist the current invocation report")
            if should_execute_on_cluster(definition):
                _write_remote_verified_report(
                    current_report,
                    definition,
                    report_path,
                )
        except BaseException as exc:
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            _write_failed_acceptance_report(
                path=report_path,
                scenario=validation_scenario,
                cluster=cluster,
                check_id="live.completed",
                summary="complete live acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        _echo_lines(lines)

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


def _parse_age_seconds(value: str) -> int:
    """Parse a positive operator age threshold such as ``30m`` or ``2h``."""
    match = re.fullmatch(r"(?P<count>[1-9][0-9]*)(?P<unit>[smhd]?)", value.strip().lower())
    if match is None:
        raise typer.BadParameter("age threshold must be a positive integer with s, m, h, or d")
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86_400}[match.group("unit")]
    return int(match.group("count")) * multiplier


def _optional_datetime(value: str | None) -> datetime | None:
    """Parse an optional strict ISO-8601 timestamp for optimistic concurrency."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("expected timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise typer.BadParameter("expected timestamp must include a timezone")
    return parsed


def _public_json(value: object) -> str:
    """Serialize operator-facing JSON without exposing durable credentials."""
    return json.dumps(redact_sensitive_values(value), indent=2)


def _managed_queue_from_env() -> StorageManagedQueue:
    """Open the production queue with durable storage reconciliation enabled."""
    return storage_managed_queue(RelaySettings.from_env())


def _submit_managed_job(job: RelayJob) -> RelayJob:
    """Submit through storage admission and emit stable JSON on refusal."""
    try:
        return _managed_queue_from_env().submit_job(job)
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc


def _echo_storage_admission_error(error: StorageAdmissionError) -> None:
    """Write the stable CLI storage refusal envelope to stderr."""
    typer.echo(
        json.dumps(
            {
                "error": "storage_admission_denied",
                "storage_decision": error.decision.to_dict(),
            },
            sort_keys=True,
        ),
        err=True,
    )


def _record_page_payload(
    record_key: str,
    records: list[dict[str, object]],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Build the shared one-based collection response used by CLI surfaces."""
    return {
        record_key: records,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


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


def _with_exclusive_scheduler(pipeline_yaml: str, scheduler_provider: str) -> str:
    loaded = yaml.safe_load(pipeline_yaml)
    if not isinstance(loaded, dict):
        raise ConfigurationError("JARVIS YAML must be an object to request exclusive allocation")
    document = cast(dict[str, object], loaded)
    scheduler = document.get("scheduler")
    if scheduler is None:
        if scheduler_provider == "external":
            raise ConfigurationError(
                "--exclusive requires an explicit scheduler provider in the cluster definition"
            )
        scheduler = {"name": scheduler_provider}
    if not isinstance(scheduler, dict):
        raise ConfigurationError("scheduler must be an object to request exclusive allocation")
    typed_scheduler = cast(dict[str, object], scheduler)
    typed_scheduler.setdefault("name", scheduler_provider)
    typed_scheduler["exclusive"] = True
    document["scheduler"] = typed_scheduler
    return yaml.safe_dump(document, sort_keys=False)


@dataclass(frozen=True)
class _OwnedRelayJob:
    job_id: str
    relay_state: JobState
    scheduler_job_ids: tuple[str, ...]
    scheduler_provider: str
    owner_session_generation_id: str | None = None
    unowned_scheduler_job_ids: tuple[str, ...] = ()
    relay_cancellation_requested: bool = False
    relay_cancellation_acknowledged: bool = False
    relay_cancellation_scheduler_requested: bool | None = None


def _quiesce_owner_session_intake(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> dict[str, object]:
    """Quiesce desktop and authoritative intake under one immutable operation id."""
    existing_local_intent = queue.get_owner_session_cleanup_intent(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    if existing_local_intent is None:
        queue.mirror_owner_session_generation_open(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
    local_intent = queue.set_owner_session_closing(
        local_admission_session_id,
        session_generation_id=session_generation_id,
        operation_id=cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    if not remote_execution:
        authoritative_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        _require_matching_cleanup_intents(
            authoritative_intent,
            local_intent,
            cleanup_operation_id=cleanup_operation_id,
        )
        return authoritative_intent
    command = [
        "session",
        "quiesce-intake",
        "--session-id",
        session_id,
        "--session-generation-id",
        session_generation_id,
        "--cleanup-operation-id",
        cleanup_operation_id,
    ]
    if stop_worker:
        command.append("--cleanup-stop-worker")
    if cancel_jobs:
        command.append("--cleanup-cancel-jobs")
    if cancel_scheduler_jobs:
        command.append("--cleanup-cancel-scheduler-jobs")
    raw_result = cast(
        object,
        json.loads(
            run_remote_clio(
                definition,
                command,
            )
        ),
    )
    if not isinstance(raw_result, dict):
        raise RelayError("remote owner-session intake quiescence returned no evidence")
    result = cast(dict[str, object], raw_result)
    if (
        result.get("session_id") != session_id
        or result.get("session_generation_id") != session_generation_id
        or result.get("intake") != "quiesced"
    ):
        raise RelayError("remote owner-session intake quiescence identity did not match")
    raw_intent = result.get("cleanup_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("remote owner-session intake quiescence omitted cleanup intent")
    intent = {str(key): value for key, value in cast(dict[object, object], raw_intent).items()}
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    if (
        intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
        or intent.get("owner_session_id") != session_id
        or intent.get("session_generation_id") != session_generation_id
        or intent.get("operation_id") != cleanup_operation_id
        or any(intent.get(key) is not value for key, value in expected_policy.items())
    ):
        raise RelayError("remote owner-session cleanup intent did not match requested policy")
    _require_matching_cleanup_intents(
        intent,
        local_intent,
        cleanup_operation_id=cleanup_operation_id,
    )
    return intent


def _require_matching_cleanup_intents(
    authoritative: dict[str, object],
    local: dict[str, object],
    *,
    cleanup_operation_id: str,
) -> None:
    """Require identical operation and policy across authoritative and desktop records."""
    keys = (
        "operation_id",
        "session_generation_id",
        "stop_worker",
        "cancel_jobs",
        "cancel_scheduler_jobs",
    )
    if (
        authoritative.get("operation_id") != cleanup_operation_id
        or local.get("operation_id") != cleanup_operation_id
        or any(authoritative.get(key) != local.get(key) for key in keys)
    ):
        raise RelayError("desktop and authoritative owner-session cleanup intents did not match")


def _owner_session_admission_status(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
) -> dict[str, object]:
    """Read and strictly validate exact local or remote owner-session intake state."""
    if remote_execution:
        raw_status = cast(
            object,
            json.loads(
                run_remote_clio(
                    definition,
                    [
                        "session",
                        "admission-status",
                        "--session-id",
                        session_id,
                        "--session-generation-id",
                        session_generation_id,
                    ],
                )
            ),
        )
        if not isinstance(raw_status, dict):
            raise RelayError("remote owner-session admission status was not an object")
        status = {str(key): value for key, value in cast(dict[object, object], raw_status).items()}
    else:
        status = queue.owner_session_generation_status(
            session_id,
            session_generation_id=session_generation_id,
        )
    if (
        status.get("schema_version") != "clio-relay.owner-session-admission-status.v1"
        or status.get("owner_session_id") != session_id
        or status.get("session_generation_id") != session_generation_id
        or not all(
            isinstance(status.get(key), bool) for key in ("active", "closing", "closed", "open")
        )
    ):
        raise RelayError("owner-session admission status identity or schema did not match")
    raw_intent = status.get("cleanup_intent")
    if status.get("closing") is True:
        if not isinstance(raw_intent, dict):
            raise RelayError("closing owner-session admission status omitted cleanup intent")
        intent = cast(dict[str, object], raw_intent)
        operation_id = intent.get("operation_id")
        if (
            intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
            or intent.get("owner_session_id") != session_id
            or intent.get("session_generation_id") != session_generation_id
            or not isinstance(operation_id, str)
            or re.fullmatch(r"cleanup_[A-Za-z0-9_.-]+", operation_id) is None
            or not all(
                isinstance(intent.get(key), bool)
                for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
            )
        ):
            raise RelayError("owner-session admission cleanup intent was invalid")
    elif raw_intent is not None:
        raise RelayError("open owner-session admission status contained a cleanup intent")
    return status


def _require_owner_session_admission_open(
    status: dict[str, object],
    *,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Fail closed unless exact generation intake is authoritatively open."""
    if not (
        status.get("owner_session_id") == session_id
        and status.get("session_generation_id") == session_generation_id
        and status.get("active") is True
        and status.get("closing") is False
        and status.get("closed") is False
        and status.get("open") is True
        and status.get("active_generation_id") == session_generation_id
        and status.get("closing_generation_id") is None
    ):
        raise RelayError(
            "owned session generation is not open for gateway admission: "
            f"{session_id}:{session_generation_id}"
        )


def _select_owner_session_cleanup_operation(
    *,
    authoritative_status: dict[str, object],
    local_intent: dict[str, object] | None,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> str:
    """Reuse a retry operation or choose one id before the first cleanup mutation."""
    _require_durable_session_identity(
        session_generation_id,
        field="session_generation_id",
    )
    if not (
        authoritative_status.get("owner_session_id") == session_id
        and authoritative_status.get("session_generation_id") == session_generation_id
    ):
        raise RelayError("owner-session cleanup admission identity changed")
    if not (
        authoritative_status.get("open") is True or authoritative_status.get("closing") is True
    ):
        raise RelayError("owner-session generation is neither open nor a resumable cleanup")
    raw_authoritative_intent = authoritative_status.get("cleanup_intent")
    authoritative_intent = (
        cast(dict[str, object], raw_authoritative_intent)
        if isinstance(raw_authoritative_intent, dict)
        else None
    )
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    operation_ids: set[str] = set()
    for intent in (authoritative_intent, local_intent):
        if intent is None:
            continue
        if intent.get("session_generation_id") != session_generation_id or any(
            intent.get(key) is not value for key, value in expected_policy.items()
        ):
            raise RelayError("owner-session cleanup retry changed generation or policy")
        operation_id = intent.get("operation_id")
        if not isinstance(operation_id, str):
            raise RelayError("owner-session cleanup retry omitted its operation id")
        _require_durable_session_identity(operation_id, field="operation_id")
        operation_ids.add(operation_id)
    if len(operation_ids) > 1:
        raise RelayError("desktop and authoritative cleanup operation ids disagree")
    return next(iter(operation_ids), f"cleanup_{uuid4().hex}")


def _list_owned_active_cluster_jobs(
    queue: ClioCoreQueue,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    scheduler_provider: str,
    include_terminal: bool = False,
) -> list[_OwnedRelayJob]:
    owned: list[_OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
                owner_session_id,
                session_generation_id=membership_generation,
                cursor=cursor,
                limit=MAX_RESPONSE_PAGE_RECORDS,
                cluster=cluster,
                include_terminal=include_terminal,
            )
            if expected_total is not None and total != expected_total:
                raise RelayError("owner-session membership changed during local discovery")
            expected_total = total
            processed_source += source_window_count
            for job in jobs:
                job_document = job.model_dump(mode="json")
                tasks, tasks_truncated = queue.scan_job_tasks(job.job_id, limit=1_000)
                if tasks_truncated:
                    raise RelayError(f"owner-session task discovery was truncated: {job.job_id}")
                task_documents = [task.model_dump(mode="json") for task in tasks]
                candidate = _owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=scheduler_provider,
                )
                if include_terminal or _relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError("owner-session membership ended before its declared total")
                break
            if cursor is not None and next_cursor <= cursor:
                raise RelayError("owner-session membership cursor did not advance")
            cursor = next_cursor
    return owned


def _list_remote_owned_active_cluster_jobs(
    definition: ClusterDefinition,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    include_terminal: bool = False,
) -> list[_OwnedRelayJob]:
    owned: list[_OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            command = [
                "queue",
                "owner-jobs",
                "--cluster",
                cluster,
                "--owner-session-id",
                owner_session_id,
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ]
            if membership_generation is not None:
                command.extend(["--owner-session-generation-id", membership_generation])
            if include_terminal:
                command.append("--include-terminal")
            if cursor is not None:
                command.extend(["--cursor", cursor])
            payload = _json_output(
                run_remote_clio(definition, command),
                f"remote owner-session jobs for {cluster}",
            )
            raw_jobs = payload.get("jobs")
            if not isinstance(raw_jobs, list):
                raise RelayError("remote owner-session membership returned no jobs array")
            total = payload.get("source_total")
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                raise RelayError("remote owner-session membership returned an invalid total")
            if total > MAX_INTERNAL_COLLECTION_RECORDS:
                raise RelayError(
                    "remote owner-session membership exceeds the bounded source limit "
                    f"{MAX_INTERNAL_COLLECTION_RECORDS}"
                )
            if expected_total is not None and total != expected_total:
                raise RelayError("remote owner-session membership changed during discovery")
            expected_total = total
            source_window_count = payload.get("source_window_count")
            if (
                isinstance(source_window_count, bool)
                or not isinstance(source_window_count, int)
                or source_window_count < 0
                or source_window_count > MAX_RESPONSE_PAGE_RECORDS
            ):
                raise RelayError("remote owner-session membership returned an invalid source count")
            processed_source += source_window_count
            for raw_job in cast(list[object], raw_jobs):
                if not isinstance(raw_job, dict):
                    raise RelayError("remote owner-session membership returned a non-object job")
                job_document = {
                    str(key): value for key, value in cast(dict[object, object], raw_job).items()
                }
                if not _job_is_owned_by_session(
                    job_document,
                    owner_session_id,
                    owner_session_generation_id=owner_session_generation_id,
                ):
                    raise RelayError("remote owner-session membership target identity mismatch")
                job_id = job_document.get("job_id")
                if not isinstance(job_id, str):
                    raise RelayError("remote owner-session membership omitted job_id")
                task_documents = _complete_remote_collection(
                    definition,
                    ["job", "tasks", job_id],
                    record_key="tasks",
                    label=f"remote owner-session tasks for {job_id}",
                )
                candidate = _owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=definition.scheduler_provider,
                )
                if include_terminal or _relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            next_cursor = payload.get("source_next_cursor")
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError(
                        "remote owner-session membership ended before its declared total"
                    )
                break
            if not isinstance(next_cursor, str) or (cursor is not None and next_cursor <= cursor):
                raise RelayError("remote owner-session membership returned an invalid cursor")
            cursor = next_cursor
    return owned


def _cancel_local_owned_jobs(
    queue: ClioCoreQueue,
    jobs: list[_OwnedRelayJob],
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        canceled_job = request_cancel_job(
            queue,
            job.job_id,
            cancel_scheduler=False,
        )
        observed = _owned_relay_job(
            canceled_job.model_dump(mode="json"),
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(canceled_job.job_id)
    return requested


def _cancel_remote_owned_jobs(
    definition: ClusterDefinition,
    cluster: str,
    jobs: list[_OwnedRelayJob],
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        raw_result = cast(
            object,
            json.loads(
                run_remote_clio(
                    definition,
                    [
                        "queue",
                        "cancel",
                        job.job_id,
                        "--cluster",
                        cluster,
                        "--keep-scheduler-job",
                    ],
                )
            ),
        )
        if not isinstance(raw_result, dict):
            raise RelayError(f"owned relay cancellation returned no result: {job.job_id}")
        result = cast(dict[str, object], raw_result)
        if not isinstance(result.get("cancellation_requested"), bool):
            raise RelayError(f"owned relay cancellation omitted request evidence: {job.job_id}")
        raw_job = result.get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"owned relay cancellation omitted its job: {job.job_id}")
        observed = _owned_relay_job(
            {str(key): value for key, value in cast(dict[object, object], raw_job).items()},
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.job_id != job.job_id:
            raise RelayError(f"owned relay cancellation returned a different job: {job.job_id}")
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(job.job_id)
    return requested


def _require_durable_relay_cancellation(job: _OwnedRelayJob) -> None:
    """Require the exact relay-only request and any terminal cleanup acknowledgment."""
    if (
        not job.relay_cancellation_requested
        or job.relay_cancellation_scheduler_requested is not False
    ):
        raise RelayError(f"owned relay job cancellation was not durable: {job.job_id}")
    if job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged:
        raise RelayError(
            f"owned relay job was canceled without worker cleanup acknowledgment: {job.job_id}"
        )


def _read_owned_relay_job(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    job_id: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> _OwnedRelayJob:
    """Read one exact cancellation target and reverify its owner-session identity."""
    if remote_execution:
        raw_status = cast(
            object,
            json.loads(run_remote_clio(definition, ["job", "status", job_id])),
        )
        if not isinstance(raw_status, dict):
            raise RelayError(f"remote relay cancellation status was not an object: {job_id}")
        raw_job = cast(dict[str, object], raw_status).get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"remote relay cancellation status omitted its job: {job_id}")
        document = {str(key): value for key, value in cast(dict[object, object], raw_job).items()}
    else:
        document = queue.get_job(job_id).model_dump(mode="json")
    if document.get("job_id") != job_id or document.get("cluster") != cluster:
        raise RelayError(f"relay cancellation target identity changed: {job_id}")
    if not _job_is_owned_by_session(
        document,
        owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    ):
        raise RelayError(f"relay cancellation target ownership changed: {job_id}")
    return _owned_relay_job(
        document,
        [],
        scheduler_provider=definition.scheduler_provider,
    )


def _wait_for_owned_relay_cancellations(
    job_ids: list[str],
    *,
    read_owned_job: Callable[[str], _OwnedRelayJob],
    timeout_seconds: float,
    poll_seconds: float,
) -> list[str]:
    """Wait boundedly for worker cleanup to acknowledge exact durable cancel requests."""
    if timeout_seconds <= 0:
        raise ValueError("relay cancellation timeout must be positive")
    if poll_seconds <= 0:
        raise ValueError("relay cancellation polling interval must be positive")
    pending = dict.fromkeys(job_ids)
    if len(pending) != len(job_ids):
        raise RelayError("relay cancellation targets must be unique")
    deadline = monotonic() + timeout_seconds
    last_states: dict[str, str] = {}
    while pending:
        for job_id in list(pending):
            remaining = deadline - monotonic()
            if remaining <= 0:
                detail = ", ".join(
                    f"{pending_id}={last_states.get(pending_id, 'missing')}"
                    for pending_id in sorted(pending)
                )
                raise RelayError(
                    "timed out waiting for worker-acknowledged relay cancellation: " + detail
                )
            with remote_command_timeout(min(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS, remaining)):
                observed = read_owned_job(job_id)
            last_states[job_id] = observed.relay_state.value
            _require_durable_relay_cancellation(observed)
            if observed.relay_state is JobState.CANCELED:
                if not observed.relay_cancellation_acknowledged:
                    raise RelayError(
                        "owned relay cancellation reached CANCELED without cleanup evidence: "
                        f"{job_id}"
                    )
                pending.pop(job_id)
                continue
            if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
                raise RelayError(
                    "owned relay cancellation became terminal without acknowledged cleanup: "
                    f"{job_id} ({observed.relay_state.value})"
                )
        if not pending:
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            detail = ", ".join(
                f"{job_id}={last_states.get(job_id, 'missing')}" for job_id in sorted(pending)
            )
            raise RelayError(
                "timed out waiting for worker-acknowledged relay cancellation: " + detail
            )
        sleep(min(poll_seconds, remaining))
    return list(job_ids)


def _job_is_owned_by_session(
    job: dict[str, object],
    owner_session_id: str,
    *,
    owner_session_generation_id: str | None = None,
) -> bool:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return False
    typed_metadata = cast(dict[str, object], metadata)
    if (
        typed_metadata.get("owner") != "clio-relay"
        or typed_metadata.get("owner_session_id") != owner_session_id
    ):
        return False
    recorded_generation = typed_metadata.get("owner_session_generation_id")
    return recorded_generation == owner_session_generation_id


def _relay_cancellation_evidence(
    job_id: str,
    metadata: dict[str, object],
) -> tuple[bool, bool, bool | None]:
    """Parse the durable cancellation request and cleanup acknowledgment contract."""
    raw_request = metadata.get("cancellation_request")
    if raw_request is None:
        return False, False, None
    if not isinstance(raw_request, dict):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    request = cast(dict[str, object], raw_request)
    requested_at = request.get("requested_at")
    previous_state = request.get("previous_state")
    cancel_scheduler = request.get("cancel_scheduler")
    if (
        request.get("schema_version") != "clio-relay.cancellation-request.v1"
        or not isinstance(requested_at, str)
        or previous_state
        not in {
            JobState.QUEUED.value,
            JobState.LEASED.value,
            JobState.RUNNING.value,
        }
        or not isinstance(cancel_scheduler, bool)
    ):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    try:
        parsed_requested_at = datetime.fromisoformat(requested_at)
    except ValueError as exc:
        raise RelayError(
            f"owned relay job has invalid cancellation request time: {job_id}"
        ) from exc
    if parsed_requested_at.tzinfo is None:
        raise RelayError(f"owned relay job cancellation request time is naive: {job_id}")
    acknowledged = request.get("cleanup_acknowledged") is True
    acknowledged_at = request.get("acknowledged_at")
    if acknowledged:
        if not isinstance(acknowledged_at, str):
            raise RelayError(
                f"owned relay job cancellation acknowledgment omitted its time: {job_id}"
            )
        try:
            parsed_acknowledged_at = datetime.fromisoformat(acknowledged_at)
        except ValueError as exc:
            raise RelayError(
                f"owned relay job has invalid cancellation acknowledgment time: {job_id}"
            ) from exc
        if parsed_acknowledged_at.tzinfo is None:
            raise RelayError(f"owned relay job cancellation acknowledgment time is naive: {job_id}")
    elif acknowledged_at is not None:
        raise RelayError(
            f"owned relay job has an acknowledgment time without cleanup proof: {job_id}"
        )
    return True, acknowledged, cancel_scheduler


def _owned_relay_job(
    job: dict[str, object],
    tasks: list[dict[str, object]],
    *,
    scheduler_provider: str,
) -> _OwnedRelayJob:
    job_id = job.get("job_id")
    if not isinstance(job_id, str):
        raise RelayError("owned relay job is missing a job id")
    raw_state = job.get("state")
    if not isinstance(raw_state, str):
        raise RelayError(f"owned relay job is missing its state: {job_id}")
    try:
        relay_state = JobState(raw_state)
    except ValueError as exc:
        raise RelayError(f"owned relay job has an invalid state: {job_id}: {raw_state}") from exc
    job_metadata = job.get("metadata")
    if not isinstance(job_metadata, dict):
        raise RelayError(f"owned relay job is missing metadata: {job_id}")
    typed_job_metadata = cast(dict[str, object], job_metadata)
    raw_generation_id = typed_job_metadata.get("owner_session_generation_id")
    if raw_generation_id is not None and not isinstance(raw_generation_id, str):
        raise RelayError(f"owned relay job has an invalid session generation: {job_id}")
    (
        cancellation_requested,
        cancellation_acknowledged,
        cancellation_scheduler_requested,
    ) = _relay_cancellation_evidence(job_id, typed_job_metadata)
    documents = [job, *tasks]
    observed_scheduler_job_ids: list[str] = []
    owned_scheduler_job_ids: list[str] = []
    provider = _normalized_scheduler_provider(scheduler_provider)
    task_ids = {
        task_id for task in tasks if isinstance((task_id := task.get("task_id")), str) and task_id
    }
    for document in documents:
        metadata = document.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        runtime = typed_metadata.get("runtime_metadata")
        if isinstance(runtime, dict):
            typed_runtime = cast(dict[str, object], runtime)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_runtime.get("scheduler_job_id"),
            )
        _append_scheduler_job_id(
            observed_scheduler_job_ids,
            typed_metadata.get("scheduler_job_id"),
        )
        stored_ids = typed_metadata.get("scheduler_job_ids")
        if isinstance(stored_ids, list):
            for stored_id in cast(list[object], stored_ids):
                _append_scheduler_job_id(observed_scheduler_job_ids, stored_id)
        scheduler_status = typed_metadata.get("scheduler_status")
        if isinstance(scheduler_status, dict):
            typed_status = cast(dict[str, object], scheduler_status)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_status.get("scheduler_job_id"),
            )
        ownership_records = typed_metadata.get("scheduler_job_ownership")
        if not isinstance(ownership_records, list):
            continue
        document_task_id = document.get("task_id")
        for raw_record in cast(list[object], ownership_records):
            if not isinstance(raw_record, dict):
                continue
            record = cast(dict[str, object], raw_record)
            scheduler_job_id = record.get("scheduler_job_id")
            _append_scheduler_job_id(observed_scheduler_job_ids, scheduler_job_id)
            if not isinstance(scheduler_job_id, str) or not scheduler_job_id:
                continue
            record_task_id = record.get("task_id")
            record_provider = record.get("scheduler_provider")
            record_execution_id = record.get("execution_id")
            source = record.get("runtime_metadata_source")
            expected_proofs = {
                "jarvis_mcp": {"owned_jarvis_run_mcp_result"},
                "jarvis_sidecar": {
                    "authenticated_runtime_sidecar",
                    "exact_scheduler_marker_reconciliation",
                },
                "relay_reconciliation": {"exact_scheduler_marker_reconciliation"},
            }.get(source if isinstance(source, str) else "", set())
            if (
                record.get("ownership_verified") is not True
                or record.get("relay_job_id") != job_id
                or not isinstance(document_task_id, str)
                or not isinstance(record_task_id, str)
                or record_task_id not in task_ids
                or document_task_id != record_task_id
                or not isinstance(record_provider, str)
                or _normalized_scheduler_provider(record_provider) != provider
                or not isinstance(record_execution_id, str)
                or not record_execution_id
                or not expected_proofs
                or typed_metadata.get("runtime_metadata_source") != source
                or record.get("proof") not in expected_proofs
            ):
                continue
            _append_scheduler_job_id(owned_scheduler_job_ids, scheduler_job_id)
    unowned_scheduler_job_ids = [
        scheduler_job_id
        for scheduler_job_id in observed_scheduler_job_ids
        if scheduler_job_id not in owned_scheduler_job_ids
    ]
    return _OwnedRelayJob(
        job_id=job_id,
        relay_state=relay_state,
        scheduler_job_ids=tuple(owned_scheduler_job_ids),
        scheduler_provider=provider,
        owner_session_generation_id=raw_generation_id,
        unowned_scheduler_job_ids=tuple(unowned_scheduler_job_ids),
        relay_cancellation_requested=cancellation_requested,
        relay_cancellation_acknowledged=cancellation_acknowledged,
        relay_cancellation_scheduler_requested=cancellation_scheduler_requested,
    )


def _relay_job_needs_cleanup(job: _OwnedRelayJob) -> bool:
    return (
        job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
        or bool(job.scheduler_job_ids)
        or bool(job.unowned_scheduler_job_ids)
    )


def _normalized_scheduler_provider(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _append_scheduler_job_id(target: list[str], value: object) -> None:
    if isinstance(value, str) and value and value not in target:
        target.append(value)


def _normalize_scheduler_sentinel_ids(values: list[str]) -> tuple[str, ...]:
    """Validate and de-duplicate scheduler preservation sentinel ids."""
    normalized: list[str] = []
    for value in values:
        scheduler_job_id = value.strip()
        if not scheduler_job_id:
            raise typer.BadParameter("--preserve-scheduler-job-id cannot be empty")
        if scheduler_job_id not in normalized:
            normalized.append(scheduler_job_id)
    return tuple(normalized)


def _owned_gateway_scheduler_job_ids(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[str, ...]:
    """Discover every exact-generation gateway scheduler allocation without mutation."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway scheduler discovery exceeds the bounded source limit; "
            "no scheduler cancellation was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if should_execute_on_cluster(definition):
        documents.extend(
            _complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway scheduler discovery for {cluster}",
            )
        )
    ids_by_gateway: dict[str, set[str]] = {}
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if not isinstance(session_id, str) or not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
            or typed_metadata.get("owner_session_generation_id") != owner_session_generation_id
        ):
            continue
        exact_ids = ids_by_gateway.setdefault(session_id, set())
        scheduler_job_id = gateway.get("scheduler_job_id")
        if isinstance(scheduler_job_id, str) and scheduler_job_id:
            exact_ids.add(scheduler_job_id)
        raw_gateway = gateway.get("gateway")
        if not isinstance(raw_gateway, dict):
            continue
        ownership_intents = cast(dict[str, object], raw_gateway).get("ownership_intents")
        if not isinstance(ownership_intents, dict):
            continue
        raw_scheduler_intent = cast(dict[str, object], ownership_intents).get(
            "scheduler_submission"
        )
        if not isinstance(raw_scheduler_intent, dict):
            continue
        scheduler_intent = cast(dict[str, object], raw_scheduler_intent)
        intent_state = scheduler_intent.get("state")
        intent_scheduler_job_id = scheduler_intent.get("scheduler_job_id")
        if isinstance(intent_scheduler_job_id, str) and intent_scheduler_job_id:
            exact_ids.add(intent_scheduler_job_id)
        if intent_state in {"starting", "recorded"} and not exact_ids:
            raise RelayError(
                "owned gateway has an unresolved scheduler submission; no scheduler "
                f"cancellation was attempted: {session_id}"
            )
        if len(exact_ids) > 1:
            raise RelayError(
                "owned gateway scheduler identity disagrees across durable evidence; no "
                f"scheduler cancellation was attempted: {session_id}"
            )
    return tuple(sorted({job_id for ids in ids_by_gateway.values() for job_id in ids}))


def _assert_scheduler_sentinels_unrelated(
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[_OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
) -> None:
    """Fail closed if a preservation sentinel appears in session-owned job evidence."""
    session_scheduler_ids = {
        scheduler_job_id
        for job in jobs
        for scheduler_job_id in (*job.scheduler_job_ids, *job.unowned_scheduler_job_ids)
    }
    session_scheduler_ids.update(gateway_scheduler_job_ids)
    conflicts = sorted(set(scheduler_sentinel_ids) & session_scheduler_ids)
    if conflicts:
        raise RelayError(
            "scheduler preservation sentinel ids appeared in owned or unowned scheduler "
            "evidence for the target session generation; no scheduler cancellation was "
            "attempted: " + ", ".join(conflicts)
        )


def _preflight_scheduler_sentinels(
    definition: ClusterDefinition,
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[_OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
) -> dict[str, str]:
    """Prove unrelated scheduler sentinels are active before cleanup mutation."""
    _assert_scheduler_sentinels_unrelated(
        scheduler_sentinel_ids,
        jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
    )
    provider = definition.scheduler_provider
    observed_phases: dict[str, str] = {}
    errors: list[str] = []
    for scheduler_job_id in scheduler_sentinel_ids:
        phase, error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
        )
        normalized_phase = phase.strip().lower() if phase is not None else "unknown"
        if error is not None or normalized_phase not in SCHEDULER_SENTINEL_ACTIVE_PHASES:
            errors.append(
                f"{scheduler_job_id} phase={normalized_phase}"
                + (f" error={error}" if error is not None else "")
            )
            continue
        observed_phases[scheduler_job_id] = normalized_phase
    if errors:
        raise RelayError(
            "scheduler preservation sentinels must be unrelated active jobs before "
            "cancellation; " + "; ".join(errors)
        )
    return observed_phases


def _scheduler_sentinel_preservation_resources(
    definition: ClusterDefinition,
    pre_phases: dict[str, str],
) -> tuple[list[CleanupResource], list[str]]:
    """Re-poll scheduler sentinels and emit canonical preservation evidence."""
    provider = definition.scheduler_provider
    resources: list[CleanupResource] = []
    errors: list[str] = []
    for scheduler_job_id, pre_phase in pre_phases.items():
        phase, poll_error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
        )
        post_phase = phase.strip().lower() if phase is not None else "unknown"
        preserved = poll_error is None and post_phase in SCHEDULER_SENTINEL_PRESERVED_PHASES
        detail = (
            "unrelated scheduler sentinel remained active after owned cancellation"
            if preserved and post_phase != "completed"
            else "unrelated scheduler sentinel completed naturally during owned cancellation"
            if preserved
            else "unrelated scheduler sentinel preservation was not proven"
            + (f": {poll_error}" if poll_error is not None else f": phase={post_phase}")
        )
        resource = CleanupResource(
            kind="scheduler_sentinel",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="retain",
            ownership_verified=False,
            outcome="retained" if preserved else "failed",
            provider=provider,
            verified_after_operation=preserved,
            observed_state=post_phase,
            residual=not preserved,
            detail=detail,
            metadata={
                "unowned_sentinel": True,
                "active_before_operation": True,
                "preservation_verified": preserved,
                "pre_phase": pre_phase,
                "post_phase": post_phase,
            },
        )
        resources.append(resource)
        if not preserved:
            errors.append(f"scheduler sentinel {scheduler_job_id} was not preserved: {detail}")
    return resources, errors


def _owned_job_cleanup_resources(
    jobs: list[_OwnedRelayJob],
    *,
    definition: ClusterDefinition,
    location: str,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    post_operation_jobs: list[_OwnedRelayJob] | None = None,
) -> list[CleanupResource]:
    resources: list[CleanupResource] = []
    post_by_id = {
        job.job_id: job for job in (post_operation_jobs if post_operation_jobs is not None else [])
    }
    for job in jobs:
        relay_active = job.relay_state in {
            JobState.QUEUED,
            JobState.LEASED,
            JobState.RUNNING,
        }
        post_job = post_by_id.get(job.job_id)
        canceled_with_cleanup = (
            post_job is not None
            and post_job.relay_state is JobState.CANCELED
            and post_job.relay_cancellation_requested
            and post_job.relay_cancellation_acknowledged
            and post_job.relay_cancellation_scheduler_requested is False
        )
        completed_before_request = (
            post_job is not None
            and post_job.relay_state in {JobState.SUCCEEDED, JobState.FAILED}
            and not post_job.relay_cancellation_requested
        )
        relay_verified = (
            canceled_with_cleanup or completed_before_request
            if cancel_jobs and relay_active
            else post_job is not None
        )
        if not relay_active:
            relay_action: Literal["retain", "stop", "close", "cancel"] = "retain"
            relay_outcome: Literal[
                "retained",
                "stopped",
                "closed",
                "canceled",
                "terminal",
                "missing",
                "refused",
                "failed",
            ] = "terminal"
            relay_verified = True
            relay_detail = (
                f"relay job was already terminal ({job.relay_state.value}); "
                "owned scheduler resources were evaluated independently"
            )
        else:
            relay_action = "cancel" if cancel_jobs else "retain"
            if cancel_jobs and canceled_with_cleanup:
                relay_outcome = "canceled"
                relay_detail = (
                    "worker cleanup acknowledged the durable relay-only cancellation request"
                )
            elif cancel_jobs and completed_before_request:
                relay_outcome = "terminal"
                relay_detail = "relay job completed before the cancellation request won the race"
            elif not cancel_jobs and relay_verified:
                relay_outcome = "retained"
                relay_detail = "relay job ownership matched and retention was verified"
            else:
                relay_outcome = "failed"
                relay_detail = "owned relay job cancellation or retention was not verified"
        resources.append(
            CleanupResource(
                kind="relay_job",
                resource_id=job.job_id,
                location=location,
                action=relay_action,
                ownership_verified=True,
                outcome=relay_outcome,
                verified_after_operation=relay_verified,
                residual=not relay_verified,
                detail=relay_detail,
                metadata={"scheduler_job_ids": list(job.scheduler_job_ids)},
            )
        )
        for scheduler_job_id in job.scheduler_job_ids:
            if cancel_jobs and cancel_scheduler_jobs:
                continue
            scheduler_verified = False
            phase: str | None = None
            status_error: str | None = None
            if not cancel_scheduler_jobs:
                phase, status_error = _scheduler_phase_after_operation(
                    definition,
                    scheduler_job_id,
                    provider=job.scheduler_provider,
                )
                scheduler_verified = phase in {
                    "submitted",
                    "pending",
                    "allocated",
                    "running",
                    "completed",
                    "failed",
                    "canceled",
                    "missing",
                }
            scheduler_terminal = phase in {"completed", "failed", "canceled", "missing"}
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action="retain",
                    ownership_verified=True,
                    outcome=(
                        "missing"
                        if phase == "missing"
                        else "terminal"
                        if scheduler_verified and scheduler_terminal
                        else "retained"
                        if scheduler_verified
                        else "failed"
                    ),
                    provider=job.scheduler_provider,
                    verified_after_operation=scheduler_verified,
                    observed_state=phase,
                    residual=not scheduler_verified,
                    detail=(
                        "scheduler cancellation was not requested; no active scheduler record "
                        "remained after the operation"
                        if phase == "missing"
                        else (
                            "scheduler cancellation was not requested; "
                            f"post-operation phase={phase}"
                        )
                        if scheduler_verified
                        else "scheduler preservation was not verified"
                        + (f": {status_error}" if status_error else "")
                    ),
                    metadata={"relay_job_id": job.job_id},
                )
            )
        for scheduler_job_id in job.unowned_scheduler_job_ids:
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action=("cancel" if cancel_jobs and cancel_scheduler_jobs else "retain"),
                    ownership_verified=False,
                    outcome="refused",
                    provider=job.scheduler_provider,
                    verified_after_operation=False,
                    residual=True,
                    detail=(
                        "scheduler identity was observed but no ownership record bound it "
                        "to this relay job and task with an authenticated JARVIS proof"
                    ),
                )
            )
    return resources


def _scheduler_phase_after_operation(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
) -> tuple[str | None, str | None]:
    try:
        if should_execute_on_cluster(definition):
            raw_status = cast(
                object,
                json.loads(
                    run_remote_clio(
                        definition,
                        [
                            "scheduler",
                            "status",
                            scheduler_job_id,
                            "--cluster",
                            definition.name,
                            "--provider",
                            provider,
                        ],
                    )
                ),
            )
            if not isinstance(raw_status, dict):
                raise RelayError("scheduler status did not return a JSON object")
            phase = cast(dict[str, object], raw_status).get("phase")
            active_record_found = cast(dict[str, object], raw_status).get("active_record_found")
            if phase == SchedulerPhase.UNKNOWN.value and active_record_found is False:
                return "missing", None
            return (str(phase), None) if isinstance(phase, str) else (None, None)
        status = provider_for_scheduler(provider).poll(scheduler_job_id)
        if status.phase is SchedulerPhase.UNKNOWN and status.active_record_found is False:
            return "missing", None
        return status.phase.value, None
    except (RelayError, json.JSONDecodeError) as exc:
        return None, str(exc)


def _cancel_owned_scheduler_jobs(
    definition: ClusterDefinition,
    jobs: list[_OwnedRelayJob],
    *,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.5,
) -> tuple[list[CleanupResource], list[str]]:
    resources: list[CleanupResource] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for job in jobs:
        for scheduler_job_id in job.scheduler_job_ids:
            identity = (job.scheduler_provider, scheduler_job_id)
            if identity in seen:
                continue
            seen.add(identity)
            resource, error = _cancel_owned_scheduler_job(
                definition,
                scheduler_job_id,
                relay_job_id=job.job_id,
                provider=job.scheduler_provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
            resources.append(resource)
            if error is not None:
                errors.append(error)
    return resources, errors


def _cancel_owned_scheduler_job(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    relay_job_id: str,
    provider: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[CleanupResource, str | None]:
    deadline = monotonic() + timeout_seconds
    accepted = False
    cancel_detail: str | None = None
    try:
        if should_execute_on_cluster(definition):
            with remote_command_timeout(
                min(
                    REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS,
                    max(0.01, deadline - monotonic()),
                )
            ):
                raw_cancel = cast(
                    object,
                    json.loads(
                        run_remote_clio(
                            definition,
                            [
                                "scheduler",
                                "cancel",
                                scheduler_job_id,
                                "--cluster",
                                definition.name,
                                "--provider",
                                provider,
                            ],
                        )
                    ),
                )
            accepted = (
                isinstance(raw_cancel, dict)
                and cast(dict[str, object], raw_cancel).get("accepted") is True
            )
        else:
            result = provider_for_scheduler(provider).cancel(scheduler_job_id)
            accepted = result.returncode == 0
            cancel_detail = result.stderr.strip() or result.stdout.strip() or None
    except (RelayError, json.JSONDecodeError) as exc:
        cancel_detail = str(exc)

    last_phase = "unknown"
    while monotonic() < deadline:
        try:
            if should_execute_on_cluster(definition):
                with remote_command_timeout(
                    min(
                        REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS,
                        max(0.01, deadline - monotonic()),
                    )
                ):
                    raw_status = cast(
                        object,
                        json.loads(
                            run_remote_clio(
                                definition,
                                [
                                    "scheduler",
                                    "status",
                                    scheduler_job_id,
                                    "--cluster",
                                    definition.name,
                                    "--provider",
                                    provider,
                                ],
                            )
                        ),
                    )
                if not isinstance(raw_status, dict):
                    raise RelayError("scheduler status did not return a JSON object")
                phase = cast(dict[str, object], raw_status).get("phase")
                last_phase = str(phase) if phase is not None else "unknown"
            else:
                last_phase = provider_for_scheduler(provider).poll(scheduler_job_id).phase.value
        except (RelayError, json.JSONDecodeError) as exc:
            cancel_detail = str(exc)
        if last_phase == "canceled":
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="canceled",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail="scheduler reported the canceled terminal phase",
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        if last_phase in {"completed", "failed"}:
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="terminal",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail=(
                        "scheduler reached a terminal phase during the cancellation race; "
                        f"cancellation is not claimed: accepted={accepted}, phase={last_phase}"
                        + (f", detail={cancel_detail}" if cancel_detail else "")
                    ),
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        sleep(poll_seconds)

    detail = (
        f"scheduler cancellation was not confirmed: accepted={accepted}, phase={last_phase}"
        + (f", detail={cancel_detail}" if cancel_detail else "")
    )
    return (
        CleanupResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="cancel",
            ownership_verified=True,
            outcome="failed",
            provider=provider,
            residual=True,
            detail=detail,
            metadata={"relay_job_id": relay_job_id},
        ),
        detail,
    )


def _cleanup_owned_runtime_sessions(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    scheduler_sentinel_ids: tuple[str, ...] = (),
    owned_jobs: list[_OwnedRelayJob] | None = None,
) -> list[dict[str, object]]:
    """Clean exact owned gateways and rescan boundedly until admission is stable."""
    queue = storage_managed_queue(RelaySettings.from_env())
    reports: list[dict[str, object]] = []
    if mode == "detach":
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        return _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
    for _pass in range(MAX_OWNER_GATEWAY_CLEANUP_PASSES):
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        if not target_ids:
            return reports
        if owner_session_generation_id is not None and scheduler_sentinel_ids:
            gateway_scheduler_job_ids = _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
            )
            _assert_scheduler_sentinels_unrelated(
                scheduler_sentinel_ids,
                owned_jobs or [],
                gateway_scheduler_job_ids=gateway_scheduler_job_ids,
            )
        pass_reports = _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
        reports.extend(pass_reports)
        if any(
            report.get("ok") is False or bool(report.get("residual_resources"))
            for report in pass_reports
        ):
            return reports
    residual_ids = _owned_runtime_gateway_ids_needing_cleanup(
        queue=queue,
        definition=definition,
        cluster=cluster,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if residual_ids:
        raise RelayError(
            "owned gateway cleanup did not converge after bounded rescans: "
            + ", ".join(sorted(residual_ids))
        )
    return reports


def _owned_runtime_gateway_ids_needing_cleanup(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str | None,
) -> set[str]:
    """Return the current non-closed owned gateway ids from local and remote stores."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway cleanup discovery exceeds the bounded source limit; "
            "no gateway cleanup was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if should_execute_on_cluster(definition):
        documents.extend(
            _complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway cleanup discovery for {cluster}",
            )
        )
    targets: set[str] = set()
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(session_id, str)
            or gateway.get("state") == GatewaySessionState.CLOSED.value
            or not isinstance(metadata, dict)
        ):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
        ):
            continue
        observed_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None and observed_generation not in {
            None,
            owner_session_generation_id,
        }:
            continue
        targets.add(session_id)
    return targets


def _cleanup_owned_runtime_sessions_once(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    target_session_ids: set[str],
) -> list[dict[str, object]]:
    settings = RelaySettings.from_env()
    queue = storage_managed_queue(settings)
    queue.initialize()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=cluster,
        definition=definition,
        token="",
        secret_key="",
    )
    reports: list[dict[str, object]] = []
    seen_session_ids: set[str] = set()
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway cleanup discovery exceeds the bounded source limit "
            f"{MAX_INTERNAL_COLLECTION_RECORDS}; no gateway cleanup was attempted"
        )
    remote_gateways: list[dict[str, Any]] = []
    if should_execute_on_cluster(definition):
        remote_gateways = _complete_remote_source_collection(
            definition,
            ["gateway", "list", "--cluster", cluster],
            record_key="gateway_sessions",
            label=f"remote gateway cleanup discovery for {cluster}",
        )

    for gateway in local_gateways:
        if gateway.session_id not in target_session_ids:
            continue
        if gateway.state == GatewaySessionState.CLOSED and mode == "detach":
            continue
        if gateway.metadata.get("owner") != "clio-relay":
            continue
        if gateway.metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = gateway.metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=gateway.session_id,
                        location=str(settings.core_dir),
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            result = supervisor.detach(session_id=gateway.session_id)
        else:
            result = supervisor.stop(
                session_id=gateway.session_id,
                cancel_scheduler_job=cancel_scheduler_jobs,
            )
        reports.append(result.json_payload())
        seen_session_ids.add(gateway.session_id)
    for gateway in remote_gateways:
        remote_session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(remote_session_id, str)
            or remote_session_id not in target_session_ids
            or remote_session_id in seen_session_ids
        ):
            continue
        if gateway.get("state") == GatewaySessionState.CLOSED.value and mode == "detach":
            continue
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if typed_metadata.get("owner") != "clio-relay":
            continue
        if typed_metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=remote_session_id,
                        location=definition.ssh_host,
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            args = [
                "gateway",
                "detach-runtime",
                remote_session_id,
                "--cluster",
                cluster,
            ]
        else:
            args = [
                "gateway",
                "stop-runtime",
                remote_session_id,
                "--cluster",
                cluster,
                ("--cancel-scheduler-job" if cancel_scheduler_jobs else "--keep-scheduler-job"),
            ]
        remote_report = cast(object, json.loads(run_remote_clio(definition, args)))
        if not isinstance(remote_report, dict):
            raise RelayError(
                f"remote gateway cleanup did not return a JSON object: {remote_session_id}"
            )
        reports.append(
            {str(key): value for key, value in cast(dict[object, object], remote_report).items()}
        )
        seen_session_ids.add(remote_session_id)
    return reports


def _unverified_gateway_generation_report(
    *,
    gateway_session_id: str,
    location: str,
    mode: Literal["detach", "teardown"],
    expected_generation_id: str,
    observed_generation_id: object,
) -> dict[str, object]:
    """Return fail-closed evidence for an owner-session gateway without a generation."""
    detail = (
        "owned gateway record has no exact session generation; cleanup was refused: "
        f"gateway={gateway_session_id} expected={expected_generation_id} "
        f"observed={observed_generation_id!r}"
    )
    resource = CleanupResource(
        kind="gateway_record",
        resource_id=gateway_session_id,
        location=location,
        action="retain" if mode == "detach" else "close",
        ownership_verified=False,
        outcome="refused",
        verified_after_operation=False,
        residual=True,
        detail=detail,
        metadata={
            "expected_owner_session_generation_id": expected_generation_id,
            "observed_owner_session_generation_id": observed_generation_id,
        },
    )
    return {
        "resources": [resource.model_dump(mode="json")],
        "residual_resources": [resource.model_dump(mode="json")],
        "errors": [detail],
        "ok": False,
    }


def _merge_gateway_cleanup_resources(
    report: SessionLifecycleReport,
    gateway_reports: list[dict[str, object]],
) -> None:
    """Merge gateway connector cleanup into the owning desktop-session report."""
    for gateway_report in gateway_reports:
        raw_errors = gateway_report.get("errors")
        if isinstance(raw_errors, list):
            for raw_error in cast(list[object], raw_errors):
                if isinstance(raw_error, str) and raw_error not in report.errors:
                    report.errors.append(raw_error)
        raw_resources = gateway_report.get("resources")
        if not isinstance(raw_resources, list):
            report.errors.append("gateway cleanup report did not contain resource evidence")
            continue
        for raw_resource in cast(list[object], raw_resources):
            resource = CleanupResource.model_validate(raw_resource)
            if any(
                existing.kind == resource.kind
                and existing.resource_id == resource.resource_id
                and existing.action == resource.action
                for existing in report.resources
            ):
                continue
            report.resources.append(resource)


def _verified_owner_session_generation(
    status: dict[str, object],
    *,
    session_id: str,
) -> str:
    """Return the exact durable generation for a session teardown attempt."""
    if status.get("session_id") != session_id or status.get("owner") != "clio-relay":
        raise RelayError("remote session status did not prove the requested owned session")
    generation_id = status.get("session_generation_id")
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("remote session status did not contain an owned generation id")
    _require_durable_session_identity(generation_id, field="session_generation_id")
    if status.get("running") is True and status.get("ownership_verified") is not True:
        raise RelayError("running remote session failed process ownership verification")
    return generation_id


def _require_live_owner_session_for_gateway(
    status: dict[str, object],
    *,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Require a live, owned, exact-generation API before gateway side effects."""
    if not (
        status.get("session_id") == session_id
        and status.get("owner") == "clio-relay"
        and status.get("session_generation_id") == session_generation_id
        and status.get("running") is True
        and status.get("ownership_verified") is True
    ):
        raise RelayError(
            "gateway admission requires a live owned session with the exact generation: "
            f"{session_id}:{session_generation_id}"
        )


def _assert_no_unscoped_desktop_admission_state(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Fail closed when legacy desktop state cannot be attributed to one cluster."""
    legacy = queue.owner_session_generation_status(
        session_id,
        session_generation_id=session_generation_id,
    )
    if (
        legacy.get("active_generation_id") is not None
        or legacy.get("closing_generation_id") is not None
        or legacy.get("closed") is True
    ):
        raise RelayError(
            "legacy unscoped desktop owner-session admission state cannot be safely assigned "
            f"to cluster {cluster!r} for session {session_id!r}; clean or migrate it before "
            "cluster-scoped admission"
        )


def _verified_owner_session_detach(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    expected_session_generation_id: str | None = None,
) -> str:
    """Return the exact generation only when detach retained its owned API."""
    if report.mode != "detach" or report.session_id != session_id:
        raise RelayError("session detach report identity did not match the requested session")
    generation_id = report.session_generation_id
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("session detach did not prove an owned session generation")
    _require_durable_session_identity(generation_id, field="session_generation_id")
    if (
        expected_session_generation_id is not None
        and generation_id != expected_session_generation_id
    ):
        raise RelayError("owned session generation changed during desktop detach")
    if report.errors or report.residual_resources:
        raise RelayError("session detach did not prove remote session retention")
    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session detach must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "retain"
        and api_resource.outcome == "retained"
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session detach did not verify remote relay API retention")
    return generation_id


def _verify_owner_session_teardown(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
) -> None:
    """Reject closure unless all requested owner-session cleanup is verified."""
    if report.mode != "teardown" or report.session_id != session_id:
        raise RelayError("session teardown report identity did not match the requested session")
    if report.session_generation_id != session_generation_id:
        raise RelayError("session teardown report generation did not match the quiesced generation")
    if report.errors:
        raise RelayError("session teardown reported errors: " + "; ".join(report.errors))
    if report.residual_resources:
        residual_ids = sorted(resource.resource_id for resource in report.residual_resources)
        raise RelayError(
            "session teardown left requested residual resources: " + ", ".join(residual_ids)
        )

    prior_status = report.prior_session_status
    post_status = report.post_session_status
    if (
        prior_status is None
        or prior_status.session_generation_id != session_generation_id
        or not prior_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove prior generation ownership")
    if (
        post_status is None
        or post_status.session_generation_id != session_generation_id
        or post_status.running
        or not post_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove the owned API generation stopped")

    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session teardown must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "stop"
        and api_resource.outcome in {"stopped", "missing"}
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session teardown did not verify remote relay API cleanup")

    gateway_resources = [
        resource for resource in report.resources if resource.kind == "gateway_record"
    ]
    relay_resource_ids = {
        resource.resource_id for resource in report.resources if resource.kind == "relay_job"
    }
    gateway_resource_ids = {resource.resource_id for resource in gateway_resources}
    connector_resources = [
        resource
        for resource in report.resources
        if resource.kind in {"desktop_connector", "remote_connector"}
    ]
    if (gateway_resources or connector_resources) and not cleanup_connectors_cover_gateways(
        connector_resources,
        gateway_resources,
        mode="teardown",
    ):
        raise RelayError(
            "session teardown connector evidence did not cover each owned gateway exactly"
        )

    for resource in report.resources:
        if resource.kind in {"desktop_connector", "remote_connector"} and not (
            resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify connector cleanup: {resource.resource_id}"
            )
        if resource.kind == "gateway_record" and not (
            resource.action == "close"
            and resource.outcome == "closed"
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify gateway closure: {resource.resource_id}"
            )
        if resource.kind == "scheduler_job":
            linked_relay_id = resource.metadata.get("relay_job_id")
            linked_gateway_id = resource.metadata.get("gateway_session_id")
            linked = (
                isinstance(linked_relay_id, str) and linked_relay_id in relay_resource_ids
            ) or (isinstance(linked_gateway_id, str) and linked_gateway_id in gateway_resource_ids)
            retained = (
                resource.action == "retain"
                and resource.outcome in {"retained", "terminal", "missing"}
                and resource.observed_state
                in {
                    "submitted",
                    "pending",
                    "allocated",
                    "running",
                    "completed",
                    "failed",
                    "canceled",
                    "missing",
                }
            )
            canceled = resource.action == "cancel" and (
                (resource.outcome == "canceled" and resource.observed_state == "canceled")
                or (
                    resource.outcome == "terminal"
                    and resource.observed_state in {"completed", "failed"}
                )
            )
            if not (
                linked
                and resource.provider is not None
                and (retained or canceled)
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            ):
                raise RelayError(
                    f"session teardown did not verify scheduler disposition: {resource.resource_id}"
                )

    worker_resources = [
        resource for resource in report.resources if resource.kind == "worker_service"
    ]
    if stop_worker:
        if len(worker_resources) != 1:
            raise RelayError("session teardown must contain exactly one worker service result")
        worker = worker_resources[0]
        if not (
            worker.action == "stop"
            and worker.outcome in {"stopped", "missing"}
            and worker.ownership_verified
            and worker.verified_after_operation
            and worker.observed_state in {"inactive", "not-found"}
            and not worker.residual
        ):
            raise RelayError("session teardown did not verify worker service inactivity")
    elif worker_resources:
        raise RelayError("session teardown reported worker cleanup when it was not requested")


def _mark_owner_session_closed(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    legacy_unversioned_job_ids: list[str],
) -> None:
    """Close the authoritative generation, then its cluster-scoped desktop mirror."""
    if remote_execution:
        args = [
            "session",
            "mark-closed",
            "--session-id",
            session_id,
            "--session-generation-id",
            session_generation_id,
        ]
        for job_id in legacy_unversioned_job_ids:
            args.extend(["--legacy-unversioned-job-id", job_id])
        raw_payload = cast(
            object,
            json.loads(run_remote_clio(definition, args)),
        )
        if not isinstance(raw_payload, dict):
            raise RelayError("remote owner-session closure did not return a JSON object")
        payload = cast(dict[str, object], raw_payload)
    else:
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
        )
        payload = closure.model_dump(mode="json")
        if legacy_unversioned_job_ids:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
    if (
        payload.get("owner_session_id") != session_id
        or payload.get("session_generation_id") != session_generation_id
        or payload.get("residual_resource_ids") != []
    ):
        raise RelayError("owner-session closure did not match the verified teardown generation")
    if legacy_unversioned_job_ids:
        raw_legacy_closure = payload.get("legacy_closure")
        if not isinstance(raw_legacy_closure, dict):
            raise RelayError("owner-session closure omitted legacy job coverage")
        legacy_closure = cast(dict[str, object], raw_legacy_closure)
        if (
            legacy_closure.get("session_generation_id") is not None
            or legacy_closure.get("covered_by_session_generation_id") != session_generation_id
            or legacy_closure.get("covered_legacy_job_ids")
            != sorted(set(legacy_unversioned_job_ids))
        ):
            raise RelayError("owner-session legacy coverage did not match verified job ids")
    local_closure = queue.set_owner_session_closed(
        local_admission_session_id,
        session_generation_id=session_generation_id,
        residual_resource_ids=[],
    )
    if (
        local_closure.owner_session_id != local_admission_session_id
        or local_closure.session_generation_id != session_generation_id
        or local_closure.residual_resource_ids
    ):
        raise RelayError("desktop owner-session admission mirror did not close exactly")


def _remote_mcp_cache_status(
    registration: RemoteMcpServerConfig,
    entry: RemoteMcpSchemaCacheEntry | None,
) -> dict[str, object]:
    if entry is None:
        return {"state": "missing", "fresh": False}
    execution_matches = entry.execution_fingerprint == remote_mcp_execution_fingerprint(
        registration
    )
    fresh = entry.is_fresh()
    if fresh and execution_matches:
        state = "fresh"
    elif not execution_matches:
        state = "command_changed"
    else:
        state = "stale"
    return {
        "state": state,
        "fresh": fresh,
        "execution_matches": execution_matches,
        "discovered_at": entry.discovered_at.isoformat(),
        "expires_at": entry.expires_at.isoformat(),
        "schema_digest": entry.schema_digest,
        "tool_names": sorted(tool.name for tool in entry.tools),
        "provenance": entry.provenance.model_dump(mode="json"),
    }


def _require_discovery_success(result: dict[str, object], job_id: str) -> None:
    state = result.get("state")
    if state != JobState.SUCCEEDED.value:
        error = result.get("error")
        detail = f": {error}" if isinstance(error, str) and error else ""
        raise RelayError(f"remote MCP discovery job {job_id} ended in state {state}{detail}")


def _run_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> tuple[str, dict[str, Any], list[dict[str, Any]], bytes]:
    """Discover the actual cluster-side JARVIS MCP before accepting its virtual route."""
    idempotency_key = f"mcp:jarvis-contract:{cluster}:{uuid4().hex}"
    if should_execute_on_cluster(definition):
        remote_args = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            "tools/list",
            "--idempotency-key",
            idempotency_key,
        ]
        job_id = _last_nonempty_line(run_remote_clio(definition, remote_args))
        terminal = _json_output(
            run_remote_clio(
                definition,
                [
                    "job",
                    "wait",
                    job_id,
                    "--timeout-seconds",
                    str(wait_timeout_seconds),
                    "--poll-seconds",
                    str(poll_seconds),
                ],
            ),
            "JARVIS MCP discovery wait",
        )
        _require_discovery_success(terminal, job_id)
        artifacts = _remote_artifact_records(definition, job_id)
        artifact_payload = _read_remote_artifact_kind_bytes(
            definition,
            artifacts,
            kind="mcp_result",
        )
    else:
        server = jarvis_mcp_server()
        server_args = jarvis_mcp_server_args()
        submitted = queue.submit_job(
            RelayJob(
                cluster=cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=server,
                    server_args=server_args,
                    env_from=jarvis_mcp_env_from(),
                    operation=McpOperation.TOOLS_LIST,
                    timeout_seconds=max(1, int(wait_timeout_seconds)),
                ),
                idempotency_key=idempotency_key,
            )
        )
        job_id = submitted.job_id
        terminal_job = wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_discovery_success(terminal_job.model_dump(mode="json"), job_id)
        artifacts = _complete_local_artifact_records(queue, job_id)
        artifact_payload = _read_local_artifact_kind_bytes(
            queue,
            artifacts,
            kind="mcp_result",
        )
    if artifact_payload is None:
        raise RelayError("JARVIS MCP discovery did not produce an mcp_result artifact")
    result = _decode_json_artifact(artifact_payload, kind="mcp_result")
    return job_id, result, artifacts, artifact_payload


def _persist_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    discovery_job_id: str,
    result: dict[str, Any],
    artifacts: list[dict[str, Any]],
    artifact_payload: bytes,
) -> tuple[RemoteMcpSchemaCacheEntry, str]:
    """Persist and verify the exact discovery identity used by built-in JARVIS calls."""
    server = result.get("server")
    raw_server_args = result.get("server_args")
    raw_env_from = result.get("env_from", {})
    if not isinstance(server, str) or not server:
        raise RelayError("JARVIS MCP discovery result has no server command")
    if not isinstance(raw_server_args, list) or not all(
        isinstance(item, str) for item in cast(list[object], raw_server_args)
    ):
        raise RelayError("JARVIS MCP discovery result has invalid server arguments")
    if not isinstance(raw_env_from, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in cast(dict[object, object], raw_env_from).items()
    ):
        raise RelayError("JARVIS MCP discovery result has invalid environment references")
    artifact = _artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError("JARVIS MCP discovery has no durable result artifact")
    artifact_id = artifact.get("artifact_id")
    artifact_sha256 = artifact.get("sha256")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("JARVIS MCP discovery result artifact has no artifact_id")
    if artifact_sha256 is not None and not isinstance(artifact_sha256, str):
        raise RelayError("JARVIS MCP discovery result artifact has invalid SHA-256")
    registration = RemoteMcpServerConfig(
        command=server,
        args=cast(list[str], raw_server_args),
        env_from=cast(dict[str, str], raw_env_from),
        allow_tools=[
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_add_step",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        ],
        profiles=["user"],
    )
    entry = cache_entry_from_discovery_artifact(
        cluster=cluster,
        server_name=JARVIS_MCP_CACHE_SERVER_NAME,
        registration=registration,
        discovery_job_id=discovery_job_id,
        artifact_id=artifact_id,
        artifact_sha256=artifact_sha256,
        artifact_payload=artifact_payload,
    )
    if entry.schema_digest != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RelayError(
            f"JARVIS MCP discovery contract does not match clio-kit {CLIO_KIT_JARVIS_MCP_VERSION}"
        )
    try:
        binding = jarvis_mcp_artifact_binding_from_entry(entry)
    except ValueError as exc:
        raise RelayError(str(exc)) from exc
    cache_path = default_remote_mcp_cache_path(registry_path=default_registry_path())
    RemoteMcpSchemaCache.update_entry(cache_path, entry)
    return entry, binding


def _read_remote_mcp_result_artifact(
    definition: ClusterDefinition,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    artifacts = _remote_artifact_records(definition, job_id)
    artifact = _artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("remote MCP result artifact has no artifact_id")
    envelope = _json_output(
        run_remote_clio(definition, ["job", "read-artifact", artifact_id]),
        "remote discovery artifact payload",
    )
    return artifact, _decode_artifact_envelope(envelope)


def _remote_artifact_records(
    definition: ClusterDefinition,
    job_id: str,
) -> list[dict[str, Any]]:
    return _complete_remote_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"remote artifacts for {job_id}",
    )


def _artifact_record(
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    return next((artifact for artifact in artifacts if artifact.get("kind") == kind), None)


def _read_remote_json_artifact_kind(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    payload = _read_remote_artifact_kind_bytes(definition, artifacts, kind=kind)
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_remote_artifact_kind_bytes(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> bytes | None:
    """Read the exact remote artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"remote {kind} artifact has no artifact_id")
    envelope = _json_output(
        run_remote_clio(definition, ["job", "read-artifact", artifact_id]),
        f"remote {kind} artifact payload",
    )
    return _decode_artifact_envelope(envelope)


def _read_local_json_artifact_kind(
    queue: ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    payload = _read_local_artifact_kind_bytes(queue, artifacts, kind=kind)
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_local_artifact_kind_bytes(
    queue: ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> bytes | None:
    """Read the exact local artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"local {kind} artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return _decode_artifact_envelope(envelope)


def _decode_json_artifact(payload: bytes, *, kind: str) -> dict[str, Any]:
    try:
        decoded = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, JSONDecodeError) as exc:
        raise RelayError(f"{kind} artifact must contain UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise RelayError(f"{kind} artifact must contain a JSON object")
    typed = cast(dict[object, object], decoded)
    return {str(key): value for key, value in typed.items()}


def _mcp_response_job_id(response: dict[str, Any] | None) -> str:
    if response is None:
        raise RelayError("virtual remote MCP call returned no JSON-RPC response")
    error = response.get("error")
    if isinstance(error, dict):
        typed_error = cast(dict[object, object], error)
        raise RelayError(f"virtual remote MCP call failed: {typed_error.get('message')}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RelayError("virtual remote MCP call returned no result object")
    structured = cast(dict[object, object], result).get("structuredContent")
    if not isinstance(structured, dict):
        raise RelayError("virtual remote MCP call returned no structuredContent")
    job_id = cast(dict[object, object], structured).get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise RelayError("virtual remote MCP call returned no durable job_id")
    return job_id


def _read_local_mcp_result_artifact(
    queue: ClioCoreQueue,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    artifact = next(
        (
            item
            for item in _complete_local_artifact_records(queue, job_id)
            if item.get("kind") == "mcp_result"
        ),
        None,
    )
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("local MCP result artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return cast(dict[str, object], artifact), _decode_artifact_envelope(envelope)


def _decode_artifact_envelope(envelope: dict[str, object]) -> bytes:
    if envelope.get("encoding") != "base64":
        raise RelayError("remote MCP result artifact must use base64 encoding")
    encoded = envelope.get("data")
    if not isinstance(encoded, str):
        raise RelayError("remote MCP result artifact data must be a base64 string")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RelayError("remote MCP result artifact contains invalid base64") from exc


@dataclass(frozen=True)
class _JarvisPackageSearchAcceptance:
    """Durable evidence from one bounded JARVIS package-discovery query."""

    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]


def _run_jarvis_package_search_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    profile: str,
    query: str,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> _JarvisPackageSearchAcceptance:
    """Exercise bounded package discovery through the local virtual MCP surface."""
    session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_describe",
        arguments={
            "cluster": cluster,
            "target": "package_search",
            "query": query,
            "page_size": 5,
        },
    )
    call_job_id = _mcp_response_job_id(session.tools_call_response)
    if should_execute_on_cluster(definition):
        call_status = _wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _remote_artifact_records(definition, call_job_id)
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        call_status = _wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _complete_local_artifact_records(queue, call_job_id)
        mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
        provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
    return _JarvisPackageSearchAcceptance(
        tools_list_response=session.tools_list_response,
        call_response=session.tools_call_response,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        initialize_response=session.initialize_response,
        stdio_evidence=session.evidence(),
    )


@dataclass(frozen=True)
class _JarvisExecutionQueryAcceptance:
    """Durable evidence from one post-run unified JARVIS execution query."""

    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]


def _run_post_run_jarvis_execution_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    profile: str,
    pipeline_id: str,
    execution_id: str,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> _JarvisExecutionQueryAcceptance:
    """Query the completed JARVIS execution through the local virtual MCP surface."""
    arguments: dict[str, Any] = {
        "cluster": cluster,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "include_progress": True,
        "artifacts": {"page_size": 25},
    }
    session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_get_execution",
        arguments=arguments,
    )
    call_job_id = _mcp_response_job_id(session.tools_call_response)
    if should_execute_on_cluster(definition):
        call_status = _wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _remote_artifact_records(definition, call_job_id)
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        call_status = _wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _complete_local_artifact_records(queue, call_job_id)
        mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
        provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
    return _JarvisExecutionQueryAcceptance(
        tools_list_response=session.tools_list_response,
        call_response=session.tools_call_response,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        initialize_response=session.initialize_response,
        stdio_evidence=session.evidence(),
    )


def _wait_for_remote_job_terminal(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    """Wait for one remote relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = monotonic() + timeout_seconds
    while True:
        status = _json_output(
            run_remote_clio(definition, ["job", "status", job_id]),
            "JARVIS MCP execution-query job status",
        )
        if status.get("terminal") is True:
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"job did not reach terminal state before timeout: {job_id}")
        sleep(min(poll_seconds, remaining))


def _wait_for_local_job_terminal(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    """Wait for one local relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = monotonic() + timeout_seconds
    while True:
        status = get_job_status(queue, job_id)
        if status.get("terminal") is True:
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"job did not reach terminal state before timeout: {job_id}")
        sleep(min(poll_seconds, remaining))


def _wait_for_remote_jarvis_mcp_progress(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[dict[str, object], list[dict[str, Any]], dict[str, Any] | None]:
    """Wait remotely while proving progress was observable before job completion."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = monotonic() + timeout_seconds
    live_observation: dict[str, Any] | None = None
    while True:
        progress = _complete_remote_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"JARVIS MCP validation progress for {job_id}",
        )
        status = _json_output(
            run_remote_clio(definition, ["job", "status", job_id]),
            "JARVIS MCP validation job status",
        )
        if live_observation is None:
            live_observation = _live_jarvis_progress_observation(progress, status)
        if status.get("terminal") is True:
            return status, progress, live_observation
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"job did not reach terminal state before timeout: {job_id}")
        sleep(min(poll_seconds, remaining))


def _wait_for_local_jarvis_mcp_progress(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[dict[str, object], list[dict[str, Any]], dict[str, Any] | None]:
    """Wait locally while proving progress was observable before job completion."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = monotonic() + timeout_seconds
    live_observation: dict[str, Any] | None = None
    while True:
        progress = _complete_local_progress_records(queue, job_id)
        status = get_job_status(queue, job_id)
        if live_observation is None:
            live_observation = _live_jarvis_progress_observation(progress, status)
        if status.get("terminal") is True:
            return status, progress, live_observation
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"job did not reach terminal state before timeout: {job_id}")
        sleep(min(poll_seconds, remaining))


def _validate_progress_wait(*, timeout_seconds: float, poll_seconds: float) -> None:
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if poll_seconds <= 0:
        raise ConfigurationError("poll_seconds must be positive")


def _live_jarvis_progress_observation(
    progress: list[dict[str, Any]],
    status: dict[str, object],
) -> dict[str, Any] | None:
    """Capture one native JARVIS progress record while the relay job is running."""
    job = status.get("job")
    if not isinstance(job, dict):
        return None
    typed_job = {str(key): value for key, value in cast(dict[object, object], job).items()}
    if typed_job.get("state") != JobState.RUNNING.value or status.get("terminal") is not False:
        return None
    for record in progress:
        metadata = record.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, Any], metadata)
        progress_id = record.get("progress_id")
        if (
            isinstance(progress_id, str)
            and typed_metadata.get("source") == "jarvis_execution"
            and typed_metadata.get("provider_source_authority")
            == "jarvis_mcp_progress_notification"
            and typed_metadata.get("producer_validated") is True
            and typed_metadata.get("execution_binding_validated") is False
            and isinstance(typed_metadata.get("progress_transport_sequence"), int)
            and not isinstance(typed_metadata.get("progress_transport_sequence"), bool)
            and cast(int, typed_metadata["progress_transport_sequence"]) > 0
        ):
            return {
                "progress_id": progress_id,
                "job_state": typed_job.get("state"),
                "job_updated_at": typed_job.get("updated_at"),
                "terminal": False,
                "progress_transport_sequence": typed_metadata.get("progress_transport_sequence"),
            }
    return None


def _json_value(value: str, label: str) -> object:
    try:
        return cast(object, json.loads(value))
    except JSONDecodeError as exc:
        raise RelayError(f"{label} did not return valid JSON: {exc.msg}") from exc


def _json_output(value: str, label: str) -> dict[str, object]:
    decoded = _json_value(value, label)
    if not isinstance(decoded, dict):
        raise RelayError(f"{label} did not return a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], decoded).items()}


def _complete_local_artifact_records(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded artifact snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"artifacts for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"artifacts for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_local_progress_records(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded progress snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"progress for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"progress for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Drain a remote paged CLI collection under an explicit completeness cap."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            run_remote_clio(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        page: list[dict[str, Any]] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            page.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"{label} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_source_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_source_positions: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Drain filtered global source windows while bounding every durable position."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            run_remote_clio(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            records.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("source_total")
        returned_cursor = payload.get("source_cursor")
        returned_limit = payload.get("source_limit")
        next_cursor = payload.get("source_next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if total > max_source_positions:
            raise RelayError(f"{label} exceeds the bounded source limit {max_source_positions}")
        if expected_total is not None and total != expected_total:
            raise RelayError(f"{label} changed during bounded discovery")
        expected_total = total
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is None:
            return records
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
            or next_cursor > total
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        cursor = next_cursor


def _validate_complete_page(
    *,
    label: str,
    cursor: int,
    page_count: int,
    next_cursor: int | None,
    total: int,
    expected_total: int | None,
    collected_count: int,
    max_records: int,
) -> int:
    """Validate a page chain before it can be treated as complete evidence."""
    if max_records < 1:
        raise ValueError("max_records must be positive")
    if total > max_records:
        raise RelayError(f"{label} exceeds the bounded completeness limit {max_records}")
    if expected_total is not None and total != expected_total:
        raise RelayError(f"{label} changed during bounded discovery")
    if collected_count + page_count > total:
        raise RelayError(f"{label} returned more records than its total")
    expected_next = cursor + page_count
    if next_cursor is not None and (
        page_count == 0 or next_cursor != expected_next or next_cursor > total
    ):
        raise RelayError(f"{label} returned a non-contiguous page cursor")
    if next_cursor is None and collected_count + page_count != total:
        raise RelayError(f"{label} ended before its declared total")
    return total


def _remote_worker_info(
    definition: ClusterDefinition,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, object]:
    """Read fresh process-bound worker identity over one optional total deadline."""
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    deadline = None if timeout_seconds is None else monotonic() + timeout_seconds
    info = _json_output(
        _run_remote_clio_before_deadline(
            definition,
            ["endpoint", "worker-info", "--cluster", definition.name],
            deadline=deadline,
        ),
        "remote clio-relay worker runtime info",
    )
    actual_provider = info.get("scheduler_provider")
    if actual_provider != definition.scheduler_provider:
        raise ConfigurationError(
            "remote worker scheduler provider does not match the cluster definition: "
            f"{actual_provider!r} != {definition.scheduler_provider!r}"
        )
    info["target_identity"] = _remote_target_identity(definition, deadline=deadline)
    return info


def _run_remote_clio_before_deadline(
    definition: ClusterDefinition,
    args: list[str],
    *,
    deadline: float | None,
) -> str:
    """Run one remote observation without exceeding a shared monotonic deadline."""
    if deadline is None:
        return run_remote_clio(definition, args)
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise RelayError("remote worker identity observation timed out")
    with remote_command_timeout(remaining):
        return run_remote_clio(definition, args)


def _remote_target_identity(
    definition: ClusterDefinition,
    *,
    deadline: float | None = None,
) -> dict[str, object]:
    """Verify and return one operator-pinned physical cluster identity."""
    target = definition.target_identity
    if target is None:
        raise ConfigurationError(
            f"cluster {definition.name} has no operator-pinned target_identity"
        )
    remote_target = _json_output(
        _run_remote_clio_before_deadline(
            definition,
            [
                "endpoint",
                "target-info",
                "--scheduler-provider",
                definition.scheduler_provider,
            ],
            deadline=deadline,
        ),
        "remote physical cluster target info",
    )
    if remote_target.get("schema_version") != "clio-relay.cluster-target-info.v1":
        raise ConfigurationError("remote physical target identity schema does not match")
    if remote_target.get("scheduler_provider") != definition.scheduler_provider:
        raise ConfigurationError(
            "remote physical target scheduler provider does not match the cluster definition"
        )
    observed_hostnames = {
        value
        for key in ("hostname", "fqdn")
        if isinstance((value := remote_target.get(key)), str) and value
    }
    if not observed_hostnames.intersection(target.hostnames):
        raise ConfigurationError(
            "remote hostname does not match the operator-pinned cluster identity: "
            f"observed={sorted(observed_hostnames)!r} expected={target.hostnames!r}"
        )
    if (
        target.site_marker_sha256 is not None
        and remote_target.get("site_marker_sha256") != target.site_marker_sha256
    ):
        raise ConfigurationError("remote site marker does not match cluster target identity")
    if (
        target.scheduler_cluster_name is not None
        and remote_target.get("scheduler_cluster_name") != target.scheduler_cluster_name
    ):
        raise ConfigurationError("scheduler-native cluster name does not match target identity")
    fingerprints = (
        _ssh_host_key_fingerprints(definition.ssh_host)
        if deadline is None
        else _ssh_host_key_fingerprints(definition.ssh_host, deadline=deadline)
    )
    if not set(fingerprints).intersection(target.ssh_host_key_sha256):
        raise ConfigurationError(
            "live SSH host keys do not match the operator-pinned cluster target identity"
        )
    return {
        **remote_target,
        "ssh_host": definition.ssh_host,
        "ssh_host_key_sha256": fingerprints,
        "expected_hostnames": target.hostnames,
        "expected_ssh_host_key_sha256": target.ssh_host_key_sha256,
        "expected_scheduler_cluster_name": target.scheduler_cluster_name,
        "expected_site_marker_sha256": target.site_marker_sha256,
        "verified": True,
    }


def _ssh_host_key_fingerprints(
    ssh_host: str,
    *,
    deadline: float | None = None,
) -> list[str]:
    """Return trusted SHA-256 host-key fingerprints for a configured SSH target."""
    resolved_host = ssh_host
    resolved_port = "22"
    host_key_alias: str | None = None
    known_hosts_files: list[str] = []
    diagnostics: list[str] = []
    try:
        config = subprocess.run(
            ["ssh", "-G", ssh_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh -G timed out")
    except OSError as exc:
        diagnostics.append(f"ssh -G failed: {exc}")
    else:
        if config.returncode != 0:
            diagnostics.append(config.stderr.strip() or f"ssh -G exited {config.returncode}")
        else:
            for line in config.stdout.splitlines():
                fields = line.split(maxsplit=1)
                if len(fields) != 2:
                    continue
                key, value = fields[0].casefold(), fields[1].strip()
                if key == "hostname" and value:
                    resolved_host = value
                elif key == "port" and value:
                    resolved_port = value
                elif key == "hostkeyalias" and value:
                    host_key_alias = value
                elif key == "userknownhostsfile" and value:
                    known_hosts_files.extend(_split_ssh_config_values(value))

    lookup_host = host_key_alias or resolved_host
    if resolved_port != "22":
        lookup_host = f"[{lookup_host}]:{resolved_port}"
    fingerprints: set[str] = set()
    for configured_path in known_hosts_files:
        if configured_path.casefold() == "none":
            continue
        known_hosts_path = Path(os.path.expandvars(os.path.expanduser(configured_path)))
        try:
            found = subprocess.run(
                ["ssh-keygen", "-F", lookup_host, "-f", str(known_hosts_path)],
                capture_output=True,
                text=True,
                check=False,
                timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
            )
        except subprocess.TimeoutExpired:
            diagnostics.append(f"ssh-keygen timed out for {known_hosts_path}")
            continue
        except OSError as exc:
            diagnostics.append(f"ssh-keygen failed for {known_hosts_path}: {exc}")
            break
        fingerprints.update(_ssh_fingerprints_from_key_lines(found.stdout))
    if fingerprints:
        return sorted(fingerprints)

    try:
        scanned = subprocess.run(
            ["ssh-keyscan", "-T", "10", "-p", resolved_port, resolved_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(15, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh-keyscan timed out")
        scanned = None
    except OSError as exc:
        diagnostics.append(f"ssh-keyscan failed: {exc}")
        scanned = None
    if scanned is not None:
        fingerprints.update(_ssh_fingerprints_from_key_lines(scanned.stdout))
        if scanned.returncode != 0:
            diagnostics.append(scanned.stderr.strip() or f"ssh-keyscan exited {scanned.returncode}")
    if not fingerprints:
        detail = "; ".join(item for item in diagnostics if item) or "no host keys returned"
        raise ConfigurationError(f"could not observe SSH host keys for {ssh_host}: {detail}")
    return sorted(fingerprints)


def _remote_observation_subprocess_timeout(
    default_seconds: float,
    *,
    deadline: float | None,
) -> float:
    """Return a positive subprocess timeout inside one shared observation budget."""
    if deadline is None:
        return default_seconds
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ConfigurationError("remote worker identity observation timed out")
    return min(default_seconds, remaining)


def _split_ssh_config_values(value: str) -> list[str]:
    """Split an ``ssh -G`` multi-value while preserving Windows path separators."""
    values: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            if character == quote:
                quote = None
            elif character == "\\" and index + 1 < len(value) and value[index + 1] == quote:
                index += 1
                current.append(value[index])
            else:
                current.append(character)
        elif character in {'"', "'"}:
            quote = character
        elif (
            character == "\\"
            and index + 1 < len(value)
            and (value[index + 1].isspace() or value[index + 1] in {'"', "'"})
        ):
            index += 1
            current.append(value[index])
        elif character.isspace():
            if current:
                values.append("".join(current))
                current = []
        else:
            current.append(character)
        index += 1
    if current:
        values.append("".join(current))
    return values


def _ssh_fingerprints_from_key_lines(output: str) -> set[str]:
    """Decode public-key records emitted by ``ssh-keygen`` or ``ssh-keyscan``."""
    fingerprints: set[str] = set()
    for line in output.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        marker_offset = 1 if fields and fields[0].startswith("@") else 0
        if marker_offset and fields[0].casefold() == "@revoked":
            continue
        if len(fields) < marker_offset + 3:
            continue
        try:
            key_bytes = base64.b64decode(fields[marker_offset + 2], validate=True)
        except (binascii.Error, ValueError):
            continue
        digest = base64.b64encode(hashlib.sha256(key_bytes).digest()).decode().rstrip("=")
        fingerprints.add(f"SHA256:{digest}")
    return fingerprints


def _last_nonempty_line(value: str) -> str:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        raise RelayError("remote MCP discovery submission did not return a job id")
    return lines[-1]


def _run_transport_validation(
    *,
    cluster: str,
    transport_mode: str,
    resource_id: str,
    resource_role: str,
    retain_remote_session: bool,
    validation_report: Path | None,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
    probe: Callable[[], list[str]],
) -> list[str]:
    """Run one transport probe and persist canonical success or failure evidence."""
    report_path = validation_report or default_report_path(cluster)
    connector = ValidationResource(
        kind="connector",
        resource_id=resource_id,
        role=resource_role,
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": transport_mode},
    )
    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[transport_mode],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=("transport_probe_detach" if retain_remote_session else "transport_probe_teardown"),
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    try:
        lines = probe()
    except BaseException as exc:
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": False,
                },
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.actions.append(
            {
                "kind": "transport_probe",
                "resource_id": resource_id,
                "action": "detach" if retain_remote_session else "teardown",
                "outcome": "failed",
            }
        )
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.completed", "complete transport probe", exc)
        recorder.finish(exc)
        recorder.write(report_path)
        raise

    for line in lines:
        recorder.observe_line(line)
    expected_cleanup_line = (
        "transport.cleanup=detached" if retain_remote_session else "transport.cleanup=passed"
    )
    cleanup_verified = expected_cleanup_line in lines and (
        not retain_remote_session or "transport.remote_session=retained" in lines
    )
    if not cleanup_verified:
        expected = (
            "verified active remote-session retention"
            if retain_remote_session
            else "verified transport teardown"
        )
        error = RelayError(f"transport probe returned without {expected} evidence")
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {**connector.metadata, "cleanup_verified": False},
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.cleanup", "verify transport cleanup", error)
        recorder.finish(error)
        recorder.write(report_path)
        raise error
    recorder.add_resource(
        connector.model_copy(
            update={
                "state": "stopped",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": True,
                    "remote_session_retained": retain_remote_session,
                },
            }
        )
    )
    action_outcome = "detached" if retain_remote_session else "stopped"
    recorder.report.cleanup.actions.append(
        {
            "kind": "transport_probe",
            "resource_id": resource_id,
            "action": "detach" if retain_remote_session else "teardown",
            "outcome": action_outcome,
        }
    )
    if retain_remote_session:
        retained_session = ValidationResource(
            kind="relay_session",
            resource_id=resource_id,
            role="transport_probe",
            cluster=cluster,
            state="retained",
            metadata={
                "ownership": "clio-relay",
                "ownership_verified": True,
                "verified_after_operation": True,
            },
        )
        recorder.add_resource(retained_session)
        recorder.report.cleanup.actions.append(
            {
                "kind": "relay_session",
                "resource_id": resource_id,
                "action": "retain",
                "outcome": "retained",
                "ownership_verified": True,
                "verified_after_operation": True,
            }
        )
    try:
        _attach_verified_remote_worker(recorder.report, _require_cluster(cluster))
    except BaseException as exc:
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(report_path)
        raise
    recorder.finish()
    recorder.write(report_path)
    lines.append(f"validation.report={report_path.resolve()}")
    return lines


def _run_frpc_connection_validation(
    *,
    cluster: str,
    proxy_name: str,
    frpc_bin: str,
    config: FrpcConfig,
    timeout_seconds: float,
    validation_report: Path,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
) -> list[str]:
    """Run the bounded frpc process probe and persist canonical evidence."""
    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[config.transport_protocol.value],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode="frpc_connection_probe",
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    connector = ValidationResource(
        kind="connector",
        resource_id=proxy_name,
        role="frpc_connection_probe",
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": config.transport_protocol.value},
    )

    def record_stopped_connector() -> None:
        recorder.add_resource(
            connector.model_copy(
                update={
                    "state": "stopped",
                    "metadata": {**connector.metadata, "cleanup_verified": True},
                }
            )
        )
        if not any(
            action.get("kind") == "connector" and action.get("resource_id") == proxy_name
            for action in recorder.report.cleanup.actions
        ):
            recorder.report.cleanup.actions.append(
                {
                    "kind": "connector",
                    "resource_id": proxy_name,
                    "action": "stop",
                    "outcome": "stopped",
                    "ownership_verified": True,
                    "verified_after_operation": True,
                }
            )

    try:
        with recorder.check(
            "transport.frpc-connection",
            "frpc stayed connected for the bounded probe interval",
        ) as evidence:
            lines = run_frpc_connection_check(
                frpc_bin=frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
            )
            output = "\n".join(lines)
            evidence.append(
                EvidenceReference(
                    kind="frpc_probe",
                    excerpt=lines[0] if lines else "frpc connection probe completed",
                    metadata={
                        "line_count": len(lines),
                        "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
                        "timeout_seconds": timeout_seconds,
                    },
                )
            )
        record_stopped_connector()
        _attach_verified_remote_worker(recorder.report, _require_cluster(cluster))
    except BaseException as exc:
        if not recorder.report.checks:
            recorder.record_failure(
                "transport.frpc-connection",
                "frpc stayed connected for the bounded probe interval",
                exc,
            )
        elif all(check.status is ValidationStatus.PASSED for check in recorder.report.checks):
            recorder.record_failure(
                "worker.installation-info",
                "verify remote worker installation identity",
                exc,
            )
        record_stopped_connector()
        recorder.finish(exc)
        recorder.write(validation_report)
        raise
    recorder.finish()
    recorder.write(validation_report)
    lines.append(f"validation.report={validation_report.resolve()}")
    return lines


def _attach_verified_remote_worker(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    *,
    observed_worker_info: dict[str, object] | None = None,
) -> None:
    """Attach exact remote installation identity when the target executes over SSH."""
    if not should_execute_on_cluster(definition):
        return
    remote_info = (
        observed_worker_info
        if observed_worker_info is not None
        else _remote_worker_info(definition)
    )
    attach_verified_worker_identity(report, remote_info)


def _observe_worker_before_cleanup(
    definition: ClusterDefinition,
) -> tuple[dict[str, object] | None, Exception | None]:
    """Capture bounded worker evidence before cleanup can stop remote services."""
    if not should_execute_on_cluster(definition):
        return None, None
    try:
        return (
            _remote_worker_info(
                definition,
                timeout_seconds=REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS,
            ),
            None,
        )
    except Exception as exc:
        return None, exc


def _write_remote_verified_report(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    path: Path,
    *,
    observed_worker_info: dict[str, object] | None = None,
    worker_observation_error: Exception | None = None,
) -> None:
    """Persist a report only after recording remote installation verification."""
    if observed_worker_info is not None and worker_observation_error is not None:
        raise ValueError("worker observation cannot contain both info and an error")
    try:
        if worker_observation_error is not None:
            raise worker_observation_error
        _attach_verified_remote_worker(
            report,
            definition,
            observed_worker_info=observed_worker_info,
        )
        if observed_worker_info is not None:
            for resource in report.resources:
                if (
                    resource.kind == "relay_worker"
                    and resource.resource_id == f"worker:{definition.name}"
                ):
                    resource.metadata["observation_phase"] = "before_cleanup"
    except BaseException as exc:
        recorder = ValidationRecorder(report)
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(path)
        raise
    write_validation_report(report, path)


def _new_cleanup_acceptance_report(
    *,
    scenario: str,
    cluster: str,
    mode: str,
    resource_kind: str,
    resource_id: str,
    action: str,
    cancel_relay_jobs: bool,
    cancel_scheduler_jobs: bool,
    stop_worker: bool,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
) -> LiveValidationReport:
    """Seed requested cleanup policy before any fallible preflight or observation."""
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    report = new_live_validation_report(
        scenario=scenario,
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=mode,
        cancel_relay_jobs=cancel_relay_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        stop_worker=stop_worker,
        actions=[
            {
                "kind": resource_kind,
                "resource_id": resource_id,
                "action": action,
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
            }
        ],
    )
    return report


def _write_failed_acceptance_report(
    *,
    path: Path,
    scenario: str,
    cluster: str,
    check_id: str,
    summary: str,
    error: BaseException,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
    partial_report: LiveValidationReport | None = None,
) -> None:
    """Persist one canonical failed report without discarding partial evidence."""
    report = partial_report
    if partial_report is not None and path.exists():
        with suppress(OSError, ValidationError, ValueError):
            existing = load_validation_report(path)
            if existing.report_id == partial_report.report_id:
                expected_error = f"{type(error).__name__}: {error}"
                already_recorded = (
                    existing.status is ValidationStatus.FAILED
                    and existing.error == expected_error
                    and any(
                        check.check_id == check_id
                        and check.status is ValidationStatus.FAILED
                        and check.error == expected_error
                        for check in existing.checks
                    )
                )
                if already_recorded:
                    return
                # The caller's in-memory report may contain the latest observation that
                # failed before its next checkpoint write. The on-disk copy is used only
                # for idempotency here; replacing the partial would discard that evidence.
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    if report is None:
        report = new_live_validation_report(
            scenario=scenario,
            cluster=cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
    recorder = ValidationRecorder(report)
    recorder.record_failure(check_id, summary, error)
    recorder.finish(error)
    recorder.write(path)


def _load_current_acceptance_report(
    path: Path,
    *,
    expected_report_id: str,
) -> LiveValidationReport | None:
    """Load strict evidence only when it belongs to the current CLI invocation."""
    try:
        report = load_validation_report(path)
    except ConfigurationError:
        return None
    return report if report.report_id == expected_report_id else None


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


def _try_remote_gateway_session_passthrough(cluster: str | None, args: list[str]) -> bool:
    """Render a validated remote gateway record through the local public projection."""
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not should_execute_on_cluster(definition):
        return False

    def action() -> None:
        payload = run_remote_clio(definition, args)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RelayError("remote gateway command did not return valid JSON") from exc
        try:
            session = GatewaySession.model_validate(decoded)
        except ValidationError as exc:
            raise RelayError("remote gateway command returned an invalid session") from exc
        if session.cluster != cluster:
            raise RelayError("remote gateway command returned a different cluster")
        typer.echo(_public_json(public_gateway_session(session)))

    _run_or_exit(action)
    return True


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


def _session_transition_lock(*, cluster: str, session_id: str) -> FileLock:
    """Serialize start, detach, and teardown orchestration for one desktop session."""
    directory = default_registry_path().parent / "session-transitions"
    directory.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(f"{cluster}\0{session_id}".encode()).hexdigest()
    return FileLock(str(directory / f"{identity}.lock"), timeout=60)


def _desktop_owner_session_admission_id(*, cluster: str, session_id: str) -> str:
    """Return a cluster-scoped local admission key without exposing raw names in paths."""
    identity = hashlib.sha256(f"{cluster}\0{session_id}".encode()).hexdigest()
    return f"desktop_{identity}"


def _require_durable_session_identity(value: str, *, field: str) -> str:
    """Validate a session identity before it reaches local or remote persistence."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error


def _kind_concurrency_options(items: list[str] | None) -> dict[JobKind, int]:
    try:
        return parse_kind_concurrency_options(items)
    except ConfigurationError as exc:
        raise typer.BadParameter(
            str(exc),
            param_hint="--kind-concurrency",
        ) from exc


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


def _environment_references(items: list[str] | None) -> dict[str, str]:
    """Parse repeatable CHILD=SOURCE environment references without reading values."""
    references: dict[str, str] = {}
    for item in items or []:
        child_name, separator, source_name = item.partition("=")
        if not separator or not child_name or not source_name:
            raise typer.BadParameter("--env-from entries must use CHILD=SOURCE")
        if child_name in references:
            raise typer.BadParameter(f"--env-from child name is repeated: {child_name}")
        references[child_name] = source_name
    return references


def _artifact_use_refs(items: list[str] | None) -> list[ArtifactUse]:
    """Parse repeatable ``ARTIFACT_ID=SHA256`` dependency bindings."""
    refs: list[ArtifactUse] = []
    for item in items or []:
        artifact_id, separator, sha256 = item.partition("=")
        if not separator or not artifact_id or not sha256:
            raise typer.BadParameter(
                "--used-artifact must use ARTIFACT_ID=SHA256",
                param_hint="--used-artifact",
            )
        try:
            refs.append(ArtifactUse(artifact_id=artifact_id, sha256=sha256))
        except ValueError as exc:
            raise typer.BadParameter(
                str(exc),
                param_hint="--used-artifact",
            ) from exc
    artifact_ids = [ref.artifact_id for ref in refs]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise typer.BadParameter(
            "--used-artifact values must have unique artifact IDs",
            param_hint="--used-artifact",
        )
    return sorted(refs, key=lambda ref: ref.artifact_id)


def _artifact_use_idempotency_suffix(refs: list[ArtifactUse]) -> str:
    """Return a stable suffix only when a submission has artifact dependencies."""
    if not refs:
        return ""
    payload = [ref.model_dump(mode="json") for ref in refs]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f":uses-{hashlib.sha256(encoded).hexdigest()}"


def _run_or_exit(action: Callable[[], None]) -> None:
    try:
        action()
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc
    except (ConfigurationError, RelayError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
