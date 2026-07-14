"""End-to-end frp transport probes for relay HTTP surfaces."""

from __future__ import annotations

import json
import os
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
)
from clio_relay.remote_values import render_remote_shell_path, render_remote_shell_value
from clio_relay.session_lifecycle import (
    CleanupResource,
    SessionLifecycleReport,
    detach_remote_session,
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)
from clio_relay.validation_report import (
    TransportCleanupAction,
    TransportCleanupOutcome,
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    transport_probe_evidence_line,
)

MAX_REMOTE_CLEANUP_OUTPUT_BYTES = 1024 * 1024
MAX_REMOTE_CLEANUP_RESOURCES = 128
MAX_TRANSPORT_ERROR_EVIDENCE_LINES = 16
REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS = 120.0


class _RemoteCleanupResource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: str = Field(min_length=1, max_length=128)
    pid: int = Field(gt=0)
    outcome: Literal["missing", "replaced", "refused", "stopped", "residual"]
    ownership_verified: bool


class _RemoteResidualProcess(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    pid: int = Field(gt=0)
    pgid: int = Field(gt=0)
    state: str = Field(min_length=1, max_length=32)


class _RemoteCleanupPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    outcome: Literal[
        "passed",
        "failed",
        "metadata_missing",
        "invalid_metadata",
        "ownership_refused",
    ]
    completed_at: str | None = Field(default=None, max_length=128)
    resources: list[_RemoteCleanupResource] = Field(
        default_factory=lambda: list[_RemoteCleanupResource](),
        max_length=MAX_REMOTE_CLEANUP_RESOURCES,
    )
    residual_processes: list[_RemoteResidualProcess] = Field(
        default_factory=lambda: list[_RemoteResidualProcess](),
        max_length=MAX_REMOTE_CLEANUP_RESOURCES,
    )
    errors: list[str] = Field(default_factory=list, max_length=MAX_REMOTE_CLEANUP_RESOURCES)
    error: str | None = Field(default=None, max_length=8192)

    @model_validator(mode="after")
    def validate_shape_for_outcome(self) -> _RemoteCleanupPayload:
        if len(self.resources) + len(self.residual_processes) > MAX_REMOTE_CLEANUP_RESOURCES:
            raise ValueError("remote cleanup contains too many aggregate resources")
        if self.outcome in {"passed", "failed"} and self.completed_at is None:
            raise ValueError("completed remote cleanup requires completed_at")
        if self.outcome == "passed" and (self.errors or self.residual_processes):
            raise ValueError("passed remote cleanup cannot contain errors or residual processes")
        if self.outcome in {"invalid_metadata", "ownership_refused"} and self.error is None:
            raise ValueError("refused remote cleanup requires an error detail")
        return self


class ManagedProcess(Protocol):
    """Subset of subprocess.Popen used by the transport probe."""

    stdin: Any | None

    def poll(self) -> int | None:
        """Return process status."""
        ...

    def terminate(self) -> None:
        """Terminate the process."""
        ...

    def kill(self) -> None:
        """Kill the process."""
        ...

    def wait(self, timeout: float | None = None) -> int:
        """Wait for process termination."""
        ...


ProcessFactory = Callable[..., ManagedProcess]
HttpCheck = Callable[[str], list[str]]


def transport_evidence_lines_from_error(error: BaseException) -> list[str]:
    """Return bounded structured transport evidence attached during cleanup."""
    raw = error.__dict__.get("_clio_relay_transport_evidence_lines")
    if not isinstance(raw, list):
        return []
    lines = [item for item in cast(list[object], raw) if isinstance(item, str)]
    return lines[:MAX_TRANSPORT_ERROR_EVIDENCE_LINES]


def _attach_transport_evidence(
    error: BaseException,
    lines: list[str],
) -> BaseException:
    structured = [line for line in lines if line.startswith("transport.probe_evidence=")][
        :MAX_TRANSPORT_ERROR_EVIDENCE_LINES
    ]
    existing = transport_evidence_lines_from_error(error)
    combined = list(dict.fromkeys([*existing, *structured]))[:MAX_TRANSPORT_ERROR_EVIDENCE_LINES]
    try:
        error.__dict__["_clio_relay_transport_evidence_lines"] = combined
    except (AttributeError, TypeError):
        wrapped = RelayError(str(error))
        wrapped.__dict__["_clio_relay_transport_evidence_lines"] = combined
        return wrapped
    return error


def _transport_resource_line(
    *,
    probe_id: str,
    cluster: str,
    cleanup_mode: str,
    resources: list[TransportCleanupResourceEvidence],
) -> str:
    return transport_probe_evidence_line(
        TransportProbeEvidence(
            probe_id=probe_id,
            cluster=cluster,
            cleanup_mode=cleanup_mode,
            resources=resources,
        )
    )


def _process_cleanup_resource(
    *,
    kind: str,
    resource_id: str,
    role: str,
    location: str,
    ownership_verified: bool,
    outcome: TransportCleanupOutcome,
    verified_after_operation: bool,
    observed_state: str | None,
    residual: bool,
    detail: str | None,
    action: TransportCleanupAction = "stop",
    metadata: dict[str, object] | None = None,
) -> TransportCleanupResourceEvidence:
    return TransportCleanupResourceEvidence(
        kind=kind,
        resource_id=resource_id,
        role=role,
        location=location,
        action=action,
        ownership_verified=ownership_verified,
        outcome=outcome,
        verified_after_operation=verified_after_operation,
        observed_state=observed_state,
        residual=residual,
        detail=detail,
        metadata=metadata or {},
    )


def _reject_nonfinite_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def run_frp_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int = 8765,
    proxy_name: str = "relay-http",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
) -> list[str]:
    """Probe desktop-to-cluster HTTP reachability through frp STCP."""
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    _assert_local_bind_port_available(local_bind_port)
    factory = process_factory or _popen
    transport = definition.frp_transport
    server_addr = _require_frp_server_addr(transport.server_addr, cluster)
    _require_api_token(api_token)
    protocol = FrpTransportProtocol(transport.protocol)
    with tempfile.TemporaryDirectory(prefix="clio-relay-transport-") as temp_dir:
        temp_path = Path(temp_dir)
        probe_id = _probe_id(cluster=cluster, proxy_name=proxy_name)
        remote_frpc_config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=token,
                transport_protocol=protocol,
                proxy_name=proxy_name,
                local_port=remote_api_port,
                secret_key=secret_key,
            )
        )
        visitor_config_path = temp_path / "frpc-visitor.toml"
        visitor_config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=token,
                    transport_protocol=protocol,
                    visitor_name=f"{proxy_name}-visitor",
                    server_name=proxy_name,
                    bind_port=local_bind_port,
                    secret_key=secret_key,
                )
            ),
            encoding="utf-8",
        )
        remote = factory(
            ["ssh", definition.ssh_host, "bash", "-s"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert remote.stdin is not None
        remote.stdin.write(
            _remote_probe_script(
                cluster=cluster,
                definition=definition,
                probe_id=probe_id,
                api_token=api_token,
                api_port=remote_api_port,
                frpc_config=remote_frpc_config,
            ).encode("utf-8")
        )
        remote.stdin.close()
        visitor = factory(
            [frpc_bin, "-c", str(visitor_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        lines: list[str] = []
        primary_error: BaseException | None = None
        try:
            time.sleep(1)
            if remote.poll() is not None:
                raise RelayError(_process_output_message(remote, "remote transport probe failed"))
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            try:
                _wait_for_healthz(
                    f"http://127.0.0.1:{local_bind_port}/healthz",
                    timeout_seconds=timeout_seconds,
                )
            except RelayError as exc:
                _terminate(visitor)
                _terminate(remote)
                details = [
                    str(exc),
                    _process_output_message(remote, "remote transport probe still running"),
                    _process_output_message(visitor, "local frpc visitor still running"),
                ]
                raise RelayError("\n".join(details)) from exc
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            lines = [
                f"transport.cluster={cluster}",
                f"transport.server={server_addr}:{transport.server_port}",
                f"transport.protocol={transport.protocol}",
                f"transport.local_url=http://127.0.0.1:{local_bind_port}",
                "transport.healthz=ok",
            ]
            if http_check is not None:
                lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
        except BaseException as exc:
            primary_error = exc
        cleanup_lines = _finish_frp_probe_cleanup(
            cluster=cluster,
            definition=definition,
            probe_id=probe_id,
            visitor=visitor,
            remote=remote,
            primary_error=primary_error,
        )
        return [*lines, *cleanup_lines]


def run_frp_direct_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int = 8765,
    proxy_name: str = "relay-http-direct",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
    allow_stcp_fallback: bool = True,
) -> list[str]:
    """Probe direct XTCP HTTP reachability, optionally falling back to STCP."""
    try:
        lines = _run_frp_http_probe_with_proxy_type(
            cluster=cluster,
            definition=definition,
            frpc_bin=frpc_bin,
            token=token,
            secret_key=secret_key,
            local_bind_port=local_bind_port,
            remote_api_port=remote_api_port,
            proxy_name=proxy_name,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            process_factory=process_factory,
            http_check=http_check,
            proxy_type="xtcp",
        )
    except RelayError as exc:
        if not allow_stcp_fallback:
            raise
        failed_attempt_evidence = transport_evidence_lines_from_error(exc)
        fallback_lines = run_frp_http_probe(
            cluster=cluster,
            definition=definition,
            frpc_bin=frpc_bin,
            token=token,
            secret_key=secret_key,
            local_bind_port=local_bind_port,
            remote_api_port=remote_api_port,
            proxy_name=f"{proxy_name}-fallback",
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            process_factory=process_factory,
            http_check=http_check,
        )
        return [
            f"direct_transport.cluster={cluster}",
            "direct_transport.mode=xtcp",
            "direct_transport.result=frp_stcp",
            f"direct_transport.xtcp_error={str(exc).splitlines()[0]}",
            *failed_attempt_evidence,
            *fallback_lines,
        ]
    return [
        f"direct_transport.cluster={cluster}",
        "direct_transport.mode=xtcp",
        "direct_transport.result=xtcp",
        *lines,
    ]


def run_ssh_forward_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    local_bind_port: int,
    remote_api_port: int = 8765,
    session_id: str = "relay-ssh-forward",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
    detach_remote: bool = False,
    replace_remote: bool = True,
) -> list[str]:
    """Probe desktop-to-cluster HTTP reachability through SSH port forwarding."""
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if api_token is None or api_token == "":
        raise ConfigurationError(
            "SSH transport probes require CLIO_RELAY_API_TOKEN for the owned remote API"
        )
    _assert_local_bind_port_available(local_bind_port)
    try:
        start_lines = start_remote_session(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            replace=replace_remote,
        )
        session_generation_id = _started_session_generation_id(
            start_lines,
            definition=definition,
            session_id=session_id,
        )
    except BaseException as exc:
        evidence_line = _transport_resource_line(
            probe_id=f"ssh-probe:{session_id}:generation-unverified",
            cluster=cluster,
            cleanup_mode=(
                "transport_probe_detach" if detach_remote else "transport_probe_teardown"
            ),
            resources=[
                _process_cleanup_resource(
                    kind="relay_session",
                    resource_id=session_id,
                    role="remote_transport_session",
                    location=definition.ssh_host,
                    action="retain" if detach_remote else "stop",
                    ownership_verified=False,
                    outcome="unknown",
                    verified_after_operation=False,
                    observed_state="running_or_unknown",
                    residual=True,
                    detail=f"remote session start was not verified: {type(exc).__name__}: {exc}",
                    metadata={"session_id": session_id, "session_generation_id": None},
                )
            ],
        )
        attached = _attach_transport_evidence(exc, [evidence_line])
        if attached is not exc:
            raise attached from exc
        raise
    factory = process_factory or _popen
    forward: ManagedProcess | None = None
    lines: list[str] = []
    primary_error: BaseException | None = None
    try:
        forward = factory(
            [
                "ssh",
                "-N",
                "-L",
                f"127.0.0.1:{local_bind_port}:127.0.0.1:{remote_api_port}",
                definition.ssh_host,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(1)
        if forward.poll() is not None:
            raise RelayError(_process_output_message(forward, "local ssh forward failed"))
        try:
            _wait_for_healthz(
                f"http://127.0.0.1:{local_bind_port}/healthz",
                timeout_seconds=timeout_seconds,
            )
        except RelayError as exc:
            _terminate(forward)
            details = [
                str(exc),
                _process_output_message(forward, "local ssh forward still running"),
            ]
            raise RelayError("\n".join(details)) from exc
        if forward.poll() is not None:
            raise RelayError(_process_output_message(forward, "local ssh forward failed"))
        lines = [
            f"transport.cluster={cluster}",
            "transport.protocol=ssh_forward",
            f"transport.ssh_host={definition.ssh_host}",
            f"transport.session_id={session_id}",
            f"transport.remote_api_port={remote_api_port}",
            f"transport.local_url=http://127.0.0.1:{local_bind_port}",
            "transport.healthz=ok",
            *start_lines,
        ]
        if http_check is not None:
            lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
    except BaseException as exc:
        primary_error = exc

    cleanup_errors: list[str] = []
    cleanup_evidence_lines: list[str] = []
    if forward is not None:
        forward_stopped = False
        forward_detail: str | None = None
        try:
            _terminate(forward)
            forward_stopped = forward.poll() is not None
            if not forward_stopped:
                forward_detail = "local SSH forward remains running"
                cleanup_errors.append(forward_detail)
        except BaseException as exc:
            forward_detail = f"local SSH cleanup failed: {type(exc).__name__}: {exc}"
            cleanup_errors.append(forward_detail)
        cleanup_evidence_lines.append(
            _transport_resource_line(
                probe_id=f"ssh-probe:{session_id}:{session_generation_id}",
                cluster=cluster,
                cleanup_mode=(
                    "transport_probe_detach" if detach_remote else "transport_probe_teardown"
                ),
                resources=[
                    _process_cleanup_resource(
                        kind="connector",
                        resource_id=(
                            f"ssh-forward:{session_id}:{session_generation_id}:{local_bind_port}"
                        ),
                        role="desktop_ssh_forward",
                        location="desktop",
                        ownership_verified=True,
                        outcome="stopped" if forward_stopped else "failed",
                        verified_after_operation=forward_stopped,
                        observed_state=("stopped" if forward_stopped else "running_or_unknown"),
                        residual=not forward_stopped,
                        detail=forward_detail,
                        metadata={
                            "session_id": session_id,
                            "session_generation_id": session_generation_id,
                            "local_bind_port": local_bind_port,
                            "remote_api_port": remote_api_port,
                        },
                    )
                ],
            )
        )
    session_evidence_recorded = False
    if detach_remote:
        try:
            detached = detach_remote_session(
                definition=definition,
                session_id=session_id,
                cluster=cluster,
            )
            cleanup_evidence_lines.append(
                _session_lifecycle_evidence_line(
                    detached,
                    cluster=cluster,
                    session_id=session_id,
                    session_generation_id=session_generation_id,
                )
            )
            session_evidence_recorded = True
            lines.extend(
                _verified_session_detach_lines(
                    detached,
                    session_id=session_id,
                    session_generation_id=session_generation_id,
                )
            )
        except BaseException as exc:
            detail = f"remote session detach verification failed: {type(exc).__name__}: {exc}"
            cleanup_errors.append(detail)
            if not session_evidence_recorded:
                cleanup_evidence_lines.append(
                    _unverified_session_evidence_line(
                        cluster=cluster,
                        definition=definition,
                        session_id=session_id,
                        session_generation_id=session_generation_id,
                        detail=detail,
                        action="retain",
                    )
                )
    else:
        try:
            teardown = teardown_remote_session(
                definition=definition,
                session_id=session_id,
                expected_session_generation_id=session_generation_id,
                cluster=cluster,
            )
            cleanup_evidence_lines.append(
                _session_lifecycle_evidence_line(
                    teardown,
                    cluster=cluster,
                    session_id=session_id,
                    session_generation_id=session_generation_id,
                )
            )
            session_evidence_recorded = True
            lines.extend(
                _verified_session_teardown_lines(
                    teardown,
                    session_id=session_id,
                    session_generation_id=session_generation_id,
                )
            )
        except BaseException as exc:
            detail = f"remote session cleanup failed: {type(exc).__name__}: {exc}"
            cleanup_errors.append(detail)
            if not session_evidence_recorded:
                cleanup_evidence_lines.append(
                    _unverified_session_evidence_line(
                        cluster=cluster,
                        definition=definition,
                        session_id=session_id,
                        session_generation_id=session_generation_id,
                        detail=detail,
                        action="stop",
                    )
                )
    lines.extend(cleanup_evidence_lines)
    if cleanup_errors:
        detail = "; ".join(cleanup_errors)
        if primary_error is not None:
            error = RelayError(f"{primary_error}\ntransport cleanup errors: {detail}")
            raise _attach_transport_evidence(error, cleanup_evidence_lines) from primary_error
        error = RelayError(f"transport cleanup errors: {detail}")
        raise _attach_transport_evidence(error, cleanup_evidence_lines)
    if primary_error is not None:
        raise _attach_transport_evidence(primary_error, cleanup_evidence_lines)
    return lines


def _started_session_generation_id(
    start_lines: list[str],
    *,
    definition: ClusterDefinition,
    session_id: str,
) -> str:
    """Recover the exact generation created or reused by a remote session start."""
    prefix = "session_generation_id="
    values = [line.removeprefix(prefix) for line in start_lines if line.startswith(prefix)]
    if len(values) == 1 and values[0]:
        return values[0]
    status = status_remote_session(definition=definition, session_id=session_id)
    generation_id = status.get("session_generation_id")
    owned_session = status.get("session_id") == session_id and status.get("owner") == "clio-relay"
    running_ownership_verified = (
        status.get("running") is not True or status.get("ownership_verified") is True
    )
    if (
        owned_session
        and running_ownership_verified
        and isinstance(generation_id, str)
        and generation_id
    ):
        return generation_id
    raise RelayError("remote session start did not return a verifiable session generation id")


def _verified_session_detach_lines(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
) -> list[str]:
    if (
        report.mode != "detach"
        or report.session_id != session_id
        or report.session_generation_id != session_generation_id
    ):
        raise RelayError("remote session detach identity or generation did not match its start")
    if report.errors or report.residual_resources:
        detail = "; ".join(
            [
                *report.errors,
                *[item.detail or item.resource_id for item in report.residual_resources],
            ]
        )
        raise RelayError(f"remote session detach verification failed: {detail}")
    retained = [
        resource
        for resource in report.resources
        if resource.kind == "remote_relay_api"
        and resource.action == "retain"
        and resource.outcome == "retained"
        and resource.ownership_verified
        and resource.verified_after_operation
        and not resource.residual
    ]
    if not retained:
        raise RelayError("remote session detach did not prove an active owned relay API")
    return [
        "transport.remote_session=retained",
        f"transport.remote_session_resource={retained[0].resource_id}",
        "transport.remote_session_ownership=verified",
        "transport.cleanup=detached",
    ]


def _verified_session_teardown_lines(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
) -> list[str]:
    """Reject SSH probe cleanup unless the owned remote API is proven absent."""
    identity_verified = (
        report.mode == "teardown"
        and report.session_id == session_id
        and report.session_generation_id == session_generation_id
    )
    prior_status = report.prior_session_status
    post_status = report.post_session_status
    transition_verified = (
        prior_status is not None
        and prior_status.session_generation_id == session_generation_id
        and prior_status.ownership_verified
        and post_status is not None
        and post_status.session_generation_id == session_generation_id
        and not post_status.running
        and post_status.ownership_verified
    )
    remote_apis = [resource for resource in report.resources if resource.kind == "remote_relay_api"]
    valid_outcomes = {"stopped", "missing"}
    verified = (
        identity_verified
        and transition_verified
        and not report.errors
        and not report.residual_resources
        and len(remote_apis) == 1
        and remote_apis[0].action == "stop"
        and remote_apis[0].outcome in valid_outcomes
        and remote_apis[0].ownership_verified
        and remote_apis[0].verified_after_operation
        and not remote_apis[0].residual
        and all(
            _verified_auxiliary_teardown_resource(resource)
            for resource in report.resources
            if resource.kind != "remote_relay_api"
        )
    )
    if not verified:
        raise RelayError(
            "owned SSH probe session cleanup was not verified: "
            + json.dumps(report.json_payload(), sort_keys=True)
        )
    summary = ",".join(f"{resource.kind}:{resource.outcome}" for resource in report.resources)
    return [
        "transport.remote_cleanup=passed",
        f"transport.remote_cleanup_resources={summary}",
        "transport.remote_cleanup_residuals=0",
        "transport.cleanup=passed",
    ]


def _verified_auxiliary_teardown_resource(resource: CleanupResource) -> bool:
    if resource.kind in {"desktop_connector", "remote_connector"}:
        return (
            resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.verified_after_operation
            and not resource.residual
        )
    if resource.kind == "gateway_record":
        return (
            resource.action == "close"
            and resource.outcome == "closed"
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        )
    return not resource.residual and resource.verified_after_operation


def _session_lifecycle_evidence_line(
    report: SessionLifecycleReport,
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
) -> str:
    stable_session_id = f"{session_id}:{session_generation_id}"
    probe_id = f"ssh-probe:{stable_session_id}"
    report_detail = "; ".join(report.errors) if report.errors else None
    resources: list[TransportCleanupResourceEvidence] = []
    for resource in report.resources:
        metadata = {
            **resource.metadata,
            "cleanup_kind": resource.kind,
            "session_id": session_id,
            "session_generation_id": session_generation_id,
        }
        if resource.kind == "remote_relay_api":
            session_metadata = {
                **metadata,
                "api_pid": resource.resource_id,
                "prior_session_status": (
                    report.prior_session_status.model_dump(mode="json")
                    if report.prior_session_status is not None
                    else None
                ),
                "post_session_status": (
                    report.post_session_status.model_dump(mode="json")
                    if report.post_session_status is not None
                    else None
                ),
            }
            resources.append(
                _process_cleanup_resource(
                    kind="relay_session",
                    resource_id=stable_session_id,
                    role="remote_transport_session",
                    location=resource.location,
                    action=resource.action,
                    ownership_verified=resource.ownership_verified,
                    outcome=resource.outcome,
                    verified_after_operation=resource.verified_after_operation,
                    observed_state=resource.observed_state,
                    residual=resource.residual,
                    detail=resource.detail or report_detail,
                    metadata=session_metadata,
                )
            )
            resources.append(
                _process_cleanup_resource(
                    kind="relay_process",
                    resource_id=resource.resource_id,
                    role="remote_relay_api_process",
                    location=resource.location,
                    action=resource.action,
                    ownership_verified=resource.ownership_verified,
                    outcome=resource.outcome,
                    verified_after_operation=resource.verified_after_operation,
                    observed_state=resource.observed_state,
                    residual=resource.residual,
                    detail=resource.detail or report_detail,
                    metadata=metadata,
                )
            )
            continue
        canonical_kind = {
            "desktop_connector": "connector",
            "remote_connector": "connector",
            "gateway_record": "gateway_session",
            "worker_service": "relay_worker",
        }.get(resource.kind, resource.kind)
        resources.append(
            _process_cleanup_resource(
                kind=canonical_kind,
                resource_id=resource.resource_id,
                role=f"{resource.kind}:{resource.action}",
                location=resource.location,
                action=resource.action,
                ownership_verified=resource.ownership_verified,
                outcome=resource.outcome,
                verified_after_operation=resource.verified_after_operation,
                observed_state=resource.observed_state,
                residual=resource.residual,
                detail=resource.detail or report_detail,
                metadata=metadata,
            )
        )
    if not resources:
        resources.append(
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=stable_session_id,
                role="remote_transport_session",
                location="remote",
                action="stop" if report.mode == "teardown" else "retain",
                ownership_verified=False,
                outcome="unknown",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=report_detail or "session lifecycle report omitted resource results",
                metadata={
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                },
            )
        )
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode=(
            "transport_probe_detach" if report.mode == "detach" else "transport_probe_teardown"
        ),
        resources=resources,
    )


def _unverified_session_evidence_line(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: str,
    detail: str,
    action: Literal["retain", "stop"],
) -> str:
    stable_session_id = f"{session_id}:{session_generation_id}"
    return _transport_resource_line(
        probe_id=f"ssh-probe:{stable_session_id}",
        cluster=cluster,
        cleanup_mode=(
            "transport_probe_detach" if action == "retain" else "transport_probe_teardown"
        ),
        resources=[
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=stable_session_id,
                role="remote_transport_session",
                location=definition.ssh_host,
                action=action,
                ownership_verified=False,
                outcome="failed",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=detail,
                metadata={
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                },
            )
        ],
    )


def _run_frp_http_probe_with_proxy_type(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int,
    proxy_name: str,
    api_token: str | None,
    timeout_seconds: float,
    process_factory: ProcessFactory | None,
    http_check: HttpCheck | None,
    proxy_type: str,
) -> list[str]:
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if proxy_type not in {"stcp", "xtcp"}:
        raise ConfigurationError(f"unsupported transport proxy type: {proxy_type}")
    _assert_local_bind_port_available(local_bind_port)
    factory = process_factory or _popen
    transport = definition.frp_transport
    server_addr = _require_frp_server_addr(transport.server_addr, cluster)
    _require_api_token(api_token)
    protocol = FrpTransportProtocol(transport.protocol)
    with tempfile.TemporaryDirectory(prefix="clio-relay-transport-") as temp_dir:
        temp_path = Path(temp_dir)
        probe_id = _probe_id(cluster=cluster, proxy_name=proxy_name)
        remote_frpc_config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=token,
                transport_protocol=protocol,
                proxy_name=proxy_name,
                proxy_type=proxy_type,
                local_port=remote_api_port,
                secret_key=secret_key,
            )
        )
        visitor_config_path = temp_path / "frpc-visitor.toml"
        visitor_config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=token,
                    transport_protocol=protocol,
                    visitor_name=f"{proxy_name}-visitor",
                    visitor_type=proxy_type,
                    server_name=proxy_name,
                    bind_port=local_bind_port,
                    secret_key=secret_key,
                    keep_tunnel_open=proxy_type == "xtcp",
                )
            ),
            encoding="utf-8",
        )
        remote = factory(
            ["ssh", definition.ssh_host, "bash", "-s"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert remote.stdin is not None
        remote.stdin.write(
            _remote_probe_script(
                cluster=cluster,
                definition=definition,
                probe_id=probe_id,
                api_token=api_token,
                api_port=remote_api_port,
                frpc_config=remote_frpc_config,
            ).encode("utf-8")
        )
        remote.stdin.close()
        visitor = factory(
            [frpc_bin, "-c", str(visitor_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        lines: list[str] = []
        primary_error: BaseException | None = None
        try:
            time.sleep(1)
            if remote.poll() is not None:
                raise RelayError(_process_output_message(remote, "remote transport probe failed"))
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            try:
                _wait_for_healthz(
                    f"http://127.0.0.1:{local_bind_port}/healthz",
                    timeout_seconds=timeout_seconds,
                )
            except RelayError as exc:
                _terminate(visitor)
                _terminate(remote)
                details = [
                    str(exc),
                    _process_output_message(remote, "remote transport probe still running"),
                    _process_output_message(visitor, "local frpc visitor still running"),
                ]
                raise RelayError("\n".join(details)) from exc
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            lines = [
                f"transport.cluster={cluster}",
                f"transport.server={server_addr}:{transport.server_port}",
                f"transport.protocol={transport.protocol}",
                f"transport.proxy_type={proxy_type}",
                f"transport.local_url=http://127.0.0.1:{local_bind_port}",
                "transport.healthz=ok",
            ]
            if http_check is not None:
                lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
        except BaseException as exc:
            primary_error = exc
        cleanup_lines = _finish_frp_probe_cleanup(
            cluster=cluster,
            definition=definition,
            probe_id=probe_id,
            visitor=visitor,
            remote=remote,
            primary_error=primary_error,
        )
        return [*lines, *cleanup_lines]


def _finish_frp_probe_cleanup(
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
    visitor: ManagedProcess,
    remote: ManagedProcess,
    primary_error: BaseException | None,
) -> list[str]:
    """Verify local and remote probe teardown before reporting cleanup success."""
    cleanup_errors: list[str] = []
    cleanup_lines: list[str] = []
    local_stopped = False
    local_detail: str | None = None
    try:
        _terminate(visitor)
        local_stopped = visitor.poll() is not None
        if not local_stopped:
            local_detail = "local frpc visitor remains running"
            cleanup_errors.append(local_detail)
    except BaseException as exc:
        local_detail = f"local frpc cleanup failed: {type(exc).__name__}: {exc}"
        cleanup_errors.append(local_detail)
    try:
        cleanup_lines.extend(
            _cleanup_remote_probe(
                cluster=cluster,
                definition=definition,
                probe_id=probe_id,
                require_metadata=primary_error is None,
            )
        )
    except BaseException as exc:
        cleanup_lines.extend(transport_evidence_lines_from_error(exc))
        cleanup_errors.append(f"remote cleanup failed: {type(exc).__name__}: {exc}")
    remote_control_stopped = False
    remote_control_detail: str | None = None
    try:
        _terminate(remote)
        remote_control_stopped = remote.poll() is not None
        if not remote_control_stopped:
            remote_control_detail = "remote SSH probe process remains running"
            cleanup_errors.append(remote_control_detail)
    except BaseException as exc:
        remote_control_detail = f"remote SSH cleanup failed: {type(exc).__name__}: {exc}"
        cleanup_errors.append(remote_control_detail)
    cleanup_lines.append(
        _transport_resource_line(
            probe_id=probe_id,
            cluster=cluster,
            cleanup_mode="transport_probe_teardown",
            resources=[
                _process_cleanup_resource(
                    kind="connector",
                    resource_id=f"frpc-visitor:{probe_id}",
                    role="desktop_frpc_visitor",
                    location="desktop",
                    ownership_verified=True,
                    outcome="stopped" if local_stopped else "failed",
                    verified_after_operation=local_stopped,
                    observed_state="stopped" if local_stopped else "running_or_unknown",
                    residual=not local_stopped,
                    detail=local_detail,
                ),
                _process_cleanup_resource(
                    kind="connector",
                    resource_id=f"ssh-probe-control:{probe_id}",
                    role="desktop_ssh_probe_control",
                    location="desktop",
                    ownership_verified=True,
                    outcome="stopped" if remote_control_stopped else "failed",
                    verified_after_operation=remote_control_stopped,
                    observed_state=("stopped" if remote_control_stopped else "running_or_unknown"),
                    residual=not remote_control_stopped,
                    detail=remote_control_detail,
                ),
            ],
        )
    )
    if cleanup_errors:
        detail = "; ".join(cleanup_errors)
        evidence_lines = [
            *(transport_evidence_lines_from_error(primary_error) if primary_error else []),
            *cleanup_lines,
        ]
        if primary_error is not None:
            error = RelayError(f"{primary_error}\ntransport cleanup errors: {detail}")
            raise _attach_transport_evidence(error, evidence_lines) from primary_error
        error = RelayError(f"transport cleanup errors: {detail}")
        raise _attach_transport_evidence(error, evidence_lines)
    if primary_error is not None:
        raise _attach_transport_evidence(primary_error, cleanup_lines)
    return [*cleanup_lines, "transport.cleanup=passed"]


def _remote_probe_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
    api_token: str | None,
    api_port: int,
    frpc_config: str,
) -> str:
    token_export = ""
    require_token = ""
    if api_token is not None:
        token_export = f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}"
        require_token = " --require-token"
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    return f"""set -euo pipefail
umask 077
export PATH="$HOME/.local/bin:$PATH"
export CLIO_RELAY_CORE_DIR={render_remote_shell_path(definition.core_dir, field="core_dir")}
export CLIO_RELAY_SPOOL_DIR={render_remote_shell_path(definition.spool_dir, field="spool_dir")}
export CLIO_RELAY_JARVIS_BIN={render_remote_shell_value(jarvis_bin, field="jarvis_bin")}
export CLIO_RELAY_FRPC_BIN={render_remote_shell_value(frpc_bin, field="frpc_bin")}
export CLIO_RELAY_AGENT_BIN={render_remote_shell_value(agent_bin, field="agent_bin")}
export CLIO_RELAY_AGENT_ADAPTER={_shell_single_quote(definition.agent_adapter)}
{token_export}
tmp="$(mktemp -d)"
probe_id={_shell_single_quote(probe_id)}
probe_dir="$HOME/.local/share/clio-relay/transport-probes/$probe_id"
metadata_file="$probe_dir/metadata.json"
mkdir -p "$probe_dir"
api_pid=""
frpc_pid=""
owner_token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
cleanup() {{
  if [ -n "$frpc_pid" ]; then kill -- "-$frpc_pid" 2>/dev/null || true; fi
  if [ -n "$api_pid" ]; then kill -- "-$api_pid" 2>/dev/null || true; fi
  wait 2>/dev/null || true
  rm -rf "$tmp"
}}
trap cleanup EXIT
cat > "$tmp/frpc.toml" <<'__CLIO_RELAY_FRPC_CONFIG__'
{frpc_config.rstrip()}
__CLIO_RELAY_FRPC_CONFIG__
echo "transport_probe_cluster={cluster}"
if python3 - {api_port} <<'__CLIO_RELAY_PORT_CHECK__'
import socket
import sys
port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(1)
__CLIO_RELAY_PORT_CHECK__
then
  :
else
  echo "remote API port is already occupied: {api_port}" >&2
  exit 1
fi
setsid env "CLIO_RELAY_PROBE_OWNER_TOKEN=$owner_token" \
  clio-relay api start --host 127.0.0.1 --port {api_port}{require_token} \
  >"$probe_dir/api.log" 2>&1 &
api_pid="$!"
sleep 1
if ! kill -0 "$api_pid" 2>/dev/null; then
  cat "$probe_dir/api.log" >&2
  exit 1
fi
setsid env "CLIO_RELAY_PROBE_OWNER_TOKEN=$owner_token" \
  "$CLIO_RELAY_FRPC_BIN" -c "$tmp/frpc.toml" >"$probe_dir/frpc.log" 2>&1 &
frpc_pid="$!"
python3 - "$metadata_file" "$probe_id" "$owner_token" "$api_pid" "$frpc_pid" \
  "$tmp" "{api_port}" <<'__CLIO_RELAY_PROBE_METADATA__'
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

path, probe_id, owner_token, api_pid, frpc_pid, tmp, api_port = sys.argv[1:]

def identity(pid_raw, kind, markers):
    pid = int(pid_raw)
    proc = Path("/proc") / str(pid)
    for _ in range(40):
        try:
            pgid = os.getpgid(pid)
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
                "utf-8", errors="replace"
            )
            start_ticks = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[19]
        except OSError:
            time.sleep(0.05)
            continue
        owned = (
            pgid == pid
            and f"CLIO_RELAY_PROBE_OWNER_TOKEN={{owner_token}}".encode() in environment
            and all(marker in command for marker in markers)
        )
        if owned:
            return {{
                "kind": kind,
                "pid": pid,
                "pgid": pgid,
                "process_start_ticks": start_ticks,
                "command_contains": markers,
            }}
        time.sleep(0.05)
    raise RuntimeError(f"owned {{kind}} process did not establish its identity")

processes = [
    identity(api_pid, "remote_relay_api", ["clio-relay", "api", "start", "--port", api_port]),
    identity(frpc_pid, "remote_connector", ["frpc", f"{{tmp}}/frpc.toml"]),
]
metadata = {{
    "owner": "clio-relay",
    "probe_id": probe_id,
    "cluster": {cluster!r},
    "owner_token": owner_token,
    "tmp": tmp,
    "processes": processes,
    "logs": [
        str(Path(path).parent / "api.log"),
        str(Path(path).parent / "frpc.log"),
    ],
    "started_at": datetime.now(timezone.utc).isoformat(),
}}
temporary = Path(path).with_suffix(".tmp")
temporary.write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
os.replace(temporary, path)
__CLIO_RELAY_PROBE_METADATA__
wait
"""


def _cleanup_remote_probe(
    *,
    definition: ClusterDefinition,
    probe_id: str,
    require_metadata: bool = True,
    cluster: str | None = None,
) -> list[str]:
    """Stop only token-verified remote probe groups and return cleanup evidence."""
    script = f"""set -euo pipefail
probe_id={_shell_single_quote(probe_id)}
probe_dir="$HOME/.local/share/clio-relay/transport-probes/$probe_id"
metadata_file="$probe_dir/metadata.json"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ -f "$metadata_file" ] && break
  sleep 0.5
done
[ -f "$metadata_file" ] || {{
  echo '{{"outcome":"metadata_missing","resources":[],"residual_processes":[]}}'
  exit {2 if require_metadata else 0}
}}
python3 - "$metadata_file" "$probe_id" <<'__CLIO_RELAY_CLEANUP_PROBE__'
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

metadata_path = Path(sys.argv[1])
expected_probe_id = sys.argv[2]
try:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    print(json.dumps({{"outcome": "invalid_metadata", "error": str(exc)}}))
    raise SystemExit(2)
if (
    metadata.get("owner") != "clio-relay"
    or metadata.get("probe_id") != expected_probe_id
):
    print(json.dumps({{"outcome": "ownership_refused", "error": "metadata owner/probe mismatch"}}))
    raise SystemExit(2)
token = metadata.get("owner_token")
processes = metadata.get("processes")
if not isinstance(token, str) or not token or not isinstance(processes, list):
    print(json.dumps({{
        "outcome": "invalid_metadata",
        "error": "missing owner token/process records",
    }}))
    raise SystemExit(2)

def process_info(pid):
    proc = Path("/proc") / str(pid)
    try:
        command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
            "utf-8", errors="replace"
        )
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        stat_fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        pgid = os.getpgid(pid)
    except OSError:
        return None
    return {{
        "command": command,
        "environment": environment,
        "pgid": pgid,
        "state": stat_fields[0],
        "start_ticks": stat_fields[19],
    }}

def token_processes():
    matches = []
    needle = f"CLIO_RELAY_PROBE_OWNER_TOKEN={{token}}".encode()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            if needle not in environment:
                continue
            info = process_info(int(proc.name))
        except OSError:
            continue
        if info is not None and info["state"] != "Z":
            matches.append((int(proc.name), info))
    return matches

resources = []
authorized_groups = set()
recorded_groups = set()
errors = []
for raw in processes:
    if not isinstance(raw, dict):
        errors.append("process record is not an object")
        continue
    kind = raw.get("kind")
    pid = raw.get("pid")
    pgid = raw.get("pgid")
    start_ticks = raw.get("process_start_ticks")
    markers = raw.get("command_contains")
    if (
        not isinstance(kind, str)
        or not isinstance(pid, int)
        or not isinstance(pgid, int)
        or not isinstance(start_ticks, str)
        or not isinstance(markers, list)
        or not all(isinstance(marker, str) for marker in markers)
    ):
        errors.append(f"invalid process record: {{raw!r}}")
        continue
    recorded_groups.add(pgid)
    info = process_info(pid)
    if info is None or info["state"] == "Z":
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "missing",
            "ownership_verified": True,
        }})
        continue
    if info["start_ticks"] != start_ticks:
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "replaced",
            "ownership_verified": False,
        }})
        continue
    owned = (
        pgid == pid
        and info["pgid"] == pgid
        and f"CLIO_RELAY_PROBE_OWNER_TOKEN={{token}}".encode() in info["environment"]
        and all(marker in info["command"] for marker in markers)
    )
    if not owned:
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "refused",
            "ownership_verified": False,
        }})
        errors.append(f"ownership proof failed for {{kind}} pid {{pid}}")
        continue
    authorized_groups.add(pgid)
    resources.append({{
        "kind": kind,
        "pid": pid,
        "outcome": "stopping",
        "ownership_verified": True,
    }})

for _pid, info in token_processes():
    if info["pgid"] in recorded_groups:
        authorized_groups.add(info["pgid"])
for pgid in sorted(authorized_groups):
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    if not token_processes():
        break
    time.sleep(0.2)
for pid, info in token_processes():
    pgid = info["pgid"]
    if pgid not in authorized_groups:
        errors.append(f"token-owned pid {{pid}} has unexpected process group {{pgid}}")
        continue
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
time.sleep(0.2)
residuals = []
for pid, info in token_processes():
    residuals.append({{"pid": pid, "pgid": info["pgid"], "state": info["state"]}})
for resource in resources:
    if resource["outcome"] == "stopping":
        resource["outcome"] = "stopped" if not residuals else "residual"
tmp = metadata.get("tmp")
if isinstance(tmp, str) and tmp.startswith("/tmp/"):
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
cleanup = {{
    "outcome": "passed" if not errors and not residuals else "failed",
    "completed_at": datetime.now(timezone.utc).isoformat(),
    "resources": resources,
    "residual_processes": residuals,
    "errors": errors,
}}
metadata["cleanup"] = cleanup
temporary = metadata_path.with_suffix(".tmp")
temporary.write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
os.replace(temporary, metadata_path)
print(json.dumps(cleanup, sort_keys=True))
if errors or residuals:
    raise SystemExit(2)
__CLIO_RELAY_CLEANUP_PROBE__
"""
    try:
        result = subprocess.run(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input=script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        detail = (
            "remote cleanup command timed out after "
            f"{REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS:g} seconds"
            if isinstance(exc, subprocess.TimeoutExpired)
            else f"remote cleanup command could not start: {exc}"
        )
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail=detail,
        )
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line]) from exc
    if (
        len(result.stdout) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES
        or len(result.stderr) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES
    ):
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail="remote cleanup output exceeded the bounded size",
        )
        error = RelayError(f"remote transport cleanup output was too large for {probe_id}")
        raise _attach_transport_evidence(error, [evidence_line])
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    try:
        cleanup = _RemoteCleanupPayload.model_validate(_last_json_line(stdout))
    except (ValueError, RelayError) as exc:
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail=f"remote cleanup output was invalid: {exc}",
        )
        detail = stderr or stdout or str(exc)
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line]) from exc
    evidence_line = _remote_cleanup_evidence_line(
        cleanup,
        cluster=cluster or definition.name,
        definition=definition,
        probe_id=probe_id,
    )
    if cleanup.outcome == "metadata_missing" and not require_metadata:
        return [evidence_line, "transport.remote_cleanup=not_started"]
    if result.returncode != 0 or cleanup.outcome != "passed":
        detail = stderr or cleanup.error or "; ".join(cleanup.errors) or stdout
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line])
    resource_parts = [
        f"{resource.kind}:{resource.pid}:{resource.outcome}" for resource in cleanup.resources
    ]
    return [
        evidence_line,
        "transport.remote_cleanup=passed",
        f"transport.remote_cleanup_resources={','.join(resource_parts)}",
        f"transport.remote_cleanup_residuals={len(cleanup.residual_processes)}",
        (
            "transport.remote_cleanup_metadata="
            f"~/.local/share/clio-relay/transport-probes/{probe_id}/metadata.json"
        ),
    ]


def _remote_cleanup_evidence_line(
    cleanup: _RemoteCleanupPayload,
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
) -> str:
    detail = cleanup.error or ("; ".join(cleanup.errors) if cleanup.errors else None)
    completed = cleanup.outcome in {"passed", "failed"}
    resources: list[TransportCleanupResourceEvidence] = [
        _process_cleanup_resource(
            kind="relay_session",
            resource_id=f"frp-probe:{probe_id}",
            role="remote_transport_probe_session",
            location=definition.ssh_host,
            ownership_verified=completed,
            outcome="stopped" if cleanup.outcome == "passed" else cleanup.outcome,
            verified_after_operation=cleanup.outcome == "passed",
            observed_state=("stopped" if cleanup.outcome == "passed" else "running_or_unknown"),
            residual=cleanup.outcome != "passed",
            detail=detail,
            metadata={"remote_cleanup_outcome": cleanup.outcome},
        )
    ]
    for resource in cleanup.resources:
        canonical_kind = "connector" if resource.kind == "remote_connector" else "relay_process"
        role = (
            "remote_frpc_connector"
            if resource.kind == "remote_connector"
            else "remote_relay_api_process"
        )
        residual = resource.outcome in {"refused", "residual"}
        verified = resource.outcome in {"missing", "replaced", "stopped"}
        resources.append(
            _process_cleanup_resource(
                kind=canonical_kind,
                resource_id=str(resource.pid),
                role=role,
                location=definition.ssh_host,
                ownership_verified=resource.ownership_verified,
                outcome=resource.outcome,
                verified_after_operation=verified,
                observed_state=resource.outcome,
                residual=residual,
                detail=(
                    f"cleanup {resource.outcome} for {resource.kind} pid {resource.pid}"
                    if resource.outcome not in {"missing", "stopped"}
                    else None
                ),
                metadata={
                    "cleanup_kind": resource.kind,
                    "pid": resource.pid,
                },
            )
        )
    existing = {(resource.kind, resource.resource_id) for resource in resources}
    for residual in cleanup.residual_processes:
        identity = ("relay_process", str(residual.pid))
        if identity in existing:
            continue
        existing.add(identity)
        resources.append(
            _process_cleanup_resource(
                kind="relay_process",
                resource_id=str(residual.pid),
                role="remote_probe_residual_process",
                location=definition.ssh_host,
                ownership_verified=True,
                outcome="residual",
                verified_after_operation=False,
                observed_state=residual.state,
                residual=True,
                detail=f"owned process group {residual.pgid} remained after cleanup",
                metadata={"pid": residual.pid, "pgid": residual.pgid},
            )
        )
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode="transport_probe_teardown",
        resources=resources,
    )


def _unverified_remote_cleanup_evidence_line(
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
    detail: str,
) -> str:
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode="transport_probe_teardown",
        resources=[
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=f"frp-probe:{probe_id}",
                role="remote_transport_probe_session",
                location=definition.ssh_host,
                ownership_verified=False,
                outcome="unknown",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=detail,
            )
        ],
    )


def _wait_for_healthz(url: str, *, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
                last_error = f"status={response.status}"
        except (OSError, urllib.error.URLError) as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RelayError(f"transport health check failed for {url}: {last_error}")


def _last_json_line(output: str) -> dict[str, object]:
    """Return the last JSON object emitted by a cleanup command."""
    if len(output.encode("utf-8")) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES:
        raise RelayError("remote cleanup output exceeded the bounded size")
    for line in reversed(output.splitlines()):
        try:
            value = cast(
                object,
                json.loads(line, parse_constant=_reject_nonfinite_json_constant),
            )
        except (json.JSONDecodeError, RecursionError, ValueError):
            continue
        if isinstance(value, dict):
            return cast(dict[str, object], value)
    raise RelayError("remote cleanup did not emit a bounded JSON object")


def _assert_local_bind_port_available(port: int) -> None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    except OSError as exc:
        raise ConfigurationError(f"local visitor port is already occupied: {port}") from exc


def _require_frp_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay cluster add --frp-server-addr ...`"
    )


def _require_api_token(api_token: str | None) -> str:
    if api_token is None or api_token == "":
        raise ConfigurationError(
            "transport probes require CLIO_RELAY_API_TOKEN for the owned remote API"
        )
    return api_token


def _terminate(process: ManagedProcess) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _process_output_message(process: ManagedProcess, fallback: str) -> str:
    parts: list[str] = []
    for stream_name in ("stdout", "stderr"):
        stream = getattr(process, stream_name, None)
        if stream is None or not hasattr(stream, "read"):
            continue
        output = stream.read()
        if isinstance(output, bytes):
            text = output.decode("utf-8", errors="replace").strip()
        else:
            text = str(output).strip()
        if text:
            parts.append(text)
    return "\n".join(parts) if parts else fallback


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _popen(*args: Any, **kwargs: Any) -> ManagedProcess:
    return subprocess.Popen(*args, **kwargs)


def _probe_id(*, cluster: str, proxy_name: str) -> str:
    safe_cluster = "".join(item if item.isalnum() else "-" for item in cluster).strip("-")
    safe_proxy = "".join(item if item.isalnum() else "-" for item in proxy_name).strip("-")
    return f"{safe_cluster}-{safe_proxy}-{secrets.token_hex(8)}"
