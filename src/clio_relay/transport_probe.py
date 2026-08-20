"""End-to-end frp transport probes for relay HTTP surfaces.

Split (iowarp/clio-relay#231) into a facade plus five owner modules:
``transport_probe_primitives.py`` (the ``ManagedProcess`` protocol, probe
callback type aliases, and small process/health/shell helpers),
``transport_probe_evidence.py`` (structured cleanup-evidence assembly),
``transport_probe_session_lifecycle.py`` (SSH-forward session start/detach/
teardown verification), ``transport_probe_remote_script.py`` (the remote FRP
bootstrap script), and ``transport_probe_remote_cleanup_models.py`` /
``transport_probe_remote_cleanup.py`` (the remote cleanup payload shape and
its token-verified stop-and-report logic).

Five functions stay physically resident here rather than moving with the
rest of their concern: ``run_frp_http_probe``, ``run_frp_direct_http_probe``,
``run_ssh_forward_http_probe``, ``_run_frp_http_probe_with_proxy_type``, and
``_finish_frp_probe_cleanup``. ``tests/test_transport_probe.py`` patches
``clio_relay.transport_probe._wait_for_healthz``,
``clio_relay.transport_probe._cleanup_remote_probe``,
``clio_relay.transport_probe.teardown_remote_session``, and
``clio_relay.transport_probe.detach_remote_session`` directly, expecting the
probe orchestration to pick up the fake the next time it calls the bare
name -- which only happens if that call site's enclosing ``def`` is looked
up dynamically against *this* module's own namespace at call time (the same
"patched where it's looked up" rule ``tests/test_cli_patch_seam.py``
documents for ``cli.py``'s collaborators, worked in the caller-stays-put
direction rather than the forwarder-function one). Every other helper these
five functions call was safe to move: none of them is itself a patched
seam, so importing its real implementation back in by the same name (a
plain ``from clio_relay.transport_probe_X import name``) is enough for a
bare-name call inside one of the five resident functions to keep finding
it.
"""

from __future__ import annotations

import subprocess
import time
from typing import cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import (
    FrpLinkConfig,
    FrpVisitorType,
    HeldFrpVisitor,
    require_frp_server_addr,
)
from clio_relay.frp_link import assert_loopback_port_available as _assert_local_bind_port_available
from clio_relay.relay_host import FrpcConfig, FrpTransportProtocol, render_frpc_config
from clio_relay.session_lifecycle import (
    detach_remote_session,
    start_remote_session_durable,
    teardown_remote_session,
)
from clio_relay.session_start_query import plan_remote_session_start
from clio_relay.transport_probe_evidence import (
    _attach_transport_evidence,
    _process_cleanup_resource,
    _transport_resource_line,
    transport_evidence_lines_from_error,
)
from clio_relay.transport_probe_primitives import (
    HttpCheck,
    ManagedProcess,
    OwnedSessionHttpCheck,
    ProcessFactory,
    _popen,
    _probe_id,
    _process_output_message,
    _require_api_token,
    _terminate,
    _visitor_failure_message,
    _wait_for_healthz,
)
from clio_relay.transport_probe_remote_cleanup import _cleanup_remote_probe
from clio_relay.transport_probe_remote_script import _remote_probe_script
from clio_relay.transport_probe_session_lifecycle import (
    _remote_session_start_not_ready_error,
    _session_lifecycle_evidence_line,
    _unverified_session_evidence_line,
    _verified_session_detach_lines,
    _verified_session_teardown_lines,
)
from clio_relay.validation_report import TransportCleanupResourceEvidence


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
    return _run_frp_http_probe_with_proxy_type(
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
        proxy_type="stcp",
    )


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
    http_check: OwnedSessionHttpCheck | None = None,
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
    start_plan = plan_remote_session_start(
        cluster=cluster,
        definition=definition,
        session_id=session_id,
        remote_api_port=remote_api_port,
        replace=replace_remote,
        require_token=True,
    )
    start_result = start_remote_session_durable(
        definition=definition,
        plan=start_plan,
        api_token=api_token,
    )
    if start_result.state != "ready":
        raise _remote_session_start_not_ready_error(
            result=start_result,
            definition=definition,
        )
    session_generation_id = start_result.session_generation_id
    if (
        session_generation_id is None
        or not start_result.ownership_verified
        or not start_result.recovery_verified
    ):
        raise RelayError("ready remote session start omitted its verified generation identity")
    start_lines = [
        f"session_start_state={start_result.state}",
        f"session_started={start_result.session_id}",
        f"start_operation_id={start_result.start_operation_id}",
        f"session_generation_id={session_generation_id}",
        f"remote_api_port={start_result.remote_api_port}",
    ]
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
            lines.extend(
                http_check(
                    f"http://127.0.0.1:{local_bind_port}",
                    session_id,
                    session_generation_id,
                )
            )
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
    server_addr = require_frp_server_addr(transport.server_addr, cluster)
    _require_api_token(api_token)
    protocol = FrpTransportProtocol(transport.protocol)
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

    lines: list[str] = []
    primary_error: BaseException | None = None
    visitor: HeldFrpVisitor | None = None
    try:
        # The local-visitor half (write the 0600 config, spawn `frpc -c <toml>`,
        # track/terminate it) delegates to the shared substrate (#231 R4) rather
        # than reimplementing it here -- the remote-side script above is the only
        # part of this probe that stays local to transport_probe.py. establish()
        # is inside this try (not before it) so a spawn failure still reaches
        # _finish_frp_probe_cleanup below instead of leaking the remote process
        # (#231 R4 opus review F9).
        visitor = HeldFrpVisitor(
            frpc_bin=frpc_bin,
            config=FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=protocol,
                token=token,
                secret_key=secret_key,
                proxy_name=proxy_name,
            ),
            local_bind_port=local_bind_port,
            visitor_type=cast(FrpVisitorType, proxy_type),
            keep_tunnel_open=proxy_type == "xtcp",
            process_factory=process_factory,
        )
        visitor.establish()

        time.sleep(1)
        if remote.poll() is not None:
            raise RelayError(_process_output_message(remote, "remote transport probe failed"))
        if not visitor.is_alive():
            raise RelayError(_visitor_failure_message(visitor, "local frpc visitor failed"))
        try:
            _wait_for_healthz(
                f"{visitor.base_url}/healthz",
                timeout_seconds=timeout_seconds,
            )
        except RelayError as exc:
            visitor.close()
            _terminate(remote)
            details = [
                str(exc),
                _process_output_message(remote, "remote transport probe still running"),
                _visitor_failure_message(visitor, "local frpc visitor still running"),
            ]
            raise RelayError("\n".join(details)) from exc
        if not visitor.is_alive():
            raise RelayError(_visitor_failure_message(visitor, "local frpc visitor failed"))
        lines = [
            f"transport.cluster={cluster}",
            f"transport.server={server_addr}:{transport.server_port}",
            f"transport.protocol={transport.protocol}",
            f"transport.proxy_type={proxy_type}",
            f"transport.local_url={visitor.base_url}",
            "transport.healthz=ok",
        ]
        if http_check is not None:
            lines.extend(http_check(visitor.base_url))
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
    visitor: HeldFrpVisitor | None,
    remote: ManagedProcess,
    primary_error: BaseException | None,
) -> list[str]:
    """Verify local and remote probe teardown before reporting cleanup success."""
    cleanup_errors: list[str] = []
    cleanup_lines: list[str] = []
    local_stopped = True
    local_detail: str | None = None
    config_cleanup_error: str | None = None
    if visitor is not None:
        local_stopped = False
        try:
            visitor.close()
            local_stopped = not visitor.is_alive()
            if not local_stopped:
                local_detail = "local frpc visitor remains running"
                cleanup_errors.append(local_detail)
            config_cleanup_error = visitor.config_cleanup_error
            if config_cleanup_error is not None:
                # Never folded into local_detail: this is a distinct resource
                # (a leaked plaintext-secret file, not the process) and gets
                # its own ledger entry below (#231 R4 opus review F3).
                cleanup_errors.append(config_cleanup_error)
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
    resources: list[TransportCleanupResourceEvidence] = []
    if visitor is not None:
        # Omitted entirely when no visitor was ever constructed (#231 R5 opus
        # review item R14): with no visitor object, nothing was verified
        # stopped -- reporting "outcome=stopped, verified_after_operation=True"
        # for a resource that never existed would be a fabricated claim, not a
        # residual-secret gap like the config-file entry below.
        resources.append(
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
            )
        )
    resources.append(
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
        )
    )
    if config_cleanup_error is not None:
        # A residual, secret-bearing config file is a distinct resource from
        # the process above: the process can be confirmed stopped while its
        # config directory still failed to delete (#231 R4 opus review F3).
        resources.append(
            _process_cleanup_resource(
                kind="secret_config_file",
                resource_id=f"frpc-visitor-config:{probe_id}",
                role="desktop_frpc_visitor_config",
                location="desktop",
                ownership_verified=True,
                outcome="residual",
                verified_after_operation=False,
                observed_state="residual",
                residual=True,
                detail=config_cleanup_error,
            )
        )
    cleanup_lines.append(
        _transport_resource_line(
            probe_id=probe_id,
            cluster=cluster,
            cleanup_mode="transport_probe_teardown",
            resources=resources,
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
