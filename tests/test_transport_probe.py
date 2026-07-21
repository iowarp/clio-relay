from __future__ import annotations

import socket
import subprocess
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.transport_probe as transport_probe
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.session_lifecycle import (
    CleanupResource,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartStatusSelector,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
)
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
    transport_evidence_lines_from_error,
)
from clio_relay.validation_report import parse_transport_probe_evidence


def _frp_cluster_definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="test-cluster",
        ssh_host="test-host",
        frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
    )


def _ready_owned_session_start_lines(kwargs: dict[str, object]) -> list[str]:
    operation_id = cast(str, kwargs["start_operation_id"])
    session_id = cast(str, kwargs["session_id"])
    remote_api_port = cast(int, kwargs["remote_api_port"])
    assert operation_id.startswith("start_")
    assert kwargs["expected_cluster_route_revision"]
    return [
        f"session_started={session_id}",
        f"start_operation_id={operation_id}",
        "session_generation_id=generation-1",
        f"remote_api_port={remote_api_port}",
        "api_pid=123",
    ]


def test_frp_http_probe_starts_remote_proxy_and_local_visitor(monkeypatch: MonkeyPatch) -> None:
    processes: list[FakeProcess] = []
    health_urls: list[str] = []
    cleanup_probe_ids: list[str] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        processes.append(process)
        return process

    def fake_healthz(url: str, *, timeout_seconds: float) -> None:
        health_urls.append(url)
        assert timeout_seconds == 3

    def fake_cleanup(**kwargs: object) -> list[str]:
        cleanup_probe_ids.append(str(kwargs["probe_id"]))
        return ["transport.remote_cleanup=passed"]

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr("clio_relay.transport_probe._cleanup_remote_probe", fake_cleanup)

    lines = run_frp_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="stcp-secret",
        local_bind_port=9876,
        remote_api_port=8765,
        proxy_name="relay-http-test",
        api_token="api-token",
        timeout_seconds=3,
        process_factory=fake_process_factory,
    )

    assert lines[-1] == "transport.cleanup=passed"
    assert "transport.healthz=ok" in lines
    assert health_urls == ["http://127.0.0.1:9876/healthz"]
    assert processes[0].command == ["ssh", "test-host", "bash", "-s"]
    assert processes[1].command[0] == "frpc"
    remote_script = processes[0].stdin.getvalue().decode("utf-8")
    assert cleanup_probe_ids
    assert f"probe_id='{cleanup_probe_ids[0]}'" in remote_script
    assert "transport-probes/$probe_id" in remote_script
    assert '"owner": "clio-relay"' in remote_script
    assert "CLIO_RELAY_API_TOKEN='api-token'" in remote_script
    assert "clio-relay api start --host 127.0.0.1 --port 8765 --require-token" in remote_script
    assert "remote API port is already occupied: 8765" in remote_script
    assert remote_script.index("remote API port is already occupied") < remote_script.index(
        "clio-relay api start"
    )
    assert "pkill" not in remote_script
    assert 'kill -- "-$api_pid"' in remote_script
    assert 'kill -- "-$frpc_pid"' in remote_script
    assert "CLIO_RELAY_PROBE_OWNER_TOKEN" in remote_script
    assert "process_start_ticks" in remote_script
    assert "pgid == pid" in remote_script
    assert 'name = "relay-http-test"' in remote_script
    assert 'auth.token = "frp-token"' in remote_script


def test_frp_http_probe_requires_configured_relay_host() -> None:
    with pytest.raises(ConfigurationError, match="frp server address is not configured"):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=9876,
        )


def test_frp_http_probe_requires_api_token() -> None:
    with pytest.raises(ConfigurationError, match="CLIO_RELAY_API_TOKEN"):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=9876,
            api_token=None,
            process_factory=_unexpected_process_factory,
        )


def test_frp_http_probe_runs_optional_http_check(monkeypatch: MonkeyPatch) -> None:
    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        return FakeProcess(command)

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30.0

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr(
        transport_probe,
        "_cleanup_remote_probe",
        _verified_remote_cleanup,
    )

    lines = run_frp_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="stcp-secret",
        local_bind_port=9876,
        api_token="api-token",
        process_factory=fake_process_factory,
        http_check=lambda local_url: [f"http_check_url={local_url}", "http_check=ok"],
    )

    assert "http_check_url=http://127.0.0.1:9876" in lines
    assert "http_check=ok" in lines
    assert lines[-1] == "transport.cleanup=passed"


def test_frp_http_probe_surfaces_remote_port_conflict(monkeypatch: MonkeyPatch) -> None:
    processes: list[FakeProcess] = []
    cleanup_calls: list[str] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        if command[:2] == ["ssh", "test-host"]:
            process.returncode = 1
            process.stderr = BytesIO(b"remote API port is already occupied: 8765\n")
        processes.append(process)
        return process

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        del timeout_seconds
        raise AssertionError("health check should not run after remote probe exits")

    def fake_cleanup(**kwargs: object) -> list[str]:
        cleanup_calls.append(str(kwargs["probe_id"]))
        return ["transport.remote_cleanup=not_started"]

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr("clio_relay.transport_probe._cleanup_remote_probe", fake_cleanup)

    with pytest.raises(RelayError, match="remote API port is already occupied: 8765"):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=9876,
            remote_api_port=8765,
            api_token="api-token",
            process_factory=fake_process_factory,
        )

    assert len(processes) == 2
    assert cleanup_calls


def test_remote_probe_cleanup_script_targets_real_proc_paths(monkeypatch: MonkeyPatch) -> None:
    scripts: list[str] = []

    def fake_run(
        _command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        assert timeout == 120
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(
            _command,
            0,
            (
                b'{"outcome":"passed","completed_at":"2026-07-11T12:00:00Z",'
                b'"resources":[{"kind":"remote_relay_api","pid":701,'
                b'"outcome":"stopped","ownership_verified":true},'
                b'{"kind":"remote_connector","pid":702,"outcome":"stopped",'
                b'"ownership_verified":true}],"residual_processes":[],"errors":[]}\n'
            ),
            b"",
        )

    monkeypatch.setattr(transport_probe.subprocess, "run", fake_run)

    cleanup_name = "_cleanup" + "_remote_probe"
    cleanup = getattr(transport_probe, cleanup_name)
    typed_cleanup = cast(Callable[..., list[str]], cleanup)
    lines = typed_cleanup(
        definition=_frp_cluster_definition(),
        probe_id="test-cluster-probe",
    )

    assert scripts
    assert 'proc = Path("/proc") / str(pid)' in scripts[0]
    assert '(proc / "cmdline").read_bytes()' in scripts[0]
    assert 'Path("/proc") / str(pid)' in scripts[0]
    assert "int | None" not in scripts[0]
    assert "/proc/{{pid}}" not in scripts[0]
    assert "transport-probes/$probe_id" in scripts[0]
    assert "owner_token" in scripts[0]
    assert "process_start_ticks" in scripts[0]
    assert "os.killpg" in scripts[0]
    assert 'rm -rf "$probe_dir"' not in scripts[0]
    evidence_line = next(line for line in lines if line.startswith("transport.probe_evidence="))
    evidence = parse_transport_probe_evidence(evidence_line.partition("=")[2])
    assert {(item.kind, item.resource_id) for item in evidence.resources} == {
        ("relay_session", "frp-probe:test-cluster-probe"),
        ("relay_process", "701"),
        ("connector", "702"),
    }
    assert all(item.ownership_verified for item in evidence.resources)
    assert "transport.remote_cleanup=passed" in lines


def test_remote_probe_cleanup_rejects_failed_ssh_cleanup(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        del input
        assert capture_output is True
        assert check is False
        assert timeout == 120
        return subprocess.CompletedProcess(
            command,
            2,
            (
                b'{"outcome":"failed","completed_at":"2026-07-11T12:00:00Z",'
                b'"resources":[{"kind":"remote_connector","pid":733,'
                b'"outcome":"residual","ownership_verified":true}],'
                b'"residual_processes":[{"pid":733,"pgid":733,"state":"S"}],'
                b'"errors":["connector remained running"]}\n'
            ),
            b"",
        )

    monkeypatch.setattr(transport_probe.subprocess, "run", fake_run)

    with pytest.raises(RelayError, match="remote transport cleanup failed") as exc_info:
        transport_probe._cleanup_remote_probe(  # pyright: ignore[reportPrivateUsage]
            definition=_frp_cluster_definition(),
            probe_id="failed-cleanup",
        )
    evidence_line = transport_evidence_lines_from_error(exc_info.value)[0]
    evidence = parse_transport_probe_evidence(evidence_line.partition("=")[2])
    connector = next(item for item in evidence.resources if item.kind == "connector")
    assert connector.resource_id == "733"
    assert connector.ownership_verified is True
    assert connector.outcome == "residual"
    assert connector.observed_state == "residual"
    assert connector.residual is True


def test_remote_probe_cleanup_timeout_preserves_unverified_evidence(
    monkeypatch: MonkeyPatch,
) -> None:
    def timed_out(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        del input, capture_output, check
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(transport_probe.subprocess, "run", timed_out)

    with pytest.raises(RelayError, match="timed out after 120 seconds") as exc_info:
        transport_probe._cleanup_remote_probe(  # pyright: ignore[reportPrivateUsage]
            definition=_frp_cluster_definition(),
            probe_id="timed-out-cleanup",
        )

    evidence_line = transport_evidence_lines_from_error(exc_info.value)[0]
    evidence = parse_transport_probe_evidence(evidence_line.partition("=")[2])
    assert evidence.cleanup_mode == "transport_probe_teardown"
    assert evidence.resources[0].outcome == "unknown"
    assert evidence.resources[0].ownership_verified is False
    assert evidence.resources[0].residual is True


def test_frp_http_probe_rejects_dead_visitor_even_if_local_healthz_passes(
    monkeypatch: MonkeyPatch,
) -> None:
    processes: list[FakeProcess] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        if command and command[0] == "frpc":
            process.returncode = 1
            process.stderr = BytesIO(b"bind: address already in use\n")
        processes.append(process)
        return process

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30.0

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr(
        transport_probe,
        "_cleanup_remote_probe",
        _verified_remote_cleanup,
    )

    with pytest.raises(RelayError, match="address already in use"):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=9876,
            remote_api_port=8765,
            api_token="api-token",
            process_factory=fake_process_factory,
        )

    assert len(processes) == 2


def test_frp_http_probe_rejects_occupied_local_visitor_port() -> None:
    with (
        _occupied_loopback_port() as port,
        pytest.raises(RelayError, match=f"local visitor port is already occupied: {port}"),
    ):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=port,
            remote_api_port=8765,
            process_factory=_unexpected_process_factory,
        )


def test_frp_direct_http_probe_uses_xtcp_proxy_and_visitor(
    monkeypatch: MonkeyPatch,
) -> None:
    processes: list[FakeProcess] = []
    health_urls: list[str] = []
    visitor_configs: list[str] = []
    cleanup_calls: list[str] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        if command and command[0] == "frpc":
            visitor_configs.append(Path(command[-1]).read_text(encoding="utf-8"))
        processes.append(process)
        return process

    def fake_healthz(url: str, *, timeout_seconds: float) -> None:
        health_urls.append(url)
        assert timeout_seconds == 4

    def fake_cleanup(**kwargs: object) -> list[str]:
        cleanup_calls.append(str(kwargs["probe_id"]))
        return ["transport.remote_cleanup=passed"]

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr("clio_relay.transport_probe._cleanup_remote_probe", fake_cleanup)

    lines = run_frp_direct_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="xtcp-secret",
        local_bind_port=9876,
        remote_api_port=8765,
        proxy_name="relay-http-direct-test",
        api_token="api-token",
        timeout_seconds=4,
        process_factory=fake_process_factory,
        allow_stcp_fallback=False,
    )

    assert lines[:3] == [
        "direct_transport.cluster=test-cluster",
        "direct_transport.mode=xtcp",
        "direct_transport.result=xtcp",
    ]
    assert "transport.proxy_type=xtcp" in lines
    assert health_urls == ["http://127.0.0.1:9876/healthz"]
    remote_script = processes[0].stdin.getvalue().decode("utf-8")
    assert cleanup_calls
    assert 'type = "xtcp"' in remote_script
    assert len(visitor_configs) == 1
    assert 'type = "xtcp"' in visitor_configs[0]
    assert "keepTunnelOpen = true" in visitor_configs[0]


def test_frp_direct_http_probe_rejects_occupied_local_visitor_port() -> None:
    with (
        _occupied_loopback_port() as port,
        pytest.raises(RelayError, match=f"local visitor port is already occupied: {port}"),
    ):
        run_frp_direct_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="xtcp-secret",
            local_bind_port=port,
            remote_api_port=8765,
            proxy_name="relay-http-direct-test",
            process_factory=_unexpected_process_factory,
        )


def test_frp_direct_http_probe_reports_stcp_fallback_when_xtcp_fails(
    monkeypatch: MonkeyPatch,
) -> None:
    processes: list[FakeProcess] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        if command and command[0] == "frpc":
            config_text = Path(command[-1]).read_text(encoding="utf-8")
            if 'type = "xtcp"' in config_text:
                process.returncode = 1
                process.stderr = BytesIO(b"xtcp hole punching failed\n")
        processes.append(process)
        return process

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 5

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)
    monkeypatch.setattr(
        transport_probe,
        "_cleanup_remote_probe",
        _verified_remote_cleanup,
    )

    lines = run_frp_direct_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="shared-secret",
        local_bind_port=9876,
        remote_api_port=8765,
        proxy_name="relay-http-direct-test",
        api_token="api-token",
        timeout_seconds=5,
        process_factory=fake_process_factory,
        allow_stcp_fallback=True,
    )

    assert lines[:4] == [
        "direct_transport.cluster=test-cluster",
        "direct_transport.mode=xtcp",
        "direct_transport.result=frp_stcp",
        "direct_transport.xtcp_error=xtcp hole punching failed",
    ]
    assert "transport.healthz=ok" in lines
    assert any(
        "relay-http-direct-test-fallback" in process.stdin.getvalue().decode("utf-8")
        for process in processes
        if process.stdin is not None
    )


def test_ssh_forward_http_probe_starts_owned_remote_api_and_local_forward(
    monkeypatch: MonkeyPatch,
) -> None:
    processes: list[FakeProcess] = []
    teardowns: list[str] = []
    http_bindings: list[tuple[str, str, str]] = []

    def fake_start(**kwargs: object) -> list[str]:
        assert kwargs["session_id"] == "session-1"
        assert kwargs["remote_api_port"] == 9001
        assert kwargs["api_token"] == "token"
        return _ready_owned_session_start_lines(kwargs)

    def fake_teardown(**kwargs: object) -> SessionLifecycleReport:
        teardowns.append(str(kwargs["session_id"]))
        assert kwargs["expected_session_generation_id"] == "generation-1"
        observed_at = datetime.now(UTC)
        return SessionLifecycleReport(
            cluster="test-cluster",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="teardown",
            prior_session_status=RemoteSessionStateEvidence(
                session_generation_id="generation-1",
                running=True,
                ownership_verified=True,
                observed_at=observed_at,
            ),
            post_session_status=RemoteSessionStateEvidence(
                session_generation_id="generation-1",
                running=False,
                ownership_verified=True,
                observed_at=observed_at,
            ),
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="test-host",
                    action="stop",
                    ownership_verified=True,
                    outcome="stopped",
                    verified_after_operation=True,
                )
            ],
        )

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        processes.append(process)
        return process

    def fake_healthz(url: str, *, timeout_seconds: float) -> None:
        assert url == "http://127.0.0.1:19001/healthz"
        assert timeout_seconds == 4

    def fake_http_check(local_url: str, session_id: str, generation_id: str) -> list[str]:
        http_bindings.append((local_url, session_id, generation_id))
        return ["transport.http_binding=verified"]

    monkeypatch.setattr(session_lifecycle, "start_remote_session", fake_start)
    monkeypatch.setattr("clio_relay.transport_probe.teardown_remote_session", fake_teardown)
    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    lines = run_ssh_forward_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        local_bind_port=19001,
        remote_api_port=9001,
        session_id="session-1",
        api_token="token",
        timeout_seconds=4,
        process_factory=fake_process_factory,
        http_check=fake_http_check,
    )

    assert "transport.protocol=ssh_forward" in lines
    assert "transport.healthz=ok" in lines
    assert "transport.http_binding=verified" in lines
    assert "transport.cleanup=passed" in lines
    assert "session_started=session-1" in lines
    assert processes[0].command == [
        "ssh",
        "-N",
        "-L",
        "127.0.0.1:19001:127.0.0.1:9001",
        "test-host",
    ]
    assert teardowns == ["session-1"]
    assert http_bindings == [("http://127.0.0.1:19001", "session-1", "generation-1")]


def test_ssh_forward_http_probe_can_detach_remote_session(monkeypatch: MonkeyPatch) -> None:
    teardowns: list[str] = []
    detaches: list[str] = []

    def fake_start(**_kwargs: object) -> list[str]:
        return _ready_owned_session_start_lines(_kwargs)

    def fake_teardown(**kwargs: object) -> SessionLifecycleReport:
        teardowns.append(str(kwargs["session_id"]))
        return SessionLifecycleReport(
            cluster="test-cluster",
            session_id="session-1",
            mode="teardown",
        )

    def fake_detach(**kwargs: object) -> SessionLifecycleReport:
        detaches.append(str(kwargs["session_id"]))
        return SessionLifecycleReport(
            cluster="test-cluster",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="test-host",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        del timeout_seconds

    def fake_process_factory(command: list[str], **_kwargs: object) -> FakeProcess:
        return FakeProcess(command)

    monkeypatch.setattr(
        session_lifecycle,
        "start_remote_session",
        fake_start,
    )
    monkeypatch.setattr(
        "clio_relay.transport_probe.teardown_remote_session",
        fake_teardown,
    )
    monkeypatch.setattr(
        "clio_relay.transport_probe.detach_remote_session",
        fake_detach,
    )
    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    lines = run_ssh_forward_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        local_bind_port=19001,
        remote_api_port=9001,
        session_id="session-1",
        api_token="token",
        process_factory=fake_process_factory,
        detach_remote=True,
    )

    assert teardowns == []
    assert detaches == ["session-1"]
    assert "transport.cleanup=detached" in lines
    assert "transport.cleanup=passed" not in lines
    assert "transport.remote_session_ownership=verified" in lines


@pytest.mark.parametrize(
    ("expected_state", "attempt_verified"),
    [("starting", True), ("ambiguous", False)],
)
def test_ssh_forward_probe_preserves_nonterminal_start_after_transport_deadline(
    monkeypatch: MonkeyPatch,
    expected_state: str,
    attempt_verified: bool,
) -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-host")
    start_invocations: list[dict[str, object]] = []
    status_selectors: list[OwnedSessionStartStatusSelector] = []

    def deadline(**kwargs: object) -> list[str]:
        start_invocations.append(kwargs)
        raise session_lifecycle._RemoteSessionCommandDeadline(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "start transport deadline"
        )

    def fake_status(
        *,
        definition: ClusterDefinition,
        selector: OwnedSessionStartStatusSelector,
    ) -> OwnedSessionRecoveryStatus:
        assert definition == ClusterDefinition(name="test-cluster", ssh_host="test-host")
        status_selectors.append(selector)
        return OwnedSessionRecoveryStatus(
            cluster=selector.cluster,
            session_id=selector.session_id,
            session_generation_id=("generation-starting" if attempt_verified else None),
            start_operation_id=selector.start_operation_id,
            cluster_route_revision=selector.cluster_route_revision,
            remote_api_port=selector.remote_api_port if attempt_verified else None,
            start_state="starting",
            start_phase="admitted" if attempt_verified else None,
            start_attempt_verified=attempt_verified,
            start_retryable=True,
            start_replace=selector.replace if attempt_verified else None,
            start_require_token=selector.require_token if attempt_verified else None,
            start_expected_api_release_identity_sha256=(
                selector.expected_api_release_identity_sha256 if attempt_verified else None
            ),
            errors=[] if attempt_verified else ["start transition is not yet observable"],
        )

    monkeypatch.setattr(session_lifecycle, "start_remote_session", deadline)
    monkeypatch.setattr(session_lifecycle, "status_remote_session_start", fake_status)

    with pytest.raises(RelayError, match=f"state={expected_state}") as raised:
        run_ssh_forward_http_probe(
            cluster="test-cluster",
            definition=definition,
            local_bind_port=19001,
            remote_api_port=9001,
            session_id="session-1",
            api_token="token",
            process_factory=_unexpected_process_factory,
        )

    assert len(start_invocations) == 1
    assert len(status_selectors) == 1
    operation_id = cast(str, start_invocations[0]["start_operation_id"])
    assert operation_id == status_selectors[0].start_operation_id
    evidence_lines = transport_evidence_lines_from_error(raised.value)
    assert len(evidence_lines) == 1
    evidence = parse_transport_probe_evidence(evidence_lines[0].partition("=")[2])
    assert evidence.cleanup_mode == "transport_probe_start_observation"
    resource = evidence.resources[0]
    assert resource.kind == "relay_session_start_operation"
    assert resource.resource_id == operation_id
    assert resource.action == "retain"
    assert resource.ownership_verified is True
    assert resource.outcome == "retained"
    assert resource.verified_after_operation is True
    assert resource.observed_state == expected_state
    assert resource.residual is False
    assert resource.metadata["terminal"] is False
    assert resource.metadata["retryable"] is True
    assert resource.metadata["transport_deadline_exceeded"] is True
    assert resource.metadata["start_operation_id"] == operation_id
    assert resource.metadata["status_selector"] == status_selectors[0].model_dump(mode="json")
    assert resource.metadata["retry_selector"] == {
        **status_selectors[0].model_dump(mode="json"),
        "operation": "session.start",
    }


def test_ssh_forward_http_probe_rejects_residual_remote_session(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_start(**_kwargs: object) -> list[str]:
        return _ready_owned_session_start_lines(_kwargs)

    def fake_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return SessionLifecycleReport(
            cluster="test-cluster",
            session_id="session-1",
            mode="teardown",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="test-host",
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail="ownership mismatch",
                )
            ],
            errors=["ownership mismatch"],
        )

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        del timeout_seconds

    monkeypatch.setattr(session_lifecycle, "start_remote_session", fake_start)
    monkeypatch.setattr(transport_probe, "teardown_remote_session", fake_teardown)
    monkeypatch.setattr(transport_probe, "_wait_for_healthz", fake_healthz)

    def fake_process_factory(command: list[str], **_kwargs: object) -> FakeProcess:
        return FakeProcess(command)

    with pytest.raises(RelayError, match="remote session cleanup failed"):
        run_ssh_forward_http_probe(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            local_bind_port=19001,
            remote_api_port=9001,
            session_id="session-1",
            api_token="token",
            process_factory=fake_process_factory,
        )


def test_ssh_forward_http_probe_requires_api_token() -> None:
    with pytest.raises(ConfigurationError, match="CLIO_RELAY_API_TOKEN"):
        run_ssh_forward_http_probe(
            cluster="test-cluster",
            definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
            local_bind_port=19001,
            remote_api_port=9001,
            session_id="session-1",
            api_token=None,
            process_factory=_unexpected_process_factory,
        )


@contextmanager
def _occupied_loopback_port() -> Generator[int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        yield int(sock.getsockname()[1])
    finally:
        sock.close()


def _unexpected_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
    raise AssertionError(f"process should not start with occupied local port: {command}")


def _verified_remote_cleanup(**_kwargs: object) -> list[str]:
    return ["transport.remote_cleanup=passed"]


class FakeProcess:
    def __init__(self, command: list[str]) -> None:
        self.command = command
        self.stdin = CapturingBytesIO()
        self.stdout = BytesIO()
        self.stderr = BytesIO()
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.returncode = 0
        return 0


class CapturingBytesIO(BytesIO):
    def close(self) -> None:
        pass
