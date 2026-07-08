from __future__ import annotations

import socket
import subprocess
from collections.abc import Callable, Generator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

import clio_relay.transport_probe as transport_probe
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
)


def _frp_cluster_definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="test-cluster",
        ssh_host="test-host",
        frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
    )


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

    def fake_cleanup(**kwargs: object) -> None:
        cleanup_probe_ids.append(str(kwargs["probe_id"]))

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

    assert lines[-1] == "transport.healthz=ok"
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
    assert 'kill "$api_pid"' in remote_script
    assert 'kill "$frpc_pid"' in remote_script
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


def test_frp_http_probe_runs_optional_http_check(monkeypatch: MonkeyPatch) -> None:
    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        return FakeProcess(command)

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30.0

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    lines = run_frp_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="stcp-secret",
        local_bind_port=9876,
        process_factory=fake_process_factory,
        http_check=lambda local_url: [f"http_check_url={local_url}", "http_check=ok"],
    )

    assert lines[-2:] == ["http_check_url=http://127.0.0.1:9876", "http_check=ok"]


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

    def fake_cleanup(**kwargs: object) -> None:
        cleanup_calls.append(str(kwargs["probe_id"]))

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
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(_command, 0, b"", b"")

    monkeypatch.setattr(transport_probe.subprocess, "run", fake_run)

    cleanup_name = "_cleanup" + "_remote_probe"
    cleanup = getattr(transport_probe, cleanup_name)
    typed_cleanup = cast(Callable[..., None], cleanup)
    typed_cleanup(
        definition=_frp_cluster_definition(),
        probe_id="test-cluster-probe",
    )

    assert scripts
    assert 'Path("/proc") / str(pid) / "cmdline"' in scripts[0]
    assert 'Path("/proc") / str(pid)' in scripts[0]
    assert "int | None" not in scripts[0]
    assert "/proc/{{pid}}" not in scripts[0]
    assert "transport-probes/$probe_id" in scripts[0]


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

    with pytest.raises(RelayError, match="address already in use"):
        run_frp_http_probe(
            cluster="test-cluster",
            definition=_frp_cluster_definition(),
            frpc_bin="frpc",
            token="frp-token",
            secret_key="stcp-secret",
            local_bind_port=9876,
            remote_api_port=8765,
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

    def fake_cleanup(**kwargs: object) -> None:
        cleanup_calls.append(str(kwargs["probe_id"]))

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

    lines = run_frp_direct_http_probe(
        cluster="test-cluster",
        definition=_frp_cluster_definition(),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="shared-secret",
        local_bind_port=9876,
        remote_api_port=8765,
        proxy_name="relay-http-direct-test",
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

    def fake_start(**kwargs: object) -> list[str]:
        assert kwargs["session_id"] == "session-1"
        assert kwargs["remote_api_port"] == 9001
        return ["session_started=session-1", "api_pid=123"]

    def fake_teardown(**kwargs: object) -> list[str]:
        teardowns.append(str(kwargs["session_id"]))
        return ["session_teardown=session-1"]

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        processes.append(process)
        return process

    def fake_healthz(url: str, *, timeout_seconds: float) -> None:
        assert url == "http://127.0.0.1:19001/healthz"
        assert timeout_seconds == 4

    monkeypatch.setattr("clio_relay.transport_probe.start_remote_session", fake_start)
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
    )

    assert "transport.protocol=ssh_forward" in lines
    assert "transport.healthz=ok" in lines
    assert "session_started=session-1" in lines
    assert processes[0].command == [
        "ssh",
        "-N",
        "-L",
        "127.0.0.1:19001:127.0.0.1:9001",
        "test-host",
    ]
    assert teardowns == ["session-1"]


def test_ssh_forward_http_probe_can_detach_remote_session(monkeypatch: MonkeyPatch) -> None:
    teardowns: list[str] = []

    def fake_start(**_kwargs: object) -> list[str]:
        return ["session_started=session-1"]

    def fake_teardown(**kwargs: object) -> list[str]:
        teardowns.append(str(kwargs["session_id"]))
        return ["session_teardown=session-1"]

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        del timeout_seconds

    def fake_process_factory(command: list[str], **_kwargs: object) -> FakeProcess:
        return FakeProcess(command)

    monkeypatch.setattr(
        "clio_relay.transport_probe.start_remote_session",
        fake_start,
    )
    monkeypatch.setattr(
        "clio_relay.transport_probe.teardown_remote_session",
        fake_teardown,
    )
    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    run_ssh_forward_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        local_bind_port=19001,
        remote_api_port=9001,
        session_id="session-1",
        process_factory=fake_process_factory,
        detach_remote=True,
    )

    assert teardowns == []


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
