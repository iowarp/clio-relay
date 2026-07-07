from __future__ import annotations

from io import BytesIO
from typing import Any

from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.transport_probe import run_frp_http_probe


def test_frp_http_probe_starts_remote_proxy_and_local_visitor(monkeypatch: MonkeyPatch) -> None:
    processes: list[FakeProcess] = []
    health_urls: list[str] = []

    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        process = FakeProcess(command)
        processes.append(process)
        return process

    def fake_healthz(url: str, *, timeout_seconds: float) -> None:
        health_urls.append(url)
        assert timeout_seconds == 3

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    lines = run_frp_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
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
    assert "CLIO_RELAY_API_TOKEN='api-token'" in remote_script
    assert "clio-relay api start --host 127.0.0.1 --port 8765 --require-token" in remote_script
    assert "pkill" not in remote_script
    assert 'name = "relay-http-test"' in remote_script
    assert 'auth.token = "frp-token"' in remote_script


def test_frp_http_probe_runs_optional_http_check(monkeypatch: MonkeyPatch) -> None:
    def fake_process_factory(command: list[str], **_kwargs: Any) -> FakeProcess:
        return FakeProcess(command)

    def fake_healthz(_url: str, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30.0

    monkeypatch.setattr("clio_relay.transport_probe._wait_for_healthz", fake_healthz)

    lines = run_frp_http_probe(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
        frpc_bin="frpc",
        token="frp-token",
        secret_key="stcp-secret",
        local_bind_port=9876,
        process_factory=fake_process_factory,
        http_check=lambda local_url: [f"http_check_url={local_url}", "http_check=ok"],
    )

    assert lines[-2:] == ["http_check_url=http://127.0.0.1:9876", "http_check=ok"]


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
