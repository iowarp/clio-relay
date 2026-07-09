from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pytest
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import GatewaySession, GatewaySessionState, ServiceRuntimeSpec
from clio_relay.service_runtime import (
    CommandRunner,
    ServiceRuntimeSupervisor,
)


class FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid


class FakeRunner(CommandRunner):
    def __init__(self) -> None:
        self.commands: list[list[str]] = []
        self.inputs: list[str | None] = []
        self.popen_commands: list[list[str]] = []
        self.canceled_jobs: list[str] = []

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(command))
        self.inputs.append(input_text)
        script = input_text or ""
        if "remote-frpc.toml" in script and "nohup" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "remote_frpc_pid=444",
                        "remote_frpc_config=/home/user/.local/share/clio-relay/service-sessions/gateway_x/remote-frpc.toml",
                        "remote_frpc_log=/home/user/.local/share/clio-relay/service-sessions/gateway_x/remote-frpc.log",
                    ]
                )
                + "\n",
                "",
            )
        if "jarvis run /remote/service-runtime.yaml" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                '{"scheduler_job_id":"12345","service_host":"compute-01"}\n',
                "",
            )
        if "http.client.HTTPConnection" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                "service_health=ok\nservice_status=200\n",
                "",
            )
        if "remote_frpc_stopped" in script:
            return subprocess.CompletedProcess(command, 0, "remote_frpc_stopped=444\n", "")
        if "jarvis runtime cancel 12345" in script:
            self.canceled_jobs.append("12345")
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 1, "", f"unexpected script: {script}")

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
    ) -> subprocess.Popen[bytes]:
        self.popen_commands.append(list(command))
        stdout_path.write_bytes(b"")
        stderr_path.write_bytes(b"")
        return cast(subprocess.Popen[bytes], FakeProcess(555))


class FailingSubmitRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(command))
        self.inputs.append(input_text)
        return subprocess.CompletedProcess(command, 1, "", "scheduler unavailable")


class UnstructuredSubmitRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(command))
        self.inputs.append(input_text)
        return subprocess.CompletedProcess(command, 0, "deployment accepted as job 67890\n", "")


class DeferredHostRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(command))
        self.inputs.append(input_text)
        script = input_text or ""
        if "remote-frpc.toml" in script and "nohup" in script:
            return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)
        if "jarvis run /remote/service-runtime.yaml" in script:
            return subprocess.CompletedProcess(command, 0, '{"scheduler_job_id":"12345"}\n', "")
        if "jarvis runtime status 12345" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                (
                    '{"state":"allocated","service_host":"compute-02",'
                    '"events":[{"type":"progress","source":"jarvis_package",'
                    '"package":"example_stream","message":"runtime allocated"}]}\n'
                ),
                "",
            )
        if "http.client.HTTPConnection" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                "service_health=ok\nservice_status=200\n",
                "",
            )
        return subprocess.CompletedProcess(command, 1, "", f"unexpected script: {script}")


def test_service_runtime_supervisor_starts_generic_streaming_service(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    definition = _definition()
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    supervisor._wait_for_local_health = lambda *_args: None  # type: ignore[method-assign]

    result = supervisor.start(name="generic-image-service", spec=_runtime_spec())

    session = result.session
    assert session.state == GatewaySessionState.READY
    assert session.scheduler_job_id == "12345"
    assert session.node == "compute-01"
    assert session.gateway["service"]["host"] == "compute-01"
    assert session.gateway["service"]["port"] == 18777
    assert session.gateway["service"]["stream_mode"] == "push"
    assert session.gateway["service"]["stream_path"] == "/live-data"
    assert session.gateway["stream_url"] == "http://127.0.0.1:28777/live-data"
    assert session.gateway["compatibility_urls"] == {
        "snapshot": "http://127.0.0.1:28777/debug/snapshot"
    }
    assert session.gateway["transport"]["mode"] == "frp-stcp-wss"
    assert session.gateway["transport"]["remote_target"] == "compute-01:18777"
    assert session.gateway["transport"]["desktop_bind"] == "127.0.0.1:28777"
    assert session.gateway["connect_url"] == "http://127.0.0.1:28777"
    assert result.health_url == "http://127.0.0.1:28777/healthz"
    assert result.events_url == "http://127.0.0.1:28777/events"
    assert runner.popen_commands == [
        ["frpc-test", "-c", str(_visitor_config_path(settings, session.session_id))]
    ]
    visitor_config = _visitor_config_path(settings, session.session_id).read_text(encoding="utf-8")
    assert 'serverAddr = "frps.example.org"' in visitor_config
    assert 'serverName = "generic-service-proxy"' in visitor_config
    assert "bindPort = 28777" in visitor_config
    remote_scripts = "\n".join(script or "" for script in runner.inputs)
    assert 'localIP = \\"compute-01\\"' not in remote_scripts
    assert "Z2VuZXJpYy" not in remote_scripts


def test_service_runtime_stop_keeps_scheduler_job_by_default(tmp_path: Path) -> None:
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="",
        secret_key="",
        runner=runner,
        sleep=lambda _seconds: None,
    )

    result = supervisor.stop(session_id=session_id)

    assert result.session.state == GatewaySessionState.CLOSED
    assert result.canceled_scheduler_job is None
    assert runner.canceled_jobs == []


def test_service_runtime_stop_can_cancel_scheduler_job_explicitly(tmp_path: Path) -> None:
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="",
        secret_key="",
        runner=runner,
        sleep=lambda _seconds: None,
    )

    result = supervisor.stop(session_id=session_id, cancel_scheduler_job=True)

    assert result.canceled_scheduler_job == "12345"
    assert runner.canceled_jobs == ["12345"]


def test_service_runtime_failed_start_records_failed_session(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=FailingSubmitRunner(),
        sleep=lambda _seconds: None,
    )

    with pytest.raises(Exception, match="scheduler unavailable"):
        supervisor.start(name="generic-image-service", spec=_runtime_spec())

    sessions = queue.list_gateway_sessions(cluster="test-cluster")
    assert len(sessions) == 1
    assert sessions[0].state == GatewaySessionState.FAILED
    assert sessions[0].metadata["last_error"] == (
        "remote service runtime command failed: scheduler unavailable"
    )


def test_service_runtime_supports_xtcp_transport_mode(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    supervisor._wait_for_local_health = lambda *_args: None  # type: ignore[method-assign]
    spec = _runtime_spec().model_copy(update={"transport_mode": "frp-xtcp-wss"})

    result = supervisor.start(name="generic-direct-image-service", spec=spec)

    assert result.session.gateway["transport"]["mode"] == "frp-xtcp-wss"
    visitor_config = _visitor_config_path(settings, result.session.session_id).read_text(
        encoding="utf-8"
    )
    assert 'type = "xtcp"' in visitor_config
    assert "keepTunnelOpen = true" in visitor_config


def test_service_runtime_uses_package_status_command_for_deferred_service_host(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = DeferredHostRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    supervisor._wait_for_local_health = lambda *_args: None  # type: ignore[method-assign]
    spec = _runtime_spec().model_copy(
        update={"status_command": ["jarvis", "runtime", "status", "{scheduler_job_id}"]}
    )

    result = supervisor.start(name="generic-image-service", spec=spec)

    assert result.session.node == "compute-02"
    assert result.session.gateway["runtime_events"] == [
        {
            "type": "progress",
            "source": "jarvis_package",
            "package": "example_stream",
            "message": "runtime allocated",
        }
    ]
    scripts = "\n".join(script or "" for script in runner.inputs)
    assert "jarvis runtime status 12345" in scripts


def test_service_runtime_requires_status_command_for_deferred_service_host(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    runner = DeferredHostRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )

    with pytest.raises(Exception, match="status_command is required"):
        supervisor.start(name="generic-image-service", spec=_runtime_spec())


def test_service_runtime_rejects_unstructured_scheduler_output(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=UnstructuredSubmitRunner(),
        sleep=lambda _seconds: None,
    )

    with pytest.raises(Exception, match="deployment output must include a JSON object"):
        supervisor.start(name="generic-image-service", spec=_runtime_spec())


def test_gateway_start_runtime_cli_uses_service_runtime_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path = tmp_path / "runtime.json"
    spec_path.write_text(_runtime_spec().model_dump_json(), encoding="utf-8")
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        '{"clusters":{"test-cluster":' + _definition().model_dump_json() + "}}",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("CLIO_RELAY_FRPC_BIN", "frpc-test")
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def fake_start(
        self: ServiceRuntimeSupervisor,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
    ) -> object:
        session = self.queue.create_gateway_session(
            gateway_session_for_cli(cluster="test-cluster", name=name, spec=spec)
        )

        class Result:
            def __init__(self) -> None:
                self.session = session

        return Result()

    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", fake_start)
    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "start-runtime",
            "--cluster",
            "test-cluster",
            "--name",
            "generic-image-service",
            "--runtime-json-file",
            str(spec_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"runtime_kind": "image-service"' in result.output


def gateway_session_for_cli(
    *,
    cluster: str,
    name: str,
    spec: ServiceRuntimeSpec,
) -> GatewaySession:
    return GatewaySession(
        cluster=cluster,
        name=name,
        state=GatewaySessionState.READY,
        scheduler=spec.scheduler,
        scheduler_job_id="12345",
        node="compute-01",
        gateway={"runtime_spec": spec.model_dump(mode="json")},
        metadata={"runtime_kind": spec.kind},
    )


def _definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="test-cluster",
        ssh_host="test-login",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="frps.example.org",
            server_port=443,
        ),
    )


def _runtime_spec() -> ServiceRuntimeSpec:
    return ServiceRuntimeSpec(
        kind="image-service",
        deployment_driver="jarvis",
        submit_command=[
            "jarvis",
            "run",
            "/remote/service-runtime.yaml",
            "--set",
            "RELAY_APPLICATION_PORT=18777",
        ],
        cancel_command=["jarvis", "runtime", "cancel", "{scheduler_job_id}"],
        service_port=18777,
        stream_path="/live-data",
        compatibility_paths={"snapshot": "/debug/snapshot"},
        desktop_bind_port=28777,
        proxy_name="generic-service-proxy",
        metadata={"dataset_id": "generic-dataset"},
    )


def _visitor_config_path(settings: RelaySettings, session_id: str) -> Path:
    return settings.core_dir.parent / "runtime-sessions" / session_id / "desktop-frpc.toml"


def _started_session(
    tmp_path: Path,
) -> tuple[ClioCoreQueue, RelaySettings, ClusterDefinition, FakeRunner, str]:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    definition = _definition()
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    supervisor._wait_for_local_health = lambda *_args: None  # type: ignore[method-assign]
    result = supervisor.start(name="generic-image-service", spec=_runtime_spec())
    return queue, settings, definition, runner, result.session.session_id
