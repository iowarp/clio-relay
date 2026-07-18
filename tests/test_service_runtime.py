from __future__ import annotations

import json
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from typer.testing import CliRunner

import clio_relay.service_runtime as service_runtime
from clio_relay import cli as relay_cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, FrpTransportConfig
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
)
from clio_relay.service_runtime import (
    CommandRunner,
    LocalConnectorIdentity,
    ServiceRuntimeStartResult,
    ServiceRuntimeStopResult,
    ServiceRuntimeSupervisor,
)
from clio_relay.session_lifecycle import CleanupResource
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
)
from tests.gateway_ownership_crash_fixture import (
    DurableFixtureRunner,
)
from tests.gateway_ownership_crash_fixture import (
    definition as crash_fixture_definition,
)


class FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid


def test_local_connector_does_not_retain_captured_cli_pipes(tmp_path: Path) -> None:
    """A long-lived connector grandchild must not keep its captured CLI parent open."""

    started_path = tmp_path / "grandchild-started"
    stop_path = tmp_path / "grandchild-stop"
    stopped_path = tmp_path / "grandchild-stopped"
    stdout_path = tmp_path / "connector.out"
    stderr_path = tmp_path / "connector.err"
    grandchild = "\n".join(
        (
            "import os",
            "import sys",
            "import time",
            "from pathlib import Path",
            "started, stop, stopped = map(Path, sys.argv[1:4])",
            "started.write_text(str(os.getpid()), encoding='utf-8')",
            "deadline = time.monotonic() + 10.0",
            "while not stop.exists() and time.monotonic() < deadline:",
            "    time.sleep(0.05)",
            "stopped.write_text('stopped', encoding='utf-8')",
        )
    )
    captured_cli = "\n".join(
        (
            "import sys",
            "from pathlib import Path",
            "from clio_relay.service_runtime import SubprocessCommandRunner",
            "started, stop, stopped, stdout, stderr = map(Path, sys.argv[1:6])",
            "process = SubprocessCommandRunner().popen(",
            "    [sys.executable, '-c', sys.argv[6], str(started), str(stop), str(stopped)],",
            "    stdout_path=stdout,",
            "    stderr_path=stderr,",
            ")",
            "print(process.pid, flush=True)",
        )
    )
    cli = subprocess.Popen(
        [
            sys.executable,
            "-c",
            captured_cli,
            str(started_path),
            str(stop_path),
            str(stopped_path),
            str(stdout_path),
            str(stderr_path),
            grandchild,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    timed_out = False
    stdout = ""
    stderr = ""
    try:
        try:
            stdout, stderr = cli.communicate(timeout=2.0)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_path.touch()
            stdout, stderr = cli.communicate(timeout=5.0)

        assert not timed_out, (
            "captured relay CLI waited for its long-lived connector grandchild to exit"
        )
        assert cli.returncode == 0, stderr
        assert stdout.strip().isdigit()
        deadline = time.monotonic() + 2.0
        while not started_path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert started_path.is_file()
        assert not stopped_path.exists()
    finally:
        stop_path.touch()
        deadline = time.monotonic() + 2.0
        while not stopped_path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        if cli.poll() is None:
            cli.kill()
            cli.wait(timeout=2.0)


def _script_assignment(script: str, name: str) -> str:
    prefix = f"{name}="
    for line in script.splitlines():
        if line.startswith(prefix):
            values = shlex.split(line.removeprefix(prefix))
            if len(values) == 1:
                return values[0]
    raise AssertionError(f"generated script has no exact {name} assignment")


def test_detached_remote_connector_closes_transition_lock_descriptor() -> None:
    """A long-lived frpc child must not inherit the connector transition lock."""

    script = service_runtime._remote_frpc_start_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition=_definition(),
        session_id="gateway_test",
        config_text="serverAddr = 'relay.example.org'\n",
        owner_token="owner-token",
        connector_generation_id="generation-1",
    )

    assert "flock -w 10 -x 9" in script
    assert "nohup setsid env" in script
    assert '>"$log_file" 2>&1 9>&- &' in script


@pytest.fixture(autouse=True)
def _fake_connector_process_absent(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def process_absent(_pid: int) -> None:
        return None

    monkeypatch.setattr(service_runtime, "_observe_local_process", process_absent)


class FakeRunner(CommandRunner):
    def __init__(self) -> None:
        self.commands: list[list[str]] = []
        self.inputs: list[str | None] = []
        self.popen_commands: list[list[str]] = []
        self.popen_environments: list[dict[str, str] | None] = []
        self.isolated_processes: list[bool] = []
        self.canceled_jobs: list[str] = []
        self.provider_canceled_jobs: list[str] = []
        self.submission_record: dict[str, object] | None = None

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
        if "__CLIO_READ_SUBMISSION__" in script:
            record = self.submission_record or {"present": False}
            return subprocess.CompletedProcess(command, 0, json.dumps(record) + "\n", "")
        if "remote-frpc.toml" in script and "nohup" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "remote_frpc_pid=444",
                        "remote_frpc_pgid=444",
                        "connector_generation_id=generation-444",
                        "remote_frpc_config=/home/user/.local/share/clio-relay/service-sessions/gateway_x/remote-frpc.toml",
                        "remote_frpc_log=/home/user/.local/share/clio-relay/service-sessions/gateway_x/remote-frpc.log",
                    ]
                )
                + "\n",
                "",
            )
        if "__CLIO_CAPTURE_SUBMISSION__" in script:
            output = '{"scheduler_job_id":"12345","service_host":"compute-01"}\n'
            self.submission_record = {
                "schema_version": "clio-relay.gateway-submission-sidecar.v1",
                "present": True,
                "session_id": _script_assignment(script, "session_id"),
                "submission_id": _script_assignment(script, "submission_id"),
                "scheduler_provider": _script_assignment(script, "scheduler_provider"),
                "submission_marker": _script_assignment(script, "submission_marker"),
                "returncode": 0,
                "output": output,
                "output_truncated": False,
            }
            return subprocess.CompletedProcess(
                command,
                0,
                output,
                "",
            )
        if "http.client.HTTPConnection" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                "service_health=ok\nservice_status=200\n",
                "",
            )
        if "__CLIO_STOP_CONNECTOR__" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                (
                    '{"pid":444,"outcome":"stopped","ownership_verified":true,'
                    '"verified_after_operation":true,"residual":false,'
                    '"remaining_pids":[]}\n'
                ),
                "",
            )
        if "__CLIO_CONNECTOR_STATUS__" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                ('{"pid":444,"ownership_verified":true,"running":true,"matching_pids":[444]}\n'),
                "",
            )
        if "jarvis runtime cancel 12345" in script:
            self.canceled_jobs.append("12345")
            return subprocess.CompletedProcess(command, 0, "", "")
        if "jarvis runtime status 12345" in script:
            state = "canceled" if self.canceled_jobs else "running"
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps({"state": state, "service_host": "compute-01"}) + "\n",
                "",
            )
        if "clio-relay scheduler cancel 12345" in script:
            self.provider_canceled_jobs.append("12345")
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "scheduler": "slurm",
                        "scheduler_job_id": "12345",
                        "cancel_requested": True,
                        "accepted": True,
                        "returncode": 0,
                        "stdout": "",
                        "stderr": "",
                    },
                    indent=2,
                )
                + "\n",
                "",
            )
        if "clio-relay scheduler status 12345" in script:
            state = "canceled" if self.provider_canceled_jobs else "running"
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "scheduler": "slurm",
                        "scheduler_job_id": "12345",
                        "phase": state,
                        "raw_state": state.upper(),
                    },
                    indent=2,
                )
                + "\n",
                "",
            )
        return subprocess.CompletedProcess(command, 1, "", f"unexpected script: {script}")

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
        env: dict[str, str] | None = None,
        isolate_process_group: bool = False,
    ) -> subprocess.Popen[bytes]:
        self.popen_commands.append(list(command))
        self.popen_environments.append(env)
        self.isolated_processes.append(isolate_process_group)
        stdout_path.write_bytes(b"")
        stderr_path.write_bytes(b"")
        return cast(subprocess.Popen[bytes], FakeProcess(555))

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> LocalConnectorIdentity:
        assert expected_config.endswith("desktop-frpc.toml")
        return LocalConnectorIdentity(
            pid=pid,
            process_group_id=pid,
            process_start_marker=f"start-{pid}",
            owner_token=owner_token,
        )


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


class RetryableConnectorRunner(FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.stop_attempts = 0

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if "__CLIO_STOP_CONNECTOR__" in (input_text or ""):
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                return subprocess.CompletedProcess(command, 1, "", "ownership proof failed")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class RetryableCancellationRunner(FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.confirm_terminal = False

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "jarvis runtime status 12345" in script:
            state = "canceled" if self.confirm_terminal else "running"
            return subprocess.CompletedProcess(command, 0, json.dumps({"state": state}), "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class NaturalCompletionCancellationRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "jarvis runtime status 12345" in script and self.canceled_jobs:
            return subprocess.CompletedProcess(command, 0, '{"state":"completed"}\n', "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class UnknownRetentionRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if "jarvis runtime status 12345" in (input_text or ""):
            return subprocess.CompletedProcess(command, 0, '{"state":"unknown"}\n', "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class CompletedRetentionRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if "jarvis runtime status 12345" in (input_text or ""):
            return subprocess.CompletedProcess(command, 0, '{"state":"completed"}\n', "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class InvalidRetentionRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if "jarvis runtime status 12345" in (input_text or ""):
            return subprocess.CompletedProcess(command, 0, '{"state":"banana"}\n', "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class ProviderRetentionRunner(FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.retention_status: SchedulerStatus | None = None

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if self.retention_status is not None and "clio-relay scheduler status 12345" in (
            input_text or ""
        ):
            self.commands.append(list(command))
            self.inputs.append(input_text)
            return subprocess.CompletedProcess(
                command,
                0,
                self.retention_status.model_dump_json(indent=2) + "\n",
                "",
            )
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class AlreadyCanceledRetryRunner(FakeRunner):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "jarvis runtime cancel 12345" in script:
            return subprocess.CompletedProcess(command, 1, "", "job is already canceled")
        if "jarvis runtime status 12345" in script:
            return subprocess.CompletedProcess(command, 0, '{"state":"canceled"}\n', "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


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
        if "__CLIO_CAPTURE_SUBMISSION__" in script:
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
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
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
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    result = supervisor.start(
        name="generic-image-service",
        spec=_runtime_spec(),
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    session = result.session
    assert session.state == GatewaySessionState.READY
    assert session.scheduler_job_id == "12345"
    assert session.node == "compute-01"
    assert session.metadata["owner"] == "clio-relay"
    assert session.metadata["owner_session_id"] == "desktop-session-1"
    assert session.metadata["owner_session_generation_id"] == "generation-1"
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
    assert runner.popen_commands[0][-3:] == [
        "frpc-test",
        "-c",
        str(_visitor_config_path(settings, session.session_id)),
    ]
    assert runner.isolated_processes == [True]
    connector = session.gateway["transport"]["desktop_connector"]
    assert connector["process_group_id"] == 555
    assert connector["process_start_marker"] == "start-555"
    assert connector["owner_token"]
    assert runner.popen_environments[0] is not None
    assert (
        runner.popen_environments[0]["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] == connector["owner_token"]
    )
    visitor_config = _visitor_config_path(settings, session.session_id).read_text(encoding="utf-8")
    assert 'serverAddr = "frps.example.org"' in visitor_config
    assert 'serverName = "generic-service-proxy"' in visitor_config
    assert "bindPort = 28777" in visitor_config
    remote_scripts = "\n".join(script or "" for script in runner.inputs)
    assert 'localIP = \\"compute-01\\"' not in remote_scripts
    assert "Z2VuZXJpYy" not in remote_scripts
    assert "refusing to replace an active remote connector without complete ownership proof" in (
        remote_scripts
    )
    assert 'kill -0 -- "-$pid"' in remote_scripts
    assert "incomplete remote connector process group cleanup" in remote_scripts


@pytest.mark.parametrize("mode", ["scheduler", "scheduler_output", "remote", "local"])
def test_gateway_ownership_hard_exit_reconciles_before_closure(
    tmp_path: Path,
    mode: str,
) -> None:
    state_path = tmp_path / "remote-state.json"
    crashed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.gateway_ownership_crash_fixture",
            str(tmp_path),
            str(state_path),
            mode,
        ],
        cwd=Path(__file__).parents[1],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert crashed.returncode == 84, crashed.stderr
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="fixture-frpc",
    )
    queue = ClioCoreQueue(settings.core_dir)
    sessions = queue.list_gateway_sessions(cluster="configured-target")
    assert len(sessions) == 1
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="configured-target",
        definition=crash_fixture_definition(),
        token="token",
        secret_key="secret",
        runner=DurableFixtureRunner(state_path),
        sleep=lambda _seconds: None,
    )

    result = supervisor.stop(session_id=sessions[0].session_id)

    assert result.session.state is GatewaySessionState.CLOSED
    assert result.errors == []
    assert result.residual_resources == []
    assert result.session.scheduler_job_id == "fixture-job"
    intents = result.session.gateway["ownership_intents"]
    assert intents["scheduler_submission"]["state"] == "recorded"
    if mode in {"remote", "local"}:
        assert intents["remote_connector"]["state"] == "recorded"
    if mode == "local":
        assert intents["desktop_connector"]["state"] == "recorded"


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
    scheduler_resources = [item for item in result.resources if item.kind == "scheduler_job"]
    assert scheduler_resources[0].action == "retain"
    assert scheduler_resources[0].outcome == "retained"
    assert scheduler_resources[0].ownership_verified is True
    assert scheduler_resources[0].verified_after_operation is True
    assert scheduler_resources[0].observed_state == "running"
    assert result.residual_resources == []
    cleanup = result.to_cleanup_evidence()
    assert cleanup.mode == "teardown"
    assert cleanup.cancel_scheduler_jobs is False
    assert {resource.kind for resource in result.validation_resources()} >= {
        "gateway_session",
        "scheduler_job",
    }
    assert result.json_payload()["cleanup_evidence"] == cleanup.model_dump(mode="json")
    stop_script = next(
        script or "" for script in runner.inputs if "__CLIO_STOP_CONNECTOR__" in (script or "")
    )
    stop_program = stop_script.split("<<'__CLIO_STOP_CONNECTOR__'\n", 1)[1].split(
        "\n__CLIO_STOP_CONNECTOR__",
        1,
    )[0]
    compile(stop_program, "remote-connector-stop", "exec")
    owned_scan = stop_script.split("def owned_group_processes():", 1)[1].split("proc = Path", 1)[0]
    assert "process_group != pgid" not in owned_scan
    assert "proc.stat().st_uid != os.geteuid()" in stop_script
    assert "os.killpg" not in stop_script
    assert "os.pidfd_open(member_pid, 0)" in stop_script
    assert "signal.pidfd_send_signal(process_fd, sig, None, 0)" in stop_script
    assert "signal_owned_processes(signal.SIGTERM)" in stop_script
    assert "signal_owned_processes(signal.SIGKILL)" in stop_script

    incomplete = ServiceRuntimeStopResult(
        session=result.session,
        mode="teardown",
        stopped_local_pid=result.stopped_local_pid,
        stopped_remote_pid=None,
        canceled_scheduler_job=None,
        resources=[
            resource for resource in result.resources if resource.kind != "remote_connector"
        ],
        errors=[],
    )
    incomplete_report = incomplete.to_live_validation_report()
    connector_check = next(
        check for check in incomplete_report.checks if check.check_id == "gateway.stop-connectors"
    )
    assert connector_check.status is ValidationStatus.FAILED

    duplicate_scheduler = ServiceRuntimeStopResult(
        session=result.session,
        mode="teardown",
        stopped_local_pid=result.stopped_local_pid,
        stopped_remote_pid=result.stopped_remote_pid,
        canceled_scheduler_job=None,
        resources=[
            *result.resources,
            scheduler_resources[0].model_copy(update={"resource_id": "unexpected-job"}),
        ],
        errors=[],
    ).to_live_validation_report()
    scheduler_check = next(
        check
        for check in duplicate_scheduler.checks
        if check.check_id == "gateway.jobs-preserved-default"
    )
    assert scheduler_check.status is ValidationStatus.FAILED


def test_service_runtime_stop_rehydrates_cleanup_evidence_after_closed_record(
    tmp_path: Path,
) -> None:
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

    first = supervisor.stop(session_id=session_id)
    retried = supervisor.stop(session_id=session_id)

    assert first.session.state is GatewaySessionState.CLOSED
    assert retried.session.state is GatewaySessionState.CLOSED
    assert retried.errors == []
    assert retried.residual_resources == []
    assert {resource.kind for resource in retried.resources} == {
        "desktop_connector",
        "remote_connector",
        "scheduler_job",
        "gateway_record",
    }
    assert retried.to_live_validation_report().status is ValidationStatus.PASSED


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
    scheduler_resources = [item for item in result.resources if item.kind == "scheduler_job"]
    assert scheduler_resources[0].action == "cancel"
    assert scheduler_resources[0].outcome == "canceled"
    assert scheduler_resources[0].verified_after_operation is True
    assert scheduler_resources[0].observed_state == "canceled"
    assert result.to_cleanup_evidence().cancel_scheduler_jobs is True


def test_service_runtime_scheduler_cancel_retry_accepts_already_canceled_state(
    tmp_path: Path,
) -> None:
    retry_runner = AlreadyCanceledRetryRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=retry_runner,
    )
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    assert result.session.state is GatewaySessionState.CLOSED
    assert result.canceled_scheduler_job == "12345"
    assert result.errors == []
    assert scheduler.outcome == "canceled"
    assert scheduler.observed_state == "canceled"
    assert "repeated cancel request returned an error" in (scheduler.detail or "")


def test_service_runtime_uses_explicit_slurm_provider_for_tracking_and_cancel(
    tmp_path: Path,
) -> None:
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    queue, settings, definition, runner, session_id = _started_session(tmp_path, spec=spec)
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
    assert runner.provider_canceled_jobs == ["12345"]
    assert runner.canceled_jobs == []
    provider_scripts = [
        script or "" for script in runner.inputs if "clio-relay scheduler" in (script or "")
    ]
    assert any("clio-relay scheduler status 12345" in script for script in provider_scripts)
    assert any("clio-relay scheduler cancel 12345" in script for script in provider_scripts)
    scheduler_resource = next(item for item in result.resources if item.kind == "scheduler_job")
    assert scheduler_resource.provider == "slurm"
    assert scheduler_resource.observed_state == "canceled"
    assert scheduler_resource.verified_after_operation is True


def test_service_runtime_keep_scheduler_accepts_provider_proven_no_active_record(
    tmp_path: Path,
) -> None:
    runner = ProviderRetentionRunner()
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    queue, settings, definition, _runner, session_id = _started_session(
        tmp_path,
        runner=runner,
        spec=spec,
    )
    runner.retention_status = SchedulerStatus(
        scheduler="slurm",
        scheduler_job_id="12345",
        phase=SchedulerPhase.UNKNOWN,
        record_found=False,
        active_record_found=False,
    )
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    report = result.to_live_validation_report()
    retention_check = next(
        check for check in report.checks if check.check_id == "gateway.jobs-preserved-default"
    )
    assert result.session.state is GatewaySessionState.CLOSED
    assert result.errors == []
    assert result.canceled_scheduler_job is None
    assert runner.provider_canceled_jobs == []
    assert scheduler.action == "retain"
    assert scheduler.outcome == "missing"
    assert scheduler.ownership_verified is True
    assert scheduler.verified_after_operation is True
    assert scheduler.observed_state == "missing"
    assert scheduler.residual is False
    scheduler_status = cast(dict[str, object], scheduler.metadata["scheduler_status"])
    assert scheduler_status["phase"] == "unknown"
    assert scheduler_status["active_record_found"] is False
    assert "no completed or canceled state is claimed" in (scheduler.detail or "")
    assert retention_check.status is ValidationStatus.PASSED
    assert report.status is ValidationStatus.PASSED


def test_service_runtime_keep_scheduler_rejects_ambiguous_unknown_provider_state(
    tmp_path: Path,
) -> None:
    runner = ProviderRetentionRunner()
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    queue, settings, definition, _runner, session_id = _started_session(
        tmp_path,
        runner=runner,
        spec=spec,
    )
    runner.retention_status = SchedulerStatus(
        scheduler="slurm",
        scheduler_job_id="12345",
        phase=SchedulerPhase.UNKNOWN,
        record_found=None,
        active_record_found=None,
    )
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    assert result.session.state is GatewaySessionState.DEGRADED
    assert result.canceled_scheduler_job is None
    assert runner.provider_canceled_jobs == []
    assert scheduler.action == "retain"
    assert scheduler.outcome == "failed"
    assert scheduler.ownership_verified is True
    assert scheduler.verified_after_operation is False
    assert scheduler.observed_state == "unknown"
    assert scheduler.residual is True
    assert "verification remained unresolved: unknown" in (scheduler.detail or "")
    assert result.to_live_validation_report().status is ValidationStatus.FAILED


@pytest.mark.parametrize("mutation", ["gateway_job_id", "intent_job_id", "provider"])
def test_service_runtime_refuses_changed_scheduler_ownership_before_cancel(
    tmp_path: Path,
    mutation: str,
) -> None:
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    session = queue.get_gateway_session(session_id)
    gateway = dict(session.gateway)
    if mutation == "gateway_job_id":
        queue.update_gateway_session(session_id, scheduler_job_id="forged-job")
    elif mutation == "intent_job_id":
        intents = dict(cast(dict[str, object], gateway["ownership_intents"]))
        scheduler_intent = dict(cast(dict[str, object], intents["scheduler_submission"]))
        scheduler_intent["scheduler_job_id"] = "forged-job"
        intents["scheduler_submission"] = scheduler_intent
        gateway["ownership_intents"] = intents
        queue.update_gateway_session(session_id, gateway=gateway)
    else:
        queue.update_gateway_session(session_id, scheduler="slurm")
    runner.inputs.clear()
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    scheduler_scripts = "\n".join(script or "" for script in runner.inputs)
    assert result.session.state is GatewaySessionState.DEGRADED
    assert scheduler.action == "cancel"
    assert scheduler.outcome == "refused"
    assert scheduler.ownership_verified is False
    assert scheduler.residual is True
    assert "scheduler ownership verification failed" in (scheduler.detail or "")
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert "jarvis runtime cancel" not in scheduler_scripts
    assert "jarvis runtime status" not in scheduler_scripts
    assert "clio-relay scheduler cancel" not in scheduler_scripts
    assert "clio-relay scheduler status" not in scheduler_scripts


def test_service_runtime_refuses_client_forged_scheduler_state_without_remote_anchor(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="client-forged-runtime",
            state=GatewaySessionState.READY,
            scheduler="external",
            scheduler_job_id="victim-job",
            gateway={
                "runtime_spec": _runtime_spec().model_dump(mode="json"),
                "ownership_intents": {
                    "scheduler_submission": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "recorded",
                        "submission_id": "forged-submission",
                        "scheduler_provider": "external",
                        "submission_marker": "forged-marker",
                        "scheduler_job_id": "victim-job",
                    },
                    "desktop_connector": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "not_started",
                    },
                    "remote_connector": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "not_started",
                    },
                },
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=runner,
        sleep=lambda _seconds: None,
    )

    result = supervisor.stop(
        session_id=gateway.session_id,
        cancel_scheduler_job=True,
    )

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    scripts = "\n".join(script or "" for script in runner.inputs)
    assert result.session.state is GatewaySessionState.DEGRADED
    assert scheduler.outcome == "refused"
    assert scheduler.ownership_verified is False
    assert "sidecar identity is invalid" in (scheduler.detail or "")
    assert "jarvis runtime cancel" not in scripts
    assert "jarvis runtime status" not in scripts


def test_service_runtime_does_not_misreport_natural_completion_as_cancellation(
    tmp_path: Path,
) -> None:
    natural_completion = NaturalCompletionCancellationRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=natural_completion,
    )
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

    scheduler = [item for item in result.resources if item.kind == "scheduler_job"]
    assert result.session.state == GatewaySessionState.CLOSED
    assert result.canceled_scheduler_job is None
    assert result.errors == []
    assert scheduler[0].action == "cancel"
    assert scheduler[0].outcome == "terminal"
    assert scheduler[0].verified_after_operation is True
    assert scheduler[0].observed_state == "completed"
    canonical = result.to_live_validation_report()
    cancellation = next(
        check for check in canonical.checks if check.check_id == "gateway.scheduler-canceled"
    )
    assert cancellation.status is ValidationStatus.FAILED
    assert canonical.status is ValidationStatus.FAILED


def test_service_runtime_unverified_retention_cannot_pass_preservation_report(
    tmp_path: Path,
) -> None:
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    session = queue.get_gateway_session(session_id)
    spec = ServiceRuntimeSpec.model_validate(session.gateway["runtime_spec"]).model_copy(
        update={"status_command": None}
    )
    queue.update_gateway_session(
        session_id,
        gateway={**session.gateway, "runtime_spec": spec.model_dump(mode="json")},
    )
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

    scheduler = [item for item in result.resources if item.kind == "scheduler_job"]
    assert result.session.state == GatewaySessionState.DEGRADED
    assert result.session.metadata["cleanup_retryable"] is True
    assert scheduler[0].outcome == "failed"
    assert scheduler[0].ownership_verified is True
    assert scheduler[0].verified_after_operation is False
    assert scheduler[0].observed_state is None
    assert scheduler[0].residual is True
    assert result.errors
    canonical = result.to_live_validation_report()
    retention = next(
        check for check in canonical.checks if check.check_id == "gateway.jobs-preserved-default"
    )
    assert retention.status is ValidationStatus.FAILED
    assert canonical.status is ValidationStatus.FAILED


def test_service_runtime_unknown_retention_remains_retryable(tmp_path: Path) -> None:
    unknown_runner = UnknownRetentionRunner()
    queue, settings, definition, _runner, session_id = _started_session(
        tmp_path,
        runner=unknown_runner,
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="",
        secret_key="",
        runner=unknown_runner,
        sleep=lambda _seconds: None,
    )

    result = supervisor.stop(session_id=session_id)

    scheduler = next(item for item in result.resources if item.kind == "scheduler_job")
    gateway = next(item for item in result.resources if item.kind == "gateway_record")
    assert result.session.state == GatewaySessionState.DEGRADED
    assert result.session.metadata["cleanup_retryable"] is True
    assert scheduler.observed_state is None
    assert scheduler.verified_after_operation is False
    assert scheduler.residual is True
    assert "unsupported state" in (scheduler.detail or "")
    assert gateway.outcome == "failed"
    assert gateway.residual is True
    assert result.to_live_validation_report().status is ValidationStatus.FAILED


def test_service_runtime_terminal_job_is_valid_default_retention_evidence(
    tmp_path: Path,
) -> None:
    completed_runner = CompletedRetentionRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=completed_runner,
    )
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    canonical = result.to_live_validation_report()
    assert result.session.state is GatewaySessionState.CLOSED
    assert scheduler.action == "retain"
    assert scheduler.outcome == "terminal"
    assert scheduler.observed_state == "completed"
    assert canonical.status is ValidationStatus.PASSED


def test_service_runtime_detach_rejects_terminal_scheduler_as_reattachable(
    tmp_path: Path,
) -> None:
    completed_runner = CompletedRetentionRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=completed_runner,
    )
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

    result = supervisor.detach(session_id=session_id)

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    canonical = result.to_live_validation_report()
    retention = next(
        check for check in canonical.checks if check.check_id == "gateway.jobs-preserved-default"
    )
    assert result.session.state is GatewaySessionState.DEGRADED
    assert result.errors == [
        "scheduler job is terminal; detached runtime cannot be proven reattachable"
    ]
    assert result.session.metadata["cleanup_retryable"] is False
    assert scheduler.outcome == "terminal"
    assert retention.status is ValidationStatus.FAILED
    assert canonical.status is ValidationStatus.FAILED


def test_service_runtime_rejects_unknown_external_scheduler_state(tmp_path: Path) -> None:
    invalid_runner = InvalidRetentionRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=invalid_runner,
    )
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

    scheduler = next(resource for resource in result.resources if resource.kind == "scheduler_job")
    assert result.session.state is GatewaySessionState.DEGRADED
    assert scheduler.outcome == "failed"
    assert scheduler.residual is True
    assert "unsupported state" in (scheduler.detail or "")


def test_service_runtime_remote_command_timeout_is_reported(tmp_path: Path) -> None:
    class TimeoutRunner(FakeRunner):
        def run(
            self,
            command: Sequence[str],
            *,
            input_text: str | None = None,
            timeout_seconds: float | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del input_text
            raise subprocess.TimeoutExpired(command, timeout_seconds or 0.0)

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=ClioCoreQueue(settings.core_dir),
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=TimeoutRunner(),
    )

    with pytest.raises(RelayError, match="timed out after 120 seconds"):
        supervisor._ssh("true")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_service_runtime_cleanup_failure_remains_retryable(tmp_path: Path) -> None:
    retry_runner = RetryableConnectorRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=retry_runner,
    )
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

    failed = supervisor.stop(session_id=session_id)

    assert failed.session.state == GatewaySessionState.DEGRADED
    assert failed.errors
    assert failed.residual_resources
    assert failed.session.metadata["cleanup_retryable"] is True
    assert failed.session.metadata["closed_at"] is None
    gateway = [item for item in failed.resources if item.kind == "gateway_record"]
    assert gateway[0].outcome == "failed"
    assert gateway[0].residual is True

    retried = supervisor.stop(session_id=session_id)

    assert retried.session.state == GatewaySessionState.CLOSED
    assert retried.errors == []
    assert retried.residual_resources == []
    assert retried.session.metadata["cleanup_retryable"] is False
    assert retry_runner.stop_attempts == 2


def test_service_runtime_cleanup_retry_rejects_scheduler_policy_drift(
    tmp_path: Path,
) -> None:
    retry_runner = RetryableConnectorRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=retry_runner,
    )
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

    failed = supervisor.stop(session_id=session_id, cancel_scheduler_job=False)
    operation_id = failed.session.gateway["teardown_intent"]["operation_id"]

    with pytest.raises(RelayError, match="cleanup policy changed during retry"):
        supervisor.stop(session_id=session_id, cancel_scheduler_job=True)

    persisted = queue.get_gateway_session(session_id)
    assert persisted.gateway["teardown_intent"]["operation_id"] == operation_id
    assert persisted.gateway["teardown_intent"]["cancel_scheduler_job"] is False
    assert retry_runner.stop_attempts == 1
    with pytest.raises(RelayError, match="committed to teardown and cannot detach"):
        supervisor.detach(session_id=session_id)
    with pytest.raises(RelayError, match="committed to teardown and cannot attach"):
        supervisor.attach(session_id=session_id)


def test_gateway_teardown_policy_creation_is_atomic(tmp_path: Path) -> None:
    queue, _settings, _definition_value, _runner, session_id = _started_session(tmp_path)
    barrier = Barrier(2)

    def prepare(cancel_scheduler_job: bool) -> tuple[str, bool, str]:
        barrier.wait(timeout=5)
        try:
            session = queue.prepare_gateway_teardown_intent(
                session_id,
                cancel_scheduler_job=cancel_scheduler_job,
            )
        except RelayError as exc:
            return "rejected", cancel_scheduler_job, str(exc)
        intent = session.gateway["teardown_intent"]
        return "accepted", cancel_scheduler_job, str(intent["operation_id"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(prepare, [False, True]))

    accepted = [outcome for outcome in outcomes if outcome[0] == "accepted"]
    rejected = [outcome for outcome in outcomes if outcome[0] == "rejected"]
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert "cleanup policy changed during retry" in rejected[0][2]
    persisted = queue.get_gateway_session(session_id).gateway["teardown_intent"]
    assert persisted["cancel_scheduler_job"] is accepted[0][1]
    assert persisted["operation_id"] == accepted[0][2]


def test_service_runtime_cancel_requires_terminal_confirmation_and_can_retry(
    tmp_path: Path,
) -> None:
    retry_runner = RetryableCancellationRunner()
    queue, settings, definition, runner, session_id = _started_session(
        tmp_path,
        runner=retry_runner,
    )
    session = queue.get_gateway_session(session_id)
    spec = ServiceRuntimeSpec.model_validate(session.gateway["runtime_spec"]).model_copy(
        update={"readiness_timeout_seconds": 0.01, "poll_seconds": 0.001}
    )
    queue.update_gateway_session(
        session_id,
        gateway={**session.gateway, "runtime_spec": spec.model_dump(mode="json")},
    )
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

    failed = supervisor.stop(session_id=session_id, cancel_scheduler_job=True)

    assert failed.session.state == GatewaySessionState.DEGRADED
    assert failed.canceled_scheduler_job is None
    scheduler = [item for item in failed.resources if item.kind == "scheduler_job"]
    assert scheduler[0].outcome == "failed"
    assert scheduler[0].residual is True
    assert "not confirmed terminal" in (scheduler[0].detail or "")

    retry_runner.confirm_terminal = True
    retried = supervisor.stop(session_id=session_id, cancel_scheduler_job=True)

    assert retried.session.state == GatewaySessionState.CLOSED
    assert retried.canceled_scheduler_job == "12345"
    assert retried.residual_resources == []


def test_service_runtime_detach_stops_only_desktop_connector(tmp_path: Path) -> None:
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

    result = supervisor.detach(session_id=session_id)

    assert result.session.state == GatewaySessionState.DEGRADED
    assert result.canceled_scheduler_job is None
    assert runner.canceled_jobs == []
    remote = [item for item in result.resources if item.kind == "remote_connector"]
    scheduler = [item for item in result.resources if item.kind == "scheduler_job"]
    gateway = [item for item in result.resources if item.kind == "gateway_record"]
    assert remote[0].outcome == "retained"
    assert scheduler[0].outcome == "retained"
    assert scheduler[0].verified_after_operation is True
    assert scheduler[0].observed_state == "running"
    assert gateway[0].action == "retain"
    assert gateway[0].outcome == "retained"
    assert gateway[0].verified_after_operation is True
    assert gateway[0].observed_state == GatewaySessionState.DEGRADED.value
    assert result.to_cleanup_evidence().mode == "detach"
    canonical = result.to_live_validation_report()
    assert canonical.status is ValidationStatus.PASSED
    assert {check.check_id for check in canonical.checks} == {
        "gateway.detach-connectors",
        "gateway.detached-record",
        "gateway.jobs-preserved-default",
    }

    unbound_resources = [
        resource.model_copy(update={"metadata": {}})
        if resource.kind == "desktop_connector"
        else resource
        for resource in result.resources
    ]
    unbound = ServiceRuntimeStopResult(
        session=result.session,
        mode="detach",
        stopped_local_pid=result.stopped_local_pid,
        stopped_remote_pid=None,
        canceled_scheduler_job=None,
        resources=unbound_resources,
        errors=[],
    ).to_live_validation_report()
    connector_check = next(
        check for check in unbound.checks if check.check_id == "gateway.detach-connectors"
    )
    assert connector_check.status is ValidationStatus.FAILED

    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    attached = supervisor.attach(session_id=session_id)
    assert attached.session.state == GatewaySessionState.READY
    assert len(runner.popen_commands) == 2


def test_service_runtime_detach_is_idempotent_and_keeps_explicit_record_evidence(
    tmp_path: Path,
) -> None:
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

    first = supervisor.detach(session_id=session_id)
    second = supervisor.detach(session_id=session_id)

    assert first.residual_resources == []
    assert second.residual_resources == []
    assert second.errors == []
    assert second.session.state is GatewaySessionState.DEGRADED
    assert [resource.outcome for resource in second.resources] == [
        "missing",
        "retained",
        "retained",
        "retained",
    ]
    gateway = next(resource for resource in second.resources if resource.kind == "gateway_record")
    assert gateway.action == "retain"
    assert gateway.verified_after_operation is True
    assert second.to_live_validation_report().status is ValidationStatus.PASSED


def test_service_runtime_refuses_unowned_gateway_session(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="operator-managed",
            gateway={"runtime_spec": _runtime_spec().model_dump(mode="json")},
        )
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=FakeRunner(),
    )

    with pytest.raises(Exception, match="not an owned clio-relay runtime"):
        supervisor.stop(session_id=session.session_id)


def test_unverified_absent_connector_records_block_owned_gateway_closure(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="legacy-owned-runtime",
            state=GatewaySessionState.SUBMITTED,
            gateway={"runtime_spec": _runtime_spec().model_dump(mode="json")},
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=FakeRunner(),
    )

    result = supervisor.stop(session_id=session.session_id)

    assert result.session.state is GatewaySessionState.DEGRADED
    assert result.residual_resources
    connectors = [
        resource
        for resource in result.resources
        if resource.kind in {"desktop_connector", "remote_connector"}
    ]
    assert len(connectors) == 2
    assert all(resource.ownership_verified is False for resource in connectors)
    assert all(resource.residual is True for resource in connectors)


def test_unresolved_scheduler_submission_intent_blocks_closure(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    intent_schema = "clio-relay.gateway-ownership-intent.v1"
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="unresolved-submission",
            state=GatewaySessionState.SUBMITTED,
            gateway={
                "runtime_spec": _runtime_spec().model_dump(mode="json"),
                "ownership_intents": {
                    "scheduler_submission": {
                        "schema_version": intent_schema,
                        "state": "starting",
                        "submission_id": "unresolved-submission-id",
                    },
                    "desktop_connector": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                    "remote_connector": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                },
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=FakeRunner(),
    )

    result = supervisor.stop(session_id=session.session_id)

    assert result.session.state is GatewaySessionState.DEGRADED
    scheduler = [resource for resource in result.resources if resource.kind == "scheduler_job"]
    assert len(scheduler) == 1
    assert scheduler[0].ownership_verified is False
    assert scheduler[0].residual is True
    assert result.session.metadata["cleanup_retryable"] is True


def test_tampered_scheduler_submission_anchor_remains_unresolved(tmp_path: Path) -> None:
    class TamperedSubmissionRunner(FakeRunner):
        def run(
            self,
            command: Sequence[str],
            *,
            input_text: str | None = None,
            timeout_seconds: float | None = None,
        ) -> subprocess.CompletedProcess[str]:
            if "__CLIO_READ_SUBMISSION__" in (input_text or ""):
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(
                        {
                            "schema_version": "clio-relay.gateway-submission-sidecar.v1",
                            "present": True,
                            "session_id": session.session_id,
                            "submission_id": "submission-1",
                            "scheduler_provider": "external",
                            "submission_marker": "forged-marker",
                            "returncode": 0,
                            "output": '{"scheduler_job_id":"forged-job"}\n',
                        }
                    )
                    + "\n",
                    "",
                )
            return super().run(
                command,
                input_text=input_text,
                timeout_seconds=timeout_seconds,
            )

    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    intent_schema = "clio-relay.gateway-ownership-intent.v1"
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="tampered-submission",
            state=GatewaySessionState.SUBMITTED,
            gateway={
                "runtime_spec": _runtime_spec().model_dump(mode="json"),
                "ownership_intents": {
                    "scheduler_submission": {
                        "schema_version": intent_schema,
                        "state": "starting",
                        "submission_id": "submission-1",
                        "scheduler_provider": "external",
                        "submission_marker": "expected-marker",
                    },
                    "desktop_connector": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                    "remote_connector": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                },
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=TamperedSubmissionRunner(),
    )

    result = supervisor.stop(session_id=session.session_id)

    assert result.session.state is GatewaySessionState.DEGRADED
    assert result.session.scheduler_job_id is None
    scheduler = [resource for resource in result.resources if resource.kind == "scheduler_job"]
    assert len(scheduler) == 1
    assert scheduler[0].ownership_verified is False
    assert scheduler[0].residual is True
    assert result.session.metadata["cleanup_retryable"] is True


def test_unobservable_local_connector_intent_remains_retryable_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    intent_schema = "clio-relay.gateway-ownership-intent.v1"
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="unobservable-local-connector",
            state=GatewaySessionState.SUBMITTED,
            gateway={
                "runtime_spec": _runtime_spec().model_dump(mode="json"),
                "transport": {"mode": "xtcp"},
                "ownership_intents": {
                    "scheduler_submission": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                    "remote_connector": {
                        "schema_version": intent_schema,
                        "state": "not_started",
                    },
                    "desktop_connector": {
                        "schema_version": intent_schema,
                        "state": "starting",
                        "owner_token": "owner-token",
                        "connector_generation_id": "generation-1",
                        "config_path": str(tmp_path / "desktop-frpc.toml"),
                        "metadata_path": str(tmp_path / "desktop-frpc-owner.json"),
                        "stdout_path": str(tmp_path / "desktop-frpc.out"),
                        "stderr_path": str(tmp_path / "desktop-frpc.err"),
                    },
                },
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )

    def candidate_processes(*, command_markers: tuple[str, ...] = ()) -> list[int]:
        del command_markers
        return [4242]

    monkeypatch.setattr(service_runtime, "_local_process_ids", candidate_processes)

    def inaccessible(_pid: int) -> object:
        raise RelayError("candidate process identity read failed")

    monkeypatch.setattr(service_runtime, "_observe_local_process", inaccessible)
    restarted = ServiceRuntimeSupervisor(
        settings=settings,
        queue=ClioCoreQueue(settings.core_dir),
        cluster="test-cluster",
        definition=_definition(),
        token="",
        secret_key="",
        runner=FakeRunner(),
    )

    result = restarted.stop(session_id=session.session_id)

    assert result.session.state is GatewaySessionState.DEGRADED
    local = [resource for resource in result.resources if resource.kind == "desktop_connector"]
    assert len(local) == 1
    assert local[0].ownership_verified is False
    assert local[0].residual is True
    assert local[0].outcome == "refused"
    assert result.session.metadata["cleanup_retryable"] is True


def test_local_connector_pid_reuse_is_not_authorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = service_runtime._ObservedLocalProcess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        pid=555,
        process_group_id=555,
        process_start_marker="new-start",
        command_line="python wrapper owner-token frpc -c /owned/desktop-frpc.toml",
        environment=b"CLIO_RELAY_CONNECTOR_OWNER_TOKEN=owner-token\0",
    )

    def observe(_pid: int) -> object:
        return observed

    monkeypatch.setattr(service_runtime, "_observe_local_process", observe)
    connector: dict[str, object] = {
        "pid": 555,
        "process_group_id": 555,
        "process_start_marker": "old-start",
        "owner_token": "owner-token",
        "config_path": "/owned/desktop-frpc.toml",
    }
    assert (
        service_runtime._local_connector_identity_status(connector)[0]  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        == "replaced"
    )
    assert (
        service_runtime._terminate_local_connector(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            connector
        )
        is None
    )


def test_posix_connector_cleanup_skips_pid_reused_after_pidfd_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scans = iter([[555], []])
    closed: list[int] = []
    signaled: list[tuple[int, int]] = []
    connector: dict[str, object] = {
        "pid": 555,
        "process_group_id": 555,
        "owner_token": "owner-token",
        "connector_generation_id": "generation-1",
        "config_path": "/owned/desktop-frpc.toml",
    }

    def group_members(_connector: dict[str, object]) -> list[int]:
        return next(scans)

    def pidfd_open(_pid: int, _flags: int) -> int:
        return 91

    def pidfd_send_signal(
        process_fd: int,
        sig: int,
        _info: object,
        _flags: int,
    ) -> None:
        signaled.append((process_fd, sig))

    def fail_killpg(_process_group_id: int, _sig: int) -> None:
        raise AssertionError("killpg must not be used")

    monkeypatch.setattr(
        service_runtime,
        "_local_connector_group_members",
        group_members,
    )
    monkeypatch.setattr(service_runtime.os, "pidfd_open", pidfd_open, raising=False)
    monkeypatch.setattr(service_runtime.os, "close", closed.append)
    monkeypatch.setattr(
        service_runtime.signal,
        "pidfd_send_signal",
        pidfd_send_signal,
        raising=False,
    )
    monkeypatch.setattr(
        service_runtime.os,
        "killpg",
        fail_killpg,
        raising=False,
    )

    result = service_runtime._signal_owned_posix_connector_processes(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        connector,
        signal.SIGTERM,
    )

    assert result == []
    assert signaled == []
    assert closed == [91]


def test_posix_connector_cleanup_signals_only_revalidated_pidfd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scans = iter([[555], [555]])
    closed: list[int] = []
    signaled: list[tuple[int, int]] = []
    connector: dict[str, object] = {
        "pid": 555,
        "process_group_id": 555,
        "owner_token": "owner-token",
        "connector_generation_id": "generation-1",
        "config_path": "/owned/desktop-frpc.toml",
    }

    def group_members(_connector: dict[str, object]) -> list[int]:
        return next(scans)

    def pidfd_open(_pid: int, _flags: int) -> int:
        return 92

    def pidfd_send_signal(
        process_fd: int,
        sig: int,
        _info: object,
        _flags: int,
    ) -> None:
        signaled.append((process_fd, sig))

    monkeypatch.setattr(
        service_runtime,
        "_local_connector_group_members",
        group_members,
    )
    monkeypatch.setattr(service_runtime.os, "pidfd_open", pidfd_open, raising=False)
    monkeypatch.setattr(service_runtime.os, "close", closed.append)
    monkeypatch.setattr(
        service_runtime.signal,
        "pidfd_send_signal",
        pidfd_send_signal,
        raising=False,
    )
    sigkill = getattr(signal, "SIGKILL", 9)

    result = service_runtime._signal_owned_posix_connector_processes(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        connector,
        sigkill,
    )

    assert result == [555]
    assert signaled == [(92, sigkill)]
    assert closed == [92]


def test_windows_connector_pid_reuse_is_not_authorized_by_descendant_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = service_runtime._ObservedLocalProcess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        pid=555,
        process_group_id=555,
        process_start_marker="new-start",
        command_line="unrelated.exe",
        environment=None,
    )
    connector: dict[str, object] = {
        "pid": 555,
        "process_group_id": 555,
        "process_start_marker": "old-start",
        "owner_token": "owner-token",
        "connector_generation_id": "generation-1",
        "config_path": r"C:\owned\desktop-frpc.toml",
    }

    def observe(_pid: int) -> object:
        return observed

    def group_members(_connector: dict[str, object]) -> list[int]:
        return [556]

    monkeypatch.setattr(service_runtime.os, "name", "nt")
    monkeypatch.setattr(service_runtime, "_observe_local_process", observe)
    monkeypatch.setattr(
        service_runtime,
        "_local_connector_group_members",
        group_members,
    )

    status, detail = service_runtime._local_connector_identity_status(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        connector
    )

    assert status == "replaced"
    assert detail == "recorded connector PID now belongs to a different process"


def test_local_connector_token_mismatch_is_not_authorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = service_runtime._ObservedLocalProcess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        pid=555,
        process_group_id=555,
        process_start_marker="same-start",
        command_line="python wrapper other-token frpc -c /owned/desktop-frpc.toml",
        environment=b"CLIO_RELAY_CONNECTOR_OWNER_TOKEN=other-token\0",
    )

    def observe(_pid: int) -> object:
        return observed

    monkeypatch.setattr(service_runtime, "_observe_local_process", observe)
    connector: dict[str, object] = {
        "pid": 555,
        "process_group_id": 555,
        "process_start_marker": "same-start",
        "owner_token": "owner-token",
        "config_path": "/owned/desktop-frpc.toml",
    }
    assert (
        service_runtime._local_connector_identity_status(connector)[0]  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        == "unverified"
    )


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


def test_service_runtime_failed_health_cleans_owned_connectors_without_canceling_job(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core", spool_dir=tmp_path / "spool", frpc_bin="frpc-test"
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

    def fail_health(*_args: object, **_kwargs: object) -> None:
        raise RelayError("desktop health failed")

    supervisor._wait_for_local_health = fail_health  # type: ignore[method-assign]
    with pytest.raises(RelayError, match="desktop health failed"):
        supervisor.start(name="failed-health", spec=_runtime_spec())

    session = queue.list_gateway_sessions(cluster="test-cluster")[0]
    assert session.state == GatewaySessionState.FAILED
    assert session.gateway["teardown"]["stopped_remote_pid"] == 444
    assert session.gateway["teardown"]["canceled_scheduler_job"] is None
    assert runner.canceled_jobs == []


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
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
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
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
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
        supervisor.start(
            name="generic-image-service",
            spec=_runtime_spec().model_copy(update={"status_command": None}),
        )


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
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
    ) -> ServiceRuntimeStartResult:
        del owner_session_id, owner_session_generation_id
        session = self.queue.create_gateway_session(
            gateway_session_for_cli(cluster="test-cluster", name=name, spec=spec)
        )

        return ServiceRuntimeStartResult(
            session=session,
            connect_url="http://127.0.0.1:28777",
            health_url="http://127.0.0.1:28777/healthz",
            stream_url=None,
            compatibility_urls={},
            events_url=None,
        )

    def fake_worker_identity(
        report: LiveValidationReport,
        definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        assert definition.name == "test-cluster"
        recorder = ValidationRecorder(report)
        with recorder.check("worker.artifact-version", "verified remote worker") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="worker verified"))
        recorder.add_resource(
            ValidationResource(
                kind="relay_worker",
                resource_id="worker:test-cluster",
                cluster="test-cluster",
                state="running",
            )
        )

    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", fake_start)
    monkeypatch.setattr("clio_relay.cli._attach_verified_remote_worker", fake_worker_identity)
    validation_report = tmp_path / "gateway-validation.json"
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
            "--validation-report",
            str(validation_report),
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"runtime_kind": "image-service"' in result.output
    report = json.loads(validation_report.read_text(encoding="utf-8"))
    assert "worker.artifact-version" in {check["check_id"] for check in report["checks"]}
    assert "relay_worker" in {resource["kind"] for resource in report["resources"]}


@pytest.mark.parametrize(
    "status",
    [
        {
            "owner": "clio-relay",
            "session_id": "desktop-session",
            "session_generation_id": "wrong-generation",
            "running": True,
            "ownership_verified": True,
        },
        {
            "owner": "clio-relay",
            "session_id": "desktop-session",
            "session_generation_id": "generation-1",
            "running": False,
            "ownership_verified": True,
        },
        {
            "owner": "clio-relay",
            "session_id": "desktop-session",
            "session_generation_id": "generation-1",
            "running": True,
            "ownership_verified": False,
        },
    ],
    ids=("wrong-generation", "stopped", "unverified-owner"),
)
def test_owned_gateway_start_requires_live_exact_remote_generation_before_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: dict[str, object],
) -> None:
    spec_path = tmp_path / "runtime.json"
    spec_path.write_text(_runtime_spec().model_dump_json(), encoding="utf-8")
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        '{"clusters":{"test-cluster":' + _definition().model_dump_json() + "}}",
        encoding="utf-8",
    )
    core_dir = tmp_path / "core"
    report_path = tmp_path / "gateway-admission-failed.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def fake_process_status(**_kwargs: object) -> dict[str, object]:
        return status

    monkeypatch.setattr(relay_cli, "status_remote_session", fake_process_status)

    def forbidden_start(
        _self: ServiceRuntimeSupervisor,
        **_kwargs: object,
    ) -> ServiceRuntimeStartResult:
        raise AssertionError("runtime side effects must not start")

    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", forbidden_start)
    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "start-runtime",
            "--cluster",
            "test-cluster",
            "--name",
            "owned-runtime",
            "--runtime-json-file",
            str(spec_path),
            "--owner-session-id",
            "desktop-session",
            "--owner-session-generation-id",
            "generation-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "requires a live owned session with the exact generation" in result.output
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "failed"
    assert ClioCoreQueue(core_dir).list_gateway_sessions() == []


def test_owned_gateway_start_holds_transition_lock_through_runtime_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path = tmp_path / "runtime.json"
    spec = _runtime_spec()
    spec_path.write_text(spec.model_dump_json(), encoding="utf-8")
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        '{"clusters":{"test-cluster":' + _definition().model_dump_json() + "}}",
        encoding="utf-8",
    )
    core_dir = tmp_path / "core"
    events: list[str] = []
    lock_held = False
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    class RecordingLock:
        def __enter__(self) -> None:
            nonlocal lock_held
            assert lock_held is False
            lock_held = True
            events.append("enter")

        def __exit__(self, *_args: object) -> None:
            nonlocal lock_held
            assert lock_held is True
            events.append("exit")
            lock_held = False

    def make_lock(*, cluster: str, session_id: str) -> RecordingLock:
        assert (cluster, session_id) == ("test-cluster", "desktop-session")
        return RecordingLock()

    def process_status(**_kwargs: object) -> dict[str, object]:
        assert lock_held is True
        events.append("status")
        return {
            "owner": "clio-relay",
            "session_id": "desktop-session",
            "session_generation_id": "generation-1",
            "running": True,
            "ownership_verified": True,
        }

    def remote_status(_definition: ClusterDefinition, args: list[str]) -> str:
        assert lock_held is True
        assert args[:2] == ["session", "admission-status"]
        events.append("admission")
        return json.dumps(
            {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": "desktop-session",
                "session_generation_id": "generation-1",
                "active_generation_id": "generation-1",
                "closing_generation_id": None,
                "active": True,
                "closing": False,
                "closed": False,
                "open": True,
                "cleanup_intent": None,
            }
        )

    def fake_start(
        self: ServiceRuntimeSupervisor,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
    ) -> ServiceRuntimeStartResult:
        assert lock_held is True
        events.append("start")
        assert owner_session_id == "desktop-session"
        assert owner_session_generation_id == "generation-1"
        assert owner_session_admission_id is not None
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster="test-cluster",
                name=name,
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": owner_session_id,
                    "owner_session_generation_id": owner_session_generation_id,
                    "owner_session_admission_id": owner_session_admission_id,
                },
                gateway={"runtime_spec": spec.model_dump(mode="json")},
            )
        )
        return ServiceRuntimeStartResult(
            session=session,
            connect_url="http://127.0.0.1:28777",
            health_url="http://127.0.0.1:28777/healthz",
            stream_url=None,
            compatibility_urls={},
            events_url=None,
        )

    monkeypatch.setattr(relay_cli, "_session_transition_lock", make_lock)
    monkeypatch.setattr(relay_cli, "status_remote_session", process_status)
    monkeypatch.setattr(relay_cli, "run_remote_clio", remote_status)

    def skip_worker_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        return

    monkeypatch.setattr(relay_cli, "_attach_verified_remote_worker", skip_worker_identity)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", fake_start)
    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "start-runtime",
            "--cluster",
            "test-cluster",
            "--name",
            "owned-runtime",
            "--runtime-json-file",
            str(spec_path),
            "--owner-session-id",
            "desktop-session",
            "--owner-session-generation-id",
            "generation-1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert events == ["enter", "status", "admission", "status", "admission", "start", "exit"]


def test_gateway_start_runtime_cli_writes_canonical_failure_report(
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

    def fail_start(
        _self: ServiceRuntimeSupervisor,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
    ) -> ServiceRuntimeStartResult:
        del name, spec, owner_session_id, owner_session_generation_id
        raise RelayError("desktop connector did not establish owned process identity")

    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", fail_start)
    validation_report = tmp_path / "gateway-failed-validation.json"

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
            "--validation-report",
            str(validation_report),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(validation_report.read_text(encoding="utf-8"))
    assert report["schema_version"] == "1.0"
    assert report["scenario"] == "gateway-runtime"
    assert report["cluster"] == "test-cluster"
    assert report["status"] == "failed"
    assert report["completed_at"] is not None
    assert report["checks"] == [
        {
            "check_id": "gateway.start-runtime",
            "summary": "start scheduler-backed gateway runtime",
            "status": "failed",
            "started_at": report["checks"][0]["started_at"],
            "completed_at": report["checks"][0]["completed_at"],
            "evidence": [],
            "error": ("RelayError: desktop connector did not establish owned process identity"),
        }
    ]
    assert report["error"] == (
        "RelayError: desktop connector did not establish owned process identity"
    )


def test_gateway_stop_runtime_cli_writes_canonical_failure_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        '{"clusters":{"test-cluster":' + _definition().model_dump_json() + "}}",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    def fail_stop(
        _self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
    ) -> object:
        del session_id, cancel_scheduler_job
        raise RelayError("remote connector ownership changed")

    monkeypatch.setattr(ServiceRuntimeSupervisor, "stop", fail_stop)
    validation_report = tmp_path / "gateway-stop-failed-validation.json"

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "stop-runtime",
            "gateway-owned",
            "--cluster",
            "test-cluster",
            "--validation-report",
            str(validation_report),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(validation_report.read_text(encoding="utf-8"))
    assert report["scenario"] == "gateway-runtime"
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "gateway.stop-runtime"
    assert report["error"] == "RelayError: remote connector ownership changed"


def test_gateway_stop_runtime_default_report_failure_controls_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster_registry(tmp_path)
    session = gateway_session_for_cli(
        cluster="test-cluster",
        name="canonical-failure",
        spec=_runtime_spec(),
    ).model_copy(update={"state": GatewaySessionState.CLOSED})
    result = ServiceRuntimeStopResult(
        session=session,
        mode="teardown",
        stopped_local_pid=555,
        stopped_remote_pid=444,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind=kind,
                resource_id=resource_id,
                location="test-cluster",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            )
            for kind, resource_id in (
                ("desktop_connector", "555"),
                ("remote_connector", "444"),
            )
        ]
        + [
            CleanupResource(
                kind="scheduler_job",
                resource_id="12345",
                location="test-cluster",
                action="cancel",
                ownership_verified=True,
                outcome="terminal",
                verified_after_operation=True,
                observed_state="completed",
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id=session.session_id,
                location="test-cluster",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
            ),
        ],
        errors=[],
    )

    def return_stop_result(
        _self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
    ) -> ServiceRuntimeStopResult:
        del session_id, cancel_scheduler_job
        return result

    monkeypatch.setattr(ServiceRuntimeSupervisor, "stop", return_stop_result)

    invoked = CliRunner().invoke(
        app,
        [
            "gateway",
            "stop-runtime",
            session.session_id,
            "--cluster",
            "test-cluster",
            "--cancel-scheduler-job",
        ],
    )

    assert invoked.exit_code == 1
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    assert len(reports) == 1
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    checks = {check["check_id"]: check["status"] for check in canonical["checks"]}
    assert checks["gateway.scheduler-canceled"] == "failed"


def test_gateway_detach_runtime_default_report_requires_verified_retention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster_registry(tmp_path)
    session = gateway_session_for_cli(
        cluster="test-cluster",
        name="detach-canonical-failure",
        spec=_runtime_spec(),
    ).model_copy(update={"state": GatewaySessionState.DEGRADED})
    result = ServiceRuntimeStopResult(
        session=session,
        mode="detach",
        stopped_local_pid=555,
        stopped_remote_pid=None,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind="desktop_connector",
                resource_id="555",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="444",
                location="test-cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="12345",
                location="test-cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=False,
                observed_state="unknown",
            ),
        ],
        errors=[],
    )
    worker_verifications: list[str] = []

    def record_worker_verification(
        _report: LiveValidationReport,
        definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        worker_verifications.append(definition.name)

    def return_detach_result(
        _self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
    ) -> ServiceRuntimeStopResult:
        del session_id
        return result

    monkeypatch.setattr(ServiceRuntimeSupervisor, "detach", return_detach_result)
    monkeypatch.setattr(
        relay_cli,
        "_attach_verified_remote_worker",
        record_worker_verification,
    )

    invoked = CliRunner().invoke(
        app,
        ["gateway", "detach-runtime", session.session_id, "--cluster", "test-cluster"],
    )

    assert invoked.exit_code == 1
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    assert len(reports) == 1
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    checks = {check["check_id"]: check["status"] for check in canonical["checks"]}
    assert checks["gateway.jobs-preserved-default"] == "failed"
    assert worker_verifications == ["test-cluster"]


def test_gateway_report_fails_when_remote_worker_identity_does_not_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = gateway_session_for_cli(
        cluster="test-cluster",
        name="gateway-worker-mismatch",
        spec=_runtime_spec(),
    )
    report = ServiceRuntimeStartResult(
        session=session,
        connect_url="http://127.0.0.1:28777",
        health_url="http://127.0.0.1:28777/healthz",
        stream_url=None,
        compatibility_urls={},
        events_url=None,
    ).to_live_validation_report()

    def fail_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        raise RelayError("remote installation receipt mismatch")

    monkeypatch.setattr(relay_cli, "_attach_verified_remote_worker", fail_identity)
    destination = tmp_path / "gateway-worker-mismatch.json"

    with pytest.raises(RelayError, match="receipt mismatch"):
        relay_cli._write_remote_verified_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            report,
            _definition(),
            destination,
        )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    worker_checks = [
        check for check in payload["checks"] if check["check_id"] == "worker.installation-info"
    ]
    assert worker_checks[0]["status"] == "failed"


def test_managed_gateway_commands_prefer_desktop_runtime_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_FRPC_BIN", "frpc-test")
    _write_cluster_registry(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    stored_session = gateway_session_for_cli(
        cluster="test-cluster",
        name="desktop-managed-runtime",
        spec=_runtime_spec(),
    )
    stored_session.gateway["transport"] = {
        "desktop_connector": {"owner_token": "private-owner-capability"}
    }
    session = queue.create_gateway_session(stored_session)

    def unexpected_remote(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("desktop-managed gateway must not be routed to remote CLI")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", unexpected_remote)

    read = CliRunner().invoke(
        app,
        ["gateway", "get", session.session_id, "--cluster", "test-cluster"],
    )
    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session.session_id,
            "--cluster",
            "test-cluster",
            "--state",
            "degraded",
        ],
    )
    closed = CliRunner().invoke(
        app,
        ["gateway", "close", session.session_id, "--cluster", "test-cluster"],
    )

    assert read.exit_code == 0
    assert updated.exit_code == 0
    assert "private-owner-capability" not in read.output
    assert (
        json.loads(read.output)["gateway"]["transport"]["desktop_connector"]["owner_token"]
        == "<redacted>"
    )
    assert "private-owner-capability" not in updated.output
    assert json.loads(updated.output)["state"] == "degraded"
    assert closed.exit_code == 1
    assert "must be closed with stop-runtime" in closed.output
    assert "private-owner-capability" not in closed.output
    persisted = queue.get_gateway_session(session.session_id)
    assert persisted.state == GatewaySessionState.DEGRADED
    assert (
        persisted.gateway["transport"]["desktop_connector"]["owner_token"]
        == "private-owner-capability"
    )


def test_gateway_list_combines_desktop_and_remote_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_FRPC_BIN", "frpc-test")
    _write_cluster_registry(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    local = queue.create_gateway_session(
        gateway_session_for_cli(
            cluster="test-cluster",
            name="desktop-managed-runtime",
            spec=_runtime_spec(),
        )
    )
    remote = GatewaySession(
        session_id="gateway_remote_record",
        cluster="test-cluster",
        name="cluster-managed-runtime",
    )

    def fake_remote(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps([remote.model_dump(mode="json")]).encode(),
            b"",
        )

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_remote)

    result = CliRunner().invoke(
        app,
        ["gateway", "list", "--cluster", "test-cluster"],
    )

    assert result.exit_code == 0
    page = json.loads(result.output)
    assert {item["session_id"] for item in page["gateway_sessions"]} == {
        local.session_id,
        remote.session_id,
    }
    assert page["source_totals"] == {"desktop": 1, "cluster": 1}
    assert page["aggregate_record_limit"] == 200


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
        metadata={"owner": "clio-relay", "runtime_kind": spec.kind},
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


def _write_cluster_registry(root: Path) -> None:
    ClusterRegistry(clusters={"test-cluster": _definition()}).save(
        root / ".clio-relay" / "clusters.json"
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
        status_command=["jarvis", "runtime", "status", "{scheduler_job_id}"],
        service_port=18777,
        stream_path="/live-data",
        compatibility_paths={"snapshot": "/debug/snapshot"},
        desktop_bind_port=28777,
        proxy_name="generic-service-proxy",
        metadata={"dataset_id": "generic-dataset"},
    )


def _health_probe_supervisor(tmp_path: Path) -> ServiceRuntimeSupervisor:
    """Return a supervisor suitable for direct local health-probe tests."""
    return ServiceRuntimeSupervisor(
        settings=RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            frpc_bin="frpc-test",
        ),
        queue=ClioCoreQueue(tmp_path / "core"),
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=FakeRunner(),
        sleep=lambda _seconds: None,
    )


@pytest.mark.parametrize(
    ("status_code", "body", "error"),
    [
        (404, b"runtime-nonce", "HTTP 404"),
        (200, b"wrong-runtime", "did not match the runtime identity"),
    ],
)
def test_local_health_probe_rejects_non_2xx_and_wrong_runtime_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    body: bytes,
    error: str,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    clock = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(
        service_runtime,
        "time",
        SimpleNamespace(time=lambda: next(clock)),
    )

    def fake_httpx_get(*_args: object, **_kwargs: object) -> httpx.Response:
        return httpx.Response(status_code, content=body)

    monkeypatch.setattr(
        service_runtime.httpx,
        "get",
        fake_httpx_get,
    )

    with pytest.raises(RelayError, match=error):
        supervisor._wait_for_local_health(  # pyright: ignore[reportPrivateUsage]
            "http://127.0.0.1:28777/healthz",
            1.0,
            0.1,
            expected_body="runtime-nonce",
        )


def test_local_health_probe_accepts_exact_2xx_runtime_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)

    def fake_httpx_get(*_args: object, **_kwargs: object) -> httpx.Response:
        return httpx.Response(200, content=b"runtime-nonce")

    monkeypatch.setattr(
        service_runtime.httpx,
        "get",
        fake_httpx_get,
    )

    supervisor._wait_for_local_health(  # pyright: ignore[reportPrivateUsage]
        "http://127.0.0.1:28777/healthz",
        1.0,
        0.1,
        expected_body="runtime-nonce",
    )


def test_remote_health_probe_requires_2xx_and_exact_runtime_identity() -> None:
    script = service_runtime._remote_http_probe_script(  # pyright: ignore[reportPrivateUsage]
        "compute-01",
        18777,
        "/healthz",
        expected_body="runtime-nonce",
    )

    assert "200 <= response.status < 300" in script
    assert "body == expected_body" in script
    assert "cnVudGltZS1ub25jZQ==" in script


def _visitor_config_path(settings: RelaySettings, session_id: str) -> Path:
    return settings.core_dir.parent / "runtime-sessions" / session_id / "desktop-frpc.toml"


def _started_session(
    tmp_path: Path,
    *,
    runner: FakeRunner | None = None,
    spec: ServiceRuntimeSpec | None = None,
) -> tuple[ClioCoreQueue, RelaySettings, ClusterDefinition, FakeRunner, str]:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    definition = _definition()
    selected_runner = runner or FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=definition,
        token="token",
        secret_key="secret",
        runner=selected_runner,
        sleep=lambda _seconds: None,
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    result = supervisor.start(name="generic-image-service", spec=spec or _runtime_spec())
    return queue, settings, definition, selected_runner, result.session.session_id
