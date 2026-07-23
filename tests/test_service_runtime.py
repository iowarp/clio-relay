from __future__ import annotations

import ctypes
import errno
import gzip
import hashlib
import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
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
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
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
    ServiceRuntimePendingResult,
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

_REAL_OBSERVE_LOCAL_PROCESS = service_runtime._observe_local_process  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


class FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid


class _PidfdSyscallFixture:
    def __init__(self, outcomes: dict[int, tuple[int, int]]) -> None:
        self.outcomes = outcomes
        self.calls: list[int] = []
        self.restype: object = None

    def __call__(self, *arguments: object) -> int:
        number = getattr(arguments[0], "value", None)
        if not isinstance(number, int):
            raise AssertionError("pidfd syscall number is not an integer")
        self.calls.append(number)
        result, error_number = self.outcomes[number]
        ctypes.set_errno(error_number)
        return result


class _PidfdLibcWithoutSymbols:
    pidfd_open: None = None
    pidfd_send_signal: None = None

    def __init__(self, syscall: _PidfdSyscallFixture) -> None:
        self.syscall = syscall


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


def test_subprocess_runner_delivers_private_input_and_immediate_eof(tmp_path: Path) -> None:
    """The real runner writes one anonymous-pipe payload without retaining stdin."""
    observed_path = tmp_path / "observed-bootstrap.bin"
    stdout_path = tmp_path / "bootstrap.out"
    stderr_path = tmp_path / "bootstrap.err"
    payload = b'{"schema_version":"private-bootstrap-test.v1","secret":"not-in-argv"}'
    child = (
        "import sys; from pathlib import Path; "
        "Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read())"
    )

    process = service_runtime.SubprocessCommandRunner().popen(
        [
            sys.executable,
            "-c",
            service_runtime._LOCAL_CONNECTOR_WRAPPER_CODE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "owner-token",
            "generation-id",
            sys.executable,
            "-c",
            child,
            str(observed_path),
        ],
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        isolate_process_group=True,
        input_bytes=payload,
    )

    assert process.wait(timeout=5.0) == 0
    assert observed_path.read_bytes() == payload
    assert process.stdin is not None and process.stdin.closed
    assert payload.decode("utf-8") not in " ".join(cast(list[str], process.args))


def test_subprocess_runner_terminates_process_group_when_private_input_delivery_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child that rejects its bootstrap cannot survive as an unowned process."""

    class RejectingPipe:
        closed = False

        def write(self, _content: bytes) -> int:
            raise BrokenPipeError("child closed bootstrap pipe")

        def flush(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    class StartedProcess:
        pid = 99123
        stdin = RejectingPipe()

    process = cast(subprocess.Popen[bytes], StartedProcess())

    def fake_popen(*_args: object, **_kwargs: object) -> subprocess.Popen[bytes]:
        return process

    monkeypatch.setattr(service_runtime.subprocess, "Popen", fake_popen)
    terminated: list[int] = []
    monkeypatch.setattr(
        service_runtime,
        "_terminate_just_started_process_group",
        terminated.append,
    )
    secret = b"private-bootstrap-value"

    with pytest.raises(RelayError, match="failed to deliver private process bootstrap") as caught:
        service_runtime.SubprocessCommandRunner().popen(
            ["browser-gateway-test"],
            stdout_path=tmp_path / "failed-bootstrap.out",
            stderr_path=tmp_path / "failed-bootstrap.err",
            isolate_process_group=True,
            input_bytes=secret,
        )

    assert terminated == [99123]
    assert StartedProcess.stdin.closed is True
    assert secret.decode("utf-8") not in str(caught.value)


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


@pytest.mark.parametrize("mutation", ["generation", "process_group", "config_path", "log_path"])
def test_remote_connector_start_rejects_response_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """SSH output cannot replace the connector identity committed before launch."""
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    session = GatewaySession(cluster="test-cluster", name="identity-check")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
    )
    generation_id = "generation-1"
    session_root = f"/home/user/.local/share/clio-relay/service-sessions/{session.session_id}"
    values = {
        "remote_frpc_pid": "444",
        "remote_frpc_pgid": "444",
        "connector_generation_id": generation_id,
        "remote_frpc_config": f"{session_root}/remote-frpc.toml",
        "remote_frpc_log": f"{session_root}/remote-frpc.log",
    }
    if mutation == "generation":
        values["connector_generation_id"] = "generation-forged"
    elif mutation == "process_group":
        values["remote_frpc_pgid"] = "445"
    elif mutation == "config_path":
        values["remote_frpc_config"] = (
            "/home/user/.local/share/clio-relay/service-sessions/gateway_other/remote-frpc.toml"
        )
    else:
        values["remote_frpc_log"] = (
            "/home/user/.local/share/clio-relay/service-sessions/gateway_other/remote-frpc.log"
        )

    def ssh(_script: str) -> str:
        return "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"

    monkeypatch.setattr(supervisor, "_ssh", ssh)

    with pytest.raises(RelayError, match="remote connector start"):
        supervisor._start_remote_connector(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            session=session,
            spec=_runtime_spec(),
            node="compute-01",
            proxy_name="identity-check",
            ownership_intent={
                "owner_token": "owner-token",
                "connector_generation_id": generation_id,
            },
        )


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
        self.popen_inputs: list[bytes | None] = []
        self.isolated_processes: list[bool] = []
        self.canceled_jobs: list[str] = []
        self.provider_canceled_jobs: list[str] = []
        self.submission_record: dict[str, object] | None = None
        self.remote_connector: dict[str, object] | None = None

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
            session_id = _script_assignment(script, "session_id")
            generation_id = _script_assignment(script, "connector_generation_id")
            owner_token = _script_assignment(script, "owner_token")
            session_root = f"/home/user/.local/share/clio-relay/service-sessions/{session_id}"
            self.remote_connector = {
                "owner": "clio-relay",
                "session_id": session_id,
                "pid": 444,
                "process_group_id": 444,
                "connector_generation_id": generation_id,
                "owner_token": owner_token,
                "config_path": f"{session_root}/remote-frpc.toml",
                "log_path": f"{session_root}/remote-frpc.log",
            }
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "remote_frpc_pid=444",
                        "remote_frpc_pgid=444",
                        f"connector_generation_id={generation_id}",
                        f"remote_frpc_config={session_root}/remote-frpc.toml",
                        f"remote_frpc_log={session_root}/remote-frpc.log",
                    ]
                )
                + "\n",
                "",
            )
        if "__CLIO_DISCOVER_CONNECTOR__" in script:
            expected_session = _script_assignment(script, "session_id")
            expected_token = _script_assignment(script, "owner_token")
            expected_generation = _script_assignment(script, "generation_id")
            connector = self.remote_connector
            verified = bool(
                connector is not None
                and connector.get("session_id") == expected_session
                and connector.get("owner_token") == expected_token
                and connector.get("connector_generation_id") == expected_generation
            )
            payload: dict[str, object] = (
                {
                    "present": True,
                    "ownership_verified": True,
                    "matching_pids": [444],
                    "connector": connector,
                }
                if verified
                else {
                    "present": False,
                    "ownership_verified": connector is None,
                    "matching_pids": [],
                    "error": (None if connector is None else "remote connector identity mismatch"),
                }
            )
            return subprocess.CompletedProcess(command, 0, json.dumps(payload) + "\n", "")
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
            self.remote_connector = None
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
        input_bytes: bytes | None = None,
    ) -> subprocess.Popen[bytes]:
        self.popen_commands.append(list(command))
        self.popen_environments.append(env)
        self.popen_inputs.append(input_bytes)
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
        assert expected_config.endswith(("desktop-frpc.toml", ".browser-gateway.json"))
        return LocalConnectorIdentity(
            pid=pid,
            process_group_id=pid,
            process_start_marker=f"start-{pid}",
            owner_token=owner_token,
        )


class TransientConnectorDiscoveryRunner(FakeRunner):
    """Inject a bounded remote discovery outage after one connector is durable."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_remote_discovery = False

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if self.fail_remote_discovery and "__CLIO_DISCOVER_CONNECTOR__" in (input_text or ""):
            self.commands.append(list(command))
            self.inputs.append(input_text)
            return subprocess.CompletedProcess(
                command,
                1,
                "",
                "temporary remote connector sidecar read failure",
            )
        return super().run(
            command,
            input_text=input_text,
            timeout_seconds=timeout_seconds,
        )


class AmbiguousSubmissionRunner(FakeRunner):
    """Lose the first submit response after optionally publishing its exact sidecar."""

    def __init__(self, *, publish_sidecar: bool) -> None:
        super().__init__()
        self.publish_sidecar = publish_sidecar
        self.submit_attempts = 0

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "__CLIO_CAPTURE_SUBMISSION__" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            self.submit_attempts += 1
            if self.submit_attempts > 1:
                pytest.fail("an ambiguous scheduler submission must never be resubmitted")
            if self.publish_sidecar:
                output = '{"scheduler_job_id":"12345","service_host":"compute-01"}\n'
                self.submission_record = {
                    "schema_version": "clio-relay.gateway-submission-sidecar.v1",
                    "present": True,
                    "session_id": _script_assignment(script, "session_id"),
                    "submission_id": _script_assignment(script, "submission_id"),
                    "scheduler_provider": _script_assignment(
                        script,
                        "scheduler_provider",
                    ),
                    "submission_marker": _script_assignment(script, "submission_marker"),
                    "returncode": 0,
                    "output": output,
                    "output_truncated": False,
                }
            raise subprocess.TimeoutExpired(command, timeout_seconds or 120.0)
        return super().run(
            command,
            input_text=input_text,
            timeout_seconds=timeout_seconds,
        )


class ProductionSubmissionVerifierRunner(AmbiguousSubmissionRunner):
    """Execute the generated verifier against a corrupt or incomplete sidecar set."""

    def __init__(self, *, sidecar_state: str) -> None:
        if sidecar_state not in {"record_identity_mismatch", "output_incomplete"}:
            raise ValueError(f"unsupported test sidecar state: {sidecar_state}")
        super().__init__(publish_sidecar=False)
        self.sidecar_state = sidecar_state
        self.verifier_runs = 0

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "__CLIO_READ_SUBMISSION__" not in script:
            return super().run(
                command,
                input_text=input_text,
                timeout_seconds=timeout_seconds,
            )
        self.commands.append(list(command))
        self.inputs.append(input_text)
        self.verifier_runs += 1
        session_id = _script_assignment(script, "session_id")
        submission_id = _script_assignment(script, "submission_id")
        scheduler_provider = _script_assignment(script, "scheduler_provider")
        submission_marker = _script_assignment(script, "submission_marker")
        expected = {
            "session_id": session_id,
            "submission_id": submission_id,
            "scheduler_provider": scheduler_provider,
            "submission_marker": submission_marker,
        }
        intent = {
            "schema_version": "clio-relay.gateway-submission-intent.v1",
            **expected,
        }
        output = b'{"scheduler_job_id":"should-not-be-trusted"}\n'
        record = {
            "schema_version": "clio-relay.gateway-submission-sidecar.v1",
            **expected,
            "session_id": "corrupt-session-identity",
            "returncode": 0,
            "output": output.decode(),
            "output_sha256": hashlib.sha256(output).hexdigest(),
            "output_size": len(output),
            "output_truncated": False,
        }
        wrapper = f"""set -euo pipefail
test_home=$(mktemp -d)
trap 'rm -rf -- "$test_home"' EXIT
export HOME="$test_home"
root="$HOME/.local/share/clio-relay/service-sessions/{shlex.quote(session_id)}/submissions"
mkdir -p "$root"
python3 - "$root/{shlex.quote(submission_id)}.intent.json" \
  "$root/{shlex.quote(submission_id)}.json" \
  "$root/{shlex.quote(submission_id)}.out" \
  {shlex.quote(json.dumps(intent, sort_keys=True))} \
  {shlex.quote(json.dumps(record, sort_keys=True))} \
  {shlex.quote(self.sidecar_state)} <<'__CLIO_TEST_SUBMISSION_SIDECARS__'
import os
import sys
from pathlib import Path

intent_raw, record_raw, output_raw, intent, record, sidecar_state = sys.argv[1:]
Path(intent_raw).write_text(intent, encoding="utf-8")
os.chmod(intent_raw, 0o600)
if sidecar_state == "record_identity_mismatch":
    Path(record_raw).write_text(record, encoding="utf-8")
    os.chmod(record_raw, 0o600)
else:
    Path(output_raw).write_bytes(b'{{"scheduler_job_id":"in-flight"}}\\n')
    os.chmod(output_raw, 0o600)
__CLIO_TEST_SUBMISSION_SIDECARS__
{script}
"""
        if sys.platform == "win32":
            wsl = Path(os.environ.get("SYSTEMROOT", r"C:\Windows")) / "System32" / "wsl.exe"
            if not wsl.is_file():
                return self._emulate_verifier_result(command, script)
            shell_command = [str(wsl), "-e", "bash", "-s"]
        else:
            shell_command = ["bash", "-s"]
        completed = subprocess.run(  # noqa: S603
            shell_command,
            input=wrapper.encode("utf-8"),
            capture_output=True,
            timeout=30,
            check=False,
        )
        return subprocess.CompletedProcess(
            command,
            completed.returncode,
            completed.stdout.decode("utf-8", errors="replace"),
            completed.stderr.decode("utf-8", errors="replace"),
        )

    def _emulate_verifier_result(
        self,
        command: Sequence[str],
        script: str,
    ) -> subprocess.CompletedProcess[str]:
        """Preserve the generated verifier contract where no POSIX shell is installed."""
        assert "scheduler submission record identity mismatch" in script
        if self.sidecar_state == "output_incomplete":
            payload: dict[str, object] = {
                "schema_version": "clio-relay.gateway-submission-verification.v1",
                "present": False,
                "anchored": True,
                "verification_outcome": "retryable",
                "error_code": "output_incomplete",
            }
        else:
            payload = {
                "schema_version": "clio-relay.gateway-submission-verification.v1",
                "present": True,
                "verification_outcome": "definitive_invalid",
                "failure_kind": "relay_integrity_failure",
                "error_code": "record_identity_mismatch",
                "error": "scheduler submission record identity mismatch",
                "invalid_component": "record",
                "observed_identity": {"session_id": "corrupt-session-identity"},
            }
        return subprocess.CompletedProcess(command, 0, json.dumps(payload) + "\n", "")


class AmbiguousRemoteConnectorStartRunner(FakeRunner):
    """Lose one remote start response after the owned connector is live."""

    def __init__(self) -> None:
        super().__init__()
        self.lose_remote_start_response = True
        self.remote_start_attempts = 0

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "remote-frpc.toml" in script and "nohup" in script:
            self.remote_start_attempts += 1
            if self.remote_start_attempts > 1:
                pytest.fail("an ambiguous remote connector start must not be repeated")
            completed = super().run(
                command,
                input_text=input_text,
                timeout_seconds=timeout_seconds,
            )
            if self.lose_remote_start_response:
                self.lose_remote_start_response = False
                raise subprocess.TimeoutExpired(command, timeout_seconds or 120.0)
            return completed
        return super().run(
            command,
            input_text=input_text,
            timeout_seconds=timeout_seconds,
        )


def test_browser_proxy_secrets_exist_only_in_anonymous_stdin_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capability and upstream bearer avoid argv, env, files, records, and logs."""
    runner = FakeRunner()
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=ClioCoreQueue(settings.core_dir),
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    session = GatewaySession(cluster="test-cluster", name="browser-secret-handoff")
    runtime_dir = settings.core_dir.parent / "runtime-sessions" / session.session_id
    runtime_dir.mkdir(parents=True)
    config_path = runtime_dir / "browser-test.browser-gateway.json"
    stdout_path = runtime_dir / "browser-test.browser-gateway.out"
    stderr_path = runtime_dir / "browser-test.browser-gateway.err"
    metadata_path = runtime_dir / "browser-test.browser-gateway-owner.json"
    capability = "q" * 43
    bearer_token = "a" * 64
    authorization = f"Bearer {bearer_token}"
    config = service_runtime.BrowserGatewayConfig(
        attachment_id="browser-secret-test",
        token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
        bind_port=28778,
        upstream_protocol="http",
        upstream_port=28777,
        allowed_paths=["/healthz", "/commands"],
        command_path="/commands",
        expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        revocation_path=str((runtime_dir / "browser-test.revoked").resolve()),
    )
    intent: dict[str, object] = {
        "owner_token": "owner-token",
        "connector_generation_id": "generation-id",
        "config_path": str(config_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "metadata_path": str(metadata_path),
    }
    monkeypatch.setenv(service_runtime.CAPABILITY_ENV, "stale-capability")
    monkeypatch.setenv(
        service_runtime.UPSTREAM_AUTHORIZATION_ENV,
        f"Bearer {'b' * 64}",
    )

    proxy = supervisor._start_browser_proxy(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session=session,
        config=config,
        capability=capability,
        upstream_authorization=authorization,
        ownership_intent=intent,
    )

    assert runner.popen_inputs[0] is not None
    bootstrap = service_runtime.BrowserGatewayBootstrap.model_validate_json(runner.popen_inputs[0])
    assert bootstrap.capability == capability
    assert bootstrap.upstream_authorization == authorization
    environment = cast(dict[str, str], runner.popen_environments[0])
    assert service_runtime.CAPABILITY_ENV not in environment
    assert service_runtime.UPSTREAM_AUTHORIZATION_ENV not in environment
    assert environment["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] == "owner-token"
    assert environment["CLIO_RELAY_CONNECTOR_GENERATION_ID"] == "generation-id"
    non_pipe_surfaces = "\n".join(
        (
            json.dumps(runner.popen_commands[0], sort_keys=True),
            json.dumps(environment, sort_keys=True),
            config_path.read_text(encoding="utf-8"),
            json.dumps(proxy, sort_keys=True),
            metadata_path.read_text(encoding="utf-8"),
            stdout_path.read_text(encoding="utf-8"),
            stderr_path.read_text(encoding="utf-8"),
            session.model_dump_json(),
        )
    )
    assert capability not in non_pipe_surfaces
    assert authorization not in non_pipe_surfaces
    assert bearer_token not in non_pipe_surfaces


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


class LongPendingDeferredHostRunner(FakeRunner):
    """Hold one exact scheduler job pending before reporting its allocation."""

    def __init__(self, *, pending_polls: int) -> None:
        super().__init__()
        self.pending_polls = pending_polls
        self.scheduler_polls = 0
        self.fail_submission_reads = False

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if self.fail_submission_reads and "__CLIO_READ_SUBMISSION__" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            return subprocess.CompletedProcess(
                command,
                1,
                "",
                "temporary sidecar read failure",
            )
        if "__CLIO_CAPTURE_SUBMISSION__" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            output = '{"scheduler_job_id":"12345"}\n'
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
        if "clio-relay scheduler status 12345" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            self.scheduler_polls += 1
            phase = "pending" if self.scheduler_polls <= self.pending_polls else "running"
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "scheduler": "slurm",
                        "scheduler_job_id": "12345",
                        "phase": phase,
                        "raw_state": phase.upper(),
                    }
                )
                + "\n",
                "",
            )
        if "jarvis runtime status 12345" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            status = (
                {"state": "pending"}
                if self.scheduler_polls <= self.pending_polls
                else {"state": "allocated", "service_host": "compute-02"}
            )
            return subprocess.CompletedProcess(command, 0, json.dumps(status) + "\n", "")
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


class NotReadyThenHealthyRunner(LongPendingDeferredHostRunner):
    """Report an allocated service whose first bounded health observation is not ready."""

    def __init__(self) -> None:
        super().__init__(pending_polls=0)
        self.health_observations = 0

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        script = input_text or ""
        if "http.client.HTTPConnection" in script:
            self.commands.append(list(command))
            self.inputs.append(input_text)
            self.health_observations += 1
            if self.health_observations == 1:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    "service_health=not_ready\nservice_status=503\n",
                    "",
                )
        return super().run(command, input_text=input_text, timeout_seconds=timeout_seconds)


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

    assert isinstance(result, ServiceRuntimeStartResult)
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


@pytest.mark.parametrize("mode", ["remote", "local"])
def test_gateway_connector_hard_exit_resumes_exact_live_sidecar_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """A real process crash is recovered from sidecars without connector relaunch."""
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
    monkeypatch.setattr(
        service_runtime,
        "_observe_local_process",
        _REAL_OBSERVE_LOCAL_PROCESS,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="fixture-frpc",
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = queue.list_gateway_sessions(cluster="configured-target")[0]
    before_intents = cast(dict[str, object], session.gateway["ownership_intents"])
    expected_remote_generation = str(
        cast(dict[str, object], before_intents["remote_connector"])["connector_generation_id"]
    )
    expected_local_generation = (
        str(cast(dict[str, object], before_intents["desktop_connector"])["connector_generation_id"])
        if mode == "local"
        else None
    )
    runner = DurableFixtureRunner(state_path)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="configured-target",
        definition=crash_fixture_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("crash recovery must not poll or sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    resumed = supervisor.resume_start(session_id=session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.session_id == session.session_id
    assert resumed.session.scheduler_job_id == "fixture-job"
    transport = cast(dict[str, object], resumed.session.gateway["transport"])
    remote = cast(dict[str, object], transport["remote_connector"])
    local = cast(dict[str, object], transport["desktop_connector"])
    assert remote["connector_generation_id"] == expected_remote_generation
    if expected_local_generation is not None:
        assert local["connector_generation_id"] == expected_local_generation
    intents = cast(dict[str, object], resumed.session.gateway["ownership_intents"])
    assert cast(dict[str, object], intents["remote_connector"])["live_identity_verified"] is True
    if mode == "local":
        assert (
            cast(dict[str, object], intents["desktop_connector"])["live_identity_verified"] is True
        )

    stopped = supervisor.stop(session_id=session.session_id)
    assert stopped.errors == []
    assert stopped.canceled_scheduler_job is None


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
    assert 'native_open = getattr(os, "pidfd_open", None)' in stop_script
    assert "ctypes.c_long(434)" in stop_script
    assert 'native_send = getattr(signal, "pidfd_send_signal", None)' in stop_script
    assert "ctypes.c_long(424)" in stop_script
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


def test_cleanup_evidence_reports_requested_cancel_without_a_scheduler_resource() -> None:
    """Requested policy comes from durable intent, not observed resource cardinality."""
    operation_id = "gateway_cleanup_00000000000000000000000000000000"
    session = GatewaySession(
        cluster="test-cluster",
        name="external-runtime",
        state=GatewaySessionState.CLOSED,
        scheduler="external",
    )
    session = session.model_copy(
        update={
            "gateway": {
                "teardown_intent": {
                    "operation_id": operation_id,
                    "gateway_session_id": session.session_id,
                    "cancel_scheduler_job": True,
                }
            }
        }
    )
    result = ServiceRuntimeStopResult(
        session=session,
        mode="teardown",
        stopped_local_pid=None,
        stopped_remote_pid=None,
        canceled_scheduler_job=None,
        resources=[],
        errors=[],
    )

    cleanup = result.to_cleanup_evidence()

    assert cleanup.operation_id == operation_id
    assert cleanup.cancel_scheduler_jobs is True


def test_service_runtime_stop_rehydrates_cleanup_evidence_after_closed_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    local_stop_calls = 0
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def count_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        local_stop_calls += 1
        return original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(supervisor, "_stop_local_connector", count_local_stop)

    first = supervisor.stop(session_id=session_id)
    first_runner_inputs = list(runner.inputs)
    retried = supervisor.stop(session_id=session_id)

    assert retried == first
    assert runner.inputs == first_runner_inputs
    assert local_stop_calls == 1
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


def test_concurrent_same_policy_runtime_stop_executes_each_side_effect_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same-policy callers serialize and the waiter rehydrates exact evidence."""
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
    start_barrier = Barrier(2)
    both_calling = threading.Event()
    calls_lock = threading.Lock()
    callers_started = 0
    local_stop_calls = 0
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def count_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        with calls_lock:
            local_stop_calls += 1
        assert both_calling.wait(timeout=5)
        return original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(supervisor, "_stop_local_connector", count_local_stop)

    def stop_concurrently() -> ServiceRuntimeStopResult:
        nonlocal callers_started
        start_barrier.wait(timeout=5)
        with calls_lock:
            callers_started += 1
            if callers_started == 2:
                both_calling.set()
        return supervisor.stop(session_id=session_id, cancel_scheduler_job=False)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(stop_concurrently) for _index in range(2)]
        results = [future.result(timeout=15) for future in futures]

    assert results[0] == results[1]
    assert local_stop_calls == 1
    assert sum("__CLIO_STOP_CONNECTOR__" in (script or "") for script in runner.inputs) == 1
    assert sum("jarvis runtime status 12345" in (script or "") for script in runner.inputs) == 1


def test_runtime_start_serializes_connector_creation_against_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop cannot prove a starting connector absent and then let its producer launch."""
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
    connector_start_entered = threading.Event()
    stop_calling = threading.Event()
    teardown_prepared = threading.Event()
    original_remote_start = supervisor._start_remote_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    original_prepare = supervisor._prepare_teardown_intent  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def pause_remote_start(**kwargs: object) -> dict[str, object]:
        connector_start_entered.set()
        assert stop_calling.wait(timeout=5)
        assert not teardown_prepared.wait(timeout=0.1)
        return original_remote_start(**kwargs)  # pyright: ignore[reportArgumentType]

    def observe_teardown_prepare(
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        teardown_prepared.set()
        return original_prepare(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )

    monkeypatch.setattr(supervisor, "_start_remote_connector", pause_remote_start)
    monkeypatch.setattr(supervisor, "_prepare_teardown_intent", observe_teardown_prepare)

    with ThreadPoolExecutor(max_workers=2) as pool:
        start_future = pool.submit(
            supervisor.start,
            name="serialized-start",
            spec=_runtime_spec(),
        )
        assert connector_start_entered.wait(timeout=5)
        sessions = queue.list_gateway_sessions(cluster="test-cluster")
        assert len(sessions) == 1
        session_id = sessions[0].session_id

        def stop_while_starting() -> ServiceRuntimeStopResult:
            stop_calling.set()
            return supervisor.stop(session_id=session_id)

        stop_future = pool.submit(stop_while_starting)
        started = start_future.result(timeout=15)
        stopped = stop_future.result(timeout=15)

    assert started.session.state is GatewaySessionState.READY
    assert stopped.session.state is GatewaySessionState.CLOSED
    assert teardown_prepared.is_set()
    assert sum("__CLIO_STOP_CONNECTOR__" in (script or "") for script in runner.inputs) == 1
    assert stopped.canceled_scheduler_job is None
    assert runner.canceled_jobs == []


def test_detach_waits_for_concurrent_teardown_and_has_no_losing_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A detach that loses the shared transition lock rereads and refuses teardown."""
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
    policy_committed = threading.Event()
    detach_calling = threading.Event()
    original_policy = supervisor._prepare_teardown_policy  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    local_stop_calls = 0

    def pause_after_policy(
        session: GatewaySession,
        **kwargs: object,
    ) -> GatewaySession:
        policy = original_policy(session, **kwargs)  # pyright: ignore[reportArgumentType]
        policy_committed.set()
        assert detach_calling.wait(timeout=5)
        return policy

    def count_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        local_stop_calls += 1
        return original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(supervisor, "_prepare_teardown_policy", pause_after_policy)
    monkeypatch.setattr(supervisor, "_stop_local_connector", count_local_stop)

    def detach_after_policy() -> ServiceRuntimeStopResult:
        detach_calling.set()
        return supervisor.detach(session_id=session_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        stop_future = pool.submit(supervisor.stop, session_id=session_id)
        assert policy_committed.wait(timeout=5)
        detach_future = pool.submit(detach_after_policy)
        stopped = stop_future.result(timeout=15)
        with pytest.raises(ConfigurationError, match="is closed"):
            detach_future.result(timeout=15)

    assert stopped.session.state is GatewaySessionState.CLOSED
    assert local_stop_calls == 1
    assert sum("__CLIO_STOP_CONNECTOR__" in (script or "") for script in runner.inputs) == 1
    assert sum("jarvis runtime status 12345" in (script or "") for script in runner.inputs) == 1


def test_malformed_completed_runtime_teardown_fails_closed_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    completed = supervisor.stop(session_id=session_id)
    malformed = dict(cast(dict[str, object], completed.session.gateway["teardown"]))
    malformed.pop("resources")
    queue.update_gateway_session(
        session_id,
        allow_owned_runtime_close=True,
        gateway={**completed.session.gateway, "teardown": malformed},
    )
    prior_runner_inputs = list(runner.inputs)
    local_stop_calls = 0
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def count_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        local_stop_calls += 1
        return original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(supervisor, "_stop_local_connector", count_local_stop)

    with pytest.raises(RelayError, match="completed gateway teardown evidence is invalid"):
        supervisor.stop(session_id=session_id)

    assert local_stop_calls == 0
    assert runner.inputs == prior_runner_inputs


@pytest.mark.parametrize(
    "mutation",
    ["missing-scheduler", "unverified-desktop", "wrong-scheduler-provider"],
)
def test_completed_runtime_teardown_rejects_semantically_incomplete_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """Schema-valid replay evidence still requires every exact owned-resource proof."""
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
    completed = supervisor.stop(session_id=session_id)
    teardown = dict(cast(dict[str, object], completed.session.gateway["teardown"]))
    raw_resources = cast(list[object], teardown["resources"])
    resources = [dict(cast(dict[str, object], item)) for item in raw_resources]
    if mutation == "missing-scheduler":
        resources = [item for item in resources if item["kind"] != "scheduler_job"]
    elif mutation == "unverified-desktop":
        desktop = next(item for item in resources if item["kind"] == "desktop_connector")
        desktop["ownership_verified"] = False
        desktop["verified_after_operation"] = False
    else:
        scheduler = next(item for item in resources if item["kind"] == "scheduler_job")
        scheduler["provider"] = "slurm"
    teardown["resources"] = resources
    queue.update_gateway_session(
        session_id,
        allow_owned_runtime_close=True,
        gateway={**completed.session.gateway, "teardown": teardown},
    )
    prior_runner_inputs = list(runner.inputs)
    local_stop_calls = 0

    def forbidden_local_stop(**_kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        local_stop_calls += 1
        raise AssertionError("malformed completed evidence must fail before side effects")

    monkeypatch.setattr(supervisor, "_stop_local_connector", forbidden_local_stop)
    with pytest.raises(RelayError, match="completed gateway teardown evidence is invalid"):
        supervisor.stop(session_id=session_id)

    assert local_stop_calls == 0
    assert runner.inputs == prior_runner_inputs


def test_runtime_teardown_lock_timeout_fails_closed_before_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    lock_path = supervisor._gateway_transition_lock_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id
    )
    monkeypatch.setattr(service_runtime, "_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS", 0.02)
    prior_runner_inputs = list(runner.inputs)

    with (
        service_runtime.FileLock(
            str(service_runtime.internal_filesystem_path(lock_path, force_extended=True))
        ),
        pytest.raises(RelayError, match="timed out acquiring"),
    ):
        supervisor.stop(session_id=session_id)

    assert runner.inputs == prior_runner_inputs
    persisted = queue.get_gateway_session(session_id)
    assert persisted.gateway.get("teardown_intent") is None


def test_completed_runtime_teardown_rejects_final_state_policy_drift(
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
    first = supervisor.stop(session_id=session_id, final_state=GatewaySessionState.FAILED)
    prior_runner_inputs = list(runner.inputs)

    with pytest.raises(RelayError, match="final-state policy changed during retry"):
        supervisor.stop(session_id=session_id, final_state=GatewaySessionState.CLOSED)

    assert first.session.state is GatewaySessionState.FAILED
    assert runner.inputs == prior_runner_inputs


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


def test_service_runtime_keep_scheduler_rejects_transient_missing_record(
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
    assert result.session.state is GatewaySessionState.DEGRADED
    assert result.errors != []
    assert result.canceled_scheduler_job is None
    assert runner.provider_canceled_jobs == []
    assert scheduler.action == "retain"
    assert scheduler.outcome == "failed"
    assert scheduler.ownership_verified is True
    assert scheduler.verified_after_operation is False
    assert scheduler.observed_state == "unknown"
    assert scheduler.residual is True
    assert "verification remained unresolved: unknown" in (scheduler.detail or "")
    assert retention_check.status is ValidationStatus.FAILED
    assert report.status is ValidationStatus.FAILED


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
    cleanup = result.to_cleanup_evidence()
    assert cleanup.mode == "detach"
    assert cleanup.operation_id == result.session.gateway["detach_intent"]["operation_id"]
    assert cleanup.cancel_scheduler_jobs is False
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


def test_detached_starting_runtime_attach_is_single_observation_and_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        sleep=lambda _seconds: pytest.fail("one attach observation must not sleep"),
    )

    def health_pending(*_args: object, **_kwargs: object) -> None:
        raise RelayError("desktop readiness is still pending")

    supervisor._wait_for_local_health = health_pending  # type: ignore[method-assign]
    first = supervisor.start(name="detached-starting-runtime", spec=_runtime_spec())
    assert isinstance(first, ServiceRuntimePendingResult)
    first_transport = cast(dict[str, object], first.session.gateway["transport"])
    remote_generation = str(
        cast(dict[str, object], first_transport["remote_connector"])["connector_generation_id"]
    )
    detached = supervisor.detach(session_id=first.session.session_id)
    assert detached.errors == []

    pending = supervisor.attach(session_id=first.session.session_id)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.STARTING
    assert pending.session.session_id == first.session.session_id
    assert pending.session.scheduler_job_id == first.session.scheduler_job_id == "12345"
    pending_transport = cast(dict[str, object], pending.session.gateway["transport"])
    pending_remote = cast(dict[str, object], pending_transport["remote_connector"])
    pending_local = cast(dict[str, object], pending_transport["desktop_connector"])
    assert pending_remote["connector_generation_id"] == remote_generation
    local_generation = str(pending_local["connector_generation_id"])
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert (
        sum(
            "remote-frpc.toml" in (script or "") and "nohup" in (script or "")
            for script in runner.inputs
        )
        == 1
    )

    def prove_persisted_fake_connector_live(
        intent: dict[str, object],
        *,
        session_id: str,
    ) -> tuple[dict[str, object] | None, bool]:
        metadata_path = Path(str(intent["metadata_path"]))
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        assert payload["session_id"] == session_id
        return (
            {key: value for key, value in payload.items() if key != "schema_version"},
            False,
        )

    monkeypatch.setattr(
        service_runtime,
        "_discover_local_connector",
        prove_persisted_fake_connector_live,
    )
    observed: list[tuple[str, str, str, str]] = []
    for _day in range(3):
        repeated = supervisor.attach(session_id=first.session.session_id)
        assert isinstance(repeated, ServiceRuntimePendingResult)
        transport = cast(dict[str, object], repeated.session.gateway["transport"])
        observed.append(
            (
                repeated.session.session_id,
                str(repeated.session.scheduler_job_id),
                str(
                    cast(dict[str, object], transport["remote_connector"])[
                        "connector_generation_id"
                    ]
                ),
                str(
                    cast(dict[str, object], transport["desktop_connector"])[
                        "connector_generation_id"
                    ]
                ),
            )
        )
    assert set(observed) == {
        (first.session.session_id, "12345", remote_generation, local_generation)
    }
    assert len(runner.popen_commands) == 2
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_committed_gateway_teardown_intent_cannot_be_overwritten(tmp_path: Path) -> None:
    """A stale whole-gateway update cannot erase immutable cleanup policy."""
    queue, _settings, _definition_value, _runner, session_id = _started_session(tmp_path)
    stale = queue.get_gateway_session(session_id)
    committed = queue.prepare_gateway_teardown_intent(
        session_id,
        cancel_scheduler_job=False,
    )
    expected_intent = dict(cast(dict[str, object], committed.gateway["teardown_intent"]))

    with pytest.raises(QueueConflictError, match="teardown intent cannot be removed"):
        queue.update_gateway_session(session_id, gateway=stale.gateway)

    persisted = queue.get_gateway_session(session_id)
    assert persisted.gateway["teardown_intent"] == expected_intent


def test_attach_loses_to_concurrent_teardown_and_rolls_back_connector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teardown committed after connector start prevents stale attach publication."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
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
    supervisor.detach(session_id=session_id)
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    original_start = supervisor._start_local_visitor  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    committed_intent: dict[str, object] = {}

    def start_then_commit_teardown(
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        connector = original_start(
            session=session,
            spec=spec,
            proxy_name=proxy_name,
            ownership_intent=ownership_intent,
        )
        committed = queue.prepare_gateway_teardown_intent(
            session_id,
            cancel_scheduler_job=False,
        )
        committed_intent.update(cast(dict[str, object], committed.gateway["teardown_intent"]))
        return connector

    monkeypatch.setattr(supervisor, "_start_local_visitor", start_then_commit_teardown)

    with pytest.raises(QueueConflictError, match="changed during a runtime transition"):
        supervisor.attach(session_id=session_id)

    persisted = queue.get_gateway_session(session_id)
    assert persisted.gateway["teardown_intent"] == committed_intent
    assert "attached_at" not in persisted.metadata
    assert len(runner.popen_commands) == 2
    assert not _visitor_config_path(settings, session_id).exists()


def test_detach_loses_to_concurrent_teardown_without_erasing_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A teardown committed during detach prevents its stale final gateway write."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
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
    original_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    committed_intent: dict[str, object] = {}

    def stop_then_commit_teardown(
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        stopped = original_stop(
            session_id=session_id,
            connector=connector,
            require_record=require_record,
            absence_verified=absence_verified,
        )
        committed = queue.prepare_gateway_teardown_intent(
            session_id,
            cancel_scheduler_job=False,
        )
        committed_intent.update(cast(dict[str, object], committed.gateway["teardown_intent"]))
        return stopped

    monkeypatch.setattr(supervisor, "_stop_local_connector", stop_then_commit_teardown)

    with pytest.raises(QueueConflictError, match="changed during a runtime transition"):
        supervisor.detach(session_id=session_id)

    persisted = queue.get_gateway_session(session_id)
    assert persisted.gateway["teardown_intent"] == committed_intent
    assert "detach" not in persisted.gateway


def test_two_concurrent_attaches_create_at_most_one_connector_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialized attach callers reuse the one connector proven live by the winner."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
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
    supervisor.detach(session_id=session_id)
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    original_start = supervisor._start_local_visitor  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    created_generations: list[str] = []

    def track_start(**kwargs: object) -> dict[str, object]:
        connector = original_start(**kwargs)  # pyright: ignore[reportArgumentType]
        created_generations.append(str(connector["connector_generation_id"]))
        return connector

    def connector_status(connector: dict[str, object]) -> tuple[str, str | None]:
        generation = connector.get("connector_generation_id")
        if created_generations and generation == created_generations[-1]:
            return "owned", None
        return "missing", "the prior detached connector is absent"

    monkeypatch.setattr(supervisor, "_start_local_visitor", track_start)
    monkeypatch.setattr(service_runtime, "_local_connector_identity_status", connector_status)

    def attach_once() -> str:
        try:
            return supervisor.attach(session_id=session_id).session.state.value
        except QueueConflictError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(attach_once) for _index in range(2)]
        outcomes = [future.result() for future in futures]

    assert outcomes == ["ready", "ready"]
    assert len(created_generations) == 1
    assert len(runner.popen_commands) == 2
    assert queue.get_gateway_session(session_id).state is GatewaySessionState.READY


def test_attach_recovers_and_stops_local_connector_after_lost_start_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reattach rolls back a visitor found through its durable local sidecar."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
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
    supervisor.detach(session_id=session_id)
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    original_start = supervisor._start_local_visitor  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    stopped: list[int] = []
    launched_generation: str | None = None

    def start_then_lose_response(**kwargs: object) -> dict[str, object]:
        nonlocal launched_generation
        connector = original_start(**kwargs)  # pyright: ignore[reportArgumentType]
        launched_generation = str(connector["connector_generation_id"])
        raise RelayError("lost reattach connector start response")

    def connector_owned(
        connector: dict[str, object],
    ) -> tuple[str, str | None]:
        if connector.get("connector_generation_id") == launched_generation:
            return "owned", None
        return "missing", "the detached connector is absent"

    def stop_local(
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        del session_id, require_record, absence_verified
        pid = int(cast(int, connector["pid"]))
        stopped.append(pid)
        return pid, CleanupResource(
            kind="desktop_connector",
            resource_id=str(pid),
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="stopped",
            verified_after_operation=True,
        )

    monkeypatch.setattr(supervisor, "_start_local_visitor", start_then_lose_response)
    monkeypatch.setattr(service_runtime, "_local_connector_identity_status", connector_owned)
    monkeypatch.setattr(supervisor, "_stop_local_connector", stop_local)

    with pytest.raises(RelayError, match="lost reattach connector start response"):
        supervisor.attach(session_id=session_id)

    persisted = queue.get_gateway_session(session_id)
    assert persisted.state is GatewaySessionState.DEGRADED
    assert stopped == [555]
    assert persisted.metadata["attach_cleanup_error"] is None
    assert not _visitor_config_path(settings, session_id).exists()


def test_service_runtime_detach_is_idempotent_and_keeps_explicit_record_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    local_stop_calls = 0
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def count_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        nonlocal local_stop_calls
        local_stop_calls += 1
        return original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(supervisor, "_stop_local_connector", count_local_stop)
    first = supervisor.detach(session_id=session_id)
    first_runner_inputs = list(runner.inputs)
    second = supervisor.detach(session_id=session_id)

    assert second == first
    assert local_stop_calls == 1
    assert runner.inputs == first_runner_inputs
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


def test_service_runtime_detach_persists_intent_before_a_hard_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash after connector termination leaves a durable, retryable detach identity."""
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
    original_local_stop = supervisor._stop_local_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def crash_after_local_stop(**kwargs: object) -> tuple[int | None, CleanupResource]:
        original_local_stop(**kwargs)  # pyright: ignore[reportArgumentType]
        raise KeyboardInterrupt("simulated hard exit after local connector stop")

    monkeypatch.setattr(supervisor, "_stop_local_connector", crash_after_local_stop)
    with pytest.raises(KeyboardInterrupt, match="simulated hard exit"):
        supervisor.detach(session_id=session_id)

    interrupted = queue.get_gateway_session(session_id)
    intent = cast(dict[str, object], interrupted.gateway["detach_intent"])
    assert interrupted.state is GatewaySessionState.READY
    assert intent["schema_version"] == "clio-relay.gateway-detach-intent.v1"
    assert intent["gateway_session_id"] == session_id
    assert interrupted.metadata["detach_operation_id"] == intent["operation_id"]
    assert interrupted.metadata["detach_retryable"] is True
    assert "detach" not in interrupted.gateway
    assert runner.canceled_jobs == []

    monkeypatch.setattr(supervisor, "_stop_local_connector", original_local_stop)
    recovered = supervisor.detach(session_id=session_id)
    assert recovered.session.state is GatewaySessionState.DEGRADED
    assert recovered.session.gateway["detach"]["operation_id"] == intent["operation_id"]
    assert recovered.session.metadata["detach_retryable"] is False
    assert recovered.canceled_scheduler_job is None
    assert runner.canceled_jobs == []


@pytest.mark.parametrize("missing_kind", ["remote_connector", "scheduler_job"])
def test_completed_detach_rejects_missing_retention_evidence_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_kind: str,
) -> None:
    """A replay cannot omit either the retained connector or scheduler disposition."""
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
    completed = supervisor.detach(session_id=session_id)
    detach = dict(cast(dict[str, object], completed.session.gateway["detach"]))
    detach["resources"] = [
        item
        for item in cast(list[object], detach["resources"])
        if cast(dict[str, object], item)["kind"] != missing_kind
    ]
    queue.update_gateway_session(
        session_id,
        gateway={**completed.session.gateway, "detach": detach},
    )
    prior_runner_inputs = list(runner.inputs)

    def forbidden_local_stop(**_kwargs: object) -> tuple[int | None, CleanupResource]:
        raise AssertionError("malformed detach evidence must fail before side effects")

    monkeypatch.setattr(supervisor, "_stop_local_connector", forbidden_local_stop)
    with pytest.raises(RelayError, match="gateway detach evidence is invalid"):
        supervisor.detach(session_id=session_id)

    assert runner.inputs == prior_runner_inputs


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


@pytest.mark.parametrize("intent_state", ["missing", "malformed"])
def test_missing_scheduler_identity_proof_blocks_false_closure(
    tmp_path: Path,
    intent_state: str,
) -> None:
    """A nullable job ID needs explicit absence evidence, not silence or malformed state."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    session = queue.get_gateway_session(session_id)
    gateway = dict(session.gateway)
    intents = dict(cast(dict[str, object], gateway["ownership_intents"]))
    if intent_state == "missing":
        intents.pop("scheduler_submission")
    else:
        intents["scheduler_submission"] = {
            "schema_version": "clio-relay.gateway-ownership-intent.invalid",
            "state": "absent_verified",
        }
    gateway["ownership_intents"] = intents
    queue.update_gateway_session(
        session_id,
        scheduler_job_id=None,
        gateway=gateway,
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

    scheduler = [resource for resource in result.resources if resource.kind == "scheduler_job"]
    assert result.session.state is GatewaySessionState.DEGRADED
    assert len(scheduler) == 1
    assert scheduler[0].action == "retain"
    assert scheduler[0].ownership_verified is False
    assert scheduler[0].residual is True
    assert "exact job id" in (scheduler[0].detail or "")


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


def test_windows_connector_discovery_ignores_system_idle_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PID zero cannot own a connector and must not block marker discovery."""
    owner_token = "owner-token"
    generation_id = "generation-1"
    config_path = r"D:\owned\desktop-frpc.toml"
    command_line = (
        f"frpc -c {config_path} {owner_token} CLIO_RELAY_CONNECTOR_GENERATION_ID={generation_id}"
    )
    payload = json.dumps(
        [
            {"ProcessId": 0, "CommandLine": None},
            {"ProcessId": 555, "CommandLine": command_line},
        ]
    )

    def enumerate_processes(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, payload, "")

    monkeypatch.setattr(service_runtime.os, "name", "nt")
    monkeypatch.setattr(
        service_runtime,
        "_run_bounded_local_cleanup",
        enumerate_processes,
    )

    assert service_runtime._local_process_ids(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        command_markers=(owner_token, generation_id, config_path),
    ) == [555]


def test_windows_system_idle_process_allows_connector_absence_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restart cleanup can prove an unrecorded connector absent on Windows."""
    payload = json.dumps({"ProcessId": 0, "CommandLine": None})

    def enumerate_processes(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, payload, "")

    real_local_process_ids = service_runtime._local_process_ids  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def windows_process_ids(*, command_markers: tuple[str, ...] = ()) -> list[int]:
        with monkeypatch.context() as windows:
            windows.setattr(service_runtime.os, "name", "nt")
            return real_local_process_ids(command_markers=command_markers)

    def forbid_process_observation(_pid: int) -> object:
        raise AssertionError("PID zero must never reach connector identity observation")

    monkeypatch.setattr(
        service_runtime,
        "_run_bounded_local_cleanup",
        enumerate_processes,
    )
    monkeypatch.setattr(service_runtime, "_local_process_ids", windows_process_ids)
    monkeypatch.setattr(
        service_runtime,
        "_observe_local_process",
        forbid_process_observation,
    )
    session_id = "owned-session"
    connector, absence_verified = service_runtime._discover_local_connector(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {
            "owner_token": "owner-token",
            "connector_generation_id": "generation-1",
            "config_path": str(tmp_path / "desktop-frpc.toml"),
            "metadata_path": str(tmp_path / "desktop-frpc-owner.json"),
        },
        session_id=session_id,
    )

    assert connector is None
    assert absence_verified is True


@pytest.mark.parametrize("process_id", [None, -1, True, "555"])
def test_windows_connector_discovery_rejects_invalid_process_identity(
    monkeypatch: pytest.MonkeyPatch,
    process_id: object,
) -> None:
    """Ignoring PID zero must not weaken fail-closed process identity parsing."""
    payload = json.dumps({"ProcessId": process_id, "CommandLine": "frpc"})

    def enumerate_processes(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, payload, "")

    monkeypatch.setattr(service_runtime.os, "name", "nt")
    monkeypatch.setattr(
        service_runtime,
        "_run_bounded_local_cleanup",
        enumerate_processes,
    )

    with pytest.raises(
        RelayError,
        match="local Windows process enumeration returned an invalid process id",
    ):
        service_runtime._local_process_ids()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


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


def test_posix_connector_cleanup_uses_libc_when_python_omits_pidfd_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scans = iter([[555], [555]])
    opened: list[int] = []
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

    def libc_open(pid: int) -> int:
        opened.append(pid)
        return 93

    def libc_send(process_fd: int, sig: int) -> None:
        signaled.append((process_fd, sig))

    monkeypatch.setattr(
        service_runtime,
        "_local_connector_group_members",
        group_members,
    )
    monkeypatch.setattr(service_runtime.os, "pidfd_open", None, raising=False)
    monkeypatch.setattr(
        service_runtime.signal,
        "pidfd_send_signal",
        None,
        raising=False,
    )
    monkeypatch.setattr(service_runtime, "_linux_pidfd_open", libc_open)
    monkeypatch.setattr(service_runtime, "_linux_pidfd_send_signal", libc_send)
    monkeypatch.setattr(service_runtime.os, "close", closed.append)

    result = service_runtime._signal_owned_posix_connector_processes(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        connector,
        signal.SIGTERM,
    )

    assert result == [555]
    assert opened == [555]
    assert signaled == [(93, signal.SIGTERM)]
    assert closed == [93]


def test_linux_pidfd_raw_syscall_fallback_preserves_errno(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    syscall = _PidfdSyscallFixture(
        {
            424: (-1, errno.EPERM),
            434: (-1, errno.ESRCH),
        }
    )
    library = _PidfdLibcWithoutSymbols(syscall)

    def load_libc(_name: object, *, use_errno: bool) -> _PidfdLibcWithoutSymbols:
        assert use_errno is True
        return library

    monkeypatch.setattr(service_runtime.sys, "platform", "linux")
    monkeypatch.setattr(service_runtime.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(service_runtime.ctypes, "CDLL", load_libc)

    with pytest.raises(ProcessLookupError) as open_error:
        service_runtime._linux_pidfd_open(555)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert open_error.value.errno == errno.ESRCH

    with pytest.raises(PermissionError) as send_error:
        service_runtime._linux_pidfd_send_signal(93, signal.SIGTERM)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert send_error.value.errno == errno.EPERM
    assert syscall.calls == [434, 424]


def test_remote_pidfd_helpers_fall_back_and_preserve_errno(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    syscall = _PidfdSyscallFixture(
        {
            424: (-1, errno.EPERM),
            434: (93, 0),
        }
    )
    library = _PidfdLibcWithoutSymbols(syscall)

    def load_libc(_name: object, *, use_errno: bool) -> _PidfdLibcWithoutSymbols:
        assert use_errno is True
        return library

    monkeypatch.setattr(service_runtime.os, "pidfd_open", None, raising=False)
    monkeypatch.setattr(
        service_runtime.signal,
        "pidfd_send_signal",
        None,
        raising=False,
    )
    monkeypatch.setattr(service_runtime.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(service_runtime.ctypes, "CDLL", load_libc)
    stop_script = service_runtime._remote_stop_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id="gateway-fixture",
        pid=555,
    )
    stop_program = stop_script.split("<<'__CLIO_STOP_CONNECTOR__'\n", 1)[1].split(
        "\n__CLIO_STOP_CONNECTOR__",
        1,
    )[0]
    helper_start = stop_program.index("def open_process_fd")
    helper_end = stop_program.index("\ndef signal_owned_processes", helper_start)
    helper_program = stop_program[helper_start:helper_end]
    namespace: dict[str, object] = {
        "ctypes": service_runtime.ctypes,
        "errno": errno,
        "os": service_runtime.os,
        "platform": service_runtime.platform,
        "signal": service_runtime.signal,
    }
    exec(compile(helper_program, "remote-pidfd-helpers", "exec"), namespace)
    open_process_fd = cast(Callable[[int], int], namespace["open_process_fd"])
    send_process_fd_signal = cast(
        Callable[[int, int], None],
        namespace["send_process_fd_signal"],
    )

    assert open_process_fd(555) == 93
    with pytest.raises(PermissionError) as send_error:
        send_process_fd_signal(93, signal.SIGTERM)
    assert send_error.value.errno == errno.EPERM
    assert syscall.calls == [434, 424]


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


def test_service_runtime_failure_record_conflict_does_not_mask_original_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    def conflict_failure_record(
        *,
        session_id: str,
        error: BaseException,
        cleanup_errors: Sequence[str],
    ) -> None:
        del session_id, error, cleanup_errors
        raise QueueConflictError("injected failure-record race")

    monkeypatch.setattr(
        supervisor,
        "_record_runtime_start_failure",
        conflict_failure_record,
    )

    with pytest.raises(RelayError, match="scheduler unavailable") as caught:
        supervisor.start(name="generic-image-service", spec=_runtime_spec())

    assert caught.value.__notes__ == [
        "runtime failure handling could not persist its final record: injected failure-record race"
    ]
    assert queue.list_gateway_sessions(cluster="test-cluster")[0].state is (
        GatewaySessionState.FAILED
    )


def test_service_runtime_local_health_delay_retains_connectors_for_resume(
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
    pending = supervisor.start(name="delayed-health", spec=_runtime_spec())

    assert isinstance(pending, ServiceRuntimePendingResult)
    session = pending.session
    assert session.state is GatewaySessionState.STARTING
    assert session.queue_state == "running"
    assert session.metadata["runtime_observation_error"] == "desktop health failed"
    assert cast(dict[str, object], session.gateway["scheduler_status"])["state"] == "allocated"
    assert cast(dict[str, object], session.gateway["runtime_observation"])["state"] == ("not_ready")
    assert "teardown_intent" not in session.gateway
    transport = cast(dict[str, object], session.gateway["transport"])
    assert cast(dict[str, object], transport["remote_connector"])["pid"] == 444
    assert cast(dict[str, object], transport["desktop_connector"])["pid"] == 555
    assert not any("__CLIO_STOP_CONNECTOR__" in (script or "") for script in runner.inputs)
    assert runner.canceled_jobs == []

    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    resumed = supervisor.resume_start(session_id=session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.state is GatewaySessionState.READY
    assert "runtime_observation" not in resumed.session.gateway
    # The fake process has no live OS identity. Exact absence authorizes one new
    # desktop generation; the durable remote connector is still reused.
    assert len(runner.popen_commands) == 2


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


def test_service_runtime_pending_queue_time_does_not_consume_readiness_deadline(
    tmp_path: Path,
) -> None:
    clock = [0.0]

    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = LongPendingDeferredHostRunner(pending_polls=3)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("queued runtime observation must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    spec = _runtime_spec().model_copy(
        update={"readiness_timeout_seconds": 1.0, "scheduler": "slurm"}
    )

    pending = supervisor.start(name="long-queued-service", spec=spec)
    assert isinstance(pending, ServiceRuntimePendingResult)
    session_id = pending.session.session_id
    assert pending.session.state is GatewaySessionState.PENDING
    assert pending.retry_selector() == {
        "cluster": "test-cluster",
        "gateway_session_id": session_id,
        "scheduler_provider": "slurm",
        "scheduler_job_id": "12345",
    }

    for expected_poll in (2, 3):
        clock[0] += 86_400.0
        pending = supervisor.resume_start(session_id=session_id)
        assert isinstance(pending, ServiceRuntimePendingResult)
        assert pending.session.session_id == session_id
        assert pending.session.scheduler_job_id == "12345"
        assert runner.scheduler_polls == expected_poll
    assert not any("jarvis runtime status 12345" in (script or "") for script in runner.inputs)

    clock[0] += 86_400.0
    result = supervisor.resume_start(session_id=session_id)

    assert isinstance(result, ServiceRuntimeStartResult)
    assert clock[0] == 259_200.0
    assert runner.scheduler_polls == 4
    assert result.session.state is GatewaySessionState.READY
    assert result.session.session_id == session_id
    assert result.session.scheduler_job_id == "12345"
    assert result.session.node == "compute-02"
    assert sum("jarvis runtime status 12345" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert not any("__CLIO_STOP_CONNECTOR__" in (script or "") for script in runner.inputs)


def test_service_runtime_transient_scheduler_observation_stays_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = LongPendingDeferredHostRunner(pending_polls=0)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    original_poll = supervisor._poll_scheduler_provider  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    observations = 0

    def transient_poll(*, provider: str, scheduler_job_id: str) -> SchedulerStatus:
        nonlocal observations
        observations += 1
        if observations == 1:
            raise RelayError("scheduler status transport interrupted")
        return original_poll(provider=provider, scheduler_job_id=scheduler_job_id)

    monkeypatch.setattr(supervisor, "_poll_scheduler_provider", transient_poll)
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})

    pending = supervisor.start(name="transient-observation", spec=spec)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.PENDING
    assert pending.session.queue_state == "observation_unknown"
    assert pending.session.metadata["runtime_observation_error"] == (
        "scheduler status transport interrupted"
    )
    assert "teardown_intent" not in pending.session.gateway
    assert pending.session.metadata.get("last_error") is None

    resumed = supervisor.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.state is GatewaySessionState.READY
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_service_runtime_transient_submission_query_stays_resumable(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = LongPendingDeferredHostRunner(pending_polls=1)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    first = supervisor.start(name="transient-submission-query", spec=spec)
    assert isinstance(first, ServiceRuntimePendingResult)
    runner.fail_submission_reads = True

    pending = supervisor.resume_start(session_id=first.session.session_id)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.PENDING
    assert pending.session.queue_state == "observation_unknown"
    assert "temporary sidecar read failure" in str(
        pending.session.metadata["runtime_observation_error"]
    )
    assert "teardown_intent" not in pending.session.gateway
    assert runner.scheduler_polls == 1

    runner.fail_submission_reads = False
    resumed = supervisor.resume_start(session_id=first.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.state is GatewaySessionState.READY
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1


def test_service_runtime_allocated_but_unhealthy_returns_pending_without_waiting(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = NotReadyThenHealthyRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})

    pending = supervisor.start(name="allocated-not-healthy", spec=spec)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.ALLOCATED
    assert pending.session.node == "compute-02"
    assert pending.session.queue_state == "observation_unknown"
    assert runner.health_observations == 1
    assert "teardown_intent" not in pending.session.gateway

    resumed = supervisor.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.state is GatewaySessionState.READY
    assert runner.health_observations == 2
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1


def test_service_runtime_terminal_scheduler_job_is_not_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )

    def scheduler_status(*, provider: str, scheduler_job_id: str) -> SchedulerStatus:
        del provider, scheduler_job_id
        return SchedulerStatus(
            scheduler="slurm",
            scheduler_job_id="12345",
            phase=SchedulerPhase.FAILED,
            active_record_found=False,
        )

    monkeypatch.setattr(supervisor, "_poll_scheduler_provider", scheduler_status)

    with pytest.raises(RelayError, match="terminal state"):
        supervisor.start(
            name="terminal-before-ready",
            spec=_runtime_spec().model_copy(update={"scheduler": "slurm"}),
        )

    session = queue.list_gateway_sessions(cluster="test-cluster")[0]
    assert session.state is GatewaySessionState.FAILED
    assert session.queue_state != "pending"
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_service_runtime_missing_scheduler_observation_remains_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient scheduler visibility gap cannot terminalize a submitted job."""
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    runner = FakeRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )

    def scheduler_status(*, provider: str, scheduler_job_id: str) -> SchedulerStatus:
        del provider, scheduler_job_id
        return SchedulerStatus(
            scheduler="slurm",
            scheduler_job_id="12345",
            phase=SchedulerPhase.UNKNOWN,
            record_found=False,
            active_record_found=False,
        )

    monkeypatch.setattr(supervisor, "_poll_scheduler_provider", scheduler_status)

    pending = supervisor.start(
        name="temporarily-unobservable",
        spec=_runtime_spec().model_copy(update={"scheduler": "slurm"}),
    )

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.ALLOCATED
    assert pending.session.queue_state == "observation_unknown"
    assert pending.session.scheduler_job_id == "12345"
    assert pending.retry_selector()["scheduler_job_id"] == "12345"
    assert "absence is not terminal proof" in str(
        pending.session.metadata["runtime_observation_error"]
    )
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_resume_terminal_scheduler_observation_fails_without_canceling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    runner = LongPendingDeferredHostRunner(pending_polls=10)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    pending = supervisor.start(
        name="terminal-after-pending",
        spec=_runtime_spec().model_copy(update={"scheduler": "slurm"}),
    )
    assert isinstance(pending, ServiceRuntimePendingResult)
    terminal = SchedulerStatus(
        scheduler="slurm",
        scheduler_job_id="12345",
        phase=SchedulerPhase.FAILED,
        active_record_found=False,
    )

    def terminal_status(*, provider: str, scheduler_job_id: str) -> SchedulerStatus:
        del provider, scheduler_job_id
        return terminal

    monkeypatch.setattr(supervisor, "_poll_scheduler_provider", terminal_status)

    with pytest.raises(RelayError, match="terminal state"):
        supervisor.resume_start(session_id=pending.session.session_id)

    session = queue.get_gateway_session(pending.session.session_id)
    assert session.state is GatewaySessionState.FAILED
    assert session.scheduler_job_id == "12345"
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_resume_recovers_exact_scheduler_job_from_submission_sidecar(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    runner = LongPendingDeferredHostRunner(pending_polls=10)
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    submission_id = "submission-after-hard-exit"
    submission_marker = "marker-after-hard-exit"
    session = GatewaySession(
        cluster="test-cluster",
        name="sidecar-recovery",
        state=GatewaySessionState.SUBMITTED,
        scheduler="slurm",
        gateway={
            "runtime_spec": spec.model_dump(mode="json"),
            "transport": {"mode": spec.transport_mode},
            "ownership_intents": {
                "scheduler_submission": {
                    "schema_version": "clio-relay.gateway-ownership-intent.v1",
                    "state": "starting",
                    "submission_id": submission_id,
                    "scheduler_provider": "slurm",
                    "submission_marker": submission_marker,
                },
                "remote_connector": {
                    "schema_version": "clio-relay.gateway-ownership-intent.v1",
                    "state": "not_started",
                },
                "desktop_connector": {
                    "schema_version": "clio-relay.gateway-ownership-intent.v1",
                    "state": "not_started",
                },
            },
        },
        metadata={"owner": "clio-relay", "runtime_kind": spec.kind},
    )
    session = queue.create_gateway_session(session)
    runner.submission_record = {
        "schema_version": "clio-relay.gateway-submission-sidecar.v1",
        "present": True,
        "session_id": session.session_id,
        "submission_id": submission_id,
        "scheduler_provider": "slurm",
        "submission_marker": submission_marker,
        "returncode": 0,
        "output": '{"scheduler_job_id":"12345"}\n',
        "output_truncated": False,
    }
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )

    pending = supervisor.resume_start(session_id=session.session_id)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.scheduler_job_id == "12345"
    assert pending.session.state is GatewaySessionState.PENDING
    scheduler_intent = cast(
        dict[str, object],
        cast(dict[str, object], pending.session.gateway["ownership_intents"])[
            "scheduler_submission"
        ],
    )
    assert scheduler_intent["state"] == "recorded"
    assert scheduler_intent["scheduler_job_id"] == "12345"
    assert scheduler_intent["reconciled"] is True
    assert not any("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs)


@pytest.mark.parametrize("publish_sidecar", [False, True])
def test_ambiguous_scheduler_submit_is_query_only_and_resumable(
    tmp_path: Path,
    publish_sidecar: bool,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = AmbiguousSubmissionRunner(publish_sidecar=publish_sidecar)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("ambiguous submit recovery must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    pending = supervisor.start(name="ambiguous-submit", spec=_runtime_spec())

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.PENDING
    assert pending.session.scheduler_job_id is None
    selector = pending.retry_selector()
    assert selector["gateway_session_id"] == pending.session.session_id
    assert selector["scheduler_job_id"] is None
    assert isinstance(selector["submission_id"], str)
    assert isinstance(selector["submission_marker"], str)
    report = pending.to_live_validation_report()
    assert report.status is ValidationStatus.PENDING
    assert report.checks[0].summary == (
        "exact scheduler submission intent is durable; submission outcome is unresolved"
    )
    rendered_report = report.model_dump_json()
    assert "durably submitted" not in rendered_report
    assert "scheduler job preserved" not in rendered_report
    submission_resource = next(
        resource for resource in report.resources if resource.kind == "scheduler_submission"
    )
    assert submission_resource.resource_id == selector["submission_id"]
    assert submission_resource.metadata["scheduler_job_id"] is None
    assert "retained" not in submission_resource.metadata
    assert submission_resource.metadata["submission_outcome"] == "unresolved"
    assert submission_resource.metadata["cancel_requested"] is False
    assert submission_resource.metadata["resubmit_requested"] is False
    intent = cast(
        dict[str, object],
        cast(dict[str, object], pending.session.gateway["ownership_intents"])[
            "scheduler_submission"
        ],
    )
    assert intent["state"] == "starting"
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []

    if not publish_sidecar:
        observed: list[dict[str, object]] = []
        for _day in range(3):
            still_pending = supervisor.resume_start(session_id=pending.session.session_id)
            assert isinstance(still_pending, ServiceRuntimePendingResult)
            observed.append(still_pending.retry_selector())
        assert observed == [selector, selector, selector]
        assert runner.submit_attempts == 1
        runner.publish_sidecar = True
        output = '{"scheduler_job_id":"12345","service_host":"compute-01"}\n'
        runner.submission_record = {
            "schema_version": "clio-relay.gateway-submission-sidecar.v1",
            "present": True,
            "session_id": pending.session.session_id,
            "submission_id": selector["submission_id"],
            "scheduler_provider": "external",
            "submission_marker": selector["submission_marker"],
            "returncode": 0,
            "output": output,
            "output_truncated": False,
        }

    resumed = supervisor.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.session_id == pending.session.session_id
    assert resumed.session.scheduler_job_id == "12345"
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


@pytest.mark.parametrize("failure_kind", ["nonzero", "identity"])
def test_exact_failed_submission_sidecar_is_definitive_without_resubmit_or_cancel(
    tmp_path: Path,
    failure_kind: str,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = AmbiguousSubmissionRunner(publish_sidecar=False)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("definitive reconciliation must not sleep"),
    )
    pending = supervisor.start(name="definitive-submit-failure", spec=_runtime_spec())
    assert isinstance(pending, ServiceRuntimePendingResult)
    selector = pending.retry_selector()
    output = (
        "scheduler rejected the request\n"
        if failure_kind == "nonzero"
        else ('{"scheduler_job_id":"unexpected-job"}\n')
    )
    runner.submission_record = {
        "schema_version": "clio-relay.gateway-submission-sidecar.v1",
        "present": True,
        "session_id": (
            pending.session.session_id if failure_kind == "nonzero" else "tampered-session-identity"
        ),
        "submission_id": selector["submission_id"],
        "scheduler_provider": "external",
        "submission_marker": selector["submission_marker"],
        "returncode": 64 if failure_kind == "nonzero" else 0,
        "output": output,
        "output_truncated": False,
    }

    with pytest.raises(RelayError, match="sidecar identity|completed unsuccessfully"):
        supervisor.resume_start(session_id=pending.session.session_id)

    failed = queue.get_gateway_session(pending.session.session_id)
    assert failed.state is GatewaySessionState.FAILED
    assert failed.scheduler_job_id is None
    intents = cast(dict[str, object], failed.gateway["ownership_intents"])
    intent = cast(dict[str, object], intents["scheduler_submission"])
    assert intent["state"] == "starting"
    assert intent["submission_id"] == selector["submission_id"]
    assert intent["submission_marker"] == selector["submission_marker"]
    assert intent["reconciliation_outcome"] == "definitive_failure"
    expected_failure_kind = "command_failure" if failure_kind == "nonzero" else "integrity_failure"
    expected_queue_state = (
        "submission_failed" if failure_kind == "nonzero" else "submission_integrity_failed"
    )
    expected_submission_outcome = (
        "submit_command_failed" if failure_kind == "nonzero" else "unknown_due_to_integrity_failure"
    )
    assert intent["reconciliation_failure_kind"] == expected_failure_kind
    assert failed.queue_state == expected_queue_state
    assert failed.metadata["scheduler_submission_outcome"] == expected_submission_outcome
    evidence = cast(dict[str, object], intent["failure_evidence"])
    assert evidence["sidecar_present"] is True
    assert evidence["failure_kind"] == expected_failure_kind
    assert evidence["scheduler_submission_outcome"] == expected_submission_outcome
    assert evidence["cancel_requested"] is False
    assert evidence["resubmit_requested"] is False
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert not runner.popen_commands


def test_production_submission_verifier_mismatch_is_integrity_failure(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = ProductionSubmissionVerifierRunner(
        sidecar_state="record_identity_mismatch",
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("integrity reconciliation must not sleep"),
    )
    pending = supervisor.start(name="production-verifier-corruption", spec=_runtime_spec())
    assert isinstance(pending, ServiceRuntimePendingResult)

    with pytest.raises(RelayError, match="scheduler submission record identity mismatch"):
        supervisor.resume_start(session_id=pending.session.session_id)

    failed = queue.get_gateway_session(pending.session.session_id)
    assert failed.state is GatewaySessionState.FAILED
    assert failed.queue_state == "submission_integrity_failed"
    assert failed.scheduler_job_id is None
    assert failed.metadata["scheduler_submission_outcome"] == "unknown_due_to_integrity_failure"
    intents = cast(dict[str, object], failed.gateway["ownership_intents"])
    intent = cast(dict[str, object], intents["scheduler_submission"])
    assert intent["state"] == "starting"
    assert intent["reconciliation_outcome"] == "definitive_failure"
    assert intent["reconciliation_failure_kind"] == "integrity_failure"
    evidence = cast(dict[str, object], intent["failure_evidence"])
    assert evidence["failure_kind"] == "integrity_failure"
    assert evidence["error_code"] == "record_identity_mismatch"
    assert evidence["invalid_component"] == "record"
    assert evidence["scheduler_submission_outcome"] == ("unknown_due_to_integrity_failure")
    assert evidence["cancel_requested"] is False
    assert evidence["resubmit_requested"] is False
    assert runner.verifier_runs == 1
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert not runner.popen_commands


def test_production_submission_verifier_incomplete_output_remains_pending(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = ProductionSubmissionVerifierRunner(sidecar_state="output_incomplete")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("in-flight reconciliation must not sleep"),
    )
    pending = supervisor.start(name="production-verifier-in-flight", spec=_runtime_spec())
    assert isinstance(pending, ServiceRuntimePendingResult)
    selector = pending.retry_selector()

    resumed = supervisor.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimePendingResult)
    assert resumed.session.state is GatewaySessionState.PENDING
    assert resumed.session.scheduler_job_id is None
    assert resumed.retry_selector() == selector
    assert resumed.session.metadata.get("scheduler_submission_outcome") != (
        "unknown_due_to_integrity_failure"
    )
    assert runner.verifier_runs == 1
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert not runner.popen_commands


def test_unresolved_submission_detach_replay_and_three_day_reconnect_are_exact(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = AmbiguousSubmissionRunner(publish_sidecar=False)
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("detached unresolved submission must not sleep"),
    )
    pending = supervisor.start(name="unresolved-detach", spec=_runtime_spec())
    assert isinstance(pending, ServiceRuntimePendingResult)
    selector = pending.retry_selector()

    detached = supervisor.detach(session_id=pending.session.session_id)

    assert detached.errors == []
    assert detached.residual_resources == []
    assert detached.session.state is GatewaySessionState.DEGRADED
    assert detached.session.scheduler_job_id is None
    assert detached.canceled_scheduler_job is None
    resources = {resource.kind: resource for resource in detached.resources}
    assert "scheduler_job" not in resources
    submission = resources["scheduler_submission"]
    assert submission.resource_id == selector["submission_id"]
    assert submission.outcome == "retained"
    assert submission.observed_state == "intent_recorded"
    assert submission.metadata["scheduler_job_id"] is None
    assert submission.metadata["cancel_requested"] is False
    assert submission.metadata["resubmit_requested"] is False
    assert resources["desktop_connector"].outcome == "missing"
    assert resources["remote_connector"].outcome == "missing"
    assert resources["remote_connector"].observed_state == "not_created"
    report = detached.to_live_validation_report()
    assert report.status is ValidationStatus.PASSED
    rendered_report = report.model_dump_json()
    assert "scheduler job preserved" not in rendered_report
    assert "remote connector retained" not in rendered_report
    assert "no job, cancellation, or resubmission is claimed" in rendered_report

    replay = supervisor.detach(session_id=pending.session.session_id)
    assert replay == detached

    observed: list[dict[str, object]] = []
    for _day in range(3):
        reconnected = supervisor.attach(session_id=pending.session.session_id)
        assert isinstance(reconnected, ServiceRuntimePendingResult)
        observed.append(reconnected.retry_selector())
        assert reconnected.session.scheduler_job_id is None
        intents = cast(dict[str, object], reconnected.session.gateway["ownership_intents"])
        scheduler_intent = cast(dict[str, object], intents["scheduler_submission"])
        assert scheduler_intent["state"] == "starting"
        assert scheduler_intent["submission_id"] == selector["submission_id"]
        assert scheduler_intent["submission_marker"] == selector["submission_marker"]
        assert cast(dict[str, object], intents["remote_connector"])["state"] == ("not_started")
        assert cast(dict[str, object], intents["desktop_connector"])["state"] == ("not_started")

    assert observed == [selector, selector, selector]
    assert runner.submit_attempts == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []
    assert not runner.popen_commands


def test_ambiguous_remote_connector_start_recovers_same_generation_without_relaunch(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = AmbiguousRemoteConnectorStartRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("connector recovery must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    pending = supervisor.start(name="ambiguous-remote-connector", spec=_runtime_spec())

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.STARTING
    intent = cast(
        dict[str, object],
        cast(dict[str, object], pending.session.gateway["ownership_intents"])["remote_connector"],
    )
    generation_id = str(intent["connector_generation_id"])
    assert intent["state"] == "starting"
    assert runner.remote_start_attempts == 1

    resumed = supervisor.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    transport = cast(dict[str, object], resumed.session.gateway["transport"])
    remote = cast(dict[str, object], transport["remote_connector"])
    assert remote["connector_generation_id"] == generation_id
    assert runner.remote_start_attempts == 1
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_resume_rejects_incomplete_published_connector_records_without_relaunch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = LongPendingDeferredHostRunner(pending_polls=0)
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    submission_id = "connector-sidecar-submission"
    submission_marker = "connector-sidecar-marker"
    session = GatewaySession(
        cluster="test-cluster",
        name="connector-sidecar-recovery",
        state=GatewaySessionState.STARTING,
        scheduler="slurm",
        scheduler_job_id="12345",
        queue_state="running",
        node="compute-02",
        metadata={"owner": "clio-relay", "runtime_kind": spec.kind},
    )
    remote_connector: dict[str, object] = {
        "owner": "clio-relay",
        "session_id": session.session_id,
        "pid": 444,
        "connector_generation_id": "remote-generation",
    }
    desktop_connector: dict[str, object] = {
        "owner": "clio-relay",
        "session_id": session.session_id,
        "pid": 555,
        "connector_generation_id": "desktop-generation",
    }
    session = session.model_copy(
        update={
            "gateway": {
                "runtime_spec": spec.model_dump(mode="json"),
                "transport": {
                    "mode": spec.transport_mode,
                    "proxy_name": "connector-sidecar-recovery",
                    "remote_connector": remote_connector,
                    "desktop_connector": desktop_connector,
                },
                "ownership_intents": {
                    "scheduler_submission": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "recorded",
                        "submission_id": submission_id,
                        "scheduler_provider": "slurm",
                        "submission_marker": submission_marker,
                        "scheduler_job_id": "12345",
                    },
                    "remote_connector": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "recorded",
                        **remote_connector,
                    },
                    "desktop_connector": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "recorded",
                        **desktop_connector,
                    },
                },
            }
        }
    )
    session = queue.create_gateway_session(session)
    runner.submission_record = {
        "schema_version": "clio-relay.gateway-submission-sidecar.v1",
        "present": True,
        "session_id": session.session_id,
        "submission_id": submission_id,
        "scheduler_provider": "slurm",
        "submission_marker": submission_marker,
        "returncode": 0,
        "output": '{"scheduler_job_id":"12345"}\n',
        "output_truncated": False,
    }
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    supervisor._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    def refuse_relaunch(**_kwargs: object) -> dict[str, object]:
        pytest.fail("a published connector sidecar must be adopted, not relaunched")

    monkeypatch.setattr(supervisor, "_start_remote_connector", refuse_relaunch)
    monkeypatch.setattr(supervisor, "_start_local_visitor", refuse_relaunch)

    resumed = supervisor.resume_start(session_id=session.session_id)

    assert isinstance(resumed, ServiceRuntimePendingResult)
    assert resumed.session.state is GatewaySessionState.STARTING
    transport = cast(dict[str, object], resumed.session.gateway["transport"])
    assert transport["remote_connector"] == remote_connector
    assert transport["desktop_connector"] == desktop_connector
    assert not runner.popen_commands
    assert "owner_token" in str(resumed.session.metadata["runtime_observation_error"])


@pytest.mark.parametrize("role", ["remote_connector", "desktop_connector"])
def test_transient_connector_discovery_preserves_exact_intent_and_returns_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = TransientConnectorDiscoveryRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("connector recovery must not sleep"),
    )

    def health_pending(*_args: object, **_kwargs: object) -> None:
        raise RelayError("desktop health is still starting")

    supervisor._wait_for_local_health = health_pending  # type: ignore[method-assign]
    first = supervisor.start(name="connector-recovery-pending", spec=_runtime_spec())
    assert isinstance(first, ServiceRuntimePendingResult)
    before_intents = cast(dict[str, object], first.session.gateway["ownership_intents"])
    before_intent = dict(cast(dict[str, object], before_intents[role]))
    before_transport = cast(dict[str, object], first.session.gateway["transport"])
    before_connector = dict(cast(dict[str, object], before_transport[role]))
    remote_starts = sum(
        "remote-frpc.toml" in (script or "") and "nohup" in (script or "")
        for script in runner.inputs
    )
    local_starts = len(runner.popen_commands)

    if role == "remote_connector":
        runner.fail_remote_discovery = True
    else:

        def fail_local_discovery(
            _intent: dict[str, object],
            *,
            session_id: str,
        ) -> tuple[dict[str, object] | None, bool]:
            del session_id
            raise RelayError("temporary desktop connector sidecar read failure")

        monkeypatch.setattr(
            service_runtime,
            "_discover_local_connector",
            fail_local_discovery,
        )

    observed_ids: list[tuple[str, str, str, str]] = []
    for _day in range(3):
        pending = supervisor.resume_start(session_id=first.session.session_id)
        assert isinstance(pending, ServiceRuntimePendingResult)
        assert pending.session.state is GatewaySessionState.STARTING
        intents = cast(dict[str, object], pending.session.gateway["ownership_intents"])
        intent = cast(dict[str, object], intents[role])
        transport = cast(dict[str, object], pending.session.gateway["transport"])
        connector = cast(dict[str, object], transport[role])
        observed_ids.append(
            (
                pending.session.session_id,
                str(pending.session.scheduler_job_id),
                str(intent["connector_generation_id"]),
                str(connector["connector_generation_id"]),
            )
        )
        assert intent["state"] == before_intent["state"] == "recorded"
        assert intent["owner_token"] == before_intent["owner_token"]
        assert connector == before_connector
        assert "temporary" in str(pending.session.metadata["runtime_observation_error"])

    assert len(set(observed_ids)) == 1
    assert remote_starts == sum(
        "remote-frpc.toml" in (script or "") and "nohup" in (script or "")
        for script in runner.inputs
    )
    assert len(runner.popen_commands) == local_starts
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


@pytest.mark.parametrize("role", ["remote_connector", "desktop_connector"])
def test_tampered_connector_record_is_not_adopted_or_replaced(
    tmp_path: Path,
    role: str,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    runner = TransientConnectorDiscoveryRunner()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("tamper rejection must not sleep"),
    )

    def health_pending(*_args: object, **_kwargs: object) -> None:
        raise RelayError("desktop health is still starting")

    supervisor._wait_for_local_health = health_pending  # type: ignore[method-assign]
    first = supervisor.start(name="connector-record-tamper", spec=_runtime_spec())
    assert isinstance(first, ServiceRuntimePendingResult)
    current = queue.get_gateway_session(first.session.session_id)
    transport = dict(cast(dict[str, object], current.gateway["transport"]))
    connector = dict(cast(dict[str, object], transport[role]))
    connector["owner_token"] = "tampered-owner-token"
    transport[role] = connector
    queue.update_gateway_session(
        current.session_id,
        expected_updated_at=current.updated_at,
        gateway={**current.gateway, "transport": transport},
    )
    remote_starts = sum(
        "remote-frpc.toml" in (script or "") and "nohup" in (script or "")
        for script in runner.inputs
    )
    local_starts = len(runner.popen_commands)

    pending = supervisor.resume_start(session_id=current.session_id)

    assert isinstance(pending, ServiceRuntimePendingResult)
    assert pending.session.state is GatewaySessionState.STARTING
    persisted_transport = cast(dict[str, object], pending.session.gateway["transport"])
    assert cast(dict[str, object], persisted_transport[role])["owner_token"] == (
        "tampered-owner-token"
    )
    assert "durable intent" in str(pending.session.metadata["runtime_observation_error"])
    assert remote_starts == sum(
        "remote-frpc.toml" in (script or "") and "nohup" in (script or "")
        for script in runner.inputs
    )
    assert len(runner.popen_commands) == local_starts
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_pending_runtime_detach_and_reconnect_resumes_exact_submission(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    queue = ClioCoreQueue(settings.core_dir)
    runner = LongPendingDeferredHostRunner(pending_polls=2)
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )

    pending = supervisor.start(name="overnight-queued", spec=spec)
    assert isinstance(pending, ServiceRuntimePendingResult)

    detached = supervisor.detach(session_id=pending.session.session_id)

    assert detached.session.state is GatewaySessionState.DEGRADED
    assert detached.errors == []
    assert detached.residual_resources == []
    assert detached.session.scheduler_job_id == "12345"
    assert detached.canceled_scheduler_job is None

    reconnected = ServiceRuntimeSupervisor(
        settings=settings,
        queue=ClioCoreQueue(settings.core_dir),
        cluster="test-cluster",
        definition=_definition(),
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: pytest.fail("runtime observation must not sleep"),
    )
    reconnected._wait_for_local_health = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    resumed = reconnected.resume_start(session_id=pending.session.session_id)

    assert isinstance(resumed, ServiceRuntimeStartResult)
    assert resumed.session.state is GatewaySessionState.READY
    assert resumed.session.scheduler_job_id == "12345"
    assert "detach_intent" not in resumed.session.gateway
    assert "detach" not in resumed.session.gateway
    assert sum("__CLIO_CAPTURE_SUBMISSION__" in (script or "") for script in runner.inputs) == 1
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


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

    session = queue.list_gateway_sessions(cluster="test-cluster")[0]
    assert session.state is GatewaySessionState.FAILED
    assert session.metadata["last_error"] == (
        "service host was not reported by submission output; "
        "ServiceRuntimeSpec.status_command is required"
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

    session = queue.list_gateway_sessions(cluster="test-cluster")[0]
    assert session.state is GatewaySessionState.FAILED
    assert session.metadata["last_error"] == (
        "deployment output must include a JSON object: 'deployment accepted as job 67890\\n'"
    )


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


def test_gateway_start_runtime_cli_returns_resumable_pending_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path = tmp_path / "runtime.json"
    spec = _runtime_spec().model_copy(update={"scheduler": "slurm"})
    spec_path.write_text(spec.model_dump_json(), encoding="utf-8")
    cluster_path = tmp_path / ".clio-relay" / "clusters.json"
    cluster_path.parent.mkdir(parents=True)
    cluster_path.write_text(
        '{"clusters":{"test-cluster":' + _definition().model_dump_json() + "}}",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def fake_start(
        self: ServiceRuntimeSupervisor,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
    ) -> ServiceRuntimePendingResult:
        del owner_session_id, owner_session_generation_id, owner_session_admission_id
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster="test-cluster",
                name=name,
                state=GatewaySessionState.PENDING,
                scheduler="slurm",
                scheduler_job_id="998877",
                queue_state="pending",
                gateway={"runtime_spec": spec.model_dump(mode="json")},
                metadata={"owner": "clio-relay", "runtime_kind": spec.kind},
            )
        )
        return ServiceRuntimePendingResult(session=session)

    def forbid_worker_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        del observed_worker_info
        pytest.fail("pending runtime must return before worker provenance observation")

    monkeypatch.setattr(ServiceRuntimeSupervisor, "start", fake_start)
    monkeypatch.setattr(relay_cli, "_attach_verified_remote_worker", forbid_worker_identity)
    report_path = tmp_path / "gateway-pending.json"

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "start-runtime",
            "--cluster",
            "test-cluster",
            "--name",
            "queued-runtime",
            "--runtime-json-file",
            str(spec_path),
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["outcome"] == "pending"
    assert payload["scheduler_job_id"] == "998877"
    assert payload["scheduler_action"] == "none"
    assert payload["relay_action"] == "none"
    assert payload["retry_selector"] == {
        "cluster": "test-cluster",
        "gateway_session_id": payload["session_id"],
        "scheduler_provider": "slurm",
        "scheduler_job_id": "998877",
    }
    report = LiveValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    assert report.status is ValidationStatus.PENDING
    assert report.checks[0].status is ValidationStatus.PENDING
    assert report.error is None
    assert any(
        resource.kind == "scheduler_job"
        and resource.resource_id == "998877"
        and resource.metadata["retained"] is True
        for resource in report.resources
    )

    resumed_session_ids: list[str] = []

    def fake_resume(
        self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
    ) -> ServiceRuntimePendingResult:
        resumed_session_ids.append(session_id)
        return ServiceRuntimePendingResult(session=self.queue.get_gateway_session(session_id))

    monkeypatch.setattr(ServiceRuntimeSupervisor, "resume_start", fake_resume)
    resume_report_path = tmp_path / "gateway-resume-pending.json"
    resumed = CliRunner().invoke(
        app,
        [
            "gateway",
            "resume-runtime",
            payload["session_id"],
            "--cluster",
            "test-cluster",
            "--validation-report",
            str(resume_report_path),
        ],
    )

    assert resumed.exit_code == 0, resumed.output
    assert resumed_session_ids == [payload["session_id"]]
    resumed_payload = json.loads(resumed.output)
    assert resumed_payload["retry_selector"] == payload["retry_selector"]
    resumed_report = LiveValidationReport.model_validate_json(
        resume_report_path.read_text(encoding="utf-8")
    )
    assert resumed_report.status is ValidationStatus.PENDING


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
    ).model_copy(
        update={
            "state": GatewaySessionState.CLOSED,
            "gateway": {
                "runtime_spec": _runtime_spec().model_dump(mode="json"),
                "teardown_intent": {
                    "schema_version": "clio-relay.gateway-teardown-intent.v1",
                    "operation_id": "gateway_cleanup_canonical_failure",
                    "gateway_session_id": "canonical-failure",
                    "cancel_scheduler_job": True,
                    "created_at": "2026-07-19T00:00:00Z",
                },
            },
        }
    )
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
        sleep=time.sleep,
    )


def _mock_http_client_factory(
    transport: httpx.BaseTransport,
) -> Callable[[float], httpx.Client]:
    """Return fresh operation-owned clients over one deterministic transport."""

    def client_factory(_timeout_seconds: float) -> httpx.Client:
        return httpx.Client(transport=transport)

    return client_factory


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

    transport = httpx.MockTransport(lambda _request: httpx.Response(status_code, content=body))
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )
    with pytest.raises(RelayError, match=error):
        supervisor._wait_for_local_health(  # pyright: ignore[reportPrivateUsage]
            "http://127.0.0.1:28777/healthz",
            2.0,
            0.01,
            expected_body="runtime-nonce",
            max_attempts=1,
        )


def test_local_health_probe_accepts_exact_2xx_runtime_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)

    transport = httpx.MockTransport(lambda _request: httpx.Response(200, content=b"runtime-nonce"))
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )
    supervisor._wait_for_local_health(  # pyright: ignore[reportPrivateUsage]
        "http://127.0.0.1:28777/healthz",
        1.0,
        0.1,
        expected_body="runtime-nonce",
    )


def test_jarvis_v2_health_accepts_opaque_body_only_after_anonymous_401_and_bearer_2xx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    authorization = f"Bearer {'a' * 64}"
    observed_headers: list[str | None] = []

    def protected_health(request: httpx.Request) -> httpx.Response:
        observed = request.headers.get("authorization")
        observed_headers.append(observed)
        if observed is None:
            return httpx.Response(401, content=b"not-json-and-not-inspected")
        return httpx.Response(200, content=b"\xff\x00opaque-health")

    transport = httpx.MockTransport(protected_health)
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )

    supervisor._wait_for_jarvis_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "http://127.0.0.1:28777/healthz",
        timeout_seconds=1.0,
        poll_seconds=0.1,
        runtime_schema_version="jarvis.service-runtime.v2",
        authorization=authorization,
    )

    assert observed_headers == [None, authorization]


def test_jarvis_v2_health_rejects_an_unprotected_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    observed_headers: list[str | None] = []

    def unprotected_health(request: httpx.Request) -> httpx.Response:
        observed_headers.append(request.headers.get("authorization"))
        return httpx.Response(200, content=b"ignored")

    transport = httpx.MockTransport(unprotected_health)
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )

    with pytest.raises(RelayError, match="accepted an anonymous request"):
        supervisor._wait_for_jarvis_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "http://127.0.0.1:28777/healthz",
            timeout_seconds=1.0,
            poll_seconds=0.1,
            runtime_schema_version="jarvis.service-runtime.v2",
            authorization=f"Bearer {'a' * 64}",
        )

    assert observed_headers == [None]


def test_legacy_jarvis_v1_health_accepts_anonymous_opaque_2xx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    observed_headers: list[str | None] = []

    def legacy_health(request: httpx.Request) -> httpx.Response:
        observed_headers.append(request.headers.get("authorization"))
        return httpx.Response(200, content=b"\xfflegacy-opaque")

    transport = httpx.MockTransport(legacy_health)
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )

    supervisor._wait_for_jarvis_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "http://127.0.0.1:28777/healthz",
        timeout_seconds=1.0,
        poll_seconds=0.1,
        runtime_schema_version="jarvis.service-runtime.v1",
        authorization=None,
    )

    assert observed_headers == [None]


def test_browser_health_accepts_opaque_2xx_with_exact_null_origin_cors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    observed_origins: list[str | None] = []

    def browser_health(request: httpx.Request) -> httpx.Response:
        observed_origins.append(request.headers.get("origin"))
        return httpx.Response(
            200,
            headers={"Access-Control-Allow-Origin": "null"},
            content=b"\xffopaque-browser-health",
        )

    transport = httpx.MockTransport(browser_health)
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )

    supervisor._wait_for_browser_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "http://127.0.0.1:28778/healthz?capability=private",
        timeout_seconds=1.0,
        poll_seconds=0.1,
    )

    assert observed_origins == ["null"]


def test_browser_health_rejects_wildcard_cors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = _health_probe_supervisor(tmp_path)
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            headers={"Access-Control-Allow-Origin": "*"},
            content=b"ignored",
        )
    )
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )

    with pytest.raises(RelayError, match="not exactly null"):
        supervisor._wait_for_browser_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "http://127.0.0.1:28778/healthz?capability=private",
            timeout_seconds=0.03,
            poll_seconds=1.0,
        )


def test_authenticated_jarvis_health_failure_redacts_echoed_bearer_from_error_and_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authenticated upstream cannot reflect its bearer into durable relay state."""
    supervisor = _health_probe_supervisor(tmp_path)
    token = "a" * 64
    authorization = f"Bearer {token}"
    response_body = json.dumps(
        {"echoed_authorization": authorization, "private_marker": "health-body-secret"}
    ).encode("utf-8")
    observed_headers: list[str | None] = []

    def echo_bearer(request: httpx.Request) -> httpx.Response:
        observed = request.headers.get("authorization")
        observed_headers.append(observed)
        if observed is None:
            return httpx.Response(401, content=b"authorization required")
        return httpx.Response(503, content=response_body)

    transport = httpx.MockTransport(echo_bearer)
    monkeypatch.setattr(
        service_runtime,
        "_new_readiness_http_client",
        _mock_http_client_factory(transport),
    )
    with pytest.raises(RelayError, match="authenticated health status=503") as caught:
        supervisor._wait_for_jarvis_health(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "http://127.0.0.1:28777/healthz",
            timeout_seconds=1.0,
            poll_seconds=1.0,
            runtime_schema_version="jarvis.service-runtime.v2",
            authorization=authorization,
        )

    error_text = str(caught.value)
    assert observed_headers == [None, authorization]
    assert token not in error_text
    assert "health-body-secret" not in error_text
    supervisor.queue.initialize()
    session = supervisor.queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="authenticated-health-failure",
            state=GatewaySessionState.STARTING,
        )
    )
    supervisor._record_attach_failure(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id=session.session_id,
        error=caught.value,
        cleanup_error=None,
    )
    persisted = supervisor.queue.get_gateway_session(session.session_id).model_dump_json()
    assert token not in persisted
    assert "health-body-secret" not in persisted


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


def test_local_readiness_responses_enforce_fixed_chunked_and_compressed_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness never buffers beyond its decompressed response budget."""
    maximum = 128

    def assert_rejected(response: httpx.Response) -> None:
        transport = httpx.MockTransport(lambda _request: response)
        with httpx.Client(transport=transport) as client:

            def client_factory(_timeout_seconds: float) -> httpx.Client:
                return client

            monkeypatch.setattr(
                service_runtime,
                "_new_readiness_http_client",
                client_factory,
            )
            with pytest.raises(ValueError, match="decompressed limit"):
                service_runtime._read_bounded_http_response(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    "http://readiness.invalid/state",
                    headers=None,
                    maximum_bytes=maximum,
                )

    assert_rejected(
        httpx.Response(
            200,
            headers={"Content-Length": str(maximum + 1)},
            content=b"{}",
        )
    )
    assert_rejected(httpx.Response(200, content=b"x" * (maximum + 1)))
    compressed = gzip.compress(b"x" * (maximum + 1))
    assert len(compressed) < maximum
    assert_rejected(
        httpx.Response(
            200,
            headers={
                "Content-Encoding": "gzip",
                "Content-Length": str(len(compressed)),
            },
            content=compressed,
        )
    )


def test_local_readiness_response_has_one_absolute_slow_drip_deadline() -> None:
    """Frequent response bytes cannot reset the readiness operation's total budget."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(1.0)
    port = int(listener.getsockname()[1])
    stop = threading.Event()

    def serve_slow_response() -> None:
        try:
            connection, _address = listener.accept()
        except OSError:
            return
        with connection:
            connection.settimeout(1.0)
            try:
                connection.recv(64 * 1024)
                connection.sendall(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: 1024\r\n"
                    b"Connection: close\r\n\r\n"
                )
                while not stop.wait(0.02):
                    connection.sendall(b"x")
            except OSError:
                return

    server_thread = threading.Thread(target=serve_slow_response, daemon=True)
    server_thread.start()
    started_at = time.monotonic()
    try:
        deadline = started_at + 0.2
        with pytest.raises(httpx.TimeoutException, match="total monotonic deadline"):
            service_runtime._read_bounded_http_response(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                f"http://127.0.0.1:{port}/state",
                headers=None,
                maximum_bytes=2048,
                deadline=deadline,
            )
        elapsed = time.monotonic() - started_at
        assert 0.1 <= elapsed < 0.8
    finally:
        stop.set()
        listener.close()
        server_thread.join(timeout=1.0)


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
