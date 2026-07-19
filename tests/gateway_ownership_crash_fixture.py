"""Hard-exit at scheduler and connector ownership-persistence windows."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import ServiceRuntimeSpec
from clio_relay.service_runtime import (
    CommandRunner,
    LocalConnectorIdentity,
    ServiceRuntimeSupervisor,
    _capture_local_connector_identity,  # pyright: ignore[reportPrivateUsage]
)


class DurableFixtureRunner(CommandRunner):
    """Represent remote sidecars in a file shared across hard-exit processes."""

    def __init__(self, state_path: Path, *, crash_mode: str | None = None) -> None:
        self.state_path = state_path
        self.crash_mode = crash_mode

    def _load(self) -> dict[str, object]:
        try:
            loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        if not isinstance(loaded, dict):
            raise RuntimeError("fixture state is not an object")
        return cast(dict[str, object], loaded)

    def _save(self, state: dict[str, object]) -> None:
        temporary = self.state_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(state, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.state_path)

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds
        script = input_text or ""
        state = self._load()
        if "__CLIO_RECORD_SUBMISSION__" in script:
            session_id = _assignment(script, "session_id")
            submission_id = _assignment(script, "submission_id")
            scheduler_provider = _assignment(script, "scheduler_provider")
            submission_marker = _assignment(script, "submission_marker")
            output = '{"scheduler_job_id":"fixture-job","service_host":"compute-fixture"}\n'
            submission = {
                "schema_version": "clio-relay.gateway-submission-sidecar.v1",
                "present": True,
                "session_id": session_id,
                "submission_id": submission_id,
                "scheduler_provider": scheduler_provider,
                "submission_marker": submission_marker,
                "returncode": 0,
                "output": output,
            }
            if self.crash_mode == "scheduler_output":
                state["recoverable_submission"] = submission
                self._save(state)
                os._exit(84)
            state["submission"] = submission
            self._save(state)
            return subprocess.CompletedProcess(command, 0, output, "")
        if "__CLIO_READ_SUBMISSION__" in script:
            record = state.get("submission")
            if not isinstance(record, dict):
                recoverable = state.get("recoverable_submission")
                if isinstance(recoverable, dict):
                    record = cast(dict[str, object], recoverable)
                    state["submission"] = record
                    self._save(state)
                else:
                    record = {"present": False}
            return subprocess.CompletedProcess(command, 0, json.dumps(record) + "\n", "")
        if "remote-frpc.toml" in script and "nohup" in script:
            session_id = _assignment(script, "session_id")
            owner_token = _assignment(script, "owner_token")
            generation_id = _assignment(script, "connector_generation_id")
            session_dir_template = _assignment(script, "session_dir")
            if session_dir_template != (
                "$HOME/.local/share/clio-relay/service-sessions/$session_id"
            ):
                raise RuntimeError("fixture received an unexpected owned session directory")
            session_dir = session_dir_template.replace("$HOME", "/home/fixture").replace(
                "$session_id", session_id
            )
            config_path = _assignment(script, "config_file").replace("$session_dir", session_dir)
            log_path = _assignment(script, "log_file").replace("$session_dir", session_dir)
            connector: dict[str, object] = {
                "owner": "clio-relay",
                "session_id": session_id,
                "pid": 444,
                "process_group_id": 444,
                "connector_generation_id": generation_id,
                "owner_token": owner_token,
                "config_path": config_path,
                "log_path": log_path,
            }
            state["remote_connector"] = connector
            self._save(state)
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "remote_frpc_pid=444",
                        "remote_frpc_pgid=444",
                        f"connector_generation_id={generation_id}",
                        f"remote_frpc_config={config_path}",
                        f"remote_frpc_log={log_path}",
                    ]
                )
                + "\n",
                "",
            )
        if "__CLIO_DISCOVER_CONNECTOR__" in script:
            raw_connector = state.get("remote_connector")
            payload: dict[str, object] = (
                {
                    "present": True,
                    "ownership_verified": True,
                    "matching_pids": [444],
                    "connector": cast(dict[str, object], raw_connector),
                }
                if isinstance(raw_connector, dict)
                else {
                    "present": False,
                    "ownership_verified": True,
                    "matching_pids": [],
                }
            )
            return subprocess.CompletedProcess(command, 0, json.dumps(payload) + "\n", "")
        if "__CLIO_STOP_CONNECTOR__" in script:
            state.pop("remote_connector", None)
            self._save(state)
            stop_payload: dict[str, object] = {
                "pid": 444,
                "outcome": "stopped",
                "ownership_verified": True,
                "verified_after_operation": True,
                "residual": False,
                "remaining_pids": [],
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(stop_payload) + "\n", "")
        if "jarvis runtime status fixture-job" in script:
            return subprocess.CompletedProcess(
                command,
                0,
                '{"state":"allocated","service_host":"compute-fixture"}\n',
                "",
            )
        if "http.client.HTTPConnection" in script:
            return subprocess.CompletedProcess(command, 0, "service_health=ok\n", "")
        return subprocess.CompletedProcess(command, 1, "", f"unexpected fixture script: {script}")

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
        del command, input_bytes
        if env is None:
            raise RuntimeError("connector environment is required")
        owner_token = env["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"]
        generation_id = env["CLIO_RELAY_CONNECTOR_GENERATION_ID"]
        config_path = next(self.state_path.parent.glob("runtime-sessions/*/desktop-frpc.toml"))
        child_command = [
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            owner_token,
            generation_id,
            "frpc",
            "-c",
            str(config_path),
        ]
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        return subprocess.Popen(
            child_command,
            stdout=stdout_path.open("ab"),
            stderr=stderr_path.open("ab"),
            env=env,
            creationflags=creationflags,
            start_new_session=isolate_process_group and os.name != "nt",
        )

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> LocalConnectorIdentity:
        return _capture_local_connector_identity(
            pid=pid,
            owner_token=owner_token,
            expected_config=expected_config,
        )


class _CrashSupervisor(ServiceRuntimeSupervisor):
    mode: str

    def _ssh(self, script: str) -> str:
        output = super()._ssh(script)
        if self.mode == "scheduler" and "__CLIO_RECORD_SUBMISSION__" in script:
            os._exit(84)
        return output

    def _start_remote_connector(self, **kwargs: object) -> dict[str, object]:
        connector = super()._start_remote_connector(**kwargs)  # type: ignore[arg-type]
        if self.mode == "remote":
            os._exit(84)
        return connector

    def _start_local_visitor(self, **kwargs: object) -> dict[str, object]:
        connector = super()._start_local_visitor(**kwargs)  # type: ignore[arg-type]
        if self.mode == "local":
            os._exit(84)
        return connector


def definition() -> ClusterDefinition:
    """Return the configured target used by the crash fixture."""
    return ClusterDefinition(
        name="configured-target",
        ssh_host="fixture-login",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="relay.example.org",
            server_port=443,
        ),
    )


def runtime_spec() -> ServiceRuntimeSpec:
    """Return a generic external-runtime contract for ownership tests."""
    return ServiceRuntimeSpec(
        kind="generic-service",
        deployment_driver="jarvis",
        submit_command=["jarvis", "run", "/fixture/runtime.yaml"],
        status_command=["jarvis", "runtime", "status", "{scheduler_job_id}"],
        cancel_command=["jarvis", "runtime", "cancel", "{scheduler_job_id}"],
        service_port=18080,
        desktop_bind_port=28080,
        proxy_name="fixture-service",
    )


def _assignment(script: str, name: str) -> str:
    for line in script.splitlines():
        if line.startswith(f"{name}="):
            values = shlex.split(line.split("=", 1)[1])
            if len(values) == 1:
                return values[0]
    raise RuntimeError(f"fixture script has no {name} assignment")


def main() -> None:
    """Run a service start until the selected ownership hard-exit window."""
    root = Path(sys.argv[1])
    state_path = Path(sys.argv[2])
    mode = sys.argv[3]
    settings = RelaySettings(
        core_dir=root / "core",
        spool_dir=root / "spool",
        frpc_bin="fixture-frpc",
    )
    supervisor = _CrashSupervisor(
        settings=settings,
        queue=ClioCoreQueue(settings.core_dir),
        cluster="configured-target",
        definition=definition(),
        token="token",
        secret_key="secret",
        runner=DurableFixtureRunner(state_path, crash_mode=mode),
        sleep=lambda _seconds: None,
    )
    supervisor.mode = mode
    supervisor._wait_for_local_health = lambda *_args: None  # type: ignore[method-assign]
    supervisor.start(name=f"hard-crash-{mode}", spec=runtime_spec())
    raise AssertionError("gateway ownership crash injection did not terminate")


if __name__ == "__main__":
    main()
