"""Generic supervisor for scheduler-backed streaming service sessions."""

from __future__ import annotations

import base64
import json
import os
import shlex
import signal
import subprocess
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import httpx

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import GatewaySession, GatewaySessionState, ServiceRuntimeSpec, utc_now
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
)
from clio_relay.remote_cli import remote_env


class CommandRunner(Protocol):
    """Protocol for local command execution used by the supervisor."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a command and return the completed process."""
        ...

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
    ) -> subprocess.Popen[bytes]:
        """Start a long-running local process."""
        ...


class SubprocessCommandRunner:
    """Command runner backed by subprocess."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a local subprocess with text output."""
        input_bytes = input_text.encode("utf-8") if input_text is not None else None
        result = subprocess.run(
            list(command),
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return subprocess.CompletedProcess(
            args=result.args,
            returncode=result.returncode,
            stdout=result.stdout.decode("utf-8", errors="replace"),
            stderr=result.stderr.decode("utf-8", errors="replace"),
        )

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
    ) -> subprocess.Popen[bytes]:
        """Start a local subprocess with owned log files."""
        stdout_handle = stdout_path.open("ab")
        stderr_handle = stderr_path.open("ab")
        try:
            return subprocess.Popen(
                list(command),
                stdout=stdout_handle,
                stderr=stderr_handle,
                close_fds=os.name != "nt",
            )
        finally:
            stdout_handle.close()
            stderr_handle.close()


@dataclass(frozen=True)
class ServiceRuntimeStartResult:
    """Result of a started service runtime session."""

    session: GatewaySession
    connect_url: str
    health_url: str
    stream_url: str | None
    compatibility_urls: dict[str, str]
    events_url: str | None


@dataclass(frozen=True)
class ServiceRuntimeStopResult:
    """Result of stopping owned runtime connector processes."""

    session: GatewaySession
    stopped_local_pid: int | None
    stopped_remote_pid: int | None
    canceled_scheduler_job: str | None


class ServiceRuntimeSupervisor:
    """Start, bind, probe, and tear down scheduler-backed remote service sessions."""

    def __init__(
        self,
        *,
        settings: RelaySettings,
        queue: ClioCoreQueue,
        cluster: str,
        definition: ClusterDefinition,
        token: str,
        secret_key: str,
        runner: CommandRunner | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.cluster = cluster
        self.definition = definition
        self.token = token
        self.secret_key = secret_key
        self.runner = runner or SubprocessCommandRunner()
        self.sleep = sleep

    def start(
        self,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
    ) -> ServiceRuntimeStartResult:
        """Start a scheduler-backed remote service and bind it to a desktop port."""
        self.queue.initialize()
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster=self.cluster,
                name=name,
                state=GatewaySessionState.CREATED,
                scheduler=spec.scheduler,
                requested_resources={"service_port": spec.service_port},
                gateway={
                    "runtime_spec": spec.model_dump(mode="json"),
                    "transport": {"mode": spec.transport_mode},
                },
                metadata={"runtime_kind": spec.kind},
            )
        )
        try:
            session = self._update(
                session,
                state=GatewaySessionState.SUBMITTED,
                metadata={"submitted_at": utc_now().isoformat()},
            )
            submit_output = self._ssh(_submit_script(spec.submit_command))
            submission = _parse_runtime_submission(submit_output)
            scheduler_job_id = submission.scheduler_job_id
            session = self._update(
                session,
                scheduler_job_id=scheduler_job_id,
                queue_state="submitted",
                gateway={
                    **session.gateway,
                    "submit_output": submit_output.strip(),
                },
            )
            node = self._wait_for_allocation_and_health(
                session,
                spec,
                scheduler_job_id,
                initial_service_host=submission.service_host,
            )
            session = self.queue.get_gateway_session(session.session_id)
            proxy_name = spec.proxy_name or f"{session.session_id}-service"
            remote_connector = self._start_remote_connector(
                session=session,
                spec=spec,
                node=node,
                proxy_name=proxy_name,
            )
            local_connector = self._start_local_visitor(
                session=session,
                spec=spec,
                proxy_name=proxy_name,
            )
            connect_url = spec.connect_url_template.format(
                bind_addr=spec.desktop_bind_addr,
                bind_port=spec.desktop_bind_port,
                session_id=session.session_id,
            )
            health_url = (
                f"http://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{spec.health_path}"
            )
            self._wait_for_local_health(
                health_url,
                spec.readiness_timeout_seconds,
                spec.poll_seconds,
            )
            events_url = (
                f"http://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{spec.event_stream_path}"
                if spec.event_stream_path is not None
                else None
            )
            stream_url = (
                f"http://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{spec.stream_path}"
                if spec.stream_path is not None
                else None
            )
            compatibility_urls = {
                name: f"http://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{path}"
                for name, path in spec.compatibility_paths.items()
            }
            session = self._update(
                session,
                state=GatewaySessionState.READY,
                queue_state="running",
                node=node,
                gateway={
                    **session.gateway,
                    "connect_url": connect_url,
                    "health_url": health_url,
                    "stream_url": stream_url,
                    "compatibility_urls": compatibility_urls,
                    "events_url": events_url,
                    "service": {
                        "host": node,
                        "port": spec.service_port,
                        "health_path": spec.health_path,
                        "stream_mode": spec.stream_mode,
                        "stream_path": spec.stream_path,
                        "compatibility_paths": spec.compatibility_paths,
                        "state_path": spec.state_path,
                        "event_stream_path": spec.event_stream_path,
                        "deployment_driver": spec.deployment_driver,
                    },
                    "transport": {
                        "mode": spec.transport_mode,
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                        "remote_target": f"{node}:{spec.service_port}",
                        "desktop_bind": f"{spec.desktop_bind_addr}:{spec.desktop_bind_port}",
                    },
                },
                metadata={"ready_at": utc_now().isoformat()},
            )
            return ServiceRuntimeStartResult(
                session=session,
                connect_url=connect_url,
                health_url=health_url,
                stream_url=stream_url,
                compatibility_urls=compatibility_urls,
                events_url=events_url,
            )
        except Exception as exc:
            self._update(
                session,
                state=GatewaySessionState.FAILED,
                metadata={
                    "failed_at": utc_now().isoformat(),
                    "last_error": str(exc),
                },
            )
            raise

    def stop(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
    ) -> ServiceRuntimeStopResult:
        """Stop owned relay connector processes and optionally cancel the scheduler job."""
        session = self.queue.get_gateway_session(session_id)
        transport = _object(session.gateway.get("transport", {}))
        desktop_connector = _object(transport.get("desktop_connector", {}))
        remote_connector = _object(transport.get("remote_connector", {}))
        stopped_local_pid = _terminate_local_pid(
            _optional_int(desktop_connector.get("pid")),
            expected_config=_optional_str(desktop_connector.get("config_path")),
        )
        stopped_remote_pid = None
        remote_pid = _optional_int(remote_connector.get("pid"))
        if remote_pid is not None:
            self._ssh(_remote_stop_script(session_id=session.session_id, pid=remote_pid))
            stopped_remote_pid = remote_pid
        canceled_scheduler_job = None
        if cancel_scheduler_job and session.scheduler_job_id is not None:
            spec = ServiceRuntimeSpec.model_validate(session.gateway["runtime_spec"])
            if spec.cancel_command is None:
                raise ConfigurationError(
                    "cancel_scheduler_job requires ServiceRuntimeSpec.cancel_command"
                )
            self._ssh(_template_command_script(spec.cancel_command, session.scheduler_job_id))
            canceled_scheduler_job = session.scheduler_job_id
        updated = self.queue.update_gateway_session(
            session_id,
            state=GatewaySessionState.CLOSED,
            metadata={
                "closed_at": utc_now().isoformat(),
                "cancel_scheduler_job": cancel_scheduler_job,
            },
            gateway={
                **session.gateway,
                "teardown": {
                    "stopped_local_pid": stopped_local_pid,
                    "stopped_remote_pid": stopped_remote_pid,
                    "canceled_scheduler_job": canceled_scheduler_job,
                },
            },
        )
        return ServiceRuntimeStopResult(
            session=updated,
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
        )

    def _wait_for_allocation_and_health(
        self,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
        initial_service_host: str | None = None,
    ) -> str:
        deadline = time.time() + spec.readiness_timeout_seconds
        last_status = ""
        current_session = session
        while time.time() < deadline:
            if initial_service_host is not None:
                scheduler_state = "allocated"
                node = initial_service_host
                reason = None
                runtime_events: list[dict[str, object]] | None = None
                status_text = json.dumps(
                    {
                        "scheduler_job_id": scheduler_job_id,
                        "service_host": initial_service_host,
                    },
                    sort_keys=True,
                )
            else:
                if spec.status_command is None:
                    raise ConfigurationError(
                        "service host was not reported by submission output; "
                        "ServiceRuntimeSpec.status_command is required"
                    )
                status_text = self._ssh(
                    _template_command_script(spec.status_command, scheduler_job_id)
                )
                status = _parse_runtime_status(status_text)
                scheduler_state = status.state or "unknown"
                node = status.service_host
                reason = status.reason
                runtime_events = status.events
            last_status = status_text.strip()
            state = (
                GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING
            )
            current_session = self._update(
                current_session,
                state=state,
                queue_state=scheduler_state.lower() if scheduler_state else None,
                node=node,
                gateway={
                    **current_session.gateway,
                    "scheduler_status": {
                        "raw": last_status,
                        "state": scheduler_state,
                        "reason": reason,
                    },
                    "runtime_events": runtime_events or [],
                },
            )
            if node is not None:
                health = self._ssh(
                    _remote_http_probe_script(node, spec.service_port, spec.health_path)
                )
                if "service_health=ok" in health:
                    return node
            self.sleep(spec.poll_seconds)
        raise RelayError(
            f"service did not become healthy before timeout; job={scheduler_job_id} "
            f"last_status={last_status!r}"
        )

    def _start_remote_connector(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        node: str,
        proxy_name: str,
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _require_server_addr(transport.server_addr, self.cluster)
        config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=self.token,
                transport_protocol=FrpTransportProtocol(transport.protocol),
                proxy_name=proxy_name,
                proxy_type=_frp_proxy_type(spec.transport_mode),
                local_ip=node,
                local_port=spec.service_port,
                secret_key=self.secret_key,
            )
        )
        output = self._ssh(
            _remote_frpc_start_script(
                definition=self.definition,
                session_id=session.session_id,
                config_text=config,
            )
        )
        metadata = _key_value_output(output)
        pid = int(metadata["remote_frpc_pid"])
        return {
            "pid": pid,
            "config_path": metadata["remote_frpc_config"],
            "log_path": metadata["remote_frpc_log"],
        }

    def _start_local_visitor(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _require_server_addr(transport.server_addr, self.cluster)
        runtime_dir = self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = (runtime_dir / "desktop-frpc.toml").resolve()
        stdout_path = (runtime_dir / "desktop-frpc.out").resolve()
        stderr_path = (runtime_dir / "desktop-frpc.err").resolve()
        config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=self.token,
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    visitor_name=f"{proxy_name}-visitor",
                    visitor_type=_frp_proxy_type(spec.transport_mode),
                    server_name=proxy_name,
                    bind_addr=spec.desktop_bind_addr,
                    bind_port=spec.desktop_bind_port,
                    secret_key=self.secret_key,
                    keep_tunnel_open=_frp_proxy_type(spec.transport_mode) == "xtcp",
                )
            ),
            encoding="utf-8",
        )
        process = self.runner.popen(
            [self.settings.frpc_bin, "-c", str(config_path)],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        return {
            "pid": process.pid,
            "config_path": str(config_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }

    def _wait_for_local_health(
        self,
        health_url: str,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> None:
        deadline = time.time() + timeout_seconds
        last_error: str | None = None
        while time.time() < deadline:
            try:
                response = httpx.get(health_url, timeout=5.0)
                if response.status_code < 500:
                    return
                last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            self.sleep(poll_seconds)
        raise RelayError(f"local service health probe failed: {health_url}: {last_error}")

    def _update(
        self,
        session: GatewaySession,
        *,
        state: GatewaySessionState | None = None,
        metadata: dict[str, object] | None = None,
        **updates: object,
    ) -> GatewaySession:
        return self.queue.update_gateway_session(
            session.session_id,
            state=state,
            metadata=metadata,
            **updates,
        )

    def _ssh(self, script: str) -> str:
        result = self.runner.run(
            ["ssh", self.definition.ssh_host, "bash", "-s"],
            input_text=script,
            timeout_seconds=None,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RelayError(f"remote service runtime command failed: {detail}")
        return result.stdout


def _submit_script(command: Sequence[str]) -> str:
    return "set -euo pipefail\n" + shlex.join(list(command)) + "\n"


def _template_command_script(command: Sequence[str], scheduler_job_id: str) -> str:
    templated = [part.format(scheduler_job_id=scheduler_job_id) for part in command]
    return "set -euo pipefail\n" + shlex.join(templated) + "\n"


def _remote_http_probe_script(host: str, port: int, path: str) -> str:
    return f"""set -euo pipefail
python3 - {shlex.quote(host)} {port} {shlex.quote(path)} <<'__CLIO_SERVICE_HEALTH__'
import http.client
import sys
host, port, path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
try:
    conn = http.client.HTTPConnection(host, port, timeout=5)
    conn.request("GET", path)
    response = conn.getresponse()
    print(f"service_health={{'ok' if response.status < 500 else 'bad'}}")
    print(f"service_status={{response.status}}")
except OSError as exc:
    print(f"service_health=unreachable")
    print(f"service_error={{exc}}")
__CLIO_SERVICE_HEALTH__
"""


def _remote_frpc_start_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    config_text: str,
) -> str:
    encoded = base64.b64encode(config_text.encode("utf-8")).decode("ascii")
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    return f"""set -euo pipefail
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir"
config_file="$session_dir/remote-frpc.toml"
log_file="$session_dir/remote-frpc.log"
pid_file="$session_dir/remote-frpc.pid"
metadata_file="$session_dir/metadata.json"
python3 - "$config_file" <<'__CLIO_WRITE_FRPC__'
import base64
import sys
path = sys.argv[1]
data = base64.b64decode({encoded!r}).decode("utf-8")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(data)
__CLIO_WRITE_FRPC__
frpc_bin={_shell_double_quote(frpc_bin)}
nohup "$frpc_bin" -c "$config_file" >"$log_file" 2>&1 &
pid="$!"
echo "$pid" > "$pid_file"
python3 - "$metadata_file" "$pid" "$config_file" "$log_file" <<'__CLIO_METADATA__'
import json
import sys
from datetime import datetime, timezone
metadata_file, pid, config_file, log_file = sys.argv[1:]
with open(metadata_file, "w", encoding="utf-8") as handle:
    json.dump({{
        "owner": "clio-relay",
        "session_id": {session_id!r},
        "remote_frpc_pid": int(pid),
        "remote_frpc_config": config_file,
        "remote_frpc_log": log_file,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }}, handle, indent=2)
__CLIO_METADATA__
sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
  cat "$log_file" >&2
  exit 1
fi
echo "remote_frpc_pid=$pid"
echo "remote_frpc_config=$config_file"
echo "remote_frpc_log=$log_file"
"""


def _remote_stop_script(*, session_id: str, pid: int) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
pid={pid}
metadata_file="$HOME/.local/share/clio-relay/service-sessions/$session_id/metadata.json"
python3 - "$metadata_file" "$pid" "$session_id" <<'__CLIO_VALIDATE_OWNER__'
import json
import sys
metadata_file, pid, session_id = sys.argv[1:]
with open(metadata_file, encoding="utf-8") as handle:
    metadata = json.load(handle)
if metadata.get("owner") != "clio-relay" or metadata.get("session_id") != session_id:
    raise SystemExit(1)
if str(metadata.get("remote_frpc_pid")) != pid:
    raise SystemExit(1)
__CLIO_VALIDATE_OWNER__
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid" 2>/dev/null || true; fi
fi
rm -f "$HOME/.local/share/clio-relay/service-sessions/$session_id/remote-frpc.pid"
echo "remote_frpc_stopped=$pid"
"""


@dataclass(frozen=True)
class RuntimeSubmission:
    """Structured submission result emitted by a deployment driver."""

    scheduler_job_id: str
    service_host: str | None = None


@dataclass(frozen=True)
class RuntimeStatus:
    """Structured status emitted by a deployment driver."""

    state: str | None = None
    service_host: str | None = None
    reason: str | None = None
    events: list[dict[str, object]] | None = None


def _parse_runtime_submission(output: str) -> RuntimeSubmission:
    """Parse structured JSON submission output from a deployment driver."""
    record = _last_json_object(output)
    scheduler_job_id = record.get("scheduler_job_id")
    if not isinstance(scheduler_job_id, str) or scheduler_job_id == "":
        raise RelayError(
            f"deployment output must include JSON field scheduler_job_id; received: {output!r}"
        )
    service_host = record.get("service_host")
    if service_host is not None and not isinstance(service_host, str):
        raise RelayError("deployment output JSON field service_host must be a string")
    return RuntimeSubmission(scheduler_job_id=scheduler_job_id, service_host=service_host)


def _parse_runtime_status(output: str) -> RuntimeStatus:
    """Parse structured JSON status output from a deployment driver."""
    record = _last_json_object(output)
    state = record.get("state")
    service_host = record.get("service_host")
    reason = record.get("reason")
    events = _runtime_events(record.get("events"))
    return RuntimeStatus(
        state=state if isinstance(state, str) else None,
        service_host=service_host if isinstance(service_host, str) else None,
        reason=reason if isinstance(reason, str) else None,
        events=events,
    )


def _last_json_object(output: str) -> dict[str, object]:
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return cast(dict[str, object], loaded)
    raise RelayError(f"deployment output must include a JSON object: {output!r}")


def _runtime_events(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list):
        return None
    raw_items = cast(list[object], value)
    events: list[dict[str, object]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            return None
        events.append(cast(dict[str, object], item))
    return events


def _key_value_output(output: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    return values


def _terminate_local_pid(pid: int | None, *, expected_config: str | None) -> int | None:
    if pid is None:
        return None
    if not _local_pid_matches(pid, expected_config=expected_config):
        return None
    if os.name == "nt":
        return _terminate_windows_pid(pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return None
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _pid_exists(pid):
            return pid
        time.sleep(0.2)
    with suppress(OSError):
        os.kill(pid, signal.SIGTERM)
    return pid


def _local_pid_matches(pid: int, *, expected_config: str | None) -> bool:
    if not _pid_exists(pid):
        return False
    if os.name == "nt":
        return _windows_pid_matches(pid, expected_config=expected_config)
    if expected_config is None:
        return True
    cmdline = Path(f"/proc/{pid}/cmdline")
    try:
        text = cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", errors="replace")
    except OSError:
        return False
    return "frpc" in text and expected_config in text


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_exists(pid)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _windows_pid_exists(pid: int) -> bool:
    result = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and str(pid) in result.stdout


def _windows_pid_matches(pid: int, *, expected_config: str | None) -> bool:
    if expected_config is None:
        return True
    expected_path = Path(expected_config)
    normalized_expected = str(
        expected_path.resolve() if not expected_path.is_absolute() else expected_path
    )
    escaped = expected_config.replace("\\", "\\\\")
    escaped_resolved = normalized_expected.replace("\\", "\\\\")
    command = (
        "Get-CimInstance Win32_Process "
        f'-Filter "ProcessId = {pid}" | '
        "Select-Object -ExpandProperty CommandLine"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return False
    command_line = result.stdout.replace("\\", "\\\\")
    return "frpc" in command_line.lower() and (
        escaped in command_line or escaped_resolved in command_line
    )


def _terminate_windows_pid(pid: int) -> int | None:
    result = subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in {0, 128}:
        return None
    return pid


def _object(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _require_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(f"frp server address is not configured for cluster {cluster}")


def _frp_proxy_type(transport_mode: str) -> str:
    normalized = transport_mode.strip().lower().replace("_", "-")
    if normalized in {"frp-stcp", "frp-stcp-wss", "stcp", "relay"}:
        return "stcp"
    if normalized in {"frp-xtcp", "frp-xtcp-wss", "xtcp", "direct", "nat-bypass"}:
        return "xtcp"
    raise ConfigurationError(f"unsupported service runtime transport mode: {transport_mode}")


def _shell_double_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
