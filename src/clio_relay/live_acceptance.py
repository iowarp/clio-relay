"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
import json
import posixpath
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError, RelayError


class CommandRunner(Protocol):
    """Protocol for command execution used by the live acceptance runner."""

    def __call__(
        self,
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run a command and return the completed process."""
        ...


@dataclass(frozen=True)
class LiveAcceptanceOptions:
    """Inputs for a full live acceptance run."""

    cluster: str
    definition: ClusterDefinition
    jarvis_yaml: Path | None = None
    monitor_pattern: str | None = None
    agent_prompt: str | None = None
    agent_mcp_config: str | None = None
    timeout_seconds: float = 600
    poll_seconds: float = 2


def run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner | None = None,
) -> list[str]:
    """Run configured live acceptance checks against a cluster deployment."""
    command_runner = runner or _run_command
    jarvis_yaml = options.jarvis_yaml or _configured_path(options.definition.live_test.jarvis_yaml)
    monitor_pattern = options.monitor_pattern or options.definition.live_test.monitor_pattern
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    if jarvis_yaml is None:
        raise ConfigurationError(
            "live-test requires --jarvis-yaml or cluster live_test.jarvis_yaml"
        )
    if not jarvis_yaml.exists():
        raise ConfigurationError(f"live-test JARVIS YAML does not exist: {jarvis_yaml}")

    lines: list[str] = []
    lines.extend(run_cluster_doctor(options.definition))
    run_id = _acceptance_run_id(jarvis_yaml)
    remote_yaml = f".local/share/clio-relay/live-tests/{run_id}/pipeline.yaml"
    _remote_write_file(
        options.definition.ssh_host,
        remote_yaml,
        jarvis_yaml.read_bytes(),
        runner=command_runner,
    )
    lines.append(f"acceptance.pipeline={remote_yaml}")

    submit = _remote_clio_json(
        options.definition,
        [
            "job",
            "submit",
            "--cluster",
            options.cluster,
            "--jarvis-yaml",
            remote_yaml,
            "--idempotency-key",
            f"live-test:{options.cluster}:{run_id}:jarvis",
        ],
        runner=command_runner,
        raw_text=True,
    )
    job_id = submit.strip().splitlines()[-1]
    if not job_id.startswith("job_"):
        raise RelayError(f"live-test submit did not return a job id: {submit}")
    lines.append(f"acceptance.job_id={job_id}")

    job = _remote_clio_json(
        options.definition,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(options.timeout_seconds),
            "--poll-seconds",
            str(options.poll_seconds),
        ],
        runner=command_runner,
    )
    if job["state"] != "succeeded":
        raise RelayError(f"acceptance job did not succeed: {job['state']}")
    lines.append("acceptance.job_state=succeeded")

    monitor = _remote_clio_json(
        options.definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "250"],
        runner=command_runner,
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"acceptance job missing events: {sorted(missing_events)}")
    lines.append("acceptance.events=ok")

    tasks = _remote_clio_json(
        options.definition,
        ["job", "tasks", job_id],
        runner=command_runner,
    )
    task_items = cast(list[dict[str, Any]], tasks)
    if not task_items or not any(task["state"] == "succeeded" for task in task_items):
        raise RelayError("acceptance job missing succeeded task record")
    lines.append(f"acceptance.tasks={len(task_items)}")

    stdout = _remote_clio_json(
        options.definition,
        ["job", "read-log", job_id, "--stream", "stdout", "--offset", "0", "--limit", "200000"],
        runner=command_runner,
    )
    stderr = _remote_clio_json(
        options.definition,
        ["job", "read-log", job_id, "--stream", "stderr", "--offset", "0", "--limit", "200000"],
        runner=command_runner,
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("acceptance stdout log is empty")
    lines.append(f"acceptance.stdout_bytes={stdout['next_offset']}")
    lines.append(f"acceptance.stderr_bytes={stderr['next_offset']}")

    artifacts = _remote_clio_json(
        options.definition,
        ["job", "list-artifacts", job_id],
        runner=command_runner,
    )
    artifact_items = cast(list[dict[str, Any]], artifacts)
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(f"acceptance artifacts incomplete: {sorted(artifact_kinds)}")
    lines.append(f"acceptance.artifacts={','.join(sorted(artifact_kinds))}")

    stdout_artifact = next(artifact for artifact in artifact_items if artifact["kind"] == "stdout")
    artifact_payload = _remote_clio_json(
        options.definition,
        ["job", "read-artifact", str(stdout_artifact["artifact_id"])],
        runner=command_runner,
    )
    if artifact_payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    lines.append("acceptance.artifact_read=ok")

    provenance_artifact = next(
        artifact for artifact in artifact_items if artifact["kind"] == "provenance"
    )
    provenance_payload = _remote_clio_json(
        options.definition,
        ["job", "read-artifact", str(provenance_artifact["artifact_id"])],
        runner=command_runner,
    )
    if provenance_payload.get("encoding") != "base64":
        raise RelayError("acceptance provenance payload was not base64 encoded")
    lines.append("acceptance.provenance=ok")

    if monitor_pattern is not None:
        _remote_clio_json(
            options.definition,
            [
                "monitor",
                "add-regex",
                job_id,
                "--pattern",
                monitor_pattern,
                "--event-type",
                "stdout.delta",
            ],
            runner=command_runner,
        )
        actions = _remote_clio_json(
            options.definition,
            ["monitor", "run-once", "--limit", "250"],
            runner=command_runner,
        )
        if not actions:
            raise RelayError(f"acceptance monitor pattern did not match: {monitor_pattern}")
        lines.append("acceptance.monitor=ok")

    if agent_prompt is not None:
        agent_args = [
            "agent",
            "run",
            "--cluster",
            options.cluster,
            "--prompt",
            agent_prompt,
            "--idempotency-key",
            f"live-test:{options.cluster}:{run_id}:agent",
        ]
        if agent_mcp_config is not None:
            agent_args.extend(["--mcp-config", agent_mcp_config])
        agent_submit = _remote_clio_json(
            options.definition,
            agent_args,
            runner=command_runner,
            raw_text=True,
        )
        agent_job_id = agent_submit.strip().splitlines()[-1]
        agent_job = _remote_clio_json(
            options.definition,
            [
                "job",
                "wait",
                agent_job_id,
                "--timeout-seconds",
                str(options.timeout_seconds),
                "--poll-seconds",
                str(options.poll_seconds),
            ],
            runner=command_runner,
        )
        if agent_job["state"] != "succeeded":
            raise RelayError(f"acceptance agent job did not succeed: {agent_job['state']}")
        lines.append(f"acceptance.agent_job_id={agent_job_id}")
        lines.append("acceptance.agent_state=succeeded")

    lines.append("live acceptance passed")
    return lines


def _configured_path(value: str | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser()


def _acceptance_run_id(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return f"{path.stem}-{digest}-{uuid4().hex[:8]}"


def _remote_write_file(
    ssh_host: str,
    remote_path: str,
    data: bytes,
    *,
    runner: CommandRunner,
) -> None:
    mkdir_command = f"mkdir -p {shlex.quote(posixpath.dirname(remote_path))}"
    _remote_shell(ssh_host, mkdir_command, runner=runner)
    result = runner(["ssh", ssh_host, f"cat > {shlex.quote(remote_path)}"], input=data)
    if result.returncode != 0:
        raise RelayError(_command_error("remote file write failed", result))


def _remote_clio_json(
    definition: ClusterDefinition,
    args: list[str],
    *,
    runner: CommandRunner,
    raw_text: bool = False,
) -> Any:
    rendered_args = " ".join(shlex.quote(arg) for arg in args)
    output = _remote_shell(
        definition.ssh_host,
        f"{_remote_env(definition)} clio-relay {rendered_args}",
        runner=runner,
    )
    if raw_text:
        return output
    return json.loads(output)


def _remote_shell(ssh_host: str, script: str, *, runner: CommandRunner) -> str:
    result = runner(["ssh", ssh_host, f"bash -lc {shlex.quote(script)}"])
    if result.returncode != 0:
        raise RelayError(_command_error("remote command failed", result))
    return result.stdout.decode("utf-8", errors="replace")


def _remote_env(definition: ClusterDefinition) -> str:
    return " ".join(
        [
            'export PATH="$HOME/.local/bin:$PATH";',
            f"export CLIO_RELAY_CORE_DIR={_shell_double_quoted(definition.core_dir)};",
            f"export CLIO_RELAY_SPOOL_DIR={_shell_double_quoted(definition.spool_dir)};",
            'export CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis";',
            'export CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc";',
            f'export CLIO_RELAY_AGENT_BIN="$HOME/.local/bin/{definition.agent_npm_bin}";',
            f"export CLIO_RELAY_AGENT_ADAPTER={shlex.quote(definition.agent_adapter)};",
        ]
    )


def _shell_double_quoted(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _run_command(
    command: list[str],
    *,
    input: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, input=input, capture_output=True, check=False)


def _command_error(prefix: str, result: subprocess.CompletedProcess[bytes]) -> str:
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    detail = stderr or stdout
    return f"{prefix}: {detail}"
