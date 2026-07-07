"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import shlex
import subprocess
from base64 import b64decode
from dataclasses import dataclass, field
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


def _empty_progress_payload() -> dict[str, object]:
    return {}


@dataclass(frozen=True)
class LiveAcceptanceOptions:
    """Inputs for a full live acceptance run."""

    cluster: str
    definition: ClusterDefinition
    jarvis_yaml: Path | None = None
    monitor_pattern: str | None = None
    progress_pattern: str | None = None
    progress_action_payload: dict[str, object] = field(default_factory=_empty_progress_payload)
    agent_prompt: str | None = None
    agent_mcp_config: str | None = None
    require_agent_child_job: bool | None = None
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
    progress_pattern = options.progress_pattern or options.definition.live_test.progress_pattern
    progress_action_payload = (
        options.progress_action_payload
        if options.progress_action_payload
        else options.definition.live_test.progress_action_payload
    )
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
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

    _wait_for_success(
        options.definition,
        job_id,
        timeout_seconds=options.timeout_seconds,
        poll_seconds=options.poll_seconds,
        runner=command_runner,
    )
    lines.append("acceptance.job_state=succeeded")

    _verify_completed_job(
        options.definition,
        job_id,
        line_prefix="acceptance",
        lines=lines,
        runner=command_runner,
    )

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

    if progress_pattern is not None:
        _verify_progress_monitor(
            options.definition,
            job_id,
            pattern=progress_pattern,
            action_payload=progress_action_payload,
            lines=lines,
            runner=command_runner,
        )

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
        _wait_for_success(
            options.definition,
            agent_job_id,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append(f"acceptance.agent_job_id={agent_job_id}")
        lines.append("acceptance.agent_state=succeeded")
        if require_agent_child_job:
            child_job_id = _find_agent_child_job(
                options.definition,
                agent_job_id,
                runner=command_runner,
            )
            _wait_for_success(
                options.definition,
                child_job_id,
                timeout_seconds=options.timeout_seconds,
                poll_seconds=options.poll_seconds,
                runner=command_runner,
            )
            lines.append(f"acceptance.agent_child_job_id={child_job_id}")
            _verify_completed_job(
                options.definition,
                child_job_id,
                line_prefix="acceptance.agent_child",
                lines=lines,
                runner=command_runner,
            )

    lines.append("live acceptance passed")
    return lines


def _wait_for_success(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> dict[str, Any]:
    job = _remote_clio_json(
        definition,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ],
        runner=runner,
    )
    typed = cast(dict[str, Any], job)
    if typed["state"] != "succeeded":
        raise RelayError(f"acceptance job did not succeed: {typed['state']}")
    return typed


def _verify_completed_job(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    runner: CommandRunner,
) -> None:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "250"],
        runner=runner,
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"acceptance job missing events: {sorted(missing_events)}")
    lines.append(f"{line_prefix}.events=ok")

    tasks = _remote_clio_json(
        definition,
        ["job", "tasks", job_id],
        runner=runner,
    )
    task_items = cast(list[dict[str, Any]], tasks)
    if not task_items or not any(task["state"] == "succeeded" for task in task_items):
        raise RelayError("acceptance job missing succeeded task record")
    lines.append(f"{line_prefix}.tasks={len(task_items)}")

    stdout = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stdout", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    stderr = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stderr", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("acceptance stdout log is empty")
    lines.append(f"{line_prefix}.stdout_bytes={stdout['next_offset']}")
    lines.append(f"{line_prefix}.stderr_bytes={stderr['next_offset']}")

    artifacts = _remote_clio_json(
        definition,
        ["job", "list-artifacts", job_id],
        runner=runner,
    )
    artifact_items = cast(list[dict[str, Any]], artifacts)
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(f"acceptance artifacts incomplete: {sorted(artifact_kinds)}")
    lines.append(f"{line_prefix}.artifacts={','.join(sorted(artifact_kinds))}")

    stdout_artifact = next(artifact for artifact in artifact_items if artifact["kind"] == "stdout")
    artifact_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(stdout_artifact["artifact_id"])],
        runner=runner,
    )
    if artifact_payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    lines.append(f"{line_prefix}.artifact_read=ok")

    provenance_artifact = next(
        artifact for artifact in artifact_items if artifact["kind"] == "provenance"
    )
    provenance_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(provenance_artifact["artifact_id"])],
        runner=runner,
    )
    if provenance_payload.get("encoding") != "base64":
        raise RelayError("acceptance provenance payload was not base64 encoded")
    lines.append(f"{line_prefix}.provenance=ok")


def _verify_progress_monitor(
    definition: ClusterDefinition,
    job_id: str,
    *,
    pattern: str,
    action_payload: dict[str, object],
    lines: list[str],
    runner: CommandRunner,
) -> None:
    _remote_clio_json(
        definition,
        [
            "monitor",
            "add-regex",
            job_id,
            "--pattern",
            pattern,
            "--action",
            "record_progress",
            "--event-type",
            "stdout.delta",
            "--action-payload-json",
            json.dumps(action_payload, sort_keys=True, separators=(",", ":")),
        ],
        runner=runner,
    )
    actions = _remote_clio_json(
        definition,
        ["monitor", "run-once", "--limit", "250"],
        runner=runner,
    )
    action_items = cast(list[dict[str, Any]], actions)
    progress_actions = [
        action for action in action_items if action.get("action") == "record_progress"
    ]
    if not progress_actions:
        raise RelayError(f"acceptance progress pattern did not record progress: {pattern}")
    progress = _remote_clio_json(
        definition,
        ["job", "progress", job_id],
        runner=runner,
    )
    progress_items = cast(list[dict[str, Any]], progress)
    if not progress_items:
        raise RelayError("acceptance progress records missing after monitor evaluation")
    lines.append(f"acceptance.progress={len(progress_items)}")


def _find_agent_child_job(
    definition: ClusterDefinition,
    agent_job_id: str,
    *,
    runner: CommandRunner,
) -> str:
    artifacts = _remote_clio_json(
        definition,
        ["job", "list-artifacts", agent_job_id],
        runner=runner,
    )
    artifact_items = cast(list[dict[str, Any]], artifacts)
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if "agent_result" not in artifact_kinds:
        raise RelayError("acceptance agent job missing agent_result artifact")
    candidate_texts: list[str] = []
    for artifact in artifact_items:
        if str(artifact["kind"]) not in {"agent_last_message", "stdout", "agent_result"}:
            continue
        payload = _remote_clio_json(
            definition,
            ["job", "read-artifact", str(artifact["artifact_id"])],
            runner=runner,
        )
        candidate_texts.append(_decode_artifact_text(payload))
    stdout = _remote_clio_json(
        definition,
        [
            "job",
            "read-log",
            agent_job_id,
            "--stream",
            "stdout",
            "--offset",
            "0",
            "--limit",
            "200000",
        ],
        runner=runner,
    )
    candidate_texts.append(str(stdout.get("text", "")))
    child_job_ids = sorted(
        {
            match
            for text in candidate_texts
            for match in re.findall(r"\bjob_[0-9a-f]{32}\b", text)
            if match != agent_job_id
        }
    )
    if not child_job_ids:
        raise RelayError("acceptance agent did not report a child relay job id")
    return child_job_ids[-1]


def _decode_artifact_text(payload: dict[str, Any]) -> str:
    if payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    data = payload.get("data")
    if not isinstance(data, str):
        raise RelayError("acceptance artifact payload missing base64 data")
    return b64decode(data.encode("ascii")).decode("utf-8", errors="replace")


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
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = definition.agent_bin or f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return " ".join(
        [
            'export PATH="$HOME/.local/bin:$PATH";',
            f"export CLIO_RELAY_CORE_DIR={_shell_double_quoted(definition.core_dir)};",
            f"export CLIO_RELAY_SPOOL_DIR={_shell_double_quoted(definition.spool_dir)};",
            f"export CLIO_RELAY_JARVIS_BIN={_shell_double_quoted(jarvis_bin)};",
            f"export CLIO_RELAY_FRPC_BIN={_shell_double_quoted(frpc_bin)};",
            f"export CLIO_RELAY_AGENT_BIN={_shell_double_quoted(agent_bin)};",
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
