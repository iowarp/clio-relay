"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64decode
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

import yaml

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.transport_probe import run_frp_http_probe


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
    verify_transport: bool | None = None
    transport_token: str | None = None
    transport_secret_key: str | None = None
    transport_frpc_bin: str = "frpc"
    transport_local_bind_port: int | None = None
    transport_remote_api_port: int | None = None
    transport_proxy_name: str | None = None
    api_token: str | None = None
    agent_child_jarvis_yaml: Path | None = None
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
    agent_child_jarvis_yaml = options.agent_child_jarvis_yaml or _configured_path(
        options.definition.live_test.agent_child_jarvis_yaml
    )
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
    verify_transport = (
        options.definition.live_test.verify_transport
        if options.verify_transport is None
        else options.verify_transport
    )
    if jarvis_yaml is None:
        raise ConfigurationError(
            "live-test requires --jarvis-yaml or cluster live_test.jarvis_yaml"
        )
    if not jarvis_yaml.exists():
        raise ConfigurationError(f"live-test JARVIS YAML does not exist: {jarvis_yaml}")
    if agent_child_jarvis_yaml is not None and not agent_child_jarvis_yaml.exists():
        raise ConfigurationError(
            f"live-test agent child JARVIS YAML does not exist: {agent_child_jarvis_yaml}"
        )
    if agent_child_jarvis_yaml is not None and agent_mcp_config is None:
        raise ConfigurationError(
            "live-test --agent-child-jarvis-yaml requires --agent-mcp-config "
            "or cluster live_test.agent_mcp_config"
        )
    if agent_child_jarvis_yaml is not None and agent_prompt is not None:
        raise ConfigurationError(
            "live-test cannot use both an explicit agent prompt and agent_child_jarvis_yaml"
        )
    transport_token: str | None = None
    transport_secret_key: str | None = None
    if verify_transport:
        transport_token, transport_secret_key = _require_transport_secrets(
            token=options.transport_token,
            secret_key=options.transport_secret_key,
        )
    run_id = _acceptance_run_id(jarvis_yaml)
    pipeline_yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    pipeline_yaml_text = _stage_acceptance_files(
        options.definition,
        jarvis_yaml=jarvis_yaml,
        pipeline_yaml_text=pipeline_yaml_text,
        run_id=run_id,
        runner=command_runner,
    )
    expected_progress_adapter = _expected_progress_adapter(pipeline_yaml_text)

    lines: list[str] = []
    lines.extend(run_cluster_doctor(options.definition))
    if verify_transport:
        assert transport_token is not None
        assert transport_secret_key is not None
        lines.extend(
            _verify_transport(
                options,
                token=transport_token,
                secret_key=transport_secret_key,
                pipeline_yaml=pipeline_yaml_text,
                expected_progress_adapter=expected_progress_adapter,
            )
        )
    remote_yaml = f".local/share/clio-relay/live-tests/{run_id}/pipeline.yaml"
    _remote_write_file(
        options.definition.ssh_host,
        remote_yaml,
        pipeline_yaml_text.encode("utf-8"),
        runner=command_runner,
    )
    lines.append(f"acceptance.pipeline={remote_yaml}")
    if agent_child_jarvis_yaml is not None:
        agent_prompt = _write_generated_agent_prompt(
            options.definition,
            cluster=options.cluster,
            run_id=run_id,
            child_yaml=agent_child_jarvis_yaml,
            runner=command_runner,
        )
        lines.append(f"acceptance.agent_prompt={agent_prompt}")

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
        expected_progress_adapter=expected_progress_adapter,
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
        agent_job = _wait_for_success(
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
                agent_created_at=str(agent_job["created_at"]),
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
                expected_progress_adapter=expected_progress_adapter,
            )

    lines.append("live acceptance passed")
    return lines


def _verify_transport(
    options: LiveAcceptanceOptions,
    *,
    token: str,
    secret_key: str,
    pipeline_yaml: str,
    expected_progress_adapter: str | None,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_frp_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        frpc_bin=options.transport_frpc_bin,
        token=token,
        secret_key=secret_key,
        local_bind_port=(
            options.definition.live_test.transport_local_bind_port
            if options.transport_local_bind_port is None
            else options.transport_local_bind_port
        ),
        remote_api_port=(
            options.definition.live_test.transport_remote_api_port
            if options.transport_remote_api_port is None
            else options.transport_remote_api_port
        )
        or _unique_transport_port(run_suffix),
        proxy_name=(
            options.transport_proxy_name
            or options.definition.live_test.transport_proxy_name
            or f"relay-http-live-test-{run_suffix}"
        ),
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        http_check=lambda local_url: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=expected_progress_adapter,
        ),
    )


def _unique_transport_port(run_suffix: str) -> int:
    return 20000 + (int(run_suffix[:6], 16) % 20000)


def _verify_transport_http_api(
    local_url: str,
    *,
    cluster: str,
    pipeline_yaml: str,
    api_token: str | None,
    timeout_seconds: float,
    poll_seconds: float,
    expected_progress_adapter: str | None,
) -> list[str]:
    run_digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()[:16]
    idempotency_key = f"live-test:http-transport:{cluster}:{run_digest}:{uuid4().hex}"
    submitted = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "POST",
            "/jobs/jarvis",
            api_token=api_token,
            body={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml,
                "idempotency_key": idempotency_key,
            },
            timeout_seconds=10,
        ),
    )
    job_id = str(submitted["job_id"])
    _wait_for_transport_http_success(
        local_url,
        job_id,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    monitor = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/monitor",
            api_token=api_token,
            query={"cursor": "1", "limit": "250"},
            timeout_seconds=10,
        ),
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"transport HTTP job missing events: {sorted(missing_events)}")
    stdout = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/logs/stdout",
            api_token=api_token,
            query={"offset": "0", "limit": "65536"},
            timeout_seconds=10,
        ),
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("transport HTTP stdout log was empty")
    artifacts = cast(
        list[dict[str, Any]],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/artifacts",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    artifact_kinds = {artifact["kind"] for artifact in artifacts}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(
            f"transport HTTP artifacts missing required kinds: {sorted(artifact_kinds)}"
        )
    provenance_id = next(
        str(artifact["artifact_id"]) for artifact in artifacts if artifact["kind"] == "provenance"
    )
    provenance = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/artifacts/{provenance_id}/content",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    if provenance["artifact"]["artifact_id"] != provenance_id:
        raise RelayError("transport HTTP provenance artifact id mismatch")
    if provenance["encoding"] != "base64" or str(provenance["data"]) == "":
        raise RelayError("transport HTTP provenance artifact was empty")
    lines = [
        f"transport.http_job_id={job_id}",
        "transport.http_wait=succeeded",
        "transport.http_events=ok",
        f"transport.http_stdout_bytes={stdout['next_offset']}",
        "transport.http_artifacts=ok",
        "transport.http_provenance=ok",
    ]
    if expected_progress_adapter is not None:
        progress = cast(
            list[dict[str, Any]],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}/progress",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        _assert_progress_adapter(progress, expected_progress_adapter, job_id=job_id)
        lines.append(f"transport.http_progress_adapter={expected_progress_adapter}")
    return lines


def _http_json(
    base_url: str,
    method: str,
    path: str,
    *,
    api_token: str | None,
    body: dict[str, object] | None = None,
    query: dict[str, str] | None = None,
    timeout_seconds: float,
) -> dict[str, Any] | list[dict[str, Any]]:
    encoded_query = "" if not query else "?" + urllib.parse.urlencode(query)
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base_url + path + encoded_query,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    if api_token is not None:
        request.add_header("Authorization", f"Bearer {api_token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RelayError(f"transport HTTP request failed: {method} {path}: {detail}") from exc
    except (OSError, urllib.error.URLError) as exc:
        raise RelayError(f"transport HTTP request failed: {method} {path}: {exc}") from exc
    return cast(dict[str, Any] | list[dict[str, Any]], json.loads(payload))


def _wait_for_transport_http_success(
    local_url: str,
    job_id: str,
    *,
    api_token: str | None,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = cast(
            dict[str, Any],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        if job["state"] == "succeeded":
            return job
        if job["state"] in {"failed", "canceled"}:
            raise RelayError(f"transport HTTP job did not succeed: {job['state']}")
        if time.monotonic() >= deadline:
            raise RelayError(f"transport HTTP job did not reach terminal state: {job_id}")
        time.sleep(poll_seconds)


def _require_transport_secrets(
    *,
    token: str | None,
    secret_key: str | None,
) -> tuple[str, str]:
    if token is None:
        raise ConfigurationError("live transport acceptance requires a frp token")
    if secret_key is None:
        raise ConfigurationError("live transport acceptance requires an stcp secret")
    return token, secret_key


def _write_generated_agent_prompt(
    definition: ClusterDefinition,
    *,
    cluster: str,
    run_id: str,
    child_yaml: Path,
    runner: CommandRunner,
) -> str:
    remote_home = _remote_home(definition.ssh_host, runner=runner)
    remote_prompt = f"{remote_home}/.local/share/clio-relay/live-tests/{run_id}/agent-prompt.md"
    idempotency_key = f"live-test:{cluster}:{run_id}:agent-child"
    child_pipeline_yaml = _stage_acceptance_files(
        definition,
        jarvis_yaml=child_yaml,
        pipeline_yaml_text=child_yaml.read_text(encoding="utf-8"),
        run_id=f"{run_id}-agent-child",
        runner=runner,
    )
    prompt = _generated_agent_prompt(
        cluster=cluster,
        idempotency_key=idempotency_key,
        pipeline_yaml=child_pipeline_yaml,
    )
    _remote_write_file(
        definition.ssh_host,
        remote_prompt,
        prompt.encode("utf-8"),
        runner=runner,
    )
    return remote_prompt


def _remote_home(ssh_host: str, *, runner: CommandRunner) -> str:
    home = _remote_shell(ssh_host, 'printf "%s" "$HOME"', runner=runner).strip()
    if not home.startswith("/"):
        raise RelayError(f"remote HOME did not resolve to an absolute path: {home}")
    return home


def _generated_agent_prompt(
    *,
    cluster: str,
    idempotency_key: str,
    pipeline_yaml: str,
) -> str:
    return (
        "Use only the MCP tool named relay_submit_jarvis_pipeline. "
        "Do not use shell commands.\n\n"
        "Call relay_submit_jarvis_pipeline with:\n"
        f"- cluster: {cluster}\n"
        f"- idempotency_key: {idempotency_key}\n"
        "- pipeline_yaml: the exact YAML below\n\n"
        "After the tool returns, respond with only the relay job id.\n\n"
        "```yaml\n"
        f"{pipeline_yaml.rstrip()}\n"
        "```\n"
    )


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
    expected_progress_adapter: str | None = None,
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
    if expected_progress_adapter is not None:
        progress = _remote_clio_json(
            definition,
            ["job", "progress", job_id],
            runner=runner,
        )
        _assert_progress_adapter(
            cast(list[dict[str, Any]], progress),
            expected_progress_adapter,
            job_id=job_id,
        )
        lines.append(f"{line_prefix}.progress_adapter={expected_progress_adapter}")


def _expected_progress_adapter(pipeline_yaml: str) -> str | None:
    loaded = yaml.safe_load(pipeline_yaml)
    typed_document = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    for package in cast(list[object], packages):
        if not isinstance(package, dict):
            continue
        typed_package = cast(dict[str, Any], package)
        package_type = typed_package.get("pkg_type")
        if package_type in {"builtin.lammps", "lammps", "jarvis_cd.builtin.lammps"}:
            return "lammps"
        progress = typed_package.get("progress")
        if not isinstance(progress, dict):
            continue
        typed_progress = cast(dict[str, Any], progress)
        adapter = typed_progress.get("adapter")
        if isinstance(adapter, str) and adapter not in {"", "none"}:
            return adapter
    return None


def _assert_progress_adapter(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
) -> None:
    for item in progress:
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, Any], metadata)
        if (
            typed_metadata.get("adapter") == expected_adapter
            and typed_metadata.get("source") == "jarvis_package"
            and isinstance(typed_metadata.get("package_name"), str)
            and isinstance(typed_metadata.get("package_version"), str)
            and typed_metadata.get("run_id") == job_id
            and typed_metadata.get("execution_id") == job_id
        ):
            return
    raise RelayError(f"expected package progress adapter was not recorded: {expected_adapter}")


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
    agent_created_at: str,
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
    agent_created = _parse_datetime(agent_created_at)
    stale_child_ids: list[str] = []
    for child_job_id in reversed(child_job_ids):
        child_created = _child_job_created_at(
            definition,
            child_job_id,
            runner=runner,
        )
        if child_created >= agent_created:
            return child_job_id
        stale_child_ids.append(child_job_id)
    raise RelayError(
        "acceptance agent only reported stale child relay jobs created before "
        f"the agent run: {stale_child_ids}"
    )


def _child_job_created_at(
    definition: ClusterDefinition,
    child_job_id: str,
    *,
    runner: CommandRunner,
) -> datetime:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", child_job_id, "--cursor", "1", "--limit", "1"],
        runner=runner,
    )
    return _parse_datetime(str(monitor["job"]["created_at"]))


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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


def _stage_acceptance_files(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    pipeline_yaml_text: str,
    run_id: str,
    runner: CommandRunner,
) -> str:
    loaded = cast(object, yaml.safe_load(pipeline_yaml_text))
    if not isinstance(loaded, dict):
        return pipeline_yaml_text
    document = cast(dict[str, object], loaded)
    relay_extension = document.pop("x_clio_relay", None)
    if relay_extension is None:
        return yaml.safe_dump(document, sort_keys=False)
    if not isinstance(relay_extension, dict):
        raise ConfigurationError("x_clio_relay must be an object")
    typed_extension = cast(dict[str, object], relay_extension)
    stage_files = typed_extension.get("stage_files", [])
    if not isinstance(stage_files, list):
        raise ConfigurationError("x_clio_relay.stage_files must be a list")
    for item in cast(list[object], stage_files):
        if not isinstance(item, dict):
            raise ConfigurationError("x_clio_relay.stage_files entries must be objects")
        typed_item = cast(dict[str, object], item)
        local_path_value = typed_item.get("local_path")
        remote_path_value = typed_item.get("remote_path")
        if not isinstance(local_path_value, str) or not isinstance(remote_path_value, str):
            raise ConfigurationError(
                "x_clio_relay.stage_files entries require local_path and remote_path strings"
            )
        local_path = Path(local_path_value)
        if not local_path.is_absolute():
            local_path = jarvis_yaml.parent / local_path
        if not local_path.exists():
            raise ConfigurationError(f"staged acceptance file does not exist: {local_path}")
        remote_path = remote_path_value.format(run_id=run_id)
        _remote_write_file(
            definition.ssh_host,
            remote_path,
            local_path.read_bytes(),
            runner=runner,
        )
    formatted_document = _format_run_id(document, run_id)
    return yaml.safe_dump(formatted_document, sort_keys=False)


def _format_run_id(value: object, run_id: str) -> object:
    if isinstance(value, str):
        return value.format(run_id=run_id)
    if isinstance(value, list):
        return [_format_run_id(item, run_id) for item in cast(list[object], value)]
    if isinstance(value, dict):
        typed = cast(dict[object, object], value)
        return {str(key): _format_run_id(item, run_id) for key, item in typed.items()}
    return value


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
    agent_bin = _cluster_agent_bin(definition)
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


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


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
