"""Focused endpoint-owned remote MCP execution tests."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
)


class _DirectMcpProvider(JarvisCdProvider):
    """Capture the generic contained-command boundary without launching a process."""

    def __init__(self) -> None:
        super().__init__(jarvis_bin="forbidden-jarvis")
        self.commands: list[list[str]] = []
        self.environments: list[dict[str, str]] = []
        self.timeouts: list[int | None] = []

    def run_pipeline_streaming(self, *_args: object, **_kwargs: object) -> Any:
        """Fail if an MCP operation regresses to an outer JARVIS pipeline."""
        raise AssertionError("generic MCP call used an outer JARVIS pipeline")

    def run_command_streaming(
        self,
        command: list[str],
        *,
        process_label: str = "JARVIS-CD",
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        credential_payload: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Write the runner result exactly where the endpoint contract requires it."""
        del credential_payload, on_timeout
        assert cwd is not None
        assert env is not None
        assert should_cancel is not None and should_cancel() is False
        assert process_label == "endpoint MCP operation"
        self.commands.append(command)
        self.environments.append(env)
        self.timeouts.append(timeout_seconds)
        (cwd / "mcp-result.json").write_text(
            json.dumps(
                {
                    "server": "science-mcp",
                    "server_args": ["--stdio"],
                    "operation": "tools/call",
                    "tool": "inspect",
                    "arguments": {"dataset": "sample"},
                    "structured_result": {"observed": True},
                    "stdout": "protocol output\n",
                    "stderr": "",
                    "returncode": 0,
                    "timed_out": False,
                    "protocol_error": None,
                }
            ),
            encoding="utf-8",
        )
        if on_start is not None:
            on_start(4242)
        if on_stdout is not None:
            # MCP payload text is untrusted transport output, not evidence that
            # relay itself submitted or owns a scheduler job.
            on_stdout("Submitted batch job 999\n")
        if on_stderr is not None:
            on_stderr("")
        if on_poll is not None:
            on_poll()
        return subprocess.CompletedProcess(command, 0, "protocol output\n", "")


def test_worker_runs_generic_mcp_in_endpoint_containment_without_jarvis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic MCP calls retain receipts and cleanup without a JARVIS pipeline."""
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "must-not-reach-endpoint-mcp-runner")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="alpha",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="science-mcp",
                server_args=["--stdio"],
                tool="inspect",
                arguments={"dataset": "sample"},
                timeout_seconds=30,
            ),
            idempotency_key="direct-generic-mcp",
        )
    )
    provider = _DirectMcpProvider()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="alpha",
        queue=queue,
        provider=provider,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None and result.state is JobState.SUCCEEDED
    assert len(provider.commands) == 1
    command = provider.commands[0]
    assert Path(command[1]).name == "runner.py"
    assert command[2] == "mcp-request.json"
    assert "jarvis" not in Path(command[0]).name.lower()
    assert provider.timeouts == [35]
    assert "CLIO_RELAY_API_TOKEN" not in provider.environments[0]
    assert "CLIO_RELAY_RUNTIME_METADATA_TOKEN" not in provider.environments[0]
    assert "CLIO_RELAY_PROGRESS_TOKEN" not in provider.environments[0]
    assert "CLIO_RELAY_RUNTIME_SCHEDULER_PROVIDER" not in provider.environments[0]

    spool = settings.spool_dir / job.job_id
    request = json.loads((spool / "mcp-request.json").read_text(encoding="utf-8"))
    assert request["server"] == "science-mcp"
    assert request["tool"] == "inspect"
    assert request["arguments"] == {"dataset": "sample"}
    artifact_kinds = {artifact.kind for artifact in queue.list_artifacts(job.job_id)}
    assert artifact_kinds == {
        "log_capture",
        "mcp_request",
        "mcp_result",
        "provenance",
        "stderr",
        "stdout",
    }
    assert "jarvis_pipeline" not in artifact_kinds
    provenance = json.loads((spool / "provenance.json").read_text(encoding="utf-8"))
    assert provenance["provider"]["name"] == "clio-relay-endpoint-mcp"
    assert provenance["provider"]["outer_jarvis_pipeline"] is False
    assert provenance["spool"]["request"] == str(spool / "mcp-request.json")
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    event_types = {event.event_type for event in events}
    assert "mcp.started" in event_types
    assert "jarvis.started" not in event_types
    task = queue.list_tasks(job.job_id)[0]
    assert "scheduler_job_ids" not in task.metadata
    assert "runtime_sidecar_channel" not in task.metadata
    assert "scheduler_submission_intent" not in task.metadata["execution_sidecars"]
    assert "runtime_metadata" not in queue.get_job(job.job_id).metadata
    assert list(spool.glob(".progress-*.jsonl")) == []
    assert list(spool.glob(".runtime-*.jsonl")) == []
