"""clio-relay#162: refuse an empty ``jarvis_run`` pipeline before scheduler submission.

Two layers:

* Pure-function unit coverage for ``clio_relay.jarvis_pipeline_precheck``
  (declared step-count extraction from a ``jarvis_describe(target="pipeline")``
  result, the typed refusal payload/text).
* End-to-end ``EndpointWorker`` lifecycle coverage proving: a pipeline with
  zero declared steps never reaches ``_run_execution_streaming`` (no
  scheduler submission attempted); a non-empty pipeline dispatches exactly
  as before; and an INCONCLUSIVE precheck (the query itself failed) changes
  nothing -- dispatch proceeds unchanged, never silently treated as either
  a refusal or a clean bill of health.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import endpoint_jarvis_recovery
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_pipeline_precheck import (
    JARVIS_PIPELINE_EMPTY_REASON,
    empty_pipeline_refusal_payload,
    empty_pipeline_refusal_text,
    pipeline_describe_query_spec,
    pipeline_step_count,
)
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    deterministic_jarvis_execution_id,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from tests.execution_watch_fakes import envelope
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact

# --------------------------------------------------------------------------
# Pure-function unit coverage.
# --------------------------------------------------------------------------


def test_pipeline_describe_query_spec_targets_the_pipeline() -> None:
    base = McpCallSpec(
        server="clio-kit",
        server_args=["mcp-server", "jarvis"],
        env_from={"TOKEN": "SOURCE"},
        expected_server_artifact_digest="a" * 64,
        tool="jarvis_run",
        arguments={"pipeline_id": "p"},
    )
    spec = pipeline_describe_query_spec(base, pipeline_id="p", timeout_seconds=60)
    assert spec.tool == "jarvis_describe"
    assert spec.arguments == {"pipeline_id": "p", "target": "pipeline"}
    assert spec.env_from == {"TOKEN": "SOURCE"}
    assert spec.timeout_seconds == 60


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        (
            {"structured_result": {"result": {"target": "pipeline", "pipeline": {"pkgs": []}}}},
            0,
        ),
        (
            {
                "structured_result": {
                    "result": {
                        "target": "pipeline",
                        "pipeline": {"pkgs": [{"pkg_id": "a"}, {"pkg_id": "b"}]},
                    }
                }
            },
            2,
        ),
        ({"structured_result": None}, None),
        ({"structured_result": {"result": None}}, None),
        ({"structured_result": {"result": {"target": "step"}}}, None),
        ({"structured_result": {"result": {"target": "pipeline"}}}, None),
        (
            {
                "structured_result": {
                    "result": {"target": "pipeline", "pipeline": {"no_pkgs_key": True}}
                }
            },
            None,
        ),
    ],
)
def test_pipeline_step_count_reads_pkgs_or_is_inconclusive(
    document: dict[str, object],
    expected: int | None,
) -> None:
    assert pipeline_step_count(document) == expected


def test_empty_pipeline_refusal_payload_and_text() -> None:
    payload = empty_pipeline_refusal_payload(pipeline_id="clio_demo_md2", execution_id="exec-1")
    assert payload["reason"] == JARVIS_PIPELINE_EMPTY_REASON
    assert payload["pipeline_id"] == "clio_demo_md2"
    assert payload["execution_id"] == "exec-1"
    text = empty_pipeline_refusal_text(payload)
    assert "clio_demo_md2" in text
    assert "zero declared steps" in text


# --------------------------------------------------------------------------
# End-to-end EndpointWorker lifecycle coverage.
# --------------------------------------------------------------------------


class _PrecheckTransportProvider(JarvisCdProvider):
    """Fabricate the #162 precheck query and, if reached, the jarvis_run dispatch."""

    def __init__(
        self,
        *,
        pipeline_id: str,
        pkgs: list[dict[str, object]] | None,
        server_artifact: dict[str, Any],
        precheck_inconclusive: bool = False,
        run_returncode: int = 0,
    ) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.pipeline_id = pipeline_id
        self.pkgs = pkgs
        self.server_artifact = server_artifact
        self.precheck_inconclusive = precheck_inconclusive
        self.run_returncode = run_returncode
        self.precheck_calls = 0
        self.dispatch_calls = 0

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
        del command, credential_payload, should_cancel, on_poll
        del timeout_seconds, on_timeout
        assert cwd is not None
        if process_label == "jarvis pipeline precheck query":
            self.precheck_calls += 1
            if self.precheck_inconclusive:
                return subprocess.CompletedProcess(["jarvis-describe"], 1, "", "")
            params = json.loads((cwd / "params.json").read_text(encoding="utf-8"))
            query_spec = McpCallSpec.model_validate(params)
            structured: dict[str, object] = {
                "result": {
                    "target": "pipeline",
                    "pipeline": {"pkgs": self.pkgs if self.pkgs is not None else []},
                }
            }
            document = envelope(
                spec=query_spec,
                server_artifact=self.server_artifact,
                operation="tools/call",
                tool="jarvis_describe",
                structured_result=structured,
            )
            (cwd / "mcp-result.json").write_text(json.dumps(document), encoding="utf-8")
            return subprocess.CompletedProcess(["jarvis-describe"], 0, "", "")
        assert process_label == "endpoint MCP operation"
        self.dispatch_calls += 1
        if on_start is not None:
            on_start(4242)
        if on_stdout is not None:
            on_stdout("jarvis_run accepted\n")
        return subprocess.CompletedProcess(["jarvis-run"], self.run_returncode, "", "")


def _submit_pipeline_job(
    queue: ClioCoreQueue,
    *,
    command: list[str],
    digest: str,
    pipeline_id: str,
    idempotency_key: str,
) -> RelayJob:
    job = queue.submit_job(
        RelayJob(
            cluster="precheck-test",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_jarvis_recovery.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": pipeline_id},
            ),
            idempotency_key=idempotency_key,
        )
    )
    assert isinstance(job.spec, McpCallSpec)
    execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    assert job.spec.arguments["execution_id"] == execution_id
    return job


@pytest.fixture
def _precheck_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str]:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_jarvis_recovery, "jarvis_mcp_command", lambda: command)
    return settings, queue, command, server_artifact, digest


def test_empty_pipeline_is_refused_before_scheduler_submission(
    _precheck_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Live evidence reproduction: zero packages must never reach the scheduler."""
    settings, queue, command, server_artifact, digest = _precheck_env
    job = _submit_pipeline_job(
        queue,
        command=command,
        digest=digest,
        pipeline_id="clio_demo_md2",
        idempotency_key="empty-pipeline-001",
    )
    provider = _PrecheckTransportProvider(
        pipeline_id="clio_demo_md2", pkgs=[], server_artifact=server_artifact
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=provider,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "zero declared steps" in result.last_error
    assert provider.precheck_calls == 1
    assert provider.dispatch_calls == 0  # the scheduler was never reached
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    refusal = cast(dict[str, Any], task.metadata["jarvis_pipeline_empty_refusal"])
    assert refusal["reason"] == JARVIS_PIPELINE_EMPTY_REASON
    assert refusal["pipeline_id"] == "clio_demo_md2"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert any(event.event_type == "jarvis.pipeline_empty_refused" for event in events)


def test_non_empty_pipeline_dispatches_normally(
    _precheck_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """The precheck never blocks a pipeline that genuinely has declared steps."""
    settings, queue, command, server_artifact, digest = _precheck_env
    job = _submit_pipeline_job(
        queue,
        command=command,
        digest=digest,
        pipeline_id="copper-elastic-v1",
        idempotency_key="non-empty-pipeline-001",
    )
    provider = _PrecheckTransportProvider(
        pipeline_id="copper-elastic-v1",
        pkgs=[{"pkg_id": "copper-elastic-step1"}],
        server_artifact=server_artifact,
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=provider,
    )

    worker.run_once()

    assert provider.precheck_calls == 1
    assert provider.dispatch_calls == 1  # dispatch proceeded exactly as before #162
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert "jarvis.pipeline_empty_refused" not in {event.event_type for event in events}


def test_inconclusive_precheck_never_blocks_dispatch(
    _precheck_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """An unanswered/unreadable precheck query changes nothing -- never a refusal,
    never a fabricated clean bill of health either; just today's pre-#162
    behavior, with a typed, auditable event marking the gap.
    """
    settings, queue, command, server_artifact, digest = _precheck_env
    job = _submit_pipeline_job(
        queue,
        command=command,
        digest=digest,
        pipeline_id="unverifiable-pipeline",
        idempotency_key="inconclusive-precheck-001",
    )
    provider = _PrecheckTransportProvider(
        pipeline_id="unverifiable-pipeline",
        pkgs=None,
        server_artifact=server_artifact,
        precheck_inconclusive=True,
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=provider,
    )

    worker.run_once()

    assert provider.precheck_calls == 1
    assert provider.dispatch_calls == 1
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    event_types = {event.event_type for event in events}
    assert "jarvis.pipeline_precheck_inconclusive" in event_types
    assert "jarvis.pipeline_empty_refused" not in event_types
