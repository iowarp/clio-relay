"""clio-relay#183 residual + #248: typed reasons for a generic mcp_call dispatch failure.

Two layers:

* Pure-function unit coverage for ``clio_relay.mcp_call_result_error``
  (structural extraction from a result document's own
  ``protocol_result.isError`` / ``structured_result.error`` /
  ``protocol_error`` fields, plus the no-result-document shape).
* End-to-end ``EndpointWorker`` lifecycle coverage proving the typed reason
  reaches the durable job record for a non-``jarvis_run`` ``mcp_call`` --
  the JARVIS-route-scoped refusal detector (``_trusted_jarvis_mcp_route``'s
  default ``expected_tool="jarvis_run"``) never reaches these dispatches, so
  before this slice they fell straight to a bare ``exit code N``. Uses the
  fresh live evidence this slice's own campaign is grounded in: a
  ``spack_install`` MCP call answering with a fully typed
  ``spack.mcp.error.v1`` error, and a dispatch that produces no terminal
  result document at all (a spawn failure, #248's other live example).
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.mcp_call_result_error import (
    DISPATCH_NO_RESULT_DOCUMENT_REASON,
    MCP_CALL_RESULT_ERROR_REASON,
    dispatch_no_result_document_detail,
    mcp_call_dispatch_failure_text,
    mcp_call_result_error,
)
from clio_relay.models import Cursor, EndpointRole, JobKind, JobState, McpCallSpec, RelayJob

# --------------------------------------------------------------------------
# Pure-function unit coverage.
# --------------------------------------------------------------------------


def test_typed_structured_error_is_extracted_regardless_of_schema() -> None:
    """The live spack_install specimen: a typed error under any tool's own schema."""
    document: dict[str, object] = {
        "tool": "spack_install",
        "protocol_result": {"isError": True},
        "structured_result": {
            "schema_version": "spack.mcp.error.v1",
            "error": {
                "code": "command_failed",
                "detail": "==> Error: No such variant {'osmesa'}",
                "schema_version": "spack.mcp.error.v1",
            },
        },
        "protocol_error": "tools/call returned isError=true",
        "stderr": "",
    }

    detail = mcp_call_result_error(document)

    assert detail is not None
    assert detail["reason"] == MCP_CALL_RESULT_ERROR_REASON
    assert detail["tool"] == "spack_install"
    assert detail["code"] == "command_failed"
    assert detail["detail"] == "==> Error: No such variant {'osmesa'}"
    assert detail["tool_error_schema_version"] == "spack.mcp.error.v1"
    assert detail["protocol_error"] == "tools/call returned isError=true"
    assert mcp_call_result_error_text_contains(detail, "command_failed")
    assert mcp_call_result_error_text_contains(detail, "spack_install")


def mcp_call_result_error_text_contains(detail: dict[str, object], needle: str) -> bool:
    return needle in mcp_call_dispatch_failure_text(detail)


def test_protocol_error_only_is_still_typed() -> None:
    """#248's own specimen: a process exited mid-handshake -- no structured error,
    but the runner's own populated ``protocol_error`` (plus stderr) is still typed.
    """
    document: dict[str, object] = {
        "tool": None,
        "protocol_result": None,
        "structured_result": None,
        "protocol_error": '{"code": -32000, "message": "process exited before answering"}',
        "stderr": "jarvis-mcp: error: argument --jarvis-root: JARVIS root does not exist: "
        "/mnt/common/x/jarvis-state",
        "returncode": 2,
    }

    detail = mcp_call_result_error(document)

    assert detail is not None
    assert detail["code"] is None
    assert detail["protocol_error"] == (
        '{"code": -32000, "message": "process exited before answering"}'
    )
    assert detail["stderr_tail"] is not None
    assert "jarvis-root" in cast(str, detail["stderr_tail"])
    text = mcp_call_dispatch_failure_text(detail)
    assert "process exited before answering" in text


def test_clean_result_reports_no_error() -> None:
    document: dict[str, object] = {
        "tool": "jarvis_get_execution",
        "protocol_result": {"isError": False},
        "structured_result": {"ok": True},
        "protocol_error": None,
    }

    assert mcp_call_result_error(document) is None


@pytest.mark.parametrize(
    "structured_result",
    [None, {"error": "not-a-dict"}, {"error": {}}],
)
def test_malformed_or_absent_error_object_never_crashes(
    structured_result: object,
) -> None:
    """A malformed error shape falls back to protocol_error/isError, never raises."""
    document: dict[str, object] = {
        "tool": "spack_install",
        "protocol_result": {"isError": True},
        "structured_result": structured_result,
        "protocol_error": None,
    }
    detail = mcp_call_result_error(document)
    assert detail is not None
    assert detail["code"] is None


def test_dispatch_no_result_document_detail_bounds_stderr_tail() -> None:
    detail = dispatch_no_result_document_detail(
        returncode=1,
        stderr_tail="x" * 5000,
    )
    assert detail["reason"] == DISPATCH_NO_RESULT_DOCUMENT_REASON
    assert detail["returncode"] == 1
    assert len(cast(str, detail["stderr_tail"])) == 2_000
    text = mcp_call_dispatch_failure_text(detail)
    assert "exit code 1" in text


# --------------------------------------------------------------------------
# End-to-end EndpointWorker lifecycle coverage.
# --------------------------------------------------------------------------


class _GenericMcpCallProvider(JarvisCdProvider):
    """Fabricate one non-jarvis_run ``mcp_call`` dispatch's outcome.

    ``document`` is written to ``mcp-result.json`` exactly as a real
    runner would when not ``None``; when ``None``, nothing is written at
    all (the #248 spawn-failure shape), and ``stderr_text`` is streamed
    through ``on_stderr`` so it lands in the job's own spool stderr.log --
    the bounded source :func:`~clio_relay.mcp_call_result_error.
    mcp_call_dispatch_failure_detail` reads back for the typed
    ``dispatch_no_result_document`` reason.
    """

    def __init__(
        self,
        *,
        document: dict[str, object] | None,
        returncode: int,
        stderr_text: str = "",
    ) -> None:
        super().__init__(jarvis_bin="jarvis")
        self._document = document
        self._returncode = returncode
        self._stderr_text = stderr_text

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
        assert process_label == "endpoint MCP operation"
        assert cwd is not None
        if on_start is not None:
            on_start(4242)
        if self._document is not None:
            (cwd / "mcp-result.json").write_text(
                json.dumps(self._document, sort_keys=True),
                encoding="utf-8",
            )
        if self._stderr_text and on_stderr is not None:
            on_stderr(self._stderr_text)
        return subprocess.CompletedProcess(["endpoint-mcp-runner"], self._returncode, "", "")


def _submit_generic_mcp_call(queue: ClioCoreQueue, *, tool: str) -> RelayJob:
    return queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="registered-spack-mcp",
                server_args=["--stdio"],
                tool=tool,
                arguments={"spec": "hdf5+mpi"},
                timeout_seconds=600,
            ),
            idempotency_key=f"{tool}-run-001",
        )
    )


def _worker(
    settings: RelaySettings, queue: ClioCoreQueue, provider: JarvisCdProvider
) -> EndpointWorker:
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="test-cluster",
        queue=queue,
        provider=provider,
    )
    worker.register()
    return worker


def test_spack_install_typed_error_reaches_the_durable_job(tmp_path: Path) -> None:
    """The live north-star specimen: a spack_install typed error must not read
    as a bare 'exit code 1' -- the JARVIS-route-scoped refusal detector never
    covers this tool, so this is #183's residual gap, closed.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = _submit_generic_mcp_call(queue, tool="spack_install")
    structured: dict[str, object] = {
        "schema_version": "spack.mcp.error.v1",
        "error": {
            "code": "command_failed",
            "detail": "==> Error: No such variant {'osmesa'} for spec hdf5+mpi",
            "schema_version": "spack.mcp.error.v1",
        },
    }
    document: dict[str, object] = {
        "server": "registered-spack-mcp",
        "server_args": ["--stdio"],
        "tool": "spack_install",
        "arguments": {"spec": "hdf5+mpi"},
        "protocol_result": {
            "content": [{"type": "text", "text": json.dumps(structured, sort_keys=True)}],
            "isError": True,
        },
        "structured_result": structured,
        "protocol_error": "tools/call returned isError=true",
        "returncode": 1,
        "timed_out": False,
        "stderr": "",
    }
    provider = _GenericMcpCallProvider(document=document, returncode=1)

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "command_failed" in result.last_error
    assert "osmesa" in result.last_error
    assert result.last_error != "exit code 1"
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    dispatch_failure = cast(dict[str, Any], task.metadata["mcp_dispatch_failure"])
    assert dispatch_failure["schema_version"] == "clio-relay.mcp-call-result-error.v1"
    assert dispatch_failure["code"] == "command_failed"
    assert dispatch_failure["tool_error_schema_version"] == "spack.mcp.error.v1"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert any(event.event_type == "mcp.dispatch_result_error" for event in events)


def test_dispatch_with_no_result_document_carries_stderr_tail(tmp_path: Path) -> None:
    """#248's own OS-level spawn-failure specimen: no mcp-result.json is ever
    produced -- the typed reason must still carry the transport rc and the
    job's own captured stderr, never a bare 'exit code N'.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = _submit_generic_mcp_call(queue, tool="spack_install")
    provider = _GenericMcpCallProvider(
        document=None,
        returncode=1,
        stderr_text="[Errno 2] No such file or directory: 'spack'\n",
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "No such file or directory" in result.last_error
    assert result.last_error != "exit code 1"
    task = queue.list_tasks(job.job_id)[0]
    dispatch_failure = cast(dict[str, Any], task.metadata["mcp_dispatch_failure"])
    assert dispatch_failure["schema_version"] == "clio-relay.dispatch-no-result-document.v1"
    assert dispatch_failure["reason"] == DISPATCH_NO_RESULT_DOCUMENT_REASON
    assert dispatch_failure["returncode"] == 1
    assert "spack" in cast(str, dispatch_failure["stderr_tail"])
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert any(event.event_type == "mcp.dispatch_no_result_document" for event in events)
