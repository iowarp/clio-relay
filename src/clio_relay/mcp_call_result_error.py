"""Typed reasons for a generic (non-``jarvis_run``) MCP call dispatch failure.

clio-relay#183 residual + clio-relay#248: ``_ingest_mcp_runtime_metadata``
(``endpoint_runtime_metadata_ingest.py``) only recognizes a durable dispatch
refusal on the JARVIS-route-scoped ``jarvis_run`` tool
(``_trusted_jarvis_mcp_route``'s default ``expected_tool="jarvis_run"``) --
every OTHER endpoint-owned ``mcp_call`` (a registered ``remote-mcp refresh``
tools/list discovery, ``spack_install``, ...) that fails falls all the way
through ``_run_job_impl``'s terminal-failure branch to a bare
``f"exit code {effective_returncode}"``, discarding whatever typed reason
the dispatch itself already carried.

Fresh live evidence (2026-08-26 ares operations) is this module's north
star: a ``spack_install`` MCP call answered with a fully typed error in its
own result payload (``{"error":{"code":"command_failed","detail":"==>
Error: No such variant {'osmesa'} ...","schema_version":
"spack.mcp.error.v1"}}``, ``protocol_error="tools/call returned
isError=true"``) -- the durable job's ONLY failure field was
``last_error: "exit code 1"``. The typed reason existed in
``mcp-result.json`` the whole time; the verdict layer just never read it
back out for anything other than ``jarvis_run``.

clio-relay#248's own specimen (a ``remote-mcp refresh`` whose registered
server process exec'd successfully but errored out of its own argument
parsing before ever completing the MCP ``initialize`` handshake) and a
genuine OS-level spawn failure (``Popen`` raising ``FileNotFoundError``
before the packaged runner can write anything at all -- #248's other live
example, ``[Errno 2] No such file or directory: 'spack'``) are the same
defect family: the runner-produced ``mcp-result.json`` (or its total
absence) already carries a faithful, structural reason; this module is the
one place that reads it back out. Two distinct, typed outcomes -- #248's
own "process failed to start" vs "server started and refused" split, but
discriminated STRUCTURALLY (result document present or not; ``isError``
flag; a typed error object; a populated ``protocol_error`` string the
runner itself sets exactly when no matching response was ever seen), never
by matching stdout/stderr prose:

* :func:`dispatch_no_result_document_detail` -- no terminal
  ``mcp-result.json`` was ever produced (the runner crashed before writing
  one, e.g. an uncaught ``Popen`` spawn error). Carries the transport
  returncode and a bounded stderr tail read back from the job's own spool.
* :func:`mcp_call_result_error` -- a result document exists, and its own
  ``protocol_result.isError`` / ``structured_result.error`` /
  ``protocol_error`` fields say the call did not succeed.

:func:`mcp_call_dispatch_failure_detail` is the one orchestrating entry
point ``endpoint_job_execution.py`` calls (that module sits at its own
line-count ratchet ceiling, #774/#775, so it holds only the wiring) once
none of the higher-priority typed reasons (JARVIS dispatch refusal, #266
execution-watch failure, #265 outputs-missing) applied.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.spool import MAX_LOG_READ_BYTES

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.models import RelayJob
    from clio_relay.spool import JobSpool

MCP_CALL_RESULT_ERROR_SCHEMA = "clio-relay.mcp-call-result-error.v1"
MCP_CALL_RESULT_ERROR_REASON = "mcp_call_result_error"
DISPATCH_NO_RESULT_DOCUMENT_SCHEMA = "clio-relay.dispatch-no-result-document.v1"
DISPATCH_NO_RESULT_DOCUMENT_REASON = "dispatch_no_result_document"

#: Short human-facing text embedded directly in a typed detail payload --
#: matches jarvis_dispatch_failure.py's own MAX_REFUSAL_MESSAGE_CHARS
#: precedent (a small, local T1-style budget, not the durable-evidence tier).
_MAX_DETAIL_TEXT_CHARS = 2_000


def dispatch_no_result_document_detail(*, returncode: int, stderr_tail: str) -> dict[str, object]:
    """Typed reason for a dispatch that left no terminal result document at all."""
    return {
        "schema_version": DISPATCH_NO_RESULT_DOCUMENT_SCHEMA,
        "reason": DISPATCH_NO_RESULT_DOCUMENT_REASON,
        "returncode": returncode,
        "stderr_tail": stderr_tail[-_MAX_DETAIL_TEXT_CHARS:],
    }


def mcp_call_result_error(document: dict[str, object]) -> dict[str, object] | None:
    """Extract the dispatch's own typed error, structurally, from any MCP tool.

    Returns ``None`` when the result document reports no error by any of its
    three structural signals (``protocol_result.isError``,
    ``structured_result.error``, a populated ``protocol_error``) -- callers
    must never invent a reason a document did not actually report.
    """
    raw_protocol_result = document.get("protocol_result")
    is_error = (
        isinstance(raw_protocol_result, dict)
        and cast(dict[str, object], raw_protocol_result).get("isError") is True
    )
    raw_structured = document.get("structured_result")
    error_fields: dict[str, object] | None = None
    if isinstance(raw_structured, dict):
        raw_error = cast(dict[str, object], raw_structured).get("error")
        if isinstance(raw_error, dict):
            error_fields = cast(dict[str, object], raw_error)
    raw_protocol_error = document.get("protocol_error")
    protocol_error = (
        raw_protocol_error if isinstance(raw_protocol_error, str) and raw_protocol_error else None
    )
    if not is_error and error_fields is None and protocol_error is None:
        return None
    raw_code = error_fields.get("code") if error_fields is not None else None
    raw_detail = error_fields.get("detail") if error_fields is not None else None
    raw_message = error_fields.get("message") if error_fields is not None else None
    detail = (
        raw_detail
        if isinstance(raw_detail, str)
        else raw_message
        if isinstance(raw_message, str)
        else None
    )
    raw_tool_schema = error_fields.get("schema_version") if error_fields is not None else None
    raw_stderr = document.get("stderr")
    raw_tool = document.get("tool")
    return {
        "schema_version": MCP_CALL_RESULT_ERROR_SCHEMA,
        "reason": MCP_CALL_RESULT_ERROR_REASON,
        "tool": raw_tool if isinstance(raw_tool, str) else None,
        "code": raw_code if isinstance(raw_code, str) else None,
        "detail": detail[:_MAX_DETAIL_TEXT_CHARS] if isinstance(detail, str) else None,
        "tool_error_schema_version": (
            raw_tool_schema if isinstance(raw_tool_schema, str) else None
        ),
        "protocol_error": protocol_error[:_MAX_DETAIL_TEXT_CHARS] if protocol_error else None,
        "stderr_tail": (
            raw_stderr[-_MAX_DETAIL_TEXT_CHARS:]
            if isinstance(raw_stderr, str) and raw_stderr
            else None
        ),
    }


def mcp_call_dispatch_failure_text(detail: dict[str, object]) -> str:
    """Render either of this module's two typed detail shapes as one bounded line."""
    if detail.get("reason") == DISPATCH_NO_RESULT_DOCUMENT_REASON:
        returncode = detail.get("returncode")
        tail = detail.get("stderr_tail")
        suffix = f": {tail}" if isinstance(tail, str) and tail else ""
        return f"MCP dispatch produced no terminal result document (exit code {returncode}){suffix}"
    tool = detail.get("tool")
    code = detail.get("code")
    error_detail = detail.get("detail")
    protocol_error = detail.get("protocol_error")
    if isinstance(code, str) and code:
        message = (
            f"{code}: {error_detail}" if isinstance(error_detail, str) and error_detail else code
        )
    elif isinstance(protocol_error, str) and protocol_error:
        message = protocol_error
    else:
        message = "MCP call returned an error without a typed payload"
    return (
        f"MCP call {tool} failed: {message}"
        if isinstance(tool, str) and tool
        else (f"MCP call failed: {message}")
    )


def _bounded_stderr_tail(spool: JobSpool) -> str:
    """Read a bounded tail of the job's own captured ``stderr.log``, or "" absent."""
    stderr_path = internal_filesystem_path(spool.path / "stderr.log")
    if not stderr_path.is_file():
        return ""
    size = stderr_path.stat().st_size
    if size <= 0:
        return ""
    read_bytes = min(size, MAX_LOG_READ_BYTES)
    text, _next_offset, _eof = spool.read_log("stderr", offset=size - read_bytes, limit=read_bytes)
    return text[-_MAX_DETAIL_TEXT_CHARS:]


def mcp_call_dispatch_failure_detail(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    spool: JobSpool,
    returncode: int,
) -> dict[str, object] | None:
    """Resolve #183/#248's residual typed reason for a failed ``mcp_call`` dispatch.

    Only meaningful once none of the higher-priority typed reasons (JARVIS
    dispatch refusal, #266 execution-watch failure, #265 outputs-missing)
    applied -- see this module's own docstring for the two structural
    outcomes distinguished here.
    """
    result_path = spool.path / "mcp-result.json"
    storage_result_path = internal_filesystem_path(result_path)
    if not storage_result_path.is_file():
        detail = dispatch_no_result_document_detail(
            returncode=returncode,
            stderr_tail=_bounded_stderr_tail(spool),
        )
        queue.append_event(
            job.job_id,
            "mcp.dispatch_no_result_document",
            "MCP dispatch produced no terminal result document",
            payload=detail,
        )
        return detail
    try:
        document = json.loads(storage_result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    error_detail = mcp_call_result_error(cast(dict[str, object], document))
    if error_detail is not None:
        queue.append_event(
            job.job_id,
            "mcp.dispatch_result_error",
            "MCP dispatch result carried a typed error",
            payload=error_detail,
        )
    return error_detail
