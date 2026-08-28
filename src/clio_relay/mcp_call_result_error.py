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

clio-relay#248's own two live specimens BOTH land on :func:`mcp_call_result_error`'s
``protocol_error`` structural signal, not on :func:`dispatch_no_result_document_detail`
-- corrected mechanism attribution (adversarial-review Major 5; an earlier
revision of this docstring had both wrong):

* The registered ``remote-mcp refresh`` whose server process exec'd
  successfully but errored out of its own argument parsing before ever
  completing the MCP ``initialize`` handshake (``jarvis-mcp: error:
  argument --jarvis-root: ...``): the packaged runner's own child ``Popen``
  succeeds, the child exits nonzero having printed no matching JSON-RPC
  line, and the runner's own response parser (``_protocol_error``) writes
  ``protocol_error="missing tools/call response"`` into a document it DOES
  produce.
* The genuine OS-level spawn failure (#248's other live example,
  ``[Errno 2] No such file or directory: 'spack'``): the packaged runner's
  own ``try/except (OSError, ValueError)`` around that SAME inner ``Popen``
  (``mcp_call/runner.py``) already catches this and STILL writes a
  document, with ``protocol_error=f"MCP server launch failed: {exc}"``,
  ``returncode=1``, ``stderr`` carrying the OS error text. The runner never
  crashes uncaught for a spawn failure of the remote MCP server it is
  trying to reach -- that failure is exactly as typed as any other.

:func:`dispatch_no_result_document_detail` covers a narrower, rarer case
one layer OUT from the above: the packaged relay MCP RUNNER subprocess
itself (the one the endpoint worker spawns to drive the remote MCP child)
never produced ``mcp-result.json`` at all -- a relay-side infrastructure
failure (a bug/crash in the runner's own setup code, before it ever reaches
its own try/except; a forced kill; a disk-write failure), not a remote-MCP-
server spawn failure. Note also what this module deliberately does NOT
cover: if the runner subprocess fails to even START (``jarvis_provider.py``'s
own ``except (OSError, RuntimeError)`` around ``spawn_owned_process``, its
own module-local ~line 313), that raises a ``RelayError`` which propagates
straight past ``_run_job_impl``'s terminal-state logic entirely -- it never
reaches this module, because the call site that would invoke
:func:`mcp_call_dispatch_failure_detail` is never reached either.

Both structural cases above (protocol_error / a typed tool error) plus a
``timed_out`` document (``mcp_call/runner.py``'s own ``subprocess.
TimeoutExpired`` handler: ``returncode=124``, ``timed_out=True``,
``protocol_error=None`` -- otherwise an EASY miss, since every other field
IS empty) are all read back by :func:`mcp_call_result_error`, discriminated
STRUCTURALLY (result document present or not; ``timed_out``; ``isError``
flag; a typed error object, decoded from ``structured_result`` or, when
that key is absent, structurally from ``protocol_result.content[*].text``;
a populated ``protocol_error`` string), never by matching stdout/stderr
prose:

* :func:`dispatch_no_result_document_detail` -- no terminal
  ``mcp-result.json`` was ever produced (see above). Carries the transport
  returncode and a bounded stderr tail read back from the job's own spool.
* :func:`mcp_call_result_error` -- a result document exists, and its own
  ``timed_out`` / ``protocol_result.isError`` / ``structured_result.error``
  (or its ``protocol_result.content[*].text`` fallback) / ``protocol_error``
  fields say the call did not succeed.

:func:`mcp_call_dispatch_failure_detail` is the one orchestrating entry
point ``endpoint_job_execution.py`` calls (that module sits at its own
line-count ratchet ceiling, #774/#775, so it holds only the wiring) once
none of the higher-priority typed reasons (JARVIS dispatch refusal, #266
execution-watch failure, Ruling A returncode_conflict) applied. #265's
``outputs_missing`` signal never occupies a priority slot here -- owner
ruling, current: it can never be the reason a job is FAILED at all.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.protocol_messages import structured_result_from_protocol_result
from clio_relay.spool import MAX_LOG_READ_BYTES

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.models import RelayJob
    from clio_relay.spool import JobSpool

MCP_CALL_RESULT_ERROR_SCHEMA = "clio-relay.mcp-call-result-error.v1"
MCP_CALL_RESULT_ERROR_REASON = "mcp_call_result_error"
MCP_CALL_TIMED_OUT_REASON = "mcp_call_timed_out"
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


def _error_fields_from_structured(raw_structured: object) -> dict[str, object] | None:
    """Return the tool's own ``error`` object from one structured-result dict."""
    if not isinstance(raw_structured, dict):
        return None
    raw_error = cast(dict[str, object], raw_structured).get("error")
    if not isinstance(raw_error, dict):
        return None
    return cast(dict[str, object], raw_error)


def mcp_call_result_error(document: dict[str, object]) -> dict[str, object] | None:
    """Extract the dispatch's own typed error, structurally, from any MCP tool.

    Returns ``None`` when the result document reports no error by any of its
    FOUR structural signals -- ``timed_out``, ``protocol_result.isError``,
    ``structured_result.error``, a populated ``protocol_error`` -- callers
    must never invent a reason a document did not actually report.

    A durable document that timed out (``timed_out is True``, the runner's
    own signal on ``subprocess.TimeoutExpired``) is checked FIRST and
    reported with the distinct :data:`MCP_CALL_TIMED_OUT_REASON`: a timeout
    typically leaves every other field empty (``returncode=124``,
    ``protocol_error=None``), so it would otherwise fall all the way
    through to the same bare ``exit code 124`` this whole module exists to
    kill -- the exact defect class, not a lookalike.

    When the document carries no top-level ``structured_result`` at all,
    this falls back to structurally decoding
    ``protocol_result.content[*].text`` (:func:`~clio_relay.
    protocol_messages.structured_result_from_protocol_result`, the same
    parse ``result_document.py`` itself uses to BUILD ``structured_result``
    in the first place) -- the real shape a remote MCP server that never
    populates ``structuredContent`` produces, matching the MCP content-block
    contract rather than assuming every server takes the ``structuredContent``
    shortcut.
    """
    raw_timed_out = document.get("timed_out")
    raw_returncode = document.get("returncode")
    raw_stderr = document.get("stderr")
    raw_tool = document.get("tool")
    tool = raw_tool if isinstance(raw_tool, str) else None
    stderr_tail = (
        raw_stderr[-_MAX_DETAIL_TEXT_CHARS:] if isinstance(raw_stderr, str) and raw_stderr else None
    )
    if raw_timed_out is True:
        return {
            "schema_version": MCP_CALL_RESULT_ERROR_SCHEMA,
            "reason": MCP_CALL_TIMED_OUT_REASON,
            "tool": tool,
            "code": None,
            "detail": None,
            "tool_error_schema_version": None,
            "protocol_error": None,
            "returncode": raw_returncode if isinstance(raw_returncode, int) else None,
            "stderr_tail": stderr_tail,
        }
    raw_protocol_result = document.get("protocol_result")
    protocol_result = (
        cast(dict[str, object], raw_protocol_result)
        if isinstance(raw_protocol_result, dict)
        else None
    )
    is_error = protocol_result is not None and protocol_result.get("isError") is True
    error_fields = _error_fields_from_structured(document.get("structured_result"))
    if error_fields is None and protocol_result is not None:
        raw_operation = document.get("operation")
        operation = raw_operation if isinstance(raw_operation, str) else "tools/call"
        fallback_structured = structured_result_from_protocol_result(
            protocol_result, operation=operation
        )
        error_fields = _error_fields_from_structured(fallback_structured)
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
    return {
        "schema_version": MCP_CALL_RESULT_ERROR_SCHEMA,
        "reason": MCP_CALL_RESULT_ERROR_REASON,
        "tool": tool,
        "code": raw_code if isinstance(raw_code, str) else None,
        "detail": detail[:_MAX_DETAIL_TEXT_CHARS] if isinstance(detail, str) else None,
        "tool_error_schema_version": (
            raw_tool_schema if isinstance(raw_tool_schema, str) else None
        ),
        "protocol_error": protocol_error[:_MAX_DETAIL_TEXT_CHARS] if protocol_error else None,
        "returncode": raw_returncode if isinstance(raw_returncode, int) else None,
        "stderr_tail": stderr_tail,
    }


def mcp_call_dispatch_failure_text(detail: dict[str, object]) -> str:
    """Render any of this module's typed detail shapes as one bounded line."""
    if detail.get("reason") == DISPATCH_NO_RESULT_DOCUMENT_REASON:
        returncode = detail.get("returncode")
        tail = detail.get("stderr_tail")
        suffix = f": {tail}" if isinstance(tail, str) and tail else ""
        return f"MCP dispatch produced no terminal result document (exit code {returncode}){suffix}"
    if detail.get("reason") == MCP_CALL_TIMED_OUT_REASON:
        tool = detail.get("tool")
        returncode = detail.get("returncode")
        tail = detail.get("stderr_tail")
        suffix = f": {tail}" if isinstance(tail, str) and tail else ""
        named = (
            f"MCP call {tool} timed out" if isinstance(tool, str) and tool else "MCP call timed out"
        )
        return f"{named} (exit code {returncode}){suffix}"
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
    endpoint_mcp_call: bool,
    dispatch_refusal_present: bool,
    watch_failure_present: bool,
    application_verdict_failure_present: bool,
) -> dict[str, object] | None:
    """Resolve #183/#248's residual typed reason for a failed ``mcp_call`` dispatch.

    Owns its own priority guard (adversarial-review item 2: pushed the
    guard in from ``endpoint_job_execution.py``, which sits at its own
    line-count ratchet ceiling, #774/#775) -- ``None`` whenever this is not
    an endpoint-owned ``mcp_call`` at all, or one of the higher-priority
    typed reasons (JARVIS dispatch refusal, #266 execution-watch failure,
    Ruling A returncode_conflict) already applied. #265's ``outputs_missing``
    signal is deliberately NOT a guard input here: owner ruling, current,
    is that it can never be the reason a job is FAILED at all (existence/
    size heuristics deciding success/failure are banned), so it can never
    suppress this tier either -- unlike the earlier Ruling B shape this
    guard used to also check (a GATED, since-removed
    ``outputs_missing_failure_present``).
    """
    if (
        not endpoint_mcp_call
        or dispatch_refusal_present
        or watch_failure_present
        or application_verdict_failure_present
    ):
        return None
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
