"""Assemble the durable ``mcp-result.json`` document for one MCP call/list run.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). The
normal ``run_mcp_call_from_params`` flow always supplies a non-``None``
``server_artifact``, so ``_write_mcp_result``'s own re-discovery fallback (the
``if server_artifact is None`` branch) is rarely exercised -- but when it is, it
calls ``_server_artifact_identity``/``_server_artifact_digest``, both
individually monkeypatched on the ``runner`` facade by
``tests/test_mcp_call_runner.py``. Both calls go through ``_facade()`` so that
override still takes effect regardless of caller -- see
:mod:`clio_relay.clio_kit_runtime_identity` for the full reach-back
contract this decomposition wave relies on.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.bounded_payload import (
    STDERR_HEAD_MAX_BYTES,
    STDERR_TAIL_MAX_BYTES,
    STDOUT_HEAD_MAX_BYTES,
    STDOUT_TAIL_MAX_BYTES,
    bound_stream_capture,
)
from clio_relay.constants import _TOOLS_LIST_PAGINATION_KEY
from clio_relay.protocol_messages import _response_id, _response_result, _structured_result


def _write_mcp_result(
    *,
    result_path: Path,
    server: str,
    server_args: list[str],
    env_from: dict[str, str],
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    server_artifact: dict[str, Any] | None,
    observed_server_artifact_digest: str | None,
    execution_artifact: dict[str, Any] | None,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    jarvis_input_manifest: dict[str, Any] | None,
    returncode: int,
    stdout: str,
    stderr: str,
    started_at: float,
    timed_out: bool,
    protocol_error: str | None,
    progress_bridge: dict[str, Any] | None,
    result_validation: dict[str, Any] | None,
) -> None:
    finished_at = time.time()
    protocol_result = _response_result(stdout, response_id=_response_id(operation))
    pagination: dict[str, Any] | None = None
    if protocol_result is not None and isinstance(
        protocol_result.get(_TOOLS_LIST_PAGINATION_KEY), dict
    ):
        protocol_result = dict(protocol_result)
        pagination = protocol_result.pop(_TOOLS_LIST_PAGINATION_KEY)
    initialize_result = _response_result(stdout, response_id="clio-relay-mcp-init")
    protocol_version = (
        initialize_result.get("protocolVersion") if initialize_result is not None else None
    )
    server_info: object = (
        initialize_result.get("serverInfo", {}) if initialize_result is not None else {}
    )
    if server_artifact is None:
        server_artifact = (
            _facade()._server_artifact_identity(
                server,
                server_args,
                verify_relay_jarvis_cd_lock=True,
            )
            if expected_jarvis_cd_lock_binding is not None
            else _facade()._server_artifact_identity(server, server_args)
        )
    if observed_server_artifact_digest is None:
        observed_server_artifact_digest = _facade()._server_artifact_digest(server_artifact)
    # T3 (doc §6.4): bound the durable capture at RECORD time only, after every
    # protocol/pagination/initialize parse above has already run against the
    # full, unbounded stdout -- narrowing this earlier would corrupt a chatty
    # server's JSON-RPC parse (the read-time caps, MCP_SESSION_MAX_STDOUT_BYTES/
    # MCP_SESSION_MAX_STDERR_BYTES, stay generous and untouched). This is the
    # head+tail record-time bound doc §6.4/§6.5 named as R6's actual, larger-
    # than-a-lift scope: no such bound existed here before.
    bounded_stdout, stdout_truncation = bound_stream_capture(
        stdout,
        head_max=STDOUT_HEAD_MAX_BYTES,
        tail_max=STDOUT_TAIL_MAX_BYTES,
        stream_name="stdout",
    )
    bounded_stderr, stderr_truncation = bound_stream_capture(
        stderr,
        head_max=STDERR_HEAD_MAX_BYTES,
        tail_max=STDERR_TAIL_MAX_BYTES,
        stream_name="stderr",
    )
    result_document: dict[str, Any] = {
        "server": server,
        "server_args": server_args,
        "env_from": env_from,
        "operation": operation,
        "tool": tool,
        "arguments": arguments,
        "input_reconciliation": jarvis_input_manifest,
        "protocol_result": protocol_result,
        "structured_result": _structured_result(protocol_result, operation=operation),
        "protocol_version": protocol_version,
        "server_info": server_info,
        "server_artifact": server_artifact,
        "server_execution_artifact": execution_artifact,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "observed_server_artifact_digest": observed_server_artifact_digest,
        "pagination": pagination,
        "returncode": returncode,
        "stdout": bounded_stdout,
        "stderr": bounded_stderr,
        "stdout_truncation": stdout_truncation,
        "stderr_truncation": stderr_truncation,
        "timed_out": timed_out,
        "protocol_error": protocol_error,
        "package_progress_bridge": progress_bridge,
        "result_validation": result_validation,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": finished_at - started_at,
    }
    if expected_registered_contract is not None:
        result_document["expected_registered_contract"] = expected_registered_contract
    if expected_jarvis_cd_lock_binding is not None:
        result_document["expected_jarvis_cd_lock_binding"] = expected_jarvis_cd_lock_binding
    result_path.write_text(
        json.dumps(
            result_document,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
