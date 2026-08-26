"""Unit coverage for job_terminal_verdict.py's priority-ordered rendering.

Extracted from endpoint_job_execution.py (adversarial-review item 2: that
module sits at its own line-count ratchet ceiling, #774/#775) -- this file
proves the fixed priority order (dispatch refusal > execution-watch failure
> Ruling A application-verdict failure > Ruling B declared_outputs_missing
> #183/#248 generic mcp_call failure > bare exit code) survived the move,
and that a returncode_conflict verdict (Ruling A's new consumer) never
falls through to the bare exit code this whole campaign exists to kill.
"""

from __future__ import annotations

from typing import cast

from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal
from clio_relay.job_terminal_verdict import (
    terminal_failure_error_text,
    terminal_failure_message,
    terminal_failure_metadata,
)

_REFUSAL = JarvisDispatchRefusal(
    code="jarvis_run_failed",
    message="Spack executable was not found",
    pipeline_id="pipe",
    execution_id="exec",
    payload_schema_version="jarvis.error.v1",
)
_WATCH_FAILURE: dict[str, object] = {
    "schema_version": "clio-relay.execution-watch-failure.v1",
    "pipeline_id": "pipe",
    "execution_id": "exec",
    "state": "failed",
    "returncode": 1,
    "reason": "application exited with code 137",
}
_APPLICATION_VERDICT_FAILURE: dict[str, object] = {
    "schema_version": "clio-relay.application-verdict.v1",
    "status": "failed",
    "application_returncode": 3,
    "reason": "returncode_conflict",
}
_OUTPUTS_MISSING: dict[str, object] = {
    "schema_version": "clio-relay.execution-outputs-missing.v1",
    "reason": "declared_outputs_missing",
    "execution_id": "exec",
    "declared_count": 1,
    "missing": [
        {
            "relative_path": "dump.h5",
            "role": "output",
            "reason": "absent",
            "declared_size_bytes": 2048,
        }
    ],
}
_MCP_DISPATCH_FAILURE: dict[str, object] = {
    "schema_version": "clio-relay.mcp-call-result-error.v1",
    "reason": "mcp_call_result_error",
    "tool": "spack_install",
    "code": "command_failed",
    "detail": "==> Error: No such variant",
    "tool_error_schema_version": "spack.mcp.error.v1",
    "protocol_error": "tools/call returned isError=true",
    "returncode": 1,
    "stderr_tail": None,
}


def test_priority_order_dispatch_refusal_wins_over_everything() -> None:
    message = terminal_failure_message(
        dispatch_refusal=_REFUSAL,
        watch_failure=_WATCH_FAILURE,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=_OUTPUTS_MISSING,
        endpoint_mcp_call=True,
    )
    assert message == "JARVIS run failed"
    error = terminal_failure_error_text(
        dispatch_refusal=_REFUSAL,
        watch_failure=_WATCH_FAILURE,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=_OUTPUTS_MISSING,
        mcp_dispatch_failure=_MCP_DISPATCH_FAILURE,
        effective_returncode=1,
    )
    assert error is not None
    assert "jarvis_run_failed" in error


def test_priority_order_watch_failure_wins_over_application_verdict_and_below() -> None:
    message = terminal_failure_message(
        dispatch_refusal=None,
        watch_failure=_WATCH_FAILURE,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=_OUTPUTS_MISSING,
        endpoint_mcp_call=True,
    )
    assert message == "JARVIS execution ended in failure"
    error = terminal_failure_error_text(
        dispatch_refusal=None,
        watch_failure=_WATCH_FAILURE,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=_OUTPUTS_MISSING,
        mcp_dispatch_failure=None,
        effective_returncode=1,
    )
    assert error is not None
    assert "137" in error


def test_returncode_conflict_never_falls_through_to_a_bare_exit_code() -> None:
    """Ruling A's own point: a returncode_conflict must never read as a bare
    exit code just because no OTHER typed reason happened to fire.
    """
    message = terminal_failure_message(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=None,
        endpoint_mcp_call=True,
    )
    assert "returncode_conflict" in message
    error = terminal_failure_error_text(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=None,
        mcp_dispatch_failure=None,
        effective_returncode=1,
    )
    assert error is not None
    assert "returncode_conflict" in error
    assert "application_returncode=3" in error
    assert error != "exit code 1"


def test_priority_order_outputs_missing_wins_over_mcp_dispatch_failure() -> None:
    message = terminal_failure_message(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=_OUTPUTS_MISSING,
        endpoint_mcp_call=True,
    )
    assert "declared outputs are missing or empty" in message
    error = terminal_failure_error_text(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=_OUTPUTS_MISSING,
        mcp_dispatch_failure=_MCP_DISPATCH_FAILURE,
        effective_returncode=1,
    )
    assert error is not None
    assert "dump.h5" in error


def test_mcp_dispatch_failure_is_the_last_typed_reason_before_bare_exit_code() -> None:
    message = terminal_failure_message(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        endpoint_mcp_call=True,
    )
    assert message == "Endpoint MCP operation failed"
    error = terminal_failure_error_text(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        mcp_dispatch_failure=_MCP_DISPATCH_FAILURE,
        effective_returncode=1,
    )
    assert error is not None
    assert "command_failed" in error


def test_bare_exit_code_is_the_true_last_resort_only() -> None:
    error = terminal_failure_error_text(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        mcp_dispatch_failure=None,
        effective_returncode=7,
    )
    assert error == "exit code 7"


def test_non_mcp_call_message_names_jarvis_cd() -> None:
    message = terminal_failure_message(
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        endpoint_mcp_call=False,
    )
    assert message == "JARVIS-CD run failed"


def test_terminal_failure_metadata_folds_every_typed_reason_present() -> None:
    metadata = terminal_failure_metadata(
        effective_returncode=1,
        dispatch_refusal=None,
        watch_failure=_WATCH_FAILURE,
        application_verdict_failure=_APPLICATION_VERDICT_FAILURE,
        outputs_missing_detail=_OUTPUTS_MISSING,
        mcp_dispatch_failure=_MCP_DISPATCH_FAILURE,
    )
    assert metadata["returncode"] == 1
    assert metadata["execution_watch_failure"] == _WATCH_FAILURE
    assert metadata["application_verdict_failure"] == _APPLICATION_VERDICT_FAILURE
    assert metadata["execution_outputs_missing"] == _OUTPUTS_MISSING
    assert metadata["mcp_dispatch_failure"] == _MCP_DISPATCH_FAILURE
    assert "jarvis_dispatch_refusal" not in metadata


def test_terminal_failure_metadata_omits_absent_typed_reasons() -> None:
    metadata = terminal_failure_metadata(
        effective_returncode=1,
        dispatch_refusal=None,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        mcp_dispatch_failure=None,
    )
    assert metadata == {"returncode": 1}


def test_terminal_failure_metadata_carries_dispatch_refusal_payload() -> None:
    metadata = terminal_failure_metadata(
        effective_returncode=1,
        dispatch_refusal=_REFUSAL,
        watch_failure=None,
        application_verdict_failure=None,
        outputs_missing_detail=None,
        mcp_dispatch_failure=None,
    )
    refusal_payload = cast(dict[str, object], metadata["jarvis_dispatch_refusal"])
    assert refusal_payload["code"] == "jarvis_run_failed"
