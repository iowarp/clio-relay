"""Tests for the one door error-translation owner (clio-relay#231, R3).

Each test below is written failing-first against
``docs/design/relay-architecture-2026-08.md`` §6: the live hole (a
deliberately-bare re-raise), the #218/#215 regressions re-pointed at the
frozen ``REASONS`` table instead of their old ad hoc, per-site shapes, an
adapter-contract proof of the ``launcher_resolution_failed`` shape, a
sabotage twin proving unclassified exceptions never reach the wire, the
browser_gateway fourth adapter replacing its bare ``{"error": message}``
dict, and the opus re-review findings (F1-F16): the MCP-SDK code-band
collision, silent truncation, the unenforced byte budget, a hostile
``__str__``, and the ``payload_too_large``/deviation-ownership gaps.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
import socket
import subprocess
import sys
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import mcp_types
import mcp_types.jsonrpc
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP
from fastmcp.tools import ToolResult
from fastmcp_tasks.client import call_tool_task  # pyright: ignore[reportUnknownVariableType]
from fastmcp_tasks.client_models import ClientGetTaskResult, GetTaskRequest, GetTaskRequestParams
from mcp.shared.exceptions import MCPError

from clio_relay import door_errors
from clio_relay.browser_gateway import BrowserGatewayConfig, CapabilityProxyServer
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
    PublicMessageError,
    QueueConflictError,
    TaskInputParkConflictError,
)
from clio_relay.fastmcp_server import RelayMcpRuntime, RelayTasksExtension, RelayTool
from clio_relay.http_api import create_app
from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal
from clio_relay.job_identity import OwnerSessionIdentityError
from clio_relay.mcp_server import mcp_tool_definitions_and_remote_catalog
from clio_relay.models import JobKind, JobState
from clio_relay.storage_policy import StorageDecision, StorageReason
from clio_relay.storage_runtime import StorageAdmissionError, StorageRuntimeViolation

# --------------------------------------------------------------------------- #
# The frozen REASONS registry
# --------------------------------------------------------------------------- #

_EXPECTED_REASONS = frozenset(
    {
        "mcp_task_input_park_conflict",
        "mcp_task_conflict",
        "mcp_task_status_reconciliation_failed",
        "jarvis_dispatch_refused",
        "not_found",
        "configuration_error",
        "storage_admission_refused",
        "storage_safety_violation",
        "observation_timeout",
        "launcher_resolution_failed",
        "owner_session_identity_refused",
        "internal_error",
        "payload_too_large",
        "http_request_malformed",
        "poll_interval_invalid",
        "observation_pattern_invalid",
        "observation_pattern_unsafe",
        "log_stream_invalid",
        "log_offset_invalid",
        "log_offset_beyond_eof",
        "authentication_required",
        "resource_ownership_refused",
        "scheduler_job_ownership_refused",
        "session_scope_refused",
        "session_identity_unavailable",
        "session_status_unavailable",
        "session_intake_quiescence_unavailable",
        "session_admission_status_unavailable",
        "jarvis_runtime_authority_unavailable",
        "input_ingest_unavailable",
        "job_not_found",
        "task_not_found",
        "gateway_not_found",
        "artifact_not_found",
        "execution_not_found",
        "session_generation_identity_unavailable",
        "session_intake_closed",
        "session_binding_headers_required",
        "session_binding_identity_mismatch",
        "unbound_session_api",
        "job_cluster_mismatch",
        "job_submission_conflict",
        "mcp_submission_conflict",
        "jarvis_runtime_authority_conflict",
        "jarvis_artifact_conflict",
        "input_ingest_conflict",
        "transform_conflict",
        "gateway_cluster_mismatch",
        "gateway_conflict",
        "queue_operation_conflict",
        "retention_conflict",
        "job_submission_refused",
        "mcp_admission_refused",
        "input_ingest_refused",
        "job_route_refused",
        "jarvis_submission_refused",
        "transform_refused",
        "wait_parameters_invalid",
        "queue_query_refused",
        "input_ingest_terminalization_failed",
        "session_authentication_unavailable",
        "request_validation_failed",
        "route_not_found",
        "method_not_allowed",
        "framework_http_error",
        "browser_gateway_overloaded",
        "browser_preflight_refused",
        "browser_attachment_not_found",
        "browser_origin_refused",
        "browser_capability_refused",
        "browser_method_not_allowed",
        "browser_upstream_unavailable",
        "websocket_authentication_failed",
        "websocket_session_binding_failed",
        "websocket_page_limit_invalid",
        "websocket_poll_interval_invalid",
        "websocket_cursor_invalid",
        "websocket_resource_ownership_refused",
        "websocket_resource_not_found",
    }
)

_EXPECTED_R9_HTTP_STATUSES = {
    **dict.fromkeys(
        {
            "http_request_malformed",
            "poll_interval_invalid",
            "log_stream_invalid",
            "log_offset_invalid",
            "log_offset_beyond_eof",
        },
        400,
    ),
    "authentication_required": 401,
    **dict.fromkeys(
        {"resource_ownership_refused", "scheduler_job_ownership_refused", "session_scope_refused"},
        403,
    ),
    **dict.fromkeys(
        {
            "session_identity_unavailable",
            "session_status_unavailable",
            "session_intake_quiescence_unavailable",
            "session_admission_status_unavailable",
            "jarvis_runtime_authority_unavailable",
            "input_ingest_unavailable",
            "job_not_found",
            "task_not_found",
            "gateway_not_found",
            "artifact_not_found",
            "execution_not_found",
        },
        404,
    ),
    **dict.fromkeys(
        {
            "session_generation_identity_unavailable",
            "session_intake_closed",
            "session_binding_headers_required",
            "session_binding_identity_mismatch",
            "unbound_session_api",
            "job_cluster_mismatch",
            "job_submission_conflict",
            "mcp_submission_conflict",
            "jarvis_runtime_authority_conflict",
            "jarvis_artifact_conflict",
            "input_ingest_conflict",
            "transform_conflict",
            "gateway_cluster_mismatch",
            "gateway_conflict",
            "queue_operation_conflict",
            "retention_conflict",
        },
        409,
    ),
    **dict.fromkeys(
        {
            "job_submission_refused",
            "mcp_admission_refused",
            "input_ingest_refused",
            "job_route_refused",
            "jarvis_submission_refused",
            "transform_refused",
            "wait_parameters_invalid",
            "queue_query_refused",
        },
        422,
    ),
    "input_ingest_terminalization_failed": 500,
    "session_authentication_unavailable": 503,
    "request_validation_failed": 422,
    "route_not_found": 404,
    "method_not_allowed": 405,
    "framework_http_error": 500,
    "browser_gateway_overloaded": 503,
    **dict.fromkeys({"browser_preflight_refused", "browser_origin_refused"}, 403),
    "browser_attachment_not_found": 404,
    "browser_capability_refused": 401,
    "browser_method_not_allowed": 405,
    "browser_upstream_unavailable": 502,
    "websocket_authentication_failed": 401,
    "websocket_session_binding_failed": 409,
    "websocket_page_limit_invalid": 422,
    **dict.fromkeys({"websocket_poll_interval_invalid", "websocket_cursor_invalid"}, 400),
    "websocket_resource_ownership_refused": 403,
    "websocket_resource_not_found": 404,
}


def test_every_reason_is_registered() -> None:
    """The frozen set is exactly the doc §6.3 table -- no more, no fewer."""
    assert set(door_errors.REASONS) == _EXPECTED_REASONS
    assert len(door_errors.REASONS) == 79
    for reason, spec in door_errors.REASONS.items():
        assert spec.reason == reason
        assert len(reason) <= 64
        assert re.fullmatch(r"[a-z][a-z0-9_]*", reason)
        assert isinstance(spec.retryable, bool)
        assert isinstance(spec.mcp_code, int) and spec.mcp_code < 0
        assert 400 <= spec.http_status < 600
        assert spec.title
    assert len(_EXPECTED_R9_HTTP_STATUSES) == 64
    assert {
        reason: door_errors.REASONS[reason].http_status for reason in _EXPECTED_R9_HTTP_STATUSES
    } == _EXPECTED_R9_HTTP_STATUSES


def test_reasons_is_a_read_only_mapping() -> None:
    """F9: REASONS is a MappingProxyType, read-only at the type level."""
    from types import MappingProxyType

    assert isinstance(door_errors.REASONS, MappingProxyType)
    with pytest.raises(TypeError):
        door_errors.REASONS["not_found"] = door_errors.REASONS["not_found"]  # type: ignore[index]


def test_classify_table_override_is_a_private_kwarg() -> None:
    """F9: the injectable-table escape hatch is ``_table``, not a public ``table=``."""
    import inspect

    signature = inspect.signature(door_errors.classify)
    assert "table" not in signature.parameters
    assert "_table" in signature.parameters
    with pytest.raises(TypeError):
        door_errors.classify(NotFoundError("x"), table=door_errors.REASONS)  # type: ignore[call-arg]


def test_reasons_mcp_codes_are_disjoint_from_sdk_reserved_codes() -> None:
    """F1: the MCP SDK reserves -32000..-32019 for its OWN transport-level
    codes (mcp_types.jsonrpc.CONNECTION_CLOSED/REQUEST_TIMEOUT/HEADER_MISMATCH/
    MISSING_REQUIRED_CLIENT_CAPABILITY/UNSUPPORTED_PROTOCOL_VERSION/...). A
    relay reason code that collides is read as an SDK-internal signal by any
    client that discriminates by code alone, not by the relay's own typed
    ``reason`` (clio-agent's ``tools/mcp_errors.py`` does exactly that) --
    e.g. relay's own mcp_task_input_park_conflict, before this fix, shared
    -32001 with the SDK's own REQUEST_TIMEOUT and would have been retried
    with timeout semantics.
    """
    sdk_codes = {
        value
        for name, value in vars(mcp_types.jsonrpc).items()
        if name.isupper() and isinstance(value, int) and value < 0
    }
    assert sdk_codes, "sanity: mcp_types.jsonrpc must actually export negative int codes"
    # INVALID_PARAMS/INTERNAL_ERROR are the *standard* JSON-RPC reserved
    # codes (not SDK-internal ones) -- REASONS deliberately reuses them, the
    # same way the SDK itself does.
    reused_standard_codes = {mcp_types.INVALID_PARAMS, mcp_types.INTERNAL_ERROR}
    for spec in door_errors.REASONS.values():
        if spec.mcp_code in reused_standard_codes:
            continue
        assert spec.mcp_code not in sdk_codes, (
            f"{spec.reason}'s mcp_code {spec.mcp_code} collides with an SDK-reserved code"
        )


def test_storage_admission_refused_keeps_its_shipped_code_as_a_documented_exception() -> None:
    """-32007 is the one deliberate exception to the -32050..-32059
    reallocation: already shipped and pinned
    (tests/test_production_admin_surfaces.py's ``denied["error"]["code"] ==
    -32007``), so renumbering it is a separate, riskier change than moving
    the five never-shipped codes this same slice introduced.
    """
    assert door_errors.REASONS["storage_admission_refused"].mcp_code == -32007


def test_adapters_derive_their_codes_from_the_table_not_a_duplicate() -> None:
    """Perturbing a copy of REASONS moves both as_mcp_error and as_http_problem.

    Proves the adapters read ``mcp_code``/``http_status``/``retryable`` from
    whatever table classify() was given, rather than re-deriving them from
    some second, hand-duplicated mapping inside the adapters themselves.
    """
    perturbed = dict(door_errors.REASONS)
    original = perturbed["not_found"]
    perturbed["not_found"] = door_errors.ReasonSpec(
        reason="not_found",
        retryable=True,
        mcp_code=-99999,
        http_status=418,
        title="Perturbed not found",
    )

    fault = door_errors.classify(NotFoundError("missing"), _table=perturbed)
    assert fault.mcp_code == -99999
    assert fault.http_status == 418
    assert fault.retryable is True

    mcp_error = door_errors.as_mcp_error(fault)
    assert mcp_error.code == -99999

    problem = door_errors.as_http_problem(fault)
    assert problem["status"] == 418
    assert problem["retryable"] is True
    assert problem["title"] == "Perturbed not found"

    # Sanity: the untouched table still produces the original values.
    baseline_fault = door_errors.classify(NotFoundError("missing"))
    assert baseline_fault.mcp_code == original.mcp_code
    assert baseline_fault.http_status == original.http_status


# --------------------------------------------------------------------------- #
# classify() dispatch rules
# --------------------------------------------------------------------------- #


def _storage_decision(reason: StorageReason) -> StorageDecision:
    return StorageDecision(allowed=False, reason=reason, message="storage refused")


@pytest.mark.parametrize(
    ("exc", "expected_reason"),
    [
        (
            TaskInputParkConflictError("private CAS internals 7a"),
            "mcp_task_input_park_conflict",
        ),
        (NotFoundError("missing"), "not_found"),
        (ConfigurationError("bad config"), "configuration_error"),
        (
            StorageAdmissionError(_storage_decision(StorageReason.CORE_HIGH_WATER)),
            "storage_admission_refused",
        ),
        (
            StorageRuntimeViolation(_storage_decision(StorageReason.SPOOL_HIGH_WATER)),
            "storage_safety_violation",
        ),
        (ObservationTimeoutError("timed out"), "observation_timeout"),
        (
            OwnerSessionIdentityError(
                code="owner_session_identity_required",
                job_kind=JobKind.JARVIS,
                message="owner-session identity headers required",
            ),
            "owner_session_identity_refused",
        ),
    ],
)
def test_classify_type_dispatch_for_each_of_the_seven_typed_reasons(
    exc: BaseException,
    expected_reason: str,
) -> None:
    """The seven exception types with an unambiguous 1:1 reason (doc §6.3)."""
    fault = door_errors.classify(exc)
    assert fault.reason == expected_reason
    spec = door_errors.REASONS[expected_reason]
    expected_message = (
        exc.public_message if isinstance(exc, PublicMessageError) else f"{spec.title}."
    )
    assert fault.message == expected_message
    if not isinstance(exc, PublicMessageError):
        assert str(exc) not in fault.message
    assert fault.retryable == spec.retryable
    assert fault.mcp_code == spec.mcp_code
    assert fault.http_status == spec.http_status


def test_classify_carries_storage_decision_as_extension_data() -> None:
    decision = _storage_decision(StorageReason.PER_JOB_LIMIT)
    fault = door_errors.classify(StorageAdmissionError(decision))
    assert fault.data == {"storage_decision": decision.to_dict()}


def test_classify_carries_owner_session_identity_detail_as_extension_data() -> None:
    exc = OwnerSessionIdentityError(
        code="owner_session_identity_invalid",
        job_kind=None,
        message="owner-session identity headers are invalid",
    )
    fault = door_errors.classify(exc)
    assert fault.data == {key: value for key, value in exc.detail.items() if key != "message"}
    assert fault.data["code"] == "owner_session_identity_invalid"
    assert str(exc) not in json.dumps(fault.data)


def test_classify_jarvis_dispatch_refusal_is_an_object_entry_point() -> None:
    """JarvisDispatchRefusal is a durable-result dataclass, never raised-and-caught.

    F11: this reason is declared in the frozen table but not yet emitted by
    any production call site -- no code today constructs a durable
    ``jarvis_run`` result and hands its refusal to ``classify()``. This test
    (and the vocabulary itself) is forward-declared contract, not a
    regression proof of live behavior.
    """
    refusal = JarvisDispatchRefusal(
        code="jarvis_tool_error",
        message="the pipeline refused to dispatch",
        pipeline_id="pipeline-1",
        execution_id="execution-1",
        payload_schema_version="jarvis.error.v1",
    )
    fault = door_errors.classify(refusal)
    assert fault.reason == "jarvis_dispatch_refused"
    assert fault.message == "JARVIS dispatch refused."
    assert refusal.message not in fault.message
    assert fault.data == {
        "code": "jarvis_tool_error",
        "pipeline_id": "pipeline-1",
        "execution_id": "execution-1",
    }
    assert fault.retryable is False


def test_classify_reason_override_is_call_path_scoped_not_type_based() -> None:
    """A bare QueueConflictError means mcp_task_conflict only when the call
    site says so -- the same type is raised 651 other times in
    core_queue.py for unrelated invariants (doc §6.3), so type-based
    dispatch alone would over-match.
    """
    # No override: a bare QueueConflictError is not one of the seven typed
    # reasons, so it correctly falls through to internal_error.
    default_fault = door_errors.classify(QueueConflictError("unrelated invariant"))
    assert default_fault.reason == "internal_error"

    scoped_fault = door_errors.classify(
        QueueConflictError("MCP task identity was reused"),
        reason="mcp_task_conflict",
    )
    assert scoped_fault.reason == "mcp_task_conflict"
    spec = door_errors.REASONS["mcp_task_conflict"]
    assert scoped_fault.mcp_code == spec.mcp_code
    assert scoped_fault.http_status == spec.http_status


def test_classify_unregistered_reason_override_is_a_loud_caller_bug() -> None:
    """Frozen-set discipline: an unregistered reason is a caller bug, not a
    silent fallback to some default.
    """
    with pytest.raises(ValueError, match="not a registered REASONS entry"):
        door_errors.classify(ValueError("boom"), reason="not_a_real_reason")


def test_classify_internal_error_fallback_never_leaks_str_exc(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The sabotage twin at the classify() level: a novel, unclassified
    exception type falls back to internal_error, its traceback is logged
    once server-side, and the underlying exception's own text never
    reaches ``message``.
    """

    class _NovelSurpriseError(RuntimeError):
        pass

    with caplog.at_level("ERROR", logger="clio_relay.door_errors"):
        try:
            raise _NovelSurpriseError("a distinctly identifiable internal detail 8f2c1a")
        except _NovelSurpriseError as exc:
            fault = door_errors.classify(exc)

    assert fault.reason == "internal_error"
    assert fault.retryable is False
    assert fault.http_status == 500
    assert fault.mcp_code == mcp_types.INTERNAL_ERROR
    assert "8f2c1a" not in fault.message
    assert fault.data == {}

    logged = [
        record
        for record in caplog.records
        if record.name == "clio_relay.door_errors" and record.exc_info is not None
    ]
    assert len(logged) == 1
    exc_info = logged[0].exc_info
    assert exc_info is not None
    assert exc_info[0] is _NovelSurpriseError


def test_classify_guards_a_hostile_str_and_never_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F5: a raising ``__str__`` must never escape classify(). On the HTTP
    surface an unguarded ``str(exc)`` would collapse straight into
    Starlette's ``ServerErrorMiddleware``, replacing the original exception
    with a new, undiagnosable one and losing the typed response entirely.
    """

    class _HostileConfigurationError(ConfigurationError):
        def __str__(self) -> str:
            raise RuntimeError("hostile __str__ payload -- must never propagate")

    with caplog.at_level("ERROR", logger="clio_relay.door_errors"):
        fault = door_errors.classify(_HostileConfigurationError("irrelevant"))

    assert fault.reason == "configuration_error"
    assert fault.message == "Configuration error."
    assert "hostile __str__ payload" not in fault.message

    logged = [
        record
        for record in caplog.records
        if record.name == "clio_relay.door_errors" and record.exc_info is not None
    ]
    assert len(logged) == 1
    exc_info = logged[0].exc_info
    assert exc_info is not None
    assert exc_info[0] is RuntimeError


def test_classify_guards_a_hostile_typed_data_extractor() -> None:
    """F5: a StorageRuntimeError whose ``.decision.to_dict()`` raises must
    degrade to an empty extension payload, not crash classification.

    ``StorageRuntimeError.__init__`` itself already calls ``to_dict()`` once
    (to build its own base ``RuntimeError`` message) before this exception
    even exists -- only the SECOND call, the one ``_typed_data`` makes, is
    made hostile, so construction succeeds and ``classify()`` is what gets
    exercised.
    """
    call_count = {"n": 0}

    class _HostileStorageDecision(StorageDecision):
        def to_dict(self) -> dict[str, object]:
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise RuntimeError("to_dict() access exploded")
            return super().to_dict()

    hostile_decision = _HostileStorageDecision(
        allowed=False,
        reason=StorageReason.CORE_HIGH_WATER,
        message="storage refused",
    )
    exc = StorageAdmissionError(hostile_decision)
    fault = door_errors.classify(exc)
    assert fault.reason == "storage_admission_refused"
    assert fault.data == {}


def test_message_is_hard_truncated_to_the_t1_budget_with_a_truthful_record() -> None:
    """F2: _bounded_text must never claim ``truncation: null`` on a
    document it actually cut -- a 50k-char message is bounded to 2,000
    chars AND carries a populated, byte-accurate truncation record.
    """
    oversized = "x" * 50_000
    fault = door_errors.classify(ConfigurationError("logged only"), message=oversized)
    assert len(fault.message) == door_errors.MAX_MESSAGE_CHARS
    assert fault.truncation is not None
    assert fault.truncation["schema_version"] == door_errors.TRUNCATION_SCHEMA_VERSION
    assert fault.truncation["truncated"] is True
    assert fault.truncation["retention"] == "head"
    assert fault.truncation["original_bytes"] == 50_000
    assert fault.truncation["retained_head_bytes"] == door_errors.MAX_MESSAGE_CHARS
    assert fault.truncation["elided_bytes"] == 50_000 - door_errors.MAX_MESSAGE_CHARS

    document = door_errors.as_http_problem(fault)
    assert document["truncation"] is not None
    assert document["truncation"]["truncated"] is True


def test_message_under_the_t1_budget_has_no_truncation_record() -> None:
    fault = door_errors.classify(ConfigurationError("short and unremarkable"))
    assert fault.truncation is None
    document = door_errors.as_http_problem(fault)
    assert document["truncation"] is None


# --------------------------------------------------------------------------- #
# as_mcp_error / as_http_problem / as_browser_gateway_error
# --------------------------------------------------------------------------- #


def test_as_mcp_error_data_merges_reason_and_extension_payload() -> None:
    decision = _storage_decision(StorageReason.TOTAL_HIGH_WATER)
    fault = door_errors.classify(StorageAdmissionError(decision))
    mcp_error = door_errors.as_mcp_error(fault)
    assert isinstance(mcp_error, MCPError)
    assert mcp_error.code == -32007
    assert mcp_error.data == {
        "reason": "storage_admission_refused",
        "storage_decision": decision.to_dict(),
    }


def test_as_mcp_error_reason_cannot_be_shadowed_by_a_colliding_data_key() -> None:
    """F4: contract members always win -- a caller-supplied ``data={"reason": ...}``
    must never overwrite the real, classified reason.
    """
    fault = door_errors.classify(
        ConfigurationError("x"),
        data={"reason": "an attacker-controlled string"},
    )
    mcp_error = door_errors.as_mcp_error(fault)
    assert mcp_error.data["reason"] == "configuration_error"


def test_as_http_problem_matches_the_doc_worked_example_shape() -> None:
    fault = door_errors.classify(
        TaskInputParkConflictError("the task's input round could not be admitted after CAS retries")
    )
    document = door_errors.as_http_problem(fault)
    assert document["type"] == "urn:clio-relay:error:mcp_task_input_park_conflict"
    assert document["title"] == "MCP task input park conflict"
    assert document["status"] == 409
    assert document["detail"] == fault.message
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert document["reason"] == "mcp_task_input_park_conflict"
    assert document["retryable"] is True
    assert document["truncation"] is None


def test_as_http_problem_contract_members_cannot_be_shadowed_by_data() -> None:
    """F4: build the envelope as {**fault.data, <contract members>} so a
    colliding data key (status/reason/type/...) can never overwrite the
    real classified value.
    """
    fault = door_errors.classify(
        ConfigurationError("x"),
        data={
            "status": 999,
            "reason": "an attacker-controlled string",
            "type": "urn:clio-relay:error:not-the-real-type",
            "retryable": "not-a-bool",
        },
    )
    document = door_errors.as_http_problem(fault)
    assert document["status"] == 400
    assert document["reason"] == "configuration_error"
    assert document["type"] == "urn:clio-relay:error:configuration_error"
    assert document["retryable"] is False


def test_as_http_problem_drops_evidence_first_when_over_budget() -> None:
    """The ≤8KiB drop order (doc §6.4): evidence is dropped first -- the RFC
    7807 core four and reason/retryable are never dropped.
    """
    fault = door_errors.classify(
        ConfigurationError("oversized"),
        data={"evidence": {"artifact_id": "x" * 9000}},
    )
    document = door_errors.as_http_problem(fault)
    encoded = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    assert "evidence" not in document
    assert len(encoded) <= door_errors.MAX_ENVELOPE_BYTES
    for required in ("type", "title", "status", "detail", "schema_version", "reason", "retryable"):
        assert required in document


def test_as_http_problem_drops_any_oversized_extension_member_not_just_evidence() -> None:
    """F3: a single oversized extension member that is not literally named
    "evidence" must not sail through unbounded -- the exact gap the
    original ≤8KiB enforcement missed (an injected 9KiB member reached the
    wire at 11,213 bytes before this fix).
    """
    fault = door_errors.classify(
        ConfigurationError("oversized"),
        data={"evidence": {"a": "z" * 9000}, "an_unnamed_extension_member": "z" * 9000},
    )
    document = door_errors.as_http_problem(fault)
    encoded = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    assert len(encoded) <= door_errors.MAX_ENVELOPE_BYTES
    assert "evidence" not in document
    assert "an_unnamed_extension_member" not in document
    for required in ("type", "title", "status", "detail", "schema_version", "reason", "retryable"):
        assert required in document


def test_as_http_problem_truncates_detail_and_flags_overflow_when_core_alone_is_oversized() -> None:
    """F3: once every extension member is gone, an oversized ``detail`` (a
    T1-truncated message built from 4-byte UTF-8 characters can still
    exceed the ≤8KiB budget on its own) is truncated further and the
    document is stamped ``envelope_overflow: true`` -- never a silent
    pass-through of an over-budget document.
    """
    message = "\U0001f600" * 2_500  # 4-byte-per-char emoji; truncates to 2,000 chars = 8,000 bytes
    fault = door_errors.classify(ConfigurationError("logged only"), message=message)
    assert fault.truncation is not None  # T1 already cut this at the char level

    document = door_errors.as_http_problem(fault)
    encoded = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    assert len(encoded) <= door_errors.MAX_ENVELOPE_BYTES
    assert document.get("envelope_overflow") is True
    assert document["detail"]  # never reduced to an empty string
    for required in ("type", "title", "status", "schema_version", "reason", "retryable"):
        assert required in document


def test_as_http_problem_measures_bytes_with_ensure_ascii_false() -> None:
    """F10: the budget must reflect the actual wire encoding. A
    default-``ensure_ascii=True`` measurement inflates every non-ASCII
    character to a 6-byte ``\\uXXXX`` escape, materially overstating cost
    for ordinary non-ASCII text.
    """
    fault = door_errors.classify(
        ConfigurationError("logged only"),
        message="café " * 50,
    )
    document = door_errors.as_http_problem(fault)
    honest = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    inflated = json.dumps(document, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    assert len(honest) < len(inflated)
    assert len(honest) <= door_errors.MAX_ENVELOPE_BYTES


def test_as_browser_gateway_error_maps_onto_the_two_arg_shape() -> None:
    fault = door_errors.classify(ConfigurationError("bad body"), reason="configuration_error")
    status, document = door_errors.as_browser_gateway_error(fault)
    assert status == fault.http_status == 400
    assert document == door_errors.as_http_problem(fault)
    assert "error" not in document  # not the old bare {"error": message} shape


@pytest.mark.parametrize(
    "reason",
    [
        "websocket_authentication_failed",
        "websocket_session_binding_failed",
        "websocket_page_limit_invalid",
        "websocket_poll_interval_invalid",
        "websocket_cursor_invalid",
        "websocket_resource_ownership_refused",
        "websocket_resource_not_found",
    ],
)
def test_websocket_refusal_adapter_preserves_policy_code_and_reason(reason: str) -> None:
    refusal = door_errors.websocket_refusal(reason)
    assert refusal.code == 1008
    assert refusal.reason == reason
    assert len(refusal.reason.encode("utf-8")) <= 123


# --------------------------------------------------------------------------- #
# fastmcp_server.py wiring: the live hole + #218/#215 re-pointed + MCPError pass-through
# --------------------------------------------------------------------------- #


def _fastmcp_task_server(
    settings: RelaySettings,
    queue: ClioCoreQueue,
) -> tuple[FastMCP[dict[str, Any]], RelayMcpRuntime, RelayTool]:
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(
        profile="user", registry_path=settings.cluster_registry_path
    )
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    tool = RelayTool(definition, runtime=runtime, catalog_revision=None, task_capable=True)
    server: FastMCP[dict[str, Any]] = FastMCP(
        "door-errors-fastmcp-test",
        tools=[tool],
        lifespan=runtime.lifespan,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))
    return server, runtime, tool


def test_task_input_park_conflict_is_typed_not_bare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live hole (doc §6.1, fastmcp_server.py:1106-1115): before R3,
    ``intercept_tool_call`` left ``TaskInputParkConflictError`` deliberately
    unwrapped, so it escaped through FastMCP's generic handler as a bare
    internal error. Patching the runtime's own raise site proves it is now
    typed via door_errors instead.
    """
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        cluster_registry_path=tmp_path / "cluster-registry" / "clusters.json",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server, runtime, _tool = _fastmcp_task_server(settings, queue)

    async def always_park_conflict(*_args: Any, **_kwargs: Any) -> Any:
        raise TaskInputParkConflictError("forced park conflict for the door_errors live-hole test")

    monkeypatch.setattr(runtime, "_park_agent_input", always_park_conflict)

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            arguments = {
                "cluster": "test-cluster",
                "prompt_path": str(tmp_path / "prompt.md"),
                "timeout_seconds": 45,
                "idempotency_key": "door-errors-park-conflict",
                "request_followup_message": True,
            }
            with pytest.raises(MCPError) as failure:
                await call_tool_task(client, "relay_submit_agent", arguments)
            spec = door_errors.REASONS["mcp_task_input_park_conflict"]
            assert failure.value.code == spec.mcp_code
            assert failure.value.code != mcp_types.INTERNAL_ERROR
            assert failure.value.code != mcp_types.INVALID_PARAMS
            assert spec.retryable is True
            assert failure.value.data == {"reason": "mcp_task_input_park_conflict"}
            assert failure.value.message == "MCP task input park conflict."
            assert "forced park conflict" not in failure.value.message

    asyncio.run(scenario())


def test_218_regression_mcp_task_conflict_reason_comes_from_the_table() -> None:
    """#218 re-pointed at door_errors: a genuine task-identity conflict is
    INVALID_PARAMS with the frozen table's ``mcp_task_conflict`` reason --
    not the ad hoc ``mcp_task_identity_conflict`` string the site used to
    invent locally (the full wire proof lives in test_fastmcp_server.py;
    this proves door_errors' own contract for the identical scenario).
    """
    fault = door_errors.classify(
        QueueConflictError("MCP task identity was reused with different semantics: job-1"),
        reason="mcp_task_conflict",
        data={"task_id": "job-1"},
    )
    mcp_error = door_errors.as_mcp_error(fault)
    assert mcp_error.code == mcp_types.INVALID_PARAMS
    assert mcp_error.data == {"reason": "mcp_task_conflict", "task_id": "job-1"}
    assert mcp_error.message == "MCP task conflict."
    assert "different semantics" not in mcp_error.message


def test_215_regression_reconciliation_failure_never_leaks_str_exc() -> None:
    """#215 re-pointed at door_errors: the reconciliation-failure reason's
    message is always the fixed, handler-internals-free string -- never the
    underlying exception's own text, in either ``message`` or ``data`` (the
    full wire proof, including the logger.exception assertion, lives in
    test_fastmcp_server.py).
    """
    distinctive = "a distinctly identifiable underlying failure detail 71cbe4"
    fault = door_errors.classify(
        RuntimeError(distinctive),
        reason="mcp_task_status_reconciliation_failed",
        message="relay could not reconcile this task's status.",
        data={"task_id": "job-2"},
    )
    mcp_error = door_errors.as_mcp_error(fault)
    assert mcp_error.message == "relay could not reconcile this task's status."
    assert distinctive not in mcp_error.message
    assert distinctive not in str(mcp_error.data)
    assert mcp_error.data == {
        "reason": "mcp_task_status_reconciliation_failed",
        "task_id": "job-2",
    }


def test_mcp_error_pass_through_is_never_reclassified_by_the_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression pin: ``_handle_get``'s ``except MCPError: raise`` must
    stay. ``door_errors`` has no dedicated reason for an already-typed
    ``MCPError`` -- it is not one of the seven typed reasons dispatched by
    ``isinstance``, so handing one directly to ``classify()`` collapses it
    into the generic ``internal_error`` bucket, destroying its original
    code and data. If the pass-through clause were ever removed, an
    ``MCPError`` raised inside ``task_status`` (e.g. from a nested call)
    would fall into the broader ``except Exception`` and be silently
    re-wrapped.
    """
    original = MCPError(
        code=mcp_types.URL_ELICITATION_REQUIRED,
        message="a nested, already-typed failure",
        data={"reason": "url_elicitation_required"},
    )

    # Ground truth: classify() alone would destroy this if ever reached.
    reclassified = door_errors.classify(original)
    assert reclassified.reason == "internal_error"
    assert reclassified.mcp_code != original.code

    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        cluster_registry_path=tmp_path / "cluster-registry" / "clusters.json",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server, runtime, tool = _fastmcp_task_server(settings, queue)

    async def raise_original(*_args: Any, **_kwargs: Any) -> Any:
        raise original

    async def scenario() -> None:
        saved = await runtime.create_task(
            tool=tool,
            arguments={"value": "mcp-error-pass-through"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="queued")],
                structured_content={
                    "job_id": "job-mcp-error-pass-through",
                    "state": JobState.QUEUED.value,
                    "terminal": False,
                },
            ),
        )
        assert saved is not None
        monkeypatch.setattr(runtime, "task_status", raise_original)

        async with Client(server, mode="auto") as client:
            with pytest.raises(MCPError) as failure:
                await client.session.send_request(
                    GetTaskRequest(params=GetTaskRequestParams(task_id=saved.task_id)),
                    ClientGetTaskResult,
                )
            assert failure.value.code == original.code
            assert failure.value.data == original.data
            assert failure.value.message == original.message

    asyncio.run(scenario())


def test_public_error_constructors_never_interpolate_a_caught_exception() -> None:
    """Foreign caught text must be logged, never embedded in a marked source message."""
    source_root = Path(__file__).parents[1] / "src" / "clio_relay"
    offenders: list[str] = []
    for path in source_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for handler in (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ExceptHandler) and node.name is not None
        ):
            scope = ast.Module(body=handler.body, type_ignores=[])
            for call in (node for node in ast.walk(scope) if isinstance(node, ast.Call)):
                constructor = call.func.id if isinstance(call.func, ast.Name) else ""
                if constructor not in {"QueueConflictError", "RelayAuthoredError"}:
                    continue
                if any(
                    isinstance(argument, ast.JoinedStr)
                    and any(
                        isinstance(part, ast.FormattedValue)
                        and isinstance(part.value, ast.Name)
                        and part.value.id == handler.name
                        for part in argument.values
                    )
                    for argument in call.args
                ):
                    offenders.append(f"{path.relative_to(source_root)}:{call.lineno}")
    assert offenders == []


# --------------------------------------------------------------------------- #
# http_api.py wiring: the global exception handler
# --------------------------------------------------------------------------- #

# split/http-api-w3 (iowarp/clio-relay#231): http_api.py's ~1900-line
# create_app() (every route body as a nested closure) is now a 165-line
# facade over a shared RelayApiContext (http_api_context.py) plus the
# bearer-token/owner-session dependency factories (http_api_auth.py) and six
# route owner modules -- the code the three tests below count (door_errors.
# http_problem raise sites, the middleware refusal sites) moved WITH the
# routes, unchanged. Same call sites, same counts, different files: these
# tests now scan across the full split module set instead of the single
# pre-split file.
_HTTP_API_SPLIT_MODULES = (
    "http_api.py",
    "http_api_auth.py",
    "http_api_context.py",
    "http_api_routes_session.py",
    "http_api_routes_jobs.py",
    "http_api_routes_events.py",
    "http_api_routes_artifacts.py",
    "http_api_routes_gateway.py",
    "http_api_routes_queue.py",
    # clio-relay#179 dial burn-down: three new owned-session-channel route
    # modules (review M5) -- ten more door_errors.http_problem(...) sites
    # (2 in owner-session-admin, 2 in worker-probe, 6 in scheduler,
    # including S1(b)'s server-side ownership gate).
    "http_api_routes_owner_session_admin.py",
    "http_api_routes_scheduler.py",
    "http_api_routes_worker_probe.py",
)


def _http_api_split_sources() -> list[tuple[str, str]]:
    root = Path(__file__).parents[1] / "src" / "clio_relay"
    return [(name, (root / name).read_text(encoding="utf-8")) for name in _HTTP_API_SPLIT_MODULES]


def test_http_api_rewrites_exactly_138_deliberate_sites_through_registered_reasons() -> None:
    """The R9 inventory is closed: 123 raises plus 15 middleware refusals.

    The middleware refusal count is sourced from http_api_middleware.py
    alone: InputArtifactBodyLimitMiddleware moved there as one atomic,
    unsplit unit (clio-relay#231), so its three refusal-counting functions
    still all live in the one file the pre-split test already walked.

    112 (was 107): clio-relay#221/#259's SSE log-tail route
    (http_api_routes_artifacts.py) adds 5 sites across its two commits --
    poll_interval_invalid, log_stream_invalid, job_not_found (the route
    itself), plus log_offset_invalid and log_offset_beyond_eof (the
    adversarial-review D3/D5 fixes).

    113 (was 112): clio-relay#278's new execution-scoped artifact-listing
    route (``GET /executions/{execution_id}/artifacts``,
    http_api_routes_artifacts.py) adds exactly ONE site --
    ``execution_not_found``, the route's own resolution failure.
    Adversarial-review D1/D2 fix: resolution is ownership-filtered BEFORE
    its exactly-one-owner check, so "unknown" and "resolves to a job this
    session cannot see" collapse into the SAME ``execution_not_found``
    refusal -- there is no second, job_not_found branch here (the route's
    own ``ctx.require_owned_job`` call below stays unguarded, exactly like
    the sibling ``GET /jobs/{job_id}/artifacts`` route, so its own residual
    failures flow through the generic exception path rather than a bespoke
    ``door_errors.http_problem(...)`` call this test would count).

    123 (was 113): clio-relay#179 dial burn-down (review M5): three new
    owned-session-channel route modules add 10 more raise sites -- see
    ``_HTTP_API_SPLIT_MODULES``'s own comment for the per-module breakdown.
    """
    calls: list[ast.Call] = []
    for _name, source in _http_api_split_sources():
        tree = ast.parse(source)
        calls.extend(
            node.exc
            for node in ast.walk(tree)
            if isinstance(node, ast.Raise)
            and isinstance(node.exc, ast.Call)
            and isinstance(node.exc.func, ast.Attribute)
            and isinstance(node.exc.func.value, ast.Name)
            and node.exc.func.value.id == "door_errors"
            and node.exc.func.attr == "http_problem"
        )
        assert not any(
            isinstance(node, ast.Raise)
            and isinstance(node.exc, ast.Call)
            and isinstance(node.exc.func, ast.Name)
            and node.exc.func.id in {"HTTPException", "StarletteHTTPException"}
            for node in ast.walk(tree)
        )
        assert 'json.dumps({"detail"' not in source

    assert len(calls) == 123
    reasons = {
        call.args[0].value for call in calls if call.args and isinstance(call.args[0], ast.Constant)
    }
    assert len(reasons) > 1
    assert reasons <= set(door_errors.REASONS)
    assert all(
        call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str)
        for call in calls
    )

    middleware_source = (
        Path(__file__).parents[1] / "src" / "clio_relay" / "http_api_middleware.py"
    ).read_text(encoding="utf-8")
    middleware_tree = ast.parse(middleware_source)
    functions = {
        node.name: node
        for node in ast.walk(middleware_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    middleware_direct = sum(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_send_error"
        and len(node.args) > 1
        and isinstance(node.args[1], ast.Constant)
        for function_name in ("__call__", "_buffer_and_dispatch")
        for node in ast.walk(functions[function_name])
    )
    middleware_too_large = sum(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_send_too_large"
        for node in ast.walk(functions["_buffer_and_dispatch"])
    )
    middleware_authentication = sum(
        isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple)
        for node in ast.walk(functions["_authentication_error"])
    )
    assert middleware_direct + middleware_too_large + middleware_authentication == 15
    assert len(calls) + middleware_direct + middleware_too_large + middleware_authentication == 138


def test_every_registered_reason_is_a_served_error_v1_document(tmp_path: Path) -> None:
    """Every frozen reason survives the real FastAPI exception-handler boundary."""
    app = create_app(RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"))

    def route_for(reason: str) -> Any:
        def route() -> None:
            raise door_errors.http_problem(reason, f"deliberate {reason} probe")

        return route

    for reason in door_errors.REASONS:
        app.add_api_route(f"/__door_reason/{reason}", route_for(reason), methods=["GET"])

    client = cast(Any, TestClient(app))
    for reason, spec in door_errors.REASONS.items():
        response = client.get(f"/__door_reason/{reason}")
        document = response.json()

        assert response.status_code == spec.http_status
        assert response.headers["content-type"].startswith("application/problem+json")
        assert document["schema_version"] == door_errors.SCHEMA_VERSION
        assert document["reason"] == reason
        assert document["status"] == spec.http_status
        assert document["retryable"] is spec.retryable
        assert document["type"] == f"urn:clio-relay:error:{reason}"
        assert isinstance(document["detail"], str)
        assert len(document["detail"]) <= door_errors.MAX_MESSAGE_CHARS
        assert len(json.dumps(document, ensure_ascii=False).encode("utf-8")) <= 8 * 1024


def test_all_63_exception_backed_http_sites_use_stable_public_messages() -> None:
    """Every migrated ``exc=``-only site rejects raw exception text as wire detail.

    clio-relay#242 actionability audit: 2 of the original 58 sites
    (``mcp_submission_conflict`` on ``submit_mcp_call``,
    ``job_submission_conflict`` on ``submit_owned``) deliberately opted OUT
    of this closed set -- an agent meeting either refusal needs the raw
    conflict/mismatch detail PLUS an authored what-to-do-next tail (the
    conflicting idempotency_key and the retry-with-a-new-key move; the
    refresh-discovery move), so they now pass an explicit, reviewed
    ``message=`` instead of relying on the generic reason title. 56 (now 57
    with clio-relay#221/#259's ``get_log_sse`` -> ``job_not_found`` site, and
    58 with clio-relay#278's execution-scoped listing route's own
    ``execution_not_found`` site -- exactly one, per the D1/D2 ownership-
    filtered-before-count-check fix's honest refusal shape: there is no
    separate ``job_not_found`` site on this route to add a second one)
    keep the closed-set discipline this test proves.

    clio-relay#179 dial burn-down: 5 more ``exc=``-only sites (2 in
    ``http_api_routes_worker_probe.py``, 3 in ``http_api_routes_
    scheduler.py``, each a ``ConfigurationError`` re-raised as
    ``configuration_error``) join the closed set unchanged: 58 -> 63.
    """
    calls: list[ast.Call] = []
    for _name, source in _http_api_split_sources():
        tree = ast.parse(source)
        calls.extend(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "http_problem"
            and any(keyword.arg == "exc" for keyword in node.keywords)
            and len(node.args) == 1
            and not any(keyword.arg == "message" for keyword in node.keywords)
        )
    assert len(calls) == 63

    for index, call in enumerate(calls):
        reason = ast.literal_eval(call.args[0])
        distinctive = f"private exception detail {index:02d} 6d71aa"
        error = door_errors.http_problem(reason, exc=RuntimeError(distinctive))
        document = door_errors.as_http_problem(error.fault)

        assert document["detail"] == f"{door_errors.REASONS[reason].title}."
        assert distinctive not in json.dumps(document)


def test_session_binding_course_corrections_are_distinct_at_the_five_sites() -> None:
    calls = [
        node
        for _name, source in _http_api_split_sources()
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "http_problem"
        and len(node.args) >= 2
        and all(isinstance(argument, ast.Constant) for argument in node.args[:2])
    ]
    reason_by_detail = {
        cast(str, cast(ast.Constant, call.args[1]).value): cast(
            str, cast(ast.Constant, call.args[0]).value
        )
        for call in calls
    }
    assert {
        detail: reason_by_detail[detail]
        for detail in (
            "relay session has no exact generation identity",
            "relay session generation is not open for new work",
            "exact owner session and generation headers are required",
            "owner session or generation does not match this API process",
            "relay API is not bound to an owner session",
        )
    } == {
        "relay session has no exact generation identity": (
            "session_generation_identity_unavailable"
        ),
        "relay session generation is not open for new work": "session_intake_closed",
        "exact owner session and generation headers are required": (
            "session_binding_headers_required"
        ),
        "owner session or generation does not match this API process": (
            "session_binding_identity_mismatch"
        ),
        "relay API is not bound to an owner session": "unbound_session_api",
    }


def test_launcher_resolution_failed_adapter_contract_on_a_synthetic_route() -> None:
    """Typed launcher prose is public while foreign text stays generic."""
    app = FastAPI()

    @app.get("/probe")
    def probe(authored: bool) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        try:
            raise ValueError("receipt-bound clio-kit runtime identity did not verify")
        except ValueError as exc:
            classified = door_errors.public_message_error(exc) if authored else exc
            fault = door_errors.classify(classified, reason="launcher_resolution_failed")
            return JSONResponse(
                door_errors.as_http_problem(fault),
                status_code=fault.http_status,
                media_type="application/problem+json",
            )

    client = TestClient(app)
    authored_response = client.get("/probe", params={"authored": "true"})
    foreign_response = client.get("/probe", params={"authored": "false"})

    assert authored_response.status_code == 409
    document = authored_response.json()
    assert document["status"] == 409
    assert document["reason"] == "launcher_resolution_failed"
    assert document["retryable"] is False
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert document["type"] == "urn:clio-relay:error:launcher_resolution_failed"
    assert document["detail"] == "receipt-bound clio-kit runtime identity did not verify"

    assert foreign_response.status_code == 409
    foreign_document = foreign_response.json()
    assert foreign_document["detail"] == "Launcher resolution failed."
    assert "did not verify" not in foreign_document["detail"]


def test_sabotage_twin_novel_exception_via_testclient_returns_typed_internal_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The global exception handler wired into ``create_app`` (doc §3's "0
    unclassified exceptions reach the wire") -- an injected route raising a
    type door_errors has never seen still produces a typed internal_error
    document, with the traceback logged once server-side and never on the wire.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    app = create_app(settings)

    class _InjectedSabotageError(RuntimeError):
        pass

    @app.get("/__sabotage_twin_9f4b1c")
    def sabotage() -> None:  # pyright: ignore[reportUnusedFunction]
        raise _InjectedSabotageError("a distinctly identifiable sabotage-twin detail 9f4b1c")

    # Starlette's ServerErrorMiddleware always re-raises after invoking an
    # Exception-class handler (by design, so a real ASGI server can still log
    # it) -- the response is genuinely sent to the client either way; only
    # TestClient's default raise_server_exceptions=True would otherwise
    # surface that re-raise as a test failure instead of a response to assert on.
    client = cast(Any, TestClient(app, raise_server_exceptions=False))
    with caplog.at_level("ERROR", logger="clio_relay.door_errors"):
        response = client.get("/__sabotage_twin_9f4b1c")

    assert response.status_code == 500
    document = response.json()
    assert document["reason"] == "internal_error"
    assert document["retryable"] is False
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert "9f4b1c" not in response.text

    logged = [
        record
        for record in caplog.records
        if record.name == "clio_relay.door_errors" and record.exc_info is not None
    ]
    assert len(logged) == 1
    exc_info = logged[0].exc_info
    assert exc_info is not None
    assert exc_info[0] is _InjectedSabotageError


def test_handler_survives_even_when_door_errors_itself_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F5: the handler body itself is guarded, independent of classify()'s
    own str(exc)/data guards -- if door_errors somehow still fails (a
    defect in door_errors.py, not merely a hostile exception it already
    handles), the handler must fall back to the hardcoded internal_error
    document rather than let a second exception replace the first.
    """
    import clio_relay.http_api as http_api_module

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    app = create_app(settings)

    @app.get("/__handler_guard_probe")
    def probe() -> None:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("anything -- classify() itself will be made to fail")

    def broken_classify(*_args: Any, **_kwargs: Any) -> Any:
        raise TypeError("simulated door_errors defect")

    monkeypatch.setattr(http_api_module.door_errors, "classify", broken_classify)

    client = cast(Any, TestClient(app, raise_server_exceptions=False))
    with caplog.at_level("ERROR", logger="clio_relay.http_api"):
        response = client.get("/__handler_guard_probe")

    assert response.status_code == 500
    document = response.json()
    assert document["reason"] == "internal_error"
    assert document["schema_version"] == door_errors.SCHEMA_VERSION

    logged = [
        record
        for record in caplog.records
        if record.name == "clio_relay.http_api" and record.exc_info is not None
    ]
    assert len(logged) == 1


def test_deliberate_route_failure_uses_specific_handler_before_global_fallback(
    tmp_path: Path,
) -> None:
    """A migrated route serves its specific reason instead of internal_error."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    app = create_app(settings)
    client = cast(Any, TestClient(app))

    response = client.get("/jobs/does-not-exist/monitor")

    assert response.status_code == 404
    body = response.json()
    assert body["schema_version"] == door_errors.SCHEMA_VERSION
    assert body["reason"] == "job_not_found"
    assert body["status"] == 404


# --------------------------------------------------------------------------- #
# browser_gateway.py wiring: the fourth adapter
# --------------------------------------------------------------------------- #


class _OversizedBodyBackendHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        payload = b'{"accepted":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        del format, args


@contextmanager
def _backend_server() -> Any:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OversizedBodyBackendHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _bind_capability_proxy_server(
    config: BrowserGatewayConfig,
    capability: str,
    *,
    attempts: int = 5,
) -> CapabilityProxyServer:
    """Construct ``CapabilityProxyServer``, retrying on a TOCTOU port race.

    ``BrowserGatewayConfig.bind_port`` requires ``gt=0`` (Pydantic), so the
    server cannot bind directly to an OS-assigned ephemeral port the way
    ``ThreadingHTTPServer(("127.0.0.1", 0), ...)`` does elsewhere in this
    file -- ``_free_port()`` must reserve a number, close it, and hand it
    back, leaving a real window another process could grab it in between.
    Retry-on-EADDRINUSE (F13, opus re-review) reduces that window instead of
    a single unguarded bind.
    """
    last_error: OSError | None = None
    for _ in range(attempts):
        port = _free_port()
        try:
            return CapabilityProxyServer(config.model_copy(update={"bind_port": port}), capability)
        except OSError as exc:
            last_error = exc
            continue
    assert last_error is not None
    raise last_error


def _raw_http_response(
    host: str,
    port: int,
    request_bytes: bytes,
    *,
    timeout: float = 10.0,
) -> tuple[int, bytes]:
    """Send a raw HTTP/1.1 request and return (status, body).

    Avoids httpx: a client can't express a deliberately-invalid request
    (chunked encoding with no chunked body, or a declared-but-unsent
    oversized Content-Length) through a conformant HTTP client's normal
    API, and driving the exact bytes over the wire is also immune to the
    megabytes-in-flight race a genuinely oversized body would add (the
    server writes its rejection before reading any body at all in both
    cases this file exercises).
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.sendall(request_bytes)
        buffer = b""
        while b"\r\n\r\n" not in buffer:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buffer += chunk
        header_blob, _, body = buffer.partition(b"\r\n\r\n")
        lines = header_blob.split(b"\r\n")
        status = int(lines[0].decode("latin-1").split(" ", 2)[1])
        headers = {
            key.decode("latin-1").strip().lower(): value.decode("latin-1").strip()
            for key, _, value in (line.partition(b":") for line in lines[1:])
            if key
        }
        content_length = int(headers.get("content-length", "0"))
        while len(body) < content_length:
            chunk = sock.recv(4096)
            if not chunk:
                break
            body += chunk
    return status, body


@contextmanager
def _capability_proxy(tmp_path: Path, *, attachment_id: str) -> Any:
    with _backend_server() as backend_port:
        capability = "b" * 43
        config = BrowserGatewayConfig(
            attachment_id=attachment_id,
            token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
            bind_port=_free_port(),
            upstream_protocol="http",
            upstream_port=backend_port,
            allowed_paths=["/commands"],
            command_path="/commands",
            expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            revocation_path=str((tmp_path / "revoked").resolve()),
        )
        server = _bind_capability_proxy_server(config, capability)
        proxy_port = int(server.server_address[1])
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield proxy_port, capability
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def test_browser_gateway_exception_path_returns_7807_document_not_bare_error(
    tmp_path: Path,
) -> None:
    """browser_gateway.py's ``_error`` (doc §6.1's fourth surface, the least
    existing structure of the four) previously returned a bare
    ``{"error": message}`` dict with no code/data/reason at all. A real
    client-triggered ``ValueError`` from ``_request_body`` (chunked request
    bodies are refused outright) now returns the same RFC 7807 document
    shape the HTTP surface uses.
    """
    with _capability_proxy(tmp_path, attachment_id="door-errors-browser-gateway-test") as (
        proxy_port,
        capability,
    ):
        path = f"/commands?{urlencode({'capability': capability})}"
        request_bytes = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{proxy_port}\r\n"
            "Origin: null\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        status, body = _raw_http_response("127.0.0.1", proxy_port, request_bytes)

    assert status == 400
    document = json.loads(body)
    assert "error" not in document
    assert document["reason"] == "configuration_error"
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert document["status"] == 400
    assert document["detail"] == "Configuration error."
    assert "chunked" not in document["detail"]


def test_browser_gateway_oversized_body_gets_its_own_payload_too_large_reason(
    tmp_path: Path,
) -> None:
    """F7+F14: the oversize branch specifically (``_request_body``'s
    ``length > MAX_REQUEST_BODY_BYTES`` check) is wired to the dedicated
    ``payload_too_large`` reason (413) -- not lumped into the blanket
    ``configuration_error`` (400) the other three ``_request_body``
    failures still use. A declared, never-sent ``Content-Length`` triggers
    the check before any body bytes are read, so this needs no real
    megabyte transfer.
    """
    from clio_relay.browser_gateway import MAX_REQUEST_BODY_BYTES

    with _capability_proxy(tmp_path, attachment_id="door-errors-oversize-test") as (
        proxy_port,
        capability,
    ):
        path = f"/commands?{urlencode({'capability': capability})}"
        request_bytes = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{proxy_port}\r\n"
            "Origin: null\r\n"
            f"Content-Length: {MAX_REQUEST_BODY_BYTES + 1}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        status, body = _raw_http_response("127.0.0.1", proxy_port, request_bytes)

    assert status == 413
    document = json.loads(body)
    assert document["reason"] == "payload_too_large"
    assert document["status"] == 413
    assert document["retryable"] is False
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert "exceeds the browser gateway limit" in document["detail"]


def test_browser_gateway_does_not_create_an_import_cycle_with_door_errors() -> None:
    """browser_gateway.py must not import door_errors at module scope: doing
    so creates a real cycle (browser_gateway -> door_errors -> storage_runtime
    -> core_queue -> browser_gateway, since core_queue already imports
    ``BrowserAttachmentRecord`` from browser_gateway.py). Run in a fresh
    subprocess -- this repo's own test session has already imported both
    modules, so only an isolated interpreter proves the import ORDER is
    actually safe rather than merely already-cached.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import clio_relay.core_queue; import clio_relay.browser_gateway",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
