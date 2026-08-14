"""Tests for the one door error-translation owner (clio-relay#231, R3).

Each test below is written failing-first against
``docs/design/relay-architecture-2026-08.md`` §6 before ``door_errors.py``
existed: the live hole (a deliberately-bare re-raise), the #218/#215/#228
regressions re-pointed at the frozen ``REASONS`` table instead of their old
ad hoc, per-site shapes, a sabotage twin proving unclassified exceptions
never reach the wire, and the browser_gateway fourth adapter replacing its
bare ``{"error": message}`` dict.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
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
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP
from fastmcp_tasks.client import call_tool_task  # pyright: ignore[reportUnknownVariableType]
from mcp.shared.exceptions import MCPError

from clio_relay import door_errors
from clio_relay.browser_gateway import BrowserGatewayConfig, CapabilityProxyServer
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
    QueueConflictError,
    TaskInputParkConflictError,
)
from clio_relay.fastmcp_server import RelayMcpRuntime, RelayTasksExtension, RelayTool
from clio_relay.http_api import create_app
from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal
from clio_relay.job_identity import OwnerSessionIdentityError
from clio_relay.mcp_server import mcp_tool_definitions_and_remote_catalog
from clio_relay.models import JobKind
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
    }
)


def test_every_reason_is_registered() -> None:
    """The frozen set is exactly the doc §6.3 table -- no more, no fewer."""
    assert set(door_errors.REASONS) == _EXPECTED_REASONS
    assert len(door_errors.REASONS) == 12
    for reason, spec in door_errors.REASONS.items():
        assert spec.reason == reason
        assert isinstance(spec.retryable, bool)
        assert isinstance(spec.mcp_code, int) and spec.mcp_code < 0
        assert 400 <= spec.http_status < 600
        assert spec.title


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

    fault = door_errors.classify(NotFoundError("missing"), table=perturbed)
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
        (TaskInputParkConflictError("park conflict"), "mcp_task_input_park_conflict"),
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
    assert fault.message == str(exc)
    spec = door_errors.REASONS[expected_reason]
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
    assert fault.data == exc.detail
    assert fault.data["code"] == "owner_session_identity_invalid"


def test_classify_jarvis_dispatch_refusal_is_an_object_entry_point() -> None:
    """JarvisDispatchRefusal is a durable-result dataclass, never raised-and-caught."""
    refusal = JarvisDispatchRefusal(
        code="jarvis_tool_error",
        message="the pipeline refused to dispatch",
        pipeline_id="pipeline-1",
        execution_id="execution-1",
        payload_schema_version="jarvis.error.v1",
    )
    fault = door_errors.classify(refusal)
    assert fault.reason == "jarvis_dispatch_refused"
    assert fault.message == refusal.message
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
    exactly once server-side, and the underlying exception's own text never
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


def test_message_is_hard_truncated_to_the_t1_budget() -> None:
    oversized = "x" * (door_errors.MAX_MESSAGE_CHARS + 500)
    fault = door_errors.classify(ConfigurationError(oversized))
    assert len(fault.message) == door_errors.MAX_MESSAGE_CHARS


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


def test_as_http_problem_drops_evidence_first_when_over_budget() -> None:
    """The ≤8KiB drop order (doc §6.4): evidence is dropped first -- the RFC
    7807 core four and reason/retryable are never dropped, and a small
    ``truncation`` need not also go once evidence alone frees enough room.
    """
    fault = door_errors.classify(
        ConfigurationError("oversized"),
        data={
            "evidence": {"artifact_id": "x" * 9000},
            "truncation": {"schema_version": "clio-relay.truncation.v1", "truncated": False},
        },
    )
    document = door_errors.as_http_problem(fault)
    encoded = json.dumps(document, separators=(",", ":")).encode("utf-8")
    assert "evidence" not in document
    assert "truncation" in document
    assert len(encoded) <= door_errors.MAX_ENVELOPE_BYTES
    for required in ("type", "title", "status", "detail", "schema_version", "reason", "retryable"):
        assert required in document


def test_as_http_problem_drops_truncation_too_when_still_oversized() -> None:
    """When evidence alone is not enough, truncation is dropped second."""
    fault = door_errors.classify(
        ConfigurationError("oversized"),
        data={
            "evidence": {"artifact_id": "x" * 6000},
            "truncation": {"marker": "y" * 8100},
        },
    )
    document = door_errors.as_http_problem(fault)
    encoded = json.dumps(document, separators=(",", ":")).encode("utf-8")
    assert "evidence" not in document
    assert "truncation" not in document
    assert len(encoded) <= door_errors.MAX_ENVELOPE_BYTES


def test_as_browser_gateway_error_maps_onto_the_two_arg_shape() -> None:
    fault = door_errors.classify(ConfigurationError("bad body"), reason="configuration_error")
    status, document = door_errors.as_browser_gateway_error(fault)
    assert status == fault.http_status == 400
    assert document == door_errors.as_http_problem(fault)
    assert "error" not in document  # not the old bare {"error": message} shape


# --------------------------------------------------------------------------- #
# fastmcp_server.py wiring: the live hole + #218/#215 re-pointed
# --------------------------------------------------------------------------- #


def _fastmcp_task_server(
    settings: RelaySettings,
    queue: ClioCoreQueue,
) -> tuple[FastMCP[dict[str, Any]], RelayMcpRuntime]:
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
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
    return server, runtime


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
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    server, runtime = _fastmcp_task_server(settings, queue)

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
            assert "forced park conflict" in failure.value.message

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
    assert "different semantics" in mcp_error.message


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


# --------------------------------------------------------------------------- #
# http_api.py wiring: the global exception handler + #228 re-pointed
# --------------------------------------------------------------------------- #


def test_228_regression_via_testclient_launcher_resolution_returns_7807_document() -> None:
    """#228 re-pointed at door_errors, exercised over a real HTTP round trip.

    ``jarvis_mcp_command()`` raises a bare ``ValueError`` for dozens of
    unrelated failures (doc §6.3's own caveat on ``launcher_resolution_failed``),
    so classify() cannot type-dispatch it automatically -- the call path
    that already knows this is a launcher-resolution failure (grounded in
    the exact #228 scenario: ``http_api.py:1753-1758``) supplies the reason.
    """
    app = FastAPI()

    @app.get("/probe")
    def probe() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        try:
            raise ValueError("receipt-bound clio-kit runtime identity did not verify")
        except ValueError as exc:
            fault = door_errors.classify(exc, reason="launcher_resolution_failed")
            return JSONResponse(
                door_errors.as_http_problem(fault),
                status_code=fault.http_status,
                media_type="application/problem+json",
            )

    client = TestClient(app)
    response = client.get("/probe")

    assert response.status_code == 409
    document = response.json()
    assert document["status"] == 409
    assert document["reason"] == "launcher_resolution_failed"
    assert document["retryable"] is False
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert document["type"] == "urn:clio-relay:error:launcher_resolution_failed"
    assert "did not verify" in document["detail"]


def test_sabotage_twin_novel_exception_via_testclient_returns_typed_internal_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The global exception handler wired into ``create_app`` (doc §3's "0
    unclassified exceptions reach the wire") -- an injected route raising a
    type door_errors has never seen still produces a typed internal_error
    document, with the traceback logged exactly once and never on the wire.
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


def test_107_existing_httpexception_sites_are_unaffected_by_the_global_handler(
    tmp_path: Path,
) -> None:
    """The global Exception handler must never intercept HTTPException --
    Starlette dispatches to the most specific registered handler, so the
    107 hand-rolled ``raise HTTPException(...)`` sites this slice explicitly
    does not touch (doc §6.2) keep their exact existing bare-string shape.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    app = create_app(settings)
    client = cast(Any, TestClient(app))

    response = client.get("/jobs/does-not-exist/monitor")

    assert response.status_code in (401, 403, 404)
    body = response.json()
    assert "schema_version" not in body


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


def _raw_http_response(
    host: str,
    port: int,
    request_bytes: bytes,
    *,
    timeout: float = 10.0,
) -> tuple[int, bytes]:
    """Send a raw HTTP/1.1 request and return (status, body).

    Avoids httpx: a client can't express a deliberately-invalid request
    (chunked encoding with no chunked body) through a conformant HTTP
    client's normal API, and driving the exact bytes over the wire is also
    immune to the megabytes-in-flight race a genuinely oversized body would
    add (the server writes its rejection before reading any body at all).
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
    with _backend_server() as backend_port:
        capability = "b" * 43
        proxy_port = _free_port()
        config = BrowserGatewayConfig(
            attachment_id="door-errors-browser-gateway-test",
            token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
            bind_port=proxy_port,
            upstream_protocol="http",
            upstream_port=backend_port,
            allowed_paths=["/commands"],
            command_path="/commands",
            expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            revocation_path=str((tmp_path / "revoked").resolve()),
        )
        server = CapabilityProxyServer(config, capability)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
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
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    assert status == 400
    document = json.loads(body)
    assert "error" not in document
    assert document["reason"] == "configuration_error"
    assert document["schema_version"] == door_errors.SCHEMA_VERSION
    assert document["status"] == 400
    assert "chunked" in document["detail"]


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
