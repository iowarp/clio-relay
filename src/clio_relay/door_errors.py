"""The one error-translation owner for every relay error surface (#231, R3).

``docs/design/relay-architecture-2026-08.md`` §6 is the governing design:
four call surfaces -- ``fastmcp_server.py`` (SEP-2663 task handlers plus
``intercept_tool_call``), ``http_api.py`` (FastAPI routes),
``browser_gateway.py`` (``CapabilityProxyHandler._error``, the fourth
surface named in §6.1 -- the shape with the least existing structure), and
``mcp_server.py`` (the stdio JSON-RPC ``_error`` helper, not yet wired
through this module -- tracked as its own live issue,
`iowarp/clio-relay#235 <https://github.com/iowarp/clio-relay/issues/235>`_)
-- each translated exceptions to their own locally-invented wire shape.
This module is the single classification owner: :func:`classify` turns a
caught exception (or a durable, never-raised
:class:`~clio_relay.jarvis_dispatch_failure.JarvisDispatchRefusal`) into a
:class:`RelayFault`. The cohesive renderers live in
:mod:`clio_relay.door_error_adapters` and are re-exported here for the
established public and test patch surface. A call site never picks its own
status code: every ``reason``, and everything that follows from it
(``retryable``/``mcp_code``/``http_status``), comes from the frozen
:data:`REASONS` table (§6.3).

**Dispatch order** (see :func:`classify`):

1. A :class:`JarvisDispatchRefusal` is a frozen dataclass a durable
   ``jarvis_run`` result *carries* -- never raised and caught -- so it gets
   its own object-typed entry point ahead of exception dispatch.
2. An explicit ``reason=`` keyword is the call-path-scope override for sites
   that already have a shipped, call-path-fixed reason (``fastmcp_server.py``'s
   ``_handle_get`` catch-all always means
   ``mcp_task_status_reconciliation_failed``, regardless of the underlying
   exception's type). These call sites already know their own reason; this
   module still owns everything that follows from it.
3. Seven exception types have an unambiguous 1:1 reason and are dispatched
   by ``isinstance`` with no call-site help:
   :class:`TaskInputParkConflictError`, :class:`NotFoundError`,
   :class:`ConfigurationError`,
   :class:`~clio_relay.storage_runtime.StorageAdmissionError`,
   :class:`~clio_relay.storage_runtime.StorageRuntimeViolation`,
   :class:`ObservationTimeoutError`, and
   :class:`~clio_relay.job_identity.OwnerSessionIdentityError`.
4. Anything else is ``internal_error``: the traceback is logged once, here,
   via ``logger.exception`` -- and never reaches ``message`` (nor, by
   construction, the wire). This is what makes §3's "0 unclassified
   exceptions reach the wire" exit criterion meetable. "Once" describes
   this module's own logging, not the whole process: on the HTTP surface,
   Starlette's ``ServerErrorMiddleware`` re-raises after sending the
   response it built from the handler's return value (by design, so a real
   ASGI server can still observe the error), and uvicorn logs that re-raise
   too -- a second, server-side-only log line is expected there, not a
   ``door_errors`` defect (F6). A streaming response that has already
   started sending bytes before an exception is never covered by this
   module either way: nothing can rewrite headers/status once a response
   is underway, so mid-stream failures stay a transport-level cut, not a
   ``clio-relay.error.v1`` document.

**Per-reason grounding** (moved here from the per-site rationale comments
this slice deletes at their old call sites, per doc §10):

``mcp_task_input_park_conflict``
    ``RelayMcpRuntime._park_agent_input`` (``fastmcp_server.py``, not
    ``RelayTasksExtension`` -- see the corrected docstring on
    :class:`~clio_relay.errors.TaskInputParkConflictError`) exhausts 8 CAS
    retries when ``update_mcp_task_projection``'s optimistic-concurrency
    check keeps losing a race. This is a transient concurrency conflict,
    never a client parameter problem (clio-relay#218 rework) -- before this
    slice, ``intercept_tool_call`` left it deliberately unwrapped rather than
    mistype it as ``INVALID_PARAMS``, so it escaped through FastMCP's generic
    handler as a bare internal error. Routing it through :func:`classify`
    closes that hole with a correctly-typed, retryable conversion instead.
``mcp_task_conflict``
    ``put_mcp_task``'s dedicated genuine task-identity-reuse conflict
    (clio-relay#218), refused as a typed, queryable ``MCPError`` rather than
    escaping through FastMCP's generic handler as a bare, typeless
    ``INTERNAL_ERROR`` (the original #218 symptom). The dedicated subtype is
    the only queue conflict whose authored message is marked public there.
``mcp_task_status_reconciliation_failed``
    ``_handle_get``'s catch-all (clio-relay#215): ``task_status`` can
    re-derive a task's status over network round trips, and an unwrapped
    failure there previously escaped as a bare, typeless "Internal server
    error". Always this reason on this call path, regardless of the
    underlying exception's type -- the typed reason plus ``task_id`` in
    ``data`` is the queryable signal, handler internals (``str(exc)``) never
    reach ``message``.

See docs/design/relay-architecture-2026-08.md §6.3 for the original 13 rows.
R9 adds the HTTP-originated rows that replace ``http_api.py``'s 107 legacy
sites and outer request-body middleware serializer; each keeps its shipped
status while gaining a specific, frozen agent-facing reason.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final

import mcp_types

from clio_relay.bounded_payload import (
    # Re-exported (not redefined, F12 #231 R6 review): bounded_payload.py is
    # the single owner of the "clio-relay.truncation.v1" schema tag; R6
    # moved this module's own record CONSTRUCTION there too (see
    # _bounded_text below), closing the "second copy of the same schema"
    # gap an earlier revision of this comment described.
    TRUNCATION_SCHEMA_VERSION as TRUNCATION_SCHEMA_VERSION,
)
from clio_relay.bounded_payload import build_truncation_record
from clio_relay.door_error_adapters import MAX_ENVELOPE_BYTES as MAX_ENVELOPE_BYTES
from clio_relay.door_error_adapters import SCHEMA_VERSION as SCHEMA_VERSION
from clio_relay.door_error_adapters import as_browser_gateway_error as as_browser_gateway_error
from clio_relay.door_error_adapters import as_http_problem as as_http_problem
from clio_relay.door_error_adapters import as_mcp_error as as_mcp_error
from clio_relay.door_error_adapters import websocket_refusal as websocket_refusal
from clio_relay.door_error_messages import public_message, resolved_public_message
from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
    PublicMessageError,
    RelayAuthoredError,
    TaskInputParkConflictError,
)
from clio_relay.errors import public_message_error as public_message_error
from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal
from clio_relay.job_identity import OwnerSessionIdentityError
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageRuntimeError,
    StorageRuntimeViolation,
)

logger = logging.getLogger(__name__)

JSON = dict[str, Any]

# T1 refusal/detail text budget from design §6.4.
MAX_MESSAGE_CHARS: Final = 2_000


@dataclass(frozen=True, slots=True)
class ReasonSpec:
    """One frozen row of the ``REASONS`` registry (doc §6.3)."""

    reason: str
    retryable: bool
    mcp_code: int
    http_status: int
    title: str


def _row(
    reason: str,
    *,
    retryable: bool,
    mcp_code: int,
    http_status: int,
    title: str,
) -> ReasonSpec:
    return ReasonSpec(
        reason=reason,
        retryable=retryable,
        mcp_code=mcp_code,
        http_status=http_status,
        title=title,
    )


def _http_row(
    reason: str,
    http_status: int,
    title: str,
    *,
    retryable: bool = False,
) -> ReasonSpec:
    """Define an HTTP-originated reason without inventing a second code table."""
    return _row(
        reason,
        retryable=retryable,
        mcp_code=mcp_types.INTERNAL_ERROR if http_status >= 500 else mcp_types.INVALID_PARAMS,
        http_status=http_status,
        title=title,
    )


# Relay-owned MCP codes avoid the SDK-reserved -32000..-32019 band.
# The already-shipped storage admission code -32007 remains pinned.
_RELAY_CUSTOM_CODE_BAND_START: Final = -32059
_RELAY_CUSTOM_CODE_BAND_END: Final = -32050  # inclusive


def _relay_code(offset: int) -> int:
    code = _RELAY_CUSTOM_CODE_BAND_END - offset
    if not (_RELAY_CUSTOM_CODE_BAND_START <= code <= _RELAY_CUSTOM_CODE_BAND_END):
        raise ValueError(f"door_errors: {code} falls outside the relay-owned -32050..-32059 band")
    return code


#: The frozen, closed reason set (doc §6.3). Adding a reason is a deliberate
#: contract change edited alongside ``tests/test_door_errors.py``'s
#: membership test -- never a silent side effect of some other change.
#: A ``MappingProxyType`` (F9): read-only at the type level, not merely by
#: convention.
REASONS: Final[Mapping[str, ReasonSpec]] = MappingProxyType(
    {
        spec.reason: spec
        for spec in (
            _row(
                "mcp_task_input_park_conflict",
                retryable=True,
                mcp_code=_relay_code(0),  # -32050 (was -32001, F1)
                http_status=409,
                title="MCP task input park conflict",
            ),
            _row(
                "mcp_task_conflict",
                retryable=False,
                mcp_code=mcp_types.INVALID_PARAMS,
                http_status=409,
                title="MCP task conflict",
            ),
            _row(
                "mcp_task_status_reconciliation_failed",
                retryable=True,
                mcp_code=mcp_types.INTERNAL_ERROR,
                http_status=500,
                title="MCP task status reconciliation failed",
            ),
            _row(
                "jarvis_dispatch_refused",
                retryable=False,
                mcp_code=mcp_types.INVALID_PARAMS,
                http_status=422,
                title="JARVIS dispatch refused",
            ),
            _row(
                "not_found",
                retryable=False,
                mcp_code=mcp_types.INVALID_PARAMS,
                http_status=404,
                title="Not found",
            ),
            _row(
                "configuration_error",
                retryable=False,
                mcp_code=mcp_types.INVALID_PARAMS,
                http_status=400,
                title="Configuration error",
            ),
            _row(
                # Already shipped, not reallocated -- see the band comment
                # above. -32007 sits inside the SDK-reserved numeric range
                # but no mcp_types.jsonrpc constant currently occupies it;
                # kept as a deliberate, documented exception rather than a
                # silent squat.
                "storage_admission_refused",
                retryable=True,
                mcp_code=-32007,
                http_status=507,
                title="Storage admission refused",
            ),
            _row(
                # A verification-pass gap beyond the seed ten (doc §6.3): the
                # sibling of storage_admission_refused for a RUNNING child that
                # already crossed a durable storage safety boundary
                # (StorageRuntimeViolation, storage_runtime.py:80-81) rather than
                # a new admission that could not be reserved. Not retryable --
                # unlike admission, the boundary was already crossed, so retrying
                # the same job does not undo it.
                "storage_safety_violation",
                retryable=False,
                mcp_code=_relay_code(1),  # -32051 (was -32008, F1)
                http_status=507,
                title="Storage safety violation",
            ),
            _row(
                "observation_timeout",
                retryable=True,
                mcp_code=_relay_code(2),  # -32052 (was -32002, F1)
                http_status=504,
                title="Observation timeout",
            ),
            _row(
                "launcher_resolution_failed",
                retryable=False,
                mcp_code=_relay_code(3),  # -32053 (was -32003, F1)
                http_status=409,
                title="Launcher resolution failed",
            ),
            _row(
                # The second verification-pass gap (doc §6.3): OwnerSessionIdentityError
                # (job_identity.py:39-40+), already caught by name at
                # http_api.py:1158-1159 and :2965 and mapped to HTTP 422 today.
                "owner_session_identity_refused",
                retryable=False,
                mcp_code=_relay_code(4),  # -32054 (was -32004, F1)
                http_status=422,
                title="Owner session identity refused",
            ),
            _row(
                "internal_error",
                retryable=False,
                mcp_code=mcp_types.INTERNAL_ERROR,
                http_status=500,
                title="Internal error",
            ),
            _row(
                # F7+F14 (opus review): browser_gateway's own oversize
                # request-body branch (_request_body's length > MAX_REQUEST_
                # BODY_BYTES check) previously fell into configuration_error's
                # 400 blanket mapping alongside three unrelated protocol
                # validation failures (chunked encoding, a malformed
                # Content-Length). It is a distinct, well-known HTTP concept
                # (413) worth its own reason rather than an ad hoc 413 chosen
                # at the call site.
                "payload_too_large",
                retryable=False,
                mcp_code=_relay_code(5),  # -32055
                http_status=413,
                title="Payload too large",
            ),
            _http_row("http_request_malformed", 400, "HTTP request malformed"),
            _http_row("poll_interval_invalid", 400, "Poll interval invalid"),
            _http_row("log_stream_invalid", 400, "Log stream invalid"),
            _http_row("authentication_required", 401, "Authentication required"),
            _http_row("resource_ownership_refused", 403, "Resource ownership refused"),
            _http_row("session_scope_refused", 403, "Session scope refused"),
            _http_row("session_identity_unavailable", 404, "Session identity unavailable"),
            _http_row("session_status_unavailable", 404, "Session status unavailable"),
            _http_row(
                "jarvis_runtime_authority_unavailable",
                404,
                "JARVIS runtime authority unavailable",
            ),
            _http_row("input_ingest_unavailable", 404, "Input ingest unavailable"),
            _http_row("job_not_found", 404, "Job not found"),
            _http_row("task_not_found", 404, "Task not found"),
            _http_row("gateway_not_found", 404, "Gateway not found"),
            _http_row("artifact_not_found", 404, "Artifact not found"),
            _http_row(
                "session_generation_identity_unavailable",
                409,
                "Session generation identity unavailable",
            ),
            _http_row("session_intake_closed", 409, "Session intake closed"),
            _http_row("session_binding_headers_required", 409, "Session binding headers required"),
            _http_row(
                "session_binding_identity_mismatch", 409, "Session binding identity mismatch"
            ),
            _http_row("unbound_session_api", 409, "Unbound session API"),
            _http_row("job_cluster_mismatch", 409, "Job cluster mismatch"),
            _http_row("job_submission_conflict", 409, "Job submission conflict"),
            _http_row("mcp_submission_conflict", 409, "MCP submission conflict"),
            _http_row(
                "jarvis_runtime_authority_conflict",
                409,
                "JARVIS runtime authority conflict",
            ),
            _http_row("jarvis_artifact_conflict", 409, "JARVIS artifact conflict"),
            _http_row("input_ingest_conflict", 409, "Input ingest conflict", retryable=True),
            _http_row("transform_conflict", 409, "Transform conflict"),
            _http_row("gateway_cluster_mismatch", 409, "Gateway cluster mismatch"),
            _http_row("gateway_conflict", 409, "Gateway conflict"),
            _http_row("queue_operation_conflict", 409, "Queue operation conflict"),
            _http_row("retention_conflict", 409, "Retention conflict"),
            _http_row("job_submission_refused", 422, "Job submission refused"),
            _http_row("mcp_admission_refused", 422, "MCP admission refused"),
            _http_row("input_ingest_refused", 422, "Input ingest refused"),
            _http_row("job_route_refused", 422, "Job route refused"),
            _http_row("jarvis_submission_refused", 422, "JARVIS submission refused"),
            _http_row("transform_refused", 422, "Transform refused"),
            _http_row("wait_parameters_invalid", 422, "Wait parameters invalid"),
            _http_row("queue_query_refused", 422, "Queue query refused"),
            _http_row(
                "input_ingest_terminalization_failed",
                500,
                "Input ingest terminalization failed",
            ),
            _http_row(
                "session_authentication_unavailable",
                503,
                "Session authentication unavailable",
                retryable=True,
            ),
            _http_row("request_validation_failed", 422, "Request validation failed"),
            _http_row("route_not_found", 404, "Route not found"),
            _http_row("method_not_allowed", 405, "Method not allowed"),
            _http_row("framework_http_error", 500, "Framework HTTP error"),
            _http_row(
                "browser_gateway_overloaded",
                503,
                "Browser gateway overloaded",
                retryable=True,
            ),
            _http_row("browser_preflight_refused", 403, "Browser preflight refused"),
            _http_row("browser_attachment_not_found", 404, "Browser attachment not found"),
            _http_row("browser_origin_refused", 403, "Browser origin refused"),
            _http_row("browser_capability_refused", 401, "Browser capability refused"),
            _http_row("browser_method_not_allowed", 405, "Browser method not allowed"),
            _http_row(
                "browser_upstream_unavailable",
                502,
                "Browser upstream unavailable",
                retryable=True,
            ),
            _http_row("websocket_authentication_failed", 401, "WebSocket authentication failed"),
            _http_row("websocket_session_binding_failed", 409, "WebSocket session binding failed"),
            _http_row("websocket_page_limit_invalid", 422, "WebSocket page limit invalid"),
            _http_row("websocket_poll_interval_invalid", 400, "WebSocket poll interval invalid"),
            _http_row("websocket_cursor_invalid", 400, "WebSocket cursor invalid"),
            _http_row(
                "websocket_resource_ownership_refused",
                403,
                "WebSocket resource ownership refused",
            ),
            _http_row("websocket_resource_not_found", 404, "WebSocket resource not found"),
        )
    }
)


def _empty_data() -> Mapping[str, Any]:
    return {}


@dataclass(frozen=True, slots=True)
class RelayFault:
    """The classified shape every door_errors adapter renders from.

    Every field traces back to one :data:`REASONS` row (via :func:`classify`)
    except ``message`` (T1-bounded, §6.4), ``data`` (reason-specific
    extension payload, e.g. a ``task_id`` or a storage decision), and
    ``truncation`` (the T1 elision record :func:`_bounded_text` populates
    whenever it actually cuts ``message`` -- ``None`` only when nothing was
    elided, never a blanket default regardless of truth, F2).
    """

    reason: str
    retryable: bool
    mcp_code: int
    http_status: int
    title: str
    message: str
    data: Mapping[str, Any] = field(default_factory=_empty_data)
    truncation: JSON | None = None


class HTTPProblemError(Exception):
    """Carry one preclassified fault through FastAPI without shaping its body there."""

    def __init__(self, fault: RelayFault) -> None:
        super().__init__(fault.reason)
        self.fault = fault


# The seven exception types with an unambiguous 1:1 reason (doc §6.3
# dispatch rule 3). Order does not matter for correctness here -- none of
# the seven is a superclass of another -- but is kept in the table's row
# order for readability.
_TYPE_REASONS: Final[tuple[tuple[type[BaseException], str], ...]] = (
    (TaskInputParkConflictError, "mcp_task_input_park_conflict"),
    (NotFoundError, "not_found"),
    (ConfigurationError, "configuration_error"),
    (StorageAdmissionError, "storage_admission_refused"),
    (StorageRuntimeViolation, "storage_safety_violation"),
    (ObservationTimeoutError, "observation_timeout"),
    (OwnerSessionIdentityError, "owner_session_identity_refused"),
)


def _bounded_text(text: str) -> tuple[str, JSON | None]:
    """Hard-truncate refusal/detail text to the T1 budget (doc §6.4).

    Returns the (possibly truncated) text and, when truncation actually
    happened, a populated ``clio-relay.truncation.v1`` record -- ``None``
    only when nothing was elided. F2: the envelope must never claim
    ``"truncation": null`` on a document whose ``detail`` was in fact cut.
    The record itself is built by :func:`~clio_relay.bounded_payload.
    build_truncation_record` (R6, #231) -- this module supplies the T1
    char-count policy (what gets kept), bounded_payload owns the record
    shape (how the cut is described), so the schema has exactly one
    constructor regardless of which tier or module is bounding.
    """
    truncated = text[:MAX_MESSAGE_CHARS]
    if truncated == text:
        return truncated, None
    original_bytes = len(text.encode("utf-8"))
    retained_bytes = len(truncated.encode("utf-8"))
    record = build_truncation_record(
        retention="head",
        original_bytes=original_bytes,
        retained_head_bytes=retained_bytes,
        stream="message",
    )
    return truncated, record


def _safe_str(exc: BaseException) -> str | None:
    """Render ``str(exc)`` for server logging without letting it escape.

    A raising ``__str__`` (or one that returns something ``str()`` itself
    cannot coerce) must never propagate out of :func:`classify` -- on the
    HTTP surface that would collapse past ``door_errors`` entirely into
    Starlette's ``ServerErrorMiddleware``, replacing the original exception
    with a new, undiagnosable one and losing the typed response this module
    exists to guarantee.
    """
    try:
        detail: Any = exc.public_message if isinstance(exc, PublicMessageError) else str(exc)
        if not isinstance(detail, str):
            raise TypeError("public exception message must be a string")
        return detail
    except Exception:
        logger.exception(
            "clio-relay: %s.__str__() raised; door_errors could not render its message",
            type(exc).__name__,
        )
        return None


def _log_classified_exception(exc: BaseException, *, reason: str) -> str | None:
    """Log a classified exception once and return its safely rendered text."""
    detail = _safe_str(exc)
    logger.info(
        "clio-relay: classified %s as %s: %s",
        type(exc).__name__,
        reason,
        detail if detail is not None else "<message unavailable>",
    )
    return detail


def _typed_data(exc: BaseException) -> JSON:
    """Return the reason-specific extension payload a typed exception carries.

    Guarded (F5): a malformed instance of one of the seven typed exceptions
    (e.g. a subclass that overrides ``.decision``/``.detail`` with something
    that raises on access) must degrade to no extension payload, never crash
    classification.
    """
    if isinstance(exc, RelayAuthoredError):
        exc = exc.source
    try:
        if isinstance(exc, StorageRuntimeError):
            return {"storage_decision": exc.decision.to_dict()}
        if isinstance(exc, OwnerSessionIdentityError):
            return {key: value for key, value in exc.detail.items() if key != "message"}
    except Exception:
        logger.exception(
            "clio-relay: could not extract %s's typed extension payload",
            type(exc).__name__,
        )
        return {}
    return {}


def _build(spec: ReasonSpec, *, message: str, data: Mapping[str, Any]) -> RelayFault:
    bounded_message, truncation = _bounded_text(message)
    return RelayFault(
        reason=spec.reason,
        retryable=spec.retryable,
        mcp_code=spec.mcp_code,
        http_status=spec.http_status,
        title=spec.title,
        message=bounded_message,
        data=data,
        truncation=truncation,
    )


def fault_for_reason(
    reason: str,
    message: str,
    *,
    data: Mapping[str, Any] | None = None,
) -> RelayFault:
    """Build a bounded fault for one deliberate, registered refusal site."""
    try:
        spec = REASONS[reason]
    except KeyError as exc:
        raise ValueError(f"door_errors: {reason!r} is not a registered REASONS entry") from exc
    return _build(spec, message=message, data=data if data is not None else {})


def fault_for_http_status(
    reason: str,
    http_status: int,
    *,
    message: str | None = None,
) -> RelayFault:
    """Build an owner-rendered framework fault while preserving its exact status."""
    if not 400 <= http_status <= 599:
        raise ValueError("door_errors: framework HTTP status must be between 400 and 599")
    try:
        registered = REASONS[reason]
    except KeyError as exc:
        raise ValueError(f"door_errors: {reason!r} is not a registered REASONS entry") from exc
    spec = ReasonSpec(
        reason=registered.reason,
        retryable=http_status >= 500,
        mcp_code=(mcp_types.INTERNAL_ERROR if http_status >= 500 else mcp_types.INVALID_PARAMS),
        http_status=http_status,
        title=registered.title,
    )
    return _build(
        spec,
        message=(
            message
            if message is not None
            else public_message(reason=registered.reason, title=registered.title)
        ),
        data={},
    )


def http_problem(
    reason: str,
    message: str | None = None,
    *,
    exc: BaseException | None = None,
    data: Mapping[str, Any] | None = None,
) -> HTTPProblemError:
    """Create FastAPI control flow for one deliberate registered HTTP problem."""
    if exc is None:
        if message is None:
            raise ValueError("door_errors.http_problem() requires message or exc")
        fault = fault_for_reason(reason, message, data=data)
    else:
        fault = classify(
            exc,
            reason=reason,
            message=message,
            data=_typed_data(exc) if data is None else data,
        )
    return HTTPProblemError(fault)


def classify(
    exc: BaseException | JarvisDispatchRefusal,
    *,
    reason: str | None = None,
    message: str | None = None,
    data: Mapping[str, Any] | None = None,
    _table: Mapping[str, ReasonSpec] = REASONS,
) -> RelayFault:
    """Classify one caught exception (or a carried refusal) into a :class:`RelayFault`.

    Args:
        exc: The caught exception, or a durable
            :class:`JarvisDispatchRefusal` a ``jarvis_run`` result carries
            (never raised-and-caught -- see dispatch rule 1 in the module
            docstring).
        reason: A call-path-scope override (dispatch rule 2). Must already be
            a key in the reason table; an unregistered reason is a caller
            bug, not a silent fallback, and raises :class:`ValueError`.
        message: Deliberate bounded public detail. When omitted, a marked
            exception's relay-authored message is used; unmarked exception
            text is logging-only and the reason's stable formatter is used.
        data: Overrides the default reason-specific extension payload
            (:func:`_typed_data`, or ``{}``).
        _table: Private, test-only override of :data:`REASONS` (F9) -- lets
            ``tests/test_door_errors.py`` prove the adapters read their codes
            from whatever table classification used, rather than duplicating
            it. Not part of the public contract; do not pass this from
            production call sites.

    Returns:
        The classified :class:`RelayFault`. Falls back to ``internal_error``
        (dispatch rule 4) when nothing else matches, logging the traceback
        once via ``logger.exception`` here -- on the HTTP surface, Starlette's
        ``ServerErrorMiddleware`` still re-raises after the response is sent
        (by design, so a real ASGI server can log it too), so a second,
        server-side-only log line from uvicorn is expected, not a bug (F6).
        Unmarked exception internals never reach the wire either way.
    """
    if isinstance(exc, JarvisDispatchRefusal):
        spec = _table["jarvis_dispatch_refused"]
        return _build(
            spec,
            message=(
                message
                if message is not None
                else public_message(reason=spec.reason, title=spec.title)
            ),
            data=data
            if data is not None
            else {
                "code": exc.code,
                "pipeline_id": exc.pipeline_id,
                "execution_id": exc.execution_id,
            },
        )
    if reason is not None:
        if reason not in _table:
            msg = f"door_errors.classify(): {reason!r} is not a registered REASONS entry"
            raise ValueError(msg)
        logged_detail = _log_classified_exception(exc, reason=reason)
        spec = _table[reason]
        return _build(
            spec,
            message=resolved_public_message(
                exc,
                logged_detail=logged_detail,
                explicit=message,
                reason=spec.reason,
                title=spec.title,
            ),
            data=data if data is not None else _typed_data(exc),
        )
    for exc_type, mapped_reason in _TYPE_REASONS:
        if isinstance(exc, exc_type):
            logged_detail = _log_classified_exception(exc, reason=mapped_reason)
            spec = _table[mapped_reason]
            return _build(
                spec,
                message=resolved_public_message(
                    exc,
                    logged_detail=logged_detail,
                    explicit=message,
                    reason=spec.reason,
                    title=spec.title,
                ),
                data=data if data is not None else _typed_data(exc),
            )
    logger.exception(
        "clio-relay: door_errors.classify() found no registered reason for %s",
        type(exc).__name__,
    )
    return _build(
        _table["internal_error"],
        message="relay encountered an internal error.",
        data={},
    )
