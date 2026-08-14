"""The one error-translation owner for every relay error surface (#231, R3).

``docs/design/relay-architecture-2026-08.md`` §6 is the governing design:
four call surfaces -- ``fastmcp_server.py`` (SEP-2663 task handlers plus
``intercept_tool_call``), ``http_api.py`` (FastAPI routes), ``mcp_server.py``
(the stdio JSON-RPC ``_error`` helper, not yet wired through this module),
and ``browser_gateway.py`` (``CapabilityProxyHandler._error``, the shape with
the least existing structure) -- each translated exceptions to their own
locally-invented wire shape. This module is the single owner: :func:`classify`
turns a caught exception (or a durable, never-raised
:class:`~clio_relay.jarvis_dispatch_failure.JarvisDispatchRefusal`) into a
:class:`RelayFault`, and :func:`as_mcp_error`, :func:`as_http_problem`, and
:func:`as_browser_gateway_error` render that one fault onto each surface's
wire shape. A call site never picks its own status code: every ``reason``,
and everything that follows from it (``retryable``/``mcp_code``/
``http_status``), comes from the frozen :data:`REASONS` table (§6.3).

**Dispatch order** (see :func:`classify`):

1. A :class:`JarvisDispatchRefusal` is a frozen dataclass a durable
   ``jarvis_run`` result *carries* -- never raised and caught -- so it gets
   its own object-typed entry point ahead of exception dispatch.
2. An explicit ``reason=`` keyword is the call-path-scope override: a small
   number of raise sites cannot be told apart by exception type alone (a
   bare ``QueueConflictError`` means a genuine MCP task-identity conflict
   only on the ``intercept_tool_call`` path -- the same type is raised 651
   other times in ``core_queue.py`` for unrelated invariants) or already
   have a shipped, call-path-fixed reason (``fastmcp_server.py``'s
   ``_handle_get`` catch-all always means
   ``mcp_task_status_reconciliation_failed``, regardless of the underlying
   exception's type). These call sites already know their own reason; this
   module still owns everything that follows from it.
3. Seven exception types have an unambiguous 1:1 reason and are dispatched
   by ``isinstance`` with no call-site help: :class:`TaskInputParkConflictError`,
   :class:`NotFoundError`, :class:`ConfigurationError`,
   :class:`~clio_relay.storage_runtime.StorageAdmissionError`,
   :class:`~clio_relay.storage_runtime.StorageRuntimeViolation`,
   :class:`ObservationTimeoutError`, and
   :class:`~clio_relay.job_identity.OwnerSessionIdentityError`.
4. Anything else is ``internal_error``: the traceback is logged exactly once,
   server-side, via ``logger.exception`` -- and never reaches ``message``
   (nor, by construction, the wire). This is what makes §3's "0 unclassified
   exceptions reach the wire" exit criterion meetable.

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
    ``put_mcp_task``'s genuine task-identity-reuse ``QueueConflictError``
    (clio-relay#218), refused as a typed, queryable ``MCPError`` rather than
    escaping through FastMCP's generic handler as a bare, typeless
    ``INTERNAL_ERROR`` (the original #218 symptom). Call-path-scoped (see
    dispatch rule 2 above): a bare ``QueueConflictError`` means this only on
    the MCP-task-creation path.
``mcp_task_status_reconciliation_failed``
    ``_handle_get``'s catch-all (clio-relay#215): ``task_status`` can
    re-derive a task's status over network round trips, and an unwrapped
    failure there previously escaped as a bare, typeless "Internal server
    error". Always this reason on this call path, regardless of the
    underlying exception's type -- the typed reason plus ``task_id`` in
    ``data`` is the queryable signal, handler internals (``str(exc)``) never
    reach ``message``.

See docs/design/relay-architecture-2026-08.md §6.3 for the remaining nine
rows' grounding (already-shipped precedents, verified raise sites, and the
two reasons this design pass's own verification found beyond the seed ten).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from mcp.shared.exceptions import MCPError

from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
    TaskInputParkConflictError,
)
from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal
from clio_relay.job_identity import OwnerSessionIdentityError
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageRuntimeError,
    StorageRuntimeViolation,
)

logger = logging.getLogger(__name__)

JSON = dict[str, Any]

#: Schema tag stamped on every :func:`as_http_problem` document (doc §6.3).
SCHEMA_VERSION: Final = "clio-relay.error.v1"

# T1 (doc §6.4): refusal/detail text budget, hard-truncated. A fourth
# independent literal agreeing with jarvis_dispatch_failure.py's
# MAX_REFUSAL_MESSAGE_CHARS, control_channel.py's
# MAX_CHANNEL_EVENT_DETAIL_CHARS, and remote_connection.py's inline
# [:2_000] slice -- R6 is scoped to name one shared constant instead of the
# four that now independently agree (§6.4).
MAX_MESSAGE_CHARS: Final = 2_000

# Whole-envelope budget (doc §6.4 T1): as_http_problem drops "evidence" then
# "truncation", in that order, before ever touching the RFC 7807 core four
# (type/title/status/detail) or reason/retryable -- those are never dropped.
MAX_ENVELOPE_BYTES: Final = 8 * 1024

_DROP_ORDER: Final[tuple[str, ...]] = ("evidence", "truncation")


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


#: The frozen, closed reason set (doc §6.3). Adding a reason is a deliberate
#: contract change edited alongside ``tests/test_door_errors.py``'s
#: membership test -- never a silent side effect of some other change.
REASONS: Final[Mapping[str, ReasonSpec]] = {
    spec.reason: spec
    for spec in (
        _row(
            "mcp_task_input_park_conflict",
            retryable=True,
            mcp_code=-32001,
            http_status=409,
            title="MCP task input park conflict",
        ),
        _row(
            "mcp_task_conflict",
            retryable=False,
            mcp_code=-32602,
            http_status=409,
            title="MCP task conflict",
        ),
        _row(
            "mcp_task_status_reconciliation_failed",
            retryable=True,
            mcp_code=-32603,
            http_status=500,
            title="MCP task status reconciliation failed",
        ),
        _row(
            "jarvis_dispatch_refused",
            retryable=False,
            mcp_code=-32602,
            http_status=422,
            title="JARVIS dispatch refused",
        ),
        _row(
            "not_found",
            retryable=False,
            mcp_code=-32602,
            http_status=404,
            title="Not found",
        ),
        _row(
            "configuration_error",
            retryable=False,
            mcp_code=-32602,
            http_status=400,
            title="Configuration error",
        ),
        _row(
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
            mcp_code=-32008,
            http_status=507,
            title="Storage safety violation",
        ),
        _row(
            "observation_timeout",
            retryable=True,
            mcp_code=-32002,
            http_status=504,
            title="Observation timeout",
        ),
        _row(
            "launcher_resolution_failed",
            retryable=False,
            mcp_code=-32003,
            http_status=409,
            title="Launcher resolution failed",
        ),
        _row(
            # The second verification-pass gap (doc §6.3): OwnerSessionIdentityError
            # (job_identity.py:39-40+), already caught by name at
            # http_api.py:1158-1159 and :2965 and mapped to HTTP 422 today.
            "owner_session_identity_refused",
            retryable=False,
            mcp_code=-32004,
            http_status=422,
            title="Owner session identity refused",
        ),
        _row(
            "internal_error",
            retryable=False,
            mcp_code=-32603,
            http_status=500,
            title="Internal error",
        ),
    )
}


def _empty_data() -> Mapping[str, Any]:
    return {}


@dataclass(frozen=True, slots=True)
class RelayFault:
    """The classified shape every door_errors adapter renders from.

    Every field traces back to one :data:`REASONS` row (via :func:`classify`)
    except ``message`` (T1-bounded, §6.4) and ``data`` (reason-specific
    extension payload, e.g. a ``task_id`` or a storage decision).
    """

    reason: str
    retryable: bool
    mcp_code: int
    http_status: int
    title: str
    message: str
    data: Mapping[str, Any] = field(default_factory=_empty_data)


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


def _bounded_text(text: str) -> str:
    """Hard-truncate refusal/detail text to the T1 budget (doc §6.4)."""
    return text[:MAX_MESSAGE_CHARS]


def _typed_data(exc: BaseException) -> JSON:
    """Return the reason-specific extension payload a typed exception carries."""
    if isinstance(exc, StorageRuntimeError):
        return {"storage_decision": exc.decision.to_dict()}
    if isinstance(exc, OwnerSessionIdentityError):
        return dict(exc.detail)
    return {}


def _build(spec: ReasonSpec, *, message: str, data: Mapping[str, Any]) -> RelayFault:
    return RelayFault(
        reason=spec.reason,
        retryable=spec.retryable,
        mcp_code=spec.mcp_code,
        http_status=spec.http_status,
        title=spec.title,
        message=_bounded_text(message),
        data=data,
    )


def classify(
    exc: BaseException | JarvisDispatchRefusal,
    *,
    reason: str | None = None,
    message: str | None = None,
    data: Mapping[str, Any] | None = None,
    table: Mapping[str, ReasonSpec] = REASONS,
) -> RelayFault:
    """Classify one caught exception (or a carried refusal) into a :class:`RelayFault`.

    Args:
        exc: The caught exception, or a durable
            :class:`JarvisDispatchRefusal` a ``jarvis_run`` result carries
            (never raised-and-caught -- see dispatch rule 1 in the module
            docstring).
        reason: A call-path-scope override (dispatch rule 2). Must already be
            a key in ``table``; an unregistered reason is a caller bug, not a
            silent fallback, and raises :class:`ValueError`.
        message: Overrides the default ``str(exc)`` (or, for a refusal,
            ``exc.message``) wire message. Used by call sites whose typed
            message must never depend on an arbitrary underlying exception's
            text (e.g. ``mcp_task_status_reconciliation_failed``'s fixed,
            handler-internals-free message).
        data: Overrides the default reason-specific extension payload
            (:func:`_typed_data`, or ``{}``).
        table: The ``REASONS`` mapping to classify against. Overridable only
            so tests can prove :func:`as_mcp_error`/:func:`as_http_problem`
            read their codes from the table rather than duplicating it.

    Returns:
        The classified :class:`RelayFault`. Falls back to ``internal_error``
        (dispatch rule 4) when nothing else matches, logging the traceback
        exactly once via ``logger.exception`` and never placing exception
        internals on the wire.
    """
    if isinstance(exc, JarvisDispatchRefusal):
        spec = table["jarvis_dispatch_refused"]
        return _build(
            spec,
            message=message if message is not None else exc.message,
            data=data
            if data is not None
            else {
                "code": exc.code,
                "pipeline_id": exc.pipeline_id,
                "execution_id": exc.execution_id,
            },
        )
    if reason is not None:
        if reason not in table:
            msg = f"door_errors.classify(): {reason!r} is not a registered REASONS entry"
            raise ValueError(msg)
        return _build(
            table[reason],
            message=message if message is not None else str(exc),
            data=data if data is not None else {},
        )
    for exc_type, mapped_reason in _TYPE_REASONS:
        if isinstance(exc, exc_type):
            return _build(
                table[mapped_reason],
                message=message if message is not None else str(exc),
                data=data if data is not None else _typed_data(exc),
            )
    logger.exception(
        "clio-relay: door_errors.classify() found no registered reason for %s",
        type(exc).__name__,
    )
    return _build(
        table["internal_error"],
        message="relay encountered an internal error.",
        data={},
    )


def as_mcp_error(fault: RelayFault) -> MCPError:
    """Render a :class:`RelayFault` as the MCP wire shape.

    ``data`` always carries ``reason`` (the queryable, frozen-vocabulary
    signal) merged with whatever reason-specific payload :func:`classify`
    attached.
    """
    payload: JSON = {"reason": fault.reason, **fault.data}
    return MCPError(code=fault.mcp_code, message=fault.message, data=payload)


def _document_bytes(document: JSON) -> int:
    return len(json.dumps(document, separators=(",", ":")).encode("utf-8"))


def _bounded_document(document: JSON) -> JSON:
    """Enforce the whole-envelope ≤8KiB budget (doc §6.4), dropping evidence then truncation.

    The RFC 7807 core four and ``reason``/``retryable`` are never dropped. If
    the document is still oversized after both optional members are gone,
    that is an oversized ``message``/``data`` at the raise site, not
    something this function silently papers over further.
    """
    if _document_bytes(document) <= MAX_ENVELOPE_BYTES:
        return document
    shrunk = dict(document)
    for key in _DROP_ORDER:
        if key not in shrunk:
            continue
        del shrunk[key]
        if _document_bytes(shrunk) <= MAX_ENVELOPE_BYTES:
            return shrunk
    return shrunk


def as_http_problem(fault: RelayFault) -> JSON:
    """Render a :class:`RelayFault` as an RFC 7807 ``application/problem+json`` document.

    The doc §6.3 worked example's shape: ``type``/``title``/``status``/
    ``detail`` are the RFC 7807 core four; ``schema_version``/``reason``/
    ``retryable`` are this contract's load-bearing extension members (never
    dropped); ``truncation`` is the T3 companion record, ``null`` when
    nothing was elided (§6.4, not yet populated by any R3 call site -- R6
    scope). Anything :func:`classify` attached via ``fault.data`` (a
    ``task_id``, a storage decision, ...) rides along as additional
    extension members, subject to the same ≤8KiB budget.
    """
    document: JSON = {
        "type": f"urn:clio-relay:error:{fault.reason}",
        "title": fault.title,
        "status": fault.http_status,
        "detail": fault.message,
        "schema_version": SCHEMA_VERSION,
        "reason": fault.reason,
        "retryable": fault.retryable,
        "truncation": None,
        **fault.data,
    }
    return _bounded_document(document)


def as_browser_gateway_error(fault: RelayFault) -> tuple[int, JSON]:
    """Render a :class:`RelayFault` onto ``CapabilityProxyHandler._error``'s two-arg shape.

    ``browser_gateway.py``'s ``_error(self, status: int, message: str)`` is
    the fourth error surface (doc §6.1) and the one with the least existing
    structure: a bare ``{"error": message}`` dict with no ``code``/``data``/
    ``reason``/``detail`` field at all. This adapter reuses the same RFC 7807
    document :func:`as_http_problem` already builds for the HTTP surface --
    one shape across the two HTTP-shaped surfaces -- paired with
    ``fault.http_status`` so a call site can hand both straight to a
    document-aware sibling of ``_error``.
    """
    return fault.http_status, as_http_problem(fault)
