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
This module is the single owner: :func:`classify`
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

See docs/design/relay-architecture-2026-08.md §6.3 for the remaining ten
rows' grounding (already-shipped precedents, verified raise sites, the two
reasons this design pass's own verification found beyond the seed ten, and
``payload_too_large``, added by the R3 re-review that also moved every
relay-owned MCP code out of the SDK's reserved -32000..-32019 band).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final

import mcp_types
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

#: Schema tag for the T1 refusal-text truncation record (doc §6.4). Reuses
#: the ``clio-relay.truncation.v1`` name R6's T3 record-time bound is also
#: scoped to use -- both describe the identical elision shape, just at
#: different budgets (2,000 chars here vs. head+tail byte windows there).
TRUNCATION_SCHEMA_VERSION: Final = "clio-relay.truncation.v1"

# T1 (doc §6.4): refusal/detail text budget, hard-truncated. A fourth
# independent literal agreeing with jarvis_dispatch_failure.py's
# MAX_REFUSAL_MESSAGE_CHARS, control_channel.py's
# MAX_CHANNEL_EVENT_DETAIL_CHARS, and remote_connection.py's inline
# [:2_000] slice -- R6 is scoped to name one shared constant instead of the
# four that now independently agree (§6.4).
MAX_MESSAGE_CHARS: Final = 2_000

# Whole-envelope budget (doc §6.4 T1): as_http_problem drops "evidence" then
# "truncation", then any other extension member, before ever touching the
# RFC 7807 core four (type/title/status/detail) or reason/retryable -- those
# are never dropped (F3: silently exceeding this budget is forbidden; see
# _bounded_document's "detail" truncation + "envelope_overflow" backstop).
MAX_ENVELOPE_BYTES: Final = 8 * 1024

_DROP_ORDER: Final[tuple[str, ...]] = ("evidence", "truncation")

#: RFC 7807 core four plus this contract's load-bearing extension members --
#: never dropped by :func:`_bounded_document`, never overwritten by
#: ``fault.data`` (F4: a colliding data key must not shadow a contract field).
_PROTECTED_KEYS: Final[frozenset[str]] = frozenset(
    {"type", "title", "status", "detail", "schema_version", "reason", "retryable"}
)


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


# F1 (opus review): the MCP SDK reserves -32000..-32019 for its OWN
# transport-level codes (mcp_types.jsonrpc.CONNECTION_CLOSED=-32000,
# .REQUEST_TIMEOUT=-32001, .HEADER_MISMATCH=-32020's neighbors, etc.) --
# relay's own custom codes MUST NOT land in that band. A client that
# discriminates by code alone (not this contract's typed ``reason``, e.g.
# clio-agent's tools/mcp_errors.py) would read relay's own
# mcp_task_input_park_conflict (formerly -32001) as the SDK's
# REQUEST_TIMEOUT and retry it with timeout semantics -- silently wrong.
# Relay-owned custom codes live in -32050..-32059 instead, a band the SDK
# does not use. -32007 (storage_admission_refused) is the one deliberate
# exception: it is already shipped and pinned
# (mcp_server.py's StorageAdmissionError handler,
# tests/test_production_admin_surfaces.py's ``denied["error"]["code"] ==
# -32007`` assertion) -- renumbering a live, tested wire value is a
# separate, riskier change than reallocating five never-shipped codes this
# same slice introduced.
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
    """
    truncated = text[:MAX_MESSAGE_CHARS]
    if truncated == text:
        return truncated, None
    original_bytes = len(text.encode("utf-8"))
    retained_bytes = len(truncated.encode("utf-8"))
    elided_bytes = original_bytes - retained_bytes
    record: JSON = {
        "schema_version": TRUNCATION_SCHEMA_VERSION,
        "truncated": True,
        "retention": "head",
        "original_bytes": original_bytes,
        "retained_head_bytes": retained_bytes,
        "retained_tail_bytes": 0,
        "elided_bytes": elided_bytes,
        "marker": f"[clio-relay: elided {elided_bytes} bytes of message]",
        "evidence_ref": None,
    }
    return truncated, record


def _safe_str(exc: BaseException) -> str:
    """Render ``str(exc)`` without letting a hostile ``__str__`` escape (F5).

    A raising ``__str__`` (or one that returns something ``str()`` itself
    cannot coerce) must never propagate out of :func:`classify` -- on the
    HTTP surface that would collapse past ``door_errors`` entirely into
    Starlette's ``ServerErrorMiddleware``, replacing the original exception
    with a new, undiagnosable one and losing the typed response this module
    exists to guarantee.
    """
    try:
        return str(exc)
    except Exception:
        logger.exception(
            "clio-relay: %s.__str__() raised; door_errors could not render its message",
            type(exc).__name__,
        )
        return f"<{type(exc).__name__}: message unavailable>"


def _typed_data(exc: BaseException) -> JSON:
    """Return the reason-specific extension payload a typed exception carries.

    Guarded (F5): a malformed instance of one of the seven typed exceptions
    (e.g. a subclass that overrides ``.decision``/``.detail`` with something
    that raises on access) must degrade to no extension payload, never crash
    classification.
    """
    try:
        if isinstance(exc, StorageRuntimeError):
            return {"storage_decision": exc.decision.to_dict()}
        if isinstance(exc, OwnerSessionIdentityError):
            return dict(exc.detail)
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
        message: Overrides the default ``str(exc)`` (or, for a refusal,
            ``exc.message``) wire message. Used by call sites whose typed
            message must never depend on an arbitrary underlying exception's
            text (e.g. ``mcp_task_status_reconciliation_failed``'s fixed,
            handler-internals-free message). ``str(exc)`` itself is never
            called unguarded (F5) -- a hostile ``__str__`` degrades to a
            generic placeholder rather than escaping classification.
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
        Exception internals never reach the wire either way.
    """
    if isinstance(exc, JarvisDispatchRefusal):
        spec = _table["jarvis_dispatch_refused"]
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
        if reason not in _table:
            msg = f"door_errors.classify(): {reason!r} is not a registered REASONS entry"
            raise ValueError(msg)
        return _build(
            _table[reason],
            message=message if message is not None else _safe_str(exc),
            data=data if data is not None else {},
        )
    for exc_type, mapped_reason in _TYPE_REASONS:
        if isinstance(exc, exc_type):
            return _build(
                _table[mapped_reason],
                message=message if message is not None else _safe_str(exc),
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


def as_mcp_error(fault: RelayFault) -> MCPError:
    """Render a :class:`RelayFault` as the MCP wire shape.

    ``data`` always carries ``reason`` (the queryable, frozen-vocabulary
    signal) merged with whatever reason-specific payload :func:`classify`
    attached -- ``fault.data`` first, ``reason`` applied on top (F4's same
    contract-wins discipline as :func:`as_http_problem`), so a colliding
    ``"reason"`` key in ``fault.data`` can never shadow the real one.
    """
    payload: JSON = {**fault.data, "reason": fault.reason}
    return MCPError(code=fault.mcp_code, message=fault.message, data=payload)


def _document_bytes(document: JSON) -> int | None:
    """Return the document's UTF-8 JSON-encoded byte length, or ``None`` if unmeasurable.

    ``ensure_ascii=False`` (F10): measures the actual wire encoding. The
    default ``ensure_ascii=True`` inflates every non-ASCII character to a
    6-byte ``\\uXXXX`` escape, materially overstating cost for ordinary
    non-ASCII ``detail``/``data`` text and mismeasuring the real budget.

    ``None`` (F5) means ``json.dumps`` could not serialize something in
    ``document`` (a non-JSON-safe value someone put in ``fault.data``) --
    every caller treats that identically to "over budget," so the drop
    order below still proceeds and removes the offending member instead of
    crashing.
    """
    try:
        payload = json.dumps(document, separators=(",", ":"), ensure_ascii=False)
    except TypeError:
        return None
    return len(payload.encode("utf-8"))


def _fits_budget(document: JSON) -> bool:
    size = _document_bytes(document)
    return size is not None and size <= MAX_ENVELOPE_BYTES


def _bounded_document(document: JSON) -> JSON:
    """Enforce the whole-envelope ≤8KiB budget (doc §6.4).

    Drop order: ``"evidence"`` first, then ``"truncation"`` (§6.4's named
    precedent), then any OTHER extension member in a deterministic (sorted)
    order -- F3: a single oversized or non-serializable extension value that
    is not literally named ``"evidence"`` must not sail through unbounded.
    Only once every extension member is gone does this fall back to
    truncating ``"detail"`` itself and stamping ``envelope_overflow: true``;
    the RFC 7807 core four plus ``reason``/``retryable`` are never dropped,
    and ``detail`` is never reduced to an empty string. Silent pass-through
    of an oversized document is forbidden.
    """
    if _fits_budget(document):
        return document
    shrunk = dict(document)
    for key in _DROP_ORDER:
        if key not in shrunk:
            continue
        del shrunk[key]
        if _fits_budget(shrunk):
            return shrunk
    for key in sorted(set(shrunk) - _PROTECTED_KEYS):
        del shrunk[key]
        if _fits_budget(shrunk):
            return shrunk
    # Every optional/extension member is gone; the core seven alone still
    # exceed budget (or one of them, in practice only "detail", could not be
    # measured). Truncate "detail" directly and record the overflow rather
    # than pass an oversized or unmeasurable document through.
    shrunk["envelope_overflow"] = True
    overhead = _document_bytes({**shrunk, "detail": ""}) or MAX_ENVELOPE_BYTES
    budget_for_detail = max(MAX_ENVELOPE_BYTES - overhead, 0)
    detail_text = shrunk.get("detail")
    detail_bytes = detail_text.encode("utf-8") if isinstance(detail_text, str) else b""
    shrunk["detail"] = detail_bytes[:budget_for_detail].decode("utf-8", errors="ignore") or "…"
    return shrunk


def as_http_problem(fault: RelayFault) -> JSON:
    """Render a :class:`RelayFault` as an RFC 7807 ``application/problem+json`` document.

    The doc §6.3 worked example's shape: ``type``/``title``/``status``/
    ``detail`` are the RFC 7807 core four; ``schema_version``/``reason``/
    ``retryable`` are this contract's load-bearing extension members (never
    dropped); ``truncation`` is the T1 elision record :func:`_bounded_text`
    populated, or ``null`` when nothing was elided (§6.4). Anything
    :func:`classify` attached via ``fault.data`` (a ``task_id``, a storage
    decision, ...) rides along as additional extension members, subject to
    the same ≤8KiB budget -- but F4: ``fault.data`` is spread FIRST and the
    contract members are applied on top, so a colliding data key (e.g. a
    stray ``"status"``) can never shadow a contract field.
    """
    document: JSON = {
        **fault.data,
        "type": f"urn:clio-relay:error:{fault.reason}",
        "title": fault.title,
        "status": fault.http_status,
        "detail": fault.message,
        "schema_version": SCHEMA_VERSION,
        "reason": fault.reason,
        "retryable": fault.retryable,
        "truncation": fault.truncation,
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
