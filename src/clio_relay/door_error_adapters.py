"""Render classified relay faults onto MCP, HTTP, browser, and WebSocket surfaces.

This module is the cohesive wire-adapter owner split from ``door_errors`` by
the #231 no-accretion rule. Classification, the frozen reason registry, and
the fault model remain in ``door_errors``; these functions only project an
already-classified fault without choosing a new reason or status.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final, Protocol

from mcp.shared.exceptions import MCPError
from starlette.exceptions import WebSocketException

JSON = dict[str, Any]

SCHEMA_VERSION: Final = "clio-relay.error.v1"
MAX_ENVELOPE_BYTES: Final = 8 * 1024

_DROP_ORDER: Final[tuple[str, ...]] = ("evidence", "truncation")
_PROTECTED_KEYS: Final[frozenset[str]] = frozenset(
    {"type", "title", "status", "detail", "schema_version", "reason", "retryable"}
)


class RelayFaultLike(Protocol):
    """Structural fault fields required by every wire adapter."""

    @property
    def reason(self) -> str: ...

    @property
    def retryable(self) -> bool: ...

    @property
    def mcp_code(self) -> int: ...

    @property
    def http_status(self) -> int: ...

    @property
    def title(self) -> str: ...

    @property
    def message(self) -> str: ...

    @property
    def data(self) -> Mapping[str, Any]: ...

    @property
    def truncation(self) -> JSON | None: ...


def websocket_refusal(reason: str) -> WebSocketException:
    """Create a byte-bounded policy close carrying only a registered reason token."""
    from clio_relay.door_errors import REASONS

    if reason not in REASONS:
        raise ValueError(f"door_errors: {reason!r} is not a registered REASONS entry")
    if len(reason.encode("ascii")) > 123:
        raise ValueError("door_errors: WebSocket refusal reason exceeds 123 bytes")
    return WebSocketException(code=1008, reason=reason)


def as_mcp_error(fault: RelayFaultLike) -> MCPError:
    """Render a classified fault as the MCP wire shape.

    ``data`` always carries ``reason`` (the queryable, frozen-vocabulary
    signal) merged with whatever reason-specific payload classification
    attached. ``fault.data`` is spread first and ``reason`` applied on top,
    so a colliding extension key can never shadow the real reason.
    """
    payload: JSON = {**fault.data, "reason": fault.reason}
    return MCPError(code=fault.mcp_code, message=fault.message, data=payload)


def _document_bytes(document: JSON) -> int | None:
    """Return the document's UTF-8 JSON byte length, or ``None`` if unmeasurable.

    ``ensure_ascii=False`` measures the actual wire encoding. The default
    ``ensure_ascii=True`` inflates every non-ASCII character to a six-byte
    escape, materially overstating cost for ordinary public detail and
    extension data.

    ``None`` means ``json.dumps`` could not serialize an extension value.
    Every caller treats that identically to over-budget, so the deterministic
    drop order removes the offending member instead of crashing.
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
    """Enforce the whole-envelope 8 KiB budget (design §6.4).

    Drop ``evidence`` first, then ``truncation``, then any other extension
    member in deterministic sorted order. Only after all extension members
    are gone may ``detail`` itself be truncated and the document stamped
    ``envelope_overflow: true``. The RFC 7807 core fields plus
    ``reason``/``retryable`` are never dropped, and ``detail`` is never
    reduced to an empty string. Silent over-budget pass-through is forbidden.
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
    shrunk["envelope_overflow"] = True
    overhead = _document_bytes({**shrunk, "detail": ""}) or MAX_ENVELOPE_BYTES
    budget_for_detail = max(MAX_ENVELOPE_BYTES - overhead, 0)
    detail_text = shrunk.get("detail")
    detail_bytes = detail_text.encode("utf-8") if isinstance(detail_text, str) else b""
    shrunk["detail"] = detail_bytes[:budget_for_detail].decode("utf-8", errors="ignore") or "…"
    return shrunk


def as_http_problem(fault: RelayFaultLike) -> JSON:
    """Render a classified fault as a bounded RFC 7807 HTTP document.

    ``type``/``title``/``status``/``detail`` are the RFC 7807 core four;
    ``schema_version``/``reason``/``retryable`` are this contract's
    load-bearing extension members. ``truncation`` is the T1 elision record
    classification populated, or ``null`` when nothing was elided. Any
    reason-specific data rides as additional extension members subject to
    the same 8 KiB budget, but contract members are applied last so extension
    data cannot shadow them.
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
    if fault.reason == "owner_session_identity_refused":
        document["message"] = fault.message
    return _bounded_document(document)


def as_browser_gateway_error(fault: RelayFaultLike) -> tuple[int, JSON]:
    """Render a fault onto the browser gateway's two-argument response shape.

    The legacy gateway surface carried only ``{"error": message}``. This
    adapter reuses the same RFC 7807 document as the HTTP surface and pairs it
    with ``fault.http_status`` for the document-aware gateway responder.
    """
    return fault.http_status, as_http_problem(fault)
