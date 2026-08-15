"""The shared owner for tiered byte-budget enforcement (#231, R6).

``docs/design/relay-architecture-2026-08.md`` §6.4 is the governing design:
three independent tiers, each with its own budget shape --

* **T1 -- refusal/detail text.** Small, hard-truncated, in-band marker.
  :func:`build_truncation_record` is the ``clio-relay.truncation.v1`` record
  builder every tier below shares -- ``door_errors.py``'s R3-landed
  ``_bounded_text`` (the char-count refusal-text case, §6.3) now calls
  through here instead of building the record inline, so the schema has
  exactly one owner (ground rule 1) instead of a second copy drifting
  alongside it.
* **T2 -- agent-parsed payload.** Never truncated; overflow is a typed
  delivery-failure document instead. :func:`build_delivery_refusal` and
  :func:`is_delivery_refusal` generalize the precedent ``mcp_server.py``'s
  ``_bounded_mcp_result`` originated (``MAX_INLINE_MCP_RESULT_BYTES`` stays
  local there; its own former ``MCP_RESULT_DELIVERY_SCHEMA`` literal was
  retired in the F7 review-fix migration -- ``_bounded_mcp_result`` now
  imports and calls :func:`build_delivery_refusal` directly, since
  ``mcp_server.py`` already imports ``relay_ops.read_artifact_bytes``, one
  of this module's own T2 consumers, so the reverse import here would
  cycle).
* **T3 -- durable operator evidence.** Read bounds stay generous (doc's own
  correction: narrowing a read-time cap breaks chatty-server protocol
  parses); :func:`bound_stream_capture` is the record-time head+tail bound
  applied when a durable document is *built*, not when it is read.

Every raw payload path this module bounds picks the tier that matches what
it is bounding, not the other way around -- T1 is for a short human-facing
message, T2 is for a document an agent parses field-by-field, T3 is for a
long-running process's captured stdout/stderr kept as durable evidence.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final, Literal, cast, overload

JSON = dict[str, Any]

Retention = Literal["head", "tail", "head_tail"]

#: Schema tag for the T1/T3 elision record (doc §6.4). ``door_errors.py``
#: re-exports this name (``from clio_relay.bounded_payload import
#: TRUNCATION_SCHEMA_VERSION``) rather than defining its own copy -- single
#: owner, one schema tag regardless of which tier or module builds a record.
TRUNCATION_SCHEMA_VERSION: Final = "clio-relay.truncation.v1"

#: T2 (doc §6.4) delivery-failure schema for a payload that would otherwise
#: need mid-body truncation. Originated as ``mcp_server.py``'s own
#: ``MCP_RESULT_DELIVERY_SCHEMA`` precedent; that local literal is retired
#: (F7, #231 R6 review) -- ``mcp_server.py``'s ``_bounded_mcp_result`` now
#: imports this constant (via :func:`build_delivery_refusal`) instead of
#: carrying an independent copy. Single owner, one schema tag, no second
#: vocabulary.
DELIVERY_FAILURE_SCHEMA_VERSION: Final = "clio-relay.mcp-result-delivery.v1"

# T1 (doc §6.4): byte-bounded tail-retention budget for raw-payload paths
# that select a byte window directly (frp_check.py's frpc failure detail) as
# opposed to door_errors.py's char-count MAX_MESSAGE_CHARS (its own budget,
# left in place -- unifying every T1 site into one literal is named in the
# doc's own §6.4 prose but is not this slice's stated scope, which is the
# three raw payload paths in §6.4/§6.5, not the four already-agreeing
# refusal-text literals).
T1_TEXT_MAX_BYTES: Final = 2_000

# T3 (doc §6.4) record-time head+tail retention defaults.
STDOUT_HEAD_MAX_BYTES: Final = 1024 * 1024
STDOUT_TAIL_MAX_BYTES: Final = 1024 * 1024
STDERR_HEAD_MAX_BYTES: Final = 256 * 1024
STDERR_TAIL_MAX_BYTES: Final = 256 * 1024


def build_truncation_record(
    *,
    retention: Retention,
    original_bytes: int,
    retained_head_bytes: int = 0,
    retained_tail_bytes: int = 0,
    stream: str = "message",
    evidence_ref: str | None = None,
) -> JSON:
    """Build one populated ``clio-relay.truncation.v1`` record (doc §6.4).

    Callers that already know nothing was elided should not call this at
    all -- every field here describes an actual cut (``truncated`` is always
    ``True``); the ``None`` case a caller reports instead lives in each
    tier's own helper (:func:`bound_stream_capture` returns ``None`` when
    the input already fits, door_errors' ``_bounded_text`` does the same for
    an under-budget message).

    Args:
        retention: Which side(s) of the original content survive --
            ``"head"``, ``"tail"``, or ``"head_tail"``.
        original_bytes: The pre-truncation size, in bytes.
        retained_head_bytes: Bytes kept from the start. ``0`` when
            ``retention`` doesn't include the head.
        retained_tail_bytes: Bytes kept from the end. ``0`` when
            ``retention`` doesn't include the tail.
        stream: Named in the in-band marker (``"stdout"``, ``"stderr"``,
            ``"message"``, ...).
        evidence_ref: An artifact-id reference to a durable copy of the
            elided bytes, when one exists. ``None`` when nothing else
            preserves what was cut -- never a fabricated reference.

    Returns:
        The ``clio-relay.truncation.v1`` record, including the full-line
        in-band marker ``"[clio-relay: elided N bytes of <stream>]"``.
    """
    elided_bytes = original_bytes - retained_head_bytes - retained_tail_bytes
    return {
        "schema_version": TRUNCATION_SCHEMA_VERSION,
        "truncated": True,
        "retention": retention,
        "original_bytes": original_bytes,
        "retained_head_bytes": retained_head_bytes,
        "retained_tail_bytes": retained_tail_bytes,
        "elided_bytes": elided_bytes,
        "marker": f"[clio-relay: elided {elided_bytes} bytes of {stream}]",
        "evidence_ref": evidence_ref,
    }


@overload
def bound_stream_capture(
    data: str,
    *,
    head_max: int,
    tail_max: int,
    stream_name: str,
) -> tuple[str, JSON | None]: ...


@overload
def bound_stream_capture(
    data: bytes,
    *,
    head_max: int,
    tail_max: int,
    stream_name: str,
) -> tuple[bytes, JSON | None]: ...


def bound_stream_capture(
    data: bytes | str,
    *,
    head_max: int,
    tail_max: int,
    stream_name: str,
) -> tuple[bytes | str, JSON | None]:
    """Bound one durable text/byte capture to a head+tail retention window (T3).

    Applied at RECORD time -- when a durable document is being *built* --
    never at READ time: the doc's own correction against an earlier working
    hint is that narrowing a read-time cap breaks chatty-server protocol
    parses (§6.4/§6.5), so callers must keep their existing generous read
    bounds and apply this only where the already-read content is about to be
    written into a durable record.

    Args:
        data: The captured content, ``bytes`` or ``str``. The return type
            matches the input type.
        head_max: Bytes retained from the start. ``0`` means nothing from
            the head is kept (a pure tail retention).
        tail_max: Bytes retained from the end. ``0`` means nothing from the
            tail is kept (a pure head retention).
        stream_name: Named in the in-band marker and the record's
            ``marker``/context (``"stdout"``, ``"stderr"``, ...).

    Returns:
        ``(data, None)`` unchanged when ``data`` already fits within
        ``head_max + tail_max`` bytes. Otherwise, the retained content --
        head bytes, then the in-band marker on its own line, then tail
        bytes -- plus a populated truncation record. Decoding a ``str``
        result uses ``errors="replace"`` at the cut boundary: the head/tail
        split happens on raw bytes, which is not guaranteed to land on a
        UTF-8 character boundary, and this is diagnostic evidence, not a
        byte-exact round trip -- a character straddling the cut renders as
        U+FFFD (the replacement character), never a decode crash or silent
        corruption.

    Raises:
        ValueError: If ``head_max`` or ``tail_max`` is negative, or both are
            ``0`` (#231 R6 review, F9) -- retaining nothing from either side
            has no correct ``retention`` label to report (neither ``"head"``,
            ``"tail"``, nor ``"head_tail"`` is honest), so this is refused as
            a caller bug rather than silently mislabeled.
    """
    if head_max < 0 or tail_max < 0:
        raise ValueError("bound_stream_capture: head_max and tail_max must be non-negative")
    if head_max == 0 and tail_max == 0:
        raise ValueError(
            "bound_stream_capture: head_max and tail_max cannot both be 0 -- "
            "retaining nothing from either side has no correct retention label"
        )
    is_text = isinstance(data, str)
    raw = data.encode("utf-8") if is_text else data
    original_bytes = len(raw)
    if original_bytes <= head_max + tail_max:
        return data, None
    head = raw[:head_max] if head_max else b""
    tail = raw[len(raw) - tail_max :] if tail_max else b""
    if head_max and tail_max:
        retention: Retention = "head_tail"
    elif head_max:
        retention = "head"
    else:
        retention = "tail"
    record = build_truncation_record(
        retention=retention,
        original_bytes=original_bytes,
        retained_head_bytes=len(head),
        retained_tail_bytes=len(tail),
        stream=stream_name,
    )
    marker = cast(str, record["marker"]).encode("utf-8")
    retained = b"\n".join(part for part in (head, marker, tail) if part)
    result: bytes | str = retained.decode("utf-8", errors="replace") if is_text else retained
    return result, record


def build_delivery_refusal(
    *,
    code: str,
    message: str,
    max_bytes: int,
    remote_side_effects_may_have_occurred: bool,
    private_evidence_preserved: bool = True,
) -> JSON:
    """Build a typed T2 over-budget refusal -- never a partial/truncated payload.

    Arbitrary agent-parsed payloads (an MCP tool result, a durable artifact
    body) have no generic, safe pagination or redaction contract: returning
    a successful-looking partial projection would lose the only
    agent-readable result, while exposing selected fields could disclose
    application-defined secrets. This is the shared shape every T2 raw path
    returns instead -- mirrors ``mcp_server.py``'s ``_bounded_mcp_result``
    failure document (doc §6.4) so an agent that has already learned to
    recognize one T2 refusal recognizes all of them.

    Args:
        code: A short, stable machine-readable refusal code
            (e.g. ``"inline_result_limit_exceeded"``,
            ``"artifact_content_too_large"``).
        message: A human-readable explanation of the refusal.
        max_bytes: The budget that was exceeded.
        remote_side_effects_may_have_occurred: Whether the operation that
            produced the oversized payload may have had side effects the
            caller cannot see or undo (an MCP tool call did; a local
            artifact read did not).
        private_evidence_preserved: Whether the full payload remains
            available to an operator through some other channel (durable
            artifact storage, a cursor-based log endpoint). Almost always
            ``True`` -- refusing delivery to the agent does not mean the
            evidence itself was discarded.

    Returns:
        ``{content_truncated: True, result_available: False, delivery: {...}}``.
    """
    return {
        "content_truncated": True,
        "result_available": False,
        "delivery": {
            "schema_version": DELIVERY_FAILURE_SCHEMA_VERSION,
            "status": "failed",
            "code": code,
            "max_inline_bytes": max_bytes,
            "private_evidence_preserved": private_evidence_preserved,
            "remote_side_effects_may_have_occurred": remote_side_effects_may_have_occurred,
            "message": message,
        },
    }


def is_delivery_refusal(document: Mapping[str, Any]) -> bool:
    """Whether ``document`` is a :func:`build_delivery_refusal` T2 refusal shape.

    Callers that receive a document from a T2-bounded path (e.g.
    ``relay_ops.read_artifact_bytes``) use this to branch before assuming
    the document carries a normal, fully-delivered payload.
    """
    delivery = document.get("delivery")
    return (
        document.get("result_available") is False
        and isinstance(delivery, dict)
        and cast(JSON, delivery).get("schema_version") == DELIVERY_FAILURE_SCHEMA_VERSION
    )


#: The fallback text every T2 refusal reader has historically hardcoded by
#: hand when a refusal document happened to omit its own ``delivery.message``
#: -- centralized in :func:`describe_delivery_refusal` (#231 R6 review, A2)
#: so a future refusal-message change never needs updating in five places.
_DELIVERY_REFUSAL_FALLBACK_MESSAGE = "artifact content exceeds the transfer limit"


def describe_delivery_refusal(document: Mapping[str, Any]) -> str:
    """The refusal's own human-readable message, with the shared fallback text.

    Every caller that SURFACES a delivery refusal's reason (as a CLI error,
    an HTTP problem detail, a ``ValueError``/``RelayError`` message) --
    not merely detects one, see :func:`is_delivery_refusal` -- uses this
    instead of re-deriving the same
    ``delivery.get("message", "artifact content exceeds the transfer
    limit")`` extraction independently. Five near-identical copies existed
    across ``cli.py``, ``http_api.py``, ``jarvis_service_runtime.py``,
    ``live_acceptance.py``, and ``remote_mcp.py``, each spelling the same
    fallback text by hand -- this is the single owner now (ground rule 1).

    Args:
        document: A document :func:`is_delivery_refusal` already verified
            ``True`` for.
    """
    delivery = cast(JSON, document.get("delivery", {}))
    return cast(str, delivery.get("message", _DELIVERY_REFUSAL_FALLBACK_MESSAGE))


def is_delivery_refusal_failed(document: object) -> bool:
    """Whether ``document`` is a T2 refusal (doc §6.4) with ``delivery.status == "failed"``.

    Promoted from ``mcp_server.py``'s private ``_delivery_refusal_failed``
    (#231 R6 review F1) to the single owner here (#231 R6 review, A4) so
    every FAILURE-discriminating caller -- not only the MCP tool-result
    boundary -- shares the one ``delivery.status`` check: a per-code match
    silently misses every OTHER typed refusal (e.g.
    ``artifact_content_too_large``, doc §6.5).
    """
    if not isinstance(document, dict):
        return False
    typed_document = cast(dict[str, object], document)
    if not is_delivery_refusal(typed_document):
        return False
    delivery = typed_document.get("delivery")
    if not isinstance(delivery, dict):
        return False
    return cast(dict[str, object], delivery).get("status") == "failed"


def parse_delivery_refusal(data: bytes) -> JSON | None:
    """Recognize a T2 delivery-refusal document (doc §6.4) in raw bytes.

    For a caller whose only failure signal is a subprocess exit code, not
    a return value that already carries a typed document -- a remote
    command's stdout (#231 R6 review, A1). A remote CLI guard (e.g.
    ``job read-artifact``) already exits non-zero *after* printing the
    refusal JSON; without this, a blanket non-zero-exit check discards
    that structure and reports a generic "remote command failed: <blob>"
    instead of the refusal's own typed code/message.

    Returns the parsed document only when it genuinely is a refusal shape
    (not merely valid JSON) -- never raises, so callers can use this as an
    unconditional first check before falling back to their own generic
    failure handling.
    """
    try:
        document = json.loads(data.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None
    if not isinstance(document, dict):
        return None
    typed_document = cast(JSON, document)
    if not is_delivery_refusal(typed_document):
        return None
    return typed_document
