"""Shared JSON/type primitives for the JARVIS MCP validation evidence builders.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): the ``JSON`` alias, the ``_UNBOUND_JARVIS_IDENTITY``
sentinel (a true module-level singleton every other split module imports
rather than redefines, so ``is _UNBOUND_JARVIS_IDENTITY`` identity checks stay
valid across module boundaries), and the small type-guard/lookup helpers every
other split module depends on (``_mapping``, ``_is_sha256``,
``_is_string_list``, ``_nonnegative_int``, ``_positive_int``, ``_listed_tool``,
``_response_job_id``, ``_check``, ``_stdio_initialize_passed``). None of these
are part of the facade's public surface -- every owner module that needs them
imports directly from here.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, TypeGuard, cast

from clio_relay.validation_report import (
    EvidenceReference,
    ValidationCheck,
    ValidationStatus,
)

JSON = dict[str, Any]
_UNBOUND_JARVIS_IDENTITY = object()


def _mapping(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.lower())
    )


def _is_string_list(value: object) -> TypeGuard[list[str]]:
    """Return whether a JSON value is a list containing only strings."""
    items = cast(list[object], value)
    return isinstance(value, list) and all(isinstance(item, str) for item in items)


def _nonnegative_int(value: object) -> TypeGuard[int]:
    """Return whether a value is a non-boolean, nonnegative integer."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _positive_int(value: object) -> TypeGuard[int]:
    """Return whether a value is a non-boolean, positive integer."""
    return _nonnegative_int(value) and value > 0


def _listed_tool(response: JSON | None, tool: str) -> JSON | None:
    if response is None or "error" in response:
        return None
    result = _mapping(response.get("result"))
    tools = result.get("tools") if result else None
    if not isinstance(tools, list):
        return None
    typed_tools = cast(list[object], tools)
    return next(
        (
            cast(JSON, item)
            for item in typed_tools
            if isinstance(item, dict) and cast(JSON, item).get("name") == tool
        ),
        None,
    )


def _response_job_id(response: JSON | None) -> str | None:
    if response is None or "error" in response:
        return None
    result = _mapping(response.get("result"))
    structured = _mapping(result.get("structuredContent")) if result else None
    if structured is not None and isinstance(structured.get("job_id"), str):
        return cast(str, structured["job_id"])
    content = result.get("content") if result else None
    if not isinstance(content, list):
        return None
    for item in cast(list[object], content):
        typed = _mapping(item)
        if typed is None or typed.get("type") != "text" or not isinstance(typed.get("text"), str):
            continue
        try:
            payload = cast(object, json.loads(cast(str, typed["text"])))
        except (TypeError, ValueError):
            continue
        typed_payload = _mapping(payload)
        if typed_payload is not None and isinstance(typed_payload.get("job_id"), str):
            return cast(str, typed_payload["job_id"])
    return None


def _check(
    check_id: str,
    summary: str,
    passed: bool,
    started_at: datetime,
    completed_at: datetime,
    metadata: JSON,
) -> ValidationCheck:
    return ValidationCheck(
        check_id=check_id,
        summary=summary,
        status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
        started_at=started_at,
        completed_at=completed_at,
        evidence=[
            EvidenceReference(
                kind="jarvis_mcp_acceptance",
                excerpt=summary,
                metadata=metadata,
            )
        ],
        error=None if passed else summary,
    )


def _stdio_initialize_passed(*, initialize_response: JSON | None, evidence: JSON | None) -> bool:
    if evidence is None:
        return True
    if (
        evidence.get("boundary") != "packaged_clio_relay_mcp_server_stdio"
        or evidence.get("returncode") != 0
        or initialize_response is None
        or initialize_response.get("error") is not None
    ):
        return False
    result = _mapping(initialize_response.get("result"))
    server_info = _mapping(result.get("serverInfo")) if result else None
    return (
        result is not None
        and isinstance(result.get("protocolVersion"), str)
        and server_info is not None
        and server_info.get("name") == "clio-relay"
    )
