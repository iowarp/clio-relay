"""Bounded JSON decoding, canonical digests, and diagnostic sanitization.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (clio-relay#231-style
file-size decomposition; see ``scripts/check_file_size.py``). This module
owns the leaf, domain-agnostic primitives every other packaged-stdio owner
module leans on: duplicate-key/non-finite-rejecting strict JSON decode,
canonical (sorted-key, ``allow_nan=False``) SHA-256 digests over JSON-shaped
evidence, and bounded, secret-redacting diagnostic rendering for untrusted
child-process stderr.

``decode_strict_json`` is re-exported from :mod:`clio_relay.mcp_stdio_validation`
under its original name -- ``live_acceptance_browser_http.py`` and
``live_acceptance_packaged_mcp.py`` import it from there, and
``tests/test_mcp_stdio_validation.py`` calls it directly on the facade module
object. The rest are private helpers with no external callers (confirmed by
grep across ``src/`` and ``tests/`` before the move), so no re-export is
needed for them.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Iterable
from typing import Any, cast

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import RelayError

JSON = dict[str, Any]

_DIAGNOSTIC_BYTES = 4_096
_SENSITIVE_DIAGNOSTIC = re.compile(
    r"(?i)\b(authorization|bearer|capability|credential|password|secret|token)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_BEARER_DIAGNOSTIC = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
# Shared with mcp_stdio_validation_process_io.py (packaged launch environment
# scrubbing) and mcp_stdio_validation_process.py (child-emitted secret
# detection): the single pattern that decides which environment variable
# NAMES are treated as carrying a private value, wherever that decision is
# made.
_SENSITIVE_ENVIRONMENT_NAME = re.compile(
    r"(?i)(authorization|auth|bearer|capability|credential|key|password|secret|token)"
)


def decode_strict_json(payload: bytes | str, *, label: str) -> object:
    """Decode duplicate-free UTF-8 JSON and reject every non-finite number."""
    failed = False
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        decoded = cast(
            object,
            json.loads(
                text,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite_json,
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        failed = True
        decoded = None
    if failed:
        raise RelayError(f"{label} contained invalid JSON") from None
    _reject_nested_nonfinite_json(decoded, label=label)
    return decoded


def _reject_nested_nonfinite_json(value: object, *, label: str) -> None:
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > 64:
            raise RelayError(f"{label} exceeded the JSON nesting limit")
        if isinstance(current, float) and not math.isfinite(current):
            raise RelayError(f"{label} contained a non-finite JSON number")
        if isinstance(current, dict):
            stack.extend(
                (nested, depth + 1) for nested in cast(dict[object, object], current).values()
            )
        elif isinstance(current, list):
            stack.extend((nested, depth + 1) for nested in cast(list[object], current))


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> JSON:
    result: JSON = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> None:
    raise ValueError(f"non-finite JSON value: {value}")


def _mapping(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None


def _canonical_digest(value: object) -> str:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("packaged MCP contract was not canonical JSON") from exc
    return hashlib.sha256(payload).hexdigest()


def _tools_digest(tools: list[JSON]) -> str:
    ordered = sorted(tools, key=lambda definition: cast(str, definition.get("name")))
    return _canonical_digest({"tools": ordered})


def _sanitized_diagnostic(
    stderr: bytes,
    *,
    forbidden_values: Iterable[str] = (),
) -> str:
    decoded = stderr.decode("utf-8", errors="replace")
    printable = "".join(
        character if character in "\n\r\t" or character.isprintable() else "?"
        for character in decoded
    )
    redacted = printable
    sensitive_values = {
        value
        for name, value in os.environ.items()
        if len(value) >= 8 and _SENSITIVE_ENVIRONMENT_NAME.search(name)
    }
    sensitive_values.update(value for value in forbidden_values if value)
    for value in sorted(sensitive_values, key=len, reverse=True):
        redacted = redacted.replace(value, "[redacted]")
    redacted = _BEARER_DIAGNOSTIC.sub("Bearer [redacted]", redacted)
    redacted = _SENSITIVE_DIAGNOSTIC.sub(r"\1\2[redacted]", redacted)
    bounded = bounded_error_detail(redacted)
    if bounded is None:
        return ""
    encoded = bounded.encode("utf-8")
    if len(encoded) <= _DIAGNOSTIC_BYTES:
        return bounded
    return encoded[:_DIAGNOSTIC_BYTES].decode("utf-8", errors="ignore")
