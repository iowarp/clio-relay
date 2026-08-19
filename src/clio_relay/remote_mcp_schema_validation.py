"""Bounded JSON / JSON-Schema validation primitives for remote MCP virtualization.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns the
generic, domain-agnostic guards that every other remote-MCP concern in that
file leans on before trusting untrusted JSON pulled from a cluster-side
discovery artifact or a tool call result: bounded-depth/node-count structural
walks, non-finite-number rejection, JSON-Schema dialect dispatch, and bounded
diagnostic rendering for error messages that must never grow unbounded from
attacker-controlled input.

:mod:`clio_relay.remote_mcp` imports every name here directly (these are all
private helpers with no external callers -- confirmed by grep across
``src/`` and ``tests/`` before the move -- so no re-export is needed there).
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Protocol, cast

from jsonschema import (
    Draft3Validator,
    Draft4Validator,
    Draft6Validator,
    Draft7Validator,
    Draft201909Validator,
    Draft202012Validator,
)
from jsonschema.exceptions import SchemaError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

JSON = dict[str, object]

# Structural bounds on untrusted JSON pulled from remote discovery artifacts
# and tool call results, applied before any recursive validator or
# transformation runs over the data.
MAX_REMOTE_MCP_JSON_DEPTH = 64
MAX_REMOTE_MCP_JSON_NODES = 100_000
MAX_REMOTE_MCP_DIAGNOSTIC_CHARS = 4_096

_COMPOSED_SCHEMA_KEYS = {
    "$dynamicRef",
    "$recursiveRef",
    "$ref",
    "allOf",
    "anyOf",
    "else",
    "if",
    "oneOf",
    "not",
    "then",
}
_FLAT_SCHEMA_KEYS = {
    "$comment",
    "$defs",
    "$id",
    "$schema",
    "additionalProperties",
    "default",
    "definitions",
    "deprecated",
    "description",
    "examples",
    "properties",
    "readOnly",
    "required",
    "title",
    "type",
    "writeOnly",
}
_JSON_SCHEMA_VALIDATORS = {
    str(validator.META_SCHEMA.get("$id") or validator.META_SCHEMA.get("id")).rstrip("#"): validator
    for validator in (
        Draft3Validator,
        Draft4Validator,
        Draft6Validator,
        Draft7Validator,
        Draft201909Validator,
        Draft202012Validator,
    )
}
_JSON_SCHEMA_VALIDATORS.update(
    {
        dialect.replace("http://", "https://", 1): validator
        for dialect, validator in tuple(_JSON_SCHEMA_VALIDATORS.items())
        if dialect.startswith("http://")
    }
)


class _NonFiniteJsonError(ValueError):
    """Non-standard NaN or infinity token in a purported JSON artifact."""


class _JsonSchemaInstanceValidator(Protocol):
    """Typed subset of a jsonschema validator used for instance checks."""

    def iter_errors(self, instance: object) -> Iterable[JsonSchemaValidationError]:
        """Yield every schema violation observed in one JSON-compatible instance."""
        ...


def _validate_json_schema(schema: JSON, *, label: str) -> None:
    """Reject malformed or unsupported JSON Schema contracts at ingestion."""
    _require_bounded_json_structure(schema, label=label)
    declared_dialect = schema.get("$schema")
    if isinstance(declared_dialect, str):
        normalized_dialect = declared_dialect.rstrip("#")
        validator = _JSON_SCHEMA_VALIDATORS.get(normalized_dialect)
        if validator is None:
            raise ValueError(f"remote MCP {label} declares an unsupported JSON Schema dialect")
    else:
        validator = Draft202012Validator
    try:
        validator.check_schema(schema)
    except RecursionError as exc:
        raise ValueError(
            f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_DEPTH} nesting levels"
        ) from exc
    except SchemaError as exc:
        raise ValueError(
            f"remote MCP {label} is not valid JSON Schema: " + _bounded_diagnostic(exc.message)
        ) from exc


def _require_bounded_json_structure(value: object, *, label: str) -> None:
    """Bound untrusted JSON before recursive validators or transformations run."""
    stack: list[tuple[object, int]] = [(value, 0)]
    node_count = 0
    while stack:
        current, depth = stack.pop()
        node_count += 1
        if node_count > MAX_REMOTE_MCP_JSON_NODES:
            raise ValueError(f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_NODES} JSON nodes")
        if depth > MAX_REMOTE_MCP_JSON_DEPTH:
            raise ValueError(
                f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_DEPTH} nesting levels"
            )
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in cast(dict[object, object], current).values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in cast(list[object], current))


def _require_finite_json(value: object, *, label: str) -> None:
    """Reject non-finite numbers that cannot round-trip through strict JSON."""
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, float) and not math.isfinite(current):
            raise ValueError(f"remote MCP {label} contains a non-finite JSON number")
        if isinstance(current, dict):
            stack.extend(cast(dict[object, object], current).values())
        elif isinstance(current, list):
            stack.extend(cast(list[object], current))


def _bounded_diagnostic(value: object) -> str:
    """Render an untrusted diagnostic without allowing unbounded error output."""
    rendered = value if isinstance(value, str) else repr(value)
    if len(rendered) <= MAX_REMOTE_MCP_DIAGNOSTIC_CHARS:
        return rendered
    return rendered[:MAX_REMOTE_MCP_DIAGNOSTIC_CHARS] + "... [truncated]"


def _reject_nonfinite_json_constant(value: str) -> None:
    """Reject NaN and infinity tokens accepted by Python's permissive decoder."""
    raise _NonFiniteJsonError(
        f"remote MCP discovery artifact contains non-finite JSON token: {value}"
    )
