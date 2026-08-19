"""Tests for the bounded JSON / JSON-Schema validation primitives (#231).

``clio_relay.remote_mcp_schema_validation`` is the owner module. Before this
slice, none of these primitives were tested directly by name -- every prior
test exercised them only indirectly through higher-level remote MCP paths
(``RemoteMcpToolSchema`` construction, ``cache_entry_from_discovery_artifact``)
in ``tests/test_remote_mcp.py``, which correctly stay there since they test
those higher-level contracts, not these generic guards. This file adds net-
new focused unit coverage for the primitives themselves.
"""

from __future__ import annotations

import pytest

from clio_relay.remote_mcp_schema_validation import (
    MAX_REMOTE_MCP_DIAGNOSTIC_CHARS,
    MAX_REMOTE_MCP_JSON_DEPTH,
    MAX_REMOTE_MCP_JSON_NODES,
    _bounded_diagnostic,
    _NonFiniteJsonError,
    _reject_nonfinite_json_constant,
    _require_bounded_json_structure,
    _require_finite_json,
    _validate_json_schema,
)


def test_bounded_json_structure_accepts_shallow_small_document() -> None:
    _require_bounded_json_structure({"a": [1, 2, {"b": "c"}]}, label="value")


def test_bounded_json_structure_rejects_excess_nesting() -> None:
    nested: object = 0
    for _ in range(MAX_REMOTE_MCP_JSON_DEPTH + 1):
        nested = {"nested": nested}

    with pytest.raises(ValueError, match="nesting levels"):
        _require_bounded_json_structure(nested, label="value")


def test_bounded_json_structure_rejects_excess_node_count() -> None:
    wide = {"items": list(range(MAX_REMOTE_MCP_JSON_NODES))}

    with pytest.raises(ValueError, match="JSON nodes"):
        _require_bounded_json_structure(wide, label="value")


def test_require_finite_json_accepts_finite_numbers() -> None:
    _require_finite_json({"a": 1.5, "b": [1, 2.0, "c"]}, label="value")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_require_finite_json_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError, match="non-finite JSON number"):
        _require_finite_json({"a": [value]}, label="value")


def test_bounded_diagnostic_passes_through_short_strings() -> None:
    assert _bounded_diagnostic("short message") == "short message"


def test_bounded_diagnostic_truncates_long_strings() -> None:
    rendered = _bounded_diagnostic("x" * (MAX_REMOTE_MCP_DIAGNOSTIC_CHARS + 500))

    assert rendered.endswith("... [truncated]")
    assert len(rendered) == MAX_REMOTE_MCP_DIAGNOSTIC_CHARS + len("... [truncated]")


def test_bounded_diagnostic_renders_non_string_values() -> None:
    assert _bounded_diagnostic({"a": 1}) == repr({"a": 1})


def test_reject_nonfinite_json_constant_raises_typed_error() -> None:
    with pytest.raises(_NonFiniteJsonError, match="NaN"):
        _reject_nonfinite_json_constant("NaN")


def test_validate_json_schema_accepts_valid_schema() -> None:
    _validate_json_schema({"type": "object", "properties": {"a": {"type": "string"}}}, label="s")


def test_validate_json_schema_rejects_unsupported_dialect() -> None:
    with pytest.raises(ValueError, match="unsupported JSON Schema dialect"):
        _validate_json_schema({"$schema": "https://example.com/not-a-real-dialect#"}, label="s")


def test_validate_json_schema_rejects_malformed_schema() -> None:
    with pytest.raises(ValueError, match="not valid JSON Schema"):
        _validate_json_schema({"type": "not-a-real-type"}, label="s")
