"""Tests for the local relay control envelope injection cluster (#231).

Two concerns:

1. **Extraction seam** -- ``clio_relay.remote_mcp_schema_wrapping`` is the
   owner module; ``clio_relay.remote_mcp`` must re-export
   ``inject_cluster_argument`` and ``VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS``
   under an identical binding so existing callers (tests, queue_tasks.py)
   keep resolving to the *same* object after the move.
2. **Private helper behavior** -- ``virtual_schema_error``,
   ``remote_input_schema_requires_wrapper``,
   ``_contains_document_root_reference``, ``_schema_identifier_keyword``,
   ``_schema_establishes_embedded_resource``, and
   ``_relocate_legacy_local_references`` were previously exercised only
   indirectly, through ``inject_cluster_argument``'s end-to-end output in
   ``tests/test_remote_mcp.py`` (which correctly stays there -- it tests
   that higher-level contract). This file adds net-new focused coverage
   for the helpers themselves.
"""

from __future__ import annotations

import clio_relay.remote_mcp as remote_mcp
from clio_relay.remote_mcp_schema_wrapping import (
    VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS,
    _contains_document_root_reference,
    _relocate_legacy_local_references,
    _schema_establishes_embedded_resource,
    _schema_identifier_keyword,
    inject_cluster_argument,
    remote_input_schema_requires_wrapper,
    virtual_schema_error,
)


def test_remote_mcp_reexports_are_identical_objects() -> None:
    assert remote_mcp.inject_cluster_argument is inject_cluster_argument
    assert (
        remote_mcp.VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
        is VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
    )


def test_virtual_schema_error_accepts_a_closed_flat_object_schema() -> None:
    schema = {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}
    assert virtual_schema_error(schema) is None


def test_virtual_schema_error_rejects_non_object_root_type() -> None:
    assert virtual_schema_error({"type": "string"}) == "remote inputSchema must have type object"


def test_virtual_schema_error_rejects_duplicate_required_entries() -> None:
    schema = {"type": "object", "required": ["path", "path"]}
    assert virtual_schema_error(schema) == "remote inputSchema required entries must be unique"


def test_requires_wrapper_for_open_additional_properties() -> None:
    schema = {"type": "object", "properties": {}, "additionalProperties": True}
    assert remote_input_schema_requires_wrapper(schema) is True


def test_requires_wrapper_false_for_closed_plain_schema() -> None:
    schema = {
        "type": "object",
        "properties": {"a": {"type": "string"}},
        "additionalProperties": False,
    }
    assert remote_input_schema_requires_wrapper(schema) is False


def test_requires_wrapper_for_declared_composition_keyword() -> None:
    schema = {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
        "allOf": [{"type": "object"}],
    }
    assert remote_input_schema_requires_wrapper(schema) is True


def test_requires_wrapper_when_property_collides_with_relay_control_field() -> None:
    schema = {
        "type": "object",
        "properties": {"idempotency_key": {"type": "string"}},
        "additionalProperties": False,
    }
    assert remote_input_schema_requires_wrapper(schema) is True


def test_contains_document_root_reference_detects_bare_hash_ref() -> None:
    assert _contains_document_root_reference({"$ref": "#"}) is True
    assert _contains_document_root_reference({"$ref": "#/definitions/x"}) is True


def test_contains_document_root_reference_false_for_external_ref() -> None:
    assert _contains_document_root_reference({"$ref": "other.json#/x"}) is False


def test_schema_identifier_keyword_uses_legacy_id_for_old_drafts() -> None:
    assert (
        _schema_identifier_keyword({"$schema": "http://json-schema.org/draft-04/schema#"}) == "id"
    )


def test_schema_identifier_keyword_defaults_to_dollar_id() -> None:
    assert _schema_identifier_keyword({}) == "$id"
    assert _schema_identifier_keyword(
        {"$schema": "https://json-schema.org/draft/2020-12/schema"}
    ) == ("$id")


def test_schema_establishes_embedded_resource_requires_nonempty_base() -> None:
    assert (
        _schema_establishes_embedded_resource(
            {"$id": "urn:example:schema"}, identifier_keyword="$id"
        )
        is True
    )
    assert (
        _schema_establishes_embedded_resource({"$id": "#/fragment-only"}, identifier_keyword="$id")
        is False
    )
    assert _schema_establishes_embedded_resource({}, identifier_keyword="$id") is False


def test_relocate_legacy_local_references_rewrites_root_ref() -> None:
    schema = {"$ref": "#", "properties": {"nested": {"$ref": "#/definitions/x"}}}
    _relocate_legacy_local_references(schema, pointer_prefix="/properties/arguments")
    assert schema["$ref"] == "#/properties/arguments"
    assert schema["properties"]["nested"]["$ref"] == "#/properties/arguments/definitions/x"


def test_relocate_legacy_local_references_stops_at_nested_resource_boundary() -> None:
    schema = {
        "properties": {
            "embedded": {
                "id": "urn:example:nested-resource",
                "$ref": "#",
            }
        }
    }
    _relocate_legacy_local_references(schema, pointer_prefix="/properties/arguments")
    # The embedded resource establishes its own base -- its "#" ref is a
    # self-reference to *that* resource, not the outer document root, so it
    # must not be rewritten.
    assert schema["properties"]["embedded"]["$ref"] == "#"


def test_inject_cluster_argument_flattens_a_closed_plain_schema() -> None:
    schema = {
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
        "additionalProperties": False,
    }
    result = inject_cluster_argument(schema, clusters=["alpha", "beta"])
    assert result["properties"]["cluster"]["enum"] == ["alpha", "beta"]
    assert "idempotency_key" in result["properties"]
    assert result["required"] == ["cluster", "path"]


def test_inject_cluster_argument_wraps_an_open_schema_under_arguments() -> None:
    schema = {"type": "object", "properties": {}, "additionalProperties": True}
    result = inject_cluster_argument(schema, clusters=["alpha"])
    assert result["properties"]["arguments"] is not None
    assert result["required"] == ["cluster", "arguments"]
    assert result["additionalProperties"] is False
