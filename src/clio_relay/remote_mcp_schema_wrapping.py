"""Local relay control envelope injection for virtualized remote MCP tool schemas.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns the
JSON-Schema transformation that turns a remote server's discovered
``inputSchema`` into the schema an agent actually calls: adding the
``cluster`` selector and the local-only relay control fields
(:data:`VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS` -- ``idempotency_key``,
``wait_for_terminal``, and friends, none of which are ever forwarded to the
remote server), either flattened onto a closed plain-object contract or
nested below an ``arguments`` wrapper when the contract's own shape makes
flat augmentation unsafe (:func:`inject_cluster_argument`,
:func:`remote_input_schema_requires_wrapper`, :func:`virtual_schema_error`),
plus the Draft 3/4 document-root ``$ref`` retargeting an embedded/wrapped
schema needs (:func:`_relocate_legacy_local_references`,
:func:`_schema_identifier_keyword`, :func:`_schema_establishes_embedded_resource`,
:func:`_contains_document_root_reference`).

:mod:`clio_relay.remote_mcp` re-exports :func:`inject_cluster_argument` and
:data:`VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS` under their original names --
tests import ``inject_cluster_argument`` directly, and
``VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS`` has an external importer
(``queue_tasks.py``) plus a real reader still in remote_mcp.py's own
catalog-assembly body. :data:`VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS` and
:data:`MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES` are imported back into
remote_mcp.py too (both have a real reader left there), but have no
external importer, so they are not re-exported. Every other name here is
private with no caller outside remote_mcp.py's own body (confirmed by grep
before the move), so remote_mcp.py imports them directly with no
re-export.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from clio_relay.remote_mcp_schema_validation import (
    _COMPOSED_SCHEMA_KEYS,
    _FLAT_SCHEMA_KEYS,
    _require_bounded_json_structure,
)
from clio_relay.remote_mcp_tool_schema import _stable_digest

JSON = dict[str, Any]

MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES = 32_768

VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS: dict[str, JSON] = {
    "idempotency_key": {
        "type": "string",
        "minLength": 1,
        "maxLength": 512,
        "description": (
            "Stable retry identity consumed by clio-relay and never forwarded to the "
            "remote MCP server. Reuse it only for the exact same call payload."
        ),
    },
    "wait_for_terminal": {
        "type": "boolean",
        "default": False,
        "description": (
            "Wait for this relay job to finish and return its bounded MCP result in the "
            "same tool response. This field is consumed by clio-relay and is never "
            "forwarded to the remote MCP server."
        ),
    },
    "wait_timeout_seconds": {
        "type": "number",
        "default": 600,
        "exclusiveMinimum": 0,
        "description": "Maximum local relay wait; never forwarded to the remote MCP server.",
    },
    "poll_seconds": {
        "type": "number",
        "default": 2,
        "exclusiveMinimum": 0,
        "description": "Local relay wait polling interval; never forwarded remotely.",
    },
    "include_logs": {
        "type": "boolean",
        "default": False,
        "description": (
            "Include bounded stdout and stderr when waiting for a terminal result. "
            "This field is never forwarded to the remote MCP server."
        ),
    },
    "log_limit": {
        "type": "integer",
        "default": MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES,
        "minimum": 1,
        "maximum": MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES,
        "description": "Maximum bytes per returned log stream; never forwarded remotely.",
    },
}
VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS = frozenset(VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS)


def inject_cluster_argument(input_schema: JSON, *, clusters: list[str]) -> JSON:
    """Copy a remote input schema and add the local-only relay envelope.

    Closed plain-object contracts remain flat for agent ergonomics. Contracts
    whose open root, composition, or own local-control field makes flat
    augmentation unsafe are preserved under an ``arguments`` object instead.
    """
    _require_bounded_json_structure(input_schema, label="inputSchema")
    error = virtual_schema_error(input_schema)
    if error is not None:
        raise ValueError(error)
    cluster_schema: JSON = {
        "type": "string",
        "enum": sorted(clusters),
        "description": "Configured clio-relay cluster target.",
    }
    if remote_input_schema_requires_wrapper(input_schema):
        nested_schema = deepcopy(input_schema)
        identifier_keyword = _schema_identifier_keyword(nested_schema)
        if identifier_keyword == "id":
            _relocate_legacy_local_references(
                nested_schema,
                pointer_prefix="/properties/arguments",
            )
        elif not _schema_establishes_embedded_resource(
            nested_schema,
            identifier_keyword=identifier_keyword,
        ):
            nested_schema[identifier_keyword] = (
                "urn:clio-relay:remote-mcp-schema:" + _stable_digest({"input_schema": input_schema})
            )
        wrapper: JSON = {
            "type": "object",
            "properties": {
                "cluster": cluster_schema,
                "arguments": nested_schema,
                **deepcopy(VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS),
            },
            "required": ["cluster", "arguments"],
            "additionalProperties": False,
        }
        dialect = input_schema.get("$schema")
        if isinstance(dialect, str):
            wrapper["$schema"] = dialect
        return wrapper
    rendered = deepcopy(input_schema)
    properties = cast(JSON, rendered.setdefault("properties", {}))
    properties["cluster"] = cluster_schema
    properties.update(deepcopy(VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS))
    required = cast(list[str], rendered.setdefault("required", []))
    rendered["required"] = ["cluster", *required]
    rendered["type"] = "object"
    return rendered


def virtual_schema_error(input_schema: JSON) -> str | None:
    """Return why a remote input contract cannot be safely virtualized."""
    schema_type = input_schema.get("type", "object")
    if schema_type != "object":
        return "remote inputSchema must have type object"
    properties = input_schema.get("properties", {})
    if not isinstance(properties, dict):
        return "remote inputSchema properties must be an object"
    required = input_schema.get("required", [])
    if not isinstance(required, list) or not all(
        isinstance(item, str) for item in cast(list[object], required)
    ):
        return "remote inputSchema required must be a string array"
    typed_required = cast(list[str], required)
    if len(typed_required) != len(set(typed_required)):
        return "remote inputSchema required entries must be unique"
    return None


def remote_input_schema_requires_wrapper(input_schema: JSON) -> bool:
    """Return whether a remote schema must be nested below local relay fields."""
    _require_bounded_json_structure(input_schema, label="inputSchema")
    properties = input_schema.get("properties", {})
    required = input_schema.get("required", [])
    property_names = (
        set(cast(dict[str, object], properties)) if isinstance(properties, dict) else set[str]()
    )
    required_names = set(cast(list[str], required)) if isinstance(required, list) else set[str]()
    root_identifier = input_schema.get("$id")
    return (
        (isinstance(root_identifier, str) and bool(root_identifier))
        or input_schema.get("additionalProperties", True) is not False
        or any(key in input_schema for key in _COMPOSED_SCHEMA_KEYS)
        or bool(set(input_schema) - _FLAT_SCHEMA_KEYS)
        or _contains_document_root_reference(input_schema)
        or (isinstance(properties, dict) and "cluster" in properties)
        or (isinstance(required, list) and "cluster" in required)
        or bool(property_names.intersection(VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS))
        or bool(required_names.intersection(VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS))
    )


def _contains_document_root_reference(value: object) -> bool:
    """Return whether a nested schema reference depends on the document root."""
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            mapping = cast(dict[object, object], current)
            for key, item in mapping.items():
                if (
                    key in {"$ref", "$dynamicRef", "$recursiveRef"}
                    and isinstance(item, str)
                    and (item == "#" or item.startswith("#/"))
                ):
                    return True
                stack.append(item)
        elif isinstance(current, list):
            stack.extend(cast(list[object], current))
    return False


def _schema_identifier_keyword(schema: JSON) -> str:
    """Return the resource identifier keyword for a declared JSON Schema dialect."""
    dialect = schema.get("$schema")
    if isinstance(dialect, str) and ("draft-03" in dialect or "draft-04" in dialect):
        return "id"
    return "$id"


def _schema_establishes_embedded_resource(
    schema: JSON,
    *,
    identifier_keyword: str,
) -> bool:
    """Return whether an identifier gives an embedded schema its own resource base."""
    schema_id = schema.get(identifier_keyword)
    if not isinstance(schema_id, str):
        return False
    return bool(schema_id.partition("#")[0])


def _relocate_legacy_local_references(
    value: object,
    *,
    pointer_prefix: str,
    nested_resource: bool = False,
    root: bool = True,
) -> None:
    """Retarget Draft 3/4 document-root references after schema embedding."""
    if isinstance(value, dict):
        mapping = cast(JSON, value)
        establishes_nested_resource = not root and (
            isinstance(mapping.get("id"), str) and bool(cast(str, mapping["id"]).partition("#")[0])
        )
        rewrite_here = not nested_resource and not establishes_nested_resource
        child_nested_resource = nested_resource or establishes_nested_resource
        for key, item in list(mapping.items()):
            if key == "$ref" and isinstance(item, str) and rewrite_here:
                if item == "#":
                    mapping[key] = f"#{pointer_prefix}"
                elif item.startswith("#/"):
                    mapping[key] = f"#{pointer_prefix}{item[1:]}"
                continue
            _relocate_legacy_local_references(
                item,
                pointer_prefix=pointer_prefix,
                nested_resource=child_nested_resource,
                root=False,
            )
    elif isinstance(value, list):
        for item in cast(list[object], value):
            _relocate_legacy_local_references(
                item,
                pointer_prefix=pointer_prefix,
                nested_resource=nested_resource,
                root=False,
            )
