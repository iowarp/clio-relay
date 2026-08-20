"""Pinned JARVIS virtual-tool contract enforcement for packaged stdio validation.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (file-size
decomposition; see ``scripts/check_file_size.py``). This is a distinct,
security-relevant concern layered on top of a generic, already-validated
``tools/list`` (``mcp_stdio_validation_contract.py``): once ANY built-in
JARVIS tool is advertised, the WHOLE pinned v3.6 agent-facing contract
(schema, annotations, configured cluster enum) becomes mandatory, so a
partial or mixed-version JARVIS surface can never pass release validation.
Both functions are private helpers with no external callers (confirmed by
grep across ``src/`` and ``tests/`` before the move); :mod:`clio_relay.
mcp_stdio_validation` imports ``_validate_pinned_jarvis_contract`` directly
rather than re-exporting it.
"""

from __future__ import annotations

from typing import cast

from mcp_types import ToolAnnotations

from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
    jarvis_user_contract_digest,
    virtual_jarvis_tool_definitions,
)
from clio_relay.mcp_stdio_validation_support import JSON, _mapping, _tools_digest


def _validate_pinned_jarvis_contract(tools: list[JSON]) -> str | None:
    if jarvis_user_contract_digest() != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RelayError("bundled clio-kit JARVIS contract digest did not match its pin")
    pinned_names = set(jarvis_user_contract())
    actual = {cast(str, tool["name"]): tool for tool in tools if tool.get("name") in pinned_names}
    # Built-in JARVIS tools are advertised only when this profile has a verified
    # JARVIS route. Generic remote-MCP acceptance must therefore permit the exact
    # empty surface. Once any built-in JARVIS tool is exposed, however, the whole
    # pinned contract remains mandatory so a partial or mixed-version surface can
    # never pass release validation.
    if not actual:
        return None
    if set(actual) != pinned_names:
        raise RelayError("packaged MCP tools/list omitted part of the pinned JARVIS contract")
    cluster_enums: list[tuple[str, ...]] = []
    for name in sorted(pinned_names):
        input_schema = _mapping(actual[name].get("inputSchema")) or {}
        properties = _mapping(input_schema.get("properties")) or {}
        cluster_schema = _mapping(properties.get("cluster")) or {}
        raw_enum = cluster_schema.get("enum")
        enum_items = cast(list[object], raw_enum) if isinstance(raw_enum, list) else []
        if not isinstance(raw_enum, list) or not all(
            isinstance(item, str) and item for item in enum_items
        ):
            raise RelayError("packaged JARVIS tools omitted their configured cluster enum")
        enum = tuple(cast(list[str], raw_enum))
        if list(enum) != sorted(set(enum)):
            raise RelayError("packaged JARVIS tools exposed an invalid configured cluster enum")
        cluster_enums.append(enum)
    if len(set(cluster_enums)) != 1:
        raise RelayError("packaged JARVIS tools disagreed about configured cluster targets")
    clusters = list(cluster_enums[0])
    expected = {
        cast(str, definition["name"]): definition
        for definition in virtual_jarvis_tool_definitions(clusters=clusters)
    }
    contract_fields = (
        "name",
        "description",
        "inputSchema",
        "outputSchema",
    )
    normalized_expected_annotations = {
        name: _normalized_tool_annotations(definition.get("annotations")) or {}
        for name, definition in expected.items()
    }
    for name, definition in actual.items():
        actual_annotations = _normalized_tool_annotations(definition.get("annotations")) or {}
        expected_annotations = normalized_expected_annotations[name]
        annotation_differences = {
            key: {
                "actual": actual_annotations.get(key),
                "expected": value,
            }
            for key, value in expected_annotations.items()
            if actual_annotations.get(key) != value
        }
        if annotation_differences:
            raise RelayError(
                "packaged MCP JARVIS v3.6 annotation semantics did not match its pin: "
                f"{name} {annotation_differences}"
            )
    actual_contract = {
        name: {
            **{field: definition.get(field) for field in contract_fields},
            "annotations": normalized_expected_annotations[name],
        }
        for name, definition in actual.items()
    }
    expected_contract = {
        name: {
            **{field: definition.get(field) for field in contract_fields},
            "annotations": normalized_expected_annotations[name],
        }
        for name, definition in expected.items()
    }
    if actual_contract != expected_contract:
        differing_fields = {
            name: [
                field
                for field in (*contract_fields, "annotations")
                if actual_contract[name].get(field) != expected_contract[name].get(field)
            ]
            for name in sorted(actual_contract)
            if actual_contract[name] != expected_contract[name]
        }
        raise RelayError(
            "packaged MCP JARVIS v3.6 agent-facing schema did not match its pin: "
            f"{differing_fields}"
        )
    return _tools_digest([actual[name] for name in sorted(actual)])


def _normalized_tool_annotations(value: object) -> JSON | None:
    """Normalize MCP annotation defaults before comparing a pinned catalog."""
    if value is None:
        return None
    return ToolAnnotations.model_validate(value).model_dump(
        by_alias=True,
        mode="json",
        exclude_none=True,
    )
