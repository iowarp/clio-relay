"""Tests for the agent-facing wire-schema builder cluster (#231).

Two concerns:

1. **Extraction seam** -- ``clio_relay.remote_mcp_wire_schemas`` is the
   owner module; ``clio_relay.remote_mcp`` must re-export all four names
   under an identical binding (proven by identity, not structural
   equality) so existing callers -- mcp_server.py, jarvis_mcp.py,
   jarvis_mcp_validation.py, and this file's own
   ``VirtualRemoteMcpTool.definition()`` -- keep resolving to the *same*
   object after the move.
2. **Schema builder behavior** -- these four builders were previously
   exercised only indirectly, through ``VirtualRemoteMcpTool.definition()``'s
   end-to-end output in ``tests/test_remote_mcp.py`` and jarvis_mcp's own
   contract tests (which correctly stay where they are -- they test those
   higher-level contracts). This file adds net-new focused coverage for
   the builders themselves, including the deferred CLIO_KIT_JARVIS_USER_TOOL_NAMES
   import inside virtual_jarvis_job_output_schema.
"""

from __future__ import annotations

import pytest

import clio_relay.remote_mcp as remote_mcp
from clio_relay.remote_mcp_wire_schemas import (
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    cluster_route_revision_json_schema,
    jarvis_service_runtime_handoff_json_schema,
    virtual_jarvis_job_output_schema,
)


def test_remote_mcp_reexports_are_identical_objects() -> None:
    assert remote_mcp.cluster_route_revision_json_schema is cluster_route_revision_json_schema
    assert remote_mcp.VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA is VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA
    assert (
        remote_mcp.jarvis_service_runtime_handoff_json_schema
        is jarvis_service_runtime_handoff_json_schema
    )
    assert remote_mcp.virtual_jarvis_job_output_schema is virtual_jarvis_job_output_schema


def test_cluster_route_revision_json_schema_is_a_bounded_hex_string() -> None:
    schema = cluster_route_revision_json_schema()
    assert schema["type"] == "string"
    assert schema["pattern"] == "^[0-9a-f]{64}$"


def test_job_output_schema_requires_the_core_receipt_fields() -> None:
    assert set(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA["required"]) == {
        "cluster",
        "job_id",
        "state",
        "kind",
        "terminal",
        "route_revision",
        "catalog_revision",
    }
    assert VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA["additionalProperties"] is False


def test_jarvis_service_runtime_handoff_schema_enumerates_declared_clusters() -> None:
    schema = jarvis_service_runtime_handoff_json_schema(clusters=["alpha", "beta"])
    assert schema["properties"]["cluster"]["enum"] == ["alpha", "beta"]


def test_jarvis_service_runtime_handoff_schema_omits_enum_without_clusters() -> None:
    schema = jarvis_service_runtime_handoff_json_schema()
    assert "enum" not in schema["properties"]["cluster"]


def test_virtual_jarvis_job_output_schema_rejects_unknown_tool() -> None:
    with pytest.raises(ValueError, match="unknown virtual JARVIS tool"):
        virtual_jarvis_job_output_schema("not_a_real_tool")


def test_virtual_jarvis_job_output_schema_adds_service_bindings_for_get_execution() -> None:
    schema = virtual_jarvis_job_output_schema("jarvis_get_execution", clusters=["alpha"])
    properties = schema["properties"]
    assert "service_runtime_bindings" in properties
    assert properties["service_runtime_bindings"]["items"]["properties"]["cluster"]["enum"] == [
        "alpha"
    ]


def test_virtual_jarvis_job_output_schema_omits_service_bindings_for_other_tools() -> None:
    schema = virtual_jarvis_job_output_schema("jarvis_run")
    assert "service_runtime_bindings" not in schema["properties"]
