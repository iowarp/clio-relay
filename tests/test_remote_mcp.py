from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

import pytest
from jsonschema import Draft4Validator, Draft201909Validator, Draft202012Validator
from pydantic import ValidationError
from pytest import MonkeyPatch
from typer.testing import CliRunner

from clio_relay import remote_mcp
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    RemoteMcpServerConfig,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.mcp_server import handle_request
from clio_relay.models import JobKind, JobState, McpCallSpec, McpOperation, RelayJob
from clio_relay.remote_mcp import (
    MAX_REMOTE_MCP_CACHE_BYTES,
    MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES,
    MAX_REMOTE_MCP_TOOLS_PER_SERVER,
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    RemoteMcpAcceptanceReport,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpToolSchema,
    build_remote_mcp_acceptance_report,
    build_virtual_remote_mcp_catalog,
    cache_entry_from_discovery_artifact,
    inject_cluster_argument,
    remote_mcp_server_artifact_digest,
)
from clio_relay.spool import JobSpool

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


class _SchemaValidator(Protocol):
    def validate(self, instance: object) -> None:
        """Validate one JSON-compatible instance."""


def test_remote_mcp_registration_is_deny_by_default_and_validated() -> None:
    registration = RemoteMcpServerConfig(command="science-mcp")

    assert registration.allow_tools == []
    assert registration.profiles == ["admin"]
    assert registration.call_timeout_seconds == 300
    assert registration.allows_tool("inspect") is False

    with pytest.raises(ValidationError, match="exact names or '\\*' only"):
        RemoteMcpServerConfig(command="science-mcp", allow_tools=["inspect*"])
    with pytest.raises(ValidationError, match="must not be empty"):
        RemoteMcpServerConfig(command="science-mcp", profiles=[])
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        RemoteMcpServerConfig(command="science-mcp", call_timeout_seconds=0)
    with pytest.raises(ValidationError, match="less than or equal"):
        RemoteMcpServerConfig(command="science-mcp", schema_cache_ttl_seconds=10**100)
    with pytest.raises(ValidationError, match="clio-kit-spack-user-v3"):
        RemoteMcpServerConfig.model_validate(
            {"command": "science-mcp", "contract": "unknown-contract"}
        )
    with pytest.raises(ValidationError, match="cannot expose relay credential"):
        RemoteMcpServerConfig(
            command="science-mcp",
            env_from={"SCIENCE_TOKEN": "CLIO_RELAY_API_TOKEN"},
        )
    with pytest.raises(ValidationError, match="cannot expose relay credential"):
        McpCallSpec(
            server="science-mcp",
            env_from={"CLIO_RELAY_API_TOKEN": "SCIENCE_TOKEN"},
            tool="inspect",
        )
    with pytest.raises(ValidationError, match="mutable remote MCP artifacts cannot be exposed"):
        RemoteMcpServerConfig(
            command="science-mcp",
            allow_tools=["inspect"],
            profiles=["user"],
            allow_mutable_artifact=True,
        )
    with pytest.raises(ValidationError, match="relay transport credential SITE_FRP_TOKEN"):
        ClusterDefinition(
            name="alpha",
            ssh_host="localhost",
            frp_transport=FrpTransportConfig(token_env="SITE_FRP_TOKEN"),
            remote_mcp_servers={
                "science": RemoteMcpServerConfig(
                    command="science-mcp",
                    env_from={"SCIENCE_TOKEN": "SITE_FRP_TOKEN"},
                )
            },
        )


def test_discovery_artifact_creates_fresh_provenance_cache_entry(tmp_path: Path) -> None:
    registration = _registration()
    payload = _discovery_artifact(
        registration,
        tools=[_tool("inspect", required=["path"])],
    )

    entry = cache_entry_from_discovery_artifact(
        cluster="cluster-one",
        server_name="science",
        registration=registration,
        discovery_job_id="job_discovery",
        artifact_id="artifact_result",
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        artifact_payload=payload,
        discovered_at=NOW,
    )
    cache_path = tmp_path / "remote-mcp-cache.json"
    saved = RemoteMcpSchemaCache.update_entry(cache_path, entry)
    loaded = RemoteMcpSchemaCache.load(cache_path)

    assert saved == loaded
    assert entry.expires_at == NOW + timedelta(seconds=3600)
    assert entry.provenance.source == "durable_relay_mcp_tools_list"
    assert entry.provenance.discovery_job_id == "job_discovery"
    assert entry.provenance.protocol_version == "2024-11-05"
    assert entry.provenance.server_info == {"name": "science", "version": "1.2.3"}
    assert entry.provenance.server_artifact == _server_artifact(registration)
    assert entry.tools[0].input_schema["required"] == ["path"]


def test_remote_mcp_cache_rejects_oversized_or_non_regular_files(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized-cache.json"
    oversized.write_bytes(b" " * (MAX_REMOTE_MCP_CACHE_BYTES + 1))
    non_regular = tmp_path / "directory-cache.json"
    non_regular.mkdir()

    with pytest.raises(ConfigurationError, match="exceeds"):
        RemoteMcpSchemaCache.load(oversized)
    with pytest.raises(ConfigurationError, match="regular owned file"):
        RemoteMcpSchemaCache.load(non_regular)


def test_remote_mcp_cache_rejects_naive_timestamps() -> None:
    entry = _entry(_registration(), cluster="alpha", server_name="science")
    payload = entry.model_dump(mode="python")
    payload["discovered_at"] = NOW.replace(tzinfo=None)

    with pytest.raises(ValidationError, match="timezone-aware"):
        RemoteMcpSchemaCacheEntry.model_validate(payload)


def test_remote_mcp_cache_retries_windows_sharing_violation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    registration = _registration()
    entry = _entry(registration, cluster="alpha", server_name="science")
    path = tmp_path / "remote-mcp-cache.json"
    attempts = 0
    original_replace = remote_mcp.os.replace

    def sharing_once(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError("simulated sharing violation")
        original_replace(source, target)

    def no_sleep(_seconds: float) -> None:
        return

    monkeypatch.setattr(remote_mcp.os, "replace", sharing_once)
    monkeypatch.setattr(remote_mcp.time, "sleep", no_sleep)

    RemoteMcpSchemaCache.update_entry(path, entry)

    assert attempts == 2
    assert RemoteMcpSchemaCache.load(path).entry_for("alpha", "science") == entry
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_remote_mcp_cache_preserves_old_file_when_replace_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    registration = _registration()
    original = _entry(registration, cluster="alpha", server_name="science")
    replacement = _entry(registration, cluster="beta", server_name="science")
    path = tmp_path / "remote-mcp-cache.json"
    RemoteMcpSchemaCache.update_entry(path, original)

    def fail_replace(source: object, target: object) -> None:
        del source, target
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(remote_mcp.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated replacement failure"):
        RemoteMcpSchemaCache.update_entry(path, replacement)

    loaded = RemoteMcpSchemaCache.load(path)
    assert loaded.entry_for("alpha", "science") == original
    assert loaded.entry_for("beta", "science") is None
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_remote_mcp_discovery_and_schema_sizes_are_bounded() -> None:
    registration = _registration()
    tools = [_tool(f"tool-{index}") for index in range(MAX_REMOTE_MCP_TOOLS_PER_SERVER + 1)]

    with pytest.raises(ValueError, match="tools/list exceeds"):
        _entry_from_payload(registration, _discovery_artifact(registration, tools=tools))
    with pytest.raises(ValidationError, match="tool schema exceeds"):
        RemoteMcpToolSchema(
            name="oversized",
            input_schema={
                "type": "object",
                "description": "x" * MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES,
            },
        )
    with pytest.raises(ValidationError, match="not valid JSON Schema"):
        RemoteMcpToolSchema(
            name="malformed",
            input_schema={"type": "object", "properties": {"path": 17}},
        )
    with pytest.raises(ValidationError, match="non-finite JSON number"):
        RemoteMcpToolSchema(
            name="nonfinite",
            input_schema={"type": "object", "properties": {"value": {"const": float("inf")}}},
        )
    with pytest.raises(ValidationError, match="unsupported JSON Schema dialect"):
        RemoteMcpToolSchema(
            name="unknown-dialect",
            input_schema={
                "$schema": "https://example.invalid/schema",
                "type": "object",
            },
        )


def test_remote_mcp_schema_depth_is_bounded_before_recursive_validation() -> None:
    nested: dict[str, object] = {"type": "object"}
    for _ in range(remote_mcp.MAX_REMOTE_MCP_JSON_DEPTH + 1):
        nested = {"allOf": [nested]}

    with pytest.raises(ValidationError, match="nesting levels"):
        RemoteMcpToolSchema(name="deep-schema", input_schema=nested)

    with pytest.raises(ValueError, match="nesting levels"):
        inject_cluster_argument(nested, clusters=["alpha"])


def test_remote_mcp_discovery_parser_recursion_fails_closed() -> None:
    registration = _registration()
    depth = 2_000
    payload = (b'{"nested":' * depth) + b"0" + (b"}" * depth)

    with pytest.raises(ValueError, match="nesting levels"):
        _entry_from_payload(registration, payload)


def test_remote_mcp_discovery_json_node_count_is_bounded(
    monkeypatch: MonkeyPatch,
) -> None:
    registration = _registration()
    artifact = json.loads(_discovery_artifact(registration, tools=[_tool("inspect")]))
    artifact["server_info"] = {"wide": list(range(200))}
    payload = json.dumps(artifact).encode()
    monkeypatch.setattr(remote_mcp, "MAX_REMOTE_MCP_JSON_NODES", 100)

    with pytest.raises(ValueError, match="100 JSON nodes"):
        _entry_from_payload(registration, payload)


def test_remote_mcp_protocol_error_diagnostic_is_bounded() -> None:
    registration = _registration()
    artifact = json.loads(_discovery_artifact(registration, tools=[]))
    artifact["protocol_error"] = "x" * 100_000
    payload = json.dumps(artifact).encode()

    with pytest.raises(ValueError) as error:
        _entry_from_payload(registration, payload)

    assert len(str(error.value)) <= remote_mcp.MAX_REMOTE_MCP_DIAGNOSTIC_CHARS + 100
    assert str(error.value).endswith("... [truncated]")


def test_discovery_artifact_rejects_wrong_command_and_protocol_failure() -> None:
    registration = _registration()
    wrong_command = json.loads(_discovery_artifact(registration, tools=[]))
    wrong_command["server"] = "other-mcp"
    failed_protocol = json.loads(_discovery_artifact(registration, tools=[]))
    failed_protocol["protocol_error"] = '{"code":-32603}'
    nonfinite_provenance = json.loads(_discovery_artifact(registration, tools=[]))
    nonfinite_provenance["server_info"]["version"] = float("inf")

    with pytest.raises(ValueError, match="server does not match"):
        _entry_from_payload(registration, json.dumps(wrong_command).encode())
    with pytest.raises(ValueError, match="protocol error"):
        _entry_from_payload(registration, json.dumps(failed_protocol).encode())
    with pytest.raises(ValueError, match="non-finite"):
        _entry_from_payload(registration, json.dumps(nonfinite_provenance).encode())
    valid_payload = _discovery_artifact(registration, tools=[_tool("inspect")])
    with pytest.raises(ValueError, match="SHA-256 does not match"):
        cache_entry_from_discovery_artifact(
            cluster="alpha",
            server_name="science",
            registration=registration,
            discovery_job_id="job_discovery",
            artifact_id="artifact_result",
            artifact_sha256="0" * 64,
            artifact_payload=valid_payload,
            discovered_at=NOW,
        )


def test_remote_mcp_cache_rejects_stale_schema_digest() -> None:
    entry = _entry(_registration(), cluster="alpha", server_name="science")
    payload = entry.model_dump(mode="python")
    payload["schema_digest"] = "0" * 64

    with pytest.raises(ValidationError, match="schema digest does not match"):
        RemoteMcpSchemaCacheEntry.model_validate(payload)


def test_cluster_injection_flattens_simple_schema_and_wraps_composed_contracts() -> None:
    remote_schema: dict[str, object] = {
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
        "additionalProperties": False,
    }
    before = deepcopy(remote_schema)

    rendered = inject_cluster_argument(remote_schema, clusters=["beta", "alpha"])

    assert remote_schema == before
    assert rendered["required"] == ["cluster", "path"]
    cluster_schema = rendered["properties"]["cluster"]
    assert cluster_schema["enum"] == ["alpha", "beta"]
    assert rendered["additionalProperties"] is False

    owns_cluster = inject_cluster_argument(
        {
            "type": "object",
            "properties": {"cluster": {"type": "string"}},
            "required": ["cluster"],
            "additionalProperties": False,
        },
        clusters=["alpha"],
    )
    assert owns_cluster["required"] == ["cluster", "arguments"]
    assert owns_cluster["properties"]["arguments"]["properties"]["cluster"] == {"type": "string"}

    referenced = inject_cluster_argument(
        {
            "$ref": "#/$defs/Arguments",
            "$defs": {
                "Arguments": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "additionalProperties": False,
                }
            },
        },
        clusters=["alpha"],
    )
    assert referenced["properties"]["arguments"]["$ref"] == "#/$defs/Arguments"
    assert referenced["properties"]["arguments"]["$id"].startswith(
        "urn:clio-relay:remote-mcp-schema:"
    )
    assert referenced["additionalProperties"] is False
    cast(_SchemaValidator, Draft202012Validator(referenced)).validate(
        {"cluster": "alpha", "arguments": {"path": "/data/run"}}
    )

    composed = inject_cluster_argument(
        {
            "type": "object",
            "allOf": [
                {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": False,
                }
            ],
        },
        clusters=["alpha"],
    )
    assert composed["required"] == ["cluster", "arguments"]
    assert composed["properties"]["arguments"]["allOf"][0]["additionalProperties"] is False

    property_constrained = inject_cluster_argument(
        {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "propertyNames": {"pattern": "^path$"},
            "maxProperties": 1,
        },
        clusters=["alpha"],
    )
    assert property_constrained["required"] == ["cluster", "arguments"]
    assert property_constrained["properties"]["arguments"]["maxProperties"] == 1


def test_cluster_wrapper_preserves_recursive_and_empty_id_schema_semantics() -> None:
    empty_id = inject_cluster_argument(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "",
            "$ref": "#/$defs/node",
            "$defs": {
                "node": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": False,
                }
            },
        },
        clusters=["alpha"],
    )
    embedded = empty_id["properties"]["arguments"]
    assert embedded["$id"].startswith("urn:clio-relay:remote-mcp-schema:")
    cast(_SchemaValidator, Draft202012Validator(empty_id)).validate(
        {"cluster": "alpha", "arguments": {"value": "ok"}}
    )

    recursive = inject_cluster_argument(
        {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$recursiveAnchor": True,
            "type": "object",
            "properties": {"child": {"$recursiveRef": "#"}},
            "additionalProperties": False,
        },
        clusters=["alpha"],
    )
    assert recursive["properties"]["arguments"]["properties"]["child"]["$recursiveRef"] == "#"
    cast(_SchemaValidator, Draft201909Validator(recursive)).validate(
        {
            "cluster": "alpha",
            "arguments": {"child": {"child": {}}},
        }
    )

    draft_four = inject_cluster_argument(
        {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "$ref": "#/definitions/arguments",
            "definitions": {
                "arguments": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": False,
                }
            },
        },
        clusters=["alpha"],
    )
    assert draft_four["properties"]["arguments"]["$ref"] == (
        "#/properties/arguments/definitions/arguments"
    )
    cast(_SchemaValidator, Draft4Validator(draft_four)).validate(
        {"cluster": "alpha", "arguments": {"path": "/data/run"}}
    )


def test_nested_root_reference_forces_semantics_preserving_wrapper() -> None:
    recursive = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"child": {"$ref": "#"}},
        "additionalProperties": False,
    }

    rendered = inject_cluster_argument(recursive, clusters=["alpha"])

    assert rendered["required"] == ["cluster", "arguments"]
    cast(_SchemaValidator, Draft202012Validator(rendered)).validate(
        {"cluster": "alpha", "arguments": {"child": {"child": {}}}}
    )


def test_catalog_shares_one_alias_across_clusters_with_identical_contracts() -> None:
    registration = _registration(
        profiles=["user"], env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    )
    entry_one = _entry(registration, cluster="alpha", server_name="science")
    entry_two = _entry(registration, cluster="beta", server_name="science")
    registry = ClusterRegistry(
        clusters={
            "alpha": _cluster("alpha", {"science": registration}),
            "beta": _cluster("beta", {"science": registration}),
        }
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        RemoteMcpSchemaCache(entries=[entry_one, entry_two]),
        profile="user",
        now=NOW,
    )

    assert list(catalog.tools) == ["remote_science_inspect"]
    virtual = catalog.tools["remote_science_inspect"]
    assert sorted(virtual.routes) == ["alpha", "beta"]
    assert virtual.definition()["inputSchema"]["properties"]["cluster"]["enum"] == [
        "alpha",
        "beta",
    ]
    assert virtual.definition()["outputSchema"] == VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA
    assert "durable relay job" in virtual.definition()["description"]


def test_catalog_namespace_unifies_differently_named_cross_cluster_routes() -> None:
    primary = _registration(namespace="Science", profiles=["user"])
    secondary = _registration(namespace="science", profiles=["user"])
    registry = ClusterRegistry(
        clusters={
            "alpha": _cluster("alpha", {"science-primary": primary}),
            "beta": _cluster("beta", {"science-v2": secondary}),
        }
    )
    cache = RemoteMcpSchemaCache(
        entries=[
            _entry(primary, cluster="alpha", server_name="science-primary"),
            _entry(secondary, cluster="beta", server_name="science-v2"),
        ]
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile="user",
        now=NOW,
    )

    assert list(catalog.tools) == ["remote_science_inspect"]
    routes = catalog.tools["remote_science_inspect"].routes
    assert routes["alpha"].server_name == "science-primary"
    assert routes["beta"].server_name == "science-v2"


def test_catalog_rejects_ambiguous_same_cluster_namespace_route() -> None:
    registration = _registration(namespace="science", profiles=["user"])
    registry = ClusterRegistry(
        clusters={
            "alpha": _cluster(
                "alpha",
                {"science-primary": registration, "science-secondary": registration},
            )
        }
    )
    cache = RemoteMcpSchemaCache(
        entries=[
            _entry(registration, cluster="alpha", server_name="science-primary"),
            _entry(registration, cluster="alpha", server_name="science-secondary"),
        ]
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile="user",
        now=NOW,
    )

    assert catalog.tools == {}
    assert len(catalog.issues) == 2
    assert all("route is ambiguous" in issue.reason for issue in catalog.issues)


def test_artifact_only_refresh_changes_catalog_revision_without_renaming_alias() -> None:
    registration = _registration(profiles=["user"])
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})})
    original_entry = _entry(registration, cluster="alpha", server_name="science")
    changed_artifact = deepcopy(original_entry.provenance.server_artifact)
    changed_artifact["install_artifact_sha256"] = "d" * 64
    input_files = cast(list[dict[str, object]], changed_artifact["input_files"])
    input_files[0]["sha256"] = "d" * 64
    refreshed_entry = original_entry.model_copy(
        update={
            "provenance": original_entry.provenance.model_copy(
                update={"server_artifact": changed_artifact}
            )
        }
    )

    original = build_virtual_remote_mcp_catalog(
        registry,
        RemoteMcpSchemaCache(entries=[original_entry]),
        profile="user",
        now=NOW,
    )
    refreshed = build_virtual_remote_mcp_catalog(
        registry,
        RemoteMcpSchemaCache(entries=[refreshed_entry]),
        profile="user",
        now=NOW,
    )

    assert list(original.tools) == ["remote_science_inspect"]
    assert list(refreshed.tools) == ["remote_science_inspect"]
    assert original.revision != refreshed.revision
    assert (
        original.tools["remote_science_inspect"].routes["alpha"].expected_server_artifact_digest
        != refreshed.tools["remote_science_inspect"].routes["alpha"].expected_server_artifact_digest
    )


def test_catalog_alias_collisions_are_deterministic_and_reserved_safe() -> None:
    first = _registration(namespace="science-a", profiles=["user"])
    second = _registration(namespace="science_a", profiles=["user"])
    registry = ClusterRegistry(
        clusters={
            "alpha": _cluster("alpha", {"first": first, "second": second}),
        }
    )
    cache = RemoteMcpSchemaCache(
        entries=[
            _entry(first, cluster="alpha", server_name="first"),
            _entry(second, cluster="alpha", server_name="second"),
        ]
    )

    one = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile="user",
        reserved_names={"remote_science_a_inspect"},
        now=NOW,
    )
    two = build_virtual_remote_mcp_catalog(
        ClusterRegistry(clusters=dict(reversed(list(registry.clusters.items())))),
        RemoteMcpSchemaCache(entries=list(reversed(cache.entries))),
        profile="user",
        reserved_names={"remote_science_a_inspect"},
        now=NOW,
    )

    assert sorted(one.tools) == sorted(two.tools)
    assert len(one.tools) == 2
    assert "remote_science_a_inspect" not in one.tools
    assert all(name.startswith("remote_science_a_inspect_") for name in one.tools)


def test_catalog_candidate_count_is_bounded_and_reported(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(remote_mcp, "MAX_VIRTUAL_REMOTE_MCP_CANDIDATES", 1)
    registration = _registration(profiles=["user"]).model_copy(update={"allow_tools": ["*"]})
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})})
    cache = RemoteMcpSchemaCache(
        entries=[
            _entry(
                registration,
                cluster="alpha",
                server_name="science",
                tools=[_tool("inspect"), _tool("summarize")],
            )
        ]
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile="user",
        now=NOW,
    )

    assert len(catalog.tools) == 1
    assert len(catalog.issues) == 1
    assert "candidate limit" in catalog.issues[0].reason


def test_catalog_diagnostics_are_bounded(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(remote_mcp, "MAX_REMOTE_MCP_CATALOG_ISSUES", 2)
    registration = _registration(profiles=["user"]).model_copy(
        update={"allow_tools": ["inspect", "missing-one", "missing-two"]}
    )
    catalog = build_virtual_remote_mcp_catalog(
        ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}),
        RemoteMcpSchemaCache(
            entries=[_entry(registration, cluster="alpha", server_name="science")]
        ),
        profile="user",
        now=NOW,
    )

    assert len(catalog.issues) == 2
    assert "issue limit" in catalog.issues[-1].reason
    assert "remote_science_inspect" in catalog.tools


def test_catalog_filters_profiles_before_assigning_aliases_and_revision() -> None:
    visible = _registration(namespace="science-a", profiles=["user"])
    hidden = _registration(namespace="science_a", profiles=["admin"])
    visible_cluster = _cluster("alpha", {"visible": visible})
    registry = ClusterRegistry(
        clusters={
            "alpha": _cluster("alpha", {"visible": visible, "hidden": hidden}),
        }
    )
    visible_entry = _entry(visible, cluster="alpha", server_name="visible")
    cache = RemoteMcpSchemaCache(
        entries=[
            visible_entry,
            _entry(hidden, cluster="alpha", server_name="hidden"),
        ]
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile="user",
        now=NOW,
    )
    visible_only = build_virtual_remote_mcp_catalog(
        ClusterRegistry(clusters={"alpha": visible_cluster}),
        RemoteMcpSchemaCache(entries=[visible_entry]),
        profile="user",
        now=NOW,
    )

    assert list(catalog.tools) == ["remote_science_a_inspect"]
    assert catalog.issues == ()
    assert catalog.revision == visible_only.revision


def test_catalog_hides_stale_changed_and_wrong_profile_entries() -> None:
    user_registration = _registration(profiles=["user"])
    stale = _entry(user_registration, cluster="stale", server_name="science").model_copy(
        update={
            "discovered_at": NOW - timedelta(hours=2),
            "expires_at": NOW - timedelta(hours=1),
        }
    )
    changed_registration = _registration(command="science-mcp-v2", profiles=["user"])
    changed = _entry(
        user_registration,
        cluster="changed",
        server_name="science",
    )
    registry = ClusterRegistry(
        clusters={
            "stale": _cluster("stale", {"science": user_registration}),
            "changed": _cluster("changed", {"science": changed_registration}),
        }
    )

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        RemoteMcpSchemaCache(entries=[stale, changed]),
        profile="user",
        now=NOW,
    )
    admin_catalog = build_virtual_remote_mcp_catalog(
        ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": user_registration})}),
        RemoteMcpSchemaCache(
            entries=[_entry(user_registration, cluster="alpha", server_name="science")]
        ),
        profile="admin",
        now=NOW,
    )

    assert catalog.tools == {}
    assert {issue.reason for issue in catalog.issues} == {
        "registered command changed; run remote-mcp refresh",
        f"schema cache expired at {(NOW - timedelta(hours=1)).isoformat()}",
    }
    assert admin_catalog.tools == {}


def test_mcp_server_reloads_catalog_and_routes_without_forwarding_cluster(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(
        profiles=["user"], env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    )
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})})
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    registry.save(registry_path)
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()

    before_refresh = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
    )
    assert before_refresh is not None
    assert "remote_science_inspect" not in {
        tool["name"] for tool in before_refresh["result"]["tools"]
    }

    refreshed_at = datetime.now(UTC)
    entry = _entry(registration, cluster="alpha", server_name="science").model_copy(
        update={
            "discovered_at": refreshed_at,
            "expires_at": refreshed_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(
        tmp_path / ".clio-relay" / "remote-mcp-cache.json",
        entry,
    )
    after_refresh = handle_request(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        queue=queue,
        profile="user",
    )
    assert after_refresh is not None
    assert "remote_science_inspect" in {tool["name"] for tool in after_refresh["result"]["tools"]}

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    job_id = response["result"]["structuredContent"]["job_id"]
    job = queue.get_job(job_id)
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.operation == McpOperation.TOOLS_CALL
    assert job.spec.server == "uvx"
    assert job.spec.server_args == [
        "--from",
        "/opt/wheels/science_kit-1.2.3-py3-none-any.whl",
        "science-mcp",
    ]
    assert job.spec.env_from == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert job.spec.expected_server_artifact_digest == remote_mcp_server_artifact_digest(
        _server_artifact(registration)
    )
    assert job.spec.tool == "inspect"
    assert job.spec.arguments == {"path": "/data/run"}
    assert job.spec.timeout_seconds == 300


def test_mcp_server_wraps_composed_schema_and_unwraps_remote_arguments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})})
    registry.save(tmp_path / ".clio-relay" / "clusters.json")
    discovered_at = datetime.now(UTC)
    composed_tool: dict[str, object] = {
        "name": "inspect",
        "description": "Inspect data through a composed contract.",
        "inputSchema": {
            "$ref": "#/$defs/Arguments",
            "$defs": {
                "Arguments": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "cluster": {"type": "string"},
                    },
                    "required": ["path", "cluster"],
                    "additionalProperties": False,
                }
            },
        },
    }
    entry = _entry(
        registration,
        cluster="alpha",
        server_name="science",
        tools=[composed_tool],
    ).model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(
        tmp_path / ".clio-relay" / "remote-mcp-cache.json",
        entry,
    )
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()

    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
    )
    assert listed is not None
    definition = next(
        tool for tool in listed["result"]["tools"] if tool["name"] == "remote_science_inspect"
    )
    assert definition["inputSchema"]["required"] == ["cluster", "arguments"]
    assert definition["inputSchema"]["properties"]["arguments"]["$ref"] == ("#/$defs/Arguments")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {
                    "cluster": "alpha",
                    "arguments": {"path": "/data/run", "cluster": "business-region"},
                },
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    job = queue.get_job(response["result"]["structuredContent"]["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"path": "/data/run", "cluster": "business-region"}


def test_mcp_tools_list_keeps_static_safety_surface_when_remote_cache_is_corrupt(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    cache_path.write_text("{not-json", encoding="utf-8")
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()

    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
    )
    context = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "relay_remote_mcp_context", "arguments": {}},
        },
        queue=queue,
        profile="user",
    )

    assert listed is not None
    listed_names = {tool["name"] for tool in listed["result"]["tools"]}
    assert "relay_status" in listed_names
    assert "remote_science_inspect" not in listed_names
    assert context is not None
    issues = context["result"]["structuredContent"]["catalog_issues"]
    assert len(issues) == 1
    assert issues[0]["cluster"] == "*"
    assert "catalog unavailable" in issues[0]["reason"]


def test_cli_registers_and_submits_durable_tools_list_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("SITE_SCIENCE_TOKEN", "must-not-be-persisted")
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    runner = CliRunner()

    registered = runner.invoke(
        app,
        [
            "remote-mcp",
            "register",
            "--cluster",
            "alpha",
            "--name",
            "science",
            "--command",
            "uvx",
            "--arg=--from",
            "--arg",
            "science-kit",
            "--arg",
            "science-mcp",
            "--allow-tool",
            "inspect",
            "--env-from",
            "SCIENCE_TOKEN=SITE_SCIENCE_TOKEN",
            "--profile",
            "user",
        ],
    )
    submitted = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "alpha",
            "--server",
            "uvx",
            "--server-arg=--from",
            "--server-arg",
            "science-kit",
            "--server-arg",
            "science-mcp",
            "--operation",
            "tools/list",
            "--idempotency-key",
            "discovery-test",
        ],
    )

    assert registered.exit_code == 0, registered.output
    assert json.loads(registered.output)["cache_reusable"] is False
    stored = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    assert stored.require("alpha").remote_mcp_servers["science"].enabled is True
    assert stored.require("alpha").remote_mcp_servers["science"].env_from == {
        "SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"
    }
    assert "must-not-be-persisted" not in (tmp_path / ".clio-relay" / "clusters.json").read_text(
        encoding="utf-8"
    )
    assert submitted.exit_code == 0, submitted.output
    job = ClioCoreQueue(tmp_path / "core").get_job(submitted.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.operation == McpOperation.TOOLS_LIST
    assert job.spec.env_from == {}
    assert job.spec.tool is None
    assert job.spec.arguments == {}


def test_cli_refresh_ingests_only_terminal_discovery_artifact(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    registration = _registration(
        profiles=["user"], env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    )
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )

    def complete_discovery(
        queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        del timeout_seconds, poll_seconds
        job = queue.get_job(job_id)
        assert isinstance(job.spec, McpCallSpec)
        assert job.spec.operation == McpOperation.TOOLS_LIST
        assert job.spec.env_from == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
        spool = JobSpool(tmp_path / "spool", job)
        spool.initialize()
        result_path = spool.path / "mcp-result.json"
        result_path.write_bytes(
            _discovery_artifact(registration, tools=[_tool("inspect", required=["path"])])
        )
        queue.append_artifact(spool.artifact_for(result_path, kind="mcp_result"))
        return queue.update_job_state(job_id, JobState.SUCCEEDED, message="discovery complete")

    monkeypatch.setattr("clio_relay.cli.wait_for_terminal", complete_discovery)

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "refresh",
            "--cluster",
            "alpha",
            "--name",
            "science",
            "--idempotency-key",
            "refresh-test",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cache_entry"]["provenance"]["discovery_job_id"] == payload["discovery_job_id"]
    assert payload["profiles"]["user"]["virtual_tools"] == ["remote_science_inspect"]
    cache = RemoteMcpSchemaCache.load(tmp_path / ".clio-relay" / "remote-mcp-cache.json")
    assert cache.entry_for("alpha", "science") is not None


def test_acceptance_report_has_canonical_checks_and_durable_provenance() -> None:
    registration = _registration(namespace="science", profiles=["user"])
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"spack": registration})})
    cache = RemoteMcpSchemaCache(
        entries=[_entry(registration, cluster="alpha", server_name="spack")]
    )
    job_id = "job_virtual_call"
    job: dict[str, object] = {
        "job_id": job_id,
        "cluster": "alpha",
        "kind": "mcp_call",
        "state": "succeeded",
        "spec": {
            "server": registration.command,
            "server_args": registration.args,
            "operation": "tools/call",
            "tool": "inspect",
            "arguments": {"path": "/data/run"},
        },
    }
    artifacts = [
        {"artifact_id": f"artifact_{kind}", "job_id": job_id, "kind": kind, "sha256": kind}
        for kind in ("stdout", "stderr", "mcp_result", "provenance")
    ]

    report = build_remote_mcp_acceptance_report(
        registry=registry,
        cache=cache,
        cluster="alpha",
        server_name="spack",
        remote_tool_name="inspect",
        profile="user",
        call_job_id=job_id,
        call_status={"job": job, "terminal": True},
        artifacts=artifacts,
        mcp_result={
            "server": registration.command,
            "server_args": registration.args,
            "operation": "tools/call",
            "tool": "inspect",
            "arguments": {"path": "/data/run"},
            "returncode": 0,
            "protocol_result": {"content": [{"type": "text", "text": "ok"}]},
            "server_artifact": _server_artifact(registration),
            "protocol_error": None,
        },
        provenance={"job": job},
        now=NOW,
    )

    assert report.passed is True
    assert report.virtual_alias == "remote_science_inspect"
    assert [check.name for check in report.checks] == [
        "remote-mcp.register",
        "remote-mcp.discover",
        "remote-mcp.tools-list",
        "remote-mcp.call",
        "remote-mcp.server-artifact",
        "remote-mcp.durable-result",
    ]
    assert all(check.passed for check in report.checks)
    assert report.discovery["provenance"]["discovery_job_id"] == "job_alpha_spack"
    canonical = report.to_live_validation_report()
    call_resource = next(
        resource for resource in canonical.resources if resource.role == "virtual_remote_mcp_call"
    )
    assert call_resource.metadata["remote_mcp_server_name"] == "spack"
    assert call_resource.metadata["remote_mcp_tool_name"] == "inspect"
    assert call_resource.metadata["virtual_alias"] == "remote_science_inspect"
    server_resource = next(
        resource for resource in canonical.resources if resource.kind == "mcp_server"
    )
    assert server_resource.state == "verified"
    assert server_resource.metadata["install_source"] == "wheel"


def test_spack_acceptance_enforces_exact_stateless_user_contract(
    monkeypatch: MonkeyPatch,
) -> None:
    expected_names = ["spack_find", "spack_locate", "spack_install"]
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/clio/clio_kit-3.0.0-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "spack",
        ],
        allow_tools=expected_names,
        profiles=["user"],
        contract="clio-kit-spack-user-v3",
    )
    registry = ClusterRegistry(
        clusters={"alpha": _cluster("alpha", {"site-software": registration})}
    )
    job_id = "job_spack_find"
    job: dict[str, object] = {
        "job_id": job_id,
        "cluster": "alpha",
        "kind": "mcp_call",
        "state": "succeeded",
        "spec": {
            "server": registration.command,
            "server_args": registration.args,
            "env_from": {},
            "operation": "tools/call",
            "tool": "spack_find",
            "arguments": {},
        },
    }
    artifacts = [
        {"artifact_id": f"artifact_{kind}", "job_id": job_id, "kind": kind, "sha256": kind}
        for kind in ("stdout", "stderr", "mcp_result", "provenance")
    ]

    def build(tools: list[dict[str, object]]) -> RemoteMcpAcceptanceReport:
        artifact_payload = _discovery_artifact(registration, tools=tools)
        entry = cache_entry_from_discovery_artifact(
            cluster="alpha",
            server_name="site-software",
            registration=registration,
            discovery_job_id="job_spack_discovery",
            artifact_id="artifact_spack_schema",
            artifact_sha256=hashlib.sha256(artifact_payload).hexdigest(),
            artifact_payload=artifact_payload,
            discovered_at=NOW,
        )
        return build_remote_mcp_acceptance_report(
            registry=registry,
            cache=RemoteMcpSchemaCache(entries=[entry]),
            cluster="alpha",
            server_name="site-software",
            remote_tool_name="spack_find",
            profile="user",
            call_job_id=job_id,
            call_status={"job": job, "terminal": True},
            artifacts=artifacts,
            mcp_result={
                "server": registration.command,
                "server_args": registration.args,
                "env_from": {},
                "operation": "tools/call",
                "tool": "spack_find",
                "arguments": {},
                "returncode": 0,
                "protocol_result": {"content": [{"type": "text", "text": "ok"}]},
                "server_artifact": _server_artifact(registration),
                "protocol_error": None,
            },
            provenance={"job": job},
            now=NOW,
        )

    exact_tools = [_spack_tool(name) for name in expected_names]
    exact_artifact_payload = _discovery_artifact(registration, tools=exact_tools)
    expected_entry = cache_entry_from_discovery_artifact(
        cluster="alpha",
        server_name="site-software",
        registration=registration,
        discovery_job_id="job_spack_discovery",
        artifact_id="artifact_spack_schema",
        artifact_sha256=hashlib.sha256(exact_artifact_payload).hexdigest(),
        artifact_payload=exact_artifact_payload,
        discovered_at=NOW,
    )
    monkeypatch.setattr(
        "clio_relay.remote_mcp.CLIO_KIT_SPACK_USER_CONTRACT_SHA256",
        expected_entry.schema_digest,
    )

    exact_report = build(exact_tools)
    contract = next(
        check for check in exact_report.checks if check.name == "remote-mcp.spack-user-contract"
    )

    assert exact_report.passed is True
    assert contract.passed is True
    assert contract.evidence["stateful_load_exposed"] is False
    server_resource = next(
        resource
        for resource in exact_report.to_live_validation_report().resources
        if resource.kind == "mcp_server"
    )
    assert server_resource.metadata["remote_tool_names"] == sorted(expected_names)
    assert server_resource.metadata["allowlisted_tool_names"] == sorted(expected_names)

    drifted_report = build(
        [*[_spack_tool(name) for name in expected_names], _spack_tool("spack_load")]
    )
    drifted_contract = next(
        check for check in drifted_report.checks if check.name == "remote-mcp.spack-user-contract"
    )
    assert drifted_report.passed is False
    assert drifted_contract.passed is False
    assert drifted_contract.evidence["stateful_load_exposed"] is True


def test_declared_spack_contract_fails_closed_before_catalog_exposure() -> None:
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/clio/clio_kit-3.0.0-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "spack",
        ],
        allow_tools=["*"],
        profiles=["user"],
        contract="clio-kit-spack-user-v3",
    )
    entry = _entry(
        registration,
        cluster="alpha",
        server_name="software",
        tools=[
            _spack_tool("spack_find"),
            _spack_tool("spack_locate"),
            _spack_tool("spack_install"),
            _spack_tool("spack_load"),
        ],
    )
    catalog = build_virtual_remote_mcp_catalog(
        ClusterRegistry(clusters={"alpha": _cluster("alpha", {"software": registration})}),
        RemoteMcpSchemaCache(entries=[entry]),
        profile="user",
        now=NOW,
    )

    assert catalog.tools == {}
    assert len(catalog.issues) == 1
    assert "declared contract" in catalog.issues[0].reason


@pytest.mark.parametrize(
    ("remote_schema", "arguments_json", "expected_remote_arguments"),
    [
        (None, '{"path":"/data/run"}', {"path": "/data/run"}),
        (
            {
                "type": "object",
                "properties": {"cluster": {"type": "string"}},
                "required": ["cluster"],
                "additionalProperties": False,
            },
            '{"cluster":"remote-native-cluster"}',
            {"cluster": "remote-native-cluster"},
        ),
        (
            {
                "type": "object",
                "allOf": [
                    {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"],
                        "additionalProperties": False,
                    }
                ],
            },
            '{"path":"/data/composed"}',
            {"path": "/data/composed"},
        ),
    ],
)
def test_cli_validate_calls_virtual_alias_and_writes_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    remote_schema: dict[str, object] | None,
    arguments_json: str,
    expected_remote_arguments: dict[str, object],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    registration = _registration(profiles=["user"])
    registry = ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})})
    registry.save(tmp_path / ".clio-relay" / "clusters.json")
    refreshed_at = datetime.now(UTC)
    tool = _tool("inspect", required=["path"])
    if remote_schema is not None:
        tool["inputSchema"] = remote_schema
    discovery_entry = _entry(
        registration,
        cluster="alpha",
        server_name="science",
        tools=[tool],
    ).model_copy(
        update={
            "discovered_at": refreshed_at,
            "expires_at": refreshed_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(
        tmp_path / ".clio-relay" / "remote-mcp-cache.json",
        discovery_entry,
    )

    def complete_virtual_call(
        queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        del timeout_seconds, poll_seconds
        job = queue.get_job(job_id)
        assert isinstance(job.spec, McpCallSpec)
        assert job.spec.operation == McpOperation.TOOLS_CALL
        assert job.spec.arguments == expected_remote_arguments
        spool = JobSpool(tmp_path / "spool", job)
        spool.initialize()
        mcp_result_path = spool.path / "mcp-result.json"
        mcp_result_path.write_text(
            json.dumps(
                {
                    "server": registration.command,
                    "server_args": registration.args,
                    "operation": "tools/call",
                    "tool": "inspect",
                    "arguments": expected_remote_arguments,
                    "returncode": 0,
                    "protocol_result": {"content": [{"type": "text", "text": "ok"}]},
                    "server_artifact": _server_artifact(registration),
                    "protocol_error": None,
                    "timed_out": False,
                }
            ),
            encoding="utf-8",
        )
        provenance_path = spool.path / "provenance.json"
        provenance_path.write_text(
            json.dumps({"job": job.model_dump(mode="json")}, default=str),
            encoding="utf-8",
        )
        for kind, path in (
            ("stdout", spool.path / "stdout.log"),
            ("stderr", spool.path / "stderr.log"),
            ("mcp_result", mcp_result_path),
            ("provenance", provenance_path),
        ):
            queue.append_artifact(spool.artifact_for(path, kind=kind))
        return queue.update_job_state(job_id, JobState.SUCCEEDED, message="call complete")

    monkeypatch.setattr("clio_relay.cli.wait_for_terminal", complete_virtual_call)
    output_path = tmp_path / "validation" / "remote-mcp.json"

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "validate",
            "--cluster",
            "alpha",
            "--name",
            "science",
            "--tool",
            "inspect",
            "--arguments-json",
            arguments_json,
            "--profile",
            "user",
            "--output-json",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["passed"] is True
    assert report["virtual_alias"] == "remote_science_inspect"
    assert report["mcp_stdio"]["boundary"] == "packaged_clio_relay_mcp_server_stdio"
    assert report["mcp_stdio"]["returncode"] == 0
    assert report["mcp_stdio"]["command"][1:] == ["mcp-server", "--profile", "user"]
    assert [check["name"] for check in report["checks"]] == [
        "remote-mcp.register",
        "remote-mcp.discover",
        "remote-mcp.tools-list",
        "remote-mcp.call",
        "remote-mcp.server-artifact",
        "remote-mcp.durable-result",
    ]
    assert json.loads(output_path.read_text(encoding="utf-8")) == report


def test_cli_validate_preflight_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    report_path = tmp_path / "validation" / "preflight-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "validate",
            "--cluster",
            "alpha",
            "--name",
            "missing",
            "--tool",
            "inspect",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "remote-mcp.preflight"


def _registration(
    *,
    command: str = "uvx",
    namespace: str | None = None,
    profiles: list[str] | None = None,
    env_from: dict[str, str] | None = None,
) -> RemoteMcpServerConfig:
    return RemoteMcpServerConfig.model_validate(
        {
            "command": command,
            "args": [
                "--from",
                "/opt/wheels/science_kit-1.2.3-py3-none-any.whl",
                "science-mcp",
            ],
            "env_from": env_from or {},
            "namespace": namespace,
            "allow_tools": ["inspect"],
            "profiles": profiles or ["admin"],
            "schema_cache_ttl_seconds": 3600,
        }
    )


def _cluster(
    name: str,
    registrations: dict[str, RemoteMcpServerConfig],
) -> ClusterDefinition:
    return ClusterDefinition(
        name=name,
        ssh_host="localhost",
        remote_mcp_servers=registrations,
    )


def _tool(name: str, *, required: list[str] | None = None) -> dict[str, object]:
    return {
        "name": name,
        "title": "Inspect science data",
        "description": "Inspect a remote science dataset.",
        "inputSchema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": required or [],
            "additionalProperties": False,
        },
        "outputSchema": {
            "type": "object",
            "properties": {"count": {"type": "integer"}},
        },
        "annotations": {"readOnlyHint": True},
    }


def _spack_tool(name: str) -> dict[str, object]:
    """Return one representative schema from the audited Spack MCP surface."""
    read_only = name != "spack_install"
    required = ["spec"] if name in {"spack_locate", "spack_install"} else []
    properties: dict[str, object] = {}
    if "spec" in required:
        properties["spec"] = {"type": "string"}
    tool: dict[str, object] = {
        "name": name,
        "description": f"Audited {name} operation.",
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": False,
            "idempotentHint": read_only,
            "openWorldHint": name == "spack_install",
        },
    }
    if name == "spack_locate":
        tool["outputSchema"] = {
            "type": "object",
            "properties": {"load_spec": {"type": "string"}},
            "required": ["load_spec"],
            "additionalProperties": False,
        }
    return tool


def _discovery_artifact(
    registration: RemoteMcpServerConfig,
    *,
    tools: list[dict[str, object]],
) -> bytes:
    return json.dumps(
        {
            "server": registration.command,
            "server_args": registration.args,
            "env_from": registration.env_from,
            "operation": "tools/list",
            "tool": None,
            "arguments": {},
            "protocol_result": {"tools": tools},
            "structured_result": None,
            "protocol_version": "2024-11-05",
            "server_info": {"name": "science", "version": "1.2.3"},
            "server_artifact": _server_artifact(registration),
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "protocol_error": None,
        }
    ).encode()


def _server_artifact(registration: RemoteMcpServerConfig) -> dict[str, object]:
    install_index = registration.args.index("--from") + 1
    install_spec = registration.args[install_index]
    install_file = {
        "path": install_spec,
        "filename": Path(install_spec).name,
        "sha256": "c" * 64,
        "size_bytes": 2,
    }
    return {
        "requested_command": registration.command,
        "resolved_executable": "/opt/clio/bin/uvx",
        "executable": {
            "path": "/opt/clio/bin/uvx",
            "filename": "uvx",
            "sha256": "uvx-sha256",
            "size_bytes": 1,
        },
        "install_spec": install_spec,
        "install_source": "wheel",
        "install_artifact_sha256": "c" * 64,
        "input_files": [install_file],
        "launcher_artifact_verified": True,
        "nested_launcher": False,
        "server_process_artifact_verified": True,
        "identity_error": None,
        "verified": True,
    }


def _entry_from_payload(
    registration: RemoteMcpServerConfig,
    payload: bytes,
) -> RemoteMcpSchemaCacheEntry:
    return cache_entry_from_discovery_artifact(
        cluster="alpha",
        server_name="science",
        registration=registration,
        discovery_job_id="job_discovery",
        artifact_id="artifact_result",
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        artifact_payload=payload,
        discovered_at=NOW,
    )


def _entry(
    registration: RemoteMcpServerConfig,
    *,
    cluster: str,
    server_name: str,
    tools: list[dict[str, object]] | None = None,
) -> RemoteMcpSchemaCacheEntry:
    artifact_payload = _discovery_artifact(
        registration,
        tools=(tools if tools is not None else [_tool("inspect", required=["path"])]),
    )
    return cache_entry_from_discovery_artifact(
        cluster=cluster,
        server_name=server_name,
        registration=registration,
        discovery_job_id=f"job_{cluster}_{server_name}",
        artifact_id=f"artifact_{cluster}_{server_name}",
        artifact_sha256=hashlib.sha256(artifact_payload).hexdigest(),
        artifact_payload=artifact_payload,
        discovered_at=NOW,
    )
