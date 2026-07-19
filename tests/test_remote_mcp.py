from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import stat
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest
from click import unstyle
from jsonschema import Draft4Validator, Draft201909Validator, Draft202012Validator
from pydantic import ValidationError
from pytest import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli as relay_cli
from clio_relay import remote_mcp
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    RemoteMcpServerConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    jarvis_user_contract,
)
from clio_relay.mcp_server import McpSessionState, handle_request, serve_stdio
from clio_relay.models import JobKind, JobState, McpCallSpec, McpOperation, RelayJob
from clio_relay.remote_mcp import (
    MAX_REMOTE_MCP_CACHE_BYTES,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES,
    MAX_REMOTE_MCP_TOOLS_PER_SERVER,
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS,
    RemoteMcpAcceptanceReport,
    RemoteMcpDiscoveryProvenance,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpSpackInstallTransitionEvidence,
    RemoteMcpStructuredResultExpectation,
    RemoteMcpToolSchema,
    build_remote_mcp_acceptance_report,
    build_remote_mcp_spack_fresh_install_transition_report,
    build_remote_mcp_structured_result_check,
    build_virtual_remote_mcp_catalog,
    cache_entry_from_discovery_artifact,
    inject_cluster_argument,
    remote_mcp_server_artifact_digest,
)
from clio_relay.spool import JobSpool
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact

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

    catalog_registration = RemoteMcpServerConfig(
        command="clio-kit",
        allow_tools=["scientific_dataset_search", "scientific_dataset_describe"],
        profiles=["user"],
        contract="clio-kit-scientific-catalog-user-v1.1",
    )
    legacy_catalog_registration = RemoteMcpServerConfig(
        command="clio-kit",
        contract="clio-kit-scientific-catalog-user-v1",
    )
    assert catalog_registration.contract == "clio-kit-scientific-catalog-user-v1.1"
    assert legacy_catalog_registration.contract == "clio-kit-scientific-catalog-user-v1"

    current_spack_registration = RemoteMcpServerConfig(
        command="clio-kit",
        contract="clio-kit-spack-user-v2.1",
    )
    legacy_spack_registration = RemoteMcpServerConfig(
        command="clio-kit",
        contract="clio-kit-spack-user-v2",
    )
    assert current_spack_registration.contract == "clio-kit-spack-user-v2.1"
    assert legacy_spack_registration.contract == "clio-kit-spack-user-v2"

    with pytest.raises(ValidationError, match="exact names or '\\*' only"):
        RemoteMcpServerConfig(command="science-mcp", allow_tools=["inspect*"])
    with pytest.raises(ValidationError, match="must not be empty"):
        RemoteMcpServerConfig(command="science-mcp", profiles=[])
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        RemoteMcpServerConfig(command="science-mcp", call_timeout_seconds=0)
    with pytest.raises(ValidationError, match="less than or equal"):
        RemoteMcpServerConfig(command="science-mcp", schema_cache_ttl_seconds=10**100)
    with pytest.raises(ValidationError, match="clio-kit-spack-user-v2"):
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


@pytest.mark.parametrize(
    "contract_id",
    [
        "clio-kit-scientific-catalog-user-v1.1",
        "clio-kit-scientific-catalog-user-v1",
    ],
)
def test_scientific_catalog_contract_is_read_only_and_exact(
    monkeypatch: MonkeyPatch,
    contract_id: str,
) -> None:
    """The released catalog contract must be registrable and fail closed on drift."""
    expected_names = ["scientific_dataset_describe", "scientific_dataset_search"]
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/clio/clio_kit-2.5.0-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "scientific-catalog",
        ],
        allow_tools=expected_names,
        profiles=["user"],
        contract=cast(Any, contract_id),
    )
    tools = [_scientific_catalog_tool(name, contract_id=contract_id) for name in expected_names]
    entry = _entry(
        registration,
        cluster="alpha",
        server_name="scientific-catalog",
        tools=tools,
    )
    monkeypatch.setattr(
        remote_mcp,
        "CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID",
        {
            **remote_mcp.CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID,
            contract_id: entry.schema_digest,
        },
    )

    exact = remote_mcp._declared_contract_check(  # pyright: ignore[reportPrivateUsage]
        entry, registration
    )

    assert exact.passed is True
    assert exact.name == "remote-mcp.scientific-catalog-user-contract"
    assert exact.evidence["remote_tool_names"] == expected_names
    assert exact.evidence["allowlisted_tool_names"] == expected_names
    assert exact.evidence["declared_contract"] == contract_id
    if contract_id == "clio-kit-scientific-catalog-user-v1.1":
        assert exact.evidence["dataset_descriptor_handoff_required"] is True
        assert exact.evidence["dataset_descriptor_handoff_matches"] is True
    else:
        assert exact.evidence["dataset_descriptor_handoff_required"] is False
        assert exact.evidence["dataset_descriptor_handoff_matches"] is None

    drifted_tools = deepcopy(tools)
    cast(dict[str, object], drifted_tools[1]["annotations"])["openWorldHint"] = True
    drifted_entry = _entry(
        registration,
        cluster="alpha",
        server_name="scientific-catalog",
        tools=drifted_tools,
    )
    monkeypatch.setattr(
        remote_mcp,
        "CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID",
        {
            **remote_mcp.CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID,
            contract_id: drifted_entry.schema_digest,
        },
    )

    drifted = remote_mcp._declared_contract_check(  # pyright: ignore[reportPrivateUsage]
        drifted_entry, registration
    )

    assert drifted.passed is False
    assert drifted.evidence["annotations_match"] == {
        "scientific_dataset_describe": True,
        "scientific_dataset_search": False,
    }


def test_current_scientific_catalog_contract_requires_explicit_handoff(
    monkeypatch: MonkeyPatch,
) -> None:
    """The v1.1 declaration cannot pass with the ambiguous historical output shape."""
    contract_id = "clio-kit-scientific-catalog-user-v1.1"
    expected_names = ["scientific_dataset_describe", "scientific_dataset_search"]
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/wheels/clio_kit-2.5.17-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "scientific-catalog",
        ],
        allow_tools=expected_names,
        profiles=["user"],
        contract=contract_id,
    )
    tools = [
        _scientific_catalog_tool(
            name,
            contract_id="clio-kit-scientific-catalog-user-v1",
        )
        for name in expected_names
    ]
    entry = _entry(
        registration,
        cluster="alpha",
        server_name="scientific-catalog",
        tools=tools,
    )
    monkeypatch.setattr(
        remote_mcp,
        "CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID",
        {
            **remote_mcp.CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID,
            contract_id: entry.schema_digest,
        },
    )

    check = remote_mcp._declared_contract_check(  # pyright: ignore[reportPrivateUsage]
        entry, registration
    )

    assert check.passed is False
    assert check.evidence["dataset_descriptor_handoff_matches"] is False


def test_scientific_catalog_contract_projects_identity_to_live_report() -> None:
    """A live describe call proves and projects the v1.1 descriptor handoff."""
    contract_id = "clio-kit-scientific-catalog-user-v1.1"
    contract_sha256 = "80a9b583c26a084ff07d638ddf0c2c7d4325dbc8d4299931d0c4f3627cb8674c"
    dataset_id = "deep-water-impact-2018-yb31-first5"
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/wheels/clio_kit-2.5.17-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "scientific-catalog",
        ],
        allow_tools=["scientific_dataset_describe", "scientific_dataset_search"],
        profiles=["user"],
        contract=contract_id,
    )
    contract_artifact = json.loads(
        (
            Path(remote_mcp.__file__).with_name("_contracts") / "scientific-catalog-user-v1.1.json"
        ).read_text(encoding="utf-8")
    )
    tools = cast(list[dict[str, object]], contract_artifact["tools"])
    entry = _entry(
        registration,
        cluster="alpha",
        server_name="scientific-catalog",
        tools=tools,
    )
    registry = ClusterRegistry(
        clusters={"alpha": _cluster("alpha", {"scientific-catalog": registration})}
    )
    server_artifact = _server_artifact(registration)
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
    job_id = "job_catalog_describe"
    arguments = {"dataset_id": dataset_id}
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
            "tool": "scientific_dataset_describe",
            "arguments": arguments,
            "expected_server_artifact_digest": server_artifact_digest,
        },
    }
    structured = _scientific_catalog_describe_result(dataset_id)
    acceptance = build_remote_mcp_acceptance_report(
        registry=registry,
        cache=RemoteMcpSchemaCache(entries=[entry]),
        cluster="alpha",
        server_name="scientific-catalog",
        remote_tool_name="scientific_dataset_describe",
        profile="user",
        call_job_id=job_id,
        call_status={"job": job, "terminal": True},
        artifacts=[
            {
                "artifact_id": f"artifact_{kind}",
                "job_id": job_id,
                "kind": kind,
                "sha256": hashlib.sha256(kind.encode()).hexdigest(),
            }
            for kind in ("stdout", "stderr", "mcp_result", "provenance")
        ],
        mcp_result={
            "server": registration.command,
            "server_args": registration.args,
            "env_from": {},
            "operation": "tools/call",
            "tool": "scientific_dataset_describe",
            "arguments": arguments,
            "returncode": 0,
            "protocol_result": {"structuredContent": structured},
            "server_artifact": server_artifact,
            "expected_server_artifact_digest": server_artifact_digest,
            "observed_server_artifact_digest": server_artifact_digest,
            "protocol_error": None,
        },
        provenance={"job": job},
        now=NOW,
    )

    assert acceptance.passed is True
    catalog_result = next(
        check for check in acceptance.checks if check.name == "remote-mcp.scientific-catalog-result"
    )
    assert catalog_result.passed is True
    assert catalog_result.evidence["dataset_descriptor_handoff_matches"] is True
    assert catalog_result.evidence["descriptor_digest_matches"] is True
    report = acceptance.to_live_validation_report()

    server_resource = next(
        resource for resource in report.resources if resource.kind == "mcp_server"
    )
    assert server_resource.metadata["contract_id"] == contract_id
    assert server_resource.metadata["contract_sha256"] == contract_sha256
    call_resource = next(
        resource for resource in report.resources if resource.role == "virtual_remote_mcp_call"
    )
    assert call_resource.metadata["scientific_catalog_result_assertion"] == (
        catalog_result.evidence
    )


@pytest.mark.parametrize("drift", ["handoff", "digest"])
def test_scientific_catalog_result_rejects_descriptor_drift(drift: str) -> None:
    """Schema-valid output cannot substitute a divergent or unbound descriptor."""
    dataset_id = "deep-water-impact-2018-yb31-first5"
    structured = _scientific_catalog_describe_result(dataset_id)
    if drift == "handoff":
        nested_descriptor = cast(
            dict[str, object],
            cast(dict[str, object], structured["dataset"])["descriptor"],
        )
        cast(list[dict[str, object]], nested_descriptor["members"])[0]["location"] = (
            "/mnt/common/datasets-staging/scivis/drifted.vti"
        )
    else:
        structured["descriptor_sha256"] = "0" * 64
    contract_artifact = json.loads(
        (
            Path(remote_mcp.__file__).with_name("_contracts") / "scientific-catalog-user-v1.1.json"
        ).read_text(encoding="utf-8")
    )
    describe_tool = next(
        tool
        for tool in cast(list[dict[str, object]], contract_artifact["tools"])
        if tool["name"] == "scientific_dataset_describe"
    )

    check = remote_mcp._scientific_catalog_structured_result_check(  # pyright: ignore[reportPrivateUsage]
        arguments={"dataset_id": dataset_id},
        protocol_result={"structuredContent": structured},
        output_schema=cast(dict[str, object], describe_tool["outputSchema"]),
    )

    assert check.passed is False
    assert check.evidence["output_schema"]["structured_content_valid"] is True
    assert (
        check.evidence[
            "dataset_descriptor_handoff_matches"
            if drift == "handoff"
            else "descriptor_digest_matches"
        ]
        is False
    )


def test_scientific_catalog_result_rejects_nan_without_raising() -> None:
    """A non-finite descriptor fails into bounded evidence before hashing."""
    dataset_id = "deep-water-impact-2018-yb31-first5"
    structured = _scientific_catalog_describe_result(dataset_id)
    descriptor = cast(dict[str, object], structured["dataset_descriptor"])
    cast(list[float], descriptor["bounds"])[0] = float("nan")
    cast(dict[str, object], structured["dataset"])["descriptor"] = deepcopy(descriptor)

    check = remote_mcp._scientific_catalog_structured_result_check(  # pyright: ignore[reportPrivateUsage]
        arguments={"dataset_id": dataset_id},
        protocol_result={"structuredContent": structured},
        output_schema=_scientific_catalog_describe_output_schema(),
    )

    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    assert check.passed is False
    assert schema_evidence["structured_content_bounded"] is True
    assert schema_evidence["structured_content_finite"] is False
    assert "non-finite JSON number" in cast(str, schema_evidence["structured_content_guard_error"])
    assert check.evidence["computed_descriptor_sha256"] is None
    assert check.evidence["descriptor_digest_matches"] is False


def test_scientific_catalog_result_rejects_overdeep_content_without_traversing() -> None:
    """Unsafe nesting fails before schema evaluation or descriptor comparison."""
    dataset_id = "deep-water-impact-2018-yb31-first5"
    structured = _scientific_catalog_describe_result(dataset_id)
    nested: dict[str, object] = {"leaf": True}
    for _ in range(remote_mcp.MAX_REMOTE_MCP_JSON_DEPTH + 1):
        nested = {"child": nested}
    descriptor = cast(dict[str, object], structured["dataset_descriptor"])
    descriptor["unsafe_test_nesting"] = nested
    cast(dict[str, object], structured["dataset"])["descriptor"] = deepcopy(descriptor)

    check = remote_mcp._scientific_catalog_structured_result_check(  # pyright: ignore[reportPrivateUsage]
        arguments={"dataset_id": dataset_id},
        protocol_result={"structuredContent": structured},
        output_schema=_scientific_catalog_describe_output_schema(),
    )

    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    assert check.passed is False
    assert schema_evidence["structured_content_bounded"] is False
    assert "nesting levels" in cast(str, schema_evidence["structured_content_guard_error"])
    assert check.evidence["computed_descriptor_sha256"] is None
    assert check.evidence["dataset_descriptor_handoff_matches"] is False


def test_scientific_catalog_result_bounds_oversized_evidence() -> None:
    """Oversized catalog strings fail without being copied into report evidence."""
    dataset_id = "deep-water-impact-2018-yb31-first5"
    structured = _scientific_catalog_describe_result(dataset_id)
    structured["schema_version"] = "x" * (
        remote_mcp.MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES + 1
    )

    check = remote_mcp._scientific_catalog_structured_result_check(  # pyright: ignore[reportPrivateUsage]
        arguments={"dataset_id": dataset_id},
        protocol_result={"structuredContent": structured},
        output_schema=_scientific_catalog_describe_output_schema(),
    )

    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    assert check.passed is False
    assert schema_evidence["structured_content_bounded"] is False
    assert schema_evidence["structured_content_finite"] is True
    assert cast(int, schema_evidence["structured_content_bytes"]) > cast(
        int, schema_evidence["structured_content_bytes_limit"]
    )
    assert "exceeds" in cast(str, schema_evidence["structured_content_guard_error"])
    assert schema_evidence["schema_evaluated"] is False
    assert check.evidence["response_schema_version"] is None
    assert check.evidence["computed_descriptor_sha256"] is None
    assert len(json.dumps(check.evidence, allow_nan=False)) < 16_384


def test_scientific_catalog_result_hashes_unicode_like_clio_kit() -> None:
    """Unicode descriptor bytes use clio-kit's unescaped canonical JSON."""
    dataset_id = "deep-water-impact-2018-yb31-first5"
    structured = _scientific_catalog_describe_result(dataset_id)
    descriptor = cast(dict[str, object], structured["dataset_descriptor"])
    arrays = cast(list[dict[str, object]], descriptor["arrays"])
    arrays[0]["name"] = "temperature-Δ"
    identity = {key: value for key, value in descriptor.items() if key != "fingerprint"}
    fingerprint = cast(dict[str, object], descriptor["fingerprint"])
    fingerprint["digest"] = _canonical_json_sha256(identity)
    cast(dict[str, object], structured["dataset"])["descriptor"] = deepcopy(descriptor)
    expected_digest = _canonical_json_sha256(descriptor)
    ascii_escaped_digest = hashlib.sha256(
        json.dumps(
            descriptor,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    structured["descriptor_sha256"] = expected_digest

    check = remote_mcp._scientific_catalog_structured_result_check(  # pyright: ignore[reportPrivateUsage]
        arguments={"dataset_id": dataset_id},
        protocol_result={"structuredContent": structured},
        output_schema=_scientific_catalog_describe_output_schema(),
    )

    assert expected_digest != ascii_escaped_digest
    assert check.passed is True
    assert check.evidence["computed_descriptor_sha256"] == expected_digest
    assert check.evidence["descriptor_digest_matches"] is True


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


def test_virtual_remote_mcp_relay_envelope_is_local_and_collision_safe() -> None:
    """Relay wait and log controls never become arbitrary server arguments."""

    remote_schema: dict[str, object] = {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
        "additionalProperties": False,
    }
    rendered = inject_cluster_argument(remote_schema, clusters=["alpha"])
    properties = cast(dict[str, object], rendered["properties"])
    assert {
        "cluster",
        "query",
        "wait_for_terminal",
        "wait_timeout_seconds",
        "poll_seconds",
        "include_logs",
        "log_limit",
    }.issubset(properties)
    invocation = {
        "cluster": "alpha",
        "query": "asteroid",
        "wait_for_terminal": True,
        "wait_timeout_seconds": 45,
        "poll_seconds": 0.25,
        "include_logs": True,
        "log_limit": 1_024,
    }
    cast(_SchemaValidator, Draft202012Validator(rendered)).validate(invocation)
    virtual = remote_mcp.VirtualRemoteMcpTool(
        alias="remote_science_search",
        namespace="science",
        remote_tool=RemoteMcpToolSchema(name="search", input_schema=remote_schema),
        routes={},
        arguments_wrapped=False,
    )

    assert virtual.forwarded_arguments(invocation) == {"query": "asteroid"}
    assert virtual.relay_arguments(invocation) == {
        "wait_for_terminal": True,
        "wait_timeout_seconds": 45,
        "poll_seconds": 0.25,
        "include_logs": True,
        "log_limit": 1_024,
    }
    with pytest.raises(ValueError, match="wait_for_terminal must be a boolean"):
        virtual.relay_arguments({"wait_for_terminal": "yes"})

    colliding_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "wait_for_terminal": {"type": "boolean"},
        },
        "required": ["query", "wait_for_terminal"],
        "additionalProperties": False,
    }
    colliding = inject_cluster_argument(colliding_schema, clusters=["alpha"])
    assert colliding["required"] == ["cluster", "arguments"]
    cast(_SchemaValidator, Draft202012Validator(colliding)).validate(
        {
            "cluster": "alpha",
            "arguments": {"query": "asteroid", "wait_for_terminal": False},
            "wait_for_terminal": True,
        }
    )
    wrapped = remote_mcp.VirtualRemoteMcpTool(
        alias="remote_science_colliding_search",
        namespace="science",
        remote_tool=RemoteMcpToolSchema(name="search", input_schema=colliding_schema),
        routes={},
        arguments_wrapped=True,
    )
    wrapped_invocation = {
        "cluster": "alpha",
        "arguments": {"query": "asteroid", "wait_for_terminal": False},
        "wait_for_terminal": True,
    }
    assert wrapped.forwarded_arguments(wrapped_invocation) == {
        "query": "asteroid",
        "wait_for_terminal": False,
    }
    assert wrapped.relay_arguments(wrapped_invocation) == {"wait_for_terminal": True}

    open_schema: dict[str, object] = {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
    }
    open_rendered = inject_cluster_argument(open_schema, clusters=["alpha"])
    assert open_rendered["required"] == ["cluster", "arguments"]
    cast(_SchemaValidator, Draft202012Validator(open_rendered)).validate(
        {
            "cluster": "alpha",
            "arguments": {"query": "asteroid", "wait_for_terminal": "remote value"},
            "wait_for_terminal": True,
        }
    )


def test_cluster_injection_wraps_nonempty_root_id_without_changing_remote_schema() -> None:
    remote_schema: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.org/mcp/schemas/inspect-input.json",
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
        "additionalProperties": False,
    }
    before = deepcopy(remote_schema)

    rendered = inject_cluster_argument(remote_schema, clusters=["alpha"])

    assert remote_schema == before
    assert "$id" not in rendered
    assert rendered["$schema"] == before["$schema"]
    assert rendered["required"] == ["cluster", "arguments"]
    assert rendered["properties"]["arguments"] == before
    cast(_SchemaValidator, Draft202012Validator(rendered)).validate(
        {"cluster": "alpha", "arguments": {"path": "/data/run"}}
    )


def test_cluster_injection_flattens_empty_root_id_and_ingestion_rejects_invalid_id() -> None:
    empty_identifier: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "",
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "additionalProperties": False,
    }

    rendered = inject_cluster_argument(empty_identifier, clusters=["alpha"])

    assert rendered["$id"] == ""
    assert "arguments" not in rendered["properties"]
    assert rendered["properties"]["cluster"]["enum"] == ["alpha"]
    with pytest.raises(ValidationError, match="not valid JSON Schema"):
        RemoteMcpToolSchema(
            name="invalid-id",
            input_schema={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": 7,
                "type": "object",
            },
        )


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


def test_catalog_aliases_remain_interoperable_for_long_remote_names() -> None:
    """Arbitrary server/tool names produce deterministic MCP-compatible aliases."""
    namespace = "science_" + "namespace" * 20
    first_name = "inspect-" + "dataset" * 60
    second_name = "inspect_" + "dataset" * 60
    registration = _registration(namespace=namespace, profiles=["user"]).model_copy(
        update={"allow_tools": [first_name, second_name]}
    )
    registry = ClusterRegistry(
        clusters={"alpha": _cluster("alpha", {"long-science-server": registration})}
    )
    cache = RemoteMcpSchemaCache(
        entries=[
            _entry(
                registration,
                cluster="alpha",
                server_name="long-science-server",
                tools=[_tool(first_name), _tool(second_name)],
            )
        ]
    )

    first = build_virtual_remote_mcp_catalog(registry, cache, profile="user", now=NOW)
    second = build_virtual_remote_mcp_catalog(registry, cache, profile="user", now=NOW)

    assert sorted(first.tools) == sorted(second.tools)
    assert len(first.tools) == 2
    assert all(
        len(alias) <= remote_mcp.MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH for alias in first.tools
    )
    assert all(re.fullmatch(r"[a-z0-9_]+", alias) is not None for alias in first.tools)


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
    session = McpSessionState()

    before_refresh = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
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
        session=session,
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
        session=session,
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
    assert response["result"]["structuredContent"]["catalog_revision"] == (
        session.observed_remote_mcp_catalog_revision(profile="user")
    )


def test_mcp_server_executes_cached_alias_on_fresh_session(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A lazy MCP client may execute a cached tool without listing on its new connection."""
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    discovered_at = datetime.now(UTC)
    entry = _entry(registration, cluster="alpha", server_name="science").model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(
        tmp_path / ".clio-relay" / "remote-mcp-cache.json",
        entry,
    )
    discovery_queue = ClioCoreQueue(tmp_path / "discovery-core")
    discovery_queue.initialize()

    discovery_session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=discovery_queue,
        profile="user",
        session=discovery_session,
    )
    assert listed is not None
    assert "remote_science_inspect" in {tool["name"] for tool in listed["result"]["tools"]}
    cached_revision = listed["result"]["_meta"]["clio-relay/remote-mcp-catalog-revision"]

    stdout = StringIO()
    requests = [
        {"jsonrpc": "2.0", "id": 2, "method": "initialize"},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
    ]
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    serve_stdio(
        stdin=StringIO("".join(f"{json.dumps(request)}\n" for request in requests)),
        stdout=stdout,
        settings=settings,
        profile="user",
    )

    responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [response["id"] for response in responses] == [2, 3]
    response = responses[1]
    structured = response["result"]["structuredContent"]
    assert structured["catalog_revision"] == cached_revision
    assert structured["route_revision"] == cluster_route_revision(
        _cluster("alpha", {"science": registration})
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    job = queue.get_job(structured["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == remote_mcp_server_artifact_digest(
        _server_artifact(registration)
    )
    assert job.spec.arguments == {"path": "/data/run"}


def test_mcp_server_fresh_session_rejects_alias_removed_from_current_catalog(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A cached alias fails closed when its registration no longer matches discovery."""
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}).save(
        registry_path
    )
    discovered_at = datetime.now(UTC)
    entry = _entry(registration, cluster="alpha", server_name="science").model_copy(
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

    discovery_session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=discovery_session,
    )
    assert listed is not None
    assert "remote_science_inspect" in {tool["name"] for tool in listed["result"]["tools"]}

    changed_registration = _registration(command="science-mcp-v2", profiles=["user"])
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": changed_registration})}).save(
        registry_path
    )
    execution_session = McpSessionState()
    initialized = handle_request(
        {"jsonrpc": "2.0", "id": 2, "method": "initialize"},
        queue=queue,
        profile="user",
        session=execution_session,
    )
    assert initialized is not None

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
        session=execution_session,
    )

    assert response is not None
    assert "tool is not available in MCP profile 'user'" in response["error"]["message"]
    assert queue.list_jobs() == []


def test_mcp_server_rejects_stable_alias_after_schema_revision_changes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"science": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    discovered_at = datetime.now(UTC)
    original_entry = _entry(
        registration,
        cluster="alpha",
        server_name="science",
        tools=[_tool("inspect", required=["path"])],
    ).model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    RemoteMcpSchemaCache.update_entry(cache_path, original_entry)
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    session = McpSessionState()

    original_list = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert original_list is not None
    original_definition = next(
        tool
        for tool in original_list["result"]["tools"]
        if tool["name"] == "remote_science_inspect"
    )
    assert set(original_definition["inputSchema"]["properties"]) == {
        "cluster",
        "path",
        *VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS,
    }
    assert "catalog_revision" not in original_definition["inputSchema"]["properties"]
    original_revision = session.observed_remote_mcp_catalog_revision(profile="user")
    assert original_revision is not None
    assert original_list["result"]["_meta"] == {
        "clio-relay/remote-mcp-catalog-revision": original_revision,
        "clio-relay/profile": "user",
    }

    changed_tool = _tool("inspect", required=["path", "mode"])
    changed_schema = cast(dict[str, object], changed_tool["inputSchema"])
    changed_properties = cast(dict[str, object], changed_schema["properties"])
    changed_properties["mode"] = {"type": "string", "enum": ["summary", "full"]}
    changed_entry = _entry(
        registration,
        cluster="alpha",
        server_name="science",
        tools=[changed_tool],
    ).model_copy(
        update={
            "discovered_at": discovered_at + timedelta(seconds=1),
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(cache_path, changed_entry)

    stale_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )
    assert stale_call is not None
    assert stale_call["error"]["code"] == -32000
    assert "catalog changed after tools/list" in stale_call["error"]["message"]

    refreshed_list = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert refreshed_list is not None
    refreshed_definition = next(
        tool
        for tool in refreshed_list["result"]["tools"]
        if tool["name"] == "remote_science_inspect"
    )
    assert set(refreshed_definition["inputSchema"]["properties"]) == {
        "cluster",
        "path",
        "mode",
        *VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS,
    }
    assert "catalog_revision" not in refreshed_definition["inputSchema"]["properties"]
    refreshed_revision = session.observed_remote_mcp_catalog_revision(profile="user")
    assert refreshed_revision is not None
    assert refreshed_revision != original_revision
    assert refreshed_list["result"]["_meta"] == {
        "clio-relay/remote-mcp-catalog-revision": refreshed_revision,
        "clio-relay/profile": "user",
    }

    refreshed_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {
                    "cluster": "alpha",
                    "path": "/data/run",
                    "mode": "summary",
                },
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )
    assert refreshed_call is not None
    structured = refreshed_call["result"]["structuredContent"]
    assert structured["catalog_revision"] == refreshed_revision
    cast(
        _SchemaValidator,
        Draft202012Validator(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA),
    ).validate(structured)
    job = queue.get_job(structured["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"path": "/data/run", "mode": "summary"}


def test_mcp_session_rejects_cluster_route_rebind_and_recovers_after_relist(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    original_cluster = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-old",
        remote_mcp_servers={"science": registration},
    )
    ClusterRegistry(clusters={"alpha": original_cluster}).save(registry_path)
    discovered_at = datetime.now(UTC)
    entry = _entry(registration, cluster="alpha", server_name="science").model_copy(
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
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None
    assert "remote_science_inspect" in {tool["name"] for tool in listed["result"]["tools"]}

    rebound_cluster = original_cluster.model_copy(update={"ssh_host": "alpha-new"})
    ClusterRegistry(clusters={"alpha": rebound_cluster}).save(registry_path)
    writes: list[str] = []
    remote_commands: list[tuple[str, list[str]]] = []
    removals: list[str] = []

    def write_remote(
        _definition: ClusterDefinition,
        path: str,
        _data: bytes,
    ) -> None:
        writes.append(path)

    def run_remote(definition: ClusterDefinition, args: list[str]) -> str:
        remote_commands.append((definition.ssh_host, args))
        return "job-rebound-route\n"

    def remove_remote(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool = False,
    ) -> None:
        del remove_empty_parent
        removals.append(path)

    monkeypatch.setattr("clio_relay.mcp_server.write_remote_file", write_remote)
    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", run_remote)
    monkeypatch.setattr("clio_relay.mcp_server.remove_remote_file", remove_remote)
    call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "remote_science_inspect",
            "arguments": {"cluster": "alpha", "path": "/data/run"},
        },
    }
    stale = handle_request(
        call,
        queue=queue,
        profile="user",
        session=session,
    )
    assert stale is not None
    assert "catalog changed after tools/list" in stale["error"]["message"]
    assert queue.list_jobs() == []
    assert writes == []
    assert remote_commands == []

    refreshed = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert refreshed is not None
    recovered = handle_request(
        {**call, "id": 4},
        queue=queue,
        profile="user",
        session=session,
    )
    assert recovered is not None
    assert recovered["result"]["structuredContent"]["job_id"] == "job-rebound-route"
    assert remote_commands[0][0] == "alpha-new"
    assert len(writes) == 1
    assert removals == writes


def test_mcp_dispatch_rejects_registration_revocation_during_final_route_read(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _registration(profiles=["user"])
    registered = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        remote_mcp_servers={"science": registration},
    )
    ClusterRegistry(clusters={"alpha": registered}).save(tmp_path / ".clio-relay" / "clusters.json")
    discovered_at = datetime.now(UTC)
    entry = _entry(registration, cluster="alpha", server_name="science").model_copy(
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
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None

    revoked = registered.model_copy(update={"remote_mcp_servers": {}})

    def revoked_cluster(_cluster: str) -> ClusterDefinition:
        return revoked

    monkeypatch.setattr(
        "clio_relay.mcp_server._remote_cluster_definition",
        revoked_cluster,
    )
    writes: list[str] = []
    remote_commands: list[list[str]] = []

    def unexpected_write(
        _definition: ClusterDefinition,
        path: str,
        _data: bytes,
    ) -> None:
        writes.append(path)

    def unexpected_remote(
        _definition: ClusterDefinition,
        args: list[str],
    ) -> str:
        remote_commands.append(args)
        return "unexpected\n"

    monkeypatch.setattr(
        "clio_relay.mcp_server.write_remote_file",
        unexpected_write,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.run_remote_clio",
        unexpected_remote,
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )

    assert response is not None
    assert "remote MCP registration changed" in response["error"]["message"]
    assert queue.list_jobs() == []
    assert writes == []
    assert remote_commands == []


def test_mcp_session_rejects_jarvis_artifact_rebind_and_recovers_after_relist(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    original_entry, _ = _jarvis_cache_entry(cluster="alpha", executable_sha="a" * 64)
    RemoteMcpSchemaCache.update_entry(cache_path, original_entry)
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    session = McpSessionState()
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None
    assert "jarvis_run" in {tool["name"] for tool in listed["result"]["tools"]}

    rebound_entry, rebound_artifact = _jarvis_cache_entry(
        cluster="alpha",
        executable_sha="b" * 64,
    )
    RemoteMcpSchemaCache.update_entry(cache_path, rebound_entry)
    call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "jarvis_run",
            "arguments": {"cluster": "alpha", "pipeline_id": "gray-scott"},
        },
    }
    stale = handle_request(
        call,
        queue=queue,
        profile="user",
        session=session,
    )
    assert stale is not None
    assert "catalog changed after tools/list" in stale["error"]["message"]
    assert queue.list_jobs() == []

    refreshed = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert refreshed is not None
    advertised = next(tool for tool in refreshed["result"]["tools"] if tool["name"] == "jarvis_run")
    advertised_revision = refreshed["result"]["_meta"]["clio-relay/remote-mcp-catalog-revision"]
    recovered = handle_request(
        {**call, "id": 4},
        queue=queue,
        profile="user",
        session=session,
    )
    assert recovered is not None
    structured = recovered["result"]["structuredContent"]
    cast(_SchemaValidator, Draft202012Validator(advertised["outputSchema"])).validate(structured)
    assert structured["catalog_revision"] == advertised_revision
    assert structured["catalog_revision"] == session.observed_remote_mcp_catalog_revision(
        profile="user"
    )
    job = queue.get_job(structured["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == remote_mcp_server_artifact_digest(
        rebound_artifact
    )


def test_mcp_server_rejects_alias_after_collision_reassigns_catalog(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    primary = _registration(namespace="science", profiles=["user"])
    secondary = _registration(namespace="science", profiles=["user"])
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"primary": primary})}).save(registry_path)
    discovered_at = datetime.now(UTC)
    primary_entry = _entry(
        primary,
        cluster="alpha",
        server_name="primary",
        tools=[_tool("inspect", required=["path"])],
    ).model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    RemoteMcpSchemaCache.update_entry(cache_path, primary_entry)
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    session = McpSessionState()

    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert listed is not None
    assert "remote_science_inspect" in {tool["name"] for tool in listed["result"]["tools"]}

    alternate_tool = _tool("inspect", required=["sample"])
    alternate_schema = cast(dict[str, object], alternate_tool["inputSchema"])
    alternate_properties = cast(dict[str, object], alternate_schema["properties"])
    alternate_properties.clear()
    alternate_properties["sample"] = {"type": "string"}
    ClusterRegistry(
        clusters={
            "alpha": _cluster(
                "alpha",
                {"primary": primary, "secondary": secondary},
            )
        }
    ).save(registry_path)
    secondary_entry = _entry(
        secondary,
        cluster="alpha",
        server_name="secondary",
        tools=[alternate_tool],
    ).model_copy(
        update={
            "discovered_at": discovered_at + timedelta(seconds=1),
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(cache_path, secondary_entry)

    stale_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "remote_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )
    assert stale_call is not None
    assert "catalog changed after tools/list" in stale_call["error"]["message"]

    collision_list = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert collision_list is not None
    collision_definitions = [
        tool
        for tool in collision_list["result"]["tools"]
        if tool["name"].startswith("remote_science_inspect_")
    ]
    assert len(collision_definitions) == 2
    assert all(tool["name"] != "remote_science_inspect" for tool in collision_definitions)
    path_definition = next(
        tool for tool in collision_definitions if "path" in tool["inputSchema"]["properties"]
    )

    collision_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": path_definition["name"],
                "arguments": {"cluster": "alpha", "path": "/data/run"},
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )
    assert collision_call is not None
    job = queue.get_job(collision_call["result"]["structuredContent"]["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"path": "/data/run"}


def test_mcp_session_binds_user_and_operator_catalogs_independently(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    user_registration = _registration(namespace="user-science", profiles=["user"])
    operator_registration = _registration(
        namespace="operator-science",
        profiles=["operator"],
    )
    ClusterRegistry(
        clusters={
            "alpha": _cluster(
                "alpha",
                {
                    "user-science": user_registration,
                    "operator-science": operator_registration,
                },
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    discovered_at = datetime.now(UTC)
    user_entry = _entry(
        user_registration,
        cluster="alpha",
        server_name="user-science",
    ).model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    operator_entry = _entry(
        operator_registration,
        cluster="alpha",
        server_name="operator-science",
    ).model_copy(
        update={
            "discovered_at": discovered_at,
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    RemoteMcpSchemaCache.update_entry(cache_path, user_entry)
    RemoteMcpSchemaCache.update_entry(cache_path, operator_entry)
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    session = McpSessionState()

    user_list = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
    )
    assert user_list is not None
    user_alias = next(
        tool["name"]
        for tool in user_list["result"]["tools"]
        if tool["name"].startswith("remote_user_science_")
    )

    cross_profile_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": user_alias,
                "arguments": {"cluster": "alpha", "path": "/user"},
            },
        },
        queue=queue,
        profile="operator",
        session=session,
    )
    assert cross_profile_call is not None
    assert (
        "tool is not available in MCP profile 'operator'" in cross_profile_call["error"]["message"]
    )

    cached_operator_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "remote_operator_science_inspect",
                "arguments": {"cluster": "alpha", "path": "/operator"},
            },
        },
        queue=queue,
        profile="operator",
        session=session,
    )
    assert cached_operator_call is not None
    assert "result" in cached_operator_call

    operator_list = handle_request(
        {"jsonrpc": "2.0", "id": 4, "method": "tools/list"},
        queue=queue,
        profile="operator",
        session=session,
    )
    assert operator_list is not None
    operator_alias = next(
        tool["name"]
        for tool in operator_list["result"]["tools"]
        if tool["name"].startswith("remote_operator_science_")
    )

    changed_operator_tool = _tool("inspect", required=["path", "detail"])
    changed_operator_schema = cast(dict[str, object], changed_operator_tool["inputSchema"])
    changed_operator_properties = cast(dict[str, object], changed_operator_schema["properties"])
    changed_operator_properties["detail"] = {"type": "boolean"}
    changed_operator_entry = _entry(
        operator_registration,
        cluster="alpha",
        server_name="operator-science",
        tools=[changed_operator_tool],
    ).model_copy(
        update={
            "discovered_at": discovered_at + timedelta(seconds=1),
            "expires_at": discovered_at + timedelta(hours=1),
        }
    )
    RemoteMcpSchemaCache.update_entry(cache_path, changed_operator_entry)

    user_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": user_alias,
                "arguments": {"cluster": "alpha", "path": "/user"},
            },
        },
        queue=queue,
        profile="user",
        session=session,
    )
    assert user_call is not None
    assert "result" in user_call

    stale_operator_call = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": operator_alias,
                "arguments": {"cluster": "alpha", "path": "/operator"},
            },
        },
        queue=queue,
        profile="operator",
        session=session,
    )
    assert stale_operator_call is not None
    assert "catalog changed after tools/list" in stale_operator_call["error"]["message"]


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
    session = McpSessionState()

    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
        session=session,
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
        session=session,
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


def test_cli_registers_released_scientific_catalog_contract(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The operator CLI accepts the catalog contract and preserves its shell-free argv."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "register",
            "--cluster",
            "alpha",
            "--name",
            "scientific-catalog",
            "--command",
            "clio-kit",
            "--arg",
            "mcp-server",
            "--arg",
            "scientific-catalog",
            "--arg=--",
            "--arg=--catalog-file",
            "--arg",
            "/srv/catalog/scientific-catalog.json",
            "--allow-tool",
            "scientific_dataset_search",
            "--allow-tool",
            "scientific_dataset_describe",
            "--profile",
            "user",
            "--contract",
            "clio-kit-scientific-catalog-user-v1.1",
        ],
    )

    assert result.exit_code == 0, result.output
    registration = (
        ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
        .require("alpha")
        .remote_mcp_servers["scientific-catalog"]
    )
    assert registration.contract == "clio-kit-scientific-catalog-user-v1.1"
    assert registration.args == [
        "mcp-server",
        "scientific-catalog",
        "--",
        "--catalog-file",
        "/srv/catalog/scientific-catalog.json",
    ]


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
    server_artifact = _server_artifact(registration)
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
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
            "expected_server_artifact_digest": server_artifact_digest,
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
            "server_artifact": server_artifact,
            "expected_server_artifact_digest": server_artifact_digest,
            "observed_server_artifact_digest": server_artifact_digest,
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


@pytest.mark.parametrize(
    (
        "runtime_closure_verified",
        "locked_runtime_verified",
        "artifact_digest_bound",
        "expected",
    ),
    [
        (True, True, True, True),
        (False, True, True, False),
        (True, False, True, False),
        (True, True, False, False),
    ],
)
def test_acceptance_report_requires_verified_persistent_uv_tool_runtime(
    runtime_closure_verified: bool,
    locked_runtime_verified: bool,
    artifact_digest_bound: bool,
    expected: bool,
) -> None:
    registration = RemoteMcpServerConfig(
        command="clio-kit",
        args=["mcp-server", "scientific-catalog"],
        allow_tools=["scientific_dataset_search"],
        profiles=["user"],
        namespace="scientific_catalog",
    )
    server_artifact: dict[str, object] = {
        "requested_command": "clio-kit",
        "resolved_executable": "/opt/clio-kit/bin/clio-kit",
        "executable": {
            "path": "/opt/clio-kit/bin/clio-kit",
            "filename": "clio-kit",
            "sha256": "a" * 64,
            "size_bytes": 256,
        },
        "install_spec": "/opt/wheels/clio_kit-2.5.0-py3-none-any.whl",
        "install_source": "uv-tool",
        "install_artifact_sha256": "b" * 64,
        "python_distribution_runtime": {
            "runtime_closure_verified": runtime_closure_verified,
        },
        "nested_launcher": True,
        "nested_runtime": {
            "persistent_tool": True,
            "locked_runtime_verified": locked_runtime_verified,
        },
        "launcher_artifact_verified": True,
        "server_process_artifact_verified": True,
        "identity_error": None,
        "verified": True,
    }
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
    result_server_artifact_digest = server_artifact_digest if artifact_digest_bound else "c" * 64
    discovery_payload = _discovery_artifact(
        registration,
        tools=[_tool("scientific_dataset_search")],
        server_artifact=server_artifact,
    )
    entry = cache_entry_from_discovery_artifact(
        cluster="alpha",
        server_name="scientific-catalog",
        registration=registration,
        discovery_job_id="job_discovery",
        artifact_id="artifact_discovery",
        artifact_sha256=hashlib.sha256(discovery_payload).hexdigest(),
        artifact_payload=discovery_payload,
        discovered_at=NOW,
    )
    job_id = "job_catalog_search"
    job: dict[str, object] = {
        "job_id": job_id,
        "cluster": "alpha",
        "kind": "mcp_call",
        "state": "succeeded",
        "spec": {
            "server": registration.command,
            "server_args": registration.args,
            "operation": "tools/call",
            "tool": "scientific_dataset_search",
            "arguments": {},
            "expected_server_artifact_digest": server_artifact_digest,
        },
    }
    report = build_remote_mcp_acceptance_report(
        registry=ClusterRegistry(
            clusters={
                "alpha": _cluster(
                    "alpha",
                    {"scientific-catalog": registration},
                )
            }
        ),
        cache=RemoteMcpSchemaCache(entries=[entry]),
        cluster="alpha",
        server_name="scientific-catalog",
        remote_tool_name="scientific_dataset_search",
        profile="user",
        call_job_id=job_id,
        call_status={"job": job, "terminal": True},
        artifacts=[
            {
                "artifact_id": f"artifact_{kind}",
                "job_id": job_id,
                "kind": kind,
                "sha256": kind,
            }
            for kind in ("stdout", "stderr", "mcp_result", "provenance")
        ],
        mcp_result={
            "server": registration.command,
            "server_args": registration.args,
            "operation": "tools/call",
            "tool": "scientific_dataset_search",
            "arguments": {},
            "returncode": 0,
            "protocol_result": {"content": [{"type": "text", "text": "ok"}]},
            "server_artifact": server_artifact,
            "expected_server_artifact_digest": result_server_artifact_digest,
            "observed_server_artifact_digest": result_server_artifact_digest,
            "protocol_error": None,
        },
        provenance={"job": job},
        now=NOW,
    )

    server_check = next(
        check for check in report.checks if check.name == "remote-mcp.server-artifact"
    )
    assert server_check.passed is expected
    assert report.passed is expected


@pytest.mark.parametrize(
    "contract_id",
    [
        "clio-kit-spack-user-v2.1",
        "clio-kit-spack-user-v2",
    ],
)
def test_spack_acceptance_enforces_exact_stateless_user_contract(
    monkeypatch: MonkeyPatch,
    contract_id: str,
) -> None:
    expected_names = ["spack_find", "spack_locate", "spack_install"]
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/clio/clio_kit-2.3.0-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "spack",
        ],
        allow_tools=expected_names,
        profiles=["user"],
        contract=cast(Any, contract_id),
    )
    registry = ClusterRegistry(
        clusters={"alpha": _cluster("alpha", {"site-software": registration})}
    )
    server_artifact = _server_artifact(registration)
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
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
            "expected_server_artifact_digest": server_artifact_digest,
        },
    }
    artifacts = [
        {"artifact_id": f"artifact_{kind}", "job_id": job_id, "kind": kind, "sha256": kind}
        for kind in ("stdout", "stderr", "mcp_result", "provenance")
    ]

    def build(
        tools: list[dict[str, object]],
        *,
        result_expectation: RemoteMcpStructuredResultExpectation | None = None,
        protocol_result: dict[str, object] | None = None,
    ) -> RemoteMcpAcceptanceReport:
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
                "protocol_result": protocol_result or {"content": [{"type": "text", "text": "ok"}]},
                "server_artifact": server_artifact,
                "expected_server_artifact_digest": server_artifact_digest,
                "observed_server_artifact_digest": server_artifact_digest,
                "protocol_error": None,
            },
            provenance={"job": job},
            result_expectation=result_expectation,
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
        remote_mcp,
        "CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID",
        {
            **remote_mcp.CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID,
            contract_id: expected_entry.schema_digest,
        },
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
    assert server_resource.metadata["contract_id"] == contract_id
    assert server_resource.metadata["contract_sha256"] == expected_entry.schema_digest

    expectation = RemoteMcpStructuredResultExpectation(
        contract=cast(Any, contract_id),
        tool="spack_find",
        package_name="lammps",
        dag_hash="a" * 32,
    )
    semantic_report = build(
        exact_tools,
        result_expectation=expectation,
        protocol_result={
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": None,
                "count": 1,
                "packages": [{"name": "lammps", "dag_hash": "a" * 32}],
            }
        },
    )
    semantic_check = next(
        check for check in semantic_report.checks if check.name == "remote-mcp.structured-result"
    )
    assert semantic_report.passed is True
    assert semantic_check.passed is True

    drifted_report = build(
        [*[_spack_tool(name) for name in expected_names], _spack_tool("spack_load")]
    )
    drifted_contract = next(
        check for check in drifted_report.checks if check.name == "remote-mcp.spack-user-contract"
    )
    assert drifted_report.passed is False
    assert drifted_contract.passed is False
    assert drifted_contract.evidence["stateful_load_exposed"] is True


@pytest.mark.parametrize(
    ("tool", "arguments", "expectation_fields", "structured", "observed_fields"),
    [
        (
            "spack_find",
            {"query": "solver"},
            {},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "solver",
                "count": 1,
                "packages": [{"name": "solver", "dag_hash": "a" * 32}],
            },
            {"expected_package_match_count": 1, "count": 1},
        ),
        (
            "spack_locate",
            {"spec": "solver"},
            {"requested_spec": "solver", "prefix": "/opt/spack/solver-a"},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "locate",
                "requested_spec": "solver",
                "load_spec": f"/{'a' * 32}",
                "package": {"name": "solver", "dag_hash": "a" * 32},
                "prefix": "/opt/spack/solver-a",
            },
            {
                "expected_package_match_count": 1,
                "prefix_is_canonical_absolute": True,
                "prefix_matches_expected": True,
                "load_spec": f"/{'a' * 32}",
            },
        ),
        (
            "spack_install",
            {"spec": "solver", "reuse": True},
            {"requested_spec": "solver", "reuse": True},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "install",
                "requested_spec": "solver",
                "reuse": True,
                "status": "installed",
                "duration_seconds": 0.25,
                "packages": [{"name": "solver", "dag_hash": "a" * 32}],
            },
            {
                "expected_package_match_count": 1,
                "reuse": True,
                "status": "installed",
            },
        ),
    ],
)
def test_spack_structured_result_expectations_prove_operation_semantics(
    tool: str,
    arguments: dict[str, object],
    expectation_fields: dict[str, object],
    structured: dict[str, object],
    observed_fields: dict[str, object],
) -> None:
    expectation = RemoteMcpStructuredResultExpectation.model_validate(
        {
            "contract": "clio-kit-spack-user-v2",
            "tool": tool,
            "package_name": "solver",
            "dag_hash": "a" * 32,
            **expectation_fields,
        }
    )

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name=tool,
        arguments=arguments,
        protocol_result={"structuredContent": structured},
        output_schema=_spack_result_schema(tool),
    )

    assert check.passed is True
    assert check.evidence["failures"] == []
    assert check.evidence["expected"] == expectation.model_dump(mode="json")
    observed = cast(dict[str, object], check.evidence["observed"])
    assert observed.items() >= observed_fields.items()

    report = RemoteMcpAcceptanceReport(
        cluster="alpha",
        server_name="spack",
        remote_tool_name=tool,
        virtual_alias=f"remote_spack_{tool}",
        profile="user",
        passed=True,
        checks=[check],
        call_job={"job_id": f"job_{tool}", "state": "succeeded"},
    ).to_live_validation_report()
    call = next(
        resource for resource in report.resources if resource.role == "virtual_remote_mcp_call"
    )
    assert call.metadata["structured_result_assertion"] == check.evidence


@pytest.mark.parametrize(
    ("tool", "expectation_fields", "wrong_structured"),
    [
        (
            "spack_find",
            {},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "solver",
                "count": 1,
                "packages": [{"name": "solver", "dag_hash": "b" * 32}],
            },
        ),
        (
            "spack_locate",
            {"requested_spec": "solver", "prefix": "/opt/spack/solver-a"},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "locate",
                "requested_spec": "solver",
                "load_spec": f"/{'b' * 32}",
                "package": {"name": "solver", "dag_hash": "b" * 32},
                "prefix": "/opt/spack/solver-b",
            },
        ),
        (
            "spack_install",
            {"requested_spec": "solver", "reuse": True},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "install",
                "requested_spec": "solver",
                "reuse": False,
                "status": "installed",
                "duration_seconds": 0.25,
                "packages": [{"name": "solver", "dag_hash": "b" * 32}],
            },
        ),
    ],
)
def test_spack_result_rejects_success_text_when_structured_content_is_wrong(
    tool: str,
    expectation_fields: dict[str, object],
    wrong_structured: dict[str, object],
) -> None:
    expectation = RemoteMcpStructuredResultExpectation.model_validate(
        {
            "contract": "clio-kit-spack-user-v2",
            "tool": tool,
            "package_name": "solver",
            "dag_hash": "a" * 32,
            **expectation_fields,
        }
    )
    arguments: dict[str, object] = (
        {"query": "solver"}
        if tool == "spack_find"
        else {
            "spec": "solver",
            **({"reuse": True} if tool == "spack_install" else {}),
        }
    )

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name=tool,
        arguments=arguments,
        protocol_result={
            "structuredContent": wrong_structured,
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "success": True,
                            "package_name": "solver",
                            "dag_hash": "a" * 32,
                        }
                    ),
                }
            ],
        },
        output_schema=_spack_result_schema(tool),
    )

    assert check.passed is False
    assert check.evidence["failures"]


def test_spack_find_rejects_an_additional_hash_for_the_same_package_name() -> None:
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_find",
        package_name="solver",
        dag_hash="a" * 32,
    )

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name="spack_find",
        arguments={"query": "solver"},
        protocol_result={
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "solver",
                "count": 2,
                "packages": [
                    {"name": "solver", "dag_hash": "a" * 32},
                    {"name": "solver", "dag_hash": "b" * 32},
                ],
            }
        },
        output_schema=_spack_result_schema("spack_find"),
    )

    assert check.passed is False
    assert check.evidence["observed"]["package_hashes_for_expected_name"] == [
        "a" * 32,
        "b" * 32,
    ]
    assert "ambiguous hash" in " ".join(cast(list[str], check.evidence["failures"]))


def test_spack_find_rejects_an_additional_other_package_record() -> None:
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_find",
        package_name="solver",
        dag_hash="a" * 32,
    )

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name="spack_find",
        arguments={"query": "solver"},
        protocol_result={
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "solver",
                "count": 2,
                "packages": [
                    {"name": "solver", "dag_hash": "a" * 32},
                    {"name": "dependency", "dag_hash": "c" * 32},
                ],
            }
        },
        output_schema=_spack_result_schema("spack_find"),
    )

    assert check.passed is False
    assert check.evidence["observed"]["package_count"] == 2
    assert "matching root package" in " ".join(cast(list[str], check.evidence["failures"]))


@pytest.mark.parametrize(
    "prefix",
    [
        "../mutable-prefix",
        "/opt/spack/with\x00nul",
        "/opt/spack/with\nnewline",
        "/opt/spack/with\rcarriage-return",
        "/opt/spack/with\ttab",
        "/opt/spack/with\x7fdelete",
    ],
)
def test_spack_result_expectation_requires_exact_locate_prefix(prefix: str) -> None:
    with pytest.raises(ValidationError, match="canonical absolute POSIX prefix"):
        RemoteMcpStructuredResultExpectation.model_validate(
            {
                "contract": "clio-kit-spack-user-v2",
                "tool": "spack_locate",
                "package_name": "solver",
                "dag_hash": "a" * 32,
                "requested_spec": "solver",
                "prefix": prefix,
            }
        )


@pytest.mark.parametrize(
    "output_schema",
    [
        None,
        {"type": "not-a-json-schema-type"},
        {
            "type": "object",
            "properties": {"required_marker": {"const": "present"}},
            "required": ["required_marker"],
            "additionalProperties": True,
        },
    ],
)
def test_spack_structured_result_fails_closed_without_a_matching_output_schema(
    output_schema: dict[str, object] | None,
) -> None:
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_find",
        package_name="solver",
        dag_hash="a" * 32,
    )

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name="spack_find",
        arguments={"query": "solver"},
        protocol_result={
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "solver",
                "count": 1,
                "packages": [{"name": "solver", "dag_hash": "a" * 32}],
            }
        },
        output_schema=output_schema,
    )

    assert check.passed is False
    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    assert schema_evidence["structured_content_valid"] is False
    assert check.evidence["failures"]


@pytest.mark.parametrize(
    "mutation",
    cast(
        list[dict[str, object]],
        [
            {"unexpected": "must be rejected"},
            {"packages": [{"name": "solver", "dag_hash": "a" * 32, "version": {}}]},
        ],
    ),
)
def test_spack_structured_result_rejects_output_schema_violations(
    mutation: dict[str, object],
) -> None:
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_find",
        package_name="solver",
        dag_hash="a" * 32,
    )
    structured: dict[str, object] = {
        "schema_version": "spack.mcp.result.v1",
        "operation": "find",
        "query": "solver",
        "count": 1,
        "packages": [{"name": "solver", "dag_hash": "a" * 32}],
        **mutation,
    }

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name="spack_find",
        arguments={"query": "solver"},
        protocol_result={"structuredContent": structured},
        output_schema=_spack_result_schema("spack_find"),
    )

    assert check.passed is False
    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    assert schema_evidence["schema_present"] is True
    assert schema_evidence["schema_valid"] is True
    schema_sha256 = schema_evidence["schema_sha256"]
    assert isinstance(schema_sha256, str)
    assert len(schema_sha256) == 64
    assert schema_evidence["structured_content_valid"] is False
    assert cast(list[str], schema_evidence["validation_errors"])


def test_spack_structured_result_bounds_output_schema_error_evidence() -> None:
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_find",
        package_name="solver",
        dag_hash="a" * 32,
    )
    output_schema = _spack_result_schema("spack_find")
    properties = cast(dict[str, object], output_schema["properties"])
    structured: dict[str, object] = {
        "schema_version": "spack.mcp.result.v1",
        "operation": "find",
        "query": "solver",
        "count": 1,
        "packages": [{"name": "solver", "dag_hash": "a" * 32}],
    }
    for index in range(remote_mcp.MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS + 3):
        field = f"invalid_{index}"
        properties[field] = {"type": "integer"}
        structured[field] = "not-an-integer"

    check = build_remote_mcp_structured_result_check(
        expectation=expectation,
        remote_tool_name="spack_find",
        arguments={"query": "solver"},
        protocol_result={"structuredContent": structured},
        output_schema=output_schema,
    )

    assert check.passed is False
    schema_evidence = cast(dict[str, object], check.evidence["output_schema"])
    errors = cast(list[str], schema_evidence["validation_errors"])
    assert len(errors) == remote_mcp.MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS
    assert all(len(error) <= remote_mcp.MAX_REMOTE_MCP_DIAGNOSTIC_CHARS for error in errors)
    assert schema_evidence["validation_errors_truncated"] is True


def test_spack_fresh_install_transition_proves_ordered_disposable_install() -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/run-1/spack-store"
    configuration_sha256 = "6" * 64
    configuration_manifest_path = "/scratch/relay-acceptance/run-1/configuration.json"
    spack_command = _spack_command_for_manifest(configuration_manifest_path)
    preinstall = _ordinary_spack_transition_report(
        "preinstall", spec=spec, spack_command=spack_command
    )
    install = _ordinary_spack_transition_report("install", spec=spec, spack_command=spack_command)
    postinstall = _ordinary_spack_transition_report(
        "postinstall", spec=spec, spack_command=spack_command
    )
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/linux-x86_64/gcc-12.3.0/zlib-1.3.1-a",
    )
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_install",
        package_name="zlib",
        dag_hash=dag_hash,
        requested_spec=spec,
        reuse=False,
        fresh_install_store_root=store_root,
        fresh_install_configuration_sha256=configuration_sha256,
        fresh_install_configuration_manifest_path=configuration_manifest_path,
    )
    report = build_remote_mcp_spack_fresh_install_transition_report(
        preinstall_report=preinstall,
        install_report=install,
        postinstall_report=postinstall,
        preinstall_protocol_result=protocols[0],
        install_protocol_result=protocols[1],
        postinstall_protocol_result=protocols[2],
        install_expectation=expectation,
        preinstall_configuration=_spack_configuration_observation(
            "preinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
        postinstall_configuration=_spack_configuration_observation(
            "postinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
    )

    assert report.passed is True
    assert report.spack_install_transition is not None
    assert report.spack_install_transition.fresh_install_store_root == store_root
    assert (
        report.spack_install_transition.fresh_install_configuration_sha256 == configuration_sha256
    )
    assert (
        report.spack_install_transition.fresh_install_configuration_manifest_path
        == configuration_manifest_path
    )
    assert report.spack_install_transition.executed_spack_command_path == spack_command
    assert report.spack_install_transition.executed_spack_command_relative_path == "bin/spack"
    assert report.spack_install_transition.executed_spack_command_sha256 == "8" * 64
    assert report.spack_install_transition.executed_spack_command_size_bytes == 256
    assert report.spack_install_transition.preinstall_configuration.phase == "preinstall"
    assert report.spack_install_transition.postinstall_configuration.phase == "postinstall"
    names = [check.name for check in report.checks]
    assert len(names) == len(set(names))
    assert "remote-mcp.preinstall.register" in names
    assert "remote-mcp.register" in names
    assert "remote-mcp.postinstall.register" in names
    for name in (
        "remote-mcp.spack-preinstall-absent",
        "remote-mcp.spack-fresh-install",
        "remote-mcp.spack-postinstall-locate",
        "remote-mcp.spack-disposable-store",
        "remote-mcp.spack-transition-identity",
        "remote-mcp.spack-transition-durable-evidence",
        "remote-mcp.spack-fresh-configuration",
    ):
        assert next(check for check in report.checks if check.name == name).passed is True

    live = report.to_live_validation_report()
    transition_jobs = {
        resource.role: resource
        for resource in live.resources
        if resource.kind == "relay_job" and resource.role is not None
    }
    assert set(transition_jobs) >= {
        "spack_preinstall_find",
        "spack_fresh_install",
        "spack_postinstall_locate",
    }
    assert transition_jobs["spack_preinstall_find"].resource_id == "job-spack-preinstall"
    assert transition_jobs["spack_fresh_install"].resource_id == "job-spack-install"
    assert transition_jobs["spack_postinstall_locate"].resource_id == "job-spack-postinstall"
    transition_artifacts = [
        resource
        for resource in live.resources
        if resource.kind == "artifact" and resource.role and resource.role.startswith("spack_")
    ]
    assert len(transition_artifacts) == 12
    assert len({resource.resource_id for resource in transition_artifacts}) == 12
    assert all(resource.role is not None for resource in transition_artifacts)
    configuration_resource = next(
        resource for resource in live.resources if resource.kind == "configuration_manifest"
    )
    assert configuration_resource.role == "spack_fresh_install_configuration"
    assert configuration_resource.resource_id == configuration_sha256
    assert configuration_resource.references == [configuration_manifest_path]
    assert [
        component["relative_path"]
        for component in configuration_resource.metadata["preinstall"]["components"]
    ] == ["bin/spack", "config/config.yaml"]
    live.model_validate(live.model_dump(mode="python"))


@pytest.mark.parametrize("failure", ["digest", "path", "wrapper"])
def test_spack_fresh_install_transition_rejects_configuration_identity_drift(
    failure: str,
) -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/config-drift/spack-store"
    configuration_sha256 = "6" * 64
    configuration_manifest_path = "/scratch/relay-acceptance/config-drift/configuration.json"
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/zlib-1.3.1-a",
    )
    preinstall_configuration = _spack_configuration_observation(
        "preinstall",
        manifest_path=configuration_manifest_path,
        manifest_sha256=configuration_sha256,
    )
    postinstall_payload = _spack_configuration_observation(
        "postinstall",
        manifest_path=configuration_manifest_path,
        manifest_sha256=configuration_sha256,
    ).model_dump(mode="python")
    if failure == "digest":
        postinstall_payload["manifest_sha256"] = "9" * 64
    elif failure == "path":
        postinstall_payload["manifest_path"] = (
            "/scratch/relay-acceptance/config-drift/replaced.json"
        )
    else:
        components = cast(list[dict[str, object]], postinstall_payload["components"])
        components[0]["sha256"] = "9" * 64
    postinstall_configuration = RemoteMcpSpackConfigurationObservation.model_validate(
        postinstall_payload
    )
    spack_command = _spack_command_for_manifest(configuration_manifest_path)

    report = build_remote_mcp_spack_fresh_install_transition_report(
        preinstall_report=_ordinary_spack_transition_report(
            "preinstall", spec=spec, spack_command=spack_command
        ),
        install_report=_ordinary_spack_transition_report(
            "install", spec=spec, spack_command=spack_command
        ),
        postinstall_report=_ordinary_spack_transition_report(
            "postinstall", spec=spec, spack_command=spack_command
        ),
        preinstall_protocol_result=protocols[0],
        install_protocol_result=protocols[1],
        postinstall_protocol_result=protocols[2],
        install_expectation=RemoteMcpStructuredResultExpectation(
            contract="clio-kit-spack-user-v2",
            tool="spack_install",
            package_name="zlib",
            dag_hash=dag_hash,
            requested_spec=spec,
            reuse=False,
            fresh_install_store_root=store_root,
            fresh_install_configuration_sha256=configuration_sha256,
            fresh_install_configuration_manifest_path=configuration_manifest_path,
        ),
        preinstall_configuration=preinstall_configuration,
        postinstall_configuration=postinstall_configuration,
    )

    assert report.passed is False
    check = _transition_check(report, "remote-mcp.spack-fresh-configuration")
    assert check.passed is False
    expected_failed_flag = {
        "digest": "digest_matches",
        "path": "path_matches",
        "wrapper": "wrapper_matches",
    }[failure]
    assert check.evidence[expected_failed_flag] is False
    if failure == "wrapper":
        wrapper = cast(dict[str, object], check.evidence["executed_spack_command"])
        assert wrapper["matches"] is False
        assert wrapper["failures"] == [
            "executed Spack wrapper bytes or regular-file identity changed"
        ]
    assert report.spack_install_transition is not None
    assert report.spack_install_transition.postinstall_configuration == (postinstall_configuration)


@pytest.mark.parametrize("binding", ["outside-manifest-root", "unmanifested-component"])
def test_spack_fresh_install_transition_rejects_unbound_executed_wrapper(
    binding: str,
) -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/wrapper-binding/spack-store"
    manifest_path = "/scratch/relay-acceptance/wrapper-binding/configuration.json"
    configuration_sha256 = "6" * 64
    valid_spack_command = _spack_command_for_manifest(manifest_path)
    executed_spack_command = (
        "/opt/site-spack/bin/spack"
        if binding == "outside-manifest-root"
        else "/scratch/relay-acceptance/wrapper-binding/bin/unlisted-spack"
    )
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/zlib-1.3.1-a",
    )

    report = build_remote_mcp_spack_fresh_install_transition_report(
        preinstall_report=_ordinary_spack_transition_report(
            "preinstall", spec=spec, spack_command=valid_spack_command
        ),
        install_report=_ordinary_spack_transition_report(
            "install", spec=spec, spack_command=executed_spack_command
        ),
        postinstall_report=_ordinary_spack_transition_report(
            "postinstall", spec=spec, spack_command=valid_spack_command
        ),
        preinstall_protocol_result=protocols[0],
        install_protocol_result=protocols[1],
        postinstall_protocol_result=protocols[2],
        install_expectation=RemoteMcpStructuredResultExpectation(
            contract="clio-kit-spack-user-v2",
            tool="spack_install",
            package_name="zlib",
            dag_hash=dag_hash,
            requested_spec=spec,
            reuse=False,
            fresh_install_store_root=store_root,
            fresh_install_configuration_sha256=configuration_sha256,
            fresh_install_configuration_manifest_path=manifest_path,
        ),
        preinstall_configuration=_spack_configuration_observation(
            "preinstall",
            manifest_path=manifest_path,
            manifest_sha256=configuration_sha256,
        ),
        postinstall_configuration=_spack_configuration_observation(
            "postinstall",
            manifest_path=manifest_path,
            manifest_sha256=configuration_sha256,
        ),
    )

    assert report.passed is False
    check = _transition_check(report, "remote-mcp.spack-fresh-configuration")
    assert check.passed is False
    assert check.evidence["wrapper_matches"] is False
    wrapper = cast(dict[str, object], check.evidence["executed_spack_command"])
    expected_failure = (
        "executed Spack wrapper is outside the configuration manifest root"
        if binding == "outside-manifest-root"
        else "executed Spack wrapper is not one unique manifest component"
    )
    assert wrapper["failures"] == [expected_failure]


def test_spack_fresh_install_transition_fails_closed_across_each_boundary() -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/run-2/spack-store"
    configuration_sha256 = "6" * 64
    configuration_manifest_path = "/scratch/relay-acceptance/run-2/configuration.json"
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_install",
        package_name="zlib",
        dag_hash=dag_hash,
        requested_spec=spec,
        reuse=False,
        fresh_install_store_root=store_root,
        fresh_install_configuration_sha256=configuration_sha256,
        fresh_install_configuration_manifest_path=configuration_manifest_path,
    )
    spack_command = _spack_command_for_manifest(configuration_manifest_path)

    def build(
        *,
        preinstall: RemoteMcpAcceptanceReport | None = None,
        install: RemoteMcpAcceptanceReport | None = None,
        postinstall: RemoteMcpAcceptanceReport | None = None,
        protocols: tuple[dict[str, object], dict[str, object], dict[str, object]] | None = None,
    ) -> RemoteMcpAcceptanceReport:
        selected_protocols = protocols or _spack_transition_protocol_results(
            spec=spec,
            dag_hash=dag_hash,
            prefix=f"{store_root}/zlib-1.3.1-a",
        )
        return build_remote_mcp_spack_fresh_install_transition_report(
            preinstall_report=preinstall
            or _ordinary_spack_transition_report(
                "preinstall", spec=spec, spack_command=spack_command
            ),
            install_report=install
            or _ordinary_spack_transition_report("install", spec=spec, spack_command=spack_command),
            postinstall_report=postinstall
            or _ordinary_spack_transition_report(
                "postinstall", spec=spec, spack_command=spack_command
            ),
            preinstall_protocol_result=selected_protocols[0],
            install_protocol_result=selected_protocols[1],
            postinstall_protocol_result=selected_protocols[2],
            install_expectation=expectation,
            preinstall_configuration=_spack_configuration_observation(
                "preinstall",
                manifest_path=configuration_manifest_path,
                manifest_sha256=configuration_sha256,
            ),
            postinstall_configuration=_spack_configuration_observation(
                "postinstall",
                manifest_path=configuration_manifest_path,
                manifest_sha256=configuration_sha256,
            ),
        )

    bad_pre_protocols = list(
        _spack_transition_protocol_results(
            spec=spec,
            dag_hash=dag_hash,
            prefix=f"{store_root}/zlib-1.3.1-a",
        )
    )
    bad_pre_protocols[0] = {
        "structuredContent": {
            "schema_version": "spack.mcp.result.v1",
            "operation": "find",
            "query": spec,
            "count": 1,
            "packages": [{"name": "zlib", "dag_hash": dag_hash}],
        }
    }
    bad_pre = build(protocols=(bad_pre_protocols[0], bad_pre_protocols[1], bad_pre_protocols[2]))
    assert bad_pre.passed is False
    assert _transition_check(bad_pre, "remote-mcp.spack-preinstall-absent").passed is False

    bad_install_protocols = list(
        _spack_transition_protocol_results(
            spec=spec,
            dag_hash=dag_hash,
            prefix=f"{store_root}/zlib-1.3.1-a",
        )
    )
    install_content = cast(dict[str, object], bad_install_protocols[1]["structuredContent"])
    install_content["reuse"] = True
    install_content["status"] = "reused"
    bad_install = build(
        protocols=(
            bad_install_protocols[0],
            bad_install_protocols[1],
            bad_install_protocols[2],
        )
    )
    assert bad_install.passed is False
    assert _transition_check(bad_install, "remote-mcp.spack-fresh-install").passed is False

    outside_protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix="/opt/global-spack/zlib-1.3.1-a",
    )
    outside = build(protocols=outside_protocols)
    assert outside.passed is False
    assert _transition_check(outside, "remote-mcp.spack-postinstall-locate").passed is True
    assert _transition_check(outside, "remote-mcp.spack-disposable-store").passed is False

    mismatched_route = _ordinary_spack_transition_report(
        "postinstall", spec=spec, spack_command=spack_command
    )
    tools_check = next(
        check for check in mismatched_route.checks if check.name == "remote-mcp.tools-list"
    )
    tools_check.evidence["catalog_revision"] = "b" * 64
    identity_failure = build(postinstall=mismatched_route)
    assert identity_failure.passed is False
    assert (
        _transition_check(identity_failure, "remote-mcp.spack-transition-identity").passed is False
    )

    duplicate_job = _ordinary_spack_transition_report(
        "postinstall", spec=spec, spack_command=spack_command
    )
    duplicate_job.call_job["job_id"] = "job-spack-install"
    for artifact in duplicate_job.artifacts:
        artifact["job_id"] = "job-spack-install"
    duplicate_job.mcp_stdio = _transition_stdio(
        alias=cast(str, duplicate_job.virtual_alias),
        job_id="job-spack-install",
    )
    durable_failure = build(postinstall=duplicate_job)
    assert durable_failure.passed is False
    assert (
        _transition_check(
            durable_failure,
            "remote-mcp.spack-transition-durable-evidence",
        ).passed
        is False
    )


def test_spack_fresh_install_transition_requires_artifacts_stdio_and_passing_reports() -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/run-3/spack-store"
    configuration_sha256 = "6" * 64
    configuration_manifest_path = "/scratch/relay-acceptance/run-3/configuration.json"
    spack_command = _spack_command_for_manifest(configuration_manifest_path)
    expectation = RemoteMcpStructuredResultExpectation(
        contract="clio-kit-spack-user-v2",
        tool="spack_install",
        package_name="zlib",
        dag_hash=dag_hash,
        requested_spec=spec,
        reuse=False,
        fresh_install_store_root=store_root,
        fresh_install_configuration_sha256=configuration_sha256,
        fresh_install_configuration_manifest_path=configuration_manifest_path,
    )
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/zlib-1.3.1-a",
    )

    missing_artifact = _ordinary_spack_transition_report(
        "preinstall", spec=spec, spack_command=spack_command
    )
    missing_artifact.artifacts = [
        artifact for artifact in missing_artifact.artifacts if artifact["kind"] != "provenance"
    ]
    missing_stdio = _ordinary_spack_transition_report(
        "install", spec=spec, spack_command=spack_command
    )
    missing_stdio.mcp_stdio = {}
    failed_postinstall = _ordinary_spack_transition_report(
        "postinstall", spec=spec, spack_command=spack_command
    )
    failed_postinstall.passed = False

    report = build_remote_mcp_spack_fresh_install_transition_report(
        preinstall_report=missing_artifact,
        install_report=missing_stdio,
        postinstall_report=failed_postinstall,
        preinstall_protocol_result=protocols[0],
        install_protocol_result=protocols[1],
        postinstall_protocol_result=protocols[2],
        install_expectation=expectation,
        preinstall_configuration=_spack_configuration_observation(
            "preinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
        postinstall_configuration=_spack_configuration_observation(
            "postinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
    )

    assert report.passed is False
    assert _transition_check(report, "remote-mcp.spack-transition-identity").passed is False
    durable = _transition_check(report, "remote-mcp.spack-transition-durable-evidence")
    assert durable.passed is False
    assert durable.evidence["phases"]["preinstall"]["artifacts_valid"] is True
    assert durable.evidence["phases"]["install"]["stdio_valid"] is False


@pytest.mark.parametrize(
    ("tool", "reuse", "root", "message"),
    [
        ("spack_find", None, "/scratch/store", "must not declare"),
        ("spack_locate", None, "/scratch/store", "must not declare"),
        ("spack_install", True, "/scratch/store", "requires spack_install reuse=false"),
        ("spack_install", False, "/scratch/store/../shared", "canonical absolute POSIX"),
        ("spack_install", False, "relative/store", "canonical absolute POSIX"),
    ],
)
def test_spack_fresh_install_store_root_is_strictly_operation_scoped(
    tool: str,
    reuse: bool | None,
    root: str,
    message: str,
) -> None:
    payload: dict[str, object] = {
        "contract": "clio-kit-spack-user-v2",
        "tool": tool,
        "package_name": "zlib",
        "dag_hash": "a" * 32,
        "fresh_install_store_root": root,
    }
    if tool != "spack_find":
        payload["requested_spec"] = "zlib@1.3.1"
    if tool == "spack_locate":
        payload["prefix"] = "/opt/spack/zlib"
    if reuse is not None:
        payload["reuse"] = reuse
    if tool == "spack_install":
        payload["fresh_install_configuration_sha256"] = "6" * 64
        payload["fresh_install_configuration_manifest_path"] = "/scratch/configuration.json"

    with pytest.raises(ValidationError, match=message):
        RemoteMcpStructuredResultExpectation.model_validate(payload)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"fresh_install_configuration_sha256": None}, "requires store root"),
        ({"fresh_install_configuration_manifest_path": None}, "requires store root"),
        ({"fresh_install_store_root": None}, "requires store root"),
        ({"fresh_install_configuration_sha256": "A" * 64}, "string_pattern_mismatch"),
        (
            {"fresh_install_configuration_manifest_path": "relative/configuration.json"},
            "canonical absolute POSIX",
        ),
    ],
)
def test_spack_fresh_install_requires_complete_configuration_identity(
    mutation: dict[str, object],
    message: str,
) -> None:
    payload: dict[str, object] = {
        "contract": "clio-kit-spack-user-v2",
        "tool": "spack_install",
        "package_name": "zlib",
        "dag_hash": "a" * 32,
        "requested_spec": "zlib@1.3.1",
        "reuse": False,
        "fresh_install_store_root": "/scratch/acceptance/spack-store",
        "fresh_install_configuration_sha256": "6" * 64,
        "fresh_install_configuration_manifest_path": "/scratch/acceptance/configuration.json",
        **mutation,
    }

    with pytest.raises(ValidationError, match=message):
        RemoteMcpStructuredResultExpectation.model_validate(payload)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"manifest_path": "relative/manifest.json"}, "canonical and absolute"),
        ({"manifest_path": "/scratch/config/../manifest.json"}, "canonical and absolute"),
        ({"manifest_sha256": "A" * 64}, "string_pattern_mismatch"),
        ({"manifest_size_bytes": 0}, "greater_than_equal"),
        (
            {
                "components": [
                    {
                        "relative_path": "/absolute/wrapper",
                        "sha256": "7" * 64,
                        "size_bytes": 1,
                    }
                ]
            },
            "canonical and relative",
        ),
        (
            {
                "components": [
                    {
                        "relative_path": "../outside",
                        "sha256": "7" * 64,
                        "size_bytes": 1,
                    }
                ]
            },
            "canonical and relative",
        ),
        (
            {
                "components": [
                    {
                        "relative_path": "wrapper/z",
                        "sha256": "7" * 64,
                        "size_bytes": 1,
                    },
                    {
                        "relative_path": "config/a",
                        "sha256": "8" * 64,
                        "size_bytes": 1,
                    },
                ]
            },
            "unique and sorted",
        ),
        (
            {
                "components": [
                    {
                        "relative_path": f"config/file-{index:02d}",
                        "sha256": "7" * 64,
                        "size_bytes": 1,
                    }
                    for index in range(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS + 1)
                ]
            },
            "too_long",
        ),
        (
            {"manifest_size_bytes": MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES + 1},
            "less_than_equal",
        ),
        (
            {
                "components": [
                    {
                        "relative_path": "config/config.yaml",
                        "sha256": "7" * 64,
                        "size_bytes": MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES + 1,
                    }
                ]
            },
            "less_than_equal",
        ),
    ],
)
def test_spack_configuration_observation_rejects_unsafe_or_unbounded_manifest(
    mutation: dict[str, object],
    message: str,
) -> None:
    payload: dict[str, object] = {
        "phase": "preinstall",
        "manifest_path": "/scratch/acceptance/configuration.json",
        "manifest_sha256": "6" * 64,
        "manifest_size_bytes": 128,
        "components": [
            {
                "relative_path": "config/config.yaml",
                "sha256": "7" * 64,
                "size_bytes": 1,
            }
        ],
        **mutation,
    }

    with pytest.raises(ValidationError, match=message):
        RemoteMcpSpackConfigurationObservation.model_validate(payload)


def test_remote_mcp_acceptance_without_transition_remains_backward_compatible() -> None:
    ordinary = _ordinary_spack_transition_report("install", spec="zlib@1.3.1")

    assert ordinary.spack_install_transition is None
    live = ordinary.to_live_validation_report()
    call = next(resource for resource in live.resources if resource.kind == "relay_job")
    assert call.role == "virtual_remote_mcp_call"
    dumped = ordinary.model_dump(mode="json", exclude_none=True)
    assert "spack_install_transition" not in dumped


def test_spack_transition_evidence_rejects_forged_phase_or_store_root() -> None:
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/relay-acceptance/run-4/spack-store"
    configuration_sha256 = "6" * 64
    configuration_manifest_path = "/scratch/relay-acceptance/run-4/configuration.json"
    spack_command = _spack_command_for_manifest(configuration_manifest_path)
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/zlib-1.3.1-a",
    )
    report = build_remote_mcp_spack_fresh_install_transition_report(
        preinstall_report=_ordinary_spack_transition_report(
            "preinstall", spec=spec, spack_command=spack_command
        ),
        install_report=_ordinary_spack_transition_report(
            "install", spec=spec, spack_command=spack_command
        ),
        postinstall_report=_ordinary_spack_transition_report(
            "postinstall", spec=spec, spack_command=spack_command
        ),
        preinstall_protocol_result=protocols[0],
        install_protocol_result=protocols[1],
        postinstall_protocol_result=protocols[2],
        install_expectation=RemoteMcpStructuredResultExpectation(
            contract="clio-kit-spack-user-v2",
            tool="spack_install",
            package_name="zlib",
            dag_hash=dag_hash,
            requested_spec=spec,
            reuse=False,
            fresh_install_store_root=store_root,
            fresh_install_configuration_sha256=configuration_sha256,
            fresh_install_configuration_manifest_path=configuration_manifest_path,
        ),
        preinstall_configuration=_spack_configuration_observation(
            "preinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
        postinstall_configuration=_spack_configuration_observation(
            "postinstall",
            manifest_path=configuration_manifest_path,
            manifest_sha256=configuration_sha256,
        ),
    )
    transition = cast(
        RemoteMcpSpackInstallTransitionEvidence,
        report.spack_install_transition,
    ).model_dump(mode="python")

    invalid_wrapper_identities = [
        {"executed_spack_command_sha256": None},
        {"executed_spack_command_path": "/scratch/wrapper/../bin/spack"},
        {"executed_spack_command_relative_path": "bin/../spack"},
        {"executed_spack_command_sha256": "A" * 64},
        {"executed_spack_command_sha256": "9" * 64},
        {"executed_spack_command_size_bytes": 0},
        {"executed_spack_command_size_bytes": 257},
    ]
    for mutation in invalid_wrapper_identities:
        candidate = {**deepcopy(transition), **mutation}
        with pytest.raises(ValidationError, match="executed Spack command"):
            RemoteMcpSpackInstallTransitionEvidence.model_validate(candidate)

    transition["fresh_install_store_root"] = "/scratch/store/../shared"
    with pytest.raises(ValidationError, match="canonical absolute POSIX"):
        RemoteMcpSpackInstallTransitionEvidence.model_validate(transition)

    transition["fresh_install_store_root"] = store_root
    cast(dict[str, object], transition["preinstall"])["remote_tool_name"] = "spack_install"
    with pytest.raises(ValidationError, match="preinstall evidence must represent spack_find"):
        RemoteMcpSpackInstallTransitionEvidence.model_validate(transition)


def test_cli_rejects_mismatched_structured_result_expectation_before_dispatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = RemoteMcpServerConfig(
        command="clio-kit",
        args=["mcp-server", "spack"],
        allow_tools=["spack_find", "spack_locate", "spack_install"],
        profiles=["user"],
        contract="clio-kit-spack-user-v2",
    )
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"spack": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    report_path = tmp_path / "validation" / "preflight.json"

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "validate",
            "--cluster",
            "alpha",
            "--name",
            "spack",
            "--tool",
            "spack_find",
            "--result-expectation-json",
            json.dumps(
                {
                    "contract": "clio-kit-spack-user-v2",
                    "tool": "spack_install",
                    "package_name": "solver",
                    "dag_hash": "a" * 32,
                    "requested_spec": "solver",
                    "reuse": True,
                }
            ),
            "--validation-report",
            str(report_path),
        ],
        color=False,
        terminal_width=200,
    )

    assert result.exit_code != 0
    assert "expectation tool must match --tool" in unstyle(result.output)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["checks"][-1]["check_id"] == "remote-mcp.preflight"


def test_cli_fresh_spack_preflight_resolves_every_alias_before_dispatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registration = _spack_validation_registration()
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"spack": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    catalog = _fake_spack_validation_catalog(omit={"spack_find"})

    def load_catalog(_profile: str) -> Any:
        return catalog

    monkeypatch.setattr("clio_relay.cli.load_registered_remote_mcp_catalog", load_catalog)
    dispatched = False

    def reject_dispatch(**_kwargs: object) -> None:
        nonlocal dispatched
        dispatched = True
        raise AssertionError("preflight failure must precede dispatch")

    monkeypatch.setattr("clio_relay.cli._execute_remote_mcp_validation_call", reject_dispatch)
    report_path = tmp_path / "validation" / "fresh-preflight.json"

    result = CliRunner().invoke(
        app,
        _fresh_spack_cli_arguments(validation_report=report_path),
    )

    assert result.exit_code != 0
    assert "alpha/spack/spack_find" in result.output
    assert dispatched is False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["checks"][-1]["check_id"] == "remote-mcp.preflight"


def test_cli_fresh_spack_runs_ordered_transition_and_emits_combined_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    registration = _spack_validation_registration()
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"spack": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )

    def load_catalog(_profile: str) -> Any:
        return _fake_spack_validation_catalog()

    monkeypatch.setattr("clio_relay.cli.load_registered_remote_mcp_catalog", load_catalog)
    spec = "zlib@1.3.1"
    dag_hash = "a" * 32
    store_root = "/scratch/acceptance/spack-store"
    protocols = _spack_transition_protocol_results(
        spec=spec,
        dag_hash=dag_hash,
        prefix=f"{store_root}/zlib-1.3.1-a",
    )
    phase_for_tool = {
        "spack_find": "preinstall",
        "spack_install": "install",
        "spack_locate": "postinstall",
    }
    protocol_for_tool = dict(zip(phase_for_tool, protocols, strict=True))
    events: list[str] = []

    def execute(**kwargs: object) -> Any:
        remote_tool_name = cast(str, kwargs["remote_tool_name"])
        remote_arguments = cast(dict[str, object], kwargs["remote_arguments"])
        route = cast(Any, kwargs["route"])
        events.append(remote_tool_name)
        assert route.alias == f"remote_spack_fresh_{remote_tool_name.removeprefix('spack_')}"
        expected_arguments = {
            "spack_find": {"query": spec},
            "spack_install": {"spec": spec, "reuse": False},
            "spack_locate": {"spec": f"/{dag_hash}"},
        }[remote_tool_name]
        assert remote_arguments == expected_arguments
        phase = phase_for_tool[remote_tool_name]
        return SimpleNamespace(
            report=_ordinary_spack_transition_report(phase, spec=spec),
            protocol_result=protocol_for_tool[remote_tool_name],
            stdio_session=cast(Any, None),
        )

    def observe(**kwargs: object) -> RemoteMcpSpackConfigurationObservation:
        phase = cast(str, kwargs["phase"])
        events.append(f"configuration-{phase}")
        return _spack_configuration_observation(
            phase,
            manifest_path="/scratch/acceptance/configuration.json",
            manifest_sha256="6" * 64,
        )

    monkeypatch.setattr("clio_relay.cli._execute_remote_mcp_validation_call", execute)
    monkeypatch.setattr("clio_relay.cli._collect_spack_configuration_observation", observe)
    output_path = tmp_path / "validation" / "fresh-transition.json"

    result = CliRunner().invoke(
        app,
        _fresh_spack_cli_arguments(output_json=output_path),
    )

    assert result.exit_code == 0, result.output
    assert events == [
        "spack_find",
        "configuration-preinstall",
        "spack_install",
        "spack_locate",
        "configuration-postinstall",
    ]
    report = json.loads(result.output)
    assert report["passed"] is True
    assert report["spack_install_transition"]["requested_spec"] == spec
    assert report["spack_install_transition"]["dag_hash"] == dag_hash
    assert (
        next(
            check
            for check in report["checks"]
            if check["name"] == "remote-mcp.spack-fresh-configuration"
        )["passed"]
        is True
    )
    assert json.loads(output_path.read_text(encoding="utf-8")) == report


def test_cli_fresh_spack_refuses_install_when_preinstall_is_not_absent(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    registration = _spack_validation_registration()
    ClusterRegistry(clusters={"alpha": _cluster("alpha", {"spack": registration})}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )

    def load_catalog(_profile: str) -> Any:
        return _fake_spack_validation_catalog()

    monkeypatch.setattr("clio_relay.cli.load_registered_remote_mcp_catalog", load_catalog)
    calls: list[str] = []

    def execute(**kwargs: object) -> Any:
        remote_tool_name = cast(str, kwargs["remote_tool_name"])
        calls.append(remote_tool_name)
        assert remote_tool_name == "spack_find"
        return SimpleNamespace(
            report=_ordinary_spack_transition_report("preinstall", spec="zlib@1.3.1"),
            protocol_result={
                "structuredContent": {
                    "schema_version": "spack.mcp.result.v1",
                    "operation": "find",
                    "query": "zlib@1.3.1",
                    "count": 1,
                    "packages": [{"name": "zlib", "dag_hash": "a" * 32}],
                }
            },
            stdio_session=cast(Any, None),
        )

    observed = False

    def observe(**_kwargs: object) -> None:
        nonlocal observed
        observed = True
        raise AssertionError("configuration observation must follow proven absence")

    monkeypatch.setattr("clio_relay.cli._execute_remote_mcp_validation_call", execute)
    monkeypatch.setattr("clio_relay.cli._collect_spack_configuration_observation", observe)
    report_path = tmp_path / "validation" / "fresh-refused.json"

    result = CliRunner().invoke(
        app,
        _fresh_spack_cli_arguments(validation_report=report_path),
    )

    assert result.exit_code != 0
    assert calls == ["spack_find"]
    assert observed is False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert "count=0" in report["checks"][-1]["error"]


def test_remote_spack_configuration_observation_uses_bounded_bash_command(
    monkeypatch: MonkeyPatch,
) -> None:
    manifest_path = "/scratch/fresh root/acceptance-manifest.sha256"
    expected_sha256 = "6" * 64
    observed_timeout: list[float | None] = []
    commands: list[str] = []
    payload = _spack_configuration_observation(
        "preinstall",
        manifest_path=manifest_path,
        manifest_sha256=expected_sha256,
    ).model_dump(mode="json")

    def run_shell(_definition: ClusterDefinition, command: str) -> str:
        from clio_relay import remote_cli

        commands.append(command)
        timeout_context = remote_cli.__dict__["_REMOTE_COMMAND_TIMEOUT_SECONDS"]
        observed_timeout.append(timeout_context.get())
        return json.dumps(payload)

    monkeypatch.setattr("clio_relay.cli.run_remote_shell", run_shell)
    collect_remote = relay_cli.__dict__["_collect_remote_spack_configuration_observation"]
    observation = collect_remote(
        definition=ClusterDefinition(name="alpha", ssh_host="ares"),
        phase="preinstall",
        manifest_path=manifest_path,
        expected_sha256=expected_sha256,
    )

    assert observation.manifest_sha256 == expected_sha256
    assert observed_timeout == [relay_cli.SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS]
    assert len(commands) == 1
    assert commands[0].startswith("python3 -c ")
    assert "powershell" not in commands[0].lower()
    assert "cmd.exe" not in commands[0].lower()
    assert shlex.quote(manifest_path) in commands[0]
    assert commands[0].endswith(f" {relay_cli.MAX_SPACK_CONFIGURATION_TREE_ENTRIES}")
    observer_script = relay_cli.__dict__["_remote_spack_configuration_observer_script"]
    observer_source = observer_script()
    compile(observer_source, "<observer>", "exec")
    assert "O_NOFOLLOW" in observer_source
    assert "stat.S_ISREG" in observer_source
    assert "with os.scandir(current) as entries" in observer_source
    assert "configuration tree entry count exceeded its bound" in observer_source
    assert "configuration tree entry must not be a symbolic link" in observer_source
    assert "configuration tree files do not exactly match" in observer_source
    assert "file exceeded bound while reading" in observer_source


def test_spack_configuration_tree_rejects_extra_symlink_and_unbounded_entries(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    require_exact_tree = relay_cli.__dict__["_require_exact_spack_configuration_component_set"]
    wrapper = tmp_path / "bin" / "spack"
    configuration = tmp_path / "config" / "config.yaml"
    wrapper.parent.mkdir()
    configuration.parent.mkdir()
    wrapper.write_bytes(b'#!/bin/sh\nexec /opt/spack/bin/spack "$@"\n')
    configuration.write_bytes(b"config:\n  install_tree: /scratch/store\n")
    declarations = [
        (hashlib.sha256(wrapper.read_bytes()).hexdigest(), "bin/spack"),
        (hashlib.sha256(configuration.read_bytes()).hexdigest(), "config/config.yaml"),
    ]

    require_exact_tree(tmp_path, declarations)

    extra = tmp_path / "config" / "unlisted.yaml"
    extra.write_text("unlisted: true\n", encoding="utf-8")
    with pytest.raises(RelayError, match="do not exactly match"):
        require_exact_tree(tmp_path, declarations)

    original_lstat = Path.lstat

    def symlink_lstat(path: Path) -> os.stat_result:
        if path == extra:
            return os.stat_result((stat.S_IFLNK | 0o777, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        return original_lstat(path)

    with monkeypatch.context() as symlink_patch:
        symlink_patch.setattr(Path, "lstat", symlink_lstat)
        with pytest.raises(RelayError, match="must not be a symbolic link"):
            require_exact_tree(tmp_path, declarations)

    extra.unlink()
    with monkeypatch.context() as bound_patch:
        bound_patch.setattr(relay_cli, "MAX_SPACK_CONFIGURATION_TREE_ENTRIES", 2)
        with pytest.raises(RelayError, match="entry count exceeded"):
            require_exact_tree(tmp_path, declarations)


def test_local_spack_configuration_observation_is_real_and_nofollow(
    tmp_path: Path,
) -> None:
    collect_local = relay_cli.__dict__["_collect_local_spack_configuration_observation"]
    if os.name == "nt":
        with pytest.raises(RelayError, match="requires a POSIX host"):
            collect_local(
                phase="preinstall",
                manifest_path="/scratch/configuration.json",
                expected_sha256="6" * 64,
            )
        return

    component = tmp_path / "config" / "config.yaml"
    component.parent.mkdir()
    component.write_bytes(b"config:\n  install_tree: /scratch/store\n")
    component_sha256 = hashlib.sha256(component.read_bytes()).hexdigest()
    manifest = tmp_path / "acceptance-manifest.sha256"
    manifest.write_text(
        f"{component_sha256}  config/config.yaml\n",
        encoding="utf-8",
        newline="\n",
    )
    manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()

    observation = collect_local(
        phase="preinstall",
        manifest_path=manifest.as_posix(),
        expected_sha256=manifest_sha256,
    )

    assert observation.manifest_sha256 == manifest_sha256
    assert observation.components[0].sha256 == component_sha256
    assert observation.components[0].size_bytes == component.stat().st_size

    component.unlink()
    component.symlink_to(tmp_path / "replacement.yaml")
    with pytest.raises(RelayError, match="symbolic link"):
        collect_local(
            phase="postinstall",
            manifest_path=manifest.as_posix(),
            expected_sha256=manifest_sha256,
        )


@pytest.mark.parametrize("case", ["oversized", "digest-mismatch"])
def test_remote_spack_configuration_observation_fails_closed(
    monkeypatch: MonkeyPatch,
    case: str,
) -> None:
    output = (
        "x" * (relay_cli.MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES + 1)
        if case == "oversized"
        else json.dumps(
            {
                "schema_version": "clio-relay.spack-configuration-observation.v1",
                "phase": "preinstall",
                "manifest_path": "/scratch/configuration.json",
                "manifest_sha256": "9" * 64,
                "manifest_size_bytes": 128,
                "manifest_regular_file": True,
                "components": [
                    {
                        "relative_path": "config/config.yaml",
                        "sha256": "7" * 64,
                        "size_bytes": 1,
                        "regular_file": True,
                    }
                ],
            }
        )
    )

    def run_shell(_definition: ClusterDefinition, _script: str) -> str:
        return output

    monkeypatch.setattr("clio_relay.cli.run_remote_shell", run_shell)
    collect = relay_cli.__dict__["_collect_spack_configuration_observation"]

    with pytest.raises(RelayError):
        collect(
            definition=ClusterDefinition(name="alpha", ssh_host="ares"),
            execute_remotely=True,
            expectation=RemoteMcpStructuredResultExpectation(
                contract="clio-kit-spack-user-v2",
                tool="spack_install",
                package_name="zlib",
                dag_hash="a" * 32,
                requested_spec="zlib@1.3.1",
                reuse=False,
                fresh_install_store_root="/scratch/store",
                fresh_install_configuration_sha256="6" * 64,
                fresh_install_configuration_manifest_path="/scratch/configuration.json",
            ),
            phase="preinstall",
        )


@pytest.mark.parametrize(
    "manifest",
    [
        f"{'a' * 64}  ../outside\n".encode(),
        f"{'a' * 64}  config/a\n{'b' * 64}  config/a\n".encode(),
        f"{'a' * 64} *config/a\n".encode(),
        f"{'a' * 64}  config/a".encode(),
    ],
)
def test_spack_configuration_manifest_parser_rejects_unsafe_input(
    manifest: bytes,
) -> None:
    parse_manifest = relay_cli.__dict__["_parse_spack_configuration_manifest"]
    with pytest.raises(RelayError):
        parse_manifest(manifest)


def test_declared_spack_contract_fails_closed_before_catalog_exposure() -> None:
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/clio/clio_kit-2.3.0-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "spack",
        ],
        allow_tools=["*"],
        profiles=["user"],
        contract="clio-kit-spack-user-v2",
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
    server_artifact = _server_artifact(registration)
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)

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
        assert job.spec.expected_server_artifact_digest == server_artifact_digest
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
                    "server_artifact": server_artifact,
                    "expected_server_artifact_digest": server_artifact_digest,
                    "observed_server_artifact_digest": server_artifact_digest,
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


@pytest.mark.parametrize("complete", [True, False], ids=["durable-result", "nonterminal"])
def test_cli_validate_catalog_waits_and_projects_automatic_assertion(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    complete: bool,
) -> None:
    """CLI validation waits on the queued call and projects catalog semantics."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    dataset_id = "deep-water-impact-2018-yb31-first5"
    registration = RemoteMcpServerConfig(
        command="uvx",
        args=[
            "--from",
            "/opt/wheels/clio_kit-2.5.17-py3-none-any.whl",
            "clio-kit",
            "mcp-server",
            "scientific-catalog",
        ],
        allow_tools=["scientific_dataset_describe", "scientific_dataset_search"],
        profiles=["user"],
        contract="clio-kit-scientific-catalog-user-v1.1",
    )
    registry = ClusterRegistry(
        clusters={"alpha": _cluster("alpha", {"scientific-catalog": registration})}
    )
    registry.save(tmp_path / ".clio-relay" / "clusters.json")
    contract_artifact = json.loads(
        (
            Path(remote_mcp.__file__).with_name("_contracts") / "scientific-catalog-user-v1.1.json"
        ).read_text(encoding="utf-8")
    )
    refreshed_at = datetime.now(UTC)
    discovery_entry = _entry(
        registration,
        cluster="alpha",
        server_name="scientific-catalog",
        tools=cast(list[dict[str, object]], contract_artifact["tools"]),
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
    server_artifact = _server_artifact(registration)
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
    observed_initial_states: list[JobState] = []

    def finish_or_leave_queued(
        queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        del timeout_seconds, poll_seconds
        job = queue.get_job(job_id)
        observed_initial_states.append(job.state)
        assert job.state == JobState.QUEUED
        assert isinstance(job.spec, McpCallSpec)
        assert job.spec.tool == "scientific_dataset_describe"
        assert job.spec.arguments == {"dataset_id": dataset_id}
        if not complete:
            return job
        spool = JobSpool(tmp_path / "spool", job)
        spool.initialize()
        mcp_result_path = spool.path / "mcp-result.json"
        mcp_result_path.write_text(
            json.dumps(
                {
                    "server": registration.command,
                    "server_args": registration.args,
                    "operation": "tools/call",
                    "tool": "scientific_dataset_describe",
                    "arguments": {"dataset_id": dataset_id},
                    "returncode": 0,
                    "protocol_result": {
                        "structuredContent": _scientific_catalog_describe_result(dataset_id)
                    },
                    "server_artifact": server_artifact,
                    "expected_server_artifact_digest": server_artifact_digest,
                    "observed_server_artifact_digest": server_artifact_digest,
                    "protocol_error": None,
                    "timed_out": False,
                },
                ensure_ascii=False,
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

    monkeypatch.setattr("clio_relay.cli.wait_for_terminal", finish_or_leave_queued)
    output_path = tmp_path / "validation" / "catalog-domain.json"
    canonical_path = tmp_path / "validation" / "catalog-live.json"

    result = CliRunner().invoke(
        app,
        [
            "remote-mcp",
            "validate",
            "--cluster",
            "alpha",
            "--name",
            "scientific-catalog",
            "--tool",
            "scientific_dataset_describe",
            "--arguments-json",
            json.dumps({"dataset_id": dataset_id}),
            "--profile",
            "user",
            "--output-json",
            str(output_path),
            "--validation-report",
            str(canonical_path),
        ],
    )

    assert result.exit_code == (0 if complete else 1), result.output
    assert observed_initial_states == [JobState.QUEUED]
    domain_report = json.loads(output_path.read_text(encoding="utf-8"))
    catalog_check = next(
        check
        for check in domain_report["checks"]
        if check["name"] == "remote-mcp.scientific-catalog-result"
    )
    assert catalog_check["passed"] is complete
    canonical_report = json.loads(canonical_path.read_text(encoding="utf-8"))
    assert canonical_report["status"] == ("passed" if complete else "failed")
    call_resource = next(
        resource
        for resource in canonical_report["resources"]
        if resource.get("role") == "virtual_remote_mcp_call"
    )
    assertion = call_resource["metadata"]["scientific_catalog_result_assertion"]
    assert assertion["contract"] == "clio-kit-scientific-catalog-user-v1.1"
    assert assertion["requested_dataset_id"] == dataset_id
    assert assertion["dataset_descriptor_handoff_matches"] is complete


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


def _spack_validation_registration() -> RemoteMcpServerConfig:
    """Return the audited three-tool registration used by CLI transition tests."""
    return RemoteMcpServerConfig(
        command="clio-kit",
        args=["mcp-server", "spack"],
        allow_tools=["spack_find", "spack_install", "spack_locate"],
        profiles=["user"],
        contract="clio-kit-spack-user-v2",
    )


def _fake_spack_validation_catalog(*, omit: set[str] | None = None) -> Any:
    """Return the minimum three-route catalog needed to exercise CLI orchestration."""
    omitted = omit or set()
    tools: dict[str, object] = {}
    for remote_tool_name in ("spack_find", "spack_install", "spack_locate"):
        if remote_tool_name in omitted:
            continue
        alias = f"remote_spack_fresh_{remote_tool_name.removeprefix('spack_')}"
        tools[alias] = SimpleNamespace(
            remote_tool=SimpleNamespace(name=remote_tool_name),
            routes={"alpha": SimpleNamespace(server_name="spack")},
            arguments_wrapped=False,
        )
    return SimpleNamespace(tools=tools)


def _fresh_spack_cli_arguments(
    *,
    validation_report: Path | None = None,
    output_json: Path | None = None,
) -> list[str]:
    """Render one complete fresh-install CLI invocation for focused tests."""
    arguments = [
        "remote-mcp",
        "validate",
        "--cluster",
        "alpha",
        "--name",
        "spack",
        "--tool",
        "spack_install",
        "--arguments-json",
        json.dumps({"spec": "zlib@1.3.1", "reuse": False}),
        "--result-expectation-json",
        json.dumps(
            {
                "contract": "clio-kit-spack-user-v2",
                "tool": "spack_install",
                "package_name": "zlib",
                "dag_hash": "a" * 32,
                "requested_spec": "zlib@1.3.1",
                "reuse": False,
                "fresh_install_store_root": "/scratch/acceptance/spack-store",
                "fresh_install_configuration_sha256": "6" * 64,
                "fresh_install_configuration_manifest_path": (
                    "/scratch/acceptance/configuration.json"
                ),
            }
        ),
        "--profile",
        "user",
    ]
    if validation_report is not None:
        arguments.extend(("--validation-report", str(validation_report)))
    if output_json is not None:
        arguments.extend(("--output-json", str(output_json)))
    return arguments


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


def _ordinary_spack_transition_report(
    phase: str,
    *,
    spec: str,
    spack_command: str = "/scratch/acceptance/bin/spack",
) -> RemoteMcpAcceptanceReport:
    """Return one passing ordinary acceptance report for a transition phase."""
    phase_tools = {
        "preinstall": ("spack_find", {"query": spec}),
        "install": ("spack_install", {"spec": spec, "reuse": False}),
        "postinstall": ("spack_locate", {"spec": f"/{'a' * 32}"}),
    }
    tool, arguments = phase_tools[phase]
    job_id = f"job-spack-{phase}"
    alias = f"remote_spack_{tool}"
    server_artifact: dict[str, object] = {
        "verified": True,
        "server_process_artifact_verified": True,
        "requested_command": "/opt/clio-kit/bin/clio-kit",
        "resolved_executable": "/opt/clio-kit/bin/clio-kit",
        "executable": {
            "path": "/opt/clio-kit/bin/clio-kit",
            "sha256": "1" * 64,
        },
        "install_source": "wheel",
        "install_spec": "/opt/wheels/clio_kit-2.3.1-py3-none-any.whl",
        "install_artifact_sha256": "2" * 64,
    }
    checks = [
        remote_mcp.RemoteMcpAcceptanceCheck(
            name="remote-mcp.register",
            passed=True,
            message="registered",
            evidence={
                "registration_revision": "3" * 64,
                "cluster_route_revision": "4" * 64,
            },
        ),
        remote_mcp.RemoteMcpAcceptanceCheck(
            name="remote-mcp.tools-list",
            passed=True,
            message="listed",
            evidence={
                "catalog_revision": "5" * 64,
                "registration_revision": "3" * 64,
                "cluster_route_revision": "4" * 64,
            },
        ),
        remote_mcp.RemoteMcpAcceptanceCheck(
            name="remote-mcp.call",
            passed=True,
            message="call bound",
        ),
        remote_mcp.RemoteMcpAcceptanceCheck(
            name="remote-mcp.server-artifact",
            passed=True,
            message="artifact verified",
            evidence={
                "discovery_server_artifact": deepcopy(server_artifact),
                "call_server_artifact": deepcopy(server_artifact),
            },
        ),
        remote_mcp.RemoteMcpAcceptanceCheck(
            name="remote-mcp.durable-result",
            passed=True,
            message="durable",
        ),
    ]
    artifacts = [
        {
            "artifact_id": f"artifact-{phase}-{kind}",
            "job_id": job_id,
            "kind": kind,
            "sha256": hashlib.sha256(f"{phase}:{kind}".encode()).hexdigest(),
            "uri": f"relay-artifact://alpha/artifact-{phase}-{kind}",
        }
        for kind in ("stdout", "stderr", "mcp_result", "provenance")
    ]
    return RemoteMcpAcceptanceReport(
        cluster="alpha",
        server_name="spack",
        remote_tool_name=tool,
        virtual_alias=alias,
        profile="user",
        passed=True,
        checks=checks,
        discovery={
            "provenance": {
                "discovery_job_id": "job-spack-discovery",
                "artifact_id": "artifact-spack-discovery",
            }
        },
        call_job={
            "job_id": job_id,
            "cluster": "alpha",
            "kind": "mcp_call",
            "state": "succeeded",
            "spec": {
                "operation": "tools/call",
                "tool": tool,
                "arguments": arguments,
                "server_args": [
                    "mcp-server",
                    "spack",
                    "--",
                    "--spack-command",
                    spack_command,
                ],
            },
        },
        artifacts=artifacts,
        mcp_stdio=_transition_stdio(alias=alias, job_id=job_id),
    )


def _spack_command_for_manifest(manifest_path: str) -> str:
    """Return the manifest-covered wrapper path used by transition fixtures."""
    return f"{manifest_path.rsplit('/', maxsplit=1)[0]}/bin/spack"


def _transition_stdio(*, alias: str, job_id: str) -> dict[str, object]:
    """Return passing bounded packaged-stdio evidence for one virtual call."""
    return {
        "boundary": "packaged_clio_relay_mcp_server_stdio",
        "returncode": 0,
        "initialize_response": {
            "result": {
                "protocolVersion": "2025-06-18",
                "serverInfo": {"name": "clio-relay", "version": "1.0.0"},
            }
        },
        "tools_list_response": {"result": {"tools": [{"name": alias}]}},
        "tools_call_response": {"result": {"structuredContent": {"job_id": job_id}}},
    }


def _spack_configuration_observation(
    phase: str,
    *,
    manifest_path: str,
    manifest_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Return one bounded wrapper/config manifest observation."""
    return RemoteMcpSpackConfigurationObservation.model_validate(
        {
            "phase": phase,
            "manifest_path": manifest_path,
            "manifest_sha256": manifest_sha256,
            "manifest_size_bytes": 512,
            "manifest_regular_file": True,
            "components": [
                {
                    "relative_path": "bin/spack",
                    "sha256": "8" * 64,
                    "size_bytes": 256,
                    "regular_file": True,
                },
                {
                    "relative_path": "config/config.yaml",
                    "sha256": "7" * 64,
                    "size_bytes": 128,
                    "regular_file": True,
                },
            ],
        }
    )


def _spack_transition_protocol_results(
    *,
    spec: str,
    dag_hash: str,
    prefix: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    """Return valid absence, install, and exact-hash locate protocol results."""
    package = {
        "name": "zlib",
        "version": "1.3.1",
        "dag_hash": dag_hash,
        "compiler": "gcc@12.3.0",
        "architecture": "linux-x86_64",
    }
    exact_hash_spec = f"/{dag_hash}"
    return (
        {
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": spec,
                "count": 0,
                "packages": [],
            }
        },
        {
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "install",
                "requested_spec": spec,
                "reuse": False,
                "status": "installed",
                "duration_seconds": 1.25,
                "packages": [package],
                "stdout_excerpt": None,
            }
        },
        {
            "structuredContent": {
                "schema_version": "spack.mcp.result.v1",
                "operation": "locate",
                "requested_spec": exact_hash_spec,
                "load_spec": exact_hash_spec,
                "package": package,
                "prefix": prefix,
            }
        },
    )


def _transition_check(
    report: RemoteMcpAcceptanceReport,
    name: str,
) -> remote_mcp.RemoteMcpAcceptanceCheck:
    """Return one uniquely named transition assertion from a report."""
    matches = [check for check in report.checks if check.name == name]
    assert len(matches) == 1
    return matches[0]


def _spack_result_schema(name: str) -> dict[str, object]:
    """Return a strict representative of the released Spack result schema."""
    nullable_string = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    package_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "version": deepcopy(nullable_string),
            "dag_hash": deepcopy(nullable_string),
            "compiler": deepcopy(nullable_string),
            "architecture": deepcopy(nullable_string),
        },
        "required": ["name"],
        "additionalProperties": False,
    }
    common_properties: dict[str, object] = {
        "schema_version": {"type": "string", "const": "spack.mcp.result.v1"},
        "operation": {"type": "string", "const": name.removeprefix("spack_")},
    }
    if name == "spack_find":
        return {
            "type": "object",
            "properties": {
                **common_properties,
                "query": deepcopy(nullable_string),
                "packages": {"type": "array", "items": package_schema},
                "count": {"type": "integer"},
            },
            "required": ["count"],
            "additionalProperties": False,
        }
    if name == "spack_locate":
        return {
            "type": "object",
            "properties": {
                **common_properties,
                "requested_spec": {"type": "string"},
                "load_spec": {"type": "string"},
                "package": package_schema,
                "prefix": {"type": "string"},
            },
            "required": ["requested_spec", "load_spec", "package", "prefix"],
            "additionalProperties": False,
        }
    if name == "spack_install":
        return {
            "type": "object",
            "properties": {
                **common_properties,
                "requested_spec": {"type": "string"},
                "reuse": {"type": "boolean"},
                "status": {"type": "string", "const": "installed"},
                "duration_seconds": {"type": "number"},
                "packages": {"type": "array", "items": package_schema},
                "stdout_excerpt": deepcopy(nullable_string),
            },
            "required": ["requested_spec", "reuse", "duration_seconds", "packages"],
            "additionalProperties": False,
        }
    raise AssertionError(f"unexpected Spack tool: {name}")


def _canonical_json_sha256(value: object) -> str:
    """Return the compact unescaped canonical JSON digest used by clio-kit."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _scientific_catalog_describe_output_schema() -> dict[str, object]:
    """Load the exact vendored v1.1 describe output schema."""
    contract_artifact = json.loads(
        (
            Path(remote_mcp.__file__).with_name("_contracts") / "scientific-catalog-user-v1.1.json"
        ).read_text(encoding="utf-8")
    )
    describe_tool = next(
        tool
        for tool in cast(list[dict[str, object]], contract_artifact["tools"])
        if tool["name"] == "scientific_dataset_describe"
    )
    return cast(dict[str, object], describe_tool["outputSchema"])


def _scientific_catalog_describe_result(dataset_id: str) -> dict[str, object]:
    """Return one valid v1.1 describe result with canonical descriptor identity."""
    descriptor_without_fingerprint: dict[str, object] = {
        "schema_version": "jarvis.dataset-descriptor.v1",
        "dataset_id": dataset_id,
        "kind": "temporal-volume",
        "format": "vti",
        "members": [
            {
                "index": 0,
                "location": "/mnt/common/datasets-staging/scivis/asteroid-000.vti",
                "timestep": 0.0,
            }
        ],
        "arrays": [
            {
                "name": "density",
                "association": "point",
                "components": 1,
            }
        ],
        "bounds": [0.0, 1.0, 0.0, 1.0, 0.0, 1.0],
        "source_artifact": None,
    }
    fingerprint = _canonical_json_sha256(descriptor_without_fingerprint)
    descriptor = {
        **descriptor_without_fingerprint,
        "fingerprint": {"algorithm": "sha256", "digest": fingerprint},
    }
    descriptor_sha256 = _canonical_json_sha256(descriptor)
    return {
        "schema_version": "clio-kit.scientific-dataset-description.v1",
        "site_id": "ares",
        "catalog_revision": "release-v1",
        "catalog_sha256": "a" * 64,
        "dataset": {
            "dataset_id": dataset_id,
            "title": "2018 Deep Water Impact first five timesteps",
            "summary": "Bounded release-acceptance dataset record.",
            "tags": ["asteroid", "impact", "volume"],
            "descriptor": deepcopy(descriptor),
        },
        "dataset_descriptor": deepcopy(descriptor),
        "descriptor_sha256": descriptor_sha256,
    }


def _scientific_catalog_tool(
    name: str,
    *,
    contract_id: str = "clio-kit-scientific-catalog-user-v1.1",
) -> dict[str, object]:
    """Return the bounded schema shape required by the scientific catalog contract check."""
    annotations = {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    if name == "scientific_dataset_describe":
        descriptor_schema: dict[str, object] = {
            "type": "object",
            "properties": {
                "schema_version": {
                    "const": "jarvis.dataset-descriptor.v1",
                    "type": "string",
                }
            },
            "additionalProperties": False,
        }
        output_properties: dict[str, object] = {
            "schema_version": {
                "const": "clio-kit.scientific-dataset-description.v1",
                "default": "clio-kit.scientific-dataset-description.v1",
                "type": "string",
            },
            "dataset": {
                "type": "object",
                "properties": {"descriptor": deepcopy(descriptor_schema)},
                "additionalProperties": False,
            },
        }
        output_required: list[str] = []
        if contract_id == "clio-kit-scientific-catalog-user-v1.1":
            output_properties["dataset_descriptor"] = deepcopy(descriptor_schema)
            output_required.append("dataset_descriptor")
        elif contract_id != "clio-kit-scientific-catalog-user-v1":
            raise AssertionError(f"unsupported scientific catalog contract: {contract_id}")
        return {
            "name": name,
            "description": "Return one exact operator catalog record.",
            "annotations": annotations,
            "inputSchema": {
                "type": "object",
                "properties": {"dataset_id": {"type": "string"}},
                "required": ["dataset_id"],
                "additionalProperties": False,
            },
            "outputSchema": {
                "type": "object",
                "properties": output_properties,
                "required": output_required,
                "additionalProperties": False,
            },
        }
    if name != "scientific_dataset_search":
        raise AssertionError(f"unsupported scientific catalog test tool: {name}")
    nullable_string = {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None}
    return {
        "name": name,
        "description": "Search operator-registered scientific datasets.",
        "annotations": annotations,
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": nullable_string,
                "tags": {
                    "anyOf": [
                        {"items": {"type": "string"}, "type": "array"},
                        {"type": "null"},
                    ],
                    "default": None,
                },
                "kind": nullable_string,
                "format": nullable_string,
                "page_size": {
                    "default": 20,
                    "maximum": 100,
                    "minimum": 1,
                    "type": "integer",
                },
                "cursor": nullable_string,
            },
            "additionalProperties": False,
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "schema_version": {
                    "const": "clio-kit.scientific-dataset-search.v1",
                    "default": "clio-kit.scientific-dataset-search.v1",
                    "type": "string",
                },
                "datasets": {
                    "type": "array",
                    "items": {"type": "object", "additionalProperties": False},
                },
            },
            "additionalProperties": False,
        },
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
    if name in {"spack_find", "spack_locate", "spack_install"}:
        tool["outputSchema"] = _spack_result_schema(name)
    return tool


def _discovery_artifact(
    registration: RemoteMcpServerConfig,
    *,
    tools: list[dict[str, object]],
    server_artifact: dict[str, object] | None = None,
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
            "server_artifact": (
                server_artifact if server_artifact is not None else _server_artifact(registration)
            ),
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


def _jarvis_cache_entry(
    *,
    cluster: str,
    executable_sha: str,
) -> tuple[RemoteMcpSchemaCacheEntry, dict[str, object]]:
    """Return one fresh built-in JARVIS discovery entry and its bound artifact."""
    contract = jarvis_user_contract()
    now = datetime.now(UTC)
    server_artifact = verified_jarvis_server_artifact()
    executable = cast(dict[str, object], server_artifact["executable"])
    executable["sha256"] = executable_sha
    entry = RemoteMcpSchemaCacheEntry(
        cluster=cluster,
        server_name=JARVIS_MCP_CACHE_SERVER_NAME,
        execution_fingerprint="jarvis-fixture",
        discovered_at=now,
        expires_at=now + timedelta(hours=1),
        schema_digest=CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
        tools=[
            RemoteMcpToolSchema(
                name=name,
                description=str(definition["description"]),
                input_schema=definition["inputSchema"],
                output_schema=definition["outputSchema"],
                annotations=definition["annotations"],
            )
            for name, definition in contract.items()
        ],
        provenance=RemoteMcpDiscoveryProvenance(
            discovery_job_id=f"job-{cluster}-jarvis",
            artifact_id=f"artifact-{cluster}-jarvis-{executable_sha[:8]}",
            artifact_sha256="c" * 64,
            server_artifact=server_artifact,
        ),
    )
    return entry, server_artifact
