"""Probe and schema-validate the native JARVIS execution/query contract.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns the
clio-kit MCP contract probe (``probe_clio_kit_native_execution_contract``),
the in-process JARVIS-CD capability probe
(``probe_jarvis_native_execution_capability``), the JSON-Schema shape
assertions those two probes' contract documents must satisfy, and the
component-name -> expected-capability matcher
(``_native_capability_matches_component``) that every later verification
owner module (``component_runtime_identity.py``,
``component_verification_remote.py``) calls to decide whether an observed
capability is the receipt's exact contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.contract_gate import mcp_contract_digest, run_json_probe
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import (
    JARVIS_EXECUTION_HANDLE_SCHEMA,
    JARVIS_EXECUTION_PROGRESS_SCHEMA,
    JARVIS_EXECUTION_RECORD_SCHEMA,
    NATIVE_JARVIS_CAPABILITY_SCHEMA,
    NativeJarvisExecutionCapability,
    _is_sha256_text,
)

JARVIS_EXECUTION_ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
JARVIS_ARTIFACT_SCHEMA = "jarvis.artifact.v1"
JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
CLIO_KIT_JARVIS_EXECUTION_SCHEMA = "clio-kit.jarvis-execution.v2"
CLIO_KIT_JARVIS_CONTRACT_ID = "clio-kit-jarvis-user-v3.7"
CLIO_KIT_MCP_CONTRACT_SCHEMA = "clio-kit.mcp-user-contract.v1"
CLIO_KIT_NATIVE_OPERATIONS = (
    "jarvis_get_execution",
    "jarvis_run",
)
JARVIS_CD_NATIVE_OPERATIONS = (
    "execution_handle.progress",
    "execution_store.resolve_service_runtime_authority",
    "pipeline.get_execution",
    "pipeline.get_execution_progress",
    "pipeline.run",
)


def probe_clio_kit_native_execution_contract(
    runtime_command: list[str],
) -> NativeJarvisExecutionCapability:
    """Probe the receipt-bound clio-kit wheel for the exact native JARVIS contract."""
    try:
        cli_index = max(
            index
            for index, argument in enumerate(runtime_command)
            if Path(argument).name.casefold() in {"clio-kit", "clio-kit.exe"}
        )
    except ValueError as exc:
        raise ConfigurationError(
            "clio-kit native execution probe command has no clio-kit launcher"
        ) from exc
    if runtime_command[cli_index + 1 :] != ["mcp-server", "jarvis"]:
        raise ConfigurationError(
            "clio-kit native execution probe requires the receipt-bound JARVIS MCP command"
        )
    probe_command = [
        *runtime_command[: cli_index + 1],
        "mcp-contract",
        CLIO_KIT_JARVIS_CONTRACT_ID,
    ]
    document = run_json_probe(probe_command, label="clio-kit native execution contract")
    if (
        document.get("schema_version") != CLIO_KIT_MCP_CONTRACT_SCHEMA
        or document.get("contract_id") != CLIO_KIT_JARVIS_CONTRACT_ID
    ):
        raise ConfigurationError("clio-kit native execution contract identity did not match")
    raw_tools = document.get("tools")
    if not isinstance(raw_tools, list):
        raise ConfigurationError("clio-kit native execution contract tools were invalid")
    raw_tool_items = cast(list[object], raw_tools)
    if not all(isinstance(item, dict) for item in raw_tool_items):
        raise ConfigurationError("clio-kit native execution contract tools were invalid")
    tools = [cast(dict[str, object], item) for item in raw_tool_items]
    by_name: dict[str, dict[str, object]] = {}
    for tool in tools:
        name = tool.get("name")
        if not isinstance(name, str) or not name or name in by_name:
            raise ConfigurationError("clio-kit native execution contract tool identity was invalid")
        by_name[name] = tool
    missing_operations = sorted(set(CLIO_KIT_NATIVE_OPERATIONS) - set(by_name))
    if missing_operations:
        raise ConfigurationError(
            f"clio-kit native execution contract omitted operations: {missing_operations}"
        )
    _require_native_output_documents(
        by_name["jarvis_run"],
        {
            "execution_handle": JARVIS_EXECUTION_HANDLE_SCHEMA,
            "execution_record": JARVIS_EXECUTION_RECORD_SCHEMA,
            "progress": JARVIS_EXECUTION_PROGRESS_SCHEMA,
        },
    )
    run_input_schema = by_name["jarvis_run"].get("inputSchema")
    if not isinstance(run_input_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS run input schema was invalid")
    from clio_relay.jarvis_mcp import require_handle_first_jarvis_run_schema

    require_handle_first_jarvis_run_schema(
        cast(dict[str, Any], run_input_schema),
        error_type=ConfigurationError,
        label="clio-kit native JARVIS contract",
    )
    _require_native_execution_query_contract(by_name["jarvis_get_execution"])
    contract_sha256 = document.get("contract_sha256")
    observed_contract_sha256 = mcp_contract_digest(tools)
    from clio_relay.jarvis_mcp import CLIO_KIT_JARVIS_USER_CONTRACT_SHA256

    if (
        not isinstance(contract_sha256, str)
        or contract_sha256 != observed_contract_sha256
        or contract_sha256 != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    ):
        raise ConfigurationError("clio-kit native execution contract digest did not match")
    return NativeJarvisExecutionCapability(
        operations=list(CLIO_KIT_NATIVE_OPERATIONS),
        contract_id=CLIO_KIT_JARVIS_CONTRACT_ID,
        contract_schema_version=CLIO_KIT_MCP_CONTRACT_SCHEMA,
        contract_sha256=contract_sha256,
    )


def probe_jarvis_native_execution_capability(
    python: str | None,
) -> NativeJarvisExecutionCapability:
    """Probe one interpreter for JARVIS-CD native execution and query semantics."""
    if python is None:
        raise ConfigurationError("JARVIS native execution interpreter is not configured")
    script = f"""
import json

from jarvis_cd.core.execution import (
    ExecutionHandle,
    ExecutionStore,
    HANDLE_SCHEMA,
    PROGRESS_SNAPSHOT_SCHEMA,
    RECORD_SCHEMA,
)
from jarvis_cd.core.pipeline import Pipeline

operations = {{
    "execution_handle.progress": callable(getattr(ExecutionHandle, "progress", None)),
    "execution_store.resolve_service_runtime_authority": callable(
        getattr(ExecutionStore, "resolve_service_runtime_authority", None)
    ),
    "pipeline.get_execution": callable(getattr(Pipeline, "get_execution", None)),
    "pipeline.get_execution_progress": callable(
        getattr(Pipeline, "get_execution_progress", None)
    ),
    "pipeline.run": callable(getattr(Pipeline, "run", None)),
}}
if not all(operations.values()):
    raise SystemExit("JARVIS-CD native execution API is incomplete")
print(json.dumps({{
    "schema_version": {NATIVE_JARVIS_CAPABILITY_SCHEMA!r},
    "handle_schema": HANDLE_SCHEMA,
    "record_schema": RECORD_SCHEMA,
    "progress_schema": PROGRESS_SNAPSHOT_SCHEMA,
    "operations": sorted(operations),
    "contract_id": None,
    "contract_schema_version": None,
    "contract_sha256": None,
}}, sort_keys=True))
"""
    document = run_json_probe(
        [python, "-c", script],
        label="JARVIS-CD native execution capability",
    )
    try:
        capability = NativeJarvisExecutionCapability.model_validate(document)
    except ValidationError as exc:
        raise ConfigurationError(
            f"JARVIS-CD native execution capability was invalid: {exc}"
        ) from exc
    if not _native_capability_matches_component(capability, component_name="jarvis-cd"):
        raise ConfigurationError("JARVIS-CD native execution capability did not match")
    return capability


def _require_native_query_input(tool: dict[str, object]) -> None:
    """Require a query tool to bind both pipeline and execution identity."""
    input_schema = tool.get("inputSchema")
    if not isinstance(input_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted inputSchema")
    required = cast(dict[str, object], input_schema).get("required")
    if not isinstance(required, list) or set(cast(list[object], required)) != {
        "pipeline_id",
        "execution_id",
    }:
        raise ConfigurationError("clio-kit native JARVIS query identity schema did not match")


def _require_native_execution_query_contract(tool: dict[str, object]) -> None:
    """Require clio-kit's single bounded execution/progress/artifact query."""
    _require_native_query_input(tool)
    input_schema = cast(dict[str, object], tool["inputSchema"])
    if input_schema.get("additionalProperties") is not False:
        raise ConfigurationError("clio-kit native JARVIS query accepted unknown inputs")
    raw_input_properties = input_schema.get("properties")
    if not isinstance(raw_input_properties, dict):
        raise ConfigurationError("clio-kit native JARVIS query properties were incomplete")
    input_properties = cast(dict[str, object], raw_input_properties)
    if set(input_properties) != {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }:
        raise ConfigurationError("clio-kit native JARVIS query surface did not match")
    if input_properties.get("include_progress") != {"default": True, "type": "boolean"}:
        raise ConfigurationError("clio-kit native JARVIS progress selector did not match")
    if input_properties.get("include_service_runtimes") != {
        "default": False,
        "type": "boolean",
    }:
        raise ConfigurationError("clio-kit native JARVIS service selector did not match")
    raw_artifacts = input_properties.get("artifacts")
    if (
        not isinstance(raw_artifacts, dict)
        or cast(dict[str, object], raw_artifacts).get("default") is not None
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact selector was incomplete")
    artifact_query = _nullable_schema_option(
        cast(dict[str, object], raw_artifacts),
        expected_type="object",
        label="artifact selector",
    )
    raw_artifact_properties = artifact_query.get("properties")
    if artifact_query.get("additionalProperties") is not False or not isinstance(
        raw_artifact_properties, dict
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact filters were incomplete")
    artifact_properties = cast(dict[str, object], raw_artifact_properties)
    if set(artifact_properties) != {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
        "content_max_bytes",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact filter surface did not match")
    raw_content_max_bytes = artifact_properties.get("content_max_bytes")
    if (
        not isinstance(raw_content_max_bytes, dict)
        or cast(dict[str, object], raw_content_max_bytes).get("default") is not None
    ):
        raise ConfigurationError(
            "clio-kit native JARVIS artifact content_max_bytes selector did not match"
        )
    if artifact_properties.get("page_size") != {
        "default": 50,
        "description": "Maximum artifacts to return in this page.",
        "maximum": 100,
        "minimum": 1,
        "type": "integer",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact page bound did not match")
    expected_filter_limits = {
        "package_id": 256,
        "artifact_id": 90,
        "cursor": 1024,
    }
    for field_name, maximum in expected_filter_limits.items():
        raw_filter = artifact_properties.get(field_name)
        if not isinstance(raw_filter, dict):
            raise ConfigurationError(f"clio-kit native JARVIS {field_name} filter was incomplete")
        filter_schema = _nullable_schema_option(
            cast(dict[str, object], raw_filter),
            expected_type="string",
            label=f"{field_name} filter",
        )
        if filter_schema.get("maxLength") != maximum:
            raise ConfigurationError(
                f"clio-kit native JARVIS {field_name} filter bound did not match"
            )

    output_schema = tool.get("outputSchema")
    if not isinstance(output_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted outputSchema")
    typed_output = cast(dict[str, object], output_schema)
    raw_output_properties = typed_output.get("properties")
    required = typed_output.get("required")
    expected_output_fields = {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
        "service_runtimes",
    }
    if (
        typed_output.get("additionalProperties") is not False
        or not isinstance(raw_output_properties, dict)
        or not isinstance(required, list)
        or set(cast(list[object], required)) != expected_output_fields
        or set(cast(dict[str, object], raw_output_properties)) != expected_output_fields
    ):
        raise ConfigurationError("clio-kit native JARVIS execution envelope did not match")
    output_properties = cast(dict[str, object], raw_output_properties)
    if output_properties.get("schema_version") != {
        "const": CLIO_KIT_JARVIS_EXECUTION_SCHEMA,
        "type": "string",
    }:
        raise ConfigurationError("clio-kit native JARVIS execution schema did not match")
    _require_native_output_documents(
        tool,
        {
            "execution_handle": JARVIS_EXECUTION_HANDLE_SCHEMA,
            "execution_record": JARVIS_EXECUTION_RECORD_SCHEMA,
        },
    )
    raw_progress = output_properties.get("progress")
    if not isinstance(raw_progress, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted nullable progress")
    progress = _nullable_schema_option(
        cast(dict[str, object], raw_progress),
        expected_type="object",
        label="progress output",
    )
    _require_schema_identity(
        progress,
        field_name="schema_version",
        schema_version=JARVIS_EXECUTION_PROGRESS_SCHEMA,
        label="progress output",
    )
    raw_service_runtimes = output_properties.get("service_runtimes")
    if not isinstance(raw_service_runtimes, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted nullable services")
    service_runtimes = _nullable_schema_option(
        cast(dict[str, object], raw_service_runtimes),
        expected_type="object",
        label="service runtimes output",
    )
    _require_schema_identity(
        service_runtimes,
        field_name="schema_version",
        schema_version=JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA,
        label="service runtimes output",
    )
    raw_artifact_page = output_properties.get("artifact_page")
    if not isinstance(raw_artifact_page, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted nullable artifact page")
    artifact_page = _nullable_schema_option(
        cast(dict[str, object], raw_artifact_page),
        expected_type="object",
        label="artifact page",
    )
    artifact_page_required = artifact_page.get("required")
    artifact_page_properties = artifact_page.get("properties")
    expected_artifact_page_fields = {
        "producer_schema_version",
        "pipeline_id",
        "execution_id",
        "execution_state",
        "terminal",
        "artifacts",
        "matching_artifact_count",
        "returned_artifact_count",
        "next_cursor",
    }
    if (
        artifact_page.get("additionalProperties") is not False
        or not isinstance(artifact_page_required, list)
        or set(cast(list[object], artifact_page_required)) != expected_artifact_page_fields
        or not isinstance(artifact_page_properties, dict)
        or set(cast(dict[str, object], artifact_page_properties)) != expected_artifact_page_fields
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact page schema did not match")
    typed_artifact_page_properties = cast(dict[str, object], artifact_page_properties)
    if typed_artifact_page_properties.get("producer_schema_version") != {
        "const": JARVIS_EXECUTION_ARTIFACTS_SCHEMA,
        "type": "string",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact page identity did not match")
    artifacts_schema = typed_artifact_page_properties.get("artifacts")
    if not isinstance(artifacts_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS artifact page omitted artifacts")
    artifact_item = cast(dict[str, object], artifacts_schema).get("items")
    if not isinstance(artifact_item, dict):
        raise ConfigurationError("clio-kit native JARVIS artifact item schema was incomplete")
    _require_schema_identity(
        cast(dict[str, object], artifact_item),
        field_name="schema_version",
        schema_version=JARVIS_ARTIFACT_SCHEMA,
        label="artifact item",
    )


def _nullable_schema_option(
    schema: dict[str, object],
    *,
    expected_type: str,
    label: str,
) -> dict[str, object]:
    """Return the non-null branch of an exact two-way nullable schema."""
    raw_options = schema.get("anyOf")
    if not isinstance(raw_options, list):
        raise ConfigurationError(f"clio-kit native JARVIS {label} was not nullable")
    raw_option_items = cast(list[object], raw_options)
    if len(raw_option_items) != 2:
        raise ConfigurationError(f"clio-kit native JARVIS {label} was not nullable")
    options = [cast(dict[str, object], item) for item in raw_option_items if isinstance(item, dict)]
    non_null = [item for item in options if item.get("type") == expected_type]
    nulls = [item for item in options if item == {"type": "null"}]
    if len(options) != 2 or len(non_null) != 1 or len(nulls) != 1:
        raise ConfigurationError(f"clio-kit native JARVIS {label} nullable schema did not match")
    return non_null[0]


def _require_schema_identity(
    schema: dict[str, object],
    *,
    field_name: str,
    schema_version: str,
    label: str,
) -> None:
    """Require one nested object's constant schema-version property."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        raise ConfigurationError(f"clio-kit native JARVIS {label} schema was incomplete")
    identity = cast(dict[str, object], properties).get(field_name)
    if identity != {"const": schema_version, "type": "string"}:
        raise ConfigurationError(f"clio-kit native JARVIS {label} identity did not match")


def _require_native_output_documents(
    tool: dict[str, object],
    expected_documents: dict[str, str],
) -> None:
    """Require exact top-level native document fields and schema constants."""
    output_schema = tool.get("outputSchema")
    if not isinstance(output_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS tool omitted outputSchema")
    typed_output = cast(dict[str, object], output_schema)
    properties = typed_output.get("properties")
    required = typed_output.get("required")
    if not isinstance(properties, dict) or not isinstance(required, list):
        raise ConfigurationError("clio-kit native JARVIS output schema was incomplete")
    typed_properties = cast(dict[str, object], properties)
    required_names = {str(item) for item in cast(list[object], required)}
    for field_name, schema_version in expected_documents.items():
        field_schema = typed_properties.get(field_name)
        if not isinstance(field_schema, dict) or field_name not in required_names:
            raise ConfigurationError(f"clio-kit native JARVIS output omitted required {field_name}")
        schema_properties = cast(dict[str, object], field_schema).get("properties")
        if not isinstance(schema_properties, dict):
            raise ConfigurationError(f"clio-kit native JARVIS {field_name} schema was incomplete")
        schema_field = cast(dict[str, object], schema_properties).get("schema_version")
        if (
            not isinstance(schema_field, dict)
            or cast(dict[str, object], schema_field).get("const") != schema_version
        ):
            raise ConfigurationError(
                f"clio-kit native JARVIS {field_name} schema identity did not match"
            )


def _native_capability_matches_component(
    capability: NativeJarvisExecutionCapability,
    *,
    component_name: str,
) -> bool:
    """Return whether a native capability is the exact contract for its component."""
    if component_name == "clio-kit":
        return (
            capability.operations == list(CLIO_KIT_NATIVE_OPERATIONS)
            and capability.contract_id == CLIO_KIT_JARVIS_CONTRACT_ID
            and capability.contract_schema_version == CLIO_KIT_MCP_CONTRACT_SCHEMA
            and _is_sha256_text(capability.contract_sha256)
        )
    if component_name == "jarvis-cd":
        return (
            capability.operations == list(JARVIS_CD_NATIVE_OPERATIONS)
            and capability.contract_id is None
            and capability.contract_schema_version is None
            and capability.contract_sha256 is None
        )
    return False
