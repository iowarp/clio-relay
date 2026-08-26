"""Local and remote JARVIS MCP tool-contract validation.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): binds the remote JARVIS MCP tool surface (live
``tools/list``) against the pinned ``CLIO_KIT_JARVIS_USER_CONTRACT_SHA256``
contract and its computed schema digest, summarizes the bounded
package-search and unified execution-query tool schemas without copying them
wholesale into evidence, and validates one relay-exposed local (virtual)
JARVIS tool's input/output/annotation schema against the pinned contract it
was derived from. Called by ``build_jarvis_mcp_validation_report`` in
``jarvis_mcp_validation_report.py`` and by the package-search/execution-query
evidence builders.
"""

from __future__ import annotations

from copy import deepcopy
from typing import cast

from pydantic import ValidationError

from clio_relay.installation import (
    CLIO_KIT_JARVIS_EXECUTION_SCHEMA,
    JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
    virtual_jarvis_job_output_schema,
)
from clio_relay.jarvis_mcp_validation_core import JSON, _is_string_list, _mapping
from clio_relay.remote_mcp import RemoteMcpToolSchema, remote_mcp_schema_digest
from clio_relay.remote_mcp_tool_schema import resolve_remote_tool_title


def _remote_jarvis_contract(document: JSON | None) -> tuple[JSON, bool]:
    protocol = _mapping(document.get("protocol_result")) if document else None
    raw_tools = protocol.get("tools") if protocol else None
    tools = (
        [cast(JSON, item) for item in cast(list[object], raw_tools) if isinstance(item, dict)]
        if isinstance(raw_tools, list)
        else []
    )
    by_name = {str(tool["name"]): tool for tool in tools if isinstance(tool.get("name"), str)}
    expected = set(jarvis_user_contract())
    edit_schema = _mapping(by_name.get("jarvis_edit_step", {}).get("inputSchema"))
    edit_properties = _mapping(edit_schema.get("properties")) if edit_schema else None
    operation = _mapping(edit_properties.get("operation")) if edit_properties else None
    run_schema = _mapping(by_name.get("jarvis_run", {}).get("inputSchema"))
    run_properties = _mapping(run_schema.get("properties")) if run_schema else None
    spack_specs = _mapping(run_properties.get("spack_specs")) if run_properties else None
    handle_first_run = run_properties is not None and "wait" not in run_properties
    query_evidence, query_passed = _execution_query_contract_evidence(
        by_name.get("jarvis_get_execution")
    )
    package_search_evidence, package_search_passed = _package_search_contract_evidence(
        by_name.get("jarvis_describe")
    )
    observed_digest: str | None = None
    contract_error: str | None = None
    try:
        typed_tools = [_remote_contract_tool(tool) for tool in tools]
        observed_digest = remote_mcp_schema_digest(typed_tools)
    except (TypeError, ValueError, ValidationError) as exc:
        contract_error = str(exc)
    passed = (
        document is not None
        and document.get("returncode") == 0
        and set(by_name) == expected
        and operation is not None
        and operation.get("enum") == ["edit", "remove"]
        and spack_specs is not None
        and handle_first_run
        and query_passed
        and package_search_passed
        and observed_digest == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    )
    return (
        {
            "remote_tool_names": sorted(by_name),
            "expected_tool_names": sorted(expected),
            "edit_operation_schema": operation or {},
            "spack_specs_schema": spack_specs or {},
            "jarvis_run_input_fields": sorted(run_properties) if run_properties else [],
            "handle_first_run": handle_first_run,
            "internal_wait_exposed": bool(run_properties and "wait" in run_properties),
            "package_search": package_search_evidence,
            "execution_query": query_evidence,
            "expected_contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "expected_clio_kit_version": CLIO_KIT_JARVIS_MCP_VERSION,
            "observed_contract_sha256": observed_digest,
            "contract_error": contract_error,
        },
        passed,
    )


def _package_search_contract_evidence(tool: JSON | None) -> tuple[JSON, bool]:
    """Summarize the bounded package-discovery surface from live tools/list."""
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
    target = _mapping(properties.get("target")) if properties else None
    query_selector = _mapping(properties.get("query")) if properties else None
    query = _schema_option(query_selector, expected_type="string")
    page_size = _mapping(properties.get("page_size")) if properties else None
    cursor_selector = _mapping(properties.get("cursor")) if properties else None
    cursor = _schema_option(cursor_selector, expected_type="string")
    expected_fields = {
        "target",
        "package_name",
        "query",
        "page_size",
        "cursor",
        "pipeline_id",
        "step_id",
        "include_yaml",
    }
    target_values = (
        cast(list[object], target.get("enum"))
        if target is not None and isinstance(target.get("enum"), list)
        else []
    )
    passed = (
        input_schema is not None
        and input_schema.get("additionalProperties") is False
        and properties is not None
        and set(properties) == expected_fields
        and required == ["target"]
        and target_values == ["packages", "package_search", "package", "pipeline", "step"]
        and query == {"maxLength": 256, "minLength": 1, "type": "string"}
        and page_size is not None
        and page_size.get("default") == 10
        and page_size.get("minimum") == 1
        and page_size.get("maximum") == 25
        and page_size.get("type") == "integer"
        and cursor == {"maxLength": 1024, "minLength": 1, "type": "string"}
    )
    return (
        {
            "input_fields": sorted(properties) if properties is not None else [],
            "required": required if isinstance(required, list) else [],
            "target_values": target_values,
            "query_schema": query or {},
            "page_size_schema": page_size or {},
            "cursor_schema": cursor or {},
            "bounded": passed,
        },
        passed,
    )


def _execution_query_contract_evidence(tool: JSON | None) -> tuple[JSON, bool]:
    """Summarize the unified progress/artifact query without copying its full schema."""
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    input_properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
    include_progress = (
        _mapping(input_properties.get("include_progress")) if input_properties else None
    )
    include_service_runtimes = (
        _mapping(input_properties.get("include_service_runtimes")) if input_properties else None
    )
    artifact_selector = _mapping(input_properties.get("artifacts")) if input_properties else None
    artifact_query = _schema_option(artifact_selector, expected_type="object")
    artifact_filters = _mapping(artifact_query.get("properties")) if artifact_query else None
    page_size = _mapping(artifact_filters.get("page_size")) if artifact_filters else None
    output_schema = _mapping(tool.get("outputSchema")) if tool else None
    output_properties = _mapping(output_schema.get("properties")) if output_schema else None
    output_required = output_schema.get("required") if output_schema else None
    progress_selector = _mapping(output_properties.get("progress")) if output_properties else None
    progress = _schema_option(progress_selector, expected_type="object")
    progress_properties = _mapping(progress.get("properties")) if progress else None
    page_selector = _mapping(output_properties.get("artifact_page")) if output_properties else None
    artifact_page = _schema_option(page_selector, expected_type="object")
    artifact_page_properties = _mapping(artifact_page.get("properties")) if artifact_page else None
    artifacts_schema = (
        _mapping(artifact_page_properties.get("artifacts")) if artifact_page_properties else None
    )
    artifact_item = _mapping(artifacts_schema.get("items")) if artifacts_schema else None
    artifact_item_properties = _mapping(artifact_item.get("properties")) if artifact_item else None
    service_selector = (
        _mapping(output_properties.get("service_runtimes")) if output_properties else None
    )
    service_runtimes = _schema_option(service_selector, expected_type="object")
    service_runtime_properties = (
        _mapping(service_runtimes.get("properties")) if service_runtimes else None
    )
    expected_inputs = {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }
    expected_filters = {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
        "content_max_bytes",
    }
    expected_outputs = {
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
    passed = bool(
        input_schema is not None
        and input_schema.get("additionalProperties") is False
        and input_properties is not None
        and set(input_properties) == expected_inputs
        and isinstance(required, list)
        and set(cast(list[object], required)) == {"pipeline_id", "execution_id"}
        and include_progress == {"default": True, "type": "boolean"}
        and include_service_runtimes == {"default": False, "type": "boolean"}
        and artifact_selector is not None
        and artifact_selector.get("default") is None
        and artifact_query is not None
        and artifact_query.get("additionalProperties") is False
        and artifact_filters is not None
        and set(artifact_filters) == expected_filters
        and page_size
        == {
            "default": 50,
            "description": "Maximum artifacts to return in this page.",
            "maximum": 100,
            "minimum": 1,
            "type": "integer",
        }
        and output_schema is not None
        and output_schema.get("additionalProperties") is False
        and output_properties is not None
        and set(output_properties) == expected_outputs
        and isinstance(output_required, list)
        and set(cast(list[object], output_required)) == expected_outputs
        and output_properties.get("schema_version")
        == {"const": CLIO_KIT_JARVIS_EXECUTION_SCHEMA, "type": "string"}
        and progress_properties is not None
        and progress_properties.get("schema_version")
        == {"const": "jarvis.execution.progress.v1", "type": "string"}
        and artifact_page_properties is not None
        and artifact_page_properties.get("producer_schema_version")
        == {"const": "jarvis.execution.artifacts.v1", "type": "string"}
        and artifact_item_properties is not None
        and artifact_item_properties.get("schema_version")
        == {"const": "jarvis.artifact.v1", "type": "string"}
        and service_runtime_properties is not None
        and service_runtime_properties.get("schema_version")
        == {"const": JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA, "type": "string"}
    )
    return (
        {
            "input_fields": sorted(input_properties) if input_properties else [],
            "required_identity_fields": sorted(str(item) for item in cast(list[object], required))
            if isinstance(required, list)
            else [],
            "include_progress_schema": include_progress or {},
            "include_service_runtimes_schema": include_service_runtimes or {},
            "artifact_filter_fields": sorted(artifact_filters) if artifact_filters else [],
            "artifact_page_size_schema": page_size or {},
            "output_fields": sorted(output_properties) if output_properties else [],
            "progress_schema_version": (
                progress_properties.get("schema_version") if progress_properties else None
            ),
            "artifact_page_schema_version": (
                artifact_page_properties.get("producer_schema_version")
                if artifact_page_properties
                else None
            ),
            "artifact_schema_version": (
                artifact_item_properties.get("schema_version") if artifact_item_properties else None
            ),
            "service_runtimes_schema_version": (
                service_runtime_properties.get("schema_version")
                if service_runtime_properties
                else None
            ),
        },
        passed,
    )


def _schema_option(schema: JSON | None, *, expected_type: str) -> JSON | None:
    """Return one non-null branch from a nullable JSON Schema property."""
    raw_options = schema.get("anyOf") if schema else None
    if not isinstance(raw_options, list):
        return None
    options = [
        cast(JSON, item) for item in cast(list[object], raw_options) if isinstance(item, dict)
    ]
    non_null = [item for item in options if item.get("type") == expected_type]
    nulls = [item for item in options if item == {"type": "null"}]
    if len(options) != 2 or len(non_null) != 1 or len(nulls) != 1:
        return None
    return non_null[0]


def _remote_contract_tool(tool: JSON) -> RemoteMcpToolSchema:
    """Parse one live JARVIS ``tools/list`` entry for the contract digest check.

    clio-relay#164: title resolution goes through the shared
    :func:`resolve_remote_tool_title` helper, the same one
    ``remote_mcp_tool_schema._parse_remote_tool`` uses for the discovery ->
    schema-cache -> catalog path. Both are compared against the same pinned
    ``CLIO_KIT_JARVIS_USER_CONTRACT_SHA256`` digest for the same live
    server (see ``_remote_jarvis_contract`` below and
    ``remote_mcp_contract_checks._jarvis_user_contract_check``); an
    un-shared title resolution would make the two paths silently disagree
    about that server's schema digest.
    """
    name = tool.get("name")
    input_schema = _mapping(tool.get("inputSchema"))
    if not isinstance(name, str) or input_schema is None:
        raise ValueError("remote JARVIS MCP returned an invalid tool contract")
    title = _optional_contract_string(tool, "title")
    description = _optional_contract_string(tool, "description")
    output_schema = _optional_contract_mapping(tool, "outputSchema")
    annotations = _optional_contract_mapping(tool, "annotations")
    return RemoteMcpToolSchema(
        name=name,
        title=resolve_remote_tool_title(title, annotations),
        description=description,
        input_schema=input_schema,
        output_schema=output_schema,
        annotations=annotations,
    )


def _optional_contract_string(tool: JSON, key: str) -> str | None:
    value = tool.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"remote JARVIS MCP tool {key} must be a string")
    return value


def _optional_contract_mapping(tool: JSON, key: str) -> JSON | None:
    value = tool.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"remote JARVIS MCP tool {key} must be an object")
    return cast(JSON, value)


def _local_jarvis_contract(tool: JSON | None, name: str) -> tuple[JSON, bool]:
    expected = jarvis_user_contract().get(name)
    if tool is None or expected is None:
        return ({"tool": name, "error": "tool is not part of the pinned contract"}, False)
    actual_input = _mapping(tool.get("inputSchema"))
    actual_output = _mapping(tool.get("outputSchema"))
    actual_annotations = _mapping(tool.get("annotations"))
    if actual_input is None:
        return ({"tool": name, "error": "tool has no input schema"}, False)

    remote_input = deepcopy(actual_input)
    properties = _mapping(remote_input.get("properties"))
    if properties is None:
        return ({"tool": name, "error": "tool has no property map"}, False)
    cluster_property = _mapping(properties.get("cluster"))
    cluster_values = cluster_property.get("enum") if cluster_property is not None else None
    clusters = cluster_values if _is_string_list(cluster_values) else None
    for key in (
        "cluster",
        "timeout_seconds",
        "idempotency_key",
        "wait_for_terminal",
        "wait_timeout_seconds",
        "poll_seconds",
    ):
        properties.pop(key, None)
    required = remote_input.get("required")
    if isinstance(required, list):
        remote_input["required"] = [
            item for item in cast(list[object], required) if item != "cluster"
        ]

    expected_description = expected.get("description")
    actual_description = tool.get("description")
    input_matches = remote_input == expected.get("inputSchema")
    annotations_match = actual_annotations == expected.get("annotations")
    output_matches = actual_output == virtual_jarvis_job_output_schema(name, clusters=clusters)
    description_matches = (
        isinstance(expected_description, str)
        and isinstance(actual_description, str)
        and actual_description.startswith(expected_description)
    )
    return (
        {
            "tool": name,
            "input_matches_pinned_contract": input_matches,
            "annotations_match_pinned_contract": annotations_match,
            "async_output_contract": output_matches,
            "description_derived_from_pinned_contract": description_matches,
        },
        input_matches and annotations_match and output_matches and description_matches,
    )
