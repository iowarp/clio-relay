"""Bounded JARVIS package-discovery (``jarvis_describe``) call evidence.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): validates one durable ``jarvis_describe`` call against
the local tool surface/contract, the request's page-size and query bounds,
the verified server-artifact binding, and the returned summary-only,
size-bounded package-search result page. Called by
``build_jarvis_mcp_validation_report`` in ``jarvis_mcp_validation_report.py``.
"""

from __future__ import annotations

import json
from typing import cast

from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_verified,
)
from clio_relay.jarvis_mcp_validation_contract import _local_jarvis_contract
from clio_relay.jarvis_mcp_validation_core import (
    JSON,
    _is_sha256,
    _listed_tool,
    _mapping,
    _positive_int,
    _response_job_id,
    _stdio_initialize_passed,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest


def _jarvis_package_search_evidence(
    *,
    cluster: str,
    query: str,
    expected_server_artifact: JSON | None,
    tools_list_response: JSON | None,
    call_response: JSON | None,
    call_job_id: str,
    call_status: JSON,
    artifacts: list[JSON],
    mcp_result: JSON | None,
    provenance: JSON | None,
    initialize_response: JSON | None,
    stdio_evidence: JSON | None,
) -> tuple[JSON, bool]:
    """Validate one durable, bounded package-search result from JARVIS."""
    tool = _listed_tool(tools_list_response, "jarvis_describe")
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = cast(object, input_schema.get("required")) if input_schema else None
    required_fields: set[str] = (
        {item for item in cast(list[object], required) if isinstance(item, str)}
        if isinstance(required, list)
        else set[str]()
    )
    local_contract, local_contract_passed = _local_jarvis_contract(
        tool,
        "jarvis_describe",
    )
    stdio_passed = _stdio_initialize_passed(
        initialize_response=initialize_response,
        evidence=stdio_evidence,
    )
    local_surface_passed = bool(
        properties is not None
        and isinstance(properties.get("cluster"), dict)
        and {"cluster", "target"}.issubset(required_fields)
        and local_contract_passed
        and stdio_passed
    )

    job = _mapping(call_status.get("job")) or {}
    spec = _mapping(job.get("spec")) or {}
    arguments = _mapping(spec.get("arguments")) or {}
    page_size = cast(object, arguments.get("page_size"))
    page_size_value: int | None = page_size if _positive_int(page_size) else None
    request_bounded = bool(
        bool(query)
        and len(query) <= 256
        and set(arguments) == {"target", "query", "page_size"}
        and arguments.get("target") == "package_search"
        and arguments.get("query") == query
        and page_size_value is not None
        and page_size_value <= 25
    )
    response_job_id = _response_job_id(call_response)
    durable_artifacts = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifacts = {"stdout", "stderr", "mcp_result", "provenance"}
    provenance_job = _mapping(provenance.get("job")) if provenance else None
    job_passed = bool(
        response_job_id == call_job_id
        and job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == "jarvis_describe"
        and request_bounded
        and required_artifacts.issubset(durable_artifacts)
        and provenance_job is not None
        and provenance_job.get("job_id") == call_job_id
    )

    expected_server_artifact_digest = (
        remote_mcp_server_artifact_digest(expected_server_artifact)
        if expected_server_artifact is not None
        else None
    )
    result_server_artifact = _mapping(mcp_result.get("server_artifact")) if mcp_result else None
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()
    server_binding_passed = bool(
        _is_sha256(expected_server_artifact_digest)
        and jarvis_mcp_server_artifact_verified(expected_server_artifact)
        and spec.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and spec.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result is not None
        and mcp_result.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and mcp_result.get("observed_server_artifact_digest") == expected_server_artifact_digest
        and result_server_artifact == expected_server_artifact
    )

    structured = _mapping(mcp_result.get("structured_result")) if mcp_result else None
    raw_packages = structured.get("packages") if structured else None
    packages = cast(list[object], raw_packages) if isinstance(raw_packages, list) else []
    summaries_valid = bool(packages) and all(
        _valid_package_search_summary(package) for package in packages
    )
    total_matches = cast(object, structured.get("total_matches")) if structured else None
    returned_count = cast(object, structured.get("returned_count")) if structured else None
    total_matches_value: int | None = total_matches if _positive_int(total_matches) else None
    returned_count_value: int | None = returned_count if _positive_int(returned_count) else None
    next_cursor = cast(object, structured.get("next_cursor")) if structured else None
    encoded_bytes = (
        len(
            json.dumps(
                structured,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        if structured is not None
        else None
    )
    result_bounded = bool(
        structured is not None
        and set(structured)
        == {
            "schema_version",
            "target",
            "query",
            "inventory_revision",
            "packages",
            "total_matches",
            "returned_count",
            "next_cursor",
        }
        and structured.get("schema_version") == "jarvis.package-search.v1"
        and structured.get("target") == "package_search"
        and structured.get("query") == query
        and _is_sha256(structured.get("inventory_revision"))
        and summaries_valid
        and returned_count_value is not None
        and total_matches_value is not None
        and page_size_value is not None
        and returned_count_value == len(packages)
        and returned_count_value <= page_size_value
        and total_matches_value >= returned_count_value
        and (
            next_cursor is None or (isinstance(next_cursor, str) and 1 <= len(next_cursor) <= 1024)
        )
        and encoded_bytes is not None
        and encoded_bytes <= 64 * 1024
    )
    protocol_passed = bool(
        mcp_result is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("tool") == "jarvis_describe"
        and mcp_result.get("protocol_error") is None
    )
    assertions = {
        "local_surface": local_surface_passed,
        "durable_call": job_passed,
        "server_artifact_binding": server_binding_passed,
        "protocol_result": protocol_passed,
        "bounded_summary_page": result_bounded,
    }
    return (
        {
            "query": query,
            "page_size": page_size_value,
            "response_job_id": response_job_id,
            "job_id": job.get("job_id"),
            "artifact_kinds": sorted(durable_artifacts),
            "required_artifact_kinds": sorted(required_artifacts),
            "expected_server_artifact_digest": expected_server_artifact_digest,
            "expected_jarvis_cd_lock_binding": expected_jarvis_cd_lock_binding,
            "spec_jarvis_cd_lock_binding": spec.get("expected_jarvis_cd_lock_binding"),
            "result_jarvis_cd_lock_binding": (
                mcp_result.get("expected_jarvis_cd_lock_binding") if mcp_result else None
            ),
            "returned_count": returned_count_value,
            "total_matches": total_matches_value,
            "next_cursor_present": isinstance(next_cursor, str),
            "serialized_result_bytes": encoded_bytes,
            "result": structured or {},
            "local_contract": local_contract,
            "packaged_stdio": stdio_evidence or {},
            "assertions": assertions,
        },
        all(assertions.values()),
    )


def _valid_package_search_summary(value: object) -> bool:
    """Return whether one package-search item is summary-only and bounded."""
    summary = _mapping(value)
    if summary is None:
        return False
    if not {"name", "short_name", "repository"}.issubset(summary):
        return False
    if not set(summary).issubset({"name", "short_name", "repository", "description"}):
        return False
    for key in ("name", "short_name", "repository"):
        item = summary.get(key)
        if not isinstance(item, str) or not item:
            return False
    description = summary.get("description")
    return description is None or (
        isinstance(description, str) and len(description.encode("utf-8")) <= 4096
    )
