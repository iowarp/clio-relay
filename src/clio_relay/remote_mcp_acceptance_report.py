"""Canonical remote-MCP release-acceptance report builder.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns
:func:`build_remote_mcp_acceptance_report`, the top-level orchestrator that
assembles one durable job's release evidence into the ordered acceptance
checks other clio-relay owner modules depend on by name (registration,
discovery, tools-list, call, server-artifact, durable-result, plus the
optional structured-result and scientific-catalog checks it dispatches to).
It composes the catalog-assembly, declared-contract, stdio-evidence, and
structured-result owner modules extracted alongside it, rather than
duplicating any of their logic.

Re-exported under its original name (``cli_remote_mcp_validate.py`` and
tests import it directly from ``clio_relay.remote_mcp``;
``remote_mcp_validation.py`` calls it as
``remote_mcp.build_remote_mcp_acceptance_report`` -- a module-qualified
lookup that resolves through this same re-export).

This function reads ``CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID``, a
contract-pin constant that still lives in ``remote_mcp.py`` (unsequenced,
post-campaign per the design doc). A module-scope import back into
``remote_mcp.py`` (which imports this module for the re-export above) would
be a load-order circular import; importing it inside the function body
instead is the proven idiom for that shape (see ``remote_mcp_wire_schemas.py``'s
own ``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from clio_relay.cluster_config import ClusterRegistry, cluster_route_revision
from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpAcceptanceCheck,
    RemoteMcpAcceptanceReport,
    RemoteMcpStructuredResultExpectation,
)
from clio_relay.remote_mcp_aliasing import _profile_allows
from clio_relay.remote_mcp_cache import (
    RemoteMcpSchemaCache,
    remote_mcp_execution_fingerprint,
    remote_mcp_registration_revision,
    remote_mcp_server_artifact_digest,
)
from clio_relay.remote_mcp_catalog_build import build_virtual_remote_mcp_catalog
from clio_relay.remote_mcp_contract_checks import _declared_contract_check
from clio_relay.remote_mcp_scientific_catalog_result import (
    _scientific_catalog_structured_result_check,
)
from clio_relay.remote_mcp_stdio_evidence import (
    _stdio_call_job_id,
    _stdio_initialize_passed,
    _stdio_listed_tool_names,
)
from clio_relay.remote_mcp_structured_result import build_remote_mcp_structured_result_check
from clio_relay.remote_mcp_tool_schema import (
    REMOTE_MCP_CACHE_SOURCE,
    _immutable_remote_mcp_install_verified,
    _is_sha256,
)

JSON = dict[str, Any]


def build_remote_mcp_acceptance_report(
    *,
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    cluster: str,
    server_name: str,
    remote_tool_name: str,
    profile: str,
    call_job_id: str,
    call_status: JSON,
    artifacts: list[JSON],
    mcp_result: JSON | None,
    provenance: JSON | None,
    result_expectation: RemoteMcpStructuredResultExpectation | None = None,
    mcp_stdio_evidence: JSON | None = None,
    now: datetime | None = None,
    reserved_names: set[str] | None = None,
) -> RemoteMcpAcceptanceReport:
    """Build canonical release checks from live durable job evidence."""
    from clio_relay.remote_mcp import CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID

    current = now or datetime.now(UTC)
    definition = registry.clusters.get(cluster)
    registration = (
        definition.remote_mcp_servers.get(server_name) if definition is not None else None
    )
    registration_passed = (
        registration is not None
        and registration.enabled
        and registration.allows_tool(remote_tool_name)
        and _profile_allows(registration.profiles, profile)
    )
    registration_evidence: JSON = {
        "cluster_configured": definition is not None,
        "server_registered": registration is not None,
        "enabled": registration.enabled if registration is not None else False,
        "tool_allowlisted": (
            registration.allows_tool(remote_tool_name) if registration is not None else False
        ),
        "profile_allowed": (
            _profile_allows(registration.profiles, profile) if registration is not None else False
        ),
        "declared_contract": registration.contract if registration is not None else None,
        "registration_revision": (
            remote_mcp_registration_revision(registration) if registration is not None else None
        ),
        "cluster_route_revision": (
            cluster_route_revision(definition) if definition is not None else None
        ),
    }
    checks = [
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.register",
            passed=registration_passed,
            message=(
                "registered server, allowlist, and profile are active"
                if registration_passed
                else "registered server, allowlist, or profile gate is not active"
            ),
            evidence=registration_evidence,
        )
    ]

    entry = cache.entry_for(cluster, server_name)
    effective_expires_at = (
        entry.discovered_at + timedelta(seconds=registration.schema_cache_ttl_seconds)
        if entry is not None and registration is not None
        else None
    )
    discovery_passed = (
        entry is not None
        and registration is not None
        and entry.execution_fingerprint == remote_mcp_execution_fingerprint(registration)
        and effective_expires_at is not None
        and current < effective_expires_at
        and entry.provenance.source == REMOTE_MCP_CACHE_SOURCE
        and bool(entry.provenance.discovery_job_id)
        and bool(entry.provenance.artifact_id)
    )
    discovery_evidence: JSON = (
        {
            "schema_digest": entry.schema_digest,
            "discovered_at": entry.discovered_at.isoformat(),
            "effective_expires_at": (
                effective_expires_at.isoformat() if effective_expires_at is not None else None
            ),
            "execution_fingerprint": entry.execution_fingerprint,
            "provenance": entry.provenance.model_dump(mode="json"),
            "remote_tool_names": sorted(tool.name for tool in entry.tools),
            "allowlisted_tool_names": (
                sorted(registration.allow_tools) if registration is not None else []
            ),
        }
        if entry is not None
        else {}
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.discover",
            passed=discovery_passed,
            message=(
                "fresh schema is backed by a durable discovery job and artifact"
                if discovery_passed
                else "fresh durable discovery evidence is missing or invalid"
            ),
            evidence=discovery_evidence,
        )
    )
    if registration is not None and registration.contract is not None:
        checks.append(_declared_contract_check(entry, registration))

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile=profile,
        reserved_names=reserved_names,
        now=current,
    )
    matching_aliases = [
        alias
        for alias, virtual in catalog.tools.items()
        if virtual.remote_tool.name == remote_tool_name
        and cluster in virtual.routes
        and virtual.routes[cluster].server_name == server_name
    ]
    virtual_alias = matching_aliases[0] if len(matching_aliases) == 1 else None
    selected_route = (
        catalog.tools[virtual_alias].routes.get(cluster) if virtual_alias is not None else None
    )
    stdio_initialize_passed = _stdio_initialize_passed(mcp_stdio_evidence)
    stdio_listed_tools = _stdio_listed_tool_names(mcp_stdio_evidence)
    stdio_tools_list_passed = mcp_stdio_evidence is None or (
        stdio_initialize_passed
        and virtual_alias is not None
        and virtual_alias in stdio_listed_tools
    )
    tools_list_passed = virtual_alias is not None and stdio_tools_list_passed
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.tools-list",
            passed=tools_list_passed,
            message=(
                "one deterministic virtual alias exposes the selected cluster"
                if tools_list_passed
                else "the refreshed schema did not produce exactly one eligible virtual alias"
            ),
            evidence={
                "catalog_revision": catalog.revision,
                "registration_revision": (
                    selected_route.registration_revision if selected_route is not None else None
                ),
                "cluster_route_revision": (
                    selected_route.cluster_route_revision if selected_route is not None else None
                ),
                "matching_aliases": sorted(matching_aliases),
                "catalog_issues": [issue.model_dump(mode="json") for issue in catalog.issues],
                "packaged_stdio": mcp_stdio_evidence or {},
            },
        )
    )

    raw_job = call_status.get("job")
    job = cast(JSON, raw_job) if isinstance(raw_job, dict) else {}
    raw_spec = job.get("spec")
    spec = cast(JSON, raw_spec) if isinstance(raw_spec, dict) else {}
    stdio_call_job_id = _stdio_call_job_id(mcp_stdio_evidence)
    stdio_call_passed = mcp_stdio_evidence is None or (
        stdio_initialize_passed and stdio_call_job_id == call_job_id
    )
    expected_server_artifact_digest = (
        selected_route.expected_server_artifact_digest if selected_route is not None else None
    )
    call_passed = (
        job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and registration is not None
        and spec.get("server") == registration.command
        and spec.get("server_args") == registration.args
        and spec.get("env_from", {}) == registration.env_from
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == remote_tool_name
        and _is_sha256(expected_server_artifact_digest)
        and spec.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and stdio_call_passed
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.call",
            passed=call_passed,
            message=(
                "virtual alias created the expected durable MCP call job"
                if call_passed
                else "durable call job does not match the selected virtual route"
            ),
            evidence={
                "job_id": job.get("job_id"),
                "cluster": job.get("cluster"),
                "kind": job.get("kind"),
                "spec": spec,
                "selected_route_server_artifact_digest": expected_server_artifact_digest,
                "stdio_call_job_id": stdio_call_job_id,
                "packaged_stdio": mcp_stdio_evidence or {},
            },
        )
    )

    call_server_artifact = (
        cast(JSON, mcp_result.get("server_artifact"))
        if mcp_result is not None and isinstance(mcp_result.get("server_artifact"), dict)
        else None
    )
    discovery_server_artifact = entry.provenance.server_artifact if entry is not None else None
    computed_server_artifact_digest = (
        remote_mcp_server_artifact_digest(call_server_artifact)
        if call_server_artifact is not None
        else None
    )
    server_artifact_passed = (
        call_server_artifact is not None
        and call_server_artifact.get("verified") is True
        and call_server_artifact.get("server_process_artifact_verified") is True
        and bool(call_server_artifact.get("executable"))
        and _immutable_remote_mcp_install_verified(call_server_artifact)
        and _is_sha256(call_server_artifact.get("install_artifact_sha256"))
        and call_server_artifact == discovery_server_artifact
        and computed_server_artifact_digest == expected_server_artifact_digest
        and mcp_result is not None
        and mcp_result.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and mcp_result.get("observed_server_artifact_digest") == expected_server_artifact_digest
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.server-artifact",
            passed=server_artifact_passed,
            message=(
                "discovery and call used the same verified MCP server artifact"
                if server_artifact_passed
                else "MCP server artifact identity is missing, mutable, or changed after discovery"
            ),
            evidence={
                "discovery_server_artifact": discovery_server_artifact or {},
                "call_server_artifact": call_server_artifact or {},
                "selected_route_server_artifact_digest": expected_server_artifact_digest,
                "computed_server_artifact_digest": computed_server_artifact_digest,
                "result_expected_server_artifact_digest": (
                    mcp_result.get("expected_server_artifact_digest")
                    if mcp_result is not None
                    else None
                ),
                "result_observed_server_artifact_digest": (
                    mcp_result.get("observed_server_artifact_digest")
                    if mcp_result is not None
                    else None
                ),
            },
        )
    )

    artifacts_by_kind = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifact_kinds = {"stdout", "stderr", "mcp_result", "provenance"}
    protocol_result = (
        cast(JSON, mcp_result.get("protocol_result"))
        if mcp_result is not None and isinstance(mcp_result.get("protocol_result"), dict)
        else None
    )
    mcp_result_matches = (
        mcp_result is not None
        and registration is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("server") == registration.command
        and mcp_result.get("server_args") == registration.args
        and mcp_result.get("env_from", {}) == registration.env_from
        and mcp_result.get("tool") == remote_tool_name
        and mcp_result.get("arguments", {}) == spec.get("arguments", {})
        and mcp_result.get("protocol_error") is None
        and protocol_result is not None
        and protocol_result.get("isError") is not True
    )
    provenance_job = provenance.get("job") if provenance is not None else None
    provenance_matches = (
        isinstance(provenance_job, dict) and cast(JSON, provenance_job).get("job_id") == call_job_id
    )
    durable_result_passed = (
        job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and required_artifact_kinds.issubset(artifacts_by_kind)
        and mcp_result_matches
        and provenance_matches
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.durable-result",
            passed=durable_result_passed,
            message=(
                "terminal call has logs, MCP result, and matching provenance artifacts"
                if durable_result_passed
                else "terminal state or required durable result provenance is incomplete"
            ),
            evidence={
                "state": job.get("state"),
                "terminal": call_status.get("terminal"),
                "artifact_kinds": sorted(artifacts_by_kind),
                "required_artifact_kinds": sorted(required_artifact_kinds),
                "mcp_result_matches": mcp_result_matches,
                "provenance_matches": provenance_matches,
            },
        )
    )
    if result_expectation is not None:
        matching_tools = (
            [tool for tool in entry.tools if tool.name == remote_tool_name]
            if entry is not None
            else []
        )
        output_schema = matching_tools[0].output_schema if len(matching_tools) == 1 else None
        checks.append(
            build_remote_mcp_structured_result_check(
                expectation=result_expectation,
                remote_tool_name=remote_tool_name,
                arguments=spec.get("arguments", {}),
                protocol_result=protocol_result,
                output_schema=output_schema,
            )
        )
    if (
        registration is not None
        and registration.contract == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID
        and remote_tool_name == "scientific_dataset_describe"
    ):
        matching_tools = (
            [tool for tool in entry.tools if tool.name == remote_tool_name]
            if entry is not None
            else []
        )
        output_schema = matching_tools[0].output_schema if len(matching_tools) == 1 else None
        checks.append(
            _scientific_catalog_structured_result_check(
                arguments=spec.get("arguments", {}),
                protocol_result=protocol_result,
                output_schema=output_schema,
            )
        )
    passed = all(check.passed for check in checks)
    return RemoteMcpAcceptanceReport(
        cluster=cluster,
        server_name=server_name,
        remote_tool_name=remote_tool_name,
        virtual_alias=virtual_alias,
        profile=profile,
        passed=passed,
        checks=checks,
        discovery=discovery_evidence,
        call_job=job,
        artifacts=artifacts,
        mcp_stdio=mcp_stdio_evidence or {},
    )
