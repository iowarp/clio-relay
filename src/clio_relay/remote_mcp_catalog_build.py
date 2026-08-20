"""Virtual remote MCP catalog assembly: fresh, allowlisted schemas to aliases.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns
:func:`build_virtual_remote_mcp_catalog`, the deterministic catalog-assembly
pass that walks every enabled, profile-allowed registration, withholds a
tool for any staleness/verification/declared-contract/allowlist reason
(recording a typed :class:`~clio_relay.remote_mcp_acceptance_models.RemoteMcpCatalogIssue`,
including the dev-mode "deferred, never silent" enforcement path), and
assigns collision-free deterministic aliases to the survivors -- plus
:func:`load_virtual_remote_mcp_catalog`, the explicit-reload wrapper that
reads the current cluster registry and schema cache from disk on every call.

Both functions are re-exported under their original names (``mcp_server.py``
and tests import them directly from ``clio_relay.remote_mcp``).

``build_virtual_remote_mcp_catalog`` reads ``MAX_REMOTE_MCP_CATALOG_ISSUES``,
a bound that still lives in ``remote_mcp.py`` (unsequenced, post-campaign per
the design doc), for the load-order reason every such constant in this split
is read via a function-scope import (see ``remote_mcp_wire_schemas.py``'s own
``virtual_jarvis_job_output_schema``). It also reads
``MAX_VIRTUAL_REMOTE_MCP_CANDIDATES`` -- owned by ``remote_mcp_aliasing.py``,
not ``remote_mcp.py`` -- the same way rather than via a module-scope import
from its owner: the test suite's
``monkeypatch.setattr(remote_mcp, "MAX_VIRTUAL_REMOTE_MCP_CANDIDATES", 1)``
patches the attribute on the *facade* module object (this function lived in
``remote_mcp.py`` itself before the move, where a bare-name lookup resolved
through that exact namespace); a module-scope `from ... import` here would
bind this module's own name once at import time and never observe that
patch, silently breaking the test the same way a bare re-binding broke
``cli_support.py``'s re-exports before its R8(ii) fix -- so this constant
too is re-read from ``remote_mcp`` on every call.

The function also logs at WARNING through ``logging.getLogger("clio_relay.remote_mcp")``
-- the exact logger name ``remote_mcp.py``'s own module scope binds via
``logging.getLogger(__name__)`` -- rather than this module's own ``__name__``,
so ``caplog.at_level(..., logger=remote_mcp.__name__)`` in the test suite
keeps observing these records after the move (``logging.getLogger`` returns
the same process-wide singleton for a given name regardless of which module
calls it).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from clio_relay.cluster_config import ClusterRegistry, cluster_route_revision, default_registry_path
from clio_relay.models import McpControlQueryEvidence
from clio_relay.remote_mcp_acceptance_models import RemoteMcpCatalogIssue
from clio_relay.remote_mcp_aliasing import _assign_aliases, _profile_allows, _safe_name
from clio_relay.remote_mcp_cache import (
    RemoteMcpSchemaCache,
    default_remote_mcp_cache_path,
    remote_mcp_execution_fingerprint,
    remote_mcp_registration_revision,
    remote_mcp_server_artifact_digest,
)
from clio_relay.remote_mcp_catalog_models import (
    RemoteMcpRoute,
    VirtualRemoteMcpCatalog,
    VirtualRemoteMcpTool,
    _Candidate,
)
from clio_relay.remote_mcp_contract_checks import _declared_contract_check
from clio_relay.remote_mcp_schema_wrapping import (
    remote_input_schema_requires_wrapper,
    virtual_schema_error,
)
from clio_relay.remote_mcp_tool_schema import _server_artifact_verified, _stable_digest

# Preserved by name, not derived from this module's own __name__: see the
# module docstring above (caplog-by-name test contract).
logger = logging.getLogger("clio_relay.remote_mcp")


def build_virtual_remote_mcp_catalog(
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    *,
    profile: str,
    reserved_names: set[str] | None = None,
    now: datetime | None = None,
) -> VirtualRemoteMcpCatalog:
    """Build deterministic aliases from fresh, allowlisted remote schemas."""
    from clio_relay.remote_mcp import (
        MAX_REMOTE_MCP_CATALOG_ISSUES,
        MAX_VIRTUAL_REMOTE_MCP_CANDIDATES,
    )

    current = now or datetime.now(UTC)
    candidates: list[_Candidate] = []
    issues: list[RemoteMcpCatalogIssue] = []
    issues_capped = False

    def record_issue(issue: RemoteMcpCatalogIssue) -> None:
        nonlocal issues_capped
        if issues_capped:
            return
        if len(issues) < MAX_REMOTE_MCP_CATALOG_ISSUES - 1:
            issues.append(issue)
            return
        issues.append(
            RemoteMcpCatalogIssue(
                cluster="*",
                server_name="*",
                reason=(
                    "remote MCP catalog diagnostics reached the "
                    f"{MAX_REMOTE_MCP_CATALOG_ISSUES} issue limit"
                ),
            )
        )
        issues_capped = True

    candidate_limit_reached = False
    for cluster_name, cluster in sorted(registry.clusters.items()):
        if candidate_limit_reached:
            break
        for server_name, registration in sorted(cluster.remote_mcp_servers.items()):
            if candidate_limit_reached:
                break
            if not registration.enabled:
                continue
            if not _profile_allows(registration.profiles, profile):
                continue
            entry = cache.entry_for(cluster_name, server_name)
            if entry is None:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason="schema cache is missing; run remote-mcp refresh",
                    )
                )
                continue
            if entry.execution_fingerprint != remote_mcp_execution_fingerprint(registration):
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason="registered command changed; run remote-mcp refresh",
                    )
                )
                continue
            effective_expires_at = entry.discovered_at + timedelta(
                seconds=registration.schema_cache_ttl_seconds
            )
            if current >= effective_expires_at:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason=(
                            "schema cache expired at "
                            f"{effective_expires_at.astimezone(UTC).isoformat()}"
                        ),
                    )
                )
                continue
            server_artifact_verified = _server_artifact_verified(entry.provenance.server_artifact)
            artifact_verification_deferred = False
            if server_artifact_verified is False:
                from clio_relay.dev_mode import dev_mode_enabled

                if dev_mode_enabled():
                    server_artifact_verified = True
                    artifact_verification_deferred = True
            if not server_artifact_verified and not registration.allow_mutable_artifact:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason=(
                            "discovery server artifact identity is unverified; refresh from an "
                            "immutable executable or exact artifact"
                        ),
                    )
                )
                continue
            if artifact_verification_deferred:
                # clio-relay#242 course correction: dev mode is LOUD AND
                # NON-BLOCKING -- the boolean flip above must never be
                # silent. Record the same reason a withheld registration
                # would carry, tagged as deferred (this tool IS served),
                # and log it at WARNING so a security-phase retest can find
                # it after the fact.
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason="discovery server artifact identity is unverified",
                        enforcement="deferred_dev_mode",
                    )
                )
                logger.warning(
                    "clio-relay: DEV MODE enforcement=deferred_dev_mode cluster=%s server=%s "
                    "reason=%s",
                    cluster_name,
                    server_name,
                    "discovery server artifact identity is unverified",
                )
            if registration.contract is not None:
                contract_check = _declared_contract_check(entry, registration)
                if not contract_check.passed:
                    from clio_relay.dev_mode import dev_mode_enabled

                    contract_deferred = dev_mode_enabled()
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            reason=(
                                f"declared contract {registration.contract!r} failed: "
                                f"{contract_check.message}"
                            ),
                            enforcement=("deferred_dev_mode" if contract_deferred else "enforced"),
                        )
                    )
                    if not contract_deferred:
                        continue
                    # Owner ruling (clio-relay#242 course correction): an
                    # agent must be able to deploy and run WITH jarvis under
                    # no security enforcement of sha/version/contract in dev
                    # mode -- this is the exact "jarvis withheld from the
                    # catalog" failure the ares live run hit. PROCEED past
                    # the declared-contract refusal instead of withholding
                    # the whole registration; the per-tool allowlist/schema
                    # checks below still gate exactly what gets served.
                    logger.warning(
                        "clio-relay: DEV MODE enforcement=deferred_dev_mode cluster=%s server=%s "
                        "declared_contract=%r reason=%s",
                        cluster_name,
                        server_name,
                        registration.contract,
                        contract_check.message,
                    )
                drift_notice = contract_check.evidence.get("contract_drift_notice")
                if isinstance(drift_notice, str) and drift_notice:
                    # Non-fatal: the declared contract still passed (its exact
                    # tool subset is served unchanged), but the live server has
                    # moved to a newer audited contract. Record the typed
                    # notice and keep going -- this registration must NOT be
                    # silently dropped, and must not be silently "upgraded"
                    # either (allow_tools still gates on the declared subset).
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            reason=drift_notice,
                        )
                    )
            discovered_names = {tool.name for tool in entry.tools}
            for allowed_tool in registration.allow_tools:
                if allowed_tool != "*" and allowed_tool not in discovered_names:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=allowed_tool,
                            reason="allowlisted tool was not returned by remote tools/list",
                        )
                    )
            for tool in entry.tools:
                if not registration.allows_tool(tool.name):
                    continue
                schema_error = virtual_schema_error(tool.input_schema)
                if schema_error is not None:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=tool.name,
                            reason=schema_error,
                        )
                    )
                    continue
                namespace = (registration.namespace or server_name).casefold()
                base_alias = f"remote_{_safe_name(namespace)}_{_safe_name(tool.name)}"
                if len(candidates) >= MAX_VIRTUAL_REMOTE_MCP_CANDIDATES:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=tool.name,
                            reason=(
                                "virtual remote MCP catalog exceeds the "
                                f"{MAX_VIRTUAL_REMOTE_MCP_CANDIDATES} candidate limit"
                            ),
                        )
                    )
                    candidate_limit_reached = True
                    break
                identity = _stable_digest(
                    {
                        "namespace": namespace,
                        "remote_tool": tool.name,
                        "schema": tool.model_dump(mode="json"),
                        "contract": registration.contract,
                    }
                )
                candidates.append(
                    _Candidate(
                        cluster=cluster_name,
                        server_name=server_name,
                        namespace=namespace,
                        registration=registration,
                        tool=tool,
                        base_alias=base_alias,
                        identity=identity,
                        expected_server_artifact_digest=(
                            remote_mcp_server_artifact_digest(entry.provenance.server_artifact)
                            if not registration.allow_mutable_artifact
                            else None
                        ),
                        discovery_provenance=entry.provenance,
                        discovery_schema_digest=entry.schema_digest,
                    )
                )

    route_revisions = {
        cluster: cluster_route_revision(definition)
        for cluster, definition in sorted(registry.clusters.items())
    }
    grouped: dict[str, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.identity, []).append(candidate)
    unambiguous_groups: dict[str, list[_Candidate]] = {}
    for identity, group in sorted(grouped.items()):
        cluster_counts: dict[str, int] = {}
        for candidate in group:
            cluster_counts[candidate.cluster] = cluster_counts.get(candidate.cluster, 0) + 1
        ambiguous_clusters = {cluster for cluster, count in cluster_counts.items() if count > 1}
        for candidate in group:
            if candidate.cluster in ambiguous_clusters:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=candidate.cluster,
                        server_name=candidate.server_name,
                        tool_name=candidate.tool.name,
                        reason=(
                            "multiple registrations provide the same namespace, tool, schema, "
                            "and contract on this cluster; the route is ambiguous"
                        ),
                    )
                )
        remaining = [
            candidate for candidate in group if candidate.cluster not in ambiguous_clusters
        ]
        if remaining:
            unambiguous_groups[identity] = remaining
    grouped = unambiguous_groups
    aliases = _assign_aliases(grouped, reserved_names=reserved_names or set())
    virtual_tools: dict[str, VirtualRemoteMcpTool] = {}
    for identity, group in sorted(grouped.items()):
        alias = aliases[identity]
        first = group[0]
        routes: dict[str, RemoteMcpRoute] = {}
        for candidate in group:
            registration_revision = remote_mcp_registration_revision(candidate.registration)
            expected_digest = candidate.expected_server_artifact_digest
            evidence = (
                McpControlQueryEvidence(
                    cluster=candidate.cluster,
                    registered_server_name=candidate.server_name,
                    cluster_route_revision=route_revisions[candidate.cluster],
                    registration_revision=registration_revision,
                    discovery_job_id=candidate.discovery_provenance.discovery_job_id,
                    discovery_artifact_id=candidate.discovery_provenance.artifact_id,
                    discovery_artifact_sha256=(candidate.discovery_provenance.artifact_sha256),
                    discovery_schema_digest=candidate.discovery_schema_digest,
                    expected_server_artifact_digest=expected_digest,
                )
                if expected_digest is not None
                else None
            )
            routes[candidate.cluster] = RemoteMcpRoute(
                cluster=candidate.cluster,
                server_name=candidate.server_name,
                command=candidate.registration.command,
                args=tuple(candidate.registration.args),
                env_from=tuple(sorted(candidate.registration.env_from.items())),
                expected_server_artifact_digest=expected_digest,
                remote_tool_name=candidate.tool.name,
                timeout_seconds=candidate.registration.call_timeout_seconds,
                contract=candidate.registration.contract,
                cluster_route_revision=route_revisions[candidate.cluster],
                registration_revision=registration_revision,
                control_query_evidence=evidence,
            )
        virtual_tools[alias] = VirtualRemoteMcpTool(
            alias=alias,
            namespace=first.namespace,
            remote_tool=first.tool,
            routes=routes,
            arguments_wrapped=remote_input_schema_requires_wrapper(first.tool.input_schema),
        )
    revision = _stable_digest(
        {
            "profile": profile,
            "cluster_routes": route_revisions,
            "tools": {
                alias: {
                    "namespace": tool.namespace,
                    "remote_tool": tool.remote_tool.model_dump(mode="json"),
                    "arguments_wrapped": tool.arguments_wrapped,
                    "routes": {
                        cluster: {
                            "server_name": route.server_name,
                            "registration_revision": route.registration_revision,
                            "expected_server_artifact_digest": (
                                route.expected_server_artifact_digest
                            ),
                            "control_query_evidence": (
                                None
                                if route.control_query_evidence is None
                                else route.control_query_evidence.model_dump(mode="json")
                            ),
                        }
                        for cluster, route in sorted(tool.routes.items())
                    },
                }
                for alias, tool in sorted(virtual_tools.items())
            },
            "issues": [issue.model_dump(mode="json") for issue in issues],
        }
    )
    return VirtualRemoteMcpCatalog(
        revision=revision,
        tools=virtual_tools,
        issues=tuple(issues),
        cluster_route_revisions=route_revisions,
    )


def load_virtual_remote_mcp_catalog(
    *,
    profile: str,
    reserved_names: set[str] | None = None,
    registry_path: Path | None = None,
    cache_path: Path | None = None,
    now: datetime | None = None,
) -> VirtualRemoteMcpCatalog:
    """Load current config and cache on every call for explicit reload semantics."""
    resolved_registry_path = registry_path or default_registry_path()
    if not resolved_registry_path.exists():
        registry = ClusterRegistry.default()
    else:
        registry = ClusterRegistry.load(resolved_registry_path)
    resolved_cache_path = cache_path or default_remote_mcp_cache_path(
        registry_path=resolved_registry_path
    )
    cache = RemoteMcpSchemaCache.load(resolved_cache_path)
    return build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile=profile,
        reserved_names=reserved_names,
        now=now,
    )
