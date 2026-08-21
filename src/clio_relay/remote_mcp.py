"""Registry-backed virtualization for remote MCP servers.

Remote discovery is deliberately separated from local MCP ``tools/list``.
Operators refresh schemas through durable relay jobs, and the local MCP server
only renders validated, fresh cache entries. This keeps agent discovery fast,
deterministic, and free of cluster-side execution side effects.
"""

from __future__ import annotations

import logging
from typing import Any

# Facade: every owner module extracted in #231 slices 8-19 (this pass),
# imported once here (module scope -- none of them import anything from
# THIS module at their own module scope, so there is no circular import) so
# its public names resolve as `clio_relay.remote_mcp.*` for every existing
# external importer and monkeypatch target. The qualified-assignment
# re-exports for names with no reader left in this file's own body are
# grouped by owner module further below, each with its own comment
# cross-referencing that module's docstring rather than repeating the
# rationale here.
import clio_relay.remote_mcp_acceptance_evidence as remote_mcp_acceptance_evidence
import clio_relay.remote_mcp_acceptance_report as remote_mcp_acceptance_report
import clio_relay.remote_mcp_admission as remote_mcp_admission
import clio_relay.remote_mcp_aliasing as remote_mcp_aliasing
import clio_relay.remote_mcp_catalog_build as remote_mcp_catalog_build
import clio_relay.remote_mcp_catalog_models as remote_mcp_catalog_models
import clio_relay.remote_mcp_contract_checks as remote_mcp_contract_checks
import clio_relay.remote_mcp_scientific_catalog_result as remote_mcp_scientific_catalog_result
import clio_relay.remote_mcp_spack_result_validation as remote_mcp_spack_result_validation
import clio_relay.remote_mcp_spack_transition_checks as remote_mcp_spack_transition_checks
import clio_relay.remote_mcp_spack_transition_report as remote_mcp_spack_transition_report
import clio_relay.remote_mcp_stdio_evidence as remote_mcp_stdio_evidence
import clio_relay.remote_mcp_structured_result as remote_mcp_structured_result

# Release-acceptance evidence wire models moved to
# remote_mcp_acceptance_models.py (#231; design doc §4.5/§5). Every model
# class this module still references directly is re-exported under its
# original name -- cli.py and remote_mcp.py's own validator functions
# (staying here; doc §4.5 flags that cluster as needing reordering, a
# separate slice) construct and consume them. Three of the four bound
# Spack-configuration constants (cli.py's own Spack fresh-install report
# command imports them, but nothing in remote_mcp.py's own body reads them
# any more) are re-exported below via qualified assignment rather than a
# `from ... import` -- ruff's unused-import check has no equivalent for a
# plain module-level assignment, so this is the re-export-only idiom
# (matching cli_support.py's forwarder pattern) instead of a `from` import
# ruff would keep stripping as dead. RemoteMcpSpackConfigurationComponentObservation
# is used only inside the moved model definitions themselves and has no
# external importer either, so it alone is not re-exported.
# The schema discovery cache (classes, digest/fingerprint helpers, and the
# durable-artifact parser) moved to remote_mcp_cache.py (#231; design doc
# §4.5/§5). Eight of these nine names have a real external importer (verified
# by grep across the whole tree), so they are imported via a plain `from ...
# import` with no unused-import risk -- that same import is the re-export
# cli.py, mcp_server.py, jarvis_mcp.py, and jarvis_mcp_validation.py rely on.
# remote_mcp_server_artifact_binding_verified has no reader left in this
# file's own body -- only endpoint_jarvis_recovery.py and
# jarvis_service_runtime_validation.py import it directly -- so it is
# re-exported via qualified assignment instead (ruff's unused-import check
# has no equivalent for a plain module-level assignment, unlike the `from
# ... import` it kept stripping as dead).
# Agent-facing JSON-Schema builders for remote MCP job receipts/handoffs
# moved to remote_mcp_wire_schemas.py (#231; design doc §4.5/§5). Both names
# below are re-exported under their original names -- mcp_server.py,
# jarvis_mcp.py, jarvis_mcp_validation.py, and tests import them directly
# from clio_relay.remote_mcp. cluster_route_revision_json_schema and
# jarvis_service_runtime_handoff_json_schema have no reader left in this
# file's own body, so they are re-exported via qualified assignment instead
# (ruff's unused-import check has no equivalent for a plain module-level
# assignment).
#
# #231 slices 8-19 (this pass; design doc §4.5/§5, the deferred "validator
# families" row): remote_mcp.py's remaining ~3,300-line body -- the virtual
# catalog data model, admission resolution, catalog assembly, the canonical
# acceptance-report builder, the fresh-install Spack transition report and
# its per-phase checks, the shared bounded-evidence primitives, the
# structured-result and scientific-catalog result checks, the per-operation
# Spack result validators, the declared semantic-contract checks, and the
# packaged stdio evidence extraction -- moved to twelve new owner modules
# (remote_mcp_catalog_models.py, remote_mcp_admission.py,
# remote_mcp_catalog_build.py, remote_mcp_acceptance_report.py,
# remote_mcp_spack_transition_report.py, remote_mcp_spack_transition_checks.py,
# remote_mcp_acceptance_evidence.py, remote_mcp_structured_result.py,
# remote_mcp_scientific_catalog_result.py, remote_mcp_spack_result_validation.py,
# remote_mcp_contract_checks.py, remote_mcp_stdio_evidence.py). Each owner
# module's own docstring documents its exact re-export/private-import split
# and, where it reads one of the CLIO_KIT_*/MAX_* constants still resident
# below, why that read is a function-scope import (the same load-order
# circular-import idiom remote_mcp_wire_schemas.py's own
# virtual_jarvis_job_output_schema already established) rather than a
# module-scope one. The facade block at the bottom of this file re-exports
# every name that survives with a real caller (confirmed by a whole-tree
# grep for both `from clio_relay.remote_mcp import` and
# `monkeypatch.setattr(remote_mcp, ...)` before this split); every name with
# zero remaining caller anywhere is not re-imported at all, matching this
# file's own established convention (see e.g. the removed
# REMOTE_MCP_REPLACE_ATTEMPTS/REMOTE_MCP_REPLACE_RETRY_SECONDS precedent
# below). None of remote_mcp.py's own former body remains below this point
# -- the file is a pure facade + the still-resident contract-pin constants.
from clio_relay import (
    remote_mcp_acceptance_models,
    remote_mcp_cache,
    remote_mcp_schema_wrapping,
    remote_mcp_tool_schema,
    remote_mcp_wire_schemas,
)
from clio_relay.models import REGISTERED_JARVIS_USER_CONTRACT

JSON = dict[str, Any]
MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS = 600
MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES = 1024 * 1024
# cluster_route_revision_json_schema / jarvis_service_runtime_handoff_json_schema
# re-export (see comment on the remote_mcp_wire_schemas import above) --
# qualified assignment, not `from ... import`, since this file's own body
# has no reader for either any more.
cluster_route_revision_json_schema = remote_mcp_wire_schemas.cluster_route_revision_json_schema
jarvis_service_runtime_handoff_json_schema = (
    remote_mcp_wire_schemas.jarvis_service_runtime_handoff_json_schema
)
# remote_mcp_server_artifact_binding_verified re-export (see comment on the
# remote_mcp_cache import above) -- qualified assignment, not `from ...
# import`, since this file's own body has no reader for it any more.
remote_mcp_server_artifact_binding_verified = (
    remote_mcp_cache.remote_mcp_server_artifact_binding_verified
)
# The remaining eight remote_mcp_cache.py names (the schema discovery cache
# itself, its digest/fingerprint helpers, and the durable-artifact parser)
# are re-exported the same way -- cli.py, cli_remote_mcp.py,
# cli_remote_mcp_validate.py, jarvis_mcp.py, jarvis_mcp_validation_contract.py,
# jarvis_mcp_validation_package_search.py, jarvis_mcp_validation_report.py,
# mcp_server.py, mcp_stdio_validation.py (transitively, via jarvis_mcp.py),
# and tests import them directly from clio_relay.remote_mcp; none have a
# reader left in this file's own body.
RemoteMcpSchemaCache = remote_mcp_cache.RemoteMcpSchemaCache
RemoteMcpSchemaCacheEntry = remote_mcp_cache.RemoteMcpSchemaCacheEntry
cache_entry_from_discovery_artifact = remote_mcp_cache.cache_entry_from_discovery_artifact
default_remote_mcp_cache_path = remote_mcp_cache.default_remote_mcp_cache_path
remote_mcp_execution_fingerprint = remote_mcp_cache.remote_mcp_execution_fingerprint
remote_mcp_registration_revision = remote_mcp_cache.remote_mcp_registration_revision
remote_mcp_schema_digest = remote_mcp_cache.remote_mcp_schema_digest
remote_mcp_server_artifact_digest = remote_mcp_cache.remote_mcp_server_artifact_digest

# Local relay control envelope injection (remote_mcp_schema_wrapping.py):
# VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS (queue_tasks.py imports it
# directly) and inject_cluster_argument (tests import it directly) are
# re-exported; the rest of that module has no caller outside
# remote_mcp.py's own former body and is not re-imported at all.
VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS = (
    remote_mcp_schema_wrapping.VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
)
inject_cluster_argument = remote_mcp_schema_wrapping.inject_cluster_argument

# Discovered remote MCP tool schema, provenance, and identity verification
# (remote_mcp_tool_schema.py): RemoteMcpDiscoveryProvenance, RemoteMcpToolSchema,
# and is_remote_mcp_control_query are re-exported -- external callers across
# several modules and tests import them directly from clio_relay.remote_mcp.
# The rest of that module (the identity/verification predicates, the cache
# source constant) has no caller outside remote_mcp.py's own former body and
# is not re-imported at all.
RemoteMcpDiscoveryProvenance = remote_mcp_tool_schema.RemoteMcpDiscoveryProvenance
RemoteMcpToolSchema = remote_mcp_tool_schema.RemoteMcpToolSchema
is_remote_mcp_control_query = remote_mcp_tool_schema.is_remote_mcp_control_query

# Agent-facing JSON-Schema builders (remote_mcp_wire_schemas.py):
# VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA and virtual_jarvis_job_output_schema
# are re-exported too (cluster_route_revision_json_schema and
# jarvis_service_runtime_handoff_json_schema are handled above) -- tests and
# jarvis_mcp.py import them directly.
VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA = remote_mcp_wire_schemas.VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA
virtual_jarvis_job_output_schema = remote_mcp_wire_schemas.virtual_jarvis_job_output_schema

# The release-acceptance evidence wire model cluster
# (remote_mcp_acceptance_models.py): RemoteMcpAcceptanceCheck,
# RemoteMcpAcceptanceReport, RemoteMcpSpackConfigurationObservation,
# RemoteMcpSpackInstallTransitionEvidence, and
# RemoteMcpStructuredResultExpectation are re-exported -- external callers
# across several modules and tests import them directly. The rest of that
# module (the transition-evidence sub-models, the two path-canonicalization
# primitives) has no caller outside remote_mcp.py's own former body and is
# not re-imported at all.
RemoteMcpAcceptanceCheck = remote_mcp_acceptance_models.RemoteMcpAcceptanceCheck
RemoteMcpAcceptanceReport = remote_mcp_acceptance_models.RemoteMcpAcceptanceReport
RemoteMcpSpackConfigurationObservation = (
    remote_mcp_acceptance_models.RemoteMcpSpackConfigurationObservation
)
RemoteMcpSpackInstallTransitionEvidence = (
    remote_mcp_acceptance_models.RemoteMcpSpackInstallTransitionEvidence
)
RemoteMcpStructuredResultExpectation = (
    remote_mcp_acceptance_models.RemoteMcpStructuredResultExpectation
)
MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS = 8
# The other three Spack-configuration constants below moved to
# remote_mcp_acceptance_models.py too but have no reader left in this file's
# own body -- cli.py imports them directly, so they are re-exported below
# via qualified assignment (ruff's unused-import check has no equivalent for
# a plain module-level assignment, unlike a `from ... import` it would keep
# stripping as dead).
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES = (
    remote_mcp_acceptance_models.MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES
)
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS = (
    remote_mcp_acceptance_models.MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS
)
MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES = (
    remote_mcp_acceptance_models.MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES
)
MAX_REMOTE_MCP_CATALOG_ISSUES = 10_000

logger = logging.getLogger(__name__)
# REMOTE_MCP_REPLACE_ATTEMPTS and REMOTE_MCP_REPLACE_RETRY_SECONDS moved to
# remote_mcp_cache.py with RemoteMcpSchemaCache._write_atomic, their only
# reader.
CLIO_KIT_JARVIS_USER_CONTRACT_ID = REGISTERED_JARVIS_USER_CONTRACT
CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID = "clio-kit-jarvis-user-v3.6"
CLIO_KIT_JARVIS_USER_CONTRACT_IDS = frozenset(
    {
        CLIO_KIT_JARVIS_USER_CONTRACT_ID,
        CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
    }
)
CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID = {
    CLIO_KIT_JARVIS_USER_CONTRACT_ID: (
        "52238d942a15e48e4d92984b5c1ca939ac224dcc067452c6828c63247e1dd2e5"
    ),
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID: (
        "a326af259e50b57b19b7aa1d209720d5a71e57d7c2e64979596c4fc21850bdda"
    ),
}
CLIO_KIT_JARVIS_USER_WIRE_SHA256_BY_ID = {
    CLIO_KIT_JARVIS_USER_CONTRACT_ID: (
        "ee3f0d5edd92f635646e9f8463ed79bfea0fda963fafed85845d0303cb59ac24"
    ),
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID: (
        "0f912b4dfa7135448834f915984ca413f77b73ef53b0f758594006cd7551cd35"
    ),
}
CLIO_KIT_JARVIS_USER_ARTIFACT_SHA256_BY_ID = {
    CLIO_KIT_JARVIS_USER_CONTRACT_ID: (
        "915cbc6efdbc3e252022025e96acee38b3b5b9b6cb832a781d6253c8eca51896"
    ),
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID: (
        "08c86eda618ec83109fa3c86fe28eccfd44c1ed0e647b93bcb0a82e470fd0d5e"
    ),
}
CLIO_KIT_JARVIS_USER_CONTRACT_ARTIFACT_BY_ID = {
    CLIO_KIT_JARVIS_USER_CONTRACT_ID: "jarvis-user-v3.7.2.json",
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID: "jarvis-user-v3.6.json",
}
CLIO_KIT_JARVIS_USER_CONTRACT_SHA256 = CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID[
    CLIO_KIT_JARVIS_USER_CONTRACT_ID
]
# The sole definition of the current JARVIS user-contract digests: jarvis_mcp.py
# imports both constants below rather than keeping its own duplicate literals
# (clio-relay#199 found the two copies had drifted independently).
CLIO_KIT_JARVIS_USER_WIRE_SHA256 = CLIO_KIT_JARVIS_USER_WIRE_SHA256_BY_ID[
    CLIO_KIT_JARVIS_USER_CONTRACT_ID
]
CLIO_KIT_JARVIS_USER_TOOL_NAMES = frozenset(
    {
        "jarvis_add_step",
        "jarvis_create_pipeline",
        "jarvis_describe",
        "jarvis_edit_step",
        "jarvis_get_execution",
        "jarvis_run",
    }
)
CLIO_KIT_SPACK_USER_WHEEL_VERSION = "2.7.2"
CLIO_KIT_SPACK_USER_CONTRACT_ID = "clio-kit-spack-user-v2.1"
CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID = "clio-kit-spack-user-v2"
# v2.3 adds spack_search/spack_info to the audited surface (clio-kit 2.8.0) and
# revises spack_install's outputSchema; a v2.1/v2-declared registration whose
# live server now answers v2.3 is handled as a forward-compatible subset (see
# _spack_user_contract_check), not a hard failure -- a v2.1/v2-only relay build
# used to drop the ENTIRE spack registration once the kit shipped ahead of the
# relay's known contract set.
CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3 = "clio-kit-spack-user-v2.3"
CLIO_KIT_SPACK_USER_CONTRACT_IDS = frozenset(
    {
        CLIO_KIT_SPACK_USER_CONTRACT_ID,
        CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID,
        CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3,
    }
)
# Digest the MCP wire ``tools/list`` result. FastMCP's in-process FunctionTool
# schemas retain ``$defs`` that its protocol serializer dereferences, so their
# digest is intentionally not the relay contract.
CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: (
        "4a065d2c67c0dd34e2cc18bca9dc53ed87ce35aa4ac524ef3e5c954a875c19db"
    ),
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: (
        "3c5412148c770f4844e98eb893c4db0d0afdbf13afe967df67bd5f7d25e1f7db"
    ),
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: (
        "ce5d1827c365b549308130e110cffac66a21e0202d688ac38636cd8ad98b6b85"
    ),
}
CLIO_KIT_SPACK_USER_WIRE_SHA256_BY_ID = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: (
        "c7f1d1a4ce35b58664b46d2994863257a1e5a30e5c4ab7501b0a96a4becc08b7"
    ),
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: (
        "e575c901226a34a1f4286228b3f71966fe55b68d82bae5c6fd6582af0e43fd2d"
    ),
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: (
        "6bc1de3515226777f4a4823d921787a87766d6657451cf0f053f56c9740fe3ae"
    ),
}
CLIO_KIT_SPACK_USER_ARTIFACT_SHA256_BY_ID = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: (
        "b8da9a3cad05ad734ac3a20adb635f11fa45a8870afe08a9f4e261fdc713b57d"
    ),
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: (
        "6a254d2d6734b71d8069b6806a81a4e237cc682e3bf6dde4b76b61de7464701b"
    ),
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: (
        "f6c8305d9ac5c983ceb8f05c40d89308851670972a67105bf253d5056d3418b6"
    ),
}
CLIO_KIT_SPACK_USER_CONTRACT_ARTIFACT_BY_ID = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: "spack-user-v2.1.json",
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: "spack-user-v2.json",
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: "spack-user-v2.3.json",
}
CLIO_KIT_SPACK_USER_WHEEL_VERSION_BY_ID = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: "2.7.2",
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: "2.7.2",
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: "2.8.0",
}
CLIO_KIT_SPACK_USER_CONTRACT_SHA256 = CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID[
    CLIO_KIT_SPACK_USER_CONTRACT_ID
]
# The audited tool surface per declared contract. v2.1/v2 share the original
# 3-tool surface; v2.3 adds spack_search/spack_info.
CLIO_KIT_SPACK_USER_LEGACY_TOOL_NAMES = frozenset({"spack_find", "spack_locate", "spack_install"})
CLIO_KIT_SPACK_USER_V2_3_TOOL_NAMES = frozenset(
    {"spack_find", "spack_locate", "spack_install", "spack_search", "spack_info"}
)
CLIO_KIT_SPACK_USER_TOOL_NAMES_BY_ID: dict[str, frozenset[str]] = {
    CLIO_KIT_SPACK_USER_CONTRACT_ID: CLIO_KIT_SPACK_USER_LEGACY_TOOL_NAMES,
    CLIO_KIT_SPACK_USER_LEGACY_CONTRACT_ID: CLIO_KIT_SPACK_USER_LEGACY_TOOL_NAMES,
    CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3: CLIO_KIT_SPACK_USER_V2_3_TOOL_NAMES,
}
CLIO_KIT_SCIENTIFIC_CATALOG_USER_WHEEL_VERSION = "2.7.2"
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID = "clio-kit-scientific-catalog-user-v1.1"
CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID = "clio-kit-scientific-catalog-user-v1"
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_IDS = frozenset(
    {
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID,
    }
)
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID = {
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID: (
        "fd9fd4ba76617f1fd13560420cd650f78adc55d0957bd950d10d09c72ebe1889"
    ),
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID: (
        "a53006f24f4698f659f0a7c8bf61fc7bd7ad23274b06d2eed2ccfca68b9ecb0a"
    ),
}
CLIO_KIT_SCIENTIFIC_CATALOG_USER_WIRE_SHA256_BY_ID = {
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID: (
        "6a8fc61e31515880c722db3447d2f01584e4b297cb02b70b5618bff081840380"
    ),
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID: (
        "55bd04f45653c9117b8a1ae41cbc5e79dd6383e86a851b43caa071ae357bb2c4"
    ),
}
CLIO_KIT_SCIENTIFIC_CATALOG_USER_ARTIFACT_SHA256_BY_ID = {
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID: (
        "8548aa8f0d1993ec644bb2fea778a4759b27d34bea3ed93ff92254b6fbf3052e"
    ),
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID: (
        "86097ef70d5b4e740a93ae0cdd0eb723cae1ac8898cfd8cbbc1911835cb22d56"
    ),
}
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ARTIFACT_BY_ID = {
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID: "scientific-catalog-user-v1.1.json",
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID: ("scientific-catalog-user-v1.json"),
}
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256 = (
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID[
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID
    ]
)

# Virtual remote MCP catalog data model (RemoteMcpRoute, VirtualRemoteMcpTool,
# VirtualRemoteMcpCatalog, unavailable_virtual_remote_mcp_catalog) moved to
# remote_mcp_catalog_models.py -- see that module's own docstring for the
# full re-export/private-import rationale. All four have a caller outside
# remote_mcp.py's own former body (external modules/tests import the first
# three; mcp_server.py imports the fourth), so all four are re-exported.
# _virtual_remote_mcp_relay_arguments and _Candidate are private with no
# outside caller (remote_mcp_catalog_build.py imports _Candidate directly
# from remote_mcp_catalog_models.py, not from here) but are imported back
# for backward-compatible attribute access. None have a reader left in this
# file's own body, so all six use qualified assignment.
RemoteMcpRoute = remote_mcp_catalog_models.RemoteMcpRoute
VirtualRemoteMcpTool = remote_mcp_catalog_models.VirtualRemoteMcpTool
VirtualRemoteMcpCatalog = remote_mcp_catalog_models.VirtualRemoteMcpCatalog
unavailable_virtual_remote_mcp_catalog = (
    remote_mcp_catalog_models.unavailable_virtual_remote_mcp_catalog
)
_virtual_remote_mcp_relay_arguments = remote_mcp_catalog_models._virtual_remote_mcp_relay_arguments
_Candidate = remote_mcp_catalog_models._Candidate

# MCP admission resolution (resolve_registered_remote_mcp_admission,
# resolve_pinned_mcp_admission, _validate_pinned_control_query_timeout,
# _control_query_discovery_artifact_bytes) moved to remote_mcp_admission.py.
# The first two are re-exported under their original names -- cli.py,
# cli_jarvis_mcp.py, http_api.py, cli_remote_mcp.py, session_api.py, and
# tests import them directly. The two private helpers have no caller outside
# remote_mcp.py's own former body, imported back for backward-compatible
# attribute access. All four use qualified assignment.
resolve_registered_remote_mcp_admission = (
    remote_mcp_admission.resolve_registered_remote_mcp_admission
)
resolve_pinned_mcp_admission = remote_mcp_admission.resolve_pinned_mcp_admission
_validate_pinned_control_query_timeout = remote_mcp_admission._validate_pinned_control_query_timeout
_control_query_discovery_artifact_bytes = (
    remote_mcp_admission._control_query_discovery_artifact_bytes
)

# Declared semantic-contract acceptance checks (_spack_user_contract_check,
# _scientific_catalog_user_contract_check, _declared_contract_check,
# _jarvis_user_contract_check, plus the two Spack tool-annotation/input-
# property expectation tables) moved to remote_mcp_contract_checks.py. None
# have a caller outside remote_mcp.py's own former body EXCEPT this file's
# own test suite, which reaches `remote_mcp._declared_contract_check` and
# `remote_mcp._spack_user_contract_check` directly by module-qualified
# attribute access, so all six are imported back via qualified assignment.
CLIO_KIT_SPACK_USER_ANNOTATION_EXPECTATIONS = (
    remote_mcp_contract_checks.CLIO_KIT_SPACK_USER_ANNOTATION_EXPECTATIONS
)
CLIO_KIT_SPACK_USER_REQUIRED_INPUT_PROPERTY = (
    remote_mcp_contract_checks.CLIO_KIT_SPACK_USER_REQUIRED_INPUT_PROPERTY
)
_spack_user_contract_check = remote_mcp_contract_checks._spack_user_contract_check
_scientific_catalog_user_contract_check = (
    remote_mcp_contract_checks._scientific_catalog_user_contract_check
)
_declared_contract_check = remote_mcp_contract_checks._declared_contract_check
_jarvis_user_contract_check = remote_mcp_contract_checks._jarvis_user_contract_check

# Virtual remote MCP catalog assembly (build_virtual_remote_mcp_catalog,
# load_virtual_remote_mcp_catalog) moved to remote_mcp_catalog_build.py. Both
# are re-exported under their original names -- mcp_server.py and tests
# import them directly. MAX_VIRTUAL_REMOTE_MCP_CANDIDATES, owned by
# remote_mcp_aliasing.py, is re-exported here too (not there) because the
# test suite's `monkeypatch.setattr(remote_mcp, "MAX_VIRTUAL_REMOTE_MCP_CANDIDATES",
# 1)` patches this facade's own attribute -- see remote_mcp_catalog_build.py's
# own docstring for why that function reads it back via a call-time import
# rather than a module-scope one. All three use qualified assignment.
build_virtual_remote_mcp_catalog = remote_mcp_catalog_build.build_virtual_remote_mcp_catalog
load_virtual_remote_mcp_catalog = remote_mcp_catalog_build.load_virtual_remote_mcp_catalog
MAX_VIRTUAL_REMOTE_MCP_CANDIDATES = remote_mcp_aliasing.MAX_VIRTUAL_REMOTE_MCP_CANDIDATES

# The canonical release-acceptance report builder
# (build_remote_mcp_acceptance_report) moved to remote_mcp_acceptance_report.py.
# Re-exported under its original name -- cli_remote_mcp_validate.py and
# tests import it directly, and remote_mcp_validation.py calls it as
# `remote_mcp.build_remote_mcp_acceptance_report`, a module-qualified lookup
# that resolves through this same re-export. Qualified assignment.
build_remote_mcp_acceptance_report = remote_mcp_acceptance_report.build_remote_mcp_acceptance_report

# The fresh-install Spack transition report builder
# (build_remote_mcp_spack_fresh_install_transition_report) moved to
# remote_mcp_spack_transition_report.py, and its constituent per-phase
# checks moved to remote_mcp_spack_transition_checks.py, a separate owner
# module extracted first so the report builder can depend on it without a
# circular import. build_remote_mcp_spack_fresh_install_transition_report is
# re-exported under its original name -- cli_remote_mcp_validate.py and
# tests import it directly. Every other name has no caller outside
# remote_mcp.py's own former body, imported back for backward-compatible
# attribute access. All use qualified assignment.
build_remote_mcp_spack_fresh_install_transition_report = (
    remote_mcp_spack_transition_report.build_remote_mcp_spack_fresh_install_transition_report
)
_spack_fresh_configuration_check = (
    remote_mcp_spack_transition_report._spack_fresh_configuration_check
)
_spack_command_configuration_binding = (
    remote_mcp_spack_transition_report._spack_command_configuration_binding
)
_phase_prefixed_acceptance_checks = (
    remote_mcp_spack_transition_report._phase_prefixed_acceptance_checks
)
_uniquely_named_acceptance_checks = (
    remote_mcp_spack_transition_report._uniquely_named_acceptance_checks
)
_spack_transition_identity_check = (
    remote_mcp_spack_transition_checks._spack_transition_identity_check
)
_spack_transition_durable_evidence_check = (
    remote_mcp_spack_transition_checks._spack_transition_durable_evidence_check
)
_spack_preinstall_absent_check = remote_mcp_spack_transition_checks._spack_preinstall_absent_check
_spack_fresh_install_check = remote_mcp_spack_transition_checks._spack_fresh_install_check
_spack_postinstall_locate_check = remote_mcp_spack_transition_checks._spack_postinstall_locate_check
_spack_transition_structured_result = (
    remote_mcp_spack_transition_checks._spack_transition_structured_result
)
_spack_transition_output_schema = remote_mcp_spack_transition_checks._spack_transition_output_schema
_spack_transition_call_evidence = remote_mcp_spack_transition_checks._spack_transition_call_evidence

# Bounded evidence-projection primitives shared by the acceptance validators
# moved to remote_mcp_acceptance_evidence.py. None have a caller outside
# remote_mcp.py's own former body, imported back for backward-compatible
# attribute access. All use qualified assignment.
_transition_call_arguments = remote_mcp_acceptance_evidence._transition_call_arguments
_bounded_transition_arguments = remote_mcp_acceptance_evidence._bounded_transition_arguments
_bounded_spack_package_identity = remote_mcp_acceptance_evidence._bounded_spack_package_identity
_bounded_evidence_scalar = remote_mcp_acceptance_evidence._bounded_evidence_scalar
_bounded_optional_string = remote_mcp_acceptance_evidence._bounded_optional_string
_acceptance_check_string = remote_mcp_acceptance_evidence._acceptance_check_string
_acceptance_server_artifact = remote_mcp_acceptance_evidence._acceptance_server_artifact
_same_nonempty_strings = remote_mcp_acceptance_evidence._same_nonempty_strings
_common_string = remote_mcp_acceptance_evidence._common_string
_is_strict_canonical_posix_descendant = (
    remote_mcp_acceptance_evidence._is_strict_canonical_posix_descendant
)

# Structured-result acceptance checks against an explicit semantic contract
# moved to remote_mcp_structured_result.py. build_remote_mcp_structured_result_check
# is re-exported under its original name -- tests import it directly. The
# other two have no caller outside remote_mcp.py's own former body, imported
# back for backward-compatible attribute access. All use qualified assignment.
build_remote_mcp_structured_result_check = (
    remote_mcp_structured_result.build_remote_mcp_structured_result_check
)
_spack_structured_result_check = remote_mcp_structured_result._spack_structured_result_check
_structured_result_schema_evidence = remote_mcp_structured_result._structured_result_schema_evidence

# Scientific-catalog structured-result acceptance check moved to
# remote_mcp_scientific_catalog_result.py. None have a caller outside
# remote_mcp.py's own former body, imported back for backward-compatible
# attribute access. All use qualified assignment.
_scientific_catalog_structured_result_check = (
    remote_mcp_scientific_catalog_result._scientific_catalog_structured_result_check
)
_scientific_catalog_structured_schema_evidence = (
    remote_mcp_scientific_catalog_result._scientific_catalog_structured_schema_evidence
)
_scientific_catalog_descriptor_sha256 = (
    remote_mcp_scientific_catalog_result._scientific_catalog_descriptor_sha256
)
_bounded_catalog_identity = remote_mcp_scientific_catalog_result._bounded_catalog_identity

# Per-operation Spack structured-result validation moved to
# remote_mcp_spack_result_validation.py. None have a caller outside
# remote_mcp.py's own former body, imported back for backward-compatible
# attribute access. All use qualified assignment.
_validate_spack_find_result = remote_mcp_spack_result_validation._validate_spack_find_result
_validate_spack_locate_result = remote_mcp_spack_result_validation._validate_spack_locate_result
_validate_spack_install_result = remote_mcp_spack_result_validation._validate_spack_install_result
_record_expected_spack_package = remote_mcp_spack_result_validation._record_expected_spack_package
_spack_package_records = remote_mcp_spack_result_validation._spack_package_records
_spack_package_matches = remote_mcp_spack_result_validation._spack_package_matches
_spack_package_identity = remote_mcp_spack_result_validation._spack_package_identity

# Packaged relay MCP stdio evidence extraction moved to
# remote_mcp_stdio_evidence.py. None have a caller outside remote_mcp.py's
# own former body, imported back for backward-compatible attribute access.
# All use qualified assignment.
_stdio_initialize_passed = remote_mcp_stdio_evidence._stdio_initialize_passed
_stdio_listed_tool_names = remote_mcp_stdio_evidence._stdio_listed_tool_names
_stdio_call_job_id = remote_mcp_stdio_evidence._stdio_call_job_id
_as_json = remote_mcp_stdio_evidence._as_json
