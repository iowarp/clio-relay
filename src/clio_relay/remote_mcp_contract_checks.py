"""Declared semantic-contract acceptance checks (JARVIS/Spack/scientific catalog).

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the per-family audited-surface
checks an operator's declared ``contract=`` dispatches to
(:func:`_declared_contract_check`): the exact six-tool JARVIS contract
(:func:`_jarvis_user_contract_check`), the audited stateless Spack surface
with its forward-compatible v2.1/v2 -> v2.3 subset acceptance
(:func:`_spack_user_contract_check`, plus its two pinned tool-annotation/
input-property expectation tables), and the exact read-only scientific
catalog surface (:func:`_scientific_catalog_user_contract_check`). Each
check re-derives the live tool digest from the schema cache entry and
compares it against the SHA-256-pinned contract this relay build audited --
never a version-number rubber stamp.

None of these four functions have a caller outside ``remote_mcp.py``
(confirmed by grep before the move; ``remote_mcp_catalog_build.py`` and
``remote_mcp_acceptance_report.py`` import ``_declared_contract_check``
directly from here, and ``remote_mcp.py``'s own test suite reaches
``_declared_contract_check`` and ``_spack_user_contract_check`` via
``remote_mcp.<name>``), so ``remote_mcp.py`` imports them directly rather
than re-exporting them.

Each function reads several of the ``CLIO_KIT_*`` contract-pin constants that
still live in ``remote_mcp.py`` (unsequenced, post-campaign per the design
doc). A module-scope import back into ``remote_mcp.py`` (which imports this
module for its own private-name access) would be a load-order circular
import; importing them inside each function body instead is the proven
idiom for that shape (see ``remote_mcp_wire_schemas.py``'s own
``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

from typing import Any, cast

from clio_relay.cluster_config import RemoteMcpServerConfig
from clio_relay.remote_mcp_acceptance_models import RemoteMcpAcceptanceCheck
from clio_relay.remote_mcp_cache import RemoteMcpSchemaCacheEntry, remote_mcp_schema_digest
from clio_relay.remote_mcp_stdio_evidence import _as_json

JSON = dict[str, Any]

CLIO_KIT_SPACK_USER_ANNOTATION_EXPECTATIONS: dict[str, dict[str, bool]] = {
    "spack_find": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    "spack_locate": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    "spack_install": {"readOnlyHint": False, "destructiveHint": False, "openWorldHint": True},
    "spack_search": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    "spack_info": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
}
# The one required input property that identifies each tool's request, or
# ``None`` for spack_find (which takes only an optional query).
CLIO_KIT_SPACK_USER_REQUIRED_INPUT_PROPERTY: dict[str, str | None] = {
    "spack_find": None,
    "spack_locate": "spec",
    "spack_install": "spec",
    "spack_search": "query",
    "spack_info": "package",
}


def _spack_user_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig | None,
) -> RemoteMcpAcceptanceCheck:
    """Require the audited stateless Spack surface approved for desktop agents.

    A registration declaring an older contract (v2.1/v2, 3 tools) is accepted
    when the live server has moved on to a newer *audited* contract (v2.3, 5
    tools) that still serves every declared tool unchanged: only the declared
    subset is exposed (``allow_tools`` stays pinned to the declared set), and
    the acceptance evidence carries a typed ``contract_drift_notice`` naming
    the live contract so operators can re-register to unlock the rest. This is
    a forward-compatible subset check, not a rubber stamp: the live server's
    *full* tool digest must still match one of the SHA-256-pinned contracts
    this relay build knows about, or the registration fails closed exactly as
    it always has.
    """
    from clio_relay.remote_mcp import (
        CLIO_KIT_SPACK_USER_ARTIFACT_SHA256_BY_ID,
        CLIO_KIT_SPACK_USER_CONTRACT_ARTIFACT_BY_ID,
        CLIO_KIT_SPACK_USER_CONTRACT_IDS,
        CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID,
        CLIO_KIT_SPACK_USER_LEGACY_TOOL_NAMES,
        CLIO_KIT_SPACK_USER_TOOL_NAMES_BY_ID,
        CLIO_KIT_SPACK_USER_WHEEL_VERSION,
        CLIO_KIT_SPACK_USER_WHEEL_VERSION_BY_ID,
        CLIO_KIT_SPACK_USER_WIRE_SHA256_BY_ID,
    )

    declared_contract = registration.contract if registration is not None else None
    expected_names: set[str] = set(
        CLIO_KIT_SPACK_USER_TOOL_NAMES_BY_ID.get(
            declared_contract or "", CLIO_KIT_SPACK_USER_LEGACY_TOOL_NAMES
        )
    )
    tools = {tool.name: tool for tool in entry.tools} if entry is not None else {}
    actual_names = set(tools)
    allowlisted_names: set[str] = (
        set(registration.allow_tools) if registration is not None else set()
    )
    observed_contract_digest = remote_mcp_schema_digest(list(tools.values()))
    expected_contract_digest = CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID.get(
        declared_contract or ""
    )

    annotation_matches: dict[str, bool] = {}
    schema_matches: dict[str, bool] = {}
    for name, expected_annotations in CLIO_KIT_SPACK_USER_ANNOTATION_EXPECTATIONS.items():
        tool = tools.get(name)
        annotations = tool.annotations if tool is not None else None
        annotation_matches[name] = annotations is not None and all(
            annotations.get(key) is value for key, value in expected_annotations.items()
        )
        schema = tool.input_schema if tool is not None else {}
        raw_required = schema.get("required", [])
        required = cast(list[object], raw_required) if isinstance(raw_required, list) else []
        raw_properties = schema.get("properties", {})
        properties = cast(JSON, raw_properties) if isinstance(raw_properties, dict) else {}
        required_property = CLIO_KIT_SPACK_USER_REQUIRED_INPUT_PROPERTY[name]
        schema_matches[name] = (
            schema.get("type") == "object"
            and schema.get("additionalProperties") is False
            and (
                required_property is None
                or (
                    required_property in required
                    and isinstance(properties.get(required_property), dict)
                )
            )
        )

    locate_output = tools.get("spack_locate")
    output_schema = locate_output.output_schema if locate_output is not None else None
    output_properties = (
        cast(JSON, output_schema.get("properties"))
        if output_schema is not None and isinstance(output_schema.get("properties"), dict)
        else {}
    )
    output_required_value: object = (
        output_schema.get("required", []) if output_schema is not None else []
    )
    output_required = (
        cast(list[object], output_required_value) if isinstance(output_required_value, list) else []
    )
    locate_load_spec_matches = (
        output_schema is not None
        and isinstance(output_properties.get("load_spec"), dict)
        and cast(JSON, output_properties["load_spec"]).get("type") == "string"
        and "load_spec" in output_required
    )

    declared_tools_present = expected_names <= actual_names
    exact_match = actual_names == expected_names
    drifted = declared_tools_present and not exact_match
    live_matched_contract_id: str | None = None
    if not declared_tools_present:
        digest_ok = False
    elif exact_match:
        digest_ok = observed_contract_digest == expected_contract_digest
        live_matched_contract_id = declared_contract if digest_ok else None
    else:
        # Forward-compatible path: the declared tools are all present, plus
        # undeclared extras. Only accept it when the FULL live surface digest
        # matches some other contract this relay build has audited by
        # SHA-256 -- never a blind "newer is fine" acceptance.
        live_matched_contract_id = next(
            (
                candidate_id
                for candidate_id, digest in CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID.items()
                if digest == observed_contract_digest
            ),
            None,
        )
        digest_ok = live_matched_contract_id is not None

    passed = (
        declared_tools_present
        and allowlisted_names == expected_names
        and registration is not None
        and registration.contract in CLIO_KIT_SPACK_USER_CONTRACT_IDS
        and "user" in registration.profiles
        and all(annotation_matches[name] for name in expected_names)
        and all(schema_matches[name] for name in expected_names)
        and locate_load_spec_matches
        and digest_ok
    )

    contract_drift_notice: str | None = None
    message: str
    if not passed:
        message = "Spack user tools, allowlist, schemas, or safety annotations drifted"
    elif drifted:
        contract_drift_notice = (
            f"declared contract {declared_contract!r} audits {sorted(expected_names)}; "
            f"live server now answers {sorted(actual_names)}"
            + (
                f", matching audited contract {live_matched_contract_id!r}"
                if live_matched_contract_id is not None
                else ""
            )
            + f" -- only the declared {sorted(expected_names)} subset is served; "
            "re-register this remote MCP server with "
            f"--contract {live_matched_contract_id!r} to expose the rest"
        )
        message = contract_drift_notice
    else:
        message = "Spack exposes exactly the declared audited contract's tool set and schemas"

    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-user-contract",
        passed=passed,
        message=message,
        evidence={
            "expected_tool_names": sorted(expected_names),
            "remote_tool_names": sorted(actual_names),
            "allowlisted_tool_names": sorted(allowlisted_names),
            "profiles": registration.profiles if registration is not None else [],
            "declared_contract": declared_contract,
            "annotations_match": annotation_matches,
            "schemas_match": schema_matches,
            "locate_load_spec_matches": locate_load_spec_matches,
            "stateful_load_exposed": "spack_load" in actual_names,
            "expected_contract_sha256": expected_contract_digest,
            "expected_wire_sha256": CLIO_KIT_SPACK_USER_WIRE_SHA256_BY_ID.get(
                declared_contract or ""
            ),
            "expected_contract_artifact": CLIO_KIT_SPACK_USER_CONTRACT_ARTIFACT_BY_ID.get(
                declared_contract or ""
            ),
            "expected_contract_artifact_sha256": (
                CLIO_KIT_SPACK_USER_ARTIFACT_SHA256_BY_ID.get(declared_contract or "")
            ),
            "expected_clio_kit_version": CLIO_KIT_SPACK_USER_WHEEL_VERSION_BY_ID.get(
                declared_contract or "", CLIO_KIT_SPACK_USER_WHEEL_VERSION
            ),
            "observed_contract_sha256": observed_contract_digest,
            "live_contract_drifted": drifted,
            "live_matched_contract_id": live_matched_contract_id,
            "live_tool_names_beyond_declared": sorted(actual_names - expected_names),
            "contract_drift_notice": contract_drift_notice,
        },
    )


def _scientific_catalog_user_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig | None,
) -> RemoteMcpAcceptanceCheck:
    """Require the exact read-only scientific catalog surface approved for agents."""
    from clio_relay.remote_mcp import (
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_ARTIFACT_SHA256_BY_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ARTIFACT_BY_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_IDS,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_WHEEL_VERSION,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_WIRE_SHA256_BY_ID,
    )

    expected_names = {"scientific_dataset_describe", "scientific_dataset_search"}
    tools = {tool.name: tool for tool in entry.tools} if entry is not None else {}
    actual_names = set(tools)
    allowlisted_names: set[str] = (
        set(registration.allow_tools) if registration is not None else set()
    )
    observed_contract_digest = remote_mcp_schema_digest(list(tools.values()))
    declared_contract = registration.contract if registration is not None else None
    expected_contract_digest = CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID.get(
        declared_contract or ""
    )

    annotation_matches: dict[str, bool] = {}
    expected_annotations = {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    for name in expected_names:
        tool = tools.get(name)
        annotations = tool.annotations if tool is not None else None
        annotation_matches[name] = annotations is not None and all(
            annotations.get(key) is value for key, value in expected_annotations.items()
        )

    describe = tools.get("scientific_dataset_describe")
    describe_input = describe.input_schema if describe is not None else {}
    describe_input_properties = _as_json(describe_input.get("properties")) or {}
    describe_required = describe_input.get("required")
    describe_input_matches = (
        describe_input.get("type") == "object"
        and describe_input.get("additionalProperties") is False
        and set(describe_input_properties) == {"dataset_id"}
        and _as_json(describe_input_properties.get("dataset_id")) == {"type": "string"}
        and isinstance(describe_required, list)
        and cast(list[object], describe_required) == ["dataset_id"]
    )

    search = tools.get("scientific_dataset_search")
    search_input = search.input_schema if search is not None else {}
    search_input_properties = _as_json(search_input.get("properties")) or {}
    page_size = _as_json(search_input_properties.get("page_size")) or {}
    search_input_matches = (
        search_input.get("type") == "object"
        and search_input.get("additionalProperties") is False
        and set(search_input_properties)
        == {"query", "tags", "kind", "format", "page_size", "cursor"}
        and page_size == {"default": 20, "maximum": 100, "minimum": 1, "type": "integer"}
        and search_input.get("required", []) == []
    )

    describe_output = describe.output_schema if describe is not None else None
    describe_output_properties = (
        _as_json(describe_output.get("properties")) if describe_output is not None else None
    ) or {}
    describe_output_required_value: object = (
        describe_output.get("required", []) if describe_output is not None else []
    )
    describe_output_required = (
        cast(list[object], describe_output_required_value)
        if isinstance(describe_output_required_value, list)
        else []
    )
    dataset_schema = _as_json(describe_output_properties.get("dataset")) or {}
    dataset_properties = _as_json(dataset_schema.get("properties")) or {}
    descriptor_schema = _as_json(dataset_properties.get("descriptor")) or {}
    descriptor_properties = _as_json(descriptor_schema.get("properties")) or {}
    handoff_descriptor_schema = _as_json(describe_output_properties.get("dataset_descriptor")) or {}
    descriptor_handoff_required = declared_contract == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID
    descriptor_handoff_matches: bool | None = (
        "dataset_descriptor" in describe_output_required
        and handoff_descriptor_schema == descriptor_schema
        if descriptor_handoff_required
        else None
    )
    descriptor_handoff_accepted = (
        descriptor_handoff_matches is True
        if descriptor_handoff_required
        else declared_contract == CLIO_KIT_SCIENTIFIC_CATALOG_USER_LEGACY_CONTRACT_ID
    )
    describe_output_matches = (
        describe_output is not None
        and describe_output.get("type") == "object"
        and describe_output.get("additionalProperties") is False
        and _as_json(describe_output_properties.get("schema_version"))
        == {
            "const": "clio-kit.scientific-dataset-description.v1",
            "default": "clio-kit.scientific-dataset-description.v1",
            "type": "string",
        }
        and descriptor_schema.get("type") == "object"
        and descriptor_schema.get("additionalProperties") is False
        and _as_json(descriptor_properties.get("schema_version"))
        == {"const": "jarvis.dataset-descriptor.v1", "type": "string"}
        and descriptor_handoff_accepted
    )

    search_output = search.output_schema if search is not None else None
    search_output_properties = (
        _as_json(search_output.get("properties")) if search_output is not None else None
    ) or {}
    datasets_schema = _as_json(search_output_properties.get("datasets")) or {}
    dataset_item_schema = _as_json(datasets_schema.get("items")) or {}
    search_output_matches = (
        search_output is not None
        and search_output.get("type") == "object"
        and search_output.get("additionalProperties") is False
        and _as_json(search_output_properties.get("schema_version"))
        == {
            "const": "clio-kit.scientific-dataset-search.v1",
            "default": "clio-kit.scientific-dataset-search.v1",
            "type": "string",
        }
        and datasets_schema.get("type") == "array"
        and dataset_item_schema.get("type") == "object"
        and dataset_item_schema.get("additionalProperties") is False
    )

    schema_matches = {
        "scientific_dataset_describe": describe_input_matches and describe_output_matches,
        "scientific_dataset_search": search_input_matches and search_output_matches,
    }
    passed = (
        actual_names == expected_names
        and allowlisted_names == expected_names
        and registration is not None
        and registration.contract in CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_IDS
        and "user" in registration.profiles
        and all(annotation_matches.values())
        and all(schema_matches.values())
        and observed_contract_digest == expected_contract_digest
    )
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.scientific-catalog-user-contract",
        passed=passed,
        message=(
            "Scientific catalog exposes only read-only search and exact descriptor lookup"
            if passed
            else "Scientific catalog tools, allowlist, schemas, or safety annotations drifted"
        ),
        evidence={
            "expected_tool_names": sorted(expected_names),
            "remote_tool_names": sorted(actual_names),
            "allowlisted_tool_names": sorted(allowlisted_names),
            "profiles": registration.profiles if registration is not None else [],
            "declared_contract": declared_contract,
            "annotations_match": annotation_matches,
            "schemas_match": schema_matches,
            "dataset_descriptor_handoff_required": descriptor_handoff_required,
            "dataset_descriptor_handoff_matches": descriptor_handoff_matches,
            "expected_contract_sha256": expected_contract_digest,
            "expected_wire_sha256": (
                CLIO_KIT_SCIENTIFIC_CATALOG_USER_WIRE_SHA256_BY_ID.get(declared_contract or "")
            ),
            "expected_contract_artifact": (
                CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ARTIFACT_BY_ID.get(
                    declared_contract or ""
                )
            ),
            "expected_contract_artifact_sha256": (
                CLIO_KIT_SCIENTIFIC_CATALOG_USER_ARTIFACT_SHA256_BY_ID.get(declared_contract or "")
            ),
            "expected_clio_kit_version": (CLIO_KIT_SCIENTIFIC_CATALOG_USER_WHEEL_VERSION),
            "observed_contract_sha256": observed_contract_digest,
        },
    )


def _declared_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig,
) -> RemoteMcpAcceptanceCheck:
    """Evaluate the semantic contract explicitly declared by an operator."""
    from clio_relay.remote_mcp import (
        CLIO_KIT_JARVIS_USER_CONTRACT_IDS,
        CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_IDS,
        CLIO_KIT_SPACK_USER_CONTRACT_IDS,
    )

    if registration.contract in CLIO_KIT_JARVIS_USER_CONTRACT_IDS:
        return _jarvis_user_contract_check(entry, registration)
    if registration.contract in CLIO_KIT_SPACK_USER_CONTRACT_IDS:
        return _spack_user_contract_check(entry, registration)
    if registration.contract in CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_IDS:
        return _scientific_catalog_user_contract_check(entry, registration)
    raise ValueError(f"unsupported remote MCP semantic contract: {registration.contract}")


def _jarvis_user_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig,
) -> RemoteMcpAcceptanceCheck:
    """Require the exact six-tool JARVIS contract approved for desktop agents."""
    from clio_relay.remote_mcp import (
        CLIO_KIT_JARVIS_USER_ARTIFACT_SHA256_BY_ID,
        CLIO_KIT_JARVIS_USER_CONTRACT_ARTIFACT_BY_ID,
        CLIO_KIT_JARVIS_USER_CONTRACT_IDS,
        CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID,
        CLIO_KIT_JARVIS_USER_TOOL_NAMES,
        CLIO_KIT_JARVIS_USER_WIRE_SHA256_BY_ID,
    )

    tools = {tool.name: tool for tool in entry.tools} if entry is not None else {}
    actual_names = set(tools)
    allowed_names = {name for name in actual_names if registration.allows_tool(name)}
    observed_digest = remote_mcp_schema_digest(list(tools.values()))
    declared_contract = registration.contract
    expected_contract_digest = CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID.get(
        declared_contract or ""
    )
    passed = (
        actual_names == set(CLIO_KIT_JARVIS_USER_TOOL_NAMES)
        and allowed_names == set(CLIO_KIT_JARVIS_USER_TOOL_NAMES)
        and "user" in registration.profiles
        and registration.contract in CLIO_KIT_JARVIS_USER_CONTRACT_IDS
        and observed_digest == expected_contract_digest
    )
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.jarvis-user-contract",
        passed=passed,
        message=(
            "JARVIS exposes the exact audited six-tool agent contract"
            if passed
            else "JARVIS user tools, allowlist, profile, or schemas drifted"
        ),
        evidence={
            "declared_contract": declared_contract,
            "expected_tool_names": sorted(CLIO_KIT_JARVIS_USER_TOOL_NAMES),
            "remote_tool_names": sorted(actual_names),
            "allowlisted_tool_names": sorted(allowed_names),
            "profiles": registration.profiles,
            "expected_contract_sha256": expected_contract_digest,
            "expected_wire_sha256": CLIO_KIT_JARVIS_USER_WIRE_SHA256_BY_ID.get(
                declared_contract or ""
            ),
            "expected_contract_artifact": CLIO_KIT_JARVIS_USER_CONTRACT_ARTIFACT_BY_ID.get(
                declared_contract or ""
            ),
            "expected_contract_artifact_sha256": (
                CLIO_KIT_JARVIS_USER_ARTIFACT_SHA256_BY_ID.get(declared_contract or "")
            ),
            "observed_contract_sha256": observed_digest,
        },
    )
