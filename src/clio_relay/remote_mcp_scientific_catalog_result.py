"""Scientific-catalog structured-result acceptance check (v1.1 descriptor handoff).

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the ``scientific_dataset_describe``
result check that proves the exact JARVIS descriptor handoff -- the requested
``dataset_id`` matches the catalog record and both the top-level and nested
descriptor, the top-level ``dataset_descriptor`` equals ``dataset.descriptor``
verbatim, and ``descriptor_sha256`` matches the canonical hash of that exact
descriptor -- plus the two small helpers it alone needs (the bounded
finite-JSON schema guard ahead of schema evaluation, and the canonical
descriptor digest).

None of these four names have a caller outside ``remote_mcp.py`` (confirmed
by grep before the move; ``build_remote_mcp_acceptance_report`` in
``remote_mcp_acceptance_report.py`` imports
``_scientific_catalog_structured_result_check`` directly from here, not from
``remote_mcp.py``), so ``remote_mcp.py`` imports them directly rather than
re-exporting them.

``_scientific_catalog_structured_result_check`` reads
``MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES`` and
``CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID`` -- a bound and a contract-pin
constant that still live in ``remote_mcp.py`` (unsequenced, post-campaign per
the design doc). A module-scope import back into ``remote_mcp.py`` (which
imports this module for its own private-name access) would be a load-order
circular import; importing them inside the function body instead is the
proven idiom for that shape (see ``remote_mcp_wire_schemas.py``'s own
``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from clio_relay.remote_mcp_acceptance_evidence import _bounded_optional_string
from clio_relay.remote_mcp_acceptance_models import RemoteMcpAcceptanceCheck
from clio_relay.remote_mcp_schema_validation import (
    _bounded_diagnostic,
    _require_bounded_json_structure,
    _require_finite_json,
)
from clio_relay.remote_mcp_stdio_evidence import _as_json
from clio_relay.remote_mcp_structured_result import _structured_result_schema_evidence
from clio_relay.remote_mcp_tool_schema import _is_sha256

JSON = dict[str, Any]


def _scientific_catalog_structured_result_check(
    *,
    arguments: object,
    protocol_result: JSON | None,
    output_schema: JSON | None,
) -> RemoteMcpAcceptanceCheck:
    """Verify the v1.1 catalog describe result and explicit JARVIS handoff."""
    from clio_relay.remote_mcp import CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID

    failures: list[str] = []
    typed_arguments = _as_json(arguments) or {}
    structured_value = (
        protocol_result.get("structuredContent") if protocol_result is not None else None
    )
    structured = _as_json(structured_value)
    output_schema_evidence = _scientific_catalog_structured_schema_evidence(
        output_schema=output_schema,
        structured_value=structured_value,
    )
    structured_content_safe = (
        output_schema_evidence["structured_content_bounded"] is True
        and output_schema_evidence["structured_content_finite"] is True
    )
    if not structured_content_safe:
        failures.append("structuredContent is not bounded finite JSON")
    elif output_schema_evidence["schema_present"] is not True:
        failures.append("cached tool outputSchema is absent")
    elif output_schema_evidence["schema_valid"] is not True:
        failures.append("cached tool outputSchema is invalid")
    elif output_schema_evidence["structured_content_valid"] is not True:
        failures.append("structuredContent does not satisfy the cached tool outputSchema")

    requested_dataset_id = _bounded_catalog_identity(typed_arguments.get("dataset_id"))
    dataset: JSON | None = None
    dataset_descriptor: JSON | None = None
    nested_descriptor: JSON | None = None
    computed_descriptor_sha256: str | None = None
    descriptor_digest_error: str | None = None
    if requested_dataset_id is None:
        failures.append("call arguments do not contain one bounded dataset_id")
    if structured is None:
        failures.append("protocol result has no structuredContent object")
    elif structured_content_safe:
        dataset = _as_json(structured.get("dataset"))
        dataset_descriptor = _as_json(structured.get("dataset_descriptor"))
        nested_descriptor = _as_json(dataset.get("descriptor")) if dataset is not None else None
        if dataset_descriptor is not None:
            try:
                computed_descriptor_sha256 = _scientific_catalog_descriptor_sha256(
                    dataset_descriptor
                )
            except (RecursionError, TypeError, ValueError) as exc:
                descriptor_digest_error = _bounded_diagnostic(str(exc))
                failures.append("dataset_descriptor is not canonical finite JSON")

    response_schema_version = (
        structured.get("schema_version")
        if structured is not None and structured_content_safe
        else None
    )
    descriptor_schema_version = (
        dataset_descriptor.get("schema_version") if dataset_descriptor is not None else None
    )
    nested_descriptor_schema_version = (
        nested_descriptor.get("schema_version") if nested_descriptor is not None else None
    )
    schema_versions_match = (
        response_schema_version == "clio-kit.scientific-dataset-description.v1"
        and descriptor_schema_version == "jarvis.dataset-descriptor.v1"
        and nested_descriptor_schema_version == "jarvis.dataset-descriptor.v1"
    )
    if not schema_versions_match:
        failures.append("catalog result or descriptor schema_version does not match v1.1")

    dataset_id = _bounded_catalog_identity(
        dataset.get("dataset_id") if dataset is not None else None
    )
    descriptor_dataset_id = _bounded_catalog_identity(
        dataset_descriptor.get("dataset_id") if dataset_descriptor is not None else None
    )
    nested_descriptor_dataset_id = _bounded_catalog_identity(
        nested_descriptor.get("dataset_id") if nested_descriptor is not None else None
    )
    dataset_identity_matches = (
        requested_dataset_id is not None
        and requested_dataset_id == dataset_id
        and requested_dataset_id == descriptor_dataset_id
        and requested_dataset_id == nested_descriptor_dataset_id
    )
    if not dataset_identity_matches:
        failures.append(
            "requested dataset_id does not match the catalog record and both descriptors"
        )

    descriptor_handoff_matches = (
        dataset_descriptor is not None
        and nested_descriptor is not None
        and dataset_descriptor == nested_descriptor
    )
    if not descriptor_handoff_matches:
        failures.append("dataset_descriptor does not equal dataset.descriptor")

    observed_descriptor_sha256 = (
        structured.get("descriptor_sha256") if structured is not None else None
    )
    descriptor_digest_matches = (
        _is_sha256(observed_descriptor_sha256)
        and observed_descriptor_sha256 == computed_descriptor_sha256
    )
    if not descriptor_digest_matches:
        failures.append("descriptor_sha256 does not match the canonical dataset_descriptor")

    passed = not failures
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.scientific-catalog-result",
        passed=passed,
        message=(
            "scientific catalog result proves the exact JARVIS descriptor handoff"
            if passed
            else "scientific catalog result does not prove the exact JARVIS descriptor handoff"
        ),
        evidence={
            "contract": CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID,
            "tool": "scientific_dataset_describe",
            "requested_dataset_id": requested_dataset_id,
            "dataset_id": dataset_id,
            "descriptor_dataset_id": descriptor_dataset_id,
            "nested_descriptor_dataset_id": nested_descriptor_dataset_id,
            "response_schema_version": _bounded_optional_string(response_schema_version, 128),
            "descriptor_schema_version": _bounded_optional_string(descriptor_schema_version, 128),
            "nested_descriptor_schema_version": _bounded_optional_string(
                nested_descriptor_schema_version, 128
            ),
            "schema_versions_match": schema_versions_match,
            "dataset_identity_matches": dataset_identity_matches,
            "dataset_descriptor_handoff_matches": descriptor_handoff_matches,
            "descriptor_sha256": (
                observed_descriptor_sha256 if _is_sha256(observed_descriptor_sha256) else None
            ),
            "computed_descriptor_sha256": computed_descriptor_sha256,
            "descriptor_digest_matches": descriptor_digest_matches,
            "descriptor_digest_error": descriptor_digest_error,
            "output_schema": output_schema_evidence,
            "failures": failures,
        },
    )


def _scientific_catalog_structured_schema_evidence(
    *,
    output_schema: JSON | None,
    structured_value: object,
) -> JSON:
    """Bound and reject non-finite catalog content before schema evaluation."""
    from clio_relay.remote_mcp import MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES

    guard_evidence: JSON = {
        "structured_content_bounded": False,
        "structured_content_finite": False,
        "structured_content_bytes": None,
        "structured_content_bytes_limit": (MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES),
        "structured_content_guard_error": None,
        "schema_evaluated": False,
    }
    try:
        _require_bounded_json_structure(
            structured_value,
            label="scientific catalog structuredContent",
        )
        guard_evidence["structured_content_bounded"] = True
        _require_finite_json(
            structured_value,
            label="scientific catalog structuredContent",
        )
        guard_evidence["structured_content_finite"] = True
        encoded = json.dumps(
            structured_value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        guard_evidence["structured_content_bytes"] = len(encoded)
        if len(encoded) > MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES:
            guard_evidence["structured_content_bounded"] = False
            raise ValueError(
                "remote MCP scientific catalog structuredContent exceeds "
                f"{MAX_REMOTE_MCP_SCIENTIFIC_CATALOG_STRUCTURED_BYTES} bytes"
            )
    except (RecursionError, TypeError, ValueError) as exc:
        guard_evidence["structured_content_guard_error"] = _bounded_diagnostic(str(exc))
        return {
            "schema_present": output_schema is not None,
            "schema_valid": False,
            "schema_sha256": None,
            "structured_content_valid": False,
            "validation_errors": [guard_evidence["structured_content_guard_error"]],
            "validation_errors_truncated": False,
            **guard_evidence,
        }
    schema_evidence = _structured_result_schema_evidence(
        output_schema=output_schema,
        structured_value=structured_value,
    )
    guard_evidence["schema_evaluated"] = True
    return {
        **schema_evidence,
        **guard_evidence,
    }


def _scientific_catalog_descriptor_sha256(descriptor: JSON) -> str:
    """Hash a descriptor with the exact canonical JSON used by clio-kit."""
    payload = json.dumps(
        descriptor,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _bounded_catalog_identity(value: object) -> str | None:
    """Return one bounded printable catalog identity for release evidence."""
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 256
        or not value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return None
    return value
