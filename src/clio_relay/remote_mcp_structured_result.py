"""Structured-result acceptance checks against an explicit semantic contract.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the operator-declared
:class:`~clio_relay.remote_mcp_acceptance_models.RemoteMcpStructuredResultExpectation`
dispatcher (:func:`build_remote_mcp_structured_result_check`), the Spack v2/
v2.1/v2.3 structured-result semantic check it dispatches to today (dispatch
on ``expectation.contract`` -- the design doc names a scientific-catalog
result-expectation family as future work, not yet declared here), and the
shared cached-``outputSchema`` validation the fresh-install Spack transition
checks (``remote_mcp_spack_transition_checks.py``) and the scientific-catalog
result check (``remote_mcp_scientific_catalog_result.py``) both reuse.

``build_remote_mcp_structured_result_check`` is re-exported under its
original name (tests and ``build_remote_mcp_acceptance_report`` in
``remote_mcp_acceptance_report.py`` import it directly from
``clio_relay.remote_mcp``). ``_spack_structured_result_check`` and
``_structured_result_schema_evidence`` are private with no caller outside
``remote_mcp.py`` (other split owner modules import the latter directly from
here), so they are imported directly rather than re-exported.

``build_remote_mcp_structured_result_check`` reads
``CLIO_KIT_SPACK_USER_CONTRACT_IDS``, and ``_structured_result_schema_evidence``
reads ``MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS`` -- contract-pin/bound constants
that still live in ``remote_mcp.py`` (unsequenced, post-campaign per the
design doc). A module-scope import back into ``remote_mcp.py`` (which
imports this module for the re-export above) would be a load-order circular
import; importing them inside the function body instead is the proven idiom
for that shape (see ``remote_mcp_wire_schemas.py``'s own
``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

from typing import Any, cast

from jsonschema import Draft202012Validator

from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpAcceptanceCheck,
    RemoteMcpStructuredResultExpectation,
)
from clio_relay.remote_mcp_schema_validation import (
    _JSON_SCHEMA_VALIDATORS,
    _bounded_diagnostic,
    _JsonSchemaInstanceValidator,
    _require_bounded_json_structure,
    _require_finite_json,
    _validate_json_schema,
)
from clio_relay.remote_mcp_spack_result_validation import (
    _validate_spack_find_result,
    _validate_spack_install_result,
    _validate_spack_locate_result,
)
from clio_relay.remote_mcp_stdio_evidence import _as_json
from clio_relay.remote_mcp_tool_schema import _stable_digest

JSON = dict[str, Any]


def build_remote_mcp_structured_result_check(
    *,
    expectation: RemoteMcpStructuredResultExpectation,
    remote_tool_name: str,
    arguments: object,
    protocol_result: JSON | None,
    output_schema: JSON | None,
) -> RemoteMcpAcceptanceCheck:
    """Validate a remote structured result against an explicit semantic contract."""
    from clio_relay.remote_mcp import CLIO_KIT_SPACK_USER_CONTRACT_IDS

    if expectation.contract in CLIO_KIT_SPACK_USER_CONTRACT_IDS:
        return _spack_structured_result_check(
            expectation=expectation,
            remote_tool_name=remote_tool_name,
            arguments=arguments,
            protocol_result=protocol_result,
            output_schema=output_schema,
        )
    raise ValueError(f"unsupported structured result contract: {expectation.contract}")


def _spack_structured_result_check(
    *,
    expectation: RemoteMcpStructuredResultExpectation,
    remote_tool_name: str,
    arguments: object,
    protocol_result: JSON | None,
    output_schema: JSON | None,
) -> RemoteMcpAcceptanceCheck:
    """Validate the exact clio-kit Spack v2 or v2.1 result semantics for one operation."""
    failures: list[str] = []
    typed_arguments = _as_json(arguments) or {}
    structured_value = (
        protocol_result.get("structuredContent") if protocol_result is not None else None
    )
    structured = _as_json(structured_value)
    output_schema_evidence = _structured_result_schema_evidence(
        output_schema=output_schema,
        structured_value=structured_value,
    )
    observed: JSON = {
        "structured_content_present": structured is not None,
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
    }
    if remote_tool_name != expectation.tool:
        failures.append("called tool does not match the configured result expectation")
    if output_schema_evidence["schema_present"] is not True:
        failures.append("cached tool outputSchema is absent")
    elif output_schema_evidence["schema_valid"] is not True:
        failures.append("cached tool outputSchema is invalid")
    elif output_schema_evidence["structured_content_valid"] is not True:
        failures.append("structuredContent does not satisfy the cached tool outputSchema")
    if structured is None:
        failures.append("protocol result has no structuredContent object")
    else:
        if structured.get("schema_version") != "spack.mcp.result.v1":
            failures.append("structured result schema_version is not spack.mcp.result.v1")
        expected_operation = expectation.tool.removeprefix("spack_")
        if structured.get("operation") != expected_operation:
            failures.append("structured result operation does not match the called tool")
        if expectation.tool == "spack_find":
            _validate_spack_find_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
        elif expectation.tool == "spack_locate":
            _validate_spack_locate_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
        else:
            _validate_spack_install_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
    passed = not failures
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.structured-result",
        passed=passed,
        message=(
            "structured MCP result matches the configured semantic expectations"
            if passed
            else "structured MCP result does not match the configured semantic expectations"
        ),
        evidence={
            "contract": expectation.contract,
            "tool": expectation.tool,
            "expected": expectation.model_dump(mode="json"),
            "observed": observed,
            "output_schema": output_schema_evidence,
            "failures": failures,
        },
    )


def _structured_result_schema_evidence(
    *,
    output_schema: JSON | None,
    structured_value: object,
) -> JSON:
    """Validate one result against its cached schema and return bounded evidence."""
    evidence: JSON = {
        "schema_present": output_schema is not None,
        "schema_valid": False,
        "schema_sha256": None,
        "structured_content_valid": False,
        "validation_errors": [],
        "validation_errors_truncated": False,
    }
    if output_schema is None:
        return evidence
    try:
        _require_bounded_json_structure(output_schema, label="outputSchema")
        _require_finite_json(output_schema, label="outputSchema")
        _validate_json_schema(output_schema, label="outputSchema")
    except (RecursionError, ValueError) as exc:
        evidence["validation_errors"] = [_bounded_diagnostic(str(exc))]
        return evidence
    evidence["schema_sha256"] = _stable_digest(output_schema)
    evidence["schema_valid"] = True
    declared_dialect = output_schema.get("$schema")
    validator_type = (
        _JSON_SCHEMA_VALIDATORS.get(declared_dialect.rstrip("#"), Draft202012Validator)
        if isinstance(declared_dialect, str)
        else Draft202012Validator
    )
    errors: list[str] = []
    truncated = False
    from clio_relay.remote_mcp import MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS

    try:
        validator = cast(_JsonSchemaInstanceValidator, validator_type(output_schema))
        for index, error in enumerate(validator.iter_errors(structured_value)):
            if index >= MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS:
                truncated = True
                break
            path = "/".join(str(part) for part in error.absolute_path)
            prefix = f"/{path}: " if path else ""
            errors.append(_bounded_diagnostic(f"{prefix}{error.message}"))
    except Exception as exc:  # A broken external reference must fail closed as evidence.
        errors.append(
            _bounded_diagnostic(f"outputSchema evaluation failed: {type(exc).__name__}: {exc}")
        )
    evidence["structured_content_valid"] = not errors and not truncated
    evidence["validation_errors"] = errors
    evidence["validation_errors_truncated"] = truncated
    return evidence
