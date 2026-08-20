"""Per-phase Spack fresh-install transition checks (identity/durable/find/install/locate).

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the individual checks
:func:`~clio_relay.remote_mcp_spack_transition_report.build_remote_mcp_spack_fresh_install_transition_report`
(``remote_mcp_spack_transition_report.py``) assembles into one fail-closed
proof: that all three phases share one immutable route and server artifact
(``_spack_transition_identity_check``), that each phase's job/stdio/artifact
evidence is durable and distinct (``_spack_transition_durable_evidence_check``),
that the exact requested spec was absent before installation
(``_spack_preinstall_absent_check``), that it was installed with reuse
disabled (``_spack_fresh_install_check``), and that the installed DAG hash
resolves to one canonical prefix (``_spack_postinstall_locate_check``) --
plus the shared schema-validated transition-result reader and its pinned
per-tool output schema the three phase checks all call through.

None of these eight names have a caller outside ``remote_mcp.py`` (confirmed
by grep before the move; ``remote_mcp_spack_transition_report.py`` imports
them directly from here, not from ``remote_mcp.py``), so ``remote_mcp.py``
imports them directly rather than re-exporting them.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Literal, cast

from clio_relay.remote_mcp_acceptance_evidence import (
    _acceptance_check_string,
    _acceptance_server_artifact,
    _bounded_evidence_scalar,
    _bounded_optional_string,
    _bounded_spack_package_identity,
    _bounded_transition_arguments,
    _common_string,
    _same_nonempty_strings,
    _transition_call_arguments,
)
from clio_relay.remote_mcp_acceptance_models import (
    MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL,
    RemoteMcpAcceptanceCheck,
    RemoteMcpAcceptanceReport,
    RemoteMcpSpackTransitionArtifactEvidence,
    RemoteMcpSpackTransitionCallEvidence,
    RemoteMcpSpackTransitionStdioEvidence,
    RemoteMcpStructuredResultExpectation,
    _is_canonical_absolute_posix_path,
)
from clio_relay.remote_mcp_schema_validation import (
    _bounded_diagnostic,
    _require_bounded_json_structure,
    _require_finite_json,
)
from clio_relay.remote_mcp_spack_result_validation import (
    _spack_package_matches,
    _spack_package_records,
)
from clio_relay.remote_mcp_stdio_evidence import (
    _as_json,
    _stdio_call_job_id,
    _stdio_initialize_passed,
    _stdio_listed_tool_names,
)
from clio_relay.remote_mcp_structured_result import _structured_result_schema_evidence
from clio_relay.remote_mcp_tool_schema import _is_sha256, _stable_digest

JSON = dict[str, Any]


def _spack_transition_identity_check(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
) -> tuple[RemoteMcpAcceptanceCheck, dict[str, str | None]]:
    """Require all phases to retain one registration, catalog, and wheel identity."""
    reports = (preinstall_report, install_report, postinstall_report)
    scopes = {(report.cluster, report.server_name, report.profile) for report in reports}
    tool_names = tuple(report.remote_tool_name for report in reports)
    reports_passed = all(
        report.passed and all(check.passed for check in report.checks) for report in reports
    )
    registration_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.register", "registration_revision")
        for report in reports
    )
    cluster_route_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.register", "cluster_route_revision")
        for report in reports
    )
    catalog_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.tools-list", "catalog_revision")
        for report in reports
    )
    server_artifacts = tuple(_acceptance_server_artifact(report) for report in reports)
    same_server_artifact = (
        all(artifact is not None for artifact in server_artifacts)
        and server_artifacts[0] == server_artifacts[1] == server_artifacts[2]
    )
    server_artifact_sha256 = (
        _stable_digest(server_artifacts[1]) if server_artifacts[1] is not None else None
    )
    revision_matches = {
        "registration": _same_nonempty_strings(registration_revisions),
        "cluster_route": _same_nonempty_strings(cluster_route_revisions),
        "catalog": _same_nonempty_strings(catalog_revisions),
    }
    expected_tools = ("spack_find", "spack_install", "spack_locate")
    passed = (
        reports_passed
        and len(scopes) == 1
        and tool_names == expected_tools
        and all(revision_matches.values())
        and same_server_artifact
    )
    identity: dict[str, str | None] = {
        "registration_revision": _common_string(registration_revisions),
        "cluster_route_revision": _common_string(cluster_route_revisions),
        "catalog_revision": _common_string(catalog_revisions),
        "server_artifact_sha256": server_artifact_sha256,
    }
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-transition-identity",
            passed=passed,
            message=(
                "all Spack phases share one passing route and verified server artifact"
                if passed
                else "Spack transition phases do not share one passing immutable route"
            ),
            evidence={
                "underlying_reports_passed": reports_passed,
                "scopes": [list(scope) for scope in sorted(scopes)],
                "tool_names": list(tool_names),
                "expected_tool_names": list(expected_tools),
                "registration_revisions": list(registration_revisions),
                "cluster_route_revisions": list(cluster_route_revisions),
                "catalog_revisions": list(catalog_revisions),
                "revision_matches": revision_matches,
                "same_server_artifact": same_server_artifact,
                "server_artifact_sha256": server_artifact_sha256,
            },
        ),
        identity,
    )


def _spack_transition_durable_evidence_check(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
) -> RemoteMcpAcceptanceCheck:
    """Require distinct succeeded jobs, packaged stdio, and hashed durable artifacts."""
    reports = (preinstall_report, install_report, postinstall_report)
    required_kinds = {"stdout", "stderr", "mcp_result", "provenance"}
    jobs: list[str] = []
    phases: JSON = {}
    all_artifact_ids: list[str] = []
    passed = True
    for phase, report in zip(("preinstall", "install", "postinstall"), reports, strict=True):
        raw_job_id = report.call_job.get("job_id")
        job_id = raw_job_id if isinstance(raw_job_id, str) else None
        if job_id is not None:
            jobs.append(job_id)
        relevant_artifacts = report.artifacts[:MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL]
        artifact_kinds: set[str] = set()
        artifacts_valid = len(report.artifacts) <= MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL
        for artifact in relevant_artifacts:
            kind = artifact.get("kind")
            artifact_id = artifact.get("artifact_id")
            if isinstance(kind, str):
                artifact_kinds.add(kind)
            if isinstance(artifact_id, str):
                all_artifact_ids.append(artifact_id)
            artifacts_valid = artifacts_valid and (
                isinstance(artifact_id, str)
                and artifact.get("job_id") == job_id
                and _is_sha256(artifact.get("sha256"))
            )
        stdio_valid = (
            bool(report.mcp_stdio)
            and _stdio_initialize_passed(report.mcp_stdio)
            and report.virtual_alias is not None
            and report.virtual_alias in _stdio_listed_tool_names(report.mcp_stdio)
            and _stdio_call_job_id(report.mcp_stdio) == job_id
        )
        phase_passed = (
            job_id is not None
            and report.call_job.get("state") == "succeeded"
            and required_kinds.issubset(artifact_kinds)
            and artifacts_valid
            and stdio_valid
        )
        passed = passed and phase_passed
        phases[phase] = {
            "job_id": job_id,
            "state": report.call_job.get("state"),
            "artifact_kinds": sorted(artifact_kinds),
            "artifact_count": len(report.artifacts),
            "artifacts_valid": artifacts_valid,
            "stdio_valid": stdio_valid,
            "passed": phase_passed,
        }
    distinct_jobs = len(jobs) == 3 and len(set(jobs)) == 3
    distinct_artifacts = len(all_artifact_ids) == len(set(all_artifact_ids))
    passed = passed and distinct_jobs and distinct_artifacts
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-transition-durable-evidence",
        passed=passed,
        message=(
            "three distinct succeeded jobs retain packaged stdio and durable artifacts"
            if passed
            else "Spack transition jobs, stdio, or durable artifacts are incomplete"
        ),
        evidence={
            "required_artifact_kinds": sorted(required_kinds),
            "job_ids": jobs,
            "distinct_job_ids": distinct_jobs,
            "distinct_artifact_ids": distinct_artifacts,
            "phases": phases,
        },
    )


def _spack_preinstall_absent_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Prove an exact requested spec was absent immediately before installation."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_find",
    )
    arguments = _transition_call_arguments(report)
    expected_spec = cast(str, expectation.requested_spec)
    packages = structured.get("packages") if structured is not None else None
    count = structured.get("count") if structured is not None else None
    query = structured.get("query") if structured is not None else None
    if arguments.get("query") != expected_spec:
        failures.append("preinstall find call did not query the exact requested spec")
    if query != expected_spec:
        failures.append("preinstall find result query does not match the requested spec")
    if not isinstance(count, int) or isinstance(count, bool) or count != 0:
        failures.append("preinstall find result count is not zero")
    if packages != []:
        failures.append("preinstall find result packages is not an empty array")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "query": _bounded_evidence_scalar(query),
        "count": count,
        "packages": [],
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-preinstall-absent",
            passed=passed,
            message=(
                "exact requested spec was absent immediately before installation"
                if passed
                else "preinstall absence for the exact requested spec was not proven"
            ),
            evidence={
                "expected_requested_spec": expected_spec,
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_find"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
    )


def _spack_fresh_install_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Prove one exact package identity was installed with reuse disabled."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_install",
    )
    arguments = _transition_call_arguments(report)
    expected_spec = cast(str, expectation.requested_spec)
    packages = (
        _spack_package_records(structured.get("packages")) if structured is not None else None
    )
    duration = structured.get("duration_seconds") if structured is not None else None
    if arguments.get("spec") != expected_spec or arguments.get("reuse") is not False:
        failures.append("install call did not submit the exact spec with reuse=false")
    if structured is None or structured.get("requested_spec") != expected_spec:
        failures.append("install result requested_spec does not match the exact submitted spec")
    if structured is None or structured.get("reuse") is not False:
        failures.append("install result does not prove reuse=false")
    if structured is None or structured.get("status") != "installed":
        failures.append("install result status is not installed")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        failures.append("install duration is not a finite non-negative number")
    package = packages[0] if packages is not None and len(packages) == 1 else None
    if package is None or not _spack_package_matches(package, expectation):
        failures.append("install result does not contain exactly one expected package identity")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "requested_spec": _bounded_evidence_scalar(
            structured.get("requested_spec") if structured is not None else None
        ),
        "reuse": structured.get("reuse") if structured is not None else None,
        "status": _bounded_evidence_scalar(
            structured.get("status") if structured is not None else None
        ),
        "duration_seconds": duration,
        "package": _bounded_spack_package_identity(package),
        "package_count": len(packages) if packages is not None else None,
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-fresh-install",
            passed=passed,
            message=(
                "exact package identity was installed with reuse disabled"
                if passed
                else "fresh non-reusing installation of the exact package was not proven"
            ),
            evidence={
                "expected": {
                    "requested_spec": expected_spec,
                    "package_name": expectation.package_name,
                    "dag_hash": expectation.dag_hash,
                    "reuse": False,
                    "status": "installed",
                },
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_install"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
    )


def _spack_postinstall_locate_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON, object]:
    """Prove the exact installed DAG hash resolves to one canonical prefix."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_locate",
    )
    arguments = _transition_call_arguments(report)
    exact_hash_spec = f"/{expectation.dag_hash}"
    requested_spec = structured.get("requested_spec") if structured is not None else None
    load_spec = structured.get("load_spec") if structured is not None else None
    prefix = structured.get("prefix") if structured is not None else None
    package = _as_json(structured.get("package")) if structured is not None else None
    if arguments.get("spec") != exact_hash_spec:
        failures.append("postinstall locate call did not query the exact /dag_hash")
    if requested_spec != exact_hash_spec or load_spec != exact_hash_spec:
        failures.append("postinstall locate result is not bound to the exact /dag_hash")
    if package is None or not _spack_package_matches(package, expectation):
        failures.append("postinstall locate result package identity does not match")
    if not _is_canonical_absolute_posix_path(prefix):
        failures.append("postinstall locate prefix is not a canonical absolute POSIX path")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "requested_spec": _bounded_evidence_scalar(requested_spec),
        "load_spec": _bounded_evidence_scalar(load_spec),
        "prefix": _bounded_evidence_scalar(prefix),
        "package": _bounded_spack_package_identity(package),
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-postinstall-locate",
            passed=passed,
            message=(
                "exact installed DAG hash resolves to one canonical prefix"
                if passed
                else "postinstall locate did not prove the exact installed DAG identity"
            ),
            evidence={
                "expected": {
                    "requested_spec": exact_hash_spec,
                    "package_name": expectation.package_name,
                    "dag_hash": expectation.dag_hash,
                },
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_locate"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
        prefix,
    )


def _spack_transition_structured_result(
    protocol_result: JSON | None,
    *,
    tool: Literal["spack_find", "spack_install", "spack_locate"],
) -> tuple[JSON | None, JSON, list[str]]:
    """Return a schema-validated transition result without retaining MCP text output."""
    failures: list[str] = []
    if protocol_result is None:
        failures.append("protocol result is missing")
        structured_value: object = None
    else:
        try:
            _require_bounded_json_structure(protocol_result, label="transition protocol result")
            _require_finite_json(protocol_result, label="transition protocol result")
        except (RecursionError, ValueError) as exc:
            failures.append(_bounded_diagnostic(str(exc)))
            structured_value = None
        else:
            if protocol_result.get("isError") is True:
                failures.append("protocol result reports isError=true")
            structured_value = protocol_result.get("structuredContent")
    structured = _as_json(structured_value)
    schema_evidence = _structured_result_schema_evidence(
        output_schema=_spack_transition_output_schema(tool),
        structured_value=structured_value,
    )
    if schema_evidence.get("structured_content_valid") is not True:
        failures.append("structuredContent does not satisfy the pinned Spack result schema")
    expected_operation = tool.removeprefix("spack_")
    if structured is None:
        failures.append("protocol result has no structuredContent object")
    else:
        if structured.get("schema_version") != "spack.mcp.result.v1":
            failures.append("structured result schema_version is not spack.mcp.result.v1")
        if structured.get("operation") != expected_operation:
            failures.append("structured result operation does not match the transition phase")
    return structured, schema_evidence, failures


def _spack_transition_output_schema(
    tool: Literal["spack_find", "spack_install", "spack_locate"],
) -> JSON:
    """Return the strict result schema pinned by the clio-kit Spack user contract."""
    nullable_string: JSON = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    package_schema: JSON = {
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
    common: JSON = {
        "schema_version": {"type": "string", "const": "spack.mcp.result.v1"},
        "operation": {"type": "string", "const": tool.removeprefix("spack_")},
    }
    if tool == "spack_find":
        return {
            "type": "object",
            "properties": {
                **common,
                "query": deepcopy(nullable_string),
                "packages": {"type": "array", "items": package_schema},
                "count": {"type": "integer"},
            },
            "required": ["count"],
            "additionalProperties": False,
        }
    if tool == "spack_install":
        return {
            "type": "object",
            "properties": {
                **common,
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
    return {
        "type": "object",
        "properties": {
            **common,
            "requested_spec": {"type": "string"},
            "load_spec": {"type": "string"},
            "package": package_schema,
            "prefix": {"type": "string"},
        },
        "required": ["requested_spec", "load_spec", "package", "prefix"],
        "additionalProperties": False,
    }


def _spack_transition_call_evidence(
    *,
    report: RemoteMcpAcceptanceReport,
    phase: Literal["preinstall", "install", "postinstall"],
    structured_result: JSON,
) -> RemoteMcpSpackTransitionCallEvidence:
    """Project one ordinary acceptance report into bounded transition evidence."""
    artifacts = [
        RemoteMcpSpackTransitionArtifactEvidence(
            artifact_id=_bounded_optional_string(artifact.get("artifact_id"), 1_024),
            job_id=_bounded_optional_string(artifact.get("job_id"), 1_024),
            kind=_bounded_optional_string(artifact.get("kind"), 128),
            sha256=_bounded_optional_string(artifact.get("sha256"), 64),
            uri=_bounded_optional_string(artifact.get("uri"), 4_096),
        )
        for artifact in report.artifacts[:MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL]
    ]
    alias = report.virtual_alias
    return RemoteMcpSpackTransitionCallEvidence(
        phase=phase,
        report_passed=report.passed and all(check.passed for check in report.checks),
        cluster=report.cluster,
        server_name=report.server_name,
        profile=report.profile,
        remote_tool_name=report.remote_tool_name,
        virtual_alias=alias,
        job_id=_bounded_optional_string(report.call_job.get("job_id"), 1_024),
        state=_bounded_optional_string(report.call_job.get("state"), 128),
        arguments=_bounded_transition_arguments(
            _transition_call_arguments(report),
            report.remote_tool_name,
        ),
        artifacts=artifacts,
        artifacts_truncated=(len(report.artifacts) > MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL),
        stdio=RemoteMcpSpackTransitionStdioEvidence(
            boundary=_bounded_optional_string(report.mcp_stdio.get("boundary"), 128),
            returncode=(
                cast(int, report.mcp_stdio["returncode"])
                if isinstance(report.mcp_stdio.get("returncode"), int)
                and not isinstance(report.mcp_stdio.get("returncode"), bool)
                else None
            ),
            initialize_passed=_stdio_initialize_passed(report.mcp_stdio),
            tools_list_passed=(
                alias is not None and alias in _stdio_listed_tool_names(report.mcp_stdio)
            ),
            call_job_id=_bounded_optional_string(_stdio_call_job_id(report.mcp_stdio), 1_024),
        ),
        structured_result=structured_result,
    )
