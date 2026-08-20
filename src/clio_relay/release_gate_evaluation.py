"""Evaluate live validation reports against a release-gate policy (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). This module owns the
gate-evaluation concern: :func:`evaluate_release_gate` is the entry point a
release cut calls with a :class:`~clio_relay.validation_schema.
ReleaseGatePolicy` and the collected
:class:`~clio_relay.validation_schema.LiveValidationReport` evidence, and
returns a :class:`~clio_relay.validation_schema.ReleaseGateResult` naming
exactly which requirements passed and why any that failed did not -- never a
bare boolean. A requirement is satisfied either by one report that alone
proves every clause (:func:`report_requirement_failures`), or, when the
requirement names an ``evidence_group_resource_kind``, by combining several
reports that share one stable resource and collectively prove the union of
clauses without mixing evidence across different builds or artifacts
(:func:`combined_evidence_identity_failures`). Per-report launcher and
producer-identity trust (:func:`has_complete_producer_identity`,
:func:`launcher_identity_failures`) is checked here too since it gates every
requirement uniformly rather than being one requirement's own concern.

Physical target-identity binding and stateful resource/JARVIS-execution
matching are large enough concerns to have their own owner modules --
:mod:`clio_relay.release_gate_targets` and
:mod:`clio_relay.release_gate_resources` -- which this module calls into.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

from clio_relay.errors import ConfigurationError
from clio_relay.release_gate_resources import (
    jarvis_execution_identity_failures,
    required_resource_failures,
    requirement_resource_scope_failures,
)
from clio_relay.release_gate_targets import (
    policy_target_identity_digests,
    report_set_target_identities,
    report_target_identity,
)
from clio_relay.spack_transition_checks import spack_fresh_install_transition_failures
from clio_relay.validation_schema import (
    EvidenceTrust,
    LiveValidationReport,
    ReleaseGatePolicy,
    ReleaseGateRequirement,
    ReleaseGateResult,
    ValidationStatus,
)


def evaluate_release_gate(
    policy: ReleaseGatePolicy,
    reports: Iterable[LiveValidationReport],
    *,
    expected_artifact_sha256: str | None = None,
) -> ReleaseGateResult:
    """Evaluate immutable-artifact reports without inferring untested claims."""
    all_reports = list(reports)
    expected_digest = validated_sha256(expected_artifact_sha256)
    if policy_requires_expected_artifact_digest(policy) and expected_digest is None:
        raise ConfigurationError(
            f"{policy.artifact_stage} gates requiring artifact SHA-256 evidence require an "
            "independently computed expected artifact SHA-256"
        )
    matrix = policy.acceptance_matrix
    if policy.acceptance_matrix_path is not None and matrix is None:
        raise ConfigurationError(
            "release gate policy acceptance matrix was not digest-verified by the policy loader"
        )
    matrix_stage: dict[str, object] | None = None
    matrix_pairs: list[tuple[dict[str, object], LiveValidationReport]] = []
    matrix_failures: list[str] = []
    if matrix is not None:
        stages = cast(list[dict[str, object]], matrix["stages"])
        matching_stages = [
            stage for stage in stages if stage.get("artifact_stage") == policy.artifact_stage
        ]
        if len(matching_stages) != 1:
            raise ConfigurationError(
                f"release acceptance matrix does not define artifact stage {policy.artifact_stage}"
            )
        matrix_stage = matching_stages[0]
        prefix = cast(str, matrix_stage["filename_prefix"])
        matrix_reports = cast(list[dict[str, object]], matrix["reports"])
        expected_names = [f"{prefix}-{entry['id']}.json" for entry in matrix_reports]
        nonlocal_reports = [report for report in all_reports if report.cluster != "local"]
        reports_by_name: dict[str, LiveValidationReport] = {}
        duplicate_names: set[str] = set()
        missing_source_ids: list[str] = []
        for report in nonlocal_reports:
            if report.source_path is None:
                missing_source_ids.append(report.report_id)
                continue
            name = report.source_path.name
            if name in reports_by_name:
                duplicate_names.add(name)
            reports_by_name[name] = report
        if missing_source_ids:
            matrix_failures.append(
                "matrix reports were not loaded from provenance-bearing paths: "
                f"{sorted(missing_source_ids)}"
            )
        if duplicate_names:
            matrix_failures.append(f"duplicate matrix report filenames: {sorted(duplicate_names)}")
        actual_names = set(reports_by_name)
        if len(nonlocal_reports) != len(expected_names) or actual_names != set(expected_names):
            matrix_failures.append(
                "non-local report filenames do not exactly match the acceptance matrix: "
                f"missing={sorted(set(expected_names) - actual_names)}, "
                f"unexpected={sorted(actual_names - set(expected_names))}"
            )
        document_ids = [report.report_id for report in nonlocal_reports]
        if len(document_ids) != len(set(document_ids)):
            matrix_failures.append(
                "acceptance matrix reports contain duplicate document report ids"
            )
        for entry, filename in zip(matrix_reports, expected_names, strict=True):
            report = reports_by_name.get(filename)
            if report is None:
                continue
            if report.cluster != entry["cluster"] or report.scenario != entry["scenario"]:
                matrix_failures.append(
                    f"{filename} cluster/scenario does not match acceptance matrix entry "
                    f"{entry['id']}"
                )
            if report.software.version != policy.release_version:
                matrix_failures.append(
                    f"{filename} does not identify clio-relay {policy.release_version}"
                )
            matrix_pairs.append((entry, report))

    candidates = [
        report for report in all_reports if report.software.version == policy.release_version
    ]
    policy_target_identity_sha256 = policy_target_identity_digests(policy)
    target_identity_sha256: dict[str, str] = {}
    target_identity_failures: list[str] = []
    if policy.require_target_identity:
        target_identity_sha256, target_identity_failures = report_set_target_identities(
            policy,
            candidates,
        )
    satisfied: list[str] = []
    unsatisfied: dict[str, list[str]] = {}
    used_report_ids: set[str] = set()
    for requirement in policy.requirements:
        reasons: set[str] = set()
        matching_report: LiveValidationReport | None = None
        for report in candidates:
            report_reasons = report_requirement_failures(
                policy,
                requirement,
                report,
                expected_artifact_sha256=expected_digest,
            )
            if not report_reasons:
                matching_report = report
                break
            reasons.update(report_reasons)
        if matching_report is not None:
            satisfied.append(requirement.requirement_id)
            used_report_ids.add(matching_report.report_id)
            continue
        eligible = [
            report
            for report in candidates
            if not report_requirement_failures(
                policy,
                requirement,
                report,
                include_requirement_evidence=False,
                expected_artifact_sha256=expected_digest,
            )
        ]
        evidence_groups = requirement_evidence_groups(requirement, eligible)
        group_failures: list[tuple[int, list[str], list[str], list[str], list[str]]] = []
        matched_group: list[LiveValidationReport] | None = None
        for group in evidence_groups:
            combined_checks = {
                check.check_id
                for report in group
                for check in report.checks
                if check.status is ValidationStatus.PASSED and check.evidence
            }
            combined_resources = {
                resource.kind
                for report in group
                for resource in report.resources
                if resource.cluster == requirement.cluster
            }
            missing_checks = sorted(set(requirement.required_checks) - combined_checks)
            missing_resources = sorted(
                set(requirement.required_resource_kinds) - combined_resources
            )
            resource_predicate_failures = required_resource_failures(
                requirement,
                [resource for report in group for resource in report.resources],
                expected_cluster=requirement.cluster,
            )
            resource_scope_failures = requirement_resource_scope_failures(
                requirement,
                [resource for report in group for resource in report.resources],
            )
            identity_failures = combined_evidence_identity_failures(
                policy,
                requirement,
                group,
                expected_artifact_sha256=expected_digest,
            )
            if (
                not missing_checks
                and not missing_resources
                and not resource_predicate_failures
                and not resource_scope_failures
                and not identity_failures
            ):
                matched_group = group
                break
            group_failures.append(
                (
                    len(missing_checks)
                    + len(missing_resources)
                    + len(resource_predicate_failures)
                    + len(resource_scope_failures)
                    + len(identity_failures),
                    missing_checks,
                    missing_resources,
                    [*resource_scope_failures, *resource_predicate_failures],
                    identity_failures,
                )
            )
        if matched_group is not None:
            satisfied.append(requirement.requirement_id)
            used_report_ids.update(report.report_id for report in matched_group)
            continue
        if eligible and not evidence_groups:
            if requirement.evidence_group_resource_kind is None:
                reasons.add("requirement evidence must be satisfied by one coherent report")
            else:
                reasons.add(
                    "no reports share required evidence group resource kind "
                    f"{requirement.evidence_group_resource_kind}"
                )
        if group_failures:
            (
                _,
                missing_checks,
                missing_resources,
                resource_predicate_failures,
                identity_failures,
            ) = min(group_failures, key=lambda item: item[0])
            if missing_checks:
                reasons.add(f"missing passed checks across reports: {missing_checks}")
            if missing_resources:
                reasons.add(f"missing resource evidence across reports: {missing_resources}")
            reasons.update(resource_predicate_failures)
            reasons.update(identity_failures)
        unsatisfied[requirement.requirement_id] = sorted(reasons) or [
            f"no report for clio-relay {policy.release_version}"
        ]
    used_reports = [report for report in candidates if report.report_id in used_report_ids]
    nonlocal_commits = {
        report.software.commit
        for report in used_reports
        if report.cluster != "local" and report.software.commit is not None
    }
    if policy.require_commit and len(nonlocal_commits) > 1:
        unsatisfied["release-artifact-identity"] = [
            "used non-local reports identify different source commits"
        ]
    if target_identity_failures:
        unsatisfied["target-identity"] = target_identity_failures
    if policy.release_blockers:
        unsatisfied["declared-release-blockers"] = list(policy.release_blockers)
    if matrix_pairs:
        unused_matrix_ids = [
            cast(str, entry["id"])
            for entry, report in matrix_pairs
            if report.report_id not in used_report_ids
        ]
        if unused_matrix_ids:
            matrix_failures.append(
                "acceptance matrix reports were not used by any policy requirement: "
                f"{unused_matrix_ids}"
            )
    if matrix_failures:
        unsatisfied["acceptance-matrix"] = matrix_failures
    return ReleaseGateResult(
        release_version=policy.release_version,
        artifact_sha256=expected_digest,
        acceptance_matrix_schema_version=(
            cast(str, matrix["schema_version"]) if matrix is not None else None
        ),
        acceptance_matrix_release_version=(
            cast(str, matrix["release_version"]) if matrix is not None else None
        ),
        acceptance_matrix_sha256=(
            cast(str, matrix["matrix_sha256"]) if matrix is not None else None
        ),
        acceptance_matrix_stage=(
            cast(str, matrix_stage["name"]) if matrix_stage is not None else None
        ),
        acceptance_report_ids=[cast(str, entry["id"]) for entry, _ in matrix_pairs],
        acceptance_report_document_ids=[report.report_id for _, report in matrix_pairs],
        policy_target_identity_sha256=policy_target_identity_sha256,
        target_identity_sha256=target_identity_sha256,
        passed=not unsatisfied,
        satisfied_requirements=satisfied,
        unsatisfied_requirements=unsatisfied,
        report_ids=sorted(used_report_ids),
    )


def validated_sha256(value: str | None) -> str | None:
    """Normalize and validate an independently computed SHA-256 digest."""
    if value is None:
        return None
    normalized = value.strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ConfigurationError("expected artifact SHA-256 must be 64 hexadecimal characters")
    return normalized


def policy_requires_expected_artifact_digest(policy: ReleaseGatePolicy) -> bool:
    """Return whether any effective gate requirement needs an external artifact digest."""
    if policy.require_artifact_sha256:
        return True
    return any(requirement.require_artifact_sha256 is True for requirement in policy.requirements)


def combined_evidence_identity_failures(
    policy: ReleaseGatePolicy,
    requirement: ReleaseGateRequirement,
    reports: list[LiveValidationReport],
    *,
    expected_artifact_sha256: str | None,
) -> list[str]:
    """Reject evidence aggregation across different builds or release artifacts."""
    failures: list[str] = []
    commits = {report.software.commit for report in reports if report.software.commit is not None}
    if policy.require_commit and len(commits) > 1:
        failures.append("combined reports identify different source commits")
    require_artifact_sha256 = (
        policy.require_artifact_sha256
        if requirement.require_artifact_sha256 is None
        else requirement.require_artifact_sha256
    )
    artifact_hashes = {
        report.install_source.artifact_sha256
        for report in reports
        if report.install_source.artifact_sha256 is not None
    }
    if require_artifact_sha256 and len(artifact_hashes) > 1:
        failures.append("combined reports identify different tested artifact SHA-256 values")
    if expected_artifact_sha256 is not None and any(
        report.cluster != "local"
        and report.install_source.artifact_sha256 != expected_artifact_sha256
        for report in reports
    ):
        failures.append("combined reports do not identify the expected candidate artifact")
    return failures


def requirement_evidence_groups(
    requirement: ReleaseGateRequirement,
    reports: list[LiveValidationReport],
) -> list[list[LiveValidationReport]]:
    """Group multi-report evidence by a shared stable resource when required."""
    kind = requirement.evidence_group_resource_kind
    if kind is None:
        return []
    grouped: dict[str, list[LiveValidationReport]] = {}
    for report in reports:
        resource_ids = {
            resource.resource_id for resource in report.resources if resource.kind == kind
        }
        if len(resource_ids) != 1:
            continue
        resource_id = next(iter(resource_ids))
        grouped.setdefault(resource_id, []).append(report)
    return list(grouped.values())


def report_requirement_failures(
    policy: ReleaseGatePolicy,
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
    *,
    include_requirement_evidence: bool = True,
    expected_artifact_sha256: str | None = None,
) -> list[str]:
    failures: list[str] = []
    allowed_sources = requirement.allowed_install_sources or policy.allowed_install_sources
    allowed_launchers = requirement.allowed_launchers or policy.allowed_launchers
    require_released = (
        policy.require_released_artifact
        if requirement.require_released_artifact is None
        else requirement.require_released_artifact
    )
    require_artifact_sha256 = (
        policy.require_artifact_sha256
        if requirement.require_artifact_sha256 is None
        else requirement.require_artifact_sha256
    )
    if report.cluster != requirement.cluster:
        failures.append(f"requires cluster {requirement.cluster}")
    if report.scenario not in requirement.scenarios:
        failures.append(f"requires scenario in {requirement.scenarios}")
    if report.status is not ValidationStatus.PASSED:
        failures.append("report did not pass")
    if report.cluster != "local" and not has_complete_producer_identity(report.evidence_trust):
        failures.append(
            "non-local report omits authenticated producer GitHub identity or invocation id"
        )
    if report.cluster != "local":
        failures.extend(launcher_identity_failures(policy, report))
    if report.install_source.kind not in allowed_sources:
        failures.append(
            f"install source {report.install_source.kind.value} is not release-approved"
        )
    if report.install_source.detected_kind not in allowed_sources:
        failures.append(
            "detected install source "
            f"{report.install_source.detected_kind.value} is not release-approved"
        )
    if report.install_source.launcher not in allowed_launchers:
        failures.append(f"launcher {report.install_source.launcher} is not release-approved")
    if (
        report.install_source.launcher in {"uv-tool", "uvx"}
        and not report.install_source.launcher_verified
    ):
        failures.append("report does not contain a process-observed uv launcher receipt")
    if require_released and not report.install_source.released_artifact:
        failures.append("report does not prove a released artifact")
    if require_released and not report.install_source.artifact_identity_verified:
        failures.append("report does not bind the running distribution to the released wheel")
    if require_artifact_sha256 and report.install_source.artifact_sha256 is None:
        failures.append("report does not identify the tested artifact SHA-256")
    if (
        expected_artifact_sha256 is not None
        and report.cluster != "local"
        and report.install_source.artifact_sha256 != expected_artifact_sha256
    ):
        failures.append(
            "tested artifact SHA-256 does not match the immutable candidate: "
            f"{report.install_source.artifact_sha256 or 'missing'}"
        )
    if (
        expected_artifact_sha256 is not None
        and report.cluster != "local"
        and not report.install_source.artifact_identity_verified
    ):
        failures.append("running distribution is not bound to the expected wheel bytes")
    if policy.require_clean_build and report.software.dirty is not False:
        failures.append("report does not prove a clean build")
    if policy.require_commit and report.software.commit is None:
        failures.append("report does not identify a source commit")
    if policy.require_exact_tag and report.software.tag != f"v{policy.release_version}":
        failures.append(
            f"report source tag must be v{policy.release_version}, got {report.software.tag}"
        )
    if report.install_source.distribution_version != policy.release_version:
        failures.append(
            "installed distribution version does not match the release policy: "
            f"{report.install_source.distribution_version}"
        )
    if policy.require_target_identity and report.cluster != "local":
        _, identity_failures = report_target_identity(
            report,
            policy.targets.get(report.cluster),
        )
        failures.extend(identity_failures)
    failures.extend(requirement_resource_scope_failures(requirement, report.resources))
    if include_requirement_evidence:
        passed_checks = {
            check.check_id
            for check in report.checks
            if check.status is ValidationStatus.PASSED and check.evidence
        }
        missing_checks = sorted(set(requirement.required_checks) - passed_checks)
        if missing_checks:
            failures.append(f"missing passed checks: {missing_checks}")
        resource_kinds = {
            resource.kind
            for resource in report.resources
            if resource.cluster == requirement.cluster
        }
        missing_resources = sorted(set(requirement.required_resource_kinds) - resource_kinds)
        if missing_resources:
            failures.append(f"missing resource evidence: {missing_resources}")
        if requirement.evidence_group_resource_kind is not None:
            grouping_ids = {
                resource.resource_id
                for resource in report.resources
                if resource.kind == requirement.evidence_group_resource_kind
            }
            if len(grouping_ids) != 1:
                failures.append(
                    "report must identify exactly one evidence-group resource "
                    f"of kind {requirement.evidence_group_resource_kind}; "
                    f"found {sorted(grouping_ids)}"
                )
        failures.extend(
            required_resource_failures(
                requirement,
                report.resources,
                expected_cluster=requirement.cluster,
            )
        )
        failures.extend(spack_fresh_install_transition_failures(requirement, report))
        failures.extend(jarvis_execution_identity_failures(requirement, report))
    return failures


def has_complete_producer_identity(trust: EvidenceTrust) -> bool:
    """Return whether report provenance contains the complete producer tuple."""
    return (
        trust.producer_github_login is not None
        and trust.producer_github_id is not None
        and trust.invocation_id is not None
    )


def launcher_identity_failures(
    policy: ReleaseGatePolicy,
    report: LiveValidationReport,
) -> list[str]:
    """Require the launcher binary and invocation nonce to be process-bound evidence."""
    receipt = report.install_source.launcher_receipt
    failures: list[str] = []
    if receipt.get("verified") is not True or receipt.get("uv_executable_verified") is not True:
        failures.append("launcher receipt does not verify the exact uv executable")
    invocation_id = receipt.get("invocation_id")
    if invocation_id != report.evidence_trust.invocation_id:
        failures.append("launcher receipt invocation id does not match report producer provenance")
    uv_version = receipt.get("uv_version")
    if (
        not isinstance(uv_version, str)
        or re.fullmatch(
            r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)?",
            uv_version,
        )
        is None
    ):
        failures.append("launcher receipt omits an exact uv version")
    elif policy.required_uv_version is not None and uv_version != policy.required_uv_version:
        failures.append(
            f"launcher receipt uv version must be {policy.required_uv_version}, got {uv_version}"
        )
    executable_sha256 = receipt.get("uv_executable_sha256")
    if (
        not isinstance(executable_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", executable_sha256) is None
    ):
        failures.append("launcher receipt omits a lowercase uv executable SHA-256")
    if report.install_source.launcher == "uv-tool":
        if receipt.get("claimed_launcher") != "uv-tool":
            failures.append("launcher receipt does not identify the persistent uv tool path")
        for field in ("uv_tool_directory", "uv_tool_bin_directory", "process_prefix"):
            value = receipt.get(field)
            if not isinstance(value, str) or not value or not Path(value).is_absolute():
                failures.append(f"launcher receipt omits absolute {field}")
        for field in (
            "tool_environment_verified",
            "tool_bin_bound",
            "tool_target_bound",
            "pyvenv_matches_uv",
            "package_in_process_environment",
            "executable_in_process_environment",
            "executable_target_bound",
            "isolated_environment",
        ):
            if receipt.get(field) is not True:
                failures.append(f"launcher receipt does not verify {field}")
        record = receipt.get("distribution_record")
        record_mapping = cast(dict[str, Any], record) if isinstance(record, dict) else {}
        if record_mapping.get("verified") is not True:
            failures.append("launcher receipt does not verify the installed RECORD closure")
        for field in ("record_sha256", "runtime_closure_sha256"):
            value = record_mapping.get(field)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                failures.append(f"launcher receipt omits lowercase {field}")
    return failures
