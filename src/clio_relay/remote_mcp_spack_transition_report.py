"""Fresh-install Spack transition report: bind absent/install/locate into one proof.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns
:func:`build_remote_mcp_spack_fresh_install_transition_report`, which binds
three ordinary acceptance reports (preinstall ``spack_find``, ``spack_install``,
postinstall ``spack_locate``) plus independently observed pre/post Spack
configuration bytes into one fail-closed proof that a package was absent
immediately before a non-reusing install and was subsequently located
strictly inside the disposable acceptance store -- flattening every
underlying phase check (phase-prefixed to keep names unambiguous) alongside
its own identity/durable-evidence/configuration-binding/disposable-store
checks. The per-phase semantic checks it calls live in
``remote_mcp_spack_transition_checks.py``, a separate owner module (extracted
first, so this report builder can build on it without a circular import).

``build_remote_mcp_spack_fresh_install_transition_report`` is re-exported
under its original name (``cli_remote_mcp_validate.py`` and tests import it
directly from ``clio_relay.remote_mcp``). The three private helpers have no
caller outside ``remote_mcp.py`` (confirmed by grep before the move), so they
are imported directly rather than re-exported.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Literal, cast

from clio_relay.remote_mcp_acceptance_evidence import (
    _bounded_evidence_scalar,
    _is_strict_canonical_posix_descendant,
)
from clio_relay.remote_mcp_acceptance_models import (
    RemoteMcpAcceptanceCheck,
    RemoteMcpAcceptanceReport,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpSpackInstallTransitionEvidence,
    RemoteMcpStructuredResultExpectation,
    _is_canonical_absolute_posix_path,
    _is_canonical_relative_posix_path,
)
from clio_relay.remote_mcp_spack_transition_checks import (
    _spack_fresh_install_check,
    _spack_postinstall_locate_check,
    _spack_preinstall_absent_check,
    _spack_transition_call_evidence,
    _spack_transition_durable_evidence_check,
    _spack_transition_identity_check,
)
from clio_relay.remote_mcp_stdio_evidence import _as_json

JSON = dict[str, Any]


def build_remote_mcp_spack_fresh_install_transition_report(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
    preinstall_protocol_result: JSON | None,
    install_protocol_result: JSON | None,
    postinstall_protocol_result: JSON | None,
    install_expectation: RemoteMcpStructuredResultExpectation,
    preinstall_configuration: RemoteMcpSpackConfigurationObservation,
    postinstall_configuration: RemoteMcpSpackConfigurationObservation,
) -> RemoteMcpAcceptanceReport:
    """Bind absent, install, and locate calls into one fail-closed Spack proof.

    The returned acceptance report retains the install call as its primary
    operation, while phase-prefixed checks and transition evidence prove that
    the package was absent immediately before a non-reusing install and was
    subsequently located strictly inside the disposable acceptance store.
    """
    store_root = install_expectation.fresh_install_store_root
    requested_spec = install_expectation.requested_spec
    configuration_sha256 = install_expectation.fresh_install_configuration_sha256
    configuration_manifest_path = install_expectation.fresh_install_configuration_manifest_path
    if (
        install_expectation.tool != "spack_install"
        or install_expectation.reuse is not False
        or requested_spec is None
        or store_root is None
        or configuration_sha256 is None
        or configuration_manifest_path is None
    ):
        raise ValueError(
            "fresh Spack transition requires a spack_install expectation with "
            "reuse=false, fresh_install_store_root, configuration SHA-256, and "
            "configuration manifest path"
        )

    configuration_check, executed_wrapper = _spack_fresh_configuration_check(
        expected_sha256=configuration_sha256,
        expected_manifest_path=configuration_manifest_path,
        preinstall=preinstall_configuration,
        postinstall=postinstall_configuration,
        install_report=install_report,
    )

    preinstall_check, preinstall_structured = _spack_preinstall_absent_check(
        report=preinstall_report,
        protocol_result=preinstall_protocol_result,
        expectation=install_expectation,
    )
    install_check, install_structured = _spack_fresh_install_check(
        report=install_report,
        protocol_result=install_protocol_result,
        expectation=install_expectation,
    )
    locate_check, locate_structured, locate_prefix = _spack_postinstall_locate_check(
        report=postinstall_report,
        protocol_result=postinstall_protocol_result,
        expectation=install_expectation,
    )
    disposable_store_passed = _is_strict_canonical_posix_descendant(
        locate_prefix,
        store_root,
    )
    disposable_store_check = RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-disposable-store",
        passed=disposable_store_passed,
        message=(
            "installed prefix is strictly inside the disposable Spack store"
            if disposable_store_passed
            else "installed prefix is not strictly inside the disposable Spack store"
        ),
        evidence={
            "fresh_install_store_root": store_root,
            "observed_prefix": _bounded_evidence_scalar(locate_prefix),
            "root_is_canonical_absolute": _is_canonical_absolute_posix_path(store_root),
            "prefix_is_strict_descendant": disposable_store_passed,
        },
    )

    identity_check, identity = _spack_transition_identity_check(
        preinstall_report=preinstall_report,
        install_report=install_report,
        postinstall_report=postinstall_report,
    )
    durable_check = _spack_transition_durable_evidence_check(
        preinstall_report=preinstall_report,
        install_report=install_report,
        postinstall_report=postinstall_report,
    )
    transition = RemoteMcpSpackInstallTransitionEvidence(
        cluster=install_report.cluster,
        server_name=install_report.server_name,
        profile=install_report.profile,
        requested_spec=requested_spec,
        package_name=install_expectation.package_name,
        dag_hash=install_expectation.dag_hash,
        fresh_install_store_root=store_root,
        fresh_install_configuration_sha256=configuration_sha256,
        fresh_install_configuration_manifest_path=configuration_manifest_path,
        preinstall_configuration=preinstall_configuration,
        postinstall_configuration=postinstall_configuration,
        executed_spack_command_path=(
            executed_wrapper["path"] if configuration_check.passed else None
        ),
        executed_spack_command_relative_path=(
            executed_wrapper["relative_path"] if configuration_check.passed else None
        ),
        executed_spack_command_sha256=(
            executed_wrapper["sha256"] if configuration_check.passed else None
        ),
        executed_spack_command_size_bytes=(
            executed_wrapper["size_bytes"] if configuration_check.passed else None
        ),
        registration_revision=identity["registration_revision"],
        cluster_route_revision=identity["cluster_route_revision"],
        catalog_revision=identity["catalog_revision"],
        server_artifact_sha256=identity["server_artifact_sha256"],
        preinstall=_spack_transition_call_evidence(
            report=preinstall_report,
            phase="preinstall",
            structured_result=preinstall_structured,
        ),
        install=_spack_transition_call_evidence(
            report=install_report,
            phase="install",
            structured_result=install_structured,
        ),
        postinstall=_spack_transition_call_evidence(
            report=postinstall_report,
            phase="postinstall",
            structured_result=locate_structured,
        ),
    )

    flattened_checks = [
        *_phase_prefixed_acceptance_checks(preinstall_report, phase="preinstall"),
        *(check.model_copy(deep=True) for check in install_report.checks),
        *_phase_prefixed_acceptance_checks(postinstall_report, phase="postinstall"),
        identity_check,
        durable_check,
        configuration_check,
        preinstall_check,
        install_check,
        locate_check,
        disposable_store_check,
    ]
    flattened_checks = _uniquely_named_acceptance_checks(flattened_checks)
    passed = all(check.passed for check in flattened_checks)
    payload = install_report.model_dump(mode="python")
    payload.update(
        {
            "passed": passed,
            "checks": flattened_checks,
            "spack_install_transition": transition,
        }
    )
    return RemoteMcpAcceptanceReport.model_validate(payload)


def _spack_fresh_configuration_check(
    *,
    expected_sha256: str,
    expected_manifest_path: str,
    preinstall: RemoteMcpSpackConfigurationObservation,
    postinstall: RemoteMcpSpackConfigurationObservation,
    install_report: RemoteMcpAcceptanceReport,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Bind independently observed wrapper/config bytes before and after installation."""
    pre_components = [component.model_dump(mode="json") for component in preinstall.components]
    post_components = [component.model_dump(mode="json") for component in postinstall.components]
    digest_matches = (
        preinstall.manifest_sha256 == expected_sha256
        and postinstall.manifest_sha256 == expected_sha256
    )
    path_matches = (
        preinstall.manifest_path == expected_manifest_path
        and postinstall.manifest_path == expected_manifest_path
    )
    components_match = pre_components == post_components
    manifest_metadata_matches = (
        preinstall.manifest_size_bytes == postinstall.manifest_size_bytes
        and preinstall.manifest_regular_file
        and postinstall.manifest_regular_file
    )
    phases_match = preinstall.phase == "preinstall" and postinstall.phase == "postinstall"
    wrapper_binding = _spack_command_configuration_binding(
        install_report=install_report,
        manifest_path=expected_manifest_path,
        preinstall=preinstall,
        postinstall=postinstall,
    )
    wrapper_matches = wrapper_binding["matches"] is True
    passed = (
        digest_matches
        and path_matches
        and components_match
        and manifest_metadata_matches
        and phases_match
        and wrapper_matches
    )
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-fresh-configuration",
            passed=passed,
            message=(
                "executed Spack wrapper and configuration bytes remained exactly bound"
                if passed
                else "executed Spack wrapper or configuration identity was not exactly bound"
            ),
            evidence={
                "expected": {
                    "manifest_path": expected_manifest_path,
                    "configuration_sha256": expected_sha256,
                },
                "preinstall": preinstall.model_dump(mode="json"),
                "postinstall": postinstall.model_dump(mode="json"),
                "digest_matches": digest_matches,
                "path_matches": path_matches,
                "components_match": components_match,
                "manifest_metadata_matches": manifest_metadata_matches,
                "phases_match": phases_match,
                "executed_spack_command": wrapper_binding,
                "wrapper_matches": wrapper_matches,
            },
        ),
        wrapper_binding,
    )


def _spack_command_configuration_binding(
    *,
    install_report: RemoteMcpAcceptanceReport,
    manifest_path: str,
    preinstall: RemoteMcpSpackConfigurationObservation,
    postinstall: RemoteMcpSpackConfigurationObservation,
) -> JSON:
    """Bind the executed ``--spack-command`` path to one observed manifest file."""
    evidence: JSON = {
        "matches": False,
        "path": None,
        "relative_path": None,
        "sha256": None,
        "size_bytes": None,
        "failures": [],
    }
    failures = cast(list[str], evidence["failures"])
    call_checks = [check for check in install_report.checks if check.name == "remote-mcp.call"]
    if len(call_checks) != 1 or not call_checks[0].passed or not install_report.passed:
        failures.append("install report does not contain one passing immutable call binding")
    spec = _as_json(install_report.call_job.get("spec")) or {}
    raw_server_args = spec.get("server_args")
    server_args = cast(list[object], raw_server_args) if isinstance(raw_server_args, list) else []
    candidates: list[str] = []
    for index, value in enumerate(server_args):
        if value == "--spack-command" and index + 1 < len(server_args):
            next_value = server_args[index + 1]
            if isinstance(next_value, str):
                candidates.append(next_value)
        elif isinstance(value, str) and value.startswith("--spack-command="):
            candidates.append(value.partition("=")[2])
    if len(candidates) != 1 or not _is_canonical_absolute_posix_path(candidates[0]):
        failures.append("install call does not contain one canonical --spack-command path")
        return evidence
    wrapper_path = candidates[0]
    manifest_parent = PurePosixPath(manifest_path).parent
    typed_wrapper = PurePosixPath(wrapper_path)
    try:
        relative = typed_wrapper.relative_to(manifest_parent)
    except ValueError:
        failures.append("executed Spack wrapper is outside the configuration manifest root")
        return evidence
    relative_path = str(relative)
    if not _is_canonical_relative_posix_path(relative_path):
        failures.append("executed Spack wrapper relative path is not canonical")
        return evidence
    pre_matches = [
        component for component in preinstall.components if component.relative_path == relative_path
    ]
    post_matches = [
        component
        for component in postinstall.components
        if component.relative_path == relative_path
    ]
    if len(pre_matches) != 1 or len(post_matches) != 1:
        failures.append("executed Spack wrapper is not one unique manifest component")
        return evidence
    pre_component = pre_matches[0]
    post_component = post_matches[0]
    component_matches = pre_component == post_component and pre_component.regular_file
    if not component_matches:
        failures.append("executed Spack wrapper bytes or regular-file identity changed")
    evidence.update(
        {
            "matches": not failures,
            "path": wrapper_path,
            "relative_path": relative_path,
            "sha256": pre_component.sha256,
            "size_bytes": pre_component.size_bytes,
        }
    )
    return evidence


def _phase_prefixed_acceptance_checks(
    report: RemoteMcpAcceptanceReport,
    *,
    phase: Literal["preinstall", "postinstall"],
) -> list[RemoteMcpAcceptanceCheck]:
    """Copy ordinary acceptance checks under one unambiguous phase namespace."""
    checks: list[RemoteMcpAcceptanceCheck] = []
    for check in report.checks:
        suffix = check.name.removeprefix("remote-mcp.")
        checks.append(
            RemoteMcpAcceptanceCheck(
                name=f"remote-mcp.{phase}.{suffix}",
                passed=check.passed,
                message=check.message,
                evidence=deepcopy(check.evidence),
            )
        )
    return checks


def _uniquely_named_acceptance_checks(
    checks: list[RemoteMcpAcceptanceCheck],
) -> list[RemoteMcpAcceptanceCheck]:
    """Preserve every assertion while giving duplicate source checks stable suffixes."""
    occurrences: dict[str, int] = {}
    result: list[RemoteMcpAcceptanceCheck] = []
    for check in checks:
        occurrence = occurrences.get(check.name, 0) + 1
        occurrences[check.name] = occurrence
        if occurrence == 1:
            result.append(check)
            continue
        result.append(
            RemoteMcpAcceptanceCheck(
                name=f"{check.name}-{occurrence}",
                passed=check.passed,
                message=check.message,
                evidence=deepcopy(check.evidence),
            )
        )
    return result
