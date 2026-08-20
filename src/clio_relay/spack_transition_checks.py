"""Bind one typed Spack fresh-install transition report (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). A release policy's
``spack_fresh_install_transition`` requirement
(:class:`~clio_relay.validation_schema.ReleaseSpackFreshInstallRequirement`)
is satisfied only by one coherent report proving a real fresh install:
exactly one preinstall/install/postinstall phase job, each producing the
exact structured MCP result the policy's requested spec/reuse=false demands,
cross-bound to the seven :data:`~clio_relay.validation_schema.
SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS` structured-evidence checks, the
disposable install store, the fresh configuration manifest, and the twelve
per-phase durable artifacts.
:func:`spack_fresh_install_transition_failures` is the single entry point
gate evaluation calls; everything else here is a private binding helper for
one piece of that cross-checked evidence graph.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, cast

from clio_relay.validation_schema import (
    SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS,
    LiveValidationReport,
    ReleaseGateRequirement,
    ReleaseSpackFreshInstallRequirement,
    ValidationResource,
    ValidationStatus,
)


def spack_fresh_install_transition_failures(
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
) -> list[str]:
    """Independently bind one typed Spack fresh-install transition report."""
    expected = requirement.spack_fresh_install_transition
    if expected is None:
        return []
    failures: list[str] = []
    checks: dict[str, dict[str, Any]] = {}
    for check_id in SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS:
        metadata = _unique_spack_transition_check_metadata(report, check_id, failures)
        if metadata is not None:
            checks[check_id] = metadata

    phase_definitions = (
        (
            "preinstall",
            "spack_preinstall_find",
            "spack_find",
            {"query": expected.requested_spec},
        ),
        (
            "install",
            "spack_fresh_install",
            "spack_install",
            {"spec": expected.requested_spec, "reuse": False},
        ),
        ("postinstall", "spack_postinstall_locate", "spack_locate", None),
    )
    phase_resources: dict[str, ValidationResource] = {}
    phase_indexes: list[int] = []
    for phase, role, tool, arguments in phase_definitions:
        matches = [
            (index, resource)
            for index, resource in enumerate(report.resources)
            if resource.cluster == requirement.cluster
            and resource.kind == "relay_job"
            and resource.role == role
        ]
        if len(matches) != 1:
            failures.append(
                f"Spack fresh-install transition requires exactly one {phase} phase job; "
                f"found {len(matches)}"
            )
            continue
        index, resource = matches[0]
        phase_indexes.append(index)
        phase_resources[phase] = resource
        metadata = resource.metadata
        if resource.state != "succeeded":
            failures.append(f"Spack {phase} phase job did not succeed")
        if metadata.get("remote_mcp_server_name") != expected.server_name:
            failures.append(f"Spack {phase} phase job identifies the wrong server")
        if metadata.get("profile") != expected.profile:
            failures.append(f"Spack {phase} phase job identifies the wrong profile")
        if metadata.get("remote_mcp_tool_name") != tool:
            failures.append(f"Spack {phase} phase job identifies the wrong tool")
        if arguments is not None and metadata.get("arguments") != arguments:
            failures.append(f"Spack {phase} phase job arguments do not match policy")

    if len(phase_indexes) == len(phase_definitions) and phase_indexes != sorted(phase_indexes):
        failures.append("Spack phase jobs are not recorded in preinstall/install/postinstall order")
    phase_job_ids = [
        phase_resources[phase].resource_id
        for phase in ("preinstall", "install", "postinstall")
        if phase in phase_resources
    ]
    if len(phase_job_ids) == 3 and len(set(phase_job_ids)) != 3:
        failures.append("Spack transition phase jobs do not have distinct durable identities")

    preinstall_result = _spack_phase_structured_result(
        phase_resources.get("preinstall"),
        phase="preinstall",
        failures=failures,
    )
    if preinstall_result is not None and preinstall_result != {
        "schema_version": "spack.mcp.result.v1",
        "operation": "find",
        "query": expected.requested_spec,
        "count": 0,
        "packages": [],
    }:
        failures.append("Spack preinstall phase does not prove the exact spec was absent")

    install_result = _spack_phase_structured_result(
        phase_resources.get("install"),
        phase="install",
        failures=failures,
    )
    dag_hash: str | None = None
    if install_result is not None:
        package = _spack_transition_mapping(install_result.get("package"))
        raw_hash = package.get("dag_hash") if package is not None else None
        if isinstance(raw_hash, str) and re.fullmatch(r"[a-z0-9]{32}", raw_hash) is not None:
            dag_hash = raw_hash
        install_matches = (
            install_result.get("schema_version") == "spack.mcp.result.v1"
            and install_result.get("operation") == "install"
            and install_result.get("requested_spec") == expected.requested_spec
            and install_result.get("reuse") is expected.reuse
            and install_result.get("status") == "installed"
            and install_result.get("package_count") == 1
            and package is not None
            and package.get("name") == expected.package_name
            and dag_hash is not None
        )
        if not install_matches:
            failures.append(
                "Spack install phase does not bind the exact package/spec with reuse=false"
            )

    postinstall_resource = phase_resources.get("postinstall")
    postinstall_result = _spack_phase_structured_result(
        postinstall_resource,
        phase="postinstall",
        failures=failures,
    )
    prefix: str | None = None
    exact_hash_spec = f"/{dag_hash}" if dag_hash is not None else None
    if postinstall_result is not None:
        package = _spack_transition_mapping(postinstall_result.get("package"))
        raw_prefix = postinstall_result.get("prefix")
        prefix = raw_prefix if isinstance(raw_prefix, str) and raw_prefix else None
        postinstall_matches = (
            exact_hash_spec is not None
            and postinstall_result.get("schema_version") == "spack.mcp.result.v1"
            and postinstall_result.get("operation") == "locate"
            and postinstall_result.get("requested_spec") == exact_hash_spec
            and postinstall_result.get("load_spec") == exact_hash_spec
            and package is not None
            and package.get("name") == expected.package_name
            and package.get("dag_hash") == dag_hash
            and prefix is not None
        )
        if not postinstall_matches:
            failures.append("Spack postinstall phase does not locate the exact installed DAG")
    if postinstall_resource is not None and postinstall_resource.metadata.get("arguments") != {
        "spec": exact_hash_spec
    }:
        failures.append("Spack postinstall phase does not query the exact /dag_hash")

    _bind_spack_transition_phase_checks(
        checks,
        expected=expected,
        preinstall_result=preinstall_result,
        install_result=install_result,
        postinstall_result=postinstall_result,
        dag_hash=dag_hash,
        failures=failures,
    )
    _bind_spack_transition_identity(
        checks.get("remote-mcp.spack-transition-identity"),
        requirement=requirement,
        expected=expected,
        failures=failures,
    )
    _bind_spack_transition_durable_evidence(
        checks.get("remote-mcp.spack-transition-durable-evidence"),
        phase_job_ids=phase_job_ids,
        failures=failures,
    )
    store_root = _bind_spack_disposable_store(
        checks.get("remote-mcp.spack-disposable-store"),
        prefix=prefix,
        failures=failures,
    )
    if store_root is None or prefix is None:
        failures.append("Spack transition omits its disposable store or installed prefix")
    _bind_spack_configuration_identity(
        checks.get("remote-mcp.spack-fresh-configuration"),
        report=report,
        requirement=requirement,
        failures=failures,
    )
    _bind_spack_transition_artifacts(
        report,
        requirement=requirement,
        phase_resources=phase_resources,
        failures=failures,
    )
    server_resources = [
        resource
        for resource in report.resources
        if resource.cluster == requirement.cluster
        and resource.kind == "mcp_server"
        and resource.role == "remote_mcp_server"
        and resource.metadata.get("server_name") == expected.server_name
    ]
    if len(server_resources) != 1 or server_resources[0].state != "verified":
        failures.append("Spack transition does not identify one verified fresh MCP server")
    return failures


def _unique_spack_transition_check_metadata(
    report: LiveValidationReport,
    check_id: str,
    failures: list[str],
) -> dict[str, Any] | None:
    """Return one passed transition check's single structured evidence object."""
    matches = [check for check in report.checks if check.check_id == check_id]
    if len(matches) != 1:
        failures.append(f"Spack transition check {check_id} must appear exactly once")
        return None
    check = matches[0]
    if check.status is not ValidationStatus.PASSED or len(check.evidence) != 1:
        failures.append(f"Spack transition check {check_id} is not one passed evidence record")
        return None
    metadata = check.evidence[0].metadata
    if not metadata:
        failures.append(f"Spack transition check {check_id} has no structured evidence")
        return None
    return metadata


def _spack_transition_mapping(value: object) -> dict[str, Any] | None:
    """Narrow one untrusted report value to a string-keyed mapping."""
    if not isinstance(value, dict):
        return None
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        return None
    return cast(dict[str, Any], value)


def _spack_phase_structured_result(
    resource: ValidationResource | None,
    *,
    phase: str,
    failures: list[str],
) -> dict[str, Any] | None:
    """Read one phase result from its exact durable relay-job resource."""
    result = (
        _spack_transition_mapping(resource.metadata.get("structured_result"))
        if resource is not None
        else None
    )
    if result is None:
        failures.append(f"Spack {phase} phase job omits structured result evidence")
    return result


def _bind_spack_transition_phase_checks(
    checks: dict[str, dict[str, Any]],
    *,
    expected: ReleaseSpackFreshInstallRequirement,
    preinstall_result: dict[str, Any] | None,
    install_result: dict[str, Any] | None,
    postinstall_result: dict[str, Any] | None,
    dag_hash: str | None,
    failures: list[str],
) -> None:
    """Cross-bind phase check evidence to the three canonical job projections."""
    phase_checks = (
        (
            "remote-mcp.spack-preinstall-absent",
            {"query": expected.requested_spec},
            preinstall_result,
        ),
        (
            "remote-mcp.spack-fresh-install",
            {"spec": expected.requested_spec, "reuse": False},
            install_result,
        ),
        (
            "remote-mcp.spack-postinstall-locate",
            {"spec": f"/{dag_hash}" if dag_hash is not None else None},
            postinstall_result,
        ),
    )
    for check_id, arguments, observed in phase_checks:
        evidence = checks.get(check_id)
        if evidence is None:
            continue
        if (
            evidence.get("submitted_arguments") != arguments
            or evidence.get("observed") != observed
            or evidence.get("failures") != []
        ):
            failures.append(f"Spack transition check {check_id} is not bound to its phase job")
    preinstall = checks.get("remote-mcp.spack-preinstall-absent")
    if preinstall is not None and preinstall.get("expected_requested_spec") != (
        expected.requested_spec
    ):
        failures.append("Spack absence check identifies the wrong requested spec")
    install = checks.get("remote-mcp.spack-fresh-install")
    install_expected = (
        _spack_transition_mapping(install.get("expected")) if install is not None else None
    )
    if install_expected != {
        "requested_spec": expected.requested_spec,
        "package_name": expected.package_name,
        "dag_hash": dag_hash,
        "reuse": False,
        "status": "installed",
    }:
        failures.append("Spack fresh-install check does not match the policy package identity")
    locate = checks.get("remote-mcp.spack-postinstall-locate")
    locate_expected = (
        _spack_transition_mapping(locate.get("expected")) if locate is not None else None
    )
    if locate_expected != {
        "requested_spec": f"/{dag_hash}" if dag_hash is not None else None,
        "package_name": expected.package_name,
        "dag_hash": dag_hash,
    }:
        failures.append("Spack postinstall check does not match the installed package identity")


def _bind_spack_transition_identity(
    evidence: dict[str, Any] | None,
    *,
    requirement: ReleaseGateRequirement,
    expected: ReleaseSpackFreshInstallRequirement,
    failures: list[str],
) -> None:
    """Require all phases to retain the policy server, profile, and route identity."""
    if evidence is None:
        return
    revision_matches = _spack_transition_mapping(evidence.get("revision_matches"))
    if (
        evidence.get("underlying_reports_passed") is not True
        or evidence.get("scopes") != [[requirement.cluster, expected.server_name, expected.profile]]
        or evidence.get("tool_names") != ["spack_find", "spack_install", "spack_locate"]
        or evidence.get("expected_tool_names") != ["spack_find", "spack_install", "spack_locate"]
        or revision_matches != {"registration": True, "cluster_route": True, "catalog": True}
        or evidence.get("same_server_artifact") is not True
        or not _spack_sha256(evidence.get("server_artifact_sha256"))
    ):
        failures.append("Spack transition phases do not share one verified route identity")


def _bind_spack_transition_durable_evidence(
    evidence: dict[str, Any] | None,
    *,
    phase_job_ids: list[str],
    failures: list[str],
) -> None:
    """Cross-bind ordered phase jobs to the durable-evidence assertion."""
    if evidence is None:
        return
    phases = _spack_transition_mapping(evidence.get("phases"))
    valid = (
        len(phase_job_ids) == 3
        and evidence.get("job_ids") == phase_job_ids
        and evidence.get("distinct_job_ids") is True
        and evidence.get("distinct_artifact_ids") is True
        and evidence.get("required_artifact_kinds")
        == ["mcp_result", "provenance", "stderr", "stdout"]
        and phases is not None
    )
    if valid and phases is not None:
        for phase, job_id in zip(
            ("preinstall", "install", "postinstall"), phase_job_ids, strict=True
        ):
            phase_evidence = _spack_transition_mapping(phases.get(phase))
            valid = (
                valid
                and phase_evidence is not None
                and (
                    phase_evidence.get("job_id") == job_id
                    and phase_evidence.get("state") == "succeeded"
                    and phase_evidence.get("artifacts_valid") is True
                    and phase_evidence.get("stdio_valid") is True
                    and phase_evidence.get("passed") is True
                )
            )
    if not valid:
        failures.append("Spack transition durable evidence is not bound to its ordered jobs")


def _bind_spack_disposable_store(
    evidence: dict[str, Any] | None,
    *,
    prefix: str | None,
    failures: list[str],
) -> str | None:
    """Require nonempty dynamic store/prefix fields and their producer-validated relation."""
    if evidence is None:
        return None
    raw_root = evidence.get("fresh_install_store_root")
    store_root = raw_root if isinstance(raw_root, str) and raw_root else None
    if (
        store_root is None
        or prefix is None
        or not _release_spack_canonical_absolute_path(store_root)
        or not _release_spack_canonical_absolute_path(prefix)
        or not _release_spack_strict_descendant(prefix, store_root)
        or evidence.get("observed_prefix") != prefix
        or evidence.get("root_is_canonical_absolute") is not True
        or evidence.get("prefix_is_strict_descendant") is not True
    ):
        failures.append("Spack disposable-store evidence is missing or not prefix-bound")
    return store_root


def _bind_spack_configuration_identity(
    evidence: dict[str, Any] | None,
    *,
    report: LiveValidationReport,
    requirement: ReleaseGateRequirement,
    failures: list[str],
) -> None:
    """Bind one dynamic configuration SHA/path across checks, resource, and artifact."""
    if evidence is None:
        return
    expected = _spack_transition_mapping(evidence.get("expected"))
    preinstall = _spack_transition_mapping(evidence.get("preinstall"))
    postinstall = _spack_transition_mapping(evidence.get("postinstall"))
    path = expected.get("manifest_path") if expected is not None else None
    sha256 = expected.get("configuration_sha256") if expected is not None else None
    observations_match = (
        isinstance(path, str)
        and bool(path)
        and _release_spack_canonical_absolute_path(path)
        and _spack_sha256(sha256)
        and _spack_configuration_observation_matches(preinstall, "preinstall", path, sha256)
        and _spack_configuration_observation_matches(postinstall, "postinstall", path, sha256)
        and preinstall is not None
        and postinstall is not None
        and preinstall.get("components") == postinstall.get("components")
        and evidence.get("digest_matches") is True
        and evidence.get("path_matches") is True
        and evidence.get("components_match") is True
        and evidence.get("manifest_metadata_matches") is True
        and evidence.get("phases_match") is True
    )
    if not observations_match:
        failures.append("Spack configuration observations do not share one SHA/path identity")
        return
    resources = [
        resource
        for resource in report.resources
        if resource.cluster == requirement.cluster
        and resource.kind == "configuration_manifest"
        and resource.role == "spack_fresh_install_configuration"
    ]
    if len(resources) != 1:
        failures.append("Spack transition requires exactly one configuration manifest resource")
    else:
        resource = resources[0]
        if (
            resource.state != "verified"
            or resource.resource_id != sha256
            or resource.references != [path]
            or resource.metadata.get("expected_sha256") != sha256
            or resource.metadata.get("preinstall") != preinstall
            or resource.metadata.get("postinstall") != postinstall
        ):
            failures.append("Spack configuration resource differs from transition evidence")
    artifacts = [
        artifact
        for artifact in report.artifacts
        if artifact.kind == "spack_fresh_install_configuration"
    ]
    if len(artifacts) != 1 or artifacts[0].reference != path or artifacts[0].sha256 != sha256:
        failures.append("Spack configuration artifact differs from transition evidence")


def _spack_configuration_observation_matches(
    observation: dict[str, Any] | None,
    phase: str,
    path: object,
    sha256: object,
) -> bool:
    """Validate bounded dynamic configuration fields retained in canonical evidence."""
    if observation is None:
        return False
    components = observation.get("components")
    if not isinstance(components, list) or not components:
        return False
    for raw in cast(list[object], components):
        component = _spack_transition_mapping(raw)
        if (
            component is None
            or not isinstance(component.get("relative_path"), str)
            or not component.get("relative_path")
            or not _release_spack_canonical_relative_path(component.get("relative_path"))
            or not _spack_sha256(component.get("sha256"))
            or not isinstance(component.get("size_bytes"), int)
            or isinstance(component.get("size_bytes"), bool)
            or cast(int, component["size_bytes"]) < 0
            or component.get("regular_file") is not True
        ):
            return False
    size = observation.get("manifest_size_bytes")
    return (
        observation.get("schema_version") == "clio-relay.spack-configuration-observation.v1"
        and observation.get("phase") == phase
        and observation.get("manifest_path") == path
        and observation.get("manifest_sha256") == sha256
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size > 0
        and observation.get("manifest_regular_file") is True
    )


def _bind_spack_transition_artifacts(
    report: LiveValidationReport,
    *,
    requirement: ReleaseGateRequirement,
    phase_resources: dict[str, ValidationResource],
    failures: list[str],
) -> None:
    """Require four distinct hashed durable artifacts for every phase job."""
    roles = {
        "preinstall": "spack_preinstall_find",
        "install": "spack_fresh_install",
        "postinstall": "spack_postinstall_locate",
    }
    artifact_ids: list[str] = []
    for phase, base_role in roles.items():
        phase_resource = phase_resources.get(phase)
        if phase_resource is None:
            continue
        for kind in ("stdout", "stderr", "mcp_result", "provenance"):
            role = f"{base_role}_{kind}"
            matches = [
                resource
                for resource in report.resources
                if resource.cluster == requirement.cluster
                and resource.kind == "artifact"
                and resource.role == role
            ]
            if len(matches) != 1:
                failures.append(f"Spack {phase} phase requires exactly one {kind} artifact")
                continue
            artifact = matches[0]
            artifact_ids.append(artifact.resource_id)
            if (
                artifact.metadata.get("transition_phase") != phase
                or artifact.metadata.get("kind") != kind
                or artifact.metadata.get("job_id") != phase_resource.resource_id
                or not _spack_sha256(artifact.metadata.get("sha256"))
            ):
                failures.append(f"Spack {phase} {kind} artifact is not phase/job/hash bound")
    if len(artifact_ids) == 12 and len(set(artifact_ids)) != 12:
        failures.append("Spack transition artifacts do not have distinct durable identities")


def _spack_sha256(value: object) -> bool:
    """Return whether dynamic transition evidence carries one lowercase SHA-256."""
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _release_spack_canonical_absolute_path(value: object) -> bool:
    """Validate dynamic POSIX paths at the release boundary after JSON projection."""
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or value.startswith("//")
        or value == "/"
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _release_spack_canonical_relative_path(value: object) -> bool:
    """Validate component paths retained inside dynamic configuration evidence."""
    if (
        not isinstance(value, str)
        or value.startswith("/")
        or value in {"", "."}
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _release_spack_strict_descendant(path: str, root: str) -> bool:
    """Independently prove the located prefix is contained by the disposable store."""
    candidate = PurePosixPath(path)
    parent = PurePosixPath(root)
    return candidate != parent and parent in candidate.parents
