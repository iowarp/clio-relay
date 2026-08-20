"""Prove a live worker process against its receipt, and build acceptance evidence.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns
``worker_runtime_info`` (freshness/liveness proof against the fresh-endpoint
index), the ``verify_remote_*`` identity comparisons a worker's self-reported
payload must pass, and ``attach_verified_worker_identity`` -- the top-level
function that turns a remote worker's runtime-info payload into checks and
resources on a :class:`~clio_relay.validation_report.LiveValidationReport`.

``worker_runtime_info`` calls ``installation_info``/``load_install_receipt``,
which stay resident in ``installation.py`` (the receipt-lifecycle keystone).
Importing them at module scope here would be circular (installation.py must
import this module to re-export ``worker_runtime_info``), so both calls use
a function-scope import back through the facade -- the same proven idiom
this function already uses for its ``config``/``core_queue``/``models``
dependencies below.

This module is over the usual 150-500-line sweet spot by design: its three
concerns (liveness proof, remote identity comparison, report attachment)
form one real call chain -- ``attach_verified_worker_identity`` ->
``verify_remote_worker_info`` -> ``verify_remote_installation_info`` -- each
consuming the previous stage's typed result, not unrelated code sharing a
file. It stays under the 800-line ratchet cap (scripts/check_file_size.py)
without a baseline entry.
"""

from __future__ import annotations

import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlsplit

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from pydantic import ValidationError

from clio_relay.component_verification_remote import (
    _is_released_component,
    _remote_component_runtime_identity,
    _worker_process_matches,
    verify_remote_clio_kit_native_execution_component,
    verify_remote_native_jarvis_component,
)
from clio_relay.dev_mode import VerificationFindings, enforce
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import InstallReceipt
from clio_relay.remote_values import expand_remote_value_on_host
from clio_relay.validation_report import (
    EvidenceReference,
    InstallSourceKind,
    LiveValidationReport,
    SoftwareIdentity,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
)

MAX_WORKER_ENDPOINT_RECORDS = 10_000


def worker_runtime_info(
    *,
    cluster: str,
    freshness_seconds: float = 120.0,
    current_installation: dict[str, object] | None = None,
    readiness_only: bool = False,
    pinned_install_receipt_path: str | None = None,
    dev_mode: bool = False,
) -> dict[str, object]:
    """Prove the active worker process loaded the same exact installation receipt.

    ``pinned_install_receipt_path`` names a cluster-registered runtime receipt
    (the 1.6.6 cluster schema's ``relay_install_receipt`` field, threaded in by
    the caller that holds the cluster registry). When given, the worker's
    self-reported identity is independently checked against that pinned
    receipt (``identity_matches_pinned``/``pinned_installation``) rather than
    only against this invocation's own ambient ``current`` installation: a
    multi-tenant host can keep its shared ``current`` pointed at a different
    generation for other tenants while one cluster is pinned to its own.

    ``pinned_install_receipt_path`` may be recorded ``$HOME/``-anchored (the
    same convention ``jarvis_run_environment.registered_site_spack_command``
    expands for ``spack_executable``, and ``jarvis_mcp.jarvis_mcp_command``
    expands for its own per-cluster receipt pin, clio-relay#228). It is
    expanded via :func:`clio_relay.remote_values.expand_remote_value_on_host`
    against this process's own home before loading -- ``Path.expanduser()``
    alone only expands a leading ``~`` and silently leaves a literal
    ``$HOME/`` prefix unresolved, which previously either misreported this
    #205 identity-verification chain as broken (a nonexistent-path load
    failure, loud but misleading) or, worse on a shell-quoted remote command
    line, prevented remote shell expansion entirely.

    ``dev_mode`` (clio-relay#211, resolved by the caller from
    ``CLIO_RELAY_DEV_MODE``/the cluster's ``dev_mode`` flag) is threaded
    into the ``current`` self-check so a non-generation dev install here
    still produces a readable status instead of raising; it never affects
    freshness/liveness or the fresh-endpoint scan below, which stay hard.
    """
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import MAX_ENDPOINT_FRESH_SECONDS, ClioCoreQueue
    from clio_relay.installation import installation_info, load_install_receipt
    from clio_relay.models import EndpointRole

    if freshness_seconds <= 0:
        raise ConfigurationError("worker freshness_seconds must be positive")
    if freshness_seconds > MAX_ENDPOINT_FRESH_SECONDS:
        raise ConfigurationError(
            "worker freshness_seconds exceeds the bounded fresh endpoint window"
        )
    current = (
        installation_info(dev_mode=dev_mode)
        if current_installation is None
        else current_installation
    )
    if current.get("schema_version") != "clio-relay.installation-info.v1":
        raise ConfigurationError("current installation snapshot is invalid")
    pinned_installation: dict[str, object] | None = None
    if pinned_install_receipt_path is not None:
        try:
            pinned_receipt_path = Path(
                expand_remote_value_on_host(
                    pinned_install_receipt_path,
                    field="relay_install_receipt",
                    home=os.path.expanduser("~"),
                )
            ).expanduser()
            pinned_receipt = load_install_receipt(pinned_receipt_path)
        except ConfigurationError as exc:
            raise ConfigurationError(
                f"cluster {cluster} pinned install receipt could not be loaded: {exc}"
            ) from exc
        pinned_installation = pinned_receipt.model_dump(mode="json")
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    endpoint_records, endpoints_truncated = queue.scan_fresh_endpoints_read_only(
        limit=MAX_WORKER_ENDPOINT_RECORDS,
        fresh_seconds=math.ceil(freshness_seconds),
        cluster=cluster,
    )
    if endpoints_truncated:
        raise ConfigurationError(
            "fresh worker endpoint discovery exceeds the bounded limit "
            f"{MAX_WORKER_ENDPOINT_RECORDS}: {cluster}"
        )
    endpoints = [endpoint for endpoint in endpoint_records if endpoint.role is EndpointRole.WORKER]
    if not endpoints:
        raise ConfigurationError(f"no worker endpoint is registered for cluster {cluster}")
    endpoint = max(endpoints, key=lambda item: item.last_seen_at)
    endpoint_installation = endpoint.metadata.get("installation_info")
    if not isinstance(endpoint_installation, dict):
        raise ConfigurationError("active worker endpoint has no installation identity")
    observed_at = datetime.now(UTC)
    age_seconds = (observed_at - endpoint.last_seen_at).total_seconds()
    fresh = 0 <= age_seconds <= freshness_seconds
    process_running = _worker_process_matches(endpoint.pid)
    identity_matches_current = endpoint_installation == current
    identity_matches_pinned: bool | None = None
    if pinned_installation is not None:
        typed_endpoint_installation = cast(dict[str, object], endpoint_installation)
        identity_matches_pinned = (
            typed_endpoint_installation.get("receipt_matches_install") is True
            and typed_endpoint_installation.get("receipt") == pinned_installation
        )
    scheduler_provider = endpoint.metadata.get("scheduler_provider")
    readiness: dict[str, object] = {
        "schema_version": "clio-relay.worker-runtime-info.v1",
        "cluster": cluster,
        "observed_at": observed_at.isoformat(),
        "freshness_seconds": freshness_seconds,
        "endpoint_age_seconds": age_seconds,
        "fresh": fresh,
        "process_running": process_running,
        "identity_matches_current": identity_matches_current,
        "scheduler_provider": scheduler_provider,
        "running": fresh and process_running and identity_matches_current,
    }
    if readiness_only:
        readiness["schema_version"] = "clio-relay.worker-readiness.v1"
        return readiness
    return {
        **readiness,
        "endpoint": endpoint.model_dump(mode="json"),
        "installation": current,
        "endpoint_installation": endpoint_installation,
        "pinned_installation": pinned_installation,
        "identity_matches_pinned": identity_matches_pinned,
    }


def verify_remote_installation_info(
    info: dict[str, object],
    *,
    expected_version: str,
    expected_software: SoftwareIdentity,
    expected_artifact_sha256: str | None,
    expected_source: str | None,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> InstallReceipt:
    """Require a remote receipt to match the exact local acceptance artifact.

    In dev mode (clio-relay#211) every semantic identity comparison below
    is downgraded to a recorded warning instead of raising, and the parsed
    receipt is still returned. Payload shape (the receipt must parse as a
    valid ``InstallReceipt``) stays hard either way -- a malformed payload
    is corruption, not a would-have-failed release check.
    """
    findings = findings if findings is not None else VerificationFindings()
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=info.get("distribution_version") == expected_version,
        message="remote clio-relay distribution version does not match",
    )
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=info.get("receipt_matches_install") is True,
        message="remote installation receipt does not match the running package",
    )
    raw_software = info.get("software")
    raw_receipt = info.get("receipt")
    try:
        software = SoftwareIdentity.model_validate(raw_software)
        receipt = InstallReceipt.model_validate(raw_receipt)
    except ValidationError as exc:
        raise ConfigurationError(f"remote installation identity is invalid: {exc}") from exc
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=software == expected_software,
        message="remote worker commit/tag identity does not match",
    )
    if expected_artifact_sha256 is None:
        enforce(
            findings,
            dev_mode=dev_mode,
            condition=False,
            message="acceptance did not identify the tested artifact SHA-256",
        )
    else:
        enforce(
            findings,
            dev_mode=dev_mode,
            condition=receipt.artifact_sha256 == expected_artifact_sha256,
            message="remote worker wheel SHA-256 does not match",
        )
    if expected_source is not None:
        enforce(
            findings,
            dev_mode=dev_mode,
            condition=receipt.requested_source == expected_source,
            message=(
                "remote worker install source does not match: "
                f"{receipt.requested_source} != {expected_source}"
            ),
        )
    return receipt


def _installation_identity_label(receipt: InstallReceipt) -> str:
    """Return one short human-readable identity label for a proven receipt."""
    generation = receipt.generation
    artifact = receipt.artifact_sha256
    return (
        f"{receipt.distribution_version}"
        f"(generation={generation[:12] if generation else 'none'}, "
        f"artifact={artifact[:12] if artifact else 'none'})"
    )


def verify_remote_worker_info(
    info: dict[str, object],
    *,
    expected_cluster: str,
    expected_version: str,
    expected_software: SoftwareIdentity,
    expected_artifact_sha256: str | None,
    expected_source: str | None,
    require_target_identity: bool = True,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> InstallReceipt:
    """Require fresh live-worker proof in addition to a static install receipt.

    When the worker's cluster declares a pinned runtime (``info`` carries a
    non-null ``pinned_installation``, resolved server-side from the cluster
    registry's ``relay_install_receipt``), the worker's self-reported
    identity must match that pin (``identity_matches_pinned``) instead of
    this invocation's ambient ``identity_matches_current``/``running``: a
    shared host's ``current`` symlink is not required to agree with a pin
    made for one cluster. A cluster with no pin keeps requiring
    ``identity_matches_current`` exactly as before.

    In dev mode (clio-relay#211) every identity/receipt/sha comparison below
    is downgraded to a recorded warning instead of raising, and the best
    receipt available is still returned. Liveness (``fresh``,
    ``process_running``) and every structural/shape check (schema, cluster
    and role match, scheduler-provider attestation, physical target
    identity) stay hard regardless -- dev mode relaxes release-integrity
    ceremony for a trusted git sha, not proof the worker process is real,
    live, and the one this call actually asked about.
    """
    findings = findings if findings is not None else VerificationFindings()
    if info.get("schema_version") != "clio-relay.worker-runtime-info.v1":
        raise ConfigurationError("remote worker runtime identity schema does not match")
    if info.get("cluster") != expected_cluster:
        raise ConfigurationError("remote worker runtime cluster does not match")
    raw_pinned_installation = info.get("pinned_installation")
    if raw_pinned_installation is not None and not isinstance(raw_pinned_installation, dict):
        raise ConfigurationError("remote worker pinned installation identity is invalid")
    cluster_pins_runtime = raw_pinned_installation is not None
    for flag in ("fresh", "process_running"):
        if info.get(flag) is not True:
            raise ConfigurationError(f"remote worker runtime did not prove {flag}")
    if not cluster_pins_runtime:
        for flag in ("identity_matches_current", "running"):
            enforce(
                findings,
                dev_mode=dev_mode,
                condition=info.get(flag) is True,
                message=f"remote worker runtime did not prove {flag}",
            )
    current = info.get("installation")
    endpoint_installation = info.get("endpoint_installation")
    endpoint = info.get("endpoint")
    if not isinstance(current, dict) or not isinstance(endpoint_installation, dict):
        raise ConfigurationError("remote worker runtime omitted installation identity")
    if not isinstance(endpoint, dict):
        raise ConfigurationError("remote worker runtime omitted endpoint identity")
    typed_current = cast(dict[object, object], current)
    typed_endpoint_installation = cast(dict[object, object], endpoint_installation)
    typed_endpoint = {
        str(key): value for key, value in cast(dict[object, object], endpoint).items()
    }
    if typed_endpoint.get("cluster") != expected_cluster or typed_endpoint.get("role") != "worker":
        raise ConfigurationError("remote worker endpoint role or cluster does not match")
    endpoint_metadata = typed_endpoint.get("metadata")
    if not isinstance(endpoint_metadata, dict):
        raise ConfigurationError("remote worker endpoint omitted scheduler-provider metadata")
    scheduler_provider = cast(dict[str, object], endpoint_metadata).get("scheduler_provider")
    if (
        not isinstance(scheduler_provider, str)
        or not scheduler_provider
        or info.get("scheduler_provider") != scheduler_provider
    ):
        raise ConfigurationError("remote worker scheduler-provider attestation does not match")
    endpoint_receipt = verify_remote_installation_info(
        {str(key): value for key, value in typed_endpoint_installation.items()},
        expected_version=expected_version,
        expected_software=expected_software,
        expected_artifact_sha256=expected_artifact_sha256,
        expected_source=expected_source,
        dev_mode=dev_mode,
        findings=findings,
    )
    if cluster_pins_runtime:
        if info.get("identity_matches_pinned") is not True:
            try:
                pinned_receipt = InstallReceipt.model_validate(raw_pinned_installation)
                message = (
                    "remote worker runtime does not match its cluster's pinned installation: "
                    f"worker={_installation_identity_label(endpoint_receipt)} "
                    f"pinned={_installation_identity_label(pinned_receipt)}"
                )
            except ValidationError as exc:
                if not dev_mode:
                    raise ConfigurationError(
                        f"remote worker pinned installation identity is invalid: {exc}"
                    ) from exc
                message = f"remote worker pinned installation identity is invalid: {exc}"
            enforce(findings, dev_mode=dev_mode, condition=False, message=message)
        current_receipt = endpoint_receipt
    else:
        current_receipt = verify_remote_installation_info(
            {str(key): value for key, value in typed_current.items()},
            expected_version=expected_version,
            expected_software=expected_software,
            expected_artifact_sha256=expected_artifact_sha256,
            expected_source=expected_source,
            dev_mode=dev_mode,
            findings=findings,
        )
        enforce(
            findings,
            dev_mode=dev_mode,
            condition=endpoint_receipt == current_receipt,
            message="running worker receipt differs from current installation",
        )
    if require_target_identity:
        target_identity = info.get("target_identity")
        if not isinstance(target_identity, dict):
            raise ConfigurationError(
                "remote worker evidence omitted verified physical target identity"
            )
        typed_target_identity = cast(dict[str, object], target_identity)
        if typed_target_identity.get("verified") is not True:
            raise ConfigurationError(
                "remote worker evidence omitted verified physical target identity"
            )
    return current_receipt


def _verify_release_worker_install_source(receipt: InstallReceipt) -> str:
    """Validate the worker's own release-pinned installation source.

    The desktop operator and persistent cluster worker may obtain the same
    released wheel through different transports.  For example, an operator can
    run a GitHub-release wheel while cluster bootstrap downloads the exact
    version-pinned PyPI wheel.  Artifact, version, and software identity remain
    exact; this check validates the worker receipt's source semantics instead
    of incorrectly requiring it to equal the desktop launcher's source kind.
    """
    filename = receipt.artifact_filename
    if not isinstance(filename, str) or not filename:
        raise ConfigurationError("remote worker install receipt omitted its wheel filename")
    try:
        project, version, _build, _tags = parse_wheel_filename(filename)
    except InvalidWheelFilename as exc:
        raise ConfigurationError("remote worker install receipt wheel filename is invalid") from exc
    if project != canonicalize_name("clio-relay") or str(version) != receipt.distribution_version:
        raise ConfigurationError(
            "remote worker install receipt wheel does not match its distribution version"
        )

    requested_source = receipt.requested_source
    if requested_source == InstallSourceKind.PYPI.value:
        expected_spec = f"clio-relay=={receipt.distribution_version}"
        if receipt.install_spec != expected_spec:
            raise ConfigurationError(
                "remote worker PyPI install source is not pinned to the exact release version"
            )
    elif requested_source == InstallSourceKind.WHEEL.value:
        parsed = urlsplit(receipt.install_spec)
        if parsed.query or parsed.fragment:
            raise ConfigurationError("remote worker wheel install source is not immutable")
        install_filename = unquote(parsed.path).replace("\\", "/").rsplit("/", 1)[-1]
        if install_filename != filename:
            raise ConfigurationError(
                "remote worker wheel install source does not match its receipt artifact"
            )
    else:
        raise ConfigurationError(
            "remote worker install source is not a release-pinned PyPI or wheel source"
        )
    return requested_source


def _verify_report_worker_receipt(
    report: LiveValidationReport,
    info: dict[str, object],
) -> InstallReceipt:
    """Bind a report to the exact worker artifact and its own pinned source."""
    receipt = verify_remote_worker_info(
        info,
        expected_cluster=report.cluster,
        expected_version=report.install_source.distribution_version,
        expected_software=report.software,
        expected_artifact_sha256=report.install_source.artifact_sha256,
        expected_source=None,
    )
    _verify_release_worker_install_source(receipt)
    return receipt


def attach_verified_worker_identity(
    report: LiveValidationReport,
    info: dict[str, object],
) -> InstallReceipt:
    """Verify and attach remote worker identity checks to a canonical report."""
    receipt = _verify_report_worker_receipt(report, info)
    now = datetime.now(UTC)
    checks = {
        "worker.artifact-version": receipt.distribution_version,
        "worker.artifact-sha256": receipt.artifact_sha256 or "none",
        "worker.install-source": receipt.requested_source,
        "worker.source-identity": (
            f"{receipt.software.commit or 'none'}:"
            f"{receipt.software.tag or 'none'}:{receipt.software.dirty}"
        ),
        "worker.scheduler-provider": str(info["scheduler_provider"]),
        "worker.target-identity": "verified",
    }
    for check_id, value in checks.items():
        report.checks.append(
            ValidationCheck(
                check_id=check_id,
                summary=check_id.replace(".", " "),
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[
                    EvidenceReference(
                        kind="remote_install_receipt",
                        excerpt=f"{check_id}={value}",
                    )
                ],
            )
        )
    installation_payload = info.get("installation")
    component_runtime: object = (
        cast(dict[str, object], installation_payload).get("component_runtime", {})
        if isinstance(installation_payload, dict)
        else {}
    )
    report.resources.append(
        ValidationResource(
            kind="relay_worker",
            resource_id=f"worker:{report.cluster}",
            role="cluster_worker",
            cluster=report.cluster,
            state="running",
            metadata={
                **receipt.model_dump(mode="json"),
                "component_runtime": component_runtime,
                "scheduler_provider": info.get("scheduler_provider"),
                "runtime_proof": {
                    "endpoint": info.get("endpoint"),
                    "observed_at": info.get("observed_at"),
                    "endpoint_age_seconds": info.get("endpoint_age_seconds"),
                    "fresh": info.get("fresh"),
                    "process_running": info.get("process_running"),
                    "identity_matches_current": info.get("identity_matches_current"),
                },
            },
        )
    )
    target_identity = cast(dict[str, object], info["target_identity"])
    report.resources.append(
        ValidationResource(
            kind="cluster_target",
            resource_id=f"target:{report.cluster}",
            role="physical_cluster_target",
            cluster=report.cluster,
            state="verified",
            provider=str(info["scheduler_provider"]),
            metadata=target_identity,
        )
    )
    component = receipt.component_artifacts.get("clio-kit")
    runtime_identity = _remote_component_runtime_identity(info, "clio-kit")
    component_valid = (
        component is not None
        and _is_released_component(component)
        and runtime_identity.get("artifact_identity_verified") is True
        and runtime_identity.get("command_matches_receipt") is True
        and runtime_identity.get("locked_server_runtime_verified") is True
    )
    report.checks.append(
        ValidationCheck(
            check_id="worker.component-clio-kit-released",
            summary="worker uses an exact hashed released clio-kit artifact",
            status=(ValidationStatus.PASSED if component_valid else ValidationStatus.FAILED),
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="remote_install_receipt",
                    excerpt=(
                        "clio-kit component artifact is exact and released"
                        if component_valid
                        else (
                            "clio-kit component artifact is missing or not an exact "
                            "hashed released wheel"
                        )
                    ),
                    metadata={
                        "component": (
                            component.model_dump(mode="json") if component is not None else {}
                        ),
                        "runtime": runtime_identity,
                    },
                )
            ],
            error=(
                None
                if component_valid
                else "worker clio-kit component is not bound to an exact hashed released artifact"
            ),
        )
    )
    if not component_valid:
        report.status = ValidationStatus.FAILED
        report.error = "worker component artifact verification failed"
        raise ConfigurationError(
            "worker clio-kit component is not bound to an exact hashed released artifact"
        )
    clio_kit_native_runtime = runtime_identity
    try:
        clio_kit_native_runtime = verify_remote_clio_kit_native_execution_component(
            info,
            receipt,
        )
    except ConfigurationError as exc:
        clio_kit_native_valid = False
        clio_kit_native_error = str(exc)
    else:
        clio_kit_native_valid = True
        clio_kit_native_error = None
    report.checks.append(
        ValidationCheck(
            check_id="worker.component-clio-kit-native-jarvis-contract",
            summary="worker exposes the receipt-bound native JARVIS MCP contract",
            status=(ValidationStatus.PASSED if clio_kit_native_valid else ValidationStatus.FAILED),
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="remote_install_receipt",
                    excerpt=(
                        "clio-kit native JARVIS contract is verified"
                        if clio_kit_native_valid
                        else "clio-kit native JARVIS contract is not verified"
                    ),
                    metadata={
                        "component": (
                            component.model_dump(mode="json") if component is not None else {}
                        ),
                        "runtime": (
                            clio_kit_native_runtime if clio_kit_native_valid else runtime_identity
                        ),
                    },
                )
            ],
            error=clio_kit_native_error,
        )
    )
    if not clio_kit_native_valid:
        report.status = ValidationStatus.FAILED
        report.error = "worker clio-kit native JARVIS contract verification failed"
        raise ConfigurationError(
            clio_kit_native_error or "worker clio-kit native JARVIS contract verification failed"
        )
    jarvis_component = receipt.component_artifacts.get("jarvis-cd")
    jarvis_runtime = _remote_component_runtime_identity(info, "jarvis-cd")
    try:
        verify_remote_native_jarvis_component(info, receipt)
    except ConfigurationError as exc:
        jarvis_component_valid = False
        jarvis_error = str(exc)
    else:
        jarvis_component_valid = True
        jarvis_error = None
    report.checks.append(
        ValidationCheck(
            check_id="worker.component-jarvis-native-execution",
            summary="worker uses the receipt-bound native JARVIS execution API",
            status=(ValidationStatus.PASSED if jarvis_component_valid else ValidationStatus.FAILED),
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="remote_install_receipt",
                    excerpt=(
                        "JARVIS native execution API identity is verified"
                        if jarvis_component_valid
                        else "JARVIS native execution API identity is not verified"
                    ),
                    metadata={
                        "component": (
                            jarvis_component.model_dump(mode="json")
                            if jarvis_component is not None
                            else {}
                        ),
                        "runtime": jarvis_runtime,
                    },
                )
            ],
            error=jarvis_error,
        )
    )
    if not jarvis_component_valid:
        report.status = ValidationStatus.FAILED
        report.error = "worker native JARVIS execution verification failed"
        raise ConfigurationError(
            jarvis_error or "worker native JARVIS execution verification failed"
        )
    return receipt
