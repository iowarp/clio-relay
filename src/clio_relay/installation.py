"""Durable identity for the exact clio-relay artifact installed on a cluster."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clio_relay.errors import ConfigurationError
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    SoftwareIdentity,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    detect_software_identity,
    sha256_file,
)

INSTALL_RECEIPT_SCHEMA = "clio-relay.install-receipt.v1"
INSTALL_RECEIPT_PATH_ENV = "CLIO_RELAY_INSTALL_RECEIPT"
MAX_WORKER_ENDPOINT_RECORDS = 10_000


class ComponentArtifactIdentity(BaseModel):
    """Immutable install identity for a runtime component used by the relay."""

    model_config = ConfigDict(extra="forbid")

    distribution: str
    distribution_version: str | None = None
    install_spec: str
    requested_source: str
    artifact_filename: str | None = None
    artifact_sha256: str | None = None
    runtime_artifact_path: str | None = None
    runtime_command: list[str] = Field(default_factory=list)
    runtime_interpreters: dict[str, str] = Field(default_factory=dict)
    runtime_executables: dict[str, str] = Field(default_factory=dict)
    source_commit: str | None = None
    entry_points: list[str] = Field(default_factory=list)


class InstallReceipt(BaseModel):
    """Artifact and source identity recorded at cluster installation time."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = INSTALL_RECEIPT_SCHEMA
    installed_at: datetime
    install_spec: str
    requested_source: str
    artifact_filename: str | None = None
    artifact_sha256: str | None = None
    distribution_version: str
    software: SoftwareIdentity
    components: dict[str, str] = Field(default_factory=dict)
    component_artifacts: dict[str, ComponentArtifactIdentity] = Field(default_factory=dict)


def default_install_receipt_path() -> Path:
    """Return the user-scoped cluster installation receipt path."""
    configured = os.environ.get(INSTALL_RECEIPT_PATH_ENV)
    if configured is not None and configured.strip():
        return Path(configured).expanduser()
    return Path.home() / ".local" / "share" / "clio-relay" / "install-receipt.json"


def write_install_receipt(
    *,
    install_spec: str,
    artifact_path: Path | None = None,
    path: Path | None = None,
    components: dict[str, str] | None = None,
    component_artifacts: dict[str, ComponentArtifactIdentity] | None = None,
) -> InstallReceipt:
    """Atomically record the installed distribution and optional wheel digest."""
    resolved_artifact = artifact_path.resolve() if artifact_path is not None else None
    if resolved_artifact is not None and not resolved_artifact.is_file():
        raise ConfigurationError(f"installed artifact does not exist: {resolved_artifact}")
    receipt = InstallReceipt(
        installed_at=datetime.now(UTC),
        install_spec=install_spec,
        requested_source=_requested_source(install_spec, resolved_artifact),
        artifact_filename=(resolved_artifact.name if resolved_artifact is not None else None),
        artifact_sha256=(sha256_file(resolved_artifact) if resolved_artifact is not None else None),
        distribution_version=metadata.version("clio-relay"),
        software=detect_software_identity(),
        components=components or {},
        component_artifacts=component_artifacts or {},
    )
    destination = path or default_install_receipt_path()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    payload = json.dumps(receipt.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, destination)
    return receipt


def load_install_receipt(path: Path | None = None) -> InstallReceipt:
    """Load and strictly validate the cluster installation receipt."""
    source = path or default_install_receipt_path()
    try:
        return InstallReceipt.model_validate_json(source.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as exc:
        raise ConfigurationError(f"could not read install receipt {source}: {exc}") from exc


def installation_info(path: Path | None = None) -> dict[str, object]:
    """Return current package identity together with its durable install receipt."""
    receipt = load_install_receipt(path)
    current_software = detect_software_identity()
    current_version = metadata.version("clio-relay")
    component_runtime = _component_runtime_identity(receipt)
    return {
        "schema_version": "clio-relay.installation-info.v1",
        "distribution_version": current_version,
        "software": current_software.model_dump(mode="json"),
        "receipt": receipt.model_dump(mode="json"),
        "receipt_matches_install": (
            receipt.distribution_version == current_version and receipt.software == current_software
        ),
        "component_runtime": component_runtime,
    }


def worker_runtime_info(
    *,
    cluster: str,
    freshness_seconds: float = 120.0,
) -> dict[str, object]:
    """Prove the active worker process loaded the same exact installation receipt."""
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.models import EndpointRole

    if freshness_seconds <= 0:
        raise ConfigurationError("worker freshness_seconds must be positive")
    current = installation_info()
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    endpoint_records, endpoints_truncated = queue.scan_endpoints(
        limit=MAX_WORKER_ENDPOINT_RECORDS,
        cluster=cluster,
    )
    if endpoints_truncated:
        raise ConfigurationError(
            "worker endpoint discovery exceeds the bounded limit "
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
    scheduler_provider = endpoint.metadata.get("scheduler_provider")
    return {
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
        "endpoint": endpoint.model_dump(mode="json"),
        "installation": current,
        "endpoint_installation": endpoint_installation,
    }


def verify_remote_installation_info(
    info: dict[str, object],
    *,
    expected_version: str,
    expected_software: SoftwareIdentity,
    expected_artifact_sha256: str | None,
    expected_source: str | None,
) -> InstallReceipt:
    """Require a remote receipt to match the exact local acceptance artifact."""
    if info.get("distribution_version") != expected_version:
        raise ConfigurationError("remote clio-relay distribution version does not match")
    if info.get("receipt_matches_install") is not True:
        raise ConfigurationError("remote installation receipt does not match the running package")
    raw_software = info.get("software")
    raw_receipt = info.get("receipt")
    try:
        software = SoftwareIdentity.model_validate(raw_software)
        receipt = InstallReceipt.model_validate(raw_receipt)
    except ValidationError as exc:
        raise ConfigurationError(f"remote installation identity is invalid: {exc}") from exc
    if software != expected_software:
        raise ConfigurationError("remote worker commit/tag identity does not match")
    if expected_artifact_sha256 is None:
        raise ConfigurationError("acceptance did not identify the tested artifact SHA-256")
    if receipt.artifact_sha256 != expected_artifact_sha256:
        raise ConfigurationError("remote worker wheel SHA-256 does not match")
    if expected_source is not None and receipt.requested_source != expected_source:
        raise ConfigurationError(
            "remote worker install source does not match: "
            f"{receipt.requested_source} != {expected_source}"
        )
    return receipt


def verify_remote_worker_info(
    info: dict[str, object],
    *,
    expected_cluster: str,
    expected_version: str,
    expected_software: SoftwareIdentity,
    expected_artifact_sha256: str | None,
    expected_source: str | None,
    require_target_identity: bool = True,
) -> InstallReceipt:
    """Require fresh live-worker proof in addition to a static install receipt."""
    if info.get("schema_version") != "clio-relay.worker-runtime-info.v1":
        raise ConfigurationError("remote worker runtime identity schema does not match")
    if info.get("cluster") != expected_cluster:
        raise ConfigurationError("remote worker runtime cluster does not match")
    for flag in ("fresh", "process_running", "identity_matches_current", "running"):
        if info.get(flag) is not True:
            raise ConfigurationError(f"remote worker runtime did not prove {flag}")
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
    current_receipt = verify_remote_installation_info(
        {str(key): value for key, value in typed_current.items()},
        expected_version=expected_version,
        expected_software=expected_software,
        expected_artifact_sha256=expected_artifact_sha256,
        expected_source=expected_source,
    )
    endpoint_receipt = verify_remote_installation_info(
        {str(key): value for key, value in typed_endpoint_installation.items()},
        expected_version=expected_version,
        expected_software=expected_software,
        expected_artifact_sha256=expected_artifact_sha256,
        expected_source=expected_source,
    )
    if endpoint_receipt != current_receipt:
        raise ConfigurationError("running worker receipt differs from current installation")
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


def attach_verified_worker_identity(
    report: LiveValidationReport,
    info: dict[str, object],
) -> InstallReceipt:
    """Verify and attach remote worker identity checks to a canonical report."""
    receipt = verify_remote_worker_info(
        info,
        expected_cluster=report.cluster,
        expected_version=report.install_source.distribution_version,
        expected_software=report.software,
        expected_artifact_sha256=report.install_source.artifact_sha256,
        expected_source=report.install_source.kind.value,
    )
    now = datetime.now(UTC)
    checks = {
        "worker.artifact-version": receipt.distribution_version,
        "worker.artifact-sha256": receipt.artifact_sha256 or "none",
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
                        else "clio-kit component artifact is missing or not a hashed PyPI wheel"
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
                else "worker clio-kit component is not bound to an exact hashed PyPI artifact"
            ),
        )
    )
    if not component_valid:
        report.status = ValidationStatus.FAILED
        report.error = "worker component artifact verification failed"
        raise ConfigurationError(
            "worker clio-kit component is not bound to an exact hashed PyPI artifact"
        )
    jarvis_component = receipt.component_artifacts.get("jarvis-cd")
    jarvis_runtime = _remote_component_runtime_identity(info, "jarvis-cd")
    try:
        verify_remote_package_progress_component(info, receipt)
    except ConfigurationError as exc:
        jarvis_component_valid = False
        jarvis_error = str(exc)
    else:
        jarvis_component_valid = True
        jarvis_error = None
    report.checks.append(
        ValidationCheck(
            check_id="worker.component-jarvis-package-provenance",
            summary="worker uses the receipt-bound JARVIS package progress provider",
            status=(ValidationStatus.PASSED if jarvis_component_valid else ValidationStatus.FAILED),
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="remote_install_receipt",
                    excerpt=(
                        "JARVIS package progress provider identity is verified"
                        if jarvis_component_valid
                        else "JARVIS package progress provider identity is not verified"
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
        report.error = "worker JARVIS package progress provider verification failed"
        raise ConfigurationError(
            jarvis_error or "worker JARVIS package progress provider verification failed"
        )
    return receipt


def _component_runtime_identity(receipt: InstallReceipt) -> dict[str, object]:
    """Return current process evidence for receipt-bound component launchers."""
    identities: dict[str, object] = {}
    if "clio-kit" in receipt.component_artifacts:
        from clio_relay.jarvis_mcp import jarvis_mcp_runtime_identity

        identities["clio-kit"] = jarvis_mcp_runtime_identity(receipt)
    for component_name, component in receipt.component_artifacts.items():
        if component.entry_points:
            identities[component_name] = _entry_point_component_runtime_identity(component)
    return identities


def _entry_point_component_runtime_identity(
    component: ComponentArtifactIdentity,
) -> dict[str, object]:
    """Verify both provider and execution interpreters against one released wheel."""
    try:
        distribution = metadata.distribution(component.distribution)
    except metadata.PackageNotFoundError:
        return {
            "verified": False,
            "distribution_identity_verified": False,
            "entry_points_visible": False,
            "runtime_artifact_path_verified": False,
            "artifact_sha256_verified": False,
            "provider_interpreter_verified": False,
            "execution_interpreter_verified": False,
            "jarvis_executable_verified": False,
        }
    installed_entry_points = _distribution_progress_entry_points(distribution)
    expected_entry_points = sorted(component.entry_points)
    direct_url = _distribution_direct_url(distribution)
    runtime_path = (
        Path(component.runtime_artifact_path).expanduser().resolve()
        if component.runtime_artifact_path is not None
        else None
    )
    runtime_artifact_path_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_filename == runtime_path.name
        and direct_url.get("url") == runtime_path.as_uri()
    )
    artifact_sha256_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_sha256 is not None
        and sha256_file(runtime_path) == component.artifact_sha256
    )
    distribution_identity_verified = (
        _normalized_distribution_name(distribution.name)
        == _normalized_distribution_name(component.distribution)
        and distribution.version == component.distribution_version
    )
    entry_points_visible = bool(expected_entry_points) and set(expected_entry_points).issubset(
        installed_entry_points
    )
    provider_python = component.runtime_interpreters.get("provider")
    provider_interpreter_verified = (
        provider_python is not None
        and Path(provider_python).expanduser().resolve() == Path(sys.executable).resolve()
        and distribution_identity_verified
        and entry_points_visible
        and runtime_artifact_path_verified
    )
    execution_python = component.runtime_interpreters.get("execution")
    execution = _probe_python_distribution(execution_python, component.distribution)
    execution_distribution_identity_verified = (
        execution.get("distribution") is not None
        and _normalized_distribution_name(str(execution["distribution"]))
        == _normalized_distribution_name(component.distribution)
        and execution.get("distribution_version") == component.distribution_version
    )
    execution_entry_points = execution.get("entry_points")
    execution_entry_points_visible = (
        isinstance(execution_entry_points, list)
        and bool(expected_entry_points)
        and set(expected_entry_points).issubset(
            {str(value) for value in cast(list[object], execution_entry_points)}
        )
    )
    execution_source_verified = (
        runtime_path is not None and execution.get("direct_url") == runtime_path.as_uri()
    )
    execution_interpreter_verified = (
        execution_python is not None
        and execution.get("executable") is not None
        and Path(str(execution["executable"])).resolve()
        == Path(execution_python).expanduser().resolve()
        and execution_distribution_identity_verified
        and execution_entry_points_visible
        and execution_source_verified
    )
    jarvis_executable = component.runtime_executables.get("jarvis")
    jarvis_executable_verified = _jarvis_executable_matches_interpreter(
        jarvis_executable,
        execution_python,
        runtime_command=component.runtime_command,
    )
    verified = (
        distribution_identity_verified
        and entry_points_visible
        and runtime_artifact_path_verified
        and artifact_sha256_verified
        and provider_interpreter_verified
        and execution_interpreter_verified
        and jarvis_executable_verified
    )
    return {
        "verified": verified,
        "distribution": distribution.name,
        "distribution_version": distribution.version,
        "distribution_identity_verified": distribution_identity_verified,
        "entry_points": installed_entry_points,
        "entry_points_visible": entry_points_visible,
        "runtime_artifact_path": str(runtime_path) if runtime_path is not None else None,
        "runtime_artifact_path_verified": runtime_artifact_path_verified,
        "artifact_sha256": (
            sha256_file(runtime_path)
            if runtime_path is not None and runtime_path.is_file()
            else None
        ),
        "artifact_sha256_verified": artifact_sha256_verified,
        "provider_interpreter": provider_python,
        "provider_interpreter_verified": provider_interpreter_verified,
        "execution_interpreter": execution_python,
        "execution_interpreter_verified": execution_interpreter_verified,
        "execution_distribution_identity_verified": execution_distribution_identity_verified,
        "execution_entry_points_visible": execution_entry_points_visible,
        "execution_source_verified": execution_source_verified,
        "execution": execution,
        "jarvis_executable": jarvis_executable,
        "jarvis_executable_verified": jarvis_executable_verified,
    }


def _distribution_progress_entry_points(distribution: metadata.Distribution) -> list[str]:
    """Return stable package-progress entry-point identities for one distribution."""
    return sorted(
        f"{entry_point.group}:{entry_point.name}"
        for entry_point in distribution.entry_points
        if entry_point.group == "clio_relay.package_progress_adapters"
    )


def _distribution_direct_url(distribution: metadata.Distribution) -> dict[str, object]:
    """Return a normalized PEP 610 direct-url document when present."""
    direct_url_text = distribution.read_text("direct_url.json")
    if direct_url_text is None:
        return {}
    try:
        loaded = json.loads(direct_url_text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def _probe_python_distribution(python: str | None, distribution_name: str) -> dict[str, object]:
    """Inspect a second interpreter without importing provider application code."""
    if python is None:
        return {"verified": False, "error": "execution interpreter is not configured"}
    script = """
import json
import sys
from importlib import metadata

distribution = metadata.distribution(sys.argv[1])
direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
entry_points = sorted(
    f"{entry_point.group}:{entry_point.name}"
    for entry_point in distribution.entry_points
    if entry_point.group == "clio_relay.package_progress_adapters"
)
print(json.dumps({
    "executable": sys.executable,
    "distribution": distribution.name,
    "distribution_version": distribution.version,
    "direct_url": direct_url.get("url"),
    "entry_points": entry_points,
}, sort_keys=True))
"""
    try:
        completed = subprocess.run(
            [python, "-c", script, distribution_name],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"verified": False, "error": f"{type(exc).__name__}: {exc}"}
    if completed.returncode != 0:
        return {
            "verified": False,
            "error": completed.stderr.strip() or completed.stdout.strip(),
        }
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {"verified": False, "error": f"invalid interpreter probe JSON: {exc}"}
    if not isinstance(loaded, dict):
        return {"verified": False, "error": "interpreter probe was not an object"}
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def _jarvis_executable_matches_interpreter(
    executable: str | None,
    python: str | None,
    *,
    runtime_command: list[str],
) -> bool:
    """Require the configured JARVIS launcher to live in the execution environment."""
    if executable is None or python is None or not runtime_command:
        return False
    executable_path = Path(executable).expanduser()
    python_path = Path(python).expanduser()
    try:
        return (
            executable_path.is_file()
            and os.access(executable_path, os.X_OK)
            and executable_path.resolve().parent == python_path.resolve().parent
            and Path(runtime_command[0]).expanduser().resolve() == executable_path.resolve()
        )
    except OSError:
        return False


def _normalized_distribution_name(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _remote_component_runtime_identity(
    info: dict[str, object],
    component_name: str,
) -> dict[str, object]:
    installation = info.get("installation")
    if not isinstance(installation, dict):
        return {}
    runtime = cast(dict[object, object], installation).get("component_runtime")
    if not isinstance(runtime, dict):
        return {}
    identity = cast(dict[object, object], runtime).get(component_name)
    if not isinstance(identity, dict):
        return {}
    return {str(key): value for key, value in cast(dict[object, object], identity).items()}


def verify_remote_package_progress_component(
    info: dict[str, object],
    receipt: InstallReceipt,
    *,
    component_name: str = "jarvis-cd",
) -> dict[str, object]:
    """Require receipt and runtime proof for a cluster-side progress plugin provider."""
    component = receipt.component_artifacts.get(component_name)
    if component is None:
        raise ConfigurationError(
            f"worker installation omitted package progress component {component_name}"
        )
    version = component.distribution_version
    digest = component.artifact_sha256
    if (
        component.requested_source != "github_release"
        or version is None
        or not component.install_spec.startswith("https://github.com/")
        or "/releases/download/" not in component.install_spec
        or digest is None
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest.lower())
        or component.runtime_artifact_path is None
        or set(component.runtime_interpreters) != {"provider", "execution"}
        or set(component.runtime_executables) != {"jarvis"}
        or not component.entry_points
    ):
        raise ConfigurationError(
            f"worker package progress component {component_name} has incomplete provenance"
        )
    runtime = _remote_component_runtime_identity(info, component_name)
    for field in (
        "verified",
        "distribution_identity_verified",
        "entry_points_visible",
        "runtime_artifact_path_verified",
        "artifact_sha256_verified",
        "provider_interpreter_verified",
        "execution_interpreter_verified",
        "execution_distribution_identity_verified",
        "execution_entry_points_visible",
        "execution_source_verified",
        "jarvis_executable_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker package progress component {component_name} did not prove {field}"
            )
    return runtime


def _is_released_component(component: ComponentArtifactIdentity) -> bool:
    version = component.distribution_version
    digest = component.artifact_sha256
    return (
        component.requested_source == "pypi"
        and version is not None
        and component.install_spec == f"{component.distribution}=={version}"
        and digest is not None
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest.lower())
        and component.runtime_artifact_path is not None
        and bool(component.runtime_command)
    )


def _worker_process_matches(pid: int) -> bool:
    """Return whether pid is a live clio-relay endpoint worker process."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    if os.name == "nt":
        return True
    try:
        command = (Path("/proc") / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ")
    except OSError:
        return False
    normalized = command.decode("utf-8", errors="replace")
    return "clio-relay" in normalized and "endpoint" in normalized and "start" in normalized


def _requested_source(install_spec: str, artifact_path: Path | None) -> str:
    normalized = install_spec.strip().lower()
    if normalized.startswith("clio-relay=="):
        return "pypi"
    if artifact_path is not None or normalized.endswith(".whl"):
        return "wheel"
    return "checkout"
