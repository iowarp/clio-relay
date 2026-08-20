"""Durable identity for the exact clio-relay artifact installed on a cluster."""

from __future__ import annotations

import json
import math
import os
import re
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Literal, cast
from urllib.parse import unquote, urlsplit

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from pydantic import ValidationError

# Owner-module re-exports (iowarp/clio-relay#231 split/installation). Each
# extracted concern is re-imported here under its original name so every
# existing `from clio_relay.installation import X` caller (bootstrap.py,
# cli.py, jarvis_mcp.py, ...) and every `clio_relay.installation.X`
# qualified/monkeypatch access keeps resolving unchanged -- a pure move, not
# a behavior change. See each owner module's own docstring for what it owns.
from clio_relay.component_runtime_identity import (
    _component_runtime_identity,
    persistent_component_runtime_identity,  # noqa: F401
)
from clio_relay.contract_gate import (
    SurfaceContractDegradation,
    SurfaceContractStatus,
    require_surface_contract,
)
from clio_relay.dev_mode import VerificationFindings, dev_mode_enabled, enforce
from clio_relay.distribution_source_identity import (
    verify_distribution_file_source,  # noqa: F401
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import (
    INSTALL_RECEIPT_SCHEMA,  # noqa: F401
    JARVIS_EXECUTION_HANDLE_SCHEMA,  # noqa: F401
    JARVIS_EXECUTION_PROGRESS_SCHEMA,  # noqa: F401
    JARVIS_EXECUTION_RECORD_SCHEMA,  # noqa: F401
    NATIVE_JARVIS_CAPABILITY_SCHEMA,  # noqa: F401
    PERSISTENT_UV_TOOL_IDENTITY_SCHEMA,  # noqa: F401
    ComponentArtifactIdentity,
    InstallReceipt,
    NativeJarvisExecutionCapability,  # noqa: F401
    PersistentUvToolIdentity,  # noqa: F401
    _is_sha256_text,
)
from clio_relay.native_jarvis_contract import (
    CLIO_KIT_JARVIS_CONTRACT_ID,  # noqa: F401
    CLIO_KIT_JARVIS_EXECUTION_SCHEMA,  # noqa: F401
    CLIO_KIT_MCP_CONTRACT_SCHEMA,  # noqa: F401
    CLIO_KIT_NATIVE_OPERATIONS,  # noqa: F401
    JARVIS_ARTIFACT_SCHEMA,  # noqa: F401
    JARVIS_CD_NATIVE_OPERATIONS,  # noqa: F401
    JARVIS_EXECUTION_ARTIFACTS_SCHEMA,  # noqa: F401
    JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA,  # noqa: F401
    _native_capability_matches_component,
    probe_clio_kit_native_execution_contract,  # noqa: F401
    probe_jarvis_native_execution_capability,  # noqa: F401
)
from clio_relay.persistent_uv_tool_probe import (
    MAX_UV_TOOL_RECEIPT_BYTES,  # noqa: F401
    probe_persistent_uv_tool_identity,  # noqa: F401
)
from clio_relay.python_distribution_probe import _normalized_distribution_name
from clio_relay.remote_values import expand_remote_value_on_host
from clio_relay.validation_report import (
    EvidenceReference,
    InstallSource,
    InstallSourceKind,
    LiveValidationReport,
    SoftwareIdentity,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    detect_install_source,
    detect_software_identity,
    sha256_file,
)

INSTALL_RECEIPT_PATH_ENV = "CLIO_RELAY_INSTALL_RECEIPT"
MAX_WORKER_ENDPOINT_RECORDS = 10_000
OFFICIAL_COMPONENT_RELEASE_REPOSITORIES = {
    "clio-kit": ("iowarp", "clio-kit"),
    "jarvis-cd": ("grc-iit", "jarvis-cd"),
}


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
    contract_surfaces: dict[str, SurfaceContractStatus] | None = None,
    contract_degradations: list[SurfaceContractDegradation] | None = None,
    deployment_fingerprint: str | None = None,
    deployment_manifest: dict[str, object] | None = None,
    generation: str | None = None,
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
        contract_surfaces=contract_surfaces or {},
        contract_degradations=contract_degradations or [],
        deployment_fingerprint=deployment_fingerprint,
        deployment_manifest=deployment_manifest,
        generation=generation,
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


def _current_install_receipt(
    path: Path | None = None,
    *,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> tuple[InstallReceipt, Literal["bootstrap", "uv-tool"], InstallSource | None]:
    """Load the durable receipt that owns the current relay installation."""
    receipt_path = path or default_install_receipt_path()
    receipt_origin: Literal["bootstrap", "uv-tool"] = "bootstrap"
    install_source: InstallSource | None = None
    if receipt_path.exists() or path is not None or os.environ.get(INSTALL_RECEIPT_PATH_ENV):
        receipt = load_install_receipt(receipt_path)
    else:
        receipt, install_source = _persistent_uv_tool_install_receipt(
            dev_mode=dev_mode, findings=findings
        )
        receipt_origin = "uv-tool"
    return receipt, receipt_origin, install_source


def verified_session_api_install_receipt(path: Path | None = None) -> InstallReceipt:
    """Return the durable relay receipt verified against this API process.

    Session API identity depends only on the relay distribution version,
    embedded software identity, and relay artifact digest. Component runtime
    probes validate separate execution surfaces and can launch expensive child
    processes, so they are deliberately excluded from this startup-critical
    check.
    """
    receipt, _receipt_origin, _install_source = _current_install_receipt(path)
    current_software = detect_software_identity()
    current_version = metadata.version("clio-relay")
    if (
        receipt.distribution_version != current_version
        or receipt.software != current_software
        or receipt.artifact_sha256 is None
    ) and not dev_mode_enabled():
        raise ConfigurationError(
            "session API installation receipt does not match the running package"
        )
    return receipt


def installation_info(
    path: Path | None = None,
    *,
    dev_mode: bool | None = None,
) -> dict[str, object]:
    """Return current package identity together with its durable install receipt.

    ``dev_mode`` defaults to :func:`clio_relay.dev_mode.dev_mode_enabled` (the
    ``CLIO_RELAY_DEV_MODE`` environment switch) when omitted, so every
    existing caller of this function honors the environment switch for
    free; a caller holding cluster context (e.g. ``worker_runtime_info``)
    passes the resolved value explicitly to also honor a cluster's
    ``dev_mode`` registry flag (clio-relay#211).
    """
    resolved_dev_mode = dev_mode_enabled() if dev_mode is None else dev_mode
    findings = VerificationFindings()
    receipt, receipt_origin, install_source = _current_install_receipt(
        path, dev_mode=resolved_dev_mode, findings=findings
    )
    current_software = detect_software_identity()
    current_version = metadata.version("clio-relay")
    component_runtime = _component_runtime_identity(receipt)
    info: dict[str, object] = {
        "schema_version": "clio-relay.installation-info.v1",
        "distribution_version": current_version,
        "software": current_software.model_dump(mode="json"),
        "receipt": receipt.model_dump(mode="json"),
        "receipt_origin": receipt_origin,
        "install_source": (
            install_source.model_dump(mode="json") if install_source is not None else None
        ),
        "receipt_matches_install": (
            receipt.distribution_version == current_version and receipt.software == current_software
        ),
        "component_runtime": component_runtime,
    }
    dev_mode_payload = findings.payload()
    if dev_mode_payload is not None:
        info.update(dev_mode_payload)
    return info


def _persistent_uv_tool_install_receipt(
    *,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> tuple[InstallReceipt, InstallSource]:
    """Derive a compatible receipt from uv's exact persistent-tool identity.

    A VCS-kind install counts only when pinned to an exact 40-hex git commit
    sha (``git+https://.../clio-relay@<sha>``); ``detect_install_source``
    resolves that sha into ``artifact_sha256`` (see
    ``_vcs_commit_identity_verified``), where it plays the same
    identity-anchor role a wheel's sha256 digest plays. A branch or tag
    reference leaves ``artifact_identity_verified`` False and is rejected
    below exactly like an unverified wheel.

    In dev mode (clio-relay#211) a failed check is recorded on ``findings``
    instead of raising, and derivation still produces a best-effort receipt
    -- honestly less-verified (``artifact_sha256`` may be ``None``, the
    install timestamp may fall back to now) rather than blocking a
    non-generation dev install from getting a receipt at all.
    """
    findings = findings if findings is not None else VerificationFindings()
    source = detect_install_source(
        launcher="uv-tool",
        infer_artifact_sha256=True,
    )
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=source.kind
        in {InstallSourceKind.WHEEL, InstallSourceKind.PYPI, InstallSourceKind.VCS},
        message="running clio-relay is not installed as a persistent uv tool",
    )
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=source.launcher_verified,
        message="persistent uv tool launcher identity could not be verified",
    )
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=source.artifact_identity_verified and source.artifact_sha256 is not None,
        message="persistent uv tool wheel identity could not be verified",
    )
    raw_uv_receipt = source.launcher_receipt.get("uv_tool_receipt")
    uv_receipt: dict[str, object] = (
        cast(dict[str, object], raw_uv_receipt) if isinstance(raw_uv_receipt, dict) else {}
    )
    enforce(
        findings,
        dev_mode=dev_mode,
        condition=isinstance(raw_uv_receipt, dict) and uv_receipt.get("verified") is True,
        message="persistent uv tool receipt could not be verified",
    )
    receipt_path_value = uv_receipt.get("path")
    installed_at = datetime.now(UTC)
    if isinstance(receipt_path_value, str):
        try:
            installed_at = datetime.fromtimestamp(Path(receipt_path_value).stat().st_mtime, tz=UTC)
        except OSError as exc:
            enforce(
                findings,
                dev_mode=dev_mode,
                condition=False,
                message=f"persistent uv tool receipt path is unavailable: {exc}",
            )
    else:
        enforce(
            findings,
            dev_mode=dev_mode,
            condition=False,
            message="persistent uv tool receipt path is missing",
        )
    reference = source.reference
    artifact_filename: str | None = None
    if reference is not None and source.kind is not InstallSourceKind.VCS:
        parsed = urlsplit(reference)
        if parsed.path:
            artifact_filename = Path(unquote(parsed.path)).name or None
    receipt = InstallReceipt(
        installed_at=installed_at,
        install_spec=reference or f"clio-relay=={source.distribution_version}",
        requested_source=source.kind.value,
        artifact_filename=artifact_filename,
        artifact_sha256=source.artifact_sha256,
        distribution_version=source.distribution_version,
        software=detect_software_identity(),
    )
    return receipt, source


def write_self_install_receipt(
    path: Path,
    *,
    force: bool = False,
    components_from: Path | None = None,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> InstallReceipt:
    """Mint a durable receipt describing the RUNNING installation's own identity.

    A dev-tool install has no receipt describing itself: ``installation_info()``
    prefers any existing on-disk receipt at the default bootstrap-style path,
    so a stale receipt left over from an earlier bootstrap deployment shadows
    the actual running identity. A cluster's pinned runtime
    (``cluster pin-runtime --install-receipt``, clio-relay#205) needs an
    explicit, accurate receipt file to point at -- this mints one.

    Identity is resolved exactly the way the persistent-uv-tool identity
    check already trusts it: a wheel's sha256 digest for a WHEEL/PYPI
    install, or the exact pinned commit sha for an exact-sha VCS install
    (``_vcs_commit_identity_verified``, clio-relay#206). Never re-derived a
    second, potentially divergent way.

    ``components_from`` names another receipt -- the generation receipt
    that genuinely installed components such as clio-kit/jarvis-cd -- whose
    ``components``/``component_artifacts`` blocks are copied verbatim into
    the minted receipt. This is for a legitimate mixed dev-channel install:
    relay identity is this process's own (self), while its components are
    still served by a bootstrap generation's locked runtime. The source
    path is recorded on ``components_source_receipt`` so the mix is
    traceable, never silent. Refuses when the source receipt carries no
    component artifacts to inherit.

    ``dev_mode`` (clio-relay#211) downgrades identity derivation to a
    best-effort receipt with recorded warnings instead of raising -- for a
    non-generation dev install (a checkout, an unverified launcher, ...)
    that still needs *a* receipt to iterate against. ``force`` still
    governs overwriting the destination path unconditionally.
    """
    if path.exists() and not force:
        raise ConfigurationError(
            f"install receipt already exists: {path} (use --force to overwrite)"
        )
    receipt, _source = _persistent_uv_tool_install_receipt(dev_mode=dev_mode, findings=findings)
    if components_from is not None:
        try:
            source_receipt = load_install_receipt(components_from)
        except ConfigurationError as exc:
            raise ConfigurationError(
                f"components source receipt could not be loaded: {components_from}: {exc}"
            ) from exc
        if not source_receipt.component_artifacts:
            raise ConfigurationError(
                "components source receipt has no component artifacts to inherit: "
                f"{components_from}"
            )
        receipt = receipt.model_copy(
            update={
                "components": dict(source_receipt.components),
                "component_artifacts": dict(source_receipt.component_artifacts),
                "components_source_receipt": str(components_from),
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(receipt.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return receipt


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
    """Verify a legacy package-progress plugin for compatibility diagnostics only.

    This compatibility proof is intentionally not used by the 1.0 release gate.
    """
    component = receipt.component_artifacts.get(component_name)
    if component is None:
        raise ConfigurationError(
            f"worker installation omitted package progress component {component_name}"
        )
    if (
        not _is_official_github_release_component(component, component_name="jarvis-cd")
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


def verify_remote_clio_kit_native_execution_component(
    info: dict[str, object],
    receipt: InstallReceipt,
) -> dict[str, object]:
    """Require the exact receipt-bound clio-kit native JARVIS MCP contract.

    A receipt with no ``native_execution`` for a RECORDED, below-pin jarvis
    surface (``receipt.contract_surfaces["jarvis"].meets_requirement`` is
    False) is not a generic configuration problem: bootstrap already proved
    that surface's shipped identity and recorded the gap loudly
    (iowarp/clio-relay#242). Refuse with the typed
    :class:`clio_relay.errors.ContractSurfaceUnavailableError` naming
    surface/have/need instead of the generic message below, which stays for
    a receipt that never probed the surface at all.

    In dev mode (clio-relay#242 course correction), the below-pin refusal
    :func:`clio_relay.contract_gate.require_surface_contract` raises is
    deferred (logged at WARNING, never silent), and this returns the
    worker's self-reported runtime identity UNVERIFIED instead of falling
    into the generic "omitted" error next -- dev mode means the surface
    still serves.
    """
    component = receipt.component_artifacts.get("clio-kit")
    if component is None or component.native_execution is None:
        jarvis_surface = receipt.contract_surfaces.get("jarvis")
        if jarvis_surface is not None and not jarvis_surface.meets_requirement:
            require_surface_contract(jarvis_surface)
            return _remote_component_runtime_identity(info, "clio-kit")
        raise ConfigurationError("worker installation omitted the clio-kit native JARVIS contract")
    if not _native_capability_matches_component(
        component.native_execution,
        component_name="clio-kit",
    ):
        raise ConfigurationError("worker clio-kit native JARVIS contract is invalid")
    runtime = _remote_component_runtime_identity(info, "clio-kit")
    for field in (
        "artifact_identity_verified",
        "command_matches_receipt",
        "locked_server_runtime_verified",
        "native_execution_capability_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker clio-kit native JARVIS contract did not prove {field}"
            )
    try:
        observed = NativeJarvisExecutionCapability.model_validate(
            runtime.get("native_execution_capability")
        )
    except ValidationError as exc:
        raise ConfigurationError(
            f"worker clio-kit native JARVIS runtime contract was invalid: {exc}"
        ) from exc
    if observed != component.native_execution:
        raise ConfigurationError(
            "worker clio-kit native JARVIS runtime contract changed from its receipt"
        )
    return runtime


def verify_remote_native_jarvis_component(
    info: dict[str, object],
    receipt: InstallReceipt,
    *,
    component_name: str = "jarvis-cd",
) -> dict[str, object]:
    """Require immutable JARVIS-CD provenance and native execution API proof."""
    component = receipt.component_artifacts.get(component_name)
    if component is None:
        raise ConfigurationError(
            f"worker installation omitted native JARVIS component {component_name}"
        )
    if (
        not _is_official_github_release_component(
            component,
            component_name=component_name,
        )
        or component.runtime_artifact_path is None
        or "execution" not in component.runtime_interpreters
        or set(component.runtime_executables) != {"jarvis"}
        or component.native_execution is None
        or not _native_capability_matches_component(
            component.native_execution,
            component_name="jarvis-cd",
        )
    ):
        raise ConfigurationError(
            f"worker native JARVIS component {component_name} has incomplete provenance"
        )
    runtime = _remote_component_runtime_identity(info, component_name)
    for field in (
        "verified",
        "distribution_identity_verified",
        "runtime_artifact_path_verified",
        "artifact_sha256_verified",
        "execution_interpreter_verified",
        "execution_distribution_identity_verified",
        "execution_source_verified",
        "jarvis_executable_verified",
        "execution_native_execution_capability_verified",
        "native_execution_capability_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker native JARVIS component {component_name} did not prove {field}"
            )
    return runtime


def _is_released_component(component: ComponentArtifactIdentity) -> bool:
    version = component.distribution_version
    if (
        version is None
        or not _is_sha256_text(component.artifact_sha256)
        or component.runtime_artifact_path is None
        or not component.runtime_command
    ):
        return False
    if component.requested_source == "pypi":
        return component.install_spec == f"{component.distribution}=={version}"
    return _is_official_github_release_component(
        component,
        component_name="clio-kit",
    )


def _is_official_github_release_component(
    component: ComponentArtifactIdentity,
    *,
    component_name: str,
) -> bool:
    """Return whether provenance names one canonical, hash-bound release wheel."""
    normalized_name = _normalized_distribution_name(component_name)
    repository = OFFICIAL_COMPONENT_RELEASE_REPOSITORIES.get(normalized_name)
    version = component.distribution_version
    if (
        repository is None
        or version is None
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version) is None
        or _normalized_distribution_name(component.distribution) != normalized_name
        or component.requested_source != "github_release"
        or not _is_sha256_text(component.artifact_sha256)
    ):
        return False
    owner, repository_name = repository
    wheel_distribution = normalized_name.replace("-", "_")
    filename = f"{wheel_distribution}-{version}-py3-none-any.whl"
    expected_url = (
        f"https://github.com/{owner}/{repository_name}/releases/download/v{version}/{filename}"
    )
    return component.artifact_filename == filename and component.install_spec == expected_url


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
