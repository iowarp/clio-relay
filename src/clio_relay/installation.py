"""Durable identity for the exact clio-relay artifact installed on a cluster."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Literal, cast
from urllib.parse import unquote, urlsplit

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
from clio_relay.component_verification_remote import (
    OFFICIAL_COMPONENT_RELEASE_REPOSITORIES,  # noqa: F401
    _is_released_component,  # noqa: F401
    verify_remote_clio_kit_native_execution_component,  # noqa: F401
    verify_remote_native_jarvis_component,  # noqa: F401
    verify_remote_package_progress_component,  # noqa: F401
)
from clio_relay.contract_gate import (
    SurfaceContractDegradation,
    SurfaceContractStatus,
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
    probe_clio_kit_native_execution_contract,  # noqa: F401
    probe_jarvis_native_execution_capability,  # noqa: F401
)
from clio_relay.persistent_uv_tool_probe import (
    MAX_UV_TOOL_RECEIPT_BYTES,  # noqa: F401
    probe_persistent_uv_tool_identity,  # noqa: F401
)
from clio_relay.validation_report import (
    InstallSource,
    InstallSourceKind,
    detect_install_source,
    detect_software_identity,
    sha256_file,
)
from clio_relay.worker_runtime_verification import (
    MAX_WORKER_ENDPOINT_RECORDS,  # noqa: F401
    _installation_identity_label,  # noqa: F401
    _verify_release_worker_install_source,  # noqa: F401
    _verify_report_worker_receipt,  # noqa: F401
    attach_verified_worker_identity,  # noqa: F401
    verify_remote_installation_info,  # noqa: F401
    verify_remote_worker_info,  # noqa: F401
    worker_runtime_info,  # noqa: F401
)

INSTALL_RECEIPT_PATH_ENV = "CLIO_RELAY_INSTALL_RECEIPT"


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


def _requested_source(install_spec: str, artifact_path: Path | None) -> str:
    normalized = install_spec.strip().lower()
    if normalized.startswith("clio-relay=="):
        return "pypi"
    if artifact_path is not None or normalized.endswith(".whl"):
        return "wheel"
    return "checkout"
