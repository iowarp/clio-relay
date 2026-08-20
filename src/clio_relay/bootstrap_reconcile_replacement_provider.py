"""Retained-state replacement-provider attestation.

Attests a normally-imported candidate relay wheel (a staged uv-tool
identity bound to this exact process and desired state) before legacy
component planning may treat it as the running relay
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

from clio_relay.bootstrap_reconcile_constants import _GETUID
from clio_relay.bootstrap_reconcile_models import (
    BootstrapDesiredState,
    BootstrapPersistentUvToolIdentity,
    BootstrapReplacementProviderEvidence,
)
from clio_relay.bootstrap_reconcile_primitives import _is_sha256
from clio_relay.errors import ConfigurationError


def prove_bootstrap_replacement_provider(
    desired: BootstrapDesiredState,
    *,
    uv_executable: Path,
    tool_executable: Path,
    source_artifact: Path,
    tool_directory: Path,
    tool_bin_directory: Path,
    preparing_root: Path,
    extracted_source_root: Path,
    source_archive_sha256: str,
    expected_provider_interpreter_sha256: str | None = None,
    home: Path | None = None,
) -> BootstrapReplacementProviderEvidence:
    """Attest the normally imported candidate wheel before legacy planning."""
    from clio_relay.installation import probe_persistent_uv_tool_identity

    if not _is_sha256(source_archive_sha256):
        raise ConfigurationError("candidate source archive SHA-256 is invalid")
    try:
        distribution_version = desired.relay_install_spec.removeprefix("clio-relay==")
    except AttributeError as exc:  # pragma: no cover - typed as str by pydantic
        raise ConfigurationError("candidate relay install spec is invalid") from exc
    if (
        not distribution_version
        or desired.relay_install_spec != f"clio-relay=={distribution_version}"
        or desired.relay_artifact_sha256 is None
    ):
        raise ConfigurationError(
            "retained-state replacement requires one exact released clio-relay wheel"
        )
    probed_identity = probe_persistent_uv_tool_identity(
        uv_executable=str(uv_executable),
        tool_executable=str(tool_executable),
        provider_interpreter=str(Path(sys.executable).absolute()),
        source_artifact=source_artifact,
        distribution="clio-relay",
        distribution_version=distribution_version,
        entry_point="clio-relay",
        tool_directory=str(tool_directory),
        tool_bin_directory=str(tool_bin_directory),
        expected_uv_executable_sha256=desired.uv_sha256,
        expected_provider_interpreter_sha256=expected_provider_interpreter_sha256,
    )
    identity = BootstrapPersistentUvToolIdentity.model_validate(
        probed_identity.model_dump(mode="json")
    )
    evidence = BootstrapReplacementProviderEvidence(
        desired_fingerprint=desired.fingerprint,
        relay_install_spec=desired.relay_install_spec,
        preparing_root=str(Path(os.path.abspath(preparing_root.expanduser()))),
        extracted_source_root=str(Path(os.path.abspath(extracted_source_root.expanduser()))),
        source_archive_sha256=source_archive_sha256,
        coordinator_provider_sha256=expected_provider_interpreter_sha256,
        persistent_tool=identity,
    )
    _verify_bootstrap_replacement_provider(desired, evidence, home=home)
    return evidence


def _verify_bootstrap_replacement_provider(
    desired: BootstrapDesiredState,
    evidence: BootstrapReplacementProviderEvidence,
    *,
    home: Path | None = None,
) -> None:
    """Re-probe a staged candidate and bind it to this process and desired state."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    try:
        parent_lexical = lexical_home / ".local/share/clio-relay/preparing"
        parent_details = parent_lexical.lstat()
        expected_parent = parent_lexical.resolve(strict=True)
        root_lexical = Path(evidence.preparing_root)
        root_details = root_lexical.lstat()
        root = root_lexical.resolve(strict=True)
        source_lexical = Path(evidence.extracted_source_root)
        source_details = source_lexical.lstat()
        source_root = source_lexical.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement root is unavailable") from exc
    if (
        parent_lexical.is_symlink()
        or not stat.S_ISDIR(parent_details.st_mode)
        or (os.name != "nt" and stat.S_IMODE(parent_details.st_mode) & 0o022)
        or (_GETUID is not None and parent_details.st_uid != _GETUID())
    ):
        raise ConfigurationError("candidate replacement parent is not private")
    if (
        not root_lexical.is_absolute()
        or ".." in root_lexical.parts
        or root_lexical.is_symlink()
        or not stat.S_ISDIR(root_details.st_mode)
        or root_lexical.name != "active"
        or root.parent != expected_parent
        or (os.name != "nt" and stat.S_IMODE(root_details.st_mode) & 0o077)
        or (_GETUID is not None and root_details.st_uid != _GETUID())
    ):
        raise ConfigurationError("candidate replacement root is not owner-private")
    if (
        source_lexical.is_symlink()
        or not stat.S_ISDIR(source_details.st_mode)
        or source_root == root
        or not source_root.is_relative_to(root)
        or not source_root.is_dir()
    ):
        raise ConfigurationError("candidate extracted source escaped its preparing root")

    identity = evidence.persistent_tool
    if (
        evidence.desired_fingerprint != desired.fingerprint
        or evidence.relay_install_spec != desired.relay_install_spec
        or not _is_sha256(evidence.source_archive_sha256)
        or desired.relay_artifact_sha256 is None
        or identity.distribution.lower().replace("_", "-") != "clio-relay"
        or desired.relay_install_spec != f"clio-relay=={identity.distribution_version}"
        or identity.entry_point != "clio-relay"
        or identity.source_artifact_sha256 != desired.relay_artifact_sha256
        or identity.uv_version != desired.uv_version
        or identity.uv_executable_sha256 != desired.uv_sha256
        or (
            evidence.coordinator_provider_sha256 is not None
            and evidence.coordinator_provider_sha256 != identity.provider_interpreter_sha256
        )
    ):
        raise ConfigurationError("candidate replacement identity does not match desired state")
    current_provider = Path(sys.executable).absolute()
    try:
        if Path(identity.provider_interpreter).absolute() != current_provider or Path(
            identity.provider_interpreter
        ).resolve(strict=True) != current_provider.resolve(strict=True):
            raise ConfigurationError("candidate planner is not running under its attested provider")
        expected_uv_lexical = root_lexical / "pinned-uv"
        expected_uv_details = expected_uv_lexical.lstat()
        expected_uv = expected_uv_lexical.resolve(strict=True)
        observed_uv = Path(identity.uv_executable).resolve(strict=True)
        if (
            expected_uv_lexical.is_symlink()
            or not stat.S_ISREG(expected_uv_details.st_mode)
            or expected_uv.parent != root
            or expected_uv != observed_uv
        ):
            raise ConfigurationError(
                "candidate replacement did not use the pinned uv executable: "
                f"expected={expected_uv}, observed={observed_uv}, root={root}, "
                f"lexical_symlink={expected_uv_lexical.is_symlink()}, "
                f"regular={stat.S_ISREG(expected_uv_details.st_mode)}"
            )
        environment = Path(identity.environment_prefix).resolve(strict=True)
        imported_module = Path(__file__).resolve(strict=True)
        provider_target = current_provider.resolve(strict=True)
        base_prefix = Path(sys.base_prefix).resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement runtime path is unavailable") from exc
    if (
        os.name != "nt"
        and provider_target != base_prefix
        and not provider_target.is_relative_to(base_prefix)
    ):
        raise ConfigurationError("candidate provider target escaped its Python base prefix")
    staged_paths = (
        identity.tool_directory,
        identity.tool_bin_directory,
        identity.environment_prefix,
        identity.provider_interpreter,
        identity.tool_executable,
        identity.tool_executable_resolved,
        identity.distribution_console_script_path,
        identity.uv_receipt_path,
        identity.distribution_metadata_path,
        identity.source_artifact_path,
        identity.record_path,
    )
    try:
        escaped_paths: list[dict[str, str]] = []
        for value in staged_paths:
            lexical = Path(os.path.abspath(Path(value).expanduser()))
            located = lexical.parent.resolve(strict=True) / lexical.name
            if (
                not lexical.is_absolute()
                or ".." in lexical.parts
                or located == root
                or not located.is_relative_to(root)
                or not lexical.exists()
            ):
                escaped_paths.append(
                    {
                        "lexical": str(lexical)[:512],
                        "located": str(located)[:512],
                    }
                )
        if escaped_paths:
            raise ConfigurationError(
                "candidate replacement runtime escaped its preparing root: "
                + json.dumps(escaped_paths[:16], sort_keys=True, separators=(",", ":"))
            )
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement runtime path is unavailable") from exc
    if imported_module == environment or not imported_module.is_relative_to(environment):
        raise ConfigurationError("candidate planner module was not imported from its uv tool")
    from clio_relay.installation import probe_persistent_uv_tool_identity

    probed_observed = probe_persistent_uv_tool_identity(
        uv_executable=identity.uv_executable,
        tool_executable=identity.tool_executable,
        provider_interpreter=identity.provider_interpreter,
        source_artifact=Path(identity.source_artifact_path),
        distribution="clio-relay",
        distribution_version=identity.distribution_version,
        entry_point="clio-relay",
        tool_directory=identity.tool_directory,
        tool_bin_directory=identity.tool_bin_directory,
        expected_uv_executable_sha256=desired.uv_sha256,
        expected_provider_interpreter_sha256=evidence.coordinator_provider_sha256,
    )
    observed = BootstrapPersistentUvToolIdentity.model_validate(
        probed_observed.model_dump(mode="json")
    )
    if observed != identity:
        raise ConfigurationError("candidate replacement runtime changed during attestation")
