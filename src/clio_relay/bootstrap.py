"""Autonomous installation helpers for desktop and cluster targets."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import platform  # noqa: F401 -- re-exported; tests patch clio_relay.bootstrap.platform.*
import shlex
import shutil  # noqa: F401 -- re-exported; tests patch bootstrap.shutil.which
import stat
import subprocess
import tarfile
import zipfile
from dataclasses import dataclass
from email.message import Message
from email.parser import BytesParser
from email.policy import default
from importlib import resources
from pathlib import Path, PurePosixPath
from time import monotonic
from typing import cast
from uuid import uuid4  # noqa: F401 -- re-exported; bootstrap_ssh_deploy reads bootstrap.uuid4

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import InvalidVersion, Version

from clio_relay import (
    __version__,
    bootstrap_full_activation_staging,
    bootstrap_receipt_validation,
)
from clio_relay.bootstrap_candidate_uv_install_source import (
    _BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE,
    _bootstrap_candidate_package_sources,
)
from clio_relay.bootstrap_constants import (
    BOOTSTRAP_PERSISTENT_RECEIPT_PATH,
    BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS,  # noqa: F401 -- re-exported facade surface
    DEFAULT_REMOTE_CORE_DIR,
    DEFAULT_REMOTE_SPOOL_DIR,
    FRP_LINUX_AMD64_SHA256,  # noqa: F401 -- re-exported facade surface (test_bootstrap.py)
    FRP_VERSION,
    FRPC_LINUX_AMD64_SHA256,
    FRPS_LINUX_AMD64_SHA256,
    JARVIS_CD_VERSION,
    JARVIS_CD_WHEEL_FILENAME,  # noqa: F401 -- re-exported facade surface (test_bootstrap.py)
    JARVIS_CD_WHEEL_SHA256,
    JARVIS_CD_WHEEL_URL,
    JARVIS_UTIL_COMMIT,
    MAX_RELAY_WHEEL_METADATA_BYTES,
    UV_LINUX_AMD64_ARCHIVE_SHA256,  # noqa: F401 -- re-exported facade surface (test_bootstrap.py)
    UV_LINUX_AMD64_EXECUTABLE_SHA256,
    UV_VERSION,
)
from clio_relay.bootstrap_frp_local_install import (  # noqa: F401 -- re-exported facade surface
    _assert_frp_pair,
    _install_frp_from_release_archive,
    install_local_frp,
)
from clio_relay.bootstrap_pinned_copy_sources import (
    _BOOTSTRAP_PINNED_LOCAL_ARTIFACT_COPY_SOURCE,
    _BOOTSTRAP_PINNED_UV_COPY_SOURCE,
)
from clio_relay.bootstrap_preparing_root_source import _BOOTSTRAP_PREPARING_ROOT_SOURCE
from clio_relay.bootstrap_receipt_classifier_source import (
    _BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE,  # noqa: F401 -- re-exported facade surface
)
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
)
from clio_relay.bootstrap_reconcile_script_activation import reconcile_script_activation
from clio_relay.bootstrap_reconcile_script_commit import reconcile_script_commit
from clio_relay.bootstrap_reconcile_script_generation_prepare import (
    reconcile_script_generation_prepare,
)
from clio_relay.bootstrap_reconcile_script_recovery import reconcile_script_recovery
from clio_relay.bootstrap_script_commit import script_commit
from clio_relay.bootstrap_script_jarvis_repo_setup import script_jarvis_repo_setup
from clio_relay.bootstrap_script_jarvis_state import script_jarvis_state
from clio_relay.bootstrap_script_preamble import script_preamble
from clio_relay.bootstrap_script_provider_install import script_provider_install
from clio_relay.bootstrap_ssh_deploy import (  # noqa: F401 -- re-exported facade surface
    _bootstrap_preflight_over_ssh,
    bootstrap_cluster_over_ssh,
)
from clio_relay.bootstrap_staged_provider_source import (
    _STAGED_PROVIDER_ENVIRONMENT_SANITIZER,
    _STAGED_PROVIDER_EXEC_PROGRAM,
)
from clio_relay.bootstrap_worker_fence_script import _worker_upgrade_fence_script
from clio_relay.bootstrap_worker_proof_source import (  # noqa: F401 -- re-exported test/debug reads
    _WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON,
    _WORKER_WRITER_PROOF_PYTHON,
)
from clio_relay.bounded_process import (
    BoundedProcessError,
    BoundedProcessOutputLimit,
    BoundedProcessTimeout,
    run_bounded_process,
)
from clio_relay.deployment import endpoint_user_service_name
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,  # noqa: F401 -- re-exported facade surface (test_bootstrap.py)
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.remote_values import render_remote_shell_path


@dataclass(frozen=True)
class BootstrapArchive:
    """Remote bootstrap archive and relay install source."""

    archive: Path
    install_spec: str


@dataclass(frozen=True)
class BootstrapRelayIdentity:
    """Payload-independent identity used for an exact remote preflight."""

    install_spec: str
    transport_install_spec: str
    source_identity: str
    deployment_artifact_sha256: str | None


@dataclass(frozen=True)
class BootstrapPreflightResult:
    """One typed payload-free bootstrap inspection result."""

    action: str
    receipt: dict[str, object] | None
    lines: list[str]


def bootstrap_relay_identity(
    *,
    source_root: Path,
    relay_wheel: Path | None,
    relay_artifact_sha256: str | None,
) -> BootstrapRelayIdentity:
    """Derive desired relay identity without reading or building its payload."""
    if relay_wheel is not None:
        if relay_artifact_sha256 is None:
            raise ConfigurationError(
                "a relay bootstrap wheel requires its expected SHA-256 before preflight"
            )
        if not bootstrap_receipt_validation.is_sha256_value(relay_artifact_sha256):
            raise ConfigurationError("relay bootstrap wheel SHA-256 must be lowercase hex")
        if (
            relay_wheel.name != str(relay_wheel.name).strip()
            or any(character in relay_wheel.name for character in "\x00\r\n")
            or not relay_wheel.name.endswith(".whl")
        ):
            raise ConfigurationError("relay bootstrap wheel name is invalid")
        try:
            distribution, version, _build, _tags = parse_wheel_filename(relay_wheel.name)
        except InvalidWheelFilename as exc:
            raise ConfigurationError("relay bootstrap wheel filename is invalid") from exc
        if distribution != canonicalize_name("clio-relay") or version != Version(__version__):
            raise ConfigurationError(
                "relay bootstrap wheel must match the running clio-relay release"
            )
        return BootstrapRelayIdentity(
            install_spec=f"clio-relay=={version}",
            transport_install_spec=f"$DEST/wheels/{relay_wheel.name}",
            source_identity=(f"release:clio-relay=={version}:sha256:{relay_artifact_sha256}"),
            deployment_artifact_sha256=relay_artifact_sha256,
        )
    if _is_clio_relay_git_checkout(source_root):
        assert_clean_git_checkout(source_root)
        first = _git_checkout_identity(source_root)
        if _git_checkout_identity(source_root) != first:
            raise ConfigurationError("git checkout changed while deriving bootstrap identity")
        return BootstrapRelayIdentity(
            install_spec="$DEST",
            transport_install_spec="$DEST",
            source_identity=f"git:commit:{first[0]}:tree:{first[1]}",
            deployment_artifact_sha256=None,
        )
    if relay_artifact_sha256 is None:
        raise ConfigurationError(
            "released bootstrap requires --relay-artifact-sha256 from the exact wheel; "
            "this preserves offline identity and distinguishes rebuilt artifacts"
        )
    if not bootstrap_receipt_validation.is_sha256_value(relay_artifact_sha256):
        raise ConfigurationError("relay release artifact SHA-256 must be lowercase hex")
    install_spec = f"clio-relay=={__version__}"
    return BootstrapRelayIdentity(
        install_spec=install_spec,
        transport_install_spec=install_spec,
        source_identity=f"release:{install_spec}:sha256:{relay_artifact_sha256}",
        deployment_artifact_sha256=relay_artifact_sha256,
    )


def _git_checkout_identity(source_root: Path) -> tuple[str, str]:
    result = _run(
        ["git", "rev-parse", "HEAD", "HEAD^{tree}"],
        cwd=source_root,
        timeout_seconds=20,
    )
    values = result.stdout.splitlines()
    if len(values) != 2 or any(
        len(value) not in {40, 64}
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
        for value in values
    ):
        raise ConfigurationError("git checkout omitted a canonical commit/tree identity")
    return values[0], values[1]


def _bootstrap_desired_state(
    *,
    identity: BootstrapRelayIdentity,
    cluster: str | None,
    core_dir: str,
    spool_dir: str,
    frp_version: str,
    clio_kit_install_spec: str,
    clio_kit_artifact_sha256: str,
    agent_adapter: str,
    agent_npm_package: str | None,
    agent_npm_bin: str | None,
    agent_args: list[str],
    jarvis_resource_graph_profile: str | None = None,
    allow_jarvis_resource_graph_build: bool = False,
) -> BootstrapDesiredState:
    """Build one canonical deployed-state identity without transport fields."""
    return BootstrapDesiredState(
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        worker_service=(endpoint_user_service_name(cluster) if cluster is not None else None),
        relay_install_spec=identity.install_spec,
        relay_artifact_sha256=identity.deployment_artifact_sha256,
        relay_source_identity=identity.source_identity,
        frp_version=frp_version,
        frpc_sha256=FRPC_LINUX_AMD64_SHA256,
        frps_sha256=FRPS_LINUX_AMD64_SHA256,
        uv_version=UV_VERSION,
        uv_sha256=UV_LINUX_AMD64_EXECUTABLE_SHA256,
        jarvis_util_commit=JARVIS_UTIL_COMMIT,
        jarvis_cd_version=JARVIS_CD_VERSION,
        jarvis_cd_wheel_url=JARVIS_CD_WHEEL_URL,
        jarvis_cd_wheel_sha256=JARVIS_CD_WHEEL_SHA256,
        jarvis_resource_graph_profile=jarvis_resource_graph_profile,
        allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
        clio_kit_install_spec=clio_kit_install_spec,
        clio_kit_version=CLIO_KIT_JARVIS_MCP_VERSION,
        clio_kit_artifact_sha256=clio_kit_artifact_sha256,
        agent_adapter=agent_adapter,
        agent_npm_package=agent_npm_package,
        agent_npm_bin=agent_npm_bin,
        agent_args=agent_args,
    )


def _read_stability_identity(details: os.stat_result) -> tuple[int, ...]:
    """Return a before/after-read identity with stable Windows semantics."""
    identity = (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
    )
    if os.name == "nt" and stat.S_ISREG(details.st_mode):
        return identity
    return (*identity, details.st_ctime_ns)


def _sha256_regular_file(path: Path) -> str:
    """Hash one regular file without loading it into memory."""
    digest = hashlib.sha256()
    try:
        before = path.lstat()
        if path.is_symlink() or not path.is_file():
            raise ConfigurationError(f"bootstrap payload is not a regular file: {path}")
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        after = path.lstat()
    except OSError as exc:
        raise ConfigurationError(f"bootstrap payload could not be hashed: {path}") from exc
    # Opening a Windows file may churn ctime. Device, inode, mode, size, and
    # mtime still detect real cross-open changes; the computed SHA-256 pins bytes.
    identity_before = _read_stability_identity(before)
    identity_after = _read_stability_identity(after)
    if identity_after != identity_before:
        raise ConfigurationError("bootstrap payload changed while hashing")
    return digest.hexdigest()


def _verify_persistent_bootstrap_receipt(
    *,
    ssh_host: str,
    receipt: dict[str, object],
    timeout_seconds: float,
) -> None:
    """Require persistent receipt bytes to match current invocation evidence."""
    receipt_result = _run(
        [
            "ssh",
            ssh_host,
            "cat",
            BOOTSTRAP_PERSISTENT_RECEIPT_PATH,
        ],
        timeout_seconds=min(10, timeout_seconds),
        stdout_maximum_bytes=1024 * 1024,
        stderr_maximum_bytes=16 * 1024,
    )
    if len(receipt_result.stdout.encode()) > 1024 * 1024:
        raise RelayError("persistent bootstrap receipt exceeds the bounded size")
    try:
        persistent = cast(object, json.loads(receipt_result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError(f"persistent bootstrap receipt was not valid JSON: {exc}") from exc
    if persistent != receipt:
        raise RelayError("persistent bootstrap receipt differs from current stdout evidence")


def _remaining_public_deadline(deadline: float, *, action: str) -> float:
    """Return a positive shared host-side deadline for one public bootstrap phase."""
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise RelayError(f"bootstrap {action} exceeded its public deadline")
    return remaining


def package_source_root() -> Path:
    """Return the project root for editable installs, or the package root for wheels."""
    return Path(__file__).resolve().parents[2]


def _pinned_artifact_fetch_shell_function() -> str:
    """Render the digest-pinned HTTPS or managed local artifact fetcher."""
    local_copy_program = shlex.quote(_BOOTSTRAP_PINNED_LOCAL_ARTIFACT_COPY_SOURCE)
    return f"""bootstrap_fetch_exact_artifact() {{
  local source_url="$1"
  local expected_sha256="$2"
  local destination="$3"
  case "$expected_sha256" in
    (*[!0-9a-f]*|'')
      echo "bootstrap artifact digest is invalid" >&2
      return 1
      ;;
  esac
  if [ "${{#expected_sha256}}" -ne 64 ]; then
    echo "bootstrap artifact digest has an invalid length" >&2
    return 1
  fi
  case "$source_url" in
    file://*)
      python3 -I -c {local_copy_program} \
        "$source_url" "$expected_sha256" "$destination" \
        "$HOME/.local/share/clio-relay/candidate-wheels"
      ;;
    https://*)
      curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --retry 3 --retry-all-errors --retry-max-time 180 \
        --connect-timeout 20 --max-time 180 \
        --output "$destination" "$source_url"
      echo "$expected_sha256 *$destination" | sha256sum --check --strict -
      ;;
    *)
      echo "bootstrap artifact URL must use HTTPS or managed file staging" >&2
      return 1
      ;;
  esac
}}"""


def _relay_only_reconcile_script(
    *,
    worker_fence: str,
    worker_recheck: str,
    init_command: str,
    worker_restart: str,
    rendered_core_dir: str,
    rendered_spool_dir: str,
    rendered_agent_adapter: str,
    rendered_agent_args: str,
    rendered_relay_install_spec: str,
    rendered_relay_artifact_sha256: str,
    rendered_jarvis_mcp_install_spec: str,
    rendered_jarvis_mcp_artifact_sha256: str,
    rendered_source_archive: str,
    rendered_source_archive_sha256: str,
    invocation_id: str,
    candidate_uv_install_program: str,
) -> str:
    """Render the staged relay-only generation transaction."""
    staged_provider_exec_program = shlex.quote(_STAGED_PROVIDER_EXEC_PROGRAM)
    staged_provider_environment_sanitizer = _STAGED_PROVIDER_ENVIRONMENT_SANITIZER
    reconcile_kwargs = dict(
        worker_fence=worker_fence,
        worker_recheck=worker_recheck,
        init_command=init_command,
        worker_restart=worker_restart,
        rendered_core_dir=rendered_core_dir,
        rendered_spool_dir=rendered_spool_dir,
        rendered_agent_adapter=rendered_agent_adapter,
        rendered_agent_args=rendered_agent_args,
        rendered_relay_install_spec=rendered_relay_install_spec,
        rendered_relay_artifact_sha256=rendered_relay_artifact_sha256,
        rendered_jarvis_mcp_install_spec=rendered_jarvis_mcp_install_spec,
        rendered_jarvis_mcp_artifact_sha256=rendered_jarvis_mcp_artifact_sha256,
        rendered_source_archive=rendered_source_archive,
        rendered_source_archive_sha256=rendered_source_archive_sha256,
        invocation_id=invocation_id,
        candidate_uv_install_program=candidate_uv_install_program,
        staged_provider_exec_program=staged_provider_exec_program,
        staged_provider_environment_sanitizer=staged_provider_environment_sanitizer,
    )
    return (
        reconcile_script_recovery(**reconcile_kwargs)
        + reconcile_script_generation_prepare(**reconcile_kwargs)
        + reconcile_script_activation(**reconcile_kwargs)
        + reconcile_script_commit(**reconcile_kwargs)
    )


def render_linux_user_bootstrap_script(
    *,
    frp_version: str = FRP_VERSION,
    cluster: str | None = None,
    core_dir: str = DEFAULT_REMOTE_CORE_DIR,
    spool_dir: str = DEFAULT_REMOTE_SPOOL_DIR,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    jarvis_resource_graph_profile: str | None = None,
    allow_jarvis_resource_graph_build: bool = False,
    relay_install_spec: str = "$DEST",
    relay_deployment_install_spec: str | None = None,
    relay_artifact_sha256: str | None = None,
    relay_source_identity: str | None = None,
    jarvis_mcp_install_spec: str | None = None,
    jarvis_mcp_artifact_sha256: str | None = None,
    invocation_id: str = "manual",
    source_archive: str = "/tmp/clio-relay-head.tar",
    source_archive_sha256: str | None = None,
) -> str:
    """Render the idempotent shell script used for the current Linux cluster bootstrap."""
    rendered_core_dir = render_remote_shell_path(core_dir, field="core_dir")
    rendered_spool_dir = render_remote_shell_path(spool_dir, field="spool_dir")
    worker_fence, worker_recheck, init_command, worker_restart = _worker_upgrade_fence_script(
        cluster,
        rendered_core_dir=rendered_core_dir,
    )
    rendered_agent_adapter = shlex.quote(agent_adapter)
    rendered_agent_args = shlex.quote(" ".join(agent_args or []))
    rendered_agent_npm_package = shlex.quote(agent_npm_package or "")
    rendered_agent_npm_bin = shlex.quote(agent_npm_bin or "")
    rendered_jarvis_resource_graph_profile = shlex.quote(jarvis_resource_graph_profile or "")
    rendered_allow_jarvis_resource_graph_build = "1" if allow_jarvis_resource_graph_build else "0"
    rendered_relay_install_spec = _render_relay_install_spec(relay_install_spec)
    rendered_candidate_relay_install_spec = shlex.quote(relay_install_spec)
    resolved_relay_deployment_install_spec = relay_deployment_install_spec or relay_install_spec
    source_archive_path = PurePosixPath(source_archive)
    if (
        not source_archive_path.is_absolute()
        or ".." in source_archive_path.parts
        or any(character in source_archive for character in "\x00\r\n")
    ):
        raise ConfigurationError("bootstrap source archive must be one safe absolute path")
    if source_archive_sha256 is not None and (
        len(source_archive_sha256) != 64
        or any(character not in "0123456789abcdef" for character in source_archive_sha256)
    ):
        raise ConfigurationError("bootstrap source archive SHA-256 must be lowercase hex")
    rendered_source_archive = shlex.quote(source_archive)
    rendered_source_archive_sha256 = shlex.quote(source_archive_sha256 or "")
    if relay_artifact_sha256 is not None and (
        len(relay_artifact_sha256) != 64
        or any(character not in "0123456789abcdef" for character in relay_artifact_sha256)
    ):
        raise ConfigurationError("relay bootstrap wheel SHA-256 must be lowercase hex")
    if relay_install_spec.endswith(".whl") and relay_artifact_sha256 is None:
        raise ConfigurationError("a relay bootstrap wheel requires its expected SHA-256")
    rendered_relay_artifact_sha256 = shlex.quote(relay_artifact_sha256 or "")
    resolved_relay_source_identity = relay_source_identity or (
        (f"release:{resolved_relay_deployment_install_spec}:sha256:{relay_artifact_sha256}")
        if relay_artifact_sha256 is not None
        else f"install-spec:{resolved_relay_deployment_install_spec}"
    )
    if frp_version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Linux checksum is registered for frp {frp_version}")
    resolved_jarvis_mcp_install_spec = jarvis_mcp_install_spec or os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC",
        CLIO_KIT_JARVIS_MCP_WHEEL_URL,
    )
    resolved_jarvis_mcp_artifact_sha256 = (
        jarvis_mcp_artifact_sha256
        or os.environ.get("CLIO_RELAY_JARVIS_MCP_ARTIFACT_SHA256")
        or (
            CLIO_KIT_JARVIS_MCP_WHEEL_SHA256
            if resolved_jarvis_mcp_install_spec == CLIO_KIT_JARVIS_MCP_WHEEL_URL
            else None
        )
    )
    if resolved_jarvis_mcp_artifact_sha256 is None:
        raise ConfigurationError(
            "a custom clio-kit bootstrap source requires its expected wheel SHA-256"
        )
    if len(resolved_jarvis_mcp_artifact_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in resolved_jarvis_mcp_artifact_sha256
    ):
        raise ConfigurationError("clio-kit bootstrap wheel SHA-256 must be lowercase hex")
    if resolved_jarvis_mcp_artifact_sha256 != CLIO_KIT_JARVIS_MCP_WHEEL_SHA256:
        raise ConfigurationError(
            "the built-in JARVIS MCP bootstrap requires the released clio-kit wheel; "
            "register a different JARVIS server through the generic remote MCP registry"
        )
    if resolved_jarvis_mcp_install_spec.startswith("clio-kit==") and (
        resolved_jarvis_mcp_install_spec != f"clio-kit=={CLIO_KIT_JARVIS_MCP_VERSION}"
    ):
        raise ConfigurationError(
            "the built-in JARVIS MCP bootstrap requires the released clio-kit version"
        )
    rendered_jarvis_mcp_install_spec = shlex.quote(resolved_jarvis_mcp_install_spec)
    rendered_jarvis_mcp_artifact_sha256 = shlex.quote(resolved_jarvis_mcp_artifact_sha256)
    desired_state = _bootstrap_desired_state(
        identity=BootstrapRelayIdentity(
            install_spec=resolved_relay_deployment_install_spec,
            transport_install_spec=relay_install_spec,
            source_identity=resolved_relay_source_identity,
            deployment_artifact_sha256=relay_artifact_sha256,
        ),
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        frp_version=frp_version,
        clio_kit_install_spec=resolved_jarvis_mcp_install_spec,
        clio_kit_artifact_sha256=resolved_jarvis_mcp_artifact_sha256,
        agent_adapter=agent_adapter,
        agent_npm_package=agent_npm_package,
        agent_npm_bin=agent_npm_bin,
        agent_args=agent_args or [],
        jarvis_resource_graph_profile=jarvis_resource_graph_profile,
        allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
    )
    worker_service = desired_state.worker_service
    rendered_desired_state = shlex.quote(
        json.dumps(desired_state.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    )
    candidate_package_sources = _bootstrap_candidate_package_sources()
    rendered_candidate_package_sources = json.dumps(
        {
            name: base64.b64encode(payload).decode("ascii")
            for name, payload in sorted(candidate_package_sources.items())
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    preparing_root_program = shlex.quote(_BOOTSTRAP_PREPARING_ROOT_SOURCE)
    pinned_uv_copy_program = shlex.quote(_BOOTSTRAP_PINNED_UV_COPY_SOURCE)
    candidate_uv_install_program = shlex.quote(_BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE)
    artifact_fetch_function = _pinned_artifact_fetch_shell_function()
    candidate_package_sha256 = {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in candidate_package_sources.items()
    }
    candidate_reconcile_sha256 = candidate_package_sha256["bootstrap_reconcile.py"]
    candidate_provider_build_info_sha256 = candidate_package_sha256[
        "bootstrap_provider_build_info.py"
    ]
    candidate_bounded_process_sha256 = candidate_package_sha256["bounded_process.py"]
    candidate_errors_sha256 = candidate_package_sha256["errors.py"]
    candidate_process_containment_sha256 = candidate_package_sha256["process_containment.py"]
    candidate_safe_archive_sha256 = candidate_package_sha256["safe_archive.py"]
    bootstrap_journal_source = Path(__file__).with_name("bootstrap_journal.py").read_bytes()
    rendered_bootstrap_journal_source = base64.b64encode(bootstrap_journal_source).decode("ascii")
    relay_only_reconcile = _relay_only_reconcile_script(
        worker_fence=worker_fence,
        worker_recheck=worker_recheck,
        init_command=init_command,
        worker_restart=worker_restart,
        rendered_core_dir=rendered_core_dir,
        rendered_spool_dir=rendered_spool_dir,
        rendered_agent_adapter=rendered_agent_adapter,
        rendered_agent_args=rendered_agent_args,
        rendered_relay_install_spec=rendered_relay_install_spec,
        rendered_relay_artifact_sha256=rendered_relay_artifact_sha256,
        rendered_jarvis_mcp_install_spec=rendered_jarvis_mcp_install_spec,
        rendered_jarvis_mcp_artifact_sha256=rendered_jarvis_mcp_artifact_sha256,
        rendered_source_archive=rendered_source_archive,
        rendered_source_archive_sha256=rendered_source_archive_sha256,
        invocation_id=invocation_id,
        candidate_uv_install_program=candidate_uv_install_program,
    )
    # clio-relay#257: rendered-script chunks from bootstrap_full_activation_staging.
    bfas = bootstrap_full_activation_staging
    ownership_proof_adoption_python = bfas.ownership_proof_populated_host_adoption_python()
    stable_activation_link_adoption = bfas.stable_activation_link_adoption_shell()
    shared_directory_mkdir_owned_helper = bfas.shared_directory_mkdir_owned_helper_shell()
    script_kwargs = dict(
        artifact_fetch_function=artifact_fetch_function,
        candidate_bounded_process_sha256=candidate_bounded_process_sha256,
        candidate_errors_sha256=candidate_errors_sha256,
        candidate_process_containment_sha256=candidate_process_containment_sha256,
        candidate_provider_build_info_sha256=candidate_provider_build_info_sha256,
        candidate_reconcile_sha256=candidate_reconcile_sha256,
        candidate_safe_archive_sha256=candidate_safe_archive_sha256,
        candidate_uv_install_program=candidate_uv_install_program,
        cluster=cluster,
        frp_version=frp_version,
        init_command=init_command,
        invocation_id=invocation_id,
        ownership_proof_adoption_python=ownership_proof_adoption_python,
        pinned_uv_copy_program=pinned_uv_copy_program,
        preparing_root_program=preparing_root_program,
        relay_only_reconcile=relay_only_reconcile,
        rendered_agent_adapter=rendered_agent_adapter,
        rendered_agent_args=rendered_agent_args,
        rendered_agent_npm_bin=rendered_agent_npm_bin,
        rendered_agent_npm_package=rendered_agent_npm_package,
        rendered_allow_jarvis_resource_graph_build=rendered_allow_jarvis_resource_graph_build,
        rendered_bootstrap_journal_source=rendered_bootstrap_journal_source,
        rendered_candidate_package_sources=rendered_candidate_package_sources,
        rendered_candidate_relay_install_spec=rendered_candidate_relay_install_spec,
        rendered_core_dir=rendered_core_dir,
        rendered_desired_state=rendered_desired_state,
        rendered_jarvis_mcp_artifact_sha256=rendered_jarvis_mcp_artifact_sha256,
        rendered_jarvis_mcp_install_spec=rendered_jarvis_mcp_install_spec,
        rendered_jarvis_resource_graph_profile=rendered_jarvis_resource_graph_profile,
        rendered_relay_artifact_sha256=rendered_relay_artifact_sha256,
        rendered_relay_install_spec=rendered_relay_install_spec,
        rendered_source_archive=rendered_source_archive,
        rendered_source_archive_sha256=rendered_source_archive_sha256,
        rendered_spool_dir=rendered_spool_dir,
        shared_directory_mkdir_owned_helper=shared_directory_mkdir_owned_helper,
        stable_activation_link_adoption=stable_activation_link_adoption,
        worker_fence=worker_fence,
        worker_recheck=worker_recheck,
        worker_restart=worker_restart,
        worker_service=worker_service,
    )
    script = (
        "set -euo pipefail\n"
        + script_preamble(**script_kwargs)
        + script_jarvis_state(**script_kwargs)
        + script_provider_install(**script_kwargs)
        + script_jarvis_repo_setup(**script_kwargs)
        + script_commit(**script_kwargs)
    )
    return script.replace("\r\n", "\n")


def _render_relay_install_spec(relay_install_spec: str) -> str:
    if relay_install_spec == "$DEST":
        return '"$DEST"'
    if relay_install_spec.startswith("$DEST/"):
        return '"$DEST"/' + shlex.quote(relay_install_spec.removeprefix("$DEST/"))
    return shlex.quote(relay_install_spec)


def _validate_relay_bootstrap_wheel(path: Path) -> str:
    """Validate one local relay wheel before any remote bootstrap mutation."""
    try:
        details = path.lstat()
    except OSError as exc:
        raise ConfigurationError(f"could not inspect relay bootstrap wheel {path}: {exc}") from exc
    if path.is_symlink() or not stat.S_ISREG(details.st_mode):
        raise ConfigurationError(f"relay bootstrap wheel must be one regular file: {path}")

    try:
        project, version, _build, _tags = parse_wheel_filename(path.name)
    except InvalidWheelFilename as exc:
        raise ConfigurationError(
            f"relay bootstrap wheel filename is not canonical: {path.name}: {exc}"
        ) from exc
    if project != canonicalize_name("clio-relay"):
        raise ConfigurationError(
            f"relay bootstrap wheel distribution must be clio-relay, got {project}"
        )

    metadata = _read_relay_wheel_metadata(path)
    names = metadata.get_all("Name", [])
    versions = metadata.get_all("Version", [])
    if len(names) != 1 or not str(names[0]).strip():
        raise ConfigurationError("relay bootstrap wheel METADATA must contain exactly one Name")
    if len(versions) != 1 or not str(versions[0]).strip():
        raise ConfigurationError("relay bootstrap wheel METADATA must contain exactly one Version")
    metadata_name = str(names[0]).strip()
    metadata_version = str(versions[0]).strip()
    if canonicalize_name(metadata_name) != project:
        raise ConfigurationError("relay bootstrap wheel METADATA Name does not match its filename")
    try:
        parsed_metadata_version = Version(metadata_version)
    except InvalidVersion as exc:
        raise ConfigurationError(
            f"relay bootstrap wheel METADATA Version is invalid: {metadata_version}"
        ) from exc
    if parsed_metadata_version != version:
        raise ConfigurationError(
            "relay bootstrap wheel METADATA Version does not match its filename"
        )
    try:
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    except OSError as exc:
        raise ConfigurationError(f"could not hash relay bootstrap wheel {path}: {exc}") from exc


def _read_relay_wheel_metadata(path: Path) -> Message:
    """Read bounded core metadata from one wheel without executing package code."""
    try:
        with zipfile.ZipFile(path) as archive:
            candidates = [
                member
                for member in archive.infolist()
                if not member.is_dir()
                and member.filename.count("/") == 1
                and member.filename.endswith(".dist-info/METADATA")
            ]
            if len(candidates) != 1:
                raise ConfigurationError(
                    "relay bootstrap wheel must contain exactly one top-level METADATA file"
                )
            member = candidates[0]
            if not 1 <= member.file_size <= MAX_RELAY_WHEEL_METADATA_BYTES:
                raise ConfigurationError("relay bootstrap wheel METADATA size is invalid")
            with archive.open(member) as stream:
                content = stream.read(MAX_RELAY_WHEEL_METADATA_BYTES + 1)
    except ConfigurationError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, NotImplementedError) as exc:
        raise ConfigurationError(f"could not inspect relay bootstrap wheel {path}: {exc}") from exc
    if len(content) > MAX_RELAY_WHEEL_METADATA_BYTES:
        raise ConfigurationError("relay bootstrap wheel METADATA exceeds the size limit")
    return BytesParser(policy=default).parsebytes(content, headersonly=True)


def create_bootstrap_archive(
    *,
    source_root: Path,
    archive: Path,
    relay_wheel: Path | None = None,
) -> BootstrapArchive:
    """Create the archive used by remote bootstrap.

    A clean git checkout deploys that exact committed tree. Installed-package
    runs deploy packaged JARVIS assets and install either the supplied candidate
    wheel or the exact package version, so bootstrap does not require a checkout.
    """
    if relay_wheel is not None:
        _write_packaged_bootstrap_archive(archive, relay_wheel=relay_wheel)
        return BootstrapArchive(
            archive=archive,
            install_spec=f"$DEST/wheels/{relay_wheel.name}",
        )
    if _is_clio_relay_git_checkout(source_root):
        assert_clean_git_checkout(source_root)
        _run(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source_root)
        return BootstrapArchive(archive=archive, install_spec="$DEST")
    _write_packaged_bootstrap_archive(archive, relay_wheel=None)
    return BootstrapArchive(archive=archive, install_spec=f"clio-relay=={__version__}")


def _write_packaged_bootstrap_archive(archive: Path, *, relay_wheel: Path | None) -> None:
    if relay_wheel is not None and not relay_wheel.is_file():
        raise ConfigurationError(f"relay wheel does not exist: {relay_wheel}")
    assets = resources.files("clio_relay").joinpath("assets", "jarvis-packages")
    source_assets = Path(__file__).resolve().parents[2] / "jarvis-packages"
    with tarfile.open(archive, "w") as tar:
        if relay_wheel is not None:
            _add_canonical_archive_member(
                tar=tar,
                source=relay_wheel,
                arcname=PurePosixPath("wheels", relay_wheel.name),
            )
        if assets.is_dir():
            with resources.as_file(assets) as asset_path:
                _add_jarvis_assets_to_archive(tar=tar, asset_path=asset_path)
            return
        if source_assets.is_dir():
            _add_jarvis_assets_to_archive(tar=tar, asset_path=source_assets)
            return
    raise ConfigurationError("installed clio-relay package does not include jarvis package assets")


def _is_clio_relay_git_checkout(source_root: Path) -> bool:
    pyproject = source_root / "pyproject.toml"
    if not (source_root / ".git").exists() or not pyproject.exists():
        return False
    try:
        text = pyproject.read_text(encoding="utf-8")
    except OSError:
        return False
    return 'name = "clio-relay"' in text


def _add_jarvis_assets_to_archive(*, tar: tarfile.TarFile, asset_path: Path) -> None:
    for item in sorted(
        asset_path.rglob("*"),
        key=lambda path: path.relative_to(asset_path).as_posix(),
    ):
        relative_parts = item.relative_to(asset_path).parts
        if "__pycache__" in relative_parts or item.name.endswith(".pyc"):
            continue
        _add_canonical_archive_member(
            tar=tar,
            source=item,
            arcname=PurePosixPath("jarvis-packages", *relative_parts),
        )


def _add_canonical_archive_member(
    *,
    tar: tarfile.TarFile,
    source: Path,
    arcname: PurePosixPath,
) -> None:
    """Add one deterministic regular file or directory to a bootstrap tar."""
    try:
        details = source.lstat()
    except OSError as exc:
        raise ConfigurationError(f"bootstrap archive member is unavailable: {source}") from exc
    # Opening a Windows regular file may churn ctime. Device, inode, mode, size,
    # and mtime still detect real changes, and the final source-archive SHA-256
    # pins the emitted bytes; directory comparisons continue to include ctime.
    identity = _read_stability_identity(details)
    if source.is_symlink() or not (stat.S_ISREG(details.st_mode) or stat.S_ISDIR(details.st_mode)):
        raise ConfigurationError(f"bootstrap archive member is not a regular file: {source}")
    info = tar.gettarinfo(str(source), arcname=arcname.as_posix())
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.pax_headers = {}
    info.mode = 0o755 if stat.S_ISDIR(details.st_mode) or details.st_mode & 0o111 else 0o644
    if stat.S_ISDIR(details.st_mode):
        tar.addfile(info)
    else:
        try:
            with source.open("rb") as stream:
                tar.addfile(info, stream)
        except OSError as exc:
            raise ConfigurationError(
                f"bootstrap archive member could not be read: {source}"
            ) from exc
    after = source.lstat()
    if _read_stability_identity(after) != identity:
        raise ConfigurationError(f"bootstrap archive member changed while reading: {source}")


def assert_clean_git_checkout(source_root: Path) -> None:
    """Raise if source_root has uncommitted changes that git archive would omit."""
    result = _run(
        ["git", "status", "--porcelain"],
        cwd=source_root,
        timeout_seconds=20,
        stdout_maximum_bytes=1024 * 1024,
        stderr_maximum_bytes=64 * 1024,
    )
    if result.stdout.strip():
        raise ConfigurationError(
            "remote bootstrap deploys git HEAD; commit or stash local changes before bootstrap"
        )


def _validate_ssh_destination(value: str) -> None:
    """Reject SSH destinations that could be parsed as client options."""
    if (
        not value
        or value != value.strip()
        or value.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise ConfigurationError(
            "ssh host must be one non-option destination without whitespace or controls"
        )


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    input_bytes: bytes | None = None,
    timeout_seconds: float | None = None,
    stdout_maximum_bytes: int = 2 * 1024 * 1024,
    stderr_maximum_bytes: int = 64 * 1024,
) -> subprocess.CompletedProcess[str]:
    """Run one local transport command with finite time and output bounds.

    ``input_bytes`` carries a payload on stdin, which is how any
    script-sized content must reach a remote shell: command-line arguments
    are subject to client length limits that truncate SILENTLY (#158).
    """
    env = os.environ.copy()
    effective_timeout = 120.0 if timeout_seconds is None else timeout_seconds
    try:
        result = run_bounded_process(
            command,
            cwd=cwd,
            environment=env,
            input_bytes=input_bytes,
            timeout_seconds=effective_timeout,
            stdout_maximum_bytes=stdout_maximum_bytes,
            stderr_maximum_bytes=stderr_maximum_bytes,
        )
    except BoundedProcessTimeout as exc:
        raise RelayError(
            f"command exceeded {effective_timeout:g} seconds ({' '.join(command)})"
        ) from exc
    except BoundedProcessOutputLimit as exc:
        raise RelayError(f"command exceeded its output bound ({' '.join(command)})") from exc
    except (OSError, BoundedProcessError) as exc:
        raise RelayError(f"command containment failed ({' '.join(command)}): {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"command failed ({' '.join(command)}): {detail}")
    return result
