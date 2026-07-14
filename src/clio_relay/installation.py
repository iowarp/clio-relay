"""Durable identity for the exact clio-relay artifact installed on a cluster."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

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
NATIVE_JARVIS_CAPABILITY_SCHEMA = "clio-relay.jarvis-native-execution-capability.v1"
PERSISTENT_UV_TOOL_IDENTITY_SCHEMA = "clio-relay.persistent-uv-tool-identity.v2"
MAX_UV_TOOL_RECEIPT_BYTES = 256 * 1024
JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"
JARVIS_EXECUTION_ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
JARVIS_ARTIFACT_SCHEMA = "jarvis.artifact.v1"
CLIO_KIT_JARVIS_EXECUTION_SCHEMA = "clio-kit.jarvis-execution.v1"
CLIO_KIT_JARVIS_CONTRACT_ID = "clio-kit-jarvis-user-v3"
CLIO_KIT_MCP_CONTRACT_SCHEMA = "clio-kit.mcp-user-contract.v1"
CLIO_KIT_NATIVE_OPERATIONS = (
    "jarvis_get_execution",
    "jarvis_run",
)
JARVIS_CD_NATIVE_OPERATIONS = (
    "execution_handle.progress",
    "pipeline.get_execution",
    "pipeline.get_execution_progress",
    "pipeline.run",
)


class NativeJarvisExecutionCapability(BaseModel):
    """Receipt-bound native execution and progress contract for one component."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.jarvis-native-execution-capability.v1"] = (
        NATIVE_JARVIS_CAPABILITY_SCHEMA
    )
    handle_schema: Literal["jarvis.execution.handle.v1"] = JARVIS_EXECUTION_HANDLE_SCHEMA
    record_schema: Literal["jarvis.execution.record.v1"] = JARVIS_EXECUTION_RECORD_SCHEMA
    progress_schema: Literal["jarvis.execution.progress.v1"] = JARVIS_EXECUTION_PROGRESS_SCHEMA
    operations: list[str] = Field(min_length=1)
    contract_id: str | None = None
    contract_schema_version: str | None = None
    contract_sha256: str | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> NativeJarvisExecutionCapability:
        """Reject ambiguous operation or optional contract identities."""
        if any(not operation or operation.strip() != operation for operation in self.operations):
            raise ValueError("native JARVIS capability operations must be non-empty strings")
        if len(set(self.operations)) != len(self.operations) or self.operations != sorted(
            self.operations
        ):
            raise ValueError("native JARVIS capability operations must be unique and sorted")
        contract_values = (
            self.contract_id,
            self.contract_schema_version,
            self.contract_sha256,
        )
        if any(value is not None for value in contract_values):
            if not all(isinstance(value, str) and value for value in contract_values):
                raise ValueError("native JARVIS contract identity must be complete")
            assert self.contract_sha256 is not None
            if not _is_sha256_text(self.contract_sha256):
                raise ValueError("native JARVIS contract SHA-256 is invalid")
        return self


class PersistentUvToolIdentity(BaseModel):
    """Receipt-bound identity of one install-once uv tool environment."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.persistent-uv-tool-identity.v2"] = (
        PERSISTENT_UV_TOOL_IDENTITY_SCHEMA
    )
    manager: Literal["uv"] = "uv"
    uv_executable: str
    uv_version: str
    uv_executable_sha256: str
    tool_directory: str
    tool_bin_directory: str
    environment_prefix: str
    provider_interpreter: str
    provider_interpreter_sha256: str
    tool_executable: str
    tool_executable_resolved: str
    tool_executable_sha256: str
    distribution_console_script_path: str
    distribution_console_script_sha256: str
    uv_receipt_path: str
    uv_receipt_sha256: str
    distribution: str
    distribution_version: str
    distribution_metadata_path: str
    entry_point: str
    source_artifact_path: str
    source_artifact_sha256: str
    record_path: str
    record_sha256: str
    runtime_closure_sha256: str
    runtime_file_count: int = Field(gt=0)
    runtime_bytes: int = Field(gt=0)
    pyvenv_uv_version: str

    @model_validator(mode="after")
    def validate_identity(self) -> PersistentUvToolIdentity:
        """Reject incomplete path, version, and digest identities."""
        paths = (
            self.uv_executable,
            self.tool_directory,
            self.tool_bin_directory,
            self.environment_prefix,
            self.provider_interpreter,
            self.tool_executable,
            self.tool_executable_resolved,
            self.distribution_console_script_path,
            self.uv_receipt_path,
            self.distribution_metadata_path,
            self.source_artifact_path,
            self.record_path,
        )
        if any(not path or path.strip() != path for path in paths):
            raise ValueError("persistent uv tool paths must be non-empty strings")
        digests = (
            self.uv_executable_sha256,
            self.provider_interpreter_sha256,
            self.tool_executable_sha256,
            self.distribution_console_script_sha256,
            self.uv_receipt_sha256,
            self.source_artifact_sha256,
            self.record_sha256,
            self.runtime_closure_sha256,
        )
        if any(not _is_sha256_text(digest) for digest in digests):
            raise ValueError("persistent uv tool SHA-256 identity is invalid")
        if self.pyvenv_uv_version != self.uv_version:
            raise ValueError("persistent uv tool pyvenv marker must match uv")
        return self


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
    native_execution: NativeJarvisExecutionCapability | None = None
    persistent_tool: PersistentUvToolIdentity | None = None


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


def verify_distribution_file_source(
    *,
    direct_url_text: str | None,
    expected_artifact: Path,
) -> Path:
    """Verify that installed distribution metadata names one exact local artifact.

    Package installers preserve the lexical path supplied by the operator in
    ``direct_url.json``. Cluster home directories can be mounted through a
    different canonical path, so provenance identity must compare resolved
    filesystem paths instead of raw file-URL strings.
    """
    try:
        loaded = cast(object, json.loads(direct_url_text or "{}"))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("distribution direct_url.json is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise ConfigurationError("distribution direct_url.json must contain an object")
    payload = cast(dict[str, object], loaded)
    source_url = payload.get("url")
    if not isinstance(source_url, str) or not source_url:
        raise ConfigurationError("distribution direct_url.json does not name a source URL")
    try:
        parsed = urlsplit(source_url)
    except ValueError as exc:
        raise ConfigurationError("distribution source URL is not valid") from exc
    if parsed.scheme.lower() != "file":
        raise ConfigurationError("distribution source URL is not a local file URL")
    if parsed.netloc:
        raise ConfigurationError("distribution source file URL must not contain an authority")
    if parsed.query or parsed.fragment:
        raise ConfigurationError("distribution source file URL must not contain query or fragment")
    source_path_text = unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", source_path_text):
        source_path_text = source_path_text[1:]
    if not source_path_text:
        raise ConfigurationError("distribution source file URL has no path")
    source_artifact = Path(source_path_text)
    if not source_artifact.is_absolute():
        raise ConfigurationError("distribution source file URL path must be absolute")
    try:
        expected = expected_artifact.resolve(strict=True)
        source = source_artifact.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("distribution source artifact cannot be resolved") from exc
    if not expected.is_file() or not source.is_file():
        raise ConfigurationError("distribution source artifact is not a regular file")
    if source != expected:
        raise ConfigurationError("distribution source artifact does not match the verified wheel")
    return source


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


def probe_persistent_uv_tool_identity(
    *,
    uv_executable: str,
    tool_executable: str,
    provider_interpreter: str,
    source_artifact: Path,
    distribution: str,
    distribution_version: str,
    entry_point: str,
) -> PersistentUvToolIdentity:
    """Capture and verify an install-once uv tool environment and wheel closure."""
    uv_path = _required_regular_file(uv_executable, label="uv executable")
    executable_location = _absolute_path(tool_executable, label="tool executable")
    executable_path = _required_regular_file(executable_location, label="tool executable")
    provider_location = _absolute_path(
        provider_interpreter,
        label="tool provider interpreter",
    )
    provider_path = _required_regular_file(
        provider_location,
        label="tool provider interpreter",
    )
    provider_environment_location = _resolved_parent_location(
        provider_location,
        label="tool provider interpreter",
    )
    source_path = _required_regular_file(source_artifact, label="tool source artifact")
    uv_version_output = _bounded_identity_command([str(uv_path), "--version"])
    version_match = re.fullmatch(
        r"uv ([0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*))(?: .*)?",
        uv_version_output,
    )
    if version_match is None:
        raise ConfigurationError("persistent tool uv executable returned no exact version")
    uv_version = version_match.group(1)
    tool_directory = _required_directory_output(
        _bounded_identity_command([str(uv_path), "tool", "dir", "--no-config"]),
        label="uv tool directory",
    )
    tool_bin_directory = _required_directory_output(
        _bounded_identity_command([str(uv_path), "tool", "dir", "--bin", "--no-config"]),
        label="uv tool bin directory",
    )
    if executable_location.parent.resolve() != tool_bin_directory:
        raise ConfigurationError("persistent tool executable is outside uv's tool bin directory")

    probe_source = r"""
import base64
import hashlib
import json
import stat
import sys
from importlib.metadata import distribution
from pathlib import Path

name, expected_entry_point, launcher_value = sys.argv[1:]
launcher = Path(launcher_value).resolve(strict=True)
launcher_identity = launcher.stat()
launcher_identity_key = (
    launcher_identity.st_dev,
    launcher_identity.st_ino,
    launcher_identity.st_mode,
    launcher_identity.st_size,
    launcher_identity.st_mtime_ns,
    launcher_identity.st_ctime_ns,
)
if (
    not stat.S_ISREG(launcher_identity.st_mode)
    or not 1 <= launcher_identity.st_size <= 64 * 1024 * 1024
):
    raise SystemExit("persistent uv tool launcher is not one bounded regular file")
launcher_hash = hashlib.sha256()
with launcher.open("rb") as launcher_stream:
    while launcher_chunk := launcher_stream.read(1024 * 1024):
        launcher_hash.update(launcher_chunk)
launcher_identity_after = launcher.stat()
if (
    launcher_identity_after.st_dev,
    launcher_identity_after.st_ino,
    launcher_identity_after.st_mode,
    launcher_identity_after.st_size,
    launcher_identity_after.st_mtime_ns,
    launcher_identity_after.st_ctime_ns,
) != launcher_identity_key:
    raise SystemExit("persistent uv tool launcher changed while hashing")
installed = distribution(name)
files = installed.files
if not files or len(files) > 100_000:
    raise SystemExit("persistent tool distribution has no bounded RECORD closure")
closure = hashlib.sha256()
runtime_bytes = 0
record_paths = []
console_scripts = []
launcher_digest = launcher_hash.hexdigest()
console_names = {expected_entry_point.casefold(), f"{expected_entry_point}.exe".casefold()}
for item in sorted(files, key=lambda value: str(value)):
    relative = str(item).replace("\\", "/")
    located = Path(installed.locate_file(item)).resolve(strict=True)
    if not located.is_file():
        raise SystemExit("persistent tool RECORD contains a non-file member")
    digest = hashlib.sha256()
    size = 0
    with located.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    runtime_bytes += size
    if runtime_bytes > 4 * 1024 * 1024 * 1024:
        raise SystemExit("persistent tool RECORD closure exceeded its byte limit")
    expected_hash = item.hash
    if expected_hash is not None:
        if expected_hash.mode != "sha256":
            raise SystemExit("persistent tool RECORD uses an unsupported digest")
        encoded = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
        if encoded != expected_hash.value:
            raise SystemExit("persistent tool RECORD member digest mismatch")
    elif not relative.endswith(".dist-info/RECORD"):
        raise SystemExit("persistent tool RECORD member omitted its digest")
    closure.update(relative.encode("utf-8"))
    closure.update(b"\0")
    closure.update(digest.hexdigest().encode("ascii"))
    closure.update(b"\0")
    closure.update(str(size).encode("ascii"))
    closure.update(b"\n")
    if relative.endswith(".dist-info/RECORD"):
        record_paths.append(located)
    if located.name.casefold() in console_names:
        console_scripts.append({"path": str(located), "sha256": digest.hexdigest()})
if len(record_paths) != 1:
    raise SystemExit("persistent tool RECORD ownership is ambiguous")
matching_console_scripts = [
    item for item in console_scripts if item["sha256"] == launcher_digest
]
if len(matching_console_scripts) != 1:
    raise SystemExit("persistent uv tool launcher does not match one RECORD-owned entry point")
direct_url = json.loads(installed.read_text("direct_url.json") or "{}")
entry_points = sorted(
    item.name for item in installed.entry_points if item.group == "console_scripts"
)
metadata_path = Path(installed._path).resolve(strict=True)
print(json.dumps({
    "provider_interpreter": str(Path(sys.executable).absolute()),
    "environment_prefix": str(Path(sys.prefix).resolve(strict=True)),
    "distribution": installed.metadata.get("Name"),
    "distribution_version": installed.version,
    "distribution_metadata_path": str(metadata_path),
    "entry_points": entry_points,
    "direct_url": direct_url,
    "external_launcher_sha256": launcher_digest,
    "distribution_console_script": matching_console_scripts[0],
    "record_path": str(record_paths[0]),
    "record_sha256": hashlib.sha256(record_paths[0].read_bytes()).hexdigest(),
    "runtime_closure_sha256": closure.hexdigest(),
    "runtime_file_count": len(files),
    "runtime_bytes": runtime_bytes,
}, sort_keys=True))
"""
    raw_probe = _bounded_identity_command(
        [
            str(provider_location),
            "-I",
            "-c",
            probe_source,
            distribution,
            entry_point,
            str(executable_location),
        ],
        maximum_bytes=256 * 1024,
        timeout_seconds=60,
    )
    try:
        decoded = json.loads(raw_probe)
    except json.JSONDecodeError as exc:
        raise ConfigurationError("persistent uv tool probe returned invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise ConfigurationError("persistent uv tool probe returned no identity object")
    evidence = cast(dict[str, Any], decoded)
    try:
        observed_provider = _absolute_path(
            str(evidence["provider_interpreter"]),
            label="observed tool provider interpreter",
        )
        observed_provider_target = observed_provider.resolve(strict=True)
        environment_prefix = Path(str(evidence["environment_prefix"])).resolve(strict=True)
        metadata_path = Path(str(evidence["distribution_metadata_path"])).resolve(strict=True)
        record_path = Path(str(evidence["record_path"])).resolve(strict=True)
        raw_console_script = evidence["distribution_console_script"]
        if not isinstance(raw_console_script, dict):
            raise ValueError("distribution console script is not an object")
        console_script = cast(dict[str, object], raw_console_script)
        console_script_path = Path(str(console_script["path"])).resolve(strict=True)
    except (KeyError, OSError) as exc:
        raise ConfigurationError("persistent uv tool probe returned invalid paths") from exc
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("persistent uv tool probe returned invalid paths") from exc
    if (
        _lexical_path_key(observed_provider) != _lexical_path_key(provider_location)
        or observed_provider_target != provider_path
    ):
        raise ConfigurationError("persistent uv tool probe used the wrong interpreter")
    if not _path_within(provider_environment_location, environment_prefix):
        raise ConfigurationError("persistent uv tool provider is outside its environment")
    if not _path_within(environment_prefix, tool_directory):
        raise ConfigurationError("persistent tool environment is outside uv's tool directory")
    if not _path_within(metadata_path, environment_prefix) or not _path_within(
        record_path,
        environment_prefix,
    ):
        raise ConfigurationError("persistent tool metadata is outside its environment")
    if not _path_within(console_script_path, environment_prefix):
        raise ConfigurationError("persistent tool RECORD-owned launcher is outside its environment")
    external_launcher_sha256 = evidence.get("external_launcher_sha256")
    console_script_sha256 = console_script.get("sha256")
    if (
        not isinstance(external_launcher_sha256, str)
        or not isinstance(console_script_sha256, str)
        or external_launcher_sha256 != console_script_sha256
        or sha256_file(executable_location) != external_launcher_sha256
        or sha256_file(console_script_path) != console_script_sha256
    ):
        raise ConfigurationError(
            "persistent uv tool launcher does not match its RECORD-owned entry point"
        )
    direct_url = evidence.get("direct_url")
    try:
        direct_url_text = json.dumps(direct_url, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("persistent tool source metadata is invalid") from exc
    try:
        verify_distribution_file_source(
            direct_url_text=direct_url_text,
            expected_artifact=source_path,
        )
    except ConfigurationError as exc:
        raise ConfigurationError(
            "persistent tool was not installed from the receipt wheel"
        ) from exc
    observed_distribution = str(evidence.get("distribution", "")).lower().replace("_", "-")
    expected_distribution = distribution.lower().replace("_", "-")
    if (
        observed_distribution != expected_distribution
        or evidence.get("distribution_version") != distribution_version
        or not isinstance(evidence.get("entry_points"), list)
        or entry_point not in cast(list[object], evidence["entry_points"])
    ):
        raise ConfigurationError("persistent tool distribution identity did not match")
    pyvenv_uv_version = _pyvenv_uv_marker(environment_prefix)
    if pyvenv_uv_version is None or pyvenv_uv_version != uv_version:
        raise ConfigurationError("persistent tool pyvenv uv marker did not match uv")
    uv_receipt_path, uv_receipt_sha256 = _verify_uv_tool_receipt(
        environment_prefix=environment_prefix,
        executable_location=executable_location,
        source_path=source_path,
        distribution=distribution,
        entry_point=entry_point,
    )
    try:
        return PersistentUvToolIdentity(
            uv_executable=str(uv_path),
            uv_version=uv_version,
            uv_executable_sha256=sha256_file(uv_path),
            tool_directory=str(tool_directory),
            tool_bin_directory=str(tool_bin_directory),
            environment_prefix=str(environment_prefix),
            provider_interpreter=str(provider_location),
            provider_interpreter_sha256=sha256_file(provider_path),
            tool_executable=str(executable_location),
            tool_executable_resolved=str(executable_path),
            tool_executable_sha256=external_launcher_sha256,
            distribution_console_script_path=str(console_script_path),
            distribution_console_script_sha256=console_script_sha256,
            uv_receipt_path=str(uv_receipt_path),
            uv_receipt_sha256=uv_receipt_sha256,
            distribution=distribution,
            distribution_version=distribution_version,
            distribution_metadata_path=str(metadata_path),
            entry_point=entry_point,
            source_artifact_path=str(source_path),
            source_artifact_sha256=sha256_file(source_path),
            record_path=str(record_path),
            record_sha256=str(evidence["record_sha256"]),
            runtime_closure_sha256=str(evidence["runtime_closure_sha256"]),
            runtime_file_count=int(evidence["runtime_file_count"]),
            runtime_bytes=int(evidence["runtime_bytes"]),
            pyvenv_uv_version=pyvenv_uv_version,
        )
    except (KeyError, TypeError, ValueError, ValidationError) as exc:
        raise ConfigurationError("persistent uv tool probe returned invalid identity") from exc


def _bounded_identity_command(
    command: list[str],
    *,
    maximum_bytes: int = 65_536,
    timeout_seconds: int = 30,
) -> str:
    """Run one identity command and return one bounded non-empty line."""
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ConfigurationError(f"persistent tool identity command failed: {exc}") from exc
    encoded = completed.stdout.encode("utf-8")
    if completed.returncode != 0 or not encoded or len(encoded) > maximum_bytes:
        detail = completed.stderr.strip()
        suffix = f": {detail}" if detail else ""
        raise ConfigurationError(f"persistent tool identity command failed{suffix}")
    return completed.stdout.strip()


def _required_regular_file(value: str | Path, *, label: str) -> Path:
    """Resolve one required regular identity file."""
    try:
        path = Path(value).expanduser().resolve(strict=True)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"{label} is unavailable") from exc
    if not path.is_file():
        raise ConfigurationError(f"{label} is not a regular file")
    return path


def _absolute_path(value: str | Path, *, label: str) -> Path:
    """Return one lexical absolute path without resolving its final symlink."""
    try:
        path = Path(value).expanduser()
        absolute = Path(os.path.abspath(path))
    except (OSError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"{label} path is invalid") from exc
    if not absolute.is_absolute():
        raise ConfigurationError(f"{label} path is not absolute")
    return absolute


def _resolved_parent_location(path: Path, *, label: str) -> Path:
    """Resolve a path's parent while preserving its final symlink location."""
    try:
        return path.parent.resolve(strict=True) / path.name
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError(f"{label} parent is unavailable") from exc


def _verify_uv_tool_receipt(
    *,
    environment_prefix: Path,
    executable_location: Path,
    source_path: Path,
    distribution: str,
    entry_point: str,
) -> tuple[Path, str]:
    """Bind uv's external launcher mapping to one exact wheel-backed environment."""
    receipt = environment_prefix / "uv-receipt.toml"
    try:
        details = receipt.lstat()
        if receipt.is_symlink() or not receipt.is_file():
            raise ConfigurationError("persistent uv tool receipt is not a regular file")
        if details.st_size < 1 or details.st_size > MAX_UV_TOOL_RECEIPT_BYTES:
            raise ConfigurationError("persistent uv tool receipt size is invalid")
        payload = receipt.read_bytes()
        if len(payload) != details.st_size or _stat_identity(receipt.lstat()) != _stat_identity(
            details
        ):
            raise ConfigurationError("persistent uv tool receipt changed while reading")
        document = tomllib.loads(payload.decode("utf-8"))
    except ConfigurationError:
        raise
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError("persistent uv tool receipt is invalid") from exc
    tool = document.get("tool")
    if not isinstance(tool, dict):
        raise ConfigurationError("persistent uv tool receipt omitted its tool record")
    tool_record = cast(dict[str, object], tool)
    raw_entrypoints = tool_record.get("entrypoints")
    raw_requirements = tool_record.get("requirements")
    if not isinstance(raw_entrypoints, list) or not isinstance(raw_requirements, list):
        raise ConfigurationError("persistent uv tool receipt omitted its installation mapping")
    entrypoint_matches: list[dict[str, object]] = []
    for raw_entrypoint in cast(list[object], raw_entrypoints):
        if not isinstance(raw_entrypoint, dict):
            raise ConfigurationError("persistent uv tool receipt has an invalid entry point")
        item = cast(dict[str, object], raw_entrypoint)
        if item.get("name") == entry_point:
            entrypoint_matches.append(item)
    if len(entrypoint_matches) != 1:
        raise ConfigurationError("persistent uv tool receipt entry point is ambiguous")
    entrypoint_mapping = entrypoint_matches[0]
    install_path = entrypoint_mapping.get("install-path")
    source_distribution = entrypoint_mapping.get("from")
    if (
        not isinstance(install_path, str)
        or not isinstance(source_distribution, str)
        or _normalized_distribution(source_distribution) != _normalized_distribution(distribution)
        or _lexical_path_key(_absolute_path(install_path, label="uv receipt install path"))
        != _lexical_path_key(executable_location)
    ):
        raise ConfigurationError("persistent uv tool receipt does not own the selected launcher")
    requirement_matches: list[dict[str, object]] = []
    for raw_requirement in cast(list[object], raw_requirements):
        if not isinstance(raw_requirement, dict):
            raise ConfigurationError("persistent uv tool receipt has an invalid requirement")
        item = cast(dict[str, object], raw_requirement)
        name = item.get("name")
        if isinstance(name, str) and _normalized_distribution(name) == _normalized_distribution(
            distribution
        ):
            requirement_matches.append(item)
    if len(requirement_matches) != 1:
        raise ConfigurationError("persistent uv tool receipt source requirement is ambiguous")
    requirement_path = requirement_matches[0].get("path")
    if not isinstance(requirement_path, str):
        raise ConfigurationError("persistent uv tool receipt does not bind the source wheel")
    try:
        requirement_location = Path(requirement_path).expanduser()
        if not requirement_location.is_absolute():
            raise ConfigurationError("persistent uv tool receipt does not bind the source wheel")
        receipt_source_path = _required_regular_file(
            requirement_location,
            label="uv receipt source artifact",
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError(
            "persistent uv tool receipt does not bind the source wheel"
        ) from exc
    if receipt_source_path != source_path:
        raise ConfigurationError("persistent uv tool receipt does not bind the source wheel")
    return receipt.resolve(strict=True), hashlib.sha256(payload).hexdigest()


def _lexical_path_key(path: Path) -> str:
    """Return a platform-normalized key without resolving the final path component."""
    return os.path.normcase(os.path.normpath(str(path)))


def _stat_identity(details: os.stat_result) -> tuple[int, int, int, int, int, int]:
    """Return stable regular-file identity fields while ignoring read access time."""
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def _normalized_distribution(value: str) -> str:
    """Normalize one Python distribution name for identity comparison."""
    return re.sub(r"[-_.]+", "-", value).casefold()


def _required_directory_output(value: str, *, label: str) -> Path:
    """Resolve one absolute directory returned by an identity command."""
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise ConfigurationError(f"{label} output is invalid")
    candidate = Path(value)
    if not candidate.is_absolute():
        raise ConfigurationError(f"{label} output is not absolute")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ConfigurationError(f"{label} is unavailable") from exc
    if not resolved.is_dir():
        raise ConfigurationError(f"{label} is not a directory")
    return resolved


def _pyvenv_uv_marker(prefix: Path) -> str | None:
    """Read uv's exact version marker from one bounded pyvenv.cfg."""
    config = prefix / "pyvenv.cfg"
    try:
        if not config.is_file() or config.stat().st_size > 64 * 1024:
            return None
        text = config.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        normalized = key.strip().casefold()
        if normalized in values:
            return None
        values[normalized] = value.strip()
    return values.get("uv")


def _path_within(path: Path, root: Path) -> bool:
    """Return whether a resolved path is strictly below one resolved root."""
    try:
        return path != root and path.is_relative_to(root)
    except (OSError, ValueError):
        return False


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


def probe_clio_kit_native_execution_contract(
    runtime_command: list[str],
) -> NativeJarvisExecutionCapability:
    """Probe the receipt-bound clio-kit wheel for the exact native JARVIS contract."""
    try:
        cli_index = max(
            index
            for index, argument in enumerate(runtime_command)
            if Path(argument).name.casefold() in {"clio-kit", "clio-kit.exe"}
        )
    except ValueError as exc:
        raise ConfigurationError(
            "clio-kit native execution probe command has no clio-kit launcher"
        ) from exc
    if runtime_command[cli_index + 1 :] != ["mcp-server", "jarvis"]:
        raise ConfigurationError(
            "clio-kit native execution probe requires the receipt-bound JARVIS MCP command"
        )
    probe_command = [
        *runtime_command[: cli_index + 1],
        "mcp-contract",
        CLIO_KIT_JARVIS_CONTRACT_ID,
    ]
    document = _run_json_probe(probe_command, label="clio-kit native execution contract")
    if (
        document.get("schema_version") != CLIO_KIT_MCP_CONTRACT_SCHEMA
        or document.get("contract_id") != CLIO_KIT_JARVIS_CONTRACT_ID
    ):
        raise ConfigurationError("clio-kit native execution contract identity did not match")
    raw_tools = document.get("tools")
    if not isinstance(raw_tools, list):
        raise ConfigurationError("clio-kit native execution contract tools were invalid")
    raw_tool_items = cast(list[object], raw_tools)
    if not all(isinstance(item, dict) for item in raw_tool_items):
        raise ConfigurationError("clio-kit native execution contract tools were invalid")
    tools = [cast(dict[str, object], item) for item in raw_tool_items]
    by_name: dict[str, dict[str, object]] = {}
    for tool in tools:
        name = tool.get("name")
        if not isinstance(name, str) or not name or name in by_name:
            raise ConfigurationError("clio-kit native execution contract tool identity was invalid")
        by_name[name] = tool
    missing_operations = sorted(set(CLIO_KIT_NATIVE_OPERATIONS) - set(by_name))
    if missing_operations:
        raise ConfigurationError(
            f"clio-kit native execution contract omitted operations: {missing_operations}"
        )
    _require_native_output_documents(
        by_name["jarvis_run"],
        {
            "execution_handle": JARVIS_EXECUTION_HANDLE_SCHEMA,
            "execution_record": JARVIS_EXECUTION_RECORD_SCHEMA,
            "progress": JARVIS_EXECUTION_PROGRESS_SCHEMA,
        },
    )
    _require_native_execution_query_contract(by_name["jarvis_get_execution"])
    contract_sha256 = document.get("contract_sha256")
    observed_contract_sha256 = _mcp_contract_digest(tools)
    from clio_relay.jarvis_mcp import CLIO_KIT_JARVIS_USER_CONTRACT_SHA256

    if (
        not isinstance(contract_sha256, str)
        or contract_sha256 != observed_contract_sha256
        or contract_sha256 != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    ):
        raise ConfigurationError("clio-kit native execution contract digest did not match")
    return NativeJarvisExecutionCapability(
        operations=list(CLIO_KIT_NATIVE_OPERATIONS),
        contract_id=CLIO_KIT_JARVIS_CONTRACT_ID,
        contract_schema_version=CLIO_KIT_MCP_CONTRACT_SCHEMA,
        contract_sha256=contract_sha256,
    )


def probe_jarvis_native_execution_capability(
    python: str | None,
) -> NativeJarvisExecutionCapability:
    """Probe one interpreter for JARVIS-CD native execution and query semantics."""
    if python is None:
        raise ConfigurationError("JARVIS native execution interpreter is not configured")
    script = f"""
import json

from jarvis_cd.core.execution import (
    ExecutionHandle,
    HANDLE_SCHEMA,
    PROGRESS_SNAPSHOT_SCHEMA,
    RECORD_SCHEMA,
)
from jarvis_cd.core.pipeline import Pipeline

operations = {{
    "execution_handle.progress": callable(getattr(ExecutionHandle, "progress", None)),
    "pipeline.get_execution": callable(getattr(Pipeline, "get_execution", None)),
    "pipeline.get_execution_progress": callable(
        getattr(Pipeline, "get_execution_progress", None)
    ),
    "pipeline.run": callable(getattr(Pipeline, "run", None)),
}}
if not all(operations.values()):
    raise SystemExit("JARVIS-CD native execution API is incomplete")
print(json.dumps({{
    "schema_version": {NATIVE_JARVIS_CAPABILITY_SCHEMA!r},
    "handle_schema": HANDLE_SCHEMA,
    "record_schema": RECORD_SCHEMA,
    "progress_schema": PROGRESS_SNAPSHOT_SCHEMA,
    "operations": sorted(operations),
    "contract_id": None,
    "contract_schema_version": None,
    "contract_sha256": None,
}}, sort_keys=True))
"""
    document = _run_json_probe(
        [python, "-c", script],
        label="JARVIS-CD native execution capability",
    )
    try:
        capability = NativeJarvisExecutionCapability.model_validate(document)
    except ValidationError as exc:
        raise ConfigurationError(
            f"JARVIS-CD native execution capability was invalid: {exc}"
        ) from exc
    if not _native_capability_matches_component(capability, component_name="jarvis-cd"):
        raise ConfigurationError("JARVIS-CD native execution capability did not match")
    return capability


def _run_json_probe(command: list[str], *, label: str) -> dict[str, object]:
    """Run one bounded component probe and require exactly one JSON object."""
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ConfigurationError(f"{label} failed: {type(exc).__name__}: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic"
        raise ConfigurationError(f"{label} failed: {detail[:2000]}")
    encoded = completed.stdout.encode("utf-8")
    if len(encoded) > 4 * 1024 * 1024:
        raise ConfigurationError(f"{label} exceeded the output limit")
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"{label} returned invalid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ConfigurationError(f"{label} did not return a JSON object")
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def _require_native_query_input(tool: dict[str, object]) -> None:
    """Require a query tool to bind both pipeline and execution identity."""
    input_schema = tool.get("inputSchema")
    if not isinstance(input_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted inputSchema")
    required = cast(dict[str, object], input_schema).get("required")
    if not isinstance(required, list) or set(cast(list[object], required)) != {
        "pipeline_id",
        "execution_id",
    }:
        raise ConfigurationError("clio-kit native JARVIS query identity schema did not match")


def _require_native_execution_query_contract(tool: dict[str, object]) -> None:
    """Require clio-kit's single bounded execution/progress/artifact query."""
    _require_native_query_input(tool)
    input_schema = cast(dict[str, object], tool["inputSchema"])
    if input_schema.get("additionalProperties") is not False:
        raise ConfigurationError("clio-kit native JARVIS query accepted unknown inputs")
    raw_input_properties = input_schema.get("properties")
    if not isinstance(raw_input_properties, dict):
        raise ConfigurationError("clio-kit native JARVIS query properties were incomplete")
    input_properties = cast(dict[str, object], raw_input_properties)
    if set(input_properties) != {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "artifacts",
    }:
        raise ConfigurationError("clio-kit native JARVIS query surface did not match")
    if input_properties.get("include_progress") != {"default": True, "type": "boolean"}:
        raise ConfigurationError("clio-kit native JARVIS progress selector did not match")
    raw_artifacts = input_properties.get("artifacts")
    if (
        not isinstance(raw_artifacts, dict)
        or cast(dict[str, object], raw_artifacts).get("default") is not None
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact selector was incomplete")
    artifact_query = _nullable_schema_option(
        cast(dict[str, object], raw_artifacts),
        expected_type="object",
        label="artifact selector",
    )
    raw_artifact_properties = artifact_query.get("properties")
    if artifact_query.get("additionalProperties") is not False or not isinstance(
        raw_artifact_properties, dict
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact filters were incomplete")
    artifact_properties = cast(dict[str, object], raw_artifact_properties)
    if set(artifact_properties) != {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact filter surface did not match")
    if artifact_properties.get("page_size") != {
        "default": 50,
        "description": "Maximum artifacts to return in this page.",
        "maximum": 100,
        "minimum": 1,
        "type": "integer",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact page bound did not match")
    expected_filter_limits = {
        "package_id": 256,
        "artifact_id": 90,
        "cursor": 1024,
    }
    for field_name, maximum in expected_filter_limits.items():
        raw_filter = artifact_properties.get(field_name)
        if not isinstance(raw_filter, dict):
            raise ConfigurationError(f"clio-kit native JARVIS {field_name} filter was incomplete")
        filter_schema = _nullable_schema_option(
            cast(dict[str, object], raw_filter),
            expected_type="string",
            label=f"{field_name} filter",
        )
        if filter_schema.get("maxLength") != maximum:
            raise ConfigurationError(
                f"clio-kit native JARVIS {field_name} filter bound did not match"
            )

    output_schema = tool.get("outputSchema")
    if not isinstance(output_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted outputSchema")
    typed_output = cast(dict[str, object], output_schema)
    raw_output_properties = typed_output.get("properties")
    required = typed_output.get("required")
    expected_output_fields = {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
    }
    if (
        typed_output.get("additionalProperties") is not False
        or not isinstance(raw_output_properties, dict)
        or not isinstance(required, list)
        or set(cast(list[object], required)) != expected_output_fields
        or set(cast(dict[str, object], raw_output_properties)) != expected_output_fields
    ):
        raise ConfigurationError("clio-kit native JARVIS execution envelope did not match")
    output_properties = cast(dict[str, object], raw_output_properties)
    if output_properties.get("schema_version") != {
        "const": CLIO_KIT_JARVIS_EXECUTION_SCHEMA,
        "type": "string",
    }:
        raise ConfigurationError("clio-kit native JARVIS execution schema did not match")
    _require_native_output_documents(
        tool,
        {
            "execution_handle": JARVIS_EXECUTION_HANDLE_SCHEMA,
            "execution_record": JARVIS_EXECUTION_RECORD_SCHEMA,
        },
    )
    raw_progress = output_properties.get("progress")
    if not isinstance(raw_progress, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted nullable progress")
    progress = _nullable_schema_option(
        cast(dict[str, object], raw_progress),
        expected_type="object",
        label="progress output",
    )
    _require_schema_identity(
        progress,
        field_name="schema_version",
        schema_version=JARVIS_EXECUTION_PROGRESS_SCHEMA,
        label="progress output",
    )
    raw_artifact_page = output_properties.get("artifact_page")
    if not isinstance(raw_artifact_page, dict):
        raise ConfigurationError("clio-kit native JARVIS query omitted nullable artifact page")
    artifact_page = _nullable_schema_option(
        cast(dict[str, object], raw_artifact_page),
        expected_type="object",
        label="artifact page",
    )
    artifact_page_required = artifact_page.get("required")
    artifact_page_properties = artifact_page.get("properties")
    expected_artifact_page_fields = {
        "producer_schema_version",
        "pipeline_id",
        "execution_id",
        "execution_state",
        "terminal",
        "artifacts",
        "matching_artifact_count",
        "returned_artifact_count",
        "next_cursor",
    }
    if (
        artifact_page.get("additionalProperties") is not False
        or not isinstance(artifact_page_required, list)
        or set(cast(list[object], artifact_page_required)) != expected_artifact_page_fields
        or not isinstance(artifact_page_properties, dict)
        or set(cast(dict[str, object], artifact_page_properties)) != expected_artifact_page_fields
    ):
        raise ConfigurationError("clio-kit native JARVIS artifact page schema did not match")
    typed_artifact_page_properties = cast(dict[str, object], artifact_page_properties)
    if typed_artifact_page_properties.get("producer_schema_version") != {
        "const": JARVIS_EXECUTION_ARTIFACTS_SCHEMA,
        "type": "string",
    }:
        raise ConfigurationError("clio-kit native JARVIS artifact page identity did not match")
    artifacts_schema = typed_artifact_page_properties.get("artifacts")
    if not isinstance(artifacts_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS artifact page omitted artifacts")
    artifact_item = cast(dict[str, object], artifacts_schema).get("items")
    if not isinstance(artifact_item, dict):
        raise ConfigurationError("clio-kit native JARVIS artifact item schema was incomplete")
    _require_schema_identity(
        cast(dict[str, object], artifact_item),
        field_name="schema_version",
        schema_version=JARVIS_ARTIFACT_SCHEMA,
        label="artifact item",
    )


def _nullable_schema_option(
    schema: dict[str, object],
    *,
    expected_type: str,
    label: str,
) -> dict[str, object]:
    """Return the non-null branch of an exact two-way nullable schema."""
    raw_options = schema.get("anyOf")
    if not isinstance(raw_options, list):
        raise ConfigurationError(f"clio-kit native JARVIS {label} was not nullable")
    raw_option_items = cast(list[object], raw_options)
    if len(raw_option_items) != 2:
        raise ConfigurationError(f"clio-kit native JARVIS {label} was not nullable")
    options = [cast(dict[str, object], item) for item in raw_option_items if isinstance(item, dict)]
    non_null = [item for item in options if item.get("type") == expected_type]
    nulls = [item for item in options if item == {"type": "null"}]
    if len(options) != 2 or len(non_null) != 1 or len(nulls) != 1:
        raise ConfigurationError(f"clio-kit native JARVIS {label} nullable schema did not match")
    return non_null[0]


def _require_schema_identity(
    schema: dict[str, object],
    *,
    field_name: str,
    schema_version: str,
    label: str,
) -> None:
    """Require one nested object's constant schema-version property."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        raise ConfigurationError(f"clio-kit native JARVIS {label} schema was incomplete")
    identity = cast(dict[str, object], properties).get(field_name)
    if identity != {"const": schema_version, "type": "string"}:
        raise ConfigurationError(f"clio-kit native JARVIS {label} identity did not match")


def _require_native_output_documents(
    tool: dict[str, object],
    expected_documents: dict[str, str],
) -> None:
    """Require exact top-level native document fields and schema constants."""
    output_schema = tool.get("outputSchema")
    if not isinstance(output_schema, dict):
        raise ConfigurationError("clio-kit native JARVIS tool omitted outputSchema")
    typed_output = cast(dict[str, object], output_schema)
    properties = typed_output.get("properties")
    required = typed_output.get("required")
    if not isinstance(properties, dict) or not isinstance(required, list):
        raise ConfigurationError("clio-kit native JARVIS output schema was incomplete")
    typed_properties = cast(dict[str, object], properties)
    required_names = {str(item) for item in cast(list[object], required)}
    for field_name, schema_version in expected_documents.items():
        field_schema = typed_properties.get(field_name)
        if not isinstance(field_schema, dict) or field_name not in required_names:
            raise ConfigurationError(f"clio-kit native JARVIS output omitted required {field_name}")
        schema_properties = cast(dict[str, object], field_schema).get("properties")
        if not isinstance(schema_properties, dict):
            raise ConfigurationError(f"clio-kit native JARVIS {field_name} schema was incomplete")
        schema_field = cast(dict[str, object], schema_properties).get("schema_version")
        if (
            not isinstance(schema_field, dict)
            or cast(dict[str, object], schema_field).get("const") != schema_version
        ):
            raise ConfigurationError(
                f"clio-kit native JARVIS {field_name} schema identity did not match"
            )


def _mcp_contract_digest(tools: list[dict[str, object]]) -> str:
    """Recompute clio-kit's documented agent-facing contract projection."""
    projected = [
        {
            "annotations": tool.get("annotations"),
            "description": tool.get("description"),
            "input_schema": tool.get("inputSchema"),
            "name": tool.get("name"),
            "output_schema": tool.get("outputSchema"),
            "title": tool.get("title"),
        }
        for tool in sorted(tools, key=lambda item: str(item.get("name")))
    ]
    try:
        payload = json.dumps(
            {"tools": projected},
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"clio-kit native execution contract was not JSON: {exc}") from exc
    return hashlib.sha256(payload).hexdigest()


def _native_capability_matches_component(
    capability: NativeJarvisExecutionCapability,
    *,
    component_name: str,
) -> bool:
    """Return whether a native capability is the exact contract for its component."""
    if component_name == "clio-kit":
        return (
            capability.operations == list(CLIO_KIT_NATIVE_OPERATIONS)
            and capability.contract_id == CLIO_KIT_JARVIS_CONTRACT_ID
            and capability.contract_schema_version == CLIO_KIT_MCP_CONTRACT_SCHEMA
            and _is_sha256_text(capability.contract_sha256)
        )
    if component_name == "jarvis-cd":
        return (
            capability.operations == list(JARVIS_CD_NATIVE_OPERATIONS)
            and capability.contract_id is None
            and capability.contract_schema_version is None
            and capability.contract_sha256 is None
        )
    return False


def _is_sha256_text(value: object) -> bool:
    """Return whether a value is one canonical hexadecimal SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _component_runtime_identity(receipt: InstallReceipt) -> dict[str, object]:
    """Return current process evidence for receipt-bound component launchers."""
    identities: dict[str, object] = {}
    if "clio-kit" in receipt.component_artifacts:
        from clio_relay.jarvis_mcp import jarvis_mcp_runtime_identity

        component = receipt.component_artifacts["clio-kit"]
        runtime_identity = jarvis_mcp_runtime_identity(receipt)
        expected_capability = component.native_execution
        try:
            observed_capability = probe_clio_kit_native_execution_contract(
                component.runtime_command
            )
        except ConfigurationError as exc:
            observed_capability = None
            runtime_identity["native_execution_error"] = str(exc)
        runtime_identity.update(
            {
                "native_execution_capability": (
                    observed_capability.model_dump(mode="json")
                    if observed_capability is not None
                    else None
                ),
                "native_execution_capability_verified": (
                    expected_capability is not None
                    and observed_capability == expected_capability
                    and _native_capability_matches_component(
                        expected_capability,
                        component_name="clio-kit",
                    )
                ),
            }
        )
        identities["clio-kit"] = runtime_identity
    for component_name, component in receipt.component_artifacts.items():
        if component.native_execution is not None and component_name != "clio-kit":
            identities[component_name] = _native_jarvis_component_runtime_identity(component)
    return identities


def _native_jarvis_component_runtime_identity(
    component: ComponentArtifactIdentity,
) -> dict[str, object]:
    """Verify native JARVIS API visibility in its execution interpreter."""
    try:
        provider_distribution = metadata.distribution(component.distribution)
    except metadata.PackageNotFoundError:
        provider_distribution = None
    installed_entry_points = (
        _distribution_progress_entry_points(provider_distribution)
        if provider_distribution is not None
        else []
    )
    expected_entry_points = sorted(component.entry_points)
    provider_direct_url = (
        _distribution_direct_url(provider_distribution) if provider_distribution is not None else {}
    )
    runtime_path = (
        Path(component.runtime_artifact_path).expanduser().resolve()
        if component.runtime_artifact_path is not None
        else None
    )
    artifact_sha256_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_sha256 is not None
        and sha256_file(runtime_path) == component.artifact_sha256
    )
    provider_distribution_identity_verified = (
        provider_distribution is not None
        and _normalized_distribution_name(provider_distribution.name)
        == _normalized_distribution_name(component.distribution)
        and provider_distribution.version == component.distribution_version
    )
    entry_points_visible = set(expected_entry_points).issubset(installed_entry_points)
    provider_python = component.runtime_interpreters.get("provider")
    provider_source_verified = runtime_path is not None and _direct_url_source_matches(
        provider_direct_url,
        expected_artifact=runtime_path,
    )
    provider_interpreter_verified = (
        provider_python is not None
        and Path(provider_python).expanduser().resolve() == Path(sys.executable).resolve()
        and provider_distribution_identity_verified
        and provider_source_verified
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
    execution_entry_points_visible = isinstance(execution_entry_points, list) and set(
        expected_entry_points
    ).issubset({str(value) for value in cast(list[object], execution_entry_points)})
    execution_source_verified = runtime_path is not None and _direct_url_source_matches(
        execution.get("direct_url"),
        expected_artifact=runtime_path,
    )
    runtime_artifact_path_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_filename == runtime_path.name
        and execution_source_verified
    )
    distribution_identity_verified = execution_distribution_identity_verified
    execution_interpreter_verified = (
        execution_python is not None
        and execution.get("executable") is not None
        and Path(str(execution["executable"])).resolve()
        == Path(execution_python).expanduser().resolve()
        and execution_distribution_identity_verified
        and execution_source_verified
    )
    expected_native_capability = component.native_execution
    execution_native_capability: NativeJarvisExecutionCapability | None = None
    execution_native_error: str | None = None
    try:
        execution_native_capability = probe_jarvis_native_execution_capability(execution_python)
    except ConfigurationError as exc:
        execution_native_error = str(exc)
    execution_native_execution_capability_verified = (
        expected_native_capability is not None
        and execution_native_capability == expected_native_capability
    )
    native_execution_capability_verified = (
        expected_native_capability is not None
        and _native_capability_matches_component(
            expected_native_capability,
            component_name="jarvis-cd",
        )
        and execution_native_execution_capability_verified
    )
    jarvis_executable = component.runtime_executables.get("jarvis")
    jarvis_executable_verified = _jarvis_executable_matches_interpreter(
        jarvis_executable,
        execution_python,
        runtime_command=component.runtime_command,
    )
    verified = (
        distribution_identity_verified
        and runtime_artifact_path_verified
        and artifact_sha256_verified
        and execution_interpreter_verified
        and native_execution_capability_verified
        and jarvis_executable_verified
    )
    return {
        "verified": verified,
        "distribution": execution.get("distribution"),
        "distribution_version": execution.get("distribution_version"),
        "distribution_identity_verified": distribution_identity_verified,
        "entry_points": installed_entry_points,
        "entry_points_visible": entry_points_visible,
        "compatibility_entry_points_declared": bool(expected_entry_points),
        "compatibility_entry_points_visible": entry_points_visible,
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
        "provider_distribution_identity_verified": provider_distribution_identity_verified,
        "execution_interpreter": execution_python,
        "execution_interpreter_verified": execution_interpreter_verified,
        "execution_distribution_identity_verified": execution_distribution_identity_verified,
        "execution_entry_points_visible": execution_entry_points_visible,
        "execution_source_verified": execution_source_verified,
        "execution": execution,
        "native_execution_capability": (
            expected_native_capability.model_dump(mode="json")
            if expected_native_capability is not None
            else None
        ),
        "provider_native_execution_capability": None,
        "provider_native_execution_capability_verified": None,
        "provider_native_execution_error": "not required by the native execution boundary",
        "execution_native_execution_capability": (
            execution_native_capability.model_dump(mode="json")
            if execution_native_capability is not None
            else None
        ),
        "execution_native_execution_capability_verified": (
            execution_native_execution_capability_verified
        ),
        "execution_native_execution_error": execution_native_error,
        "native_execution_capability_verified": native_execution_capability_verified,
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


def _direct_url_source_matches(value: object, *, expected_artifact: Path) -> bool:
    """Return whether direct-url evidence resolves to one exact local artifact."""
    document: object = {"url": value} if isinstance(value, str) else value
    try:
        direct_url_text = json.dumps(document, sort_keys=True)
        verify_distribution_file_source(
            direct_url_text=direct_url_text,
            expected_artifact=expected_artifact,
        )
    except (ConfigurationError, TypeError, ValueError):
        return False
    return True


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
        execution_bin_directory = python_path.parent.resolve(strict=True)
        return (
            executable_path.is_file()
            and os.access(executable_path, os.X_OK)
            and executable_path.resolve(strict=True).parent == execution_bin_directory
            and Path(runtime_command[0]).expanduser().resolve(strict=True)
            == executable_path.resolve(strict=True)
        )
    except (OSError, RuntimeError, ValueError):
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
    """Verify a legacy package-progress plugin for compatibility diagnostics only.

    This compatibility proof is intentionally not used by the 1.0 release gate.
    """
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


def verify_remote_clio_kit_native_execution_component(
    info: dict[str, object],
    receipt: InstallReceipt,
) -> dict[str, object]:
    """Require the exact receipt-bound clio-kit native JARVIS MCP contract."""
    component = receipt.component_artifacts.get("clio-kit")
    if component is None or component.native_execution is None:
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
    version = component.distribution_version
    digest = component.artifact_sha256
    if (
        component.requested_source != "github_release"
        or version is None
        or not component.install_spec.startswith("https://github.com/")
        or "/releases/download/" not in component.install_spec
        or not _is_sha256_text(digest)
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
