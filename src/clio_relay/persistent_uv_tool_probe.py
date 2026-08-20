"""Verify a receipt-bound install-once uv tool environment.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns the one
large probe -- ``probe_persistent_uv_tool_identity`` -- that reads uv's own
`tool dir`/`tool dir --bin` output, launches a bounded identity subprocess to
walk the installed distribution's RECORD closure byte-for-byte, and binds the
result to uv's own ``uv-receipt.toml``. Its dozen private helpers
(bounded-command execution, path/digest primitives, the receipt-toml reader)
exist ONLY to serve this one probe -- no other installation concern calls
them -- so they stay in this module rather than splitting further; the
module's size is dominated by the probe's own embedded verification script,
not by unrelated concerns sharing a file.

This module is intentionally over the usual 150-500-line sweet spot (see
scripts/check_file_size.py's DEFAULT_MAX_LINES=800): the probe itself
performs one continuous chain of identity checks against a single wheel
closure and does not have a clean internal seam without duplicating verified
state across a split.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import sys
import tomllib
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.distribution_source_identity import verify_distribution_file_source
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import PersistentUvToolIdentity
from clio_relay.validation_report import sha256_file

MAX_UV_TOOL_RECEIPT_BYTES = 256 * 1024
_UV_FD_EXEC_SOURCE = r"""import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path


def identity(details):
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def cross_identity(details):
    if os.name == "nt":
        return (
            details.st_dev,
            details.st_ino,
            stat.S_IFMT(details.st_mode),
            details.st_size,
        )
    return identity(details)


path = Path(sys.argv[1])
expected_sha256 = sys.argv[2]
arguments = sys.argv[3:]
before = path.lstat()
if (
    not stat.S_ISREG(before.st_mode)
    or not 1 <= before.st_size <= 256 * 1024 * 1024
    or len(expected_sha256) != 64
    or any(character not in "0123456789abcdef" for character in expected_sha256)
):
    raise SystemExit("uv executable identity is invalid")
flags = (
    os.O_RDONLY
    | getattr(os, "O_BINARY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
descriptor = os.open(path, flags)
try:
    opened = os.fstat(descriptor)
    if cross_identity(opened) != cross_identity(before):
        raise SystemExit("uv executable changed before its pinned read")
    digest = hashlib.sha256()
    size = 0
    while chunk := os.read(descriptor, 1024 * 1024):
        size += len(chunk)
        if size > 256 * 1024 * 1024:
            raise SystemExit("uv executable exceeded its byte bound")
        digest.update(chunk)
    after = os.fstat(descriptor)
    linked_after = path.lstat()
    if (
        size != opened.st_size
        or digest.hexdigest() != expected_sha256
        or cross_identity(after) != cross_identity(opened)
        or cross_identity(linked_after) != cross_identity(opened)
    ):
        raise SystemExit("uv executable changed or did not match its release pin")
    os.lseek(descriptor, 0, os.SEEK_SET)
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("LD_", "PYTHON")) and name not in {"BASH_ENV", "ENV"}
    }
    if os.execve in os.supports_fd:
        os.execve(descriptor, [str(path), *arguments], environment)
    if os.name == "nt":
        raise SystemExit(subprocess.run([str(path), *arguments], env=environment).returncode)
    os.execve(str(path), [str(path), *arguments], environment)
finally:
    os.close(descriptor)
"""


def probe_persistent_uv_tool_identity(
    *,
    uv_executable: str,
    tool_executable: str,
    provider_interpreter: str,
    source_artifact: Path,
    distribution: str,
    distribution_version: str,
    entry_point: str,
    tool_directory: str | None = None,
    tool_bin_directory: str | None = None,
    expected_uv_executable_sha256: str | None = None,
    expected_provider_interpreter_sha256: str | None = None,
) -> PersistentUvToolIdentity:
    """Capture and verify an install-once uv tool environment and wheel closure."""
    if (tool_directory is None) is not (tool_bin_directory is None):
        raise ConfigurationError(
            "persistent tool directory and bin directory must be specified together"
        )
    uv_environment = (
        {
            "UV_TOOL_DIR": str(_absolute_path(tool_directory, label="uv tool directory")),
            "UV_TOOL_BIN_DIR": str(
                _absolute_path(tool_bin_directory, label="uv tool bin directory")
            ),
        }
        if tool_directory is not None and tool_bin_directory is not None
        else None
    )
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
    provider_interpreter_sha256 = sha256_file(provider_path)
    if (
        expected_provider_interpreter_sha256 is not None
        and provider_interpreter_sha256 != expected_provider_interpreter_sha256
    ):
        raise ConfigurationError(
            "persistent tool provider interpreter did not match its coordinator pin"
        )
    provider_environment_location = _resolved_parent_location(
        provider_location,
        label="tool provider interpreter",
    )
    source_path = _required_regular_file(source_artifact, label="tool source artifact")
    uv_executable_sha256 = sha256_file(uv_path)
    if (
        expected_uv_executable_sha256 is not None
        and uv_executable_sha256 != expected_uv_executable_sha256
    ):
        raise ConfigurationError("persistent tool uv executable did not match its release pin")

    def uv_command(
        *arguments: str,
        environment: dict[str, str] | None = None,
    ) -> str:
        if expected_uv_executable_sha256 is not None:
            return _bounded_uv_identity_command(
                uv_path,
                uv_executable_sha256,
                list(arguments),
                environment=environment,
            )
        return _bounded_identity_command(
            [str(uv_path), *arguments],
            environment=environment,
        )

    uv_version_output = uv_command("--version")
    version_match = re.fullmatch(
        r"uv ([0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*))(?: .*)?",
        uv_version_output,
    )
    if version_match is None:
        raise ConfigurationError("persistent tool uv executable returned no exact version")
    uv_version = version_match.group(1)
    observed_tool_directory = _required_directory_output(
        uv_command("tool", "dir", "--no-config", environment=uv_environment),
        label="uv tool directory",
    )
    observed_tool_bin_directory = _required_directory_output(
        uv_command("tool", "dir", "--bin", "--no-config", environment=uv_environment),
        label="uv tool bin directory",
    )
    if executable_location.parent.resolve() != observed_tool_bin_directory:
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
    probe_arguments = [distribution, entry_point, str(executable_location)]
    if expected_provider_interpreter_sha256 is not None and os.name == "posix":
        raw_probe = _in_process_candidate_provider_probe(
            source=probe_source,
            arguments=probe_arguments,
            provider_location=provider_location,
            expected_provider_sha256=expected_provider_interpreter_sha256,
            maximum_bytes=256 * 1024,
        )
    else:
        raw_probe = _bounded_identity_command(
            [
                str(provider_location),
                "-I",
                "-c",
                probe_source,
                *probe_arguments,
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
    if not _path_within(environment_prefix, observed_tool_directory):
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
        if not dev_mode_enabled():
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
            uv_executable_sha256=uv_executable_sha256,
            tool_directory=str(observed_tool_directory),
            tool_bin_directory=str(observed_tool_bin_directory),
            environment_prefix=str(environment_prefix),
            provider_interpreter=str(provider_location),
            provider_interpreter_sha256=(
                expected_provider_interpreter_sha256 or provider_interpreter_sha256
            ),
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


def _in_process_candidate_provider_probe(
    *,
    source: str,
    arguments: list[str],
    provider_location: Path,
    expected_provider_sha256: str,
    maximum_bytes: int,
) -> str:
    """Run a fixed identity probe inside the coordinator-bound provider process."""
    if sys.flags.isolated != 1:
        raise ConfigurationError("candidate provider probe is not isolated")
    if any(
        name.startswith(("LD_", "PYTHON")) or name in {"BASH_ENV", "ENV"} for name in os.environ
    ):
        raise ConfigurationError("candidate provider probe environment was not sanitized")
    if os.environ.get("BOOTSTRAP_PLAN_PROVIDER") != str(provider_location):
        raise ConfigurationError("candidate provider lexical path was not coordinator-bound")
    if os.environ.get("BOOTSTRAP_PLAN_PROVIDER_SHA256") != expected_provider_sha256:
        raise ConfigurationError("candidate provider digest was not coordinator-bound")
    execution_sha256 = os.environ.get("BOOTSTRAP_PLAN_PROVIDER_EXEC_SHA256", "")
    if len(execution_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in execution_sha256
    ):
        raise ConfigurationError("candidate provider execution digest is invalid")
    try:
        executable_link = os.readlink("/proc/self/exe")
        executable_sha256 = sha256_file(Path("/proc/self/exe"))
    except OSError as exc:
        raise ConfigurationError("candidate provider execution image is unavailable") from exc
    if (
        not executable_link.startswith("/memfd:clio-relay-candidate-provider")
        or not executable_link.endswith(" (deleted)")
        or executable_sha256 != execution_sha256
    ):
        raise ConfigurationError("candidate provider process is not the sealed execution image")

    output = io.StringIO()
    previous_argv = sys.argv
    try:
        sys.argv = [str(provider_location), *arguments]
        with redirect_stdout(output):
            exec(compile(source, "<clio-relay-candidate-provider-probe>", "exec"), {})
    finally:
        sys.argv = previous_argv
    value = output.getvalue()
    if not value or len(value.encode("utf-8")) > maximum_bytes:
        raise ConfigurationError("candidate provider probe returned invalid bounded output")
    return value.strip()


def _bounded_uv_identity_command(
    uv_executable: Path,
    expected_sha256: str,
    arguments: list[str],
    *,
    environment: dict[str, str] | None = None,
) -> str:
    """Run one uv command from its verified open descriptor on POSIX."""
    sanitized = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("LD_", "PYTHON")) and name not in {"BASH_ENV", "ENV"}
    }
    if environment is not None:
        sanitized.update(environment)
    wrapper_provider = (
        "/proc/self/exe"
        if os.name == "posix" and "BOOTSTRAP_PLAN_PROVIDER_EXEC_SHA256" in os.environ
        else sys.executable
    )
    return _bounded_identity_command(
        [
            wrapper_provider,
            "-I",
            "-c",
            _UV_FD_EXEC_SOURCE,
            str(uv_executable),
            expected_sha256,
            *arguments,
        ],
        environment=sanitized,
        replace_environment=True,
    )


def _bounded_identity_command(
    command: list[str],
    *,
    maximum_bytes: int = 65_536,
    timeout_seconds: int = 30,
    environment: dict[str, str] | None = None,
    replace_environment: bool = False,
) -> str:
    """Run one identity command and return one bounded non-empty line."""
    try:
        completed = run_bounded_process(
            command,
            timeout_seconds=timeout_seconds,
            stdout_maximum_bytes=maximum_bytes,
            stderr_maximum_bytes=4096,
            environment=(
                environment
                if replace_environment
                else ({**os.environ, **environment} if environment is not None else None)
            ),
        )
    except (OSError, BoundedProcessError) as exc:
        raise ConfigurationError(f"persistent tool identity command failed: {exc}") from exc
    encoded = completed.stdout.encode("utf-8")
    if completed.returncode != 0 or not encoded:
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
