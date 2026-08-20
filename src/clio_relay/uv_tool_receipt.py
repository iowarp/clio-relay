"""Bind an install-once ``uv tool`` invocation to its receipt and RECORD (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). ``launcher="uv-tool"``
is the strongest install-source claim a validation report can make -- it
means the running process is provably the exact console-script entry point
``uv tool install`` wrote, inside the exact isolated environment ``uv``
manages, running the exact package files ``pip``'s installed-``RECORD``
closure names. :func:`detect_persistent_uv_tool_receipt` is the entry point
gate evaluation and install-source detection call; it cross-binds five
independent pieces of structural evidence (the environment's relationship to
``uv tool dir``, the process prefix/executable location, ``pyvenv.cfg``'s uv
version marker, the entry-point/requirement rows in ``uv-receipt.toml`` via
:func:`persistent_uv_tool_receipt_identity`, and the installed-file closure
via :func:`installed_record_identity`) and only reports ``verified=True``
when every one of them agrees.
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.parse
from importlib import metadata
from pathlib import Path
from typing import Any, cast

from clio_relay.artifact_identity_verification import (
    _local_wheel_archive_path,
    distribution_direct_url,
)
from clio_relay.process_ancestry import strictly_contains, uv_executable_identity, within_or_equal
from clio_relay.redaction import redact_url
from clio_relay.regular_file_identity import (
    hash_open_regular_file,
    read_open_regular_file,
    regular_file_identity,
)
from clio_relay.validation_limits import MAX_PYVENV_CONFIG_BYTES, MAX_UV_TOOL_RECEIPT_BYTES
from clio_relay.validation_schema import InstallSourceKind


def detect_persistent_uv_tool_receipt(
    *,
    detected_kind: InstallSourceKind,
    package_path: str,
    distribution: metadata.Distribution,
) -> tuple[bool, dict[str, Any]]:
    """Capture structural evidence for an install-once uv tool invocation."""
    uv_executable = os.environ.get("UV") or shutil.which("uv")
    uv_path = Path(uv_executable) if uv_executable is not None else None
    uv_identity_before = regular_file_identity(uv_path) if uv_path is not None else None
    uv_path_verified, uv_version, uv_executable_sha256 = uv_executable_identity(uv_executable)
    uv_identity_after = regular_file_identity(uv_path) if uv_path is not None else None
    uv_stable = uv_identity_before is not None and uv_identity_after == uv_identity_before
    tool_directory = uv_tool_dir(uv_path, bin_directory=False) if uv_path_verified else None
    tool_bin_directory = uv_tool_dir(uv_path, bin_directory=True) if uv_path_verified else None
    prefix = Path(sys.prefix).resolve()
    base_prefix = Path(sys.base_prefix).resolve()
    process_executable = Path(os.path.abspath(sys.executable))
    process_executable_resolved = process_executable.resolve()
    package = Path(package_path).resolve()
    package_in_environment = within_or_equal(package, prefix)
    # POSIX virtual environments normally expose ``bin/python`` as a symlink to
    # the base interpreter. Keep the launcher location and its resolved target
    # as separate trust claims: the lexical executable must belong to the uv
    # tool environment, while the target may belong to that environment or to
    # the interpreter's exact base prefix.
    executable_in_environment = within_or_equal(process_executable, prefix)
    executable_target_bound = within_or_equal(process_executable_resolved, prefix) or (
        within_or_equal(process_executable_resolved, base_prefix)
    )
    environment_in_tool_directory = tool_directory is not None and strictly_contains(
        tool_directory, prefix
    )
    pyvenv_uv_version = pyvenv_uv_version_marker(prefix)
    pyvenv_matches_uv = uv_version is not None and pyvenv_uv_version == uv_version
    configured_tool = os.environ.get("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE")
    tool_name = "clio-relay.exe" if os.name == "nt" else "clio-relay"
    # Ambient PATH and the Windows current directory can name a different tool environment.
    selected_tool = configured_tool or (
        shutil.which(str(tool_bin_directory / tool_name))
        if tool_bin_directory is not None
        else None
    )
    tool_path = Path(selected_tool).expanduser() if selected_tool is not None else None
    try:
        tool_path_absolute = tool_path.absolute() if tool_path is not None else None
        tool_target = tool_path.resolve(strict=True) if tool_path is not None else None
    except OSError:
        tool_path_absolute = None
        tool_target = None
    tool_bin_bound = (
        tool_path_absolute is not None
        and tool_bin_directory is not None
        and tool_path_absolute.parent.resolve() == tool_bin_directory
    )
    tool_target_identity = regular_file_identity(tool_target) if tool_target is not None else None
    tool_executable_sha256 = (
        hash_open_regular_file(tool_target, tool_target_identity)
        if tool_target is not None
        else None
    )
    record_identity = installed_record_identity(distribution)
    owned_console_digests = record_identity.pop("console_script_sha256", [])
    tool_target_bound = tool_target is not None and (
        within_or_equal(tool_target, prefix)
        or (
            isinstance(tool_executable_sha256, str)
            and tool_executable_sha256 in owned_console_digests
        )
    )
    project_environment = (Path.cwd() / ".venv").resolve()
    isolated_environment = prefix != base_prefix and prefix != project_environment
    uv_receipt_identity = persistent_uv_tool_receipt_identity(
        environment_prefix=prefix,
        tool_executable=tool_path_absolute,
        distribution=distribution,
    )
    verified = (
        detected_kind in {InstallSourceKind.WHEEL, InstallSourceKind.PYPI, InstallSourceKind.VCS}
        and uv_path_verified
        and uv_stable
        and tool_directory is not None
        and tool_bin_directory is not None
        and environment_in_tool_directory
        and pyvenv_matches_uv
        and package_in_environment
        and executable_in_environment
        and executable_target_bound
        and tool_bin_bound
        and tool_target_bound
        and record_identity.get("verified") is True
        and uv_receipt_identity.get("verified") is True
        and isolated_environment
    )
    return verified, {
        "schema_version": "clio-relay.launcher-receipt.v3",
        "claimed_launcher": "uv-tool",
        "uv_executable": uv_executable,
        "uv_executable_verified": uv_path_verified,
        "uv_executable_stable": uv_stable,
        "uv_version": uv_version,
        "uv_executable_sha256": uv_executable_sha256,
        "uv_tool_directory": str(tool_directory) if tool_directory is not None else None,
        "uv_tool_bin_directory": (
            str(tool_bin_directory) if tool_bin_directory is not None else None
        ),
        "tool_environment_verified": environment_in_tool_directory,
        "tool_executable": str(tool_path_absolute) if tool_path_absolute is not None else None,
        "tool_executable_resolved": str(tool_target) if tool_target is not None else None,
        "tool_executable_sha256": tool_executable_sha256,
        "tool_bin_bound": tool_bin_bound,
        "tool_target_bound": tool_target_bound,
        "invocation_id": os.environ.get("CLIO_RELAY_VALIDATION_INVOCATION_ID"),
        "process_prefix": str(prefix),
        "base_prefix": str(base_prefix),
        "process_executable": str(process_executable),
        "process_executable_resolved": str(process_executable_resolved),
        "package_in_process_environment": package_in_environment,
        "executable_in_process_environment": executable_in_environment,
        "executable_target_bound": executable_target_bound,
        "pyvenv_uv_version": pyvenv_uv_version,
        "pyvenv_matches_uv": pyvenv_matches_uv,
        "isolated_environment": isolated_environment,
        "distribution_record": record_identity,
        "uv_tool_receipt": uv_receipt_identity,
        "detected_install_source": detected_kind.value,
        "verified": verified,
    }


def persistent_uv_tool_receipt_identity(
    *,
    environment_prefix: Path,
    tool_executable: Path | None,
    distribution: metadata.Distribution,
) -> dict[str, Any]:
    """Bind uv's launcher and requirement records to the running distribution."""
    receipt_path = environment_prefix / "uv-receipt.toml"
    identity = regular_file_identity(receipt_path)
    if identity is None or not 1 <= identity[2] <= MAX_UV_TOOL_RECEIPT_BYTES:
        return {"verified": False, "error": "uv tool receipt is missing or invalid"}
    payload = read_open_regular_file(
        receipt_path,
        identity,
        maximum_bytes=MAX_UV_TOOL_RECEIPT_BYTES,
    )
    if payload is None:
        return {"verified": False, "error": "uv tool receipt changed while reading"}
    try:
        document = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError):
        return {"verified": False, "error": "uv tool receipt is not valid TOML"}
    tool = document.get("tool")
    if not isinstance(tool, dict):
        return {"verified": False, "error": "uv tool receipt omitted its tool record"}
    tool_record = cast(dict[str, object], tool)
    entrypoints = tool_record.get("entrypoints")
    requirements = tool_record.get("requirements")
    if not isinstance(entrypoints, list) or not isinstance(requirements, list):
        return {"verified": False, "error": "uv tool receipt omitted its mappings"}

    launcher_matches: list[dict[str, object]] = []
    for raw_entrypoint in cast(list[object], entrypoints):
        if not isinstance(raw_entrypoint, dict):
            return {"verified": False, "error": "uv tool receipt entry point is invalid"}
        entrypoint = cast(dict[str, object], raw_entrypoint)
        source = entrypoint.get("from")
        if isinstance(source, str) and normalized_distribution_name(source) == "clio-relay":
            launcher_matches.append(entrypoint)
    launcher_bound = False
    if len(launcher_matches) == 1 and tool_executable is not None:
        install_path = launcher_matches[0].get("install-path")
        install_location = (
            Path(install_path).expanduser() if isinstance(install_path, str) else None
        )
        launcher_bound = (
            install_location is not None
            and install_location.is_absolute()
            and lexical_path_key(install_location) == lexical_path_key(tool_executable)
        )

    requirement_matches: list[dict[str, object]] = []
    for raw_requirement in cast(list[object], requirements):
        if not isinstance(raw_requirement, dict):
            return {"verified": False, "error": "uv tool receipt requirement is invalid"}
        requirement = cast(dict[str, object], raw_requirement)
        name = requirement.get("name")
        if isinstance(name, str) and normalized_distribution_name(name) == "clio-relay":
            requirement_matches.append(requirement)
    direct_url = distribution_direct_url(distribution)
    source_bound = len(requirement_matches) == 1 and uv_requirement_matches_distribution_source(
        requirement_matches[0] if requirement_matches else {},
        direct_url=direct_url,
        distribution_version=distribution.version,
    )
    requirement = requirement_matches[0] if len(requirement_matches) == 1 else {}
    source_url = requirement.get("url")
    source_path = requirement.get("path")
    source_specifier = requirement.get("specifier")
    verified = launcher_bound and source_bound
    return {
        "schema_version": "clio-relay.uv-tool-receipt.v1",
        "path": str(receipt_path.resolve()),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "launcher_bound": launcher_bound,
        "requirement_name": requirement.get("name"),
        "requirement_url": redact_url(source_url) if isinstance(source_url, str) else None,
        "requirement_path": source_path if isinstance(source_path, str) else None,
        "requirement_specifier": source_specifier if isinstance(source_specifier, str) else None,
        "distribution_url": direct_url.get("url") if direct_url is not None else None,
        "source_bound": source_bound,
        "verified": verified,
    }


def uv_requirement_matches_distribution_source(
    requirement: dict[str, object],
    *,
    direct_url: dict[str, Any] | None,
    distribution_version: str,
) -> bool:
    """Match one uv requirement to the exact PEP 610 installation source."""
    source_url = requirement.get("url")
    source_path = requirement.get("path")
    source_specifier = requirement.get("specifier")
    if direct_url is None:
        return (
            source_url is None
            and source_path is None
            and source_specifier in {None, f"=={distribution_version}"}
        )
    distribution_url = direct_url.get("url")
    if not isinstance(distribution_url, str):
        return False
    parsed = urllib.parse.urlsplit(distribution_url)
    if parsed.scheme.casefold() == "file":
        if not isinstance(source_path, str):
            return False
        direct_path = _local_wheel_archive_path(direct_url)
        if direct_path is None:
            return False
        try:
            return Path(source_path).expanduser().resolve(strict=True) == direct_path.resolve(
                strict=True
            )
        except (OSError, RuntimeError, ValueError):
            return False
    return (
        parsed.scheme.casefold() == "https"
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
        and isinstance(source_url, str)
        and source_url == distribution_url
        and redact_url(source_url) == source_url
    )


def normalized_distribution_name(value: str) -> str:
    """Return the canonical comparison key for one Python distribution name."""
    return re.sub(r"[-_.]+", "-", value).casefold()


def lexical_path_key(path: Path) -> str:
    """Return a platform-normalized lexical path key."""
    return os.path.normcase(os.path.normpath(str(path)))


def uv_tool_dir(executable: Path | None, *, bin_directory: bool) -> Path | None:
    """Return one directory reported by the exact stable uv executable."""
    identity = regular_file_identity(executable) if executable is not None else None
    if executable is None or identity is None:
        return None
    command = [str(executable), "tool", "dir"]
    if bin_directory:
        command.append("--bin")
    command.append("--no-config")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = completed.stdout.strip()
    if (
        completed.returncode != 0
        or regular_file_identity(executable) != identity
        or not output
        or "\x00" in output
        or "\n" in output
        or "\r" in output
    ):
        return None
    candidate = Path(output)
    if not candidate.is_absolute():
        return None
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        return None
    return resolved if resolved.is_dir() else None


def installed_record_identity(distribution: metadata.Distribution) -> dict[str, Any]:
    """Verify and summarize the complete installed distribution RECORD closure."""
    files = distribution.files
    if files is None or not files or len(files) > 100_000:
        return {"verified": False, "console_script_sha256": []}
    closure = hashlib.sha256()
    runtime_bytes = 0
    record_paths: list[Path] = []
    console_digests: list[str] = []
    try:
        for item in sorted(files, key=lambda value: str(value)):
            relative = str(item).replace("\\", "/")
            located = Path(str(distribution.locate_file(item))).resolve(strict=True)
            identity = regular_file_identity(located)
            if identity is None:
                return {"verified": False, "console_script_sha256": []}
            digest = hash_open_regular_file(located, identity)
            if digest is None:
                return {"verified": False, "console_script_sha256": []}
            size = identity[2]
            runtime_bytes += size
            if runtime_bytes > 4 * 1024 * 1024 * 1024:
                return {"verified": False, "console_script_sha256": []}
            expected_hash = item.hash
            if expected_hash is not None:
                if expected_hash.mode != "sha256":
                    return {"verified": False, "console_script_sha256": []}
                encoded = base64.urlsafe_b64encode(bytes.fromhex(digest)).rstrip(b"=").decode()
                if encoded != expected_hash.value:
                    return {"verified": False, "console_script_sha256": []}
            elif not relative.endswith(".dist-info/RECORD"):
                return {"verified": False, "console_script_sha256": []}
            closure.update(relative.encode("utf-8"))
            closure.update(b"\0")
            closure.update(digest.encode("ascii"))
            closure.update(b"\0")
            closure.update(str(size).encode("ascii"))
            closure.update(b"\n")
            if relative.endswith(".dist-info/RECORD"):
                record_paths.append(located)
            if located.name.casefold() in {"clio-relay", "clio-relay.exe"}:
                console_digests.append(digest)
    except (OSError, ValueError):
        return {"verified": False, "console_script_sha256": []}
    if len(record_paths) != 1:
        return {"verified": False, "console_script_sha256": []}
    record_identity = regular_file_identity(record_paths[0])
    record_sha256 = hash_open_regular_file(record_paths[0], record_identity)
    verified = record_sha256 is not None and bool(console_digests)
    return {
        "record_path": str(record_paths[0]),
        "record_sha256": record_sha256,
        "runtime_closure_sha256": closure.hexdigest(),
        "runtime_file_count": len(files),
        "runtime_bytes": runtime_bytes,
        "console_script_sha256": sorted(set(console_digests)),
        "verified": verified,
    }


def uv_cache_dir(executable: Path) -> Path | None:
    """Return the cache directory reported by the exact uv executable."""
    identity = regular_file_identity(executable)
    if identity is None:
        return None
    try:
        completed = subprocess.run(
            [str(executable), "cache", "dir"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0 or regular_file_identity(executable) != identity:
        return None
    output = completed.stdout.strip()
    if not output or "\x00" in output or "\n" in output or "\r" in output:
        return None
    candidate = Path(output)
    if not candidate.is_absolute():
        return None
    try:
        return candidate.resolve(strict=True)
    except OSError:
        return None


def pyvenv_uv_version_marker(prefix: Path) -> str | None:
    """Read uv's version marker from a bounded, path-anchored ``pyvenv.cfg``."""
    config = prefix / "pyvenv.cfg"
    identity = regular_file_identity(config)
    if identity is None:
        return None
    content = read_open_regular_file(config, identity, maximum_bytes=MAX_PYVENV_CONFIG_BYTES)
    if content is None:
        return None
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
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
    version = values.get("uv")
    if (
        version is None
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)", version) is None
    ):
        return None
    return version
