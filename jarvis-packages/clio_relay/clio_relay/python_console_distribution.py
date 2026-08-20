"""Bind a direct Python console launcher to its complete installed wheel RECORD.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_python_console_distribution_identity`` and ``_direct_distribution_source_identity``
call ``_file_identity`` -- individually monkeypatched on the ``runner`` facade by
``tests/test_mcp_call_runner.py`` -- through ``_facade()`` (see
:mod:`clio_relay.clio_kit_runtime_identity` for the full reach-back
contract). ``_external_python_console_distribution_identity`` is not itself a
monkeypatch target here, so the fallback call to it is a normal import from
:mod:`clio_relay.python_external_distribution`.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sys
from importlib import metadata
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.bounded_file_io import _record_bound_sha256, _urlsafe_sha256_digest
from clio_relay.constants import (
    PYTHON_DISTRIBUTION_MAX_BYTES,
    PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS,
    PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS,
    PYTHON_DISTRIBUTION_MAX_FILES,
)
from clio_relay.python_external_distribution import (
    _external_python_console_distribution_identity,
)


def _python_console_distribution_identity(executable: Path) -> dict[str, Any]:
    """Bind a direct Python console launcher to its complete installed wheel RECORD."""
    evidence: dict[str, Any] = {
        "schema_version": "clio-relay.python-distribution-runtime.v1",
        "distribution": None,
        "distribution_version": None,
        "entry_point": None,
        "entry_point_value": None,
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "direct_url": None,
        "provider_interpreter": None,
        "external_launcher_identity": None,
        "contract_source_path": None,
        "server_lock_paths": {},
        "error": None,
    }
    try:
        resolved_executable = executable.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"could not resolve direct server executable: {exc}"
        return evidence
    launcher_identity = _facade()._file_identity(resolved_executable)
    if launcher_identity is None:
        evidence["error"] = "direct server executable has no stable file identity"
        return evidence
    evidence["external_launcher_identity"] = launcher_identity
    command_name = (
        resolved_executable.stem
        if resolved_executable.suffix.casefold() == ".exe"
        else resolved_executable.name
    )
    matches: list[tuple[metadata.Distribution, metadata.EntryPoint]] = []
    distribution_count = 0
    entry_point_count = 0
    try:
        distributions = metadata.distributions()
        for distribution in distributions:
            distribution_count += 1
            if distribution_count > PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS:
                evidence["error"] = "installed Python distribution count exceeded its limit"
                return evidence
            files = distribution.files
            if files is None or not _distribution_contains_executable(
                distribution,
                files,
                resolved_executable,
            ):
                continue
            for entry_point in distribution.entry_points:
                entry_point_count += 1
                if entry_point_count > PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS:
                    evidence["error"] = "installed Python entry-point count exceeded its limit"
                    return evidence
                if entry_point.group == "console_scripts" and entry_point.name == command_name:
                    matches.append((distribution, entry_point))
    except (OSError, TypeError, ValueError) as exc:
        evidence["error"] = f"could not inspect installed Python distributions: {exc}"
        return evidence
    if len(matches) != 1:
        return _external_python_console_distribution_identity(
            resolved_executable,
            command_name=command_name,
        )
    distribution, entry_point = matches[0]
    evidence.update(
        {
            "distribution": distribution.metadata.get("Name"),
            "distribution_version": distribution.version,
            "entry_point": entry_point.name,
            "entry_point_value": entry_point.value,
            "provider_interpreter": sys.executable,
        }
    )
    files = distribution.files or []
    for member in files:
        normalized = str(member).replace("\\", "/")
        path = str(Path(str(distribution.locate_file(member))).resolve())
        if normalized.endswith("clio_kit/__init__.py"):
            evidence["contract_source_path"] = path
        match = re.search(r"clio-kit-mcp-servers/([^/]+)/uv\.lock$", normalized)
        if match is not None:
            cast(dict[str, str], evidence["server_lock_paths"])[match.group(1)] = path
    direct_url = _distribution_direct_url(distribution)
    evidence["direct_url"] = direct_url
    if direct_url is not None:
        directory = direct_url.get("dir_info")
        typed_directory = cast(dict[str, Any], directory) if isinstance(directory, dict) else {}
        if typed_directory.get("editable") is True:
            evidence["error"] = "editable Python distributions have no immutable runtime closure"
            return evidence
    closure = _verify_distribution_record_closure(distribution)
    evidence.update(closure)
    return evidence


def _direct_distribution_source_identity(
    runtime: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return the retained wheel behind one verified persistent tool install."""
    direct_url = runtime.get("direct_url") if runtime is not None else None
    if not isinstance(direct_url, dict):
        return None
    url = cast(dict[str, Any], direct_url).get("url")
    if not isinstance(url, str):
        return None
    parsed = urlsplit(url)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        return None
    source_value = unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", source_value):
        source_value = source_value[1:]
    source = Path(source_value)
    if source.suffix.lower() != ".whl":
        return None
    return _facade()._file_identity(source)


def _distribution_contains_executable(
    distribution: metadata.Distribution,
    files: list[metadata.PackagePath],
    executable: Path,
) -> bool:
    """Return whether a distribution RECORD owns the exact console launcher path."""
    for member in files:
        try:
            candidate = Path(str(distribution.locate_file(member))).resolve(strict=True)
        except OSError:
            continue
        if candidate == executable:
            return True
    return False


def _distribution_direct_url(distribution: metadata.Distribution) -> dict[str, Any] | None:
    """Read PEP 610 provenance without trusting malformed metadata."""
    try:
        raw = distribution.read_text("direct_url.json")
    except (OSError, UnicodeDecodeError):
        return None
    if raw is None:
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return cast(dict[str, Any], decoded) if isinstance(decoded, dict) else None


def _verify_distribution_record_closure(
    distribution: metadata.Distribution,
) -> dict[str, Any]:
    """Verify every installed wheel file against RECORD and digest the exact closure."""
    files = distribution.files
    failure: dict[str, Any] = {
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "error": None,
    }
    if files is None or not files or len(files) > PYTHON_DISTRIBUTION_MAX_FILES:
        failure["error"] = "Python distribution RECORD file list was missing or exceeded its limit"
        return failure
    normalized_names: set[str] = set()
    record_members: list[metadata.PackagePath] = []
    total_bytes = 0
    closure_inputs: list[tuple[str, int, str]] = []
    for member in files:
        normalized = str(member).replace("\\", "/")
        if normalized in normalized_names:
            failure["error"] = "Python distribution RECORD contained duplicate paths"
            return failure
        normalized_names.add(normalized)
        if normalized.endswith(".dist-info/RECORD"):
            record_members.append(member)
            continue
        expected_hash = member.hash
        expected_size = member.size
        if (
            expected_hash is None
            or expected_hash.mode != "sha256"
            or expected_size is None
            or expected_size < 0
        ):
            failure["error"] = (
                f"Python distribution RECORD entry was not SHA-256 bound: {normalized}"
            )
            return failure
        total_bytes += expected_size
        if total_bytes > PYTHON_DISTRIBUTION_MAX_BYTES:
            failure["error"] = "Python distribution RECORD byte total exceeded its limit"
            return failure
        path = Path(str(distribution.locate_file(member)))
        actual_hash = _record_bound_sha256(path, expected_size=expected_size)
        if actual_hash is None:
            failure["error"] = f"Python distribution file was missing or unstable: {normalized}"
            return failure
        expected_digest = _urlsafe_sha256_digest(expected_hash.value)
        if expected_digest is None or not hmac.compare_digest(actual_hash, expected_digest):
            failure["error"] = f"Python distribution RECORD hash mismatch: {normalized}"
            return failure
        closure_inputs.append((normalized, expected_size, actual_hash))
    if len(record_members) != 1:
        failure["error"] = "Python distribution had no unique RECORD file"
        return failure
    record_path = Path(str(distribution.locate_file(record_members[0])))
    try:
        record_size = record_path.lstat().st_size
    except OSError:
        record_size = -1
    record_sha256 = _record_bound_sha256(record_path, expected_size=record_size)
    if record_sha256 is None:
        failure["error"] = "Python distribution RECORD file was missing"
        return failure
    closure_hash = hashlib.sha256()
    for normalized, size_bytes, digest in sorted(closure_inputs):
        encoded = normalized.encode("utf-8")
        closure_hash.update(len(encoded).to_bytes(8, "big"))
        closure_hash.update(encoded)
        closure_hash.update(size_bytes.to_bytes(8, "big"))
        closure_hash.update(bytes.fromhex(digest))
    closure_hash.update(bytes.fromhex(record_sha256))
    return {
        "record_sha256": record_sha256,
        "runtime_closure_sha256": closure_hash.hexdigest(),
        "runtime_file_count": len(closure_inputs),
        "runtime_bytes": total_bytes,
        "runtime_closure_verified": True,
        "error": None,
    }
