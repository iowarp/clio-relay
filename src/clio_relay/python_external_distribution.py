"""Verify a console script installed in an isolated persistent-tool environment.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_external_python_console_distribution_identity`` calls ``_persistent_tool_launcher_shebang``
(defined in this same module) and ``_file_identity`` -- both individually
monkeypatched on the ``runner`` facade by ``tests/test_mcp_call_runner.py``, even
for the same-module call. A monkeypatched facade attribute is a *different*
binding from this module's own top-level name, so both calls go through
``_facade()`` -- see :mod:`clio_relay.clio_kit_runtime_identity` for the
full reach-back contract this decomposition wave relies on. ``subprocess.run`` is
called via the ``subprocess`` module object itself (not ``from subprocess import
run``), so ``monkeypatch.setattr(runner.subprocess, "run", ...)`` -- which
mutates the one shared ``subprocess`` module every importer sees -- still takes
effect here without any reach-back.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import shlex
import subprocess
import zipfile
from pathlib import Path
from typing import Any, cast

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.bounded_file_io import (
    _bounded_regular_file_snapshot,
    _record_bound_sha256,
    _urlsafe_sha256_digest,
)
from clio_relay.clio_kit_wheel_archive import (
    _read_bounded_zip_member,
    _zip_member_is_regular,
)
from clio_relay.constants import (
    CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
    PYTHON_DISTRIBUTION_MAX_BYTES,
    PYTHON_DISTRIBUTION_MAX_FILES,
    PYTHON_TOOL_IDENTITY_MAX_BYTES,
    PYTHON_TOOL_IDENTITY_TIMEOUT_SECONDS,
)
from clio_relay.wheel_snapshot_identity import _file_descriptor_identity


def _persistent_tool_launcher_shebang(payload: bytes, *, executable_name: str) -> str:
    """Return the provider shebang from a script or Windows uv trampoline."""
    script = payload
    if not payload.startswith(b"#!"):
        if Path(executable_name).suffix.casefold() != ".exe":
            raise ValueError("persistent tool launcher has no Python shebang")
        with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
            candidates = [
                member for member in archive.infolist() if member.filename == "__main__.py"
            ]
            if len(candidates) != 1 or not _zip_member_is_regular(candidates[0]):
                raise ValueError("Windows persistent tool launcher has no unique __main__.py")
            if candidates[0].flag_bits & 0x1:
                raise ValueError("Windows persistent tool launcher script is encrypted")
            script = _read_bounded_zip_member(
                archive,
                candidates[0].filename,
                max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
            )
    lines = script.split(b"\n", 3)
    first_line = lines[0]
    if len(first_line) > 4096:
        raise ValueError("persistent tool launcher shebang exceeded its byte limit")
    shebang = first_line.decode("utf-8", errors="strict").rstrip("\r")
    if not shebang.startswith("#!") or not shebang[2:]:
        raise ValueError("persistent tool launcher has no direct Python interpreter shebang")
    if "\x00" in shebang:
        raise ValueError("persistent tool launcher shebang contains a null byte")
    if shebang == "#!/bin/sh":
        if len(lines) < 3 or any(len(line) > 4096 for line in lines[1:3]):
            raise ValueError("persistent uv shell trampoline is incomplete or oversized")
        execution_line = lines[1].decode("utf-8", errors="strict").rstrip("\r")
        closing_line = lines[2].decode("utf-8", errors="strict").rstrip("\r")
        try:
            execution = shlex.split(execution_line, posix=True)
        except ValueError as exc:
            raise ValueError("persistent uv shell trampoline has invalid quoting") from exc
        if len(execution) != 4 or execution[0] != "exec" or execution[2:] != ["$0", "$@"]:
            raise ValueError("persistent uv shell trampoline has an unsupported exec contract")
        provider = execution[1]
        quoted_provider = "'" + provider.replace("'", "'\"'\"'") + "'"
        canonical_execution_line = f"'''exec' {quoted_provider} \"$0\" \"$@\""
        if (
            not provider.startswith("/")
            or "\x00" in provider
            or execution_line != canonical_execution_line
            or closing_line != "' '''"
        ):
            raise ValueError("persistent uv shell trampoline has an invalid provider contract")
        return f"#!{provider}"
    return shebang


def _external_python_console_distribution_identity(
    executable: Path,
    *,
    command_name: str,
) -> dict[str, Any]:
    """Verify a console script installed in an isolated persistent tool environment."""
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
        "provider_interpreter_identity": None,
        "external_launcher_identity": None,
        "distribution_console_script": None,
        "launcher_copy_verified": False,
        "contract_source_path": None,
        "server_lock_paths": {},
        "error": None,
    }
    launcher_snapshot = _bounded_regular_file_snapshot(
        executable,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if launcher_snapshot is None:
        evidence["error"] = "persistent tool launcher is not one stable bounded file"
        return evidence
    launcher_bytes, launcher_descriptor = launcher_snapshot
    launcher_sha256 = hashlib.sha256(launcher_bytes).hexdigest()
    evidence["external_launcher_identity"] = {
        "path": str(executable),
        "filename": executable.name,
        "sha256": launcher_sha256,
        "size_bytes": len(launcher_bytes),
    }
    try:
        shebang = _facade()._persistent_tool_launcher_shebang(
            launcher_bytes,
            executable_name=executable.name,
        )
    except (
        NotImplementedError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
        zipfile.BadZipFile,
    ) as exc:
        evidence["error"] = f"could not read persistent tool launcher: {exc}"
        return evidence
    if not shebang.startswith("#!") or not shebang[2:]:
        evidence["error"] = "persistent tool launcher has no direct Python interpreter shebang"
        return evidence
    provider_launcher = Path(shebang[2:]).expanduser()
    if not provider_launcher.is_absolute():
        evidence["error"] = "persistent tool provider interpreter path is not absolute"
        return evidence
    try:
        provider_launcher_identity = _file_descriptor_identity(provider_launcher.lstat())
        provider = provider_launcher.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool provider interpreter is unavailable: {exc}"
        return evidence
    provider_identity = _facade()._file_identity(provider)
    if provider_identity is None:
        evidence["error"] = "persistent tool provider interpreter has no file identity"
        return evidence
    probe = r"""
import hashlib
import json
import sys
from importlib import metadata
from pathlib import Path

command_name = sys.argv[1]
external_launcher_sha256 = sys.argv[2]
matches = []
distribution_count = 0
entry_point_count = 0
candidate_names = {command_name.casefold(), f"{command_name}.exe".casefold()}
for distribution in metadata.distributions():
    distribution_count += 1
    if distribution_count > 10_000:
        raise SystemExit("installed Python distribution count exceeded its limit")
    files = distribution.files or []
    if len(files) > 100_000:
        raise SystemExit("installed Python distribution file count exceeded its limit")
    entry_points = []
    for entry_point in distribution.entry_points:
        entry_point_count += 1
        if entry_point_count > 100_000:
            raise SystemExit("installed Python entry-point count exceeded its limit")
        if entry_point.group == "console_scripts" and entry_point.name == command_name:
            entry_points.append(entry_point)
    if len(entry_points) != 1:
        continue
    for launcher_member in files:
        located_launcher = Path(str(distribution.locate_file(launcher_member))).resolve()
        if located_launcher.name.casefold() not in candidate_names:
            continue
        launcher_stat = located_launcher.stat()
        if not located_launcher.is_file() or not 1 <= launcher_stat.st_size <= 8 * 1024 * 1024:
            continue
        launcher_hash = hashlib.sha256()
        with located_launcher.open("rb") as launcher_stream:
            while launcher_chunk := launcher_stream.read(1024 * 1024):
                launcher_hash.update(launcher_chunk)
        launcher_after = located_launcher.stat()
        launcher_key = (
            launcher_stat.st_dev,
            launcher_stat.st_ino,
            launcher_stat.st_size,
            launcher_stat.st_mtime_ns,
        )
        if (
            launcher_after.st_dev,
            launcher_after.st_ino,
            launcher_after.st_size,
            launcher_after.st_mtime_ns,
        ) != launcher_key:
            raise SystemExit("persistent tool RECORD-owned launcher changed during inspection")
        if launcher_hash.hexdigest() != external_launcher_sha256:
            continue
        entry_point = entry_points[0]
        direct_url_text = distribution.read_text("direct_url.json")
        direct_url = json.loads(direct_url_text) if direct_url_text else None
        serialized_files = []
        contract_source_path = None
        server_lock_paths = {}
        for member in files:
            normalized = str(member).replace("\\", "/")
            located = str(Path(str(distribution.locate_file(member))).resolve())
            member_hash = member.hash
            serialized_files.append({
                "name": normalized,
                "path": located,
                "hash_mode": member_hash.mode if member_hash is not None else None,
                "hash_value": member_hash.value if member_hash is not None else None,
                "size": member.size,
            })
            if normalized.endswith("clio_kit/__init__.py"):
                contract_source_path = located
            marker = "clio-kit-mcp-servers/"
            if marker in normalized and normalized.endswith("/uv.lock"):
                server_name = normalized.split(marker, 1)[1].split("/", 1)[0]
                server_lock_paths[server_name] = located
        matches.append({
            "executable": sys.executable,
            "distribution_console_script": str(located_launcher),
            "distribution_console_script_sha256": launcher_hash.hexdigest(),
            "distribution": distribution.metadata.get("Name"),
            "distribution_version": distribution.version,
            "entry_point": entry_point.name,
            "entry_point_value": entry_point.value,
            "direct_url": direct_url,
            "files": serialized_files,
            "contract_source_path": contract_source_path,
            "server_lock_paths": server_lock_paths,
        })
print(json.dumps({"matches": matches}, sort_keys=True))
"""
    try:
        completed = subprocess.run(
            [str(provider_launcher), "-I", "-c", probe, command_name, launcher_sha256],
            check=False,
            capture_output=True,
            text=True,
            timeout=PYTHON_TOOL_IDENTITY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        evidence["error"] = f"persistent tool distribution probe failed: {exc}"
        return evidence
    try:
        launcher_after = _file_descriptor_identity(provider_launcher.lstat())
        provider_after = provider_launcher.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool provider interpreter changed: {exc}"
        return evidence
    if (
        launcher_after != provider_launcher_identity
        or provider_after != provider
        or _facade()._file_identity(provider) != provider_identity
    ):
        evidence["error"] = "persistent tool provider interpreter changed during inspection"
        return evidence
    stdout_bytes = completed.stdout.encode("utf-8")
    if (
        completed.returncode != 0
        or not stdout_bytes
        or len(stdout_bytes) > PYTHON_TOOL_IDENTITY_MAX_BYTES
    ):
        evidence["error"] = "persistent tool distribution probe returned no bounded evidence"
        return evidence
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        evidence["error"] = f"persistent tool distribution probe returned invalid JSON: {exc}"
        return evidence
    decoded_mapping = cast(dict[str, object], decoded) if isinstance(decoded, dict) else {}
    raw_matches: object = decoded_mapping.get("matches")
    if not isinstance(raw_matches, list):
        evidence["error"] = (
            "persistent tool launcher has no unique installed console-script distribution"
        )
        return evidence
    matches = cast(list[object], raw_matches)
    if len(matches) != 1 or not isinstance(matches[0], dict):
        evidence["error"] = (
            "persistent tool launcher has no unique installed console-script distribution"
        )
        return evidence
    identity = cast(dict[str, Any], matches[0])
    direct_url = identity.get("direct_url")
    if isinstance(direct_url, dict):
        dir_info = cast(dict[str, Any], direct_url).get("dir_info")
        if isinstance(dir_info, dict) and cast(dict[str, Any], dir_info).get("editable") is True:
            evidence["error"] = "editable Python distributions have no immutable runtime closure"
            return evidence
    raw_files = identity.get("files")
    if not isinstance(raw_files, list):
        evidence["error"] = "persistent tool distribution omitted its RECORD closure"
        return evidence
    try:
        observed_provider = Path(str(identity.get("executable"))).resolve(strict=True)
    except OSError:
        observed_provider = Path("__unverified_provider__")
    if observed_provider != provider:
        evidence["error"] = "persistent tool probe executed under the wrong interpreter"
        return evidence
    console_script_value = identity.get("distribution_console_script")
    if not isinstance(console_script_value, str):
        evidence["error"] = "persistent tool distribution omitted its RECORD-owned launcher"
        return evidence
    try:
        console_script = Path(console_script_value).resolve(strict=True)
        provider_environment_bin = provider_launcher.parent.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool RECORD-owned launcher is unavailable: {exc}"
        return evidence
    console_script_snapshot = _bounded_regular_file_snapshot(
        console_script,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if console_script_snapshot is None:
        evidence["error"] = "persistent tool RECORD-owned launcher is not one bounded file"
        return evidence
    console_script_bytes, console_script_descriptor = console_script_snapshot
    console_script_sha256 = hashlib.sha256(console_script_bytes).hexdigest()
    if (
        console_script.parent != provider_environment_bin
        or identity.get("distribution_console_script_sha256") != launcher_sha256
        or not hmac.compare_digest(console_script_sha256, launcher_sha256)
        or not hmac.compare_digest(console_script_bytes, launcher_bytes)
    ):
        evidence["error"] = (
            "persistent tool launcher does not match its RECORD-owned console script"
        )
        return evidence
    closure = _verify_external_distribution_record_closure(cast(list[object], raw_files))
    launcher_after = _bounded_regular_file_snapshot(
        executable,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    console_script_after = _bounded_regular_file_snapshot(
        console_script,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if (
        launcher_after is None
        or console_script_after is None
        or launcher_after[1] != launcher_descriptor
        or console_script_after[1] != console_script_descriptor
        or not hmac.compare_digest(launcher_after[0], launcher_bytes)
        or not hmac.compare_digest(console_script_after[0], console_script_bytes)
    ):
        evidence["error"] = "persistent tool launcher changed during inspection"
        return evidence
    evidence.update(
        {
            "distribution": identity.get("distribution"),
            "distribution_version": identity.get("distribution_version"),
            "entry_point": identity.get("entry_point"),
            "entry_point_value": identity.get("entry_point_value"),
            "direct_url": identity.get("direct_url"),
            "provider_interpreter": str(provider),
            "provider_interpreter_identity": provider_identity,
            "distribution_console_script": {
                "path": str(console_script),
                "filename": console_script.name,
                "sha256": console_script_sha256,
                "size_bytes": len(console_script_bytes),
            },
            "launcher_copy_verified": True,
            "contract_source_path": identity.get("contract_source_path"),
            "server_lock_paths": identity.get("server_lock_paths", {}),
            **closure,
        }
    )
    return evidence


def _verify_external_distribution_record_closure(
    raw_files: list[object],
) -> dict[str, Any]:
    """Verify the RECORD closure described by an isolated tool interpreter."""
    failure: dict[str, Any] = {
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "error": None,
    }
    if not raw_files or len(raw_files) > PYTHON_DISTRIBUTION_MAX_FILES:
        failure["error"] = "persistent tool RECORD file list was missing or exceeded its limit"
        return failure
    names: set[str] = set()
    record_paths: list[Path] = []
    closure_inputs: list[tuple[str, int, str]] = []
    total_bytes = 0
    for item in raw_files:
        if not isinstance(item, dict):
            failure["error"] = "persistent tool RECORD entry was not an object"
            return failure
        member = cast(dict[str, Any], item)
        name = member.get("name")
        path = member.get("path")
        if not isinstance(name, str) or not name or name in names or not isinstance(path, str):
            failure["error"] = "persistent tool RECORD contained an invalid or duplicate path"
            return failure
        names.add(name)
        member_path = Path(path)
        if name.endswith(".dist-info/RECORD"):
            record_paths.append(member_path)
            continue
        size = member.get("size")
        if (
            member.get("hash_mode") != "sha256"
            or not isinstance(member.get("hash_value"), str)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            failure["error"] = f"persistent tool RECORD entry was not SHA-256 bound: {name}"
            return failure
        total_bytes += size
        if total_bytes > PYTHON_DISTRIBUTION_MAX_BYTES:
            failure["error"] = "persistent tool RECORD byte total exceeded its limit"
            return failure
        actual = _record_bound_sha256(member_path, expected_size=size)
        expected = _urlsafe_sha256_digest(cast(str, member["hash_value"]))
        if actual is None or expected is None or not hmac.compare_digest(actual, expected):
            failure["error"] = f"persistent tool distribution file hash mismatch: {name}"
            return failure
        closure_inputs.append((name, size, actual))
    if len(record_paths) != 1:
        failure["error"] = "persistent tool distribution had no unique RECORD file"
        return failure
    try:
        record_size = record_paths[0].lstat().st_size
    except OSError:
        record_size = -1
    record_sha256 = _record_bound_sha256(record_paths[0], expected_size=record_size)
    if record_sha256 is None:
        failure["error"] = "persistent tool RECORD file was missing"
        return failure
    closure_hash = hashlib.sha256()
    for name, size, digest in sorted(closure_inputs):
        encoded = name.encode("utf-8")
        closure_hash.update(len(encoded).to_bytes(8, "big"))
        closure_hash.update(encoded)
        closure_hash.update(size.to_bytes(8, "big"))
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
