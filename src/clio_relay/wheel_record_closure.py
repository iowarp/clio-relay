"""Verify wheel-owned installed bytes through a bounded RECORD closure walk.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns
``_probe_python_distribution_record_closure``, the one probe that launches an
isolated interpreter to walk a distribution's RECORD entries, re-derive each
member's digest from disk, and verify installed console-script/data-file
bytes trace back to their wheel. Its size is entirely the embedded
verification script (installed script/launcher provenance, console-script
trampoline shape, wheel-member ownership) that script runs in the target
interpreter -- there is no clean internal seam to split further without
duplicating verified state across files, so this module stays over the
usual 150-500-line sweet spot by design (documented here per the ratchet
guard in scripts/check_file_size.py).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

from clio_relay.bounded_process import BoundedProcessError, run_bounded_process

_PYTHON_RECORD_CLOSURE_SAFE_ERRORS = frozenset(
    {
        "execution data path is outside its environment",
        "execution environment has no data installation path",
        "execution environment has no scripts installation path",
        "execution scripts path is outside its environment",
        "installed console script is not a canonical declared wrapper",
        "installed wheel script body does not match its wheel member",
        "installed wheel script disagrees with distribution RECORD",
        "installed wheel script exceeds the byte bound",
        "installed wheel script has an invalid POSIX shell trampoline",
        "installed wheel script is not bound to the execution interpreter",
        "installed wheel script is not executable",
        "installed wheel script is not owned by distribution RECORD",
        "installed wheel script must not be a symbolic link",
        "installed wheel data file is not owned by distribution RECORD",
        "installed wheel data file must not be a symbolic link",
        "wheel script does not use an exact #!python shebang",
        "wheel script has an ambiguous console_scripts declaration",
        "wheel script has an unsafe member name",
        "wheel script is not installed in the execution environment",
        "wheel script console_scripts target is not canonical",
        "wheel script exceeds the byte bound",
        "wheel scripts must use one flat member name",
        "wheel data file is not installed in the execution environment",
    }
)


def _python_record_closure_error_code(error: str) -> str:
    """Return a bounded public diagnostic code for one known closure error."""
    if error not in _PYTHON_RECORD_CLOSURE_SAFE_ERRORS:
        return "unclassified-record-closure-error"
    return error.replace("#!", "hashbang-").replace("_", "-").replace(" ", "-")


def _probe_python_distribution_record_closure(
    python: str | None,
    distribution_name: str,
    expected_artifact: Path | None,
    *,
    environment: dict[str, str] | None = None,
) -> dict[str, object]:
    """Verify wheel-owned installed bytes through one bounded RECORD closure."""
    if python is None or expected_artifact is None:
        return {
            "verified": False,
            "error": "execution interpreter or source wheel is not configured",
            "tree_scanned": False,
            "tree_copied": False,
        }
    script = r"""
import base64
import csv
import hashlib
import io
import json
import keyword
import os
import shlex
import stat
import sys
import sysconfig
import zipfile
from importlib import metadata
from pathlib import Path, PurePosixPath

MAX_FILES = 100_000
MAX_BYTES = 4 * 1024 * 1024 * 1024
MAX_SCRIPT_BYTES = 16 * 1024 * 1024


def digest_stream(stream):
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
        if size > MAX_BYTES:
            raise SystemExit("distribution RECORD member exceeds the byte bound")
    return digest, size


def within(path, roots):
    return any(path == root or root in path.parents for root in roots)


distribution_name, wheel_text = sys.argv[1:]
wheel = Path(wheel_text).resolve(strict=True)
if not wheel.is_file():
    raise SystemExit("retained distribution wheel is not a regular file")
installed = metadata.distribution(distribution_name)
installed_files = installed.files
if not installed_files or len(installed_files) > MAX_FILES:
    raise SystemExit("installed distribution has no bounded RECORD closure")
distribution_root = Path(installed.locate_file("")).resolve(strict=True)
environment_prefix = Path(sys.prefix).resolve(strict=True)
allowed_roots = (distribution_root, environment_prefix)
installed_closure = hashlib.sha256()
installed_bytes = 0
installed_record_paths = []
installed_record_locations = {}
for item in sorted(installed_files, key=lambda value: str(value)):
    relative = str(item).replace("\\", "/")
    location = Path(installed.locate_file(item)).resolve(strict=True)
    if not within(location, allowed_roots) or not location.is_file():
        raise SystemExit("installed RECORD contains a file outside its environment")
    location_key = os.path.normcase(str(location))
    if location_key in installed_record_locations:
        raise SystemExit("installed RECORD maps multiple members to one file")
    with location.open("rb") as stream:
        digest, size = digest_stream(stream)
    installed_bytes += size
    if installed_bytes > MAX_BYTES:
        raise SystemExit("installed distribution RECORD closure exceeds the byte bound")
    expected_hash = item.hash
    if expected_hash is not None:
        if expected_hash.mode != "sha256":
            raise SystemExit("installed RECORD uses an unsupported digest")
        encoded = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
        if encoded != expected_hash.value:
            raise SystemExit("installed RECORD member digest mismatch")
    elif not (relative.endswith(".dist-info/RECORD") or relative.endswith(".pyc")):
        raise SystemExit("installed RECORD member omitted its digest")
    if relative.endswith(".dist-info/RECORD"):
        installed_record_paths.append(location)
    installed_record_locations[location_key] = {
        "relative": relative,
        "sha256": digest.digest(),
        "size": size,
    }
    installed_closure.update(relative.encode("utf-8"))
    installed_closure.update(b"\0")
    installed_closure.update(digest.hexdigest().encode("ascii"))
    installed_closure.update(b"\0")
    installed_closure.update(str(size).encode("ascii"))
    installed_closure.update(b"\n")
if len(installed_record_paths) != 1:
    raise SystemExit("installed distribution RECORD ownership is ambiguous")

console_scripts = {}
for entry_point in installed.entry_points:
    if entry_point.group == "console_scripts":
        console_scripts.setdefault(entry_point.name, []).append(entry_point.value)


def console_script_target(script_name):
    values = console_scripts.get(script_name, [])
    if not values:
        return None
    if len(values) != 1:
        raise SystemExit("wheel script has an ambiguous console_scripts declaration")
    value = values[0]
    if not isinstance(value, str) or value != value.strip() or "[" in value or "]" in value:
        raise SystemExit("wheel script console_scripts target is not canonical")
    module, separator, attribute = value.partition(":")
    identifiers = [*module.split("."), attribute]
    if (
        separator != ":"
        or not module
        or not attribute
        or "." in attribute
        or any(not item.isidentifier() or keyword.iskeyword(item) for item in identifiers)
    ):
        raise SystemExit("wheel script console_scripts target is not canonical")
    return module, attribute, value


def canonical_console_script_bodies(module, attribute):
    current = (
        "import sys\n"
        f"from {module} import {attribute}\n"
        "if __name__ == '__main__':\n"
        "    sys.argv[0] = sys.argv[0].removesuffix('.exe')\n"
        f"    sys.exit({attribute}())\n"
    ).encode("utf-8")
    legacy = (
        "# -*- coding: utf-8 -*-\n"
        "import re\n"
        "import sys\n"
        f"from {module} import {attribute}\n"
        "if __name__ == '__main__':\n"
        r"    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])"
        "\n"
        f"    sys.exit({attribute}())\n"
    ).encode("utf-8")
    return current, legacy


def wheel_script_body(payload):
    for shebang in (b"#!python\n", b"#!python\r\n", b"#!pythonw\n", b"#!pythonw\r\n"):
        if payload.startswith(shebang):
            return payload[len(shebang):], shebang.rstrip(b"\r\n").decode("ascii")
    raise SystemExit("wheel script does not use an exact #!python shebang")


def installed_launcher_body(payload):
    executable = os.fsencode(sys.executable)
    for shebang in (b"#!" + executable + b"\n", b"#!" + executable + b"\r\n"):
        if payload.startswith(shebang):
            return payload[len(shebang):], "direct-interpreter"
    if os.name != "posix":
        raise SystemExit("installed wheel script is not bound to the execution interpreter")
    lines = payload.split(b"\n", 3)
    if len(lines) != 4 or lines[0] != b"#!/bin/sh":
        raise SystemExit("installed wheel script is not bound to the execution interpreter")
    if any(len(line) > 4096 for line in lines[:3]):
        raise SystemExit("installed wheel script trampoline exceeds its byte bound")
    try:
        execution_line = lines[1].decode("utf-8")
        closing_line = lines[2].decode("utf-8")
        execution = shlex.split(execution_line, posix=True)
    except (UnicodeDecodeError, ValueError) as exc:
        raise SystemExit("installed wheel script has an invalid POSIX shell trampoline") from exc
    quoted_executable = "'" + sys.executable.replace("'", "'\"'\"'") + "'"
    canonical_uv_line = f"'''exec' {quoted_executable} \"$0\" \"$@\""
    canonical_pip_line = f"'''exec' {sys.executable} \"$0\" \"$@\""
    if (
        len(execution) != 4
        or execution[0] != "exec"
        or execution[1] != sys.executable
        or execution[2:] != ["$0", "$@"]
        or closing_line != "' '''"
    ):
        raise SystemExit("installed wheel script has an invalid POSIX shell trampoline")
    if execution_line == canonical_uv_line:
        launcher_kind = "uv-posix-trampoline"
    elif shlex.quote(sys.executable) == sys.executable and execution_line == canonical_pip_line:
        launcher_kind = "pip-posix-trampoline"
    else:
        raise SystemExit("installed wheel script has an invalid POSIX shell trampoline")
    return lines[3], launcher_kind


def verify_wheel_script(archive, info, member, path):
    if len(path.parts) != 3:
        raise SystemExit("wheel scripts must use one flat member name")
    script_name = path.parts[2]
    if (
        not script_name
        or script_name.startswith(".")
        or not script_name.isascii()
        or any(not (character.isalnum() or character in "._+-") for character in script_name)
    ):
        raise SystemExit("wheel script has an unsafe member name")
    scripts_value = sysconfig.get_path("scripts")
    if not isinstance(scripts_value, str) or not scripts_value:
        raise SystemExit("execution environment has no scripts installation path")
    scripts_root = Path(scripts_value).resolve(strict=True)
    if not within(scripts_root, (environment_prefix,)):
        raise SystemExit("execution scripts path is outside its environment")
    candidate = scripts_root / script_name
    if candidate.is_symlink():
        raise SystemExit("installed wheel script must not be a symbolic link")
    installed_location = candidate.resolve(strict=True)
    if not within(installed_location, (environment_prefix,)) or not installed_location.is_file():
        raise SystemExit("wheel script is not installed in the execution environment")
    if os.name == "posix" and installed_location.stat().st_mode & 0o111 == 0:
        raise SystemExit("installed wheel script is not executable")
    installed_record = installed_record_locations.get(
        os.path.normcase(str(installed_location))
    )
    if installed_record is None:
        raise SystemExit("installed wheel script is not owned by distribution RECORD")
    if info.file_size > MAX_SCRIPT_BYTES:
        raise SystemExit("wheel script exceeds the byte bound")
    with archive.open(info) as stream:
        wheel_payload = stream.read(MAX_SCRIPT_BYTES + 1)
    if len(wheel_payload) > MAX_SCRIPT_BYTES:
        raise SystemExit("wheel script exceeds the byte bound")
    with installed_location.open("rb") as stream:
        installed_payload = stream.read(MAX_SCRIPT_BYTES + 1)
    if len(installed_payload) > MAX_SCRIPT_BYTES:
        raise SystemExit("installed wheel script exceeds the byte bound")
    installed_digest = hashlib.sha256(installed_payload).digest()
    if (
        installed_record["size"] != len(installed_payload)
        or installed_record["sha256"] != installed_digest
    ):
        raise SystemExit("installed wheel script disagrees with distribution RECORD")
    source_body, source_shebang = wheel_script_body(wheel_payload)
    installed_body, launcher_kind = installed_launcher_body(installed_payload)
    target = console_script_target(script_name)
    if target is None:
        if installed_body != source_body:
            raise SystemExit("installed wheel script body does not match its wheel member")
        transform_kind = "interpreter-shebang"
        entry_point = None
    else:
        module, attribute, entry_point = target
        if installed_body not in canonical_console_script_bodies(module, attribute):
            raise SystemExit("installed console script is not a canonical declared wrapper")
        transform_kind = "declared-console-wrapper"
    return {
        "member": member,
        "installed_record_member": installed_record["relative"],
        "transform": transform_kind,
        "launcher": launcher_kind,
        "source_shebang": source_shebang,
        "entry_point": entry_point,
    }


def locate_wheel_data_file(path):
    relative = PurePosixPath(*path.parts[2:])
    data_value = sysconfig.get_path("data")
    if not isinstance(data_value, str) or not data_value:
        raise SystemExit("execution environment has no data installation path")
    data_root = Path(data_value).resolve(strict=True)
    if not within(data_root, (environment_prefix,)):
        raise SystemExit("execution data path is outside its environment")
    candidate = data_root.joinpath(*relative.parts)
    if candidate.is_symlink():
        raise SystemExit("installed wheel data file must not be a symbolic link")
    installed_location = candidate.resolve(strict=True)
    if (
        not within(installed_location, (data_root, environment_prefix))
        or not installed_location.is_file()
    ):
        raise SystemExit("wheel data file is not installed in the execution environment")
    if os.path.normcase(str(installed_location)) not in installed_record_locations:
        raise SystemExit("installed wheel data file is not owned by distribution RECORD")
    return installed_location

wheel_closure = hashlib.sha256()
wheel_bytes = 0
wheel_members = 0
wheel_script_transforms = []
with zipfile.ZipFile(wheel) as archive:
    infos = archive.infolist()
    if not infos or len(infos) > MAX_FILES:
        raise SystemExit("retained wheel has no bounded member closure")
    record_names = [
        info.filename
        for info in infos
        if not info.is_dir() and info.filename.endswith(".dist-info/RECORD")
    ]
    if len(record_names) != 1:
        raise SystemExit("retained wheel RECORD ownership is ambiguous")
    record_name = record_names[0]
    record_info = archive.getinfo(record_name)
    if record_info.file_size > 16 * 1024 * 1024:
        raise SystemExit("retained wheel RECORD exceeds the byte bound")
    with archive.open(record_info) as record_stream:
        record_bytes = record_stream.read(16 * 1024 * 1024 + 1)
    if len(record_bytes) > 16 * 1024 * 1024:
        raise SystemExit("retained wheel RECORD exceeds the byte bound")
    rows = list(csv.reader(io.StringIO(record_bytes.decode("utf-8"))))
    if len(rows) > MAX_FILES or any(len(row) != 3 for row in rows):
        raise SystemExit("retained wheel RECORD has an invalid shape")
    record = {row[0]: (row[1], row[2]) for row in rows}
    if len(record) != len(rows):
        raise SystemExit("retained wheel RECORD contains duplicate members")
    archive_files = {info.filename for info in infos if not info.is_dir()}
    if set(record) != archive_files:
        raise SystemExit("retained wheel RECORD does not cover every member")
    for info in sorted(infos, key=lambda value: value.filename):
        if info.is_dir():
            continue
        member = info.filename
        path = PurePosixPath(member)
        if (
            not member
            or "\\" in member
            or path.is_absolute()
            or ".." in path.parts
            or "\x00" in member
        ):
            raise SystemExit("retained wheel contains an unsafe member path")
        mode = (info.external_attr >> 16) & 0o177777
        file_type = stat.S_IFMT(mode)
        if file_type not in {0, stat.S_IFREG}:
            raise SystemExit("retained wheel contains a non-regular member")
        expected_digest, expected_size = record[member]
        with archive.open(info) as stream:
            digest, size = digest_stream(stream)
        wheel_bytes += size
        if wheel_bytes > MAX_BYTES:
            raise SystemExit("retained wheel closure exceeds the byte bound")
        if expected_size and int(expected_size) != size:
            raise SystemExit("retained wheel member size does not match RECORD")
        if member == record_name:
            if expected_digest or expected_size:
                raise SystemExit("retained wheel RECORD self-entry is invalid")
            continue
        if not expected_digest.startswith("sha256="):
            raise SystemExit("retained wheel member omitted a SHA-256 digest")
        encoded = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
        if encoded != expected_digest.removeprefix("sha256="):
            raise SystemExit("retained wheel member digest does not match RECORD")
        parts = path.parts
        script_transform = None
        installed_location = None
        if parts and parts[0].endswith(".data"):
            if len(parts) < 3:
                raise SystemExit("retained wheel uses an unsupported installation scheme")
            if parts[1] == "scripts":
                script_transform = verify_wheel_script(archive, info, member, path)
                wheel_script_transforms.append(script_transform)
            elif parts[1] in {"purelib", "platlib"}:
                installed_relative = PurePosixPath(*parts[2:])
            elif parts[1] == "data":
                installed_location = locate_wheel_data_file(path)
            else:
                raise SystemExit("retained wheel uses an unsupported installation scheme")
        else:
            installed_relative = path
        if script_transform is None:
            if installed_location is None:
                installed_location = Path(
                    installed.locate_file(str(installed_relative))
                ).resolve(strict=True)
            if not within(installed_location, allowed_roots) or not installed_location.is_file():
                raise SystemExit("wheel-owned distribution member is not installed")
            with installed_location.open("rb") as stream:
                installed_digest, installed_size = digest_stream(stream)
            if installed_size != size or installed_digest.digest() != digest.digest():
                raise SystemExit("wheel-owned installed member digest mismatch")
        wheel_closure.update(member.encode("utf-8"))
        wheel_closure.update(b"\0")
        wheel_closure.update(digest.hexdigest().encode("ascii"))
        wheel_closure.update(b"\0")
        wheel_closure.update(str(size).encode("ascii"))
        wheel_closure.update(b"\n")
        wheel_members += 1

installed_record = installed_record_paths[0]
print(json.dumps({
    "schema_version": "clio-relay.python-record-closure.v1",
    "verified": True,
    "distribution": installed.name,
    "distribution_version": installed.version,
    "record_path": str(installed_record),
    "record_sha256": hashlib.sha256(installed_record.read_bytes()).hexdigest(),
    "installed_record_closure_sha256": installed_closure.hexdigest(),
    "installed_record_file_count": len(installed_files),
    "installed_record_bytes": installed_bytes,
    "wheel_payload_closure_sha256": wheel_closure.hexdigest(),
    "wheel_payload_file_count": wheel_members,
    "wheel_payload_bytes": wheel_bytes,
    "wheel_script_transform_count": len(wheel_script_transforms),
    "wheel_script_transforms": wheel_script_transforms,
    "tree_scanned": False,
    "tree_copied": False,
}, sort_keys=True))
"""
    try:
        completed = run_bounded_process(
            [python, "-I", "-c", script, distribution_name, str(expected_artifact)],
            timeout_seconds=60,
            stdout_maximum_bytes=64 * 1024,
            stderr_maximum_bytes=64 * 1024,
            environment=({**os.environ, **environment} if environment is not None else None),
        )
    except (OSError, BoundedProcessError) as exc:
        return {
            "verified": False,
            "error": f"{type(exc).__name__}: {exc}",
            "tree_scanned": False,
            "tree_copied": False,
        }
    if completed.returncode != 0:
        error = completed.stderr.strip() or completed.stdout.strip()
        return {
            "verified": False,
            "error": error,
            "error_code": _python_record_closure_error_code(error),
            "tree_scanned": False,
            "tree_copied": False,
        }
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {
            "verified": False,
            "error": f"invalid RECORD closure probe JSON: {exc}",
            "tree_scanned": False,
            "tree_copied": False,
        }
    typed_loaded = cast(dict[object, object], loaded) if isinstance(loaded, dict) else {}
    if not isinstance(loaded, dict) or typed_loaded.get("verified") is not True:
        return {
            "verified": False,
            "error": "distribution RECORD closure probe was not verified",
            "tree_scanned": False,
            "tree_copied": False,
        }
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}
