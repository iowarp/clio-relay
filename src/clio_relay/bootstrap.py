"""Autonomous installation helpers for desktop and cluster targets."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import shlex
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from email.message import Message
from email.parser import BytesParser
from email.policy import default
from importlib import resources
from pathlib import Path, PurePosixPath
from time import monotonic
from typing import cast
from urllib.request import urlretrieve
from uuid import uuid4

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import InvalidVersion, Version

from clio_relay import __version__
from clio_relay.bootstrap_reconcile import (
    LEGACY_MANAGED_JARVIS_REPO_PATH,
    MANAGED_JARVIS_REPO_PATH,
    BootstrapDesiredState,
    validate_jarvis_builtin_result,
)
from clio_relay.bounded_process import (
    BoundedProcessError,
    BoundedProcessOutputLimit,
    BoundedProcessTimeout,
    run_bounded_process,
)
from clio_relay.deployment import (
    endpoint_user_service_name,
    render_bounded_user_service_activation_helper,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.remote_values import render_remote_shell_path
from clio_relay.worker_lifetime_lock import (
    WORKER_LIFETIME_GUARD_FD_ENV,
    WORKER_LIFETIME_LOCK_NAME,
)

FRP_VERSION = "0.69.1"
FRP_WINDOWS_AMD64_SHA256 = "829ac915f8655d4d4e021b8db61b46c3445205ed80d32b04cda7fa89d87c46e0"
FRP_LINUX_AMD64_SHA256 = "7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1"
FRPC_LINUX_AMD64_SHA256 = "142f447f43fef286acc8da8a6852dda80631db631d604b2e63634b2db4d6848c"
FRPS_LINUX_AMD64_SHA256 = "68d2908bb73fe7a03c29d9227d2acc2104bff3fea6b1cece0b8388c1a0660442"
FRPC_WINDOWS_AMD64_SHA256 = "1d1c4f988b1808bb458a4ba38f00359052d14636023a504520e0afed127d636d"
FRPS_WINDOWS_AMD64_SHA256 = "bd463ef89370abc6973c86258256fa65776baa5f515ef91ebeabd6070b92e229"
UV_VERSION = "0.11.28"
UV_LINUX_AMD64_ARCHIVE_SHA256 = "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
UV_LINUX_AMD64_EXECUTABLE_SHA256 = (
    "1cb9cd0a1749debf6049d7d2bb933882cc52d81016326ee6d99a786d6c988b03"
)
JARVIS_UTIL_COMMIT = "c91bfdc9bba802e4b03bfb1babe614ffa3e09644"
JARVIS_CD_VERSION = "1.4.8"
JARVIS_CD_WHEEL_FILENAME = f"jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/{JARVIS_CD_WHEEL_FILENAME}"
)
JARVIS_CD_WHEEL_SHA256 = "ebf5e5f375b921f20c79075d461926431a5a017ca8b45e598878a89b229b3935"
DEFAULT_REMOTE_CORE_DIR = "$HOME/.local/share/clio-relay/core"
DEFAULT_REMOTE_SPOOL_DIR = "$HOME/.local/share/clio-relay/spool"


_STAGED_PROVIDER_ENVIRONMENT_SANITIZER = r"""
while IFS= read -r bootstrap_environment_name; do
  case "$bootstrap_environment_name" in
    LD_*|PYTHON*|BASH_ENV|ENV) unset "$bootstrap_environment_name" ;;
  esac
done < <(compgen -e)
""".strip()
_STAGED_PROVIDER_EXEC_PROGRAM = r"""
import ctypes
import fcntl
import hashlib
import json
import os
import shlex
import stat
import struct
import sys

MAX_STATE = 4 * 1024 * 1024
MAX_PROVIDER = 256 * 1024 * 1024
MAX_RUNTIME_LIBRARY = 512 * 1024 * 1024


def create_memfd(name, flags):
    creator = getattr(os, "memfd_create", None)
    if creator is not None:
        return creator(name, flags)
    library = ctypes.CDLL(None, use_errno=True)
    creator = library.memfd_create
    creator.argtypes = (ctypes.c_char_p, ctypes.c_uint)
    creator.restype = ctypes.c_int
    descriptor = creator(name.encode(), flags)
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def read_bounded(descriptor, maximum, label):
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode) or before.st_size < 1 or before.st_size > maximum:
        raise SystemExit(f"{label} is not one bounded regular file")
    chunks = []
    remaining = maximum + 1
    while remaining:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    after = os.fstat(descriptor)
    identity = lambda value: (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if (
        len(payload) != before.st_size
        or len(payload) > maximum
        or identity(before) != identity(after)
    ):
        raise SystemExit(f"{label} changed while it was read")
    return payload


def origin_dependency_relocations(payload):
    if payload[:4] != b"\x7fELF":
        return []
    if len(payload) < 64 or payload[4:6] != b"\x02\x01":
        raise SystemExit("staged provider is not a supported ELF64 executable")
    program_offset = struct.unpack_from("<Q", payload, 32)[0]
    program_entry_size = struct.unpack_from("<H", payload, 54)[0]
    program_count = struct.unpack_from("<H", payload, 56)[0]
    if (
        program_entry_size < 56
        or program_count < 1
        or program_offset + program_entry_size * program_count > len(payload)
    ):
        raise SystemExit("staged provider ELF program table is invalid")
    load_segments = []
    dynamic_segment = None
    for index in range(program_count):
        offset = program_offset + index * program_entry_size
        (
            program_type,
            _flags,
            file_offset,
            virtual_address,
            _physical_address,
            file_size,
            _memory_size,
            _alignment,
        ) = struct.unpack_from("<IIQQQQQQ", payload, offset)
        if file_offset + file_size > len(payload):
            raise SystemExit("staged provider ELF segment is out of bounds")
        if program_type == 1:
            load_segments.append((file_offset, virtual_address, file_size))
        elif program_type == 2:
            if dynamic_segment is not None:
                raise SystemExit("staged provider has multiple ELF dynamic segments")
            dynamic_segment = (file_offset, file_size)
    if dynamic_segment is None:
        return []
    dynamic_offset, dynamic_size = dynamic_segment
    if dynamic_size % 16:
        raise SystemExit("staged provider ELF dynamic segment is invalid")
    string_address = None
    string_size = None
    needed_offsets = []
    for offset in range(dynamic_offset, dynamic_offset + dynamic_size, 16):
        tag, value = struct.unpack_from("<qQ", payload, offset)
        if tag == 0:
            break
        if tag == 1:
            needed_offsets.append(value)
        elif tag == 5:
            string_address = value
        elif tag == 10:
            string_size = value
    if not needed_offsets:
        return []
    if string_address is None or string_size is None or string_size < 1:
        raise SystemExit("staged provider ELF string table is missing")
    string_candidates = [
        file_offset + string_address - virtual_address
        for file_offset, virtual_address, file_size in load_segments
        if virtual_address <= string_address
        and string_address + string_size <= virtual_address + file_size
    ]
    if len(string_candidates) != 1:
        raise SystemExit("staged provider ELF string table is ambiguous")
    string_offset = string_candidates[0]
    string_end = string_offset + string_size
    origin_prefix = b"$ORIGIN/../lib/"
    relocations = []
    for needed_offset in needed_offsets:
        start = string_offset + needed_offset
        if start < string_offset or start >= string_end:
            raise SystemExit("staged provider ELF dependency is out of bounds")
        end = payload.find(b"\0", start, string_end)
        if end < 0:
            raise SystemExit("staged provider ELF dependency is unterminated")
        dependency = payload[start:end]
        if not dependency.startswith(origin_prefix):
            continue
        library_name_bytes = dependency[len(origin_prefix) :]
        try:
            library_name = library_name_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            raise SystemExit("staged provider ELF origin dependency is invalid") from error
        if (
            not library_name
            or library_name != os.path.basename(library_name)
            or any(
                character
                not in (
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "0123456789._+-"
                )
                for character in library_name
            )
        ):
            raise SystemExit("staged provider ELF origin dependency is unsafe")
        relocations.append((start, len(dependency), library_name))
    return relocations


def sealed_memfd(name, payload, *, inheritable):
    flags = getattr(os, "MFD_ALLOW_SEALING", 2) | getattr(os, "MFD_EXEC", 0)
    if not inheritable:
        flags |= getattr(os, "MFD_CLOEXEC", 1)
    descriptor = create_memfd(name, flags)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise SystemExit(f"could not copy {name} into sealed memory")
            view = view[written:]
        os.fchmod(descriptor, 0o500)
        seals = (
            getattr(fcntl, "F_SEAL_WRITE", 0x0008)
            | getattr(fcntl, "F_SEAL_GROW", 0x0004)
            | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
            | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
        )
        add_seals = getattr(fcntl, "F_ADD_SEALS", 1033)
        get_seals = getattr(fcntl, "F_GET_SEALS", 1034)
        fcntl.fcntl(descriptor, add_seals, seals)
        if fcntl.fcntl(descriptor, get_seals) & seals != seals:
            raise SystemExit(f"{name} memfd did not seal")
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.set_inheritable(descriptor, inheritable)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


generation, expected_manifest_sha256, *provider_arguments = sys.argv[1:]
if len(expected_manifest_sha256) != 64 or any(
    character not in "0123456789abcdef" for character in expected_manifest_sha256
):
    raise SystemExit("staged manifest digest is invalid")
generation = os.path.abspath(generation)
generation_descriptor = os.open(
    generation,
    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
)
try:
    manifest_descriptor = os.open(
        "manifest.json",
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=generation_descriptor,
    )
    try:
        manifest_payload = read_bounded(manifest_descriptor, MAX_STATE, "staged manifest")
    finally:
        os.close(manifest_descriptor)
    if hashlib.sha256(manifest_payload).hexdigest() != expected_manifest_sha256:
        raise SystemExit("staged manifest changed before provider execution")
    manifest = json.loads(manifest_payload)
    receipt_path = os.path.join(generation, "install-receipt.json")
    if manifest.get("install_receipt") != receipt_path:
        raise SystemExit("staged receipt path is not bound to its manifest")
    receipt_descriptor = os.open(
        "install-receipt.json",
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=generation_descriptor,
    )
    try:
        receipt_payload = read_bounded(receipt_descriptor, MAX_STATE, "staged receipt")
    finally:
        os.close(receipt_descriptor)
    if manifest.get("install_receipt_sha256") != hashlib.sha256(receipt_payload).hexdigest():
        raise SystemExit("staged receipt is not bound to its manifest")
    receipt = json.loads(receipt_payload)
    component = receipt["component_artifacts"]["clio-relay"]
    persistent = component["persistent_tool"]
    provider = component["runtime_interpreters"]["provider"]
    relay = component["runtime_executables"]["clio-relay"]
    provider_sha256 = persistent["provider_interpreter_sha256"]
    relay_sha256 = persistent["tool_executable_sha256"]
    if provider != persistent["provider_interpreter"]:
        raise SystemExit("staged provider receipt paths disagree")
    expected_relay = os.path.join(generation, "bin", "clio-relay")
    if relay != persistent["tool_executable"] or relay != expected_relay:
        raise SystemExit("staged relay receipt paths disagree")
    provider_prefix = os.path.join(generation, "tools") + os.sep
    if (
        not isinstance(provider, str)
        or not provider.startswith(provider_prefix)
        or os.path.normpath(provider) != provider
        or any(character in provider for character in "\x00\r\n")
    ):
        raise SystemExit("staged provider path is outside its generation")
    for digest, label in (
        (provider_sha256, "provider"),
        (relay_sha256, "relay"),
    ):
        if not isinstance(digest, str) or len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise SystemExit(f"staged {label} digest is invalid")
    relay_descriptor = os.open(
        os.path.relpath(relay, generation),
        os.O_RDONLY,
        dir_fd=generation_descriptor,
    )
    try:
        relay_payload = read_bounded(relay_descriptor, MAX_STATE, "staged relay launcher")
    finally:
        os.close(relay_descriptor)
    if hashlib.sha256(relay_payload).hexdigest() != relay_sha256:
        raise SystemExit("staged relay launcher digest changed")
    launcher_lines = relay_payload.splitlines()
    direct_shebang = bool(
        launcher_lines and launcher_lines[0] == ("#!" + provider).encode("utf-8")
    )
    quoted_provider = {
        shlex.quote(provider),
        "'" + provider.replace("'", "'\"'\"'") + "'",
    }
    trampoline_lines = {
        "'''exec' " + value + ' "$0" "$@"' for value in quoted_provider
    }
    try:
        trampoline_command = (
            launcher_lines[1].decode("utf-8") if len(launcher_lines) >= 2 else ""
        )
    except UnicodeDecodeError as exc:
        raise SystemExit("staged relay launcher is not valid UTF-8") from exc
    uv_shell_trampoline = bool(
        len(launcher_lines) >= 3
        and launcher_lines[0] == b"#!/bin/sh"
        and trampoline_command in trampoline_lines
        and launcher_lines[2] == b"' '''"
    )
    if not direct_shebang and not uv_shell_trampoline:
        raise SystemExit("staged relay launcher is not bound to its provider")
    provider_descriptor = os.open(
        os.path.relpath(provider, generation),
        os.O_RDONLY,
        dir_fd=generation_descriptor,
    )
    try:
        provider_details = os.fstat(provider_descriptor)
        if provider_details.st_mode & 0o111 == 0:
            raise SystemExit("staged provider is not executable")
        descriptor_path = f"/proc/self/fd/{provider_descriptor}"
        source_provider = os.path.realpath(descriptor_path)
        if (
            not source_provider
            or source_provider.endswith(" (deleted)")
            or not os.path.isfile(source_provider)
        ):
            raise SystemExit("staged provider backing path is unavailable")
        source_details = os.stat(source_provider, follow_symlinks=False)
        if (source_details.st_dev, source_details.st_ino) != (
            provider_details.st_dev,
            provider_details.st_ino,
        ):
            raise SystemExit("staged provider backing path changed")
        provider_library = os.path.normpath(
            os.path.join(os.path.dirname(source_provider), "..", "lib")
        )
        runtime_library_memfds = []
        try:
            provider_payload = read_bounded(
                provider_descriptor,
                MAX_PROVIDER,
                "staged provider",
            )
            if hashlib.sha256(provider_payload).hexdigest() != provider_sha256:
                raise SystemExit("staged provider digest changed")
            relocations = origin_dependency_relocations(provider_payload)
            relocated_provider = bytearray(provider_payload)
            library_memfds_by_name = {}
            if relocations:
                try:
                    library_directory_descriptor = os.open(
                        provider_library,
                        os.O_RDONLY
                        | os.O_DIRECTORY
                        | getattr(os, "O_NOFOLLOW", 0),
                    )
                except OSError as error:
                    raise SystemExit(
                        "staged provider origin library directory is unavailable"
                    ) from error
                try:
                    for _start, _size, library_name in relocations:
                        if library_name in library_memfds_by_name:
                            continue
                        try:
                            library_descriptor = os.open(
                                library_name,
                                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                                dir_fd=library_directory_descriptor,
                            )
                        except OSError as error:
                            raise SystemExit(
                                f"staged provider origin dependency is unavailable: {library_name}"
                            ) from error
                        try:
                            library_payload = read_bounded(
                                library_descriptor,
                                MAX_RUNTIME_LIBRARY,
                                f"staged provider origin dependency {library_name}",
                            )
                        finally:
                            os.close(library_descriptor)
                        library_memfd = sealed_memfd(
                            f"clio-relay-{library_name}",
                            library_payload,
                            inheritable=True,
                        )
                        runtime_library_memfds.append(library_memfd)
                        library_memfds_by_name[library_name] = library_memfd
                finally:
                    os.close(library_directory_descriptor)
                for start, size, library_name in relocations:
                    replacement = (
                        f"/proc/self/fd/{library_memfds_by_name[library_name]}".encode()
                    )
                    if len(replacement) > size:
                        raise SystemExit(
                            "staged provider origin dependency fd path is too long"
                        )
                    relocated_provider[start : start + size] = replacement + b"\0" * (
                        size - len(replacement)
                    )
            provider_memfd = sealed_memfd(
                "clio-relay-staged-provider",
                relocated_provider,
                inheritable=False,
            )
            try:
                if os.execve not in os.supports_fd:
                    raise SystemExit("provider fd execution is unavailable")
                provider_environment = {
                    name: value
                    for name, value in os.environ.items()
                    if not name.startswith("LD_") and not name.startswith("PYTHON")
                }
                os.execve(
                    provider_memfd,
                    [provider, "-I", *provider_arguments],
                    provider_environment,
                )
            finally:
                os.close(provider_memfd)
        finally:
            for runtime_library_memfd in runtime_library_memfds:
                os.close(runtime_library_memfd)
    finally:
        os.close(provider_descriptor)
finally:
    os.close(generation_descriptor)
"""
MAX_RELAY_WHEEL_METADATA_BYTES = 1024 * 1024
_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE = r"""import json
import os
import stat
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


path = Path(sys.argv[1])
home = Path.home()
stable_details = path.lstat()
stable_target = None
current = None
current_details = None
current_target_text = None
if stat.S_ISLNK(stable_details.st_mode):
    stable_target = os.readlink(path)
    expected_target = str(home / ".local/share/clio-relay/current/install-receipt.json")
    if stable_target != expected_target:
        raise SystemExit("bootstrap install receipt link has an unsupported target")
    current = home / ".local/share/clio-relay/current"
    current_details = current.lstat()
    if not stat.S_ISLNK(current_details.st_mode):
        raise SystemExit("bootstrap current generation pointer is not a symbolic link")
    current_target_text = os.readlink(current)
    if not current_target_text or any(character in current_target_text for character in "\x00\r\n"):
        raise SystemExit("bootstrap current generation target is invalid")
    current_target = Path(current_target_text)
    if not current_target.is_absolute():
        current_target = current.parent / current_target
    if ".." in current_target.parts:
        raise SystemExit("bootstrap current generation target is not normalized")
    generations = (home / ".local/share/clio-relay/generations").resolve(strict=True)
    resolved_generation = current_target.resolve(strict=True)
    try:
        relative_generation = resolved_generation.relative_to(generations)
    except ValueError as exc:
        raise SystemExit("bootstrap current pointer escaped managed generations") from exc
    if (
        len(relative_generation.parts) != 1
        or len(relative_generation.name) != 64
        or any(character not in "0123456789abcdef" for character in relative_generation.name)
    ):
        raise SystemExit("bootstrap current pointer has an invalid generation identity")
    generation_details = current_target.lstat()
    if current_target.is_symlink() or not stat.S_ISDIR(generation_details.st_mode):
        raise SystemExit("bootstrap current pointer target is not one real generation")
    read_path = resolved_generation / "install-receipt.json"
    details = read_path.lstat()
elif stat.S_ISREG(stable_details.st_mode):
    read_path = path
    details = stable_details
else:
    raise SystemExit("bootstrap install receipt has an unsupported file type")
if read_path.is_symlink() or not stat.S_ISREG(details.st_mode):
    raise SystemExit("bootstrap install receipt target is not one regular file")
if not 1 <= details.st_size <= 4 * 1024 * 1024:
    raise SystemExit("bootstrap install receipt size is invalid")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(read_path, flags)
try:
    opened = os.fstat(descriptor)
    if identity(opened) != identity(details):
        raise SystemExit("bootstrap install receipt changed before reading")
    with os.fdopen(descriptor, "rb", closefd=False) as stream:
        payload = stream.read(4 * 1024 * 1024 + 1)
    after = os.fstat(descriptor)
finally:
    os.close(descriptor)
if (
    len(payload) > 4 * 1024 * 1024
    or identity(after) != identity(opened)
    or identity(read_path.lstat()) != identity(details)
    or identity(path.lstat()) != identity(stable_details)
):
    raise SystemExit("bootstrap install receipt changed while reading")
if stable_target is not None:
    assert current is not None
    assert current_details is not None
    assert current_target_text is not None
    if (
        os.readlink(path) != stable_target
        or identity(current.lstat()) != identity(current_details)
        or os.readlink(current) != current_target_text
    ):
        raise SystemExit("bootstrap generation links changed while reading the receipt")
value = json.loads(payload)
if not isinstance(value, dict):
    raise SystemExit("bootstrap install receipt is not an object")
artifacts = value.get("component_artifacts")
relay = artifacts.get("clio-relay") if isinstance(artifacts, dict) else None
print(
    "current"
    if isinstance(relay, dict) and relay.get("persistent_tool") is not None
    else "legacy"
)"""
_BOOTSTRAP_PREPARING_ROOT_SOURCE = r"""import os
import stat
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


def object_identity(details):
    return (details.st_dev, details.st_ino, details.st_mode, details.st_uid)


def owned_private_directory(details, label):
    if not stat.S_ISDIR(details.st_mode):
        raise SystemExit(f"{label} is not one real directory")
    if hasattr(os, "getuid") and details.st_uid != os.getuid():
        raise SystemExit(f"{label} is not owned by the current user")
    if stat.S_IMODE(details.st_mode) & 0o077:
        raise SystemExit(f"{label} is not owner-private")


def entry_details(parent_descriptor, name):
    try:
        return os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None


def remove_entry(parent_descriptor, name):
    details = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    if not stat.S_ISDIR(details.st_mode):
        if not (stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode)):
            raise SystemExit("bootstrap scratch contains an unsupported entry")
        os.unlink(name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
        return
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    try:
        opened = os.fstat(descriptor)
        if identity(opened) != identity(details):
            raise SystemExit("bootstrap scratch entry changed before pinned cleanup")
        for child in os.listdir(descriptor):
            if child in {"", ".", ".."} or "/" in child or "\x00" in child:
                raise SystemExit("bootstrap scratch contains an invalid child name")
            remove_entry(descriptor, child)
        if object_identity(os.fstat(descriptor)) != object_identity(opened):
            raise SystemExit("bootstrap scratch directory changed during pinned cleanup")
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_descriptor)
    os.fsync(parent_descriptor)


def remove_owned_root(parent_descriptor, name):
    details = entry_details(parent_descriptor, name)
    if details is None:
        return
    owned_private_directory(details, "bootstrap scratch quarantine")
    remove_entry(parent_descriptor, name)


parent = Path(sys.argv[1])
root = Path(sys.argv[2])
action = sys.argv[3]
if action not in {"prepare", "cleanup"}:
    raise SystemExit("bootstrap scratch action is invalid")
if not parent.is_absolute() or root.parent != parent or root.name != "active":
    raise SystemExit("bootstrap scratch path escaped its fixed private parent")
parent_before = parent.lstat()
owned_private_directory(parent_before, "bootstrap preparing parent")
flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
flags |= getattr(os, "O_NOFOLLOW", 0)
parent_descriptor = os.open(parent, flags)
try:
    parent_opened = os.fstat(parent_descriptor)
    if identity(parent_opened) != identity(parent_before):
        raise SystemExit("bootstrap preparing parent changed before it was pinned")
    quarantine = ".active.quarantine"
    remove_owned_root(parent_descriptor, quarantine)
    active = entry_details(parent_descriptor, root.name)
    if active is not None:
        owned_private_directory(active, "bootstrap preparing root")
        os.rename(
            root.name,
            quarantine,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
        os.fsync(parent_descriptor)
        moved = os.stat(quarantine, dir_fd=parent_descriptor, follow_symlinks=False)
        if object_identity(moved) != object_identity(active):
            raise SystemExit("bootstrap preparing root changed during quarantine")
        remove_owned_root(parent_descriptor, quarantine)
    if action == "prepare":
        os.mkdir(root.name, mode=0o700, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
        created = os.stat(root.name, dir_fd=parent_descriptor, follow_symlinks=False)
        owned_private_directory(created, "bootstrap preparing root")
    parent_after = parent.lstat()
    if object_identity(parent_after) != object_identity(parent_opened):
        raise SystemExit("bootstrap preparing parent changed while it was pinned")
finally:
    os.close(parent_descriptor)"""
_BOOTSTRAP_PINNED_UV_COPY_SOURCE = r"""import hashlib
import os
import stat
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


def object_identity(details):
    return (details.st_dev, details.st_ino, details.st_mode, details.st_uid)


source = Path(sys.argv[1])
root = Path(sys.argv[2])
expected_sha256 = sys.argv[3]
if (
    not source.is_absolute()
    or not root.is_absolute()
    or len(expected_sha256) != 64
    or any(character not in "0123456789abcdef" for character in expected_sha256)
):
    raise SystemExit("candidate uv copy arguments are invalid")
source_before = source.lstat()
root_before = root.lstat()
if (
    not stat.S_ISREG(source_before.st_mode)
    or source_before.st_nlink != 1
    or source_before.st_mode & 0o111 == 0
    or not 1 <= source_before.st_size <= 256 * 1024 * 1024
    or (hasattr(os, "getuid") and source_before.st_uid != os.getuid())
    or stat.S_IMODE(source_before.st_mode) & 0o022
):
    raise SystemExit("candidate uv source is not one private bounded executable")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
source_descriptor = os.open(source, flags)
directory_flags = flags | os.O_DIRECTORY
root_descriptor = os.open(root, directory_flags)
destination_descriptor = None
destination_created = False
try:
    source_opened = os.fstat(source_descriptor)
    root_opened = os.fstat(root_descriptor)
    if identity(source_opened) != identity(source_before):
        raise SystemExit("candidate uv source changed before its pinned copy")
    if object_identity(root_opened) != object_identity(root_before) or (
        not stat.S_ISDIR(root_opened.st_mode)
        or (hasattr(os, "getuid") and root_opened.st_uid != os.getuid())
        or stat.S_IMODE(root_opened.st_mode) & 0o077
    ):
        raise SystemExit("candidate uv destination root is not owner-private")
    destination_flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    destination_descriptor = os.open(
        "pinned-uv",
        destination_flags,
        0o500,
        dir_fd=root_descriptor,
    )
    destination_created = True
    digest = hashlib.sha256()
    copied = 0
    while chunk := os.read(source_descriptor, 1024 * 1024):
        copied += len(chunk)
        if copied > 256 * 1024 * 1024:
            raise SystemExit("candidate uv source exceeded its copy bound")
        digest.update(chunk)
        view = memoryview(chunk)
        while view:
            written = os.write(destination_descriptor, view)
            if written < 1:
                raise SystemExit("candidate uv copy made no progress")
            view = view[written:]
    os.fchmod(destination_descriptor, 0o500)
    os.fsync(destination_descriptor)
    destination_written = os.fstat(destination_descriptor)
    source_after = os.fstat(source_descriptor)
    source_linked_after = source.lstat()
    if (
        copied != source_opened.st_size
        or digest.hexdigest() != expected_sha256
        or identity(source_after) != identity(source_opened)
        or identity(source_linked_after) != identity(source_opened)
        or destination_written.st_size != copied
        or destination_written.st_nlink != 1
        or stat.S_IMODE(destination_written.st_mode) != 0o500
        or (destination_written.st_dev, destination_written.st_ino)
        == (source_opened.st_dev, source_opened.st_ino)
    ):
        raise SystemExit("candidate uv source changed or did not match its release pin")
    os.close(destination_descriptor)
    destination_descriptor = None
    verification_descriptor = os.open("pinned-uv", flags, dir_fd=root_descriptor)
    try:
        verification_opened = os.fstat(verification_descriptor)
        if identity(verification_opened) != identity(destination_written):
            raise SystemExit("candidate uv private copy changed before verification")
        verified_digest = hashlib.sha256()
        verified_size = 0
        while chunk := os.read(verification_descriptor, 1024 * 1024):
            verified_size += len(chunk)
            verified_digest.update(chunk)
        verification_after = os.fstat(verification_descriptor)
    finally:
        os.close(verification_descriptor)
    linked_copy = os.stat("pinned-uv", dir_fd=root_descriptor, follow_symlinks=False)
    if (
        verified_size != copied
        or verified_digest.hexdigest() != expected_sha256
        or identity(verification_after) != identity(verification_opened)
        or identity(linked_copy) != identity(verification_opened)
    ):
        raise SystemExit("candidate uv private copy did not retain its pinned identity")
    os.fsync(root_descriptor)
    if object_identity(root.lstat()) != object_identity(root_opened):
        raise SystemExit("candidate uv destination root changed while it was pinned")
except BaseException:
    if destination_descriptor is not None:
        os.close(destination_descriptor)
    if destination_created:
        try:
            os.unlink("pinned-uv", dir_fd=root_descriptor)
            os.fsync(root_descriptor)
        except FileNotFoundError:
            pass
    raise
finally:
    os.close(root_descriptor)
    os.close(source_descriptor)
print(root / "pinned-uv")"""
_BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE = r"""import base64
import ctypes
import csv
import fcntl
import hashlib
import json
import os
import signal
import stat
import struct
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath


MAX_PROVIDER = 256 * 1024 * 1024
MAX_RUNTIME_LIBRARY = 512 * 1024 * 1024


def create_memfd(name, flags):
    creator = getattr(os, "memfd_create", None)
    if creator is not None:
        return creator(name, flags)
    library = ctypes.CDLL(None, use_errno=True)
    creator = library.memfd_create
    creator.argtypes = (ctypes.c_char_p, ctypes.c_uint)
    creator.restype = ctypes.c_int
    descriptor = creator(name.encode(), flags)
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def origin_dependency_relocations(payload):
    if payload[:4] != b"\x7fELF":
        return []
    if len(payload) < 64 or payload[4:6] != b"\x02\x01":
        raise SystemExit("candidate provider is not a supported ELF64 executable")
    program_offset = struct.unpack_from("<Q", payload, 32)[0]
    program_entry_size = struct.unpack_from("<H", payload, 54)[0]
    program_count = struct.unpack_from("<H", payload, 56)[0]
    if (
        program_entry_size < 56
        or program_count < 1
        or program_offset + program_entry_size * program_count > len(payload)
    ):
        raise SystemExit("candidate provider ELF program table is invalid")
    load_segments = []
    dynamic_segment = None
    for index in range(program_count):
        offset = program_offset + index * program_entry_size
        (
            program_type,
            _flags,
            file_offset,
            virtual_address,
            _physical_address,
            file_size,
            _memory_size,
            _alignment,
        ) = struct.unpack_from("<IIQQQQQQ", payload, offset)
        if file_offset + file_size > len(payload):
            raise SystemExit("candidate provider ELF segment is out of bounds")
        if program_type == 1:
            load_segments.append((file_offset, virtual_address, file_size))
        elif program_type == 2:
            if dynamic_segment is not None:
                raise SystemExit("candidate provider has multiple ELF dynamic segments")
            dynamic_segment = (file_offset, file_size)
    if dynamic_segment is None:
        return []
    dynamic_offset, dynamic_size = dynamic_segment
    if dynamic_size % 16:
        raise SystemExit("candidate provider ELF dynamic segment is invalid")
    string_address = None
    string_size = None
    needed_offsets = []
    for offset in range(dynamic_offset, dynamic_offset + dynamic_size, 16):
        tag, value = struct.unpack_from("<qQ", payload, offset)
        if tag == 0:
            break
        if tag == 1:
            needed_offsets.append(value)
        elif tag == 5:
            string_address = value
        elif tag == 10:
            string_size = value
    if not needed_offsets:
        return []
    if string_address is None or string_size is None or string_size < 1:
        raise SystemExit("candidate provider ELF string table is missing")
    string_candidates = [
        file_offset + string_address - virtual_address
        for file_offset, virtual_address, file_size in load_segments
        if virtual_address <= string_address
        and string_address + string_size <= virtual_address + file_size
    ]
    if len(string_candidates) != 1:
        raise SystemExit("candidate provider ELF string table is ambiguous")
    string_offset = string_candidates[0]
    string_end = string_offset + string_size
    origin_prefix = b"$ORIGIN/../lib/"
    relocations = []
    for needed_offset in needed_offsets:
        start = string_offset + needed_offset
        if start < string_offset or start >= string_end:
            raise SystemExit("candidate provider ELF dependency is out of bounds")
        end = payload.find(b"\0", start, string_end)
        if end < 0:
            raise SystemExit("candidate provider ELF dependency is unterminated")
        dependency = payload[start:end]
        if not dependency.startswith(origin_prefix):
            continue
        library_name_bytes = dependency[len(origin_prefix) :]
        try:
            library_name = library_name_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            raise SystemExit("candidate provider ELF origin dependency is invalid") from error
        if (
            not library_name
            or library_name != os.path.basename(library_name)
            or any(
                character
                not in (
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "0123456789._+-"
                )
                for character in library_name
            )
        ):
            raise SystemExit("candidate provider ELF origin dependency is unsafe")
        relocations.append((start, len(dependency), library_name))
    return relocations


def sealed_memfd(name, payload, *, inheritable):
    flags = getattr(os, "MFD_ALLOW_SEALING", 2) | getattr(os, "MFD_EXEC", 0)
    if not inheritable:
        flags |= getattr(os, "MFD_CLOEXEC", 1)
    descriptor = create_memfd(name, flags)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise SystemExit(f"could not copy {name} into sealed memory")
            view = view[written:]
        os.fchmod(descriptor, 0o500)
        seals = (
            getattr(fcntl, "F_SEAL_WRITE", 0x0008)
            | getattr(fcntl, "F_SEAL_GROW", 0x0004)
            | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
            | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
        )
        add_seals = getattr(fcntl, "F_ADD_SEALS", 1033)
        get_seals = getattr(fcntl, "F_GET_SEALS", 1034)
        fcntl.fcntl(descriptor, add_seals, seals)
        if fcntl.fcntl(descriptor, get_seals) & seals != seals:
            raise SystemExit(f"{name} memfd did not seal")
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.set_inheritable(descriptor, inheritable)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def exec_candidate_provider(
    provider,
    source_provider,
    provider_payload,
    provider_arguments,
    environment,
):
    relocations = origin_dependency_relocations(provider_payload)
    relocated_provider = bytearray(provider_payload)
    runtime_library_memfds = []
    library_memfds_by_name = {}
    try:
        if relocations:
            provider_library = os.path.normpath(
                os.path.join(os.path.dirname(source_provider), "..", "lib")
            )
            library_directory_descriptor = os.open(
                provider_library,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                for _start, _size, library_name in relocations:
                    if library_name in library_memfds_by_name:
                        continue
                    library_descriptor = os.open(
                        library_name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=library_directory_descriptor,
                    )
                    try:
                        library_payload, _details = read_descriptor(
                            library_descriptor,
                            MAX_RUNTIME_LIBRARY,
                            f"candidate provider origin dependency {library_name}",
                        )
                    finally:
                        os.close(library_descriptor)
                    library_memfd = sealed_memfd(
                        f"clio-relay-{library_name}",
                        library_payload,
                        inheritable=True,
                    )
                    runtime_library_memfds.append(library_memfd)
                    library_memfds_by_name[library_name] = library_memfd
            finally:
                os.close(library_directory_descriptor)
            for start, size, library_name in relocations:
                replacement = f"/proc/self/fd/{library_memfds_by_name[library_name]}".encode()
                if len(replacement) > size:
                    raise SystemExit("candidate provider origin dependency fd path is too long")
                relocated_provider[start : start + size] = replacement + b"\0" * (
                    size - len(replacement)
                )
        provider_memfd = sealed_memfd(
            "clio-relay-candidate-provider",
            relocated_provider,
            inheritable=False,
        )
        try:
            if os.execve not in os.supports_fd:
                raise SystemExit("candidate provider fd execution is unavailable")
            environment["BOOTSTRAP_PLAN_PROVIDER_EXEC_SHA256"] = hashlib.sha256(
                relocated_provider
            ).hexdigest()
            os.execve(provider_memfd, [str(provider), *provider_arguments], environment)
        finally:
            os.close(provider_memfd)
    finally:
        for runtime_library_memfd in runtime_library_memfds:
            os.close(runtime_library_memfd)


def identity(details):
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def read_descriptor(descriptor, maximum, label):
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= maximum:
        raise SystemExit(f"{label} is not one bounded regular file")
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    remaining = maximum + 1
    while remaining:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    after = os.fstat(descriptor)
    if (
        len(payload) != before.st_size
        or len(payload) > maximum
        or identity(after) != identity(before)
    ):
        raise SystemExit(f"{label} changed while it was pinned")
    return payload, before


def read_path(path, maximum, label):
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        payload, opened = read_descriptor(descriptor, maximum, label)
    finally:
        os.close(descriptor)
    if identity(opened) != identity(before) or identity(path.lstat()) != identity(opened):
        raise SystemExit(f"{label} path changed while it was pinned")
    return payload


def read_path_allow_empty(path, maximum, label):
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or not 0 <= opened.st_size <= maximum:
            raise SystemExit(f"{label} is not one bounded regular file")
        chunks = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        len(payload) != opened.st_size
        or len(payload) > maximum
        or identity(before) != identity(opened)
        or identity(after) != identity(opened)
        or identity(path.lstat()) != identity(opened)
    ):
        raise SystemExit(f"{label} changed while it was pinned")
    return payload


action, *values = sys.argv[1:]
if action not in {
    "install-and-verify",
    "install-verify-and-exec",
    "verify-installed",
    "verify-installed-and-exec",
}:
    raise SystemExit("candidate uv installation action is invalid")
if len(values) < 8:
    raise SystemExit("candidate uv installation arguments are incomplete")
identity_values = values[:8]
remaining_values = values[8:]
expected_provider_sha256 = None
if action == "verify-installed-and-exec":
    if not remaining_values:
        raise SystemExit("candidate provider execution omitted its expected digest")
    expected_provider_sha256, *provider_arguments = remaining_values
    if len(expected_provider_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in expected_provider_sha256
    ):
        raise SystemExit("candidate provider expected digest is invalid")
else:
    provider_arguments = remaining_values
if action.endswith("-and-exec"):
    if not provider_arguments or provider_arguments[0] != "-I":
        raise SystemExit("candidate provider execution must be isolated")
elif provider_arguments:
    raise SystemExit("candidate uv verification received unexpected arguments")
(
    uv_value,
    expected_uv_sha256,
    wheel_value,
    expected_wheel_sha256,
    tool_directory_value,
    tool_bin_directory_value,
    cache_directory_value,
    python_install_directory_value,
) = identity_values
if os.name != "posix" or os.execve not in os.supports_fd:
    raise SystemExit("candidate uv installation requires POSIX fd execution")
for digest, label in (
    (expected_uv_sha256, "uv"),
    (expected_wheel_sha256, "wheel"),
):
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise SystemExit(f"candidate {label} digest is invalid")
uv_path = Path(uv_value)
wheel_path = Path(wheel_value)
tool_directory = Path(tool_directory_value)
tool_bin_directory = Path(tool_bin_directory_value)
cache_directory = Path(cache_directory_value)
python_install_directory = Path(python_install_directory_value)
if any(
    not path.is_absolute()
    for path in (
        uv_path,
        wheel_path,
        tool_directory,
        tool_bin_directory,
        cache_directory,
        python_install_directory,
    )
):
    raise SystemExit("candidate uv installation paths must be absolute")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
uv_before = uv_path.lstat()
wheel_before = wheel_path.lstat()
uv_descriptor = os.open(uv_path, flags)
wheel_descriptor = os.open(wheel_path, flags)
provider_descriptor = None
try:
    uv_payload, uv_opened = read_descriptor(
        uv_descriptor,
        256 * 1024 * 1024,
        "candidate uv executable",
    )
    wheel_payload, wheel_opened = read_descriptor(
        wheel_descriptor,
        256 * 1024 * 1024,
        "candidate relay wheel",
    )
    if (
        identity(uv_before) != identity(uv_opened)
        or identity(wheel_before) != identity(wheel_opened)
        or hashlib.sha256(uv_payload).hexdigest() != expected_uv_sha256
        or hashlib.sha256(wheel_payload).hexdigest() != expected_wheel_sha256
    ):
        raise SystemExit("candidate uv or wheel changed before fd-bound installation")
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("LD_", "PYTHON", "UV_", "PIP_"))
        and name not in {"BASH_ENV", "ENV", "VIRTUAL_ENV", "CONDA_PREFIX"}
    }
    environment.update(
        {
            "UV_TOOL_DIR": str(tool_directory),
            "UV_TOOL_BIN_DIR": str(tool_bin_directory),
            "UV_CACHE_DIR": str(cache_directory),
            "UV_PYTHON_INSTALL_DIR": str(python_install_directory),
            "UV_PYTHON_DOWNLOADS": "never",
        }
    )
    if action in {"install-and-verify", "install-verify-and-exec"}:
        command = [
            str(uv_path),
            "tool",
            "install",
            "--force",
            "--python",
            "3.12",
            "--no-config",
            "--default-index",
            "https://pypi.org/simple",
            str(wheel_path),
        ]
        process = subprocess.Popen(
            command,
            executable=f"/proc/self/fd/{uv_descriptor}",
            pass_fds=(uv_descriptor,),
            env=environment,
            start_new_session=True,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
        try:
            returncode = process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            raise SystemExit("candidate fd-bound uv installation timed out") from None
        if returncode != 0:
            raise SystemExit(f"candidate fd-bound uv installation failed: {returncode}")

    provider_location = tool_directory / "clio-relay/bin/python"
    try:
        provider_target = provider_location.resolve(strict=True)
        provider_before = provider_target.lstat()
        provider_descriptor = os.open(
            provider_target,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except (OSError, RuntimeError) as error:
        raise SystemExit("candidate provider target is unavailable") from error

    stream = os.fdopen(os.dup(wheel_descriptor), "rb")
    with stream, zipfile.ZipFile(stream) as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos if not item.is_dir()]
        if not names or len(names) > 100_000 or len(names) != len(set(names)):
            raise SystemExit("candidate wheel has an invalid bounded member set")
        for item in infos:
            path = PurePosixPath(item.filename)
            mode = item.external_attr >> 16
            if (
                path.is_absolute()
                or ".." in path.parts
                or any(part in {"", "."} for part in path.parts)
                or (mode and stat.S_IFMT(mode) not in {0, stat.S_IFREG})
                or item.file_size > 256 * 1024 * 1024
            ):
                raise SystemExit("candidate wheel contains an unsafe member")
        record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(record_names) != 1:
            raise SystemExit("candidate wheel RECORD ownership is ambiguous")
        record_name = record_names[0]
        record_bytes = archive.read(record_name)
        if len(record_bytes) > 8 * 1024 * 1024:
            raise SystemExit("candidate wheel RECORD exceeds its byte bound")
        try:
            rows = list(csv.reader(record_bytes.decode("utf-8").splitlines(), strict=True))
        except (UnicodeDecodeError, csv.Error) as error:
            raise SystemExit("candidate wheel RECORD is malformed") from error
        wheel_rows = {row[0]: row for row in rows if len(row) == 3}
        if set(wheel_rows) != set(names) or len(wheel_rows) != len(rows):
            raise SystemExit("candidate wheel RECORD does not close over its members")

        environment_prefix = (tool_directory / "clio-relay").resolve(strict=True)
        site_package_matches = list(environment_prefix.glob("lib/python*/site-packages"))
        if len(site_package_matches) != 1:
            raise SystemExit("candidate uv environment has no exact site-packages root")
        site_packages = site_package_matches[0].resolve(strict=True)
        dist_info = PurePosixPath(record_name).parent
        installed_record = site_packages.joinpath(*PurePosixPath(record_name).parts)
        if installed_record.is_symlink() or not installed_record.resolve(
            strict=True
        ).is_relative_to(site_packages):
            raise SystemExit("installed candidate RECORD escaped site-packages")
        installed_record_bytes = read_path(
            installed_record,
            8 * 1024 * 1024,
            "installed candidate RECORD",
        )
        try:
            installed_rows = list(
                csv.reader(installed_record_bytes.decode("utf-8").splitlines(), strict=True)
            )
        except (UnicodeDecodeError, csv.Error) as error:
            raise SystemExit("installed candidate RECORD is malformed") from error
        installed_names = {row[0] for row in installed_rows if len(row) == 3}
        launcher_name = os.path.relpath(
            environment_prefix / "bin/clio-relay", site_packages
        ).replace(os.sep, "/")
        required_generated = {
            launcher_name,
            str(dist_info / "INSTALLER"),
            str(dist_info / "REQUESTED"),
            str(dist_info / "direct_url.json"),
        }
        optional_generated = {
            str(dist_info / "uv_build.json"),
            str(dist_info / "uv_cache.json"),
        }
        wheel_names = set(names)
        required_names = wheel_names | required_generated
        allowed_names = required_names | optional_generated
        missing_names = required_names - installed_names
        unexpected_names = installed_names - allowed_names
        if (
            len(installed_names) != len(installed_rows)
            or missing_names
            or unexpected_names
        ):
            details = {
                "row_count": len(installed_rows),
                "unique_name_count": len(installed_names),
                "missing_count": len(missing_names),
                "missing": [name[:256] for name in sorted(missing_names)[:16]],
                "unexpected_count": len(unexpected_names),
                "unexpected": [name[:256] for name in sorted(unexpected_names)[:16]],
            }
            raise SystemExit(
                "installed candidate distribution contains unpinned members: "
                + json.dumps(details, sort_keys=True, separators=(",", ":"))
            )
        installed_row_map = {row[0]: row for row in installed_rows}
        generated_payloads = {}
        for name in installed_names - wheel_names:
            row = installed_row_map[name]
            generated_path = site_packages.joinpath(*PurePosixPath(name).parts)
            if generated_path.is_symlink() or not generated_path.resolve(
                strict=True
            ).is_relative_to(environment_prefix):
                raise SystemExit("installed candidate generated member escaped its environment")
            payload = read_path_allow_empty(
                generated_path,
                8 * 1024 * 1024,
                "installed candidate generated member",
            )
            expected_hash, expected_size_text = row[1:]
            digest = hashlib.sha256(payload).digest()
            encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
            if (
                expected_hash != "sha256=" + encoded
                or not expected_size_text.isdigit()
                or int(expected_size_text) != len(payload)
            ):
                raise SystemExit(
                    "installed candidate generated member differs from its RECORD identity"
                )
            generated_payloads[name] = payload
        if (
            generated_payloads[str(dist_info / "INSTALLER")] != b"uv"
            or generated_payloads[str(dist_info / "REQUESTED")] != b""
        ):
            raise SystemExit("installed candidate generated ownership metadata is invalid")
        for name in (
            str(dist_info / "direct_url.json"),
            *(sorted(optional_generated & installed_names)),
        ):
            try:
                document = json.loads(generated_payloads[name])
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise SystemExit("installed candidate generated JSON is invalid") from error
            if not isinstance(document, dict):
                raise SystemExit("installed candidate generated JSON must contain an object")

        total = 0
        for name in names:
            row = wheel_rows[name]
            installed_path = site_packages.joinpath(*PurePosixPath(name).parts)
            if name == record_name:
                continue
            if installed_path.is_symlink() or not installed_path.resolve(
                strict=True
            ).is_relative_to(site_packages):
                raise SystemExit("installed candidate member escaped site-packages")
            expected_hash, expected_size_text = row[1:]
            if not expected_hash.startswith("sha256=") or not expected_size_text.isdigit():
                raise SystemExit("candidate wheel RECORD member omitted its identity")
            expected_size = int(expected_size_text)
            total += expected_size
            if total > 2 * 1024 * 1024 * 1024:
                raise SystemExit("candidate wheel expanded closure exceeds its byte bound")
            wheel_member = archive.read(name)
            installed_member = read_path(
                installed_path,
                256 * 1024 * 1024,
                "installed candidate member",
            )
            digest = hashlib.sha256(wheel_member).digest()
            encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
            if (
                len(wheel_member) != expected_size
                or expected_hash != "sha256=" + encoded
                or installed_member != wheel_member
            ):
                raise SystemExit("installed candidate differs from the pinned wheel fd")
    if (
        identity(os.fstat(uv_descriptor)) != identity(uv_opened)
        or identity(os.fstat(wheel_descriptor)) != identity(wheel_opened)
    ):
        raise SystemExit("candidate uv or wheel descriptor changed during installation")
    assert provider_descriptor is not None
    try:
        provider_payload, provider_opened = read_descriptor(
            provider_descriptor,
            256 * 1024 * 1024,
            "candidate provider",
        )
        source_provider = os.path.realpath(f"/proc/self/fd/{provider_descriptor}")
        source_details = os.stat(source_provider, follow_symlinks=False)
        if (
            identity(provider_before) != identity(provider_opened)
            or provider_location.resolve(strict=True) != provider_target
            or identity(provider_target.lstat()) != identity(provider_opened)
            or (source_details.st_dev, source_details.st_ino)
            != (provider_opened.st_dev, provider_opened.st_ino)
            or provider_opened.st_mode & 0o111 == 0
        ):
            raise SystemExit("candidate provider path changed while it was pinned")
        provider_sha256 = hashlib.sha256(provider_payload).hexdigest()
        if (
            expected_provider_sha256 is not None
            and provider_sha256 != expected_provider_sha256
        ):
            raise SystemExit("candidate provider changed after its planning pin")
    finally:
        os.close(provider_descriptor)
        provider_descriptor = None
finally:
    if provider_descriptor is not None:
        os.close(provider_descriptor)
    os.close(wheel_descriptor)
    os.close(uv_descriptor)
if action.endswith("-and-exec"):
    provider_environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("LD_", "PYTHON")) and name not in {"BASH_ENV", "ENV"}
    }
    provider_environment["BOOTSTRAP_PLAN_PROVIDER"] = str(provider_location)
    provider_environment["BOOTSTRAP_PLAN_PROVIDER_SHA256"] = provider_sha256
    exec_candidate_provider(
        provider_location,
        source_provider,
        provider_payload,
        provider_arguments,
        provider_environment,
    )
print("bootstrap_candidate_provider_sha256=" + provider_sha256)
print("bootstrap_candidate_install=fd-bound-wheel-verified:" + action)"""
BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS = 1800.0
BOOTSTRAP_PUBLIC_EXACT_DEADLINE_SECONDS = 29.0
BOOTSTRAP_PUBLIC_REPAIR_DEADLINE_SECONDS = 58.0

_BOOTSTRAP_CANDIDATE_PACKAGE_OVERLAY = (
    b"\nfrom importlib import metadata as _clio_relay_metadata\n"
    b"from pkgutil import extend_path\n\n"
    b"__path__ = extend_path(__path__, __name__)\n"
    b"try:\n"
    b"    __version__ = _clio_relay_metadata.version('clio-relay')\n"
    b"except _clio_relay_metadata.PackageNotFoundError:\n"
    b"    pass\n"
)
_BOOTSTRAP_CANDIDATE_SOURCE_NAMES = (
    "bootstrap_provider_build_info.py",
    "bootstrap_reconcile.py",
    "bounded_process.py",
    "errors.py",
    "process_containment.py",
    "safe_archive.py",
)


def _bootstrap_candidate_package_sources() -> dict[str, bytes]:
    """Return the exact sources overlaid during candidate reconciliation."""
    package_root = Path(__file__).parent
    sources = {
        "__init__.py": (package_root / "__init__.py").read_bytes()
        + _BOOTSTRAP_CANDIDATE_PACKAGE_OVERLAY
    }
    for name in _BOOTSTRAP_CANDIDATE_SOURCE_NAMES:
        sources[name] = (package_root / name).read_bytes()
    return sources


_WORKER_WRITER_PROOF_PYTHON = r'''from __future__ import annotations

import errno
import json
import os
import posixpath
import socket
import stat
import sys
from pathlib import Path

MAX_PROC_ENTRIES = 1_000_000
MAX_OWNED_PROCESSES = 65_536
MAX_PROC_FILE_BYTES = 1_048_576
MAX_ENDPOINT_RECORDS = 10_000
MAX_ENDPOINT_TOTAL_BYTES = 64 * 1_048_576


def fail(message: str) -> "NoReturn":
    """Stop the bootstrap because writer exclusion could not be proved."""
    raise SystemExit(f"relay worker writer proof failed: {message}")


def vanished(error: OSError) -> bool:
    """Return whether a proc entry disappeared during inspection."""
    return error.errno in {errno.ENOENT, errno.ESRCH}


def read_bounded(path: Path) -> bytes | None:
    """Read one proc pseudo-file without accepting an unbounded value."""
    try:
        with path.open("rb") as stream:
            value = stream.read(MAX_PROC_FILE_BYTES + 1)
    except OSError as error:
        if vanished(error):
            return None
        fail(f"cannot inspect {path}: {error}")
    if len(value) > MAX_PROC_FILE_BYTES:
        fail(f"{path} exceeds the bounded inspection size")
    return value


def decode_nul_values(value: bytes) -> list[str]:
    """Decode an exact NUL-delimited proc value with filesystem semantics."""
    return [os.fsdecode(part) for part in value.split(b"\0") if part]


def relay_process_invocation(argv: list[str]) -> list[str] | None:
    """Return command arguments for one exact installed relay invocation."""
    for index, argument in enumerate(argv):
        if os.path.basename(argument) == "clio-relay":
            return argv[index + 1 :]
    return None


def option_value(arguments: list[str], name: str) -> str | None:
    """Return the last exact Click-style option value before an option terminator."""
    found: str | None = None
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--":
            break
        if argument == name:
            if index + 1 >= len(arguments):
                return None
            found = arguments[index + 1]
            index += 2
            continue
        prefix = f"{name}="
        if argument.startswith(prefix):
            found = argument[len(prefix) :]
        index += 1
    return found


def environment(value: bytes) -> dict[str, str]:
    """Parse the process environment without substring or shell matching."""
    parsed: dict[str, str] = {}
    for item in value.split(b"\0"):
        if b"=" not in item:
            continue
        key, raw_value = item.split(b"=", 1)
        parsed[os.fsdecode(key)] = os.fsdecode(raw_value)
    return parsed


def process_cwd(process: Path) -> str | None:
    """Read a process working directory, accounting for an ordinary exit race."""
    try:
        return os.readlink(process / "cwd")
    except OSError as error:
        if vanished(error):
            return None
        fail(f"cannot inspect {process / 'cwd'}: {error}")


def target_home(process_environment: dict[str, str], uid: int | None) -> str:
    """Resolve Path.home() as the inspected process would resolve it."""
    if "HOME" in process_environment:
        # posixpath.expanduser maps an explicitly empty HOME to the filesystem
        # root for both '~' and '~/...'; it does not fall back to passwd.
        return process_environment["HOME"].rstrip("/") or "/"
    if uid is None:
        fail("an inspected process has no HOME and no numeric uid")
    try:
        import pwd

        return pwd.getpwuid(uid).pw_dir
    except (ImportError, KeyError, OSError) as error:
        fail(f"cannot resolve the inspected process home directory: {error}")


def path_is_absolute(value: str) -> bool:
    """Recognize target Linux roots even when the proof is tested on Windows."""
    return value.startswith("/") or os.path.isabs(value)


def path_join(base: str, *parts: str) -> str:
    """Join target Linux paths without inheriting a Windows test host's flavor."""
    if base.startswith("/"):
        return posixpath.join(base, *parts)
    return os.path.join(base, *parts)


def expand_user(value: str, home: str) -> str:
    """Expand a user path with the inspected process's HOME semantics."""
    if value == "~":
        return home
    if value.startswith("~/"):
        return path_join(home, value[2:])
    if not value.startswith("~"):
        return value
    user, separator, suffix = value[1:].partition("/")
    try:
        import pwd

        user_home = pwd.getpwnam(user).pw_dir
    except (ImportError, KeyError, OSError) as error:
        fail(f"cannot expand inspected core directory {value!r}: {error}")
    return path_join(user_home, suffix) if separator else user_home


def canonical(value: str, *, cwd: str | None = None) -> str:
    """Return a non-strict canonical absolute path."""
    if not path_is_absolute(value):
        if cwd is None:
            fail(f"relative path {value!r} has no inspected working directory")
        value = path_join(cwd, value)
    if os.name == "nt" and value.startswith("/"):
        # The embedded proof runs on Linux in production.  Python 3.13 changed
        # ntpath.isabs('/core') to false, so use the target path flavor when
        # exercising the exact source in the Windows CI matrix.
        return posixpath.normpath(value)
    return os.path.realpath(os.path.abspath(value))


def process_core_candidates(
    process: Path,
    process_environment: dict[str, str],
    uid: int | None,
) -> set[str] | None:
    """Reconstruct every core path the live endpoint could have selected at startup."""
    home = target_home(process_environment, uid)
    configured = process_environment.get("CLIO_RELAY_CORE_DIR")
    if configured:
        expanded = expand_user(configured, home)
        cwd = None if path_is_absolute(expanded) else process_cwd(process)
        if cwd is None and not path_is_absolute(expanded):
            return None
        return {canonical(expanded, cwd=cwd)}

    cwd = process_cwd(process)
    if cwd is None:
        return None
    # RelaySettings selects the bootstrap directory when it exists, otherwise
    # its cwd-relative compatibility directory.  /proc cannot prove which one
    # existed at process startup, so both are safety-relevant candidates.
    return {
        canonical(path_join(home, ".local", "share", "clio-relay", "core")),
        canonical(path_join(".clio-relay", "core"), cwd=cwd),
    }


def endpoint_record_pids(
    expected_core: str,
) -> dict[int, list[tuple[str, dict[str, object] | None]]]:
    """Read bounded worker PID evidence from the exact core's endpoint records."""
    endpoint_directory = Path(expected_core) / "endpoints"
    try:
        directory_stat = os.lstat(endpoint_directory)
    except FileNotFoundError:
        return {}
    except OSError as error:
        fail(f"cannot inspect endpoint evidence directory {endpoint_directory}: {error}")
    if not stat.S_ISDIR(directory_stat.st_mode) or stat.S_ISLNK(directory_stat.st_mode):
        fail(f"endpoint evidence path is not a real directory: {endpoint_directory}")
    current_uid = os.getuid() if hasattr(os, "getuid") else None
    if current_uid is not None and directory_stat.st_uid != current_uid:
        fail(f"endpoint evidence directory has a foreign owner: {endpoint_directory}")

    records: dict[int, list[tuple[str, dict[str, object] | None]]] = {}
    record_count = 0
    total_bytes = 0
    try:
        entries = os.scandir(endpoint_directory)
    except OSError as error:
        fail(f"cannot enumerate endpoint evidence {endpoint_directory}: {error}")
    with entries:
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            record_count += 1
            if record_count > MAX_ENDPOINT_RECORDS:
                fail("endpoint evidence exceeds the bounded record count")
            path = Path(entry.path)
            try:
                before = os.lstat(path)
            except OSError as error:
                if vanished(error):
                    fail(f"endpoint evidence changed during inspection: {path}")
                fail(f"cannot inspect endpoint evidence {path}: {error}")
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or (current_uid is not None and before.st_uid != current_uid)
            ):
                fail(f"endpoint evidence is not one owned regular file: {path}")
            value = read_bounded(path)
            if value is None:
                fail(f"endpoint evidence disappeared during inspection: {path}")
            total_bytes += len(value)
            if total_bytes > MAX_ENDPOINT_TOTAL_BYTES:
                fail("endpoint evidence exceeds the bounded aggregate size")
            try:
                after = os.lstat(path)
            except OSError as error:
                fail(f"endpoint evidence changed during inspection: {path}: {error}")
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
            ):
                fail(f"endpoint evidence changed during inspection: {path}")
            try:
                document = json.loads(value)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                fail(f"endpoint evidence is not valid JSON: {path}: {error}")
            if not isinstance(document, dict):
                fail(f"endpoint evidence is not an object: {path}")
            endpoint_id = document.get("endpoint_id")
            role = document.get("role")
            hostname = document.get("hostname")
            pid = document.get("pid")
            cluster = document.get("cluster")
            metadata = document.get("metadata")
            if not isinstance(endpoint_id, str) or path.stem != endpoint_id:
                fail(f"endpoint evidence identity does not match its filename: {path}")
            if role != "worker":
                continue
            if hostname != socket.gethostname():
                continue
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                fail(f"worker endpoint evidence has an invalid pid: {path}")
            if not isinstance(cluster, str) or not cluster:
                fail(f"worker endpoint evidence has no cluster: {path}")
            process_identity: dict[str, object] | None = None
            raw_identity = metadata.get("process_identity") if isinstance(metadata, dict) else None
            if isinstance(raw_identity, dict):
                identity_start_ticks = raw_identity.get("start_ticks")
                identity_uid = raw_identity.get("uid")
                identity_pid = raw_identity.get("pid")
                if (
                    raw_identity.get("schema_version") == "clio-relay.process-identity.v1"
                    and isinstance(raw_identity.get("boot_id"), str)
                    and bool(raw_identity["boot_id"])
                    and len(raw_identity["boot_id"]) <= 128
                    and isinstance(identity_start_ticks, int)
                    and not isinstance(identity_start_ticks, bool)
                    and identity_start_ticks > 0
                    and isinstance(identity_uid, int)
                    and not isinstance(identity_uid, bool)
                    and identity_uid >= 0
                    and isinstance(identity_pid, int)
                    and not isinstance(identity_pid, bool)
                    and identity_pid == pid
                ):
                    process_identity = raw_identity
                # A malformed identity is deliberately treated as legacy PID
                # evidence.  Only a complete exact identity may dismiss PID
                # reuse; malformed metadata can never weaken writer proof.
            records.setdefault(pid, []).append((cluster, process_identity))
    return records


def proc_boot_id(proc_root: Path) -> str:
    """Read the exact Linux boot identity used by new endpoint records."""
    value = read_bounded(proc_root / "sys" / "kernel" / "random" / "boot_id")
    if value is None:
        fail("cannot read Linux boot identity for endpoint evidence")
    try:
        boot_id = value.decode("ascii").strip()
    except UnicodeDecodeError as error:
        fail(f"Linux boot identity is invalid: {error}")
    if not boot_id or len(boot_id) > 128:
        fail("Linux boot identity is empty or oversized")
    return boot_id


def process_identity_matches(
    raw_stat: bytes,
    *,
    process_pid: int,
    process_uid: int,
    boot_id: str,
    identity: dict[str, object] | None,
) -> bool:
    """Match new exact identities; conservatively retain legacy PID evidence."""
    closing_parenthesis = raw_stat.rfind(b")")
    fields = raw_stat[closing_parenthesis + 1 :].split()
    if closing_parenthesis < 0 or len(fields) <= 19:
        fail("cannot parse live endpoint process generation")
    if fields[0] == b"Z":
        return False
    if identity is None:
        return True
    try:
        start_ticks = int(fields[19])
    except ValueError as error:
        fail(f"cannot parse live endpoint start ticks: {error}")
    return (
        identity.get("schema_version") == "clio-relay.process-identity.v1"
        and identity.get("boot_id") == boot_id
        and identity.get("start_ticks") == start_ticks
        and identity.get("uid") == process_uid
        and identity.get("pid") == process_pid
    )


def prove_no_writer(cluster: str, expected_core: str, proc_root: Path) -> None:
    """Fail if a same-user long-lived process can write the configured core queue."""
    expected = canonical(expected_core)
    current_uid = os.getuid() if hasattr(os, "getuid") else None
    endpoint_pids = endpoint_record_pids(expected)
    boot_id = proc_boot_id(proc_root) if endpoint_pids else ""
    total_entries = 0
    owned_processes = 0
    try:
        entries = os.scandir(proc_root)
    except OSError as error:
        fail(f"cannot enumerate {proc_root}: {error}")
    with entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            total_entries += 1
            if total_entries > MAX_PROC_ENTRIES:
                fail(f"{proc_root} exceeds the bounded process-entry count")
            try:
                process_uid = entry.stat(follow_symlinks=False).st_uid
            except OSError as error:
                if vanished(error):
                    continue
                fail(f"cannot identify process owner for {entry.path}: {error}")
            if current_uid is not None and process_uid != current_uid:
                continue
            owned_processes += 1
            if owned_processes > MAX_OWNED_PROCESSES:
                fail("same-user process count exceeds the bounded inspection limit")
            process = Path(entry.path)
            endpoint_evidence = endpoint_pids.get(int(entry.name))
            if endpoint_evidence is not None:
                raw_stat = read_bounded(process / "stat")
                if raw_stat is None:
                    continue
                for endpoint_cluster, process_identity in endpoint_evidence:
                    if process_identity_matches(
                        raw_stat,
                        process_pid=int(entry.name),
                        process_uid=process_uid,
                        boot_id=boot_id,
                        identity=process_identity,
                    ):
                        fail(
                            f"live endpoint pid={entry.name} has exact-core record "
                            f"cluster={endpoint_cluster!r} while bootstrapping cluster={cluster!r}"
                        )
            raw_cmdline = read_bounded(process / "cmdline")
            if raw_cmdline is None:
                continue
            argv = decode_nul_values(raw_cmdline)
            command = relay_process_invocation(argv)
            if command is None:
                continue
            writer_kind = "clio-relay"
            options = command
            if command[:2] == ["endpoint", "start"]:
                writer_kind = "endpoint"
                options = command[2:]
            elif command[:2] == ["api", "start"]:
                writer_kind = "api"
                options = command[2:]
            elif command[:1] == ["mcp-server"]:
                writer_kind = "mcp-server"
                options = command[1:]
            process_cluster = option_value(options, "--cluster")
            raw_environment = read_bounded(process / "environ")
            if raw_environment is None:
                continue
            candidates = process_core_candidates(
                process,
                environment(raw_environment),
                process_uid,
            )
            if candidates is None:
                continue
            if expected in candidates:
                if (
                    writer_kind == "endpoint"
                    and option_value(options, "--role") == "worker"
                    and process_cluster is not None
                ):
                    fail(
                        f"live endpoint pid={entry.name} still owns "
                        f"cluster={process_cluster!r} core={expected!r} "
                        f"while bootstrapping cluster={cluster!r}"
                    )
                if writer_kind in {"api", "mcp-server"}:
                    fail(
                        f"live {writer_kind} writer pid={entry.name} still owns "
                        f"core={expected!r} while bootstrapping cluster={cluster!r}; "
                        "stop or detach it before bootstrap"
                    )
                fail(
                    f"live clio-relay process pid={entry.name} still owns "
                    f"core={expected!r} while bootstrapping cluster={cluster!r}; "
                    "wait for it to exit before bootstrap"
                )
    print("relay_worker_writer_proof=clear")


if len(sys.argv) != 4:
    fail("writer proof requires cluster, canonical core, and proc root")
prove_no_writer(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
'''

_WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON = r'''from __future__ import annotations

import errno
import os
import stat
import sys
import time
from pathlib import Path


def fail(message: str) -> "NoReturn":
    """Fail inherited-FD validation with one operator-facing reason."""
    raise SystemExit(f"worker lifetime inherited-fd proof failed: {message}")


if len(sys.argv) != 4:
    fail("proof requires canonical core, inherited fd, and lock filename")
core_value, descriptor_value, lock_name = sys.argv[1:]
try:
    import fcntl

    descriptor = int(descriptor_value)
    if descriptor < 3:
        fail("inherited descriptor is invalid")
    core = Path(core_value)
    core = core.resolve(strict=True)
    core_stat = os.lstat(core)
    if not stat.S_ISDIR(core_stat.st_mode) or stat.S_ISLNK(core_stat.st_mode):
        fail("worker lifetime core is not a real directory")
    if core_stat.st_uid != os.getuid():
        fail("worker lifetime core has a foreign owner")
    if stat.S_IMODE(core_stat.st_mode) & 0o022:
        fail("worker lifetime core is writable by group or other users")

    lock_path = core / lock_name
    opened = os.fstat(descriptor)
    linked = os.lstat(lock_path)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) & 0o077
        or opened.st_dev != linked.st_dev
        or opened.st_ino != linked.st_ino
    ):
        fail("worker lifetime lock is not one owner-private regular file")

    deadline = time.monotonic() + 30.0
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError as lock_error:
            if lock_error.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            if time.monotonic() >= deadline:
                fail("timed out acquiring exclusive worker lifetime lock")
            time.sleep(0.05)
    print(f"relay_worker_lifetime_fd=exclusive:{descriptor}:{core}")
except Exception as error:
    fail(f"{type(error).__name__}: {error}")
'''


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
        if not _is_sha256_value(relay_artifact_sha256):
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
    if not _is_sha256_value(relay_artifact_sha256):
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


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    destination.mkdir(parents=True, exist_ok=True)
    frpc = destination / "frpc.exe"
    frps = destination / "frps.exe"
    if frpc.exists() and frps.exists():
        try:
            _assert_frp_pair(frpc, frps)
            return frpc
        except ConfigurationError:
            pass
    cleanup_errors = _remove_local_frp_pair(frpc, frps)
    if cleanup_errors:
        raise ConfigurationError(
            "could not remove an unverified existing frp installation: " + "; ".join(cleanup_errors)
        )
    try:
        with tempfile.TemporaryDirectory(
            prefix=".clio-relay-frp-",
            dir=destination.parent,
        ) as temporary_directory:
            staging = Path(temporary_directory) / "bin"
            staging.mkdir()
            _install_frp_from_release_archive(staging, FRP_VERSION)
            staged_frpc = staging / "frpc.exe"
            staged_frps = staging / "frps.exe"
            _assert_frp_pair(staged_frpc, staged_frps)
            shutil.copy2(staged_frpc, frpc)
            shutil.copy2(staged_frps, frps)
            _assert_frp_pair(frpc, frps)
        _assert_frp_pair(frpc, frps)
        return frpc
    except (ConfigurationError, OSError) as exc:
        cleanup_errors = _remove_local_frp_pair(frpc, frps)
        cleanup_detail = (
            ""
            if not cleanup_errors
            else "; unverified destination cleanup failed: " + "; ".join(cleanup_errors)
        )
        raise ConfigurationError(
            f"failed to install verified frp release: {exc}{cleanup_detail}"
        ) from exc


def _remove_local_frp_pair(frpc: Path, frps: Path) -> list[str]:
    """Remove both local frp executables and return any cleanup errors."""
    errors: list[str] = []
    for path in (frpc, frps):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    return errors


def _bootstrap_preflight_over_ssh(
    *,
    ssh_host: str,
    invocation_id: str,
    desired: BootstrapDesiredState,
    core_dir: str,
    spool_dir: str,
    repair: bool,
    timeout_seconds: float,
) -> BootstrapPreflightResult:
    """Ask an installed relay to verify/repair exact state without a payload."""
    if timeout_seconds <= 2:
        raise RelayError("bootstrap preflight has no remaining public deadline")
    remote_timeout = max(1, min(55 if repair else 24, int(timeout_seconds - 1)))
    encoded = base64.b64encode(
        json.dumps(
            desired.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).decode("ascii")
    command = "\n".join(
        [
            "set -u",
            *_STAGED_PROVIDER_ENVIRONMENT_SANITIZER.splitlines(),
            f"export CLIO_RELAY_CORE_DIR={render_remote_shell_path(core_dir, field='core_dir')}",
            f"export CLIO_RELAY_SPOOL_DIR={render_remote_shell_path(spool_dir, field='spool_dir')}",
            ("export CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64=" + shlex.quote(encoded)),
            'if [ ! -x "$HOME/.local/bin/clio-relay" ]; then '
            "echo bootstrap_preflight_unsupported=not_installed; exit 0; fi",
            'BOOTSTRAP_INSTALL_RECEIPT="$HOME/.local/share/clio-relay/install-receipt.json"',
            'if [ -e "$BOOTSTRAP_INSTALL_RECEIPT" ] || [ -L "$BOOTSTRAP_INSTALL_RECEIPT" ]; then',
            '  if ! BOOTSTRAP_RELAY_RECEIPT_CLASS="$(python3 -I - '
            "\"$BOOTSTRAP_INSTALL_RECEIPT\" <<'__CLIO_RELAY_PREFLIGHT_RECEIPT__'",
            *_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE.splitlines(),
            "__CLIO_RELAY_PREFLIGHT_RECEIPT__",
            '  )"; then',
            "    echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "    exit 0",
            "  fi",
            '  if [ "$BOOTSTRAP_RELAY_RECEIPT_CLASS" != "current" ]; then',
            "    echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "    exit 0",
            "  fi",
            "else",
            "  echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "  exit 0",
            "fi",
            "if ! command -v timeout >/dev/null 2>&1; then",
            '  echo "timeout is required" >&2',
            "  exit 1",
            "fi",
            "set +e",
            (
                'BOOTSTRAP_PREFLIGHT_OUTPUT="$(timeout --signal=TERM --kill-after=2s '
                f"{remote_timeout}s "
                '"$HOME/.local/bin/clio-relay" '
                f"bootstrap-inspect --invocation-id {shlex.quote(invocation_id)} "
                + ("--repair " if repair else "--inspect-only ")
                + '2>&1)"'
            ),
            "BOOTSTRAP_PREFLIGHT_STATUS=$?",
            "set -e",
            'if [ "$BOOTSTRAP_PREFLIGHT_STATUS" -ne 0 ]; then',
            "  if printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\" | "
            "grep -Eqi "
            "'no such command.*bootstrap-inspect|bootstrap-inspect.*no such command'; then",
            "    echo bootstrap_preflight_unsupported=missing_command",
            "    exit 0",
            "  fi",
            '  if BOOTSTRAP_PREFLIGHT_OUTPUT="$BOOTSTRAP_PREFLIGHT_OUTPUT" '
            "python3 -I - <<'__CLIO_RELAY_REPAIRABLE_QUEUE_ROOT__'",
            "import json",
            "import os",
            "import sys",
            "from pathlib import Path",
            "",
            'prefix = "error: "',
            "matches = [line.removeprefix(prefix) for line in "
            'os.environ["BOOTSTRAP_PREFLIGHT_OUTPUT"].splitlines() '
            "if line.startswith(prefix)]",
            "if len(matches) != 1:",
            "    raise SystemExit(1)",
            "try:",
            "    report = json.loads(matches[0])",
            "except json.JSONDecodeError:",
            "    raise SystemExit(1) from None",
            "expected = {",
            '    "schema_version": "clio-relay.legacy-state-audit.v1",',
            '    "family": "root",',
            '    "reason": "queue directory is readable or writable by another user",',
            '    "action": (',
            '        "move the unsafe state aside or export records with portable durable IDs "',
            '        "before retrying"',
            "    ),",
            "}",
            'if not isinstance(report, dict) or set(report) != {*expected, "path"}:',
            "    raise SystemExit(1)",
            "if any(report.get(name) != value for name, value in expected.items()):",
            "    raise SystemExit(1)",
            'path = report.get("path")',
            "if not isinstance(path, str):",
            "    raise SystemExit(1)",
            "try:",
            "    observed = Path(path).resolve(strict=True)",
            '    configured = Path(os.environ["CLIO_RELAY_CORE_DIR"]).resolve(strict=True)',
            "except OSError:",
            "    raise SystemExit(1) from None",
            "if observed != configured:",
            "    raise SystemExit(1)",
            "__CLIO_RELAY_REPAIRABLE_QUEUE_ROOT__",
            "  then",
            "    echo bootstrap_preflight_unsupported=repairable_queue_permissions",
            "    exit 0",
            "  fi",
            "  printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\" >&2",
            '  exit "$BOOTSTRAP_PREFLIGHT_STATUS"',
            "fi",
            "printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\"",
        ]
    )
    result = _run(
        ["ssh", ssh_host, "bash", "-c", command],
        timeout_seconds=timeout_seconds,
    )
    lines = result.stdout.splitlines()
    payload_lines = [
        line.removeprefix("bootstrap_preflight_json=")
        for line in lines
        if line.startswith("bootstrap_preflight_json=")
    ]
    if not payload_lines:
        unsupported = [
            line
            for line in lines
            if line
            in {
                "bootstrap_preflight_unsupported=not_installed",
                "bootstrap_preflight_unsupported=missing_command",
                "bootstrap_preflight_unsupported=legacy_relay_provider",
                "bootstrap_preflight_unsupported=repairable_queue_permissions",
            }
        ]
        if len(unsupported) != 1:
            raise RelayError("bootstrap preflight returned no supported inspector evidence")
        return BootstrapPreflightResult(action="payload_required", receipt=None, lines=lines)
    if len(payload_lines) != 1 or len(payload_lines[0].encode()) > 1024 * 1024:
        raise RelayError("bootstrap preflight returned invalid bounded evidence")
    try:
        raw = cast(object, json.loads(payload_lines[0]))
    except json.JSONDecodeError as exc:
        raise RelayError("bootstrap preflight returned invalid JSON") from exc
    if not isinstance(raw, dict):
        raise RelayError("bootstrap preflight did not return an object")
    payload = cast(dict[str, object], raw)
    if (
        payload.get("schema_version") != "clio-relay.bootstrap-preflight.v1"
        or payload.get("desired_fingerprint") != desired.fingerprint
        or not isinstance(payload.get("exact_match"), bool)
    ):
        raise RelayError("bootstrap preflight identity did not match the request")
    if payload.get("exact_match") is not True:
        action = payload.get("action")
        if (
            action not in {"payload_required", "repair_required"}
            or payload.get("receipt") is not None
        ):
            raise RelayError("bootstrap preflight returned ambiguous non-exact action evidence")
        if repair and action == "repair_required":
            raise RelayError("explicit bootstrap repair returned another repair request")
        return BootstrapPreflightResult(action=cast(str, action), receipt=None, lines=lines)
    raw_receipt = payload.get("receipt")
    if not isinstance(raw_receipt, dict):
        raise RelayError("successful bootstrap preflight omitted its receipt")
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("invocation_id") != invocation_id:
        raise RelayError("bootstrap preflight receipt invocation changed")
    return BootstrapPreflightResult(action="exact", receipt=receipt, lines=lines)


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
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_after != identity_before:
        raise ConfigurationError("bootstrap payload changed while hashing")
    return digest.hexdigest()


def _install_frp_from_release_archive(destination: Path, version: str) -> None:
    if version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Windows checksum is registered for frp {version}")
    archive = destination.parent / f"frp_{version}_windows_amd64.zip"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{version}/frp_{version}_windows_amd64.zip"
    )
    urlretrieve(url, archive)
    observed = hashlib.sha256(archive.read_bytes()).hexdigest()
    if observed != FRP_WINDOWS_AMD64_SHA256:
        raise ConfigurationError(
            f"frp archive SHA-256 mismatch: {observed} != {FRP_WINDOWS_AMD64_SHA256}"
        )
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(destination.parent)
    extracted = destination.parent / f"frp_{version}_windows_amd64"
    shutil.copy2(extracted / "frpc.exe", destination / "frpc.exe")
    shutil.copy2(extracted / "frps.exe", destination / "frps.exe")


def bootstrap_cluster_over_ssh(
    *,
    bootstrap_profile: str,
    ssh_host: str,
    source_root: Path,
    cluster: str | None = None,
    core_dir: str = DEFAULT_REMOTE_CORE_DIR,
    spool_dir: str = DEFAULT_REMOTE_SPOOL_DIR,
    relay_wheel: Path | None = None,
    relay_artifact_sha256: str | None = None,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    frp_version: str = FRP_VERSION,
    jarvis_resource_graph_profile: str | None = None,
    allow_jarvis_resource_graph_build: bool = False,
) -> list[str]:
    """Install relay dependencies and the current source tree on a cluster over SSH."""
    public_started = monotonic()
    if bootstrap_profile != "linux-user":
        raise ConfigurationError(f"unsupported bootstrap profile: {bootstrap_profile}")
    if cluster is not None:
        endpoint_user_service_name(cluster)
    render_remote_shell_path(core_dir, field="core_dir")
    render_remote_shell_path(spool_dir, field="spool_dir")
    _validate_ssh_destination(ssh_host)
    expected_jarvis_mcp_spec = os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC",
        CLIO_KIT_JARVIS_MCP_WHEEL_URL,
    )
    expected_jarvis_mcp_sha256 = os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_ARTIFACT_SHA256",
        (
            CLIO_KIT_JARVIS_MCP_WHEEL_SHA256
            if expected_jarvis_mcp_spec == CLIO_KIT_JARVIS_MCP_WHEEL_URL
            else ""
        ),
    )
    if not _is_sha256_value(expected_jarvis_mcp_sha256):
        raise ConfigurationError("clio-kit bootstrap source requires its expected SHA-256")
    planned_identity = bootstrap_relay_identity(
        source_root=source_root,
        relay_wheel=relay_wheel,
        relay_artifact_sha256=relay_artifact_sha256,
    )
    if shutil.which("ssh") is None or shutil.which("scp") is None:
        raise ConfigurationError("ssh and scp are required for remote bootstrap")
    expected_desired_state = _bootstrap_desired_state(
        identity=planned_identity,
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        frp_version=frp_version,
        clio_kit_install_spec=expected_jarvis_mcp_spec,
        clio_kit_artifact_sha256=expected_jarvis_mcp_sha256,
        agent_adapter=agent_adapter,
        agent_npm_package=agent_npm_package,
        agent_npm_bin=agent_npm_bin,
        agent_args=agent_args or [],
        jarvis_resource_graph_profile=jarvis_resource_graph_profile,
        allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
    )
    invocation_id = f"bootstrap_{uuid4().hex}"
    exact_deadline = public_started + BOOTSTRAP_PUBLIC_EXACT_DEADLINE_SECONDS
    repair_deadline = public_started + BOOTSTRAP_PUBLIC_REPAIR_DEADLINE_SECONDS
    preflight = _bootstrap_preflight_over_ssh(
        ssh_host=ssh_host,
        invocation_id=invocation_id,
        desired=expected_desired_state,
        core_dir=core_dir,
        spool_dir=spool_dir,
        repair=False,
        timeout_seconds=_remaining_public_deadline(exact_deadline, action="inspection"),
    )
    preflight_lines = list(preflight.lines)
    receipt_deadline = exact_deadline
    if preflight.action == "repair_required":
        repaired = _bootstrap_preflight_over_ssh(
            ssh_host=ssh_host,
            invocation_id=invocation_id,
            desired=expected_desired_state,
            core_dir=core_dir,
            spool_dir=spool_dir,
            repair=True,
            timeout_seconds=_remaining_public_deadline(repair_deadline, action="repair"),
        )
        preflight_lines.extend(repaired.lines)
        preflight = repaired
        receipt_deadline = repair_deadline
    preflight_receipt = preflight.receipt
    if preflight_receipt is not None:
        if (relay_wheel is not None or _is_clio_relay_git_checkout(source_root)) and (
            bootstrap_relay_identity(
                source_root=source_root,
                relay_wheel=relay_wheel,
                relay_artifact_sha256=relay_artifact_sha256,
            )
            != planned_identity
        ):
            raise ConfigurationError("bootstrap source identity changed during preflight")
        _validate_bootstrap_receipt(
            preflight_receipt,
            bootstrap_profile=bootstrap_profile,
            relay_install_spec=planned_identity.install_spec,
            desired_fingerprint=expected_desired_state.fingerprint,
            expected_jarvis_resource_graph_profile=(
                expected_desired_state.jarvis_resource_graph_profile
            ),
            expected_allow_jarvis_resource_graph_build=(
                expected_desired_state.allow_jarvis_resource_graph_build
            ),
            expected_worker_service=(
                endpoint_user_service_name(cluster) if cluster is not None else None
            ),
        )
        _verify_persistent_bootstrap_receipt(
            ssh_host=ssh_host,
            receipt=preflight_receipt,
            timeout_seconds=_remaining_public_deadline(
                receipt_deadline,
                action="persistent receipt verification",
            ),
        )
        return [
            *preflight_lines,
            "bootstrap_receipt=$HOME/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json="
            + json.dumps(preflight_receipt, sort_keys=True, separators=(",", ":")),
        ]

    if jarvis_resource_graph_profile is None:
        raise ConfigurationError(
            "cluster bootstrap requires an operator-selected "
            "jarvis_resource_graph_profile before payload reconciliation"
        )

    if relay_wheel is not None:
        observed_relay_sha256 = _validate_relay_bootstrap_wheel(relay_wheel)
        if relay_artifact_sha256 != observed_relay_sha256:
            raise ConfigurationError("relay bootstrap wheel SHA-256 does not match its pin")
    remote_root = f"/tmp/clio-relay-{invocation_id}"
    remote_archive = f"{remote_root}/clio-relay-head.tar"
    remote_script = f"{remote_root}/clio-relay-bootstrap.sh"
    remote_created = False
    primary_error: BaseException | None = None
    try:
        _run(
            [
                "ssh",
                ssh_host,
                "bash",
                "-c",
                f"umask 077; mkdir -- {shlex.quote(remote_root)}; "
                f"chmod 700 -- {shlex.quote(remote_root)}",
            ]
        )
        remote_created = True
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive = temp_path / "clio-relay-head.tar"
            script_path = temp_path / "clio-relay-bootstrap.sh"
            deployment = create_bootstrap_archive(
                source_root=source_root,
                archive=archive,
                relay_wheel=relay_wheel,
            )
            rebound_identity = bootstrap_relay_identity(
                source_root=source_root,
                relay_wheel=relay_wheel,
                relay_artifact_sha256=relay_artifact_sha256,
            )
            if rebound_identity != planned_identity or (
                deployment.install_spec != planned_identity.transport_install_spec
            ):
                raise ConfigurationError(
                    "bootstrap source identity changed between preflight and payload build"
                )
            source_archive_sha256 = _sha256_regular_file(deployment.archive)
            _run(["scp", str(deployment.archive), f"{ssh_host}:{remote_archive}"])
            script_path.write_text(
                render_linux_user_bootstrap_script(
                    frp_version=frp_version,
                    cluster=cluster,
                    core_dir=core_dir,
                    spool_dir=spool_dir,
                    agent_adapter=agent_adapter,
                    agent_npm_package=agent_npm_package,
                    agent_npm_bin=agent_npm_bin,
                    agent_args=agent_args or [],
                    jarvis_resource_graph_profile=jarvis_resource_graph_profile,
                    allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
                    relay_install_spec=deployment.install_spec,
                    relay_deployment_install_spec=planned_identity.install_spec,
                    relay_artifact_sha256=planned_identity.deployment_artifact_sha256,
                    relay_source_identity=planned_identity.source_identity,
                    invocation_id=invocation_id,
                    source_archive=remote_archive,
                    source_archive_sha256=source_archive_sha256,
                ),
                encoding="utf-8",
                newline="\n",
            )
            _run(["scp", str(script_path), f"{ssh_host}:{remote_script}"])
        result = _run(
            ["ssh", ssh_host, "bash", remote_script],
            timeout_seconds=BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS,
        )
        receipt_lines = [
            line.removeprefix("bootstrap_receipt_json=")
            for line in result.stdout.splitlines()
            if line.startswith("bootstrap_receipt_json=")
        ]
        if len(receipt_lines) != 1:
            raise RelayError(
                "bootstrap output must contain exactly one current invocation receipt, "
                f"observed {len(receipt_lines)}"
            )
        if len(receipt_lines[0].encode("utf-8")) > 1024 * 1024:
            raise RelayError("bootstrap stdout receipt exceeds the bounded size")
        try:
            raw_receipt = cast(object, json.loads(receipt_lines[0]))
        except json.JSONDecodeError as exc:
            raise RelayError(f"bootstrap receipt was not valid JSON: {exc}") from exc
        if not isinstance(raw_receipt, dict):
            raise RelayError("bootstrap receipt was not a JSON object")
        receipt = cast(dict[str, object], raw_receipt)
        if receipt.get("invocation_id") != invocation_id:
            raise RelayError("bootstrap receipt does not match the completed invocation")
        _validate_bootstrap_receipt(
            receipt,
            bootstrap_profile=bootstrap_profile,
            relay_install_spec=planned_identity.install_spec,
            desired_fingerprint=expected_desired_state.fingerprint,
            expected_jarvis_resource_graph_profile=(
                expected_desired_state.jarvis_resource_graph_profile
            ),
            expected_allow_jarvis_resource_graph_build=(
                expected_desired_state.allow_jarvis_resource_graph_build
            ),
            expected_worker_service=(
                endpoint_user_service_name(cluster) if cluster is not None else None
            ),
        )
        _verify_persistent_bootstrap_receipt(
            ssh_host=ssh_host,
            receipt=receipt,
            timeout_seconds=10,
        )
        return result.stdout.splitlines()
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if remote_created:
            try:
                _run(["ssh", ssh_host, "rm", "-rf", "--", remote_root])
            except RelayError as cleanup_error:
                if primary_error is None:
                    raise
                primary_error.add_note(f"remote bootstrap staging cleanup failed: {cleanup_error}")


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
            "$HOME/.local/share/clio-relay/bootstrap-receipt.json",
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


def _validate_bootstrap_receipt(
    receipt: dict[str, object],
    *,
    bootstrap_profile: str,
    relay_install_spec: str,
    desired_fingerprint: str,
    expected_jarvis_resource_graph_profile: str | None,
    expected_allow_jarvis_resource_graph_build: bool,
    expected_worker_service: str | None,
) -> None:
    """Validate action-specific v2 evidence from the current remote invocation."""
    install_receipt_sha256 = receipt.get("install_receipt_sha256")
    outcome = receipt.get("outcome")
    duration = receipt.get("duration_seconds")
    components = receipt.get("components")
    operations = receipt.get("operations")
    preservation = receipt.get("preservation")
    worker = receipt.get("worker")
    generation = receipt.get("generation")
    queue_operation = receipt.get("queue_operation")
    jarvis_initialization = receipt.get("jarvis_initialization")
    jarvis_resource_graph = receipt.get("jarvis_resource_graph")
    jarvis_commands = receipt.get("jarvis_commands")
    jarvis_preservation = receipt.get("jarvis_preservation")
    service = receipt.get("service")
    contract = {
        "schema_version": receipt.get("schema_version") == "clio-relay.bootstrap-receipt.v2",
        "bootstrap_profile": receipt.get("bootstrap_profile") == bootstrap_profile,
        "relay_install_spec": receipt.get("relay_install_spec") == relay_install_spec,
        "desired_fingerprint": receipt.get("desired_fingerprint") == desired_fingerprint,
        "outcome": outcome
        in {
            "noop_verified",
            "verified_after_transfer",
            "repaired",
            "reconciled",
            "full",
        },
        "install_receipt_sha256": _is_sha256_value(install_receipt_sha256),
        "duration_seconds": (
            not isinstance(duration, bool) and isinstance(duration, (int, float)) and duration >= 0
        ),
        "completed_at": isinstance(receipt.get("completed_at"), str)
        and bool(receipt.get("completed_at")),
        "components": isinstance(components, dict)
        and len(cast(dict[object, object], components)) > 0,
        "operations": isinstance(operations, dict),
        "preservation": isinstance(preservation, dict),
        "worker": isinstance(worker, dict),
        "generation": isinstance(generation, dict),
        "queue_operation": isinstance(queue_operation, dict),
        "jarvis_initialization": isinstance(jarvis_initialization, dict),
        "jarvis_resource_graph": isinstance(jarvis_resource_graph, dict),
        "jarvis_commands": isinstance(jarvis_commands, dict),
        "jarvis_preservation": isinstance(jarvis_preservation, dict),
        "service": isinstance(service, dict),
    }
    failed = sorted(name for name, passed in contract.items() if not passed)
    if failed:
        raise RelayError(f"bootstrap receipt contract failed: {failed}")
    assert isinstance(components, dict)
    typed_components = cast(dict[str, object], components)
    required_components = {"clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv"}
    if not required_components.issubset(typed_components):
        raise RelayError("bootstrap receipt omitted required component evidence")
    component_actions: dict[str, str] = {}
    for name, raw_evidence in typed_components.items():
        if not isinstance(raw_evidence, dict):
            raise RelayError(f"bootstrap component evidence is invalid: {name}")
        evidence = cast(dict[str, object], raw_evidence)
        action = evidence.get("action")
        component_duration = evidence.get("duration_seconds")
        if (
            action not in {"reused", "prepared", "materialized", "replaced"}
            or not isinstance(evidence.get("observed_identity"), dict)
            or isinstance(component_duration, bool)
            or not isinstance(component_duration, (int, float))
            or component_duration < 0
        ):
            raise RelayError(f"bootstrap component action evidence is invalid: {name}")
        component_actions[name] = cast(str, action)
    assert isinstance(operations, dict)
    typed_operations = cast(dict[str, object], operations)
    count_fields = (
        "download_count",
        "service_restart_count",
        "service_start_count",
        "service_stop_count",
        "service_enable_count",
        "scheduler_submission_count",
        "scheduler_cancellation_count",
        "generation_gc_count",
        "payload_transfer_count",
        "payload_transfer_bytes",
    )
    for field in count_fields:
        value = typed_operations.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RelayError(f"bootstrap operation count is invalid: {field}")
    downloads = typed_operations.get("downloads")
    if not isinstance(downloads, list) or len(cast(list[object], downloads)) != cast(
        int, typed_operations["download_count"]
    ):
        raise RelayError("bootstrap download evidence does not match its count")
    if any(
        typed_operations[field] != 0
        for field in (
            "scheduler_submission_count",
            "scheduler_cancellation_count",
            "generation_gc_count",
        )
    ):
        raise RelayError("bootstrap performed a forbidden scheduler or generation operation")
    payload_count = cast(int, typed_operations["payload_transfer_count"])
    payload_bytes = cast(int, typed_operations["payload_transfer_bytes"])
    if payload_count not in {0, 2} or (payload_count == 0) != (payload_bytes == 0):
        raise RelayError("bootstrap payload transfer evidence is inconsistent")
    assert isinstance(preservation, dict)
    typed_preservation = cast(dict[str, object], preservation)
    if typed_preservation != {
        "scheduler_jobs_cancelled": False,
        "old_generations_retained": True,
        "jarvis_init_on_existing_root": False,
    }:
        raise RelayError("bootstrap preservation evidence is invalid")
    assert isinstance(worker, dict)
    typed_worker = cast(dict[str, object], worker)
    if typed_worker.get("service_name") != expected_worker_service:
        raise RelayError("bootstrap worker service evidence does not match")
    assert isinstance(service, dict)
    typed_service = cast(dict[str, object], service)
    service_pending_install = typed_service.get("pending_install")
    if not isinstance(service_pending_install, bool):
        raise RelayError("bootstrap service pending-install evidence is invalid")
    if expected_worker_service is not None:
        if outcome == "full" and service_pending_install:
            if (
                typed_worker.get("service_was_active") is not False
                or typed_worker.get("service_was_enabled") is not False
                or typed_worker.get("worker_ready") is not False
            ):
                raise RelayError("fresh bootstrap service-pending evidence is inconsistent")
        elif (
            typed_worker.get("service_was_active") is not True
            or typed_worker.get("worker_ready") is not True
            or service_pending_install
        ):
            raise RelayError("managed bootstrap did not leave a ready endpoint service")
    assert isinstance(generation, dict)
    typed_generation = cast(dict[str, object], generation)
    if (
        typed_generation.get("active") != desired_fingerprint
        or not isinstance(typed_generation.get("current_target"), str)
        or not cast(str, typed_generation["current_target"])
    ):
        raise RelayError("bootstrap generation evidence does not prove desired activation")
    assert isinstance(queue_operation, dict)
    typed_queue_operation = cast(dict[str, object], queue_operation)
    queue_action = typed_queue_operation.get("action")
    queue_duration = typed_queue_operation.get("duration_seconds")
    if (
        queue_action not in {"verified_read_only", "audited_and_sealed"}
        or isinstance(queue_duration, bool)
        or not isinstance(queue_duration, (int, float))
        or queue_duration < 0
        or (queue_action == "audited_and_sealed" and queue_duration <= 0)
    ):
        raise RelayError("bootstrap queue action evidence is invalid")
    assert isinstance(jarvis_initialization, dict)
    typed_jarvis_initialization = cast(dict[str, object], jarvis_initialization)
    jarvis_init_action = typed_jarvis_initialization.get("action")
    jarvis_init_duration = typed_jarvis_initialization.get("duration_seconds")
    if (
        jarvis_init_action not in {"preserved", "initialized"}
        or isinstance(jarvis_init_duration, bool)
        or not isinstance(jarvis_init_duration, (int, float))
        or jarvis_init_duration < 0
        or (jarvis_init_action == "initialized" and jarvis_init_duration <= 0)
        or (jarvis_init_action == "preserved" and jarvis_init_duration != 0)
    ):
        raise RelayError("bootstrap JARVIS initialization evidence is invalid")
    assert isinstance(jarvis_resource_graph, dict)
    typed_jarvis_graph = cast(dict[str, object], jarvis_resource_graph)
    jarvis_graph_action = typed_jarvis_graph.get("action")
    jarvis_graph_duration = typed_jarvis_graph.get("duration_seconds")
    jarvis_builtin_result = typed_jarvis_graph.get("builtin_result")
    if (
        set(typed_jarvis_graph)
        != {
            "action",
            "duration_seconds",
            "benchmark_enabled",
            "selected_profile",
            "allow_build_fallback",
            "builtin_result",
        }
        or jarvis_graph_action not in {"preserved", "loaded", "built"}
        or typed_jarvis_graph.get("benchmark_enabled") is not False
        or typed_jarvis_graph.get("selected_profile") != expected_jarvis_resource_graph_profile
        or typed_jarvis_graph.get("allow_build_fallback")
        is not expected_allow_jarvis_resource_graph_build
        or isinstance(jarvis_graph_duration, bool)
        or not isinstance(jarvis_graph_duration, (int, float))
        or jarvis_graph_duration < 0
        or (jarvis_graph_action in {"loaded", "built"} and jarvis_graph_duration <= 0)
        or (jarvis_graph_action == "preserved" and jarvis_graph_duration != 0)
    ):
        raise RelayError("bootstrap JARVIS resource graph evidence is invalid")
    if jarvis_graph_action == "preserved":
        if jarvis_builtin_result is not None:
            raise RelayError("preserved JARVIS graph claimed builtin activation evidence")
    else:
        if expected_jarvis_resource_graph_profile is None or not isinstance(
            jarvis_builtin_result, dict
        ):
            raise RelayError("JARVIS graph activation omitted builtin result evidence")
        try:
            validate_jarvis_builtin_result(
                cast(dict[str, object], jarvis_builtin_result),
                requested_profile=expected_jarvis_resource_graph_profile,
            )
        except ValueError as exc:
            raise RelayError(f"bootstrap JARVIS builtin graph evidence is invalid: {exc}") from exc
        expected_builtin_action = "loaded" if jarvis_graph_action == "loaded" else "unavailable"
        if cast(dict[str, object], jarvis_builtin_result).get("action") != expected_builtin_action:
            raise RelayError("bootstrap JARVIS graph action contradicts builtin evidence")
        if jarvis_graph_action == "built" and not expected_allow_jarvis_resource_graph_build:
            raise RelayError("bootstrap reported an unauthorized JARVIS graph build")
    assert isinstance(jarvis_commands, dict)
    typed_jarvis_commands = cast(dict[str, object], jarvis_commands)
    command_count = typed_jarvis_commands.get("count")
    command_argv = typed_jarvis_commands.get("argv")
    typed_command_argv = cast(list[object], command_argv) if isinstance(command_argv, list) else []
    if (
        isinstance(command_count, bool)
        or not isinstance(command_count, int)
        or command_count < 0
        or not isinstance(command_argv, list)
        or len(typed_command_argv) != command_count
        or any(
            not isinstance(raw_command, list)
            or not raw_command
            or any(
                not isinstance(value, str) or not value for value in cast(list[object], raw_command)
            )
            for raw_command in typed_command_argv
        )
    ):
        raise RelayError("bootstrap JARVIS command evidence is invalid")
    assert isinstance(jarvis_preservation, dict)
    typed_jarvis_preservation = cast(dict[str, object], jarvis_preservation)
    if not isinstance(typed_jarvis_preservation.get("before"), dict) or not isinstance(
        typed_jarvis_preservation.get("after"), dict
    ):
        raise RelayError("bootstrap JARVIS preservation evidence is invalid")
    raw_binding = typed_jarvis_preservation.get("repositories")
    if not isinstance(raw_binding, dict):
        raise RelayError("bootstrap JARVIS repository binding evidence is invalid")
    binding = cast(dict[str, object], raw_binding)
    if set(binding) != {"link_action", "link", "target", "repositories"} or binding.get(
        "link_action"
    ) not in {"reused", "created", "retargeted"}:
        raise RelayError("bootstrap JARVIS repository link evidence is invalid")
    raw_repository_update = binding.get("repositories")
    if not isinstance(raw_repository_update, dict):
        raise RelayError("bootstrap JARVIS repository update evidence is invalid")
    repository_update = cast(dict[str, object], raw_repository_update)
    if set(repository_update) != {
        "action",
        "managed_repo",
        "added_managed_repos",
        "removed_previous_managed_repos",
        "before_sha256",
        "after_sha256",
    } or repository_update.get("action") not in {"reused", "updated"}:
        raise RelayError("bootstrap JARVIS repository update evidence is invalid")
    before_state = cast(dict[str, object], typed_jarvis_preservation["before"])
    after_state = cast(dict[str, object], typed_jarvis_preservation["after"])
    if (
        repository_update.get("before_sha256") != before_state.get("repos_sha256")
        or repository_update.get("after_sha256") != after_state.get("repos_sha256")
        or not isinstance(repository_update.get("added_managed_repos"), list)
        or not isinstance(repository_update.get("removed_previous_managed_repos"), list)
    ):
        raise RelayError("bootstrap JARVIS repository hashes do not bind preservation evidence")
    if jarvis_graph_action == "loaded" and cast(dict[str, object], jarvis_builtin_result).get(
        "source_sha256"
    ) != after_state.get("resource_graph_sha256"):
        raise RelayError("loaded JARVIS graph does not match its packaged source digest")
    if outcome == "noop_verified":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
            or typed_operations["service_restart_count"] != 0
            or typed_operations["service_start_count"] != 0
            or typed_operations["service_stop_count"] != 0
            or typed_operations["service_enable_count"] != 0
            or typed_operations["payload_transfer_count"] != 0
            or typed_operations["payload_transfer_bytes"] != 0
            or queue_action != "verified_read_only"
            or queue_duration != 0
            or jarvis_init_action != "preserved"
            or jarvis_graph_action != "preserved"
            or command_count != 0
            or typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or typed_jarvis_preservation.get("repositories_byte_identical") is not True
            or binding.get("link_action") != "reused"
            or repository_update.get("action") != "reused"
        ):
            raise RelayError("bootstrap no-op receipt reported mutation")
    elif outcome == "verified_after_transfer":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
            or typed_operations["service_restart_count"] != 0
            or typed_operations["service_start_count"] != 0
            or typed_operations["service_stop_count"] != 0
            or typed_operations["service_enable_count"] != 0
            or payload_count != 2
            or payload_bytes <= 0
            or queue_action != "verified_read_only"
            or queue_duration != 0
            or jarvis_init_action != "preserved"
            or jarvis_graph_action != "preserved"
            or command_count != 0
            or typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or typed_jarvis_preservation.get("repositories_byte_identical") is not True
            or binding.get("link_action") != "reused"
            or repository_update.get("action") != "reused"
        ):
            raise RelayError("post-transfer verification receipt reported mutation")
    elif outcome == "repaired":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
        ):
            raise RelayError("bootstrap repair receipt reported component replacement")
        if jarvis_init_action != "preserved":
            raise RelayError("bootstrap repair receipt reported JARVIS initialization")
        if jarvis_graph_action != "preserved" or command_count != 0:
            raise RelayError("bootstrap repair receipt reported JARVIS commands")
        managed_repo = repository_update.get("managed_repo")
        added_repositories = repository_update.get("added_managed_repos")
        removed_repositories = repository_update.get("removed_previous_managed_repos")
        if (
            typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
        ):
            raise RelayError("bootstrap repair receipt reported JARVIS state mutation")
        if (
            not isinstance(managed_repo, str)
            or not PurePosixPath(managed_repo).is_absolute()
            or any(character in managed_repo for character in "\x00\r\n")
            or binding.get("link") != managed_repo
            or binding.get("link_action") not in {"reused", "created", "retargeted"}
            or not isinstance(added_repositories, list)
            or not isinstance(removed_repositories, list)
            or not _is_sha256_value(typed_generation.get("previous"))
        ):
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        managed_suffix = MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        if not managed_repo.endswith(managed_suffix):
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        remote_home = managed_repo[: -len(managed_suffix)]
        expected_target = (
            remote_home + "/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        )
        expected_previous = remote_home + "/.local/src/clio-relay/jarvis-packages/clio_relay"
        expected_legacy = remote_home + LEGACY_MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        typed_removed_repositories = cast(list[object], removed_repositories)
        removed_repositories_are_proven = bool(
            all(isinstance(value, str) for value in typed_removed_repositories)
            and typed_removed_repositories
            == sorted(set(cast(list[str], typed_removed_repositories)))
            and set(cast(list[str], typed_removed_repositories))
            <= {expected_previous, expected_legacy}
        )
        repository_action = repository_update.get("action")
        if binding.get("target") != expected_target:
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        if repository_action == "reused":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not True
                or added_repositories
                or removed_repositories
            ):
                raise RelayError("bootstrap managed JARVIS repository reuse is invalid")
        elif repository_action == "updated":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not False
                or added_repositories not in ([], [managed_repo])
                or not removed_repositories_are_proven
                or (not added_repositories and not removed_repositories)
            ):
                raise RelayError("bootstrap managed JARVIS repository repair is invalid")
        else:
            raise RelayError("bootstrap managed JARVIS repository repair is invalid")
    elif outcome == "reconciled":
        raw_transaction = receipt.get("transaction")
        transaction_mode = (
            cast(dict[str, object], raw_transaction).get("mode")
            if isinstance(raw_transaction, dict)
            else None
        )
        if transaction_mode == "component-upgrade":
            expected_actions = {
                "clio-relay": "replaced",
                "clio-kit": "replaced",
                "jarvis-cd": "replaced",
                "jarvis-util": "reused",
                "frp": "reused",
                "uv": "reused",
            }
        elif transaction_mode == "relay-only":
            expected_actions = {
                "clio-relay": "prepared",
                "clio-kit": "reused",
                "jarvis-cd": "reused",
                "jarvis-util": "reused",
                "frp": "reused",
                "uv": "reused",
            }
        else:
            raise RelayError("reconciled bootstrap receipt has an invalid transaction mode")
        if any(component_actions.get(name) != action for name, action in expected_actions.items()):
            raise RelayError("staged reconcile receipt has invalid component actions")
        if jarvis_init_action != "preserved":
            raise RelayError("staged reconcile reported JARVIS initialization")
        if jarvis_graph_action != "preserved" or command_count != 0:
            raise RelayError("staged reconcile reported JARVIS commands")
        previous_generation = typed_generation.get("previous")
        link_action = binding.get("link_action")
        previous_generation_is_proven = bool(
            previous_generation == "legacy" or _is_sha256_value(previous_generation)
        )
        link_action_is_proven = bool(
            link_action == "reused"
            or (link_action in {"created", "retargeted"} and previous_generation_is_proven)
        )
        if (
            typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or not link_action_is_proven
        ):
            raise RelayError("staged reconcile did not preserve existing JARVIS state")
        managed_repo = repository_update.get("managed_repo")
        added_repositories = repository_update.get("added_managed_repos")
        removed_repositories = repository_update.get("removed_previous_managed_repos")
        if (
            not isinstance(managed_repo, str)
            or not PurePosixPath(managed_repo).is_absolute()
            or any(character in managed_repo for character in "\x00\r\n")
            or binding.get("link") != managed_repo
        ):
            raise RelayError("staged reconcile repository binding is invalid")
        managed_suffix = MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        if not managed_repo.endswith(managed_suffix):
            raise RelayError("staged reconcile repository binding is invalid")
        remote_home = managed_repo[: -len(managed_suffix)]
        expected_target = (
            remote_home + "/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        )
        expected_previous = remote_home + "/.local/src/clio-relay/jarvis-packages/clio_relay"
        expected_legacy = remote_home + LEGACY_MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        typed_removed_repositories = cast(list[object], removed_repositories)
        removed_repositories_are_proven = bool(
            all(isinstance(value, str) for value in typed_removed_repositories)
            and typed_removed_repositories
            == sorted(set(cast(list[str], typed_removed_repositories)))
            and set(cast(list[str], typed_removed_repositories))
            <= {expected_previous, expected_legacy}
        )
        repository_action = repository_update.get("action")
        if (
            binding.get("target") != expected_target
            or not isinstance(added_repositories, list)
            or not isinstance(removed_repositories, list)
        ):
            raise RelayError("staged reconcile repository binding is invalid")
        if repository_action == "reused":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not True
                or added_repositories
                or removed_repositories
            ):
                raise RelayError("staged reconcile repository reuse is invalid")
        elif repository_action == "updated":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not False
                or added_repositories not in ([], [managed_repo])
                or not removed_repositories_are_proven
                or (not added_repositories and not removed_repositories)
            ):
                raise RelayError("staged reconcile repository migration is invalid")
        else:  # pragma: no cover - rejected by the generic evidence contract above
            raise RelayError("staged reconcile repository action is invalid")
        if payload_count != 2 or payload_bytes <= 0:
            raise RelayError("staged reconcile omitted its transferred payload evidence")
    elif outcome == "full":
        if any(action != "prepared" for action in component_actions.values()):
            raise RelayError("fresh bootstrap receipt has invalid component actions")
        if jarvis_init_action != "initialized":
            raise RelayError("fresh bootstrap did not report JARVIS initialization")
        expected_command_count = 2 if jarvis_graph_action == "loaded" else 3
        if (
            jarvis_graph_action not in {"loaded", "built"}
            or command_count != expected_command_count
        ):
            raise RelayError("fresh bootstrap did not report exact graph activation commands")
        expected_graph_commands: list[list[str]] = [
            [
                "jarvis",
                "rg",
                "load-builtin",
                cast(str, expected_jarvis_resource_graph_profile),
                "+json",
            ]
        ]
        if jarvis_graph_action == "built":
            expected_graph_commands.append(["jarvis", "rg", "build", "+no_benchmark"])
        if cast(list[object], command_argv)[1:] != expected_graph_commands:
            raise RelayError("fresh bootstrap reported unexpected graph commands")
        if payload_count != 2 or payload_bytes <= 0:
            raise RelayError("fresh bootstrap omitted its transferred payload evidence")


def _is_sha256_value(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _worker_writer_proof_shell(*, rendered_core_dir: str, success_variable: str) -> str:
    """Render one bounded legacy-writer proof against the configured core."""
    return "\n".join(
        [
            (
                'if ! python3 - "$WORKER_CLUSTER_NAME" '
                f"{rendered_core_dir} /proc <<'__CLIO_RELAY_WORKER_WRITER_PROOF__'"
            ),
            _WORKER_WRITER_PROOF_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_WRITER_PROOF__",
            "then",
            ('  echo "cannot prove exclusive relay writer ownership for $WORKER_CLUSTER_NAME" >&2'),
            "  exit 1",
            "fi",
            f"{success_variable}=1",
        ]
    )


def _worker_upgrade_fence_script(
    cluster: str | None,
    *,
    rendered_core_dir: str,
    activation_observation_timeout_seconds: int | None = None,
    activation_poll_seconds: int | None = None,
    activation_progress_seconds: int | None = None,
) -> tuple[str, str, str, str]:
    """Render managed fencing, recheck, migration command, and restart step."""
    service_name = endpoint_user_service_name(cluster) if cluster is not None else ""
    declarations = "\n".join(
        [
            f"WORKER_SERVICE_NAME={shlex.quote(service_name)}",
            f"WORKER_CLUSTER_NAME={shlex.quote(cluster or '')}",
            "WORKER_WAS_ACTIVE=0",
            "WORKER_STOP_CONFIRMED=0",
            "WORKER_WRITER_PROOF=0",
            "WORKER_WRITER_RECHECK=0",
            "WORKER_LIFETIME_EXCLUSIVE=0",
            "WORKER_LIFETIME_GUARD_FD=",
            "WORKER_LIFETIME_LOCK_PATH=",
            "WORKER_RESTART_ATTEMPTED=0",
            "WORKER_RESTARTED=0",
            "WORKER_POST_START_STATE=unknown",
            "WORKER_POST_START_SUB_STATE=unknown",
            "WORKER_RESTART_OUTCOME=not-attempted",
        ]
    )
    if not service_name:
        no_service_fence = "\n".join(
            [
                declarations,
                "bootstrap_restore_fenced_worker_on_failure() { :; }",
                "bootstrap_release_worker_lifetime_guard() { :; }",
            ]
        )
        return no_service_fence, "", "clio-relay init", ""
    initial_proof = _worker_writer_proof_shell(
        rendered_core_dir=rendered_core_dir,
        success_variable="WORKER_WRITER_PROOF",
    )
    inherited_fd_check = "\n".join(
        [
            (
                f'python3 - {rendered_core_dir} "$WORKER_LIFETIME_GUARD_FD" '
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)} "
                "<<'__CLIO_RELAY_WORKER_LIFETIME_FD__'"
            ),
            _WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_LIFETIME_FD__",
        ]
    )
    recheck = "\n".join(
        [
            "bootstrap_require_worker_lifetime_guard",
            _worker_writer_proof_shell(
                rendered_core_dir=rendered_core_dir,
                success_variable="WORKER_WRITER_RECHECK",
            ),
        ]
    )
    fence = "\n".join(
        [
            declarations,
            f"CLIO_RELAY_ENDPOINT_SERVICE_NAME={shlex.quote(service_name)}",
            "CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=start",
            render_bounded_user_service_activation_helper(
                observation_timeout_seconds=activation_observation_timeout_seconds,
                poll_seconds=activation_poll_seconds,
                progress_seconds=activation_progress_seconds,
            ),
            "bootstrap_release_worker_lifetime_guard() {",
            '  case "$WORKER_LIFETIME_GUARD_FD" in',
            "    '') return 0 ;;",
            "    8)",
            "      WORKER_LIFETIME_GUARD_FD=",
            "      exec 8>&-",
            "      ;;",
            "    *)",
            (
                '      echo "refusing to release unexpected worker lifetime guard fd: '
                '$WORKER_LIFETIME_GUARD_FD" >&2'
            ),
            "      return 1",
            "      ;;",
            "  esac",
            "}",
            "bootstrap_bounded_worker_restart() {",
            "  WORKER_RESTART_ATTEMPTED=1",
            "  bootstrap_release_worker_lifetime_guard || return 1",
            "  if ! clio_relay_endpoint_activate_bounded; then",
            '    WORKER_POST_START_STATE="$CLIO_RELAY_ENDPOINT_ACTIVE_STATE"',
            '    WORKER_POST_START_SUB_STATE="$CLIO_RELAY_ENDPOINT_SUB_STATE"',
            '    WORKER_RESTART_OUTCOME="$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"',
            "    return 1",
            "  fi",
            '  WORKER_POST_START_STATE="$CLIO_RELAY_ENDPOINT_ACTIVE_STATE"',
            '  WORKER_POST_START_SUB_STATE="$CLIO_RELAY_ENDPOINT_SUB_STATE"',
            '  WORKER_RESTART_OUTCOME="$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"',
            "  WORKER_RESTARTED=1",
            "}",
            "bootstrap_restore_fenced_worker_on_failure() {",
            '  local status="$1"',
            (
                '  if [ "$status" -ne 0 ] && [ "$WORKER_WAS_ACTIVE" = "1" ]'
                ' && [ "$WORKER_RESTARTED" != "1" ]; then'
            ),
            '    if [ "$WORKER_STOP_CONFIRMED" = "1" ]; then',
            '      if [ "$WORKER_RESTART_ATTEMPTED" = "1" ]; then',
            (
                '        echo "bootstrap failed after the worker start was already '
                'enqueued; observing $WORKER_SERVICE_NAME without a duplicate start" >&2'
            ),
            "      else",
            (
                '        echo "bootstrap failed; attempting bounded recovery of '
                '$WORKER_SERVICE_NAME" >&2'
            ),
            "      fi",
            "      if bootstrap_bounded_worker_restart; then",
            (
                '        echo "bootstrap worker_recovery=restored '
                'service=$WORKER_SERVICE_NAME state=active" >&2'
            ),
            "      else",
            '        case "$WORKER_RESTART_OUTCOME" in',
            "          in-progress)",
            (
                '            echo "bootstrap worker_recovery=in-progress '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE; systemd start job retained "
                'without a duplicate request" >&2'
            ),
            "            ;;",
            "          failed)",
            (
                '            echo "bootstrap worker_recovery=failed '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE; "
                'operator action is required" >&2'
            ),
            "            ;;",
            "          *)",
            (
                '            echo "bootstrap worker_recovery=unverified '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE "
                "outcome=$WORKER_RESTART_OUTCOME; "
                'operator verification is required" >&2'
            ),
            "            ;;",
            "        esac",
            "      fi",
            "    else",
            (
                '      echo "bootstrap failed while fencing $WORKER_SERVICE_NAME; '
                'worker state is unknown and requires operator verification" >&2'
            ),
            "    fi",
            "  fi",
            "}",
            "bootstrap_worker_fence_exit() {",
            "  status=$?",
            "  trap - EXIT",
            '  bootstrap_restore_fenced_worker_on_failure "$status"',
            "  bootstrap_release_worker_lifetime_guard || true",
            "  if declare -F bootstrap_cleanup_preparing_root >/dev/null; then",
            "    bootstrap_cleanup_preparing_root || true",
            "  fi",
            '  exit "$status"',
            "}",
            "trap bootstrap_worker_fence_exit EXIT",
            "command -v systemctl >/dev/null 2>&1 || {",
            '  echo "systemctl is required to fence the configured relay worker" >&2',
            "  exit 1",
            "}",
            "command -v timeout >/dev/null 2>&1 || {",
            '  echo "timeout is required to bound relay worker recovery" >&2',
            "  exit 1",
            "}",
            (
                'if ! WORKER_LOAD_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=LoadState --value)"; then'
            ),
            '  echo "cannot inspect relay worker unit: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            (
                'if ! WORKER_ACTIVE_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=ActiveState --value)"; then'
            ),
            '  echo "cannot inspect relay worker state: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            'case "$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE" in',
            "  loaded:active|loaded:activating|loaded:reloading|loaded:deactivating)",
            "    WORKER_WAS_ACTIVE=1",
            '    systemctl --user stop "$WORKER_SERVICE_NAME"',
            (
                '    if ! WORKER_POST_STOP_STATE="$(systemctl --user show '
                '"$WORKER_SERVICE_NAME" --property=ActiveState --value)"; then'
            ),
            '      echo "cannot verify stopped relay worker: $WORKER_SERVICE_NAME" >&2',
            "      exit 1",
            "    fi",
            '    case "$WORKER_POST_STOP_STATE" in',
            "      inactive|failed) WORKER_STOP_CONFIRMED=1 ;;",
            "      *)",
            (
                '        echo "relay worker stop has unknown state '
                '$WORKER_POST_STOP_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "        exit 1",
            "        ;;",
            "    esac",
            "    ;;",
            "  loaded:inactive|loaded:failed|masked:inactive|not-found:inactive) ;;",
            "  *)",
            (
                '    echo "refusing bootstrap with unknown relay worker state '
                '$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "    ;;",
            "esac",
            initial_proof,
            f"mkdir -p -- {rendered_core_dir}",
            (
                f"WORKER_LIFETIME_LOCK_PATH={rendered_core_dir}/"
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)}"
            ),
            'exec 8<>"$WORKER_LIFETIME_LOCK_PATH"',
            "WORKER_LIFETIME_GUARD_FD=8",
            "bootstrap_require_worker_lifetime_guard() {",
            inherited_fd_check,
            "}",
            "bootstrap_require_worker_lifetime_guard",
            "WORKER_LIFETIME_EXCLUSIVE=1",
        ]
    )
    restart = "\n".join(
        [
            'if [ "$WORKER_WAS_ACTIVE" = "1" ]; then',
            "  bootstrap_require_worker_lifetime_guard",
            "  if ! bootstrap_bounded_worker_restart; then",
            (
                '    echo "relay worker did not become active '
                "state=${WORKER_POST_START_STATE:-unknown} "
                "sub_state=${WORKER_POST_START_SUB_STATE:-unknown} "
                "outcome=${WORKER_RESTART_OUTCOME:-unverified}: "
                '$WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "  fi",
            "fi",
        ]
    )
    return fence, recheck, "clio-relay init --migrate-legacy-output", restart


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
    return f"""
bootstrap_plan_value() {{
  local field="$1"
  python3 - "$field" <<'__CLIO_RELAY_PLAN_VALUE__'
import json
import os
import sys

value = json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])
for part in sys.argv[1].split("."):
    value = value[part]
if not isinstance(value, str):
    raise SystemExit("bootstrap plan value is not a string")
print(value)
__CLIO_RELAY_PLAN_VALUE__
}}

bootstrap_provider_exec() (
{staged_provider_environment_sanitizer}
  if [ -n "${{BOOTSTRAP_STAGED_GENERATION:-}}" ]; then
    exec python3 -I -c {staged_provider_exec_program} \
      "$BOOTSTRAP_STAGED_GENERATION" \
      "$BOOTSTRAP_STAGED_MANIFEST_SHA256" "$@"
  elif [ "${{BOOTSTRAP_CANDIDATE_PROVIDER_READY:-0}}" = "1" ]; then
    exec python3 -I -c {candidate_uv_install_program} \
      verify-installed-and-exec \
      "$BOOTSTRAP_PINNED_UV" {UV_LINUX_AMD64_EXECUTABLE_SHA256} \
      "$BOOTSTRAP_CANDIDATE_ARTIFACT" "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
      "$BOOTSTRAP_CANDIDATE_TOOL_DIR" "$BOOTSTRAP_CANDIDATE_BIN_DIR" \
      "$BOOTSTRAP_CANDIDATE_CACHE_DIR" \
      "$BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR" \
      "$BOOTSTRAP_CANDIDATE_PROVIDER_SHA256" -I "$@"
  else
    exec "$BOOTSTRAP_RECOVERY_PROVIDER" -I "$@"
  fi
)

bootstrap_candidate_action() {{
  local action="$1"
  shift
  bootstrap_provider_exec - "$BOOTSTRAP_CANDIDATE_RECONCILE" "$action" "$@" \
    <<'__CLIO_RELAY_CANDIDATE_ACTION__'
import importlib.util
import json
import os
import sys
from pathlib import Path

path, action, *arguments = sys.argv[1:]
if os.environ.get("BOOTSTRAP_STAGED_GENERATION") and action != "repair-legacy-cursors":
    from clio_relay import bootstrap_reconcile as module
else:
    candidate_root = os.environ["BOOTSTRAP_CANDIDATE_PYTHON_ROOT"]
    if not sys.path or sys.path[0] != candidate_root:
        sys.path.insert(0, candidate_root)
    name = "clio_relay.bootstrap_reconcile_candidate_action"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise SystemExit("could not load candidate bootstrap reconciler")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
journal_path = Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"])
if action == "journal-create":
    service_value = os.environ["BOOTSTRAP_SERVICE_ACTIVE_BEFORE"]
    journal = module.BootstrapTransactionJournal(
        invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
        desired_fingerprint=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
        mode=os.environ.get("BOOTSTRAP_PLAN_MODE", "relay-only"),
        state=module.BootstrapTransactionState.LOCKED,
        previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"] or None,
        service_name=os.environ["WORKER_SERVICE_NAME"] or None,
        service_was_active=(
            True if service_value == "1" else (False if service_value == "0" else None)
        ),
        service_was_enabled=(
            True
            if os.environ.get("BOOTSTRAP_SERVICE_ENABLED_BEFORE") == "1"
            else (
                False
                if os.environ.get("BOOTSTRAP_SERVICE_ENABLED_BEFORE") == "0"
                else None
            )
        ),
        phase_identities={{"locked": os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]}},
    )
    journal.persist(journal_path)
elif action == "journal-advance":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    target = module.BootstrapTransactionState(arguments[0])
    if target is module.BootstrapTransactionState.PREPARED:
        journal.prepared_generation = os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]
    journal.advance(target)
    journal.persist(journal_path)
elif action == "journal-phase":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    journal.record_phase(arguments[0], arguments[1])
    journal.persist(journal_path)
elif action == "journal-state":
    print(module.BootstrapTransactionJournal.load(journal_path).state.value)
elif action == "recovery-plan":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    payload = journal.model_dump(mode="json")
    payload["recovery_mode"] = journal.recovery_mode
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
elif action == "recovery-complete":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    journal.complete_recovery()
    journal.persist(journal_path)
elif action == "execution-boundary":
    root = Path(arguments[0])
    print(
        json.dumps(
            module.execution_environment_identity(
                root,
                executables={{
                    "python": Path(arguments[1]),
                    "jarvis": Path(arguments[2]),
                }},
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "jarvis-wrapper":
    print(
        json.dumps(
            module.write_jarvis_wrapper(Path(arguments[0]), Path(arguments[1])),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "finish-activation":
    if os.environ.get("BOOTSTRAP_STAGED_GENERATION"):
        receipt_payload = module._read_regular_bounded(
            Path(arguments[0]) / "install-receipt.json",
            maximum=4 * 1024 * 1024,
        )
        receipt_document = json.loads(receipt_payload)
        desired_payload = receipt_document.get("deployment_manifest")
        if not isinstance(desired_payload, dict):
            raise SystemExit("staged install receipt omitted its desired state")
    else:
        desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
        desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
        desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
    desired = module.BootstrapDesiredState.model_validate(desired_payload)
    print(
        json.dumps(
            module.finish_staged_activation(
                desired,
                generation=Path(arguments[0]),
                expected_manifest_sha256=arguments[1],
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "repair-legacy-cursors":
    print(
        json.dumps(
            module.repair_legacy_cursor_permissions_for_upgrade(Path(arguments[0])),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "repair-managed-binding":
    desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
    desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
    desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
    desired = module.BootstrapDesiredState.model_validate(desired_payload)
    before = module.inspect_jarvis_state(desired)
    binding = module.repair_managed_jarvis_binding(
        desired,
        previous_managed_repos=(
            Path.home() / ".local/src/clio-relay/jarvis-packages/clio_relay",
        ),
    )
    after = module.inspect_jarvis_state(desired)
    print(
        json.dumps(
            {{
                "schema_version": "clio-relay.bootstrap-binding-repair.v1",
                "before": before.model_dump(mode="json"),
                "after": after.model_dump(mode="json"),
                "binding": binding,
            }},
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "exchange-preflight":
    print(
        json.dumps(
            module.verify_atomic_exchange_support(
                tuple(Path(value) for value in arguments),
                identity=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
else:
    raise SystemExit(f"unknown candidate bootstrap action: {{action}}")
__CLIO_RELAY_CANDIDATE_ACTION__
}}

bootstrap_use_staged_provider() {{
  local generation="$1"
  local expected_manifest_sha256="$2"
  if [ -L "$generation" ] || [ ! -d "$generation" ]; then
    echo "staged bootstrap generation is not one owned directory" >&2
    return 1
  fi
  case "$expected_manifest_sha256" in
    (*[!0-9a-f]*|'') echo "staged manifest digest is invalid" >&2; return 1 ;;
  esac
  if [ "${{#expected_manifest_sha256}}" -ne 64 ]; then
    echo "staged manifest digest has an invalid length" >&2
    return 1
  fi
  BOOTSTRAP_STAGED_GENERATION="$generation"
  BOOTSTRAP_STAGED_MANIFEST_SHA256="$expected_manifest_sha256"
  export BOOTSTRAP_STAGED_GENERATION BOOTSTRAP_STAGED_MANIFEST_SHA256
  bootstrap_provider_exec -c \
    'import clio_relay,jarvis_cd; print("staged_provider=sealed_memfd")' >/dev/null
}}

bootstrap_require_stable_link() {{
  local path="$1"
  local expected="$2"
  if [ ! -L "$path" ] || [ "$(readlink "$path")" != "$expected" ]; then
    echo "bootstrap stable activation link changed: $path" >&2
    return 1
  fi
}}

bootstrap_verify_stable_activation_links() {{
  bootstrap_require_stable_link \
    "$HOME/.local/share/clio-relay/install-receipt.json" \
    "$HOME/.local/share/clio-relay/current/install-receipt.json"
  bootstrap_require_stable_link "$HOME/.local/bin/clio-relay" \
    "$HOME/.local/share/clio-relay/current/bin/clio-relay"
  bootstrap_require_stable_link "$HOME/.local/bin/jarvis" \
    "$HOME/.local/share/clio-relay/current/bin/jarvis"
  bootstrap_require_stable_link \
    "$HOME/.local/share/clio-relay/clio_relay" \
      "$HOME/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
}}

bootstrap_active_generation_identity() {{
  local current target prefix identity
  current="$HOME/.local/share/clio-relay/current"
  if [ ! -L "$current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  target="$(readlink -f "$current")"
  prefix="$(readlink -f "$HOME/.local/share/clio-relay/generations")/"
  case "$target" in
    "$prefix"*) identity="${{target#"$prefix"}}" ;;
    *)
      echo "bootstrap current pointer does not name one managed generation" >&2
      return 1
      ;;
  esac
  case "$identity" in
    (*[!0-9a-f]*|'')
      echo "bootstrap current generation identity is invalid" >&2
      return 1
      ;;
  esac
  if [ "${{#identity}}" -ne 64 ]; then
    echo "bootstrap current generation identity has an invalid length" >&2
    return 1
  fi
  echo "$identity"
}}

bootstrap_active_generation_provider() {{
  local identity generations provider
  identity="$(bootstrap_active_generation_identity)" || return 1
  generations="$(readlink -f "$HOME/.local/share/clio-relay/generations")"
  provider="$generations/$identity/tools/clio-relay/bin/python"
  if [ ! -f "$provider" ] || [ ! -x "$provider" ]; then
    echo "bootstrap active generation provider is unavailable" >&2
    return 1
  fi
  echo "$provider"
}}

bootstrap_reconcile_transaction_exit() {{
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    local state
    state="$(bootstrap_candidate_action journal-state 2>/dev/null || true)"
    case "$state" in
      activating|activated|migration_started|migrated|starting|service_verified)
        echo "bootstrap reconcile crossed its forward-only activation boundary;" \
          "new generation retained for forward recovery" >&2
        ;;
      *)
        if [ "$WORKER_WAS_ACTIVE" = "1" ] && [ "$WORKER_RESTARTED" != "1" ]; then
          bootstrap_bounded_worker_restart || true
        fi
        ;;
    esac
  fi
  bootstrap_release_worker_lifetime_guard 2>/dev/null || true
  bootstrap_cleanup_preparing_root || true
  exit "$status"
}}

bootstrap_recovery_value() {{
  local field="$1"
  python3 - "$field" <<'__CLIO_RELAY_RECOVERY_VALUE__'
import json
import os
import sys

value = json.loads(os.environ["BOOTSTRAP_RECOVERY_JSON"])[sys.argv[1]]
if value is None:
    print("")
elif isinstance(value, bool):
    print("1" if value else "0")
elif isinstance(value, str):
    print(value)
else:
    raise SystemExit("bootstrap recovery field has an invalid type")
__CLIO_RELAY_RECOVERY_VALUE__
}}

bootstrap_recover_service() {{
  local service_name="$1"
  [ -n "$service_name" ] || return 0
  if [ "$(systemctl --user show "$service_name" --property=LoadState --value)" != \
       "loaded" ]; then
    echo "bootstrap recovery requires the registered endpoint service:" \
      "$service_name" >&2
    return 1
  fi
  systemctl --user enable "$service_name"
  systemctl --user start "$service_name"
  for _BOOTSTRAP_RECOVERY_START_ATTEMPT in $(seq 1 90); do
    if systemctl --user is-active --quiet "$service_name"; then
      return 0
    fi
    sleep 2
  done
  echo "bootstrap recovery could not restore endpoint service: $service_name" >&2
  return 1
}}

bootstrap_fence_recovered_service() {{
  local service_name="$1"
  local load_state active_state stopped_state
  [ -n "$service_name" ] || return 0
  load_state="$(
    systemctl --user show "$service_name" --property=LoadState --value
  )" || return 1
  active_state="$(
    systemctl --user show "$service_name" --property=ActiveState --value
  )" || return 1
  case "$load_state:$active_state" in
    loaded:active|loaded:activating|loaded:reloading|loaded:deactivating)
      systemctl --user stop "$service_name"
      stopped_state="$(
        systemctl --user show "$service_name" --property=ActiveState --value
      )" || return 1
      case "$stopped_state" in
        inactive|failed) return 0 ;;
        *)
          echo "bootstrap recovery could not fence endpoint service: $service_name" >&2
          return 1
          ;;
      esac
      ;;
    loaded:inactive|loaded:failed|masked:inactive|not-found:inactive) return 0 ;;
    *)
      echo "bootstrap recovery found unknown endpoint service state: " \
        "$load_state:$active_state:$service_name" >&2
      return 1
      ;;
  esac
}}

bootstrap_recover_interrupted_repair() {{
  local service_was_active="$1"
  local cluster_name="$2"
  local interrupted_service_name="$3"
  local recovery_service_should_run=0
  local recovery_queue recovery_worker recovery_worker_ready
  if [ "$service_was_active" = "1" ]; then
    recovery_service_should_run=1
  fi

{worker_fence}

  if [ -n "$interrupted_service_name" ] && \
     [ "$interrupted_service_name" != "$WORKER_SERVICE_NAME" ]; then
    echo "bootstrap repair recovery service identity changed:" \
      "$interrupted_service_name != $WORKER_SERVICE_NAME" >&2
    return 1
  fi

  # A power loss releases the lifetime lock after the original fence.  Preserve
  # both the journaled pre-transaction state and an operator restart observed
  # at recovery entry, then keep the exact queue writer fence until readiness is
  # sealed or the service restart relinquishes it.
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    recovery_service_should_run=1
  elif [ "$service_was_active" = "1" ]; then
    WORKER_WAS_ACTIVE=1
    WORKER_STOP_CONFIRMED=1
  fi

{worker_recheck}

  # The managed repository/link operation is exact and idempotent.  Re-running
  # it covers interruption before, during, or after the original activation
  # without depending on staged-generation evidence that repair never records.
  bootstrap_candidate_action repair-managed-binding >/dev/null

  recovery_queue="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
  )"
  export recovery_queue
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["recovery_queue"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
    2>/dev/null; then
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
    CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
    CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
    CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
    CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
    CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
    {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
    {init_command}
  fi
  recovery_queue="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
  export recovery_queue
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["recovery_queue"]); '\
'sys.exit(0 if value.get("complete") is True else 1)'; then
    echo "bootstrap repair recovery did not establish queue readiness" >&2
    return 1
  fi

  if [ "$recovery_service_should_run" = "1" ] && [ -n "$WORKER_SERVICE_NAME" ]; then
    if ! bootstrap_bounded_worker_restart; then
      echo "bootstrap repair recovery could not restore endpoint service:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    recovery_worker_ready=0
    for _BOOTSTRAP_REPAIR_RECOVERY_WORKER_ATTEMPT in $(seq 1 90); do
      recovery_worker="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          "$HOME/.local/bin/clio-relay" endpoint worker-info \
            --cluster "$cluster_name" --freshness-seconds 120 2>/dev/null || true
      )"
      if printf '%s\\n' "$recovery_worker" | python3 -c \
        'import json,sys; value=json.load(sys.stdin); '\
'sys.exit(0 if value.get("running") is True else 1)' 2>/dev/null; then
        recovery_worker_ready=1
        break
      fi
      sleep 2
    done
    if [ "$recovery_worker_ready" != "1" ]; then
      echo "bootstrap repair recovery did not observe a ready worker" >&2
      return 1
    fi
  fi
  BOOTSTRAP_REPAIR_RECOVERY_ACTIVE=1
}}

bootstrap_recover_previous_transaction() {{
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  export BOOTSTRAP_TRANSACTION_JOURNAL
  BOOTSTRAP_RECOVERY_JSON="$(bootstrap_candidate_action recovery-plan)"
  export BOOTSTRAP_RECOVERY_JSON
  local recovery_mode interrupted_mode interrupted_invocation service_name
  local service_was_active cluster_name
  recovery_mode="$(bootstrap_recovery_value recovery_mode)"
  interrupted_mode="$(bootstrap_recovery_value mode)"
  interrupted_invocation="$(bootstrap_recovery_value invocation_id)"
  service_name="$(bootstrap_recovery_value service_name)"
  service_was_active="$(bootstrap_recovery_value service_was_active)"
  cluster_name="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])["cluster"] or "")'
  )"
  case "$interrupted_invocation" in
    (*[!A-Za-z0-9_.-]*|'')
      echo "bootstrap transaction has an invalid invocation identity" >&2
      return 1
      ;;
  esac
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$interrupted_invocation"
  BOOTSTRAP_ROLLBACK_DIR="$BOOTSTRAP_TRANSACTION_ROOT/rollback"
  export BOOTSTRAP_TRANSACTION_ROOT BOOTSTRAP_ROLLBACK_DIR
  case "$recovery_mode" in
    discard)
      if [ "$service_was_active" = "1" ]; then
        bootstrap_recover_service "$service_name"
      fi
      ;;
    rollback)
      echo "legacy automatic bootstrap rollback is disabled because activation" \
        "identities cannot be proved; operator reconciliation is required" >&2
      return 1
      ;;
    forward)
      if [ "$interrupted_mode" = "repair" ]; then
        bootstrap_recover_interrupted_repair \
          "$service_was_active" "$cluster_name" "$service_name"
      else
        local prepared_generation prepared_manifest_sha256 recovery_generation
        prepared_generation="$(bootstrap_recovery_value prepared_generation)"
      case "$prepared_generation" in
        (*[!0-9a-f]*|'')
          echo "bootstrap forward recovery has an invalid generation" >&2
          return 1
          ;;
      esac
      [ "${{#prepared_generation}}" -eq 64 ] || return 1
      prepared_manifest_sha256="$(
        python3 - <<'__CLIO_RELAY_RECOVERY_MANIFEST__'
import json
import os

value = json.loads(os.environ["BOOTSTRAP_RECOVERY_JSON"])
identity = value.get("phase_identities", {{}}).get("prepared_manifest")
if not isinstance(identity, str):
    raise SystemExit("bootstrap recovery omitted prepared manifest identity")
print(identity)
__CLIO_RELAY_RECOVERY_MANIFEST__
      )"
      case "$prepared_manifest_sha256" in
        (*[!0-9a-f]*|'')
          echo "bootstrap forward recovery has an invalid manifest identity" >&2
          return 1
          ;;
      esac
      [ "${{#prepared_manifest_sha256}}" -eq 64 ] || return 1
      recovery_generation="$HOME/.local/share/clio-relay/generations/$prepared_generation"
      if [ -L "$recovery_generation" ] || [ ! -d "$recovery_generation" ]; then
        echo "bootstrap forward recovery generation identity changed" >&2
        return 1
      fi
      if [ "$JARVIS_EXISTING_FILE_COUNT" -ne 3 ] || \
         [ -z "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE" ] || \
         [ -z "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE" ]; then
        echo "bootstrap forward recovery cannot prove preserved JARVIS state" >&2
        return 1
      fi
      echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
        sha256sum --check --strict -
      echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
        sha256sum --check --strict -
      bootstrap_fence_recovered_service "$service_name"
      bootstrap_use_staged_provider "$recovery_generation" "$prepared_manifest_sha256"
      bootstrap_candidate_action finish-activation \
        "$recovery_generation" "$prepared_manifest_sha256" >/dev/null
      bootstrap_verify_stable_activation_links
      echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
        sha256sum --check --strict -
      echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
        sha256sum --check --strict -
      mkdir -p -- {rendered_core_dir}
      exec 8<>"{rendered_core_dir}/{WORKER_LIFETIME_LOCK_NAME}"
      WORKER_LIFETIME_GUARD_FD=8
      if ! flock -n 8; then
        echo "bootstrap forward recovery cannot prove exclusive queue ownership" >&2
        return 1
      fi
      {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
        bootstrap_candidate_action repair-legacy-cursors {rendered_core_dir} >/dev/null
      CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
      {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
        "$HOME/.local/bin/clio-relay" init --migrate-legacy-output
      CLIO_RELAY_CORE_DIR={rendered_core_dir} \
        "$HOME/.local/bin/clio-relay" queue readiness-info >/dev/null
      exec 8>&-
        if [ "$service_was_active" = "1" ] && [ -n "$service_name" ]; then
        bootstrap_recover_service "$service_name"
        local recovery_worker recovery_worker_ready
        recovery_worker_ready=0
        for _BOOTSTRAP_RECOVERY_WORKER_ATTEMPT in $(seq 1 90); do
          recovery_worker="$(
            CLIO_RELAY_CORE_DIR={rendered_core_dir} \
              "$HOME/.local/bin/clio-relay" endpoint worker-info \
                --cluster "$cluster_name" --freshness-seconds 120 2>/dev/null || true
          )"
          if printf '%s\\n' "$recovery_worker" | python3 -c \
            'import json,sys; value=json.load(sys.stdin); '\
'sys.exit(0 if value.get("running") is True else 1)' 2>/dev/null; then
            recovery_worker_ready=1
            break
          fi
          sleep 2
        done
        if [ "$recovery_worker_ready" != "1" ]; then
          echo "bootstrap forward recovery did not observe a ready worker" >&2
          return 1
        fi
        fi
      fi
      ;;
    none)
      return 0
      ;;
    *)
      echo "bootstrap transaction has an invalid recovery mode" >&2
      return 1
      ;;
  esac
  bootstrap_candidate_action recovery-complete
  if [ "${{BOOTSTRAP_REPAIR_RECOVERY_ACTIVE:-0}}" = "1" ]; then
    bootstrap_release_worker_lifetime_guard
    trap - EXIT
  fi
}}

bootstrap_relay_only_reconcile() {{
  BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
  WORKER_SERVICE_NAME="$(
    python3 -c \
      'import json,os; value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'print(value["worker_service"] or "")'
  )"
  BOOTSTRAP_DESIRED_FINGERPRINT="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["desired_fingerprint"])'
  )"
  case "$BOOTSTRAP_DESIRED_FINGERPRINT" in
    (*[!0-9a-f]*|'') echo "invalid desired generation fingerprint" >&2; return 1 ;;
  esac
  if [ "${{#BOOTSTRAP_DESIRED_FINGERPRINT}}" -ne 64 ]; then
    echo "invalid desired generation fingerprint length" >&2
    return 1
  fi
  BOOTSTRAP_GENERATIONS_ROOT="$HOME/.local/share/clio-relay/generations"
  BOOTSTRAP_GENERATION="$BOOTSTRAP_GENERATIONS_ROOT/$BOOTSTRAP_DESIRED_FINGERPRINT"
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  BOOTSTRAP_ROLLBACK_DIR="$BOOTSTRAP_TRANSACTION_ROOT/rollback"
  BOOTSTRAP_PREVIOUS_GENERATION="legacy"
  if [ -L "$HOME/.local/share/clio-relay/current" ]; then
    BOOTSTRAP_PREVIOUS_GENERATION="$(bootstrap_active_generation_identity)"
  elif [ -e "$HOME/.local/share/clio-relay/current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  BOOTSTRAP_SERVICE_ACTIVE_BEFORE="unknown"
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
  if [ -n "${{WORKER_SERVICE_NAME:-}}" ]; then
    if systemctl --user is-active --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=1
    else
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=0
    fi
    if systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ENABLED_BEFORE=1
    fi
  fi
  export BOOTSTRAP_INVOCATION_ID BOOTSTRAP_DESIRED_FINGERPRINT
  export BOOTSTRAP_TRANSACTION_JOURNAL BOOTSTRAP_PREVIOUS_GENERATION
  export BOOTSTRAP_SERVICE_ACTIVE_BEFORE BOOTSTRAP_SERVICE_ENABLED_BEFORE
  export WORKER_SERVICE_NAME
  mkdir -p "$BOOTSTRAP_GENERATIONS_ROOT" "$BOOTSTRAP_TRANSACTION_ROOT"
  bootstrap_candidate_action journal-create
  bootstrap_candidate_action journal-advance inspected
  bootstrap_candidate_action journal-advance preparing
  BOOTSTRAP_PREPARE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_RELAY_DOWNLOAD_COUNT=0
  BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT=0
  BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT=0

  if [ -e "$BOOTSTRAP_GENERATION" ]; then
    if [ ! -f "$BOOTSTRAP_GENERATION/.prepared" ]; then
      if [ -L "$HOME/.local/share/clio-relay/current" ]; then
        BOOTSTRAP_INCOMPLETE_CURRENT_TARGET="$(
          readlink -f "$HOME/.local/share/clio-relay/current" || true
        )"
        if [ -z "$BOOTSTRAP_INCOMPLETE_CURRENT_TARGET" ]; then
          echo "active generation pointer could not be resolved" >&2
          return 1
        fi
        if [ "$BOOTSTRAP_INCOMPLETE_CURRENT_TARGET" = \
             "$(readlink -f "$BOOTSTRAP_GENERATION")" ]; then
          echo "incomplete generation is active; recovery is required" >&2
          return 1
        fi
      fi
      rm -rf -- "$BOOTSTRAP_GENERATION"
    fi
  fi
  LEGACY_JARVIS_VENV="$(bootstrap_plan_value reusable_paths.jarvis_execution_environment)"
  LEGACY_JARVIS_PYTHON="$(bootstrap_plan_value reusable_paths.jarvis_execution_python)"
  LEGACY_JARVIS_EXECUTABLE="$(
    bootstrap_plan_value reusable_paths.jarvis_execution_executable
  )"
  if [ "$LEGACY_JARVIS_PYTHON" != "$LEGACY_JARVIS_VENV/bin/python" ] || \
     [ "$LEGACY_JARVIS_EXECUTABLE" != "$LEGACY_JARVIS_VENV/bin/jarvis" ] || \
     [ ! -x "$LEGACY_JARVIS_PYTHON" ] || [ ! -x "$LEGACY_JARVIS_EXECUTABLE" ]; then
    echo "legacy JARVIS executables do not match the retained execution boundary" >&2
    return 1
  fi
  JARVIS_CD_WHEEL=""
  CLIO_KIT_EXECUTABLE=""
  ACTIVE_JARVIS_VENV="$LEGACY_JARVIS_VENV"
  ACTIVE_JARVIS_PYTHON="$LEGACY_JARVIS_PYTHON"
  JARVIS_MCP_INSTALL_SPEC=""
  JARVIS_MCP_ARTIFACT_SHA256=""
  JARVIS_MCP_ARTIFACT_PATH=""
  CLIO_KIT_PROVIDER_PYTHON=""
  REUSED_RELAY_ARTIFACT=""
  if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
    JARVIS_CD_WHEEL="$(bootstrap_plan_value reusable_paths.jarvis-cd_artifact)"
    CLIO_KIT_EXECUTABLE="$(
      bootstrap_plan_value reusable_paths.clio-kit_clio-kit_executable
    )"
    REUSED_RELAY_ARTIFACT="$(
      bootstrap_plan_value reusable_paths.clio-relay_artifact
    )"
  else
    JARVIS_CD_WHEEL="$BOOTSTRAP_GENERATION/artifacts/{JARVIS_CD_WHEEL_FILENAME}"
    CLIO_KIT_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-kit"
    ACTIVE_JARVIS_VENV="$BOOTSTRAP_GENERATION/jarvis-venv"
    ACTIVE_JARVIS_PYTHON="$BOOTSTRAP_GENERATION/jarvis-venv/bin/python"
    JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
    JARVIS_MCP_ARTIFACT_SHA256={rendered_jarvis_mcp_artifact_sha256}
    JARVIS_MCP_ARTIFACT_PATH="$BOOTSTRAP_GENERATION/artifacts/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
  fi
  BOOTSTRAP_LEGACY_IDENTITY="$(
    bootstrap_candidate_action execution-boundary \
      "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
  )"
  export BOOTSTRAP_LEGACY_IDENTITY
  if [ ! -f "$BOOTSTRAP_GENERATION/.prepared" ]; then
    mkdir -m 0700 "$BOOTSTRAP_GENERATION"
    mkdir -p "$BOOTSTRAP_GENERATION/bin" "$BOOTSTRAP_GENERATION/tools"
    SOURCE_ARCHIVE={rendered_source_archive}
    SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
    if [ -z "$SOURCE_ARCHIVE_SHA256" ]; then
      echo "relay-only reconcile requires a verified source archive digest" >&2
      return 1
    fi
    echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
    bootstrap_safe_extract \
      "$BOOTSTRAP_PLAN_PROVIDER" "$SOURCE_ARCHIVE" "$BOOTSTRAP_GENERATION/source"

    DEST="$BOOTSTRAP_GENERATION/source"
    if [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
      STAGED_JARVIS_VENV="$BOOTSTRAP_GENERATION/jarvis-venv"
      "$HOME/.local/bin/uv" venv --python 3.12 --seed "$STAGED_JARVIS_VENV"
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated \
        --index-url https://pypi.org/simple \
        -r "$HOME/.local/src/jarvis-util/requirements.txt"
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated --no-deps \
        "$HOME/.local/src/jarvis-util"

      mkdir -m 0700 "$BOOTSTRAP_GENERATION/artifacts"
      curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --retry 3 --retry-all-errors --retry-max-time 180 \
        --connect-timeout 20 --max-time 180 \
        --output "$JARVIS_CD_WHEEL" "{JARVIS_CD_WHEEL_URL}"
      echo "{JARVIS_CD_WHEEL_SHA256} *$JARVIS_CD_WHEEL" | \
        sha256sum --check --strict -
      BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT=1
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated \
        --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"
      JARVIS_VERSION_PROBE='from importlib.metadata import version; '
      JARVIS_VERSION_PROBE+='assert version("jarvis-cd") == "{JARVIS_CD_VERSION}"'
      "$ACTIVE_JARVIS_PYTHON" -c "$JARVIS_VERSION_PROBE"

      if [ "$JARVIS_MCP_INSTALL_SPEC" != "{CLIO_KIT_JARVIS_MCP_WHEEL_URL}" ]; then
        echo "staged component upgrade requires the released clio-kit wheel URL" >&2
        return 1
      fi
      curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --retry 3 --retry-all-errors --retry-max-time 180 \
        --connect-timeout 20 --max-time 180 \
        --output "$JARVIS_MCP_ARTIFACT_PATH" "$JARVIS_MCP_INSTALL_SPEC"
      echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | \
        sha256sum --check --strict -
      BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT=1
      UV_TOOL_DIR="$BOOTSTRAP_GENERATION/tools" \
      UV_TOOL_BIN_DIR="$BOOTSTRAP_GENERATION/bin" \
        "$HOME/.local/bin/uv" tool install --force --python 3.12 --no-config \
          --default-index https://pypi.org/simple "$JARVIS_MCP_ARTIFACT_PATH"
      test -x "$CLIO_KIT_EXECUTABLE"
      CLIO_KIT_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-kit/bin/python"
      test -x "$CLIO_KIT_PROVIDER_PYTHON"
      test "$("$CLIO_KIT_PROVIDER_PYTHON" -c \
        'from importlib.metadata import version; print(version("clio-kit"))')" = \
        "{CLIO_KIT_JARVIS_MCP_VERSION}"
      "$CLIO_KIT_EXECUTABLE" --help >/dev/null
    fi
    RELAY_INSTALL_SPEC={rendered_relay_install_spec}
    RELAY_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
    RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
    RELAY_ARTIFACT_PATH=""
    if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
      RELAY_ARTIFACT_PATH="$REUSED_RELAY_ARTIFACT"
      RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    else
      case "$RELAY_INSTALL_SPEC" in
        clio-relay==*)
          DOWNLOAD_DIR="$DEST/downloaded-wheels"
          mkdir -p "$DOWNLOAD_DIR"
          "$LEGACY_JARVIS_PYTHON" -m pip download --isolated \
            --disable-pip-version-check --no-cache-dir --index-url https://pypi.org/simple \
            --no-deps --only-binary=:all: --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
          mapfile -t RELAY_WHEELS < <(
            find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
          )
          if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
            echo "expected exactly one downloaded clio-relay wheel" >&2
            return 1
          fi
          RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
          RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
          BOOTSTRAP_RELAY_DOWNLOAD_COUNT=1
          if [ -z "$RELAY_ARTIFACT_SHA256" ]; then
            RELAY_VERSION="${{RELAY_INSTALL_SPEC#clio-relay==}}"
            RELAY_ARTIFACT_SHA256="$(
              "$LEGACY_JARVIS_PYTHON" - \
                "$RELAY_VERSION" "$(basename "$RELAY_ARTIFACT_PATH")" \
                <<'__CLIO_RELAY_RECONCILE_PYPI_DIGEST__'
import json
import re
import sys
from urllib.parse import quote
from urllib.request import urlopen

version, filename = sys.argv[1:]
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    content = response.read(4 * 1024 * 1024 + 1)
if len(content) > 4 * 1024 * 1024:
    raise SystemExit("PyPI clio-relay metadata exceeds the bounded response size")
document = json.loads(content)
matches = [
    item
    for item in document.get("urls", [])
    if item.get("filename") == filename and item.get("packagetype") == "bdist_wheel"
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return one exact clio-relay wheel identity")
digest = matches[0].get("digests", {{}}).get("sha256")
if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{{64}}", digest) is None:
    raise SystemExit("PyPI clio-relay wheel identity omitted a valid SHA-256")
print(digest)
__CLIO_RELAY_RECONCILE_PYPI_DIGEST__
            )"
          fi
          ;;
        *.whl)
          RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
          ;;
      esac
    fi
    if [ -n "$RELAY_ARTIFACT_PATH" ]; then
      test -n "$RELAY_ARTIFACT_SHA256"
      echo "$RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH" | \
        sha256sum --check --strict -
    fi
    UV_TOOL_DIR="$BOOTSTRAP_GENERATION/tools" \
    UV_TOOL_BIN_DIR="$BOOTSTRAP_GENERATION/bin" \
      "$HOME/.local/bin/uv" tool install --force --python 3.12 --no-config \
        --default-index https://pypi.org/simple --with "$JARVIS_CD_WHEEL" \
        "$RELAY_INSTALL_TARGET"
    RELAY_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-relay"
    RELAY_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-relay/bin/python"
    test -x "$RELAY_EXECUTABLE" -a -x "$RELAY_PROVIDER_PYTHON"
    if [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
      "$HOME/.local/bin/uv" pip install --python "$ACTIVE_JARVIS_PYTHON" \
        --default-index https://pypi.org/simple \
        --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
    fi
    bootstrap_candidate_action jarvis-wrapper \
      "$BOOTSTRAP_GENERATION/bin/jarvis" "$ACTIVE_JARVIS_PYTHON"
    ACTIVE_JARVIS_EXECUTABLE="$ACTIVE_JARVIS_VENV/bin/jarvis"
    BOOTSTRAP_ACTIVE_IDENTITY="$(
      bootstrap_candidate_action execution-boundary \
        "$ACTIVE_JARVIS_VENV" "$ACTIVE_JARVIS_PYTHON" "$ACTIVE_JARVIS_EXECUTABLE"
    )"
    export BOOTSTRAP_ACTIVE_IDENTITY
    if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
      ln -s "$CLIO_KIT_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-kit"
    fi
    JARVIS_PACKAGE_PROBE='import clio_relay, jarvis_cd; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.bounded_command.pkg; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.mcp_call.pkg; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.remote_agent.pkg'
    "$RELAY_PROVIDER_PYTHON" -I -c "$JARVIS_PACKAGE_PROBE"
    "$ACTIVE_JARVIS_PYTHON" -I -c "$JARVIS_PACKAGE_PROBE"

    export BOOTSTRAP_GENERATION RELAY_INSTALL_SPEC RELAY_ARTIFACT_PATH
    export RELAY_ARTIFACT_SHA256 RELAY_EXECUTABLE RELAY_PROVIDER_PYTHON
    export JARVIS_CD_WHEEL CLIO_KIT_EXECUTABLE ACTIVE_JARVIS_PYTHON
    export BOOTSTRAP_RELAY_DOWNLOAD_COUNT BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT
    export BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT JARVIS_MCP_ARTIFACT_PATH
    export JARVIS_MCP_INSTALL_SPEC JARVIS_MCP_ARTIFACT_SHA256
    export CLIO_KIT_PROVIDER_PYTHON
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_GENERATION_RECEIPT__'
import json
import os
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.bootstrap_reconcile import BootstrapDesiredState
from clio_relay.installation import (
    ComponentArtifactIdentity,
    load_install_receipt,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
    probe_persistent_uv_tool_identity,
    write_install_receipt,
)
from clio_relay.jarvis_mcp import jarvis_mcp_server_artifact_verified
from clio_relay.mcp_call.runner import mcp_server_artifact_identity
from clio_relay.validation_report import sha256_file

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
generation = Path(os.environ["BOOTSTRAP_GENERATION"])
old = load_install_receipt(Path.home() / ".local/share/clio-relay/install-receipt.json")
relay_artifact_text = os.environ["RELAY_ARTIFACT_PATH"]
relay_artifact = Path(relay_artifact_text).resolve() if relay_artifact_text else None
relay_distribution = distribution("clio-relay")
relay_persistent = None
if relay_artifact is not None:
    relay_persistent = probe_persistent_uv_tool_identity(
        uv_executable=str(Path.home() / ".local/bin/uv"),
        tool_executable=os.environ["RELAY_EXECUTABLE"],
        provider_interpreter=os.environ["RELAY_PROVIDER_PYTHON"],
        source_artifact=relay_artifact,
        distribution="clio-relay",
        distribution_version=relay_distribution.version,
        entry_point="clio-relay",
        tool_directory=str(generation / "tools"),
        tool_bin_directory=str(generation / "bin"),
    )
relay_component = ComponentArtifactIdentity(
    distribution=relay_distribution.name,
    distribution_version=relay_distribution.version,
    install_spec=os.environ["RELAY_INSTALL_SPEC"],
    requested_source=(
        "pypi"
        if os.environ["RELAY_INSTALL_SPEC"].startswith("clio-relay==")
        else ("wheel" if relay_artifact is not None else "checkout")
    ),
    artifact_filename=(relay_artifact.name if relay_artifact is not None else None),
    artifact_sha256=(sha256_file(relay_artifact) if relay_artifact is not None else None),
    runtime_artifact_path=(str(relay_artifact) if relay_artifact is not None else None),
    runtime_command=[os.environ["RELAY_EXECUTABLE"], "installation-info"],
    runtime_interpreters={{
        "provider": os.environ["RELAY_PROVIDER_PYTHON"],
        "execution": os.environ["ACTIVE_JARVIS_PYTHON"],
    }},
    runtime_executables={{
        "clio-relay": os.environ["RELAY_EXECUTABLE"],
        "uv": str(Path.home() / ".local/bin/uv"),
    }},
    persistent_tool=relay_persistent,
)
components = dict(old.components)
components["clio-relay"] = relay_distribution.version
component_artifacts = {{
    **old.component_artifacts,
    "clio-relay": relay_component,
}}
if os.environ["BOOTSTRAP_PLAN_MODE"] == "component-upgrade":
    clio_kit_wheel = Path(os.environ["JARVIS_MCP_ARTIFACT_PATH"]).resolve(strict=True)
    clio_kit_sha256 = os.environ["JARVIS_MCP_ARTIFACT_SHA256"]
    if sha256_file(clio_kit_wheel) != clio_kit_sha256:
        raise SystemExit("staged clio-kit wheel digest changed before receipt creation")
    clio_kit_executable = os.environ["CLIO_KIT_EXECUTABLE"]
    clio_kit_provider = os.environ["CLIO_KIT_PROVIDER_PYTHON"]
    clio_kit_command = [clio_kit_executable, "mcp-server", "jarvis"]
    clio_kit_native = probe_clio_kit_native_execution_contract(clio_kit_command)
    clio_kit_persistent = probe_persistent_uv_tool_identity(
        uv_executable=str(Path.home() / ".local/bin/uv"),
        tool_executable=clio_kit_executable,
        provider_interpreter=clio_kit_provider,
        source_artifact=clio_kit_wheel,
        distribution="clio-kit",
        distribution_version=desired.clio_kit_version,
        entry_point="clio-kit",
        tool_directory=str(generation / "tools"),
        tool_bin_directory=str(generation / "bin"),
    )
    clio_kit_server = mcp_server_artifact_identity(
        clio_kit_executable,
        ["mcp-server", "jarvis"],
        verify_relay_jarvis_cd_lock=True,
    )
    if not jarvis_mcp_server_artifact_verified(clio_kit_server):
        raise SystemExit("staged clio-kit server artifact did not verify its JARVIS lock")
    component_artifacts["clio-kit"] = ComponentArtifactIdentity(
        distribution="clio-kit",
        distribution_version=desired.clio_kit_version,
        install_spec=os.environ["JARVIS_MCP_INSTALL_SPEC"],
        requested_source="github_release",
        artifact_filename=clio_kit_wheel.name,
        artifact_sha256=clio_kit_sha256,
        runtime_artifact_path=str(clio_kit_wheel),
        runtime_command=clio_kit_command,
        runtime_interpreters={{"provider": clio_kit_provider}},
        runtime_executables={{
            "clio-kit": clio_kit_executable,
            "uv": str(Path.home() / ".local/bin/uv"),
        }},
        native_execution=clio_kit_native,
        persistent_tool=clio_kit_persistent,
        locked_server_runtime=clio_kit_server["nested_runtime"],
    )

    jarvis_wheel = Path(os.environ["JARVIS_CD_WHEEL"]).resolve(strict=True)
    if sha256_file(jarvis_wheel) != desired.jarvis_cd_wheel_sha256:
        raise SystemExit("staged JARVIS-CD wheel digest changed before receipt creation")
    jarvis_python = os.environ["ACTIVE_JARVIS_PYTHON"]
    jarvis_executable = str(Path(jarvis_python).parent / "jarvis")
    component_artifacts["jarvis-cd"] = ComponentArtifactIdentity(
        distribution="jarvis-cd",
        distribution_version=desired.jarvis_cd_version,
        install_spec=desired.jarvis_cd_wheel_url,
        requested_source="github_release",
        artifact_filename=jarvis_wheel.name,
        artifact_sha256=desired.jarvis_cd_wheel_sha256,
        runtime_artifact_path=str(jarvis_wheel),
        runtime_command=[jarvis_executable, "--help"],
        runtime_interpreters={{
            "provider": os.environ["RELAY_PROVIDER_PYTHON"],
            "execution": jarvis_python,
        }},
        runtime_executables={{"jarvis": jarvis_executable}},
        native_execution=probe_jarvis_native_execution_capability(jarvis_python),
    )
    components["clio-kit"] = desired.clio_kit_version
    components["jarvis-cd"] = desired.jarvis_cd_version
write_install_receipt(
    install_spec=desired.relay_install_spec,
    artifact_path=relay_artifact,
    path=generation / "install-receipt.json",
    components=components,
    component_artifacts=component_artifacts,
    deployment_fingerprint=desired.fingerprint,
    deployment_manifest=desired.model_dump(mode="json"),
    generation=desired.fingerprint,
)
__CLIO_RELAY_GENERATION_RECEIPT__
    CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json" \
      "$RELAY_EXECUTABLE" installation-info >"$BOOTSTRAP_GENERATION/installation-info.json"
    export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json"
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_VERIFY_GENERATION__'
import json
import os
from pathlib import Path

info = json.loads(Path(os.environ["BOOTSTRAP_GENERATION"] + "/installation-info.json").read_text())
runtime = info.get("component_runtime", {{}})
jarvis_runtime = runtime.get("jarvis-cd", {{}})
checks = {{
    "receipt_matches_install": info.get("receipt_matches_install") is True,
    "clio-relay.persistent_tool_verified": (
        runtime.get("clio-relay", {{}}).get("persistent_tool_verified") is True
    ),
    "clio-relay.execution_runtime_verified": (
        runtime.get("clio-relay", {{}}).get("execution_runtime_verified") is True
    ),
    "clio-kit.persistent_tool_verified": (
        runtime.get("clio-kit", {{}}).get("persistent_tool_verified") is True
    ),
    "clio-kit.native_execution_capability_verified": (
        runtime.get("clio-kit", {{}}).get("native_execution_capability_verified") is True
    ),
    "jarvis-cd.verified": jarvis_runtime.get("verified") is True,
}}
if checks["jarvis-cd.verified"] is False:
    record_closure = jarvis_runtime.get("execution_record_closure", {{}})
    record_closure_error_code = (
        record_closure.get("error_code") if isinstance(record_closure, dict) else None
    )
    checks.update({{
        "jarvis-cd.distribution_identity_verified": (
            jarvis_runtime.get("distribution_identity_verified") is True
        ),
        "jarvis-cd.runtime_artifact_path_verified": (
            jarvis_runtime.get("runtime_artifact_path_verified") is True
        ),
        "jarvis-cd.artifact_sha256_verified": (
            jarvis_runtime.get("artifact_sha256_verified") is True
        ),
        "jarvis-cd.execution_interpreter_verified": (
            jarvis_runtime.get("execution_interpreter_verified") is True
        ),
        "jarvis-cd.execution_record_closure_verified": (
            jarvis_runtime.get("execution_record_closure_verified") is True
        ),
        "jarvis-cd.native_execution_capability_verified": (
            jarvis_runtime.get("native_execution_capability_verified") is True
        ),
        "jarvis-cd.jarvis_executable_verified": (
            jarvis_runtime.get("jarvis_executable_verified") is True
        ),
    }})
    if (
        isinstance(record_closure_error_code, str)
        and 0 < len(record_closure_error_code) <= 96
        and all(
            character.isascii() and (character.isalnum() or character == "-")
            for character in record_closure_error_code
        )
    ):
        checks[
            "jarvis-cd.execution_record_closure_error." + record_closure_error_code
        ] = False
failed_checks = sorted(name for name, verified in checks.items() if not verified)
if failed_checks:
    raise SystemExit(
        "prepared relay generation runtime identity did not verify: "
        + ", ".join(failed_checks)
    )
__CLIO_RELAY_VERIFY_GENERATION__
    unset CLIO_RELAY_INSTALL_RECEIPT
    BOOTSTRAP_LEGACY_IDENTITY_AFTER="$(
      bootstrap_candidate_action execution-boundary \
        "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
    )"
    if [ "$BOOTSTRAP_LEGACY_IDENTITY_AFTER" != "$BOOTSTRAP_LEGACY_IDENTITY" ]; then
      echo "legacy JARVIS execution environment changed during preparation" >&2
      return 1
    fi
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_GENERATION_MANIFEST__'
import json
import os
from pathlib import Path

from clio_relay.validation_report import sha256_file

generation = Path(os.environ["BOOTSTRAP_GENERATION"])
manifest = {{
    "schema_version": "clio-relay.bootstrap-generation.v1",
    "fingerprint": os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
    "plan": json.loads(os.environ["BOOTSTRAP_PLAN_JSON"]),
    "legacy_execution_identity": json.loads(os.environ["BOOTSTRAP_LEGACY_IDENTITY"]),
    "active_execution_identity": json.loads(os.environ["BOOTSTRAP_ACTIVE_IDENTITY"]),
    "jarvis_wrapper_sha256": sha256_file(generation / "bin/jarvis"),
    "install_receipt": str(generation / "install-receipt.json"),
    "install_receipt_sha256": sha256_file(generation / "install-receipt.json"),
}}
path = generation / "manifest.json"
temporary = generation / ".manifest.tmp"
with temporary.open("x", encoding="utf-8", newline="\\n") as stream:
    stream.write(json.dumps(manifest, indent=2, sort_keys=True) + "\\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, path)
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
prepared = generation / ".prepared"
prepared_temporary = generation / ".prepared.tmp"
with prepared_temporary.open("x", encoding="ascii", newline="\\n") as stream:
    stream.write(manifest["fingerprint"] + "\\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(prepared_temporary, prepared)
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
__CLIO_RELAY_GENERATION_MANIFEST__
  fi
  export BOOTSTRAP_RELAY_DOWNLOAD_COUNT BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT
  export BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT
  export BOOTSTRAP_GENERATION LEGACY_JARVIS_VENV
  RELAY_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-relay"
  if [ ! -L "$RELAY_EXECUTABLE" ]; then
    echo "prepared generation relay launcher is not a symbolic link" >&2
    return 1
  fi
  RELAY_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-relay/bin/python"
  if [ ! -x "$RELAY_PROVIDER_PYTHON" ]; then
    echo "prepared generation provider is unavailable" >&2
    return 1
  fi
  BOOTSTRAP_LEGACY_IDENTITY_AFTER="$(
    bootstrap_candidate_action execution-boundary \
      "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
  )"
  if [ "$BOOTSTRAP_LEGACY_IDENTITY_AFTER" != "$BOOTSTRAP_LEGACY_IDENTITY" ]; then
    echo "legacy JARVIS execution environment changed before activation" >&2
    return 1
  fi
  BOOTSTRAP_PREPARED_INSPECTION="$(
    CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json" \
      "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_VERIFY_PREPARED_GENERATION__'
import json
import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    inspect_prepared_generation,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
inspection = inspect_prepared_generation(
    desired,
    generation=Path(os.environ["BOOTSTRAP_GENERATION"]),
    legacy_execution_identity=json.loads(os.environ["BOOTSTRAP_LEGACY_IDENTITY"]),
)
print(json.dumps(inspection, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_VERIFY_PREPARED_GENERATION__
  )"
  export BOOTSTRAP_PREPARED_INSPECTION
  BOOTSTRAP_PREPARED_MANIFEST_SHA256="$(
    python3 - <<'__CLIO_RELAY_PREPARED_MANIFEST_SHA256__'
import json
import os

print(json.loads(os.environ["BOOTSTRAP_PREPARED_INSPECTION"])["manifest_sha256"])
__CLIO_RELAY_PREPARED_MANIFEST_SHA256__
  )"
  case "$BOOTSTRAP_PREPARED_MANIFEST_SHA256" in
    (*[!0-9a-f]*|'') echo "prepared manifest identity is invalid" >&2; return 1 ;;
  esac
  if [ "${{#BOOTSTRAP_PREPARED_MANIFEST_SHA256}}" -ne 64 ]; then
    echo "prepared manifest identity has an invalid length" >&2
    return 1
  fi
  (
    BOOTSTRAP_STAGED_GENERATION="$BOOTSTRAP_GENERATION"
    BOOTSTRAP_STAGED_MANIFEST_SHA256="$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
    export BOOTSTRAP_STAGED_GENERATION BOOTSTRAP_STAGED_MANIFEST_SHA256
    bootstrap_provider_exec -c \
      'import clio_relay,jarvis_cd; print("staged_provider=sealed_memfd")'
  ) >/dev/null
  bootstrap_candidate_action journal-phase prepared_manifest \
    "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  bootstrap_candidate_action exchange-preflight \
    "$HOME/.local/share/clio-relay" \
    "$HOME/.local/bin" \
    "$(dirname "$JARVIS_REPOS_FILE")" >/dev/null
  BOOTSTRAP_PREPARE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_PREPARE_STARTED_NS BOOTSTRAP_PREPARE_COMPLETED_NS
  bootstrap_candidate_action journal-advance prepared
  bootstrap_candidate_action journal-advance fencing

{worker_fence}

  if [ "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" = "1" ] && [ "$WORKER_WAS_ACTIVE" != "1" ]; then
    echo "endpoint service activity changed before fencing" >&2
    return 1
  fi
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" = "0" ] && [ "$WORKER_WAS_ACTIVE" != "0" ]; then
    echo "endpoint service activity changed before fencing" >&2
    return 1
  fi
  bootstrap_candidate_action journal-advance fenced
  trap bootstrap_reconcile_transaction_exit EXIT
  echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
    sha256sum --check --strict -
  echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
    sha256sum --check --strict -
  bootstrap_candidate_action journal-advance activating

  bootstrap_use_staged_provider \
    "$BOOTSTRAP_GENERATION" "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  BOOTSTRAP_STAGED_ACTIVATION="$(
    bootstrap_candidate_action finish-activation \
      "$BOOTSTRAP_GENERATION" "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  )"
  export BOOTSTRAP_STAGED_ACTIVATION
  BOOTSTRAP_JARVIS_REPO_RECONCILIATION="$(
    python3 - <<'__CLIO_RELAY_STAGED_REPOSITORY_EVIDENCE__'
import json
import os

activation = json.loads(os.environ["BOOTSTRAP_STAGED_ACTIVATION"])
print(json.dumps(activation["jarvis_repository"], sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_STAGED_REPOSITORY_EVIDENCE__
  )"
  export BOOTSTRAP_JARVIS_REPO_RECONCILIATION
  bootstrap_verify_stable_activation_links
  "$HOME/.local/share/clio-relay/current/bin/clio-relay" installation-info >/dev/null
  "$HOME/.local/share/clio-relay/current/bin/clio-relay" --help >/dev/null
  bootstrap_candidate_action journal-advance activated
  echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
    sha256sum --check --strict -
  echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
    sha256sum --check --strict -

  bootstrap_candidate_action journal-advance migration_started
{worker_recheck}
  BOOTSTRAP_QUEUE_ACTION=verified_read_only
  BOOTSTRAP_QUEUE_DURATION_NS=0
  BOOTSTRAP_QUEUE_BEFORE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
  )"
  export BOOTSTRAP_QUEUE_BEFORE
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_QUEUE_BEFORE"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
    2>/dev/null; then
    BOOTSTRAP_QUEUE_ACTION=audited_and_sealed
    BOOTSTRAP_QUEUE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
    CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
    CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
    CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
    CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
    CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
    {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
    {init_command}
    BOOTSTRAP_QUEUE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
    BOOTSTRAP_QUEUE_DURATION_NS=$((BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS))
  fi
  bootstrap_candidate_action journal-advance migrated
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
  BOOTSTRAP_SERVICE_RESTART_COUNT=0
  BOOTSTRAP_SERVICE_START_COUNT=0
  BOOTSTRAP_SERVICE_STOP_COUNT=0
  BOOTSTRAP_SERVICE_ENABLE_COUNT=0
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    BOOTSTRAP_SERVICE_STOP_COUNT=1
    BOOTSTRAP_SERVICE_RESTART_COUNT=1
    bootstrap_candidate_action journal-advance starting
  elif [ -n "$WORKER_SERVICE_NAME" ]; then
    if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
      echo "managed endpoint unit is unavailable; install it before bootstrap:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    bootstrap_candidate_action journal-advance starting
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready after reconcile" >&2
      return 1
    fi
  fi
{worker_restart}
  if [ -n "$WORKER_SERVICE_NAME" ]; then
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi

  BOOTSTRAP_QUEUE_EVIDENCE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
  BOOTSTRAP_WORKER_EVIDENCE=""
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
    for _BOOTSTRAP_READY_ATTEMPT in $(seq 1 90); do
      if BOOTSTRAP_WORKER_EVIDENCE="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          "$HOME/.local/bin/clio-relay" endpoint worker-info \
            --cluster "$WORKER_CLUSTER_NAME" --freshness-seconds 120 \
            --readiness-only 2>/dev/null
      )"; then
        export BOOTSTRAP_WORKER_EVIDENCE
        if python3 -c \
          'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_WORKER_EVIDENCE"]); '\
'sys.exit(0 if value.get("running") is True else 1)'; then
          break
        fi
      fi
      BOOTSTRAP_WORKER_EVIDENCE=""
      sleep 2
    done
    if [ -z "$BOOTSTRAP_WORKER_EVIDENCE" ]; then
      echo "endpoint worker did not publish bounded ready identity" >&2
      return 1
    fi
  fi
  export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=false
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
    BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=true
  fi
  export BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON
  export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS
  export BOOTSTRAP_SERVICE_RESTART_COUNT BOOTSTRAP_SERVICE_START_COUNT
  export BOOTSTRAP_SERVICE_STOP_COUNT BOOTSTRAP_SERVICE_ENABLE_COUNT
  bootstrap_candidate_action journal-advance service_verified
  bootstrap_candidate_action journal-advance committed

  BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_COMPLETED_NS
  bootstrap_provider_exec - <<'__CLIO_RELAY_RECONCILE_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    JarvisStateEvidence,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_was_active = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"] == "true"
queue = json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"])
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
worker = json.loads(worker_text) if worker_text else None
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_was_active,
    service_was_enabled=(True if desired.worker_service is not None else None),
    queue_evidence=queue,
    worker_evidence=worker,
)
if not inspection.exact_match:
    raise SystemExit(
        "reconciled generation did not pass exact inspection: " + repr(inspection.reasons)
    )
install_receipt = load_install_receipt()
plan = json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])
plan_duration = (
    int(os.environ["BOOTSTRAP_PLAN_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_PLAN_STARTED_NS"])
) / 1_000_000_000
prepare_duration = (
    int(os.environ["BOOTSTRAP_PREPARE_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_PREPARE_STARTED_NS"])
) / 1_000_000_000
components = {{}}
for name, action in plan["component_actions"].items():
    artifact = install_receipt.component_artifacts.get(name)
    observed = (
        artifact.model_dump(mode="json")
        if artifact is not None
        else {{"identity": install_receipt.components.get(name)}}
    )
    receipt_action = (
        "replaced"
        if action == "replace" and plan["mode"] == "component-upgrade"
        else ("prepared" if action == "replace" else "reused")
    )
    components[name] = {{
        "action": receipt_action,
        "observed_identity": observed,
        "duration_seconds": prepare_duration if action == "replace" else plan_duration,
    }}
for name in ("frp", "uv", "jarvis-util"):
    components.setdefault(
        name,
        {{
            "action": "reused",
            "observed_identity": {{"identity": install_receipt.components.get(name)}},
            "duration_seconds": plan_duration,
        }},
    )
transaction = BootstrapTransactionJournal.load(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
started_ns = min(
    int(os.environ["BOOTSTRAP_PLAN_STARTED_NS"]),
    int(os.environ["BOOTSTRAP_PREPARE_STARTED_NS"]),
)
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
duration = (completed_ns - started_ns) / 1_000_000_000
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="reconciled",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    active_generation=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
    components=components,
    duration_seconds=duration,
    downloads=[
        *(
            [{{"component": "clio-relay", "source": desired.relay_install_spec}}]
            if os.environ["BOOTSTRAP_RELAY_DOWNLOAD_COUNT"] == "1"
            else []
        ),
        *(
            [{{"component": "jarvis-cd", "source": desired.jarvis_cd_wheel_url}}]
            if os.environ["BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT"] == "1"
            else []
        ),
        *(
            [{{"component": "clio-kit", "source": desired.clio_kit_install_spec}}]
            if os.environ["BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT"] == "1"
            else []
        ),
    ],
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_state_before=JarvisStateEvidence(
        **{{
            **inspection.jarvis_state.model_dump(mode="json"),
            "config_sha256": os.environ["BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE"],
            "repos_sha256": os.environ["BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE"],
            "resource_graph_sha256": os.environ["BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE"],
        }}
    ),
    jarvis_repo_reconciliation=json.loads(
        os.environ["BOOTSTRAP_JARVIS_REPO_RECONCILIATION"]
    ),
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_RECONCILE_RECEIPT__
  trap - EXIT
  bootstrap_release_worker_lifetime_guard || true
  bootstrap_cleanup_preparing_root
}}

bootstrap_repair_transaction_exit() {{
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    echo "bootstrap readiness repair did not complete; queue migration state is retained" >&2
  fi
  bootstrap_restore_fenced_worker_on_failure "$status"
  bootstrap_release_worker_lifetime_guard 2>/dev/null || true
  bootstrap_cleanup_preparing_root || true
  exit "$status"
}}

bootstrap_reuse_repair() {{
  BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
  BOOTSTRAP_DESIRED_FINGERPRINT="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["desired_fingerprint"])'
  )"
  WORKER_SERVICE_NAME="$(
    python3 -c \
      'import json,os; value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'print(value["worker_service"] or "")'
  )"
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  BOOTSTRAP_PREVIOUS_GENERATION="legacy"
  if [ -L "$HOME/.local/share/clio-relay/current" ]; then
    BOOTSTRAP_PREVIOUS_GENERATION="$(bootstrap_active_generation_identity)"
  fi
  BOOTSTRAP_SERVICE_ACTIVE_BEFORE="unknown"
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
  if [ -n "$WORKER_SERVICE_NAME" ]; then
    if systemctl --user is-active --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=1
    else
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=0
    fi
    if systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ENABLED_BEFORE=1
    fi
  fi
  export BOOTSTRAP_INVOCATION_ID BOOTSTRAP_DESIRED_FINGERPRINT
  export BOOTSTRAP_TRANSACTION_JOURNAL BOOTSTRAP_PREVIOUS_GENERATION
  export BOOTSTRAP_SERVICE_ACTIVE_BEFORE BOOTSTRAP_SERVICE_ENABLED_BEFORE
  export WORKER_SERVICE_NAME
  mkdir -p "$BOOTSTRAP_TRANSACTION_ROOT"
  bootstrap_candidate_action journal-create
  bootstrap_candidate_action journal-advance inspected
  bootstrap_candidate_action journal-advance preparing
  bootstrap_candidate_action journal-advance prepared
  bootstrap_candidate_action journal-advance fencing

{worker_fence}

  bootstrap_candidate_action journal-advance fenced
  bootstrap_candidate_action journal-advance activating
  BOOTSTRAP_JARVIS_BINDING_REPAIR="$(
    bootstrap_candidate_action repair-managed-binding
  )"
  export BOOTSTRAP_JARVIS_BINDING_REPAIR
  BOOTSTRAP_JARVIS_BINDING_REPAIR_SHA256="$(
    printf '%s' "$BOOTSTRAP_JARVIS_BINDING_REPAIR" | sha256sum | awk '{{print $1}}'
  )"
  bootstrap_candidate_action journal-phase managed_repository_repaired \
    "$BOOTSTRAP_JARVIS_BINDING_REPAIR_SHA256"
  bootstrap_candidate_action journal-advance activated
  bootstrap_candidate_action journal-advance migration_started
  trap bootstrap_repair_transaction_exit EXIT
{worker_recheck}
  BOOTSTRAP_QUEUE_ACTION=verified_read_only
  BOOTSTRAP_QUEUE_DURATION_NS=0
  BOOTSTRAP_QUEUE_BEFORE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
  )"
  export BOOTSTRAP_QUEUE_BEFORE
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_QUEUE_BEFORE"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
    2>/dev/null; then
    BOOTSTRAP_QUEUE_ACTION=audited_and_sealed
    BOOTSTRAP_QUEUE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
    CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
    CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
    CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
    CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
    CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
    {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
    {init_command}
    BOOTSTRAP_QUEUE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
    BOOTSTRAP_QUEUE_DURATION_NS=$((BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS))
  fi
  bootstrap_candidate_action journal-advance migrated
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
  BOOTSTRAP_SERVICE_RESTART_COUNT=0
  BOOTSTRAP_SERVICE_START_COUNT=0
  BOOTSTRAP_SERVICE_STOP_COUNT=0
  BOOTSTRAP_SERVICE_ENABLE_COUNT=0
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    BOOTSTRAP_SERVICE_STOP_COUNT=1
    BOOTSTRAP_SERVICE_RESTART_COUNT=1
    bootstrap_candidate_action journal-advance starting
{worker_restart}
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  elif [ -n "$WORKER_SERVICE_NAME" ]; then
    if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
      echo "managed endpoint unit is unavailable; install it before bootstrap:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    bootstrap_candidate_action journal-advance starting
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready during repair" >&2
      return 1
    fi
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi
  BOOTSTRAP_QUEUE_EVIDENCE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
  BOOTSTRAP_WORKER_EVIDENCE=""
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
    for _BOOTSTRAP_READY_ATTEMPT in $(seq 1 90); do
      if BOOTSTRAP_WORKER_EVIDENCE="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          "$HOME/.local/bin/clio-relay" endpoint worker-info \
            --cluster "$WORKER_CLUSTER_NAME" --freshness-seconds 120 \
            --readiness-only 2>/dev/null
      )"; then
        export BOOTSTRAP_WORKER_EVIDENCE
        if python3 -c \
          'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_WORKER_EVIDENCE"]); '\
'sys.exit(0 if value.get("running") is True else 1)'; then
          break
        fi
      fi
      BOOTSTRAP_WORKER_EVIDENCE=""
      sleep 2
    done
    if [ -z "$BOOTSTRAP_WORKER_EVIDENCE" ]; then
      echo "endpoint worker did not publish bounded ready identity after repair" >&2
      return 1
    fi
  fi
  export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
  export BOOTSTRAP_SERVICE_ACTIVE_AFTER BOOTSTRAP_SERVICE_RESTART_COUNT
  export BOOTSTRAP_SERVICE_START_COUNT BOOTSTRAP_SERVICE_STOP_COUNT
  export BOOTSTRAP_SERVICE_ENABLE_COUNT
  export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS
  bootstrap_candidate_action journal-advance service_verified
  bootstrap_candidate_action journal-advance committed
  BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_COMPLETED_NS
  CURRENT_RELAY_PROVIDER="$BOOTSTRAP_CURRENT_PROVIDER"
  test -x "$CURRENT_RELAY_PROVIDER"
  "$CURRENT_RELAY_PROVIDER" - <<'__CLIO_RELAY_REPAIR_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    JarvisStateEvidence,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_active_after = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER"] == "1"
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_active_after,
    service_was_enabled=(True if desired.worker_service is not None else None),
    queue_evidence=json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    worker_evidence=json.loads(worker_text) if worker_text else None,
)
if not inspection.exact_match:
    raise SystemExit("readiness repair did not pass exact inspection: " + repr(inspection.reasons))
binding_repair = json.loads(os.environ["BOOTSTRAP_JARVIS_BINDING_REPAIR"])
if (
    binding_repair.get("schema_version") != "clio-relay.bootstrap-binding-repair.v1"
    or binding_repair.get("after") != inspection.jarvis_state.model_dump(mode="json")
    or not isinstance(binding_repair.get("before"), dict)
    or not isinstance(binding_repair.get("binding"), dict)
):
    raise SystemExit("readiness repair JARVIS binding evidence is invalid")
started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
duration = (completed_ns - started_ns) / 1_000_000_000
install_receipt = load_install_receipt()
transaction = BootstrapTransactionJournal.load(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="repaired",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    active_generation=install_receipt.generation or os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    duration_seconds=duration,
    downloads=[],
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_state_before=JarvisStateEvidence.model_validate(binding_repair["before"]),
    jarvis_repo_reconciliation=binding_repair["binding"],
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_REPAIR_RECEIPT__
  trap - EXIT
  bootstrap_release_worker_lifetime_guard || true
  bootstrap_cleanup_preparing_root
}}
"""


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
    script = f"""set -euo pipefail
umask 077
export PATH="$HOME/.local/bin:$PATH"
while IFS= read -r variable_name; do
  case "$variable_name" in
    LD_*|PYTHON*|BASH_ENV|ENV) unset "$variable_name" ;;
  esac
done < <(compgen -e)
export UV_TOOL_DIR="$HOME/.local/share/clio-relay/uv-tools"
export UV_TOOL_BIN_DIR="$HOME/.local/share/clio-relay/uv-bin"
export UV_PYTHON_INSTALL_DIR="$HOME/.local/share/clio-relay/uv-python"
while IFS= read -r variable_name; do
  case "$variable_name" in
    UV_TOOL_DIR|UV_TOOL_BIN_DIR|UV_PYTHON_INSTALL_DIR|UV_CACHE_DIR) ;;
    UV_*|PIP_*) unset "$variable_name" ;;
  esac
done < <(compgen -e)
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay"
bootstrap_active_generation_identity() {{
  local current target prefix identity
  current="$HOME/.local/share/clio-relay/current"
  if [ ! -L "$current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  target="$(readlink -f "$current")"
  prefix="$(readlink -f "$HOME/.local/share/clio-relay/generations")/"
  case "$target" in
    "$prefix"*) identity="${{target#"$prefix"}}" ;;
    *)
      echo "bootstrap current pointer does not name one managed generation" >&2
      return 1
      ;;
  esac
  case "$identity" in
    (*[!0-9a-f]*|'')
      echo "bootstrap current generation identity is invalid" >&2
      return 1
      ;;
  esac
  if [ "${{#identity}}" -ne 64 ]; then
    echo "bootstrap current generation identity has an invalid length" >&2
    return 1
  fi
  echo "$identity"
}}
bootstrap_active_generation_provider() {{
  local identity generations provider
  identity="$(bootstrap_active_generation_identity)" || return 1
  generations="$(readlink -f "$HOME/.local/share/clio-relay/generations")"
  provider="$generations/$identity/tools/clio-relay/bin/python"
  if [ ! -f "$provider" ] || [ ! -x "$provider" ]; then
    echo "bootstrap active generation provider is unavailable" >&2
    return 1
  fi
  echo "$provider"
}}
command -v flock >/dev/null 2>&1 || {{
  echo "flock is required to serialize clio-relay bootstrap" >&2
  exit 1
}}
if [ "${{CLIO_RELAY_BOOTSTRAP_LOCK_FD:-}}" != 9 ]; then
  python3 - "$0" <<'__CLIO_RELAY_BOOTSTRAP_LOCK_AND_REEXEC__'
import fcntl
import os
import stat
import sys
from pathlib import Path

directory = Path.home() / ".local/share/clio-relay"
directory_flags = os.O_RDONLY
for flag_name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
    directory_flags |= getattr(os, flag_name, 0)
try:
    directory_descriptor = os.open(directory, directory_flags)
except OSError as exc:
    raise SystemExit("bootstrap lock directory must be a real directory") from exc
if directory_descriptor == 9:
    replacement_descriptor = os.dup(directory_descriptor)
    os.close(directory_descriptor)
    directory_descriptor = replacement_descriptor
descriptor = None
try:
    opened_directory = os.fstat(directory_descriptor)
    linked_directory = directory.lstat()
    if (
        not stat.S_ISDIR(opened_directory.st_mode)
        or not stat.S_ISDIR(linked_directory.st_mode)
        or opened_directory.st_uid != os.getuid()
        or (opened_directory.st_dev, opened_directory.st_ino)
        != (linked_directory.st_dev, linked_directory.st_ino)
    ):
        raise SystemExit("bootstrap lock directory must be an owned real directory")
    if stat.S_IMODE(opened_directory.st_mode) != 0o700:
        os.fchmod(directory_descriptor, 0o700)
    repaired_directory = os.fstat(directory_descriptor)
    relinked_directory = directory.lstat()
    if (
        not stat.S_ISDIR(repaired_directory.st_mode)
        or not stat.S_ISDIR(relinked_directory.st_mode)
        or repaired_directory.st_uid != os.getuid()
        or stat.S_IMODE(repaired_directory.st_mode) != 0o700
        or (repaired_directory.st_dev, repaired_directory.st_ino)
        != (relinked_directory.st_dev, relinked_directory.st_ino)
    ):
        raise SystemExit("bootstrap lock directory could not be made owner-private")
    flags = os.O_RDWR | os.O_CREAT
    for flag_name in ("O_CLOEXEC", "O_NOFOLLOW"):
        flags |= getattr(os, flag_name, 0)
    descriptor = os.open("bootstrap.lock", flags, 0o600, dir_fd=directory_descriptor)
    opened = os.fstat(descriptor)
    linked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(linked.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        raise SystemExit("bootstrap lock must be one owned regular file")
    if stat.S_IMODE(opened.st_mode) != 0o600:
        os.fchmod(descriptor, 0o600)
    repaired = os.fstat(descriptor)
    relinked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(repaired.st_mode)
        or not stat.S_ISREG(relinked.st_mode)
        or repaired.st_nlink != 1
        or repaired.st_uid != os.getuid()
        or stat.S_IMODE(repaired.st_mode) != 0o600
        or (repaired.st_dev, repaired.st_ino) != (relinked.st_dev, relinked.st_ino)
    ):
        raise SystemExit("bootstrap lock could not be made owner-private")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise SystemExit("another clio-relay bootstrap is already running") from exc
    os.dup2(descriptor, 9, inheritable=True)
finally:
    os.close(directory_descriptor)
    if descriptor is not None and descriptor != 9:
        os.close(descriptor)
environment = dict(os.environ)
environment["CLIO_RELAY_BOOTSTRAP_LOCK_FD"] = "9"
script = str(Path(sys.argv[1]).resolve(strict=True))
os.execve("/bin/bash", ["bash", script], environment)
__CLIO_RELAY_BOOTSTRAP_LOCK_AND_REEXEC__
  exit $?
fi
python3 - <<'__CLIO_RELAY_BOOTSTRAP_LOCK_VERIFY__'
import fcntl
import os
import stat
from pathlib import Path

directory = Path.home() / ".local/share/clio-relay"
directory_flags = os.O_RDONLY
for flag_name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
    directory_flags |= getattr(os, flag_name, 0)
try:
    directory_descriptor = os.open(directory, directory_flags)
except OSError as exc:
    raise SystemExit("inherited bootstrap lock directory changed") from exc
opened = os.fstat(9)
try:
    opened_directory = os.fstat(directory_descriptor)
    linked_directory = directory.lstat()
    linked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISDIR(opened_directory.st_mode)
        or not stat.S_ISDIR(linked_directory.st_mode)
        or opened_directory.st_uid != os.getuid()
        or stat.S_IMODE(opened_directory.st_mode) != 0o700
        or (opened_directory.st_dev, opened_directory.st_ino)
        != (linked_directory.st_dev, linked_directory.st_ino)
        or not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(linked.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) != 0o600
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        raise SystemExit("inherited bootstrap lock identity changed")
    fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
finally:
    os.close(directory_descriptor)
__CLIO_RELAY_BOOTSTRAP_LOCK_VERIFY__
BOOTSTRAP_INVOCATION_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
BOOTSTRAP_INVOCATION_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
read -r BOOTSTRAP_PAYLOAD_TRANSFER_COUNT BOOTSTRAP_PAYLOAD_TRANSFER_BYTES < <(
  python3 - "$0" {rendered_source_archive} <<'__CLIO_RELAY_PAYLOAD_IDENTITY__'
import os
import stat
import sys
from pathlib import Path

total = 0
for value in sys.argv[1:]:
    path = Path(value)
    before = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(before.st_mode):
        raise SystemExit(f"bootstrap payload is not one regular file: {{path}}")
    total += before.st_size
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise SystemExit(f"bootstrap payload changed during inspection: {{path}}")
print(len(sys.argv) - 1, total)
__CLIO_RELAY_PAYLOAD_IDENTITY__
)
export BOOTSTRAP_INVOCATION_STARTED_AT BOOTSTRAP_INVOCATION_STARTED_NS
export BOOTSTRAP_PAYLOAD_TRANSFER_COUNT BOOTSTRAP_PAYLOAD_TRANSFER_BYTES
AGENT_NPM_PACKAGE={rendered_agent_npm_package}
AGENT_NPM_BIN={rendered_agent_npm_bin}
JARVIS_RESOURCE_GRAPH_PROFILE={rendered_jarvis_resource_graph_profile}
ALLOW_JARVIS_RESOURCE_GRAPH_BUILD={rendered_allow_jarvis_resource_graph_build}
AGENT_BIN=""
if [ -z "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"
fi
BOOTSTRAP_DESIRED_STATE={rendered_desired_state}
export BOOTSTRAP_DESIRED_STATE AGENT_NPM_PACKAGE AGENT_NPM_BIN AGENT_BIN
export JARVIS_RESOURCE_GRAPH_PROFILE ALLOW_JARVIS_RESOURCE_GRAPH_BUILD
bootstrap_journal_action() {{
  python3 - "$@" <<'__CLIO_RELAY_BOOTSTRAP_JOURNAL_ACTION__'
import base64

source = base64.b64decode(
    "{rendered_bootstrap_journal_source}",
    validate=True,
)
namespace = {{"__name__": "__main__", "__file__": "bootstrap_journal.py"}}
exec(compile(source, "bootstrap_journal.py", "exec"), namespace)
__CLIO_RELAY_BOOTSTRAP_JOURNAL_ACTION__
}}
bootstrap_path_set_identity() {{
  python3 - "$@" <<'__CLIO_RELAY_BOOTSTRAP_PATH_SET_IDENTITY__'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

evidence = []
for value in sys.argv[1:]:
    path = Path(value)
    details = path.lstat()
    if stat.S_ISREG(details.st_mode):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        identity = {{"kind": "file", "sha256": digest.hexdigest(), "size": details.st_size}}
    elif stat.S_ISLNK(details.st_mode):
        identity = {{"kind": "symlink", "target": os.readlink(path)}}
    elif stat.S_ISDIR(details.st_mode):
        identity = {{
            "kind": "directory",
            "device": details.st_dev,
            "inode": details.st_ino,
        }}
    else:
        raise SystemExit(f"bootstrap phase path has an unsupported type: {{path}}")
    evidence.append({{"path": str(path), "identity": identity}})
payload = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_BOOTSTRAP_PATH_SET_IDENTITY__
}}
BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
BOOTSTRAP_RECOVERY_REQUIRED=0
if [ -L "$BOOTSTRAP_TRANSACTION_JOURNAL" ]; then
  echo "bootstrap transaction journal must not be a symbolic link" >&2
  exit 1
elif [ -f "$BOOTSTRAP_TRANSACTION_JOURNAL" ]; then
  BOOTSTRAP_RECOVERY_REQUIRED="$(
    python3 - "$BOOTSTRAP_TRANSACTION_JOURNAL" \
      <<'__CLIO_RELAY_RECOVERY_REQUIRED__'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if path.stat().st_size > 1024 * 1024:
    raise SystemExit("bootstrap transaction journal exceeds its bound")
value = json.loads(path.read_text(encoding="utf-8"))
state = value.get("state") if isinstance(value, dict) else None
print("0" if state in {{"committed", "recovered"}} else "1")
__CLIO_RELAY_RECOVERY_REQUIRED__
  )"
fi
export BOOTSTRAP_TRANSACTION_JOURNAL BOOTSTRAP_RECOVERY_REQUIRED
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "1" ]; then
  BOOTSTRAP_EARLY_MODE="$(
    python3 - "$BOOTSTRAP_TRANSACTION_JOURNAL" \
      <<'__CLIO_RELAY_EARLY_RECOVERY_MODE__'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(value.get("mode", "legacy") if isinstance(value, dict) else "invalid")
__CLIO_RELAY_EARLY_RECOVERY_MODE__
  )"
  if [ "$BOOTSTRAP_EARLY_MODE" = "full" ]; then
    BOOTSTRAP_EARLY_RECOVERY_JSON="$(
      bootstrap_journal_action recovery-plan "$BOOTSTRAP_TRANSACTION_JOURNAL"
    )"
    export BOOTSTRAP_EARLY_RECOVERY_JSON
    read -r BOOTSTRAP_EARLY_DIRECTION BOOTSTRAP_EARLY_SERVICE \
      BOOTSTRAP_EARLY_SERVICE_ACTIVE < <(
        python3 - <<'__CLIO_RELAY_EARLY_RECOVERY_FIELDS__'
import json
import os

value = json.loads(os.environ["BOOTSTRAP_EARLY_RECOVERY_JSON"])
service = value.get("service_name") or "-"
active = value.get("service_was_active")
active_text = "true" if active is True else ("false" if active is False else "unknown")
print(value["recovery_mode"], service, active_text)
__CLIO_RELAY_EARLY_RECOVERY_FIELDS__
      )
    if [ "$BOOTSTRAP_EARLY_DIRECTION" = "discard" ]; then
      bootstrap_journal_action discard-full "$BOOTSTRAP_TRANSACTION_JOURNAL" "$HOME"
      if [ "$BOOTSTRAP_EARLY_SERVICE_ACTIVE" = "true" ] && \
         [ "$BOOTSTRAP_EARLY_SERVICE" != "-" ]; then
        command -v timeout >/dev/null 2>&1 || {{
          echo "timeout is required for bootstrap service recovery" >&2
          exit 1
        }}
        timeout 55 systemctl --user start "$BOOTSTRAP_EARLY_SERVICE"
      fi
      exec 9>&-
      unset CLIO_RELAY_BOOTSTRAP_LOCK_FD
      exec bash "$0"
    fi
  fi
fi
BOOTSTRAP_CURRENT_RELAY="$HOME/.local/bin/clio-relay"
BOOTSTRAP_CURRENT_PROVIDER=""
BOOTSTRAP_LEGACY_RELAY_PROVIDER=1
BOOTSTRAP_INSTALL_RECEIPT="$HOME/.local/share/clio-relay/install-receipt.json"
if [ -e "$BOOTSTRAP_INSTALL_RECEIPT" ] || [ -L "$BOOTSTRAP_INSTALL_RECEIPT" ]; then
  if BOOTSTRAP_RELAY_RECEIPT_CLASS="$(
    env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
      python3 -I - "$BOOTSTRAP_INSTALL_RECEIPT" <<'__CLIO_RELAY_RECEIPT_CLASSIFY__'
{_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE}
__CLIO_RELAY_RECEIPT_CLASSIFY__
  )"; then
    if [ "$BOOTSTRAP_RELAY_RECEIPT_CLASS" = "current" ]; then
      BOOTSTRAP_LEGACY_RELAY_PROVIDER=0
    fi
  else
    echo "bootstrap receipt classification failed; candidate payload is required" >&2
  fi
fi
if [ -x "$BOOTSTRAP_CURRENT_RELAY" ]; then
  BOOTSTRAP_CURRENT_PROVIDER="$(bootstrap_active_generation_provider 2>/dev/null || true)"
  if [ -z "$BOOTSTRAP_CURRENT_PROVIDER" ] && \
     [ -f "$UV_TOOL_DIR/clio-relay/bin/python" ] && \
     [ -x "$UV_TOOL_DIR/clio-relay/bin/python" ]; then
    BOOTSTRAP_CURRENT_PROVIDER="$UV_TOOL_DIR/clio-relay/bin/python"
  fi
fi
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "0" ] && \
   [ "$BOOTSTRAP_LEGACY_RELAY_PROVIDER" = "0" ] && \
   [ -x "$BOOTSTRAP_CURRENT_RELAY" ]; then
  if [ -x "$BOOTSTRAP_CURRENT_PROVIDER" ] && \
     "$BOOTSTRAP_CURRENT_PROVIDER" -c \
       'from clio_relay.bootstrap_reconcile import BootstrapDesiredState' \
       >/dev/null 2>&1; then
    BOOTSTRAP_SERVICE_WAS_ACTIVE="unknown"
    BOOTSTRAP_SERVICE_WAS_ENABLED="unknown"
    if [ -n {shlex.quote(worker_service or "")} ]; then
      if systemctl --user is-active --quiet {shlex.quote(worker_service or "")}; then
        BOOTSTRAP_SERVICE_WAS_ACTIVE="true"
      else
        BOOTSTRAP_SERVICE_WAS_ACTIVE="false"
      fi
      if systemctl --user is-enabled --quiet {shlex.quote(worker_service or "")}; then
        BOOTSTRAP_SERVICE_WAS_ENABLED="true"
      else
        BOOTSTRAP_SERVICE_WAS_ENABLED="false"
      fi
    fi
    BOOTSTRAP_QUEUE_EVIDENCE=""
    BOOTSTRAP_WORKER_EVIDENCE=""
    if command -v timeout >/dev/null 2>&1; then
      BOOTSTRAP_QUEUE_EVIDENCE="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          timeout 20 "$BOOTSTRAP_CURRENT_RELAY" queue readiness-info 2>/dev/null || true
      )"
      if [ "$BOOTSTRAP_SERVICE_WAS_ACTIVE" = "true" ]; then
        BOOTSTRAP_WORKER_EVIDENCE="$(
          CLIO_RELAY_CORE_DIR={rendered_core_dir} \
            timeout 20 "$BOOTSTRAP_CURRENT_RELAY" endpoint worker-info \
              --cluster {shlex.quote(cluster or "")} --freshness-seconds 120 \
              --readiness-only 2>/dev/null || true
        )"
      fi
    fi
    export BOOTSTRAP_SERVICE_WAS_ACTIVE BOOTSTRAP_SERVICE_WAS_ENABLED
    export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
    set +e
    BOOTSTRAP_NOOP_OUTPUT="$(
      "$BOOTSTRAP_CURRENT_PROVIDER" - {invocation_id!r} <<'__CLIO_RELAY_BOOTSTRAP_NOOP__'
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_value = os.environ["BOOTSTRAP_SERVICE_WAS_ACTIVE"]
service_was_active = (
    True if service_value == "true" else (False if service_value == "false" else None)
)
enabled_value = os.environ["BOOTSTRAP_SERVICE_WAS_ENABLED"]
service_was_enabled = (
    True if enabled_value == "true" else (False if enabled_value == "false" else None)
)

def optional_json(name: str):
    value = os.environ[name]
    return json.loads(value) if value else None

inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_was_active,
    service_was_enabled=service_was_enabled,
    queue_evidence=optional_json("BOOTSTRAP_QUEUE_EVIDENCE"),
    worker_evidence=optional_json("BOOTSTRAP_WORKER_EVIDENCE"),
)
if inspection.exact_match:
    completed_ns = time.monotonic_ns()
    started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
    receipt = make_bootstrap_receipt(
        invocation_id=sys.argv[1],
        desired=desired,
        outcome="verified_after_transfer",
        inspection=inspection,
        started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
        transaction=None,
        previous_generation=inspection.active_generation,
        active_generation=inspection.active_generation,
        duration_seconds=(completed_ns - started_ns) / 1_000_000_000,
        downloads=[],
        service_restart_count=0,
        payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
        payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
    )
    destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
    write_bootstrap_receipt(destination, receipt)
    print(f"bootstrap_receipt={{destination}}")
    print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
else:
    print("bootstrap_reconcile_reasons=" + json.dumps(inspection.reasons, sort_keys=True))
__CLIO_RELAY_BOOTSTRAP_NOOP__
    )"
    BOOTSTRAP_NOOP_STATUS=$?
    set -e
    if [ "$BOOTSTRAP_NOOP_STATUS" -ne 0 ]; then
      echo "$BOOTSTRAP_NOOP_OUTPUT" >&2
      exit "$BOOTSTRAP_NOOP_STATUS"
    fi
    echo "$BOOTSTRAP_NOOP_OUTPUT"
    if printf '%s\n' "$BOOTSTRAP_NOOP_OUTPUT" | \
       grep -q '^bootstrap_receipt_json='; then
      exit 0
    fi
  fi
fi
JARVIS_STATE_ROOT="$HOME/.ppi-jarvis"
JARVIS_CONFIG_FILE="$JARVIS_STATE_ROOT/jarvis_config.yaml"
JARVIS_REPOS_FILE="$JARVIS_STATE_ROOT/repos.yaml"
JARVIS_GRAPH_FILE="$JARVIS_STATE_ROOT/resource_graph.yaml"
export JARVIS_STATE_ROOT JARVIS_CONFIG_FILE JARVIS_REPOS_FILE JARVIS_GRAPH_FILE
JARVIS_EXISTING_FILE_COUNT="$(python3 - <<'__CLIO_RELAY_JARVIS_STATE_CLASSIFY__'
import os
import stat
from pathlib import Path

root = Path(os.environ["JARVIS_STATE_ROOT"])
try:
    root_details = root.lstat()
except FileNotFoundError:
    root_details = None
if root_details is not None and not stat.S_ISDIR(root_details.st_mode):
    raise SystemExit("JARVIS state root must be one real directory")
paths = [
    (Path(os.environ["JARVIS_CONFIG_FILE"]), 1024 * 1024),
    (Path(os.environ["JARVIS_REPOS_FILE"]), 4 * 1024 * 1024),
    (Path(os.environ["JARVIS_GRAPH_FILE"]), 64 * 1024 * 1024),
]
identities = []
count = 0
for path, maximum in paths:
    try:
        details = path.lstat()
    except FileNotFoundError:
        continue
    if not stat.S_ISREG(details.st_mode) or not 0 < details.st_size <= maximum:
        raise SystemExit(f"JARVIS state is not one bounded regular file: {{path}}")
    identities.append((details.st_dev, details.st_ino))
    count += 1
if len(set(identities)) != len(identities):
    raise SystemExit("JARVIS state files must not share one file identity")
print(count)
__CLIO_RELAY_JARVIS_STATE_CLASSIFY__
)"
if [ "$JARVIS_EXISTING_FILE_COUNT" -ne 0 ] && [ "$JARVIS_EXISTING_FILE_COUNT" -ne 3 ]; then
  echo "JARVIS state is partially initialized; refusing bootstrap mutation" >&2
  exit 1
fi
BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE=""
BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE=""
BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE=""
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  command -v timeout >/dev/null 2>&1 || {{
    echo "timeout is required for bounded candidate staging" >&2
    exit 1
  }}
  BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE="$(sha256sum "$JARVIS_CONFIG_FILE" | awk '{{print $1}}')"
  BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE="$(sha256sum "$JARVIS_REPOS_FILE" | awk '{{print $1}}')"
  BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE="$(sha256sum "$JARVIS_GRAPH_FILE" | awk '{{print $1}}')"
fi
export BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE
export BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE
export BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ] && \
   ! [ -x "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" ]; then
  echo "existing JARVIS state has no verifiable relay-managed interpreter" >&2
  exit 1
fi
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  export JARVIS_CONFIG_FILE
  "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" -I - <<'__CLIO_RELAY_JARVIS_ROOT_PROBE__'
import os
from pathlib import Path

import yaml

config_path = Path(os.environ["JARVIS_CONFIG_FILE"])
value = yaml.safe_load(config_path.read_text(encoding="utf-8"))
if not isinstance(value, dict):
    raise SystemExit("JARVIS configuration must contain one mapping")
for field in ("config_dir", "private_dir", "shared_dir"):
    observed = value.get(field)
    if not isinstance(observed, str):
        raise SystemExit(f"JARVIS {{field}} is missing")
    path = Path(observed).expanduser()
    if not path.is_absolute() or not path.resolve(strict=True).is_dir():
        raise SystemExit(f"JARVIS {{field}} is not one existing absolute directory")
print("jarvis_existing_roots=verified")
__CLIO_RELAY_JARVIS_ROOT_PROBE__
fi
{relay_only_reconcile}
BOOTSTRAP_PREPARING_PARENT="$HOME/.local/share/clio-relay/preparing"
BOOTSTRAP_PREPARING_ROOT="$BOOTSTRAP_PREPARING_PARENT/active"
export BOOTSTRAP_PREPARING_PARENT BOOTSTRAP_PREPARING_ROOT
mkdir -m 0700 -p "$BOOTSTRAP_PREPARING_PARENT"
env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
  python3 -I -c {preparing_root_program} \
  "$BOOTSTRAP_PREPARING_PARENT" "$BOOTSTRAP_PREPARING_ROOT" prepare
bootstrap_cleanup_preparing_root() {{
  env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
    python3 -I -c {preparing_root_program} \
    "$BOOTSTRAP_PREPARING_PARENT" "$BOOTSTRAP_PREPARING_ROOT" cleanup
}}
trap bootstrap_cleanup_preparing_root EXIT
BOOTSTRAP_CANDIDATE_PYTHON_ROOT="$BOOTSTRAP_PREPARING_ROOT/candidate-python"
BOOTSTRAP_CANDIDATE_PACKAGE="$BOOTSTRAP_CANDIDATE_PYTHON_ROOT/clio_relay"
if [ -L "$BOOTSTRAP_CANDIDATE_PYTHON_ROOT" ] || \
   [ -L "$BOOTSTRAP_CANDIDATE_PACKAGE" ]; then
  echo "bootstrap candidate package root must not be a symbolic link" >&2
  exit 1
fi
mkdir -m 0700 -p "$BOOTSTRAP_CANDIDATE_PACKAGE"
python3 -I - "$BOOTSTRAP_CANDIDATE_PACKAGE" <<'__CLIO_RELAY_CANDIDATE_PACKAGE__'
import base64
import hashlib
import json
import os
import sys
from pathlib import Path

destination = Path(sys.argv[1])
encoded_sources = json.loads(r'''{rendered_candidate_package_sources}''')
if not isinstance(encoded_sources, dict):
    raise SystemExit("bootstrap candidate source manifest is invalid")
sources = {{
    name: base64.b64decode(encoded, validate=True)
    for name, encoded in encoded_sources.items()
}}
for name, payload in sources.items():
    path = destination / name
    if path.is_symlink():
        raise SystemExit(f"bootstrap candidate source must not be a symbolic link: {{name}}")
    try:
        observed = path.read_bytes()
    except FileNotFoundError:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, 0o600)
        observed = payload
    if observed != payload:
        raise SystemExit(f"bootstrap candidate source identity changed: {{name}}")
    print(f"bootstrap_candidate_source={{name}}:{{hashlib.sha256(observed).hexdigest()}}")
__CLIO_RELAY_CANDIDATE_PACKAGE__
BOOTSTRAP_CANDIDATE_RECONCILE="$BOOTSTRAP_CANDIDATE_PACKAGE/bootstrap_reconcile.py"
BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO="$BOOTSTRAP_CANDIDATE_PACKAGE/bootstrap_provider_build_info.py"
BOOTSTRAP_CANDIDATE_PROCESS_CONTAINMENT="$BOOTSTRAP_CANDIDATE_PACKAGE/process_containment.py"
echo "{candidate_reconcile_sha256} *$BOOTSTRAP_CANDIDATE_RECONCILE" | \
  sha256sum --check --strict -
echo "{candidate_provider_build_info_sha256} *$BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO" | \
  sha256sum --check --strict -
echo "{candidate_bounded_process_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/bounded_process.py" | \
  sha256sum --check --strict -
echo "{candidate_errors_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/errors.py" | \
  sha256sum --check --strict -
echo "{candidate_process_containment_sha256} *$BOOTSTRAP_CANDIDATE_PROCESS_CONTAINMENT" | \
  sha256sum --check --strict -
echo "{candidate_safe_archive_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/safe_archive.py" | \
  sha256sum --check --strict -
export BOOTSTRAP_CANDIDATE_PYTHON_ROOT BOOTSTRAP_CANDIDATE_RECONCILE
bootstrap_safe_extract() {{
  local provider="$1"
  local archive="$2"
  local destination="$3"
  bootstrap_safe_extract_provider() {{
    if [ "${{BOOTSTRAP_CANDIDATE_PROVIDER_READY:-0}}" = "1" ] && \
       [ "$provider" = "${{BOOTSTRAP_PLAN_PROVIDER:-}}" ]; then
      bootstrap_provider_exec "$@"
    else
      "$provider" -I "$@"
    fi
  }}
  bootstrap_safe_extract_provider - "$BOOTSTRAP_CANDIDATE_PYTHON_ROOT" \
    "$archive" "$destination" \
      <<'__CLIO_RELAY_SAFE_EXTRACT__'
import json
import sys
from pathlib import Path

candidate_root, archive_value, destination_value = sys.argv[1:]
sys.path.insert(0, candidate_root)
from clio_relay.safe_archive import safe_extract_tar

receipt = safe_extract_tar(Path(archive_value), Path(destination_value))
print(
    "bootstrap_archive_extraction="
    + json.dumps(
        {{
            "archive_bytes": receipt.archive_bytes,
            "destination": str(receipt.destination),
            "directory_count": receipt.directory_count,
            "extracted_bytes": receipt.extracted_bytes,
            "member_count": receipt.member_count,
            "regular_file_count": receipt.regular_file_count,
        }},
        sort_keys=True,
        separators=(",", ":"),
    )
)
__CLIO_RELAY_SAFE_EXTRACT__
}}
BOOTSTRAP_PLAN_MODE="full"
BOOTSTRAP_PLAN_JSON=""
BOOTSTRAP_RECOVERY_PROVIDER=""
BOOTSTRAP_CANDIDATE_PROVIDER_READY=0
if [ -x "$BOOTSTRAP_CURRENT_PROVIDER" ]; then
  BOOTSTRAP_RECOVERY_PROVIDER="$BOOTSTRAP_CURRENT_PROVIDER"
elif [ -x "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" ]; then
  BOOTSTRAP_RECOVERY_PROVIDER="$HOME/.local/share/clio-relay/jarvis-venv/bin/python"
fi
export BOOTSTRAP_RECOVERY_PROVIDER BOOTSTRAP_CANDIDATE_PROVIDER_READY
export BOOTSTRAP_CANDIDATE_RECONCILE
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "1" ]; then
  if [ -z "$BOOTSTRAP_RECOVERY_PROVIDER" ]; then
    echo "bootstrap recovery has no trusted installed Python provider" >&2
    exit 1
  fi
  env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
    "$BOOTSTRAP_RECOVERY_PROVIDER" -I "$BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO" \
      "$BOOTSTRAP_CANDIDATE_PACKAGE"
  bootstrap_recover_previous_transaction
  exec 9>&-
  unset CLIO_RELAY_BOOTSTRAP_LOCK_FD
  bootstrap_cleanup_preparing_root
  trap - EXIT
  exec bash "$0"
fi
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  SOURCE_ARCHIVE={rendered_source_archive}
  SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
  if [ -z "$SOURCE_ARCHIVE_SHA256" ]; then
    echo "retained-state reconcile requires a verified source archive digest" >&2
    exit 1
  fi
  echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
  BOOTSTRAP_CANDIDATE_SOURCE_ROOT="$BOOTSTRAP_PREPARING_ROOT/source"
  bootstrap_safe_extract python3 "$SOURCE_ARCHIVE" "$BOOTSTRAP_CANDIDATE_SOURCE_ROOT"

  BOOTSTRAP_CANDIDATE_INSTALL_SPEC={rendered_candidate_relay_install_spec}
  BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
  if [ -z "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" ]; then
    echo "retained-state reconcile requires an exact relay wheel SHA-256" >&2
    exit 1
  fi
  BOOTSTRAP_CANDIDATE_ARTIFACT=""
  case "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" in
    '$DEST/'*)
      BOOTSTRAP_CANDIDATE_ARTIFACT="$(
        python3 -I - "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" \
          "$BOOTSTRAP_CANDIDATE_SOURCE_ROOT" <<'__CLIO_RELAY_CANDIDATE_WHEEL_PATH__'
import sys
from pathlib import Path

specification, source_value = sys.argv[1:]
if not specification.startswith("$DEST/"):
    raise SystemExit("transported relay wheel did not use the archive destination")
source = Path(source_value).resolve(strict=True)
lexical_candidate = source / specification.removeprefix("$DEST/")
candidate_details = lexical_candidate.lstat()
if lexical_candidate.is_symlink() or not lexical_candidate.is_file():
    raise SystemExit("transported relay wheel is not one regular file")
candidate = lexical_candidate.resolve(strict=True)
if candidate == source or not candidate.is_relative_to(source):
    raise SystemExit("transported relay wheel escaped the verified source archive")
observed = lexical_candidate.lstat()
identity = lambda value: (
    value.st_dev,
    value.st_ino,
    value.st_mode,
    value.st_size,
    value.st_mtime_ns,
    value.st_ctime_ns,
)
if identity(observed) != identity(candidate_details) or candidate.suffix != ".whl":
    raise SystemExit("transported relay wheel is not one regular wheel file")
print(candidate)
__CLIO_RELAY_CANDIDATE_WHEEL_PATH__
      )"
      ;;
    clio-relay==*)
      BOOTSTRAP_CANDIDATE_ARTIFACT="$(
        timeout --signal=TERM --kill-after=5s 180 \
          python3 -I - "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" \
          "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
          "$BOOTSTRAP_PREPARING_ROOT" <<'__CLIO_RELAY_CANDIDATE_PYPI_WHEEL__'
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit
from urllib.request import urlopen

specification, expected_sha256, destination_root_value = sys.argv[1:]
version = specification.removeprefix("clio-relay==")
if not version or specification != f"clio-relay=={{version}}":
    raise SystemExit("candidate PyPI install spec is not exact")
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    metadata = response.read(4 * 1024 * 1024 + 1)
if len(metadata) > 4 * 1024 * 1024:
    raise SystemExit("candidate PyPI metadata exceeds its bound")
document = json.loads(metadata)
expected_filename = f"clio_relay-{{version}}-py3-none-any.whl"
matches = [
    item
    for item in document.get("urls", [])
    if isinstance(item, dict)
    and item.get("filename") == expected_filename
    and item.get("packagetype") == "bdist_wheel"
    and item.get("digests", {{}}).get("sha256") == expected_sha256
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return the exact digest-pinned relay wheel")
filename = matches[0].get("filename")
if (
    not isinstance(filename, str)
    or not filename.endswith(".whl")
    or Path(filename).name != filename
    or any(character in filename for character in "\\x00\\r\\n")
):
    raise SystemExit("PyPI relay wheel filename is unsafe")
url = matches[0].get("url")
if not isinstance(url, str):
    raise SystemExit("PyPI relay wheel URL is missing")
parsed = urlsplit(url)
if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(
    ".pythonhosted.org"
):
    raise SystemExit("PyPI relay wheel URL has an unsupported origin")
if PurePosixPath(unquote(parsed.path)).name != filename:
    raise SystemExit("PyPI relay wheel URL does not preserve its verified filename")
destination_root = Path(destination_root_value)
if (
    not destination_root.is_absolute()
    or destination_root.is_symlink()
    or not destination_root.is_dir()
):
    raise SystemExit("candidate relay wheel destination root is unsafe")
destination_root = destination_root.resolve(strict=True)
destination = destination_root / filename
if destination.parent != destination_root:
    raise SystemExit("candidate relay wheel destination escaped its private root")
digest = hashlib.sha256()
size = 0
with urlopen(url, timeout=60) as response, destination.open("xb") as stream:
    while chunk := response.read(1024 * 1024):
        size += len(chunk)
        if size > 256 * 1024 * 1024:
            raise SystemExit("candidate relay wheel exceeds its bound")
        digest.update(chunk)
        stream.write(chunk)
if size < 1 or digest.hexdigest() != expected_sha256:
    destination.unlink(missing_ok=True)
    raise SystemExit("downloaded candidate relay wheel did not match its digest")
print(destination)
__CLIO_RELAY_CANDIDATE_PYPI_WHEEL__
      )"
      ;;
    *)
      echo "retained-state reconcile supports only an exact released relay wheel" >&2
      exit 1
      ;;
  esac
  echo "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256 *$BOOTSTRAP_CANDIDATE_ARTIFACT" | \
    sha256sum --check --strict -

  BOOTSTRAP_PINNED_UV_SOURCE="$HOME/.local/bin/uv"
  BOOTSTRAP_PINNED_UV="$(
    env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
      python3 -I -c {pinned_uv_copy_program} \
      "$BOOTSTRAP_PINNED_UV_SOURCE" "$BOOTSTRAP_PREPARING_ROOT" \
      {UV_LINUX_AMD64_EXECUTABLE_SHA256}
  )"
  BOOTSTRAP_CANDIDATE_TOOL_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-tools"
  BOOTSTRAP_CANDIDATE_BIN_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-bin"
  BOOTSTRAP_CANDIDATE_CACHE_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-cache"
  BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR="$HOME/.local/share/clio-relay/uv-python"
  mkdir -m 0700 "$BOOTSTRAP_CANDIDATE_CACHE_DIR"
  BOOTSTRAP_CANDIDATE_RELAY="$BOOTSTRAP_CANDIDATE_BIN_DIR/clio-relay"
  BOOTSTRAP_PLAN_PROVIDER="$BOOTSTRAP_CANDIDATE_TOOL_DIR/clio-relay/bin/python"
  export BOOTSTRAP_PLAN_PROVIDER BOOTSTRAP_CANDIDATE_RECONCILE
  export BOOTSTRAP_CANDIDATE_SOURCE_ROOT BOOTSTRAP_CANDIDATE_ARTIFACT
  export BOOTSTRAP_CANDIDATE_TOOL_DIR BOOTSTRAP_CANDIDATE_BIN_DIR
  export BOOTSTRAP_CANDIDATE_CACHE_DIR BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR
  export BOOTSTRAP_CANDIDATE_RELAY BOOTSTRAP_PINNED_UV SOURCE_ARCHIVE_SHA256
  BOOTSTRAP_PLAN_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_PLAN_CAPTURE="$(
    python3 -I -c {candidate_uv_install_program} \
      install-verify-and-exec \
      "$BOOTSTRAP_PINNED_UV" {UV_LINUX_AMD64_EXECUTABLE_SHA256} \
      "$BOOTSTRAP_CANDIDATE_ARTIFACT" "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
      "$BOOTSTRAP_CANDIDATE_TOOL_DIR" "$BOOTSTRAP_CANDIDATE_BIN_DIR" \
      "$BOOTSTRAP_CANDIDATE_CACHE_DIR" \
      "$BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR" -I - \
      <<'__CLIO_RELAY_RECONCILE_PLAN__'
import json
import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    plan_bootstrap_reconcile,
    prove_bootstrap_replacement_provider,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
evidence = prove_bootstrap_replacement_provider(
    desired,
    uv_executable=Path(os.environ["BOOTSTRAP_PINNED_UV"]),
    tool_executable=Path(os.environ["BOOTSTRAP_CANDIDATE_RELAY"]),
    source_artifact=Path(os.environ["BOOTSTRAP_CANDIDATE_ARTIFACT"]),
    tool_directory=Path(os.environ["BOOTSTRAP_CANDIDATE_TOOL_DIR"]),
    tool_bin_directory=Path(os.environ["BOOTSTRAP_CANDIDATE_BIN_DIR"]),
    preparing_root=Path(os.environ["BOOTSTRAP_PREPARING_ROOT"]),
    extracted_source_root=Path(os.environ["BOOTSTRAP_CANDIDATE_SOURCE_ROOT"]),
    source_archive_sha256=os.environ["SOURCE_ARCHIVE_SHA256"],
    expected_provider_interpreter_sha256=os.environ["BOOTSTRAP_PLAN_PROVIDER_SHA256"],
)
plan = plan_bootstrap_reconcile(desired, replacement_provider=evidence)
print("bootstrap_candidate_provider_sha256=" + os.environ["BOOTSTRAP_PLAN_PROVIDER_SHA256"])
print(json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_RECONCILE_PLAN__
  )"
  BOOTSTRAP_PLAN_JSON=""
  BOOTSTRAP_CANDIDATE_PROVIDER_SHA256=""
  while IFS= read -r bootstrap_plan_line; do
    case "$bootstrap_plan_line" in
      bootstrap_candidate_provider_sha256=*)
        BOOTSTRAP_CANDIDATE_PROVIDER_SHA256="${{bootstrap_plan_line#*=}}"
        ;;
      '{{'*'}}')
        if [ -n "$BOOTSTRAP_PLAN_JSON" ]; then
          echo "candidate planner returned multiple plan objects" >&2
          exit 1
        fi
        BOOTSTRAP_PLAN_JSON="$bootstrap_plan_line"
        ;;
      *)
        echo "candidate planner returned unrecognized output" >&2
        exit 1
        ;;
    esac
  done <<< "$BOOTSTRAP_PLAN_CAPTURE"
  if [ "${{#BOOTSTRAP_CANDIDATE_PROVIDER_SHA256}}" -ne 64 ]; then
    echo "candidate planner omitted its pinned provider digest" >&2
    exit 1
  fi
  case "$BOOTSTRAP_CANDIDATE_PROVIDER_SHA256" in
    *[!0-9a-f]*) echo "candidate planner provider digest is invalid" >&2; exit 1 ;;
  esac
  BOOTSTRAP_CANDIDATE_PROVIDER_READY=1
  export BOOTSTRAP_CANDIDATE_PROVIDER_READY BOOTSTRAP_CANDIDATE_PROVIDER_SHA256
  BOOTSTRAP_PLAN_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_PLAN_JSON
  BOOTSTRAP_PLAN_MODE="$(
    python3 -c 'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["mode"])'
  )"
fi
export BOOTSTRAP_PLAN_MODE BOOTSTRAP_PLAN_JSON BOOTSTRAP_PLAN_STARTED_NS \
  BOOTSTRAP_PLAN_COMPLETED_NS
if [ "$BOOTSTRAP_PLAN_MODE" = "repair" ]; then
  bootstrap_reuse_repair
  exit 0
fi
if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ] || \
   [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
  bootstrap_relay_only_reconcile
  exit 0
fi
if [ "$BOOTSTRAP_PLAN_MODE" = "full" ] && \
   {{ [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ] || \
      [ -e "$HOME/.local/share/clio-relay/jarvis-venv" ]; }}; then
  printf '%s\\n' "bootstrap_reconcile_plan=$BOOTSTRAP_PLAN_JSON" >&2
  echo "full component reconcile requires a staged generation;" \
    "refusing to clear the retained legacy JARVIS execution environment" >&2
  exit 1
fi
if [ "$BOOTSTRAP_PLAN_MODE" = "full" ] && \
   [ "$JARVIS_EXISTING_FILE_COUNT" -eq 0 ] && \
   [ -z "$JARVIS_RESOURCE_GRAPH_PROFILE" ]; then
  echo "fresh bootstrap requires an operator-selected JARVIS resource graph profile" >&2
  exit 1
fi
BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
BOOTSTRAP_DESIRED_FINGERPRINT="$(
  python3 - <<'__CLIO_RELAY_FRESH_DESIRED_FINGERPRINT__'
import hashlib
import json
import os

value = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
value["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
value["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
payload = json.dumps(
    value,
    ensure_ascii=True,
    allow_nan=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_DESIRED_FINGERPRINT__
)"
WORKER_SERVICE_NAME="$(
  python3 - <<'__CLIO_RELAY_FRESH_WORKER_SERVICE__'
import json
import os

print(json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])["worker_service"] or "")
__CLIO_RELAY_FRESH_WORKER_SERVICE__
)"
BOOTSTRAP_SERVICE_ACTIVE_BEFORE=unknown
BOOTSTRAP_SERVICE_ENABLED_BEFORE=unknown
if [ -n "$WORKER_SERVICE_NAME" ]; then
  if systemctl --user is-active --quiet "$WORKER_SERVICE_NAME"; then
    BOOTSTRAP_SERVICE_ACTIVE_BEFORE=true
  else
    BOOTSTRAP_SERVICE_ACTIVE_BEFORE=false
  fi
  if systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
    BOOTSTRAP_SERVICE_ENABLED_BEFORE=true
  else
    BOOTSTRAP_SERVICE_ENABLED_BEFORE=false
  fi
fi
BOOTSTRAP_GENERATION="$HOME/.local/share/clio-relay/generations/$BOOTSTRAP_DESIRED_FINGERPRINT"
BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
BOOTSTRAP_OWNED_PATHS_JSON="$(
  python3 - "$BOOTSTRAP_DESIRED_FINGERPRINT" "$BOOTSTRAP_INVOCATION_ID" \
    <<'__CLIO_RELAY_FRESH_OWNERSHIP__'
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

home = Path.home()
fingerprint = sys.argv[1]
invocation_id = sys.argv[2]

def classify(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None

def require_regular_executable(path: Path, expected_sha256: str | None = None) -> None:
    details = classify(path)
    if details is None or not stat.S_ISREG(details.st_mode) or not os.access(path, os.X_OK):
        raise SystemExit(f"bootstrap cannot adopt an existing executable: {{path}}")
    if expected_sha256 is not None:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected_sha256:
            raise SystemExit(f"bootstrap existing executable digest changed: {{path}}")

owned: dict[str, dict[str, str]] = {{}}

def absent(name: str, path: Path, kind: str) -> None:
    if classify(path) is not None:
        raise SystemExit(f"fresh bootstrap refuses a preexisting mutation target: {{path}}")
    owned[name] = {{"path": str(path), "kind": kind}}

frpc = home / ".local/bin/frpc"
frps = home / ".local/bin/frps"
if classify(frpc) is None and classify(frps) is None:
    owned["frpc"] = {{"path": str(frpc), "kind": "file"}}
    owned["frps"] = {{"path": str(frps), "kind": "file"}}
else:
    require_regular_executable(frpc, "{FRPC_LINUX_AMD64_SHA256}")
    require_regular_executable(frps, "{FRPS_LINUX_AMD64_SHA256}")

uv = home / ".local/bin/uv"
uvx = home / ".local/bin/uvx"
if classify(uv) is None and classify(uvx) is None:
    owned["uv"] = {{"path": str(uv), "kind": "file"}}
    owned["uvx"] = {{"path": str(uvx), "kind": "file"}}
else:
    require_regular_executable(uv, "{UV_LINUX_AMD64_EXECUTABLE_SHA256}")
    require_regular_executable(uvx)
    completed = subprocess.run(
        [str(uv), "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0 or completed.stdout.strip() != "uv {UV_VERSION}":
        raise SystemExit("bootstrap cannot adopt an existing uv version")

jarvis_util = home / ".local/src/jarvis-util"
if classify(jarvis_util) is None:
    owned["jarvis_util"] = {{"path": str(jarvis_util), "kind": "directory"}}
else:
    if jarvis_util.is_symlink() or not (jarvis_util / ".git").is_dir():
        raise SystemExit("bootstrap cannot adopt the existing jarvis-util path")
    commit = subprocess.run(
        ["git", "-C", str(jarvis_util), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "-C", str(jarvis_util), "status", "--porcelain=v1", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout
    if commit != "{JARVIS_UTIL_COMMIT}" or status:
        raise SystemExit("bootstrap cannot mutate an existing jarvis-util checkout")

for name, path, kind in (
    ("jarvis_venv", home / ".local/share/clio-relay/jarvis-venv", "directory"),
    ("clio_kit_wheels", home / ".local/share/clio-relay/component-wheels/clio-kit", "directory"),
    ("jarvis_cd_wheels", home / ".local/share/clio-relay/component-wheels/jarvis-cd", "directory"),
    ("uv_tools", home / ".local/share/clio-relay/uv-tools", "directory"),
    ("uv_bin", home / ".local/share/clio-relay/uv-bin", "directory"),
    ("uv_python", home / ".local/share/clio-relay/uv-python", "directory"),
    (
        "transaction_root",
        home / ".local/share/clio-relay/transactions" / invocation_id,
        "directory",
    ),
    ("relay_source", home / ".local/src/clio-relay", "symlink"),
    ("jarvis_state", home / ".ppi-jarvis", "directory"),
    ("jarvis_config", home / ".local/share/clio-relay/jarvis-config", "directory"),
    ("jarvis_private", home / ".local/share/clio-relay/jarvis-private", "directory"),
    ("jarvis_shared", home / ".local/share/clio-relay/jarvis-shared", "directory"),
    ("generation", home / ".local/share/clio-relay/generations" / fingerprint, "directory"),
    ("current", home / ".local/share/clio-relay/current", "symlink"),
    (
        "install_receipt",
        home / ".local/share/clio-relay/install-receipt.json",
        "symlink",
    ),
    ("managed_repo", home / ".local/share/clio-relay/clio_relay", "symlink"),
    ("relay_launcher", home / ".local/bin/clio-relay", "symlink"),
    ("jarvis_launcher", home / ".local/bin/jarvis", "symlink"),
):
    absent(name, path, kind)

print(json.dumps(owned, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_FRESH_OWNERSHIP__
)"
export BOOTSTRAP_INVOCATION_ID BOOTSTRAP_DESIRED_FINGERPRINT
export BOOTSTRAP_GENERATION BOOTSTRAP_TRANSACTION_ROOT WORKER_SERVICE_NAME
export BOOTSTRAP_SERVICE_ACTIVE_BEFORE BOOTSTRAP_SERVICE_ENABLED_BEFORE
bootstrap_journal_action create \
  "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  "$BOOTSTRAP_INVOCATION_ID" \
  "$BOOTSTRAP_DESIRED_FINGERPRINT" \
  full \
  "$WORKER_SERVICE_NAME" \
  "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" \
  "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" \
  "$BOOTSTRAP_OWNED_PATHS_JSON"
BOOTSTRAP_OWNERSHIP_IDENTITY="$(
  BOOTSTRAP_OWNED_PATHS_JSON="$BOOTSTRAP_OWNED_PATHS_JSON" \
    python3 - <<'__CLIO_RELAY_FRESH_OWNERSHIP_IDENTITY__'
import hashlib
import json
import os

value = json.loads(os.environ["BOOTSTRAP_OWNED_PATHS_JSON"])
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_OWNERSHIP_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  ownership_manifest "$BOOTSTRAP_OWNERSHIP_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" inspected
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" fencing
{worker_fence}
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" fenced
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" preparing
mkdir -m 0700 -p \
  "$HOME/.local/share/clio-relay/transactions" \
  "$HOME/.local/share/clio-relay/component-wheels" \
  "$HOME/.local/share/clio-relay/generations"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" transaction_root
mkdir -m 0700 "$BOOTSTRAP_TRANSACTION_ROOT/downloads"
mkdir -m 0700 "$BOOTSTRAP_TRANSACTION_ROOT/uv-cache"
export UV_CACHE_DIR="$BOOTSTRAP_TRANSACTION_ROOT/uv-cache"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" generation
BOOTSTRAP_FULL_PREPARE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
BOOTSTRAP_FRP_DOWNLOADED=0
BOOTSTRAP_UV_DOWNLOADED=0
BOOTSTRAP_JARVIS_UTIL_DOWNLOADED=0
BOOTSTRAP_JARVIS_CD_DOWNLOADED=0
BOOTSTRAP_CLIO_KIT_DOWNLOADED=0
BOOTSTRAP_RELAY_DOWNLOAD_COUNT=0

cd "$BOOTSTRAP_TRANSACTION_ROOT/downloads"
FRP_VERSION="{frp_version}"
FRP_SHA256="{FRP_LINUX_AMD64_SHA256}"
FRPC_SHA256="{FRPC_LINUX_AMD64_SHA256}"
FRPS_SHA256="{FRPS_LINUX_AMD64_SHA256}"
ARCHIVE="frp_${{FRP_VERSION}}_linux_amd64.tar.gz"
if [ ! -x "$HOME/.local/bin/frpc" ] \
  || [ ! -x "$HOME/.local/bin/frps" ] \
  || ! echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --status - \
  || ! echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --status -; then
  curl -L --fail --retry 3 -o "$ARCHIVE" \
    "https://github.com/fatedier/frp/releases/download/v${{FRP_VERSION}}/${{ARCHIVE}}"
  echo "$FRP_SHA256 *$ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$ARCHIVE"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frpc" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frpc.install"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frps" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frps.install"
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" frpc \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frpc.install" 0755
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" frps \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frps.install" 0755
  echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --strict -
  echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --strict -
  BOOTSTRAP_FRP_DOWNLOADED=1
fi

UV_VERSION="{UV_VERSION}"
UV_ARCHIVE_SHA256="{UV_LINUX_AMD64_ARCHIVE_SHA256}"
UV_EXECUTABLE_SHA256="{UV_LINUX_AMD64_EXECUTABLE_SHA256}"
UV_ARCHIVE="uv-x86_64-unknown-linux-gnu.tar.gz"
if [ ! -x "$HOME/.local/bin/uv" ] \
  || [ "$("$HOME/.local/bin/uv" --version | awk '{{print $1 " " $2}}')" != "uv $UV_VERSION" ] \
  || ! echo "$UV_EXECUTABLE_SHA256 *$HOME/.local/bin/uv" | \
       sha256sum --check --status -; then
  curl -L --fail --retry 3 -o "$UV_ARCHIVE" \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"
  echo "$UV_ARCHIVE_SHA256 *$UV_ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$UV_ARCHIVE"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uv" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uv.install"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uvx" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uvx.install"
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uv \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uv.install" 0755
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uvx \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uvx.install" 0755
  echo "$UV_EXECUTABLE_SHA256 *$HOME/.local/bin/uv" | sha256sum --check --strict -
  BOOTSTRAP_UV_DOWNLOADED=1
fi
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uv_python
uv python install 3.12

if [ ! -x "$AGENT_BIN" ] && [ -n "$AGENT_NPM_PACKAGE" ] && command -v npm >/dev/null 2>&1; then
  npm install -g "$AGENT_NPM_PACKAGE"
fi

JARVIS_VENV="$HOME/.local/share/clio-relay/jarvis-venv"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_venv
uv venv --python 3.12 --seed "$JARVIS_VENV"
. "$JARVIS_VENV/bin/activate"
JARVIS_UTIL_COMMIT="{JARVIS_UTIL_COMMIT}"
if [ ! -d "$HOME/.local/src/jarvis-util/.git" ]; then
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_util
  git clone --no-checkout https://github.com/grc-iit/jarvis-util.git \
    "$HOME/.local/src/jarvis-util"
  git -C "$HOME/.local/src/jarvis-util" fetch --depth 1 origin "$JARVIS_UTIL_COMMIT"
  BOOTSTRAP_JARVIS_UTIL_DOWNLOADED=1
  git -C "$HOME/.local/src/jarvis-util" checkout --detach "$JARVIS_UTIL_COMMIT"
else
  test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = \
    "$JARVIS_UTIL_COMMIT"
  test -z "$(
    git -C "$HOME/.local/src/jarvis-util" status --porcelain=v1 --untracked-files=all
  )"
fi
test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = "$JARVIS_UTIL_COMMIT"
python -m pip install --isolated --index-url https://pypi.org/simple \\
  -r "$HOME/.local/src/jarvis-util/requirements.txt"
python -m pip install --isolated --no-deps "$HOME/.local/src/jarvis-util"
JARVIS_CD_VERSION="{JARVIS_CD_VERSION}"
JARVIS_CD_WHEEL_URL="{JARVIS_CD_WHEEL_URL}"
JARVIS_CD_WHEEL_SHA256="{JARVIS_CD_WHEEL_SHA256}"
JARVIS_CD_WHEEL_DIR="$HOME/.local/share/clio-relay/component-wheels/jarvis-cd"
JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL_DIR/{JARVIS_CD_WHEEL_FILENAME}"
mkdir -m 0700 -p "$(dirname "$JARVIS_CD_WHEEL_DIR")"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_cd_wheels
JARVIS_CD_STAGING="$(mktemp "${{JARVIS_CD_WHEEL}}.XXXXXX")"
curl -L --fail --retry 3 -o "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL_URL"
BOOTSTRAP_JARVIS_CD_DOWNLOADED=1
echo "$JARVIS_CD_WHEEL_SHA256 *$JARVIS_CD_STAGING" | sha256sum --check --strict -
mv "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL"
python -m pip install --isolated --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"
JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
JARVIS_MCP_ARTIFACT_SHA256={rendered_jarvis_mcp_artifact_sha256}
JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_INSTALL_SPEC"
JARVIS_MCP_ARTIFACT_PATH=""
JARVIS_MCP_REQUESTED_SOURCE="checkout"
JARVIS_MCP_VERSION=""
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" clio_kit_wheels
case "$JARVIS_MCP_INSTALL_SPEC" in
  "{CLIO_KIT_JARVIS_MCP_WHEEL_URL}")
    JARVIS_MCP_VERSION="{CLIO_KIT_JARVIS_MCP_VERSION}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
    COMPONENT_STAGING="$(mktemp "${{JARVIS_MCP_ARTIFACT_PATH}}.XXXXXX")"
    curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
      --retry 3 --retry-all-errors --retry-max-time 180 \
      --connect-timeout 20 --max-time 180 \
      --output "$COMPONENT_STAGING" "$JARVIS_MCP_INSTALL_SPEC"
    echo "$JARVIS_MCP_ARTIFACT_SHA256 *$COMPONENT_STAGING" | \
      sha256sum --check --strict -
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="github_release"
    BOOTSTRAP_CLIO_KIT_DOWNLOADED=1
    ;;
  clio-kit==*)
    JARVIS_MCP_VERSION="${{JARVIS_MCP_INSTALL_SPEC#clio-kit==}}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    python -m pip download --isolated --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$COMPONENT_DOWNLOAD_DIR" "$JARVIS_MCP_INSTALL_SPEC"
    mapfile -t JARVIS_MCP_WHEELS < <(
      find "$COMPONENT_DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_kit-*.whl' -print
    )
    if [ "${{#JARVIS_MCP_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-kit wheel" >&2
      exit 1
    fi
    JARVIS_MCP_ARTIFACT_PATH="${{JARVIS_MCP_WHEELS[0]}}"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="pypi"
    BOOTSTRAP_CLIO_KIT_DOWNLOADED=1
    ;;
  *.whl)
    test -f "$JARVIS_MCP_INSTALL_SPEC"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    COMPONENT_STAGING="$(mktemp "$BOOTSTRAP_TRANSACTION_ROOT/downloads/clio-kit.XXXXXX.whl")"
    cp "$JARVIS_MCP_INSTALL_SPEC" "$COMPONENT_STAGING"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/$(basename "$JARVIS_MCP_INSTALL_SPEC")"
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="wheel"
    ;;
  *)
    echo "clio-kit source must be the pinned URL, an exact version, or a local wheel" >&2
    exit 1
    ;;
esac
echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | \
  sha256sum --check --strict -
deactivate
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uv_tools
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uv_bin
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple "$JARVIS_MCP_INSTALL_TARGET"
JARVIS_MCP_UV_EXECUTABLE="$(command -v uv)"
test -x "$JARVIS_MCP_UV_EXECUTABLE"
JARVIS_MCP_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-kit"
test -x "$JARVIS_MCP_EXECUTABLE"
JARVIS_MCP_PROVIDER_PYTHON="$UV_TOOL_DIR/clio-kit/bin/python"
test -x "$JARVIS_MCP_PROVIDER_PYTHON"
JARVIS_MCP_INSTALLED_VERSION="$("$JARVIS_MCP_PROVIDER_PYTHON" -c \
  'from importlib.metadata import version; print(version("clio-kit"))')"
if [ -n "$JARVIS_MCP_VERSION" ] && \
   [ "$JARVIS_MCP_INSTALLED_VERSION" != "$JARVIS_MCP_VERSION" ]; then
  echo "installed clio-kit tool version does not match the release pin" >&2
  exit 1
fi
JARVIS_MCP_VERSION="$JARVIS_MCP_INSTALLED_VERSION"
"$JARVIS_MCP_EXECUTABLE" --help >/dev/null

DEST="$BOOTSTRAP_GENERATION/source"
SOURCE_ARCHIVE={rendered_source_archive}
SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
if [ -n "$SOURCE_ARCHIVE_SHA256" ]; then
  echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
fi
bootstrap_safe_extract "$JARVIS_VENV/bin/python" "$SOURCE_ARCHIVE" "$DEST"
bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" relay_source \
  "$DEST"
RELAY_INSTALL_SPEC={rendered_relay_install_spec}
RELAY_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
RELAY_ARTIFACT_PATH=""
case "$RELAY_INSTALL_SPEC" in
  clio-relay==*)
    DOWNLOAD_DIR="$DEST/downloaded-wheels"
    rm -rf "$DOWNLOAD_DIR"
    mkdir -p "$DOWNLOAD_DIR"
    "$JARVIS_VENV/bin/python" -m pip download --isolated \
      --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
    mapfile -t RELAY_WHEELS < <(
      find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
    )
    if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-relay wheel" >&2
      exit 1
    fi
    RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
    BOOTSTRAP_RELAY_DOWNLOAD_COUNT=1
    RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    if [ -z "$RELAY_ARTIFACT_SHA256" ]; then
      RELAY_VERSION="${{RELAY_INSTALL_SPEC#clio-relay==}}"
      RELAY_ARTIFACT_SHA256="$(
        "$JARVIS_VENV/bin/python" - "$RELAY_VERSION" "$(basename "$RELAY_ARTIFACT_PATH")" \
          <<'__CLIO_RELAY_PYPI_DIGEST__'
import json
import re
import sys
from urllib.parse import quote
from urllib.request import urlopen

version, filename = sys.argv[1:]
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    content = response.read(4 * 1024 * 1024 + 1)
if len(content) > 4 * 1024 * 1024:
    raise SystemExit("PyPI clio-relay metadata exceeds the bounded response size")
document = json.loads(content)
matches = [
    item
    for item in document.get("urls", [])
    if item.get("filename") == filename and item.get("packagetype") == "bdist_wheel"
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return one exact clio-relay wheel identity")
digest = matches[0].get("digests", {{}}).get("sha256")
if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{{64}}", digest) is None:
    raise SystemExit("PyPI clio-relay wheel identity omitted a valid SHA-256")
print(digest)
__CLIO_RELAY_PYPI_DIGEST__
      )"
    fi
    ;;
  *.whl)
    RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
    ;;
esac
if [ -n "$RELAY_ARTIFACT_PATH" ]; then
  test -n "$RELAY_ARTIFACT_SHA256"
  echo "$RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH" | sha256sum --check --strict -
fi
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple \\
  --with "$JARVIS_CD_WHEEL" "$RELAY_INSTALL_TARGET"
RELAY_UV_EXECUTABLE="$(command -v uv)"
test -x "$RELAY_UV_EXECUTABLE"
RELAY_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-relay"
test -x "$RELAY_EXECUTABLE"
RELAY_PROVIDER_PYTHON="$UV_TOOL_DIR/clio-relay/bin/python"
test -x "$RELAY_PROVIDER_PYTHON"
uv pip install --python "$JARVIS_VENV/bin/python" \\
  --default-index https://pypi.org/simple \\
  --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
JARVIS_PACKAGE_PROBE='import clio_relay, jarvis_cd; '
JARVIS_PACKAGE_PROBE+='import clio_relay.bounded_command.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.mcp_call.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.remote_agent.pkg'
"$RELAY_PROVIDER_PYTHON" -c "$JARVIS_PACKAGE_PROBE"
"$JARVIS_VENV/bin/python" -c "$JARVIS_PACKAGE_PROBE"
verify_jarvis_cd_distribution() {{
  local interpreter="$1"
  "$interpreter" - \\
    "$JARVIS_CD_WHEEL" \\
    "$JARVIS_CD_WHEEL_SHA256" \\
    "$JARVIS_CD_VERSION" \\
    <<'__CLIO_RELAY_NATIVE_JARVIS_PROBE__'
import hashlib
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.errors import ConfigurationError
from clio_relay.installation import verify_distribution_file_source

wheel = Path(sys.argv[1]).resolve()
expected_sha256 = sys.argv[2]
expected_version = sys.argv[3]
if hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_sha256:
    raise SystemExit("JARVIS-CD release wheel digest changed after installation")
installed = distribution("jarvis_cd")
if installed.version != expected_version:
    raise SystemExit("JARVIS-CD installed version does not match the release pin")
try:
    verify_distribution_file_source(
        direct_url_text=installed.read_text("direct_url.json"),
        expected_artifact=wheel,
    )
except ConfigurationError as exc:
    raise SystemExit(
        f"JARVIS-CD was not installed from the verified release wheel: {{exc}}"
    ) from exc
print(f"jarvis_cd_distribution={{installed.name}}=={{installed.version}}")
__CLIO_RELAY_NATIVE_JARVIS_PROBE__
}}
verify_jarvis_cd_distribution "$RELAY_PROVIDER_PYTHON"
verify_jarvis_cd_distribution "$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC="$RELAY_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_ARTIFACT="$RELAY_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE="$RELAY_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON="$RELAY_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE="$RELAY_UV_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT="$JARVIS_UTIL_COMMIT"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION="$JARVIS_CD_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL="$JARVIS_CD_WHEEL_URL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256="$JARVIS_CD_WHEEL_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON="$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE="$JARVIS_VENV/bin/jarvis"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC="$JARVIS_MCP_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT="$JARVIS_MCP_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256="$JARVIS_MCP_ARTIFACT_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE="$JARVIS_MCP_REQUESTED_SOURCE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION="$JARVIS_MCP_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE="$JARVIS_MCP_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON="$JARVIS_MCP_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE="$JARVIS_MCP_UV_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_DESIRED_STATE="$BOOTSTRAP_DESIRED_STATE"
export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_TRANSACTION_ROOT/install-receipt.json"
"$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_INSTALL_RECEIPT__'
import json
import os
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.bootstrap_reconcile import BootstrapDesiredState
from clio_relay.installation import (
    ComponentArtifactIdentity,
    probe_persistent_uv_tool_identity,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
    write_install_receipt,
)
from clio_relay.mcp_call.runner import mcp_server_artifact_identity
from clio_relay.validation_report import sha256_file

artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_ARTIFACT"]
desired_payload = json.loads(os.environ["CLIO_RELAY_BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
relay_artifact = Path(artifact_value).resolve() if artifact_value else None
relay_distribution = distribution("clio-relay")
relay_persistent_tool = None
if relay_artifact is not None:
    relay_persistent_tool = probe_persistent_uv_tool_identity(
        uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
        tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
        provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
        source_artifact=relay_artifact,
        distribution="clio-relay",
        distribution_version=relay_distribution.version,
        entry_point="clio-relay",
    )
component_artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT"]
component_artifact = Path(component_artifact_value).resolve() if component_artifact_value else None
component_artifact_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256"]
if component_artifact is None or sha256_file(component_artifact) != component_artifact_sha256:
    raise SystemExit("clio-kit wheel digest changed after persistent-tool installation")
component_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION"] or None
component_spec = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC"]
jarvis_cd_wheel = Path(os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL"]).resolve()
jarvis_cd_wheel_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if sha256_file(jarvis_cd_wheel) != jarvis_cd_wheel_sha256:
    raise SystemExit("jarvis-cd receipt wheel digest does not match bootstrap pin")
jarvis_cd_distribution = distribution("jarvis_cd")
if jarvis_cd_distribution.version != os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]:
    raise SystemExit("jarvis-cd receipt version does not match the released wheel pin")
runtime_command = [
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    "mcp-server",
    "jarvis",
]
if not runtime_command:
    raise SystemExit("clio-kit native JARVIS contract requires a persistent uv tool")
clio_kit_native_execution = probe_clio_kit_native_execution_contract(runtime_command)
persistent_clio_kit_tool = probe_persistent_uv_tool_identity(
    uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
    tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
    source_artifact=component_artifact,
    distribution="clio-kit",
    distribution_version=component_version,
    entry_point="clio-kit",
)
clio_kit_server_artifact = mcp_server_artifact_identity(
    runtime_command[0],
    runtime_command[1:],
    verify_relay_jarvis_cd_lock=True,
)
locked_server_runtime = clio_kit_server_artifact.get("nested_runtime")
if not isinstance(locked_server_runtime, dict):
    raise SystemExit("clio-kit JARVIS runtime omitted locked-server evidence")
jarvis_cd_lock_binding = locked_server_runtime.get("jarvis_cd_lock_binding")
if not isinstance(jarvis_cd_lock_binding, dict):
    raise SystemExit("clio-kit JARVIS runtime omitted jarvis-cd lock binding")
expected_jarvis_cd_url = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"]
expected_jarvis_cd_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]
expected_jarvis_cd_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if not (
    clio_kit_server_artifact.get("verified") is True
    and locked_server_runtime.get("schema_version") == "clio-kit.locked-server.v4"
    and locked_server_runtime.get("server_name") == "jarvis"
    and locked_server_runtime.get("locked_runtime_verified") is True
    and jarvis_cd_lock_binding.get("schema_version")
    == "clio-relay.jarvis-cd-lock-binding.v1"
    and jarvis_cd_lock_binding.get("dependency") == "jarvis-cd"
    and jarvis_cd_lock_binding.get("verified") is True
    and jarvis_cd_lock_binding.get("error") is None
    and jarvis_cd_lock_binding.get("expected_version") == expected_jarvis_cd_version
    and jarvis_cd_lock_binding.get("expected_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("expected_sha256") == expected_jarvis_cd_sha256
    and jarvis_cd_lock_binding.get("observed_version") == expected_jarvis_cd_version
    and jarvis_cd_lock_binding.get("observed_source_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("observed_wheel_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("observed_wheel_sha256") == expected_jarvis_cd_sha256
    and jarvis_cd_lock_binding.get("jarvis_mcp_package_entry_count") == 1
    and jarvis_cd_lock_binding.get("resolved_dependency_entry_count") == 1
    and jarvis_cd_lock_binding.get("observed_resolved_dependency_entries")
    == [{{"name": "jarvis-cd"}}]
    and jarvis_cd_lock_binding.get("metadata_requirement_entry_count") == 1
    and jarvis_cd_lock_binding.get("observed_metadata_requirement_entries")
    == [{{"name": "jarvis-cd", "url": expected_jarvis_cd_url}}]
    and jarvis_cd_lock_binding.get("observed_metadata_requirement_urls")
    == [expected_jarvis_cd_url]
    and jarvis_cd_lock_binding.get("package_entry_count") == 1
    and jarvis_cd_lock_binding.get("wheel_entry_count") == 1
):
    raise SystemExit(
        "clio-kit locked JARVIS dependency does not match the relay jarvis-cd release pin"
    )
jarvis_execution_native_execution = probe_jarvis_native_execution_capability(
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"]
)
receipt = write_install_receipt(
    install_spec=desired.relay_install_spec,
    artifact_path=Path(artifact_value) if artifact_value else None,
    components={{
        "clio-relay": relay_distribution.version,
        "clio-kit": component_version or component_spec,
        "jarvis-cd": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"],
        "jarvis-util": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT"],
    }},
    component_artifacts={{
        "clio-relay": ComponentArtifactIdentity(
            distribution=relay_distribution.name,
            distribution_version=relay_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"],
            requested_source=(
                "pypi"
                if os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"].startswith("clio-relay==")
                else ("wheel" if relay_artifact is not None else "checkout")
            ),
            artifact_filename=(relay_artifact.name if relay_artifact is not None else None),
            artifact_sha256=(sha256_file(relay_artifact) if relay_artifact is not None else None),
            runtime_artifact_path=(str(relay_artifact) if relay_artifact is not None else None),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "installation-info",
            ],
            runtime_interpreters={{
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
                "execution": os.environ[
                    "CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"
                ],
            }},
            runtime_executables={{
                "clio-relay": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
            }},
            persistent_tool=relay_persistent_tool,
        ),
        "clio-kit": ComponentArtifactIdentity(
            distribution="clio-kit",
            distribution_version=component_version,
            install_spec=component_spec,
            requested_source=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE"],
            artifact_filename=(component_artifact.name if component_artifact else None),
            artifact_sha256=component_artifact_sha256,
            runtime_artifact_path=(str(component_artifact) if component_artifact else None),
            runtime_command=runtime_command,
            runtime_interpreters={{
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
            }},
            runtime_executables={{
                "clio-kit": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
            }},
            native_execution=clio_kit_native_execution,
            persistent_tool=persistent_clio_kit_tool,
            locked_server_runtime=locked_server_runtime,
        ),
        "jarvis-cd": ComponentArtifactIdentity(
            distribution=jarvis_cd_distribution.name,
            distribution_version=jarvis_cd_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"],
            requested_source="github_release",
            artifact_filename=jarvis_cd_wheel.name,
            artifact_sha256=jarvis_cd_wheel_sha256,
            runtime_artifact_path=str(jarvis_cd_wheel),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
                "--help",
            ],
            runtime_interpreters={{
                "provider": sys.executable,
                "execution": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"],
            }},
            runtime_executables={{
                "jarvis": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
            }},
            native_execution=jarvis_execution_native_execution,
        ),
    }},
    deployment_fingerprint=desired.fingerprint,
    deployment_manifest=desired.model_dump(mode="json"),
    generation=desired.fingerprint,
)
print(f"relay_install_receipt={{receipt.schema_version}}")
print(f"relay_artifact_sha256={{receipt.artifact_sha256 or 'none'}}")
__CLIO_RELAY_INSTALL_RECEIPT__
BOOTSTRAP_COMPONENTS_IDENTITY="$(bootstrap_path_set_identity \
  "$CLIO_RELAY_INSTALL_RECEIPT" \
  "$JARVIS_VENV/bin/python" \
  "$JARVIS_VENV/bin/jarvis" \
  "$RELAY_EXECUTABLE" \
  "$JARVIS_MCP_EXECUTABLE" \
  "$HOME/.local/bin/frpc" \
  "$HOME/.local/bin/frps" \
  "$HOME/.local/bin/uv")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  components_prepared "$BOOTSTRAP_COMPONENTS_IDENTITY"

if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 0 ]; then
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_config
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_private
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_shared
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_state
  BOOTSTRAP_JARVIS_INIT_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  "$JARVIS_VENV/bin/jarvis" init \
    "$HOME/.local/share/clio-relay/jarvis-config" \
    "$HOME/.local/share/clio-relay/jarvis-private" \
    "$HOME/.local/share/clio-relay/jarvis-shared"
  BOOTSTRAP_JARVIS_INIT_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_INIT_DURATION_NS=$((
    BOOTSTRAP_JARVIS_INIT_COMPLETED_NS - BOOTSTRAP_JARVIS_INIT_STARTED_NS
  ))
  BOOTSTRAP_JARVIS_INIT_IDENTITY="$(bootstrap_path_set_identity \
    "$JARVIS_CONFIG_FILE" "$JARVIS_REPOS_FILE" "$JARVIS_GRAPH_FILE")"
  bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    jarvis_initialized "$BOOTSTRAP_JARVIS_INIT_IDENTITY"
  BOOTSTRAP_JARVIS_GRAPH_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE="$BOOTSTRAP_TRANSACTION_ROOT/jarvis-builtin-result.json"
  : > "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"
  chmod 0600 "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"
  if ! timeout --signal=TERM --kill-after=2s 30s \
    "$JARVIS_VENV/bin/jarvis" rg load-builtin \
      "$JARVIS_RESOURCE_GRAPH_PROFILE" +json \
      > "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"; then
    echo "JARVIS builtin resource graph activation failed" >&2
    exit 1
  fi
  BOOTSTRAP_JARVIS_BUILTIN_ACTION="$(
    "$RELAY_PROVIDER_PYTHON" - \
      "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE" \
      "$JARVIS_RESOURCE_GRAPH_PROFILE" \
      "$JARVIS_GRAPH_FILE" <<'__CLIO_RELAY_JARVIS_BUILTIN_RESULT__'
import hashlib
import json
import stat
import sys
from pathlib import Path

from clio_relay.bootstrap_reconcile import validate_jarvis_builtin_result

result_path = Path(sys.argv[1])
requested_profile = sys.argv[2]
active_graph_path = Path(sys.argv[3])
before = result_path.lstat()
if (
    not stat.S_ISREG(before.st_mode)
    or before.st_nlink != 1
    or not 0 < before.st_size <= 64 * 1024
):
    raise SystemExit("JARVIS builtin graph result is not one bounded regular file")
payload = result_path.read_bytes()
after = result_path.lstat()
if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
    after.st_dev,
    after.st_ino,
    after.st_size,
    after.st_mtime_ns,
):
    raise SystemExit("JARVIS builtin graph result changed during validation")
try:
    result = json.loads(payload)
except (UnicodeDecodeError, json.JSONDecodeError) as exc:
    raise SystemExit("JARVIS builtin graph result is not valid JSON") from exc
if not isinstance(result, dict):
    raise SystemExit("JARVIS builtin graph result is not an object")
try:
    validate_jarvis_builtin_result(result, requested_profile=requested_profile)
except ValueError as exc:
    raise SystemExit(f"JARVIS builtin graph result is invalid: {{exc}}") from exc
action = result["action"]
if action == "loaded":
    source_sha256 = result["source_sha256"]
    assert isinstance(source_sha256, str)
    graph_before = active_graph_path.lstat()
    if not stat.S_ISREG(graph_before.st_mode) or not 0 < graph_before.st_size <= 64 * 1024 * 1024:
        raise SystemExit("activated JARVIS resource graph is not one bounded regular file")
    digest = hashlib.sha256()
    with active_graph_path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    graph_after = active_graph_path.lstat()
    if (
        graph_before.st_dev,
        graph_before.st_ino,
        graph_before.st_size,
        graph_before.st_mtime_ns,
    ) != (
        graph_after.st_dev,
        graph_after.st_ino,
        graph_after.st_size,
        graph_after.st_mtime_ns,
    ):
        raise SystemExit("activated JARVIS resource graph changed during validation")
    if digest.hexdigest() != source_sha256:
        raise SystemExit("activated JARVIS resource graph does not match builtin evidence")
print(action)
__CLIO_RELAY_JARVIS_BUILTIN_RESULT__
  )"
  case "$BOOTSTRAP_JARVIS_BUILTIN_ACTION" in
    loaded)
      JARVIS_GRAPH_ACTION="loaded"
      ;;
    unavailable)
      if [ "$ALLOW_JARVIS_RESOURCE_GRAPH_BUILD" != "1" ]; then
        echo "requested JARVIS builtin resource graph is unavailable;" \
          "build fallback is disabled" >&2
        exit 1
      fi
      "$JARVIS_VENV/bin/jarvis" rg build +no_benchmark
      JARVIS_GRAPH_ACTION="built"
      ;;
    *)
      echo "JARVIS builtin resource graph validator returned an invalid action" >&2
      exit 1
      ;;
  esac
  BOOTSTRAP_JARVIS_GRAPH_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_GRAPH_DURATION_NS=$((
    BOOTSTRAP_JARVIS_GRAPH_COMPLETED_NS - BOOTSTRAP_JARVIS_GRAPH_STARTED_NS
  ))
  BOOTSTRAP_JARVIS_GRAPH_IDENTITY="$(bootstrap_path_set_identity "$JARVIS_GRAPH_FILE")"
  bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    "resource_graph_$JARVIS_GRAPH_ACTION" "$BOOTSTRAP_JARVIS_GRAPH_IDENTITY"
  JARVIS_INIT_ACTION="initialized"
  BOOTSTRAP_JARVIS_COMMANDS_JSON="$(
    "$JARVIS_VENV/bin/python" - \
      "$HOME" "$JARVIS_RESOURCE_GRAPH_PROFILE" "$JARVIS_GRAPH_ACTION" \
      <<'__CLIO_RELAY_JARVIS_COMMANDS__'
import json
import sys

home, profile, graph_action = sys.argv[1:]
commands = [
    [
        "jarvis",
        "init",
        f"{{home}}/.local/share/clio-relay/jarvis-config",
        f"{{home}}/.local/share/clio-relay/jarvis-private",
        f"{{home}}/.local/share/clio-relay/jarvis-shared",
    ],
    ["jarvis", "rg", "load-builtin", profile, "+json"],
]
if graph_action == "built":
    commands.append(["jarvis", "rg", "build", "+no_benchmark"])
print(json.dumps(commands, separators=(",", ":")))
__CLIO_RELAY_JARVIS_COMMANDS__
  )"
else
  BOOTSTRAP_JARVIS_INIT_DURATION_NS=0
  BOOTSTRAP_JARVIS_GRAPH_DURATION_NS=0
  BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE=""
  JARVIS_INIT_ACTION="preserved"
  JARVIS_GRAPH_ACTION="preserved"
  BOOTSTRAP_JARVIS_COMMANDS_JSON='[]'
fi
MANAGED_JARVIS_REPO="$HOME/.local/share/clio-relay/clio_relay"
MANAGED_JARVIS_REPO_TARGET="$HOME/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
if [ -L "$MANAGED_JARVIS_REPO" ]; then
  if [ "$(readlink "$MANAGED_JARVIS_REPO")" != "$MANAGED_JARVIS_REPO_TARGET" ]; then
    echo "relay-managed JARVIS repository link points to an unexpected target" >&2
    exit 1
  fi
elif [ -e "$MANAGED_JARVIS_REPO" ]; then
  echo "relay-managed JARVIS repository path is not a symbolic link" >&2
  exit 1
else
  bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    managed_repo "$MANAGED_JARVIS_REPO_TARGET"
fi
export MANAGED_JARVIS_REPO JARVIS_REPOS_FILE
"$RELAY_PROVIDER_PYTHON" - "$DEST/jarvis-packages/clio_relay" \
  "$HOME/.local/share/clio-relay/managed-jarvis-repo" \
  <<'__CLIO_RELAY_JARVIS_REPO_RECONCILE__'
import os
import sys
from pathlib import Path

from clio_relay.bootstrap_reconcile import reconcile_managed_jarvis_repository

evidence = reconcile_managed_jarvis_repository(
    Path(os.environ["JARVIS_REPOS_FILE"]),
    Path(os.environ["MANAGED_JARVIS_REPO"]),
    previous_managed_repos=(Path(sys.argv[1]), Path(sys.argv[2])),
)
print(f"jarvis_managed_repo={{evidence['action']}}")
__CLIO_RELAY_JARVIS_REPO_RECONCILE__
BOOTSTRAP_MANAGED_REPO_IDENTITY="$(bootstrap_path_set_identity \
  "$JARVIS_REPOS_FILE" "$MANAGED_JARVIS_REPO")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  managed_repository_reconciled "$BOOTSTRAP_MANAGED_REPO_IDENTITY"

BOOTSTRAP_VERIFIED_DESIRED_FINGERPRINT="$(
  "$RELAY_PROVIDER_PYTHON" -c \
    'import json,os; from clio_relay.bootstrap_reconcile import BootstrapDesiredState; '\
'value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'value["agent_npm_package"]=os.environ["AGENT_NPM_PACKAGE"] or None; '\
'value["agent_npm_bin"]=os.environ["AGENT_NPM_BIN"] or None; '\
'print(BootstrapDesiredState.model_validate(value).fingerprint)'
)"
if [ "$BOOTSTRAP_VERIFIED_DESIRED_FINGERPRINT" != \
     "$BOOTSTRAP_DESIRED_FINGERPRINT" ]; then
  echo "fresh bootstrap desired fingerprint changed after provider installation" >&2
  exit 1
fi
if [ -e "$HOME/.local/share/clio-relay/current" ] || \
   [ -L "$HOME/.local/share/clio-relay/current" ]; then
  echo "fresh bootstrap found an existing current generation pointer" >&2
  exit 1
fi
RELAY_TOOL_EXECUTABLE="$(readlink -f "$RELAY_EXECUTABLE")"
JARVIS_TOOL_EXECUTABLE="$(readlink -f "$JARVIS_VENV/bin/jarvis")"
CLIO_KIT_TOOL_EXECUTABLE="$(readlink -f "$JARVIS_MCP_EXECUTABLE")"
test -x "$RELAY_TOOL_EXECUTABLE"
test -x "$JARVIS_TOOL_EXECUTABLE"
test -x "$CLIO_KIT_TOOL_EXECUTABLE"
mkdir -m 0700 "$BOOTSTRAP_GENERATION/bin"
ln -s "$RELAY_TOOL_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-relay"
ln -s "$CLIO_KIT_TOOL_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-kit"
mv "$CLIO_RELAY_INSTALL_RECEIPT" "$BOOTSTRAP_GENERATION/install-receipt.json"
export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json"
export BOOTSTRAP_GENERATION JARVIS_VENV JARVIS_TOOL_EXECUTABLE
"$RELAY_PROVIDER_PYTHON" - "$BOOTSTRAP_GENERATION" \
  <<'__CLIO_RELAY_FULL_GENERATION_MANIFEST__'
import json
import os
import sys
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapReconcilePlan,
    execution_environment_identity,
    write_jarvis_wrapper,
)
from clio_relay.validation_report import sha256_file

generation = Path(sys.argv[1])
execution_root = Path(os.environ["JARVIS_VENV"])
execution_python = execution_root / "bin/python"
jarvis_executable = Path(os.environ["JARVIS_TOOL_EXECUTABLE"])
execution_identity = execution_environment_identity(
    execution_root,
    executables={{
        "python": execution_python,
        "jarvis": jarvis_executable,
    }},
)
wrapper = write_jarvis_wrapper(generation / "bin/jarvis", execution_python)
fingerprint = os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]
plan = BootstrapReconcilePlan(
    mode="full",
    desired_fingerprint=fingerprint,
    reasons=["fresh cluster bootstrap"],
    component_actions={{
        "clio-relay": "replace",
        "jarvis-cd": "replace",
        "jarvis-util": "replace",
        "clio-kit": "replace",
        "frp": "replace",
        "uv": "replace",
    }},
)
manifest = {{
    "schema_version": "clio-relay.bootstrap-generation.v1",
    "fingerprint": fingerprint,
    "plan": plan.model_dump(mode="json"),
    "legacy_execution_identity": execution_identity,
    "active_execution_identity": execution_identity,
    "jarvis_wrapper_sha256": wrapper["sha256"],
    "install_receipt": str(generation / "install-receipt.json"),
    "install_receipt_sha256": sha256_file(generation / "install-receipt.json"),
}}
for name, payload in (
    ("manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\\n"),
    (".prepared", manifest["fingerprint"] + "\\n"),
):
    path = generation / name
    with path.open("x", encoding="utf-8", newline="\\n") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
__CLIO_RELAY_FULL_GENERATION_MANIFEST__
BOOTSTRAP_GENERATION_IDENTITY="$(bootstrap_path_set_identity \
  "$BOOTSTRAP_GENERATION/manifest.json" \
  "$BOOTSTRAP_GENERATION/.prepared" \
  "$BOOTSTRAP_GENERATION/install-receipt.json" \
  "$BOOTSTRAP_GENERATION/bin/clio-relay" \
  "$BOOTSTRAP_GENERATION/bin/clio-kit" \
  "$BOOTSTRAP_GENERATION/bin/jarvis" \
  "$BOOTSTRAP_GENERATION/source")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  generation_prepared "$BOOTSTRAP_GENERATION_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  prepared "$BOOTSTRAP_DESIRED_FINGERPRINT"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" activating
bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  current "$BOOTSTRAP_GENERATION"
bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  install_receipt "$HOME/.local/share/clio-relay/current/install-receipt.json"
bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  relay_launcher "$HOME/.local/share/clio-relay/current/bin/clio-relay"
bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  jarvis_launcher "$HOME/.local/share/clio-relay/current/bin/jarvis"
BOOTSTRAP_ACTIVATION_IDENTITY="$(bootstrap_path_set_identity \
  "$HOME/.local/share/clio-relay/current" \
  "$HOME/.local/share/clio-relay/install-receipt.json" \
  "$HOME/.local/bin/clio-relay" \
  "$HOME/.local/bin/jarvis" \
  "$HOME/.local/share/clio-relay/clio_relay")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  generation_activated "$BOOTSTRAP_ACTIVATION_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" activated
BOOTSTRAP_FULL_PREPARE_COMPLETED_NS="$(
  python3 -c 'import time; print(time.monotonic_ns())'
)"

{worker_recheck}
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migration_started
BOOTSTRAP_QUEUE_ACTION=verified_read_only
BOOTSTRAP_QUEUE_DURATION_NS=0
BOOTSTRAP_QUEUE_BEFORE="$(
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
)"
export BOOTSTRAP_QUEUE_BEFORE
if ! python3 -c \
  'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_QUEUE_BEFORE"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
  2>/dev/null; then
  BOOTSTRAP_QUEUE_ACTION=audited_and_sealed
  BOOTSTRAP_QUEUE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
  CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
  CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
  CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
  CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
  CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
  CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
  {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
  {init_command}
  BOOTSTRAP_QUEUE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_QUEUE_DURATION_NS=$((
    BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS
  ))
fi
BOOTSTRAP_QUEUE_EVIDENCE="$(
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    "$HOME/.local/bin/clio-relay" queue readiness-info
)"
BOOTSTRAP_QUEUE_IDENTITY="$(
  BOOTSTRAP_QUEUE_EVIDENCE="$BOOTSTRAP_QUEUE_EVIDENCE" \
    python3 - <<'__CLIO_RELAY_FRESH_QUEUE_IDENTITY__'
import hashlib
import json
import os

value = json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"])
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_QUEUE_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  queue_migrated "$BOOTSTRAP_QUEUE_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migrated

BOOTSTRAP_SERVICE_RESTART_COUNT=0
BOOTSTRAP_SERVICE_START_COUNT=0
BOOTSTRAP_SERVICE_STOP_COUNT=0
BOOTSTRAP_SERVICE_ENABLE_COUNT=0
BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
BOOTSTRAP_SERVICE_PENDING_INSTALL=0
if [ -n "$WORKER_SERVICE_NAME" ] && \
   systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=1
fi
if [ "$WORKER_WAS_ACTIVE" = "1" ] || \
   {{ [ -n "$WORKER_SERVICE_NAME" ] && \
      [ "${{WORKER_LOAD_STATE:-unknown}}" = "loaded" ]; }}; then
  bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" starting
fi
if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
  BOOTSTRAP_SERVICE_STOP_COUNT=1
  BOOTSTRAP_SERVICE_RESTART_COUNT=1
{worker_restart}
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
elif [ -n "$WORKER_SERVICE_NAME" ]; then
  if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
    BOOTSTRAP_SERVICE_PENDING_INSTALL=1
  else
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready after full bootstrap" >&2
      exit 1
    fi
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi
fi

BOOTSTRAP_WORKER_EVIDENCE=""
if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
  for _BOOTSTRAP_READY_ATTEMPT in $(seq 1 90); do
    if BOOTSTRAP_WORKER_EVIDENCE="$(
      CLIO_RELAY_CORE_DIR={rendered_core_dir} \
        "$HOME/.local/bin/clio-relay" endpoint worker-info \
          --cluster "$WORKER_CLUSTER_NAME" --freshness-seconds 120 \
          --readiness-only 2>/dev/null
    )"; then
      export BOOTSTRAP_WORKER_EVIDENCE
      if python3 -c \
        'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_WORKER_EVIDENCE"]); '\
'sys.exit(0 if value.get("running") is True else 1)'; then
        break
      fi
    fi
    BOOTSTRAP_WORKER_EVIDENCE=""
    sleep 2
  done
  if [ -z "$BOOTSTRAP_WORKER_EVIDENCE" ]; then
    echo "endpoint worker did not publish bounded ready identity after full bootstrap" >&2
    exit 1
  fi
fi
BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=unknown
BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=unknown
if [ "$BOOTSTRAP_SERVICE_PENDING_INSTALL" = "1" ]; then
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=false
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=false
elif [ -n "$WORKER_SERVICE_NAME" ]; then
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=true
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=true
fi
BOOTSTRAP_SERVICE_IDENTITY="$(
  BOOTSTRAP_QUEUE_EVIDENCE="$BOOTSTRAP_QUEUE_EVIDENCE" \
  BOOTSTRAP_WORKER_EVIDENCE="$BOOTSTRAP_WORKER_EVIDENCE" \
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON="$BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON" \
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON="$BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON" \
  BOOTSTRAP_SERVICE_PENDING_INSTALL="$BOOTSTRAP_SERVICE_PENDING_INSTALL" \
    python3 - <<'__CLIO_RELAY_FRESH_SERVICE_IDENTITY__'
import hashlib
import json
import os

worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
value = {{
    "queue": json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    "worker": json.loads(worker_text) if worker_text else None,
    "active": os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"],
    "enabled": os.environ["BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON"],
    "pending_install": os.environ["BOOTSTRAP_SERVICE_PENDING_INSTALL"] == "1",
}}
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_SERVICE_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  service_verified "$BOOTSTRAP_SERVICE_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" service_verified
BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS BOOTSTRAP_QUEUE_EVIDENCE
export BOOTSTRAP_WORKER_EVIDENCE BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON
export BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON BOOTSTRAP_SERVICE_PENDING_INSTALL
export BOOTSTRAP_SERVICE_RESTART_COUNT BOOTSTRAP_SERVICE_START_COUNT
export BOOTSTRAP_SERVICE_STOP_COUNT BOOTSTRAP_SERVICE_ENABLE_COUNT
export BOOTSTRAP_FULL_PREPARE_STARTED_NS BOOTSTRAP_FULL_PREPARE_COMPLETED_NS
export BOOTSTRAP_JARVIS_INIT_DURATION_NS BOOTSTRAP_COMPLETED_NS
export BOOTSTRAP_JARVIS_GRAPH_DURATION_NS BOOTSTRAP_JARVIS_COMMANDS_JSON
export BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE JARVIS_INIT_ACTION JARVIS_GRAPH_ACTION
export BOOTSTRAP_FRP_DOWNLOADED BOOTSTRAP_UV_DOWNLOADED
export BOOTSTRAP_JARVIS_UTIL_DOWNLOADED BOOTSTRAP_JARVIS_CD_DOWNLOADED
export BOOTSTRAP_CLIO_KIT_DOWNLOADED BOOTSTRAP_RELAY_DOWNLOAD_COUNT

"$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_BOOTSTRAP_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    JarvisStateEvidence,
    canonical_json_sha256,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_value = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"]
service_active = (
    True if service_value == "true" else (False if service_value == "false" else None)
)
enabled_value = os.environ["BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON"]
service_enabled = (
    True if enabled_value == "true" else (False if enabled_value == "false" else None)
)
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_active,
    service_was_enabled=service_enabled,
    queue_evidence=json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    worker_evidence=json.loads(worker_text) if worker_text else None,
)
service_pending_install = os.environ["BOOTSTRAP_SERVICE_PENDING_INSTALL"] == "1"
pending_reasons = {{
    "managed endpoint service is inactive",
    "managed endpoint service is disabled",
}}
if not inspection.exact_match and not (
    service_pending_install and set(inspection.reasons) == pending_reasons
):
    raise SystemExit(
        "full bootstrap did not pass exact inspection: " + repr(inspection.reasons)
    )
install_receipt = load_install_receipt()
prepare_duration = (
    int(os.environ["BOOTSTRAP_FULL_PREPARE_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_FULL_PREPARE_STARTED_NS"])
) / 1_000_000_000
components = {{}}
for name in ("clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv"):
    artifact = install_receipt.component_artifacts.get(name)
    observed = (
        artifact.model_dump(mode="json")
        if artifact is not None
        else {{"identity": install_receipt.components.get(name)}}
    )
    components[name] = {{
        "action": "prepared",
        "observed_identity": observed,
        "duration_seconds": prepare_duration,
    }}
download_sources = {{
    "frp": f"github-release:{{desired.frp_version}}",
    "uv": f"github-release:{{desired.uv_version}}",
    "jarvis-util": f"git-commit:{{desired.jarvis_util_commit}}",
    "jarvis-cd": desired.jarvis_cd_wheel_url,
    "clio-kit": desired.clio_kit_install_spec,
    "clio-relay": desired.relay_install_spec,
}}
download_flags = {{
    "frp": "BOOTSTRAP_FRP_DOWNLOADED",
    "uv": "BOOTSTRAP_UV_DOWNLOADED",
    "jarvis-util": "BOOTSTRAP_JARVIS_UTIL_DOWNLOADED",
    "jarvis-cd": "BOOTSTRAP_JARVIS_CD_DOWNLOADED",
    "clio-kit": "BOOTSTRAP_CLIO_KIT_DOWNLOADED",
    "clio-relay": "BOOTSTRAP_RELAY_DOWNLOAD_COUNT",
}}
downloads = [
    {{"component": name, "source": download_sources[name]}}
    for name, flag in download_flags.items()
    if os.environ[flag] == "1"
]
transaction = BootstrapTransactionJournal.load(
    Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"])
)
if transaction.mode != "full" or transaction.desired_fingerprint != desired.fingerprint:
    raise SystemExit("full bootstrap transaction identity changed before commit")
transaction.record_phase(
    "final_inspection",
    canonical_json_sha256(inspection.model_dump(mode="json")),
)
transaction.advance(BootstrapTransactionState.COMMITTED)
transaction.persist(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="full",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=None,
    active_generation=desired.fingerprint,
    components=components,
    duration_seconds=(completed_ns - started_ns) / 1_000_000_000,
    downloads=downloads,
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    service_pending_install=service_pending_install,
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_init_action=os.environ["JARVIS_INIT_ACTION"],
    jarvis_init_duration_seconds=(
        int(os.environ["BOOTSTRAP_JARVIS_INIT_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_graph_action=os.environ["JARVIS_GRAPH_ACTION"],
    jarvis_graph_duration_seconds=(
        int(os.environ["BOOTSTRAP_JARVIS_GRAPH_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_builtin_result=(
        json.loads(Path(os.environ["BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"]).read_bytes())
        if os.environ["BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"]
        else None
    ),
    jarvis_commands=json.loads(os.environ["BOOTSTRAP_JARVIS_COMMANDS_JSON"]),
    jarvis_state_before=JarvisStateEvidence(
        initialized=False,
        root=inspection.jarvis_state.root,
    ),
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_BOOTSTRAP_RECEIPT__

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
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
    identity = (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )
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
    if (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ) != identity:
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


def _assert_executable(path: Path) -> None:
    try:
        _run(
            [str(path), "--version"],
            timeout_seconds=10,
            stdout_maximum_bytes=4096,
            stderr_maximum_bytes=4096,
        )
    except (OSError, RelayError) as exc:
        raise ConfigurationError(f"installed executable cannot run: {path}: {exc}") from exc


def _assert_frp_pair(frpc: Path, frps: Path) -> None:
    _assert_sha256(frpc, FRPC_WINDOWS_AMD64_SHA256)
    _assert_sha256(frps, FRPS_WINDOWS_AMD64_SHA256)
    _assert_executable(frpc)
    _assert_executable(frps)


def _assert_sha256(path: Path, expected: str) -> None:
    try:
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ConfigurationError(f"installed executable cannot be hashed: {path}: {exc}") from exc
    if observed != expected:
        raise ConfigurationError(f"installed executable SHA-256 mismatch: {path}: {observed}")


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
    timeout_seconds: float | None = None,
    stdout_maximum_bytes: int = 2 * 1024 * 1024,
    stderr_maximum_bytes: int = 64 * 1024,
) -> subprocess.CompletedProcess[str]:
    """Run one local transport command with finite time and output bounds."""
    env = os.environ.copy()
    effective_timeout = 120.0 if timeout_seconds is None else timeout_seconds
    try:
        result = run_bounded_process(
            command,
            cwd=cwd,
            environment=env,
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
