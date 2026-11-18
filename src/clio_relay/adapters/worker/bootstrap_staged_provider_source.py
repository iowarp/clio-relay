"""Embedded staged-provider exec program for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). `_STAGED_PROVIDER_EXEC_PROGRAM` is
the memfd-sealed Python program bootstrap_provider_exec() execs when a
staged generation is active; the sanitizer/env-name constants strip the
caller's LD_*/PYTHON*/BASH_ENV/ENV before that exec. Pure string data, not
independently monkeypatched -- a plain re-import is sufficient.
"""

from __future__ import annotations

_STAGED_PROVIDER_ENVIRONMENT_SANITIZER = r"""
for bootstrap_environment_name in "${!LD_@}" "${!PYTHON@}" BASH_ENV ENV; do
  unset "$bootstrap_environment_name"
done
""".strip()
_POSIX_REMOTE_SHELL_STARTUP_ENVIRONMENT_NAMES = (
    "BASH_ENV",
    "ENV",
    "LD_AUDIT",
    "LD_BIND_NOT",
    "LD_BIND_NOW",
    "LD_DEBUG",
    "LD_DEBUG_OUTPUT",
    "LD_DYNAMIC_WEAK",
    "LD_HWCAP_MASK",
    "LD_LIBRARY_PATH",
    "LD_ORIGIN_PATH",
    "LD_POINTER_GUARD",
    "LD_PRELOAD",
    "LD_PROFILE",
    "LD_PROFILE_OUTPUT",
    "LD_SHOW_AUXV",
    "LD_TRACE_LOADED_OBJECTS",
    "LD_TRACE_PRELINKING",
    "LD_USE_LOAD_BIAS",
    "LD_VERBOSE",
    "LD_WARN",
    "PYTHONCASEOK",
    "PYTHONDEBUG",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONEXECUTABLE",
    "PYTHONFAULTHANDLER",
    "PYTHONHASHSEED",
    "PYTHONHOME",
    "PYTHONINSPECT",
    "PYTHONIOENCODING",
    "PYTHONMALLOC",
    "PYTHONNOUSERSITE",
    "PYTHONOPTIMIZE",
    "PYTHONPATH",
    "PYTHONPROFILEIMPORTTIME",
    "PYTHONPYCACHEPREFIX",
    "PYTHONSAFEPATH",
    "PYTHONSTARTUP",
    "PYTHONTRACEMALLOC",
    "PYTHONUNBUFFERED",
    "PYTHONUSERBASE",
    "PYTHONUTF8",
    "PYTHONWARNDEFAULTENCODING",
    "PYTHONWARNINGS",
)
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
    # Resolve the string table by the segment holding its START, then bound the
    # whole range against the FILE. Requiring one segment to contain the entire
    # table rejects ordinary binaries: in CPython 3.12.13 as shipped by uv,
    # .dynstr begins in one PT_LOAD and continues into the next (the two are
    # contiguous and share a vaddr-to-offset delta), so NO segment contained it
    # and the count-based test reported "ambiguous" for a perfectly good file
    # (#158). A set keeps the anti-tamper property: segments that disagree
    # about the offset are still refused.
    string_candidates = {
        file_offset + string_address - virtual_address
        for file_offset, virtual_address, file_size in load_segments
        if virtual_address <= string_address < virtual_address + file_size
    }
    if len(string_candidates) != 1:
        raise SystemExit("staged provider ELF string table is ambiguous")
    string_offset = next(iter(string_candidates))
    if string_offset < 0 or string_offset + string_size > len(payload):
        raise SystemExit("staged provider ELF string table exceeds the file")
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
