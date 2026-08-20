"""Embedded candidate-uv-install program and candidate package identity.

Split from bootstrap.py (clio-relay#255). `_BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE`
is the standalone Python program that verifies/installs the pinned uv and
the relay's own provider before any candidate code runs; the package-source
overlay/name list and `_bootstrap_candidate_package_sources()` identify
exactly which repository-relative sources travel with a candidate.
"""

from __future__ import annotations

from pathlib import Path

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
    # Same resolution as the staged-provider path above: locate by the segment
    # holding the table's START and bound the range against the file, because a
    # real .dynstr can span two contiguous PT_LOAD segments (#158).
    string_candidates = {
        file_offset + string_address - virtual_address
        for file_offset, virtual_address, file_size in load_segments
        if virtual_address <= string_address < virtual_address + file_size
    }
    if len(string_candidates) != 1:
        raise SystemExit("candidate provider ELF string table is ambiguous")
    string_offset = next(iter(string_candidates))
    if string_offset < 0 or string_offset + string_size > len(payload):
        raise SystemExit("candidate provider ELF string table exceeds the file")
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


def change_identity(details):
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def cross_open_identity(details):
    # Opening a Windows file may churn ctime. Device, inode, mode, size, and
    # mtime still pin the object across open; payload SHA-256 or RECORD digests
    # retain the byte-integrity pin.
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
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
        or change_identity(after) != change_identity(before)
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
    if (
        cross_open_identity(opened) != cross_open_identity(before)
        or cross_open_identity(path.lstat()) != cross_open_identity(opened)
    ):
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
        or cross_open_identity(before) != cross_open_identity(opened)
        or change_identity(after) != change_identity(opened)
        or cross_open_identity(path.lstat()) != cross_open_identity(opened)
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
        cross_open_identity(uv_before) != cross_open_identity(uv_opened)
        or cross_open_identity(wheel_before) != cross_open_identity(wheel_opened)
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
        change_identity(os.fstat(uv_descriptor)) != change_identity(uv_opened)
        or change_identity(os.fstat(wheel_descriptor)) != change_identity(wheel_opened)
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
            cross_open_identity(provider_before) != cross_open_identity(provider_opened)
            or provider_location.resolve(strict=True) != provider_target
            or cross_open_identity(provider_target.lstat())
            != cross_open_identity(provider_opened)
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
    "bootstrap_full_activation_staging.py",
    "bootstrap_jarvis_staging.py",
    "bootstrap_provider_build_info.py",
    "bootstrap_reconcile.py",
    "bootstrap_recovery.py",
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
