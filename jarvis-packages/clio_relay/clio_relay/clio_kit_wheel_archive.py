"""Bounded, path-safe reading of a clio-kit wheel's ZIP archive members.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). This
module holds the canonical ``_file_identity`` definition -- the file-hash/stat
identity primitive most other mcp_call modules treat as an overridable facade
attribute. This module's own body never calls it back (nothing here needs
reach-back), but callers elsewhere that invoke it through the facade rely on
this module being the one true implementation the facade re-exports.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import stat
import zipfile
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from clio_relay.constants import (
    _CLIO_KIT_RUNTIME_PROJECT_EXCLUDED_NAMES,
    CLIO_KIT_WHEEL_MAX_FILES,
    CLIO_KIT_WHEEL_MAX_PROJECT_BYTES,
    CLIO_KIT_WHEEL_MAX_PROJECT_FILES,
    FILE_HASH_CHUNK_BYTES,
)


@contextmanager
def verified_wheel_archive(
    path: Path,
    artifact: dict[str, Any] | None,
) -> Generator[zipfile.ZipFile]:
    """Open the exact hashed regular wheel and reject replacement during inspection."""
    expected_sha256 = artifact.get("sha256") if artifact is not None else None
    expected_size = artifact.get("size_bytes") if artifact is not None else None
    if not isinstance(expected_sha256, str) or not isinstance(expected_size, int):
        raise ValueError("clio-kit wheel identity is incomplete")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
            raise ValueError("clio-kit wheel changed before runtime verification")
        identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        digest = hashlib.sha256()
        while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
            digest.update(chunk)
        if not hmac.compare_digest(digest.hexdigest(), expected_sha256):
            raise ValueError("clio-kit wheel changed before runtime verification")
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            yield archive
        after = os.fstat(stream.fileno())
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
            raise ValueError("clio-kit wheel changed during runtime verification")


def validated_wheel_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    """Return unique, normalized wheel members after bounded path validation."""
    infos = archive.infolist()
    if len(infos) > CLIO_KIT_WHEEL_MAX_FILES:
        raise ValueError("clio-kit wheel exceeded its file-count limit")
    members: dict[str, zipfile.ZipInfo] = {}
    for info in infos:
        name = info.filename
        if info.flag_bits & 0x1:
            raise ValueError("clio-kit wheel contains an encrypted member")
        if not name or "\x00" in name or "\\" in name:
            raise ValueError("clio-kit wheel contains an unsafe member path")
        path_text = name[:-1] if info.is_dir() else name
        path = PurePosixPath(path_text)
        first_part = path.parts[0] if path.parts else ""
        if (
            not path_text
            or path_text.startswith("/")
            or path.as_posix() != path_text
            or any(part in {"", ".", ".."} for part in path.parts)
            or (len(first_part) >= 2 and first_part[1] == ":")
        ):
            raise ValueError(f"clio-kit wheel contains an unsafe member path: {name}")
        if name in members:
            raise ValueError("clio-kit wheel contains duplicate member names")
        members[name] = info
    return members


def clio_kit_runtime_project_members(
    members: dict[str, zipfile.ZipInfo],
    *,
    prefix: str,
    server_name: str,
) -> list[tuple[str, zipfile.ZipInfo]]:
    """Select the exact bounded project file set used by clio-kit's v4 launcher."""
    inputs: list[tuple[str, zipfile.ZipInfo]] = []
    relative_files: set[str] = set()
    casefolded_files: set[str] = set()
    declared_bytes = 0
    for name, member in members.items():
        if not name.startswith(prefix) or name == prefix:
            continue
        relative = name[len(prefix) :]
        relative_path = PurePosixPath(relative.rstrip("/"))
        if any(part in _CLIO_KIT_RUNTIME_PROJECT_EXCLUDED_NAMES for part in relative_path.parts):
            continue
        if member.is_dir():
            if not _zip_member_is_directory(member):
                raise ValueError(
                    f"clio-kit embedded server project contains a non-directory: {relative}"
                )
            continue
        if not _zip_member_is_regular(member):
            raise ValueError(
                f"clio-kit embedded server project contains a non-regular file: {relative}"
            )
        if relative in relative_files or relative.casefold() in casefolded_files:
            raise ValueError("clio-kit embedded server project contains colliding paths")
        relative_files.add(relative)
        casefolded_files.add(relative.casefold())
        declared_bytes += member.file_size
        inputs.append((relative, member))
        if (
            len(inputs) > CLIO_KIT_WHEEL_MAX_PROJECT_FILES
            or declared_bytes > CLIO_KIT_WHEEL_MAX_PROJECT_BYTES
        ):
            raise ValueError("clio-kit embedded project exceeded its materialization bound")
    for relative in relative_files:
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            if parent.as_posix() in relative_files:
                raise ValueError("clio-kit embedded server project contains colliding paths")
            parent = parent.parent
    if not {"pyproject.toml", "uv.lock"}.issubset(relative_files):
        raise ValueError(f"clio-kit embedded server project is incomplete: {server_name}")
    return sorted(inputs, key=lambda item: item[0])


def _zip_member_is_regular(member: zipfile.ZipInfo) -> bool:
    """Return whether one ZIP member represents a regular file."""
    if member.is_dir():
        return False
    file_type = stat.S_IFMT((member.external_attr >> 16) & 0xFFFF)
    return file_type in {0, stat.S_IFREG}


def _zip_member_is_directory(member: zipfile.ZipInfo) -> bool:
    """Return whether one ZIP directory entry has a compatible file mode."""
    if not member.is_dir():
        return False
    file_type = stat.S_IFMT((member.external_attr >> 16) & 0xFFFF)
    return file_type in {0, stat.S_IFDIR}


def _file_identity(path: Path) -> dict[str, Any] | None:
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_file():
            return None
        digest = _sha256_file(resolved)
        size_bytes = resolved.stat().st_size
    except OSError:
        return None
    return {
        "path": str(resolved),
        "filename": resolved.name,
        "sha256": digest,
        "size_bytes": size_bytes,
    }


def _sha256_file(path: Path) -> str:
    """Hash one file with fixed memory use."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def read_bounded_zip_member(
    archive: zipfile.ZipFile,
    name: str,
    *,
    max_bytes: int,
) -> bytes:
    """Read one small wheel member after enforcing its decompressed limit."""
    return b"".join(bounded_zip_member_chunks(archive, name, max_bytes=max_bytes))


def bounded_zip_member_chunks(
    archive: zipfile.ZipFile,
    name: str,
    *,
    max_bytes: int,
) -> Iterator[bytes]:
    """Read a wheel member in bounded chunks and reject decompression growth."""
    info = archive.getinfo(name)
    if info.file_size > max_bytes:
        raise ValueError(f"wheel member exceeded its byte limit: {name}")
    observed = 0
    with archive.open(info, "r") as stream:
        while chunk := stream.read(min(FILE_HASH_CHUNK_BYTES, max_bytes - observed + 1)):
            observed += len(chunk)
            if observed > max_bytes:
                raise ValueError(f"wheel member exceeded its byte limit: {name}")
            yield chunk
    if observed != info.file_size:
        raise ValueError(f"wheel member size did not match its directory record: {name}")


def _install_spec_source(install_spec: str | None) -> str | None:
    if install_spec is None:
        return None
    candidate = Path(install_spec).expanduser()
    if candidate.suffix.lower() == ".whl" and candidate.is_file():
        return "wheel"
    package, separator, version = install_spec.rpartition("==")
    if separator and package and version and not any(char.isspace() for char in install_spec):
        return "pypi"
    return "unverified"
