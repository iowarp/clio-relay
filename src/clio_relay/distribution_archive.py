"""Safe, bounded inspection of wheel and sdist release distribution archives.

The owner for "read exactly what a wheel/sdist claims to contain without
executing any packaged code" -- ZIP/tar member-by-member bounded reads, path
safety, ZIP64/central-directory preflight, and core-metadata (PKG-INFO /
dist-info METADATA) identity binding, assembled into one
``build_distribution_archive_receipt``. Extracted from ``ci_validation.py``
per clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import email.parser
import email.policy
import gzip
import os
import re
import stat
import struct
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from clio_relay.provenance_primitives import (
    MAX_DISTRIBUTION_BYTES,
    MAX_DISTRIBUTION_MEMBER_BYTES,
    MAX_DISTRIBUTION_MEMBERS,
    MAX_DISTRIBUTION_METADATA_BYTES,
    MAX_DISTRIBUTION_PATH_LENGTH,
    MAX_DISTRIBUTION_TAR_BYTES,
    MAX_DISTRIBUTION_UNCOMPRESSED_BYTES,
    ProvenanceError,
    _sha256_bounded_file,
)


class _BinaryReader(Protocol):
    def read(self, size: int = -1, /) -> bytes:
        """Read at most ``size`` bytes from an archive member."""
        ...


def build_distribution_archive_receipt(
    wheel_path: Path,
    sdist_path: Path,
    *,
    project: str,
    version: str,
) -> dict[str, object]:
    """Safely inspect exact wheel and sdist bytes without executing package code."""
    canonical_project = _canonical_distribution_name(project)
    if canonical_project != "clio-relay":
        raise ProvenanceError("distribution project identity must be clio-relay")
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?", version) is None:
        raise ProvenanceError("distribution version is not a supported release version")
    wheel = _inspect_wheel_archive(
        wheel_path,
        expected_project=canonical_project,
        expected_version=version,
    )
    sdist = _inspect_sdist_archive(
        sdist_path,
        expected_project=canonical_project,
        expected_version=version,
    )
    return {
        "schema_version": "clio-relay.distribution-archives.v1",
        "project": canonical_project,
        "version": version,
        "limits": {
            "maximum_archive_bytes": MAX_DISTRIBUTION_BYTES,
            "maximum_members": MAX_DISTRIBUTION_MEMBERS,
            "maximum_member_bytes": MAX_DISTRIBUTION_MEMBER_BYTES,
            "maximum_uncompressed_bytes": MAX_DISTRIBUTION_UNCOMPRESSED_BYTES,
            "maximum_uncompressed_tar_bytes": MAX_DISTRIBUTION_TAR_BYTES,
            "maximum_metadata_bytes": MAX_DISTRIBUTION_METADATA_BYTES,
            "maximum_path_length": MAX_DISTRIBUTION_PATH_LENGTH,
        },
        "wheel": wheel,
        "sdist": sdist,
    }


def _inspect_wheel_archive(
    path: Path,
    *,
    expected_project: str,
    expected_version: str,
) -> dict[str, object]:
    subject = _distribution_subject(path, kind="wheel")
    seen: set[str] = set()
    files: set[str] = set()
    directories: set[str] = set()
    metadata_documents: dict[str, bytes] = {}
    aggregate = 0
    member_count = 0
    try:
        declared_members = _preflight_zip_member_count(path)
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                member_count += 1
                if member_count > MAX_DISTRIBUTION_MEMBERS:
                    raise ProvenanceError("wheel member count exceeds the limit")
                is_directory = member.is_dir()
                normalized = _validate_distribution_member_path(
                    member.filename,
                    is_directory=is_directory,
                )
                folded = normalized.casefold()
                if folded in seen:
                    raise ProvenanceError(f"wheel contains a duplicate path: {normalized}")
                seen.add(folded)
                mode = member.external_attr >> 16
                file_type = stat.S_IFMT(mode)
                if is_directory:
                    if file_type not in {0, stat.S_IFDIR} or member.file_size != 0:
                        raise ProvenanceError(f"wheel directory member is invalid: {normalized}")
                    directories.add(normalized)
                    continue
                if file_type not in {0, stat.S_IFREG}:
                    raise ProvenanceError(f"wheel member is not regular: {normalized}")
                if member.flag_bits & 0x1:
                    raise ProvenanceError(f"wheel member is encrypted: {normalized}")
                if member.file_size < 0 or member.file_size > MAX_DISTRIBUTION_MEMBER_BYTES:
                    raise ProvenanceError(f"wheel member exceeds the byte limit: {normalized}")
                aggregate += member.file_size
                if aggregate > MAX_DISTRIBUTION_UNCOMPRESSED_BYTES:
                    raise ProvenanceError("wheel uncompressed aggregate exceeds the limit")
                files.add(normalized)
                capture = normalized.endswith(".dist-info/METADATA")
                with archive.open(member, "r") as stream:
                    content = _read_declared_archive_member(
                        stream,
                        declared_size=member.file_size,
                        label=f"wheel member {normalized}",
                        capture=capture,
                    )
                if capture:
                    metadata_documents[normalized] = content
    except ProvenanceError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, NotImplementedError) as exc:
        raise ProvenanceError(f"could not safely inspect wheel archive: {exc}") from exc
    if member_count == 0:
        raise ProvenanceError("wheel archive contains no members")
    if member_count != declared_members:
        raise ProvenanceError("wheel central-directory member count changed while reading")
    _verify_distribution_path_topology(files, directories, kind="wheel")
    if len(metadata_documents) != 1:
        raise ProvenanceError("wheel must contain exactly one dist-info/METADATA")
    metadata_path, metadata = next(iter(metadata_documents.items()))
    if metadata_path.count("/") != 1:
        raise ProvenanceError("wheel dist-info metadata must be at archive top level")
    if not 1 <= len(metadata) <= MAX_DISTRIBUTION_METADATA_BYTES:
        raise ProvenanceError("wheel metadata size is invalid")
    dist_info = metadata_path.rsplit("/", 1)[0]
    expected_dist_info = (
        f"{expected_project.replace('-', '_')}-{expected_version.replace('-', '_')}.dist-info"
    )
    if dist_info != expected_dist_info:
        raise ProvenanceError("wheel dist-info directory identity does not match the release")
    wheel_prefix = expected_dist_info.removesuffix(".dist-info") + "-"
    if not path.name.startswith(wheel_prefix) or not path.name.endswith(".whl"):
        raise ProvenanceError("wheel filename identity does not match its metadata")
    for required in (f"{dist_info}/WHEEL", f"{dist_info}/RECORD"):
        if required not in files:
            raise ProvenanceError(f"wheel is missing required metadata member: {required}")
    _verify_core_metadata(
        metadata,
        expected_project=expected_project,
        expected_version=expected_version,
        label="wheel",
    )
    return {
        **subject,
        "member_count": member_count,
        "uncompressed_bytes": aggregate,
        "metadata_path": metadata_path,
    }


def _inspect_sdist_archive(
    path: Path,
    *,
    expected_project: str,
    expected_version: str,
) -> dict[str, object]:
    subject = _distribution_subject(path, kind="sdist")
    seen: set[str] = set()
    files: set[str] = set()
    directories: set[str] = set()
    roots: set[str] = set()
    metadata_documents: dict[str, bytes] = {}
    aggregate = 0
    member_count = 0
    try:
        with tempfile.TemporaryFile(mode="w+b") as bounded_tar:
            bounded_stream = cast(BinaryIO, bounded_tar)
            _inflate_sdist_to_bounded_tar(path, bounded_stream)
            with tarfile.open(fileobj=bounded_stream, mode="r:") as archive:
                for member in archive:
                    member_count += 1
                    if member_count > MAX_DISTRIBUTION_MEMBERS:
                        raise ProvenanceError("sdist member count exceeds the limit")
                    is_directory = member.isdir()
                    normalized = _validate_distribution_member_path(
                        member.name,
                        is_directory=is_directory,
                    )
                    folded = normalized.casefold()
                    if folded in seen:
                        raise ProvenanceError(f"sdist contains a duplicate path: {normalized}")
                    seen.add(folded)
                    roots.add(normalized.split("/", 1)[0])
                    if is_directory:
                        directories.add(normalized)
                        continue
                    if not member.isreg():
                        raise ProvenanceError(f"sdist member is not regular: {normalized}")
                    if member.size < 0 or member.size > MAX_DISTRIBUTION_MEMBER_BYTES:
                        raise ProvenanceError(f"sdist member exceeds the byte limit: {normalized}")
                    aggregate += member.size
                    if aggregate > MAX_DISTRIBUTION_UNCOMPRESSED_BYTES:
                        raise ProvenanceError("sdist uncompressed aggregate exceeds the limit")
                    files.add(normalized)
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise ProvenanceError(f"could not read regular sdist member: {normalized}")
                    capture = normalized.count("/") == 1 and normalized.endswith("/PKG-INFO")
                    with stream:
                        content = _read_declared_archive_member(
                            stream,
                            declared_size=member.size,
                            label=f"sdist member {normalized}",
                            capture=capture,
                        )
                    if capture:
                        metadata_documents[normalized] = content
    except ProvenanceError:
        raise
    except (OSError, EOFError, tarfile.TarError) as exc:
        raise ProvenanceError(f"could not safely inspect sdist archive: {exc}") from exc
    if member_count == 0:
        raise ProvenanceError("sdist archive contains no members")
    if len(roots) != 1:
        raise ProvenanceError("sdist must contain exactly one top-level directory")
    _verify_distribution_path_topology(files, directories, kind="sdist")
    if len(metadata_documents) != 1:
        raise ProvenanceError("sdist must contain exactly one top-level PKG-INFO")
    metadata_path, metadata = next(iter(metadata_documents.items()))
    if not 1 <= len(metadata) <= MAX_DISTRIBUTION_METADATA_BYTES:
        raise ProvenanceError("sdist metadata size is invalid")
    root = next(iter(roots))
    if root != f"{expected_project.replace('-', '_')}-{expected_version}":
        raise ProvenanceError("sdist top-level directory identity does not match the release")
    if path.name != f"{root}.tar.gz":
        raise ProvenanceError("sdist filename identity does not match its top-level directory")
    if f"{root}/pyproject.toml" not in files:
        raise ProvenanceError("sdist is missing its top-level pyproject.toml")
    _verify_core_metadata(
        metadata,
        expected_project=expected_project,
        expected_version=expected_version,
        label="sdist",
    )
    return {
        **subject,
        "member_count": member_count,
        "uncompressed_bytes": aggregate,
        "metadata_path": metadata_path,
        "top_level_directory": root,
    }


def _inflate_sdist_to_bounded_tar(path: Path, target: BinaryIO) -> None:
    """Inflate a gzip sdist into a private file without crossing the tar ceiling."""
    written = 0
    try:
        with gzip.open(path, "rb") as source:
            while True:
                remaining = MAX_DISTRIBUTION_TAR_BYTES - written
                chunk = source.read(min(1024 * 1024, remaining + 1))
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_DISTRIBUTION_TAR_BYTES:
                    raise ProvenanceError("sdist uncompressed tar stream exceeds the byte limit")
                if target.write(chunk) != len(chunk):
                    raise ProvenanceError("sdist temporary tar write was incomplete")
        if written < 1:
            raise ProvenanceError("sdist uncompressed tar stream is empty")
        target.flush()
        os.fsync(target.fileno())
        target.seek(0)
    except ProvenanceError:
        raise
    except (EOFError, gzip.BadGzipFile, OSError) as exc:
        raise ProvenanceError(f"could not safely inflate sdist archive: {exc}") from exc


def _distribution_subject(path: Path, *, kind: str) -> dict[str, object]:
    try:
        details = path.lstat()
    except OSError as exc:
        raise ProvenanceError(f"could not inspect {kind} archive: {exc}") from exc
    if path.is_symlink() or not stat.S_ISREG(details.st_mode):
        raise ProvenanceError(f"{kind} archive is not a regular file")
    if details.st_size < 1 or details.st_size > MAX_DISTRIBUTION_BYTES:
        raise ProvenanceError(f"{kind} archive compressed size is invalid")
    return {
        "filename": path.name,
        "size_bytes": details.st_size,
        "sha256": _sha256_bounded_file(path, maximum_bytes=MAX_DISTRIBUTION_BYTES),
    }


def _preflight_zip_member_count(path: Path) -> int:
    """Reject oversized or ZIP64 central directories before ``ZipFile`` allocates them."""
    size = path.stat().st_size
    tail_size = min(size, 65557)
    with path.open("rb") as stream:
        stream.seek(size - tail_size)
        tail = stream.read(tail_size)
    marker = tail.rfind(b"PK\x05\x06")
    if marker < 0 or len(tail) - marker < 22:
        raise ProvenanceError("wheel end-of-central-directory record is missing")
    (
        signature,
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
        comment_size,
    ) = struct.unpack_from("<4s4H2LH", tail, marker)
    if signature != b"PK\x05\x06" or marker + 22 + comment_size != len(tail):
        raise ProvenanceError("wheel end-of-central-directory record is invalid")
    if disk_number != 0 or central_disk != 0 or disk_entries != total_entries:
        raise ProvenanceError("wheel uses unsupported multi-disk ZIP topology")
    if total_entries in {0, 0xFFFF} or total_entries > MAX_DISTRIBUTION_MEMBERS:
        raise ProvenanceError("wheel central-directory member count exceeds the limit")
    if central_size == 0xFFFFFFFF or central_offset == 0xFFFFFFFF:
        raise ProvenanceError("wheel uses unsupported ZIP64 topology")
    if central_size > size or central_offset > size or central_offset + central_size > size:
        raise ProvenanceError("wheel central-directory bounds are invalid")
    return total_entries


def _validate_distribution_member_path(name: str, *, is_directory: bool) -> str:
    if not name or len(name) > MAX_DISTRIBUTION_PATH_LENGTH:
        raise ProvenanceError("distribution archive member path is empty or too long")
    has_control = any(ord(char) < 32 or ord(char) == 127 for char in name)
    if "\\" in name or name.startswith("/") or has_control:
        raise ProvenanceError(f"distribution archive member path is unsafe: {name!r}")
    normalized = name[:-1] if is_directory and name.endswith("/") else name
    if not normalized or (not is_directory and name.endswith("/")):
        raise ProvenanceError(f"distribution archive member path is invalid: {name!r}")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts) or ":" in parts[0]:
        raise ProvenanceError(f"distribution archive member path is unsafe: {name!r}")
    return normalized


def _verify_distribution_path_topology(
    files: set[str],
    directories: set[str],
    *,
    kind: str,
) -> None:
    for path in files | directories:
        parts = path.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            if parent in files:
                raise ProvenanceError(f"{kind} path traverses a regular-file parent: {path}")


def _read_declared_archive_member(
    stream: _BinaryReader,
    *,
    declared_size: int,
    label: str,
    capture: bool,
) -> bytes:
    if declared_size < 0:
        raise ProvenanceError(f"{label} has a negative declared size")
    content = bytearray()
    observed = 0
    while True:
        chunk = stream.read(min(1024 * 1024, declared_size + 1 - observed))
        if not chunk:
            break
        observed += len(chunk)
        if observed > declared_size:
            raise ProvenanceError(f"{label} expanded past its declared size")
        if capture:
            if observed > MAX_DISTRIBUTION_METADATA_BYTES:
                raise ProvenanceError(f"{label} metadata exceeds the byte limit")
            content.extend(chunk)
    if observed != declared_size:
        raise ProvenanceError(f"{label} size changed while reading")
    return bytes(content)


def _verify_core_metadata(
    content: bytes,
    *,
    expected_project: str,
    expected_version: str,
    label: str,
) -> None:
    metadata = email.parser.BytesParser(policy=email.policy.default).parsebytes(content)
    names = metadata.get_all("Name", [])
    versions = metadata.get_all("Version", [])
    if len(names) != 1 or len(versions) != 1:
        raise ProvenanceError(f"{label} core metadata identity is missing or duplicated")
    if (
        _canonical_distribution_name(str(names[0])) != expected_project
        or str(versions[0]) != expected_version
    ):
        raise ProvenanceError(f"{label} core metadata identity does not match the release")


def _canonical_distribution_name(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9._-]+", value) is None:
        raise ProvenanceError("distribution project name is invalid")
    return re.sub(r"[-_.]+", "-", value).lower()
