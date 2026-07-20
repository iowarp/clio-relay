"""Safe extraction for inert relay bootstrap tar archives."""

from __future__ import annotations

import ntpath
import os
import shutil
import stat
import tarfile
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, BinaryIO, cast

from clio_relay.errors import RelayError

_COPY_CHUNK_BYTES = 1024 * 1024
_WINDOWS_RESERVED_NAMES = {
    "aux",
    "con",
    "nul",
    "prn",
    *(f"com{index}" for index in range(1, 10)),
    *(f"lpt{index}" for index in range(1, 10)),
}


class UnsafeArchiveError(RelayError):
    """Raised when an archive cannot be proven safe to extract."""


@dataclass(frozen=True, slots=True)
class TarExtractionLimits:
    """Resource and path limits applied before bootstrap archive extraction."""

    max_archive_bytes: int = 1024 * 1024 * 1024
    max_members: int = 20_000
    max_member_bytes: int = 512 * 1024 * 1024
    max_total_bytes: int = 1024 * 1024 * 1024
    max_path_bytes: int = 4096
    max_component_bytes: int = 255

    def __post_init__(self) -> None:
        """Reject limits that would disable or ambiguously coerce a bound."""
        for field_name in (
            "max_archive_bytes",
            "max_members",
            "max_member_bytes",
            "max_total_bytes",
            "max_path_bytes",
            "max_component_bytes",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class TarExtractionReceipt:
    """Summary of one completed, bounded archive extraction."""

    destination: Path
    archive_bytes: int
    member_count: int
    regular_file_count: int
    directory_count: int
    extracted_bytes: int


@dataclass(frozen=True, slots=True)
class _ValidatedMember:
    member: tarfile.TarInfo
    path: PurePosixPath
    is_directory: bool
    executable: bool


def safe_extract_tar(
    archive_path: Path,
    destination: Path,
    *,
    limits: TarExtractionLimits | None = None,
    case_sensitive: bool | None = None,
) -> TarExtractionReceipt:
    """Validate and extract one uncompressed tar into a new private directory.

    The destination must not already exist. Only regular files and directories
    are accepted. Archive ownership, timestamps, and write permissions are not
    applied; extracted files are owner-only and retain only an executable/not
    executable distinction.
    """
    resolved_limits = limits or TarExtractionLimits()
    resolved_case_sensitive = (
        _platform_paths_are_case_sensitive() if case_sensitive is None else case_sensitive
    )

    source = _open_regular_archive(archive_path, max_bytes=resolved_limits.max_archive_bytes)
    archive_bytes = os.fstat(source.fileno()).st_size
    root: Path | None = None
    try:
        with source, tarfile.open(fileobj=source, mode="r:") as archive:
            members = _validate_members(
                archive,
                limits=resolved_limits,
                case_sensitive=resolved_case_sensitive,
            )
            root = _create_private_destination(destination)
            extracted_bytes = _extract_members(archive, members=members, root=root)
    except UnsafeArchiveError:
        if root is not None:
            _remove_failed_destination(root)
        raise
    except (EOFError, OSError, tarfile.TarError) as exc:
        if root is not None:
            _remove_failed_destination(root)
        raise UnsafeArchiveError(f"could not safely extract bootstrap archive: {exc}") from exc

    assert root is not None
    return TarExtractionReceipt(
        destination=root,
        archive_bytes=archive_bytes,
        member_count=len(members),
        regular_file_count=sum(not item.is_directory for item in members),
        directory_count=sum(item.is_directory for item in members),
        extracted_bytes=extracted_bytes,
    )


def _open_regular_archive(path: Path, *, max_bytes: int) -> BinaryIO:
    try:
        path_details = path.lstat()
    except OSError as exc:
        raise UnsafeArchiveError(f"could not inspect bootstrap archive: {exc}") from exc
    if (
        stat.S_ISLNK(path_details.st_mode)
        or _details_are_reparse_point(path_details)
        or not stat.S_ISREG(path_details.st_mode)
    ):
        raise UnsafeArchiveError("bootstrap archive must be a non-reparse regular file")
    flags: int = (
        os.O_RDONLY
        | cast(int, getattr(os, "O_BINARY", 0))
        | cast(int, getattr(os, "O_CLOEXEC", 0))
        | cast(int, getattr(os, "O_NOFOLLOW", 0))
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise UnsafeArchiveError(f"could not open bootstrap archive: {exc}") from exc
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise UnsafeArchiveError("bootstrap archive must be a regular file")
        if (details.st_dev, details.st_ino) != (path_details.st_dev, path_details.st_ino):
            raise UnsafeArchiveError("bootstrap archive identity changed while opening it")
        if not 1 <= details.st_size <= max_bytes:
            raise UnsafeArchiveError("bootstrap archive size exceeds the configured limit")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _validate_members(
    archive: tarfile.TarFile,
    *,
    limits: TarExtractionLimits,
    case_sensitive: bool,
) -> list[_ValidatedMember]:
    validated: list[_ValidatedMember] = []
    member_paths: set[str] = set()
    regular_paths: set[str] = set()
    required_directories: set[str] = set()
    collision_spellings: dict[str, str] = {}
    total_bytes = 0

    for member in archive:
        if len(validated) >= limits.max_members:
            raise UnsafeArchiveError("bootstrap archive member count exceeds the configured limit")
        is_directory = member.isdir()
        if not (is_directory or member.isreg()):
            raise UnsafeArchiveError(
                f"bootstrap archive member is not a regular file or directory: {member.name!r}"
            )
        canonical = _canonical_member_path(
            member.name,
            is_directory=is_directory,
            limits=limits,
        )
        canonical_text = canonical.as_posix() if canonical.parts else ""
        if canonical_text in member_paths:
            raise UnsafeArchiveError(
                f"bootstrap archive contains duplicate normalized path: {member.name!r}"
            )
        member_paths.add(canonical_text)
        _record_case_spellings(
            canonical,
            collision_spellings=collision_spellings,
            case_sensitive=case_sensitive,
        )
        _validate_member_mode(member)
        if is_directory:
            if member.size != 0:
                raise UnsafeArchiveError(
                    f"bootstrap archive directory has a nonzero declared size: {member.name!r}"
                )
        else:
            if not 0 <= member.size <= limits.max_member_bytes:
                raise UnsafeArchiveError(
                    f"bootstrap archive member size exceeds the configured limit: {member.name!r}"
                )
            total_bytes += member.size
            if total_bytes > limits.max_total_bytes:
                raise UnsafeArchiveError(
                    "bootstrap archive declared bytes exceed the configured total limit"
                )

        parents = _parent_paths(canonical)
        if regular_paths.intersection(parents):
            raise UnsafeArchiveError(
                f"bootstrap archive path traverses a regular-file parent: {member.name!r}"
            )
        if not is_directory and canonical_text in required_directories:
            raise UnsafeArchiveError(
                f"bootstrap archive regular file conflicts with a child path: {member.name!r}"
            )
        required_directories.update(parents)
        if is_directory:
            if canonical_text:
                required_directories.add(canonical_text)
        else:
            regular_paths.add(canonical_text)
        validated.append(
            _ValidatedMember(
                member=member,
                path=canonical,
                is_directory=is_directory,
                executable=bool(member.mode & 0o111),
            )
        )
    if not validated:
        raise UnsafeArchiveError("bootstrap archive contains no members")
    return validated


def _canonical_member_path(
    name: str,
    *,
    is_directory: bool,
    limits: TarExtractionLimits,
) -> PurePosixPath:
    if not name:
        raise UnsafeArchiveError("bootstrap archive member path is empty")
    try:
        encoded_name = name.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise UnsafeArchiveError("bootstrap archive member path is not valid Unicode") from exc
    if len(encoded_name) > limits.max_path_bytes:
        raise UnsafeArchiveError("bootstrap archive member path exceeds the configured limit")
    if any(ord(character) < 32 or ord(character) == 127 for character in name):
        raise UnsafeArchiveError(f"bootstrap archive member path contains control data: {name!r}")
    if "\\" in name or name.startswith("/") or ntpath.splitdrive(name)[0]:
        raise UnsafeArchiveError(
            f"bootstrap archive member path is absolute or ambiguous: {name!r}"
        )
    if not is_directory and name.endswith("/"):
        raise UnsafeArchiveError(f"bootstrap archive regular-file path ends with '/': {name!r}")

    raw_parts = name.split("/")
    if any(part == ".." for part in raw_parts):
        raise UnsafeArchiveError(f"bootstrap archive member path contains '..': {name!r}")
    parts = tuple(unicodedata.normalize("NFC", part) for part in raw_parts if part not in {"", "."})
    if not parts:
        if is_directory:
            return PurePosixPath()
        raise UnsafeArchiveError(f"bootstrap archive member path resolves to the root: {name!r}")
    for part in parts:
        try:
            encoded_part = part.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise UnsafeArchiveError(
                "bootstrap archive path component is not valid Unicode"
            ) from exc
        if not encoded_part or len(encoded_part) > limits.max_component_bytes:
            raise UnsafeArchiveError(
                f"bootstrap archive path component exceeds the configured limit: {name!r}"
            )
        if ":" in part:
            raise UnsafeArchiveError(
                f"bootstrap archive path component uses a drive or stream separator: {name!r}"
            )
        if os.name == "nt":
            folded_stem = part.rstrip(" .").split(".", 1)[0].casefold()
            if part.endswith((" ", ".")) or folded_stem in _WINDOWS_RESERVED_NAMES:
                raise UnsafeArchiveError(
                    f"bootstrap archive path is not representable safely on Windows: {name!r}"
                )
    canonical = PurePosixPath(*parts)
    canonical_bytes = canonical.as_posix().encode("utf-8", errors="strict")
    if len(canonical_bytes) > limits.max_path_bytes:
        raise UnsafeArchiveError("normalized bootstrap archive path exceeds the configured limit")
    return canonical


def _record_case_spellings(
    path: PurePosixPath,
    *,
    collision_spellings: dict[str, str],
    case_sensitive: bool,
) -> None:
    if case_sensitive:
        return
    for depth in range(1, len(path.parts) + 1):
        spelling = PurePosixPath(*path.parts[:depth]).as_posix()
        key = unicodedata.normalize("NFC", spelling).casefold()
        previous = collision_spellings.setdefault(key, spelling)
        if previous != spelling:
            raise UnsafeArchiveError(
                "bootstrap archive contains paths that collide on a case-insensitive filesystem: "
                f"{previous!r} and {spelling!r}"
            )


def _validate_member_mode(member: tarfile.TarInfo) -> None:
    mode = member.mode
    if mode < 0 or mode > 0o7777:
        raise UnsafeArchiveError(f"bootstrap archive member mode is invalid: {member.name!r}")
    if mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX | 0o022):
        raise UnsafeArchiveError(f"bootstrap archive member mode is unsafe: {member.name!r}")


def _parent_paths(path: PurePosixPath) -> set[str]:
    return {PurePosixPath(*path.parts[:depth]).as_posix() for depth in range(1, len(path.parts))}


def _create_private_destination(destination: Path) -> Path:
    try:
        parent = destination.parent.resolve(strict=True)
    except OSError as exc:
        raise UnsafeArchiveError(f"bootstrap extraction parent is unavailable: {exc}") from exc
    root = parent / destination.name
    if destination.name in {"", ".", ".."}:
        raise UnsafeArchiveError("bootstrap extraction destination must name a new directory")
    try:
        root.mkdir(mode=0o700, exist_ok=False)
    except OSError as exc:
        raise UnsafeArchiveError(
            f"bootstrap extraction destination must be a new directory: {exc}"
        ) from exc
    try:
        _assert_real_directory(root, root=root)
        return root.resolve(strict=True)
    except (OSError, UnsafeArchiveError):
        _remove_failed_destination(root)
        raise


def _extract_members(
    archive: tarfile.TarFile,
    *,
    members: list[_ValidatedMember],
    root: Path,
) -> int:
    extracted_bytes = 0
    for validated in members:
        if not validated.path.parts:
            continue
        if validated.is_directory:
            _ensure_directory(root, validated.path)
            continue
        parent = _ensure_directory(root, validated.path.parent)
        target = parent / validated.path.name
        _assert_lexically_within_root(target, root=root)
        stream = archive.extractfile(validated.member)
        if stream is None:
            raise UnsafeArchiveError(
                f"bootstrap archive regular file has no payload: {validated.member.name!r}"
            )
        observed = _copy_regular_member(
            stream,
            target=target,
            declared_size=validated.member.size,
            executable=validated.executable,
        )
        extracted_bytes += observed
    return extracted_bytes


def _ensure_directory(root: Path, relative: PurePosixPath) -> Path:
    current = root
    for part in relative.parts:
        current /= part
        _assert_lexically_within_root(current, root=root)
        try:
            current.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise UnsafeArchiveError(
                f"could not create bootstrap extraction directory {relative.as_posix()!r}: {exc}"
            ) from exc
        _assert_real_directory(current, root=root)
    return current


def _copy_regular_member(
    source: IO[bytes],
    *,
    target: Path,
    declared_size: int,
    executable: bool,
) -> int:
    safe_mode = 0o700 if executable else 0o600
    descriptor = _open_new_regular_file(target, mode=safe_mode)
    try:
        output = os.fdopen(descriptor, "wb", buffering=0)
    except OSError as exc:
        os.close(descriptor)
        raise UnsafeArchiveError(
            f"could not open bootstrap archive output {target.name!r}: {exc}"
        ) from exc
    try:
        with output:
            output_details = os.fstat(output.fileno())
            if not stat.S_ISREG(output_details.st_mode):
                raise UnsafeArchiveError("bootstrap archive output is not a regular file")
            observed = 0
            while observed <= declared_size:
                remaining = declared_size + 1 - observed
                chunk = source.read(min(_COPY_CHUNK_BYTES, remaining))
                if not chunk:
                    break
                observed += len(chunk)
                if observed > declared_size:
                    raise UnsafeArchiveError(
                        f"bootstrap archive member expanded past its declared size: {target.name!r}"
                    )
                _write_all(output.fileno(), chunk)
            if observed != declared_size:
                raise UnsafeArchiveError(
                    f"bootstrap archive member size changed while reading: {target.name!r}"
                )
            output.flush()
            _set_safe_output_mode(output.fileno(), target=target, mode=safe_mode)
            os.fsync(output.fileno())
            if os.fstat(output.fileno()).st_size != declared_size:
                raise UnsafeArchiveError(
                    f"bootstrap archive output size is not exact: {target.name!r}"
                )
    except UnsafeArchiveError:
        raise
    except OSError as exc:
        raise UnsafeArchiveError(
            f"could not write bootstrap archive member {target.name!r}: {exc}"
        ) from exc
    return observed


def _open_new_regular_file(path: Path, *, mode: int) -> int:
    flags: int = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | cast(int, getattr(os, "O_BINARY", 0))
        | cast(int, getattr(os, "O_CLOEXEC", 0))
        | cast(int, getattr(os, "O_NOFOLLOW", 0))
    )
    try:
        return os.open(path, flags, mode)
    except OSError as exc:
        raise UnsafeArchiveError(f"could not create bootstrap archive output: {exc}") from exc


def _write_all(descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise UnsafeArchiveError("bootstrap archive output write made no progress")
        view = view[written:]


def _set_safe_output_mode(descriptor: int, *, target: Path, mode: int) -> None:
    fchmod = cast(Callable[[int, int], None] | None, getattr(os, "fchmod", None))
    if fchmod is not None:
        fchmod(descriptor, mode)
        return
    try:
        os.chmod(target, mode, follow_symlinks=False)
    except NotImplementedError:
        os.chmod(target, mode)


def _assert_real_directory(path: Path, *, root: Path) -> None:
    try:
        details = path.lstat()
    except OSError as exc:
        raise UnsafeArchiveError(f"could not inspect bootstrap extraction path: {exc}") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or _details_are_reparse_point(details)
        or not stat.S_ISDIR(details.st_mode)
    ):
        raise UnsafeArchiveError("bootstrap extraction path is not a real directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise UnsafeArchiveError(f"could not resolve bootstrap extraction path: {exc}") from exc
    if resolved != root and not resolved.is_relative_to(root):
        raise UnsafeArchiveError("bootstrap extraction path escapes its destination root")


def _assert_lexically_within_root(path: Path, *, root: Path) -> None:
    if path == root or path.is_relative_to(root):
        return
    raise UnsafeArchiveError("bootstrap extraction path escapes its destination root")


def _remove_failed_destination(destination: Path) -> None:
    try:
        if destination.is_symlink():
            raise UnsafeArchiveError(
                "bootstrap extraction failed and its destination identity changed"
            )
        shutil.rmtree(destination)
    except FileNotFoundError:
        return
    except UnsafeArchiveError:
        raise
    except OSError as exc:
        raise UnsafeArchiveError(
            f"bootstrap extraction failed and its partial destination could not be removed: {exc}"
        ) from exc


def _platform_paths_are_case_sensitive() -> bool:
    return os.path.normcase("ClioRelay") != os.path.normcase("cliorelay")


def _details_are_reparse_point(details: os.stat_result) -> bool:
    reparse_flag = cast(int, getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    file_attributes = cast(int, getattr(details, "st_file_attributes", 0))
    return bool(file_attributes & reparse_flag)
