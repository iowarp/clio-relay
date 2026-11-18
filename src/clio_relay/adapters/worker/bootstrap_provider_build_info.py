"""Safely stage installed-provider build identity into a bootstrap overlay."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from csv import Error as CsvError
from csv import reader as csv_reader
from importlib import metadata
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import cast

_BUILD_INFO_PARTS = ("clio_relay", "_build_info.json")
_MAX_BUILD_INFO_BYTES = 64 * 1024
_MAX_RECORD_BYTES = 8 * 1024 * 1024


def _identity(details: os.stat_result) -> tuple[int, int, int, int, int]:
    """Return the fields used to detect replacement or mutation."""
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
    )


def _read_pinned_regular(path: Path, *, require_single_link: bool) -> tuple[bytes, os.stat_result]:
    """Read one bounded regular file while pinning and reverifying its inode."""
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or not 0 < before.st_size <= _MAX_BUILD_INFO_BYTES
        or (require_single_link and before.st_nlink != 1)
    ):
        raise RuntimeError("provider build info must be one bounded regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _identity(opened) != _identity(before):
            raise RuntimeError("provider build info changed before its pinned read")
        chunks: list[bytes] = []
        remaining = _MAX_BUILD_INFO_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 16 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after_descriptor = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = path.lstat()
    if (
        len(payload) > _MAX_BUILD_INFO_BYTES
        or _identity(after_descriptor) != _identity(opened)
        or _identity(after_path) != _identity(opened)
    ):
        raise RuntimeError("provider build info changed during its pinned read")
    return payload, opened


def _validate_build_info(payload: bytes, *, distribution_version: str) -> None:
    """Require the exact build-info schema emitted by the release build hook."""
    try:
        loaded = cast(object, json.loads(payload.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("provider build info is not valid UTF-8 JSON") from exc
    if not isinstance(loaded, dict):
        raise RuntimeError("provider build info must be one JSON object")
    document = cast(dict[str, object], loaded)
    if set(document) != {"version", "commit", "tag", "dirty"}:
        raise RuntimeError("provider build info has an unsupported schema")
    if document["version"] != distribution_version:
        raise RuntimeError("provider build info version does not match its distribution")
    commit = document["commit"]
    tag = document["tag"]
    dirty = document["dirty"]
    if commit is not None and (
        not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None
    ):
        raise RuntimeError("provider build info commit is invalid")
    if tag is not None and (
        not isinstance(tag, str)
        or not tag
        or len(tag) > 256
        or any(ord(character) < 0x21 or ord(character) == 0x7F for character in tag)
    ):
        raise RuntimeError("provider build info tag is invalid")
    if dirty is not None and not isinstance(dirty, bool):
        raise RuntimeError("provider build info dirty flag is invalid")


def _write_candidate_copy(
    destination: Path,
    payload: bytes,
    *,
    source_details: os.stat_result,
) -> None:
    """Create or verify the private candidate copy without following links."""
    try:
        existing, candidate_details = _read_pinned_regular(
            destination,
            require_single_link=True,
        )
    except FileNotFoundError:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(destination, flags, 0o600)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise RuntimeError("provider build info candidate write made no progress")
                view = view[written:]
            os.fsync(descriptor)
            descriptor_chmod = getattr(os, "fchmod", None)
            if descriptor_chmod is not None:
                descriptor_chmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        existing, candidate_details = _read_pinned_regular(
            destination,
            require_single_link=True,
        )
    if existing != payload:
        raise RuntimeError("candidate build info does not match the installed provider")
    if (candidate_details.st_dev, candidate_details.st_ino) == (
        source_details.st_dev,
        source_details.st_ino,
    ):
        raise RuntimeError("candidate build info must be an independent private copy")


def stage_installed_provider_build_info(destination_package: Path) -> bool:
    """Stage exact embedded identity from the interpreter's installed relay provider.

    A provider distribution without an embedded build-info record is supported for
    legacy upgrades, but an unexplained candidate build-info file is never used.
    """
    destination_details = destination_package.lstat()
    if not stat.S_ISDIR(destination_details.st_mode):
        raise RuntimeError("bootstrap candidate package must be one real directory")
    destination = destination_package / "_build_info.json"
    try:
        distribution = metadata.distribution("clio-relay")
    except metadata.PackageNotFoundError:
        try:
            destination.lstat()
        except FileNotFoundError:
            return False
        raise RuntimeError("candidate build info exists without an installed provider") from None

    record = distribution.read_text("RECORD")
    if record is not None:
        if len(record.encode("utf-8")) > _MAX_RECORD_BYTES:
            raise RuntimeError("provider distribution record exceeds its bound")
        try:
            rows = list(csv_reader(StringIO(record), strict=True))
        except CsvError as exc:
            raise RuntimeError("provider distribution record is malformed") from exc
        matches = [
            metadata.PackagePath(row[0])
            for row in rows
            if row and PurePosixPath(row[0].replace("\\", "/")).parts == _BUILD_INFO_PARTS
        ]
    else:
        matches = [
            item
            for item in distribution.files or []
            if PurePosixPath(str(item).replace("\\", "/")).parts == _BUILD_INFO_PARTS
        ]
    if not matches:
        try:
            destination.lstat()
        except FileNotFoundError:
            return False
        raise RuntimeError("candidate build info exists without provider metadata") from None
    if len(matches) != 1:
        raise RuntimeError("provider metadata contains ambiguous build-info records")
    source = Path(str(distribution.locate_file(matches[0])))
    try:
        payload, source_details = _read_pinned_regular(source, require_single_link=False)
    except FileNotFoundError:
        raise RuntimeError("provider build info recorded source is missing") from None
    _validate_build_info(payload, distribution_version=distribution.version)
    _write_candidate_copy(destination, payload, source_details=source_details)
    return True


def main(arguments: list[str] | None = None) -> int:
    """Stage provider build info for the rendered bootstrap script."""
    values = list(sys.argv[1:] if arguments is None else arguments)
    if len(values) != 1:
        raise SystemExit("expected exactly one candidate package path")
    try:
        staged = stage_installed_provider_build_info(Path(values[0]))
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f"provider build info staging failed: {exc}") from exc
    print("bootstrap_provider_build_info=" + ("staged" if staged else "unavailable"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
