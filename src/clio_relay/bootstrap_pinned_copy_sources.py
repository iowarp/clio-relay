"""Embedded pinned-artifact copy programs for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). `_BOOTSTRAP_PINNED_UV_COPY_SOURCE`
and `_BOOTSTRAP_PINNED_LOCAL_ARTIFACT_COPY_SOURCE` copy an already-verified
pinned uv / local artifact into the staged generation without re-fetching.
"""

from __future__ import annotations

_BOOTSTRAP_PINNED_UV_COPY_SOURCE = r"""import hashlib
import os
import stat
import sys
from pathlib import Path


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
    # Opening or copying a Windows file may churn ctime. Device, inode, mode,
    # size, and mtime plus the SHA-256 digest retain the complete integrity pin.
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
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
    if cross_open_identity(source_opened) != cross_open_identity(source_before):
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
        or change_identity(source_after) != change_identity(source_opened)
        or cross_open_identity(source_linked_after) != cross_open_identity(source_opened)
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
        if cross_open_identity(verification_opened) != cross_open_identity(
            destination_written
        ):
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
        or change_identity(verification_after) != change_identity(verification_opened)
        or cross_open_identity(linked_copy) != cross_open_identity(verification_opened)
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
_BOOTSTRAP_PINNED_LOCAL_ARTIFACT_COPY_SOURCE = r"""import hashlib
import os
import stat
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


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
    # Opening or copying a Windows file may churn ctime. Device, inode, mode,
    # size, and mtime plus the SHA-256 digest retain the complete integrity pin.
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
    )


source_url, expected_sha256, destination_value, allowed_root_value = sys.argv[1:]
parsed = urlsplit(source_url)
source_path_value = unquote(parsed.path)
if os.name == "nt" and len(source_path_value) > 2 and source_path_value[0] == "/":
    source_path_value = source_path_value[1:]
if (
    parsed.scheme != "file"
    or parsed.netloc
    or parsed.query
    or parsed.fragment
    or not source_path_value
    or any(character in source_path_value for character in "\x00\r\n")
    or len(expected_sha256) != 64
    or any(character not in "0123456789abcdef" for character in expected_sha256)
):
    raise SystemExit("pinned local artifact arguments are invalid")

source = Path(source_path_value)
destination = Path(destination_value)
allowed_root = Path(allowed_root_value)
if not source.is_absolute() or not destination.is_absolute() or not allowed_root.is_absolute():
    raise SystemExit("pinned local artifact paths must be absolute")
try:
    allowed_root_before = allowed_root.lstat()
    source_before = source.lstat()
    destination_before = destination.lstat()
    resolved_allowed_root = allowed_root.resolve(strict=True)
    resolved_source = source.resolve(strict=True)
    resolved_source.relative_to(resolved_allowed_root)
except (OSError, RuntimeError, ValueError) as exc:
    raise SystemExit("pinned local artifact escaped its managed staging root") from exc
getuid = getattr(os, "getuid", None)
if (
    not stat.S_ISDIR(allowed_root_before.st_mode)
    or allowed_root.is_symlink()
    or not stat.S_ISREG(source_before.st_mode)
    or source.is_symlink()
    or not 1 <= source_before.st_size <= 256 * 1024 * 1024
    or not stat.S_ISREG(destination_before.st_mode)
    or destination.is_symlink()
    or destination_before.st_nlink != 1
    or destination_before.st_size != 0
    or (callable(getuid) and allowed_root_before.st_uid != getuid())
    or (callable(getuid) and source_before.st_uid != getuid())
    or (callable(getuid) and destination_before.st_uid != getuid())
):
    raise SystemExit("pinned local artifact paths are not owned bounded files")

read_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
write_flags = os.O_WRONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
source_descriptor = os.open(source, read_flags)
destination_descriptor = os.open(destination, write_flags)
try:
    source_opened = os.fstat(source_descriptor)
    destination_opened = os.fstat(destination_descriptor)
    if (
        cross_open_identity(source_opened) != cross_open_identity(source_before)
        or cross_open_identity(destination_opened)
        != cross_open_identity(destination_before)
    ):
        raise SystemExit("pinned local artifact changed before copying")
    digest = hashlib.sha256()
    copied = 0
    while chunk := os.read(source_descriptor, 1024 * 1024):
        copied += len(chunk)
        if copied > 256 * 1024 * 1024:
            raise SystemExit("pinned local artifact exceeded its copy bound")
        digest.update(chunk)
        view = memoryview(chunk)
        while view:
            written = os.write(destination_descriptor, view)
            if written < 1:
                raise SystemExit("pinned local artifact copy made no progress")
            view = view[written:]
    os.fsync(destination_descriptor)
    source_after = os.fstat(source_descriptor)
    destination_after = os.fstat(destination_descriptor)
    if (
        copied != source_opened.st_size
        or digest.hexdigest() != expected_sha256
        or change_identity(source_after) != change_identity(source_opened)
        or cross_open_identity(source.lstat()) != cross_open_identity(source_opened)
        or destination_after.st_size != copied
        or destination_after.st_nlink != 1
        or (destination_after.st_dev, destination_after.st_ino)
        == (source_opened.st_dev, source_opened.st_ino)
    ):
        os.ftruncate(destination_descriptor, 0)
        os.fsync(destination_descriptor)
        raise SystemExit("pinned local artifact changed or did not match its digest")
finally:
    os.close(destination_descriptor)
    os.close(source_descriptor)

if cross_open_identity(destination.lstat()) != cross_open_identity(destination_after):
    raise SystemExit("pinned local artifact destination changed after copying")"""
