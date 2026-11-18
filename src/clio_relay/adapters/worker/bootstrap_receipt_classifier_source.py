"""Embedded receipt-classifier program for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). `_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE`
classifies an existing install-receipt.json as current/legacy during ssh
preflight, before any payload is transferred.
"""

from __future__ import annotations

_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE = r"""import json
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
    # Windows may change ctime when this file is opened. Device, inode, mode,
    # size, and mtime still pin the cross-open object; the bounded read and
    # receipt validation protect its payload.
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
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
    if cross_open_identity(opened) != cross_open_identity(details):
        raise SystemExit("bootstrap install receipt changed before reading")
    with os.fdopen(descriptor, "rb", closefd=False) as stream:
        payload = stream.read(4 * 1024 * 1024 + 1)
    after = os.fstat(descriptor)
finally:
    os.close(descriptor)
if (
    len(payload) > 4 * 1024 * 1024
    or change_identity(after) != change_identity(opened)
    or cross_open_identity(read_path.lstat()) != cross_open_identity(details)
    or (
        change_identity(path.lstat())
        if stable_target is not None
        else cross_open_identity(path.lstat())
    )
    != (
        change_identity(stable_details)
        if stable_target is not None
        else cross_open_identity(stable_details)
    )
):
    raise SystemExit("bootstrap install receipt changed while reading")
if stable_target is not None:
    assert current is not None
    assert current_details is not None
    assert current_target_text is not None
    if (
        os.readlink(path) != stable_target
        or change_identity(current.lstat()) != change_identity(current_details)
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
