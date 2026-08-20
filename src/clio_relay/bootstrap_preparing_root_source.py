"""Embedded preparing-root staging program for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). `_BOOTSTRAP_PREPARING_ROOT_SOURCE`
stages the next generation's root directory ahead of activation.
"""

from __future__ import annotations

_BOOTSTRAP_PREPARING_ROOT_SOURCE = r"""import os
import stat
import sys
from pathlib import Path


def identity(details):
    # Opening a Windows directory may churn ctime. Device, inode, mode, size,
    # and mtime still pin the exact directory across the open boundary.
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_size,
        details.st_mtime_ns,
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
