"""Owned-session API startup receipt signing and atomic publication (#231 rework).

Extracted from ``session_lifecycle.py``: the HMAC signature over a startup
receipt document, and the owner-private atomic write of the receipt file.
``publish_owned_session_api_startup_receipt`` (the public entry point cli.py
calls by module-qualified attribute access) stays resident in
``session_lifecycle.py`` and calls into this module for both operations.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import cast
from uuid import uuid4

from clio_relay.errors import RelayError

_MAX_API_STARTUP_RECEIPT_BYTES = 64 * 1024


def _startup_receipt_signature(document: dict[str, object], *, owner_token: str) -> str:
    unsigned = {key: value for key, value in document.items() if key != "hmac_sha256"}
    payload = json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(owner_token.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _atomic_write_startup_receipt(path: Path, payload: bytes) -> None:
    """Publish one owner-private startup receipt without acquiring the parent-held lock."""
    if len(payload) > _MAX_API_STARTUP_RECEIPT_BYTES:
        raise RelayError("owned API startup receipt exceeds its byte limit")
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned API startup receipt cannot verify the effective user")
    uid = get_effective_uid()
    parent = path.parent
    parent_status = parent.lstat()
    if (
        not stat.S_ISDIR(parent_status.st_mode)
        or parent_status.st_uid != uid
        or stat.S_IMODE(parent_status.st_mode) != 0o700
    ):
        raise RelayError("owned API startup receipt parent is not owner-private")
    directory_fd = os.open(
        parent,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    temporary_name = f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise RelayError("owned API startup receipt write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        existing = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(existing.st_mode)
            or existing.st_uid != uid
            or existing.st_nlink != 1
            or stat.S_IMODE(existing.st_mode) != 0o600
        ):
            raise RelayError("owned API startup receipt target is not owner-private")
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    except FileNotFoundError:
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            os.unlink(temporary_name, dir_fd=directory_fd)
        os.close(directory_fd)
