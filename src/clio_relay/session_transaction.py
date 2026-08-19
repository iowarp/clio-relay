"""Pinned owned-session directory transaction primitive (#231 rework slice).

Extracted from ``session_lifecycle.py``: pins one owner-private session
directory by descriptor, acquires its no-follow transition lock, and performs
every bounded, identity-verified read/write against it. Every other
owned-session lifecycle module builds on this primitive.
"""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import re
import stat
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from clio_relay.errors import RelayError
from clio_relay.session_validation import _validate_session

_MAX_OWNED_SESSION_DOCUMENT_BYTES = 1024 * 1024
_MAX_OWNED_SESSION_DIRECTORY_ENTRIES = 256
_MAX_OWNED_SESSION_CLEANUP_REPORT_CANDIDATES = 4
_OWNED_SESSION_LOCK_RETRY_SECONDS = 0.05
_CLEANUP_REPORT_SIDECAR_PATTERN = re.compile(r"^coordinator-cleanup-report-[0-9a-f]{64}\.json$")
_CLEANUP_REPORT_PENDING_PATTERN = re.compile(
    r"^\.coordinator-cleanup-report-[0-9a-f]{64}\.json\.pending$"
)


class _FcntlModule(Protocol):
    """Typed surface for the POSIX-only advisory lock module."""

    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int

    def flock(self, fd: int, operation: int) -> Any:
        """Apply an advisory lock operation to an open descriptor."""


@dataclass
class _OwnedSessionTransaction:
    """Pinned owner-private session directory and exact transition-lock inode."""

    session_id: str
    path: Path
    sessions_fd: int
    directory_fd: int
    lock_fd: int
    uid: int
    _fcntl: _FcntlModule

    def __enter__(self) -> _OwnedSessionTransaction:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Release the exact lock and close pinned descriptors."""
        if self.lock_fd >= 0:
            with suppress(OSError):
                self._fcntl.flock(self.lock_fd, self._fcntl.LOCK_UN)
            with suppress(OSError):
                os.close(self.lock_fd)
            self.lock_fd = -1
        if self.directory_fd >= 0:
            with suppress(OSError):
                os.close(self.directory_fd)
            self.directory_fd = -1
        if self.sessions_fd >= 0:
            with suppress(OSError):
                os.close(self.sessions_fd)
            self.sessions_fd = -1

    def read_bytes(
        self,
        name: str,
        *,
        maximum_bytes: int,
        required: bool = True,
    ) -> bytes | None:
        """Read one exact bounded regular file without following links."""
        _validate_owned_session_filename(name)
        descriptor: int | None = None
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=self.directory_fd,
            )
        except FileNotFoundError:
            if required:
                raise RelayError(f"owned session file is unavailable: {name}") from None
            return None
        except OSError as exc:
            raise RelayError(f"owned session file cannot be opened safely: {name}: {exc}") from exc
        try:
            opened = os.fstat(descriptor)
            linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            _verify_owned_session_file(
                opened,
                linked,
                uid=self.uid,
                name=name,
            )
            if not 0 <= opened.st_size <= maximum_bytes:
                raise RelayError(f"owned session file exceeds its byte limit: {name}")
            payload = bytearray()
            while len(payload) <= maximum_bytes:
                chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
            if len(payload) != opened.st_size or len(payload) > maximum_bytes:
                raise RelayError(f"owned session file changed or exceeded its limit: {name}")
            final_opened = os.fstat(descriptor)
            final_linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            _verify_owned_session_file(
                final_opened,
                final_linked,
                uid=self.uid,
                name=name,
            )
            initial_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            final_identity = (
                final_opened.st_dev,
                final_opened.st_ino,
                final_opened.st_size,
                final_opened.st_mtime_ns,
                final_opened.st_ctime_ns,
            )
            if final_identity != initial_identity:
                raise RelayError(f"owned session file changed while it was read: {name}")
            return bytes(payload)
        finally:
            os.close(descriptor)

    def read_tail(
        self,
        name: str,
        *,
        maximum_bytes: int,
        required: bool = True,
    ) -> bytes | None:
        """Read a bounded tail from one pinned owner-private regular file.

        Unlike :meth:`read_bytes`, this operation deliberately accepts a file
        larger than ``maximum_bytes``.  It seeks directly to the bounded tail
        and still rejects replacement, truncation, growth, or metadata changes
        while the descriptor is being observed.
        """
        _validate_owned_session_filename(name)
        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        descriptor: int | None = None
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=self.directory_fd,
            )
        except FileNotFoundError:
            if required:
                raise RelayError(f"owned session file is unavailable: {name}") from None
            return None
        except OSError as exc:
            raise RelayError(f"owned session file cannot be opened safely: {name}: {exc}") from exc
        try:
            opened = os.fstat(descriptor)
            linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            _verify_owned_session_file(opened, linked, uid=self.uid, name=name)
            initial_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            tail_size = min(opened.st_size, maximum_bytes)
            os.lseek(descriptor, opened.st_size - tail_size, os.SEEK_SET)
            payload = bytearray()
            while len(payload) < tail_size:
                chunk = os.read(descriptor, min(64 * 1024, tail_size - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
            final_opened = os.fstat(descriptor)
            final_linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            _verify_owned_session_file(final_opened, final_linked, uid=self.uid, name=name)
            final_identity = (
                final_opened.st_dev,
                final_opened.st_ino,
                final_opened.st_size,
                final_opened.st_mtime_ns,
                final_opened.st_ctime_ns,
            )
            if len(payload) != tail_size or final_identity != initial_identity:
                raise RelayError(f"owned session file changed while its tail was read: {name}")
            return bytes(payload)
        finally:
            os.close(descriptor)

    def read_json(self, name: str, *, required: bool = True) -> dict[str, object] | None:
        """Read one exact bounded UTF-8 JSON object."""
        payload = self.read_bytes(
            name,
            maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
            required=required,
        )
        if payload is None:
            return None
        try:
            raw = cast(object, json.loads(payload))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RelayError(f"owned session file is not valid UTF-8 JSON: {name}: {exc}") from exc
        if not isinstance(raw, dict):
            raise RelayError(f"owned session file is not a JSON object: {name}")
        return {str(key): value for key, value in cast(dict[object, object], raw).items()}

    def cleanup_report_candidate_names(self) -> list[str]:
        """Enumerate bounded report sidecar names through the pinned directory fd."""
        candidates: list[str] = []
        scanned = 0
        with os.scandir(self.directory_fd) as entries:
            for entry in entries:
                scanned += 1
                if scanned > _MAX_OWNED_SESSION_DIRECTORY_ENTRIES:
                    raise RelayError("owned session directory exceeds its entry limit")
                name = entry.name
                resembles_sidecar = name.startswith(
                    "coordinator-cleanup-report-"
                ) or name.startswith(".coordinator-cleanup-report-")
                if not resembles_sidecar:
                    continue
                if not (
                    _CLEANUP_REPORT_SIDECAR_PATTERN.fullmatch(name)
                    or _CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name)
                ):
                    raise RelayError(
                        f"owned session cleanup report candidate has an invalid name: {name}"
                    )
                candidates.append(name)
                if len(candidates) > _MAX_OWNED_SESSION_CLEANUP_REPORT_CANDIDATES:
                    raise RelayError("owned session has too many cleanup report candidates")
        return sorted(candidates)

    def atomic_write(
        self,
        name: str,
        payload: bytes,
        *,
        maximum_bytes: int = _MAX_OWNED_SESSION_DOCUMENT_BYTES,
    ) -> None:
        """Atomically replace one owner-private regular file through the pinned directory."""
        _validate_owned_session_filename(name)
        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        if len(payload) > maximum_bytes:
            raise RelayError(f"owned session write exceeds its byte limit: {name}")
        temporary_name = f".{name}.{os.getpid()}.{uuid4().hex}.tmp"
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
                dir_fd=self.directory_fd,
            )
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise RelayError(f"owned session write made no progress: {name}")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            existing = self.stat_regular(name, required=False)
            if existing is not None and existing.st_uid != self.uid:
                raise RelayError(f"owned session target has a foreign owner: {name}")
            os.replace(
                temporary_name,
                name,
                src_dir_fd=self.directory_fd,
                dst_dir_fd=self.directory_fd,
            )
            os.fsync(self.directory_fd)
        except OSError as exc:
            raise RelayError(
                f"owned session file cannot be replaced safely: {name}: {exc}"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=self.directory_fd)

    def atomic_write_immutable(
        self,
        name: str,
        payload: bytes,
        *,
        maximum_bytes: int,
    ) -> None:
        """Install one immutable sidecar, accepting only exact idempotent reuse."""
        _validate_owned_session_filename(name)
        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        if not payload or len(payload) > maximum_bytes:
            raise RelayError(f"owned session immutable write exceeds its byte limit: {name}")
        pending_name = f".{name}.pending"
        _validate_owned_session_filename(pending_name)

        def linked_status(candidate: str) -> os.stat_result | None:
            try:
                return os.stat(candidate, dir_fd=self.directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                return None

        def read_candidate(candidate: str, *, expected_nlink: int) -> bytes:
            """Read one pinned immutable candidate with an explicit link count."""
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    candidate,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=self.directory_fd,
                )
                opened = os.fstat(descriptor)
                linked = os.stat(candidate, dir_fd=self.directory_fd, follow_symlinks=False)
                if not (
                    stat.S_ISREG(opened.st_mode)
                    and stat.S_ISREG(linked.st_mode)
                    and opened.st_uid == self.uid
                    and linked.st_uid == self.uid
                    and stat.S_IMODE(opened.st_mode) == 0o600
                    and stat.S_IMODE(linked.st_mode) == 0o600
                    and opened.st_nlink == expected_nlink
                    and linked.st_nlink == expected_nlink
                    and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
                    and 0 <= opened.st_size <= maximum_bytes
                ):
                    raise RelayError(f"owned session immutable candidate is unsafe: {candidate}")
                value = bytearray()
                while len(value) <= maximum_bytes:
                    chunk = os.read(
                        descriptor,
                        min(64 * 1024, maximum_bytes + 1 - len(value)),
                    )
                    if not chunk:
                        break
                    value.extend(chunk)
                final_opened = os.fstat(descriptor)
                final_linked = os.stat(
                    candidate,
                    dir_fd=self.directory_fd,
                    follow_symlinks=False,
                )
                initial_identity = (
                    opened.st_dev,
                    opened.st_ino,
                    opened.st_size,
                    opened.st_mtime_ns,
                    opened.st_ctime_ns,
                    opened.st_nlink,
                )
                final_identity = (
                    final_opened.st_dev,
                    final_opened.st_ino,
                    final_opened.st_size,
                    final_opened.st_mtime_ns,
                    final_opened.st_ctime_ns,
                    final_opened.st_nlink,
                )
                if (
                    len(value) != opened.st_size
                    or final_identity != initial_identity
                    or (final_linked.st_dev, final_linked.st_ino, final_linked.st_nlink)
                    != (final_opened.st_dev, final_opened.st_ino, expected_nlink)
                ):
                    raise RelayError(
                        f"owned session immutable candidate changed while read: {candidate}"
                    )
                return bytes(value)
            except OSError as exc:
                raise RelayError(
                    f"owned session immutable candidate cannot be read safely: {candidate}: {exc}"
                ) from exc
            finally:
                if descriptor is not None:
                    os.close(descriptor)

        final_status = linked_status(name)
        pending_status = linked_status(pending_name)
        if final_status is not None and pending_status is not None:
            if (final_status.st_dev, final_status.st_ino) != (
                pending_status.st_dev,
                pending_status.st_ino,
            ):
                raise RelayError(f"owned session immutable publication is ambiguous: {name}")
            if not (
                stat.S_ISREG(final_status.st_mode)
                and final_status.st_uid == self.uid
                and stat.S_IMODE(final_status.st_mode) == 0o600
                and final_status.st_nlink == 2
            ):
                raise RelayError(f"owned session immutable publication is unsafe: {name}")
            # Recover the one crash window after link publication and before the
            # private pending link was removed, but only after validating the
            # linked bytes.  A corrupt final must remain visible for diagnosis.
            linked_payload = read_candidate(pending_name, expected_nlink=2)
            if not hmac.compare_digest(linked_payload, payload):
                raise RelayError(f"owned session immutable linked file differs: {name}")
            os.unlink(pending_name, dir_fd=self.directory_fd)
            os.fsync(self.directory_fd)
            pending_status = None
        existing = self.read_bytes(
            name,
            maximum_bytes=maximum_bytes,
            required=False,
        )
        if existing is not None:
            if hmac.compare_digest(existing, payload):
                return
            raise RelayError(f"owned session immutable file already differs: {name}")
        if pending_status is not None:
            staged = read_candidate(pending_name, expected_nlink=1)
            if not hmac.compare_digest(staged, payload):
                # A pending-only file is unreferenced staging.  Once its exact
                # owner-private identity has been proven it is safe to remove
                # and recreate after an interrupted/ENOSPC write.
                self.unlink_verified(
                    pending_name,
                    expected_device=pending_status.st_dev,
                    expected_inode=pending_status.st_ino,
                    expected_size=pending_status.st_size,
                    expected_sha256=None,
                    maximum_bytes=None,
                )
                pending_status = None
        if pending_status is None:
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    pending_name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=self.directory_fd,
                )
                view = memoryview(payload)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise RelayError(f"owned session immutable write made no progress: {name}")
                    view = view[written:]
                os.fsync(descriptor)
            except OSError as exc:
                raise RelayError(
                    f"owned session immutable file cannot be staged safely: {name}: {exc}"
                ) from exc
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        staged = self.read_bytes(pending_name, maximum_bytes=maximum_bytes)
        if staged is None or not hmac.compare_digest(staged, payload):
            raise RelayError(f"owned session immutable pending file differs: {name}")
        publication_complete = False
        try:
            os.link(
                pending_name,
                name,
                src_dir_fd=self.directory_fd,
                dst_dir_fd=self.directory_fd,
                follow_symlinks=False,
            )
            os.fsync(self.directory_fd)
            publication_complete = True
        except FileExistsError:
            winner = self.read_bytes(name, maximum_bytes=maximum_bytes)
            if winner is None or not hmac.compare_digest(winner, payload):
                raise RelayError(f"owned session immutable file already differs: {name}") from None
            publication_complete = True
        except OSError as exc:
            raise RelayError(
                f"owned session immutable file cannot be published safely: {name}: {exc}"
            ) from exc
        finally:
            if publication_complete:
                with suppress(FileNotFoundError):
                    os.unlink(pending_name, dir_fd=self.directory_fd)
                os.fsync(self.directory_fd)
        reread = self.read_bytes(name, maximum_bytes=maximum_bytes)
        if reread is None or not hmac.compare_digest(reread, payload):
            raise RelayError(f"owned session immutable file changed after commit: {name}")

    def stat_regular(self, name: str, *, required: bool = True) -> os.stat_result | None:
        """Return exact no-follow status for one owner-private regular file."""
        _validate_owned_session_filename(name)
        try:
            linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            if required:
                raise RelayError(f"owned session file is unavailable: {name}") from None
            return None
        _verify_owned_session_file(linked, linked, uid=self.uid, name=name)
        return linked

    def open_output(self, name: str) -> int:
        """Open one owner-private output file through the pinned directory."""
        _validate_owned_session_filename(name)
        existing = self.stat_regular(name, required=False)
        flags = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        flags |= os.O_CREAT | os.O_EXCL if existing is None else 0
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=self.directory_fd)
        except OSError as exc:
            raise RelayError(
                f"owned session output cannot be opened safely: {name}: {exc}"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            linked = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            _verify_owned_session_file(opened, linked, uid=self.uid, name=name)
            if existing is not None and (existing.st_dev, existing.st_ino) != (
                opened.st_dev,
                opened.st_ino,
            ):
                raise RelayError(f"owned session output changed while it was opened: {name}")
            os.ftruncate(descriptor, 0)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def unlink_verified(
        self,
        name: str,
        *,
        expected_device: int,
        expected_inode: int,
        expected_size: int,
        expected_sha256: str | None,
        maximum_bytes: int | None,
    ) -> bool:
        """Delete one file only when its complete pinned identity still matches."""
        linked = self.stat_regular(name, required=False)
        if linked is None:
            return False
        if (linked.st_dev, linked.st_ino, linked.st_size) != (
            expected_device,
            expected_inode,
            expected_size,
        ):
            raise RelayError(f"owned session file identity changed before deletion: {name}")
        if expected_sha256 is not None:
            if maximum_bytes is None:  # pragma: no cover - internal contract
                raise RelayError(f"owned session file digest bound is missing: {name}")
            payload = self.read_bytes(name, maximum_bytes=maximum_bytes)
            if payload is None:  # pragma: no cover - required read
                raise RelayError(f"owned session file disappeared before deletion: {name}")
            if hashlib.sha256(payload).hexdigest() != expected_sha256:
                raise RelayError(f"owned session file digest changed before deletion: {name}")
        final = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
        if (final.st_dev, final.st_ino, final.st_size) != (
            expected_device,
            expected_inode,
            expected_size,
        ):
            raise RelayError(f"owned session file changed before deletion: {name}")
        try:
            os.unlink(name, dir_fd=self.directory_fd)
            os.fsync(self.directory_fd)
        except OSError as exc:
            raise RelayError(
                f"owned session file could not be deleted safely: {name}: {exc}"
            ) from exc
        return True


def _validate_owned_session_filename(name: str) -> None:
    if not name or Path(name).name != name or name in {".", ".."}:
        raise RelayError("owned session filename must be one safe basename")


def _verify_owned_session_file(
    opened: os.stat_result,
    linked: os.stat_result,
    *,
    uid: int,
    name: str,
) -> None:
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(linked.st_mode)
        or opened.st_nlink != 1
        or linked.st_nlink != 1
        or opened.st_uid != uid
        or linked.st_uid != uid
        or stat.S_IMODE(opened.st_mode) != 0o600
        or stat.S_IMODE(linked.st_mode) != 0o600
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        raise RelayError(f"owned session file is not one owner-private regular file: {name}")


def open_owned_session_transaction(
    *,
    session_id: str,
    create: bool,
    timeout_seconds: float,
    home: Path | None = None,
) -> _OwnedSessionTransaction:
    """Pin one session directory and acquire its exact no-follow transition lock."""
    _validate_session(session_id=session_id, remote_api_port=1)
    if os.name != "posix":
        raise RelayError("owned session transactions require POSIX descriptor semantics")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    try:
        fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
    except ImportError as exc:
        raise RelayError("owned session transactions require POSIX fcntl locking") from exc
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session transactions cannot verify the effective user")
    uid = get_effective_uid()
    selected_home = (home or Path.home()).resolve(strict=True)
    descriptors: list[int] = []
    lock_fd: int | None = None
    session_fd: int | None = None
    sessions_fd: int | None = None
    try:
        current = os.open(
            selected_home,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptors.append(current)
        components = (".local", "share", "clio-relay", "sessions", session_id)
        for index, component in enumerate(components):
            try:
                child = os.open(
                    component,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=current,
                )
            except FileNotFoundError:
                if not create:
                    raise RelayError("owned session directory is unavailable") from None
                os.mkdir(component, 0o700, dir_fd=current)
                child = os.open(
                    component,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=current,
                )
            child_status = os.fstat(child)
            private_component = index >= 2
            if (
                not stat.S_ISDIR(child_status.st_mode)
                or child_status.st_uid != uid
                or (private_component and stat.S_IMODE(child_status.st_mode) & 0o022)
            ):
                os.close(child)
                raise RelayError("owned session directory path is not owner-private")
            descriptors.append(child)
            current = child
            if index == 3:
                sessions_fd = child
        session_fd = descriptors.pop()
        lock_flags = (
            os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | (os.O_CREAT if create else 0)
        )
        lock_fd = os.open("transition.lock", lock_flags, 0o600, dir_fd=session_fd)
        opened = os.fstat(lock_fd)
        linked = os.stat("transition.lock", dir_fd=session_fd, follow_symlinks=False)
        _verify_owned_session_file(opened, linked, uid=uid, name="transition.lock")
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in {11, 13}:
                    raise RelayError(
                        f"cannot acquire owned session transition lock: {exc}"
                    ) from exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RelayError("owned session transition lock timed out") from exc
                time.sleep(min(_OWNED_SESSION_LOCK_RETRY_SECONDS, remaining))
        final = os.stat("transition.lock", dir_fd=session_fd, follow_symlinks=False)
        if (final.st_dev, final.st_ino) != (opened.st_dev, opened.st_ino):
            raise RelayError("owned session transition lock changed during acquisition")
        if sessions_fd is None:
            raise RelayError("owned session parent descriptor was not established")
        retained_sessions_fd = sessions_fd
        retained_session_fd = session_fd
        retained_lock_fd = lock_fd
        for descriptor in descriptors:
            if descriptor != retained_sessions_fd:
                os.close(descriptor)
        descriptors.clear()
        sessions_fd = None
        session_fd = None
        lock_fd = None
        return _OwnedSessionTransaction(
            session_id=session_id,
            path=selected_home / ".local" / "share" / "clio-relay" / "sessions" / session_id,
            sessions_fd=retained_sessions_fd,
            directory_fd=retained_session_fd,
            lock_fd=retained_lock_fd,
            uid=uid,
            _fcntl=fcntl,
        )
    except RelayError:
        raise
    except OSError as exc:
        raise RelayError(f"owned session transaction path is unsafe: {exc}") from exc
    finally:
        for descriptor in descriptors:
            with suppress(OSError):
                os.close(descriptor)
        if session_fd is not None:
            with suppress(OSError):
                os.close(session_fd)
        if lock_fd is not None:
            with suppress(OSError):
                os.close(lock_fd)
