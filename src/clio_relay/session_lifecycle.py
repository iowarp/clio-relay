"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import re
import secrets
import shlex
import socket
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.remote_cli import remote_env
from clio_relay.validation_report import SoftwareIdentity

if TYPE_CHECKING:
    from clio_relay.validation_report import (
        CleanupEvidence,
        LiveValidationReport,
        ValidationResource,
    )

SESSION_DETACH_CHECK_ID = "cleanup.detach"
SESSION_TEARDOWN_CHECK_ID = "cleanup.relay-session"
SESSION_CONNECTORS_CHECK_ID = "cleanup.connectors"
SESSION_GATEWAY_CHECK_ID = "cleanup.gateway-record"
SESSION_WORKER_CHECK_ID = "cleanup.worker-service"
SESSION_NO_RESIDUALS_CHECK_ID = "cleanup.no-owned-resources"
SESSION_SCHEDULER_RETAINED_CHECK_ID = "cleanup.jobs-preserved-default"
SESSION_RELAY_CANCELED_CHECK_ID = "cleanup.relay-jobs-canceled"
SESSION_SCHEDULER_CANCELED_CHECK_ID = "cleanup.explicit-job-cancel"
_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS = 120.0
_REMOTE_SESSION_START_RECOVERY_TIMEOUT_SECONDS = 15.0
_REMOTE_API_READINESS_TIMEOUT_SECONDS = 60.0
_MAX_OWNED_SESSION_DOCUMENT_BYTES = 1024 * 1024
MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES = 32 * 1024 * 1024
MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES = MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + 256 * 1024
_MAX_PROC_RECORD_BYTES = 1024 * 1024
_OWNED_SESSION_LOCK_RETRY_SECONDS = 0.05
_MAX_REMOTE_SESSION_SCRIPT_BYTES = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
_MAX_REMOTE_SESSION_STDOUT_BYTES = 1024 * 1024
_MAX_REMOTE_SESSION_STDERR_BYTES = 1024 * 1024
_MAX_API_HEALTH_RESPONSE_BYTES = 64 * 1024
_MAX_API_STARTUP_RECEIPT_BYTES = 64 * 1024
_MAX_SESSION_START_ERROR_CHARS = 8192
_API_STARTUP_RECEIPT_ENV = "CLIO_RELAY_SESSION_STARTUP_RECEIPT"
_SYSTEMD_UNIT_ENV = "CLIO_RELAY_SESSION_SYSTEMD_UNIT"
_SYSTEMD_CGROUP_ENV = "CLIO_RELAY_SESSION_SYSTEMD_CGROUP"
_SYSTEMD_INVOCATION_ENV = "CLIO_RELAY_SESSION_SYSTEMD_INVOCATION_ID"
_SYSTEMD_DESCRIPTION_ENV = "CLIO_RELAY_SESSION_SYSTEMD_DESCRIPTION"
_MAX_OWNED_SESSION_DIRECTORY_ENTRIES = 256
_MAX_OWNED_SESSION_CLEANUP_REPORT_CANDIDATES = 4
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


class _OwnedSessionQueue(Protocol):
    """Typed core-queue surface required by crash-surviving start promotion."""

    root: Path

    def clear_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> None:
        """Clear a matching closing marker after exact API recovery."""


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


@dataclass(frozen=True)
class RemoteSession:
    """A remotely owned relay session."""

    session_id: str
    remote_api_port: int
    api_token: str | None


class SessionApiReleaseIdentity(BaseModel):
    """Exact released artifact identity bound to an owned session API process."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.session-api-release.v1"] = (
        "clio-relay.session-api-release.v1"
    )
    distribution_version: str = Field(min_length=1)
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    software: SoftwareIdentity

    def canonical_json(self) -> str:
        """Return the canonical JSON representation used for process attestation."""
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )

    def sha256(self) -> str:
        """Return the canonical release-identity digest."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class OwnedSessionStartRequest(BaseModel):
    """Exact stdin contract for one cluster-local owned-session start."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool = False
    require_token: bool = True
    expected_api_release_identity: SessionApiReleaseIdentity | None = None
    cluster_registry: dict[str, object]
    cluster_registry_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cluster_route_revision: str = Field(min_length=1)


class OwnedSessionStartRejection(BaseModel):
    """Exact rejection of one invocation, not proof the durable operation failed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-start-rejection.v1"] = (
        "clio-relay.owner-session-start-rejection.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    invocation_rejected: Literal[True] = True
    error: str = Field(min_length=1, max_length=_MAX_SESSION_START_ERROR_CHARS)


class OwnedSessionStartStatusSelector(BaseModel):
    """Selector for the current transition until a later start supersedes it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation: Literal["session.start-status"] = "session.start-status"
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool
    require_token: bool
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class OwnedSessionStartRetrySelector(BaseModel):
    """Secret-free selector for safely retrying one owned-session start."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation: Literal["session.start"] = "session.start"
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool
    require_token: bool
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class OwnedSessionTeardownRequest(BaseModel):
    """Exact stdin contract for one cluster-local owned-session teardown."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    expected_cleanup_operation_id: DurableRecordId
    stop_worker: bool = False
    cancel_jobs: bool = False
    cancel_scheduler_jobs: bool = False


class OwnedSessionIdentityChallengeRequest(BaseModel):
    """Exact stdin contract for a bounded owned-session identity challenge."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    session_generation_id: DurableRecordId
    nonce: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionCleanupTarget(BaseModel):
    """Pinned identity for one file authorized by a cleanup receipt."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    present: bool
    device: int | None = Field(default=None, ge=0)
    inode: int | None = Field(default=None, gt=0)
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    identity_mode: Literal["inode", "content_sha256"] = "content_sha256"

    def identity_is_complete(self) -> bool:
        """Return whether present/absent state has the exact permitted shape."""
        stat_identity = (self.device, self.inode, self.size)
        if not self.present:
            return all(value is None for value in (*stat_identity, self.sha256))
        if not all(value is not None for value in stat_identity):
            return False
        return self.sha256 is None if self.identity_mode == "inode" else self.sha256 is not None


class CleanupResource(BaseModel):
    """Machine-readable result for one lifecycle-owned resource."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    resource_id: str
    location: str
    action: Literal["retain", "stop", "close", "cancel"]
    ownership_verified: bool
    outcome: Literal[
        "retained",
        "stopped",
        "closed",
        "canceled",
        "terminal",
        "missing",
        "refused",
        "failed",
    ]
    provider: str | None = None
    verified_after_operation: bool = False
    observed_state: str | None = None
    residual: bool = False
    detail: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)

    def to_validation_resource(self, *, cluster: str | None) -> ValidationResource:
        """Convert this cleanup result to canonical live-validation resource evidence."""
        from clio_relay.validation_report import ValidationResource

        validation_kind = {
            "remote_relay_api": "relay_session",
            "desktop_connector": "connector",
            "remote_connector": "connector",
            "gateway_record": "gateway_session",
            "worker_service": "relay_worker",
            "scheduler_sentinel": "scheduler_job",
        }.get(self.kind, self.kind)
        return ValidationResource(
            kind=validation_kind,
            resource_id=self.resource_id,
            role=f"{self.kind}:{self.action}",
            cluster=cluster,
            state=self.outcome,
            provider=self.provider,
            references=[self.location],
            metadata={
                "ownership_verified": self.ownership_verified,
                "cleanup_kind": self.kind,
                "provider": self.provider,
                "verified_after_operation": self.verified_after_operation,
                "observed_state": self.observed_state,
                "residual": self.residual,
                "detail": self.detail,
                **self.metadata,
            },
        )


class RemoteSessionStateEvidence(BaseModel):
    """Observed state linked to a remote session API lifecycle operation."""

    model_config = ConfigDict(extra="forbid")

    api_pid: int | None = None
    session_generation_id: DurableRecordId | None = None
    process_start_marker: str | None = None
    running: bool
    ownership_verified: bool
    observed_at: datetime
    started_at: datetime | None = None


class OwnedSessionCleanupReportReference(BaseModel):
    """Immutable owner-private sidecar identity for one coordinator report."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-cleanup-report-ref.v1"] = (
        "clio-relay.owner-session-cleanup-report-ref.v1"
    )
    name: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^coordinator-cleanup-report-[0-9a-f]{64}\.json$",
    )
    size: int = Field(gt=0, le=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionRecoveryStatus(BaseModel):
    """Fail-closed recovery evidence for one exact owned session generation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-recovery-status.v1"] = (
        "clio-relay.owner-session-recovery-status.v1"
    )
    cluster: str
    session_id: str
    session_generation_id: DurableRecordId | None = None
    start_operation_id: DurableRecordId | None = None
    cluster_route_revision: str | None = None
    owner: str | None = None
    api_pid: int | None = None
    remote_api_port: int | None = None
    process_start_marker: str | None = None
    leader_process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "unverified",
    ] = "unverified"
    process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "cleanup_pending",
        "already_closed",
        "unverified",
    ] = "unverified"
    running: bool = False
    process_absence_verified: bool = False
    generation_process_pids: list[int] = Field(default_factory=list[int])
    generation_process_absence_verified: bool = False
    metadata_verified: bool = False
    cluster_registry_verified: bool = False
    durable_generation_verified: bool = False
    cleanup_receipt: bool = False
    cleanup_paths_pending: bool | None = None
    # Compatibility-null only. Full reports are never copied into status responses.
    coordinator_report: None = None
    coordinator_report_ref: OwnedSessionCleanupReportReference | None = None
    coordinator_report_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    coordinator_report_bound: bool = False
    ownership_verified: bool = False
    recovery_verified: bool = False
    api_release_identity: SessionApiReleaseIdentity | None = None
    api_release_identity_verified: bool = False
    ownership_token_present: bool = False
    admission_status: dict[str, object] | None = None
    start_state: Literal["unknown", "starting", "ready", "failed", "not_current"] = "unknown"
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    start_attempt_verified: bool = False
    start_retryable: bool = False
    start_replace: bool | None = None
    start_require_token: bool | None = None
    start_expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    start_error: str | None = Field(default=None, max_length=_MAX_SESSION_START_ERROR_CHARS)
    errors: list[str] = Field(default_factory=list[str])

    @model_validator(mode="after")
    def _validate_coordinator_report_reference(self) -> OwnedSessionRecoveryStatus:
        """Keep the compatibility digest identical to the compact sidecar reference."""
        if (
            self.coordinator_report_ref is not None
            and self.coordinator_report_sha256 != self.coordinator_report_ref.sha256
        ):
            raise ValueError("coordinator report reference digest does not match status")
        return self


class OwnedSessionStartResult(BaseModel):
    """Desktop-visible outcome for a possibly asynchronous remote session start."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-start-result.v1"] = (
        "clio-relay.owner-session-start-result.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    session_generation_id: DurableRecordId | None = None
    remote_api_port: int = Field(gt=0, le=65_535)
    state: Literal["ready", "starting", "ambiguous", "failed", "not_current"]
    terminal: bool
    retryable: bool
    transition_accepted: bool | None = None
    transport_deadline_exceeded: bool = False
    running: bool = False
    ownership_verified: bool = False
    recovery_verified: bool = False
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    error: str | None = Field(default=None, max_length=_MAX_SESSION_START_ERROR_CHARS)
    status_selector: OwnedSessionStartStatusSelector
    retry_selector: OwnedSessionStartRetrySelector
    compatibility_lines: list[str] = Field(default_factory=list[str], max_length=32)

    @model_validator(mode="after")
    def _validate_start_result(self) -> OwnedSessionStartResult:
        """Keep state, identity, and the advertised recovery operations exact."""
        if not (
            self.status_selector.cluster == self.cluster
            and self.status_selector.session_id == self.session_id
            and self.status_selector.start_operation_id == self.start_operation_id
            and self.status_selector.cluster_route_revision == self.cluster_route_revision
            and self.status_selector.remote_api_port == self.remote_api_port
            and self.status_selector.replace == self.retry_selector.replace
            and self.status_selector.require_token == self.retry_selector.require_token
            and self.status_selector.expected_api_release_identity_sha256
            == self.retry_selector.expected_api_release_identity_sha256
            and self.retry_selector.cluster == self.cluster
            and self.retry_selector.session_id == self.session_id
            and self.retry_selector.start_operation_id == self.start_operation_id
            and self.retry_selector.cluster_route_revision == self.cluster_route_revision
            and self.retry_selector.remote_api_port == self.remote_api_port
        ):
            raise ValueError("owned-session start selectors changed result identity")
        if self.state == "ready":
            if not (
                self.terminal
                and not self.retryable
                and self.transition_accepted is True
                and self.session_generation_id is not None
                and self.ownership_verified
                and self.recovery_verified
            ):
                raise ValueError("ready owned-session start result is incomplete")
        elif self.state == "starting":
            if not (
                not self.terminal
                and self.retryable
                and self.transition_accepted is True
                and self.session_generation_id is not None
                and self.start_phase is not None
            ):
                raise ValueError("starting owned-session result lacks a durable attempt")
        elif self.state == "ambiguous":
            if self.terminal or not self.retryable or self.transition_accepted is not None:
                raise ValueError("ambiguous owned-session result claimed a terminal transition")
        elif self.state == "not_current":
            if (
                not self.terminal
                or self.retryable
                or self.transition_accepted is not None
                or self.error is None
            ):
                raise ValueError("non-current owned-session selector is incomplete")
        elif not self.terminal or self.retryable or self.error is None:
            raise ValueError("failed owned-session start result is incomplete")
        return self


class OwnedSessionStartPlan(BaseModel):
    """Read-only, persistable selector set for one future session start."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-start-plan.v1"] = (
        "clio-relay.owner-session-start-plan.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    status_selector: OwnedSessionStartStatusSelector
    retry_selector: OwnedSessionStartRetrySelector

    @model_validator(mode="after")
    def _validate_plan_selectors(self) -> OwnedSessionStartPlan:
        """Require both plan selectors to bind the same immutable request identity."""
        if not (
            self.status_selector.cluster == self.cluster
            and self.status_selector.session_id == self.session_id
            and self.status_selector.start_operation_id == self.start_operation_id
            and self.status_selector.cluster_route_revision == self.cluster_route_revision
            and self.status_selector.remote_api_port == self.remote_api_port
            and self.status_selector.replace == self.retry_selector.replace
            and self.status_selector.require_token == self.retry_selector.require_token
            and self.status_selector.expected_api_release_identity_sha256
            == self.expected_api_release_identity_sha256
            and self.retry_selector.cluster == self.cluster
            and self.retry_selector.session_id == self.session_id
            and self.retry_selector.start_operation_id == self.start_operation_id
            and self.retry_selector.cluster_route_revision == self.cluster_route_revision
            and self.retry_selector.remote_api_port == self.remote_api_port
            and self.retry_selector.expected_api_release_identity_sha256
            == self.expected_api_release_identity_sha256
        ):
            raise ValueError("owned-session start plan selectors changed identity")
        return self


@dataclass(frozen=True)
class _OwnedGenerationProcess:
    """One live process carrying the exact complete owned-generation identity."""

    pid: int
    process_group_id: int
    start_ticks: str


def _read_bounded_proc_bytes(path: Path, *, maximum_bytes: int) -> bytes:
    """Read one proc pseudo-file without following links or allocating without bound."""
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise RelayError(f"process identity file exceeded its byte limit: {path}")
        return bytes(payload)
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_proc_identity(*, proc_root: Path, pid: int) -> _OwnedGenerationProcess:
    """Read one bounded process-group and start identity from procfs."""
    try:
        stat_payload = _read_bounded_proc_bytes(
            proc_root / str(pid) / "stat",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).decode("utf-8")
        fields = stat_payload.rsplit(")", 1)[1].split()
        return _OwnedGenerationProcess(
            pid=pid,
            process_group_id=int(fields[2]),
            start_ticks=fields[19],
        )
    except (FileNotFoundError, ProcessLookupError):
        raise
    except (IndexError, OSError, UnicodeDecodeError, ValueError) as exc:
        raise RelayError(f"process identity record is invalid for pid {pid}: {exc}") from exc


def _current_linux_cgroup_path(
    *,
    pid: int,
    proc_root: Path = Path("/proc"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Path:
    """Return the exact cgroup-v2 path containing one process."""
    try:
        payload = _read_bounded_proc_bytes(
            proc_root / str(pid) / "cgroup",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).decode("ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise RelayError(f"cannot inspect process cgroup for pid {pid}: {exc}") from exc
    matches = [line[3:] for line in payload.splitlines() if line.startswith("0::/")]
    relative = matches[0].lstrip("/") if len(matches) == 1 else ""
    if not relative or any(part in {"", ".", ".."} for part in relative.split("/")):
        raise RelayError(f"process cgroup-v2 identity is invalid for pid {pid}")
    try:
        root = cgroup_root.resolve(strict=True)
        observed = (root / relative).resolve(strict=True)
    except OSError as exc:
        raise RelayError(f"process cgroup-v2 path is unavailable for pid {pid}: {exc}") from exc
    if observed == root or not observed.is_relative_to(root):
        raise RelayError(f"process cgroup-v2 identity escaped its root for pid {pid}")
    return observed


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


def publish_owned_session_api_startup_receipt() -> bool:
    """Publish the signed API identity after gated environment and cgroup entry."""
    receipt_path_raw = os.environ.get(_API_STARTUP_RECEIPT_ENV)
    if receipt_path_raw is None:
        return False
    required_names = (
        "CLIO_RELAY_SESSION_OWNER_TOKEN",
        "CLIO_RELAY_SESSION_GENERATION_ID",
        "CLIO_RELAY_OWNER_SESSION_ID",
        "CLIO_RELAY_OWNER_SESSION_CLUSTER",
        "CLIO_RELAY_API_RELEASE_IDENTITY_SHA256",
        "CLIO_RELAY_CLUSTER_REGISTRY",
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        _SYSTEMD_UNIT_ENV,
        _SYSTEMD_CGROUP_ENV,
        _SYSTEMD_INVOCATION_ENV,
        _SYSTEMD_DESCRIPTION_ENV,
    )
    values = {name: os.environ.get(name) for name in required_names}
    if any(not value for value in values.values()):
        raise RelayError("owned API startup receipt environment is incomplete")
    owner_token = cast(str, values["CLIO_RELAY_SESSION_OWNER_TOKEN"])
    generation_id = validate_durable_record_id(
        cast(str, values["CLIO_RELAY_SESSION_GENERATION_ID"])
    )
    receipt_path = Path(receipt_path_raw)
    registry_path = Path(cast(str, values["CLIO_RELAY_CLUSTER_REGISTRY"]))
    expected_receipt = registry_path.parent / f"api-startup-{generation_id}.json"
    if receipt_path != expected_receipt:
        raise RelayError("owned API startup receipt path is not generation-scoped")
    invocation_id = cast(str, values[_SYSTEMD_INVOCATION_ENV])
    if os.environ.get("INVOCATION_ID") != invocation_id:
        raise RelayError("owned API process systemd invocation identity mismatched")
    pid = os.getpid()
    process_identity = _read_proc_identity(proc_root=Path("/proc"), pid=pid)
    observed_cgroup = _current_linux_cgroup_path(pid=pid)
    expected_cgroup = Path(cast(str, values[_SYSTEMD_CGROUP_ENV])).resolve(strict=True)
    if observed_cgroup != expected_cgroup:
        raise RelayError("owned API process is outside its persisted cgroup")
    document: dict[str, object] = {
        "schema_version": "clio-relay.owner-session-api-startup.v1",
        "cluster": values["CLIO_RELAY_OWNER_SESSION_CLUSTER"],
        "session_id": values["CLIO_RELAY_OWNER_SESSION_ID"],
        "session_generation_id": generation_id,
        "api_pid": pid,
        "api_pgid": process_identity.process_group_id,
        "process_start_ticks": process_identity.start_ticks,
        "api_release_identity_sha256": values["CLIO_RELAY_API_RELEASE_IDENTITY_SHA256"],
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": values["CLIO_RELAY_SESSION_REGISTRY_SHA256"],
        "cluster_route_revision": values["CLIO_RELAY_SESSION_ROUTE_REVISION"],
        "systemd_unit": values[_SYSTEMD_UNIT_ENV],
        "systemd_cgroup_path": str(expected_cgroup),
        "systemd_invocation_id": invocation_id,
        "systemd_description": values[_SYSTEMD_DESCRIPTION_ENV],
        "observed_at": datetime.now(UTC).isoformat(),
    }
    document["hmac_sha256"] = _startup_receipt_signature(document, owner_token=owner_token)
    _atomic_write_startup_receipt(
        receipt_path,
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    os.environ.pop("CLIO_RELAY_SESSION_OWNER_TOKEN", None)
    return True


def _recorded_scope_processes(
    *,
    proc_root: Path,
    systemd_unit: str,
    systemd_cgroup_path: str,
    systemd_invocation_id: str,
    systemd_description: str,
) -> list[_OwnedGenerationProcess]:
    """Enumerate only members of one exact persistent systemd generation scope."""
    from clio_relay.process_containment import recorded_linux_systemd_scope_process_ids

    try:
        process_ids = recorded_linux_systemd_scope_process_ids(
            unit=systemd_unit,
            cgroup_path=systemd_cgroup_path,
            invocation_id=systemd_invocation_id,
            description=systemd_description,
        )
    except RuntimeError as exc:
        raise RelayError(f"owned session scope identity could not be verified: {exc}") from exc
    processes: list[_OwnedGenerationProcess] = []
    for pid in process_ids:
        try:
            processes.append(_read_proc_identity(proc_root=proc_root, pid=pid))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return sorted(processes, key=lambda process: process.pid)


def _terminate_recorded_session_scope(
    *,
    systemd_unit: str,
    systemd_cgroup_path: str,
    systemd_invocation_id: str,
    systemd_description: str,
) -> None:
    """Terminate one exact persisted session cgroup after InvocationID verification."""
    from clio_relay.process_containment import terminate_recorded_linux_systemd_scope

    try:
        terminate_recorded_linux_systemd_scope(
            unit=systemd_unit,
            cgroup_path=systemd_cgroup_path,
            invocation_id=systemd_invocation_id,
            description=systemd_description,
        )
    except RuntimeError as exc:
        raise RelayError(f"owned session scope termination failed: {exc}") from exc


def _is_clio_relay_api_leader(*, proc_root: Path, pid: int) -> bool:
    """Return whether one bounded command line is the owned API leader command."""
    try:
        command = (
            _read_bounded_proc_bytes(
                proc_root / str(pid) / "cmdline",
                maximum_bytes=_MAX_PROC_RECORD_BYTES,
            )
            .replace(bytes([0]), b" ")
            .decode("utf-8", errors="replace")
        )
    except (FileNotFoundError, ProcessLookupError):
        return False
    except OSError as exc:
        raise RelayError(f"cannot inspect API leader command for pid {pid}: {exc}") from exc
    return "clio-relay" in command and " api " in f" {command} " and " start" in command


def _inspect_owned_session_start_attempt_status(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    proc_root: Path,
    transaction: _OwnedSessionTransaction,
    metadata_error: str,
    expected_start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> OwnedSessionRecoveryStatus | None:
    """Project one exact pre-metadata start journal into read-only status evidence."""
    from clio_relay.core_queue import ClioCoreQueue

    try:
        current_attempt = _validated_start_attempt(
            transaction,
            cluster=cluster,
            session_id=session_id,
        )
    except RelayError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[metadata_error, "owned-session start attempt identity is invalid"],
        )
    if (
        expected_start_operation_id is not None
        and current_attempt is not None
        and current_attempt.get("start_operation_id") != expected_start_operation_id
    ):
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=expected_start_operation_id,
            cluster_route_revision=expected_cluster_route_revision,
            start_state="not_current",
            start_retryable=False,
            errors=[
                "another operation owns the current transition; this selector was never "
                "accepted or is no longer current"
            ],
        )
    try:
        attempt = _validated_start_attempt(
            transaction,
            cluster=cluster,
            session_id=session_id,
            start_operation_id=expected_start_operation_id,
            cluster_route_revision_value=expected_cluster_route_revision,
        )
    except RelayError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[metadata_error, "owned-session start attempt identity is invalid"],
        )
    if attempt is None:
        return None
    generation_id = cast(str, attempt["session_generation_id"])
    validated_start_operation_id = cast(str, attempt["start_operation_id"])
    registry_sha256 = attempt.get("cluster_registry_sha256")
    route_revision = attempt.get("cluster_route_revision")
    remote_api_port = attempt.get("remote_api_port")
    start_phase = attempt.get("start_phase")
    systemd_unit = attempt.get("systemd_unit")
    systemd_description = attempt.get("systemd_description")
    cgroup_path = attempt.get("systemd_cgroup_path")
    invocation_id = attempt.get("systemd_invocation_id")
    phase = cast(Literal["pending", "admitted", "scope_bound", "contained"], start_phase)
    errors: list[str] = []
    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    try:
        admission_status = ClioCoreQueue(core_dir).owner_session_generation_status(
            session_id,
            session_generation_id=generation_id,
        )
        active_generation = admission_status.get("active_generation_id")
        closing_generation = admission_status.get("closing_generation_id")
        common_admission_identity = bool(
            admission_status.get("owner_session_id") == session_id
            and admission_status.get("session_generation_id") == generation_id
            and closing_generation is None
        )
        if phase == "pending":
            admission_consistent = common_admission_identity and active_generation in {
                None,
                generation_id,
            }
        else:
            admission_consistent = bool(
                common_admission_identity
                and active_generation == generation_id
                and admission_status.get("open") is True
            )
        durable_generation_verified = bool(
            common_admission_identity
            and active_generation == generation_id
            and admission_status.get("open") is True
        )
        if not admission_consistent:
            errors.append("owned-session start attempt conflicts with durable core admission")
    except (OSError, RelayError, ValueError) as exc:
        errors.append(f"could not verify owned-session start admission: {exc}")

    cluster_registry_verified = False
    registry_payload = transaction.read_bytes(
        f"cluster-registry-{generation_id}.json",
        maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
        required=False,
    )
    if registry_payload is not None:
        try:
            raw_registry = cast(object, json.loads(registry_payload))
            registry = ClusterRegistry.model_validate(raw_registry)
            cluster_registry_verified = bool(
                hashlib.sha256(registry_payload).hexdigest() == registry_sha256
                and set(registry.clusters) == {cluster}
                and registry.clusters[cluster].name == cluster
                and cluster_route_revision(registry.clusters[cluster]) == route_revision
            )
        except (TypeError, ValueError):
            cluster_registry_verified = False
        if not cluster_registry_verified:
            errors.append("owned-session start registry identity is invalid")

    generation_processes: list[_OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if phase in {"scope_bound", "contained"}:
        try:
            generation_processes = _recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=cast(str, systemd_unit),
                systemd_cgroup_path=cast(str, cgroup_path),
                systemd_invocation_id=cast(str, invocation_id),
                systemd_description=cast(str, systemd_description),
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))

    attempt_verified = not errors
    start_error = cast(str | None, attempt.get("error"))
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=generation_id,
        start_operation_id=validated_start_operation_id,
        cluster_route_revision=cast(str, route_revision),
        owner="clio-relay",
        remote_api_port=cast(int, remote_api_port),
        process_state="unverified",
        running=False,
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=False,
        cluster_registry_verified=cluster_registry_verified,
        durable_generation_verified=durable_generation_verified,
        ownership_verified=False,
        recovery_verified=False,
        ownership_token_present=True,
        admission_status=admission_status,
        start_state=("failed" if start_error is not None else "starting"),
        start_phase=phase,
        start_attempt_verified=attempt_verified,
        start_retryable=bool(attempt_verified and start_error is None),
        start_replace=cast(bool, attempt["replace"]),
        start_require_token=cast(bool, attempt["require_token"]),
        start_expected_api_release_identity_sha256=cast(
            str | None,
            attempt["expected_api_release_identity_sha256"],
        ),
        start_error=start_error,
        errors=errors,
    )


def inspect_owned_session_recovery_status(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    proc_root: Path = Path("/proc"),
    effective_uid: int | None = None,
    transaction: _OwnedSessionTransaction | None = None,
    expected_start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> OwnedSessionRecoveryStatus:
    """Inspect durable metadata, process identity, and core admission for recovery.

    This function is deliberately read-only.  A dead process is recoverable only
    when its protected metadata, exact cluster registry, and authoritative core
    generation all agree.  A live or reused PID must additionally pass the full
    process identity check before any teardown coordinator may mutate state.
    """
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(session_id=session_id, remote_api_port=1)
    if not cluster:
        raise RelayError("cluster must not be empty")
    selected_home = home or Path.home()
    session_dir = selected_home / ".local" / "share" / "clio-relay" / "sessions" / session_id
    metadata_path = session_dir / "metadata.json"
    errors: list[str] = []
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    uid = (
        get_effective_uid()
        if effective_uid is None and get_effective_uid is not None
        else effective_uid
    )
    try:
        if transaction is None:
            document, _ = _read_owned_session_document(
                metadata_path,
                label="owned session metadata",
                effective_uid=uid,
            )
        else:
            if transaction.session_id != session_id or transaction.path != session_dir:
                raise RelayError("owned session transaction identity does not match recovery")
            transaction_document = transaction.read_json("metadata.json")
            if transaction_document is None:  # pragma: no cover - required read
                raise RelayError("owned session metadata is unavailable")
            document = transaction_document
    except RelayError as exc:
        if transaction is not None:
            attempt_status = _inspect_owned_session_start_attempt_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                proc_root=proc_root,
                transaction=transaction,
                metadata_error=str(exc),
                expected_start_operation_id=expected_start_operation_id,
                expected_cluster_route_revision=expected_cluster_route_revision,
            )
            if attempt_status is not None:
                return attempt_status
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[str(exc)],
        )

    if document.get("schema_version") == "clio-relay.owner-session-cleanup-receipt.v1":
        if transaction is None and document.get("coordinator_report_ref") is not None:
            try:
                with open_owned_session_transaction(
                    session_id=session_id,
                    create=False,
                    timeout_seconds=10.0,
                    home=selected_home,
                ) as pinned_transaction:
                    return inspect_owned_session_recovery_status(
                        cluster=cluster,
                        session_id=session_id,
                        core_dir=core_dir,
                        home=selected_home,
                        proc_root=proc_root,
                        effective_uid=uid,
                        transaction=pinned_transaction,
                    )
            except RelayError as exc:
                return OwnedSessionRecoveryStatus(
                    cluster=cluster,
                    session_id=session_id,
                    errors=[str(exc)],
                )
        return _inspect_owned_session_cleanup_receipt(
            cluster=cluster,
            session_id=session_id,
            document=document,
            core_dir=core_dir,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )

    owner = document.get("owner")
    generation = document.get("session_generation_id")
    recorded_cluster = document.get("cluster")
    owner_token = document.get("owner_token")
    api_pid = document.get("api_pid")
    api_pgid = document.get("api_pgid")
    process_start = document.get("process_start_ticks")
    registry_path_raw = document.get("cluster_registry_path")
    registry_sha256 = document.get("cluster_registry_sha256")
    route_revision = document.get("cluster_route_revision")
    release_identity = document.get("api_release_identity")
    release_sha256 = document.get("api_release_identity_sha256")
    started_at = document.get("started_at")
    remote_api_port = document.get("remote_api_port")
    containment_mode = document.get("containment_mode")
    systemd_unit = document.get("systemd_unit")
    systemd_cgroup_path = document.get("systemd_cgroup_path")
    systemd_invocation_id = document.get("systemd_invocation_id")
    systemd_description = document.get("systemd_description")
    containment_broker_pid = document.get("containment_broker_pid")
    containment_broker_start = document.get("containment_broker_start_identity")
    startup_receipt_path_raw = document.get("api_startup_receipt_path")
    startup_receipt_sha256 = document.get("api_startup_receipt_sha256")

    try:
        validated_release = SessionApiReleaseIdentity.model_validate(release_identity)
    except ValueError:
        validated_release = None
    try:
        parsed_started_at = (
            datetime.fromisoformat(started_at) if isinstance(started_at, str) else None
        )
    except ValueError:
        parsed_started_at = None

    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
    except ValueError:
        validated_generation = None
    expected_metadata_keys = {
        "cluster",
        "session_id",
        "remote_api_port",
        "api_pid",
        "api_pgid",
        "owner_token",
        "session_generation_id",
        "api_release_identity",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "cluster_authority_verified",
        "process_start_ticks",
        "containment_mode",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "api_startup_receipt_path",
        "api_startup_receipt_sha256",
        "started_at",
        "owner",
    }
    metadata_verified = bool(
        set(document) == expected_metadata_keys
        and owner == "clio-relay"
        and document.get("session_id") == session_id
        and recorded_cluster == cluster
        and isinstance(remote_api_port, int)
        and not isinstance(remote_api_port, bool)
        and remote_api_port > 0
        and validated_generation is not None
        and isinstance(owner_token, str)
        and len(owner_token) == 64
        and all(character in "0123456789abcdef" for character in owner_token)
        and isinstance(api_pid, int)
        and not isinstance(api_pid, bool)
        and api_pid > 1
        and isinstance(api_pgid, int)
        and not isinstance(api_pgid, bool)
        and api_pgid > 0
        and isinstance(process_start, str)
        and process_start.isdigit()
        and isinstance(registry_path_raw, str)
        and isinstance(registry_sha256, str)
        and len(registry_sha256) == 64
        and all(character in "0123456789abcdef" for character in registry_sha256)
        and isinstance(route_revision, str)
        and bool(route_revision)
        and document.get("cluster_authority_verified") is True
        and validated_release is not None
        and isinstance(release_sha256, str)
        and len(release_sha256) == 64
        and all(character in "0123456789abcdef" for character in release_sha256)
        and validated_release.sha256() == release_sha256
        and containment_mode == "linux_systemd_scope"
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_cgroup_path, str)
        and bool(systemd_cgroup_path)
        and isinstance(systemd_invocation_id, str)
        and len(systemd_invocation_id) == 32
        and all(character in "0123456789abcdef" for character in systemd_invocation_id)
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and isinstance(containment_broker_pid, int)
        and not isinstance(containment_broker_pid, bool)
        and containment_broker_pid > 1
        and isinstance(containment_broker_start, str)
        and bool(containment_broker_start)
        and startup_receipt_path_raw
        == str(session_dir / f"api-startup-{validated_generation}.json")
        and isinstance(startup_receipt_sha256, str)
        and len(startup_receipt_sha256) == 64
        and all(character in "0123456789abcdef" for character in startup_receipt_sha256)
        and parsed_started_at is not None
        and parsed_started_at.tzinfo is not None
    )
    if not metadata_verified:
        errors.append("owned session metadata identity is incomplete or mismatched")

    startup_receipt_verified = False
    if (
        metadata_verified
        and isinstance(startup_receipt_path_raw, str)
        and isinstance(startup_receipt_sha256, str)
        and isinstance(owner_token, str)
        and isinstance(api_pid, int)
        and isinstance(api_pgid, int)
        and isinstance(process_start, str)
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
        and isinstance(release_sha256, str)
        and isinstance(registry_path_raw, str)
        and isinstance(registry_sha256, str)
        and isinstance(route_revision, str)
    ):
        try:
            receipt_path = Path(startup_receipt_path_raw)
            if transaction is None:
                receipt_document, receipt_payload = _read_owned_session_document(
                    receipt_path,
                    label="owned API startup receipt",
                    effective_uid=uid,
                )
            else:
                receipt_payload = transaction.read_bytes(
                    receipt_path.name,
                    maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
                )
                if receipt_payload is None:  # pragma: no cover - required read
                    raise RelayError("owned API startup receipt is unavailable")
                raw_receipt = cast(object, json.loads(receipt_payload))
                if not isinstance(raw_receipt, dict):
                    raise RelayError("owned API startup receipt is not a JSON object")
                receipt_document = {
                    str(key): value
                    for key, value in cast(dict[object, object], raw_receipt).items()
                }
            expected_receipt = {
                "cluster": cluster,
                "session_id": session_id,
                "session_generation_id": validated_generation,
                "api_pid": api_pid,
                "api_pgid": api_pgid,
                "process_start_ticks": process_start,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": registry_path_raw,
                "cluster_registry_sha256": registry_sha256,
                "cluster_route_revision": route_revision,
                "systemd_unit": systemd_unit,
                "systemd_cgroup_path": systemd_cgroup_path,
                "systemd_invocation_id": systemd_invocation_id,
                "systemd_description": systemd_description,
            }
            signature = receipt_document.get("hmac_sha256")
            startup_receipt_verified = bool(
                hashlib.sha256(receipt_payload).hexdigest() == startup_receipt_sha256
                and receipt_document.get("schema_version")
                == "clio-relay.owner-session-api-startup.v1"
                and all(
                    receipt_document.get(key) == value for key, value in expected_receipt.items()
                )
                and isinstance(signature, str)
                and hmac.compare_digest(
                    signature,
                    _startup_receipt_signature(receipt_document, owner_token=owner_token),
                )
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(str(exc))
        if not startup_receipt_verified:
            errors.append("owned API startup receipt identity is invalid")

    cluster_registry_verified = False
    registry_path: Path | None = None
    if (
        metadata_verified
        and validated_generation is not None
        and isinstance(registry_path_raw, str)
    ):
        registry_path = Path(registry_path_raw)
        expected_registry_path = session_dir / f"cluster-registry-{validated_generation}.json"
        if registry_path != expected_registry_path:
            errors.append("owned session cluster registry path is not generation-scoped")
        else:
            try:
                if transaction is None:
                    registry_document, registry_bytes = _read_owned_session_document(
                        registry_path,
                        label="owned session cluster registry",
                        effective_uid=uid,
                    )
                else:
                    registry_bytes = transaction.read_bytes(
                        registry_path.name,
                        maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
                    )
                    if registry_bytes is None:  # pragma: no cover - required read
                        raise RelayError("owned session cluster registry is unavailable")
                    raw_registry = cast(object, json.loads(registry_bytes))
                    if not isinstance(raw_registry, dict):
                        raise RelayError("owned session cluster registry is not a JSON object")
                    registry_document = {
                        str(key): value
                        for key, value in cast(dict[object, object], raw_registry).items()
                    }
                registry = ClusterRegistry.model_validate(registry_document)
                cluster_registry_verified = bool(
                    hashlib.sha256(registry_bytes).hexdigest() == registry_sha256
                    and set(registry.clusters) == {cluster}
                    and registry.clusters[cluster].name == cluster
                    and cluster_route_revision(registry.clusters[cluster]) == route_revision
                )
            except (RelayError, ValueError) as exc:
                errors.append(str(exc))
            if not cluster_registry_verified and not any(
                "cluster registry" in error for error in errors
            ):
                errors.append("owned session cluster registry digest or identity mismatched")

    process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "cleanup_pending",
        "already_closed",
        "unverified",
    ] = "unverified"
    leader_process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "unverified",
    ] = "unverified"
    running = False
    process_absence_verified = False
    generation_processes: list[_OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if (
        metadata_verified
        and startup_receipt_verified
        and isinstance(api_pid, int)
        and isinstance(api_pgid, int)
        and isinstance(process_start, str)
        and validated_generation is not None
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
    ):
        try:
            generation_processes = _recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))
        proc = proc_root / str(api_pid)
        try:
            stat_text = _read_bounded_proc_bytes(
                proc / "stat",
                maximum_bytes=_MAX_PROC_RECORD_BYTES,
            ).decode("utf-8")
        except FileNotFoundError:
            leader_process_state = "absent"
        except (OSError, UnicodeDecodeError, RelayError) as exc:
            errors.append(f"could not inspect recorded API pid {api_pid}: {exc}")
        else:
            try:
                fields = stat_text.rsplit(")", 1)[1].split()
                observed_state = fields[0]
                observed_pgid = int(fields[2])
                observed_start = fields[19]
            except (IndexError, ValueError) as exc:
                errors.append(f"recorded API pid {api_pid} has invalid proc stat: {exc}")
            else:
                if observed_start != process_start:
                    leader_process_state = "reused"
                    errors.append(f"recorded API pid {api_pid} was reused")
                elif observed_pgid != api_pgid:
                    leader_process_state = "foreign"
                    errors.append(f"recorded API pid {api_pid} changed process group")
                elif observed_state == "Z":
                    leader_process_state = "owned_terminal"
                elif any(
                    process.pid == api_pid
                    and process.process_group_id == api_pgid
                    and process.start_ticks == process_start
                    for process in generation_processes
                ) and _is_clio_relay_api_leader(proc_root=proc_root, pid=api_pid):
                    leader_process_state = "owned_running"
                else:
                    leader_process_state = "foreign"
                    errors.append(f"recorded API pid {api_pid} failed process identity")

        if leader_process_state in {"reused", "foreign"}:
            process_state = leader_process_state
        elif generation_processes:
            process_state = "owned_running"
            running = True
        elif generation_process_scan_verified:
            process_state = (
                "owned_terminal" if leader_process_state == "owned_terminal" else "absent"
            )
            process_absence_verified = True

    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    if validated_generation is not None:
        try:
            admission_status = ClioCoreQueue(core_dir).owner_session_generation_status(
                session_id,
                session_generation_id=validated_generation,
            )
            active_generation = admission_status.get("active_generation_id")
            closing_generation = admission_status.get("closing_generation_id")
            durable_generation_verified = bool(
                admission_status.get("owner_session_id") == session_id
                and admission_status.get("session_generation_id") == validated_generation
                and active_generation in {None, validated_generation}
                and closing_generation in {None, validated_generation}
                and (
                    (
                        admission_status.get("open") is True
                        and active_generation == validated_generation
                        and closing_generation is None
                    )
                    or (
                        admission_status.get("closing") is True
                        and closing_generation == validated_generation
                    )
                )
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"could not verify durable owner-session generation: {exc}")
        if not durable_generation_verified:
            errors.append("durable owner-session generation is not active or closing")

    start_operation_id: str | None = None
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    start_attempt_verified = False
    start_replace: bool | None = None
    start_require_token: bool | None = None
    start_expected_release_sha256: str | None = None
    start_attempt_release_sha256: str | None = None
    start_error: str | None = None
    if transaction is not None:
        attempt_status = _inspect_owned_session_start_attempt_status(
            cluster=cluster,
            session_id=session_id,
            core_dir=core_dir,
            proc_root=proc_root,
            transaction=transaction,
            metadata_error="owned session metadata exists without its start journal",
            expected_start_operation_id=expected_start_operation_id,
            expected_cluster_route_revision=expected_cluster_route_revision,
        )
        if attempt_status is not None and attempt_status.start_state == "not_current":
            return attempt_status
        if (
            attempt_status is not None
            and attempt_status.start_attempt_verified
            and attempt_status.session_generation_id == validated_generation
            and attempt_status.remote_api_port == remote_api_port
            and attempt_status.cluster_route_revision == route_revision
        ):
            start_operation_id = attempt_status.start_operation_id
            start_phase = attempt_status.start_phase
            start_attempt_verified = True
            start_replace = attempt_status.start_replace
            start_require_token = attempt_status.start_require_token
            start_expected_release_sha256 = (
                attempt_status.start_expected_api_release_identity_sha256
            )
            bound_attempt = _validated_start_attempt(
                transaction,
                cluster=cluster,
                session_id=session_id,
                start_operation_id=start_operation_id,
                cluster_route_revision_value=cast(str, route_revision),
            )
            if bound_attempt is None:  # pragma: no cover - status validated the same journal
                raise RelayError("owned-session start journal disappeared during inspection")
            start_attempt_release_sha256 = cast(
                str,
                bound_attempt["api_release_identity_sha256"],
            )
            start_error = attempt_status.start_error
        elif expected_start_operation_id is not None:
            errors.extend(
                attempt_status.errors
                if attempt_status is not None and attempt_status.errors
                else ["owned-session start selector has no exact durable journal"]
            )

    start_release_committed = bool(
        start_attempt_verified
        and isinstance(release_sha256, str)
        and start_attempt_release_sha256 == release_sha256
    )
    replacement_in_progress = bool(
        start_attempt_verified and start_replace is True and not start_release_committed
    )
    if start_attempt_verified and not (start_release_committed or replacement_in_progress):
        errors.append("owned-session start journal release does not match committed metadata")

    acceptable_process_state = process_state in {
        "absent",
        "owned_running",
        "owned_terminal",
    }
    recovery_verified = bool(
        metadata_verified
        and cluster_registry_verified
        and durable_generation_verified
        and acceptable_process_state
        and not errors
    )
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=validated_generation,
        start_operation_id=start_operation_id,
        cluster_route_revision=route_revision if isinstance(route_revision, str) else None,
        owner=owner if isinstance(owner, str) else None,
        api_pid=api_pid if isinstance(api_pid, int) and not isinstance(api_pid, bool) else None,
        remote_api_port=(
            remote_api_port
            if isinstance(remote_api_port, int) and not isinstance(remote_api_port, bool)
            else None
        ),
        process_start_marker=process_start if isinstance(process_start, str) else None,
        leader_process_state=leader_process_state,
        process_state=process_state,
        running=running,
        process_absence_verified=process_absence_verified,
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=metadata_verified,
        cluster_registry_verified=cluster_registry_verified,
        durable_generation_verified=durable_generation_verified,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        api_release_identity=validated_release,
        api_release_identity_verified=bool(validated_release is not None and running),
        ownership_token_present=isinstance(owner_token, str) and bool(owner_token),
        admission_status=admission_status,
        start_state=(
            "ready"
            if recovery_verified and start_release_committed
            else "starting"
            if recovery_verified and replacement_in_progress
            else "unknown"
        ),
        start_phase=start_phase,
        start_attempt_verified=start_attempt_verified,
        start_retryable=bool(recovery_verified and replacement_in_progress),
        start_replace=start_replace,
        start_require_token=start_require_token,
        start_expected_api_release_identity_sha256=start_expected_release_sha256,
        start_error=start_error,
        errors=errors,
    )


def inspect_owned_session_start_status(
    *,
    cluster: str,
    session_id: str,
    start_operation_id: str,
    cluster_route_revision: str,
    core_dir: Path,
    home: Path | None = None,
    proc_root: Path = Path("/proc"),
    lock_timeout_seconds: float = 0.05,
) -> OwnedSessionRecoveryStatus:
    """Inspect one exact start selector without waiting for its transition writer."""
    _validate_session(session_id=session_id, remote_api_port=1)
    try:
        validated_operation_id = validate_durable_record_id(start_operation_id)
    except (TypeError, ValueError) as exc:
        raise RelayError(f"invalid start_operation_id: {exc}") from exc
    if not cluster_route_revision:
        raise RelayError("cluster_route_revision must not be empty")
    if lock_timeout_seconds <= 0:
        raise ValueError("lock_timeout_seconds must be positive")
    selected_home = home or Path.home()
    try:
        with open_owned_session_transaction(
            session_id=session_id,
            create=False,
            timeout_seconds=lock_timeout_seconds,
            home=selected_home,
        ) as transaction:
            return inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
                proc_root=proc_root,
                transaction=transaction,
                expected_start_operation_id=validated_operation_id,
                expected_cluster_route_revision=cluster_route_revision,
            )
    except RelayError as exc:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=validated_operation_id,
            cluster_route_revision=cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)[:_MAX_SESSION_START_ERROR_CHARS]],
        )


def _inspect_owned_session_cleanup_receipt(
    *,
    cluster: str,
    session_id: str,
    document: dict[str, object],
    core_dir: Path,
    proc_root: Path,
    effective_uid: int | None,
    transaction: _OwnedSessionTransaction | None,
) -> OwnedSessionRecoveryStatus:
    """Validate a sanitized receipt for an idempotent teardown retry."""
    from clio_relay.core_queue import ClioCoreQueue

    queue = ClioCoreQueue(core_dir)
    errors: list[str] = []
    generation = document.get("session_generation_id")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
    except ValueError:
        validated_generation = None
    report: SessionLifecycleReport | None = None
    try:
        report = SessionLifecycleReport.model_validate(document.get("report"))
    except (TypeError, ValueError) as exc:
        errors.append(f"owned session cleanup receipt report is invalid: {exc}")
    coordinator_report: SessionLifecycleReport | None = None
    coordinator_report_ref: OwnedSessionCleanupReportReference | None = None
    coordinator_report_bound = False
    coordinator_report_sha256: object = None
    raw_coordinator_report_ref = document.get("coordinator_report_ref")
    raw_coordinator_report = document.get("coordinator_report")
    legacy_coordinator_sha256 = document.get("coordinator_report_sha256")
    coordinator_fields_valid = bool(
        raw_coordinator_report_ref is None
        and raw_coordinator_report is None
        and legacy_coordinator_sha256 is None
    )
    if raw_coordinator_report_ref is not None:
        try:
            if transaction is None:
                raise RelayError("coordinator cleanup report sidecar has no pinned directory")
            coordinator_report_ref = OwnedSessionCleanupReportReference.model_validate(
                raw_coordinator_report_ref
            )
            if validated_generation is None or not isinstance(
                document.get("cleanup_operation_id"), str
            ):
                raise RelayError("coordinator cleanup report reference has no durable identity")
            coordinator_report = _read_coordinator_report_sidecar(
                transaction,
                coordinator_report_ref,
                expected_session_generation_id=validated_generation,
                expected_cleanup_operation_id=cast(str, document.get("cleanup_operation_id")),
            )
            coordinator_report_sha256 = coordinator_report_ref.sha256
            remote_resources = report.resources if report is not None else []
            coordinator_report_bound = bool(
                report is not None
                and coordinator_report.cluster == report.cluster
                and coordinator_report.session_id == report.session_id
                and coordinator_report.session_generation_id == report.session_generation_id
                and coordinator_report.mode == report.mode
                and coordinator_report.cleanup_operation_id == report.cleanup_operation_id
                and coordinator_report.cleanup_policy == report.cleanup_policy
                and coordinator_report.relay_cancel_requested == report.relay_cancel_requested
                and coordinator_report.scheduler_cancel_requested
                == report.scheduler_cancel_requested
                and coordinator_report.prior_session_status == report.prior_session_status
                and coordinator_report.post_session_status == report.post_session_status
                and len(coordinator_report.resources) >= len(remote_resources)
                and coordinator_report.resources[: len(remote_resources)] == remote_resources
            )
            coordinator_fields_valid = coordinator_report_bound
        except (RelayError, TypeError, ValueError) as exc:
            errors.append(f"owned session coordinator cleanup report is invalid: {exc}")
        if not coordinator_fields_valid:
            errors.append("owned session coordinator cleanup report binding is invalid")
    elif raw_coordinator_report is not None or legacy_coordinator_sha256 is not None:
        # Transitional support for receipts written by the unreleased inline
        # implementation. Status still never returns the resource array.
        try:
            coordinator_report = SessionLifecycleReport.model_validate(raw_coordinator_report)
            observed_coordinator_sha256 = session_lifecycle_report_sha256(coordinator_report)
            remote_resources = report.resources if report is not None else []
            coordinator_report_bound = bool(
                isinstance(legacy_coordinator_sha256, str)
                and legacy_coordinator_sha256 == observed_coordinator_sha256
                and report is not None
                and coordinator_report.cluster == report.cluster
                and coordinator_report.session_id == report.session_id
                and coordinator_report.session_generation_id == report.session_generation_id
                and coordinator_report.mode == report.mode
                and coordinator_report.cleanup_operation_id == report.cleanup_operation_id
                and coordinator_report.cleanup_policy == report.cleanup_policy
                and coordinator_report.relay_cancel_requested == report.relay_cancel_requested
                and coordinator_report.scheduler_cancel_requested
                == report.scheduler_cancel_requested
                and coordinator_report.prior_session_status == report.prior_session_status
                and coordinator_report.post_session_status == report.post_session_status
                and len(coordinator_report.resources) >= len(remote_resources)
                and coordinator_report.resources[: len(remote_resources)] == remote_resources
            )
            coordinator_report_sha256 = legacy_coordinator_sha256
            coordinator_fields_valid = coordinator_report_bound
        except (RelayError, TypeError, ValueError) as exc:
            errors.append(f"owned session legacy coordinator cleanup report is invalid: {exc}")
        if not coordinator_fields_valid:
            errors.append("owned session coordinator cleanup report binding is invalid")
    common_expected_keys = {
        "schema_version",
        "owner",
        "cluster",
        "session_id",
        "session_generation_id",
        "api_pid",
        "api_pgid",
        "remote_api_port",
        "process_start_ticks",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "containment_mode",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "metadata_sha256",
        "cleanup_operation_id",
        "cleanup_policy",
        "cleanup_paths",
        "cleanup_targets",
        "cleanup_paths_pending",
        "cluster_registry_verified",
        "cluster_registry_removed",
        "completed_at",
        "report",
    }
    expected_key_sets = (
        common_expected_keys | {"coordinator_report_ref"},
        common_expected_keys | {"coordinator_report", "coordinator_report_sha256"},
    )
    raw_policy = document.get("cleanup_policy")
    policy = cast(dict[str, object], raw_policy) if isinstance(raw_policy, dict) else None
    completed_at = document.get("completed_at")
    try:
        parsed_completed_at = (
            datetime.fromisoformat(completed_at) if isinstance(completed_at, str) else None
        )
    except ValueError:
        parsed_completed_at = None
    receipt_file_resources = (
        [resource for resource in report.resources if resource.kind == "remote_session_files"]
        if report is not None
        else []
    )
    api_pid = document.get("api_pid")
    api_pgid = document.get("api_pgid")
    remote_api_port = document.get("remote_api_port")
    process_start = document.get("process_start_ticks")
    owner_token_sha256 = document.get("owner_token_sha256")
    release_sha256 = document.get("api_release_identity_sha256")
    registry_path = document.get("cluster_registry_path")
    registry_sha256 = document.get("cluster_registry_sha256")
    route_revision = document.get("cluster_route_revision")
    containment_mode = document.get("containment_mode")
    systemd_unit = document.get("systemd_unit")
    systemd_cgroup_path = document.get("systemd_cgroup_path")
    systemd_invocation_id = document.get("systemd_invocation_id")
    systemd_description = document.get("systemd_description")
    containment_broker_pid = document.get("containment_broker_pid")
    containment_broker_start = document.get("containment_broker_start_identity")
    cleanup_targets_verified = False
    validated_targets: list[OwnedSessionCleanupTarget] = []
    if validated_generation is not None:
        try:
            validated_targets = _validate_cleanup_targets(
                document.get("cleanup_targets"),
                generation_id=validated_generation,
            )
            cleanup_targets_verified = True
        except RelayError as exc:
            errors.append(str(exc))
    metadata_verified = bool(
        set(document) in expected_key_sets
        and document.get("owner") == "clio-relay"
        and document.get("cluster") == cluster
        and document.get("session_id") == session_id
        and validated_generation is not None
        and isinstance(api_pid, int)
        and not isinstance(api_pid, bool)
        and api_pid > 1
        and isinstance(api_pgid, int)
        and not isinstance(api_pgid, bool)
        and api_pgid > 0
        and isinstance(remote_api_port, int)
        and not isinstance(remote_api_port, bool)
        and remote_api_port > 0
        and isinstance(process_start, str)
        and process_start.isdigit()
        and isinstance(owner_token_sha256, str)
        and len(owner_token_sha256) == 64
        and all(character in "0123456789abcdef" for character in owner_token_sha256)
        and isinstance(release_sha256, str)
        and len(release_sha256) == 64
        and all(character in "0123456789abcdef" for character in release_sha256)
        and isinstance(registry_path, str)
        and bool(registry_path)
        and isinstance(registry_sha256, str)
        and len(registry_sha256) == 64
        and all(character in "0123456789abcdef" for character in registry_sha256)
        and isinstance(route_revision, str)
        and bool(route_revision)
        and containment_mode == "linux_systemd_scope"
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_cgroup_path, str)
        and bool(systemd_cgroup_path)
        and isinstance(systemd_invocation_id, str)
        and len(systemd_invocation_id) == 32
        and all(character in "0123456789abcdef" for character in systemd_invocation_id)
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and isinstance(containment_broker_pid, int)
        and not isinstance(containment_broker_pid, bool)
        and containment_broker_pid > 1
        and isinstance(containment_broker_start, str)
        and bool(containment_broker_start)
        and isinstance(document.get("metadata_sha256"), str)
        and len(cast(str, document.get("metadata_sha256"))) == 64
        and all(
            character in "0123456789abcdef"
            for character in cast(str, document.get("metadata_sha256"))
        )
        and document.get("cluster_registry_verified") is True
        and isinstance(document.get("cluster_registry_removed"), bool)
        and isinstance(document.get("cleanup_paths_pending"), bool)
        and document.get("cluster_registry_removed") is not document.get("cleanup_paths_pending")
        and document.get("cleanup_paths")
        == sorted(
            (
                "api.log",
                "api.pid",
                f"api-startup-{validated_generation}.json",
                f"cluster-registry-{validated_generation}.json",
            )
        )
        and cleanup_targets_verified
        and policy is not None
        and set(policy) == {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
        and all(isinstance(value, bool) for value in policy.values())
        and not (policy["cancel_scheduler_jobs"] and not policy["cancel_jobs"])
        and parsed_completed_at is not None
        and parsed_completed_at.tzinfo is not None
        and report is not None
        and report.cluster == cluster
        and report.session_id == session_id
        and report.session_generation_id == validated_generation
        and report.mode == "teardown"
        and report.cleanup_operation_id == document.get("cleanup_operation_id")
        and report.cleanup_policy == policy
        and report.relay_cancel_requested is policy["cancel_jobs"]
        and report.scheduler_cancel_requested is policy["cancel_scheduler_jobs"]
        and report.prior_session_status is not None
        and report.prior_session_status.ownership_verified
        and report.post_session_status is not None
        and report.post_session_status.running is False
        and report.post_session_status.ownership_verified
        and len(receipt_file_resources) == 1
        and receipt_file_resources[0].action == "close"
        and receipt_file_resources[0].outcome == "closed"
        and receipt_file_resources[0].ownership_verified
        and receipt_file_resources[0].verified_after_operation
        and receipt_file_resources[0].metadata.get("metadata_sanitized") is True
        and receipt_file_resources[0].metadata.get("target_identities")
        == [target.model_dump(mode="json") for target in validated_targets]
        and not report.errors
        and not report.residual_resources
        and coordinator_fields_valid
    )
    if not metadata_verified:
        errors.append("owned session cleanup receipt identity is invalid")

    generation_processes: list[_OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if (
        metadata_verified
        and validated_generation is not None
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
    ):
        try:
            generation_processes = _recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))
    if generation_processes:
        errors.append("owned generation processes remain after the cleanup receipt")

    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    if validated_generation is not None:
        try:
            admission_status = queue.owner_session_generation_status(
                session_id,
                session_generation_id=validated_generation,
            )
            raw_intent = admission_status.get("cleanup_intent")
            intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
            active_generation = admission_status.get("active_generation_id")
            closing_generation = admission_status.get("closing_generation_id")
            intent_matches = bool(
                intent is not None
                and intent.get("operation_id") == document.get("cleanup_operation_id")
                and {
                    key: intent.get(key)
                    for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
                }
                == document.get("cleanup_policy")
            )
            exact_pending_closure = bool(
                admission_status.get("closing") is True
                and admission_status.get("closed") is False
                and active_generation == validated_generation
                and closing_generation == validated_generation
                and intent_matches
            )
            exact_completed_closure = bool(
                admission_status.get("closing") is True
                and admission_status.get("closed") is True
                and active_generation is None
                and closing_generation == validated_generation
                and intent_matches
            )
            durable_generation_verified = bool(
                admission_status.get("owner_session_id") == session_id
                and admission_status.get("session_generation_id") == validated_generation
                and (exact_pending_closure or exact_completed_closure)
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"could not verify closed owner-session generation: {exc}")
        if not durable_generation_verified:
            errors.append("cleanup receipt has no exact durable closed-generation proof")

    recovery_verified = bool(
        metadata_verified
        and durable_generation_verified
        and generation_process_scan_verified
        and not generation_processes
        and not errors
    )
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=validated_generation,
        owner="clio-relay" if document.get("owner") == "clio-relay" else None,
        api_pid=api_pid if isinstance(api_pid, int) and not isinstance(api_pid, bool) else None,
        remote_api_port=(
            remote_api_port
            if isinstance(remote_api_port, int) and not isinstance(remote_api_port, bool)
            else None
        ),
        process_start_marker=process_start if isinstance(process_start, str) else None,
        leader_process_state="absent" if recovery_verified else "unverified",
        process_state=(
            "owned_running"
            if generation_processes
            else "already_closed"
            if recovery_verified and admission_status is not None and admission_status.get("closed")
            else "cleanup_pending"
            if recovery_verified
            else "unverified"
        ),
        running=bool(generation_processes),
        process_absence_verified=generation_process_scan_verified and not generation_processes,
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=metadata_verified,
        cluster_registry_verified=document.get("cluster_registry_verified") is True,
        durable_generation_verified=durable_generation_verified,
        cleanup_receipt=True,
        cleanup_paths_pending=(
            cast(bool, document.get("cleanup_paths_pending"))
            if isinstance(document.get("cleanup_paths_pending"), bool)
            else None
        ),
        coordinator_report=None,
        coordinator_report_ref=(coordinator_report_ref if coordinator_report_bound else None),
        coordinator_report_sha256=(
            coordinator_report_sha256
            if coordinator_report_bound and isinstance(coordinator_report_sha256, str)
            else None
        ),
        coordinator_report_bound=coordinator_report_bound,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        api_release_identity_verified=bool(
            metadata_verified and generation_process_scan_verified and not generation_processes
        ),
        ownership_token_present=False,
        admission_status=admission_status,
        errors=errors,
    )


def _read_owned_session_document(
    path: Path,
    *,
    label: str,
    effective_uid: int | None,
) -> tuple[dict[str, object], bytes]:
    """Read one bounded, regular, owner-scoped JSON document without following links."""
    descriptor: int | None = None
    parent_descriptor: int | None = None
    session_descriptor: int | None = None
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    binary = getattr(os, "O_BINARY", 0)
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    try:
        if os.name == "posix" and no_follow and directory_flag:
            parent_descriptor = os.open(
                path.parent.parent,
                os.O_RDONLY | directory_flag | no_follow,
            )
            parent_status = os.fstat(parent_descriptor)
            if not stat.S_ISDIR(parent_status.st_mode):
                raise RelayError(f"{label} parent is not a directory")
            if effective_uid is not None and parent_status.st_uid != effective_uid:
                raise RelayError(f"{label} parent is not owned by the current user")
            session_descriptor = os.open(
                path.parent.name,
                os.O_RDONLY | directory_flag | no_follow,
                dir_fd=parent_descriptor,
            )
            session_status = os.fstat(session_descriptor)
            if not stat.S_ISDIR(session_status.st_mode):
                raise RelayError(f"{label} session path is not a directory")
            if effective_uid is not None and session_status.st_uid != effective_uid:
                raise RelayError(f"{label} session path is not owned by the current user")
            descriptor = os.open(
                path.name,
                os.O_RDONLY | binary | no_follow,
                dir_fd=session_descriptor,
            )
        else:
            for directory, directory_label in (
                (path.parent.parent, "parent"),
                (path.parent, "session path"),
            ):
                directory_status = directory.lstat()
                if not stat.S_ISDIR(directory_status.st_mode):
                    raise RelayError(f"{label} {directory_label} is not a directory")
                if effective_uid is not None and directory_status.st_uid != effective_uid:
                    raise RelayError(f"{label} {directory_label} is not owned by the current user")
            path_status = path.lstat()
            if not stat.S_ISREG(path_status.st_mode):
                raise RelayError(f"{label} is not a regular file")
            if effective_uid is not None and path_status.st_uid != effective_uid:
                raise RelayError(f"{label} is not owned by the current user")
            descriptor = os.open(path, os.O_RDONLY | binary | no_follow)
        file_status = os.fstat(descriptor)
        if not stat.S_ISREG(file_status.st_mode):
            raise RelayError(f"{label} is not a regular file")
        if effective_uid is not None and file_status.st_uid != effective_uid:
            raise RelayError(f"{label} is not owned by the current user")
        if not 0 < file_status.st_size <= _MAX_OWNED_SESSION_DOCUMENT_BYTES:
            raise RelayError(f"{label} has an invalid size")
        payload = os.read(descriptor, _MAX_OWNED_SESSION_DOCUMENT_BYTES + 1)
        if len(payload) != file_status.st_size:
            raise RelayError(f"{label} changed while it was read")
    except FileNotFoundError as exc:
        raise RelayError(f"{label} is unavailable") from exc
    except RelayError:
        raise
    except OSError as exc:
        raise RelayError(f"{label} cannot be opened safely: {exc}") from exc
    finally:
        for open_descriptor in (descriptor, session_descriptor, parent_descriptor):
            if open_descriptor is not None:
                os.close(open_descriptor)
    try:
        raw = cast(object, json.loads(payload))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"{label} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RelayError(f"{label} is not a JSON object")
    return {str(key): value for key, value in cast(dict[object, object], raw).items()}, payload


def cleanup_connectors_cover_gateways(
    connector_resources: list[CleanupResource],
    gateway_resources: list[CleanupResource],
    *,
    mode: Literal["detach", "teardown"],
) -> bool:
    """Require exactly one desktop and remote connector disposition per gateway."""
    gateway_counts = Counter(resource.resource_id for resource in gateway_resources)
    if not gateway_counts or any(count != 1 for count in gateway_counts.values()):
        return False
    connector_counts: Counter[tuple[str, str]] = Counter()
    for resource in connector_resources:
        gateway_id = resource.metadata.get("gateway_session_id")
        if not isinstance(gateway_id, str) or gateway_id not in gateway_counts:
            return False
        connector_counts[(gateway_id, resource.kind)] += 1
        if not (
            resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            return False
        if resource.kind == "desktop_connector":
            if resource.action != "stop" or resource.outcome not in {"stopped", "missing"}:
                return False
        elif resource.kind == "remote_connector":
            if mode == "detach":
                if resource.action != "retain" or resource.outcome != "retained":
                    return False
            elif resource.action != "stop" or resource.outcome not in {"stopped", "missing"}:
                return False
        else:
            return False
    expected = {
        (gateway_id, connector_kind): 1
        for gateway_id in gateway_counts
        for connector_kind in ("desktop_connector", "remote_connector")
    }
    return connector_counts == Counter(expected)


class SessionLifecycleReport(BaseModel):
    """Machine-readable detach or teardown report for an owned relay session."""

    model_config = ConfigDict(extra="forbid")

    cluster: str | None = None
    session_id: str
    session_generation_id: DurableRecordId | None = None
    mode: Literal["detach", "teardown"]
    cleanup_operation_id: DurableRecordId | None = None
    cleanup_policy: dict[str, bool] = Field(default_factory=dict[str, bool])
    relay_cancel_requested: bool = False
    scheduler_cancel_requested: bool = False
    prior_session_status: RemoteSessionStateEvidence | None = None
    post_session_status: RemoteSessionStateEvidence | None = None
    resources: list[CleanupResource] = Field(default_factory=list[CleanupResource])
    errors: list[str] = Field(default_factory=list)

    @property
    def residual_resources(self) -> list[CleanupResource]:
        """Return resources that remain after a requested destructive action."""
        return [resource for resource in self.resources if resource.residual]

    def json_payload(self) -> dict[str, object]:
        """Return the report with an explicit residual-resource summary."""
        payload = self.model_dump(mode="json")
        payload["residual_resources"] = [
            resource.model_dump(mode="json") for resource in self.residual_resources
        ]
        payload["validation_resources"] = [
            resource.model_dump(mode="json") for resource in self.validation_resources()
        ]
        payload["cleanup_evidence"] = self.to_cleanup_evidence().model_dump(mode="json")
        payload["ok"] = not self.errors and not self.residual_resources
        return payload

    def validation_resources(self) -> list[ValidationResource]:
        """Return all lifecycle resources in the shared validation-report shape."""
        from clio_relay.validation_report import ValidationResource

        resources: list[ValidationResource] = []
        generation_id = self.session_generation_id
        stable_session_id = (
            f"{self.session_id}:{generation_id}" if generation_id is not None else self.session_id
        )
        for resource in self.resources:
            if resource.kind != "remote_relay_api":
                resources.append(resource.to_validation_resource(cluster=self.cluster))
                continue
            resources.append(
                ValidationResource(
                    kind="relay_session",
                    resource_id=stable_session_id,
                    role=f"{resource.kind}:{resource.action}",
                    cluster=self.cluster,
                    state=resource.outcome,
                    references=[resource.location],
                    metadata={
                        "session_id": self.session_id,
                        "session_generation_id": generation_id,
                        "api_pid": resource.resource_id,
                        "ownership_verified": resource.ownership_verified,
                        "verified_after_operation": resource.verified_after_operation,
                        "residual": resource.residual,
                        "detail": resource.detail,
                        **resource.metadata,
                    },
                )
            )
            resources.append(
                ValidationResource(
                    kind="relay_process",
                    resource_id=resource.resource_id,
                    role="remote_relay_api_process",
                    cluster=self.cluster,
                    state=resource.outcome,
                    references=[resource.location],
                    metadata={
                        "session_id": self.session_id,
                        "session_generation_id": generation_id,
                        "ownership_verified": resource.ownership_verified,
                        "verified_after_operation": resource.verified_after_operation,
                        "residual": resource.residual,
                        **resource.metadata,
                    },
                )
            )
        return resources

    def to_cleanup_evidence(self, *, stop_worker: bool | None = None) -> CleanupEvidence:
        """Convert this lifecycle result to shared cleanup evidence."""
        from clio_relay.validation_report import CleanupEvidence

        effective_stop_worker = (
            any(
                resource.kind == "worker_service" and resource.action == "stop"
                for resource in self.resources
            )
            if stop_worker is None
            else stop_worker
        )
        return CleanupEvidence(
            requested=True,
            mode=self.mode,
            operation_id=self.cleanup_operation_id,
            cancel_relay_jobs=self.relay_cancel_requested,
            cancel_scheduler_jobs=self.scheduler_cancel_requested,
            stop_worker=effective_stop_worker,
            actions=[resource.model_dump(mode="json") for resource in self.resources],
            remaining_resources=[
                resource.to_validation_resource(cluster=self.cluster)
                for resource in self.residual_resources
            ],
        )

    def to_live_validation_report(
        self,
        *,
        stop_worker: bool | None = None,
        cancel_jobs: bool | None = None,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert one live lifecycle operation to canonical release evidence."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationStatus,
            new_live_validation_report,
        )

        cluster = self.cluster or "unknown"
        report = new_live_validation_report(
            scenario="cleanup",
            cluster=cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        effective_stop_worker = (
            any(
                resource.kind == "worker_service" and resource.action == "stop"
                for resource in self.resources
            )
            if stop_worker is None
            else stop_worker
        )
        effective_cancel_jobs = self.relay_cancel_requested if cancel_jobs is None else cancel_jobs
        completed_at = datetime.now(UTC)
        checks: list[tuple[str, str, bool]] = []
        relay_stopped = False
        if self.mode == "detach":
            relay_resources = [
                resource for resource in self.resources if resource.kind == "remote_relay_api"
            ]
            retained = len(relay_resources) == 1 and all(
                resource.action == "retain"
                and resource.outcome == "retained"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
                for resource in relay_resources
            )
            checks.append(
                (
                    SESSION_DETACH_CHECK_ID,
                    "detach retained the owned session and removed desktop resources",
                    retained
                    and self.session_generation_id is not None
                    and not self.errors
                    and not self.residual_resources,
                )
            )
        else:
            relay_resources = [
                resource for resource in self.resources if resource.kind == "remote_relay_api"
            ]
            prior = self.prior_session_status
            post = self.post_session_status
            linked_pid = None if prior is None or prior.api_pid is None else str(prior.api_pid)
            relay_stopped = (
                prior is not None
                and prior.ownership_verified
                and post is not None
                and post.api_pid == prior.api_pid
                and not post.running
                and bool(relay_resources)
                and all(
                    resource.outcome in {"stopped", "missing"}
                    and resource.ownership_verified
                    and resource.resource_id == linked_pid
                    and resource.verified_after_operation
                    and not resource.residual
                    for resource in relay_resources
                )
            )
            checks.append((SESSION_TEARDOWN_CHECK_ID, "owned relay session stopped", relay_stopped))
        if effective_cancel_jobs:
            relay_cancel_resources = [
                resource for resource in self.resources if resource.kind == "relay_job"
            ]
            if relay_cancel_resources:
                checks.append(
                    (
                        SESSION_RELAY_CANCELED_CHECK_ID,
                        "owned relay jobs reached acknowledged cancellation or terminal state",
                        all(
                            resource.action in {"cancel", "retain"}
                            and resource.ownership_verified
                            and resource.outcome in {"canceled", "terminal"}
                            and resource.verified_after_operation
                            and not resource.residual
                            for resource in relay_cancel_resources
                        ),
                    )
                )
        retained_jobs = [
            resource
            for resource in self.resources
            if resource.action == "retain"
            and (
                resource.kind == "scheduler_job"
                or (resource.kind == "relay_job" and not effective_cancel_jobs)
            )
        ]
        if not self.scheduler_cancel_requested and retained_jobs:
            relay_resource_ids = {
                resource.resource_id for resource in self.resources if resource.kind == "relay_job"
            }
            gateway_resource_ids = {
                resource.resource_id
                for resource in self.resources
                if resource.kind == "gateway_record"
            }
            allowed_retention_outcomes = (
                {"retained"} if self.mode == "detach" else {"retained", "terminal", "missing"}
            )
            checks.append(
                (
                    SESSION_SCHEDULER_RETAINED_CHECK_ID,
                    (
                        "scheduler jobs were preserved while relay cancellation completed"
                        if effective_cancel_jobs
                        else "owned relay and scheduler jobs were preserved by default"
                    ),
                    all(
                        resource.ownership_verified
                        and (
                            resource.kind != "scheduler_job"
                            or (
                                resource.provider is not None
                                and (
                                    resource.metadata.get("relay_job_id") in relay_resource_ids
                                    or resource.metadata.get("gateway_session_id")
                                    in gateway_resource_ids
                                )
                            )
                        )
                        and resource.outcome in allowed_retention_outcomes
                        and (
                            self.mode != "detach"
                            or resource.observed_state
                            in {
                                "submitted",
                                "pending",
                                "queued",
                                "allocated",
                                "starting",
                                "ready",
                                "running",
                            }
                        )
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in retained_jobs
                    ),
                )
            )
        if self.scheduler_cancel_requested:
            relay_resources = {
                resource.resource_id: resource
                for resource in self.resources
                if resource.kind == "relay_job"
                and (
                    resource.action == "cancel"
                    or (resource.action == "retain" and resource.outcome == "terminal")
                )
            }
            scheduler_ids_by_relay: dict[str, list[object]] = {}
            for relay_id, resource in relay_resources.items():
                raw_scheduler_ids = resource.metadata.get("scheduler_job_ids")
                scheduler_ids_by_relay[relay_id] = (
                    cast(list[object], raw_scheduler_ids)
                    if isinstance(raw_scheduler_ids, list)
                    else []
                )
            expected_scheduler_links = {
                (relay_id, scheduler_id)
                for relay_id, scheduler_ids in scheduler_ids_by_relay.items()
                for scheduler_id in scheduler_ids
                if isinstance(scheduler_id, str)
            }
            canceled_scheduler_resources = [
                resource
                for resource in self.resources
                if resource.kind == "scheduler_job" and resource.action == "cancel"
            ]
            observed_scheduler_links = {
                (relay_id, resource.resource_id)
                for resource in canceled_scheduler_resources
                if isinstance((relay_id := resource.metadata.get("relay_job_id")), str)
                and resource.outcome == "canceled"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            }
            gateway_resource_ids = {
                resource.resource_id
                for resource in self.resources
                if resource.kind == "gateway_record"
            }
            every_scheduler_resource_linked = all(
                (
                    isinstance(resource.metadata.get("relay_job_id"), str)
                    and resource.metadata.get("relay_job_id") in relay_resources
                )
                or (
                    isinstance(resource.metadata.get("gateway_session_id"), str)
                    and resource.metadata.get("gateway_session_id") in gateway_resource_ids
                )
                for resource in canceled_scheduler_resources
            )
            scheduler_canceled = (
                every_scheduler_resource_linked
                and expected_scheduler_links == observed_scheduler_links
                and all(
                    resource.outcome == "canceled"
                    and resource.ownership_verified
                    and resource.verified_after_operation
                    and not resource.residual
                    for resource in canceled_scheduler_resources
                )
            )
            checks.append(
                (
                    SESSION_SCHEDULER_CANCELED_CHECK_ID,
                    "explicit scheduler cancellation completed",
                    scheduler_canceled,
                )
            )
        gateway_resources = [
            resource for resource in self.resources if resource.kind == "gateway_record"
        ]
        connector_resources = [
            resource
            for resource in self.resources
            if resource.kind in {"desktop_connector", "remote_connector"}
        ]
        if self.mode == "detach" and (connector_resources or gateway_resources):
            checks.append(
                (
                    SESSION_CONNECTORS_CHECK_ID,
                    "desktop connectors stopped and remote connectors retained",
                    cleanup_connectors_cover_gateways(
                        connector_resources,
                        gateway_resources,
                        mode="detach",
                    ),
                )
            )
        elif self.mode == "teardown" and (connector_resources or gateway_resources):
            checks.append(
                (
                    SESSION_CONNECTORS_CHECK_ID,
                    "owned connectors were cleaned",
                    cleanup_connectors_cover_gateways(
                        connector_resources,
                        gateway_resources,
                        mode="teardown",
                    ),
                )
            )
        if self.mode == "detach" and gateway_resources:
            checks.append(
                (
                    SESSION_GATEWAY_CHECK_ID,
                    "owned gateway records were retained for reattachment",
                    all(
                        resource.action == "retain"
                        and resource.outcome == "retained"
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in gateway_resources
                    ),
                )
            )
        elif self.mode == "teardown" and gateway_resources:
            checks.append(
                (
                    SESSION_GATEWAY_CHECK_ID,
                    "owned gateway records were closed or detached",
                    all(
                        resource.action == "close"
                        and resource.outcome == "closed"
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in gateway_resources
                    ),
                )
            )
        worker_resources = [
            resource for resource in self.resources if resource.kind == "worker_service"
        ]
        if self.mode == "teardown" and effective_stop_worker:
            checks.append(
                (
                    SESSION_WORKER_CHECK_ID,
                    "owned worker service reached a proven inactive state",
                    len(worker_resources) == 1
                    and all(
                        resource.action == "stop"
                        and resource.outcome in {"stopped", "missing"}
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and resource.observed_state in {"inactive", "not-found"}
                        and not resource.residual
                        for resource in worker_resources
                    ),
                )
            )
        if self.mode == "teardown":
            checks.append(
                (
                    SESSION_NO_RESIDUALS_CHECK_ID,
                    "no requested owned resources remain",
                    relay_stopped and not self.errors and not self.residual_resources,
                )
            )
        report.checks = [
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
                started_at=report.started_at,
                completed_at=completed_at,
                evidence=[
                    EvidenceReference(
                        kind="cleanup",
                        excerpt=summary,
                        metadata=self.json_payload(),
                    )
                ],
                error=None if passed else summary,
            )
            for check_id, summary, passed in checks
        ]
        report.resources = self.validation_resources()
        report.cleanup = self.to_cleanup_evidence(stop_worker=effective_stop_worker)
        report.completed_at = completed_at
        report.status = (
            ValidationStatus.PASSED
            if report.checks
            and all(check.status is ValidationStatus.PASSED for check in report.checks)
            else ValidationStatus.FAILED
        )
        report.error = None if report.status is ValidationStatus.PASSED else "cleanup failed"
        return report


class OwnedSessionCleanupFinalizeRequest(BaseModel):
    """Exact stdin contract for binding a fully verified cleanup report."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    expected_cleanup_operation_id: DurableRecordId
    expected_cleanup_policy: dict[str, bool]
    coordinator_report: SessionLifecycleReport
    coordinator_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionCleanupReportReadRequest(BaseModel):
    """Exact request for reading one finalized coordinator-report sidecar."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    coordinator_report_ref: OwnedSessionCleanupReportReference


def session_lifecycle_report_bytes(report: SessionLifecycleReport) -> bytes:
    """Return the canonical bounded sidecar encoding for one lifecycle report."""
    payload = json.dumps(
        report.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not payload or len(payload) > MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES:
        raise RelayError("coordinator cleanup report exceeds its byte limit")
    return payload


def session_lifecycle_report_sha256(report: SessionLifecycleReport) -> str:
    """Return the canonical digest for an exact lifecycle report."""
    return hashlib.sha256(session_lifecycle_report_bytes(report)).hexdigest()


def _coordinator_report_sidecar_name(
    *,
    session_generation_id: str,
    cleanup_operation_id: str,
) -> str:
    """Derive a stable basename without exposing variable-length identifiers."""
    validate_durable_record_id(session_generation_id)
    validate_durable_record_id(cleanup_operation_id)
    identity = (
        "clio-relay.owner-session-cleanup-report.v1\0"
        f"{session_generation_id}\0{cleanup_operation_id}"
    ).encode("ascii")
    return f"coordinator-cleanup-report-{hashlib.sha256(identity).hexdigest()}.json"


def _coordinator_report_reference(
    report: SessionLifecycleReport,
) -> tuple[OwnedSessionCleanupReportReference, bytes]:
    """Build the exact immutable sidecar reference and canonical payload."""
    generation_id = report.session_generation_id
    operation_id = report.cleanup_operation_id
    if generation_id is None or operation_id is None:
        raise RelayError("coordinator cleanup report omitted its durable identity")
    payload = session_lifecycle_report_bytes(report)
    reference = OwnedSessionCleanupReportReference(
        name=_coordinator_report_sidecar_name(
            session_generation_id=generation_id,
            cleanup_operation_id=operation_id,
        ),
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    return reference, payload


def _prune_unreferenced_cleanup_report_sidecars(
    transaction: _OwnedSessionTransaction,
    *,
    preserve_names: set[str],
) -> None:
    """Remove at most one proven orphan while preserving the in-flight publication."""
    candidates = transaction.cleanup_report_candidate_names()
    unexpected_pending = [
        name
        for name in candidates
        if _CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name) and name not in preserve_names
    ]
    if unexpected_pending:
        raise RelayError(
            "owned session has an unreferenced cleanup report pending file: "
            + ", ".join(unexpected_pending)
        )
    orphan_names = [
        name
        for name in candidates
        if _CLEANUP_REPORT_SIDECAR_PATTERN.fullmatch(name) and name not in preserve_names
    ]
    if len(orphan_names) > 1:
        raise RelayError("owned session has multiple unreferenced cleanup report sidecars")
    for name in orphan_names:
        linked = transaction.stat_regular(name)
        if linked is None:  # pragma: no cover - required stat
            raise RelayError(f"owned session cleanup report sidecar disappeared: {name}")
        if not 0 < linked.st_size <= MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES:
            raise RelayError(f"owned session cleanup report sidecar has an invalid size: {name}")
        transaction.unlink_verified(
            name,
            expected_device=linked.st_dev,
            expected_inode=linked.st_ino,
            expected_size=linked.st_size,
            expected_sha256=None,
            maximum_bytes=None,
        )
    remaining = set(transaction.cleanup_report_candidate_names())
    if not remaining.issubset(preserve_names):
        raise RelayError("owned session cleanup report sidecar pruning was not exact")


def _read_coordinator_report_sidecar(
    transaction: _OwnedSessionTransaction,
    reference: OwnedSessionCleanupReportReference,
    *,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str,
) -> SessionLifecycleReport:
    """Read and verify one exact coordinator report through the pinned dirfd."""
    expected_name = _coordinator_report_sidecar_name(
        session_generation_id=expected_session_generation_id,
        cleanup_operation_id=expected_cleanup_operation_id,
    )
    if reference.name != expected_name:
        raise RelayError("coordinator cleanup report sidecar name does not match its identity")
    payload = transaction.read_bytes(
        reference.name,
        maximum_bytes=reference.size,
    )
    if payload is None:  # pragma: no cover - required read
        raise RelayError("coordinator cleanup report sidecar is unavailable")
    if len(payload) != reference.size:
        raise RelayError("coordinator cleanup report sidecar size does not match its reference")
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), reference.sha256):
        raise RelayError("coordinator cleanup report sidecar digest does not match its reference")
    try:
        report = SessionLifecycleReport.model_validate_json(payload)
    except ValueError as exc:
        raise RelayError(f"coordinator cleanup report sidecar is invalid: {exc}") from exc
    if not hmac.compare_digest(session_lifecycle_report_bytes(report), payload):
        raise RelayError("coordinator cleanup report sidecar is not canonically encoded")
    return report


def _coordinator_report_extends_remote_report(
    report: SessionLifecycleReport,
    remote_report: SessionLifecycleReport,
) -> bool:
    """Return whether the full coordinator report preserves the remote prefix exactly."""
    return bool(
        report.cluster == remote_report.cluster
        and report.session_id == remote_report.session_id
        and report.session_generation_id == remote_report.session_generation_id
        and report.mode == remote_report.mode
        and report.cleanup_operation_id == remote_report.cleanup_operation_id
        and report.cleanup_policy == remote_report.cleanup_policy
        and report.relay_cancel_requested == remote_report.relay_cancel_requested
        and report.scheduler_cancel_requested == remote_report.scheduler_cancel_requested
        and report.prior_session_status == remote_report.prior_session_status
        and report.post_session_status == remote_report.post_session_status
        and len(report.resources) >= len(remote_report.resources)
        and report.resources[: len(remote_report.resources)] == remote_report.resources
    )


@dataclass(frozen=True)
class _BoundedCommandResult:
    """Bounded output captured from one local child command."""

    returncode: int
    stdout: bytes
    stderr: bytes


class _RemoteSessionCommandDeadline(RelayError):
    """The local transport deadline expired without proving remote completion."""


class _RemoteSessionCommandRejected(RelayError):
    """The authenticated remote command rejected this invocation."""

    def __init__(self, rejection: OwnedSessionStartRejection) -> None:
        super().__init__(rejection.error)
        self.rejection = rejection


class _RemoteSessionCommandAmbiguous(RelayError):
    """The SSH transport ended without proving whether the remote command completed."""


def _run_bounded_command(
    command: list[str],
    *,
    input_bytes: bytes = b"",
    timeout_seconds: float,
    stdout_limit: int,
    stderr_limit: int,
    environment: dict[str, str] | None = None,
) -> _BoundedCommandResult:
    """Run one isolated process tree while bounding both pipes before allocation."""
    from clio_relay.bounded_process import (
        BoundedProcessError,
        BoundedProcessOutputLimit,
        BoundedProcessTimeout,
        run_bounded_process,
    )

    try:
        result = run_bounded_process(
            command,
            environment=environment,
            input_bytes=input_bytes,
            timeout_seconds=timeout_seconds,
            stdout_maximum_bytes=stdout_limit,
            stderr_maximum_bytes=stderr_limit,
            require_enforceable=os.name == "nt",
        )
    except BoundedProcessTimeout as exc:
        raise RelayError(f"bounded command timed out after {timeout_seconds:g} seconds") from exc
    except BoundedProcessOutputLimit as exc:
        raise RelayError("bounded command output exceeded its byte limit") from exc
    except BoundedProcessError as exc:
        raise RelayError(f"bounded command process-tree cleanup failed: {exc}") from exc
    return _BoundedCommandResult(
        returncode=result.returncode,
        stdout=result.stdout.encode("utf-8"),
        stderr=result.stderr.encode("utf-8"),
    )


def _current_session_api_release_identity() -> SessionApiReleaseIdentity:
    """Return the exact locally installed release identity for an API child."""
    from clio_relay.installation import installation_info

    info = installation_info()
    raw_receipt = info.get("receipt")
    receipt = (
        {str(key): value for key, value in cast(dict[object, object], raw_receipt).items()}
        if isinstance(raw_receipt, dict)
        else None
    )
    software = info.get("software")
    identity: dict[str, object] = {
        "schema_version": "clio-relay.session-api-release.v1",
        "distribution_version": info.get("distribution_version"),
        "artifact_sha256": (receipt.get("artifact_sha256") if receipt is not None else None),
        "software": software,
    }
    try:
        validated = SessionApiReleaseIdentity.model_validate(identity)
    except ValueError as exc:
        raise RelayError("session API installation identity is incomplete") from exc
    if (
        info.get("schema_version") != "clio-relay.installation-info.v1"
        or info.get("receipt_matches_install") is not True
        or receipt is None
        or receipt.get("schema_version") != "clio-relay.install-receipt.v1"
        or receipt.get("distribution_version") != validated.distribution_version
        or receipt.get("software") != validated.software.model_dump(mode="json")
    ):
        raise RelayError("session API installation receipt does not match the running package")
    return validated


def _validated_start_registry(
    request: OwnedSessionStartRequest,
) -> tuple[ClusterRegistry, bytes]:
    """Validate one exact request-carried cluster registry and its route identity."""
    try:
        registry = ClusterRegistry.model_validate(request.cluster_registry)
    except ValueError as exc:
        raise RelayError(f"owned session cluster registry is invalid: {exc}") from exc
    payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_CLUSTER_REGISTRY_BYTES:
        raise RelayError("owned session cluster registry exceeds its byte limit")
    if hashlib.sha256(payload).hexdigest() != request.cluster_registry_sha256:
        raise RelayError("owned session cluster registry digest does not match its payload")
    if set(registry.clusters) != {request.cluster}:
        raise RelayError("owned session cluster registry does not contain one exact cluster")
    definition = registry.clusters[request.cluster]
    if (
        definition.name != request.cluster
        or cluster_route_revision(definition) != request.cluster_route_revision
    ):
        raise RelayError("owned session cluster route identity does not match its registry")
    return registry, payload


def _write_session_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    operation: Literal["start", "teardown"],
    identity: dict[str, object],
    error: str | None = None,
) -> None:
    """Write one atomic, resumable owner-session attempt record."""
    document = {
        "schema_version": (
            "clio-relay.owner-session-attempt.v2"
            if operation == "start"
            else "clio-relay.owner-session-attempt.v1"
        ),
        "operation": operation,
        **identity,
        "observed_at": datetime.now(UTC).isoformat(),
        "error": error[:_MAX_SESSION_START_ERROR_CHARS] if error is not None else None,
    }
    transaction.atomic_write(
        f"{operation}-attempt.json",
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


def _assert_remote_port_available(port: int) -> None:
    """Fail before core admission changes when the requested loopback port is busy."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as exc:
            raise RelayError(f"remote API port is already occupied: {port}") from exc


def _owned_session_api_token(*, require_token: bool) -> str | None:
    """Select the child API token while honoring an explicit auth-disabled plan."""
    ambient_token = os.environ.get("CLIO_RELAY_API_TOKEN")
    if require_token and not ambient_token:
        raise RelayError("owned session API token is required but unavailable")
    return ambient_token if require_token else None


def _wait_for_api_ready(
    *,
    process: subprocess.Popen[bytes],
    port: int,
    require_token: bool,
) -> float:
    """Wait boundedly for an API child to report the exact planned auth policy."""
    started = time.monotonic()
    deadline = started + _REMOTE_API_READINESS_TIMEOUT_SECONDS
    url = f"http://127.0.0.1:{port}/healthz"
    last_error = "API did not become ready"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError("owned API process exited before readiness")
        try:
            with urllib.request.urlopen(url, timeout=0.25) as response:
                response_bytes = response.read(_MAX_API_HEALTH_RESPONSE_BYTES + 1)
                if len(response_bytes) > _MAX_API_HEALTH_RESPONSE_BYTES:
                    raise RelayError("owned API health response exceeded its byte limit")
                payload = cast(object, json.loads(response_bytes))
                if (
                    response.status == 200
                    and isinstance(payload, dict)
                    and cast(dict[str, object], payload).get("ok") is True
                    and cast(dict[str, object], payload).get("auth") is require_token
                ):
                    return time.monotonic() - started
                last_error = f"unexpected health response: {payload!r}"
        except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(0.1)
    raise RelayError(
        "owned API did not become ready within "
        f"{_REMOTE_API_READINESS_TIMEOUT_SECONDS:g} seconds: {last_error}"
    )


def _owned_api_startup_log_detail(
    transaction: _OwnedSessionTransaction,
    *,
    secret_values: Iterable[str],
) -> str:
    """Return one bounded credential-redacted API startup diagnostic."""
    try:
        payload = transaction.read_bytes(
            "api.log",
            maximum_bytes=_MAX_SESSION_START_ERROR_CHARS,
            required=False,
        )
    except RelayError:
        return ""
    if not payload:
        return ""
    detail = payload.decode("utf-8", errors="replace").strip()
    for value in secret_values:
        if len(value) >= 4:
            detail = detail.replace(value, "<redacted>")
    return detail[-_MAX_SESSION_START_ERROR_CHARS:]


def _wait_for_api_startup_receipt(
    *,
    transaction: _OwnedSessionTransaction,
    process: subprocess.Popen[Any],
    receipt_name: str,
    owner_token: str,
    expected: dict[str, object],
    proc_root: Path,
) -> _OwnedGenerationProcess:
    """Wait for and verify the API child's signed cgroup-bound startup receipt."""
    from clio_relay.process_containment import recorded_linux_systemd_scope_process_ids

    expected_keys = {
        "schema_version",
        "cluster",
        "session_id",
        "session_generation_id",
        "api_pid",
        "api_pgid",
        "process_start_ticks",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "observed_at",
        "hmac_sha256",
    }
    deadline = time.monotonic() + _REMOTE_API_READINESS_TIMEOUT_SECONDS
    last_error = "startup receipt did not materialize"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError("owned API containment exited before startup receipt")
        try:
            document = transaction.read_json(receipt_name, required=False)
            if document is None:
                time.sleep(0.05)
                continue
            observed_at = document.get("observed_at")
            parsed_observed_at = (
                datetime.fromisoformat(observed_at) if isinstance(observed_at, str) else None
            )
            api_pid = document.get("api_pid")
            api_pgid = document.get("api_pgid")
            process_start = document.get("process_start_ticks")
            signature = document.get("hmac_sha256")
            exact_expected = all(document.get(key) == value for key, value in expected.items())
            if not (
                set(document) == expected_keys
                and document.get("schema_version") == "clio-relay.owner-session-api-startup.v1"
                and exact_expected
                and isinstance(api_pid, int)
                and not isinstance(api_pid, bool)
                and api_pid > 1
                and isinstance(api_pgid, int)
                and not isinstance(api_pgid, bool)
                and api_pgid > 0
                and isinstance(process_start, str)
                and process_start.isdigit()
                and parsed_observed_at is not None
                and parsed_observed_at.tzinfo is not None
                and isinstance(signature, str)
                and hmac.compare_digest(
                    signature,
                    _startup_receipt_signature(document, owner_token=owner_token),
                )
            ):
                raise RelayError("owned API startup receipt identity is invalid")
            process_identity = _read_proc_identity(proc_root=proc_root, pid=api_pid)
            if (
                process_identity.process_group_id != api_pgid
                or process_identity.start_ticks != process_start
                or not _is_clio_relay_api_leader(proc_root=proc_root, pid=api_pid)
            ):
                raise RelayError("owned API startup receipt process identity changed")
            pids = recorded_linux_systemd_scope_process_ids(
                unit=cast(str, expected["systemd_unit"]),
                cgroup_path=cast(str, expected["systemd_cgroup_path"]),
                invocation_id=cast(str, expected["systemd_invocation_id"]),
                description=cast(str, expected["systemd_description"]),
            )
            if api_pid not in pids:
                raise RelayError("owned API startup receipt leader is outside its exact cgroup")
            return process_identity
        except (OSError, RelayError, ValueError) as exc:
            last_error = str(exc)
            time.sleep(0.05)
    raise RelayError(f"owned API startup receipt was not verified: {last_error}")


def _validated_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    cluster: str,
    session_id: str,
    start_operation_id: str | None = None,
    cluster_registry_sha256: str | None = None,
    cluster_route_revision_value: str | None = None,
    remote_api_port: int | None = None,
    replace: bool | None = None,
    require_token: bool | None = None,
    expected_api_release_identity_sha256: str | None = None,
    allow_legacy: bool = False,
) -> dict[str, object] | None:
    """Return one structurally exact start journal matching optional selectors."""
    attempt = transaction.read_json("start-attempt.json", required=False)
    if attempt is None:
        return None
    expected_keys = {
        "schema_version",
        "operation",
        "cluster",
        "session_id",
        "start_operation_id",
        "session_generation_id",
        "owner_token",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "expected_api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "remote_api_port",
        "replace",
        "require_token",
        "start_phase",
        "systemd_unit",
        "systemd_description",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "observed_at",
        "error",
    }
    legacy_keys = expected_keys - {
        "start_operation_id",
        "expected_api_release_identity_sha256",
        "replace",
        "require_token",
    }
    legacy = attempt.get("schema_version") == "clio-relay.owner-session-attempt.v1"
    generation = attempt.get("session_generation_id")
    operation_id = attempt.get("start_operation_id")
    observed_at = attempt.get("observed_at")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
        validated_operation_id = (
            validate_durable_record_id(operation_id) if isinstance(operation_id, str) else None
        )
        parsed_observed_at = (
            datetime.fromisoformat(observed_at) if isinstance(observed_at, str) else None
        )
    except ValueError:
        validated_generation = None
        validated_operation_id = None
        parsed_observed_at = None
    registry_path = attempt.get("cluster_registry_path")
    owner_token = attempt.get("owner_token")
    owner_token_sha256 = attempt.get("owner_token_sha256")
    start_phase = attempt.get("start_phase")
    systemd_unit = attempt.get("systemd_unit")
    systemd_description = attempt.get("systemd_description")
    cgroup_path = attempt.get("systemd_cgroup_path")
    invocation_id = attempt.get("systemd_invocation_id")
    broker_pid = attempt.get("containment_broker_pid")
    broker_start = attempt.get("containment_broker_start_identity")
    expected_registry_path = (
        transaction.path / f"cluster-registry-{validated_generation}.json"
        if validated_generation is not None
        else None
    )
    if not (
        set(attempt) == (legacy_keys if legacy else expected_keys)
        and (
            attempt.get("schema_version") == "clio-relay.owner-session-attempt.v2"
            or (allow_legacy and legacy)
        )
        and attempt.get("operation") == "start"
        and attempt.get("cluster") == cluster
        and attempt.get("session_id") == session_id
        and (
            (not legacy and validated_operation_id is not None)
            or (legacy and validated_operation_id is None)
        )
        and (
            start_operation_id is None
            or (not legacy and validated_operation_id == start_operation_id)
        )
        and validated_generation is not None
        and isinstance(owner_token, str)
        and len(owner_token) == 64
        and all(character in "0123456789abcdef" for character in owner_token)
        and owner_token_sha256 == hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
        and isinstance(attempt.get("api_release_identity_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", cast(str, attempt.get("api_release_identity_sha256")))
        is not None
        and (
            legacy
            or (
                attempt.get("expected_api_release_identity_sha256") is None
                or (
                    isinstance(attempt.get("expected_api_release_identity_sha256"), str)
                    and re.fullmatch(
                        r"[0-9a-f]{64}",
                        cast(str, attempt.get("expected_api_release_identity_sha256")),
                    )
                    is not None
                )
            )
        )
        and (
            expected_api_release_identity_sha256 is None
            or (
                not legacy
                and attempt.get("expected_api_release_identity_sha256")
                == expected_api_release_identity_sha256
            )
        )
        and registry_path == str(expected_registry_path)
        and isinstance(attempt.get("cluster_registry_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", cast(str, attempt.get("cluster_registry_sha256")))
        is not None
        and (
            cluster_registry_sha256 is None
            or attempt.get("cluster_registry_sha256") == cluster_registry_sha256
        )
        and isinstance(attempt.get("cluster_route_revision"), str)
        and bool(attempt.get("cluster_route_revision"))
        and (
            cluster_route_revision_value is None
            or attempt.get("cluster_route_revision") == cluster_route_revision_value
        )
        and isinstance(attempt.get("remote_api_port"), int)
        and not isinstance(attempt.get("remote_api_port"), bool)
        and 0 < cast(int, attempt.get("remote_api_port")) <= 65_535
        and (remote_api_port is None or attempt.get("remote_api_port") == remote_api_port)
        and (legacy or isinstance(attempt.get("replace"), bool))
        and (replace is None or (not legacy and attempt.get("replace") is replace))
        and (legacy or isinstance(attempt.get("require_token"), bool))
        and (
            require_token is None or (not legacy and attempt.get("require_token") is require_token)
        )
        and start_phase in {"pending", "admitted", "scope_bound", "contained"}
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and (
            (
                start_phase in {"pending", "admitted"}
                and cgroup_path is None
                and invocation_id is None
                and broker_pid is None
                and broker_start is None
            )
            or (
                start_phase in {"scope_bound", "contained"}
                and isinstance(cgroup_path, str)
                and bool(cgroup_path)
                and isinstance(invocation_id, str)
                and len(invocation_id) == 32
                and all(character in "0123456789abcdef" for character in invocation_id)
                and (
                    (start_phase == "scope_bound" and broker_pid is None and broker_start is None)
                    or (
                        start_phase == "contained"
                        and isinstance(broker_pid, int)
                        and not isinstance(broker_pid, bool)
                        and broker_pid > 1
                        and isinstance(broker_start, str)
                        and bool(broker_start)
                    )
                )
            )
        )
        and parsed_observed_at is not None
        and parsed_observed_at.tzinfo is not None
        and (
            attempt.get("error") is None
            or (
                isinstance(attempt.get("error"), str)
                and len(cast(str, attempt.get("error"))) <= _MAX_SESSION_START_ERROR_CHARS
            )
        )
    ):
        raise RelayError("prior owned-session start attempt identity is invalid")
    return attempt


def _validated_resumable_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    request: OwnedSessionStartRequest,
    release_identity_sha256: str,
) -> dict[str, object] | None:
    """Return the exact prior start attempt selected by a retry request."""
    expected_release_sha256 = (
        request.expected_api_release_identity.sha256()
        if request.expected_api_release_identity is not None
        else None
    )
    attempt = _validated_start_attempt(
        transaction,
        cluster=request.cluster,
        session_id=request.session_id,
        start_operation_id=request.start_operation_id,
        cluster_registry_sha256=request.cluster_registry_sha256,
        cluster_route_revision_value=request.cluster_route_revision,
        remote_api_port=request.remote_api_port,
        replace=request.replace,
        require_token=request.require_token,
        expected_api_release_identity_sha256=expected_release_sha256,
    )
    if attempt is not None and (
        attempt.get("expected_api_release_identity_sha256") != expected_release_sha256
        or attempt.get("api_release_identity_sha256") != release_identity_sha256
    ):
        raise RelayError("prior owned-session start release identity changed")
    return attempt


def _legacy_start_attempt_matches_metadata(
    *,
    attempt: dict[str, object],
    metadata: dict[str, object],
) -> bool:
    """Return whether a v1 start journal names the exact committed generation."""
    identity_fields = (
        "cluster",
        "session_id",
        "session_generation_id",
        "owner_token",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "remote_api_port",
        "systemd_unit",
        "systemd_description",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "containment_broker_pid",
        "containment_broker_start_identity",
    )
    owner_token = metadata.get("owner_token")
    return bool(
        attempt.get("start_phase") == "contained"
        and all(attempt.get(field) == metadata.get(field) for field in identity_fields)
        and isinstance(owner_token, str)
        and attempt.get("owner_token_sha256")
        == hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
    )


def _migrate_legacy_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    request: OwnedSessionStartRequest,
    release_identity_sha256: str,
    replacement_identity_verified: bool = False,
) -> dict[str, object] | None:
    """Bind a valid pre-v2 attempt to a caller-supplied planned operation.

    Version 1 did not contain an operation selector or the complete request
    policy, so it is never exposed as a queryable start.  A new planned request
    may adopt its exact generation only after every identity v1 did record has
    matched; a failed legacy attempt additionally requires explicit replacement.
    """
    attempt = _validated_start_attempt(
        transaction,
        cluster=request.cluster,
        session_id=request.session_id,
        cluster_registry_sha256=request.cluster_registry_sha256,
        cluster_route_revision_value=request.cluster_route_revision,
        remote_api_port=request.remote_api_port,
        allow_legacy=True,
    )
    if attempt is None or attempt.get("schema_version") != ("clio-relay.owner-session-attempt.v1"):
        return attempt
    release_changed = attempt.get("api_release_identity_sha256") != release_identity_sha256
    if release_changed and not (request.replace and replacement_identity_verified):
        raise RelayError("legacy owned-session start release identity changed")
    if attempt.get("error") is not None and not request.replace:
        raise RelayError("a failed legacy owned-session start requires --replace")
    identity = {
        key: value
        for key, value in attempt.items()
        if key not in {"schema_version", "operation", "observed_at", "error"}
    }
    identity.update(
        {
            "start_operation_id": request.start_operation_id,
            "api_release_identity_sha256": release_identity_sha256,
            "expected_api_release_identity_sha256": (
                request.expected_api_release_identity.sha256()
                if request.expected_api_release_identity is not None
                else None
            ),
            "replace": request.replace,
            "require_token": request.require_token,
        }
    )
    _write_session_attempt(transaction, operation="start", identity=identity)
    return _validated_resumable_start_attempt(
        transaction,
        request=request,
        release_identity_sha256=release_identity_sha256,
    )


def _owned_api_requires_token(*, proc_root: Path, pid: int) -> bool:
    """Read the exact verified API leader argv and return its auth policy."""
    try:
        arguments = _read_bounded_proc_bytes(
            proc_root / str(pid) / "cmdline",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).split(bytes([0]))
    except (FileNotFoundError, ProcessLookupError) as exc:
        raise RelayError("owned API leader disappeared during auth verification") from exc
    if not _is_clio_relay_api_leader(proc_root=proc_root, pid=pid):
        raise RelayError("owned API auth policy cannot be tied to the verified leader")
    return b"--require-token" in arguments


class _RecoveredStartProbe:
    """Minimal process observation used while adopting an exact persistent scope."""

    def poll(self) -> None:
        """The receipt and scope checks, not a stale parent handle, prove liveness."""
        return None


def _promote_resumable_contained_start(
    *,
    transaction: _OwnedSessionTransaction,
    attempt: dict[str, object],
    request: OwnedSessionStartRequest,
    release_identity: SessionApiReleaseIdentity,
    queue: _OwnedSessionQueue,
    proc_root: Path,
    home: Path | None,
) -> list[str] | None:
    """Commit ready metadata when an exact crash-surviving API already exists."""
    if attempt.get("start_phase") != "contained" or attempt.get("error") is not None:
        return None
    generation_id = cast(str, attempt["session_generation_id"])
    owner_token = cast(str, attempt["owner_token"])
    receipt_name = f"api-startup-{generation_id}.json"
    receipt_path = transaction.path / receipt_name
    expected_receipt = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation_id,
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
    }
    probe = cast(subprocess.Popen[Any], _RecoveredStartProbe())
    try:
        process_identity = _wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        ready_seconds = _wait_for_api_ready(
            process=cast(subprocess.Popen[bytes], probe),
            port=request.remote_api_port,
            require_token=request.require_token,
        )
        final_process_identity = _wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        if final_process_identity != process_identity:
            raise RelayError("recovered owned API identity changed after health verification")
    except RelayError:
        return None
    receipt_payload = transaction.read_bytes(
        receipt_name,
        maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
    )
    if receipt_payload is None:  # pragma: no cover - required read
        return None
    metadata = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "remote_api_port": request.remote_api_port,
        "api_pid": process_identity.pid,
        "api_pgid": process_identity.process_group_id,
        "owner_token": owner_token,
        "session_generation_id": generation_id,
        "api_release_identity": release_identity.model_dump(mode="json"),
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "cluster_authority_verified": True,
        "process_start_ticks": process_identity.start_ticks,
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
        "containment_broker_pid": attempt["containment_broker_pid"],
        "containment_broker_start_identity": attempt["containment_broker_start_identity"],
        "api_startup_receipt_path": str(receipt_path),
        "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "owner": "clio-relay",
    }
    transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
    transaction.atomic_write("metadata.json", json.dumps(metadata, indent=2).encode("utf-8"))
    queue.clear_owner_session_closing(request.session_id, session_generation_id=generation_id)
    promoted_status = inspect_owned_session_recovery_status(
        cluster=request.cluster,
        session_id=request.session_id,
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
        transaction=transaction,
        expected_start_operation_id=request.start_operation_id,
        expected_cluster_route_revision=request.cluster_route_revision,
    )
    if not (
        promoted_status.recovery_verified
        and promoted_status.leader_process_state == "owned_running"
        and promoted_status.api_pid == process_identity.pid
        and promoted_status.ownership_verified
        and promoted_status.session_generation_id == generation_id
        and promoted_status.start_attempt_verified
    ):
        raise RelayError("recovered owned API did not pass post-commit identity verification")
    return [
        f"remote_api_ready_seconds={ready_seconds:.3f}",
        f"session_started={request.session_id}",
        f"start_operation_id={request.start_operation_id}",
        f"api_pid={process_identity.pid}",
        f"session_generation_id={generation_id}",
        f"remote_api_port={request.remote_api_port}",
        f"metadata={transaction.path / 'metadata.json'}",
    ]


def execute_owned_session_identity_challenge(
    request: OwnedSessionIdentityChallengeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> dict[str, object]:
    """Sign one nonce only after pinned metadata and live leader verification."""
    from clio_relay.config import RelaySettings

    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session challenge cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.session_generation_id == request.session_generation_id
            and status.running
            and status.leader_process_state == "owned_running"
            and status.api_pid is not None
            and status.api_pid in status.generation_process_pids
        ):
            detail = "; ".join(status.errors) or "live API leader proof was incomplete"
            raise RelayError(f"owned session identity challenge was refused: {detail}")
        owner_token = document.get("owner_token")
        if not isinstance(owner_token, str) or len(owner_token) != 64:
            raise RelayError("owned session identity challenge token is invalid")
        message = "\n".join(
            (
                "clio-relay.session-identity.v1",
                request.cluster,
                request.session_id,
                request.session_generation_id,
                request.nonce,
            )
        ).encode("utf-8")
        signature = hmac.new(
            owner_token.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()
        return {
            "schema_version": "clio-relay.session-identity.v1",
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": request.session_generation_id,
            "nonce": request.nonce,
            "hmac_sha256": signature,
        }


def execute_owned_session_start(
    request: OwnedSessionStartRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> list[str]:
    """Execute one exact cluster-local start under the pinned session transaction."""
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(
        session_id=request.session_id,
        remote_api_port=request.remote_api_port,
    )
    _, registry_payload = _validated_start_registry(request)
    release_identity = _current_session_api_release_identity()
    if (
        request.expected_api_release_identity is not None
        and release_identity != request.expected_api_release_identity
    ):
        raise RelayError("session API installation changed after compatibility verification")
    api_token = _owned_session_api_token(require_token=request.require_token)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    queue = ClioCoreQueue(settings_core_dir)
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session start cannot verify the effective user")
    uid = get_effective_uid()

    with open_owned_session_transaction(
        session_id=request.session_id,
        create=True,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        existing = transaction.read_json("metadata.json", required=False)
        raw_attempt = transaction.read_json("start-attempt.json", required=False)
        legacy_migrated = bool(
            raw_attempt is not None
            and raw_attempt.get("schema_version") == "clio-relay.owner-session-attempt.v1"
        )
        if legacy_migrated:
            legacy_attempt = _validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
                cluster_registry_sha256=request.cluster_registry_sha256,
                cluster_route_revision_value=request.cluster_route_revision,
                remote_api_port=request.remote_api_port,
                allow_legacy=True,
            )
            if legacy_attempt is None:  # pragma: no cover - raw attempt exists
                raise RelayError("legacy owned-session start attempt disappeared")
            replacement_identity_verified = False
            if existing is not None:
                legacy_status = inspect_owned_session_recovery_status(
                    cluster=request.cluster,
                    session_id=request.session_id,
                    core_dir=settings_core_dir,
                    home=home,
                    proc_root=proc_root,
                    effective_uid=uid,
                )
                if not (
                    legacy_status.recovery_verified
                    and legacy_status.ownership_verified
                    and legacy_status.session_generation_id
                    == legacy_attempt.get("session_generation_id")
                    and legacy_status.cluster_route_revision
                    == legacy_attempt.get("cluster_route_revision")
                    and legacy_status.remote_api_port == legacy_attempt.get("remote_api_port")
                    and legacy_status.api_release_identity is not None
                    and legacy_status.api_release_identity.sha256()
                    == legacy_attempt.get("api_release_identity_sha256")
                    and _legacy_start_attempt_matches_metadata(
                        attempt=legacy_attempt,
                        metadata=existing,
                    )
                ):
                    raise RelayError(
                        "legacy start journal does not match exact verified session metadata"
                    )
                replacement_identity_verified = True
                if not request.replace:
                    if not (
                        legacy_status.running
                        and legacy_status.leader_process_state == "owned_running"
                        and legacy_status.api_pid is not None
                    ):
                        raise RelayError(
                            "legacy owned session cannot be adopted without exact live proof; "
                            "use --replace"
                        )
                    if (
                        _owned_api_requires_token(
                            proc_root=proc_root,
                            pid=legacy_status.api_pid,
                        )
                        is not request.require_token
                    ):
                        raise RelayError("legacy owned session auth policy differs; use --replace")
            elif (
                request.replace
                and legacy_attempt.get("api_release_identity_sha256") != release_identity.sha256()
            ):
                legacy_generation = cast(str, legacy_attempt["session_generation_id"])
                legacy_phase = cast(str, legacy_attempt["start_phase"])
                admission = queue.owner_session_generation_status(
                    request.session_id,
                    session_generation_id=legacy_generation,
                )
                active_generation = admission.get("active_generation_id")
                admission_verified = bool(
                    admission.get("owner_session_id") == request.session_id
                    and admission.get("session_generation_id") == legacy_generation
                    and admission.get("closing_generation_id") is None
                    and (
                        (
                            legacy_phase == "pending"
                            and active_generation in {None, legacy_generation}
                        )
                        or (
                            legacy_phase != "pending"
                            and active_generation == legacy_generation
                            and admission.get("open") is True
                        )
                    )
                )
                if not admission_verified:
                    raise RelayError(
                        "legacy owned-session generation conflicts with durable core admission"
                    )
                if legacy_phase in {"scope_bound", "contained"}:
                    _recorded_scope_processes(
                        proc_root=proc_root,
                        systemd_unit=cast(str, legacy_attempt["systemd_unit"]),
                        systemd_cgroup_path=cast(
                            str,
                            legacy_attempt["systemd_cgroup_path"],
                        ),
                        systemd_invocation_id=cast(
                            str,
                            legacy_attempt["systemd_invocation_id"],
                        ),
                        systemd_description=cast(
                            str,
                            legacy_attempt["systemd_description"],
                        ),
                    )
                replacement_identity_verified = True
            elif (
                existing is None
                and legacy_attempt.get("start_phase") == "contained"
                and not request.replace
            ):
                raise RelayError(
                    "legacy contained start requires --replace because v1 did not bind auth policy"
                )
            prior_attempt = _migrate_legacy_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
                replacement_identity_verified=replacement_identity_verified,
            )
        else:
            prior_attempt = _validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
            )
        exact_prior_attempt: dict[str, object] | None = None
        if (
            prior_attempt is not None
            and prior_attempt.get("start_operation_id") == request.start_operation_id
        ):
            exact_prior_attempt = _validated_resumable_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
            )
        if (
            existing is None
            and prior_attempt is not None
            and prior_attempt.get("error") is not None
        ):
            if prior_attempt.get("start_operation_id") == request.start_operation_id:
                raise RelayError("owned-session start operation already failed terminally")
            if not request.replace:
                raise RelayError("a new start operation requires --replace after terminal failure")
            if not (
                prior_attempt.get("api_release_identity_sha256") == release_identity.sha256()
                and prior_attempt.get("cluster_registry_sha256") == request.cluster_registry_sha256
                and prior_attempt.get("cluster_route_revision") == request.cluster_route_revision
                and prior_attempt.get("remote_api_port") == request.remote_api_port
            ):
                raise RelayError(
                    "failed owned-session generation identity changed before replacement"
                )
            replacement_attempt = {
                key: value
                for key, value in prior_attempt.items()
                if key not in {"schema_version", "operation", "observed_at", "error"}
            }
            replacement_attempt.update(
                {
                    "start_operation_id": request.start_operation_id,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "expected_api_release_identity_sha256": (
                        request.expected_api_release_identity.sha256()
                        if request.expected_api_release_identity is not None
                        else None
                    ),
                }
            )
            _write_session_attempt(
                transaction,
                operation="start",
                identity=replacement_attempt,
            )
        resumable_attempt = (
            exact_prior_attempt
            or _validated_resumable_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
            )
            if existing is None
            else None
        )
        recorded_generation: str | None = None
        existing_status: OwnedSessionRecoveryStatus | None = None
        if existing is not None:
            existing_status = inspect_owned_session_recovery_status(
                cluster=request.cluster,
                session_id=request.session_id,
                core_dir=settings_core_dir,
                home=home,
                proc_root=proc_root,
                effective_uid=uid,
                transaction=transaction,
            )
            if not existing_status.recovery_verified:
                detail = "; ".join(existing_status.errors) or "recovery proof was incomplete"
                raise RelayError(f"existing owned session recovery was refused: {detail}")
            recorded_generation = existing_status.session_generation_id
            if recorded_generation is None:
                raise RelayError("existing owned session has no durable generation")
            if existing_status.cleanup_receipt:
                if existing.get("cleanup_paths_pending") is True:
                    raise RelayError(
                        "owned session cleanup receipt still has pending file deletion"
                    )
                if not (
                    isinstance(existing_status.admission_status, dict)
                    and existing_status.admission_status.get("closed") is True
                ):
                    raise RelayError(
                        "owned session cleanup is complete but its authoritative generation "
                        "is still closing; retry after the teardown coordinator marks it closed"
                    )
            else:
                same_completed_operation = bool(
                    not legacy_migrated
                    and prior_attempt is not None
                    and prior_attempt.get("start_operation_id") == request.start_operation_id
                    and existing_status.start_attempt_verified
                    and existing_status.start_state == "ready"
                )
                if same_completed_operation and (request.replace or not existing_status.running):
                    raise RelayError(
                        "owned-session start operation already completed; use a fresh operation id"
                    )
                existing_release = existing_status.api_release_identity
                registry_matches = bool(
                    existing.get("cluster_registry_sha256") == request.cluster_registry_sha256
                    and existing.get("cluster_route_revision") == request.cluster_route_revision
                )
                release_matches = existing_release == release_identity
                port_matches = existing_status.remote_api_port == request.remote_api_port
                if existing_status.running and existing_status.leader_process_state != (
                    "owned_running"
                ):
                    if not request.replace:
                        raise RelayError(
                            "owned session API leader is absent while generation children remain; "
                            "use --replace"
                        )
                elif existing_status.running and not request.replace:
                    if not (registry_matches and release_matches and port_matches):
                        raise RelayError("existing owned session identity differs; use --replace")
                    if (
                        prior_attempt is None
                        or prior_attempt.get("require_token") is not request.require_token
                    ):
                        raise RelayError(
                            "existing owned session token policy is not proven; use --replace"
                        )
                    existing_owner_token = cast(str, existing["owner_token"])
                    _write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            "cluster": request.cluster,
                            "session_id": request.session_id,
                            "start_operation_id": request.start_operation_id,
                            "session_generation_id": recorded_generation,
                            "owner_token": existing_owner_token,
                            "owner_token_sha256": hashlib.sha256(
                                existing_owner_token.encode("utf-8")
                            ).hexdigest(),
                            "api_release_identity_sha256": release_identity.sha256(),
                            "expected_api_release_identity_sha256": (
                                request.expected_api_release_identity.sha256()
                                if request.expected_api_release_identity is not None
                                else None
                            ),
                            "cluster_registry_path": existing["cluster_registry_path"],
                            "cluster_registry_sha256": request.cluster_registry_sha256,
                            "cluster_route_revision": request.cluster_route_revision,
                            "remote_api_port": request.remote_api_port,
                            "replace": request.replace,
                            "require_token": request.require_token,
                            "start_phase": "contained",
                            "systemd_unit": existing["systemd_unit"],
                            "systemd_description": existing["systemd_description"],
                            "systemd_cgroup_path": existing["systemd_cgroup_path"],
                            "systemd_invocation_id": existing["systemd_invocation_id"],
                            "containment_broker_pid": existing["containment_broker_pid"],
                            "containment_broker_start_identity": existing[
                                "containment_broker_start_identity"
                            ],
                        },
                    )
                    queue.clear_owner_session_closing(
                        request.session_id,
                        session_generation_id=recorded_generation,
                    )
                    return [
                        f"session_already_running={request.session_id}",
                        f"start_operation_id={request.start_operation_id}",
                        f"api_pid={existing_status.api_pid}",
                        f"session_generation_id={recorded_generation}",
                        f"remote_api_port={request.remote_api_port}",
                    ]
                if not registry_matches:
                    raise RelayError(
                        "an owned generation cannot change cluster authority during restart"
                    )
                if existing_status.generation_process_pids:
                    if not request.replace:
                        raise RelayError("owned generation processes remain; use --replace")
                    existing_unit = existing.get("systemd_unit")
                    existing_cgroup = existing.get("systemd_cgroup_path")
                    existing_invocation = existing.get("systemd_invocation_id")
                    existing_description = existing.get("systemd_description")
                    if not all(
                        isinstance(value, str)
                        for value in (
                            existing_unit,
                            existing_cgroup,
                            existing_invocation,
                            existing_description,
                        )
                    ):
                        raise RelayError("owned generation process identity is incomplete")
                    _terminate_recorded_session_scope(
                        systemd_unit=cast(str, existing_unit),
                        systemd_cgroup_path=cast(str, existing_cgroup),
                        systemd_invocation_id=cast(str, existing_invocation),
                        systemd_description=cast(str, existing_description),
                    )
        elif resumable_attempt is not None:
            recorded_generation = cast(str, resumable_attempt["session_generation_id"])
            attempt_phase = cast(str, resumable_attempt["start_phase"])
            if attempt_phase in {"pending", "admitted"}:
                from clio_relay.process_containment import adopt_linux_systemd_scope_identity

                try:
                    adopted_scope = adopt_linux_systemd_scope_identity(
                        unit=cast(str, resumable_attempt["systemd_unit"]),
                        description=cast(str, resumable_attempt["systemd_description"]),
                    )
                except RuntimeError as exc:
                    raise RelayError(f"prior owned-session scope recovery failed: {exc}") from exc
                if adopted_scope is not None:
                    resumable_attempt.update(
                        {
                            "start_phase": "scope_bound",
                            "systemd_cgroup_path": adopted_scope["cgroup_path"],
                            "systemd_invocation_id": adopted_scope["systemd_invocation_id"],
                        }
                    )
                    _write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            key: value
                            for key, value in resumable_attempt.items()
                            if key not in {"schema_version", "operation", "observed_at", "error"}
                        },
                    )
                    attempt_phase = "scope_bound"
            if attempt_phase in {"scope_bound", "contained"}:
                if attempt_phase == "contained":
                    promoted = _promote_resumable_contained_start(
                        transaction=transaction,
                        attempt=resumable_attempt,
                        request=request,
                        release_identity=release_identity,
                        queue=queue,
                        proc_root=proc_root,
                        home=home,
                    )
                    if promoted is not None:
                        return promoted
                    if legacy_migrated and not request.replace:
                        raise RelayError(
                            "legacy contained start could not be adopted exactly; use --replace"
                        )
                _recorded_scope_processes(
                    proc_root=proc_root,
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                _terminate_recorded_session_scope(
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                resumable_attempt.update(
                    {
                        "start_phase": "admitted",
                        "systemd_cgroup_path": None,
                        "systemd_invocation_id": None,
                        "containment_broker_pid": None,
                        "containment_broker_start_identity": None,
                    }
                )
                _write_session_attempt(
                    transaction,
                    operation="start",
                    identity={
                        key: value
                        for key, value in resumable_attempt.items()
                        if key not in {"schema_version", "operation", "observed_at", "error"}
                    },
                )

        _assert_remote_port_available(request.remote_api_port)
        release_sha256 = release_identity.sha256()
        expected_release_sha256 = (
            request.expected_api_release_identity.sha256()
            if request.expected_api_release_identity is not None
            else None
        )
        if existing is None:
            if resumable_attempt is None:
                candidate_generation = uuid4().hex
                owner_token = secrets.token_hex(32)
                owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = f"clio-relay-session-{candidate_generation}.scope"
                systemd_description = (
                    f"clio-relay-owned-session:{request.session_id}:{candidate_generation}:"
                    f"{secrets.token_hex(16)}"
                )
                attempt_identity: dict[str, object] = {
                    "cluster": request.cluster,
                    "session_id": request.session_id,
                    "start_operation_id": request.start_operation_id,
                    "session_generation_id": candidate_generation,
                    "owner_token": owner_token,
                    "owner_token_sha256": owner_token_sha256,
                    "api_release_identity_sha256": release_sha256,
                    "expected_api_release_identity_sha256": expected_release_sha256,
                    "cluster_registry_path": str(registry_path),
                    "cluster_registry_sha256": request.cluster_registry_sha256,
                    "cluster_route_revision": request.cluster_route_revision,
                    "remote_api_port": request.remote_api_port,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "start_phase": "pending",
                    "systemd_unit": systemd_unit,
                    "systemd_description": systemd_description,
                    "systemd_cgroup_path": None,
                    "systemd_invocation_id": None,
                    "containment_broker_pid": None,
                    "containment_broker_start_identity": None,
                }
                _write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
            else:
                attempt_identity = {
                    key: value
                    for key, value in resumable_attempt.items()
                    if key not in {"schema_version", "operation", "observed_at", "error"}
                }
                candidate_generation = cast(str, attempt_identity["session_generation_id"])
                owner_token = cast(str, attempt_identity["owner_token"])
                owner_token_sha256 = cast(str, attempt_identity["owner_token_sha256"])
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = cast(str, attempt_identity["systemd_unit"])
                systemd_description = cast(str, attempt_identity["systemd_description"])
            admission = queue.owner_session_generation_status(
                request.session_id,
                session_generation_id=candidate_generation,
            )
            active_generation = admission.get("active_generation_id")
            if active_generation is None:
                selected_generation = queue.prepare_owner_session_start(
                    request.session_id,
                    recorded_generation_id=None,
                    candidate_generation_id=candidate_generation,
                )
            elif active_generation == candidate_generation and admission.get("closing") is False:
                selected_generation = candidate_generation
            else:
                raise RelayError("core selected a different unrecorded owned-session generation")
            if selected_generation != candidate_generation:
                raise RelayError("core selected an unrecorded owned-session generation")
            if attempt_identity["start_phase"] != "contained":
                attempt_identity["start_phase"] = "admitted"
                _write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
        else:
            candidate_generation = uuid4().hex
            selected_generation = queue.prepare_owner_session_start(
                request.session_id,
                recorded_generation_id=recorded_generation,
                candidate_generation_id=candidate_generation,
            )
            registry_name = f"cluster-registry-{selected_generation}.json"
            registry_path = transaction.path / registry_name
            owner_token = secrets.token_hex(32)
            owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
            systemd_unit = f"clio-relay-session-{selected_generation}.scope"
            systemd_description = (
                f"clio-relay-owned-session:{request.session_id}:{selected_generation}:"
                f"{secrets.token_hex(16)}"
            )
            attempt_identity = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "start_operation_id": request.start_operation_id,
                "session_generation_id": selected_generation,
                "owner_token": owner_token,
                "owner_token_sha256": owner_token_sha256,
                "api_release_identity_sha256": release_sha256,
                "expected_api_release_identity_sha256": expected_release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "remote_api_port": request.remote_api_port,
                "replace": request.replace,
                "require_token": request.require_token,
                "start_phase": "admitted",
                "systemd_unit": systemd_unit,
                "systemd_description": systemd_description,
                "systemd_cgroup_path": None,
                "systemd_invocation_id": None,
                "containment_broker_pid": None,
                "containment_broker_start_identity": None,
            }
            _write_session_attempt(
                transaction,
                operation="start",
                identity=attempt_identity,
            )
        registry_name = f"cluster-registry-{selected_generation}.json"
        registry_path = transaction.path / registry_name
        existing_registry = transaction.read_bytes(
            registry_name,
            maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
            required=False,
        )
        if existing_registry is None:
            transaction.atomic_write(registry_name, registry_payload)
        elif hashlib.sha256(existing_registry).hexdigest() != request.cluster_registry_sha256:
            raise RelayError("owned generation cluster registry changed before restart")

        log_descriptor = transaction.open_output("api.log")
        process: subprocess.Popen[Any] | None = None
        metadata_committed = False
        child_environment: dict[str, str] = {}
        try:
            from clio_relay.process_containment import (
                broker_child_environment_payload,
                process_start_identity,
                spawn_owned_process,
            )

            child_environment = {
                "CLIO_RELAY_SESSION_OWNER_TOKEN": owner_token,
                "CLIO_RELAY_SESSION_GENERATION_ID": selected_generation,
                "CLIO_RELAY_API_RELEASE_IDENTITY_SHA256": release_sha256,
                "CLIO_RELAY_CLUSTER_REGISTRY": str(registry_path),
                "CLIO_RELAY_SESSION_REGISTRY_SHA256": request.cluster_registry_sha256,
                "CLIO_RELAY_SESSION_ROUTE_REVISION": request.cluster_route_revision,
                "CLIO_RELAY_OWNER_SESSION_ID": request.session_id,
                "CLIO_RELAY_OWNER_SESSION_CLUSTER": request.cluster,
                "CLIO_RELAY_REMOTE_CLUSTER": request.cluster,
            }
            if api_token is not None:
                child_environment["CLIO_RELAY_API_TOKEN"] = api_token
            environment = dict(os.environ)
            for name in child_environment:
                environment.pop(name, None)
            provider_interpreter = Path(sys.executable).absolute()
            interpreter_identity = provider_interpreter.stat()
            command = [
                str(provider_interpreter),
                "-I",
                "-c",
                "from clio_relay.cli import app; app()",
                "api",
                "start",
                "--host",
                "127.0.0.1",
                "--port",
                str(request.remote_api_port),
            ]
            if request.require_token:
                command.append("--require-token")
            receipt_name = f"api-startup-{selected_generation}.json"
            receipt_path = transaction.path / receipt_name
            containment_identity: dict[str, object] = {}

            def persist_containment(
                broker_pid: int,
                containment: dict[str, object],
            ) -> None:
                if not (
                    containment.get("mode") == "linux_systemd_scope"
                    and containment.get("enforceable") is True
                    and containment.get("systemd_unit") == systemd_unit
                    and containment.get("systemd_description") == systemd_description
                    and isinstance(containment.get("cgroup_path"), str)
                    and isinstance(containment.get("systemd_invocation_id"), str)
                ):
                    raise RelayError("owned API containment identity is incomplete")
                broker_start = process_start_identity(broker_pid)
                if broker_start is None:
                    raise RelayError("owned API containment broker identity is unavailable")
                containment_identity.update(containment)
                attempt_identity.update(
                    {
                        "start_phase": "contained",
                        "systemd_cgroup_path": containment["cgroup_path"],
                        "systemd_invocation_id": containment["systemd_invocation_id"],
                        "containment_broker_pid": broker_pid,
                        "containment_broker_start_identity": broker_start,
                    }
                )
                _write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )

            def release_child_environment(
                _broker_pid: int,
                containment: dict[str, object],
            ) -> str:
                gated_environment = dict(child_environment)
                gated_environment.update(
                    {
                        _API_STARTUP_RECEIPT_ENV: str(receipt_path),
                        _SYSTEMD_UNIT_ENV: cast(str, containment["systemd_unit"]),
                        _SYSTEMD_CGROUP_ENV: cast(str, containment["cgroup_path"]),
                        _SYSTEMD_INVOCATION_ENV: cast(
                            str,
                            containment["systemd_invocation_id"],
                        ),
                        _SYSTEMD_DESCRIPTION_ENV: cast(
                            str,
                            containment["systemd_description"],
                        ),
                    }
                )
                return broker_child_environment_payload(gated_environment)

            process = cast(
                subprocess.Popen[Any],
                spawn_owned_process(
                    command,
                    stdout=log_descriptor,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    close_fds=True,
                    require_enforceable=True,
                    linux_systemd_unit_base=systemd_unit.removesuffix(".scope"),
                    linux_systemd_description=systemd_description,
                    on_ready=persist_containment,
                    credential_payload_factory=release_child_environment,
                ),
            )
            final_interpreter_identity = provider_interpreter.stat()
            if (final_interpreter_identity.st_dev, final_interpreter_identity.st_ino) != (
                interpreter_identity.st_dev,
                interpreter_identity.st_ino,
            ):
                raise RelayError("verified provider interpreter changed during API spawn")
            expected_receipt = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "session_generation_id": selected_generation,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
            }
            process_identity = _wait_for_api_startup_receipt(
                transaction=transaction,
                process=process,
                receipt_name=receipt_name,
                owner_token=owner_token,
                expected=expected_receipt,
                proc_root=proc_root,
            )
            ready_seconds = _wait_for_api_ready(
                process=cast(subprocess.Popen[bytes], process),
                port=request.remote_api_port,
                require_token=request.require_token,
            )
            receipt_payload = transaction.read_bytes(
                receipt_name,
                maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
            )
            if receipt_payload is None:  # pragma: no cover - required read
                raise RelayError("owned API startup receipt disappeared before metadata commit")
            metadata = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "remote_api_port": request.remote_api_port,
                "api_pid": process_identity.pid,
                "api_pgid": process_identity.process_group_id,
                "owner_token": owner_token,
                "session_generation_id": selected_generation,
                "api_release_identity": release_identity.model_dump(mode="json"),
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "cluster_authority_verified": True,
                "process_start_ticks": process_identity.start_ticks,
                "containment_mode": "linux_systemd_scope",
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
                "containment_broker_pid": process.pid,
                "containment_broker_start_identity": attempt_identity[
                    "containment_broker_start_identity"
                ],
                "api_startup_receipt_path": str(receipt_path),
                "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
                "started_at": datetime.now(UTC).isoformat(),
                "owner": "clio-relay",
            }
            transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
            transaction.atomic_write(
                "metadata.json",
                json.dumps(metadata, indent=2).encode("utf-8"),
            )
            metadata_committed = True
            queue.clear_owner_session_closing(
                request.session_id,
                session_generation_id=selected_generation,
            )
            return [
                f"remote_api_ready_seconds={ready_seconds:.3f}",
                f"session_started={request.session_id}",
                f"start_operation_id={request.start_operation_id}",
                f"api_pid={process_identity.pid}",
                f"session_generation_id={selected_generation}",
                f"remote_api_port={request.remote_api_port}",
                f"metadata={transaction.path / 'metadata.json'}",
            ]
        except BaseException as exc:
            if not metadata_committed:
                startup_detail = _owned_api_startup_log_detail(
                    transaction,
                    secret_values=child_environment.values(),
                )
                try:
                    if attempt_identity.get("start_phase") == "contained":
                        _terminate_recorded_session_scope(
                            systemd_unit=cast(str, attempt_identity["systemd_unit"]),
                            systemd_cgroup_path=cast(
                                str,
                                attempt_identity["systemd_cgroup_path"],
                            ),
                            systemd_invocation_id=cast(
                                str,
                                attempt_identity["systemd_invocation_id"],
                            ),
                            systemd_description=cast(str, attempt_identity["systemd_description"]),
                        )
                        attempt_identity.update(
                            {
                                "start_phase": "admitted",
                                "systemd_cgroup_path": None,
                                "systemd_invocation_id": None,
                                "containment_broker_pid": None,
                                "containment_broker_start_identity": None,
                            }
                        )
                except (RelayError, RuntimeError) as cleanup_error:
                    cleanup_detail = f"{exc}; cleanup failed: {cleanup_error}"
                else:
                    cleanup_detail = str(exc)
                if startup_detail:
                    cleanup_detail = f"{cleanup_detail}; api_log={startup_detail}"
                with suppress(RelayError):
                    _write_session_attempt(
                        transaction,
                        operation="start",
                        identity=attempt_identity,
                        error=cleanup_detail,
                    )
                if startup_detail and isinstance(exc, RelayError):
                    raise RelayError(cleanup_detail) from exc
            raise
        finally:
            os.close(log_descriptor)


def _capture_cleanup_target(
    transaction: _OwnedSessionTransaction,
    *,
    name: str,
    maximum_bytes: int | None,
) -> OwnedSessionCleanupTarget:
    """Capture an exact cleanup target identity through the pinned directory."""
    linked = transaction.stat_regular(name, required=False)
    if linked is None:
        return OwnedSessionCleanupTarget(name=name, present=False)
    payload = (
        transaction.read_bytes(name, maximum_bytes=maximum_bytes)
        if maximum_bytes is not None
        else None
    )
    final = transaction.stat_regular(name)
    if final is None:  # pragma: no cover - required stat
        raise RelayError(f"owned cleanup target disappeared: {name}")
    if (linked.st_dev, linked.st_ino, linked.st_size) != (
        final.st_dev,
        final.st_ino,
        final.st_size,
    ):
        raise RelayError(f"owned cleanup target changed while it was captured: {name}")
    return OwnedSessionCleanupTarget(
        name=name,
        present=True,
        device=linked.st_dev,
        inode=linked.st_ino,
        size=linked.st_size,
        sha256=hashlib.sha256(payload).hexdigest() if payload is not None else None,
        identity_mode="content_sha256" if payload is not None else "inode",
    )


def _validate_cleanup_targets(
    raw_targets: object,
    *,
    generation_id: str,
) -> list[OwnedSessionCleanupTarget]:
    """Validate an exact, duplicate-free cleanup target identity collection."""
    if not isinstance(raw_targets, list):
        raise RelayError("owned session cleanup receipt targets are unavailable")
    try:
        targets = [
            OwnedSessionCleanupTarget.model_validate(target)
            for target in cast(list[object], raw_targets)
        ]
    except ValueError as exc:
        raise RelayError(f"owned session cleanup receipt target is invalid: {exc}") from exc
    expected_names = sorted(
        (
            "api.log",
            "api.pid",
            f"api-startup-{generation_id}.json",
            f"cluster-registry-{generation_id}.json",
        )
    )
    if [target.name for target in targets] != expected_names:
        raise RelayError("owned session cleanup receipt target names are invalid")
    if not all(target.identity_is_complete() for target in targets):
        raise RelayError("owned session cleanup receipt target identity is incomplete")
    return targets


def _delete_cleanup_targets(
    transaction: _OwnedSessionTransaction,
    targets: list[OwnedSessionCleanupTarget],
) -> None:
    """Delete only receipt-authorized inodes, accepting already-absent retry targets."""
    for target in targets:
        current = transaction.stat_regular(target.name, required=False)
        if not target.present:
            if current is not None:
                raise RelayError(f"an absent cleanup target appeared during retry: {target.name}")
            continue
        if current is None:
            continue
        if target.device is None or target.inode is None or target.size is None:
            raise RelayError(f"cleanup target identity is incomplete: {target.name}")
        transaction.unlink_verified(
            target.name,
            expected_device=target.device,
            expected_inode=target.inode,
            expected_size=target.size,
            expected_sha256=target.sha256,
            maximum_bytes=(
                _MAX_OWNED_SESSION_DOCUMENT_BYTES
                if target.identity_mode == "content_sha256"
                else None
            ),
        )


def _cleanup_intent_matches_request(
    intent: dict[str, object],
    request: OwnedSessionTeardownRequest,
) -> bool:
    """Return whether a durable intent is the request's exact immutable policy."""
    return bool(
        intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
        and intent.get("operation_id") == request.expected_cleanup_operation_id
        and intent.get("owner_session_id") == request.session_id
        and intent.get("session_generation_id") == request.expected_session_generation_id
        and intent.get("stop_worker") is request.stop_worker
        and intent.get("cancel_jobs") is request.cancel_jobs
        and intent.get("cancel_scheduler_jobs") is request.cancel_scheduler_jobs
    )


def _stop_owned_worker_service(*, cluster: str) -> CleanupResource:
    """Stop only a user service whose unit metadata proves relay ownership."""
    service = f"clio-relay-worker-{cluster}.service"
    try:
        ownership = _run_bounded_command(
            [
                "systemctl",
                "--user",
                "show",
                service,
                "--property=LoadState",
                "--property=FragmentPath",
                "--property=ExecStart",
            ],
            timeout_seconds=20.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
        ownership_text = ownership.stdout.decode("utf-8", errors="replace")
        missing = "LoadState=not-found" in ownership_text
        owned = bool(
            ownership.returncode == 0
            and not missing
            and "clio-relay" in ownership_text
            and "endpoint start" in ownership_text
        )
        stopped: _BoundedCommandResult | None = None
        if owned:
            stopped = _run_bounded_command(
                ["systemctl", "--user", "stop", service],
                timeout_seconds=20.0,
                stdout_limit=64 * 1024,
                stderr_limit=64 * 1024,
            )
        active = _run_bounded_command(
            ["systemctl", "--user", "is-active", service],
            timeout_seconds=20.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
    except (OSError, RelayError) as exc:
        return CleanupResource(
            kind="worker_service",
            resource_id=service,
            location=cluster,
            action="stop",
            ownership_verified=False,
            outcome="failed",
            residual=True,
            detail=str(exc),
        )
    state = active.stdout.decode("utf-8", errors="replace").strip().lower() or "unknown"
    observed_state = "not-found" if missing else state
    verified = bool(
        missing
        or (owned and stopped is not None and stopped.returncode == 0 and state == "inactive")
    )
    if missing:
        outcome: Literal["stopped", "missing", "refused", "failed"] = "missing"
        detail = "worker service is not installed"
    elif not owned:
        outcome = "refused"
        detail = "worker service ownership proof failed; service was not stopped"
    elif verified:
        outcome = "stopped"
        detail = None
    else:
        outcome = "failed"
        detail = (
            stopped.stderr.decode("utf-8", errors="replace").strip()
            if stopped is not None
            else "worker service stop was not attempted"
        )
    return CleanupResource(
        kind="worker_service",
        resource_id=service,
        location=cluster,
        action="stop",
        ownership_verified=owned or missing,
        outcome=outcome,
        verified_after_operation=verified,
        observed_state=observed_state,
        residual=not verified,
        detail=detail,
    )


def _complete_cleanup_receipt_retry(
    *,
    transaction: _OwnedSessionTransaction,
    document: dict[str, object],
    request: OwnedSessionTeardownRequest,
) -> SessionLifecycleReport:
    """Complete only deletions authorized by an exact sanitized receipt."""
    if document.get("cleanup_operation_id") != request.expected_cleanup_operation_id:
        raise RelayError("cleanup receipt operation does not match the teardown request")
    expected_policy = {
        "stop_worker": request.stop_worker,
        "cancel_jobs": request.cancel_jobs,
        "cancel_scheduler_jobs": request.cancel_scheduler_jobs,
    }
    if document.get("cleanup_policy") != expected_policy:
        raise RelayError("cleanup receipt policy does not match the teardown request")
    targets = _validate_cleanup_targets(
        document.get("cleanup_targets"),
        generation_id=request.expected_session_generation_id,
    )
    report = SessionLifecycleReport.model_validate(document.get("report"))
    if document.get("cleanup_paths_pending") is True:
        _delete_cleanup_targets(transaction, targets)
        for target in targets:
            if transaction.stat_regular(target.name, required=False) is not None:
                raise RelayError(f"owned session cleanup target remained: {target.name}")
        completed = dict(document)
        completed["cleanup_paths_pending"] = False
        completed["cluster_registry_removed"] = True
        transaction.atomic_write(
            "metadata.json",
            json.dumps(completed, indent=2).encode("utf-8"),
        )
    return report


def execute_owned_session_teardown(
    request: OwnedSessionTeardownRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> SessionLifecycleReport:
    """Execute exact cluster-local teardown with fail-closed durable evidence."""
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(session_id=request.session_id, remote_api_port=1)
    if request.cancel_scheduler_jobs and not request.cancel_jobs:
        raise RelayError("cancel_scheduler_jobs requires cancel_jobs")
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    queue = ClioCoreQueue(settings_core_dir)
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session teardown cannot verify the effective user")
    uid = get_effective_uid()
    expected_policy = {
        "stop_worker": request.stop_worker,
        "cancel_jobs": request.cancel_jobs,
        "cancel_scheduler_jobs": request.cancel_scheduler_jobs,
    }

    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        original_metadata = transaction.read_bytes(
            "metadata.json",
            maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
        )
        if original_metadata is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if (
            not status.recovery_verified
            or status.session_generation_id != request.expected_session_generation_id
        ):
            detail = "; ".join(status.errors) or "generation identity did not match"
            raise RelayError(f"owned session teardown recovery was refused: {detail}")

        intent = queue.set_owner_session_closing(
            request.session_id,
            session_generation_id=request.expected_session_generation_id,
            operation_id=request.expected_cleanup_operation_id,
            stop_worker=request.stop_worker,
            cancel_jobs=request.cancel_jobs,
            cancel_scheduler_jobs=request.cancel_scheduler_jobs,
        )
        if not _cleanup_intent_matches_request(intent, request):
            raise RelayError("durable cleanup intent does not match the teardown request")
        if status.cleanup_receipt:
            return _complete_cleanup_receipt_retry(
                transaction=transaction,
                document=document,
                request=request,
            )

        owner_token = document.get("owner_token")
        api_pid = document.get("api_pid")
        api_pgid = document.get("api_pgid")
        remote_api_port = document.get("remote_api_port")
        process_start = document.get("process_start_ticks")
        release_sha256 = document.get("api_release_identity_sha256")
        registry_path = document.get("cluster_registry_path")
        registry_sha256 = document.get("cluster_registry_sha256")
        route_revision = document.get("cluster_route_revision")
        systemd_unit = document.get("systemd_unit")
        systemd_cgroup_path = document.get("systemd_cgroup_path")
        systemd_invocation_id = document.get("systemd_invocation_id")
        systemd_description = document.get("systemd_description")
        containment_broker_pid = document.get("containment_broker_pid")
        containment_broker_start = document.get("containment_broker_start_identity")
        startup_receipt_path = document.get("api_startup_receipt_path")
        started_at_raw = document.get("started_at")
        if not (
            isinstance(owner_token, str)
            and isinstance(api_pid, int)
            and not isinstance(api_pid, bool)
            and isinstance(api_pgid, int)
            and not isinstance(api_pgid, bool)
            and isinstance(remote_api_port, int)
            and not isinstance(remote_api_port, bool)
            and isinstance(process_start, str)
            and isinstance(release_sha256, str)
            and isinstance(registry_path, str)
            and isinstance(registry_sha256, str)
            and isinstance(route_revision, str)
            and isinstance(systemd_unit, str)
            and isinstance(systemd_cgroup_path, str)
            and isinstance(systemd_invocation_id, str)
            and isinstance(systemd_description, str)
            and isinstance(containment_broker_pid, int)
            and not isinstance(containment_broker_pid, bool)
            and isinstance(containment_broker_start, str)
            and isinstance(startup_receipt_path, str)
            and isinstance(started_at_raw, str)
        ):
            raise RelayError("owned session metadata became incomplete before teardown")
        try:
            started_at = datetime.fromisoformat(started_at_raw)
        except ValueError as exc:  # pragma: no cover - recovery validated
            raise RelayError("owned session start timestamp is invalid") from exc
        owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
        attempt_identity: dict[str, object] = {
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": request.expected_session_generation_id,
            "cleanup_operation_id": request.expected_cleanup_operation_id,
            "cleanup_policy": expected_policy,
            "owner_token_sha256": owner_token_sha256,
            "api_release_identity_sha256": release_sha256,
            "cluster_registry_path": registry_path,
            "cluster_registry_sha256": registry_sha256,
            "cluster_route_revision": route_revision,
            "systemd_unit": systemd_unit,
            "systemd_cgroup_path": systemd_cgroup_path,
            "systemd_invocation_id": systemd_invocation_id,
            "systemd_description": systemd_description,
        }
        receipt_committed = False
        try:
            processes = _recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            prior_running = bool(processes)
            prior_observed_at = datetime.now(UTC)
            targeted_pids = [process.pid for process in processes]
            _terminate_recorded_session_scope(
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            final_processes = _recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            if final_processes:
                raise RelayError("owned generation process absence was not verified")
            api_resource = CleanupResource(
                kind="remote_relay_api",
                resource_id=str(api_pid),
                location=request.cluster,
                action="stop",
                ownership_verified=True,
                outcome="stopped" if targeted_pids else "missing",
                verified_after_operation=True,
                observed_state="absent",
                residual=False,
                detail=(
                    "the exact owned-generation systemd cgroup was stopped"
                    if targeted_pids
                    else "no exact owned-generation process remained"
                ),
                metadata={"targeted_process_pids": targeted_pids},
            )
            resources = [api_resource]
            if request.stop_worker:
                worker_resource = _stop_owned_worker_service(cluster=request.cluster)
                resources.append(worker_resource)
                if worker_resource.residual:
                    raise RelayError(
                        worker_resource.detail or "owned worker service cleanup failed"
                    )

            generation_id = request.expected_session_generation_id
            target_names = sorted(
                (
                    "api.log",
                    "api.pid",
                    Path(startup_receipt_path).name,
                    f"cluster-registry-{generation_id}.json",
                )
            )
            targets = [
                _capture_cleanup_target(
                    transaction,
                    name=name,
                    maximum_bytes=(
                        None
                        if name == "api.log"
                        else _MAX_API_STARTUP_RECEIPT_BYTES
                        if name.startswith("api-startup-")
                        else _MAX_OWNED_SESSION_DOCUMENT_BYTES
                    ),
                )
                for name in target_names
            ]
            registry_target = next(
                target for target in targets if target.name.startswith("cluster-registry-")
            )
            if not registry_target.present or registry_target.sha256 != registry_sha256:
                raise RelayError("owned session registry cleanup identity changed")
            pid_target = next(target for target in targets if target.name == "api.pid")
            if pid_target.present:
                pid_payload = transaction.read_bytes(
                    "api.pid",
                    maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
                )
                if pid_payload is None or pid_payload.strip() != str(api_pid).encode("ascii"):
                    raise RelayError("owned session PID file content is not authoritative")

            resources.append(
                CleanupResource(
                    kind="remote_session_files",
                    resource_id=f"{request.session_id}:{generation_id}",
                    location=request.cluster,
                    action="close",
                    ownership_verified=True,
                    outcome="closed",
                    verified_after_operation=True,
                    residual=False,
                    metadata={
                        "cleanup_paths": target_names,
                        "metadata_sanitized": True,
                        "transition_lock_retained": True,
                        "target_identities": [target.model_dump(mode="json") for target in targets],
                    },
                )
            )
            report = SessionLifecycleReport(
                cluster=request.cluster,
                session_id=request.session_id,
                session_generation_id=generation_id,
                mode="teardown",
                cleanup_operation_id=request.expected_cleanup_operation_id,
                cleanup_policy=expected_policy,
                relay_cancel_requested=request.cancel_jobs,
                scheduler_cancel_requested=request.cancel_scheduler_jobs,
                prior_session_status=RemoteSessionStateEvidence(
                    api_pid=api_pid,
                    session_generation_id=generation_id,
                    process_start_marker=process_start,
                    running=prior_running,
                    ownership_verified=True,
                    observed_at=prior_observed_at,
                    started_at=started_at,
                ),
                post_session_status=RemoteSessionStateEvidence(
                    api_pid=api_pid,
                    session_generation_id=generation_id,
                    process_start_marker=process_start,
                    running=False,
                    ownership_verified=True,
                    observed_at=datetime.now(UTC),
                    started_at=started_at,
                ),
                resources=resources,
            )
            receipt = {
                "schema_version": "clio-relay.owner-session-cleanup-receipt.v1",
                "owner": "clio-relay",
                "cluster": request.cluster,
                "session_id": request.session_id,
                "session_generation_id": generation_id,
                "api_pid": api_pid,
                "api_pgid": api_pgid,
                "remote_api_port": remote_api_port,
                "process_start_ticks": process_start,
                "owner_token_sha256": owner_token_sha256,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": registry_path,
                "cluster_registry_sha256": registry_sha256,
                "cluster_route_revision": route_revision,
                "containment_mode": "linux_systemd_scope",
                "systemd_unit": systemd_unit,
                "systemd_cgroup_path": systemd_cgroup_path,
                "systemd_invocation_id": systemd_invocation_id,
                "systemd_description": systemd_description,
                "containment_broker_pid": containment_broker_pid,
                "containment_broker_start_identity": containment_broker_start,
                "metadata_sha256": hashlib.sha256(original_metadata).hexdigest(),
                "cleanup_operation_id": request.expected_cleanup_operation_id,
                "cleanup_policy": expected_policy,
                "cleanup_paths": target_names,
                "cleanup_targets": [target.model_dump(mode="json") for target in targets],
                "cleanup_paths_pending": True,
                "cluster_registry_verified": True,
                "cluster_registry_removed": False,
                "completed_at": datetime.now(UTC).isoformat(),
                "report": report.model_dump(mode="json"),
                "coordinator_report_ref": None,
            }
            transaction.atomic_write(
                "metadata.json",
                json.dumps(receipt, indent=2).encode("utf-8"),
            )
            receipt_committed = True
            _delete_cleanup_targets(transaction, targets)
            for target in targets:
                if transaction.stat_regular(target.name, required=False) is not None:
                    raise RelayError(f"owned session cleanup target remained: {target.name}")
            receipt["cleanup_paths_pending"] = False
            receipt["cluster_registry_removed"] = True
            transaction.atomic_write(
                "metadata.json",
                json.dumps(receipt, indent=2).encode("utf-8"),
            )
            return report
        except BaseException as exc:
            if not receipt_committed:
                with suppress(RelayError):
                    _write_session_attempt(
                        transaction,
                        operation="teardown",
                        identity=attempt_identity,
                        error=str(exc),
                    )
            raise


def execute_owned_session_cleanup_finalize(
    request: OwnedSessionCleanupFinalizeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> OwnedSessionRecoveryStatus:
    """Immutably bind a coordinator-verified report to a completed receipt."""
    from clio_relay.config import RelaySettings

    _validate_session(session_id=request.session_id, remote_api_port=1)
    expected_policy_keys = {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
    if set(request.expected_cleanup_policy) != expected_policy_keys:
        raise RelayError("coordinator cleanup policy has unexpected fields")
    if (
        request.expected_cleanup_policy["cancel_scheduler_jobs"]
        and not request.expected_cleanup_policy["cancel_jobs"]
    ):
        raise RelayError("cancel_scheduler_jobs requires cancel_jobs")
    report = request.coordinator_report
    report_reference, report_payload = _coordinator_report_reference(report)
    if report_reference.sha256 != request.coordinator_report_sha256:
        raise RelayError("coordinator cleanup report digest does not match its request")
    if not (
        report.cluster == request.cluster
        and report.session_id == request.session_id
        and report.session_generation_id == request.expected_session_generation_id
        and report.mode == "teardown"
        and report.cleanup_operation_id == request.expected_cleanup_operation_id
        and report.cleanup_policy == request.expected_cleanup_policy
        and report.relay_cancel_requested is request.expected_cleanup_policy["cancel_jobs"]
        and report.scheduler_cancel_requested
        is request.expected_cleanup_policy["cancel_scheduler_jobs"]
    ):
        raise RelayError("coordinator cleanup report identity or policy does not match")

    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned cleanup finalization cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.cleanup_receipt
            and status.cleanup_paths_pending is False
            and status.session_generation_id == request.expected_session_generation_id
        ):
            detail = "; ".join(status.errors) or "completed receipt was not exact"
            raise RelayError(f"coordinator cleanup finalization was refused: {detail}")
        if document.get("cleanup_operation_id") != request.expected_cleanup_operation_id:
            raise RelayError("cleanup receipt operation does not match coordinator report")
        if document.get("cleanup_policy") != request.expected_cleanup_policy:
            raise RelayError("cleanup receipt policy does not match coordinator report")

        remote_report = SessionLifecycleReport.model_validate(document.get("report"))
        if not _coordinator_report_extends_remote_report(report, remote_report):
            raise RelayError("coordinator cleanup report does not extend the exact remote report")

        existing_reference_raw = document.get("coordinator_report_ref")
        existing_report = document.get("coordinator_report")
        existing_sha256 = document.get("coordinator_report_sha256")
        if existing_reference_raw is not None:
            try:
                existing_reference = OwnedSessionCleanupReportReference.model_validate(
                    existing_reference_raw
                )
            except ValueError as exc:
                raise RelayError(
                    "existing coordinator cleanup report reference is invalid"
                ) from exc
            if not (
                existing_reference == report_reference
                and status.coordinator_report_bound
                and status.coordinator_report_ref == report_reference
                and status.coordinator_report_sha256 == report_reference.sha256
                and status.coordinator_report is None
            ):
                raise RelayError(
                    "coordinator cleanup report is immutable and cannot be replaced or downgraded"
                )
            return status

        legacy_bound = existing_report is not None or existing_sha256 is not None
        if legacy_bound and not (
            existing_sha256 == request.coordinator_report_sha256
            and existing_report == report.model_dump(mode="json")
            and status.coordinator_report_bound
            and status.coordinator_report_ref is None
        ):
            raise RelayError(
                "coordinator cleanup report is immutable and cannot be replaced or downgraded"
            )

        _prune_unreferenced_cleanup_report_sidecars(
            transaction,
            preserve_names={
                report_reference.name,
                f".{report_reference.name}.pending",
            },
        )
        transaction.atomic_write_immutable(
            report_reference.name,
            report_payload,
            maximum_bytes=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
        finalized = dict(document)
        finalized.pop("coordinator_report", None)
        finalized.pop("coordinator_report_sha256", None)
        finalized["coordinator_report_ref"] = report_reference.model_dump(mode="json")
        transaction.atomic_write(
            "metadata.json",
            json.dumps(finalized, indent=2).encode("utf-8"),
        )
        reread = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            reread.recovery_verified
            and reread.coordinator_report_bound
            and reread.coordinator_report_ref == report_reference
            and reread.coordinator_report_sha256 == report_reference.sha256
            and reread.coordinator_report is None
        ):
            raise RelayError("coordinator cleanup report was not durably re-read after commit")
        return reread


def execute_owned_session_cleanup_report_read(
    request: OwnedSessionCleanupReportReadRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> SessionLifecycleReport:
    """Read one exact finalized report only through its pinned receipt reference."""
    from clio_relay.config import RelaySettings

    _validate_session(session_id=request.session_id, remote_api_port=1)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned cleanup report read cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.cleanup_receipt
            and status.cleanup_paths_pending is False
            and status.session_generation_id == request.expected_session_generation_id
            and status.coordinator_report_bound
            and status.coordinator_report is None
            and status.coordinator_report_ref == request.coordinator_report_ref
            and status.coordinator_report_sha256 == request.coordinator_report_ref.sha256
        ):
            detail = "; ".join(status.errors) or "finalized report reference was not exact"
            raise RelayError(f"owned cleanup report read was refused: {detail}")
        cleanup_operation_id = document.get("cleanup_operation_id")
        if not isinstance(cleanup_operation_id, str):
            raise RelayError("owned cleanup report receipt omitted its operation id")
        return _read_coordinator_report_sidecar(
            transaction,
            request.coordinator_report_ref,
            expected_session_generation_id=request.expected_session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
        )


def plan_remote_session_start(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    replace: bool,
    require_token: bool,
    start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
    expected_api_release_identity_sha256: str | None = None,
) -> OwnedSessionStartPlan:
    """Create a read-only exact selector plan before any remote mutation."""
    _validate_session(session_id=session_id, remote_api_port=remote_api_port)
    _, _, route_revision = _session_cluster_registry_authority(
        cluster=cluster,
        definition=definition,
    )
    if (
        expected_cluster_route_revision is not None
        and expected_cluster_route_revision != route_revision
    ):
        raise RelayError("owned-session start plan route revision changed")
    operation_id = start_operation_id or f"start_{uuid4().hex}"
    _validate_durable_session_identity(operation_id, field="start_operation_id")
    status_selector = OwnedSessionStartStatusSelector(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=require_token,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
    )
    retry_selector = OwnedSessionStartRetrySelector(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=require_token,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
    )
    return OwnedSessionStartPlan(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        status_selector=status_selector,
        retry_selector=retry_selector,
    )


def start_remote_session(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None = None,
    replace: bool = False,
    start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> list[str]:
    """Start a cluster-side relay API owned by a session id."""
    plan = plan_remote_session_start(
        cluster=cluster,
        definition=definition,
        session_id=session_id,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=api_token is not None,
        start_operation_id=start_operation_id,
        expected_cluster_route_revision=expected_cluster_route_revision,
        expected_api_release_identity_sha256=(
            expected_api_release_identity.sha256()
            if expected_api_release_identity is not None
            else None
        ),
    )
    result = _ssh_script(
        definition,
        _start_script(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            start_operation_id=plan.start_operation_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            expected_api_release_identity=expected_api_release_identity,
            replace=replace,
            expected_cluster_route_revision=plan.cluster_route_revision,
        ),
    )
    return result.splitlines()


def status_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    pre_start_cleanup_probe: bool = False,
) -> dict[str, object]:
    """Return status for a previously started remote relay session.

    The pre-start cleanup probe is an internal, read-only observation that may
    report an uninitialized transition.  It must not be used as authoritative
    absence evidence by teardown or cleanup callers.
    """
    _validate_session(session_id=session_id, remote_api_port=1)
    output = _ssh_script(
        definition,
        _owned_status_script(
            definition=definition,
            cluster=definition.name,
            session_id=session_id,
            pre_start_cleanup_probe=pre_start_cleanup_probe,
        ),
    )
    return cast(dict[str, object], json.loads(output))


def status_remote_session_start(
    *,
    definition: ClusterDefinition,
    selector: OwnedSessionStartStatusSelector,
) -> OwnedSessionRecoveryStatus:
    """Return a nonblocking remote observation for one exact start operation."""
    if definition.name != selector.cluster:
        raise RelayError("owned-session start status selector changed cluster")
    try:
        output = _ssh_script(
            definition,
            _owned_start_status_script(definition=definition, selector=selector),
            timeout_seconds=_REMOTE_SESSION_START_RECOVERY_TIMEOUT_SECONDS,
        )
    except _RemoteSessionCommandDeadline as exc:
        return OwnedSessionRecoveryStatus(
            cluster=selector.cluster,
            session_id=selector.session_id,
            start_operation_id=selector.start_operation_id,
            cluster_route_revision=selector.cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)],
        )
    try:
        status = OwnedSessionRecoveryStatus.model_validate_json(output)
    except ValueError as exc:
        raise RelayError(f"owned-session start status is invalid: {exc}") from exc
    if not (
        status.cluster == selector.cluster
        and status.session_id == selector.session_id
        and status.start_operation_id == selector.start_operation_id
        and status.cluster_route_revision == selector.cluster_route_revision
    ):
        raise RelayError("owned-session start status changed its exact selector")
    return status


def _owned_session_start_result(
    *,
    plan: OwnedSessionStartPlan,
    state: Literal["ready", "starting", "ambiguous", "failed", "not_current"],
    terminal: bool,
    retryable: bool,
    transition_accepted: bool | None,
    transport_deadline_exceeded: bool,
    compatibility_lines: list[str],
    session_generation_id: str | None = None,
    running: bool = False,
    ownership_verified: bool = False,
    recovery_verified: bool = False,
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None,
    error: str | None = None,
) -> OwnedSessionStartResult:
    """Build one typed result while copying the exact immutable plan identity."""
    return OwnedSessionStartResult(
        cluster=plan.cluster,
        session_id=plan.session_id,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        session_generation_id=session_generation_id,
        remote_api_port=plan.remote_api_port,
        state=state,
        terminal=terminal,
        retryable=retryable,
        transition_accepted=transition_accepted,
        transport_deadline_exceeded=transport_deadline_exceeded,
        running=running,
        ownership_verified=ownership_verified,
        recovery_verified=recovery_verified,
        start_phase=start_phase,
        error=error,
        status_selector=plan.status_selector,
        retry_selector=plan.retry_selector,
        compatibility_lines=compatibility_lines,
    )


def _session_start_result_from_status(
    *,
    plan: OwnedSessionStartPlan,
    status: OwnedSessionRecoveryStatus,
    transport_deadline_exceeded: bool,
) -> OwnedSessionStartResult:
    """Project exact remote recovery evidence into the public start contract."""
    generation_id = status.session_generation_id
    if status.start_state == "not_current":
        detail = "; ".join(status.errors) or "owned-session start selector is no longer current"
        return _owned_session_start_result(
            plan=plan,
            state="not_current",
            terminal=True,
            retryable=False,
            transition_accepted=None,
            transport_deadline_exceeded=transport_deadline_exceeded,
            error=detail[:_MAX_SESSION_START_ERROR_CHARS],
            compatibility_lines=[
                "session_start_state=not_current",
                f"session_id={plan.session_id}",
                f"start_operation_id={plan.start_operation_id}",
                "session_generation_id=",
            ],
        )
    if status.start_attempt_verified and not (
        status.start_replace is plan.retry_selector.replace
        and status.start_require_token is plan.retry_selector.require_token
        and status.start_expected_api_release_identity_sha256
        == plan.expected_api_release_identity_sha256
        and status.remote_api_port == plan.remote_api_port
    ):
        return _owned_session_start_result(
            plan=plan,
            state="failed",
            terminal=True,
            retryable=False,
            transition_accepted=None,
            transport_deadline_exceeded=transport_deadline_exceeded,
            error="remote start journal does not match the persisted retry selector",
            compatibility_lines=[
                "session_start_state=failed",
                f"session_id={plan.session_id}",
                f"start_operation_id={plan.start_operation_id}",
                "session_generation_id=",
            ],
        )
    if (
        status.recovery_verified
        and status.ownership_verified
        and generation_id is not None
        and status.start_attempt_verified
        and status.start_state == "ready"
    ):
        return _owned_session_start_result(
            plan=plan,
            session_generation_id=generation_id,
            state="ready",
            terminal=True,
            retryable=False,
            transition_accepted=True,
            transport_deadline_exceeded=transport_deadline_exceeded,
            running=status.leader_process_state == "owned_running",
            ownership_verified=True,
            recovery_verified=True,
            start_phase=status.start_phase,
            compatibility_lines=[
                "session_start_state=ready",
                f"session_started={plan.session_id}",
                f"start_operation_id={plan.start_operation_id}",
                f"session_generation_id={generation_id}",
                f"remote_api_port={plan.remote_api_port}",
            ],
        )
    if status.start_attempt_verified and generation_id is not None:
        if status.start_state == "failed":
            detail = status.start_error or "owned-session start attempt failed"
            return _owned_session_start_result(
                plan=plan,
                session_generation_id=generation_id,
                state="failed",
                terminal=True,
                retryable=False,
                transition_accepted=True,
                transport_deadline_exceeded=transport_deadline_exceeded,
                start_phase=status.start_phase,
                error=detail,
                compatibility_lines=[
                    "session_start_state=failed",
                    f"session_id={plan.session_id}",
                    f"start_operation_id={plan.start_operation_id}",
                    f"session_generation_id={generation_id}",
                    f"error={detail}",
                ],
            )
        return _owned_session_start_result(
            plan=plan,
            session_generation_id=generation_id,
            state="starting",
            terminal=False,
            retryable=True,
            transition_accepted=True,
            transport_deadline_exceeded=transport_deadline_exceeded,
            start_phase=status.start_phase,
            compatibility_lines=[
                "session_start_state=starting",
                f"session_id={plan.session_id}",
                f"start_operation_id={plan.start_operation_id}",
                f"session_generation_id={generation_id}",
            ],
        )
    detail = "; ".join(status.errors) or "remote start transition is not yet observable"
    return _owned_session_start_result(
        plan=plan,
        state="ambiguous",
        terminal=False,
        retryable=True,
        transition_accepted=None,
        transport_deadline_exceeded=transport_deadline_exceeded,
        error=detail[:_MAX_SESSION_START_ERROR_CHARS],
        compatibility_lines=[
            "session_start_state=ambiguous",
            f"session_id={plan.session_id}",
            f"start_operation_id={plan.start_operation_id}",
            "session_generation_id=",
        ],
    )


def query_remote_session_start(
    *,
    definition: ClusterDefinition,
    plan: OwnedSessionStartPlan,
    transport_deadline_exceeded: bool = False,
) -> OwnedSessionStartResult:
    """Query one exact start once; callers choose any aggregate polling policy."""
    try:
        status = status_remote_session_start(
            definition=definition,
            selector=plan.status_selector,
        )
    except RelayError as exc:
        status = OwnedSessionRecoveryStatus(
            cluster=plan.cluster,
            session_id=plan.session_id,
            start_operation_id=plan.start_operation_id,
            cluster_route_revision=plan.cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)[:_MAX_SESSION_START_ERROR_CHARS]],
        )
    return _session_start_result_from_status(
        plan=plan,
        status=status,
        transport_deadline_exceeded=transport_deadline_exceeded,
    )


def start_remote_session_durable(
    *,
    definition: ClusterDefinition,
    plan: OwnedSessionStartPlan,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None = None,
    starter: Callable[..., list[str]] | None = None,
) -> OwnedSessionStartResult:
    """Start or recover one exact remote transition without erasing deadline ambiguity."""
    if (api_token is not None) is not plan.retry_selector.require_token:
        raise RelayError("owned-session start token policy changed after planning")
    observed_release_sha256 = (
        expected_api_release_identity.sha256()
        if expected_api_release_identity is not None
        else None
    )
    if observed_release_sha256 != plan.expected_api_release_identity_sha256:
        raise RelayError("owned-session start release identity changed after planning")
    start_callable = starter or start_remote_session
    try:
        lines = start_callable(
            cluster=plan.cluster,
            definition=definition,
            session_id=plan.session_id,
            remote_api_port=plan.remote_api_port,
            api_token=api_token,
            expected_api_release_identity=expected_api_release_identity,
            replace=plan.retry_selector.replace,
            start_operation_id=plan.start_operation_id,
            expected_cluster_route_revision=plan.cluster_route_revision,
        )
    except _RemoteSessionCommandDeadline:
        return query_remote_session_start(
            definition=definition,
            plan=plan,
            transport_deadline_exceeded=True,
        )
    except _RemoteSessionCommandRejected as exc:
        rejection = exc.rejection
        if not (
            rejection.cluster == plan.cluster
            and rejection.session_id == plan.session_id
            and rejection.start_operation_id == plan.start_operation_id
            and rejection.cluster_route_revision == plan.cluster_route_revision
        ):
            return query_remote_session_start(definition=definition, plan=plan)
        observed = query_remote_session_start(definition=definition, plan=plan)
        if observed.state != "ambiguous":
            return observed
        return observed.model_copy(update={"error": str(exc)[:_MAX_SESSION_START_ERROR_CHARS]})
    except RelayError:
        observed = query_remote_session_start(definition=definition, plan=plan)
        return observed
    values: dict[str, str] = {}
    for line in lines:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in values and values[key] != value:
            return query_remote_session_start(definition=definition, plan=plan)
        values[key] = value
    generation = values.get("session_generation_id")
    marker = values.get("session_started") or values.get("session_already_running")
    try:
        validated_generation = validate_durable_record_id(generation)
    except (TypeError, ValueError):
        return query_remote_session_start(definition=definition, plan=plan)
    if (
        marker != plan.session_id
        or values.get("start_operation_id") != plan.start_operation_id
        or values.get("remote_api_port") != str(plan.remote_api_port)
    ):
        return query_remote_session_start(definition=definition, plan=plan)
    return OwnedSessionStartResult(
        cluster=plan.cluster,
        session_id=plan.session_id,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        session_generation_id=validated_generation,
        remote_api_port=plan.remote_api_port,
        state="ready",
        terminal=True,
        retryable=False,
        transition_accepted=True,
        running=True,
        ownership_verified=True,
        recovery_verified=True,
        start_phase="contained",
        status_selector=plan.status_selector,
        retry_selector=plan.retry_selector,
        compatibility_lines=["session_start_state=ready", *lines],
    )


def challenge_remote_session_identity(
    *,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: DurableRecordId,
    nonce: str,
) -> dict[str, object]:
    """Return an SSH-authenticated HMAC challenge for one live session API."""
    _validate_session(session_id=session_id, remote_api_port=1)
    validate_durable_record_id(session_generation_id)
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("session identity nonce must be a lowercase 256-bit hexadecimal value")
    output = _ssh_script(
        definition,
        _owned_identity_challenge_script(
            definition=definition,
            cluster=definition.name,
            session_id=session_id,
            session_generation_id=session_generation_id,
            nonce=nonce,
        ),
    )
    return cast(dict[str, object], json.loads(output))


def teardown_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str | None = None,
    stop_worker: bool = False,
    cancel_jobs: bool = False,
    cancel_scheduler_jobs: bool = False,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Stop processes owned by a remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    _validate_durable_session_identity(
        expected_session_generation_id,
        field="expected_session_generation_id",
    )
    cleanup_operation_id = expected_cleanup_operation_id or f"cleanup_{uuid4().hex}"
    _validate_durable_session_identity(
        cleanup_operation_id,
        field="expected_cleanup_operation_id",
    )
    output = _ssh_script(
        definition,
        _owned_teardown_script(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=expected_session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        ),
    )
    report = SessionLifecycleReport.model_validate_json(output)
    if report.cleanup_operation_id != cleanup_operation_id:
        raise RelayError(
            "remote teardown cleanup operation does not match the durable owner-session intent"
        )
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    if report.cleanup_policy != expected_policy:
        raise RelayError(
            "remote teardown cleanup policy does not match the durable owner-session intent"
        )
    if (
        report.relay_cancel_requested is not cancel_jobs
        or report.scheduler_cancel_requested is not cancel_scheduler_jobs
    ):
        raise RelayError(
            "remote teardown cancellation evidence does not match the durable owner-session intent"
        )
    return report


def finalize_remote_session_cleanup_report(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    cleanup_policy: dict[str, bool],
    report: SessionLifecycleReport,
) -> OwnedSessionRecoveryStatus:
    """Persist and re-read one immutable coordinator-verified cleanup report."""
    request = OwnedSessionCleanupFinalizeRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=cleanup_policy,
        coordinator_report=report,
        coordinator_report_sha256=session_lifecycle_report_sha256(report),
    )
    request_payload = request.model_dump_json().encode("utf-8")
    output = _ssh_stdin_command(
        definition,
        _owned_cleanup_finalize_script(definition=definition),
        input_bytes=request_payload,
        input_limit=MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES,
        stdout_limit=_MAX_REMOTE_SESSION_STDOUT_BYTES,
    )
    status = OwnedSessionRecoveryStatus.model_validate_json(output)
    expected_reference, _ = _coordinator_report_reference(report)
    if not (
        status.recovery_verified
        and status.cleanup_receipt
        and status.cleanup_paths_pending is False
        and status.session_generation_id == session_generation_id
        and status.coordinator_report_bound
        and status.coordinator_report is None
        and status.coordinator_report_ref == expected_reference
        and status.coordinator_report_sha256 == expected_reference.sha256
    ):
        raise RelayError("remote coordinator cleanup report finalization was not exact")
    return status


def read_remote_session_cleanup_report(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    status: OwnedSessionRecoveryStatus,
) -> SessionLifecycleReport:
    """Retrieve one finalized report through its exact bounded sidecar reference."""
    reference = status.coordinator_report_ref
    generation_id = status.session_generation_id
    if not (
        status.cluster == cluster
        and status.session_id == session_id
        and status.recovery_verified
        and status.cleanup_receipt
        and status.cleanup_paths_pending is False
        and generation_id is not None
        and status.coordinator_report_bound
        and status.coordinator_report is None
        and reference is not None
        and status.coordinator_report_sha256 == reference.sha256
    ):
        raise RelayError("remote coordinator cleanup report reference is not exact")
    request = OwnedSessionCleanupReportReadRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=generation_id,
        coordinator_report_ref=reference,
    )
    output = _ssh_stdin_command(
        definition,
        _owned_cleanup_report_read_script(definition=definition),
        input_bytes=request.model_dump_json().encode("utf-8"),
        input_limit=256 * 1024,
        stdout_limit=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + 64 * 1024,
    )
    try:
        report = SessionLifecycleReport.model_validate_json(output)
    except ValueError as exc:
        raise RelayError(f"remote coordinator cleanup report is invalid: {exc}") from exc
    payload = session_lifecycle_report_bytes(report)
    if not (
        len(payload) == reference.size
        and hmac.compare_digest(hashlib.sha256(payload).hexdigest(), reference.sha256)
        and report.cluster == cluster
        and report.session_id == session_id
        and report.session_generation_id == generation_id
    ):
        raise RelayError("remote coordinator cleanup report did not match its exact reference")
    return report


def detach_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Detach the desktop while intentionally retaining the remote session."""
    status = status_remote_session(definition=definition, session_id=session_id)
    pid = status.get("api_pid")
    running = status.get("running") is True
    ownership_verified = status.get("ownership_verified") is True
    identity_verified = status.get("session_id") == session_id
    generation_id = status.get("session_generation_id")
    generation_verified = isinstance(generation_id, str) and bool(generation_id)
    retained = running and ownership_verified and identity_verified and generation_verified
    resource_id = str(pid) if isinstance(pid, int) else session_id
    if retained:
        outcome: Literal["retained", "missing", "refused"] = "retained"
        detail = "remote relay session intentionally retained for reattachment"
    elif not running:
        outcome = "missing"
        detail = "remote relay API was not running after detach"
    else:
        outcome = "refused"
        detail = "remote relay API retention could not be tied to the requested owned generation"
    return SessionLifecycleReport(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=str(generation_id) if generation_verified else None,
        mode="detach",
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id=resource_id,
                location=definition.ssh_host,
                action="retain",
                ownership_verified=ownership_verified and identity_verified,
                outcome=outcome,
                verified_after_operation=retained,
                residual=not retained,
                detail=detail,
            )
        ],
        errors=[] if retained else [detail],
    )


def _start_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    start_operation_id: str,
    remote_api_port: int,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None,
    replace: bool,
    expected_cluster_route_revision: str,
) -> str:
    cluster_registry_json, cluster_registry_sha256, route_revision = (
        _session_cluster_registry_authority(cluster=cluster, definition=definition)
    )
    if route_revision != expected_cluster_route_revision:
        raise RelayError("owned-session start route revision changed after planning")
    request = OwnedSessionStartRequest(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=start_operation_id,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=api_token is not None,
        expected_api_release_identity=expected_api_release_identity,
        cluster_registry=cast(dict[str, object], json.loads(cluster_registry_json)),
        cluster_registry_sha256=cluster_registry_sha256,
        cluster_route_revision=route_revision,
    )
    token_export = (
        f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}\n"
        if api_token is not None
        else ""
    )
    request_json = request.model_dump_json()
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"{token_export}"
        f"printf '%s' {_shell_single_quote(request_json)} | "
        "clio-relay session start-owned\n"
    )


def _session_cluster_registry_authority(
    *, cluster: str, definition: ClusterDefinition
) -> tuple[str, str, str]:
    """Return the exact registry payload and identities owned by one session API."""
    if definition.name != cluster:
        raise RelayError("session cluster does not match its cluster definition")
    registry = ClusterRegistry(clusters={cluster: definition})
    payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    encoded_payload = payload.encode("utf-8")
    if len(encoded_payload) > MAX_CLUSTER_REGISTRY_BYTES:
        raise RelayError(
            "session cluster registry exceeds the "
            f"{MAX_CLUSTER_REGISTRY_BYTES}-byte configuration limit"
        )
    return (
        payload,
        hashlib.sha256(encoded_payload).hexdigest(),
        cluster_route_revision(definition),
    )


def _owned_status_script(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    pre_start_cleanup_probe: bool = False,
) -> str:
    """Use the bounded, lock-coordinated recovery contract for public status."""
    probe_argument = " --pre-start-cleanup-probe" if pre_start_cleanup_probe else ""
    return (
        "set -euo pipefail\n"
        f"{remote_env(definition)}\n"
        f"clio-relay session recovery-status --cluster {shlex.quote(cluster)} "
        f"--session-id {shlex.quote(session_id)}{probe_argument}\n"
    )


def _owned_start_status_script(
    *,
    definition: ClusterDefinition,
    selector: OwnedSessionStartStatusSelector,
) -> str:
    """Render the nonblocking exact-operation start-status command."""
    return (
        "set -euo pipefail\n"
        f"{remote_env(definition)}\n"
        f"clio-relay session start-status-owned --cluster {shlex.quote(selector.cluster)} "
        f"--session-id {shlex.quote(selector.session_id)} "
        f"--start-operation-id {shlex.quote(selector.start_operation_id)} "
        "--cluster-route-revision "
        f"{shlex.quote(selector.cluster_route_revision)}\n"
    )


def _owned_identity_challenge_script(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
) -> str:
    request = OwnedSessionIdentityChallengeRequest(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        nonce=nonce,
    )
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"printf '%s' {_shell_single_quote(request.model_dump_json())} | "
        "clio-relay session challenge-owned\n"
    )


def _owned_cleanup_finalize_script(
    *,
    definition: ClusterDefinition,
) -> str:
    """Run the bounded coordinator-report finalizer with SSH stdin left intact."""
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        "clio-relay session finalize-cleanup-owned\n"
    )


def _owned_cleanup_report_read_script(*, definition: ClusterDefinition) -> str:
    """Run the pinned coordinator-report reader with SSH stdin left intact."""
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        "clio-relay session read-cleanup-report-owned\n"
    )


def _owned_teardown_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    cluster: str | None,
) -> str:
    if stop_worker and cluster is None:
        raise RelayError("cluster is required when stopping the worker service")
    if cluster is None:
        raise RelayError("cluster is required for owned session teardown")
    request = OwnedSessionTeardownRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=expected_session_generation_id,
        expected_cleanup_operation_id=expected_cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"printf '%s' {_shell_single_quote(request.model_dump_json())} | "
        "clio-relay session teardown-owned\n"
    )


def _validate_session(*, session_id: str, remote_api_port: int) -> None:
    if not session_id or not all(item.isalnum() or item in {"-", "_"} for item in session_id):
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if remote_api_port <= 0:
        raise RelayError("remote_api_port must be positive")


def _validate_durable_session_identity(value: str, *, field: str) -> str:
    """Validate an execution identity before any remote lifecycle I/O."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error


def _ssh_script(
    definition: ClusterDefinition,
    script: str,
    *,
    timeout_seconds: float = _REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
) -> str:
    if timeout_seconds <= 0:
        raise ValueError("remote session command timeout must be positive")
    encoded_script = script.encode("utf-8")
    if len(encoded_script) > _MAX_REMOTE_SESSION_SCRIPT_BYTES:
        raise RelayError("remote session command exceeds its byte limit")
    try:
        result = _run_bounded_command(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input_bytes=encoded_script,
            timeout_seconds=timeout_seconds,
            stdout_limit=_MAX_REMOTE_SESSION_STDOUT_BYTES,
            stderr_limit=_MAX_REMOTE_SESSION_STDERR_BYTES,
        )
    except RelayError as exc:
        if "timed out" in str(exc):
            raise _RemoteSessionCommandDeadline(
                f"remote session command timed out after {timeout_seconds:g} seconds"
            ) from exc
        raise RelayError(f"remote session command failed safely: {exc}") from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        try:
            rejection = OwnedSessionStartRejection.model_validate_json(stdout)
        except ValueError:
            raise _RemoteSessionCommandAmbiguous(
                "remote session transport ended without an exact structured response: "
                f"{detail or f'exit {result.returncode}'}"
            ) from None
        raise _RemoteSessionCommandRejected(rejection)
    return result.stdout.decode("utf-8", errors="replace")


def _ssh_stdin_command(
    definition: ClusterDefinition,
    script: str,
    *,
    input_bytes: bytes,
    input_limit: int,
    stdout_limit: int,
) -> str:
    """Run a small remote command while carrying a separately bounded stdin payload."""
    encoded_script = script.encode("utf-8")
    if len(encoded_script) > _MAX_REMOTE_SESSION_SCRIPT_BYTES:
        raise RelayError("remote session command exceeds its byte limit")
    if input_limit <= 0 or stdout_limit <= 0:
        raise ValueError("remote session input and output limits must be positive")
    if len(input_bytes) > input_limit:
        raise RelayError("remote session stdin exceeds its byte limit")
    remote_command = f"bash -lc {shlex.quote(script)}"
    try:
        result = _run_bounded_command(
            ["ssh", definition.ssh_host, remote_command],
            input_bytes=input_bytes,
            timeout_seconds=_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
            stdout_limit=stdout_limit,
            stderr_limit=_MAX_REMOTE_SESSION_STDERR_BYTES,
        )
    except RelayError as exc:
        if "timed out" in str(exc):
            raise RelayError(
                "remote session command timed out after "
                f"{_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        raise RelayError(f"remote session command failed safely: {exc}") from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        raise RelayError(f"remote session command failed: {detail}")
    return result.stdout.decode("utf-8", errors="replace")


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"
