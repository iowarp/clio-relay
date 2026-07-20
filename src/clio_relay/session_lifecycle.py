"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import secrets
import shlex
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

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
_REMOTE_API_READINESS_TIMEOUT_SECONDS = 60.0
_MAX_OWNED_SESSION_DOCUMENT_BYTES = 1024 * 1024
_MAX_OWNED_SESSION_LOG_BYTES = 16 * 1024 * 1024
_MAX_PROC_RECORD_BYTES = 1024 * 1024
_MAX_PROC_ENTRIES = 131_072
_OWNED_SESSION_LOCK_RETRY_SECONDS = 0.05
_OWNED_SESSION_PROCESS_STOP_TIMEOUT_SECONDS = 5.0
_MAX_REMOTE_SESSION_SCRIPT_BYTES = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
_MAX_REMOTE_SESSION_STDOUT_BYTES = 1024 * 1024
_MAX_REMOTE_SESSION_STDERR_BYTES = 1024 * 1024
_MAX_API_HEALTH_RESPONSE_BYTES = 64 * 1024


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
            final = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            if (final.st_dev, final.st_ino) != (opened.st_dev, opened.st_ino):
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

    def atomic_write(self, name: str, payload: bytes) -> None:
        """Atomically replace one owner-private regular file through the pinned directory."""
        _validate_owned_session_filename(name)
        if len(payload) > _MAX_OWNED_SESSION_DOCUMENT_BYTES:
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

    def unlink_regular(self, name: str, *, expected_sha256: str | None = None) -> bool:
        """Delete one exact regular file after optional content proof."""
        linked = self.stat_regular(name, required=False)
        if linked is None:
            return False
        if expected_sha256 is not None:
            payload = self.read_bytes(name, maximum_bytes=_MAX_OWNED_SESSION_LOG_BYTES)
            if payload is None or hashlib.sha256(payload).hexdigest() != expected_sha256:
                raise RelayError(f"owned session file digest changed before deletion: {name}")
        final = os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
        if (final.st_dev, final.st_ino) != (linked.st_dev, linked.st_ino):
            raise RelayError(f"owned session file changed before deletion: {name}")
        try:
            os.unlink(name, dir_fd=self.directory_fd)
            os.fsync(self.directory_fd)
        except OSError as exc:
            raise RelayError(
                f"owned session file could not be deleted safely: {name}: {exc}"
            ) from exc
        return True

    def unlink_verified(
        self,
        name: str,
        *,
        expected_device: int,
        expected_inode: int,
        expected_size: int,
        expected_sha256: str,
        maximum_bytes: int,
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
        or opened.st_uid != uid
        or stat.S_IMODE(opened.st_mode) != 0o600
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
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool = False
    require_token: bool = True
    expected_api_release_identity: SessionApiReleaseIdentity | None = None
    cluster_registry: dict[str, object]
    cluster_registry_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cluster_route_revision: str = Field(min_length=1)


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

    def identity_is_complete(self) -> bool:
        """Return whether present/absent state has the exact permitted shape."""
        identity = (self.device, self.inode, self.size, self.sha256)
        return (
            all(value is not None for value in identity)
            if self.present
            else all(value is None for value in identity)
        )


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


class OwnedSessionRecoveryStatus(BaseModel):
    """Fail-closed recovery evidence for one exact owned session generation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-recovery-status.v1"] = (
        "clio-relay.owner-session-recovery-status.v1"
    )
    cluster: str
    session_id: str
    session_generation_id: DurableRecordId | None = None
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
    coordinator_report: dict[str, object] | None = None
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
    errors: list[str] = Field(default_factory=list[str])


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


def _scan_owned_generation_processes(
    *,
    proc_root: Path,
    owner_token_sha256: str,
    generation_id: str,
    session_id: str,
    cluster: str,
    release_sha256: str,
    registry_path: str,
    registry_sha256: str,
    route_revision: str,
    effective_uid: int | None,
) -> list[_OwnedGenerationProcess]:
    """Scan every bounded live process for one exact owned-generation identity."""
    expected_markers = {
        f"CLIO_RELAY_SESSION_GENERATION_ID={generation_id}".encode(),
        f"CLIO_RELAY_OWNER_SESSION_ID={session_id}".encode(),
        f"CLIO_RELAY_OWNER_SESSION_CLUSTER={cluster}".encode(),
        f"CLIO_RELAY_REMOTE_CLUSTER={cluster}".encode(),
        f"CLIO_RELAY_API_RELEASE_IDENTITY_SHA256={release_sha256}".encode(),
        f"CLIO_RELAY_CLUSTER_REGISTRY={registry_path}".encode(),
        f"CLIO_RELAY_SESSION_REGISTRY_SHA256={registry_sha256}".encode(),
        f"CLIO_RELAY_SESSION_ROUTE_REVISION={route_revision}".encode(),
    }
    matches: list[_OwnedGenerationProcess] = []
    observed_entries = 0
    try:
        entries = os.scandir(proc_root)
    except OSError as exc:
        raise RelayError(f"cannot scan process identities: {exc}") from exc
    with entries:
        for entry in entries:
            if not entry.name.isdigit():
                continue
            observed_entries += 1
            if observed_entries > _MAX_PROC_ENTRIES:
                raise RelayError("process identity scan exceeded its entry limit")
            pid = int(entry.name)
            proc = proc_root / entry.name
            try:
                proc_status = entry.stat(follow_symlinks=False)
                stat_payload = _read_bounded_proc_bytes(
                    proc / "stat",
                    maximum_bytes=_MAX_PROC_RECORD_BYTES,
                ).decode("utf-8")
                fields = stat_payload.rsplit(")", 1)[1].split()
                state = fields[0]
                process_group_id = int(fields[2])
                start_ticks = fields[19]
            except (FileNotFoundError, ProcessLookupError):
                continue
            except (IndexError, UnicodeDecodeError, ValueError) as exc:
                raise RelayError(
                    f"process identity record is invalid for pid {pid}: {exc}"
                ) from exc
            except OSError as exc:
                raise RelayError(f"cannot inspect process identity for pid {pid}: {exc}") from exc
            if effective_uid is not None and proc_status.st_uid != effective_uid:
                continue
            if state == "Z":
                continue
            try:
                environment_entries = _read_bounded_proc_bytes(
                    proc / "environ",
                    maximum_bytes=_MAX_PROC_RECORD_BYTES,
                ).split(bytes([0]))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as exc:
                raise RelayError(f"cannot verify protected process identity for pid {pid}") from exc
            except OSError as exc:
                raise RelayError(
                    f"cannot inspect process environment for pid {pid}: {exc}"
                ) from exc
            environment = [entry for entry in environment_entries if entry]
            by_name: dict[bytes, list[bytes]] = {}
            for marker in environment:
                name, separator, value = marker.partition(b"=")
                if not separator:
                    continue
                by_name.setdefault(name, []).append(value)
            token_values = [value for value in by_name.get(b"CLIO_RELAY_SESSION_OWNER_TOKEN", [])]
            token_matches = bool(
                len(token_values) == 1
                and hashlib.sha256(token_values[0]).hexdigest() == owner_token_sha256
            )
            exact_markers = all(
                by_name.get(expected.partition(b"=")[0]) == [expected.partition(b"=")[2]]
                for expected in expected_markers
            )
            if not (token_matches and exact_markers):
                continue
            matches.append(
                _OwnedGenerationProcess(
                    pid=pid,
                    process_group_id=process_group_id,
                    start_ticks=start_ticks,
                )
            )
    return sorted(matches, key=lambda process: process.pid)


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


def _signal_owned_generation_processes(
    *,
    processes: list[_OwnedGenerationProcess],
    signal_number: int,
    proc_root: Path,
    owner_token_sha256: str,
    generation_id: str,
    session_id: str,
    cluster: str,
    release_sha256: str,
    registry_path: str,
    registry_sha256: str,
    route_revision: str,
    effective_uid: int | None,
) -> list[int]:
    """Signal exact generation processes through pidfds after an identity rescan."""
    pidfd_open = cast(Callable[[int, int], int] | None, getattr(os, "pidfd_open", None))
    pidfd_send_signal = cast(
        Callable[[int, int, object | None, int], None] | None,
        getattr(signal, "pidfd_send_signal", None),
    )
    if pidfd_open is None or pidfd_send_signal is None:
        raise RelayError("race-safe pidfd session cleanup is unavailable")
    signaled: list[int] = []
    for process in processes:
        try:
            process_fd = pidfd_open(process.pid, 0)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RelayError(f"cannot open session pidfd for {process.pid}: {exc}") from exc
        try:
            current = _scan_owned_generation_processes(
                proc_root=proc_root,
                owner_token_sha256=owner_token_sha256,
                generation_id=generation_id,
                session_id=session_id,
                cluster=cluster,
                release_sha256=release_sha256,
                registry_path=registry_path,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=effective_uid,
            )
            if process not in current:
                continue
            try:
                pidfd_send_signal(process_fd, signal_number, None, 0)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RelayError(f"cannot signal owned session pid {process.pid}: {exc}") from exc
            signaled.append(process.pid)
        finally:
            os.close(process_fd)
    return signaled


def inspect_owned_session_recovery_status(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    proc_root: Path = Path("/proc"),
    effective_uid: int | None = None,
    transaction: _OwnedSessionTransaction | None = None,
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
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[str(exc)],
        )

    if document.get("schema_version") == "clio-relay.owner-session-cleanup-receipt.v1":
        return _inspect_owned_session_cleanup_receipt(
            cluster=cluster,
            session_id=session_id,
            document=document,
            core_dir=core_dir,
            proc_root=proc_root,
            effective_uid=uid,
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
        and api_pgid == api_pid
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
        and parsed_started_at is not None
        and parsed_started_at.tzinfo is not None
    )
    if not metadata_verified:
        errors.append("owned session metadata identity is incomplete or mismatched")

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
        and isinstance(api_pid, int)
        and isinstance(api_pgid, int)
        and isinstance(process_start, str)
        and isinstance(owner_token, str)
        and validated_generation is not None
        and isinstance(release_sha256, str)
        and isinstance(registry_path_raw, str)
        and isinstance(registry_sha256, str)
        and isinstance(route_revision, str)
    ):
        try:
            generation_processes = _scan_owned_generation_processes(
                proc_root=proc_root,
                owner_token_sha256=hashlib.sha256(owner_token.encode()).hexdigest(),
                generation_id=validated_generation,
                session_id=session_id,
                cluster=cluster,
                release_sha256=release_sha256,
                registry_path=registry_path_raw,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=uid,
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
        errors=errors,
    )


def _inspect_owned_session_cleanup_receipt(
    *,
    cluster: str,
    session_id: str,
    document: dict[str, object],
    core_dir: Path,
    proc_root: Path,
    effective_uid: int | None,
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
    coordinator_report_bound = False
    coordinator_report_sha256 = document.get("coordinator_report_sha256")
    raw_coordinator_report = document.get("coordinator_report")
    coordinator_fields_valid = bool(
        raw_coordinator_report is None and coordinator_report_sha256 is None
    )
    if raw_coordinator_report is not None or coordinator_report_sha256 is not None:
        try:
            coordinator_report = SessionLifecycleReport.model_validate(raw_coordinator_report)
            observed_coordinator_sha256 = session_lifecycle_report_sha256(coordinator_report)
            remote_resources = report.resources if report is not None else []
            coordinator_report_bound = bool(
                isinstance(coordinator_report_sha256, str)
                and coordinator_report_sha256 == observed_coordinator_sha256
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
            coordinator_fields_valid = coordinator_report_bound
        except (TypeError, ValueError) as exc:
            errors.append(f"owned session coordinator cleanup report is invalid: {exc}")
        if not coordinator_fields_valid:
            errors.append("owned session coordinator cleanup report binding is invalid")
    expected_keys = {
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
        "coordinator_report",
        "coordinator_report_sha256",
    }
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
        set(document) == expected_keys
        and document.get("owner") == "clio-relay"
        and document.get("cluster") == cluster
        and document.get("session_id") == session_id
        and validated_generation is not None
        and isinstance(api_pid, int)
        and not isinstance(api_pid, bool)
        and api_pid > 1
        and api_pgid == api_pid
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
        == sorted(("api.log", "api.pid", f"cluster-registry-{validated_generation}.json"))
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
        and isinstance(owner_token_sha256, str)
        and isinstance(release_sha256, str)
        and isinstance(registry_path, str)
        and isinstance(registry_sha256, str)
        and isinstance(route_revision, str)
    ):
        try:
            generation_processes = _scan_owned_generation_processes(
                proc_root=proc_root,
                owner_token_sha256=owner_token_sha256,
                generation_id=validated_generation,
                session_id=session_id,
                cluster=cluster,
                release_sha256=release_sha256,
                registry_path=registry_path,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=effective_uid,
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
        coordinator_report=(
            coordinator_report.model_dump(mode="json")
            if coordinator_report_bound and coordinator_report is not None
            else None
        ),
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


def session_lifecycle_report_sha256(report: SessionLifecycleReport) -> str:
    """Return the canonical digest for an exact lifecycle report."""
    payload = json.dumps(
        report.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class _BoundedCommandResult:
    """Bounded output captured from one local child command."""

    returncode: int
    stdout: bytes
    stderr: bytes


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
    from clio_relay.process_containment import (
        ensure_owned_process_tree_empty,
        owner_popen_kwargs,
        terminate_process_tree,
    )

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        **owner_popen_kwargs(),
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        process.kill()
        raise RelayError("bounded command pipes were not created")
    process_stdin = process.stdin
    process_stdout = process.stdout
    process_stderr = process.stderr
    stdout = bytearray()
    stderr = bytearray()
    overflow = threading.Event()
    writer_error: list[BaseException] = []

    def read_pipe(pipe: Any, target: bytearray, limit: int) -> None:
        try:
            while True:
                chunk = pipe.read(min(64 * 1024, limit + 1 - len(target)))
                if not chunk:
                    return
                target.extend(chunk)
                if len(target) > limit:
                    overflow.set()
                    return
        finally:
            pipe.close()

    def write_stdin() -> None:
        try:
            if input_bytes:
                process_stdin.write(input_bytes)
                process_stdin.flush()
        except BrokenPipeError:
            pass
        except BaseException as exc:  # pragma: no cover - platform pipe failure
            writer_error.append(exc)
        finally:
            with suppress(OSError):
                process_stdin.close()

    readers = [
        threading.Thread(
            target=read_pipe,
            args=(process_stdout, stdout, stdout_limit),
            daemon=True,
        ),
        threading.Thread(
            target=read_pipe,
            args=(process_stderr, stderr, stderr_limit),
            daemon=True,
        ),
    ]
    writer = threading.Thread(target=write_stdin, daemon=True)
    for thread in readers:
        thread.start()
    writer.start()
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    tree_cleanup_error: BaseException | None = None
    while process.poll() is None:
        if overflow.is_set():
            break
        if time.monotonic() >= deadline:
            timed_out = True
            break
        time.sleep(0.01)
    if overflow.is_set() or timed_out:
        try:
            terminate_process_tree(
                process,
                owns_group=True,
                timeout_seconds=2.0,
            )
        except BaseException as exc:  # pragma: no cover - provider-specific failure
            tree_cleanup_error = exc
            if process.poll() is None:
                process.kill()
    with suppress(subprocess.TimeoutExpired):
        process.wait(timeout=2.0)
    writer.join(timeout=2.0)
    for thread in readers:
        thread.join(timeout=2.0)
    if any(thread.is_alive() for thread in readers):
        try:
            terminate_process_tree(
                process,
                owns_group=True,
                timeout_seconds=2.0,
            )
        except BaseException as exc:  # pragma: no cover - provider-specific failure
            tree_cleanup_error = tree_cleanup_error or exc
        for thread in readers:
            thread.join(timeout=2.0)
    elif not (overflow.is_set() or timed_out):
        try:
            ensure_owned_process_tree_empty(process)
        except RuntimeError as exc:
            tree_cleanup_error = exc
    if any(thread.is_alive() for thread in (*readers, writer)):
        raise RelayError("bounded command pipes did not close")
    if tree_cleanup_error is not None:
        raise RelayError(f"bounded command process-tree cleanup failed: {tree_cleanup_error}")
    if overflow.is_set():
        raise RelayError("bounded command output exceeded its byte limit")
    if timed_out:
        raise RelayError(f"bounded command timed out after {timeout_seconds:g} seconds")
    if writer_error:
        raise RelayError(f"bounded command input failed: {writer_error[0]}")
    return _BoundedCommandResult(
        returncode=cast(int, process.returncode),
        stdout=bytes(stdout),
        stderr=bytes(stderr),
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


def _generation_processes(
    *,
    proc_root: Path,
    owner_token_sha256: str,
    generation_id: str,
    session_id: str,
    cluster: str,
    release_sha256: str,
    registry_path: str,
    registry_sha256: str,
    route_revision: str,
    effective_uid: int,
) -> list[_OwnedGenerationProcess]:
    """Return every exact process for one fully bound owned generation."""
    return _scan_owned_generation_processes(
        proc_root=proc_root,
        owner_token_sha256=owner_token_sha256,
        generation_id=generation_id,
        session_id=session_id,
        cluster=cluster,
        release_sha256=release_sha256,
        registry_path=registry_path,
        registry_sha256=registry_sha256,
        route_revision=route_revision,
        effective_uid=effective_uid,
    )


def _stop_owned_generation_processes(
    *,
    processes: list[_OwnedGenerationProcess],
    proc_root: Path,
    owner_token_sha256: str,
    generation_id: str,
    session_id: str,
    cluster: str,
    release_sha256: str,
    registry_path: str,
    registry_sha256: str,
    route_revision: str,
    effective_uid: int,
) -> list[int]:
    """Stop every exact generation process by pidfd and prove bounded absence."""

    def scan() -> list[_OwnedGenerationProcess]:
        return _generation_processes(
            proc_root=proc_root,
            owner_token_sha256=owner_token_sha256,
            generation_id=generation_id,
            session_id=session_id,
            cluster=cluster,
            release_sha256=release_sha256,
            registry_path=registry_path,
            registry_sha256=registry_sha256,
            route_revision=route_revision,
            effective_uid=effective_uid,
        )

    def send(
        selected: list[_OwnedGenerationProcess],
        signal_number: int,
    ) -> list[int]:
        return _signal_owned_generation_processes(
            processes=selected,
            signal_number=signal_number,
            proc_root=proc_root,
            owner_token_sha256=owner_token_sha256,
            generation_id=generation_id,
            session_id=session_id,
            cluster=cluster,
            release_sha256=release_sha256,
            registry_path=registry_path,
            registry_sha256=registry_sha256,
            route_revision=route_revision,
            effective_uid=effective_uid,
        )

    targeted = [process.pid for process in processes]
    if processes:
        send(processes, signal.SIGTERM)
    deadline = time.monotonic() + _OWNED_SESSION_PROCESS_STOP_TIMEOUT_SECONDS
    remaining = scan()
    while remaining and time.monotonic() < deadline:
        time.sleep(0.1)
        remaining = scan()
    if remaining:
        send(remaining, cast(int, getattr(signal, "SIGKILL", 9)))
        deadline = time.monotonic() + _OWNED_SESSION_PROCESS_STOP_TIMEOUT_SECONDS
        remaining = scan()
        while remaining and time.monotonic() < deadline:
            time.sleep(0.1)
            remaining = scan()
    if remaining:
        raise RelayError(
            "owned session generation processes remained after pidfd cleanup: "
            + ", ".join(str(process.pid) for process in remaining)
        )
    return targeted


def _write_session_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    operation: Literal["start", "teardown"],
    identity: dict[str, object],
    error: str | None = None,
) -> None:
    """Write non-authoritative atomic attempt evidence without an owner secret."""
    document = {
        "schema_version": "clio-relay.owner-session-attempt.v1",
        "operation": operation,
        **identity,
        "observed_at": datetime.now(UTC).isoformat(),
        "error": error,
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


def _wait_for_api_ready(*, process: subprocess.Popen[bytes], port: int) -> float:
    """Wait boundedly for an API child to answer its local health probe."""
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


def _validated_resumable_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    request: OwnedSessionStartRequest,
) -> dict[str, object] | None:
    """Return an exact prior start attempt that can resume one orphan generation."""
    attempt = transaction.read_json("start-attempt.json", required=False)
    if attempt is None:
        return None
    expected_keys = {
        "schema_version",
        "operation",
        "cluster",
        "session_id",
        "session_generation_id",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "remote_api_port",
        "observed_at",
        "error",
    }
    generation = attempt.get("session_generation_id")
    observed_at = attempt.get("observed_at")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
        parsed_observed_at = (
            datetime.fromisoformat(observed_at) if isinstance(observed_at, str) else None
        )
    except ValueError:
        validated_generation = None
        parsed_observed_at = None
    registry_path = attempt.get("cluster_registry_path")
    expected_registry_path = (
        transaction.path / f"cluster-registry-{validated_generation}.json"
        if validated_generation is not None
        else None
    )
    if not (
        set(attempt) == expected_keys
        and attempt.get("schema_version") == "clio-relay.owner-session-attempt.v1"
        and attempt.get("operation") == "start"
        and attempt.get("cluster") == request.cluster
        and attempt.get("session_id") == request.session_id
        and validated_generation is not None
        and isinstance(attempt.get("owner_token_sha256"), str)
        and len(cast(str, attempt.get("owner_token_sha256"))) == 64
        and isinstance(attempt.get("api_release_identity_sha256"), str)
        and len(cast(str, attempt.get("api_release_identity_sha256"))) == 64
        and registry_path == str(expected_registry_path)
        and attempt.get("cluster_registry_sha256") == request.cluster_registry_sha256
        and attempt.get("cluster_route_revision") == request.cluster_route_revision
        and attempt.get("remote_api_port") == request.remote_api_port
        and parsed_observed_at is not None
        and parsed_observed_at.tzinfo is not None
        and (attempt.get("error") is None or isinstance(attempt.get("error"), str))
    ):
        raise RelayError("prior owned-session start attempt identity is invalid")
    return attempt


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
    api_token = os.environ.get("CLIO_RELAY_API_TOKEN")
    if request.require_token and not api_token:
        raise RelayError("owned session API token is required but unavailable")
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
        resumable_attempt = (
            _validated_resumable_start_attempt(transaction, request=request)
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
                    queue.clear_owner_session_closing(
                        request.session_id,
                        session_generation_id=recorded_generation,
                    )
                    return [
                        f"session_already_running={request.session_id}",
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
                    owner_token = existing.get("owner_token")
                    release_sha256 = existing.get("api_release_identity_sha256")
                    registry_path = existing.get("cluster_registry_path")
                    registry_sha256 = existing.get("cluster_registry_sha256")
                    route_revision = existing.get("cluster_route_revision")
                    if not all(
                        isinstance(value, str)
                        for value in (
                            owner_token,
                            release_sha256,
                            registry_path,
                            registry_sha256,
                            route_revision,
                        )
                    ):
                        raise RelayError("owned generation process identity is incomplete")
                    _stop_owned_generation_processes(
                        processes=[
                            process
                            for process in _generation_processes(
                                proc_root=proc_root,
                                owner_token_sha256=hashlib.sha256(
                                    cast(str, owner_token).encode("utf-8")
                                ).hexdigest(),
                                generation_id=recorded_generation,
                                session_id=request.session_id,
                                cluster=request.cluster,
                                release_sha256=cast(str, release_sha256),
                                registry_path=cast(str, registry_path),
                                registry_sha256=cast(str, registry_sha256),
                                route_revision=cast(str, route_revision),
                                effective_uid=uid,
                            )
                        ],
                        proc_root=proc_root,
                        owner_token_sha256=hashlib.sha256(
                            cast(str, owner_token).encode("utf-8")
                        ).hexdigest(),
                        generation_id=recorded_generation,
                        session_id=request.session_id,
                        cluster=request.cluster,
                        release_sha256=cast(str, release_sha256),
                        registry_path=cast(str, registry_path),
                        registry_sha256=cast(str, registry_sha256),
                        route_revision=cast(str, route_revision),
                        effective_uid=uid,
                    )
        elif resumable_attempt is not None:
            recorded_generation = cast(str, resumable_attempt["session_generation_id"])
            attempt_token_sha256 = cast(str, resumable_attempt["owner_token_sha256"])
            attempt_release_sha256 = cast(
                str,
                resumable_attempt["api_release_identity_sha256"],
            )
            attempt_registry_path = cast(str, resumable_attempt["cluster_registry_path"])
            attempt_processes = _generation_processes(
                proc_root=proc_root,
                owner_token_sha256=attempt_token_sha256,
                generation_id=recorded_generation,
                session_id=request.session_id,
                cluster=request.cluster,
                release_sha256=attempt_release_sha256,
                registry_path=attempt_registry_path,
                registry_sha256=request.cluster_registry_sha256,
                route_revision=request.cluster_route_revision,
                effective_uid=uid,
            )
            if attempt_processes and not request.replace:
                raise RelayError(
                    "a prior start attempt still owns generation processes; use --replace"
                )
            if attempt_processes:
                _stop_owned_generation_processes(
                    processes=attempt_processes,
                    proc_root=proc_root,
                    owner_token_sha256=attempt_token_sha256,
                    generation_id=recorded_generation,
                    session_id=request.session_id,
                    cluster=request.cluster,
                    release_sha256=attempt_release_sha256,
                    registry_path=attempt_registry_path,
                    registry_sha256=request.cluster_registry_sha256,
                    route_revision=request.cluster_route_revision,
                    effective_uid=uid,
                )

        _assert_remote_port_available(request.remote_api_port)
        candidate_generation = uuid4().hex
        selected_generation = queue.prepare_owner_session_start(
            request.session_id,
            recorded_generation_id=recorded_generation,
            candidate_generation_id=candidate_generation,
        )
        if (
            existing is None
            and resumable_attempt is None
            and selected_generation != (candidate_generation)
        ):
            raise RelayError("core selected an unrecorded owned-session generation")
        registry_name = f"cluster-registry-{selected_generation}.json"
        registry_path = transaction.path / registry_name
        owner_token = secrets.token_hex(32)
        owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
        release_sha256 = release_identity.sha256()
        attempt_identity: dict[str, object] = {
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": selected_generation,
            "owner_token_sha256": owner_token_sha256,
            "api_release_identity_sha256": release_sha256,
            "cluster_registry_path": str(registry_path),
            "cluster_registry_sha256": request.cluster_registry_sha256,
            "cluster_route_revision": request.cluster_route_revision,
            "remote_api_port": request.remote_api_port,
        }
        _write_session_attempt(
            transaction,
            operation="start",
            identity=attempt_identity,
        )
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
        process: subprocess.Popen[bytes] | None = None
        metadata_committed = False
        try:
            environment = dict(os.environ)
            environment.update(
                {
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
            )
            provider_interpreter = Path(sys.executable).resolve(strict=True)
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
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=log_descriptor,
                stderr=subprocess.STDOUT,
                env=environment,
                start_new_session=True,
                close_fds=True,
            )
            final_interpreter_identity = provider_interpreter.stat()
            if (final_interpreter_identity.st_dev, final_interpreter_identity.st_ino) != (
                interpreter_identity.st_dev,
                interpreter_identity.st_ino,
            ):
                raise RelayError("verified provider interpreter changed during API spawn")
            process_identity: _OwnedGenerationProcess | None = None
            identity_deadline = time.monotonic() + 2.0
            while time.monotonic() < identity_deadline:
                matches = _generation_processes(
                    proc_root=proc_root,
                    owner_token_sha256=owner_token_sha256,
                    generation_id=selected_generation,
                    session_id=request.session_id,
                    cluster=request.cluster,
                    release_sha256=release_sha256,
                    registry_path=str(registry_path),
                    registry_sha256=request.cluster_registry_sha256,
                    route_revision=request.cluster_route_revision,
                    effective_uid=uid,
                )
                process_identity = next(
                    (candidate for candidate in matches if candidate.pid == process.pid),
                    None,
                )
                if process_identity is not None:
                    break
                if process.poll() is not None:
                    break
                time.sleep(0.05)
            if (
                process_identity is None
                or process_identity.process_group_id != process.pid
                or not _is_clio_relay_api_leader(proc_root=proc_root, pid=process.pid)
            ):
                raise RelayError("owned API process did not establish its exact identity")
            ready_seconds = _wait_for_api_ready(
                process=process,
                port=request.remote_api_port,
            )
            metadata = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "remote_api_port": request.remote_api_port,
                "api_pid": process.pid,
                "api_pgid": process.pid,
                "owner_token": owner_token,
                "session_generation_id": selected_generation,
                "api_release_identity": release_identity.model_dump(mode="json"),
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "cluster_authority_verified": True,
                "process_start_ticks": process_identity.start_ticks,
                "started_at": datetime.now(UTC).isoformat(),
                "owner": "clio-relay",
            }
            transaction.atomic_write("api.pid", f"{process.pid}\n".encode("ascii"))
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
                f"api_pid={process.pid}",
                f"session_generation_id={selected_generation}",
                f"remote_api_port={request.remote_api_port}",
                f"metadata={transaction.path / 'metadata.json'}",
            ]
        except BaseException as exc:
            if not metadata_committed:
                try:
                    remaining = _generation_processes(
                        proc_root=proc_root,
                        owner_token_sha256=owner_token_sha256,
                        generation_id=selected_generation,
                        session_id=request.session_id,
                        cluster=request.cluster,
                        release_sha256=release_sha256,
                        registry_path=str(registry_path),
                        registry_sha256=request.cluster_registry_sha256,
                        route_revision=request.cluster_route_revision,
                        effective_uid=uid,
                    )
                    _stop_owned_generation_processes(
                        processes=remaining,
                        proc_root=proc_root,
                        owner_token_sha256=owner_token_sha256,
                        generation_id=selected_generation,
                        session_id=request.session_id,
                        cluster=request.cluster,
                        release_sha256=release_sha256,
                        registry_path=str(registry_path),
                        registry_sha256=request.cluster_registry_sha256,
                        route_revision=request.cluster_route_revision,
                        effective_uid=uid,
                    )
                except RelayError as cleanup_error:
                    cleanup_detail = f"{exc}; cleanup failed: {cleanup_error}"
                else:
                    cleanup_detail = str(exc)
                with suppress(RelayError):
                    _write_session_attempt(
                        transaction,
                        operation="start",
                        identity=attempt_identity,
                        error=cleanup_detail,
                    )
            raise
        finally:
            os.close(log_descriptor)


def _capture_cleanup_target(
    transaction: _OwnedSessionTransaction,
    *,
    name: str,
    maximum_bytes: int,
) -> OwnedSessionCleanupTarget:
    """Capture an exact cleanup target identity through the pinned directory."""
    linked = transaction.stat_regular(name, required=False)
    if linked is None:
        return OwnedSessionCleanupTarget(name=name, present=False)
    payload = transaction.read_bytes(name, maximum_bytes=maximum_bytes)
    if payload is None:  # pragma: no cover - required read
        raise RelayError(f"owned cleanup target disappeared: {name}")
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
        sha256=hashlib.sha256(payload).hexdigest(),
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
    expected_names = sorted(("api.log", "api.pid", f"cluster-registry-{generation_id}.json"))
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
        if (
            target.device is None
            or target.inode is None
            or target.size is None
            or target.sha256 is None
        ):  # pragma: no cover - validated model shape
            raise RelayError(f"cleanup target identity is incomplete: {target.name}")
        maximum_bytes = (
            _MAX_OWNED_SESSION_LOG_BYTES
            if target.name == "api.log"
            else _MAX_OWNED_SESSION_DOCUMENT_BYTES
        )
        transaction.unlink_verified(
            target.name,
            expected_device=target.device,
            expected_inode=target.inode,
            expected_size=target.size,
            expected_sha256=target.sha256,
            maximum_bytes=maximum_bytes,
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
        }
        receipt_committed = False
        try:
            processes = _generation_processes(
                proc_root=proc_root,
                owner_token_sha256=owner_token_sha256,
                generation_id=request.expected_session_generation_id,
                session_id=request.session_id,
                cluster=request.cluster,
                release_sha256=release_sha256,
                registry_path=registry_path,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=uid,
            )
            prior_running = bool(processes)
            prior_observed_at = datetime.now(UTC)
            targeted_pids = _stop_owned_generation_processes(
                processes=processes,
                proc_root=proc_root,
                owner_token_sha256=owner_token_sha256,
                generation_id=request.expected_session_generation_id,
                session_id=request.session_id,
                cluster=request.cluster,
                release_sha256=release_sha256,
                registry_path=registry_path,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=uid,
            )
            final_processes = _generation_processes(
                proc_root=proc_root,
                owner_token_sha256=owner_token_sha256,
                generation_id=request.expected_session_generation_id,
                session_id=request.session_id,
                cluster=request.cluster,
                release_sha256=release_sha256,
                registry_path=registry_path,
                registry_sha256=registry_sha256,
                route_revision=route_revision,
                effective_uid=uid,
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
                    "all exact owned-generation processes were stopped by pidfd"
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
            target_names = sorted(("api.log", "api.pid", f"cluster-registry-{generation_id}.json"))
            targets = [
                _capture_cleanup_target(
                    transaction,
                    name=name,
                    maximum_bytes=(
                        _MAX_OWNED_SESSION_LOG_BYTES
                        if name == "api.log"
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
                "coordinator_report": None,
                "coordinator_report_sha256": None,
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
    observed_sha256 = session_lifecycle_report_sha256(report)
    if observed_sha256 != request.coordinator_report_sha256:
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
        if not (
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
        ):
            raise RelayError("coordinator cleanup report does not extend the exact remote report")

        existing_report = document.get("coordinator_report")
        existing_sha256 = document.get("coordinator_report_sha256")
        if existing_report is not None or existing_sha256 is not None:
            if not (
                existing_sha256 == request.coordinator_report_sha256
                and existing_report == report.model_dump(mode="json")
                and status.coordinator_report_bound
            ):
                raise RelayError(
                    "coordinator cleanup report is immutable and cannot be replaced or downgraded"
                )
            return status

        finalized = dict(document)
        finalized["coordinator_report"] = report.model_dump(mode="json")
        finalized["coordinator_report_sha256"] = request.coordinator_report_sha256
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
            and reread.coordinator_report_sha256 == request.coordinator_report_sha256
            and reread.coordinator_report == report.model_dump(mode="json")
        ):
            raise RelayError("coordinator cleanup report was not durably re-read after commit")
        return reread


def start_remote_session(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None = None,
    replace: bool = False,
) -> list[str]:
    """Start a cluster-side relay API owned by a session id."""
    _validate_session(session_id=session_id, remote_api_port=remote_api_port)
    result = _ssh_script(
        definition,
        _start_script(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            expected_api_release_identity=expected_api_release_identity,
            replace=replace,
        ),
    )
    return result.splitlines()


def status_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
) -> dict[str, object]:
    """Return status for a previously started remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    output = _ssh_script(
        definition,
        _owned_status_script(cluster=definition.name, session_id=session_id),
    )
    return cast(dict[str, object], json.loads(output))


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
    output = _ssh_script(
        definition,
        _owned_cleanup_finalize_script(definition=definition, request=request),
    )
    status = OwnedSessionRecoveryStatus.model_validate_json(output)
    if not (
        status.recovery_verified
        and status.cleanup_receipt
        and status.cleanup_paths_pending is False
        and status.session_generation_id == session_generation_id
        and status.coordinator_report_bound
        and status.coordinator_report_sha256 == request.coordinator_report_sha256
        and status.coordinator_report == report.model_dump(mode="json")
    ):
        raise RelayError("remote coordinator cleanup report finalization was not exact")
    return status


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
    remote_api_port: int,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None,
    replace: bool,
) -> str:
    cluster_registry_json, cluster_registry_sha256, route_revision = (
        _session_cluster_registry_authority(cluster=cluster, definition=definition)
    )
    request = OwnedSessionStartRequest(
        cluster=cluster,
        session_id=session_id,
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


def _owned_status_script(*, cluster: str, session_id: str) -> str:
    """Use the bounded, lock-coordinated recovery contract for public status."""
    return (
        "set -euo pipefail\n"
        f"clio-relay session recovery-status --cluster {shlex.quote(cluster)} "
        f"--session-id {shlex.quote(session_id)}\n"
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
    request: OwnedSessionCleanupFinalizeRequest,
) -> str:
    """Carry one bounded coordinator cleanup report over the remote stdin contract."""
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"printf '%s' {_shell_single_quote(request.model_dump_json())} | "
        "clio-relay session finalize-cleanup-owned\n"
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


def _ssh_script(definition: ClusterDefinition, script: str) -> str:
    encoded_script = script.encode("utf-8")
    if len(encoded_script) > _MAX_REMOTE_SESSION_SCRIPT_BYTES:
        raise RelayError("remote session command exceeds its byte limit")
    try:
        result = _run_bounded_command(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input_bytes=encoded_script,
            timeout_seconds=_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
            stdout_limit=_MAX_REMOTE_SESSION_STDOUT_BYTES,
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
