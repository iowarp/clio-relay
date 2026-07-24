"""Crash-safe desired-state reconciliation primitives for cluster bootstrap.

The SSH bootstrap shell is intentionally a small transaction driver.  This
module owns the durable contract used by that driver: canonical desired-state
identity, read-only no-op verification, JARVIS state preservation evidence,
and the fsync-backed transaction journal.
"""

from __future__ import annotations

import csv
import ctypes
import hashlib
import io
import json
import os
import shlex
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Literal, cast
from urllib.parse import unquote, urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.bounded_process import (
    BoundedProcessError,
    BoundedProcessOutputLimit,
    run_bounded_process,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info
from clio_relay.validation_report import sha256_file
from clio_relay.worker_lifetime_lock import (
    WorkerLifetimeLock,
    WorkerLifetimeLockUnavailable,
    exclusive_migration_lifetime,
)

BOOTSTRAP_DESIRED_STATE_SCHEMA = "clio-relay.bootstrap-desired-state.v1"
BOOTSTRAP_RECEIPT_SCHEMA = "clio-relay.bootstrap-receipt.v2"
BOOTSTRAP_TRANSACTION_SCHEMA = "clio-relay.bootstrap-transaction.v1"
MANAGED_JARVIS_REPO_PATH = "~/.local/share/clio-relay/clio_relay"
LEGACY_MANAGED_JARVIS_REPO_PATH = "~/.local/share/clio-relay/managed-jarvis-repo"
MAX_JARVIS_CONFIG_BYTES = 1024 * 1024
MAX_JARVIS_REPOS_BYTES = 4 * 1024 * 1024
MAX_JARVIS_GRAPH_BYTES = 64 * 1024 * 1024
MAX_JARVIS_DISTRIBUTION_METADATA_BYTES = 1024 * 1024
MAX_JARVIS_DISTRIBUTION_RECORD_BYTES = 64 * 1024 * 1024
BOOTSTRAP_LOCK_TIMEOUT_SECONDS = 30.0
_O_BINARY = cast(int, getattr(os, "O_BINARY", 0))
_O_NOFOLLOW = cast(int, getattr(os, "O_NOFOLLOW", 0))
_FCHMOD = cast(
    Callable[[int, int], None] | None,
    getattr(os, "fchmod", None),  # noqa: B009 - absent from Windows typing/runtime
)
_GETUID = cast(Callable[[], int] | None, getattr(os, "getuid", None))
_AT_FDCWD = -100
_RENAME_EXCHANGE = 2


def repair_legacy_cursor_permissions_for_upgrade(core_dir: Path) -> dict[str, object]:
    """Privatize the fixed legacy cursor directory through a pinned queue root.

    Forward recovery can execute a generation whose queue initializer predates
    cursor-directory repair. The current candidate calls this compatibility
    operation while holding the inherited exclusive writer-lifetime guard, so
    the old generation can finish its journal before the candidate replaces it.
    Missing cursors are a no-op; links, foreign ownership, and identity changes
    fail closed.
    """
    if os.name != "posix" or _FCHMOD is None or _GETUID is None:
        raise ConfigurationError("legacy cursor permission repair requires POSIX fchmod")
    with exclusive_migration_lifetime(core_dir) as locked_core:
        root_descriptor = locked_core.filesystem_root_descriptor
        if root_descriptor is None:
            raise ConfigurationError("legacy cursor permission repair has no pinned root")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open("cursors", flags, dir_fd=root_descriptor)
        except FileNotFoundError:
            return {
                "schema_version": "clio-relay.bootstrap-legacy-cursor-repair.v1",
                "action": "absent",
            }
        except OSError as exc:
            raise ConfigurationError(
                "legacy cursor directory cannot be safely opened through the pinned root"
            ) from exc
        try:
            try:
                os.set_inheritable(descriptor, False)
                before = os.fstat(descriptor)
                if not stat.S_ISDIR(before.st_mode) or before.st_uid != _GETUID():
                    raise ConfigurationError(
                        "legacy cursor directory is not one owned real directory"
                    )
                action = "reused"
                if stat.S_IMODE(before.st_mode) != 0o700:
                    _FCHMOD(descriptor, 0o700)
                    action = "repaired"
                after = os.fstat(descriptor)
                if (
                    (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                    or not stat.S_ISDIR(after.st_mode)
                    or after.st_uid != _GETUID()
                    or stat.S_IMODE(after.st_mode) != 0o700
                ):
                    raise ConfigurationError(
                        "legacy cursor directory identity changed during permission repair"
                    )
                return {
                    "schema_version": "clio-relay.bootstrap-legacy-cursor-repair.v1",
                    "action": action,
                    "device": after.st_dev,
                    "inode": after.st_ino,
                }
            except OSError as exc:
                raise ConfigurationError(
                    "legacy cursor directory permissions could not be repaired"
                ) from exc
        finally:
            with suppress(OSError):
                os.close(descriptor)


@contextmanager
def bootstrap_invocation_lock(
    *,
    home: Path | None = None,
    timeout_seconds: float = BOOTSTRAP_LOCK_TIMEOUT_SECONDS,
) -> Generator[Path]:
    """Serialize bootstrap inspection and mutation through one private lock."""
    if timeout_seconds <= 0:
        raise ValueError("bootstrap lock timeout must be positive")
    resolved_home = (home or Path.home()).resolve()
    directory = resolved_home / ".local/share/clio-relay"
    lock = WorkerLifetimeLock(
        directory,
        mode="exclusive",
        timeout_seconds=timeout_seconds,
        lock_name="bootstrap.lock",
    )
    try:
        lock.acquire()
    except WorkerLifetimeLockUnavailable as exc:
        raise ConfigurationError("timed out acquiring the bootstrap lock") from exc
    except ConfigurationError as exc:
        raise ConfigurationError(f"private bootstrap lock is invalid: {exc}") from exc
    try:
        yield lock.path
    finally:
        lock.release()


class BootstrapDesiredState(BaseModel):
    """Complete, canonical identity requested by one bootstrap invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.bootstrap-desired-state.v1"] = (
        BOOTSTRAP_DESIRED_STATE_SCHEMA
    )
    bootstrap_profile: Literal["linux-user"] = "linux-user"
    cluster: str | None = None
    core_dir: str
    spool_dir: str
    worker_service: str | None = None
    relay_install_spec: str
    relay_artifact_sha256: str | None = None
    relay_source_identity: str = Field(min_length=1, max_length=512)
    frp_version: str
    frpc_sha256: str
    frps_sha256: str
    uv_version: str
    uv_sha256: str
    jarvis_util_commit: str
    jarvis_cd_version: str
    jarvis_cd_wheel_url: str
    jarvis_cd_wheel_sha256: str
    jarvis_resource_graph_profile: str | None = None
    allow_jarvis_resource_graph_build: bool = False
    clio_kit_install_spec: str
    clio_kit_version: str
    clio_kit_artifact_sha256: str
    agent_adapter: str
    agent_npm_package: str | None = None
    agent_npm_bin: str | None = None
    agent_args: list[str] = Field(default_factory=list)
    jarvis_root: str = "~/.ppi-jarvis"
    jarvis_config_dir: str = "~/.local/share/clio-relay/jarvis-config"
    jarvis_private_dir: str = "~/.local/share/clio-relay/jarvis-private"
    jarvis_shared_dir: str = "~/.local/share/clio-relay/jarvis-shared"
    managed_jarvis_repo: str = MANAGED_JARVIS_REPO_PATH

    @model_validator(mode="after")
    def validate_identity(self) -> BootstrapDesiredState:
        """Reject incomplete or ambiguous desired identities."""
        for field_name in (
            "frpc_sha256",
            "frps_sha256",
            "uv_sha256",
            "jarvis_cd_wheel_sha256",
            "clio_kit_artifact_sha256",
        ):
            _require_sha256(getattr(self, field_name), field=field_name)
        if self.relay_artifact_sha256 is not None:
            _require_sha256(self.relay_artifact_sha256, field="relay_artifact_sha256")
        if self.relay_artifact_sha256 is not None and not self.relay_source_identity.endswith(
            f":sha256:{self.relay_artifact_sha256}"
        ):
            raise ValueError("relay source identity must match its artifact SHA-256")
        if any(character in self.relay_source_identity for character in "\x00\r\n"):
            raise ValueError("relay source identity contains a control boundary")
        if self.cluster is None and self.worker_service is not None:
            raise ValueError("an unmanaged bootstrap cannot name a worker service")
        if self.cluster is not None and not self.worker_service:
            raise ValueError("a managed cluster bootstrap must name its worker service")
        profile = self.jarvis_resource_graph_profile
        if profile is not None and (
            not profile
            or profile != profile.strip()
            or len(profile) > 256
            or profile in {".", ".."}
            or "/" in profile
            or "\\" in profile
            or any(ord(character) < 32 or ord(character) == 127 for character in profile)
        ):
            raise ValueError("JARVIS resource graph profile must be one safe exact name")
        if self.allow_jarvis_resource_graph_build and profile is None:
            raise ValueError("JARVIS graph build fallback requires an exact requested profile")
        return self

    @property
    def fingerprint(self) -> str:
        """Return the content identity of this desired deployment."""
        return canonical_json_sha256(self.model_dump(mode="json"))


class JarvisStateEvidence(BaseModel):
    """Read-only identity of operator-owned JARVIS state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    initialized: bool
    root: str
    roots: dict[str, str] = Field(default_factory=dict)
    config_sha256: str | None = None
    repos_sha256: str | None = None
    resource_graph_sha256: str | None = None
    managed_repo_registered: bool = False
    managed_builtin_repo_registered: bool = False


class BootstrapReadinessEvidence(BaseModel):
    """Bounded no-scheduler readiness proof for an installed generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    service_name: str | None = None
    service_was_active: bool | None = None
    service_was_enabled: bool | None = None
    queue_ready: bool
    queue: dict[str, object] | None = None
    worker_ready: bool | None = None
    worker: dict[str, object] | None = None
    scheduler_jobs_submitted: int = 0


class BootstrapInspection(BaseModel):
    """Result of a read-only exact-no-op inspection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exact_match: bool
    desired_fingerprint: str
    reasons: list[str] = Field(default_factory=list)
    install_receipt_sha256: str | None = None
    active_generation: str | None = None
    current_generation_target: str | None = None
    jarvis_state: JarvisStateEvidence
    readiness: BootstrapReadinessEvidence


class BootstrapActivationPathIdentity(BaseModel):
    """Immutable identity of one pre-activation path."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    mode: int = Field(ge=0)
    size: int = Field(ge=0)
    modified_ns: int = Field(ge=0)
    changed_ns: int = Field(ge=0)
    file_type: Literal["file", "symlink"]
    sha256: str | None = None
    symlink_target: str | None = None

    @model_validator(mode="after")
    def validate_content_identity(self) -> BootstrapActivationPathIdentity:
        """Require content evidence appropriate to the captured file type."""
        if self.file_type == "file":
            if self.sha256 is None or self.symlink_target is not None:
                raise ValueError("bootstrap activation file identity is incomplete")
            _require_sha256(self.sha256, field="activation_path.sha256")
        elif self.sha256 is not None or not self.symlink_target:
            raise ValueError("bootstrap activation symlink identity is incomplete")
        return self


class BootstrapActivationPath(BaseModel):
    """One stable activation path and its exact state before fencing."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    kind: Literal["file", "file_or_symlink", "symlink"]
    before: BootstrapActivationPathIdentity | None = None

    @model_validator(mode="after")
    def validate_path(self) -> BootstrapActivationPath:
        """Require an absolute normalized path and a compatible identity."""
        candidate = Path(self.path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or os.path.normpath(self.path) != self.path
            or any(character in self.path for character in "\x00\r\n")
        ):
            raise ValueError("bootstrap activation path must be absolute and normalized")
        if self.before is not None and not (
            (self.kind == "file" and self.before.file_type == "file")
            or (self.kind == "symlink" and self.before.file_type == "symlink")
            or self.kind == "file_or_symlink"
        ):
            raise ValueError("bootstrap activation path identity has an invalid type")
        return self


class BootstrapReconcilePlan(BaseModel):
    """Read-only component plan produced before journaling, fencing, or activation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["repair", "relay-only", "component-upgrade", "full"]
    desired_fingerprint: str
    reasons: list[str] = Field(default_factory=list)
    component_actions: dict[str, Literal["reuse", "replace"]]
    reusable_paths: dict[str, str] = Field(default_factory=dict)
    activation_paths: dict[str, BootstrapActivationPath] = Field(default_factory=dict)


class BootstrapPersistentUvToolIdentity(BaseModel):
    """Typed candidate uv-tool identity independent of the installed relay version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.persistent-uv-tool-identity.v2"]
    manager: Literal["uv"]
    uv_executable: str
    uv_version: str
    uv_executable_sha256: str
    tool_directory: str
    tool_bin_directory: str
    environment_prefix: str
    provider_interpreter: str
    provider_interpreter_sha256: str
    tool_executable: str
    tool_executable_resolved: str
    tool_executable_sha256: str
    distribution_console_script_path: str
    distribution_console_script_sha256: str
    uv_receipt_path: str
    uv_receipt_sha256: str
    distribution: str
    distribution_version: str
    distribution_metadata_path: str
    entry_point: str
    source_artifact_path: str
    source_artifact_sha256: str
    record_path: str
    record_sha256: str
    runtime_closure_sha256: str
    runtime_file_count: int = Field(gt=0)
    runtime_bytes: int = Field(gt=0)
    pyvenv_uv_version: str


class BootstrapReplacementProviderEvidence(BaseModel):
    """Attested candidate provider allowed to replace one legacy relay runtime."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.bootstrap-replacement-provider.v1"] = (
        "clio-relay.bootstrap-replacement-provider.v1"
    )
    desired_fingerprint: str
    relay_install_spec: str
    preparing_root: str
    extracted_source_root: str
    source_archive_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    coordinator_provider_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    persistent_tool: BootstrapPersistentUvToolIdentity


class BootstrapTransactionState(StrEnum):
    """Persisted states for one bootstrap generation transaction."""

    LOCKED = "locked"
    RECOVERING = "recovering"
    INSPECTED = "inspected"
    NOOP_VERIFIED = "noop_verified"
    PREPARING = "preparing"
    PREPARED = "prepared"
    FENCING = "fencing"
    FENCED = "fenced"
    ACTIVATING = "activating"
    ACTIVATED = "activated"
    MIGRATION_STARTED = "migration_started"
    MIGRATED = "migrated"
    STARTING = "starting"
    SERVICE_VERIFIED = "service_verified"
    COMMITTED = "committed"
    RECOVERED = "recovered"


class BootstrapOwnedPathIdentity(BaseModel):
    """Durable identity of one path created by a bootstrap transaction."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    file_type: Literal["directory", "file", "symlink"]
    changed_ns: int = Field(ge=0)
    size: int = Field(ge=0)
    sha256: str | None = None
    symlink_target: str | None = None

    @model_validator(mode="after")
    def validate_type_evidence(self) -> BootstrapOwnedPathIdentity:
        """Require content evidence appropriate to the path type."""
        if self.file_type == "file":
            if self.sha256 is None or self.symlink_target is not None:
                raise ValueError("bootstrap owned file identity is incomplete")
            _require_sha256(self.sha256, field="owned_path.sha256")
        elif self.file_type == "symlink":
            if self.sha256 is not None or self.symlink_target is None:
                raise ValueError("bootstrap owned symlink identity is incomplete")
        elif self.sha256 is not None or self.symlink_target is not None:
            raise ValueError("bootstrap owned directory identity is invalid")
        return self


class BootstrapOwnedPath(BaseModel):
    """One path proven absent before a full bootstrap transaction."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    kind: Literal["directory", "file", "file_or_symlink", "symlink"]
    identity: BootstrapOwnedPathIdentity | None = None

    @model_validator(mode="after")
    def validate_path(self) -> BootstrapOwnedPath:
        """Require one unambiguous absolute lexical path."""
        candidate = Path(self.path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or os.path.normpath(self.path) != self.path
            or any(character in self.path for character in "\x00\r\n")
        ):
            raise ValueError("bootstrap owned path must be absolute and normalized")
        if self.identity is not None:
            file_type = self.identity.file_type
            valid = (
                (self.kind == "directory" and file_type == "directory")
                or (self.kind == "file" and file_type == "file")
                or (self.kind == "symlink" and file_type == "symlink")
                or (self.kind == "file_or_symlink" and file_type in {"file", "symlink"})
            )
            if not valid:
                raise ValueError("bootstrap owned path kind and identity disagree")
        return self


_TRANSACTION_TRANSITIONS: dict[BootstrapTransactionState, frozenset[BootstrapTransactionState]] = {
    BootstrapTransactionState.LOCKED: frozenset(
        {BootstrapTransactionState.RECOVERING, BootstrapTransactionState.INSPECTED}
    ),
    BootstrapTransactionState.RECOVERING: frozenset(
        {
            BootstrapTransactionState.INSPECTED,
            BootstrapTransactionState.MIGRATION_STARTED,
            BootstrapTransactionState.MIGRATED,
            BootstrapTransactionState.STARTING,
            BootstrapTransactionState.SERVICE_VERIFIED,
            BootstrapTransactionState.COMMITTED,
        }
    ),
    BootstrapTransactionState.INSPECTED: frozenset(
        {
            BootstrapTransactionState.NOOP_VERIFIED,
            BootstrapTransactionState.PREPARING,
            BootstrapTransactionState.FENCING,
        }
    ),
    BootstrapTransactionState.NOOP_VERIFIED: frozenset({BootstrapTransactionState.COMMITTED}),
    BootstrapTransactionState.PREPARING: frozenset({BootstrapTransactionState.PREPARED}),
    BootstrapTransactionState.PREPARED: frozenset(
        {BootstrapTransactionState.FENCING, BootstrapTransactionState.ACTIVATING}
    ),
    BootstrapTransactionState.FENCING: frozenset({BootstrapTransactionState.FENCED}),
    BootstrapTransactionState.FENCED: frozenset(
        {BootstrapTransactionState.ACTIVATING, BootstrapTransactionState.PREPARING}
    ),
    BootstrapTransactionState.ACTIVATING: frozenset({BootstrapTransactionState.ACTIVATED}),
    BootstrapTransactionState.ACTIVATED: frozenset({BootstrapTransactionState.MIGRATION_STARTED}),
    BootstrapTransactionState.MIGRATION_STARTED: frozenset({BootstrapTransactionState.MIGRATED}),
    BootstrapTransactionState.MIGRATED: frozenset(
        {BootstrapTransactionState.STARTING, BootstrapTransactionState.SERVICE_VERIFIED}
    ),
    BootstrapTransactionState.STARTING: frozenset({BootstrapTransactionState.SERVICE_VERIFIED}),
    BootstrapTransactionState.SERVICE_VERIFIED: frozenset({BootstrapTransactionState.COMMITTED}),
    BootstrapTransactionState.COMMITTED: frozenset(),
    BootstrapTransactionState.RECOVERED: frozenset(),
}


class BootstrapTransactionJournal(BaseModel):
    """Fsync-backed recovery record for one generation activation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.bootstrap-transaction.v1"] = BOOTSTRAP_TRANSACTION_SCHEMA
    invocation_id: str
    desired_fingerprint: str
    mode: Literal["repair", "relay-only", "component-upgrade", "full"] = "relay-only"
    state: BootstrapTransactionState = BootstrapTransactionState.LOCKED
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    previous_generation: str | None = None
    prepared_generation: str | None = None
    service_name: str | None = None
    service_was_active: bool | None = None
    service_was_enabled: bool | None = None
    recovered_from: BootstrapTransactionState | None = None
    irreversible_boundary: bool = False
    owned_paths: dict[str, BootstrapOwnedPath] = Field(default_factory=dict)
    phase_identities: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_generation_state(self) -> BootstrapTransactionJournal:
        """Require enough identity to recover every mutation boundary."""
        _require_sha256(self.desired_fingerprint, field="desired_fingerprint")
        if not self.invocation_id or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
            for character in self.invocation_id
        ):
            raise ValueError("bootstrap invocation identity is invalid")
        if len(set(item.path for item in self.owned_paths.values())) != len(self.owned_paths):
            raise ValueError("bootstrap owned paths must be unique")
        for phase, identity in self.phase_identities.items():
            if not phase or any(
                character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
                for character in phase
            ):
                raise ValueError("bootstrap phase name is invalid")
            _require_sha256(identity, field=f"phase_identities.{phase}")
        if (
            self.state.value
            in {
                BootstrapTransactionState.PREPARED.value,
                BootstrapTransactionState.ACTIVATING.value,
                BootstrapTransactionState.ACTIVATED.value,
                BootstrapTransactionState.MIGRATION_STARTED.value,
                BootstrapTransactionState.MIGRATED.value,
                BootstrapTransactionState.STARTING.value,
                BootstrapTransactionState.SERVICE_VERIFIED.value,
                BootstrapTransactionState.COMMITTED.value,
            }
            and not self.prepared_generation
        ):
            raise ValueError("prepared generation is required after preparation")
        if (
            self.state.value
            in {
                BootstrapTransactionState.MIGRATION_STARTED.value,
                BootstrapTransactionState.MIGRATED.value,
                BootstrapTransactionState.STARTING.value,
                BootstrapTransactionState.SERVICE_VERIFIED.value,
                BootstrapTransactionState.COMMITTED.value,
            }
            and not self.irreversible_boundary
        ):
            raise ValueError("queue migration states must record the irreversible boundary")
        return self

    def advance(self, state: BootstrapTransactionState) -> None:
        """Advance by one valid state and update crash-recovery evidence."""
        if state not in _TRANSACTION_TRANSITIONS[self.state]:
            raise ConfigurationError(
                f"invalid bootstrap transaction transition: {self.state.value} -> {state.value}"
            )
        self.state = state
        if state is BootstrapTransactionState.MIGRATION_STARTED or (
            state is BootstrapTransactionState.ACTIVATING and self.mode != "full"
        ):
            self.irreversible_boundary = True
        self.updated_at = datetime.now(UTC)

    def persist(self, path: Path) -> None:
        """Atomically write and fsync this journal and its parent directory."""
        _atomic_json(path, self.model_dump(mode="json"))

    def complete_recovery(self) -> None:
        """Record terminal recovery while preserving the interrupted boundary."""
        if self.state in {
            BootstrapTransactionState.COMMITTED,
            BootstrapTransactionState.RECOVERED,
        }:
            raise ConfigurationError("a terminal bootstrap transaction cannot be recovered")
        if self.recovered_from is None:
            self.recovered_from = self.state
        self.state = BootstrapTransactionState.RECOVERED
        self.updated_at = datetime.now(UTC)

    def record_phase(self, phase: str, identity: str) -> None:
        """Bind a completed transaction phase to one immutable identity."""
        if self.state in {
            BootstrapTransactionState.COMMITTED,
            BootstrapTransactionState.RECOVERED,
        }:
            raise ConfigurationError("a terminal bootstrap transaction cannot record a phase")
        if not phase or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
            for character in phase
        ):
            raise ConfigurationError("bootstrap phase name is invalid")
        _require_sha256(identity, field=f"phase_identities.{phase}")
        previous = self.phase_identities.get(phase)
        if previous is not None and previous != identity:
            raise ConfigurationError(f"bootstrap phase identity changed: {phase}")
        self.phase_identities[phase] = identity
        self.updated_at = datetime.now(UTC)

    @classmethod
    def load(cls, path: Path) -> BootstrapTransactionJournal:
        """Strictly load one existing transaction journal."""
        try:
            return cls.model_validate_json(_read_bounded(path, maximum=1024 * 1024))
        except (OSError, ValueError) as exc:
            raise ConfigurationError(f"bootstrap transaction journal is invalid: {path}") from exc

    @property
    def recovery_mode(self) -> Literal["discard", "rollback", "forward", "none"]:
        """Return the only safe crash recovery direction for this state."""
        if self.state in {
            BootstrapTransactionState.COMMITTED,
            BootstrapTransactionState.RECOVERED,
        }:
            return "none"
        if self.irreversible_boundary:
            return "forward"
        if self.mode == "full":
            return "discard"
        if self.state in {
            BootstrapTransactionState.ACTIVATED,
            BootstrapTransactionState.ACTIVATING,
        }:
            return "rollback"
        return "discard"


def canonical_json_sha256(value: object) -> str:
    """Hash one JSON value using the deployment contract's canonical form."""
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def execution_environment_identity(
    root: Path,
    *,
    executables: dict[str, Path],
) -> dict[str, object]:
    """Identify a reused execution boundary without scanning or copying its tree."""
    lexical_root = Path(os.path.abspath(root.expanduser()))
    try:
        root_details = lexical_root.lstat()
    except OSError as exc:
        raise ConfigurationError("execution environment is unavailable") from exc
    if lexical_root.is_symlink() or not lexical_root.is_dir():
        raise ConfigurationError("execution environment is not one owned directory")
    identities: dict[str, object] = {}
    resolved_root = lexical_root.resolve(strict=True)
    for name, executable in sorted(executables.items()):
        try:
            lexical = Path(os.path.abspath(executable.expanduser()))
            before = lexical.lstat()
            located = lexical.parent.resolve(strict=True) / lexical.name
            resolved = lexical.resolve(strict=True)
            if (
                located == resolved_root
                or not located.is_relative_to(resolved_root)
                or not resolved.is_file()
                or not os.access(resolved, os.X_OK)
            ):
                raise ConfigurationError(f"execution boundary executable is invalid: {name}")
            digest = sha256_file(resolved)
            if _stat_identity(lexical.lstat()) != _stat_identity(before):
                raise ConfigurationError(f"execution boundary executable changed: {name}")
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"execution boundary executable is invalid: {name}") from exc
        identities[name] = {
            "lexical_path": str(lexical),
            "resolved_path": str(resolved),
            "sha256": digest,
            "size_bytes": resolved.stat().st_size,
        }
    config_path = lexical_root / "pyvenv.cfg"
    config_sha256 = sha256_file(config_path) if config_path.is_file() else None
    if _stat_identity(lexical_root.lstat()) != _stat_identity(root_details):
        raise ConfigurationError("execution environment changed during inspection")
    return {
        "schema_version": "clio-relay.execution-boundary.v1",
        "root": str(lexical_root),
        "root_identity": {
            "device": root_details.st_dev,
            "inode": root_details.st_ino,
            "mode": root_details.st_mode,
            "modified_ns": root_details.st_mtime_ns,
            "changed_ns": root_details.st_ctime_ns,
        },
        "pyvenv_cfg_sha256": config_sha256,
        "executables": identities,
        "tree_scanned": False,
        "tree_copied": False,
    }


def jarvis_wrapper_payload(execution_python: Path) -> bytes:
    """Return the deterministic relay-owned JARVIS launcher payload."""
    lexical_python = Path(os.path.abspath(execution_python.expanduser()))
    try:
        before = lexical_python.lstat()
        resolved_python = lexical_python.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("JARVIS execution interpreter is unavailable") from exc
    if (
        any(character in str(lexical_python) for character in "\x00\r\n")
        or not resolved_python.is_file()
        or not os.access(lexical_python, os.X_OK)
        or _stat_identity(lexical_python.lstat()) != _stat_identity(before)
    ):
        raise ConfigurationError("JARVIS execution interpreter is not executable")
    invocation = "from jarvis_cd.core.cli import main; raise SystemExit(main())"
    return (
        f'#!/bin/sh\nexec {shlex.quote(str(lexical_python))} -c {shlex.quote(invocation)} "$@"\n'
    ).encode()


def write_jarvis_wrapper(path: Path, execution_python: Path) -> dict[str, object]:
    """Create and fsync one exclusive relay-owned JARVIS launcher."""
    payload = jarvis_wrapper_payload(execution_python)
    try:
        parent_details = path.parent.lstat()
        parent_identity = (
            parent_details.st_dev,
            parent_details.st_ino,
            parent_details.st_mode,
        )
        if path.parent.is_symlink() or not path.parent.is_dir():
            raise ConfigurationError("JARVIS wrapper parent is not an owned directory")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW
        descriptor = os.open(path, flags, 0o755)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if _FCHMOD is not None:
                _FCHMOD(descriptor, 0o755)
            else:
                os.chmod(path, 0o755)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        parent_after = path.parent.lstat()
        if (parent_after.st_dev, parent_after.st_ino, parent_after.st_mode) != parent_identity:
            raise ConfigurationError("JARVIS wrapper parent changed during creation")
        _fsync_directory(path.parent)
    except BaseException:
        with suppress(OSError):
            path.unlink(missing_ok=True)
        raise
    return {
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "execution_python": str(execution_python.resolve(strict=True)),
    }


def inspect_exact_bootstrap_noop(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    service_was_active: bool | None,
    service_was_enabled: bool | None = None,
    queue_evidence: dict[str, object] | None,
    worker_evidence: dict[str, object] | None,
    installation_snapshot: dict[str, object] | None = None,
) -> BootstrapInspection:
    """Verify that the exact desired deployment is live without mutating it.

    The caller obtains systemd state and invokes the bounded queue/worker read
    commands.  No scheduler command is part of this contract.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    reasons: list[str] = []
    receipt_path = lexical_home / ".local/share/clio-relay/install-receipt.json"
    install_receipt_sha256: str | None = None
    info: dict[str, object] | None = installation_snapshot
    try:
        install_receipt_sha256 = sha256_file(receipt_path)
        if info is None:
            info = installation_info(receipt_path)
    except (ConfigurationError, OSError, ValueError) as exc:
        reasons.append(f"installation identity did not verify: {exc}")
    if info is not None:
        _inspect_installation_identity(desired, info, reasons)
    active_generation, current_generation_target = _inspect_active_generation(
        desired,
        home=lexical_home,
        installation=info,
        reasons=reasons,
    )

    _verify_binary(
        lexical_home / ".local/bin/frpc",
        desired.frpc_sha256,
        label="frpc",
        reasons=reasons,
    )
    _verify_binary(
        lexical_home / ".local/bin/frps",
        desired.frps_sha256,
        label="frps",
        reasons=reasons,
    )
    _verify_uv(lexical_home / ".local/bin/uv", desired=desired, reasons=reasons)
    jarvis_state = inspect_jarvis_state(desired, home=lexical_home)
    if not jarvis_state.initialized:
        reasons.append("JARVIS is not initialized")
    if not jarvis_state.managed_repo_registered:
        reasons.append("the exact relay-managed JARVIS repository is not registered")
    if not jarvis_state.managed_builtin_repo_registered:
        reasons.append("the exact JARVIS-managed builtin repository slot is not registered")

    queue_ready = _queue_readiness_verified(queue_evidence)
    if not queue_ready:
        reasons.append("queue migration readiness did not verify")
    worker_ready: bool | None
    if desired.worker_service is None:
        worker_ready = None
    elif service_was_active is False:
        worker_ready = False
        reasons.append("managed endpoint service is inactive")
    elif service_was_active is True:
        worker_ready = _worker_readiness_verified(worker_evidence, desired.cluster)
        if not worker_ready:
            reasons.append("active endpoint worker readiness did not verify")
    else:
        worker_ready = None
        reasons.append("managed endpoint service state was not observed")
    if desired.worker_service is not None:
        if service_was_enabled is False:
            reasons.append("managed endpoint service is disabled")
        elif service_was_enabled is None:
            reasons.append("managed endpoint service enabled state was not observed")

    return BootstrapInspection(
        exact_match=not reasons,
        desired_fingerprint=desired.fingerprint,
        reasons=reasons,
        install_receipt_sha256=install_receipt_sha256,
        active_generation=active_generation,
        current_generation_target=current_generation_target,
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=service_was_active,
            service_was_enabled=service_was_enabled,
            queue_ready=queue_ready,
            queue=queue_evidence,
            worker_ready=worker_ready,
            worker=worker_evidence,
        ),
    )


def inspect_jarvis_state(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> JarvisStateEvidence:
    """Validate initialized JARVIS roots and hash operator-owned state read-only."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    jarvis_root = _expand_home(desired.jarvis_root, lexical_home)
    config_file = jarvis_root / "jarvis_config.yaml"
    repos_file = jarvis_root / "repos.yaml"
    resource_graph_file = jarvis_root / "resource_graph.yaml"
    state_files = (config_file, repos_file, resource_graph_file)
    metadata: list[os.stat_result | None] = []
    for path in state_files:
        try:
            details = path.lstat()
        except FileNotFoundError:
            details = None
        except OSError as exc:
            raise ConfigurationError(f"could not classify JARVIS state: {path}") from exc
        metadata.append(details)
    existing = [details is not None for details in metadata]
    if not any(existing):
        return JarvisStateEvidence(initialized=False, root=str(jarvis_root))
    if not all(existing):
        raise ConfigurationError(
            "JARVIS state is partially initialized; refusing bootstrap mutation"
        )
    typed_metadata = [cast(os.stat_result, details) for details in metadata]
    if any(
        not stat.S_ISREG(details.st_mode) or details.st_size < 1 or details.st_size > maximum
        for details, maximum in zip(
            typed_metadata,
            (MAX_JARVIS_CONFIG_BYTES, MAX_JARVIS_REPOS_BYTES, MAX_JARVIS_GRAPH_BYTES),
            strict=True,
        )
    ):
        raise ConfigurationError("JARVIS state must contain three bounded regular files")
    file_ids = [(details.st_dev, details.st_ino) for details in typed_metadata]
    if len(set(file_ids)) != len(file_ids):
        raise ConfigurationError("JARVIS state files must not share one file identity")

    raw_config = _read_regular_bounded(config_file, maximum=MAX_JARVIS_CONFIG_BYTES)
    raw_repos = _read_regular_bounded(repos_file, maximum=MAX_JARVIS_REPOS_BYTES)
    raw_graph = _read_regular_bounded(resource_graph_file, maximum=MAX_JARVIS_GRAPH_BYTES)
    config = _yaml_mapping(raw_config, label="JARVIS configuration")
    repos = _yaml_mapping(raw_repos, label="JARVIS repositories")
    observed_roots: dict[str, str] = {}
    for field in ("config_dir", "private_dir", "shared_dir"):
        observed = config.get(field)
        if not isinstance(observed, str) or not observed:
            raise ConfigurationError(f"JARVIS configuration omitted {field}")
        try:
            observed_path = Path(observed).expanduser()
            if not observed_path.is_absolute():
                raise ConfigurationError(f"JARVIS {field} is not absolute")
            normalized_path = observed_path.resolve(strict=True)
            if not normalized_path.is_dir():
                raise ConfigurationError(f"JARVIS {field} is not a directory")
            normalized = str(normalized_path)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"JARVIS {field} is invalid") from exc
        observed_roots[field] = normalized
    raw_repo_values = repos.get("repos")
    typed_repo_values = (
        cast(list[object], raw_repo_values) if isinstance(raw_repo_values, list) else []
    )
    if not isinstance(raw_repo_values, list) or any(
        not isinstance(value, str) or not value for value in typed_repo_values
    ):
        raise ConfigurationError("JARVIS repositories must contain a string list")
    managed_repo_path = _expand_home(desired.managed_jarvis_repo, lexical_home)
    managed_repo = str(_canonical_path_preserving_final(managed_repo_path))
    managed_aliases = {str(managed_repo_path.absolute()), managed_repo}
    managed_builtin_path = jarvis_root / "builtin"
    managed_builtin = str(_canonical_path_preserving_final(managed_builtin_path))
    managed_builtin_aliases = {
        str(Path(os.path.abspath(managed_builtin_path.expanduser()))),
        managed_builtin,
    }
    repo_values = cast(list[str], raw_repo_values)
    managed_matches = [value for value in repo_values if value in managed_aliases]
    if len(managed_matches) > 1:
        raise ConfigurationError(
            "relay-managed JARVIS repository is registered through multiple path aliases"
        )
    managed_builtin_matches = [value for value in repo_values if value in managed_builtin_aliases]
    if len(managed_builtin_matches) > 1:
        raise ConfigurationError(
            "JARVIS-managed builtin repository is registered through multiple path aliases"
        )
    return JarvisStateEvidence(
        initialized=True,
        root=str(jarvis_root),
        roots=observed_roots,
        config_sha256=hashlib.sha256(raw_config).hexdigest(),
        repos_sha256=hashlib.sha256(raw_repos).hexdigest(),
        resource_graph_sha256=hashlib.sha256(raw_graph).hexdigest(),
        managed_repo_registered=len(managed_matches) == 1,
        managed_builtin_repo_registered=len(managed_builtin_matches) == 1,
    )


def inspect_prepared_generation(
    desired: BootstrapDesiredState,
    *,
    generation: Path,
    legacy_execution_identity: dict[str, object],
) -> dict[str, object]:
    """Reverify a content-addressed generation before any activation fence."""
    resolved_generation = generation.resolve(strict=True)
    if generation.is_symlink() or not generation.is_dir():
        raise ConfigurationError("prepared generation is not one owned directory")
    prepared_path = generation / ".prepared"
    manifest_path = generation / "manifest.json"
    receipt_path = generation / "install-receipt.json"
    prepared = _read_regular_bounded(prepared_path, maximum=1024)
    if prepared != (desired.fingerprint + "\n").encode("ascii"):
        raise ConfigurationError("prepared generation marker fingerprint changed")
    raw_manifest = _read_regular_bounded(manifest_path, maximum=4 * 1024 * 1024)
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("prepared generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("prepared generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    if set(manifest) != {
        "schema_version",
        "fingerprint",
        "plan",
        "legacy_execution_identity",
        "active_execution_identity",
        "jarvis_wrapper_sha256",
        "install_receipt",
        "install_receipt_sha256",
    }:
        raise ConfigurationError("prepared generation manifest has an unknown shape")
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest.get("legacy_execution_identity") == legacy_execution_identity
        and manifest.get("install_receipt") == str(receipt_path)
        and manifest.get("install_receipt_sha256") == sha256_file(receipt_path)
    ):
        raise ConfigurationError("prepared generation manifest identity changed")
    plan = manifest.get("plan")
    if (
        not isinstance(plan, dict)
        or cast(dict[str, object], plan).get("desired_fingerprint") != desired.fingerprint
    ):
        raise ConfigurationError("prepared generation plan identity changed")
    raw_active_identity = manifest.get("active_execution_identity")
    if not isinstance(raw_active_identity, dict):
        raise ConfigurationError("prepared generation omitted active execution identity")
    active_identity = cast(dict[str, object], raw_active_identity)
    raw_active_root = active_identity.get("root")
    raw_executables = active_identity.get("executables")
    if not isinstance(raw_active_root, str) or not isinstance(raw_executables, dict):
        raise ConfigurationError("prepared generation omitted active execution boundary")
    typed_executables = cast(dict[str, object], raw_executables)
    if set(typed_executables) != {"python", "jarvis"}:
        raise ConfigurationError("prepared generation active executable set changed")
    raw_python = typed_executables.get("python")
    raw_jarvis = typed_executables.get("jarvis")
    raw_python_path = (
        cast(dict[str, object], raw_python).get("lexical_path")
        if isinstance(raw_python, dict)
        else None
    )
    raw_jarvis_path = (
        cast(dict[str, object], raw_jarvis).get("lexical_path")
        if isinstance(raw_jarvis, dict)
        else None
    )
    if not isinstance(raw_python_path, str) or not isinstance(raw_jarvis_path, str):
        raise ConfigurationError("prepared generation omitted active interpreter identity")
    recomputed_active_identity = execution_environment_identity(
        Path(raw_active_root),
        executables={
            "python": Path(raw_python_path),
            "jarvis": Path(raw_jarvis_path),
        },
    )
    if recomputed_active_identity != active_identity:
        raise ConfigurationError("prepared generation active execution identity changed")
    jarvis_payload = jarvis_wrapper_payload(Path(raw_python_path))
    jarvis_wrapper = generation / "bin/jarvis"
    wrapper_bytes = _read_regular_bounded(jarvis_wrapper, maximum=64 * 1024)
    wrapper_sha256 = hashlib.sha256(wrapper_bytes).hexdigest()
    if (
        wrapper_bytes != jarvis_payload
        or manifest.get("jarvis_wrapper_sha256") != wrapper_sha256
        or not os.access(jarvis_wrapper, os.X_OK)
    ):
        raise ConfigurationError("prepared generation JARVIS wrapper identity changed")

    info = installation_info(receipt_path)
    receipt = info.get("receipt")
    runtime = info.get("component_runtime")
    if not isinstance(receipt, dict) or not isinstance(runtime, dict):
        raise ConfigurationError("prepared generation omitted runtime identity")
    typed_receipt = cast(dict[str, object], receipt)
    typed_runtime = cast(dict[str, object], runtime)
    receipt_checks = {
        "receipt_matches_install": info.get("receipt_matches_install") is True,
        "deployment_fingerprint": (
            typed_receipt.get("deployment_fingerprint") == desired.fingerprint
        ),
        "deployment_manifest": (
            typed_receipt.get("deployment_manifest") == desired.model_dump(mode="json")
        ),
        "generation": typed_receipt.get("generation") == desired.fingerprint,
    }
    failed_receipt_checks = sorted(
        name for name, verified in receipt_checks.items() if not verified
    )
    if failed_receipt_checks:
        raise ConfigurationError(
            "prepared generation install receipt identity changed: "
            + ", ".join(failed_receipt_checks)
        )
    raw_artifacts = typed_receipt.get("component_artifacts")
    raw_jarvis_artifact = (
        cast(dict[str, object], raw_artifacts).get("jarvis-cd")
        if isinstance(raw_artifacts, dict)
        else None
    )
    raw_interpreters = (
        cast(dict[str, object], raw_jarvis_artifact).get("runtime_interpreters")
        if isinstance(raw_jarvis_artifact, dict)
        else None
    )
    receipt_execution_python = (
        cast(dict[str, object], raw_interpreters).get("execution")
        if isinstance(raw_interpreters, dict)
        else None
    )
    if (
        not isinstance(receipt_execution_python, str)
        or receipt_execution_python != raw_python_path
        or not Path(receipt_execution_python).is_absolute()
        or os.path.normpath(receipt_execution_python) != receipt_execution_python
        or any(character in receipt_execution_python for character in "\x00\r\n")
    ):
        raise ConfigurationError(
            "prepared active JARVIS interpreter is not bound to its install receipt"
        )
    relay_runtime = typed_runtime.get("clio-relay")
    clio_kit_runtime = typed_runtime.get("clio-kit")
    jarvis_runtime = typed_runtime.get("jarvis-cd")
    if not (
        isinstance(relay_runtime, dict)
        and cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is True
        and cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is True
        and isinstance(clio_kit_runtime, dict)
        and cast(dict[str, object], clio_kit_runtime).get("persistent_tool_verified") is True
        and cast(dict[str, object], clio_kit_runtime).get("native_execution_capability_verified")
        is True
        and isinstance(jarvis_runtime, dict)
        and cast(dict[str, object], jarvis_runtime).get("verified") is True
    ):
        raise ConfigurationError("prepared generation runtime identity changed")
    launcher_targets: dict[str, str] = {}
    for name in ("clio-relay", "clio-kit"):
        launcher = generation / "bin" / name
        try:
            before = launcher.lstat()
            if not launcher.is_symlink():
                raise ConfigurationError(f"prepared generation launcher is invalid: {name}")
            target = launcher.resolve(strict=True)
            if not target.is_file() or not os.access(target, os.X_OK):
                raise ConfigurationError(f"prepared generation launcher is invalid: {name}")
            if _stat_identity(launcher.lstat()) != _stat_identity(before):
                raise ConfigurationError(f"prepared generation launcher changed: {name}")
        except OSError as exc:
            raise ConfigurationError(f"prepared generation launcher is invalid: {name}") from exc
        launcher_targets[name] = str(target)
    launcher_targets["jarvis"] = str(jarvis_wrapper)
    if resolved_generation != generation.resolve(strict=True):
        raise ConfigurationError("prepared generation changed during inspection")
    return {
        "fingerprint": desired.fingerprint,
        "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "install_receipt_sha256": sha256_file(receipt_path),
        "launcher_targets": launcher_targets,
    }


def finish_staged_activation(
    desired: BootstrapDesiredState,
    *,
    generation: Path,
    expected_manifest_sha256: str,
    home: Path | None = None,
) -> dict[str, object]:
    """Reverify and idempotently finish activation plus exact repo migration."""
    try:
        _require_sha256(expected_manifest_sha256, field="expected_manifest_sha256")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    raw_manifest = _read_regular_bounded(generation / "manifest.json", maximum=4 * 1024 * 1024)
    if hashlib.sha256(raw_manifest).hexdigest() != expected_manifest_sha256:
        raise ConfigurationError("prepared generation manifest changed before activation")
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("prepared generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("prepared generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    try:
        plan = BootstrapReconcilePlan.model_validate(manifest.get("plan"))
    except ValueError as exc:
        raise ConfigurationError("prepared generation reconcile plan is invalid") from exc
    if plan.desired_fingerprint != desired.fingerprint or plan.mode not in {
        "relay-only",
        "component-upgrade",
    }:
        raise ConfigurationError("prepared generation reconcile plan changed")
    legacy_venv = plan.reusable_paths.get("jarvis_execution_environment")
    legacy_python = plan.reusable_paths.get("jarvis_execution_python")
    legacy_jarvis = plan.reusable_paths.get("jarvis_execution_executable")
    if not all(
        isinstance(value, str) and value for value in (legacy_venv, legacy_python, legacy_jarvis)
    ):
        raise ConfigurationError("prepared generation omitted its legacy execution boundary")
    assert legacy_venv is not None
    assert legacy_python is not None
    assert legacy_jarvis is not None
    legacy_identity = execution_environment_identity(
        Path(legacy_venv),
        executables={"python": Path(legacy_python), "jarvis": Path(legacy_jarvis)},
    )
    if manifest.get("legacy_execution_identity") != legacy_identity:
        raise ConfigurationError("legacy execution environment changed before activation")
    inspection = inspect_prepared_generation(
        desired,
        generation=generation,
        legacy_execution_identity=legacy_identity,
    )
    if inspection.get("manifest_sha256") != expected_manifest_sha256:
        raise ConfigurationError("prepared generation inspection did not bind its manifest")
    activation = reconcile_staged_activation_links(plan, generation=generation, home=home)
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    managed_repo = _expand_home(desired.managed_jarvis_repo, lexical_home)
    legacy_managed_repo = _expand_home(LEGACY_MANAGED_JARVIS_REPO_PATH, lexical_home)
    previous_repo = lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay"
    relay_owned_builtin_repos = _relay_owned_jarvis_builtin_repositories(
        home=lexical_home,
        execution_environments=(Path(legacy_venv), generation / "jarvis-venv"),
    )
    repositories = reconcile_managed_jarvis_repository(
        _expand_home(desired.jarvis_root, lexical_home) / "repos.yaml",
        managed_repo,
        managed_builtin_repo=_expand_home(desired.jarvis_root, lexical_home) / "builtin",
        previous_managed_repos=(
            legacy_managed_repo,
            previous_repo,
            *relay_owned_builtin_repos,
        ),
        exchange_identity=desired.fingerprint,
    )
    expected_managed_target = (
        lexical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
    )
    _verify_stable_symlink(
        managed_repo,
        expected=expected_managed_target,
        label="relay-managed repository",
    )
    canonical_home = lexical_home.resolve(strict=True)
    reported_managed_repo = _expand_home(desired.managed_jarvis_repo, canonical_home)
    reported_managed_target = (
        canonical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
    )
    actions = activation.get("actions")
    if not isinstance(actions, dict):  # pragma: no cover - produced above
        raise ConfigurationError("staged activation omitted link actions")
    return {
        "schema_version": "clio-relay.bootstrap-staged-activation.v1",
        "prepared_inspection": inspection,
        "activation": activation,
        "jarvis_repository": {
            "link_action": cast(dict[str, object], actions).get("managed_repo"),
            "link": str(reported_managed_repo),
            "target": str(reported_managed_target),
            "repositories": repositories,
        },
    }


def prove_bootstrap_replacement_provider(
    desired: BootstrapDesiredState,
    *,
    uv_executable: Path,
    tool_executable: Path,
    source_artifact: Path,
    tool_directory: Path,
    tool_bin_directory: Path,
    preparing_root: Path,
    extracted_source_root: Path,
    source_archive_sha256: str,
    expected_provider_interpreter_sha256: str | None = None,
    home: Path | None = None,
) -> BootstrapReplacementProviderEvidence:
    """Attest the normally imported candidate wheel before legacy planning."""
    from clio_relay.installation import probe_persistent_uv_tool_identity

    if not _is_sha256(source_archive_sha256):
        raise ConfigurationError("candidate source archive SHA-256 is invalid")
    try:
        distribution_version = desired.relay_install_spec.removeprefix("clio-relay==")
    except AttributeError as exc:  # pragma: no cover - typed as str by pydantic
        raise ConfigurationError("candidate relay install spec is invalid") from exc
    if (
        not distribution_version
        or desired.relay_install_spec != f"clio-relay=={distribution_version}"
        or desired.relay_artifact_sha256 is None
    ):
        raise ConfigurationError(
            "retained-state replacement requires one exact released clio-relay wheel"
        )
    probed_identity = probe_persistent_uv_tool_identity(
        uv_executable=str(uv_executable),
        tool_executable=str(tool_executable),
        provider_interpreter=str(Path(sys.executable).absolute()),
        source_artifact=source_artifact,
        distribution="clio-relay",
        distribution_version=distribution_version,
        entry_point="clio-relay",
        tool_directory=str(tool_directory),
        tool_bin_directory=str(tool_bin_directory),
        expected_uv_executable_sha256=desired.uv_sha256,
        expected_provider_interpreter_sha256=expected_provider_interpreter_sha256,
    )
    identity = BootstrapPersistentUvToolIdentity.model_validate(
        probed_identity.model_dump(mode="json")
    )
    evidence = BootstrapReplacementProviderEvidence(
        desired_fingerprint=desired.fingerprint,
        relay_install_spec=desired.relay_install_spec,
        preparing_root=str(Path(os.path.abspath(preparing_root.expanduser()))),
        extracted_source_root=str(Path(os.path.abspath(extracted_source_root.expanduser()))),
        source_archive_sha256=source_archive_sha256,
        coordinator_provider_sha256=expected_provider_interpreter_sha256,
        persistent_tool=identity,
    )
    _verify_bootstrap_replacement_provider(desired, evidence, home=home)
    return evidence


def _verify_bootstrap_replacement_provider(
    desired: BootstrapDesiredState,
    evidence: BootstrapReplacementProviderEvidence,
    *,
    home: Path | None = None,
) -> None:
    """Re-probe a staged candidate and bind it to this process and desired state."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    try:
        parent_lexical = lexical_home / ".local/share/clio-relay/preparing"
        parent_details = parent_lexical.lstat()
        expected_parent = parent_lexical.resolve(strict=True)
        root_lexical = Path(evidence.preparing_root)
        root_details = root_lexical.lstat()
        root = root_lexical.resolve(strict=True)
        source_lexical = Path(evidence.extracted_source_root)
        source_details = source_lexical.lstat()
        source_root = source_lexical.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement root is unavailable") from exc
    if (
        parent_lexical.is_symlink()
        or not stat.S_ISDIR(parent_details.st_mode)
        or (os.name != "nt" and stat.S_IMODE(parent_details.st_mode) & 0o022)
        or (_GETUID is not None and parent_details.st_uid != _GETUID())
    ):
        raise ConfigurationError("candidate replacement parent is not private")
    if (
        not root_lexical.is_absolute()
        or ".." in root_lexical.parts
        or root_lexical.is_symlink()
        or not stat.S_ISDIR(root_details.st_mode)
        or root_lexical.name != "active"
        or root.parent != expected_parent
        or (os.name != "nt" and stat.S_IMODE(root_details.st_mode) & 0o077)
        or (_GETUID is not None and root_details.st_uid != _GETUID())
    ):
        raise ConfigurationError("candidate replacement root is not owner-private")
    if (
        source_lexical.is_symlink()
        or not stat.S_ISDIR(source_details.st_mode)
        or source_root == root
        or not source_root.is_relative_to(root)
        or not source_root.is_dir()
    ):
        raise ConfigurationError("candidate extracted source escaped its preparing root")

    identity = evidence.persistent_tool
    if (
        evidence.desired_fingerprint != desired.fingerprint
        or evidence.relay_install_spec != desired.relay_install_spec
        or not _is_sha256(evidence.source_archive_sha256)
        or desired.relay_artifact_sha256 is None
        or identity.distribution.lower().replace("_", "-") != "clio-relay"
        or desired.relay_install_spec != f"clio-relay=={identity.distribution_version}"
        or identity.entry_point != "clio-relay"
        or identity.source_artifact_sha256 != desired.relay_artifact_sha256
        or identity.uv_version != desired.uv_version
        or identity.uv_executable_sha256 != desired.uv_sha256
        or (
            evidence.coordinator_provider_sha256 is not None
            and evidence.coordinator_provider_sha256 != identity.provider_interpreter_sha256
        )
    ):
        raise ConfigurationError("candidate replacement identity does not match desired state")
    current_provider = Path(sys.executable).absolute()
    try:
        if Path(identity.provider_interpreter).absolute() != current_provider or Path(
            identity.provider_interpreter
        ).resolve(strict=True) != current_provider.resolve(strict=True):
            raise ConfigurationError("candidate planner is not running under its attested provider")
        expected_uv_lexical = root_lexical / "pinned-uv"
        expected_uv_details = expected_uv_lexical.lstat()
        expected_uv = expected_uv_lexical.resolve(strict=True)
        observed_uv = Path(identity.uv_executable).resolve(strict=True)
        if (
            expected_uv_lexical.is_symlink()
            or not stat.S_ISREG(expected_uv_details.st_mode)
            or expected_uv.parent != root
            or expected_uv != observed_uv
        ):
            raise ConfigurationError(
                "candidate replacement did not use the pinned uv executable: "
                f"expected={expected_uv}, observed={observed_uv}, root={root}, "
                f"lexical_symlink={expected_uv_lexical.is_symlink()}, "
                f"regular={stat.S_ISREG(expected_uv_details.st_mode)}"
            )
        environment = Path(identity.environment_prefix).resolve(strict=True)
        imported_module = Path(__file__).resolve(strict=True)
        provider_target = current_provider.resolve(strict=True)
        base_prefix = Path(sys.base_prefix).resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement runtime path is unavailable") from exc
    if (
        os.name != "nt"
        and provider_target != base_prefix
        and not provider_target.is_relative_to(base_prefix)
    ):
        raise ConfigurationError("candidate provider target escaped its Python base prefix")
    staged_paths = (
        identity.tool_directory,
        identity.tool_bin_directory,
        identity.environment_prefix,
        identity.provider_interpreter,
        identity.tool_executable,
        identity.tool_executable_resolved,
        identity.distribution_console_script_path,
        identity.uv_receipt_path,
        identity.distribution_metadata_path,
        identity.source_artifact_path,
        identity.record_path,
    )
    try:
        escaped_paths: list[dict[str, str]] = []
        for value in staged_paths:
            lexical = Path(os.path.abspath(Path(value).expanduser()))
            located = lexical.parent.resolve(strict=True) / lexical.name
            if (
                not lexical.is_absolute()
                or ".." in lexical.parts
                or located == root
                or not located.is_relative_to(root)
                or not lexical.exists()
            ):
                escaped_paths.append(
                    {
                        "lexical": str(lexical)[:512],
                        "located": str(located)[:512],
                    }
                )
        if escaped_paths:
            raise ConfigurationError(
                "candidate replacement runtime escaped its preparing root: "
                + json.dumps(escaped_paths[:16], sort_keys=True, separators=(",", ":"))
            )
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("candidate replacement runtime path is unavailable") from exc
    if imported_module == environment or not imported_module.is_relative_to(environment):
        raise ConfigurationError("candidate planner module was not imported from its uv tool")
    from clio_relay.installation import probe_persistent_uv_tool_identity

    probed_observed = probe_persistent_uv_tool_identity(
        uv_executable=identity.uv_executable,
        tool_executable=identity.tool_executable,
        provider_interpreter=identity.provider_interpreter,
        source_artifact=Path(identity.source_artifact_path),
        distribution="clio-relay",
        distribution_version=identity.distribution_version,
        entry_point="clio-relay",
        tool_directory=identity.tool_directory,
        tool_bin_directory=identity.tool_bin_directory,
        expected_uv_executable_sha256=desired.uv_sha256,
        expected_provider_interpreter_sha256=evidence.coordinator_provider_sha256,
    )
    observed = BootstrapPersistentUvToolIdentity.model_validate(
        probed_observed.model_dump(mode="json")
    )
    if observed != identity:
        raise ConfigurationError("candidate replacement runtime changed during attestation")


def plan_bootstrap_reconcile(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    replacement_provider: BootstrapReplacementProviderEvidence | None = None,
) -> BootstrapReconcilePlan:
    """Plan a relay-only generation when every non-relay component verifies.

    This deliberately supports the first upgrade from a pre-generation install:
    an older receipt need not contain a deployment manifest, but every reusable
    component must have exact artifact and live-runtime evidence.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    resolved_home = lexical_home.resolve()
    reasons: list[str] = []
    upgrade_reasons: list[str] = []
    upgrade_components: set[str] = set()
    reusable_paths: dict[str, str] = {}
    receipt_path = resolved_home / ".local/share/clio-relay/install-receipt.json"
    replacement_verified = False
    if replacement_provider is not None:
        try:
            _verify_bootstrap_replacement_provider(
                desired,
                replacement_provider,
                home=lexical_home,
            )
        except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
            return _full_plan(desired, f"candidate replacement provider did not verify: {exc}")
        replacement_verified = True
    try:
        info = installation_info(receipt_path)
    except (ConfigurationError, OSError, ValueError) as exc:
        return _full_plan(desired, f"installation identity did not verify: {exc}")
    if info.get("receipt_matches_install") is not True and not replacement_verified:
        return _full_plan(desired, "install receipt does not match the running relay")
    raw_receipt = info.get("receipt")
    raw_runtime = info.get("component_runtime")
    if not isinstance(raw_receipt, dict) or not isinstance(raw_runtime, dict):
        return _full_plan(desired, "installation identity omitted component evidence")
    receipt = cast(dict[str, object], raw_receipt)
    runtime = cast(dict[str, object], raw_runtime)
    relay_runtime = runtime.get("clio-relay")
    if not replacement_verified and (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is not True
        or cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True
    ):
        relay_runtime_error = (
            cast(dict[str, object], relay_runtime).get("error")
            if isinstance(relay_runtime, dict)
            else None
        )
        reason = "clio-relay live provider is not reusable"
        if isinstance(relay_runtime_error, str) and relay_runtime_error:
            reason += ": " + relay_runtime_error[:512]
        return _full_plan(desired, reason)
    raw_components = receipt.get("components")
    raw_artifacts = receipt.get("component_artifacts")
    if not isinstance(raw_components, dict) or not isinstance(raw_artifacts, dict):
        return _full_plan(desired, "install receipt omitted reusable component artifacts")
    components = cast(dict[str, object], raw_components)
    artifacts = cast(dict[str, object], raw_artifacts)
    raw_relay_artifact = artifacts.get("clio-relay")
    relay_executable = None
    if isinstance(raw_relay_artifact, dict):
        raw_relay_executables = cast(dict[str, object], raw_relay_artifact).get(
            "runtime_executables"
        )
        if isinstance(raw_relay_executables, dict):
            relay_executable = cast(dict[str, object], raw_relay_executables).get("clio-relay")
    expected_relay_executable = lexical_home / ".local/bin/clio-relay"
    if not replacement_verified and (
        not isinstance(relay_executable, str) or relay_executable != str(expected_relay_executable)
    ):
        return _full_plan(desired, "clio-relay launcher is not bound to its install receipt")
    relay_execution_reusable = False
    resolved_relay_artifact: Path | None = None
    if isinstance(raw_relay_artifact, dict):
        relay_artifact = cast(dict[str, object], raw_relay_artifact)
        relay_artifact_path = relay_artifact.get("runtime_artifact_path")
        relay_artifact_sha256 = relay_artifact.get("artifact_sha256")
        relay_execution_runtime = runtime.get("clio-relay")
        if (
            isinstance(relay_artifact_path, str)
            and isinstance(relay_artifact_sha256, str)
            and desired.relay_artifact_sha256 is not None
            and relay_artifact.get("install_spec") == desired.relay_install_spec
            and relay_artifact_sha256 == desired.relay_artifact_sha256
            and isinstance(relay_execution_runtime, dict)
            and cast(dict[str, object], relay_execution_runtime).get("execution_runtime_verified")
            is True
        ):
            try:
                lexical_relay_artifact = Path(relay_artifact_path).expanduser()
                relay_artifact_before = lexical_relay_artifact.lstat()
                resolved_relay_artifact = lexical_relay_artifact.resolve(strict=True)
                relay_execution_reusable = (
                    not lexical_relay_artifact.is_symlink()
                    and resolved_relay_artifact.is_file()
                    and sha256_file(resolved_relay_artifact) == relay_artifact_sha256
                    and _stat_identity(lexical_relay_artifact.lstat())
                    == _stat_identity(relay_artifact_before)
                )
            except (OSError, RuntimeError, ValueError):
                relay_execution_reusable = False
            if relay_execution_reusable and resolved_relay_artifact is not None:
                reusable_paths["clio-relay_artifact"] = str(resolved_relay_artifact)
    expected_components = {
        "clio-kit": (desired.clio_kit_version, desired.clio_kit_artifact_sha256),
        "jarvis-cd": (desired.jarvis_cd_version, desired.jarvis_cd_wheel_sha256),
    }
    for component, (expected_version, expected_digest) in expected_components.items():
        raw_artifact = artifacts.get(component)
        if isinstance(raw_artifact, dict):
            artifact = cast(dict[str, object], raw_artifact)
            raw_interpreters = artifact.get("runtime_interpreters")
            raw_executables = artifact.get("runtime_executables")
            if isinstance(raw_interpreters, dict):
                for name, value in cast(dict[str, object], raw_interpreters).items():
                    if isinstance(value, str) and value:
                        reusable_paths[f"{component}_{name}_interpreter"] = value
            if isinstance(raw_executables, dict):
                for name, value in cast(dict[str, object], raw_executables).items():
                    if isinstance(value, str) and value:
                        reusable_paths[f"{component}_{name}_executable"] = value
        if components.get(component) != expected_version:
            reason = f"{component} version requires a staged upgrade"
            reasons.append(reason)
            upgrade_reasons.append(reason)
            upgrade_components.add(component)
            continue
        if not isinstance(raw_artifact, dict):
            reasons.append(f"{component} artifact identity is missing")
            continue
        artifact = cast(dict[str, object], raw_artifact)
        artifact_path = artifact.get("runtime_artifact_path")
        if artifact.get("artifact_sha256") != expected_digest or not isinstance(artifact_path, str):
            reasons.append(f"{component} artifact identity is not reusable")
            continue
        try:
            lexical_path = Path(artifact_path).expanduser()
            details = lexical_path.lstat()
            if lexical_path.is_symlink() or not lexical_path.is_file():
                raise ConfigurationError("artifact is not one regular file")
            path = lexical_path.resolve(strict=True)
            if _stat_identity(lexical_path.lstat()) != _stat_identity(details):
                raise ConfigurationError("artifact changed while resolving")
            if sha256_file(path) != expected_digest:
                raise ConfigurationError("artifact changed")
        except (ConfigurationError, OSError, RuntimeError, ValueError):
            reasons.append(f"{component} artifact did not reverify")
            continue
        reusable_paths[f"{component}_artifact"] = str(path)
    if components.get("jarvis-util") != desired.jarvis_util_commit:
        reasons.append("jarvis-util commit is not reusable")
    else:
        _verify_jarvis_util_reuse(
            resolved_home,
            desired=desired,
            reusable_paths=reusable_paths,
            reasons=reasons,
        )

    if "clio-kit" not in upgrade_components:
        clio_kit_runtime = runtime.get("clio-kit")
        if not isinstance(clio_kit_runtime, dict) or any(
            cast(dict[str, object], clio_kit_runtime).get(flag) is not True
            for flag in (
                "artifact_identity_verified",
                "command_matches_receipt",
                "locked_server_runtime_verified",
                "native_execution_capability_verified",
                "persistent_tool_verified",
            )
        ):
            reasons.append("clio-kit live runtime is not reusable")
    if "jarvis-cd" not in upgrade_components:
        jarvis_runtime = runtime.get("jarvis-cd")
        if (
            not isinstance(jarvis_runtime, dict)
            or cast(dict[str, object], jarvis_runtime).get("verified") is not True
        ):
            reasons.append("JARVIS-CD live execution runtime is not reusable")

    _verify_binary(
        resolved_home / ".local/bin/frpc",
        desired.frpc_sha256,
        label="frpc",
        reasons=reasons,
    )
    _verify_binary(
        resolved_home / ".local/bin/frps",
        desired.frps_sha256,
        label="frps",
        reasons=reasons,
    )
    _verify_uv(resolved_home / ".local/bin/uv", desired=desired, reasons=reasons)
    try:
        jarvis_state = inspect_jarvis_state(desired, home=resolved_home)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"existing JARVIS state is incompatible with bootstrap: {exc}"
        ) from exc
    if not jarvis_state.initialized:
        reasons.append("JARVIS is not initialized")
    legacy_python_text = reusable_paths.get("jarvis-cd_execution_interpreter")
    legacy_executable_text = reusable_paths.get("jarvis-cd_jarvis_executable")
    legacy_python = (
        Path(legacy_python_text).expanduser()
        if legacy_python_text is not None
        else resolved_home
        / ".local/share/clio-relay/jarvis-venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    legacy_executable = (
        Path(legacy_executable_text).expanduser()
        if legacy_executable_text is not None
        else legacy_python.parent / ("jarvis.exe" if os.name == "nt" else "jarvis")
    )
    lexical_legacy_venv = legacy_python.parent.parent
    supported_execution_roots: set[Path] = set()
    supported_legacy_venv = resolved_home / ".local/share/clio-relay/jarvis-venv"
    try:
        if supported_legacy_venv.is_dir() and not _path_is_directory_alias(supported_legacy_venv):
            supported_execution_roots.add(supported_legacy_venv.resolve(strict=True))
    except (OSError, RuntimeError, ValueError):
        pass
    managed_execution_root = _managed_generation_jarvis_environment(
        receipt,
        execution_environment=lexical_legacy_venv,
        home=resolved_home,
    )
    if managed_execution_root is not None:
        supported_execution_roots.add(managed_execution_root)
    expected_legacy_executable = legacy_python.parent / (
        "jarvis.exe" if os.name == "nt" else "jarvis"
    )
    resolved_legacy_executable: Path | None = None
    try:
        legacy_python_before = legacy_python.lstat()
        legacy_executable_before = legacy_executable.lstat()
        expected_executable_before = expected_legacy_executable.lstat()
        resolved_legacy_venv = lexical_legacy_venv.resolve(strict=True)
        resolved_legacy_python_target = legacy_python.resolve(strict=True)
        legacy_python_target_before = resolved_legacy_python_target.lstat()
        resolved_legacy_executable = legacy_executable.resolve(strict=True)
        resolved_expected_executable = expected_legacy_executable.resolve(strict=True)
        executable_target_before = resolved_expected_executable.lstat()
        executable_payload, _executable_target_identity = _read_regular_bounded_with_identity(
            resolved_expected_executable,
            maximum=1024 * 1024,
        )
        legacy_boundary_reusable = (
            lexical_legacy_venv.is_absolute()
            and ".." not in lexical_legacy_venv.parts
            and not lexical_legacy_venv.is_symlink()
            and resolved_legacy_venv in supported_execution_roots
            and legacy_python.is_file()
            and expected_legacy_executable.is_file()
            and bool(executable_payload)
            and os.access(legacy_python, os.X_OK)
            and os.access(expected_legacy_executable, os.X_OK)
            and resolved_legacy_executable == resolved_expected_executable
            and _stat_identity(legacy_python.lstat()) == _stat_identity(legacy_python_before)
            and _stat_identity(legacy_executable.lstat())
            == _stat_identity(legacy_executable_before)
            and _stat_identity(expected_legacy_executable.lstat())
            == _stat_identity(expected_executable_before)
            and _stat_identity(resolved_legacy_python_target.lstat())
            == _stat_identity(legacy_python_target_before)
            and _stat_identity(resolved_expected_executable.lstat())
            == _stat_identity(executable_target_before)
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError):
        legacy_boundary_reusable = False
    if not legacy_boundary_reusable or resolved_legacy_executable is None:
        reasons.append("legacy JARVIS execution environment is not reusable")
    else:
        reusable_paths["jarvis_execution_environment"] = str(lexical_legacy_venv)
        reusable_paths["jarvis_execution_python"] = str(legacy_python)
        reusable_paths["jarvis_execution_executable"] = str(expected_legacy_executable)

    if reasons:
        if upgrade_reasons and reasons == upgrade_reasons:
            try:
                activation_paths = _capture_reconcile_activation_paths(home=lexical_home)
            except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
                return _full_plan(desired, f"legacy activation boundary is not reusable: {exc}")
            return BootstrapReconcilePlan(
                mode="component-upgrade",
                desired_fingerprint=desired.fingerprint,
                reasons=upgrade_reasons,
                component_actions={
                    "clio-relay": "replace",
                    "jarvis-cd": "replace",
                    "jarvis-util": "reuse",
                    "clio-kit": "replace",
                    "frp": "reuse",
                    "uv": "reuse",
                },
                reusable_paths=reusable_paths,
                activation_paths=activation_paths,
            )
        return BootstrapReconcilePlan(
            mode="full",
            desired_fingerprint=desired.fingerprint,
            reasons=reasons,
            component_actions={
                "clio-relay": "replace",
                "jarvis-cd": "replace",
                "jarvis-util": "replace",
                "clio-kit": "replace",
                "frp": "replace",
                "uv": "replace",
            },
        )
    exact_install_reasons: list[str] = []
    _inspect_installation_identity(desired, info, exact_install_reasons)
    if not exact_install_reasons:
        return BootstrapReconcilePlan(
            mode="repair",
            desired_fingerprint=desired.fingerprint,
            reasons=["deployment components match; queue or worker readiness requires repair"],
            component_actions={
                "clio-relay": "reuse",
                "jarvis-cd": "reuse",
                "jarvis-util": "reuse",
                "clio-kit": "reuse",
                "frp": "reuse",
                "uv": "reuse",
            },
            reusable_paths=reusable_paths,
        )
    try:
        activation_paths = _capture_reconcile_activation_paths(home=lexical_home)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
        return _full_plan(desired, f"legacy activation boundary is not reusable: {exc}")
    if not relay_execution_reusable:
        return BootstrapReconcilePlan(
            mode="component-upgrade",
            desired_fingerprint=desired.fingerprint,
            reasons=["relay JARVIS execution runtime requires a staged replacement"],
            component_actions={
                "clio-relay": "replace",
                "jarvis-cd": "replace",
                "jarvis-util": "reuse",
                "clio-kit": "replace",
                "frp": "reuse",
                "uv": "reuse",
            },
            reusable_paths=reusable_paths,
            activation_paths=activation_paths,
        )
    return BootstrapReconcilePlan(
        mode="relay-only",
        desired_fingerprint=desired.fingerprint,
        reasons=["relay desired identity changed; all non-relay components reverified"],
        component_actions={
            "clio-relay": "replace",
            "jarvis-cd": "reuse",
            "jarvis-util": "reuse",
            "clio-kit": "reuse",
            "frp": "reuse",
            "uv": "reuse",
        },
        reusable_paths=reusable_paths,
        activation_paths=activation_paths,
    )


def _path_is_directory_alias(path: Path) -> bool:
    """Return whether a directory path is a symbolic-link or junction alias."""
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(is_junction()) if callable(is_junction) else False


def _managed_generation_jarvis_environment(
    receipt: dict[str, object],
    *,
    execution_environment: Path,
    home: Path,
) -> Path | None:
    """Return a receipt-bound relay generation's real JARVIS execution root.

    Relay-only generations intentionally retain the JARVIS environment from
    the preceding component generation.  The active receipt therefore binds
    both the active generation and an execution root that may belong to a
    different retained generation.
    """
    active_generation = receipt.get("generation")
    if not isinstance(active_generation, str) or not _is_sha256(active_generation):
        return None
    relay_root = home / ".local/share/clio-relay"
    generations_root = relay_root / "generations"
    active_generation_root = generations_root / active_generation
    current = relay_root / "current"
    environment = Path(os.path.abspath(execution_environment.expanduser()))
    try:
        current_before = current.lstat()
        generations_before = generations_root.lstat()
        active_generation_before = active_generation_root.lstat()
        environment_before = environment.lstat()
        if (
            not _path_is_directory_alias(current)
            or not stat.S_ISDIR(generations_before.st_mode)
            or _path_is_directory_alias(generations_root)
            or not stat.S_ISDIR(active_generation_before.st_mode)
            or _path_is_directory_alias(active_generation_root)
            or not stat.S_ISDIR(environment_before.st_mode)
            or _path_is_directory_alias(environment)
            or not environment.is_absolute()
            or ".." in environment.parts
        ):
            return None
        resolved_generations = generations_root.resolve(strict=True)
        resolved_active_generation = active_generation_root.resolve(strict=True)
        resolved_environment = environment.resolve(strict=True)
        if current.resolve(strict=True) != resolved_active_generation:
            return None
        relative_environment = resolved_environment.relative_to(resolved_generations)
        if (
            len(relative_environment.parts) != 2
            or relative_environment.parts[1] != "jarvis-venv"
            or not _is_sha256(relative_environment.parts[0])
        ):
            return None
        execution_generation_root = generations_root / relative_environment.parts[0]
        execution_generation_before = execution_generation_root.lstat()
        if (
            not stat.S_ISDIR(execution_generation_before.st_mode)
            or _path_is_directory_alias(execution_generation_root)
            or execution_generation_root.resolve(strict=True) != resolved_environment.parent
        ):
            return None
        current_after = current.lstat()
        generations_after = generations_root.lstat()
        active_generation_after = active_generation_root.lstat()
        execution_generation_after = execution_generation_root.lstat()
        environment_after = environment.lstat()
        if (
            _stat_identity(current_after) != _stat_identity(current_before)
            or _stat_identity(generations_after) != _stat_identity(generations_before)
            or _stat_identity(active_generation_after) != _stat_identity(active_generation_before)
            or _stat_identity(execution_generation_after)
            != _stat_identity(execution_generation_before)
            or _stat_identity(environment_after) != _stat_identity(environment_before)
        ):
            return None
        getuid = getattr(os, "getuid", None)
        if callable(getuid) and any(
            details.st_uid != getuid()
            for details in (
                generations_after,
                active_generation_after,
                execution_generation_after,
                environment_after,
            )
        ):
            return None
    except (OSError, RuntimeError, ValueError):
        return None
    return resolved_environment


def _full_plan(desired: BootstrapDesiredState, reason: str) -> BootstrapReconcilePlan:
    return BootstrapReconcilePlan(
        mode="full",
        desired_fingerprint=desired.fingerprint,
        reasons=[reason],
        component_actions={
            "clio-relay": "replace",
            "jarvis-cd": "replace",
            "jarvis-util": "replace",
            "clio-kit": "replace",
            "frp": "replace",
            "uv": "replace",
        },
    )


def _verify_jarvis_util_reuse(
    home: Path,
    *,
    desired: BootstrapDesiredState,
    reusable_paths: dict[str, str],
    reasons: list[str],
) -> None:
    checkout = home / ".local/src/jarvis-util"
    try:
        if checkout.is_symlink() or not (checkout / ".git").is_dir():
            raise ConfigurationError("jarvis-util checkout is unavailable")
        commit = _bounded_subprocess(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            maximum=4096,
        )
        status = _bounded_subprocess(
            [
                "git",
                "-C",
                str(checkout),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            maximum=1024 * 1024,
        )
        if commit != desired.jarvis_util_commit or status:
            raise ConfigurationError("jarvis-util checkout commit or cleanliness changed")
        receipt_python = reusable_paths.get("jarvis-cd_execution_interpreter")
        legacy_python = (
            Path(receipt_python).expanduser()
            if receipt_python is not None
            else home
            / ".local/share/clio-relay/jarvis-venv"
            / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        )
        probe = _bounded_subprocess(
            [
                str(legacy_python),
                "-c",
                (
                    "import json; from importlib.metadata import distribution; "
                    "d=distribution('jarvis-util'); "
                    "print(json.dumps({'name':d.metadata['Name'],"
                    "'direct_url':d.read_text('direct_url.json'),"
                    "'record':d.read_text('RECORD') is not None}))"
                ),
            ],
            maximum=1024 * 1024,
        )
        raw_probe = cast(object, json.loads(probe))
        if not isinstance(raw_probe, dict):
            raise ConfigurationError("jarvis-util distribution probe is invalid")
        evidence = cast(dict[str, object], raw_probe)
        direct_url_text = evidence.get("direct_url")
        if not isinstance(direct_url_text, str) or evidence.get("record") is not True:
            raise ConfigurationError("jarvis-util distribution omitted source evidence")
        raw_direct_url = cast(object, json.loads(direct_url_text))
        if not isinstance(raw_direct_url, dict):
            raise ConfigurationError("jarvis-util direct-url evidence is invalid")
        source_url = cast(dict[str, object], raw_direct_url).get("url")
        if not isinstance(source_url, str):
            raise ConfigurationError("jarvis-util distribution source changed")
        parsed = urlsplit(source_url)
        source_path_text = unquote(parsed.path)
        if os.name == "nt" and len(source_path_text) > 2 and source_path_text[0] == "/":
            source_path_text = source_path_text[1:]
        if (
            parsed.scheme != "file"
            or parsed.netloc
            or parsed.query
            or parsed.fragment
            or Path(source_path_text).resolve() != checkout.resolve()
        ):
            raise ConfigurationError("jarvis-util distribution source changed")
    except (ConfigurationError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        reasons.append(f"jarvis-util live installation is not reusable: {exc}")
        return
    reusable_paths["jarvis_util_checkout"] = str(checkout.resolve())


def _bounded_subprocess(command: list[str], *, maximum: int) -> str:
    """Run one identity command while retaining at most bounded output bytes."""
    if maximum < 1:
        raise ValueError("identity command output bound must be positive")
    try:
        completed = run_bounded_process(
            command,
            timeout_seconds=20,
            stdout_maximum_bytes=maximum,
            stderr_maximum_bytes=4096,
        )
    except BoundedProcessOutputLimit as exc:
        raise ConfigurationError(
            f"identity command output exceeded its bound: {command[0]}"
        ) from exc
    except (OSError, BoundedProcessError) as exc:
        raise ConfigurationError(f"identity command failed: {command[0]}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        raise ConfigurationError(
            f"identity command failed: {command[0]}" + (f": {detail}" if detail else "")
        )
    return completed.stdout.strip()


def validate_jarvis_builtin_result(
    result: dict[str, object],
    *,
    requested_profile: str,
) -> None:
    """Validate the bounded JARVIS builtin resource-graph result contract."""
    expected_fields = {
        "schema_version",
        "profile",
        "action",
        "available",
        "source",
        "source_sha256",
        "catalog",
    }
    if set(result) != expected_fields:
        raise ValueError("JARVIS builtin graph result has an unexpected shape")
    if (
        result.get("schema_version") != "jarvis.resource-graph-builtin.v1"
        or result.get("profile") != requested_profile
    ):
        raise ValueError("JARVIS builtin graph result does not match the requested profile")
    raw_catalog = result.get("catalog")
    if not isinstance(raw_catalog, list):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    catalog = cast(list[object], raw_catalog)
    if len(catalog) > 128 or any(
        not isinstance(profile, str)
        or not profile
        or len(profile) > 256
        or profile != profile.strip()
        or profile in {".", ".."}
        or "/" in profile
        or "\\" in profile
        or any(ord(character) < 32 or ord(character) == 127 for character in profile)
        for profile in catalog
    ):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    typed_catalog = cast(list[str], catalog)
    if typed_catalog != sorted(set(typed_catalog)):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    action = result.get("action")
    available = result.get("available")
    source = result.get("source")
    source_sha256 = result.get("source_sha256")
    if action == "loaded":
        if (
            available is not True
            or not isinstance(source, str)
            or not source
            or len(source) > 4096
            or not PurePosixPath(source).is_absolute()
            or any(character in source for character in "\x00\r\n")
            or not _is_sha256(source_sha256)
            or requested_profile not in typed_catalog
        ):
            raise ValueError("loaded JARVIS builtin graph evidence is invalid")
    elif action == "unavailable":
        if (
            available is not False
            or source is not None
            or source_sha256 is not None
            or requested_profile in typed_catalog
        ):
            raise ValueError("unavailable JARVIS builtin graph evidence is invalid")
    else:
        raise ValueError("JARVIS builtin graph result has an invalid action")


def _is_sha256(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def make_bootstrap_receipt(
    *,
    invocation_id: str,
    desired: BootstrapDesiredState,
    outcome: Literal[
        "noop_verified",
        "verified_after_transfer",
        "repaired",
        "reconciled",
        "full",
    ],
    inspection: BootstrapInspection,
    started_at: datetime,
    transaction: BootstrapTransactionJournal | None,
    previous_generation: str | None,
    active_generation: str | None,
    components: dict[str, dict[str, object]] | None = None,
    duration_seconds: float = 0.0,
    inspection_duration_seconds: float = 0.0,
    downloads: list[dict[str, object]] | None = None,
    service_restart_count: int = 0,
    service_start_count: int = 0,
    service_stop_count: int = 0,
    service_enable_count: int = 0,
    queue_action: Literal["verified_read_only", "audited_and_sealed"] = ("verified_read_only"),
    queue_duration_seconds: float = 0.0,
    jarvis_init_action: Literal["preserved", "initialized"] = "preserved",
    jarvis_init_duration_seconds: float = 0.0,
    jarvis_graph_action: Literal["preserved", "loaded", "built"] = "preserved",
    jarvis_graph_duration_seconds: float = 0.0,
    jarvis_builtin_result: dict[str, object] | None = None,
    jarvis_commands: list[list[str]] | None = None,
    jarvis_state_before: JarvisStateEvidence | None = None,
    jarvis_repo_reconciliation: dict[str, object] | None = None,
    initial_inspection_reasons: list[str] | None = None,
    service_active_before: bool | None = None,
    service_enabled_before: bool | None = None,
    service_active_after: bool | None = None,
    service_enabled_after: bool | None = None,
    service_pending_install: bool = False,
    payload_transfer_count: int = 0,
    payload_transfer_bytes: int = 0,
) -> dict[str, object]:
    """Build the machine-readable v2 receipt for a completed acceptance run."""
    if (
        min(
            duration_seconds,
            inspection_duration_seconds,
            queue_duration_seconds,
            jarvis_init_duration_seconds,
            jarvis_graph_duration_seconds,
        )
        < 0
    ):
        raise ValueError("bootstrap duration cannot be negative")
    if (
        min(
            service_restart_count,
            service_start_count,
            service_stop_count,
            service_enable_count,
            payload_transfer_count,
            payload_transfer_bytes,
        )
        < 0
    ):
        raise ValueError("service action counts cannot be negative")
    component_evidence = components or _default_noop_components(
        desired,
        duration_seconds=duration_seconds,
    )
    commands = jarvis_commands or []
    if any(not command or any(not value for value in command) for command in commands):
        raise ValueError("JARVIS command evidence must contain non-empty argument vectors")
    if jarvis_graph_action == "preserved":
        if jarvis_builtin_result is not None:
            raise ValueError("a preserved JARVIS graph cannot claim builtin activation")
    else:
        if desired.jarvis_resource_graph_profile is None or jarvis_builtin_result is None:
            raise ValueError("JARVIS graph activation requires exact builtin result evidence")
        validate_jarvis_builtin_result(
            jarvis_builtin_result,
            requested_profile=desired.jarvis_resource_graph_profile,
        )
        expected_builtin_action = "loaded" if jarvis_graph_action == "loaded" else "unavailable"
        if jarvis_builtin_result["action"] != expected_builtin_action:
            raise ValueError("JARVIS graph action does not match builtin activation evidence")
        if (
            jarvis_graph_action == "loaded"
            and jarvis_builtin_result["source_sha256"]
            != inspection.jarvis_state.resource_graph_sha256
        ):
            raise ValueError("loaded JARVIS graph does not match the packaged source digest")
        if jarvis_graph_action == "built" and not desired.allow_jarvis_resource_graph_build:
            raise ValueError("JARVIS graph build was not enabled by the desired state")
    before = jarvis_state_before or inspection.jarvis_state
    repo_evidence = jarvis_repo_reconciliation or {
        "link_action": "reused",
        "link": desired.managed_jarvis_repo,
        "target": None,
        "repositories": {
            "action": "reused",
            "managed_repo": None,
            "added_managed_repos": [],
            "removed_previous_managed_repos": [],
            "before_sha256": before.repos_sha256,
            "after_sha256": inspection.jarvis_state.repos_sha256,
        },
    }
    return {
        "schema_version": BOOTSTRAP_RECEIPT_SCHEMA,
        "invocation_id": invocation_id,
        "bootstrap_profile": desired.bootstrap_profile,
        "relay_install_spec": desired.relay_install_spec,
        "desired_fingerprint": desired.fingerprint,
        "outcome": outcome,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "plan": {
            "mode": "none" if outcome == "noop_verified" else "reconcile",
            "reasons": inspection.reasons,
        },
        "transaction": (transaction.model_dump(mode="json") if transaction is not None else None),
        "generation": {
            "previous": previous_generation,
            "active": active_generation,
            "current_target": inspection.current_generation_target,
        },
        "duration_seconds": duration_seconds,
        "inspection": {
            "duration_seconds": inspection_duration_seconds,
            "read_only": True,
            "initial_reasons": initial_inspection_reasons or [],
        },
        "components": component_evidence,
        "operations": {
            "downloads": downloads or [],
            "download_count": len(downloads or []),
            "service_restart_count": service_restart_count,
            "service_start_count": service_start_count,
            "service_stop_count": service_stop_count,
            "service_enable_count": service_enable_count,
            "scheduler_submission_count": 0,
            "scheduler_cancellation_count": 0,
            "generation_gc_count": 0,
            "payload_transfer_count": payload_transfer_count,
            "payload_transfer_bytes": payload_transfer_bytes,
        },
        "install_receipt_sha256": inspection.install_receipt_sha256,
        "jarvis_state": inspection.jarvis_state.model_dump(mode="json"),
        "jarvis_initialization": {
            "action": jarvis_init_action,
            "duration_seconds": jarvis_init_duration_seconds,
        },
        "jarvis_resource_graph": {
            "action": jarvis_graph_action,
            "duration_seconds": jarvis_graph_duration_seconds,
            "benchmark_enabled": False,
            "selected_profile": desired.jarvis_resource_graph_profile,
            "allow_build_fallback": desired.allow_jarvis_resource_graph_build,
            "builtin_result": jarvis_builtin_result,
        },
        "jarvis_commands": {
            "count": len(commands),
            "argv": commands,
        },
        "jarvis_preservation": {
            "before": before.model_dump(mode="json"),
            "after": inspection.jarvis_state.model_dump(mode="json"),
            "config_byte_identical": (
                before.config_sha256 == inspection.jarvis_state.config_sha256
            ),
            "resource_graph_byte_identical": (
                before.resource_graph_sha256 == inspection.jarvis_state.resource_graph_sha256
            ),
            "repositories_byte_identical": (
                before.repos_sha256 == inspection.jarvis_state.repos_sha256
            ),
            "repositories": repo_evidence,
        },
        "queue": inspection.readiness.queue,
        "queue_operation": {
            "action": queue_action,
            "duration_seconds": queue_duration_seconds,
            "records_examined": (
                inspection.readiness.queue.get("records_examined")
                if inspection.readiness.queue is not None
                else None
            ),
            "bounds": (
                inspection.readiness.queue.get("bounds")
                if inspection.readiness.queue is not None
                else None
            ),
        },
        "worker": inspection.readiness.model_dump(mode="json"),
        "service": {
            "name": desired.worker_service,
            "pending_install": service_pending_install,
            "active_before": service_active_before,
            "enabled_before": service_enabled_before,
            "active_after": (
                inspection.readiness.service_was_active
                if service_active_after is None
                else service_active_after
            ),
            "enabled_after": (
                inspection.readiness.service_was_enabled
                if service_enabled_after is None
                else service_enabled_after
            ),
        },
        "preservation": {
            "scheduler_jobs_cancelled": False,
            "old_generations_retained": True,
            "jarvis_init_on_existing_root": False,
        },
    }


def _default_noop_components(
    desired: BootstrapDesiredState,
    *,
    duration_seconds: float,
) -> dict[str, dict[str, object]]:
    identities: dict[str, object] = {
        "clio-relay": {
            "install_spec": desired.relay_install_spec,
            "artifact_sha256": desired.relay_artifact_sha256,
        },
        "clio-kit": {
            "version": desired.clio_kit_version,
            "artifact_sha256": desired.clio_kit_artifact_sha256,
        },
        "jarvis-cd": {
            "version": desired.jarvis_cd_version,
            "artifact_sha256": desired.jarvis_cd_wheel_sha256,
        },
        "jarvis-util": {"commit": desired.jarvis_util_commit},
        "frp": {
            "version": desired.frp_version,
            "frpc_sha256": desired.frpc_sha256,
            "frps_sha256": desired.frps_sha256,
        },
        "uv": {"version": desired.uv_version, "sha256": desired.uv_sha256},
    }
    return {
        name: {
            "action": "reused",
            "observed_identity": identity,
            "duration_seconds": duration_seconds,
        }
        for name, identity in identities.items()
    }


def write_bootstrap_receipt(path: Path, receipt: dict[str, object]) -> None:
    """Atomically persist one current invocation acceptance receipt."""
    _atomic_json(path, receipt)


def _relay_owned_jarvis_builtin_repositories(
    *,
    home: Path,
    execution_environments: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    """Return builtin repositories proven to belong to relay-managed JARVIS venvs.

    Old JARVIS releases registered their wheel-installed ``builtin`` package
    directly in ``repos.yaml``.  JARVIS now owns a stable repository slot, but
    that cannot identify the historical path when it lives in relay's legacy
    virtual environment.  Constrain migration to the fixed legacy venv and to
    content-addressed generation venvs, then require wheel ``METADATA`` and
    ``RECORD`` evidence proving that ``jarvis-cd`` installed the repository.
    """
    lexical_home = Path(os.path.abspath(home.expanduser()))
    resolved_home = lexical_home.resolve(strict=True)
    lexical_relay_root = lexical_home / ".local/share/clio-relay"
    resolved_relay_root = resolved_home / ".local/share/clio-relay"
    fixed_legacy = lexical_relay_root / "jarvis-venv"
    candidates = (fixed_legacy, *execution_environments)
    repositories: dict[str, Path] = {}
    seen_environments: set[str] = set()
    for candidate in candidates:
        lexical_environment = Path(os.path.abspath(candidate.expanduser()))
        lexical_identity: tuple[int, int, int, int, int, int]
        try:
            before = lexical_environment.lstat()
            if (
                not stat.S_ISDIR(before.st_mode)
                or _path_is_directory_alias(lexical_environment)
                or not lexical_environment.is_absolute()
                or ".." in lexical_environment.parts
            ):
                continue
            resolved_environment = lexical_environment.resolve(strict=True)
            fixed_environment = resolved_relay_root / "jarvis-venv"
            owned_layout = resolved_environment == fixed_environment
            if not owned_layout:
                relative = resolved_environment.relative_to(
                    (resolved_relay_root / "generations").resolve(strict=True)
                )
                owned_layout = bool(
                    len(relative.parts) == 2
                    and _is_sha256(relative.parts[0])
                    and relative.parts[1] == "jarvis-venv"
                )
            if not owned_layout:
                continue
            lexical_identity = _stat_identity(before)
            if _stat_identity(lexical_environment.lstat()) != lexical_identity:
                continue
            getuid = getattr(os, "getuid", None)
            if callable(getuid) and before.st_uid != getuid():
                continue
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            continue
        environment_key = str(resolved_environment)
        if environment_key in seen_environments:
            continue
        seen_environments.add(environment_key)
        for site_packages in _jarvis_site_package_directories(lexical_environment):
            repository = _jarvis_cd_builtin_repository(site_packages)
            if repository is not None:
                repositories[str(repository)] = repository
        if _stat_identity(lexical_environment.lstat()) != lexical_identity:
            raise ConfigurationError(
                "relay-owned JARVIS environment changed during repository reconciliation"
            )
    return tuple(repositories[key] for key in sorted(repositories))


def _jarvis_site_package_directories(environment: Path) -> tuple[Path, ...]:
    """Enumerate bounded, real site-package directories inside one proven venv."""
    candidates = [environment / "Lib/site-packages"]
    for library_name in ("lib", "lib64"):
        library = environment / library_name
        try:
            python_directories = sorted(library.glob("python*"), key=lambda path: path.name)
        except OSError:
            continue
        if len(python_directories) > 16:
            raise ConfigurationError("relay-owned JARVIS environment has too many Python roots")
        candidates.extend(path / "site-packages" for path in python_directories)
    directories: dict[str, Path] = {}
    for candidate in candidates:
        try:
            before = candidate.lstat()
            if not stat.S_ISDIR(before.st_mode) or _path_is_directory_alias(candidate):
                continue
            resolved = candidate.resolve(strict=True)
            relative = resolved.relative_to(environment.resolve(strict=True))
            parts = relative.parts
            posix_shape = bool(
                len(parts) == 3
                and parts[0] in {"lib", "lib64"}
                and _is_python_library_directory(parts[1])
                and parts[2] == "site-packages"
            )
            windows_shape = parts == ("Lib", "site-packages")
            if not posix_shape and not windows_shape:
                continue
            if _stat_identity(candidate.lstat()) != _stat_identity(before):
                continue
            directories[str(candidate)] = candidate
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            continue
    return tuple(directories[key] for key in sorted(directories))


def _is_python_library_directory(value: str) -> bool:
    """Return whether a venv library name is exactly ``python<major>.<minor>``."""
    if not value.startswith("python"):
        return False
    version = value.removeprefix("python")
    major, separator, minor = version.partition(".")
    return bool(separator and major.isdigit() and minor.isdigit())


def _jarvis_cd_builtin_repository(site_packages: Path) -> Path | None:
    """Prove that one site-packages ``builtin`` directory came from jarvis-cd."""
    builtin = site_packages / "builtin"
    try:
        builtin_before = builtin.lstat()
        if not stat.S_ISDIR(builtin_before.st_mode) or _path_is_directory_alias(builtin):
            return None
        distributions = sorted(
            site_packages.glob("jarvis_cd-*.dist-info"),
            key=lambda path: path.name,
        )
        if len(distributions) > 8:
            raise ConfigurationError("relay-owned JARVIS environment has too many distributions")
        for distribution in distributions:
            distribution_before = distribution.lstat()
            if not stat.S_ISDIR(distribution_before.st_mode) or _path_is_directory_alias(
                distribution
            ):
                continue
            metadata_payload = _read_regular_bounded(
                distribution / "METADATA",
                maximum=MAX_JARVIS_DISTRIBUTION_METADATA_BYTES,
            )
            record_payload = _read_regular_bounded(
                distribution / "RECORD",
                maximum=MAX_JARVIS_DISTRIBUTION_RECORD_BYTES,
            )
            if not _jarvis_cd_metadata(metadata_payload) or not _record_installs_jarvis_builtin(
                record_payload
            ):
                continue
            if _stat_identity(distribution.lstat()) != _stat_identity(
                distribution_before
            ) or _stat_identity(builtin.lstat()) != _stat_identity(builtin_before):
                raise ConfigurationError(
                    "relay-owned JARVIS distribution changed during repository reconciliation"
                )
            getuid = getattr(os, "getuid", None)
            if callable(getuid) and (
                distribution_before.st_uid != getuid() or builtin_before.st_uid != getuid()
            ):
                continue
            return builtin
    except (FileNotFoundError, OSError, RuntimeError, UnicodeError, ValueError):
        return None
    return None


def _jarvis_cd_metadata(payload: bytes) -> bool:
    """Return whether wheel metadata names the jarvis-cd distribution exactly."""
    for line in payload.decode("utf-8").splitlines():
        field, separator, value = line.partition(":")
        if separator and field.casefold() == "name":
            return value.strip().casefold().replace("_", "-") == "jarvis-cd"
    return False


def _record_installs_jarvis_builtin(payload: bytes) -> bool:
    """Require both repository package markers in a jarvis-cd wheel RECORD."""
    paths: set[str] = set()
    reader = csv.reader(io.StringIO(payload.decode("utf-8"), newline=""))
    for row in reader:
        if row:
            paths.add(row[0].replace("\\", "/"))
    return {"builtin/__init__.py", "builtin/builtin/__init__.py"} <= paths


def _managed_repository_payload(
    raw: bytes,
    *,
    managed: str,
    managed_aliases: set[str],
    managed_builtin: str | None,
    managed_builtin_aliases: set[str],
    previous_aliases: dict[str, str],
    previous_builtin_aliases: set[str],
) -> tuple[bytes, list[str], list[str]]:
    """Return the exact converged repository bytes and mutation evidence."""
    document = _yaml_mapping(raw, label="JARVIS repositories")
    raw_repos = document.get("repos")
    typed_repos = cast(list[object], raw_repos) if isinstance(raw_repos, list) else []
    if not isinstance(raw_repos, list) or any(
        not isinstance(value, str) or not value for value in typed_repos
    ):
        raise ConfigurationError("JARVIS repositories must contain a string list")
    repos = list(cast(list[str], raw_repos))
    managed_matches = [value for value in repos if value in managed_aliases]
    if len(managed_matches) > 1:
        raise ConfigurationError(
            "relay-managed JARVIS repository is registered through multiple path aliases"
        )
    managed_builtin_matches = [value for value in repos if value in managed_builtin_aliases]
    if len(managed_builtin_matches) > 1:
        raise ConfigurationError(
            "JARVIS-managed builtin repository is registered through multiple path aliases"
        )
    previous_matches: dict[str, list[str]] = {}
    for value in repos:
        normalized = previous_aliases.get(value)
        if normalized is not None:
            previous_matches.setdefault(normalized, []).append(value)
    if any(len(values) > 1 for values in previous_matches.values()):
        raise ConfigurationError(
            "a proven previous relay-managed JARVIS repository is registered through "
            "multiple path aliases"
        )
    removed_previous = sorted(previous_matches)
    if managed_builtin is None:
        if managed_matches == [managed] and not removed_previous:
            return raw, [], []
        updated = [
            value
            for value in repos
            if value not in managed_aliases and value not in previous_aliases
        ]
        updated.insert(0, managed)
        added_managed = [managed] if managed_matches != [managed] else []
        document["repos"] = updated
        return (
            yaml.safe_dump(document, sort_keys=False).encode("utf-8"),
            added_managed,
            removed_previous,
        )
    if (
        managed_matches == [managed]
        and managed_builtin_matches == [managed_builtin]
        and not removed_previous
    ):
        return raw, [], []
    builtin_anchor: int | None = None
    if managed_builtin_matches:
        builtin_anchor = repos.index(managed_builtin_matches[0])
    else:
        builtin_anchor = next(
            (index for index, value in enumerate(repos) if value in previous_builtin_aliases),
            None,
        )
    operator_repositories: list[str] = []
    builtin_position: int | None = None
    for index, value in enumerate(repos):
        if index == builtin_anchor:
            builtin_position = len(operator_repositories)
        if (
            value in managed_aliases
            or value in managed_builtin_aliases
            or value in previous_aliases
        ):
            continue
        operator_repositories.append(value)
    if builtin_position is None:
        builtin_position = len(operator_repositories)
    operator_repositories.insert(builtin_position, managed_builtin)
    updated = [managed, *operator_repositories]
    if repos == updated and not removed_previous:
        return raw, [], []
    added_managed: list[str] = []
    if not repos or repos[0] != managed:
        added_managed.append(managed)
    if managed_builtin_matches != [managed_builtin]:
        added_managed.append(managed_builtin)
    document["repos"] = updated
    return (
        yaml.safe_dump(document, sort_keys=False).encode("utf-8"),
        added_managed,
        removed_previous,
    )


def reconcile_managed_jarvis_repository(
    repos_file: Path,
    managed_repo: Path,
    *,
    managed_builtin_repo: Path | None = None,
    previous_managed_repos: tuple[Path, ...] = (),
    exchange_identity: str | None = None,
) -> dict[str, object]:
    """Register only the exact relay-owned repository without basename matching.

    JARVIS's public ``repo add --force`` replaces every repository with the
    same basename. Relay instead performs a compare-before-replace update of
    its exact paths and leaves operator repositories, including same-name
    repositories, untouched. ``managed_builtin_repo`` may name only JARVIS's
    exact ``<JARVIS_ROOT>/builtin`` slot. JARVIS 1.6+ rebinds that stable slot
    in memory to the builtin repository shipped by the running distribution,
    so the independently installed execution and MCP runtimes each see their
    release-pinned package contract. ``previous_managed_repos`` is a caller-supplied
    provenance boundary: each path must come from an earlier relay receipt or
    an exact relay-owned generation path. This operation is serialized by the
    bootstrap lock; the final byte-and-file-identity comparison also detects
    non-cooperating writers before the atomic replacement.
    """
    if managed_repo.name != "clio_relay":
        raise ConfigurationError(
            "relay-managed JARVIS repository basename must match its clio_relay namespace"
        )
    lexical_managed = str(Path(os.path.abspath(managed_repo.expanduser())))
    managed = str(_canonical_path_preserving_final(managed_repo))
    managed_aliases = {lexical_managed, managed}
    managed_builtin: str | None = None
    managed_builtin_aliases: set[str] = set()
    if managed_builtin_repo is not None:
        lexical_builtin_path = Path(os.path.abspath(managed_builtin_repo.expanduser()))
        expected_builtin_path = Path(os.path.abspath(repos_file.parent.expanduser())) / "builtin"
        canonical_builtin_path = _canonical_path_preserving_final(managed_builtin_repo)
        canonical_expected_builtin = _canonical_path_preserving_final(expected_builtin_path)
        if (
            lexical_builtin_path != expected_builtin_path
            or canonical_builtin_path != canonical_expected_builtin
        ):
            raise ConfigurationError(
                "JARVIS-managed builtin repository must be the exact active root slot"
            )
        managed_builtin = str(canonical_builtin_path)
        managed_builtin_aliases = {str(lexical_builtin_path), managed_builtin}
    managed_alias_union = managed_aliases | managed_builtin_aliases
    previous_aliases: dict[str, str] = {}
    previous_builtin_aliases: set[str] = set()
    for path in previous_managed_repos:
        lexical_previous = str(Path(os.path.abspath(path.expanduser())))
        try:
            canonical_previous = str(_canonical_path_preserving_final(path))
        except ConfigurationError:
            canonical_previous = lexical_previous
        if canonical_previous in managed_alias_union:
            continue
        previous_aliases[lexical_previous] = canonical_previous
        previous_aliases[canonical_previous] = canonical_previous
        if Path(canonical_previous).name == "builtin":
            previous_builtin_aliases.update({lexical_previous, canonical_previous})
    token_source = managed if managed_builtin is None else f"{managed}\0{managed_builtin}"
    token = exchange_identity or hashlib.sha256(token_source.encode("utf-8")).hexdigest()
    try:
        _require_sha256(token, field="repository_exchange_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    temporary = repos_file.with_name(f".{repos_file.name}.{token}.exchange")
    raw, before_identity = _read_regular_bounded_with_identity(
        repos_file,
        maximum=MAX_JARVIS_REPOS_BYTES,
    )
    payload, added_managed, removed_previous = _managed_repository_payload(
        raw,
        managed=managed,
        managed_aliases=managed_aliases,
        managed_builtin=managed_builtin,
        managed_builtin_aliases=managed_builtin_aliases,
        previous_aliases=previous_aliases,
        previous_builtin_aliases=previous_builtin_aliases,
    )
    if temporary.exists() or temporary.is_symlink():
        displaced, _displaced_identity = _read_regular_bounded_with_identity(
            temporary,
            maximum=MAX_JARVIS_REPOS_BYTES,
        )
        displaced_payload, displaced_added, displaced_removed = _managed_repository_payload(
            displaced,
            managed=managed,
            managed_aliases=managed_aliases,
            managed_builtin=managed_builtin,
            managed_builtin_aliases=managed_builtin_aliases,
            previous_aliases=previous_aliases,
            previous_builtin_aliases=previous_builtin_aliases,
        )
        if displaced != displaced_payload and displaced_payload == raw:
            temporary.unlink()
            _fsync_directory(repos_file.parent)
            return {
                "action": "updated",
                "managed_repo": managed,
                "added_managed_repos": displaced_added,
                "removed_previous_managed_repos": displaced_removed,
                "before_sha256": hashlib.sha256(displaced).hexdigest(),
                "after_sha256": hashlib.sha256(raw).hexdigest(),
            }
        if raw != payload and payload == displaced:
            temporary.unlink()
            _fsync_directory(repos_file.parent)
        else:
            raise ConfigurationError(
                "JARVIS repository exchange recovery found unproven path states: "
                f"{repos_file}, {temporary}"
            )
    if payload == raw:
        return {
            "action": "reused",
            "managed_repo": managed,
            "added_managed_repos": [],
            "removed_previous_managed_repos": [],
            "before_sha256": hashlib.sha256(raw).hexdigest(),
            "after_sha256": hashlib.sha256(raw).hexdigest(),
        }
    exchanged = False
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        desired_identity = _stat_identity(temporary.lstat())
        exchanged = True
        _atomic_exchange_paths(temporary, repos_file)
        try:
            displaced, displaced_identity = _read_regular_bounded_with_identity(
                temporary,
                maximum=MAX_JARVIS_REPOS_BYTES,
            )
        except ConfigurationError:
            displaced = b""
            displaced_identity = (-1, -1, -1, -1, -1, -1)
        if displaced != raw or not _identity_matches_after_rename(
            before_identity, displaced_identity
        ):
            try:
                active, active_identity = _read_regular_bounded_with_identity(
                    repos_file,
                    maximum=MAX_JARVIS_REPOS_BYTES,
                )
            except ConfigurationError as exc:
                raise ConfigurationError(
                    "JARVIS repositories changed during atomic reconciliation; "
                    f"displaced state retained at {temporary}"
                ) from exc
            if active != payload or not _identity_matches_after_rename(
                desired_identity, active_identity
            ):
                raise ConfigurationError(
                    "JARVIS repositories changed during atomic reconciliation; "
                    f"displaced state retained at {temporary}"
                )
            _atomic_exchange_paths(temporary, repos_file)
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError("JARVIS repositories changed during reconciliation")
        try:
            active, active_identity = _read_regular_bounded_with_identity(
                repos_file,
                maximum=MAX_JARVIS_REPOS_BYTES,
            )
        except ConfigurationError as exc:
            temporary.unlink()
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError(
                "JARVIS repositories changed after atomic reconciliation"
            ) from exc
        if active != payload or not _identity_matches_after_rename(
            desired_identity, active_identity
        ):
            temporary.unlink()
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError("JARVIS repositories changed after atomic reconciliation")
        temporary.unlink()
        exchanged = False
        _fsync_directory(repos_file.parent)
    except BaseException:
        if not exchanged:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise
    return {
        "action": "updated",
        "managed_repo": managed,
        "added_managed_repos": added_managed,
        "removed_previous_managed_repos": removed_previous,
        "before_sha256": hashlib.sha256(raw).hexdigest(),
        "after_sha256": hashlib.sha256(payload).hexdigest(),
    }


def repair_managed_jarvis_binding(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    previous_managed_repos: tuple[Path, ...] = (),
) -> dict[str, object]:
    """Repair only relay's stable package link and exact repository registration."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    resolved_home = lexical_home.resolve(strict=True)
    generation_path = lexical_home / ".local/share/clio-relay/generations" / desired.fingerprint
    generation = generation_path.resolve(strict=True)
    generations = (lexical_home / ".local/share/clio-relay/generations").resolve(strict=True)
    if (
        generation_path.is_symlink()
        or generation.parent != generations
        or generation.name != desired.fingerprint
    ):
        raise ConfigurationError("desired generation path is not one owned directory")
    current = lexical_home / ".local/share/clio-relay/current"
    _verify_stable_symlink(current, expected=generation, label="active generation")
    expected_target = current / "source/jarvis-packages/clio_relay"
    if not expected_target.resolve(strict=True).is_dir():
        raise ConfigurationError("desired generation has no relay JARVIS package repository")
    managed = _expand_home(desired.managed_jarvis_repo, lexical_home)
    managed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    snapshot = _capture_activation_path(
        managed,
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if snapshot.before is not None:
        lexical_target = _activation_symlink_lexical_target(snapshot)
        try:
            target_is_current = lexical_target.resolve(strict=True) == expected_target.resolve(
                strict=True
            )
        except (OSError, RuntimeError, ValueError):
            target_is_current = False
        if lexical_target != expected_target and not target_is_current:
            proven_targets = {
                Path(os.path.abspath(path.expanduser())) for path in previous_managed_repos
            }
            fixed_legacy_target = lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay"
            try:
                target_is_fixed_legacy = (
                    lexical_target == fixed_legacy_target
                    or lexical_target.resolve(strict=True)
                    == fixed_legacy_target.resolve(strict=True)
                )
            except (OSError, RuntimeError, ValueError):
                target_is_fixed_legacy = lexical_target == fixed_legacy_target
            target_is_proven_generation = bool(
                lexical_target in proven_targets
                and _is_generation_repository_target(
                    lexical_target.resolve(strict=True),
                    home=resolved_home,
                )
            )
            if not target_is_fixed_legacy and not target_is_proven_generation:
                raise ConfigurationError(
                    "relay-managed repository link target is not proven by an earlier receipt"
                )
    link_action = _reconcile_activation_symlink(
        snapshot,
        expected_target=expected_target,
        label="relay-managed repository",
        exchange_identity=desired.fingerprint,
    )
    repos_file = _expand_home(desired.jarvis_root, lexical_home) / "repos.yaml"
    legacy_managed_repo = _expand_home(LEGACY_MANAGED_JARVIS_REPO_PATH, lexical_home)
    relay_owned_builtin_repos = _relay_owned_jarvis_builtin_repositories(
        home=lexical_home,
        execution_environments=(generation / "jarvis-venv",),
    )
    repo_evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        managed_builtin_repo=_expand_home(desired.jarvis_root, lexical_home) / "builtin",
        previous_managed_repos=(
            *previous_managed_repos,
            legacy_managed_repo,
            *relay_owned_builtin_repos,
        ),
        exchange_identity=desired.fingerprint,
    )
    canonical_home = lexical_home.resolve(strict=True)
    return {
        "link_action": link_action,
        "link": str(_expand_home(desired.managed_jarvis_repo, canonical_home)),
        "target": str(
            canonical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        ),
        "repositories": repo_evidence,
    }


def _is_generation_repository_target(path: Path, *, home: Path) -> bool:
    """Return whether a proven path has the exact relay generation repository shape."""
    generations = home / ".local/share/clio-relay/generations"
    try:
        relative = path.relative_to(generations)
    except ValueError:
        return False
    fingerprint = relative.parts[0] if relative.parts else ""
    return bool(
        len(relative.parts) == 4
        and len(fingerprint) == 64
        and all(character in "0123456789abcdef" for character in fingerprint)
        and relative.parts[1:] == ("source", "jarvis-packages", "clio_relay")
    )


def proven_active_generation_mismatch(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> str | None:
    """Return a safely identified active generation only when it differs.

    This deliberately proves only that the stable ``current`` pointer names a
    different relay-managed generation.  It never proves an exact deployment
    match, so callers may use it solely to request normal payload
    reconciliation before performing the comparatively expensive runtime
    identity inspection.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    share = lexical_home / ".local/share/clio-relay"
    generations_path = share / "generations"
    current = share / "current"
    try:
        before = current.lstat()
        if not stat.S_ISLNK(before.st_mode):
            return None
        raw_target = os.readlink(current)
        if not raw_target or any(character in raw_target for character in "\x00\r\n"):
            return None
        target = Path(raw_target)
        if not target.is_absolute():
            target = current.parent / target
        generations_before = generations_path.lstat()
        if not stat.S_ISDIR(generations_before.st_mode) or _path_is_directory_alias(
            generations_path
        ):
            return None
        generations = generations_path.resolve(strict=True)
        resolved_target = target.resolve(strict=True)
        relative = resolved_target.relative_to(generations)
        generation = relative.name
        generation_path = generations_path / generation
        generation_before = generation_path.lstat()
        if (
            not stat.S_ISDIR(generation_before.st_mode)
            or _path_is_directory_alias(generation_path)
            or generation_path.resolve(strict=True) != resolved_target
        ):
            return None
        after = current.lstat()
        generations_after = generations_path.lstat()
        generation_after = generation_path.lstat()
    except (OSError, RuntimeError, ValueError):
        return None
    if (
        _stat_identity(after) != _stat_identity(before)
        or _stat_identity(generations_after) != _stat_identity(generations_before)
        or _stat_identity(generation_after) != _stat_identity(generation_before)
        or len(relative.parts) != 1
        or len(generation) != 64
        or any(character not in "0123456789abcdef" for character in generation)
        or generation == desired.fingerprint
    ):
        return None
    getuid = getattr(os, "getuid", None)
    if callable(getuid) and any(
        details.st_uid != getuid()
        for details in (
            generations_after,
            generation_after,
        )
    ):
        return None
    return generation


def _inspect_installation_identity(
    desired: BootstrapDesiredState,
    info: dict[str, object],
    reasons: list[str],
) -> None:
    if info.get("schema_version") != "clio-relay.installation-info.v1":
        reasons.append("installation identity schema does not match")
    if info.get("receipt_matches_install") is not True:
        reasons.append("install receipt does not match the running relay")
    raw_receipt = info.get("receipt")
    if not isinstance(raw_receipt, dict):
        reasons.append("installation identity omitted its receipt")
        return
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("deployment_fingerprint") != desired.fingerprint:
        reasons.append("desired deployment fingerprint changed")
    if receipt.get("deployment_manifest") != desired.model_dump(mode="json"):
        reasons.append("desired deployment manifest changed")
    if receipt.get("install_spec") != desired.relay_install_spec:
        reasons.append("relay install specification changed")
    if desired.relay_artifact_sha256 is not None and (
        receipt.get("artifact_sha256") != desired.relay_artifact_sha256
    ):
        reasons.append("relay artifact digest changed")
    raw_components = receipt.get("components")
    expected_components = {
        "clio-kit": desired.clio_kit_version,
        "jarvis-cd": desired.jarvis_cd_version,
        "jarvis-util": desired.jarvis_util_commit,
    }
    if not isinstance(raw_components, dict):
        reasons.append("install receipt omitted component identities")
    else:
        components = cast(dict[str, object], raw_components)
        for component, expected in expected_components.items():
            if components.get(component) != expected:
                reasons.append(f"{component} identity changed")
    raw_runtime = info.get("component_runtime")
    if not isinstance(raw_runtime, dict):
        reasons.append("installation identity omitted component runtime evidence")
        return
    runtime = cast(dict[str, object], raw_runtime)
    relay_runtime = runtime.get("clio-relay")
    if (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is not True
    ):
        reasons.append("clio-relay persistent tool identity did not verify")
    elif cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True:
        reasons.append("clio-relay JARVIS execution runtime did not verify")
    clio_kit_runtime = runtime.get("clio-kit")
    required_clio_kit = (
        "artifact_identity_verified",
        "command_matches_receipt",
        "locked_server_runtime_verified",
        "native_execution_capability_verified",
        "persistent_tool_verified",
    )
    if not isinstance(clio_kit_runtime, dict) or any(
        cast(dict[str, object], clio_kit_runtime).get(flag) is not True
        for flag in required_clio_kit
    ):
        reasons.append("clio-kit runtime identity did not verify")
    jarvis_runtime = runtime.get("jarvis-cd")
    if (
        not isinstance(jarvis_runtime, dict)
        or cast(dict[str, object], jarvis_runtime).get("verified") is not True
    ):
        reasons.append("JARVIS-CD execution identity did not verify")


def _inspect_active_generation(
    desired: BootstrapDesiredState,
    *,
    home: Path,
    installation: dict[str, object] | None,
    reasons: list[str],
) -> tuple[str | None, str | None]:
    """Verify the stable pointer and receipt name the desired generation."""
    active_generation: str | None = None
    raw_receipt = installation.get("receipt") if installation is not None else None
    if isinstance(raw_receipt, dict):
        raw_generation = cast(dict[str, object], raw_receipt).get("generation")
        if isinstance(raw_generation, str) and raw_generation:
            active_generation = raw_generation
    if active_generation != desired.fingerprint:
        reasons.append("install receipt does not name the desired active generation")

    current = home / ".local/share/clio-relay/current"
    try:
        expected_target = (
            home / ".local/share/clio-relay/generations" / desired.fingerprint
        ).resolve(strict=True)
        resolved_target = _verify_stable_symlink(
            current,
            expected=expected_target,
            label="current generation pointer",
        )
        _verify_stable_symlink(
            home / ".local/share/clio-relay/install-receipt.json",
            expected=expected_target / "install-receipt.json",
            label="stable install receipt",
        )
        for executable in ("clio-relay", "jarvis"):
            _verify_stable_symlink(
                home / ".local/bin" / executable,
                expected=expected_target / "bin" / executable,
                label=f"stable {executable} launcher",
            )
        _verify_stable_symlink(
            _expand_home(desired.managed_jarvis_repo, home),
            expected=expected_target / "source/jarvis-packages/clio_relay",
            label="relay-managed JARVIS repository",
        )
        _verify_active_generation_jarvis_wrapper(
            expected_target,
            desired=desired,
            installation=installation,
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
        reasons.append(str(exc))
        return active_generation, None
    return active_generation, str(resolved_target)


def _verify_active_generation_jarvis_wrapper(
    generation: Path,
    *,
    desired: BootstrapDesiredState,
    installation: dict[str, object] | None,
) -> None:
    """Bind the active launcher and manifest to immutable installed evidence."""
    raw_manifest = _read_regular_bounded(generation / "manifest.json", maximum=4 * 1024 * 1024)
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("active generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("active generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    expected_manifest_keys = {
        "schema_version",
        "fingerprint",
        "plan",
        "legacy_execution_identity",
        "active_execution_identity",
        "jarvis_wrapper_sha256",
        "install_receipt",
        "install_receipt_sha256",
    }
    if set(manifest) != expected_manifest_keys:
        raise ConfigurationError("active generation manifest has an unknown shape")
    expected_receipt_path = generation / "install-receipt.json"
    manifest_receipt_value = manifest.get("install_receipt")
    manifest_receipt_matches = False
    if isinstance(manifest_receipt_value, str):
        manifest_receipt_path = Path(manifest_receipt_value)
        if (
            manifest_receipt_path.is_absolute()
            and os.path.normpath(manifest_receipt_value) == manifest_receipt_value
            and not any(character in manifest_receipt_value for character in "\x00\r\n")
            and manifest_receipt_path.name == "install-receipt.json"
        ):
            try:
                manifest_receipt_matches = manifest_receipt_path.parent.resolve(
                    strict=True
                ) == generation.resolve(strict=True) and manifest_receipt_path.resolve(
                    strict=True
                ) == expected_receipt_path.resolve(strict=True)
            except (OSError, RuntimeError, ValueError):
                manifest_receipt_matches = False
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest_receipt_matches
        and manifest.get("install_receipt_sha256") == sha256_file(expected_receipt_path)
    ):
        raise ConfigurationError("active generation manifest identity changed")
    raw_plan = manifest.get("plan")
    try:
        plan = BootstrapReconcilePlan.model_validate(raw_plan)
    except ValueError as exc:
        raise ConfigurationError("active generation reconcile plan is invalid") from exc
    if plan.desired_fingerprint != desired.fingerprint:
        raise ConfigurationError("active generation reconcile plan identity changed")
    raw_identity = manifest.get("active_execution_identity")
    if not isinstance(raw_identity, dict):
        raise ConfigurationError("active generation omitted active execution identity")
    identity = cast(dict[str, object], raw_identity)
    raw_root = identity.get("root")
    raw_executables = identity.get("executables")
    if not isinstance(raw_root, str) or not isinstance(raw_executables, dict):
        raise ConfigurationError("active generation omitted active execution boundary")
    typed_executables = cast(dict[str, object], raw_executables)
    if set(typed_executables) != {"python", "jarvis"}:
        raise ConfigurationError("active generation executable set changed")
    raw_python = typed_executables.get("python")
    raw_jarvis = typed_executables.get("jarvis")
    python_path = (
        cast(dict[str, object], raw_python).get("lexical_path")
        if isinstance(raw_python, dict)
        else None
    )
    jarvis_path = (
        cast(dict[str, object], raw_jarvis).get("lexical_path")
        if isinstance(raw_jarvis, dict)
        else None
    )
    if not isinstance(python_path, str) or not isinstance(jarvis_path, str):
        raise ConfigurationError("active generation omitted JARVIS interpreter identity")
    recomputed_identity = execution_environment_identity(
        Path(raw_root),
        executables={
            "python": Path(python_path),
            "jarvis": Path(jarvis_path),
        },
    )
    if recomputed_identity != identity:
        raise ConfigurationError("active generation JARVIS execution identity changed")
    receipt = installation.get("receipt") if installation is not None else None
    raw_artifacts = (
        cast(dict[str, object], receipt).get("component_artifacts")
        if isinstance(receipt, dict)
        else None
    )
    raw_jarvis_artifact = (
        cast(dict[str, object], raw_artifacts).get("jarvis-cd")
        if isinstance(raw_artifacts, dict)
        else None
    )
    raw_interpreters = (
        cast(dict[str, object], raw_jarvis_artifact).get("runtime_interpreters")
        if isinstance(raw_jarvis_artifact, dict)
        else None
    )
    receipt_execution_python = (
        cast(dict[str, object], raw_interpreters).get("execution")
        if isinstance(raw_interpreters, dict)
        else None
    )
    if (
        not isinstance(receipt_execution_python, str)
        or receipt_execution_python != python_path
        or not Path(receipt_execution_python).is_absolute()
        or os.path.normpath(receipt_execution_python) != receipt_execution_python
        or any(character in receipt_execution_python for character in "\x00\r\n")
    ):
        raise ConfigurationError("active JARVIS interpreter is not bound to its install receipt")
    expected_payload = jarvis_wrapper_payload(Path(python_path))
    wrapper = generation / "bin/jarvis"
    observed_payload = _read_regular_bounded(wrapper, maximum=64 * 1024)
    expected_sha256 = manifest.get("jarvis_wrapper_sha256")
    if (
        observed_payload != expected_payload
        or not isinstance(expected_sha256, str)
        or hashlib.sha256(observed_payload).hexdigest() != expected_sha256
        or not os.access(wrapper, os.X_OK)
    ):
        raise ConfigurationError("active generation JARVIS wrapper identity changed")


def _relay_managed_jarvis_launcher_selected(
    stable_launcher: Path,
    *,
    lexical_home: Path,
) -> bool:
    """Return whether one stable launcher names the relay activation namespace.

    ``~/.local/bin`` is a conventional location shared by uv, pipx, and manual
    installations.  The path alone therefore proves no relay ownership.  A
    relay-managed launcher is distinguished by its stable symlink target: the
    current relay generation, or the equivalent direct generation target used
    by an older activation.  Receipt and generation validation remains the
    caller's fail-closed responsibility after this ownership boundary is met.
    """
    try:
        before = stable_launcher.lstat()
    except (OSError, RuntimeError, ValueError):
        return False
    if not stat.S_ISLNK(before.st_mode):
        return False
    try:
        raw_target = os.readlink(stable_launcher)
    except (OSError, RuntimeError, ValueError):
        return False
    target = Path(raw_target)
    if not target.is_absolute():
        target = stable_launcher.parent / target
    target = Path(os.path.abspath(target))
    try:
        canonical_home = lexical_home.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return False
    home_aliases = {lexical_home, canonical_home}
    current_targets = {root / ".local/share/clio-relay/current/bin/jarvis" for root in home_aliases}
    relay_target = target in current_targets
    if not relay_target and target.name == "jarvis" and target.parent.name == "bin":
        generation = target.parent.parent
        generation_name = generation.name
        generation_roots = {root / ".local/share/clio-relay/generations" for root in home_aliases}
        relay_target = bool(
            generation.parent in generation_roots
            and len(generation_name) == 64
            and all(character in "0123456789abcdef" for character in generation_name)
        )
    if not relay_target:
        return False
    try:
        after = stable_launcher.lstat()
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError(
            "relay-managed JARVIS launcher changed during ownership inspection"
        ) from exc
    if _stat_identity(after) != _stat_identity(before):
        raise ConfigurationError(
            "relay-managed JARVIS launcher changed during ownership inspection"
        )
    return True


def resolve_receipt_bound_jarvis_python(
    jarvis_bin: str,
    *,
    home: Path | None = None,
) -> str | None:
    """Return the verified interpreter for a relay-managed JARVIS launcher.

    Non-managed launchers return ``None`` so an explicitly unmanaged provider can
    retain its compatibility discovery.  A conventional ``~/.local/bin/jarvis``
    file or external symlink is not relay ownership evidence.  Once the exact
    relay activation symlink is selected, every receipt, generation, runtime,
    and wrapper mismatch fails closed instead of falling back to an ambient
    Python interpreter.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    launcher = Path(jarvis_bin).expanduser()
    if not launcher.is_absolute():
        discovered = shutil.which(jarvis_bin)
        if discovered is None:
            return None
        launcher = Path(discovered)
    lexical_launcher = Path(os.path.abspath(launcher))
    stable_launcher = lexical_home / ".local/bin/jarvis"
    if lexical_launcher != stable_launcher:
        try:
            canonical_launcher = (
                lexical_launcher.parent.resolve(strict=True) / lexical_launcher.name
            )
            canonical_stable = stable_launcher.parent.resolve(strict=True) / stable_launcher.name
        except (OSError, RuntimeError, ValueError):
            return None
        if canonical_launcher != canonical_stable:
            return None
    if not _relay_managed_jarvis_launcher_selected(
        stable_launcher,
        lexical_home=lexical_home,
    ):
        return None

    stable_receipt = lexical_home / ".local/share/clio-relay/install-receipt.json"
    try:
        installation = installation_info(stable_receipt)
    except (ConfigurationError, OSError, ValueError) as exc:
        raise ConfigurationError("relay-managed JARVIS installation receipt is invalid") from exc
    if (
        installation.get("schema_version") != "clio-relay.installation-info.v1"
        or installation.get("receipt_matches_install") is not True
    ):
        raise ConfigurationError(
            "relay-managed JARVIS installation receipt does not match this worker"
        )
    raw_receipt = installation.get("receipt")
    raw_runtime = installation.get("component_runtime")
    if not isinstance(raw_receipt, dict) or not isinstance(raw_runtime, dict):
        raise ConfigurationError("relay-managed JARVIS installation identity is incomplete")
    receipt = cast(dict[str, object], raw_receipt)
    runtime = cast(dict[str, object], raw_runtime)
    jarvis_runtime = runtime.get("jarvis-cd")
    if (
        not isinstance(jarvis_runtime, dict)
        or cast(dict[str, object], jarvis_runtime).get("verified") is not True
    ):
        raise ConfigurationError("relay-managed JARVIS runtime did not verify its receipt")
    relay_runtime = runtime.get("clio-relay")
    if (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True
    ):
        raise ConfigurationError(
            "relay-managed JARVIS execution runtime did not verify its relay receipt"
        )

    raw_manifest = receipt.get("deployment_manifest")
    fingerprint = receipt.get("deployment_fingerprint")
    generation_name = receipt.get("generation")
    try:
        desired = BootstrapDesiredState.model_validate(raw_manifest)
    except ValueError as exc:
        raise ConfigurationError(
            "relay-managed JARVIS receipt omitted a valid deployment manifest"
        ) from exc
    if (
        not isinstance(fingerprint, str)
        or fingerprint != desired.fingerprint
        or generation_name != fingerprint
    ):
        raise ConfigurationError("relay-managed JARVIS generation identity changed")

    generation = lexical_home / ".local/share/clio-relay/generations" / fingerprint
    _verify_stable_symlink(
        lexical_home / ".local/share/clio-relay/current",
        expected=generation,
        label="current generation pointer",
    )
    _verify_stable_symlink(
        stable_receipt,
        expected=generation / "install-receipt.json",
        label="stable install receipt",
    )
    _verify_stable_symlink(
        stable_launcher,
        expected=generation / "bin/jarvis",
        label="stable JARVIS launcher",
    )
    _verify_active_generation_jarvis_wrapper(
        generation,
        desired=desired,
        installation=installation,
    )

    raw_artifacts = receipt.get("component_artifacts")
    raw_jarvis_artifact = (
        cast(dict[str, object], raw_artifacts).get("jarvis-cd")
        if isinstance(raw_artifacts, dict)
        else None
    )
    raw_interpreters = (
        cast(dict[str, object], raw_jarvis_artifact).get("runtime_interpreters")
        if isinstance(raw_jarvis_artifact, dict)
        else None
    )
    execution_python = (
        cast(dict[str, object], raw_interpreters).get("execution")
        if isinstance(raw_interpreters, dict)
        else None
    )
    if not isinstance(execution_python, str):
        raise ConfigurationError("relay-managed JARVIS receipt omitted its interpreter")
    return execution_python


def _verify_stable_symlink(path: Path, *, expected: Path, label: str) -> Path:
    """Resolve one exact lexical symlink and reject replacement races."""
    before = path.lstat()
    if not path.is_symlink():
        raise ConfigurationError(f"{label} is not a symbolic link")
    raw_target = os.readlink(path)
    target = Path(raw_target)
    if not target.is_absolute():
        target = path.parent / target
    resolved = target.resolve(strict=True)
    if resolved != expected.resolve(strict=True):
        raise ConfigurationError(f"{label} does not name desired state")
    if _stat_identity(path.lstat()) != _stat_identity(before):
        raise ConfigurationError(f"{label} changed during inspection")
    return resolved


def _capture_activation_path(
    path: Path,
    *,
    kind: Literal["file", "file_or_symlink", "symlink"],
    maximum: int,
    allow_absent: bool,
) -> BootstrapActivationPath:
    """Capture one exact pre-fence path without following its final link."""
    lexical = Path(os.path.abspath(path.expanduser()))
    try:
        before = lexical.lstat()
    except FileNotFoundError:
        if not allow_absent:
            raise ConfigurationError(
                f"required activation path is unavailable: {lexical}"
            ) from None
        return BootstrapActivationPath(path=str(lexical), kind=kind)
    except OSError as exc:
        raise ConfigurationError(f"activation path could not be classified: {lexical}") from exc
    file_type: Literal["file", "symlink"]
    digest: str | None = None
    link_target: str | None = None
    if stat.S_ISLNK(before.st_mode):
        if kind == "file":
            raise ConfigurationError(f"activation path must be a regular file: {lexical}")
        try:
            link_target = os.readlink(lexical)
            after = lexical.lstat()
        except OSError as exc:
            raise ConfigurationError(f"activation symlink could not be read: {lexical}") from exc
        if (
            not link_target
            or any(character in link_target for character in "\x00\r\n")
            or _stat_identity(before) != _stat_identity(after)
        ):
            raise ConfigurationError(f"activation symlink changed while inspected: {lexical}")
        file_type = "symlink"
    elif stat.S_ISREG(before.st_mode):
        if kind == "symlink":
            raise ConfigurationError(f"activation path must be a symbolic link: {lexical}")
        raw, _identity = _read_regular_bounded_with_identity(lexical, maximum=maximum)
        try:
            after = lexical.lstat()
        except OSError as exc:
            raise ConfigurationError(f"activation file changed while inspected: {lexical}") from exc
        if _stat_identity(before) != _stat_identity(after):
            raise ConfigurationError(f"activation file changed while inspected: {lexical}")
        digest = hashlib.sha256(raw).hexdigest()
        file_type = "file"
    else:
        raise ConfigurationError(f"activation path has an unsafe type: {lexical}")
    return BootstrapActivationPath(
        path=str(lexical),
        kind=kind,
        before=BootstrapActivationPathIdentity(
            device=before.st_dev,
            inode=before.st_ino,
            mode=before.st_mode,
            size=before.st_size,
            modified_ns=before.st_mtime_ns,
            changed_ns=before.st_ctime_ns,
            file_type=file_type,
            sha256=digest,
            symlink_target=link_target,
        ),
    )


def _activation_path_identity(path: BootstrapActivationPath) -> BootstrapActivationPathIdentity:
    """Re-capture an existing activation path using its original contract."""
    captured = _capture_activation_path(
        Path(path.path),
        kind=path.kind,
        maximum=4 * 1024 * 1024,
        allow_absent=False,
    )
    if captured.before is None:  # pragma: no cover - excluded by allow_absent
        raise ConfigurationError(f"activation path disappeared: {path.path}")
    return captured.before


def _capture_activation_object(
    path: Path,
    *,
    kind: Literal["file", "file_or_symlink", "symlink"],
    maximum: int,
) -> BootstrapActivationPathIdentity:
    """Capture a file or link, including the Windows symlink test representation."""
    lexical = Path(os.path.abspath(path.expanduser()))
    try:
        before = lexical.lstat()
        raw_target = os.readlink(lexical)
        after = lexical.lstat()
    except OSError:
        captured = _capture_activation_path(
            lexical,
            kind=kind,
            maximum=maximum,
            allow_absent=False,
        )
        if captured.before is None:  # pragma: no cover - excluded by allow_absent
            raise ConfigurationError(f"activation path disappeared: {lexical}") from None
        return captured.before
    if (
        kind == "file"
        or not raw_target
        or any(character in raw_target for character in "\x00\r\n")
        or _stat_identity(before) != _stat_identity(after)
    ):
        raise ConfigurationError(f"activation symlink changed while inspected: {lexical}")
    return BootstrapActivationPathIdentity(
        device=before.st_dev,
        inode=before.st_ino,
        mode=before.st_mode,
        size=before.st_size,
        modified_ns=before.st_mtime_ns,
        changed_ns=before.st_ctime_ns,
        file_type="symlink",
        symlink_target=raw_target,
    )


def _capture_reconcile_activation_paths(
    *,
    home: Path,
) -> dict[str, BootstrapActivationPath]:
    """Capture the exact legacy/stable paths a staged activation may replace."""
    lexical_home = Path(os.path.abspath(home.expanduser()))
    share = lexical_home / ".local/share/clio-relay"
    current = _capture_activation_path(
        share / "current",
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if current.before is not None:
        current_target = _activation_symlink_lexical_target(current)
        try:
            relative = current_target.resolve(strict=True).relative_to(
                (share / "generations").resolve(strict=True)
            )
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(
                "active generation pointer does not name one managed generation"
            ) from exc
        if (
            len(relative.parts) != 1
            or len(relative.name) != 64
            or any(character not in "0123456789abcdef" for character in relative.name)
        ):
            raise ConfigurationError(
                "active generation pointer does not name one managed generation"
            )
    managed = _capture_activation_path(
        _expand_home(MANAGED_JARVIS_REPO_PATH, lexical_home),
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if managed.before is not None:
        managed_target = _activation_symlink_lexical_target(managed)
        allowed_targets = {
            lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay",
            share / "current/source/jarvis-packages/clio_relay",
        }
        try:
            target_matches_allowed_alias = any(
                managed_target.resolve(strict=True) == target.resolve(strict=True)
                for target in allowed_targets
            )
        except (OSError, RuntimeError, ValueError):
            target_matches_allowed_alias = False
        if (
            managed_target not in allowed_targets
            and not target_matches_allowed_alias
            and not _is_generation_repository_target(
                managed_target.resolve(strict=True),
                home=lexical_home.resolve(strict=True),
            )
        ):
            raise ConfigurationError(
                "relay-managed repository link is not one proven legacy binding"
            )
    paths = {
        "current": current,
        "install_receipt": _capture_activation_path(
            share / "install-receipt.json",
            kind="file_or_symlink",
            maximum=4 * 1024 * 1024,
            allow_absent=False,
        ),
        "relay_launcher": _capture_activation_path(
            lexical_home / ".local/bin/clio-relay",
            kind="file_or_symlink",
            maximum=1024 * 1024,
            allow_absent=False,
        ),
        "jarvis_launcher": _capture_activation_path(
            lexical_home / ".local/bin/jarvis",
            kind="file_or_symlink",
            maximum=1024 * 1024,
            allow_absent=False,
        ),
        "managed_repo": managed,
    }
    for name in ("relay_launcher", "jarvis_launcher"):
        launcher = Path(paths[name].path)
        try:
            target = launcher.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"legacy activation launcher is unavailable: {name}") from exc
        if not target.is_file() or not os.access(target, os.X_OK):
            raise ConfigurationError(f"legacy activation launcher is not executable: {name}")
    return paths


def _activation_symlink_lexical_target(path: BootstrapActivationPath) -> Path:
    """Return one captured symlink target without resolving its final object."""
    if path.before is None or path.before.file_type != "symlink":
        raise ConfigurationError(f"activation path is not a captured symlink: {path.path}")
    raw_target = path.before.symlink_target
    if raw_target is None:  # pragma: no cover - enforced by the model
        raise ConfigurationError(f"activation path omitted its symlink target: {path.path}")
    candidate = Path(raw_target)
    if not candidate.is_absolute():
        candidate = Path(path.path).parent / candidate
    return Path(os.path.abspath(candidate))


def _reconcile_activation_symlink(
    snapshot: BootstrapActivationPath,
    *,
    expected_target: Path,
    label: str,
    exchange_identity: str | None = None,
) -> str:
    """Atomically publish one stable link from either its snapshot or desired state."""
    path = Path(snapshot.path)
    target = Path(os.path.abspath(expected_target.expanduser()))
    token = exchange_identity or hashlib.sha256(f"{snapshot.path}\0{target}".encode()).hexdigest()
    try:
        _require_sha256(token, field="activation_exchange_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    temporary = path.with_name(f".{path.name}.{token}.exchange")
    action = "created" if snapshot.before is None else "retargeted"
    if temporary.exists() or temporary.is_symlink():
        try:
            active = _capture_activation_object(
                path,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
            displaced = _capture_activation_object(
                temporary,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
        except ConfigurationError as exc:
            raise ConfigurationError(
                f"{label} exchange recovery found an invalid path state"
            ) from exc
        active_is_desired = bool(
            active.file_type == "symlink" and active.symlink_target == str(target)
        )
        displaced_is_desired = bool(
            displaced.file_type == "symlink" and displaced.symlink_target == str(target)
        )
        active_is_before = bool(
            snapshot.before is not None
            and _activation_identity_matches_after_rename(snapshot.before, active)
        )
        displaced_is_before = bool(
            snapshot.before is not None
            and _activation_identity_matches_after_rename(snapshot.before, displaced)
        )
        if active_is_desired and displaced_is_before:
            _verify_stable_symlink(path, expected=target, label=label)
            temporary.unlink()
            _fsync_directory(path.parent)
            return action
        if active_is_before and displaced_is_desired:
            temporary.unlink()
            _fsync_directory(path.parent)
        else:
            raise ConfigurationError(
                f"{label} exchange recovery found unproven path states: {path}, {temporary}"
            )
    try:
        before = path.lstat()
        raw_target = os.readlink(path)
        if raw_target != str(target):
            raise ConfigurationError(f"{label} does not use its canonical target")
        _verify_stable_symlink(path, expected=target, label=label)
        if _stat_identity(path.lstat()) != _stat_identity(before):
            raise ConfigurationError(f"{label} changed during inspection")
    except (ConfigurationError, OSError, RuntimeError, ValueError):
        pass
    else:
        return "reused"
    try:
        observed = _activation_path_identity(snapshot)
    except ConfigurationError:
        if snapshot.before is not None:
            raise ConfigurationError(f"{label} changed after bootstrap inspection") from None
        try:
            path.lstat()
        except FileNotFoundError:
            observed = None
        except OSError as exc:
            raise ConfigurationError(f"{label} could not be classified") from exc
        else:
            raise ConfigurationError(f"{label} appeared after bootstrap inspection") from None
    else:
        if snapshot.before is None or observed != snapshot.before:
            raise ConfigurationError(f"{label} changed after bootstrap inspection")
    if snapshot.before is None:
        try:
            path.symlink_to(target, target_is_directory=target.is_dir())
            _fsync_directory(path.parent)
        except FileExistsError as exc:
            raise ConfigurationError(f"{label} appeared before activation") from exc
        _verify_stable_symlink(path, expected=target, label=label)
        if os.readlink(path) != str(target):  # pragma: no cover - written above
            raise ConfigurationError(f"{label} did not use its canonical target")
        return action
    exchanged = False
    try:
        temporary.symlink_to(target, target_is_directory=target.is_dir())
        desired = _capture_activation_object(
            temporary,
            kind="symlink",
            maximum=4096,
        )
        if _activation_path_identity(snapshot) != snapshot.before:
            raise ConfigurationError(f"{label} changed before activation")
        exchanged = True
        _atomic_exchange_paths(temporary, path)
        try:
            displaced = _capture_activation_object(
                temporary,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
        except ConfigurationError:
            displaced = None
        if displaced is None or not _activation_identity_matches_after_rename(
            snapshot.before, displaced
        ):
            try:
                active = _capture_activation_object(
                    path,
                    kind="symlink",
                    maximum=4096,
                )
            except ConfigurationError as exc:
                raise ConfigurationError(
                    f"{label} changed during atomic activation; "
                    f"displaced state retained at {temporary}"
                ) from exc
            if not _activation_identity_matches_after_rename(desired, active):
                raise ConfigurationError(
                    f"{label} changed during atomic activation; "
                    f"displaced state retained at {temporary}"
                )
            _atomic_exchange_paths(temporary, path)
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed before atomic activation")
        try:
            active = _capture_activation_object(
                path,
                kind="symlink",
                maximum=4096,
            )
        except ConfigurationError as exc:
            temporary.unlink()
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed after atomic activation") from exc
        if not _activation_identity_matches_after_rename(desired, active):
            temporary.unlink()
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed after atomic activation")
        temporary.unlink()
        exchanged = False
        _fsync_directory(path.parent)
    except BaseException:
        if not exchanged:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise
    _verify_stable_symlink(path, expected=target, label=label)
    if os.readlink(path) != str(target):  # pragma: no cover - written above
        raise ConfigurationError(f"{label} did not use its canonical target")
    return action


def reconcile_staged_activation_links(
    plan: BootstrapReconcilePlan,
    *,
    generation: Path,
    home: Path | None = None,
) -> dict[str, object]:
    """Idempotently finish one fenced generation activation after any crash boundary."""
    if plan.mode not in {"relay-only", "component-upgrade"}:
        raise ConfigurationError("staged activation requires a replacement reconcile plan")
    expected_names = {
        "current",
        "install_receipt",
        "relay_launcher",
        "jarvis_launcher",
        "managed_repo",
    }
    if set(plan.activation_paths) != expected_names:
        raise ConfigurationError("staged activation plan omitted its path identities")
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    expected_generation = (
        lexical_home / ".local/share/clio-relay/generations" / plan.desired_fingerprint
    )
    if generation != expected_generation or generation.is_symlink() or not generation.is_dir():
        raise ConfigurationError("staged activation generation path is invalid")
    targets = {
        "current": generation,
        "install_receipt": lexical_home / ".local/share/clio-relay/current/install-receipt.json",
        "relay_launcher": lexical_home / ".local/share/clio-relay/current/bin/clio-relay",
        "jarvis_launcher": lexical_home / ".local/share/clio-relay/current/bin/jarvis",
        "managed_repo": lexical_home
        / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay",
    }
    for name, target_path in {
        "current": lexical_home / ".local/share/clio-relay/current",
        "install_receipt": lexical_home / ".local/share/clio-relay/install-receipt.json",
        "relay_launcher": lexical_home / ".local/bin/clio-relay",
        "jarvis_launcher": lexical_home / ".local/bin/jarvis",
        "managed_repo": _expand_home(MANAGED_JARVIS_REPO_PATH, lexical_home),
    }.items():
        if Path(plan.activation_paths[name].path) != target_path:
            raise ConfigurationError(f"staged activation path destination changed: {name}")
    actions: dict[str, str] = {}
    for name in (
        "current",
        "install_receipt",
        "relay_launcher",
        "jarvis_launcher",
        "managed_repo",
    ):
        actions[name] = _reconcile_activation_symlink(
            plan.activation_paths[name],
            expected_target=targets[name],
            label=f"bootstrap stable activation path {name}",
            exchange_identity=plan.desired_fingerprint,
        )
    return {
        "schema_version": "clio-relay.bootstrap-activation.v1",
        "generation": plan.desired_fingerprint,
        "actions": actions,
    }


def _queue_readiness_verified(evidence: dict[str, object] | None) -> bool:
    if evidence is None:
        return False
    return bool(
        evidence.get("schema_version") == "clio-relay.queue-readiness.v1"
        and evidence.get("complete") is True
        and evidence.get("sealed") is True
        and evidence.get("repair_required") is False
    )


def _worker_readiness_verified(
    evidence: dict[str, object] | None,
    cluster: str | None,
) -> bool:
    return bool(
        evidence is not None
        and evidence.get("schema_version")
        in {
            "clio-relay.worker-runtime-info.v1",
            "clio-relay.worker-readiness.v1",
        }
        and evidence.get("cluster") == cluster
        and evidence.get("fresh") is True
        and evidence.get("process_running") is True
        and evidence.get("identity_matches_current") is True
        and evidence.get("running") is True
    )


def _verify_binary(path: Path, expected: str, *, label: str, reasons: list[str]) -> None:
    try:
        if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
            raise ConfigurationError(f"{label} is not one regular executable")
        if sha256_file(path) != expected:
            raise ConfigurationError(f"{label} digest changed")
    except (ConfigurationError, OSError, ValueError) as exc:
        reasons.append(str(exc))


def _verify_uv(path: Path, *, desired: BootstrapDesiredState, reasons: list[str]) -> None:
    _verify_binary(path, desired.uv_sha256, label="uv", reasons=reasons)
    if any(reason.startswith("uv ") for reason in reasons):
        return
    try:
        completed = run_bounded_process(
            [str(path), "--version"],
            timeout_seconds=10,
            stdout_maximum_bytes=4096,
            stderr_maximum_bytes=4096,
        )
    except (OSError, BoundedProcessError) as exc:
        reasons.append(f"uv version probe failed: {exc}")
        return
    if completed.returncode != 0 or not _uv_version_output_matches(
        completed.stdout,
        expected_version=desired.uv_version,
    ):
        reasons.append("uv version changed")


def _uv_version_output_matches(value: str, *, expected_version: str) -> bool:
    """Match uv's pinned version with its optional bounded build target."""
    observed = value
    if observed.endswith("\r\n"):
        observed = observed[:-2]
    elif observed.endswith("\n"):
        observed = observed[:-1]
    if (
        not observed
        or observed != observed.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in observed)
    ):
        return False
    exact = f"uv {expected_version}"
    if observed == exact:
        return True
    prefix = exact + " ("
    if (
        len(observed) > len(prefix) + 128
        or not observed.startswith(prefix)
        or not observed.endswith(")")
    ):
        return False
    target = observed[len(prefix) : -1]
    return bool(
        target
        and all(
            character.isascii() and (character.isalnum() or character in {"-", "_", "."})
            for character in target
        )
    )


def _expand_home(value: str, home: Path) -> Path:
    if value == "~":
        return home
    if value.startswith("~/"):
        return home / value[2:]
    path = Path(value)
    if not path.is_absolute():
        raise ConfigurationError(f"bootstrap state path is not absolute: {value}")
    return path


def _canonical_path_preserving_final(path: Path) -> Path:
    """Canonicalize ancestor aliases without following the final path component."""
    lexical = Path(os.path.abspath(path.expanduser()))
    if any(character in str(lexical) for character in "\x00\r\n"):
        raise ConfigurationError("managed path contains unsafe characters")
    try:
        parent = lexical.parent.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("managed path parent is unavailable") from exc
    if not parent.is_dir():  # pragma: no cover - resolve(strict=True) normally proves this
        raise ConfigurationError("managed path parent is not a directory")
    return parent / lexical.name


def _yaml_mapping(raw: bytes, *, label: str) -> dict[str, object]:
    try:
        value = cast(object, yaml.safe_load(raw.decode("utf-8")))
    except (UnicodeError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"{label} is invalid") from exc
    typed_value = cast(dict[object, object], value) if isinstance(value, dict) else {}
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in typed_value):
        raise ConfigurationError(f"{label} must contain one string-keyed mapping")
    return cast(dict[str, object], value)


def _read_regular_bounded(path: Path, *, maximum: int) -> bytes:
    raw, _identity = _read_regular_bounded_with_identity(path, maximum=maximum)
    return raw


def _read_regular_bounded_with_identity(
    path: Path,
    *,
    maximum: int,
) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
    """Read one bounded regular file and retain its stable filesystem identity."""
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | _O_BINARY | _O_NOFOLLOW
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        linked = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size < 1 or before.st_size > maximum:
            raise ConfigurationError(f"state file is not one bounded regular file: {path}")
        if _cross_handle_stat_identity(before) != _cross_handle_stat_identity(linked):
            raise ConfigurationError(f"state file path changed while it was opened: {path}")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        linked_after = path.lstat()
    except OSError as exc:
        raise ConfigurationError(f"could not read state file: {path}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if (
        len(raw) != before.st_size
        or len(raw) > maximum
        or _stat_identity(before) != _stat_identity(after)
        or _cross_handle_stat_identity(before) != _cross_handle_stat_identity(linked_after)
    ):
        raise ConfigurationError(f"state file changed while it was inspected: {path}")
    return raw, _stat_identity(before)


def _read_bounded(path: Path, *, maximum: int) -> str:
    return _read_regular_bounded(path, maximum=maximum).decode("utf-8")


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _cross_handle_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Return fields stable across descriptor/path stat handles on this platform."""
    if os.name == "nt":
        # Windows may report ctime_ns with different rounding and synthesize
        # execute permission bits from a path's extension only for lstat.
        # Device/inode and file type still bind the file object; size and mtime
        # retain change detection across descriptor and path handles.
        return (
            value.st_dev,
            value.st_ino,
            stat.S_IFMT(value.st_mode),
            value.st_size,
            value.st_mtime_ns,
        )
    return _stat_identity(value)


def _atomic_exchange_paths(left: Path, right: Path) -> None:
    """Atomically exchange two existing pathnames without dropping either object."""
    if sys.platform == "linux":
        library = ctypes.CDLL(None, use_errno=True)
        renameat2 = library.renameat2
        renameat2.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        renameat2.restype = ctypes.c_int
        result = renameat2(
            _AT_FDCWD,
            os.fsencode(left),
            _AT_FDCWD,
            os.fsencode(right),
            _RENAME_EXCHANGE,
        )
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), f"{left} <-> {right}")
        return
    if os.name != "nt":
        raise ConfigurationError("atomic bootstrap path exchange requires Linux")
    # The staged bootstrap runs on Linux. This fallback keeps the path contract
    # testable on Windows without weakening the supported cluster operation.
    holding = right.with_name(f".{right.name}.{os.getpid()}.exchange")
    if holding.exists() or holding.is_symlink():
        raise ConfigurationError(f"atomic exchange holding path already exists: {holding}")
    os.replace(right, holding)
    try:
        os.replace(left, right)
    except BaseException:
        os.replace(holding, right)
        raise
    os.replace(holding, left)


def _identity_matches_after_rename(
    before: tuple[int, int, int, int, int, int],
    after: tuple[int, int, int, int, int, int],
) -> bool:
    """Compare file identity while excluding ctime, which rename changes on Linux."""
    return before[:5] == after[:5]


def _activation_identity_matches_after_rename(
    before: BootstrapActivationPathIdentity,
    after: BootstrapActivationPathIdentity,
) -> bool:
    """Compare a captured activation object after an atomic pathname exchange."""
    return bool(
        before.device == after.device
        and before.inode == after.inode
        and before.mode == after.mode
        and before.size == after.size
        and before.modified_ns == after.modified_ns
        and before.file_type == after.file_type
        and before.sha256 == after.sha256
        and before.symlink_target == after.symlink_target
    )


def verify_atomic_exchange_support(
    directories: tuple[Path, ...],
    *,
    identity: str,
) -> dict[str, object]:
    """Exercise and restore atomic exchange on every staged-mutation filesystem."""
    try:
        _require_sha256(identity, field="exchange_preflight_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    verified: list[str] = []
    seen: set[Path] = set()
    for raw_directory in directories:
        directory = Path(os.path.abspath(raw_directory.expanduser()))
        try:
            details = directory.lstat()
            resolved = directory.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(
                f"atomic exchange preflight directory is unavailable: {directory}"
            ) from exc
        if directory.is_symlink() or not stat.S_ISDIR(details.st_mode):
            raise ConfigurationError(
                f"atomic exchange preflight path is not one directory: {directory}"
            )
        if resolved in seen:
            continue
        seen.add(resolved)
        left = directory / f".clio-relay-exchange-{identity}.left"
        right = directory / f".clio-relay-exchange-{identity}.right"
        left_payload = f"left:{identity}\n".encode("ascii")
        right_payload = f"right:{identity}\n".encode("ascii")
        if left.exists() or left.is_symlink() or right.exists() or right.is_symlink():
            try:
                observed = {
                    _read_regular_bounded(left, maximum=256),
                    _read_regular_bounded(right, maximum=256),
                }
            except ConfigurationError as exc:
                raise ConfigurationError(
                    f"atomic exchange preflight recovery is unproven: {directory}"
                ) from exc
            if observed != {left_payload, right_payload}:
                raise ConfigurationError(
                    f"atomic exchange preflight recovery is unproven: {directory}"
                )
            left.unlink()
            right.unlink()
            _fsync_directory(directory)
        try:
            for path, payload in ((left, left_payload), (right, right_payload)):
                with path.open("xb") as stream:
                    os.chmod(path, 0o600)
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            _fsync_directory(directory)
            _atomic_exchange_paths(left, right)
            if (
                _read_regular_bounded(left, maximum=256) != right_payload
                or _read_regular_bounded(right, maximum=256) != left_payload
            ):
                raise ConfigurationError(
                    f"atomic exchange preflight produced invalid state: {directory}"
                )
            _atomic_exchange_paths(left, right)
            if (
                _read_regular_bounded(left, maximum=256) != left_payload
                or _read_regular_bounded(right, maximum=256) != right_payload
            ):
                raise ConfigurationError(
                    f"atomic exchange preflight did not restore state: {directory}"
                )
            left.unlink()
            right.unlink()
            _fsync_directory(directory)
        except BaseException:
            with suppress(OSError):
                left.unlink(missing_ok=True)
            with suppress(OSError):
                right.unlink(missing_ok=True)
            _fsync_directory(directory)
            raise
        verified.append(str(directory))
    return {
        "schema_version": "clio-relay.atomic-exchange-preflight.v1",
        "identity": identity,
        "directories": verified,
    }


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            os.chmod(temporary, 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_sha256(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must contain one lowercase SHA-256 digest")
