"""Crash-safe desired-state reconciliation primitives for cluster bootstrap.

The SSH bootstrap shell is intentionally a small transaction driver.  This
module owns the durable contract used by that driver: canonical desired-state
identity, read-only no-op verification, JARVIS state preservation evidence,
and the fsync-backed transaction journal.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import stat
import subprocess
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
)

BOOTSTRAP_DESIRED_STATE_SCHEMA = "clio-relay.bootstrap-desired-state.v1"
BOOTSTRAP_RECEIPT_SCHEMA = "clio-relay.bootstrap-receipt.v2"
BOOTSTRAP_TRANSACTION_SCHEMA = "clio-relay.bootstrap-transaction.v1"
MAX_JARVIS_CONFIG_BYTES = 1024 * 1024
MAX_JARVIS_REPOS_BYTES = 4 * 1024 * 1024
MAX_JARVIS_GRAPH_BYTES = 64 * 1024 * 1024
BOOTSTRAP_LOCK_TIMEOUT_SECONDS = 30.0
_O_BINARY = cast(int, getattr(os, "O_BINARY", 0))
_O_NOFOLLOW = cast(int, getattr(os, "O_NOFOLLOW", 0))
_FCHMOD = cast(
    Callable[[int, int], None] | None,
    getattr(os, "fchmod", None),  # noqa: B009 - absent from Windows typing/runtime
)


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
    managed_jarvis_repo: str = "~/.local/share/clio-relay/managed-jarvis-repo"

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


class BootstrapReconcilePlan(BaseModel):
    """Read-only component plan produced before preparation or fencing."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["repair", "relay-only", "component-upgrade", "full"]
    desired_fingerprint: str
    reasons: list[str] = Field(default_factory=list)
    component_actions: dict[str, Literal["reuse", "replace"]]
    reusable_paths: dict[str, str] = Field(default_factory=dict)


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
    try:
        root_details = root.lstat()
    except OSError as exc:
        raise ConfigurationError("execution environment is unavailable") from exc
    if root.is_symlink() or not root.is_dir():
        raise ConfigurationError("execution environment is not one owned directory")
    identities: dict[str, object] = {}
    resolved_root = root.resolve(strict=True)
    for name, executable in sorted(executables.items()):
        try:
            lexical = executable.absolute()
            lexical.relative_to(root.absolute())
            before = lexical.lstat()
            resolved = lexical.resolve(strict=True)
            if not resolved.is_file() or not os.access(resolved, os.X_OK):
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
    config_path = root / "pyvenv.cfg"
    config_sha256 = sha256_file(config_path) if config_path.is_file() else None
    if _stat_identity(root.lstat()) != _stat_identity(root_details):
        raise ConfigurationError("execution environment changed during inspection")
    return {
        "schema_version": "clio-relay.execution-boundary.v1",
        "root": str(resolved_root),
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
    try:
        resolved_python = execution_python.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("JARVIS execution interpreter is unavailable") from exc
    if not resolved_python.is_file() or not os.access(resolved_python, os.X_OK):
        raise ConfigurationError("JARVIS execution interpreter is not executable")
    invocation = "from jarvis_cd.core.cli import main; raise SystemExit(main())"
    return (
        f'#!/bin/sh\nexec {shlex.quote(str(resolved_python))} -c {shlex.quote(invocation)} "$@"\n'
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
    resolved_home = (home or Path.home()).resolve()
    reasons: list[str] = []
    receipt_path = resolved_home / ".local/share/clio-relay/install-receipt.json"
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
        home=resolved_home,
        installation=info,
        reasons=reasons,
    )

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
    jarvis_state = inspect_jarvis_state(desired, home=resolved_home)
    if not jarvis_state.initialized:
        reasons.append("JARVIS is not initialized")
    if not jarvis_state.managed_repo_registered:
        reasons.append("the exact relay-managed JARVIS repository is not registered")

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
    resolved_home = (home or Path.home()).resolve()
    jarvis_root = _expand_home(desired.jarvis_root, resolved_home)
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
    managed_repo = str(_expand_home(desired.managed_jarvis_repo, resolved_home).absolute())
    repo_values = cast(list[str], raw_repo_values)
    return JarvisStateEvidence(
        initialized=True,
        root=str(jarvis_root),
        roots=observed_roots,
        config_sha256=hashlib.sha256(raw_config).hexdigest(),
        repos_sha256=hashlib.sha256(raw_repos).hexdigest(),
        resource_graph_sha256=hashlib.sha256(raw_graph).hexdigest(),
        managed_repo_registered=managed_repo in repo_values,
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
        "jarvis_wrapper_sha256",
        "install_receipt",
    }:
        raise ConfigurationError("prepared generation manifest has an unknown shape")
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest.get("legacy_execution_identity") == legacy_execution_identity
        and manifest.get("install_receipt") == str(receipt_path)
    ):
        raise ConfigurationError("prepared generation manifest identity changed")
    plan = manifest.get("plan")
    if (
        not isinstance(plan, dict)
        or cast(dict[str, object], plan).get("desired_fingerprint") != desired.fingerprint
    ):
        raise ConfigurationError("prepared generation plan identity changed")
    raw_executables = legacy_execution_identity.get("executables")
    raw_python = (
        cast(dict[str, object], raw_executables).get("python")
        if isinstance(raw_executables, dict)
        else None
    )
    raw_python_path = (
        cast(dict[str, object], raw_python).get("resolved_path")
        if isinstance(raw_python, dict)
        else None
    )
    if not isinstance(raw_python_path, str):
        raise ConfigurationError("prepared generation omitted JARVIS interpreter identity")
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
    if not (
        info.get("receipt_matches_install") is True
        and typed_receipt.get("deployment_fingerprint") == desired.fingerprint
        and typed_receipt.get("deployment_manifest") == desired.model_dump(mode="json")
        and typed_receipt.get("generation") == desired.fingerprint
    ):
        raise ConfigurationError("prepared generation install receipt identity changed")
    relay_runtime = typed_runtime.get("clio-relay")
    clio_kit_runtime = typed_runtime.get("clio-kit")
    jarvis_runtime = typed_runtime.get("jarvis-cd")
    if not (
        isinstance(relay_runtime, dict)
        and cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is True
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


def plan_bootstrap_reconcile(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> BootstrapReconcilePlan:
    """Plan a relay-only generation when every non-relay component verifies.

    This deliberately supports the first upgrade from a pre-generation install:
    an older receipt need not contain a deployment manifest, but every reusable
    component must have exact artifact and live-runtime evidence.
    """
    resolved_home = (home or Path.home()).resolve()
    reasons: list[str] = []
    upgrade_reasons: list[str] = []
    upgrade_components: set[str] = set()
    reusable_paths: dict[str, str] = {}
    receipt_path = resolved_home / ".local/share/clio-relay/install-receipt.json"
    try:
        info = installation_info(receipt_path)
    except (ConfigurationError, OSError, ValueError) as exc:
        return _full_plan(desired, f"installation identity did not verify: {exc}")
    if info.get("receipt_matches_install") is not True:
        return _full_plan(desired, "install receipt does not match the running relay")
    raw_receipt = info.get("receipt")
    raw_runtime = info.get("component_runtime")
    if not isinstance(raw_receipt, dict) or not isinstance(raw_runtime, dict):
        return _full_plan(desired, "installation identity omitted component evidence")
    receipt = cast(dict[str, object], raw_receipt)
    runtime = cast(dict[str, object], raw_runtime)
    raw_components = receipt.get("components")
    raw_artifacts = receipt.get("component_artifacts")
    if not isinstance(raw_components, dict) or not isinstance(raw_artifacts, dict):
        return _full_plan(desired, "install receipt omitted reusable component artifacts")
    components = cast(dict[str, object], raw_components)
    artifacts = cast(dict[str, object], raw_artifacts)
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
    supported_legacy_venv = resolved_home / ".local/share/clio-relay/jarvis-venv"
    expected_legacy_executable = legacy_python.parent / (
        "jarvis.exe" if os.name == "nt" else "jarvis"
    )
    resolved_legacy_executable: Path | None = None
    try:
        legacy_python_before = legacy_python.lstat()
        legacy_executable_before = legacy_executable.lstat()
        expected_executable_before = expected_legacy_executable.lstat()
        resolved_legacy_venv = lexical_legacy_venv.resolve(strict=True)
        resolved_supported_venv = supported_legacy_venv.resolve(strict=True)
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
            and resolved_legacy_venv == resolved_supported_venv
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
    )


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
        legacy_python = (
            home
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


def reconcile_managed_jarvis_repository(
    repos_file: Path,
    managed_repo: Path,
    *,
    previous_managed_repos: tuple[Path, ...] = (),
) -> dict[str, object]:
    """Register only the exact relay-owned repository without basename matching.

    JARVIS's public ``repo add --force`` replaces every repository with the
    same basename. Relay instead performs a compare-before-replace update of
    its one exact path and leaves operator repositories, including same-name
    repositories, untouched. ``previous_managed_repos`` is a caller-supplied
    provenance boundary: each path must come from an earlier relay receipt or
    an exact relay-owned generation path. This operation is serialized by the
    bootstrap lock; the final byte-and-file-identity comparison also detects
    non-cooperating writers before the atomic replacement.
    """
    raw, before_identity = _read_regular_bounded_with_identity(
        repos_file,
        maximum=MAX_JARVIS_REPOS_BYTES,
    )
    document = _yaml_mapping(raw, label="JARVIS repositories")
    raw_repos = document.get("repos")
    typed_repos = cast(list[object], raw_repos) if isinstance(raw_repos, list) else []
    if not isinstance(raw_repos, list) or any(
        not isinstance(value, str) or not value for value in typed_repos
    ):
        raise ConfigurationError("JARVIS repositories must contain a string list")
    repos = list(cast(list[str], raw_repos))
    managed = str(managed_repo.absolute())
    if repos.count(managed) == 1:
        return {
            "action": "reused",
            "managed_repo": managed,
            "added_managed_repos": [],
            "removed_previous_managed_repos": [],
            "before_sha256": hashlib.sha256(raw).hexdigest(),
            "after_sha256": hashlib.sha256(raw).hexdigest(),
        }
    if repos.count(managed) > 1:
        raise ConfigurationError("relay-managed JARVIS repository is registered more than once")
    previous = {str(path.absolute()) for path in previous_managed_repos}
    previous.discard(managed)
    if any(repos.count(value) > 1 for value in previous):
        raise ConfigurationError(
            "a proven previous relay-managed JARVIS repository is registered more than once"
        )
    updated = [value for value in repos if value not in previous]
    updated.insert(0, managed)
    document["repos"] = updated
    payload = yaml.safe_dump(document, sort_keys=False).encode("utf-8")
    temporary = repos_file.with_name(f".{repos_file.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        current, current_identity = _read_regular_bounded_with_identity(
            repos_file,
            maximum=MAX_JARVIS_REPOS_BYTES,
        )
        if current != raw or current_identity != before_identity:
            raise ConfigurationError("JARVIS repositories changed during reconciliation")
        os.replace(temporary, repos_file)
        _fsync_directory(repos_file.parent)
    except BaseException:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise
    return {
        "action": "updated",
        "managed_repo": managed,
        "added_managed_repos": [managed],
        "removed_previous_managed_repos": sorted(previous.intersection(repos)),
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
    resolved_home = (home or Path.home()).resolve()
    generation = (
        resolved_home / ".local/share/clio-relay/generations" / desired.fingerprint
    ).resolve(strict=True)
    expected_target = (generation / "source/jarvis-packages/clio_relay").resolve(strict=True)
    if not expected_target.is_dir():
        raise ConfigurationError("desired generation has no relay JARVIS package repository")
    managed = _expand_home(desired.managed_jarvis_repo, resolved_home)
    managed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    link_action = "reused"
    try:
        managed_details = managed.lstat()
    except FileNotFoundError:
        link_action = "created"
    except OSError as exc:
        raise ConfigurationError("relay-managed repository link could not be classified") from exc
    else:
        if not stat.S_ISLNK(managed_details.st_mode):
            raise ConfigurationError(
                "relay-managed repository path is not a replaceable symbolic link"
            )
        raw_target = Path(os.readlink(managed))
        if not raw_target.is_absolute():
            raw_target = managed.parent / raw_target
        lexical_target = Path(os.path.abspath(raw_target))
        if lexical_target == expected_target:
            _verify_stable_symlink(
                managed,
                expected=expected_target,
                label="relay-managed repository",
            )
        else:
            proven_targets = {
                Path(os.path.abspath(path.expanduser())) for path in previous_managed_repos
            }
            if lexical_target not in proven_targets or not _is_generation_repository_target(
                lexical_target,
                home=resolved_home,
            ):
                raise ConfigurationError(
                    "relay-managed repository link target is not proven by an earlier receipt"
                )
            link_action = "retargeted"
    if link_action != "reused":
        temporary = managed.with_name(f".{managed.name}.{os.getpid()}.tmp")
        try:
            temporary.symlink_to(expected_target, target_is_directory=True)
            os.replace(temporary, managed)
            _fsync_directory(managed.parent)
        except BaseException:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
            raise
        _verify_stable_symlink(managed, expected=expected_target, label="relay-managed repository")
    repos_file = _expand_home(desired.jarvis_root, resolved_home) / "repos.yaml"
    repo_evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        previous_managed_repos=previous_managed_repos,
    )
    return {
        "link_action": link_action,
        "link": str(managed),
        "target": str(expected_target),
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
            home / ".local/share/clio-relay/managed-jarvis-repo",
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
        "jarvis_wrapper_sha256",
        "install_receipt",
    }
    if set(manifest) != expected_manifest_keys:
        raise ConfigurationError("active generation manifest has an unknown shape")
    expected_receipt_path = generation / "install-receipt.json"
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest.get("install_receipt") == str(expected_receipt_path)
    ):
        raise ConfigurationError("active generation manifest identity changed")
    raw_plan = manifest.get("plan")
    try:
        plan = BootstrapReconcilePlan.model_validate(raw_plan)
    except ValueError as exc:
        raise ConfigurationError("active generation reconcile plan is invalid") from exc
    if plan.desired_fingerprint != desired.fingerprint:
        raise ConfigurationError("active generation reconcile plan identity changed")
    raw_identity = manifest.get("legacy_execution_identity")
    if not isinstance(raw_identity, dict):
        raise ConfigurationError("active generation omitted JARVIS execution identity")
    identity = cast(dict[str, object], raw_identity)
    raw_root = identity.get("root")
    raw_executables = identity.get("executables")
    if not isinstance(raw_root, str) or not isinstance(raw_executables, dict):
        raise ConfigurationError("active generation omitted JARVIS execution boundary")
    typed_executables = cast(dict[str, object], raw_executables)
    if set(typed_executables) != {"python", "jarvis"}:
        raise ConfigurationError("active generation JARVIS executable set changed")
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
    if not isinstance(receipt_execution_python, str) or (
        Path(receipt_execution_python).resolve(strict=True)
        != Path(python_path).resolve(strict=True)
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
        and evidence.get("schema_version") == "clio-relay.worker-runtime-info.v1"
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
    if completed.returncode != 0 or completed.stdout.strip() != f"uv {desired.uv_version}":
        reasons.append("uv version changed")


def _expand_home(value: str, home: Path) -> Path:
    if value == "~":
        return home
    if value.startswith("~/"):
        return home / value[2:]
    path = Path(value)
    if not path.is_absolute():
        raise ConfigurationError(f"bootstrap state path is not absolute: {value}")
    return path


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
