"""Fsync-backed transaction journal and state machine for one generation activation.

Owns ``BootstrapTransactionState`` (the state machine), the owned-path
evidence models, and ``BootstrapTransactionJournal`` itself -- the
crash-recovery record a bootstrap transaction advances and persists at every
mutation boundary (iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.bootstrap_reconcile_constants import BOOTSTRAP_TRANSACTION_SCHEMA
from clio_relay.bootstrap_reconcile_primitives import _atomic_json, _read_bounded, _require_sha256
from clio_relay.errors import ConfigurationError


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
