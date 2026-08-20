"""Shared constants, protocols, exceptions, and records for process containment.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231):
holds the foundational data this subsystem's other owner modules and the
facade all depend on, with no outbound dependency of its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

CONTAINMENT_ENV = "CLIO_RELAY_PROCESS_CONTAINMENT"
CONTAINMENT_VALUE = "relay-owned-v1"
BROKER_CHILD_ENVIRONMENT_SCHEMA = "clio-relay.child-environment.v1"
BROKER_CREDENTIAL_FD_ENV = "CLIO_RELAY_BROKER_CREDENTIAL_FD"
BROKER_READY_FD_ENV = "CLIO_RELAY_BROKER_READY_FD"
BROKER_PROTOCOL_MAX_BYTES = 16 * 1024
BROKER_STDIN_MAX_BYTES = 4 * 1024 * 1024
BROKER_SETUP_MAX_BYTES = 6 * 1024 * 1024
BROKER_HANDSHAKE_TIMEOUT_SECONDS = 5.0
BROKER_READY_TIMEOUT_SECONDS = 10.0
BROKER_STARTUP_RECORD_SCHEMA = "clio-relay.broker-startup.v1"
BROKER_STARTUP_RECORD_MAX_BYTES = 1024
DISCOVERY_TIMEOUT_SECONDS = 5.0
TERMINATION_TIMEOUT_SECONDS = 10.0
POLL_SECONDS = 0.05
DISCOVERY_ROUNDS = 3
SYSTEMCTL_OUTPUT_MAX_BYTES = 64 * 1024

_BROKER_STARTUP_STAGE_CODES: dict[str, frozenset[str]] = {
    "memory_gate": frozenset({"internal_error"}),
    "setup_parse": frozenset({"internal_error"}),
    "child_spawn": frozenset(
        {
            "executable_not_found",
            "executable_not_permitted",
            "executable_format_invalid",
            "internal_error",
        }
    ),
    "credential_write": frozenset({"child_exited", "credential_timeout", "internal_error"}),
    "credential_ack": frozenset({"child_exited", "ack_timeout", "ack_mismatch", "internal_error"}),
    "readiness_publish": frozenset({"internal_error"}),
    "stdin_forward": frozenset({"child_exited", "internal_error"}),
}
_BROKER_EXCEPTION_TYPE_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}")
_CGROUP_ROOT = Path("/sys/fs/cgroup")


class _ResourceModule(Protocol):
    RLIMIT_CORE: int

    def setrlimit(self, resource_id: int, limits: tuple[int, int]) -> None: ...

    def getrlimit(self, resource_id: int) -> tuple[int, int]: ...


class OwnedProcessSpawnError(RuntimeError):
    """Safe ownership evidence for a contained process that failed during startup."""

    def __init__(
        self,
        *,
        process_id: int,
        mode: str,
        cleanup_errors: list[str],
        cause: BaseException,
    ) -> None:
        self.process_id = process_id
        self.mode = mode
        self.cleanup_verified = not cleanup_errors
        self.cleanup_errors = tuple(cleanup_errors)
        self.startup_error_type = type(cause).__name__
        self.startup_error_message = str(cause)
        detail = ",".join(cleanup_errors) if cleanup_errors else "none"
        super().__init__(
            "owned process startup failed: "
            f"cause={self.startup_error_type}: {self.startup_error_message}; "
            f"pid={process_id} mode={mode} cleanup_verified={self.cleanup_verified} "
            f"cleanup_errors={detail}"
        )


@dataclass(frozen=True, slots=True)
class _BrokerStartupDiagnostic:
    """Strict, non-sensitive diagnostic published by a containment broker."""

    stage: str
    code: str
    exception_type: str | None
    error_number: int | None
    child_return_code: int | None

    def safe_message(self) -> str:
        """Render only allowlisted fields that cannot contain runtime payloads."""
        fields = [f"stage={self.stage}", f"code={self.code}"]
        if self.exception_type is not None:
            fields.append(f"exception_type={self.exception_type}")
        if self.error_number is not None:
            fields.append(f"errno={self.error_number}")
        if self.child_return_code is not None:
            fields.append(f"child_return_code={self.child_return_code}")
        return "containment broker startup failed: " + " ".join(fields)


@dataclass(frozen=True, slots=True)
class _BrokerStartupRecord:
    """Authenticated completion record read from the private readiness channel."""

    ready: bool
    diagnostic: _BrokerStartupDiagnostic | None


def _reject_broker_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class _OwnedProcessState:
    mode: str
    enforceable: bool
    job_handle: int | None = None
    cgroup_path: Path | None = None
    systemd_unit: str | None = None
    systemd_invocation_id: str | None = None
    systemd_description: str | None = None


@dataclass(slots=True)
class _BrokerReadiness:
    """Pinned bounded readiness channel shared with one containment broker."""

    path: Path
    descriptor: int | None
    token: str
    device: int
    inode: int
    owner: int
    link_count: int
    mode: int

    def anchor(self) -> dict[str, int]:
        """Return the non-secret filesystem identity supplied to the broker."""
        return {
            "device": self.device,
            "inode": self.inode,
            "owner": self.owner,
            "link_count": self.link_count,
            "mode": self.mode,
        }
