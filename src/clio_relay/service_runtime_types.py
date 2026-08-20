"""Pure wire/result types and the local-command-runner protocol.

Extracted from ``service_runtime.py`` (#231 rework slice): three narrow
``RelayError`` subclasses used to discriminate observation/reconciliation
failures by type rather than by message match, five frozen dataclasses
describing captured process/scheduler identity, and the ``CommandRunner``
Protocol the supervisor depends on for local process execution.

These have zero dependency on any other piece of the service-runtime split --
every other extracted owner module (``service_runtime_connector_identity``,
``service_runtime_command_runner``, ``service_runtime_scheduler_contracts``,
``service_runtime_readiness``, and the supervisor class that remains in
``service_runtime.py``) imports from here, never the reverse.
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import httpx

from clio_relay.errors import RelayError
from clio_relay.models import ServiceRuntimeSpec


class _DefinitiveRuntimeObservationError(RelayError):
    """An observation that proves this runtime cannot safely become ready."""


class _AmbiguousRemoteSideEffectError(RelayError):
    """A remote command may have completed after its transport observation was lost."""


class _DefinitiveSubmissionReconciliationError(RelayError):
    """An exact submission sidecar proves that its submission cannot be resumed."""

    def __init__(
        self,
        message: str,
        *,
        evidence: dict[str, object],
        failure_kind: Literal[
            "command_failure",
            "integrity_failure",
            "response_invalid",
        ],
    ) -> None:
        super().__init__(message)
        self.evidence = evidence
        self.failure_kind = failure_kind

    @property
    def queue_state(self) -> str:
        """Return a queue state that does not overclaim scheduler disposition."""
        return {
            "command_failure": "submission_failed",
            "integrity_failure": "submission_integrity_failed",
            "response_invalid": "submission_response_invalid",
        }[self.failure_kind]

    @property
    def scheduler_submission_outcome(self) -> str:
        """Describe only what the durable evidence proves about submission."""
        return {
            "command_failure": "submit_command_failed",
            "integrity_failure": "unknown_due_to_integrity_failure",
            "response_invalid": "unknown_due_to_invalid_response",
        }[self.failure_kind]


@dataclass(frozen=True)
class LocalConnectorIdentity:
    """Immutable identity captured for an owned desktop connector process group."""

    pid: int
    process_group_id: int
    process_start_marker: str
    owner_token: str


@dataclass(frozen=True)
class _BoundedHttpResponse:
    """Response metadata plus an optional fully consumed, caller-bounded body."""

    status_code: int
    headers: httpx.Headers
    content: bytes


@dataclass
class _BoundedHttpReadState:
    """Cross-thread state for one absolute-deadline HTTP response read."""

    result: _BoundedHttpResponse | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _ObservedLocalProcess:
    pid: int
    process_group_id: int
    process_start_marker: str
    command_line: str
    environment: bytes | None


@dataclass(frozen=True)
class _VerifiedSchedulerSubmission:
    """Scheduler identity proven against the relay-created remote sidecar."""

    provider: str
    scheduler_job_id: str
    spec: ServiceRuntimeSpec


@dataclass(frozen=True)
class _DurableSchedulerContract:
    """Scheduler identity or explicit absence proven by durable gateway state."""

    provider: str
    scheduler_job_id: str | None
    unresolved_submission: bool = False


class CommandRunner(Protocol):
    """Protocol for local command execution used by the supervisor."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a command and return the completed process."""
        ...

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
        env: dict[str, str] | None = None,
        isolate_process_group: bool = False,
        input_bytes: bytes | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start a long-running local process."""
        ...

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> LocalConnectorIdentity:
        """Capture and verify immutable process identity after launch."""
        ...
