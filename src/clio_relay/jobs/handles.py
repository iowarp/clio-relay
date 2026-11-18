"""What adapters return: job handles and states, exec results, endpoints, file access."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from clio_relay.jobs.spec import PortProtocol


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


class JobState(StrEnum):
    """The lifecycle of a job, as the conformance kit checks it."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def final(self) -> bool:
        """Return whether the job can change state again."""
        return self in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED)


class JobHandle(BaseModel):
    """What an adapter needs to find a job again, from any relay process.

    ``data`` belongs to the adapter and must be JSON. Relay keeps the handle in
    the operation record, so a restarted relay can poll and cancel the job.
    """

    adapter: str
    native_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict[str, Any])


class OutputRef(BaseModel):
    """A file a job produced."""

    name: str
    path: str
    size: int | None = None


class JobStatus(BaseModel):
    """One observation of a job."""

    state: JobState
    exit_code: int | None = None
    message: str | None = None
    native_state: str | None = None
    observed_at: datetime = Field(default_factory=utc_now)
    outputs: list[OutputRef] = Field(default_factory=list[OutputRef])


class ExecResult(BaseModel):
    """Bounded output of one short command."""

    argv: list[str]
    exit_code: int | None
    stdout: str
    stderr: str
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    timed_out: bool = False
    duration_s: float


STALL_S = 30.0
"""Seconds without output before a command with no time limit is reported quiet."""

StallReport = Callable[[float, float], Awaitable[None]]
"""Called while a command is quiet, with the seconds since its last output and
the seconds until the next check. The command keeps running; the person may
cancel the call."""

class EndpointPort(BaseModel):
    """One reachable port of a service."""

    name: str
    protocol: PortProtocol
    host: str
    port: int
    url: str | None = None


class Endpoint(BaseModel):
    """Where a running service can be reached (RL19 to RL23)."""

    service: str
    ports: list[EndpointPort] = Field(default_factory=list[EndpointPort])
    durable: bool = True
    connect_command: str | None = None
    note: str | None = None


class FileEntry(BaseModel):
    """One entry of a directory listing."""

    path: str
    kind: Literal["file", "dir", "other"]
    size: int | None = None
    modified: datetime | None = None


@runtime_checkable
class FileAccess(Protocol):
    """File operations on one site. Paths are the site's own paths."""

    async def list(self, path: str) -> list[FileEntry]:
        """List a directory."""
        ...

    async def stat(self, path: str) -> FileEntry:
        """Describe one path. Refuses with ``path_not_found``."""
        ...

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        """Read bytes from a file."""
        ...

    async def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileEntry:
        """Write a file. Refuses with ``path_exists`` unless ``overwrite``."""
        ...

    async def stage(self, data: bytes) -> str:
        """Put bytes into the site's content store and return their SHA-256 (RL16)."""
        ...

    async def has(self, digest: str) -> bool:
        """Return whether the site's content store holds ``digest``."""
        ...
