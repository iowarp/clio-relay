"""What adapters return: job handles and states, exec results, endpoints, file access."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, Field, computed_field

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


VIEW_METADATA = "view"
"""The ``OutputRef.metadata`` key for an output's view descriptor (RL40, RL44)."""

PROVENANCE_METADATA = "provenance"
"""The ``OutputRef.metadata`` key for an output's provenance (RL40)."""


class OutputRef(BaseModel):
    """A file a job produced.

    ``metadata`` is JSON the adapter or package attaches, and relay passes it on
    unchanged. Two keys have a set meaning. ``view`` is the output's view
    descriptor: the path or path pattern, the format, the ParaView reader and
    that reader's configuration, and whether the output is still being written.
    ``provenance`` is the package version and hash and the digests of its inputs.
    """

    name: str
    path: str
    size: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict[str, Any])


class JobProgress(BaseModel):
    """The latest step a running job reported (RL40 progress, RL43).

    Relay keeps it in the operation's ``result["progress"]``, names it in the
    task's status message, and returns it with the operation. ``outputs`` are
    what the step wrote, each with its view descriptor in ``metadata["view"]``.
    ``data`` belongs to the adapter or package and must be JSON.
    """

    step: int | None = None
    total: int | None = None
    message: str | None = None
    outputs: list[OutputRef] = Field(default_factory=list[OutputRef])
    data: dict[str, Any] = Field(default_factory=dict[str, Any])


class JobStatus(BaseModel):
    """One observation of a job."""

    state: JobState
    exit_code: int | None = None
    message: str | None = None
    native_state: str | None = None
    observed_at: datetime = Field(default_factory=utc_now)
    outputs: list[OutputRef] = Field(default_factory=list[OutputRef])
    progress: JobProgress | None = None
    control: dict[str, Any] | None = Field(
        default=None,
        description=(
            "How a cancel or stop went, as the adapter's scheduler reports it: for a JARVIS "
            "job, the execution record's cancel_request, stop_request and termination"
        ),
    )
    queue: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Where a pending job stands, as JARVIS reports it (jarvis.scheduler.queue.v1): "
            "its partition, the reason it waits, the estimated start, and the other "
            "partitions with whether the job fits there now"
        ),
    )
    issues: list[dict[str, Any]] = Field(
        default_factory=list[dict[str, Any]],
        description=(
            "JARVIS's typed issues (jarvis.issue.v1) on the execution, such as an "
            "mpi_network issue naming the resource graph field to change"
        ),
    )
    site_facts: dict[str, Any] | None = Field(
        default=None,
        description=(
            "JARVIS's metadata.site_facts on the execution (jarvis.site-facts.v1), untouched: "
            "the advisory site_fact_uncertain questions the run went ahead with"
        ),
    )
    service_error: dict[str, Any] | None = Field(
        default=None,
        description=(
            "For a service: a refusal's wire form saying why relay cannot bring it to the "
            "desktop, such as host_key_unverified. The job itself runs on"
        ),
    )
    components: list[dict[str, Any]] = Field(
        default_factory=list[dict[str, Any]],
        description=(
            "For a JARVIS job: each package's latest state as JARVIS records it "
            "(components.jsonl, jarvis.job.v1): component, package, state (waiting, "
            "starting, running, done, failed or cancelled), hosts, at and reason"
        ),
    )


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

    @computed_field
    @property
    def address(self) -> str:
        """``host:port`` as one string, the form to hand to a client (IPv6 in brackets)."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}"


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
    """File operations on one site, under the rules in ``clio_relay.jobs.file_rules``."""

    async def list(self, path: str) -> list[FileEntry]:
        """List a directory."""
        ...

    async def stat(self, path: str) -> FileEntry:
        """Describe one path. Refuses with ``path_not_found``."""
        ...

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        """Read bytes from a file. A dropped download resumes at ``offset``."""
        ...

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        """Write a file. Refuses with ``path_exists`` unless ``overwrite``.

        With ``offset == 0`` and ``final`` the write is whole. Otherwise it is
        one chunk of an upload into ``<path>.relay-part``, which the chunk
        with ``final`` moves into place; a chunk returns the part's entry.
        """
        ...

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        """Delete a file, a symlink or a directory (``recursive`` if not empty)."""
        ...

    async def stage(self, data: bytes) -> str:
        """Put bytes into the site's content store and return their SHA-256 (RL16)."""
        ...

    async def has(self, digest: str) -> bool:
        """Return whether the site's content store holds ``digest``."""
        ...
