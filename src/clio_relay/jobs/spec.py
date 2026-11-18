"""The job and service specs.

Field names follow `psij-python` 0.9.11 (RL15). Relay adds two things PSI/J
does not have: content-addressed inputs (RL16) and a package reference that a
guarded site takes in place of ``executable`` (RL24).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import timedelta
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

if TYPE_CHECKING:
    import psij

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

CustomValue = str | int | float | bool


def _check_digest(value: str) -> str:
    value = value.lower()
    if not SHA256_PATTERN.match(value):
        raise ValueError("sha256 must be 64 hex characters")
    return value


class _Spec(BaseModel):
    model_config = ConfigDict(extra="forbid")


class JobResources(_Spec):
    """PSI/J ``ResourceSpecV1`` fields."""

    node_count: int | None = Field(default=None, ge=1)
    process_count: int | None = Field(default=None, ge=1)
    processes_per_node: int | None = Field(default=None, ge=1)
    cpu_cores_per_process: int | None = Field(default=None, ge=1)
    gpu_cores_per_process: int | None = Field(default=None, ge=1)
    exclusive_node_use: bool = False
    memory: int | None = Field(default=None, ge=1, description="Bytes of memory per node")

    def nodes(self) -> int:
        """Return the node count this request occupies, for pricing."""
        if self.node_count is not None:
            return self.node_count
        if self.process_count is not None and self.processes_per_node is not None:
            return max(1, -(-self.process_count // self.processes_per_node))
        return 1

    def to_psij(self) -> psij.ResourceSpecV1:
        """Build a ``psij.ResourceSpecV1``. The bare ``ResourceSpec`` is abstract."""
        from psij import ResourceSpecV1

        return ResourceSpecV1(
            node_count=self.node_count,
            process_count=self.process_count,
            processes_per_node=self.processes_per_node,
            cpu_cores_per_process=self.cpu_cores_per_process,
            gpu_cores_per_process=self.gpu_cores_per_process,
            exclusive_node_use=self.exclusive_node_use,
            memory=self.memory,
        )


class JobAttributes(_Spec):
    """PSI/J ``JobAttributes`` fields."""

    duration: timedelta | None = Field(
        default=None,
        description=(
            "Walltime, as seconds or an ISO 8601 duration. Leave it out to use the "
            "site's default walltime or, with none, the scheduler's own limit"
        ),
    )
    queue_name: str | None = None
    account: str | None = None
    reservation_id: str | None = None
    project_name: str | None = None
    custom_attributes: dict[str, CustomValue] = Field(default_factory=dict[str, CustomValue])

    def to_psij(self) -> psij.JobAttributes:
        """Build a ``psij.JobAttributes``."""
        from psij import JobAttributes as PsijJobAttributes

        custom: dict[str, object] | None = dict(self.custom_attributes) or None
        attributes = PsijJobAttributes(
            queue_name=self.queue_name,
            account=self.account,
            reservation_id=self.reservation_id,
            project_name=self.project_name,
            custom_attributes=custom,
        )
        # psij's constructor defaults to 10 minutes and refuses None; the setter takes
        # None, which sends no walltime at all.
        attributes.duration = self.duration
        return attributes


class InputRef(_Spec):
    """One input file, named by its SHA-256 and placed at ``name`` in the job directory."""

    name: str = Field(min_length=1, description="Relative path inside the job directory")
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return _check_digest(value)

    @field_validator("name")
    @classmethod
    def _relative(cls, value: str) -> str:
        path = PurePosixPath(value.replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts or str(path) in ("", "."):
            raise ValueError("input names are relative paths inside the job directory")
        return str(path)


class PackageRef(_Spec):
    """A reviewed JARVIS package, pinned by version and hash (RL24)."""

    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    sha256: str
    parameters: dict[str, CustomValue] = Field(default_factory=dict[str, CustomValue])

    @field_validator("sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return _check_digest(value)


class JobSpec(_Spec):
    """A job, in PSI/J terms, plus relay's inputs and package reference."""

    executable: str | None = None
    arguments: list[str] = Field(default_factory=list[str])
    directory: str | None = None
    name: str | None = None
    environment: dict[str, str] = Field(default_factory=dict[str, str])
    inherit_environment: bool = True
    stdin_path: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    launcher: str | None = None
    resources: JobResources = Field(default_factory=JobResources)
    attributes: JobAttributes = Field(default_factory=JobAttributes)
    inputs: list[InputRef] = Field(default_factory=list[InputRef])
    package: PackageRef | None = None

    @model_validator(mode="after")
    def _one_entry_point(self) -> Self:
        if (self.executable is None) == (self.package is None):
            raise ValueError("give exactly one of executable or package")
        return self

    def digest(self) -> str:
        """Return the SHA-256 of the canonical JSON form. Approvals bind to it."""
        return canonical_digest(self.model_dump(mode="json"))

    def node_hours(self) -> float | None:
        """Return the most node-hours this job may consume, or None when no walltime is set."""
        duration = self.attributes.duration
        if duration is None:
            return None
        return self.resources.nodes() * duration.total_seconds() / 3600.0

    def to_psij(
        self,
        *,
        directory: str | None = None,
        stdout_path: str | None = None,
        stderr_path: str | None = None,
        name: str | None = None,
    ) -> psij.JobSpec:
        """Build a ``psij.JobSpec``. The keywords fill paths relay chose for the job."""
        from psij import JobSpec as PsijJobSpec

        if self.executable is None:
            raise ValueError("a package job is rendered by its package runner, not by PSI/J")
        environment: dict[str, str | int] | None = dict(self.environment) or None
        return PsijJobSpec(
            executable=self.executable,
            arguments=list(self.arguments),
            directory=self.directory or directory,
            name=name or self.name,
            inherit_environment=self.inherit_environment,
            environment=environment,
            stdin_path=self.stdin_path,
            stdout_path=self.stdout_path or stdout_path,
            stderr_path=self.stderr_path or stderr_path,
            resources=self.resources.to_psij(),
            attributes=self.attributes.to_psij(),
            launcher=self.launcher,
        )


PortProtocol = Literal["tcp", "http", "ws", "mcp"]


class ServicePort(_Spec):
    """One port a service listens on (RL19)."""

    name: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_-]*$")
    port: int | None = Field(default=None, ge=1, le=65535)
    protocol: PortProtocol = "tcp"
    health_path: str | None = None
    protection: str | None = Field(
        default=None,
        description="How the service protects this port. Required for http and ws (RL22)",
    )


class ServiceLifetime(_Spec):
    """How long a service lives (RL19)."""

    bound_to: str | None = Field(default=None, description="Operation id of a job it ends with")
    idle_release: timedelta | None = None


class ServiceSpec(_Spec):
    """A service is a job with ports (RL19)."""

    job: JobSpec
    ports: list[ServicePort] = Field(min_length=1)
    lifetime: ServiceLifetime = Field(default_factory=ServiceLifetime)

    @model_validator(mode="after")
    def _protected(self) -> Self:
        for port in self.ports:
            if port.protocol in ("http", "ws") and not port.protection:
                raise ValueError(
                    f"port {port.name!r} is {port.protocol}: say how it is protected (RL22)"
                )
        return self

    def digest(self) -> str:
        """Return the SHA-256 of the canonical JSON form."""
        return canonical_digest(self.model_dump(mode="json"))


def canonical_digest(value: Any) -> str:
    """Return the SHA-256 of ``value`` as sorted, compact JSON."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
