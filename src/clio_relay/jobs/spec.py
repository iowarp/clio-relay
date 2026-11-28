"""The job and service specs.

Field names follow `psij-python` 0.9.11 (RL15). Relay adds two things PSI/J
does not have: content-addressed inputs (RL16) and the JARVIS packages a
guarded site takes in place of ``executable`` (RL24). ``packages`` is a JARVIS
pipeline's ordered steps, sharing one allocation; ``package`` is the one-step
shorthand.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import timedelta
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    TypeAdapter,
    WithJsonSchema,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    import psij

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

CustomValue = str | int | float | bool


def _seconds(value: object) -> object:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return timedelta(seconds=value)
    if isinstance(value, str):
        return _TIMEDELTA.validate_python(value)
    return value


_TIMEDELTA: TypeAdapter[timedelta] = TypeAdapter(timedelta)

# Strict validation refuses both a number and a string for a timedelta, so either is
# turned into one first, and the schema offers both forms.
Duration = Annotated[
    timedelta,
    BeforeValidator(_seconds),
    WithJsonSchema(
        {"anyOf": [{"type": "number", "minimum": 0}, {"type": "string", "format": "duration"}]}
    ),
]


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
    cpu_cores_per_process: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Whole physical cores per rank, for a threaded rank. Leave it out for one: JARVIS "
            "gives each rank a whole core by itself. Relay asks the scheduler for these cores "
            "in its own unit (Slurm's hardware threads)"
        ),
    )
    gpu_cores_per_process: int | None = Field(default=None, ge=1)
    exclusive_node_use: bool = Field(
        default=False,
        description="Keep the nodes to this job; otherwise cores it leaves free can go to others",
    )
    memory: int | None = Field(default=None, ge=1, description="Bytes of memory per node")

    def nodes(self) -> int:
        """Return the node count this request occupies, for pricing."""
        if self.node_count is not None:
            return self.node_count
        if self.process_count is not None and self.processes_per_node is not None:
            return max(1, -(-self.process_count // self.processes_per_node))
        return 1

    def ranks_per_node(self) -> int | None:
        """Return the processes (MPI ranks) each node runs, or None when the job leaves it out."""
        if self.processes_per_node is not None:
            return self.processes_per_node
        if self.process_count is not None:
            return max(1, -(-self.process_count // self.nodes()))
        return None

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

    duration: Duration | None = Field(
        default=None,
        description=(
            "Walltime, as seconds or an ISO 8601 duration (PT2H). Leave it out to use the "
            "site's default walltime or, with none, the scheduler's own limit"
        ),
    )
    queue_name: str | None = Field(
        default=None,
        description=(
            "The partition or queue. Leave it out unless the person names one: relay uses "
            "the site's defaults.queue (describe shows it) or, with none, the scheduler's"
        ),
    )
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
        # None, which sends no walltime at all, though psij's types do not say so.
        attributes.duration = self.duration  # pyright: ignore[reportAttributeAccessIssue]
        return attributes


class InputRef(_Spec):
    """One input file, named by its SHA-256 and placed at ``name`` in the job directory."""

    name: str = Field(min_length=1, description="Relative path inside the job directory")
    sha256: str
    transient: bool = Field(
        default=False,
        description="Never outlives its job: relay and the site drop their copies when it ends",
    )

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
    """A reviewed JARVIS package, pinned by version and hash (RL24).

    A request may leave out ``version`` and ``sha256``. Relay then fills them
    from its package catalog before the guard sees the job, so what is stored,
    reviewed and run is always pinned.
    """

    name: str = Field(min_length=1, description="<repository>.<package>")
    version: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "Leave out when relay has one archive of the package; with several, relay "
            "refuses and lists them, so pin one (describe(site=) lists them)"
        ),
    )
    sha256: str | None = Field(
        default=None,
        description="The archive's SHA-256. Leave out to use the catalogued archive",
    )
    parameters: dict[str, CustomValue] = Field(
        default_factory=dict[str, CustomValue],
        description=(
            "The package's parameters. To pass a run input, set the parameter to the input's "
            "name, such as body.stl; relay puts in its absolute path on the site. Do not "
            "write a directory placeholder: nothing on the site expands one"
        ),
    )
    service: bool = Field(
        default=False,
        description=(
            "Run the package as a service: start it and keep it up until it is stopped. "
            "Optional: in a serve pipeline JARVIS decides which step serves, on which "
            "node and when it starts, from each package's deployment profile, host_slice "
            "and start_mode, and relay's connector runs on that node."
        ),
    )

    def nodes(self) -> str | None:
        """Return the ``host_slice`` parameter, the allocation's nodes this step gets."""
        value = self.parameters.get("host_slice")
        return value if isinstance(value, str) and value.strip() else None

    @field_validator("sha256")
    @classmethod
    def _digest(cls, value: str | None) -> str | None:
        return None if value is None else _check_digest(value)

    def pinned(self) -> tuple[str, str]:
        """Return ``(version, sha256)``, which relay fills before a job is checked."""
        if self.version is None or self.sha256 is None:
            raise ValueError(f"package {self.name} is not pinned to a version and SHA-256")
        return self.version, self.sha256


COUPLING_KINDS = ("stream", "file", "service", "interceptor", "control", "other")


class Coupling(_Spec):
    """Data moving from one pipeline step to another: producer to consumer.

    The direction is the data's, not the order JARVIS starts the steps in: JARVIS
    starts a service before the solver that feeds it. Relay declares each
    coupling to JARVIS as the pipeline's ``couplings:``.
    """

    model_config = ConfigDict(
        extra="forbid", validate_by_alias=True, validate_by_name=True, serialize_by_alias=True
    )

    producer: str = Field(
        alias="from",
        min_length=1,
        description=(
            "The step whose data flows out: its package, e.g. clio_jarvis.gray_scott, or its "
            "step name when the package appears twice (gray_scott_2)"
        ),
    )
    consumer: str = Field(alias="to", min_length=1, description="The step that reads it")
    kind: Literal["stream", "file", "service", "interceptor", "control", "other"] = "stream"
    label: str | None = Field(default=None, max_length=120)


def step_ids(steps: list[PackageRef]) -> list[str]:
    """Return each step's ``pkg_name`` in the pipeline.

    Each is named by its package, with ``_2``, ``_3`` for a package that appears
    again.
    """
    names: list[str] = []
    for step in steps:
        base = step.name.partition(".")[2] or step.name
        name, count = base, 1
        while name in names:
            count += 1
            name = f"{base}_{count}"
        names.append(name)
    return names


def step_id(steps: list[PackageRef], end: str) -> str:
    """Return the step id a coupling's end names: a step id, else one step's package."""
    ids = step_ids(steps)
    if end in ids:
        return end
    found = [
        id_
        for id_, step in zip(ids, steps, strict=True)
        if end in (step.name, step.name.partition(".")[2])
    ]
    if len(found) != 1:
        named = "names no step" if not found else "names more than one step"
        raise ValueError(f"coupling end {end!r} {named}; use one of {', '.join(ids)}")
    return found[0]


def flow_order(ids: list[str], edges: list[tuple[str, str]]) -> list[str]:
    """Return ``ids`` producer before consumer along ``edges``, else in their given order.

    ``ids`` come in launch order, which keeps ties and anything a cycle leaves.
    """
    feeds = {id_: 0 for id_ in ids}
    for _producer, consumer in edges:
        if consumer in feeds:
            feeds[consumer] += 1
    ordered: list[str] = []
    left = list(ids)
    while left:
        ready = next((id_ for id_ in left if feeds[id_] == 0), left[0])
        left.remove(ready)
        ordered.append(ready)
        for producer, consumer in edges:
            if producer == ready and consumer in left:
                feeds[consumer] -= 1
    return ordered


class Layout(_Spec):
    """A way to lay a job out on the allocation: what the agent chose, or an alternative."""

    nodes: int = Field(ge=1)
    ranks_per_node: int | None = Field(default=None, ge=1)
    threads_per_rank: int | None = Field(default=None, ge=1)
    walltime_s: float | None = Field(default=None, gt=0)
    memory_mb: int | None = Field(default=None, ge=1, description="Per node")
    queue: str | None = None
    note: str | None = Field(default=None, description="Why not this one, in a few words")


class Sizing(_Spec):
    """Why a job asks for its layout, and what else was weighed. The spend ask shows it."""

    rationale: str = Field(
        min_length=1,
        max_length=600,
        description="Why these nodes, ranks and walltime, e.g. the grid and the partition",
    )
    alternatives: list[Layout] = Field(
        default_factory=list[Layout],
        max_length=4,
        description="Other layouts that would work, each with a note",
    )


class InstallSetting(_Spec):
    """An answer to one of the install settings a JARVIS failure record offered.

    JARVIS offers a site's install settings (its temp dir, for one) under a
    failure record's ``settings``, each with its ``issue_id``, its question and
    what an answer does. Relay sends the value with the job's install answers;
    JARVIS records it for the site. Relay takes only an issue id a failure on
    the same site offered.
    """

    issue_id: str = Field(
        pattern=r"^sha256:[0-9a-f]{64}$", description="The setting's issue_id from the record"
    )
    value: str = Field(min_length=1, max_length=4096, description="The answer, as JARVIS asks it")
    basis: str | None = Field(
        default=None,
        max_length=2048,
        description="Why this value, e.g. what the person said; JARVIS records it",
    )


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
    package: PackageRef | None = Field(
        default=None, description="A one-step pipeline: the same as packages with one entry"
    )
    packages: list[PackageRef] = Field(
        default_factory=list[PackageRef],
        description=(
            "A JARVIS pipeline's steps, in the order JARVIS starts them, sharing the job's "
            "allocation. Give each its nodes with the host_slice parameter, e.g. '0:12'"
        ),
    )
    sizing: Sizing | None = Field(
        default=None,
        description=(
            "Why the job asks for its nodes, ranks per node and walltime, and the "
            "alternatives. The person sees them in the spend ask"
        ),
    )
    couplings: list[Coupling] = Field(
        default_factory=list[Coupling],
        description=(
            "Which way data moves between a pipeline's steps, producer to consumer, e.g. "
            "{from: clio_jarvis.gray_scott, to: clio_jarvis.pvserver}. Optional: relay "
            "takes them from the packages' contracts when left out. The packages' order is "
            "the order JARVIS starts them in, not the data's direction"
        ),
    )
    install_settings: list[InstallSetting] = Field(
        default_factory=list[InstallSetting],
        max_length=16,
        description=(
            "Answers to install settings an earlier install failure on this site offered "
            "(its failure records' settings, by issue_id). JARVIS gets them with the "
            "install answers and records them for the site"
        ),
    )

    @field_validator("environment")
    @classmethod
    def _environment_names(cls, value: dict[str, str]) -> dict[str, str]:
        # The site command passes each entry to env as NAME=VALUE; a name is never an option.
        bad = [key for key in value if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)]
        if bad:
            raise ValueError(f"environment names must be shell variable names: {bad}")
        return value

    @model_validator(mode="after")
    def _one_entry_point(self) -> Self:
        if self.install_settings and not self.steps():
            raise ValueError("install_settings go with a package job")
        if self.package is not None and self.packages:
            raise ValueError("give package or packages, not both")
        if (self.executable is None) == (not self.steps()):
            raise ValueError("set package: {name: ...} or executable")
        if self.couplings:
            steps = self.steps()
            if len(steps) < 2:
                raise ValueError("couplings join the steps of a pipeline of two or more")
            for edge in self.couplings:
                if step_id(steps, edge.producer) == step_id(steps, edge.consumer):
                    raise ValueError(f"coupling {edge.producer!r} feeds itself")
        return self

    def steps(self) -> list[PackageRef]:
        """Return the JARVIS pipeline's packages, in order; empty for a command job."""
        if self.package is not None:
            return [self.package]
        return list(self.packages)

    def with_steps(self, steps: list[PackageRef]) -> JobSpec:
        """Return a copy with ``steps`` in place of the packages, in the same shape."""
        if self.package is not None and len(steps) == 1:
            return self.model_copy(update={"package": steps[0]})
        return self.model_copy(update={"package": None, "packages": steps})

    def dump(self) -> dict[str, Any]:
        """Return the JSON form. Empty ``packages``, ``couplings``, ``sizing`` and
        ``install_settings`` are left out, so older digests hold."""
        data = self.model_dump(mode="json")
        for key in ("packages", "couplings", "install_settings"):
            if not data.get(key):
                data.pop(key, None)
        if data.get("sizing") is None:
            data.pop("sizing", None)
        return data

    def digest(self) -> str:
        """Return the SHA-256 of the canonical JSON form. Approvals bind to it."""
        return canonical_digest(self.dump())

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
            raise ValueError("a package job is rendered by JARVIS, not by PSI/J")
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
    idle_release: Duration | None = Field(
        default=None, description="Seconds or an ISO 8601 duration of idleness before release"
    )


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

    def dump(self) -> dict[str, Any]:
        """Return the JSON form, with the job's as :meth:`JobSpec.dump`."""
        data = self.model_dump(mode="json")
        data["job"] = self.job.dump()
        return data

    def digest(self) -> str:
        """Return the SHA-256 of the canonical JSON form."""
        return canonical_digest(self.dump())


def canonical_digest(value: Any) -> str:
    """Return the SHA-256 of ``value`` as sorted, compact JSON."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
