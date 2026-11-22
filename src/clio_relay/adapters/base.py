"""The adapter contract: everything an adapter implements and uses.

An adapter is one way into a site: SSH, a facility's REST API, a transfer
service, a cloud. It is a subclass of :class:`BaseAdapter` with three class
attributes and the verbs it supports. Everything it needs is importable from
this module; docs/adding-an-adapter.md walks through it and
``examples/adapter-template`` is a complete one to copy.

What an adapter declares:

- ``name``: the adapter's name in ``sites.yaml`` and the entry point.
- ``Config``: a pydantic model of a site's settings. Its JSON Schema validates
  ``sites.yaml`` and draws the Compute tab's form. Use ``extra="forbid"``.
- ``implements``: the verbs it supports, from :data:`VERBS`.

What it implements, by verb (each one it leaves out refuses with
``verb_not_implemented``, which the conformance kit checks):

- ``describe``: :meth:`BaseAdapter.describe`, the probe. Report the site; it
  must work without side effects.
- ``run``: :meth:`~BaseAdapter.run`, :meth:`~BaseAdapter.status`,
  :meth:`~BaseAdapter.cancel` and :meth:`~BaseAdapter.find`. Handles are JSON
  and outlive the process: a fresh instance must observe and cancel a job.
  Inputs named by digest come from ``context.content``.
- ``exec``: :meth:`~BaseAdapter.exec`, one bounded command. A time limit
  returns ``timed_out``; with none, report quiet stretches to ``on_stall``
  from :data:`STALL_S` on, and stop the command when the call is cancelled.
- ``files``: :meth:`~BaseAdapter.files` returns a
  :class:`~clio_relay.jobs.handles.FileAccess` under docs/files.md. A remote
  site subclasses :class:`SiteFiles` and implements six primitives.
- ``serve``: :meth:`~BaseAdapter.serve`, used by the serve hook.

Sign-in and secrets: :meth:`~BaseAdapter.sign_in_needed` returns a
:class:`SignInNeed` when the adapter cannot act yet. ``provider`` is
``keyring:<name>`` for a key the person types on relay's page, which the
adapter then reads with :meth:`AdapterContext.secret`, or the provider's own
``url`` for a browser sign-in, after which relay calls
:meth:`~BaseAdapter.sign_in_done`.

Errors: every failure the person or the agent can act on is a :class:`Refusal`
with one :class:`RefusalReason`; clients branch on the reason. A missing token
is ``needs_sign_in``, a facility that does not answer is ``unreachable``, a bad
setting is ``invalid_config``, and a missing path is ``path_not_found``.

Policy: a site is open or guarded. The guard sits in front of every adapter,
so an adapter does not enforce it. A JARVIS pipeline job (:meth:`JobSpec.steps`:
``package``, or the ordered ``packages``) goes only to an adapter that sets
``runs_packages``, which hands it to ``jarvis ppl submit`` on a batch site
(:mod:`clio_relay.jarvis_submit`), or to JARVIS's direct run on a machine with
no scheduler. Relay never turns a package into its own command job inside an
allocation; any other adapter refuses it
(:func:`clio_relay.jarvis_submit.not_run_here`). Such an adapter also
implements the JARVIS lifecycle:

- ``cancel`` and :meth:`~BaseAdapter.stop` run ``jarvis execution cancel`` and
  ``stop``; ``stop`` lets the pipeline run ``ppl stop`` and finish.
- When ``run`` finds software to install, it raises
  :func:`clio_relay.jarvis_report.install_needed`; relay asks the person and calls
  :meth:`~BaseAdapter.install` with the answers, then ``run`` again.
- When the site has no JARVIS, ``run`` raises
  :func:`clio_relay.jarvis_submit.missing_jarvis`; after the person agrees, relay
  calls :meth:`~BaseAdapter.install_jarvis`.

Optional hooks, found by name when an adapter defines them:

- ``on_site_added(site)``: :class:`SiteAdded`; set the site up when it is saved.
- ``job_name(operation_id) -> str``: the scheduler job name relay shows.
- ``job_dir(handle) -> str | None``: the job's directory, as ``files`` names it,
  when it is not ``handle.data["job_dir"]``. Relay reads a package job's live
  step and outputs from the JARVIS records there (see
  :mod:`clio_relay.surface.package_progress`) unless ``status`` reports them.
- ``price(spec) -> Charge``: what a job would cost, for the spend question.
- ``system_id()``: the facility's own id for the site, shown in the tab.
- ``ssh_argv``, ``hop_exec``, ``transport``: SSH-based adapters only, for the
  serve hook's port forwarding.

Registration: an entry point in the group ``clio_relay.adapters`` names the
class (see :mod:`clio_relay.adapters.registry`). Relay's own code does not
change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, get_args, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters.site_files import Kind, SiteFiles
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import (
    STALL_S,
    Endpoint,
    EndpointPort,
    ExecResult,
    FileAccess,
    FileEntry,
    JobHandle,
    JobProgress,
    JobState,
    JobStatus,
    OutputRef,
    StallReport,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobSpec, PackageRef, ServiceSpec

if TYPE_CHECKING:
    from clio_relay.sites.models import SiteEntry

Verb = Literal["describe", "run", "exec", "files", "serve"]
VERBS: tuple[Verb, ...] = get_args(Verb)


class PartitionInfo(BaseModel):
    """One batch partition (queue) of a site, as its scheduler reports it live.

    Per-node figures are what every node of the partition has: the smallest
    when its nodes differ. ``max_time_s`` is None when the partition has no
    limit or relay could not read it; ``max_time`` is the scheduler's own text.
    """

    name: str
    default: bool = False
    nodes: int = 0
    cpus_per_node: int | None = None
    sockets: int | None = None
    cores_per_socket: int | None = None
    threads_per_core: int | None = None
    physical_cores: int | None = Field(default=None, description="sockets x cores per socket")
    memory_mb: int | None = None
    max_time: str | None = None
    max_time_s: int | None = None
    allocated: int = 0
    idle: int = 0
    other: int = Field(default=0, description="down, drained or otherwise not usable")
    gres: list[str] = Field(default_factory=list[str])
    features: list[str] = Field(default_factory=list[str])


class SiteInfo(BaseModel):
    """What an adapter reports about its site.

    ``partitions`` are the batch partitions with their limits and live node
    counts, empty when the adapter cannot read them. ``mpi`` is what the site
    offers for MPI (the scheduler's MPI plugins, the ``mpiexec`` on the PATH),
    for the person and the agent sizing a run.
    """

    adapter: str
    address: str | None = None
    scheduler: str | None = Field(default=None, description="slurm, pbs, lsf, cobalt or none")
    queues: list[str] = Field(default_factory=list[str])
    accounts: list[str] = Field(default_factory=list[str])
    limits: dict[str, Any] = Field(default_factory=dict[str, Any])
    partitions: list[PartitionInfo] = Field(default_factory=list[PartitionInfo])
    mpi: dict[str, Any] = Field(default_factory=dict[str, Any])
    facts: dict[str, Any] = Field(default_factory=dict[str, Any])
    verbs: list[Verb] = Field(default_factory=list[Verb])


class SignInNeed(BaseModel):
    """An adapter needs the person to sign in before it can act.

    ``url`` is the provider's own ``https`` page, or ``None`` when relay should
    serve its own single-use page through the ask provider.
    """

    model_config = ConfigDict(extra="forbid")

    provider: str
    message: str
    url: str | None = None


@dataclass(frozen=True)
class AdapterContext:
    """What relay gives an adapter when it builds one.

    ``state_dir`` is the adapter's own directory for this site; ``content`` is
    relay's content store, where inputs named by digest are.
    """

    site_id: str
    state_dir: Path
    content: ContentStore

    def secret(self, name: str) -> str | None:
        """Return a secret the person typed on relay's page, such as an API key.

        Ask for it by returning ``SignInNeed(provider=f"keyring:{name}", ...)``.
        """
        from clio_relay.identity.store import TokenStore

        return TokenStore().secret(name)


@runtime_checkable
class Adapter(Protocol):
    """The interface every adapter implements."""

    name: ClassVar[str]
    Config: ClassVar[type[BaseModel]]
    implements: ClassVar[frozenset[Verb]]

    async def describe(self) -> SiteInfo:
        """Report the site."""
        ...

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        """Submit a job. ``operation_id`` names it, so :meth:`find` can reconcile."""
        ...

    async def status(self, handle: JobHandle) -> JobStatus:
        """Observe a job, from any relay process."""
        ...

    async def cancel(self, handle: JobHandle) -> JobStatus:
        """Cancel a job and return what was observed after."""
        ...

    async def find(self, operation_id: str) -> JobHandle | None:
        """Find a job submitted for ``operation_id`` whose handle relay never recorded."""
        ...

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        """Run one command and return bounded output.

        With no `timeout_s` the command runs until it ends; the adapter calls
        `on_stall` while it is quiet, and cancelling the call stops it.
        """
        ...

    async def files(self) -> FileAccess:
        """Return file access on the site."""
        ...

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        """Start a service. Used by the serve hook."""
        ...

    async def sign_in_needed(self) -> SignInNeed | None:
        """Return what sign-in the adapter needs before acting, or ``None``."""
        ...

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        """Take the result of a completed sign-in ask."""
        ...


@runtime_checkable
class SiteAdded(Protocol):
    """An adapter that sets a site up when relay adds or enables it. Optional.

    Relay calls ``on_site_added`` on the adapter in use and on every alternative that
    implements it, after the site is saved: on add, add from the catalog, edit and
    switch. Tapis registers the person's host here. A typed refusal (for example, no
    sign-in yet) is reported to the person and the site stays saved; running it
    again must be safe.
    """

    async def on_site_added(self, site: SiteEntry) -> dict[str, Any] | None:
        """Set the site up and return a short summary for the person, or ``None``."""
        ...


@runtime_checkable
class ShutsDown(Protocol):
    """An adapter that holds long-lived processes for its site. Optional.

    ``clio-relay shutdown`` calls it to stop them: a placed worker, an ssh control
    connection. It must not cancel jobs, and it must return within a few seconds. It
    returns one ``{kind, pid?, detail}`` per process it stopped, ``kind`` being one of
    ``site_serve``, ``hop_proxy``, ``connector``, ``tunnel`` or ``frpc``. A typed refusal
    means the site could not be reached; nothing was stopped there.
    """

    async def shutdown(self) -> list[dict[str, Any]]:
        """Stop the site's relay-owned processes and say which."""
        ...


@runtime_checkable
class CleansSite(Protocol):
    """An adapter that can remove what relay deployed on its site. Optional.

    ``clio-relay site clean`` calls it after ``shutdown``. ``keep`` is a list of site
    paths, ``~/`` meaning the home directory, that must survive (a Spack store). It
    returns ``{removed: [...], kept: [...]}``. An adapter whose ``clean`` goes over the
    connection its ``shutdown`` closes sets ``clean_over_connection``; that connection
    is then closed after the clean instead of before it.
    """

    def clean_targets(self) -> tuple[list[str], list[str]]:
        """Return the site paths relay deployed, and those it keeps."""
        ...

    async def clean(self, targets: list[str], keep: list[str]) -> dict[str, Any]:
        """Remove ``targets`` on the site, sparing ``keep``."""
        ...


@runtime_checkable
class ListsLiveJobs(Protocol):
    """An adapter that can ask its site's scheduler which of relay's jobs still run. Optional.

    ``clio-relay site clean`` asks before it removes anything, so a job relay's
    records no longer show as live (its record ended early, or another relay
    started it) still stops the clean. ``known`` are native job ids from relay's
    records; any job named ``relay-...`` counts too. It returns
    ``[{native_id, name, state}, ...]``.
    """

    async def live_jobs(self, known: list[str]) -> list[dict[str, str]]:
        """Return relay's jobs the site's scheduler still lists."""
        ...


def not_implemented(adapter: str, verb: str) -> Refusal:
    """Return the refusal for a verb an adapter does not implement."""
    return Refusal(
        RefusalReason.VERB_NOT_IMPLEMENTED,
        f"the {adapter} adapter does not implement {verb}",
        adapter=adapter,
        verb=verb,
    )


class EmptyConfig(BaseModel):
    """An adapter with nothing to configure."""

    model_config = ConfigDict(extra="forbid")


class BaseAdapter[ConfigT: BaseModel]:
    """Defaults for every optional verb: a typed refusal."""

    name: ClassVar[str] = "base"
    Config: ClassVar[type[BaseModel]] = EmptyConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe"})
    # True when the site runs a reviewed package from the package store, not the agent's command.
    runs_packages: ClassVar[bool] = False
    # True when service_runtimes reads JARVIS's record, so a package service picks its own port.
    reads_service_runtimes: ClassVar[bool] = False
    # False for test-only adapters: the Compute tab does not offer them for a new site.
    listed: ClassVar[bool] = True

    def __init__(self, config: ConfigT, context: AdapterContext) -> None:
        self.config = config
        self.context = context

    async def describe(self) -> SiteInfo:
        return SiteInfo(adapter=self.name, verbs=sorted(self.implements))

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        raise not_implemented(self.name, "run")

    async def status(self, handle: JobHandle) -> JobStatus:
        raise not_implemented(self.name, "run")

    async def cancel(self, handle: JobHandle) -> JobStatus:
        raise not_implemented(self.name, "run")

    async def find(self, operation_id: str) -> JobHandle | None:
        return None

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        raise not_implemented(self.name, "exec")

    async def files(self) -> FileAccess:
        raise not_implemented(self.name, "files")

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        raise not_implemented(self.name, "serve")

    async def stop(self, handle: JobHandle) -> JobStatus:
        """Ask a running job to end cleanly (``jarvis execution stop``), and return its status."""
        raise not_implemented(self.name, "stop")

    async def plan_job(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any] | None:
        """Return the plan of the package job :meth:`run` would submit, submitting nothing.

        An adapter that submits through JARVIS returns JARVIS's plan
        (:func:`clio_relay.jarvis_plan.parse`): the directives, the components,
        the couplings and the script, with the ``plan_sha256`` a later
        :meth:`run` of a spec :func:`~clio_relay.jarvis_plan.bound` to it must
        match. It stages the job's files and runs no Spack and no job. ``None``
        means there is no plan to show (the default, and any job JARVIS does not
        submit); the card then shows the job as relay records it.
        """
        return None

    async def install_check(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any]:
        """Ask JARVIS whether ``spec``'s software is installed, and launch nothing.

        Runs the site's ``ppl install +check`` for the pipeline :meth:`run` would
        submit, and returns JARVIS's install report when the software is all
        there. Otherwise it raises what :meth:`run` raises: ``install_needed``
        (exit 3) with the report, ``missing_jarvis`` when the site has no JARVIS,
        or JARVIS's own refusal.
        """
        raise not_implemented(self.name, "install_check")

    async def install(
        self, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        """Submit the job that installs ``spec``'s software with the person's ``answers``.

        The handle is polled with :meth:`status` like any job; its progress is
        the install's (``metadata.install``).
        """
        raise not_implemented(self.name, "install")

    async def install_jarvis(self) -> str:
        """Install jarvis-cd at :data:`clio_relay.jarvis_submit.JARVIS_PIN` in the site home.

        Returns the path of the ``jarvis`` program it linked.
        """
        raise not_implemented(self.name, "install_jarvis")

    async def move_queue(self, handle: JobHandle, queue_name: str) -> None:
        """Move a job that still waits in the scheduler's queue to ``queue_name``.

        The job keeps its id, directory and place in relay's records. Relay calls
        it only on the person's answer to its queue ask, and only while
        :meth:`status` reports the job's ``queue`` (it waits).
        """
        raise not_implemented(self.name, "move_queue")

    async def service_runtimes(self, handle: JobHandle) -> dict[str, Any] | None:
        """Return ``jarvis execution service-runtimes +json`` for a JARVIS job, or None.

        It is JARVIS's ``jarvis.execution.service-runtimes.v1`` document: the
        host and port each package service reported it bound. None when the
        adapter cannot read it (``reads_service_runtimes`` is False) or JARVIS
        printed no document yet.
        """
        return None

    async def sign_in_needed(self) -> SignInNeed | None:
        return None

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        return None


__all__ = [
    "VERBS",
    "Adapter",
    "AdapterContext",
    "BaseAdapter",
    "CleansSite",
    "EmptyConfig",
    "Endpoint",
    "EndpointPort",
    "ExecResult",
    "FileAccess",
    "FileEntry",
    "InputRef",
    "JobHandle",
    "JobProgress",
    "JobSpec",
    "JobState",
    "JobStatus",
    "Kind",
    "OutputRef",
    "PackageRef",
    "Refusal",
    "RefusalReason",
    "STALL_S",
    "ServiceSpec",
    "ShutsDown",
    "SignInNeed",
    "SiteAdded",
    "SiteFiles",
    "SiteInfo",
    "StallReport",
    "Verb",
    "not_implemented",
]
