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
  :func:`clio_relay.jarvis_submit.install_needed`; relay asks the person and calls
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


class SiteInfo(BaseModel):
    """What an adapter reports about its site."""

    adapter: str
    address: str | None = None
    scheduler: str | None = Field(default=None, description="slurm, pbs, lsf, cobalt or none")
    queues: list[str] = Field(default_factory=list[str])
    accounts: list[str] = Field(default_factory=list[str])
    limits: dict[str, Any] = Field(default_factory=dict[str, Any])
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

    async def sign_in_needed(self) -> SignInNeed | None:
        return None

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        return None


__all__ = [
    "VERBS",
    "Adapter",
    "AdapterContext",
    "BaseAdapter",
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
    "SignInNeed",
    "SiteAdded",
    "SiteFiles",
    "SiteInfo",
    "StallReport",
    "Verb",
    "not_implemented",
]
