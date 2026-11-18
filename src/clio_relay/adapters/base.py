"""The adapter interface (RL6) and the base class adapters extend.

An adapter is one way into a site. It declares a ``Config`` model, whose JSON
Schema drives validation and the compute tab's form, and the verbs it
implements. Every verb it does not implement refuses with
``verb_not_implemented``, which the conformance kit checks (RL7).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal, Protocol, get_args, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import Endpoint, ExecResult, FileAccess, JobHandle, JobStatus
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec

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
    """What relay gives an adapter when it builds one."""

    site_id: str
    state_dir: Path
    content: ContentStore


@runtime_checkable
class Adapter(Protocol):
    """The interface every adapter implements (RL6)."""

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
        self, command: list[str], *, timeout_s: float, max_output_bytes: int
    ) -> ExecResult:
        """Run one short command and return bounded output."""
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
        self, command: list[str], *, timeout_s: float, max_output_bytes: int
    ) -> ExecResult:
        raise not_implemented(self.name, "exec")

    async def files(self) -> FileAccess:
        raise not_implemented(self.name, "files")

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        raise not_implemented(self.name, "serve")

    async def sign_in_needed(self) -> SignInNeed | None:
        return None

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        return None
