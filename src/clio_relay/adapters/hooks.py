"""The hooks other workstreams implement: asks, the guard and serve.

Relay ships a stub of each. An installed distribution replaces one by
registering a factory under the entry point group ``clio_relay.hooks`` with
the name ``asks``, ``guard`` or ``serve``. A factory takes a
:class:`HookContext` and returns the implementation.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import EntryPoint, entry_points
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, runtime_checkable

from pydantic import BaseModel, Field

from clio_relay.adapters.base import BaseAdapter, SignInNeed
from clio_relay.jobs.handles import Endpoint, JobStatus, utc_now
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, Layout, PackageRef, ServiceSpec, canonical_digest
from clio_relay.sites.models import SiteEntry

if TYPE_CHECKING:
    from fastmcp import FastMCP

    from clio_relay.surface.changes import ServerChanges

HOOK_GROUP = "clio_relay.hooks"

_log = getLogger(__name__)

AskKind = Literal["choose", "approve", "url"]
AskState = Literal["pending", "accepted", "declined", "cancelled"]
Transport = Literal["stdio", "http", "memory"]

APPROVE_YES = "approve"
APPROVE_NO = "decline"


def approve_schema(*, plain: bool = True) -> dict[str, Any]:
    """Return an approval's form: one boolean when ``plain``, else a titled choice.

    A plain approval only confirms what the call's own arguments already say, so a
    host that answers a plain confirm itself for a call the person allowed loses
    nothing. An approval that names facts the arguments do not (a cost, a gap in
    the site's software, what a clean removes) is a choice, which such a host
    still puts to the person.
    """
    if plain:
        answer: dict[str, Any] = {"type": "boolean", "title": "Approve", "default": False}
    else:
        answer = {
            "type": "string",
            "title": "Answer",
            "oneOf": [
                {"const": APPROVE_YES, "title": "Approve"},
                {"const": APPROVE_NO, "title": "Decline"},
            ],
        }
    return {"type": "object", "properties": {"approve": answer}, "required": ["approve"]}


def said_yes(content: dict[str, Any] | None) -> bool:
    """Say whether an approval's answer content is a yes, in either form."""
    value = (content or {}).get("approve")
    return value is True or value == APPROVE_YES


class Ask(BaseModel):
    """A question for the person, durable so any path can answer it.

    ``binding`` is what an approval approves: the site, the spec digest and the
    charge. An answer applies only while the operation still has that binding,
    so a retry with changed arguments needs a new approval (RL25a).
    """

    ask_id: str
    kind: AskKind
    site: str
    operation_id: str | None = None
    message: str
    requested_schema: dict[str, Any] | None = None
    url: str | None = None
    binding: dict[str, Any] = Field(default_factory=dict[str, Any])
    binding_digest: str = ""
    meta: dict[str, Any] | None = Field(
        default=None, description="Optional typed data the elicitation carries in its _meta"
    )
    state: AskState = "pending"
    answer: dict[str, Any] | None = None
    answered_by: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    answered_at: datetime | None = None
    completed_at: datetime | None = Field(
        default=None,
        description=(
            "When a URL ask's page reported the sign-in finished, or when a files approval was used"
        ),
    )

    def bind(self, binding: dict[str, Any]) -> None:
        """Set the binding and its digest."""
        self.binding = binding
        self.binding_digest = canonical_digest(binding)

    def resolve(self, state: AskState, answer: dict[str, Any] | None, by: str) -> None:
        """Record the person's answer once. A second answer is ignored."""
        if self.state != "pending":
            return
        self.state = state
        self.answer = answer
        self.answered_by = by
        self.answered_at = utc_now()


@dataclass
class HookContext:
    """What relay gives a hook factory."""

    state_dir: Path
    asks: RecordStore[Ask]
    transport: Transport = "stdio"
    settings: dict[str, Any] = field(default_factory=dict[str, Any])


@runtime_checkable
class AskProvider(Protocol):
    """Builds asks and serves the pages behind them (W6 implements it)."""

    async def choose(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        field_name: str,
        options: list[str],
    ) -> Ask:
        """Build a form ask that picks one of ``options``."""
        ...

    async def approve(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        binding: dict[str, Any],
        plain: bool = True,
    ) -> Ask:
        """Build a form ask for a yes or no bound to ``binding``.

        ``plain=False`` when the message names facts the call's arguments do
        not: the form is then a choice (:func:`approve_schema`).
        """
        ...

    async def url(self, *, site: SiteEntry, operation_id: str | None, need: SignInNeed) -> Ask:
        """Build a URL ask: the provider's page, or relay's own single-use page."""
        ...

    async def satisfied(self, ask: Ask) -> bool:
        """Return whether a URL ask's page was completed."""
        ...

    def page_address(self, ask: Ask) -> str | None:
        """Return the approval page for a client that cannot elicit (RL25a)."""
        ...

    async def close(self) -> None:
        """Stop any page server."""
        ...


@runtime_checkable
class MountsPages(Protocol):
    """An ask provider that serves its pages on relay's own HTTP server.

    Optional: relay calls ``mount`` after building an HTTP server when the
    provider has it.
    """

    def mount(self, server: FastMCP[Any]) -> None:
        """Add the provider's routes to ``server``."""
        ...


class ChargeStep(BaseModel):
    """The allocation's nodes one step of a pipeline gets."""

    package: str
    host_slice: str | None = Field(default=None, description="None: all the nodes")
    nodes: int | None = Field(default=None, description="None when the slice depends on the size")


def charge_steps(steps: Sequence[PackageRef], allocation: int) -> list[ChargeStep]:
    """Return the nodes each step of a pipeline gets, of an ``allocation``-node job."""
    from clio_relay.packages import node_range

    shares: list[ChargeStep] = []
    for step in steps:
        host_slice = step.nodes()
        found = node_range(step)
        if host_slice is None:
            nodes: int | None = allocation
        else:
            nodes = None if found is None else found[1] - found[0]
        shares.append(ChargeStep(package=step.name, host_slice=host_slice, nodes=nodes))
    return shares


class Charge(BaseModel):
    """What a job may cost and how it is laid out, as the spend gate shows it (RL25).

    The layout (nodes, ranks per node, threads, memory, walltime, each step's
    nodes) is what the job asks the scheduler for. ``rationale`` and
    ``alternatives`` are the job's ``sizing``: why that layout, and what else
    would work. The approval binds the whole charge, so the person approves this
    layout and no other.
    """

    site: str
    queue: str | None = None
    account: str | None = None
    nodes: int
    ranks_per_node: int | None = Field(default=None, description="None when the job leaves it out")
    ranks: int | None = Field(default=None, description="All processes, None when left out")
    threads_per_rank: int | None = Field(default=None, description="None when left out")
    memory_mb: int | None = Field(default=None, description="Per node, None when left out")
    walltime_s: float | None = Field(default=None, description="None when no walltime is set")
    node_hours: float | None = Field(default=None, description="None when unknown")
    amount: float | None = None
    unit: str = "node-hours"
    steps: list[ChargeStep] = Field(default_factory=list[ChargeStep])
    rationale: str | None = None
    alternatives: list[Layout] = Field(default_factory=list[Layout])

    def with_layout(self, spec: JobSpec) -> Charge:
        """Return a copy with ``spec``'s threads, memory, steps and sizing filled in."""
        resources = spec.resources
        steps = spec.steps()
        sizing = spec.sizing
        return self.model_copy(
            update={
                "threads_per_rank": self.threads_per_rank or resources.cpu_cores_per_process,
                "memory_mb": self.memory_mb
                or (None if resources.memory is None else -(-resources.memory // 2**20)),
                "steps": self.steps or (charge_steps(steps, self.nodes) if len(steps) > 1 else []),
                "rationale": self.rationale or (sizing.rationale if sizing else None),
                "alternatives": self.alternatives or (sizing.alternatives if sizing else []),
            }
        )


class GuardDecision(BaseModel):
    """The guard's answer. A refusal carries a typed reason."""

    allowed: bool
    reason: RefusalReason | None = None
    message: str | None = None
    data: dict[str, Any] = Field(default_factory=dict[str, Any])

    def raise_if_refused(self) -> None:
        """Raise the refusal this decision carries."""
        if not self.allowed:
            raise Refusal(
                self.reason or RefusalReason.POLICY_REFUSED,
                self.message or "refused by the site's policy",
                **self.data,
            )


@runtime_checkable
class GuardHook(Protocol):
    """Code policy per site (RL24) and pricing (RL25). W13 implements it."""

    async def check_run(self, site: SiteEntry, spec: JobSpec) -> GuardDecision:
        """Decide whether ``spec`` may run on ``site``."""
        ...

    async def check_exec(
        self, site: SiteEntry, command: list[str], *, read_only: bool = False
    ) -> GuardDecision:
        """Decide whether ``command`` may run through ``exec``.

        With ``read_only`` the call came through ``exec_read``, which a client
        trusts never to change anything: allow ``command`` only when it reads,
        whatever the site's policy.
        """
        ...

    async def price(self, site: SiteEntry, spec: JobSpec) -> Charge:
        """Return the most ``spec`` may cost on ``site``."""
        ...

    async def prepare(
        self,
        site: SiteEntry,
        spec: JobSpec,
        *,
        adapter: BaseAdapter[Any],
        service: bool = False,
    ) -> JobSpec:
        """Return the spec ``adapter`` submits, for example with a reviewed package staged.

        Relay calls it just before it submits a job, and with ``service=True``
        just before it starts a service's job, which runs until it is stopped
        rather than to completion. It never changes the operation record, so the
        approved digest stays the one the person saw. A refusal fails the operation.
        """
        ...


@runtime_checkable
class ServeHook(Protocol):
    """The service runtime (RL19 to RL23). W9 implements it.

    Relay keeps the operation record. The hook keeps its own job handles under
    ``state_dir/serve/``, keyed by the record's operation id.
    """

    def attach(self, server: FastMCP[Any], changes: ServerChanges) -> None:
        """Mount the hook's tools on ``server``. Called once, while relay builds it."""
        ...

    async def start(
        self,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        spec: ServiceSpec,
        record: OperationRecord,
    ) -> Endpoint | None:
        """Start a service. ``None`` means it is not ready yet."""
        ...

    async def status(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        """Observe a service."""
        ...

    async def stop(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> JobStatus:
        """Stop a service, giving it its grace period."""
        ...

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        """Return what the service's panel needs, secret-bearing URLs included.

        Relay puts it only in the serve result's ``_meta``, never in describe
        or a task status (RL22).
        """
        ...

    async def close(self) -> None:
        """Release held forwards and sessions."""
        ...


SERVICE_CHOICES = "service_choices"
"""The ``record.result`` key of what a planned service's start keeps (its ports)."""


def service_choices(record: OperationRecord) -> dict[str, Any] | None:
    """Return the choices a service's plan stored on ``record``, if any."""
    found = (record.result or {}).get(SERVICE_CHOICES)
    return cast(dict[str, Any], found) if isinstance(found, dict) else None


@runtime_checkable
class PlansServices(Protocol):
    """A serve hook that can say which package job a service's start submits.

    Relay asks JARVIS to plan that job before the person's yes, and binds the
    start to it. A serve hook without it gets an unbound service card.
    """

    async def plan_job(
        self,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        spec: ServiceSpec,
        record: OperationRecord,
    ) -> tuple[JobSpec, dict[str, Any]] | None:
        """Return the job and what its start keeps, or ``None`` for no package job."""
        ...


type HookFactory = Callable[[HookContext], object]

_overrides: dict[str, HookFactory] = {}


def set_hook(name: Literal["asks", "guard", "serve"], factory: HookFactory | None) -> None:
    """Override a hook in this process. ``None`` removes the override."""
    if factory is None:
        _overrides.pop(name, None)
    else:
        _overrides[name] = factory


def _module_missing(point: EntryPoint, exc: ModuleNotFoundError) -> bool:
    """Return whether ``exc`` says the entry point's own module is not installed."""
    module = point.module
    return exc.name is not None and (module == exc.name or module.startswith(f"{exc.name}."))


def hook_factory(name: Literal["asks", "guard", "serve"], default: HookFactory) -> HookFactory:
    """Return the factory for ``name``: an override, an entry point, or the default.

    An entry point whose module is not installed falls back to the default,
    which is the safe stub (the guard stub refuses guarded sites). Any other
    import error is raised.
    """
    if name in _overrides:
        return _overrides[name]
    for point in entry_points(group=HOOK_GROUP):
        if point.name == name:
            try:
                loaded: object = point.load()
            except ModuleNotFoundError as exc:
                if not _module_missing(point, exc):
                    raise
                _log.warning("hook %s: %s is not installed; using the stub", name, point.value)
                return default
            if not callable(loaded):
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    f"hook entry point {point.value!r} is not callable",
                    hook=name,
                )
            return loaded  # pyright: ignore[reportReturnType]
    return default
