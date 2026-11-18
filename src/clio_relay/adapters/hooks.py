"""The hooks other workstreams implement: asks, the guard and serve.

Relay ships a stub of each. An installed distribution replaces one by
registering a factory under the entry point group ``clio_relay.hooks`` with
the name ``asks``, ``guard`` or ``serve``. A factory takes a
:class:`HookContext` and returns the implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from clio_relay.adapters.base import BaseAdapter, SignInNeed
from clio_relay.jobs.handles import Endpoint, JobStatus, utc_now
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec, canonical_digest
from clio_relay.sites.models import SiteEntry

HOOK_GROUP = "clio_relay.hooks"

AskKind = Literal["choose", "approve", "url"]
AskState = Literal["pending", "accepted", "declined", "cancelled"]
Transport = Literal["stdio", "http", "memory"]


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
    state: AskState = "pending"
    answer: dict[str, Any] | None = None
    answered_by: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    answered_at: datetime | None = None
    completed_at: datetime | None = Field(
        default=None, description="When a URL ask's page reported the sign-in finished"
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
    ) -> Ask:
        """Build a form ask for a yes or no bound to ``binding``."""
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


class Charge(BaseModel):
    """What a job may cost, as the spend gate shows it (RL25)."""

    site: str
    queue: str | None = None
    account: str | None = None
    nodes: int
    walltime_s: float
    node_hours: float
    amount: float | None = None
    unit: str = "node-hours"


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

    async def check_exec(self, site: SiteEntry, command: list[str]) -> GuardDecision:
        """Decide whether ``command`` may run through ``exec``."""
        ...

    async def price(self, site: SiteEntry, spec: JobSpec) -> Charge:
        """Return the most ``spec`` may cost on ``site``."""
        ...


@runtime_checkable
class ServeHook(Protocol):
    """The service runtime (RL19 to RL23). W9 implements it."""

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

    async def close(self) -> None:
        """Release held forwards and sessions."""
        ...


type HookFactory = Callable[[HookContext], object]

_overrides: dict[str, HookFactory] = {}


def set_hook(name: Literal["asks", "guard", "serve"], factory: HookFactory | None) -> None:
    """Override a hook in this process. ``None`` removes the override."""
    if factory is None:
        _overrides.pop(name, None)
    else:
        _overrides[name] = factory


def hook_factory(name: Literal["asks", "guard", "serve"], default: HookFactory) -> HookFactory:
    """Return the factory for ``name``: an override, an entry point, or the default."""
    if name in _overrides:
        return _overrides[name]
    for point in entry_points(group=HOOK_GROUP):
        if point.name == name:
            loaded: object = point.load()
            if not callable(loaded):
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    f"hook entry point {point.value!r} is not callable",
                    hook=name,
                )
            return loaded  # pyright: ignore[reportReturnType]
    return default
