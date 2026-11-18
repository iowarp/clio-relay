"""The hooks relay runs with when no other distribution provides one.

* :class:`StubAskProvider` builds asks and serves relay's own loopback page.
  W6 replaces it.
* :class:`DefaultGuard` refuses guarded sites, since it cannot review packages,
  and prices a job by node-hours. W13 replaces it.
* :class:`StubServe` refuses ``serve``. W9 replaces it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from clio_relay.adapters.base import BaseAdapter, SignInNeed
from clio_relay.adapters.hooks import Ask, Charge, GuardDecision, HookContext
from clio_relay.jobs.handles import Endpoint, JobStatus
from clio_relay.jobs.records import OperationRecord, new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.loopback import LoopbackPages

if TYPE_CHECKING:
    from fastmcp import FastMCP

    from clio_relay.surface.changes import ServerChanges

APPROVE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "approve": {"type": "boolean", "title": "Approve", "default": False},
    },
    "required": ["approve"],
}


class StubAskProvider:
    """Builds durable asks. URL asks without a provider page use relay's own page."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        self.pages = LoopbackPages(context.asks)

    def _store(self, ask: Ask) -> Ask:
        self.context.asks.put(ask.ask_id, ask)
        return ask

    async def choose(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        field_name: str,
        options: list[str],
    ) -> Ask:
        schema = {
            "type": "object",
            "properties": {field_name: {"type": "string", "enum": list(options)}},
            "required": [field_name],
        }
        return self._store(
            Ask(
                ask_id=new_id("ask"),
                kind="choose",
                site=site.id,
                operation_id=operation_id,
                message=message,
                requested_schema=schema,
            )
        )

    async def approve(
        self,
        *,
        site: SiteEntry,
        operation_id: str | None,
        message: str,
        binding: dict[str, Any],
    ) -> Ask:
        ask = Ask(
            ask_id=new_id("ask"),
            kind="approve",
            site=site.id,
            operation_id=operation_id,
            message=message,
            requested_schema=APPROVE_SCHEMA,
        )
        ask.bind(binding)
        return self._store(ask)

    async def url(self, *, site: SiteEntry, operation_id: str | None, need: SignInNeed) -> Ask:
        ask = Ask(
            ask_id=new_id("ask"),
            kind="url",
            site=site.id,
            operation_id=operation_id,
            message=need.message,
            url=need.url,
            binding={"provider": need.provider},
        )
        if ask.url is None:
            ask.url = self.pages.address(ask)
        return self._store(ask)

    async def satisfied(self, ask: Ask) -> bool:
        current = self.context.asks.get(ask.ask_id) or ask
        return current.completed_at is not None

    def page_address(self, ask: Ask) -> str | None:
        if ask.kind == "url":
            return ask.url
        return self.pages.address(ask)

    async def close(self) -> None:
        self.pages.close()


class DefaultGuard:
    """Open sites run; guarded sites need a guard that can review packages."""

    def __init__(self, context: HookContext) -> None:
        self.context = context

    async def check_run(self, site: SiteEntry, spec: JobSpec) -> GuardDecision:
        if site.policy == "open":
            return GuardDecision(allowed=True)
        return GuardDecision(
            allowed=False,
            reason=RefusalReason.GUARD_UNAVAILABLE,
            message=(
                f"{site.title} is guarded and no package guard is installed, so relay "
                "cannot run anything there. Set the site's policy to open, or install a guard."
            ),
            data={"site": site.id},
        )

    async def check_exec(self, site: SiteEntry, command: list[str]) -> GuardDecision:
        return await self.check_run(site, JobSpec(executable=command[0] if command else "true"))

    async def price(self, site: SiteEntry, spec: JobSpec) -> Charge:
        duration = spec.attributes.duration
        node_hours = spec.node_hours()
        return Charge(
            site=site.id,
            queue=spec.attributes.queue_name,
            account=spec.attributes.account,
            nodes=spec.resources.nodes(),
            walltime_s=None if duration is None else duration.total_seconds(),
            node_hours=None if node_hours is None else round(node_hours, 4),
        )

    async def prepare(
        self,
        site: SiteEntry,
        spec: JobSpec,
        *,
        adapter: BaseAdapter[Any],
        service: bool = False,
    ) -> JobSpec:
        return spec


class StubServe:
    """No service runtime is installed."""

    def __init__(self, context: HookContext) -> None:
        self.context = context

    def attach(self, server: FastMCP[Any], changes: ServerChanges) -> None:
        return None

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        return None

    def _refuse(self) -> Refusal:
        return Refusal(
            RefusalReason.VERB_NOT_IMPLEMENTED,
            "serve needs a service runtime, and none is installed",
            verb="serve",
        )

    async def start(
        self,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        spec: ServiceSpec,
        record: OperationRecord,
    ) -> Endpoint | None:
        raise self._refuse()

    async def status(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        raise self._refuse()

    async def stop(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> JobStatus:
        raise self._refuse()

    async def close(self) -> None:
        return None
