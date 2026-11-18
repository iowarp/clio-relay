"""The operations service: everything the verbs do, without the wire.

The MCP server, the CLI and the tests all drive this. It owns the site store,
the adapters, the hooks, and the durable operation and ask records. It submits
each operation at most once, from whichever process gets there first, and any
process can observe, answer or cancel an operation by its id.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, cast

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SiteAdded, Verb
from clio_relay.adapters.hooks import (
    Ask,
    AskProvider,
    Charge,
    GuardHook,
    HookContext,
    ServeHook,
    Transport,
    hook_factory,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import Endpoint, JobState, JobStatus, StallReport, utc_now
from clio_relay.jobs.records import (
    OperationKind,
    OperationRecord,
    OperationState,
    RecordStore,
    StateChange,
    new_id,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec, canonical_digest
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import Route, Routed, route, routes
from clio_relay.sites.store import SiteStore
from clio_relay.surface.describing import relay_report, site_report
from clio_relay.surface.endpoints import public_endpoint, service_ready
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.stubs import DefaultGuard, StubAskProvider, StubServe

logger = logging.getLogger(__name__)

SUBMIT_GRACE = timedelta(minutes=2)
OUTPUT_TAIL_BYTES = 8 * 1024

_JOB_TO_OPERATION = {
    JobState.QUEUED: OperationState.QUEUED,
    JobState.RUNNING: OperationState.RUNNING,
    JobState.COMPLETED: OperationState.COMPLETED,
    JobState.FAILED: OperationState.FAILED,
    JobState.CANCELLED: OperationState.CANCELLED,
}


def _job_name(adapter: BaseAdapter[Any], operation_id: str) -> str | None:
    namer = getattr(adapter, "job_name", None)
    if not callable(namer):
        return None
    name = namer(operation_id)
    return name if isinstance(name, str) else None


def _approval_message(action: str, site: SiteEntry, charge: Charge) -> str:
    if charge.walltime_s is None or charge.node_hours is None:
        return (
            f"{action} on {site.title}? It asks for {charge.nodes} node(s) and sets no "
            "walltime, so its duration and cost are unknown: the scheduler's default "
            "limit applies."
        )
    return (
        f"{action} on {site.title}? It may use up to {charge.node_hours:g} {charge.unit} "
        f"({charge.nodes} node(s) for {charge.walltime_s / 60:g} min)."
    )



class RelayService:
    """One relay host's view of its sites and operations."""

    def __init__(
        self,
        paths: RelayPaths,
        *,
        transport: Transport = "stdio",
        public_base_url: str | None = None,
    ) -> None:
        self.paths = paths
        self.sites = SiteStore(paths.sites_file)
        self.content = ContentStore(paths.content_dir)
        self.operations: RecordStore[OperationRecord] = RecordStore(
            paths.operations_dir, OperationRecord
        )
        self.asks: RecordStore[Ask] = RecordStore(paths.asks_dir, Ask)
        settings: dict[str, Any] = {
            "grant_files": {key: str(value) for key, value in paths.grant_files.items()},
            "config_dir": str(paths.config_dir),
        }
        if public_base_url is not None:
            settings["public_base_url"] = public_base_url
        context = HookContext(
            state_dir=paths.state_dir, asks=self.asks, transport=transport, settings=settings
        )
        self.ask_provider = cast(AskProvider, hook_factory("asks", StubAskProvider)(context))
        self.guard = cast(GuardHook, hook_factory("guard", DefaultGuard)(context))
        self.serve_hook = cast(ServeHook, hook_factory("serve", StubServe)(context))
        self._adapters: dict[tuple[str, str, str], BaseAdapter[Any]] = {}

    async def close(self) -> None:
        """Release the hooks."""
        await self.ask_provider.close()
        await self.serve_hook.close()

    # Sites and adapters

    def site(self, site_id: str) -> SiteEntry:
        """Return a site or refuse with ``site_not_found``."""
        return self.sites.get(site_id)

    def _build(self, site: SiteEntry, name: str, config: dict[str, Any]) -> BaseAdapter[Any]:
        key = (site.id, name, canonical_digest(config))
        adapter = self._adapters.get(key)
        if adapter is None:
            context = AdapterContext(
                site_id=site.id,
                state_dir=self.paths.adapter_dir(site.id, name),
                content=self.content,
            )
            adapter = registry.build(name, config, context)
            self._adapters[key] = adapter
        return adapter

    def route(self, site: SiteEntry, verb: Verb) -> Routed:
        """Route ``verb`` on ``site``, skipping adapters whose settings do not build (RL5)."""
        return route(site, verb, lambda name, config: self._build(site, name, config))

    def site_routes(self, site: SiteEntry) -> list[Route]:
        """Return every verb's route on ``site``, with what was skipped and why."""
        return routes(site, lambda name, config: self._build(site, name, config))

    def adapter_for(self, site: SiteEntry, verb: Verb) -> BaseAdapter[Any]:
        """Return the adapter that serves ``verb`` on ``site`` (RL5)."""
        name, config, _skipped = self.route(site, verb)
        return self._build(site, name, config)

    def adapter_named(self, site: SiteEntry, name: str) -> BaseAdapter[Any]:
        """Return the adapter an operation was submitted through."""
        if name == site.adapter:
            return self._build(site, name, site.config)
        if name in site.alternatives:
            return self._build(site, name, site.alternatives[name])
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"{site.id!r} no longer lists the {name} adapter this operation used",
            site=site.id,
            adapter=name,
        )

    async def site_added(self, site_id: str) -> dict[str, Any]:
        """Let each adapter that implements ``SiteAdded`` set up a saved site.

        An adapter whose settings do not build is left out here; ``describe`` names it.
        """
        site = self.site(site_id)
        results: dict[str, Any] = {}
        for name, config in [(site.adapter, site.config), *site.alternatives.items()]:
            try:
                adapter = self._build(site, name, config)
            except Refusal:
                continue
            if not isinstance(adapter, SiteAdded):
                continue
            try:
                results[name] = await adapter.on_site_added(site)
            except Refusal as refusal:
                results[name] = {"refusal": refusal.to_dict()}
        return results

    # describe

    async def describe(self, site_id: str | None = None, job: str | None = None) -> dict[str, Any]:
        """Describe relay, one site, or one operation."""
        if job is not None:
            return self.view(await self.refresh(job))
        if site_id is None:
            return await relay_report(self)
        return await site_report(self, self.site(site_id))

    # run

    def apply_defaults(self, site: SiteEntry, spec: JobSpec) -> JobSpec:
        """Fill the queue, account and walltime the site defaults to."""
        attributes = spec.attributes
        changes: dict[str, Any] = {}
        if attributes.queue_name is None and site.defaults.queue is not None:
            changes["queue_name"] = site.defaults.queue
        if attributes.account is None and site.defaults.account is not None:
            changes["account"] = site.defaults.account
        if attributes.duration is None and site.defaults.duration is not None:
            changes["duration"] = site.defaults.duration
        if not changes:
            return spec
        return spec.model_copy(update={"attributes": attributes.model_copy(update=changes)})

    async def start_run(
        self, site_id: str, spec: JobSpec, *, task: bool = False
    ) -> OperationRecord:
        """Check, price and record a run, ask what must be asked, and submit if nothing must."""
        site = self.site(site_id)
        spec = self.apply_defaults(site, spec)
        what = spec.executable or (spec.package.name if spec.package else "a job")
        return await self._open(
            site,
            "run",
            spec,
            spec.model_dump(mode="json"),
            spec.digest(),
            f"Run {what}",
            task=task,
        )

    def prepare_service(self, site: SiteEntry, spec: ServiceSpec) -> ServiceSpec:
        """Fill the service job's site defaults."""
        return spec.model_copy(update={"job": self.apply_defaults(site, spec.job)})

    async def start_serve(
        self, site_id: str, spec: ServiceSpec, *, task: bool = False
    ) -> OperationRecord:
        """Check and price a service's job, record it, ask, and start it if nothing must."""
        site = self.site(site_id)
        self.route(site, "serve")
        if isinstance(self.serve_hook, StubServe):
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "serve needs a service runtime, and none is installed",
                verb="serve",
                site=site.id,
            )
        spec = self.prepare_service(site, spec)
        ports = ", ".join(port.name for port in spec.ports)
        what = spec.job.executable or (spec.job.package.name if spec.job.package else "a service")
        return await self._open(
            site,
            "serve",
            spec.job,
            spec.model_dump(mode="json"),
            spec.digest(),
            f"Start {what} as a service with port(s) {ports}",
            task=task,
        )

    async def _open(
        self,
        site: SiteEntry,
        kind: OperationKind,
        job: JobSpec,
        spec: dict[str, Any],
        spec_digest: str,
        action: str,
        *,
        task: bool,
    ) -> OperationRecord:
        verb: Verb = "serve" if kind == "serve" else "run"
        adapter_name, config, skipped = self.route(site, verb)
        adapter = self._build(site, adapter_name, config)
        (await self.guard.check_run(site, job)).raise_if_refused()
        charge = await self.guard.price(site, job)
        operation_id = new_id("op")
        result: dict[str, Any] = {"charge": charge.model_dump(mode="json")}
        if skipped:
            result["skipped"] = [item.model_dump() for item in skipped]
        record = OperationRecord(
            operation_id=operation_id,
            kind=kind,
            site=site.id,
            adapter=adapter_name,
            spec=spec,
            spec_digest=spec_digest,
            input_digests=[item.sha256 for item in job.inputs],
            state=OperationState.AWAITING_INPUT,
            history=[StateChange(state=OperationState.AWAITING_INPUT)],
            task=task,
            result=result,
        )
        asks: list[Ask] = []
        need = await adapter.sign_in_needed()
        if need is not None:
            asks.append(
                await self.ask_provider.url(site=site, operation_id=operation_id, need=need)
            )
        if site.spend.needs_approval(charge.node_hours):
            binding = {
                "site": site.id,
                "spec_digest": record.spec_digest,
                "charge": charge.model_dump(mode="json"),
            }
            asks.append(
                await self.ask_provider.approve(
                    site=site,
                    operation_id=operation_id,
                    message=_approval_message(action, site, charge),
                    binding=binding,
                )
            )
        record.asks = [ask.ask_id for ask in asks]
        self.operations.put(operation_id, record)
        if not asks:
            return await self.submit(operation_id)
        return record

    def operation(self, operation_id: str) -> OperationRecord:
        """Return an operation or refuse with ``operation_not_found``."""
        record = self.operations.get(operation_id)
        if record is None:
            raise Refusal(
                RefusalReason.OPERATION_NOT_FOUND,
                f"there is no operation {operation_id!r}",
                operation_id=operation_id,
            )
        return record

    def operation_asks(self, record: OperationRecord) -> list[Ask]:
        """Return the asks an operation is waiting on, current from disk."""
        found: list[Ask] = []
        for ask_id in record.asks:
            ask = self.asks.get(ask_id)
            if ask is not None:
                found.append(ask)
        return found

    def outstanding(self, record: OperationRecord) -> list[Ask]:
        """Return the asks nobody has answered yet."""
        if record.state != OperationState.AWAITING_INPUT:
            return []
        return [ask for ask in self.operation_asks(record) if ask.state == "pending"]

    def answer(
        self,
        ask_id: str,
        action: str,
        content: dict[str, Any] | None = None,
        *,
        by: str,
    ) -> Ask:
        """Record a person's answer to an ask, once."""
        with self.asks.locked(ask_id):
            ask = self.asks.get(ask_id)
            if ask is None:
                raise Refusal(
                    RefusalReason.ASK_NOT_FOUND, f"there is no ask {ask_id!r}", ask_id=ask_id
                )
            if action == "accept":
                if ask.kind == "approve" and not bool((content or {}).get("approve")):
                    ask.resolve("declined", {"action": "accept", "content": content}, by)
                else:
                    ask.resolve("accepted", {"action": "accept", "content": content}, by)
            elif action == "decline":
                ask.resolve("declined", {"action": "decline"}, by)
            else:
                ask.resolve("cancelled", {"action": "cancel"}, by)
            self.asks.put(ask_id, ask)
        return ask

    def same_request(
        self, record: OperationRecord, site: str | None, spec: JobSpec | ServiceSpec | None
    ) -> None:
        """Refuse a retry or resume whose site or spec differs from what was approved."""
        if site is None and spec is None:
            return
        changed = site is not None and site != record.site
        if spec is not None and not changed:
            entry = self.site(record.site)
            current = (
                self.prepare_service(entry, spec)
                if isinstance(spec, ServiceSpec)
                else self.apply_defaults(entry, spec)
            )
            changed = current.digest() != record.spec_digest
        if changed:
            raise Refusal(
                RefusalReason.APPROVAL_MISMATCH,
                "the arguments differ from what was asked about; call run again",
                operation_id=record.operation_id,
            )

    async def _signed_in(self, ask: Ask, record: OperationRecord) -> bool:
        site = self.site(record.site)
        adapter = self.adapter_named(site, record.adapter)
        if await adapter.sign_in_needed() is None:
            return True
        if not await self.ask_provider.satisfied(ask):
            return False
        await adapter.sign_in_done(ask.answer or {})
        return await adapter.sign_in_needed() is None

    async def _signed_in_elsewhere(self, ask: Ask, record: OperationRecord) -> bool:
        """Close a pending sign-in ask the person finished outside relay's page.

        A device-flow sign-in completes at the provider, so nobody answers the
        ask; the adapter saying it no longer needs a sign-in is the answer.
        """
        site = self.site(record.site)
        adapter = self.adapter_named(site, record.adapter)
        if await adapter.sign_in_needed() is not None:
            return False
        with self.asks.locked(ask.ask_id):
            stored = self.asks.get(ask.ask_id)
            if stored is not None and stored.state == "pending":
                stored.resolve("accepted", {"action": "accept", "content": None}, "adapter")
                if stored.completed_at is None:
                    stored.completed_at = utc_now()
                self.asks.put(ask.ask_id, stored)
        return True

    async def advance(self, operation_id: str) -> OperationRecord:
        """Move an operation past its asks once they are all answered."""
        record = self.operation(operation_id)
        if record.state != OperationState.AWAITING_INPUT:
            return record
        waiting = False
        for ask in self.operation_asks(record):
            if ask.state in ("declined", "cancelled"):
                return self._finish(
                    operation_id,
                    OperationState.CANCELLED,
                    Refusal(RefusalReason.DECLINED, f"the person declined: {ask.message}"),
                )
            if ask.state == "pending":
                if ask.kind == "url" and await self._signed_in_elsewhere(ask, record):
                    continue
                waiting = True
                continue
            if ask.kind == "approve" and (
                ask.binding_digest != canonical_digest(ask.binding)
                or ask.binding.get("spec_digest") != record.spec_digest
            ):
                return self._finish(
                    operation_id,
                    OperationState.FAILED,
                    Refusal(
                        RefusalReason.APPROVAL_MISMATCH,
                        "the approval was for a different job; ask again",
                    ),
                )
            if ask.kind == "url" and not await self._signed_in(ask, record):
                waiting = True
        if waiting:
            return record
        return await self.submit(operation_id)

    def _finish(
        self, operation_id: str, state: OperationState, refusal: Refusal | None
    ) -> OperationRecord:
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if not record.state.final:
                if refusal is not None:
                    record.error = refusal.to_dict()
                record.move(state, None if refusal is None else refusal.message)
                self.operations.put(operation_id, record)
        return record

    async def submit(self, operation_id: str) -> OperationRecord:
        """Submit once. Whoever moves the record to ``submitting`` submits.

        The scheduler job name, when the adapter names jobs, is recorded with
        the move, before anything reaches the scheduler (RL25a).
        """
        record = self.operation(operation_id)
        if record.state != OperationState.AWAITING_INPUT:
            return record
        site = self.site(record.site)
        try:
            adapter = self.adapter_named(site, record.adapter)
        except Refusal as refusal:
            return self._finish(operation_id, OperationState.FAILED, refusal)
        job_name = _job_name(adapter, operation_id) if record.kind == "run" else None
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if record.state != OperationState.AWAITING_INPUT:
                return record
            if job_name is not None:
                record.result = {**(record.result or {}), "job_name": job_name}
            record.move(
                OperationState.SUBMITTING, None if job_name is None else f"job name {job_name}"
            )
            self.operations.put(operation_id, record)
        if record.kind == "serve":
            return await self._start_service(operation_id, site, adapter, record)
        try:
            handle = await adapter.run(
                JobSpec.model_validate(record.spec), operation_id=operation_id
            )
        except Refusal as refusal:
            return self._finish(operation_id, OperationState.FAILED, refusal)
        except Exception as exc:
            logger.exception("submitting %s failed", operation_id)
            return self._finish(
                operation_id,
                OperationState.FAILED,
                Refusal(RefusalReason.SUBMIT_FAILED, f"submission failed: {exc}"),
            )
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            record.handle = handle
            cancelled = record.state == OperationState.CANCELLED
            record.move(OperationState.QUEUED, f"native id {handle.native_id}")
            self.operations.put(operation_id, record)
        if cancelled:
            await adapter.cancel(handle)
        return record

    async def _start_service(
        self,
        operation_id: str,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        record: OperationRecord,
    ) -> OperationRecord:
        try:
            endpoint = await self.serve_hook.start(
                site, adapter, ServiceSpec.model_validate(record.spec), record
            )
        except Refusal as refusal:
            return self._finish(operation_id, OperationState.FAILED, refusal)
        except Exception as exc:
            logger.exception("starting service %s failed", operation_id)
            return self._finish(
                operation_id,
                OperationState.FAILED,
                Refusal(RefusalReason.SUBMIT_FAILED, f"starting the service failed: {exc}"),
            )
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            cancelled = record.state == OperationState.CANCELLED
            if endpoint is not None:
                record.result = {
                    **(record.result or {}),
                    "endpoint": public_endpoint(endpoint),
                }
                record.move(OperationState.RUNNING, "the service is ready")
            else:
                record.move(OperationState.QUEUED, "the service is starting")
            self.operations.put(operation_id, record)
        if cancelled:
            status = await self.serve_hook.stop(site, adapter, record)
            return await self._apply(operation_id, adapter, status)
        return record

    async def refresh(self, operation_id: str) -> OperationRecord:
        """Observe an operation's job and record what changed."""
        record = self.operation(operation_id)
        if record.state.final:
            return record
        if record.state == OperationState.AWAITING_INPUT:
            return await self.advance(operation_id)
        site = self.site(record.site)
        adapter = self.adapter_named(site, record.adapter)
        if record.kind == "serve":
            return await self._refresh_service(operation_id, site, adapter, record)
        handle = record.handle
        if handle is None:
            found = await adapter.find(operation_id)
            with self.operations.locked(operation_id):
                record = self.operation(operation_id)
                if record.handle is not None or record.state.final:
                    return record
                if found is not None:
                    record.handle = found
                    record.move(OperationState.QUEUED, "reconciled after an interrupted submit")
                elif utc_now() - record.updated_at > SUBMIT_GRACE:
                    record.error = Refusal(
                        RefusalReason.SUBMIT_FAILED,
                        "the submission was interrupted and no job was found",
                    ).to_dict()
                    record.move(OperationState.FAILED, "lost during submission")
                self.operations.put(operation_id, record)
            return record
        try:
            status = await adapter.status(handle)
        except Refusal as refusal:
            logger.warning("observing %s: %s", operation_id, refusal.message)
            return record
        return await self._apply(operation_id, adapter, status)

    async def _refresh_service(
        self,
        operation_id: str,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        record: OperationRecord,
    ) -> OperationRecord:
        try:
            status, endpoint = await self.serve_hook.status(site, adapter, record)
        except Refusal as refusal:
            if (
                record.state == OperationState.SUBMITTING
                and utc_now() - record.updated_at > SUBMIT_GRACE
            ):
                return self._finish(operation_id, OperationState.FAILED, refusal)
            logger.warning("observing service %s: %s", operation_id, refusal.message)
            return self._observation_failed(operation_id, refusal.message)
        except Exception as exc:
            # One failed look is not the service's end; it stays open and says why.
            logger.warning("observing service %s", operation_id, exc_info=True)
            return self._observation_failed(operation_id, f"{type(exc).__name__}: {exc}")
        return await self._apply(operation_id, adapter, status, endpoint)

    def _observation_failed(self, operation_id: str, error: str) -> OperationRecord:
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if not record.state.final:
                result = dict(record.result or {})
                result["message"] = f"relay could not observe the service: {error}"
                record.result = result
                self.operations.put(operation_id, record)
            return record

    async def observe_open_services(self) -> None:
        """Observe each open service once, so one that ended while relay was down ends."""
        open_services = [
            record.operation_id
            for record in self.operations.all()
            if record.kind == "serve"
            and not record.state.final
            and record.state != OperationState.AWAITING_INPUT
        ]

        async def observe(operation_id: str) -> None:
            try:
                await self.refresh(operation_id)
            except Exception:
                logger.warning("observing service %s at startup", operation_id, exc_info=True)

        await asyncio.gather(*(observe(item) for item in open_services))

    async def _apply(
        self,
        operation_id: str,
        adapter: BaseAdapter[Any],
        status: JobStatus,
        endpoint: Endpoint | None = None,
    ) -> OperationRecord:
        tail: str | None = None
        if status.state.final:
            tail = await self._stdout_tail(adapter, status)
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if record.state.final:
                return record
            record.move(_JOB_TO_OPERATION[status.state], status.message)
            record.exit_code = status.exit_code
            record.outputs = status.outputs
            result = dict(record.result or {})
            if record.kind == "serve":
                if endpoint is not None:
                    result["endpoint"] = public_endpoint(endpoint)
                if status.message is not None:
                    result["message"] = status.message
            if status.state.final:
                result["native_state"] = status.native_state
                if tail is not None:
                    result["stdout_tail"] = tail
            record.result = result
            self.operations.put(operation_id, record)
        return record

    async def _stdout_tail(self, adapter: BaseAdapter[Any], status: JobStatus) -> str | None:
        if "files" not in type(adapter).implements:
            return None
        output = next((item for item in status.outputs if item.name.startswith("stdout")), None)
        if output is None:
            output = next((item for item in status.outputs if item.name.endswith(".out")), None)
        if output is None:
            return None
        try:
            files = await adapter.files()
            size = output.size if output.size is not None else (await files.stat(output.path)).size
            offset = max(0, (size or 0) - OUTPUT_TAIL_BYTES)
            data = await files.read(output.path, offset=offset, limit=OUTPUT_TAIL_BYTES)
        except Refusal:
            return None
        return data.decode("utf-8", errors="replace")

    async def cancel(self, operation_id: str) -> OperationRecord:
        """Cancel an operation wherever it is."""
        record = self.operation(operation_id)
        if record.state.final:
            return record
        started = (
            record.state not in (OperationState.AWAITING_INPUT, OperationState.SUBMITTING)
            if record.kind == "serve"
            else record.handle is not None
        )
        if not started:
            for ask in self.outstanding(record):
                self.answer(ask.ask_id, "cancel", by="cancel")
            record = self._finish(operation_id, OperationState.CANCELLED, None)
            return record
        site = self.site(record.site)
        adapter = self.adapter_named(site, record.adapter)
        if record.kind == "serve":
            status = await self.serve_hook.stop(site, adapter, record)
        elif record.handle is not None:
            status = await adapter.cancel(record.handle)
        else:
            return record
        return await self._apply(operation_id, adapter, status)

    async def wait(
        self, operation_id: str, timeout_s: float, poll_s: float = 1.0
    ) -> OperationRecord:
        """Refresh until the operation is final, waits on a person, or time runs out.

        A service also stops the wait once it is running with an endpoint.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout_s)
        while True:
            record = await self.refresh(operation_id)
            if record.state.final:
                return record
            if record.state == OperationState.AWAITING_INPUT and self.outstanding(record):
                return record
            if service_ready(record):
                return record
            if loop.time() >= deadline:
                return record
            await asyncio.sleep(min(poll_s, max(0.05, deadline - loop.time())))

    # exec and files

    async def exec(
        self,
        site_id: str,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> dict[str, Any]:
        """Run one command (RL9). With no ``timeout_s`` it runs until it ends."""
        site = self.site(site_id)
        if not command:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "exec needs a command")
        (await self.guard.check_exec(site, command)).raise_if_refused()
        name, config, skipped = self.route(site, "exec")
        adapter = self._build(site, name, config)
        result = await adapter.exec(
            command, timeout_s=timeout_s, max_output_bytes=max_output_bytes, on_stall=on_stall
        )
        shown = result.model_dump(mode="json")
        if skipped:
            shown["skipped"] = [item.model_dump() for item in skipped]
        return shown

    # views

    def view(self, record: OperationRecord) -> dict[str, Any]:
        """Return what a client sees of an operation."""
        asks: list[dict[str, Any]] = []
        for ask in self.operation_asks(record):
            shown: dict[str, Any] = {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "state": ask.state,
                "message": ask.message,
            }
            if ask.kind == "url":
                shown["url"] = ask.url
                shown["completed"] = ask.completed_at is not None
            asks.append(shown)
        return {
            "operation_id": record.operation_id,
            "kind": record.kind,
            "site": record.site,
            "adapter": record.adapter,
            "state": record.state.value,
            "native_id": record.handle.native_id if record.handle else None,
            "exit_code": record.exit_code,
            "outputs": [item.model_dump(mode="json") for item in record.outputs],
            "asks": asks,
            "result": record.result,
            "error": record.error,
            "spec_digest": record.spec_digest,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
