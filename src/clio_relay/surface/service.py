"""The operations service: everything the verbs do, without the wire.

The MCP server, the CLI and the tests all drive this. It owns the site store,
the adapters, the hooks, and the durable operation and ask records. It submits
each operation at most once, from whichever process gets there first, and any
process can observe, answer or cancel an operation by its id.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, cast

from clio_relay import __version__, install_failures, jarvis_submit, relay_state, site_clean
from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SiteAdded, Verb
from clio_relay.adapters.hooks import (
    Ask,
    AskProvider,
    GuardHook,
    HookContext,
    ServeHook,
    Transport,
    hook_factory,
    said_yes,
    split_question,
)
from clio_relay.jarvis_site import Unset
from clio_relay.jarvis_submit import not_run_here
from clio_relay.jobs import declines
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import Endpoint, JobState, JobStatus, StallReport, utc_now
from clio_relay.jobs.ownership import ServeLease
from clio_relay.jobs.records import (
    OperationKind,
    OperationRecord,
    OperationState,
    RecordStore,
    StateChange,
    new_id,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason, declined
from clio_relay.jobs.spec import (
    JobSpec,
    ServiceSpec,
    canonical_digest,
    flow_order,
    step_id,
    step_ids,
)
from clio_relay.login_load import light_argv
from clio_relay.packages import (
    PackageCatalog,
    person_name,
    with_contract_couplings,
    with_contract_resources,
)
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import Route, Routed, route, routes
from clio_relay.sites.store import SiteStore
from clio_relay.surface import confirm as confirm_card
from clio_relay.surface import (
    exec_plan,
    held_services,
    install_op,
    installing,
    interrupted,
    queue_switch,
    same_run,
)
from clio_relay.surface import plan_card as job_plan
from clio_relay.surface.describing import relay_report, site_report
from clio_relay.surface.elicit import ask_view
from clio_relay.surface.endpoints import public_endpoint, service_ready
from clio_relay.surface.exec_plan import ExecPlan
from clio_relay.surface.job_card import snapshot as job_snapshot
from clio_relay.surface.outputs import marked_status, run_provenance, stdout_tail
from clio_relay.surface.package_progress import with_package_progress
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.stubs import DefaultGuard, StubAskProvider, StubServe
from clio_relay.surface.transient import drop_transient

logger = logging.getLogger(__name__)

SUBMIT_GRACE = timedelta(minutes=2)


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


def _steps_label(spec: JobSpec, titles: Mapping[str, str] | None = None) -> str | None:
    """Name a package job: its package, or its pipeline by the way data flows.

    Each package is named as a person knows it (:func:`person_name`), so the
    question reads "Run LAMMPS", not "Run clio_jarvis.lammps". With couplings,
    each producer is named feeding its consumer, and when JARVIS starts the
    steps in another order the label says which starts first, so no one reads
    the launch order as the data's direction. Without couplings the steps are
    named in launch order.
    """
    steps = spec.steps()
    names = [person_name(step.name, titles) for step in steps]
    if len(names) <= 1:
        return names[0] if names else None
    if not spec.couplings:
        return "the pipeline " + " then ".join(names)
    ids = step_ids(steps)
    package = dict(zip(ids, names, strict=True))
    if len(set(names)) < len(names):
        package = {id_: id_ for id_ in ids}
    edges = [(step_id(steps, e.producer), step_id(steps, e.consumer)) for e in spec.couplings]
    parts = [f"{package[producer]} feeding {package[consumer]}" for producer, consumer in edges]
    joined = {end for edge in edges for end in edge}
    parts += [f"{package[id_]} beside them" for id_ in ids if id_ not in joined]
    label = "the pipeline " + ", ".join(parts)
    if flow_order(ids, edges)[0] != ids[0]:
        label += f" ({package[ids[0]]} starts first)"
    return label


def _record_job(record: OperationRecord) -> JobSpec | None:
    raw = record.spec.get("job") if record.kind == "serve" else record.spec
    if not isinstance(raw, dict):
        return None
    try:
        return JobSpec.model_validate(raw)
    except ValueError:
        return None


def _steps_view(record: OperationRecord) -> list[dict[str, Any]]:
    """Return a package job's pipeline steps, in launch order, with the nodes each gets."""
    job = _record_job(record)
    if job is None:
        return []
    steps = job.steps()
    return [
        {
            "id": id_,
            "package": step.name,
            "version": step.version,
            "sha256": step.sha256,
            "nodes": step.nodes(),
            "service": step.service,
        }
        for id_, step in zip(step_ids(steps), steps, strict=True)
    ]


def _couplings_view(record: OperationRecord) -> list[dict[str, Any]]:
    """Return the pipeline's couplings, producer to consumer, each end a step's id."""
    job = _record_job(record)
    if job is None or not job.couplings:
        return []
    steps = job.steps()
    return [
        {
            "from": step_id(steps, edge.producer),
            "to": step_id(steps, edge.consumer),
            "kind": edge.kind,
            "label": edge.label or edge.kind,
        }
        for edge in job.couplings
    ]


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
        self.packages = PackageCatalog(paths.packages_dir)
        self.operations: RecordStore[OperationRecord] = RecordStore(
            paths.operations_dir, OperationRecord
        )
        self.asks: RecordStore[Ask] = RecordStore(paths.asks_dir, Ask)
        self.lease = ServeLease(paths.operations_dir / ".serves", __version__, new_id("serve"))
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
        self.offered_plans: dict[tuple[str, str], dict[str, Any]] = {}
        """Commands relay offered in ``describe``, by site and argv digest (:mod:`exec_plan`)."""

    async def close(self) -> None:
        """Release the hooks."""
        await self.ask_provider.close()
        await self.serve_hook.close()
        self.lease.release()

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
        """Pin the packages, fill what their contracts set, then the site's defaults.

        The packages' contracts (walltime, queue and process layout, read from
        their parameters) come before the site's defaults, so the price, the
        spend ask and the submitted job agree. What the job sets itself wins.
        """
        if spec.steps():
            steps = [self.packages.resolve(site.id, step) for step in spec.steps()]
            spec = spec.with_steps(steps)
            entries = [self.packages.entry(site.id, step) for step in steps]
            spec = with_contract_resources(spec, entries)
            spec = with_contract_couplings(spec, entries)
        if spec.install_settings:
            offers = install_failures.site_offers(self.operations.all(), site.id)
            install_failures.check_settings(spec, offers, site=site.id)
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
        self, site_id: str, spec: JobSpec, *, task: bool = False, confirm: bool = False
    ) -> OperationRecord:
        """Check, price and record a run, ask what must be asked, and submit if nothing must."""
        site = self.site(site_id)
        spec = await self.prepared_run(site, spec)
        what = spec.executable or _steps_label(spec, self.packages.titles(site.id)) or "a job"
        digest, dumped = spec.digest(), spec.model_dump(mode="json")
        return await self.open_operation(
            site, "run", spec, dumped, digest, f"Run {what}", task=task, confirm=confirm
        )

    async def list_packages(self, site: SiteEntry, job: JobSpec, verb: Verb) -> None:
        """List the site's packages when the catalog lacks a step, refusing when not set up.

        A cached entry is enough: the site script checks each step's digest again.
        So is a pinned step on an open site; a guarded one reviews from the listing.
        """
        missing = [step for step in job.steps() if self.packages.entry(site.id, step) is None]
        pinned = all(step.version and step.sha256 for step in missing)
        if not missing or (pinned and site.policy != "guarded"):
            return
        adapter = self.adapter_for(site, verb)
        if adapter.runs_packages:
            listed = await self.packages.refresh(site.id, adapter)
            if isinstance(listed, Unset):
                raise jarvis_submit.not_at_pin(site.title, listed)

    async def prepared_run(self, site: SiteEntry, spec: JobSpec) -> JobSpec:
        """Return the run's job as relay records and digests it."""
        await self.list_packages(site, spec, "run")
        return self.apply_defaults(site, spec)

    def prepare_service(self, site: SiteEntry, spec: ServiceSpec) -> ServiceSpec:
        """Fill the service job's site defaults.

        Which step of a coupled pipeline serves, where, and when it starts is
        JARVIS's: ``ppl check`` reads each package's deployment profile, its
        ``host_slice`` and ``start_mode``, before anything is submitted.
        """
        return spec.model_copy(update={"job": self.apply_defaults(site, spec.job)})

    async def prepared_serve(self, site: SiteEntry, spec: ServiceSpec) -> ServiceSpec:
        """Return the service as relay records and digests it."""
        await self.list_packages(site, spec.job, "serve")
        return self.prepare_service(site, spec)

    async def start_serve(
        self, site_id: str, spec: ServiceSpec, *, task: bool = False, confirm: bool = False
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
        spec = await self.prepared_serve(site, spec)
        ports = ", ".join(port.name for port in spec.ports)
        what = (
            spec.job.executable
            or _steps_label(spec.job, self.packages.titles(site.id))
            or "a service"
        )
        return await self.open_operation(
            site,
            "serve",
            spec.job,
            spec.model_dump(mode="json"),
            spec.digest(),
            f"Start {what} as a service with port(s) {ports}",
            task=task,
            confirm=confirm,
        )

    async def open_operation(
        self,
        site: SiteEntry,
        kind: OperationKind,
        job: JobSpec,
        spec: dict[str, Any],
        spec_digest: str,
        action: str,
        *,
        task: bool,
        confirm: bool = False,
    ) -> OperationRecord:
        """Record a run, serve or install operation, ask what must be asked, and submit.

        ``confirm``: the host left the call's confirm to relay, so one typed card asks.
        """
        verb: Verb = "serve" if kind == "serve" else "run"
        adapter_name, config, skipped = self.route(site, verb)
        adapter = self._build(site, adapter_name, config)
        relay_state.refuse_relay_state(adapter, job, site.id)
        if job.steps() and not adapter.runs_packages:
            raise not_run_here(adapter_name)
        decision = await self.guard.check_run(site, job)
        decision.raise_if_refused()
        charge = (await self.guard.price(site, job)).with_layout(job)
        operation_id = new_id("op")
        spends = kind != "install"  # JARVIS sizes the install job, and its own ask names it
        result: dict[str, Any] = {"charge": charge.model_dump(mode="json")} if spends else {}
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
            owner=self.lease.owner(),
        )
        asks = await confirm_card.operation_asks(
            self,
            site,
            record,
            job,
            adapter=adapter,
            decision=decision,
            charge=charge,
            action=action,
            confirm=confirm,
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
        comment: str | None = None,
    ) -> Ask:
        """Record a person's answer to an ask, once, with what they said beside it."""
        with self.asks.locked(ask_id):
            ask = self.asks.get(ask_id)
            if ask is None:
                raise Refusal(
                    RefusalReason.ASK_NOT_FOUND, f"there is no ask {ask_id!r}", ask_id=ask_id
                )
            said = {"comment": comment} if comment else {}
            if action == "accept":
                state = "accepted" if ask.kind != "approve" or said_yes(content) else "declined"
                ask.resolve(state, {"action": "accept", "content": content, **said}, by)
            elif action == "decline":
                ask.resolve("declined", {"action": "decline", **said}, by)
            else:
                ask.resolve("cancelled", {"action": "cancel", **said}, by)
            self.asks.put(ask_id, ask)
        return ask

    async def same_request(
        self, record: OperationRecord, site: str | None, spec: JobSpec | ServiceSpec | None
    ) -> None:
        """Refuse a retry or resume whose site or spec differs from what was approved.

        The spec is prepared as start_run or start_serve prepared it, site cores
        included, so the same request digests the same; the form without site
        cores also matches, for a site described differently since.
        """
        if site is None and spec is None:
            return
        changed = site is not None and site != record.site
        if spec is not None and not changed:
            entry = self.site(record.site)
            if isinstance(spec, ServiceSpec):
                forms = [self.prepare_service(entry, spec), await self.prepared_serve(entry, spec)]
            else:
                forms = [self.apply_defaults(entry, spec), await self.prepared_run(entry, spec)]
            changed = all(form.digest() != record.spec_digest for form in forms)
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
        if record.state != OperationState.AWAITING_INPUT or not self.acts_on(record):
            return record
        if queue_switch.asking(record):
            phase = queue_switch.state_of(record).get("phase")
            handle = installing.install_handle(record) if phase == "install" else record.handle
            return await queue_switch.advance(self, operation_id, handle)
        relayed, kept = queue_switch.ask_ids(record), held_services.ask_ids(record)
        waiting = False
        for ask in self.operation_asks(record):
            if ask.ask_id in relayed or (ask.ask_id in kept and ask.state != "pending"):
                continue
            if ask.state in ("declined", "cancelled"):
                return self.finish(
                    operation_id,
                    OperationState.CANCELLED,
                    declined(
                        split_question(ask.message)[0],
                        declines.not_done(record),
                        ask_id=ask.ask_id,
                        operation_id=operation_id,
                    ),
                )
            if ask.state == "pending":
                if ask.kind == "url" and await self._signed_in_elsewhere(ask, record):
                    continue
                waiting = True
                continue
            if (mismatch := confirm_card.stale(ask, record)) is not None:
                return self.finish(operation_id, OperationState.FAILED, mismatch)
            if ask.kind == "url" and not await self._signed_in(ask, record):
                waiting = True
        if waiting:
            return record
        if (reuse := same_run.chosen(self, record)) is not None:
            return self.finish(operation_id, OperationState.CANCELLED, reuse)
        if installing.pending(record):
            return await self._install(operation_id)
        return await self.submit(operation_id)

    async def _install(self, operation_id: str) -> OperationRecord:
        """Start the install the answered asks approved, once, behind ``submitting``."""
        record = self.operation(operation_id)
        site = self.site(record.site)
        try:
            adapter = self.adapter_named(site, record.adapter)
        except Refusal as refusal:
            return self.finish(operation_id, OperationState.FAILED, refusal)
        lease = self.operations.claim_submit(operation_id)
        if lease is None:  # another call is starting it; it moves the record on
            return self.operation(operation_id)
        with lease:
            with self.operations.locked(operation_id):
                record = self.operation(operation_id)
                if record.state != OperationState.AWAITING_INPUT or not installing.pending(record):
                    return record
                state = {**installing.state_of(record), "stage": "starting"}
                record.result = {**(record.result or {}), installing.INSTALL_KEY: state}
                record.move(OperationState.SUBMITTING, "starting the install")
                self.operations.put(operation_id, record)
            return await installing.start_install(self, operation_id, site, adapter)

    def acts_on(self, record: OperationRecord) -> bool:
        """Say whether this process may move `record` (see `jobs.ownership`)."""
        return self.lease.acts_on(self.operations, record)

    def finish(
        self,
        operation_id: str,
        state: OperationState,
        refusal: Refusal | None,
        *,
        message: str | None = None,
        exit_code: int | None = None,
        result: dict[str, Any] | None = None,
    ) -> OperationRecord:
        """End an operation: with a refusal, or as a job ends, its message, exit code and result."""
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if not record.state.final:
                if refusal is not None:
                    record.error = refusal.to_dict()
                if exit_code is not None:
                    record.exit_code = exit_code
                if result:
                    record.result = {**(record.result or {}), **result}
                record.move(state, message if refusal is None else refusal.message)
                self.operations.put(operation_id, record)
                drop_transient(self.content, self.operations, record)
        return record

    async def submit(self, operation_id: str) -> OperationRecord:
        """Submit once. Whoever moves the record to ``submitting`` submits.

        The scheduler job name, when the adapter names jobs, is recorded with
        the move, before anything reaches the scheduler (RL25a).
        """
        record = self.operation(operation_id)
        if record.state != OperationState.AWAITING_INPUT or not self.acts_on(record):
            return record
        site = self.site(record.site)
        try:
            adapter = self.adapter_named(site, record.adapter)
        except Refusal as refusal:
            return self.finish(operation_id, OperationState.FAILED, refusal)
        job_name = _job_name(adapter, operation_id) if record.kind == "run" else None
        # The lease is taken before the move, so a live process holds it for as
        # long as the record reads ``submitting``; a crash drops it. Whoever finds
        # it held leaves the submission to its holder.
        lease = self.operations.claim_submit(operation_id)
        if lease is None:
            return self.operation(operation_id)
        with lease:
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
            return await self._submit_held(operation_id, site, adapter, record)

    async def _submit_held(
        self,
        operation_id: str,
        site: SiteEntry,
        adapter: BaseAdapter[Any],
        record: OperationRecord,
    ) -> OperationRecord:
        await held_services.end_chosen(self, operation_id)
        if record.kind == "serve":
            return await self._start_service(operation_id, site, adapter, record)
        if record.kind == "install":
            return await install_op.check(self, operation_id, site, adapter)
        try:
            # The guard may rewrite what is submitted; record.spec stays what was approved.
            spec = await self.guard.prepare(
                site,
                queue_switch.applied(record, JobSpec.model_validate(record.spec)),
                adapter=adapter,
            )
            handle = await adapter.run(job_plan.bound(spec, record), operation_id=operation_id)
        except Refusal as refusal:
            return await job_plan.refused(self, operation_id, site, refusal)
        except Exception as exc:
            logger.exception("submitting %s failed", operation_id)
            return self.finish(
                operation_id,
                OperationState.FAILED,
                Refusal(RefusalReason.SUBMIT_FAILED, f"submission failed: {exc}"),
            )
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            record.handle = handle
            # Cancelled, or ended by anyone, while it was submitting: the job must not outlive it.
            ended = record.state.final
            record.move(OperationState.QUEUED, f"native id {handle.native_id}")
            self.operations.put(operation_id, record)
        if ended:
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
            service = ServiceSpec.model_validate(record.spec)
            job = await self.guard.prepare(
                site, queue_switch.applied(record, service.job), adapter=adapter, service=True
            )
            job = job_plan.bound(job, record)
            endpoint = await self.serve_hook.start(
                site, adapter, service.model_copy(update={"job": job}), record
            )
        except Refusal as refusal:
            return await job_plan.refused(self, operation_id, site, refusal)
        except Exception as exc:
            logger.exception("starting service %s failed", operation_id)
            return self.finish(
                operation_id,
                OperationState.FAILED,
                Refusal(RefusalReason.SUBMIT_FAILED, f"starting the service failed: {exc}"),
            )
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            # Cancelled, or ended by anyone, while it started: the service must not outlive it.
            cancelled = record.state.final
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
        if record.state.final or not self.acts_on(record):
            return record
        if record.state == OperationState.AWAITING_INPUT:
            return await self.advance(operation_id)
        if record.state == OperationState.SUBMITTING and self.operations.submit_in_hand(
            operation_id
        ):
            # A live process is submitting it and moves it on; only a crash leaves it here.
            return record
        site = self.site(record.site)
        adapter = self.adapter_named(site, record.adapter)
        install = installing.install_handle(record)
        if install is not None:
            return await installing.refresh_install(self, operation_id, adapter, install)
        if installing.state_of(record).get("stage") == "starting":
            # The process installing JARVIS or submitting the install job is gone.
            return await interrupted.start_cut_short(self, operation_id, adapter, record)
        if record.kind == "serve":
            return await self._refresh_service(operation_id, site, adapter, record)
        if record.kind == "install":
            if record.state == OperationState.SUBMITTING:
                return await interrupted.check_cut_short(self, operation_id)
            return record
        handle = record.handle
        if handle is None:
            found = await adapter.find(operation_id)
            if (
                found is None
                and record.state == OperationState.SUBMITTING
                and utc_now() - record.updated_at > SUBMIT_GRACE
                and interrupted.can_find(adapter)
            ):
                return await interrupted.submit_cut_short(self, operation_id)
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
                drop_transient(self.content, self.operations, record)
            return record
        try:
            status = await adapter.status(handle)
        except Exception as exc:
            # One failed look is not the job's end; it stays open and says why.
            logger.warning("observing %s", operation_id, exc_info=True)
            reason = exc.message if isinstance(exc, Refusal) else f"{type(exc).__name__}: {exc}"
            return self.observation_failed(operation_id, reason, "job")
        status = await with_package_progress(adapter, record, status)
        record = await self._apply(operation_id, adapter, status)
        return await queue_switch.consider(self, operation_id, adapter, handle, "job")

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
                return self.finish(operation_id, OperationState.FAILED, refusal)
            logger.warning("observing service %s: %s", operation_id, refusal.message)
            return self.observation_failed(operation_id, refusal.message)
        except Exception as exc:
            # One failed look is not the service's end; it stays open and says why.
            logger.warning("observing service %s", operation_id, exc_info=True)
            return self.observation_failed(operation_id, f"{type(exc).__name__}: {exc}")
        record = await self._apply(operation_id, adapter, status, endpoint)
        return await queue_switch.consider(self, operation_id, adapter, record.handle, "job")

    def observation_failed(
        self, operation_id: str, error: str, what: str = "service"
    ) -> OperationRecord:
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if not record.state.final:
                result = dict(record.result or {})
                result["message"] = f"relay could not observe the {what}: {error}"
                result["observation_failed_at"] = utc_now().isoformat()
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
            tail = await stdout_tail(adapter, status)
        with self.operations.locked(operation_id):
            record = self.operation(operation_id)
            if record.state.final:
                return record
            record.move(_JOB_TO_OPERATION[status.state], status.message)
            record.exit_code = status.exit_code
            status = marked_status(record, status)
            record.outputs = status.outputs
            result = dict(record.result or {})
            result["observed_at"] = status.observed_at.isoformat()
            result["native_state"] = status.native_state
            result.pop("observation_failed_at", None)
            if status.progress is not None:
                result["progress"] = status.progress.model_dump(mode="json")
            if status.control is not None:
                result["control"] = status.control
            if status.queue:
                result["queue"] = status.queue
                status.message = status.message or jarvis_submit.queue_message(status.queue)
            else:
                result.pop("queue", None)
            if status.message is not None:
                result["message"] = status.message
            elif record.kind != "serve":
                result.pop("message", None)
            if record.kind == "serve" and endpoint is not None:
                result["endpoint"] = public_endpoint(endpoint)
                # The first time the forward answers: a task client wakes on it.
                result.setdefault("ready_at", status.observed_at.isoformat())
            if status.state.final and tail is not None:
                result["stdout_tail"] = tail
            for key in (
                "issues",
                "site_facts",
                "service_error",
                "components",
                "outcomes",
                "started_at",
            ):
                result.pop(key, None)
                if getattr(status, key):
                    result[key] = status.model_dump(mode="json", include={key})[key]
            record.result = result
            self.operations.put(operation_id, record)
            drop_transient(self.content, self.operations, record)
        return record

    async def cancel(self, operation_id: str) -> OperationRecord:
        """Cancel an operation wherever it is.

        Once it ends, any ask still pending on it is withdrawn, so no page or
        ``asks list`` keeps offering a question about a cancelled operation.
        """
        record = await self._cancel(operation_id)
        if record.state.final:
            for ask in installing.withdrawable(self, self.operation_asks(record), operation_id):
                if ask.state == "pending":
                    self.answer(ask.ask_id, "cancel", by="cancel")
        return record

    async def _cancel(self, operation_id: str) -> OperationRecord:
        record = self.operation(operation_id)
        if record.state.final:
            return await site_clean.leftover_cancel(self, record)
        if not self.acts_on(record):
            return record
        started = (
            record.state not in (OperationState.AWAITING_INPUT, OperationState.SUBMITTING)
            if record.kind == "serve"
            else record.handle is not None
        )
        install = installing.install_handle(record)
        if install is not None:
            adapter = self.adapter_named(self.site(record.site), record.adapter)
            status = await adapter.cancel(install)
            if status.state.final:
                return self.finish(operation_id, OperationState.CANCELLED, None)
            return await installing.refresh_install(self, operation_id, adapter, install)
        if not started:
            for ask in installing.withdrawable(self, self.outstanding(record), operation_id):
                self.answer(ask.ask_id, "cancel", by="cancel")
            return self.finish(operation_id, OperationState.CANCELLED, None)
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
        plan: ExecPlan | None = None,
        read_only: bool = False,
        confirm: bool = False,
    ) -> dict[str, Any]:
        """Run one command (RL9). With no ``timeout_s`` it runs until it ends.

        On a guarded site a command that is not read-only runs only with a
        ``plan`` the person approved (:mod:`clio_relay.surface.exec_plan`);
        until then this raises :class:`NeedsPerson`. ``read_only`` runs only reads.
        ``confirm``: the host left the confirm to relay, so the person is asked anyway.
        """
        site = self.site(site_id)
        if not command:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "exec needs a command")
        decision = await self.guard.check_exec(site, command, read_only=read_only)
        approved: Ask | None = None
        if not decision.allowed:
            guarded = site.policy == "guarded" and not read_only
            if plan is None or not guarded or decision.reason != RefusalReason.POLICY_REFUSED:
                decision.raise_if_refused()
            else:
                approved = await exec_plan.approve(self, site, command, plan, typed=confirm)
                cap = exec_plan.PLAN_TIMEOUT_S
                timeout_s = min(timeout_s or cap, cap)
        light = approved is not None or bool(decision.data.get("light"))
        if confirm and approved is None:
            approved = await confirm_card.exec_confirm(self, site, command)
        name, config, skipped = self.route(site, "exec")
        adapter = self._build(site, name, config)
        argv = light_argv(command, exec_plan.PLAN_LIMITS) if light else command
        try:
            result = await adapter.exec(
                argv, timeout_s=timeout_s, max_output_bytes=max_output_bytes, on_stall=on_stall
            )
        finally:
            if approved is not None:
                exec_plan.used(self, approved)
        shown = result.model_dump(mode="json")
        shown["argv"] = list(command)
        if light:
            shown["light"] = True
        if approved is not None:
            shown["ask_id"] = approved.ask_id
        if skipped:
            shown["skipped"] = [item.model_dump() for item in skipped]
        return shown

    # views

    def view(self, record: OperationRecord) -> dict[str, Any]:
        """Return what a client sees of an operation."""
        asks = [ask_view(ask) for ask in self.operation_asks(record)]
        declined_as = declines.outcome(record)
        return {
            "operation_id": record.operation_id,
            "kind": record.kind,
            "site": record.site,
            "adapter": record.adapter,
            "state": record.state.value,
            "native_id": record.handle.native_id if record.handle else None,
            "exit_code": record.exit_code,
            "outputs": [item.model_dump(mode="json") for item in record.outputs],
            "provenance": run_provenance(record.outputs),
            "asks": asks,
            "steps": _steps_view(record),
            "couplings": _couplings_view(record),
            "result": record.result,
            "error": None if declined_as else record.error,
            "declined": declined_as,
            "spec_digest": record.spec_digest,
            "owner": self.lease.shown(record.owner, final=record.state.final),
            "job": job_snapshot(self, record),
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
