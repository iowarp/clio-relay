"""JARVIS's plan on a run's or serve's one card, and the card again when the plan changed.

Before the one ask of a ``run`` or ``serve`` whose host left the confirm to relay,
:func:`plan_first` has the adapter plan the job JARVIS would submit
(:meth:`~clio_relay.adapters.base.BaseAdapter.plan_job`), and keeps that plan on
the record, so the card shows JARVIS's directives, components, couplings,
advisories and the script. The submit is bound to it
(:func:`~clio_relay.jarvis_plan.bound`). A serve's job is the one its start
submits, connector and all (:class:`~clio_relay.adapters.hooks.PlansServices`),
and the start keeps what that plan chose. When JARVIS refuses the submit because
the plan changed since the yes, :func:`refused` plans again: a plan the person
sees no change in is bound and submitted without asking, and otherwise the
person is asked once more, with what changed.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from clio_relay import jarvis_plan
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import (
    SERVICE_CHOICES,
    Ask,
    GuardDecision,
    PlansServices,
    ask_text,
)
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import installing, job_card

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

ROUNDS = "plan_rounds"
MAX_ROUNDS = 2
REBINDS = "plan_rebinds"
"""The new plans a submit was bound to with no ask, as the person saw no change in them."""
MAX_REBINDS = 3
CHANGES = "plan_changes"
"""What the person saw change in JARVIS's plan, as the last ask listed it."""
CHANGED = "JARVIS's plan changed since your yes, so relay asks again with the new plan."
REBOUND = (
    "JARVIS's plan changed since your yes only in the input it seals, which the card does "
    "not show (such as the environment a software check since filled in): the directives, "
    "components and batch script are the same, so relay submits under the new plan without "
    "asking again"
)
UNBOUND = frozenset({"plan_sha256", "script"})
"""What a serve's plan leaves out when its serve hook cannot say which job its
start submits: the start adds a connector the plan did not see, so the yes binds
no plan."""
SERVICE_NOTE = (
    "Relay adds the service's connector to this job when it starts it, so the "
    "batch script is made then."
)


async def plan_first(
    service: RelayService,
    site: SiteEntry,
    record: OperationRecord,
    *,
    adapter: BaseAdapter[Any],
    decision: GuardDecision,
) -> dict[str, Any] | None:
    """Keep on ``record`` the plan of the package job its card asks about, when there is one.

    Only a package run or serve whose card is the first ask plans here: a
    package review comes before JARVIS may load the package. A site with no
    JARVIS yet makes no plan, and its card shows the job as relay records it.
    """
    if record.kind not in ("run", "serve") or decision.data.get("asks"):
        return None
    if not installing.job_of(record).steps():
        return None
    plan, kept = await _plan(service, site, record, adapter)
    if plan is not None:
        record.result = {**(record.result or {}), jarvis_plan.KEY: plan, **kept}
    return plan


async def _plan(
    service: RelayService, site: SiteEntry, record: OperationRecord, adapter: BaseAdapter[Any]
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """JARVIS's plan of the job the record submits, and what its start keeps."""
    serve = record.kind == "serve"
    kept: dict[str, Any] = {}
    try:
        job = installing.job_of(record)
        job = _as_service(job) if serve else job
        spec = await service.guard.prepare(site, job, adapter=adapter, service=serve)
        hook = service.serve_hook
        bound = not serve
        if serve and isinstance(hook, PlansServices):
            whole = ServiceSpec.model_validate(record.spec).model_copy(update={"job": spec})
            planned = await hook.plan_job(site, adapter, whole, record)
            if planned is not None:
                spec, kept, bound = planned[0], {SERVICE_CHOICES: planned[1]}, True
        plan = await adapter.plan_job(spec, operation_id=record.operation_id)
    except Exception:
        # No plan is not a refusal: the card shows the job as relay records it,
        # and the submit says what JARVIS refused.
        logger.info("planning %s found no plan", record.operation_id, exc_info=True)
        return None, {}
    if plan is None or bound:
        return plan, kept
    return {key: value for key, value in plan.items() if key not in UNBOUND}, {}


def unbound(plan: dict[str, Any]) -> bool:
    """Say whether a serve's plan binds nothing (:data:`UNBOUND`)."""
    return not plan.get("plan_sha256")


def _as_service(job: JobSpec) -> JobSpec:
    """The service's package job with its serving step marked, as its start marks it."""
    marked = any(step.service for step in job.steps())
    steps = [
        step.model_copy(update={"service": step.service or not marked}) for step in job.steps()
    ]
    return job.with_steps(steps)


def plan_of(record: OperationRecord) -> dict[str, Any] | None:
    """The plan the record's submit is bound to, if any."""
    found = (record.result or {}).get(jarvis_plan.KEY)
    return found if isinstance(found, dict) else None  # pyright: ignore[reportUnknownVariableType]


def bound(spec: JobSpec, record: OperationRecord) -> JobSpec:
    """``spec`` bound to the plan the person said yes to, when the card showed one."""
    return jarvis_plan.bound(spec, plan_of(record))


async def refused(
    service: RelayService, operation_id: str, site: SiteEntry, refusal: Refusal
) -> OperationRecord:
    """Plan again when JARVIS's plan changed since the yes; hand any other refusal on.

    JARVIS refuses a submit whose script or sealed input differ from the plan
    the yes named. Relay plans again and compares the plans as the person sees
    them (:func:`~clio_relay.jarvis_plan.changes`). When nothing they see
    changed, as when a software check JARVIS ran since filled in the
    environment it loads, relay binds the new plan and submits without asking,
    up to :data:`MAX_REBINDS` times. When something did, it asks again, once
    more at most, and the ask says what changed. When JARVIS plans the very
    plan it refused, it cannot keep its own plan, and the operation fails.
    """
    record = service.operation(operation_id)
    card = _card_of(service, record)
    old = plan_of(record)
    if not jarvis_plan.changed(refusal) or old is None or card is None:
        return await installing.ask_install(service, operation_id, site, refusal)
    adapter = service.adapter_named(site, record.adapter)
    plan, kept = await _plan(service, site, record, adapter)
    if plan is None:
        return service.finish(operation_id, OperationState.FAILED, refusal)
    submitted = jarvis_plan.refused_digests(refusal)
    if not plan.get("plan_sha256"):
        return service.finish(operation_id, OperationState.FAILED, refusal)
    if plan.get("plan_sha256") == old.get("plan_sha256"):
        return service.finish(operation_id, OperationState.FAILED, _unkept(site, plan, submitted))
    what = jarvis_plan.changes(old, plan)
    if not what:
        return await _rebind(service, operation_id, refusal, (old, plan, kept), submitted)
    rounds = int((record.result or {}).get(ROUNDS) or 0)
    if rounds >= MAX_ROUNDS:
        return service.finish(operation_id, OperationState.FAILED, refusal)
    message = ask_text(card).replace(jarvis_plan.summary(old), jarvis_plan.summary(plan))
    listed = "\n".join(
        f"  - {line.strip()}" if line.startswith("  ") else f"- {line}" for line in what
    )
    binding = {**card.binding, "plan_sha256": plan.get("plan_sha256")}
    ask = await service.ask_provider.approve(
        site=site,
        operation_id=operation_id,
        message=f"{CHANGED}\n\nWhat changed:\n{listed}\n\n{message}",
        binding=binding,
        plain=False,
    )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.result = {
            **(record.result or {}),
            jarvis_plan.KEY: plan,
            **kept,
            ROUNDS: rounds + 1,
            CHANGES: what,
        }
        record.asks = [*record.asks, ask.ask_id]
        record.move(OperationState.AWAITING_INPUT, CHANGED)
        ask.meta = {**(ask.meta or {}), job_card.JOB_META: job_card.snapshot(service, record)}
        service.asks.put(ask.ask_id, ask)
        service.operations.put(operation_id, record)
    return record


async def _rebind(
    service: RelayService,
    operation_id: str,
    refusal: Refusal,
    plans: tuple[dict[str, Any], dict[str, Any], dict[str, Any]],
    submitted: dict[str, Any],
) -> OperationRecord:
    """Bind the new plan the person sees no change in, and submit it without asking."""
    old, plan, kept = plans
    record = service.operation(operation_id)
    rebinds = list((record.result or {}).get(REBINDS) or [])
    if len(rebinds) >= MAX_REBINDS:
        return service.finish(operation_id, OperationState.FAILED, refusal)
    rebinds.append(
        {
            "from": old.get("plan_sha256"),
            "to": plan.get("plan_sha256"),
            "input_sha256": {"was": old.get("input_sha256"), "now": plan.get("input_sha256")},
            "script_sha256": jarvis_plan.script_sha(plan),
            "submit_rendered": submitted,
        }
    )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.result = {**(record.result or {}), jarvis_plan.KEY: plan, **kept, REBINDS: rebinds}
        record.move(OperationState.AWAITING_INPUT, REBOUND)
        service.operations.put(operation_id, record)
    return await service.submit(operation_id)


def _unkept(site: SiteEntry, plan: dict[str, Any], submitted: dict[str, Any]) -> Refusal:
    """The refusal when JARVIS refuses a submit off the plan it makes again unchanged."""
    return Refusal(
        RefusalReason.SUBMIT_FAILED,
        f"JARVIS on {site.title} refused the submit as off its plan, yet planning again "
        f"gives that same plan (sha256 {str(plan.get('plan_sha256'))[:12]}): its plan and "
        "its submit render the job differently, so relay stops rather than ask again. "
        "This is JARVIS's to fix.",
        kinds=[jarvis_plan.PLAN_CHANGED],
        plan_sha256=plan.get("plan_sha256"),
        plan_input_sha256=plan.get("input_sha256"),
        plan_script_sha256=jarvis_plan.script_sha(plan),
        submit_rendered=submitted,
        retry=False,
    )


def _card_of(service: RelayService, record: OperationRecord) -> Ask | None:
    from clio_relay.surface.confirm import CARD_KIND

    for ask_id in reversed(record.asks):
        ask = service.asks.get(ask_id)
        if ask is not None and ask.binding.get("kind") == CARD_KIND:
            return ask
    return None
