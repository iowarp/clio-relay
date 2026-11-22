"""JARVIS's plan on a run's one card, and the card again when the plan changed.

Before the one ask of a ``run`` whose host left the confirm to relay,
:func:`plan_first` has the adapter plan the job JARVIS would submit
(:meth:`~clio_relay.adapters.base.BaseAdapter.plan_job`), and keeps that plan on
the record, so the card shows JARVIS's directives, components, couplings and
script. The submit is bound to it (:func:`~clio_relay.jarvis_plan.bound`). When
JARVIS refuses the submit because the plan changed since the yes,
:func:`refused` plans again and asks once more with the new plan.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from clio_relay import jarvis_plan
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import Ask, GuardDecision
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import installing, job_card, queue_switch

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

ROUNDS = "plan_rounds"
MAX_ROUNDS = 2
CHANGED = "JARVIS's plan changed since your yes, so relay asks again with the new plan."


async def plan_first(
    service: RelayService,
    site: SiteEntry,
    record: OperationRecord,
    *,
    adapter: BaseAdapter[Any],
    decision: GuardDecision,
) -> dict[str, Any] | None:
    """Keep on ``record`` the plan of the package job its card asks about, when there is one.

    Only a package run whose card is the first ask plans here: a package
    review comes before JARVIS may load the package. A site with no JARVIS yet
    makes no plan, and its card shows the job as relay records it.
    """
    job = JobSpec.model_validate(record.spec)
    if record.kind != "run" or not job.steps() or decision.data.get("asks"):
        return None
    plan = await _plan(service, site, record, adapter)
    if plan is not None:
        record.result = {**(record.result or {}), jarvis_plan.KEY: plan}
    return plan


async def _plan(
    service: RelayService, site: SiteEntry, record: OperationRecord, adapter: BaseAdapter[Any]
) -> dict[str, Any] | None:
    try:
        spec = await service.guard.prepare(
            site, queue_switch.applied(record, JobSpec.model_validate(record.spec)), adapter=adapter
        )
        return await adapter.plan_job(spec, operation_id=record.operation_id)
    except Exception:
        # No plan is not a refusal: the card shows the job as relay records it,
        # and the submit says what JARVIS refused.
        logger.info("planning %s found no plan", record.operation_id, exc_info=True)
        return None


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
    """Ask again when JARVIS's plan changed since the yes; hand any other refusal on."""
    record = service.operation(operation_id)
    rounds = int((record.result or {}).get(ROUNDS) or 0)
    card = _card_of(service, record)
    if not jarvis_plan.changed(refusal) or plan_of(record) is None or card is None:
        return await installing.ask_install(service, operation_id, site, refusal)
    if rounds >= MAX_ROUNDS:
        return service.finish(operation_id, OperationState.FAILED, refusal)
    adapter = service.adapter_named(site, record.adapter)
    plan = await _plan(service, site, record, adapter)
    if plan is None:
        return service.finish(operation_id, OperationState.FAILED, refusal)
    old = plan_of(record) or {}
    message = card.message.replace(jarvis_plan.summary(old), jarvis_plan.summary(plan))
    binding = {**card.binding, "plan_sha256": plan.get("plan_sha256")}
    ask = await service.ask_provider.approve(
        site=site,
        operation_id=operation_id,
        message=f"{CHANGED}\n\n{message}",
        binding=binding,
        plain=False,
    )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.result = {**(record.result or {}), jarvis_plan.KEY: plan, ROUNDS: rounds + 1}
        record.asks = [*record.asks, ask.ask_id]
        record.move(OperationState.AWAITING_INPUT, CHANGED)
        ask.meta = {job_card.JOB_META: job_card.snapshot(service, record)}
        service.asks.put(ask.ask_id, ask)
        service.operations.put(operation_id, record)
    return record


def _card_of(service: RelayService, record: OperationRecord) -> Ask | None:
    from clio_relay.surface.confirm import CARD_KIND

    for ask_id in reversed(record.asks):
        ask = service.asks.get(ask_id)
        if ask is not None and ask.binding.get("kind") == CARD_KIND:
            return ask
    return None
