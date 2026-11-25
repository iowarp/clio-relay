"""One ask per call: the typed card a tool that confirms itself raises.

A host that declares relay's ``run``, ``serve`` and ``exec`` as confirming their
own side effects (:data:`~clio_relay.surface.job_card.CONFIRM_META` in the tool's
``_meta``) skips its own permission ask for such a call and flags the request.
Relay then puts exactly one typed ask to the person before anything happens,
whatever the site's spend rule or guard says:

- a submit's card folds the package reviews, the charge and the choice about
  the site's running services into one approve ask, with the job's planned
  snapshot in ``_meta`` (:func:`operation_asks`). A package run's card shows
  JARVIS's own plan of the job, and the yes binds the submit to it
  (:mod:`~clio_relay.surface.plan_card`). A sign-in stays its own URL ask, and
  JARVIS's install questions come later, from JARVIS;
- an ``exec`` asks for that exact argv, once (:func:`exec_confirm`);
- a cancel or stop asks for that operation, once (:func:`cancel_confirm`).

Each is a choice (``approve`` is a ``oneOf``), never a plain confirm, so a host
that answers plain confirms itself still shows it. A client that cannot show a
form gets a typed ``needs_approval`` refusal and nothing runs. Unflagged calls
ask as before.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from fastmcp import Context
from fastmcp.tools import InputRequiredToolResult, ToolResult

from clio_relay import jarvis_plan, jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import (
    Ask,
    Charge,
    GuardDecision,
    ask_detail,
    explain_approval,
    split_question,
)
from clio_relay.guard.reviews import REVIEW_KIND
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import OperationKind, OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason, declined
from clio_relay.jobs.spec import JobSpec, canonical_digest
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import held_services, job_card, plan_card
from clio_relay.surface.elicit import (
    elicit_inline,
    elicitation_modes,
    input_required,
    modern,
    reads_ask_meta,
    refusal_result,
    response_parts,
)
from clio_relay.surface.files_approval import NeedsPerson
from clio_relay.surface.guarding import (
    approval_message,
    bootstraps_jarvis,
    cost_text,
    jarvis_line,
    review_asks,
)

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

CARD_KIND = "job_card"
EXEC_CONFIRM_KIND = "exec_confirm"
CANCEL_KIND = "cancel_confirm"
_LEFT_TO_RELAY = "Your host left this call's confirm to relay: this is the one question for it."


async def operation_asks(
    service: RelayService,
    site: SiteEntry,
    record: OperationRecord,
    job: JobSpec,
    *,
    adapter: BaseAdapter[Any],
    decision: GuardDecision,
    charge: Charge,
    action: str,
    confirm: bool,
) -> list[Ask]:
    """Return the asks a new operation waits on; ``confirm`` folds them into one card."""
    kind: OperationKind = record.kind
    spends = kind != "install"  # JARVIS sizes the install job, and its own ask names it
    asks: list[Ask] = []
    if (need := await adapter.sign_in_needed()) is not None:
        asks.append(
            await service.ask_provider.url(site=site, operation_id=record.operation_id, need=need)
        )
    if confirm:
        if not asks:
            await plan_card.plan_first(service, site, record, adapter=adapter, decision=decision)
        asks.append(await _card(service, site, record, job, decision, charge, action, spends))
        return asks
    asks.extend(await review_asks(service, site, record, decision))
    held = await held_services.question(service, site, kind, record.operation_id)
    if spends and site.spend.needs_approval(charge.node_hours):
        binding = {
            "site": site.id,
            "spec_digest": record.spec_digest,
            "charge": charge.model_dump(mode="json"),
        }
        spend = await service.ask_provider.approve(
            site=site,
            operation_id=record.operation_id,
            message=approval_message(action, site, charge, job.steps()),
            binding=binding,
            plain=False,
        )
        _explain(spend, action, site, charge, spends=True)
        snapshot = job_card.snapshot(service, record)
        if snapshot:
            spend.meta = {**(spend.meta or {}), job_card.JOB_META: snapshot}
        service.asks.put(spend.ask_id, spend)
        asks.append(spend)
    if held is not None:
        # The job's last yes-or-no ask carries the choice; one ask, not two.
        own = [a for a in asks if a.kind == "approve"]
        if own:
            held_services.fold(service, own[-1], record, held)
        else:
            asks.append(await held_services.ask(service, site, record, held))
    return asks


async def _card(
    service: RelayService,
    site: SiteEntry,
    record: OperationRecord,
    job: JobSpec,
    decision: GuardDecision,
    charge: Charge,
    action: str,
    spends: bool,
) -> Ask:
    lines = approval_message(action, site, charge, job.steps()).split("\n")
    if not (spends and site.spend.needs_approval(charge.node_hours)):
        lines = lines[:-2]  # the spend rule does not ask here; the host left the ask to relay
    message = "\n".join(lines)
    binding: dict[str, Any] = {
        "kind": CARD_KIND,
        "site": site.id,
        "spec_digest": record.spec_digest,
    }
    if spends:
        binding["charge"] = charge.model_dump(mode="json")
    if (plan := plan_card.plan_of(record)) is not None:
        if plan.get("plan_sha256"):
            binding["plan_sha256"] = plan["plan_sha256"]
        message += "\n\n" + jarvis_plan.summary(plan)
        if record.kind == "serve" and plan_card.unbound(plan):
            message += "\n" + plan_card.SERVICE_NOTE
    reviews: list[dict[str, Any]] = decision.data.get("asks", [])
    if reviews:
        packages: list[dict[str, Any]] = []
        for review in reviews:
            found = review["binding"]
            listed = found.get("packages")
            if isinstance(listed, list):
                packages.extend(listed)  # pyright: ignore[reportUnknownArgumentType]
            else:
                packages.append({k: v for k, v in found.items() if k != "kind"})
            message += "\n\n" + review["message"]
        binding.update(kind=REVIEW_KIND, packages=packages)
        if bootstraps_jarvis(service, site, record.adapter):
            binding["jarvis"] = jarvis_submit.JARVIS_PIN
            message += "\n" + jarvis_line(site)
    held = await held_services.question(service, site, record.kind, record.operation_id)
    if held is not None:
        message += "\n\n" + held[0]
    message += "\n\n" + _LEFT_TO_RELAY
    ask = await service.ask_provider.approve(
        site=site, operation_id=record.operation_id, message=message, binding=binding, plain=False
    )
    if held is not None:
        held_services.fold(service, ask, record, held)
    _explain(ask, action, site, charge, spends=spends)
    ask.meta = {**(ask.meta or {}), job_card.JOB_META: job_card.snapshot(service, record)}
    service.asks.put(ask.ask_id, ask)
    return ask


def _explain(ask: Ask, action: str, site: SiteEntry, charge: Charge, *, spends: bool) -> None:
    """Say what approving and declining a submit do, beside the Approve and Decline labels."""
    cost = cost_text(charge) if spends else None
    queue = f"the {charge.queue} queue" if charge.queue else "the scheduler's default queue"
    yes = f"Relay submits it to {queue}" + (f"; it may use {cost}." if cost else ".")
    explain_approval(
        ask,
        title=f"{action} on {site.title}",
        yes=yes,
        no="Nothing is submitted and nothing is spent.",
    )


async def one_shot(
    service: RelayService,
    site: SiteEntry,
    *,
    binding: dict[str, Any],
    message: str,
    plain: bool,
    not_done: str,
    operation_id: str | None = None,
    meta: dict[str, Any] | None = None,
) -> Ask:
    """Return the accepted ask for ``binding``, asking the person first when there is none.

    Raises :class:`NeedsPerson` while the ask is pending, and raises the typed
    ``declined`` outcome, saying ``not_done``, when the person said no.
    :func:`used` spends the yes.
    """
    digest = canonical_digest(binding)
    found = [a for a in service.asks.all() if a.binding_digest == digest and a.completed_at is None]
    if found:
        ask = found[0]
    else:
        ask = await service.ask_provider.approve(
            site=site, operation_id=None, message=message, binding=binding, plain=plain
        )
        if meta:
            ask.meta = {**(ask.meta or {}), **meta}
            service.asks.put(ask.ask_id, ask)
    if ask.state == "pending":
        raise NeedsPerson(ask)
    if ask.state != "accepted":
        used(service, ask)
        question = split_question(ask.message)[0]
        raise declined(
            question, not_done, ask_id=ask.ask_id, operation_id=operation_id, site=site.id
        )
    return ask


def used(service: RelayService, ask: Ask) -> None:
    """Mark an approval as spent, so the next call asks again."""
    with service.asks.locked(ask.ask_id):
        current = service.asks.get(ask.ask_id) or ask
        current.completed_at = utc_now()
        service.asks.put(ask.ask_id, current)


async def exec_confirm(service: RelayService, site: SiteEntry, command: list[str]) -> Ask:
    """The one typed ask for an ``exec`` whose host left the confirm to relay."""
    from clio_relay.surface.exec_plan import argv_digest, shown_command

    binding = {"kind": EXEC_CONFIRM_KIND, "site": site.id, "argv_sha256": argv_digest(command)}
    message = (
        f"Run this command on {site.title}'s login side?\n\n{shown_command(command)}\n\n"
        + _LEFT_TO_RELAY
    )
    return await one_shot(
        service,
        site,
        binding=binding,
        message=message,
        plain=False,
        not_done="Relay did not run the command.",
    )


async def cancel_confirm(service: RelayService, record: OperationRecord, verb: str) -> Ask:
    """The one typed ask for a cancel or stop whose host left the confirm to relay."""
    site = service.site(record.site)
    native = (
        f", scheduler job {record.handle.native_id}"
        if record.handle and record.handle.native_id
        else ""
    )
    binding = {"kind": CANCEL_KIND, "operation_id": record.operation_id, "verb": verb}
    if record.kind == "serve" and record.state == OperationState.RUNNING:
        # The one offer to end a service: it says what ends and what the person gets back.
        message = (
            f"{verb.capitalize()} the service {held_services.described(record)} on "
            f"{site.title}{native}? Its programs end and its nodes are released.\n\n"
            f"{_LEFT_TO_RELAY}"
        )
    else:
        message = (
            f"{verb.capitalize()} {record.operation_id} on {site.title}{native}? "
            f"It is {record.state.value}.\n\n{_LEFT_TO_RELAY}"
        )
    snapshot = job_card.snapshot(service, record)
    return await one_shot(
        service,
        site,
        binding=binding,
        message=message,
        plain=False,
        not_done=(
            "Relay did not stop it, so the service keeps running."
            if record.kind == "serve" and record.state == OperationState.RUNNING
            else f"Relay did not {verb} it, so it goes on."
        ),
        operation_id=record.operation_id,
        meta={job_card.JOB_META: snapshot} if snapshot else None,
    )


_LEADS = {
    "exec_plan": "the site is guarded, so relay asks the person before it runs a command "
    "that is not read-only",
}


def needs_person(service: RelayService, ask: Ask, again: str) -> ToolResult:
    """The typed refusal for a one-shot ask a client cannot show; ``again`` says how to go on."""
    lead = _LEADS.get(str(ask.binding.get("kind")), "relay asks the person first")
    refusal = Refusal(
        RefusalReason.NEEDS_APPROVAL,
        f"{lead}. Open the page, or run `clio-relay asks answer`, then {again}",
        ask_id=ask.ask_id,
        asks=[
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "message": ask.message,
                "detail": ask_detail(ask),
                "page": service.ask_provider.page_address(ask),
            }
        ],
    )
    return refusal_result(refusal)


async def with_person[T](
    ctx: Context,
    service: RelayService,
    call: Callable[[], Awaitable[T]],
    *,
    again: str,
) -> T | ToolResult | InputRequiredToolResult:
    """Run ``call``; when it needs the person, ask in the call the way the client can.

    A modern client gets an input request whose ``request_state`` is the ask id, a
    legacy one an elicitation, and one with no form the typed refusal.
    """
    try:
        return await call()
    except NeedsPerson as waiting:
        ask = waiting.ask
        if "form" not in elicitation_modes(ctx):
            return needs_person(service, ask, again)
        if modern(ctx):
            return InputRequiredToolResult(
                input_required([ask], ask.ask_id, reads_meta=reads_ask_meta(ctx))
            )
        action, content = response_parts(await elicit_inline(ctx, ask))
        service.answer(ask.ask_id, action, content, by="elicitation")
        try:
            return await call()
        except NeedsPerson as still:
            return needs_person(service, still.ask, again)


def answer_round(
    service: RelayService, ask_id: str, responses: dict[str, Any], kinds: set[str]
) -> None:
    """Record the answer a retried call carries, for one of its own asks only."""
    ask = service.asks.get(ask_id)
    if ask is None or ask_id not in responses or ask.binding.get("kind") not in kinds:
        return
    action, content = response_parts(responses[ask_id])
    service.answer(ask_id, action, content, by="input_required")
