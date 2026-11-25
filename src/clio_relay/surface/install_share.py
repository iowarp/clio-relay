"""One ask per site for what is the site's, however many operations need it at once.

Operations that find a site without JARVIS at the same time share one site-scoped
approval (its binding names the site and the pin, not one job). The bootstrap
script takes a lock in the site's jarvis-cd home, so a second install waits for
the first and reuses it (:data:`~clio_relay.jarvis_bootstrap.BOOTSTRAP_SCRIPT`).

JARVIS's own site-scope questions (which MPI, which compiler) are shared the same
way: operations on the site that get the same question, the same ``issue_id``
with the same candidates, hold one ask (:func:`choice_ask`). JARVIS records the
answer for the site, so it is one answer for every holder.

An answer to a shared ask moves on every operation that holds it
(:func:`holders`). Cancelling one leaves the ask to the others (:func:`withdrawable`).
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, cast

from clio_relay import jarvis_report, jarvis_submit
from clio_relay.adapters.hooks import Ask
from clio_relay.guard.reviews import REVIEW_KIND
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.spec import canonical_digest
from clio_relay.sites.models import SiteEntry

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

JARVIS_KIND = "jarvis_bootstrap"
CHOICE_KIND = "jarvis_choice"
CHOICE_FIELD = "constraint"
_log = logging.getLogger(__name__)


def _install_state(record: OperationRecord) -> dict[str, Any]:
    found = (record.result or {}).get("install")
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def site_lock_id(site_id: str) -> str:
    """The record lock relay holds while it asks a site-scoped question on ``site_id``."""
    return "jarvis-site-" + hashlib.sha256(site_id.encode()).hexdigest()[:24]


def _live_elsewhere(service: RelayService, operation_id: str) -> list[OperationRecord]:
    return [
        other
        for other in service.operations.all()
        if other.operation_id != operation_id and not other.state.final
    ]


def shared_jarvis_ask(service: RelayService, site_id: str, operation_id: str) -> Ask | None:
    """Return the jarvis-cd ask another live operation on the site already holds.

    It covers this operation too: one still pending, or one approved for an
    install of jarvis-cd that has not finished yet. The bootstrap's site lock
    makes the second install wait for the first and reuse it.
    """
    for other in _live_elsewhere(service, operation_id):
        if other.site != site_id:
            continue
        other_state = _install_state(other)
        installing = other_state.get("kind") == "jarvis" and other_state.get("stage") != "done"
        for ask in service.operation_asks(other):
            if (
                ask.kind == "approve"
                and ask.binding.get("scope") == "site"
                and ask.binding.get("kind") == JARVIS_KIND
                and ask.binding.get("jarvis") == jarvis_submit.JARVIS_PIN
                and (ask.state == "pending" or (ask.state == "accepted" and installing))
            ):
                return ask
    return None


def jarvis_consented(service: RelayService, site_id: str, record: OperationRecord) -> bool:
    """Say whether the person already agreed that relay installs its jarvis-cd here.

    A package review that named relay's jarvis-cd at this pin covers it
    (:func:`~clio_relay.surface.guarding.review_asks`): this operation's own,
    accepted and unchanged, or one on another live operation of the site that is
    installing jarvis-cd now. The bootstrap's site lock makes the second install
    wait for the first and reuse it.
    """
    for other in [record, *_live_elsewhere(service, record.operation_id)]:
        if other.site != site_id:
            continue
        own = other.operation_id == record.operation_id
        other_state = _install_state(other)
        if not own and (other_state.get("kind") != "jarvis" or other_state.get("stage") == "done"):
            continue
        for ask in service.operation_asks(other):
            if (
                ask.kind == "approve"
                and ask.state == "accepted"
                and ask.binding.get("kind") == REVIEW_KIND
                and ask.binding.get("jarvis") == jarvis_submit.JARVIS_PIN
                and ask.binding_digest == canonical_digest(ask.binding)
            ):
                return True
    return False


def choice_binding(site_id: str, issue: dict[str, Any]) -> dict[str, Any] | None:
    """Return what a site-scope JARVIS question is bound to, or ``None`` for any other.

    JARVIS's ``issue_id`` digests the question with its candidates; the options
    are bound too, so a question that offers something else is asked on its own.
    A question for this occasion only (:func:`~clio_relay.jarvis_report.once`) is
    each operation's own.
    """
    issue_id = issue.get("issue_id")
    if issue.get("scope") != "site" or not isinstance(issue_id, str) or not issue_id:
        return None
    if jarvis_report.once(issue):
        return None
    return {
        "site": site_id,
        "scope": "site",
        "kind": CHOICE_KIND,
        "issue_id": issue_id,
        "options": jarvis_report.choice_options(issue),
    }


def offer_map(issue: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Return ``{option: (issue_id, constraint)}`` for every option of a question's ask."""
    issue_id = str(issue.get("issue_id"))
    own = {option: (issue_id, option) for option in jarvis_report.choice_options(issue)}
    return {**own, **jarvis_report.override_choices(issue)}


def choice_answers(service: RelayService, state: dict[str, Any]) -> dict[str, Any]:
    """Return the person's answers to this round's open issues; JARVIS replays the rest.

    ``state`` has ``choices`` (``{ask_id: issue_id}``) and ``offers``
    (``{ask_id:`` :func:`offer_map` ``}``). A per-package option
    (``<constraint> for <package> only``) answers that package's override issue
    instead, and leaves the site's question open. An option this operation was
    not offered (a shared ask's other package) answers nothing here.
    """
    picked: dict[str, tuple[str, str]] = {}
    offers = cast(dict[str, dict[str, list[str]]], state.get("offers") or {})
    for ask_id, issue_id in cast(dict[str, str], state.get("choices") or {}).items():
        ask = service.asks.get(ask_id)
        content = cast(dict[str, Any], (ask.answer or {}).get("content") or {}) if ask else {}
        value = content.get(CHOICE_FIELD)
        if not isinstance(value, str) or not value:
            continue
        if ask_id not in offers:
            picked[issue_id] = (value, ask_id)
        elif (offer := offers[ask_id].get(value)) is not None:
            picked[offer[0]] = (offer[1], ask_id)
    return jarvis_report.answers_document(picked)


def _answers_site(ask: Ask, issue: dict[str, Any]) -> bool:
    """Say whether an accepted shared ask answered the site's question itself.

    A per-package answer (``... for <package> only``) leaves the question open.
    """
    content = cast(dict[str, Any], (ask.answer or {}).get("content") or {})
    return ask.state == "accepted" and content.get(CHOICE_FIELD) in jarvis_report.choice_options(
        issue
    )


async def choice_ask(
    service: RelayService, site: SiteEntry, operation_id: str, issue: dict[str, Any]
) -> Ask:
    """Ask one of JARVIS's questions, or reuse the same site-scope one held elsewhere.

    Another live operation on the site that holds the same question, unanswered
    or answered, lends it. The caller holds :func:`site_lock_id`.
    """
    binding = choice_binding(site.id, issue)
    if binding is not None:
        for other in _live_elsewhere(service, operation_id):
            if other.site != site.id:
                continue
            for ask in service.operation_asks(other):
                if (
                    ask.kind == "choose"
                    and ask.binding == binding
                    and (ask.state == "pending" or _answers_site(ask, issue))
                ):
                    return ask
    ask = await service.ask_provider.choose(
        site=site,
        operation_id=operation_id,
        message=jarvis_report.choice_message(issue),
        field_name=CHOICE_FIELD,
        options=jarvis_report.choice_details(issue),
        title=jarvis_report.choice_title(issue),
        recommended_reason=jarvis_report.recommended_reason(issue),
    )
    if binding is not None:
        ask.bind(binding)
        service.asks.put(ask.ask_id, ask)
    return ask


def withdrawable(service: RelayService, asks: list[Ask], operation_id: str) -> list[Ask]:
    """Return ``asks`` but the site-scoped ones another live operation also waits on."""
    if not any(ask.binding.get("scope") == "site" for ask in asks):
        return asks
    held = {ask_id for other in _live_elsewhere(service, operation_id) for ask_id in other.asks}
    return [ask for ask in asks if ask.ask_id not in held]


def holders(service: RelayService, ask: Ask) -> list[str]:
    """Return the operations an answer to ``ask`` moves on: all that hold a shared one."""
    own = [ask.operation_id] if ask.operation_id else []
    if ask.binding.get("scope") != "site":
        return own
    live = [
        other.operation_id
        for other in service.operations.all()
        if ask.ask_id in other.asks and not other.state.final
    ]
    return live or own


async def advance_holders(
    service: RelayService, operation_id: str, answered: list[Ask]
) -> OperationRecord:
    """Advance ``operation_id``, then every other operation an answer in ``answered`` moves on.

    However the answer came (``tasks/update``, an ``input_required`` round, an
    elicitation), a shared ask moves every holder at once, so no other task is
    left showing a question that is already answered.
    """
    others: list[str] = []
    for ask in answered:
        others.extend(held for held in holders(service, ask) if held != operation_id)
    record = await service.advance(operation_id)
    for other in dict.fromkeys(others):
        try:
            await service.advance(other)
        except Exception as error:  # noqa: BLE001 - the other operation records its own failure
            _log.warning("could not advance %s after a shared answer: %s", other, error)
    return record
