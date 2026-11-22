"""One jarvis-cd install per site, however many operations find it missing at once.

Operations that find a site without JARVIS at the same time share one site-scoped
approval (its binding names the site and the pin, not one job), and an answer to
it moves on every operation that holds it. Cancelling one leaves the ask to the
others. The bootstrap script takes a lock in the site's jarvis-cd home, so a
second install waits for the first and reuses it
(:data:`~clio_relay.jarvis_bootstrap.BOOTSTRAP_SCRIPT`).
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, cast

from clio_relay import jarvis_submit
from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.records import OperationRecord

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

JARVIS_KIND = "jarvis_bootstrap"


def _install_state(record: OperationRecord) -> dict[str, Any]:
    found = (record.result or {}).get("install")
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def site_lock_id(site_id: str) -> str:
    """The record lock relay holds while it asks to install jarvis-cd on ``site_id``."""
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
