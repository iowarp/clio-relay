"""On a guarded site, deleting or replacing a file waits for the person.

docs/guarded-sites.md: ``files delete`` and a ``write`` that would replace an
existing file need the person's yes. Writing a new file does not. The yes is
bound to the site, the op, the path and ``recursive``, and it is used once:
by the delete, or by the last chunk of the upload it approved.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.handles import FileAccess, utc_now
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import canonical_digest
from clio_relay.sites.models import SiteEntry

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

FILES_KIND = "files"


class NeedsPerson(Exception):
    """A files op waits on the person's answer to ``ask``."""

    def __init__(self, ask: Ask) -> None:
        super().__init__(ask.message)
        self.ask = ask


async def approval_action(
    site: SiteEntry,
    access: FileAccess,
    op: str,
    path: str,
    *,
    overwrite: bool = False,
    offset: int = 0,
    final: bool = True,
    recursive: bool = False,
) -> str | None:
    """Say what the person approves before this files op, or None when it needs no yes."""
    if site.policy != "guarded":
        return None
    if op == "delete":
        return f"Delete {path} and everything under it" if recursive else f"Delete {path}"
    if op != "write" or not overwrite or (offset and not final):
        return None
    try:
        entry = await access.stat(path)
    except Refusal as refusal:
        if refusal.reason == RefusalReason.PATH_NOT_FOUND:
            return None
        raise
    size = "" if entry.size is None else f" ({entry.size} bytes)"
    return f"Replace {path}{size}"


def _binding(site: SiteEntry, op: str, path: str, recursive: bool) -> dict[str, object]:
    kind: Literal["delete", "replace"] = "delete" if op == "delete" else "replace"
    return {"kind": FILES_KIND, "site": site.id, "op": kind, "path": path, "recursive": recursive}


async def approve(
    service: RelayService, site: SiteEntry, op: str, path: str, action: str, *, recursive: bool
) -> Ask:
    """Return the accepted ask for this op, asking the person first when there is none.

    Raises :class:`NeedsPerson` while the ask is pending, and refuses with
    ``policy_refused`` when the person said no.
    """
    binding = _binding(site, op, path, recursive)
    digest = canonical_digest(binding)
    found = [
        ask
        for ask in service.asks.all()
        if ask.binding_digest == digest and ask.completed_at is None
    ]
    if found:
        ask = found[0]
    else:
        message = (
            f"{action} on {site.title}? The site is guarded, so relay asks before it "
            "deletes or replaces a file."
        )
        ask = await service.ask_provider.approve(
            site=site, operation_id=None, message=message, binding=binding
        )
    if ask.state == "pending":
        raise NeedsPerson(ask)
    if ask.state != "accepted":
        used(service, ask)
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"the person did not approve: {action}",
            site=site.id,
            path=path,
            ask_id=ask.ask_id,
        )
    return ask


def used(service: RelayService, ask: Ask) -> None:
    """Mark an approval as spent, so the next delete or replace asks again."""
    with service.asks.locked(ask.ask_id):
        current = service.asks.get(ask.ask_id) or ask
        current.completed_at = utc_now()
        service.asks.put(ask.ask_id, current)
