"""The asks the guard's decision and the price add to an operation (RL24, RL25)."""

from __future__ import annotations

from typing import Any

from clio_relay.adapters.hooks import Ask, AskProvider, Charge, GuardDecision
from clio_relay.jobs.records import OperationRecord
from clio_relay.sites.models import SiteEntry


def approval_message(action: str, site: SiteEntry, charge: Charge) -> str:
    """Say what the person approves when the price needs an approval."""
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


async def review_asks(
    provider: AskProvider, site: SiteEntry, record: OperationRecord, decision: GuardDecision
) -> list[Ask]:
    """Turn the guard's package reviews into approve asks.

    Each review in ``decision.data["asks"]`` has a ``message`` and a ``binding``
    (``kind: "package_review"``, ``package``, ``version``, ``package_hash`` and
    ``source``). The ask binds it to the site and the spec the person sees, so
    ``advance`` checks it like any approval.
    """
    reviews: list[dict[str, Any]] = decision.data.get("asks", [])
    return [
        await provider.approve(
            site=site,
            operation_id=record.operation_id,
            message=review["message"],
            binding={**review["binding"], "site": site.id, "spec_digest": record.spec_digest},
        )
        for review in reviews
    ]
