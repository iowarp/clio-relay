"""The asks the guard's decision and the price add to an operation (RL24, RL25)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from clio_relay.adapters.hooks import Ask, AskProvider, Charge, GuardDecision
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.spec import PackageRef
from clio_relay.sites.models import SiteEntry


def _duration(seconds: float) -> str:
    """Say a walltime in the largest whole unit, such as ``2 h`` or ``90 min``."""
    for size, unit in ((3600, "h"), (60, "min")):
        if seconds >= size and seconds % size == 0:
            return f"{seconds / size:g} {unit}"
    if seconds >= 60:
        return f"{seconds / 60:.4g} min"
    return f"{seconds:g} s"


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def _step_lines(steps: Sequence[PackageRef]) -> list[str]:
    """Say which of the allocation's nodes each step of a pipeline gets."""
    if len(steps) < 2:
        return []
    lines = ["- Pipeline, one allocation:"]
    for number, step in enumerate(steps, start=1):
        nodes = step.nodes()
        where = f"nodes {nodes}" if nodes is not None else "all the nodes"
        lines.append(f"  {number}. {step.name} on {where}")
    return lines


def approval_message(
    action: str, site: SiteEntry, charge: Charge, steps: Sequence[PackageRef] = ()
) -> str:
    """Say what the person approves when the price needs an approval.

    The first line is the question. Three short lines follow with what the job
    asks the scheduler for (nodes and ranks, walltime and node-hours, queue),
    then why relay asks and where the person changes that. A pipeline's steps
    share the allocation; a list after the nodes line says which nodes
    (``host_slice``) each gets. It is plain Markdown, kept short so a client's
    question card shows it without scrolling.
    """
    known = charge.walltime_s is not None and charge.node_hours is not None
    if known:
        title = f"{action} on {site.title}? It may use up to {charge.node_hours:g} {charge.unit}."
    else:
        title = f"{action} on {site.title}? It sets no walltime, so its cost is unknown."
    nodes = f"- Nodes: {charge.nodes}"
    if charge.ranks_per_node is not None:
        nodes += f", {_plural(charge.ranks_per_node, 'rank')} per node"
        if charge.ranks is not None and charge.ranks != charge.ranks_per_node:
            nodes += f" ({charge.ranks} in all)"
    elif charge.ranks is not None:
        nodes += f", {_plural(charge.ranks, 'rank')} in all"
    if charge.walltime_s is None:
        walltime = "- Walltime: none set, so the scheduler's default limit applies"
    else:
        walltime = f"- Walltime: {_duration(charge.walltime_s)}"
        if charge.node_hours is not None:
            hours = f"{charge.node_hours:g} node-hour{'' if charge.node_hours == 1 else 's'}"
            walltime += f", up to {hours} in all"
    if charge.amount is not None and charge.unit != "node-hours":
        walltime += f"; up to {charge.amount:g} {charge.unit}"
    queue = f"- Queue: {charge.queue or 'the scheduler default'}"
    if charge.account:
        queue += f", account {charge.account}"
    ask = site.spend.ask
    if isinstance(ask, str) or not known:
        why = (
            "Relay asks before every job here."
            if ask == "always"
            else "Relay asks when it cannot tell a job's cost."
        )
    else:
        why = f"Relay asks above {ask:g} node-hours here."
    change = "Change this under Spending on the Compute page."
    return "\n".join(
        [title, "", nodes, *_step_lines(steps), walltime, queue, "", f"{why} {change}"]
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
