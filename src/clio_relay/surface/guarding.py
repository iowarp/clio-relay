"""The asks the guard's decision and the price add to an operation (RL24, RL25)."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from clio_relay import jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import Ask, Charge, GuardDecision, charge_steps
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import Layout, PackageRef
from clio_relay.sites.models import SiteEntry

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService


def _duration(seconds: float) -> str:
    """Say a walltime as a person reads it, such as ``2 h``, ``1 h 30 min`` or ``10 min``."""
    if seconds < 60:
        return f"{seconds:g} s"
    minutes = round(seconds / 60)
    if minutes < 60:
        return f"{minutes} min"
    hours, rest = divmod(minutes, 60)
    return f"{hours} h" if rest == 0 else f"{hours} h {rest} min"


def node_hours_text(value: float) -> str:
    """Say a node-hour count the way a person reads it: ``1 node-hour``, ``about 0.17 node-hours``.

    Two significant digits under one, one decimal under ten, whole numbers above;
    a rounded count says "about".
    """
    if 0 < value < 0.01:
        return "under 0.01 node-hours"
    if value >= 10:
        shown = f"{round(value):d}"
    elif value >= 1:
        shown = f"{round(value, 1):g}"
    else:
        shown = f"{value:.2g}"
    about = "" if math.isclose(float(shown), value, rel_tol=1e-9) else "about "
    return f"{about}{shown} node-hour{'' if shown == '1' else 's'}"


def cost_text(charge: Charge) -> str | None:
    """Say a job's most use in words: ``up to 10 min on 1 node (about 0.17 node-hours)``."""
    if charge.walltime_s is None or charge.node_hours is None:
        return None
    nodes = _plural(charge.nodes, "node")
    return f"up to {_duration(charge.walltime_s)} on {nodes} ({node_hours_text(charge.node_hours)})"


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def _step_lines(charge: Charge, steps: Sequence[PackageRef]) -> list[str]:
    """Say which of the allocation's nodes each step of a pipeline gets."""
    shares = charge.steps or (charge_steps(steps, charge.nodes) if len(steps) >= 2 else [])
    if len(shares) < 2:
        return []
    lines = ["- Pipeline, one allocation:"]
    for number, share in enumerate(shares, start=1):
        if share.host_slice is None:
            where = "all the nodes"
        else:
            count = "" if share.nodes is None else f", {_plural(share.nodes, 'node')}"
            where = f"nodes {share.host_slice}{count}"
        lines.append(f"  {number}. {share.package} on {where}")
    return lines


def _layout(layout: Layout) -> str:
    parts = [_plural(layout.nodes, "node")]
    if layout.ranks_per_node is not None:
        parts.append(f"{_plural(layout.ranks_per_node, 'rank')} per node")
    if layout.threads_per_rank is not None:
        parts.append(f"{_plural(layout.threads_per_rank, 'thread')} per rank")
    if layout.walltime_s is not None:
        parts.append(_duration(layout.walltime_s))
    if layout.queue:
        parts.append(f"queue {layout.queue}")
    text = ", ".join(parts)
    return f"{text} ({layout.note})" if layout.note else text


def _sizing_lines(charge: Charge) -> list[str]:
    """Say why the job asks for its layout, and what else would work."""
    lines: list[str] = []
    if charge.rationale:
        lines.append(f"- Why: {charge.rationale}")
    if charge.alternatives:
        lines.append("- Alternatives:")
        lines.extend(f"  - {_layout(layout)}" for layout in charge.alternatives)
    return lines


def approval_message(
    action: str, site: SiteEntry, charge: Charge, steps: Sequence[PackageRef] = ()
) -> str:
    """Say what the person approves when the price needs an approval.

    The first line is the question. Short lines follow with what the job asks
    the scheduler for (nodes, ranks and threads, memory, walltime and
    node-hours, queue). A pipeline's steps share the allocation; a list after
    the nodes line says which nodes (``host_slice``) each gets. When the job
    says why it asks for that layout, a "Why" line and its alternatives follow.
    Then why relay asks and where the person changes that. It is plain
    Markdown, kept short so a client's question card shows it without
    scrolling.
    """
    known = (cost := cost_text(charge)) is not None
    if known:
        title = f"{action} on {site.title}? It may use {cost}."
    else:
        title = f"{action} on {site.title}? It sets no walltime, so its cost is unknown."
    nodes = f"- Nodes: {charge.nodes}"
    if charge.ranks_per_node is not None:
        nodes += f", {_plural(charge.ranks_per_node, 'rank')} per node"
        if charge.ranks is not None and charge.ranks != charge.ranks_per_node:
            nodes += f" ({charge.ranks} in all)"
    elif charge.ranks is not None:
        nodes += f", {_plural(charge.ranks, 'rank')} in all"
    if charge.threads_per_rank is not None:
        nodes += f", {_plural(charge.threads_per_rank, 'thread')} per rank"
    if charge.memory_mb is not None:
        nodes += f", {charge.memory_mb:g} MB per node"
    if charge.walltime_s is None:
        walltime = "- Walltime: none set, so the scheduler's default limit applies"
    else:
        walltime = f"- Walltime: {_duration(charge.walltime_s)}"
        if charge.node_hours is not None:
            walltime += f", up to {node_hours_text(charge.node_hours)} in all"
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
        [
            title,
            "",
            nodes,
            *_step_lines(charge, steps),
            walltime,
            queue,
            *_sizing_lines(charge),
            "",
            f"{why} {change}",
        ]
    )


def bootstraps_jarvis(service: RelayService, site: SiteEntry, adapter: str) -> bool:
    """Say whether relay would install its own jarvis-cd for ``adapter`` on ``site``.

    It does when the adapter has the hook and the site's settings name no
    jarvis-cd of the site's own.
    """
    try:
        found = service.adapter_named(site, adapter)
    except Refusal:
        return False
    hook = "install_jarvis"
    if getattr(type(found), hook, None) is getattr(BaseAdapter, hook):
        return False
    return not getattr(found.config, "jarvis", "x")


def jarvis_line(site: SiteEntry) -> str:
    """The sentence a package review adds when its yes also covers relay's jarvis-cd."""
    return (
        f"If {site.title} has no jarvis-cd of relay's yet, a yes also lets relay install "
        f"jarvis-cd {jarvis_submit.JARVIS_PIN[:8]} in a virtual environment under the CLIO "
        "home there, which needs the site to reach github.com."
    )


async def review_asks(
    service: RelayService, site: SiteEntry, record: OperationRecord, decision: GuardDecision
) -> list[Ask]:
    """Turn the guard's package reviews into approve asks.

    Each review in ``decision.data["asks"]`` has a ``message`` and a ``binding``
    (``kind: "package_review"``, ``package``, ``version``, ``package_hash`` and
    ``source``). The ask binds it to the site and the spec the person sees, so
    ``advance`` checks it like any approval. Where relay would install its own
    jarvis-cd, the review says so and binds the pin: the person answers one
    question, not a second one about jarvis-cd once the submit finds none.
    """
    reviews: list[dict[str, Any]] = decision.data.get("asks", [])
    jarvis = bool(reviews) and bootstraps_jarvis(service, site, record.adapter)
    extra: dict[str, Any] = {"jarvis": jarvis_submit.JARVIS_PIN} if jarvis else {}
    return [
        await service.ask_provider.approve(
            site=site,
            operation_id=record.operation_id,
            message=review["message"] + ("\n" + jarvis_line(site) if jarvis else ""),
            binding={
                **review["binding"],
                **extra,
                "site": site.id,
                "spec_digest": record.spec_digest,
            },
            plain=False,
        )
        for review in reviews
    ]
