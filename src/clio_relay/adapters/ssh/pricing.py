"""The spend price of an ``ssh`` job (RL25), in node-hours."""

from __future__ import annotations

from clio_relay.adapters.hooks import Charge
from clio_relay.jobs.spec import JobSpec


def charge(site_id: str, spec: JobSpec) -> Charge:
    """Return the most ``spec`` may use: nodes times walltime.

    With no walltime the cost is unknown: the charge has no amount and no
    node-hours, so the spend gate asks unless the site never asks.
    """
    found = Charge(
        site=site_id,
        queue=spec.attributes.queue_name,
        account=spec.attributes.account,
        nodes=spec.resources.nodes(),
    )
    duration = spec.attributes.duration
    node_hours = spec.node_hours()
    if duration is None or node_hours is None:
        return found
    node_hours = round(node_hours, 4)
    return found.model_copy(
        update={
            "walltime_s": duration.total_seconds(),
            "node_hours": node_hours,
            "amount": node_hours,
        }
    )
