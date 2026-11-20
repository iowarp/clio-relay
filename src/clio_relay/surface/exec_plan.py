"""On a guarded site, a command that is not read-only runs only as an approved plan.

docs/guarded-sites.md: ``exec`` there runs read-only commands. A tool that has
to change the site from its login node, such as preparing a Spack checkout,
sends its exact argument list with a ``plan`` (a kind and a summary). Relay
asks the person once, bound to the site, the plan's kind and the digest of the
argument list, and runs that list only after a yes, lightly
(:func:`clio_relay.login_load.light_argv`) and with a capped timeout. The yes
is used once: the next run of the plan asks again. No raw shell is opened:
the argument list the person approved is the one that runs.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import canonical_digest
from clio_relay.login_load import LoginLimits
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.files_approval import NeedsPerson

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

EXEC_PLAN_KIND = "exec_plan"
PLAN_TIMEOUT_S = 900.0
"""The longest an approved plan runs on a login node."""
SHOWN_CHARS = 4000
"""How much of the command the question shows; the digest covers all of it."""
PLAN_LIMITS = LoginLimits(cpu_quota_percent=100, memory_mb=4096, address_space_mb=16384, cpu_s=900)
"""What an approved plan or a Spack query may use on a login node. Spack's
concretizer needs more memory than relay's own long-lived processes."""


class ExecPlan(BaseModel):
    """What a command that changes a guarded site is for."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(
        min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9_.-]*$", description="e.g. spack"
    )
    summary: str = Field(
        min_length=1, max_length=2000, description="What the command does, for the person"
    )


def argv_digest(command: list[str]) -> str:
    """Return the sha256 of the argument list, as canonical JSON."""
    return hashlib.sha256(json.dumps(command, separators=(",", ":")).encode()).hexdigest()


def _binding(site: SiteEntry, command: list[str], plan: ExecPlan) -> dict[str, object]:
    return {
        "kind": EXEC_PLAN_KIND,
        "site": site.id,
        "plan_kind": plan.kind,
        "argv_sha256": argv_digest(command),
    }


def _message(site: SiteEntry, command: list[str], plan: ExecPlan) -> str:
    shown = shlex.join(command)
    if len(shown) > SHOWN_CHARS:
        shown = f"{shown[:SHOWN_CHARS]}... ({len(shown) - SHOWN_CHARS} more characters)"
    return (
        f"Run a {plan.kind} plan on {site.title}'s login node? {plan.summary}\n\n"
        f"The site is guarded, so relay asks before a command that is not read-only. It runs "
        f"once, at low priority, for at most {PLAN_TIMEOUT_S:.0f} s. Command (sha256 "
        f"{argv_digest(command)[:16]}):\n{shown}"
    )


async def approve(
    service: RelayService, site: SiteEntry, command: list[str], plan: ExecPlan
) -> Ask:
    """Return the accepted ask for this exact command, asking the person first when there is none.

    Raises :class:`NeedsPerson` while the ask is pending, and refuses with
    ``policy_refused`` when the person said no.
    """
    binding = _binding(site, command, plan)
    digest = canonical_digest(binding)
    found = [
        ask
        for ask in service.asks.all()
        if ask.binding_digest == digest and ask.completed_at is None
    ]
    if found:
        ask = found[0]
    else:
        ask = await service.ask_provider.approve(
            site=site, operation_id=None, message=_message(site, command, plan), binding=binding
        )
    if ask.state == "pending":
        raise NeedsPerson(ask)
    if ask.state != "accepted":
        used(service, ask)
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"the person did not approve the {plan.kind} plan",
            site=site.id,
            ask_id=ask.ask_id,
        )
    return ask


def used(service: RelayService, ask: Ask) -> None:
    """Mark an approval as spent, so the next run of the plan asks again."""
    with service.asks.locked(ask.ask_id):
        current = service.asks.get(ask.ask_id) or ask
        current.completed_at = utc_now()
        service.asks.put(ask.ask_id, current)
