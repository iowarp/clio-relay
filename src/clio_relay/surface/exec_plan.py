"""On a guarded site, a command that is not read-only runs only as an approved plan.

docs/guarded-sites.md: ``exec`` there runs read-only commands. A tool that has
to change the site from its login node, such as preparing a Spack checkout,
sends its exact argument list with a ``plan`` (a kind and a summary). Relay
asks the person once, bound to the site, the plan's kind and the digest of the
argument list, and runs that list only after a yes, lightly
(:func:`clio_relay.login_load.light_argv`) and with a capped timeout. The yes
is used once: the next run of the plan asks again. No raw shell is opened:
the argument list the person approved is the one that runs. The ask is a plain
confirm: it shows only the call's own command, plan and site, plus relay's own
limits on it, so a host that confirms a call the person already allowed asks once.
When the host left the call's confirm to relay (:mod:`clio_relay.surface.confirm`),
the same ask is typed instead, as it is then the only one.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters.hooks import Ask
from clio_relay.login_load import LoginLimits
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import confirm
from clio_relay.wording import with_article

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
    submits: bool = Field(
        default=False,
        description=(
            "The command submits a scheduler job and the work runs there; the login node "
            "only submits it"
        ),
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


def shown_command(command: list[str]) -> str:
    """Return the command as the person reads it, cut at :data:`SHOWN_CHARS`."""
    shown = shlex.join(command)
    if len(shown) > SHOWN_CHARS:
        shown = f"{shown[:SHOWN_CHARS]}... ({len(shown) - SHOWN_CHARS} more characters)"
    return shown


def _message(site: SiteEntry, command: list[str], plan: ExecPlan) -> str:
    shown = shown_command(command)
    if plan.submits:
        question = f"Submit {with_article(plan.kind)} job from {site.title}'s login node?"
        runs = (
            "It submits once, at low priority, for at most "
            f"{PLAN_TIMEOUT_S:.0f} s; the work runs in the scheduler's job, not on the login node."
        )
    else:
        question = f"Run {with_article(plan.kind)} plan on {site.title}'s login node?"
        runs = f"It runs once, at low priority, for at most {PLAN_TIMEOUT_S:.0f} s."
    return (
        f"{question} {plan.summary}\n\n"
        f"The site is guarded, so relay asks before a command that is not read-only. {runs} "
        f"Command (sha256 {argv_digest(command)[:16]}):\n{shown}"
    )


async def approve(
    service: RelayService,
    site: SiteEntry,
    command: list[str],
    plan: ExecPlan,
    *,
    typed: bool = False,
) -> Ask:
    """Return the accepted ask for this exact command, asking the person first when there is none.

    Raises :class:`~clio_relay.surface.files_approval.NeedsPerson` while the ask
    is pending, and refuses with ``policy_refused`` when the person said no.
    ``typed`` makes the ask a choice, for a call whose host left the confirm to relay.
    """
    return await confirm.one_shot(
        service,
        site,
        binding=_binding(site, command, plan),
        message=_message(site, command, plan),
        plain=not typed,
        refused=f"the person did not approve the {plan.kind} plan",
    )


used = confirm.used
"""Mark an approval as spent, so the next run of the plan asks again."""
