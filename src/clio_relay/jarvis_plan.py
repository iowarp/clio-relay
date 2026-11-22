"""JARVIS's plan of a package job: what the job card shows, and what a yes binds.

JARVIS plans a submission without making it (``jarvis ppl submit +plan +json``,
one ``jarvis.submit-plan.v1`` document): the scheduler directives, each package
as a component with its role, node count and host slice, the couplings between
them and the batch script, sealed by ``plan_sha256``. It runs no Spack, submits
nothing and writes no record. Relay shows that plan on the job's card and never
reads the script for it.

- Before the one ask of a ``run`` whose host left the confirm to relay, the
  adapter plans the job (:meth:`~clio_relay.adapters.base.BaseAdapter.plan_job`,
  the site script's ``plan`` mode, which exits :data:`EXIT_PLANNED`), so the card
  carries JARVIS's plan. The person's yes binds the submit to that plan
  (:func:`bound`, ``--plan_sha256``); JARVIS refuses a submit whose script or
  input changed since (:data:`PLAN_CHANGED`), and relay asks again with the new plan.
- Every other package submit plans in the same call and is bound to that plan,
  so a running job's card shows what JARVIS submitted (:func:`parse`).

JARVIS writes each component's state to ``components.jsonl`` in the execution
(``jarvis.job.v1`` lines); :func:`component_states` folds them, the first
terminal line of a component winning.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from clio_relay import jarvis_submit
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import JobSpec

PLAN_SCHEMA = "jarvis.submit-plan.v1"
STATE_SCHEMA = "jarvis.job.v1"
PLAN_FILE = "relay.plan.json"
"""The plan the site script keeps in the job directory, and prints before a submit."""
STATES_FILE = "components.jsonl"
"""JARVIS's component states, in the execution directory."""
BIND_ENV = "CLIO_RELAY_PLAN"
"""``sha256:<plan_sha256>``: the plan the person said yes to, which the submit must match.

The site script reads it and unsets it before JARVIS starts, so it never
reaches the environment JARVIS seals."""
EXIT_PLANNED = 10
"""The site script's ``plan`` mode made a plan, and submitted nothing."""
PLAN_CHANGED = "submission_plan_changed"
"""JARVIS's issue kind when the script or the input differ from the plan the yes named."""
KEY = "plan"
"""Where a record's result, and a JARVIS job handle's data, keep the plan."""
TERMINAL = frozenset({"done", "failed", "cancelled"})
SCRIPT_LIMIT = 256 * 1024
"""The most of the batch script a plan keeps; a longer one keeps its sha and path."""

_KEPT = (
    "plan_sha256",
    "action",
    "scheduler",
    "partition",
    "account",
    "qos",
    "time",
    "time_limit_s",
    "job_name",
    "nodes",
    "sources",
    "components",
    "couplings",
    "execution_id",
    "component_states",
    "warnings",
    "exit_code",
)


def read[T](path: Path, reader: Callable[[str], T]) -> T | None:
    """``reader`` of the file at ``path`` on this machine, or ``None`` when there is none."""
    try:
        return reader(path.read_text("utf-8"))
    except OSError:
        return None


def parse(text: str) -> dict[str, Any] | None:
    """Return the part of the plan JARVIS printed in ``text`` that relay keeps, or ``None``."""
    found = jarvis_submit.parse_document(text, PLAN_SCHEMA)
    if found is None or not isinstance(found.get("plan_sha256"), str):
        return None
    kept = {key: found[key] for key in _KEPT if key in found}
    script = found.get("script")
    if isinstance(script, dict):
        script = cast(dict[str, Any], script)
        text_ = script.get("text")
        kept["script"] = {
            "sha256": script.get("sha256"),
            "path": script.get("path"),
            "text": text_ if isinstance(text_, str) and len(text_) <= SCRIPT_LIMIT else None,
        }
    return kept


def planned(exit_code: int | str | None, output: str) -> dict[str, Any] | None:
    """Return the plan a site script's ``plan`` mode made, or ``None`` when it made none."""
    if str(exit_code) != str(EXIT_PLANNED):
        return None
    return parse(output)


def bound(spec: JobSpec, plan: Mapping[str, Any] | None) -> JobSpec:
    """Return ``spec`` bound to ``plan``: JARVIS submits only that plan."""
    sha = plan.get("plan_sha256") if plan else None
    if not isinstance(sha, str) or not sha:
        return spec
    environment = {**spec.environment, BIND_ENV: f"sha256:{sha}"}
    return spec.model_copy(update={"environment": environment})


def changed(refusal: Refusal) -> bool:
    """Say whether JARVIS refused the submit because its plan changed since the yes."""
    kinds = refusal.data.get("kinds")
    return isinstance(kinds, list) and PLAN_CHANGED in kinds


def component_states(text: str) -> list[dict[str, Any]]:
    """Fold JARVIS's ``jarvis.job.v1`` lines into each component's latest state, in order.

    A component's first terminal line wins, as JARVIS's own rule says.
    """
    folded: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if not isinstance(found, dict):
            continue
        entry = cast(dict[str, Any], found)
        component = entry.get("component")
        if entry.get("schema_version") != STATE_SCHEMA or not isinstance(component, str):
            continue
        if folded.get(component, {}).get("state") in TERMINAL:
            continue
        hosts = entry.get("hosts")
        folded[component] = {
            "component": component,
            "package": entry.get("package"),
            "state": entry.get("state"),
            "hosts": [str(h) for h in cast(list[Any], hosts)] if isinstance(hosts, list) else None,
            "at": entry.get("at"),
            "reason": entry.get("reason"),
        }
    return list(folded.values())


def role(plan: Mapping[str, Any], component: Mapping[str, Any]) -> str:
    """Return the card's role for a JARVIS component: compute, service or install.

    JARVIS's roles are ``batch`` and ``service`` (``ppl check``'s); its install
    job is one ``batch`` component.
    """
    if str(plan.get("action") or "").startswith("install"):
        return "install"
    found = component.get("role")
    return "service" if found == "service" else "compute" if found == "batch" else str(found)


def _nodes(count: object) -> str:
    if not isinstance(count, int):
        return "its nodes"
    return "1 node" if count == 1 else f"{count} nodes"


def summary(plan: Mapping[str, Any]) -> str:
    """The plan in one line, for a host that shows only the ask's text."""
    parts = [
        f"partition {plan.get('partition') or 'the site default'}",
        f"account {plan.get('account') or 'the site default'}",
    ]
    if plan.get("time"):
        parts.append(f"time {plan['time']}")
    components = cast(list[dict[str, Any]], plan.get("components") or [])
    shown = ", then ".join(
        f"{c.get('label') or c.get('id')} on {_nodes(c.get('nodes'))}" for c in components
    )
    line = "JARVIS's plan: " + ", ".join(parts)
    return f"{line}; {shown}" if shown else line
