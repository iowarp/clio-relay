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
  input changed since (:data:`PLAN_CHANGED`). Relay plans again and compares the
  plans as the person sees them (:func:`changes`): it asks again, saying what
  changed, only when something they see did.
- Every other package submit plans in the same call and is bound to that plan,
  so a running job's card shows what JARVIS submitted (:func:`parse`).

JARVIS writes each component's state to ``components.jsonl`` in the execution
(``jarvis.job.v1`` lines); :func:`component_states` folds them, the first
terminal line of a component winning.
"""

from __future__ import annotations

import difflib
import json
import re
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
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")
_FIELD_NAME = re.compile(r"\bdetails\.[a-z_]+")
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
    "input_sha256",
)
_SHOWN = (
    ("action", "Action"),
    ("scheduler", "Scheduler"),
    ("partition", "Partition"),
    ("account", "Account"),
    ("qos", "QOS"),
    ("time", "Walltime"),
    ("nodes", "Nodes"),
    ("job_name", "Job name"),
)
"""The directives of a plan the card shows, with the name the person reads."""
_COMPONENT_SHOWN = (
    ("label", "name"),
    ("package", "package"),
    ("role", "role"),
    ("starts", "start"),
    ("after_steps", "start order"),
    ("nodes", "nodes"),
    ("node_range", "node range"),
    ("host_slice", "hosts"),
    ("ranks", "ranks"),
    ("ports", "ports"),
    ("readiness", "readiness"),
)
_SCRIPT_LINES = 4
"""The most changed batch-script lines one change list quotes."""


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


def script_sha(plan: Mapping[str, Any]) -> str | None:
    """The sha256 of the batch script ``plan`` carries, if any."""
    script = plan.get("script")
    found = cast(dict[str, Any], script).get("sha256") if isinstance(script, dict) else None
    return found if isinstance(found, str) else None


def refused_digests(refusal: Refusal) -> dict[str, Any]:
    """The digests of JARVIS's :data:`PLAN_CHANGED` issue: the sha the yes named and the submit's.

    ``expected_plan_sha256`` is the plan the yes bound; ``plan_sha256``,
    ``script_sha256`` and ``input_sha256`` are what the submit rendered.
    """
    for issue in _listed(refusal.data.get("issues")):
        if isinstance(issue, dict) and cast(dict[str, Any], issue).get("kind") == PLAN_CHANGED:
            details = cast(dict[str, Any], issue).get("details")
            if isinstance(details, dict):
                return {
                    key: value
                    for key, value in cast(dict[str, Any], details).items()
                    if key.endswith("_sha256") and isinstance(value, str)
                }
    return {}


def changes(old: Mapping[str, Any], new: Mapping[str, Any]) -> list[str]:
    """What the person would see differ between two of JARVIS's plans of one job, a line each.

    The directives, each component, the couplings, a note JARVIS added and the
    batch script count. Empty when the plans differ only in JARVIS's sealed
    input (``input_sha256``: the pipeline relay rendered from the approved spec,
    which the yes fixed, and the environment JARVIS loads, which a software
    check it ran since can fill in) or in a note JARVIS dropped. A changed batch
    script's quoted lines follow its line, each indented by two spaces.
    """
    lines = [
        f"{name}: {_shown(old.get(key))}, now {_shown(new.get(key))}"
        for key, name in _SHOWN
        if old.get(key) != new.get(key)
    ]
    lines += _component_changes(old, new)
    if (old.get("couplings") or []) != (new.get("couplings") or []):
        lines.append("The couplings between the components changed")
    before = {a["message"] for a in advisories(old) if a.get("message")}
    lines += [
        f"JARVIS now notes: {a['message']}"
        for a in advisories(new)
        if a.get("message") and a["message"] not in before
    ]
    return lines + _script_changes(old.get("script"), new.get("script"))


def _shown(value: object) -> str:
    if value is None or value == "":
        return "the site default"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _component_changes(old: Mapping[str, Any], new: Mapping[str, Any]) -> list[str]:
    def keyed(plan: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        for index, item in enumerate(_listed(plan.get("components"))):
            if isinstance(item, dict):
                component = cast(dict[str, Any], item)
                found[str(component.get("id") or index)] = component
        return found

    before, after = keyed(old), keyed(new)
    lines: list[str] = []
    for key, component in after.items():
        label = component.get("label") or key
        was = before.get(key)
        if was is None:
            lines.append(f"New component: {label} on {_nodes(component.get('nodes'))}")
            continue
        lines += [
            f"{label} {name}: {_shown(was.get(field))}, now {_shown(component.get(field))}"
            for field, name in _COMPONENT_SHOWN
            if was.get(field) != component.get(field)
        ]
    lines += [
        f"Component gone: {component.get('label') or key}"
        for key, component in before.items()
        if key not in after
    ]
    return lines


def _script_changes(old: object, new: object) -> list[str]:
    was = cast(dict[str, Any], old) if isinstance(old, dict) else {}
    now = cast(dict[str, Any], new) if isinstance(new, dict) else {}
    if was.get("sha256") == now.get("sha256"):
        return []
    line = (
        f"The batch script changed (sha256 {str(was.get('sha256'))[:12]}, "
        f"now {str(now.get('sha256'))[:12]})"
    )
    texts = was.get("text"), now.get("text")
    if not all(isinstance(text, str) for text in texts):
        return [line]
    diff = difflib.unified_diff(
        str(texts[0]).splitlines(), str(texts[1]).splitlines(), lineterm="", n=0
    )
    quoted = [
        f"  {'was' if text[0] == '-' else 'now'}: {text[1:].strip()[:160]}"
        for text in diff
        if text[:1] in "+-" and not text.startswith(("+++", "---"))
    ]
    more = len(quoted) - _SCRIPT_LINES
    tail = [f"  and {more} more changed line{'s' if more > 1 else ''}"] if more > 0 else []
    return [line, *quoted[:_SCRIPT_LINES], *tail]


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


def _advisory(item: object) -> dict[str, Any] | None:
    if isinstance(item, str):
        return _advisory({"message": item}) if item.strip() else None
    if not isinstance(item, dict):
        return None
    issue = cast(dict[str, Any], item)
    details = issue.get("details")
    details = cast(dict[str, Any], details) if isinstance(details, dict) else {}
    message = issue.get("message") or details.get("message")
    if not message and not issue.get("issue_id"):
        return None
    pick = details.get("pick")
    pick = cast(dict[str, Any], pick) if isinstance(pick, dict) else {}
    line, guidance = _split_message(issue, details, message)
    settled = _settled_at_launch(details)
    probes = details.get("probes")
    return {
        "issue_id": issue.get("issue_id"),
        "kind": issue.get("kind"),
        "subject": issue.get("subject"),
        "fact": details.get("fact") or issue.get("field"),
        "reason_codes": issue.get("reason_codes") or [],
        "message": line,
        "pick": {key: value for key, value in pick.items() if key != "consequence_if_wrong"}
        or None,
        "consequence_if_wrong": pick.get("consequence_if_wrong") or [],
        "remedy": issue.get("remedy"),
        "agent_guidance": guidance,
        "probes": list(cast(list[Any], probes)) if isinstance(probes, list) else [],
        "settled_at_launch": settled,
    }


def _settled_at_launch(details: Mapping[str, Any]) -> bool:
    """Whether JARVIS settles every reason of an issue at launch (``plan: collect_at_launch``).

    At check time JARVIS does not know the job's hosts, so a job larger than the
    graph's sample is told as ``hosts_not_in_graph``; the launch then collects
    the facts on its own hosts, merges them into the site graph and supersedes
    the advisory (``metadata.site_facts.superseded_by_launch``).
    """
    reasons = details.get("reasons")
    if not isinstance(reasons, list) or not reasons:
        return False
    return all(
        isinstance(reason, dict) and cast(dict[str, Any], reason).get("plan") == "collect_at_launch"
        for reason in cast(list[object], reasons)
    )


def _split_message(
    issue: Mapping[str, Any], details: Mapping[str, Any], message: object
) -> tuple[str | None, str | None]:
    """Split an issue's message into the person's line and the agent's guidance.

    JARVIS may give both (``person_message`` and ``agent_guidance``). Otherwise a
    sentence that names one of the issue's own fields (``details.probes``) is
    guidance for the agent, which can read those fields; the person sees the rest.
    """
    text = str(message) if message else ""
    person = issue.get("person_message") or details.get("person_message")
    guidance = issue.get("agent_guidance") or details.get("agent_guidance")
    if person:
        return str(person), str(guidance) if guidance else None
    sentences = _SENTENCE_END.split(text.strip()) if text.strip() else []
    kept = [s for s in sentences if not _FIELD_NAME.search(s)]
    agent = [s for s in sentences if _FIELD_NAME.search(s)]
    if guidance:
        agent.append(str(guidance))
    line = " ".join(kept) or None
    if line and agent and line[-1] not in ".!?":
        line += "."
    return line, " ".join(agent) or None


def _ids(found: object) -> set[str]:
    ids: set[str] = set()
    for item in _listed(found):
        if isinstance(item, dict):
            item = cast(dict[str, Any], item).get("issue_id")
        if isinstance(item, str):
            ids.add(item)
    return ids


def _listed(found: object) -> list[object]:
    return list(cast(list[object], found)) if isinstance(found, list) else []


def advisories(
    plan: Mapping[str, Any] | None,
    site_facts: Mapping[str, Any] | None = None,
    *,
    settled: bool = False,
) -> list[dict[str, Any]]:
    """JARVIS's warnings on the job, once each: the picks it went ahead with.

    They come from the plan's ``warnings[]`` and, once submitted, the record's
    ``metadata.site_facts`` ``advisories[]`` (less the ones the person
    confirmed or a launch's own evaluation replaced), such as a
    ``site_fact_uncertain`` saying JARVIS launches over TCP because it could not
    verify UCX. Each is a proposal the person sees and can answer later
    (``jarvis rg facts answer``, or ``rg net set``); none holds the job.

    An advisory JARVIS settles itself at launch (``settled_at_launch``) asks
    nothing of the person, so it is not among them: ``settled=True`` returns
    those instead, for the agent.
    """
    facts = site_facts or {}
    dropped = _ids(facts.get("confirmed")) | _ids(facts.get("superseded_by_launch"))
    items = _listed(facts.get("advisories")) if facts else _listed((plan or {}).get("warnings"))
    found: dict[str, dict[str, Any]] = {}
    for item in items:
        advisory = _advisory(item)
        if advisory is None or advisory.get("issue_id") in dropped:
            continue
        if advisory["settled_at_launch"] != settled:
            continue
        found.setdefault(str(advisory.get("issue_id") or advisory["message"]), advisory)
    return list(found.values())


def summary(plan: Mapping[str, Any]) -> str:
    """The plan in one line, for a host that shows only the ask's text.

    Each of JARVIS's warnings follows on a line of its own.
    """
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
    labels = {str(c.get("id")): str(c.get("label") or c.get("id")) for c in components}
    flows = "; ".join(
        f"{labels.get(str(e.get('from')), e.get('from'))} feeds "
        f"{labels.get(str(e.get('to')), e.get('to'))} ({e.get('label') or e.get('kind')})"
        for e in cast(list[dict[str, Any]], plan.get("couplings") or [])
        if e.get("kind") != "interceptor"
    )
    line = "JARVIS's plan: " + ", ".join(parts)
    line = f"{line}; starts {shown}" if shown else line
    line = f"{line}; {flows}" if flows else line
    notes = [f"JARVIS notes: {a['message']}" for a in advisories(plan) if a.get("message")]
    return "\n".join([line, *notes])
