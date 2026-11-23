"""A job as one typed snapshot: what a host draws as the job's card.

The snapshot is plain JSON at :data:`JOB_META`. Relay sends the whole of it each
time, never a patch, and ``revision`` rises with every change of the operation,
so a host keeps the newest one per ``operation_id``. It rides as optional data:

- on the ask that confirms a submit, with phase ``planned``: in the
  elicitation's ``params._meta`` on an older connection, and on the
  ``input_required`` result's own ``_meta`` on a ``2026-07-28`` one, whose
  embedded requests carry no ``_meta``;
- in the result of ``run``, ``serve`` and ``describe(job=...)`` as
  ``structuredContent.job``, and ``describe(site=...)`` lists the site's jobs
  as ``jobs``;
- in each task status a task-shaped ``run`` or ``serve`` reports, in its ``_meta``;
- as the resource :data:`JOB_URI`. A ``serve`` task ends at readiness, so a
  host that keeps a service's card follows it there: relay looks at every open
  job itself and announces the resource updated when its card changes
  (``notifications/resources/updated``, or the ``subscriptions/listen`` stream).

The text of each message stays the same, so a host that ignores the snapshot
loses nothing. ``source`` says where the plan came from:

- ``jarvis`` for a package job JARVIS planned (:mod:`clio_relay.jarvis_plan`):
  the directives, components, couplings and script are JARVIS's plan, and each
  component's state and hosts are the latest line JARVIS wrote for it in
  ``components.jsonl``, or follow the job's state until JARVIS writes one;
- ``relay`` otherwise, from the job as relay records it, with no couplings and
  no script.

:data:`CONFIRM_META` is the org-level key for a tool that confirms its own side
effects with one typed ask. A tool's ``_meta`` declares it; a host that skipped
its own ask for the call says so in the request's ``params._meta`` with
:data:`CONFIRM_SERVER`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, cast

from fastmcp import Context
from fastmcp_tasks.models import GetTaskResult
from pydantic import Field

from clio_relay import jarvis_plan, jarvis_submit
from clio_relay.adapters.hooks import Charge
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef, ServiceSpec
from clio_relay.surface.endpoints import service_ready

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

JOB_META = "io.iowarp.clio-relay/job"
CONFIRM_META = "io.iowarp/confirm"
CONFIRM_TOOL: dict[str, Any] = {"version": 1}
CONFIRM_SERVER = "server"
CONFIRM_NOT_NEEDED = "not_needed"
"""On a result: the call acted only on answers given before it, or only observed."""
VERSION = 1
LISTED_ENDED = 20
"""How many ended jobs of a site ``describe(site=...)`` lists, newest first."""
JOB_URI = "relay://jobs/{operation_id}"
"""A resource holding one job's newest card; relay announces it updated on each change."""


def job_uri(operation_id: str) -> str:
    """Return the resource URI of one job's card."""
    return JOB_URI.format(operation_id=operation_id)


class JobTaskResult(GetTaskResult):
    """A task status with relay's job snapshot in its ``_meta``."""

    meta: dict[str, Any] | None = Field(default=None, alias="_meta")


def flagged(ctx: Context) -> bool:
    """Say whether the host left this call's confirm to relay."""
    rc = ctx.request_context
    meta = getattr(rc, "meta", None) if rc is not None else None
    if not isinstance(meta, dict):
        return False
    return cast(dict[str, Any], meta).get(CONFIRM_META) == CONFIRM_SERVER


def not_needed(meta: dict[str, Any] | None) -> dict[str, Any]:
    """Return a result's ``_meta`` saying this call raised no confirm and needed none."""
    return {**(meta or {}), CONFIRM_META: CONFIRM_NOT_NEEDED}


def _phase(record: OperationRecord) -> str:
    return {
        OperationState.AWAITING_INPUT: "planned",
        OperationState.SUBMITTING: "submitted",
        OperationState.QUEUED: "queued",
        OperationState.RUNNING: "running",
    }.get(record.state, "ended")


def _outcome(record: OperationRecord) -> str | None:
    if record.state == OperationState.COMPLETED:
        return "succeeded" if record.exit_code in (0, None) else "failed"
    if record.state == OperationState.FAILED:
        return "failed"
    if record.state == OperationState.CANCELLED:
        return "cancelled"
    return None


def _state(record: OperationRecord, outcome: str | None, *, service: bool) -> str:
    phase = _phase(record)
    if phase == "planned":
        return "planned"
    if phase in ("submitted", "queued"):
        return "waiting"
    if phase == "running":
        return "starting" if service and not service_ready(record) else "running"
    return {"succeeded": "done", "failed": "failed"}.get(outcome or "", "cancelled")


def _job(record: OperationRecord) -> JobSpec | None:
    try:
        if record.kind == "serve":
            return ServiceSpec.model_validate(record.spec).job
        if record.kind == "run":
            return JobSpec.model_validate(record.spec)
    except ValueError:
        return None
    return None


def _charge(record: OperationRecord) -> Charge | None:
    found = (record.result or {}).get("charge")
    try:
        return Charge.model_validate(found) if isinstance(found, dict) else None
    except ValueError:
        return None


def _step_nodes(charge: Charge | None, steps: list[PackageRef], index: int) -> int | None:
    if charge is None:
        return None
    if len(steps) == 1 or not charge.steps:
        return charge.nodes
    shares = charge.steps
    return shares[index].nodes if index < len(shares) else None


def _components(
    record: OperationRecord, job: JobSpec | None, charge: Charge | None, state: str
) -> list[dict[str, Any]]:
    held = state in ("running", "starting")
    serve = record.kind == "serve"

    def component(
        id_: str, label: str, package: str | None, role: str, nodes: int | None, **extra: Any
    ) -> dict[str, Any]:
        running = state == "running"
        return {
            "id": id_,
            "label": label,
            "package": package,
            "role": role,
            "nodes": nodes,
            "state": state,
            "nodes_allocated": (nodes or 0) if held else 0,
            "nodes_running": (nodes or 0) if held and running else 0,
            "hosts": [],
            **extra,
        }

    if record.kind == "install":
        return [component("install", "Software install", None, "install", 1)]
    if job is None:
        return []
    steps = job.steps()
    if not steps:
        name = PurePosixPath(job.executable or "job").name or "job"
        ranks = charge.ranks if charge else None
        role = "service" if serve else "compute"
        return [component("job", name, None, role, charge.nodes if charge else None, ranks=ranks)]
    names = jarvis_submit.step_names(steps)
    found: list[dict[str, Any]] = []
    for index, (step, name) in enumerate(zip(steps, names, strict=True)):
        role = "service" if step.service or (serve and len(steps) == 1) else "compute"
        found.append(
            component(
                name,
                step.name.partition(".")[2] or step.name,
                step.name,
                role,
                _step_nodes(charge, steps, index),
                ranks=charge.ranks if charge and len(steps) == 1 else None,
                host_slice=step.nodes(),
            )
        )
    return found


def _plan(record: OperationRecord) -> dict[str, Any] | None:
    """The plan JARVIS submitted the job with, else the one its card showed."""
    found = record.handle.data.get(jarvis_plan.KEY) if record.handle else None
    found = found or (record.result or {}).get(jarvis_plan.KEY)
    return cast(dict[str, Any], found) if isinstance(found, dict) else None


def _planned(record: OperationRecord, plan: dict[str, Any], state: str) -> list[dict[str, Any]]:
    """The components of JARVIS's plan, each in the state JARVIS last wrote for it."""
    held = state in ("running", "starting")
    lines = {
        str(line.get("component")): line
        for line in cast(list[dict[str, Any]], (record.result or {}).get("components") or [])
    }
    found: list[dict[str, Any]] = []
    for item in cast(list[dict[str, Any]], plan.get("components") or []):
        nodes = item.get("nodes")
        line = lines.get(str(item.get("id"))) if state != "planned" else None
        current = str(line.get("state") or state) if line else state
        if (
            line
            and state in ("done", "failed", "cancelled")
            and current not in jarvis_plan.TERMINAL
        ):
            current = state
        hosts = cast(list[str], (line or {}).get("hosts") or [])
        allocated = len(hosts) if hosts else (nodes or 0) if held else 0
        found.append(
            {
                "id": item.get("id"),
                "label": item.get("label") or item.get("package") or item.get("id"),
                "package": item.get("package"),
                "role": jarvis_plan.role(plan, item),
                "nodes": nodes,
                "ranks": item.get("ranks"),
                "host_slice": item.get("host_slice"),
                "node_range": item.get("node_range"),
                "state": current,
                "nodes_allocated": allocated,
                "nodes_running": allocated if current == "running" else 0,
                "hosts": hosts,
                "reason": (line or {}).get("reason"),
            }
        )
    return found


def _couplings(plan: dict[str, Any]) -> list[dict[str, Any]]:
    keys = ("from", "to", "kind", "label", "source")
    couplings = cast(list[dict[str, Any]], plan.get("couplings") or [])
    return [{key: edge.get(key) for key in keys} for edge in couplings]


def _scheduler(service: RelayService, record: OperationRecord) -> str | None:
    try:
        config = service.site(record.site).config
    except Exception:  # noqa: BLE001 - a removed site still has its records
        return None
    found = config.get("scheduler")
    return found if isinstance(found, str) and found not in ("auto", "") else None


def _site(service: RelayService, site_id: str) -> dict[str, Any]:
    try:
        return {"id": site_id, "label": service.site(site_id).title}
    except Exception:  # noqa: BLE001 - a removed site still has its records
        return {"id": site_id, "label": site_id}


def _when(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        found = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return found if found.tzinfo else found.replace(tzinfo=UTC)


def _started(record: OperationRecord) -> datetime | None:
    """When the job itself started, not the operation.

    The scheduler's start for this job, as its adapter reported it (``started_at``
    in the result, set only for the handle's own job id), comes first;
    then the first state JARVIS wrote from inside the allocation (a component
    line with hosts); then the start of relay's latest running stretch, after
    any install job or requeue before it.
    """
    result = record.result or {}
    found = _when(result.get("started_at"))
    if found is not None:
        return found
    lines = cast(list[dict[str, Any]], result.get("components") or [])
    inside = [at for line in lines if line.get("hosts") and (at := _when(line.get("at")))]
    if inside:
        return min(inside)
    if record.handle is None:
        return None
    running: datetime | None = None
    for change in record.history:
        if change.state == OperationState.RUNNING:
            running = running or change.at
        elif not change.state.final:
            # Waiting again (an install job before it, a requeue): a later stretch counts.
            running = None
    return running


def snapshot(service: RelayService, record: OperationRecord) -> dict[str, Any] | None:
    """Return the operation's job card, or ``None`` for an operation that is not a job."""
    if record.kind not in ("run", "serve", "install"):
        return None
    job = _job(record)
    charge = _charge(record)
    phase = _phase(record)
    outcome = _outcome(record)
    state = _state(record, outcome, service=record.kind == "serve")
    queue = cast(dict[str, Any], (record.result or {}).get("queue") or {})
    started = _started(record)
    ended = record.history[-1].at if record.state.final and record.history else None
    walltime = charge.walltime_s if charge else None
    if walltime is None and job is not None and job.attributes.duration is not None:
        walltime = job.attributes.duration.total_seconds()
    plan = _plan(record) or {}
    walltime = plan.get("time_limit_s") or walltime
    return {
        "version": VERSION,
        "operation_id": record.operation_id,
        "resource": job_uri(record.operation_id),
        "kind": record.kind,
        "job_id": record.handle.native_id if record.handle else None,
        "revision": int(record.updated_at.timestamp() * 1000),
        "updated_at": record.updated_at.isoformat(),
        "source": "jarvis" if plan else "relay",
        "site": _site(service, record.site),
        "scheduler": plan.get("scheduler") or _scheduler(service, record),
        "phase": phase,
        "outcome": outcome,
        "partition": plan.get("partition")
        or (charge.queue if charge else None)
        or queue.get("partition"),
        "account": plan.get("account") or (charge.account if charge else None),
        "qos": plan.get("qos"),
        "time_limit_s": None if walltime is None else int(walltime),
        "started_at": started.isoformat() if started else None,
        "ended_at": ended.isoformat() if ended else None,
        "queue": (
            {
                "reason": queue.get("reason"),
                "position": None,
                "starts_in_s": queue.get("starts_in_s"),
            }
            if phase == "queued" and queue
            else None
        ),
        "components": _planned(record, plan, state)
        if plan
        else _components(record, job, charge, state),
        "couplings": _couplings(plan),
        "advisories": jarvis_plan.advisories(
            plan, cast(dict[str, Any] | None, (record.result or {}).get("site_facts"))
        ),
        "script": plan.get("script"),
    }


def site_jobs(service: RelayService, site_id: str) -> list[dict[str, Any]]:
    """Return the site's open jobs and its latest ended ones, newest first."""
    records = sorted(
        (r for r in service.operations.all() if r.site == site_id),
        key=lambda r: r.created_at,
        reverse=True,
    )
    shown = [r for r in records if not r.state.final]
    shown += [r for r in records if r.state.final][:LISTED_ENDED]
    shown.sort(key=lambda r: r.created_at, reverse=True)
    return [card for r in shown if (card := snapshot(service, r)) is not None]
