"""Installing what a package job needs before it runs: its software, or JARVIS itself.

A package job's submit runs ``jarvis ppl submit`` with ``JARVIS_INSTALL_ON_LOAD=check``.
When the site lacks the software, JARVIS exits 3 before anything reaches the
scheduler and prints its install report; the adapter raises that as
:func:`~clio_relay.jarvis_submit.install_needed`. When the site has no JARVIS
at all, the adapter raises :func:`~clio_relay.jarvis_submit.missing_jarvis`.

Either refusal turns the operation back into asks instead of a failure:

- software: one approval of the install (what is reused and what is built),
  then one choice per issue JARVIS needs a person for. An issue no answer can
  fix fails the operation. Once all are answered, relay submits
  ``jarvis ppl install +submit`` with the answers, follows the install job
  (its ``metadata.install`` progress and the end of ``install/install.log``)
  for as long as it takes, then submits the run again;
- JARVIS: one approval, then relay installs jarvis-cd at the pinned commit
  in the site's CLIO home and submits the run again.

The state lives in ``record.result["install"]``.

JARVIS records each answer in the site's decisions file
(:func:`~clio_relay.jarvis_submit.site_decisions`) and replays it on a later
install, of any package, while it is still one of its candidates; it asks again,
with the earlier answer, when it no longer is. Relay keeps no store of its own:
it asks only the issues JARVIS leaves open, and the install approval names the
decisions JARVIS applies (:func:`~clio_relay.jarvis_submit.applied_decisions`).
"""

from __future__ import annotations

import logging
import posixpath
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, cast

from clio_relay import install_failures, jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import queue_switch

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

INSTALL_KEY = "install"
SOFTWARE_KIND = "jarvis_install"
JARVIS_KIND = "jarvis_bootstrap"
CHOICE_FIELD = "constraint"
LOG_TAIL_BYTES = 4096
MAX_ROUNDS = 3
"""How many installs one operation may go through before relay stops asking."""

START_GRACE = timedelta(hours=6)
"""How long installing jarvis-cd or submitting the install job may hold ``submitting``."""

InstallKind = Literal["software", "jarvis"]


def install_kind(refusal: Refusal) -> InstallKind | None:
    """Return what ``refusal`` asks relay to install, or None when it is a plain refusal."""
    data = refusal.data
    if data.get("needs") == jarvis_submit.NEEDS_INSTALL and isinstance(
        data.get("install_report"), dict
    ):
        return "software"
    if data.get("missing") == "jarvis" and data.get("installable"):
        return "jarvis"
    return None


def state_of(record: OperationRecord) -> dict[str, Any]:
    """Return the operation's install state, empty when it has none."""
    found = (record.result or {}).get(INSTALL_KEY)
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def install_handle(record: OperationRecord) -> JobHandle | None:
    """Return the handle of the install job the operation follows, if one is running."""
    state = state_of(record)
    if state.get("stage") != "installing" or not isinstance(state.get("handle"), dict):
        return None
    return JobHandle.model_validate(state["handle"])


def pending(record: OperationRecord) -> bool:
    """Return whether the operation's answered asks start an install, not the run."""
    return state_of(record).get("stage") == "asking"


def job_of(record: OperationRecord) -> JobSpec:
    """Return the job an operation submits: the run's spec, or the service's job.

    A partition the person moved the operation to applies here too.
    """
    if record.kind == "serve":
        return queue_switch.applied(record, ServiceSpec.model_validate(record.spec).job)
    return queue_switch.applied(record, JobSpec.model_validate(record.spec))


def _can_install(service: RelayService, site: SiteEntry, adapter: str, kind: InstallKind) -> bool:
    """Say whether the site's adapter defines the hook the install needs."""
    try:
        found = service.adapter_named(site, adapter)
    except Refusal:
        return False
    hook = "install_jarvis" if kind == "jarvis" else "install"
    return getattr(type(found), hook, None) is not getattr(BaseAdapter, hook)


def _what(job: JobSpec) -> str:
    return " then ".join(step.name for step in job.steps()) or "the package"


async def ask_install(
    service: RelayService, operation_id: str, site: SiteEntry, refusal: Refusal
) -> OperationRecord:
    """Turn an install refusal into asks, or fail the operation when no answer helps."""
    kind = install_kind(refusal)
    record = service.operation(operation_id)
    rounds = int(state_of(record).get("rounds") or 0)
    if (
        kind is None
        or rounds >= MAX_ROUNDS
        or not _can_install(service, site, record.adapter, kind)
    ):
        return service.finish(operation_id, OperationState.FAILED, refusal)
    provider = service.ask_provider
    binding: dict[str, Any] = {"site": site.id, "spec_digest": record.spec_digest}
    state: dict[str, Any] = {"kind": kind, "stage": "asking", "rounds": rounds + 1}
    asks: list[Ask] = []
    if kind == "software":
        report = cast(dict[str, Any], refusal.data["install_report"])
        blocking = jarvis_submit.blocking_issues(report)
        if blocking:
            return service.finish(
                operation_id,
                OperationState.FAILED,
                jarvis_submit.install_blocked(blocking, site=site.title),
            )
        names = jarvis_submit.package_names(job_of(record).steps())
        state["summary"] = jarvis_submit.install_summary(report, names)
        state["report_digest"] = jarvis_submit.report_digest(report)
        to_ask = jarvis_submit.choice_issues(report)
        asks.append(
            await provider.approve(
                site=site,
                operation_id=operation_id,
                message=cast(str | None, refusal.data.get("ask_message"))
                or jarvis_submit.install_message(
                    report,
                    site=site.title,
                    package=_what(job_of(record)),
                    names=names,
                    walltime=cast(str | None, refusal.data.get("install_walltime")),
                ),
                binding={
                    **binding,
                    "kind": SOFTWARE_KIND,
                    "report_digest": state["report_digest"],
                },
            )
        )
        choices: dict[str, str] = {}
        for issue in to_ask:
            ask = await provider.choose(
                site=site,
                operation_id=operation_id,
                message=jarvis_submit.choice_message(issue),
                field_name=CHOICE_FIELD,
                options=jarvis_submit.choice_options(issue),
            )
            choices[ask.ask_id] = str(issue.get("issue_id"))
            asks.append(ask)
        state["choices"] = choices
    else:
        asks.append(
            await provider.approve(
                site=site,
                operation_id=operation_id,
                message=(
                    f"{refusal.message}. Relay installs jarvis-cd "
                    f"{jarvis_submit.JARVIS_PIN[:8]} in a virtual environment under the CLIO "
                    "home on the site, which needs the site to reach github.com. Install it?"
                ),
                binding={**binding, "kind": JARVIS_KIND, "jarvis": jarvis_submit.JARVIS_PIN},
            )
        )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.result = {**(record.result or {}), INSTALL_KEY: state, "message": refusal.message}
        record.asks = [*record.asks, *(ask.ask_id for ask in asks)]
        record.move(OperationState.AWAITING_INPUT, refusal.message)
        service.operations.put(operation_id, record)
    return record


def _answers(service: RelayService, state: dict[str, Any]) -> dict[str, Any]:
    """Return the person's answers to this round's open issues; JARVIS replays the rest."""
    picked: dict[str, tuple[str, str]] = {}
    for ask_id, issue_id in cast(dict[str, str], state.get("choices") or {}).items():
        ask = service.asks.get(ask_id)
        content = cast(dict[str, Any], (ask.answer or {}).get("content") or {}) if ask else {}
        value = content.get(CHOICE_FIELD)
        if isinstance(value, str) and value:
            picked[issue_id] = (value, ask_id)
    return jarvis_submit.answers_document(picked)


def _update(service: RelayService, operation_id: str, **changes: Any) -> OperationRecord:
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if not record.state.final:
            state = {**state_of(record), **changes}
            record.result = {**(record.result or {}), INSTALL_KEY: state}
            service.operations.put(operation_id, record)
        return record


async def start_install(
    service: RelayService, operation_id: str, site: SiteEntry, adapter: BaseAdapter[Any]
) -> OperationRecord:
    """Start what the answered asks approved. The caller holds the ``submitting`` fence."""
    record = service.operation(operation_id)
    state = state_of(record)
    try:
        if state.get("kind") == "jarvis":
            jarvis = await adapter.install_jarvis()
            _update(service, operation_id, stage="done", jarvis=jarvis)
            return await _resubmit(service, operation_id, f"jarvis-cd is installed at {jarvis}")
        job = await service.guard.prepare(
            site, job_of(record), adapter=adapter, service=record.kind == "serve"
        )
        handle = await adapter.install(
            job, operation_id=operation_id, answers=_answers(service, state)
        )
    except Refusal as refusal:
        if install_kind(refusal) is not None:
            return await ask_install(service, operation_id, site, refusal)
        return service.finish(operation_id, OperationState.FAILED, refusal)
    except Exception as exc:
        logger.exception("starting the install of %s failed", operation_id)
        return service.finish(
            operation_id,
            OperationState.FAILED,
            Refusal(RefusalReason.SUBMIT_FAILED, f"starting the install failed: {exc}"),
        )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        state = {
            **state_of(record),
            "stage": "installing",
            "handle": handle.model_dump(mode="json"),
            "log_offset": 0,
        }
        message = "installing the package's software as a job on the site"
        record.result = {**(record.result or {}), INSTALL_KEY: state, "message": message}
        cancelled = record.state == OperationState.CANCELLED
        record.move(OperationState.QUEUED, message)
        service.operations.put(operation_id, record)
    if cancelled:
        await adapter.cancel(handle)
    return record


async def _resubmit(service: RelayService, operation_id: str, note: str) -> OperationRecord:
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.move(OperationState.AWAITING_INPUT, note)
        service.operations.put(operation_id, record)
    return await service.submit(operation_id)


def _log_path(handle: JobHandle) -> str | None:
    execution = handle.data.get("execution_dir")
    job_dir = handle.data.get("job_dir") or handle.data.get("root")
    if not isinstance(execution, str) or not execution:
        if not isinstance(job_dir, str) or not job_dir:
            return None
        execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
        execution = jarvis_submit.execution_dir(job_dir, execution_id, pipeline=pipeline)
    return posixpath.join(execution, "install", "install.log")


async def _log_tail(
    adapter: BaseAdapter[Any], handle: JobHandle, offset: int
) -> tuple[str | None, int]:
    """Return the install log written since ``offset`` (its last part), and the new offset."""
    path = _log_path(handle)
    if path is None or "files" not in type(adapter).implements:
        return None, offset
    try:
        files = await adapter.files()
        size = (await files.stat(path)).size or 0
        if size <= offset:
            return None, offset
        start = max(offset, size - LOG_TAIL_BYTES)
        data = await files.read(path, offset=start, limit=size - start)
    except Exception:
        # The log is not there until the install job starts, and a missed read is no loss.
        logger.debug("reading the install log %s", path, exc_info=True)
        return None, offset
    return data.decode("utf-8", errors="replace"), size


async def refresh_install(
    service: RelayService, operation_id: str, adapter: BaseAdapter[Any], handle: JobHandle
) -> OperationRecord:
    """Follow the install job; once it completes, submit the run itself."""
    try:
        status = await adapter.status(handle)
    except Exception as exc:
        logger.warning("observing the install of %s", operation_id, exc_info=True)
        reason = exc.message if isinstance(exc, Refusal) else f"{type(exc).__name__}: {exc}"
        return service.observation_failed(operation_id, reason, "install")
    state = state_of(service.operation(operation_id))
    tail, offset = await _log_tail(adapter, handle, int(state.get("log_offset") or 0))
    if status.state == JobState.COMPLETED:
        _update(service, operation_id, stage="done", log_offset=offset, log_tail=tail)
        return await _resubmit(service, operation_id, "the install finished; submitting the run")
    if status.state.final:
        return await _ended(
            service, operation_id, status, (adapter, handle), tail or state.get("log_tail")
        )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        result = dict(record.result or {})
        changes: dict[str, Any] = {"log_offset": offset}
        if tail is not None:
            changes["log_tail"] = tail
        result[INSTALL_KEY] = {**state_of(record), **changes}
        message = status.progress.message if status.progress else status.message
        if status.queue:
            message = jarvis_submit.queue_message(status.queue)
            result["queue"] = status.queue
        else:
            result.pop("queue", None)
        result["message"] = f"installing: {message}" if message else "installing"
        if status.progress is not None:
            result["progress"] = status.progress.model_dump(mode="json")
        result["native_state"] = status.native_state
        result["observed_at"] = status.observed_at.isoformat()
        result.pop("observation_failed_at", None)
        record.result = result
        if status.state == JobState.RUNNING:
            record.move(OperationState.RUNNING, "the install job is running")
        service.operations.put(operation_id, record)
    if status.queue:
        return await queue_switch.consider(service, operation_id, adapter, handle, "install")
    return record


async def _job_issues(adapter: BaseAdapter[Any], handle: JobHandle) -> list[dict[str, Any]]:
    """Return the typed issues the install job wrote to ``install/issues.jsonl``."""
    log = _log_path(handle)
    if log is None or "files" not in type(adapter).implements:
        return []
    path = posixpath.join(posixpath.dirname(log), "issues.jsonl")
    try:
        data = await (await adapter.files()).read(path)
    except Exception:
        logger.warning("reading the install issues %s", path, exc_info=True)
        return []
    return install_failures.read_jsonl(data.decode("utf-8", errors="replace"))


async def _ended(
    service: RelayService,
    operation_id: str,
    status: JobStatus,
    job: tuple[BaseAdapter[Any], JobHandle],
    tail: object,
) -> OperationRecord:
    adapter, handle = job
    if status.state == JobState.CANCELLED:
        return service.finish(operation_id, OperationState.CANCELLED, None)
    if status.exit_code == jarvis_submit.EXIT_DECISION:
        # The job found a question (a compiler the build host lacks): JARVIS's check
        # on the login node reads what the job recorded and asks it.
        return await _resubmit(
            service, operation_id, "the install job needs a decision; JARVIS checks again"
        )
    record = service.operation(operation_id)
    found = install_failures.from_issues(
        await _job_issues(adapter, handle),
        site=service.site(record.site).title,
        install_log=_log_path(handle),
        install_job=handle.native_id,
    )
    if found is not None and install_kind(found) is not None:
        return await ask_install(service, operation_id, service.site(record.site), found)
    if found is not None:
        return service.finish(operation_id, OperationState.FAILED, found)
    detail = status.message or f"the install job ended {status.native_state or 'badly'}"
    message = f"JARVIS could not install the package's software: {detail}"
    if isinstance(tail, str) and tail.strip():
        message = f"{message}\n{tail.strip()[-1500:]}"
    return service.finish(
        operation_id,
        OperationState.FAILED,
        Refusal(
            RefusalReason.SUBMIT_FAILED,
            message,
            needs=jarvis_submit.NEEDS_INSTALL,
            exit_code=status.exit_code,
            install_log=_log_path(handle),
            install_job=handle.native_id,
        ),
    )
