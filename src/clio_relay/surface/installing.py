"""Installing what a package job needs before it runs: its software, or JARVIS itself.

A package job's submit first runs JARVIS's light ``ppl install +check`` on the
login host, which runs no Spack: it answers from the record of the last check a
job made. When the site lacks the software, needs a decision, or has no matching
record yet, JARVIS exits 3 before anything reaches the scheduler and prints its
install report; the adapter raises that as
:func:`~clio_relay.jarvis_report.install_needed`. When the site has no JARVIS
at all, the adapter raises :func:`~clio_relay.jarvis_submit.missing_jarvis`.

Either refusal turns the operation back into asks instead of a failure:

- software: first one choice per issue JARVIS needs a person for, then, once
  JARVIS's check has no question left, one approval of the install (what is
  reused and what is built, and JARVIS's time for it). JARVIS asks its
  questions a few at a time, so each answered round goes back to JARVIS's check
  (``recheck``, below) until it asks nothing more; the approval is asked last,
  once, and a later round asks it again only when JARVIS names more to build
  than was approved. An issue no answer can fix fails the operation. Once the
  install is approved, relay submits
  ``jarvis ppl install +submit`` with the answers, follows the install job
  (its ``metadata.install`` progress and the end of ``install/install.log``)
  for as long as it takes, then submits the run again. While JARVIS still has
  questions, relay asks only the questions: JARVIS checks again
  with the answers in a job (``recheck``, ``ppl install +check +submit``), and
  the run goes ahead when that job finds the software all there, or relay asks
  to install what it finds missing. When JARVIS has no record to answer from
  (``install_unchecked``), relay asks nothing and runs that check job at once;
  if the light check still finds no record after a job ended, the operation
  fails with JARVIS's reason rather than loop. Where checks and builds run is
  the site's answer to JARVIS's ``install_placement`` question, relayed like the
  others: when the person placed the build on the login host
  (``login_limited``), JARVIS's own ``ppl install --answers`` runs there in the
  background under JARVIS's limits (:func:`~clio_relay.jarvis_exits.login_build`),
  relay follows it the same way with no time limit, and only a cancel stops it;
- JARVIS: one approval, then relay installs jarvis-cd at the pinned commit
  in the site's CLIO home and submits the run again. The approval is
  site-scoped: operations that find the site without JARVIS at the same time
  share it, and the install script's site lock makes the later install wait
  for the first and reuse it. On a guarded site the package review already
  says relay installs its jarvis-cd when the site has none, so an accepted
  review covers it and relay asks no second question
  (:func:`~clio_relay.surface.install_share.jarvis_consented`).

JARVIS's site-scope questions (which MPI) are shared the same way: operations on
the site that get the same question hold one ask, and one answer moves them all
(:mod:`~clio_relay.surface.install_share`).

An ``install`` operation (:mod:`clio_relay.surface.install_op`) is the same flow
with no run: its submit is the light ``ppl install +check``, and it completes once
JARVIS's record says the software is all there.

The state lives in ``record.result["install"]``.

JARVIS replays the answers it records (which Spack, where checks and builds
run, a site copy and the like) on a later install while they are still among
its candidates, and asks again, with the earlier answer, when they no longer
are. Others (a "this time" placement, or one only a job can apply) hold only
for the call they are sent with. Relay keeps no answer state of its own for
either: what the person said is the operation's asks. Each round reads them
all (:func:`~clio_relay.surface.install_share.choice_answers`), the newest
answer to an issue winning, and sends JARVIS every one; JARVIS looks an answer
up only for an issue it raises. The install approval is one of those asks too:
its binding names the software it approves building (``build``), so a later
round of the operation is covered while JARVIS finds nothing more missing.
Relay asks only the issues JARVIS leaves open, and the install approval names
the decisions JARVIS applies
(:func:`~clio_relay.jarvis_report.applied_decisions`).
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, cast

from clio_relay import (
    install_failures,
    install_time,
    install_wording,
    jarvis_report,
    jarvis_submit,
)
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import Ask, explain_approval
from clio_relay.asks.core import approval_covers as approval_covers
from clio_relay.jarvis_exits import build_refused, built_here, ran_here
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec, canonical_digest
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import install_logs, queue_switch
from clio_relay.surface.install_share import CHOICE_FIELD as CHOICE_FIELD
from clio_relay.surface.install_share import JARVIS_KIND as JARVIS_KIND
from clio_relay.surface.install_share import (
    choice_answers,
    choice_ask,
    jarvis_consented,
    offer_map,
    shared_jarvis_ask,
)
from clio_relay.surface.install_share import holders as holders
from clio_relay.surface.install_share import site_lock_id as site_lock_id
from clio_relay.surface.install_share import withdrawable as withdrawable

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

INSTALL_KEY = "install"
SOFTWARE_KIND = "jarvis_install"
BUILD_KEY = "build"
"""In an install approval's binding: JARVIS's keys of the software it approves building."""
OFFERS_KEY = "offers"
INSTALL_TIME_KEY = "install_time"
MAX_ROUNDS = 8
"""How many install rounds (check jobs, questions, installs) one operation may go through
before relay stops asking."""

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
    chosen = state_of(record).get("install_scheduler")
    if isinstance(chosen, dict):
        state["install_scheduler"] = chosen
    asks: list[Ask] = []
    if kind == "software" and refusal.data.get("kind") == install_time.KIND:
        return await _ask_install_time(service, operation_id, site, refusal, state)
    if kind == "software":
        report = cast(dict[str, Any], refusal.data["install_report"])
        blocking = jarvis_report.blocking_issues(report)
        if blocking:
            return service.finish(
                operation_id,
                OperationState.FAILED,
                jarvis_report.install_blocked(blocking, site=site.title),
            )
        missing_keys = jarvis_report.missing_keys(report)
        found = state_of(record).get("installed")
        installed = (
            [str(key) for key in cast(list[object], found)] if isinstance(found, list) else []
        )
        repeated = set(installed) & set(missing_keys)
        if repeated:
            # An install job succeeded, yet JARVIS's check reports the same software
            # missing: asking the same approval again would loop the person.
            return service.finish(
                operation_id,
                OperationState.FAILED,
                jarvis_report.still_missing(report, repeated, site=site.title),
            )
        state["missing_keys"] = missing_keys
        if installed:
            state["installed"] = installed
        names = jarvis_submit.package_names(job_of(record).steps())
        state["summary"] = jarvis_report.install_summary(report, names)
        state[OFFERS_KEY] = _offers(state_of(record))
        to_ask = jarvis_report.choice_issues(report)
        missing = jarvis_report.software_missing(report)
        unchecked = jarvis_report.unchecked(report)
        if unchecked and not to_ask and not missing:
            if state_of(record).get("job_ended"):
                return service.finish(
                    operation_id,
                    OperationState.FAILED,
                    jarvis_report.check_unrecorded(unchecked, site=site.title),
                )
            # Nobody answers this: JARVIS checks in a job, which writes the record.
            state["recheck"] = True
            _hold(service, operation_id, state, [], refusal.message)
            return await service.advance(operation_id)
        approved = _approved(service, record, missing_keys)
        if to_ask:
            # JARVIS asks its questions one at a time; the approval waits until it
            # has none, so it is asked once and carries JARVIS's spec and time.
            state["recheck"] = not (missing and approved)
        elif approved:
            # The person already approved building this; JARVIS's last answer
            # changed nothing they were asked about.
            _hold(service, operation_id, state, [], refusal.message)
            return await service.advance(operation_id)
        else:
            package = _what(job_of(record))
            runs = record.kind != "install"
            approval = await provider.approve(
                site=site,
                operation_id=operation_id,
                message=install_wording.install_message(
                    report,
                    site=site.title,
                    package=package,
                    names=names,
                    walltime=cast(str | None, refusal.data.get("install_walltime")),
                    run=runs,
                    settings=_settings_said(service, site, job_of(record)),
                ),
                binding={
                    **binding,
                    "kind": SOFTWARE_KIND,
                    BUILD_KEY: sorted(missing_keys),
                },
                plain=False,
            )
            title, yes, no = install_wording.install_meanings(
                report, site=site.title, package=package, names=names, run=runs
            )
            explain_approval(approval, title=title, yes=yes, no=no)
            service.asks.put(approval.ask_id, approval)
            asks.append(approval)
        with service.asks.locked(site_lock_id(site.id)):
            for issue in to_ask:
                ask = await choice_ask(service, site, operation_id, issue)
                state[OFFERS_KEY][ask.ask_id] = offer_map(issue)
                asks.append(ask)
    else:
        if jarvis_consented(service, site.id, record):
            # The package review the person accepted already said relay installs it.
            _hold(service, operation_id, state, [], refusal.message)
            return await service.advance(operation_id)
        with service.asks.locked(site_lock_id(site.id)):
            ask = shared_jarvis_ask(service, site.id, operation_id) or await provider.approve(
                site=site,
                operation_id=operation_id,
                message=(
                    f"{refusal.message}. Relay installs jarvis-cd "
                    f"{jarvis_submit.JARVIS_PIN[:8]} in a virtual environment under the CLIO "
                    "home on the site, which needs the site to reach github.com. Install "
                    f"jarvis-cd on {site.title}?"
                ),
                binding={
                    "site": site.id,
                    "scope": "site",
                    "kind": JARVIS_KIND,
                    "jarvis": jarvis_submit.JARVIS_PIN,
                },
                plain=False,
            )
            return _hold(service, operation_id, state, [ask], refusal.message)
    return _hold(service, operation_id, state, asks, refusal.message)


def _settings_said(service: RelayService, site: SiteEntry, job: JobSpec) -> list[tuple[str, str]]:
    """Return ``(JARVIS's question, the value)`` for each install setting the job gives."""
    if not job.install_settings:
        return []
    offers = install_failures.site_offers(service.operations.all(), site.id)
    said: list[tuple[str, str]] = []
    for item in job.install_settings:
        offer = offers.get(item.issue_id) or {}
        question = str(offer.get("question") or offer.get("setting") or item.issue_id)
        said.append((question, item.value))
    return said


def _approved(service: RelayService, record: OperationRecord, missing_keys: list[str]) -> bool:
    """Say whether the person already approved building everything ``missing_keys`` names.

    That is an accepted install approval among the operation's asks, for this
    job, whose binding names at least what JARVIS now finds missing.
    """
    wanted = set(missing_keys)
    for ask in service.operation_asks(record):
        build = ask.binding.get(BUILD_KEY)
        if (
            ask.kind == "approve"
            and ask.state == "accepted"
            and ask.binding.get("kind") == SOFTWARE_KIND
            and ask.binding_digest == canonical_digest(ask.binding)
            and approval_covers(ask.binding, record)
            and isinstance(build, list)
            and wanted <= {str(key) for key in cast(list[object], build)}
        ):
            return True
    return False


def _offers(state: dict[str, Any]) -> dict[str, Any]:
    """Return what each choice ask of the operation answers in JARVIS's terms, by ask id.

    It grows with every round: an ask's options mean the same for the whole
    operation, so :func:`~clio_relay.surface.install_share.choice_answers` reads
    every answer the person gave from it.
    """
    found = state.get(OFFERS_KEY)
    return dict(cast(dict[str, Any], found)) if isinstance(found, dict) else {}


_CARRIED = ("missing_keys", "installed", "summary", "recheck", OFFERS_KEY)
"""What an install round keeps when JARVIS stops it to ask how the build job runs."""


async def _ask_install_time(
    service: RelayService,
    operation_id: str,
    site: SiteEntry,
    refusal: Refusal,
    state: dict[str, Any],
) -> OperationRecord:
    """Ask how JARVIS's install job runs when its estimate is past the partition's limit.

    The person already approved the install, so this is the only ask; the
    round's other state stays (:data:`_CARRIED`). The pick applies in
    :func:`start_install` (:func:`_install_time_answer`).
    """
    previous = state_of(service.operation(operation_id))
    state.update({key: previous[key] for key in _CARRIED if key in previous})
    issue = cast(dict[str, Any], refusal.data["install_report"])["issues"][0]
    ask = await choice_ask(service, site, operation_id, issue)
    state[INSTALL_TIME_KEY] = {"ask_id": ask.ask_id, OFFERS_KEY: offer_map(issue)}
    return _hold(service, operation_id, state, [ask], refusal.message)


def _install_time_answer(
    service: RelayService, operation_id: str, state: dict[str, Any], answers: dict[str, Any]
) -> dict[str, Any]:
    """Turn the person's install-time pick into relay's answers; JARVIS gets none for it.

    The question is relay's own (how its install job runs), so it is not among
    the operation's offers and never reaches JARVIS's answers file.
    """
    found = state.get(INSTALL_TIME_KEY)
    asked = cast(dict[str, Any], found) if isinstance(found, dict) else {}
    ask = service.asks.get(str(asked.get("ask_id"))) if asked else None
    content = cast(dict[str, Any], (ask.answer or {}).get("content") or {}) if ask else {}
    offers = cast(dict[str, Any], asked.get(OFFERS_KEY) or {})
    offer = offers.get(str(content.get(CHOICE_FIELD) or ""))
    if isinstance(offer, (list, tuple)) and len(cast(list[Any], offer)) == 2:
        settings = install_time.picked(str(cast(list[Any], offer)[1]))
        if settings is None:
            state = {**state, "recheck": True}
        else:
            state = {**state, "install_scheduler": settings, "recheck": False}
        _update(
            service,
            operation_id,
            recheck=state["recheck"],
            install_scheduler=state.get("install_scheduler"),
        )
    if isinstance(state.get("install_scheduler"), dict):
        answers[jarvis_submit.INSTALL_SCHEDULER] = state["install_scheduler"]
    return state


def _hold(
    service: RelayService,
    operation_id: str,
    state: dict[str, Any],
    asks: list[Ask],
    message: str,
) -> OperationRecord:
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return record
        record.result = {**(record.result or {}), INSTALL_KEY: state, "message": message}
        record.asks = [*record.asks, *(ask.ask_id for ask in asks if ask.ask_id not in record.asks)]
        record.move(OperationState.AWAITING_INPUT, message)
        service.operations.put(operation_id, record)
    return record


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
        answers = {
            **install_failures.setting_answers(job_of(record)),
            **choice_answers(service, record, _offers(state)),
        }
        state = _install_time_answer(service, operation_id, state, answers)
        if state.get("recheck"):
            answers[jarvis_submit.RECHECK] = True
        handle = await adapter.install(job, operation_id=operation_id, answers=answers)
        if ran_here(handle):
            # The site runs JARVIS's check on the submitting host (install_placement).
            _update(service, operation_id, stage="done", job_ended=True)
            return await _resubmit(service, operation_id, "JARVIS checked the software here")
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
        message = (
            "JARVIS checks the package's software in a job on the site"
            if state.get("recheck")
            else "installing the package's software as a job on the site"
        )
        if built_here(handle) is not None:
            message = (
                "JARVIS builds the package's software on the site's login host under its "
                "own limits, in the background with no time limit; cancel stops it"
            )
        record.result = {**(record.result or {}), INSTALL_KEY: state, "message": message}
        cancelled = record.state.final
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
    tail, offset = await install_logs.log_tail(adapter, handle, int(state.get("log_offset") or 0))
    if status.state.final:
        # One caller moves a job's end on: concurrent observers, here or in
        # another process, would each resubmit and ask the person twice.
        lease = service.operations.claim_submit(operation_id)
        if lease is None:
            return service.operation(operation_id)
        with lease:
            if install_handle(service.operation(operation_id)) != handle:
                return service.operation(operation_id)
            return await _job_done(
                service, operation_id, status, (adapter, handle), (tail, offset, state)
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
            running = "the install job is running"
            if built_here(handle) is not None:
                running = "JARVIS is building on the login host"
            record.move(OperationState.RUNNING, running)
        service.operations.put(operation_id, record)
    if status.queue:
        return await queue_switch.consider(service, operation_id, adapter, handle, "install")
    return record


async def _job_done(
    service: RelayService,
    operation_id: str,
    status: JobStatus,
    job: tuple[BaseAdapter[Any], JobHandle],
    seen: tuple[str | None, int, dict[str, Any]],
) -> OperationRecord:
    """Move an ended install job on. The caller holds the submit lease."""
    tail, offset, state = seen
    if status.state != JobState.COMPLETED:
        return await _ended(service, operation_id, status, job, tail or state.get("log_tail"))
    checked = bool(state.get("recheck"))
    installed: dict[str, Any] = {} if checked else {"installed": state.get("missing_keys") or []}
    _update(
        service,
        operation_id,
        stage="done",
        job_ended=True,
        log_offset=offset,
        log_tail=tail,
        **installed,
    )
    then = (
        "JARVIS checks it"
        if service.operation(operation_id).kind == "install"
        else "submitting the run"
    )
    done = "the check job finished" if checked else "the install finished"
    return await _resubmit(service, operation_id, f"{done}; {then}")


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
    login = built_here(handle)
    if login is not None and status.exit_code == jarvis_submit.EXIT_BUILD_REFUSED:
        # JARVIS would not build on the login host after all (it was too busy for
        # its limits): final, and the person chooses again on a later install.
        text = await install_logs.login_report(adapter, login)
        site = service.site(service.operation(operation_id).site).title
        refusal = build_refused(text or str(tail or ""), site=site)
        return service.finish(operation_id, OperationState.FAILED, refusal)
    record = service.operation(operation_id)
    site = service.site(record.site).title
    issues = await install_logs.job_issues(adapter, handle)
    where: dict[str, Any] = {
        "install_log": install_logs.log_path(handle),
        "install_job": handle.native_id,
    }
    output = tail if isinstance(tail, str) else ""
    if status.exit_code == install_failures.EXIT_INSTALL_FAILED:
        # Something JARVIS ran failed: its records and words go to the agent whole.
        return service.finish(
            operation_id,
            OperationState.FAILED,
            install_failures.failed(issues, site=site, output=output, **where),
        )
    if status.exit_code == jarvis_submit.EXIT_DECISION:
        blocking = jarvis_report.blocking_issues({"issues": issues})
        if blocking:
            return service.finish(
                operation_id,
                OperationState.FAILED,
                jarvis_report.install_blocked(blocking, site=site),
            )
        # The job found a question (which MPI, a compiler the build host lacks) and
        # recorded it: the light check on the login host reads that record and asks it.
        _update(service, operation_id, stage="done", job_ended=True)
        return await _resubmit(
            service, operation_id, "the install job needs a decision; JARVIS checks again"
        )
    # Any other end: what the job printed, as it printed it.
    message = (
        output.strip()[-4000:]
        or status.message
        or f"the install job ended {status.native_state or status.state.value}"
    )
    return service.finish(
        operation_id,
        OperationState.FAILED,
        Refusal(
            RefusalReason.SUBMIT_FAILED,
            message,
            site=site,
            needs=jarvis_submit.NEEDS_INSTALL,
            exit_code=status.exit_code,
            issues=issues,
            **where,
        ),
    )
