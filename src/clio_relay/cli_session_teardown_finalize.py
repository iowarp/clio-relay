"""``session teardown``'s coordinator-call and closure phase (iowarp/
clio-relay#231 continuation, ``cli_session_teardown.py`` split): call
the coordinator's authoritative ``teardown_remote_session``, reconcile
any relay/scheduler jobs that raced the API stop, verify every touched
resource is accounted for, persist and checkpoint the finalized report,
mark the owner-session generation closed, and emit the completed
report.

Moved verbatim off the pre-split module's nested closure body; only the
enclosing free variables (now read from the shared
:class:`~clio_relay.cli_session_teardown_state._TeardownState`) and the
report-emission/close-report calls (now reached through
``cli_session_teardown_report``/``cli_session_teardown_state``) changed
shape. ``_persist_verified_cleanup_report_before_closure`` is reached
through the facade module (a deferred import, taken only when this
function actually runs) rather than directly off
``cli_session_teardown_state``: ``tests/test_cli.py`` monkeypatches it
on the facade module object by name, and a module-scope import of the
facade here would cycle back through ``cli.py`` -> ``cli_session_
teardown`` -> ``cli_session_teardown_action`` -> this module -> the
facade.
"""

from __future__ import annotations

from typing import cast

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_report_artifact as cli_owned_report_artifact
import clio_relay.cli_owned_runtime_cleanup as cli_owned_runtime_cleanup
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_owner_session_teardown_verify as cli_owner_session_teardown_verify
import clio_relay.cli_session_teardown_jobs as cli_session_teardown_jobs
import clio_relay.cli_session_teardown_report as cli_session_teardown_report
import clio_relay.core_queue as core_queue
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.cli_session_teardown_state import _TeardownState
from clio_relay.errors import RelayError
from clio_relay.models import JobState
from clio_relay.session_lifecycle import session_lifecycle_report_sha256
from clio_relay.validation_report import sha256_file


def _run_teardown_finalize_phase(state: _TeardownState) -> None:
    """Call the coordinator teardown, reconcile, verify, close, and report."""
    import clio_relay.cli_session_teardown as cli_session_teardown

    definition = state.definition
    cluster = state.cluster
    session_id = state.session_id
    session_generation_id = state.session_generation_id
    stop_worker = state.stop_worker
    cancel_jobs = state.cancel_jobs
    cancel_scheduler_jobs = state.cancel_scheduler_jobs
    remote_execution = state.remote_execution
    queue = cast(core_queue.ClioCoreQueue, state.queue)
    scheduler_sentinel_ids = state.scheduler_sentinel_ids
    relay_cancel_timeout_seconds = state.relay_cancel_timeout_seconds
    relay_cancel_poll_seconds = state.relay_cancel_poll_seconds
    validation_launcher = state.validation_launcher
    validation_install_source = state.validation_install_source
    validation_artifact = state.validation_artifact
    seed_report = state.seed_report
    recovery_resource = state.recovery_resource
    recovery_status = state.recovery_status
    local_admission_session_id = state.local_admission_session_id
    active_evidence_lock = state.active_evidence_lock
    cleanup_intent = state.cleanup_intent
    owned_jobs = state.owned_jobs
    canceled = state.canceled
    scheduler_sentinel_pre_phases = state.scheduler_sentinel_pre_phases
    gateway_reports = state.gateway_reports
    canonical_report_path = state.canonical_report_path
    cleanup_operation_id = state.cleanup_operation_id
    requested_policy = state.requested_policy

    def list_owned_jobs(
        *, include_terminal: bool = False
    ) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
        return cli_session_teardown_jobs._list_owned_jobs(
            remote_execution=remote_execution,
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
            queue=queue,
            scheduler_provider=definition.scheduler_provider,
            include_terminal=include_terminal,
        )

    def read_owned_job(job_id: str) -> cli_owned_relay_jobs._OwnedRelayJob:
        return cli_session_teardown_jobs._read_owned_job(
            job_id,
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )

    report = session_lifecycle.teardown_remote_session(
        definition=definition,
        session_id=session_id,
        expected_session_generation_id=session_generation_id,
        expected_cleanup_operation_id=cast(str, cleanup_intent["operation_id"]),
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        cluster=cluster,
    )
    report.cleanup_operation_id = cast(str, cleanup_intent["operation_id"])
    report.cleanup_policy = {
        key: cast(bool, cleanup_intent[key])
        for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
    }
    report.relay_cancel_requested = cancel_jobs
    report.scheduler_cancel_requested = cancel_jobs and cancel_scheduler_jobs
    partial = report.to_live_validation_report(
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    partial = partial.model_copy(
        update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
    )
    if recovery_resource is not None:
        partial.resources.append(recovery_resource)
    state.canonical_report = partial
    post_api_jobs = list_owned_jobs(include_terminal=True)
    initial_job_ids = {job.job_id for job in owned_jobs}
    late_jobs = [job for job in post_api_jobs if job.job_id not in initial_job_ids]
    if cancel_jobs and late_jobs:
        late_targets = (
            cli_owned_relay_jobs._cancel_remote_owned_jobs(definition, cluster, late_jobs)
            if remote_execution
            else cli_owned_relay_jobs._cancel_local_owned_jobs(queue, late_jobs)
        )
        canceled.extend(
            cli_owned_relay_jobs._wait_for_owned_relay_cancellations(
                late_targets,
                read_owned_job=read_owned_job,
                timeout_seconds=relay_cancel_timeout_seconds,
                poll_seconds=relay_cancel_poll_seconds,
            )
        )
        owned_jobs.extend(late_jobs)

    gateway_scheduler_job_ids = (
        cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
        )
        if scheduler_sentinel_ids
        else ()
    )
    cli_owned_scheduler_cancel._assert_scheduler_sentinels_unrelated(
        scheduler_sentinel_ids,
        owned_jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
    )

    scheduler_jobs = list_owned_jobs(include_terminal=True)
    by_job_id: dict[str, cli_owned_relay_jobs._OwnedRelayJob] = {}
    for job in [*owned_jobs, *scheduler_jobs]:
        by_job_id.setdefault(job.job_id, job)
    owned_jobs = list(by_job_id.values())
    gateway_scheduler_job_ids = (
        cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
        )
        if scheduler_sentinel_ids
        else ()
    )
    cli_owned_scheduler_cancel._assert_scheduler_sentinels_unrelated(
        scheduler_sentinel_ids,
        owned_jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
    )
    report.resources.extend(
        cli_owned_scheduler_cancel._owned_job_cleanup_resources(
            owned_jobs,
            definition=definition,
            location=definition.ssh_host,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            post_operation_jobs=scheduler_jobs,
        )
    )
    if cancel_jobs and cancel_scheduler_jobs:
        scheduler_resources, scheduler_errors = (
            cli_owned_scheduler_cancel._cancel_owned_scheduler_jobs(
                definition,
                owned_jobs,
            )
        )
        report.resources.extend(scheduler_resources)
        report.errors.extend(scheduler_errors)
    sentinel_resources, sentinel_errors = (
        cli_owned_scheduler_cancel._scheduler_sentinel_preservation_resources(
            definition,
            scheduler_sentinel_pre_phases,
        )
    )
    report.resources.extend(sentinel_resources)
    report.errors.extend(sentinel_errors)
    final_jobs = list_owned_jobs(include_terminal=True)
    if cancel_jobs:
        uncanceled = [
            job.job_id
            for job in final_jobs
            if job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
            or (job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged)
        ]
        if uncanceled:
            report.errors.append(
                "owned relay jobs remained active after final rescan: "
                + ", ".join(sorted(uncanceled))
            )
    cli_owned_runtime_cleanup._merge_gateway_cleanup_resources(report, gateway_reports)
    cli_owner_session_teardown_verify._verify_owner_session_teardown(
        report,
        session_id=session_id,
        session_generation_id=session_generation_id,
        stop_worker=stop_worker,
    )
    report, finalized_recovery = (
        cli_session_teardown._persist_verified_cleanup_report_before_closure(
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
            report=report,
        )
    )
    finalized_reference = finalized_recovery.coordinator_report_ref
    if finalized_reference is None:
        raise RelayError("finalized cleanup omitted its exact report reference")
    local_cleanup_artifact = cli_owned_report_artifact._persist_local_cleanup_report_artifact(
        report,
        validation_report_path=canonical_report_path,
        evidence_lock=active_evidence_lock,
    )
    cli_session_teardown_report._checkpoint_finalized_cleanup_artifact(
        state,
        report,
        recovery=finalized_recovery,
        local_artifact=local_cleanup_artifact,
    )
    cli_cleanup_evidence._verify_cleanup_evidence_lock(
        active_evidence_lock,
        expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
    )
    legacy_recovery = recovery_status
    legacy_unversioned_job_ids: list[str] = []
    cli_owned_session_recovery._mark_owner_session_closed(
        queue=queue,
        definition=definition,
        cluster=cluster,
        remote_execution=remote_execution,
        session_id=session_id,
        local_admission_session_id=local_admission_session_id,
        session_generation_id=session_generation_id,
        legacy_unversioned_job_ids=legacy_unversioned_job_ids,
        finalized_recovery=finalized_recovery,
        finalized_report=report,
    )
    closed_recovery = cli_owned_session_recovery._owned_session_recovery_status(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        cluster=cluster,
        session_id=session_id,
    )
    if not (
        closed_recovery.recovery_verified
        and closed_recovery.cleanup_receipt
        and closed_recovery.cleanup_paths_pending is False
        and closed_recovery.coordinator_report_bound
        and closed_recovery.session_generation_id == session_generation_id
        and closed_recovery.process_state == "already_closed"
        and isinstance(closed_recovery.admission_status, dict)
        and closed_recovery.admission_status.get("closed") is True
        and closed_recovery.coordinator_report_ref == finalized_reference
    ):
        raise RelayError("cleanup was not authoritatively closed after commit")
    closed_report = cli_cleanup_report._verified_finalized_cleanup_report(
        closed_recovery,
        report=report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=requested_policy,
    )
    if session_lifecycle_report_sha256(closed_report) != local_cleanup_artifact.report_sha256:
        raise RelayError("finalized cleanup report changed during authoritative closure")
    recovery_status = closed_recovery
    recovery_resource = cli_owned_session_recovery._owner_session_recovery_validation_resource(
        closed_recovery
    )
    cli_session_teardown_report._emit_completed_report(
        state,
        report,
        canceled_job_ids=canceled,
        gateway_reports=gateway_reports,
        recovery=closed_recovery,
        local_artifact=local_cleanup_artifact,
        legacy_recovery=legacy_recovery,
    )
