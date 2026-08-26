"""``session teardown``'s recovery-resolution phase (iowarp/clio-relay#231
continuation, ``cli_session_teardown.py`` split): establish the owner
session's exact generation identity, decide whether an earlier attempt
already finalized and closed this cleanup, and -- if so -- finish that
finalized retry completely so ``action()`` can return immediately
instead of re-running the primary teardown path.

Moved verbatim off the pre-split module's nested closure body; only the
enclosing free variables (now read from and written to the shared
:class:`~clio_relay.cli_session_teardown_state._TeardownState`) and the
two report-emission calls (now reached through
``cli_session_teardown_report``) changed shape.
"""

from __future__ import annotations

from json import JSONDecodeError
from typing import cast

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_report_artifact as cli_owned_report_artifact
import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_session_teardown_report as cli_session_teardown_report
import clio_relay.core_queue as core_queue
import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.validation_report as validation_report_module
from clio_relay.cli_session_teardown_state import _TeardownState
from clio_relay.errors import RelayError
from clio_relay.owned_session_record import clear_owned_session_record
from clio_relay.owner_session_admission import (
    assert_no_unscoped_desktop_admission_state as _assert_no_unscoped_desktop_admission_state,
)
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id as _desktop_owner_session_admission_id,
)
from clio_relay.session_lifecycle import (
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
    session_lifecycle_report_sha256,
)
from clio_relay.validation_report import ValidationResource


def _resolve_teardown_recovery(state: _TeardownState) -> bool:
    """Resolve owner-session identity/recovery status; finish a finalized retry.

    Returns ``True`` when the coordinator had already finalized and closed
    this cleanup on an earlier attempt -- in that case this function fully
    re-emits the finalized report and the caller's ``action()`` must return
    immediately without running the primary teardown path a second time.
    Returns ``False`` when the primary path (quiesce, cancel, teardown,
    close) still needs to run.
    """
    definition = state.definition
    session_id = state.session_id
    cluster = state.cluster
    queue = cast(core_queue.ClioCoreQueue, state.queue)
    remote_execution = state.remote_execution
    stop_worker = state.stop_worker
    cancel_jobs = state.cancel_jobs
    cancel_scheduler_jobs = state.cancel_scheduler_jobs
    canonical_report_path = state.canonical_report_path
    seed_report = state.seed_report

    initial_status_error: str | None = None
    try:
        pre_teardown_status = session_lifecycle.status_remote_session(
            definition=definition,
            session_id=session_id,
        )
    except (JSONDecodeError, RelayError) as exc:
        initial_status_error = f"{type(exc).__name__}: {exc}"
        pre_teardown_status = {}
    recovery_status: OwnedSessionRecoveryStatus | None = None
    recovery_resource: ValidationResource | None = None
    try:
        session_generation_id = cli_owned_session_recovery._verified_owner_session_generation(
            pre_teardown_status,
            session_id=session_id,
        )
    except RelayError:
        session_generation_id = ""
    if not session_generation_id or pre_teardown_status.get("running") is not True:
        recovery_status = cli_owned_session_recovery._owned_session_recovery_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            cluster=cluster,
            session_id=session_id,
        )
        recovery_resource = cli_owned_session_recovery._owner_session_recovery_validation_resource(
            recovery_status
        )
        if initial_status_error is not None:
            recovery_resource.metadata["initial_status_error"] = initial_status_error
        seed_report.resources.append(recovery_resource)
        state.canonical_report = seed_report
        validation_report_module.write_validation_report(seed_report, canonical_report_path)
        session_generation_id = (
            cli_owned_session_recovery._verified_recovered_owner_session_generation(
                recovery_status,
                cluster=cluster,
                session_id=session_id,
            )
        )
        pre_teardown_status = {
            "owner": recovery_status.owner,
            "session_id": recovery_status.session_id,
            "session_generation_id": recovery_status.session_generation_id,
            "api_pid": recovery_status.api_pid,
            "process_start_ticks": recovery_status.process_start_marker,
            "running": recovery_status.running,
            "ownership_verified": recovery_status.ownership_verified,
            "process_absence_verified": recovery_status.process_absence_verified,
            "process_state": recovery_status.process_state,
        }
    requested_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    state.session_generation_id = session_generation_id
    state.pre_teardown_status = pre_teardown_status
    state.recovery_status = recovery_status
    state.recovery_resource = recovery_resource
    state.requested_policy = requested_policy

    finalized_retry_report: SessionLifecycleReport | None = None
    finalized_retry_reference = None
    if (
        recovery_status is not None
        and recovery_status.cleanup_receipt
        and recovery_status.coordinator_report_bound
    ):
        retrieved_report = session_lifecycle.read_remote_session_cleanup_report(
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            status=recovery_status,
        )
        finalized_retry_report = cli_cleanup_report._verified_finalized_cleanup_report(
            recovery_status,
            report=retrieved_report,
            cluster=cluster,
            session_id=session_id,
            expected_generation_id=session_generation_id,
            expected_cleanup_policy=requested_policy,
        )
        finalized_retry_reference = recovery_status.coordinator_report_ref
    local_admission_session_id = _desktop_owner_session_admission_id(
        cluster=cluster,
        session_id=session_id,
    )
    if remote_execution:
        _assert_no_unscoped_desktop_admission_state(
            queue,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )
    authoritative_admission = cli_owned_relay_jobs._owner_session_admission_status(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        session_generation_id=session_generation_id,
    )
    local_cleanup_intent = queue.get_owner_session_cleanup_intent(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    cleanup_operation_id = cli_owned_relay_jobs._select_owner_session_cleanup_operation(
        authoritative_status=authoritative_admission,
        local_intent=local_cleanup_intent,
        session_id=session_id,
        session_generation_id=session_generation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    state.local_admission_session_id = local_admission_session_id
    state.cleanup_operation_id = cleanup_operation_id

    if finalized_retry_report is not None:
        if recovery_status is None or finalized_retry_reference is None:
            raise RelayError("finalized cleanup retry lost its exact report reference")
        finalized_retry_report = cli_cleanup_report._verified_finalized_cleanup_report(
            recovery_status,
            report=finalized_retry_report,
            cluster=cluster,
            session_id=session_id,
            expected_generation_id=session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            expected_cleanup_policy=requested_policy,
        )
        local_cleanup_artifact = cli_owned_report_artifact._persist_local_cleanup_report_artifact(
            finalized_retry_report,
            validation_report_path=canonical_report_path,
            evidence_lock=state.active_evidence_lock,
        )
        cli_session_teardown_report._checkpoint_finalized_cleanup_artifact(
            state,
            finalized_retry_report,
            recovery=recovery_status,
            local_artifact=local_cleanup_artifact,
        )
        cli_cleanup_evidence._verify_cleanup_evidence_lock(
            state.active_evidence_lock,
            expected_parent=cli_cleanup_evidence._cleanup_evidence_state_parent(),
        )
        cli_owned_session_recovery._mark_owner_session_closed(
            queue=queue,
            definition=definition,
            cluster=cluster,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            legacy_unversioned_job_ids=[],
            finalized_recovery=recovery_status,
            finalized_report=finalized_retry_report,
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
        ):
            raise RelayError("finalized cleanup retry was not authoritatively closed after commit")
        if closed_recovery.coordinator_report_ref != finalized_retry_reference:
            raise RelayError("finalized cleanup report reference changed during closure")
        # iowarp/clio-relay#276 B1: an already-finalized retry that closes here
        # is still a clean teardown from the durable-record's point of view --
        # retire it the same as the primary finalize path does.
        clear_owned_session_record(cluster)
        closed_report = cli_cleanup_report._verified_finalized_cleanup_report(
            closed_recovery,
            report=finalized_retry_report,
            cluster=cluster,
            session_id=session_id,
            expected_generation_id=session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            expected_cleanup_policy=requested_policy,
        )
        if session_lifecycle_report_sha256(closed_report) != session_lifecycle_report_sha256(
            finalized_retry_report
        ):
            raise RelayError("finalized cleanup report reference changed during closure")
        state.recovery_status = closed_recovery
        state.recovery_resource = (
            cli_owned_session_recovery._owner_session_recovery_validation_resource(closed_recovery)
        )
        cli_session_teardown_report._emit_finalized_retry_report(
            state,
            finalized_retry_report,
            recovery=closed_recovery,
            local_artifact=local_cleanup_artifact,
        )
        return True
    return False
