"""Shared mutable state for the ``session teardown`` command
(iowarp/clio-relay#231 continuation, ``cli_session_teardown.py`` split).

``session teardown``'s body is one continuous imperative procedure --
recovery/status resolution, admission quiesce, owned-job listing and
cancellation, the coordinator teardown call, and final closure -- that
used to read and write roughly twenty enclosing local variables (the
evidence lock, the mutable ``canonical_report`` cell, cluster/session
identity, the requested cleanup policy, and every value one phase
computes for a later one to consume) through nested Python closures.
Splitting those phases into standalone top-level functions in their own
owner modules means those closures' free variables become explicit
attributes on one shared, mutable context object instead --
:class:`_TeardownState` below. Every phase function
(``cli_session_teardown_recovery``, ``cli_session_teardown_jobs``,
``cli_session_teardown_finalize``, ``cli_session_teardown_report``)
takes the same ``state`` object as its first argument, reads what an
earlier phase produced, and writes what a later phase will need, in
exactly the sequence the original nested function body executed. No
field here changes what value gets computed or when; only where the
value lives between one read and the next.

:func:`_persist_verified_cleanup_report_before_closure` has no closure
state of its own -- it was already a standalone, fully explicit-
parameter function -- so it moves here verbatim as this split's other
genuinely primitive piece.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.core_queue as core_queue
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
    session_lifecycle_report_sha256,
)
from clio_relay.validation_report import LiveValidationReport, ValidationResource


@dataclass
class _TeardownState:
    """One owner session's ``session teardown`` run, threaded through every phase.

    The first block of fields is set once by the facade before the action
    runs. The remaining fields start at an inert default and are filled in,
    in order, by the action's own setup and then by each phase in turn --
    a phase never reads a field before the phase responsible for it (named
    in each block's comment) has run.
    """

    # Command inputs -- set once by the facade before the action runs.
    cluster: str
    session_id: str
    stop_worker: bool
    cancel_jobs: bool
    cancel_scheduler_jobs: bool
    scheduler_sentinel_ids: tuple[str, ...]
    relay_cancel_timeout_seconds: float
    relay_cancel_poll_seconds: float
    validation_launcher: str | None
    validation_install_source: str | None
    validation_artifact: Path | None
    canonical_report_path: Path
    seed_report: LiveValidationReport
    active_evidence_lock: cli_cleanup_evidence._CleanupEvidenceLock
    definition: ClusterDefinition

    # The mutable "current canonical report" cell (was a one-element list
    # shared by closure in the pre-split module).
    canonical_report: LiveValidationReport | None = None

    # Set by the action's own setup, before any phase runs.
    remote_execution: bool = False
    queue: core_queue.ClioCoreQueue | None = None
    cleanup_worker_info: dict[str, object] | None = None
    cleanup_worker_error: Exception | None = None

    # Produced by the recovery phase; consumed by the jobs and finalize phases.
    session_generation_id: str = ""
    pre_teardown_status: dict[str, object] = field(default_factory=dict)
    recovery_status: OwnedSessionRecoveryStatus | None = None
    recovery_resource: ValidationResource | None = None
    requested_policy: dict[str, bool] = field(default_factory=dict)
    local_admission_session_id: str = ""
    cleanup_operation_id: str = ""

    # Produced by the jobs phase; consumed by the finalize phase.
    cleanup_intent: dict[str, object] = field(default_factory=dict)
    owned_jobs: list[cli_owned_relay_jobs._OwnedRelayJob] = field(default_factory=list)
    canceled: list[str] = field(default_factory=list)
    scheduler_sentinel_pre_phases: dict[str, str] = field(default_factory=dict)
    gateway_reports: list[dict[str, object]] = field(default_factory=list)


def _persist_verified_cleanup_report_before_closure(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    report: SessionLifecycleReport,
) -> tuple[SessionLifecycleReport, OwnedSessionRecoveryStatus]:
    """Persist, re-read, and verify the immutable full cleanup report."""
    cleanup_operation_id = report.cleanup_operation_id
    if cleanup_operation_id is None:
        raise RelayError("coordinator cleanup report omitted its operation id")
    finalized_status = session_lifecycle.finalize_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=report.cleanup_policy,
        report=report,
    )
    retrieved_report = session_lifecycle.read_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        status=finalized_status,
    )
    finalized_report = cli_cleanup_report._verified_finalized_cleanup_report(
        finalized_status,
        report=retrieved_report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=report.cleanup_policy,
    )
    if session_lifecycle_report_sha256(finalized_report) != session_lifecycle_report_sha256(report):
        raise RelayError("re-read coordinator cleanup report changed before closure")
    return finalized_report, finalized_status
