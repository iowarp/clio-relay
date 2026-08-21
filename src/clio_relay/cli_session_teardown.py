"""The ``session teardown`` command (iowarp/clio-relay#231 continuation).

This used to be the single largest command body in clio-relay: a deeply
nested closure factory (``action`` -> ``checkpoint_finalized_cleanup_
artifact``/``emit_completed_report``/``emit_finalized_retry_report`` ->
``guarded_action`` -> ``locked_action``) that threaded ~20 enclosing
local variables (the evidence lock, the mutable ``canonical_report``
cell, cluster/session identity, the requested cleanup policy) through
its inner functions via Python closures rather than explicit
parameters.

This module is now a thin facade. The command's Typer signature and its
preflight (evidence lock, seed report, cluster/policy validation) stay
here -- they are irreducibly part of the decorated command function --
along with ``guarded_action``/``locked_action``, which just wrap and
run the ``action`` callable ``cli_session_teardown_action`` builds.
Everything ``action`` used to do inline now lives in its own owner
module, each taking the shared, mutable
:class:`~clio_relay.cli_session_teardown_state._TeardownState` in place
of the closures' free variables:

- ``cli_session_teardown_state``: the state object itself, plus
  ``_persist_verified_cleanup_report_before_closure`` (already a fully
  explicit-parameter function, so it moved verbatim).
- ``cli_session_teardown_recovery``: resolve owner-session identity and
  recovery status; finish an already-finalized retry in full.
- ``cli_session_teardown_jobs``: quiesce admission, list/cancel owned
  relay jobs, preflight scheduler preservation sentinels.
- ``cli_session_teardown_finalize``: the coordinator teardown call,
  post-cancel reconciliation, verification, and authoritative closure.
- ``cli_session_teardown_report``: the three report-emission functions
  (durable checkpoint, completed report, finalized-retry report).
- ``cli_session_teardown_action``: assembles the above into the
  ``action`` callable this module's command hands to ``guarded_action``.

``_persist_verified_cleanup_report_before_closure`` is re-exported here
verbatim under its original name: ``tests/test_cli.py`` monkeypatches
it as an attribute of this module, and ``cli_session_teardown_finalize``
reaches it back through this module (a deferred import, to avoid the
cycle) rather than calling ``cli_session_teardown_state`` directly, so
that patch keeps taking effect.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.cli_session as cli_session
import clio_relay.cli_session_teardown_action as cli_session_teardown_action
import clio_relay.cli_session_teardown_state as cli_session_teardown_state
import clio_relay.cli_support as cli_support
import clio_relay.remote_cli as remote_cli
import clio_relay.validation_report as validation_report_module
from clio_relay.validation_report import (
    LiveValidationReport,
    default_report_path,
)

# Re-exported verbatim under its original name: tests/test_cli.py
# monkeypatches it as an attribute of *this* module, and
# cli_session_teardown_finalize reaches it back through this module (a
# deferred import, to avoid the cycle) rather than calling
# cli_session_teardown_state directly, so that patch keeps taking effect.
_persist_verified_cleanup_report_before_closure = (
    cli_session_teardown_state._persist_verified_cleanup_report_before_closure
)

DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS = 30.0


DEFAULT_RELAY_CANCEL_POLL_SECONDS = 0.25


MAX_RELAY_CANCEL_TIMEOUT_SECONDS = 3_600.0


@cli_session.session_app.command("teardown")
@cli_support._acceptance_report_command
def session_teardown(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    stop_worker: Annotated[
        bool,
        typer.Option(help="Also stop the persistent cluster worker service for this cluster."),
    ] = False,
    cancel_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-jobs/--keep-jobs",
            help="Cancel active relay jobs. The safe default leaves all jobs running.",
        ),
    ] = False,
    cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-jobs/--keep-scheduler-jobs",
            help="Also request scheduler cancellation for canceled relay jobs.",
        ),
    ] = False,
    preserve_scheduler_job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--preserve-scheduler-job-id",
            help=(
                "Unrelated active scheduler job id that must remain uncanceled; repeat for "
                "multiple live-gate sentinels. Requires --cancel-jobs and "
                "--cancel-scheduler-jobs."
            ),
        ),
    ] = None,
    relay_cancel_timeout_seconds: Annotated[
        float,
        typer.Option(
            help="Maximum wait for worker-acknowledged relay cancellation cleanup.",
            min=0.01,
            max=MAX_RELAY_CANCEL_TIMEOUT_SECONDS,
        ),
    ] = DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS,
    relay_cancel_poll_seconds: Annotated[
        float,
        typer.Option(
            help="Polling interval while awaiting relay cancellation acknowledgment.",
            min=0.01,
            max=60.0,
        ),
    ] = DEFAULT_RELAY_CANCEL_POLL_SECONDS,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned remote relay session processes, optionally stopping the worker service."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    evidence_lock: cli_cleanup_evidence._CleanupEvidenceLock | None = None
    try:
        evidence_lock = cli_cleanup_evidence._acquire_cleanup_evidence_lock()
        seed_report = cli_remote_worker_attach._new_cleanup_acceptance_report(
            scenario="cleanup",
            cluster=cluster,
            mode="teardown",
            resource_kind="owner_session",
            resource_id=session_id,
            action="teardown",
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            stop_worker=stop_worker,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        canonical_report: list[LiveValidationReport | None] = [seed_report]
        validation_report_module.write_validation_report(seed_report, canonical_report_path)
    except BaseException:
        cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)
        raise
    active_evidence_lock = evidence_lock
    try:
        definition = cli._require_cluster(cluster)
        scheduler_sentinel_ids = cli_owned_scheduler_cancel._normalize_scheduler_sentinel_ids(
            preserve_scheduler_job_ids or []
        )
        if cancel_scheduler_jobs and not cancel_jobs:
            raise typer.BadParameter(
                "--cancel-scheduler-jobs requires the separate --cancel-jobs flag"
            )
        if scheduler_sentinel_ids and not (cancel_jobs and cancel_scheduler_jobs):
            raise typer.BadParameter(
                "--preserve-scheduler-job-id requires both --cancel-jobs and "
                "--cancel-scheduler-jobs"
            )
    except BaseException as exc:
        try:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown.preflight",
                summary="validate owned session teardown inputs",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
        finally:
            cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)
        raise

    state = cli_session_teardown_state._TeardownState(
        cluster=cluster,
        session_id=session_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        scheduler_sentinel_ids=scheduler_sentinel_ids,
        relay_cancel_timeout_seconds=relay_cancel_timeout_seconds,
        relay_cancel_poll_seconds=relay_cancel_poll_seconds,
        validation_launcher=validation_launcher,
        validation_install_source=validation_install_source,
        validation_artifact=validation_artifact,
        canonical_report_path=canonical_report_path,
        seed_report=seed_report,
        active_evidence_lock=active_evidence_lock,
        definition=definition,
        canonical_report=canonical_report[0],
    )
    action = cli_session_teardown_action.build_teardown_action(state)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown",
                summary="teardown owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=state.canonical_report,
            )
            raise

    def locked_action() -> None:
        with (
            remote_cli.remote_command_timeout(
                cli_owned_relay_jobs.REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS
            ),
            cli._session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    try:
        cli._run_or_exit(locked_action)
    finally:
        cli_cleanup_evidence._release_cleanup_evidence_lock(evidence_lock)
