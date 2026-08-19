"""The ``queue`` maintenance command group (iowarp/clio-relay#231).

``src/clio_relay/cli_queue.py``'s own docstring explains the fifteen
``queue_app`` commands' extraction off ``cli.py``. This module owns the
five **maintenance** commands (cleanup-stale/retention-plan/
retention-status/retention-collect/validate -- terminal-job retention and
the acceptance validation check), registered onto ``cli_queue.queue_app``
via ``@cli_queue.queue_app.command(...)``, the same two-file-one-Typer
pattern ``cli_cluster.py``'s registry/deployment split and ``cli_job.py``'s
lifecycle/records split established.

**Domain logic stays where it lives.** The commands below delegate to
``queue_management.cleanup_stale_jobs``, ``retention.
TerminalRetentionCoordinator``, ``queue_validation.
run_queue_management_validation``, and ``scheduler_providers.
validation_provider_for_scheduler`` exactly as they did inside ``cli.py``.
``storage_runtime.storage_managed_queue`` and ``remote_cli.
should_execute_on_cluster``/``run_remote_clio`` are module-attribute
imported since they are audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``); each is still used by several other
groups, so their ``caller`` entry stays ``"cli"``.

**Reassigned patch-seam callers.** ``queue_validation.
run_queue_management_validation`` and ``scheduler_providers.
validation_provider_for_scheduler`` had exactly one call site in the whole
of ``cli.py`` -- ``queue_validate`` itself (the scheduler group's own two
call sites moved to ``cli_scheduler.py`` in an earlier slice, leaving this
one the last). This slice reassigns both collaborators' ``caller`` entry in
``AUDITED_COLLABORATORS`` from ``"cli"`` to ``"cli_queue_maintenance"`` and
registers this module in ``_GUARDED_CALLERS``, the same bookkeeping this
campaign already did for ``cli_api.py``/``cli_release.py``/
``cli_endpoint.py``/``cli_scheduler.py``/``cli_job.py``.

**What moves here as a private helper, and why.** ``_optional_datetime``
had both of its call sites inside this five-command group (retention-plan,
retention-collect) -- unlike the cross-cutting helpers left in ``cli.py``
(``_require_cluster``, ``_run_or_exit``, ``_write_failed_acceptance_report``,
``_managed_queue_from_env``, ``_parse_age_seconds`` -- the last also used by
this module's sibling, ``cli_queue.py``). Single-caller-group helpers are
domain logic for this group, not shared plumbing, the same reasoning
``cli_api.py``, ``cli_endpoint.py``, and ``cli_cluster.py`` already
document.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_queue as cli_queue
import clio_relay.cli_support as cli_support
import clio_relay.core_queue as core_queue
import clio_relay.queue_validation as queue_validation
import clio_relay.remote_cli as remote_cli
import clio_relay.scheduler_providers as scheduler_providers
import clio_relay.storage_runtime as storage_runtime
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.models import JobKind
from clio_relay.queue_management import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_SCAN_LIMIT,
    cleanup_stale_jobs,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    ValidationStatus,
    default_report_path,
    sha256_file,
)


def _optional_datetime(value: str | None) -> datetime | None:
    """Parse an optional strict ISO-8601 timestamp for optimistic concurrency."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("expected timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise typer.BadParameter("expected timestamp must include a timezone")
    return parsed


@cli_queue.queue_app.command("cleanup-stale")
def queue_cleanup_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose stale leases should be recovered.")],
    job_id: Annotated[
        str | None,
        typer.Option(
            help="Optional exact job; prevents neighboring stale jobs from being acted on."
        ),
    ] = None,
    max_attempts: Annotated[
        int,
        typer.Option(help="Maximum attempts before expired leased jobs fail instead of requeue."),
    ] = 3,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    cancel_queued: Annotated[
        bool,
        typer.Option(help="Explicitly cancel queued jobs older than the threshold."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(help="Preview recoverable jobs without changing state."),
    ] = True,
    limit: Annotated[int, typer.Option(help="Maximum jobs acted on.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Preview or recover stale jobs; queued cancellation is explicit and relay-only."""
    import clio_relay.cli as cli

    args = [
        "queue",
        "cleanup-stale",
        "--cluster",
        cluster,
        "--max-attempts",
        str(max_attempts),
        "--older-than",
        older_than,
        "--limit",
        str(limit),
        "--scan-limit",
        str(scan_limit),
    ]
    if job_id is not None:
        args.extend(["--job-id", job_id])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if cancel_queued:
        args.append("--cancel-queued")
    args.append("--dry-run" if dry_run else "--no-dry-run")
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = cli._managed_queue_from_env()
    try:
        result = cleanup_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=cli._parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            max_attempts=max_attempts,
            dry_run=dry_run,
            cancel_queued=cancel_queued,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@cli_queue.queue_app.command("retention-plan")
def queue_retention_plan(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Build a read-only terminal-job retention plan."""
    import clio_relay.cli as cli

    args = ["queue", "retention-plan", job_id]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    settings = RelaySettings.from_env()
    coordinator = TerminalRetentionCoordinator(
        core_queue.ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    )
    plan = coordinator.plan(
        job_id,
        expected_updated_at=_optional_datetime(expected_updated_at),
    )
    typer.echo(
        json.dumps(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@cli_queue.queue_app.command("retention-status")
def queue_retention_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read the current crash-resumable retention phase without mutation."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(cluster, ["queue", "retention-status", job_id]):
        return
    settings = RelaySettings.from_env()
    plan = TerminalRetentionCoordinator(
        core_queue.ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    ).plan(job_id)
    typer.echo(
        json.dumps(
            {
                "job_id": job_id,
                "receipt_id": plan.receipt_id,
                "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
                "complete": plan.receipt_phase is not None
                and plan.receipt_phase.value == "complete",
                "eligible": plan.eligible,
                "protections": plan.protections,
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@cli_queue.queue_app.command("retention-collect")
def queue_retention_collect(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to collect over SSH."),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute/--dry-run",
            help="Advance retention; dry-run is the default and never mutates.",
        ),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(help="Maximum bounded retention actions.", min=1, max=100),
    ] = 100,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Preview or advance terminal retention without scheduler cancellation."""
    import clio_relay.cli as cli

    args = [
        "queue",
        "retention-collect",
        job_id,
        "--execute" if execute else "--dry-run",
        "--batch-size",
        str(batch_size),
    ]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return

    def action() -> None:
        settings = RelaySettings.from_env()
        queue: core_queue.ClioCoreQueue = (
            storage_runtime.storage_managed_queue(settings)
            if execute
            else core_queue.ClioCoreQueue(settings.core_dir)
        )
        result = TerminalRetentionCoordinator(queue, settings.spool_dir).collect(
            job_id,
            execute=execute,
            batch_size=batch_size,
            expected_updated_at=_optional_datetime(expected_updated_at),
        )
        typer.echo(result.model_dump_json(indent=2))

    cli._run_or_exit(action)


@cli_queue.queue_app.command("validate")
@cli_support._acceptance_report_command
def queue_validate(
    cluster: Annotated[str, typer.Option(help="Cluster containing the live worker service.")],
    job_id: Annotated[
        str | None,
        typer.Argument(help="Optional expendable queued compatibility anchor."),
    ] = None,
    kind: Annotated[
        JobKind,
        typer.Option(help="Controlled process kind; 1.0 live validation requires jarvis."),
    ] = JobKind.JARVIS,
    older_than: Annotated[
        str,
        typer.Option(help="Age that makes the queued test job stale, such as 1m or 2h."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
    provider: Annotated[
        str | None,
        typer.Option(
            "--scheduler-provider",
            help="Explicit provider for the bounded scheduler-preservation fixture.",
        ),
    ] = None,
    scheduler_run_seconds: Annotated[
        int,
        typer.Option(help="Bounded scheduler fixture runtime after release.", min=5, max=300),
    ] = 5,
    scheduler_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time for each scheduler fixture transition.", min=0.1, max=600),
    ] = 120.0,
    scheduler_poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler fixture polling interval.", min=0.01, max=10),
    ] = 1.0,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical JSON report path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Acceptance launcher identity, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Acceptance install source override, such as pypi."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional exact wheel whose SHA-256 binds the acceptance report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_artifact_sha256: Annotated[
        str | None,
        typer.Option(hidden=True),
    ] = None,
    report_json_only: Annotated[
        bool,
        typer.Option(hidden=True),
    ] = False,
) -> None:
    """Validate real bounded queue admission, cleanup, and scheduler preservation."""
    import clio_relay.cli as cli

    resolved_report = report or default_report_path(cluster)
    artifact_sha256 = validation_artifact_sha256 or (
        sha256_file(validation_artifact) if validation_artifact is not None else None
    )

    def action() -> None:
        definition = cli._require_cluster(cluster)
        selected_provider = provider or definition.scheduler_provider
        if remote_cli.should_execute_on_cluster(definition):
            args = [
                "queue",
                "validate",
                "--cluster",
                cluster,
                "--kind",
                kind.value,
                "--older-than",
                older_than,
                "--scan-limit",
                str(scan_limit),
                "--scheduler-provider",
                selected_provider,
                "--scheduler-run-seconds",
                str(scheduler_run_seconds),
                "--scheduler-timeout-seconds",
                str(scheduler_timeout_seconds),
                "--scheduler-poll-seconds",
                str(scheduler_poll_seconds),
                "--report-json-only",
            ]
            if job_id is not None:
                args.insert(2, job_id)
            if validation_launcher is not None:
                args.extend(["--validation-launcher", validation_launcher])
            if validation_install_source is not None:
                args.extend(["--validation-install-source", validation_install_source])
            if artifact_sha256 is not None:
                args.extend(["--validation-artifact-sha256", artifact_sha256])
            canonical = LiveValidationReport.model_validate_json(
                remote_cli.run_remote_clio(definition, args).strip()
            )
            cli._write_remote_verified_report(canonical, definition, resolved_report)
            if markdown_report is not None:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        else:
            canonical = queue_validation.run_queue_management_validation(
                cli._managed_queue_from_env(),
                job_id=job_id,
                cluster=cluster,
                kind=kind,
                older_than_seconds=cli._parse_age_seconds(older_than),
                scan_limit=scan_limit,
                scheduler_provider=scheduler_providers.validation_provider_for_scheduler(
                    selected_provider
                ),
                scheduler_run_seconds=scheduler_run_seconds,
                scheduler_timeout_seconds=scheduler_timeout_seconds,
                scheduler_poll_seconds=scheduler_poll_seconds,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact_sha256=artifact_sha256,
            )
            if not report_json_only:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        if report_json_only:
            typer.echo(canonical.model_dump_json(indent=2))
            return
        typer.echo(f"validation.status={canonical.status.value}")
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(canonical.model_dump_json(indent=2))
        if canonical.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    try:
        action()
    except typer.Exit:
        raise
    except BaseException as exc:
        if not report_json_only:
            cli._write_failed_acceptance_report(
                path=resolved_report,
                scenario="queue-management",
                cluster=cluster,
                check_id="queue.completed",
                summary="complete queue-management acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
            )
        raise
