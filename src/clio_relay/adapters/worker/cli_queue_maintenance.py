"""The ``queue`` maintenance commands: cleanup-stale and terminal-job retention.

They register onto ``cli_queue.queue_app`` and delegate to
``queue_management.cleanup_stale_jobs`` and
``retention.TerminalRetentionCoordinator``. ``cli`` is imported inside each
command body, never at module level, to keep the import cycle open.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated

import typer

import clio_relay.adapters.worker.cli_queue as cli_queue
import clio_relay.adapters.worker.core_queue as core_queue
import clio_relay.adapters.worker.storage_runtime as storage_runtime
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.models import JobKind
from clio_relay.adapters.worker.queue_management import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_SCAN_LIMIT,
    cleanup_stale_jobs,
)
from clio_relay.adapters.worker.retention import TerminalRetentionCoordinator


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
    import clio_relay.adapters.worker.cli as cli

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
    import clio_relay.adapters.worker.cli as cli

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
    import clio_relay.adapters.worker.cli as cli

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
    import clio_relay.adapters.worker.cli as cli

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
