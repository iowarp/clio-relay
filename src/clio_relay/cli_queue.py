"""The ``queue`` core-management command group (iowarp/clio-relay#231).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)). ``queue_app``
has fifteen commands spanning 838 body lines -- past the 800-line new-file
cap (SS2 ground rule 6), so it splits by real seam rather than forcing all
fifteen into one file: this module owns the ten **core-management**
commands (list/owner-jobs/migrate-indexes/migration-status/readiness-info/
repair-lease-indexes/audit-lease-capacity/diagnose/stale/cancel -- everyday
read/repair/cancel operations) and the canonical ``queue_app`` Typer
instance; the five **maintenance** commands (cleanup-stale/retention-plan/
retention-status/retention-collect/validate -- terminal-job retention and
the acceptance validation check) live in ``src/clio_relay/
cli_queue_maintenance.py``, registered onto this module's ``queue_app`` via
``@cli_queue.queue_app.command(...)``, the same two-file-one-Typer pattern
``cli_cluster.py``'s registry/deployment split and ``cli_job.py``'s
lifecycle/records split established.

**Domain logic stays where it lives.** The commands below delegate to
``core_queue.ClioCoreQueue`` and ``queue_management.list_queue_jobs``/
``discover_stale_jobs``/``diagnose_job``/``cancel_queue_job`` exactly as
they did inside ``cli.py`` -- already-correct owner modules.
``core_queue.ClioCoreQueue`` is module-attribute imported since it is an
audited patch-seam collaborator (``tests/test_cli_patch_seam.py``), still
used by many other groups, so its ``caller`` entry stays ``"cli"``. None of
the ``queue_management`` functions used here are audited, so they are
imported directly, matching ``cli.py``'s own prior style.

**What does NOT move here.** ``_try_remote_cluster_passthrough``,
``_parse_age_seconds`` (also used by this group's sibling maintenance
module), and ``_managed_queue_from_env`` are cross-cutting ``cli.py``
helpers used beyond this group -- moving them here would just relocate SS2
ground rule 2's violation, not fix it. They stay in ``cli.py`` and are
reached through the same import-cycle discipline ``cli_relay_host.py``
established.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from typing import Annotated

import typer

import clio_relay.core_queue as core_queue
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.models import JobKind, JobState
from clio_relay.queue_management import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    diagnose_job,
    discover_stale_jobs,
    list_queue_jobs,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
queue_app = typer.Typer(no_args_is_help=True)


@queue_app.command("list")
def queue_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    state: Annotated[
        JobState | None,
        typer.Option(help="Optional job state filter."),
    ] = None,
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include succeeded, failed, and canceled jobs."),
    ] = False,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global job source cursor.", min=1),
    ] = 1,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """List relay queue jobs."""
    import clio_relay.cli as cli

    args = ["queue", "list"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if state is not None:
        args.extend(["--state", state.value])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if include_terminal:
        args.append("--include-terminal")
    args.extend(
        [
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
    )
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = list_queue_jobs(
            queue,
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("owner-jobs")
def queue_owner_jobs(
    owner_session_id: Annotated[
        str,
        typer.Option(help="Exact owner session id."),
    ],
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owner session generation; omit only for legacy membership."),
    ] = None,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include terminal generation members."),
    ] = False,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque owner-session membership cursor."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum source records returned.", min=1, max=500)] = (
        500
    ),
) -> None:
    """List one generation's durable job membership without global history."""
    import clio_relay.cli as cli

    args = [
        "queue",
        "owner-jobs",
        "--owner-session-id",
        owner_session_id,
        "--limit",
        str(limit),
    ]
    if owner_session_generation_id is not None:
        args.extend(["--owner-session-generation-id", owner_session_generation_id])
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if include_terminal:
        args.append("--include-terminal")
    if cursor is not None:
        args.extend(["--cursor", cursor])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=owner_session_generation_id,
            cursor=cursor,
            limit=limit,
            cluster=cluster,
            include_terminal=include_terminal,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        json.dumps(
            {
                "jobs": [job.model_dump(mode="json") for job in jobs],
                "owner_session_id": owner_session_id,
                "owner_session_generation_id": owner_session_generation_id,
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_window_count": source_window_count,
            },
            indent=2,
        )
    )


@queue_app.command("migrate-indexes")
def queue_migrate_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to migrate over SSH, or local storage."),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            help="Maximum flat records parsed in each crash-safe batch.", min=1, max=10_000
        ),
    ] = 500,
    all_batches: Annotated[
        bool,
        typer.Option("--all", help="Run bounded batches until migration completes."),
    ] = False,
) -> None:
    """Build v1 active and per-job indexes for an existing v0.9 queue."""
    import clio_relay.cli as cli

    args = ["queue", "migrate-indexes", "--batch-size", str(batch_size)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if all_batches:
        args.append("--all")
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.migrate_indexes_batch(batch_size=batch_size)
        while all_batches and result.get("complete") is not True:
            result = queue.migrate_indexes_batch(batch_size=batch_size)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("migration-status")
def queue_migration_status(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local storage."),
    ] = None,
) -> None:
    """Read the crash-safe queue index migration checkpoint without mutation."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(cluster, ["queue", "migration-status"]):
        return
    status_payload = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).index_migration_status()
    typer.echo(json.dumps(status_payload, indent=2))


@queue_app.command("readiness-info")
def queue_readiness_info(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local storage."),
    ] = None,
) -> None:
    """Verify the sealed fixed queue layout without initialization or repair."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(cluster, ["queue", "readiness-info"]):
        return
    try:
        payload = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).readiness_info()
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(payload, indent=2))


@queue_app.command("repair-lease-indexes")
def queue_repair_lease_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to repair over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases rebuilt in the crash-safe repair.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Rebuild and prune exact endpoint, kind, identity, and expiry lease indexes."""
    import clio_relay.cli as cli

    args = ["queue", "repair-lease-indexes", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.repair_lease_operational_indexes(limit=limit)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("audit-lease-capacity")
def queue_audit_lease_capacity(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to audit over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases and index records audited.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Audit canonical leases, exact indexes, and the O(1) capacity aggregate."""
    import clio_relay.cli as cli

    args = ["queue", "audit-lease-capacity", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    report = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).audit_lease_capacity(
        limit=limit
    )
    typer.echo(json.dumps(report, indent=2))
    if report.get("valid") is not True:
        raise typer.Exit(code=1)


@queue_app.command("diagnose")
def queue_diagnose(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """Explain why one exact relay job is not progressing."""
    import clio_relay.cli as cli

    args = [
        "queue",
        "diagnose",
        job_id,
        "--older-than",
        older_than,
        "--scan-limit",
        str(scan_limit),
    ]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = diagnose_job(
            queue,
            job_id,
            cluster=cluster,
            stale_after_seconds=cli._parse_age_seconds(older_than),
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("stale")
def queue_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose active jobs should be inspected.")],
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional exact job to inspect without acting on neighboring jobs."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Discover stale relay jobs without changing queue or scheduler state."""
    import clio_relay.cli as cli

    args = [
        "queue",
        "stale",
        "--cluster",
        cluster,
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
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    try:
        result = discover_stale_jobs(
            core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir),
            cluster=cluster,
            older_than_seconds=cli._parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("cancel")
def queue_cancel(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Request scheduler cancellation for already-submitted remote work.",
        ),
    ] = False,
) -> None:
    """Cancel a relay job with explicit scheduler policy."""
    import clio_relay.cli as cli

    args = ["queue", "cancel", job_id]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    args.append("--cancel-scheduler-job" if cancel_scheduler_job else "--keep-scheduler-job")
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = cli._managed_queue_from_env()
    try:
        result = cancel_queue_job(
            queue,
            job_id,
            cluster=cluster,
            scheduler_policy="request-scheduler" if cancel_scheduler_job else "relay-only",
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))
