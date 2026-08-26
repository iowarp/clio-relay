"""The ``job`` durable-record command group (iowarp/clio-relay#231).

``src/clio_relay/cli_job.py``'s own docstring explains the seventeen
``job_app`` commands' extraction off ``cli.py``. At 969 lines that single
new module broke the 800-line new-file cap (SS2 ground rule 6), so it
splits by real seam rather than forcing all seventeen into one file:
``cli_job.py`` keeps the four job **lifecycle** commands (submit/
submit-pipeline/wait/cancel -- create, wait-for-terminal, and cancel a
job) and owns the canonical ``job_app`` Typer instance; this module owns
the thirteen **durable-record** commands (watch/monitor/status/tasks/
task-events/record-task-event/read-log/read-artifact/list-artifacts/
used-artifacts/used-by/progress/record-progress -- everything that reads
or appends a durable queue record) and registers onto
``cli_job.job_app`` via ``@cli_job.job_app.command(...)``, the same
two-file-one-Typer pattern ``cli_cluster.py``'s registry/deployment split
established.

**Domain logic stays where it lives.** The commands below delegate to
``core_queue.ClioCoreQueue`` and ``relay_ops.job_status`` exactly as they
did inside ``cli.py`` -- already-correct owner modules.
``core_queue.ClioCoreQueue`` is module-attribute imported since it is an
audited patch-seam collaborator (``tests/test_cli_patch_seam.py``), still
used by many other groups, so its ``caller`` entry stays ``"cli"``.
``relay_ops.job_status`` is the same idea for a non-audited helper (two
more call sites remain in ``cli.py``, unrelated to this group).

**What moves here as private helpers, and why.** ``_job_event_cursor`` and
``_record_page_payload`` had every one of their call sites inside this
thirteen-command group -- unlike the cross-cutting helpers left in
``cli.py`` (``_require_cluster``, ``_try_remote_cluster_passthrough``,
``_json_object``, ``_json_text_from_option``, each with call sites in
several other groups). Single-caller-group helpers are domain logic for
this group, not shared plumbing, the same reasoning ``cli_job.py`` and
``cli_cluster.py`` already document.

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
from pathlib import Path
from typing import Annotated, cast

import typer

import clio_relay.cli_job as cli_job
import clio_relay.core_queue as core_queue
import clio_relay.relay_ops as relay_ops
from clio_relay.bounded_payload import is_delivery_refusal
from clio_relay.config import RelaySettings
from clio_relay.jarvis_execution_artifacts import resolve_jarvis_run_owner_by_execution_id
from clio_relay.models import Cursor, ProgressRecord, TaskEventStatus, TaskTimelineEvent
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.relay_ops import monitor_job, read_artifact_bytes, read_job_log


def _job_event_cursor(cursor: int) -> int:
    """Normalize CLI event cursors while preserving the durable cursor contract."""
    if cursor < 0:
        raise typer.BadParameter("cursor must be greater than or equal to 0")
    return 1 if cursor == 0 else cursor


def _record_page_payload(
    record_key: str,
    records: list[dict[str, object]],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Build the shared one-based collection response used by CLI surfaces."""
    return {
        record_key: records,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


@cli_job.job_app.command("watch")
def job_watch(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job events from a cursor."""
    import clio_relay.cli as cli

    cursor = _job_event_cursor(cursor)
    if cli._try_remote_cluster_passthrough(
        cluster,
        ["job", "watch", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    for event in events:
        typer.echo(f"{event.seq} {event.created_at.isoformat()} {event.event_type} {event.message}")
    typer.echo(f"next_cursor={next_cursor.next_seq}")


@cli_job.job_app.command("monitor")
def job_monitor(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job state and event stream data from a cursor as JSON."""
    import clio_relay.cli as cli

    cursor = _job_event_cursor(cursor)
    if cli._try_remote_cluster_passthrough(
        cluster,
        ["job", "monitor", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    result = monitor_job(
        core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir),
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@cli_job.job_app.command("status")
def job_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read job, relay queue, and scheduler status as JSON."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(cluster, ["job", "status", job_id]):
        return
    result = relay_ops.job_status(
        core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir), job_id
    )
    typer.echo(json.dumps(result, indent=2))


@cli_job.job_app.command("tasks")
def job_tasks(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based task record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum task records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of durable task records for a job as JSON."""
    import clio_relay.cli as cli

    args = [
        "job",
        "tasks",
        job_id,
        "--cursor",
        str(cursor),
        "--limit",
        str(limit),
    ]
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    tasks, next_cursor, total = queue.list_tasks_page(
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            _record_page_payload(
                "tasks",
                [task.model_dump(mode="json") for task in tasks],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@cli_job.job_app.command("task-events")
def job_task_events(
    task_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="First task event sequence to read.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(help="Maximum task events to read.", min=1),
    ] = 100,
) -> None:
    """Read structured task timeline events from a cursor as JSON."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(
        cluster,
        ["job", "task-events", task_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    events, next_cursor = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).drain_task_events(
        task_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            {
                "events": [event.model_dump(mode="json") for event in events],
                "next_cursor": next_cursor,
            },
            indent=2,
        )
    )


@cli_job.job_app.command("record-task-event")
def job_record_task_event(
    task_id: str,
    event_type: Annotated[str, typer.Option(help="Structured task event type.")],
    label: Annotated[str, typer.Option(help="Short UI step label.")],
    summary: Annotated[str, typer.Option(help="Short event summary.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to record the event over SSH."),
    ] = None,
    status: Annotated[
        TaskEventStatus,
        typer.Option(help="Task step status."),
    ] = TaskEventStatus.RUNNING,
    detail: Annotated[str | None, typer.Option(help="Optional detail text.")] = None,
    path_ref: Annotated[
        list[str] | None,
        typer.Option(help="Path reference; repeat for multiple paths."),
    ] = None,
    artifact_ref: Annotated[
        list[str] | None,
        typer.Option(help="Artifact reference; repeat for multiple artifacts."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this task event."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Record a structured task timeline event."""
    import clio_relay.cli as cli

    metadata_source = cli._json_text_from_option(metadata_json, metadata_json_file)
    remote_args = [
        "job",
        "record-task-event",
        task_id,
        "--event-type",
        event_type,
        "--label",
        label,
        "--summary",
        summary,
        "--status",
        status.value,
        "--metadata-json",
        metadata_source,
    ]
    if detail is not None:
        remote_args.extend(["--detail", detail])
    for value in path_ref or []:
        remote_args.extend(["--path-ref", value])
    for value in artifact_ref or []:
        remote_args.extend(["--artifact-ref", value])
    if cli._try_remote_cluster_passthrough(cluster, remote_args):
        return
    event = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).append_task_event(
        TaskTimelineEvent(
            task_id=task_id,
            event_type=event_type,
            label=label,
            status=status,
            summary=summary,
            detail=detail,
            path_refs=path_ref or [],
            artifact_refs=artifact_ref or [],
            metadata=cli._json_object(metadata_source),
        )
    )
    typer.echo(event.model_dump_json(indent=2))


@cli_job.job_app.command("read-log")
def job_read_log(
    job_id: str,
    stream: Annotated[str, typer.Option(help="stdout or stderr.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    offset: Annotated[int, typer.Option(help="Byte offset.")] = 0,
    limit: Annotated[int, typer.Option(help="Maximum bytes.")] = 65536,
) -> None:
    """Read stdout or stderr from a job log by byte offset."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "read-log",
            job_id,
            "--stream",
            stream,
            "--offset",
            str(offset),
            "--limit",
            str(limit),
        ],
    ):
        return
    settings = RelaySettings.from_env()
    queue = core_queue.ClioCoreQueue(settings.core_dir)
    if stream not in {"stdout", "stderr"}:
        raise typer.BadParameter("--stream must be stdout or stderr")
    result = read_job_log(
        settings,
        queue.get_job(job_id),
        stream_name="stdout" if stream == "stdout" else "stderr",
        offset=offset,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@cli_job.job_app.command("read-artifact")
def job_read_artifact(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read an artifact payload as base64 JSON."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(cluster, ["job", "read-artifact", artifact_id]):
        return
    result = read_artifact_bytes(
        core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir), artifact_id
    )
    typer.echo(json.dumps(result, indent=2))
    if is_delivery_refusal(result):
        # F6 (#231 R6 review): a T2 refusal (doc SS6.4) is not a successful
        # read -- exit 1 so scripts piping this command's exit code (not
        # just grepping its stdout) still observe the failure, instead of
        # a silent 0 alongside a body that says result_available: false.
        raise typer.Exit(code=1)


@cli_job.job_app.command("list-artifacts")
def job_list_artifacts(
    job_id: Annotated[
        str | None,
        typer.Argument(help="Relay job id. Alternative to --execution-id; pass exactly one."),
    ] = None,
    execution_id: Annotated[
        str | None,
        typer.Option(
            help=(
                "JARVIS execution id (from jarvis_run/jarvis_get_execution). Alternative "
                "to JOB_ID; pass exactly one."
            ),
        ),
    ] = None,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based artifact record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of artifact references for a job as JSON.

    clio-relay#278: pass either JOB_ID or ``--execution-id`` (the id
    ``jarvis_run``/``jarvis_get_execution`` hand back), never both or
    neither -- an execution id resolves to its owning job through the same
    ``resolve_jarvis_run_owner_by_execution_id`` the door's execution-scoped
    route and the MCP tool's ``execution_id`` branch both use.
    """
    import clio_relay.cli as cli

    if (job_id is None) == (execution_id is None):
        raise typer.BadParameter(
            "artifact_scope_ambiguous: pass exactly one of JOB_ID or --execution-id"
        )
    remote_args = ["job", "list-artifacts"]
    if job_id is not None:
        remote_args.append(job_id)
    else:
        remote_args.extend(["--execution-id", cast(str, execution_id)])
    remote_args.extend(["--cursor", str(cursor), "--limit", str(limit)])
    if cli._try_remote_cluster_passthrough(cluster, remote_args):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    resolved_job_id = (
        job_id
        if job_id is not None
        else resolve_jarvis_run_owner_by_execution_id(
            queue, cast(str, execution_id), cluster=cluster
        ).job_id
    )
    artifacts, next_cursor, total = queue.list_artifacts_page(
        resolved_job_id, cursor=cursor, limit=limit
    )
    typer.echo(
        json.dumps(
            _record_page_payload(
                "artifacts",
                [artifact.model_dump(mode="json") for artifact in artifacts],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@cli_job.job_app.command("used-artifacts")
def job_used_artifacts(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Artifact ID cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum used-artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List content-pinned artifacts consumed by a job as JSON."""
    import clio_relay.cli as cli

    remote_args = ["job", "used-artifacts", job_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if cli._try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_used_artifacts_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_artifacts": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@cli_job.job_app.command("used-by")
def job_used_by(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque edge cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum consuming-job records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List jobs that consumed a content-pinned artifact as JSON."""
    import clio_relay.cli as cli

    remote_args = ["job", "used-by", artifact_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if cli._try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_artifact_users_page(artifact_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_by": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@cli_job.job_app.command("progress")
def job_progress(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based progress record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum progress records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of structured progress observations as JSON."""
    import clio_relay.cli as cli

    if cli._try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "progress",
            job_id,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
        ],
    ):
        return
    progress, next_cursor, total = core_queue.ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_progress_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            _record_page_payload(
                "progress",
                [item.model_dump(mode="json") for item in progress],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@cli_job.job_app.command("record-progress")
def job_record_progress(
    job_id: str,
    label: Annotated[str, typer.Option(help="Progress label.")] = "progress",
    current: Annotated[float | None, typer.Option(help="Current progress value.")] = None,
    total: Annotated[float | None, typer.Option(help="Total progress value.")] = None,
    unit: Annotated[str | None, typer.Option(help="Progress unit.")] = None,
    message: Annotated[str | None, typer.Option(help="Human-readable progress message.")] = None,
    source_event_seq: Annotated[
        int | None,
        typer.Option(help="Source event sequence for this progress observation."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this observation."),
    ] = "{}",
) -> None:
    """Record a structured progress observation for a job."""
    import clio_relay.cli as cli

    metadata = external_progress_metadata("external_cli", cli._json_object(metadata_json))
    progress = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).append_progress(
        ProgressRecord(
            job_id=job_id,
            label=label,
            current=current,
            total=total,
            unit=unit,
            message=message,
            source_event_seq=source_event_seq,
            metadata=metadata,
        )
    )
    typer.echo(progress.model_dump_json(indent=2))
