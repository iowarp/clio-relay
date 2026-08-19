"""The ``scheduler`` command group (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)): the eleven
``scheduler_app`` commands (scheduler-job status/cancel, batch status,
allocation-connector-step lifecycle, held-validation submit/release, and
the deterministic lifecycle acceptance check) move out of the monolith into
their own capped module, per ground rule 2 (SS2) -- ``cli.py`` parses and
renders only; this module does the same for its own eleven commands and
nothing more.

**Domain logic stays where it lives.** The commands below delegate to
``scheduler_providers.provider_for_scheduler``/
``allocation_connector_provider_for_scheduler``/
``validation_provider_for_scheduler`` and
``scheduler_validation.run_scheduler_lifecycle_validation`` exactly as they
did inside ``cli.py`` -- already-correct owner modules, module-attribute
imported since all four are audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``). ``remote_cli.should_execute_on_cluster``
is the same, imported the same way for the same reason.

**Reassigned patch-seam callers.**
``allocation_connector_provider_for_scheduler`` and
``run_scheduler_lifecycle_validation`` had every one of their call sites
inside this exact eleven-command group -- unlike
``scheduler_providers.provider_for_scheduler`` (used by three more call
sites elsewhere in ``cli.py``, stays ``"cli"``) and
``validation_provider_for_scheduler`` (also used by ``queue validate``,
stays ``"cli"``). This slice reassigns those two collaborators' ``caller``
entry in ``AUDITED_COLLABORATORS`` from ``"cli"`` to ``"cli_scheduler"`` and
registers this module in ``_GUARDED_CALLERS``, the same bookkeeping this
campaign already did for ``cli_api.py``, ``cli_release.py``, and
``cli_endpoint.py``.

**What does NOT move here.** ``_require_cluster``, ``_run_remote_or_exit``,
``_run_or_exit``, ``_write_failed_acceptance_report``,
``MAX_SCHEDULER_STATUS_BATCH`` (also used by a job-status batching helper
elsewhere), and ``_remote_worker_info`` (used by session/live-acceptance
paths too) are cross-cutting ``cli.py`` state/helpers used beyond this
group -- moving them here would just relocate SS2 ground rule 2's
violation, not fix it. They stay in ``cli.py`` and are reached through the
same import-cycle discipline ``cli_relay_host.py`` established.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_support as cli_support
import clio_relay.remote_cli as remote_cli
import clio_relay.scheduler_providers as scheduler_providers
import clio_relay.scheduler_validation as scheduler_validation
import clio_relay.validation_report as validation_report_module
from clio_relay.installation import attach_verified_worker_identity
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    ValidationStatus,
    default_report_path,
    sha256_file,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
scheduler_app = typer.Typer(no_args_is_help=True)


@scheduler_app.command("status")
def scheduler_status_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Read and normalize one scheduler job through the configured provider."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "status",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return
    cli._run_or_exit(
        lambda: typer.echo(
            scheduler_providers.provider_for_scheduler(selected)
            .poll(scheduler_job_id)
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("status-batch", hidden=True)
def scheduler_status_batch_command(
    scheduler_job_ids: Annotated[
        list[str],
        typer.Option(
            "--scheduler-job-id",
            help="Exact scheduler job identity to query; repeat for a bounded batch.",
        ),
    ],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Read a bounded exact scheduler batch through one cluster invocation."""
    import clio_relay.cli as cli

    if not scheduler_job_ids or len(scheduler_job_ids) > cli.MAX_SCHEDULER_STATUS_BATCH:
        raise typer.BadParameter(f"provide 1-{cli.MAX_SCHEDULER_STATUS_BATCH} scheduler job ids")
    if len(set(scheduler_job_ids)) != len(scheduler_job_ids):
        raise typer.BadParameter("scheduler job ids cannot contain duplicates")
    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = ["scheduler", "status-batch", "--cluster", cluster, "--provider", selected]
    for scheduler_job_id in scheduler_job_ids:
        args.extend(["--scheduler-job-id", scheduler_job_id])
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        scheduler = scheduler_providers.provider_for_scheduler(selected)
        statuses = [
            scheduler.poll(scheduler_job_id).model_dump(mode="json")
            for scheduler_job_id in scheduler_job_ids
        ]
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-status-batch.v1",
                    "scheduler": selected,
                    "statuses": statuses,
                },
                indent=2,
            )
        )

    cli._run_or_exit(action)


@scheduler_app.command("cancel")
def scheduler_cancel_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Explicitly request cancellation of one scheduler job through its provider."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "cancel",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = scheduler_providers.provider_for_scheduler(selected).cancel(scheduler_job_id)
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "cancel_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    cli._run_or_exit(action)


@scheduler_app.command("connector-placement", hidden=True)
def scheduler_connector_placement_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Resolve one provider-verified host for an allocation-scoped connector."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-placement",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return
    cli._run_or_exit(
        lambda: typer.echo(
            scheduler_providers.allocation_connector_provider_for_scheduler(selected)
            .connector_placement(scheduler_job_id)
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command(
    "connector-step-start",
    hidden=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def scheduler_connector_step_start_command(
    ctx: typer.Context,
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Crash-reconciliation marker for the connector step."),
    ],
    output_path: Annotated[
        str,
        typer.Option(help="Absolute cluster-side connector output path."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Launch one asynchronous provider-owned connector step."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    connector_command = list(ctx.args)
    if connector_command and connector_command[0] == "--":
        connector_command = connector_command[1:]
    args = [
        "scheduler",
        "connector-step-start",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
        "--output-path",
        output_path,
        "--",
        *connector_command,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return
    cli._run_or_exit(
        lambda: typer.echo(
            scheduler_providers.allocation_connector_provider_for_scheduler(selected)
            .launch_connector_step(
                scheduler_job_id,
                placement_host=placement_host,
                step_marker=step_marker,
                command=connector_command,
                output_path=output_path,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-status", hidden=True)
def scheduler_connector_step_status_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Observe one exact allocation connector step."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-status",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return
    cli._run_or_exit(
        lambda: typer.echo(
            scheduler_providers.allocation_connector_provider_for_scheduler(selected)
            .poll_connector_step(
                scheduler_job_id,
                scheduler_step_id=scheduler_step_id,
                placement_host=placement_host,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-cancel", hidden=True)
def scheduler_connector_step_cancel_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Cancel one exact connector step without canceling its allocation."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-cancel",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = scheduler_providers.allocation_connector_provider_for_scheduler(
            selected
        ).cancel_connector_step(
            scheduler_job_id,
            scheduler_step_id=scheduler_step_id,
        )
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "scheduler_step_id": scheduler_step_id,
                    "cancel_requested": True,
                    "accepted": result.returncode == 0,
                    "returncode": result.returncode,
                    "stdout": result.stdout.strip(),
                    "stderr": result.stderr.strip(),
                },
                indent=2,
            )
        )
        if result.returncode != 0:
            raise typer.Exit(code=1)

    cli._run_or_exit(action)


@scheduler_app.command("connector-step-reconcile", hidden=True)
def scheduler_connector_step_reconcile_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Exact connector step reconciliation marker."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Find an interrupted connector launch by exact provider marker."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-reconcile",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        step = scheduler_providers.allocation_connector_provider_for_scheduler(
            selected
        ).find_connector_step(
            scheduler_job_id,
            step_marker=step_marker,
            placement_host=placement_host,
        )
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-reconciliation.v1",
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "step_marker": step_marker,
                    "placement_host": placement_host,
                    "found": step is not None,
                    "step": step.model_dump(mode="json") if step is not None else None,
                },
                indent=2,
            )
        )

    cli._run_or_exit(action)


@scheduler_app.command("submit-held-validation", hidden=True)
def scheduler_submit_held_validation(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    job_name: Annotated[str, typer.Option(help="Unique bounded validation job name.")],
    run_seconds: Annotated[int, typer.Option(help="Bounded sleep duration.")] = 30,
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Submit one held provider-owned validation job."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "submit-held-validation",
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--job-name",
        job_name,
        "--run-seconds",
        str(run_seconds),
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        scheduler_job_id = scheduler_providers.validation_provider_for_scheduler(
            selected
        ).submit_held_validation_job(job_name=job_name, run_seconds=run_seconds)
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "held": True,
                    "owned_validation_job": True,
                },
                indent=2,
            )
        )

    cli._run_or_exit(action)


@scheduler_app.command("release-validation", hidden=True)
def scheduler_release_validation(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Release one exact held validation job."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "release-validation",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if remote_cli.should_execute_on_cluster(definition):
        cli._run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = scheduler_providers.validation_provider_for_scheduler(
            selected
        ).release_validation_job(scheduler_job_id)
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "release_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    cli._run_or_exit(action)


@scheduler_app.command("validate-lifecycle")
@cli_support._acceptance_report_command
def scheduler_validate_lifecycle(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
    run_seconds: Annotated[
        int,
        typer.Option(help="Bounded validation job runtime in seconds."),
    ] = 30,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Timeout for each required lifecycle phase."),
    ] = 180.0,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler polling interval."),
    ] = 1.0,
    report_path: Annotated[
        Path | None,
        typer.Option("--report", help="Canonical scheduler lifecycle JSON path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional Markdown rendering of the JSON report."),
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
            help="Optional wheel whose SHA-256 is recorded in scheduler evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Deterministically validate held-to-completed scheduler lifecycle semantics."""
    import clio_relay.cli as cli

    resolved_report = report_path or default_report_path(cluster)
    try:
        definition = cli._require_cluster(cluster)
        selected = provider or definition.scheduler_provider
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=resolved_report,
            scenario="scheduler-lifecycle",
            cluster=cluster,
            check_id="scheduler.preflight",
            summary="validate scheduler lifecycle acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    canonical_report: list[LiveValidationReport | None] = [None]

    def action() -> None:
        report = scheduler_validation.run_scheduler_lifecycle_validation(
            cluster=cluster,
            definition=definition,
            provider=selected,
            run_seconds=run_seconds,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = report
        if remote_cli.should_execute_on_cluster(definition):
            try:
                attach_verified_worker_identity(
                    report,
                    cli._remote_worker_info(definition),
                )
            except BaseException as exc:
                recorder = ValidationRecorder(report)
                recorder.record_failure(
                    "worker.identity",
                    "verify exact cluster worker artifact identity",
                    exc,
                )
                recorder.finish(exc)
                validation_report_module.write_validation_report(report, resolved_report)
                raise
        validation_report_module.write_validation_report(report, resolved_report)
        if markdown_report is not None:
            ValidationRecorder(report).write(resolved_report, markdown_report)
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(report.model_dump_json(indent=2))
        if report.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            cli._write_failed_acceptance_report(
                path=resolved_report,
                scenario="scheduler-lifecycle",
                cluster=cluster,
                check_id="scheduler.completed",
                summary="complete scheduler lifecycle acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    cli._run_or_exit(guarded_action)
