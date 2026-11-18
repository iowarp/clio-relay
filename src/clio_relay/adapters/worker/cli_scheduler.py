"""The ``scheduler`` command group: scheduler job status, batch status and cancel.

The commands delegate to ``scheduler.provider_for_scheduler`` (RL18, psij
backed) and run on the cluster through ``remote_cli`` when a cluster is
named. The allocation connector-step and held-validation commands went with
the old Slurm-only provider.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from typing import Annotated

import typer

import clio_relay.adapters.worker.scheduler as scheduler_providers
import clio_relay.remote_cli as remote_cli

# `cli` (`clio_relay.adapters.worker.cli`) is deliberately NOT imported at module level.
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
    import clio_relay.adapters.worker.cli as cli

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
    import clio_relay.adapters.worker.cli as cli
    import clio_relay.adapters.worker.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel

    if (
        not scheduler_job_ids
        or len(scheduler_job_ids) > cli_owned_scheduler_cancel.MAX_SCHEDULER_STATUS_BATCH
    ):
        raise typer.BadParameter(
            f"provide 1-{cli_owned_scheduler_cancel.MAX_SCHEDULER_STATUS_BATCH} scheduler job ids"
        )
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
    import clio_relay.adapters.worker.cli as cli

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


