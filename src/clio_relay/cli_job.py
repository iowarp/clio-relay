"""The ``job`` lifecycle command group (iowarp/clio-relay#231).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names one command-module extraction per ``cli.py`` typer sub-app, following
the ``relay-host``/``cli_relay_host.py`` precedent (R8(ii)). ``job_app`` has
seventeen commands spanning 969 body lines -- past the 800-line new-file cap
(SS2 ground rule 6), so it splits by real seam rather than forcing all
seventeen into one file: this module owns the four **lifecycle** commands
(submit/submit-pipeline/wait/cancel -- create, wait-for-terminal, and cancel
a job) and the canonical ``job_app`` Typer instance; the thirteen
**durable-record** commands (watch/monitor/status/tasks/task-events/
record-task-event/read-log/read-artifact/list-artifacts/used-artifacts/
used-by/progress/record-progress) live in ``src/clio_relay/
cli_job_records.py``, registered onto this module's ``job_app`` via
``@cli_job.job_app.command(...)``, the same two-file-one-Typer pattern
``cli_cluster.py``'s registry/deployment split established.

**Domain logic stays where it lives.** The commands below delegate to
``core_queue.ClioCoreQueue``, ``remote_cli.should_execute_on_cluster``, and
``relay_ops.observe_until_terminal`` exactly as they did inside ``cli.py``
-- already-correct owner modules, module-attribute imported since all three
are audited patch-seam collaborators (``tests/test_cli_patch_seam.py``).
``relay_ops.job_wait_result`` and ``remote_cli.stage_jarvis_yaml`` are the
same idea for the non-audited helpers this group's commands call.

**Reassigned patch-seam caller.** ``relay_ops.observe_until_terminal`` had
exactly one call site in the whole of ``cli.py`` -- ``job_wait`` itself --
unlike ``core_queue.ClioCoreQueue``/``remote_cli.should_execute_on_cluster``
(used by many other groups, stay ``"cli"``). This slice reassigns
``observe_until_terminal``'s ``caller`` entry in ``AUDITED_COLLABORATORS``
from ``"cli"`` to ``"cli_job"`` and registers this module in
``_GUARDED_CALLERS``, the same bookkeeping this campaign already did for
``cli_api.py``/``cli_release.py``/``cli_endpoint.py``/``cli_scheduler.py``.

**What moves here as private helpers, and why.**
``_with_exclusive_scheduler``, ``_file_idempotency_key``, and
``_try_remote_job_wait_passthrough`` (with its own exclusive collaborator,
``REMOTE_JOB_WAIT_STATUS_TIMEOUT_SECONDS``) had every one of their call
sites inside this group -- unlike the cross-cutting helpers left in
``cli.py`` (``_require_cluster``, ``_run_or_exit``, ``_run_remote_or_exit``,
``_try_remote_cluster_passthrough``, ``_submit_managed_job``,
``_managed_queue_from_env``, ``_json_output``, and the artifact-use
helpers, each with call sites in several other groups).
Single-caller-group helpers are domain logic for this group, not shared
plumbing, the same reasoning ``cli_api.py``, ``cli_endpoint.py``, and
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

import hashlib
import math
import os
from pathlib import Path
from typing import Annotated, cast

import typer
import yaml
from pydantic import ValidationError

import clio_relay.core_queue as core_queue
import clio_relay.relay_ops as relay_ops
import clio_relay.remote_cli as remote_cli
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    JobWaitResult,
    RelayJob,
)
from clio_relay.relay_ops import cancel_job as request_cancel_job
from clio_relay.relay_ops import (
    job_wait_result,
)
from clio_relay.session_api import OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS

# Exclusive to `_try_remote_job_wait_passthrough` below -- moved with its
# only caller, see this module's own docstring.
REMOTE_JOB_WAIT_STATUS_TIMEOUT_SECONDS = 30.0

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
job_app = typer.Typer(no_args_is_help=True)


def _with_exclusive_scheduler(pipeline_yaml: str, scheduler_provider: str) -> str:
    loaded = yaml.safe_load(pipeline_yaml)
    if not isinstance(loaded, dict):
        raise ConfigurationError("JARVIS YAML must be an object to request exclusive allocation")
    document = cast(dict[str, object], loaded)
    scheduler = document.get("scheduler")
    if scheduler is None:
        if scheduler_provider == "external":
            raise ConfigurationError(
                "--exclusive requires an explicit scheduler provider in the cluster definition"
            )
        scheduler = {"name": scheduler_provider}
    if not isinstance(scheduler, dict):
        raise ConfigurationError("scheduler must be an object to request exclusive allocation")
    typed_scheduler = cast(dict[str, object], scheduler)
    typed_scheduler.setdefault("name", scheduler_provider)
    typed_scheduler["exclusive"] = True
    document["scheduler"] = typed_scheduler
    return yaml.safe_dump(document, sort_keys=False)


def _file_idempotency_key(path: Path, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"jarvis:{path.resolve()}:{digest}"


def _try_remote_job_wait_passthrough(
    cluster: str | None,
    *,
    job_id: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> bool:
    """Run one bounded remote wait and preserve its durable receipt on observation expiry."""
    import clio_relay.cli as cli

    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = cli._require_cluster(cluster)
    if not remote_cli.should_execute_on_cluster(definition):
        return False
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise typer.BadParameter("timeout-seconds must be positive and finite")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise typer.BadParameter("poll-seconds must be positive and finite")

    def action() -> None:
        try:
            with remote_cli.remote_command_timeout(
                timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
            ):
                payload = remote_cli.run_remote_clio(
                    definition,
                    [
                        "job",
                        "wait",
                        job_id,
                        "--timeout-seconds",
                        str(timeout_seconds),
                        "--poll-seconds",
                        str(poll_seconds),
                    ],
                )
            document = cli._json_output(payload, "remote job wait")
            if "observation" in document:
                try:
                    result = JobWaitResult.model_validate(document)
                except ValidationError as exc:
                    raise RelayError("remote job wait returned an invalid result") from exc
            else:
                result = job_wait_result(
                    RelayJob.model_validate(document),
                    timeout_seconds=timeout_seconds,
                )
        except ObservationTimeoutError as observation_error:
            with remote_cli.remote_command_timeout(REMOTE_JOB_WAIT_STATUS_TIMEOUT_SECONDS):
                status = cli._json_output(
                    remote_cli.run_remote_clio(definition, ["job", "status", job_id]),
                    "remote job status after bounded wait",
                )
            job = RelayJob.model_validate(status.get("job"))
            terminal = job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
            if status.get("terminal") is not terminal:
                raise RelayError(
                    "remote job status disagrees with its durable job state"
                ) from observation_error
            result = job_wait_result(
                job,
                timeout_seconds=timeout_seconds,
            )

        if result.job_id != job_id or result.cluster != cluster:
            raise RelayError("remote job wait returned a different durable receipt")
        typer.echo(result.model_dump_json(indent=2))

    cli._run_or_exit(action)
    return True


@job_app.command("submit")
def job_submit(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[Path, typer.Option(help="Path to JARVIS YAML.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
    exclusive: Annotated[
        bool,
        typer.Option("--exclusive/--shared", help="Request exclusive scheduler allocation."),
    ] = False,
) -> None:
    """Submit a JARVIS pipeline job."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    if exclusive:
        yaml_text = _with_exclusive_scheduler(yaml_text, definition.scheduler_provider)
    artifact_uses = cli._artifact_use_refs(used_artifact)
    key = idempotency_key or (
        _file_idempotency_key(jarvis_yaml, yaml_text)
        + cli._artifact_use_idempotency_suffix(artifact_uses)
    )
    if remote_cli.should_execute_on_cluster(definition):
        remote_yaml = remote_cli.stage_jarvis_yaml(
            definition,
            jarvis_yaml=jarvis_yaml,
            pipeline_yaml_text=yaml_text,
            idempotency_key=key,
        )
        remote_command = [
            "job",
            "submit",
            "--cluster",
            cluster,
            "--jarvis-yaml",
            remote_yaml,
            "--idempotency-key",
            key,
            "--exclusive" if exclusive else "--shared",
        ]
        for ref in cli._artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", cli._artifact_use_cli_value(ref)])
        cli._run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_yaml=yaml_text),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = cli._submit_managed_job(job)
    typer.echo(saved.job_id)


@job_app.command("submit-pipeline")
def job_submit_pipeline(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    pipeline_name: Annotated[str, typer.Option(help="Existing JARVIS pipeline name.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit an existing JARVIS pipeline by name on the target cluster."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    artifact_uses = cli._artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"jarvis-pipeline:{cluster}:{pipeline_name}"
        + cli._artifact_use_idempotency_suffix(artifact_uses)
    )
    if remote_cli.should_execute_on_cluster(definition):
        remote_command = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            pipeline_name,
            "--idempotency-key",
            key,
        ]
        for ref in cli._artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", cli._artifact_use_cli_value(ref)])
        cli._run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name=pipeline_name),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = cli._submit_managed_job(job)
    typer.echo(saved.job_id)


@job_app.command("wait")
def job_wait(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds for this terminal-state observation."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Observe until terminal, returning current durable state when the bound expires."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise typer.BadParameter("timeout-seconds must be positive and finite")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise typer.BadParameter("poll-seconds must be positive and finite")
    if _try_remote_job_wait_passthrough(
        cluster,
        job_id=job_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    ):
        return
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    job = relay_ops.observe_until_terminal(
        queue,
        job_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    typer.echo(job.model_dump_json(indent=2))


@job_app.command("cancel")
def job_cancel(
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
    """Cancel a queued or running job."""
    import clio_relay.cli as cli

    args = ["job", "cancel", job_id]
    if cancel_scheduler_job:
        args.append("--cancel-scheduler-job")
    if cli._try_remote_cluster_passthrough(cluster, args):
        return
    job = request_cancel_job(
        cli._managed_queue_from_env(),
        job_id,
        cancel_scheduler=cancel_scheduler_job,
    )
    typer.echo(f"{job.job_id} {job.state.value}")
