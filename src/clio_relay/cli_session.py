"""The user-facing ``session`` command group (iowarp/clio-relay#231).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names ``session_app`` as the largest remaining ``cli.py`` sub-app: 20
commands spanning 3,489 lines. This module owns the canonical
``session_app = typer.Typer(...)`` instance plus the six commands a human or
script invokes directly to observe or drive an owned session's public
lifecycle: ``plan-start``, ``status``, ``start-status``, ``start-watch``,
``submit-jarvis``, ``detach``. The twelve ``hidden=True`` commands that exist
only as the coordinator/worker's own stdin-JSON internal handoff protocol
(``quiesce-intake``, ``admission-status``, ``recovery-status``,
``start-status-owned``, ``start-owned``, ``teardown-owned``,
``challenge-owned``, ``finalize-cleanup-owned``, ``read-cleanup-report-owned``,
``prepare-start``, ``resume-intake``, ``mark-closed``) are a separate,
thematically distinct concern -- they move to ``cli_session_owned.py``,
registered onto this module's ``session_app`` via
``@cli_session.session_app.command(...)``, the same two-file-one-Typer
pattern ``cli_queue.py``/``cli_queue_maintenance.py`` established.

**``start`` and ``teardown`` deliberately stay in ``cli.py``.** ``session_start``
is a small (96-line) command, but it sits beside 1,117 lines of exclusive
helper functions it and ``session_teardown`` share -- dominated by
``_persist_local_cleanup_report_artifact`` (812 lines by itself, an
atomic-verified-write utility with 13 nested closures). ``session_teardown``
itself is 1,368 lines -- the largest function in the file -- almost entirely
three levels of nested closures (``action()`` containing
``checkpoint_finalized_cleanup_artifact``, ``emit_completed_report``, and a
492-line ``emit_finalized_retry_report``, among others) that capture
``session_teardown``'s own locals by closure rather than taking explicit
parameters. ``scripts/check_file_size.py``'s ``DEFAULT_MAX_LINES`` (800) has
no grandfathering for a brand-new file, and neither function -- nor the
812-line helper alone -- fits under that cap without converting these
closures into parameterized top-level functions: a real behavioral-risk
refactor of the single most business-critical function in the codebase, not
a mechanical extraction. SS5's target owner-module map already carries
"Session orchestration (``cli.py``'s ``session_teardown``, ~1365 lines)...
not yet sequenced" as its own future row; this slice does not attempt it.
Both commands stay in ``cli.py``, re-decorated from ``@session_app.command(...)``
to ``@cli_session.session_app.command(...)`` -- ``cli.py`` becomes one of the
registrants onto the Typer instance this module now owns, exactly as
``cli_cluster.py``'s own docstring documents for its still-in-``cli.py``
deployment commands.

**Domain logic stays where it lives.** ``session_lifecycle``, ``session_api``,
and ``remote_cli`` are imported module-attribute style
(``import clio_relay.X as X``) because several of their symbols used here
(``session_lifecycle.detach_remote_session``, ``session_api.
submit_owned_session_job``, ``remote_cli.should_execute_on_cluster``,
``remote_cli.remote_command_timeout``) are audited patch-seam collaborators
(``tests/test_cli_patch_seam.py``) -- calling every symbol on those two
modules the same way, audited or not, avoids ever having to remember which
individual name needs the module-attribute form. ``detach``'s move is why
``session_lifecycle.detach_remote_session`` and ``session_api.
submit_owned_session_job`` (from ``submit-jarvis``) reassign caller from
``cli`` to ``cli_session`` in ``AUDITED_COLLABORATORS`` -- this group's move
left neither with any remaining ``cli.py`` call site.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator (the many private ``_owner_session_*``/cleanup
helpers ``detach`` calls, plus ``_require_cluster``/``_run_or_exit``/
``_write_failed_acceptance_report``). ``@cli_support._acceptance_report_command``
is applied straight from its true owner on ``detach`` (not through ``cli.py``)
for the same reason ``cli_relay_host.py`` documents: it is a real
attribute read at this module's own import time, so routing it through
``cli.py`` would recreate the cycle the function-local discipline exists to
avoid.
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
import clio_relay.session_api as session_api
import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.validation_report as validation_report_module
from clio_relay.config import RelaySettings
from clio_relay.dev_mode import VerificationFindings, dev_mode_enabled
from clio_relay.errors import RelayError
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationStatus,
    default_report_path,
    sha256_file,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
session_app = typer.Typer(no_args_is_help=True)


@session_app.command("plan-start")
def session_plan_start(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Plan replacement of an existing API."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Plan a token-protected remote API."),
    ] = True,
    start_operation_id: Annotated[
        str | None,
        typer.Option(help="Reuse an existing exact operation id; omitted mints one."),
    ] = None,
) -> None:
    """Emit a read-only exact plan that can survive loss of the start client."""
    import clio_relay.cli as cli
    import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
    import clio_relay.cli_session_start as cli_session_start

    definition = cli._require_cluster(cluster)
    settings = RelaySettings.from_env()

    def action() -> None:
        dev_mode_findings = VerificationFindings()
        release_identity = cli_session_start._verify_session_start_worker_release_identity(
            definition,
            dev_mode=dev_mode_enabled(cluster_dev_mode=definition.dev_mode),
            findings=dev_mode_findings,
        )
        cli_session_start._echo_dev_mode_findings(dev_mode_findings)
        typer.echo(
            session_lifecycle.plan_remote_session_start(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                replace=replace,
                require_token=require_token,
                input_policy=cli_cleanup_evidence._owned_session_input_policy(settings),
                start_operation_id=start_operation_id,
                expected_api_release_identity_sha256=release_identity.sha256(),
            ).model_dump_json(indent=2)
        )

    cli._run_or_exit(action)


@session_app.command("status")
def session_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
) -> None:
    """Inspect an owned remote relay API session."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: typer.echo(
            json.dumps(
                session_lifecycle.status_remote_session(
                    definition=definition, session_id=session_id
                ),
                indent=2,
            )
        )
    )


@session_app.command("start-status")
def session_start_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact planned start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact route revision from session plan-start."),
    ],
    remote_api_port: Annotated[int, typer.Option(help="Planned remote API port.")],
    expected_api_release_identity_sha256: Annotated[
        str,
        typer.Option(help="Exact release digest from session plan-start."),
    ],
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Planned replacement policy."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Planned API token policy."),
    ] = True,
) -> None:
    """Query one exact start once without imposing an aggregate wait deadline."""
    import clio_relay.cli as cli
    import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence

    definition = cli._require_cluster(cluster)
    settings = RelaySettings.from_env()

    def action() -> None:
        plan = session_lifecycle.plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            input_policy=cli_cleanup_evidence._owned_session_input_policy(settings),
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        typer.echo(
            session_lifecycle.query_remote_session_start(
                definition=definition, plan=plan
            ).model_dump_json(indent=2)
        )

    cli._run_or_exit(action)


@session_app.command("start-watch")
def session_start_watch(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact planned start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact route revision from session plan-start."),
    ],
    remote_api_port: Annotated[int, typer.Option(help="Planned remote API port.")],
    expected_api_release_identity_sha256: Annotated[
        str,
        typer.Option(help="Exact release digest from session plan-start."),
    ],
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Planned replacement policy."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Planned API token policy."),
    ] = True,
    timeout_seconds: Annotated[
        float,
        typer.Option(min=0.1, max=3600.0, help="Bounded aggregate watch duration."),
    ] = 120.0,
    poll_seconds: Annotated[
        float,
        typer.Option(min=0.05, max=60.0, help="Delay between exact status observations."),
    ] = 0.5,
) -> None:
    """Watch a durable handle; exit 0 is ready, 1 failed, and 2 detached."""
    import clio_relay.cli as cli
    import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence

    definition = cli._require_cluster(cluster)
    settings = RelaySettings.from_env()

    def action() -> None:
        plan = session_lifecycle.plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            input_policy=cli_cleanup_evidence._owned_session_input_policy(settings),
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        result = session_lifecycle.watch_remote_session_start(
            definition=definition,
            plan=plan,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        typer.echo(result.model_dump_json(indent=2))
        if result.state in {"failed", "not_current"}:
            raise typer.Exit(code=1)
        if not result.usable:
            raise typer.Exit(code=2)

    cli._run_or_exit(action)


@session_app.command("submit-jarvis")
def session_submit_jarvis(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    pipeline_yaml_file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Local JARVIS pipeline YAML file.",
        ),
    ],
    idempotency_key: Annotated[str, typer.Option(help="Durable submission identity.")],
    timeout_seconds: Annotated[
        float,
        typer.Option(min=1, max=300, help="Bounded session API transport timeout."),
    ] = 30,
) -> None:
    """Submit JARVIS through the identity-proven exact-generation session API."""
    import clio_relay.cli as cli

    settings = RelaySettings.from_env().model_copy(
        update={
            "owner_session_id": session_id,
            "owner_session_generation_id": session_generation_id,
            "owner_session_cluster": cluster,
        }
    )
    definition = cli._require_cluster(cluster)

    def action() -> None:
        job = session_api.submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/jarvis",
            payload={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml_file.read_text(encoding="utf-8"),
                "idempotency_key": idempotency_key,
            },
            timeout_seconds=timeout_seconds,
        )
        typer.echo(json.dumps(job.model_dump(mode="json"), indent=2))

    cli._run_or_exit(action)


@session_app.command("detach")
@cli_support._acceptance_report_command
def session_detach(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
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
    """Close the desktop attachment while retaining remote work and session processes."""
    import clio_relay.cli as cli
    import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
    import clio_relay.cli_owned_runtime_cleanup as cli_owned_runtime_cleanup
    import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
    import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
    import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach

    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = cli_remote_worker_attach._new_cleanup_acceptance_report(
        scenario="cleanup",
        cluster=cluster,
        mode="detach",
        resource_kind="owner_session",
        resource_id=session_id,
        action="detach",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    validation_report_module.write_validation_report(seed_report, canonical_report_path)
    try:
        definition = cli._require_cluster(cluster)
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="cleanup",
            cluster=cluster,
            check_id="session.detach.preflight",
            summary="validate owned session detach inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=canonical_report[0],
        )
        raise

    def action() -> None:
        remote_execution = remote_cli.should_execute_on_cluster(definition)
        queue = cli._managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = (
            cli_remote_worker_attach._observe_worker_before_cleanup(definition)
        )
        pre_detach_report = session_lifecycle.detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        pre_detach_canonical = pre_detach_report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = pre_detach_canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        session_generation_id = cli_owned_session_recovery._verified_owner_session_detach(
            pre_detach_report,
            session_id=session_id,
        )
        if remote_execution:
            owned_jobs = cli_owned_relay_jobs._list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            owned_jobs = cli_owned_relay_jobs._list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        gateway_reports = cli_owned_runtime_cleanup._cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="detach",
            cancel_scheduler_jobs=False,
        )
        if remote_execution:
            post_operation_jobs = cli_owned_relay_jobs._list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            post_operation_jobs = cli_owned_relay_jobs._list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        report = session_lifecycle.detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        try:
            cli_owned_session_recovery._verified_owner_session_detach(
                report,
                session_id=session_id,
                expected_session_generation_id=session_generation_id,
            )
        except RelayError as exc:
            detail = str(exc)
            if detail not in report.errors:
                report.errors.append(detail)
        report.resources.extend(
            cli_owned_scheduler_cancel._owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=False,
                cancel_scheduler_jobs=False,
                post_operation_jobs=post_operation_jobs,
            )
        )
        cli_owned_runtime_cleanup._merge_gateway_cleanup_resources(report, gateway_reports)
        payload = report.json_payload()
        payload["gateway_sessions"] = gateway_reports
        canonical = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        provenance_warning = cli_remote_worker_attach._write_cleanup_validation_report(
            canonical,
            definition,
            canonical_report_path,
            observed_worker_info=cleanup_worker_info,
            worker_observation_error=cleanup_worker_error,
        )
        payload["validation_report"] = str(canonical_report_path.resolve())
        payload["validation_status"] = canonical.status.value
        payload["validation_provenance_warning"] = provenance_warning
        typer.echo(cli._public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if payload.get("ok") is not True or (not canonical_ok and not provenance_warning):
            raise typer.Exit(code=1)

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
                check_id="session.detach",
                summary="detach owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
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

    cli._run_or_exit(locked_action)
