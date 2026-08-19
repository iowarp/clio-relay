"""The ``gateway`` runtime-lifecycle commands (iowarp/clio-relay#231).

Sibling of ``cli_gateway.py`` (see that module's own docstring for the full
seam rationale, including why ``start-runtime`` landed here rather than
alongside the CRUD five): this module owns all seven runtime-lifecycle
commands -- ``start-runtime``/``resume-runtime``/``browser-attach``/
``browser-detach``/``detach-runtime``/``attach-runtime``/``stop-runtime`` --
registered onto ``cli_gateway.py``'s canonical ``gateway_app`` Typer instance
via ``@cli_gateway.gateway_app.command(...)``, the same two-file-one-Typer
pattern ``cli_queue_maintenance.py`` and ``cli_session_owned.py`` established
for their own sibling files.

**Domain logic stays where it lives.** ``owner_session_admission.
owner_session_gateway_admission`` is an audited patch-seam collaborator
(``tests/test_cli_patch_seam.py``) whose only two ``cli.py`` call sites were
``start-runtime`` and ``resume-runtime`` -- both here now that ``start-runtime``
moved, so ``AUDITED_COLLABORATORS`` reassigns its caller from ``cli`` to
``cli_gateway_runtime`` alone. Every other collaborator this module calls
(``service_runtime.ServiceRuntimeSupervisor``, ``storage_runtime.
storage_managed_queue``, ``session_lifecycle.status_remote_session``) keeps
additional call sites elsewhere in ``cli.py`` (mostly inside ``session_start``/
``session_teardown``, which stay put per ``cli_session.py``'s own docstring),
so those keep caller ``cli`` unchanged.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator. ``@cli_support._acceptance_report_command`` is
applied straight from its true owner on ``start-runtime``/``detach-runtime``/
``stop-runtime`` (not through ``cli.py``), the same discipline
``cli_relay_host.py``/``cli_session.py`` document.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import Annotated, cast

import typer

import clio_relay.cli_gateway as cli_gateway
import clio_relay.cli_support as cli_support
import clio_relay.owner_session_admission as owner_session_admission
import clio_relay.service_runtime as service_runtime
import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.storage_runtime as storage_runtime
import clio_relay.validation_report as validation_report_module
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import ServiceRuntimeSpec
from clio_relay.public_records import public_gateway_session
from clio_relay.service_runtime import ServiceRuntimePendingResult
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    ValidationStatus,
    default_report_path,
    load_validation_report,
    new_live_validation_report,
    sha256_file,
)


@cli_gateway.gateway_app.command("start-runtime")
@cli_support._acceptance_report_command
def gateway_start_runtime(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable runtime session name.")],
    runtime_json_file: Annotated[
        Path,
        typer.Option(help="Path to a generic ServiceRuntimeSpec JSON document."),
    ],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    owner_session_id: Annotated[
        str | None,
        typer.Option(help="Owned desktop relay session that controls this runtime."),
    ] = None,
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owned desktop relay session generation."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime validation JSON path. Defaults under .clio-relay."
        ),
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
            help="Optional wheel whose SHA-256 is recorded in gateway evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Start and bind a scheduler-backed streaming service runtime."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    report_id: list[str | None] = [None]

    def action() -> None:
        definition = cli._require_cluster(cluster)
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "--owner-session-id and --owner-session-generation-id must be provided together"
            )
        if not runtime_json_file.exists():
            raise ConfigurationError(f"runtime spec does not exist: {runtime_json_file}")
        spec = ServiceRuntimeSpec.model_validate_json(
            runtime_json_file.read_text(encoding="utf-8-sig")
        )
        settings = RelaySettings.from_env()
        queue = storage_runtime.storage_managed_queue(settings)
        supervisor = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=queue,
            cluster=cluster,
            definition=definition,
            token=cli._resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=cli._resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )

        if owner_session_id is None or owner_session_generation_id is None:
            result = supervisor.start(name=name, spec=spec)
        else:
            with owner_session_admission.owner_session_gateway_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
                transition_lock_factory=cli._session_transition_lock,
                session_status_reader=session_lifecycle.status_remote_session,
                admission_status_reader=cli._owner_session_admission_status,
            ) as admission:
                result = supervisor.start(
                    name=name,
                    spec=spec,
                    owner_session_id=admission.owner_session_id,
                    owner_session_generation_id=admission.owner_session_generation_id,
                    owner_session_admission_id=admission.owner_session_admission_id,
                )
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        report_id[0] = canonical.report_id
        if isinstance(result, ServiceRuntimePendingResult):
            # A nonterminal report cannot satisfy the release gate. Persist and
            # return its exact retry selector without adding another fallible
            # remote observation that could hide the already-durable result.
            validation_report_module.write_validation_report(canonical, canonical_report_path)
        else:
            cli._write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            payload["outcome"] = result.outcome
            payload["retry_selector"] = result.retry_selector()
            payload["scheduler_action"] = result.scheduler_action
            payload["relay_action"] = result.relay_action
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(cli._public_json(payload))

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            report_already_written = False
            if report_id[0] is not None:
                with suppress(ConfigurationError):
                    report_already_written = (
                        load_validation_report(canonical_report_path).report_id == report_id[0]
                    )
            if not report_already_written:
                artifact_sha256: str | None = None
                if validation_artifact is not None:
                    with suppress(OSError):
                        artifact_sha256 = sha256_file(validation_artifact)
                failed_report = new_live_validation_report(
                    scenario="gateway-runtime",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=artifact_sha256,
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "gateway.start-runtime",
                    "start scheduler-backed gateway runtime",
                    exc,
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    cli._run_or_exit(guarded_action)


@cli_gateway.gateway_app.command("resume-runtime")
@cli_support._acceptance_report_command
def gateway_resume_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime validation JSON path. Defaults under .clio-relay."
        ),
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
            help="Optional wheel whose SHA-256 is recorded in gateway evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Advance one exact submitted runtime without creating another scheduler job."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    report_id: list[str | None] = [None]

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        queue = storage_runtime.storage_managed_queue(settings)
        supervisor = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=queue,
            cluster=cluster,
            definition=definition,
            token=cli._resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=cli._resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        session = queue.get_gateway_session(session_id)
        owner_session_id = session.metadata.get("owner_session_id")
        owner_generation_id = session.metadata.get("owner_session_generation_id")
        owner_admission_id = session.metadata.get("owner_session_admission_id")
        owner_values = (owner_session_id, owner_generation_id, owner_admission_id)
        if all(value is None for value in owner_values):
            result = supervisor.resume_start(session_id=session_id)
        else:
            if not all(isinstance(value, str) and value for value in owner_values):
                raise RelayError(
                    "owned gateway runtime omitted its exact owner-session admission identity"
                )
            typed_owner_session_id = cast(str, owner_session_id)
            typed_owner_generation_id = cast(str, owner_generation_id)
            typed_owner_admission_id = cast(str, owner_admission_id)
            expected_admission_id = cli._desktop_owner_session_admission_id(
                cluster=cluster,
                session_id=typed_owner_session_id,
            )
            if typed_owner_admission_id != expected_admission_id:
                raise RelayError("owned gateway runtime admission identity changed")
            with owner_session_admission.owner_session_gateway_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                session_id=typed_owner_session_id,
                session_generation_id=typed_owner_generation_id,
                transition_lock_factory=cli._session_transition_lock,
                session_status_reader=session_lifecycle.status_remote_session,
                admission_status_reader=cli._owner_session_admission_status,
            ) as admission:
                if admission.owner_session_admission_id != typed_owner_admission_id:
                    raise RelayError("owned gateway runtime admission identity changed")
                result = supervisor.resume_start(session_id=session_id)
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        report_id[0] = canonical.report_id
        if isinstance(result, ServiceRuntimePendingResult):
            # Pending is an operational checkpoint, not release evidence. Its
            # successful return must not depend on a second worker-provenance
            # observation after the exact runtime query already completed.
            validation_report_module.write_validation_report(canonical, canonical_report_path)
        else:
            cli._write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            payload["outcome"] = result.outcome
            payload["retry_selector"] = result.retry_selector()
            payload["scheduler_action"] = result.scheduler_action
            payload["relay_action"] = result.relay_action
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(cli._public_json(payload))

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            report_already_written = False
            if report_id[0] is not None:
                with suppress(ConfigurationError):
                    report_already_written = (
                        load_validation_report(canonical_report_path).report_id == report_id[0]
                    )
            if not report_already_written:
                artifact_sha256: str | None = None
                if validation_artifact is not None:
                    with suppress(OSError):
                        artifact_sha256 = sha256_file(validation_artifact)
                failed_report = new_live_validation_report(
                    scenario="gateway-runtime",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=artifact_sha256,
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "gateway.resume-runtime",
                    "resume exact scheduler-backed gateway runtime",
                    exc,
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    cli._run_or_exit(guarded_action)


@cli_gateway.gateway_app.command("browser-attach", hidden=True)
def gateway_browser_attach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ttl_seconds: Annotated[
        int,
        typer.Option(help="Short-lived browser capability lifetime in seconds."),
    ] = 1_800,
    bind_port: Annotated[
        int | None,
        typer.Option(help="Optional desktop loopback proxy port."),
    ] = None,
) -> None:
    """Issue one sandbox-browser attachment capability for a verified gateway."""
    import clio_relay.cli as cli

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_runtime.storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_attach(
            session_id=session_id,
            ttl_seconds=ttl_seconds,
            bind_port=bind_port,
        )
        # This is the sole one-time capability output. Do not route it through
        # routine gateway serialization or persist it in the gateway record.
        typer.echo(result.model_dump_json(indent=2))

    cli._run_or_exit(action)


@cli_gateway.gateway_app.command("browser-detach", hidden=True)
def gateway_browser_detach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    attachment_id: Annotated[
        str,
        typer.Option(help="Exact browser attachment identity to revoke."),
    ],
) -> None:
    """Revoke one exact browser capability and stop its owned proxy."""
    import clio_relay.cli as cli

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_runtime.storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_detach(session_id=session_id, attachment_id=attachment_id)
        typer.echo(cli._public_json(result.model_dump(mode="json")))

    cli._run_or_exit(action)


@cli_gateway.gateway_app.command("detach-runtime")
@cli_support._acceptance_report_command
def gateway_detach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime detach JSON path. Defaults under .clio-relay."
        ),
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
            help="Optional wheel whose SHA-256 is recorded in gateway detach evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop the owned desktop connector while retaining the remote runtime and job."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = cli._new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="detach",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="retain",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    validation_report_module.write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_runtime.storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.detach(session_id=session_id)
        canonical = result.to_live_validation_report(
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
        cli._write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(cli._public_json(payload))
        if (
            result.errors
            or result.residual_resources
            or canonical.status is not ValidationStatus.PASSED
        ):
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.detach-runtime",
                summary="detach owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    cli._run_or_exit(guarded_action)


@cli_gateway.gateway_app.command("attach-runtime")
def gateway_attach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
) -> None:
    """Recreate the desktop connector for a detached owned runtime."""
    import clio_relay.cli as cli

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_runtime.storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token=cli._resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=cli._resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        result = supervisor.attach(session_id=session_id)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            gateway = cast(dict[str, object], payload.get("gateway", {}))
            for key in (
                "connect_url",
                "health_url",
                "stream_url",
                "events_url",
                "state_url",
                "command_url",
                "compatibility_urls",
            ):
                gateway.pop(key, None)
            payload["gateway"] = gateway
        payload.update(
            {
                "outcome": (
                    result.outcome if isinstance(result, ServiceRuntimePendingResult) else "ready"
                ),
                "retry_selector": (
                    result.retry_selector()
                    if isinstance(result, ServiceRuntimePendingResult)
                    else None
                ),
                "scheduler_action": "none",
                "relay_action": "none",
                "scheduler_cancel_requested": False,
            }
        )
        typer.echo(cli._public_json(payload))

    cli._run_or_exit(action)


@cli_gateway.gateway_app.command("stop-runtime")
@cli_support._acceptance_report_command
def gateway_stop_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Cancel the scheduler job after closing relay connectors.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime cleanup JSON path. Defaults under .clio-relay."
        ),
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
            help="Optional wheel whose SHA-256 is recorded in gateway cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned runtime relay connectors and optionally cancel scheduler work."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = cli._new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="teardown",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="close",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=cancel_scheduler_job,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    validation_report_module.write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = cli._require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = service_runtime.ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_runtime.storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.stop(
            session_id=session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        canonical = result.to_live_validation_report(
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
        cli._write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(cli._public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if result.errors or result.residual_resources or not canonical_ok:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            cli._write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.stop-runtime",
                summary="stop owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    cli._run_or_exit(guarded_action)
