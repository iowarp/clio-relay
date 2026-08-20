"""The ``doctor``/``live-test`` top-level commands (iowarp/clio-relay#231
cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names the 13 flat, un-namespaced ``@app.command(...)`` entries directly on
``cli.py``'s top-level ``app`` as a group to split by concern. This module
owns the doctor/live-test concern: ``doctor`` (local + live cluster
configuration checks) and ``live-test`` (the configurable live acceptance
runner) -- two commands, 349 lines combined at the pin this slice measured.

**Domain logic stays where it lives.** ``doctor`` delegates entirely to
``doctor.run_doctor``/``doctor.run_cluster_doctor`` (both already-correct
owners); ``live-test`` delegates its actual acceptance work to
``live_acceptance.run_live_acceptance`` -- this module's own code is parsing
and result rendering only, ground rule 2.

**Registration seam.** Both commands attach to the *shared* top-level ``app``
Typer instance, not a namespaced sub-app of their own (unlike ``relay-host``
or ``release``, which already had their own ``typer.Typer()``). ``app`` is
defined in ``cli.py`` itself, so a module-level ``@app.command(...)``
decorator here would need ``cli.app`` at import time, which is a genuine
cycle (``cli.py`` also imports this module at module level, to trigger
registration): this module owns neither ``app`` nor its own Typer instance.
Instead, ``cli.py`` imports this module for its plain function objects (full
Typer ``Annotated`` signatures included -- Typer introspects the function
itself, not the decorator call site) and applies the registration in one
line apiece: ``app.command("doctor")(cli_diagnostics.doctor)`` and
``app.command("live-test")(cli_diagnostics.live_test)``. This keeps the
public CLI surface (``clio-relay doctor``, ``clio-relay live-test``, both
flat, no new namespace) byte-identical to before the move.

**The import-cycle discipline.** Same as ``cli_relay_host.py``/
``cli_cluster_deploy.py``: ``cli`` (``clio_relay.cli``) is never bound as a
module-level name here, only imported function-locally as the first
statement of each command body (``import clio_relay.cli as cli``, then
``cli.<symbol>(...)``) -- deferred until the command actually runs, well
after ``cli.py`` has finished loading. A nested closure (``live_test``'s own
``_run``) reaches the same ``cli`` binding via ordinary lexical closure, no
second import needed, matching ``cli_cluster_deploy.py``'s ``action()``
precedent.

**Collaborators reached through ``cli.py``'s own name (not moved here).**
``_require_cluster``, ``_run_or_exit``, ``_echo_lines``, ``_resolve_env_secret``
(``cli_support.py``'s cross-cutting helpers, same forwarder cli_relay_host.py
reaches); ``_write_failed_acceptance_report`` (same); ``_json_object`` (not
yet a cli_support.py owner -- SS5's shared-plumbing relocation row is a
separate, later slice); ``_write_remote_verified_report`` (shared with
``_write_cleanup_validation_report``, which stays cli.py-resident --
``docs/design/relay-architecture-2026-08.md``'s session-orchestration row is
unsequenced, so moving this shared helper's body here would just split it
across two owners); ``_load_current_acceptance_report`` (cross-cutting, 5
call sites total -- 2 here, 2 in ``cli_release.py``, 1 that stayed put when
``cli_release.py`` was extracted; that module's own docstring names the same
reason for not moving it there either).

**Exclusive helper moved with its only caller.**
``_live_acceptance_resume_output_path`` had exactly one call site in the
whole of ``cli.py`` -- ``live_test`` itself -- so it moves here outright, no
forwarder needed.

**Reassigned patch-seam caller.** ``live_acceptance.run_live_acceptance`` had
exactly one call site in the whole of ``cli.py`` -- ``live_test`` itself --
unlike ``remote_cli.should_execute_on_cluster`` and ``validation_report.
write_validation_report`` (both used pervasively elsewhere in ``cli.py``,
stay ``"cli"``). This slice reassigns ``run_live_acceptance``'s ``caller``
entry in ``AUDITED_COLLABORATORS`` from ``"cli"`` to ``"cli_diagnostics"``
and registers this module in ``_GUARDED_CALLERS``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer

import clio_relay.cli_support as cli_support
import clio_relay.live_acceptance as live_acceptance
import clio_relay.remote_cli as remote_cli
import clio_relay.validation_report as validation_report_module
from clio_relay.config import RelaySettings
from clio_relay.doctor import run_cluster_doctor, run_doctor
from clio_relay.errors import RelayError
from clio_relay.live_acceptance import LiveAcceptanceOptions
from clio_relay.validation_report import (
    ValidationStatus,
    default_report_path,
    new_live_validation_report,
    sha256_file,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false


def _live_acceptance_resume_output_path(source: Path) -> Path:
    """Return a collision-resistant sibling without altering the source checkpoint."""
    return source.with_name(f"{source.stem}.resume-{uuid4().hex[:8]}{source.suffix}")


def doctor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Check local or live cluster configuration."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)

    def _run() -> None:
        cli._echo_lines(
            run_doctor(
                RelaySettings.from_env(),
                live=True,
                frps_addr=definition.frp_transport.server_addr,
            )
        )
        cli._echo_lines(run_cluster_doctor(definition))

    cli._run_or_exit(_run)


@cli_support._acceptance_report_command
def live_test(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[
        Path | None,
        typer.Option(help="Configured acceptance JARVIS YAML. Overrides cluster config."),
    ] = None,
    monitor_pattern: Annotated[
        str | None,
        typer.Option(help="Regex expected to match stdout.delta during acceptance."),
    ] = None,
    progress_pattern: Annotated[
        str | None,
        typer.Option(help="Regex used to record structured progress from stdout.delta."),
    ] = None,
    progress_action_payload_json: Annotated[
        str,
        typer.Option(
            help="JSON object payload for progress monitor extraction, such as groups and units.",
        ),
    ] = "{}",
    agent_prompt: Annotated[
        str | None,
        typer.Option(help="Remote prompt path for optional agent acceptance."),
    ] = None,
    agent_mcp_config: Annotated[
        str | None,
        typer.Option(help="Remote MCP config path for optional agent acceptance."),
    ] = None,
    agent_child_jarvis_yaml: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Local JARVIS YAML the agent must submit through MCP. "
                "Generates a remote agent prompt with a fresh idempotency key."
            ),
        ),
    ] = None,
    require_agent_child_job: Annotated[
        bool | None,
        typer.Option(
            "--require-agent-child-job/--no-require-agent-child-job",
            help=(
                "Require optional agent acceptance to report and complete a child relay job. "
                "Defaults to enabled when --agent-mcp-config is set."
            ),
        ),
    ] = None,
    verify_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-transport/--no-verify-transport",
            help="Verify desktop-to-cluster HTTP reachability through configured frp transport.",
        ),
    ] = None,
    verify_direct_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-direct-transport/--no-verify-direct-transport",
            help="Verify desktop-to-cluster HTTP reachability through frp XTCP.",
        ),
    ] = None,
    verify_ssh_transport: Annotated[
        bool,
        typer.Option(
            "--verify-ssh-transport/--no-verify-ssh-transport",
            help="Verify an owned SSH-forward transport and teardown path.",
        ),
    ] = False,
    allow_direct_transport_fallback: Annotated[
        bool | None,
        typer.Option(
            "--allow-direct-transport-fallback/--no-allow-direct-transport-fallback",
            help="Allow live direct transport acceptance to fall back to STCP.",
        ),
    ] = None,
    transport_token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    transport_secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local desktop visitor bind port for transport acceptance."),
    ] = None,
    transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote cluster API port for transport acceptance."),
    ] = None,
    transport_proxy_name: Annotated[
        str | None,
        typer.Option(help="frp proxy/server name for transport acceptance."),
    ] = None,
    ssh_transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local bind port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote API port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_session_id: Annotated[
        str | None,
        typer.Option(help="Owned remote session id for SSH-forward acceptance."),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering of the JSON report."),
    ] = None,
    resume_report: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Resume the exact nonterminal workload recorded by a PENDING live-test report. "
                "The source checkpoint is never overwritten."
            ),
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(
            help="Launcher evidence, such as uv-tool. Can use the validation environment."
        ),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(
            help="Explicit kind:reference install evidence, such as pypi:clio-relay==1.0.0."
        ),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel artifact whose SHA-256 is recorded in the report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_scenario: Annotated[
        str,
        typer.Option(help="Release-policy scenario recorded in the JSON report."),
    ] = "live-test",
    verify_cluster_deployment: Annotated[
        bool,
        typer.Option(
            "--verify-cluster-deployment/--no-verify-cluster-deployment",
            help="Require the matching installed worker version and a live worker execution.",
        ),
    ] = False,
    require_structured_runtime_metadata: Annotated[
        bool,
        typer.Option(
            "--require-structured-runtime-metadata/--allow-legacy-runtime-metadata",
            help="Require JARVIS-owned structured runtime and scheduler metadata.",
        ),
    ] = True,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for acceptance jobs."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Run configurable live acceptance checks for a cluster."""
    import clio_relay.cli as cli
    import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach

    report_path = report or (
        _live_acceptance_resume_output_path(resume_report)
        if resume_report is not None
        else default_report_path(cluster)
    )
    if resume_report is not None and report_path.resolve() == resume_report.resolve():
        raise typer.BadParameter(
            "--report must differ from --resume-report so the checkpoint is preserved"
        )
    seed_report = new_live_validation_report(
        scenario=validation_scenario,
        cluster=cluster,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    if resume_report is None:
        validation_report_module.write_validation_report(seed_report, report_path)
    try:
        definition = cli._require_cluster(cluster)
        should_verify_transport = (
            definition.live_test.verify_transport if verify_transport is None else verify_transport
        )
        should_verify_direct_transport = (
            definition.live_test.verify_direct_transport
            if verify_direct_transport is None
            else verify_direct_transport
        )
        should_allow_direct_transport_fallback = (
            definition.live_test.allow_direct_transport_fallback
            if allow_direct_transport_fallback is None
            else allow_direct_transport_fallback
        )
        needs_transport_secrets = should_verify_transport or should_verify_direct_transport
    except BaseException as exc:
        current_report = cli._load_current_acceptance_report(
            report_path,
            expected_report_id=seed_report.report_id,
        )
        cli._write_failed_acceptance_report(
            path=report_path,
            scenario=validation_scenario,
            cluster=cluster,
            check_id="live.preflight",
            summary="validate live acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=current_report or seed_report,
        )
        raise

    def _run() -> None:
        settings = RelaySettings.from_env()
        try:
            lines = live_acceptance.run_live_acceptance(
                LiveAcceptanceOptions(
                    cluster=cluster,
                    definition=definition,
                    jarvis_yaml=jarvis_yaml,
                    monitor_pattern=monitor_pattern,
                    progress_pattern=progress_pattern,
                    progress_action_payload=cli._json_object(progress_action_payload_json),
                    agent_prompt=agent_prompt,
                    agent_mcp_config=agent_mcp_config,
                    require_agent_child_job=require_agent_child_job,
                    agent_child_jarvis_yaml=agent_child_jarvis_yaml,
                    verify_transport=verify_transport,
                    verify_direct_transport=should_verify_direct_transport,
                    verify_ssh_transport=verify_ssh_transport,
                    allow_direct_transport_fallback=should_allow_direct_transport_fallback,
                    transport_token=(
                        cli._resolve_env_secret(
                            transport_token,
                            definition.frp_transport.token_env,
                            "frp token",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_secret_key=(
                        cli._resolve_env_secret(
                            transport_secret_key,
                            definition.frp_transport.stcp_secret_env,
                            "stcp secret",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_frpc_bin=settings.frpc_bin,
                    transport_local_bind_port=transport_local_bind_port,
                    transport_remote_api_port=transport_remote_api_port,
                    transport_proxy_name=transport_proxy_name,
                    ssh_transport_local_bind_port=ssh_transport_local_bind_port,
                    ssh_transport_remote_api_port=ssh_transport_remote_api_port,
                    ssh_transport_session_id=ssh_transport_session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    validation_launcher=validation_launcher,
                    validation_install_source=validation_install_source,
                    validation_artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                    require_structured_runtime_metadata=require_structured_runtime_metadata,
                    validation_scenario=validation_scenario,
                    verify_cluster_deployment=verify_cluster_deployment,
                    report_id=seed_report.report_id,
                    resume_report_path=resume_report,
                )
            )
            current_report = cli._load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None:
                raise RelayError("live acceptance did not persist the current invocation report")
            if (
                current_report.status is ValidationStatus.PASSED
                and remote_cli.should_execute_on_cluster(definition)
            ):
                cli_remote_worker_attach._write_remote_verified_report(
                    current_report,
                    definition,
                    report_path,
                )
        except BaseException as exc:
            current_report = cli._load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            cli._write_failed_acceptance_report(
                path=report_path,
                scenario=validation_scenario,
                cluster=cluster,
                check_id="live.completed",
                summary="complete live acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        cli._echo_lines(lines)

    cli._run_or_exit(_run)
