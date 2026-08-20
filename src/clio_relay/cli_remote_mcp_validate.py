"""The ``remote-mcp validate`` command (iowarp/clio-relay#231 cli.py
decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names ``remote_mcp_app`` for a two-way command split (see the sibling
``cli_remote_mcp.py``'s own docstring for the other five commands and why
``validate`` -- 314 body lines, cli.py's own docstring once called it among
the "other giants" -- moves separately): this module owns just this one
command, so it stays comfortably under the 800-line cap the combined six
would have exceeded, matching the ``cli_jarvis_mcp.py``/
``cli_jarvis_mcp_validate.py`` split precedent.

**Domain logic stays where it lives.** This command's own code is the
preflight/dispatch/report-rendering orchestration Typer parses into: it
drives the route-resolution and durable-call engine in the new
``remote_mcp_validation.py`` (route resolution, one-or-three durable virtual
MCP calls, and for the fresh-Spack-install transition, a bounded
configuration-tree observation) and renders the resulting canonical
acceptance report -- it does not itself implement any of that engine.

**Registration seam.** ``remote_mcp_app`` is owned by the sibling
``cli_remote_mcp.py``, not this module, so this file reaches it by importing
that module and decorating onto ``cli_remote_mcp.remote_mcp_app`` directly
-- the same two-file-one-Typer pattern ``cli_session_owned.py`` established
for ``cli_session.session_app``. ``@cli_support._acceptance_report_command``
is applied as a bare decorator, read straight from ``cli_support`` at this
module's own import time (the same reason every other command-module using
it does: a decorator fires before any function-local import can run, so
routing it through ``cli.py`` would recreate the import cycle the discipline
below exists to avoid).

**Collaborators reached through cli.py's own name (not moved here).**
``_json_text_from_option``, ``_json_object``, ``_write_failed_acceptance_
report``, ``_remote_worker_info``, and ``_run_or_exit`` are all cli.py-
resident shared helpers -- reached through cli.py's own name via the
established function-local ``import clio_relay.cli as cli`` discipline, the
same shape ``cli_jarvis_mcp_validate.py`` established for its own engine
calls.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

import clio_relay.cli_remote_mcp as cli_remote_mcp
import clio_relay.cli_support as cli_support
import clio_relay.mcp_server as mcp_server_module
import clio_relay.remote_cli as remote_cli
import clio_relay.remote_mcp_validation as remote_mcp_validation
import clio_relay.storage_runtime as storage_runtime
import clio_relay.validation_report as validation_report_module
from clio_relay.cluster_config import ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.installation import attach_verified_worker_identity
from clio_relay.mcp_server import static_mcp_tool_names
from clio_relay.remote_mcp import (
    RemoteMcpSchemaCache,
    RemoteMcpStructuredResultExpectation,
    build_remote_mcp_spack_fresh_install_transition_report,
    default_remote_mcp_cache_path,
)
from clio_relay.validation_report import (
    ValidationRecorder,
    default_report_path,
    new_live_validation_report,
    sha256_file,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false


@cli_remote_mcp.remote_mcp_app.command("validate")
@cli_support._acceptance_report_command
def remote_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    tool: Annotated[str, typer.Option(help="Allowlisted remote MCP tool name to call.")],
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote tool."),
    ] = None,
    result_expectation_json: Annotated[
        str,
        typer.Option(
            help=("Optional JSON object describing semantic expectations for structuredContent.")
        ),
    ] = "{}",
    result_expectation_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a structured-result expectation JSON object."),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and the virtual call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable virtual call.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    output_json: Annotated[
        Path | None,
        typer.Option(help="Optional path for the machine-readable acceptance report."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical release-evidence JSON path. Defaults under .clio-relay."),
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
            help="Optional wheel whose SHA-256 is recorded in canonical evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Call one virtual tool and emit canonical durable acceptance evidence."""
    import clio_relay.cli as cli

    canonical_report_path = validation_report or default_report_path(cluster)
    canonical_written = [False]

    def preflight() -> remote_mcp_validation.RemoteMcpValidationPreflight:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        arguments_source = cli._json_text_from_option(arguments_json, arguments_json_file)
        remote_arguments = cli._json_object(arguments_source)
        result_expectation: RemoteMcpStructuredResultExpectation | None = None
        if result_expectation_json_file is not None or result_expectation_json != "{}":
            expectation_source = cli._json_text_from_option(
                result_expectation_json,
                result_expectation_json_file,
            )
            try:
                result_expectation = RemoteMcpStructuredResultExpectation.model_validate(
                    cli._json_object(expectation_source)
                )
            except ValidationError as exc:
                raise typer.BadParameter(
                    f"structured-result expectation is invalid: {exc.errors()[0]['msg']}"
                ) from exc
        registry_path = default_registry_path()
        registry = ClusterRegistry.load(registry_path)
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        registration = definition.remote_mcp_servers[name]
        if result_expectation is not None:
            if result_expectation.tool != tool:
                raise typer.BadParameter("structured-result expectation tool must match --tool")
            if registration.contract != result_expectation.contract:
                raise typer.BadParameter(
                    "structured-result expectation contract must match the registered contract"
                )
        catalog = mcp_server_module.load_registered_remote_mcp_catalog(profile)
        fresh_transition = (
            result_expectation is not None
            and result_expectation.fresh_install_store_root is not None
        )
        if fresh_transition:
            if result_expectation is None:
                raise typer.BadParameter("fresh Spack expectation is unavailable")
            if (
                remote_arguments.get("spec") != result_expectation.requested_spec
                or remote_arguments.get("reuse") is not False
            ):
                raise typer.BadParameter(
                    "fresh Spack validation arguments must submit the expected spec "
                    "with reuse=false"
                )
        required_tools = (
            ("spack_find", "spack_install", "spack_locate") if fresh_transition else (tool,)
        )
        routes = {
            remote_tool_name: remote_mcp_validation.resolve_remote_mcp_validation_route(
                catalog=catalog,
                cluster=cluster,
                server_name=name,
                remote_tool_name=remote_tool_name,
            )
            for remote_tool_name in required_tools
        }
        requested_route = routes[tool]
        if not requested_route.arguments_wrapped and "cluster" in remote_arguments:
            raise typer.BadParameter(
                "flat remote tool arguments must not contain reserved key 'cluster'"
            )
        return remote_mcp_validation.RemoteMcpValidationPreflight(
            registry_path=registry_path,
            registry=registry,
            definition=definition,
            remote_arguments=remote_arguments,
            routes=routes,
            result_expectation=result_expectation,
        )

    try:
        prepared = preflight()
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="remote-mcp.preflight",
            summary="validate virtual remote MCP acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        settings = RelaySettings.from_env()
        queue = storage_runtime.storage_managed_queue(settings)
        queue.initialize()
        execute_remotely = remote_cli.should_execute_on_cluster(prepared.definition)
        remote_install_info = (
            cli._remote_worker_info(prepared.definition) if execute_remotely else None
        )
        cache = RemoteMcpSchemaCache.load(
            default_remote_mcp_cache_path(registry_path=prepared.registry_path)
        )
        reserved_names = static_mcp_tool_names()
        if prepared.fresh_spack_transition:
            expectation = prepared.result_expectation
            if expectation is None or expectation.requested_spec is None:
                raise RelayError("fresh Spack transition expectation became unavailable")
            preinstall_call = remote_mcp_validation.execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_find",
                route=prepared.routes["spack_find"],
                remote_arguments={"query": expectation.requested_spec},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            remote_mcp_validation.require_passing_remote_mcp_call(
                preinstall_call, phase="preinstall find"
            )
            remote_mcp_validation.require_spack_preinstall_absent(
                preinstall_call.protocol_result,
                requested_spec=expectation.requested_spec,
            )
            preinstall_configuration = (
                remote_mcp_validation.collect_spack_configuration_observation(
                    definition=prepared.definition,
                    execute_remotely=execute_remotely,
                    expectation=expectation,
                    phase="preinstall",
                )
            )
            install_call = remote_mcp_validation.execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_install",
                route=prepared.routes["spack_install"],
                remote_arguments=prepared.remote_arguments,
                result_expectation=expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            remote_mcp_validation.require_passing_remote_mcp_call(
                install_call, phase="fresh install"
            )
            postinstall_call = remote_mcp_validation.execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_locate",
                route=prepared.routes["spack_locate"],
                remote_arguments={"spec": f"/{expectation.dag_hash}"},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            postinstall_configuration = (
                remote_mcp_validation.collect_spack_configuration_observation(
                    definition=prepared.definition,
                    execute_remotely=execute_remotely,
                    expectation=expectation,
                    phase="postinstall",
                )
            )
            report = build_remote_mcp_spack_fresh_install_transition_report(
                preinstall_report=preinstall_call.report,
                install_report=install_call.report,
                postinstall_report=postinstall_call.report,
                preinstall_protocol_result=preinstall_call.protocol_result,
                install_protocol_result=install_call.protocol_result,
                postinstall_protocol_result=postinstall_call.protocol_result,
                install_expectation=expectation,
                preinstall_configuration=preinstall_configuration,
                postinstall_configuration=postinstall_configuration,
            )
        else:
            requested_call = remote_mcp_validation.execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name=tool,
                route=prepared.routes[tool],
                remote_arguments=prepared.remote_arguments,
                result_expectation=prepared.result_expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            report = requested_call.report
        canonical_report = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        if remote_install_info is not None:
            attach_verified_worker_identity(canonical_report, remote_install_info)
        validation_report_module.write_validation_report(canonical_report, canonical_report_path)
        canonical_written[0] = True
        rendered = report.model_dump_json(indent=2)
        if output_json is not None:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(rendered + "\n", encoding="utf-8")
        typer.echo(rendered)
        if not report.passed:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not canonical_written[0]:
                failed_report = new_live_validation_report(
                    scenario="remote-mcp",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "remote-mcp.completed", "complete virtual remote MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    cli._run_or_exit(guarded_action)
