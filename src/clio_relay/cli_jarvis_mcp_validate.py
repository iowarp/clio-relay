"""The ``jarvis-mcp-validate`` top-level command (iowarp/clio-relay#231
cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names the 13 flat, un-namespaced ``@app.command(...)`` entries directly on
``cli.py``'s top-level ``app`` as a group to split by concern, and separately
calls ``jarvis_mcp_validate`` out among cli.py's "other giants" by line span.
This module owns it alone, split out of the sibling ``cli_jarvis_mcp.py``
(which owns the other five, much smaller, jarvis-mcp commands) so that
module stays comfortably under the 800-line cap -- this one, at 453 body
lines of dense checkpoint-resume orchestration on its own, is already close
to it.

**Domain logic stays where it lives.** This command's own code is the
resume/dispatch/query state-machine orchestration Typer parses into: it
drives the JARVIS execution-query engine (see below) and renders the
resulting ``LiveValidationReport`` -- it does not itself implement contract
discovery, package search, checkpoint encoding, or integrity checking.

**The JARVIS execution-query engine stays cli.py-resident (unsequenced).**
Roughly two dozen cli.py-private helpers this command calls
(``_run_jarvis_remote_contract_discovery``, ``_run_jarvis_package_search_
query``, ``_new_jarvis_validation_idempotency_key``, ``_jarvis_run_execution_
intent``, ``_new_jarvis_intent_resume_checkpoint``, ``_promote_jarvis_intent_
to_dispatch_checkpoint``, ``_complete_jarvis_run_dispatch``, ``_run_post_run_
jarvis_execution_query``, ``_mark_jarvis_validation_pending``, and their own
transitive dependencies -- a ~2,450-line engine in total) are confirmed
exclusive to the jarvis-mcp concern (this command and ``jarvis-mcp-refresh``,
both moving out of cli.py in this same slice pair, are their only callers)
but are themselves genuine business logic cli.py should not own per ground
rule 2. Extracting that engine into a real owner module is a substantial,
separate design exercise -- named here explicitly (ground rule 4: gaps are
first-class, not silently dropped) as unsequenced future work, the same
category ``cli_relay_host.py``'s own docstring already puts ``_run_
transport_validation`` in. This module reaches the engine, and every other
collaborator still resident in cli.py, through cli.py's own name via the
established function-local ``import clio_relay.cli as cli`` discipline.

**No patch-seam reassignments.** Every audited collaborator this command
uses (``storage_runtime.storage_managed_queue``, ``remote_cli.should_
execute_on_cluster``, ``validation_report.write_validation_report``,
``jarvis_mcp_validation.build_jarvis_mcp_validation_report``, ``mcp_stdio_
validation.run_packaged_mcp_stdio_session``) is also called from cli.py-
resident code (the engine above, or other command groups), so all five keep
their ``"cli"`` caller entry in ``AUDITED_COLLABORATORS`` unchanged -- this
module simply adds a second, module-attribute-style caller for each, the
same shape every prior slice in this campaign established for a
still-shared collaborator.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import uuid4

import typer

import clio_relay.cli_support as cli_support
import clio_relay.jarvis_mcp_validation as jarvis_mcp_validation
import clio_relay.mcp_stdio_validation as mcp_stdio_validation
import clio_relay.remote_cli as remote_cli
import clio_relay.storage_runtime as storage_runtime
import clio_relay.validation_report as validation_report_module
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.installation import attach_verified_worker_identity
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    ValidationStatus,
    default_report_path,
    load_validation_report,
    new_live_validation_report,
    redact_sensitive_values,
    sha256_file,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false


@cli_support._acceptance_report_command
def jarvis_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    package_search_query: Annotated[
        str,
        typer.Option(
            help=("Non-blank application query used to prove bounded JARVIS package discovery."),
        ),
    ] = "",
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the virtual jarvis_run tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for virtual jarvis_run."),
    ] = None,
    resume_report: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Pending report to resume its exact idempotent jarvis_run dispatch or "
                "JARVIS execution query; never creates a new workload identity."
            ),
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and tools/call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(
            help=(
                "Maximum observation window for durable JARVIS MCP calls, not the workload "
                "lifetime. Expiry after an idempotent intent, relay receipt, or execution "
                "identity writes a resumable pending checkpoint without cancellation."
            ),
            min=1,
        ),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    report: Annotated[
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
    """Exercise JARVIS run/query semantics and persist release acceptance evidence."""
    import clio_relay.cli as cli
    import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
    import clio_relay.cli_jarvis_dispatch as cli_jarvis_dispatch
    import clio_relay.cli_jarvis_execution_run as cli_jarvis_execution_run
    import clio_relay.cli_jarvis_execution_types as cli_jarvis_execution_types
    import clio_relay.cli_jarvis_intent_checkpoint as cli_jarvis_intent_checkpoint
    import clio_relay.cli_jarvis_package_search as cli_jarvis_package_search
    import clio_relay.cli_jarvis_pending_report as cli_jarvis_pending_report
    import clio_relay.cli_jarvis_query_observation as cli_jarvis_query_observation
    import clio_relay.cli_jarvis_remote_contract as cli_jarvis_remote_contract
    import clio_relay.cli_jarvis_resume_checkpoint as cli_jarvis_resume_checkpoint
    import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe

    report_path = report or resume_report or default_report_path(cluster)
    failure_report_path = report_path
    if resume_report is not None and report_path.resolve() == resume_report.resolve():
        suffix = report_path.suffix or ".json"
        failure_report_path = report_path.with_name(
            f"{report_path.stem}.resume-failure-{uuid4().hex}{suffix}"
        )
    report_written = [False]

    def preflight() -> tuple[
        dict[str, Any],
        ClusterDefinition,
        str,
        dict[str, Any] | None,
    ]:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        definition = cli._require_cluster(cluster)
        if resume_report is not None:
            if package_search_query or arguments_json != "{}" or arguments_json_file is not None:
                raise typer.BadParameter(
                    "--resume-report cannot be combined with run or package-search arguments"
                )
            checkpoint = cli_jarvis_resume_checkpoint._load_jarvis_validation_resume_checkpoint(
                resume_report,
                cluster=cluster,
            )
            return {}, definition, "", checkpoint
        normalized_package_search_query = " ".join(package_search_query.split())
        if not normalized_package_search_query:
            raise typer.BadParameter("--package-search-query must not be blank")
        if len(normalized_package_search_query) > 256:
            raise typer.BadParameter("--package-search-query must not exceed 256 characters")
        arguments_source = cli._json_text_from_option(arguments_json, arguments_json_file)
        arguments = cli._json_object(arguments_source)
        if redact_sensitive_values(arguments) != arguments:
            raise typer.BadParameter(
                "JARVIS validation arguments cannot contain credential-valued fields because "
                "durable resume reports are always credential-redacted"
            )
        if "cluster" in arguments:
            raise typer.BadParameter(
                "JARVIS tool arguments must not contain reserved key 'cluster'"
            )
        if "wait" in arguments:
            raise typer.BadParameter(
                "jarvis_run is handle-first and does not accept internal wait; remove 'wait' "
                "and let jarvis-mcp-validate observe workload lifecycle with "
                "jarvis_get_execution"
            )
        if not isinstance(arguments.get("pipeline_id"), str):
            raise typer.BadParameter("jarvis-mcp-validate requires a string pipeline_id argument")
        return arguments, definition, normalized_package_search_query, None

    try:
        (
            arguments,
            definition,
            normalized_package_search_query,
            resume_checkpoint,
        ) = preflight()
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=failure_report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="jarvis-mcp.preflight",
            summary="validate virtual JARVIS MCP acceptance inputs",
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

        def emit(validation: LiveValidationReport, *, attach_worker: bool = False) -> None:
            if attach_worker and remote_cli.should_execute_on_cluster(definition):
                attach_verified_worker_identity(
                    validation, cli_remote_worker_probe._remote_worker_info(definition)
                )
            validation_report_module.write_validation_report(validation, report_path)
            report_written[0] = True
            typer.echo(validation.model_dump_json(indent=2))
            if validation.status is ValidationStatus.FAILED:
                raise typer.Exit(code=1)

        def retain_existing_pending_report() -> None:
            if resume_report is None:  # pragma: no cover - guarded by resume_checkpoint
                raise RelayError("JARVIS validation resume source disappeared")
            emit(load_validation_report(resume_report))

        def finish_execution_query(
            *,
            builder_inputs: dict[str, Any],
            execution_query: cli_jarvis_execution_types._JarvisExecutionQueryAcceptance
            | cli_jarvis_execution_types._JarvisExecutionQueryPending,
            checkpoint_profile: str,
        ) -> None:
            selector = execution_query.retry_selector()
            builder_inputs = {
                **builder_inputs,
                "scheduler_cluster": selector["scheduler_cluster"],
            }
            observations = (
                []
                if isinstance(
                    execution_query, cli_jarvis_execution_types._JarvisExecutionQueryPending
                )
                else list(execution_query.lifecycle_observations)
            )
            checkpoint = {
                "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,
                "phase": cli._JARVIS_VALIDATION_PHASE_QUERY,
                "observation_state": "not_observed" if not observations else "observed",
                "profile": checkpoint_profile,
                "retry_selector": selector,
                "builder_inputs": builder_inputs,
                "lifecycle_observations": observations,
            }
            if isinstance(execution_query, cli_jarvis_execution_types._JarvisExecutionQueryPending):
                validation = (
                    cli_jarvis_pending_report._build_unobserved_jarvis_query_pending_report(
                        builder_inputs=builder_inputs,
                        execution_query=execution_query,
                        checkpoint=checkpoint,
                    )
                )
            else:
                validation = jarvis_mcp_validation.build_jarvis_mcp_validation_report(
                    **builder_inputs,
                    query_tools_list_response=execution_query.tools_list_response,
                    query_call_response=execution_query.call_response,
                    query_call_job_id=execution_query.call_job_id,
                    query_call_status=execution_query.call_status,
                    query_artifacts=execution_query.artifacts,
                    query_mcp_result=execution_query.mcp_result,
                    query_provenance=execution_query.provenance,
                    query_initialize_response=execution_query.initialize_response,
                    query_stdio_evidence=execution_query.stdio_evidence,
                    query_lifecycle_observations=observations,
                )
                if execution_query.outcome != "terminal":
                    validation = cli_jarvis_pending_report._mark_jarvis_validation_pending(
                        validation,
                        execution_query=execution_query,
                        resume_checkpoint=checkpoint,
                    )
            emit(validation, attach_worker=validation.status is not ValidationStatus.PENDING)

        checkpoint = resume_checkpoint
        checkpoint_profile = profile
        if checkpoint is not None:
            checkpoint_profile = cast(str, checkpoint["profile"])
            phase = checkpoint.get("phase", cli._JARVIS_VALIDATION_PHASE_QUERY)
            if phase == cli._JARVIS_VALIDATION_PHASE_QUERY:
                selector = cast(dict[str, Any], checkpoint["retry_selector"])
                query_selector: dict[str, object] = {
                    **cast(dict[str, object], selector),
                    "last_query_job_id": None,
                }
                execution_query = cli_jarvis_execution_run._run_post_run_jarvis_execution_query(
                    cluster=cluster,
                    definition=definition,
                    queue=queue,
                    profile=checkpoint_profile,
                    pipeline_id=cast(str, selector["pipeline_id"]),
                    execution_id=cast(str, selector["execution_id"]),
                    retry_selector=query_selector,
                    wait_timeout_seconds=wait_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                cli_jarvis_resume_checkpoint._require_same_jarvis_resume_identity(
                    expected=selector,
                    observed=execution_query.retry_selector(),
                )
                if isinstance(
                    execution_query, cli_jarvis_execution_types._JarvisExecutionQueryPending
                ):
                    retain_existing_pending_report()
                    return
                prior_observations = [
                    cast(dict[str, Any], observation)
                    for observation in cast(list[object], checkpoint["lifecycle_observations"])
                    if isinstance(observation, dict)
                ]
                execution_query = replace(
                    execution_query,
                    lifecycle_observations=cli_jarvis_query_observation._merge_jarvis_execution_query_observations(
                        prior_observations,
                        execution_query.lifecycle_observations,
                    ),
                )
                finish_execution_query(
                    builder_inputs=cast(dict[str, Any], checkpoint["builder_inputs"]),
                    execution_query=execution_query,
                    checkpoint_profile=checkpoint_profile,
                )
                return

        if checkpoint is None:
            (
                remote_discovery_job_id,
                remote_tools_list_result,
                remote_discovery_artifacts,
                remote_discovery_payload,
            ) = cli_jarvis_remote_contract._run_jarvis_remote_contract_discovery(
                cluster=cluster,
                definition=definition,
                queue=queue,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            cli_jarvis_remote_contract._persist_jarvis_remote_contract_discovery(
                cluster=cluster,
                discovery_job_id=remote_discovery_job_id,
                result=remote_tools_list_result,
                artifacts=remote_discovery_artifacts,
                artifact_payload=remote_discovery_payload,
            )
            package_search = cli_jarvis_package_search._run_jarvis_package_search_query(
                cluster=cluster,
                definition=definition,
                queue=queue,
                profile=profile,
                query=normalized_package_search_query,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            validation_artifact_sha256 = (
                sha256_file(validation_artifact) if validation_artifact is not None else None
            )
            pre_dispatch_inputs: dict[str, Any] = {
                "cluster": cluster,
                "tool": "jarvis_run",
                "remote_tools_list_result": remote_tools_list_result,
                "remote_discovery_job_id": remote_discovery_job_id,
                "remote_discovery_artifacts": remote_discovery_artifacts,
                "package_search_query": normalized_package_search_query,
                "package_search_tools_list_response": package_search.tools_list_response,
                "package_search_call_response": package_search.call_response,
                "package_search_call_job_id": package_search.call_job_id,
                "package_search_call_status": package_search.call_status,
                "package_search_artifacts": package_search.artifacts,
                "package_search_mcp_result": package_search.mcp_result,
                "package_search_provenance": package_search.provenance,
                "package_search_initialize_response": package_search.initialize_response,
                "package_search_stdio_evidence": package_search.stdio_evidence,
                "launcher": validation_launcher,
                "install_source": validation_install_source,
                "artifact_sha256": validation_artifact_sha256,
            }
            idempotency_key = cli_jarvis_intent_checkpoint._new_jarvis_validation_idempotency_key(
                cluster=cluster,
                profile=profile,
                arguments=arguments,
            )
            execution_intent = cli_jarvis_intent_checkpoint._jarvis_run_execution_intent(
                cluster=cluster,
                profile=profile,
                arguments=arguments,
                idempotency_key=idempotency_key,
            )
            checkpoint = cli_jarvis_intent_checkpoint._new_jarvis_intent_resume_checkpoint(
                execution_intent=execution_intent,
                pre_dispatch_inputs=pre_dispatch_inputs,
            )
            # Persist the replayable identity before crossing the ambiguous stdio boundary.
            # A process or host failure can therefore resume with this exact key.
            validation_report_module.write_validation_report(
                cli_jarvis_pending_report._new_jarvis_intent_pending_report(checkpoint), report_path
            )
        else:
            execution_intent = cast(dict[str, object], checkpoint["execution_intent"])
            pre_dispatch_inputs = cast(dict[str, Any], checkpoint["pre_dispatch_inputs"])

        if checkpoint["phase"] == cli._JARVIS_VALIDATION_PHASE_INTENT:
            try:
                stdio_session = mcp_stdio_validation.run_packaged_mcp_stdio_session(
                    profile=checkpoint_profile,
                    tool="jarvis_run",
                    arguments=cast(dict[str, Any], execution_intent["arguments"]),
                    timeout_seconds=min(60.0, max(0.001, wait_timeout_seconds)),
                )
            except ObservationTimeoutError:
                if resume_checkpoint is not None:
                    retain_existing_pending_report()
                else:
                    emit(cli_jarvis_pending_report._new_jarvis_intent_pending_report(checkpoint))
                return
            call_response = stdio_session.tools_call_response
            job_id = cli_jarvis_artifact_io._mcp_response_job_id(call_response)
            builder_inputs: dict[str, Any] = {
                **pre_dispatch_inputs,
                "scheduler_cluster": None,
                "tools_list_response": stdio_session.tools_list_response,
                "call_response": call_response,
                "call_job_id": job_id,
                "call_status": {},
                "artifacts": [],
                "mcp_result": None,
                "provenance": None,
                "runtime_metadata": None,
                "progress": [],
                "live_progress_observation": None,
                "initialize_response": stdio_session.initialize_response,
                "stdio_evidence": stdio_session.evidence(),
            }
            checkpoint = cli_jarvis_intent_checkpoint._promote_jarvis_intent_to_dispatch_checkpoint(
                checkpoint,
                job_id=job_id,
                builder_inputs=builder_inputs,
            )

        try:
            builder_inputs = cli_jarvis_dispatch._complete_jarvis_run_dispatch(
                definition=definition,
                queue=queue,
                checkpoint=checkpoint,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
        except ObservationTimeoutError:
            emit(cli_jarvis_pending_report._build_jarvis_dispatch_pending_report(checkpoint))
            return
        raw_runtime_metadata = builder_inputs.get("runtime_metadata")
        runtime_metadata = (
            cast(dict[str, Any], raw_runtime_metadata)
            if isinstance(raw_runtime_metadata, dict)
            else None
        )
        if runtime_metadata is None:
            raise RelayError("JARVIS run metadata artifact is unavailable")
        pipeline_id = runtime_metadata.get("pipeline_id")
        execution_id = runtime_metadata.get("execution_id")
        if not isinstance(pipeline_id, str) or not pipeline_id:
            raise RelayError("JARVIS run metadata omitted the pipeline_id required for its query")
        if not isinstance(execution_id, str) or not execution_id:
            raise RelayError("JARVIS run metadata omitted the execution_id required for its query")
        retry_selector = cli_jarvis_dispatch._jarvis_execution_retry_selector_from_runtime_metadata(
            runtime_metadata,
            cluster=cluster,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
        )
        execution_query = cli_jarvis_execution_run._run_post_run_jarvis_execution_query(
            cluster=cluster,
            definition=definition,
            queue=queue,
            profile=checkpoint_profile,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            retry_selector=retry_selector,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        finish_execution_query(
            builder_inputs=builder_inputs,
            execution_query=execution_query,
            checkpoint_profile=checkpoint_profile,
        )

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not report_written[0]:
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
                    "jarvis-mcp.completed", "complete virtual JARVIS MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(failure_report_path)
            raise

    cli._run_or_exit(guarded_action)
