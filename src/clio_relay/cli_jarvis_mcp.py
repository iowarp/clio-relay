"""The ``jarvis-runtime-authority``/``mcp-call``/``jarvis-mcp-call``/
``jarvis-mcp-refresh``/``mcp-server`` top-level commands (iowarp/clio-relay#231
cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names the 13 flat, un-namespaced ``@app.command(...)`` entries directly on
``cli.py``'s top-level ``app`` as a group to split by concern. This module
owns five of the six jarvis-mcp concern commands: the private-authority
resolver systemd bootstrap units use, remote/JARVIS MCP tool submission, the
JARVIS contract-discovery refresh, and the native FastMCP server entry
point. ``jarvis-mcp-validate`` -- the sixth, and by far the largest at 453
body lines with its own dense JARVIS execution-query state machine -- moves
separately, into the sibling ``cli_jarvis_mcp_validate.py``, so this module
stays comfortably under the 800-line cap.

**Domain logic stays where it lives.** Each command delegates to its
already-correct owner (``resolve_local_jarvis_service_runtime_authority``,
``resolve_registered_remote_mcp_admission``/``resolve_pinned_mcp_admission``,
``fastmcp_server.run_fastmcp_stdio``/``run_fastmcp_http``); this module's own
code is parsing, submission-queue plumbing, and result rendering only,
ground rule 2.

**Registration seam.** Same as every other top-level command extraction
(``cli_diagnostics.py``, ``cli_init.py``, ``cli_installation_receipt.py``):
all five commands attach to the shared top-level ``app`` Typer instance
cli.py owns, not a namespaced sub-app, so cli.py imports this module for its
plain function objects and applies the registration itself.

**The JARVIS execution-query engine stays cli.py-resident (unsequenced).**
``jarvis-mcp-refresh`` calls two cli.py-private helpers,
``_run_jarvis_remote_contract_discovery``/``_persist_jarvis_remote_contract_
discovery`` -- confirmed exclusive to the jarvis-mcp concern (their only
callers are this command and ``jarvis-mcp-validate``, both moving out of
cli.py in this same slice pair) but themselves part of a much larger, dense
JARVIS execution-query/checkpoint-resume/integrity-checking engine (~2,450
lines: contract discovery, package search, execution-intent/checkpoint
machinery, progress-integrity verification, the execution-query state
machine) that is genuine business logic cli.py should not own per ground
rule 2, yet whose extraction into a real owner module is a substantial,
separate design exercise of its own -- not a mechanical command-body move.
Named here explicitly (ground rule 4: gaps are first-class, not silently
dropped) as unsequenced future work, the same category
``cli_relay_host.py``'s own docstring already puts ``_run_transport_
validation`` in. Both this module and ``cli_jarvis_mcp_validate.py`` reach
the engine, and every other collaborator still resident in cli.py, through
cli.py's own name via the established function-local ``import clio_relay.cli
as cli`` discipline.

**Reassigned patch-seam callers.** ``fastmcp_server.run_fastmcp_stdio``/
``run_fastmcp_http`` (``mcp-server``'s only cli.py call sites) and
``remote_cli.write_remote_file``/``remove_remote_file`` (``mcp-call``'s and
``jarvis-mcp-call``'s only cli.py call sites, both now in this same module)
reassign their ``caller`` entry in ``AUDITED_COLLABORATORS`` from ``"cli"``
to ``"cli_jarvis_mcp"``. ``jarvis_mcp.jarvis_mcp_server`` keeps its ``"cli"``
caller unchanged: ``jarvis-mcp-call`` is one of its two call sites, but
``_run_jarvis_remote_contract_discovery`` (cli.py-resident, per the note
above) is the other, so cli.py itself still genuinely reaches it.
``remote_cli.should_execute_on_cluster``/``storage_runtime.
storage_managed_queue`` are also audited but used pervasively elsewhere in
cli.py, so they too keep their ``"cli"`` caller unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

import typer
from pydantic import ValidationError

import clio_relay.fastmcp_server as fastmcp_server
import clio_relay.jarvis_mcp as jarvis_mcp
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.jarvis_mcp import (
    is_virtual_jarvis_control_query,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_env_from,
    jarvis_mcp_server_args,
)
from clio_relay.jarvis_service_runtime import (
    private_jarvis_service_runtime_authority_document,
    resolve_local_jarvis_service_runtime_authority,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    JobKind,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    RelayJob,
)
from clio_relay.remote_cli import staged_remote_cluster_registry
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    default_remote_mcp_cache_path,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)
from clio_relay.storage_runtime import StorageAdmissionError

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false


def jarvis_runtime_authority(
    execution_id: str,
    pipeline_id: Annotated[str, typer.Option(help="Exact JARVIS pipeline identity.")],
    package_id: Annotated[str, typer.Option(help="Exact JARVIS package identity.")],
    service_instance_id: Annotated[
        str,
        typer.Option(help="Exact JARVIS service instance identity."),
    ],
    revision: Annotated[int, typer.Option(help="Exact current service revision.")],
    token_sha256: Annotated[
        str,
        typer.Option(help="Public SHA-256 identity of the private bearer capability."),
    ],
) -> None:
    """Resolve one private JARVIS authority for relay-internal transport."""
    import clio_relay.cli as cli

    def action() -> None:
        settings = RelaySettings.from_env()
        authority = resolve_local_jarvis_service_runtime_authority(
            jarvis_bin=settings.jarvis_bin,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            package_id=package_id,
            service_instance_id=service_instance_id,
            revision=revision,
            token_sha256=token_sha256,
        )
        typer.echo(
            json.dumps(
                private_jarvis_service_runtime_authority_document(authority),
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    cli._run_or_exit(action)


def mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    server: Annotated[str, typer.Option(help="Remote MCP server name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="Remote MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="Remote MCP tool name. Required for tools/call."),
    ] = None,
    server_arg: Annotated[
        list[str] | None,
        typer.Option(help="Additional remote MCP server argument. Repeatable."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
    ] = None,
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote MCP tool."),
    ] = None,
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
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time MCP server artifact SHA-256 binding."),
    ] = None,
    expected_registered_contract: Annotated[
        str | None,
        typer.Option(
            help="Internal expected operator-registered semantic contract.",
            hidden=True,
        ),
    ] = None,
    control_query_evidence_json: Annotated[
        str | None,
        typer.Option(
            help="Internal discovery evidence offered for server-side admission validation.",
            hidden=True,
        ),
    ] = None,
) -> None:
    """Submit a durable remote MCP call or schema-discovery operation."""
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = cli._json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    try:
        control_query_evidence = (
            McpControlQueryEvidence.model_validate_json(control_query_evidence_json)
            if control_query_evidence_json is not None
            else None
        )
    except ValidationError as exc:
        raise typer.BadParameter("--control-query-evidence-json is invalid") from exc
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    server_args = server_arg or []
    environment_references = cli._environment_references(env_from)
    artifact_uses = cli._artifact_use_refs(used_artifact)
    if remote_cli.should_execute_on_cluster(definition):
        remote_arguments_path: str | None = None
        remote_command = [
            "mcp-call",
            "--cluster",
            cluster,
            "--server",
            server,
            "--operation",
            operation.value,
        ]
        if idempotency_key is not None:
            remote_command.extend(["--idempotency-key", idempotency_key])
        if control_query_evidence is not None:
            remote_command.extend(
                [
                    "--control-query-evidence-json",
                    json.dumps(
                        control_query_evidence.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ]
            )
        if tool is not None:
            remote_arguments_path = (
                ".local/share/clio-relay/desktop-submissions/"
                f"mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_arguments_path])
        for child_name, source_name in sorted(environment_references.items()):
            remote_command.extend(["--env-from", f"{child_name}={source_name}"])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                [
                    "--expected-server-artifact-digest",
                    expected_server_artifact_digest,
                ]
            )
        if expected_registered_contract is not None:
            remote_command.extend(["--expected-registered-contract", expected_registered_contract])
        for ref in cli._artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", cli._artifact_use_cli_value(ref)])
        with staged_remote_cluster_registry(definition) as remote_registry_path:
            try:
                if remote_arguments_path is not None:
                    remote_cli.write_remote_file(
                        definition,
                        remote_arguments_path,
                        json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode(
                            "utf-8"
                        ),
                    )
                cli._run_remote_or_exit(
                    definition,
                    remote_command
                    + (
                        ["--timeout-seconds", str(timeout_seconds)]
                        if timeout_seconds is not None
                        else []
                    )
                    + [item for value in server_args for item in ("--server-arg", value)],
                    cluster_registry_path=remote_registry_path,
                )
            finally:
                if remote_arguments_path is not None:
                    remote_cli.remove_remote_file(
                        definition,
                        remote_arguments_path,
                        remove_empty_parent=True,
                    )
        return
    queue = cli._managed_queue_from_env()
    try:
        try:
            resolved_admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                server=server,
                server_args=server_args,
                env_from=environment_references,
                operation=operation,
                tool=tool,
                expected_server_artifact_digest=expected_server_artifact_digest,
                evidence=control_query_evidence,
                expected_registered_contract=expected_registered_contract,
                timeout_seconds=timeout_seconds,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        admission_identity: dict[str, object] = {
            "server": server,
            "args": server_args,
            "env_from": environment_references,
            "expected_server_artifact_digest": expected_server_artifact_digest,
        }
        if expected_registered_contract is not None:
            admission_identity["expected_registered_contract"] = expected_registered_contract
        if (
            resolved_admission_class is McpAdmissionClass.CONTROL_QUERY
            or admission_authority is not None
        ):
            admission_identity.update(
                {
                    "timeout_seconds": timeout_seconds,
                    "admission_class": resolved_admission_class.value,
                    "admission_authority": (
                        None
                        if admission_authority is None
                        else admission_authority.model_dump(mode="json")
                    ),
                }
            )
        server_digest = hashlib.sha256(
            json.dumps(
                admission_identity,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        key = idempotency_key or (
            f"mcp:{cluster}:{server_digest}:{operation.value}:{tool}:{digest}"
            + cli._artifact_use_idempotency_suffix(artifact_uses)
        )
        metadata = (
            {}
            if admission_authority is None
            else {MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(mode="json")}
        )
        job = RelayJob(
            cluster=cluster,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                server_args=server_args,
                env_from=environment_references,
                expected_server_artifact_digest=expected_server_artifact_digest,
                expected_registered_contract=expected_registered_contract,
                admission_class=resolved_admission_class,
                operation=operation,
                tool=tool,
                arguments=arguments,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=key,
            used_artifact_refs=artifact_uses,
            metadata=metadata,
        )
        try:
            saved = queue.submit_job(job)
        except StorageAdmissionError as exc:
            cli._echo_storage_admission_error(exc)
            raise typer.Exit(code=1) from exc
    finally:
        queue.close()
    typer.echo(saved.job_id)


def jarvis_mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="JARVIS MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="JARVIS MCP tool name. Required for tools/call."),
    ] = None,
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the JARVIS MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the JARVIS MCP tool."),
    ] = None,
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
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote JARVIS MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time JARVIS MCP artifact SHA-256 binding."),
    ] = None,
) -> None:
    """Submit a JARVIS MCP tool call that runs on the target cluster."""
    import clio_relay.cli as cli

    running_on_target = (
        os.getenv("CLIO_RELAY_CLI_MODE") == "local"
        and os.getenv("CLIO_RELAY_REMOTE_CLUSTER") == cluster
    )
    definition = None if running_on_target else cli._require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = cli._json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    try:
        resolved_admission_class, admission_authority = resolve_pinned_mcp_admission(
            operation=operation,
            tool=tool,
            expected_server_artifact_digest=expected_server_artifact_digest,
            pinned_control_query=(tool is not None and is_virtual_jarvis_control_query(tool)),
            timeout_seconds=timeout_seconds,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if resolved_admission_class is McpAdmissionClass.CONTROL_QUERY and timeout_seconds is None:
        timeout_seconds = MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    artifact_uses = cli._artifact_use_refs(used_artifact)
    legacy_key = (
        f"mcp:{cluster}:jarvis:{operation.value}:{tool}:{digest}:"
        f"{expected_server_artifact_digest or 'unbound'}"
        + cli._artifact_use_idempotency_suffix(artifact_uses)
    )
    key = idempotency_key or (
        legacy_key
        if resolved_admission_class is McpAdmissionClass.WORKLOAD
        else (
            f"{legacy_key}:{resolved_admission_class.value}:"
            f"{admission_authority.source if admission_authority is not None else 'none'}:"
            f"timeout={timeout_seconds}"
        )
    )
    if definition is not None and remote_cli.should_execute_on_cluster(definition):
        remote_args: str | None = None
        remote_command = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            operation.value,
            "--idempotency-key",
            key,
        ]
        if tool is not None:
            remote_args = (
                ".local/share/clio-relay/desktop-submissions/"
                f"jarvis-mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_args])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                [
                    "--expected-server-artifact-digest",
                    expected_server_artifact_digest,
                ]
            )
        for ref in cli._artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", cli._artifact_use_cli_value(ref)])
        try:
            if remote_args is not None:
                remote_cli.write_remote_file(
                    definition,
                    remote_args,
                    json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
            cli._run_remote_or_exit(
                definition,
                remote_command
                + (
                    ["--timeout-seconds", str(timeout_seconds)]
                    if timeout_seconds is not None
                    else []
                ),
            )
        finally:
            if remote_args is not None:
                remote_cli.remove_remote_file(definition, remote_args, remove_empty_parent=True)
        return
    server = jarvis_mcp.jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    metadata = (
        {}
        if admission_authority is None
        else {MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(mode="json")}
    )
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            env_from=jarvis_mcp_env_from(),
            expected_server_artifact_digest=expected_server_artifact_digest,
            expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
            admission_class=resolved_admission_class,
            operation=operation,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
        metadata=metadata,
    )
    saved = cli._submit_managed_job(job)
    typer.echo(saved.job_id)


def jarvis_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for durable tools/list discovery."),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Discovery job polling interval."),
    ] = 2,
) -> None:
    """Refresh the verified JARVIS contract and pre-launch artifact binding."""
    import clio_relay.cli as cli
    import clio_relay.cli_jarvis_remote_contract as cli_jarvis_remote_contract

    definition = cli._require_cluster(cluster)

    def action() -> None:
        queue = cli._managed_queue_from_env()
        queue.initialize()
        job_id, result, artifacts, artifact_payload = (
            cli_jarvis_remote_contract._run_jarvis_remote_contract_discovery(
                cluster=cluster,
                definition=definition,
                queue=queue,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
        )
        entry, binding = cli_jarvis_remote_contract._persist_jarvis_remote_contract_discovery(
            cluster=cluster,
            discovery_job_id=job_id,
            result=result,
            artifacts=artifacts,
            artifact_payload=artifact_payload,
        )
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "discovery_job_id": job_id,
                    "schema_digest": entry.schema_digest,
                    "server_artifact_digest": binding,
                    "expires_at": entry.expires_at.isoformat(),
                    "tool_names": sorted(tool.name for tool in entry.tools),
                    "cache_path": str(
                        default_remote_mcp_cache_path(
                            registry_path=default_registry_path(),
                        )
                    ),
                },
                indent=2,
            )
        )

    cli._run_or_exit(action)


def mcp_server(
    profile: Annotated[
        str,
        typer.Option(help="MCP tool profile: user, admin, operator, or all."),
    ] = "user",
    transport: Annotated[
        Literal["stdio", "http"],
        typer.Option(help="MCP transport: stdio or authenticated Streamable HTTP."),
    ] = "stdio",
    host: Annotated[
        str,
        typer.Option(help="HTTP bind address; ignored for stdio."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option(help="HTTP bind port; ignored for stdio."),
    ] = 8766,
    path: Annotated[
        str,
        typer.Option(help="Streamable HTTP MCP path; ignored for stdio."),
    ] = "/mcp",
) -> None:
    """Serve relay tools with native FastMCP and relay-backed SEP-2663 tasks."""
    if transport == "stdio":
        fastmcp_server.run_fastmcp_stdio(profile=profile)
        return
    fastmcp_server.run_fastmcp_http(profile=profile, host=host, port=port, path=path)
