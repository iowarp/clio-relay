"""JARVIS remote contract discovery (iowarp/clio-relay#231 continuation):
the handshake that discovers and persists a remote cluster's JARVIS MCP
contract identity, shared by ``jarvis-mcp-call``/``jarvis-mcp-refresh``
and ``jarvis-mcp-validate``."""

from __future__ import annotations

import math
from typing import Any, cast
from uuid import uuid4

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.core_queue as core_queue
import clio_relay.jarvis_mcp as jarvis_mcp
import clio_relay.relay_ops as relay_ops
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
    RemoteMcpServerConfig,
    default_registry_path,
)
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding_from_entry,
    jarvis_mcp_env_from,
    jarvis_mcp_server_args,
    require_handle_first_jarvis_run_schema,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    JobKind,
    JobState,
    McpCallSpec,
    McpOperation,
    RelayJob,
)
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    cache_entry_from_discovery_artifact,
    default_remote_mcp_cache_path,
    resolve_pinned_mcp_admission,
)


def _require_discovery_success(result: dict[str, object], job_id: str) -> None:
    state = result.get("state")
    if state != JobState.SUCCEEDED.value:
        error = result.get("error")
        detail = f": {error}" if isinstance(error, str) and error else ""
        raise RelayError(f"remote MCP discovery job {job_id} ended in state {state}{detail}")


def _run_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> tuple[str, dict[str, Any], list[dict[str, Any]], bytes]:
    """Discover the actual cluster-side JARVIS MCP before accepting its virtual route."""
    idempotency_key = f"mcp:jarvis-contract:{cluster}:{uuid4().hex}"
    if remote_cli.should_execute_on_cluster(definition):
        remote_args = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            "tools/list",
            "--idempotency-key",
            idempotency_key,
            "--timeout-seconds",
            str(
                min(
                    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
                    max(1, math.ceil(wait_timeout_seconds)),
                )
            ),
        ]
        job_id = cli_remote_worker_probe._last_nonempty_line(
            remote_cli.run_remote_clio(definition, remote_args)
        )
        terminal = cli_remote_collection_pagination._json_output(
            remote_cli.run_remote_clio(
                definition,
                [
                    "job",
                    "wait",
                    job_id,
                    "--timeout-seconds",
                    str(wait_timeout_seconds),
                    "--poll-seconds",
                    str(poll_seconds),
                ],
            ),
            "JARVIS MCP discovery wait",
        )
        _require_discovery_success(terminal, job_id)
        artifacts = cli_jarvis_artifact_io._remote_artifact_records(definition, job_id)
        artifact_payload = cli_jarvis_artifact_io._read_remote_artifact_kind_bytes(
            definition,
            artifacts,
            kind="mcp_result",
        )
    else:
        server = jarvis_mcp.jarvis_mcp_server()
        server_args = jarvis_mcp_server_args()
        admission_class, admission_authority = resolve_pinned_mcp_admission(
            operation=McpOperation.TOOLS_LIST,
            tool=None,
            expected_server_artifact_digest=None,
            pinned_control_query=False,
            timeout_seconds=MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
        )
        assert admission_authority is not None
        submitted = queue.submit_job(
            RelayJob(
                cluster=cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=server,
                    server_args=server_args,
                    env_from=jarvis_mcp_env_from(),
                    expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
                    admission_class=admission_class,
                    operation=McpOperation.TOOLS_LIST,
                    timeout_seconds=MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
                ),
                idempotency_key=idempotency_key,
                metadata={
                    MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(
                        mode="json"
                    )
                },
            )
        )
        job_id = submitted.job_id
        terminal_job = relay_ops.wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_discovery_success(terminal_job.model_dump(mode="json"), job_id)
        artifacts = cli_remote_collection_pagination._complete_local_artifact_records(queue, job_id)
        artifact_payload = cli_jarvis_artifact_io._read_local_artifact_kind_bytes(
            queue,
            artifacts,
            kind="mcp_result",
        )
    if artifact_payload is None:
        raise RelayError("JARVIS MCP discovery did not produce an mcp_result artifact")
    result = cli_jarvis_artifact_io._decode_json_artifact(artifact_payload, kind="mcp_result")
    return job_id, result, artifacts, artifact_payload


def _persist_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    discovery_job_id: str,
    result: dict[str, Any],
    artifacts: list[dict[str, Any]],
    artifact_payload: bytes,
) -> tuple[RemoteMcpSchemaCacheEntry, str]:
    """Persist and verify the exact discovery identity used by built-in JARVIS calls."""
    durable_result = cli_jarvis_artifact_io._decode_json_artifact(
        artifact_payload, kind="mcp_result"
    )
    if durable_result != result:
        raise RelayError(
            "JARVIS MCP discovery result did not match its durable mcp_result artifact"
        )
    result = durable_result
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()
    if result.get("expected_jarvis_cd_lock_binding") != expected_jarvis_cd_lock_binding:
        raise RelayError("JARVIS MCP discovery did not enforce the relay JARVIS-CD lock pin")
    server = result.get("server")
    raw_server_args = result.get("server_args")
    raw_env_from = result.get("env_from", {})
    if not isinstance(server, str) or not server:
        raise RelayError("JARVIS MCP discovery result has no server command")
    if not isinstance(raw_server_args, list) or not all(
        isinstance(item, str) for item in cast(list[object], raw_server_args)
    ):
        raise RelayError("JARVIS MCP discovery result has invalid server arguments")
    if not isinstance(raw_env_from, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in cast(dict[object, object], raw_env_from).items()
    ):
        raise RelayError("JARVIS MCP discovery result has invalid environment references")
    artifact = cli_jarvis_artifact_io._artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError("JARVIS MCP discovery has no durable result artifact")
    artifact_id = artifact.get("artifact_id")
    artifact_sha256 = artifact.get("sha256")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("JARVIS MCP discovery result artifact has no artifact_id")
    if artifact_sha256 is not None and not isinstance(artifact_sha256, str):
        raise RelayError("JARVIS MCP discovery result artifact has invalid SHA-256")
    registration = RemoteMcpServerConfig(
        command=server,
        args=cast(list[str], raw_server_args),
        env_from=cast(dict[str, str], raw_env_from),
        allow_tools=[
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_add_step",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        ],
        profiles=["user"],
    )
    entry = cache_entry_from_discovery_artifact(
        cluster=cluster,
        server_name=JARVIS_MCP_CACHE_SERVER_NAME,
        registration=registration,
        discovery_job_id=discovery_job_id,
        artifact_id=artifact_id,
        artifact_sha256=artifact_sha256,
        artifact_payload=artifact_payload,
    )
    run_tool = next((tool for tool in entry.tools if tool.name == "jarvis_run"), None)
    if run_tool is None:
        raise RelayError("JARVIS MCP discovery contract omitted jarvis_run")
    try:
        require_handle_first_jarvis_run_schema(run_tool.input_schema)
    except ValueError as exc:
        raise RelayError(str(exc)) from exc
    if entry.schema_digest != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RelayError(
            f"JARVIS MCP discovery contract does not match clio-kit {CLIO_KIT_JARVIS_MCP_VERSION}"
        )
    try:
        binding = jarvis_mcp_artifact_binding_from_entry(entry)
    except ValueError as exc:
        raise RelayError(str(exc)) from exc
    cache_path = default_remote_mcp_cache_path(registry_path=default_registry_path())
    RemoteMcpSchemaCache.update_entry(cache_path, entry)
    return entry, binding
