"""JARVIS post-run execution-query orchestration (iowarp/clio-relay#231
continuation): drives one execution query to a terminal or pending
acceptance after a JARVIS run dispatch."""

from __future__ import annotations

import time
from typing import Any, Literal, cast

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
import clio_relay.cli_jarvis_execution_types as cli_jarvis_execution_types
import clio_relay.cli_jarvis_query_observation as cli_jarvis_query_observation
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.core_queue as core_queue
import clio_relay.mcp_stdio_validation as mcp_stdio_validation
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import ObservationTimeoutError, RelayError


def _run_post_run_jarvis_execution_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
    profile: str,
    pipeline_id: str,
    execution_id: str,
    retry_selector: dict[str, object] | None = None,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> (
    cli_jarvis_execution_types._JarvisExecutionQueryAcceptance
    | cli_jarvis_execution_types._JarvisExecutionQueryPending
):
    """Observe one handle-first run without treating a bounded wait as execution failure."""
    cli_remote_collection_pagination._validate_progress_wait(
        timeout_seconds=wait_timeout_seconds, poll_seconds=poll_seconds
    )
    query_arguments: dict[str, Any] = {
        "cluster": cluster,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "include_progress": True,
    }
    deadline = time.monotonic() + wait_timeout_seconds
    lifecycle_observations: list[dict[str, Any]] = []
    latest_attempt: cli_jarvis_execution_types._JarvisExecutionQueryAttempt | None = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if latest_attempt is not None:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=latest_attempt,
                    lifecycle_observations=lifecycle_observations,
                )
            return _unobserved_jarvis_execution_query_pending(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                retry_selector=retry_selector,
            )
        try:
            attempt = _execute_jarvis_execution_query(
                definition=definition,
                queue=queue,
                profile=profile,
                arguments=query_arguments,
                deadline=deadline,
                poll_seconds=poll_seconds,
            )
        except ObservationTimeoutError:
            if latest_attempt is None:
                return _unobserved_jarvis_execution_query_pending(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    retry_selector=retry_selector,
                )
            return _nonterminal_jarvis_execution_query_acceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                attempt=latest_attempt,
                lifecycle_observations=lifecycle_observations,
            )
        latest_attempt = attempt
        observation = _jarvis_execution_lifecycle_observation(
            attempt.mcp_result,
            query_job_id=attempt.call_job_id,
            expected_pipeline_id=pipeline_id,
            expected_execution_id=execution_id,
        )
        cli_jarvis_query_observation._append_bounded_jarvis_execution_query_observation(
            lifecycle_observations,
            observation,
        )
        if observation["terminal"] is True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    lifecycle_observations=lifecycle_observations,
                    outcome="terminal_artifacts_pending",
                )
            try:
                terminal_attempt = _execute_jarvis_execution_query(
                    definition=definition,
                    queue=queue,
                    profile=profile,
                    arguments={**query_arguments, "artifacts": {"page_size": 100}},
                    deadline=deadline,
                    poll_seconds=poll_seconds,
                )
            except ObservationTimeoutError:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    lifecycle_observations=lifecycle_observations,
                    outcome="terminal_artifacts_pending",
                )
            terminal_observation = _jarvis_execution_lifecycle_observation(
                terminal_attempt.mcp_result,
                query_job_id=terminal_attempt.call_job_id,
                expected_pipeline_id=pipeline_id,
                expected_execution_id=execution_id,
            )
            if terminal_observation["terminal"] is not True:
                raise RelayError(
                    "JARVIS execution regressed from terminal during its artifact query"
                )
            cli_jarvis_query_observation._append_bounded_jarvis_execution_query_observation(
                lifecycle_observations,
                terminal_observation,
            )
            return cli_jarvis_execution_types._JarvisExecutionQueryAcceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                outcome="terminal",
                tools_list_response=terminal_attempt.session.tools_list_response,
                call_response=terminal_attempt.session.tools_call_response,
                call_job_id=terminal_attempt.call_job_id,
                call_status=terminal_attempt.call_status,
                artifacts=terminal_attempt.artifacts,
                mcp_result=terminal_attempt.mcp_result,
                provenance=terminal_attempt.provenance,
                initialize_response=terminal_attempt.session.initialize_response,
                stdio_evidence=terminal_attempt.session.evidence(),
                lifecycle_observations=lifecycle_observations,
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return _nonterminal_jarvis_execution_query_acceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                attempt=attempt,
                lifecycle_observations=lifecycle_observations,
            )
        time.sleep(min(poll_seconds, remaining))


def _unobserved_jarvis_execution_query_pending(
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
    retry_selector: dict[str, object] | None,
) -> cli_jarvis_execution_types._JarvisExecutionQueryPending:
    """Preserve an exact query selector when the first observation window expires."""
    selector: dict[str, object] = {
        "cluster": cluster,
        "scheduler_cluster": None,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "last_query_job_id": None,
    }
    if retry_selector is not None:
        selector.update(retry_selector)
    if (
        selector.get("cluster") != cluster
        or selector.get("pipeline_id") != pipeline_id
        or selector.get("execution_id") != execution_id
        or selector.get("last_query_job_id") is not None
    ):
        raise RelayError("JARVIS execution query retry selector changed its durable identity")
    return cli_jarvis_execution_types._JarvisExecutionQueryPending(
        cluster=cluster,
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        selector=selector,
    )


def _nonterminal_jarvis_execution_query_acceptance(
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
    attempt: cli_jarvis_execution_types._JarvisExecutionQueryAttempt,
    lifecycle_observations: list[dict[str, Any]],
    outcome: Literal["observation_unknown", "terminal_artifacts_pending"] = "observation_unknown",
) -> cli_jarvis_execution_types._JarvisExecutionQueryAcceptance:
    """Return the last proven snapshot with a query-only retry selector."""
    return cli_jarvis_execution_types._JarvisExecutionQueryAcceptance(
        cluster=cluster,
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        outcome=outcome,
        tools_list_response=attempt.session.tools_list_response,
        call_response=attempt.session.tools_call_response,
        call_job_id=attempt.call_job_id,
        call_status=attempt.call_status,
        artifacts=attempt.artifacts,
        mcp_result=attempt.mcp_result,
        provenance=attempt.provenance,
        initialize_response=attempt.session.initialize_response,
        stdio_evidence=attempt.session.evidence(),
        lifecycle_observations=lifecycle_observations,
    )


def _execute_jarvis_execution_query(
    *,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
    profile: str,
    arguments: dict[str, Any],
    deadline: float,
    poll_seconds: float,
) -> cli_jarvis_execution_types._JarvisExecutionQueryAttempt:
    """Execute one query with the workload deadline applied to every boundary."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ObservationTimeoutError("JARVIS execution query deadline expired before MCP dispatch")
    timeout_seconds = min(60.0, max(0.001, remaining))
    session = mcp_stdio_validation.run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_get_execution",
        arguments=arguments,
        timeout_seconds=timeout_seconds,
    )
    call_job_id = cli_jarvis_artifact_io._mcp_response_job_id(session.tools_call_response)
    timeout_seconds = deadline - time.monotonic()
    if timeout_seconds <= 0:
        raise ObservationTimeoutError(
            f"JARVIS execution query dispatch exceeded its deadline: {call_job_id}"
        )
    if remote_cli.should_execute_on_cluster(definition):
        call_status = cli_remote_collection_pagination._wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            deadline=deadline,
        )
        artifacts = cli_jarvis_artifact_io._remote_artifact_records(
            definition,
            call_job_id,
            deadline=deadline,
        )
        mcp_result = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
            deadline=deadline,
        )
        provenance = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
            deadline=deadline,
        )
    else:
        call_status = cli_remote_collection_pagination._wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = cli_remote_collection_pagination._complete_local_artifact_records(
            queue, call_job_id
        )
        mcp_result = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="mcp_result"
        )
        provenance = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="provenance"
        )
    return cli_jarvis_execution_types._JarvisExecutionQueryAttempt(
        session=session,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
    )


def _jarvis_execution_lifecycle_observation(
    mcp_result: dict[str, Any] | None,
    *,
    query_job_id: str,
    expected_pipeline_id: str,
    expected_execution_id: str,
) -> dict[str, Any]:
    """Extract one identity-bound workload observation from a query result."""
    structured = (
        cast(dict[str, Any], mcp_result.get("structured_result"))
        if mcp_result is not None and isinstance(mcp_result.get("structured_result"), dict)
        else None
    )
    record = (
        cast(dict[str, Any], structured.get("execution_record"))
        if structured is not None and isinstance(structured.get("execution_record"), dict)
        else None
    )
    handle = (
        cast(dict[str, Any], structured.get("execution_handle"))
        if structured is not None and isinstance(structured.get("execution_handle"), dict)
        else None
    )
    progress = (
        cast(dict[str, Any], structured.get("progress"))
        if structured is not None and isinstance(structured.get("progress"), dict)
        else None
    )
    if structured is None or handle is None or record is None or progress is None:
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} omitted its structured lifecycle result"
        )
    if (
        structured.get("pipeline_id") != expected_pipeline_id
        or structured.get("execution_id") != expected_execution_id
        or record.get("pipeline_id") != expected_pipeline_id
        or record.get("execution_id") != expected_execution_id
        or handle.get("pipeline_id") != expected_pipeline_id
        or handle.get("execution_id") != expected_execution_id
        or progress.get("pipeline_id") != expected_pipeline_id
        or progress.get("execution_id") != expected_execution_id
    ):
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} returned a different execution identity"
        )
    state = record.get("state")
    terminal = record.get("terminal")
    if not isinstance(state, str) or not isinstance(terminal, bool):
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} returned an invalid lifecycle state"
        )
    return {
        "query_job_id": query_job_id,
        "pipeline_id": expected_pipeline_id,
        "execution_id": expected_execution_id,
        "state": state,
        "terminal": terminal,
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": structured.get("runtime_metadata"),
    }
