"""Artifact-pinned JARVIS execution-recovery query: the private ``jarvis_get_execution``
re-dispatch and its serialization claim.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_recover_jarvis_execution`` re-queries JARVIS for an execution whose run response was
lost (worker crash between dispatch and ingest), validating the recovered document
against the durable recovery intent at every step;
``_run_jarvis_execution_recovery_query`` runs that query inside a pinned private
directory; ``_jarvis_execution_recovery_claim`` serializes restart recovery across
manual and supervised workers via an exclusive file lock.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from filelock import FileLock, Timeout

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery,
    _minimal_mcp_runner_environment,
    _trusted_jarvis_execution_query_validation,
    _trusted_jarvis_mcp_result,
)
from clio_relay.endpoint_recovery_directory import (
    _close_recovery_directory_anchor,
    _open_or_create_recovery_directory,
    _read_owned_recovery_result,
    _recovery_timestamp,
    _remove_owned_recovery_output,
    _validate_recovery_directory_path,
    _validate_recovery_process_cwd,
    _write_private_json_file,
)
from clio_relay.endpoint_scheduler_metadata import (
    _native_runtime_created_at,
    _native_runtime_execution_mode,
    _runtime_metadata_is_mcp_transport_wrapper,
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_QUERY_PROCESS_TIMEOUT_SECONDS,
    MCP_JARVIS_EXECUTION_QUERY_TIMEOUT_SECONDS,
    MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    McpCallSpec,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    runtime_metadata_from_mcp_result_document,
)
from clio_relay.spool import JobSpool


class JarvisRecoveryQueryMixin:
    """Mixin: JarvisRecoveryQuery methods split from EndpointWorker (clio-relay#231)."""

    def _recover_jarvis_execution(
        self,
        job: RelayJob,
        *,
        task_id: str,
        spool: JobSpool,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
    ) -> bool:
        """Recover an owned JARVIS execution after its run response was lost."""
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None:
            return False
        if intent["state"] == "resolved":
            return True
        if intent["dispatch_state"] != "started":
            raise SchedulerSubmissionUnresolvedError(
                "JARVIS execution recovery refused an unreleased dispatch"
            )
        assert isinstance(job.spec, McpCallSpec)
        attempt = cast(int, intent["attempts"]) + 1
        attempted_at = utc_now()
        self.queue.update_task_metadata(
            task_id,
            {
                "jarvis_execution_recovery": {
                    **intent,
                    "attempts": attempt,
                    "last_attempt_at": attempted_at.isoformat(),
                    "last_error": None,
                    "next_retry_at": None,
                    "result_sha256": None,
                    "query_process": None,
                }
            },
        )
        self.queue.append_event(
            job.job_id,
            "jarvis.execution_recovery_started",
            "Querying the durable JARVIS execution after its run response was unavailable",
            payload={
                "task_id": task_id,
                "pipeline_id": intent["pipeline_id"],
                "execution_id": intent["execution_id"],
                "attempt": attempt,
            },
        )
        recovery_spec = McpCallSpec(
            server=job.spec.server,
            server_args=job.spec.server_args,
            env_from=job.spec.env_from,
            expected_server_artifact_digest=job.spec.expected_server_artifact_digest,
            expected_registered_contract=job.spec.expected_registered_contract,
            expected_jarvis_cd_lock_binding=job.spec.expected_jarvis_cd_lock_binding,
            tool="jarvis_get_execution",
            arguments={
                "pipeline_id": intent["pipeline_id"],
                "execution_id": intent["execution_id"],
                "artifacts": {"page_size": 100},
            },
            timeout_seconds=MCP_JARVIS_EXECUTION_QUERY_TIMEOUT_SECONDS,
        )
        recovery_job = job.model_copy(update={"spec": recovery_spec})
        try:
            completed, result_payload, result_path = self._run_jarvis_execution_recovery_query(
                job,
                task_id=task_id,
                spool=spool,
                intent=intent,
                recovery_spec=recovery_spec,
                attempt=attempt,
            )
        except Exception as exc:
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=f"{type(exc).__name__}: {exc}",
                result_sha256=None,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery transport is unavailable"
            ) from exc
        if completed.returncode != 0:
            detail = bounded_error_detail(completed.stderr or completed.stdout or "no detail")
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=f"recovery query exited {completed.returncode}: {detail}",
                result_sha256=None,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery did not return a valid execution"
            )
        try:
            document = json.loads(result_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=f"recovery result could not be read: {exc}",
                result_sha256=hashlib.sha256(result_payload).hexdigest(),
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery result is unavailable"
            ) from exc
        result_sha256 = hashlib.sha256(result_payload).hexdigest()
        trusted, reason = _trusted_jarvis_mcp_result(
            recovery_job,
            document,
            expected_tool="jarvis_get_execution",
        )
        if not trusted or not _trusted_jarvis_execution_query_validation(
            document,
            pipeline_id=cast(str, intent["pipeline_id"]),
            execution_id=cast(str, intent["execution_id"]),
        ):
            detail = reason if not trusted else "runner execution-query validation did not match"
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=detail,
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery result was not trusted"
            )
        parser_document = {**cast(dict[str, object], document), "tool": "jarvis_run"}
        try:
            metadata = runtime_metadata_from_mcp_result_document(parser_document)
        except ValueError as exc:
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=f"native recovery documents were invalid: {exc}",
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery documents were invalid"
            ) from exc
        if (
            metadata is None
            or not _runtime_metadata_is_native(metadata)
            or metadata.pipeline_id != intent["pipeline_id"]
            or metadata.execution_id != intent["execution_id"]
        ):
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error="native recovery identity did not match the durable execution intent",
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery identity did not match"
            )
        execution_created_at = _native_runtime_created_at(metadata)
        intent_created_at = _recovery_timestamp(cast(str, intent["created_at"]))
        if intent_created_at is None or execution_created_at < intent_created_at:
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=("native execution predates the relay dispatch intent and cannot be adopted"),
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS recovery refused a pre-existing execution"
            )
        metadata = metadata.model_copy(
            update={
                "details": {
                    **metadata.details,
                    "relay_execution_recovery": {
                        "schema_version": MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
                        "pipeline_id": intent["pipeline_id"],
                        "execution_id": intent["execution_id"],
                        "expected_server_artifact_digest": (
                            intent["expected_server_artifact_digest"]
                        ),
                        "result_sha256": result_sha256,
                        "attempt": attempt,
                    },
                }
            }
        )
        current_runtime = state[0]
        superseded_transport_runtime = (
            current_runtime
            if current_runtime is not None
            and current_runtime.execution_id != metadata.execution_id
            and _runtime_metadata_is_mcp_transport_wrapper(current_runtime)
            else None
        )
        self._persist_runtime_metadata(
            job,
            task_id=task_id,
            metadata=metadata,
            state=state,
            digests=digests,
            scheduler_job_ids=scheduler_job_ids,
            superseded_transport_runtime=superseded_transport_runtime,
            allow_artifact_pinned_recovery=True,
        )
        if state[0] is None or state[0].execution_id != intent["execution_id"]:
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error="recovered runtime metadata could not be persisted",
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "artifact-pinned JARVIS execution recovery could not be persisted"
            )
        recovered_mode = _native_runtime_execution_mode(metadata)
        if recovered_mode == "scheduler" and (
            metadata.scheduler_provider is None or metadata.scheduler_job_id is None
        ):
            self._record_jarvis_recovery_failure(
                job,
                task_id=task_id,
                error=(
                    "scheduled JARVIS execution is durable but its scheduler identity "
                    "is not available yet"
                ),
                result_sha256=result_sha256,
            )
            raise SchedulerSubmissionUnresolvedError(
                "scheduled JARVIS execution recovery is awaiting scheduler identity"
            )
        self._write_recovered_jarvis_run_result(
            job,
            query_document=cast(dict[str, object], document),
            spool=spool,
            recovery_result_sha256=result_sha256,
        )
        self._resolve_jarvis_execution_recovery(
            job,
            task_id=task_id,
            metadata=metadata,
            resolution="execution_query",
            result_path=result_path,
            verified_result_sha256=result_sha256,
        )
        return True

    def _run_jarvis_execution_recovery_query(
        self,
        job: RelayJob,
        *,
        task_id: str,
        spool: JobSpool,
        intent: dict[str, Any],
        recovery_spec: McpCallSpec,
        attempt: int,
    ) -> tuple[subprocess.CompletedProcess[str], bytes, Path]:
        """Run one query inside a pinned private directory and return trusted bytes."""
        if not isinstance(job.spec, McpCallSpec):
            raise RelayError("JARVIS execution recovery requires an MCP call spec")
        recovery_directory = spool.path / cast(str, intent["recovery_directory_name"])
        result_path = recovery_directory / "mcp-result.json"
        params_path = recovery_directory / "params.json"
        anchor, _created = _open_or_create_recovery_directory(
            recovery_directory,
            expected_metadata=intent.get("recovery_directory_anchor"),
        )
        try:
            _remove_owned_recovery_output(
                result_path,
                directory_anchor=anchor,
            )
            _write_private_json_file(
                params_path,
                recovery_spec.model_dump(mode="json", exclude_none=True),
                directory_anchor=anchor,
            )
            command = [
                sys.executable,
                "-c",
                (
                    "import json,sys; from pathlib import Path; "
                    "from clio_relay.mcp_call.runner import run_mcp_call_from_params; "
                    "params=json.loads(Path(sys.argv[1]).read_text(encoding='utf-8')); "
                    "raise SystemExit(run_mcp_call_from_params(params))"
                ),
                params_path.name,
            ]

            def record_started(process_id: int) -> None:
                _validate_recovery_process_cwd(
                    process_id,
                    directory=recovery_directory,
                    anchor=anchor,
                )
                self._record_jarvis_recovery_process(
                    job,
                    task_id=task_id,
                    process_id=process_id,
                )

            try:
                completed = self.provider.run_command_streaming(
                    command,
                    cwd=internal_filesystem_path(recovery_directory),
                    env=_minimal_mcp_runner_environment(job.spec.env_from),
                    on_start=record_started,
                    timeout_seconds=MCP_JARVIS_EXECUTION_QUERY_PROCESS_TIMEOUT_SECONDS,
                    on_timeout=lambda: self._record_jarvis_recovery_query_timeout(
                        job,
                        task_id=task_id,
                        attempt=attempt,
                    ),
                )
            finally:
                self._clear_jarvis_recovery_process(job, task_id=task_id)
            _validate_recovery_directory_path(recovery_directory, anchor)
            payload = (
                _read_owned_recovery_result(result_path, directory_anchor=anchor)
                if completed.returncode == 0
                else b""
            )
            return completed, payload, result_path
        finally:
            _close_recovery_directory_anchor(anchor)

    @contextmanager
    def _jarvis_execution_recovery_claim(
        self,
        job: RelayJob,
        *,
        task: RelayTask,
    ) -> Generator[None, None, None]:
        """Serialize restart recovery across manual and supervised workers."""
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None or intent["state"] != "pending":
            raise SchedulerSubmissionUnresolvedError(
                "JARVIS execution recovery no longer has a pending intent"
            )
        recovery_directory = (
            self.settings.spool_dir
            / job.job_id
            / cast(
                str,
                intent["recovery_directory_name"],
            )
        )
        anchor, _created = _open_or_create_recovery_directory(
            recovery_directory,
            expected_metadata=intent["recovery_directory_anchor"],
        )
        _close_recovery_directory_anchor(anchor)
        claim = FileLock(str(internal_filesystem_path(recovery_directory / ".claim.lock")))
        try:
            claim.acquire(timeout=0)
        except Timeout as exc:
            raise SchedulerSubmissionUnresolvedError(
                "another worker owns the JARVIS execution recovery claim"
            ) from exc
        try:
            refreshed = self.queue.get_task(task.task_id)
            refreshed_intent = _durable_jarvis_execution_recovery(job, refreshed)
            if refreshed_intent is None or refreshed_intent["state"] != "pending":
                raise SchedulerSubmissionUnresolvedError(
                    "JARVIS execution recovery resolved before claim acquisition"
                )
            yield
        finally:
            claim.release()
