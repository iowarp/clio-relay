"""JARVIS execution-recovery durable bookkeeping: process ownership, timeouts,
failure/retry backoff, and resolution.

Owner module for iowarp/clio-relay#231's endpoint decomposition. Companion to
``endpoint_jarvis_recovery_query.py``'s recovery-query transport: records the recovery
query's own process ownership (``_record_jarvis_recovery_process``), a query timeout
(``_record_jarvis_recovery_query_timeout``), clears ownership after the process exits
(``_clear_jarvis_recovery_process``), keeps a failed attempt durable and retryable with
exponential backoff (``_record_jarvis_recovery_failure``), and marks the recovery
resolved from one verified native execution observation
(``_resolve_jarvis_execution_recovery``).
"""

from __future__ import annotations

import hashlib
import os
import re
import socket
from datetime import timedelta
from pathlib import Path
from typing import cast

from clio_relay import process_containment
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery,
)
from clio_relay.endpoint_scheduler_metadata import (
    _native_runtime_execution_mode,
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_RETRY_BASE_SECONDS,
    MCP_JARVIS_EXECUTION_RECOVERY_RETRY_MAX_SECONDS,
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    RelayJob,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)


class JarvisRecoveryBookkeepingMixin:
    """Mixin: JarvisRecoveryBookkeeping methods split from EndpointWorker (clio-relay#231)."""

    def _record_jarvis_recovery_process(
        self,
        job: RelayJob,
        *,
        task_id: str,
        process_id: int,
    ) -> None:
        """Persist exact ownership of the read-only recovery query process."""
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None or intent["state"] != "pending":
            raise RelayError("JARVIS execution recovery process has no pending intent")
        start_identity = process_containment.process_start_identity(process_id)
        if start_identity is None:
            start_identity = f"process-not-observed:{process_id}"
        ownership = {
            "schema_version": "clio-relay.execution-ownership.v1",
            "pid": process_id,
            "hostname": socket.gethostname(),
            "process_start_identity": start_identity,
            "process_group_id": process_id if os.name != "nt" else None,
            "started_at": utc_now().isoformat(),
            "endpoint_id": None if self.endpoint is None else self.endpoint.endpoint_id,
            "containment": process_containment.owned_process_metadata(process_id),
        }
        self.queue.update_task_metadata(
            task_id,
            {"jarvis_execution_recovery": {**intent, "query_process": ownership}},
        )

    def _record_jarvis_recovery_query_timeout(
        self,
        job: RelayJob,
        *,
        task_id: str,
        attempt: int,
    ) -> None:
        """Record a recovery transport timeout without requesting cancellation."""
        self.queue.append_event(
            job.job_id,
            "jarvis.execution_recovery_query_timeout",
            "Artifact-pinned JARVIS execution recovery query timed out",
            payload={
                "task_id": task_id,
                "attempt": attempt,
                "scheduler_cancel_requested": False,
            },
        )

    def _clear_jarvis_recovery_process(self, job: RelayJob, *, task_id: str) -> None:
        """Clear recovery process ownership after the process has terminated."""
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None or intent.get("query_process") is None:
            return
        self.queue.update_task_metadata(
            task_id,
            {"jarvis_execution_recovery": {**intent, "query_process": None}},
        )

    def _record_jarvis_recovery_failure(
        self,
        job: RelayJob,
        *,
        task_id: str,
        error: str,
        result_sha256: str | None,
    ) -> None:
        """Keep a failed recovery durable and retryable without cancellation intent."""
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None:
            raise RelayError("JARVIS execution recovery failure has no durable intent")
        if intent["state"] != "pending" or intent["dispatch_state"] != "started":
            raise RelayError("JARVIS execution recovery failure is not pending dispatch")
        bounded = bounded_error_detail(error)
        attempts = cast(int, intent["attempts"])
        exponent = min(max(attempts - 1, 0), 16)
        retry_delay_seconds = min(
            MCP_JARVIS_EXECUTION_RECOVERY_RETRY_BASE_SECONDS * (2**exponent),
            MCP_JARVIS_EXECUTION_RECOVERY_RETRY_MAX_SECONDS,
        )
        next_retry_at = utc_now() + timedelta(seconds=retry_delay_seconds)
        self.queue.update_task_metadata(
            task_id,
            {
                "jarvis_execution_recovery": {
                    **intent,
                    "state": "pending",
                    "last_error": bounded,
                    "next_retry_at": next_retry_at.isoformat(),
                    "result_sha256": result_sha256,
                    "query_process": None,
                }
            },
        )
        self.queue.append_event(
            job.job_id,
            "jarvis.execution_recovery_pending",
            "JARVIS execution recovery remains pending for endpoint reconciliation",
            payload={
                "task_id": task_id,
                "pipeline_id": intent["pipeline_id"],
                "execution_id": intent["execution_id"],
                "error": bounded,
                "attempt": attempts,
                "retry_delay_seconds": retry_delay_seconds,
                "next_retry_at": next_retry_at.isoformat(),
                "scheduler_cancel_requested": False,
            },
        )

    def _resolve_jarvis_execution_recovery(
        self,
        job: RelayJob,
        *,
        task_id: str,
        metadata: JarvisRuntimeMetadata,
        resolution: str,
        result_path: Path,
        verified_result_sha256: str | None = None,
    ) -> None:
        """Mark one exact native execution observation as the recovery resolution."""
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None:
            return
        if (
            intent["state"] != "pending"
            or intent["dispatch_state"] != "started"
            or not _runtime_metadata_is_native(metadata)
            or metadata.pipeline_id != intent["pipeline_id"]
            or metadata.execution_id != intent["execution_id"]
            or resolution not in {"dispatch_result", "execution_query"}
        ):
            raise RelayError("JARVIS execution recovery resolution did not match its intent")
        execution_mode = _native_runtime_execution_mode(metadata)
        scheduler_identity_present = (
            metadata.scheduler_provider is not None or metadata.scheduler_job_id is not None
        )
        if execution_mode == "direct" and scheduler_identity_present:
            raise RelayError("direct JARVIS execution recovery carried scheduler identity")
        if execution_mode == "scheduler" and (
            metadata.scheduler_provider is None or metadata.scheduler_job_id is None
        ):
            raise SchedulerSubmissionUnresolvedError(
                "scheduled JARVIS execution recovery is awaiting scheduler identity"
            )
        if verified_result_sha256 is None:
            storage_result = internal_filesystem_path(result_path)
            if not storage_result.is_file():
                raise RelayError("JARVIS execution recovery resolution has no result artifact")
            result_sha256 = hashlib.sha256(storage_result.read_bytes()).hexdigest()
        else:
            if not re.fullmatch(r"[0-9a-f]{64}", verified_result_sha256):
                raise RelayError("JARVIS execution recovery result hash is invalid")
            result_sha256 = verified_result_sha256
        resolved = {
            **intent,
            "state": "resolved",
            "last_error": None,
            "next_retry_at": None,
            "result_sha256": result_sha256,
            "resolved_at": utc_now().isoformat(),
            "resolution": resolution,
            "scheduler_provider": metadata.scheduler_provider,
            "scheduler_job_id": metadata.scheduler_job_id,
            "query_process": None,
        }
        updates: dict[str, object] = {"jarvis_execution_recovery": resolved}
        if metadata.scheduler_provider is None and metadata.scheduler_job_id is None:
            sidecars = task.metadata.get("execution_sidecars")
            if isinstance(sidecars, dict):
                updates["execution_sidecars"] = {
                    **cast(dict[str, object], sidecars),
                    "scheduler_expected_resolved": False,
                }
        self.queue.update_task_metadata(task_id, updates)
        self.queue.append_event(
            job.job_id,
            "jarvis.execution_recovered",
            "JARVIS execution ownership resolved from native durable metadata",
            payload={
                "task_id": task_id,
                "pipeline_id": metadata.pipeline_id,
                "execution_id": metadata.execution_id,
                "scheduler_provider": metadata.scheduler_provider,
                "scheduler_job_id": metadata.scheduler_job_id,
                "resolution": resolution,
                "result_sha256": result_sha256,
            },
        )
