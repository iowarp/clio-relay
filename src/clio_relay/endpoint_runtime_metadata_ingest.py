"""One-use scheduler-launch-refusal/direct-execution proof consumption plus the
remote-MCP-call runtime metadata ingest path.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_consume_scheduler_launch_refusal``/``_consume_direct_execution_proof`` validate and
redact the one-use durable proofs a JARVIS preflight embeds in its runtime record;
``_ingest_mcp_runtime_metadata`` reads ``mcp-result.json`` for a remote
(non-endpoint-owned) MCP call and adopts native runtime metadata from it.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from typing import Any, cast

from clio_relay.endpoint_jarvis_recovery import (
    _attributed_jarvis_dispatch_refusal,
    _trusted_jarvis_mcp_result,
    _trusted_jarvis_mcp_route,
)
from clio_relay.endpoint_scheduler_metadata import (
    _durable_scheduler_submission_intent,
    _runtime_metadata_is_mcp_transport_wrapper,
    _runtime_metadata_is_native,
    _runtime_sidecar_channel_failed,
)
from clio_relay.endpoint_worker_environment import (
    _configured_scheduler_provider_name,
    _jarvis_pipeline_name,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.jarvis_dispatch_failure import (
    McpRuntimeIngestOutcome,
)
from clio_relay.models import (
    McpCallSpec,
    RelayJob,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
    runtime_metadata_from_mcp_result_document,
)
from clio_relay.spool import JobSpool


class RuntimeMetadataIngestMixin:
    """Mixin: RuntimeMetadataIngest methods split from EndpointWorker (clio-relay#231)."""

    def _consume_scheduler_launch_refusal(
        self,
        job: RelayJob,
        *,
        task_id: str,
        metadata: JarvisRuntimeMetadata,
    ) -> JarvisRuntimeMetadata:
        """Validate proof that the broker rejected a named scheduler before submit."""
        raw_details = metadata.details.get("details")
        details = cast(dict[str, Any], raw_details) if isinstance(raw_details, dict) else {}
        proof = details.get("scheduler_launch_refusal_proof")
        if proof is None:
            return metadata
        task = self.queue.get_task(task_id)
        if _runtime_sidecar_channel_failed(task):
            raise ValueError("scheduler launch refusal cannot resolve a failed runtime channel")
        intent = _durable_scheduler_submission_intent(task)
        configured_provider = _configured_scheduler_provider_name(self.scheduler_provider)
        requested_provider = details.get("scheduler_provider")
        if (
            not isinstance(proof, str)
            or not proof
            or intent["scheduler_expected"] != "unknown"
            or metadata.execution_id != intent["execution_id"]
            or metadata.pipeline_id != _jarvis_pipeline_name(job)
            or metadata.scheduler_job_id is not None
            or metadata.scheduler_phase != "launch_refused"
            or metadata.terminal.state != "launch_refused"
            or metadata.terminal.terminal is not True
            or metadata.terminal.returncode != 2
            or details.get("execution_owner") != "jarvis_cd.pipeline.preflight"
            or details.get("execution_mode") != "scheduler"
            or details.get("scheduler_expected") != intent["scheduler_expected"]
            or details.get("scheduler_submission_attempted") is not False
            or details.get("scheduler_launch_refused") is not True
            or not isinstance(requested_provider, str)
            or metadata.scheduler_provider != requested_provider
            or details.get("configured_scheduler_provider") != configured_provider
            or (requested_provider == configured_provider and requested_provider == "slurm")
            or not secrets.compare_digest(
                hashlib.sha256(proof.encode("utf-8")).hexdigest(),
                cast(str, intent["direct_proof_sha256"]),
            )
        ):
            raise ValueError("scheduler launch refusal did not match durable intent")
        sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
        if sidecars.get("scheduler_submission_refused") is not True:
            self.queue.update_task_metadata(
                task_id,
                {
                    "execution_sidecars": {
                        **sidecars,
                        "scheduler_submission_refused": True,
                    }
                },
            )
            self.queue.append_event(
                job.job_id,
                "scheduler.launch_refused",
                "Authenticated JARVIS preflight refused scheduler launch before submission",
                payload={
                    "task_id": task_id,
                    "execution_id": metadata.execution_id,
                    "requested_provider": requested_provider,
                    "configured_provider": configured_provider,
                    "scheduler_submission_attempted": False,
                },
            )
        redacted_details = {**details}
        redacted_details.pop("scheduler_launch_refusal_proof", None)
        return metadata.model_copy(
            update={
                "details": {
                    **metadata.details,
                    "details": redacted_details,
                }
            }
        )

    def _consume_direct_execution_proof(
        self,
        job: RelayJob,
        *,
        task_id: str,
        metadata: JarvisRuntimeMetadata,
    ) -> JarvisRuntimeMetadata:
        """Validate and redact the one-use proof that a named pipeline is direct."""
        raw_details = metadata.details.get("details")
        details = cast(dict[str, Any], raw_details) if isinstance(raw_details, dict) else {}
        proof = details.get("direct_execution_proof")
        if proof is None:
            return metadata
        task = self.queue.get_task(task_id)
        if _runtime_sidecar_channel_failed(task):
            raise ValueError(
                "direct JARVIS execution proof cannot resolve a failed runtime channel"
            )
        intent = _durable_scheduler_submission_intent(task)
        if (
            not isinstance(proof, str)
            or not proof
            or intent["scheduler_expected"] != "unknown"
            or metadata.execution_id != intent["execution_id"]
            or metadata.scheduler_provider is not None
            or metadata.scheduler_job_id is not None
            or details.get("execution_mode") != "direct"
            or details.get("scheduler_expected") is not False
            or not secrets.compare_digest(
                hashlib.sha256(proof.encode("utf-8")).hexdigest(),
                cast(str, intent["direct_proof_sha256"]),
            )
        ):
            raise ValueError("direct JARVIS execution proof did not match durable intent")
        sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
        if sidecars.get("scheduler_expected_resolved") is not False:
            self.queue.update_task_metadata(
                task_id,
                {
                    "execution_sidecars": {
                        **sidecars,
                        "scheduler_expected_resolved": False,
                    }
                },
            )
            self.queue.append_event(
                job.job_id,
                "scheduler.direct_execution_confirmed",
                "Authenticated JARVIS load confirmed a direct named pipeline",
                payload={"task_id": task_id, "execution_id": metadata.execution_id},
            )
        redacted_details = {**details}
        redacted_details.pop("direct_execution_proof", None)
        return metadata.model_copy(
            update={
                "details": {
                    **metadata.details,
                    "details": redacted_details,
                }
            }
        )

    def _ingest_mcp_runtime_metadata(
        self,
        job: RelayJob,
        *,
        task_id: str,
        spool: JobSpool,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
    ) -> McpRuntimeIngestOutcome:
        """Ingest structured runtime metadata returned by a remote MCP call.

        Returns:
            Whether native runtime metadata was adopted, together with the typed
            refusal when the pinned route answered with an explicit tool error.
        """
        route_valid, _route_reason = _trusted_jarvis_mcp_route(job)
        if not route_valid:
            return McpRuntimeIngestOutcome(ingested=False)
        result_path = spool.path / "mcp-result.json"
        storage_result_path = internal_filesystem_path(result_path)
        if not storage_result_path.exists():
            return McpRuntimeIngestOutcome(ingested=False)
        try:
            result_document = json.loads(storage_result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_read_failed",
                f"MCP runtime result could not be read: {exc}",
            )
            return McpRuntimeIngestOutcome(ingested=False)
        trusted, reason = _trusted_jarvis_mcp_result(job, result_document)
        if not trusted:
            refusal = _attributed_jarvis_dispatch_refusal(job, result_document)
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                f"Refused JARVIS MCP runtime metadata: {reason}",
                payload={
                    "source": RuntimeMetadataSource.JARVIS_MCP.value,
                    "ownership_verified": False,
                    "reason": reason,
                    "dispatch_refused": refusal is not None,
                },
            )
            return McpRuntimeIngestOutcome(ingested=False, refusal=refusal)
        try:
            metadata = runtime_metadata_from_mcp_result_document(result_document)
        except ValueError as exc:
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                f"Refused invalid native JARVIS execution documents: {exc}",
                payload={
                    "source": RuntimeMetadataSource.JARVIS_MCP.value,
                    "ownership_verified": False,
                    "reason": str(exc),
                },
            )
            raise ConfigurationError(
                f"native JARVIS execution documents were invalid: {exc}"
            ) from exc
        if metadata is None:
            return McpRuntimeIngestOutcome(ingested=False)
        if not _runtime_metadata_is_native(metadata):
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                "Refused non-native JARVIS MCP execution metadata",
                payload={
                    "source": RuntimeMetadataSource.JARVIS_MCP.value,
                    "ownership_verified": False,
                    "reason": "native handle, record, and progress documents are required",
                },
            )
            return McpRuntimeIngestOutcome(ingested=False)
        expected_pipeline_id = (
            job.spec.arguments.get("pipeline_id") if isinstance(job.spec, McpCallSpec) else None
        )
        if metadata.pipeline_id != expected_pipeline_id:
            reason = "native JARVIS pipeline identity did not match the durable MCP request"
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                f"Refused JARVIS MCP runtime metadata: {reason}",
                payload={
                    "source": RuntimeMetadataSource.JARVIS_MCP.value,
                    "ownership_verified": False,
                    "reason": reason,
                },
            )
            raise ConfigurationError(reason)
        expected_execution_id = (
            job.spec.arguments.get("execution_id") if isinstance(job.spec, McpCallSpec) else None
        )
        if metadata.execution_id != expected_execution_id:
            reason = "native JARVIS execution identity did not match the durable MCP request"
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                f"Refused JARVIS MCP runtime metadata: {reason}",
                payload={
                    "source": RuntimeMetadataSource.JARVIS_MCP.value,
                    "ownership_verified": False,
                    "reason": reason,
                },
            )
            raise ConfigurationError(reason)
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
        )
        self._resolve_jarvis_execution_recovery(
            job,
            task_id=task_id,
            metadata=metadata,
            resolution="dispatch_result",
            result_path=result_path,
        )
        return McpRuntimeIngestOutcome(ingested=True)
