"""One-use scheduler-launch-refusal and direct-execution proof consumption.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_consume_scheduler_launch_refusal``/``_consume_direct_execution_proof`` validate and
redact the one-use durable proofs a JARVIS preflight embeds in its runtime record.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Any, cast

from clio_relay.adapters.worker.endpoint_scheduler_metadata import (
    _durable_scheduler_submission_intent,
    _runtime_sidecar_channel_failed,
)
from clio_relay.adapters.worker.endpoint_worker_environment import (
    _configured_scheduler_provider_name,
    _jarvis_pipeline_name,
)
from clio_relay.models import RelayJob
from clio_relay.runtime_metadata import JarvisRuntimeMetadata


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
