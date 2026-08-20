"""Durable runtime-metadata merge/persist: the single choke point every runtime-metadata
source (stdout marker, sidecar record, MCP result, recovery query, scheduler
reconciliation) writes through.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_persist_runtime_metadata`` merges an incoming observation against the durable state,
de-duplicates by digest, and mirrors the result onto both the task and job records plus
their scheduler-job-id index.
"""

from __future__ import annotations

import hashlib
import json
from typing import cast

from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_exact_marker_reconciliation,
    _runtime_metadata_is_native,
    _runtime_sidecar_channel_failed,
    _task_direct_execution_pinned,
)
from clio_relay.errors import ConfigurationError
from clio_relay.models import (
    RelayJob,
    SchedulerPhase,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataIdentityConflictError,
    RuntimeMetadataSource,
    merge_runtime_metadata,
)


class RuntimeMetadataPersistMixin:
    """Mixin: RuntimeMetadataPersist methods split from EndpointWorker (clio-relay#231)."""

    def _persist_runtime_metadata(
        self,
        job: RelayJob,
        *,
        task_id: str,
        metadata: JarvisRuntimeMetadata,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
        superseded_transport_runtime: JarvisRuntimeMetadata | None = None,
        allow_artifact_pinned_recovery: bool = False,
    ) -> None:
        """Persist one normalized runtime observation to job, task, and events."""
        task = self.queue.get_task(task_id)
        failed_channel = _runtime_sidecar_channel_failed(task)
        recovery_intent = _durable_jarvis_execution_recovery(job, task)
        recovery_authorized = (
            allow_artifact_pinned_recovery
            and recovery_intent is not None
            and recovery_intent["state"] == "pending"
            and metadata.source is RuntimeMetadataSource.JARVIS_MCP
            and _runtime_metadata_is_native(metadata)
            and metadata.pipeline_id == recovery_intent["pipeline_id"]
            and metadata.execution_id == recovery_intent["execution_id"]
        )
        incoming_reconciliation = _runtime_metadata_exact_marker_reconciliation(metadata)
        if failed_channel and (
            not recovery_authorized
            and (
                metadata.source is not RuntimeMetadataSource.RELAY_RECONCILIATION
                or incoming_reconciliation is None
            )
        ):
            state[0] = None
            digests.clear()
            scheduler_job_ids.clear()
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                "Refused runtime metadata after the sidecar channel failed closed",
                payload={
                    "source": metadata.source.value,
                    "execution_id": metadata.execution_id,
                    "scheduler_provider": metadata.scheduler_provider,
                    "scheduler_job_id": metadata.scheduler_job_id,
                    "ownership_verified": False,
                    "reason": "exact scheduler marker reconciliation is required",
                },
            )
            return
        if superseded_transport_runtime is not None and state[0] != superseded_transport_runtime:
            raise ConfigurationError("MCP transport runtime changed before it could be superseded")
        if (
            superseded_transport_runtime is None
            and _task_direct_execution_pinned(task)
            and (metadata.scheduler_provider is not None or metadata.scheduler_job_id is not None)
        ):
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                "Refused scheduler identity after direct JARVIS execution was pinned",
                payload={
                    "source": metadata.source.value,
                    "execution_id": metadata.execution_id,
                    "scheduler_provider": metadata.scheduler_provider,
                    "scheduler_job_id": metadata.scheduler_job_id,
                    "ownership_verified": False,
                    "reason": "direct execution cannot acquire scheduler ownership",
                },
            )
            return
        digest_payload = metadata.model_dump(mode="json", exclude={"observed_at"})
        digest = hashlib.sha256(
            json.dumps(digest_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        if digest in digests:
            return
        digests.add(digest)
        previous = None if superseded_transport_runtime is not None else state[0]
        if (
            _runtime_metadata_is_native(metadata)
            and previous is not None
            and previous.source
            in {
                RuntimeMetadataSource.LEGACY_STDOUT,
                RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY,
            }
        ):
            previous = None
        try:
            merged = merge_runtime_metadata(previous, metadata)
        except RuntimeMetadataIdentityConflictError as exc:
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_refused",
                f"Refused conflicting authoritative runtime metadata: {exc}",
                payload={
                    "source": metadata.source.value,
                    "execution_id": metadata.execution_id,
                    "scheduler_provider": metadata.scheduler_provider,
                    "scheduler_job_id": metadata.scheduler_job_id,
                    "ownership_verified": False,
                    "reason": str(exc),
                },
            )
            return
        state[0] = merged
        scheduler_identity_sources = {
            merged.field_sources.get(field_name, merged.source)
            for field_name in (
                "execution_id",
                "scheduler_provider",
                "scheduler_job_id",
            )
        }
        scheduler_id_source = merged.field_sources.get("scheduler_job_id", merged.source)
        exact_marker_reconciliation = _runtime_metadata_exact_marker_reconciliation(merged)
        scheduler_ownership_verified = (
            merged.execution_id is not None
            and merged.scheduler_provider is not None
            and merged.scheduler_job_id is not None
            and (len(scheduler_identity_sources) == 1 or exact_marker_reconciliation is not None)
            and scheduler_identity_sources
            <= {
                RuntimeMetadataSource.JARVIS_MCP,
                RuntimeMetadataSource.JARVIS_SIDECAR,
                RuntimeMetadataSource.RELAY_RECONCILIATION,
            }
        )
        scheduler_job_ids[:] = (
            [merged.scheduler_job_id]
            if scheduler_ownership_verified and merged.scheduler_job_id is not None
            else []
        )
        scheduler_ownership: list[dict[str, object]] = []
        if scheduler_ownership_verified and merged.scheduler_job_id is not None:
            scheduler_ownership.append(
                {
                    "scheduler_job_id": merged.scheduler_job_id,
                    "scheduler_provider": merged.scheduler_provider,
                    "relay_job_id": job.job_id,
                    "task_id": task_id,
                    "execution_id": merged.execution_id,
                    "runtime_metadata_source": scheduler_id_source.value,
                    "ownership_verified": True,
                    "proof": (
                        "exact_scheduler_marker_reconciliation"
                        if exact_marker_reconciliation
                        else "authenticated_runtime_sidecar"
                        if scheduler_id_source == RuntimeMetadataSource.JARVIS_SIDECAR
                        else "owned_jarvis_run_mcp_result"
                    ),
                    "reconciliation_marker": (
                        exact_marker_reconciliation.get("marker")
                        if exact_marker_reconciliation
                        else None
                    ),
                }
            )
        runtime_payload = merged.model_dump(mode="json")
        durable_metadata: dict[str, object] = {
            "runtime_metadata": runtime_payload,
            "runtime_metadata_source": merged.source.value,
            "scheduler_job_ids": list(scheduler_job_ids),
            "scheduler_job_ownership": scheduler_ownership,
        }
        if superseded_transport_runtime is not None:
            durable_metadata["mcp_transport_runtime_metadata"] = (
                superseded_transport_runtime.model_dump(mode="json")
            )
        native_execution = merged.details.get("native_execution")
        if isinstance(native_execution, dict):
            typed_native = cast(dict[str, object], native_execution)
            for source_key, durable_key in (
                ("execution_handle", "jarvis_execution_handle"),
                ("execution_record", "jarvis_execution_record"),
                ("progress", "jarvis_execution_progress"),
            ):
                document = typed_native.get(source_key)
                if isinstance(document, dict):
                    durable_metadata[durable_key] = document
        durable_metadata["scheduler"] = merged.scheduler_provider
        self.queue.update_job_metadata(job.job_id, durable_metadata)
        self.queue.update_task_metadata(task_id, durable_metadata)
        if superseded_transport_runtime is not None:
            self.queue.append_event(
                job.job_id,
                "runtime.transport_metadata_superseded",
                "Trusted JARVIS MCP runtime superseded its direct transport wrapper",
                payload={
                    "transport_execution_id": superseded_transport_runtime.execution_id,
                    "owned_execution_id": merged.execution_id,
                    "owned_scheduler_job_id": merged.scheduler_job_id,
                    "ownership_verified": scheduler_ownership_verified,
                },
            )
        if failed_channel and exact_marker_reconciliation is not None:
            self._resolve_runtime_sidecar_failure_by_reconciliation(
                job,
                task_id=task_id,
                reconciliation=exact_marker_reconciliation,
            )
        trusted_structured = metadata.source in {
            RuntimeMetadataSource.JARVIS_MCP,
            RuntimeMetadataSource.JARVIS_SIDECAR,
            RuntimeMetadataSource.RELAY_RECONCILIATION,
        }
        legacy_fallback = metadata.source == RuntimeMetadataSource.LEGACY_STDOUT
        untrusted_compatibility = metadata.source == RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY
        self.queue.append_event(
            job.job_id,
            (
                "runtime.metadata_fallback"
                if legacy_fallback
                else "runtime.metadata_untrusted"
                if untrusted_compatibility
                else "runtime.metadata_ingested"
            ),
            (
                "Using legacy scheduler metadata parsed from process output"
                if legacy_fallback
                else "Normalized runtime metadata without producer authority"
                if untrusted_compatibility
                else "Structured JARVIS runtime metadata ingested"
            ),
            payload={
                "source": metadata.source.value,
                "scheduler_provider": metadata.scheduler_provider,
                "scheduler_job_id": metadata.scheduler_job_id,
                "scheduler_job_id_source": merged.field_sources.get("scheduler_job_id"),
                "structured": trusted_structured,
                "ownership_verified": scheduler_ownership_verified,
            },
        )
        previous_phase = None if previous is None else previous.scheduler_phase
        scheduler_phase = merged.scheduler_phase
        known_scheduler_phases = {phase.value for phase in SchedulerPhase}
        if (
            trusted_structured
            and scheduler_phase in known_scheduler_phases
            and scheduler_phase != previous_phase
        ):
            self.queue.append_event(
                job.job_id,
                f"scheduler.{scheduler_phase}",
                f"Structured runtime metadata observed scheduler phase: {scheduler_phase}",
                payload={
                    "scheduler": merged.scheduler_provider,
                    "scheduler_job_id": merged.scheduler_job_id,
                    "phase": scheduler_phase,
                    "metadata_source": merged.source.value,
                    "structured": True,
                },
            )
        previous_job_id = None if previous is None else previous.scheduler_job_id
        source_changed = previous is not None and previous.source != merged.source
        if merged.scheduler_job_id is None or (
            previous_job_id == merged.scheduler_job_id and not source_changed
        ):
            return
        if not scheduler_ownership_verified:
            self.queue.append_event(
                job.job_id,
                "scheduler.job_observed_untrusted",
                f"Ignored untrusted scheduler identity: {merged.scheduler_job_id}",
                payload={
                    "scheduler": merged.scheduler_provider,
                    "scheduler_job_id": merged.scheduler_job_id,
                    "metadata_source": scheduler_id_source.value,
                    "ownership_verified": False,
                    "cancellation_eligible": False,
                },
            )
            return
        self.queue.append_event(
            job.job_id,
            "scheduler.job_detected",
            f"Scheduler job detected: {merged.scheduler_job_id}",
            payload={
                "scheduler": merged.scheduler_provider,
                "scheduler_job_id": merged.scheduler_job_id,
                "metadata_source": scheduler_id_source.value,
                "runtime_metadata_source": merged.source.value,
                "structured": scheduler_id_source != RuntimeMetadataSource.LEGACY_STDOUT,
                "ownership_verified": True,
            },
        )
        self._refresh_scheduler_status(
            self.queue.get_job(job.job_id),
            [merged.scheduler_job_id],
            task_id=task_id,
            force=True,
        )
