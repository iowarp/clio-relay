"""Recorded scheduler-submission reconciliation and prelaunch-resolution proof.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_reconcile_recorded_scheduler_submission`` re-derives scheduler ownership from a
durably recorded submission when the runtime channel cannot answer directly;
``_recorded_prelaunch_resolution_proven`` validates the one-use preflight proof that
resolved ``scheduler_expected`` to a concrete true/false before launch.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from clio_relay.endpoint_progress_trust import (
    _read_bounded_sidecar_record,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _open_owned_sidecar,
    _runtime_sidecar_anchor_from_metadata,
)
from clio_relay.endpoint_scheduler_metadata import (
    _durable_scheduler_submission_intent,
    _owned_scheduler_job_ids_from_metadata,
    _runtime_sidecar_channel_failed,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_MAX_RECORD_BYTES,
    RUNTIME_SIDECAR_MAX_RECORDS,
    RUNTIME_SIDECAR_MAX_TOTAL_BYTES,
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.endpoint_worker_environment import (
    _configured_scheduler_provider_name,
    _jarvis_pipeline_name,
    _scheduler_name_from_job,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
)


class SchedulerSubmissionReconcileMixin:
    """Mixin: SchedulerSubmissionReconcile methods split from EndpointWorker (clio-relay#231)."""

    def _reconcile_recorded_scheduler_submission(
        self,
        job: RelayJob,
        task: RelayTask,
        *,
        allow_raw_direct_proof: bool = True,
    ) -> bool:
        """Recover scheduler ownership from a relay-durable pre-release intent."""
        try:
            durable_intent = _durable_scheduler_submission_intent(task)
        except RelayError as exc:
            if _scheduler_name_from_job(job) is None and _jarvis_pipeline_name(job) is None:
                return False
            raise SchedulerSubmissionUnresolvedError(
                "scheduled cleanup has no durable submission intent"
            ) from exc
        raw_sidecars = cast(dict[str, object], task.metadata.get("execution_sidecars", {}))
        if raw_sidecars.get("scheduler_expected_resolved") is False:
            return False
        if raw_sidecars.get("scheduler_submission_refused") is True:
            return False
        if durable_intent["scheduler_expected"] is False:
            return False
        if (
            allow_raw_direct_proof
            and durable_intent["scheduler_expected"] == "unknown"
            and self._recorded_prelaunch_resolution_proven(job, task, durable_intent)
        ):
            return False
        if _owned_scheduler_job_ids_from_metadata(
            task.metadata,
            relay_job_id=job.job_id,
            task_id=task.task_id,
        ):
            return False
        provider_name = _scheduler_name_from_job(job)
        if provider_name is None and self.scheduler_provider is not None:
            provider_name = self.scheduler_provider.name
        if provider_name is None or provider_name == "external":
            raise SchedulerSubmissionUnresolvedError(
                "durable scheduler intent has no exact reconciliation provider"
            )
        marker = cast(str, durable_intent["marker"])
        scheduler_user = cast(str, durable_intent["scheduler_user"])
        submitted_after = datetime.fromisoformat(cast(str, durable_intent["created_at"]))
        try:
            provider = self._scheduler_reconciliation_provider(provider_name)
            matches = provider.find_job_ids_by_marker(
                marker,
                submitted_after=submitted_after,
                scheduler_user=scheduler_user,
            )
        except (ConfigurationError, RelayError) as exc:
            self.queue.append_event(
                job.job_id,
                "scheduler.reconciliation_unresolved",
                "Restart cleanup could not query durable scheduler intent",
                payload={"provider": provider_name, "marker": marker, "error": str(exc)},
            )
            raise SchedulerSubmissionUnresolvedError(
                "restart cleanup could not resolve scheduler intent"
            ) from exc
        if len(matches) != 1:
            self.queue.append_event(
                job.job_id,
                "scheduler.reconciliation_unresolved",
                "Restart cleanup requires exactly one scheduler marker match",
                payload={
                    "provider": provider_name,
                    "marker": marker,
                    "match_count": len(matches),
                },
            )
            raise SchedulerSubmissionUnresolvedError(
                "restart scheduler submission intent remains unresolved"
            )
        scheduler_job_id = matches[0]
        reconciliation: dict[str, object] = {
            "schema_version": "clio-relay.scheduler-marker-reconciliation.v1",
            "provider": provider_name,
            "marker": marker,
            "scheduler_job_id": scheduler_job_id,
            "match_count": 1,
            "verified_at": utc_now().isoformat(),
            "recovered_after_worker_restart": True,
        }
        submission = {
            "schema_version": "jarvis.scheduler.submission.v1",
            "provider": provider_name,
            "scheduler_job_id": scheduler_job_id,
            "identity_source": "scheduler_exact_marker_reconciliation",
            "submitted": True,
            "reconciliation_marker": marker,
        }
        metadata = JarvisRuntimeMetadata(
            source=RuntimeMetadataSource.RELAY_RECONCILIATION,
            execution_id=cast(str, durable_intent["execution_id"]),
            pipeline_id=_jarvis_pipeline_name(job),
            scheduler_provider=provider_name,
            scheduler_type=provider_name,
            scheduler_job_id=scheduler_job_id,
            scheduler_phase="reconciled",
            field_sources={
                field: RuntimeMetadataSource.RELAY_RECONCILIATION
                for field in (
                    "execution_id",
                    "scheduler_provider",
                    "scheduler_type",
                    "scheduler_job_id",
                    "scheduler_phase",
                )
            },
            details={
                "details": {
                    "scheduler_submission_intent": {
                        **durable_intent,
                        "provider": provider_name,
                    },
                    "scheduler_submission": submission,
                },
                "scheduler_marker_reconciliation": reconciliation,
                "producer_contract": {
                    "requested_source": RuntimeMetadataSource.RELAY_RECONCILIATION.value,
                    "producer_schema_version": "jarvis.runtime.v1",
                    "trusted": True,
                    "reason": "relay-durable intent matched exactly one provider job",
                },
            },
        )
        self._persist_runtime_metadata(
            job,
            task_id=task.task_id,
            metadata=metadata,
            state=[None],
            digests=set(),
            scheduler_job_ids=[],
        )
        self.queue.append_event(
            job.job_id,
            "scheduler.reconciled",
            f"Restart cleanup reconciled scheduler job: {scheduler_job_id}",
            payload=reconciliation,
        )
        return True

    def _recorded_prelaunch_resolution_proven(
        self,
        job: RelayJob,
        task: RelayTask,
        intent: dict[str, Any],
    ) -> bool:
        """Verify a one-use direct-mode or pre-submit refusal proof after restart."""
        if _runtime_sidecar_channel_failed(task):
            return False
        raw_sidecars = task.metadata.get("execution_sidecars")
        if not isinstance(raw_sidecars, dict):
            return False
        sidecars = cast(dict[str, object], raw_sidecars)
        runtime_name = sidecars.get("runtime")
        if (
            not isinstance(runtime_name, str)
            or Path(runtime_name).name != runtime_name
            or not runtime_name.startswith(".runtime-")
            or not runtime_name.endswith(".jsonl")
        ):
            return False
        anchor = _runtime_sidecar_anchor_from_metadata(
            sidecars.get("runtime_anchor"),
            task_id=task.task_id,
        )
        path = self.settings.spool_dir / job.job_id / runtime_name
        handle = _open_owned_sidecar(
            path,
            label="runtime metadata sidecar",
            expected_anchor=anchor,
        )
        if handle is None:
            return False
        with handle:
            if os.fstat(handle.fileno()).st_size > RUNTIME_SIDECAR_MAX_TOTAL_BYTES:
                raise SchedulerSubmissionUnresolvedError(
                    "prelaunch resolution proof sidecar exceeded its byte limit"
                )
            for _ in range(RUNTIME_SIDECAR_MAX_RECORDS):
                line, status = _read_bounded_sidecar_record(
                    handle,
                    max_bytes=RUNTIME_SIDECAR_MAX_RECORD_BYTES,
                    allow_final_record=True,
                )
                if status in {"eof", "incomplete"}:
                    break
                if status == "oversized" or line is None:
                    continue
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(record, dict):
                    continue
                runtime = cast(dict[str, object], record).get("runtime_metadata")
                if not isinstance(runtime, dict):
                    continue
                typed_runtime = cast(dict[str, Any], runtime)
                raw_details = typed_runtime.get("details")
                details = cast(dict[str, Any], raw_details) if isinstance(raw_details, dict) else {}
                refusal_proof = details.get("scheduler_launch_refusal_proof")
                requested_provider = details.get("scheduler_provider")
                configured_provider = _configured_scheduler_provider_name(self.scheduler_provider)
                raw_terminal = typed_runtime.get("terminal")
                terminal = (
                    cast(dict[str, Any], raw_terminal) if isinstance(raw_terminal, dict) else {}
                )
                if (
                    typed_runtime.get("schema_version") == "jarvis.runtime.v1"
                    and typed_runtime.get("execution_id") == intent["execution_id"]
                    and typed_runtime.get("pipeline_id") == _jarvis_pipeline_name(job)
                    and typed_runtime.get("scheduler_job_id") is None
                    and typed_runtime.get("scheduler_provider") == requested_provider
                    and typed_runtime.get("scheduler_phase") == "launch_refused"
                    and terminal.get("state") == "launch_refused"
                    and terminal.get("terminal") is True
                    and terminal.get("returncode") == 2
                    and details.get("execution_owner") == "jarvis_cd.pipeline.preflight"
                    and details.get("execution_mode") == "scheduler"
                    and details.get("scheduler_expected") == intent["scheduler_expected"]
                    and details.get("scheduler_submission_attempted") is False
                    and details.get("scheduler_launch_refused") is True
                    and isinstance(requested_provider, str)
                    and details.get("configured_scheduler_provider") == configured_provider
                    and (requested_provider != configured_provider or requested_provider != "slurm")
                    and isinstance(refusal_proof, str)
                    and secrets.compare_digest(
                        hashlib.sha256(refusal_proof.encode("utf-8")).hexdigest(),
                        cast(str, intent["direct_proof_sha256"]),
                    )
                ):
                    self.queue.update_task_metadata(
                        task.task_id,
                        {
                            "execution_sidecars": {
                                **sidecars,
                                "scheduler_submission_refused": True,
                            }
                        },
                    )
                    self.queue.append_event(
                        job.job_id,
                        "scheduler.launch_refusal_recovered",
                        "Restart cleanup verified scheduler launch was refused before submission",
                        payload={
                            "task_id": task.task_id,
                            "execution_id": intent["execution_id"],
                            "requested_provider": requested_provider,
                            "configured_provider": configured_provider,
                            "scheduler_submission_attempted": False,
                        },
                    )
                    return True
                proof = details.get("direct_execution_proof")
                if (
                    typed_runtime.get("schema_version") == "jarvis.runtime.v1"
                    and typed_runtime.get("execution_id") == intent["execution_id"]
                    and details.get("execution_mode") == "direct"
                    and details.get("scheduler_expected") is False
                    and isinstance(proof, str)
                    and secrets.compare_digest(
                        hashlib.sha256(proof.encode("utf-8")).hexdigest(),
                        cast(str, intent["direct_proof_sha256"]),
                    )
                ):
                    self.queue.update_task_metadata(
                        task.task_id,
                        {
                            "execution_sidecars": {
                                **sidecars,
                                "scheduler_expected_resolved": False,
                            }
                        },
                    )
                    self.queue.append_event(
                        job.job_id,
                        "scheduler.direct_execution_recovered",
                        "Restart cleanup verified direct named execution",
                        payload={"task_id": task.task_id, "execution_id": intent["execution_id"]},
                    )
                    return True
        return False
