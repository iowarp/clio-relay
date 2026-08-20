"""Execution-ownership resolution and the submit-side crash reconciliation path.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_resolve_execution_ownership`` proves direct execution or scheduler ownership before
cleanup can release sidecars; ``_reconcile_scheduler_submission_intent`` resolves a
submit-side crash (the marker was authenticated but the scheduler job id never landed)
through one exact provider-native marker match; ``_scheduler_reconciliation_provider``
resolves the reconciliation provider for a durable intent's named scheduler.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery,
)
from clio_relay.endpoint_scheduler_metadata import (
    _durable_scheduler_submission_intent,
    _owned_scheduler_job_ids_from_metadata,
    _runtime_sidecar_channel_failed,
    _task_direct_execution_pinned,
    _task_scheduler_submission_refused,
)
from clio_relay.endpoint_sidecar_types import (
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    RelayJob,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
)
from clio_relay.scheduler_providers import (
    SchedulerReconciliationProvider,
    reconciliation_provider_for_scheduler,
)


class SchedulerSubmissionMixin:
    """Mixin: SchedulerSubmission methods split from EndpointWorker (clio-relay#231)."""

    def _resolve_execution_ownership(
        self,
        job: RelayJob,
        *,
        task_id: str,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
        runtime_sidecar_failures: list[str],
    ) -> bool:
        """Prove direct execution or scheduler ownership before cleanup can succeed."""
        task = self.queue.get_task(task_id)
        recovery_intent = _durable_jarvis_execution_recovery(job, task)
        if recovery_intent is not None and recovery_intent["state"] == "resolved":
            recovered_provider = recovery_intent.get("scheduler_provider")
            recovered_job_id = recovery_intent.get("scheduler_job_id")
            if recovered_provider is None and recovered_job_id is None:
                return False
            if not isinstance(recovered_provider, str) or not isinstance(
                recovered_job_id,
                str,
            ):
                raise SchedulerSubmissionUnresolvedError(
                    "recovered JARVIS scheduler identity is incomplete"
                )
            owned_ids = _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task_id,
            )
            if owned_ids != [recovered_job_id]:
                raise SchedulerSubmissionUnresolvedError(
                    "recovered JARVIS scheduler ownership did not match task metadata"
                )
            scheduler_job_ids[:] = owned_ids
            return True
        if recovery_intent is not None:
            raise SchedulerSubmissionUnresolvedError(
                "JARVIS execution recovery has not resolved its native scheduler identity"
            )
        intent = _durable_scheduler_submission_intent(task)
        if intent["scheduler_expected"] is False:
            return False
        if _task_scheduler_submission_refused(task):
            return False
        if _runtime_sidecar_channel_failed(task):
            state[0] = None
            digests.clear()
            scheduler_job_ids.clear()
            self._reconcile_recorded_scheduler_submission(
                job,
                task,
                allow_raw_direct_proof=False,
            )
            reconciled_task = self.queue.get_task(task_id)
            reconciled_ids = _owned_scheduler_job_ids_from_metadata(
                reconciled_task.metadata,
                relay_job_id=job.job_id,
                task_id=task_id,
            )
            if reconciled_ids:
                scheduler_job_ids[:] = reconciled_ids
                return True
            failure_detail = (
                "; ".join(runtime_sidecar_failures)
                if runtime_sidecar_failures
                else "the runtime metadata channel failed closed"
            )
            raise SchedulerSubmissionUnresolvedError(
                "JARVIS execution ownership requires exact scheduler reconciliation: "
                + failure_detail
            )
        if _task_direct_execution_pinned(task):
            return False
        owned_ids = _owned_scheduler_job_ids_from_metadata(
            task.metadata,
            relay_job_id=job.job_id,
            task_id=task_id,
        )
        if owned_ids:
            scheduler_job_ids[:] = owned_ids
            return True
        reconciled = self._reconcile_scheduler_submission_intent(
            job,
            task_id=task_id,
            state=state,
            digests=digests,
            scheduler_job_ids=scheduler_job_ids,
        )
        task = self.queue.get_task(task_id)
        if not reconciled and not _task_direct_execution_pinned(task):
            reconciled = self._reconcile_recorded_scheduler_submission(
                job,
                task,
                allow_raw_direct_proof=False,
            )
        task = self.queue.get_task(task_id)
        if _task_direct_execution_pinned(task):
            return False
        owned_ids = _owned_scheduler_job_ids_from_metadata(
            task.metadata,
            relay_job_id=job.job_id,
            task_id=task_id,
        )
        if owned_ids:
            scheduler_job_ids[:] = owned_ids
            return True
        failure_detail = (
            "; ".join(runtime_sidecar_failures)
            if runtime_sidecar_failures
            else "no authenticated direct proof or scheduler identity was available"
        )
        raise SchedulerSubmissionUnresolvedError(
            f"JARVIS execution ownership remains unresolved: {failure_detail}"
        )

    def _reconcile_scheduler_submission_intent(
        self,
        job: RelayJob,
        *,
        task_id: str,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
    ) -> bool:
        """Resolve a submit-side crash through one exact provider-native marker."""
        current = state[0]
        if current is None or current.scheduler_job_id is not None:
            return False
        raw_details = current.details.get("details")
        details = cast(dict[str, Any], raw_details) if isinstance(raw_details, dict) else {}
        raw_intent = details.get("scheduler_submission_intent")
        if not isinstance(raw_intent, dict):
            return False
        intent = cast(dict[str, Any], raw_intent)
        durable_intent = _durable_scheduler_submission_intent(self.queue.get_task(task_id))
        if durable_intent["scheduler_expected"] is False or any(
            intent.get(field) != durable_intent[field]
            for field in (
                "schema_version",
                "execution_id",
                "marker",
                "created_at",
                "scheduler_user",
                "scheduler_expected",
                "direct_proof_sha256",
            )
        ):
            raise SchedulerSubmissionUnresolvedError(
                "authenticated scheduler intent did not match durable launch intent"
            )
        provider_name = intent.get("provider")
        marker = intent.get("marker")
        created_at = intent.get("created_at")
        scheduler_user = intent.get("scheduler_user")
        if (
            intent.get("schema_version") != "clio-relay.scheduler-submission-intent.v1"
            or not isinstance(provider_name, str)
            or provider_name != current.scheduler_provider
            or not isinstance(marker, str)
            or not marker
            or not isinstance(created_at, str)
            or not created_at
            or not isinstance(scheduler_user, str)
            or not scheduler_user
            or current.execution_id is None
        ):
            raise SchedulerSubmissionUnresolvedError(
                "authenticated scheduler submission intent did not match"
            )
        try:
            submitted_after = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise SchedulerSubmissionUnresolvedError(
                "authenticated scheduler submission time was invalid"
            ) from exc
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
                "Scheduler provider could not resolve interrupted submission intent",
                payload={"provider": provider_name, "marker": marker, "error": str(exc)},
            )
            raise SchedulerSubmissionUnresolvedError(
                "scheduler provider could not resolve submission intent"
            ) from exc
        if len(matches) > 1:
            self.queue.append_event(
                job.job_id,
                "scheduler.reconciliation_refused",
                "Scheduler submission marker matched more than one job",
                payload={"provider": provider_name, "marker": marker, "match_count": len(matches)},
            )
            raise SchedulerSubmissionUnresolvedError("scheduler submission marker was not unique")
        if not matches:
            self.queue.append_event(
                job.job_id,
                "scheduler.reconciliation_unresolved",
                "Interrupted scheduler submission marker had no current or historical match",
                payload={
                    "provider": provider_name,
                    "marker": marker,
                    "scheduler_user": scheduler_user,
                    "submitted_after": created_at,
                },
            )
            raise SchedulerSubmissionUnresolvedError(
                "scheduler submission intent remains unresolved"
            )
        scheduler_job_id = matches[0]
        reconciliation: dict[str, object] = {
            "schema_version": "clio-relay.scheduler-marker-reconciliation.v1",
            "provider": provider_name,
            "marker": marker,
            "scheduler_job_id": scheduler_job_id,
            "match_count": 1,
            "verified_at": utc_now().isoformat(),
        }
        submission = {
            "schema_version": "jarvis.scheduler.submission.v1",
            "provider": provider_name,
            "scheduler_job_id": scheduler_job_id,
            "identity_source": "scheduler_exact_marker_reconciliation",
            "submitted": True,
            "reconciliation_marker": marker,
        }
        reconciled = current.model_copy(
            update={
                "source": RuntimeMetadataSource.RELAY_RECONCILIATION,
                "scheduler_job_id": scheduler_job_id,
                "scheduler_phase": "reconciled",
                "field_sources": {
                    **current.field_sources,
                    "execution_id": RuntimeMetadataSource.RELAY_RECONCILIATION,
                    "scheduler_provider": RuntimeMetadataSource.RELAY_RECONCILIATION,
                    "scheduler_job_id": RuntimeMetadataSource.RELAY_RECONCILIATION,
                    "scheduler_phase": RuntimeMetadataSource.RELAY_RECONCILIATION,
                },
                "details": {
                    **current.details,
                    "details": {
                        **details,
                        "scheduler_submission": submission,
                    },
                    "scheduler_marker_reconciliation": reconciliation,
                    "producer_contract": {
                        "requested_source": RuntimeMetadataSource.RELAY_RECONCILIATION.value,
                        "producer_schema_version": "jarvis.runtime.v1",
                        "trusted": True,
                        "reason": "authenticated intent matched exactly one provider job name",
                    },
                },
            }
        )
        self._persist_runtime_metadata(
            job,
            task_id=task_id,
            metadata=reconciled,
            state=state,
            digests=digests,
            scheduler_job_ids=scheduler_job_ids,
        )
        self.queue.append_event(
            job.job_id,
            "scheduler.reconciled",
            f"Interrupted scheduler submission reconciled: {scheduler_job_id}",
            payload=reconciliation,
        )
        return True

    def _scheduler_reconciliation_provider(
        self,
        provider_name: str,
    ) -> SchedulerReconciliationProvider:
        normalized = provider_name.strip().lower().replace("_", "-")
        if self.scheduler_provider is not None:
            if self.scheduler_provider.name != normalized:
                raise ConfigurationError(
                    "scheduler reconciliation provider does not match worker configuration"
                )
            if not isinstance(self.scheduler_provider, SchedulerReconciliationProvider):
                raise ConfigurationError(
                    f"scheduler provider does not support exact submission reconciliation: "
                    f"{normalized}"
                )
            return self.scheduler_provider
        return reconciliation_provider_for_scheduler(normalized)
