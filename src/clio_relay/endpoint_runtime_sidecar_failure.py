"""Runtime-sidecar fail-closed latch, reconciliation resolution, and the authenticated
runtime-metadata sidecar reader.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_latch_runtime_sidecar_failure`` durably fails a JARVIS runtime metadata channel
closed (invalidating ownership until exact scheduler-marker reconciliation resolves it,
``_resolve_runtime_sidecar_failure_by_reconciliation``);
``_ingest_runtime_metadata_sidecar`` is the bounded, anchor-authenticated JSONL reader
that calls both plus the scheduler-launch-refusal/direct- execution-proof consumers
(owned by the sibling ``endpoint_runtime_metadata_ingest.py`` module) inline per record.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

from clio_relay.endpoint_progress_trust import (
    _read_bounded_sidecar_record,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _open_owned_sidecar,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_sidecar_channel_failed,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_CHANNEL_SCHEMA,
    RUNTIME_SIDECAR_MAX_RECORD_BYTES,
    RUNTIME_SIDECAR_MAX_RECORDS,
    RUNTIME_SIDECAR_MAX_TOTAL_BYTES,
    _RuntimeSidecarAnchor,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    RelayJob,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    runtime_metadata_from_sidecar_record,
)


class RuntimeSidecarFailureMixin:
    """Mixin: RuntimeSidecarFailure methods split from EndpointWorker (clio-relay#231)."""

    def _latch_runtime_sidecar_failure(
        self,
        job: RelayJob,
        *,
        task_id: str,
        message: str,
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
    ) -> None:
        """Durably fail-close one runtime channel and invalidate its authority."""
        task = self.queue.get_task(task_id)
        now = utc_now().isoformat()
        raw_channel = task.metadata.get("runtime_sidecar_channel")
        channel = (
            dict(cast(dict[str, object], raw_channel)) if isinstance(raw_channel, dict) else {}
        )
        raw_failures = channel.get("failures")
        recorded_failures = (
            [item for item in cast(list[object], raw_failures) if isinstance(item, str)]
            if isinstance(raw_failures, list)
            else []
        )
        if message not in recorded_failures:
            recorded_failures.append(message)
        recorded_failures = recorded_failures[:RUNTIME_SIDECAR_MAX_RECORDS]
        sidecars_raw = task.metadata.get("execution_sidecars")
        sidecars = (
            dict(cast(dict[str, object], sidecars_raw)) if isinstance(sidecars_raw, dict) else {}
        )
        sidecars.pop("scheduler_expected_resolved", None)
        failure_channel: dict[str, object] = {
            **channel,
            "schema_version": RUNTIME_SIDECAR_CHANNEL_SCHEMA,
            "state": "failed_closed",
            "latched_at": channel.get("latched_at", now),
            "last_failure_at": now,
            "failures": recorded_failures,
            "failure_count": len(recorded_failures),
            "resolution_requirement": "exact_scheduler_marker_reconciliation",
            "evidence_retention": "whole_job_spool",
        }
        invalidated_metadata: dict[str, object] = {
            "runtime_sidecar_channel": failure_channel,
            "runtime_metadata": None,
            "runtime_metadata_source": "runtime_sidecar_failed_closed",
            "scheduler_job_ids": [],
            "scheduler_job_ownership": [],
        }
        if sidecars:
            invalidated_metadata["execution_sidecars"] = sidecars
        # Task first: after this durable write no later sidecar or MCP record can
        # regain authority, even if the process dies before job metadata mirrors it.
        self.queue.update_task_metadata(task_id, invalidated_metadata)
        self.queue.update_job_metadata(
            job.job_id,
            {
                key: value
                for key, value in invalidated_metadata.items()
                if key != "execution_sidecars"
            },
        )
        state[0] = None
        digests.clear()
        scheduler_job_ids.clear()
        self.queue.append_event(
            job.job_id,
            "runtime.metadata_channel_failed_closed",
            "JARVIS runtime metadata channel was durably failed closed",
            payload={
                "task_id": task_id,
                "failure": message,
                "resolution_requirement": "exact_scheduler_marker_reconciliation",
                "evidence_retained": True,
            },
        )

    def _resolve_runtime_sidecar_failure_by_reconciliation(
        self,
        job: RelayJob,
        *,
        task_id: str,
        reconciliation: dict[str, Any],
    ) -> None:
        """Record that exact scheduler reconciliation superseded a failed channel."""
        task = self.queue.get_task(task_id)
        raw_channel = task.metadata.get("runtime_sidecar_channel")
        if not isinstance(raw_channel, dict):
            return
        channel = dict(cast(dict[str, object], raw_channel))
        if channel.get("state") != "failed_closed":
            return
        resolved = {
            **channel,
            "state": "resolved_by_exact_scheduler_reconciliation",
            "resolved_at": utc_now().isoformat(),
            "resolution": {
                "schema_version": "clio-relay.scheduler-marker-reconciliation.v1",
                "provider": reconciliation.get("provider"),
                "marker": reconciliation.get("marker"),
                "scheduler_job_id": reconciliation.get("scheduler_job_id"),
                "match_count": 1,
            },
        }
        self.queue.update_task_metadata(task_id, {"runtime_sidecar_channel": resolved})
        self.queue.update_job_metadata(job.job_id, {"runtime_sidecar_channel": resolved})
        self.queue.append_event(
            job.job_id,
            "runtime.metadata_channel_reconciled",
            "Failed runtime metadata channel was resolved by exact scheduler reconciliation",
            payload={
                "task_id": task_id,
                "scheduler_job_id": reconciliation.get("scheduler_job_id"),
                "marker": reconciliation.get("marker"),
            },
        )

    def _ingest_runtime_metadata_sidecar(
        self,
        job: RelayJob,
        *,
        task_id: str,
        path: Path,
        offset: list[int],
        record_count: list[int],
        sequence: list[int],
        expected_key: str,
        expected_anchor: _RuntimeSidecarAnchor,
        failures: list[str],
        state: list[JarvisRuntimeMetadata | None],
        digests: set[str],
        scheduler_job_ids: list[str],
        allow_final_record: bool,
    ) -> None:
        """Ingest authenticated structured runtime observations from JARVIS."""

        def fail(message: str) -> None:
            if message not in failures:
                failures.append(message)
            self._latch_runtime_sidecar_failure(
                job,
                task_id=task_id,
                message=message,
                state=state,
                digests=digests,
                scheduler_job_ids=scheduler_job_ids,
            )
            self.queue.append_event(job.job_id, "runtime.metadata_parse_failed", message)

        durable_task = self.queue.get_task(task_id)
        if _runtime_sidecar_channel_failed(durable_task):
            raw_channel = cast(
                dict[str, object],
                durable_task.metadata["runtime_sidecar_channel"],
            )
            raw_failures = raw_channel.get("failures")
            if isinstance(raw_failures, list):
                for item in cast(list[object], raw_failures):
                    if isinstance(item, str) and item not in failures:
                        failures.append(item)
            state[0] = None
            digests.clear()
            scheduler_job_ids.clear()
            return
        if not internal_filesystem_path(path).exists():
            fail("precreated JARVIS runtime metadata sidecar disappeared")
            return
        try:
            handle = _open_owned_sidecar(
                path,
                label="runtime metadata sidecar",
                expected_anchor=expected_anchor,
            )
            if handle is None:
                fail("precreated JARVIS runtime metadata sidecar disappeared while opening")
                return
            with handle:
                size = os.fstat(handle.fileno()).st_size
                if size < offset[0]:
                    fail("JARVIS runtime metadata sidecar was truncated")
                    offset[0] = size
                    return
                if size > RUNTIME_SIDECAR_MAX_TOTAL_BYTES:
                    if offset[0] <= RUNTIME_SIDECAR_MAX_TOTAL_BYTES:
                        fail("JARVIS runtime metadata sidecar exceeded its total byte limit")
                    offset[0] = size
                    return
                handle.seek(offset[0])
                while True:
                    if record_count[0] >= RUNTIME_SIDECAR_MAX_RECORDS:
                        if record_count[0] == RUNTIME_SIDECAR_MAX_RECORDS:
                            fail("JARVIS runtime metadata sidecar exceeded its record limit")
                            record_count[0] += 1
                        offset[0] = os.fstat(handle.fileno()).st_size
                        return
                    line, status = _read_bounded_sidecar_record(
                        handle,
                        max_bytes=RUNTIME_SIDECAR_MAX_RECORD_BYTES,
                        allow_final_record=allow_final_record,
                    )
                    if status in {"eof", "incomplete"}:
                        break
                    if handle.tell() > RUNTIME_SIDECAR_MAX_TOTAL_BYTES:
                        fail("JARVIS runtime metadata sidecar exceeded its total byte limit")
                        offset[0] = os.fstat(handle.fileno()).st_size
                        return
                    record_count[0] += 1
                    if status == "oversized":
                        fail("JARVIS runtime metadata sidecar record exceeded its byte limit")
                        offset[0] = handle.tell()
                        return
                    assert line is not None
                    try:
                        payload = json.loads(line)
                        metadata = runtime_metadata_from_sidecar_record(
                            payload,
                            expected_key=expected_key,
                            expected_sequence=sequence[0] + 1,
                        )
                        metadata = self._consume_scheduler_launch_refusal(
                            job,
                            task_id=task_id,
                            metadata=metadata,
                        )
                        metadata = self._consume_direct_execution_proof(
                            job,
                            task_id=task_id,
                            metadata=metadata,
                        )
                    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                        fail(f"JARVIS runtime metadata sidecar was invalid: {exc}")
                        offset[0] = handle.tell()
                        return
                    else:
                        sequence[0] += 1
                        self._persist_runtime_metadata(
                            job,
                            task_id=task_id,
                            metadata=metadata,
                            state=state,
                            digests=digests,
                            scheduler_job_ids=scheduler_job_ids,
                        )
                    offset[0] = handle.tell()
        except (ConfigurationError, OSError) as exc:
            message = f"JARVIS runtime metadata sidecar could not be read: {exc}"
            if message not in failures:
                failures.append(message)
            self._latch_runtime_sidecar_failure(
                job,
                task_id=task_id,
                message=message,
                state=state,
                digests=digests,
                scheduler_job_ids=scheduler_job_ids,
            )
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_read_failed",
                message,
            )
