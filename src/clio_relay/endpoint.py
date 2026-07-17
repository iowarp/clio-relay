"""Long-running desktop and cluster endpoint behavior."""

from __future__ import annotations

import base64
import concurrent.futures
import ctypes
import errno
import getpass
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import socket
import stat as stat_module
import subprocess
import sys
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, cast

import yaml
from filelock import FileLock, Timeout

from clio_relay import process_containment
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.config import RelaySettings
from clio_relay.core_queue import DEFAULT_EXACT_RECORD_LIMIT, ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.filesystem_paths import (
    WINDOWS_LEGACY_PATH_HEADROOM,
    internal_filesystem_path,
    logical_filesystem_path,
    logical_filesystem_text,
)
from clio_relay.identifiers import filesystem_key
from clio_relay.installation import installation_info
from clio_relay.jarvis_execution import RUNTIME_SCHEDULER_PROVIDER_ENV
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_command,
    jarvis_mcp_server_artifact_binding_verified,
)
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpCallSpec,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    SchedulerStatus,
    utc_now,
)
from clio_relay.progress_adapters import (
    PackageProgressProvider,
    package_progress_adapter_from_pipeline,
)
from clio_relay.progress_provenance import (
    PROTECTED_PROGRESS_METADATA_KEYS,
    PackageProgressSourceAuthority,
    jarvis_execution_progress_metadata,
    package_progress_metadata,
    package_progress_provider_metadata,
    validate_jarvis_execution_progress_metadata,
    validate_package_progress_metadata,
    validate_package_progress_provider_metadata,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    RuntimeMetadataIdentityConflictError,
    RuntimeMetadataSource,
    legacy_scheduler_runtime_metadata,
    merge_runtime_metadata,
    runtime_metadata_from_mcp_result_document,
    runtime_metadata_from_sidecar_record,
)
from clio_relay.scheduler_providers import (
    SchedulerProvider,
    SchedulerReconciliationProvider,
    provider_for_scheduler,
    reconciliation_provider_for_scheduler,
)
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import (
    StorageManagedQueue,
    StorageRuntime,
    StorageRuntimeViolation,
    storage_managed_queue,
)
from clio_relay.worker_concurrency import (
    KindConcurrencyInput,
    kind_concurrency_metadata,
    normalize_kind_concurrency,
)
from clio_relay.worker_lifetime_lock import WorkerLifetimeLock


@dataclass
class _PackageProgressLogState:
    """Tail checkpoint that excludes pre-launch bytes and detects source resets."""

    path: Path
    offset: int
    identity: tuple[int, int] | None
    checkpoint_offset: int
    checkpoint_sha256: str | None


@dataclass(frozen=True, slots=True)
class _RuntimeSidecarAnchor:
    """Pinned filesystem identity for one precreated runtime sidecar."""

    device: int
    inode: int
    owner: int
    link_count: int
    mode: int
    descriptor: int | None = field(default=None, compare=False, repr=False)

    def as_metadata(self) -> dict[str, int]:
        """Return the JSON form carried only through the private broker channel."""
        return {
            "device": self.device,
            "inode": self.inode,
            "owner": self.owner,
            "link_count": self.link_count,
            "mode": self.mode,
        }


PACKAGE_PROGRESS_LOG_READ_BYTES = 1024 * 1024
PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES = 64 * 1024 * 1024
PROGRESS_SIDECAR_MAX_RECORD_BYTES = 64 * 1024
PROGRESS_SIDECAR_MAX_TOTAL_BYTES = 16 * 1024 * 1024
PROGRESS_SIDECAR_MAX_RECORDS = 10_000
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
# One exact native JARVIS snapshot may be 4 MiB before the execution record,
# handle, sidecar envelope, and HMAC are added.
RUNTIME_SIDECAR_MAX_RECORD_BYTES = 5 * 1024 * 1024
RUNTIME_SIDECAR_MAX_TOTAL_BYTES = 64 * 1024 * 1024
RUNTIME_SIDECAR_MAX_RECORDS = 4_096
SIDECAR_DRAIN_CHUNK_BYTES = 64 * 1024
MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-package-progress-bridge.v1"
MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-jarvis-progress-bridge.v1"
OUTPUT_EVENT_MAX_BYTES = 64 * 1024
# One larger than the queue's enforced active-lease scan bound. Since a pending
# cleanup marker blocks a second lease for its job, a full batch cannot consist
# only of live-lease markers while hiding an eligible marker beyond the batch.
EXECUTION_CLEANUP_SCAN_LIMIT = DEFAULT_EXACT_RECORD_LIMIT + 1
EXECUTION_CLEANUP_SCHEMA = "clio-relay.execution-cleanup.v1"
EXECUTION_SIDECAR_CLEANUP_SCHEMA = "clio-relay.execution-sidecar-cleanup.v1"
EXECUTION_SIDECAR_QUARANTINE_SCHEMA = "clio-relay.execution-sidecar-quarantine.v1"
RUNTIME_SIDECAR_CHANNEL_SCHEMA = "clio-relay.runtime-sidecar-channel.v1"
EXECUTION_LAUNCH_PROTOCOL = "broker-release-after-ownership-v1"


class SchedulerSubmissionUnresolvedError(RelayError):
    """An armed scheduler intent could not yet be resolved to zero or one owned job."""


class EndpointWorker:
    """Endpoint worker for desktop or cluster roles."""

    lease_ttl_seconds = 120
    lease_renew_seconds = 30
    scheduler_cancel_max_attempts = 5
    scheduler_cancel_confirmation_max_attempts = 5
    scheduler_cancel_retry_base_seconds = 2.0
    scheduler_cancel_retry_max_seconds = 30.0
    scheduler_cancel_claim_lease_seconds = 60.0
    scheduler_cancel_confirmation_claim_lease_seconds = 60.0
    scheduler_poll_interval_seconds = 5.0

    def __init__(
        self,
        *,
        role: EndpointRole,
        settings: RelaySettings,
        cluster: str = "local",
        concurrency: int = 1,
        kind_concurrency: KindConcurrencyInput | None = None,
        queue: ClioCoreQueue | None = None,
        provider: JarvisCdProvider | None = None,
        scheduler_provider: SchedulerProvider | None = None,
        storage_runtime: StorageRuntime | None = None,
    ) -> None:
        if concurrency < 1:
            raise ConfigurationError("worker concurrency must be at least 1")
        self.role = role
        self.cluster = cluster
        self.concurrency = concurrency
        self.kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self._closed = False
        self._queue_root_path: Path | None = None
        self._queue_root_identity: tuple[int, int] | None = None
        self._worker_lifetime_lock: WorkerLifetimeLock | None = None
        self._owned_managed_queue: StorageManagedQueue | None = None
        if self.role == EndpointRole.WORKER:
            lifetime_core = queue.root if queue is not None else settings.core_dir
            self._worker_lifetime_lock = WorkerLifetimeLock(
                lifetime_core,
                mode="shared",
            ).acquire()
            settings = settings.model_copy(update={"core_dir": self._worker_lifetime_lock.core_dir})
        self.settings = settings
        try:
            resolved_queue = (
                queue
                if queue is not None
                else storage_managed_queue(
                    settings,
                    writer_lifetime_lock=self._worker_lifetime_lock,
                )
            )
            if self.role == EndpointRole.WORKER:
                canonical_stat = os.stat(settings.core_dir)
                queue_stat = os.stat(resolved_queue.root)
                if (queue_stat.st_dev, queue_stat.st_ino) != (
                    canonical_stat.st_dev,
                    canonical_stat.st_ino,
                ):
                    raise ConfigurationError(
                        "worker queue root does not match its core lifetime lock"
                    )
                self._queue_root_path = resolved_queue.root
                self._queue_root_identity = (queue_stat.st_dev, queue_stat.st_ino)
            managed_runtime = (
                resolved_queue.storage_runtime
                if isinstance(resolved_queue, StorageManagedQueue)
                else None
            )
            if queue is None and isinstance(resolved_queue, StorageManagedQueue):
                self._owned_managed_queue = resolved_queue
            if (
                storage_runtime is not None
                and managed_runtime is not None
                and storage_runtime is not managed_runtime
            ):
                raise ConfigurationError(
                    "worker storage runtime must match its managed queue instance"
                )
            self.queue = resolved_queue
            self.provider = provider or JarvisCdProvider(
                jarvis_bin=settings.jarvis_bin,
                agent_bin=settings.agent_bin,
                agent_adapter=settings.agent_adapter,
                agent_args=settings.agent_args,
            )
            self.scheduler_provider = scheduler_provider
            self.storage_runtime = storage_runtime or managed_runtime
            self._scheduler_last_poll: dict[tuple[str, str], float] = {}
            self.endpoint: EndpointRegistration | None = None
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release endpoint-owned queue and core-scoped lifetime ownership."""
        self._closed = True
        owned_queue = self._owned_managed_queue
        self._owned_managed_queue = None
        if owned_queue is not None:
            owned_queue.close()
        lifetime_lock = self._worker_lifetime_lock
        if lifetime_lock is None:
            return
        self._worker_lifetime_lock = None
        lifetime_lock.release()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    def _require_open_queue_identity(self) -> None:
        """Reject use after close or after an injected queue alias is retargeted."""
        if self._closed:
            raise ConfigurationError("endpoint worker is closed")
        if self.role != EndpointRole.WORKER:
            return
        queue_root = self._queue_root_path
        expected = self._queue_root_identity
        if queue_root is None or expected is None or self._worker_lifetime_lock is None:
            raise ConfigurationError("worker lifetime ownership is incomplete")
        try:
            observed_stat = os.stat(queue_root)
        except OSError as exc:
            raise ConfigurationError(
                f"worker queue root identity cannot be verified: {exc}"
            ) from exc
        if (observed_stat.st_dev, observed_stat.st_ino) != expected:
            raise ConfigurationError("worker queue root identity changed after lifetime locking")

    def register(self) -> EndpointRegistration:
        """Register this endpoint in the durable queue."""
        self._require_open_queue_identity()
        metadata: dict[str, object] = {
            "concurrency": self.concurrency,
            "kind_concurrency": kind_concurrency_metadata(self.kind_concurrency),
            "process_containment": process_containment.containment_capability(),
        }
        if self.role == EndpointRole.WORKER and self.concurrency > 1:
            metadata["worker_supervisor"] = True
        if self.role == EndpointRole.WORKER:
            metadata["installation_info"] = _worker_installation_snapshot()
            process_identity = _worker_process_identity()
            if process_identity is not None:
                metadata["process_identity"] = process_identity
            metadata["scheduler_provider"] = (
                self.scheduler_provider.name if self.scheduler_provider is not None else "external"
            )
        endpoint = EndpointRegistration(
            role=self.role,
            cluster=self.cluster if self.role == EndpointRole.WORKER else None,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            metadata=metadata,
        )
        self.endpoint = self.queue.register_endpoint(endpoint)
        return self.endpoint

    def run_once(self) -> RelayJob | None:
        """Run one leased cluster job if available."""
        self._require_open_queue_identity()
        if self.role != EndpointRole.WORKER:
            return None
        endpoint = self.endpoint or self.register()
        self.endpoint = self.queue.register_endpoint(endpoint)
        self.queue.recover_stale_jobs(cluster=self.cluster)
        self._reconcile_pending_execution_cleanup()
        self._reconcile_canceled_scheduler_jobs()
        lease = self.queue.acquire_next_job(
            endpoint.endpoint_id,
            cluster=self.cluster,
            ttl_seconds=self.lease_ttl_seconds,
            kind_concurrency=self.kind_concurrency,
        )
        if lease is None:
            return None
        job = self.queue.get_job(lease.job_id)
        try:
            try:
                self._run_job(job, lease)
            except Exception as exc:
                self._record_unhandled_job_failure(job, exc)
        finally:
            self.queue.release_lease(lease.lease_id)
        return self.queue.get_job(job.job_id)

    def _record_unhandled_job_failure(self, job: RelayJob, error: Exception) -> None:
        detail = logical_filesystem_text(f"{type(error).__name__}: {error}")
        current = self.queue.get_job(job.job_id)
        if current.state == JobState.CANCELED:
            self.queue.append_event(
                job.job_id,
                "worker.job_error_after_cancel",
                "Worker caught an execution error after cancellation",
                payload={"error": detail},
            )
            return
        if isinstance(current.metadata.get("cancellation_request"), dict):
            self.queue.append_event(
                job.job_id,
                "cancellation.cleanup_failed",
                "Worker cancellation cleanup failed; job will fail rather than acknowledge cancel",
                payload={"error": detail},
            )
        for task in self._bounded_job_tasks(job.job_id):
            if task.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}:
                continue
            self.queue.update_task_state(
                task.task_id,
                JobState.FAILED,
                message=f"Task failed after unhandled worker error: {detail}",
                metadata={"worker_error": detail},
            )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message=f"Worker execution failed: {detail}",
            error=detail,
        )

    def serve_forever(self, *, poll_seconds: float = 2.0) -> None:
        """Run the endpoint loop until interrupted."""
        self._require_open_queue_identity()
        self.register()
        if self.role == EndpointRole.DESKTOP:
            while True:
                time.sleep(poll_seconds)
        with self._single_cluster_worker_lock():
            if self.concurrency > 1:
                self._serve_worker_slots(poll_seconds=poll_seconds)
                return
            while True:
                self.run_once()
                time.sleep(poll_seconds)

    def _serve_worker_slots(self, *, poll_seconds: float) -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [
                executor.submit(self._serve_worker_slot, index, poll_seconds)
                for index in range(self.concurrency)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def _serve_worker_slot(self, index: int, poll_seconds: float) -> None:
        worker = EndpointWorker(
            role=self.role,
            settings=self.settings,
            cluster=self.cluster,
            concurrency=1,
            kind_concurrency=self.kind_concurrency,
            queue=self.queue,
            scheduler_provider=self.scheduler_provider,
            storage_runtime=self.storage_runtime,
        )
        endpoint = EndpointRegistration(
            role=self.role,
            cluster=self.cluster,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            metadata={
                "worker_slot": index,
                "parent_endpoint_id": None if self.endpoint is None else self.endpoint.endpoint_id,
                "concurrency": 1,
                "kind_concurrency": kind_concurrency_metadata(self.kind_concurrency),
                "process_containment": process_containment.containment_capability(),
                "installation_info": _worker_installation_snapshot(),
                "scheduler_provider": (
                    self.scheduler_provider.name
                    if self.scheduler_provider is not None
                    else "external"
                ),
            },
        )
        worker.endpoint = self.queue.register_endpoint(endpoint)
        while True:
            worker.run_once()
            time.sleep(poll_seconds)

    def _run_job(self, job: RelayJob, lease: Lease) -> None:
        sidecars: list[Path] = []
        sidecar_anchors: dict[Path, _RuntimeSidecarAnchor] = {}
        sidecar_task_ids: list[str] = []
        runtime_spools: list[JobSpool] = []
        primary_error: BaseException | None = None
        try:
            self._run_job_impl(
                job,
                lease,
                sidecars=sidecars,
                sidecar_anchors=sidecar_anchors,
                sidecar_task_ids=sidecar_task_ids,
                runtime_spools=runtime_spools,
            )
        except BaseException as exc:
            primary_error = exc
        if (
            primary_error is not None
            and not isinstance(primary_error, StorageRuntimeViolation)
            and runtime_spools
        ):
            try:
                self._check_runtime_storage(
                    job,
                    runtime_spools[0],
                    force_job_scan=True,
                )
            except BaseException as storage_error:
                primary_error = RelayError(
                    f"{primary_error}; final storage guard also failed: {storage_error}"
                )
        cleanup_error: Exception | None = None
        if sidecars:
            if isinstance(primary_error, SchedulerSubmissionUnresolvedError):
                _close_runtime_sidecar_anchors(sidecar_anchors)
                self.queue.append_event(
                    job.job_id,
                    "scheduler.reconciliation_pending",
                    "Execution evidence is retained until scheduler or direct intent resolves",
                    payload={
                        "task_ids": list(sidecar_task_ids),
                        "sidecar_count": len(sidecars),
                    },
                )
            else:
                try:
                    quarantined = _remove_execution_sidecars(
                        sidecars,
                        spool_path=self.settings.spool_dir / job.job_id,
                        expected_anchors=sidecar_anchors,
                        on_quarantined=lambda source, quarantine: (
                            self._stage_execution_sidecar_quarantine(
                                job.job_id,
                                sidecar_task_ids,
                                source,
                                quarantine,
                            )
                        ),
                    )
                    for task_id in sidecar_task_ids:
                        task = self.queue.get_task(task_id)
                        self.queue.acknowledge_execution_cleanup(
                            job.job_id,
                            task_id,
                            metadata=_execution_cleanup_ack_metadata(task, quarantined),
                        )
                    self.queue.append_event(
                        job.job_id,
                        "execution.sidecars_quarantined",
                        "Relay execution sidecars securely quarantined",
                        payload={"sidecar_count": len(sidecars)},
                    )
                except Exception as exc:
                    cleanup_error = exc
        if self.storage_runtime is not None:
            self.storage_runtime.forget_running_job(job.job_id)
        if primary_error is not None and cleanup_error is not None:
            raise RelayError(
                f"{primary_error}; execution sidecar cleanup also failed: {cleanup_error}"
            ) from primary_error
        if cleanup_error is not None:
            raise cleanup_error
        if primary_error is not None:
            raise primary_error

    def _run_job_impl(
        self,
        job: RelayJob,
        lease: Lease,
        *,
        sidecars: list[Path],
        sidecar_anchors: dict[Path, _RuntimeSidecarAnchor],
        sidecar_task_ids: list[str],
        runtime_spools: list[JobSpool],
    ) -> None:
        if self._job_cancellation_requested(job.job_id):
            self._reconcile_canceled_execution(job)
            self.queue.acknowledge_job_cancellation(job.job_id)
            self.queue.append_event(job.job_id, "job.cancel_acknowledged", "Canceled before start")
            return
        started_at = utc_now()
        last_renewed_at = [time.monotonic()]
        self.queue.update_job_state(job.job_id, JobState.RUNNING)
        task = self.queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name=f"{job.kind.value}.execution",
                metadata={"cluster": self.cluster, "attempt": job.attempts},
            )
        )
        self.queue.update_task_state(
            task.task_id,
            JobState.RUNNING,
            message=f"Task running: {task.name}",
        )
        spool = JobSpool(
            self.settings.spool_dir,
            job,
            max_log_bytes_per_stream=self.settings.spool_max_log_bytes_per_stream,
            max_log_bytes_per_job=self.settings.spool_max_log_bytes_per_job,
        )
        spool.initialize()
        runtime_spools.append(spool)
        self._check_runtime_storage(job, spool, force_job_scan=True)
        pipeline_name = _jarvis_pipeline_name(job)
        configured_scheduler_provider = _configured_scheduler_provider_name(self.scheduler_provider)
        scheduler_name: str | None = None
        if pipeline_name is None:
            yaml_text = self._render_job_yaml(job)
            scheduler_name = _scheduler_name_from_yaml(yaml_text)
            _validate_scheduler_launch_provider(
                requested=scheduler_name,
                configured=configured_scheduler_provider,
            )
            pipeline_path = spool.write_pipeline(yaml_text)
            package_progress_adapter = package_progress_adapter_from_pipeline(yaml_text)
            if package_progress_adapter is None:
                package_progress_logs = []
            else:
                package_progress_adapter.run_id = job.job_id
                declared_progress_logs = package_progress_adapter.progress_log_paths()
                if len(declared_progress_logs) > 1:
                    raise ConfigurationError(
                        "package progress providers may expose at most one log path"
                    )
                package_progress_logs = [
                    _normalize_package_progress_log_path(spool.path, path)
                    for path in declared_progress_logs
                ]
            self.queue.append_artifact(spool.artifact_for(pipeline_path, kind="jarvis_pipeline"))
            self.queue.append_event(
                job.job_id,
                "jarvis.started",
                "JARVIS-CD pipeline started",
                payload={"pipeline": str(pipeline_path)},
            )
        else:
            yaml_text = None
            pipeline_path = spool.path / "pipeline-reference.json"
            internal_filesystem_path(pipeline_path).write_text(
                json.dumps(
                    {"pipeline_name": pipeline_name, "execution": "jarvis_named_pipeline"},
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            package_progress_adapter = None
            package_progress_logs = []
            self.queue.append_artifact(
                spool.artifact_for(pipeline_path, kind="jarvis_pipeline_reference")
            )
            self.queue.append_event(
                job.job_id,
                "jarvis.named_pipeline",
                f"JARVIS-CD named pipeline started: {pipeline_name}",
                payload={"pipeline_name": pipeline_name},
            )
        if package_progress_adapter is not None:
            package_progress_source_authority = (
                PackageProgressSourceAuthority.PACKAGE_LOG
                if package_progress_logs
                else PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK
            )
            self.queue.append_event(
                job.job_id,
                "progress.provider_bound",
                "Package progress provider bound to execution",
                payload={
                    **package_progress_adapter.identity.as_metadata(),
                    "provider_source_authority": package_progress_source_authority.value,
                },
            )
        else:
            package_progress_source_authority = None
        jarvis_stdout_progress_adapter = (
            package_progress_adapter
            if package_progress_source_authority
            is PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK
            else None
        )
        package_log_progress_adapter = (
            package_progress_adapter
            if package_progress_source_authority is PackageProgressSourceAuthority.PACKAGE_LOG
            else None
        )
        package_progress_log_offsets = (
            self._baseline_package_progress_logs(job, package_progress_logs)
            if package_log_progress_adapter is not None
            else {}
        )
        if sys.platform.startswith("linux"):
            process_containment.enforce_linux_secret_memory_gate()
        progress_sidecar_token = secrets.token_urlsafe(32)
        progress_sidecar = spool.path / f".progress-{secrets.token_hex(16)}.jsonl"
        progress_sidecar_anchor = _precreate_runtime_sidecar(progress_sidecar)
        progress_sidecar_offset = [0]
        progress_sidecar_record_count = [0]
        progress_sidecar_sequence = [0]
        progress_sidecar_failures: list[str] = []
        runtime_sidecar_key = secrets.token_urlsafe(32)
        runtime_sidecar = spool.path / f".runtime-{secrets.token_hex(16)}.jsonl"
        try:
            runtime_sidecar_anchor = _precreate_runtime_sidecar(runtime_sidecar)
        except BaseException:
            _remove_execution_sidecars(
                [progress_sidecar],
                spool_path=spool.path,
                expected_anchors={progress_sidecar: progress_sidecar_anchor},
            )
            raise
        runtime_direct_proof_token = secrets.token_urlsafe(32)
        runtime_submission_intent = {
            "schema_version": "clio-relay.scheduler-submission-intent.v1",
            "execution_id": f"jarvis_{secrets.token_hex(16)}",
            "marker": f"clio-relay-{secrets.token_hex(16)}",
            "created_at": started_at.isoformat(),
            "scheduler_user": getpass.getuser(),
            "scheduler_expected": (
                True if scheduler_name is not None else "unknown" if pipeline_name else False
            ),
            "direct_proof_sha256": hashlib.sha256(
                runtime_direct_proof_token.encode("utf-8")
            ).hexdigest(),
        }
        sidecar_anchors[progress_sidecar] = progress_sidecar_anchor
        sidecar_anchors[runtime_sidecar] = runtime_sidecar_anchor
        sidecars.extend((progress_sidecar, runtime_sidecar))
        self.queue.register_execution_cleanup(
            task.task_id,
            {
                "execution_sidecars": {
                    "schema_version": "clio-relay.execution-sidecars.v1",
                    "progress": progress_sidecar.name,
                    "progress_anchor": progress_sidecar_anchor.as_metadata(),
                    "progress_anchor_required": True,
                    "runtime": runtime_sidecar.name,
                    "runtime_anchor": runtime_sidecar_anchor.as_metadata(),
                    "scheduler_submission_intent": runtime_submission_intent,
                },
                "execution_cleanup": {
                    "schema_version": EXECUTION_CLEANUP_SCHEMA,
                    "launch_protocol": EXECUTION_LAUNCH_PROTOCOL,
                    "acknowledgment_stage": "prepared",
                    "sidecars": {
                        "progress": _execution_sidecar_cleanup_plan(
                            progress_sidecar,
                            progress_sidecar_anchor,
                        ),
                        "runtime": _execution_sidecar_cleanup_plan(
                            runtime_sidecar,
                            runtime_sidecar_anchor,
                        ),
                    },
                },
                "runtime_sidecar_channel": {
                    "schema_version": RUNTIME_SIDECAR_CHANNEL_SCHEMA,
                    "state": "open",
                    "opened_at": started_at.isoformat(),
                    "evidence_retention": "whole_job_spool",
                },
            },
        )
        sidecar_task_ids.append(task.task_id)
        runtime_sidecar_offset = [0]
        runtime_sidecar_record_count = [0]
        runtime_sidecar_sequence = [0]
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] = [None]
        runtime_metadata_digests: set[str] = set()
        runtime_sidecar_failures: list[str] = []
        scheduler_job_ids: list[str] = []
        scheduler_cancel_attempted = [False]
        with _job_subprocess_env(
            {
                "CLIO_RELAY_PROGRESS_FILE": str(internal_filesystem_path(progress_sidecar)),
                "CLIO_RELAY_PROGRESS_TOKEN": progress_sidecar_token,
                "CLIO_RELAY_RUNTIME_METADATA_FILE": str(internal_filesystem_path(runtime_sidecar)),
                "CLIO_RELAY_RUNTIME_METADATA_TOKEN": runtime_sidecar_key,
                "CLIO_RELAY_RUNTIME_METADATA_ANCHOR": json.dumps(
                    runtime_sidecar_anchor.as_metadata(),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "CLIO_RELAY_RUNTIME_SUBMISSION_INTENT": json.dumps(
                    runtime_submission_intent,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "CLIO_RELAY_RUNTIME_DIRECT_PROOF": runtime_direct_proof_token,
                RUNTIME_SCHEDULER_PROVIDER_ENV: configured_scheduler_provider,
            }
        ) as execution_env:
            result = self._run_jarvis_streaming(
                job,
                pipeline_path=pipeline_path,
                pipeline_name=pipeline_name,
                cwd=spool.path,
                env=execution_env,
                on_stdout=lambda text: self._append_output(
                    job,
                    spool,
                    "stdout",
                    text,
                    package_progress_adapter=jarvis_stdout_progress_adapter,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_task_id=task.task_id,
                    runtime_metadata_state=runtime_metadata_state,
                    runtime_metadata_digests=runtime_metadata_digests,
                ),
                on_stderr=lambda text: self._append_output(
                    job,
                    spool,
                    "stderr",
                    text,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_task_id=task.task_id,
                    runtime_metadata_state=runtime_metadata_state,
                    runtime_metadata_digests=runtime_metadata_digests,
                ),
                on_start=lambda pid: self._append_execution_start(job, task, pid),
                should_cancel=lambda: self._should_cancel_job(
                    job,
                    task_id=task.task_id,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_cancel_attempted=scheduler_cancel_attempted,
                ),
                timeout_seconds=_job_timeout_seconds(job),
                on_timeout=lambda: self._handle_execution_timeout(
                    job,
                    task_id=task.task_id,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_cancel_attempted=scheduler_cancel_attempted,
                ),
                on_poll=lambda: self._poll_running_job(
                    lease,
                    last_renewed_at,
                    job=job,
                    task_id=task.task_id,
                    progress_sidecar=progress_sidecar,
                    progress_sidecar_offset=progress_sidecar_offset,
                    progress_sidecar_record_count=progress_sidecar_record_count,
                    progress_sidecar_sequence=progress_sidecar_sequence,
                    progress_sidecar_token=progress_sidecar_token,
                    progress_sidecar_anchor=progress_sidecar_anchor,
                    progress_sidecar_failures=progress_sidecar_failures,
                    scheduler_job_ids=scheduler_job_ids,
                    package_progress_adapter=package_log_progress_adapter,
                    package_progress_log_offsets=package_progress_log_offsets,
                    runtime_sidecar=runtime_sidecar,
                    runtime_sidecar_offset=runtime_sidecar_offset,
                    runtime_sidecar_record_count=runtime_sidecar_record_count,
                    runtime_sidecar_sequence=runtime_sidecar_sequence,
                    runtime_sidecar_key=runtime_sidecar_key,
                    runtime_sidecar_anchor=runtime_sidecar_anchor,
                    runtime_sidecar_failures=runtime_sidecar_failures,
                    runtime_metadata_state=runtime_metadata_state,
                    runtime_metadata_digests=runtime_metadata_digests,
                    spool=spool,
                ),
            )
        self._check_runtime_storage(job, spool, force_job_scan=True)
        self._ingest_runtime_metadata_sidecar(
            job,
            task_id=task.task_id,
            path=runtime_sidecar,
            offset=runtime_sidecar_offset,
            record_count=runtime_sidecar_record_count,
            sequence=runtime_sidecar_sequence,
            expected_key=runtime_sidecar_key,
            expected_anchor=runtime_sidecar_anchor,
            failures=runtime_sidecar_failures,
            state=runtime_metadata_state,
            digests=runtime_metadata_digests,
            scheduler_job_ids=scheduler_job_ids,
            allow_final_record=True,
        )
        native_runtime_active = runtime_metadata_state[
            0
        ] is not None and _runtime_metadata_is_native(runtime_metadata_state[0])
        if jarvis_stdout_progress_adapter is not None and not native_runtime_active:
            self._append_package_progress_records(
                job,
                jarvis_stdout_progress_adapter.finalize_jarvis_stdout(),
                source_event_seq=None,
                package_progress_provider=jarvis_stdout_progress_adapter,
                source_authority=PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK,
            )
        self._ingest_progress_sidecar(
            job,
            progress_sidecar,
            progress_sidecar_offset=progress_sidecar_offset,
            progress_sidecar_record_count=progress_sidecar_record_count,
            progress_sidecar_sequence=progress_sidecar_sequence,
            progress_sidecar_token=progress_sidecar_token,
            progress_sidecar_anchor=progress_sidecar_anchor,
            failures=progress_sidecar_failures,
            allow_final_record=True,
        )
        if package_log_progress_adapter is not None and not native_runtime_active:
            self._drain_package_progress_logs(
                job,
                package_log_progress_adapter,
                package_progress_log_offsets,
            )
            self._append_package_progress_records(
                job,
                package_log_progress_adapter.finalize_stdout(),
                source_event_seq=None,
                package_progress_provider=package_log_progress_adapter,
                source_authority=PackageProgressSourceAuthority.PACKAGE_LOG,
            )
        self._ingest_mcp_runtime_metadata(
            job,
            task_id=task.task_id,
            spool=spool,
            state=runtime_metadata_state,
            digests=runtime_metadata_digests,
            scheduler_job_ids=scheduler_job_ids,
        )
        scheduler_identity_reconciled = self._resolve_execution_ownership(
            job,
            task_id=task.task_id,
            state=runtime_metadata_state,
            digests=runtime_metadata_digests,
            scheduler_job_ids=scheduler_job_ids,
            runtime_sidecar_failures=runtime_sidecar_failures,
        )
        if (
            scheduler_identity_reconciled
            and self._job_cancellation_requested(job.job_id)
            and self._scheduler_cancel_was_requested(job.job_id)
            and scheduler_job_ids
            and not scheduler_cancel_attempted[0]
        ):
            self._cancel_scheduler_jobs(job, scheduler_job_ids)
            scheduler_cancel_attempted[0] = True
        if progress_sidecar_failures:
            raise RelayError(
                "authenticated package progress channel failed closed: "
                + "; ".join(progress_sidecar_failures)
            )
        if runtime_metadata_state[0] is not None:
            runtime_metadata_path = spool.write_runtime_metadata(
                runtime_metadata_state[0].model_dump(mode="json")
            )
            self.queue.append_artifact(
                spool.artifact_for(runtime_metadata_path, kind="runtime_metadata")
            )
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_available",
                "Structured runtime metadata available",
                payload={
                    "path": str(runtime_metadata_path),
                    "source": runtime_metadata_state[0].source.value,
                },
            )
        self.queue.append_artifact(spool.artifact_for(spool.path / "stdout.log", kind="stdout"))
        self.queue.append_artifact(spool.artifact_for(spool.path / "stderr.log", kind="stderr"))
        self.queue.append_artifact(spool.artifact_for(spool.log_capture_path, kind="log_capture"))
        self._append_optional_result_artifacts(job, spool)
        terminal_state = (
            JobState.CANCELED
            if self._job_cancellation_requested(job.job_id)
            else JobState.SUCCEEDED
            if result.returncode == 0
            else JobState.FAILED
        )
        self._append_provenance_artifact(
            job,
            spool,
            pipeline_path=pipeline_path,
            started_at=started_at.isoformat(),
            finished_at=utc_now().isoformat(),
            returncode=result.returncode,
            terminal_state=terminal_state,
            runtime_metadata=runtime_metadata_state[0],
        )
        self._check_runtime_storage(job, spool, force_job_scan=True)
        if self._job_cancellation_requested(job.job_id):
            self.queue.update_task_state(
                task.task_id,
                JobState.CANCELED,
                message=f"Task canceled: {task.name}",
                metadata={"returncode": result.returncode},
            )
            self.queue.append_event(
                job.job_id,
                "execution.canceled",
                "JARVIS-CD process terminated after cancellation",
                payload={"returncode": result.returncode},
            )
            self.queue.acknowledge_job_cancellation(job.job_id)
            return
        if result.returncode == 0:
            self.queue.update_task_state(
                task.task_id,
                JobState.SUCCEEDED,
                message=f"Task succeeded: {task.name}",
                metadata={"returncode": result.returncode},
            )
            self.queue.update_job_state(
                job.job_id,
                JobState.SUCCEEDED,
                message="JARVIS-CD run succeeded",
            )
            return
        self.queue.update_task_state(
            task.task_id,
            JobState.FAILED,
            message=f"Task failed: {task.name}",
            metadata={"returncode": result.returncode},
        )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message="JARVIS-CD run failed",
            error=f"exit code {result.returncode}",
        )

    def _append_provenance_artifact(
        self,
        job: RelayJob,
        spool: JobSpool,
        *,
        pipeline_path: Path,
        started_at: str,
        finished_at: str,
        returncode: int,
        terminal_state: JobState,
        runtime_metadata: JarvisRuntimeMetadata | None,
    ) -> None:
        provenance_path = spool.write_provenance(
            {
                "job": job.model_dump(mode="json"),
                "endpoint": None
                if self.endpoint is None
                else self.endpoint.model_dump(mode="json"),
                "execution": {
                    "cluster": self.cluster,
                    "role": self.role.value,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "returncode": returncode,
                    "terminal_state": terminal_state.value,
                },
                "provider": {
                    "name": "jarvis-cd",
                    "jarvis_bin": self.settings.jarvis_bin,
                    "agent_bin": self.settings.agent_bin,
                    "agent_adapter": self.settings.agent_adapter,
                    "agent_args": self.settings.agent_args,
                },
                "runtime_metadata": (
                    None if runtime_metadata is None else runtime_metadata.model_dump(mode="json")
                ),
                "spool": {
                    "path": str(spool.path),
                    "pipeline": str(pipeline_path),
                    "stdout": str(spool.path / "stdout.log"),
                    "stderr": str(spool.path / "stderr.log"),
                    "log_capture": spool.capture_summary(),
                },
                "artifacts": {
                    "pipeline": _file_summary(pipeline_path),
                    "stdout": _file_summary(spool.path / "stdout.log"),
                    "stderr": _file_summary(spool.path / "stderr.log"),
                    "log_capture": _file_summary(spool.log_capture_path),
                },
            }
        )
        self.queue.append_artifact(spool.artifact_for(provenance_path, kind="provenance"))
        self.queue.append_event(
            job.job_id,
            "provenance.available",
            "Execution provenance available",
            payload={"path": str(provenance_path)},
        )

    def _render_job_yaml(self, job: RelayJob) -> str:
        if job.kind == JobKind.JARVIS and isinstance(job.spec, JarvisRunSpec):
            return self.provider.render_bounded_command_yaml(job.spec)
        if job.kind == JobKind.REMOTE_AGENT and isinstance(job.spec, RemoteAgentTaskSpec):
            return self.provider.render_remote_agent_task_yaml(job.spec)
        if job.kind == JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec):
            return self.provider.render_mcp_call_yaml(job.spec)
        raise ConfigurationError(f"job kind/spec mismatch for {job.job_id}")

    def _run_jarvis_streaming(
        self,
        job: RelayJob,
        *,
        pipeline_path: Path,
        pipeline_name: str | None,
        cwd: Path | None,
        env: dict[str, str],
        on_stdout: Callable[[str], None],
        on_stderr: Callable[[str], None],
        on_start: Callable[[int], None],
        should_cancel: Callable[[], bool],
        timeout_seconds: int | None,
        on_timeout: Callable[[], None],
        on_poll: Callable[[], None],
    ) -> subprocess.CompletedProcess[str]:
        runtime_cwd = None if cwd is None else _validated_native_subprocess_cwd(cwd)
        if pipeline_name is not None:
            return self.provider.run_named_pipeline_streaming(
                pipeline_name,
                cwd=runtime_cwd,
                env=env,
                on_stdout=on_stdout,
                on_stderr=on_stderr,
                on_start=on_start,
                should_cancel=should_cancel,
                timeout_seconds=timeout_seconds,
                on_timeout=on_timeout,
                on_poll=on_poll,
            )
        return self.provider.run_pipeline_streaming(
            internal_filesystem_path(pipeline_path),
            cwd=runtime_cwd,
            env=env,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            on_start=on_start,
            should_cancel=should_cancel,
            timeout_seconds=timeout_seconds,
            on_timeout=on_timeout,
            on_poll=on_poll,
        )

    def _append_output(
        self,
        job: RelayJob,
        spool: JobSpool,
        stream_name: str,
        text: str,
        package_progress_adapter: PackageProgressProvider | None = None,
        scheduler_job_ids: list[str] | None = None,
        scheduler_task_id: str | None = None,
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] | None = None,
        runtime_metadata_digests: set[str] | None = None,
    ) -> None:
        if stream_name not in {"stdout", "stderr"}:
            raise ConfigurationError(f"unsupported stream: {stream_name}")
        typed_stream = "stdout" if stream_name == "stdout" else "stderr"
        append_result = spool.append_log(typed_stream, text)
        output_events: list[RelayEvent] = []
        for event_text in _bounded_output_event_chunks(append_result.accepted_text):
            output_events.append(
                self.queue.append_event(
                    job.job_id,
                    f"{stream_name}.delta",
                    event_text.rstrip("\n") or f"{stream_name} output",
                    payload={"stream": stream_name, "text": event_text},
                )
            )
        if append_result.truncation_event_required:
            self.queue.append_event(
                job.job_id,
                f"{stream_name}.truncated",
                f"{stream_name} durable capture reached its configured byte quota",
                payload={
                    "stream": stream_name,
                    "observed_chunk_bytes": append_result.observed_bytes,
                    "accepted_chunk_bytes": append_result.accepted_bytes,
                    "dropped_chunk_bytes": append_result.dropped_bytes,
                    "persisted_stream_bytes": append_result.persisted_stream_bytes,
                    "persisted_job_bytes": append_result.persisted_job_bytes,
                    "max_bytes_per_stream": self.settings.spool_max_log_bytes_per_stream,
                    "max_bytes_per_job": self.settings.spool_max_log_bytes_per_job,
                },
            )
            spool.mark_truncation_event_recorded(typed_stream)
        if scheduler_job_ids is not None:
            self._capture_scheduler_job_ids(
                job,
                text,
                scheduler_job_ids,
                scheduler_task_id=scheduler_task_id,
                runtime_metadata_state=runtime_metadata_state,
                runtime_metadata_digests=runtime_metadata_digests,
            )
        if typed_stream != "stdout":
            return
        self._append_ignored_stdout_markers(job, text)
        native_runtime_active = (
            runtime_metadata_state is not None
            and runtime_metadata_state[0] is not None
            and _runtime_metadata_is_native(runtime_metadata_state[0])
        )
        if package_progress_adapter is not None and not native_runtime_active:
            self._append_package_progress_records(
                job,
                package_progress_adapter.observe_jarvis_stdout(text),
                source_event_seq=(
                    output_events[0].seq
                    if append_result.dropped_bytes == 0 and len(output_events) == 1
                    else None
                ),
                package_progress_provider=package_progress_adapter,
                source_authority=PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK,
            )

    def _append_ignored_stdout_markers(
        self,
        job: RelayJob,
        text: str,
    ) -> None:
        for line in text.splitlines():
            if not line.startswith("CLIO_PROGRESS "):
                continue
            self.queue.append_event(
                job.job_id,
                "progress.marker_ignored",
                "Ignored untrusted stdout progress marker",
                payload={"reason": "stdout markers are not trusted package progress"},
            )

    def _append_package_progress_records(
        self,
        job: RelayJob,
        records: list[dict[str, object]],
        *,
        source_event_seq: int | None,
        progress_sidecar_authenticated: bool = False,
        package_progress_provider: PackageProgressProvider | None = None,
        source_authority: PackageProgressSourceAuthority | None = None,
    ) -> None:
        for typed_payload in records:
            try:
                metadata = _optional_metadata(typed_payload.get("metadata"))
                provider_validated_record = package_progress_provider is not None
                native_progress_record = False
                if not progress_sidecar_authenticated and package_progress_provider is None:
                    raise ConfigurationError("package progress record has no bound provider")
                if package_progress_provider is not None:
                    if source_authority is None:
                        raise ConfigurationError(
                            "package progress record has no selected source authority"
                        )
                    candidate_metadata = _trusted_provider_metadata(
                        metadata,
                        job_id=job.job_id,
                        provider=package_progress_provider,
                        source_authority=source_authority,
                        acceptance_validated=False,
                    )
                    acceptance_validated = False
                    try:
                        acceptance_validated = (
                            package_progress_provider.acceptance_progress_valid(
                                cast(dict[str, Any], candidate_metadata)
                            )
                            is True
                        )
                    except Exception as exc:
                        self.queue.append_event(
                            job.job_id,
                            "progress.provider_validation_failed",
                            f"Package progress provider validation failed: {type(exc).__name__}",
                            payload=package_progress_provider.identity.as_metadata(),
                        )
                    if not acceptance_validated:
                        self.queue.append_event(
                            job.job_id,
                            "progress.candidate_not_acceptance_validated",
                            "Package progress candidate did not satisfy the acceptance predicate",
                            payload=package_progress_provider.identity.as_metadata(),
                        )
                    trusted_metadata = _trusted_provider_metadata(
                        metadata,
                        job_id=job.job_id,
                        provider=package_progress_provider,
                        source_authority=source_authority,
                        acceptance_validated=acceptance_validated,
                    )
                elif progress_sidecar_authenticated and isinstance(
                    metadata.get("mcp_progress_bridge"), dict
                ):
                    trusted_metadata = _trusted_mcp_progress_metadata(job, metadata)
                    provider_validated_record = True
                elif progress_sidecar_authenticated and isinstance(
                    metadata.get("mcp_native_progress_bridge"), dict
                ):
                    trusted_metadata = _trusted_native_mcp_progress_metadata(job, metadata)
                    native_progress_record = True
                else:
                    trusted_metadata = _trusted_sidecar_metadata(metadata, job_id=job.job_id)
                progress = ProgressRecord(
                    job_id=job.job_id,
                    label=str(typed_payload.get("label", "progress")),
                    current=_optional_float(typed_payload.get("current")),
                    total=_optional_float(typed_payload.get("total")),
                    unit=_optional_str(typed_payload.get("unit")),
                    message=_optional_str(typed_payload.get("message")),
                    source_event_seq=source_event_seq,
                    metadata=trusted_metadata,
                )
                if native_progress_record:
                    validate_jarvis_execution_progress_metadata(progress.metadata)
                    if progress.metadata["progress_determinate"] is not (
                        progress.current is not None and progress.total is not None
                    ):
                        raise ConfigurationError(
                            "native JARVIS progress determinate flag did not match values"
                        )
                elif provider_validated_record:
                    validate_package_progress_provider_metadata(progress.metadata)
                else:
                    validate_package_progress_metadata(progress.metadata)
            except (ConfigurationError, TypeError, ValueError) as exc:
                if progress_sidecar_authenticated:
                    raise ConfigurationError(
                        f"authenticated package progress was invalid: {exc}"
                    ) from exc
                self.queue.append_event(
                    job.job_id,
                    "progress.parse_failed",
                    f"Package progress was invalid: {exc}",
                )
                continue
            self.queue.append_progress(progress)

    def _ingest_progress_sidecar(
        self,
        job: RelayJob,
        progress_sidecar: Path,
        *,
        progress_sidecar_offset: list[int],
        progress_sidecar_record_count: list[int],
        progress_sidecar_sequence: list[int],
        progress_sidecar_token: str,
        progress_sidecar_anchor: _RuntimeSidecarAnchor,
        failures: list[str],
        allow_final_record: bool = False,
    ) -> None:
        def fail(message: str) -> None:
            if message not in failures:
                failures.append(message)
            self.queue.append_event(job.job_id, "progress.parse_failed", message)

        try:
            handle = _open_owned_sidecar(
                progress_sidecar,
                label="package progress sidecar",
                expected_anchor=progress_sidecar_anchor,
            )
        except ConfigurationError as exc:
            fail(str(exc))
            return
        if handle is None:
            fail("precreated package progress sidecar disappeared")
            return
        with handle:
            size = os.fstat(handle.fileno()).st_size
            if size > PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                if progress_sidecar_offset[0] <= PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                    fail("Package progress sidecar exceeded its total byte limit")
                progress_sidecar_offset[0] = size
                return
            handle.seek(progress_sidecar_offset[0])
            while True:
                if progress_sidecar_record_count[0] >= PROGRESS_SIDECAR_MAX_RECORDS:
                    if progress_sidecar_record_count[0] == PROGRESS_SIDECAR_MAX_RECORDS:
                        fail("Package progress sidecar exceeded its record limit")
                        progress_sidecar_record_count[0] += 1
                    progress_sidecar_offset[0] = os.fstat(handle.fileno()).st_size
                    return
                line, status = _read_bounded_sidecar_record(
                    handle,
                    max_bytes=PROGRESS_SIDECAR_MAX_RECORD_BYTES,
                    allow_final_record=allow_final_record,
                )
                if status in {"eof", "incomplete"}:
                    break
                if handle.tell() > PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                    fail("Package progress sidecar exceeded its total byte limit")
                    progress_sidecar_offset[0] = os.fstat(handle.fileno()).st_size
                    return
                progress_sidecar_record_count[0] += 1
                if status == "oversized":
                    fail("Package progress sidecar record exceeded its byte limit")
                    continue
                assert line is not None
                try:
                    payload = json.loads(line)
                    progress_payload = _progress_from_sidecar_record(
                        payload,
                        expected_key=progress_sidecar_token,
                        expected_sequence=progress_sidecar_sequence[0] + 1,
                    )
                    self._append_package_progress_records(
                        job,
                        [progress_payload],
                        source_event_seq=None,
                        progress_sidecar_authenticated=True,
                    )
                except (
                    ConfigurationError,
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                ) as exc:
                    fail(f"Side-channel package progress was invalid: {exc}")
                else:
                    progress_sidecar_sequence[0] += 1
            progress_sidecar_offset[0] = handle.tell()

    def _poll_running_job(
        self,
        lease: Lease,
        last_renewed_at: list[float],
        *,
        job: RelayJob,
        task_id: str,
        progress_sidecar: Path,
        progress_sidecar_offset: list[int],
        progress_sidecar_record_count: list[int],
        progress_sidecar_sequence: list[int],
        progress_sidecar_token: str,
        progress_sidecar_anchor: _RuntimeSidecarAnchor,
        progress_sidecar_failures: list[str],
        scheduler_job_ids: list[str],
        package_progress_adapter: PackageProgressProvider | None = None,
        package_progress_log_offsets: dict[Path, _PackageProgressLogState] | None = None,
        runtime_sidecar: Path | None = None,
        runtime_sidecar_offset: list[int] | None = None,
        runtime_sidecar_record_count: list[int] | None = None,
        runtime_sidecar_sequence: list[int] | None = None,
        runtime_sidecar_key: str | None = None,
        runtime_sidecar_anchor: _RuntimeSidecarAnchor | None = None,
        runtime_sidecar_failures: list[str] | None = None,
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] | None = None,
        runtime_metadata_digests: set[str] | None = None,
        spool: JobSpool | None = None,
    ) -> None:
        self._renew_lease_if_needed(lease, last_renewed_at)
        if spool is not None:
            self._check_runtime_storage(job, spool)
        self._ingest_progress_sidecar(
            job,
            progress_sidecar,
            progress_sidecar_offset=progress_sidecar_offset,
            progress_sidecar_record_count=progress_sidecar_record_count,
            progress_sidecar_sequence=progress_sidecar_sequence,
            progress_sidecar_token=progress_sidecar_token,
            progress_sidecar_anchor=progress_sidecar_anchor,
            failures=progress_sidecar_failures,
        )
        if (
            runtime_sidecar is not None
            and runtime_sidecar_offset is not None
            and runtime_sidecar_record_count is not None
            and runtime_sidecar_sequence is not None
            and runtime_sidecar_key is not None
            and runtime_sidecar_anchor is not None
            and runtime_sidecar_failures is not None
            and runtime_metadata_state is not None
            and runtime_metadata_digests is not None
        ):
            self._ingest_runtime_metadata_sidecar(
                job,
                task_id=task_id,
                path=runtime_sidecar,
                offset=runtime_sidecar_offset,
                record_count=runtime_sidecar_record_count,
                sequence=runtime_sidecar_sequence,
                expected_key=runtime_sidecar_key,
                expected_anchor=runtime_sidecar_anchor,
                failures=runtime_sidecar_failures,
                state=runtime_metadata_state,
                digests=runtime_metadata_digests,
                scheduler_job_ids=scheduler_job_ids,
                allow_final_record=False,
            )
        native_runtime_active = (
            runtime_metadata_state is not None
            and runtime_metadata_state[0] is not None
            and _runtime_metadata_is_native(runtime_metadata_state[0])
        )
        if (
            package_progress_adapter is not None
            and package_progress_log_offsets is not None
            and not native_runtime_active
        ):
            self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                package_progress_log_offsets,
            )
        if scheduler_job_ids:
            self._refresh_scheduler_status(job, scheduler_job_ids, task_id=task_id)

    def _check_runtime_storage(
        self,
        job: RelayJob,
        spool: JobSpool,
        *,
        force_job_scan: bool = False,
    ) -> None:
        """Stop an owned execution which crosses a storage safety boundary."""
        if self.storage_runtime is None:
            return
        decision = self.storage_runtime.check_running_job(
            job.job_id,
            spool_path=spool.path,
            force_job_scan=force_job_scan,
        )
        if decision.allowed:
            return
        self.queue.append_event(
            job.job_id,
            "storage.runtime_guard_failed",
            "Execution crossed a durable storage safety boundary",
            payload=decision.to_dict(),
        )
        raise StorageRuntimeViolation(decision)

    def _ingest_package_progress_logs(
        self,
        job: RelayJob,
        package_progress_adapter: PackageProgressProvider,
        log_offsets: dict[Path, _PackageProgressLogState],
        *,
        max_bytes_per_path: int = PACKAGE_PROGRESS_LOG_READ_BYTES,
    ) -> tuple[int, bool]:
        if max_bytes_per_path < 1:
            raise ConfigurationError("package progress log read limit must be positive")
        bytes_read = 0
        all_at_eof = True
        for state in log_offsets.values():
            handle = _open_package_progress_log(state.path)
            if handle is None:
                continue
            with handle:
                opened_stat = os.fstat(handle.fileno())
                identity = _progress_log_identity(opened_stat)
                reset_reason: str | None = None
                if state.identity is not None and identity != state.identity:
                    reset_reason = "replaced"
                elif opened_stat.st_size < state.offset:
                    reset_reason = "truncated"
                elif not _progress_log_checkpoint_matches(state, handle):
                    reset_reason = "rewritten"
                if reset_reason is not None:
                    package_progress_adapter.reset_stdout()
                    state.offset = 0
                    state.checkpoint_offset = 0
                    state.checkpoint_sha256 = None
                    self.queue.append_event(
                        job.job_id,
                        "progress.provider_log_reset",
                        f"Package progress log source was {reset_reason}",
                        payload={
                            "path": str(state.path),
                            "reason": reset_reason,
                            "provider_source_authority": (
                                PackageProgressSourceAuthority.PACKAGE_LOG.value
                            ),
                        },
                    )
                state.identity = identity
                handle.seek(state.offset)
                payload = handle.read(max_bytes_per_path)
                state.offset = handle.tell()
                final_stat = os.fstat(handle.fileno())
                at_eof = state.offset >= final_stat.st_size
                state.checkpoint_offset, state.checkpoint_sha256 = _progress_log_checkpoint(
                    handle,
                    state.offset,
                    path=state.path,
                )
            bytes_read += len(payload)
            all_at_eof = all_at_eof and at_eof
            text = payload.decode("utf-8", errors="replace")
            if text == "":
                continue
            self._append_package_progress_records(
                job,
                package_progress_adapter.observe_stdout(text),
                source_event_seq=None,
                package_progress_provider=package_progress_adapter,
                source_authority=PackageProgressSourceAuthority.PACKAGE_LOG,
            )
        return bytes_read, all_at_eof

    def _drain_package_progress_logs(
        self,
        job: RelayJob,
        package_progress_adapter: PackageProgressProvider,
        log_offsets: dict[Path, _PackageProgressLogState],
    ) -> None:
        """Drain a completed provider log in bounded chunks before parser EOF."""
        remaining = PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES
        while remaining > 0:
            read_limit = min(PACKAGE_PROGRESS_LOG_READ_BYTES, remaining)
            consumed, at_eof = self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                log_offsets,
                max_bytes_per_path=read_limit,
            )
            remaining -= consumed
            if at_eof:
                return
            if consumed == 0:
                raise ConfigurationError("package progress log made no bounded-read progress")
        raise ConfigurationError(
            "package progress log exceeded the final bounded-read budget "
            f"of {PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES} bytes"
        )

    def _baseline_package_progress_logs(
        self,
        job: RelayJob,
        paths: list[Path],
    ) -> dict[Path, _PackageProgressLogState]:
        """Checkpoint provider logs before launch so historical bytes are never emitted."""
        states: dict[Path, _PackageProgressLogState] = {}
        for path in paths:
            handle = _open_package_progress_log(path)
            if handle is None:
                state = _PackageProgressLogState(
                    path=path,
                    offset=0,
                    identity=None,
                    checkpoint_offset=0,
                    checkpoint_sha256=None,
                )
            else:
                with handle:
                    opened_stat = os.fstat(handle.fileno())
                    checkpoint_offset, checkpoint_sha256 = _progress_log_checkpoint(
                        handle,
                        opened_stat.st_size,
                        path=path,
                    )
                    state = _PackageProgressLogState(
                        path=path,
                        offset=opened_stat.st_size,
                        identity=_progress_log_identity(opened_stat),
                        checkpoint_offset=checkpoint_offset,
                        checkpoint_sha256=checkpoint_sha256,
                    )
            states[path] = state
            self.queue.append_event(
                job.job_id,
                "progress.provider_log_baselined",
                "Package progress log baselined before launch",
                payload={
                    "path": str(path),
                    "prelaunch_size": state.offset,
                    "prelaunch_identity": _render_progress_log_identity(state.identity),
                    "provider_source_authority": PackageProgressSourceAuthority.PACKAGE_LOG.value,
                },
            )
        return states

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
    ) -> None:
        """Ingest structured runtime metadata returned by a remote MCP call."""
        route_valid, _route_reason = _trusted_jarvis_mcp_route(job)
        if not route_valid:
            return
        result_path = spool.path / "mcp-result.json"
        storage_result_path = internal_filesystem_path(result_path)
        if not storage_result_path.exists():
            return
        try:
            result_document = json.loads(storage_result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_read_failed",
                f"MCP runtime result could not be read: {exc}",
            )
            return
        trusted, reason = _trusted_jarvis_mcp_result(job, result_document)
        if not trusted:
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
            return
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
            return
        if _runtime_metadata_is_native(metadata):
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
    ) -> None:
        """Persist one normalized runtime observation to job, task, and events."""
        task = self.queue.get_task(task_id)
        failed_channel = _runtime_sidecar_channel_failed(task)
        incoming_reconciliation = _runtime_metadata_exact_marker_reconciliation(metadata)
        if failed_channel and (
            metadata.source is not RuntimeMetadataSource.RELAY_RECONCILIATION
            or incoming_reconciliation is None
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

    def _append_execution_start(self, job: RelayJob, task: RelayTask, pid: int) -> None:
        start_identity = process_containment.process_start_identity(pid)
        if start_identity is None:
            start_identity = f"process-not-observed:{pid}"
        execution = {
            "schema_version": "clio-relay.execution-ownership.v1",
            "pid": pid,
            "hostname": socket.gethostname(),
            "process_start_identity": start_identity,
            "process_group_id": pid if os.name != "nt" else None,
            "started_at": utc_now().isoformat(),
            "endpoint_id": None if self.endpoint is None else self.endpoint.endpoint_id,
            "containment": process_containment.owned_process_metadata(pid),
        }
        self.queue.update_task_metadata(task.task_id, {"execution_ownership": execution})
        self.queue.append_event(
            job.job_id,
            "execution.started",
            f"JARVIS-CD process started: {pid}",
            payload={
                "pid": pid,
                "hostname": execution["hostname"],
                "process_group_id": execution["process_group_id"],
                "task_id": task.task_id,
            },
        )

    def _reconcile_pending_execution_cleanup(self) -> None:
        """Retry durable cleanup for attempts whose worker lease is no longer live."""
        pending, has_more = self.queue.scan_execution_cleanup(
            cluster=self.cluster,
            limit=EXECUTION_CLEANUP_SCAN_LIMIT,
        )
        eligible = 0
        completed = 0
        failures: list[str] = []
        for marker in pending:
            task = self.queue.get_task(marker.task_id)
            repair_metadata: dict[str, object] = {}
            for key in ("execution_sidecars", "execution_cleanup"):
                if key not in task.metadata and key in marker.metadata:
                    repair_metadata[key] = marker.metadata[key]
            if repair_metadata:
                task = self.queue.update_task_metadata(task.task_id, repair_metadata)
            job = self.queue.get_job(task.job_id)
            leases, leases_truncated = self.queue.scan_job_leases(
                job.job_id,
                limit=EXECUTION_CLEANUP_SCAN_LIMIT,
            )
            if leases_truncated:
                failures.append(f"{task.task_id}: lease scan exceeded its safety bound")
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_cleanup_failed",
                    "Restart cleanup could not prove the job lease set",
                    payload={"task_id": task.task_id, "has_more": has_more},
                )
                continue
            if any(not lease.is_expired() for lease in leases):
                continue
            eligible += 1
            try:
                process_id = self._terminate_recorded_execution(
                    task,
                    allow_unstarted=True,
                )
                self._reconcile_recorded_scheduler_submission(job, task)
                cleanup_metadata = self._remove_recorded_execution_sidecars(job, task)
                cancellation_requested = job.state == JobState.CANCELED or isinstance(
                    job.metadata.get("cancellation_request"),
                    dict,
                )
                if task.state not in {
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                    JobState.CANCELED,
                }:
                    target_state = JobState.CANCELED if cancellation_requested else JobState.FAILED
                    self.queue.update_task_state(
                        task.task_id,
                        target_state,
                        message=(
                            f"Recovered task cancellation after worker restart: {task.name}"
                            if cancellation_requested
                            else f"Closed stale execution attempt after worker restart: {task.name}"
                        ),
                        metadata={"restart_cleanup_recovered": True},
                    )
                self.queue.acknowledge_execution_cleanup(
                    job.job_id,
                    task.task_id,
                    metadata={
                        **cleanup_metadata,
                        "restart_cleanup_acknowledged": True,
                        "restart_cleanup_at": utc_now().isoformat(),
                    },
                )
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_reconciled",
                    "Prior worker execution and sidecars were proven cleaned",
                    payload={
                        "task_id": task.task_id,
                        "pid": process_id,
                        "hostname": socket.gethostname(),
                        "cancellation_requested": cancellation_requested,
                        "has_more": has_more,
                    },
                )
                if cancellation_requested and job.state != JobState.CANCELED:
                    self.queue.acknowledge_job_cancellation(job.job_id)
                    self.queue.append_event(
                        job.job_id,
                        "job.cancel_acknowledged",
                        "Cancellation acknowledged after restart cleanup",
                    )
                completed += 1
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                failures.append(f"{task.task_id}: {detail}")
                self.queue.append_event(
                    job.job_id,
                    "execution.restart_cleanup_failed",
                    "Restart cleanup failed and remains queued for retry",
                    payload={
                        "task_id": task.task_id,
                        "error": detail,
                        "has_more": has_more,
                    },
                )
        self._record_execution_cleanup_scan(
            batch_size=len(pending),
            eligible=eligible,
            completed=completed,
            failed=len(failures),
            has_more=has_more,
        )
        if eligible > 0 and completed == 0 and failures:
            raise RelayError(
                "pending execution cleanup batch made no progress: " + "; ".join(failures)
            )

    def _record_execution_cleanup_scan(
        self,
        *,
        batch_size: int,
        eligible: int,
        completed: int,
        failed: int,
        has_more: bool,
    ) -> None:
        """Publish bounded cleanup progress in the durable worker registration."""
        if self.endpoint is None:
            return
        metadata = dict(self.endpoint.metadata)
        metadata["execution_cleanup_scan"] = {
            "schema_version": "clio-relay.execution-cleanup-scan.v1",
            "observed_at": utc_now().isoformat(),
            "batch_limit": EXECUTION_CLEANUP_SCAN_LIMIT,
            "batch_size": batch_size,
            "eligible": eligible,
            "completed": completed,
            "failed": failed,
            "has_more": has_more,
        }
        self.endpoint = self.queue.register_endpoint(
            self.endpoint.model_copy(update={"metadata": metadata})
        )

    def _terminate_recorded_execution(
        self,
        task: RelayTask,
        *,
        allow_unstarted: bool = False,
    ) -> int | None:
        """Terminate one task's recorded process tree, or prove launch was unreleased."""
        raw_ownership = task.metadata.get("execution_ownership")
        if not isinstance(raw_ownership, dict):
            raw_cleanup = task.metadata.get("execution_cleanup")
            cleanup = cast(dict[str, object], raw_cleanup) if isinstance(raw_cleanup, dict) else {}
            if (
                allow_unstarted
                and cleanup.get("schema_version") == EXECUTION_CLEANUP_SCHEMA
                and cleanup.get("launch_protocol") == EXECUTION_LAUNCH_PROTOCOL
            ):
                return None
            raise RelayError(
                f"cannot prove cleanup for prior task without execution ownership: {task.task_id}"
            )
        ownership = cast(dict[str, object], raw_ownership)
        if ownership.get("schema_version") != "clio-relay.execution-ownership.v1":
            raise RelayError(f"unsupported execution ownership for task {task.task_id}")
        current_hostname = socket.gethostname()
        hostname = ownership.get("hostname")
        if hostname != current_hostname:
            raise RelayError(
                f"cannot reconcile task {task.task_id} from host {hostname!r} "
                f"on replacement host {current_hostname!r}"
            )
        process_id = ownership.get("pid")
        start_identity = ownership.get("process_start_identity")
        process_group_id = ownership.get("process_group_id")
        raw_containment = ownership.get("containment")
        containment = (
            cast(dict[str, object], raw_containment) if isinstance(raw_containment, dict) else {}
        )
        if (
            not isinstance(process_id, int)
            or isinstance(process_id, bool)
            or process_id <= 0
            or not isinstance(start_identity, str)
            or not start_identity
            or (
                process_group_id is not None
                and (
                    not isinstance(process_group_id, int)
                    or isinstance(process_group_id, bool)
                    or process_group_id <= 0
                )
            )
        ):
            raise RelayError(f"invalid execution ownership for task {task.task_id}")
        try:
            process_containment.terminate_recorded_process_tree(
                process_id=process_id,
                expected_start_identity=start_identity,
                process_group_id=process_group_id,
                containment_mode=(
                    cast(str, containment["mode"])
                    if isinstance(containment.get("mode"), str)
                    else None
                ),
                systemd_unit=(
                    cast(str, containment["systemd_unit"])
                    if isinstance(containment.get("systemd_unit"), str)
                    else None
                ),
                cgroup_path=(
                    cast(str, containment["cgroup_path"])
                    if isinstance(containment.get("cgroup_path"), str)
                    else None
                ),
            )
        except RuntimeError as exc:
            raise RelayError(
                f"could not reconcile prior execution for task {task.task_id}: {exc}"
            ) from exc
        return process_id

    def _reconcile_canceled_execution(self, job: RelayJob) -> None:
        """Prove prior attempt cleanup before acknowledging a recovered cancellation."""
        active_tasks = [
            task
            for task in self._bounded_job_tasks(job.job_id)
            if task.state not in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
        ]
        for task in active_tasks:
            process_id = self._terminate_recorded_execution(task)
            cleanup_metadata = self._remove_recorded_execution_sidecars(job, task)
            self.queue.update_task_state(
                task.task_id,
                JobState.CANCELED,
                message=f"Recovered task cancellation after worker restart: {task.name}",
                metadata={"restart_cleanup_recovered": True},
            )
            self.queue.acknowledge_execution_cleanup(
                job.job_id,
                task.task_id,
                metadata={
                    **cleanup_metadata,
                    "restart_cleanup_acknowledged": True,
                    "restart_cleanup_at": utc_now().isoformat(),
                },
            )
            self.queue.append_event(
                job.job_id,
                "cancellation.execution_reconciled",
                "Prior worker execution tree was proven stopped",
                payload={
                    "task_id": task.task_id,
                    "pid": process_id,
                    "hostname": socket.gethostname(),
                },
            )

    def _stage_execution_sidecar_quarantine(
        self,
        job_id: str,
        task_ids: list[str],
        source: Path,
        quarantine: Path,
    ) -> None:
        """Durably stage one exact sidecar quarantine before cleanup acknowledgment."""
        matches: list[tuple[str, str]] = []
        for task_id in task_ids:
            task = self.queue.get_task(task_id)
            raw_cleanup = task.metadata.get("execution_cleanup")
            if not isinstance(raw_cleanup, dict):
                continue
            raw_sidecars = cast(dict[str, object], raw_cleanup).get("sidecars")
            if not isinstance(raw_sidecars, dict):
                continue
            for role, raw_state in cast(dict[str, object], raw_sidecars).items():
                if not isinstance(raw_state, dict):
                    continue
                state = cast(dict[str, object], raw_state)
                if state.get("source_name") == source.name:
                    matches.append((task_id, role))
        if len(matches) != 1:
            raise RelayError(
                f"execution sidecar quarantine ownership was not unique for {source.name}: {job_id}"
            )
        task_id, role = matches[0]
        self.queue.stage_execution_cleanup_sidecar(
            job_id,
            task_id,
            role=role,
            source_name=source.name,
            quarantine_name=quarantine.name,
        )

    def _ensure_recorded_execution_cleanup_plan(
        self,
        job: RelayJob,
        task: RelayTask,
        *,
        paths_by_role: dict[str, Path],
        expected_anchors: dict[Path, _RuntimeSidecarAnchor],
    ) -> RelayTask:
        """Atomically migrate an anchored legacy marker before quarantine."""
        raw_cleanup = task.metadata.get("execution_cleanup")
        if not isinstance(raw_cleanup, dict):
            raise RelayError(f"execution cleanup state is missing for task {task.task_id}")
        cleanup = cast(dict[str, object], raw_cleanup)
        if (
            cleanup.get("schema_version") != EXECUTION_CLEANUP_SCHEMA
            or cleanup.get("launch_protocol") != EXECUTION_LAUNCH_PROTOCOL
        ):
            raise RelayError(f"execution cleanup state is unsupported for task {task.task_id}")
        raw_plans = cleanup.get("sidecars")
        if isinstance(raw_plans, dict):
            return task
        if raw_plans is not None:
            raise RelayError(f"execution cleanup plans are invalid for task {task.task_id}")
        plans: dict[str, object] = {}
        for role, path in paths_by_role.items():
            anchor = expected_anchors.get(path)
            if anchor is None:
                raise RelayError(
                    f"legacy {role} sidecar anchor is missing for task {task.task_id}; "
                    "cleanup remains pending"
                )
            plans[role] = _execution_sidecar_cleanup_plan(path, anchor)
        return self.queue.migrate_execution_cleanup_plan(
            job.job_id,
            task.task_id,
            cleanup={
                **cleanup,
                "acknowledgment_stage": "prepared",
                "sidecars": plans,
            },
        )

    def _remove_recorded_execution_sidecars(
        self,
        job: RelayJob,
        task: RelayTask,
    ) -> dict[str, object]:
        raw_sidecars = task.metadata.get("execution_sidecars")
        if not isinstance(raw_sidecars, dict):
            raise RelayError(
                f"cannot prove sidecar cleanup for prior task without ownership: {task.task_id}"
            )
        sidecars = cast(dict[str, object], raw_sidecars)
        if sidecars.get("schema_version") != "clio-relay.execution-sidecars.v1":
            raise RelayError(f"unsupported execution sidecar ownership for task {task.task_id}")
        paths: list[Path] = []
        paths_by_role: dict[str, Path] = {}
        expected_anchors: dict[Path, _RuntimeSidecarAnchor] = {}
        for role, prefix in (("progress", ".progress-"), ("runtime", ".runtime-")):
            name = sidecars.get(role)
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or not name.startswith(prefix)
                or not name.endswith(".jsonl")
            ):
                raise RelayError(f"invalid {role} sidecar ownership for task {task.task_id}")
            path = self.settings.spool_dir / job.job_id / name
            paths.append(path)
            paths_by_role[role] = path
            anchor_key = f"{role}_anchor"
            raw_anchor = sidecars.get(anchor_key)
            if raw_anchor is not None:
                expected_anchors[path] = _runtime_sidecar_anchor_from_metadata(
                    raw_anchor,
                    task_id=task.task_id,
                )
            else:
                raise RelayError(
                    f"legacy {role} sidecar anchor is missing for task {task.task_id}; "
                    "cleanup remains pending"
                )
        task = self._ensure_recorded_execution_cleanup_plan(
            job,
            task,
            paths_by_role=paths_by_role,
            expected_anchors=expected_anchors,
        )
        quarantine_paths = _execution_cleanup_quarantine_paths(
            task,
            paths=paths,
            expected_anchors=expected_anchors,
        )
        quarantined = _remove_execution_sidecars(
            paths,
            spool_path=self.settings.spool_dir / job.job_id,
            expected_anchors=expected_anchors,
            expected_quarantines=quarantine_paths,
            on_quarantined=lambda source, quarantine: self._stage_execution_sidecar_quarantine(
                job.job_id,
                [task.task_id],
                source,
                quarantine,
            ),
        )
        return _execution_cleanup_ack_metadata(self.queue.get_task(task.task_id), quarantined)

    def _capture_scheduler_job_ids(
        self,
        job: RelayJob,
        text: str,
        scheduler_job_ids: list[str],
        *,
        scheduler_task_id: str | None,
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] | None,
        runtime_metadata_digests: set[str] | None,
    ) -> None:
        if (
            runtime_metadata_state is not None
            and runtime_metadata_state[0] is not None
            and runtime_metadata_state[0].source
            in {
                RuntimeMetadataSource.JARVIS_MCP,
                RuntimeMetadataSource.JARVIS_SIDECAR,
            }
        ):
            return
        for line in text.splitlines():
            job_id = _extract_scheduler_job_id(line)
            if job_id is None or job_id in scheduler_job_ids:
                continue
            if (
                scheduler_task_id is not None
                and runtime_metadata_state is not None
                and runtime_metadata_digests is not None
            ):
                self._persist_runtime_metadata(
                    job,
                    task_id=scheduler_task_id,
                    metadata=legacy_scheduler_runtime_metadata(
                        scheduler_job_id=job_id,
                        scheduler_provider=_scheduler_name_from_job(job) or "external",
                    ),
                    state=runtime_metadata_state,
                    digests=runtime_metadata_digests,
                    scheduler_job_ids=scheduler_job_ids,
                )

    def _should_cancel_job(
        self,
        job: RelayJob,
        *,
        task_id: str,
        scheduler_job_ids: list[str],
        scheduler_cancel_attempted: list[bool],
    ) -> bool:
        canceled = self._job_cancellation_requested(job.job_id)
        if not canceled or scheduler_cancel_attempted[0]:
            return canceled
        scheduler_cancel_attempted[0] = True
        if self._scheduler_cancel_was_requested(job.job_id):
            owned_ids = self._durable_scheduler_job_ids(job, task_id, scheduler_job_ids)
            if owned_ids:
                self._cancel_scheduler_jobs(job, owned_ids)
            else:
                self._record_scheduler_cancel_refused(job)
        return True

    def _job_cancellation_requested(self, job_id: str) -> bool:
        """Return whether an active or acknowledged job has a durable cancel request."""
        request = self.queue.get_job(job_id).metadata.get("cancellation_request")
        return isinstance(request, dict)

    def _handle_execution_timeout(
        self,
        job: RelayJob,
        *,
        task_id: str,
        scheduler_job_ids: list[str],
        scheduler_cancel_attempted: list[bool],
    ) -> None:
        durable_scheduler_job_ids = self._durable_scheduler_job_ids(
            job,
            task_id,
            scheduler_job_ids,
        )
        self.queue.append_event(
            job.job_id,
            "execution.timeout",
            "JARVIS-CD process exceeded timeout_seconds",
            payload={"scheduler_job_ids": durable_scheduler_job_ids},
        )
        self.queue.ensure_scheduler_cancel_pending(
            job.job_id,
            reason="execution_timeout",
        )
        if durable_scheduler_job_ids and not scheduler_cancel_attempted[0]:
            self._cancel_scheduler_jobs(job, durable_scheduler_job_ids)
            scheduler_cancel_attempted[0] = True
        elif not durable_scheduler_job_ids and not scheduler_cancel_attempted[0]:
            self._record_scheduler_cancel_refused(job)
            self.queue.complete_scheduler_cancel_identity_scan(
                job.job_id,
                cluster=job.cluster,
            )
            scheduler_cancel_attempted[0] = True

    def _record_scheduler_cancel_refused(
        self,
        job: RelayJob,
        *,
        scheduler_job_id: str | None = None,
        metadata_source: str | None = None,
    ) -> None:
        runtime_metadata = job.metadata.get("runtime_metadata")
        observed_scheduler_job_id = scheduler_job_id
        if observed_scheduler_job_id is None and isinstance(runtime_metadata, dict):
            typed_runtime = cast(dict[str, Any], runtime_metadata)
            candidate = typed_runtime.get("scheduler_job_id")
            if isinstance(candidate, str):
                observed_scheduler_job_id = candidate
            source = typed_runtime.get("source")
            if metadata_source is None and isinstance(source, str):
                metadata_source = source
        if observed_scheduler_job_id is None:
            return
        self.queue.append_event(
            job.job_id,
            "scheduler.cancel_refused",
            "Refused scheduler cancellation because no owned scheduler identity was available",
            payload={
                "scheduler_job_id": observed_scheduler_job_id,
                "metadata_source": metadata_source,
                "ownership_verified": False,
            },
        )

    def _refresh_scheduler_status(
        self,
        job: RelayJob,
        scheduler_job_ids: list[str],
        *,
        task_id: str | None,
        force: bool = False,
    ) -> None:
        provider = self._scheduler_provider_for_job(job)
        for scheduler_job_id in scheduler_job_ids:
            poll_key = (job.job_id, scheduler_job_id)
            now = time.monotonic()
            last_poll = self._scheduler_last_poll.get(poll_key)
            if (
                not force
                and last_poll is not None
                and now - last_poll < self.scheduler_poll_interval_seconds
            ):
                continue
            self._scheduler_last_poll[poll_key] = now
            try:
                status = provider.poll(scheduler_job_id)
            except RelayError as exc:
                error_detail = bounded_error_detail(str(exc)) or type(exc).__name__
                self.queue.append_event(
                    job.job_id,
                    "scheduler.poll_failed",
                    f"Scheduler status polling failed for {scheduler_job_id}: {error_detail}",
                    payload={
                        "scheduler": provider.name,
                        "scheduler_job_id": scheduler_job_id,
                        "error": error_detail,
                    },
                )
                continue
            self._record_scheduler_status(
                job,
                scheduler_job_ids,
                scheduler_job_id,
                status,
                task_id=task_id,
            )

    def _record_scheduler_status(
        self,
        job: RelayJob,
        scheduler_job_ids: list[str],
        scheduler_job_id: str,
        status: SchedulerStatus,
        *,
        task_id: str | None,
    ) -> None:
        provider = self._scheduler_provider_for_job(job)
        status = _normalized_scheduler_status(
            status,
            expected_scheduler=provider.name,
            expected_scheduler_job_id=scheduler_job_id,
        )
        tasks = self._bounded_job_tasks(job.job_id)
        target_task_id = task_id or _task_id_for_scheduler_job(tasks, scheduler_job_id)
        if target_task_id is None:
            return
        previous = _task_scheduler_status(
            tasks,
            target_task_id,
            scheduler_job_id,
        )
        status_payload = status.model_dump(mode="json")
        self.queue.update_task_metadata(
            target_task_id,
            {
                "scheduler": status.scheduler,
                "scheduler_job_ids": list(scheduler_job_ids),
                "scheduler_status": status_payload,
            },
        )
        previous_phase = previous.get("phase") if previous is not None else None
        if previous_phase == status.phase.value:
            return
        self.queue.append_event(
            job.job_id,
            f"scheduler.{status.phase.value}",
            f"Scheduler job {scheduler_job_id} is {status.phase.value}",
            payload=status_payload,
        )

    def _durable_scheduler_job_ids(
        self,
        job: RelayJob,
        task_id: str,
        scheduler_job_ids: list[str],
    ) -> list[str]:
        ids: list[str] = []
        for task in self._bounded_job_tasks(job.job_id):
            if task.task_id != task_id:
                continue
            for item in _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            ):
                if item not in ids:
                    ids.append(item)
        for scheduler_job_id in scheduler_job_ids:
            if scheduler_job_id in ids:
                continue
            if self._scheduler_job_id_is_owned(job, scheduler_job_id):
                ids.append(scheduler_job_id)
        return ids

    def _cancel_scheduler_jobs(self, job: RelayJob, scheduler_job_ids: list[str]) -> None:
        if not scheduler_job_ids:
            return
        pending = self.queue.ensure_scheduler_cancel_pending(
            job.job_id,
            reason="scheduler_cancel",
        )
        if pending.complete:
            return
        provider = self._scheduler_provider_for_job(job)
        for scheduler_job_id in scheduler_job_ids:
            ownership_verified = self._scheduler_job_id_is_owned(job, scheduler_job_id)
            registration = self.queue.register_scheduler_cancel_identity_once(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                ownership_verified=ownership_verified,
            )
            if not ownership_verified and registration.disposition_created:
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_refused",
                    f"Refused scheduler cancellation without ownership proof: {scheduler_job_id}",
                    payload={
                        "scheduler": _scheduler_name_from_job(job),
                        "scheduler_job_id": scheduler_job_id,
                        "ownership_verified": False,
                    },
                )
        finalized = self.queue.finalize_scheduler_cancel_identities(
            job.job_id,
            cluster=job.cluster,
        )
        if finalized.complete:
            return
        now = utc_now()
        confirmation_dispositions = [
            item
            for item in finalized.dispositions
            if item.state is SchedulerCancelDispositionState.CANCEL_REQUESTED
            and (item.next_attempt_at is None or item.next_attempt_at <= now)
        ]
        for disposition in confirmation_dispositions:
            self._confirm_scheduler_cancellation(
                job,
                provider,
                disposition.scheduler_job_id,
            )
        due_dispositions = [
            item
            for item in finalized.dispositions
            if item.state is SchedulerCancelDispositionState.PENDING
            or (
                item.state is SchedulerCancelDispositionState.RETRY_WAIT
                and (item.next_attempt_at is None or item.next_attempt_at <= now)
            )
        ]
        for disposition in due_dispositions:
            scheduler_job_id = disposition.scheduler_job_id
            claim = self.queue.claim_scheduler_cancel_attempt(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                lease_seconds=self.scheduler_cancel_claim_lease_seconds,
                now=utc_now(),
            )
            if claim is None:
                continue
            try:
                result = provider.cancel(scheduler_job_id)
            except (OSError, RelayError) as exc:
                result = subprocess.CompletedProcess(
                    [provider.name, scheduler_job_id],
                    1,
                    "",
                    str(exc),
                )
            error_detail = bounded_error_detail(result.stderr) if result.stderr else None
            attempt = claim.attempt
            retry_delay = min(
                self.scheduler_cancel_retry_base_seconds * 2 ** (attempt - 1),
                self.scheduler_cancel_retry_max_seconds,
            )
            recorded = self.queue.record_scheduler_cancel_attempt(
                job.job_id,
                cluster=job.cluster,
                scheduler_job_id=scheduler_job_id,
                provider=provider.name,
                claim_id=claim.claim_id,
                accepted=result.returncode == 0,
                error=error_detail,
                max_attempts=self.scheduler_cancel_max_attempts,
                retry_delay_seconds=retry_delay,
                now=utc_now(),
            )
            if recorded is None:
                continue
            if result.returncode == 0:
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_requested",
                    f"Requested scheduler cancellation: {scheduler_job_id}",
                    payload={
                        "scheduler": provider.name,
                        "scheduler_job_id": scheduler_job_id,
                    },
                )
                self._confirm_scheduler_cancellation(job, provider, scheduler_job_id)
                continue
            self.queue.append_event(
                job.job_id,
                "scheduler.cancel_failed",
                f"Scheduler cancellation failed: {scheduler_job_id}",
                payload={
                    "scheduler": provider.name,
                    "scheduler_job_id": scheduler_job_id,
                    "returncode": result.returncode,
                    "stderr": error_detail,
                    "attempt": attempt,
                    "max_attempts": self.scheduler_cancel_max_attempts,
                    "retryable": attempt < self.scheduler_cancel_max_attempts,
                    "retry_delay_seconds": retry_delay,
                },
            )

    def _reconcile_canceled_scheduler_jobs(self) -> None:
        pending_records, _ = self.queue.scan_due_scheduler_cancellations(
            cluster=self.cluster,
            limit=100,
            now=utc_now(),
        )
        for pending_record in pending_records:
            self._reconcile_canceled_scheduler_job(pending_record)

    def _reconcile_canceled_scheduler_job(
        self,
        pending_record: SchedulerCancelPending,
    ) -> None:
        """Resolve one durable scheduler-cancellation record idempotently."""
        try:
            job = self.queue.get_job(pending_record.job_id)
        except RelayError:
            return
        if pending_record.reason == "operator_request" and not self._scheduler_cancel_was_requested(
            job.job_id
        ):
            try:
                self.queue.complete_scheduler_cancel_identity_scan(
                    job.job_id,
                    cluster=self.cluster,
                    superseded=True,
                )
            except QueueConflictError:
                completed = self.queue.get_scheduler_cancel_disposition(
                    job.job_id,
                    cluster=job.cluster,
                )
                if completed is None:
                    raise
            return
        observed_ids: set[str] = set()
        owned_ids: set[str] = set()
        for task in self._bounded_job_tasks(job.job_id):
            observed_scheduler_job_ids = _scheduler_job_ids_from_metadata(task.metadata)
            scheduler_job_ids = _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            )
            observed_ids.update(observed_scheduler_job_ids)
            owned_ids.update(scheduler_job_ids)
        if pending_record.identity_resolution == "pending":
            provider = self._scheduler_provider_for_job(job)
            newly_refused: list[str] = []
            try:
                for scheduler_job_id in sorted(observed_ids):
                    ownership_verified = scheduler_job_id in owned_ids
                    registration = self.queue.register_scheduler_cancel_identity_once(
                        job.job_id,
                        cluster=job.cluster,
                        scheduler_job_id=scheduler_job_id,
                        provider=provider.name,
                        ownership_verified=ownership_verified,
                    )
                    if not ownership_verified and registration.disposition_created:
                        newly_refused.append(scheduler_job_id)
                if observed_ids:
                    self.queue.finalize_scheduler_cancel_identities(
                        job.job_id,
                        cluster=job.cluster,
                    )
                elif job.state in {
                    JobState.CANCELED,
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                }:
                    self.queue.complete_scheduler_cancel_identity_scan(
                        job.job_id,
                        cluster=job.cluster,
                    )
                    return
                else:
                    return
            except QueueConflictError:
                completed = self.queue.get_scheduler_cancel_disposition(
                    job.job_id,
                    cluster=job.cluster,
                )
                if completed is not None:
                    return
                raise
            for scheduler_job_id in newly_refused:
                self._record_scheduler_cancel_refused(
                    job,
                    scheduler_job_id=scheduler_job_id,
                    metadata_source="unverified_durable_metadata",
                )
        if owned_ids:
            self._cancel_scheduler_jobs(job, sorted(owned_ids))

    def _confirm_scheduler_cancellation(
        self,
        job: RelayJob,
        provider: SchedulerProvider,
        scheduler_job_id: str,
    ) -> None:
        """Poll one accepted cancellation until the exact scheduler id is terminal."""
        claim = self.queue.claim_scheduler_cancel_confirmation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider=provider.name,
            lease_seconds=self.scheduler_cancel_confirmation_claim_lease_seconds,
            now=utc_now(),
        )
        if claim is None:
            return
        try:
            status = provider.poll(scheduler_job_id)
        except RelayError as exc:
            error_detail = bounded_error_detail(str(exc)) or type(exc).__name__
            status = SchedulerStatus(
                scheduler=provider.name,
                scheduler_job_id=scheduler_job_id,
                phase=SchedulerPhase.UNKNOWN,
                reason="scheduler cancellation confirmation failed",
                queue_position_note=error_detail,
            )
        status = _normalized_scheduler_status(
            status,
            expected_scheduler=provider.name,
            expected_scheduler_job_id=scheduler_job_id,
        )
        if status.phase == SchedulerPhase.UNKNOWN:
            status = status.model_copy(
                update={
                    "reason": "scheduler cancellation requested; confirmation pending",
                    "queue_position_note": (
                        status.queue_position_note
                        or "provider did not return a terminal scheduler record yet"
                    ),
                }
            )
        retry_delay = min(
            self.scheduler_cancel_retry_base_seconds * 2 ** (claim.confirmation_attempt - 1),
            self.scheduler_cancel_retry_max_seconds,
        )
        recorded = self.queue.record_scheduler_cancel_observation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider=provider.name,
            claim_id=claim.claim_id,
            phase=status.phase,
            not_found=_scheduler_status_is_not_found(status),
            error=status.queue_position_note,
            max_confirmation_attempts=self.scheduler_cancel_confirmation_max_attempts,
            retry_delay_seconds=retry_delay,
            now=utc_now(),
        )
        if recorded is None:
            return
        self._record_scheduler_status(
            job,
            [scheduler_job_id],
            scheduler_job_id,
            status,
            task_id=None,
        )

    def _scheduler_job_id_is_owned(self, job: RelayJob, scheduler_job_id: str) -> bool:
        return any(
            scheduler_job_id
            in _owned_scheduler_job_ids_from_metadata(
                task.metadata,
                relay_job_id=job.job_id,
                task_id=task.task_id,
            )
            for task in self._bounded_job_tasks(job.job_id)
        )

    def _bounded_job_tasks(self, job_id: str) -> list[RelayTask]:
        """Read an exact job's tasks within the worker safety bound."""
        tasks, truncated = self.queue.scan_job_tasks(
            job_id,
            limit=DEFAULT_EXACT_RECORD_LIMIT,
        )
        if truncated:
            raise RelayError(f"job task index exceeded its safety bound: {job_id}")
        return tasks

    def _scheduler_cancel_was_requested(self, job_id: str) -> bool:
        job = self.queue.get_job(job_id)
        request = job.metadata.get("cancellation_request")
        if isinstance(request, dict):
            typed_request = cast(dict[str, Any], request)
            if typed_request.get("schema_version") == "clio-relay.cancellation-request.v1":
                cancel_scheduler = typed_request.get("cancel_scheduler")
                if isinstance(cancel_scheduler, bool):
                    return cancel_scheduler
        return False

    def _scheduler_provider_for_job(self, job: RelayJob) -> SchedulerProvider:
        runtime_metadata = job.metadata.get("runtime_metadata")
        structured_name: str | None = None
        if isinstance(runtime_metadata, dict):
            candidate = cast(dict[str, Any], runtime_metadata).get("scheduler_provider")
            if isinstance(candidate, str) and candidate.strip():
                structured_name = candidate
        if self.scheduler_provider is not None:
            if structured_name is not None:
                normalized_name = structured_name.strip().lower().replace("_", "-")
                if normalized_name in {"none", "unmanaged"}:
                    normalized_name = "external"
                if normalized_name != self.scheduler_provider.name:
                    raise ConfigurationError(
                        "JARVIS runtime metadata scheduler provider does not match the "
                        f"configured worker provider: {normalized_name} != "
                        f"{self.scheduler_provider.name}"
                    )
            return self.scheduler_provider
        if structured_name is not None:
            return provider_for_scheduler(structured_name)
        return provider_for_scheduler(_scheduler_name_from_job(job))

    def _append_optional_result_artifacts(self, job: RelayJob, spool: JobSpool) -> None:
        candidates = {
            "agent_result": spool.path / "agent-result.json",
            "agent_last_message": spool.path / "agent-last-message.txt",
            "mcp_result": spool.path / "mcp-result.json",
        }
        for kind, path in candidates.items():
            if internal_filesystem_path(path).exists():
                self.queue.append_artifact(spool.artifact_for(path, kind=kind))
                self.queue.append_event(
                    job.job_id,
                    f"{kind}.available",
                    f"Result artifact available: {kind}",
                    payload={"path": str(path)},
                )

    def _renew_lease_if_needed(self, lease: Lease, last_renewed_at: list[float]) -> None:
        now = time.monotonic()
        if now - last_renewed_at[0] < self.lease_renew_seconds:
            return
        if self.endpoint is None:
            raise QueueConflictError(
                f"worker endpoint disappeared before lease heartbeat: {lease.endpoint_id}"
            )
        if self.endpoint.endpoint_id != lease.endpoint_id:
            raise QueueConflictError(
                "worker endpoint identity does not match the running lease: "
                f"{self.endpoint.endpoint_id} != {lease.endpoint_id}"
            )
        self.endpoint = self.queue.register_endpoint(self.endpoint)
        renewed = self.queue.renew_lease(
            lease.lease_id,
            ttl_seconds=self.lease_ttl_seconds,
        )
        if renewed is None:
            raise QueueConflictError(
                f"running lease disappeared before heartbeat: {lease.lease_id}"
            )
        if renewed.job_id != lease.job_id or renewed.endpoint_id != lease.endpoint_id:
            raise QueueConflictError(
                f"running lease identity changed before heartbeat: {lease.lease_id}"
            )
        last_renewed_at[0] = now

    @contextmanager
    def _single_cluster_worker_lock(self) -> Generator[None, None, None]:
        cluster_key = filesystem_key(self.cluster, domain="cluster")
        lock_path = self.settings.core_dir / f"{cluster_key}-worker.lock"
        lock = FileLock(str(internal_filesystem_path(lock_path)), timeout=0)
        try:
            lock.acquire()
        except Timeout as exc:
            raise ConfigurationError(
                f"another {self.cluster} endpoint worker is already active"
            ) from exc
        try:
            yield
        finally:
            lock.release()


def _worker_installation_snapshot() -> dict[str, object]:
    """Capture the package/receipt identity loaded by this worker process."""
    try:
        return installation_info()
    except ConfigurationError as exc:
        return {
            "schema_version": "clio-relay.installation-info.unverified",
            "receipt_matches_install": False,
            "error": str(exc),
        }


def _worker_process_identity() -> dict[str, object] | None:
    """Return exact Linux process-generation evidence for durable endpoint records."""
    if os.name != "posix" or not hasattr(os, "getuid"):
        return None
    try:
        boot_id = (
            Path("/proc/sys/kernel/random/boot_id")
            .read_text(
                encoding="ascii",
            )
            .strip()
        )
        raw_stat = Path("/proc/self/stat").read_bytes()
    except OSError:
        return None
    if not boot_id or len(boot_id) > 128 or len(raw_stat) > 4096:
        return None
    closing_parenthesis = raw_stat.rfind(b")")
    fields = raw_stat[closing_parenthesis + 1 :].split()
    if closing_parenthesis < 0 or len(fields) <= 19:
        return None
    try:
        start_ticks = int(fields[19])
    except ValueError:
        return None
    return {
        "schema_version": "clio-relay.process-identity.v1",
        "boot_id": boot_id,
        "start_ticks": start_ticks,
        "uid": os.getuid(),
        "pid": os.getpid(),
    }


def bootstrap_cluster_environment(settings: RelaySettings) -> None:
    """Create endpoint directories and verify required executables are configured."""
    internal_filesystem_path(settings.core_dir, force_extended=True).mkdir(
        parents=True,
        exist_ok=True,
    )
    internal_filesystem_path(settings.spool_dir, force_extended=True).mkdir(
        parents=True,
        exist_ok=True,
    )
    queue = storage_managed_queue(settings)
    queue.storage_runtime.ensure_new_intake_allowed()
    provider = JarvisCdProvider(
        jarvis_bin=settings.jarvis_bin,
        agent_bin=settings.agent_bin,
        agent_adapter=settings.agent_adapter,
        agent_args=settings.agent_args,
    )
    provider.require_available()
    if settings.frps_addr is None or settings.frp_token is None:
        raise ConfigurationError("CLIO_RELAY_FRPS_ADDR and CLIO_RELAY_FRP_TOKEN are required")


def _bounded_output_event_chunks(text: str) -> list[str]:
    """Split persisted output into queue events with a strict UTF-8 byte bound."""
    if text == "":
        return []
    payload = text.encode("utf-8")
    chunks: list[str] = []
    offset = 0
    while offset < len(payload):
        end = min(offset + OUTPUT_EVENT_MAX_BYTES, len(payload))
        while end > offset:
            try:
                chunk = payload[offset:end].decode("utf-8")
            except UnicodeDecodeError as exc:
                end = offset + exc.start
                continue
            chunks.append(chunk)
            offset = end
            break
        else:
            raise RuntimeError("could not split valid UTF-8 output into bounded events")
    return chunks


def _file_summary(path: Path) -> dict[str, object]:
    storage_path = internal_filesystem_path(path)
    if not storage_path.exists():
        return {"path": str(path), "exists": False}
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": storage_path.stat().st_size,
        "sha256": hashlib.sha256(storage_path.read_bytes()).hexdigest(),
    }


def _extract_scheduler_job_id(line: str) -> str | None:
    explicit = re.search(r"\bscheduler_job_id=(?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if explicit is not None:
        return explicit.group("job_id")
    submitted = re.search(r"\bSubmitted batch job (?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if submitted is not None:
        return submitted.group("job_id")
    return None


def _trusted_jarvis_mcp_route(job: RelayJob) -> tuple[bool, str]:
    """Verify a durable job targets the configured artifact-bound JARVIS call."""
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        return False, "relay job is not an MCP call"
    try:
        configured_command = jarvis_mcp_command()
    except (ConfigurationError, ValueError) as exc:
        return False, f"configured JARVIS MCP command is invalid: {exc}"
    if [job.spec.server, *job.spec.server_args] != configured_command:
        return False, "MCP command does not match the configured JARVIS server"
    if job.spec.tool != "jarvis_run":
        return False, "MCP tool is not the owned jarvis_run operation"
    if job.spec.expected_jarvis_cd_lock_binding != jarvis_cd_lock_binding_expectation():
        return False, "MCP call did not enforce the relay JARVIS-CD lock pin"
    if job.spec.expected_server_artifact_digest is None:
        return False, "MCP call is not bound to its discovered server artifact"
    return True, "configured JARVIS MCP route and artifact binding matched"


def _trusted_jarvis_mcp_result(
    job: RelayJob,
    document: object,
) -> tuple[bool, str]:
    """Verify runtime identity came from the configured owned JARVIS MCP call."""
    route_valid, route_reason = _trusted_jarvis_mcp_route(job)
    if not route_valid:
        return False, route_reason
    assert isinstance(job.spec, McpCallSpec)
    if not isinstance(document, dict):
        return False, "MCP result artifact is not an object"
    typed = cast(dict[str, object], document)
    if typed.get("server") != job.spec.server or typed.get("server_args") != job.spec.server_args:
        return False, "MCP result command does not match the durable job spec"
    if typed.get("operation") != "tools/call" or typed.get("tool") != job.spec.tool:
        return False, "MCP result route does not match the durable job spec"
    if typed.get("arguments") != job.spec.arguments:
        return False, "MCP result arguments do not match the durable job spec"
    if typed.get("env_from") != job.spec.env_from:
        return False, "MCP result environment references do not match the durable job spec"
    if typed.get("expected_jarvis_cd_lock_binding") != job.spec.expected_jarvis_cd_lock_binding:
        return False, "MCP result JARVIS-CD lock pin does not match the durable job spec"
    if (
        typed.get("expected_server_artifact_digest") != job.spec.expected_server_artifact_digest
        or typed.get("observed_server_artifact_digest") != job.spec.expected_server_artifact_digest
    ):
        return False, "MCP result server artifact does not match the durable job spec"
    if not jarvis_mcp_server_artifact_binding_verified(
        typed.get("server_artifact"),
        expected_digest=job.spec.expected_server_artifact_digest,
    ):
        return False, "MCP result server artifact identity is not the exact relay release pin"
    if (
        typed.get("returncode") != 0
        or typed.get("timed_out") is True
        or typed.get("protocol_error") is not None
    ):
        return False, "MCP call did not complete successfully"
    if not isinstance(typed.get("structured_result"), dict) and not isinstance(
        typed.get("protocol_result"), dict
    ):
        return False, "MCP result has no persisted structured protocol result"
    protocol_result = typed.get("protocol_result")
    if (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    ):
        return False, "JARVIS MCP tool returned isError"
    return True, "configured JARVIS MCP command and durable result matched"


def _scheduler_status_is_not_found(status: SchedulerStatus) -> bool:
    """Recognize a provider's exact-job not-found terminal observation."""
    return status.phase is SchedulerPhase.UNKNOWN and status.record_found is False


_SCHEDULER_STATUS_TEXT_FIELDS = (
    "raw_state",
    "reason",
    "partition",
    "qos",
    "user",
    "memory",
    "submit_time",
    "eligible_time",
    "start_time",
    "elapsed",
    "time_limit",
    "queue_position_scope",
    "queue_position_note",
)


def _normalized_scheduler_status(
    status: SchedulerStatus,
    *,
    expected_scheduler: str,
    expected_scheduler_job_id: str,
) -> SchedulerStatus:
    """Bind provider status to the requested identity and bound all durable text."""
    if (
        status.scheduler != expected_scheduler
        or status.scheduler_job_id != expected_scheduler_job_id
    ):
        detail = bounded_error_detail(
            "scheduler provider returned mismatched identity: "
            f"expected scheduler={expected_scheduler!r} "
            f"job_id={expected_scheduler_job_id!r}; "
            f"observed scheduler={status.scheduler!r} job_id={status.scheduler_job_id!r}"
        )
        return SchedulerStatus(
            scheduler=expected_scheduler,
            scheduler_job_id=expected_scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            reason="scheduler provider response identity mismatch",
            queue_position_note=detail,
            observed_at=status.observed_at,
        )
    payload = status.model_dump(mode="python")
    for field_name in _SCHEDULER_STATUS_TEXT_FIELDS:
        value = payload.get(field_name)
        if isinstance(value, str):
            payload[field_name] = bounded_error_detail(value)
    return SchedulerStatus.model_validate(payload)


def _configured_scheduler_provider_name(provider: SchedulerProvider | None) -> str:
    raw_name = "external" if provider is None else provider.name
    normalized = raw_name.strip().lower().replace("_", "-")
    if normalized in {"none", "unmanaged"}:
        return "external"
    if not normalized:
        raise ConfigurationError("configured worker scheduler provider must be non-empty")
    return normalized


def _validate_scheduler_launch_provider(*, requested: str | None, configured: str) -> None:
    if requested is None:
        return
    normalized_requested = requested.strip().lower().replace("_", "-")
    if normalized_requested in {"none", "unmanaged"}:
        normalized_requested = "external"
    if not normalized_requested:
        raise ConfigurationError("JARVIS scheduler provider must be non-empty")
    if normalized_requested != configured:
        raise ConfigurationError(
            "JARVIS pipeline scheduler provider does not match the configured worker provider: "
            f"{normalized_requested} != {configured}; no JARVIS execution was launched"
        )
    if normalized_requested != "slurm":
        raise ConfigurationError(
            "clio-relay 1.0 supports scheduled JARVIS execution only through slurm; "
            f"requested {normalized_requested}; no JARVIS execution was launched"
        )


def _scheduler_name_from_job(job: RelayJob) -> str | None:
    if not isinstance(job.spec, JarvisRunSpec):
        return None
    if job.spec.pipeline_yaml is not None:
        return _scheduler_name_from_yaml(job.spec.pipeline_yaml)
    if job.spec.pipeline_path is not None:
        try:
            pipeline_yaml = internal_filesystem_path(Path(job.spec.pipeline_path)).read_text(
                encoding="utf-8"
            )
        except OSError:
            return None
        return _scheduler_name_from_yaml(pipeline_yaml)
    return None


def _jarvis_pipeline_name(job: RelayJob) -> str | None:
    if job.kind == JobKind.JARVIS and isinstance(job.spec, JarvisRunSpec):
        return job.spec.pipeline_name
    return None


def _scheduler_name_from_yaml(pipeline_yaml: str) -> str | None:
    try:
        loaded = yaml.safe_load(pipeline_yaml)
    except yaml.YAMLError:
        return None
    return _scheduler_name_from_document(loaded)


def _scheduler_name_from_document(document: object) -> str | None:
    if not isinstance(document, dict):
        return None
    typed = cast(dict[str, object], document)
    scheduler = typed.get("scheduler")
    if isinstance(scheduler, dict):
        typed_scheduler = cast(dict[str, object], scheduler)
        name = typed_scheduler.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    config = typed.get("config")
    if isinstance(config, dict):
        config_scheduler = _scheduler_name_from_document(cast(dict[str, object], config))
        if config_scheduler is not None:
            return config_scheduler
    experiments = typed.get("experiments")
    if isinstance(experiments, list):
        for experiment in cast(list[object], experiments):
            experiment_scheduler = _scheduler_name_from_document(experiment)
            if experiment_scheduler is not None:
                return experiment_scheduler
    return None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("numeric progress fields cannot be booleans")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str) and value != "":
        return float(value)
    raise ValueError("progress numeric field must be a number")


def _optional_metadata(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("progress metadata must be an object")
    typed = cast(dict[object, object], value)
    return {str(key): item for key, item in typed.items()}


def _progress_log_identity(stat: os.stat_result) -> tuple[int, int]:
    return stat.st_dev, stat.st_ino


def _normalize_package_progress_log_path(child_cwd: Path, path: Path) -> Path:
    expanded = path.expanduser()
    candidate = expanded if expanded.is_absolute() else child_cwd.absolute() / expanded
    return Path(os.path.abspath(candidate))


def _validated_native_subprocess_cwd(cwd: Path) -> Path:
    """Return a logical cwd or reject unverified native Windows path forms."""
    logical_cwd = logical_filesystem_path(cwd)
    if os.name != "nt":
        return logical_cwd
    absolute_cwd = os.path.abspath(logical_cwd)
    if absolute_cwd.startswith("\\\\"):
        raise ConfigurationError(
            "native JARVIS working directories on Windows must not use UNC paths"
        )
    if len(absolute_cwd) >= WINDOWS_LEGACY_PATH_HEADROOM:
        raise ConfigurationError(
            "native JARVIS working directory exceeds the verified Windows path bound"
        )
    return logical_cwd


def _render_progress_log_identity(identity: tuple[int, int] | None) -> str | None:
    if identity is None:
        return None
    return f"{identity[0]}:{identity[1]}"


def _open_package_progress_log(path: Path) -> BinaryIO | None:
    """Open one regular provider log without following symlinks or path races."""
    storage_path = internal_filesystem_path(path)
    try:
        path_stat = os.stat(storage_path, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConfigurationError(f"could not inspect package progress log {path}: {exc}") from exc
    if stat_module.S_ISLNK(path_stat.st_mode):
        raise ConfigurationError(f"package progress log symlinks are not allowed: {path}")
    if not stat_module.S_ISREG(path_stat.st_mode):
        raise ConfigurationError(f"package progress log is not a regular file: {path}")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(storage_path, flags)
    except OSError as exc:
        raise ConfigurationError(f"could not open package progress log {path}: {exc}") from exc
    try:
        opened_stat = os.fstat(descriptor)
        if not stat_module.S_ISREG(opened_stat.st_mode):
            raise ConfigurationError(f"package progress log is not a regular file: {path}")
        if _progress_log_identity(opened_stat) != _progress_log_identity(path_stat):
            raise ConfigurationError(f"package progress log changed while it was opened: {path}")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _precreate_runtime_sidecar(path: Path) -> _RuntimeSidecarAnchor:
    """Create an empty private runtime sidecar and pin its filesystem identity."""
    storage_path = internal_filesystem_path(path)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(storage_path, flags, 0o600)
    except OSError as exc:
        raise ConfigurationError(
            f"could not precreate runtime metadata sidecar {path}: {exc}"
        ) from exc
    keep_descriptor = False
    try:
        os.set_inheritable(descriptor, False)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        opened_stat = os.fstat(descriptor)
        anchor = _runtime_sidecar_anchor(
            opened_stat,
            descriptor=(descriptor if os.name != "nt" else None),
        )
        _validate_runtime_sidecar_stat(
            opened_stat,
            expected=anchor,
            label="runtime metadata sidecar",
        )
        keep_descriptor = os.name != "nt"
        return anchor
    finally:
        if not keep_descriptor:
            os.close(descriptor)


def _runtime_sidecar_anchor(
    file_stat: os.stat_result,
    *,
    descriptor: int | None = None,
) -> _RuntimeSidecarAnchor:
    return _RuntimeSidecarAnchor(
        device=int(file_stat.st_dev),
        inode=int(file_stat.st_ino),
        owner=int(file_stat.st_uid),
        link_count=int(file_stat.st_nlink),
        mode=stat_module.S_IMODE(file_stat.st_mode),
        descriptor=descriptor,
    )


def _runtime_sidecar_anchor_from_metadata(
    value: object,
    *,
    task_id: str,
) -> _RuntimeSidecarAnchor:
    """Restore one durable runtime-sidecar anchor without coercing its identity."""
    if not isinstance(value, dict):
        raise RelayError(f"runtime sidecar anchor is missing for task {task_id}")
    typed = cast(dict[str, object], value)
    fields = {"device", "inode", "owner", "link_count", "mode"}
    if set(typed) != fields or any(
        isinstance(typed[field], bool) or not isinstance(typed[field], int) for field in fields
    ):
        raise RelayError(f"runtime sidecar anchor is invalid for task {task_id}")
    return _RuntimeSidecarAnchor(
        device=cast(int, typed["device"]),
        inode=cast(int, typed["inode"]),
        owner=cast(int, typed["owner"]),
        link_count=cast(int, typed["link_count"]),
        mode=cast(int, typed["mode"]),
    )


def _validate_runtime_sidecar_stat(
    file_stat: os.stat_result,
    *,
    expected: _RuntimeSidecarAnchor,
    label: str,
) -> None:
    if not stat_module.S_ISREG(file_stat.st_mode):
        raise ConfigurationError(f"{label} is not a regular file")
    observed = _runtime_sidecar_anchor(file_stat)
    if observed != expected:
        raise ConfigurationError(f"{label} filesystem identity or permissions changed")
    if observed.link_count != 1:
        raise ConfigurationError(f"{label} must have exactly one hard link")
    if os.name != "nt":
        if observed.owner != os.getuid():
            raise ConfigurationError(f"{label} is not owned by the worker user")
        if observed.mode != 0o600:
            raise ConfigurationError(f"{label} mode must remain 0600")


def _open_owned_sidecar(
    path: Path,
    *,
    label: str,
    expected_anchor: _RuntimeSidecarAnchor | None = None,
) -> BinaryIO | None:
    """Open a regular relay sidecar without following symlinks or path races."""
    storage_path = internal_filesystem_path(path)
    if expected_anchor is not None and expected_anchor.descriptor is not None:
        _validate_runtime_sidecar_stat(
            os.fstat(expected_anchor.descriptor),
            expected=expected_anchor,
            label=label,
        )
    try:
        path_stat = os.stat(storage_path, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConfigurationError(f"could not inspect {label} {path}: {exc}") from exc
    if stat_module.S_ISLNK(path_stat.st_mode):
        raise ConfigurationError(f"{label} symlinks are not allowed: {path}")
    if not stat_module.S_ISREG(path_stat.st_mode):
        raise ConfigurationError(f"{label} is not a regular file: {path}")
    if expected_anchor is not None:
        _validate_runtime_sidecar_stat(path_stat, expected=expected_anchor, label=label)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(storage_path, flags)
    except OSError as exc:
        raise ConfigurationError(f"could not open {label} {path}: {exc}") from exc
    try:
        os.set_inheritable(descriptor, False)
        opened_stat = os.fstat(descriptor)
        if not stat_module.S_ISREG(opened_stat.st_mode):
            raise ConfigurationError(f"{label} is not a regular file: {path}")
        if _progress_log_identity(opened_stat) != _progress_log_identity(path_stat):
            raise ConfigurationError(f"{label} changed while it was opened: {path}")
        if expected_anchor is not None:
            _validate_runtime_sidecar_stat(opened_stat, expected=expected_anchor, label=label)
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _execution_sidecar_quarantine_name(anchor: _RuntimeSidecarAnchor) -> str:
    """Return a bounded deterministic retention name for one exact sidecar inode."""
    digest = hashlib.sha256(
        json.dumps(
            anchor.as_metadata(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()
    token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    # Keep the target no longer than the shortest generated runtime sidecar
    # name. This preserves all 256 identity bits while avoiding a rename that
    # crosses the legacy Windows MAX_PATH boundary after the source was
    # successfully created in the same spool directory.
    return f".q1-{token}"


def _execution_sidecar_cleanup_plan(
    path: Path,
    anchor: _RuntimeSidecarAnchor,
) -> dict[str, object]:
    """Build durable, deterministic quarantine state before execution release."""
    return {
        "schema_version": EXECUTION_SIDECAR_CLEANUP_SCHEMA,
        "quarantine_schema_version": EXECUTION_SIDECAR_QUARANTINE_SCHEMA,
        "source_name": path.name,
        "quarantine_name": _execution_sidecar_quarantine_name(anchor),
        "anchor": anchor.as_metadata(),
        "stage": "prepared",
    }


def _execution_cleanup_quarantine_paths(
    task: RelayTask,
    *,
    paths: list[Path],
    expected_anchors: dict[Path, _RuntimeSidecarAnchor],
) -> dict[Path, Path]:
    """Restore and validate exact quarantine targets from durable task state."""
    raw_cleanup = task.metadata.get("execution_cleanup")
    cleanup = cast(dict[str, object], raw_cleanup) if isinstance(raw_cleanup, dict) else {}
    raw_states = cleanup.get("sidecars")
    if not isinstance(raw_states, dict) or not raw_states:
        raise RelayError(f"execution cleanup has no staged sidecars for task {task.task_id}")
    states = cast(dict[str, object], raw_states)
    targets: dict[Path, Path] = {}
    for path in paths:
        matching_states = [
            cast(dict[str, object], value)
            for value in states.values()
            if isinstance(value, dict)
            and cast(dict[object, object], value).get("source_name") == path.name
        ]
        anchor = expected_anchors.get(path)
        if len(matching_states) != 1:
            raise RelayError(
                f"execution cleanup does not uniquely own sidecar for task {task.task_id}: "
                f"{path.name}"
            )
        state = matching_states[0]
        quarantine_name = state.get("quarantine_name")
        if (
            state.get("schema_version") != EXECUTION_SIDECAR_CLEANUP_SCHEMA
            or state.get("quarantine_schema_version") != EXECUTION_SIDECAR_QUARANTINE_SCHEMA
            or not isinstance(quarantine_name, str)
            or Path(quarantine_name).name != quarantine_name
        ):
            raise RelayError(f"execution cleanup sidecar state is invalid for task {task.task_id}")
        recorded_anchor = _runtime_sidecar_anchor_from_metadata(
            state.get("anchor"),
            task_id=task.task_id,
        )
        if anchor is None:
            raise RelayError(
                f"execution sidecar anchor is missing for task {task.task_id}: {path.name}"
            )
        if recorded_anchor != anchor:
            raise RelayError(f"execution cleanup sidecar anchor conflicts for task {task.task_id}")
        expected_name = _execution_sidecar_quarantine_name(anchor)
        if quarantine_name != expected_name:
            raise RelayError(
                f"execution cleanup quarantine identity conflicts for task {task.task_id}"
            )
        targets[path] = path.parent / _execution_sidecar_quarantine_name(anchor)
    return targets


def _execution_cleanup_ack_metadata(
    task: RelayTask,
    quarantined: dict[Path, Path],
) -> dict[str, object]:
    """Build canonical cleanup evidence written before the retry marker is removed."""
    now = utc_now().isoformat()
    raw_cleanup = task.metadata.get("execution_cleanup")
    cleanup = dict(cast(dict[str, object], raw_cleanup)) if isinstance(raw_cleanup, dict) else {}
    raw_states = cleanup.get("sidecars")
    states = dict(cast(dict[str, object], raw_states)) if isinstance(raw_states, dict) else {}
    for source, quarantine in quarantined.items():
        matching_roles = [
            role
            for role, value in states.items()
            if isinstance(value, dict)
            and cast(dict[object, object], value).get("source_name") == source.name
        ]
        if not matching_roles:
            continue
        if len(matching_roles) != 1:
            raise RelayError(
                f"execution cleanup contains duplicate sidecar state for task {task.task_id}"
            )
        role = matching_roles[0]
        state = cast(dict[str, object], states[role])
        if state.get("quarantine_name") != quarantine.name:
            raise RelayError(f"execution cleanup quarantine did not match for task {task.task_id}")
        states[role] = {
            **state,
            "stage": "quarantined",
            "quarantined_at": state.get("quarantined_at", now),
        }
    if cleanup:
        cleanup.update(
            {
                "acknowledgment_stage": "acknowledged",
                "acknowledged_at": now,
            }
        )
        if states:
            cleanup["sidecars"] = states
    evidence = {
        source.name: quarantine.name
        for source, quarantine in sorted(quarantined.items(), key=lambda item: item[0].name)
    }
    return {
        "execution_cleanup": cleanup,
        "execution_sidecars_quarantined": True,
        "execution_sidecars_quarantined_at": now,
        "execution_sidecar_quarantines": {
            "schema_version": EXECUTION_SIDECAR_QUARANTINE_SCHEMA,
            "entries": evidence,
        },
        # Compatibility for v0.9 readers: the active sidecar names are gone,
        # while exact quarantine evidence remains with the whole job spool.
        "execution_sidecars_removed": True,
        "execution_sidecars_removed_at": now,
    }


def _rename_noreplace_at(
    directory_fd: int,
    source_name: str,
    quarantine_name: str,
) -> None:
    """Atomically rename inside one Linux directory without replacing a target."""
    if not sys.platform.startswith("linux"):
        raise ConfigurationError(
            "secure execution sidecar quarantine requires Linux renameat2(RENAME_NOREPLACE)"
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ConfigurationError(
            "secure execution sidecar quarantine requires renameat2(RENAME_NOREPLACE)"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        directory_fd,
        os.fsencode(source_name),
        directory_fd,
        os.fsencode(quarantine_name),
        1,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise FileExistsError(error, os.strerror(error), quarantine_name)
    raise OSError(error, os.strerror(error), source_name)


def _remove_execution_sidecars(
    paths: list[Path],
    *,
    spool_path: Path,
    expected_anchors: dict[Path, _RuntimeSidecarAnchor] | None = None,
    expected_quarantines: dict[Path, Path] | None = None,
    on_quarantined: Callable[[Path, Path], None] | None = None,
) -> dict[Path, Path]:
    """Atomically quarantine exact sidecar inodes and retain durable evidence."""
    anchors = expected_anchors or {}
    quarantines = expected_quarantines or {}
    storage_spool_path = internal_filesystem_path(spool_path)
    try:
        if any(path.parent != spool_path for path in paths):
            raise ConfigurationError("execution sidecar path escaped its job spool")
        missing_anchors = [path for path in paths if path not in anchors]
        if missing_anchors:
            raise ConfigurationError(
                "execution sidecar cleanup requires durable anchors: "
                + ", ".join(path.name for path in missing_anchors)
            )
        if any(
            source not in paths or target.parent != spool_path or target == source
            for source, target in quarantines.items()
        ):
            raise ConfigurationError("execution sidecar quarantine path escaped its job spool")
        try:
            spool_stat = os.stat(storage_spool_path, follow_symlinks=False)
        except FileNotFoundError as exc:
            if anchors:
                raise ConfigurationError(
                    f"anchored execution spool disappeared before cleanup: {spool_path}"
                ) from exc
            return {}
        except OSError as exc:
            raise ConfigurationError(
                f"could not inspect execution spool {spool_path}: {exc}"
            ) from exc
        if not stat_module.S_ISDIR(spool_stat.st_mode) or stat_module.S_ISLNK(spool_stat.st_mode):
            raise ConfigurationError(f"execution spool is not an owned directory: {spool_path}")
        for anchor in anchors.values():
            if anchor.descriptor is not None:
                _validate_runtime_sidecar_stat(
                    os.fstat(anchor.descriptor),
                    expected=anchor,
                    label="execution sidecar",
                )
        if os.name == "nt":
            result = _remove_execution_sidecars_windows(
                paths,
                spool_path=spool_path,
                expected_spool_identity=_progress_log_identity(spool_stat),
                expected_anchors=anchors,
                expected_quarantines=quarantines,
            )
            for source, quarantine in result.items():
                if on_quarantined is not None:
                    on_quarantined(source, quarantine)
            return result
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            directory_fd = os.open(storage_spool_path, flags)
        except OSError as exc:
            raise ConfigurationError(
                f"could not anchor execution spool {spool_path}: {exc}"
            ) from exc
        try:
            opened_stat = os.fstat(directory_fd)
            if _progress_log_identity(opened_stat) != _progress_log_identity(spool_stat):
                raise ConfigurationError(f"execution spool changed while opened: {spool_path}")
            result: dict[Path, Path] = {}
            for path in paths:
                anchor = anchors.get(path)
                quarantine = quarantines.get(path)
                if quarantine is None and anchor is not None:
                    quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
                if quarantine is not None and quarantine.parent != spool_path:
                    raise ConfigurationError(
                        f"execution sidecar quarantine escaped its spool: {quarantine}"
                    )
                if quarantine is not None and quarantine.name == path.name:
                    raise ConfigurationError(
                        f"execution sidecar quarantine aliases its source: {path}"
                    )
                try:
                    quarantine_stat = (
                        None
                        if quarantine is None
                        else os.stat(
                            quarantine.name,
                            dir_fd=directory_fd,
                            follow_symlinks=False,
                        )
                    )
                except FileNotFoundError:
                    quarantine_stat = None
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not inspect execution sidecar quarantine {quarantine}: {exc}"
                    ) from exc
                try:
                    entry_stat = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    entry_stat = None
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not inspect execution sidecar {path}: {exc}"
                    ) from exc
                if quarantine_stat is not None:
                    if anchor is None:
                        raise ConfigurationError(
                            f"execution sidecar quarantine has no durable anchor: {quarantine}"
                        )
                    _validate_runtime_sidecar_stat(
                        quarantine_stat,
                        expected=anchor,
                        label="execution sidecar quarantine",
                    )
                    if entry_stat is not None:
                        raise ConfigurationError(
                            f"execution sidecar source was replaced after quarantine: {path}"
                        )
                    result[path] = cast(Path, quarantine)
                    if on_quarantined is not None:
                        on_quarantined(path, cast(Path, quarantine))
                    continue
                if entry_stat is None:
                    raise ConfigurationError(
                        f"anchored execution sidecar and quarantine disappeared: {path}"
                    )
                if stat_module.S_ISDIR(entry_stat.st_mode):
                    raise ConfigurationError(f"execution sidecar became a directory: {path}")
                if anchor is None:
                    raise ConfigurationError(f"execution sidecar has no durable anchor: {path}")
                _validate_runtime_sidecar_stat(
                    entry_stat,
                    expected=anchor,
                    label="execution sidecar",
                )
                if quarantine is None:
                    quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
                if quarantine.parent != spool_path or quarantine.name == path.name:
                    raise ConfigurationError(
                        f"invalid execution sidecar quarantine target: {quarantine}"
                    )
                with suppress(FileExistsError):
                    _rename_noreplace_at(directory_fd, path.name, quarantine.name)
                try:
                    quarantined_stat = os.stat(
                        quarantine.name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not verify execution sidecar quarantine {quarantine}: {exc}"
                    ) from exc
                _validate_runtime_sidecar_stat(
                    quarantined_stat,
                    expected=anchor,
                    label="execution sidecar quarantine",
                )
                try:
                    replacement_stat = os.stat(
                        path.name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    replacement_stat = None
                if replacement_stat is not None:
                    raise ConfigurationError(
                        f"execution sidecar source was replaced during quarantine: {path}"
                    )
                os.fsync(directory_fd)
                result[path] = quarantine
                if on_quarantined is not None:
                    on_quarantined(path, quarantine)
            return result
        finally:
            os.close(directory_fd)
    finally:
        _close_runtime_sidecar_anchors(anchors)


def _close_runtime_sidecar_anchors(
    anchors: dict[Path, _RuntimeSidecarAnchor] | None,
) -> None:
    for anchor in (anchors or {}).values():
        if anchor.descriptor is None:
            continue
        with suppress(OSError):
            os.close(anchor.descriptor)


def _remove_execution_sidecars_windows(
    paths: list[Path],
    *,
    spool_path: Path,
    expected_spool_identity: tuple[int, int],
    expected_anchors: dict[Path, _RuntimeSidecarAnchor] | None = None,
    expected_quarantines: dict[Path, Path] | None = None,
) -> dict[Path, Path]:
    """Quarantine exact Windows file handles while the parent cannot be replaced."""
    anchors = expected_anchors or {}
    quarantines = expected_quarantines or {}
    directory_handle = _open_windows_cleanup_handle(
        spool_path,
        desired_access=_WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=False,
    )
    if directory_handle is None:
        raise ConfigurationError(f"execution spool disappeared during cleanup: {spool_path}")
    try:
        attributes, file_id = _windows_handle_information(directory_handle, spool_path)
        if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
            raise ConfigurationError(f"execution spool became a reparse point: {spool_path}")
        if not attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY:
            raise ConfigurationError(f"execution spool is not a directory: {spool_path}")
        expected_file_id = expected_spool_identity[1]
        if expected_file_id and file_id != expected_file_id:
            raise ConfigurationError(f"execution spool changed while opened: {spool_path}")
        result: dict[Path, Path] = {}
        for path in paths:
            anchor = anchors.get(path)
            if anchor is None:
                raise ConfigurationError(f"execution sidecar has no durable anchor: {path}")
            quarantine = quarantines.get(path)
            if quarantine is None:
                quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
            if quarantine.parent != spool_path or quarantine.name == path.name:
                raise ConfigurationError(
                    f"invalid execution sidecar quarantine target: {quarantine}"
                )
            _quarantine_windows_sidecar_by_handle(
                path,
                quarantine=quarantine,
                anchored_directory_handle=directory_handle,
                expected_anchor=anchor,
            )
            result[path] = quarantine
        return result
    finally:
        _close_windows_cleanup_handle(directory_handle)


def _quarantine_windows_sidecar_by_handle(
    path: Path,
    *,
    quarantine: Path,
    anchored_directory_handle: int,
    expected_anchor: _RuntimeSidecarAnchor,
) -> None:
    """Rename one exact open sidecar handle to a no-replace quarantine target."""
    _windows_handle_information(anchored_directory_handle, path.parent)
    existing_quarantine = _open_windows_cleanup_handle(
        quarantine,
        desired_access=_WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=True,
    )
    if existing_quarantine is not None:
        try:
            _validate_windows_sidecar_handle(
                existing_quarantine,
                quarantine,
                expected_anchor=expected_anchor,
            )
            if os.path.lexists(internal_filesystem_path(path)):
                raise ConfigurationError(
                    f"execution sidecar source was replaced after quarantine: {path}"
                )
            return
        finally:
            _close_windows_cleanup_handle(existing_quarantine)
    file_handle = _open_windows_cleanup_handle(
        path,
        desired_access=_WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=True,
    )
    if file_handle is None:
        raise ConfigurationError(f"anchored execution sidecar and quarantine disappeared: {path}")
    try:
        _validate_windows_sidecar_handle(
            file_handle,
            path,
            expected_anchor=expected_anchor,
        )
        with suppress(FileExistsError):
            _mark_windows_handle_for_rename(file_handle, path, quarantine)
    finally:
        _close_windows_cleanup_handle(file_handle)
    if os.path.lexists(internal_filesystem_path(path)):
        raise ConfigurationError(f"execution sidecar source was replaced during quarantine: {path}")
    quarantine_handle = _open_windows_cleanup_handle(
        quarantine,
        desired_access=_WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=False,
    )
    if quarantine_handle is None:
        raise ConfigurationError(f"execution sidecar quarantine disappeared: {quarantine}")
    try:
        _validate_windows_sidecar_handle(
            quarantine_handle,
            quarantine,
            expected_anchor=expected_anchor,
        )
    finally:
        _close_windows_cleanup_handle(quarantine_handle)


def _validate_windows_sidecar_handle(
    handle: int,
    path: Path,
    *,
    expected_anchor: _RuntimeSidecarAnchor,
) -> None:
    """Validate a non-reparse Windows handle against its pre-release inode."""
    attributes, file_id = _windows_handle_information(handle, path)
    if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
        raise ConfigurationError(f"execution sidecar became a reparse point: {path}")
    if attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY:
        raise ConfigurationError(f"execution sidecar became a directory: {path}")
    if file_id != expected_anchor.inode:
        raise ConfigurationError(f"execution sidecar file identity changed: {path}")
    try:
        file_stat = os.stat(internal_filesystem_path(path), follow_symlinks=False)
    except OSError as exc:
        raise ConfigurationError(f"could not inspect execution sidecar {path}: {exc}") from exc
    _validate_runtime_sidecar_stat(
        file_stat,
        expected=expected_anchor,
        label="execution sidecar",
    )


_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_READ_ATTRIBUTES = 0x00000080
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_FILE_SHARE_WRITE = 0x00000002
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_RENAME_INFO = 3
_WINDOWS_ERROR_FILE_NOT_FOUND = 2
_WINDOWS_ERROR_PATH_NOT_FOUND = 3
_WINDOWS_ERROR_FILE_EXISTS = 80
_WINDOWS_ERROR_ALREADY_EXISTS = 183


def _open_windows_cleanup_handle(
    path: Path,
    *,
    desired_access: int,
    share_mode: int,
    flags: int,
    missing_ok: bool,
) -> int | None:
    """Open a Windows path without allowing delete sharing or reparse traversal."""
    if os.name != "nt":
        raise RuntimeError("Windows cleanup handles require Windows")
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    raw_handle = kernel32.CreateFileW(
        str(internal_filesystem_path(path)),
        desired_access,
        share_mode,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if raw_handle == invalid_handle:
        error = ctypes.get_last_error()
        if missing_ok and error in {
            _WINDOWS_ERROR_FILE_NOT_FOUND,
            _WINDOWS_ERROR_PATH_NOT_FOUND,
        }:
            return None
        raise ConfigurationError(
            f"could not open execution cleanup path {path}: Windows error {error}"
        )
    return int(raw_handle)


def _windows_handle_information(handle: int, path: Path) -> tuple[int, int]:
    """Return attributes and stable file identity for an already-open Windows handle."""
    if os.name != "nt":
        raise RuntimeError("Windows cleanup handle inspection requires Windows")
    from ctypes import wintypes

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("creation_time", wintypes.FILETIME),
            ("last_access_time", wintypes.FILETIME),
            ("last_write_time", wintypes.FILETIME),
            ("volume_serial_number", wintypes.DWORD),
            ("file_size_high", wintypes.DWORD),
            ("file_size_low", wintypes.DWORD),
            ("number_of_links", wintypes.DWORD),
            ("file_index_high", wintypes.DWORD),
            ("file_index_low", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    information = _ByHandleFileInformation()
    if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(information)):
        error = ctypes.get_last_error()
        raise ConfigurationError(
            f"could not inspect execution cleanup handle {path}: Windows error {error}"
        )
    file_id = (int(information.file_index_high) << 32) | int(information.file_index_low)
    return int(information.file_attributes), file_id


def _mark_windows_handle_for_rename(
    handle: int,
    source: Path,
    quarantine: Path,
) -> None:
    """Apply no-replace FileRenameInfo to an exact open sidecar handle."""
    if os.name != "nt":
        raise RuntimeError("Windows handle rename requires Windows")
    from ctypes import wintypes

    quarantine_text = str(internal_filesystem_path(quarantine))
    quarantine_bytes = quarantine_text.encode("utf-16-le")
    if not quarantine_bytes or "\x00" in quarantine_text:
        raise ConfigurationError(f"invalid execution sidecar quarantine name: {quarantine}")

    class _FileRenameInformationLayout(ctypes.Structure):
        _fields_ = [
            ("replace_if_exists", wintypes.BOOLEAN),
            ("root_directory", wintypes.HANDLE),
            ("file_name_length", wintypes.DWORD),
            ("file_name", wintypes.WCHAR * 1),
        ]

    # FILE_RENAME_INFORMATION ends in a flexible WCHAR array. Windows accepts
    # FileNameLength as the exact non-NUL UTF-16 payload length, but the input
    # buffer must still include storage for the terminating WCHAR. Build the
    # buffer from the field offset so ctypes structure tail padding cannot be
    # interpreted as part of the destination name.
    file_name_offset = _FileRenameInformationLayout.file_name.offset
    buffer_size = file_name_offset + len(quarantine_bytes) + ctypes.sizeof(wintypes.WCHAR)
    rename_buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_address = ctypes.addressof(rename_buffer)
    replace_if_exists = wintypes.BOOLEAN(False)
    root_directory = wintypes.HANDLE()
    file_name_length = wintypes.DWORD(len(quarantine_bytes))
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.replace_if_exists.offset,
        ctypes.byref(replace_if_exists),
        ctypes.sizeof(replace_if_exists),
    )
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.root_directory.offset,
        ctypes.byref(root_directory),
        ctypes.sizeof(root_directory),
    )
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.file_name_length.offset,
        ctypes.byref(file_name_length),
        ctypes.sizeof(file_name_length),
    )
    ctypes.memmove(
        buffer_address + file_name_offset,
        quarantine_bytes,
        len(quarantine_bytes),
    )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.SetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel32.SetFileInformationByHandle.restype = wintypes.BOOL
    if not kernel32.SetFileInformationByHandle(
        handle,
        _WINDOWS_FILE_RENAME_INFO,
        rename_buffer,
        buffer_size,
    ):
        error = ctypes.get_last_error()
        if error in {_WINDOWS_ERROR_FILE_EXISTS, _WINDOWS_ERROR_ALREADY_EXISTS}:
            raise FileExistsError(error, f"execution sidecar quarantine exists: {quarantine}")
        raise ConfigurationError(
            f"could not quarantine execution sidecar {source}: Windows error {error}"
        )


def _close_windows_cleanup_handle(handle: int) -> None:
    """Close a Windows cleanup handle without masking an earlier cleanup failure."""
    if os.name != "nt":
        raise RuntimeError("Windows handle cleanup requires Windows")
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.CloseHandle(handle)


def _read_bounded_sidecar_record(
    handle: BinaryIO,
    *,
    max_bytes: int,
    allow_final_record: bool,
) -> tuple[bytes | None, str]:
    """Read one bounded JSONL record and preserve incomplete writer output."""
    record_start = handle.tell()
    line = handle.readline(max_bytes + 1)
    if line == b"":
        return None, "eof"
    if len(line) > max_bytes:
        while not line.endswith(b"\n"):
            fragment = handle.readline(SIDECAR_DRAIN_CHUNK_BYTES)
            if fragment == b"" or fragment.endswith(b"\n"):
                break
        return None, "oversized"
    if not line.endswith(b"\n") and not allow_final_record:
        handle.seek(record_start)
        return None, "incomplete"
    return line, "record"


def _progress_log_checkpoint(
    handle: BinaryIO,
    offset: int,
    *,
    path: Path,
) -> tuple[int, str | None]:
    if offset <= 0:
        return 0, None
    checkpoint_offset = max(0, offset - 4096)
    expected_length = offset - checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not checkpoint package progress log {path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    if len(payload) != expected_length:
        raise ConfigurationError(f"package progress log changed while it was checkpointed: {path}")
    return checkpoint_offset, hashlib.sha256(payload).hexdigest()


def _progress_log_checkpoint_matches(
    state: _PackageProgressLogState,
    handle: BinaryIO,
) -> bool:
    if state.checkpoint_sha256 is None:
        return True
    expected_length = state.offset - state.checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(state.checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not verify package progress log checkpoint {state.path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    return (
        len(payload) == expected_length
        and hashlib.sha256(payload).hexdigest() == state.checkpoint_sha256
    )


def _progress_from_sidecar_record(
    record: object,
    *,
    expected_key: str,
    expected_sequence: int,
) -> dict[str, object]:
    """Verify one ordered HMAC-authenticated package-progress observation."""
    if not isinstance(record, dict):
        raise ValueError("progress sidecar record must be an object")
    typed = cast(dict[str, object], record)
    if set(typed) != {"schema_version", "sequence", "progress", "progress_hmac"}:
        raise ValueError("progress sidecar record fields did not match")
    if typed.get("schema_version") != PROGRESS_SIDECAR_RECORD_SCHEMA:
        raise ValueError("progress sidecar record schema did not match")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or sequence != expected_sequence:
        raise ValueError("progress sidecar sequence did not match")
    progress = typed.get("progress")
    if not isinstance(progress, dict):
        raise ValueError("progress sidecar omitted its progress object")
    typed_progress = {
        str(key): value for key, value in cast(dict[object, object], progress).items()
    }
    observed_hmac = typed.get("progress_hmac")
    if not isinstance(observed_hmac, str) or len(observed_hmac) != 64:
        raise ValueError("progress sidecar HMAC was invalid")
    signed = {
        "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
        "sequence": expected_sequence,
        "progress": typed_progress,
    }
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    expected_hmac = hmac.new(
        expected_key.encode("utf-8"),
        canonical,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(observed_hmac, expected_hmac):
        raise ValueError("progress sidecar HMAC did not match")
    return typed_progress


def _trusted_provider_metadata(
    metadata: dict[str, object],
    *,
    job_id: str,
    provider: PackageProgressProvider,
    source_authority: PackageProgressSourceAuthority,
    acceptance_validated: bool,
) -> dict[str, object]:
    """Stamp a provider candidate without trusting plugin-supplied provenance."""
    identity = provider.identity
    return package_progress_provider_metadata(
        metadata,
        package_name=identity.package_name,
        package_version=identity.package_version,
        run_id=job_id,
        adapter_name=identity.adapter_name,
        provider_entry_point=identity.entry_point_name,
        provider_entry_point_value=identity.entry_point_value,
        provider_distribution=identity.distribution_name,
        provider_distribution_version=identity.distribution_version,
        source_authority=source_authority,
        application_profile=identity.application_profile,
        provider_validated=True,
        acceptance_validated=acceptance_validated,
    )


def _trusted_mcp_progress_metadata(
    job: RelayJob,
    metadata: dict[str, object],
) -> dict[str, object]:
    """Cross-check one runner-bridged JARVIS provider notification."""
    route_valid, route_reason = _trusted_jarvis_mcp_route(job)
    if not route_valid:
        raise ConfigurationError(f"MCP package progress route was not trusted: {route_reason}")
    assert isinstance(job.spec, McpCallSpec)
    raw_bridge = metadata.get("mcp_progress_bridge")
    if not isinstance(raw_bridge, dict):
        raise ConfigurationError("MCP package progress bridge metadata is missing")
    bridge = {str(key): value for key, value in cast(dict[object, object], raw_bridge).items()}
    required_bridge_fields = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "notification_sequence",
        "source_authority",
        "provider",
        "provider_acceptance_validated",
        "expected_server_artifact_digest",
        "observed_server_artifact_digest",
        "execution_validated",
    }
    if set(bridge) != required_bridge_fields or bridge.get("schema_version") != (
        MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA
    ):
        raise ConfigurationError("MCP package progress bridge schema is invalid")
    execution_id = bridge.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id or len(execution_id) > 4096:
        raise ConfigurationError("MCP package progress execution id is invalid")
    pipeline_id = bridge.get("pipeline_id")
    expected_pipeline_id = job.spec.arguments.get("pipeline_id")
    if not isinstance(expected_pipeline_id, str) or pipeline_id != expected_pipeline_id:
        raise ConfigurationError("MCP package progress pipeline id did not match the job")
    sequence = bridge.get("notification_sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ConfigurationError("MCP package progress notification sequence is invalid")
    transport_source = bridge.get("source_authority")
    if transport_source not in {"package_log", "jarvis_stdout_fallback"}:
        raise ConfigurationError("MCP package progress source authority is invalid")
    expected_digest = job.spec.expected_server_artifact_digest
    if (
        expected_digest is None
        or bridge.get("expected_server_artifact_digest") != expected_digest
        or bridge.get("observed_server_artifact_digest") != expected_digest
    ):
        raise ConfigurationError("MCP package progress server artifact did not match discovery")
    execution_validated = bridge.get("execution_validated")
    provider_acceptance = bridge.get("provider_acceptance_validated")
    if not isinstance(execution_validated, bool) or not isinstance(provider_acceptance, bool):
        raise ConfigurationError("MCP package progress validation flags must be boolean")
    raw_provider = bridge.get("provider")
    if not isinstance(raw_provider, dict):
        raise ConfigurationError("MCP package progress provider identity is missing")
    provider_metadata = {
        str(key): value for key, value in cast(dict[object, object], raw_provider).items()
    }
    required_provider_fields = {
        "entry_point",
        "entry_point_value",
        "distribution",
        "distribution_version",
        "adapter",
        "package_name",
        "package_version",
    }
    if not required_provider_fields.issubset(provider_metadata) or not set(
        provider_metadata
    ).issubset(required_provider_fields | {"application_profile"}):
        raise ConfigurationError("MCP package progress provider identity is incomplete")
    for field_name in required_provider_fields:
        value = provider_metadata.get(field_name)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(
                f"MCP package progress provider {field_name} must be a non-empty string"
            )
    package_name = cast(str, provider_metadata["package_name"])
    adapter_name = cast(str, provider_metadata["adapter"])
    provider_document = yaml.safe_dump(
        {
            "pkgs": [
                {
                    "pkg_type": package_name,
                    "progress": {"adapter": adapter_name},
                }
            ]
        },
        sort_keys=True,
    )
    local_provider = package_progress_adapter_from_pipeline(provider_document)
    if local_provider is None:
        raise ConfigurationError("MCP package progress provider is not installed locally")
    identity = local_provider.identity
    identity_matches = (
        provider_metadata.get("entry_point") == identity.entry_point_name
        and provider_metadata.get("entry_point_value") == identity.entry_point_value
        and _normalized_provider_distribution(str(provider_metadata["distribution"]))
        == _normalized_provider_distribution(identity.distribution_name)
        and provider_metadata.get("distribution_version") == identity.distribution_version
        and provider_metadata.get("adapter") == identity.adapter_name
        and provider_metadata.get("package_name") == identity.package_name
        and provider_metadata.get("package_version") == identity.package_version
        and provider_metadata.get("application_profile") == identity.application_profile
    )
    if not identity_matches:
        raise ConfigurationError("MCP package progress provider identity did not match the worker")
    candidate_metadata = dict(metadata)
    candidate_metadata.pop("mcp_progress_bridge", None)
    preliminary = _trusted_provider_metadata(
        candidate_metadata,
        job_id=job.job_id,
        provider=local_provider,
        source_authority=PackageProgressSourceAuthority.MCP_PROGRESS_NOTIFICATION,
        acceptance_validated=False,
    )
    try:
        locally_accepted = (
            local_provider.acceptance_progress_valid(cast(dict[str, Any], preliminary)) is True
        )
    except Exception as exc:
        raise ConfigurationError(
            f"MCP package progress worker acceptance predicate failed: {type(exc).__name__}: {exc}"
        ) from exc
    if locally_accepted is not provider_acceptance:
        raise ConfigurationError(
            "MCP package progress provider acceptance did not match the worker predicate"
        )
    trusted = _trusted_provider_metadata(
        candidate_metadata,
        job_id=job.job_id,
        provider=local_provider,
        source_authority=PackageProgressSourceAuthority.MCP_PROGRESS_NOTIFICATION,
        acceptance_validated=execution_validated and locally_accepted,
    )
    trusted.update(
        {
            "provider_execution_id": execution_id,
            "provider_pipeline_id": pipeline_id,
            "provider_server_artifact_digest": expected_digest,
            "provider_notification_sequence": sequence,
            "provider_transport_source_authority": transport_source,
            "provider_execution_validated": execution_validated,
        }
    )
    return trusted


def _trusted_native_mcp_progress_metadata(
    job: RelayJob,
    metadata: dict[str, object],
) -> dict[str, object]:
    """Validate an HMAC-protected native JARVIS progress observation."""
    raw_bridge = metadata.get("mcp_native_progress_bridge")
    if not isinstance(raw_bridge, dict):
        raise ConfigurationError("native MCP JARVIS progress bridge metadata is missing")
    bridge = cast(dict[str, object], raw_bridge)
    expected_fields = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "execution_state",
        "terminal",
        "transport_sequence",
        "package_name",
        "package_id",
        "event_count",
        "event_schema_version",
        "event_sequence",
        "event_state",
        "observed_at_epoch",
        "determinate",
        "skipped_event_count",
        "expected_server_artifact_digest",
        "observed_server_artifact_digest",
        "execution_validated",
    }
    if (
        set(bridge) != expected_fields
        or bridge.get("schema_version") != MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA
        or bridge.get("event_schema_version") != "jarvis.progress.v1"
    ):
        raise ConfigurationError("native MCP JARVIS progress bridge schema did not match")
    route_valid, route_reason = _trusted_jarvis_mcp_route(job)
    if not route_valid:
        raise ConfigurationError(
            f"native MCP JARVIS progress route was not trusted: {route_reason}"
        )
    assert isinstance(job.spec, McpCallSpec)
    expected_digest = job.spec.expected_server_artifact_digest
    if (
        expected_digest is None
        or bridge.get("expected_server_artifact_digest") != expected_digest
        or bridge.get("observed_server_artifact_digest") != expected_digest
    ):
        raise ConfigurationError("native MCP JARVIS progress server artifact did not match")
    arguments = job.spec.arguments
    pipeline_id = bridge.get("pipeline_id")
    if (
        not isinstance(pipeline_id, str)
        or not pipeline_id
        or arguments.get("pipeline_id") != pipeline_id
    ):
        raise ConfigurationError("native MCP JARVIS progress pipeline identity did not match")
    string_fields = (
        "execution_id",
        "execution_state",
        "package_name",
        "package_id",
        "event_state",
    )
    for field_name in string_fields:
        value = bridge.get(field_name)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} must be non-empty")
    if bridge["execution_state"] not in {
        "preparing",
        "scripted",
        "submitting",
        "submitted",
        "running",
        "completed",
        "failed",
        "canceled",
        "unknown",
    }:
        raise ConfigurationError("native MCP JARVIS progress execution state was invalid")
    if bridge["event_state"] not in {
        "pending",
        "starting",
        "running",
        "ready",
        "completed",
        "failed",
        "canceled",
    }:
        raise ConfigurationError("native MCP JARVIS progress event state was invalid")
    integer_fields = (
        "transport_sequence",
        "event_count",
        "event_sequence",
        "skipped_event_count",
    )
    for field_name in integer_fields:
        value = bridge.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} was invalid")
    if cast(int, bridge["event_count"]) < 1:
        raise ConfigurationError("native MCP JARVIS progress event count was invalid")
    observed = bridge.get("observed_at_epoch")
    if (
        isinstance(observed, bool)
        or not isinstance(observed, int | float)
        or not math.isfinite(float(observed))
        or observed < 0
    ):
        raise ConfigurationError("native MCP JARVIS progress observed time was invalid")
    for field_name in ("terminal", "determinate", "execution_validated"):
        if not isinstance(bridge.get(field_name), bool):
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} was invalid")
    candidate_metadata = dict(metadata)
    candidate_metadata.pop("mcp_native_progress_bridge", None)
    return jarvis_execution_progress_metadata(
        candidate_metadata,
        relay_job_id=job.job_id,
        execution_id=cast(str, bridge["execution_id"]),
        pipeline_id=pipeline_id,
        package_name=cast(str, bridge["package_name"]),
        package_id=cast(str, bridge["package_id"]),
        progress_state=cast(str, bridge["event_state"]),
        progress_sequence=cast(int, bridge["event_sequence"]),
        observed_at_epoch=float(observed),
        determinate=cast(bool, bridge["determinate"]),
        event_count=cast(int, bridge["event_count"]),
        skipped_event_count=cast(int, bridge["skipped_event_count"]),
        execution_state=cast(str, bridge["execution_state"]),
        execution_terminal=cast(bool, bridge["terminal"]),
        transport_sequence=cast(int, bridge["transport_sequence"]),
        server_artifact_digest=expected_digest,
        execution_binding_validated=cast(bool, bridge["execution_validated"]),
    )


def _normalized_provider_distribution(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _trusted_sidecar_metadata(metadata: dict[str, object], *, job_id: str) -> dict[str, object]:
    preserved = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    preserved["adapter"] = "regex"
    return package_progress_metadata(
        preserved,
        package_name="clio_relay.bounded_command",
        package_version="builtin",
        run_id=job_id,
    )


def _job_timeout_seconds(job: RelayJob) -> int | None:
    return job.spec.timeout_seconds


def _scheduler_job_ids_from_metadata(metadata: dict[str, Any]) -> list[str]:
    stored = metadata.get("scheduler_job_ids")
    if not isinstance(stored, list):
        return []
    ids: list[str] = []
    for item in cast(list[object], stored):
        if isinstance(item, str) and item not in ids:
            ids.append(item)
    return ids


def _owned_scheduler_job_ids_from_metadata(
    metadata: dict[str, Any],
    *,
    relay_job_id: str,
    task_id: str | None = None,
) -> list[str]:
    records = metadata.get("scheduler_job_ownership")
    if not isinstance(records, list):
        return []
    owned: list[str] = []
    for item in cast(list[object], records):
        if not isinstance(item, dict):
            continue
        record = cast(dict[str, object], item)
        scheduler_job_id = record.get("scheduler_job_id")
        runtime_source = record.get("runtime_metadata_source")
        expected_proofs = {
            RuntimeMetadataSource.JARVIS_MCP.value: {"owned_jarvis_run_mcp_result"},
            RuntimeMetadataSource.JARVIS_SIDECAR.value: {
                "authenticated_runtime_sidecar",
                "exact_scheduler_marker_reconciliation",
            },
            RuntimeMetadataSource.RELAY_RECONCILIATION.value: {
                "exact_scheduler_marker_reconciliation"
            },
        }.get(runtime_source if isinstance(runtime_source, str) else "", set())
        if (
            not isinstance(scheduler_job_id, str)
            or not scheduler_job_id
            or not isinstance(record.get("scheduler_provider"), str)
            or not record.get("scheduler_provider")
            or not isinstance(record.get("execution_id"), str)
            or not record.get("execution_id")
            or record.get("ownership_verified") is not True
            or record.get("relay_job_id") != relay_job_id
            or (task_id is not None and record.get("task_id") != task_id)
            or record.get("proof") not in expected_proofs
        ):
            continue
        if scheduler_job_id not in owned:
            owned.append(scheduler_job_id)
    return owned


def _runtime_metadata_exact_marker_reconciliation(
    metadata: JarvisRuntimeMetadata,
) -> dict[str, Any] | None:
    raw = metadata.details.get("scheduler_marker_reconciliation")
    if not isinstance(raw, dict):
        return None
    reconciliation = cast(dict[str, Any], raw)
    if (
        reconciliation.get("schema_version") != "clio-relay.scheduler-marker-reconciliation.v1"
        or reconciliation.get("provider") != metadata.scheduler_provider
        or reconciliation.get("scheduler_job_id") != metadata.scheduler_job_id
        or reconciliation.get("match_count") != 1
        or not isinstance(reconciliation.get("marker"), str)
        or not cast(str, reconciliation["marker"]).startswith("clio-relay-")
    ):
        return None
    return reconciliation


def _runtime_metadata_is_native(metadata: JarvisRuntimeMetadata) -> bool:
    """Return whether exact JARVIS handle, record, and progress documents were validated."""
    producer_contract = metadata.details.get("producer_contract")
    native_execution = metadata.details.get("native_execution")
    return (
        isinstance(producer_contract, dict)
        and cast(dict[str, object], producer_contract).get("contract_kind") == "native_execution"
        and isinstance(native_execution, dict)
    )


def _runtime_metadata_is_mcp_transport_wrapper(metadata: JarvisRuntimeMetadata) -> bool:
    """Return whether metadata describes the direct wrapper around one MCP call."""
    if (
        metadata.source is not RuntimeMetadataSource.JARVIS_SIDECAR
        or metadata.scheduler_provider is not None
        or metadata.scheduler_job_id is not None
    ):
        return False
    native_execution = metadata.details.get("native_execution")
    if isinstance(native_execution, dict):
        handle = cast(dict[str, object], native_execution).get("execution_handle")
        record = cast(dict[str, object], native_execution).get("execution_record")
        return (
            isinstance(handle, dict)
            and cast(dict[str, object], handle).get("mode") == "direct"
            and isinstance(record, dict)
            and cast(dict[str, object], record).get("submitted") is False
        )
    nested_details = metadata.details.get("details")
    return metadata.details.get("execution_mode") == "direct" or (
        isinstance(nested_details, dict)
        and cast(dict[str, object], nested_details).get("execution_mode") == "direct"
    )


def _task_direct_execution_pinned(task: RelayTask) -> bool:
    raw_sidecars = task.metadata.get("execution_sidecars")
    return (
        not _runtime_sidecar_channel_failed(task)
        and isinstance(raw_sidecars, dict)
        and cast(dict[str, object], raw_sidecars).get("scheduler_expected_resolved") is False
    )


def _task_scheduler_submission_refused(task: RelayTask) -> bool:
    raw_sidecars = task.metadata.get("execution_sidecars")
    return (
        not _runtime_sidecar_channel_failed(task)
        and isinstance(raw_sidecars, dict)
        and cast(dict[str, object], raw_sidecars).get("scheduler_submission_refused") is True
    )


def _runtime_sidecar_channel_failed(task: RelayTask) -> bool:
    """Return whether runtime authority is durably latched failed closed."""
    raw_channel = task.metadata.get("runtime_sidecar_channel")
    return (
        isinstance(raw_channel, dict)
        and cast(dict[str, object], raw_channel).get("schema_version")
        == RUNTIME_SIDECAR_CHANNEL_SCHEMA
        and cast(dict[str, object], raw_channel).get("state") == "failed_closed"
    )


def _durable_scheduler_submission_intent(task: RelayTask) -> dict[str, Any]:
    raw_sidecars = task.metadata.get("execution_sidecars")
    if not isinstance(raw_sidecars, dict):
        raise RelayError(f"scheduler submission intent is missing for task {task.task_id}")
    raw_intent = cast(dict[str, object], raw_sidecars).get("scheduler_submission_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError(f"scheduler submission intent is missing for task {task.task_id}")
    intent = cast(dict[str, Any], raw_intent)
    if (
        set(intent)
        != {
            "schema_version",
            "execution_id",
            "marker",
            "created_at",
            "scheduler_user",
            "scheduler_expected",
            "direct_proof_sha256",
        }
        or intent.get("schema_version") != "clio-relay.scheduler-submission-intent.v1"
        or any(
            not isinstance(intent.get(field), str) or not intent[field]
            for field in ("execution_id", "marker", "created_at", "scheduler_user")
        )
        or not cast(str, intent["execution_id"]).startswith("jarvis_")
        or not cast(str, intent["marker"]).startswith("clio-relay-")
        or intent.get("scheduler_expected") not in {True, False, "unknown"}
        or not isinstance(intent.get("direct_proof_sha256"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", cast(str, intent["direct_proof_sha256"]))
    ):
        raise RelayError(f"scheduler submission intent is invalid for task {task.task_id}")
    try:
        created_at = datetime.fromisoformat(cast(str, intent["created_at"]))
    except ValueError as exc:
        raise RelayError(f"scheduler submission intent time is invalid for {task.task_id}") from exc
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise RelayError(f"scheduler submission intent time is naive for {task.task_id}")
    return intent


def _task_id_for_scheduler_job(tasks: list[RelayTask], scheduler_job_id: str) -> str | None:
    for task in tasks:
        if scheduler_job_id in _scheduler_job_ids_from_metadata(task.metadata):
            return task.task_id
    return None


def _task_scheduler_status(
    tasks: list[RelayTask],
    task_id: str,
    scheduler_job_id: str,
) -> dict[str, Any] | None:
    for task in tasks:
        if task.task_id != task_id:
            continue
        stored = task.metadata.get("scheduler_status")
        if not isinstance(stored, dict):
            return None
        typed = cast(dict[str, Any], stored)
        if typed.get("scheduler_job_id") != scheduler_job_id:
            return None
        return typed
    return None


@contextmanager
def _job_subprocess_env(
    values: dict[str, str],
) -> Generator[dict[str, str], None, None]:
    """Yield an isolated child environment without mutating threaded process state."""
    yield {**os.environ, **values}
