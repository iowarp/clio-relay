"""Long-running desktop and cluster endpoint behavior."""

from __future__ import annotations

import os
import secrets as secrets
import socket
import sys as sys
from collections.abc import Callable as Callable
from collections.abc import Generator as Generator
from contextlib import contextmanager as contextmanager
from contextlib import suppress as suppress
from datetime import datetime as datetime
from datetime import timedelta as timedelta
from pathlib import Path as Path
from typing import Any as Any
from typing import cast as cast

from filelock import FileLock as FileLock
from filelock import Timeout as Timeout

from clio_relay import execution_watch as execution_watch
from clio_relay import process_containment as process_containment
from clio_relay.bootstrap_reconcile import (
    resolve_receipt_bound_jarvis_python as resolve_receipt_bound_jarvis_python,
)
from clio_relay.command_evidence import bounded_error_detail as bounded_error_detail
from clio_relay.config import RelaySettings as RelaySettings
from clio_relay.console_stream import CONSOLE_STREAM as CONSOLE_STREAM
from clio_relay.console_stream import ConsoleLiveTailer as ConsoleLiveTailer
from clio_relay.console_stream import console_tailer_for_mcp_call as console_tailer_for_mcp_call
from clio_relay.console_stream import (
    flush_terminal_console_from_path as flush_terminal_console_from_path,
)
from clio_relay.core_queue import DEFAULT_EXACT_RECORD_LIMIT as DEFAULT_EXACT_RECORD_LIMIT
from clio_relay.core_queue import ClioCoreQueue as ClioCoreQueue
from clio_relay.endpoint_execution_cleanup_actions import ExecutionCleanupActionsMixin

# EXECUTION_CLEANUP_SCAN_LIMIT re-exported verbatim: tests/test_endpoint.py
# reads/patches it as `endpoint_module.EXECUTION_CLEANUP_SCAN_LIMIT` even
# though its real call sites now live on ExecutionLifecycleMixin.
from clio_relay.endpoint_execution_lifecycle import (
    EXECUTION_CLEANUP_SCAN_LIMIT as EXECUTION_CLEANUP_SCAN_LIMIT,
)
from clio_relay.endpoint_execution_lifecycle import ExecutionLifecycleMixin
from clio_relay.endpoint_execution_output import ExecutionOutputMixin
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _close_runtime_sidecar_anchors as _close_runtime_sidecar_anchors,
)
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _execution_cleanup_ack_metadata as _execution_cleanup_ack_metadata,
)
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _execution_cleanup_quarantine_paths as _execution_cleanup_quarantine_paths,
)
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _execution_sidecar_cleanup_plan as _execution_sidecar_cleanup_plan,
)
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _remove_execution_sidecars as _remove_execution_sidecars,
)
from clio_relay.endpoint_jarvis_dispatch import JarvisDispatchMixin
from clio_relay.endpoint_jarvis_recovery import (
    _attributed_jarvis_dispatch_refusal as _attributed_jarvis_dispatch_refusal,
)
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_dispatch_refusal_detail as _durable_jarvis_dispatch_refusal_detail,
)
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery as _durable_jarvis_execution_recovery,
)
from clio_relay.endpoint_jarvis_recovery import (
    _durable_runtime_recovery_state as _durable_runtime_recovery_state,
)
from clio_relay.endpoint_jarvis_recovery import (
    _endpoint_mcp_runner_command as _endpoint_mcp_runner_command,
)
from clio_relay.endpoint_jarvis_recovery import (
    _jarvis_execution_recovery_intent as _jarvis_execution_recovery_intent,
)
from clio_relay.endpoint_jarvis_recovery import (
    _jarvis_execution_recovery_is_pending as _jarvis_execution_recovery_is_pending,
)
from clio_relay.endpoint_jarvis_recovery import (
    _minimal_mcp_runner_environment as _minimal_mcp_runner_environment,
)
from clio_relay.endpoint_jarvis_recovery import (
    _trusted_jarvis_execution_query_validation as _trusted_jarvis_execution_query_validation,
)
from clio_relay.endpoint_jarvis_recovery import (
    _trusted_jarvis_mcp_result as _trusted_jarvis_mcp_result,
)
from clio_relay.endpoint_jarvis_recovery import (
    _trusted_jarvis_mcp_route as _trusted_jarvis_mcp_route,
)
from clio_relay.endpoint_jarvis_recovery_bookkeeping import JarvisRecoveryBookkeepingMixin
from clio_relay.endpoint_jarvis_recovery_query import JarvisRecoveryQueryMixin
from clio_relay.endpoint_job_execution import JobExecutionMixin
from clio_relay.endpoint_progress_ingest import ProgressIngestMixin
from clio_relay.endpoint_progress_log_io import (
    _normalize_package_progress_log_path as _normalize_package_progress_log_path,
)
from clio_relay.endpoint_progress_log_io import (
    _open_package_progress_log as _open_package_progress_log,
)
from clio_relay.endpoint_progress_log_io import _progress_log_identity as _progress_log_identity
from clio_relay.endpoint_progress_log_io import (
    _render_progress_log_identity as _render_progress_log_identity,
)
from clio_relay.endpoint_progress_log_io import (
    _validated_native_subprocess_cwd as _validated_native_subprocess_cwd,
)
from clio_relay.endpoint_progress_trust import (
    _progress_from_sidecar_record as _progress_from_sidecar_record,
)
from clio_relay.endpoint_progress_trust import _progress_log_checkpoint as _progress_log_checkpoint
from clio_relay.endpoint_progress_trust import (
    _progress_log_checkpoint_matches as _progress_log_checkpoint_matches,
)
from clio_relay.endpoint_progress_trust import (
    _read_bounded_sidecar_record as _read_bounded_sidecar_record,
)
from clio_relay.endpoint_progress_trust import (
    _trusted_mcp_progress_metadata as _trusted_mcp_progress_metadata,
)
from clio_relay.endpoint_progress_trust import (
    _trusted_native_mcp_progress_metadata as _trusted_native_mcp_progress_metadata,
)
from clio_relay.endpoint_progress_trust import (
    _trusted_provider_metadata as _trusted_provider_metadata,
)
from clio_relay.endpoint_progress_trust import (
    _trusted_sidecar_metadata as _trusted_sidecar_metadata,
)
from clio_relay.endpoint_recovery_directory import (
    _close_recovery_directory_anchor as _close_recovery_directory_anchor,
)
from clio_relay.endpoint_recovery_directory import (
    _jarvis_execution_recovery_retry_due as _jarvis_execution_recovery_retry_due,
)
from clio_relay.endpoint_recovery_directory import (
    _open_or_create_recovery_directory as _open_or_create_recovery_directory,
)
from clio_relay.endpoint_recovery_directory import (
    _read_owned_recovery_result as _read_owned_recovery_result,
)
from clio_relay.endpoint_recovery_directory import _recovery_timestamp as _recovery_timestamp
from clio_relay.endpoint_recovery_directory import (
    _remove_owned_recovery_output as _remove_owned_recovery_output,
)
from clio_relay.endpoint_recovery_directory import (
    _validate_recovery_directory_path as _validate_recovery_directory_path,
)
from clio_relay.endpoint_recovery_directory import (
    _validate_recovery_process_cwd as _validate_recovery_process_cwd,
)
from clio_relay.endpoint_recovery_directory import (
    _write_private_json_file as _write_private_json_file,
)
from clio_relay.endpoint_result_finalization import ResultFinalizationMixin
from clio_relay.endpoint_runtime_metadata_ingest import RuntimeMetadataIngestMixin
from clio_relay.endpoint_runtime_metadata_persist import RuntimeMetadataPersistMixin
from clio_relay.endpoint_runtime_sidecar_anchor import _open_owned_sidecar as _open_owned_sidecar
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _precreate_runtime_sidecar as _precreate_runtime_sidecar,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _runtime_sidecar_anchor_from_metadata as _runtime_sidecar_anchor_from_metadata,
)
from clio_relay.endpoint_runtime_sidecar_failure import RuntimeSidecarFailureMixin
from clio_relay.endpoint_scheduler_cancel import SchedulerCancelMixin
from clio_relay.endpoint_scheduler_cancel_actions import SchedulerCancelActionsMixin
from clio_relay.endpoint_scheduler_metadata import (
    _durable_scheduler_submission_intent as _durable_scheduler_submission_intent,
)
from clio_relay.endpoint_scheduler_metadata import _job_subprocess_env as _job_subprocess_env
from clio_relay.endpoint_scheduler_metadata import _job_timeout_seconds as _job_timeout_seconds
from clio_relay.endpoint_scheduler_metadata import (
    _native_runtime_created_at as _native_runtime_created_at,
)
from clio_relay.endpoint_scheduler_metadata import (
    _native_runtime_execution_mode as _native_runtime_execution_mode,
)
from clio_relay.endpoint_scheduler_metadata import (
    _owned_scheduler_job_ids_from_metadata as _owned_scheduler_job_ids_from_metadata,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_exact_marker_reconciliation as _runtime_metadata_exact_marker_reconciliation,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_is_mcp_transport_wrapper as _runtime_metadata_is_mcp_transport_wrapper,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_is_native as _runtime_metadata_is_native,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_sidecar_channel_failed as _runtime_sidecar_channel_failed,
)
from clio_relay.endpoint_scheduler_metadata import (
    _scheduler_job_ids_from_metadata as _scheduler_job_ids_from_metadata,
)
from clio_relay.endpoint_scheduler_metadata import (
    _task_direct_execution_pinned as _task_direct_execution_pinned,
)
from clio_relay.endpoint_scheduler_metadata import (
    _task_id_for_scheduler_job as _task_id_for_scheduler_job,
)
from clio_relay.endpoint_scheduler_metadata import _task_scheduler_status as _task_scheduler_status
from clio_relay.endpoint_scheduler_metadata import (
    _task_scheduler_submission_refused as _task_scheduler_submission_refused,
)
from clio_relay.endpoint_scheduler_submission import SchedulerSubmissionMixin
from clio_relay.endpoint_scheduler_submission_reconcile import SchedulerSubmissionReconcileMixin
from clio_relay.endpoint_serve_loop import ServeLoopMixin
from clio_relay.endpoint_sidecar_types import AGENT_RESULT_MAX_BYTES as AGENT_RESULT_MAX_BYTES
from clio_relay.endpoint_sidecar_types import (
    EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS as EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS,
)
from clio_relay.endpoint_sidecar_types import EXECUTION_CLEANUP_SCHEMA as EXECUTION_CLEANUP_SCHEMA
from clio_relay.endpoint_sidecar_types import EXECUTION_LAUNCH_PROTOCOL as EXECUTION_LAUNCH_PROTOCOL
from clio_relay.endpoint_sidecar_types import (
    MCP_ENDPOINT_RUNNER_EXIT_GRACE_SECONDS as MCP_ENDPOINT_RUNNER_EXIT_GRACE_SECONDS,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_QUERY_PROCESS_TIMEOUT_SECONDS as MCP_JARVIS_EXECUTION_QUERY_PROCESS_TIMEOUT_SECONDS,  # noqa: E501
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_QUERY_TIMEOUT_SECONDS as MCP_JARVIS_EXECUTION_QUERY_TIMEOUT_SECONDS,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES as MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES,  # noqa: E501
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_RETRY_BASE_SECONDS as MCP_JARVIS_EXECUTION_RECOVERY_RETRY_BASE_SECONDS,  # noqa: E501
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_RETRY_MAX_SECONDS as MCP_JARVIS_EXECUTION_RECOVERY_RETRY_MAX_SECONDS,  # noqa: E501
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA as MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
)
from clio_relay.endpoint_sidecar_types import (
    PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES as PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES,
)
from clio_relay.endpoint_sidecar_types import (
    PACKAGE_PROGRESS_LOG_READ_BYTES as PACKAGE_PROGRESS_LOG_READ_BYTES,
)
from clio_relay.endpoint_sidecar_types import (
    PROGRESS_SIDECAR_MAX_RECORD_BYTES as PROGRESS_SIDECAR_MAX_RECORD_BYTES,
)
from clio_relay.endpoint_sidecar_types import (
    PROGRESS_SIDECAR_MAX_RECORDS as PROGRESS_SIDECAR_MAX_RECORDS,
)
from clio_relay.endpoint_sidecar_types import (
    PROGRESS_SIDECAR_MAX_TOTAL_BYTES as PROGRESS_SIDECAR_MAX_TOTAL_BYTES,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_CHANNEL_SCHEMA as RUNTIME_SIDECAR_CHANNEL_SCHEMA,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_MAX_RECORD_BYTES as RUNTIME_SIDECAR_MAX_RECORD_BYTES,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_MAX_RECORDS as RUNTIME_SIDECAR_MAX_RECORDS,
)
from clio_relay.endpoint_sidecar_types import (
    RUNTIME_SIDECAR_MAX_TOTAL_BYTES as RUNTIME_SIDECAR_MAX_TOTAL_BYTES,
)

# Re-exported verbatim for tests/test_jarvis_run_failure_reporting.py:484's
# `endpoint_module.SchedulerSubmissionUnresolvedError` (a module-attribute
# access, not a name this file's own body uses) -- the facade contract every
# other endpoint-split slice keeps (scripts/check_file_size.py's endpoint.py
# history), so noqa is the honest marker rather than a silent drop.
from clio_relay.endpoint_sidecar_types import (
    SchedulerSubmissionUnresolvedError as SchedulerSubmissionUnresolvedError,
)
from clio_relay.endpoint_sidecar_types import _PackageProgressLogState as _PackageProgressLogState
from clio_relay.endpoint_sidecar_types import _RecoveryDirectoryAnchor as _RecoveryDirectoryAnchor
from clio_relay.endpoint_sidecar_types import _RuntimeSidecarAnchor as _RuntimeSidecarAnchor
from clio_relay.endpoint_worker_environment import (
    _bounded_output_event_chunks as _bounded_output_event_chunks,
)
from clio_relay.endpoint_worker_environment import (
    _configured_scheduler_provider_name as _configured_scheduler_provider_name,
)
from clio_relay.endpoint_worker_environment import (
    _extract_scheduler_job_id as _extract_scheduler_job_id,
)
from clio_relay.endpoint_worker_environment import _file_summary as _file_summary
from clio_relay.endpoint_worker_environment import _jarvis_pipeline_name as _jarvis_pipeline_name
from clio_relay.endpoint_worker_environment import (
    _normalized_scheduler_status as _normalized_scheduler_status,
)
from clio_relay.endpoint_worker_environment import _optional_float as _optional_float
from clio_relay.endpoint_worker_environment import _optional_metadata as _optional_metadata
from clio_relay.endpoint_worker_environment import _optional_str as _optional_str
from clio_relay.endpoint_worker_environment import (
    _scheduler_name_from_job as _scheduler_name_from_job,
)
from clio_relay.endpoint_worker_environment import (
    _scheduler_name_from_yaml as _scheduler_name_from_yaml,
)
from clio_relay.endpoint_worker_environment import (
    _scheduler_status_is_not_found as _scheduler_status_is_not_found,
)
from clio_relay.endpoint_worker_environment import (
    _validate_scheduler_launch_provider as _validate_scheduler_launch_provider,
)
from clio_relay.endpoint_worker_environment import (
    _worker_installation_snapshot as _worker_installation_snapshot,
)
from clio_relay.endpoint_worker_environment import (
    _worker_process_identity as _worker_process_identity,
)
from clio_relay.endpoint_worker_lanes import quarantine_relay_error as quarantine_relay_error
from clio_relay.endpoint_worker_lanes import run_worker_lane_iteration as run_worker_lane_iteration
from clio_relay.errors import ConfigurationError as ConfigurationError
from clio_relay.errors import QueueConflictError as QueueConflictError
from clio_relay.errors import RelayError as RelayError
from clio_relay.filesystem_paths import internal_filesystem_path as internal_filesystem_path
from clio_relay.filesystem_paths import logical_filesystem_text as logical_filesystem_text
from clio_relay.identifiers import filesystem_key as filesystem_key
from clio_relay.jarvis_dispatch_failure import (
    JARVIS_DISPATCH_REFUSAL_RESOLUTION as JARVIS_DISPATCH_REFUSAL_RESOLUTION,
)
from clio_relay.jarvis_dispatch_failure import JarvisDispatchRefusal as JarvisDispatchRefusal
from clio_relay.jarvis_dispatch_failure import McpRuntimeIngestOutcome as McpRuntimeIngestOutcome
from clio_relay.jarvis_execution import (
    RUNTIME_SCHEDULER_PROVIDER_ENV as RUNTIME_SCHEDULER_PROVIDER_ENV,
)
from clio_relay.jarvis_execution_artifacts import (
    ingest_jarvis_execution_outputs_from_path as ingest_jarvis_execution_outputs_from_path,
)
from clio_relay.jarvis_provider import JarvisCdProvider as JarvisCdProvider
from clio_relay.jarvis_run_environment import (
    jarvis_run_environment_values as jarvis_run_environment_values,
)
from clio_relay.jarvis_run_environment import (
    registered_site_spack_command as registered_site_spack_command,
)
from clio_relay.models import CLIO_PROVENANCE_METADATA_KEY as CLIO_PROVENANCE_METADATA_KEY
from clio_relay.models import ArtifactRef as ArtifactRef
from clio_relay.models import EndpointRegistration as EndpointRegistration
from clio_relay.models import EndpointRole as EndpointRole
from clio_relay.models import JarvisRunSpec as JarvisRunSpec
from clio_relay.models import JobKind as JobKind
from clio_relay.models import JobState as JobState
from clio_relay.models import Lease as Lease
from clio_relay.models import McpAdmissionClass as McpAdmissionClass
from clio_relay.models import McpCallSpec as McpCallSpec
from clio_relay.models import ProgressRecord as ProgressRecord
from clio_relay.models import RelayEvent as RelayEvent
from clio_relay.models import RelayJob as RelayJob
from clio_relay.models import RelayTask as RelayTask
from clio_relay.models import RemoteAgentTaskSpec as RemoteAgentTaskSpec
from clio_relay.models import SchedulerCancelDispositionState as SchedulerCancelDispositionState
from clio_relay.models import SchedulerCancelPending as SchedulerCancelPending
from clio_relay.models import SchedulerPhase as SchedulerPhase
from clio_relay.models import SchedulerStatus as SchedulerStatus
from clio_relay.models import utc_now as utc_now
from clio_relay.progress_adapters import PackageProgressProvider as PackageProgressProvider
from clio_relay.progress_adapters import (
    package_progress_adapter_from_pipeline as package_progress_adapter_from_pipeline,
)
from clio_relay.progress_provenance import (
    PackageProgressSourceAuthority as PackageProgressSourceAuthority,
)
from clio_relay.progress_provenance import (
    validate_jarvis_execution_progress_metadata as validate_jarvis_execution_progress_metadata,
)
from clio_relay.progress_provenance import (
    validate_package_progress_metadata as validate_package_progress_metadata,
)
from clio_relay.progress_provenance import (
    validate_package_progress_provider_metadata as validate_package_progress_provider_metadata,
)
from clio_relay.runtime_metadata import JarvisRuntimeMetadata as JarvisRuntimeMetadata
from clio_relay.runtime_metadata import (
    RuntimeMetadataIdentityConflictError as RuntimeMetadataIdentityConflictError,
)
from clio_relay.runtime_metadata import RuntimeMetadataSource as RuntimeMetadataSource
from clio_relay.runtime_metadata import (
    legacy_scheduler_runtime_metadata as legacy_scheduler_runtime_metadata,
)
from clio_relay.runtime_metadata import merge_runtime_metadata as merge_runtime_metadata
from clio_relay.runtime_metadata import (
    runtime_metadata_from_mcp_result_document as runtime_metadata_from_mcp_result_document,
)
from clio_relay.runtime_metadata import (
    runtime_metadata_from_sidecar_record as runtime_metadata_from_sidecar_record,
)
from clio_relay.scheduler_providers import SchedulerProvider as SchedulerProvider
from clio_relay.scheduler_providers import (
    SchedulerReconciliationProvider as SchedulerReconciliationProvider,
)
from clio_relay.scheduler_providers import provider_for_scheduler as provider_for_scheduler
from clio_relay.scheduler_providers import (
    reconciliation_provider_for_scheduler as reconciliation_provider_for_scheduler,
)
from clio_relay.spool import JobSpool as JobSpool
from clio_relay.spool import read_owned_regular_file_bytes as read_owned_regular_file_bytes
from clio_relay.storage_runtime import StorageManagedQueue as StorageManagedQueue
from clio_relay.storage_runtime import StorageRuntime as StorageRuntime
from clio_relay.storage_runtime import StorageRuntimeViolation as StorageRuntimeViolation
from clio_relay.storage_runtime import (
    initialize_queue_with_shared_writer_fencing as initialize_queue_with_shared_writer_fencing,
)
from clio_relay.storage_runtime import storage_managed_queue as storage_managed_queue
from clio_relay.worker_concurrency import KindConcurrencyInput as KindConcurrencyInput
from clio_relay.worker_concurrency import kind_concurrency_metadata as kind_concurrency_metadata
from clio_relay.worker_concurrency import normalize_kind_concurrency as normalize_kind_concurrency
from clio_relay.worker_lifetime_lock import WorkerLifetimeLock as WorkerLifetimeLock


class EndpointWorker(
    ServeLoopMixin,
    JobExecutionMixin,
    ExecutionOutputMixin,
    ProgressIngestMixin,
    RuntimeSidecarFailureMixin,
    RuntimeMetadataIngestMixin,
    JarvisRecoveryQueryMixin,
    JarvisRecoveryBookkeepingMixin,
    JarvisDispatchMixin,
    RuntimeMetadataPersistMixin,
    SchedulerSubmissionMixin,
    SchedulerSubmissionReconcileMixin,
    ExecutionLifecycleMixin,
    ExecutionCleanupActionsMixin,
    SchedulerCancelMixin,
    SchedulerCancelActionsMixin,
    ResultFinalizationMixin,
):
    """Endpoint worker for desktop or cluster roles.

    Runtime HOST only: this class is the composed facade over the 17
    ``endpoint_*.py`` mixins (iowarp/clio-relay#231's endpoint split) that
    each own one execution/recovery/cleanup concern. What stays directly
    resident here is lifecycle only -- constructing and validating the
    worker's queue/provider/lease identity (``__init__``), releasing it
    (``close``/``__del__``), the closed/identity guard every public entry
    point calls first (``_require_open_queue_identity``), and durable
    endpoint registration (``register``). Every other method an instance of
    this class answers to (``run_once``, ``_run_job_impl``,
    ``_reconcile_pending_execution_cleanup``, ...) is defined on one of the
    mixins above and resolved through the normal Python MRO -- a mixin
    method's ``self.foo()`` reaches a sibling mixin's method exactly as it
    would if every method still lived in one file.
    """

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
        control_query_concurrency: int = 0,
        kind_concurrency: KindConcurrencyInput | None = None,
        queue: ClioCoreQueue | None = None,
        provider: JarvisCdProvider | None = None,
        scheduler_provider: SchedulerProvider | None = None,
        storage_runtime: StorageRuntime | None = None,
        reconcile_execution_cleanup: bool = True,
    ) -> None:
        if (
            isinstance(concurrency, bool)
            or not isinstance(concurrency, int)  # pyright: ignore[reportUnnecessaryIsInstance]
            or concurrency < 1
        ):
            raise ConfigurationError("worker concurrency must be at least 1")
        if (
            isinstance(control_query_concurrency, bool)
            or not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                control_query_concurrency,
                int,
            )
            or control_query_concurrency < 0
        ):
            raise ConfigurationError("worker control-query concurrency cannot be negative")
        if control_query_concurrency >= concurrency:
            raise ConfigurationError(
                "worker control-query concurrency must leave at least one workload slot"
            )
        self.role = role
        self.cluster = cluster
        self.concurrency = concurrency
        self.control_query_concurrency = control_query_concurrency
        self.workload_concurrency = concurrency - control_query_concurrency
        self.kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self.reconcile_execution_cleanup = reconcile_execution_cleanup
        self._foreground_jobs_since_cleanup = 0
        self._closed = False
        self._queue_root_path: Path | None = None
        self._queue_root_identity: tuple[int, int] | None = None
        self._worker_lifetime_lock: WorkerLifetimeLock | None = None
        self._owned_managed_queue: StorageManagedQueue | None = None
        if self.role == EndpointRole.WORKER:
            try:
                lifetime_core = queue.root if queue is not None else settings.core_dir
                self._worker_lifetime_lock = WorkerLifetimeLock(
                    lifetime_core,
                    mode="shared",
                ).acquire()
                if queue is not None:
                    initialize_queue_with_shared_writer_fencing(self._worker_lifetime_lock)
                settings = settings.model_copy(
                    update={"core_dir": self._worker_lifetime_lock.core_dir}
                )
            except BaseException:
                self.close()
                raise
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
            if provider is None:
                execution_python = (
                    resolve_receipt_bound_jarvis_python(settings.jarvis_bin)
                    if self.role == EndpointRole.WORKER
                    else None
                )
                provider = JarvisCdProvider(
                    jarvis_bin=settings.jarvis_bin,
                    execution_python=execution_python,
                    agent_bin=settings.agent_bin,
                    agent_adapter=settings.agent_adapter,
                    agent_args=settings.agent_args,
                )
            self.provider = provider
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
            metadata["workload_concurrency"] = self.workload_concurrency
            metadata["control_query_concurrency"] = self.control_query_concurrency
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
