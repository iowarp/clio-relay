"""The job execution orchestrator: ``_run_job_impl``.

Owner module for iowarp/clio-relay#231's endpoint decomposition. ``_run_job_impl`` is one
sequential procedure -- launch setup, sidecar precreation, streaming execution,
progress/runtime ingest, ownership resolution, and terminal-state recording -- that
threads roughly thirty local variables through nested closures (``on_stdout``,
``on_poll``, ...) passed into ``self._run_execution_streaming``. Splitting it further
would mean rewriting that closure capture, not moving it, so unlike its sibling owner
modules it stays a single method per the sweet-spot exception already established for
``bootstrap_reconcile_activation_paths.py`` (548 lines) and
``jarvis_mcp_validation_report.py`` (797 lines) in this same ratchet.

Its thin caller, ``_run_job``, stays with ``run_once`` in ``endpoint_serve_loop.py``.
"""

from __future__ import annotations

import getpass
import hashlib
import json
import secrets
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from clio_relay import process_containment
from clio_relay.adapters.worker.endpoint_execution_sidecar_cleanup import (
    _execution_sidecar_cleanup_plan,
    _remove_execution_sidecars,
)
from clio_relay.adapters.worker.endpoint_progress_log_io import (
    _normalize_package_progress_log_path,
)
from clio_relay.adapters.worker.endpoint_runtime_sidecar_anchor import (
    _precreate_runtime_sidecar,
)
from clio_relay.adapters.worker.endpoint_scheduler_metadata import (
    _job_subprocess_env,
    _job_timeout_seconds,
    _runtime_metadata_is_native,
)
from clio_relay.adapters.worker.endpoint_sidecar_types import (
    EXECUTION_CLEANUP_SCHEMA,
    EXECUTION_LAUNCH_PROTOCOL,
    RUNTIME_SIDECAR_CHANNEL_SCHEMA,
    _RuntimeSidecarAnchor,
)
from clio_relay.adapters.worker.endpoint_worker_environment import (
    _configured_scheduler_provider_name,
    _jarvis_pipeline_name,
    _scheduler_name_from_yaml,
    _validate_scheduler_launch_provider,
)
from clio_relay.adapters.worker.jarvis_execution import RUNTIME_SCHEDULER_PROVIDER_ENV
from clio_relay.adapters.worker.progress_adapters import (
    package_progress_adapter_from_pipeline,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    JobKind,
    JobState,
    Lease,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.progress_provenance import (
    PackageProgressSourceAuthority,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)
from clio_relay.spool import JobSpool

if TYPE_CHECKING:
    from clio_relay.adapters.worker.core_queue import ClioCoreQueue

SUPPORTED_JOB_KINDS = frozenset({JobKind.JARVIS, JobKind.REMOTE_AGENT, JobKind.INPUT_INGEST})


class JobExecutionMixin:
    """Mixin: JobExecution methods split from EndpointWorker (clio-relay#231).

    ``queue`` is declared ``TYPE_CHECKING``-only (never assigned here) so
    strict pyright can resolve ``self.queue`` across this mixin's own
    methods (the sole composing class, ``EndpointWorker``, assigns it in
    ``__init__``; a mixin has none of its own).
    """

    if TYPE_CHECKING:
        queue: ClioCoreQueue

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
        if job.kind not in SUPPORTED_JOB_KINDS:
            # A queue may still hold a record of a job kind this worker dropped.
            self.queue.update_job_state(
                job.job_id,
                JobState.FAILED,
                message="Unsupported job kind",
                error=f"this worker no longer runs {job.kind.value} jobs",
            )
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
        runtime_submission_intent: dict[str, object] = {
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
        execution_sidecars_metadata: dict[str, object] = {
            "schema_version": "clio-relay.execution-sidecars.v1",
            "progress": progress_sidecar.name,
            "progress_anchor": progress_sidecar_anchor.as_metadata(),
            "runtime": runtime_sidecar.name,
            "runtime_anchor": runtime_sidecar_anchor.as_metadata(),
            "progress_anchor_required": True,
            "scheduler_submission_intent": runtime_submission_intent,
        }
        execution_cleanup_metadata: dict[str, object] = {
            "execution_sidecars": execution_sidecars_metadata,
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
        }
        self.queue.register_execution_cleanup(
            task.task_id,
            execution_cleanup_metadata,
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
        execution_environment_values = {
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
        with _job_subprocess_env(
            execution_environment_values,
            inherit_parent=True,
        ) as execution_env:
            result = self._run_execution_streaming(
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
        effective_returncode = result.returncode
        cancellation_honored = self._job_cancellation_requested(job.job_id)
        terminal_state = (
            JobState.CANCELED
            if cancellation_honored
            else JobState.SUCCEEDED
            if effective_returncode == 0
            else JobState.FAILED
        )
        self._append_provenance_artifact(
            job,
            spool,
            pipeline_path=pipeline_path,
            started_at=started_at.isoformat(),
            finished_at=utc_now().isoformat(),
            returncode=effective_returncode,
            terminal_state=terminal_state,
            runtime_metadata=runtime_metadata_state[0],
        )
        self._check_runtime_storage(job, spool, force_job_scan=True)
        if cancellation_honored:
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
        if effective_returncode == 0:
            self.queue.update_task_state(
                task.task_id,
                JobState.SUCCEEDED,
                message=f"Task succeeded: {task.name}",
                metadata={"returncode": effective_returncode},
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
            metadata={"returncode": effective_returncode},
        )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message="JARVIS-CD run failed",
            error=f"exit code {effective_returncode}",
        )
