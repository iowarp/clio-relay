"""The job execution orchestrator: ``_run_job_impl``.

Owner module for iowarp/clio-relay#231's endpoint decomposition. ``_run_job_impl`` (~645
lines) is one sequential procedure -- launch setup, sidecar precreation, streaming
execution, progress/runtime ingest, ownership resolution, and terminal-state recording
-- that threads roughly thirty local variables through nested closures (``on_stdout``,
``on_poll``, ...) passed into ``self._run_execution_streaming``. Splitting it further
would mean rewriting that closure capture, not moving it, so unlike its sibling owner
modules it stays a single method per the sweet-spot exception already established for
``bootstrap_reconcile_activation_paths.py`` (548 lines) and
``jarvis_mcp_validation_report.py`` (797 lines) in this same ratchet.

Its thin caller, ``_run_job`` (sidecar cleanup around this method on either success or
failure), stays with ``run_once`` in ``endpoint_serve_loop.py`` instead -- the pairing
that keeps both files under the 800-line cap.
"""

from __future__ import annotations

import getpass
import hashlib
import json
import secrets
import sys
import time
from pathlib import Path
from typing import cast

from clio_relay import execution_watch, process_containment
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.console_stream import (
    ConsoleLiveTailer,
    console_tailer_for_mcp_call,
)
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _execution_sidecar_cleanup_plan,
    _remove_execution_sidecars,
)
from clio_relay.endpoint_jarvis_recovery import (
    _jarvis_execution_recovery_intent,
    _minimal_mcp_runner_environment,
)
from clio_relay.endpoint_progress_log_io import (
    _normalize_package_progress_log_path,
)
from clio_relay.endpoint_recovery_directory import (
    _close_recovery_directory_anchor,
    _open_or_create_recovery_directory,
    _write_private_json_file,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _precreate_runtime_sidecar,
)
from clio_relay.endpoint_scheduler_metadata import (
    _job_subprocess_env,
    _job_timeout_seconds,
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    EXECUTION_CLEANUP_SCHEMA,
    EXECUTION_LAUNCH_PROTOCOL,
    RUNTIME_SIDECAR_CHANNEL_SCHEMA,
    _RecoveryDirectoryAnchor,
    _RuntimeSidecarAnchor,
)
from clio_relay.endpoint_worker_environment import (
    _configured_scheduler_provider_name,
    _jarvis_pipeline_name,
    _scheduler_name_from_yaml,
    _validate_scheduler_launch_provider,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.jarvis_execution import RUNTIME_SCHEDULER_PROVIDER_ENV
from clio_relay.models import (
    JobKind,
    JobState,
    Lease,
    McpCallSpec,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.progress_adapters import (
    package_progress_adapter_from_pipeline,
)
from clio_relay.progress_provenance import (
    PackageProgressSourceAuthority,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)
from clio_relay.spool import JobSpool


class JobExecutionMixin:
    """Mixin: JobExecution methods split from EndpointWorker (clio-relay#231)."""

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
        endpoint_mcp_call = job.kind is JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec)
        pipeline_name = None if endpoint_mcp_call else _jarvis_pipeline_name(job)
        configured_scheduler_provider = _configured_scheduler_provider_name(self.scheduler_provider)
        scheduler_name: str | None = None
        # #259: only a jarvis_run mcp_call gets a live tailer (every other
        # mcp_call tool has no application subprocess of its own); None
        # otherwise, and the terminal console flush below tolerates that.
        console_tailer: ConsoleLiveTailer | None = None
        if endpoint_mcp_call:
            assert isinstance(job.spec, McpCallSpec)
            console_tailer = console_tailer_for_mcp_call(job.spec, spool=spool)
            yaml_text = None
            pipeline_path = spool.path / "mcp-request.json"
            _write_private_json_file(
                pipeline_path,
                job.spec.model_dump(mode="json", exclude_none=True),
            )
            package_progress_adapter = None
            package_progress_logs = []
            self.queue.append_artifact(spool.artifact_for(pipeline_path, kind="mcp_request"))
            self.queue.append_event(
                job.job_id,
                "mcp.started",
                "Endpoint-owned MCP operation started",
                payload={
                    "request": str(pipeline_path),
                    "operation": job.spec.operation.value,
                    "tool": job.spec.tool,
                    "containment": process_containment.containment_capability(),
                },
            )
        elif pipeline_name is None:
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
        jarvis_execution_recovery = _jarvis_execution_recovery_intent(
            job,
            created_at=started_at,
        )
        progress_sidecar_enabled = not endpoint_mcp_call or jarvis_execution_recovery is not None
        runtime_sidecar_enabled = not endpoint_mcp_call
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
        runtime_direct_proof_token: str | None = None
        runtime_submission_intent: dict[str, object] | None = None
        if not endpoint_mcp_call:
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
        recovery_directory_anchor: _RecoveryDirectoryAnchor | None = None
        if jarvis_execution_recovery is not None:
            recovery_directory = spool.path / cast(
                str,
                jarvis_execution_recovery["recovery_directory_name"],
            )
            recovery_directory_anchor, _created = _open_or_create_recovery_directory(
                recovery_directory,
                expected_metadata=None,
            )
            jarvis_execution_recovery = {
                **jarvis_execution_recovery,
                "recovery_directory_anchor": recovery_directory_anchor.as_metadata(),
            }
        execution_sidecars_metadata: dict[str, object] = {
            "schema_version": "clio-relay.execution-sidecars.v1",
            "progress": progress_sidecar.name,
            "progress_anchor": progress_sidecar_anchor.as_metadata(),
            "runtime": runtime_sidecar.name,
            "runtime_anchor": runtime_sidecar_anchor.as_metadata(),
        }
        if runtime_submission_intent is not None:
            execution_sidecars_metadata.update(
                {
                    "progress_anchor_required": True,
                    "scheduler_submission_intent": runtime_submission_intent,
                }
            )
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
        }
        if runtime_sidecar_enabled:
            execution_cleanup_metadata["runtime_sidecar_channel"] = {
                "schema_version": RUNTIME_SIDECAR_CHANNEL_SCHEMA,
                "state": "open",
                "opened_at": started_at.isoformat(),
                "evidence_retention": "whole_job_spool",
            }
        if jarvis_execution_recovery is not None:
            execution_cleanup_metadata["jarvis_execution_recovery"] = jarvis_execution_recovery
        try:
            self.queue.register_execution_cleanup(
                task.task_id,
                execution_cleanup_metadata,
            )
        finally:
            if recovery_directory_anchor is not None:
                _close_recovery_directory_anchor(recovery_directory_anchor)
        if jarvis_execution_recovery is not None:
            self.queue.append_event(
                job.job_id,
                "jarvis.execution_intent_persisted",
                "JARVIS execution identity persisted before MCP dispatch",
                payload={
                    "task_id": task.task_id,
                    "pipeline_id": jarvis_execution_recovery["pipeline_id"],
                    "execution_id": jarvis_execution_recovery["execution_id"],
                    "scheduler_expected": "unknown",
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
        if endpoint_mcp_call:
            assert isinstance(job.spec, McpCallSpec)
            execution_environment_values = _minimal_mcp_runner_environment(job.spec.env_from)
            execution_environment_values.update(
                self._jarvis_run_environment_values(job, task_id=task.task_id)
            )
            if progress_sidecar_enabled:
                execution_environment_values.update(
                    {
                        "CLIO_RELAY_PROGRESS_FILE": str(internal_filesystem_path(progress_sidecar)),
                        "CLIO_RELAY_PROGRESS_TOKEN": progress_sidecar_token,
                    }
                )
        else:
            assert runtime_submission_intent is not None
            assert runtime_direct_proof_token is not None
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
            inherit_parent=not endpoint_mcp_call,
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
                    scheduler_job_ids=None if endpoint_mcp_call else scheduler_job_ids,
                    scheduler_task_id=None if endpoint_mcp_call else task.task_id,
                    runtime_metadata_state=(None if endpoint_mcp_call else runtime_metadata_state),
                    runtime_metadata_digests=(
                        None if endpoint_mcp_call else runtime_metadata_digests
                    ),
                ),
                on_stderr=lambda text: self._append_output(
                    job,
                    spool,
                    "stderr",
                    text,
                    scheduler_job_ids=None if endpoint_mcp_call else scheduler_job_ids,
                    scheduler_task_id=None if endpoint_mcp_call else task.task_id,
                    runtime_metadata_state=(None if endpoint_mcp_call else runtime_metadata_state),
                    runtime_metadata_digests=(
                        None if endpoint_mcp_call else runtime_metadata_digests
                    ),
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
                on_poll=self._wrap_poll(
                    job,
                    console_tailer,
                    lambda: self._poll_running_job(
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
                        ingest_progress_sidecar=progress_sidecar_enabled,
                        ingest_runtime_sidecar=runtime_sidecar_enabled,
                    ),
                ),
            )
        self._check_runtime_storage(job, spool, force_job_scan=True)
        if runtime_sidecar_enabled:
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
        if progress_sidecar_enabled:
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
        # #266: watch a scheduler-deferred jarvis_run to real terminal,
        # BEFORE the one-and-only ingest below ever reads mcp-result.json.
        # A synchronous/already-terminal dispatch returns None unchanged.
        execution_watch_resolution = self._watch_deferred_jarvis_execution(
            job,
            spool=spool,
            console_tailer=console_tailer,
            lease=lease,
            last_renewed_at=last_renewed_at,
        )
        mcp_runtime_outcome = self._ingest_mcp_runtime_metadata(
            job,
            task_id=task.task_id,
            spool=spool,
            state=runtime_metadata_state,
            digests=runtime_metadata_digests,
            scheduler_job_ids=scheduler_job_ids,
        )
        dispatch_recovered = False
        dispatch_refusal = mcp_runtime_outcome.refusal
        if jarvis_execution_recovery is not None and not mcp_runtime_outcome.ingested:
            if dispatch_refusal is not None:
                self._refuse_jarvis_execution_recovery(
                    job,
                    task_id=task.task_id,
                    spool=spool,
                    refusal=dispatch_refusal,
                )
            else:
                dispatch_recovered = self._recover_jarvis_execution(
                    job,
                    task_id=task.task_id,
                    spool=spool,
                    state=runtime_metadata_state,
                    digests=runtime_metadata_digests,
                    scheduler_job_ids=scheduler_job_ids,
                )
        scheduler_identity_reconciled = False
        if not endpoint_mcp_call or jarvis_execution_recovery is not None:
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
        if console_tailer is not None:
            # #259: only a jarvis_run mcp_call ever writes console.log; every
            # other job kind keeps the artifact list it already had (the log
            # door itself still serves an empty console stream for them --
            # JobSpool.read_log treats a missing file as empty+eof).
            self.queue.append_artifact(
                spool.artifact_for(spool.path / "console.log", kind="console")
            )
        self.queue.append_artifact(spool.artifact_for(spool.log_capture_path, kind="log_capture"))
        self._append_optional_result_artifacts(job, spool, console_tailer=console_tailer)
        # #266: fold a resolved watch into the pre-#266 outcome logic --
        # see execution_watch.resolve_execution_outcome's own docstring for
        # why a resolved watch always wins over a pending cancellation.
        outcome = execution_watch.resolve_execution_outcome(
            dispatch_recovered=dispatch_recovered,
            watch_resolution=execution_watch_resolution,
            dispatch_refusal_present=dispatch_refusal is not None,
            transport_returncode=result.returncode,
            cancellation_requested=self._job_cancellation_requested(job.job_id),
        )
        effective_returncode = outcome.effective_returncode
        cancellation_honored = outcome.cancellation_honored
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
                (
                    "Endpoint MCP process terminated after cancellation"
                    if endpoint_mcp_call
                    else "JARVIS-CD process terminated after cancellation"
                ),
                payload={"returncode": result.returncode},
            )
            self.queue.acknowledge_job_cancellation(job.job_id)
            return
        if effective_returncode == 0:
            self.queue.update_task_state(
                task.task_id,
                JobState.SUCCEEDED,
                message=f"Task succeeded: {task.name}",
                metadata={
                    "returncode": effective_returncode,
                    "mcp_dispatch_recovered": dispatch_recovered,
                    "mcp_transport_returncode": result.returncode,
                },
            )
            self.queue.update_job_state(
                job.job_id,
                JobState.SUCCEEDED,
                message=(
                    "Endpoint MCP operation succeeded"
                    if endpoint_mcp_call
                    else "JARVIS-CD run succeeded"
                ),
            )
            return
        watch_failure = outcome.watch_failure
        failure_metadata: dict[str, object] = {"returncode": effective_returncode}
        if dispatch_refusal is not None:
            failure_metadata["jarvis_dispatch_refusal"] = dispatch_refusal.as_payload()
        if watch_failure is not None:
            failure_metadata["execution_watch_failure"] = watch_failure
        self.queue.update_task_state(
            task.task_id,
            JobState.FAILED,
            message=f"Task failed: {task.name}",
            metadata=failure_metadata,
        )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message=(
                "JARVIS run failed"
                if dispatch_refusal is not None
                else "JARVIS execution ended in failure"
                if watch_failure is not None
                else "Endpoint MCP operation failed"
                if endpoint_mcp_call
                else "JARVIS-CD run failed"
            ),
            error=(
                bounded_error_detail(dispatch_refusal.as_error_detail())
                if dispatch_refusal is not None
                else bounded_error_detail(execution_watch.execution_watch_error_text(watch_failure))
                if watch_failure is not None
                else f"exit code {effective_returncode}"
            ),
        )
