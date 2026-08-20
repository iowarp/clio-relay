"""Execution provenance, transport dispatch, and stdout/stderr capture.

Owner module for iowarp/clio-relay#231's endpoint decomposition. Covers writing the
post-run provenance artifact (``_append_provenance_artifact``), rendering the JARVIS
pipeline YAML (``_render_job_yaml``), the endpoint-MCP-vs-JARVIS transport dispatch
(``_run_execution_streaming``/ ``_run_jarvis_streaming``), durable stdout/stderr capture
with truncation events (``_append_output``/``_append_ignored_stdout_markers``), and
package progress record validation/persistence (``_append_package_progress_records``).
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from clio_relay import process_containment
from clio_relay.endpoint_jarvis_recovery import (
    _endpoint_mcp_runner_command,
)
from clio_relay.endpoint_progress_log_io import (
    _validated_native_subprocess_cwd,
)
from clio_relay.endpoint_progress_trust import (
    _trusted_mcp_progress_metadata,
    _trusted_native_mcp_progress_metadata,
    _trusted_provider_metadata,
    _trusted_sidecar_metadata,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_ENDPOINT_RUNNER_EXIT_GRACE_SECONDS,
)
from clio_relay.endpoint_worker_environment import (
    _bounded_output_event_chunks,
    _file_summary,
    _optional_float,
    _optional_metadata,
    _optional_str,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RemoteAgentTaskSpec,
)
from clio_relay.progress_adapters import (
    PackageProgressProvider,
)
from clio_relay.progress_provenance import (
    PackageProgressSourceAuthority,
    validate_jarvis_execution_progress_metadata,
    validate_package_progress_metadata,
    validate_package_progress_provider_metadata,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)
from clio_relay.spool import JobSpool


class ExecutionOutputMixin:
    """Mixin: ExecutionOutput methods split from EndpointWorker (clio-relay#231)."""

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
        endpoint_mcp_call = job.kind is JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec)
        provider_metadata: dict[str, object]
        spool_metadata: dict[str, object]
        artifact_metadata: dict[str, object]
        if endpoint_mcp_call:
            provider_metadata = {
                "name": "clio-relay-endpoint-mcp",
                "runner": str(Path(_endpoint_mcp_runner_command(pipeline_path)[1])),
                "process_containment": process_containment.containment_capability(),
                "outer_jarvis_pipeline": False,
            }
            spool_metadata = {
                "path": str(spool.path),
                "request": str(pipeline_path),
                "stdout": str(spool.path / "stdout.log"),
                "stderr": str(spool.path / "stderr.log"),
                "log_capture": spool.capture_summary(),
            }
            artifact_metadata = {
                "request": _file_summary(pipeline_path),
                "stdout": _file_summary(spool.path / "stdout.log"),
                "stderr": _file_summary(spool.path / "stderr.log"),
                "log_capture": _file_summary(spool.log_capture_path),
            }
        else:
            provider_metadata = {
                "name": "jarvis-cd",
                "jarvis_bin": self.settings.jarvis_bin,
                "agent_bin": self.settings.agent_bin,
                "agent_adapter": self.settings.agent_adapter,
                "agent_args": self.settings.agent_args,
            }
            spool_metadata = {
                "path": str(spool.path),
                "pipeline": str(pipeline_path),
                "stdout": str(spool.path / "stdout.log"),
                "stderr": str(spool.path / "stderr.log"),
                "log_capture": spool.capture_summary(),
            }
            artifact_metadata = {
                "pipeline": _file_summary(pipeline_path),
                "stdout": _file_summary(spool.path / "stdout.log"),
                "stderr": _file_summary(spool.path / "stderr.log"),
                "log_capture": _file_summary(spool.log_capture_path),
            }
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
                "provider": provider_metadata,
                "runtime_metadata": (
                    None if runtime_metadata is None else runtime_metadata.model_dump(mode="json")
                ),
                "spool": spool_metadata,
                "artifacts": artifact_metadata,
            }
        )
        self._append_spool_artifact_once(
            job,
            spool,
            provenance_path,
            kind="provenance",
        )
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
            return self.provider.render_remote_agent_task_yaml(
                job.spec,
                relay_job_id=job.job_id,
            )
        if job.kind == JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec):
            return self.provider.render_mcp_call_yaml(job.spec)
        raise ConfigurationError(f"job kind/spec mismatch for {job.job_id}")

    def _run_execution_streaming(
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
        """Run MCP transport directly and reserve JARVIS for JARVIS-owned jobs."""
        if job.kind is JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec):
            if pipeline_name is not None:
                raise ConfigurationError("endpoint MCP operations cannot name a JARVIS pipeline")
            runtime_cwd = None if cwd is None else _validated_native_subprocess_cwd(cwd)
            outer_timeout = (
                None
                if timeout_seconds is None
                else timeout_seconds + MCP_ENDPOINT_RUNNER_EXIT_GRACE_SECONDS
            )
            return self.provider.run_command_streaming(
                _endpoint_mcp_runner_command(pipeline_path),
                process_label="endpoint MCP operation",
                cwd=runtime_cwd,
                env=env,
                on_stdout=on_stdout,
                on_stderr=on_stderr,
                on_start=on_start,
                should_cancel=should_cancel,
                timeout_seconds=outer_timeout,
                on_timeout=on_timeout,
                on_poll=on_poll,
            )
        return self._run_jarvis_streaming(
            job,
            pipeline_path=pipeline_path,
            pipeline_name=pipeline_name,
            cwd=cwd,
            env=env,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            on_start=on_start,
            should_cancel=should_cancel,
            timeout_seconds=timeout_seconds,
            on_timeout=on_timeout,
            on_poll=on_poll,
        )

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
