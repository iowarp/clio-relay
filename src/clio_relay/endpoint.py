"""Long-running desktop and cluster endpoint behavior."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import socket
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import yaml
from filelock import FileLock, Timeout

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpCallSpec,
    ProgressRecord,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    utc_now,
)
from clio_relay.progress_adapters import (
    LammpsThermoProgressAdapter,
    package_progress_adapter_from_pipeline,
)
from clio_relay.progress_provenance import (
    package_progress_metadata,
    validate_package_progress_metadata,
)
from clio_relay.spool import JobSpool


class EndpointWorker:
    """Endpoint worker for desktop or cluster roles."""

    lease_ttl_seconds = 120
    lease_renew_seconds = 30

    def __init__(
        self,
        *,
        role: EndpointRole,
        settings: RelaySettings,
        cluster: str = "local",
        queue: ClioCoreQueue | None = None,
        provider: JarvisCdProvider | None = None,
    ) -> None:
        self.role = role
        self.cluster = cluster
        self.settings = settings
        self.queue = queue or ClioCoreQueue(settings.core_dir)
        self.provider = provider or JarvisCdProvider(
            jarvis_bin=settings.jarvis_bin,
            agent_bin=settings.agent_bin,
            agent_adapter=settings.agent_adapter,
            agent_args=settings.agent_args,
        )
        self.endpoint: EndpointRegistration | None = None

    def register(self) -> EndpointRegistration:
        """Register this endpoint in the durable queue."""
        endpoint = EndpointRegistration(
            role=self.role,
            cluster=self.cluster if self.role == EndpointRole.WORKER else None,
            hostname=socket.gethostname(),
            pid=os.getpid(),
        )
        self.endpoint = self.queue.register_endpoint(endpoint)
        return self.endpoint

    def run_once(self) -> RelayJob | None:
        """Run one leased cluster job if available."""
        if self.role != EndpointRole.WORKER:
            return None
        endpoint = self.endpoint or self.register()
        self._reconcile_canceled_scheduler_jobs()
        lease = self.queue.acquire_next_job(
            endpoint.endpoint_id,
            cluster=self.cluster,
            ttl_seconds=self.lease_ttl_seconds,
        )
        if lease is None:
            return None
        job = self.queue.get_job(lease.job_id)
        try:
            self._run_job(job, lease)
        finally:
            self.queue.release_lease(lease.lease_id)
        return self.queue.get_job(job.job_id)

    def serve_forever(self, *, poll_seconds: float = 2.0) -> None:
        """Run the endpoint loop until interrupted."""
        self.register()
        if self.role == EndpointRole.DESKTOP:
            while True:
                time.sleep(poll_seconds)
        with self._single_cluster_worker_lock():
            while True:
                self.run_once()
                time.sleep(poll_seconds)

    def _run_job(self, job: RelayJob, lease: Lease) -> None:
        if self.queue.get_job(job.job_id).state == JobState.CANCELED:
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
        spool = JobSpool(self.settings.spool_dir, job)
        spool.initialize()
        yaml_text = self._render_job_yaml(job)
        pipeline_path = spool.write_pipeline(yaml_text)
        package_progress_adapter = package_progress_adapter_from_pipeline(yaml_text)
        if package_progress_adapter is not None:
            package_progress_adapter.run_id = job.job_id
        package_progress_logs = _package_progress_log_paths(yaml_text)
        package_progress_log_offsets = {path: 0 for path in package_progress_logs}
        progress_sidecar_token = secrets.token_urlsafe(32)
        progress_sidecar = spool.path / f".progress-{secrets.token_hex(16)}.jsonl"
        progress_sidecar_offset = [0]
        scheduler_job_ids: list[str] = []
        scheduler_cancel_attempted = [False]
        self.queue.append_artifact(spool.artifact_for(pipeline_path, kind="jarvis_pipeline"))
        self.queue.append_event(
            job.job_id,
            "jarvis.started",
            "JARVIS-CD pipeline started",
            payload={"pipeline": str(pipeline_path)},
        )
        with _temporary_env_vars(
            {
                "CLIO_RELAY_PROGRESS_FILE": str(progress_sidecar),
                "CLIO_RELAY_PROGRESS_TOKEN": progress_sidecar_token,
            }
        ):
            result = self.provider.run_pipeline_streaming(
                pipeline_path,
                cwd=spool.path,
                on_stdout=lambda text: self._append_output(
                    job,
                    spool,
                    "stdout",
                    text,
                    package_progress_adapter=package_progress_adapter,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_task_id=task.task_id,
                ),
                on_stderr=lambda text: self._append_output(
                    job,
                    spool,
                    "stderr",
                    text,
                    scheduler_job_ids=scheduler_job_ids,
                    scheduler_task_id=task.task_id,
                ),
                on_start=lambda pid: self._append_execution_start(job, pid),
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
                    progress_sidecar=progress_sidecar,
                    progress_sidecar_offset=progress_sidecar_offset,
                    progress_sidecar_token=progress_sidecar_token,
                    package_progress_adapter=package_progress_adapter,
                    package_progress_log_offsets=package_progress_log_offsets,
                ),
            )
        self._ingest_progress_sidecar(
            job,
            progress_sidecar,
            progress_sidecar_offset=progress_sidecar_offset,
            progress_sidecar_token=progress_sidecar_token,
        )
        if package_progress_adapter is not None:
            self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                package_progress_log_offsets,
            )
        self.queue.append_artifact(spool.artifact_for(spool.path / "stdout.log", kind="stdout"))
        self.queue.append_artifact(spool.artifact_for(spool.path / "stderr.log", kind="stderr"))
        self._append_optional_result_artifacts(job, spool)
        terminal_state = (
            JobState.CANCELED
            if self.queue.get_job(job.job_id).state == JobState.CANCELED
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
        )
        if self.queue.get_job(job.job_id).state == JobState.CANCELED:
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
                "spool": {
                    "path": str(spool.path),
                    "pipeline": str(pipeline_path),
                    "stdout": str(spool.path / "stdout.log"),
                    "stderr": str(spool.path / "stderr.log"),
                },
                "artifacts": {
                    "pipeline": _file_summary(pipeline_path),
                    "stdout": _file_summary(spool.path / "stdout.log"),
                    "stderr": _file_summary(spool.path / "stderr.log"),
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

    def _append_output(
        self,
        job: RelayJob,
        spool: JobSpool,
        stream_name: str,
        text: str,
        package_progress_adapter: LammpsThermoProgressAdapter | None = None,
        scheduler_job_ids: list[str] | None = None,
        scheduler_task_id: str | None = None,
    ) -> None:
        if stream_name not in {"stdout", "stderr"}:
            raise ConfigurationError(f"unsupported stream: {stream_name}")
        typed_stream = "stdout" if stream_name == "stdout" else "stderr"
        spool.append_log(typed_stream, text)
        event = self.queue.append_event(
            job.job_id,
            f"{stream_name}.delta",
            text.rstrip("\n") or f"{stream_name} output",
            payload={"stream": stream_name, "text": text},
        )
        if scheduler_job_ids is not None:
            self._capture_scheduler_job_ids(
                job,
                text,
                scheduler_job_ids,
                scheduler_task_id=scheduler_task_id,
            )
        if typed_stream != "stdout":
            return
        self._append_ignored_stdout_markers(job, text)
        if package_progress_adapter is not None:
            self._append_package_progress_records(
                job,
                package_progress_adapter.observe_jarvis_stdout(text),
                source_event_seq=event.seq,
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
        progress_sidecar_token: str | None = None,
    ) -> None:
        for typed_payload in records:
            try:
                metadata = _optional_metadata(typed_payload.get("metadata"))
                if progress_sidecar_token is not None:
                    _validate_progress_sidecar_token(metadata, progress_sidecar_token)
                progress = ProgressRecord(
                    job_id=job.job_id,
                    label=str(typed_payload.get("label", "progress")),
                    current=_optional_float(typed_payload.get("current")),
                    total=_optional_float(typed_payload.get("total")),
                    unit=_optional_str(typed_payload.get("unit")),
                    message=_optional_str(typed_payload.get("message")),
                    source_event_seq=source_event_seq,
                    metadata=(
                        _trusted_sidecar_metadata(metadata, job_id=job.job_id)
                        if progress_sidecar_token is not None
                        else _trusted_package_metadata(metadata, job_id=job.job_id)
                    ),
                )
                validate_package_progress_metadata(progress.metadata)
            except (ConfigurationError, ValueError) as exc:
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
        progress_sidecar_token: str,
    ) -> None:
        if not progress_sidecar.exists():
            return
        with progress_sidecar.open("r", encoding="utf-8") as handle:
            handle.seek(progress_sidecar_offset[0])
            for line in handle:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.queue.append_event(
                        job.job_id,
                        "progress.parse_failed",
                        f"Side-channel package progress could not be parsed: {exc}",
                    )
                    continue
                if not isinstance(payload, dict):
                    self.queue.append_event(
                        job.job_id,
                        "progress.parse_failed",
                        "Side-channel package progress payload was not an object",
                    )
                    continue
                self._append_package_progress_records(
                    job,
                    [cast(dict[str, object], payload)],
                    source_event_seq=None,
                    progress_sidecar_token=progress_sidecar_token,
                )
            progress_sidecar_offset[0] = handle.tell()

    def _poll_running_job(
        self,
        lease: Lease,
        last_renewed_at: list[float],
        *,
        job: RelayJob,
        progress_sidecar: Path,
        progress_sidecar_offset: list[int],
        progress_sidecar_token: str,
        package_progress_adapter: LammpsThermoProgressAdapter | None = None,
        package_progress_log_offsets: dict[Path, int] | None = None,
    ) -> None:
        self._renew_lease_if_needed(lease, last_renewed_at)
        self._ingest_progress_sidecar(
            job,
            progress_sidecar,
            progress_sidecar_offset=progress_sidecar_offset,
            progress_sidecar_token=progress_sidecar_token,
        )
        if package_progress_adapter is not None and package_progress_log_offsets is not None:
            self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                package_progress_log_offsets,
            )

    def _ingest_package_progress_logs(
        self,
        job: RelayJob,
        package_progress_adapter: LammpsThermoProgressAdapter,
        log_offsets: dict[Path, int],
    ) -> None:
        for path, offset in list(log_offsets.items()):
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(offset)
                text = handle.read()
                log_offsets[path] = handle.tell()
            if text == "":
                continue
            self._append_package_progress_records(
                job,
                package_progress_adapter.observe_stdout(text),
                source_event_seq=None,
            )

    def _append_execution_start(self, job: RelayJob, pid: int) -> None:
        self.queue.append_event(
            job.job_id,
            "execution.started",
            f"JARVIS-CD process started: {pid}",
            payload={"pid": pid},
        )

    def _capture_scheduler_job_ids(
        self,
        job: RelayJob,
        text: str,
        scheduler_job_ids: list[str],
        *,
        scheduler_task_id: str | None,
    ) -> None:
        for line in text.splitlines():
            job_id = _extract_scheduler_job_id(line)
            if job_id is None or job_id in scheduler_job_ids:
                continue
            scheduler_job_ids.append(job_id)
            if scheduler_task_id is not None:
                self._persist_scheduler_job_ids(
                    job,
                    scheduler_task_id,
                    scheduler_job_ids,
                )
            self.queue.append_event(
                job.job_id,
                "scheduler.job_detected",
                f"Scheduler job detected: {job_id}",
                payload={"scheduler": "slurm", "scheduler_job_id": job_id},
            )

    def _should_cancel_job(
        self,
        job: RelayJob,
        *,
        task_id: str,
        scheduler_job_ids: list[str],
        scheduler_cancel_attempted: list[bool],
    ) -> bool:
        canceled = self.queue.get_job(job.job_id).state == JobState.CANCELED
        if not canceled or scheduler_cancel_attempted[0]:
            return canceled
        scheduler_cancel_attempted[0] = True
        self._cancel_scheduler_jobs(
            job,
            self._durable_scheduler_job_ids(job, task_id, scheduler_job_ids),
        )
        return True

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
        if durable_scheduler_job_ids and not scheduler_cancel_attempted[0]:
            self._cancel_scheduler_jobs(job, durable_scheduler_job_ids)
            scheduler_cancel_attempted[0] = True

    def _persist_scheduler_job_ids(
        self,
        job: RelayJob,
        task_id: str,
        scheduler_job_ids: list[str],
    ) -> None:
        self.queue.update_task_state(
            task_id,
            JobState.RUNNING,
            message="Scheduler job id detected",
            metadata={
                "scheduler": "slurm",
                "scheduler_job_ids": list(scheduler_job_ids),
            },
        )

    def _durable_scheduler_job_ids(
        self,
        job: RelayJob,
        task_id: str,
        scheduler_job_ids: list[str],
    ) -> list[str]:
        ids = list(scheduler_job_ids)
        for task in self.queue.list_tasks(job.job_id):
            if task.task_id != task_id:
                continue
            stored = task.metadata.get("scheduler_job_ids")
            if not isinstance(stored, list):
                continue
            for item in cast(list[object], stored):
                if isinstance(item, str) and item not in ids:
                    ids.append(item)
        return ids

    def _cancel_scheduler_jobs(self, job: RelayJob, scheduler_job_ids: list[str]) -> None:
        if not scheduler_job_ids:
            return
        for scheduler_job_id in scheduler_job_ids:
            result = subprocess.run(
                ["scancel", scheduler_job_id],
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode == 0:
                self.queue.append_event(
                    job.job_id,
                    "scheduler.cancel_requested",
                    f"Requested scheduler cancellation: {scheduler_job_id}",
                    payload={"scheduler": "slurm", "scheduler_job_id": scheduler_job_id},
                )
                continue
            self.queue.append_event(
                job.job_id,
                "scheduler.cancel_failed",
                f"Scheduler cancellation failed: {scheduler_job_id}",
                payload={
                    "scheduler": "slurm",
                    "scheduler_job_id": scheduler_job_id,
                    "returncode": result.returncode,
                    "stderr": result.stderr,
                },
            )

    def _reconcile_canceled_scheduler_jobs(self) -> None:
        for job in self.queue.list_jobs():
            if job.cluster != self.cluster or job.state != JobState.CANCELED:
                continue
            for task in self.queue.list_tasks(job.job_id):
                scheduler_job_ids = _scheduler_job_ids_from_metadata(task.metadata)
                if not scheduler_job_ids:
                    continue
                pending = [
                    scheduler_job_id
                    for scheduler_job_id in scheduler_job_ids
                    if not self._scheduler_cancel_already_recorded(job.job_id, scheduler_job_id)
                ]
                if pending:
                    self._cancel_scheduler_jobs(job, pending)

    def _scheduler_cancel_already_recorded(
        self,
        job_id: str,
        scheduler_job_id: str,
    ) -> bool:
        events, _ = self.queue.drain_events(Cursor(job_id=job_id, next_seq=1), limit=10000)
        for event in events:
            if event.event_type not in {"scheduler.cancel_requested", "scheduler.cancel_failed"}:
                continue
            if event.payload.get("scheduler_job_id") == scheduler_job_id:
                return True
        return False

    def _append_optional_result_artifacts(self, job: RelayJob, spool: JobSpool) -> None:
        candidates = {
            "agent_result": spool.path / "agent-result.json",
            "agent_last_message": spool.path / "agent-last-message.txt",
            "mcp_result": spool.path / "mcp-result.json",
        }
        for kind, path in candidates.items():
            if path.exists():
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
        self.queue.renew_lease(lease.lease_id, ttl_seconds=self.lease_ttl_seconds)
        last_renewed_at[0] = now

    @contextmanager
    def _single_cluster_worker_lock(self) -> Generator[None]:
        lock_path = self.settings.core_dir / f"{self.cluster}-worker.lock"
        lock = FileLock(str(lock_path), timeout=0)
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


def bootstrap_cluster_environment(settings: RelaySettings) -> None:
    """Create endpoint directories and verify required executables are configured."""
    settings.core_dir.mkdir(parents=True, exist_ok=True)
    settings.spool_dir.mkdir(parents=True, exist_ok=True)
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    provider = JarvisCdProvider(
        jarvis_bin=settings.jarvis_bin,
        agent_bin=settings.agent_bin,
        agent_adapter=settings.agent_adapter,
        agent_args=settings.agent_args,
    )
    provider.require_available()
    if settings.frps_addr is None or settings.frp_token is None:
        raise ConfigurationError("CLIO_RELAY_FRPS_ADDR and CLIO_RELAY_FRP_TOKEN are required")


def _file_summary(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _extract_scheduler_job_id(line: str) -> str | None:
    explicit = re.search(r"\bscheduler_job_id=(?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if explicit is not None:
        return explicit.group("job_id")
    submitted = re.search(r"\bSubmitted batch job (?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if submitted is not None:
        return submitted.group("job_id")
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


def _validate_progress_sidecar_token(metadata: dict[str, object], expected_token: str) -> None:
    if metadata.get("relay_progress_token") != expected_token:
        raise ConfigurationError("side-channel package progress token did not match")


def _trusted_package_metadata(metadata: dict[str, object], *, job_id: str) -> dict[str, object]:
    package_name = metadata.get("package_name")
    package_version = metadata.get("package_version")
    return package_progress_metadata(
        metadata,
        package_name=package_name if isinstance(package_name, str) and package_name else "unknown",
        package_version=(
            package_version if isinstance(package_version, str) and package_version else "unknown"
        ),
        run_id=job_id,
    )


def _trusted_sidecar_metadata(metadata: dict[str, object], *, job_id: str) -> dict[str, object]:
    preserved = {
        key: value
        for key, value in metadata.items()
        if key
        not in {
            "source",
            "package_name",
            "package_version",
            "run_id",
            "execution_id",
            "adapter",
            "relay_progress_token",
        }
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


def _package_progress_log_paths(pipeline_yaml: str) -> list[Path]:
    loaded = yaml.safe_load(pipeline_yaml)
    if not isinstance(loaded, dict):
        return []
    typed_document = cast(dict[str, Any], loaded)
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return []
    typed_packages = cast(list[object], packages)
    if len(typed_packages) != 1:
        return []
    package = typed_packages[0]
    if not isinstance(package, dict):
        return []
    typed_package = cast(dict[str, Any], package)
    if typed_package.get("pkg_type") != "builtin.lammps":
        return []
    output_dir = typed_package.get("out")
    if not isinstance(output_dir, str) or output_dir == "":
        return []
    expanded = os.path.expanduser(os.path.expandvars(output_dir))
    return [Path(expanded) / "log.lammps"]


def _scheduler_job_ids_from_metadata(metadata: dict[str, Any]) -> list[str]:
    stored = metadata.get("scheduler_job_ids")
    if not isinstance(stored, list):
        return []
    ids: list[str] = []
    for item in cast(list[object], stored):
        if isinstance(item, str) and item not in ids:
            ids.append(item)
    return ids


@contextmanager
def _temporary_env_vars(values: dict[str, str]) -> Generator[None]:
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
