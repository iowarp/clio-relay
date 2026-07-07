"""Long-running desktop and cluster endpoint behavior."""

from __future__ import annotations

import hashlib
import os
import socket
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock, Timeout

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpCallSpec,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    utc_now,
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
        self.queue.append_artifact(spool.artifact_for(pipeline_path, kind="jarvis_pipeline"))
        self.queue.append_event(
            job.job_id,
            "jarvis.started",
            "JARVIS-CD pipeline started",
            payload={"pipeline": str(pipeline_path)},
        )
        result = self.provider.run_pipeline_streaming(
            pipeline_path,
            cwd=spool.path,
            on_stdout=lambda text: self._append_output(job, spool, "stdout", text),
            on_stderr=lambda text: self._append_output(job, spool, "stderr", text),
            on_start=lambda pid: self._append_execution_start(job, pid),
            should_cancel=lambda: self.queue.get_job(job.job_id).state == JobState.CANCELED,
            on_poll=lambda: self._renew_lease_if_needed(lease, last_renewed_at),
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
    ) -> None:
        if stream_name not in {"stdout", "stderr"}:
            raise ConfigurationError(f"unsupported stream: {stream_name}")
        typed_stream = "stdout" if stream_name == "stdout" else "stderr"
        spool.append_log(typed_stream, text)
        self.queue.append_event(
            job.job_id,
            f"{stream_name}.delta",
            text.rstrip("\n") or f"{stream_name} output",
            payload={"stream": stream_name, "text": text},
        )

    def _append_execution_start(self, job: RelayJob, pid: int) -> None:
        self.queue.append_event(
            job.job_id,
            "execution.started",
            f"JARVIS-CD process started: {pid}",
            payload={"pid": pid},
        )

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
