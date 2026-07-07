"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI, HTTPException

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.models import ArtifactRef, Cursor, JobState, MonitorRule, RelayEvent, RelayJob
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    monitor_job,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)


def create_app(settings: RelaySettings | None = None) -> FastAPI:
    """Create the FastAPI relay surface."""
    resolved = settings or RelaySettings.from_env()
    queue = ClioCoreQueue(resolved.core_dir)
    queue.initialize()
    app = FastAPI(title="clio-relay")

    @app.post("/jobs", response_model=RelayJob)
    def submit_job(job: RelayJob) -> RelayJob:
        return queue.submit_job(job)

    @app.get("/jobs/{job_id}", response_model=RelayJob)
    def get_job(job_id: str) -> RelayJob:
        try:
            return queue.get_job(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/events", response_model=list[RelayEvent])
    def get_events(job_id: str, cursor: int = 1, limit: int = 100) -> list[RelayEvent]:
        events, _ = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
        return events

    @app.get("/jobs/{job_id}/monitor")
    def monitor(job_id: str, cursor: int = 1, limit: int = 100) -> dict[str, object]:
        try:
            return monitor_job(queue, job_id, cursor=cursor, limit=limit)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/wait", response_model=RelayJob)
    def wait(job_id: str, timeout_seconds: float = 600, poll_seconds: float = 2) -> RelayJob:
        try:
            return wait_for_terminal(
                queue,
                job_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=408, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/logs/{stream_name}")
    def get_log(
        job_id: str,
        stream_name: str,
        offset: int = 0,
        limit: int = 65536,
    ) -> dict[str, object]:
        try:
            if stream_name not in {"stdout", "stderr"}:
                raise HTTPException(status_code=400, detail="stream must be stdout or stderr")
            return read_job_log(
                resolved,
                queue.get_job(job_id),
                stream_name="stdout" if stream_name == "stdout" else "stderr",
                offset=offset,
                limit=limit,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/artifacts", response_model=list[ArtifactRef])
    def get_artifacts(job_id: str) -> list[ArtifactRef]:
        return queue.list_artifacts(job_id)

    @app.get("/artifacts/{artifact_id}/content")
    def get_artifact_content(artifact_id: str) -> dict[str, object]:
        try:
            return read_artifact_bytes(queue, artifact_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob)
    def cancel_job(job_id: str) -> RelayJob:
        return queue.update_job_state(job_id, state=JobState.CANCELED)

    @app.post("/monitor/rules", response_model=MonitorRule)
    def create_monitor_rule(rule: MonitorRule) -> MonitorRule:
        try:
            return queue.append_monitor_rule(rule)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/monitor/rules", response_model=list[MonitorRule])
    def list_monitor_rules(job_id: str | None = None) -> list[MonitorRule]:
        return queue.list_monitor_rules(job_id)

    @app.post("/monitor/run-once")
    def run_monitor_once(limit: int = 100) -> list[dict[str, object]]:
        return evaluate_monitor_rules(queue, limit=limit)

    return app


app = create_app()
