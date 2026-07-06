"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI, HTTPException

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.models import ArtifactRef, Cursor, JobState, RelayEvent, RelayJob


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

    @app.get("/jobs/{job_id}/artifacts", response_model=list[ArtifactRef])
    def get_artifacts(job_id: str) -> list[ArtifactRef]:
        return queue.list_artifacts(job_id)

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob)
    def cancel_job(job_id: str) -> RelayJob:
        return queue.update_job_state(job_id, state=JobState.CANCELED)

    return app


app = create_app()
